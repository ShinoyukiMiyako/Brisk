//! `brisk serve` run as the binary against an in-process upstream: it starts
//! from a `*.local.toml` on an ephemeral port, answers a chat request with
//! 200, and stops on `SIGTERM` after the graceful shutdown (2.9). Windows has
//! no way to send another process Ctrl-C, so there the test ends the process
//! after the request instead.

mod support;

use std::convert::Infallible;
use std::io::{BufRead as _, BufReader};
use std::net::SocketAddr;
use std::process::{Child, Stdio};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread::JoinHandle;
use std::time::Duration;

use brisk::keygen;
use bytes::Bytes;
use http_body_util::{BodyExt as _, Full};
use hyper::body::Incoming;
use hyper::header::{AUTHORIZATION, CONTENT_TYPE};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tokio::time::{Instant, sleep};

use support::{TEST_MODEL, TempDir, brisk};

/// The channel's upstream key; the upstream answers 401 to anything else.
const UPSTREAM_KEY: &str = "sk-upstream-serve";

/// A non-streamed completion with usage, as CPA answers.
const COMPLETION: &str = r#"{"id":"chatcmpl-1","object":"chat.completion","created":0,"model":"grok-4.6(xhigh)","choices":[{"index":0,"message":{"role":"assistant","content":"pong"},"finish_reason":"stop"}],"usage":{"prompt_tokens":11,"completion_tokens":7,"total_tokens":18}}"#;

/// Longest wait for any step of the child process.
const STEP_LIMIT: Duration = Duration::from_secs(30);

/// Serves an empty 200 to warm-up requests and [`COMPLETION`] to chat
/// requests carrying [`UPSTREAM_KEY`], until the test ends.
async fn start_upstream() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind the upstream");
    let addr = listener.local_addr().expect("upstream address");
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.expect("accept");
            tokio::spawn(async move {
                // The gateway may reset the connection when it stops.
                let _ = http1::Builder::new()
                    .serve_connection(
                        TokioIo::new(stream),
                        service_fn(|request| async { Ok::<_, Infallible>(reply(request).await) }),
                    )
                    .await;
            });
        }
    });
    addr
}

async fn reply(request: Request<Incoming>) -> Response<Full<Bytes>> {
    let is_chat = request.uri().path() == "/v1/chat/completions";
    let authorized = request
        .headers()
        .get(AUTHORIZATION)
        .is_some_and(|value| value.as_bytes() == format!("Bearer {UPSTREAM_KEY}").as_bytes());
    request
        .into_body()
        .collect()
        .await
        .expect("read the request body");
    if !is_chat {
        return Response::new(Full::default());
    }
    if !authorized {
        let mut response = Response::new(Full::default());
        *response.status_mut() = StatusCode::UNAUTHORIZED;
        return response;
    }
    Response::builder()
        .header(CONTENT_TYPE, "application/json")
        .body(Full::new(Bytes::from_static(COMPLETION.as_bytes())))
        .expect("valid response")
}

/// The child's stderr, collected line by line on a thread.
struct Stderr {
    lines: Arc<Mutex<Vec<String>>>,
    reader: JoinHandle<()>,
}

impl Stderr {
    fn capture(child: &mut Child) -> Self {
        let stderr = child.stderr.take().expect("stderr is piped");
        let lines = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&lines);
        let reader = std::thread::spawn(move || {
            for line in BufReader::new(stderr).lines() {
                let line = line.expect("read stderr");
                sink.lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(line);
            }
        });
        Self { lines, reader }
    }

    fn snapshot(&self) -> Vec<String> {
        self.lines
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// The address logged by the `brisk listening` line.
    async fn listen_addr(&self) -> SocketAddr {
        let deadline = Instant::now() + STEP_LIMIT;
        loop {
            let found = self.snapshot().iter().find_map(|line| {
                if !line.contains("brisk listening") {
                    return None;
                }
                let field = line
                    .split_whitespace()
                    .find_map(|token| token.strip_prefix("listen="))?;
                Some(field.parse().expect("a socket address"))
            });
            if let Some(addr) = found {
                return addr;
            }
            assert!(
                Instant::now() < deadline,
                "no listening line; stderr:\n{}",
                self.snapshot().join("\n")
            );
            sleep(Duration::from_millis(20)).await;
        }
    }

    /// Every line, once the child has exited and closed stderr.
    fn finish(self) -> String {
        let Self { lines, reader } = self;
        reader.join().expect("the stderr reader panicked");
        lines
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .join("\n")
    }
}

/// Waits for the child to exit.
async fn wait_exit(child: &mut Child) -> std::process::ExitStatus {
    let deadline = Instant::now() + STEP_LIMIT;
    loop {
        if let Some(status) = child.try_wait().expect("poll the child") {
            return status;
        }
        assert!(Instant::now() < deadline, "brisk did not exit");
        sleep(Duration::from_millis(20)).await;
    }
}

/// Polls `/readyz` until it answers 200.
async fn wait_ready(client: &reqwest::Client, addr: SocketAddr) {
    let deadline = Instant::now() + STEP_LIMIT;
    loop {
        let response = client
            .get(format!("http://{addr}/readyz"))
            .send()
            .await
            .expect("/readyz answers");
        if response.status() == StatusCode::OK {
            return;
        }
        assert!(Instant::now() < deadline, "the gateway never became ready");
        sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serves_a_chat_request_and_stops() {
    let upstream = start_upstream().await;
    let key = keygen::generate("smoke").expect("generate a key");
    let dir = TempDir::new("serve");
    let config = dir.write(
        "smoke.local.toml",
        &format!(
            r#"
[server]
listen = "127.0.0.1:0"
workers = 2

{snippet}

[[channels]]
name = "local"
base_url = "http://{upstream}/v1"
api_key = {{ value = "{UPSTREAM_KEY}" }}
allow_private = true
models = ["{TEST_MODEL}"]
warmup = {{ method = "GET", path = "/v1/models" }}
"#,
            snippet = key.toml_snippet
        ),
    );

    let mut child = KillOnDrop(
        brisk()
            .arg("serve")
            .arg("--config")
            .arg(&config)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("start brisk serve"),
    );
    let stderr = Stderr::capture(&mut child.0);
    let addr = stderr.listen_addr().await;
    assert!(addr.ip().is_loopback() && addr.port() != 0, "{addr}");

    let client = reqwest::Client::builder()
        .no_proxy()
        .build()
        .expect("build the test client");
    wait_ready(&client, addr).await;
    let response = client
        .post(format!("http://{addr}/v1/chat/completions"))
        .bearer_auth(key.key.expose())
        .header(CONTENT_TYPE, "application/json")
        .body(format!(
            r#"{{"model":"{TEST_MODEL}","messages":[{{"role":"user","content":"Reply with exactly: pong"}}]}}"#
        ))
        .send()
        .await
        .expect("the gateway answers");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.bytes().await.expect("read the body"),
        COMPLETION.as_bytes()
    );

    let log = stop(&mut child.0, stderr).await;
    for secret in [UPSTREAM_KEY, key.key.expose().as_str()] {
        assert!(!log.contains(secret), "a key was logged:\n{log}");
    }
    let mut stdout = String::new();
    std::io::Read::read_to_string(
        &mut child.0.stdout.take().expect("stdout is piped"),
        &mut stdout,
    )
    .expect("read stdout");
    assert!(stdout.is_empty(), "serve wrote to stdout: {stdout:?}");
}

/// Ends the child if the test fails before stopping it.
struct KillOnDrop(Child);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        // Best effort on the failure path; after a normal stop the child has
        // exited and both calls are no-ops.
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Sends `SIGTERM` and expects the graceful path: exit status 0, the final
/// outcome totals with the one request, and the stop line. Returns stderr.
#[cfg(unix)]
async fn stop(child: &mut Child, stderr: Stderr) -> String {
    let status = std::process::Command::new("kill")
        .arg("-TERM")
        .arg(child.id().to_string())
        .status()
        .expect("run kill");
    assert!(status.success(), "kill -TERM failed: {status}");
    let status = wait_exit(child).await;
    let log = stderr.finish();
    assert!(status.success(), "{status}; stderr:\n{log}");
    // Without a terminal the fmt layer writes plain `name=value` fields, with
    // string values in `Debug` form.
    assert!(log.contains(r#"signal="SIGTERM""#), "{log}");
    let totals = log
        .lines()
        .find(|line| line.contains("outcome totals") && line.contains(r#"kind="final""#))
        .unwrap_or_else(|| panic!("no final totals; stderr:\n{log}"));
    assert!(totals.contains("requests=1"), "{totals}");
    assert!(totals.contains("completed=1"), "{totals}");
    assert!(log.contains("brisk stopped"), "{log}");
    log
}

/// Windows cannot deliver Ctrl-C to another process without sharing its
/// console, so the process is terminated; the graceful path is covered by the
/// library's unit tests. Returns stderr.
#[cfg(not(unix))]
async fn stop(child: &mut Child, stderr: Stderr) -> String {
    child.kill().expect("terminate brisk");
    wait_exit(child).await;
    stderr.finish()
}
