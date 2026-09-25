//! `brisk serve` run as the binary against an in-process upstream: it starts
//! from a `*.local.toml` on an ephemeral port, answers a chat request with
//! 200 in plaintext or over inbound TLS, and stops on `SIGTERM` after the
//! graceful shutdown (2.9). The default log filter keeps the per-request
//! outcome records off and `RUST_LOG` turns them on (D27). Windows has no way
//! to send another process Ctrl-C, so there the test ends the process after
//! the request instead.

mod support;

use std::convert::Infallible;
use std::io::{BufRead as _, BufReader};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Stdio};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread::JoinHandle;
use std::time::Duration;

use brisk::keygen::{self, GeneratedKey};
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

use support::{CERT_PEM, KEY_PEM, TEST_MODEL, TempDir, brisk};

/// The channel's upstream key; the upstream answers 401 to anything else.
const UPSTREAM_KEY: &str = "sk-upstream-serve";

/// A non-streamed completion with usage, as CPA answers.
const COMPLETION: &str = r#"{"id":"chatcmpl-1","object":"chat.completion","created":0,"model":"grok-4.6(xhigh)","choices":[{"index":0,"message":{"role":"assistant","content":"pong"},"finish_reason":"stop"}],"usage":{"prompt_tokens":11,"completion_tokens":7,"total_tokens":18}}"#;

/// Longest wait for any step of the child process.
const STEP_LIMIT: Duration = Duration::from_secs(30);

/// How a per-request outcome record appears on stderr: the fmt layer writes
/// the event's target, `brisk::outcome`, before the message.
const OUTCOME_RECORD: &str = " brisk::outcome: outcome ";

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

/// Writes `smoke.local.toml` into `dir`: a loopback listener on an ephemeral
/// port with `server_tables` after the `[server]` keys, the `[[keys]]` table
/// of `key`, and one channel on `upstream` with an inline secret.
///
/// On Unix the file is mode 0644, so the loader warns that it holds a secret
/// other users can read, whatever the umask of the machine.
fn write_config(
    dir: &TempDir,
    upstream: SocketAddr,
    key: &GeneratedKey,
    server_tables: &str,
) -> PathBuf {
    let config = dir.write(
        "smoke.local.toml",
        &format!(
            r#"
[server]
listen = "127.0.0.1:0"
workers = 2
{server_tables}

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
    #[cfg(unix)]
    support::set_mode(&config, 0o644);
    config
}

/// Starts `brisk serve` on `config`, with `RUST_LOG` set to `rust_log` or
/// removed, and waits for its listening line.
async fn start(config: &Path, rust_log: Option<&str>) -> (KillOnDrop, Stderr, SocketAddr) {
    let mut command = brisk();
    command
        .arg("serve")
        .arg("--config")
        .arg(config)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(filter) = rust_log {
        command.env("RUST_LOG", filter);
    }
    let mut child = KillOnDrop(command.spawn().expect("start brisk serve"));
    let stderr = Stderr::capture(&mut child.0);
    let addr = stderr.listen_addr().await;
    assert!(addr.ip().is_loopback() && addr.port() != 0, "{addr}");
    (child, stderr, addr)
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

    /// The first line containing `needle`, waiting for it up to
    /// [`STEP_LIMIT`].
    async fn wait_for(&self, needle: &str) -> String {
        let deadline = Instant::now() + STEP_LIMIT;
        loop {
            if let Some(line) = self
                .snapshot()
                .into_iter()
                .find(|line| line.contains(needle))
            {
                return line;
            }
            assert!(
                Instant::now() < deadline,
                "no line with {needle:?}; stderr:\n{}",
                self.snapshot().join("\n")
            );
            sleep(Duration::from_millis(20)).await;
        }
    }

    /// The address logged by the `brisk listening` line.
    async fn listen_addr(&self) -> SocketAddr {
        let line = self.wait_for("brisk listening").await;
        line.split_whitespace()
            .find_map(|token| token.strip_prefix("listen="))
            .unwrap_or_else(|| panic!("no listen field: {line}"))
            .parse()
            .expect("a socket address")
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

/// A client that ignores proxy settings of the machine running the tests.
fn plain_client() -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .build()
        .expect("build the test client")
}

/// Polls `{base}/readyz` until it answers 200.
async fn wait_ready(client: &reqwest::Client, base: &str) {
    let deadline = Instant::now() + STEP_LIMIT;
    loop {
        let response = client
            .get(format!("{base}/readyz"))
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

/// Sends one chat request to `{base}` and expects the upstream's completion.
async fn chat(client: &reqwest::Client, base: &str, key: &GeneratedKey) {
    let response = client
        .post(format!("{base}/v1/chat/completions"))
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
}

/// The per-request outcome records in `log`.
fn outcome_records(log: &str) -> Vec<&str> {
    log.lines()
        .filter(|line| line.contains(OUTCOME_RECORD))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serves_a_chat_request_and_stops() {
    let upstream = start_upstream().await;
    let key = keygen::generate("smoke").expect("generate a key");
    let dir = TempDir::new("serve");
    let config = write_config(&dir, upstream, &key, "");

    let (mut child, stderr, addr) = start(&config, None).await;
    let client = plain_client();
    let base = format!("http://{addr}");
    wait_ready(&client, &base).await;
    chat(&client, &base, &key).await;

    let log = stop(&mut child.0, stderr).await;
    for secret in [UPSTREAM_KEY, key.key.expose().as_str()] {
        assert!(!log.contains(secret), "a key was logged:\n{log}");
    }
    // The default filter drops the record of the request served above.
    assert_eq!(outcome_records(&log), Vec::<&str>::new(), "{log}");
    // The loader's warnings are logged as soon as tracing is up, before the
    // gateway starts listening (1.5).
    #[cfg(unix)]
    {
        let lines: Vec<&str> = log.lines().collect();
        let warning = support::world_readable_warning(&config);
        let warned = lines
            .iter()
            .position(|line| line.contains(" WARN brisk: ") && line.ends_with(&warning))
            .unwrap_or_else(|| panic!("no warning {warning:?}; stderr:\n{log}"));
        let listening = lines
            .iter()
            .position(|line| line.contains("brisk listening"))
            .expect("the listening line");
        assert!(warned < listening, "{log}");
    }
    let mut stdout = String::new();
    std::io::Read::read_to_string(
        &mut child.0.stdout.take().expect("stdout is piped"),
        &mut stdout,
    )
    .expect("read stdout");
    assert!(stdout.is_empty(), "serve wrote to stdout: {stdout:?}");
}

/// The counterpart of the default-filter assertion above: the same request
/// under `RUST_LOG=info` leaves exactly one record, so the assertion can tell
/// the two apart.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rust_log_turns_the_outcome_records_on() {
    let upstream = start_upstream().await;
    let key = keygen::generate("smoke").expect("generate a key");
    let dir = TempDir::new("serve-outcomes");
    let config = write_config(&dir, upstream, &key, "");

    let (mut child, stderr, addr) = start(&config, Some("info")).await;
    let client = plain_client();
    let base = format!("http://{addr}");
    wait_ready(&client, &base).await;
    chat(&client, &base, &key).await;
    // Windows ends the process without a final drain, so the record must be
    // out before the stop.
    stderr.wait_for(OUTCOME_RECORD).await;

    let log = stop(&mut child.0, stderr).await;
    let records = outcome_records(&log);
    assert_eq!(records.len(), 1, "{log}");
    for field in [
        "status=Completed",
        "http_status=200",
        "billed_input=11",
        "billed_output=7",
    ] {
        assert!(records[0].contains(field), "{field}: {}", records[0]);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serves_over_inbound_tls() {
    let upstream = start_upstream().await;
    let key = keygen::generate("smoke").expect("generate a key");
    let dir = TempDir::new("serve-tls");
    dir.write("cert.pem", CERT_PEM);
    dir.write("key.pem", KEY_PEM);
    // Owner-only, as a deployed key would be, whatever the umask.
    #[cfg(unix)]
    support::set_mode(&dir.path().join("key.pem"), 0o600);
    let config = write_config(
        &dir,
        upstream,
        &key,
        "\n[server.tls]\ncert = \"cert.pem\"\nkey = \"key.pem\"",
    );

    let (mut child, stderr, addr) = start(&config, None).await;
    let listening = stderr.wait_for("brisk listening").await;
    assert!(listening.contains("tls=true"), "{listening}");

    // The certificate names `localhost`; the name must resolve to the
    // listener, whichever loopback address `localhost` means here.
    let client = reqwest::Client::builder()
        .no_proxy()
        .tls_certs_only([
            reqwest::Certificate::from_pem(CERT_PEM.as_bytes()).expect("a PEM certificate")
        ])
        .resolve("localhost", addr)
        .build()
        .expect("build the TLS test client");
    let base = format!("https://localhost:{}", addr.port());
    wait_ready(&client, &base).await;
    chat(&client, &base, &key).await;

    // The listener speaks only TLS: a plaintext request gets no HTTP answer.
    let plaintext = plain_client()
        .get(format!("http://{addr}/readyz"))
        .timeout(STEP_LIMIT)
        .send()
        .await;
    assert!(plaintext.is_err(), "{plaintext:?}");

    stop(&mut child.0, stderr).await;
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
///
/// The logger drains its queue every 100 ms. Pausing for a few intervals
/// first lets it take the request's record, so that the default filter, not
/// the kill, is what keeps the record out of the log.
#[cfg(not(unix))]
async fn stop(child: &mut Child, stderr: Stderr) -> String {
    sleep(Duration::from_millis(500)).await;
    child.kill().expect("terminate brisk");
    wait_exit(child).await;
    stderr.finish()
}
