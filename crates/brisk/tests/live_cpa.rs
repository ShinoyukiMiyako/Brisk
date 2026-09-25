//! Live tests against the LAN CPA (4.9).
//!
//! Opt-in: every test is ignored by default and needs the CPA on the LAN plus
//! a configuration for it. Per D19 they must not run before the M0
//! measurements end; then run them one at a time with
//!
//! ```text
//! cargo test -p brisk --test live_cpa -- --ignored --test-threads 1
//! ```
//!
//! The configuration comes from `BRISK_LIVE_CONFIG` or, when that is unset,
//! from `cpa.local.toml` at the repository root (a copy of
//! `config/cpa.example.toml` with the real CPA key). A missing file fails
//! every test with the reason instead of skipping it: a run that asked for
//! the live tests must not pass without talking to the CPA.
//!
//! Each test loads the file with `brisk::config::load`, replaces its
//! `[[keys]]` with a freshly generated virtual key and serves the gateway in
//! plain HTTP on `127.0.0.1:0` inside the test's runtime; the file's
//! `server.listen`, `server.workers` and `[server.tls]` are not used. The
//! model is always `grok-4.6(xhigh)` and the prompt `Reply with exactly: pong`.

mod support;

use std::io;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::time::Duration;

use brisk::config::{self, LoadedConfig};
use brisk::keygen;
use brisk_gateway::Gateway;
use brisk_gateway::auth::key_digest;
use brisk_gateway::outcome::{
    Outcome, OutcomeReceiver, OutcomeStatus, OutcomeTally, outcome_channel,
};
use brisk_gateway::secret::Redacted;
use brisk_gateway::server;
use brisk_gateway::spec::{ChannelId, ChannelSpec, GatewaySpec, KeySpec};
use bytes::Bytes;
use http_body_util::{BodyExt as _, Full};
use hyper::body::Incoming;
use hyper::client::conn::http1;
use hyper::header::{AUTHORIZATION, CONTENT_TYPE, HOST, HeaderName};
use hyper::{HeaderMap, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde_json::Value;
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep, timeout};

use support::TEST_MODEL;

/// Environment variable naming the live configuration file.
const CONFIG_ENV: &str = "BRISK_LIVE_CONFIG";

/// The live configuration at the repository root when [`CONFIG_ENV`] is
/// unset; `*.local.toml` is ignored by git.
const DEFAULT_CONFIG: &str = "cpa.local.toml";

/// The prompt of every chat request.
const PROMPT: &str = "Reply with exactly: pong";

/// Chat Completions path on the gateway.
const CHAT_PATH: &str = "/v1/chat/completions";

/// Upstream key that CPA does not know.
const WRONG_KEY: &str = "sk-brisk-live-cpa-wrong-key";

/// Longest wait for `/readyz`; the gateway reports ready at the latest
/// `upstream.ready_timeout` (3 s by default) after it starts serving.
const READY_LIMIT: Duration = Duration::from_secs(30);

/// Longest wait for a response head or body: at `xhigh` effort the model may
/// think for a long time before it answers.
const RESPONSE_LIMIT: Duration = Duration::from_secs(300);

/// Longest wait for an `Outcome` after its response ended or the client
/// left; covers the bounded drain (`forwarding.drain_timeout`, 2 s).
const OUTCOME_LIMIT: Duration = Duration::from_secs(10);

/// Longest wait for `serve` to return after shutdown: the default grace
/// period is 30 s, and every test has read its responses by then.
const SHUTDOWN_LIMIT: Duration = Duration::from_secs(60);

/// Longest wait for CPA's answer to the wrong-key probe.
const PROBE_LIMIT: Duration = Duration::from_secs(30);

/// Silence before the request of the reuse test: longer than the 10 s after
/// which CPA closes a new connection that has not sent a byte (04 1.1).
const IDLE: Duration = Duration::from_secs(15);

// ---- configuration ----------------------------------------------------------

/// The live configuration file and where its path came from.
fn config_path() -> (PathBuf, &'static str) {
    if let Some(path) = std::env::var_os(CONFIG_ENV) {
        return (PathBuf::from(path), "from BRISK_LIVE_CONFIG");
    }
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crates/brisk lies two levels below the repository root");
    (
        root.join(DEFAULT_CONFIG),
        "the default; BRISK_LIVE_CONFIG is unset",
    )
}

/// Loads the live configuration, failing the test with the reason when the
/// file is missing or invalid.
fn load_live_config() -> LoadedConfig {
    let (path, source) = config_path();
    match path.try_exists() {
        Ok(true) => {}
        Ok(false) => panic!(
            "the live CPA configuration {} ({source}) does not exist. These opt-in tests \
             talk to the LAN CPA: copy config/cpa.example.toml to cpa.local.toml at the \
             repository root, or point BRISK_LIVE_CONFIG at such a file, with the CPA address \
             and key filled in",
            path.display()
        ),
        Err(err) => panic!(
            "cannot check the live CPA configuration {} ({source}): {err}",
            path.display()
        ),
    }
    let config = config::load(&path).unwrap_or_else(|err| {
        panic!(
            "{:#}",
            anyhow::Error::new(err).context(format!(
                "loading the live CPA configuration {}",
                path.display()
            ))
        )
    });
    for warning in &config.warnings {
        eprintln!("configuration warning: {warning}");
    }
    config
}

/// Whether `channel` serves `model`; an empty list serves every model.
fn serves(channel: &ChannelSpec, model: &str) -> bool {
    channel.models.is_empty() || channel.models.iter().any(|served| served == model)
}

/// Logs of the gateway (channel warnings, failovers, failed attempts) go to
/// the test output, which libtest shows for a failing test.
fn init_tracing() {
    // Fails only when an earlier test of this process installed it already.
    let _ = tracing_subscriber::fmt()
        .with_test_writer()
        .with_ansi(false)
        .try_init();
}

// ---- gateway ----------------------------------------------------------------

/// A gateway built from the live configuration, serving on
/// `127.0.0.1:<ephemeral>`.
struct LiveGateway {
    addr: SocketAddr,
    /// The virtual key in clear text.
    key: String,
    gateway: Gateway,
    outcomes: OutcomeReceiver,
    /// Outcomes read so far.
    received: Vec<Outcome>,
    /// Chat requests sent; each must produce exactly one outcome.
    sent: usize,
    shutdown: Option<oneshot::Sender<()>>,
    serving: JoinHandle<io::Result<()>>,
}

impl LiveGateway {
    /// Loads the configuration, replaces its keys with a fresh one, lets
    /// `customize` change the spec, starts serving and waits until `/readyz`
    /// answers 200.
    async fn start(customize: impl FnOnce(&mut GatewaySpec)) -> Self {
        init_tracing();
        let LoadedConfig {
            mut spec,
            outcome_queue,
            ..
        } = load_live_config();
        let key = keygen::generate("live-cpa")
            .expect("generate a virtual key")
            .key
            .into_inner();
        spec.keys = vec![KeySpec {
            name: String::from("live-cpa"),
            sha256: key_digest(key.as_bytes()),
        }];
        customize(&mut spec);

        let listener = server::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), &spec.server)
            .expect("bind 127.0.0.1:0");
        let addr = listener.local_addr().expect("the gateway address");
        let (sink, outcomes) = outcome_channel(outcome_queue);
        let gateway = Gateway::new(spec, sink).unwrap_or_else(|err| {
            panic!(
                "{:#}",
                anyhow::Error::new(err).context("building the gateway from the live configuration")
            )
        });
        let (shutdown, stop) = oneshot::channel::<()>();
        let serving = tokio::spawn(gateway.clone().serve(listener, None, async move {
            // A dropped sender also stops the gateway, so a failing test
            // cannot leak it.
            let _ = stop.await;
        }));
        let live = Self {
            addr,
            key,
            gateway,
            outcomes,
            received: Vec::new(),
            sent: 0,
            shutdown: Some(shutdown),
            serving,
        };
        live.wait_ready().await;
        live
    }

    /// Polls `/readyz` until it answers 200.
    async fn wait_ready(&self) {
        let deadline = Instant::now() + READY_LIMIT;
        loop {
            let request = Request::get("/readyz")
                .body(Full::default())
                .expect("a valid request");
            let status = Connection::open(self.addr)
                .await
                .send(request)
                .await
                .status();
            if status == StatusCode::OK {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "the gateway was not ready within {READY_LIMIT:?}"
            );
            sleep(Duration::from_millis(50)).await;
        }
    }

    /// `POST /v1/chat/completions` for [`TEST_MODEL`] with [`PROMPT`].
    fn chat_request(&self, stream: bool) -> Request<Full<Bytes>> {
        let body = serde_json::json!({
            "model": TEST_MODEL,
            "messages": [{"role": "user", "content": PROMPT}],
            "stream": stream,
        });
        Request::post(CHAT_PATH)
            .header(AUTHORIZATION, format!("Bearer {}", self.key))
            .header(CONTENT_TYPE, "application/json")
            .body(Full::new(Bytes::from(body.to_string())))
            .expect("a valid request")
    }

    /// Sends a chat request on a new connection and returns the connection
    /// with the response head; the body arrives while the connection lives.
    async fn send_chat(&mut self, stream: bool) -> (Connection, Response<Incoming>) {
        let request = self.chat_request(stream);
        let mut connection = Connection::open(self.addr).await;
        self.sent += 1;
        let response = connection.send(request).await;
        (connection, response)
    }

    /// Sends a chat request and reads the whole response.
    async fn chat(&mut self, stream: bool) -> Collected {
        let (_connection, response) = self.send_chat(stream).await;
        collect(response).await
    }

    /// Waits for the next `Outcome`, keeping it for [`Self::finish`].
    async fn next_outcome(&mut self) -> Outcome {
        let outcome = timeout(OUTCOME_LIMIT, self.outcomes.recv())
            .await
            .unwrap_or_else(|_| panic!("no outcome within {OUTCOME_LIMIT:?}"))
            .expect("the outcome queue closed");
        self.received.push(outcome.clone());
        outcome
    }

    /// Failed warm-up requests so far.
    fn warmup_failures(&self) -> u64 {
        self.gateway
            .counters()
            .warmup_failures
            .load(Ordering::Relaxed)
    }

    /// Shuts the gateway down gracefully, asserts one outcome per chat
    /// request and none dropped, and returns their totals.
    async fn finish(mut self) -> OutcomeTally {
        if let Some(shutdown) = self.shutdown.take() {
            // Fails only if serving already ended, which awaiting it reports.
            let _ = shutdown.send(());
        }
        timeout(SHUTDOWN_LIMIT, &mut self.serving)
            .await
            .unwrap_or_else(|_| panic!("serve did not return within {SHUTDOWN_LIMIT:?}"))
            .expect("the serve task panicked")
            .expect("serving failed");
        while let Ok(outcome) = self.outcomes.try_recv() {
            self.received.push(outcome);
        }
        assert_eq!(
            self.received.len(),
            self.sent,
            "one outcome per request: {:#?}",
            self.received
        );
        assert_eq!(
            self.gateway
                .counters()
                .outcomes_dropped
                .load(Ordering::Relaxed),
            0
        );
        let mut tally = OutcomeTally::default();
        for outcome in &self.received {
            tally.record(outcome);
        }
        tally
    }
}

// ---- client -----------------------------------------------------------------

/// One HTTP/1.1 connection to the gateway. Dropping it closes the TCP
/// connection, abandoning any response in flight.
struct Connection {
    addr: SocketAddr,
    sender: http1::SendRequest<Full<Bytes>>,
    task: JoinHandle<()>,
}

impl Connection {
    async fn open(addr: SocketAddr) -> Self {
        let stream = TcpStream::connect(addr)
            .await
            .expect("connect to the gateway");
        stream.set_nodelay(true).expect("set TCP_NODELAY");
        let (sender, connection) = http1::handshake(TokioIo::new(stream))
            .await
            .expect("HTTP/1.1 handshake");
        let task = tokio::spawn(async move {
            // A connection error also fails the response or body the test
            // is waiting for, which reports it.
            let _ = connection.await;
        });
        Self { addr, sender, task }
    }

    /// Sends `request` and waits for the response head.
    async fn send(&mut self, mut request: Request<Full<Bytes>>) -> Response<Incoming> {
        let host = self.addr.to_string().parse().expect("a valid Host header");
        request.headers_mut().insert(HOST, host);
        self.sender
            .ready()
            .await
            .expect("the connection accepts a request");
        timeout(RESPONSE_LIMIT, self.sender.send_request(request))
            .await
            .unwrap_or_else(|_| panic!("no response head within {RESPONSE_LIMIT:?}"))
            .expect("the gateway answered")
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// A response read to the end.
struct Collected {
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
}

impl Collected {
    /// The body as text, for assertions and failure messages.
    fn text(&self) -> &str {
        std::str::from_utf8(&self.body).expect("the body is UTF-8")
    }
}

/// Reads a response to the end.
async fn collect(response: Response<Incoming>) -> Collected {
    let (parts, body) = response.into_parts();
    let body = timeout(RESPONSE_LIMIT, body.collect())
        .await
        .unwrap_or_else(|_| panic!("the response body did not end within {RESPONSE_LIMIT:?}"))
        .expect("the response body completed")
        .to_bytes();
    Collected {
        status: parts.status,
        headers: parts.headers,
        body,
    }
}

// ---- assertions -------------------------------------------------------------

/// The `data` of every complete SSE event in `body`, in order. Lines end in
/// `\n` or `\r\n`; comments and other fields are skipped, and an event
/// without its terminating blank line is not complete.
fn sse_data(body: &str) -> Vec<String> {
    let mut events = Vec::new();
    let mut data: Option<String> = None;
    for line in body.split('\n') {
        let line = line.strip_suffix('\r').unwrap_or(line);
        if line.is_empty() {
            events.extend(data.take());
        } else if let Some(value) = line.strip_prefix("data:") {
            let value = value.strip_prefix(' ').unwrap_or(value);
            match &mut data {
                Some(event) => {
                    event.push('\n');
                    event.push_str(value);
                }
                None => data = Some(value.to_owned()),
            }
        }
    }
    events
}

/// Whether the chunk in `data` carries a non-empty `delta.content`.
fn has_content_delta(data: &str) -> bool {
    let chunk: Value = serde_json::from_str(data)
        .unwrap_or_else(|err| panic!("an SSE event is not JSON ({err}): {data}"));
    chunk["choices"].as_array().is_some_and(|choices| {
        choices.iter().any(|choice| {
            choice["delta"]["content"]
                .as_str()
                .is_some_and(|content| !content.is_empty())
        })
    })
}

/// None of CPA's own response headers reached the client: its CORS headers
/// and `X-CPA-TRACE-ID` are not on the allow list.
fn assert_no_cpa_headers(headers: &HeaderMap) {
    let leaked: Vec<&str> = headers
        .keys()
        .map(HeaderName::as_str)
        .filter(|name| name.starts_with("access-control-") || *name == "x-cpa-trace-id")
        .collect();
    assert!(
        leaked.is_empty(),
        "CPA headers reached the client: {leaked:?}"
    );
}

/// A complete streamed answer: 200, `text/event-stream`, at least one content
/// delta, `data: [DONE]` as the last event, and no CPA headers.
fn assert_complete_stream(response: &Collected) {
    assert_eq!(response.status, StatusCode::OK, "{}", response.text());
    let content_type = response
        .headers
        .get(CONTENT_TYPE)
        .expect("a content-type header")
        .to_str()
        .expect("a textual content-type");
    let media_type = content_type.split(';').next().unwrap_or_default().trim();
    assert!(
        media_type.eq_ignore_ascii_case("text/event-stream"),
        "{content_type}"
    );
    assert_no_cpa_headers(&response.headers);

    let events = sse_data(response.text());
    let (last, chunks) = events
        .split_last()
        .unwrap_or_else(|| panic!("no SSE event: {}", response.text()));
    assert_eq!(last, "[DONE]", "the last event is not [DONE]");
    assert!(
        chunks.iter().any(|data| has_content_delta(data)),
        "no content delta: {}",
        response.text()
    );
}

/// A `Completed` outcome with usage on both sides.
fn assert_completed_with_usage(outcome: &Outcome) {
    assert_eq!(outcome.status, OutcomeStatus::Completed, "{outcome:#?}");
    let usage = outcome
        .usage
        .unwrap_or_else(|| panic!("no usage: {outcome:#?}"));
    assert!(usage.input > 0 && usage.output > 0, "{outcome:#?}");
}

/// Asks CPA directly with `key`: an unknown key gets 401 with a string
/// `error` (04 2.2), the form in which Brisk's own channel key is wrong.
async fn assert_cpa_rejects_key(base_url: &str, key: &str) {
    let client = reqwest::Client::builder()
        .no_proxy()
        .build()
        .expect("build the probe client");
    let url = format!("{}/models", base_url.trim_end_matches('/'));
    let response = timeout(PROBE_LIMIT, client.get(&url).bearer_auth(key).send())
        .await
        .unwrap_or_else(|_| panic!("CPA did not answer GET {url} within {PROBE_LIMIT:?}"))
        .unwrap_or_else(|err| panic!("GET {url}: {err}"));
    let status = response.status();
    let body = response.bytes().await.expect("read CPA's answer");
    let text = String::from_utf8_lossy(&body);
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{text}");
    let answer: Value = serde_json::from_slice(&body)
        .unwrap_or_else(|err| panic!("CPA's 401 is not JSON ({err}): {text}"));
    assert!(
        answer["error"].is_string(),
        "CPA's 401 is not the string form: {text}"
    );
}

// ---- cases ------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs the LAN CPA and cpa.local.toml"]
async fn streams_a_completion() {
    let mut live = LiveGateway::start(|_| {}).await;

    let response = live.chat(true).await;
    assert_complete_stream(&response);
    let outcome = live.next_outcome().await;
    assert_completed_with_usage(&outcome);

    live.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs the LAN CPA and cpa.local.toml"]
async fn answers_a_non_streamed_completion() {
    let mut live = LiveGateway::start(|_| {}).await;

    let response = live.chat(false).await;
    assert_eq!(response.status, StatusCode::OK, "{}", response.text());
    let completion: Value = serde_json::from_slice(&response.body)
        .unwrap_or_else(|err| panic!("not JSON ({err}): {}", response.text()));
    assert_eq!(
        completion["object"],
        "chat.completion",
        "{}",
        response.text()
    );
    let outcome = live.next_outcome().await;
    assert_completed_with_usage(&outcome);

    live.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs the LAN CPA and cpa.local.toml"]
async fn fails_over_from_a_channel_with_a_wrong_key() {
    let mut first_base_url = String::new();
    let mut live = LiveGateway::start(|spec| {
        assert!(
            spec.forwarding.max_attempts >= 2,
            "failover needs forwarding.max_attempts >= 2, the configuration has {}",
            spec.forwarding.max_attempts
        );
        let serving = spec
            .channels
            .iter()
            .filter(|channel| serves(channel, TEST_MODEL))
            .count();
        assert!(
            spec.channels
                .first()
                .is_some_and(|first| serves(first, TEST_MODEL))
                && serving >= 2,
            "failover needs the first channel and at least one more to serve {TEST_MODEL}, \
             as in config/cpa.example.toml"
        );
        let first = &mut spec.channels[0];
        first.api_key = Redacted::new(String::from(WRONG_KEY));
        // With the other channels at their configured weights the first
        // channel is the first choice with near certainty.
        first.weight = u32::MAX;
        first_base_url.clone_from(&first.base_url);
    })
    .await;
    assert_cpa_rejects_key(&first_base_url, WRONG_KEY).await;

    let response = live.chat(false).await;
    assert_eq!(response.status, StatusCode::OK, "{}", response.text());
    let outcome = live.next_outcome().await;
    assert_eq!(outcome.attempts, 2, "{outcome:#?}");
    assert_ne!(outcome.channel, Some(ChannelId(0)), "{outcome:#?}");
    assert_eq!(outcome.status, OutcomeStatus::Completed, "{outcome:#?}");

    live.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs the LAN CPA and cpa.local.toml"]
async fn a_client_leaving_mid_stream_is_settled_as_cancelled() {
    let mut live = LiveGateway::start(|_| {}).await;

    let (connection, response) = live.send_chat(true).await;
    assert_eq!(response.status(), StatusCode::OK);
    let mut body = response.into_body();
    let mut received = Vec::new();
    // The stream has started once its first event is complete.
    while !received.windows(2).any(|window| window == b"\n\n") {
        let frame = timeout(RESPONSE_LIMIT, body.frame())
            .await
            .unwrap_or_else(|_| panic!("no event within {RESPONSE_LIMIT:?}"))
            .expect("the stream ended before its first event")
            .expect("the stream failed before its first event");
        if let Some(data) = frame.data_ref() {
            received.extend_from_slice(data);
        }
    }
    assert!(
        !String::from_utf8_lossy(&received).contains("data: [DONE]"),
        "the whole stream arrived before the client could leave"
    );
    drop(body);
    drop(connection);

    let outcome = live.next_outcome().await;
    assert!(
        matches!(
            outcome.status,
            OutcomeStatus::ClientCancelled | OutcomeStatus::Drained
        ),
        "{outcome:#?}"
    );

    live.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs the LAN CPA and cpa.local.toml"]
async fn reuses_warm_connections_after_15s_of_silence() {
    let mut live = LiveGateway::start(|_| {}).await;
    // The warm-up request has been answered on the pooled connection, so
    // CPA keeps it open through the silence (04 CPA-M1-7).
    sleep(IDLE).await;

    let response = live.chat(true).await;
    assert_complete_stream(&response);
    let outcome = live.next_outcome().await;
    assert_completed_with_usage(&outcome);
    let warmup_failures = live.warmup_failures();

    let tally = live.finish().await;
    assert_eq!(
        tally.failovers, 0,
        "a request after the silence failed over"
    );
    assert_eq!(warmup_failures, 0, "a warm-up request failed");
}
