//! Shared harness of the end-to-end tests: a gateway started from a
//! [`GatewaySpec`] on an ephemeral port with a fresh virtual key, HTTP/1 and
//! HTTP/2 clients that can drop their connection at any moment, request-body
//! builders, and helpers around the scripted upstream.
//!
//! [`TestGateway::finish`] shuts the gateway down, collects every `Outcome`
//! and asserts that the gateway emitted exactly one per request the clients
//! sent (section 7.1 of the M1 contract) and dropped none, so every e2e test
//! checks that invariant without saying so.

#![allow(dead_code, reason = "each e2e test binary uses a different subset")]
#![allow(
    clippy::disallowed_types,
    reason = "error bodies are inspected as a Value; brisk-gateway has no serde derive"
)]

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use brisk_gateway::auth::{KEY_RANDOM_BYTES, format_key, key_digest};
use brisk_gateway::outcome::{Outcome, OutcomeReceiver, OutcomeTally, outcome_channel};
use brisk_gateway::server::{ServerConfig, bind};
use brisk_gateway::spec::{
    ChannelSpec, ExperimentSpec, ForwardingSpec, GatewaySpec, KeySpec, LimitsSpec, StreamUsage,
    Timeouts, WarmupSpec, WarmupTarget,
};
use brisk_gateway::upstream::UpstreamClientConfig;
use brisk_gateway::{BoxError, Gateway};
use bytes::Bytes;
use http::header::{AUTHORIZATION, CONTENT_TYPE, HOST};
use http::{HeaderMap, Method, Request, Response, StatusCode};
use http_body_util::channel::{Channel, Sender};
use http_body_util::combinators::UnsyncBoxBody;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::timeout;

use crate::scripted::{RecordedRequest, Reply, ScriptedUpstream, SseEnd};

/// Default model of every test (section 4.1); the brackets must pass through
/// untouched.
pub(crate) const TEST_MODEL: &str = "grok-4.6(xhigh)";

/// Chat Completions path on the gateway.
pub(crate) const CHAT_PATH: &str = "/v1/chat/completions";

/// Weight that makes a channel the first choice. With the other channels at
/// weight 1 the chance of another channel going first is below 1e-9.
pub(crate) const FIRST: u32 = u32::MAX;

/// Body type of the test clients: a full body or a body fed through a
/// channel, so uploads can stall or run without `Content-Length`.
pub(crate) type ClientBody = UnsyncBoxBody<Bytes, BoxError>;

/// Wraps `bytes` as a client body with an exact size.
pub(crate) fn full(bytes: impl Into<Bytes>) -> ClientBody {
    Full::new(bytes.into())
        .map_err(|never: Infallible| match never {})
        .boxed_unsync()
}

/// A client body fed through the returned sender; its size is unknown, so an
/// HTTP/1 client sends it chunked.
pub(crate) fn streamed() -> (Sender<Bytes, BoxError>, ClientBody) {
    let (sender, body) = Channel::<Bytes, BoxError>::new(4);
    (sender, body.boxed_unsync())
}

/// `timing_bound(strict, lenient)` of section 4.1: the strict target only
/// with `BRISK_STRICT_TIMING=1`, which is set on the bench VM alone.
pub(crate) fn timing_bound(strict: Duration, lenient: Duration) -> Duration {
    if std::env::var_os("BRISK_STRICT_TIMING").is_some_and(|value| value == "1") {
        strict
    } else {
        lenient
    }
}

/// A Chat Completions request body for `model`, with `stream` and, when
/// `include_usage` is set, `stream_options.include_usage`.
pub(crate) fn chat_body(model: &str, stream: bool, include_usage: Option<bool>) -> Bytes {
    let model = serde_json::to_string(model).expect("a str serialises");
    let stream_options = match include_usage {
        Some(value) => format!(r#","stream_options":{{"include_usage":{value}}}"#),
        None => String::new(),
    };
    Bytes::from(format!(
        r#"{{"model":{model},"messages":[{{"role":"user","content":"Reply with exactly: pong"}}],"stream":{stream}{stream_options}}}"#
    ))
}

/// A channel to `base_url` with the test defaults: loopback allowed (the
/// test upstreams listen on 127.0.0.1), serving every model, weight 1.
pub(crate) fn channel(name: &str, base_url: &str, stream_usage: StreamUsage) -> ChannelSpec {
    ChannelSpec {
        name: name.to_owned(),
        base_url: base_url.to_owned(),
        api_key: brisk_gateway::secret::Redacted::new(format!("upstream-key-{name}")),
        weight: 1,
        models: Vec::new(),
        model_map: Vec::new(),
        stream_usage,
        timeouts: Timeouts::default(),
        client: UpstreamClientConfig {
            allow_private: true,
            ..UpstreamClientConfig::default()
        },
        warmup: WarmupTarget::default(),
        expose_ratelimit_headers: false,
    }
}

/// `http://127.0.0.1:<port>/v1` of a port nothing listens on.
pub(crate) fn refused_base_url() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind an ephemeral port");
    let addr = listener.local_addr().expect("local address");
    drop(listener);
    format!("http://{addr}/v1")
}

/// Every `Outcome` of a finished gateway and their totals.
#[derive(Debug)]
pub(crate) struct Settled {
    pub(crate) tally: OutcomeTally,
    pub(crate) outcomes: Vec<Outcome>,
}

impl Settled {
    /// The only outcome; fails when there is not exactly one.
    pub(crate) fn only(&self) -> &Outcome {
        assert_eq!(self.outcomes.len(), 1, "{:#?}", self.outcomes);
        &self.outcomes[0]
    }
}

/// A gateway serving on `127.0.0.1:<ephemeral>`.
pub(crate) struct TestGateway {
    pub(crate) addr: SocketAddr,
    /// The virtual key in clear text.
    pub(crate) key: String,
    gateway: Gateway,
    outcomes: OutcomeReceiver,
    received: Vec<Outcome>,
    sent: Arc<AtomicU64>,
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<std::io::Result<()>>,
}

/// Starts a gateway and waits until `/readyz` answers 200.
pub(crate) async fn start_gateway(customize: impl FnOnce(&mut GatewaySpec)) -> TestGateway {
    let gateway = start_gateway_unready(customize);
    gateway.wait_ready(Duration::from_secs(10)).await;
    gateway
}

/// Starts a gateway without waiting for its first warm-up round. Must run
/// inside a Tokio runtime, which serves it.
pub(crate) fn start_gateway_unready(customize: impl FnOnce(&mut GatewaySpec)) -> TestGateway {
    let mut random = [0_u8; KEY_RANDOM_BYTES];
    fastrand::fill(&mut random);
    let key = format_key(&random).into_inner();

    let mut spec = GatewaySpec {
        server: ServerConfig {
            // Tests that exercise the grace period set their own value.
            graceful_shutdown_timeout: Duration::from_secs(5),
            ..ServerConfig::default()
        },
        limits: LimitsSpec::default(),
        forwarding: ForwardingSpec::default(),
        warmup: WarmupSpec::default(),
        experiments: ExperimentSpec::default(),
        keys: vec![KeySpec {
            name: String::from("e2e"),
            sha256: key_digest(key.as_bytes()),
        }],
        channels: Vec::new(),
    };
    customize(&mut spec);

    let listener = bind("127.0.0.1:0".parse().expect("valid address"), &spec.server)
        .expect("bind the gateway listener");
    let addr = listener.local_addr().expect("local address");
    let (sink, outcomes) = outcome_channel(1024);
    let gateway = Gateway::new(spec, sink).expect("the test spec is valid");
    let (shutdown, stop) = oneshot::channel::<()>();
    let task = tokio::spawn(gateway.clone().serve(listener, None, async move {
        // A dropped sender also stops the gateway, so a failing test cannot
        // leak it.
        let _ = stop.await;
    }));
    TestGateway {
        addr,
        key,
        gateway,
        outcomes,
        received: Vec::new(),
        sent: Arc::new(AtomicU64::new(0)),
        shutdown: Some(shutdown),
        task,
    }
}

impl TestGateway {
    /// The gateway handle, for `is_ready`, `counters` and `warnings`.
    pub(crate) fn gateway(&self) -> &Gateway {
        &self.gateway
    }

    /// A new HTTP/1.1 connection to the gateway.
    pub(crate) async fn h1(&self) -> Client {
        Client::connect(self.addr, Protocol::Http1, Arc::clone(&self.sent)).await
    }

    /// A new HTTP/2 (prior knowledge) connection to the gateway.
    pub(crate) async fn h2(&self) -> Client {
        Client::connect(self.addr, Protocol::Http2, Arc::clone(&self.sent)).await
    }

    /// A new connection of `protocol`.
    pub(crate) async fn client(&self, protocol: Protocol) -> Client {
        Client::connect(self.addr, protocol, Arc::clone(&self.sent)).await
    }

    /// `POST /v1/chat/completions` with the gateway's key, without a body.
    pub(crate) fn chat_request(&self) -> http::request::Builder {
        Request::builder()
            .method(Method::POST)
            .uri(CHAT_PATH)
            .header(AUTHORIZATION, format!("Bearer {}", self.key))
            .header(CONTENT_TYPE, "application/json")
    }

    /// A complete chat request with `body`.
    pub(crate) fn chat_with(&self, body: impl Into<Bytes>) -> Request<ClientBody> {
        self.chat_request().body(full(body)).expect("valid request")
    }

    /// Sends a chat request on a fresh HTTP/1.1 connection and reads the
    /// whole response.
    pub(crate) async fn chat(&self, body: impl Into<Bytes>) -> Collected {
        let request = self.chat_with(body);
        self.h1().await.send_collect(request).await
    }

    /// Polls `/readyz` until it answers 200.
    pub(crate) async fn wait_ready(&self, limit: Duration) {
        let deadline = tokio::time::Instant::now() + limit;
        loop {
            let mut client = self.h1().await;
            let response = client
                .send_uncounted(
                    Request::get("/readyz")
                        .body(full(Bytes::new()))
                        .expect("valid request"),
                )
                .await;
            if response.status == StatusCode::OK {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "gateway not ready after {limit:?}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Requests counted so far; each must produce exactly one `Outcome`.
    pub(crate) fn sent(&self) -> u64 {
        self.sent.load(Ordering::SeqCst)
    }

    /// Waits for the next `Outcome`, keeping it for [`Self::finish`].
    pub(crate) async fn next_outcome(&mut self, limit: Duration) -> Outcome {
        let outcome = timeout(limit, self.outcomes.recv())
            .await
            .unwrap_or_else(|_| panic!("no outcome within {limit:?}"))
            .expect("the outcome channel closed");
        self.received.push(outcome.clone());
        outcome
    }

    /// Completes the `shutdown` future given to `serve`.
    pub(crate) fn begin_shutdown(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            // The serve task may already have stopped, which the caller sees
            // when it joins it.
            let _ = shutdown.send(());
        }
    }

    /// Shuts the gateway down, waits for `serve` and for the outcomes of
    /// every sent request, and asserts one outcome per request and none
    /// dropped.
    pub(crate) async fn finish(mut self) -> Settled {
        self.begin_shutdown();
        timeout(Duration::from_secs(60), &mut self.task)
            .await
            .expect("serve did not return after shutdown")
            .expect("serve task panicked")
            .expect("serve failed");

        let sent = self.sent();
        // A bounded drain may still run after `serve` returned; it emits its
        // outcome within `drain_timeout`.
        while (self.received.len() as u64) < sent {
            match timeout(Duration::from_secs(10), self.outcomes.recv()).await {
                Ok(Some(outcome)) => self.received.push(outcome),
                Ok(None) | Err(_) => break,
            }
        }
        while let Ok(outcome) = self.outcomes.try_recv() {
            self.received.push(outcome);
        }
        assert_eq!(
            self.received.len() as u64,
            sent,
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
        Settled {
            tally,
            outcomes: self.received,
        }
    }

    /// Ends `serve` as `how` says, waits until its task is gone and drops
    /// this handle's gateway; returns the outcome receiver, whose `recv`
    /// yields `None` once every other holder of an `OutcomeSink` is gone.
    ///
    /// It skips the one-outcome-per-request check of [`Self::finish`], so it
    /// refuses a gateway that was sent requests.
    pub(crate) async fn stop_serve(mut self, how: StopServe) -> OutcomeReceiver {
        assert_eq!(
            self.sent(),
            0,
            "a gateway that was sent requests ends with finish"
        );
        match how {
            StopServe::Shutdown => {
                self.begin_shutdown();
                timeout(Duration::from_secs(60), &mut self.task)
                    .await
                    .expect("serve did not return after shutdown")
                    .expect("serve task panicked")
                    .expect("serve failed");
            }
            StopServe::Drop => {
                // `self.shutdown` is still held, so `serve` cannot take the
                // graceful path before the abort lands.
                self.task.abort();
                let joined = timeout(Duration::from_secs(10), &mut self.task)
                    .await
                    .expect("the aborted serve task did not end");
                // A cancelled task resolves only after its future was dropped.
                let error = joined.expect_err("serve returned although it was aborted");
                assert!(error.is_cancelled(), "{error}");
            }
        }
        self.outcomes
    }
}

/// How [`TestGateway::stop_serve`] ends `serve`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StopServe {
    /// Completes the `shutdown` future, so `serve` returns.
    Shutdown,
    /// Aborts the serve task without completing `shutdown`, dropping the
    /// `serve` future, as `brisk` does on a second signal.
    Drop,
}

/// Protocol of a test client connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Protocol {
    Http1,
    Http2,
}

enum ClientSender {
    Http1(hyper::client::conn::http1::SendRequest<ClientBody>),
    Http2(hyper::client::conn::http2::SendRequest<ClientBody>),
}

/// One client connection to the gateway. Dropping it, or calling
/// [`Client::close`], closes the TCP connection.
pub(crate) struct Client {
    addr: SocketAddr,
    sender: ClientSender,
    connection: JoinHandle<()>,
    sent: Arc<AtomicU64>,
}

/// A response read to the end.
#[derive(Debug)]
pub(crate) struct Collected {
    pub(crate) status: StatusCode,
    pub(crate) headers: HeaderMap,
    pub(crate) body: Bytes,
}

impl Collected {
    /// The body as UTF-8 text.
    pub(crate) fn text(&self) -> &str {
        std::str::from_utf8(&self.body).expect("the body is UTF-8")
    }

    /// The body parsed as JSON.
    pub(crate) fn json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.body)
            .unwrap_or_else(|err| panic!("not JSON ({err}): {}", self.text()))
    }

    /// The `error.code` of a Brisk error body.
    pub(crate) fn error_code(&self) -> String {
        let json = self.json();
        json["error"]["code"]
            .as_str()
            .unwrap_or_else(|| panic!("not an error body: {}", self.text()))
            .to_owned()
    }
}

impl Client {
    async fn connect(addr: SocketAddr, protocol: Protocol, sent: Arc<AtomicU64>) -> Self {
        let stream = TcpStream::connect(addr)
            .await
            .expect("connect to the gateway");
        stream.set_nodelay(true).expect("set TCP_NODELAY");
        let io = TokioIo::new(stream);
        let (sender, connection) = match protocol {
            Protocol::Http1 => {
                let (sender, connection) = hyper::client::conn::http1::handshake(io)
                    .await
                    .expect("HTTP/1.1 handshake");
                let task = tokio::spawn(async move {
                    // The error of a connection the test closed on purpose
                    // carries no information.
                    let _ = connection.await;
                });
                (ClientSender::Http1(sender), task)
            }
            Protocol::Http2 => {
                let (sender, connection) =
                    hyper::client::conn::http2::handshake(TokioExecutor::new(), io)
                        .await
                        .expect("HTTP/2 handshake");
                let task = tokio::spawn(async move {
                    let _ = connection.await;
                });
                (ClientSender::Http2(sender), task)
            }
        };
        Self {
            addr,
            sender,
            connection,
            sent,
        }
    }

    /// Sends `request` and returns the response head; counts one request.
    pub(crate) async fn send(&mut self, request: Request<ClientBody>) -> Response<Incoming> {
        self.sent.fetch_add(1, Ordering::SeqCst);
        self.send_raw(request).await.expect("the gateway answered")
    }

    /// Like [`Self::send`], returning the error when the gateway closed the
    /// connection instead of answering.
    pub(crate) async fn try_send(
        &mut self,
        request: Request<ClientBody>,
    ) -> Result<Response<Incoming>, hyper::Error> {
        self.sent.fetch_add(1, Ordering::SeqCst);
        self.send_raw(request).await
    }

    /// Sends and reads the whole response; counts one request.
    pub(crate) async fn send_collect(&mut self, request: Request<ClientBody>) -> Collected {
        let response = self.send(request).await;
        collect(response).await
    }

    /// Sends a request that produces no `Outcome` (`/healthz`, `/readyz`)
    /// and reads the whole response.
    pub(crate) async fn send_uncounted(&mut self, request: Request<ClientBody>) -> Collected {
        let response = self.send_raw(request).await.expect("the gateway answered");
        collect(response).await
    }

    async fn send_raw(
        &mut self,
        mut request: Request<ClientBody>,
    ) -> Result<Response<Incoming>, hyper::Error> {
        match &mut self.sender {
            ClientSender::Http1(sender) => {
                request
                    .headers_mut()
                    .entry(HOST)
                    .or_insert_with(|| self.addr.to_string().parse().expect("valid host"));
                sender.ready().await?;
                sender.send_request(request).await
            }
            ClientSender::Http2(sender) => {
                // HTTP/2 carries the authority in the request URI.
                let path = request
                    .uri()
                    .path_and_query()
                    .map_or("/", |path| path.as_str())
                    .to_owned();
                *request.uri_mut() = format!("http://{}{path}", self.addr)
                    .parse()
                    .expect("valid URI");
                sender.ready().await?;
                sender.send_request(request).await
            }
        }
    }

    /// Closes the TCP connection now, abandoning any response in flight.
    pub(crate) fn close(self) {
        self.connection.abort();
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        self.connection.abort();
    }
}

/// Reads a response to the end.
pub(crate) async fn collect(response: Response<Incoming>) -> Collected {
    let (parts, body) = response.into_parts();
    let body = body
        .collect()
        .await
        .expect("the response body completed")
        .to_bytes();
    Collected {
        status: parts.status,
        headers: parts.headers,
        body,
    }
}

/// Reads a response body until it ends or fails, returning the bytes
/// received and whether it ended cleanly.
pub(crate) async fn collect_until_error(response: Response<Incoming>) -> (Bytes, bool) {
    let mut body = response.into_body();
    let mut received = Vec::new();
    loop {
        match body.frame().await {
            Some(Ok(frame)) => {
                if let Some(data) = frame.data_ref() {
                    received.extend_from_slice(data);
                }
            }
            Some(Err(_)) => return (Bytes::from(received), false),
            None => return (Bytes::from(received), true),
        }
    }
}

/// Reads body frames until `needle` has been received; returns everything
/// read so far.
pub(crate) async fn read_until(body: &mut Incoming, needle: &[u8]) -> Vec<u8> {
    let mut received = Vec::new();
    while !contains(&received, needle) {
        let frame = body
            .frame()
            .await
            .expect("the body ended before the expected bytes")
            .expect("the body failed before the expected bytes");
        if let Some(data) = frame.data_ref() {
            received.extend_from_slice(data);
        }
    }
    received
}

/// `needle` occurs in `haystack`.
pub(crate) fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

// ---- scripted upstream helpers -------------------------------------------

/// Headers of a scripted SSE reply.
pub(crate) fn sse_headers() -> Vec<(&'static str, String)> {
    vec![("content-type", String::from("text/event-stream"))]
}

/// Headers of a scripted JSON reply.
pub(crate) fn json_headers() -> Vec<(&'static str, String)> {
    vec![("content-type", String::from("application/json"))]
}

/// A content chunk in the shape CPA sends.
pub(crate) const CONTENT_EVENT: &[u8] = b"data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"pong\"},\"finish_reason\":null}]}\n\n";

/// The CPA-shaped last chunk: `finish_reason` and usage in one event.
pub(crate) const FINISH_USAGE_EVENT: &[u8] = b"data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":11,\"completion_tokens\":7,\"total_tokens\":18}}\n\n";

/// An OpenAI-shaped finish chunk without usage.
pub(crate) const FINISH_EVENT: &[u8] = b"data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n";

/// An OpenAI-shaped usage-only chunk.
pub(crate) const USAGE_ONLY_EVENT: &[u8] = b"data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"choices\":[],\"usage\":{\"prompt_tokens\":11,\"completion_tokens\":7,\"total_tokens\":18}}\n\n";

/// The stream terminator.
pub(crate) const DONE_EVENT: &[u8] = b"data: [DONE]\n\n";

/// Usage carried by [`FINISH_USAGE_EVENT`] and [`USAGE_ONLY_EVENT`].
pub(crate) const SCRIPTED_USAGE: (u64, u64) = (11, 7);

/// A complete CPA-shaped stream: one content chunk, the finish-and-usage
/// chunk and `[DONE]`.
pub(crate) fn stream_ok() -> Bytes {
    [CONTENT_EVENT, FINISH_USAGE_EVENT, DONE_EVENT]
        .concat()
        .into()
}

/// A 200 SSE reply sending `frames` back to back, then ending the body.
pub(crate) fn sse_reply(frames: &[&[u8]]) -> Reply {
    Reply::Sse {
        head_delay: Duration::ZERO,
        headers: sse_headers(),
        frames: frames
            .iter()
            .map(|frame| (Duration::ZERO, Bytes::copy_from_slice(frame)))
            .collect(),
        end: SseEnd::Finish,
    }
}

/// The successful stream reply most failover tests expect from channel two.
pub(crate) fn sse_ok() -> Reply {
    Reply::Sse {
        head_delay: Duration::ZERO,
        headers: sse_headers(),
        frames: vec![(Duration::ZERO, stream_ok())],
        end: SseEnd::Finish,
    }
}

/// A plain status reply with a body and `Content-Length`.
pub(crate) fn status_reply(status: u16, body: &'static [u8]) -> Reply {
    Reply::Status {
        status,
        headers: json_headers(),
        body: Bytes::from_static(body),
    }
}

/// A script that answers warm-up requests with an empty 200 and hands chat
/// requests to `script`, numbering them from 0 among chat requests only.
pub(crate) fn chat_only(
    script: impl Fn(usize, &RecordedRequest) -> Reply + Send + Sync + 'static,
) -> impl Fn(usize, &RecordedRequest) -> Reply + Send + Sync + 'static {
    let chats = AtomicUsize::new(0);
    move |_, request| {
        if is_chat(request) {
            script(chats.fetch_add(1, Ordering::SeqCst), request)
        } else {
            Reply::Status {
                status: 200,
                headers: Vec::new(),
                body: Bytes::new(),
            }
        }
    }
}

/// `request` is a forwarded Chat Completions request, not a warm-up.
pub(crate) fn is_chat(request: &RecordedRequest) -> bool {
    request.method == "POST" && request.target.ends_with("/chat/completions")
}

/// The chat requests `upstream` received, in order.
pub(crate) fn chat_requests(upstream: &ScriptedUpstream) -> Vec<RecordedRequest> {
    upstream.requests().into_iter().filter(is_chat).collect()
}

/// Values of header `name` (lowercase) in a recorded request.
pub(crate) fn header_values<'r>(request: &'r RecordedRequest, name: &str) -> Vec<&'r [u8]> {
    request
        .headers
        .iter()
        .filter(|(key, _)| key.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_slice())
        .collect()
}
