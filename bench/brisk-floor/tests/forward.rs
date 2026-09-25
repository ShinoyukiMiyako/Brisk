//! End-to-end tests of the blind forwarder: a real inbound client talks to
//! floor (floor-A, and floor-B at the end of the file), which forwards to an
//! in-test upstream.

use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use brisk_bench_core::transport::certs::{
    CA_CERT_FILE, CertBundle, SERVER_CERT_FILE, SERVER_KEY_FILE,
};
use brisk_bench_core::transport::tls as bench_tls;
use brisk_floor::forward::{self, BAD_GATEWAY_BODY, BAD_TARGET_BODY, Forwarder};
use brisk_floor::same_task::{self, SameTaskForwarder};
use brisk_floor::tls;
use brisk_gateway::server::{self, ServerConfig};
use brisk_gateway::upstream::{UpstreamClientConfig, build_client};
use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Method, Request, Response, StatusCode, Uri};
use http_body_util::channel::{Channel, Sender};
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Empty, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio_rustls::TlsConnector;

/// Upper bound for any single step; generous so a loaded CI host does not
/// flake, while a buffering forwarder still fails instead of hanging.
const STEP: Duration = Duration::from_secs(10);

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Body type of every in-test upstream response and raw client request.
type TestBody = BoxBody<Bytes, BoxError>;

async fn within<T>(what: &str, fut: impl Future<Output = T>) -> T {
    tokio::time::timeout(STEP, fut)
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {what}"))
}

/// A streaming body and the sender that feeds it.
fn channel() -> (Sender<Bytes, BoxError>, TestBody) {
    let (sender, body) = Channel::new(4);
    (sender, body.boxed())
}

fn full(data: impl Into<Bytes>) -> TestBody {
    Full::new(data.into())
        .map_err(|never: Infallible| match never {})
        .boxed()
}

fn empty() -> TestBody {
    Empty::new()
        .map_err(|never: Infallible| match never {})
        .boxed()
}

/// Starts an HTTP/1.1 upstream on an ephemeral port that answers every request
/// with `handler`.
async fn spawn_upstream<F, Fut>(handler: F) -> SocketAddr
where
    F: Fn(Request<Incoming>) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = Response<TestBody>> + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let handler = handler.clone();
            tokio::spawn(async move {
                let service = service_fn(move |req| {
                    let response = handler(req);
                    async move { Ok::<_, Infallible>(response.await) }
                });
                // Errors here are client disconnects at test teardown and the
                // deliberate body abort.
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .await;
            });
        }
    });
    addr
}

/// A floor instance on an ephemeral port; dropping it stops the server.
struct Floor {
    addr: SocketAddr,
    _shutdown: oneshot::Sender<()>,
}

fn spawn_floor(upstream: &str, tls: Option<tokio_rustls::TlsAcceptor>) -> Floor {
    let config = ServerConfig::default();
    let client = build_client(&UpstreamClientConfig {
        // The test upstreams listen on loopback, as in the benchmark
        // topologies the binary allows them for.
        allow_private: true,
        ..UpstreamClientConfig::default()
    })
    .unwrap();
    let forwarder = Forwarder::new(client, upstream).unwrap();
    let listener = server::bind("127.0.0.1:0".parse().unwrap(), &config).unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown, stopped) = oneshot::channel::<()>();
    tokio::spawn(forward::run(listener, tls, config, forwarder, async {
        let _ = stopped.await;
    }));
    Floor {
        addr,
        _shutdown: shutdown,
    }
}

fn inbound_client(
    extra_root_certs: Vec<rustls_pki_types::CertificateDer<'static>>,
) -> reqwest::Client {
    build_client(&UpstreamClientConfig {
        extra_root_certs,
        // The floor under test listens on loopback; the TLS tests reach it
        // as `localhost` for SNI.
        allow_private: true,
        ..UpstreamClientConfig::default()
    })
    .unwrap()
}

/// Opens a raw HTTP/1.1 client connection, so the test controls every header
/// and the exact request-target (reqwest would rewrite both).
async fn h1_connect(addr: SocketAddr) -> hyper::client::conn::http1::SendRequest<TestBody> {
    let stream = TcpStream::connect(addr).await.unwrap();
    let (sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .unwrap();
    tokio::spawn(conn);
    sender
}

/// What the upstream saw of one request.
#[derive(Debug)]
struct Seen {
    method: Method,
    uri: Uri,
    headers: HeaderMap,
}

impl Seen {
    fn of(parts: &http::request::Parts) -> Self {
        Self {
            method: parts.method.clone(),
            uri: parts.uri.clone(),
            headers: parts.headers.clone(),
        }
    }
}

#[tokio::test]
async fn response_body_streams_incrementally_and_unmodified() {
    let (body_tx, mut body_rx) = mpsc::unbounded_channel::<Sender<Bytes, BoxError>>();
    let upstream = spawn_upstream(move |_req| {
        let body_tx = body_tx.clone();
        async move {
            let (sender, body) = channel();
            body_tx.send(sender).unwrap();
            Response::builder()
                .header("content-type", "text/event-stream")
                .header("x-upstream", "yes")
                .header("connection", "x-drop")
                .header("x-drop", "1")
                .header("keep-alive", "timeout=5")
                .body(body)
                .unwrap()
        }
    })
    .await;
    let floor = spawn_floor(&format!("http://{upstream}"), None);

    let client = inbound_client(Vec::new());
    // Spawned because the request future is lazy and the upstream only hands
    // out its body sender once the request arrives.
    let request = tokio::spawn(
        client
            .get(format!("http://{}/v1/stream", floor.addr))
            .send(),
    );
    let mut body_sender = within("upstream request", body_rx.recv()).await.unwrap();
    let mut response = within("response head", request).await.unwrap().unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let headers = response.headers();
    assert_eq!(headers["content-type"], "text/event-stream");
    assert_eq!(headers["x-upstream"], "yes");
    assert!(
        !headers.contains_key("x-drop"),
        "Connection-nominated header forwarded"
    );
    assert!(!headers.contains_key("keep-alive"), "Keep-Alive forwarded");

    // Every chunk must reach the client while the upstream still holds the
    // next one back; a buffering forwarder would stall on the first chunk.
    let binary: Vec<u8> = (0..=255u8).collect();
    let chunks: [&[u8]; 3] = [b"data: {\"k\":1}\n\n", &binary, b"data: [DONE]\n\n"];
    let mut received = Vec::new();
    for chunk in chunks {
        within(
            "upstream write",
            body_sender.send_data(Bytes::copy_from_slice(chunk)),
        )
        .await
        .unwrap();
        let mut got = Vec::new();
        while got.len() < chunk.len() {
            let piece = within("forwarded chunk", response.chunk())
                .await
                .unwrap()
                .expect("body ended early");
            got.extend_from_slice(&piece);
        }
        assert_eq!(got, chunk, "chunk altered in transit");
        received.extend_from_slice(&got);
    }
    drop(body_sender);
    assert!(
        within("body end", response.chunk())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        received.len(),
        chunks.iter().map(|c| c.len()).sum::<usize>()
    );
}

#[tokio::test]
async fn upstream_abort_mid_body_is_not_reported_as_complete() {
    let (body_tx, mut body_rx) = mpsc::unbounded_channel::<Sender<Bytes, BoxError>>();
    let upstream = spawn_upstream(move |_req| {
        let body_tx = body_tx.clone();
        async move {
            let (sender, body) = channel();
            body_tx.send(sender).unwrap();
            Response::new(body)
        }
    })
    .await;
    let floor = spawn_floor(&format!("http://{upstream}"), None);

    let request = tokio::spawn(
        inbound_client(Vec::new())
            .get(format!("http://{}/v1/stream", floor.addr))
            .send(),
    );
    let mut body_sender = within("upstream request", body_rx.recv()).await.unwrap();
    let mut response = within("response head", request).await.unwrap().unwrap();
    within(
        "upstream write",
        body_sender.send_data(Bytes::from_static(b"data: partial\n\n")),
    )
    .await
    .unwrap();
    let first = within("first chunk", response.chunk())
        .await
        .unwrap()
        .expect("first chunk missing");
    assert_eq!(first, "data: partial\n\n");

    body_sender.abort("upstream gave up".into());
    let end = within("body after abort", response.chunk()).await;
    assert!(
        end.is_err(),
        "truncated stream ended cleanly: {end:?} (a load generator would count it as complete)"
    );
}

#[tokio::test]
async fn content_length_response_keeps_its_framing() {
    let upstream = spawn_upstream(|_req| async { Response::new(full("0123456789")) }).await;
    let floor = spawn_floor(&format!("http://{upstream}"), None);

    let response = within(
        "response",
        inbound_client(Vec::new())
            .get(format!("http://{}/v1/models", floor.addr))
            .send(),
    )
    .await
    .unwrap();
    assert_eq!(response.headers()["content-length"], "10");
    assert!(!response.headers().contains_key("transfer-encoding"));
    assert_eq!(response.text().await.unwrap(), "0123456789");
}

#[tokio::test]
async fn request_body_streams_and_hop_by_hop_headers_are_stripped() {
    let (seen_tx, mut seen_rx) = mpsc::unbounded_channel::<Seen>();
    let (frame_tx, mut frame_rx) = mpsc::unbounded_channel::<Bytes>();
    let upstream = spawn_upstream(move |req: Request<Incoming>| {
        let seen_tx = seen_tx.clone();
        let frame_tx = frame_tx.clone();
        async move {
            let (parts, mut body) = req.into_parts();
            seen_tx.send(Seen::of(&parts)).unwrap();
            let mut total = 0;
            while let Some(frame) = body.frame().await {
                if let Ok(data) = frame.unwrap().into_data() {
                    total += data.len();
                    frame_tx.send(data).unwrap();
                }
            }
            Response::new(full(format!("received {total}")))
        }
    })
    .await;
    let floor = spawn_floor(&format!("http://{upstream}"), None);

    let (mut body_sender, body) = channel();
    let request = Request::post("/v1/chat/completions?stream=1")
        .header("host", "floor.example")
        .header("connection", "x-hop")
        .header("x-hop", "1")
        .header("keep-alive", "timeout=5")
        .header("te", "gzip")
        .header("proxy-authorization", "Basic Zm9vOmJhcg==")
        .header("authorization", "Bearer sk-test")
        .header("x-multi", "a")
        .header("x-multi", "b")
        .body(body)
        .unwrap();
    let mut sender = h1_connect(floor.addr).await;
    let response = tokio::spawn(sender.send_request(request));

    let seen = within("upstream request head", seen_rx.recv())
        .await
        .unwrap();
    assert_eq!(seen.method, Method::POST);
    assert_eq!(seen.uri, "/v1/chat/completions?stream=1");
    assert_eq!(seen.headers["host"], upstream.to_string().as_str());
    assert_eq!(seen.headers["authorization"], "Bearer sk-test");
    let multi: Vec<_> = seen.headers.get_all("x-multi").iter().collect();
    assert_eq!(multi, ["a", "b"]);
    for hop in [
        "connection",
        "x-hop",
        "keep-alive",
        "te",
        "proxy-authorization",
    ] {
        assert!(!seen.headers.contains_key(hop), "{hop} was forwarded");
    }

    // The upstream must see each part before the client sends the next one.
    let parts: [&[u8]; 2] = [b"{\"model\":\"m\",", b"\"stream\":true}"];
    for part in parts {
        within(
            "client write",
            body_sender.send_data(Bytes::from_static(part)),
        )
        .await
        .unwrap();
        let mut got = Vec::new();
        while got.len() < part.len() {
            got.extend_from_slice(
                &within("forwarded request frame", frame_rx.recv())
                    .await
                    .unwrap(),
            );
        }
        assert_eq!(got, part);
    }
    drop(body_sender);

    let response = within("response", response).await.unwrap().unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let expected = format!("received {}", parts.iter().map(|p| p.len()).sum::<usize>());
    assert_eq!(body, expected.as_bytes());
}

#[tokio::test]
async fn rewritten_request_targets_are_refused_not_forwarded() {
    let (seen_tx, mut seen_rx) = mpsc::unbounded_channel::<Seen>();
    let upstream = spawn_upstream(move |req: Request<Incoming>| {
        let seen_tx = seen_tx.clone();
        async move {
            let (parts, _) = req.into_parts();
            seen_tx.send(Seen::of(&parts)).unwrap();
            Response::new(full("ok"))
        }
    })
    .await;
    let floor = spawn_floor(&format!("http://{upstream}/prefix"), None);
    let mut sender = h1_connect(floor.addr).await;

    for target in [
        "/v1/../../admin",
        "/a/%2e%2e/%2e%2e/secret",
        "/a/./b",
        "/v1/models?q=it's",
    ] {
        let request = Request::get(target)
            .header("host", "floor.example")
            .body(empty())
            .unwrap();
        let response = within("refusal", sender.send_request(request))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{target}");
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body, BAD_TARGET_BODY.as_bytes());
    }

    // An unchanged target on the same connection is forwarded, and it is the
    // first request the upstream sees: none of the refused ones got through.
    let request = Request::get("/v1/models?q=a%27b&x=%2F")
        .header("host", "floor.example")
        .body(empty())
        .unwrap();
    let response = within("forwarded", sender.send_request(request))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let seen = within("upstream request", seen_rx.recv()).await.unwrap();
    assert_eq!(seen.uri, "/prefix/v1/models?q=a%27b&x=%2F");
    assert!(seen_rx.try_recv().is_err());
}

#[tokio::test]
async fn trailers_are_forwarded_when_the_client_accepts_them() {
    let (seen_tx, mut seen_rx) = mpsc::unbounded_channel::<Seen>();
    let upstream = spawn_upstream(move |req: Request<Incoming>| {
        let seen_tx = seen_tx.clone();
        async move {
            seen_tx.send(Seen::of(&req.into_parts().0)).unwrap();
            let (mut sender, body) = channel();
            tokio::spawn(async move {
                sender
                    .send_data(Bytes::from_static(b"payload"))
                    .await
                    .unwrap();
                let mut trailers = HeaderMap::new();
                trailers.insert("x-checksum", HeaderValue::from_static("abc123"));
                sender.send_trailers(trailers).await.unwrap();
            });
            Response::builder()
                .header("trailer", "x-checksum")
                .body(body)
                .unwrap()
        }
    })
    .await;
    let floor = spawn_floor(&format!("http://{upstream}"), None);

    let request = Request::get("/v1/stream")
        .header("host", "floor.example")
        .header("te", "trailers")
        .body(empty())
        .unwrap();
    let response = within(
        "response",
        h1_connect(floor.addr).await.send_request(request),
    )
    .await
    .unwrap();
    let seen = within("upstream request", seen_rx.recv()).await.unwrap();
    assert_eq!(seen.headers["te"], "trailers");
    assert_eq!(response.headers()["trailer"], "x-checksum");

    let collected = within("body", response.into_body().collect())
        .await
        .unwrap();
    let trailers = collected.trailers().cloned().expect("trailers dropped");
    assert_eq!(trailers["x-checksum"], "abc123");
    assert_eq!(collected.to_bytes(), "payload");
}

#[tokio::test]
async fn upstream_down_maps_to_502() {
    // Bind and release a port so nothing listens on it.
    let dead = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap();
    let floor = spawn_floor(&format!("http://{dead}"), None);

    let response = within(
        "502 response",
        inbound_client(Vec::new())
            .get(format!("http://{}/v1/models", floor.addr))
            .send(),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    assert_eq!(response.text().await.unwrap(), BAD_GATEWAY_BODY);
}

/// Temporary directory holding a generated CA and server certificate.
struct CertDir(PathBuf);

impl CertDir {
    fn generate() -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir =
            std::env::temp_dir().join(format!("brisk-floor-test-{}-{nanos}", std::process::id()));
        CertBundle::generate(&["localhost".to_owned(), "127.0.0.1".to_owned()])
            .unwrap()
            .write_to(&dir)
            .unwrap();
        Self(dir)
    }

    fn acceptor(&self) -> tokio_rustls::TlsAcceptor {
        tls::acceptor(
            &self.0.join(SERVER_CERT_FILE),
            &self.0.join(SERVER_KEY_FILE),
        )
        .unwrap()
    }
}

impl Drop for CertDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

async fn hello_upstream() -> SocketAddr {
    spawn_upstream(|req: Request<Incoming>| async move {
        Response::new(full(format!("hello {}", req.uri().path())))
    })
    .await
}

/// Connects to a TLS floor offering only ALPN `h2` and completes the HTTP/2
/// handshake.
async fn h2_connect(
    certs: &CertDir,
    addr: SocketAddr,
) -> hyper::client::conn::http2::SendRequest<TestBody> {
    let base = bench_tls::client_config(&certs.0.join(CA_CERT_FILE)).unwrap();
    let mut client_config = Arc::unwrap_or_clone(base);
    client_config.alpn_protocols = vec![b"h2".to_vec()];
    let tcp = TcpStream::connect(addr).await.unwrap();
    let stream = within(
        "TLS handshake",
        TlsConnector::from(Arc::new(client_config))
            .connect(bench_tls::server_name("localhost").unwrap(), tcp),
    )
    .await
    .unwrap();
    assert_eq!(stream.get_ref().1.alpn_protocol(), Some(&b"h2"[..]));

    let (sender, conn) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(stream))
            .await
            .unwrap();
    tokio::spawn(conn);
    sender
}

#[tokio::test]
async fn tls_inbound_serves_http1() {
    let certs = CertDir::generate();
    let upstream = hello_upstream().await;
    let floor = spawn_floor(&format!("http://{upstream}"), Some(certs.acceptor()));

    let ca = tls::load_ca_certs(&certs.0.join(CA_CERT_FILE)).unwrap();
    let response = within(
        "TLS response",
        inbound_client(ca)
            .get(format!("https://localhost:{}/over-tls", floor.addr.port()))
            .send(),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.text().await.unwrap(), "hello /over-tls");
}

#[tokio::test]
async fn tls_inbound_negotiates_h2() {
    let certs = CertDir::generate();
    let upstream = hello_upstream().await;
    let floor = spawn_floor(&format!("http://{upstream}"), Some(certs.acceptor()));

    let mut sender = h2_connect(&certs, floor.addr).await;
    let request = Request::get("https://localhost/over-h2")
        .body(empty())
        .unwrap();
    let response = within("h2 response", sender.send_request(request))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body, "hello /over-h2");
}

#[tokio::test]
async fn h2_inbound_streams_both_bodies_and_joins_cookies() {
    let (seen_tx, mut seen_rx) = mpsc::unbounded_channel::<Seen>();
    let (frame_tx, mut frame_rx) = mpsc::unbounded_channel::<Bytes>();
    let (body_tx, mut body_rx) = mpsc::unbounded_channel::<Sender<Bytes, BoxError>>();
    let upstream = spawn_upstream(move |req: Request<Incoming>| {
        let (seen_tx, frame_tx, body_tx) = (seen_tx.clone(), frame_tx.clone(), body_tx.clone());
        async move {
            let (parts, mut body) = req.into_parts();
            seen_tx.send(Seen::of(&parts)).unwrap();
            // The response head goes out at once while the request body keeps
            // streaming, as with a chat completion that starts answering early.
            tokio::spawn(async move {
                while let Some(frame) = body.frame().await {
                    if let Ok(data) = frame.unwrap().into_data() {
                        frame_tx.send(data).unwrap();
                    }
                }
            });
            let (sender, response_body) = channel();
            body_tx.send(sender).unwrap();
            Response::builder()
                .header("content-type", "text/event-stream")
                .body(response_body)
                .unwrap()
        }
    })
    .await;
    let certs = CertDir::generate();
    let floor = spawn_floor(&format!("http://{upstream}"), Some(certs.acceptor()));

    let (mut request_sender, request_body) = channel();
    let request = Request::post("https://localhost/v1/chat/completions")
        .header("cookie", "a=1")
        .header("cookie", "b=2")
        .header("x-multi", "a")
        .header("x-multi", "b")
        .body(request_body)
        .unwrap();
    let mut sender = h2_connect(&certs, floor.addr).await;
    let response = tokio::spawn(sender.send_request(request));

    let seen = within("upstream request head", seen_rx.recv())
        .await
        .unwrap();
    assert_eq!(seen.method, Method::POST);
    assert_eq!(seen.uri, "/v1/chat/completions");
    let cookies: Vec<_> = seen.headers.get_all("cookie").iter().collect();
    assert_eq!(cookies, ["a=1; b=2"]);
    let multi: Vec<_> = seen.headers.get_all("x-multi").iter().collect();
    assert_eq!(multi, ["a", "b"]);

    let mut response_sender = within("upstream body sender", body_rx.recv())
        .await
        .unwrap();
    let mut response = within("response head", response)
        .await
        .unwrap()
        .unwrap()
        .into_body();

    // Request and response parts alternate: each side must see the other's
    // part before it sends its own next one.
    let exchanges: [(&[u8], &[u8]); 2] = [
        (b"{\"model\":\"m\",", b"data: {\"k\":1}\n\n"),
        (b"\"stream\":true}", b"data: [DONE]\n\n"),
    ];
    for (request_part, response_part) in exchanges {
        within(
            "client write",
            request_sender.send_data(Bytes::from_static(request_part)),
        )
        .await
        .unwrap();
        let mut got = Vec::new();
        while got.len() < request_part.len() {
            got.extend_from_slice(
                &within("forwarded request frame", frame_rx.recv())
                    .await
                    .unwrap(),
            );
        }
        assert_eq!(got, request_part);

        within(
            "upstream write",
            response_sender.send_data(Bytes::from_static(response_part)),
        )
        .await
        .unwrap();
        let mut got = Vec::new();
        while got.len() < response_part.len() {
            let frame = within("forwarded response frame", response.frame())
                .await
                .expect("response ended early")
                .unwrap();
            if let Ok(data) = frame.into_data() {
                got.extend_from_slice(&data);
            }
        }
        assert_eq!(got, response_part);
    }
    drop(request_sender);
    drop(response_sender);
    let rest = within("response end", response.collect())
        .await
        .unwrap()
        .to_bytes();
    assert!(rest.is_empty(), "unexpected trailing data: {rest:?}");
}

// ---------------------------------------------------------------- floor-B

/// Connection counts of an upstream started by [`spawn_counting_upstream`].
#[derive(Debug, Default)]
struct UpstreamStats {
    accepts: AtomicUsize,
    closed: AtomicUsize,
}

/// Like [`spawn_upstream`], optionally over TLS, and counting accepted and
/// finished connections; every finished connection is also announced on the
/// returned channel.
async fn spawn_counting_upstream<F, Fut>(
    handler: F,
    tls: Option<tokio_rustls::TlsAcceptor>,
) -> (SocketAddr, Arc<UpstreamStats>, mpsc::UnboundedReceiver<()>)
where
    F: Fn(Request<Incoming>) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = Response<TestBody>> + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let stats = Arc::new(UpstreamStats::default());
    let (closed_tx, closed_rx) = mpsc::unbounded_channel();
    let counted = Arc::clone(&stats);
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            counted.accepts.fetch_add(1, Ordering::SeqCst);
            let handler = handler.clone();
            let (counted, closed_tx, tls) = (Arc::clone(&counted), closed_tx.clone(), tls.clone());
            tokio::spawn(async move {
                let service = service_fn(move |req| {
                    let response = handler(req);
                    async move { Ok::<_, Infallible>(response.await) }
                });
                let builder = hyper::server::conn::http1::Builder::new();
                // Errors here are client disconnects, which some tests cause
                // on purpose.
                match tls {
                    Some(acceptor) => {
                        if let Ok(stream) = acceptor.accept(stream).await {
                            let _ = builder
                                .serve_connection(TokioIo::new(stream), service)
                                .await;
                        }
                    }
                    None => {
                        let _ = builder
                            .serve_connection(TokioIo::new(stream), service)
                            .await;
                    }
                }
                counted.closed.fetch_add(1, Ordering::SeqCst);
                let _ = closed_tx.send(());
            });
        }
    });
    (addr, stats, closed_rx)
}

/// A floor-B instance on an ephemeral port; dropping it stops the server.
struct FloorB {
    addr: SocketAddr,
    forwarder: SameTaskForwarder,
    _shutdown: oneshot::Sender<()>,
}

/// Starts floor-B with a plaintext inbound side; `certs` holds the CA that
/// an `https` upstream is checked against.
fn spawn_floor_b(upstream: &str, certs: Option<&CertDir>) -> FloorB {
    spawn_floor_b_with_inbound(upstream, certs, None)
}

fn spawn_floor_b_with_inbound(
    upstream: &str,
    certs: Option<&CertDir>,
    inbound_tls: Option<tokio_rustls::TlsAcceptor>,
) -> FloorB {
    let config = ServerConfig::default();
    let tls = certs.map(|certs| bench_tls::client_config(&certs.0.join(CA_CERT_FILE)).unwrap());
    let forwarder = SameTaskForwarder::new(upstream, tls).unwrap();
    let listener = server::bind("127.0.0.1:0".parse().unwrap(), &config).unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown, stopped) = oneshot::channel::<()>();
    tokio::spawn(same_task::run(
        listener,
        inbound_tls,
        config,
        forwarder.clone(),
        async {
            let _ = stopped.await;
        },
    ));
    FloorB {
        addr,
        forwarder,
        _shutdown: shutdown,
    }
}

/// Every byte value, so a body altered in transit cannot go unnoticed.
fn binary_body() -> Vec<u8> {
    (0..=255u8).cycle().take(3000).collect()
}

/// An upstream handler for the non-streaming and streaming cases of the
/// floor-B tests:
///
/// - `/v1/models`: a fixed `Content-Length` body;
/// - `/v1/empty`: an empty body, which hyper never polls;
/// - `/v1/echo`: the request body, read in full, sent back with a
///   `Content-Length`;
/// - anything else: the request body, read in full, then a chunked event
///   stream of `data: <request body>` and `data: [DONE]`.
async fn echo_handler(req: Request<Incoming>) -> Response<TestBody> {
    let path = req.uri().path().to_owned();
    match path.as_str() {
        "/v1/models" => Response::new(full("0123456789")),
        "/v1/empty" => Response::new(empty()),
        _ => {
            let received = req.into_body().collect().await.unwrap().to_bytes();
            if path == "/v1/echo" {
                return Response::new(full(received));
            }
            let (mut sender, body) = channel();
            tokio::spawn(async move {
                let mut event = b"data: ".to_vec();
                event.extend_from_slice(&received);
                event.extend_from_slice(b"\n\n");
                for part in [Bytes::from(event), Bytes::from_static(b"data: [DONE]\n\n")] {
                    sender.send_data(part).await.unwrap();
                }
            });
            Response::builder()
                .header("content-type", "text/event-stream")
                .body(body)
                .unwrap()
        }
    }
}

/// Sends one round of requests covering [`echo_handler`]'s cases through
/// `floor` and checks every response and the request bodies the upstream
/// received.
async fn exchange_echo_round(client: &reqwest::Client, floor: &str) {
    let response = within(
        "content-length response",
        client.get(format!("{floor}/v1/models")).send(),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["content-length"], "10");
    assert_eq!(response.text().await.unwrap(), "0123456789");

    let response = within(
        "empty response",
        client.get(format!("{floor}/v1/empty")).send(),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["content-length"], "0");
    assert!(response.bytes().await.unwrap().is_empty());

    let sent = binary_body();
    let response = within(
        "echoed body",
        client
            .post(format!("{floor}/v1/echo"))
            .body(sent.clone())
            .send(),
    )
    .await
    .unwrap();
    assert_eq!(
        response.headers()["content-length"],
        sent.len().to_string().as_str()
    );
    assert_eq!(
        response.bytes().await.unwrap(),
        sent,
        "request body altered"
    );

    let request = "{\"model\":\"grok-4.6(xhigh)\",\"stream\":true}";
    let response = within(
        "streamed response",
        client
            .post(format!("{floor}/v1/chat/completions"))
            .body(request)
            .send(),
    )
    .await
    .unwrap();
    assert_eq!(response.headers()["content-type"], "text/event-stream");
    assert!(!response.headers().contains_key("content-length"));
    assert_eq!(
        response.text().await.unwrap(),
        format!("data: {request}\n\ndata: [DONE]\n\n")
    );
}

#[tokio::test]
async fn floor_b_forwards_request_and_response_unmodified() {
    let (seen_tx, mut seen_rx) = mpsc::unbounded_channel::<Seen>();
    let (frame_tx, mut frame_rx) = mpsc::unbounded_channel::<Bytes>();
    let (body_tx, mut body_rx) = mpsc::unbounded_channel::<Sender<Bytes, BoxError>>();
    let (upstream, stats, _) = spawn_counting_upstream(
        move |req: Request<Incoming>| {
            let (seen_tx, frame_tx, body_tx) = (seen_tx.clone(), frame_tx.clone(), body_tx.clone());
            async move {
                let (parts, mut body) = req.into_parts();
                seen_tx.send(Seen::of(&parts)).unwrap();
                tokio::spawn(async move {
                    while let Some(frame) = body.frame().await {
                        if let Ok(data) = frame.unwrap().into_data() {
                            frame_tx.send(data).unwrap();
                        }
                    }
                });
                let (sender, response_body) = channel();
                body_tx.send(sender).unwrap();
                Response::builder()
                    .header("content-type", "text/event-stream")
                    .header("x-upstream", "yes")
                    .header("connection", "x-drop")
                    .header("x-drop", "1")
                    .header("keep-alive", "timeout=5")
                    .body(response_body)
                    .unwrap()
            }
        },
        None,
    )
    .await;
    let floor = spawn_floor_b(&format!("http://{upstream}/prefix"), None);

    let (mut request_sender, request_body) = channel();
    let request = Request::post("/v1/chat/completions?stream=1")
        .header("host", "floor.example")
        .header("connection", "x-hop")
        .header("x-hop", "1")
        .header("keep-alive", "timeout=5")
        .header("te", "trailers")
        .header("proxy-authorization", "Basic Zm9vOmJhcg==")
        .header("authorization", "Bearer sk-test")
        .header("x-multi", "a")
        .header("x-multi", "b")
        .body(request_body)
        .unwrap();
    let mut sender = h1_connect(floor.addr).await;
    let response = tokio::spawn(sender.send_request(request));

    let seen = within("upstream request head", seen_rx.recv())
        .await
        .unwrap();
    assert_eq!(seen.method, Method::POST);
    assert_eq!(seen.uri, "/prefix/v1/chat/completions?stream=1");
    assert_eq!(seen.headers["host"], upstream.to_string().as_str());
    // Added when absent, as reqwest does for floor-A.
    assert_eq!(seen.headers["accept"], "*/*");
    assert_eq!(seen.headers["te"], "trailers");
    assert_eq!(seen.headers["authorization"], "Bearer sk-test");
    let multi: Vec<_> = seen.headers.get_all("x-multi").iter().collect();
    assert_eq!(multi, ["a", "b"]);
    for hop in ["connection", "x-hop", "keep-alive", "proxy-authorization"] {
        assert!(!seen.headers.contains_key(hop), "{hop} was forwarded");
    }

    let mut response_sender = within("upstream body sender", body_rx.recv())
        .await
        .unwrap();
    let response = within("response head", response).await.unwrap().unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["content-type"], "text/event-stream");
    assert_eq!(response.headers()["x-upstream"], "yes");
    for hop in ["x-drop", "keep-alive"] {
        assert!(!response.headers().contains_key(hop), "{hop} was forwarded");
    }
    let mut response = response.into_body();

    // Both directions stream: each side sees the other's part before it
    // sends its own next one.
    let binary: Vec<u8> = (0..=255u8).collect();
    let exchanges: [(&[u8], &[u8]); 2] = [
        (b"{\"model\":\"grok-4.6(xhigh)\",", &binary),
        (b"\"stream\":true}", b"data: [DONE]\n\n"),
    ];
    for (request_part, response_part) in exchanges {
        within(
            "client write",
            request_sender.send_data(Bytes::copy_from_slice(request_part)),
        )
        .await
        .unwrap();
        let mut got = Vec::new();
        while got.len() < request_part.len() {
            got.extend_from_slice(
                &within("forwarded request frame", frame_rx.recv())
                    .await
                    .unwrap(),
            );
        }
        assert_eq!(got, request_part);

        within(
            "upstream write",
            response_sender.send_data(Bytes::copy_from_slice(response_part)),
        )
        .await
        .unwrap();
        let mut got = Vec::new();
        while got.len() < response_part.len() {
            let frame = within("forwarded response frame", response.frame())
                .await
                .expect("response ended early")
                .unwrap();
            if let Ok(data) = frame.into_data() {
                got.extend_from_slice(&data);
            }
        }
        assert_eq!(got, response_part, "chunk altered in transit");
    }
    drop(request_sender);
    drop(response_sender);
    let rest = within("response end", response.collect())
        .await
        .unwrap()
        .to_bytes();
    assert!(rest.is_empty(), "unexpected trailing data: {rest:?}");
    assert_eq!(stats.accepts.load(Ordering::SeqCst), 1);
    // Pooled before the end of the body reached the client.
    assert_eq!(floor.forwarder.idle_connections(), 1);
}

/// An upstream that records the bytes of every request it receives (head and
/// `Content-Length` body) and answers each with `200 ok`, keeping the
/// connection open.
async fn spawn_recording_upstream() -> (SocketAddr, mpsc::UnboundedReceiver<Vec<u8>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (recorded_tx, recorded_rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let recorded_tx = recorded_tx.clone();
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut chunk = [0u8; 4096];
                loop {
                    let head_len = loop {
                        if let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                            break end + 4;
                        }
                        match stream.read(&mut chunk).await {
                            // The peer went away between requests.
                            Ok(0) | Err(_) => return,
                            Ok(n) => buf.extend_from_slice(&chunk[..n]),
                        }
                    };
                    let head = String::from_utf8(buf[..head_len].to_vec()).unwrap();
                    // hyper writes header names in lower case.
                    let body_len = head
                        .lines()
                        .find_map(|line| line.strip_prefix("content-length: "))
                        .map_or(0, |len| len.parse::<usize>().unwrap());
                    while buf.len() < head_len + body_len {
                        let n = stream.read(&mut chunk).await.unwrap();
                        assert!(n > 0, "request body cut short");
                        buf.extend_from_slice(&chunk[..n]);
                    }
                    let rest = buf.split_off(head_len + body_len);
                    recorded_tx.send(std::mem::replace(&mut buf, rest)).unwrap();
                    stream
                        .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok")
                        .await
                        .unwrap();
                }
            });
        }
    });
    (addr, recorded_rx)
}

#[tokio::test]
async fn floor_b_sends_the_same_request_bytes_as_floor_a() {
    let (upstream, mut recorded) = spawn_recording_upstream().await;
    let base = format!("http://{upstream}/prefix");
    let floor_a = spawn_floor(&base, None);
    let floor_b = spawn_floor_b(&base, None);

    let requests = || {
        [
            Request::post("/v1/chat/completions?stream=1")
                .header("host", "floor.example")
                .header("connection", "x-hop")
                .header("x-hop", "1")
                .header("keep-alive", "timeout=5")
                .header("te", "trailers")
                .header("proxy-authorization", "Basic Zm9vOmJhcg==")
                .header("authorization", "Bearer sk-test")
                .header("content-type", "application/json")
                .header("x-multi", "a")
                .header("x-multi", "b")
                .body(full("{\"model\":\"grok-4.6(xhigh)\",\"stream\":true}"))
                .unwrap(),
            Request::get("/v1/models?q=a%27b")
                .header("host", "floor.example")
                .header("accept", "application/json")
                .body(empty())
                .unwrap(),
        ]
    };
    let mut seen = Vec::new();
    for addr in [floor_a.addr, floor_b.addr] {
        let mut sender = h1_connect(addr).await;
        let mut bytes = Vec::new();
        for request in requests() {
            let response = within("response", sender.send_request(request))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let body = response.into_body().collect().await.unwrap().to_bytes();
            assert_eq!(body, "ok");
            bytes.push(within("recorded request", recorded.recv()).await.unwrap());
        }
        seen.push(bytes);
    }
    let (a, b) = (&seen[0], &seen[1]);
    for (a, b) in a.iter().zip(b) {
        assert_eq!(
            String::from_utf8_lossy(b),
            String::from_utf8_lossy(a),
            "floor-B (left) and floor-A (right) sent different requests"
        );
    }
    assert!(
        String::from_utf8_lossy(&a[0])
            .starts_with("POST /prefix/v1/chat/completions?stream=1 HTTP/1.1\r\n"),
        "{:?}",
        String::from_utf8_lossy(&a[0])
    );
}

#[tokio::test]
async fn floor_b_reuses_one_upstream_connection() {
    let (upstream, stats, _) = spawn_counting_upstream(echo_handler, None).await;
    let floor = spawn_floor_b(&format!("http://{upstream}"), None);
    let client = inbound_client(Vec::new());

    // Three rounds of a known length (after which hyper stops polling the
    // body without asking for its end), an empty body (which hyper never
    // polls), an echoed request body and a chunked stream, all on one
    // upstream connection.
    for _ in 0..3 {
        exchange_echo_round(&client, &format!("http://{}", floor.addr)).await;
    }
    assert_eq!(
        stats.accepts.load(Ordering::SeqCst),
        1,
        "a request opened a new upstream connection"
    );
    assert_eq!(stats.closed.load(Ordering::SeqCst), 0);
    assert_eq!(floor.forwarder.idle_connections(), 1);
}

#[tokio::test]
async fn floor_b_opens_a_connection_per_concurrent_request_and_pools_all() {
    let (release_tx, release_rx) = tokio::sync::watch::channel(false);
    let (arrived_tx, mut arrived_rx) = mpsc::unbounded_channel::<()>();
    let (upstream, stats, _) = spawn_counting_upstream(
        move |_req| {
            let (mut release, arrived) = (release_rx.clone(), arrived_tx.clone());
            async move {
                arrived.send(()).unwrap();
                release.wait_for(|go| *go).await.unwrap();
                Response::new(full("ok"))
            }
        },
        None,
    )
    .await;
    let floor = spawn_floor_b(&format!("http://{upstream}"), None);
    let client = inbound_client(Vec::new());

    let requests: Vec<_> = (0..3)
        .map(|_| {
            tokio::spawn(
                client
                    .get(format!("http://{}/v1/models", floor.addr))
                    .send(),
            )
        })
        .collect();
    for _ in 0..3 {
        within("concurrent request", arrived_rx.recv())
            .await
            .unwrap();
    }
    release_tx.send(true).unwrap();
    for request in requests {
        let response = within("response", request).await.unwrap().unwrap();
        assert_eq!(response.text().await.unwrap(), "ok");
    }
    assert_eq!(stats.accepts.load(Ordering::SeqCst), 3);
    assert_eq!(floor.forwarder.idle_connections(), 3);

    for _ in 0..3 {
        let response = within(
            "sequential request",
            client
                .get(format!("http://{}/v1/models", floor.addr))
                .send(),
        )
        .await
        .unwrap();
        assert_eq!(response.text().await.unwrap(), "ok");
    }
    assert_eq!(
        stats.accepts.load(Ordering::SeqCst),
        3,
        "pooled connections were not reused"
    );
    assert_eq!(floor.forwarder.idle_connections(), 3);
}

#[tokio::test]
async fn floor_b_closes_the_upstream_connection_when_the_response_is_dropped() {
    let (body_tx, mut body_rx) = mpsc::unbounded_channel::<Sender<Bytes, BoxError>>();
    let (upstream, stats, mut closed_rx) = spawn_counting_upstream(
        move |req: Request<Incoming>| {
            let body_tx = body_tx.clone();
            async move {
                if req.uri().path() == "/v1/models" {
                    return Response::new(full("ok"));
                }
                let (sender, body) = channel();
                body_tx.send(sender).unwrap();
                Response::builder()
                    .header("content-type", "text/event-stream")
                    .body(body)
                    .unwrap()
            }
        },
        None,
    )
    .await;
    let floor = spawn_floor_b(&format!("http://{upstream}"), None);

    let mut sender = h1_connect(floor.addr).await;
    let request = Request::get("/v1/stream")
        .header("host", "floor.example")
        .body(empty())
        .unwrap();
    let response = tokio::spawn(sender.send_request(request));
    let mut body_sender = within("upstream request", body_rx.recv()).await.unwrap();
    let mut response = within("response head", response)
        .await
        .unwrap()
        .unwrap()
        .into_body();
    within(
        "upstream write",
        body_sender.send_data(Bytes::from_static(b"data: partial\n\n")),
    )
    .await
    .unwrap();
    let first = within("first chunk", response.frame())
        .await
        .expect("first chunk missing")
        .unwrap();
    assert_eq!(first.into_data().unwrap(), "data: partial\n\n");

    // The client goes away mid-stream: the inbound connection closes, floor
    // drops the response body, and with it the upstream connection.
    drop(response);
    drop(sender);
    within("upstream connection close", closed_rx.recv())
        .await
        .unwrap();
    assert_eq!(stats.closed.load(Ordering::SeqCst), 1);
    assert_eq!(floor.forwarder.idle_connections(), 0);

    // Nothing of it was pooled: the next request needs a new connection.
    let response = within(
        "next request",
        inbound_client(Vec::new())
            .get(format!("http://{}/v1/models", floor.addr))
            .send(),
    )
    .await
    .unwrap();
    assert_eq!(response.text().await.unwrap(), "ok");
    assert_eq!(stats.accepts.load(Ordering::SeqCst), 2);
    assert_eq!(floor.forwarder.idle_connections(), 1);
}

#[tokio::test]
async fn floor_b_closes_the_upstream_connection_when_the_client_leaves_before_the_head() {
    let (arrived_tx, mut arrived_rx) = mpsc::unbounded_channel::<()>();
    let (upstream, stats, mut closed_rx) = spawn_counting_upstream(
        move |_req| {
            let arrived = arrived_tx.clone();
            async move {
                arrived.send(()).unwrap();
                // Never answers; the client gives up first.
                std::future::pending::<Response<TestBody>>().await
            }
        },
        None,
    )
    .await;
    let floor = spawn_floor_b(&format!("http://{upstream}"), None);

    let mut sender = h1_connect(floor.addr).await;
    let request = Request::get("/v1/slow")
        .header("host", "floor.example")
        .body(empty())
        .unwrap();
    let pending = tokio::spawn(sender.send_request(request));
    within("upstream request", arrived_rx.recv()).await.unwrap();

    // Dropping the in-flight request makes hyper close the client
    // connection; floor then drops the handler that was waiting for the
    // response head, and the upstream connection with it.
    pending.abort();
    drop(sender);
    within("upstream connection close", closed_rx.recv())
        .await
        .unwrap();
    assert_eq!(stats.accepts.load(Ordering::SeqCst), 1);
    assert_eq!(floor.forwarder.idle_connections(), 0);
}

#[tokio::test]
async fn floor_b_reaches_an_https_upstream_through_the_given_ca() {
    let certs = CertDir::generate();
    let acceptor = tokio_rustls::TlsAcceptor::from(
        bench_tls::server_config(
            &certs.0.join(SERVER_CERT_FILE),
            &certs.0.join(SERVER_KEY_FILE),
        )
        .unwrap(),
    );
    let (upstream, stats, _) = spawn_counting_upstream(echo_handler, Some(acceptor)).await;
    // `localhost` is the name the certificate is checked against.
    let floor = spawn_floor_b(
        &format!("https://localhost:{}", upstream.port()),
        Some(&certs),
    );
    let client = inbound_client(Vec::new());

    for _ in 0..2 {
        exchange_echo_round(&client, &format!("http://{}", floor.addr)).await;
    }
    assert_eq!(stats.accepts.load(Ordering::SeqCst), 1);
    assert_eq!(floor.forwarder.idle_connections(), 1);
}

#[tokio::test]
async fn floor_b_streams_incrementally_from_an_https_upstream() {
    let certs = CertDir::generate();
    let acceptor = tokio_rustls::TlsAcceptor::from(
        bench_tls::server_config(
            &certs.0.join(SERVER_CERT_FILE),
            &certs.0.join(SERVER_KEY_FILE),
        )
        .unwrap(),
    );
    let (body_tx, mut body_rx) = mpsc::unbounded_channel::<Sender<Bytes, BoxError>>();
    let (upstream, stats, _) = spawn_counting_upstream(
        move |_req| {
            let body_tx = body_tx.clone();
            async move {
                let (sender, body) = channel();
                body_tx.send(sender).unwrap();
                Response::builder()
                    .header("content-type", "text/event-stream")
                    .body(body)
                    .unwrap()
            }
        },
        Some(acceptor),
    )
    .await;
    let floor = spawn_floor_b(
        &format!("https://localhost:{}", upstream.port()),
        Some(&certs),
    );
    let client = inbound_client(Vec::new());

    for _ in 0..2 {
        let request = tokio::spawn(
            client
                .post(format!("http://{}/v1/chat/completions", floor.addr))
                .body("{\"stream\":true}")
                .send(),
        );
        let mut body_sender = within("upstream request", body_rx.recv()).await.unwrap();
        let mut response = within("response head", request).await.unwrap().unwrap();
        assert_eq!(response.headers()["content-type"], "text/event-stream");

        // Each chunk must reach the client while the upstream still holds
        // the next one back.
        let binary: Vec<u8> = (0..=255u8).collect();
        let chunks: [&[u8]; 3] = [b"data: {\"k\":1}\n\n", &binary, b"data: [DONE]\n\n"];
        for chunk in chunks {
            within(
                "upstream write",
                body_sender.send_data(Bytes::copy_from_slice(chunk)),
            )
            .await
            .unwrap();
            let mut got = Vec::new();
            while got.len() < chunk.len() {
                let piece = within("forwarded chunk", response.chunk())
                    .await
                    .unwrap()
                    .expect("body ended early");
                got.extend_from_slice(&piece);
            }
            assert_eq!(got, chunk, "chunk altered in transit");
        }
        drop(body_sender);
        assert!(
            within("body end", response.chunk())
                .await
                .unwrap()
                .is_none()
        );
    }
    assert_eq!(stats.accepts.load(Ordering::SeqCst), 1);
    assert_eq!(floor.forwarder.idle_connections(), 1);
}

#[tokio::test]
async fn floor_b_refuses_rewritten_targets_and_maps_upstream_down_to_502() {
    // Bind and release a port so nothing listens on it.
    let dead = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap();
    let floor = spawn_floor_b(&format!("http://{dead}"), None);
    let mut sender = h1_connect(floor.addr).await;

    let request = Request::get("/v1/../../admin")
        .header("host", "floor.example")
        .body(empty())
        .unwrap();
    let response = within("refusal", sender.send_request(request))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body, BAD_TARGET_BODY.as_bytes());

    let request = Request::get("/v1/models")
        .header("host", "floor.example")
        .body(empty())
        .unwrap();
    let response = within("502 response", sender.send_request(request))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body, BAD_GATEWAY_BODY.as_bytes());
    assert_eq!(floor.forwarder.idle_connections(), 0);
}

#[tokio::test]
async fn floor_b_replaces_a_pooled_connection_the_upstream_closed() {
    // A raw upstream that answers one request per connection and closes the
    // first connection only when told to, after floor has pooled it.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream = listener.local_addr().unwrap();
    let accepts = Arc::new(AtomicUsize::new(0));
    let (close_tx, close_rx) = oneshot::channel::<()>();
    let (closed_tx, closed_rx) = oneshot::channel::<()>();
    let counted = Arc::clone(&accepts);
    tokio::spawn(async move {
        let mut close = Some((close_rx, closed_tx));
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            counted.fetch_add(1, Ordering::SeqCst);
            let close = close.take();
            tokio::spawn(async move {
                let mut head = Vec::new();
                let mut buf = [0u8; 1024];
                while !head.windows(4).any(|w| w == b"\r\n\r\n") {
                    let n = stream.read(&mut buf).await.unwrap();
                    assert!(n > 0, "floor closed before sending a request");
                    head.extend_from_slice(&buf[..n]);
                }
                stream
                    .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok")
                    .await
                    .unwrap();
                match close {
                    Some((close_rx, closed_tx)) => {
                        close_rx.await.unwrap();
                        drop(stream);
                        closed_tx.send(()).unwrap();
                    }
                    // Kept open until the peer goes away.
                    None => while stream.read(&mut buf).await.is_ok_and(|n| n > 0) {},
                }
            });
        }
    });
    let floor = spawn_floor_b(&format!("http://{upstream}"), None);
    let client = inbound_client(Vec::new());
    let url = format!("http://{}/v1/models", floor.addr);

    let response = within("first request", client.get(&url).send())
        .await
        .unwrap();
    assert_eq!(response.text().await.unwrap(), "ok");
    assert_eq!(floor.forwarder.idle_connections(), 1);

    close_tx.send(()).unwrap();
    within("upstream close", closed_rx).await.unwrap();
    // The close reaches floor only when it next uses the pooled connection;
    // hyper hands the unsent request back, and it goes to a new connection
    // instead of failing.
    let response = within("second request", client.get(&url).send())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.text().await.unwrap(), "ok");
    assert_eq!(accepts.load(Ordering::SeqCst), 2);
    // The dead connection is gone; only its replacement is pooled.
    assert_eq!(floor.forwarder.idle_connections(), 1);
}

#[tokio::test]
async fn floor_b_serves_h2_inbound_and_pools_the_upstream_connection() {
    let (seen_tx, mut seen_rx) = mpsc::unbounded_channel::<Seen>();
    let (upstream, stats, _) = spawn_counting_upstream(
        move |req: Request<Incoming>| {
            let seen_tx = seen_tx.clone();
            async move {
                let (parts, body) = req.into_parts();
                seen_tx.send(Seen::of(&parts)).unwrap();
                echo_handler(Request::from_parts(parts, body)).await
            }
        },
        None,
    )
    .await;
    let certs = CertDir::generate();
    let floor =
        spawn_floor_b_with_inbound(&format!("http://{upstream}"), None, Some(certs.acceptor()));
    let mut sender = h2_connect(&certs, floor.addr).await;

    // hyper serves each HTTP/2 stream from a task of its own, which then
    // drives the upstream connection; the stream's end must still wait for
    // the connection to be pooled.
    for _ in 0..2 {
        let sent = binary_body();
        let request = Request::post("https://localhost/v1/echo")
            .header("cookie", "a=1")
            .header("cookie", "b=2")
            .body(full(sent.clone()))
            .unwrap();
        let response = within("echo response", sender.send_request(request))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = within("echo body", response.into_body().collect())
            .await
            .unwrap()
            .to_bytes();
        assert_eq!(body, sent, "request body altered");
        let seen = within("upstream request", seen_rx.recv()).await.unwrap();
        let cookies: Vec<_> = seen.headers.get_all("cookie").iter().collect();
        assert_eq!(cookies, ["a=1; b=2"]);

        let request = Request::post("https://localhost/v1/chat/completions")
            .body(full("{\"stream\":true}"))
            .unwrap();
        let response = within("streamed response", sender.send_request(request))
            .await
            .unwrap();
        let body = within("streamed body", response.into_body().collect())
            .await
            .unwrap()
            .to_bytes();
        assert_eq!(body, "data: {\"stream\":true}\n\ndata: [DONE]\n\n");
        within("upstream request", seen_rx.recv()).await.unwrap();

        let request = Request::get("https://localhost/v1/empty")
            .body(empty())
            .unwrap();
        let response = within("empty response", sender.send_request(request))
            .await
            .unwrap();
        let body = within("empty body", response.into_body().collect())
            .await
            .unwrap()
            .to_bytes();
        assert!(body.is_empty());
        within("upstream request", seen_rx.recv()).await.unwrap();
    }
    assert_eq!(stats.accepts.load(Ordering::SeqCst), 1);
    assert_eq!(floor.forwarder.idle_connections(), 1);
}

#[tokio::test]
async fn floor_b_forwards_trailers_and_pools_the_connection() {
    let (seen_tx, mut seen_rx) = mpsc::unbounded_channel::<Seen>();
    let (upstream, stats, _) = spawn_counting_upstream(
        move |req: Request<Incoming>| {
            let seen_tx = seen_tx.clone();
            async move {
                seen_tx.send(Seen::of(&req.into_parts().0)).unwrap();
                let (mut sender, body) = channel();
                tokio::spawn(async move {
                    sender
                        .send_data(Bytes::from_static(b"payload"))
                        .await
                        .unwrap();
                    let mut trailers = HeaderMap::new();
                    trailers.insert("x-checksum", HeaderValue::from_static("abc123"));
                    sender.send_trailers(trailers).await.unwrap();
                });
                Response::builder()
                    .header("trailer", "x-checksum")
                    .body(body)
                    .unwrap()
            }
        },
        None,
    )
    .await;
    let floor = spawn_floor_b(&format!("http://{upstream}"), None);
    let mut sender = h1_connect(floor.addr).await;

    // hyper stops polling a body once it has returned the trailers, so they
    // are its end: the connection must be pooled by the time they arrive.
    for _ in 0..3 {
        let request = Request::get("/v1/stream")
            .header("host", "floor.example")
            .header("te", "trailers")
            .body(empty())
            .unwrap();
        let response = within("response", sender.send_request(request))
            .await
            .unwrap();
        let seen = within("upstream request", seen_rx.recv()).await.unwrap();
        assert_eq!(seen.headers["te"], "trailers");
        assert_eq!(response.headers()["trailer"], "x-checksum");

        let collected = within("body", response.into_body().collect())
            .await
            .unwrap();
        let trailers = collected.trailers().cloned().expect("trailers dropped");
        assert_eq!(trailers["x-checksum"], "abc123");
        assert_eq!(collected.to_bytes(), "payload");
        assert_eq!(floor.forwarder.idle_connections(), 1);
    }
    assert_eq!(
        stats.accepts.load(Ordering::SeqCst),
        1,
        "a response with trailers left its upstream connection unpooled"
    );
    assert_eq!(stats.closed.load(Ordering::SeqCst), 0);
}

/// Waits until `forwarder` holds `n` idle connections.
async fn wait_for_idle(forwarder: &SameTaskForwarder, n: usize) {
    within("upstream connection pooled", async {
        while forwarder.idle_connections() != n {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
}

#[tokio::test]
async fn floor_b_delivers_an_early_answer_while_the_request_body_is_uploading() {
    let (received_tx, mut received_rx) = mpsc::unbounded_channel::<Bytes>();
    let (upstream, stats, _) = spawn_counting_upstream(
        move |req: Request<Incoming>| {
            let received_tx = received_tx.clone();
            async move {
                let empty_answer = req.uri().path() == "/v1/empty";
                // Answers at once and reads the request body afterwards,
                // keeping the connection, as a server that refuses an upload
                // early may do.
                tokio::spawn(async move {
                    let received = req.into_body().collect().await.unwrap().to_bytes();
                    received_tx.send(received).unwrap();
                });
                if empty_answer {
                    Response::new(empty())
                } else {
                    Response::builder()
                        .status(StatusCode::PAYLOAD_TOO_LARGE)
                        .body(full("no"))
                        .unwrap()
                }
            }
        },
        None,
    )
    .await;
    let floor = spawn_floor_b(&format!("http://{upstream}"), None);
    let mut sender = h1_connect(floor.addr).await;

    // An empty answer (the end of the response is its head) and one with a
    // known length (the end is its last frame).
    for (path, status, answer) in [
        ("/v1/empty", StatusCode::OK, ""),
        ("/v1/sized", StatusCode::PAYLOAD_TOO_LARGE, "no"),
    ] {
        let (mut body_sender, body) = channel();
        let request = Request::post(path)
            .header("host", "floor.example")
            .body(body)
            .unwrap();
        let response = tokio::spawn(sender.send_request(request));
        within(
            "client write",
            body_sender.send_data(Bytes::from_static(b"first part")),
        )
        .await
        .unwrap();

        // The whole answer reaches the client while it still holds the rest
        // of its request body back.
        let response = within("response head", response).await.unwrap().unwrap();
        assert_eq!(response.status(), status, "{path}");
        let body = within("response body", response.into_body().collect())
            .await
            .unwrap()
            .to_bytes();
        assert_eq!(body, answer, "{path}");
        // Busy with the upload, so not pooled yet.
        assert_eq!(floor.forwarder.idle_connections(), 0, "{path}");

        within(
            "client write",
            body_sender.send_data(Bytes::from_static(b" and the rest")),
        )
        .await
        .unwrap();
        drop(body_sender);
        let received = within("upstream request body", received_rx.recv())
            .await
            .unwrap();
        assert_eq!(received, "first part and the rest", "{path}");
        // Pooled once the upload is complete.
        wait_for_idle(&floor.forwarder, 1).await;
    }
    assert_eq!(
        stats.accepts.load(Ordering::SeqCst),
        1,
        "an early answer left its upstream connection unpooled"
    );
    assert_eq!(stats.closed.load(Ordering::SeqCst), 0);
}
