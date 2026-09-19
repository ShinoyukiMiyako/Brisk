//! End-to-end tests of the blind forwarder: a real inbound client talks to
//! floor, which forwards to an in-test hyper upstream.

use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use brisk_bench_core::transport::certs::{
    CA_CERT_FILE, CertBundle, SERVER_CERT_FILE, SERVER_KEY_FILE,
};
use brisk_bench_core::transport::tls as bench_tls;
use brisk_floor::forward::{self, BAD_GATEWAY_BODY, BAD_TARGET_BODY, Forwarder};
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
