//! Integration tests for `server::serve`: connection limit, graceful shutdown,
//! inbound TLS and HTTP/2.

mod support;

use std::convert::Infallible;
use std::future::{Ready, ready};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use brisk_gateway::server::ServerConfig;
use brisk_gateway::upstream::{UpstreamClientConfig, build_client};
use bytes::Bytes;
use http::{Request, Response, StatusCode};
use http_body_util::channel::Channel;
use http_body_util::{BodyExt, Empty};
use hyper::body::Incoming;
use hyper::service::{Service, service_fn};
use hyper_util::rt::{TokioExecutor, TokioIo};
use rcgen::{BasicConstraints, CertificateParams, IsCa, Issuer, KeyPair, KeyUsagePurpose};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio_rustls::TlsAcceptor;

const OK_RESPONSE_TAIL: &[u8] = b"\r\n\r\nok";

/// Sends one HTTP/1.1 request on a raw socket without waiting for the answer.
async fn send_raw_request(stream: &mut TcpStream) {
    stream
        .write_all(b"GET / HTTP/1.1\r\nhost: test\r\n\r\n")
        .await
        .unwrap();
}

/// Reads until the fixed `ok` response of the test service has arrived.
async fn read_ok_response(stream: &mut TcpStream) -> Vec<u8> {
    let mut buf = Vec::new();
    while !buf.ends_with(OK_RESPONSE_TAIL) {
        let mut chunk = [0_u8; 1024];
        let n = stream.read(&mut chunk).await.unwrap();
        assert_ne!(n, 0, "connection closed before the response completed");
        buf.extend_from_slice(&chunk[..n]);
    }
    buf
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serve_enforces_max_connections() {
    let config = ServerConfig {
        max_connections: 1,
        ..ServerConfig::default()
    };
    let mut server = support::start(config, None, service_fn(|_req| support::text("ok")));

    let mut first = TcpStream::connect(server.addr).await.unwrap();
    send_raw_request(&mut first).await;
    let response = read_ok_response(&mut first).await;
    assert!(response.starts_with(b"HTTP/1.1 200 OK"));

    // The kernel completes the TCP handshake from the backlog, but serve must
    // not accept it while the first connection holds the only permit.
    let mut second = TcpStream::connect(server.addr).await.unwrap();
    send_raw_request(&mut second).await;
    let mut probe = [0_u8; 64];
    let blocked = tokio::time::timeout(Duration::from_millis(500), second.read(&mut probe)).await;
    assert!(
        blocked.is_err(),
        "second connection was served beyond max_connections"
    );

    drop(first);
    let response = tokio::time::timeout(Duration::from_secs(5), read_ok_response(&mut second))
        .await
        .expect("second connection was not served after the first one closed");
    assert!(response.starts_with(b"HTTP/1.1 200 OK"));

    server.trigger_shutdown();
    server.task.await.unwrap().unwrap();
}

/// Handles to drive the streaming body of [`streaming_service`] from the test.
struct StreamControl {
    /// Fires once the first chunk has been handed to hyper.
    first_sent: oneshot::Receiver<()>,
    /// Releases the remaining chunks.
    release: oneshot::Sender<()>,
}

const STREAM_CHUNKS: [&str; 4] = [
    "data: 0\n\n",
    "data: 1\n\n",
    "data: 2\n\n",
    "data: [DONE]\n\n",
];

type StreamBody = Channel<Bytes, Infallible>;
type StreamGate = (oneshot::Sender<()>, oneshot::Receiver<()>);

/// A hand-written (non-`service_fn`) service whose single response streams
/// [`STREAM_CHUNKS`], pausing after the first chunk until the test releases it.
#[derive(Clone)]
struct StreamingService {
    gate: Arc<Mutex<Option<StreamGate>>>,
}

impl StreamingService {
    fn new() -> (Self, StreamControl) {
        let (first_tx, first_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let service = Self {
            gate: Arc::new(Mutex::new(Some((first_tx, release_rx)))),
        };
        let control = StreamControl {
            first_sent: first_rx,
            release: release_tx,
        };
        (service, control)
    }
}

impl Service<Request<Incoming>> for StreamingService {
    type Response = Response<StreamBody>;
    type Error = Infallible;
    type Future = Ready<Result<Self::Response, Self::Error>>;

    fn call(&self, _req: Request<Incoming>) -> Self::Future {
        let (first_tx, release_rx) = self
            .gate
            .lock()
            .unwrap()
            .take()
            .expect("StreamingService serves a single request");
        let (mut sender, body) = StreamBody::new(1);
        tokio::spawn(async move {
            let first = Bytes::from_static(STREAM_CHUNKS[0].as_bytes());
            if sender.send_data(first).await.is_err() {
                return;
            }
            first_tx.send(()).unwrap();
            // An error means the test dropped the gate: the body stays open
            // until the server aborts the connection.
            if release_rx.await.is_err() {
                return;
            }
            for chunk in &STREAM_CHUNKS[1..] {
                tokio::time::sleep(Duration::from_millis(50)).await;
                if sender
                    .send_data(Bytes::from_static(chunk.as_bytes()))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        });
        ready(Ok(Response::new(body)))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graceful_shutdown_lets_streaming_response_finish() {
    let (service, control) = StreamingService::new();
    let mut server = support::start(ServerConfig::default(), None, service);

    let client = build_client(&UpstreamClientConfig::default()).unwrap();
    let mut resp = client
        .post(format!("{}/v1/chat/completions", server.http_base()))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    control.first_sent.await.unwrap();
    let mut received = resp.chunk().await.unwrap().unwrap().to_vec();

    server.trigger_shutdown();
    // Let serve observe the shutdown and signal the connection before the rest
    // of the body is produced.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !server.task.is_finished(),
        "serve returned with a response in flight"
    );
    control.release.send(()).unwrap();

    while let Some(chunk) = resp.chunk().await.unwrap() {
        received.extend_from_slice(&chunk);
    }
    assert_eq!(received, STREAM_CHUNKS.concat().as_bytes());

    tokio::time::timeout(Duration::from_secs(5), server.task)
        .await
        .expect("serve did not return after the in-flight response finished")
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graceful_shutdown_aborts_after_timeout() {
    let (service, control) = StreamingService::new();
    let config = ServerConfig {
        graceful_shutdown_timeout: Duration::from_millis(300),
        ..ServerConfig::default()
    };
    let mut server = support::start(config, None, service);

    let client = build_client(&UpstreamClientConfig::default()).unwrap();
    let mut resp = client
        .post(format!("{}/v1/chat/completions", server.http_base()))
        .send()
        .await
        .unwrap();
    control.first_sent.await.unwrap();
    resp.chunk().await.unwrap().unwrap();

    // The body never completes because `control.release` is held back.
    let started = Instant::now();
    server.trigger_shutdown();
    tokio::time::timeout(Duration::from_secs(5), server.task)
        .await
        .expect("serve ignored graceful_shutdown_timeout")
        .unwrap()
        .unwrap();
    assert!(started.elapsed() >= Duration::from_millis(300));

    // The aborted connection surfaces as a body error on the client.
    assert!(resp.chunk().await.is_err());
    drop(control.release);
}

struct TestPki {
    ca_der: CertificateDer<'static>,
    server_chain: Vec<CertificateDer<'static>>,
    server_key: PrivateKeyDer<'static>,
}

fn test_pki() -> TestPki {
    let mut ca_params = CertificateParams::new(Vec::new()).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    let ca_key = KeyPair::generate().unwrap();
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();
    let issuer = Issuer::new(ca_params, ca_key);

    let mut leaf_params =
        CertificateParams::new(vec!["localhost".to_owned(), "127.0.0.1".to_owned()]).unwrap();
    leaf_params.use_authority_key_identifier_extension = true;
    leaf_params
        .extended_key_usages
        .push(rcgen::ExtendedKeyUsagePurpose::ServerAuth);
    let leaf_key = KeyPair::generate().unwrap();
    let leaf_cert = leaf_params.signed_by(&leaf_key, &issuer).unwrap();

    TestPki {
        ca_der: ca_cert.der().clone(),
        server_chain: vec![leaf_cert.der().clone()],
        server_key: PrivatePkcs8KeyDer::from(leaf_key.serialize_der()).into(),
    }
}

fn tls_acceptor(pki: &TestPki) -> TlsAcceptor {
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let mut config = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(pki.server_chain.clone(), pki.server_key.clone_key())
        .unwrap();
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    TlsAcceptor::from(Arc::new(config))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tls_inbound_with_extra_root_cert() {
    let pki = test_pki();
    let mut server = support::start(
        ServerConfig::default(),
        Some(tls_acceptor(&pki)),
        service_fn(|_req| support::text("over tls")),
    );

    let client = build_client(&UpstreamClientConfig {
        extra_root_certs: vec![pki.ca_der.clone()],
        // The test server is reached as `localhost` (TLS needs a name for
        // SNI), which resolves to loopback.
        allow_private: true,
        ..UpstreamClientConfig::default()
    })
    .unwrap();
    let url = format!("https://localhost:{}/v1/models", server.addr.port());
    let resp = client.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.text().await.unwrap(), "over tls");

    // Without the extra root the self-signed chain must be rejected; loopback
    // stays allowed so that the certificate is what fails.
    let untrusting = build_client(&UpstreamClientConfig {
        allow_private: true,
        ..UpstreamClientConfig::default()
    })
    .unwrap();
    assert!(untrusting.get(&url).send().await.is_err());

    server.trigger_shutdown();
    server.task.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tls_handshake_timeout_releases_connection_permit() {
    let pki = test_pki();
    let config = ServerConfig {
        max_connections: 1,
        tls_handshake_timeout: Duration::from_millis(200),
        ..ServerConfig::default()
    };
    let mut server = support::start(
        config,
        Some(tls_acceptor(&pki)),
        service_fn(|_req| support::text("over tls")),
    );

    // A client that connects but never starts the handshake holds the only
    // permit until the handshake deadline drops it.
    let mut idle = TcpStream::connect(server.addr).await.unwrap();
    let mut probe = [0_u8; 16];
    let closed = tokio::time::timeout(Duration::from_secs(5), idle.read(&mut probe))
        .await
        .expect("stalled TLS handshake was not timed out");
    assert!(matches!(closed, Ok(0) | Err(_)));

    let client = build_client(&UpstreamClientConfig {
        extra_root_certs: vec![pki.ca_der.clone()],
        // The test server is reached as `localhost` (TLS needs a name for
        // SNI), which resolves to loopback.
        allow_private: true,
        ..UpstreamClientConfig::default()
    })
    .unwrap();
    let resp = client
        .get(format!("https://localhost:{}/", server.addr.port()))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    server.trigger_shutdown();
    server.task.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http2_prior_knowledge_is_served() {
    let mut server = support::start(
        ServerConfig::default(),
        None,
        service_fn(|req: Request<Incoming>| async move {
            assert_eq!(req.version(), http::Version::HTTP_2);
            support::text("h2").await
        }),
    );

    let stream = TcpStream::connect(server.addr).await.unwrap();
    let (mut sender, conn) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(stream))
            .await
            .unwrap();
    let conn_task = tokio::spawn(conn);

    let req = Request::builder()
        .uri(format!("http://{}/v1/models", server.addr))
        .body(Empty::<Bytes>::new())
        .unwrap();
    let resp = sender.send_request(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.version(), http::Version::HTTP_2);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body.as_ref(), b"h2");

    drop(sender);
    conn_task.await.unwrap().unwrap();
    server.trigger_shutdown();
    server.task.await.unwrap().unwrap();
}

/// Regression for silent connections holding a permit during protocol
/// detection: no bytes at all, a partial HTTP/2 preface, and a full preface
/// without any request.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn silent_connection_is_closed_and_releases_permit() {
    let prefixes: [&[u8]; 3] = [b"", b"P", b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"];
    for prefix in prefixes {
        let config = ServerConfig {
            max_connections: 1,
            header_read_timeout: Duration::from_millis(200),
            ..ServerConfig::default()
        };
        let mut server = support::start(config, None, service_fn(|_req| support::text("ok")));

        let mut idle = TcpStream::connect(server.addr).await.unwrap();
        idle.write_all(prefix).await.unwrap();

        let mut second = TcpStream::connect(server.addr).await.unwrap();
        send_raw_request(&mut second).await;
        let response = tokio::time::timeout(Duration::from_secs(2), read_ok_response(&mut second))
            .await
            .unwrap_or_else(|_| {
                panic!("silent connection with prefix {prefix:?} kept the only permit")
            });
        assert!(response.starts_with(b"HTTP/1.1 200 OK"));

        // A full preface is answered with the server's SETTINGS before the
        // close, so read until EOF or error rather than expecting 0 at once.
        let closed = tokio::time::timeout(Duration::from_secs(2), async {
            let mut probe = [0_u8; 256];
            loop {
                match idle.read(&mut probe).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {}
                }
            }
        })
        .await;
        assert!(
            closed.is_ok(),
            "silent connection with prefix {prefix:?} was not closed"
        );

        server.trigger_shutdown();
        server.task.await.unwrap().unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_stops_accepting() {
    let mut server = support::start(
        ServerConfig::default(),
        None,
        service_fn(|_req| support::text("ok")),
    );
    let mut before = TcpStream::connect(server.addr).await.unwrap();
    send_raw_request(&mut before).await;
    read_ok_response(&mut before).await;
    drop(before);

    server.trigger_shutdown();
    server.task.await.unwrap().unwrap();

    if let Ok(mut after) = TcpStream::connect(server.addr).await {
        let mut probe = [0_u8; 16];
        let read = tokio::time::timeout(Duration::from_secs(2), after.read(&mut probe))
            .await
            .expect("connection after shutdown was neither refused nor closed");
        assert!(matches!(read, Ok(0) | Err(_)));
    }
}

/// Answers once the whole request body has arrived, so request streams stay
/// open for as long as the client keeps sending.
async fn drain_then_ok(req: Request<Incoming>) -> Result<Response<Empty<Bytes>>, Infallible> {
    // A body error only means the client went away; the test inspects the
    // client side, so there is nothing to report here.
    let _ = req.into_body().collect().await;
    Ok(Response::new(Empty::new()))
}

/// Waits until the stream's send capacity stops growing and returns it.
async fn settled_capacity(stream: &mut h2::SendStream<Bytes>) -> usize {
    loop {
        let next = tokio::time::timeout(
            Duration::from_millis(300),
            std::future::poll_fn(|cx| stream.poll_capacity(cx)),
        )
        .await;
        match next {
            Ok(Some(result)) => {
                result.unwrap();
            }
            Ok(None) | Err(_) => return stream.capacity(),
        }
    }
}

fn h2_request(addr: std::net::SocketAddr) -> http::request::Builder {
    Request::builder()
        .method(http::Method::POST)
        .uri(format!("http://{addr}/v1/chat/completions"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h2_settings_follow_server_config() {
    let config = ServerConfig::default();
    let stream_window = usize::try_from(config.h2_initial_stream_window_size).unwrap();
    let mut server = support::start(config, None, service_fn(drain_then_ok));

    let reserve = 4 * stream_window;
    let tcp = TcpStream::connect(server.addr).await.unwrap();
    // h2 caps send capacity at its own send buffer (400 KiB by default), which
    // would hide the server's window.
    let (client, conn) = h2::client::Builder::new()
        .max_send_buffer_size(2 * reserve)
        .handshake::<_, Bytes>(tcp)
        .await
        .unwrap();
    let conn_task = tokio::spawn(conn);
    let mut client = client.ready().await.unwrap();

    let (_resp1, mut first) = client
        .send_request(h2_request(server.addr).body(()).unwrap(), false)
        .unwrap();
    first.reserve_capacity(reserve);
    assert_eq!(
        settled_capacity(&mut first).await,
        stream_window,
        "stream window must be h2_initial_stream_window_size"
    );
    assert_eq!(client.current_max_send_streams(), 256);

    // The 2 MiB connection window covers exactly two full stream windows.
    let mut client = client.ready().await.unwrap();
    let (_resp2, mut second) = client
        .send_request(h2_request(server.addr).body(()).unwrap(), false)
        .unwrap();
    second.reserve_capacity(reserve);
    assert_eq!(settled_capacity(&mut second).await, stream_window);
    let mut client = client.ready().await.unwrap();
    let (_resp3, mut third) = client
        .send_request(h2_request(server.addr).body(()).unwrap(), false)
        .unwrap();
    third.reserve_capacity(reserve);
    assert_eq!(
        settled_capacity(&mut third).await,
        0,
        "connection window must be h2_initial_connection_window_size"
    );

    drop((first, second, third, client));
    conn_task.abort();
    server.trigger_shutdown();
    server.task.await.unwrap().unwrap();
}

/// Sends one HTTP/2 request carrying a `len`-byte header on a fresh connection
/// and returns the response status or the h2 error.
async fn h2_status_with_header(
    addr: std::net::SocketAddr,
    len: usize,
) -> Result<StatusCode, h2::Error> {
    let tcp = TcpStream::connect(addr).await.unwrap();
    let (client, conn) = h2::client::handshake(tcp).await.unwrap();
    let conn_task = tokio::spawn(conn);
    let mut client = client.ready().await.unwrap();
    let req = h2_request(addr)
        .header("x-pad", "a".repeat(len))
        .body(())
        .unwrap();
    let outcome = match client.send_request(req, true) {
        Ok((resp, _)) => resp.await.map(|resp| resp.status()),
        Err(err) => Err(err),
    };
    conn_task.abort();
    outcome
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h2_max_header_list_size_is_enforced() {
    let mut server = support::start(ServerConfig::default(), None, service_fn(drain_then_ok));

    // Comfortably below 64 KiB passes; above it the server answers 431 or
    // tears the stream or connection down.
    let small = h2_status_with_header(server.addr, 60 * 1024).await;
    assert_eq!(small.unwrap(), StatusCode::OK);
    let large = h2_status_with_header(server.addr, 70 * 1024).await;
    assert!(
        !matches!(large, Ok(StatusCode::OK)),
        "70 KiB header list was accepted: {large:?}"
    );

    server.trigger_shutdown();
    server.task.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conn_info_reaches_service_factory() {
    use brisk_gateway::server::{ConnInfo, bind, serve_with_conn_info};

    let pki = test_pki();
    let config = ServerConfig::default();
    let listener = bind("127.0.0.1:0".parse().unwrap(), &config).unwrap();
    let addr = listener.local_addr().unwrap();
    let (seen_tx, mut seen_rx) = tokio::sync::mpsc::unbounded_channel::<ConnInfo>();
    let (stop_tx, stop_rx) = oneshot::channel::<()>();
    let task = tokio::spawn(serve_with_conn_info(
        listener,
        Some(tls_acceptor(&pki)),
        config,
        move |info: &ConnInfo| {
            seen_tx.send(info.clone()).unwrap();
            service_fn(|_req| support::text("ok"))
        },
        async move {
            let _ = stop_rx.await;
        },
    ));

    let client = build_client(&UpstreamClientConfig {
        extra_root_certs: vec![pki.ca_der.clone()],
        // The test server is reached as `localhost` (TLS needs a name for
        // SNI), which resolves to loopback.
        allow_private: true,
        ..UpstreamClientConfig::default()
    })
    .unwrap();
    let resp = client
        .get(format!("https://localhost:{}/", addr.port()))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let info = seen_rx.recv().await.unwrap();
    assert_eq!(info.local, addr);
    assert!(info.peer.ip().is_loopback());
    assert_ne!(info.peer.port(), addr.port());
    let tls = info.tls.expect("TLS connection must carry TlsInfo");
    assert_eq!(tls.server_name.as_deref(), Some("localhost"));
    // The upstream client is HTTP/1.1 only.
    assert_eq!(tls.alpn_protocol.as_deref(), Some(&b"http/1.1"[..]));

    stop_tx.send(()).unwrap();
    task.await.unwrap().unwrap();
}
