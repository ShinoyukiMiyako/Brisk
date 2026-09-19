//! End-to-end tests: a real mock server on 127.0.0.1 driven by blocking
//! HTTP/1.1 clients, plaintext and TLS.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::time::{Duration, Instant};

use brisk_bench_core::http1::{self, BodyFraming, ChunkedDecoder};
use brisk_bench_core::transport::certs::CertBundle;
use brisk_bench_core::transport::tls;
use brisk_bench_core::wire::{self, BenchParams, BodyShape, MARKER_LEN, Marker};
use brisk_mock::{MockConfig, ServerHandle, Snapshot};

const IO_TIMEOUT: Duration = Duration::from_secs(10);

fn defaults() -> BenchParams {
    BenchParams {
        ttft_us: 0,
        interval_us: 1_000,
        chunks: 2,
        chunk_bytes: 90,
        sid: 0,
        resp_bytes: 64,
    }
}

fn start(tls: Option<Arc<rustls::ServerConfig>>) -> ServerHandle {
    let mut config = MockConfig::new("127.0.0.1:0".parse().unwrap(), defaults());
    config.tls = tls;
    ServerHandle::start(config).unwrap()
}

trait Stream: Read + Write {}
impl<T: Read + Write> Stream for T {}

/// A parsed response.
#[derive(Debug)]
struct Response {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Response {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    fn json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.body).unwrap()
    }
}

/// Blocking HTTP/1.1 client that keeps unconsumed bytes for pipelining.
struct Client {
    stream: Box<dyn Stream>,
    buf: Vec<u8>,
}

impl Client {
    fn plain(addr: SocketAddr) -> Self {
        let tcp = TcpStream::connect(addr).unwrap();
        tcp.set_read_timeout(Some(IO_TIMEOUT)).unwrap();
        tcp.set_nodelay(true).unwrap();
        Self {
            stream: Box::new(tcp),
            buf: Vec::new(),
        }
    }

    fn tls(addr: SocketAddr, config: Arc<rustls::ClientConfig>) -> Self {
        let tcp = TcpStream::connect(addr).unwrap();
        tcp.set_read_timeout(Some(IO_TIMEOUT)).unwrap();
        let conn =
            rustls::ClientConnection::new(config, tls::server_name("localhost").unwrap()).unwrap();
        Self {
            stream: Box::new(rustls::StreamOwned::new(conn, tcp)),
            buf: Vec::new(),
        }
    }

    fn send(&mut self, bytes: &[u8]) {
        self.stream.write_all(bytes).unwrap();
        self.stream.flush().unwrap();
    }

    /// Reads more bytes; returns false on EOF.
    fn fill(&mut self) -> bool {
        let mut chunk = [0u8; 16 * 1024];
        let n = self.stream.read(&mut chunk).unwrap();
        self.buf.extend_from_slice(&chunk[..n]);
        n > 0
    }

    /// Reads one final (non-1xx) response.
    fn response(&mut self) -> Response {
        loop {
            let response = self.any_response();
            if !(100..200).contains(&response.status) {
                return response;
            }
        }
    }

    fn any_response(&mut self) -> Response {
        let (status, headers, framing, head_len) = loop {
            let mut header_buf = [httparse::EMPTY_HEADER; 32];
            let mut resp = httparse::Response::new(&mut header_buf);
            if let httparse::Status::Complete(len) = resp.parse(&self.buf).unwrap() {
                let status = resp.code.unwrap();
                let framing = http1::response_body_framing(status, false, resp.headers).unwrap();
                let headers = resp
                    .headers
                    .iter()
                    .map(|h| {
                        (
                            h.name.to_owned(),
                            String::from_utf8(h.value.to_vec()).unwrap(),
                        )
                    })
                    .collect::<Vec<_>>();
                break (status, headers, framing, len);
            }
            assert!(self.fill(), "EOF inside a response head");
        };
        self.buf.drain(..head_len);
        let body = match framing {
            BodyFraming::Length(len) => {
                let len = usize::try_from(len).unwrap();
                while self.buf.len() < len {
                    assert!(self.fill(), "EOF inside a body");
                }
                self.buf.drain(..len).collect()
            }
            BodyFraming::Chunked => {
                let mut decoder = ChunkedDecoder::new();
                let mut body = Vec::new();
                loop {
                    let progress = decoder
                        .decode(&self.buf, |d| body.extend_from_slice(d))
                        .unwrap();
                    self.buf.drain(..progress.consumed);
                    if progress.done {
                        break body;
                    }
                    assert!(self.fill(), "EOF inside a chunked body");
                }
            }
            BodyFraming::Unframed => panic!("mock responses are always framed"),
        };
        Response {
            status,
            headers,
            body,
        }
    }

    fn request(&mut self, method: &str, path: &str, body: &[u8]) -> Response {
        self.send(&request_bytes(method, path, body, &[]));
        self.response()
    }

    fn stats(&mut self) -> Snapshot {
        let response = self.request("GET", "/__bench/stats", b"");
        assert_eq!(response.status, 200);
        serde_json::from_slice(&response.body).unwrap()
    }
}

fn request_bytes(method: &str, path: &str, body: &[u8], extra: &[(&str, &str)]) -> Vec<u8> {
    let headers: Vec<(&str, &str)> = [("content-type", "application/json")]
        .into_iter()
        .chain(extra.iter().copied())
        .collect();
    let mut out = Vec::new();
    http1::write_request_head(
        &mut out,
        method,
        path,
        "localhost",
        &headers,
        BodyFraming::Length(body.len() as u64),
    );
    out.extend_from_slice(body);
    out
}

/// Splits an SSE body into its `data:` payloads.
fn sse_events(body: &[u8]) -> Vec<String> {
    std::str::from_utf8(body)
        .unwrap()
        .split("\n\n")
        .filter(|event| !event.is_empty())
        .map(|event| event.strip_prefix("data: ").unwrap().to_owned())
        .collect()
}

/// Checks a complete streamed response against `params` and returns its
/// markers.
fn check_stream(response: &Response, params: &BenchParams, include_usage: bool) -> Vec<Marker> {
    assert_eq!(response.status, 200);
    assert_eq!(response.header("content-type"), Some("text/event-stream"));
    let events = sse_events(&response.body);
    let chunks = usize::try_from(params.chunks).unwrap();
    assert_eq!(events.len(), chunks + 2 + usize::from(include_usage));
    assert_eq!(events.last().unwrap(), "[DONE]");

    let mut markers = Vec::new();
    for (seq, event) in events[..chunks].iter().enumerate() {
        let json: serde_json::Value = serde_json::from_str(event).unwrap();
        assert_eq!(json["object"], "chat.completion.chunk");
        let content = json["choices"][0]["delta"]["content"].as_str().unwrap();
        assert_eq!(content.len(), usize::try_from(params.chunk_bytes).unwrap());
        assert!(content.as_bytes()[MARKER_LEN..].iter().all(|&b| b == b'x'));
        let marker = Marker::parse(&content.as_bytes()[..MARKER_LEN]).unwrap();
        assert_eq!(marker.sid, params.sid);
        assert_eq!(marker.seq, u32::try_from(seq).unwrap());
        assert!(marker.t_write >= marker.t_sched, "{marker:?}");
        markers.push(marker);
    }
    for pair in markers.windows(2) {
        assert_eq!(
            pair[1].t_sched - pair[0].t_sched,
            params.interval_us * 1_000
        );
    }

    let finish: serde_json::Value = serde_json::from_str(&events[chunks]).unwrap();
    assert_eq!(finish["choices"][0]["finish_reason"], "stop");
    if include_usage {
        let usage: serde_json::Value = serde_json::from_str(&events[chunks + 1]).unwrap();
        assert_eq!(usage["usage"]["completion_tokens"], params.chunks);
    }
    markers
}

fn stream_params(sid: u64) -> BenchParams {
    BenchParams {
        ttft_us: 2_000,
        interval_us: 3_000,
        chunks: 6,
        chunk_bytes: 120,
        sid,
        resp_bytes: 0,
    }
}

#[test]
fn streams_plaintext_with_markers_and_usage() {
    let server = start(None);
    let mut client = Client::plain(server.local_addr());
    let params = stream_params(11);
    let body = wire::chat_request_body("m", true, true, &params, BodyShape::Text { bytes: 64 });

    let sent = brisk_bench_core::clock::now_ns();
    let response = client.request("POST", "/v1/chat/completions", &body);
    let markers = check_stream(&response, &params, true);
    assert!(markers[0].t_sched >= sent + params.ttft_us * 1_000);

    // The connection is kept alive: a second stream without usage follows.
    let params = stream_params(12);
    let body = wire::chat_request_body("m", true, false, &params, BodyShape::Text { bytes: 8 });
    let response = client.request("POST", "/v1/chat/completions", &body);
    check_stream(&response, &params, false);
    server.shutdown().unwrap();
}

#[test]
fn streams_over_tls() {
    let bundle = CertBundle::generate(&["localhost".to_owned(), "127.0.0.1".to_owned()]).unwrap();
    let server_config = tls::server_config_from_pem(
        bundle.server_cert_pem.as_bytes(),
        bundle.server_key_pem.as_bytes(),
    )
    .unwrap();
    let client_config = tls::client_config_from_pem(bundle.ca_cert_pem.as_bytes()).unwrap();
    let server = start(Some(server_config));
    let mut client = Client::tls(server.local_addr(), client_config);

    let params = stream_params(21);
    let body = wire::chat_request_body("m", true, true, &params, BodyShape::Text { bytes: 64 });
    let response = client.request("POST", "/v1/chat/completions", &body);
    check_stream(&response, &params, true);

    let response = client.request("GET", "/v1/models", b"");
    assert_eq!(response.json()["data"][0]["id"], "brisk-mock");
    server.shutdown().unwrap();
}

#[test]
fn pipelined_non_streaming_requests_are_answered_in_order() {
    let server = start(None);
    let mut client = Client::plain(server.local_addr());
    let slow = BenchParams {
        ttft_us: 20_000,
        resp_bytes: 300,
        sid: 31,
        ..stream_params(0)
    };
    let fast = BenchParams {
        ttft_us: 0,
        resp_bytes: 10,
        sid: 32,
        ..slow
    };
    let mut wire_bytes = Vec::new();
    for params in [&slow, &fast] {
        let body =
            wire::chat_request_body("m", false, false, params, BodyShape::Text { bytes: 16 });
        wire_bytes.extend(request_bytes("POST", "/v1/chat/completions", &body, &[]));
    }
    wire_bytes.extend(request_bytes("GET", "/v1/models", b"", &[]));
    let started = Instant::now();
    client.send(&wire_bytes);

    let first = client.response();
    assert!(started.elapsed() >= Duration::from_millis(20));
    let json = first.json();
    assert_eq!(json["object"], "chat.completion");
    assert_eq!(json["id"], "chatcmpl-mock-31");
    let content = json["choices"][0]["message"]["content"].as_str().unwrap();
    assert_eq!(content.len(), 300);
    let marker = Marker::parse(&content.as_bytes()[..MARKER_LEN]).unwrap();
    assert_eq!(marker.sid, 31);
    assert!(marker.t_write >= marker.t_sched);
    assert!(json["usage"]["total_tokens"].as_u64().unwrap() > 0);

    let second = client.response().json();
    assert_eq!(second["id"], "chatcmpl-mock-32");
    assert_eq!(second["choices"][0]["message"]["content"], "xxxxxxxxxx");

    let third = client.response().json();
    assert_eq!(third["object"], "list");
    server.shutdown().unwrap();
}

#[test]
fn chunked_upload_without_directive_uses_defaults() {
    let server = start(None);
    let mut client = Client::plain(server.local_addr());
    let body = br#"{"model":"m","stream":true,"messages":[{"role":"user","content":"hi"}]}"#;
    let mut wire_bytes = b"POST /v1/chat/completions HTTP/1.1\r\nhost: x\r\ntransfer-encoding: chunked\r\nexpect: 100-continue\r\n\r\n".to_vec();
    client.send(&wire_bytes);
    let interim = client.any_response();
    assert_eq!(interim.status, 100);

    wire_bytes.clear();
    for part in body.chunks(7) {
        http1::write_chunk(&mut wire_bytes, part);
    }
    wire_bytes.extend_from_slice(http1::LAST_CHUNK);
    client.send(&wire_bytes);
    let response = client.response();
    check_stream(&response, &defaults(), false);
    server.shutdown().unwrap();
}

#[test]
fn large_body_is_read_completely() {
    let server = start(None);
    let mut client = Client::plain(server.local_addr());
    let params = BenchParams {
        ttft_us: 0,
        resp_bytes: 100,
        sid: 41,
        ..stream_params(0)
    };
    let body = wire::chat_request_body(
        "m",
        false,
        false,
        &params,
        BodyShape::Image {
            base64_bytes: 16 * 1024 * 1024,
            text_bytes: 1024,
        },
    );
    let response = client.request("POST", "/v1/chat/completions", &body);
    let json = response.json();
    assert_eq!(json["id"], "chatcmpl-mock-41");
    let prompt_tokens = json["usage"]["prompt_tokens"].as_u64().unwrap();
    assert_eq!(prompt_tokens, (body.len() as u64).div_ceil(4));
    server.shutdown().unwrap();
}

#[test]
fn stats_count_requests_and_reset() {
    let server = start(None);
    let mut client = Client::plain(server.local_addr());
    assert_eq!(client.request("POST", "/__bench/reset", b"").status, 204);

    let params = BenchParams {
        ttft_us: 0,
        ..stream_params(51)
    };
    let body = wire::chat_request_body("m", true, false, &params, BodyShape::Text { bytes: 8 });
    check_stream(
        &client.request("POST", "/v1/chat/completions", &body),
        &params,
        false,
    );
    let body = wire::chat_request_body("m", false, false, &params, BodyShape::Text { bytes: 8 });
    assert_eq!(
        client.request("POST", "/v1/chat/completions", &body).status,
        200
    );
    assert_eq!(client.request("GET", "/nope", b"").status, 404);
    assert_eq!(
        client.request("GET", "/v1/chat/completions", b"").status,
        405
    );
    let bad = br#"{"messages":[{"role":"system","content":"bench:v1;ttft_us=1"}]}"#;
    assert_eq!(
        client.request("POST", "/v1/chat/completions", bad).status,
        400
    );

    let stats = client.stats();
    assert_eq!(stats.requests, 5);
    assert_eq!(stats.streams_started, 1);
    assert_eq!(stats.active_streams, 0);
    assert_eq!(stats.chunks, u64::from(params.chunks));
    assert_eq!(stats.errors.not_found, 1);
    assert_eq!(stats.errors.method_not_allowed, 1);
    assert_eq!(stats.errors.bad_directive, 1);
    assert_eq!(stats.shards.len(), 1);
    assert_eq!(stats.shards[0].requests, 5);
    assert_eq!(stats.shards[0].chunks, u64::from(params.chunks));
    // Kernel receive timestamps exist on Linux only; there every API request
    // arrived on a timestamped receive and is sampled, bench requests not.
    let expected_samples = if cfg!(target_os = "linux") { 5 } else { 0 };
    assert_eq!(stats.read_lag.samples, expected_samples);
    assert_eq!(stats.shards[0].read_lag_samples, expected_samples);
    assert!(
        stats.read_lag.max_ns < 1_000_000_000,
        "{:?}",
        stats.read_lag
    );

    assert_eq!(client.request("POST", "/__bench/reset", b"").status, 204);
    let stats = client.stats();
    assert_eq!(stats.requests, 0);
    assert_eq!(stats.chunks, 0);
    assert_eq!(stats.read_lag.samples, 0);
    server.shutdown().unwrap();
}

#[test]
fn concurrent_streams_on_one_shard_stay_separate() {
    let server = start(None);
    let addr = server.local_addr();
    let run = |params: BenchParams| {
        move || {
            let mut client = Client::plain(addr);
            let body =
                wire::chat_request_body("m", true, false, &params, BodyShape::Text { bytes: 8 });
            let response = client.request("POST", "/v1/chat/completions", &body);
            check_stream(&response, &params, false);
        }
    };
    let fast = BenchParams {
        ttft_us: 0,
        interval_us: 1_000,
        chunks: 20,
        ..stream_params(81)
    };
    let slow = BenchParams {
        ttft_us: 500,
        interval_us: 3_000,
        chunks: 7,
        ..stream_params(82)
    };
    std::thread::scope(|scope| {
        let a = scope.spawn(run(fast));
        let b = scope.spawn(run(slow));
        a.join().unwrap();
        b.join().unwrap();
    });
    let stats = Client::plain(addr).stats();
    assert_eq!(stats.streams_started, 2);
    assert_eq!(stats.chunks, 27);
    server.shutdown().unwrap();
}

#[test]
fn request_pipelined_behind_a_stream_waits_for_it() {
    let server = start(None);
    let mut client = Client::plain(server.local_addr());
    let params = stream_params(91);
    let body = wire::chat_request_body("m", true, true, &params, BodyShape::Text { bytes: 32 });
    let mut wire_bytes = request_bytes("POST", "/v1/chat/completions", &body, &[]);
    wire_bytes.extend(request_bytes("GET", "/v1/models", b"", &[]));
    client.send(&wire_bytes);
    check_stream(&client.response(), &params, true);
    assert_eq!(client.response().json()["object"], "list");
    server.shutdown().unwrap();
}

#[test]
fn slow_reader_gets_every_chunk_after_blocked_writes() {
    let server = start(None);
    let params = BenchParams {
        ttft_us: 0,
        interval_us: 1_000,
        chunks: 16,
        chunk_bytes: brisk_mock::server::MAX_CHUNK_BYTES,
        sid: 101,
        resp_bytes: 0,
    };
    let mut client = Client::plain(server.local_addr());
    let body = wire::chat_request_body("m", true, true, &params, BodyShape::Text { bytes: 8 });
    client.send(&request_bytes("POST", "/v1/chat/completions", &body, &[]));
    // 16 MiB of events exceed what the socket buffers hold, so the mock has
    // to queue output until the client starts reading.
    let mut observer = Client::plain(server.local_addr());
    let deadline = Instant::now() + IO_TIMEOUT;
    while observer.stats().write_blocked == 0 {
        assert!(Instant::now() < deadline, "no write ever blocked");
        std::thread::sleep(Duration::from_millis(5));
    }
    std::thread::sleep(Duration::from_millis(50));
    let markers = check_stream(&client.response(), &params, true);
    assert_eq!(markers.len(), 16);
    let stats = observer.stats();
    assert_eq!(stats.chunks, 16);
    assert_eq!(stats.active_streams, 0);
    server.shutdown().unwrap();
}

#[test]
fn oversized_directive_sizes_are_rejected() {
    let server = start(None);
    let mut client = Client::plain(server.local_addr());
    let params = BenchParams {
        chunk_bytes: brisk_mock::server::MAX_CHUNK_BYTES + 1,
        ..stream_params(111)
    };
    let body = wire::chat_request_body("m", true, false, &params, BodyShape::Text { bytes: 8 });
    let response = client.request("POST", "/v1/chat/completions", &body);
    assert_eq!(response.status, 400);
    let message = response.json()["error"]["message"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(message.contains("chunk_bytes"), "{message}");
    // The connection stays usable after the rejection.
    assert_eq!(client.stats().errors.bad_directive, 1);

    let mut config = MockConfig::new("127.0.0.1:0".parse().unwrap(), defaults());
    config.defaults.resp_bytes = brisk_mock::server::MAX_RESP_BYTES + 1;
    assert!(matches!(
        ServerHandle::start(config),
        Err(brisk_mock::ServerError::DefaultsOutOfRange(
            brisk_mock::ParamsError::RespBytes(_)
        ))
    ));
    server.shutdown().unwrap();
}

#[test]
fn connection_close_is_honoured() {
    let server = start(None);
    let mut client = Client::plain(server.local_addr());
    client.send(&request_bytes(
        "GET",
        "/v1/models",
        b"",
        &[("connection", "close")],
    ));
    let response = client.response();
    assert_eq!(response.header("connection"), Some("close"));
    assert!(!client.fill(), "server keeps the connection open");
    server.shutdown().unwrap();
}

#[test]
fn client_disconnect_mid_stream_drops_the_stream() {
    let server = start(None);
    let mut observer = Client::plain(server.local_addr());
    let params = BenchParams {
        ttft_us: 0,
        interval_us: 5_000,
        chunks: 100_000,
        chunk_bytes: 100,
        sid: 61,
        resp_bytes: 0,
    };
    {
        let mut client = Client::plain(server.local_addr());
        let body = wire::chat_request_body("m", true, false, &params, BodyShape::Text { bytes: 8 });
        client.send(&request_bytes("POST", "/v1/chat/completions", &body, &[]));
        // Wait until the head and a first event have arrived.
        while client.buf.len() < 200 {
            assert!(client.fill());
        }
        assert_eq!(observer.stats().active_streams, 1);
    }
    let deadline = Instant::now() + IO_TIMEOUT;
    loop {
        let stats = observer.stats();
        if stats.active_streams == 0 {
            assert_eq!(stats.errors.aborted, 1);
            assert!(stats.chunks < 1_000);
            break;
        }
        assert!(Instant::now() < deadline, "stream not dropped: {stats:?}");
        std::thread::sleep(Duration::from_millis(10));
    }
    server.shutdown().unwrap();
}

#[test]
fn malformed_request_is_rejected_and_closed() {
    let server = start(None);
    let mut client = Client::plain(server.local_addr());
    client.send(b"GET / HTTP/1.0\r\n\r\n");
    assert_eq!(client.response().status, 505);
    assert!(!client.fill());

    let mut client = Client::plain(server.local_addr());
    client.send(b"POST /v1/chat/completions HTTP/1.1\r\ncontent-length: x\r\n\r\n");
    assert_eq!(client.response().status, 400);
    assert_eq!(
        Client::plain(server.local_addr())
            .stats()
            .errors
            .bad_request,
        2
    );
    server.shutdown().unwrap();
}

#[cfg(target_os = "linux")]
#[test]
fn sharded_pinned_server_serves_every_connection() {
    let mut config = MockConfig::new("127.0.0.1:0".parse().unwrap(), defaults());
    config.shards = 2;
    config.cpus = Some(vec![0]);
    let server = ServerHandle::start(config).unwrap();
    let params = BenchParams {
        ttft_us: 0,
        ..stream_params(71)
    };
    let body = wire::chat_request_body("m", true, false, &params, BodyShape::Text { bytes: 8 });
    for _ in 0..8 {
        let mut client = Client::plain(server.local_addr());
        check_stream(
            &client.request("POST", "/v1/chat/completions", &body),
            &params,
            false,
        );
    }
    let stats = Client::plain(server.local_addr()).stats();
    assert_eq!(stats.requests, 8);
    assert_eq!(stats.streams_started, 8);
    assert_eq!(stats.shards.len(), 2);
    assert_eq!(stats.shards.iter().map(|s| s.requests).sum::<u64>(), 8);
    assert_eq!(stats.shards.iter().map(|s| s.accepts).sum::<u64>(), 9);
    assert_eq!(stats.read_lag.samples, 8);
    server.shutdown().unwrap();
}

#[cfg(not(target_os = "linux"))]
#[test]
fn several_shards_need_linux() {
    let mut config = MockConfig::new("127.0.0.1:0".parse().unwrap(), defaults());
    config.shards = 2;
    assert!(matches!(
        ServerHandle::start(config),
        Err(brisk_mock::ServerError::ShardsUnsupported(2))
    ));
}
