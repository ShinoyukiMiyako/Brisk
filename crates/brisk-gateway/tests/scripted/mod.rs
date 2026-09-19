//! A scripted HTTP/1.1 upstream on raw TCP, for tests that need exact control
//! over when bytes are sent, when a stream stops and how a connection ends.
//!
//! Each request is recorded, handed to the test's script together with its
//! global index, and answered with the returned [`Reply`]. Request heads are
//! parsed with `httparse`; chunked bodies are decoded and encoded with
//! `brisk_bench_core::http1`. Peer closes are detected even while a reply is
//! paused between frames, so cancellation tests can measure how fast the
//! gateway lets go of an upstream connection.

// Every test binary compiles this module and uses a different subset of it.
#![allow(dead_code)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use brisk_bench_core::http1::{self, BodyFraming, ChunkedDecoder};
use bytes::{Buf, Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{Instant, sleep, timeout};

/// Largest accepted request head.
const MAX_HEAD: usize = 64 * 1024;
/// Most header fields in a request head.
const MAX_HEADERS: usize = 64;

/// What the upstream sends back for one request.
#[derive(Debug, Clone)]
pub(crate) enum Reply {
    /// Any status with a `Content-Length` body.
    Status {
        status: u16,
        headers: Vec<(&'static str, String)>,
        body: Bytes,
    },
    /// `200` with a `Content-Length` body, sent after `head_delay`;
    /// `content-type: application/json` unless `headers` names one.
    Json {
        head_delay: Duration,
        headers: Vec<(&'static str, String)>,
        body: Bytes,
    },
    /// `200` with a chunked body, one chunk per frame, each frame sent after
    /// its delay; `content-type: text/event-stream` unless `headers` names one.
    Sse {
        head_delay: Duration,
        headers: Vec<(&'static str, String)>,
        frames: Vec<(Duration, Bytes)>,
        end: SseEnd,
    },
    /// A redirect with an empty body.
    Redirect { status: u16, location: String },
    /// `SO_LINGER` 0 then close: the client sees a reset.
    Reset,
    /// Never answer; keep the connection open.
    Hang,
}

/// How a [`Reply::Sse`] ends after its last frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SseEnd {
    /// Terminating chunk; the connection stays open for the next request.
    Finish,
    /// Close the connection without the terminating chunk: a truncated body.
    Close,
    /// Keep the connection open without sending anything more.
    Hang,
}

/// A request as the upstream received it.
#[derive(Debug, Clone)]
pub(crate) struct RecordedRequest {
    pub(crate) method: String,
    /// The request target, e.g. `/v1/chat/completions`.
    pub(crate) target: String,
    /// In arrival order; names as sent.
    pub(crate) headers: Vec<(String, Vec<u8>)>,
    /// The decoded body.
    pub(crate) body: Bytes,
    /// Index of the connection, in accept order from 0.
    pub(crate) conn: u64,
}

impl RecordedRequest {
    /// Values of the header `name`, compared case-insensitively.
    pub(crate) fn header_values<'a>(
        &'a self,
        name: &'a str,
    ) -> impl Iterator<Item = &'a [u8]> + 'a {
        self.headers
            .iter()
            .filter(move |(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_slice())
    }
}

/// Connection behavior independent of the script.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct ScriptConfig {
    /// Close a connection that stays idle between requests this long, like a
    /// server-side keep-alive timeout.
    pub(crate) idle_close: Option<Duration>,
}

type Script = dyn Fn(usize, &RecordedRequest) -> Reply + Send + Sync;

#[derive(Default)]
struct Shared {
    requests: Mutex<Vec<RecordedRequest>>,
    accepts: AtomicU64,
    closed: Mutex<HashMap<u64, Instant>>,
    closed_notify: Notify,
}

impl Shared {
    fn record_close(&self, conn: u64) {
        lock(&self.closed).entry(conn).or_insert_with(Instant::now);
        self.closed_notify.notify_waiters();
    }
}

/// A test mutex is only poisoned after another assertion already failed;
/// the data is still the best evidence to report.
fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The scripted upstream; stops accepting and drops every connection when
/// dropped.
pub(crate) struct ScriptedUpstream {
    addr: SocketAddr,
    shared: Arc<Shared>,
    task: JoinHandle<()>,
}

impl Drop for ScriptedUpstream {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl ScriptedUpstream {
    /// Listens on `127.0.0.1:0` with the default [`ScriptConfig`].
    pub(crate) async fn start(
        script: impl Fn(usize, &RecordedRequest) -> Reply + Send + Sync + 'static,
    ) -> Self {
        Self::start_with(ScriptConfig::default(), script).await
    }

    /// Listens on `127.0.0.1:0`; `script` receives the global request index
    /// (from 0) and the recorded request.
    pub(crate) async fn start_with(
        config: ScriptConfig,
        script: impl Fn(usize, &RecordedRequest) -> Reply + Send + Sync + 'static,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let shared = Arc::new(Shared::default());
        let script: Arc<Script> = Arc::new(script);
        let task = tokio::spawn(accept_loop(listener, Arc::clone(&shared), script, config));
        Self { addr, shared, task }
    }

    /// The listening address.
    pub(crate) fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// `http://127.0.0.1:<port>/v1`
    pub(crate) fn base_url(&self) -> String {
        format!("http://{}/v1", self.addr)
    }

    /// Every request received so far, in arrival order.
    pub(crate) fn requests(&self) -> Vec<RecordedRequest> {
        lock(&self.shared.requests).clone()
    }

    /// Connections accepted so far.
    pub(crate) fn accepts(&self) -> u64 {
        self.shared.accepts.load(Ordering::SeqCst)
    }

    /// When connection `conn` was closed by the peer, if it has been.
    pub(crate) fn closed_at(&self, conn: u64) -> Option<Instant> {
        lock(&self.shared.closed).get(&conn).copied()
    }

    /// Waits up to `limit` for the peer to close connection `conn`.
    pub(crate) async fn wait_closed(&self, conn: u64, limit: Duration) -> Option<Instant> {
        let deadline = Instant::now() + limit;
        loop {
            // Registered before the check, so a close in between still wakes us.
            let notified = self.shared.closed_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(at) = self.closed_at(conn) {
                return Some(at);
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return self.closed_at(conn);
            }
        }
    }
}

async fn accept_loop(
    listener: TcpListener,
    shared: Arc<Shared>,
    script: Arc<Script>,
    config: ScriptConfig,
) {
    // Owned here so that aborting the accept task drops every connection.
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, _) = accepted.unwrap();
                let conn = shared.accepts.fetch_add(1, Ordering::SeqCst);
                connections.spawn(serve_conn(
                    stream,
                    conn,
                    Arc::clone(&shared),
                    Arc::clone(&script),
                    config,
                ));
            }
            Some(joined) = connections.join_next() => {
                if let Err(err) = joined
                    && err.is_panic()
                {
                    std::panic::resume_unwind(err.into_panic());
                }
            }
        }
    }
}

/// How a connection ended, as far as bookkeeping is concerned.
enum ConnEnd {
    /// The peer closed or reset it; recorded in `closed`.
    PeerClosed,
    /// This side closed it (reset, truncation, idle close).
    Closed,
}

struct Connection {
    stream: TcpStream,
    index: u64,
    buf: BytesMut,
}

async fn serve_conn(
    stream: TcpStream,
    conn: u64,
    shared: Arc<Shared>,
    script: Arc<Script>,
    config: ScriptConfig,
) {
    stream.set_nodelay(true).unwrap();
    let mut c = Connection {
        stream,
        index: conn,
        buf: BytesMut::with_capacity(8 * 1024),
    };
    let end = loop {
        // Idle only counts between requests: a started request is read in full.
        if let Some(idle) = config.idle_close
            && c.buf.is_empty()
        {
            match timeout(idle, c.fill()).await {
                Err(_) => break ConnEnd::Closed,
                Ok(false) => break ConnEnd::PeerClosed,
                Ok(true) => {}
            }
        }
        let Some(request) = c.read_request().await else {
            break ConnEnd::PeerClosed;
        };
        let head = request.method == "HEAD";
        let index = {
            let mut requests = lock(&shared.requests);
            requests.push(request.clone());
            requests.len() - 1
        };
        let reply = script(index, &request);
        match c.reply(reply, head).await {
            Ok(()) => {}
            Err(end) => break end,
        }
    };
    if matches!(end, ConnEnd::PeerClosed) {
        shared.record_close(conn);
    }
}

impl Connection {
    /// Reads one request; `None` when the peer closed or reset the
    /// connection (also in the middle of a request).
    async fn read_request(&mut self) -> Option<RecordedRequest> {
        let (method, target, headers, framing, head_len) = loop {
            if !self.buf.is_empty() {
                let mut storage = [httparse::EMPTY_HEADER; MAX_HEADERS];
                let mut req = httparse::Request::new(&mut storage);
                match req.parse(&self.buf).expect("malformed request head") {
                    httparse::Status::Complete(len) => {
                        let framing = http1::request_body_framing(req.headers)
                            .expect("invalid request body framing");
                        let headers = req
                            .headers
                            .iter()
                            .map(|h| (h.name.to_owned(), h.value.to_vec()))
                            .collect::<Vec<_>>();
                        break (
                            req.method.unwrap().to_owned(),
                            req.path.unwrap().to_owned(),
                            headers,
                            framing,
                            len,
                        );
                    }
                    httparse::Status::Partial => {
                        assert!(self.buf.len() < MAX_HEAD, "request head too large");
                    }
                }
            }
            if !self.fill().await {
                return None;
            }
        };
        self.buf.advance(head_len);

        let body = match framing {
            BodyFraming::Length(len) => {
                let len = usize::try_from(len).unwrap();
                while self.buf.len() < len {
                    if !self.fill().await {
                        return None;
                    }
                }
                self.buf.split_to(len).freeze()
            }
            BodyFraming::Chunked => {
                let mut decoder = ChunkedDecoder::new();
                let mut body = Vec::new();
                loop {
                    let progress = decoder
                        .decode(&self.buf, |data| body.extend_from_slice(data))
                        .expect("malformed chunked request body");
                    self.buf.advance(progress.consumed);
                    if progress.done {
                        break;
                    }
                    if !self.fill().await {
                        return None;
                    }
                }
                Bytes::from(body)
            }
            BodyFraming::Unframed => unreachable!("requests are never unframed"),
        };
        Some(RecordedRequest {
            method,
            target,
            headers,
            body,
            conn: self.index,
        })
    }

    /// Reads more bytes into `buf`; false on EOF or a read error.
    async fn fill(&mut self) -> bool {
        matches!(self.stream.read_buf(&mut self.buf).await, Ok(n) if n > 0)
    }

    /// Sleeps for `delay` unless the peer closes first.
    async fn pause(&mut self, delay: Duration) -> Result<(), ConnEnd> {
        if delay.is_zero() {
            return Ok(());
        }
        let wait = sleep(delay);
        tokio::pin!(wait);
        loop {
            tokio::select! {
                () = &mut wait => return Ok(()),
                // Pipelined bytes stay in `buf` for the next request.
                open = self.fill() => if !open {
                    return Err(ConnEnd::PeerClosed);
                },
            }
        }
    }

    /// Waits until the peer closes the connection.
    async fn hang(&mut self) -> ConnEnd {
        while self.fill().await {}
        ConnEnd::PeerClosed
    }

    async fn write(&mut self, bytes: &[u8]) -> Result<(), ConnEnd> {
        self.stream
            .write_all(bytes)
            .await
            .map_err(|_| ConnEnd::PeerClosed)
    }

    async fn reply(&mut self, reply: Reply, head_request: bool) -> Result<(), ConnEnd> {
        match reply {
            Reply::Status {
                status,
                headers,
                body,
            } => self.full(status, &headers, &body, head_request).await,
            Reply::Json {
                head_delay,
                mut headers,
                body,
            } => {
                self.pause(head_delay).await?;
                default_content_type(&mut headers, "application/json");
                self.full(200, &headers, &body, head_request).await
            }
            Reply::Redirect { status, location } => {
                self.full(status, &[("location", location)], &[], head_request)
                    .await
            }
            Reply::Sse {
                head_delay,
                mut headers,
                frames,
                end,
            } => {
                self.pause(head_delay).await?;
                default_content_type(&mut headers, "text/event-stream");
                let mut out = Vec::new();
                http1::write_response_head(
                    &mut out,
                    200,
                    "OK",
                    &as_pairs(&headers),
                    BodyFraming::Chunked,
                );
                self.write(&out).await?;
                for (delay, frame) in frames {
                    self.pause(delay).await?;
                    out.clear();
                    http1::write_chunk(&mut out, &frame);
                    self.write(&out).await?;
                }
                match end {
                    SseEnd::Finish => self.write(http1::LAST_CHUNK).await,
                    SseEnd::Close => {
                        // Graceful FIN without the terminating chunk.
                        let _ = self.stream.shutdown().await;
                        Err(ConnEnd::Closed)
                    }
                    SseEnd::Hang => Err(self.hang().await),
                }
            }
            Reply::Reset => {
                self.stream.set_zero_linger().unwrap();
                Err(ConnEnd::Closed)
            }
            Reply::Hang => Err(self.hang().await),
        }
    }

    /// A complete response with a `Content-Length` body (omitted for HEAD).
    async fn full(
        &mut self,
        status: u16,
        headers: &[(&'static str, String)],
        body: &[u8],
        head_request: bool,
    ) -> Result<(), ConnEnd> {
        let reason = http::StatusCode::from_u16(status)
            .ok()
            .and_then(|s| s.canonical_reason())
            .unwrap_or("");
        let mut out = Vec::with_capacity(256 + body.len());
        http1::write_response_head(
            &mut out,
            status,
            reason,
            &as_pairs(headers),
            BodyFraming::Length(u64::try_from(body.len()).unwrap()),
        );
        if !head_request {
            out.extend_from_slice(body);
        }
        self.write(&out).await
    }
}

fn default_content_type(headers: &mut Vec<(&'static str, String)>, value: &str) {
    if !headers
        .iter()
        .any(|(name, _)| name.eq_ignore_ascii_case("content-type"))
    {
        headers.push(("content-type", value.to_owned()));
    }
}

fn as_pairs<'a>(headers: &'a [(&'static str, String)]) -> Vec<(&'a str, &'a str)> {
    headers.iter().map(|(n, v)| (*n, v.as_str())).collect()
}

/// How [`frames_of`] cuts a recorded body into frames.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Split {
    /// One frame with everything.
    Whole,
    /// One frame per SSE event, each ending with the blank line that ends
    /// the event; trailing bytes after the last event form a last frame.
    PerEvent,
    /// Seeded random cut points with piece lengths uniform in
    /// `min_piece..=max_piece`; the last piece may be shorter.
    Random {
        seed: u64,
        min_piece: usize,
        max_piece: usize,
    },
}

/// Splits a fixture into frames: whole, per event, or at seeded random
/// points with piece lengths uniform in `min_piece..=max_piece`.
///
/// # Panics
///
/// For `Split::Random` with `min_piece == 0` or `min_piece > max_piece`.
pub(crate) fn frames_of(bytes: &[u8], split: Split) -> Vec<Bytes> {
    let bytes = Bytes::copy_from_slice(bytes);
    if bytes.is_empty() {
        return Vec::new();
    }
    match split {
        Split::Whole => vec![bytes],
        Split::PerEvent => {
            let cuts = event_ends(&bytes);
            let mut frames = Vec::with_capacity(cuts.len() + 1);
            let mut start = 0;
            for end in cuts {
                frames.push(bytes.slice(start..end));
                start = end;
            }
            if start < bytes.len() {
                frames.push(bytes.slice(start..));
            }
            frames
        }
        Split::Random {
            seed,
            min_piece,
            max_piece,
        } => {
            assert!(
                min_piece >= 1 && min_piece <= max_piece,
                "invalid piece range {min_piece}..={max_piece}"
            );
            let mut rng = fastrand::Rng::with_seed(seed);
            let mut frames = Vec::new();
            let mut start = 0;
            while start < bytes.len() {
                let end = (start + rng.usize(min_piece..=max_piece)).min(bytes.len());
                frames.push(bytes.slice(start..end));
                start = end;
            }
            frames
        }
    }
}

/// A reply replaying a recorded response: status and headers from a
/// `.headers` fixture (parsed with httparse, which accepts both CRLF and LF;
/// `Transfer-Encoding`, `Content-Length` and `Date` are dropped because the
/// scripted server writes its own framing), body from the matching fixture.
/// By status and content-type: SSE becomes `Reply::Sse` with zero delays and
/// frames cut by `split`, other 2xx `Reply::Json`, non-2xx `Reply::Status`;
/// `split` only applies to SSE.
///
/// # Panics
///
/// If `headers` is not a complete response head, a header value is not
/// UTF-8, or the status is a 2xx other than 200 (`Reply::Json` and
/// `Reply::Sse` always answer 200, so replaying it would change the status).
pub(crate) fn fixture_reply(headers: &[u8], body: &[u8], split: Split) -> Reply {
    let mut storage = [httparse::EMPTY_HEADER; MAX_HEADERS];
    let mut response = httparse::Response::new(&mut storage);
    let status = response
        .parse(headers)
        .expect("fixture response head must parse");
    assert!(status.is_complete(), "fixture response head is incomplete");
    let code = response.code.expect("complete head has a status code");

    let mut is_sse = false;
    let mut replayed = Vec::with_capacity(response.headers.len());
    for header in response.headers.iter() {
        let name = header.name;
        if ["transfer-encoding", "content-length", "date"]
            .iter()
            .any(|dropped| name.eq_ignore_ascii_case(dropped))
        {
            continue;
        }
        let value = std::str::from_utf8(header.value)
            .unwrap_or_else(|_| panic!("fixture header {name} is not UTF-8"));
        if name.eq_ignore_ascii_case("content-type") {
            is_sse = value
                .trim_start()
                .get(.."text/event-stream".len())
                .is_some_and(|media| media.eq_ignore_ascii_case("text/event-stream"));
        }
        // `Reply` names are `&'static str` so that hand-written scripts stay
        // literal; a fixture's handful of names leaked once per call is
        // negligible in a test process.
        let name: &'static str = Box::leak(name.to_owned().into_boxed_str());
        replayed.push((name, value.to_owned()));
    }

    if !(200..300).contains(&code) {
        return Reply::Status {
            status: code,
            headers: replayed,
            body: Bytes::copy_from_slice(body),
        };
    }
    assert_eq!(code, 200, "fixture 2xx status {code} cannot be replayed");
    if is_sse {
        Reply::Sse {
            head_delay: Duration::ZERO,
            headers: replayed,
            frames: frames_of(body, split)
                .into_iter()
                .map(|frame| (Duration::ZERO, frame))
                .collect(),
            end: SseEnd::Finish,
        }
    } else {
        Reply::Json {
            head_delay: Duration::ZERO,
            headers: replayed,
            body: Bytes::copy_from_slice(body),
        }
    }
}

/// Offsets just past each blank line that ends a non-empty event. Line ends
/// are `\r\n`, `\n` or `\r`, as in the SSE specification.
fn event_ends(bytes: &[u8]) -> Vec<usize> {
    let mut ends = Vec::new();
    let mut line_start = 0;
    let mut event_has_lines = false;
    let mut i = 0;
    while i < bytes.len() {
        let eol_len = match bytes[i] {
            b'\r' if bytes.get(i + 1) == Some(&b'\n') => 2,
            b'\r' | b'\n' => 1,
            _ => {
                i += 1;
                continue;
            }
        };
        let blank = i == line_start;
        i += eol_len;
        line_start = i;
        if !blank {
            event_has_lines = true;
        } else if event_has_lines {
            ends.push(i);
            event_has_lines = false;
        }
    }
    ends
}
