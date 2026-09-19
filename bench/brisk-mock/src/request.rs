//! Incremental HTTP/1.1 request reading and request-body inspection.
//!
//! [`RequestReader`] consumes bytes from the front of a connection's receive
//! buffer and yields one complete request at a time, which keeps pipelined
//! requests that follow it untouched in the buffer. Bodies are accepted with
//! `Content-Length` or chunked framing.

use brisk_bench_core::http1::{self, BodyFraming, ChunkedDecoder, Http1Error};
use brisk_bench_core::transport::DEFAULT_READ_SIZE;
use brisk_bench_core::wire::{self, BenchParams, WireError};
use memchr::memmem;

/// Maximum number of request headers.
pub(crate) const MAX_HEADERS: usize = 64;
/// Maximum size of a request head, request line included.
pub(crate) const MAX_HEAD_BYTES: usize = 64 * 1024;
/// Maximum size of a request body.
pub(crate) const MAX_BODY_BYTES: usize = 64 * 1024 * 1024;

/// Why a request could not be read. Every variant ends the connection,
/// because the request boundary is lost.
#[derive(Debug, thiserror::Error)]
pub(crate) enum RequestError {
    /// The head is not valid HTTP/1.x.
    #[error("malformed request head: {0}")]
    Head(#[from] httparse::Error),
    /// The head exceeds [`MAX_HEAD_BYTES`] or [`MAX_HEADERS`].
    #[error("request head too large")]
    HeadTooLarge,
    /// Only HTTP/1.1 is served: streaming responses need chunked encoding.
    #[error("unsupported HTTP version 1.{0}")]
    Version(u8),
    /// Body framing headers are invalid or the chunked body is malformed.
    #[error("invalid body framing: {0}")]
    Framing(#[from] Http1Error),
    /// The body exceeds [`MAX_BODY_BYTES`].
    #[error("request body too large")]
    BodyTooLarge,
}

/// Request method, reduced to what routing needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Method {
    /// `GET`.
    Get,
    /// `POST`.
    Post,
    /// Anything else.
    Other,
}

/// Request target, reduced to the served endpoints.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Route {
    /// `/v1/chat/completions`.
    ChatCompletions,
    /// `/v1/models`.
    Models,
    /// `/__bench/stats`.
    Stats,
    /// `/__bench/reset`.
    Reset,
    /// Any other path.
    Unknown,
}

impl Route {
    fn from_target(target: &str) -> Self {
        let path = target.split_once('?').map_or(target, |(path, _)| path);
        match path {
            "/v1/chat/completions" => Self::ChatCompletions,
            "/v1/models" => Self::Models,
            "/__bench/stats" => Self::Stats,
            "/__bench/reset" => Self::Reset,
            _ => Self::Unknown,
        }
    }
}

/// The parts of a request head the mock acts on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Head {
    /// Request method.
    pub(crate) method: Method,
    /// Request target.
    pub(crate) route: Route,
    /// Whether the connection stays open after the response.
    pub(crate) keep_alive: bool,
    /// Whether the client waits for `100 Continue` before sending the body.
    pub(crate) expect_continue: bool,
    /// Body framing.
    pub(crate) framing: BodyFraming,
}

impl Head {
    /// Parses a complete head. Returns `Ok(None)` if more bytes are needed and
    /// otherwise the head together with its length in bytes.
    pub(crate) fn parse(buf: &[u8]) -> Result<Option<(Self, usize)>, RequestError> {
        let mut headers = [httparse::EMPTY_HEADER; MAX_HEADERS];
        let mut req = httparse::Request::new(&mut headers);
        let len = match req.parse(buf) {
            Ok(httparse::Status::Complete(len)) => len,
            Ok(httparse::Status::Partial) if buf.len() > MAX_HEAD_BYTES => {
                return Err(RequestError::HeadTooLarge);
            }
            Ok(httparse::Status::Partial) => return Ok(None),
            Err(httparse::Error::TooManyHeaders) => return Err(RequestError::HeadTooLarge),
            Err(err) => return Err(err.into()),
        };
        if len > MAX_HEAD_BYTES {
            return Err(RequestError::HeadTooLarge);
        }
        let version = req.version.expect("complete request has a version");
        if version != 1 {
            return Err(RequestError::Version(version));
        }
        let method = match req.method.expect("complete request has a method") {
            "GET" => Method::Get,
            "POST" => Method::Post,
            _ => Method::Other,
        };
        let route = Route::from_target(req.path.expect("complete request has a path"));
        let framing = http1::request_body_framing(req.headers)?;
        let mut keep_alive = true;
        let mut expect_continue = false;
        for header in &*req.headers {
            if header.name.eq_ignore_ascii_case("connection") {
                keep_alive &= !has_token(header.value, b"close");
            } else if header.name.eq_ignore_ascii_case("expect") {
                expect_continue |= header
                    .value
                    .trim_ascii()
                    .eq_ignore_ascii_case(b"100-continue");
            }
        }
        Ok(Some((
            Self {
                method,
                route,
                keep_alive,
                expect_continue,
                framing,
            },
            len,
        )))
    }
}

/// Whether a comma-separated header value contains `token`
/// (case-insensitive).
fn has_token(value: &[u8], token: &[u8]) -> bool {
    value
        .split(|&b| b == b',')
        .any(|t| t.trim_ascii().eq_ignore_ascii_case(token))
}

/// What the reader produced from the buffered input.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Progress {
    /// More input is needed.
    NeedMore,
    /// A head announcing `Expect: 100-continue` was just parsed; send the
    /// interim response, then call [`RequestReader::advance`] again.
    Continue,
    /// A complete request. Its body is available through
    /// [`RequestReader::body`] until [`RequestReader::finish`] or the next
    /// `advance`.
    Request(Head),
}

#[derive(Debug)]
enum State {
    Head,
    Length {
        head: Head,
        len: usize,
    },
    Chunked {
        head: Head,
        decoder: ChunkedDecoder,
    },
    /// A request was returned; its body (in `rx` unless chunked) is still
    /// to be discarded.
    Complete {
        body_in_rx: usize,
    },
}

/// Reads requests one at a time from the front of a receive buffer.
#[derive(Debug)]
pub(crate) struct RequestReader {
    state: State,
    /// Decoded chunked body. `Content-Length` bodies are used in place in the
    /// receive buffer so large uploads are never copied.
    chunked_body: Vec<u8>,
}

impl Default for RequestReader {
    fn default() -> Self {
        Self::new()
    }
}

/// Buffers above this capacity are released after a request instead of being
/// kept for reuse, so a few large uploads do not pin memory per connection.
const RETAIN_CAPACITY: usize = 1024 * 1024;

impl RequestReader {
    /// Creates a reader expecting a request head.
    pub(crate) fn new() -> Self {
        Self {
            state: State::Head,
            chunked_body: Vec::new(),
        }
    }

    /// Whether no part of a request has been consumed yet, i.e. the
    /// connection is between requests.
    pub(crate) fn is_idle(&self, rx: &[u8]) -> bool {
        matches!(self.state, State::Head) && rx.is_empty()
    }

    /// Discards the body of the request last returned by
    /// [`advance`](Self::advance), releasing oversized buffers. A no-op when
    /// no request is complete.
    ///
    /// Call it as soon as the body has been inspected: a streaming response
    /// can last minutes, and the body must not stay buffered that long.
    pub(crate) fn finish(&mut self, rx: &mut Vec<u8>) {
        let State::Complete { body_in_rx } = self.state else {
            return;
        };
        rx.drain(..body_in_rx);
        if rx.is_empty() && rx.capacity() > RETAIN_CAPACITY {
            *rx = Vec::new();
        }
        self.chunked_body.clear();
        if self.chunked_body.capacity() > RETAIN_CAPACITY {
            self.chunked_body = Vec::new();
        }
        self.state = State::Head;
    }

    /// Consumes input from the front of `rx` and reports what is available.
    ///
    /// After [`Progress::Request`] the body stays readable through
    /// [`body`](Self::body) until [`finish`](Self::finish) or the next call,
    /// which first discards it.
    pub(crate) fn advance(&mut self, rx: &mut Vec<u8>) -> Result<Progress, RequestError> {
        loop {
            match &mut self.state {
                State::Complete { .. } => self.finish(rx),
                State::Head => {
                    let Some((head, len)) = Head::parse(rx)? else {
                        return Ok(Progress::NeedMore);
                    };
                    rx.drain(..len);
                    self.state = match head.framing {
                        BodyFraming::Chunked => State::Chunked {
                            head,
                            decoder: ChunkedDecoder::new(),
                        },
                        BodyFraming::Length(len) => {
                            let len = usize::try_from(len)
                                .ok()
                                .filter(|&len| len <= MAX_BODY_BYTES)
                                .ok_or(RequestError::BodyTooLarge)?;
                            // One allocation for the whole body (plus the
                            // spare room a receive asks for) instead of
                            // repeated doubling, each copying what arrived.
                            rx.reserve((len + DEFAULT_READ_SIZE).saturating_sub(rx.len()));
                            State::Length { head, len }
                        }
                        BodyFraming::Unframed => {
                            unreachable!("request framing is never unframed")
                        }
                    };
                    let has_body = !matches!(head.framing, BodyFraming::Length(0));
                    if head.expect_continue && has_body {
                        return Ok(Progress::Continue);
                    }
                }
                State::Length { head, len } => {
                    if rx.len() < *len {
                        return Ok(Progress::NeedMore);
                    }
                    let head = *head;
                    self.state = State::Complete { body_in_rx: *len };
                    return Ok(Progress::Request(head));
                }
                State::Chunked { head, decoder } => {
                    let body = &mut self.chunked_body;
                    let decoded = decoder.decode(rx, |data| body.extend_from_slice(data))?;
                    rx.drain(..decoded.consumed);
                    if body.len() > MAX_BODY_BYTES {
                        return Err(RequestError::BodyTooLarge);
                    }
                    if !decoded.done {
                        return Ok(Progress::NeedMore);
                    }
                    let head = *head;
                    self.state = State::Complete { body_in_rx: 0 };
                    return Ok(Progress::Request(head));
                }
            }
        }
    }

    /// The body of the request last returned by [`advance`](Self::advance).
    /// Empty when no request is complete.
    pub(crate) fn body<'a>(&'a self, rx: &'a [u8]) -> &'a [u8] {
        match self.state {
            State::Complete { body_in_rx: 0, .. } => &self.chunked_body,
            State::Complete { body_in_rx, .. } => &rx[..body_in_rx],
            _ => &[],
        }
    }
}

/// What the mock needs from a chat completion request body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ChatRequest {
    /// Parameters from the `bench:v1;` directive, if the body has one.
    pub(crate) params: Option<BenchParams>,
    /// `"stream": true`.
    pub(crate) stream: bool,
    /// `"include_usage": true` (inside `stream_options`).
    pub(crate) include_usage: bool,
    /// Body length, for the synthetic prompt token count.
    pub(crate) body_len: usize,
}

impl ChatRequest {
    /// Inspects a whole body in one go.
    #[cfg(test)]
    pub(crate) fn inspect(body: &[u8]) -> Result<Self, WireError> {
        let mut inspector = Inspector::new();
        loop {
            if let Some(result) = inspector.step(body) {
                return result;
            }
        }
    }
}

/// Body bytes examined per [`Inspector::step`]. Scanning runs at several
/// GB/s, so one window takes a few microseconds, well inside the spin window.
pub(crate) const INSPECT_WINDOW: usize = 256 * 1024;

const STREAM_KEY: &[u8] = b"\"stream\"";
const INCLUDE_USAGE_KEY: &[u8] = b"\"include_usage\"";

/// Incremental inspection of a raw request body into a [`ChatRequest`],
/// without a full JSON parse.
///
/// The mock runs on a benchmark core and sees bodies of up to megabytes, so
/// it scans for the few keys it needs instead of building a JSON tree, one
/// [`INSPECT_WINDOW`] at a time so the caller can serve due emissions in
/// between. A key is recognised as `"key"`, optional whitespace, `:`,
/// optional whitespace and a JSON boolean; the first such occurrence wins.
/// The scan stops as soon as every key is resolved, which for bodies built
/// by [`wire::chat_request_body`] is within the first window.
#[derive(Debug, Default)]
pub(crate) struct Inspector {
    /// Start of the next window.
    pos: usize,
    /// Offset of the `bench:v1;` directive.
    directive: Option<usize>,
    stream: Option<bool>,
    include_usage: Option<bool>,
}

impl Inspector {
    /// Starts at the beginning of a body.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Scans the next window of `body`. Returns the outcome once inspection
    /// is complete; `None` asks for another call with the same body.
    pub(crate) fn step(&mut self, body: &[u8]) -> Option<Result<ChatRequest, WireError>> {
        let start = self.pos;
        let end = start.saturating_add(INSPECT_WINDOW).min(body.len());
        if self.directive.is_none() {
            self.directive = find_in_window(body, start, end, wire::DIRECTIVE_PREFIX);
        }
        if self.stream.is_none() {
            self.stream = find_bool_in_window(body, start, end, STREAM_KEY);
        }
        // `include_usage` only matters for streams, so a request known not to
        // stream is not scanned for it any further.
        if self.include_usage.is_none() && self.stream != Some(false) {
            self.include_usage = find_bool_in_window(body, start, end, INCLUDE_USAGE_KEY);
        }
        self.pos = end;
        let resolved = self.directive.is_some()
            && match self.stream {
                Some(true) => self.include_usage.is_some(),
                Some(false) => true,
                None => false,
            };
        if end < body.len() && !resolved {
            return None;
        }
        let params = match self.directive {
            Some(at) => wire::find_directive(&body[at..]),
            None => Ok(None),
        };
        let stream = self.stream == Some(true);
        Some(params.map(|params| ChatRequest {
            params,
            stream,
            include_usage: stream && self.include_usage == Some(true),
            body_len: body.len(),
        }))
    }
}

/// The slice of `body` from `start` that holds every occurrence of a
/// `needle_len`-byte needle starting in `start..end`, and no other.
fn window(body: &[u8], start: usize, end: usize, needle_len: usize) -> &[u8] {
    let stop = end.saturating_add(needle_len - 1).min(body.len());
    &body[start..stop.max(start)]
}

/// Offset of the first `needle` starting in `start..end`.
fn find_in_window(body: &[u8], start: usize, end: usize, needle: &[u8]) -> Option<usize> {
    memmem::find(window(body, start, end, needle.len()), needle).map(|at| start + at)
}

/// Value of the first `<quoted_key>: true|false` whose key starts in
/// `start..end`. The value itself may lie beyond `end`.
fn find_bool_in_window(body: &[u8], start: usize, end: usize, quoted_key: &[u8]) -> Option<bool> {
    memmem::find_iter(window(body, start, end, quoted_key.len()), quoted_key).find_map(|at| {
        let rest = body[start + at + quoted_key.len()..].trim_ascii_start();
        let rest = rest.strip_prefix(b":")?.trim_ascii_start();
        if rest.starts_with(b"true") {
            Some(true)
        } else if rest.starts_with(b"false") {
            Some(false)
        } else {
            None
        }
    })
}

#[cfg(test)]
mod tests {
    use brisk_bench_core::wire::{BodyShape, chat_request_body};

    use super::*;

    fn params() -> BenchParams {
        BenchParams {
            ttft_us: 1000,
            interval_us: 2000,
            chunks: 3,
            chunk_bytes: 100,
            sid: 42,
            resp_bytes: 200,
        }
    }

    fn expect_request(reader: &mut RequestReader, rx: &mut Vec<u8>) -> Head {
        match reader.advance(rx).unwrap() {
            Progress::Request(head) => head,
            other => panic!("expected a request, got {other:?}"),
        }
    }

    #[test]
    fn pipelined_requests_are_returned_in_order() {
        let body = chat_request_body("m", true, true, &params(), BodyShape::Text { bytes: 10 });
        let mut rx = format!(
            "POST /v1/chat/completions HTTP/1.1\r\nHost: x\r\nContent-Length: {}\r\n\r\n",
            body.len()
        )
        .into_bytes();
        rx.extend_from_slice(&body);
        rx.extend_from_slice(
            b"GET /v1/models?x=1 HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n",
        );

        let mut reader = RequestReader::new();
        let head = expect_request(&mut reader, &mut rx);
        assert_eq!(head.method, Method::Post);
        assert_eq!(head.route, Route::ChatCompletions);
        assert!(head.keep_alive);
        assert_eq!(reader.body(&rx), body.as_slice());

        let head = expect_request(&mut reader, &mut rx);
        assert_eq!(head.route, Route::Models);
        assert!(!head.keep_alive);
        assert!(reader.body(&rx).is_empty());
        assert_eq!(reader.advance(&mut rx).unwrap(), Progress::NeedMore);
        assert!(reader.is_idle(&rx));
    }

    #[test]
    fn chunked_body_split_at_every_byte() {
        let body = chat_request_body("m", false, false, &params(), BodyShape::Text { bytes: 50 });
        let mut wire =
            b"POST /v1/chat/completions HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n".to_vec();
        for part in body.chunks(17) {
            http1::write_chunk(&mut wire, part);
        }
        wire.extend_from_slice(http1::LAST_CHUNK);

        let mut reader = RequestReader::new();
        let mut rx = Vec::new();
        let mut got = None;
        for &byte in &wire {
            rx.push(byte);
            if let Progress::Request(head) = reader.advance(&mut rx).unwrap() {
                got = Some((head, reader.body(&rx).to_vec()));
            }
        }
        let (head, got_body) = got.expect("request completes");
        assert_eq!(head.framing, BodyFraming::Chunked);
        assert_eq!(got_body, body);
        let chat = ChatRequest::inspect(&got_body).unwrap();
        assert_eq!(chat.params, Some(params()));
        assert!(!chat.stream);
    }

    #[test]
    fn expect_continue_is_reported_before_the_body() {
        let mut rx =
            b"POST /v1/chat/completions HTTP/1.1\r\nContent-Length: 2\r\nExpect: 100-continue\r\n\r\n"
                .to_vec();
        let mut reader = RequestReader::new();
        assert_eq!(reader.advance(&mut rx).unwrap(), Progress::Continue);
        assert_eq!(reader.advance(&mut rx).unwrap(), Progress::NeedMore);
        rx.extend_from_slice(b"{}");
        expect_request(&mut reader, &mut rx);
    }

    #[test]
    fn limits_and_versions_are_enforced() {
        let mut reader = RequestReader::new();
        let mut rx = format!(
            "POST / HTTP/1.1\r\nContent-Length: {}\r\n\r\n",
            MAX_BODY_BYTES + 1
        )
        .into_bytes();
        assert!(matches!(
            reader.advance(&mut rx),
            Err(RequestError::BodyTooLarge)
        ));

        let mut reader = RequestReader::new();
        let mut rx = b"GET / HTTP/1.0\r\n\r\n".to_vec();
        assert!(matches!(
            reader.advance(&mut rx),
            Err(RequestError::Version(0))
        ));

        let mut reader = RequestReader::new();
        let mut rx = b"GET / HTTP/1.1\r\nX: ".to_vec();
        rx.resize(MAX_HEAD_BYTES + 1, b'a');
        assert!(matches!(
            reader.advance(&mut rx),
            Err(RequestError::HeadTooLarge)
        ));
    }

    #[test]
    fn inspect_finds_stream_flags_with_whitespace() {
        let body = br#"{"model":"m", "stream" : true, "stream_options": {"include_usage":  true}}"#;
        let chat = ChatRequest::inspect(body).unwrap();
        assert!(chat.stream);
        assert!(chat.include_usage);
        assert_eq!(chat.params, None);

        let generated =
            chat_request_body("m", true, false, &params(), BodyShape::Text { bytes: 5 });
        let chat = ChatRequest::inspect(&generated).unwrap();
        assert!(chat.stream);
        assert!(!chat.include_usage);
        assert_eq!(chat.params, Some(params()));
    }

    #[test]
    fn inspection_is_windowed_and_finds_keys_across_boundaries() {
        // Keys far into the body and straddling window boundaries are found,
        // and each step covers at most one window.
        let directive = params().to_directive();
        let mut body = b"{\"messages\":[{\"content\":\"".to_vec();
        body.resize(INSPECT_WINDOW - 4, b'a');
        body.extend_from_slice(directive.as_bytes());
        body.extend_from_slice(b"\"}],");
        body.resize(2 * INSPECT_WINDOW - 3, b' ');
        body.extend_from_slice(br#""stream" :  true, "#);
        body.resize(3 * INSPECT_WINDOW + 10, b' ');
        body.extend_from_slice(br#""include_usage":true}"#);

        let mut inspector = Inspector::new();
        let mut steps = 0;
        let chat = loop {
            steps += 1;
            if let Some(result) = inspector.step(&body) {
                break result.unwrap();
            }
        };
        assert_eq!(steps, 4);
        assert_eq!(chat.params, Some(params()));
        assert!(chat.stream);
        assert!(chat.include_usage);
        assert_eq!(chat.body_len, body.len());
    }

    #[test]
    fn inspection_stops_once_every_key_is_resolved() {
        let body = chat_request_body(
            "m",
            false,
            false,
            &params(),
            BodyShape::Image {
                base64_bytes: 4 * INSPECT_WINDOW,
                text_bytes: 10,
            },
        );
        let mut inspector = Inspector::new();
        let chat = inspector
            .step(&body)
            .expect("resolved in one window")
            .unwrap();
        assert!(!chat.stream);
        assert!(!chat.include_usage);

        // Without a directive the whole body has to be scanned.
        let mut body = br#"{"stream":false,"messages":[]"#.to_vec();
        body.resize(2 * INSPECT_WINDOW, b' ');
        let mut inspector = Inspector::new();
        assert!(inspector.step(&body).is_none());
        let chat = inspector.step(&body).expect("end of body").unwrap();
        assert_eq!(chat.params, None);
    }

    #[test]
    fn finish_releases_the_body_and_keeps_pipelined_input() {
        let body = vec![b'x'; 2 * RETAIN_CAPACITY];
        let mut rx = format!(
            "POST /v1/chat/completions HTTP/1.1\r\nContent-Length: {}\r\n\r\n",
            body.len()
        )
        .into_bytes();
        let mut reader = RequestReader::new();
        assert_eq!(reader.advance(&mut rx).unwrap(), Progress::NeedMore);
        assert!(rx.capacity() >= body.len());
        rx.extend_from_slice(&body);
        expect_request(&mut reader, &mut rx);
        assert_eq!(reader.body(&rx).len(), body.len());

        reader.finish(&mut rx);
        assert!(rx.is_empty());
        assert_eq!(rx.capacity(), 0);
        assert!(reader.body(&rx).is_empty());
        assert!(reader.is_idle(&rx));

        let mut rx = b"GET /v1/models HTTP/1.1\r\n\r\nGET /v1/models HTTP/1.1\r\n\r\n".to_vec();
        let mut reader = RequestReader::new();
        expect_request(&mut reader, &mut rx);
        reader.finish(&mut rx);
        reader.finish(&mut rx);
        expect_request(&mut reader, &mut rx);
        assert_eq!(reader.advance(&mut rx).unwrap(), Progress::NeedMore);
    }

    #[test]
    fn invalid_directive_is_an_error() {
        let body = br#"{"messages":[{"role":"system","content":"bench:v1;ttft_us=1"}]}"#;
        assert!(ChatRequest::inspect(body).is_err());
    }
}
