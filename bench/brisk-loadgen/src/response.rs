//! Incremental HTTP/1.1 response parsing for one request at a time.
//!
//! Plaintext is fed in whatever pieces the socket delivers. The head is
//! parsed with `httparse`; the body is de-framed (chunked, `Content-Length`
//! or until EOF) without copying, and for every successful (200) response the
//! decoded bytes go through a [`MarkerScanner`] that validates every marker
//! against the request it belongs to. A stream is complete when the body
//! ends; it is only accepted if every requested chunk arrived and the last
//! SSE event was `data: [DONE]`. A whole (non-streaming) response carries at
//! most one marker, and exactly one when its content is long enough to hold
//! it.

use brisk_bench_core::http1::{BodyFraming, ChunkedDecoder, Http1Error, response_body_framing};
use brisk_bench_core::wire::{Marker, MarkerScanner, WireError};

/// Largest accepted response head.
const MAX_HEAD: usize = 64 * 1024;
/// Header slots offered to `httparse`.
const MAX_HEADERS: usize = 64;
/// Bytes of decoded body kept to check the final SSE event.
const TAIL_LEN: usize = 32;
/// The final SSE event of an `OpenAI` stream.
const DONE_EVENT: &[u8] = b"data: [DONE]";

/// What a request expects back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Expect {
    /// An SSE stream with markers of stream `sid`, `chunks` of them.
    Stream {
        /// Stream id every marker must carry.
        sid: u64,
        /// Number of content chunks requested.
        chunks: u32,
    },
    /// A complete response body.
    Whole {
        /// Stream id the marker must carry.
        sid: u64,
        /// The content is long enough for the mock to embed one marker.
        marker: bool,
    },
}

impl Expect {
    /// Stream id of the request and the number of markers its successful
    /// response carries.
    fn markers(self) -> (u64, u32) {
        match self {
            Self::Stream { sid, chunks } => (sid, chunks),
            Self::Whole { sid, marker } => (sid, u32::from(marker)),
        }
    }
}

/// A fully received response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Completion {
    /// Status code.
    pub(crate) status: u16,
    /// Whether the connection may carry another request.
    pub(crate) keep_alive: bool,
}

/// Why a response was rejected.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub(crate) enum ResponseError {
    /// `httparse` rejected the head.
    #[error("malformed response head: {0}")]
    Head(httparse::Error),
    /// The head exceeds [`MAX_HEAD`].
    #[error("response head exceeds {MAX_HEAD} bytes")]
    HeadTooLarge,
    /// Invalid framing headers or chunked encoding.
    #[error("invalid body framing: {0}")]
    Framing(#[from] Http1Error),
    /// A marker prefix followed by an invalid marker.
    #[error("malformed marker: {0}")]
    Marker(#[from] WireError),
    /// A marker of another stream.
    #[error("marker of stream {got} in stream {expected}")]
    ForeignMarker {
        /// The stream id of this request.
        expected: u64,
        /// The stream id found in the marker.
        got: u64,
    },
    /// Markers out of sequence (lost, duplicated or reordered chunks).
    #[error("marker seq {got} where {expected} was due")]
    OutOfOrder {
        /// Next sequence number due.
        expected: u32,
        /// Sequence number found.
        got: u32,
    },
    /// More markers than the request asked for.
    #[error("marker beyond the {limit} requested")]
    ExcessMarker {
        /// Markers requested.
        limit: u32,
    },
    /// A whole response without the marker its content had room for.
    #[error("response carries no marker")]
    MissingMarker,
    /// The stream ended with fewer markers than chunks requested.
    #[error("stream ended after {got} of {expected} chunks")]
    ShortStream {
        /// Chunks requested.
        expected: u32,
        /// Markers received.
        got: u32,
    },
    /// The stream ended without a final `data: [DONE]` event.
    #[error("stream ended without data: [DONE]")]
    MissingDone,
    /// The server sent bytes after the end of the response.
    #[error("bytes after the end of the response")]
    TrailingBytes,
    /// The connection closed before the response was complete.
    #[error("connection closed mid-response")]
    Truncated,
}

impl ResponseError {
    /// Error kind recorded in the interval statistics.
    pub(crate) fn kind(&self) -> &'static str {
        match self {
            Self::Head(_) | Self::HeadTooLarge | Self::Framing(_) | Self::TrailingBytes => {
                "bad_response"
            }
            Self::Marker(_) => "bad_marker",
            Self::ForeignMarker { .. } | Self::OutOfOrder { .. } | Self::ExcessMarker { .. } => {
                "marker_mismatch"
            }
            Self::ShortStream { .. } => "short_stream",
            Self::MissingMarker => "missing_marker",
            Self::MissingDone => "missing_done",
            Self::Truncated => "eof",
        }
    }
}

#[derive(Debug)]
enum Framing {
    Chunked(ChunkedDecoder),
    Length(u64),
    UntilEof,
}

#[derive(Debug)]
enum State {
    Head,
    Body {
        framing: Framing,
        status: u16,
        keep_alive: bool,
    },
    Done,
}

/// The last [`TAIL_LEN`] decoded body bytes.
#[derive(Debug)]
struct Tail {
    buf: [u8; TAIL_LEN],
    len: usize,
}

impl Tail {
    const fn new() -> Self {
        Self {
            buf: [0; TAIL_LEN],
            len: 0,
        }
    }

    fn push(&mut self, data: &[u8]) {
        if data.len() >= TAIL_LEN {
            self.buf.copy_from_slice(&data[data.len() - TAIL_LEN..]);
            self.len = TAIL_LEN;
            return;
        }
        let keep = self.len.min(TAIL_LEN - data.len());
        self.buf.copy_within(self.len - keep..self.len, 0);
        self.buf[keep..keep + data.len()].copy_from_slice(data);
        self.len = keep + data.len();
    }

    fn ends_with_done(&self) -> bool {
        self.buf[..self.len].trim_ascii_end().ends_with(DONE_EVENT)
    }
}

/// Consumer of decoded body bytes.
#[derive(Debug)]
struct Sink {
    expect: Expect,
    /// Scan for markers: the response status is 200.
    scan: bool,
    scanner: MarkerScanner,
    next_seq: u32,
    tail: Tail,
}

impl Sink {
    fn data<F: FnMut(Marker)>(
        &mut self,
        data: &[u8],
        on_marker: &mut F,
    ) -> Result<(), ResponseError> {
        if !self.scan {
            return Ok(());
        }
        if matches!(self.expect, Expect::Stream { .. }) {
            self.tail.push(data);
        }
        let (sid, limit) = self.expect.markers();
        let mut mismatch = None;
        let next_seq = &mut self.next_seq;
        self.scanner.feed(data, |marker| {
            if mismatch.is_some() {
                return;
            }
            if marker.sid != sid {
                mismatch = Some(ResponseError::ForeignMarker {
                    expected: sid,
                    got: marker.sid,
                });
            } else if *next_seq >= limit {
                mismatch = Some(ResponseError::ExcessMarker { limit });
            } else if marker.seq != *next_seq {
                mismatch = Some(ResponseError::OutOfOrder {
                    expected: *next_seq,
                    got: marker.seq,
                });
            } else {
                *next_seq += 1;
                on_marker(marker);
            }
        })?;
        mismatch.map_or(Ok(()), Err)
    }

    fn finish(&self) -> Result<(), ResponseError> {
        if !self.scan {
            return Ok(());
        }
        match self.expect {
            Expect::Stream { chunks, .. } => {
                if self.next_seq != chunks {
                    return Err(ResponseError::ShortStream {
                        expected: chunks,
                        got: self.next_seq,
                    });
                }
                if !self.tail.ends_with_done() {
                    return Err(ResponseError::MissingDone);
                }
                Ok(())
            }
            Expect::Whole { marker, .. } => {
                if marker && self.next_seq == 0 {
                    return Err(ResponseError::MissingMarker);
                }
                Ok(())
            }
        }
    }
}

/// Parsed facts of a response head.
struct HeadInfo {
    len: usize,
    status: u16,
    framing: BodyFraming,
    keep_alive: bool,
}

fn parse_head(buf: &[u8]) -> Result<Option<HeadInfo>, ResponseError> {
    let mut headers = [httparse::EMPTY_HEADER; MAX_HEADERS];
    let mut response = httparse::Response::new(&mut headers);
    match response.parse(buf).map_err(ResponseError::Head)? {
        httparse::Status::Partial => Ok(None),
        httparse::Status::Complete(len) => {
            let status = response
                .code
                .expect("httparse sets the code of a complete head");
            let framing = response_body_framing(status, false, response.headers)?;
            let close = response.headers.iter().any(|h| {
                h.name.eq_ignore_ascii_case("connection")
                    && h.value
                        .split(|&b| b == b',')
                        .any(|token| token.trim_ascii().eq_ignore_ascii_case(b"close"))
            });
            let keep_alive =
                response.version == Some(1) && !close && framing != BodyFraming::Unframed;
            Ok(Some(HeadInfo {
                len,
                status,
                framing,
                keep_alive,
            }))
        }
    }
}

/// Parser for the response to the one request in flight on a connection.
#[derive(Debug)]
pub(crate) struct ResponseParser {
    state: State,
    head: Vec<u8>,
    sink: Sink,
    received_any: bool,
}

impl ResponseParser {
    /// Creates a parser for a response of the given kind.
    pub(crate) fn new(expect: Expect) -> Self {
        Self {
            state: State::Head,
            head: Vec::new(),
            sink: Sink {
                expect,
                scan: false,
                scanner: MarkerScanner::new(),
                next_seq: 0,
                tail: Tail::new(),
            },
            received_any: false,
        }
    }

    /// Prepares for the response to the next request on the connection.
    pub(crate) fn reset(&mut self, expect: Expect) {
        self.state = State::Head;
        self.head.clear();
        self.sink.expect = expect;
        self.sink.scan = false;
        self.sink.scanner.reset();
        self.sink.next_seq = 0;
        self.sink.tail = Tail::new();
        self.received_any = false;
    }

    /// Whether any byte of the response has arrived.
    pub(crate) fn received_any(&self) -> bool {
        self.received_any
    }

    /// Feeds received plaintext. `on_marker` is called for every valid
    /// marker in order. Returns the completion once the body has ended.
    ///
    /// After an error the parser must be reset (and the connection closed).
    pub(crate) fn feed<F: FnMut(Marker)>(
        &mut self,
        mut input: &[u8],
        mut on_marker: F,
    ) -> Result<Option<Completion>, ResponseError> {
        if input.is_empty() {
            return Ok(None);
        }
        self.received_any = true;
        loop {
            match &mut self.state {
                State::Head => {
                    let buffered = self.head.len();
                    let info = if buffered == 0 {
                        parse_head(input)?
                    } else {
                        self.head.extend_from_slice(input);
                        parse_head(&self.head)?
                    };
                    let Some(info) = info else {
                        if buffered == 0 {
                            self.head.extend_from_slice(input);
                        }
                        if self.head.len() > MAX_HEAD {
                            return Err(ResponseError::HeadTooLarge);
                        }
                        return Ok(None);
                    };
                    // The previously buffered bytes held no complete head, so
                    // the head ends inside `input`.
                    input = &input[info.len - buffered..];
                    self.head.clear();
                    if (100..200).contains(&info.status) {
                        // Interim response: the real one follows.
                        if input.is_empty() {
                            return Ok(None);
                        }
                        continue;
                    }
                    self.sink.scan = info.status == 200;
                    let framing = match info.framing {
                        BodyFraming::Chunked => Framing::Chunked(ChunkedDecoder::new()),
                        BodyFraming::Length(n) => Framing::Length(n),
                        BodyFraming::Unframed => Framing::UntilEof,
                    };
                    self.state = State::Body {
                        framing,
                        status: info.status,
                        keep_alive: info.keep_alive,
                    };
                    if matches!(info.framing, BodyFraming::Length(0)) {
                        return self.complete(input);
                    }
                    if input.is_empty() {
                        return Ok(None);
                    }
                }
                State::Body { framing, .. } => {
                    let sink = &mut self.sink;
                    let (consumed, done) = match framing {
                        Framing::Chunked(decoder) => {
                            let mut failure = None;
                            let progress = decoder.decode(input, |data| {
                                if failure.is_none()
                                    && let Err(e) = sink.data(data, &mut on_marker)
                                {
                                    failure = Some(e);
                                }
                            })?;
                            if let Some(e) = failure {
                                return Err(e);
                            }
                            (progress.consumed, progress.done)
                        }
                        Framing::Length(remaining) => {
                            let take = usize::try_from(*remaining)
                                .map_or(input.len(), |r| r.min(input.len()));
                            sink.data(&input[..take], &mut on_marker)?;
                            *remaining -= take as u64;
                            (take, *remaining == 0)
                        }
                        Framing::UntilEof => {
                            sink.data(input, &mut on_marker)?;
                            (input.len(), false)
                        }
                    };
                    if done {
                        return self.complete(&input[consumed..]);
                    }
                    return Ok(None);
                }
                State::Done => return Err(ResponseError::TrailingBytes),
            }
        }
    }

    /// Handles the peer closing the connection: completes a body delimited
    /// by EOF, and reports a truncated response otherwise.
    pub(crate) fn finish_eof(&mut self) -> Result<Completion, ResponseError> {
        match self.state {
            State::Body {
                framing: Framing::UntilEof,
                ..
            } => self
                .complete(&[])
                .map(|c| c.expect("completing an EOF-delimited body always yields")),
            _ => Err(ResponseError::Truncated),
        }
    }

    fn complete(&mut self, rest: &[u8]) -> Result<Option<Completion>, ResponseError> {
        let State::Body {
            status, keep_alive, ..
        } = self.state
        else {
            unreachable!("complete is only called with a body in progress");
        };
        self.state = State::Done;
        if !rest.is_empty() {
            return Err(ResponseError::TrailingBytes);
        }
        self.sink.finish()?;
        Ok(Some(Completion { status, keep_alive }))
    }
}

#[cfg(test)]
mod tests {
    use brisk_bench_core::http1::{LAST_CHUNK, write_chunk};
    use brisk_bench_core::wire::append_content;
    use proptest::prelude::*;

    use super::*;

    const SID: u64 = 77;
    /// A whole response too short to carry a marker.
    const PLAIN: Expect = Expect::Whole {
        sid: SID,
        marker: false,
    };
    /// A whole response that carries one marker.
    const MARKED: Expect = Expect::Whole {
        sid: SID,
        marker: true,
    };

    /// A mock-style `chat.completion` response with `markers` markers of
    /// stream `sid` in its content.
    fn completion_response(sid: u64, markers: u32) -> Vec<u8> {
        let mut body =
            b"{\"object\":\"chat.completion\",\"choices\":[{\"message\":{\"content\":\"".to_vec();
        for seq in 0..markers {
            let marker = Marker {
                sid,
                seq,
                t_sched: 5_000,
                t_write: 5_300,
            };
            append_content(&mut body, &marker, 100);
        }
        body.extend_from_slice(b"\"}}]}");
        let mut out =
            format!("HTTP/1.1 200 OK\r\ncontent-length: {}\r\n\r\n", body.len()).into_bytes();
        out.extend_from_slice(&body);
        out
    }

    /// A mock-style SSE response: one chunked frame per event.
    fn sse_response(sid: u64, chunks: u32, with_done: bool) -> Vec<u8> {
        let mut out = b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\n\
                        transfer-encoding: chunked\r\n\r\n"
            .to_vec();
        for seq in 0..chunks {
            let mut event = b"data: {\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"".to_vec();
            let marker = Marker {
                sid,
                seq,
                t_sched: 1_000 + u64::from(seq),
                t_write: 2_000 + u64::from(seq),
            };
            append_content(&mut event, &marker, 100);
            event.extend_from_slice(b"\"}}]}\n\n");
            write_chunk(&mut out, &event);
        }
        write_chunk(
            &mut out,
            b"data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
        );
        if with_done {
            write_chunk(&mut out, b"data: [DONE]\n\n");
        }
        out.extend_from_slice(LAST_CHUNK);
        out
    }

    fn feed_all(
        parser: &mut ResponseParser,
        pieces: &[&[u8]],
    ) -> (Vec<Marker>, Result<Option<Completion>, ResponseError>) {
        let mut markers = Vec::new();
        let mut last = Ok(None);
        for piece in pieces {
            last = parser.feed(piece, |m| markers.push(m));
            if !matches!(last, Ok(None)) {
                break;
            }
        }
        (markers, last)
    }

    #[test]
    fn whole_stream_completes_with_all_markers() {
        let wire = sse_response(SID, 5, true);
        let mut parser = ResponseParser::new(Expect::Stream {
            sid: SID,
            chunks: 5,
        });
        assert!(!parser.received_any());
        let (markers, done) = feed_all(&mut parser, &[&wire]);
        assert_eq!(
            done,
            Ok(Some(Completion {
                status: 200,
                keep_alive: true
            }))
        );
        assert!(parser.received_any());
        assert_eq!(
            markers.iter().map(|m| m.seq).collect::<Vec<_>>(),
            [0, 1, 2, 3, 4]
        );
        assert!(markers.iter().all(|m| m.sid == SID));
        assert_eq!(markers[2].t_sched, 1_002);
        assert_eq!(markers[2].t_write, 2_002);

        // The same parser serves the next request after a reset.
        parser.reset(Expect::Stream { sid: 5, chunks: 1 });
        let (markers, done) = feed_all(&mut parser, &[&sse_response(5, 1, true)]);
        assert!(matches!(done, Ok(Some(_))));
        assert_eq!(markers.len(), 1);
    }

    #[test]
    fn stream_validation_failures() {
        let mut parser = ResponseParser::new(Expect::Stream {
            sid: SID,
            chunks: 6,
        });
        let (_, done) = feed_all(&mut parser, &[&sse_response(SID, 5, true)]);
        assert_eq!(
            done,
            Err(ResponseError::ShortStream {
                expected: 6,
                got: 5
            })
        );

        let mut parser = ResponseParser::new(Expect::Stream {
            sid: SID,
            chunks: 5,
        });
        let (_, done) = feed_all(&mut parser, &[&sse_response(SID, 5, false)]);
        assert_eq!(done, Err(ResponseError::MissingDone));

        let mut parser = ResponseParser::new(Expect::Stream {
            sid: SID,
            chunks: 5,
        });
        let (_, done) = feed_all(&mut parser, &[&sse_response(SID + 1, 5, true)]);
        assert_eq!(
            done,
            Err(ResponseError::ForeignMarker {
                expected: SID,
                got: SID + 1
            })
        );
        assert_eq!(done.unwrap_err().kind(), "marker_mismatch");
    }

    #[test]
    fn out_of_order_markers_are_rejected() {
        let mut wire = sse_response(SID, 3, true);
        // Swap the sequence digits of the first two markers.
        let first = memchr::memmem::find(&wire, b"@b1:").unwrap();
        let second = first + 1 + memchr::memmem::find(&wire[first + 1..], b"@b1:").unwrap();
        let seq_digit = 4 + 20 + 1 + 9;
        wire[first + seq_digit] = b'1';
        wire[second + seq_digit] = b'0';
        let mut parser = ResponseParser::new(Expect::Stream {
            sid: SID,
            chunks: 3,
        });
        let (markers, done) = feed_all(&mut parser, &[&wire]);
        assert!(markers.is_empty());
        assert_eq!(
            done,
            Err(ResponseError::OutOfOrder {
                expected: 0,
                got: 1
            })
        );
    }

    #[test]
    fn content_length_and_eof_framing() {
        let body = b"{\"object\":\"chat.completion\"}";
        let mut wire =
            format!("HTTP/1.1 200 OK\r\ncontent-length: {}\r\n\r\n", body.len()).into_bytes();
        wire.extend_from_slice(body);
        let mut parser = ResponseParser::new(PLAIN);
        let (_, done) = feed_all(&mut parser, &[&wire[..10], &wire[10..]]);
        assert_eq!(
            done,
            Ok(Some(Completion {
                status: 200,
                keep_alive: true
            }))
        );

        let mut parser = ResponseParser::new(PLAIN);
        let (_, done) = feed_all(&mut parser, &[b"HTTP/1.1 200 OK\r\n\r\npartial"]);
        assert_eq!(done, Ok(None));
        assert_eq!(
            parser.finish_eof(),
            Ok(Completion {
                status: 200,
                keep_alive: false
            })
        );

        let mut parser = ResponseParser::new(PLAIN);
        let (_, done) = feed_all(&mut parser, &[&wire[..wire.len() - 1]]);
        assert_eq!(done, Ok(None));
        assert_eq!(parser.finish_eof(), Err(ResponseError::Truncated));

        let mut parser = ResponseParser::new(PLAIN);
        let mut extra = wire.clone();
        extra.extend_from_slice(b"junk");
        let (_, done) = feed_all(&mut parser, &[&extra]);
        assert_eq!(done, Err(ResponseError::TrailingBytes));
    }

    #[test]
    fn error_statuses_skip_marker_checks_and_honour_connection_close() {
        let wire = b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 503 Service Unavailable\r\n\
                     connection: keep-alive, close\r\ncontent-length: 4\r\n\r\nbusy";
        let mut parser = ResponseParser::new(Expect::Stream {
            sid: SID,
            chunks: 5,
        });
        let (markers, done) = feed_all(&mut parser, &[wire]);
        assert!(markers.is_empty());
        assert_eq!(
            done,
            Ok(Some(Completion {
                status: 503,
                keep_alive: false
            }))
        );

        let mut parser = ResponseParser::new(PLAIN);
        let (_, done) = feed_all(&mut parser, &[b"HTTP/1.0 204 No Content\r\n\r\n"]);
        assert_eq!(
            done,
            Ok(Some(Completion {
                status: 204,
                keep_alive: false
            }))
        );
    }

    #[test]
    fn malformed_heads_and_bodies_are_errors() {
        let mut parser = ResponseParser::new(PLAIN);
        let (_, done) = feed_all(&mut parser, &[b"HTTP/1.1 2xx OK\r\n\r\n"]);
        assert_eq!(done.unwrap_err().kind(), "bad_response");

        let mut parser = ResponseParser::new(PLAIN);
        let (_, done) = feed_all(
            &mut parser,
            &[b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\nzz\r\n"],
        );
        assert!(matches!(done, Err(ResponseError::Framing(_))));

        let mut parser = ResponseParser::new(PLAIN);
        let huge = vec![b'a'; MAX_HEAD + 1];
        let (_, done) = feed_all(&mut parser, &[b"HTTP/1.1 200 OK\r\nx: ", &huge]);
        assert_eq!(done, Err(ResponseError::HeadTooLarge));
    }

    #[test]
    fn whole_responses_carry_exactly_their_marker() {
        let ok = Ok(Some(Completion {
            status: 200,
            keep_alive: true,
        }));
        let wire = completion_response(SID, 1);
        let mut parser = ResponseParser::new(MARKED);
        let (markers, done) = feed_all(&mut parser, &[&wire[..70], &wire[70..]]);
        assert_eq!(done, ok);
        assert_eq!(
            markers,
            [Marker {
                sid: SID,
                seq: 0,
                t_sched: 5_000,
                t_write: 5_300
            }]
        );

        let mut parser = ResponseParser::new(MARKED);
        let (markers, done) = feed_all(&mut parser, &[&completion_response(SID, 0)]);
        assert!(markers.is_empty());
        assert_eq!(done, Err(ResponseError::MissingMarker));
        assert_eq!(done.unwrap_err().kind(), "missing_marker");

        let mut parser = ResponseParser::new(MARKED);
        let (markers, done) = feed_all(&mut parser, &[&completion_response(SID, 2)]);
        assert_eq!(markers.len(), 1);
        assert_eq!(done, Err(ResponseError::ExcessMarker { limit: 1 }));

        let mut parser = ResponseParser::new(MARKED);
        let (_, done) = feed_all(&mut parser, &[&completion_response(SID + 1, 1)]);
        assert_eq!(done.unwrap_err().kind(), "marker_mismatch");

        // Content too short for a marker must not contain one.
        let mut parser = ResponseParser::new(PLAIN);
        let (_, done) = feed_all(&mut parser, &[&completion_response(SID, 1)]);
        assert_eq!(done, Err(ResponseError::ExcessMarker { limit: 0 }));
        let mut parser = ResponseParser::new(PLAIN);
        let (_, done) = feed_all(&mut parser, &[&completion_response(SID, 0)]);
        assert_eq!(done, ok);

        // Error responses are not scanned.
        let mut wire = completion_response(SID, 0);
        wire[9..12].copy_from_slice(b"503");
        let mut parser = ResponseParser::new(MARKED);
        let (_, done) = feed_all(&mut parser, &[&wire]);
        assert_eq!(
            done,
            Ok(Some(Completion {
                status: 503,
                keep_alive: true
            }))
        );
    }

    #[test]
    fn streams_reject_markers_beyond_the_requested_chunks() {
        let mut parser = ResponseParser::new(Expect::Stream {
            sid: SID,
            chunks: 4,
        });
        let (markers, done) = feed_all(&mut parser, &[&sse_response(SID, 5, true)]);
        assert_eq!(markers.len(), 4);
        assert_eq!(done, Err(ResponseError::ExcessMarker { limit: 4 }));
    }

    #[test]
    fn tail_tracks_last_bytes() {
        let mut tail = Tail::new();
        tail.push(b"data: [DO");
        tail.push(b"NE]\n");
        tail.push(b"\n");
        assert!(tail.ends_with_done());
        tail.push(&[b'x'; 100]);
        assert!(!tail.ends_with_done());
        tail.push(b"data: [DONE]\r\n\r\n");
        assert!(tail.ends_with_done());
    }

    proptest! {
        #[test]
        fn any_read_split_gives_the_same_result(
            chunks in 0u32..12,
            cuts in prop::collection::vec(any::<prop::sample::Index>(), 0..40),
        ) {
            let wire = sse_response(SID, chunks, true);
            let mut points: Vec<usize> = cuts.iter().map(|i| i.index(wire.len() + 1)).collect();
            points.push(0);
            points.push(wire.len());
            points.sort_unstable();
            points.dedup();
            let pieces: Vec<&[u8]> = points.windows(2).map(|w| &wire[w[0]..w[1]]).collect();

            let mut parser = ResponseParser::new(Expect::Stream { sid: SID, chunks });
            let (markers, done) = feed_all(&mut parser, &pieces);
            prop_assert_eq!(done, Ok(Some(Completion { status: 200, keep_alive: true })));
            let seqs: Vec<u32> = markers.iter().map(|m| m.seq).collect();
            prop_assert_eq!(seqs, (0..chunks).collect::<Vec<_>>());
        }
    }
}
