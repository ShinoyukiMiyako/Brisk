//! Minimal HTTP/1.1 helpers on top of `httparse`: message heads, body framing
//! and a zero-copy incremental chunked-transfer decoder.

use std::io::Write as _;

/// The terminating chunk of a chunked body with an empty trailer section.
pub const LAST_CHUNK: &[u8] = b"0\r\n\r\n";

/// Longest accepted chunk-size line (digits plus extensions).
const MAX_SIZE_LINE: usize = 4096;
/// Longest accepted trailer section.
const MAX_TRAILER: usize = 64 * 1024;

/// HTTP/1.1 framing and chunked-decoding errors.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum Http1Error {
    /// A chunk-size line contains a byte that is not allowed at its position.
    #[error("invalid byte {0:#04x} in chunk size line")]
    InvalidChunkSize(u8),
    /// The chunk size does not fit in 64 bits.
    #[error("chunk size overflows u64")]
    ChunkSizeOverflow,
    /// A chunk-size line exceeds the length limit.
    #[error("chunk size line too long")]
    ChunkSizeLineTooLong,
    /// A CRLF was expected but another byte was found.
    #[error("expected {expected:?}, found byte {found:#04x}")]
    MissingCrlf {
        /// The expected byte (`'\r'` or `'\n'`).
        expected: char,
        /// The byte actually found.
        found: u8,
    },
    /// The trailer section exceeds the length limit.
    #[error("chunked trailer section too long")]
    TrailerTooLong,
    /// `Content-Length` is not a decimal integer.
    #[error("invalid content-length header")]
    InvalidContentLength,
    /// Several `Content-Length` headers disagree.
    #[error("conflicting content-length headers")]
    ConflictingContentLength,
    /// `Transfer-Encoding` is present but does not end in `chunked`.
    #[error("unsupported transfer-encoding")]
    UnsupportedTransferEncoding,
}

/// Appends one chunk frame: hex length, CRLF, `data`, CRLF.
///
/// An empty `data` would encode the terminating chunk, so it appends nothing;
/// use [`LAST_CHUNK`] to end the body.
pub fn write_chunk(out: &mut Vec<u8>, data: &[u8]) {
    if data.is_empty() {
        return;
    }
    write!(out, "{:x}\r\n", data.len()).expect("writing to a Vec cannot fail");
    out.extend_from_slice(data);
    out.extend_from_slice(b"\r\n");
}

/// How a message body is delimited.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BodyFraming {
    /// `Transfer-Encoding: chunked`.
    Chunked,
    /// `Content-Length: n`, or `Length(0)` for a message that has no body.
    Length(u64),
    /// The body runs until the connection closes (responses only). When
    /// writing a head it means "add no framing header".
    Unframed,
}

/// Header-level framing: `Some(true)` for a final `chunked` coding,
/// `Some(false)` for another final coding, plus the validated length.
fn header_framing(
    headers: &[httparse::Header<'_>],
) -> Result<(Option<bool>, Option<u64>), Http1Error> {
    let mut length: Option<u64> = None;
    let mut chunked = None;
    for h in headers {
        if h.name.eq_ignore_ascii_case("transfer-encoding") {
            let last = h
                .value
                .rsplit(|&b| b == b',')
                .next()
                .map(<[u8]>::trim_ascii)
                .unwrap_or_default();
            chunked = Some(last.eq_ignore_ascii_case(b"chunked"));
        } else if h.name.eq_ignore_ascii_case("content-length") {
            let value = std::str::from_utf8(h.value.trim_ascii())
                .ok()
                .filter(|v| !v.is_empty() && v.bytes().all(|b| b.is_ascii_digit()))
                .and_then(|v| v.parse::<u64>().ok())
                .ok_or(Http1Error::InvalidContentLength)?;
            if length.is_some_and(|prev| prev != value) {
                return Err(Http1Error::ConflictingContentLength);
            }
            length = Some(value);
        }
    }
    Ok((chunked, length))
}

/// Determines the body framing of a request (RFC 9112 §6.3).
///
/// `Transfer-Encoding` wins over `Content-Length`; its final coding must be
/// `chunked`. A request with neither header has no body (`Length(0)`).
pub fn request_body_framing(headers: &[httparse::Header<'_>]) -> Result<BodyFraming, Http1Error> {
    match header_framing(headers)? {
        (Some(true), _) => Ok(BodyFraming::Chunked),
        (Some(false), _) => Err(Http1Error::UnsupportedTransferEncoding),
        (None, length) => Ok(BodyFraming::Length(length.unwrap_or(0))),
    }
}

/// Determines the body framing of a response (RFC 9112 §6.3).
///
/// Responses to `HEAD` and responses with status 1xx, 204 or 304 have no
/// body whatever their headers say. Otherwise `Transfer-Encoding` wins over
/// `Content-Length`; a final coding other than `chunked`, or neither header,
/// means the body runs until the connection closes.
pub fn response_body_framing(
    status: u16,
    request_was_head: bool,
    headers: &[httparse::Header<'_>],
) -> Result<BodyFraming, Http1Error> {
    if request_was_head || (100..200).contains(&status) || status == 204 || status == 304 {
        return Ok(BodyFraming::Length(0));
    }
    match header_framing(headers)? {
        (Some(true), _) => Ok(BodyFraming::Chunked),
        (Some(false), _) | (None, None) => Ok(BodyFraming::Unframed),
        (None, Some(n)) => Ok(BodyFraming::Length(n)),
    }
}

fn write_headers(out: &mut Vec<u8>, headers: &[(&str, &str)], framing: BodyFraming) {
    for (name, value) in headers {
        out.extend_from_slice(name.as_bytes());
        out.extend_from_slice(b": ");
        out.extend_from_slice(value.as_bytes());
        out.extend_from_slice(b"\r\n");
    }
    match framing {
        BodyFraming::Chunked => out.extend_from_slice(b"transfer-encoding: chunked\r\n"),
        BodyFraming::Length(n) => {
            write!(out, "content-length: {n}\r\n").expect("writing to a Vec cannot fail");
        }
        BodyFraming::Unframed => {}
    }
    out.extend_from_slice(b"\r\n");
}

/// Appends an HTTP/1.1 request head. `framing` adds the matching
/// `content-length` or `transfer-encoding` header.
pub fn write_request_head(
    out: &mut Vec<u8>,
    method: &str,
    path: &str,
    host: &str,
    headers: &[(&str, &str)],
    framing: BodyFraming,
) {
    write!(out, "{method} {path} HTTP/1.1\r\nhost: {host}\r\n")
        .expect("writing to a Vec cannot fail");
    write_headers(out, headers, framing);
}

/// Appends an HTTP/1.1 response head. `framing` adds the matching
/// `content-length` or `transfer-encoding` header.
pub fn write_response_head(
    out: &mut Vec<u8>,
    status: u16,
    reason: &str,
    headers: &[(&str, &str)],
    framing: BodyFraming,
) {
    write!(out, "HTTP/1.1 {status} {reason}\r\n").expect("writing to a Vec cannot fail");
    write_headers(out, headers, framing);
}

/// Position within a chunk-size line:
/// `chunk-size [ BWS ";" chunk-ext ] [ BWS ] CRLF`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SizePhase {
    /// No hex digit read yet.
    Start,
    /// Inside the hex digits.
    Digits,
    /// Whitespace after the digits; only more whitespace, `;` or CR may
    /// follow.
    Bws,
    /// Inside the extensions, which are opaque up to CR.
    Ext,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Size {
        value: u64,
        phase: SizePhase,
    },
    SizeLf {
        value: u64,
    },
    Data {
        remaining: u64,
    },
    DataCr,
    DataLf,
    /// At the start of a trailer line (or of the final empty line).
    TrailerLineStart,
    TrailerLine,
    TrailerLineLf,
    TrailerEndLf,
    Done,
}

impl State {
    const START: Self = Self::Size {
        value: 0,
        phase: SizePhase::Start,
    };
}

/// Result of one [`ChunkedDecoder::decode`] call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Decoded {
    /// Bytes of the input that belong to this body. Less than the input length
    /// only when the body ended; the rest belongs to the next message.
    pub consumed: usize,
    /// Whether the terminating chunk and trailer section were fully read.
    pub done: bool,
}

/// Incremental decoder for `Transfer-Encoding: chunked` bodies.
///
/// Input may be split at any byte. Decoded data is handed out as sub-slices of
/// the input, so nothing is copied. Chunk extensions and trailer fields are
/// validated for framing and skipped.
#[derive(Debug, Clone)]
pub struct ChunkedDecoder {
    state: State,
    /// Bytes spent on the current size line or on the trailer section.
    overhead: usize,
}

impl Default for ChunkedDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl ChunkedDecoder {
    /// Creates a decoder positioned at the start of a chunked body.
    pub fn new() -> Self {
        Self {
            state: State::START,
            overhead: 0,
        }
    }

    /// Resets the decoder for the next body on the same connection.
    pub fn reset(&mut self) {
        *self = Self::new();
    }

    /// Whether the whole body, trailer included, has been decoded.
    pub fn is_done(&self) -> bool {
        self.state == State::Done
    }

    /// Decodes as much of `input` as possible, passing each run of body data
    /// to `on_data` as a sub-slice of `input`.
    ///
    /// Stops right after the end of the body; see [`Decoded::consumed`].
    /// After an error the decoder must not be used for this body again.
    pub fn decode<'a, F>(&mut self, input: &'a [u8], mut on_data: F) -> Result<Decoded, Http1Error>
    where
        F: FnMut(&'a [u8]),
    {
        let mut pos = 0;
        while pos < input.len() {
            match self.state {
                State::Done => break,
                State::Data { remaining } => {
                    let avail = input.len() - pos;
                    let take = usize::try_from(remaining).map_or(avail, |r| r.min(avail));
                    on_data(&input[pos..pos + take]);
                    pos += take;
                    let left = remaining - take as u64;
                    self.state = if left == 0 {
                        State::DataCr
                    } else {
                        State::Data { remaining: left }
                    };
                }
                _ => {
                    self.step(input[pos])?;
                    pos += 1;
                }
            }
        }
        Ok(Decoded {
            consumed: pos,
            done: self.state == State::Done,
        })
    }

    /// Advances the framing state machine by one non-data byte.
    fn step(&mut self, byte: u8) -> Result<(), Http1Error> {
        self.state = match self.state {
            State::Size { value, phase } => {
                self.bump_overhead(MAX_SIZE_LINE, Http1Error::ChunkSizeLineTooLong)?;
                let digit = hex_value(byte);
                match (phase, byte) {
                    (SizePhase::Start | SizePhase::Digits, _) if digit.is_some() => {
                        // Overflow is judged by value, so leading zeros are fine.
                        let value = value
                            .checked_mul(16)
                            .map(|v| v | digit.map_or(0, u64::from))
                            .ok_or(Http1Error::ChunkSizeOverflow)?;
                        State::Size {
                            value,
                            phase: SizePhase::Digits,
                        }
                    }
                    (SizePhase::Digits | SizePhase::Bws | SizePhase::Ext, b'\r') => {
                        State::SizeLf { value }
                    }
                    // Extension bytes are opaque; only a bare LF is illegal.
                    (SizePhase::Digits | SizePhase::Bws, b';') | (SizePhase::Ext, _)
                        if byte != b'\n' =>
                    {
                        State::Size {
                            value,
                            phase: SizePhase::Ext,
                        }
                    }
                    (SizePhase::Digits | SizePhase::Bws, b' ' | b'\t') => State::Size {
                        value,
                        phase: SizePhase::Bws,
                    },
                    _ => return Err(Http1Error::InvalidChunkSize(byte)),
                }
            }
            State::SizeLf { value } => {
                expect(byte, b'\n')?;
                self.overhead = 0;
                if value == 0 {
                    State::TrailerLineStart
                } else {
                    State::Data { remaining: value }
                }
            }
            State::DataCr => {
                expect(byte, b'\r')?;
                State::DataLf
            }
            State::DataLf => {
                expect(byte, b'\n')?;
                State::START
            }
            State::TrailerLineStart => {
                self.bump_overhead(MAX_TRAILER, Http1Error::TrailerTooLong)?;
                match byte {
                    b'\r' => State::TrailerEndLf,
                    b'\n' => {
                        return Err(Http1Error::MissingCrlf {
                            expected: '\r',
                            found: byte,
                        });
                    }
                    _ => State::TrailerLine,
                }
            }
            State::TrailerLine => {
                self.bump_overhead(MAX_TRAILER, Http1Error::TrailerTooLong)?;
                if byte == b'\r' {
                    State::TrailerLineLf
                } else {
                    State::TrailerLine
                }
            }
            State::TrailerLineLf => {
                expect(byte, b'\n')?;
                State::TrailerLineStart
            }
            State::TrailerEndLf => {
                expect(byte, b'\n')?;
                State::Done
            }
            State::Data { .. } | State::Done => {
                unreachable!("decode handles data and completion without step")
            }
        };
        Ok(())
    }

    fn bump_overhead(&mut self, limit: usize, err: Http1Error) -> Result<(), Http1Error> {
        self.overhead += 1;
        if self.overhead > limit {
            return Err(err);
        }
        Ok(())
    }
}

#[inline]
fn hex_value(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

#[inline]
fn expect(found: u8, expected: u8) -> Result<(), Http1Error> {
    if found == expected {
        Ok(())
    } else {
        Err(Http1Error::MissingCrlf {
            expected: char::from(expected),
            found,
        })
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn decode_all(dec: &mut ChunkedDecoder, input: &[u8]) -> (Vec<u8>, Decoded) {
        let mut out = Vec::new();
        let d = dec.decode(input, |s| out.extend_from_slice(s)).unwrap();
        (out, d)
    }

    #[test]
    fn write_chunk_frames() {
        let mut out = Vec::new();
        write_chunk(&mut out, b"hello world, 26 bytes long");
        write_chunk(&mut out, b"");
        out.extend_from_slice(LAST_CHUNK);
        assert_eq!(&out[..], b"1a\r\nhello world, 26 bytes long\r\n0\r\n\r\n");
    }

    #[test]
    fn decodes_extensions_trailers_and_stops_at_end() {
        let input = b"5;name=val\r\nhello\r\n6 \r\n world\r\n0\r\nX-T: 1\r\nY: 2\r\n\r\nNEXT";
        let mut dec = ChunkedDecoder::new();
        let (out, d) = decode_all(&mut dec, input);
        assert_eq!(&out[..], b"hello world");
        assert!(d.done && dec.is_done());
        assert_eq!(&input[d.consumed..], b"NEXT");
    }

    #[test]
    fn rejects_malformed_input() {
        let cases: [(&[u8], Http1Error); 8] = [
            (b"g\r\n", Http1Error::InvalidChunkSize(b'g')),
            (b"\r\n", Http1Error::InvalidChunkSize(b'\r')),
            (b"11111111111111111\r\n", Http1Error::ChunkSizeOverflow),
            (
                b"1\r\nab",
                Http1Error::MissingCrlf {
                    expected: '\r',
                    found: b'b',
                },
            ),
            (b"1\nX", Http1Error::InvalidChunkSize(b'\n')),
            // A digit after whitespace would otherwise be read as extension.
            (b"6 7\r\n", Http1Error::InvalidChunkSize(b'7')),
            (b"6 x\r\n", Http1Error::InvalidChunkSize(b'x')),
            (b" 6\r\n", Http1Error::InvalidChunkSize(b' ')),
        ];
        for (input, err) in cases {
            let mut dec = ChunkedDecoder::new();
            assert_eq!(dec.decode(input, |_| {}), Err(err), "{input:?}");
        }
    }

    #[test]
    fn size_overflow_is_judged_by_value() {
        let mut input = b"00000000000000000003\r\nabc\r\n0\r\n\r\n".to_vec();
        let (out, d) = decode_all(&mut ChunkedDecoder::new(), &input);
        assert_eq!(&out[..], b"abc");
        assert!(d.done);

        input = b"FFFFFFFFFFFFFFFF\r\n".to_vec();
        let d = ChunkedDecoder::new().decode(&input, |_| {}).unwrap();
        assert_eq!(d.consumed, input.len());
    }

    #[test]
    fn request_framing_from_headers() {
        let h = |name, value: &'static [u8]| httparse::Header { name, value };
        assert_eq!(
            request_body_framing(&[h("Transfer-Encoding", b"gzip, Chunked")]),
            Ok(BodyFraming::Chunked)
        );
        assert_eq!(
            request_body_framing(&[h("content-length", b" 12 "), h("Content-Length", b"12")]),
            Ok(BodyFraming::Length(12))
        );
        assert_eq!(
            request_body_framing(&[h("content-length", b"12"), h("content-length", b"13")]),
            Err(Http1Error::ConflictingContentLength)
        );
        assert_eq!(
            request_body_framing(&[h("content-length", b"-1")]),
            Err(Http1Error::InvalidContentLength)
        );
        assert_eq!(
            request_body_framing(&[h("transfer-encoding", b"gzip")]),
            Err(Http1Error::UnsupportedTransferEncoding)
        );
        assert_eq!(request_body_framing(&[]), Ok(BodyFraming::Length(0)));
    }

    #[test]
    fn response_framing_from_status_and_headers() {
        let h = |name, value: &'static [u8]| httparse::Header { name, value };
        let cl = [h("content-length", b"12")];
        let te = [h("transfer-encoding", b"chunked")];
        assert_eq!(
            response_body_framing(200, false, &te),
            Ok(BodyFraming::Chunked)
        );
        assert_eq!(
            response_body_framing(200, false, &cl),
            Ok(BodyFraming::Length(12))
        );
        assert_eq!(
            response_body_framing(200, false, &[]),
            Ok(BodyFraming::Unframed)
        );
        assert_eq!(
            response_body_framing(200, false, &[h("transfer-encoding", b"gzip")]),
            Ok(BodyFraming::Unframed)
        );
        for status in [100, 101, 204, 304] {
            assert_eq!(
                response_body_framing(status, false, &cl),
                Ok(BodyFraming::Length(0)),
                "{status}"
            );
        }
        assert_eq!(
            response_body_framing(200, true, &te),
            Ok(BodyFraming::Length(0))
        );
        assert_eq!(
            response_body_framing(200, false, &[h("content-length", b"x")]),
            Err(Http1Error::InvalidContentLength)
        );
    }

    #[test]
    fn heads_parse_back() {
        let mut out = Vec::new();
        write_request_head(
            &mut out,
            "POST",
            "/v1/chat/completions",
            "127.0.0.1:8317",
            &[("content-type", "application/json")],
            BodyFraming::Length(42),
        );
        let mut headers = [httparse::EMPTY_HEADER; 8];
        let mut req = httparse::Request::new(&mut headers);
        let status = req.parse(&out).unwrap();
        assert_eq!(status, httparse::Status::Complete(out.len()));
        assert_eq!(req.method, Some("POST"));
        assert_eq!(
            request_body_framing(req.headers),
            Ok(BodyFraming::Length(42))
        );

        let mut out = Vec::new();
        write_response_head(
            &mut out,
            200,
            "OK",
            &[("content-type", "text/event-stream")],
            BodyFraming::Chunked,
        );
        let mut headers = [httparse::EMPTY_HEADER; 8];
        let mut resp = httparse::Response::new(&mut headers);
        assert!(resp.parse(&out).unwrap().is_complete());
        assert_eq!(resp.code, Some(200));
        assert_eq!(
            response_body_framing(200, false, resp.headers),
            Ok(BodyFraming::Chunked)
        );
    }

    /// How one chunk-size line is spelled: uppercase hex, leading zeros,
    /// whitespace before the extension or line end, and an extension.
    type SizeStyle = (bool, usize, usize, Option<String>);

    fn write_size_line(out: &mut Vec<u8>, len: usize, style: &SizeStyle) {
        let (upper, zeros, bws, ext) = style;
        out.extend(std::iter::repeat_n(b'0', *zeros));
        let hex = if *upper {
            format!("{len:X}")
        } else {
            format!("{len:x}")
        };
        out.extend_from_slice(hex.as_bytes());
        out.extend(std::iter::repeat_n(b' ', *bws));
        if let Some(ext) = ext {
            out.push(b';');
            out.extend_from_slice(ext.as_bytes());
        }
        out.extend_from_slice(b"\r\n");
    }

    fn encode(chunks: &[(Vec<u8>, SizeStyle)], trailer: &[(String, String)]) -> Vec<u8> {
        let mut out = Vec::new();
        for (data, style) in chunks {
            write_size_line(&mut out, data.len(), style);
            out.extend_from_slice(data);
            out.extend_from_slice(b"\r\n");
        }
        out.extend_from_slice(b"0\r\n");
        for (k, v) in trailer {
            out.extend_from_slice(format!("{k}: {v}\r\n").as_bytes());
        }
        out.extend_from_slice(b"\r\n");
        out
    }

    fn size_style() -> impl Strategy<Value = SizeStyle> {
        (
            any::<bool>(),
            0usize..4,
            0usize..3,
            prop::option::of("[a-z]{1,6}(=[!-~]{0,8})?"),
        )
    }

    proptest! {
        #[test]
        fn split_anywhere_matches_one_shot(
            chunks in prop::collection::vec(
                (prop::collection::vec(any::<u8>(), 1..300), size_style()),
                0..12,
            ),
            trailer in prop::collection::vec(("[a-z]{1,8}", "[ -~]{0,16}"), 0..3),
            cuts in prop::collection::vec(any::<prop::sample::Index>(), 0..30),
            suffix in prop::collection::vec(any::<u8>(), 0..8),
        ) {
            let mut wire = encode(&chunks, &trailer);
            let body_len = wire.len();
            wire.extend_from_slice(&suffix);
            let expected: Vec<u8> = chunks.iter().flat_map(|(d, _)| d.iter().copied()).collect();

            let mut one = ChunkedDecoder::new();
            let (whole, d) = decode_all(&mut one, &wire);
            prop_assert_eq!(&whole, &expected);
            prop_assert_eq!(d, Decoded { consumed: body_len, done: true });

            let mut points: Vec<usize> = cuts.iter().map(|i| i.index(wire.len() + 1)).collect();
            points.push(0);
            points.push(wire.len());
            points.sort_unstable();
            points.dedup();

            let mut dec = ChunkedDecoder::new();
            let mut split = Vec::new();
            let mut consumed_total = 0;
            for w in points.windows(2) {
                if dec.is_done() {
                    break;
                }
                let d = dec.decode(&wire[w[0]..w[1]], |s| split.extend_from_slice(s)).unwrap();
                consumed_total += d.consumed;
            }
            prop_assert!(dec.is_done());
            prop_assert_eq!(consumed_total, body_len);
            prop_assert_eq!(split, expected);
        }
    }
}
