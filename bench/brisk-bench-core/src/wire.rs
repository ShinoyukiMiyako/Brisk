//! In-band benchmark metadata: timestamp markers in SSE content and request
//! parameter directives in the system prompt.
//!
//! Everything here travels inside ordinary `OpenAI` Chat payloads so a gateway
//! that copies headers by allow-list and rewrites nothing in the body forwards
//! it untouched.

use std::fmt::Write as _;

use memchr::memmem;

/// Fixed length of an encoded marker in bytes.
pub const MARKER_LEN: usize = 78;

/// The four bytes every marker starts with.
pub const MARKER_PREFIX: &[u8; 4] = b"@b1:";

const SID_OFFSET: usize = 4;
const SID_DIGITS: usize = 20;
const SEQ_OFFSET: usize = SID_OFFSET + SID_DIGITS + 1;
const SEQ_DIGITS: usize = 10;
const T_SCHED_OFFSET: usize = SEQ_OFFSET + SEQ_DIGITS + 1;
const TS_DIGITS: usize = 20;
/// Byte offset of the `t_write` field inside a marker.
pub const T_WRITE_OFFSET: usize = T_SCHED_OFFSET + TS_DIGITS + 1;
const TERMINATOR_OFFSET: usize = T_WRITE_OFFSET + TS_DIGITS;

const _: () = assert!(TERMINATOR_OFFSET + 1 == MARKER_LEN);

/// [`MARKER_LEN`] as the type of [`BenchParams::chunk_bytes`].
pub const MARKER_LEN_U32: u32 = 78;
const _: () = assert!(MARKER_LEN_U32 as usize == MARKER_LEN);

/// The prefix that introduces a request parameter directive.
pub const DIRECTIVE_PREFIX: &[u8] = b"bench:v1;";

/// Errors from marker and directive parsing.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum WireError {
    /// A `@b1:` prefix was found but the following bytes are not a valid marker.
    #[error("malformed marker: {reason}")]
    MalformedMarker {
        /// Which structural check failed.
        reason: &'static str,
    },
    /// A marker field does not fit its integer type.
    #[error("marker field {field} overflows")]
    MarkerOverflow {
        /// Name of the overflowing field.
        field: &'static str,
    },
    /// The directive is not terminated by a `"` before the end of the body.
    #[error("unterminated bench directive")]
    UnterminatedDirective,
    /// A directive entry is not of the form `key=value`.
    #[error("malformed directive entry {0:?}")]
    MalformedEntry(String),
    /// The directive contains a key this version does not know.
    #[error("unknown directive key {0:?}")]
    UnknownKey(String),
    /// The directive sets the same key twice.
    #[error("duplicate directive key {0:?}")]
    DuplicateKey(&'static str),
    /// A required directive key is absent.
    #[error("missing directive key {0:?}")]
    MissingKey(&'static str),
    /// A directive value is not a valid unsigned integer of the right width.
    #[error("invalid value {value:?} for directive key {key:?}")]
    InvalidValue {
        /// The key whose value failed to parse.
        key: &'static str,
        /// The offending value.
        value: String,
    },
    /// `chunk_bytes` is smaller than a marker.
    #[error("chunk_bytes {0} is smaller than the marker length {MARKER_LEN}")]
    ChunkBytesTooSmall(u32),
}

/// A decoded timestamp marker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Marker {
    /// Stream id chosen by the load generator.
    pub sid: u64,
    /// Chunk sequence number within the stream, starting at 0.
    pub seq: u32,
    /// Planned emission time, monotonic nanoseconds.
    pub t_sched: u64,
    /// Time taken immediately before the write syscall, monotonic nanoseconds.
    pub t_write: u64,
}

impl Marker {
    /// Encodes the marker into a fixed-size buffer.
    pub fn encode(&self, out: &mut [u8; MARKER_LEN]) {
        encode_marker(out, self.sid, self.seq, self.t_sched, self.t_write);
    }

    /// Parses exactly one marker from `bytes`, which must be [`MARKER_LEN`] long.
    pub fn parse(bytes: &[u8]) -> Result<Self, WireError> {
        let bytes: &[u8; MARKER_LEN] =
            bytes.try_into().map_err(|_| WireError::MalformedMarker {
                reason: "wrong length",
            })?;
        if &bytes[..SID_OFFSET] != MARKER_PREFIX {
            return Err(WireError::MalformedMarker {
                reason: "bad prefix",
            });
        }
        for (offset, reason) in [
            (SEQ_OFFSET - 1, "missing ':' after sid"),
            (T_SCHED_OFFSET - 1, "missing ':' after seq"),
            (T_WRITE_OFFSET - 1, "missing ':' after t_sched"),
        ] {
            if bytes[offset] != b':' {
                return Err(WireError::MalformedMarker { reason });
            }
        }
        if bytes[TERMINATOR_OFFSET] != b';' {
            return Err(WireError::MalformedMarker {
                reason: "missing ';' terminator",
            });
        }
        let sid = parse_digits(&bytes[SID_OFFSET..SID_OFFSET + SID_DIGITS], "sid")?;
        let seq = parse_digits(&bytes[SEQ_OFFSET..SEQ_OFFSET + SEQ_DIGITS], "seq")?;
        let seq = u32::try_from(seq).map_err(|_| WireError::MarkerOverflow { field: "seq" })?;
        let t_sched = parse_digits(
            &bytes[T_SCHED_OFFSET..T_SCHED_OFFSET + TS_DIGITS],
            "t_sched",
        )?;
        let t_write = parse_digits(
            &bytes[T_WRITE_OFFSET..T_WRITE_OFFSET + TS_DIGITS],
            "t_write",
        )?;
        Ok(Self {
            sid,
            seq,
            t_sched,
            t_write,
        })
    }
}

/// Writes `value` as exactly `out.len()` zero-padded decimal digits.
#[inline]
fn write_digits(out: &mut [u8], mut value: u64) {
    for slot in out.iter_mut().rev() {
        *slot = b'0' + (value % 10) as u8;
        value /= 10;
    }
    debug_assert_eq!(value, 0, "value does not fit the field width");
}

fn parse_digits(field: &[u8], name: &'static str) -> Result<u64, WireError> {
    field.iter().try_fold(0u64, |acc, &b| {
        if !b.is_ascii_digit() {
            return Err(WireError::MalformedMarker {
                reason: "non-digit in numeric field",
            });
        }
        acc.checked_mul(10)
            .and_then(|v| v.checked_add(u64::from(b - b'0')))
            .ok_or(WireError::MarkerOverflow { field: name })
    })
}

/// Encodes a marker: `@b1:<sid 20>:<seq 10>:<t_sched 20>:<t_write 20>;`.
pub fn encode_marker(out: &mut [u8; MARKER_LEN], sid: u64, seq: u32, t_sched: u64, t_write: u64) {
    out[..SID_OFFSET].copy_from_slice(MARKER_PREFIX);
    write_digits(&mut out[SID_OFFSET..SID_OFFSET + SID_DIGITS], sid);
    out[SEQ_OFFSET - 1] = b':';
    write_digits(
        &mut out[SEQ_OFFSET..SEQ_OFFSET + SEQ_DIGITS],
        u64::from(seq),
    );
    out[T_SCHED_OFFSET - 1] = b':';
    write_digits(
        &mut out[T_SCHED_OFFSET..T_SCHED_OFFSET + TS_DIGITS],
        t_sched,
    );
    out[T_WRITE_OFFSET - 1] = b':';
    write_digits(
        &mut out[T_WRITE_OFFSET..T_WRITE_OFFSET + TS_DIGITS],
        t_write,
    );
    out[TERMINATOR_OFFSET] = b';';
}

/// Rewrites the `t_write` field of an already encoded marker in place.
///
/// `marker` must start at the marker's first byte; anything after
/// [`MARKER_LEN`] bytes is left alone, so a slice into a larger send buffer
/// works.
///
/// # Panics
///
/// Panics if `marker` is shorter than [`MARKER_LEN`].
#[inline]
pub fn patch_t_write(marker: &mut [u8], t_write: u64) {
    debug_assert_eq!(&marker[..SID_OFFSET], MARKER_PREFIX);
    write_digits(
        &mut marker[T_WRITE_OFFSET..T_WRITE_OFFSET + TS_DIGITS],
        t_write,
    );
}

/// Appends one SSE `delta.content` value to `out`: a marker followed by `x`
/// padding up to `chunk_bytes` in total. Returns the offset of the marker in
/// `out` so the caller can [`patch_t_write`] it right before writing.
///
/// # Panics
///
/// Panics if `chunk_bytes` is smaller than [`MARKER_LEN`]; [`BenchParams`]
/// validation rules that out for parsed directives.
pub fn append_content(out: &mut Vec<u8>, marker: &Marker, chunk_bytes: u32) -> usize {
    let total = usize::try_from(chunk_bytes).expect("u32 fits usize");
    assert!(total >= MARKER_LEN, "chunk_bytes {total} < MARKER_LEN");
    let offset = out.len();
    let mut encoded = [0u8; MARKER_LEN];
    marker.encode(&mut encoded);
    out.extend_from_slice(&encoded);
    out.resize(offset + total, b'x');
    offset
}

/// Streaming marker finder that tolerates arbitrary read boundaries.
///
/// Feed decoded body bytes in order with [`feed`](Self::feed). A marker split
/// across calls is kept in a tail buffer of at most `MARKER_LEN − 1` bytes and
/// completed by the next call.
#[derive(Debug)]
pub struct MarkerScanner {
    finder: memmem::Finder<'static>,
    tail: [u8; MARKER_LEN - 1],
    tail_len: usize,
}

impl Default for MarkerScanner {
    fn default() -> Self {
        Self::new()
    }
}

impl MarkerScanner {
    /// Creates an empty scanner.
    pub fn new() -> Self {
        Self {
            finder: memmem::Finder::new(MARKER_PREFIX),
            tail: [0; MARKER_LEN - 1],
            tail_len: 0,
        }
    }

    /// Discards any partial marker, e.g. when a response ends.
    pub fn reset(&mut self) {
        self.tail_len = 0;
    }

    /// Number of bytes of an incomplete marker currently buffered.
    pub fn pending(&self) -> usize {
        self.tail_len
    }

    /// Scans `chunk` and calls `on_marker` for every marker completed by it,
    /// in stream order. Returns the number of markers reported.
    ///
    /// A `@b1:` prefix followed by bytes that are not a valid marker is an
    /// error, not silently skipped; the scanner state after an error is
    /// unspecified and should be [`reset`](Self::reset).
    pub fn feed<F>(&mut self, chunk: &[u8], mut on_marker: F) -> Result<usize, WireError>
    where
        F: FnMut(Marker),
    {
        let mut found = 0;
        let mut pos = 0;

        if self.tail_len > 0 {
            let need = MARKER_LEN - self.tail_len;
            let take = need.min(chunk.len());
            // Only the prefix part can still turn out not to be a marker.
            let prefix_have = self.tail_len.min(MARKER_PREFIX.len());
            let prefix_check_end = MARKER_PREFIX.len().min(self.tail_len + take);
            let prefix_ok = MARKER_PREFIX[prefix_have..prefix_check_end]
                == chunk[..prefix_check_end - prefix_have];
            if prefix_ok {
                if take < need {
                    self.tail[self.tail_len..self.tail_len + take].copy_from_slice(&chunk[..take]);
                    self.tail_len += take;
                    return Ok(0);
                }
                let mut full = [0u8; MARKER_LEN];
                full[..self.tail_len].copy_from_slice(&self.tail[..self.tail_len]);
                full[self.tail_len..].copy_from_slice(&chunk[..need]);
                self.tail_len = 0;
                on_marker(Marker::parse(&full)?);
                found += 1;
                pos = need;
            } else {
                // The buffered bytes were a proper prefix of `@b1:` that did
                // not continue; none of them can start another marker because
                // `@` only appears at index 0 of the prefix.
                self.tail_len = 0;
            }
        }

        while let Some(rel) = self.finder.find(&chunk[pos..]) {
            let start = pos + rel;
            let end = start + MARKER_LEN;
            if end > chunk.len() {
                let rest = &chunk[start..];
                self.tail[..rest.len()].copy_from_slice(rest);
                self.tail_len = rest.len();
                return Ok(found);
            }
            on_marker(Marker::parse(&chunk[start..end])?);
            found += 1;
            pos = end;
        }

        // Keep a trailing proper prefix of `@b1:` ("@", "@b", "@b1").
        let scan_from = pos.max(chunk.len().saturating_sub(MARKER_PREFIX.len() - 1));
        for at in memchr::memchr_iter(b'@', &chunk[scan_from..]) {
            let rest = &chunk[scan_from + at..];
            if MARKER_PREFIX.starts_with(rest) {
                self.tail[..rest.len()].copy_from_slice(rest);
                self.tail_len = rest.len();
                break;
            }
        }
        Ok(found)
    }
}

/// Per-request mock behaviour, carried in the first system message.
///
/// Wire form:
/// `bench:v1;ttft_us=<u64>;interval_us=<u64>;chunks=<u32>;chunk_bytes=<u32>;sid=<u64>;resp_bytes=<u32>`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BenchParams {
    /// Delay from request completion to the first chunk, microseconds.
    pub ttft_us: u64,
    /// Delay between consecutive chunks, microseconds.
    pub interval_us: u64,
    /// Number of content chunks in a streaming response.
    pub chunks: u32,
    /// Length of every `delta.content` string, marker included.
    pub chunk_bytes: u32,
    /// Stream id echoed in every marker.
    pub sid: u64,
    /// Content length of a non-streaming response.
    pub resp_bytes: u32,
}

impl BenchParams {
    /// Checks invariants the mock relies on.
    pub fn validate(&self) -> Result<(), WireError> {
        if self.chunk_bytes >= MARKER_LEN_U32 {
            Ok(())
        } else {
            Err(WireError::ChunkBytesTooSmall(self.chunk_bytes))
        }
    }

    /// Renders the directive string, e.g. for a system message.
    pub fn to_directive(&self) -> String {
        let mut s = String::with_capacity(128);
        self.write_directive(&mut s);
        s
    }

    fn write_directive(&self, out: &mut String) {
        write!(
            out,
            "bench:v1;ttft_us={};interval_us={};chunks={};chunk_bytes={};sid={};resp_bytes={}",
            self.ttft_us,
            self.interval_us,
            self.chunks,
            self.chunk_bytes,
            self.sid,
            self.resp_bytes
        )
        .expect("writing to a String cannot fail");
    }

    /// Parses the text following [`DIRECTIVE_PREFIX`] up to (excluding) the
    /// closing quote.
    fn parse_fields(fields: &[u8]) -> Result<Self, WireError> {
        const KEYS: [&str; 6] = [
            "ttft_us",
            "interval_us",
            "chunks",
            "chunk_bytes",
            "sid",
            "resp_bytes",
        ];
        let mut values: [Option<u64>; 6] = [None; 6];
        for entry in fields.split(|&b| b == b';') {
            let entry_str = || String::from_utf8_lossy(entry).into_owned();
            let eq = memchr::memchr(b'=', entry)
                .ok_or_else(|| WireError::MalformedEntry(entry_str()))?;
            let (key, value) = (&entry[..eq], &entry[eq + 1..]);
            let idx = KEYS
                .iter()
                .position(|k| k.as_bytes() == key)
                .ok_or_else(|| WireError::UnknownKey(String::from_utf8_lossy(key).into_owned()))?;
            let name = KEYS[idx];
            if values[idx].is_some() {
                return Err(WireError::DuplicateKey(name));
            }
            let invalid = || WireError::InvalidValue {
                key: name,
                value: String::from_utf8_lossy(value).into_owned(),
            };
            let parsed = std::str::from_utf8(value)
                .ok()
                .filter(|v| !v.is_empty() && v.bytes().all(|b| b.is_ascii_digit()))
                .and_then(|v| v.parse::<u64>().ok())
                .ok_or_else(invalid)?;
            values[idx] = Some(parsed);
        }
        let get = |idx: usize| values[idx].ok_or(WireError::MissingKey(KEYS[idx]));
        let narrow = |idx: usize| -> Result<u32, WireError> {
            let v = get(idx)?;
            u32::try_from(v).map_err(|_| WireError::InvalidValue {
                key: KEYS[idx],
                value: v.to_string(),
            })
        };
        let params = Self {
            ttft_us: get(0)?,
            interval_us: get(1)?,
            chunks: narrow(2)?,
            chunk_bytes: narrow(3)?,
            sid: get(4)?,
            resp_bytes: narrow(5)?,
        };
        params.validate()?;
        Ok(params)
    }
}

/// Finds and parses the first `bench:v1;` directive in a raw request body.
///
/// The directive runs to the next `"`, i.e. the end of the JSON string it is
/// embedded in. Returns `Ok(None)` when the body has no directive.
pub fn find_directive(body: &[u8]) -> Result<Option<BenchParams>, WireError> {
    let Some(start) = memmem::find(body, DIRECTIVE_PREFIX) else {
        return Ok(None);
    };
    let fields_start = start + DIRECTIVE_PREFIX.len();
    let len =
        memchr::memchr(b'"', &body[fields_start..]).ok_or(WireError::UnterminatedDirective)?;
    BenchParams::parse_fields(&body[fields_start..fields_start + len]).map(Some)
}

/// Shape of the user message in a generated request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BodyShape {
    /// Plain text content of exactly `bytes` printable ASCII characters that
    /// need no JSON escaping.
    Text {
        /// Length of the user text.
        bytes: usize,
    },
    /// Multi-part content: a text part and a `data:image/png;base64,...`
    /// image part.
    Image {
        /// Length of the base64 payload; rounded down to a multiple of 4 so it
        /// is valid padded base64.
        base64_bytes: usize,
        /// Length of the text part.
        text_bytes: usize,
    },
}

/// Printable filler that needs no JSON escaping.
const TEXT_PATTERN: &[u8] = b"The quick brown fox jumps over the lazy dog 0123456789. ";
/// Base64 of `ABC`, repeated as the image payload.
const BASE64_PATTERN: &[u8; 4] = b"QUJD";

fn append_repeated(out: &mut Vec<u8>, pattern: &[u8], len: usize) {
    out.reserve(len);
    let mut remaining = len;
    while remaining >= pattern.len() {
        out.extend_from_slice(pattern);
        remaining -= pattern.len();
    }
    out.extend_from_slice(&pattern[..remaining]);
}

fn append_json_string(out: &mut Vec<u8>, s: &str) {
    serde_json::to_writer(&mut *out, s).expect("serializing a str into a Vec cannot fail");
}

/// Builds a valid `OpenAI` Chat Completions request body.
///
/// The first message is a system message carrying `params` as a directive.
/// With `stream && include_usage` the body contains the exact byte sequence
/// `"include_usage":true`, which is what the mock looks for.
pub fn chat_request_body(
    model: &str,
    stream: bool,
    include_usage: bool,
    params: &BenchParams,
    shape: BodyShape,
) -> Vec<u8> {
    let payload = match shape {
        BodyShape::Text { bytes } => bytes,
        BodyShape::Image {
            base64_bytes,
            text_bytes,
        } => base64_bytes + text_bytes,
    };
    let mut out = Vec::with_capacity(payload + 512);
    out.extend_from_slice(b"{\"model\":");
    append_json_string(&mut out, model);
    if stream {
        out.extend_from_slice(b",\"stream\":true");
        if include_usage {
            out.extend_from_slice(b",\"stream_options\":{\"include_usage\":true}");
        }
    } else {
        out.extend_from_slice(b",\"stream\":false");
    }
    out.extend_from_slice(b",\"messages\":[{\"role\":\"system\",\"content\":\"");
    out.extend_from_slice(params.to_directive().as_bytes());
    out.extend_from_slice(b"\"},{\"role\":\"user\",\"content\":");
    match shape {
        BodyShape::Text { bytes } => {
            out.push(b'"');
            append_repeated(&mut out, TEXT_PATTERN, bytes);
            out.push(b'"');
        }
        BodyShape::Image {
            base64_bytes,
            text_bytes,
        } => {
            out.extend_from_slice(b"[{\"type\":\"text\",\"text\":\"");
            append_repeated(&mut out, TEXT_PATTERN, text_bytes);
            out.extend_from_slice(
                b"\"},{\"type\":\"image_url\",\"image_url\":{\"url\":\"data:image/png;base64,",
            );
            append_repeated(&mut out, BASE64_PATTERN, base64_bytes / 4 * 4);
            out.extend_from_slice(b"\"}}]");
        }
    }
    out.extend_from_slice(b"}]}");
    out
}

#[cfg(test)]
mod tests {
    use base64::Engine as _;
    use proptest::prelude::*;

    use super::*;

    fn params() -> BenchParams {
        BenchParams {
            ttft_us: 300_000,
            interval_us: 33_333,
            chunks: 150,
            chunk_bytes: 96,
            sid: 42,
            resp_bytes: 1024,
        }
    }

    #[test]
    fn marker_layout_is_fixed() {
        let mut buf = [0u8; MARKER_LEN];
        encode_marker(&mut buf, 7, 3, 11, 13);
        assert_eq!(
            &buf[..],
            b"@b1:00000000000000000007:0000000003:00000000000000000011:00000000000000000013;"
        );
        patch_t_write(&mut buf, u64::MAX);
        let marker = Marker::parse(&buf).unwrap();
        assert_eq!(
            marker,
            Marker {
                sid: 7,
                seq: 3,
                t_sched: 11,
                t_write: u64::MAX
            }
        );
    }

    #[test]
    fn marker_parse_rejects_garbage() {
        let mut buf = [0u8; MARKER_LEN];
        encode_marker(&mut buf, 1, 2, 3, 4);
        let mut bad = buf;
        bad[10] = b'z';
        assert!(matches!(
            Marker::parse(&bad),
            Err(WireError::MalformedMarker { .. })
        ));
        let mut bad = buf;
        bad[TERMINATOR_OFFSET] = b'x';
        assert!(Marker::parse(&bad).is_err());
        let mut bad = buf;
        bad[SEQ_OFFSET..SEQ_OFFSET + SEQ_DIGITS].copy_from_slice(b"9999999999");
        assert_eq!(
            Marker::parse(&bad),
            Err(WireError::MarkerOverflow { field: "seq" })
        );
        let mut bad = buf;
        bad[SID_OFFSET..SID_OFFSET + SID_DIGITS].copy_from_slice(b"99999999999999999999");
        assert_eq!(
            Marker::parse(&bad),
            Err(WireError::MarkerOverflow { field: "sid" })
        );
    }

    #[test]
    fn marker_round_trip_extremes() {
        for (sid, seq, a, b) in [(0, 0, 0, 0), (u64::MAX, u32::MAX, u64::MAX, u64::MAX)] {
            let m = Marker {
                sid,
                seq,
                t_sched: a,
                t_write: b,
            };
            let mut buf = [0u8; MARKER_LEN];
            m.encode(&mut buf);
            assert_eq!(Marker::parse(&buf).unwrap(), m);
        }
    }

    #[test]
    fn append_content_pads_and_patches() {
        let mut out = b"prefix".to_vec();
        let m = Marker {
            sid: 1,
            seq: 2,
            t_sched: 3,
            t_write: 0,
        };
        let at = append_content(&mut out, &m, 100);
        assert_eq!(at, 6);
        assert_eq!(out.len(), 106);
        assert!(out[at + MARKER_LEN..].iter().all(|&b| b == b'x'));
        patch_t_write(&mut out[at..], 99);
        assert_eq!(
            Marker::parse(&out[at..at + MARKER_LEN]).unwrap().t_write,
            99
        );
    }

    #[test]
    fn scanner_keeps_last_possible_prefix() {
        let mut s = MarkerScanner::new();
        let mut seen = Vec::new();
        s.feed(b"zz@x@", |m| seen.push(m)).unwrap();
        assert_eq!(s.pending(), 1);
        let mut buf = [0u8; MARKER_LEN];
        encode_marker(&mut buf, 1, 1, 1, 1);
        s.feed(&buf[1..], |m| seen.push(m)).unwrap();
        assert_eq!(seen.len(), 1);
    }

    #[test]
    fn scanner_reports_error_on_bad_marker() {
        let mut s = MarkerScanner::new();
        let mut data = b"xx@b1:".to_vec();
        data.resize(data.len() + MARKER_LEN, b'0');
        assert!(s.feed(&data, |_| {}).is_err());
    }

    #[test]
    fn scanner_ignores_near_misses_across_boundaries() {
        let mut s = MarkerScanner::new();
        let mut seen = Vec::new();
        assert_eq!(s.feed(b"abc@b", |m| seen.push(m)).unwrap(), 0);
        assert_eq!(s.pending(), 2);
        assert_eq!(s.feed(b"x@b1", |m| seen.push(m)).unwrap(), 0);
        assert_eq!(s.pending(), 3);
        let mut buf = [0u8; MARKER_LEN];
        encode_marker(&mut buf, 5, 6, 7, 8);
        // The pending "@b1" is followed by "@b1:..." which breaks the prefix.
        assert_eq!(s.feed(&buf, |m| seen.push(m)).unwrap(), 1);
        assert_eq!(seen[0].sid, 5);
        assert_eq!(s.pending(), 0);
    }

    /// Builds a body with markers interleaved with SSE-like noise.
    fn stream_with_markers(markers: &[Marker], filler: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        for m in markers {
            out.extend_from_slice(b"data: {\"choices\":[{\"delta\":{\"content\":\"");
            append_content(&mut out, m, 90);
            out.extend_from_slice(b"\"}}]}\n\n");
            out.extend_from_slice(filler);
        }
        out
    }

    proptest! {
        #[test]
        fn scanner_is_split_invariant(
            raw in prop::collection::vec((any::<u64>(), any::<u32>(), any::<u64>(), any::<u64>()), 0..20),
            filler in prop::collection::vec(prop::sample::select(b"@b1:x;\n ".to_vec()), 0..12),
            cuts in prop::collection::vec(any::<prop::sample::Index>(), 0..40),
        ) {
            // Filler made of marker-ish bytes must never contain a full "@b1:".
            prop_assume!(memmem::find(&filler, MARKER_PREFIX).is_none());
            let markers: Vec<Marker> = raw
                .into_iter()
                .map(|(sid, seq, t_sched, t_write)| Marker { sid, seq, t_sched, t_write })
                .collect();
            let body = stream_with_markers(&markers, &filler);
            // Filler adjacent to the next marker's "data:" cannot form "@b1:"
            // either, so every hit is a real marker.
            let mut points: Vec<usize> = cuts.iter().map(|i| i.index(body.len() + 1)).collect();
            points.push(0);
            points.push(body.len());
            points.sort_unstable();
            points.dedup();

            let mut scanner = MarkerScanner::new();
            let mut seen = Vec::new();
            for w in points.windows(2) {
                scanner.feed(&body[w[0]..w[1]], |m| seen.push(m)).unwrap();
            }
            prop_assert_eq!(seen, markers);
        }

        #[test]
        fn directive_round_trip(
            ttft_us in any::<u64>(),
            interval_us in any::<u64>(),
            chunks in any::<u32>(),
            chunk_bytes in MARKER_LEN_U32..,
            sid in any::<u64>(),
            resp_bytes in any::<u32>(),
        ) {
            let p = BenchParams { ttft_us, interval_us, chunks, chunk_bytes, sid, resp_bytes };
            let body = format!("{{\"content\":\"{}\"}}", p.to_directive());
            prop_assert_eq!(find_directive(body.as_bytes()).unwrap(), Some(p));
        }
    }

    #[test]
    fn directive_errors() {
        assert_eq!(find_directive(b"{\"content\":\"hello\"}"), Ok(None));
        assert_eq!(
            find_directive(b"bench:v1;ttft_us=1"),
            Err(WireError::UnterminatedDirective)
        );
        let full = params().to_directive();
        let unknown = format!("\"{full};extra=1\"");
        assert_eq!(
            find_directive(unknown.as_bytes()),
            Err(WireError::UnknownKey("extra".into()))
        );
        let missing = "\"bench:v1;ttft_us=1;interval_us=2;chunks=3;chunk_bytes=100;sid=5\"";
        assert_eq!(
            find_directive(missing.as_bytes()),
            Err(WireError::MissingKey("resp_bytes"))
        );
        let dup = format!("\"{full};sid=9\"");
        assert_eq!(
            find_directive(dup.as_bytes()),
            Err(WireError::DuplicateKey("sid"))
        );
        let small = full.replace("chunk_bytes=96", "chunk_bytes=77");
        assert_eq!(
            find_directive(format!("\"{small}\"").as_bytes()),
            Err(WireError::ChunkBytesTooSmall(77))
        );
        let neg = full.replace("sid=42", "sid=-1");
        assert!(matches!(
            find_directive(format!("\"{neg}\"").as_bytes()),
            Err(WireError::InvalidValue { key: "sid", .. })
        ));
        let wide = full.replace("chunks=150", "chunks=4294967296");
        assert!(matches!(
            find_directive(format!("\"{wide}\"").as_bytes()),
            Err(WireError::InvalidValue { key: "chunks", .. })
        ));
    }

    #[test]
    fn text_body_is_valid_json_and_carries_params() {
        for (stream, usage) in [(true, true), (true, false), (false, false)] {
            let body = chat_request_body(
                "gpt-\"quoted\"",
                stream,
                usage,
                &params(),
                BodyShape::Text { bytes: 1000 },
            );
            let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(v["model"], "gpt-\"quoted\"");
            assert_eq!(v["stream"], stream);
            assert_eq!(v["messages"][0]["role"], "system");
            assert_eq!(v["messages"][1]["content"].as_str().unwrap().len(), 1000);
            let has_usage = memmem::find(&body, b"\"include_usage\":true").is_some();
            assert_eq!(has_usage, stream && usage);
            assert_eq!(find_directive(&body).unwrap(), Some(params()));
        }
    }

    #[test]
    fn image_body_is_valid_json_with_decodable_base64() {
        let body = chat_request_body(
            "m",
            true,
            true,
            &params(),
            BodyShape::Image {
                base64_bytes: 4003,
                text_bytes: 17,
            },
        );
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let parts = v["messages"][1]["content"].as_array().unwrap();
        assert_eq!(parts[0]["text"].as_str().unwrap().len(), 17);
        let url = parts[1]["image_url"]["url"].as_str().unwrap();
        let b64 = url.strip_prefix("data:image/png;base64,").unwrap();
        assert_eq!(b64.len(), 4000);
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(b64)
            .unwrap();
        assert_eq!(decoded.len(), 3000);
        assert_eq!(find_directive(&body).unwrap(), Some(params()));
    }
}
