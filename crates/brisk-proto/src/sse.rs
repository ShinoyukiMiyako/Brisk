//! Incremental Server-Sent Events framing over arbitrarily split body chunks.
//!
//! Events are found by scanning for line endings only, without parsing their
//! payloads, and a `\r` at the end of one chunk is held until the next chunk
//! shows whether a `\n` completes it (R6, R7).
//!
//! Framing follows the WHATWG event-stream format: lines end in `\r\n`, `\n`
//! or `\r`; a blank line ends an event; a leading UTF-8 byte order mark is
//! ignored. Unlike a browser, the scanner reports events that hold only
//! comments, so keep-alives are visible to the caller.

use bytes::BytesMut;
use memchr::{memchr, memchr2};

/// Largest event (including a partial one carried across chunks), per R7.
pub const MAX_EVENT_BYTES: usize = 1 << 20;

const BOM: &[u8] = b"\xEF\xBB\xBF";

/// Incremental SSE event framer over body chunks.
///
/// Feed every chunk of one body in order with [`SseScanner::feed`], then call
/// [`SseScanner::finish`]. After [`SseError`] the stream is malformed and
/// must not be fed further.
#[derive(Debug, Default)]
pub struct SseScanner {
    /// The unfinished event carried over from earlier chunks, line endings
    /// included; while `bom_resolved` is false, the bytes of a possible byte
    /// order mark instead. Only ever cleared and extended, so its capacity is
    /// reused by the next event that spans chunks (R9).
    carry: BytesMut,
    /// Where the bytes fed so far end relative to events and lines.
    progress: Progress,
    /// The previous chunk ended in `\r`, so a `\n` opening the next chunk
    /// belongs to that line ending.
    pending_cr: bool,
    /// The start of the stream has been checked for a byte order mark.
    bom_resolved: bool,
}

/// Where the bytes fed so far end.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum Progress {
    /// Between events: no line of a next event has been seen.
    #[default]
    Between,
    /// Inside an event, at the start of a line.
    LineStart,
    /// Inside an event, within a line that has no line ending yet.
    MidLine,
}

/// One complete event, valid only during the callback.
#[derive(Debug, Clone, Copy)]
pub struct Event<'a> {
    /// The event's lines, each with its line ending, excluding the blank
    /// line that ended it.
    pub raw: &'a [u8],
    /// Offset in the current chunk where the event starts; `None` if it
    /// started in an earlier chunk.
    pub start: Option<usize>,
    /// Offset in the current chunk just past the blank line that ended it.
    /// When that blank line ends in a `\r` at the end of the chunk, a `\n`
    /// opening the next chunk still belongs to it.
    pub end: usize,
}

/// The `data` of an event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Data<'a> {
    /// No `data` field.
    None,
    /// The value of the only `data` field, one leading space removed.
    Single(&'a [u8]),
    /// Several `data` fields; use [`Event::join_data`].
    Multi,
}

impl<'a> Event<'a> {
    /// Only comment lines (`:` prefix).
    pub fn is_comment_only(&self) -> bool {
        lines(self.raw).all(|line| line.first() == Some(&b':'))
    }

    /// The `data` field or fields of the event.
    pub fn data(&self) -> Data<'a> {
        let mut values = self.values(b"data");
        match (values.next(), values.next()) {
            (None, _) => Data::None,
            (Some(value), None) => Data::Single(value),
            (Some(_), Some(_)) => Data::Multi,
        }
    }

    /// Joins several `data` values with `\n` into `out` (cleared first).
    pub fn join_data(&self, out: &mut Vec<u8>) {
        out.clear();
        for (index, value) in self.values(b"data").enumerate() {
            if index > 0 {
                out.push(b'\n');
            }
            out.extend_from_slice(value);
        }
    }

    /// First value of field `name`, e.g. `b"event"`.
    pub fn field(&self, name: &[u8]) -> Option<&'a [u8]> {
        self.values(name).next()
    }

    fn values(&self, name: &[u8]) -> impl Iterator<Item = &'a [u8]> {
        lines(self.raw).filter_map(move |line| {
            let (field, value) = split_field(line)?;
            (field == name).then_some(value)
        })
    }
}

/// The lines of `raw` without their line endings.
fn lines(raw: &[u8]) -> impl Iterator<Item = &[u8]> {
    let mut rest = raw;
    std::iter::from_fn(move || {
        if rest.is_empty() {
            return None;
        }
        let Some(eol) = memchr2(b'\n', b'\r', rest) else {
            let line = rest;
            rest = &[];
            return Some(line);
        };
        let line = &rest[..eol];
        let next = if rest[eol] == b'\r' && rest.get(eol + 1) == Some(&b'\n') {
            eol + 2
        } else {
            eol + 1
        };
        rest = &rest[next..];
        Some(line)
    })
}

/// Field name and value of a non-comment line. A line without a colon is a
/// field with an empty value; one space after the colon is not part of the
/// value.
fn split_field(line: &[u8]) -> Option<(&[u8], &[u8])> {
    if line.first() == Some(&b':') {
        return None;
    }
    Some(match memchr(b':', line) {
        None => (line, &[]),
        Some(colon) => {
            let value = &line[colon + 1..];
            (&line[..colon], value.strip_prefix(b" ").unwrap_or(value))
        }
    })
}

/// Where the last event boundary in a chunk was.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkScan {
    /// Offset in the chunk just past the last event boundary in it.
    pub last_boundary: Option<usize>,
}

/// How the body ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndState {
    /// The body ended between events.
    Clean,
    /// Bytes of an unfinished event remained at end of body.
    Truncated {
        /// Number of those bytes.
        pending: usize,
    },
}

/// Why the stream cannot be framed.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SseError {
    /// An event, finished or not, grew past [`MAX_EVENT_BYTES`].
    #[error("SSE event exceeds {limit} bytes")]
    EventTooLarge {
        /// The limit that was exceeded.
        limit: usize,
    },
}

const TOO_LARGE: SseError = SseError::EventTooLarge {
    limit: MAX_EVENT_BYTES,
};

impl SseScanner {
    /// A scanner at the start of a stream. Allocates nothing until an event
    /// spans two chunks.
    pub fn new() -> Self {
        Self::default()
    }

    /// Frames `chunk`, calling `on_event` for every event it completes, in
    /// order. Events made only of blank lines are not reported.
    ///
    /// Copies bytes only for an event that spans chunks, into a buffer whose
    /// capacity is kept, so this allocates only when such an event is larger
    /// than every earlier one.
    pub fn feed<F>(&mut self, chunk: &[u8], mut on_event: F) -> Result<ChunkScan, SseError>
    where
        F: FnMut(Event<'_>),
    {
        let mut scan = ChunkScan {
            last_boundary: None,
        };
        if chunk.is_empty() {
            return Ok(scan);
        }

        let mut pos = 0;
        if !self.bom_resolved {
            match self.skip_bom(chunk) {
                Some(skipped) => pos = skipped,
                None => return Ok(scan),
            }
        }
        if self.pending_cr {
            self.pending_cr = false;
            if chunk[0] == b'\n' {
                pos = 1;
                if self.progress == Progress::Between {
                    scan.last_boundary = Some(pos);
                }
            }
        }

        // Where the current event began in this chunk; `None` while it is
        // carried from an earlier chunk (or while there is none).
        let mut event_start = None;
        while let Some(found) = memchr2(b'\n', b'\r', &chunk[pos..]) {
            let eol = pos + found;
            let next = if chunk[eol] == b'\n' {
                eol + 1
            } else if let Some(&after) = chunk.get(eol + 1) {
                if after == b'\n' { eol + 2 } else { eol + 1 }
            } else {
                self.pending_cr = true;
                eol + 1
            };

            if eol > pos || self.progress == Progress::MidLine {
                if self.progress == Progress::Between {
                    event_start = Some(pos);
                }
                self.progress = Progress::LineStart;
            } else {
                if self.progress == Progress::LineStart {
                    let raw = if let Some(start) = event_start {
                        &chunk[start..pos]
                    } else {
                        if self.carry.len() + pos > MAX_EVENT_BYTES {
                            return Err(TOO_LARGE);
                        }
                        self.carry.extend_from_slice(&chunk[..pos]);
                        &self.carry[..]
                    };
                    if raw.len() > MAX_EVENT_BYTES {
                        return Err(TOO_LARGE);
                    }
                    on_event(Event {
                        raw,
                        start: event_start,
                        end: next,
                    });
                    self.carry.clear();
                    self.progress = Progress::Between;
                    event_start = None;
                }
                scan.last_boundary = Some(next);
            }
            pos = next;
        }

        if pos < chunk.len() {
            if self.progress == Progress::Between {
                event_start = Some(pos);
            }
            self.progress = Progress::MidLine;
        }
        if self.progress != Progress::Between {
            // An event carried from an earlier chunk continues from the
            // first byte of this one.
            let from = event_start.unwrap_or(0);
            if self.carry.len() + (chunk.len() - from) > MAX_EVENT_BYTES {
                return Err(TOO_LARGE);
            }
            self.carry.extend_from_slice(&chunk[from..]);
        }
        Ok(scan)
    }

    /// Everything fed so far ends exactly on an event boundary, with no
    /// pending `\r` whose meaning depends on the next byte.
    pub fn at_boundary(&self) -> bool {
        self.progress == Progress::Between && !self.pending_cr && self.carry.is_empty()
    }

    /// Bytes of the unfinished event carried so far.
    pub fn pending_bytes(&self) -> usize {
        self.carry.len()
    }

    /// Call once at end of body. Also resets the scanner, keeping its
    /// buffer, so it can frame another stream.
    pub fn finish(&mut self) -> EndState {
        let pending = self.carry.len();
        self.carry.clear();
        self.progress = Progress::Between;
        self.pending_cr = false;
        self.bom_resolved = false;
        if pending == 0 {
            EndState::Clean
        } else {
            EndState::Truncated { pending }
        }
    }

    /// Matches the start of the stream against the byte order mark, which
    /// may itself be split across chunks. Returns how many bytes of `chunk`
    /// to skip, or `None` when all of `chunk` is still a prefix of the mark
    /// (it is then held in `carry`).
    fn skip_bom(&mut self, chunk: &[u8]) -> Option<usize> {
        let held = self.carry.len();
        let wanted = &BOM[held..];
        let common = wanted
            .iter()
            .zip(chunk)
            .take_while(|(want, byte)| want == byte)
            .count();
        if common == wanted.len() {
            self.carry.clear();
            self.bom_resolved = true;
            return Some(common);
        }
        if common == chunk.len() {
            self.carry.extend_from_slice(chunk);
            return None;
        }
        // Not a byte order mark after all. Bytes held from earlier chunks
        // begin the first line; the ones in this chunk are scanned again as
        // ordinary content.
        self.bom_resolved = true;
        if held > 0 {
            self.progress = Progress::MidLine;
        }
        Some(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Owned copy of one reported event.
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct Seen {
        raw: Vec<u8>,
        start: Option<usize>,
        end: usize,
    }

    fn feed(scanner: &mut SseScanner, chunk: &[u8]) -> (Vec<Seen>, ChunkScan) {
        let mut seen = Vec::new();
        let scan = scanner
            .feed(chunk, |event| {
                seen.push(Seen {
                    raw: event.raw.to_vec(),
                    start: event.start,
                    end: event.end,
                });
            })
            .unwrap();
        (seen, scan)
    }

    #[test]
    fn single_chunk_offsets() {
        let mut scanner = SseScanner::new();
        let (seen, scan) = feed(&mut scanner, b"data: a\n\ndata: b\r\n\r\n");
        assert_eq!(
            seen,
            [
                Seen {
                    raw: b"data: a\n".to_vec(),
                    start: Some(0),
                    end: 9
                },
                Seen {
                    raw: b"data: b\r\n".to_vec(),
                    start: Some(9),
                    end: 20
                },
            ]
        );
        assert_eq!(scan.last_boundary, Some(20));
        assert!(scanner.at_boundary());
        assert_eq!(scanner.finish(), EndState::Clean);
    }

    #[test]
    fn carried_event_reports_no_start() {
        let mut scanner = SseScanner::new();
        let (seen, scan) = feed(&mut scanner, b"data: {\"a\"");
        assert!(seen.is_empty());
        assert_eq!(scan.last_boundary, None);
        assert_eq!(scanner.pending_bytes(), 10);
        assert!(!scanner.at_boundary());

        let (seen, scan) = feed(&mut scanner, b":1}\n\nda");
        assert_eq!(
            seen,
            [Seen {
                raw: b"data: {\"a\":1}\n".to_vec(),
                start: None,
                end: 5
            }]
        );
        assert_eq!(scan.last_boundary, Some(5));
        assert_eq!(scanner.pending_bytes(), 2);
        assert_eq!(scanner.finish(), EndState::Truncated { pending: 2 });
    }

    #[test]
    fn crlf_split_between_chunks() {
        let mut scanner = SseScanner::new();
        let (seen, _) = feed(&mut scanner, b"data: x\r");
        assert!(seen.is_empty());
        assert!(!scanner.at_boundary());
        let (seen, scan) = feed(&mut scanner, b"\n\r");
        assert_eq!(
            seen,
            [Seen {
                raw: b"data: x\r\n".to_vec(),
                start: None,
                end: 2
            }]
        );
        assert_eq!(scan.last_boundary, Some(2));
        // The trailing `\r` ended the blank line, but a `\n` may still follow.
        assert!(!scanner.at_boundary());
        let (seen, scan) = feed(&mut scanner, b"\n");
        assert!(seen.is_empty());
        assert_eq!(scan.last_boundary, Some(1));
        assert!(scanner.at_boundary());
    }

    #[test]
    fn bom_split_across_chunks_is_skipped() {
        let mut scanner = SseScanner::new();
        let (seen, _) = feed(&mut scanner, b"\xEF");
        assert!(seen.is_empty());
        let (seen, _) = feed(&mut scanner, b"\xBB");
        assert!(seen.is_empty());
        let (seen, _) = feed(&mut scanner, b"\xBFdata: a\n\n");
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].raw, b"data: a\n");
        assert_eq!(seen[0].start, Some(1));
    }

    #[test]
    fn partial_bom_that_is_not_one_starts_the_first_line() {
        let mut scanner = SseScanner::new();
        feed(&mut scanner, b"\xEF\xBB");
        let (seen, _) = feed(&mut scanner, b"x\n\n");
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].raw, b"\xEF\xBBx\n");
        assert_eq!(seen[0].start, None);

        let mut scanner = SseScanner::new();
        let (seen, _) = feed(&mut scanner, b"\xEFx\n\n");
        assert_eq!(seen[0].raw, b"\xEFx\n");
        assert_eq!(seen[0].start, Some(0));
    }

    #[test]
    fn bom_only_later_in_the_stream_is_content() {
        let mut scanner = SseScanner::new();
        feed(&mut scanner, b"\n");
        let (seen, _) = feed(&mut scanner, b"\xEF\xBB\xBFdata: a\n\n");
        assert_eq!(seen[0].raw, b"\xEF\xBB\xBFdata: a\n");
    }

    #[test]
    fn event_accessors() {
        let mut scanner = SseScanner::new();
        let mut checked = 0;
        scanner
            .feed(
                b": note\nevent: delta\r\ndata:one\rdata:  two\nid\n\n",
                |event| {
                    assert!(!event.is_comment_only());
                    assert_eq!(event.field(b"event"), Some(&b"delta"[..]));
                    assert_eq!(event.field(b"id"), Some(&b""[..]));
                    assert_eq!(event.field(b"retry"), None);
                    assert_eq!(event.data(), Data::Multi);
                    let mut joined = vec![b'x'];
                    event.join_data(&mut joined);
                    assert_eq!(joined, b"one\n two");
                    checked += 1;
                },
            )
            .unwrap();
        assert_eq!(checked, 1);
    }

    #[test]
    fn too_large_event_in_one_chunk() {
        let mut scanner = SseScanner::new();
        let mut chunk = b"data: ".to_vec();
        chunk.resize(MAX_EVENT_BYTES + 1, b'x');
        chunk.extend_from_slice(b"\n\n");
        assert_eq!(
            scanner.feed(&chunk, |_| {}),
            Err(SseError::EventTooLarge {
                limit: MAX_EVENT_BYTES
            })
        );
    }

    #[test]
    fn event_of_exactly_the_limit_is_accepted() {
        let mut scanner = SseScanner::new();
        let mut chunk = b"data: ".to_vec();
        chunk.resize(MAX_EVENT_BYTES - 1, b'x');
        chunk.push(b'\n');
        let half = chunk.len() / 2;
        let mut sizes = Vec::new();
        scanner.feed(&chunk[..half], |_| {}).unwrap();
        scanner
            .feed(&chunk[half..], |event| sizes.push(event.raw.len()))
            .unwrap();
        scanner
            .feed(b"\n", |event| sizes.push(event.raw.len()))
            .unwrap();
        assert_eq!(sizes, [MAX_EVENT_BYTES]);
    }
}
