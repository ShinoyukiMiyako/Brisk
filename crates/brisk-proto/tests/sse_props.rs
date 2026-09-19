//! Property tests for `SseScanner` (contract 05, 4.2 `sse_props`): random
//! event sequences with `data`, `event`, `id`, colon-less and comment lines,
//! each line ending in a random `\r\n`, `\n` or `\r`, cut into chunks at
//! arbitrary offsets (one-byte chunks and cuts between `\r` and `\n`
//! included), must frame exactly as the whole stream does.

mod common;

use proptest::collection::vec;
use proptest::prelude::*;
use proptest::sample::Index;
use proptest::test_runner::TestCaseError;

use common::sse_ref::{frame, verify_split};

const BOM: &[u8] = b"\xEF\xBB\xBF";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Eol {
    CrLf,
    Lf,
    Cr,
}

impl Eol {
    const fn bytes(self) -> &'static [u8] {
        match self {
            Self::CrLf => b"\r\n",
            Self::Lf => b"\n",
            Self::Cr => b"\r",
        }
    }
}

fn eol() -> impl Strategy<Value = Eol> {
    prop_oneof![Just(Eol::CrLf), Just(Eol::Lf), Just(Eol::Cr)]
}

/// One non-blank line.
#[derive(Debug, Clone)]
enum Line {
    /// `name:value` or `name: value`.
    Field {
        name: &'static [u8],
        space: bool,
        value: Vec<u8>,
    },
    /// A field name without a colon: a field with an empty value.
    Bare(&'static [u8]),
    /// `:` followed by text.
    Comment(Vec<u8>),
}

impl Line {
    fn bytes(&self) -> Vec<u8> {
        match self {
            Self::Field { name, space, value } => {
                let mut line = name.to_vec();
                line.push(b':');
                if *space {
                    line.push(b' ');
                }
                line.extend_from_slice(value);
                line
            }
            Self::Bare(name) => name.to_vec(),
            Self::Comment(text) => {
                let mut line = vec![b':'];
                line.extend_from_slice(text);
                line
            }
        }
    }

    /// The `data` value this line contributes, if it is a `data` field.
    fn data(&self) -> Option<Vec<u8>> {
        match self {
            Self::Field {
                name: b"data",
                space,
                value,
            } => Some(if *space {
                value.clone()
            } else {
                value.strip_prefix(b" ").unwrap_or(value).to_vec()
            }),
            Self::Bare(b"data") => Some(Vec::new()),
            _ => None,
        }
    }
}

/// Line content: any bytes except line endings, `:` and spaces included.
fn content() -> impl Strategy<Value = Vec<u8>> {
    vec(
        prop_oneof![
            4 => any::<u8>().prop_filter("no line endings", |byte| !matches!(byte, b'\r' | b'\n')),
            1 => Just(b':'),
            1 => Just(b' '),
        ],
        0..12,
    )
}

fn line() -> impl Strategy<Value = Line> {
    let name = prop_oneof![
        4 => Just(&b"data"[..]),
        1 => Just(&b"event"[..]),
        1 => Just(&b"id"[..]),
        1 => Just(&b"retry"[..]),
    ];
    prop_oneof![
        6 => (name.clone(), any::<bool>(), content())
            .prop_map(|(name, space, value)| Line::Field { name, space, value }),
        1 => name.prop_map(Line::Bare),
        2 => content().prop_map(Line::Comment),
    ]
}

/// The lines of one event, each with its line ending.
type Lines = Vec<(Line, Eol)>;

/// A generated stream: events of one or more lines, each followed by one or
/// more blank lines, optionally a trailing unfinished event.
#[derive(Debug, Clone)]
struct Stream {
    bom: bool,
    leading_blank: Vec<Eol>,
    events: Vec<(Lines, Vec<Eol>)>,
    unfinished: Lines,
    /// The unfinished event's last line has no line ending.
    open_line: bool,
}

/// Bytes of the stream and, for every finished event, its lines with
/// endings and its `data` values.
struct Rendered {
    bytes: Vec<u8>,
    events: Vec<(Vec<u8>, Vec<Vec<u8>>)>,
    pending: usize,
}

impl Stream {
    fn render(&self) -> Rendered {
        let mut out = Vec::new();
        if self.bom {
            out.extend_from_slice(BOM);
        }
        let mut events = Vec::new();
        // Written after a `\r`, a blank line ending in `\n` would merge into
        // one `\r\n` line ending, so it is written as `\r\n` instead.
        let blank = |out: &mut Vec<u8>, eol: Eol| {
            let eol = if eol == Eol::Lf && out.last() == Some(&b'\r') {
                Eol::CrLf
            } else {
                eol
            };
            out.extend_from_slice(eol.bytes());
        };
        for &eol in &self.leading_blank {
            blank(&mut out, eol);
        }
        for (lines, blanks) in &self.events {
            let start = out.len();
            let mut data = Vec::new();
            for (line, eol) in lines {
                out.extend_from_slice(&line.bytes());
                out.extend_from_slice(eol.bytes());
                data.extend(line.data());
            }
            events.push((out[start..].to_vec(), data));
            for &eol in blanks {
                blank(&mut out, eol);
            }
        }
        let start = out.len();
        for (index, (line, eol)) in self.unfinished.iter().enumerate() {
            out.extend_from_slice(&line.bytes());
            if !(self.open_line && index + 1 == self.unfinished.len()) {
                out.extend_from_slice(eol.bytes());
            }
        }
        Rendered {
            pending: out.len() - start,
            bytes: out,
            events,
        }
    }
}

fn stream() -> impl Strategy<Value = Stream> {
    let lines = || vec((line(), eol()), 1..4);
    (
        any::<bool>(),
        vec(eol(), 0..3),
        vec((lines(), vec(eol(), 1..3)), 0..8),
        vec((line(), eol()), 0..3),
        any::<bool>(),
    )
        .prop_map(
            |(bom, leading_blank, events, unfinished, open_line)| Stream {
                bom,
                leading_blank,
                events,
                unfinished,
                open_line,
            },
        )
}

/// Sorted, distinct cut offsets strictly inside a stream of `len` bytes.
fn cuts_from(len: usize, picks: &[Index]) -> Vec<usize> {
    if len < 2 {
        return Vec::new();
    }
    let mut cuts: Vec<usize> = picks.iter().map(|pick| 1 + pick.index(len - 1)).collect();
    cuts.sort_unstable();
    cuts.dedup();
    cuts
}

proptest! {
    /// The reference framer itself reproduces the generated structure.
    #[test]
    fn reference_frames_the_generated_events(stream in stream()) {
        let rendered = stream.render();
        let frames = frame(&rendered.bytes);
        let got: Vec<(Vec<u8>, Vec<Vec<u8>>)> = frames
            .events
            .into_iter()
            .map(|event| (event.raw, event.data))
            .collect();
        prop_assert_eq!(got, rendered.events);
        prop_assert_eq!(frames.pending, rendered.pending);
    }

    #[test]
    fn random_cuts_frame_like_the_whole_stream(
        stream in stream(),
        picks in vec(any::<Index>(), 0..16),
    ) {
        let bytes = stream.render().bytes;
        verify_split(&bytes, &[]).map_err(TestCaseError::fail)?;
        verify_split(&bytes, &cuts_from(bytes.len(), &picks)).map_err(TestCaseError::fail)?;
    }

    #[test]
    fn one_byte_chunks_frame_like_the_whole_stream(stream in stream()) {
        let bytes = stream.render().bytes;
        let cuts: Vec<usize> = (1..bytes.len()).collect();
        verify_split(&bytes, &cuts).map_err(TestCaseError::fail)?;
    }

    /// Every chunk but the last ends in the `\r` of a `\r\n`.
    #[test]
    fn cuts_between_cr_and_lf_frame_like_the_whole_stream(stream in stream()) {
        let bytes = stream.render().bytes;
        let cuts: Vec<usize> = bytes
            .windows(2)
            .enumerate()
            .filter(|(_, pair)| pair == b"\r\n")
            .map(|(at, _)| at + 1)
            .collect();
        verify_split(&bytes, &cuts).map_err(TestCaseError::fail)?;
    }

    /// Arbitrary bytes drawn mostly from SSE syntax, a split byte order mark
    /// included, against the reference framer.
    #[test]
    fn arbitrary_bytes_frame_like_the_reference(
        bytes in vec(
            prop_oneof![
                Just(b'\r'), Just(b'\n'), Just(b':'), Just(b' '), Just(b'd'),
                Just(0xEF), Just(0xBB), Just(0xBF), any::<u8>(),
            ],
            0..64,
        ),
        picks in vec(any::<Index>(), 0..16),
    ) {
        verify_split(&bytes, &cuts_from(bytes.len(), &picks)).map_err(TestCaseError::fail)?;
    }
}
