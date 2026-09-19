//! Reference SSE framer over a whole stream, and the split-invariance check
//! built on it (contract 05, 4.2 `sse_props` and 4.8 `sse`).
//!
//! The reference sees the entire stream at once, so it needs none of the
//! scanner's carry and pending-`\r` machinery: it splits lines on `\r\n`,
//! `\n` or `\r`, ends an event at each blank line, and records the offset
//! after every blank line as a boundary.

use brisk_proto::sse::{Data, EndState, SseScanner};

const BOM: &[u8] = b"\xEF\xBB\xBF";

/// One event as the reference frames it, offsets relative to the stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RefEvent {
    /// The event's lines with their line endings.
    pub(crate) raw: Vec<u8>,
    pub(crate) start: usize,
    /// Just past the blank line that ended it.
    pub(crate) end: usize,
    /// Every `data` value, in order.
    pub(crate) data: Vec<Vec<u8>>,
    pub(crate) event: Option<Vec<u8>>,
    pub(crate) comment_only: bool,
}

/// The reference framing of a whole stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RefFrames {
    pub(crate) events: Vec<RefEvent>,
    /// Offsets just past every blank line, in order.
    pub(crate) boundaries: Vec<usize>,
    /// Bytes of an unfinished event at end of stream.
    pub(crate) pending: usize,
}

pub(crate) fn frame(stream: &[u8]) -> RefFrames {
    let mut pos = if stream.starts_with(BOM) {
        BOM.len()
    } else {
        0
    };
    let mut events = Vec::new();
    let mut boundaries = Vec::new();
    let mut event_start = None;
    while pos < stream.len() {
        let Some(eol) = stream[pos..]
            .iter()
            .position(|&byte| byte == b'\n' || byte == b'\r')
            .map(|found| pos + found)
        else {
            event_start.get_or_insert(pos);
            break;
        };
        let next = if stream[eol] == b'\r' && stream.get(eol + 1) == Some(&b'\n') {
            eol + 2
        } else {
            eol + 1
        };
        if eol > pos {
            event_start.get_or_insert(pos);
        } else {
            if let Some(start) = event_start.take() {
                events.push(describe(&stream[start..pos], start, next));
            }
            boundaries.push(next);
        }
        pos = next;
    }
    RefFrames {
        events,
        boundaries,
        pending: event_start.map_or(0, |start| stream.len() - start),
    }
}

fn describe(raw: &[u8], start: usize, end: usize) -> RefEvent {
    let mut data = Vec::new();
    let mut event = None;
    let mut comment_only = true;
    for line in ref_lines(raw) {
        if line.first() == Some(&b':') {
            continue;
        }
        comment_only = false;
        let (name, value) = match line.iter().position(|&byte| byte == b':') {
            None => (line, &[][..]),
            Some(colon) => {
                let value = &line[colon + 1..];
                (&line[..colon], value.strip_prefix(b" ").unwrap_or(value))
            }
        };
        match name {
            b"data" => data.push(value.to_vec()),
            b"event" if event.is_none() => event = Some(value.to_vec()),
            _ => {}
        }
    }
    RefEvent {
        raw: raw.to_vec(),
        start,
        end,
        data,
        event,
        comment_only,
    }
}

/// Lines of an event without their endings; every line of `raw` has one.
fn ref_lines(raw: &[u8]) -> Vec<&[u8]> {
    let mut lines = Vec::new();
    let mut pos = 0;
    while let Some(found) = raw[pos..]
        .iter()
        .position(|&byte| byte == b'\n' || byte == b'\r')
    {
        let eol = pos + found;
        lines.push(&raw[pos..eol]);
        pos = if raw[eol] == b'\r' && raw.get(eol + 1) == Some(&b'\n') {
            eol + 2
        } else {
            eol + 1
        };
    }
    if pos < raw.len() {
        lines.push(&raw[pos..]);
    }
    lines
}

/// Feeds `stream` to one scanner in the chunks that `cuts` (sorted offsets
/// strictly inside the stream) delimit and checks everything it reports
/// against [`frame`]: each event's raw bytes, data, `event` field and
/// comment-only flag, where it starts and ends, each chunk's
/// `last_boundary`, and the end state.
///
/// A blank line ending in `\r\n` split between the two bytes ends in the
/// first chunk (the scanner cannot wait for the `\n`), so that event's `end`
/// and that chunk's boundary are one byte earlier than the reference's; the
/// check allows exactly that.
pub(crate) fn verify_split(stream: &[u8], cuts: &[usize]) -> Result<(), String> {
    let reference = frame(stream);
    let crlf_split_at = |offset: usize| {
        offset > 0
            && stream.get(offset - 1) == Some(&b'\r')
            && stream.get(offset) == Some(&b'\n')
            && reference.boundaries.contains(&(offset + 1))
    };

    let mut scanner = SseScanner::new();
    let mut seen = 0;
    let mut chunk_start = 0;
    let ends = cuts.iter().copied().chain(std::iter::once(stream.len()));
    for chunk_end in ends {
        let chunk = &stream[chunk_start..chunk_end];
        let mut failure = None;
        let scan = scanner
            .feed(chunk, |event| {
                if failure.is_some() {
                    return;
                }
                failure = check_event(
                    &reference,
                    seen,
                    chunk_start,
                    chunk_end,
                    &event,
                    &crlf_split_at,
                )
                .err();
                seen += 1;
            })
            .map_err(|error| format!("feed failed at {chunk_start}: {error}"))?;
        if let Some(failure) = failure {
            return Err(failure);
        }

        let mut want = reference
            .boundaries
            .iter()
            .copied()
            .filter(|&boundary| chunk_start < boundary && boundary <= chunk_end)
            .max();
        if crlf_split_at(chunk_end) && chunk_end > chunk_start {
            want = Some(want.map_or(chunk_end, |boundary| boundary.max(chunk_end)));
        }
        let got = scan.last_boundary.map(|offset| chunk_start + offset);
        if got != want {
            return Err(format!(
                "chunk {chunk_start}..{chunk_end}: last_boundary at {got:?}, want {want:?}"
            ));
        }
        chunk_start = chunk_end;
    }

    if seen != reference.events.len() {
        return Err(format!(
            "{seen} events reported, reference has {}",
            reference.events.len()
        ));
    }
    let want_end = if reference.pending == 0 {
        EndState::Clean
    } else {
        EndState::Truncated {
            pending: reference.pending,
        }
    };
    let got_end = scanner.finish();
    if got_end != want_end {
        return Err(format!("finish() is {got_end:?}, want {want_end:?}"));
    }
    Ok(())
}

fn check_event(
    reference: &RefFrames,
    index: usize,
    chunk_start: usize,
    chunk_end: usize,
    event: &brisk_proto::sse::Event<'_>,
    crlf_split_at: &impl Fn(usize) -> bool,
) -> Result<(), String> {
    let want = reference
        .events
        .get(index)
        .ok_or_else(|| format!("extra event {:?}", String::from_utf8_lossy(event.raw)))?;
    let context = || format!("event {index} ({:?})", String::from_utf8_lossy(&want.raw));

    if event.raw != want.raw.as_slice() {
        return Err(format!(
            "{}: raw {:?}",
            context(),
            String::from_utf8_lossy(event.raw)
        ));
    }
    let start_ok = match event.start {
        Some(start) => chunk_start + start == want.start,
        None => want.start < chunk_start,
    };
    if !start_ok {
        return Err(format!(
            "{}: start {:?} in chunk at {chunk_start}",
            context(),
            event.start
        ));
    }
    let end = chunk_start + event.end;
    let end_ok = end == want.end || (end + 1 == want.end && end == chunk_end && crlf_split_at(end));
    if !end_ok {
        return Err(format!("{}: end {end}, want {}", context(), want.end));
    }

    let data_ok = match (event.data(), want.data.as_slice()) {
        (Data::Single(value), [only]) => value == only.as_slice(),
        (Data::None, []) | (Data::Multi, [_, _, ..]) => true,
        _ => false,
    };
    let mut joined = Vec::new();
    event.join_data(&mut joined);
    if !data_ok || joined != want.data.join(&b'\n') {
        return Err(format!("{}: data {:?}", context(), event.data()));
    }
    if event.field(b"event") != want.event.as_deref() {
        return Err(format!(
            "{}: event field {:?}",
            context(),
            event.field(b"event")
        ));
    }
    if event.is_comment_only() != want.comment_only {
        return Err(format!(
            "{}: is_comment_only {}",
            context(),
            event.is_comment_only()
        ));
    }
    Ok(())
}
