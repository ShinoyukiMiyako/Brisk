//! The per-chunk path of a streamed response (R6, R7, R9): every upstream
//! frame is framed into SSE events, the few events that can carry usage or
//! `finish_reason` are parsed, and in strip mode the usage-only events the
//! gateway asked for itself are cut out.
//!
//! Nothing here reads a clock, logs, touches an atomic, takes a lock, or
//! allocates in the steady state; `scripts/check-hotpath.sh` checks this file
//! line by line (section 4.10).

use std::collections::VecDeque;

use brisk_proto::UsageErrorKind;
use brisk_proto::sse::{Data, EndState, Event, SseError, SseScanner};
use brisk_proto::usage::{UsageAcc, may_carry_finish, may_carry_usage, parse_chunk};
use bytes::{Buf, Bytes};

/// Usage facts gathered from the events of one stream.
#[derive(Debug, Default)]
pub(crate) struct Facts {
    /// The most recent usage parsed successfully.
    pub(crate) usage: UsageAcc,
    /// Some event carried a non-null `finish_reason`.
    pub(crate) finished: bool,
    /// The usage is final (D21): it arrived in the event that carried
    /// `finish_reason`, or after it.
    pub(crate) usage_final: bool,
    /// `data: [DONE]` was seen.
    pub(crate) done: bool,
    /// Kind of the first usage candidate that failed to parse.
    pub(crate) usage_error: Option<UsageErrorKind>,
}

impl Facts {
    /// Takes in one complete event. Returns true for a usage-only event,
    /// which strip mode removes from the stream.
    fn observe(&mut self, event: &Event<'_>, joined: &mut Vec<u8>) -> bool {
        let payload = match event.data() {
            Data::None => return false,
            Data::Single(payload) => payload,
            Data::Multi => {
                // Chat streams never split a payload over several `data`
                // lines; the buffer keeps its capacity if one ever does.
                event.join_data(joined);
                joined.as_slice()
            }
        };
        if payload == b"[DONE]" {
            self.done = true;
            return false;
        }
        if !may_carry_usage(payload) && !may_carry_finish(payload) {
            return false;
        }
        match parse_chunk(payload) {
            Ok(chunk) => {
                let finished_before = self.finished;
                self.finished |= chunk.finished;
                if let Some(usage) = chunk.usage {
                    self.usage.observe(usage);
                    self.usage_final |= chunk.finished || finished_before;
                }
                chunk.usage_only
            }
            Err(error) => {
                // The event is forwarded untouched and settlement turns
                // conservative; the Outcome carries the first error's kind.
                if self.usage_error.is_none() {
                    self.usage_error = Some(error.kind());
                }
                false
            }
        }
    }
}

/// Framing and usage state of one stream.
#[derive(Debug, Default)]
pub(crate) struct StreamScan {
    scanner: SseScanner,
    /// What the events seen so far said.
    pub(crate) facts: Facts,
    /// The joined value of an event with several `data` lines.
    joined: Vec<u8>,
}

impl StreamScan {
    /// Frames `chunk` and observes the events it completes. The chunk
    /// itself is forwarded unchanged by the caller.
    pub(crate) fn scan(&mut self, chunk: &[u8]) -> Result<(), SseError> {
        let Self {
            scanner,
            facts,
            joined,
        } = self;
        scanner.feed(chunk, |event| {
            facts.observe(&event, joined);
        })?;
        Ok(())
    }

    /// Ends the stream.
    pub(crate) fn finish(&mut self) -> EndState {
        self.scanner.finish()
    }
}

/// Staged pieces one unfinished event may occupy. An event spread over more
/// upstream chunks than this is no longer held piece by piece: its bytes are
/// copied out of the scanner once, when it completes (1.4.10, strip rule 5).
const STAGE_SLOTS: usize = 8;

/// Queue capacity reserved at construction: the staged pieces, plus the
/// frames one chunk adds next to them (the forwarded part, the part before a
/// removed event, the event copied out of the scanner, and the new tail).
const QUEUE_SLOTS: usize = STAGE_SLOTS + 4;

/// Strip mode: forwards each chunk only up to its last event boundary, holds
/// the unfinished tail as zero-copy slices, and removes usage-only events.
#[derive(Debug)]
pub(crate) struct Strip {
    /// Frames ready to send, followed by the staged pieces of the unfinished
    /// event. Staged pieces hold every byte after the last boundary.
    queue: VecDeque<Bytes>,
    /// Frames at the front of `queue` that are ready to send.
    ready: usize,
    /// The unfinished event outgrew [`STAGE_SLOTS`]: its bytes are only in
    /// the scanner's carry, and are copied from there when it completes.
    spilled: bool,
    /// A removed event ended with `\r` at the end of a chunk, so a `\n`
    /// opening the next chunk belongs to that event and is removed too.
    cut_lf: bool,
    /// The last byte of the stream so far.
    last_byte: u8,
}

impl Strip {
    /// Reserves the queue once, so the per-chunk path never grows it.
    pub(crate) fn new() -> Self {
        Self {
            queue: VecDeque::with_capacity(QUEUE_SLOTS),
            ready: 0,
            spilled: false,
            cut_lf: false,
            last_byte: 0,
        }
    }

    /// The next frame to send downstream.
    pub(crate) fn pop_ready(&mut self) -> Option<Bytes> {
        if self.ready == 0 {
            return None;
        }
        self.ready -= 1;
        self.queue.pop_front()
    }

    /// No frame is waiting to be sent.
    pub(crate) fn is_drained(&self) -> bool {
        self.ready == 0
    }

    /// Bytes of the frames waiting to be sent.
    pub(crate) fn ready_bytes(&self) -> u64 {
        self.queue
            .iter()
            .take(self.ready)
            .map(|frame| frame.len() as u64)
            .sum()
    }

    /// Drops everything queued or staged; the stream ended with an error.
    pub(crate) fn clear(&mut self) {
        self.queue.clear();
        self.ready = 0;
    }

    /// Frames `chunk`, queues what can be sent and stages its unfinished
    /// tail. Call only once every ready frame has been sent.
    pub(crate) fn push(&mut self, scan: &mut StreamScan, chunk: Bytes) -> Result<(), SseError> {
        debug_assert_eq!(self.ready, 0, "ready frames go out before the next chunk");
        let Some(&last_byte) = chunk.last() else {
            return Ok(());
        };
        self.last_byte = last_byte;
        let StreamScan {
            scanner,
            facts,
            joined,
        } = scan;
        // Bytes of the unfinished event carried from earlier chunks; the
        // staged pieces end with exactly these.
        let carried = scanner.pending_bytes();
        let mut cursor = 0;
        if std::mem::take(&mut self.cut_lf) && chunk[0] == b'\n' {
            cursor = 1;
        }
        let framed = scanner.feed(&chunk, |event| {
            let cut = facts.observe(&event, joined);
            match event.start {
                Some(start) => {
                    if cut && start > cursor {
                        self.push_ready(chunk.slice(cursor..start));
                    }
                }
                // The event began in an earlier chunk, so it is the first one
                // this chunk completes and nothing of the chunk precedes it.
                None => {
                    if std::mem::take(&mut self.spilled) {
                        if !cut {
                            self.push_ready(Bytes::copy_from_slice(&event.raw[..carried]));
                        }
                    } else if cut {
                        self.drop_staged_event(carried);
                    } else {
                        self.ready = self.queue.len();
                    }
                }
            }
            if cut {
                cursor = event.end;
                self.cut_lf = event.end == chunk.len() && chunk.ends_with(b"\r");
            }
        })?;

        let boundary = framed
            .last_boundary
            .map_or(cursor, |boundary| boundary.max(cursor));
        let mut rest = chunk;
        rest.advance(cursor);
        if boundary > cursor {
            let complete = rest.split_to(boundary - cursor);
            self.push_ready(complete);
        }
        if !rest.is_empty() {
            self.stage(rest, scanner.pending_bytes());
        }
        Ok(())
    }

    /// Releases what is held at end of body: the unfinished tail goes out
    /// as it is.
    pub(crate) fn finish(&mut self, scan: &mut StreamScan) -> Result<(), SseError> {
        if !std::mem::take(&mut self.spilled) {
            self.ready = self.queue.len();
            return Ok(());
        }
        // The unfinished event lives only in the scanner's carry. Ending it
        // with a line ending the upstream never sent hands its bytes to the
        // callback; the ending is chosen so that it neither merges with nor
        // extends the event's last line, and it is not forwarded.
        let close: &[u8] = match self.last_byte {
            b'\n' => b"\n",
            b'\r' => b"\r",
            _ => b"\n\n",
        };
        let carried = scan.scanner.pending_bytes();
        let mut copied = None;
        scan.scanner.feed(close, |event| {
            copied = Some(Bytes::copy_from_slice(&event.raw[..carried]));
        })?;
        debug_assert!(copied.is_some(), "closing a spilled event reports it");
        if let Some(copied) = copied {
            self.push_ready(copied);
        }
        Ok(())
    }

    /// Queues `frame` behind everything staged so far, which is released
    /// with it: once a chunk forwards bytes, the staged bytes before them
    /// either completed an event or lie outside any event.
    fn push_ready(&mut self, frame: Bytes) {
        self.queue.push_back(frame);
        self.ready = self.queue.len();
    }

    /// Holds `tail`, the start of an unfinished event whose bytes so far
    /// number `pending`, until the event completes.
    fn stage(&mut self, tail: Bytes, pending: usize) {
        if self.spilled {
            return;
        }
        self.queue.push_back(tail);
        if self.queue.len() - self.ready <= STAGE_SLOTS {
            return;
        }
        // Too many pieces: the scanner holds the event contiguously, so the
        // pieces are dropped and the event is copied out of the scanner when
        // it completes.
        let staged = self.staged_bytes();
        debug_assert!(staged >= pending, "staged pieces hold the whole event");
        self.release_outside(staged.saturating_sub(pending));
        self.spilled = true;
    }

    /// Drops the staged pieces of a removed event whose bytes from earlier
    /// chunks number `carried`.
    fn drop_staged_event(&mut self, carried: usize) {
        let staged = self.staged_bytes();
        debug_assert!(staged >= carried, "staged pieces hold the whole event");
        self.release_outside(staged.saturating_sub(carried));
    }

    /// Sends the first `outside` staged bytes, which precede the unfinished
    /// event (a byte order mark), and drops the other staged pieces.
    fn release_outside(&mut self, mut outside: usize) {
        let mut kept = self.ready;
        while outside > 0
            && let Some(piece) = self.queue.get_mut(kept)
        {
            piece.truncate(outside);
            outside -= piece.len();
            kept += 1;
        }
        self.queue.truncate(kept);
        self.ready = kept;
    }

    fn staged_bytes(&self) -> usize {
        self.queue.iter().skip(self.ready).map(Bytes::len).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scan(events: &[&[u8]]) -> Facts {
        let mut scan = StreamScan::default();
        for event in events {
            scan.scan(event).expect("well-formed events");
        }
        scan.facts
    }

    const CONTENT: &[u8] =
        b"data: {\"choices\":[{\"delta\":{\"content\":\"x\"},\"finish_reason\":null}]}\n\n";
    const FINISH: &[u8] = b"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n";
    const USAGE_ONLY: &[u8] =
        b"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":7,\"completion_tokens\":3}}\n\n";
    const FINISH_WITH_USAGE: &[u8] = b"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":7,\"completion_tokens\":3}}\n\n";

    #[test]
    fn usage_in_the_finishing_event_is_final() {
        let facts = scan(&[CONTENT, FINISH_WITH_USAGE]);
        assert!(facts.finished);
        assert!(facts.usage_final);
        assert_eq!(facts.usage.get().map(|usage| usage.output), Some(3));
    }

    #[test]
    fn usage_after_the_finish_is_final() {
        let facts = scan(&[CONTENT, FINISH, USAGE_ONLY]);
        assert!(facts.finished);
        assert!(facts.usage_final);
    }

    #[test]
    fn usage_before_any_finish_is_provisional() {
        let facts = scan(&[CONTENT, USAGE_ONLY]);
        assert!(!facts.finished);
        assert!(!facts.usage_final);
        assert!(facts.usage.get().is_some());
    }

    #[test]
    fn done_and_first_error_are_recorded() {
        let broken: &[u8] = b"data: {\"choices\":[],\"usage\":{\"completion_tokens\":3}}\n\n";
        let facts = scan(&[broken, USAGE_ONLY, b"data: [DONE]\n\n"]);
        assert!(facts.done);
        assert_eq!(
            facts.usage_error,
            Some(UsageErrorKind::MissingField("prompt_tokens"))
        );
        assert!(facts.usage.get().is_some());
    }

    #[test]
    fn several_data_lines_are_joined_before_parsing() {
        let facts = scan(&[
            b"data: {\"choices\":[],\ndata: \"usage\":{\"prompt_tokens\":7,\"completion_tokens\":3}}\n\n",
        ]);
        assert_eq!(facts.usage.get().map(|usage| usage.input), Some(7));
    }
}
