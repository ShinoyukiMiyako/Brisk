//! Response bodies of the data plane: the streamed pass-through that frames
//! SSE events and extracts usage without copying chunks, the tap that parses
//! usage from non-streaming responses, and the settlement that runs when a
//! committed response ends, fails or is dropped (R6, R7, R9, R14, R15). Also
//! the rewritten upstream request body.
//!
//! Every committed body emits exactly one `Outcome`, at EOF, on an error or
//! on drop, whichever comes first.
//!
//! A body settles as soon as its upstream reports `is_end_stream()` after a
//! frame, before returning that frame: hyper stops polling a body whose
//! `Content-Length` has been written and drops it without ever seeing
//! `None`, which would otherwise read as a client that left.

mod chunk;
mod idle;
mod passthrough;
mod settle;
mod splice;

use std::pin::Pin;
use std::time::Duration;

use brisk_proto::sse::SseError;
use bytes::Bytes;

use crate::BoxError;

pub use passthrough::{PassthroughBody, StreamPlan};
pub use settle::{DrainLimits, SettleCtx, SettleShared};
pub use splice::SpliceBody;

/// Why a committed response body ended early. The response has already
/// been committed, so the error ends the downstream stream: h1 closes the
/// connection without the terminating chunk, h2 resets the stream (D5).
#[derive(Debug, thiserror::Error)]
pub enum BodyError {
    /// The upstream body failed; the error has passed through
    /// [`scrub_error`](crate::secret::scrub_error).
    #[error("upstream body failed")]
    Upstream(#[source] BoxError),
    /// No upstream byte for the idle timeout.
    #[error("upstream idle for longer than {0:?}")]
    Idle(Duration),
    /// Committed before the first body byte, which then missed its deadline.
    #[error("no upstream byte before the first-byte deadline")]
    FirstByte,
    /// An SSE event exceeded [`MAX_EVENT_BYTES`](brisk_proto::sse::MAX_EVENT_BYTES).
    #[error("upstream SSE framing")]
    Protocol(#[source] SseError),
}

/// Timing handed from `forward` to the committed body.
#[derive(Debug)]
pub struct BodyTiming {
    /// The request's only timer, created by `forward` at send time (D31).
    /// The body resets it and never creates another.
    pub timer: Pin<Box<tokio::time::Sleep>>,
    /// Set when the response was committed before any body byte arrived.
    pub first_byte_deadline: Option<tokio::time::Instant>,
    /// Longest silence allowed between body bytes after the first one.
    pub idle: Duration,
}

/// Frames received before commit, in order. One frame, the common case,
/// is stored inline without allocating.
#[derive(Debug, Default)]
pub struct HeldFrames {
    first: Option<Bytes>,
    rest: Vec<Bytes>,
    /// Index into `rest` of the next frame [`pop_front`](Self::pop_front)
    /// returns; frames are handed out in place instead of shifting the `Vec`.
    next: usize,
    bytes: usize,
}

impl HeldFrames {
    /// Appends a frame after the ones already held.
    pub fn push(&mut self, frame: Bytes) {
        self.bytes += frame.len();
        if self.first.is_none() && self.next == self.rest.len() {
            // Nothing is queued, so the inline slot keeps the order intact.
            self.rest.clear();
            self.next = 0;
            self.first = Some(frame);
        } else {
            self.rest.push(frame);
        }
    }

    /// No frame is held.
    pub fn is_empty(&self) -> bool {
        self.first.is_none() && self.next == self.rest.len()
    }

    /// Total bytes held.
    pub fn len_bytes(&self) -> usize {
        self.bytes
    }

    /// Removes and returns the oldest held frame.
    pub(crate) fn pop_front(&mut self) -> Option<Bytes> {
        let frame = if let Some(frame) = self.first.take() {
            frame
        } else {
            let slot = self.rest.get_mut(self.next)?;
            self.next += 1;
            std::mem::take(slot)
        };
        self.bytes -= frame.len();
        Some(frame)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn drain(held: &mut HeldFrames) -> Vec<Bytes> {
        std::iter::from_fn(|| held.pop_front()).collect()
    }

    #[test]
    fn empty_by_default() {
        let mut held = HeldFrames::default();
        assert!(held.is_empty());
        assert_eq!(held.len_bytes(), 0);
        assert_eq!(held.pop_front(), None);
    }

    #[test]
    fn single_frame_is_stored_inline() {
        let mut held = HeldFrames::default();
        let frame = Bytes::from_static(b"data: {}\n\n");
        held.push(frame.clone());
        assert!(!held.is_empty());
        assert_eq!(held.len_bytes(), frame.len());
        // The common case must not allocate: the overflow `Vec` stays unused.
        assert_eq!(held.rest.capacity(), 0);

        let popped = held.pop_front().expect("one frame is held");
        assert_eq!(popped.as_ptr(), frame.as_ptr());
        assert!(held.is_empty());
        assert_eq!(held.len_bytes(), 0);
    }

    #[test]
    fn frames_come_back_in_push_order() {
        let mut held = HeldFrames::default();
        let frames = [
            Bytes::from_static(b"a"),
            Bytes::from_static(b"bb"),
            Bytes::from_static(b"ccc"),
        ];
        for frame in &frames {
            held.push(frame.clone());
        }
        assert_eq!(held.len_bytes(), 6);
        assert_eq!(drain(&mut held), frames);
        assert!(held.is_empty());
        assert_eq!(held.len_bytes(), 0);
    }

    #[test]
    fn push_after_partial_pop_keeps_order() {
        let mut held = HeldFrames::default();
        held.push(Bytes::from_static(b"1"));
        held.push(Bytes::from_static(b"2"));
        held.push(Bytes::from_static(b"3"));
        assert_eq!(held.pop_front(), Some(Bytes::from_static(b"1")));
        held.push(Bytes::from_static(b"4"));
        assert_eq!(held.pop_front(), Some(Bytes::from_static(b"2")));
        held.push(Bytes::from_static(b"5"));
        assert_eq!(
            drain(&mut held),
            [
                Bytes::from_static(b"3"),
                Bytes::from_static(b"4"),
                Bytes::from_static(b"5"),
            ]
        );
    }

    #[test]
    fn reuses_inline_slot_once_drained() {
        let mut held = HeldFrames::default();
        held.push(Bytes::from_static(b"1"));
        held.push(Bytes::from_static(b"2"));
        assert_eq!(drain(&mut held).len(), 2);
        held.push(Bytes::from_static(b"3"));
        assert!(held.first.is_some());
        assert_eq!(held.len_bytes(), 1);
        assert_eq!(drain(&mut held), [Bytes::from_static(b"3")]);
    }

    #[test]
    fn counts_empty_frames_as_held() {
        let mut held = HeldFrames::default();
        held.push(Bytes::new());
        assert!(!held.is_empty());
        assert_eq!(held.len_bytes(), 0);
        assert_eq!(held.pop_front(), Some(Bytes::new()));
        assert!(held.is_empty());
    }
}
