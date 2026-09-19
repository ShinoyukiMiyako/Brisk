//! Test support for the response bodies: [`MemBody`], an upstream body that
//! replays a scripted sequence of frames, `Pending` returns, delays, errors
//! and trailers, and helpers that cut a byte stream into frames.

#![allow(dead_code)]

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use brisk_gateway::BoxError;
use bytes::Bytes;
use http::HeaderMap;
use http_body::{Body, Frame, SizeHint};
use tokio::time::Sleep;

/// One scripted step of a [`MemBody`].
#[derive(Debug)]
pub(crate) enum Step {
    /// A data frame, returned as this very `Bytes` (pointer-equal).
    Data(Bytes),
    /// Return `Pending` once. The waker is woken first, so a task polling
    /// the body is polled again rather than stalling.
    Pending,
    /// Return `Pending` until this much time has passed (tokio time, so a
    /// paused clock controls it).
    Delay(Duration),
    /// Return an error with this message.
    Error(&'static str),
    /// A trailers frame.
    Trailers(HeaderMap),
}

/// Upstream body replaying pre-generated [`Step`]s, then ending.
///
/// Nothing allocates while polling except the timer of a [`Step::Delay`],
/// so allocation gates can drive it with `Pending` steps only.
#[derive(Debug)]
pub(crate) struct MemBody {
    steps: VecDeque<Step>,
    delay: Option<Pin<Box<Sleep>>>,
    /// Data bytes not yet returned, when the body reports an exact size.
    exact_remaining: Option<u64>,
}

impl MemBody {
    /// A body replaying `steps` in order.
    pub(crate) fn new(steps: impl IntoIterator<Item = Step>) -> Self {
        Self {
            steps: steps.into_iter().collect(),
            delay: None,
            exact_remaining: None,
        }
    }

    /// A body returning `frames` back to back.
    pub(crate) fn from_frames(frames: impl IntoIterator<Item = Bytes>) -> Self {
        Self::new(frames.into_iter().map(Step::Data))
    }

    /// A body returning `Pending` once before each of `frames`, the driving
    /// pattern of the allocation gates (section 4.7).
    pub(crate) fn pending_before_each(frames: impl IntoIterator<Item = Bytes>) -> Self {
        Self::new(
            frames
                .into_iter()
                .flat_map(|frame| [Step::Pending, Step::Data(frame)]),
        )
    }

    /// Reports the exact number of data bytes left in `size_hint`, as an
    /// upstream with `Content-Length` does.
    pub(crate) fn with_exact_size(mut self) -> Self {
        let total = self
            .steps
            .iter()
            .map(|step| match step {
                Step::Data(data) => data.len() as u64,
                _ => 0,
            })
            .sum();
        self.exact_remaining = Some(total);
        self
    }

    /// Steps not yet replayed.
    pub(crate) fn remaining_steps(&self) -> usize {
        self.steps.len()
    }
}

impl Body for MemBody {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, BoxError>>> {
        let this = &mut *self;
        loop {
            let Some(step) = this.steps.pop_front() else {
                return Poll::Ready(None);
            };
            match step {
                Step::Data(data) => {
                    if let Some(remaining) = &mut this.exact_remaining {
                        *remaining -= data.len() as u64;
                    }
                    return Poll::Ready(Some(Ok(Frame::data(data))));
                }
                Step::Pending => {
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                Step::Delay(duration) => {
                    let delay = this
                        .delay
                        .get_or_insert_with(|| Box::pin(tokio::time::sleep(duration)));
                    if delay.as_mut().poll(cx).is_pending() {
                        this.steps.push_front(Step::Delay(duration));
                        return Poll::Pending;
                    }
                    this.delay = None;
                }
                Step::Error(message) => return Poll::Ready(Some(Err(message.into()))),
                Step::Trailers(trailers) => {
                    return Poll::Ready(Some(Ok(Frame::trailers(trailers))));
                }
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.steps.is_empty()
    }

    fn size_hint(&self) -> SizeHint {
        match self.exact_remaining {
            Some(remaining) => SizeHint::with_exact(remaining),
            None => SizeHint::default(),
        }
    }
}

/// Cuts `bytes` into zero-copy pieces with lengths drawn uniformly from
/// `min_piece..=max_piece` (the last piece may be shorter), reproducibly
/// for a given `seed`.
///
/// # Panics
///
/// When `min_piece` is zero or larger than `max_piece`.
pub(crate) fn split_random(
    bytes: &Bytes,
    seed: u64,
    min_piece: usize,
    max_piece: usize,
) -> Vec<Bytes> {
    assert!(
        min_piece > 0 && min_piece <= max_piece,
        "piece lengths {min_piece}..={max_piece} are empty or include zero"
    );
    let mut rng = fastrand::Rng::with_seed(seed);
    let mut pieces = Vec::new();
    let mut start = 0;
    while start < bytes.len() {
        let end = (start + rng.usize(min_piece..=max_piece)).min(bytes.len());
        pieces.push(bytes.slice(start..end));
        start = end;
    }
    pieces
}

/// Cuts `bytes` at the given increasing offsets; empty pieces are skipped.
///
/// # Panics
///
/// When the offsets decrease or lie past the end of `bytes`.
pub(crate) fn split_at(bytes: &Bytes, cuts: &[usize]) -> Vec<Bytes> {
    let mut pieces = Vec::with_capacity(cuts.len() + 1);
    let mut start = 0;
    for &cut in cuts.iter().chain(std::iter::once(&bytes.len())) {
        assert!(
            start <= cut && cut <= bytes.len(),
            "cut {cut} is out of order or past {} bytes",
            bytes.len()
        );
        if cut > start {
            pieces.push(bytes.slice(start..cut));
        }
        start = cut;
    }
    pieces
}
