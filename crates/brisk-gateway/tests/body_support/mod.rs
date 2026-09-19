//! Test support for the response bodies: [`MemBody`], an upstream body that
//! replays a scripted sequence of frames, `Pending` returns, delays, errors
//! and trailers; helpers that cut a byte stream into frames; the CPA
//! fixtures; and [`Settlement`], which builds settlement contexts and reads
//! the Outcomes they emit.

#![allow(dead_code)]

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use brisk_gateway::BoxError;
use brisk_gateway::body::{
    BodyTiming, DrainLimits, HeldFrames, PassthroughBody, SettleCtx, SettleShared, StreamPlan,
};
use brisk_gateway::budget::ByteBudget;
use brisk_gateway::outcome::{Outcome, OutcomeReceiver, outcome_channel};
use brisk_gateway::spec::{ChannelId, KeyId};
use brisk_proto::UsageTokens;
use bytes::Bytes;
use http::HeaderMap;
use http_body::{Body, Frame, SizeHint};
use http_body_util::BodyExt;
use tokio::time::{Instant, Sleep};

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
    /// Return `Pending` from now on without waking anyone: an upstream that
    /// went silent without closing.
    Hang,
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
    /// Counts the data frames returned, for tests that attribute work to
    /// the frame being processed.
    progress: Option<Arc<AtomicUsize>>,
}

impl MemBody {
    /// A body replaying `steps` in order.
    pub(crate) fn new(steps: impl IntoIterator<Item = Step>) -> Self {
        Self {
            steps: steps.into_iter().collect(),
            delay: None,
            exact_remaining: None,
            progress: None,
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

    /// Counts every data frame returned in `counter`.
    pub(crate) fn with_progress(mut self, counter: Arc<AtomicUsize>) -> Self {
        self.progress = Some(counter);
        self
    }

    /// Appends `step` after the scripted ones.
    pub(crate) fn then(mut self, step: Step) -> Self {
        self.steps.push_back(step);
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
                    if let Some(progress) = &this.progress {
                        progress.fetch_add(1, Ordering::Relaxed);
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
                Step::Hang => {
                    this.steps.push_front(Step::Hang);
                    return Poll::Pending;
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

/// Every way the body tests cut a stream: whole, one byte per frame, and
/// seeded random pieces of a few size ranges.
pub(crate) fn splits(bytes: &Bytes) -> Vec<Vec<Bytes>> {
    let mut all = vec![vec![bytes.clone()], split_random(bytes, 0, 1, 1)];
    for (seed, min, max) in [(1, 1, 7), (2, 2, 40), (3, 64, 512), (4, 300, 900)] {
        all.push(split_random(bytes, seed, min, max));
    }
    all
}

/// Concatenates frames.
pub(crate) fn concat(frames: &[Bytes]) -> Vec<u8> {
    frames
        .iter()
        .flat_map(|frame| frame.iter().copied())
        .collect()
}

/// Fixture bytes (section 4.4, rule 7).
macro_rules! fixture {
    ($name:literal) => {
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../fixtures/cpa/",
            $name
        ))
    };
}

/// The default test model's stream: usage together with `finish_reason`.
pub(crate) const GROK46_XHIGH: &[u8] = fixture!("chat-stream-grok46-xhigh.sse");
pub(crate) const GROK46_SUFFIX: &[u8] = fixture!("chat-stream-grok46-suffix.sse");
pub(crate) const GROK43: &[u8] = fixture!("chat-stream-grok43.sse");
pub(crate) const GPT55: &[u8] = fixture!("chat-stream-gpt55.sse");
pub(crate) const NONSTREAM_GROK46_XHIGH: &[u8] = fixture!("chat-nonstream-grok46-xhigh.json");
pub(crate) const NONSTREAM_GPT55: &[u8] = fixture!("chat-nonstream-gpt55.json");

/// The last event of every chat stream fixture.
pub(crate) const DONE_EVENT: &[u8] = b"data: [DONE]\n\n";

/// `bytes` without its trailing `data: [DONE]` event.
pub(crate) fn without_done(bytes: &[u8]) -> &[u8] {
    bytes
        .strip_suffix(DONE_EVENT)
        .expect("the fixture ends with [DONE]")
}

const fn usage(input: u64, output: u64, cached: u64, reasoning: u64) -> UsageTokens {
    UsageTokens {
        input,
        output,
        cached_input: Some(cached),
        reasoning_output: Some(reasoning),
    }
}

/// Usage of `chat-stream-grok46-xhigh` (section 4.2).
pub(crate) const GROK46_XHIGH_USAGE: UsageTokens = usage(213, 71, 0, 70);

/// The chat stream fixtures with the usage section 4.2 lists for each.
pub(crate) const STREAM_FIXTURES: [(&str, &[u8], UsageTokens); 4] = [
    ("chat-stream-grok46-xhigh", GROK46_XHIGH, GROK46_XHIGH_USAGE),
    (
        "chat-stream-grok46-suffix",
        GROK46_SUFFIX,
        usage(213, 105, 128, 104),
    ),
    ("chat-stream-grok43", GROK43, usage(197, 216, 0, 215)),
    ("chat-stream-gpt55", GPT55, usage(307, 5, 0, 0)),
];

/// Request body size in every settlement context; its estimate is 213
/// input tokens, which is also the prompt of the grok46 fixtures.
pub(crate) const REQUEST_BYTES: u64 = 852;

/// Idle timeout of [`timing`].
pub(crate) const IDLE: Duration = Duration::from_secs(120);

/// Timing of a body committed after bytes arrived, with idle timeout `idle`.
/// Needs a Tokio runtime with time enabled.
pub(crate) fn timing_idle(idle: Duration) -> BodyTiming {
    BodyTiming {
        // Whatever deadline `forward` left behind; the body must replace it.
        timer: Box::pin(tokio::time::sleep(Duration::from_secs(3600))),
        first_byte_deadline: None,
        idle,
    }
}

/// [`timing_idle`] with the default idle timeout.
pub(crate) fn timing() -> BodyTiming {
    timing_idle(IDLE)
}

/// Timing of a body committed before its first byte, which is due within
/// `first_byte` from now.
pub(crate) fn timing_first_byte(first_byte: Duration, idle: Duration) -> BodyTiming {
    BodyTiming {
        first_byte_deadline: Some(Instant::now() + first_byte),
        ..timing_idle(idle)
    }
}

/// A plan for a stream committed after bytes arrived.
pub(crate) fn stream_plan(strip_usage_only: bool) -> StreamPlan {
    StreamPlan {
        strip_usage_only,
        first_event_error: false,
        timing: timing(),
    }
}

/// The settlement side of a gateway: shared state and the Outcome queue.
pub(crate) struct Settlement {
    pub(crate) shared: Arc<SettleShared>,
    rx: OutcomeReceiver,
}

impl Settlement {
    /// Defaults of section 3: 256 MiB tap budget, drain of 64 KiB or 2 s.
    pub(crate) fn new() -> Self {
        Self::with(
            256 << 20,
            DrainLimits {
                max_bytes: 64 << 10,
                timeout: Duration::from_secs(2),
            },
        )
    }

    pub(crate) fn with(tap_budget: usize, drain: DrainLimits) -> Self {
        let (sink, rx) = outcome_channel(64);
        let shared = Arc::new(SettleShared {
            sink,
            drain,
            tap_budget: ByteBudget::new(tap_budget),
        });
        Self { shared, rx }
    }

    /// Context of a streamed request answered with 200.
    pub(crate) fn ctx(&self) -> SettleCtx {
        self.ctx_with(200, true)
    }

    pub(crate) fn ctx_with(&self, http_status: u16, stream: bool) -> SettleCtx {
        let now = Instant::now();
        SettleCtx {
            shared: Arc::clone(&self.shared),
            request_id: 7,
            key: KeyId(0),
            channel: ChannelId(1),
            attempts: 1,
            stream,
            http_status,
            request_bytes: REQUEST_BYTES,
            started: now,
            committed: now,
        }
    }

    /// A streamed body over `upstream` with nothing held from before commit.
    pub(crate) fn passthrough(&self, upstream: MemBody, strip: bool) -> PassthroughBody<MemBody> {
        PassthroughBody::new(
            upstream,
            HeldFrames::default(),
            stream_plan(strip),
            self.ctx(),
        )
    }

    /// The one Outcome emitted so far; fails if there is none or more.
    pub(crate) fn outcome(&mut self) -> Outcome {
        let outcome = self.rx.try_recv().expect("an Outcome was emitted");
        self.assert_no_outcome();
        outcome
    }

    /// Waits for the next Outcome, e.g. from a drain task.
    pub(crate) async fn next_outcome(&mut self) -> Outcome {
        self.rx.recv().await.expect("the queue is open")
    }

    pub(crate) fn assert_no_outcome(&mut self) {
        assert!(self.rx.try_recv().is_err(), "no further Outcome");
    }

    /// Tap budget currently not reserved.
    pub(crate) fn tap_available(&self) -> usize {
        self.shared.tap_budget.available()
    }
}

/// Polls `body` to its end: the data frames it returned and the error that
/// ended it, if any.
pub(crate) async fn collect<B>(body: &mut B) -> (Vec<Bytes>, Option<B::Error>)
where
    B: Body<Data = Bytes> + Unpin,
{
    let mut frames = Vec::new();
    while let Some(frame) = body.frame().await {
        match frame {
            Ok(frame) => {
                if let Ok(data) = frame.into_data() {
                    frames.push(data);
                }
            }
            Err(error) => return (frames, Some(error)),
        }
    }
    (frames, None)
}

/// Polls `body` once with a no-op waker.
pub(crate) fn poll_once<B>(body: &mut B) -> Poll<Option<Result<Frame<Bytes>, B::Error>>>
where
    B: Body<Data = Bytes> + Unpin,
{
    Pin::new(body).poll_frame(&mut Context::from_waker(std::task::Waker::noop()))
}

/// Byte estimate of the output side for `response_bytes` (section 2.6).
pub(crate) fn estimate_output(response_bytes: usize) -> u64 {
    (response_bytes as u64).div_ceil(4)
}

/// [`UsageTokens`] with only the two billed counts.
pub(crate) const fn tokens(input: u64, output: u64) -> UsageTokens {
    UsageTokens {
        input,
        output,
        cached_input: None,
        reasoning_output: None,
    }
}
