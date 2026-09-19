//! Allocation gate for strip mode of `PassthroughBody` (sections 4.7 and
//! 7.4): no heap allocation in the steady state, at most one for the chunk
//! from which a usage-only event is cut, and at most one for a whole event
//! spread over 20 upstream chunks.

mod body_support;

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

use body_support::{
    CPA_CONTENT_EVENT, DONE_EVENT, MemBody, Settlement, Step, cpa_final_event, drive_counting,
    paused_runtime, split_at, stream_plan, timing_idle,
};
use brisk_gateway::body::{HeldFrames, PassthroughBody, StreamPlan};
use brisk_gateway::outcome::OutcomeStatus;
use brisk_proto::UsageTokens;
use bytes::Bytes;

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

const IDLE: Duration = Duration::from_secs(4);

/// The standalone usage event the gateway asked for; strip mode cuts it.
const USAGE_EVENT: &[u8] = b"data: {\"id\":\"00000000-0000-0000-0000-000000000001\",\"object\":\"chat.completion.chunk\",\"created\":0,\"model\":\"grok-4.6-build\",\"choices\":[],\"usage\":{\"prompt_tokens\":213,\"completion_tokens\":71,\"total_tokens\":284}}\n\n";

/// Pieces a long event is cut into.
const LONG_PIECES: usize = 20;

fn long_event() -> Vec<u8> {
    let content = "pong ".repeat(1_200);
    format!(
        "data: {{\"id\":\"00000000-0000-0000-0000-000000000001\",\"object\":\"chat.completion.chunk\",\"choices\":[{{\"index\":0,\"delta\":{{\"content\":\"{content}\"}},\"finish_reason\":null}}]}}\n\n"
    )
    .into_bytes()
}

/// Byte offsets of the special parts of the stream.
struct Layout {
    bytes: Vec<u8>,
    /// The long event early on, which grows the scanner's buffer before
    /// counting matters.
    warmup: (usize, usize),
    /// The long event that is measured.
    long: (usize, usize),
    /// The usage event.
    usage: (usize, usize),
}

fn layout() -> Layout {
    let long = long_event();
    let mut bytes = Vec::new();
    let mut push = |part: &[u8]| {
        let start = bytes.len();
        bytes.extend_from_slice(part);
        (start, bytes.len())
    };
    push(&CPA_CONTENT_EVENT.repeat(100));
    let warmup = push(&long);
    push(&CPA_CONTENT_EVENT.repeat(4_900));
    let measured = push(&long);
    push(&CPA_CONTENT_EVENT.repeat(5_000));
    push(cpa_final_event());
    let usage = push(USAGE_EVENT);
    push(DONE_EVENT);
    Layout {
        bytes,
        warmup,
        long: measured,
        usage,
    }
}

/// Cuts: random pieces of 64 to 512 bytes, except that each long event is
/// cut into 20 equal pieces and the usage event lies inside one chunk
/// together with the end of the previous event and the start of the next.
fn cuts(layout: &Layout) -> Vec<usize> {
    let mut rng = fastrand::Rng::with_seed(7);
    let mut cuts = Vec::new();
    let mut at = 0;
    loop {
        at += rng.usize(64..=512);
        if at >= layout.bytes.len() {
            break;
        }
        cuts.push(at);
    }
    let usage_chunk = (layout.usage.0 - 40, layout.usage.1 + 6);
    cuts.retain(|&cut| {
        [layout.warmup, layout.long, usage_chunk]
            .iter()
            .all(|&(start, end)| cut <= start || cut >= end)
    });
    for (start, end) in [layout.warmup, layout.long] {
        let piece = (end - start).div_ceil(LONG_PIECES);
        cuts.extend((1..LONG_PIECES).map(|k| start + k * piece));
    }
    cuts.extend([usage_chunk.0, usage_chunk.1]);
    cuts.sort_unstable();
    cuts.dedup();
    cuts
}

/// Index of the frame holding byte `offset`.
fn frame_of(cuts: &[usize], offset: usize) -> usize {
    cuts.partition_point(|&cut| cut <= offset)
}

#[test]
fn strip_steady_state_allocates_nothing() {
    let layout = layout();
    let cuts = cuts(&layout);
    let frames = split_at(&Bytes::from(layout.bytes.clone()), &cuts);
    let count = frames.len();
    assert!(count > 9_000, "{count} frames");
    let long = (
        frame_of(&cuts, layout.long.0),
        frame_of(&cuts, layout.long.1 - 1),
    );
    assert_eq!(
        long.1 - long.0 + 1,
        LONG_PIECES,
        "the event spans 20 chunks"
    );
    let usage = frame_of(&cuts, layout.usage.0);
    assert_eq!(
        usage,
        frame_of(&cuts, layout.usage.1 - 1),
        "one chunk holds the usage event"
    );
    assert!(frame_of(&cuts, layout.warmup.1) < 1_000);

    let runtime = paused_runtime();
    let progress = Arc::new(AtomicUsize::new(0));
    let mut settlement = Settlement::new();
    let mut body = {
        let _entered = runtime.enter();
        let upstream = MemBody::pending_before_each(frames)
            .then(Step::Hang)
            .with_progress(Arc::clone(&progress));
        let plan = StreamPlan {
            timing: timing_idle(IDLE),
            ..stream_plan(true)
        };
        PassthroughBody::new(upstream, HeldFrames::default(), plan, settlement.ctx())
    };

    let profiler = dhat::Profiler::builder().testing().build();
    let report = drive_counting(
        &runtime,
        &mut body,
        &progress,
        count,
        IDLE,
        &[1_500, 3_000, 4_500, 6_000, 7_500],
    );
    drop(profiler);

    // Counts of frames returned: frame `i` is processed while `i + 1` are.
    let long_blocks = report.sum(long.0 + 1..=long.1 + 1);
    let usage_blocks = report.sum(usage + 1..=usage + 1);
    let steady: u64 = (1_000..=9_000)
        .filter(|returned| !(long.0 + 1..=long.1 + 1).contains(returned))
        .map(|returned| report.per_frame[returned])
        .sum();
    println!(
        "dhat_strip: {count} frames, steady state: {steady} blocks, 20-chunk event: \
         {long_blocks} blocks, usage chunk: {usage_blocks} blocks, total: {} blocks",
        report.total()
    );
    assert!(report.advances_mid_stream >= 3);
    assert_eq!(
        steady,
        0,
        "per-frame allocations: {:?}",
        nonzero(&report.per_frame)
    );
    assert!(long_blocks <= 1, "20-chunk event: {long_blocks}");
    assert!(usage_blocks <= 1, "usage chunk: {usage_blocks}");
    assert!(report.error.contains("idle"), "{}", report.error);
    drop(body);

    let outcome = settlement.outcome();
    assert_eq!(outcome.status, OutcomeStatus::IdleTimeout);
    assert_eq!(
        outcome.usage,
        Some(UsageTokens {
            input: 213,
            output: 71,
            cached_input: None,
            reasoning_output: None,
        })
    );
    // Everything but the usage event went downstream.
    assert_eq!(
        outcome.response_bytes,
        (layout.bytes.len() - USAGE_EVENT.len()) as u64
    );
}

fn nonzero(per_frame: &[u64]) -> Vec<(usize, u64)> {
    per_frame
        .iter()
        .enumerate()
        .filter(|(_, blocks)| **blocks > 0)
        .map(|(frame, blocks)| (frame, *blocks))
        .collect()
}
