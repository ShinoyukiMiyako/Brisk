//! Allocation gate for the normal-mode per-chunk path of `PassthroughBody`
//! (sections 4.7 and 7.4): no heap allocation between the 1000th and the
//! 9000th upstream frame, with the `Pending` and idle-timer paths exercised
//! inside the counted polls, and at most 8 from construction to settlement.

mod body_support;

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

use body_support::{
    CPA_CONTENT_EVENT, DONE_EVENT, GROK46_XHIGH_USAGE, MemBody, Settlement, Step, cpa_final_event,
    drive_counting, paused_runtime, split_random, stream_plan, timing_idle,
};
use brisk_gateway::body::{HeldFrames, PassthroughBody, StreamPlan};
use brisk_gateway::outcome::OutcomeStatus;
use bytes::Bytes;

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

const IDLE: Duration = Duration::from_secs(4);

#[test]
fn passthrough_steady_state_allocates_nothing() {
    // Everything is built before the profiler starts: the frames (sliced
    // once, so their `Bytes` are already shared), the runtime and the body.
    let mut stream = CPA_CONTENT_EVENT.repeat(10_000);
    stream.extend_from_slice(cpa_final_event());
    stream.extend_from_slice(DONE_EVENT);
    let frames = split_random(&Bytes::from(stream), 42, 64, 512);
    let count = frames.len();
    assert!(count > 9_000, "{count} frames");

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
            ..stream_plan(false)
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

    let window = report.sum(1_000..=9_000);
    let total = report.total();
    println!(
        "dhat_passthrough: {count} frames, frames 1000..=9000: {window} blocks, \
         construction to settlement: {total} blocks"
    );
    assert!(report.advances_mid_stream >= 3);
    assert_eq!(
        window,
        0,
        "per-frame allocations: {:?}",
        nonzero(&report.per_frame)
    );
    assert!(
        total <= 8,
        "per-frame allocations: {:?}",
        nonzero(&report.per_frame)
    );
    // The silent periods after the last frame ended the stream, so the
    // period path did run.
    assert!(report.error.contains("idle"), "{}", report.error);
    drop(body);
    let outcome = settlement.outcome();
    assert_eq!(outcome.status, OutcomeStatus::IdleTimeout);
    // D21: the usage came with `finish_reason`, so it is billed as it is.
    assert_eq!(outcome.billed, GROK46_XHIGH_USAGE);
}

fn nonzero(per_frame: &[u64]) -> Vec<(usize, u64)> {
    per_frame
        .iter()
        .enumerate()
        .filter(|(_, blocks)| **blocks > 0)
        .map(|(frame, blocks)| (frame, *blocks))
        .collect()
}
