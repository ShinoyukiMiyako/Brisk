//! Tests of `ResponseTap`, the body of committed responses that are not SSE.

mod body_support;

use std::time::Duration;

use body_support::{
    IDLE, MemBody, NONSTREAM_GPT55, NONSTREAM_GROK46_XHIGH, REQUEST_BYTES, Settlement, Step,
    concat, estimate_output, split_random, splits, timing, timing_first_byte, timing_idle, tokens,
};
use brisk_gateway::body::{BodyError, DrainLimits, MAX_TAP_BYTES, ResponseTap, TapPlan};
use brisk_gateway::outcome::{Outcome, OutcomeStatus, OutcomeTally};
use brisk_proto::{UsageErrorKind, UsageTokens};
use bytes::Bytes;
use http_body::Body;
use tokio::time::Instant;

const GROK46_NONSTREAM_USAGE: UsageTokens = UsageTokens {
    input: 213,
    output: 128,
    cached_input: Some(128),
    reasoning_output: Some(127),
};

const GPT55_NONSTREAM_USAGE: UsageTokens = UsageTokens {
    input: 307,
    output: 5,
    cached_input: Some(0),
    reasoning_output: Some(0),
};

fn plan(usage_expected: bool) -> TapPlan {
    TapPlan {
        usage_expected,
        timing: timing(),
    }
}

fn tap(settlement: &Settlement, upstream: MemBody, usage_expected: bool) -> ResponseTap<MemBody> {
    ResponseTap::new(
        upstream,
        plan(usage_expected),
        settlement.ctx_with(if usage_expected { 200 } else { 429 }, false),
    )
}

/// Runs `upstream` through a tap to its end.
async fn run(
    settlement: &mut Settlement,
    upstream: MemBody,
    usage_expected: bool,
) -> (Vec<Bytes>, Option<BodyError>, Outcome) {
    let mut body = tap(settlement, upstream, usage_expected);
    let (out, error) = body_support::collect(&mut body).await;
    drop(body);
    (out, error, settlement.outcome())
}

fn usage_missing(outcome: &Outcome) -> u64 {
    let mut tally = OutcomeTally::default();
    tally.record(outcome);
    tally.usage_missing
}

#[tokio::test]
async fn parses_the_usage_at_eof_under_any_split() {
    for (fixture, usage) in [
        (NONSTREAM_GROK46_XHIGH, GROK46_NONSTREAM_USAGE),
        (NONSTREAM_GPT55, GPT55_NONSTREAM_USAGE),
    ] {
        let body = Bytes::from_static(fixture);
        for frames in splits(&body) {
            for exact in [false, true] {
                let mut upstream = MemBody::from_frames(frames.clone());
                if exact {
                    upstream = upstream.with_exact_size();
                }
                let mut settlement = Settlement::new();
                let (out, error, outcome) = run(&mut settlement, upstream, true).await;
                assert!(error.is_none());
                assert_eq!(out.len(), frames.len());
                for (sent, received) in out.iter().zip(&frames) {
                    assert_eq!(
                        sent.as_ptr(),
                        received.as_ptr(),
                        "frames go out as they are"
                    );
                }
                assert_eq!(outcome.status, OutcomeStatus::Completed);
                assert!(!outcome.stream);
                assert_eq!(outcome.usage, Some(usage));
                assert_eq!(outcome.usage_error, None);
                assert_eq!(outcome.billed, usage);
                assert_eq!(outcome.response_bytes, fixture.len() as u64);
                assert_eq!(usage_missing(&outcome), 0);
                assert_eq!(settlement.tap_available(), 256 << 20, "budget returned");
            }
        }
    }
}

#[tokio::test]
async fn passes_the_exact_size_hint_through() {
    let body = Bytes::from_static(NONSTREAM_GROK46_XHIGH);
    let settlement = Settlement::new();
    let mut tap = tap(
        &settlement,
        MemBody::from_frames(split_random(&body, 1, 100, 200)).with_exact_size(),
        true,
    );
    assert_eq!(tap.size_hint().exact(), Some(body.len() as u64));
    // One reservation of the exact length, not one per frame.
    assert_eq!(settlement.tap_available(), (256 << 20) - body.len());
    let first = tap.frame_ready().expect("a frame");
    assert_eq!(
        tap.size_hint().exact(),
        Some((body.len() - first.len()) as u64)
    );
    assert_eq!(settlement.tap_available(), (256 << 20) - body.len());
}

#[tokio::test]
async fn settles_before_the_last_frame_when_the_upstream_reports_its_end() {
    let body = Bytes::from_static(NONSTREAM_GROK46_XHIGH);
    let mut settlement = Settlement::new();
    let mut tap = tap(
        &settlement,
        MemBody::from_frames(split_random(&body, 2, 100, 200)).with_exact_size(),
        true,
    );
    let mut out = Vec::new();
    while !tap.is_end_stream() {
        let frame = body_support::poll_once(&mut tap);
        match frame {
            std::task::Poll::Ready(Some(Ok(frame))) => out.push(frame.into_data().unwrap()),
            other => panic!("expected a data frame, got {other:?}"),
        }
    }
    assert_eq!(concat(&out), NONSTREAM_GROK46_XHIGH);
    assert_eq!(settlement.outcome().usage, Some(GROK46_NONSTREAM_USAGE));
    drop(tap);
    settlement.assert_no_outcome();
}

#[tokio::test]
async fn an_empty_body_settles_at_construction() {
    let mut settlement = Settlement::new();
    let tap = tap(
        &settlement,
        MemBody::from_frames([]).with_exact_size(),
        true,
    );
    assert!(tap.is_end_stream());
    let outcome = settlement.outcome();
    assert_eq!(outcome.status, OutcomeStatus::Completed);
    assert_eq!(outcome.usage, None);
    assert!(
        matches!(outcome.usage_error, Some(UsageErrorKind::Json { .. })),
        "an empty 2xx body is not a completion: {:?}",
        outcome.usage_error
    );
    drop(tap);
    settlement.assert_no_outcome();
}

#[tokio::test]
async fn a_body_over_the_cap_is_forwarded_without_usage() {
    // A valid completion padded past MAX_TAP_BYTES with whitespace.
    let mut json = b"{\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1},\"pad\":\"".to_vec();
    json.resize(MAX_TAP_BYTES + 10, b'a');
    json.extend_from_slice(b"\"}");
    let body = Bytes::from(json);
    for exact in [false, true] {
        let frames = split_random(&body, 3, 1 << 20, 1 << 20);
        let mut upstream = MemBody::from_frames(frames);
        if exact {
            upstream = upstream.with_exact_size();
        }
        let mut settlement = Settlement::new();
        let (out, error, outcome) = run(&mut settlement, upstream, true).await;
        assert!(error.is_none());
        assert_eq!(concat(&out).len(), body.len());
        assert_eq!(outcome.status, OutcomeStatus::Completed);
        assert_eq!(outcome.usage, None);
        assert_eq!(outcome.usage_error, None);
        assert_eq!(outcome.billed.output, estimate_output(body.len()));
        assert_eq!(usage_missing(&outcome), 1);
        assert_eq!(settlement.tap_available(), 256 << 20);
    }
}

#[tokio::test]
async fn an_exhausted_budget_stops_retention_and_is_returned() {
    let body = Bytes::from_static(NONSTREAM_GROK46_XHIGH);
    for exact in [false, true] {
        // Too small for the body, and far too small for a 1 MiB step.
        let mut settlement = Settlement::with(
            100,
            DrainLimits {
                max_bytes: 0,
                timeout: Duration::ZERO,
            },
        );
        let mut upstream = MemBody::from_frames(split_random(&body, 4, 50, 150));
        if exact {
            upstream = upstream.with_exact_size();
        }
        let (out, error, outcome) = run(&mut settlement, upstream, true).await;
        assert!(error.is_none());
        assert_eq!(concat(&out), NONSTREAM_GROK46_XHIGH);
        assert_eq!(outcome.status, OutcomeStatus::Completed);
        assert_eq!(outcome.usage, None);
        assert_eq!(outcome.billed.output, estimate_output(body.len()));
        assert_eq!(usage_missing(&outcome), 1);
        assert_eq!(settlement.tap_available(), 100);
    }

    // With the budget mostly taken, retention stops at the first step
    // that does not fit and returns what it held.
    let mut settlement = Settlement::with(
        (1 << 20) + 10,
        DrainLimits {
            max_bytes: 0,
            timeout: Duration::ZERO,
        },
    );
    let mut json = b"{\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1},\"pad\":\"".to_vec();
    json.resize(3 << 20, b'a');
    json.extend_from_slice(b"\"}");
    let big = Bytes::from(json);
    let mut tap = tap(
        &settlement,
        MemBody::from_frames(split_random(&big, 5, 1 << 19, 1 << 19)),
        true,
    );
    let mut sent = 0;
    while let Some(frame) = tap.frame_ready() {
        sent += frame.len();
        if sent <= 1 << 20 {
            assert_eq!(settlement.tap_available(), 10, "one 1 MiB step is held");
        }
    }
    assert_eq!(sent, big.len());
    drop(tap);
    let outcome = settlement.outcome();
    assert_eq!(outcome.usage, None);
    assert_eq!(settlement.tap_available(), (1 << 20) + 10);
}

#[tokio::test]
async fn forwarded_errors_are_neither_retained_nor_billed() {
    let body =
        Bytes::from_static(b"{\"error\":{\"message\":\"rate limited\",\"type\":\"rate_limit\"}}");
    for exact in [false, true] {
        let mut upstream = MemBody::from_frames(split_random(&body, 6, 5, 20));
        if exact {
            upstream = upstream.with_exact_size();
        }
        let mut settlement = Settlement::with(
            1 << 20,
            DrainLimits {
                max_bytes: 0,
                timeout: Duration::ZERO,
            },
        );
        let mut tap = tap(&settlement, upstream, false);
        while let Some(frame) = tap.frame_ready() {
            assert!(!frame.is_empty());
            assert_eq!(settlement.tap_available(), 1 << 20, "nothing is reserved");
        }
        drop(tap);
        let outcome = settlement.outcome();
        assert_eq!(
            outcome.status,
            OutcomeStatus::ForwardedError { status: 429 }
        );
        assert_eq!(outcome.http_status, Some(429));
        assert_eq!(outcome.usage, None);
        assert_eq!(outcome.billed, UsageTokens::default());
        assert_eq!(usage_missing(&outcome), 0);
    }
}

#[tokio::test]
async fn a_2xx_error_body_is_a_forwarded_error_billed_nothing() {
    for body in [
        &b"{\"error\":{\"message\":\"x\",\"type\":\"server_error\"}}"[..],
        b"{\"error\":\"Invalid API key\"}",
    ] {
        let body = Bytes::from_static(body);
        for frames in splits(&body) {
            let mut settlement = Settlement::new();
            let (_, error, outcome) =
                run(&mut settlement, MemBody::from_frames(frames), true).await;
            assert!(error.is_none());
            assert_eq!(
                outcome.status,
                OutcomeStatus::ForwardedError { status: 200 }
            );
            assert_eq!(outcome.billed, UsageTokens::default());
            assert_eq!(usage_missing(&outcome), 0);
        }
    }
}

#[tokio::test]
async fn a_body_without_usage_bills_the_estimate() {
    let body =
        Bytes::from_static(b"{\"id\":\"x\",\"choices\":[{\"message\":{\"content\":\"pong\"}}]}");
    let mut settlement = Settlement::new();
    let (_, _, outcome) = run(&mut settlement, MemBody::from_frames([body.clone()]), true).await;
    assert_eq!(outcome.status, OutcomeStatus::Completed);
    assert_eq!(outcome.usage, None);
    assert_eq!(outcome.usage_error, None);
    assert_eq!(
        outcome.billed,
        tokens(REQUEST_BYTES.div_ceil(4), estimate_output(body.len()))
    );
    assert_eq!(usage_missing(&outcome), 1);
}

#[tokio::test]
async fn a_malformed_usage_is_reported_and_billed_conservatively() {
    let body = Bytes::from_static(b"{\"usage\":{\"completion_tokens\":128}}");
    for frames in splits(&body) {
        let mut settlement = Settlement::new();
        let (_, _, outcome) = run(&mut settlement, MemBody::from_frames(frames), true).await;
        assert_eq!(outcome.status, OutcomeStatus::Completed);
        assert_eq!(
            outcome.usage_error,
            Some(UsageErrorKind::MissingField("prompt_tokens"))
        );
        assert_eq!(outcome.billed, outcome.estimate);
        let mut tally = OutcomeTally::default();
        tally.record(&outcome);
        assert_eq!((tally.usage_missing, tally.usage_parse_errors), (1, 1));
    }
}

#[tokio::test]
async fn a_dropped_tap_is_cancelled_and_returns_its_budget() {
    let body = Bytes::from_static(NONSTREAM_GROK46_XHIGH);
    for shutting_down in [false, true] {
        let mut settlement = Settlement::new();
        let frames = split_random(&body, 7, 100, 200);
        let upstream = MemBody::from_frames(frames[..2].to_vec()).then(Step::Hang);
        let mut tap = tap(&settlement, upstream, true);
        let mut sent = 0;
        while let Some(frame) = tap.frame_ready() {
            sent += frame.len();
        }
        assert!(
            settlement.tap_available() < 256 << 20,
            "retention holds budget"
        );
        if shutting_down {
            settlement.shared.sink.begin_shutdown();
        }
        drop(tap);
        let outcome = settlement.outcome();
        let expected = if shutting_down {
            OutcomeStatus::ShutdownAborted
        } else {
            OutcomeStatus::ClientCancelled
        };
        assert_eq!(outcome.status, expected);
        assert_eq!(outcome.response_bytes, sent as u64);
        assert_eq!(outcome.billed, tokens(213, estimate_output(sent)));
        assert_eq!(settlement.tap_available(), 256 << 20);
    }
}

#[tokio::test]
async fn an_upstream_error_ends_the_body_as_truncated() {
    let body = Bytes::from_static(NONSTREAM_GROK46_XHIGH);
    let mut settlement = Settlement::new();
    let upstream = MemBody::new([Step::Data(body.slice(..100)), Step::Error("reset")]);
    let (out, error, outcome) = run(&mut settlement, upstream, true).await;
    assert_eq!(concat(&out), &NONSTREAM_GROK46_XHIGH[..100]);
    assert!(matches!(error, Some(BodyError::Upstream(_))), "{error:?}");
    assert_eq!(outcome.status, OutcomeStatus::UpstreamTruncated);
    assert_eq!(outcome.billed, tokens(213, 25));
    assert_eq!(settlement.tap_available(), 256 << 20);
}

#[tokio::test(start_paused = true)]
async fn idle_and_first_byte_timeouts_end_the_body() {
    let idle = Duration::from_millis(800);
    let mut settlement = Settlement::new();
    // The first byte follows the commit, as it does for a JSON response.
    let upstream = MemBody::new([
        Step::Delay(Duration::from_millis(10)),
        Step::Data(Bytes::from_static(b"{\"id\":")),
        Step::Hang,
    ]);
    let mut tap = ResponseTap::new(
        upstream,
        TapPlan {
            usage_expected: true,
            timing: timing_idle(idle),
        },
        settlement.ctx(),
    );
    http_body_util::BodyExt::frame(&mut tap)
        .await
        .expect("a frame")
        .expect("the first frame");
    let start = Instant::now();
    let (_, error) = body_support::collect(&mut tap).await;
    assert!(
        matches!(error, Some(BodyError::Idle(d)) if d == idle),
        "{error:?}"
    );
    let silence = start.elapsed();
    assert!(silence >= idle && silence < idle + idle / 4, "{silence:?}");
    drop(tap);
    assert_eq!(settlement.outcome().status, OutcomeStatus::IdleTimeout);
    assert_eq!(settlement.tap_available(), 256 << 20);

    let mut settlement = Settlement::new();
    let mut tap = ResponseTap::new(
        MemBody::new([Step::Hang]),
        TapPlan {
            usage_expected: true,
            timing: timing_first_byte(Duration::from_secs(5), IDLE),
        },
        settlement.ctx(),
    );
    let start = Instant::now();
    let (_, error) = body_support::collect(&mut tap).await;
    assert!(matches!(error, Some(BodyError::FirstByte)), "{error:?}");
    assert_eq!(start.elapsed(), Duration::from_secs(5));
    drop(tap);
    assert_eq!(settlement.outcome().status, OutcomeStatus::FirstByteTimeout);
}

/// Polls without waiting: the next data frame if one is ready now.
trait FrameReady {
    fn frame_ready(&mut self) -> Option<Bytes>;
}

impl FrameReady for ResponseTap<MemBody> {
    fn frame_ready(&mut self) -> Option<Bytes> {
        match body_support::poll_once(self) {
            std::task::Poll::Ready(Some(Ok(frame))) => Some(frame.into_data().expect("data")),
            std::task::Poll::Ready(Some(Err(error))) => panic!("unexpected error {error:?}"),
            std::task::Poll::Ready(None) | std::task::Poll::Pending => None,
        }
    }
}
