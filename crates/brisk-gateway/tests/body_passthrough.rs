//! Tests of the streamed response body, `PassthroughBody`, and of its test
//! upstream, `MemBody`.

mod body_support;

use std::task::Poll;
use std::time::Duration;

use body_support::{
    DONE_EVENT, GROK46_XHIGH, GROK46_XHIGH_USAGE, IDLE, MemBody, REQUEST_BYTES, STREAM_FIXTURES,
    Settlement, Step, concat, estimate_output, poll_once, split_at, split_random, splits,
    stream_plan, timing_first_byte, timing_idle, tokens, without_done,
};
use brisk_gateway::body::{BodyError, DrainLimits, HeldFrames, PassthroughBody, StreamPlan};
use brisk_gateway::outcome::{Outcome, OutcomeStatus, OutcomeTally};
use brisk_proto::sse::MAX_EVENT_BYTES;
use brisk_proto::{UsageErrorKind, UsageTokens};
use bytes::Bytes;
use http::{HeaderMap, HeaderValue};
use http_body::Body;
use http_body_util::BodyExt;
use tokio::time::Instant;

// ----- MemBody -------------------------------------------------------------

#[tokio::test]
async fn mem_body_returns_the_same_bytes_in_order() {
    let frames = [
        Bytes::from_static(b"data: 1\n\n"),
        Bytes::from_static(b"data: 2\n\n"),
    ];
    let mut body = MemBody::from_frames(frames.clone());
    for expected in &frames {
        let frame = body.frame().await.expect("a frame").expect("no error");
        let data = frame.into_data().expect("a data frame");
        assert_eq!(data.as_ptr(), expected.as_ptr());
        assert_eq!(data.len(), expected.len());
    }
    assert!(body.is_end_stream());
    assert!(body.frame().await.is_none());
}

#[test]
fn mem_body_pending_steps_return_pending_once() {
    let frame = Bytes::from_static(b"x");
    let mut body = MemBody::pending_before_each([frame.clone(), frame.clone()]);
    for _ in 0..2 {
        assert!(poll_once(&mut body).is_pending());
        match poll_once(&mut body) {
            Poll::Ready(Some(Ok(data))) => assert_eq!(data.into_data().ok(), Some(frame.clone())),
            other => panic!("expected a data frame, got {other:?}"),
        }
    }
    assert!(matches!(poll_once(&mut body), Poll::Ready(None)));
}

#[tokio::test(start_paused = true)]
async fn mem_body_delays_on_the_tokio_clock() {
    let mut body = MemBody::new([
        Step::Delay(Duration::from_millis(300)),
        Step::Data(Bytes::from_static(b"late")),
    ]);
    let start = Instant::now();
    let frame = body.frame().await.expect("a frame").expect("no error");
    assert_eq!(frame.into_data().ok(), Some(Bytes::from_static(b"late")));
    assert_eq!(start.elapsed(), Duration::from_millis(300));
}

#[test]
fn mem_body_hangs_without_ending() {
    let mut body = MemBody::new([Step::Hang]);
    for _ in 0..3 {
        assert!(poll_once(&mut body).is_pending());
    }
    assert!(!body.is_end_stream());
}

#[tokio::test]
async fn mem_body_errors_and_trailers() {
    let mut trailers = HeaderMap::new();
    trailers.insert("x-trailer", HeaderValue::from_static("1"));
    let mut body = MemBody::new([
        Step::Trailers(trailers.clone()),
        Step::Error("upstream reset"),
    ]);
    let frame = body.frame().await.expect("a frame").expect("no error");
    assert_eq!(frame.into_trailers().ok(), Some(trailers));
    let error = body.frame().await.expect("a frame").expect_err("an error");
    assert_eq!(error.to_string(), "upstream reset");
    assert!(body.frame().await.is_none());
}

#[tokio::test]
async fn mem_body_exact_size_hint_counts_down() {
    let mut body = MemBody::new([
        Step::Data(Bytes::from_static(b"abc")),
        Step::Pending,
        Step::Data(Bytes::from_static(b"de")),
    ])
    .with_exact_size();
    assert_eq!(body.size_hint().exact(), Some(5));
    body.frame().await.expect("a frame").expect("no error");
    assert_eq!(body.size_hint().exact(), Some(2));
    body.frame().await.expect("a frame").expect("no error");
    assert_eq!(body.size_hint().exact(), Some(0));
    assert_eq!(MemBody::from_frames([]).size_hint().exact(), None);
}

#[test]
fn split_random_is_reproducible_and_zero_copy() {
    let bytes = Bytes::from((0..=255u8).cycle().take(10_000).collect::<Vec<u8>>());
    let pieces = split_random(&bytes, 7, 64, 512);
    assert_eq!(pieces, split_random(&bytes, 7, 64, 512));
    let (last, whole) = pieces.split_last().expect("pieces");
    assert!(whole.iter().all(|piece| (64..=512).contains(&piece.len())));
    assert!((1..=512).contains(&last.len()));

    let mut offset = 0;
    for piece in &pieces {
        assert_eq!(piece.as_ptr(), bytes[offset..].as_ptr());
        offset += piece.len();
    }
    assert_eq!(offset, bytes.len());
}

#[test]
fn split_at_skips_empty_pieces() {
    let bytes = Bytes::from_static(b"data: x\r\n\r\n");
    let pieces = split_at(&bytes, &[0, 8, 8, 9]);
    assert_eq!(
        pieces,
        [
            Bytes::from_static(b"data: x\r"),
            Bytes::from_static(b"\n"),
            Bytes::from_static(b"\r\n"),
        ]
    );
}

// ----- Streams used below ------------------------------------------------

fn content_event(text: &str) -> String {
    format!(
        "data: {{\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"choices\":[{{\"index\":0,\"delta\":{{\"content\":\"{text}\"}},\"finish_reason\":null}}]}}\n\n"
    )
}

const FINISH_EVENT: &str = "data: {\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n";

/// The standalone usage event of the `OpenAI` shape, sent after the finish.
const USAGE_EVENT: &str = "data: {\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"choices\":[],\"usage\":{\"prompt_tokens\":213,\"completion_tokens\":9,\"total_tokens\":222}}\n\n";

const OPENAI_USAGE: UsageTokens = tokens(213, 9);

/// Content, finish, standalone usage, `[DONE]`: an `OpenAI` stream with
/// `include_usage`. Returns the stream and the stream without the usage.
fn openai_stream() -> (Bytes, Bytes) {
    let content: String = ["po", "ng", "!"].iter().map(|t| content_event(t)).collect();
    let before_usage = format!("{content}{FINISH_EVENT}");
    let stream = format!("{before_usage}{USAGE_EVENT}data: [DONE]\n\n");
    let stripped = format!("{before_usage}data: [DONE]\n\n");
    (Bytes::from(stream), Bytes::from(stripped))
}

/// Runs `frames` through a body to its end.
async fn run(
    settlement: &mut Settlement,
    frames: Vec<Bytes>,
    strip: bool,
) -> (Vec<Bytes>, Option<BodyError>, Outcome) {
    let mut body = settlement.passthrough(MemBody::from_frames(frames), strip);
    let (out, error) = body_support::collect(&mut body).await;
    drop(body);
    (out, error, settlement.outcome())
}

fn assert_billed_estimate(outcome: &Outcome) {
    assert_eq!(outcome.estimate.input, REQUEST_BYTES.div_ceil(4));
    assert_eq!(outcome.billed.input, outcome.estimate.input);
    assert_eq!(outcome.billed.output, outcome.estimate.output);
}

// ----- Normal mode -------------------------------------------------------

#[tokio::test]
async fn forwards_the_upstream_frames_themselves() {
    let stream = Bytes::from_static(GROK46_XHIGH);
    let frames = split_random(&stream, 11, 64, 512);
    let mut settlement = Settlement::new();
    let (out, error, outcome) = run(&mut settlement, frames.clone(), false).await;
    assert!(error.is_none());
    assert_eq!(out.len(), frames.len());
    for (sent, received) in out.iter().zip(&frames) {
        assert_eq!(
            sent.as_ptr(),
            received.as_ptr(),
            "the same Bytes, not a copy"
        );
        assert_eq!(sent.len(), received.len());
    }
    assert_eq!(outcome.status, OutcomeStatus::Completed);
    assert_eq!(outcome.http_status, Some(200));
    assert_eq!(outcome.usage, Some(GROK46_XHIGH_USAGE));
    assert_eq!(outcome.usage_error, None);
    assert_eq!(outcome.billed, GROK46_XHIGH_USAGE);
    assert_eq!(outcome.response_bytes, GROK46_XHIGH.len() as u64);
    assert_eq!(outcome.request_bytes, REQUEST_BYTES);
}

#[tokio::test]
async fn cpa_fixtures_give_their_usage_under_any_split() {
    for (name, fixture, usage) in STREAM_FIXTURES {
        let stream = Bytes::from_static(fixture);
        for frames in splits(&stream) {
            let pieces = frames.len();
            let mut settlement = Settlement::new();
            let (out, error, outcome) = run(&mut settlement, frames, false).await;
            assert!(error.is_none(), "{name}/{pieces}");
            assert_eq!(concat(&out), fixture, "{name}/{pieces}");
            assert_eq!(outcome.status, OutcomeStatus::Completed, "{name}/{pieces}");
            assert_eq!(outcome.usage, Some(usage), "{name}/{pieces}");
            assert_eq!(outcome.billed, usage, "{name}/{pieces}");
        }
    }
}

#[tokio::test]
async fn cpa_final_chunk_makes_the_usage_final() {
    // Cut after the final chunk, before `[DONE]`: the usage arrived together
    // with `finish_reason`, so it is billed exactly (D21) and not raised to
    // the byte estimate, which for grok46-xhigh would be 1071 output tokens.
    for (name, fixture, usage) in STREAM_FIXTURES {
        let truncated = Bytes::from_static(without_done(fixture));
        for frames in splits(&truncated) {
            let mut settlement = Settlement::new();
            let (_, error, outcome) = run(&mut settlement, frames, false).await;
            assert!(error.is_none(), "{name}");
            assert_eq!(outcome.status, OutcomeStatus::UpstreamTruncated, "{name}");
            assert_eq!(outcome.billed, usage, "{name}");
            assert!(outcome.estimate.output > usage.output, "{name}");
        }
    }
}

#[tokio::test]
async fn eof_without_done_is_truncated_and_billed_conservatively() {
    let stream = Bytes::from(content_event("po") + &content_event("ng"));
    let mut settlement = Settlement::new();
    let (out, error, outcome) = run(&mut settlement, vec![stream.clone()], false).await;
    assert!(error.is_none());
    assert_eq!(concat(&out), stream);
    assert_eq!(outcome.status, OutcomeStatus::UpstreamTruncated);
    assert_eq!(outcome.usage, None);
    assert_eq!(outcome.estimate.output, estimate_output(stream.len()));
    assert_billed_estimate(&outcome);
}

#[tokio::test]
async fn settles_before_the_last_frame_when_the_upstream_reports_its_end() {
    // hyper stops polling once `is_end_stream()` holds after a frame, so the
    // body must have settled by then rather than wait for `None`.
    for strip in [false, true] {
        let mut settlement = Settlement::new();
        let stream = Bytes::from_static(GROK46_XHIGH);
        let mut body =
            settlement.passthrough(MemBody::from_frames(splits(&stream).remove(3)), strip);
        let mut out = Vec::new();
        while !body.is_end_stream() {
            let frame = body.frame().await.expect("a frame before the end");
            out.push(frame.expect("no error").into_data().expect("data"));
        }
        assert_eq!(concat(&out), GROK46_XHIGH);
        let outcome = settlement.outcome();
        assert_eq!(outcome.status, OutcomeStatus::Completed);
        assert_eq!(outcome.response_bytes, GROK46_XHIGH.len() as u64);
        drop(body);
        settlement.assert_no_outcome();
    }
}

#[tokio::test]
async fn an_empty_upstream_settles_at_construction() {
    let mut settlement = Settlement::new();
    let body = settlement.passthrough(MemBody::from_frames([]), false);
    assert!(body.is_end_stream(), "hyper will not poll an ended body");
    let outcome = settlement.outcome();
    assert_eq!(outcome.status, OutcomeStatus::UpstreamTruncated);
    assert_eq!(outcome.response_bytes, 0);
    drop(body);
    settlement.assert_no_outcome();
}

#[tokio::test]
async fn held_frames_go_first_and_count_in_the_size_hint() {
    let stream = Bytes::from_static(GROK46_XHIGH);
    let frames = split_random(&stream, 5, 200, 700);
    let (before, after) = frames.split_at(2);
    let mut held = HeldFrames::default();
    for frame in before {
        held.push(frame.clone());
    }
    let upstream = MemBody::from_frames(after.to_vec()).with_exact_size();
    let mut settlement = Settlement::new();
    let mut body = PassthroughBody::new(upstream, held, stream_plan(false), settlement.ctx());
    assert_eq!(body.size_hint().exact(), Some(stream.len() as u64));
    let (out, error) = body_support::collect(&mut body).await;
    assert!(error.is_none());
    assert_eq!(out.len(), frames.len());
    for (sent, received) in out.iter().zip(&frames) {
        assert_eq!(sent.as_ptr(), received.as_ptr());
    }
    drop(body);
    assert_eq!(settlement.outcome().usage, Some(GROK46_XHIGH_USAGE));
}

#[tokio::test]
async fn strip_mode_has_no_size_hint() {
    let settlement = Settlement::new();
    let upstream = MemBody::from_frames([Bytes::from_static(GROK46_XHIGH)]).with_exact_size();
    let body = settlement.passthrough(upstream, true);
    assert_eq!(body.size_hint().exact(), None);
    assert_eq!(body.size_hint().upper(), None);
}

#[tokio::test]
async fn trailers_and_empty_frames_are_not_forwarded() {
    let mut trailers = HeaderMap::new();
    trailers.insert("x-trailer", HeaderValue::from_static("1"));
    let (stream, _) = openai_stream();
    let mut settlement = Settlement::new();
    let upstream = MemBody::new([
        Step::Data(stream.clone()),
        Step::Data(Bytes::new()),
        Step::Trailers(trailers),
    ]);
    let mut body = settlement.passthrough(upstream, false);
    let mut frames = Vec::new();
    while let Some(frame) = body.frame().await {
        frames.push(frame.expect("no error"));
    }
    assert_eq!(frames.len(), 1);
    assert!(frames[0].is_data());
    drop(body);
    assert_eq!(settlement.outcome().status, OutcomeStatus::Completed);
}

#[tokio::test]
async fn an_upstream_error_ends_the_stream_as_truncated() {
    let first = Bytes::from(content_event("po"));
    let mut settlement = Settlement::new();
    let upstream = MemBody::new([
        Step::Data(first.clone()),
        Step::Error("connection reset"),
        Step::Data(Bytes::from(content_event("never sent"))),
    ]);
    let mut body = settlement.passthrough(upstream, false);
    let (out, error) = body_support::collect(&mut body).await;
    assert_eq!(out, std::slice::from_ref(&first));
    let error = error.expect("the upstream error");
    assert!(matches!(error, BodyError::Upstream(_)), "{error:?}");
    assert_eq!(
        std::error::Error::source(&error).map(ToString::to_string),
        Some("connection reset".to_owned())
    );
    assert!(body.frame().await.is_none(), "nothing after the error");
    drop(body);
    let outcome = settlement.outcome();
    assert_eq!(outcome.status, OutcomeStatus::UpstreamTruncated);
    assert_eq!(outcome.response_bytes, first.len() as u64);
    assert_billed_estimate(&outcome);
}

#[tokio::test]
async fn an_oversized_event_is_a_protocol_error() {
    for strip in [false, true] {
        let mut huge = b"data: ".to_vec();
        huge.resize(MAX_EVENT_BYTES + 1, b'x');
        let first = Bytes::from(content_event("po"));
        let mut settlement = Settlement::new();
        let upstream = MemBody::from_frames([first.clone(), Bytes::from(huge)]);
        let mut body = settlement.passthrough(upstream, strip);
        let (out, error) = body_support::collect(&mut body).await;
        assert_eq!(concat(&out), first);
        assert!(matches!(error, Some(BodyError::Protocol(_))), "{error:?}");
        drop(body);
        let outcome = settlement.outcome();
        assert_eq!(outcome.status, OutcomeStatus::UpstreamProtocol);
        assert_billed_estimate(&outcome);
    }
}

#[tokio::test]
async fn first_event_error_is_forwarded_and_not_billed() {
    let stream = Bytes::from_static(
        b"data: {\"error\":{\"message\":\"x\",\"type\":\"server_error\"}}\n\ndata: [DONE]\n\n",
    );
    for truncated in [false, true] {
        let frames = if truncated {
            vec![stream.slice(..20)]
        } else {
            vec![stream.clone()]
        };
        let mut settlement = Settlement::new();
        let plan = StreamPlan {
            first_event_error: true,
            ..stream_plan(false)
        };
        let mut body = PassthroughBody::new(
            MemBody::from_frames(frames.clone()),
            HeldFrames::default(),
            plan,
            settlement.ctx(),
        );
        let (out, error) = body_support::collect(&mut body).await;
        assert!(error.is_none());
        assert_eq!(out, frames);
        drop(body);
        let outcome = settlement.outcome();
        assert_eq!(
            outcome.status,
            OutcomeStatus::ForwardedError { status: 200 }
        );
        assert_eq!(outcome.billed, UsageTokens::default());
        let mut tally = OutcomeTally::default();
        tally.record(&outcome);
        assert_eq!(tally.usage_missing, 0, "a forwarded error expects no usage");
    }
}

#[tokio::test]
async fn usage_parse_failures_are_reported_and_billed_conservatively() {
    let cases: [(&str, UsageErrorKind); 3] = [
        (
            // The CPA final chunk without `prompt_tokens`.
            "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"completion_tokens\":71}}\n\n",
            UsageErrorKind::MissingField("prompt_tokens"),
        ),
        (
            "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":[213,71]}\n\n",
            UsageErrorKind::Json {
                category: serde_json::error::Category::Data,
                column: 0,
            },
        ),
        (
            "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1},\"usage\":{\"prompt_tokens\":213,\"completion_tokens\":71}}\n\n",
            UsageErrorKind::Json {
                category: serde_json::error::Category::Data,
                column: 0,
            },
        ),
    ];
    for (broken, expected) in cases {
        for strip in [false, true] {
            let stream = Bytes::from(format!("{}{broken}data: [DONE]\n\n", content_event("pong")));
            let mut settlement = Settlement::new();
            let (out, error, outcome) =
                run(&mut settlement, splits(&stream).remove(2), strip).await;
            assert!(error.is_none());
            assert_eq!(concat(&out), stream, "the stream is forwarded untouched");
            assert_eq!(outcome.status, OutcomeStatus::Completed);
            assert_eq!(outcome.usage, None);
            match (outcome.usage_error, expected) {
                (
                    Some(UsageErrorKind::Json { category, .. }),
                    UsageErrorKind::Json { category: want, .. },
                ) => assert_eq!(category, want),
                (kind, want) => assert_eq!(kind, Some(want)),
            }
            assert_eq!(outcome.estimate.output, estimate_output(stream.len()));
            assert_billed_estimate(&outcome);
            let mut tally = OutcomeTally::default();
            tally.record(&outcome);
            assert_eq!((tally.usage_missing, tally.usage_parse_errors), (1, 1));
        }
    }
}

// ----- Strip mode --------------------------------------------------------

#[tokio::test]
async fn strip_removes_the_standalone_usage_event() {
    let (stream, stripped) = openai_stream();
    for frames in splits(&stream) {
        let pieces = frames.len();
        let mut settlement = Settlement::new();
        let (out, error, outcome) = run(&mut settlement, frames, true).await;
        assert!(error.is_none(), "{pieces}");
        assert_eq!(concat(&out), stripped, "{pieces} pieces");
        assert_eq!(outcome.status, OutcomeStatus::Completed);
        assert_eq!(outcome.usage, Some(OPENAI_USAGE));
        assert_eq!(outcome.billed, OPENAI_USAGE);
        assert_eq!(outcome.response_bytes, stripped.len() as u64);
    }
}

#[tokio::test]
async fn strip_keeps_the_cpa_final_chunk() {
    for (name, fixture, usage) in STREAM_FIXTURES {
        let stream = Bytes::from_static(fixture);
        for frames in splits(&stream) {
            let mut settlement = Settlement::new();
            let (out, error, outcome) = run(&mut settlement, frames, true).await;
            assert!(error.is_none(), "{name}");
            assert_eq!(concat(&out), fixture, "{name}: nothing is removed");
            assert_eq!(outcome.usage, Some(usage), "{name}");
        }
    }
}

#[tokio::test]
async fn strip_splits_a_chunk_around_a_removed_event() {
    let (stream, stripped) = openai_stream();
    let mut settlement = Settlement::new();
    let (out, error, _) = run(&mut settlement, vec![stream.clone()], true).await;
    assert!(error.is_none());
    // The chunk becomes the part before the usage event and the part after.
    assert_eq!(out.len(), 2);
    let usage_at = stripped.len() - DONE_EVENT.len();
    assert_eq!(out[0].as_ptr(), stream.as_ptr());
    assert_eq!(out[0].len(), usage_at);
    assert_eq!(out[1], DONE_EVENT);
}

#[tokio::test]
async fn strip_forwards_complete_events_and_holds_the_tail() {
    let (stream, _) = openai_stream();
    let first = content_event("po").len();
    // Frame 1 ends inside the second event; only the first goes out.
    let frames = split_at(&stream, &[first + 10]);
    let mut settlement = Settlement::new();
    let mut body = settlement.passthrough(
        MemBody::new([Step::Data(frames[0].clone()), Step::Hang]),
        true,
    );
    let sent = body.frame().await.expect("a frame").expect("no error");
    assert_eq!(sent.into_data().expect("data"), stream.slice(..first));
    assert!(
        poll_once(&mut body).is_pending(),
        "the tail waits for its event"
    );
    drop(body);
    let outcome = settlement.outcome();
    assert_eq!(outcome.status, OutcomeStatus::ClientCancelled);
    assert_eq!(outcome.response_bytes, first as u64);
}

#[tokio::test]
async fn strip_handles_events_over_many_chunks() {
    // One byte per frame: every event spans more chunks than strip mode
    // stages, so each is copied out of the scanner when it completes.
    let long = content_event(&"x".repeat(3000));
    let (stream, stripped) = openai_stream();
    let stream = Bytes::from([long.as_bytes(), &stream].concat());
    let stripped = [long.as_bytes(), &stripped].concat();
    for (min, max) in [(1, 1), (1, 9), (7, 64)] {
        let frames = split_random(&stream, 3, min, max);
        let mut settlement = Settlement::new();
        let (out, error, outcome) = run(&mut settlement, frames, true).await;
        assert!(error.is_none());
        assert_eq!(concat(&out), stripped, "pieces {min}..={max}");
        assert_eq!(outcome.usage, Some(OPENAI_USAGE));
    }
}

#[tokio::test]
async fn strip_releases_the_held_tail_at_eof() {
    let (stream, _) = openai_stream();
    let long = "data: {\"choices\":[{\"delta\":{\"content\":\"".to_owned() + &"y".repeat(500);
    // Unfinished tails: mid-line, after a line ending, and after a `\r` that
    // could still pair with a `\n`; short ones stay staged, long ones spill.
    for tail in [
        "data: {\"cho".to_owned(),
        "data: {\"choices\":[]}\n".to_owned(),
        "data: {\"choices\":[]}\r".to_owned(),
        long.clone(),
        long.clone() + "\n",
        long.clone() + "\r",
        long + "\r\n",
    ] {
        let input = Bytes::from([&stream[..], tail.as_bytes()].concat());
        for (min, max) in [(1, 1), (3, 30), (4096, 4096)] {
            let frames = split_random(&input, 9, min, max);
            let mut settlement = Settlement::new();
            let (out, error, outcome) = run(&mut settlement, frames, true).await;
            assert!(error.is_none());
            let expected = [&openai_stream().1[..], tail.as_bytes()].concat();
            assert_eq!(
                concat(&out),
                expected,
                "tail {:?}",
                &tail[..tail.len().min(20)]
            );
            // `[DONE]` came before the garbage tail.
            assert_eq!(outcome.status, OutcomeStatus::Completed);
            assert_eq!(outcome.response_bytes, expected.len() as u64);
        }
    }
}

#[tokio::test]
async fn strip_forwards_a_byte_order_mark() {
    let (stream, stripped) = openai_stream();
    let with_bom = Bytes::from([&b"\xEF\xBB\xBF"[..], USAGE_EVENT.as_bytes(), &stream].concat());
    let expected = [&b"\xEF\xBB\xBF"[..], &stripped].concat();
    for frames in splits(&with_bom) {
        let mut settlement = Settlement::new();
        let (out, _, _) = run(&mut settlement, frames, true).await;
        assert_eq!(concat(&out), expected);
    }
}

#[tokio::test]
async fn strip_removes_a_crlf_event_split_between_cr_and_lf() {
    let usage = USAGE_EVENT.replace('\n', "\r\n");
    let stream = Bytes::from(format!("{}{usage}data: [DONE]\r\n\r\n", content_event("a")));
    let expected = format!("{}data: [DONE]\r\n\r\n", content_event("a"));
    // Cut right after the `\r` of the usage event's blank line.
    let cut = stream.len() - "\ndata: [DONE]\r\n\r\n".len();
    for frames in [
        split_at(&stream, &[cut]),
        split_at(&stream, &[cut - 1, cut, cut + 1]),
    ] {
        let mut settlement = Settlement::new();
        let (out, _, outcome) = run(&mut settlement, frames, true).await;
        assert_eq!(String::from_utf8(concat(&out)).unwrap(), expected);
        assert_eq!(outcome.usage, Some(OPENAI_USAGE));
    }
}

// ----- Timeouts ----------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn idle_timeout_fires_within_its_window() {
    let idle = Duration::from_millis(400);
    for gap in [0u64, 37, 99] {
        let mut settlement = Settlement::new();
        let upstream = MemBody::new([
            Step::Data(Bytes::from(content_event("a"))),
            Step::Delay(Duration::from_millis(300)),
            Step::Data(Bytes::from(content_event("b"))),
            Step::Delay(Duration::from_millis(gap)),
            Step::Data(Bytes::from(content_event("c"))),
            Step::Hang,
        ]);
        let plan = StreamPlan {
            timing: timing_idle(idle),
            ..stream_plan(false)
        };
        let mut body =
            PassthroughBody::new(upstream, HeldFrames::default(), plan, settlement.ctx());
        let start = Instant::now();
        let mut last_byte = start;
        loop {
            match body.frame().await {
                Some(Ok(_)) => last_byte = Instant::now(),
                Some(Err(error)) => {
                    assert!(
                        matches!(error, BodyError::Idle(d) if d == idle),
                        "{error:?}"
                    );
                    break;
                }
                None => panic!("the upstream never ends"),
            }
        }
        let silence = last_byte.elapsed();
        assert!(
            silence >= idle && silence < idle + idle / 4,
            "timed out after {silence:?} of silence"
        );
        assert!(
            last_byte.duration_since(start) >= Duration::from_millis(300),
            "no early timeout"
        );
        drop(body);
        let outcome = settlement.outcome();
        assert_eq!(outcome.status, OutcomeStatus::IdleTimeout);
        assert_billed_estimate(&outcome);
    }
}

#[tokio::test(start_paused = true)]
async fn held_frames_do_not_restart_the_idle_period() {
    // Held frames arrived before commit, where the idle clock starts: a
    // stream silent from commit on times out exactly `idle` later.
    let idle = Duration::from_millis(400);
    let mut held = HeldFrames::default();
    held.push(Bytes::from(content_event("a")));
    held.push(Bytes::from(content_event("b")));
    let mut settlement = Settlement::new();
    let plan = StreamPlan {
        timing: timing_idle(idle),
        ..stream_plan(false)
    };
    let committed = Instant::now();
    let mut body = PassthroughBody::new(MemBody::new([Step::Hang]), held, plan, settlement.ctx());
    let (out, error) = body_support::collect(&mut body).await;
    assert_eq!(out.len(), 2);
    assert!(matches!(error, Some(BodyError::Idle(_))), "{error:?}");
    assert_eq!(committed.elapsed(), idle);
    drop(body);
    assert_eq!(settlement.outcome().status, OutcomeStatus::IdleTimeout);
}

#[tokio::test(start_paused = true)]
async fn first_byte_deadline_applies_until_the_first_byte() {
    // Nothing arrives: the stream ends at the first-byte deadline.
    let mut settlement = Settlement::new();
    let plan = StreamPlan {
        timing: timing_first_byte(Duration::from_secs(3), IDLE),
        ..stream_plan(false)
    };
    let mut body = PassthroughBody::new(
        MemBody::new([Step::Hang]),
        HeldFrames::default(),
        plan,
        settlement.ctx(),
    );
    let start = Instant::now();
    let error = body
        .frame()
        .await
        .expect("an error")
        .expect_err("the deadline");
    assert!(matches!(error, BodyError::FirstByte), "{error:?}");
    assert_eq!(start.elapsed(), Duration::from_secs(3));
    drop(body);
    let outcome = settlement.outcome();
    assert_eq!(outcome.status, OutcomeStatus::FirstByteTimeout);
    assert_eq!(outcome.billed, tokens(REQUEST_BYTES.div_ceil(4), 0));

    // The CPA shape: the first byte comes late but in time, then idle rules.
    let mut settlement = Settlement::new();
    let idle = Duration::from_secs(2);
    let plan = StreamPlan {
        timing: timing_first_byte(Duration::from_secs(3), idle),
        ..stream_plan(false)
    };
    let upstream = MemBody::new([
        Step::Delay(Duration::from_millis(2500)),
        Step::Data(Bytes::from(content_event("a"))),
        Step::Hang,
    ]);
    let mut body = PassthroughBody::new(upstream, HeldFrames::default(), plan, settlement.ctx());
    let start = Instant::now();
    body.frame().await.expect("a frame").expect("in time");
    let error = body.frame().await.expect("an error").expect_err("idle");
    assert!(matches!(error, BodyError::Idle(_)), "{error:?}");
    let silence = start.elapsed().saturating_sub(Duration::from_millis(2500));
    assert!(silence >= idle && silence < idle + idle / 4, "{silence:?}");
    drop(body);
    assert_eq!(settlement.outcome().status, OutcomeStatus::IdleTimeout);
}

// ----- Drop, drain and settlement ----------------------------------------

/// Polls `body` until it returns `Pending` (a no-op waker), then returns the
/// bytes it sent.
fn poll_until_pending(body: &mut PassthroughBody<MemBody>) -> Vec<u8> {
    let mut sent = Vec::new();
    loop {
        match poll_once(body) {
            Poll::Ready(Some(Ok(frame))) => sent.extend_from_slice(&frame.into_data().unwrap()),
            Poll::Pending => return sent,
            other @ Poll::Ready(_) => panic!("expected frames then Pending, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn cpa_shape_dropped_before_done_bills_the_usage_without_a_drain() {
    let truncated = Bytes::from_static(without_done(GROK46_XHIGH));
    for strip in [false, true] {
        let mut settlement = Settlement::new();
        let upstream = MemBody::from_frames(split_random(&truncated, 1, 64, 512)).then(Step::Hang);
        let mut body = settlement.passthrough(upstream, strip);
        let sent = poll_until_pending(&mut body);
        assert_eq!(sent, truncated);
        drop(body);
        // Settled inside `drop`: a drain task would settle only later.
        let outcome = settlement.outcome();
        assert_eq!(outcome.status, OutcomeStatus::ClientCancelled);
        assert_eq!(outcome.billed, GROK46_XHIGH_USAGE);
        assert_eq!(outcome.billed.input, 213);
        assert_eq!(outcome.billed.output, 71);
    }
}

#[tokio::test(start_paused = true)]
async fn openai_shape_dropped_after_finish_drains_the_usage() {
    let content = content_event("pong");
    for strip in [false, true] {
        let mut settlement = Settlement::new();
        let upstream = MemBody::new([
            Step::Data(Bytes::from(content.clone())),
            Step::Data(Bytes::from(FINISH_EVENT)),
            Step::Delay(Duration::from_millis(200)),
            Step::Data(Bytes::from(USAGE_EVENT)),
            Step::Data(Bytes::from(DONE_EVENT)),
            // The drain ends at `[DONE]` and never waits for this.
            Step::Hang,
        ]);
        let mut body = settlement.passthrough(upstream, strip);
        let sent = poll_until_pending(&mut body);
        assert_eq!(sent, [content.as_bytes(), FINISH_EVENT.as_bytes()].concat());
        let dropped = Instant::now();
        drop(body);
        let outcome = settlement.next_outcome().await;
        assert_eq!(
            dropped.elapsed(),
            Duration::from_millis(200),
            "ended at [DONE]"
        );
        assert_eq!(outcome.status, OutcomeStatus::Drained);
        assert_eq!(outcome.usage, Some(OPENAI_USAGE));
        assert_eq!(outcome.billed, OPENAI_USAGE);
        assert_eq!(outcome.response_bytes, sent.len() as u64);
        settlement.assert_no_outcome();
    }
}

#[tokio::test(start_paused = true)]
async fn drain_gives_up_at_its_timeout() {
    let mut settlement = Settlement::new();
    let upstream = MemBody::new([Step::Data(Bytes::from(FINISH_EVENT)), Step::Hang]);
    let mut body = settlement.passthrough(upstream, false);
    let sent = poll_until_pending(&mut body);
    let dropped = Instant::now();
    drop(body);
    let outcome = settlement.next_outcome().await;
    assert_eq!(dropped.elapsed(), Duration::from_secs(2));
    assert_eq!(outcome.status, OutcomeStatus::ClientCancelled);
    assert_eq!(outcome.estimate.output, estimate_output(sent.len()));
    assert_billed_estimate(&outcome);
}

#[tokio::test(start_paused = true)]
async fn drain_stops_at_its_byte_limit() {
    let mut settlement = Settlement::with(
        1 << 20,
        DrainLimits {
            max_bytes: 1024,
            timeout: Duration::from_secs(2),
        },
    );
    // 40 comments of 36 bytes stand between the finish and the usage.
    let padding =
        (0..40).map(|_| Step::Data(Bytes::from_static(b": padding padding padding pad\n\n")));
    let steps = [Step::Data(Bytes::from(FINISH_EVENT)), Step::Pending]
        .into_iter()
        .chain(padding)
        .chain([Step::Data(Bytes::from(USAGE_EVENT)), Step::Hang]);
    let mut body = settlement.passthrough(MemBody::new(steps), false);
    poll_until_pending(&mut body);
    let dropped = Instant::now();
    drop(body);
    let outcome = settlement.next_outcome().await;
    assert!(
        dropped.elapsed() < Duration::from_secs(2),
        "stopped by bytes, not time"
    );
    assert_eq!(outcome.status, OutcomeStatus::ClientCancelled);
    assert_eq!(outcome.usage, None);
}

#[tokio::test]
async fn dropped_before_finish_is_cancelled_and_billed_conservatively() {
    let stream = Bytes::from_static(GROK46_XHIGH);
    let half = stream.slice(..2000);
    let mut settlement = Settlement::new();
    let upstream = MemBody::from_frames(split_random(&half, 4, 64, 512)).then(Step::Hang);
    let mut body = settlement.passthrough(upstream, false);
    let sent = poll_until_pending(&mut body);
    assert_eq!(sent.len(), 2000);
    drop(body);
    let outcome = settlement.outcome();
    assert_eq!(outcome.status, OutcomeStatus::ClientCancelled);
    assert_eq!(outcome.response_bytes, 2000);
    // ceil(852 / 4) and ceil(2000 / 4).
    assert_eq!(outcome.billed, tokens(213, 500));
}

#[tokio::test]
async fn dropped_during_shutdown_is_aborted_without_a_drain() {
    let mut settlement = Settlement::new();
    let upstream = MemBody::new([Step::Data(Bytes::from(FINISH_EVENT)), Step::Hang]);
    let mut body = settlement.passthrough(upstream, false);
    let sent = poll_until_pending(&mut body);
    settlement.shared.sink.begin_shutdown();
    drop(body);
    let outcome = settlement.outcome();
    assert_eq!(outcome.status, OutcomeStatus::ShutdownAborted);
    assert_eq!(outcome.billed, tokens(213, estimate_output(sent.len())));
}

#[test]
fn dropped_outside_a_runtime_cancels_without_a_drain() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let mut settlement = Settlement::new();
    let body = runtime.block_on(async {
        let upstream = MemBody::new([Step::Data(Bytes::from(FINISH_EVENT)), Step::Hang]);
        let mut body = settlement.passthrough(upstream, false);
        poll_until_pending(&mut body);
        body
    });
    // A thread without a runtime cannot spawn the drain the finish asks for.
    std::thread::spawn(move || drop(body))
        .join()
        .expect("dropping outside a runtime does not panic");
    let outcome = settlement.outcome();
    assert_eq!(outcome.status, OutcomeStatus::ClientCancelled);
    drop(runtime);
}

#[tokio::test]
async fn one_outcome_per_body_whatever_happens_after_the_end() {
    let (stream, _) = openai_stream();
    let mut settlement = Settlement::new();
    let mut body = settlement.passthrough(MemBody::from_frames([stream]), false);
    body_support::collect(&mut body).await;
    assert!(body.frame().await.is_none());
    assert_eq!(settlement.outcome().status, OutcomeStatus::Completed);
    drop(body);
    settlement.assert_no_outcome();
}

// ----- Strip mode, property ---------------------------------------------

mod strip_property {
    use proptest::prelude::*;

    use super::*;

    #[derive(Debug, Clone, Copy)]
    enum Eol {
        Lf,
        CrLf,
        Cr,
    }

    impl Eol {
        fn bytes(self) -> &'static str {
            match self {
                Self::Lf => "\n",
                Self::CrLf => "\r\n",
                Self::Cr => "\r",
            }
        }
    }

    #[derive(Debug, Clone)]
    enum Kind {
        Content(String),
        Comment,
        Named(String),
        /// CPA: usage in the chunk that carries `finish_reason`; never cut.
        CpaFinal(u64, u64),
        /// `OpenAI`: `choices: []` and usage; cut.
        UsageOnly(u64, u64),
        /// No `choices` at all; cut.
        UsageNoChoices(u64, u64),
        /// A usage-only payload over two `data` lines; cut.
        MultiLineUsage(u64, u64),
        Done,
    }

    #[derive(Debug, Clone)]
    struct Event {
        kind: Kind,
        eol: Eol,
        /// Blank lines after the one that ends the event; never cut.
        extra_blank: usize,
    }

    fn content_line(text: &str) -> String {
        format!(
            "data: {{\"choices\":[{{\"index\":0,\"delta\":{{\"content\":\"{text}\"}},\"finish_reason\":null}}]}}"
        )
    }

    fn usage_member(input: u64, output: u64) -> String {
        format!("\"usage\":{{\"prompt_tokens\":{input},\"completion_tokens\":{output}}}")
    }

    impl Event {
        fn lines(&self) -> Vec<String> {
            match &self.kind {
                Kind::Content(text) => vec![content_line(text)],
                Kind::Comment => vec![": keep-alive".to_owned()],
                Kind::Named(text) => vec![
                    "event: delta".to_owned(),
                    "id: 5".to_owned(),
                    "retry".to_owned(),
                    content_line(text),
                ],
                Kind::CpaFinal(input, output) => vec![format!(
                    "data: {{\"choices\":[{{\"index\":0,\"delta\":{{}},\"finish_reason\":\"stop\"}}],{}}}",
                    usage_member(*input, *output)
                )],
                Kind::UsageOnly(input, output) => vec![format!(
                    "data: {{\"choices\":[],{}}}",
                    usage_member(*input, *output)
                )],
                Kind::UsageNoChoices(input, output) => {
                    vec![format!("data: {{{}}}", usage_member(*input, *output))]
                }
                Kind::MultiLineUsage(input, output) => vec![
                    "data: {\"choices\":[],".to_owned(),
                    format!("data: {}}}", usage_member(*input, *output)),
                ],
                Kind::Done => vec!["data: [DONE]".to_owned()],
            }
        }

        fn usage(&self) -> Option<UsageTokens> {
            match self.kind {
                Kind::CpaFinal(input, output)
                | Kind::UsageOnly(input, output)
                | Kind::UsageNoChoices(input, output)
                | Kind::MultiLineUsage(input, output) => Some(tokens(input, output)),
                _ => None,
            }
        }

        fn is_cut(&self) -> bool {
            matches!(
                self.kind,
                Kind::UsageOnly(..) | Kind::UsageNoChoices(..) | Kind::MultiLineUsage(..)
            )
        }
    }

    fn eol() -> impl Strategy<Value = Eol> {
        prop_oneof![Just(Eol::Lf), Just(Eol::CrLf), Just(Eol::Cr)]
    }

    fn kind() -> impl Strategy<Value = Kind> {
        let text = "[a-z ]{0,40}";
        let count = || 0u64..100_000;
        prop_oneof![
            4 => text.prop_map(Kind::Content),
            1 => Just(Kind::Comment),
            1 => text.prop_map(Kind::Named),
            1 => (count(), count()).prop_map(|(i, o)| Kind::CpaFinal(i, o)),
            2 => (count(), count()).prop_map(|(i, o)| Kind::UsageOnly(i, o)),
            1 => (count(), count()).prop_map(|(i, o)| Kind::UsageNoChoices(i, o)),
            1 => (count(), count()).prop_map(|(i, o)| Kind::MultiLineUsage(i, o)),
            1 => Just(Kind::Done),
        ]
    }

    fn event() -> impl Strategy<Value = Event> {
        (
            kind(),
            eol(),
            prop_oneof![4 => Just(0usize), 1 => 1usize..3],
        )
            .prop_map(|(kind, eol, extra_blank)| Event {
                kind,
                eol,
                extra_blank,
            })
    }

    #[derive(Debug, Clone)]
    struct Case {
        bom: bool,
        events: Vec<Event>,
        /// An unfinished event at EOF, forwarded as it is.
        tail: Option<(String, Option<Eol>)>,
        max_piece: usize,
        seed: u64,
    }

    fn case() -> impl Strategy<Value = Case> {
        (
            any::<bool>(),
            prop::collection::vec(event(), 0..24),
            prop::option::of(("[a-z]{0,300}", prop::option::of(eol()))),
            prop::sample::select(vec![1usize, 2, 5, 17, 64, 700]),
            any::<u64>(),
        )
            .prop_map(|(bom, events, tail, max_piece, seed)| Case {
                bom,
                events,
                tail,
                max_piece,
                seed,
            })
    }

    impl Case {
        /// The upstream stream and what strip mode must turn it into.
        fn render(&self) -> (Vec<u8>, Vec<u8>) {
            let mut input = Vec::new();
            let mut expected = Vec::new();
            if self.bom {
                input.extend_from_slice(b"\xEF\xBB\xBF");
                expected.extend_from_slice(b"\xEF\xBB\xBF");
            }
            for event in &self.events {
                let eol = event.eol.bytes();
                let mut bytes = String::new();
                for line in event.lines() {
                    bytes.push_str(&line);
                    bytes.push_str(eol);
                }
                bytes.push_str(eol);
                input.extend_from_slice(bytes.as_bytes());
                if !event.is_cut() {
                    expected.extend_from_slice(bytes.as_bytes());
                }
                let extra = eol.repeat(event.extra_blank);
                input.extend_from_slice(extra.as_bytes());
                expected.extend_from_slice(extra.as_bytes());
            }
            if let Some((text, eol)) = &self.tail {
                let tail = format!(
                    "data: {{\"choices\":[{{\"delta\":{{\"content\":\"{text}{}",
                    eol.map_or("", Eol::bytes)
                );
                input.extend_from_slice(tail.as_bytes());
                expected.extend_from_slice(tail.as_bytes());
            }
            (input, expected)
        }
    }

    proptest! {
        #[test]
        fn strip_removes_exactly_the_usage_only_events(case in case()) {
            let (input, expected) = case.render();
            let frames = if input.is_empty() {
                Vec::new()
            } else {
                split_random(&Bytes::from(input), case.seed, 1, case.max_piece)
            };
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .unwrap();
            let mut settlement = Settlement::new();
            let (out, error, outcome) = runtime.block_on(run(&mut settlement, frames, true));
            prop_assert!(error.is_none());
            prop_assert_eq!(concat(&out), expected);
            let done = case.events.iter().any(|event| matches!(event.kind, Kind::Done));
            let status = if done {
                OutcomeStatus::Completed
            } else {
                OutcomeStatus::UpstreamTruncated
            };
            prop_assert_eq!(outcome.status, status);
            let usage = case.events.iter().filter_map(Event::usage).next_back();
            prop_assert_eq!(outcome.usage, usage);
        }
    }
}
