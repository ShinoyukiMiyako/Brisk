//! The upstream deadlines of section 2.8 with shortened values: first byte
//! before the response head, the CPA shape where head and first chunk arrive
//! together (CPA-M1-3), first byte after a `commit_hold` commit, idle time
//! between chunks, and SSE comments resetting the idle clock (04, 8.4).

// Not built until the scripted upstream (P2-SUPPORT) and `Gateway`
// (P5-GATEWAY) are merged into m1/integration; the integrator removes this
// attribute and the `rustfmt::skip` on `mod scripted` at that checkpoint.
#![cfg(any())]

#[rustfmt::skip]
mod scripted;
mod e2e_support;

use std::time::Duration;

use brisk_gateway::outcome::{FailureClass, OutcomeStatus};
use brisk_gateway::spec::{StreamUsage, Timeouts};
use bytes::Bytes;
use e2e_support::{
    CONTENT_EVENT, DONE_EVENT, FINISH_USAGE_EVENT, TEST_MODEL, TestGateway, channel, chat_body,
    chat_only, collect_until_error, sse_headers, start_gateway, stream_ok,
};
use scripted::{Reply, ScriptedUpstream, SseEnd};
use tokio::time::Instant;

async fn gateway_with(upstream: &ScriptedUpstream, timeouts: Timeouts) -> TestGateway {
    let base_url = upstream.base_url();
    start_gateway(|spec| {
        let mut only = channel("a", &base_url, StreamUsage::Passthrough);
        only.timeouts = timeouts;
        spec.channels.push(only);
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_head_before_the_first_byte_deadline_is_a_504() {
    let upstream = ScriptedUpstream::start(chat_only(|_, _| Reply::Hang)).await;
    let gateway = gateway_with(
        &upstream,
        Timeouts {
            first_byte: Duration::from_millis(300),
            ..Timeouts::default()
        },
    )
    .await;

    let started = Instant::now();
    let response = gateway.chat(chat_body(TEST_MODEL, true, None)).await;
    let elapsed = started.elapsed();
    assert_eq!(response.status, 504);
    assert_eq!(response.error_code(), "upstream_timeout");
    assert!(elapsed >= Duration::from_millis(300), "{elapsed:?}");
    assert!(elapsed < Duration::from_secs(2), "{elapsed:?}");

    let settled = gateway.finish().await;
    assert_eq!(
        settled.only().status,
        OutcomeStatus::Failed(FailureClass::FirstByteTimeout)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_late_head_with_its_first_chunk_completes() {
    let upstream = ScriptedUpstream::start(chat_only(|_, _| Reply::Sse {
        head_delay: Duration::from_millis(1500),
        headers: sse_headers(),
        frames: vec![(Duration::ZERO, stream_ok())],
        end: SseEnd::Finish,
    }))
    .await;
    let gateway = gateway_with(
        &upstream,
        Timeouts {
            commit_hold: Duration::from_millis(500),
            first_byte: Duration::from_secs(3),
            idle: Duration::from_secs(3),
        },
    )
    .await;

    let response = gateway.chat(chat_body(TEST_MODEL, true, None)).await;
    assert_eq!(response.status, 200);
    assert!(response.body == stream_ok());
    let settled = gateway.finish().await;
    assert_eq!(settled.only().status, OutcomeStatus::Completed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn silence_after_a_commit_hold_commit_ends_at_the_first_byte_deadline() {
    let upstream = ScriptedUpstream::start(chat_only(|_, _| Reply::Sse {
        head_delay: Duration::ZERO,
        headers: sse_headers(),
        frames: Vec::new(),
        end: SseEnd::Hang,
    }))
    .await;
    let gateway = gateway_with(
        &upstream,
        Timeouts {
            commit_hold: Duration::from_millis(200),
            first_byte: Duration::from_millis(800),
            idle: Duration::from_secs(60),
        },
    )
    .await;

    let started = Instant::now();
    let mut client = gateway.h1().await;
    let response = client
        .send(gateway.chat_with(chat_body(TEST_MODEL, true, None)))
        .await;
    let committed = started.elapsed();
    assert_eq!(response.status(), 200);
    assert!(committed < Duration::from_millis(800), "{committed:?}");
    let (received, clean) = collect_until_error(response).await;
    let ended = started.elapsed();
    assert!(!clean, "the stream ends with an error");
    assert!(received.is_empty());
    assert!(ended >= Duration::from_millis(800), "{ended:?}");
    drop(client);

    let settled = gateway.finish().await;
    assert_eq!(settled.only().status, OutcomeStatus::FirstByteTimeout);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn silence_after_the_first_chunk_is_an_idle_timeout() {
    const IDLE: Duration = Duration::from_millis(400);
    let upstream = ScriptedUpstream::start(chat_only(|_, _| Reply::Sse {
        head_delay: Duration::ZERO,
        headers: sse_headers(),
        frames: vec![(Duration::ZERO, Bytes::from_static(CONTENT_EVENT))],
        end: SseEnd::Hang,
    }))
    .await;
    let gateway = gateway_with(
        &upstream,
        Timeouts {
            idle: IDLE,
            ..Timeouts::default()
        },
    )
    .await;

    let started = Instant::now();
    let mut client = gateway.h1().await;
    let response = client
        .send(gateway.chat_with(chat_body(TEST_MODEL, true, None)))
        .await;
    let (received, clean) = collect_until_error(response).await;
    let ended = started.elapsed();
    assert!(!clean);
    assert!(received == CONTENT_EVENT);
    // D15: the timeout lands in [idle, 1.25 x idle) after the last byte;
    // the upper side gets slack for scheduling on a loaded test machine.
    assert!(ended >= IDLE, "{ended:?}");
    assert!(
        ended < IDLE * 5 / 4 + Duration::from_millis(500),
        "{ended:?}"
    );
    drop(client);

    let settled = gateway.finish().await;
    assert_eq!(settled.only().status, OutcomeStatus::IdleTimeout);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn keep_alive_comments_reset_the_idle_clock() {
    const IDLE: Duration = Duration::from_millis(400);
    let upstream = ScriptedUpstream::start(chat_only(|_, _| {
        let mut frames = vec![(Duration::ZERO, Bytes::from_static(CONTENT_EVENT))];
        frames.extend((0..8).map(|_| {
            (
                Duration::from_millis(150),
                Bytes::from_static(b": keep-alive\n\n"),
            )
        }));
        frames.push((
            Duration::from_millis(150),
            Bytes::from_static(FINISH_USAGE_EVENT),
        ));
        frames.push((Duration::ZERO, Bytes::from_static(DONE_EVENT)));
        Reply::Sse {
            head_delay: Duration::ZERO,
            headers: sse_headers(),
            frames,
            end: SseEnd::Finish,
        }
    }))
    .await;
    let gateway = gateway_with(
        &upstream,
        Timeouts {
            idle: IDLE,
            ..Timeouts::default()
        },
    )
    .await;

    let response = gateway.chat(chat_body(TEST_MODEL, true, None)).await;
    assert_eq!(response.status, 200);
    assert!(response.body.ends_with(DONE_EVENT));
    let settled = gateway.finish().await;
    assert_eq!(settled.only().status, OutcomeStatus::Completed);
}
