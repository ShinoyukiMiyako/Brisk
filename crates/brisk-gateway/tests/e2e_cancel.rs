//! Client disconnects over HTTP/1.1 and HTTP/2 (section 2.7 of the M1
//! contract): before commit, mid-stream, after a `commit_hold` commit, the
//! bounded drain for OpenAI-shaped streams, no drain for CPA-shaped streams
//! (D21), and a drain that runs out of time. Every case checks that the
//! upstream connection closes promptly after the client left.

mod e2e_support;
mod scripted;

use std::time::Duration;

use brisk_gateway::outcome::{Outcome, OutcomeStatus};
use brisk_gateway::spec::{GatewaySpec, StreamUsage};
use brisk_proto::UsageTokens;
use bytes::Bytes;
use e2e_support::{
    CONTENT_EVENT, DONE_EVENT, FINISH_EVENT, FINISH_USAGE_EVENT, Protocol, SCRIPTED_USAGE,
    TEST_MODEL, TestGateway, USAGE_ONLY_EVENT, channel, chat_body, chat_only, chat_requests,
    read_until, sse_headers, start_gateway, timing_bound,
};
use scripted::{Reply, ScriptedUpstream, SseEnd};
use tokio::time::Instant;

const PROTOCOLS: [Protocol; 2] = [Protocol::Http1, Protocol::Http2];

/// Gap between upstream frames in the mid-stream cases.
const FRAME_GAP: Duration = Duration::from_millis(50);

/// Longest time from the client leaving to the upstream connection closing:
/// 5 ms plus one frame gap on the bench VM (section 4.5), 1 s elsewhere.
fn close_bound() -> Duration {
    timing_bound(Duration::from_millis(5) + FRAME_GAP, Duration::from_secs(1))
}

fn frames(frames: &[(Duration, &'static [u8])]) -> Vec<(Duration, Bytes)> {
    frames
        .iter()
        .map(|&(delay, frame)| (delay, Bytes::from_static(frame)))
        .collect()
}

fn sse(frames_: Vec<(Duration, Bytes)>, end: SseEnd) -> Reply {
    Reply::Sse {
        head_delay: Duration::ZERO,
        headers: sse_headers(),
        frames: frames_,
        end,
    }
}

async fn gateway_for(
    upstream: &ScriptedUpstream,
    customize: impl FnOnce(&mut GatewaySpec),
) -> TestGateway {
    let base_url = upstream.base_url();
    start_gateway(|spec| {
        spec.channels
            .push(channel("a", &base_url, StreamUsage::Passthrough));
        customize(spec);
    })
    .await
}

/// Asserts that the upstream connection of the only chat request closed
/// within [`close_bound`] of `left`.
async fn assert_upstream_closed(upstream: &ScriptedUpstream, left: Instant, label: &str) {
    let conn = chat_requests(upstream).pop().expect("a chat request").conn;
    let closed = upstream
        .wait_closed(conn, Duration::from_secs(5))
        .await
        .unwrap_or_else(|| panic!("{label}: upstream connection still open"));
    let delay = closed.saturating_duration_since(left);
    assert!(
        delay <= close_bound(),
        "{label}: upstream closed {delay:?} after the client left"
    );
}

async fn only_outcome(gateway: TestGateway) -> Outcome {
    gateway.finish().await.only().clone()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn leaving_before_commit_closes_the_upstream() {
    for protocol in PROTOCOLS {
        let upstream = ScriptedUpstream::start(chat_only(|_, _| Reply::Hang)).await;
        let gateway = gateway_for(&upstream, |_| {}).await;

        let mut client = gateway.client(protocol).await;
        let body = chat_body(TEST_MODEL, true, None);
        let request = gateway.chat_with(body.clone());
        let pending = tokio::spawn(async move {
            let _ = client.try_send(request).await;
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(chat_requests(&upstream).len(), 1);
        pending.abort();
        let left = Instant::now();
        let _ = pending.await;
        assert_upstream_closed(&upstream, left, &format!("{protocol:?}")).await;

        let outcome = only_outcome(gateway).await;
        assert_eq!(
            outcome.status,
            OutcomeStatus::ClientCancelled,
            "{protocol:?}"
        );
        assert_eq!(outcome.http_status, None, "{protocol:?}");
        assert_eq!(
            outcome.billed,
            UsageTokens {
                input: (body.len() as u64).div_ceil(4),
                output: 0,
                cached_input: None,
                reasoning_output: None,
            },
            "{protocol:?}: before commit only the prompt is billed"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn leaving_mid_stream_closes_the_upstream() {
    for protocol in PROTOCOLS {
        let upstream = ScriptedUpstream::start(chat_only(|_, _| {
            let mut stream = vec![(Duration::ZERO, Bytes::from_static(CONTENT_EVENT))];
            stream.extend((0..100).map(|_| (FRAME_GAP, Bytes::from_static(CONTENT_EVENT))));
            sse(stream, SseEnd::Hang)
        }))
        .await;
        let gateway = gateway_for(&upstream, |_| {}).await;

        let mut client = gateway.client(protocol).await;
        let response = client
            .send(gateway.chat_with(chat_body(TEST_MODEL, true, None)))
            .await;
        assert_eq!(response.status(), 200);
        let mut body = response.into_body();
        read_until(&mut body, CONTENT_EVENT).await;
        read_until(&mut body, CONTENT_EVENT).await;
        drop(body);
        client.close();
        let left = Instant::now();
        assert_upstream_closed(&upstream, left, &format!("{protocol:?}")).await;

        let outcome = only_outcome(gateway).await;
        assert_eq!(
            outcome.status,
            OutcomeStatus::ClientCancelled,
            "{protocol:?}"
        );
        assert_eq!(outcome.http_status, Some(200), "{protocol:?}");
        assert_eq!(outcome.usage, None);
        assert_eq!(outcome.billed, outcome.estimate, "conservative settlement");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn leaving_after_a_commit_hold_commit_closes_the_upstream() {
    for protocol in PROTOCOLS {
        let upstream =
            ScriptedUpstream::start(chat_only(|_, _| sse(Vec::new(), SseEnd::Hang))).await;
        let gateway = gateway_for(&upstream, |spec| {
            spec.channels[0].timeouts.commit_hold = Duration::from_millis(200);
        })
        .await;

        let mut client = gateway.client(protocol).await;
        let response = client
            .send(gateway.chat_with(chat_body(TEST_MODEL, true, None)))
            .await;
        assert_eq!(response.status(), 200, "committed at commit_hold expiry");
        tokio::time::sleep(Duration::from_millis(200)).await;
        drop(response);
        client.close();
        let left = Instant::now();
        assert_upstream_closed(&upstream, left, &format!("{protocol:?}")).await;

        let outcome = only_outcome(gateway).await;
        assert_eq!(
            outcome.status,
            OutcomeStatus::ClientCancelled,
            "{protocol:?}"
        );
        assert_eq!(outcome.response_bytes, 0);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn leaving_between_finish_and_usage_drains_the_usage() {
    for protocol in PROTOCOLS {
        let upstream = ScriptedUpstream::start(chat_only(|_, _| {
            sse(
                frames(&[
                    (Duration::ZERO, CONTENT_EVENT),
                    (Duration::ZERO, FINISH_EVENT),
                    (Duration::from_millis(200), USAGE_ONLY_EVENT),
                    (Duration::ZERO, DONE_EVENT),
                ]),
                SseEnd::Finish,
            )
        }))
        .await;
        let mut gateway = gateway_for(&upstream, |_| {}).await;

        let mut client = gateway.client(protocol).await;
        let response = client
            .send(gateway.chat_with(chat_body(TEST_MODEL, true, None)))
            .await;
        let mut body = response.into_body();
        read_until(&mut body, FINISH_EVENT).await;
        drop(body);
        client.close();
        // The drain settles once the usage arrives. Shutting down before the
        // gateway has even seen the client leave would turn the drop into
        // `ShutdownAborted` (2.7, rule 2), so wait for the settlement first.
        gateway.next_outcome(Duration::from_secs(5)).await;

        let outcome = only_outcome(gateway).await;
        assert_eq!(outcome.status, OutcomeStatus::Drained, "{protocol:?}");
        assert_eq!(
            outcome.usage.map(|usage| (usage.input, usage.output)),
            Some(SCRIPTED_USAGE)
        );
        assert_eq!(outcome.billed.input, SCRIPTED_USAGE.0);
        assert_eq!(outcome.billed.output, SCRIPTED_USAGE.1);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn leaving_after_the_cpa_last_chunk_bills_its_usage_without_draining() {
    for protocol in PROTOCOLS {
        let upstream = ScriptedUpstream::start(chat_only(|_, _| {
            sse(
                frames(&[
                    (Duration::ZERO, CONTENT_EVENT),
                    (Duration::ZERO, FINISH_USAGE_EVENT),
                    (Duration::from_millis(200), DONE_EVENT),
                ]),
                SseEnd::Finish,
            )
        }))
        .await;
        let gateway = gateway_for(&upstream, |_| {}).await;

        let mut client = gateway.client(protocol).await;
        let response = client
            .send(gateway.chat_with(chat_body(TEST_MODEL, true, None)))
            .await;
        let mut body = response.into_body();
        read_until(&mut body, FINISH_USAGE_EVENT).await;
        drop(body);
        client.close();
        let left = Instant::now();
        // No drain: the upstream closes long before its `[DONE]` 200 ms later.
        assert_upstream_closed(&upstream, left, &format!("{protocol:?}")).await;

        let outcome = only_outcome(gateway).await;
        assert_eq!(
            outcome.status,
            OutcomeStatus::ClientCancelled,
            "{protocol:?}"
        );
        assert_eq!(
            outcome.billed.input, SCRIPTED_USAGE.0,
            "D21: usage is final"
        );
        assert_eq!(
            outcome.billed.output, SCRIPTED_USAGE.1,
            "D21: usage is final"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_drain_that_runs_out_of_time_settles_conservatively() {
    for protocol in PROTOCOLS {
        let upstream = ScriptedUpstream::start(chat_only(|_, _| {
            sse(
                frames(&[
                    (Duration::ZERO, CONTENT_EVENT),
                    (Duration::ZERO, FINISH_EVENT),
                    (Duration::from_secs(2), USAGE_ONLY_EVENT),
                    (Duration::ZERO, DONE_EVENT),
                ]),
                SseEnd::Finish,
            )
        }))
        .await;
        let gateway = gateway_for(&upstream, |spec| {
            spec.forwarding.drain_timeout = Duration::from_millis(100);
        })
        .await;

        let mut client = gateway.client(protocol).await;
        let response = client
            .send(gateway.chat_with(chat_body(TEST_MODEL, true, None)))
            .await;
        let mut body = response.into_body();
        read_until(&mut body, FINISH_EVENT).await;
        drop(body);
        client.close();
        let left = Instant::now();
        let conn = chat_requests(&upstream).pop().expect("a chat request").conn;
        let closed = upstream
            .wait_closed(conn, Duration::from_secs(5))
            .await
            .expect("the drain gave up and closed the upstream");
        assert!(
            closed.saturating_duration_since(left)
                < Duration::from_millis(100) + Duration::from_secs(1),
            "{protocol:?}: the drain is bounded by drain_timeout"
        );

        let outcome = only_outcome(gateway).await;
        assert_eq!(
            outcome.status,
            OutcomeStatus::ClientCancelled,
            "{protocol:?}"
        );
        assert_eq!(outcome.usage, None);
        assert_eq!(outcome.billed, outcome.estimate);
    }
}
