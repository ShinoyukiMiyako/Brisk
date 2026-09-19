//! Failover cost (the 02 target): with channel one answering 429 at once and
//! channel two on a warm connection, a request costs at most
//! `timing_bound(1 ms, 50 ms)` more than the same request sent to channel two
//! alone. The strict bound applies only with `BRISK_STRICT_TIMING=1` on the
//! bench VM (section 4.6).

// Not built until the scripted upstream (P2-SUPPORT) and `Gateway`
// (P5-GATEWAY) are merged into m1/integration; the integrator removes this
// attribute and the `rustfmt::skip` on `mod scripted` at that checkpoint.
#![cfg(any())]

#[rustfmt::skip]
mod scripted;
mod e2e_support;

use std::time::Duration;

use brisk_gateway::spec::StreamUsage;
use bytes::Bytes;
use e2e_support::{
    FIRST, TEST_MODEL, TestGateway, channel, chat_body, chat_only, sse_ok, start_gateway,
    timing_bound,
};
use scripted::{Reply, ScriptedUpstream};
use tokio::time::Instant;

const ROUNDS: usize = 41;

/// Median latency of `ROUNDS` sequential requests on one client connection,
/// after one unmeasured request that warms every connection involved.
async fn median_latency(gateway: &TestGateway) -> Duration {
    let body = chat_body(TEST_MODEL, true, None);
    let mut client = gateway.h1().await;
    let warm = client.send_collect(gateway.chat_with(body.clone())).await;
    assert_eq!(warm.status, 200);
    let mut samples = Vec::with_capacity(ROUNDS);
    for _ in 0..ROUNDS {
        let started = Instant::now();
        let response = client.send_collect(gateway.chat_with(body.clone())).await;
        samples.push(started.elapsed());
        assert_eq!(response.status, 200);
    }
    samples.sort_unstable();
    samples[ROUNDS / 2]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failover_adds_little_latency() {
    let limited = ScriptedUpstream::start(chat_only(|_, _| Reply::Status {
        status: 429,
        headers: vec![("content-type", String::from("application/json"))],
        body: Bytes::from_static(br#"{"error":{"message":"slow down"}}"#),
    }))
    .await;
    let healthy = ScriptedUpstream::start(chat_only(|_, _| sse_ok())).await;
    let (limited_url, healthy_url) = (limited.base_url(), healthy.base_url());

    let direct = start_gateway(|spec| {
        spec.channels
            .push(channel("healthy", &healthy_url, StreamUsage::Passthrough));
    })
    .await;
    let baseline = median_latency(&direct).await;
    direct.finish().await;

    let failing_over = start_gateway(|spec| {
        let mut preferred = channel("limited", &limited_url, StreamUsage::Passthrough);
        preferred.weight = FIRST;
        spec.channels.push(preferred);
        spec.channels
            .push(channel("healthy", &healthy_url, StreamUsage::Passthrough));
    })
    .await;
    let with_failover = median_latency(&failing_over).await;
    let settled = failing_over.finish().await;
    assert_eq!(settled.tally.failovers, (ROUNDS + 1) as u64);

    let cost = with_failover.saturating_sub(baseline);
    let bound = timing_bound(Duration::from_millis(1), Duration::from_millis(50));
    assert!(
        cost <= bound,
        "failover cost {cost:?} (baseline {baseline:?}, with failover {with_failover:?}) exceeds {bound:?}"
    );
}
