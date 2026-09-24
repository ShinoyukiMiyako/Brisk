//! Stale pooled connections (the 02 stale-connection target, shortened): the
//! upstream closes idle connections after 1.2 s while the gateway's pool
//! keeps them for 0.9 s, so a request after 1.0 s or 1.5 s of silence must
//! never pick a connection the upstream is closing.

mod e2e_support;
mod scripted;

use std::time::Duration;

use brisk_gateway::outcome::OutcomeStatus;
use brisk_gateway::spec::StreamUsage;
use e2e_support::{TEST_MODEL, channel, chat_body, chat_only, sse_ok, start_gateway};
use scripted::{ScriptConfig, ScriptedUpstream};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn requests_after_idle_periods_never_hit_a_closed_connection() {
    let upstream = ScriptedUpstream::start_with(
        ScriptConfig {
            idle_close: Some(Duration::from_millis(1200)),
        },
        chat_only(|_, _| sse_ok()),
    )
    .await;
    let base_url = upstream.base_url();
    let gateway = start_gateway(|spec| {
        let mut only = channel("a", &base_url, StreamUsage::Passthrough);
        only.client.pool_idle_timeout = Duration::from_millis(900);
        spec.channels.push(only);
    })
    .await;

    for pause in [
        Duration::ZERO,
        Duration::from_millis(1000),
        Duration::from_millis(1500),
    ] {
        tokio::time::sleep(pause).await;
        let response = gateway.chat(chat_body(TEST_MODEL, true, None)).await;
        assert_eq!(response.status, 200, "after {pause:?}");
    }

    let settled = gateway.finish().await;
    assert_eq!(settled.tally.failovers, 0);
    assert!(
        settled
            .outcomes
            .iter()
            .all(|outcome| outcome.status == OutcomeStatus::Completed)
    );
}
