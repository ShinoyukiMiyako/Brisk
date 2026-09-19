//! Streaming and non-streaming pass-through against the in-process
//! brisk-mock and the scripted upstream: `include_usage` injection and
//! stripping (D8), usage settlement, exact upstream framing, model mapping and
//! the SSE response headers Brisk sets itself.

// Not built until the scripted upstream (P2-SUPPORT) and `Gateway`
// (P5-GATEWAY) are merged into m1/integration; the integrator removes this
// attribute and the `rustfmt::skip` on `mod scripted` at that checkpoint.
#![cfg(any())]

#[rustfmt::skip]
mod scripted;
mod e2e_support;

use std::net::SocketAddr;

use brisk_bench_core::wire::BenchParams;
use brisk_gateway::outcome::{OutcomeStatus, estimate};
use brisk_gateway::spec::{SpliceMode, StreamUsage, WarmupMethod, WarmupTarget};
use brisk_mock::{MockConfig, ServerHandle};
use brisk_proto::UsageTokens;
use bytes::Bytes;
use e2e_support::{
    CONTENT_EVENT, DONE_EVENT, FINISH_EVENT, SCRIPTED_USAGE, TEST_MODEL, USAGE_ONLY_EVENT, channel,
    chat_body, chat_only, chat_requests, contains, header_values, json_headers, sse_reply,
    start_gateway,
};
use scripted::{Reply, ScriptedUpstream};

/// `,"stream_options":{"include_usage":true}`, inserted before the closing
/// brace of a streaming request that has no `stream_options`.
const INJECTED: &[u8] = br#","stream_options":{"include_usage":true}"#;

const MOCK_CHUNKS: u32 = 5;

fn start_mock() -> ServerHandle {
    let params = BenchParams {
        ttft_us: 0,
        interval_us: 500,
        chunks: MOCK_CHUNKS,
        chunk_bytes: 128,
        sid: 7,
        resp_bytes: 256,
    };
    let listen: SocketAddr = "127.0.0.1:0".parse().expect("valid address");
    ServerHandle::start(MockConfig::new(listen, params)).expect("the mock starts")
}

fn mock_base_url(mock: &ServerHandle) -> String {
    format!("http://{}/v1", mock.local_addr())
}

/// The client body with `INJECTED` inserted before its closing brace.
fn with_injection(body: &[u8]) -> Vec<u8> {
    let end = body.len() - 1;
    assert_eq!(body[end], b'}');
    [&body[..end], INJECTED, &body[end..]].concat()
}

/// The usage-only event the mock sends when asked, recognised by its empty
/// `choices`.
fn has_usage_only_event(body: &[u8]) -> bool {
    contains(body, br#""choices":[],"usage":"#)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inject_channel_hides_the_injected_usage_and_settles_on_it() {
    let mock = start_mock();
    let base_url = mock_base_url(&mock);
    let gateway = start_gateway(|spec| {
        let mut mock_channel = channel("mock", &base_url, StreamUsage::Inject);
        mock_channel.warmup = WarmupTarget {
            method: WarmupMethod::Get,
            path: String::from("/v1/models"),
        };
        spec.channels.push(mock_channel);
    })
    .await;

    let body = chat_body(TEST_MODEL, true, None);
    let response = gateway.chat(body.clone()).await;
    assert_eq!(response.status, 200);
    assert!(response.body.ends_with(DONE_EVENT), "{}", response.text());
    assert!(!has_usage_only_event(&response.body), "{}", response.text());

    let settled = gateway.finish().await;
    let outcome = settled.only();
    assert_eq!(outcome.status, OutcomeStatus::Completed);
    // The mock estimates prompt tokens from the body it received, which
    // proves the injection reached it.
    let upstream_len = with_injection(&body).len() as u64;
    assert_eq!(
        outcome.usage,
        Some(UsageTokens {
            input: upstream_len.div_ceil(4),
            output: u64::from(MOCK_CHUNKS),
            cached_input: None,
            reasoning_output: None,
        })
    );
    assert_eq!(outcome.billed.input, upstream_len.div_ceil(4));
    assert_eq!(outcome.billed.output, u64::from(MOCK_CHUNKS));
    assert_eq!(settled.tally.usage_missing, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn usage_the_client_asked_for_stays_visible() {
    let mock = start_mock();
    let base_url = mock_base_url(&mock);
    let gateway = start_gateway(|spec| {
        let mut mock_channel = channel("mock", &base_url, StreamUsage::Inject);
        mock_channel.warmup = WarmupTarget {
            method: WarmupMethod::Get,
            path: String::from("/v1/models"),
        };
        spec.channels.push(mock_channel);
    })
    .await;

    let body = chat_body(TEST_MODEL, true, Some(true));
    let response = gateway.chat(body.clone()).await;
    assert_eq!(response.status, 200);
    assert!(has_usage_only_event(&response.body), "{}", response.text());

    let settled = gateway.finish().await;
    let outcome = settled.only();
    assert_eq!(outcome.status, OutcomeStatus::Completed);
    let usage = outcome.usage.expect("usage");
    assert_eq!(usage.input, (body.len() as u64).div_ceil(4));
    assert_eq!(usage.output, u64::from(MOCK_CHUNKS));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inject_channel_leaves_non_streaming_requests_byte_identical() {
    let upstream = ScriptedUpstream::start(chat_only(|_, _| Reply::Json {
        head_delay: std::time::Duration::ZERO,
        headers: json_headers(),
        body: Bytes::from_static(
            br#"{"object":"chat.completion","choices":[],"usage":{"prompt_tokens":3,"completion_tokens":2}}"#,
        ),
    }))
    .await;
    let base_url = upstream.base_url();
    let gateway = start_gateway(|spec| {
        spec.channels
            .push(channel("a", &base_url, StreamUsage::Inject));
    })
    .await;

    for include_usage in [None, Some(false)] {
        let body = chat_body(TEST_MODEL, false, include_usage);
        let response = gateway.chat(body.clone()).await;
        assert_eq!(response.status, 200);
        let received = chat_requests(&upstream).pop().expect("a chat request");
        assert_eq!(
            received.body, body,
            "D8: no stream_options on a non-stream request"
        );
    }
    gateway.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn passthrough_channel_without_usage_bills_the_estimate() {
    let upstream = ScriptedUpstream::start(chat_only(|_, _| {
        sse_reply(&[CONTENT_EVENT, FINISH_EVENT, DONE_EVENT])
    }))
    .await;
    let base_url = upstream.base_url();
    let gateway = start_gateway(|spec| {
        spec.channels
            .push(channel("a", &base_url, StreamUsage::Passthrough));
    })
    .await;

    let body = chat_body(TEST_MODEL, true, None);
    let response = gateway.chat(body.clone()).await;
    assert_eq!(response.status, 200);
    let received = chat_requests(&upstream).pop().expect("a chat request");
    assert_eq!(received.body, body, "passthrough never rewrites");

    let settled = gateway.finish().await;
    let outcome = settled.only();
    assert_eq!(outcome.status, OutcomeStatus::Completed);
    assert_eq!(outcome.usage, None);
    assert_eq!(settled.tally.usage_missing, 1);
    let expected = estimate(body.len() as u64, response.body.len() as u64);
    assert_eq!(outcome.estimate, expected);
    assert_eq!(outcome.billed, expected);
}

/// Content-Length of the upstream request equals the bytes sent, and no
/// Transfer-Encoding appears, for identity and rewritten bodies in both E2
/// modes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upstream_requests_carry_an_exact_content_length() {
    for splice in [SpliceMode::Segments, SpliceMode::Concat] {
        let upstream = ScriptedUpstream::start(chat_only(|_, _| {
            sse_reply(&[CONTENT_EVENT, FINISH_EVENT, USAGE_ONLY_EVENT, DONE_EVENT])
        }))
        .await;
        let base_url = upstream.base_url();
        let gateway = start_gateway(|spec| {
            spec.experiments.splice = splice;
            spec.channels
                .push(channel("a", &base_url, StreamUsage::Inject));
        })
        .await;

        // Identity: the client already asked for usage.
        let identity = chat_body(TEST_MODEL, true, Some(true));
        // Rewritten: `include_usage` is injected.
        let injected = chat_body(TEST_MODEL, true, None);
        for (body, expected) in [
            (identity.clone(), identity.to_vec()),
            (injected.clone(), with_injection(&injected)),
        ] {
            let response = gateway.chat(body).await;
            assert_eq!(response.status, 200, "{splice:?}");
            let received = chat_requests(&upstream).pop().expect("a chat request");
            assert_eq!(received.body.as_ref(), expected.as_slice(), "{splice:?}");
            assert_eq!(
                header_values(&received, "content-length"),
                [expected.len().to_string().as_bytes()],
                "{splice:?}"
            );
            assert!(
                header_values(&received, "transfer-encoding").is_empty(),
                "{splice:?}"
            );
        }

        let settled = gateway.finish().await;
        for outcome in &settled.outcomes {
            assert_eq!(
                outcome.usage.map(|usage| (usage.input, usage.output)),
                Some(SCRIPTED_USAGE)
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn model_map_rewrites_only_the_model_value() {
    const UPSTREAM_MODEL: &str = "grok-4.6-xhigh-upstream";
    for splice in [SpliceMode::Segments, SpliceMode::Concat] {
        let upstream = ScriptedUpstream::start(chat_only(|_, _| {
            sse_reply(&[CONTENT_EVENT, FINISH_EVENT, DONE_EVENT])
        }))
        .await;
        let base_url = upstream.base_url();
        let gateway = start_gateway(|spec| {
            spec.experiments.splice = splice;
            let mut mapped = channel("a", &base_url, StreamUsage::Passthrough);
            mapped.models = vec![TEST_MODEL.to_owned()];
            mapped.model_map = vec![(TEST_MODEL.to_owned(), UPSTREAM_MODEL.to_owned())];
            spec.channels.push(mapped);
        })
        .await;

        let body = chat_body(TEST_MODEL, true, None);
        let response = gateway.chat(body.clone()).await;
        assert_eq!(response.status, 200);
        let received = chat_requests(&upstream).pop().expect("a chat request");
        assert_eq!(
            received.body,
            chat_body(UPSTREAM_MODEL, true, None),
            "{splice:?}"
        );
        gateway.finish().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sse_responses_carry_brisk_cache_control() {
    let upstream = ScriptedUpstream::start(chat_only(|_, _| {
        let Reply::Sse {
            head_delay,
            mut headers,
            frames,
            end,
        } = sse_reply(&[CONTENT_EVENT, FINISH_EVENT, DONE_EVENT])
        else {
            unreachable!("sse_reply builds an SSE reply")
        };
        headers.push(("cache-control", String::from("no-store, private")));
        Reply::Sse {
            head_delay,
            headers,
            frames,
            end,
        }
    }))
    .await;
    let base_url = upstream.base_url();
    let gateway = start_gateway(|spec| {
        spec.channels
            .push(channel("a", &base_url, StreamUsage::Passthrough));
    })
    .await;

    let response = gateway.chat(chat_body(TEST_MODEL, true, None)).await;
    assert_eq!(response.status, 200);
    let cache_control: Vec<_> = response.headers.get_all("cache-control").iter().collect();
    assert_eq!(
        cache_control,
        ["no-cache"],
        "Brisk's own value, not the upstream's"
    );
    assert_eq!(response.headers["content-type"], "text/event-stream");
    gateway.finish().await;
}
