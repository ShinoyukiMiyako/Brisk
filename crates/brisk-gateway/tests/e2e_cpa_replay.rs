//! Replays of the sanitised CPA captures (section 4.4) through the gateway:
//! byte-identical bodies under random upstream framing, the response-header
//! allowlist, usage from real CPA shapes, the CPA 401 failover, the CPA 400
//! pass-through and an in-stream CPA error after commit (CPA-M1-5).

// Not built until the scripted upstream (P2-SUPPORT) and `Gateway`
// (P5-GATEWAY) are merged into m1/integration; the integrator removes this
// attribute and the `rustfmt::skip` on `mod scripted` at that checkpoint.
#![cfg(any())]

#[rustfmt::skip]
mod scripted;
mod e2e_support;

use std::time::Duration;

use brisk_gateway::outcome::{OutcomeStatus, estimate};
use brisk_gateway::spec::{ChannelId, StreamUsage};
use brisk_proto::UsageTokens;
use bytes::Bytes;
use e2e_support::{
    Collected, FIRST, TEST_MODEL, channel, chat_body, chat_only, chat_requests,
    collect_until_error, json_headers, sse_headers, start_gateway,
};
use http::HeaderMap;
use scripted::{Reply, ScriptedUpstream, Split, SseEnd, fixture_reply, frames_of};

macro_rules! fixture {
    ($name:literal) => {
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../fixtures/cpa/",
            $name
        ))
        .as_slice()
    };
}

const GROK46_XHIGH_SSE: &[u8] = fixture!("chat-stream-grok46-xhigh.sse");
const GROK46_XHIGH_SSE_HEADERS: &[u8] = fixture!("chat-stream-grok46-xhigh.headers");
const GROK46_SUFFIX_SSE: &[u8] = fixture!("chat-stream-grok46-suffix.sse");
const GROK46_SUFFIX_SSE_HEADERS: &[u8] = fixture!("chat-stream-grok46-suffix.headers");
const GROK43_SSE: &[u8] = fixture!("chat-stream-grok43.sse");
const GPT55_SSE: &[u8] = fixture!("chat-stream-gpt55.sse");
const GPT55_SSE_HEADERS: &[u8] = fixture!("chat-stream-gpt55.headers");
const GROK46_XHIGH_JSON: &[u8] = fixture!("chat-nonstream-grok46-xhigh.json");
const GROK46_XHIGH_JSON_HEADERS: &[u8] = fixture!("chat-nonstream-grok46-xhigh.headers");
const GPT55_JSON: &[u8] = fixture!("chat-nonstream-gpt55.json");
const BAD_KEY_JSON: &[u8] = fixture!("error-bad-key.json");
const BAD_KEY_HEADERS: &[u8] = fixture!("error-bad-key.headers");
const BOGUS_EFFORT_JSON: &[u8] = fixture!("error-bogus-effort.json");
const BOGUS_EFFORT_HEADERS: &[u8] = fixture!("error-bogus-effort.headers");

/// Response headers Brisk may send for a forwarded response: the allowlist
/// (without the rate-limit extension), its own `cache-control`, and what
/// hyper writes for framing and date.
const DOWNSTREAM_ALLOWED: &[&str] = &[
    "content-type",
    "x-request-id",
    "retry-after",
    "cache-control",
    "date",
    "content-length",
    "transfer-encoding",
];

fn random_split(seed: u64) -> Split {
    Split::Random {
        seed,
        min_piece: 1,
        max_piece: 97,
    }
}

/// A fixture without a `.headers` capture, replayed as a CPA stream.
fn sse_without_headers(body: &[u8], split: Split) -> Reply {
    Reply::Sse {
        head_delay: Duration::ZERO,
        headers: sse_headers(),
        frames: frames_of(body, split)
            .into_iter()
            .map(|frame| (Duration::ZERO, frame))
            .collect(),
        end: SseEnd::Finish,
    }
}

fn assert_downstream_headers(headers: &HeaderMap, sse: bool) {
    for name in headers.keys() {
        assert!(
            DOWNSTREAM_ALLOWED.contains(&name.as_str()),
            "header {name} is not on the allowlist"
        );
    }
    assert!(headers.get("x-cpa-trace-id").is_none());
    assert!(headers.get("set-cookie").is_none());
    assert!(headers.get("connection").is_none());
    assert!(
        !headers
            .keys()
            .any(|name| name.as_str().starts_with("access-control-"))
    );
    if sse {
        assert_eq!(headers["content-type"], "text/event-stream");
        assert_eq!(headers["cache-control"], "no-cache");
    } else {
        assert!(headers.get("cache-control").is_none());
        assert!(
            headers["content-type"]
                .to_str()
                .expect("ASCII")
                .starts_with("application/json")
        );
    }
}

const fn usage(input: u64, output: u64, cached: u64, reasoning: u64) -> UsageTokens {
    UsageTokens {
        input,
        output,
        cached_input: Some(cached),
        reasoning_output: Some(reasoning),
    }
}

/// One replay case: what the upstream sends and what the client must see.
struct Case {
    name: &'static str,
    stream: bool,
    body: &'static [u8],
    reply: fn(Split) -> Reply,
    expected: UsageTokens,
}

fn cases() -> Vec<Case> {
    vec![
        Case {
            name: "chat-stream-grok46-xhigh",
            stream: true,
            body: GROK46_XHIGH_SSE,
            reply: |split| fixture_reply(GROK46_XHIGH_SSE_HEADERS, GROK46_XHIGH_SSE, split),
            expected: usage(213, 71, 0, 70),
        },
        Case {
            name: "chat-stream-grok46-suffix",
            stream: true,
            body: GROK46_SUFFIX_SSE,
            reply: |split| fixture_reply(GROK46_SUFFIX_SSE_HEADERS, GROK46_SUFFIX_SSE, split),
            expected: usage(213, 105, 128, 104),
        },
        Case {
            name: "chat-stream-grok43",
            stream: true,
            body: GROK43_SSE,
            reply: |split| sse_without_headers(GROK43_SSE, split),
            expected: usage(197, 216, 0, 215),
        },
        Case {
            name: "chat-stream-gpt55",
            stream: true,
            body: GPT55_SSE,
            reply: |split| fixture_reply(GPT55_SSE_HEADERS, GPT55_SSE, split),
            expected: usage(307, 5, 0, 0),
        },
        Case {
            name: "chat-nonstream-grok46-xhigh",
            stream: false,
            body: GROK46_XHIGH_JSON,
            reply: |split| fixture_reply(GROK46_XHIGH_JSON_HEADERS, GROK46_XHIGH_JSON, split),
            expected: usage(213, 128, 128, 127),
        },
        Case {
            name: "chat-nonstream-gpt55",
            stream: false,
            body: GPT55_JSON,
            reply: |_| Reply::Json {
                head_delay: Duration::ZERO,
                headers: json_headers(),
                body: Bytes::from_static(GPT55_JSON),
            },
            expected: UsageTokens {
                input: 307,
                output: 5,
                cached_input: None,
                reasoning_output: None,
            },
        },
    ]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cpa_captures_pass_through_byte_for_byte() {
    for case in cases() {
        for seed in 1..=4 {
            let reply = case.reply;
            let upstream =
                ScriptedUpstream::start(chat_only(move |_, _| reply(random_split(seed)))).await;
            let base_url = upstream.base_url();
            let gateway = start_gateway(|spec| {
                spec.channels
                    .push(channel("cpa", &base_url, StreamUsage::Passthrough));
            })
            .await;

            let body = chat_body(TEST_MODEL, case.stream, None);
            let response: Collected = gateway.chat(body.clone()).await;
            let label = format!("{} seed {seed}", case.name);
            assert_eq!(response.status, 200, "{label}");
            assert!(response.body == case.body, "{label}: body differs");
            assert_downstream_headers(&response.headers, case.stream);

            let received = chat_requests(&upstream);
            assert_eq!(received.len(), 1, "{label}");
            assert_eq!(
                received[0].body, body,
                "{label}: the model name passes untouched"
            );

            let settled = gateway.finish().await;
            let outcome = settled.only();
            assert_eq!(outcome.status, OutcomeStatus::Completed, "{label}");
            let usage = outcome.usage.expect("usage");
            assert_eq!(usage.input, case.expected.input, "{label}");
            assert_eq!(usage.output, case.expected.output, "{label}");
            if case.expected.cached_input.is_some() {
                assert_eq!(usage, case.expected, "{label}");
            }
            assert_eq!(outcome.billed.input, case.expected.input, "{label}");
            assert_eq!(outcome.billed.output, case.expected.output, "{label}");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cpa_bad_key_fails_over_to_the_next_channel() {
    let bad = ScriptedUpstream::start(chat_only(|_, _| {
        fixture_reply(BAD_KEY_HEADERS, BAD_KEY_JSON, Split::Whole)
    }))
    .await;
    let good = ScriptedUpstream::start(chat_only(|_, _| {
        fixture_reply(GROK46_XHIGH_SSE_HEADERS, GROK46_XHIGH_SSE, Split::PerEvent)
    }))
    .await;
    let (bad_url, good_url) = (bad.base_url(), good.base_url());
    let gateway = start_gateway(|spec| {
        let mut first = channel("cpa-bad", &bad_url, StreamUsage::Passthrough);
        first.weight = FIRST;
        spec.channels.push(first);
        spec.channels
            .push(channel("cpa-good", &good_url, StreamUsage::Passthrough));
    })
    .await;

    let response = gateway.chat(chat_body(TEST_MODEL, true, None)).await;
    assert_eq!(response.status, 200);
    assert!(response.body == GROK46_XHIGH_SSE);
    assert_eq!(chat_requests(&bad).len(), 1);
    assert_eq!(chat_requests(&good).len(), 1);

    let settled = gateway.finish().await;
    let outcome = settled.only();
    assert_eq!(outcome.attempts, 2);
    assert_eq!(outcome.channel, Some(ChannelId(1)));
    assert_eq!(outcome.status, OutcomeStatus::Completed);
    assert_eq!(settled.tally.failovers, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cpa_bad_request_is_forwarded_without_failover() {
    let first = ScriptedUpstream::start(chat_only(|_, _| {
        fixture_reply(BOGUS_EFFORT_HEADERS, BOGUS_EFFORT_JSON, Split::Whole)
    }))
    .await;
    let second = ScriptedUpstream::start(chat_only(|_, _| {
        fixture_reply(GROK46_XHIGH_SSE_HEADERS, GROK46_XHIGH_SSE, Split::Whole)
    }))
    .await;
    let (first_url, second_url) = (first.base_url(), second.base_url());
    let gateway = start_gateway(|spec| {
        let mut preferred = channel("cpa-a", &first_url, StreamUsage::Passthrough);
        preferred.weight = FIRST;
        spec.channels.push(preferred);
        spec.channels
            .push(channel("cpa-b", &second_url, StreamUsage::Passthrough));
    })
    .await;

    let response = gateway.chat(chat_body(TEST_MODEL, true, None)).await;
    assert_eq!(response.status, 400);
    assert!(response.body == BOGUS_EFFORT_JSON);
    assert_downstream_headers(&response.headers, false);
    assert_eq!(chat_requests(&second).len(), 0);

    let settled = gateway.finish().await;
    let outcome = settled.only();
    assert_eq!(
        outcome.status,
        OutcomeStatus::ForwardedError { status: 400 }
    );
    assert_eq!(outcome.attempts, 1);
    assert_eq!(outcome.billed, UsageTokens::default());
}

/// Events of an SSE body, each including its terminating blank line.
fn events(body: &[u8]) -> Vec<&[u8]> {
    let mut events = Vec::new();
    let mut start = 0;
    while let Some(offset) = body[start..].windows(2).position(|pair| pair == b"\n\n") {
        let end = start + offset + 2;
        events.push(&body[start..end]);
        start = end;
    }
    assert_eq!(start, body.len(), "the fixture ends on an event boundary");
    events
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cpa_error_after_commit_truncates_without_failover() {
    let events = events(GROK46_XHIGH_SSE);
    let mut sent = events[..events.len() / 2].concat();
    sent.extend_from_slice(b"data: {\"error\":{\"message\":\"x\",\"type\":\"server_error\"}}\n\n");
    let sent = Bytes::from(sent);

    let reply_body = sent.clone();
    let first = ScriptedUpstream::start(chat_only(move |_, _| Reply::Sse {
        head_delay: Duration::ZERO,
        headers: sse_headers(),
        frames: frames_of(&reply_body, Split::PerEvent)
            .into_iter()
            .map(|frame| (Duration::ZERO, frame))
            .collect(),
        end: SseEnd::Close,
    }))
    .await;
    let second = ScriptedUpstream::start(chat_only(|_, _| {
        fixture_reply(GROK46_XHIGH_SSE_HEADERS, GROK46_XHIGH_SSE, Split::Whole)
    }))
    .await;
    let (first_url, second_url) = (first.base_url(), second.base_url());
    let gateway = start_gateway(|spec| {
        let mut preferred = channel("cpa-a", &first_url, StreamUsage::Passthrough);
        preferred.weight = FIRST;
        spec.channels.push(preferred);
        spec.channels
            .push(channel("cpa-b", &second_url, StreamUsage::Passthrough));
    })
    .await;

    let body = chat_body(TEST_MODEL, true, None);
    let mut client = gateway.h1().await;
    let response = client.send(gateway.chat_with(body.clone())).await;
    assert_eq!(response.status(), 200);
    let (received, clean) = collect_until_error(response).await;
    assert!(!clean, "a truncated stream must not end cleanly");
    assert!(received == sent, "the client sees exactly what CPA sent");
    assert_eq!(chat_requests(&second).len(), 0);
    drop(client);

    let settled = gateway.finish().await;
    let outcome = settled.only();
    assert_eq!(outcome.status, OutcomeStatus::UpstreamTruncated);
    assert_eq!(outcome.usage, None);
    let expected = estimate(body.len() as u64, sent.len() as u64);
    assert_eq!(outcome.billed, expected);
}
