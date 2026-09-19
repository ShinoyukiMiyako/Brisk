//! Every row of section 2.3 of the M1 contract: failover before commit, the
//! last-attempt replies (D3), the first-event error (D22), reclaiming small
//! abandoned responses (D26), no failover after commit, and `max_attempts`.

// Not built until the scripted upstream (P2-SUPPORT) and `Gateway`
// (P5-GATEWAY) are merged into m1/integration; the integrator removes this
// attribute and the `rustfmt::skip` on `mod scripted` at that checkpoint.
#![cfg(any())]

#[rustfmt::skip]
mod scripted;
mod e2e_support;

use std::time::Duration;

use brisk_gateway::outcome::{FailureClass, OutcomeStatus};
use brisk_gateway::spec::{ChannelId, ChannelSpec, StreamUsage};
use brisk_proto::UsageTokens;
use bytes::Bytes;
use e2e_support::{
    CONTENT_EVENT, DONE_EVENT, FINISH_USAGE_EVENT, FIRST, TEST_MODEL, channel, chat_body,
    chat_only, chat_requests, collect_until_error, contains, refused_base_url, sse_headers, sse_ok,
    sse_reply, start_gateway, status_reply, stream_ok,
};
use scripted::{Reply, ScriptedUpstream, SseEnd};

/// First-byte deadline of channels that are expected to time out.
const SHORT_FIRST_BYTE: Duration = Duration::from_millis(300);

const FIRST_EVENT_ERROR_OBJECT: &[u8] =
    b"data: {\"error\":{\"message\":\"quota\",\"type\":\"server_error\"}}\n\n";
const FIRST_EVENT_ERROR_STRING: &[u8] = b"data: {\"error\":\"Invalid API key\"}\n\n";
const UPSTREAM_ERROR_BODY: &[u8] = br#"{"error":{"message":"upstream says no","type":"x"}}"#;

/// How the first channel misbehaves.
#[derive(Debug, Clone, Copy)]
enum Failure {
    /// Nothing listens on its port.
    Refused,
    /// The connection is reset after the request.
    Reset,
    /// 307 to a third server.
    Redirect,
    /// A plain status with a JSON body.
    Status(u16),
    /// Never answers.
    Hang,
    /// 200 SSE whose first data event is an `error` object.
    FirstEventErrorObject,
    /// 200 SSE whose first data event is an `error` string (CPA 401 shape).
    FirstEventErrorString,
    /// 200 SSE that ends before any data event.
    EmptyStream,
    /// 200 SSE whose first event exceeds 1 MiB (D17).
    OversizedEvent,
}

const FAILURES: &[Failure] = &[
    Failure::Refused,
    Failure::Reset,
    Failure::Redirect,
    Failure::Status(401),
    Failure::Status(403),
    Failure::Status(408),
    Failure::Status(429),
    Failure::Status(500),
    Failure::Status(502),
    Failure::Status(503),
    Failure::Hang,
    Failure::FirstEventErrorObject,
    Failure::FirstEventErrorString,
    Failure::EmptyStream,
    Failure::OversizedEvent,
];

fn oversized_event() -> Bytes {
    let mut event = b"data: \"".to_vec();
    event.resize(event.len() + (1 << 20) + 1024, b'a');
    Bytes::from(event)
}

/// The failing channel's upstream and base URL; `third` receives redirects.
struct FailingUpstream {
    upstream: Option<ScriptedUpstream>,
    third: Option<ScriptedUpstream>,
    base_url: String,
}

impl FailingUpstream {
    async fn start(failure: Failure) -> Self {
        let third = match failure {
            Failure::Redirect => Some(ScriptedUpstream::start(chat_only(|_, _| sse_ok())).await),
            _ => None,
        };
        let location = third
            .as_ref()
            .map(|third| format!("{}/chat/completions", third.base_url()));
        let script = move |_: usize, _: &scripted::RecordedRequest| match failure {
            Failure::Refused => unreachable!("nothing listens"),
            Failure::Reset => Reply::Reset,
            Failure::Redirect => Reply::Redirect {
                status: 307,
                location: location.clone().expect("a third server"),
            },
            Failure::Status(429) => Reply::Status {
                status: 429,
                headers: vec![
                    ("content-type", String::from("application/json")),
                    ("retry-after", String::from("7")),
                ],
                body: Bytes::from_static(UPSTREAM_ERROR_BODY),
            },
            Failure::Status(status) => status_reply(status, UPSTREAM_ERROR_BODY),
            Failure::Hang => Reply::Hang,
            Failure::FirstEventErrorObject => sse_reply(&[FIRST_EVENT_ERROR_OBJECT, DONE_EVENT]),
            Failure::FirstEventErrorString => sse_reply(&[FIRST_EVENT_ERROR_STRING, DONE_EVENT]),
            Failure::EmptyStream => sse_reply(&[]),
            Failure::OversizedEvent => Reply::Sse {
                head_delay: Duration::ZERO,
                headers: sse_headers(),
                frames: vec![(Duration::ZERO, oversized_event())],
                end: SseEnd::Hang,
            },
        };
        match failure {
            Failure::Refused => Self {
                upstream: None,
                third,
                base_url: refused_base_url(),
            },
            _ => {
                let upstream = ScriptedUpstream::start(chat_only(script)).await;
                let base_url = upstream.base_url();
                Self {
                    upstream: Some(upstream),
                    third,
                    base_url,
                }
            }
        }
    }

    fn channel(&self, failure: Failure) -> ChannelSpec {
        let mut spec = channel("failing", &self.base_url, StreamUsage::Passthrough);
        if matches!(failure, Failure::Hang) {
            spec.timeouts.first_byte = SHORT_FIRST_BYTE;
        }
        spec
    }

    fn chat_count(&self) -> usize {
        self.upstream
            .as_ref()
            .map_or(0, |up| chat_requests(up).len())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_pre_commit_failure_fails_over_to_the_next_channel() {
    for &failure in FAILURES {
        let failing = FailingUpstream::start(failure).await;
        let healthy = ScriptedUpstream::start(chat_only(|_, _| sse_ok())).await;
        let healthy_url = healthy.base_url();
        let first = {
            let mut spec = failing.channel(failure);
            spec.weight = FIRST;
            spec
        };
        let gateway = start_gateway(|spec| {
            spec.channels.push(first);
            spec.channels
                .push(channel("healthy", &healthy_url, StreamUsage::Passthrough));
        })
        .await;

        let response = gateway.chat(chat_body(TEST_MODEL, true, None)).await;
        assert_eq!(response.status, 200, "{failure:?}");
        assert!(response.body == stream_ok(), "{failure:?}");
        let expected_first = usize::from(!matches!(failure, Failure::Refused));
        assert_eq!(failing.chat_count(), expected_first, "{failure:?}");
        assert_eq!(chat_requests(&healthy).len(), 1, "{failure:?}");
        if let Some(third) = &failing.third {
            assert_eq!(third.requests().len(), 0, "redirects are never followed");
        }

        let settled = gateway.finish().await;
        let outcome = settled.only();
        assert_eq!(outcome.status, OutcomeStatus::Completed, "{failure:?}");
        assert_eq!(outcome.attempts, 2, "{failure:?}");
        assert_eq!(outcome.channel, Some(ChannelId(1)), "{failure:?}");
    }
}

/// What the client sees when `failure` happens on the only channel.
enum LastAttempt {
    /// Brisk answers with its own error.
    Generated {
        status: u16,
        code: &'static str,
        class: FailureClass,
    },
    /// The upstream response is forwarded as-is.
    Forwarded { status: u16 },
}

fn last_attempt(failure: Failure) -> LastAttempt {
    use LastAttempt::{Forwarded, Generated};
    match failure {
        Failure::Refused => Generated {
            status: 502,
            code: "upstream_unreachable",
            class: FailureClass::Connect,
        },
        Failure::Reset => Generated {
            status: 502,
            code: "upstream_unreachable",
            class: FailureClass::Transport,
        },
        Failure::Redirect => Generated {
            status: 502,
            code: "upstream_redirect",
            class: FailureClass::Redirect,
        },
        Failure::Status(401 | 403) => Generated {
            status: 502,
            code: "upstream_auth_failed",
            class: FailureClass::UpstreamAuth,
        },
        Failure::Status(408) => Generated {
            status: 504,
            code: "upstream_timeout",
            class: FailureClass::Status,
        },
        Failure::Status(status) => Forwarded { status },
        Failure::Hang => Generated {
            status: 504,
            code: "upstream_timeout",
            class: FailureClass::FirstByteTimeout,
        },
        Failure::FirstEventErrorObject | Failure::FirstEventErrorString => {
            Forwarded { status: 200 }
        }
        Failure::EmptyStream => Generated {
            status: 502,
            code: "upstream_empty_response",
            class: FailureClass::EmptyStream,
        },
        Failure::OversizedEvent => Generated {
            status: 502,
            code: "upstream_protocol_error",
            class: FailureClass::Protocol,
        },
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_last_attempt_answers_per_section_2_3() {
    for &failure in FAILURES {
        let failing = FailingUpstream::start(failure).await;
        let only = failing.channel(failure);
        let base_url = failing.base_url.clone();
        let gateway = start_gateway(|spec| spec.channels.push(only)).await;

        let response = gateway.chat(chat_body(TEST_MODEL, true, None)).await;
        let settled_status = match last_attempt(failure) {
            LastAttempt::Generated {
                status,
                code,
                class,
            } => {
                assert_eq!(response.status, status, "{failure:?}");
                assert_eq!(response.error_code(), code, "{failure:?}");
                // D23: nothing about the channel reaches the client.
                let port = base_url
                    .rsplit(':')
                    .next()
                    .and_then(|rest| rest.split('/').next())
                    .expect("a port");
                for secret in ["127.0.0.1", port, "failing", "upstream-key"] {
                    assert!(
                        !contains(&response.body, secret.as_bytes()),
                        "{failure:?}: body mentions {secret}: {}",
                        response.text()
                    );
                }
                OutcomeStatus::Failed(class)
            }
            LastAttempt::Forwarded { status } => {
                assert_eq!(response.status, status, "{failure:?}");
                if status == 429 {
                    assert_eq!(response.headers["retry-after"], "7");
                }
                if status == 200 {
                    let expected: &[u8] = match failure {
                        Failure::FirstEventErrorObject => FIRST_EVENT_ERROR_OBJECT,
                        _ => FIRST_EVENT_ERROR_STRING,
                    };
                    assert!(response.body == [expected, DONE_EVENT].concat());
                } else {
                    assert!(response.body == UPSTREAM_ERROR_BODY, "{failure:?}");
                }
                OutcomeStatus::ForwardedError { status }
            }
        };

        let settled = gateway.finish().await;
        let outcome = settled.only();
        assert_eq!(outcome.status, settled_status, "{failure:?}");
        assert_eq!(outcome.attempts, 1, "{failure:?}");
        assert_eq!(outcome.billed, UsageTokens::default(), "{failure:?}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_keep_alive_comment_before_the_first_data_event_still_commits() {
    let first = ScriptedUpstream::start(chat_only(|_, _| {
        sse_reply(&[
            b": keep-alive\n\n",
            CONTENT_EVENT,
            FINISH_USAGE_EVENT,
            DONE_EVENT,
        ])
    }))
    .await;
    let second = ScriptedUpstream::start(chat_only(|_, _| sse_ok())).await;
    let (first_url, second_url) = (first.base_url(), second.base_url());
    let gateway = start_gateway(|spec| {
        let mut preferred = channel("a", &first_url, StreamUsage::Passthrough);
        preferred.weight = FIRST;
        spec.channels.push(preferred);
        spec.channels
            .push(channel("b", &second_url, StreamUsage::Passthrough));
    })
    .await;

    let response = gateway.chat(chat_body(TEST_MODEL, true, None)).await;
    assert_eq!(response.status, 200);
    assert!(response.body.starts_with(b": keep-alive\n\n"));
    assert_eq!(chat_requests(&second).len(), 0);
    let settled = gateway.finish().await;
    assert_eq!(settled.only().attempts, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn small_abandoned_responses_are_read_so_the_connection_is_reused() {
    let limited = ScriptedUpstream::start(chat_only(|_, _| Reply::Status {
        status: 429,
        headers: vec![
            ("content-type", String::from("application/json")),
            ("retry-after", String::from("1")),
        ],
        body: Bytes::from_static(UPSTREAM_ERROR_BODY),
    }))
    .await;
    let healthy = ScriptedUpstream::start(chat_only(|_, _| sse_ok())).await;
    let (limited_url, healthy_url) = (limited.base_url(), healthy.base_url());
    let gateway = start_gateway(|spec| {
        let mut preferred = channel("limited", &limited_url, StreamUsage::Passthrough);
        preferred.weight = FIRST;
        spec.channels.push(preferred);
        spec.channels
            .push(channel("healthy", &healthy_url, StreamUsage::Passthrough));
    })
    .await;

    for round in 0..10 {
        let response = gateway.chat(chat_body(TEST_MODEL, true, None)).await;
        assert_eq!(response.status, 200, "round {round}");
    }
    assert_eq!(chat_requests(&limited).len(), 10);
    assert_eq!(
        limited.accepts(),
        1,
        "D26: the 429 connection returns to the pool"
    );

    let settled = gateway.finish().await;
    assert_eq!(settled.tally.completed, 10);
    assert_eq!(settled.tally.failovers, 10);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failure_after_commit_never_fails_over() {
    let first = ScriptedUpstream::start(chat_only(|_, _| Reply::Sse {
        head_delay: Duration::ZERO,
        headers: sse_headers(),
        frames: vec![(Duration::ZERO, Bytes::from_static(CONTENT_EVENT))],
        end: SseEnd::Close,
    }))
    .await;
    let second = ScriptedUpstream::start(chat_only(|_, _| sse_ok())).await;
    let (first_url, second_url) = (first.base_url(), second.base_url());
    let gateway = start_gateway(|spec| {
        let mut preferred = channel("a", &first_url, StreamUsage::Passthrough);
        preferred.weight = FIRST;
        spec.channels.push(preferred);
        spec.channels
            .push(channel("b", &second_url, StreamUsage::Passthrough));
    })
    .await;

    let mut client = gateway.h1().await;
    let response = client
        .send(gateway.chat_with(chat_body(TEST_MODEL, true, None)))
        .await;
    assert_eq!(response.status(), 200);
    let (received, clean) = collect_until_error(response).await;
    assert!(!clean);
    assert!(received == CONTENT_EVENT);
    assert_eq!(chat_requests(&second).len(), 0);
    drop(client);

    let settled = gateway.finish().await;
    let outcome = settled.only();
    assert_eq!(outcome.status, OutcomeStatus::UpstreamTruncated);
    assert_eq!(outcome.attempts, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_attempt_means_no_failover() {
    let first =
        ScriptedUpstream::start(chat_only(|_, _| status_reply(503, UPSTREAM_ERROR_BODY))).await;
    let second = ScriptedUpstream::start(chat_only(|_, _| sse_ok())).await;
    let (first_url, second_url) = (first.base_url(), second.base_url());
    let gateway = start_gateway(|spec| {
        spec.forwarding.max_attempts = 1;
        let mut preferred = channel("a", &first_url, StreamUsage::Passthrough);
        preferred.weight = FIRST;
        spec.channels.push(preferred);
        spec.channels
            .push(channel("b", &second_url, StreamUsage::Passthrough));
    })
    .await;

    let response = gateway.chat(chat_body(TEST_MODEL, true, None)).await;
    assert_eq!(response.status, 503);
    assert!(response.body == UPSTREAM_ERROR_BODY);
    assert_eq!(chat_requests(&first).len(), 1);
    assert_eq!(chat_requests(&second).len(), 0);

    let settled = gateway.finish().await;
    let outcome = settled.only();
    assert_eq!(
        outcome.status,
        OutcomeStatus::ForwardedError { status: 503 }
    );
    assert_eq!(outcome.attempts, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upstream_sees_one_request_per_attempt() {
    // 02 M1 exit condition: reqwest must not retry a 503 or a reset itself.
    for failure in [Failure::Status(503), Failure::Reset] {
        let failing = FailingUpstream::start(failure).await;
        let only = failing.channel(failure);
        let gateway = start_gateway(|spec| spec.channels.push(only)).await;
        let _ = gateway.chat(chat_body(TEST_MODEL, false, None)).await;
        assert_eq!(failing.chat_count(), 1, "{failure:?}");
        gateway.finish().await;
    }
}
