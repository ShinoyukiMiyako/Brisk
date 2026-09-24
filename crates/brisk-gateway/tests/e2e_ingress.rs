//! The inbound side end to end: virtual-key authentication and its ambiguity
//! rules, credential query removal (D10), the request-header allowlist,
//! compressed bodies (D29), body limits, head parsing errors, model routing
//! and `include_usage` injection into a `null` `stream_options`.

mod e2e_support;
mod scripted;

use std::time::Duration;

use brisk_gateway::auth::{KEY_RANDOM_BYTES, format_key};
use brisk_gateway::outcome::{OutcomeStatus, RejectReason};
use brisk_gateway::spec::{GatewaySpec, StreamUsage};
use bytes::Bytes;
use e2e_support::{
    CONTENT_EVENT, DONE_EVENT, FINISH_EVENT, TEST_MODEL, TestGateway, channel, chat_body,
    chat_only, chat_requests, collect, full, header_values, sse_reply, start_gateway, streamed,
};
use http::Request;
use http::header::{CONTENT_LENGTH, CONTENT_TYPE};
use scripted::ScriptedUpstream;

async fn upstream() -> ScriptedUpstream {
    ScriptedUpstream::start(chat_only(|_, _| {
        sse_reply(&[CONTENT_EVENT, FINISH_EVENT, DONE_EVENT])
    }))
    .await
}

async fn gateway_to(
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

/// A chat request without any credential header.
fn anonymous() -> http::request::Builder {
    Request::post("/v1/chat/completions").header(CONTENT_TYPE, "application/json")
}

fn other_key() -> String {
    format_key(&[7_u8; KEY_RANDOM_BYTES]).into_inner()
}

/// Credential headers of one request, in the order they are sent.
type Credentials = Vec<(&'static str, String)>;

/// Sends a chat request carrying `credentials` and returns its status and
/// `error.code`, if any.
async fn send_with(gateway: &TestGateway, credentials: &Credentials) -> (u16, Option<String>) {
    let mut request = anonymous();
    for (header, value) in credentials {
        request = request.header(*header, value);
    }
    let body = full(chat_body(TEST_MODEL, true, None));
    let response = gateway
        .h1()
        .await
        .send_collect(request.body(body).expect("valid request"))
        .await;
    let code = (response.status != 200).then(|| response.error_code());
    (response.status.as_u16(), code)
}

async fn statuses(gateway: TestGateway) -> Vec<OutcomeStatus> {
    let settled = gateway.finish().await;
    settled
        .outcomes
        .iter()
        .map(|outcome| outcome.status)
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_malformed_unknown_and_ambiguous_keys_get_401() {
    let upstream = upstream().await;
    let gateway = gateway_to(&upstream, |_| {}).await;
    let key = gateway.key.clone();
    let bearer = format!("Bearer {key}");

    let rejected: Vec<(&str, Credentials, RejectReason)> = vec![
        ("missing", vec![], RejectReason::MissingKey),
        (
            "malformed",
            vec![("authorization", String::from("Bearer bk-short"))],
            RejectReason::InvalidKey,
        ),
        (
            "unknown",
            vec![("authorization", format!("Bearer {}", other_key()))],
            RejectReason::InvalidKey,
        ),
        (
            "basic scheme",
            vec![("authorization", format!("Basic {key}"))],
            RejectReason::InvalidKey,
        ),
        (
            "two spaces",
            vec![("authorization", format!("Bearer  {key}"))],
            RejectReason::InvalidKey,
        ),
        // Trailing whitespace never reaches the gateway: the HTTP/1.1 parser
        // strips it from field values (RFC 9112, section 5) and HTTP/2
        // forbids it, so the `auth` unit tests cover it. Trailing bytes
        // that are not whitespace do arrive.
        (
            "trailing bytes",
            vec![("authorization", format!("Bearer {key}x"))],
            RejectReason::InvalidKey,
        ),
        (
            "repeated header",
            vec![("x-api-key", key.clone()), ("x-api-key", key.clone())],
            RejectReason::InvalidKey,
        ),
        (
            "two different keys",
            vec![
                ("authorization", bearer.clone()),
                ("x-api-key", other_key()),
            ],
            RejectReason::InvalidKey,
        ),
        (
            "basic plus x-api-key",
            vec![
                ("authorization", format!("Basic {key}")),
                ("x-api-key", key.clone()),
            ],
            RejectReason::InvalidKey,
        ),
    ];

    let mut expected = Vec::new();
    for (name, credentials, reason) in &rejected {
        let (status, code) = send_with(&gateway, credentials).await;
        assert_eq!(status, 401, "{name}");
        assert_eq!(code.as_deref(), Some("invalid_api_key"), "{name}");
        expected.push(OutcomeStatus::Rejected(*reason));
    }
    assert_eq!(chat_requests(&upstream).len(), 0);
    assert_eq!(statuses(gateway).await, expected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_credential_header_authenticates() {
    let upstream = upstream().await;
    let gateway = gateway_to(&upstream, |_| {}).await;
    let key = gateway.key.clone();
    let bearer = format!("Bearer {key}");

    let accepted: Vec<(&str, Credentials)> = vec![
        ("authorization", vec![("authorization", bearer.clone())]),
        (
            "lowercase bearer",
            vec![("authorization", format!("bearer {key}"))],
        ),
        ("x-api-key", vec![("x-api-key", key.clone())]),
        ("x-goog-api-key", vec![("x-goog-api-key", key.clone())]),
        (
            "same key twice",
            vec![
                ("authorization", bearer.clone()),
                ("x-goog-api-key", key.clone()),
            ],
        ),
    ];
    for (name, credentials) in &accepted {
        let (status, _) = send_with(&gateway, credentials).await;
        assert_eq!(status, 200, "{name}");
    }
    assert_eq!(chat_requests(&upstream).len(), accepted.len());
    assert_eq!(
        statuses(gateway).await,
        vec![OutcomeStatus::Completed; accepted.len()]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn credential_query_parameters_never_reach_the_upstream() {
    let upstream = upstream().await;
    let gateway = gateway_to(&upstream, |_| {}).await;
    let body = chat_body(TEST_MODEL, true, None);
    let query_key = gateway.key.clone();
    let uri = format!("/v1/chat/completions?key={query_key}&auth_token=secret-token-123&x=1");
    let request = gateway
        .chat_request()
        .uri(uri)
        .body(full(body))
        .expect("valid request");
    let response = gateway.h1().await.send_collect(request).await;
    assert_eq!(response.status, 200);

    let received = chat_requests(&upstream);
    assert_eq!(received.len(), 1);
    assert_eq!(
        received[0].target, "/v1/chat/completions",
        "D10: no query at all"
    );

    let settled = gateway.finish().await;
    let outcome = format!("{:?}", settled.only());
    assert!(!outcome.contains(&query_key));
    assert!(!outcome.contains("secret-token-123"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn only_allowlisted_request_headers_are_forwarded() {
    let upstream = upstream().await;
    let gateway = gateway_to(&upstream, |_| {}).await;
    let request = gateway
        .chat_request()
        .header("user-agent", "OpenAI/Python 2.1.0")
        .header("idempotency-key", "idem-1")
        .header("session_id", "sess-1")
        .header("x-claude-code-session-id", "claude-sess-1")
        .header("x-stainless-os", "Linux")
        .header("x-stainless-lang", "python")
        .header("connection", "keep-alive, x-stainless-lang")
        .header("cookie", "a=b")
        .header("accept-encoding", "gzip, zstd")
        .header("x-forwarded-for", "203.0.113.9")
        .header("forwarded", "for=203.0.113.9")
        .header("openai-organization", "org-1")
        .body(full(chat_body(TEST_MODEL, true, None)))
        .expect("valid request");
    let response = gateway.h1().await.send_collect(request).await;
    assert_eq!(response.status, 200);

    let received = chat_requests(&upstream).pop().expect("a chat request");
    for (name, value) in [
        ("user-agent", &b"OpenAI/Python 2.1.0"[..]),
        ("idempotency-key", b"idem-1"),
        ("session_id", b"sess-1"),
        ("x-claude-code-session-id", b"claude-sess-1"),
        ("x-stainless-os", b"Linux"),
        ("authorization", b"Bearer upstream-key-a"),
        ("content-type", b"application/json"),
    ] {
        assert_eq!(header_values(&received, name), [value], "{name}");
    }
    for name in [
        "x-stainless-lang",
        "cookie",
        "accept-encoding",
        "x-forwarded-for",
        "forwarded",
        "openai-organization",
        "x-api-key",
    ] {
        assert!(header_values(&received, name).is_empty(), "{name} leaked");
    }
    let client_key = gateway.key.clone();
    assert!(
        received.headers.iter().all(|(_, value)| !value
            .windows(client_key.len())
            .any(|w| w == client_key.as_bytes())),
        "the client's key never reaches the upstream"
    );
    gateway.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compressed_request_bodies_are_rejected_unread() {
    let upstream = upstream().await;
    let gateway = gateway_to(&upstream, |_| {}).await;
    let request = gateway
        .chat_request()
        .header("content-encoding", "zstd")
        .body(full(Bytes::from_static(b"\x28\xb5\x2f\xfd")))
        .expect("valid request");
    let response = gateway.h1().await.send_collect(request).await;
    assert_eq!(response.status, 415);
    assert_eq!(response.error_code(), "unsupported_content_encoding");
    assert_eq!(chat_requests(&upstream).len(), 0);

    let settled = gateway.finish().await;
    assert_eq!(
        settled.only().status,
        OutcomeStatus::Rejected(RejectReason::UnsupportedEncoding)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oversized_bodies_get_413_with_and_without_content_length() {
    let upstream = upstream().await;
    let gateway = gateway_to(&upstream, |spec| {
        spec.limits.max_body = 1024;
        spec.limits.inflight_body_budget = 64 << 10;
    })
    .await;
    let padding = "x".repeat(2048);
    let body = Bytes::from(format!(
        r#"{{"model":"{TEST_MODEL}","messages":[{{"role":"user","content":"{padding}"}}]}}"#
    ));

    let response = gateway.chat(body.clone()).await;
    assert_eq!(response.status, 413);
    assert_eq!(response.error_code(), "request_too_large");

    let (mut sender, streamed_body) = streamed();
    let request = gateway
        .chat_request()
        .body(streamed_body)
        .expect("valid request");
    let mut client = gateway.h1().await;
    let (response, ()) = tokio::join!(client.send(request), async {
        for piece in body.chunks(256) {
            // The gateway may stop reading once the limit is crossed.
            if sender
                .send_data(Bytes::copy_from_slice(piece))
                .await
                .is_err()
            {
                break;
            }
        }
        drop(sender);
    });
    let response = collect(response).await;
    assert_eq!(response.status, 413);
    assert_eq!(chat_requests(&upstream).len(), 0);

    let settled = gateway.finish().await;
    for outcome in &settled.outcomes {
        assert_eq!(
            outcome.status,
            OutcomeStatus::Rejected(RejectReason::BodyTooLarge)
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stalled_body_gets_408() {
    let upstream = upstream().await;
    let gateway = gateway_to(&upstream, |spec| {
        spec.limits.body_min_rate_window = Duration::from_millis(200);
        spec.limits.body_read_timeout = Duration::from_secs(2);
    })
    .await;
    let (mut sender, streamed_body) = streamed();
    let request = gateway
        .chat_request()
        .body(streamed_body)
        .expect("valid request");
    let mut client = gateway.h1().await;
    sender
        .send_data(Bytes::from(vec![b' '; 1024]))
        .await
        .expect("the gateway reads the first kilobyte");
    let response = collect(client.send(request).await).await;
    assert_eq!(response.status, 408);
    assert_eq!(response.error_code(), "request_timeout");
    drop(sender);

    let settled = gateway.finish().await;
    assert_eq!(
        settled.only().status,
        OutcomeStatus::Rejected(RejectReason::BodyTooSlow)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_exhausted_body_budget_gets_503() {
    const LEN: usize = 40 << 10;
    let upstream = upstream().await;
    let gateway = gateway_to(&upstream, |spec| {
        spec.limits.max_body = 64 << 10;
        spec.limits.inflight_body_budget = 64 << 10;
    })
    .await;

    // The first request reserves its Content-Length and then stalls.
    let (mut sender, streamed_body) = streamed();
    let holding = gateway
        .chat_request()
        .header(CONTENT_LENGTH, LEN)
        .body(streamed_body)
        .expect("valid request");
    let mut holder = gateway.h1().await;
    let held = tokio::spawn(async move {
        let response = holder.try_send(holding).await;
        drop(holder);
        response.map(|response| response.status())
    });
    sender
        .send_data(Bytes::from(vec![b' '; 1024]))
        .await
        .expect("the gateway reads the first kilobyte");
    // Give the gateway time to take the reservation.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let response = gateway.chat(Bytes::from(vec![b' '; LEN])).await;
    assert_eq!(response.status, 503);
    assert_eq!(response.error_code(), "overloaded");

    // Release the first request: the rest of its body is not JSON.
    let rest = LEN - 1024;
    sender
        .send_data(Bytes::from(vec![b' '; rest]))
        .await
        .expect("the gateway reads the rest");
    drop(sender);
    let status = held.await.expect("task").expect("answered");
    assert_eq!(status, 400);

    let settled = gateway.finish().await;
    let statuses: Vec<_> = settled
        .outcomes
        .iter()
        .map(|outcome| outcome.status)
        .collect();
    assert!(statuses.contains(&OutcomeStatus::Rejected(RejectReason::Overloaded)));
    assert!(statuses.contains(&OutcomeStatus::Rejected(RejectReason::BadRequest)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unusable_heads_get_400() {
    let upstream = upstream().await;
    let gateway = gateway_to(&upstream, |_| {}).await;
    let bodies: [&[u8]; 6] = [
        br#"{"model":"grok-4.6(xhigh)","model":"other","messages":[]}"#,
        // The same key spelled with an escape (`e` is `e`).
        br#"{"model":"grok-4.6(xhigh)","model":"other","messages":[]}"#,
        br#"{"Model":"expensive","model":"grok-4.6(xhigh)","messages":[]}"#,
        br#"{"messages":[{"role":"user","content":"x","model":"grok-4.6(xhigh)"}]}"#,
        br#"{"model":null,"messages":[]}"#,
        br#"["grok-4.6(xhigh)",true]"#,
    ];
    for body in bodies {
        let response = gateway.chat(Bytes::from_static(body)).await;
        let text = String::from_utf8_lossy(body);
        assert_eq!(response.status, 400, "{text}");
        assert_eq!(response.error_code(), "invalid_json", "{text}");
    }
    assert_eq!(chat_requests(&upstream).len(), 0);

    let settled = gateway.finish().await;
    assert_eq!(settled.tally.rejected, bodies.len() as u64);
    for outcome in &settled.outcomes {
        assert_eq!(
            outcome.status,
            OutcomeStatus::Rejected(RejectReason::BadRequest)
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unserved_model_gets_404() {
    let upstream = upstream().await;
    let base_url = upstream.base_url();
    let gateway = start_gateway(|spec| {
        let mut listed = channel("a", &base_url, StreamUsage::Passthrough);
        listed.models = vec![TEST_MODEL.to_owned()];
        spec.channels.push(listed);
    })
    .await;
    for model in ["grok-4.6", "grok-4.6(XHIGH)", "gpt-5.5"] {
        let response = gateway.chat(chat_body(model, true, None)).await;
        assert_eq!(response.status, 404, "{model}");
        assert_eq!(response.error_code(), "model_not_found", "{model}");
    }
    let response = gateway.chat(chat_body(TEST_MODEL, true, None)).await;
    assert_eq!(response.status, 200);

    let settled = gateway.finish().await;
    let rejected = settled
        .outcomes
        .iter()
        .filter(|outcome| outcome.status == OutcomeStatus::Rejected(RejectReason::ModelNotFound))
        .count();
    assert_eq!(rejected, 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_null_stream_options_receives_the_injected_object() {
    let upstream = upstream().await;
    let base_url = upstream.base_url();
    let gateway = start_gateway(|spec| {
        spec.channels
            .push(channel("a", &base_url, StreamUsage::Inject));
    })
    .await;
    let body =
        format!(r#"{{"model":"{TEST_MODEL}","stream":true,"stream_options":null,"messages":[]}}"#);
    let response = gateway.chat(body).await;
    assert_eq!(response.status, 200);
    let received = chat_requests(&upstream).pop().expect("a chat request");
    assert_eq!(
        received.body,
        format!(
            r#"{{"model":"{TEST_MODEL}","stream":true,"stream_options":{{"include_usage":true}},"messages":[]}}"#
        )
    );
    gateway.finish().await;
}
