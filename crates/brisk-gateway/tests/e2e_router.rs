//! Experiment E1 parity: the axum router and the static match answer the
//! same requests with the same status and body, including `/healthz`,
//! `/readyz` before and after warm-up, unknown paths, wrong methods and a
//! Gemini-style path whose `key` query parameter must never be logged.

mod e2e_support;
mod scripted;

use std::time::Duration;

use brisk_gateway::outcome::{OutcomeStatus, RejectReason};
use brisk_gateway::spec::{RouterKind, StreamUsage};
use bytes::Bytes;
use e2e_support::{
    TEST_MODEL, TestGateway, channel, chat_body, full, is_chat, json_headers, sse_ok,
    start_gateway_unready, stream_ok,
};
use http::{Method, Request, StatusCode};
use scripted::{Reply, ScriptedUpstream};

/// Warm-up answers take this long, so `/readyz` can be seen warming.
const WARMUP_DELAY: Duration = Duration::from_secs(1);

/// One answer: a label naming the request, the status and the body.
type Answer = (String, StatusCode, Bytes);

async fn probe(gateway: &TestGateway, path: &str) -> (StatusCode, Bytes) {
    let request = Request::get(path)
        .body(full(Bytes::new()))
        .expect("valid request");
    let response = gateway.h1().await.send_uncounted(request).await;
    (response.status, response.body)
}

async fn counted(
    gateway: &TestGateway,
    method: Method,
    path: &str,
    body: Bytes,
) -> (StatusCode, Bytes) {
    let request = gateway
        .chat_request()
        .method(method)
        .uri(path)
        .body(full(body))
        .expect("valid request");
    let response = gateway.h1().await.send_collect(request).await;
    (response.status, response.body)
}

/// Every answer of one router, in request order.
async fn answers(router: RouterKind) -> Vec<Answer> {
    let upstream = ScriptedUpstream::start(|_, request| {
        if is_chat(request) {
            sse_ok()
        } else {
            Reply::Json {
                head_delay: WARMUP_DELAY,
                headers: json_headers(),
                body: Bytes::from_static(b"{}"),
            }
        }
    })
    .await;
    let base_url = upstream.base_url();
    let gateway = start_gateway_unready(|spec| {
        spec.experiments.router = router;
        spec.warmup.ready_timeout = Duration::from_secs(30);
        spec.channels
            .push(channel("a", &base_url, StreamUsage::Passthrough));
    });

    let mut answers = Vec::new();
    let (status, body) = probe(&gateway, "/readyz").await;
    answers.push((String::from("GET /readyz warming"), status, body));
    let (status, body) = probe(&gateway, "/healthz").await;
    answers.push((String::from("GET /healthz"), status, body));

    gateway.wait_ready(WARMUP_DELAY * 10).await;
    let (status, body) = probe(&gateway, "/readyz").await;
    answers.push((String::from("GET /readyz ready"), status, body));

    let gemini = format!(
        "/v1beta/models/{TEST_MODEL}:streamGenerateContent?alt=sse&key={}",
        gateway.key
    );
    let chat = chat_body(TEST_MODEL, true, None);
    for (method, path, body) in [
        (Method::POST, "/v1/chat/completions", chat.clone()),
        (Method::GET, "/v1/chat/completions", Bytes::new()),
        (Method::OPTIONS, "/v1/chat/completions", Bytes::new()),
        (Method::POST, "/healthz", Bytes::new()),
        (Method::DELETE, "/readyz", Bytes::new()),
        (Method::GET, "/v1/models", Bytes::new()),
        (Method::POST, "/v1/chat/completions/", chat.clone()),
        (Method::POST, gemini.as_str(), chat.clone()),
    ] {
        let label = format!("{method} {}", path.split('?').next().unwrap_or(path));
        let (status, body) = counted(&gateway, method, path, body).await;
        answers.push((label, status, body));
    }

    let key = gateway.key.clone();
    let settled = gateway.finish().await;
    for outcome in &settled.outcomes {
        assert!(!format!("{outcome:?}").contains(&key), "{router:?}");
    }
    let rejected: Vec<_> = settled
        .outcomes
        .iter()
        .filter_map(|outcome| match outcome.status {
            OutcomeStatus::Rejected(reason) => Some(reason),
            _ => None,
        })
        .collect();
    assert_eq!(
        rejected,
        [
            RejectReason::MethodNotAllowed,
            RejectReason::MethodNotAllowed,
            RejectReason::MethodNotAllowed,
            RejectReason::MethodNotAllowed,
            RejectReason::NotFound,
            RejectReason::NotFound,
            RejectReason::NotFound,
        ],
        "{router:?}"
    );
    answers
}

fn answer<'a>(answers: &'a [Answer], label: &str) -> &'a Answer {
    answers
        .iter()
        .find(|(name, _, _)| name == label)
        .unwrap_or_else(|| panic!("no answer for {label}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn both_routers_answer_identically() {
    let axum = answers(RouterKind::Axum).await;
    let matched = answers(RouterKind::Match).await;
    assert_eq!(axum, matched);

    for (label, status, body) in [
        ("GET /readyz warming", 503, &br#"{"status":"warming"}"#[..]),
        ("GET /healthz", 200, br#"{"status":"ok"}"#),
        ("GET /readyz ready", 200, br#"{"status":"ready"}"#),
    ] {
        let (_, actual_status, actual_body) = answer(&axum, label);
        assert_eq!(actual_status.as_u16(), status, "{label}");
        assert_eq!(actual_body, body, "{label}");
    }
    let (_, status, body) = answer(&axum, "POST /v1/chat/completions");
    assert_eq!(*status, StatusCode::OK);
    assert_eq!(*body, stream_ok());

    for label in [
        "GET /v1/chat/completions",
        "OPTIONS /v1/chat/completions",
        "POST /healthz",
        "DELETE /readyz",
    ] {
        let (_, status, body) = answer(&axum, label);
        assert_eq!(*status, StatusCode::METHOD_NOT_ALLOWED, "{label}");
        assert!(
            String::from_utf8_lossy(body).contains(r#""code":"method_not_allowed""#),
            "{label}"
        );
    }
    for label in [
        "GET /v1/models",
        "POST /v1/chat/completions/",
        "POST /v1beta/models/grok-4.6(xhigh):streamGenerateContent",
    ] {
        let (_, status, body) = answer(&axum, label);
        assert_eq!(*status, StatusCode::NOT_FOUND, "{label}");
        assert!(
            String::from_utf8_lossy(body).contains(r#""code":"not_found""#),
            "{label}"
        );
    }
}
