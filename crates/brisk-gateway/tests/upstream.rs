//! Integration tests for `upstream::build_client` and the client registry
//! against local servers.

mod scripted;
mod support;

use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use brisk_gateway::secret::Redacted;
use brisk_gateway::server::ServerConfig;
use brisk_gateway::spec::{ChannelSpec, StreamUsage, Timeouts, WarmupTarget};
use brisk_gateway::upstream::registry::{ChannelSet, ChannelWarning, ClientRegistry};
use brisk_gateway::upstream::{UpstreamClientConfig, build_client};
use bytes::Bytes;
use http::{Response, StatusCode, header};
use http_body_util::{BodyExt, Full};
use hyper::service::service_fn;
use scripted::{Reply, ScriptConfig, ScriptedUpstream, Split, SseEnd, frames_of};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redirect_is_returned_not_followed() {
    let landing_hits = Arc::new(AtomicUsize::new(0));
    let hits = Arc::clone(&landing_hits);
    let mut landing = support::start(
        ServerConfig::default(),
        None,
        service_fn(move |_req| {
            hits.fetch_add(1, Ordering::SeqCst);
            support::text("landed")
        }),
    );

    let location = format!("{}/landing", landing.http_base());
    let mut redirector = support::start(
        ServerConfig::default(),
        None,
        service_fn(move |_req| {
            let location = location.clone();
            async move {
                Ok::<_, Infallible>(
                    Response::builder()
                        .status(StatusCode::TEMPORARY_REDIRECT)
                        .header(header::LOCATION, location)
                        .body(Full::new(Bytes::new()))
                        .unwrap(),
                )
            }
        }),
    );

    let client = build_client(&UpstreamClientConfig::default()).unwrap();
    let url = format!("{}/v1/chat/completions", redirector.http_base());

    let get = client.get(&url).send().await.unwrap();
    assert_eq!(get.status(), StatusCode::TEMPORARY_REDIRECT);
    assert_eq!(
        get.headers()[header::LOCATION],
        format!("{}/landing", landing.http_base()).as_str()
    );

    let post = client
        .post(&url)
        .header("x-api-key", "secret")
        .body(r#"{"model":"m"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(post.status(), StatusCode::TEMPORARY_REDIRECT);

    // Give a (wrongly) following client ample time to reach the second server.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(landing_hits.load(Ordering::SeqCst), 0);

    redirector.trigger_shutdown();
    landing.trigger_shutdown();
    redirector.task.await.unwrap().unwrap();
    landing.task.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plain_http_upstream_round_trip() {
    let mut upstream = support::start(
        ServerConfig::default(),
        None,
        service_fn(|req: http::Request<hyper::body::Incoming>| async move {
            let (parts, body) = req.into_parts();
            let body = body.collect().await?.to_bytes();
            let mut echoed =
                format!("{} {} {:?} ", parts.method, parts.uri.path(), parts.version).into_bytes();
            echoed.extend_from_slice(&body);
            Ok::<_, hyper::Error>(Response::new(Full::new(Bytes::from(echoed))))
        }),
    );

    let client = build_client(&UpstreamClientConfig::default()).unwrap();
    let url = format!("{}/v1/chat/completions", upstream.http_base());
    for round in 0..3 {
        let payload = format!(r#"{{"model":"cpa","round":{round}}}"#);
        let resp = client
            .post(&url)
            .body(payload.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.version(), http::Version::HTTP_11);
        assert_eq!(
            resp.text().await.unwrap(),
            format!("POST /v1/chat/completions HTTP/1.1 {payload}")
        );
    }

    upstream.trigger_shutdown();
    upstream.task.await.unwrap().unwrap();
}

/// Loopback upstreams need the private-address opt-in once a host name is
/// involved; IP literals never reach the resolver but stay consistent.
fn loopback_profile() -> UpstreamClientConfig {
    UpstreamClientConfig {
        allow_private: true,
        ..UpstreamClientConfig::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upstream_503_is_not_retried() {
    let upstream = ScriptedUpstream::start(|_, _| Reply::Status {
        status: 503,
        headers: vec![("retry-after", "1".to_owned())],
        body: Bytes::from_static(br#"{"error":"busy"}"#),
    })
    .await;
    let client = build_client(&loopback_profile()).unwrap();

    let resp = client
        .post(format!("{}/chat/completions", upstream.base_url()))
        .body(r#"{"model":"grok-4.6(xhigh)"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(resp.headers()["retry-after"], "1");
    assert_eq!(resp.text().await.unwrap(), r#"{"error":"busy"}"#);

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(upstream.requests().len(), 1);
    assert_eq!(upstream.accepts(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connection_reset_is_not_retried() {
    let upstream = ScriptedUpstream::start(|_, _| Reply::Reset).await;
    let client = build_client(&loopback_profile()).unwrap();

    let err = client
        .post(format!("{}/chat/completions", upstream.base_url()))
        .body(r#"{"model":"grok-4.6(xhigh)"}"#)
        .send()
        .await
        .unwrap_err();
    assert!(!err.is_status(), "{err:?}");

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(upstream.requests().len(), 1);
    assert_eq!(upstream.accepts(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registry_shares_one_pool_per_profile() {
    let upstream = ScriptedUpstream::start(|_, _| Reply::Json {
        head_delay: Duration::ZERO,
        headers: Vec::new(),
        body: Bytes::from_static(b"{}"),
    })
    .await;
    let mut registry = ClientRegistry::default();
    let profile = loopback_profile();
    let url = format!("{}/models", upstream.base_url());

    for _ in 0..3 {
        let client = registry.client(&profile).unwrap();
        let resp = client.get(&url).send().await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.text().await.unwrap(), "{}");
    }
    // Each lookup handed out the same pool, so the keep-alive connection of
    // the first request served the other two.
    assert_eq!(upstream.accepts(), 1);
    assert_eq!(upstream.requests().len(), 3);

    let other = UpstreamClientConfig {
        connect_timeout: Duration::from_secs(1),
        ..loopback_profile()
    };
    let resp = registry
        .client(&other)
        .unwrap()
        .get(&url)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    resp.bytes().await.unwrap();
    assert_eq!(upstream.accepts(), 2);
}

fn channel(name: &str, base_url: &str, client: UpstreamClientConfig) -> ChannelSpec {
    ChannelSpec {
        name: name.to_owned(),
        base_url: base_url.to_owned(),
        api_key: Redacted::new(String::from("sk-test")),
        weight: 1,
        models: Vec::new(),
        model_map: Vec::new(),
        stream_usage: StreamUsage::Passthrough,
        timeouts: Timeouts::default(),
        client,
        warmup: WarmupTarget::default(),
        expose_ratelimit_headers: false,
    }
}

#[test]
fn split_pool_warning_for_one_host_under_two_profiles() {
    let other = UpstreamClientConfig {
        pool_idle_timeout: Duration::from_secs(5),
        ..loopback_profile()
    };
    let set = ChannelSet::build(&[
        channel("a", "http://127.0.0.1:8317/v1", loopback_profile()),
        channel("b", "http://127.0.0.1:8317/v1", loopback_profile()),
        channel("c", "http://127.0.0.1:9000/v1", other.clone()),
    ])
    .unwrap();
    assert!(set.warnings().is_empty(), "{:?}", set.warnings());

    let set = ChannelSet::build(&[
        channel("a", "http://127.0.0.1:8317/v1", loopback_profile()),
        channel("b", "http://127.0.0.1:8317/v1", other),
    ])
    .unwrap();
    assert_eq!(
        set.warnings(),
        [ChannelWarning::SplitPool {
            host: "127.0.0.1:8317".into()
        }]
    );
}

const SSE: &[u8] =
    b": keep-alive\n\ndata: {\"a\":1}\r\n\r\nevent: x\rdata: 2\r\rdata: [DONE]\n\n\n";

#[test]
fn frames_of_cuts_at_event_ends_and_random_points() {
    let whole = frames_of(SSE, Split::Whole);
    assert_eq!(whole, [Bytes::from_static(SSE)]);

    let events = frames_of(SSE, Split::PerEvent);
    let expected: [&[u8]; 5] = [
        b": keep-alive\n\n",
        b"data: {\"a\":1}\r\n\r\n",
        b"event: x\rdata: 2\r\r",
        b"data: [DONE]\n\n",
        b"\n",
    ];
    assert_eq!(events, expected);

    for seed in 0..50 {
        let pieces = frames_of(
            SSE,
            Split::Random {
                seed,
                min_piece: 1,
                max_piece: 7,
            },
        );
        assert_eq!(pieces.concat(), SSE);
        let (last, rest) = pieces.split_last().unwrap();
        assert!(rest.iter().all(|p| (1..=7).contains(&p.len())));
        assert!((1..=7).contains(&last.len()));
        let again = frames_of(
            SSE,
            Split::Random {
                seed,
                min_piece: 1,
                max_piece: 7,
            },
        );
        assert_eq!(pieces, again, "the split is deterministic per seed");
    }
    assert!(frames_of(b"", Split::PerEvent).is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scripted_sse_is_chunked_and_bytes_arrive_unchanged() {
    let upstream = ScriptedUpstream::start(|_, _| Reply::Sse {
        head_delay: Duration::ZERO,
        headers: vec![("x-cpa-trace-id", "0".to_owned())],
        frames: frames_of(SSE, Split::PerEvent)
            .into_iter()
            .map(|frame| (Duration::from_millis(5), frame))
            .collect(),
        end: SseEnd::Finish,
    })
    .await;
    let client = build_client(&loopback_profile()).unwrap();
    let body = r#"{"model":"grok-4.6(xhigh)","stream":true}"#;
    let url = format!("{}/chat/completions", upstream.base_url());
    for _ in 0..2 {
        let resp = client
            .post(&url)
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.headers()["content-type"], "text/event-stream");
        assert_eq!(resp.headers()["transfer-encoding"], "chunked");
        assert_eq!(resp.headers()["x-cpa-trace-id"], "0");
        assert_eq!(resp.bytes().await.unwrap(), SSE);
    }
    let requests = upstream.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].method, "POST");
    assert_eq!(requests[0].target, "/v1/chat/completions");
    assert_eq!(requests[0].body, body.as_bytes());
    assert_eq!(
        requests[0]
            .header_values("Content-Type")
            .collect::<Vec<_>>(),
        [b"application/json"]
    );
    assert_eq!((requests[0].conn, requests[1].conn), (0, 0));
    assert_eq!(upstream.accepts(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scripted_sse_close_truncates_the_body() {
    let upstream = ScriptedUpstream::start(|_, _| Reply::Sse {
        head_delay: Duration::ZERO,
        headers: Vec::new(),
        frames: vec![(Duration::ZERO, Bytes::from_static(b"data: 1\n\n"))],
        end: SseEnd::Close,
    })
    .await;
    let client = build_client(&loopback_profile()).unwrap();
    let resp = client
        .get(format!("{}/x", upstream.base_url()))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let err = resp.bytes().await.unwrap_err();
    assert!(err.is_body() || err.is_decode(), "{err:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scripted_upstream_sees_the_client_leave_mid_stream() {
    let upstream = ScriptedUpstream::start(|_, _| Reply::Sse {
        head_delay: Duration::ZERO,
        headers: Vec::new(),
        frames: vec![
            (Duration::ZERO, Bytes::from_static(b"data: 1\n\n")),
            (
                Duration::from_secs(3600),
                Bytes::from_static(b"data: 2\n\n"),
            ),
        ],
        end: SseEnd::Finish,
    })
    .await;
    let client = build_client(&loopback_profile()).unwrap();
    let mut resp = client
        .get(format!("{}/x", upstream.base_url()))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.chunk().await.unwrap().unwrap(), "data: 1\n\n");
    assert_eq!(upstream.closed_at(0), None);

    let dropped_at = tokio::time::Instant::now();
    drop(resp);
    drop(client);
    let closed = upstream
        .wait_closed(0, Duration::from_secs(5))
        .await
        .expect("the upstream saw the close");
    assert!(closed >= dropped_at);
    assert_eq!(upstream.closed_at(0), Some(closed));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scripted_idle_close_and_hang() {
    let upstream = ScriptedUpstream::start_with(
        ScriptConfig {
            idle_close: Some(Duration::from_millis(100)),
        },
        |index, _| match index {
            0 => Reply::Redirect {
                status: 307,
                location: "http://127.0.0.1:9/elsewhere".to_owned(),
            },
            _ => Reply::Hang,
        },
    )
    .await;
    let client = build_client(&loopback_profile()).unwrap();
    let url = format!("{}/x", upstream.base_url());
    let resp = client.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::TEMPORARY_REDIRECT);
    assert_eq!(
        resp.headers()[header::LOCATION],
        "http://127.0.0.1:9/elsewhere"
    );
    resp.bytes().await.unwrap();

    // The upstream closes the idle connection, so the next request needs a
    // new one; that one hangs until the client gives up.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let hung = tokio::time::timeout(Duration::from_millis(200), client.get(&url).send()).await;
    assert!(hung.is_err(), "the hanging reply answered: {hung:?}");
    assert_eq!(upstream.accepts(), 2);
    assert_eq!(upstream.requests().len(), 2);
    drop(client);
    upstream
        .wait_closed(1, Duration::from_secs(5))
        .await
        .expect("the hung connection was closed by the client");
    // The first connection was closed by the upstream, not by the peer.
    assert_eq!(upstream.closed_at(0), None);
}
