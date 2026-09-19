//! Integration tests for `upstream::build_client` against local servers.

mod support;

use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use brisk_gateway::server::ServerConfig;
use brisk_gateway::upstream::{UpstreamClientConfig, build_client};
use bytes::Bytes;
use http::{Response, StatusCode, header};
use http_body_util::{BodyExt, Full};
use hyper::service::service_fn;

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
