//! Graceful shutdown (section 2.9): new connections are refused, in-flight
//! streams finish within the grace period, and streams that outlive it are
//! aborted with a `ShutdownAborted` outcome.

// Not built until the scripted upstream (P2-SUPPORT) and `Gateway`
// (P5-GATEWAY) are merged into m1/integration; the integrator removes this
// attribute and the `rustfmt::skip` on `mod scripted` at that checkpoint.
#![cfg(any())]

#[rustfmt::skip]
mod scripted;
mod e2e_support;

use std::net::SocketAddr;
use std::time::Duration;

use brisk_gateway::outcome::OutcomeStatus;
use brisk_gateway::spec::StreamUsage;
use bytes::Bytes;
use e2e_support::{
    CONTENT_EVENT, DONE_EVENT, FINISH_USAGE_EVENT, TEST_MODEL, channel, chat_body, chat_only,
    collect_until_error, read_until, sse_headers, start_gateway,
};
use http::Request;
use http_body_util::Full;
use hyper_util::rt::TokioIo;
use scripted::{Reply, ScriptedUpstream, SseEnd};
use tokio::net::TcpStream;
use tokio::time::timeout;

fn slow_stream(end: SseEnd) -> Reply {
    let mut frames = vec![(Duration::ZERO, Bytes::from_static(CONTENT_EVENT))];
    if matches!(end, SseEnd::Finish) {
        frames.extend((0..5).map(|_| {
            (
                Duration::from_millis(100),
                Bytes::from_static(CONTENT_EVENT),
            )
        }));
        frames.push((Duration::ZERO, Bytes::from_static(FINISH_USAGE_EVENT)));
        frames.push((Duration::ZERO, Bytes::from_static(DONE_EVENT)));
    }
    Reply::Sse {
        head_delay: Duration::ZERO,
        headers: sse_headers(),
        frames,
        end,
    }
}

/// Whether a new connection to `addr` gets an answer to `GET /healthz`
/// within a second.
async fn probe_serves(addr: SocketAddr) -> bool {
    let Ok(stream) = TcpStream::connect(addr).await else {
        return false;
    };
    let probe = async {
        let (mut sender, connection) =
            hyper::client::conn::http1::handshake::<_, Full<Bytes>>(TokioIo::new(stream))
                .await
                .ok()?;
        tokio::spawn(connection);
        let request = Request::get("/healthz")
            .header("host", addr.to_string())
            .body(Full::default())
            .ok()?;
        sender.send_request(request).await.ok()
    };
    matches!(timeout(Duration::from_secs(1), probe).await, Ok(Some(_)))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_flight_streams_finish_and_new_connections_are_refused() {
    let upstream = ScriptedUpstream::start(chat_only(|_, _| slow_stream(SseEnd::Finish))).await;
    let base_url = upstream.base_url();
    let mut gateway = start_gateway(|spec| {
        spec.server.graceful_shutdown_timeout = Duration::from_secs(5);
        spec.channels
            .push(channel("a", &base_url, StreamUsage::Passthrough));
    })
    .await;

    let mut client = gateway.h1().await;
    let response = client
        .send(gateway.chat_with(chat_body(TEST_MODEL, true, None)))
        .await;
    assert_eq!(response.status(), 200);
    let mut body = response.into_body();
    read_until(&mut body, CONTENT_EVENT).await;

    gateway.begin_shutdown();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !probe_serves(gateway.addr).await,
        "a new connection was served during shutdown"
    );

    let rest = read_until(&mut body, DONE_EVENT).await;
    assert!(rest.ends_with(DONE_EVENT));
    drop(body);
    drop(client);

    let settled = gateway.finish().await;
    assert_eq!(settled.only().status, OutcomeStatus::Completed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn streams_outliving_the_grace_period_are_aborted() {
    let upstream = ScriptedUpstream::start(chat_only(|_, _| slow_stream(SseEnd::Hang))).await;
    let base_url = upstream.base_url();
    let mut gateway = start_gateway(|spec| {
        spec.server.graceful_shutdown_timeout = Duration::from_millis(300);
        spec.channels
            .push(channel("a", &base_url, StreamUsage::Passthrough));
    })
    .await;

    let mut client = gateway.h1().await;
    let response = client
        .send(gateway.chat_with(chat_body(TEST_MODEL, true, None)))
        .await;
    assert_eq!(response.status(), 200);
    gateway.begin_shutdown();
    let (received, clean) = collect_until_error(response).await;
    assert!(!clean, "an aborted stream does not end cleanly");
    assert!(received == CONTENT_EVENT);
    drop(client);

    let settled = gateway.finish().await;
    assert_eq!(settled.only().status, OutcomeStatus::ShutdownAborted);
    assert_eq!(settled.tally.shutdown_aborted, 1);
}
