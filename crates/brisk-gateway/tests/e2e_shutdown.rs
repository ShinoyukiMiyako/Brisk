//! Graceful shutdown (section 2.9): new connections are refused, in-flight
//! streams finish within the grace period, streams that outlive it are
//! aborted with a `ShutdownAborted` outcome, and warm-up stops with `serve`,
//! also when its future is dropped.

mod e2e_support;
mod scripted;

use std::net::SocketAddr;
use std::time::Duration;

use brisk_gateway::outcome::OutcomeStatus;
use brisk_gateway::spec::StreamUsage;
use bytes::Bytes;
use e2e_support::{
    CONTENT_EVENT, DONE_EVENT, FINISH_USAGE_EVENT, StopServe, TEST_MODEL, channel, chat_body,
    chat_only, collect_until_error, read_until, sse_headers, start_gateway, status_reply,
};
use http::Request;
use http_body_util::Full;
use hyper_util::rt::TokioIo;
use scripted::{Reply, ScriptedUpstream, SseEnd};
use tokio::net::TcpStream;
use tokio::time::{Instant, timeout};

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
    assert_eq!(received, CONTENT_EVENT);
    drop(client);

    let settled = gateway.finish().await;
    assert_eq!(settled.only().status, OutcomeStatus::ShutdownAborted);
    assert_eq!(settled.tally.shutdown_aborted, 1);
}

/// Warm-up interval of the tests below, short enough that a loop that keeps
/// running shows up within a few hundred milliseconds.
const WARMUP_INTERVAL: Duration = Duration::from_millis(50);

/// Section 2.9: `serve` stops warm-up both when it returns and when its
/// future is dropped (`brisk` aborts the serve task on a second signal).
/// The warm-up task holds an `OutcomeSink`, so while it runs the outcome
/// logger never sees every sender gone and waits out its full 3 s.
async fn warm_up_stops_with_serve(how: StopServe) {
    let upstream = ScriptedUpstream::start(|_, _| status_reply(200, b"")).await;
    let base_url = upstream.base_url();
    let gateway = start_gateway(|spec| {
        spec.warmup.interval = WARMUP_INTERVAL;
        spec.channels
            .push(channel("a", &base_url, StreamUsage::Passthrough));
    })
    .await;
    // A second request shows the interval loop running, not just the first
    // round that made the gateway ready.
    let deadline = Instant::now() + Duration::from_secs(10);
    while upstream.requests().len() < 2 {
        assert!(Instant::now() < deadline, "no second warm-up round");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let mut outcomes = gateway.stop_serve(how).await;
    // Every job task holds a clone of the sink, so this also shows that they
    // were dropped. The bound is well inside the logger's 3 s.
    let closed = timeout(Duration::from_secs(1), outcomes.recv()).await;
    assert!(
        matches!(closed, Ok(None)),
        "an OutcomeSink outlived serve ({how:?}): {closed:?}"
    );

    // A request written just before the jobs stopped may still be in
    // transit; after that the upstream must see nothing for many intervals.
    tokio::time::sleep(WARMUP_INTERVAL * 2).await;
    let settled = upstream.requests().len();
    tokio::time::sleep(WARMUP_INTERVAL * 10).await;
    assert_eq!(
        upstream.requests().len(),
        settled,
        "warm-up requests continued after serve ended ({how:?})"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn warm_up_stops_when_serve_returns() {
    warm_up_stops_with_serve(StopServe::Shutdown).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn warm_up_stops_when_the_serve_future_is_dropped() {
    warm_up_stops_with_serve(StopServe::Drop).await;
}
