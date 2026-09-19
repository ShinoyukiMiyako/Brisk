//! Connection warm-up against a scripted upstream: what is sent, how often,
//! and when the gateway reports ready.

mod scripted;

use std::net::TcpListener as StdTcpListener;
use std::sync::atomic::Ordering;
use std::time::Duration;

use brisk_gateway::outcome::{OutcomeSink, outcome_channel};
use brisk_gateway::secret::Redacted;
use brisk_gateway::spec::{
    ChannelSpec, StreamUsage, Timeouts, WarmupMethod, WarmupSpec, WarmupTarget,
};
use brisk_gateway::upstream::UpstreamClientConfig;
use brisk_gateway::upstream::registry::ChannelSet;
use brisk_gateway::upstream::warmup::{Readiness, WarmupJob, spawn_warmup};
use bytes::Bytes;
use scripted::{Reply, ScriptedUpstream};
use tokio::time::{Instant, sleep};

fn channel(name: &str, base_url: &str, method: WarmupMethod, path: &str) -> ChannelSpec {
    ChannelSpec {
        name: name.to_owned(),
        base_url: base_url.to_owned(),
        api_key: Redacted::new(format!("sk-{name}")),
        weight: 1,
        models: Vec::new(),
        model_map: Vec::new(),
        stream_usage: StreamUsage::Passthrough,
        timeouts: Timeouts::default(),
        client: UpstreamClientConfig {
            allow_private: true,
            ..UpstreamClientConfig::default()
        },
        warmup: WarmupTarget {
            method,
            path: path.to_owned(),
        },
        expose_ratelimit_headers: false,
    }
}

fn jobs(specs: &[ChannelSpec]) -> Vec<WarmupJob> {
    ChannelSet::build(specs).unwrap().warmup_jobs().to_vec()
}

fn spec(interval: Duration, ready_timeout: Duration, request_timeout: Duration) -> WarmupSpec {
    WarmupSpec {
        interval,
        ready_timeout,
        request_timeout,
    }
}

fn sink() -> OutcomeSink {
    outcome_channel(16).0
}

fn empty_ok() -> Reply {
    Reply::Status {
        status: 200,
        headers: Vec::new(),
        body: Bytes::new(),
    }
}

/// Polls `readiness` until it is ready or `limit` elapses; returns when it
/// became ready, measured from `start`.
async fn wait_ready(readiness: &Readiness, start: Instant, limit: Duration) -> Option<Duration> {
    while start.elapsed() < limit {
        if readiness.is_ready() {
            return Some(start.elapsed());
        }
        sleep(Duration::from_millis(5)).await;
    }
    None
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn head_healthz_without_credentials_once_per_origin() {
    let upstream = ScriptedUpstream::start(|_, _| empty_ok()).await;
    let base = upstream.base_url();
    let jobs = jobs(&[
        channel("cpa-a", &base, WarmupMethod::Head, "/healthz"),
        channel("cpa-b", &base, WarmupMethod::Head, "/healthz"),
    ]);
    assert_eq!(jobs.len(), 1);

    let long = Duration::from_secs(3600);
    let start = Instant::now();
    let (readiness, handle) = spawn_warmup(jobs, &spec(long, long, long), sink());
    // The first round finished long before the one-hour ready timeout.
    wait_ready(&readiness, start, Duration::from_secs(10))
        .await
        .expect("ready after the first round");

    // Give a (wrongly) duplicated job time to show up.
    sleep(Duration::from_millis(200)).await;
    let requests = upstream.requests();
    assert_eq!(requests.len(), 1, "{requests:?}");
    let request = &requests[0];
    assert_eq!(request.method, "HEAD");
    assert_eq!(request.target, "/healthz");
    assert!(request.body.is_empty());
    for name in ["authorization", "x-api-key", "x-goog-api-key", "cookie"] {
        assert_eq!(
            request.header_values(name).count(),
            0,
            "{name}: {request:?}"
        );
    }
    let raw = format!("{request:?}");
    assert!(!raw.contains("sk-cpa"), "{raw}");
    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_reads_the_body_and_reuses_the_connection() {
    let upstream = ScriptedUpstream::start(|_, _| Reply::Json {
        head_delay: Duration::ZERO,
        headers: Vec::new(),
        body: Bytes::from(vec![b' '; 10_000]),
    })
    .await;
    let jobs = jobs(&[channel(
        "mock",
        &upstream.base_url(),
        WarmupMethod::Get,
        "/v1/models",
    )]);
    let interval = Duration::from_millis(100);
    let long = Duration::from_secs(3600);
    let (_readiness, handle) = spawn_warmup(jobs, &spec(interval, long, long), sink());

    let start = Instant::now();
    while upstream.requests().len() < 3 {
        assert!(start.elapsed() < Duration::from_secs(10), "no third round");
        sleep(Duration::from_millis(10)).await;
    }
    handle.abort();
    let requests = upstream.requests();
    assert!(
        requests
            .iter()
            .all(|r| r.method == "GET" && r.target == "/v1/models")
    );
    // The body was read to the end, so every round found the pooled connection.
    assert_eq!(upstream.accepts(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interval_sends_again() {
    let upstream = ScriptedUpstream::start(|_, _| empty_ok()).await;
    let jobs = jobs(&[channel(
        "cpa",
        &upstream.base_url(),
        WarmupMethod::Head,
        "/healthz",
    )]);
    let interval = Duration::from_millis(300);
    let long = Duration::from_secs(3600);
    let start = Instant::now();
    let (readiness, handle) = spawn_warmup(jobs, &spec(interval, long, long), sink());
    wait_ready(&readiness, start, Duration::from_secs(10))
        .await
        .expect("ready after the first round");
    assert_eq!(upstream.requests().len(), 1);

    while upstream.requests().len() < 2 {
        assert!(start.elapsed() < Duration::from_secs(10), "no second round");
        sleep(Duration::from_millis(10)).await;
    }
    // The second round waits for the interval rather than following at once.
    assert!(start.elapsed() >= interval, "{:?}", start.elapsed());
    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hanging_upstream_is_ready_at_the_timeout() {
    let upstream = ScriptedUpstream::start(|_, _| Reply::Hang).await;
    let jobs = jobs(&[channel(
        "slow",
        &upstream.base_url(),
        WarmupMethod::Head,
        "/healthz",
    )]);
    let ready_timeout = Duration::from_millis(300);
    let long = Duration::from_secs(3600);
    let start = Instant::now();
    let (readiness, handle) = spawn_warmup(jobs, &spec(long, ready_timeout, long), sink());
    let became_ready = wait_ready(&readiness, start, Duration::from_secs(10))
        .await
        .expect("ready at the ready timeout");
    assert!(became_ready >= ready_timeout, "{became_ready:?}");
    assert_eq!(upstream.requests().len(), 1);
    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failures_are_counted() {
    let hanging = ScriptedUpstream::start(|_, _| Reply::Hang).await;
    let refused = {
        let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        format!("http://{addr}/v1")
    };
    let jobs = jobs(&[
        channel("refused", &refused, WarmupMethod::Head, "/"),
        channel("hanging", &hanging.base_url(), WarmupMethod::Head, "/"),
    ]);
    assert_eq!(jobs.len(), 2);
    let sink = sink();
    let long = Duration::from_secs(3600);
    let start = Instant::now();
    let (readiness, handle) = spawn_warmup(
        jobs,
        &spec(long, long, Duration::from_millis(200)),
        sink.clone(),
    );
    // Both requests fail quickly, which also ends the first round.
    wait_ready(&readiness, start, Duration::from_secs(10))
        .await
        .expect("ready after a failed first round");
    assert_eq!(sink.counters().warmup_failures.load(Ordering::Relaxed), 2);
    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_jobs_is_ready_at_once() {
    let long = Duration::from_secs(3600);
    let start = Instant::now();
    let (readiness, handle) = spawn_warmup(Vec::new(), &spec(long, long, long), sink());
    wait_ready(&readiness, start, Duration::from_secs(10))
        .await
        .expect("ready without jobs");
    handle.await.unwrap();
}
