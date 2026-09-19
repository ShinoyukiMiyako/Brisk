//! The upstream client ignores proxy environment variables (R24).
//!
//! Environment variables are process-wide, and `std::env::set_var` is unsafe
//! in edition 2024, so the check runs in a child process: the parent re-runs
//! this test binary with the proxy variables set and only the ignored child
//! test selected.

mod scripted;

use std::net::TcpListener as StdTcpListener;
use std::process::Command;
use std::time::Duration;

use brisk_gateway::upstream::{UpstreamClientConfig, build_client};
use bytes::Bytes;
use scripted::{Reply, ScriptedUpstream};

const PROXY_VARS: [&str; 6] = [
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
];
const CHILD_TEST: &str = "child_requests_go_direct";

#[test]
fn proxy_environment_is_ignored() {
    // A port that was free a moment ago: a proxied request would be refused.
    let dead = StdTcpListener::bind("127.0.0.1:0").unwrap();
    let proxy = format!("http://{}", dead.local_addr().unwrap());
    drop(dead);

    let mut child = Command::new(std::env::current_exe().unwrap());
    child.args([
        "--exact",
        CHILD_TEST,
        "--ignored",
        "--nocapture",
        "--test-threads=1",
    ]);
    for var in PROXY_VARS {
        child.env(var, &proxy);
    }
    // Without this the child would bypass the proxy for loopback anyway.
    child.env_remove("NO_PROXY").env_remove("no_proxy");
    let output = child.output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "child failed\n--- stdout\n{stdout}\n--- stderr\n{stderr}"
    );
    assert!(stdout.contains("1 passed"), "child did not run:\n{stdout}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "run by proxy_environment_is_ignored in a child process"]
async fn child_requests_go_direct() {
    let proxy = std::env::var("HTTP_PROXY").expect("the parent sets HTTP_PROXY");
    let upstream = ScriptedUpstream::start(|_, _| Reply::Json {
        head_delay: Duration::ZERO,
        headers: Vec::new(),
        body: Bytes::from_static(b"{}"),
    })
    .await;
    for scheme_var in ["HTTPS_PROXY", "ALL_PROXY"] {
        assert_eq!(std::env::var(scheme_var).as_deref(), Ok(proxy.as_str()));
    }
    let url = format!("{}/models", upstream.base_url());

    // Control: a default reqwest client honours the variables and fails.
    let proxied = reqwest::Client::new().get(&url).send().await;
    assert!(
        proxied.is_err(),
        "the proxy variables had no effect: {proxied:?}"
    );
    assert_eq!(upstream.accepts(), 0);

    let client = build_client(&UpstreamClientConfig {
        allow_private: true,
        ..UpstreamClientConfig::default()
    })
    .unwrap();
    let resp = client.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "{}");
    assert_eq!(upstream.accepts(), 1);
    assert_eq!(upstream.requests()[0].target, "/v1/models");
}
