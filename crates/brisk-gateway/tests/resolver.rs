//! The upstream address policy end to end: `SafeResolver` inside the client
//! built by `build_client`, and the IP-literal check of `ChannelSet::build`.

mod scripted;

use std::error::Error as _;
use std::time::Duration;

use brisk_gateway::secret::Redacted;
use brisk_gateway::spec::{ChannelSpec, StreamUsage, Timeouts, WarmupTarget};
use brisk_gateway::upstream::registry::{ChannelBuildError, ChannelSet};
use brisk_gateway::upstream::resolver::BaseUrlError;
use brisk_gateway::upstream::{UpstreamClientConfig, build_client};
use bytes::Bytes;
use scripted::{Reply, ScriptedUpstream};

fn profile(allow_private: bool) -> UpstreamClientConfig {
    UpstreamClientConfig {
        allow_private,
        ..UpstreamClientConfig::default()
    }
}

async fn models_upstream() -> ScriptedUpstream {
    ScriptedUpstream::start(|_, _| Reply::Json {
        head_delay: Duration::ZERO,
        headers: Vec::new(),
        body: Bytes::from_static(br#"{"object":"list","data":[]}"#),
    })
    .await
}

/// Every message in the error's source chain, outermost first.
fn chain(err: &(dyn std::error::Error + 'static)) -> Vec<String> {
    let mut messages = vec![err.to_string()];
    let mut source = err.source();
    while let Some(inner) = source {
        messages.push(inner.to_string());
        source = inner.source();
    }
    messages
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn loopback_name_is_refused_without_the_opt_in() {
    let upstream = models_upstream().await;
    let url = format!("http://localhost:{}/v1/models", upstream.addr().port());

    let client = build_client(&profile(false)).unwrap();
    let err = client.get(&url).send().await.unwrap_err();
    assert!(err.is_connect(), "{err:?}");
    let messages = chain(&err);
    assert!(
        messages
            .iter()
            .any(|m| m.contains("localhost") && m.contains("not allowed")),
        "{messages:?}"
    );
    // The refused addresses themselves are not echoed.
    assert!(
        !messages.iter().any(|m| m.contains("127.0.0.1")),
        "{messages:?}"
    );
    assert_eq!(upstream.accepts(), 0);

    let client = build_client(&profile(true)).unwrap();
    let resp = client.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), r#"{"object":"list","data":[]}"#);
    assert_eq!(upstream.accepts(), 1);
    assert_eq!(upstream.requests()[0].target, "/v1/models");
}

fn channel(base_url: &str, allow_private: bool) -> ChannelSpec {
    ChannelSpec {
        name: String::from("ch"),
        base_url: base_url.to_owned(),
        api_key: Redacted::new(String::from("sk-test")),
        weight: 1,
        models: Vec::new(),
        model_map: Vec::new(),
        stream_usage: StreamUsage::Passthrough,
        timeouts: Timeouts::default(),
        client: profile(allow_private),
        warmup: WarmupTarget::default(),
        expose_ratelimit_headers: false,
    }
}

#[test]
fn channel_set_applies_the_address_table() {
    // (base_url, accepted without the opt-in, accepted with it)
    let cases = [
        ("http://127.0.0.1/v1", false, true),
        ("http://169.254.169.254/v1", false, false),
        ("http://[fd00:ec2::254]/v1", false, false),
        ("http://100.100.100.200/v1", false, false),
        ("http://168.63.129.16/v1", false, false),
        ("http://192.0.0.192/v1", false, false),
        ("http://100.64.0.1/v1", false, true),
        ("https://1.1.1.1/v1", true, true),
    ];
    for (base_url, strict, lenient) in cases {
        for (allow_private, expected) in [(false, strict), (true, lenient)] {
            match ChannelSet::build(&[channel(base_url, allow_private)]) {
                Ok(_) => assert!(
                    expected,
                    "{base_url} accepted, allow_private={allow_private}"
                ),
                Err(err) => {
                    assert!(
                        !expected,
                        "{base_url} rejected, allow_private={allow_private}: {err}"
                    );
                    let ChannelBuildError::BaseUrl { source, .. } = &err else {
                        panic!("{base_url}: unexpected error {err:?}");
                    };
                    assert!(matches!(source, BaseUrlError::Forbidden(_)), "{source:?}");
                    assert!(err.source().is_some());
                }
            }
        }
    }
}
