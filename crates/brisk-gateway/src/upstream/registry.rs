//! Channel registry: validates every configured channel, derives its chat
//! endpoint URL from the base URL, and builds one HTTP client per distinct
//! client profile so that channels with equal profiles share a connection
//! pool (R12, R25).

use std::net::IpAddr;
use std::sync::Arc;

use brisk_proto::splice::json_string;
use bytes::Bytes;
use hashbrown::{HashMap, HashSet};
use http::HeaderValue;
use reqwest::{Client, Method, Url};
use url::Host;

use super::client::{UpstreamClientConfig, UpstreamError, build_client};
use super::resolver::{AddrClass, BaseUrlError, classify, validate_base_url};
use super::warmup::WarmupJob;
use crate::secret::Redacted;
use crate::spec::{ChannelId, ChannelSpec, StreamUsage, Timeouts, WarmupMethod, WarmupTarget};

/// Largest number of channels; selection tracks tried channels in a `u64`.
const MAX_CHANNELS: usize = 64;

/// Everything the forwarding path needs about one channel, validated and
/// precomputed so that no request re-parses configuration.
#[derive(Debug)]
pub struct ChannelRuntime {
    /// Position in `GatewaySpec::channels`.
    pub id: ChannelId,
    /// Name used in logs instead of the URL or key.
    pub name: Box<str>,
    /// `chat_url(base_url)`.
    pub chat_url: Url,
    /// `Bearer <key>`, marked sensitive.
    pub auth: Redacted<HeaderValue>,
    /// Shared with every channel of the same client profile.
    pub client: Client,
    /// Relative selection weight; at least 1.
    pub weight: u32,
    /// Client model name -> JSON string literal of the upstream name.
    pub model_map: HashMap<Box<str>, Bytes>,
    /// How usage is obtained from streamed responses (D8).
    pub stream_usage: StreamUsage,
    /// Deadlines, already merged with the global defaults.
    pub timeouts: Timeouts,
    /// Also forward the upstream's rate-limit response headers.
    pub expose_ratelimit_headers: bool,
}

/// `base` + `chat/completions`, appended as path segments (never `Url::join`).
///
/// `Url::join` resolves relative to the last segment, so
/// `http://host:8317/v1` joined with `chat/completions` would lose `v1`; CPA
/// answers such a path with a 404 that is not failed over and looks like an
/// unknown model to the client.
///
/// # Panics
///
/// When `base` cannot be a base (e.g. `mailto:`); every URL accepted by
/// [`validate_base_url`](super::resolver::validate_base_url) can.
pub fn chat_url(base: &Url) -> Url {
    let mut url = base.clone();
    url.path_segments_mut()
        .expect("http and https URLs always have a hierarchical path")
        .pop_if_empty()
        .extend(["chat", "completions"]);
    url
}

/// Something worth a startup warning; `Gateway::new` logs each one and
/// `brisk check-config` prints them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChannelWarning {
    /// The same host is reached through more than one client profile.
    SplitPool {
        /// `host:port` of the upstream.
        host: Box<str>,
    },
    /// `http://` to a host that is neither a loopback literal nor `localhost`:
    /// the channel key travels in cleartext (D28).
    PlaintextRemote {
        /// Channel name.
        channel: Box<str>,
    },
    /// `base_url` has an empty path, so `/chat/completions` is requested at
    /// the root; valid for some providers, usually a missing `/v1` (D1).
    NoVersionPath {
        /// Channel name.
        channel: Box<str>,
    },
}

/// The validated channels, their selection weights, the warm-up plan and the
/// warnings found on the way.
#[derive(Debug)]
pub struct ChannelSet {
    channels: Box<[Arc<ChannelRuntime>]>,
    weights: Box<[u32]>,
    warmup_jobs: Vec<WarmupJob>,
    warnings: Vec<ChannelWarning>,
}

impl ChannelSet {
    /// Validates every channel, builds one client per distinct
    /// `UpstreamClientConfig`, and plans warm-up jobs. No network I/O.
    pub fn build(specs: &[ChannelSpec]) -> Result<Self, ChannelBuildError> {
        if specs.is_empty() {
            return Err(ChannelBuildError::Empty);
        }
        if specs.len() > MAX_CHANNELS {
            return Err(ChannelBuildError::TooMany(specs.len()));
        }

        let mut registry = ClientRegistry::default();
        let mut names = HashSet::with_capacity(specs.len());
        let mut channels = Vec::with_capacity(specs.len());
        let mut warnings = Vec::new();
        let mut warmups = WarmupPlanner::default();
        let mut pools = PoolTracker::default();

        // `specs.len() <= MAX_CHANNELS`, so every index fits the `u16` id.
        for (index, spec) in (0_u16..).zip(specs) {
            let channel = || spec.name.clone();
            if !names.insert(spec.name.as_str()) {
                return Err(ChannelBuildError::DuplicateName(channel()));
            }
            if spec.weight == 0 {
                return Err(ChannelBuildError::ZeroWeight(channel()));
            }
            let base =
                validate_base_url(&spec.base_url, spec.client.allow_private).map_err(|source| {
                    ChannelBuildError::BaseUrl {
                        channel: channel(),
                        source,
                    }
                })?;
            let auth = bearer(spec).ok_or_else(|| ChannelBuildError::ApiKey(channel()))?;
            let model_map = model_map(spec).map_err(|reason| ChannelBuildError::ModelMap {
                channel: channel(),
                reason,
            })?;
            let warmup_url =
                warmup_url(&base, &spec.warmup).map_err(|reason| ChannelBuildError::Warmup {
                    channel: channel(),
                    reason,
                })?;
            let client =
                registry
                    .client(&spec.client)
                    .map_err(|source| ChannelBuildError::Client {
                        channel: channel(),
                        source,
                    })?;

            if matches!(base.path(), "" | "/") {
                warnings.push(ChannelWarning::NoVersionPath {
                    channel: spec.name.as_str().into(),
                });
            }
            if base.scheme() == "http" && !is_local_host(&base) {
                warnings.push(ChannelWarning::PlaintextRemote {
                    channel: spec.name.as_str().into(),
                });
            }
            if let Some(host) = pools.observe(&base, &spec.client) {
                warnings.push(ChannelWarning::SplitPool { host });
            }
            warmups.add(&spec.client, &client, spec, warmup_url);

            channels.push(Arc::new(ChannelRuntime {
                id: ChannelId(index),
                name: spec.name.as_str().into(),
                chat_url: chat_url(&base),
                auth,
                client,
                weight: spec.weight,
                model_map,
                stream_usage: spec.stream_usage,
                timeouts: spec.timeouts,
                expose_ratelimit_headers: spec.expose_ratelimit_headers,
            }));
        }

        let weights = channels.iter().map(|c| c.weight).collect();
        Ok(Self {
            channels: channels.into_boxed_slice(),
            weights,
            warmup_jobs: warmups.jobs,
            warnings,
        })
    }

    /// Number of channels; at least 1.
    pub fn len(&self) -> usize {
        self.channels.len()
    }

    /// Always false: [`build`](Self::build) rejects an empty configuration.
    pub fn is_empty(&self) -> bool {
        self.channels.is_empty()
    }

    /// The channel at `index`, which equals its [`ChannelId`].
    ///
    /// # Panics
    ///
    /// When `index >= self.len()`.
    pub fn get(&self, index: usize) -> &Arc<ChannelRuntime> {
        &self.channels[index]
    }

    /// Selection weights, indexed like the channels.
    pub fn weights(&self) -> &[u32] {
        &self.weights
    }

    /// One job per distinct (client profile, origin, method, path).
    pub fn warmup_jobs(&self) -> &[WarmupJob] {
        &self.warmup_jobs
    }

    /// Startup warnings, in channel order.
    pub fn warnings(&self) -> &[ChannelWarning] {
        &self.warnings
    }
}

/// `Authorization: Bearer <key>`, marked sensitive so that hyper's HPACK
/// encoder never indexes it and `Debug` of the header map hides it.
fn bearer(spec: &ChannelSpec) -> Option<Redacted<HeaderValue>> {
    let mut value = HeaderValue::try_from(format!("Bearer {}", spec.api_key.expose())).ok()?;
    value.set_sensitive(true);
    Some(Redacted::new(value))
}

/// Validates the model map and pre-renders every upstream name as the JSON
/// string literal that replaces the `model` value in the request body.
fn model_map(spec: &ChannelSpec) -> Result<HashMap<Box<str>, Bytes>, &'static str> {
    let mut map = HashMap::with_capacity(spec.model_map.len());
    for (client, upstream) in &spec.model_map {
        if client.is_empty() {
            return Err("model_map has an empty client model name");
        }
        if upstream.is_empty() {
            return Err("model_map has an empty upstream model name");
        }
        if !spec.models.is_empty() && !spec.models.contains(client) {
            return Err("model_map maps a model that is not listed in models");
        }
        if map
            .insert(client.as_str().into(), json_string(upstream))
            .is_some()
        {
            return Err("model_map maps the same client model name twice");
        }
    }
    Ok(map)
}

/// Builds the warm-up URL on the channel's origin.
///
/// `set_path` instead of `Url::join`: joining `//169.254.169.254/...` or
/// `/\169.254.169.254/` would replace the host, and an IP-literal host never
/// reaches the resolver that would otherwise refuse it.
fn warmup_url(base: &Url, target: &WarmupTarget) -> Result<Url, &'static str> {
    let path = target.path.as_str();
    if !path.starts_with('/') {
        return Err("path must start with `/`");
    }
    if path.starts_with("//") {
        return Err("path must start with exactly one `/`");
    }
    if path
        .chars()
        .any(|c| matches!(c, '\\' | '?' | '#') || c.is_whitespace() || c.is_control())
    {
        return Err("path must not contain `\\`, `?`, `#`, whitespace or control characters");
    }
    let mut url = base.clone();
    url.set_path(path);
    url.set_query(None);
    url.set_fragment(None);
    if url.origin() != base.origin() {
        return Err("path changes the origin");
    }
    Ok(url)
}

/// A loopback IP literal or `localhost`: plaintext never leaves the machine.
fn is_local_host(url: &Url) -> bool {
    match url.host() {
        Some(Host::Domain(domain)) => domain.eq_ignore_ascii_case("localhost"),
        Some(Host::Ipv4(ip)) => classify(IpAddr::V4(ip)) == AddrClass::Loopback,
        Some(Host::Ipv6(ip)) => classify(IpAddr::V6(ip)) == AddrClass::Loopback,
        None => false,
    }
}

/// `host:port` of `url`, the unit reqwest keeps a connection pool for.
fn authority(url: &Url) -> Box<str> {
    let host = url.host_str().unwrap_or_default();
    match url.port_or_known_default() {
        Some(port) => format!("{host}:{port}").into(),
        None => host.into(),
    }
}

/// Detects hosts reached through more than one client profile.
#[derive(Default)]
struct PoolTracker {
    /// First profile seen per `scheme://host:port`.
    first: HashMap<String, UpstreamClientConfig>,
    warned: HashSet<String>,
}

impl PoolTracker {
    /// The host to warn about, once, when `profile` differs from the first
    /// profile seen for this origin.
    fn observe(&mut self, base: &Url, profile: &UpstreamClientConfig) -> Option<Box<str>> {
        let origin = base.origin().ascii_serialization();
        match self.first.get(&origin) {
            None => {
                self.first.insert(origin, profile.clone());
                None
            }
            Some(first) if first == profile => None,
            Some(_) => self.warned.insert(origin).then(|| authority(base)),
        }
    }
}

/// Collects one warm-up job per (profile, origin, method, path).
#[derive(Default)]
struct WarmupPlanner {
    index: HashMap<(UpstreamClientConfig, String, Method, String), usize>,
    jobs: Vec<WarmupJob>,
}

impl WarmupPlanner {
    fn add(
        &mut self,
        profile: &UpstreamClientConfig,
        client: &Client,
        spec: &ChannelSpec,
        url: Url,
    ) {
        let method = match spec.warmup.method {
            WarmupMethod::Head => Method::HEAD,
            WarmupMethod::Get => Method::GET,
        };
        let key = (
            profile.clone(),
            url.origin().ascii_serialization(),
            method.clone(),
            url.path().to_owned(),
        );
        if let Some(&job) = self.index.get(&key) {
            let names = &mut self.jobs[job].channels;
            *names = format!("{names}, {}", spec.name).into();
            return;
        }
        self.index.insert(key, self.jobs.len());
        self.jobs.push(WarmupJob {
            client: client.clone(),
            method,
            url,
            channels: spec.name.as_str().into(),
        });
    }
}

/// Clients keyed by profile; channels with equal profiles share one pool.
#[derive(Debug, Default)]
pub struct ClientRegistry {
    clients: HashMap<UpstreamClientConfig, Client>,
}

impl ClientRegistry {
    /// The client for `config`, built on first use. `reqwest::Client` is a
    /// handle, so every clone shares the same pool.
    pub fn client(&mut self, config: &UpstreamClientConfig) -> Result<Client, UpstreamError> {
        if let Some(client) = self.clients.get(config) {
            return Ok(client.clone());
        }
        let client = build_client(config)?;
        self.clients.insert(config.clone(), client.clone());
        Ok(client)
    }
}

/// Why [`ChannelSet::build`] rejected the configuration. Never carries an
/// api key.
#[derive(Debug, thiserror::Error)]
pub enum ChannelBuildError {
    /// `channels` is empty.
    #[error("no channels configured")]
    Empty,
    /// More than 64 channels.
    #[error("{0} channels configured; at most 64 are supported")]
    TooMany(usize),
    /// Two channels share a name.
    #[error("duplicate channel name {0:?}")]
    DuplicateName(String),
    /// `weight` is 0.
    #[error("channel {0:?}: weight must be at least 1")]
    ZeroWeight(String),
    /// See [`BaseUrlError`].
    #[error("channel {channel:?}: invalid base_url")]
    BaseUrl {
        /// Channel name.
        channel: String,
        /// What is wrong with the URL.
        #[source]
        source: BaseUrlError,
    },
    /// `Bearer <key>` is not a valid header value (e.g. a line break).
    #[error("channel {0:?}: api_key is not a valid header value")]
    ApiKey(String),
    /// An empty or duplicate name, or a mapping for a model not in `models`.
    #[error("channel {channel:?}: {reason}")]
    ModelMap {
        /// Channel name.
        channel: String,
        /// What is wrong with the map.
        reason: &'static str,
    },
    /// The warm-up path is malformed or would leave the channel's origin.
    #[error("channel {channel:?}: invalid warm-up path: {reason}")]
    Warmup {
        /// Channel name.
        channel: String,
        /// What is wrong with the path.
        reason: &'static str,
    },
    /// See [`UpstreamError`].
    #[error("channel {channel:?}: building the HTTP client failed")]
    Client {
        /// Channel name.
        channel: String,
        /// Why the client could not be built.
        #[source]
        source: UpstreamError,
    },
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;

    use super::*;
    use crate::spec::WarmupTarget;

    const SECRET: &str = "sk-leak-123";

    fn spec(name: &str, base_url: &str) -> ChannelSpec {
        ChannelSpec {
            name: name.to_owned(),
            base_url: base_url.to_owned(),
            api_key: Redacted::new(SECRET.to_owned()),
            weight: 1,
            models: Vec::new(),
            model_map: Vec::new(),
            stream_usage: StreamUsage::default(),
            timeouts: Timeouts::default(),
            client: UpstreamClientConfig {
                allow_private: true,
                ..UpstreamClientConfig::default()
            },
            warmup: WarmupTarget::default(),
            expose_ratelimit_headers: false,
        }
    }

    fn url(raw: &str) -> Url {
        Url::parse(raw).unwrap()
    }

    fn build_err(specs: &[ChannelSpec]) -> ChannelBuildError {
        ChannelSet::build(specs).expect_err("the configuration must be rejected")
    }

    #[test]
    fn chat_url_keeps_the_version_prefix() {
        let cases = [
            (
                "http://192.168.10.180:8317/v1",
                "http://192.168.10.180:8317/v1/chat/completions",
            ),
            (
                "http://192.168.10.180:8317/v1/",
                "http://192.168.10.180:8317/v1/chat/completions",
            ),
            (
                "https://example.com/openai/v1",
                "https://example.com/openai/v1/chat/completions",
            ),
            (
                "https://api.deepseek.com",
                "https://api.deepseek.com/chat/completions",
            ),
            (
                "https://api.deepseek.com/",
                "https://api.deepseek.com/chat/completions",
            ),
        ];
        for (base, expected) in cases {
            assert_eq!(chat_url(&url(base)).as_str(), expected, "{base}");
        }
    }

    #[test]
    fn empty_path_warns_about_the_version_prefix() {
        let set = ChannelSet::build(&[spec("root", "http://127.0.0.1:9000")]).unwrap();
        assert_eq!(
            set.get(0).chat_url.as_str(),
            "http://127.0.0.1:9000/chat/completions"
        );
        assert_eq!(
            set.warnings(),
            [ChannelWarning::NoVersionPath {
                channel: "root".into()
            }]
        );
        let set = ChannelSet::build(&[spec("v1", "http://127.0.0.1:9000/v1")]).unwrap();
        assert!(set.warnings().is_empty(), "{:?}", set.warnings());
    }

    #[test]
    fn plaintext_remote_warning() {
        let set = ChannelSet::build(&[
            spec("cpa", "http://192.168.10.180:8317/v1"),
            spec("loop4", "http://127.0.0.1/v1"),
            spec("loop6", "http://[::1]:8080/v1"),
            spec("name", "http://localhost:8317/v1"),
            spec("tls", "https://192.168.10.181/v1"),
        ])
        .unwrap();
        assert_eq!(
            set.warnings(),
            [ChannelWarning::PlaintextRemote {
                channel: "cpa".into()
            }]
        );
    }

    #[test]
    fn runtime_fields() {
        let mut first = spec("a", "http://127.0.0.1:1/v1");
        first.weight = 3;
        first.stream_usage = StreamUsage::Passthrough;
        first.expose_ratelimit_headers = true;
        first.models = vec!["grok-4.6(xhigh)".to_owned(), "gpt-5.5".to_owned()];
        first.model_map = vec![
            ("grok-4.6(xhigh)".to_owned(), "grok \"4\"\n".to_owned()),
            ("gpt-5.5".to_owned(), "grok-4.6(xhigh)".to_owned()),
        ];
        let set = ChannelSet::build(&[first, spec("b", "http://127.0.0.1:2/v1")]).unwrap();

        assert_eq!(set.len(), 2);
        assert!(!set.is_empty());
        assert_eq!(set.weights(), [3, 1]);
        let a = set.get(0);
        assert_eq!(a.id, ChannelId(0));
        assert_eq!(set.get(1).id, ChannelId(1));
        assert_eq!(&*a.name, "a");
        assert_eq!(
            a.chat_url.as_str(),
            "http://127.0.0.1:1/v1/chat/completions"
        );
        assert_eq!(
            a.auth.expose().as_bytes(),
            format!("Bearer {SECRET}").as_bytes()
        );
        assert!(a.auth.expose().is_sensitive());
        assert_eq!(a.stream_usage, StreamUsage::Passthrough);
        assert!(a.expose_ratelimit_headers);
        assert_eq!(
            a.model_map.get("grok-4.6(xhigh)").map(Bytes::as_ref),
            Some(&b"\"grok \\\"4\\\"\\n\""[..])
        );
        // Bracketed upstream names go out verbatim; only JSON escaping applies.
        assert_eq!(
            a.model_map.get("gpt-5.5").map(Bytes::as_ref),
            Some(&b"\"grok-4.6(xhigh)\""[..])
        );
        assert!(!format!("{a:?}").contains(SECRET));
    }

    #[test]
    fn structural_errors() {
        assert!(matches!(build_err(&[]), ChannelBuildError::Empty));

        let many: Vec<_> = (0..65)
            .map(|i| spec(&format!("c{i}"), "http://127.0.0.1/v1"))
            .collect();
        assert!(matches!(build_err(&many), ChannelBuildError::TooMany(65)));
        ChannelSet::build(&many[..64]).unwrap();

        let dup = [
            spec("a", "http://127.0.0.1/v1"),
            spec("a", "http://127.0.0.1/v1"),
        ];
        assert!(matches!(build_err(&dup), ChannelBuildError::DuplicateName(n) if n == "a"));

        let mut zero = spec("z", "http://127.0.0.1/v1");
        zero.weight = 0;
        assert!(matches!(build_err(&[zero]), ChannelBuildError::ZeroWeight(n) if n == "z"));

        let err = build_err(&[spec("u", "http://127.0.0.1/v1/chat/completions")]);
        assert!(matches!(
            &err,
            ChannelBuildError::BaseUrl { channel, source: BaseUrlError::EndpointPath } if channel == "u"
        ));
        assert!(err.source().is_some());
    }

    #[test]
    fn private_base_url_needs_the_opt_in() {
        let mut strict = spec("s", "http://192.168.10.180:8317/v1");
        strict.client.allow_private = false;
        let err = build_err(&[strict]);
        assert!(matches!(
            err,
            ChannelBuildError::BaseUrl {
                source: BaseUrlError::Forbidden(_),
                ..
            }
        ));
    }

    #[test]
    fn api_key_must_be_a_header_value() {
        let mut bad = spec("k", "http://127.0.0.1/v1");
        bad.api_key = Redacted::new(format!("{SECRET}\n"));
        let err = build_err(&[bad]);
        assert!(matches!(&err, ChannelBuildError::ApiKey(n) if n == "k"));
        assert!(!err.to_string().contains(SECRET));
        assert!(!format!("{err:?}").contains(SECRET));
    }

    #[test]
    fn model_map_errors() {
        let pair = |a: &str, b: &str| (a.to_owned(), b.to_owned());
        let cases = [
            (vec![pair("", "x")], Vec::new()),
            (vec![pair("x", "")], Vec::new()),
            (vec![pair("x", "y"), pair("x", "z")], Vec::new()),
            (vec![pair("x", "y")], vec!["other".to_owned()]),
        ];
        for (map, models) in cases {
            let mut channel = spec("m", "http://127.0.0.1/v1");
            channel.model_map = map.clone();
            channel.models = models;
            let err = build_err(&[channel]);
            assert!(
                matches!(&err, ChannelBuildError::ModelMap { channel, .. } if channel == "m"),
                "{map:?}: {err:?}"
            );
        }
        let mut listed = spec("ok", "http://127.0.0.1/v1");
        listed.models = vec!["x".to_owned()];
        listed.model_map = vec![pair("x", "y")];
        ChannelSet::build(&[listed]).unwrap();
    }

    #[test]
    fn malicious_warmup_paths_are_rejected() {
        for path in [
            "//169.254.169.254/latest/meta-data/",
            "/\\169.254.169.254/",
            "\\169.254.169.254/",
            "healthz",
            "",
            "/healthz?x=1",
            "/healthz#frag",
            "/health z",
            "/health\tz",
            "/health\u{0}z",
            "/health\u{7f}z",
            "/health\u{85}z",
        ] {
            let mut channel = spec("w", "http://127.0.0.1:8317/v1");
            channel.warmup = WarmupTarget {
                method: WarmupMethod::Head,
                path: path.to_owned(),
            };
            let err = build_err(&[channel]);
            assert!(
                matches!(&err, ChannelBuildError::Warmup { channel, .. } if channel == "w"),
                "{path:?}: {err:?}"
            );
        }
    }

    #[test]
    fn warmup_url_stays_on_the_origin() {
        let base = url("http://192.168.10.180:8317/v1");
        for path in ["/healthz", "/", "/v1/models", "/a/../../b", "/%2e%2e/x"] {
            let target = WarmupTarget {
                method: WarmupMethod::Get,
                path: path.to_owned(),
            };
            let warm = warmup_url(&base, &target).unwrap();
            assert_eq!(warm.origin(), base.origin(), "{path}");
            assert_eq!(warm.query(), None);
        }
        let target = WarmupTarget {
            method: WarmupMethod::Head,
            path: "/healthz".to_owned(),
        };
        assert_eq!(
            warmup_url(&base, &target).unwrap().as_str(),
            "http://192.168.10.180:8317/healthz"
        );
    }

    #[test]
    fn warmup_jobs_are_shared_per_origin_and_path() {
        let healthz = WarmupTarget {
            method: WarmupMethod::Head,
            path: "/healthz".to_owned(),
        };
        let mut a = spec("cpa-a", "http://127.0.0.1:8317/v1");
        a.warmup = healthz.clone();
        let mut b = spec("cpa-b", "http://127.0.0.1:8317/v1/");
        b.warmup = healthz;
        let other_port = spec("mock", "http://127.0.0.1:9443/v1");
        let set = ChannelSet::build(&[a, b, other_port]).unwrap();

        let jobs = set.warmup_jobs();
        assert_eq!(jobs.len(), 2, "{jobs:?}");
        assert_eq!(jobs[0].url.as_str(), "http://127.0.0.1:8317/healthz");
        assert_eq!(jobs[0].method, Method::HEAD);
        assert_eq!(&*jobs[0].channels, "cpa-a, cpa-b");
        assert_eq!(jobs[1].url.as_str(), "http://127.0.0.1:9443/");
        assert_eq!(&*jobs[1].channels, "mock");
    }

    #[test]
    fn split_pool_is_reported_once_per_host() {
        let a = spec("a", "http://127.0.0.1:8317/v1");
        let mut b = spec("b", "http://127.0.0.1:8317/v1");
        b.client.connect_timeout = std::time::Duration::from_secs(1);
        let mut c = spec("c", "http://127.0.0.1:8317/v1");
        c.client.connect_timeout = std::time::Duration::from_secs(2);
        let same = spec("d", "http://127.0.0.1:8317/v1");
        let set = ChannelSet::build(&[a, b, c, same]).unwrap();
        assert_eq!(
            set.warnings(),
            [ChannelWarning::SplitPool {
                host: "127.0.0.1:8317".into()
            }]
        );
        // One job per profile: the two extra profiles have their own pools.
        assert_eq!(set.warmup_jobs().len(), 3);
    }

    #[test]
    fn client_registry_builds_once_per_profile() {
        let mut registry = ClientRegistry::default();
        let profile = UpstreamClientConfig::default();
        registry.client(&profile).unwrap();
        registry.client(&profile).unwrap();
        assert_eq!(registry.clients.len(), 1);
        let other = UpstreamClientConfig {
            allow_private: true,
            ..UpstreamClientConfig::default()
        };
        registry.client(&other).unwrap();
        assert_eq!(registry.clients.len(), 2);
    }
}
