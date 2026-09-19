//! Plain-data input from which a gateway is built.
//!
//! The `brisk` configuration loader and the tests construct a gateway only
//! through these types, so every semantic check runs on the same data no
//! matter where it came from. `Default` implementations carry the documented
//! configuration defaults; required values (keys, channels, names, URLs and
//! secrets) have none.

use std::time::Duration;

use crate::secret::Redacted;
use crate::server::ServerConfig;
use crate::upstream::UpstreamClientConfig;

/// Index of a key in [`GatewaySpec::keys`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct KeyId(pub u32);

/// Index of a channel in [`GatewaySpec::channels`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ChannelId(pub u16);

/// Everything needed to build a gateway.
#[derive(Debug, Clone)]
pub struct GatewaySpec {
    /// Inbound connection settings.
    pub server: ServerConfig,
    /// Request-body limits and the response retention budget.
    pub limits: LimitsSpec,
    /// Failover and drain settings.
    pub forwarding: ForwardingSpec,
    /// Upstream connection warm-up.
    pub warmup: WarmupSpec,
    /// Experiment switches.
    pub experiments: ExperimentSpec,
    /// Virtual keys; a [`KeyId`] indexes this list.
    pub keys: Vec<KeySpec>,
    /// Upstream channels; a [`ChannelId`] indexes this list.
    pub channels: Vec<ChannelSpec>,
}

/// Limits on inbound request bodies and on response bytes retained for usage
/// parsing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LimitsSpec {
    /// Largest accepted request body, in bytes.
    pub max_body: u32,
    /// Bytes that all in-flight request bodies may hold together; must be at
    /// least `max_body`.
    pub inflight_body_budget: usize,
    /// Deadline for receiving a whole request body, counted from the start of
    /// reading it.
    pub body_read_timeout: Duration,
    /// Minimum progress per `body_min_rate_window` while a body is received.
    pub body_min_rate_bytes: u32,
    /// Window over which `body_min_rate_bytes` is enforced.
    pub body_min_rate_window: Duration,
    /// Global cap on bytes that `ResponseTap` retains for usage parsing.
    pub response_tap_budget: usize,
}

impl Default for LimitsSpec {
    fn default() -> Self {
        Self {
            max_body: 32 << 20,
            inflight_body_budget: 256 << 20,
            body_read_timeout: Duration::from_secs(60),
            body_min_rate_bytes: 64 << 10,
            body_min_rate_window: Duration::from_secs(10),
            response_tap_budget: 256 << 20,
        }
    }
}

/// Failover and drain settings shared by every channel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForwardingSpec {
    /// Upstream attempts per request, in `1..=8`; the number of candidate
    /// channels caps it further.
    pub max_attempts: u8,
    /// Longest bounded drain after the client left, and longest background
    /// read of an abandoned small response.
    pub drain_timeout: Duration,
    /// Most bytes a bounded drain reads, and the largest abandoned non-2xx
    /// response that is read to the end so its connection returns to the pool.
    pub drain_max_bytes: u32,
}

impl Default for ForwardingSpec {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            drain_timeout: Duration::from_secs(2),
            drain_max_bytes: 64 << 10,
        }
    }
}

/// Per-channel upstream deadlines, already merged with the global defaults.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timeouts {
    /// How long a 2xx SSE response waits before commit for its first data
    /// event; `ZERO` commits at the 2xx head (E4).
    pub commit_hold: Duration,
    /// From sending the request to the first response-body byte.
    pub first_byte: Duration,
    /// Longest silence between response-body bytes after the first.
    pub idle: Duration,
}

impl Default for Timeouts {
    fn default() -> Self {
        Self {
            commit_hold: Duration::from_secs(2),
            first_byte: Duration::from_secs(600),
            idle: Duration::from_secs(120),
        }
    }
}

/// Schedule of the warm-up requests that keep upstream connections pooled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WarmupSpec {
    /// Pause between warm-up rounds.
    pub interval: Duration,
    /// The gateway reports ready once the first round finished or this long
    /// after start, whichever comes first.
    pub ready_timeout: Duration,
    /// Deadline of each warm-up request.
    pub request_timeout: Duration,
}

impl Default for WarmupSpec {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(60),
            ready_timeout: Duration::from_secs(3),
            request_timeout: Duration::from_secs(10),
        }
    }
}

/// Switches between the implementations compared by experiments.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ExperimentSpec {
    /// Request router (E1).
    pub router: RouterKind,
    /// Upstream body form of a rewritten request (E2).
    pub splice: SpliceMode,
}

/// Request router implementation (E1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RouterKind {
    /// An `axum::Router` with raw-request handlers and no middleware.
    #[default]
    Axum,
    /// A static match on method and path.
    Match,
}

/// Upstream body form of a rewritten request (E2); an unmodified body is
/// always sent as-is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SpliceMode {
    /// The splice segments, written without copying.
    #[default]
    Segments,
    /// One contiguous copy of the rewritten body.
    Concat,
}

/// A virtual key as stored in the configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeySpec {
    /// Unique name, used in logs instead of the key.
    pub name: String,
    /// SHA-256 of the whole key string; the key itself is never stored.
    pub sha256: [u8; 32],
}

/// One upstream channel.
#[derive(Debug, Clone)]
pub struct ChannelSpec {
    /// Unique name, used in logs instead of the URL or key.
    pub name: String,
    /// Includes the version prefix, e.g. `http://192.168.10.180:8317/v1` (D1).
    pub base_url: String,
    /// Upstream credential, sent as `Authorization: Bearer <key>`.
    pub api_key: Redacted<String>,
    /// Relative selection weight; at least 1.
    pub weight: u32,
    /// Client-facing model names served; empty means any model.
    pub models: Vec<String>,
    /// Client model name -> upstream model name.
    pub model_map: Vec<(String, String)>,
    /// How usage is obtained from streamed responses (D8).
    pub stream_usage: StreamUsage,
    /// Deadlines, already merged with the global defaults.
    pub timeouts: Timeouts,
    /// HTTP client profile; channels with equal profiles share one client.
    pub client: UpstreamClientConfig,
    /// Warm-up request sent to the channel's origin.
    pub warmup: WarmupTarget,
    /// Also forward the upstream's rate-limit response headers.
    pub expose_ratelimit_headers: bool,
}

/// How usage is obtained from streamed responses (D8).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StreamUsage {
    /// Ask for usage when a streaming client did not, and strip the usage-only
    /// events the client did not ask for.
    #[default]
    Inject,
    /// Forward the request unchanged and use whatever usage the upstream sends.
    Passthrough,
}

/// Warm-up request of a channel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WarmupTarget {
    /// Request method.
    pub method: WarmupMethod,
    /// Absolute path on the channel's origin, e.g. `/healthz`. Exactly one
    /// leading `/`; no `\`, `?`, `#`, spaces or control characters (checked
    /// by `ChannelSet::build`).
    pub path: String,
}

impl Default for WarmupTarget {
    fn default() -> Self {
        Self {
            method: WarmupMethod::default(),
            path: String::from("/"),
        }
    }
}

/// Method of a warm-up request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WarmupMethod {
    /// `HEAD`; the response has no body to read.
    #[default]
    Head,
    /// `GET`, for upstreams that answer `HEAD` with a body.
    Get,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limits_default_matches_configuration_defaults() {
        let limits = LimitsSpec::default();
        assert_eq!(limits.max_body, 32 * 1024 * 1024);
        assert_eq!(limits.inflight_body_budget, 256 * 1024 * 1024);
        assert_eq!(limits.body_read_timeout, Duration::from_secs(60));
        assert_eq!(limits.body_min_rate_bytes, 64 * 1024);
        assert_eq!(limits.body_min_rate_window, Duration::from_secs(10));
        assert_eq!(limits.response_tap_budget, 256 * 1024 * 1024);
        assert!(limits.inflight_body_budget >= limits.max_body as usize);
    }

    #[test]
    fn forwarding_default_matches_configuration_defaults() {
        let forwarding = ForwardingSpec::default();
        assert_eq!(forwarding.max_attempts, 3);
        assert_eq!(forwarding.drain_timeout, Duration::from_secs(2));
        assert_eq!(forwarding.drain_max_bytes, 64 * 1024);
    }

    #[test]
    fn timeouts_default_matches_configuration_defaults() {
        let timeouts = Timeouts::default();
        assert_eq!(timeouts.commit_hold, Duration::from_secs(2));
        assert_eq!(timeouts.first_byte, Duration::from_secs(600));
        assert_eq!(timeouts.idle, Duration::from_secs(120));
    }

    #[test]
    fn warmup_defaults_match_configuration_defaults() {
        let warmup = WarmupSpec::default();
        assert_eq!(warmup.interval, Duration::from_secs(60));
        assert_eq!(warmup.ready_timeout, Duration::from_secs(3));
        assert_eq!(warmup.request_timeout, Duration::from_secs(10));

        let target = WarmupTarget::default();
        assert_eq!(target.method, WarmupMethod::Head);
        assert_eq!(target.path, "/");
    }

    #[test]
    fn enum_defaults_match_configuration_defaults() {
        let experiments = ExperimentSpec::default();
        assert_eq!(experiments.router, RouterKind::Axum);
        assert_eq!(experiments.splice, SpliceMode::Segments);
        assert_eq!(StreamUsage::default(), StreamUsage::Inject);
    }

    #[test]
    fn channel_spec_debug_hides_the_api_key() {
        let channel = ChannelSpec {
            name: String::from("cpa-a"),
            base_url: String::from("http://192.168.10.180:8317/v1"),
            api_key: Redacted::new(String::from("sk-leak-123")),
            weight: 1,
            models: vec![String::from("grok-4.6(xhigh)")],
            model_map: Vec::new(),
            stream_usage: StreamUsage::Passthrough,
            timeouts: Timeouts::default(),
            client: UpstreamClientConfig {
                allow_private: true,
                ..UpstreamClientConfig::default()
            },
            warmup: WarmupTarget {
                method: WarmupMethod::Head,
                path: String::from("/healthz"),
            },
            expose_ratelimit_headers: false,
        };
        let debug = format!("{channel:?}");
        assert!(!debug.contains("sk-leak-123"), "{debug}");
        assert!(debug.contains("api_key: ***"), "{debug}");
    }
}
