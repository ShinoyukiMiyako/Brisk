//! The gateway handle: validates a spec, builds the upstream clients and the
//! configuration snapshot without network I/O, and serves the data plane
//! until shutdown (R1, R10, R16).

use std::cell::Cell;
use std::future::Future;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use hyper_util::service::TowerToHyperService;
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

use crate::auth::{KeyTable, KeyTableError};
use crate::body::{DrainLimits, SettleShared};
use crate::budget::ByteBudget;
use crate::outcome::{Counters, OutcomeSink};
use crate::router::{MatchService, Sanitize, axum_router};
use crate::server::{self, ServerConfig};
use crate::spec::{
    ExperimentSpec, ForwardingSpec, GatewaySpec, LimitsSpec, RouterKind, WarmupSpec,
};
use crate::state::{ModelRoutes, Snapshot, State};
use crate::upstream::registry::{ChannelBuildError, ChannelSet, ChannelWarning};
use crate::upstream::warmup::{Readiness, spawn_warmup};

/// Largest `forwarding.max_attempts`.
const MAX_ATTEMPTS: u8 = 8;

/// A built data plane: the configuration snapshot, the upstream clients and
/// the shared budgets. Cloning shares everything; the request path clones it
/// once per request, in the router.
#[derive(Debug, Clone)]
pub struct Gateway {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    state: State,
    server: ServerConfig,
    limits: LimitsSpec,
    forwarding: ForwardingSpec,
    warmup: WarmupSpec,
    experiments: ExperimentSpec,
    /// In-flight request bodies (`limits.inflight_body_budget`, R20).
    body_budget: ByteBudget,
    /// Handed to every committed body as its one shared reference (D27).
    settle: Arc<SettleShared>,
    /// Filled by the first [`Gateway::serve`]; empty means not ready.
    readiness: OnceLock<Readiness>,
}

/// Why [`Gateway::new`] rejected a spec.
#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    /// The virtual keys cannot form a table.
    #[error(transparent)]
    Keys(#[from] KeyTableError),
    /// A channel is invalid or its client could not be built.
    #[error(transparent)]
    Channels(#[from] ChannelBuildError),
    /// The in-flight budget could never hold a body of `max_body` bytes.
    #[error("inflight_body_budget ({budget}) is smaller than max_body ({max_body})")]
    Budget {
        /// `limits.inflight_body_budget`.
        budget: usize,
        /// `limits.max_body`.
        max_body: u32,
    },
    /// `forwarding.max_attempts` is outside `1..=8`.
    #[error("max_attempts must be in 1..=8, got {0}")]
    MaxAttempts(u8),
}

impl Gateway {
    /// Validates the spec and builds clients. No network I/O and no runtime
    /// needed, so `brisk check-config` can call it. Logs every
    /// `ChannelWarning` at warn level.
    ///
    /// Cross-field checks live here so that every source of a spec gets
    /// them: unique key names and digests, unique channel names, base URLs
    /// under their address policy, at most 64 channels, model maps that only
    /// map served models, a body budget that holds at least one maximal body,
    /// and `max_attempts` in `1..=8`.
    pub fn new(spec: GatewaySpec, outcomes: OutcomeSink) -> Result<Self, BuildError> {
        let GatewaySpec {
            server,
            limits,
            forwarding,
            warmup,
            experiments,
            keys,
            channels,
        } = spec;
        if !(1..=MAX_ATTEMPTS).contains(&forwarding.max_attempts) {
            return Err(BuildError::MaxAttempts(forwarding.max_attempts));
        }
        // A `max_body` that does not fit in `usize` cannot fit in the budget.
        if usize::try_from(limits.max_body).map_or(true, |max| limits.inflight_body_budget < max) {
            return Err(BuildError::Budget {
                budget: limits.inflight_body_budget,
                max_body: limits.max_body,
            });
        }
        let keys = KeyTable::build(&keys)?;
        let channel_set = ChannelSet::build(&channels)?;
        // `ChannelSet::build` bounds the channel count, which the route
        // bitmap relies on.
        let routes = ModelRoutes::build(&channels);
        for warning in channel_set.warnings() {
            log_warning(warning);
        }

        let settle = Arc::new(SettleShared {
            sink: outcomes,
            drain: DrainLimits {
                max_bytes: forwarding.drain_max_bytes,
                timeout: forwarding.drain_timeout,
            },
            tap_budget: ByteBudget::new(limits.response_tap_budget),
        });
        let inner = Inner {
            state: State::new(Snapshot {
                keys,
                channels: channel_set,
                routes,
            }),
            server,
            body_budget: ByteBudget::new(limits.inflight_body_budget),
            limits,
            forwarding,
            warmup,
            experiments,
            settle,
            readiness: OnceLock::new(),
        };
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    /// Starts warm-up, serves until `shutdown` completes, then drains
    /// connections for `graceful_shutdown_timeout` and stops warm-up. Must run
    /// inside a multi-thread Tokio runtime.
    ///
    /// When `shutdown` completes, bodies are told first (so a stream aborted
    /// by the grace period settles as `ShutdownAborted` and starts no drain),
    /// then the listener closes.
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] for a zero warm-up interval
    /// and for the server settings `server::serve` rejects. Serving the same
    /// gateway twice runs two warm-up loops; `/readyz` follows the first.
    pub async fn serve(
        self,
        listener: TcpListener,
        tls: Option<TlsAcceptor>,
        shutdown: impl Future<Output = ()>,
    ) -> io::Result<()> {
        if self.inner.warmup.interval.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "the warm-up interval must be greater than zero",
            ));
        }
        let jobs = self.state().resolve().channels.warmup_jobs().to_vec();
        let (readiness, warmup) = spawn_warmup(jobs, &self.inner.warmup, self.sink().clone());
        // A second `serve` keeps the first readiness; both loops warm the
        // same pools.
        let _ = self.inner.readiness.set(readiness);

        let sink = self.sink().clone();
        let shutdown = async move {
            shutdown.await;
            sink.begin_shutdown();
        };
        let config = self.inner.server.clone();
        let served = match self.inner.experiments.router {
            RouterKind::Axum => {
                let service = Sanitize::new(TowerToHyperService::new(axum_router(self)));
                server::serve(listener, tls, config, service, shutdown).await
            }
            RouterKind::Match => {
                server::serve(listener, tls, config, MatchService::new(self), shutdown).await
            }
        };
        warmup.abort();
        served
    }

    /// Whether `/readyz` answers 200: the first warm-up round finished or
    /// `ready_timeout` elapsed. False before [`serve`](Self::serve).
    pub fn is_ready(&self) -> bool {
        self.inner.readiness.get().is_some_and(Readiness::is_ready)
    }

    /// The two rare-path counters; per-status totals come from `OutcomeTally`.
    pub fn counters(&self) -> &Counters {
        self.sink().counters()
    }

    /// Startup warnings, for `brisk check-config`.
    pub fn warnings(&self) -> &[ChannelWarning] {
        self.state().resolve().channels.warnings()
    }

    /// The configuration snapshot holder.
    pub(crate) fn state(&self) -> &State {
        &self.inner.state
    }

    /// Request-body limits.
    pub(crate) fn limits(&self) -> &LimitsSpec {
        &self.inner.limits
    }

    /// Failover and drain settings.
    pub(crate) fn forwarding(&self) -> &ForwardingSpec {
        &self.inner.forwarding
    }

    /// Experiment switches.
    pub(crate) fn experiments(&self) -> &ExperimentSpec {
        &self.inner.experiments
    }

    /// Budget of in-flight request bodies.
    pub(crate) fn body_budget(&self) -> &ByteBudget {
        &self.inner.body_budget
    }

    /// Settlement state shared by every committed body.
    pub(crate) fn settle(&self) -> &Arc<SettleShared> {
        &self.inner.settle
    }

    /// Where outcomes go.
    pub(crate) fn sink(&self) -> &OutcomeSink {
        &self.inner.settle.sink
    }
}

/// One warn line per startup warning; names and hosts only, never a key.
fn log_warning(warning: &ChannelWarning) {
    match warning {
        ChannelWarning::SplitPool { host } => tracing::warn!(
            %host,
            "the same upstream host is reached through several client profiles, \
             so its connections are split across pools"
        ),
        ChannelWarning::PlaintextRemote { channel } => tracing::warn!(
            %channel,
            "channel uses plain http to a remote host; its key travels in cleartext"
        ),
        ChannelWarning::NoVersionPath { channel } => tracing::warn!(
            %channel,
            "channel base_url has no path, so /chat/completions is requested at the root; \
             a version prefix such as /v1 is usually missing"
        ),
    }
}

/// Bits of a request id that count requests within one thread.
const REQUEST_SEQUENCE_BITS: u32 = 48;
const REQUEST_SEQUENCE_MASK: u64 = (1 << REQUEST_SEQUENCE_BITS) - 1;

/// Next thread ordinal, the high 16 bits of request ids. Starts at 1 so that
/// no request id is 0, which marks an uninitialised thread below.
static NEXT_THREAD_ORDINAL: AtomicU64 = AtomicU64::new(1);

thread_local! {
    /// The next request id this thread hands out; 0 before the first call.
    static NEXT_REQUEST_ID: Cell<u64> = const { Cell::new(0) };
}

/// A process-unique request id, not monotonic across threads.
///
/// The high 16 bits are an ordinal taken from a global counter the first
/// time a thread asks, the low 48 bits count that thread's requests, so the
/// common call touches only thread-local state instead of a cross-core
/// `fetch_add` per request (D27). A thread that exhausts its 48-bit range
/// takes a fresh ordinal.
///
/// # Panics
///
/// When more than 65 535 ordinals have been handed out in the process; the
/// data plane runs on a fixed set of runtime workers, far below that.
pub(crate) fn next_request_id() -> u64 {
    NEXT_REQUEST_ID.with(|next| {
        let mut id = next.get();
        if id & REQUEST_SEQUENCE_MASK == 0 {
            id = take_thread_ordinal() << REQUEST_SEQUENCE_BITS;
        }
        // Past the end of the range the sequence wraps to zero (and the last
        // ordinal wraps to 0), which the branch above replaces on the next
        // call.
        next.set(id.wrapping_add(1));
        id
    })
}

/// Reserves a new thread ordinal.
fn take_thread_ordinal() -> u64 {
    // Only uniqueness matters; no other memory is published through it.
    let ordinal = NEXT_THREAD_ORDINAL.fetch_add(1, Ordering::Relaxed);
    assert!(
        u16::try_from(ordinal).is_ok(),
        "request id thread ordinals exhausted"
    );
    ordinal
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::thread;

    use super::*;

    #[test]
    fn request_ids_are_unique_across_threads() {
        const THREADS: usize = 8;
        const PER_THREAD: usize = 20_000;
        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                thread::spawn(|| {
                    (0..PER_THREAD)
                        .map(|_| next_request_id())
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        let mut all = HashSet::new();
        let mut ordinals = HashSet::new();
        for handle in handles {
            let ids = handle.join().expect("thread panicked");
            let ordinal = ids[0] >> REQUEST_SEQUENCE_BITS;
            assert!(ordinal > 0);
            assert!(
                ids.iter().all(|id| id >> REQUEST_SEQUENCE_BITS == ordinal),
                "one thread keeps one ordinal"
            );
            assert!(ordinals.insert(ordinal), "ordinal {ordinal} reused");
            all.extend(ids);
        }
        assert_eq!(all.len(), THREADS * PER_THREAD);
        assert!(!all.contains(&0));
    }

    #[test]
    fn a_thread_counts_up_within_its_ordinal() {
        let first = next_request_id();
        let second = next_request_id();
        assert_eq!(second, first + 1);
    }

    mod build {
        use std::time::Duration;

        use crate::auth::key_digest;
        use crate::outcome::outcome_channel;
        use crate::secret::Redacted;
        use crate::spec::{
            ChannelSpec, KeySpec, StreamUsage, Timeouts, WarmupMethod, WarmupTarget,
        };
        use crate::upstream::UpstreamClientConfig;
        use crate::upstream::resolver::BaseUrlError;

        use super::super::*;

        fn channel(name: &str, base_url: &str) -> ChannelSpec {
            ChannelSpec {
                name: name.to_owned(),
                base_url: base_url.to_owned(),
                api_key: Redacted::new(String::from("upstream-key")),
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
            }
        }

        fn valid_spec() -> GatewaySpec {
            GatewaySpec {
                server: ServerConfig::default(),
                limits: LimitsSpec::default(),
                forwarding: ForwardingSpec::default(),
                warmup: WarmupSpec::default(),
                experiments: ExperimentSpec::default(),
                keys: vec![KeySpec {
                    name: String::from("local-dev"),
                    sha256: key_digest(b"bk-unit"),
                }],
                channels: vec![channel("cpa-a", "http://127.0.0.1:8317/v1")],
            }
        }

        fn build(spec: GatewaySpec) -> Result<Gateway, BuildError> {
            let (sink, _outcomes) = outcome_channel(4);
            Gateway::new(spec, sink)
        }

        #[test]
        fn a_valid_spec_builds_without_a_runtime() {
            let gateway = build(valid_spec()).expect("a valid spec");
            assert!(!gateway.is_ready(), "not ready before serve");
            assert!(gateway.warnings().is_empty());
            assert_eq!(gateway.state().resolve().channels.len(), 1);
            assert_eq!(gateway.body_budget().available(), 256 << 20);
            assert_eq!(gateway.settle().tap_budget.available(), 256 << 20);
            assert_eq!(gateway.settle().drain.max_bytes, 64 << 10);
        }

        #[test]
        fn max_attempts_must_lie_in_1_to_8() {
            for attempts in [0, 9, u8::MAX] {
                let mut spec = valid_spec();
                spec.forwarding.max_attempts = attempts;
                assert!(
                    matches!(build(spec), Err(BuildError::MaxAttempts(got)) if got == attempts),
                    "{attempts}"
                );
            }
            for attempts in [1, 8] {
                let mut spec = valid_spec();
                spec.forwarding.max_attempts = attempts;
                build(spec).expect("in range");
            }
        }

        #[test]
        fn the_body_budget_must_hold_one_maximal_body() {
            let mut spec = valid_spec();
            spec.limits.max_body = 1024;
            spec.limits.inflight_body_budget = 1023;
            let error = build(spec).expect_err("budget below max_body");
            assert!(matches!(
                error,
                BuildError::Budget {
                    budget: 1023,
                    max_body: 1024
                }
            ));
            assert_eq!(
                error.to_string(),
                "inflight_body_budget (1023) is smaller than max_body (1024)"
            );

            let mut spec = valid_spec();
            spec.limits.max_body = 1024;
            spec.limits.inflight_body_budget = 1024;
            build(spec).expect("a budget of exactly max_body");
        }

        #[test]
        fn key_errors_surface() {
            let mut spec = valid_spec();
            spec.keys.clear();
            assert!(matches!(
                build(spec),
                Err(BuildError::Keys(KeyTableError::Empty))
            ));

            let mut spec = valid_spec();
            let first = spec.keys[0].clone();
            spec.keys.push(first);
            assert!(matches!(
                build(spec),
                Err(BuildError::Keys(KeyTableError::DuplicateName(_)))
            ));
        }

        #[test]
        fn channel_errors_surface() {
            let mut spec = valid_spec();
            spec.channels.clear();
            assert!(matches!(
                build(spec),
                Err(BuildError::Channels(ChannelBuildError::Empty))
            ));

            let mut spec = valid_spec();
            spec.channels[0].client.allow_private = false;
            assert!(matches!(
                build(spec),
                Err(BuildError::Channels(ChannelBuildError::BaseUrl {
                    source: BaseUrlError::Forbidden(_),
                    ..
                }))
            ));

            let mut spec = valid_spec();
            spec.channels[0].model_map = vec![(String::from("unlisted"), String::from("upstream"))];
            assert!(matches!(
                build(spec),
                Err(BuildError::Channels(ChannelBuildError::ModelMap { .. }))
            ));

            let mut spec = valid_spec();
            spec.channels = (0..65)
                .map(|i| channel(&format!("c{i}"), "http://127.0.0.1:8317/v1"))
                .collect();
            assert!(matches!(
                build(spec),
                Err(BuildError::Channels(ChannelBuildError::TooMany(65)))
            ));
        }

        #[test]
        fn channel_warnings_are_exposed() {
            let mut spec = valid_spec();
            spec.channels
                .push(channel("cpa-lan", "http://192.168.10.180:8317/v1"));
            let gateway = build(spec).expect("warnings do not fail the build");
            assert_eq!(
                gateway.warnings(),
                [ChannelWarning::PlaintextRemote {
                    channel: "cpa-lan".into()
                }]
            );
        }

        #[tokio::test]
        async fn serve_rejects_a_zero_warmup_interval() {
            let mut spec = valid_spec();
            spec.warmup.interval = Duration::ZERO;
            let gateway = build(spec).expect("a valid spec");
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind an ephemeral port");
            let error = gateway
                .clone()
                .serve(listener, None, std::future::pending())
                .await
                .expect_err("a zero interval would spin");
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
            assert!(!gateway.is_ready());
        }
    }

    #[test]
    fn an_exhausted_sequence_takes_a_fresh_ordinal() {
        thread::spawn(|| {
            let first = next_request_id();
            let ordinal = first >> REQUEST_SEQUENCE_BITS;
            // Jump to the last id of this ordinal's range.
            NEXT_REQUEST_ID
                .with(|next| next.set((ordinal << REQUEST_SEQUENCE_BITS) | REQUEST_SEQUENCE_MASK));
            let last = next_request_id();
            assert_eq!(last >> REQUEST_SEQUENCE_BITS, ordinal);
            let fresh = next_request_id();
            assert_ne!(fresh >> REQUEST_SEQUENCE_BITS, ordinal);
            assert_eq!(fresh & REQUEST_SEQUENCE_MASK, 0);
        })
        .join()
        .expect("thread panicked");
    }
}
