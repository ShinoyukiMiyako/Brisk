//! Settlement of committed responses (sections 2.6 and 2.7).
//!
//! A committed body settles exactly once, at EOF, on an error or on drop,
//! whichever comes first. [`SettleCtx::settle`] consumes the context, so a
//! second emission does not type-check. The billed amount always comes from
//! [`outcome::billed`](crate::outcome::billed); this module only gathers its
//! input and decides what a dropped body does.

use std::sync::Arc;
use std::time::Duration;

use brisk_proto::{UsageErrorKind, UsageTokens};
use bytes::Bytes;
use http_body::Body;
use http_body_util::BodyExt;
use tokio::runtime::Handle;
use tokio::time::Instant;

use super::HeldFrames;
use super::chunk::StreamScan;
use crate::budget::ByteBudget;
use crate::outcome::{self, Outcome, OutcomeSink, OutcomeStatus, SettleInput};
use crate::spec::{ChannelId, KeyId};

/// Bounds of the drain that runs after a client leaves between
/// `finish_reason` and the usage (section 2.7).
#[derive(Debug, Clone, Copy)]
pub struct DrainLimits {
    /// Most upstream bytes the drain reads (`forwarding.drain_max_bytes`).
    pub max_bytes: u32,
    /// Longest time the drain runs (`forwarding.drain_timeout`).
    pub timeout: Duration,
}

/// Per-gateway settlement state shared by every committed body; built once
/// by `Gateway::new`, so a request clones one `Arc` and nothing else (D27).
#[derive(Debug)]
pub struct SettleShared {
    /// Where every committed body emits its `Outcome`.
    pub sink: OutcomeSink,
    /// Bounds of the post-cancel drain.
    pub drain: DrainLimits,
    /// Global cap on bytes retained by `ResponseTap` (`limits.response_tap_budget`).
    pub tap_budget: ByteBudget,
}

/// Everything a committed body needs to settle; built by `forward`.
#[derive(Debug)]
pub struct SettleCtx {
    /// Gateway-wide settlement state.
    pub shared: Arc<SettleShared>,
    /// Process-unique request identifier.
    pub request_id: u64,
    /// The authenticated key.
    pub key: KeyId,
    /// The committed channel.
    pub channel: ChannelId,
    /// Upstream attempts made, including the committed one.
    pub attempts: u8,
    /// The client asked for a streamed response.
    pub stream: bool,
    /// Status sent downstream.
    pub http_status: u16,
    /// Request body bytes received from the client.
    pub request_bytes: u64,
    /// Arrival of the request.
    pub started: Instant,
    /// Commit of the response.
    pub committed: Instant,
}

/// How a committed body ended, with the usage facts it gathered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Ending {
    /// How the response ended.
    pub(crate) status: OutcomeStatus,
    /// Last usage parsed successfully.
    pub(crate) usage: Option<UsageTokens>,
    /// The usage is final (D21).
    pub(crate) usage_final: bool,
    /// Kind of the first usage candidate that failed to parse.
    pub(crate) usage_error: Option<UsageErrorKind>,
    /// Body bytes forwarded downstream.
    pub(crate) response_bytes: u64,
}

impl SettleCtx {
    /// Bills the request with [`outcome::billed`] and emits its one
    /// `Outcome`. Never blocks or awaits, so it may run in `Drop` (R15).
    pub(crate) fn settle(self, ending: Ending) {
        let record = self.outcome(&ending, Instant::now());
        self.shared.sink.emit(record);
    }

    /// The record [`settle`](Self::settle) emits when the body ends at `now`.
    fn outcome(&self, ending: &Ending, now: Instant) -> Outcome {
        let estimate = outcome::estimate(self.request_bytes, ending.response_bytes);
        let billed = outcome::billed(&SettleInput {
            status: ending.status,
            usage: ending.usage,
            usage_final: ending.usage_final,
            usage_error: ending.usage_error.is_some(),
            estimate,
            before_commit: false,
        });
        Outcome {
            request_id: self.request_id,
            key: Some(self.key),
            channel: Some(self.channel),
            attempts: self.attempts,
            stream: self.stream,
            status: ending.status,
            http_status: Some(self.http_status),
            usage: ending.usage,
            usage_error: ending.usage_error,
            estimate,
            billed,
            request_bytes: self.request_bytes,
            response_bytes: ending.response_bytes,
            elapsed: now.saturating_duration_since(self.started),
            to_commit: Some(self.committed.saturating_duration_since(self.started)),
        }
    }
}

/// What a body dropped before it settled does (section 2.7, rules 2 to 4).
#[derive(Debug)]
pub(crate) enum DropPlan {
    /// Graceful shutdown is under way: settle as `ShutdownAborted`, no drain.
    ShutdownAborted,
    /// Hand the upstream body to a drain task spawned on this runtime.
    Drain(Handle),
    /// Drop the upstream body, closing the connection, and settle as
    /// `ClientCancelled`.
    Cancelled,
}

impl SettleShared {
    /// Chooses the drop path. `drain_wanted` is true when the stream has
    /// seen `finish_reason` without final usage; `ResponseTap` always passes
    /// false (D16). A drop outside any runtime cannot spawn and cancels.
    pub(crate) fn plan_drop(&self, drain_wanted: bool) -> DropPlan {
        if self.sink.is_shutting_down() {
            return DropPlan::ShutdownAborted;
        }
        if !drain_wanted {
            return DropPlan::Cancelled;
        }
        match Handle::try_current() {
            Ok(handle) => DropPlan::Drain(handle),
            Err(_) => DropPlan::Cancelled,
        }
    }
}

/// The bounded drain of section 2.7, rule 3: the client left after
/// `finish_reason` but before a final usage. Reads on without forwarding,
/// first the frames still `held` from before commit, then the upstream,
/// until `[DONE]`, EOF, an error, `drain.max_bytes` or `drain.timeout`,
/// whichever comes first; then settles as `Drained` if a usage arrived and
/// as `ClientCancelled` otherwise. `response_bytes` were sent before the
/// client left.
pub(crate) async fn drain<B>(
    mut upstream: B,
    mut held: HeldFrames,
    mut scan: StreamScan,
    ctx: SettleCtx,
    response_bytes: u64,
) where
    B: Body<Data = Bytes> + Unpin,
{
    let limits = ctx.shared.drain;
    let read = async {
        let mut read: u64 = 0;
        while !scan.facts.done && read < u64::from(limits.max_bytes) {
            let data = if let Some(frame) = held.pop_front() {
                frame
            } else {
                let next = upstream.frame().await;
                match next {
                    Some(Ok(frame)) => match frame.into_data() {
                        Ok(data) => data,
                        Err(_trailers) => continue,
                    },
                    Some(Err(_)) | None => break,
                }
            };
            read += data.len() as u64;
            if scan.scan(&data).is_err() {
                break;
            }
        }
    };
    // Running out of time ends the drain the way running out of bytes does;
    // either way the settlement below decides from what was read.
    let _elapsed = tokio::time::timeout(limits.timeout, read).await;

    // The drain starts only after `finish_reason`, so any usage it read is
    // final (D21), and none was final before it started.
    let facts = &scan.facts;
    let status = if facts.usage_final {
        OutcomeStatus::Drained
    } else {
        OutcomeStatus::ClientCancelled
    };
    ctx.settle(Ending {
        status,
        usage: facts.usage.get(),
        usage_final: facts.usage_final,
        usage_error: facts.usage_error,
        response_bytes,
    });
}

#[cfg(test)]
mod tests {
    use serde_json::error::Category;

    use super::*;
    use crate::outcome::{OutcomeReceiver, outcome_channel};

    /// `chat-stream-grok46-xhigh`: usage arrives with `finish_reason` in the
    /// CPA final chunk.
    const GROK_USAGE: UsageTokens = UsageTokens {
        input: 213,
        output: 71,
        cached_input: Some(0),
        reasoning_output: Some(70),
    };

    fn shared() -> (Arc<SettleShared>, OutcomeReceiver) {
        let (sink, rx) = outcome_channel(8);
        let shared = SettleShared {
            sink,
            drain: DrainLimits {
                max_bytes: 64 << 10,
                timeout: Duration::from_secs(2),
            },
            tap_budget: ByteBudget::new(1 << 20),
        };
        (Arc::new(shared), rx)
    }

    fn ctx(shared: &Arc<SettleShared>, started: Instant, committed: Instant) -> SettleCtx {
        SettleCtx {
            shared: Arc::clone(shared),
            request_id: 42,
            key: KeyId(3),
            channel: ChannelId(1),
            attempts: 2,
            stream: true,
            http_status: 200,
            request_bytes: 852,
            started,
            committed,
        }
    }

    fn ending(status: OutcomeStatus) -> Ending {
        Ending {
            status,
            usage: None,
            usage_final: false,
            usage_error: None,
            response_bytes: 4284,
        }
    }

    fn settle_one(ending: Ending) -> Outcome {
        let (shared, mut rx) = shared();
        let now = Instant::now();
        ctx(&shared, now, now).settle(ending);
        let record = rx.try_recv().expect("settle emits one outcome");
        assert!(rx.try_recv().is_err(), "settle emits exactly one outcome");
        assert_eq!(
            shared
                .sink
                .counters()
                .outcomes_dropped
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        record
    }

    #[tokio::test(start_paused = true)]
    async fn outcome_carries_context_and_timing() {
        let (shared, _rx) = shared();
        let started = Instant::now();
        let committed = started + Duration::from_millis(150);
        let now = started + Duration::from_millis(900);
        let record = ctx(&shared, started, committed).outcome(
            &Ending {
                usage: Some(GROK_USAGE),
                ..ending(OutcomeStatus::Completed)
            },
            now,
        );
        assert_eq!(record.request_id, 42);
        assert_eq!(record.key, Some(KeyId(3)));
        assert_eq!(record.channel, Some(ChannelId(1)));
        assert_eq!(record.attempts, 2);
        assert!(record.stream);
        assert_eq!(record.status, OutcomeStatus::Completed);
        assert_eq!(record.http_status, Some(200));
        assert_eq!(record.usage, Some(GROK_USAGE));
        assert_eq!(record.usage_error, None);
        assert_eq!(record.request_bytes, 852);
        assert_eq!(record.response_bytes, 4284);
        assert_eq!(record.estimate, outcome::estimate(852, 4284));
        assert_eq!(record.billed, GROK_USAGE);
        assert_eq!(record.elapsed, Duration::from_millis(900));
        assert_eq!(record.to_commit, Some(Duration::from_millis(150)));
    }

    #[test]
    fn completed_without_usage_bills_the_estimate() {
        let record = settle_one(ending(OutcomeStatus::Completed));
        assert_eq!(record.estimate.input, 213);
        assert_eq!(record.estimate.output, 1071);
        assert_eq!(record.billed, record.estimate);
    }

    #[test]
    fn final_usage_is_billed_exactly_after_truncation() {
        // The grok46-xhigh stream cut after its final chunk, before `[DONE]`:
        // the byte estimate would bill about 15 times the real output (D21).
        for status in [
            OutcomeStatus::UpstreamTruncated,
            OutcomeStatus::ClientCancelled,
            OutcomeStatus::IdleTimeout,
        ] {
            let record = settle_one(Ending {
                usage: Some(GROK_USAGE),
                usage_final: true,
                ..ending(status)
            });
            assert_eq!(record.billed.input, 213);
            assert_eq!(record.billed.output, 71);
        }
    }

    #[test]
    fn provisional_usage_is_billed_conservatively() {
        let record = settle_one(Ending {
            usage: Some(GROK_USAGE),
            ..ending(OutcomeStatus::ClientCancelled)
        });
        assert_eq!(record.billed.input, 213);
        assert_eq!(record.billed.output, 1071);
    }

    #[test]
    fn usage_error_is_reported_and_billed_conservatively() {
        let kind = UsageErrorKind::MissingField("prompt_tokens");
        let record = settle_one(Ending {
            usage_error: Some(kind),
            ..ending(OutcomeStatus::Completed)
        });
        assert_eq!(record.usage_error, Some(kind));
        assert_eq!(record.usage, None);
        assert_eq!(record.billed, record.estimate);

        let kind = UsageErrorKind::Json {
            category: Category::Data,
            column: 12,
        };
        let record = settle_one(Ending {
            usage: Some(UsageTokens {
                input: 1000,
                output: 1,
                ..UsageTokens::default()
            }),
            usage_final: true,
            usage_error: Some(kind),
            ..ending(OutcomeStatus::Completed)
        });
        assert_eq!(record.usage_error, Some(kind));
        assert_eq!(record.billed.input, 1000);
        assert_eq!(record.billed.output, 1071);
    }

    #[test]
    fn error_only_response_bills_nothing() {
        let record = settle_one(ending(OutcomeStatus::ForwardedError { status: 200 }));
        assert_eq!(record.billed, UsageTokens::default());
    }

    #[test]
    fn drained_usage_is_billed() {
        let record = settle_one(Ending {
            usage: Some(GROK_USAGE),
            ..ending(OutcomeStatus::Drained)
        });
        assert_eq!(record.status, OutcomeStatus::Drained);
        assert_eq!(record.billed, GROK_USAGE);
    }

    #[tokio::test]
    async fn drop_plan_drains_only_when_wanted() {
        let (shared, _rx) = shared();
        assert!(matches!(shared.plan_drop(true), DropPlan::Drain(_)));
        assert!(matches!(shared.plan_drop(false), DropPlan::Cancelled));
    }

    #[tokio::test]
    async fn drop_plan_during_shutdown_never_drains() {
        let (shared, _rx) = shared();
        shared.sink.begin_shutdown();
        assert!(matches!(shared.plan_drop(true), DropPlan::ShutdownAborted));
        assert!(matches!(shared.plan_drop(false), DropPlan::ShutdownAborted));
    }

    #[test]
    fn drop_plan_outside_a_runtime_cancels() {
        let (shared, _rx) = shared();
        assert!(matches!(shared.plan_drop(true), DropPlan::Cancelled));
    }
}
