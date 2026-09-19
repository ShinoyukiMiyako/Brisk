//! Settlement records and the counters that cannot be derived from them.
//!
//! Every request produces exactly one [`Outcome`]: `forward` emits it for
//! requests that end before commit, the committed body emits it afterwards,
//! and both compute the billed amount with [`billed`], the only implementation
//! of the settlement table. Records travel over a bounded queue to the logger
//! task. Emitting never blocks or awaits (R15), and per-status totals are
//! summed from the records by [`OutcomeTally`] instead of being kept in shared
//! counters written once per request (D27).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use brisk_proto::{UsageErrorKind, UsageTokens};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;

use crate::spec::{ChannelId, KeyId};

/// Rough tokenizer used by conservative settlement.
pub const BYTES_PER_TOKEN_ESTIMATE: u64 = 4;

/// Final record of one request; emitted exactly once per request.
#[derive(Debug, Clone)]
pub struct Outcome {
    /// Process-unique request identifier.
    pub request_id: u64,
    /// The authenticated key; `None` when authentication failed or never ran.
    pub key: Option<KeyId>,
    /// The committed channel, or the last one tried.
    pub channel: Option<ChannelId>,
    /// Upstream attempts made, including the committed one.
    pub attempts: u8,
    /// The client asked for a streamed response.
    pub stream: bool,
    /// How the request ended.
    pub status: OutcomeStatus,
    /// Status sent downstream; `None` when the client left before a response head.
    pub http_status: Option<u16>,
    /// Last usage reported by the upstream and parsed successfully, if any.
    pub usage: Option<UsageTokens>,
    /// Set when a usage candidate failed to parse; settlement was then
    /// conservative (section 2.6). The logger warns on every such record.
    pub usage_error: Option<UsageErrorKind>,
    /// See [`estimate`].
    pub estimate: UsageTokens,
    /// Result of [`billed`].
    pub billed: UsageTokens,
    /// Request body bytes received from the client.
    pub request_bytes: u64,
    /// Body bytes forwarded downstream.
    pub response_bytes: u64,
    /// Request arrival to the emission of this record.
    pub elapsed: Duration,
    /// Request arrival to commit.
    pub to_commit: Option<Duration>,
}

/// How a request ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutcomeStatus {
    /// `[DONE]` seen (SSE) or the JSON body ended.
    Completed,
    /// A committed upstream error was forwarded: a non-2xx response, or a 2xx
    /// whose first data event or JSON body is an error (D22).
    ForwardedError {
        /// Status sent downstream.
        status: u16,
    },
    /// Committed; the upstream ended without `[DONE]` or failed mid-body.
    UpstreamTruncated,
    /// Committed; an SSE event exceeded `MAX_EVENT_BYTES`.
    UpstreamProtocol,
    /// Committed; the upstream stayed silent past the idle timeout.
    IdleTimeout,
    /// Committed at `commit_hold` expiry, then no byte before the deadline.
    FirstByteTimeout,
    /// The client left; before commit when `http_status` is `None`.
    ClientCancelled,
    /// Client left after `finish_reason`; the bounded drain found usage.
    Drained,
    /// Graceful shutdown ran out of time while the response was in flight.
    ShutdownAborted,
    /// No commit: every attempt failed; Brisk answered 502 or 504.
    Failed(FailureClass),
    /// Brisk answered the request itself without a usable upstream response.
    Rejected(RejectReason),
}

/// Why an upstream attempt failed before commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureClass {
    /// The connection could not be established, including addresses the
    /// resolver refused.
    Connect,
    /// Sending the request or reading the response head failed.
    Transport,
    /// No response head, or no body byte of a 2xx SSE response, before the
    /// first-byte deadline.
    FirstByteTimeout,
    /// A 3xx response; redirects are never followed.
    Redirect,
    /// 401 or 403: the upstream rejected the gateway's credentials.
    UpstreamAuth,
    /// 408, 429 or 5xx (a final 408 becomes 504, D3).
    Status,
    /// The first data event of a 2xx SSE response carried a top-level `error`.
    FirstEventError,
    /// A 2xx SSE response ended before any data event.
    EmptyStream,
    /// A 2xx SSE response sent an event larger than `MAX_EVENT_BYTES` before commit.
    Protocol,
}

/// Why Brisk answered a request itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectReason {
    /// No credential header.
    MissingKey,
    /// A malformed, ambiguous or unknown key.
    InvalidKey,
    /// Unknown path.
    NotFound,
    /// Known path, wrong method.
    MethodNotAllowed,
    /// The request body is not an acceptable Chat Completions request.
    BadRequest,
    /// `Content-Encoding` other than `identity` (D29).
    UnsupportedEncoding,
    /// No channel serves the requested model.
    ModelNotFound,
    /// The request body exceeds `max_body`.
    BodyTooLarge,
    /// The request body did not arrive within `body_read_timeout`.
    BodyTimeout,
    /// The request body arrived slower than the minimum rate.
    BodyTooSlow,
    /// The in-flight body budget is exhausted.
    Overloaded,
    /// The client aborted the request body; no response was written.
    ClientAborted,
    /// An internal invariant failed (500 `internal_error`).
    Internal,
}

/// `input = ceil(request_bytes / 4)`, `output = ceil(response_bytes / 4)`.
pub fn estimate(request_bytes: u64, response_bytes: u64) -> UsageTokens {
    UsageTokens {
        input: request_bytes.div_ceil(BYTES_PER_TOKEN_ESTIMATE),
        output: response_bytes.div_ceil(BYTES_PER_TOKEN_ESTIMATE),
        cached_input: None,
        reasoning_output: None,
    }
}

/// Everything settlement looks at; built by `forward` (before commit) or by
/// the body (after commit). Section 2.6 is the specification.
#[derive(Debug, Clone, Copy)]
pub struct SettleInput {
    /// How the request ended.
    pub status: OutcomeStatus,
    /// Last usage parsed successfully.
    pub usage: Option<UsageTokens>,
    /// Usage is final (D21): it arrived in the same event as `finish_reason`,
    /// or after an event carrying `finish_reason`.
    pub usage_final: bool,
    /// Some usage candidate failed to parse.
    pub usage_error: bool,
    /// See [`estimate`].
    pub estimate: UsageTokens,
    /// The request ended before commit (forward's drop guard).
    pub before_commit: bool,
}

/// The only implementation of the table in section 2.6; `forward` and
/// `body::settle` both call it.
///
/// The rows are checked top to bottom and the first match wins.
pub fn billed(input: &SettleInput) -> UsageTokens {
    let conservative = || {
        input
            .usage
            .map_or(input.estimate, |usage| usage.max_billable(input.estimate))
    };
    match input.status {
        OutcomeStatus::Failed(_) | OutcomeStatus::Rejected(_) => UsageTokens::default(),
        // Non-2xx bodies are never parsed and a 2xx that carried only an error
        // generated nothing (D22), so zero is the bill unless usage appeared.
        OutcomeStatus::ForwardedError { .. } => input.usage.unwrap_or_default(),
        // The upstream may already have started generating: the prompt is
        // billed, the output that never reached the client is not.
        _ if input.before_commit => UsageTokens {
            input: input.estimate.input,
            ..UsageTokens::default()
        },
        _ if input.usage_error => conservative(),
        OutcomeStatus::Completed => input.usage.unwrap_or(input.estimate),
        _ => match (input.status, input.usage) {
            (OutcomeStatus::Drained, Some(usage)) => usage,
            // D21: final usage is exact; the byte estimate would overbill.
            (_, Some(usage)) if input.usage_final => usage,
            _ => conservative(),
        },
    }
}

/// Process-wide counters that cannot be derived from `Outcome`s. Written
/// only on rare paths, never once per request (D27).
#[derive(Debug, Default)]
pub struct Counters {
    /// Outcomes lost because the queue was full or closed.
    pub outcomes_dropped: AtomicU64,
    /// Failed warm-up requests.
    pub warmup_failures: AtomicU64,
}

/// Running totals over received `Outcome`s. The logger keeps one; tests
/// build one from the `OutcomeReceiver` instead of reading shared counters.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct OutcomeTally {
    /// Every recorded outcome.
    pub requests: u64,
    /// [`OutcomeStatus::Completed`].
    pub completed: u64,
    /// [`OutcomeStatus::ForwardedError`].
    pub forwarded_errors: u64,
    /// [`OutcomeStatus::UpstreamTruncated`].
    pub truncated: u64,
    /// [`OutcomeStatus::UpstreamProtocol`].
    pub protocol_errors: u64,
    /// [`OutcomeStatus::IdleTimeout`].
    pub idle_timeouts: u64,
    /// [`OutcomeStatus::FirstByteTimeout`].
    pub first_byte_timeouts: u64,
    /// [`OutcomeStatus::ClientCancelled`].
    pub client_cancelled: u64,
    /// [`OutcomeStatus::Drained`].
    pub drained: u64,
    /// [`OutcomeStatus::ShutdownAborted`].
    pub shutdown_aborted: u64,
    /// [`OutcomeStatus::Failed`].
    pub failed: u64,
    /// [`OutcomeStatus::Rejected`].
    pub rejected: u64,
    /// Sum of `attempts - 1`.
    pub failovers: u64,
    /// Committed 2xx without a usable usage (absent or failed to parse),
    /// excluding `ForwardedError`.
    pub usage_missing: u64,
    /// Outcomes with `usage_error`.
    pub usage_parse_errors: u64,
    /// Sum of `billed.input`.
    pub billed_input: u64,
    /// Sum of `billed.output`.
    pub billed_output: u64,
}

impl OutcomeTally {
    /// Adds one outcome to the totals.
    pub fn record(&mut self, outcome: &Outcome) {
        self.requests += 1;
        let status_count = match outcome.status {
            OutcomeStatus::Completed => &mut self.completed,
            OutcomeStatus::ForwardedError { .. } => &mut self.forwarded_errors,
            OutcomeStatus::UpstreamTruncated => &mut self.truncated,
            OutcomeStatus::UpstreamProtocol => &mut self.protocol_errors,
            OutcomeStatus::IdleTimeout => &mut self.idle_timeouts,
            OutcomeStatus::FirstByteTimeout => &mut self.first_byte_timeouts,
            OutcomeStatus::ClientCancelled => &mut self.client_cancelled,
            OutcomeStatus::Drained => &mut self.drained,
            OutcomeStatus::ShutdownAborted => &mut self.shutdown_aborted,
            OutcomeStatus::Failed(_) => &mut self.failed,
            OutcomeStatus::Rejected(_) => &mut self.rejected,
        };
        *status_count += 1;
        self.failovers += u64::from(outcome.attempts.saturating_sub(1));
        if is_committed_success(outcome)
            && (outcome.usage.is_none() || outcome.usage_error.is_some())
        {
            self.usage_missing += 1;
        }
        if outcome.usage_error.is_some() {
            self.usage_parse_errors += 1;
        }
        // Token counts come from the upstream's JSON, so a hostile or broken
        // upstream can report values near `u64::MAX`. A saturated total is
        // visibly absurd in the log; a panic would kill the logger task and a
        // wrapped total would look plausible.
        self.billed_input = self.billed_input.saturating_add(outcome.billed.input);
        self.billed_output = self.billed_output.saturating_add(outcome.billed.output);
    }
}

/// A 2xx upstream response reached the client. `Failed` and `Rejected` carry
/// Brisk's own status, and a forwarded error is not expected to carry usage.
fn is_committed_success(outcome: &Outcome) -> bool {
    let upstream_body = !matches!(
        outcome.status,
        OutcomeStatus::ForwardedError { .. }
            | OutcomeStatus::Failed(_)
            | OutcomeStatus::Rejected(_)
    );
    upstream_body
        && outcome
            .http_status
            .is_some_and(|status| (200..300).contains(&status))
}

/// Creates the bounded outcome queue; `capacity` is `forwarding.outcome_queue`.
///
/// # Panics
///
/// When `capacity` is zero, which tokio's bounded channel does not support;
/// the configuration loader only accepts values of at least 1.
pub fn outcome_channel(capacity: usize) -> (OutcomeSink, OutcomeReceiver) {
    let (tx, rx) = mpsc::channel(capacity);
    let sink = OutcomeSink {
        tx,
        counters: Arc::new(Counters::default()),
        shutting_down: Arc::new(AtomicBool::new(false)),
    };
    (sink, OutcomeReceiver { rx })
}

/// Sending side of the outcome queue, plus the process-wide [`Counters`] and
/// the shutdown flag the bodies consult.
#[derive(Debug, Clone)]
pub struct OutcomeSink {
    tx: mpsc::Sender<Outcome>,
    counters: Arc<Counters>,
    shutting_down: Arc<AtomicBool>,
}

impl OutcomeSink {
    /// `try_send`s the record; a full or closed queue drops it and counts it
    /// in `outcomes_dropped`. Never blocks, never awaits (R15).
    pub fn emit(&self, outcome: Outcome) {
        if self.tx.try_send(outcome).is_err() {
            // Relaxed: the counter is only summed for reports and orders no
            // other memory.
            self.counters
                .outcomes_dropped
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    /// The process-wide counters.
    pub fn counters(&self) -> &Counters {
        &self.counters
    }

    /// Marks the start of graceful shutdown; bodies dropped from now on settle
    /// as `ShutdownAborted` and start no drain.
    pub fn begin_shutdown(&self) {
        // Relaxed: the flag publishes no other data, and a body that misses a
        // flag set a moment ago behaves as if it had ended a moment earlier.
        self.shutting_down.store(true, Ordering::Relaxed);
    }

    /// Whether [`begin_shutdown`](Self::begin_shutdown) was called.
    pub fn is_shutting_down(&self) -> bool {
        self.shutting_down.load(Ordering::Relaxed)
    }
}

/// Receiving side of the outcome queue.
#[derive(Debug)]
pub struct OutcomeReceiver {
    rx: mpsc::Receiver<Outcome>,
}

impl OutcomeReceiver {
    /// The next record; `None` once every sink is gone and the queue is empty.
    pub async fn recv(&mut self) -> Option<Outcome> {
        self.rx.recv().await
    }

    /// The next record if one is queued, without waiting.
    pub fn try_recv(&mut self) -> Result<Outcome, TryRecvError> {
        self.rx.try_recv()
    }
}

#[cfg(test)]
mod tests {
    use serde_json::error::Category;

    use super::*;

    /// `chat-stream-grok46-xhigh`: the CPA final chunk carries usage together
    /// with `finish_reason`.
    const GROK_USAGE: UsageTokens = UsageTokens {
        input: 213,
        output: 71,
        cached_input: Some(0),
        reasoning_output: Some(70),
    };

    /// The byte estimate of the same stream when the client leaves after the
    /// final chunk but before `[DONE]`: about 15 times the real output.
    fn grok_estimate() -> UsageTokens {
        estimate(852, 4284)
    }

    const SMALL_USAGE: UsageTokens = UsageTokens {
        input: 10,
        output: 5000,
        cached_input: Some(3),
        reasoning_output: None,
    };

    const AFTER_COMMIT_ENDINGS: [OutcomeStatus; 6] = [
        OutcomeStatus::ClientCancelled,
        OutcomeStatus::UpstreamTruncated,
        OutcomeStatus::UpstreamProtocol,
        OutcomeStatus::IdleTimeout,
        OutcomeStatus::FirstByteTimeout,
        OutcomeStatus::ShutdownAborted,
    ];

    fn settle(status: OutcomeStatus, usage: Option<UsageTokens>) -> SettleInput {
        SettleInput {
            status,
            usage,
            usage_final: false,
            usage_error: false,
            estimate: grok_estimate(),
            before_commit: false,
        }
    }

    fn outcome(status: OutcomeStatus) -> Outcome {
        Outcome {
            request_id: 1,
            key: Some(KeyId(0)),
            channel: Some(ChannelId(0)),
            attempts: 1,
            stream: true,
            status,
            http_status: Some(200),
            usage: Some(GROK_USAGE),
            usage_error: None,
            estimate: grok_estimate(),
            billed: GROK_USAGE,
            request_bytes: 852,
            response_bytes: 4284,
            elapsed: Duration::from_millis(1500),
            to_commit: Some(Duration::from_millis(900)),
        }
    }

    #[test]
    fn estimate_rounds_up() {
        assert_eq!(estimate(0, 0), UsageTokens::default());
        assert_eq!(estimate(1, 4), estimate(4, 1));
        let tokens = estimate(5, 8);
        assert_eq!((tokens.input, tokens.output), (2, 2));
        let tokens = estimate(4, 9);
        assert_eq!((tokens.input, tokens.output), (1, 3));
        assert_eq!(tokens.cached_input, None);
        assert_eq!(tokens.reasoning_output, None);
        assert_eq!(estimate(u64::MAX, 0).input, 1 << 62);
        assert_eq!(grok_estimate().output, 1071);
    }

    #[test]
    fn failed_and_rejected_bill_nothing() {
        for status in [
            OutcomeStatus::Failed(FailureClass::Status),
            OutcomeStatus::Rejected(RejectReason::ModelNotFound),
        ] {
            let mut input = settle(status, Some(GROK_USAGE));
            input.usage_error = true;
            input.usage_final = true;
            assert_eq!(billed(&input), UsageTokens::default(), "{status:?}");
        }
    }

    #[test]
    fn forwarded_error_bills_usage_or_nothing() {
        let status = OutcomeStatus::ForwardedError { status: 200 };
        assert_eq!(billed(&settle(status, None)), UsageTokens::default());
        assert_eq!(billed(&settle(status, Some(GROK_USAGE))), GROK_USAGE);

        // The ForwardedError row precedes the usage_error row.
        let mut input = settle(OutcomeStatus::ForwardedError { status: 429 }, None);
        input.usage_error = true;
        assert_eq!(billed(&input), UsageTokens::default());
    }

    #[test]
    fn before_commit_bills_the_estimated_prompt_only() {
        let prompt_only = UsageTokens {
            input: 213,
            output: 0,
            cached_input: None,
            reasoning_output: None,
        };
        let mut input = settle(OutcomeStatus::ClientCancelled, None);
        input.before_commit = true;
        assert_eq!(billed(&input), prompt_only);

        // The before_commit row precedes the usage_error and usage_final rows.
        let mut input = settle(OutcomeStatus::ClientCancelled, Some(SMALL_USAGE));
        input.before_commit = true;
        input.usage_error = true;
        input.usage_final = true;
        assert_eq!(billed(&input), prompt_only);
    }

    #[test]
    fn usage_error_settles_conservatively_for_every_committed_status() {
        let statuses = [OutcomeStatus::Completed, OutcomeStatus::Drained]
            .into_iter()
            .chain(AFTER_COMMIT_ENDINGS);
        for status in statuses {
            let mut with_usage = settle(status, Some(SMALL_USAGE));
            with_usage.usage_error = true;
            // usage_final does not rescue a stream with a failed candidate.
            with_usage.usage_final = true;
            assert_eq!(
                billed(&with_usage),
                UsageTokens {
                    input: 213,
                    output: 5000,
                    cached_input: Some(3),
                    reasoning_output: None,
                },
                "{status:?}"
            );

            let mut without_usage = settle(status, None);
            without_usage.usage_error = true;
            assert_eq!(billed(&without_usage), grok_estimate(), "{status:?}");
        }
    }

    #[test]
    fn completed_bills_usage_or_the_estimate() {
        let input = settle(OutcomeStatus::Completed, Some(GROK_USAGE));
        assert_eq!(billed(&input), GROK_USAGE);
        assert_eq!(
            billed(&settle(OutcomeStatus::Completed, None)),
            grok_estimate()
        );
    }

    #[test]
    fn drained_bills_the_drained_usage() {
        assert_eq!(
            billed(&settle(OutcomeStatus::Drained, Some(GROK_USAGE))),
            GROK_USAGE
        );
    }

    #[test]
    fn final_usage_is_billed_exactly_after_commit() {
        for status in AFTER_COMMIT_ENDINGS {
            let mut input = settle(status, Some(GROK_USAGE));
            input.usage_final = true;
            assert_eq!(billed(&input), GROK_USAGE, "{status:?}");
        }
    }

    #[test]
    fn grok_stream_cut_after_the_final_chunk_bills_213_by_71() {
        let input = SettleInput {
            status: OutcomeStatus::ClientCancelled,
            usage: Some(GROK_USAGE),
            usage_final: true,
            usage_error: false,
            estimate: grok_estimate(),
            before_commit: false,
        };
        let bill = billed(&input);
        assert_eq!((bill.input, bill.output), (213, 71));
        assert_eq!(bill, GROK_USAGE);
    }

    #[test]
    fn other_endings_take_the_larger_of_usage_and_estimate() {
        for status in AFTER_COMMIT_ENDINGS {
            let input = settle(status, Some(GROK_USAGE));
            assert_eq!(
                billed(&input),
                UsageTokens {
                    input: 213,
                    output: 1071,
                    cached_input: Some(0),
                    reasoning_output: Some(70),
                },
                "{status:?}"
            );
            assert_eq!(billed(&settle(status, None)), grok_estimate(), "{status:?}");
        }
    }

    #[test]
    fn tally_counts_every_status() {
        let statuses = [
            (OutcomeStatus::Completed, 0),
            (OutcomeStatus::ForwardedError { status: 429 }, 1),
            (OutcomeStatus::UpstreamTruncated, 2),
            (OutcomeStatus::UpstreamProtocol, 3),
            (OutcomeStatus::IdleTimeout, 4),
            (OutcomeStatus::FirstByteTimeout, 5),
            (OutcomeStatus::ClientCancelled, 6),
            (OutcomeStatus::Drained, 7),
            (OutcomeStatus::ShutdownAborted, 8),
            (OutcomeStatus::Failed(FailureClass::Connect), 9),
            (OutcomeStatus::Rejected(RejectReason::InvalidKey), 10),
        ];
        for (status, slot) in statuses {
            let mut tally = OutcomeTally::default();
            tally.record(&outcome(status));
            let counts = [
                tally.completed,
                tally.forwarded_errors,
                tally.truncated,
                tally.protocol_errors,
                tally.idle_timeouts,
                tally.first_byte_timeouts,
                tally.client_cancelled,
                tally.drained,
                tally.shutdown_aborted,
                tally.failed,
                tally.rejected,
            ];
            let mut expected = [0; 11];
            expected[slot] = 1;
            assert_eq!(counts, expected, "{status:?}");
            assert_eq!(tally.requests, 1);
        }
    }

    #[test]
    fn tally_sums_failovers_and_billed_tokens() {
        let mut tally = OutcomeTally::default();
        let mut first = outcome(OutcomeStatus::Completed);
        first.attempts = 3;
        let mut second = outcome(OutcomeStatus::Failed(FailureClass::Status));
        second.attempts = 2;
        second.billed = UsageTokens::default();
        let mut rejected = outcome(OutcomeStatus::Rejected(RejectReason::MissingKey));
        rejected.attempts = 0;
        rejected.billed = UsageTokens::default();
        for record in [&first, &second, &rejected] {
            tally.record(record);
        }
        assert_eq!(tally.requests, 3);
        assert_eq!(tally.failovers, 3);
        assert_eq!(tally.billed_input, 213);
        assert_eq!(tally.billed_output, 71);
    }

    #[test]
    fn tally_saturates_billed_totals_instead_of_overflowing() {
        let mut tally = OutcomeTally::default();
        let mut huge = outcome(OutcomeStatus::Completed);
        huge.billed = UsageTokens {
            input: u64::MAX,
            output: u64::MAX - 1,
            cached_input: None,
            reasoning_output: None,
        };
        tally.record(&huge);
        tally.record(&huge);
        assert_eq!(tally.requests, 2);
        assert_eq!(tally.billed_input, u64::MAX);
        assert_eq!(tally.billed_output, u64::MAX);
    }

    #[test]
    fn tally_counts_missing_and_unparsable_usage() {
        let json_error = UsageErrorKind::Json {
            category: Category::Data,
            column: 12,
        };

        let mut tally = OutcomeTally::default();
        let mut absent = outcome(OutcomeStatus::Completed);
        absent.usage = None;
        tally.record(&absent);
        assert_eq!((tally.usage_missing, tally.usage_parse_errors), (1, 0));

        let mut unparsable = outcome(OutcomeStatus::UpstreamTruncated);
        unparsable.usage_error = Some(json_error);
        tally.record(&unparsable);
        assert_eq!((tally.usage_missing, tally.usage_parse_errors), (2, 1));

        let mut forwarded = outcome(OutcomeStatus::ForwardedError { status: 200 });
        forwarded.usage = None;
        forwarded.usage_error = Some(UsageErrorKind::MissingField("prompt_tokens"));
        tally.record(&forwarded);
        assert_eq!((tally.usage_missing, tally.usage_parse_errors), (2, 2));

        let mut left_before_head = outcome(OutcomeStatus::ClientCancelled);
        left_before_head.http_status = None;
        left_before_head.usage = None;
        tally.record(&left_before_head);
        let mut failed = outcome(OutcomeStatus::Failed(FailureClass::Connect));
        failed.http_status = Some(502);
        failed.usage = None;
        tally.record(&failed);
        let mut non_2xx_head = outcome(OutcomeStatus::UpstreamTruncated);
        non_2xx_head.http_status = Some(503);
        non_2xx_head.usage = None;
        tally.record(&non_2xx_head);
        assert_eq!((tally.usage_missing, tally.usage_parse_errors), (2, 2));

        tally.record(&outcome(OutcomeStatus::Completed));
        assert_eq!((tally.usage_missing, tally.usage_parse_errors), (2, 2));
        assert_eq!(tally.requests, 7);
    }

    #[test]
    fn full_queue_drops_and_counts_without_blocking() {
        let (sink, mut receiver) = outcome_channel(1);
        sink.emit(outcome(OutcomeStatus::Completed));
        sink.emit(outcome(OutcomeStatus::Drained));
        assert_eq!(sink.counters().outcomes_dropped.load(Ordering::Relaxed), 1);
        assert_eq!(
            receiver.try_recv().unwrap().status,
            OutcomeStatus::Completed
        );
        assert!(matches!(receiver.try_recv(), Err(TryRecvError::Empty)));
    }

    #[test]
    fn closed_queue_drops_and_counts() {
        let (sink, receiver) = outcome_channel(4);
        drop(receiver);
        sink.emit(outcome(OutcomeStatus::Completed));
        sink.clone().emit(outcome(OutcomeStatus::Completed));
        assert_eq!(sink.counters().outcomes_dropped.load(Ordering::Relaxed), 2);
        assert_eq!(sink.counters().warmup_failures.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn clones_share_counters_and_the_shutdown_flag() {
        let (sink, _receiver) = outcome_channel(1);
        let clone = sink.clone();
        assert!(!sink.is_shutting_down());
        clone.begin_shutdown();
        assert!(sink.is_shutting_down());
        assert!(clone.is_shutting_down());

        clone
            .counters()
            .warmup_failures
            .fetch_add(1, Ordering::Relaxed);
        assert_eq!(sink.counters().warmup_failures.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn receiver_sees_records_in_order_then_the_end() {
        let (sink, mut receiver) = outcome_channel(8);
        let mut first = outcome(OutcomeStatus::Completed);
        first.request_id = 7;
        let mut second = outcome(OutcomeStatus::ClientCancelled);
        second.request_id = 8;
        sink.emit(first);
        sink.emit(second);
        drop(sink);

        assert_eq!(receiver.recv().await.unwrap().request_id, 7);
        assert_eq!(receiver.recv().await.unwrap().request_id, 8);
        assert!(receiver.recv().await.is_none());
        assert!(matches!(
            receiver.try_recv(),
            Err(TryRecvError::Disconnected)
        ));
    }
}
