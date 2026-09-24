//! The outcome logger task: drains the outcome queue in batches, logs every
//! record under the `brisk::outcome` target and reports running totals.
//!
//! The task sleeps 100 ms and then empties the queue with `try_recv`, instead
//! of awaiting `recv()`. While it sleeps no receiver waker is registered, so
//! `OutcomeSink::emit` on the request path never wakes a task (D27). A 16384
//! entry queue drained every 100 ms absorbs about 160k records per second;
//! beyond that `emit` drops records and counts them in `outcomes_dropped`.

use std::future::Future;
use std::sync::atomic::Ordering;
use std::time::Duration;

use brisk_gateway::outcome::{Outcome, OutcomeReceiver, OutcomeSink, OutcomeTally};
// The type is part of `OutcomeReceiver::try_recv`'s signature, so the tokio
// feature that defines it is always enabled through brisk-gateway.
use tokio::sync::mpsc::error::TryRecvError;
use tokio::time::{Instant, sleep, sleep_until};

/// Pause between two drains of the queue.
const DRAIN_INTERVAL: Duration = Duration::from_millis(100);

/// Interval of the periodic totals line.
const SUMMARY_INTERVAL: Duration = Duration::from_secs(60);

/// Longest wait for late records once the logger was told to stop (2.9).
pub(crate) const FINAL_DRAIN: Duration = Duration::from_secs(3);

/// Tracing target of the per-request records; the default filter turns it
/// off (D27).
pub(crate) const OUTCOME_TARGET: &str = "brisk::outcome";

/// The two rare-path counters at one moment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct CounterSnapshot {
    pub(crate) outcomes_dropped: u64,
    pub(crate) warmup_failures: u64,
}

impl CounterSnapshot {
    fn of(sink: &OutcomeSink) -> Self {
        let counters = sink.counters();
        // Relaxed: the counters are independent totals for reports.
        Self {
            outcomes_dropped: counters.outcomes_dropped.load(Ordering::Relaxed),
            warmup_failures: counters.warmup_failures.load(Ordering::Relaxed),
        }
    }
}

/// Runs the logger until `stop` completes, then keeps draining until every
/// sender is gone or `final_drain` has passed, and logs the final totals.
///
/// `sink` is used only to read the counters. It is dropped when `stop`
/// completes, so the queue can report that every other sender (the gateway
/// and the bodies still settling) is gone; the final totals use the counters
/// read at that moment. Returns the totals, for tests and the exit path.
pub(crate) async fn run(
    mut receiver: OutcomeReceiver,
    sink: OutcomeSink,
    stop: impl Future<Output = ()>,
    final_drain: Duration,
) -> (OutcomeTally, CounterSnapshot) {
    let mut tally = OutcomeTally::default();
    let mut next_summary = Instant::now() + SUMMARY_INTERVAL;
    let mut stop = std::pin::pin!(stop);
    loop {
        tokio::select! {
            biased;
            () = &mut stop => break,
            () = sleep(DRAIN_INTERVAL) => {}
        }
        drain(&mut receiver, &mut tally);
        let now = Instant::now();
        if now >= next_summary {
            log_summary("periodic", &tally, CounterSnapshot::of(&sink));
            next_summary = now + SUMMARY_INTERVAL;
        }
    }

    let counters = CounterSnapshot::of(&sink);
    drop(sink);
    let deadline = Instant::now() + final_drain;
    loop {
        if drain(&mut receiver, &mut tally) == Drained::Disconnected {
            break;
        }
        let now = Instant::now();
        if now >= deadline {
            tracing::warn!(
                wait_ms = u64::try_from(final_drain.as_millis()).unwrap_or(u64::MAX),
                "stopped waiting for outcome records still being settled"
            );
            break;
        }
        sleep_until(deadline.min(now + DRAIN_INTERVAL)).await;
    }
    log_summary("final", &tally, counters);
    (tally, counters)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Drained {
    /// The queue is empty; senders remain.
    Empty,
    /// The queue is empty and every sender is gone.
    Disconnected,
}

/// Takes every queued record without waiting.
fn drain(receiver: &mut OutcomeReceiver, tally: &mut OutcomeTally) -> Drained {
    loop {
        match receiver.try_recv() {
            Ok(outcome) => {
                log_outcome(&outcome);
                tally.record(&outcome);
            }
            Err(TryRecvError::Empty) => return Drained::Empty,
            Err(TryRecvError::Disconnected) => return Drained::Disconnected,
        }
    }
}

/// One structured record per request; never keys or request content.
fn log_outcome(outcome: &Outcome) {
    let key = outcome.key.map(|key| key.0);
    let channel = outcome.channel.map(|channel| channel.0);
    let usage_input = outcome.usage.map(|usage| usage.input);
    let usage_output = outcome.usage.map(|usage| usage.output);
    tracing::info!(
        target: OUTCOME_TARGET,
        request_id = outcome.request_id,
        key,
        channel,
        attempts = outcome.attempts,
        stream = outcome.stream,
        status = ?outcome.status,
        http_status = outcome.http_status,
        usage_input,
        usage_output,
        usage_error = ?outcome.usage_error,
        billed_input = outcome.billed.input,
        billed_output = outcome.billed.output,
        request_bytes = outcome.request_bytes,
        response_bytes = outcome.response_bytes,
        elapsed_us = duration_us(outcome.elapsed),
        to_commit_us = outcome.to_commit.map(duration_us),
        "outcome"
    );
    if let Some(kind) = outcome.usage_error {
        tracing::warn!(
            request_id = outcome.request_id,
            channel,
            usage_error = ?kind,
            "usage candidate failed to parse; settled conservatively"
        );
    }
}

fn duration_us(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

fn log_summary(kind: &'static str, tally: &OutcomeTally, counters: CounterSnapshot) {
    tracing::info!(
        kind,
        requests = tally.requests,
        completed = tally.completed,
        forwarded_errors = tally.forwarded_errors,
        truncated = tally.truncated,
        protocol_errors = tally.protocol_errors,
        idle_timeouts = tally.idle_timeouts,
        first_byte_timeouts = tally.first_byte_timeouts,
        client_cancelled = tally.client_cancelled,
        drained = tally.drained,
        shutdown_aborted = tally.shutdown_aborted,
        failed = tally.failed,
        rejected = tally.rejected,
        failovers = tally.failovers,
        usage_missing = tally.usage_missing,
        usage_parse_errors = tally.usage_parse_errors,
        billed_input = tally.billed_input,
        billed_output = tally.billed_output,
        outcomes_dropped = counters.outcomes_dropped,
        warmup_failures = counters.warmup_failures,
        "outcome totals"
    );
}

#[cfg(test)]
mod tests {
    use brisk_gateway::outcome::{OutcomeStatus, estimate, outcome_channel};
    use brisk_gateway::spec::ChannelId;

    use super::*;

    fn outcome(request_id: u64, status: OutcomeStatus, attempts: u8) -> Outcome {
        // brisk does not depend on brisk-proto; `estimate` yields the type.
        let mut usage = estimate(0, 0);
        usage.input = 213;
        usage.output = 71;
        Outcome {
            request_id,
            key: None,
            channel: Some(ChannelId(0)),
            attempts,
            stream: true,
            status,
            http_status: Some(200),
            usage: Some(usage),
            usage_error: None,
            estimate: estimate(852, 4284),
            billed: usage,
            request_bytes: 852,
            response_bytes: 4284,
            elapsed: Duration::from_millis(5),
            to_commit: Some(Duration::from_millis(1)),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn tallies_records_and_stops_when_senders_are_gone() {
        let (sink, receiver) = outcome_channel(16);
        let emitter = sink.clone();
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
        let logger = tokio::spawn(run(
            receiver,
            sink,
            async move {
                let _ = stop_rx.await;
            },
            FINAL_DRAIN,
        ));

        emitter.emit(outcome(1, OutcomeStatus::Completed, 1));
        emitter.emit(outcome(2, OutcomeStatus::Completed, 2));
        sleep(Duration::from_millis(250)).await;
        stop_tx.send(()).unwrap();
        sleep(Duration::from_millis(50)).await;
        // Emitted after the stop request, before the last sender went away.
        emitter.emit(outcome(3, OutcomeStatus::ClientCancelled, 1));
        drop(emitter);

        let started = Instant::now();
        let (tally, counters) = logger.await.unwrap();
        assert!(started.elapsed() < FINAL_DRAIN, "waited for the deadline");
        assert_eq!(tally.requests, 3);
        assert_eq!(tally.completed, 2);
        assert_eq!(tally.client_cancelled, 1);
        assert_eq!(tally.failovers, 1);
        assert_eq!(tally.billed_input, 3 * 213);
        assert_eq!(tally.billed_output, 3 * 71);
        assert_eq!(counters, CounterSnapshot::default());
    }

    #[tokio::test(start_paused = true)]
    async fn final_drain_is_bounded_while_a_sender_lives() {
        let (sink, receiver) = outcome_channel(16);
        let held = sink.clone();
        let started = Instant::now();
        let (tally, _) = run(
            receiver,
            sink,
            sleep(Duration::from_millis(100)),
            FINAL_DRAIN,
        )
        .await;
        let elapsed = started.elapsed();
        assert!(
            elapsed >= Duration::from_millis(100) + FINAL_DRAIN,
            "{elapsed:?}"
        );
        assert!(
            elapsed < Duration::from_millis(300) + FINAL_DRAIN,
            "{elapsed:?}"
        );
        assert_eq!(tally.requests, 0);
        drop(held);
    }

    #[tokio::test(start_paused = true)]
    async fn reports_counters_read_at_stop() {
        let (sink, receiver) = outcome_channel(1);
        let emitter = sink.clone();
        emitter.emit(outcome(1, OutcomeStatus::Completed, 1));
        // The queue holds one record; this one is dropped and counted.
        emitter.emit(outcome(2, OutcomeStatus::Completed, 1));
        emitter
            .counters()
            .warmup_failures
            .fetch_add(2, Ordering::Relaxed);
        drop(emitter);

        let (tally, counters) = run(receiver, sink, std::future::ready(()), FINAL_DRAIN).await;
        assert_eq!(tally.requests, 1);
        assert_eq!(
            counters,
            CounterSnapshot {
                outcomes_dropped: 1,
                warmup_failures: 2,
            }
        );
    }

    #[tokio::test(start_paused = true)]
    async fn periodic_drains_do_not_wait_for_stop() {
        let (sink, receiver) = outcome_channel(16);
        let emitter = sink.clone();
        emitter.emit(outcome(1, OutcomeStatus::Drained, 1));
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
        let logger = tokio::spawn(run(
            receiver,
            sink,
            async move {
                let _ = stop_rx.await;
            },
            FINAL_DRAIN,
        ));
        // Past one summary interval, so the periodic totals path runs too.
        sleep(SUMMARY_INTERVAL + Duration::from_secs(1)).await;
        assert_eq!(
            emitter.counters().outcomes_dropped.load(Ordering::Relaxed),
            0
        );
        drop(emitter);
        stop_tx.send(()).unwrap();
        let (tally, _) = logger.await.unwrap();
        assert_eq!(tally.drained, 1);
    }
}
