//! Validity rules of a run and the `selfcheck` verdict.
//!
//! Contract rules (section 5):
//!
//! - once per second the achieved concurrency and chunk rate are compared
//!   with the target; more than 1% of the measured intervals deviating by
//!   more than 5% invalidates the run;
//! - a clock step detected by the realtime offset invalidates the run;
//! - a mock write lag p99 at or above `--max-mock-write-lag-us` (the
//!   contract's 10 µs by default) invalidates the run. So does the absence of mock write lag samples where the responses carry markers
//!   (S1, S3, and S2 with room for a marker in the content): the rule must
//!   not pass for lack of evidence.
//!
//! The target of the per-second check is the load the open-loop schedule
//! offers in that second (streams it has open, chunks it has due). With
//! Poisson arrivals the number of open streams is itself Poisson
//! distributed around the nominal concurrency (±3.2% standard deviation at
//! 1000 streams), so comparing with the constant nominal value would flag
//! about one interval in nine by chance alone. The offered load isolates
//! what the check is meant to catch: a load generator or server that fails
//! to keep up with the schedule. The deviation from the nominal concurrency
//! is still reported, and `selfcheck` also bounds its mean over the run.
//!
//! Added rules, each of which otherwise lets a skewed measurement pass:
//!
//! - a run without measured intervals or completed requests is invalid;
//! - more than 0.1% failed requests (stale keep-alive retries excluded, they
//!   succeed on the retry) invalidate the run, and so do stale retries above
//!   0.1% of the requests: beyond that they are no longer keep-alive races;
//! - an emit lag p99 of 10 µs or more, or a p99.9 of 1 ms or more, means the
//!   load generator itself fell behind its schedule. TTFT and request
//!   latency run from the scheduled time and would blame that on the server,
//!   and the per-second load check cannot see it: sampling and sending share
//!   the event loop, so a stall delays both alike. The p99.9 bound tolerates
//!   the odd scheduler hiccup while catching a lag that reaches the compared
//!   tail. The reason names the likely cause from the deadline misses of
//!   [`Diagnostics`]: sends the loop reached late call for a longer spin
//!   window, lag without misses for a quieter CPU;
//! - on Linux, a receive without a kernel timestamp: the contract requires
//!   `SO_TIMESTAMPNS` for every receive, and the fallback clock would add
//!   the event loop's latency to every span;
//! - more than 0.1% of the recorded spans negative: the clock conversion is
//!   off, and those spans were recorded as 0;
//! - on request (`--max-slip-us`, always in `selfcheck`), a request slip
//!   p99 at or above the limit: the mock's inbound path queues requests, and
//!   TTFT measures the mock instead of the target. Mock write lag cannot see
//!   this, and neither can the load check: a longer TTFT stretches a
//!   stream's life by a tiny fraction only.
//!
//! The p99 rules apply however few samples there are. With the 600 sends of
//! a short S3 run the p99 is the sixth largest value, so a handful of
//! outliers decides it; the reason states the sample count, and the remedy
//! is a longer run, not a laxer rule that would pass for lack of evidence.

use std::collections::BTreeMap;

use brisk_bench_core::result::{IntervalResult, MetricSummary, Validity};
use brisk_bench_core::stats::Metric;
use serde::Serialize;

use crate::diagnostics::Diagnostics;
use crate::schedule::{PlannedInterval, SEC_NS};
use crate::shard::{Counters, STALE_RETRY};

/// Largest tolerated relative deviation of one interval.
pub(crate) const LOAD_TOLERANCE: f64 = 0.05;
/// Largest tolerated share of deviating intervals.
pub(crate) const MAX_DEVIATING_SHARE: f64 = 0.01;
/// Emit lag p99 limit. The mock write lag limit is a command-line option,
/// [`crate::cli::CommonArgs::max_mock_write_lag_us`].
pub(crate) const EMIT_LAG_P99_LIMIT_NS: u64 = 10_000;
/// Emit lag p99.9 limit.
pub(crate) const EMIT_LAG_P999_LIMIT_NS: u64 = 1_000_000;
/// Largest tolerated share of failed requests.
pub(crate) const MAX_ERROR_SHARE: f64 = 0.001;
/// Largest tolerated number of stale retries relative to the requests.
pub(crate) const MAX_STALE_SHARE: f64 = 0.001;
/// Largest tolerated share of negative spans among all recorded samples.
pub(crate) const MAX_NEGATIVE_SHARE: f64 = 0.001;

/// Request outcomes of the measured intervals.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Outcomes {
    /// Requests completed successfully.
    pub(crate) requests: u64,
    /// Failed requests, stale retries excluded.
    pub(crate) failures: u64,
    /// Stale keep-alive retries.
    pub(crate) stale_retries: u64,
}

impl Outcomes {
    /// Counts the outcomes of the intervals from `warmup_intervals` on.
    pub(crate) fn of(intervals: &[IntervalResult], warmup_intervals: u64) -> Self {
        let mut outcomes = Self::default();
        for interval in intervals.iter().filter(|i| i.index >= warmup_intervals) {
            outcomes.requests += interval.requests;
            for (kind, n) in &interval.errors {
                if kind == STALE_RETRY {
                    outcomes.stale_retries += n;
                } else {
                    outcomes.failures += n;
                }
            }
        }
        outcomes
    }

    /// `failures / (requests + failures)`; 0 without any request.
    pub(crate) fn failure_share(&self) -> f64 {
        let total = self.requests + self.failures;
        if total == 0 {
            0.0
        } else {
            as_f64(self.failures) / as_f64(total)
        }
    }
}

/// Per-second comparison of achieved and offered load.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct LoadCheck {
    /// Full-length post-warmup intervals compared.
    pub(crate) checked_intervals: usize,
    /// Intervals whose concurrency deviates by more than the tolerance.
    pub(crate) concurrency_deviating: usize,
    /// Intervals whose chunk count deviates by more than the tolerance.
    pub(crate) chunk_rate_deviating: usize,
    /// Intervals deviating in either respect.
    pub(crate) deviating: usize,
    /// Largest relative concurrency deviation.
    pub(crate) worst_concurrency_deviation: f64,
    /// Largest relative chunk-count deviation.
    pub(crate) worst_chunk_rate_deviation: f64,
    /// Mean achieved concurrency.
    pub(crate) mean_concurrency: f64,
    /// Mean offered concurrency.
    pub(crate) mean_planned_concurrency: f64,
    /// Mean achieved chunk rate per second.
    pub(crate) mean_chunk_rate: f64,
    /// Mean offered chunk rate per second.
    pub(crate) mean_planned_chunk_rate: f64,
    /// Nominal concurrency from the command line.
    pub(crate) nominal_concurrency: u32,
    /// `mean_concurrency / nominal − 1`.
    pub(crate) nominal_deviation: f64,
}

/// Relative deviation of `actual` from `planned`; any load where none was
/// planned counts as infinitely far off.
fn deviation(actual: f64, planned: f64) -> f64 {
    if planned > 0.0 {
        (actual - planned).abs() / planned
    } else if actual > 0.0 {
        f64::INFINITY
    } else {
        0.0
    }
}

#[expect(clippy::cast_precision_loss, reason = "counts are far below 2^53")]
fn as_f64(v: u64) -> f64 {
    v as f64
}

/// Compares the achieved load of every full post-warmup interval with the
/// offered load.
pub(crate) fn check_load(
    intervals: &[IntervalResult],
    planned: &[PlannedInterval],
    warmup_intervals: u64,
    nominal_concurrency: u32,
) -> LoadCheck {
    let mut check = LoadCheck {
        checked_intervals: 0,
        concurrency_deviating: 0,
        chunk_rate_deviating: 0,
        deviating: 0,
        worst_concurrency_deviation: 0.0,
        worst_chunk_rate_deviation: 0.0,
        mean_concurrency: 0.0,
        mean_planned_concurrency: 0.0,
        mean_chunk_rate: 0.0,
        mean_planned_chunk_rate: 0.0,
        nominal_concurrency,
        nominal_deviation: 0.0,
    };
    let planned: BTreeMap<u64, &PlannedInterval> = planned.iter().map(|p| (p.index, p)).collect();
    for interval in intervals
        .iter()
        .filter(|i| i.index >= warmup_intervals && i.duration_ns == SEC_NS)
    {
        let (planned_conc, planned_chunks) = planned
            .get(&interval.index)
            .map_or((0.0, 0), |p| (p.concurrency, p.chunks));
        let conc_dev = deviation(interval.concurrency.mean, planned_conc);
        let chunk_dev = deviation(as_f64(interval.chunks), as_f64(planned_chunks));
        check.checked_intervals += 1;
        check.concurrency_deviating += usize::from(conc_dev > LOAD_TOLERANCE);
        check.chunk_rate_deviating += usize::from(chunk_dev > LOAD_TOLERANCE);
        check.deviating += usize::from(conc_dev > LOAD_TOLERANCE || chunk_dev > LOAD_TOLERANCE);
        check.worst_concurrency_deviation = check.worst_concurrency_deviation.max(conc_dev);
        check.worst_chunk_rate_deviation = check.worst_chunk_rate_deviation.max(chunk_dev);
        check.mean_concurrency += interval.concurrency.mean;
        check.mean_planned_concurrency += planned_conc;
        check.mean_chunk_rate += as_f64(interval.chunks);
        check.mean_planned_chunk_rate += as_f64(planned_chunks);
    }
    if check.checked_intervals > 0 {
        let n = as_f64(check.checked_intervals as u64);
        check.mean_concurrency /= n;
        check.mean_planned_concurrency /= n;
        check.mean_chunk_rate /= n;
        check.mean_planned_chunk_rate /= n;
        check.nominal_deviation = check.mean_concurrency / f64::from(nominal_concurrency) - 1.0;
    }
    check
}

impl LoadCheck {
    /// Whether more intervals deviate than the contract tolerates.
    pub(crate) fn too_many_deviating(&self) -> bool {
        as_f64(self.deviating as u64) > MAX_DEVIATING_SHARE * as_f64(self.checked_intervals as u64)
    }
}

/// Inputs of [`evaluate`].
#[derive(Debug)]
pub(crate) struct Evidence<'a> {
    /// Merged intervals, warmup included.
    pub(crate) intervals: &'a [IntervalResult],
    /// Leading intervals excluded.
    pub(crate) warmup_intervals: u64,
    /// Post-warmup summary.
    pub(crate) summary: &'a BTreeMap<Metric, MetricSummary>,
    /// Clock steps detected by any shard.
    pub(crate) clock_steps: u64,
    /// Load check, for stream scenarios.
    pub(crate) load: Option<&'a LoadCheck>,
    /// Transport counters of the whole run.
    pub(crate) counters: &'a Counters,
    /// Post-warmup diagnostics.
    pub(crate) diagnostics: &'a Diagnostics,
    /// Successful responses carry mock timestamp markers.
    pub(crate) markers_expected: bool,
    /// Mock write lag p99 limit.
    pub(crate) mock_lag_limit_ns: u64,
}

/// Applies every validity rule.
pub(crate) fn evaluate(evidence: &Evidence<'_>) -> Validity {
    let mut validity = Validity::default();
    let measured: Vec<&IntervalResult> = evidence
        .intervals
        .iter()
        .filter(|i| i.index >= evidence.warmup_intervals)
        .collect();
    if measured.is_empty() {
        validity.invalidate("no intervals after the warmup");
    }
    if evidence.clock_steps > 0 {
        validity.invalidate(format!(
            "CLOCK_REALTIME stepped {} time(s) during the run; kernel receive timestamps are unreliable",
            evidence.clock_steps
        ));
    }
    match evidence.summary.get(&Metric::MockWriteLag) {
        Some(lag) if lag.p99_ns >= evidence.mock_lag_limit_ns => validity.invalidate(format!(
            "mock write lag p99 {:.1} us is not below {:.1} us",
            micros(lag.p99_ns),
            micros(evidence.mock_lag_limit_ns)
        )),
        None if evidence.markers_expected => {
            validity.invalidate("no mock write lag samples after the warmup");
        }
        _ => {}
    }
    if let Some(lag) = evidence.summary.get(&Metric::EmitLag) {
        let cause = || emit_lag_cause(evidence.diagnostics, lag.count);
        if lag.p99_ns >= EMIT_LAG_P99_LIMIT_NS {
            validity.invalidate(format!(
                "emit lag p99 {:.1} us over {} sends is not below {:.1} us: \
                 the load generator fell behind its schedule; {}",
                micros(lag.p99_ns),
                lag.count,
                micros(EMIT_LAG_P99_LIMIT_NS),
                cause()
            ));
        }
        if lag.p999_ns >= EMIT_LAG_P999_LIMIT_NS {
            validity.invalidate(format!(
                "emit lag p99.9 {:.1} us over {} sends is not below {:.1} us: \
                 the load generator fell behind its schedule; {}",
                micros(lag.p999_ns),
                lag.count,
                micros(EMIT_LAG_P999_LIMIT_NS),
                cause()
            ));
        }
    }
    if cfg!(target_os = "linux") && evidence.counters.rx_batches_without_timestamp > 0 {
        validity.invalidate(format!(
            "{} of {} receives carried no kernel timestamp",
            evidence.counters.rx_batches_without_timestamp, evidence.counters.rx_batches
        ));
    }
    let negative: u64 = measured.iter().map(|i| i.negative).sum();
    let samples = negative_span_denominator(evidence.summary);
    if as_f64(negative) > MAX_NEGATIVE_SHARE * as_f64(samples) {
        validity.invalidate(format!(
            "{negative} of {samples} spans after the warmup ended before they started \
             (more than {}%); the clock conversion is off",
            MAX_NEGATIVE_SHARE * 100.0
        ));
    }
    if let Some(load) = evidence.load
        && load.too_many_deviating()
    {
        validity.invalidate(format!(
            "{} of {} intervals deviate from the offered load by more than {:.0}% \
             (concurrency {}, chunk rate {}); at most {:.0}% may",
            load.deviating,
            load.checked_intervals,
            LOAD_TOLERANCE * 100.0,
            load.concurrency_deviating,
            load.chunk_rate_deviating,
            MAX_DEVIATING_SHARE * 100.0
        ));
    }
    let outcomes = Outcomes::of(evidence.intervals, evidence.warmup_intervals);
    if !measured.is_empty() && outcomes.requests == 0 {
        validity.invalidate("no request completed after the warmup");
    }
    if outcomes.failure_share() > MAX_ERROR_SHARE {
        validity.invalidate(format!(
            "{} of {} requests after the warmup failed (more than {}%)",
            outcomes.failures,
            outcomes.requests + outcomes.failures,
            MAX_ERROR_SHARE * 100.0
        ));
    }
    if as_f64(outcomes.stale_retries) > MAX_STALE_SHARE * as_f64(outcomes.requests) {
        validity.invalidate(format!(
            "{} stale keep-alive retries for {} requests after the warmup (more than {}%); \
             the server closes connections it was sent requests on",
            outcomes.stale_retries,
            outcomes.requests,
            MAX_STALE_SHARE * 100.0
        ));
    }
    validity
}

/// Total sample count the negative-span rule divides by. `TtftReused`
/// repeats a subset of `Ttft`'s samples (see its doc comment); counting both
/// would double-count those samples and loosen the rule. Excluded, the
/// denominator matches M0's.
fn negative_span_denominator(summary: &BTreeMap<Metric, MetricSummary>) -> u64 {
    summary
        .iter()
        .filter(|(metric, _)| **metric != Metric::TtftReused)
        .map(|(_, summary)| summary.count)
        .sum()
}

/// Why sends went out late, from the deadline misses.
fn emit_lag_cause(diagnostics: &Diagnostics, sends: u64) -> String {
    let misses = diagnostics.deadline_misses();
    if misses == 0 {
        "no send missed its spin window, so the thread lost the CPU while spinning or sending"
            .to_owned()
    } else {
        format!(
            "{misses} of {sends} sends were reached after their deadline ({} late wake-ups, \
             {} behind other events, worst by {:.1} us); a longer --spin-us absorbs late wake-ups",
            diagnostics.late_wakeups,
            diagnostics.busy_loop_misses,
            micros(diagnostics.max_deadline_miss_ns)
        )
    }
}

/// Invalidates the run when the request slip p99 reaches `limit_ns`, or
/// when no slip was measured at all: the rule must not pass for lack of
/// evidence.
pub(crate) fn check_slip(validity: &mut Validity, diagnostics: &Diagnostics, limit_ns: u64) {
    match &diagnostics.request_slip {
        Some(slip) if slip.p99_ns >= limit_ns => validity.invalidate(format!(
            "request slip p99 {:.1} us over {} requests is not below {:.1} us: \
             requests wait at the mock before it reads them, and TTFT measures that wait",
            micros(slip.p99_ns),
            slip.count,
            micros(limit_ns)
        )),
        Some(_) => {}
        None => validity.invalidate("no request slip samples after the warmup"),
    }
}

/// One `selfcheck` criterion.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct Criterion {
    /// What is checked.
    pub(crate) name: &'static str,
    /// Observed value, human-readable.
    pub(crate) observed: String,
    /// Limit, human-readable.
    pub(crate) limit: String,
    /// Whether the criterion holds.
    pub(crate) pass: bool,
}

/// The `selfcheck` verdict.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct SelfcheckVerdict {
    /// All criteria hold.
    pub(crate) pass: bool,
    /// Individual criteria.
    pub(crate) criteria: Vec<Criterion>,
}

/// Inputs of [`selfcheck`].
#[derive(Debug)]
pub(crate) struct SelfcheckEvidence<'a> {
    /// Post-warmup summary.
    pub(crate) summary: &'a BTreeMap<Metric, MetricSummary>,
    /// Load check of the run.
    pub(crate) load: &'a LoadCheck,
    /// Post-warmup diagnostics.
    pub(crate) diagnostics: &'a Diagnostics,
    /// Request slip p99 limit.
    pub(crate) slip_limit_ns: u64,
    /// Mock write lag p99 limit.
    pub(crate) mock_lag_limit_ns: u64,
    /// Validity of the run.
    pub(crate) validity: &'a Validity,
}

/// Judges a selfcheck run: emit lag p99 below 10 µs, mock write lag p99
/// below its limit, request slip p99 below its limit (a mock that keeps its write
/// schedule can still queue arriving requests for milliseconds), every
/// measured interval's concurrency within 5% of the offered
/// load, the mean concurrency of the run within 5% of the nominal target
/// (the contract's criterion; it catches a stream model that misses the
/// target, which the offered-load comparison cannot see), and the run valid.
///
/// The window mean still fluctuates with the Poisson arrivals, and slowly:
/// the open-stream count stays correlated for about the mean residual
/// stream life (some 11 s with the default durations). Its standard
/// deviation is about 1.6% at 2000 streams over 30 s, the contract's
/// selfcheck, where the 5% bound fails by chance about once in a thousand
/// runs; at 200 streams over 10 s it is about 6%, too much for the bound to
/// mean anything.
pub(crate) fn selfcheck(evidence: &SelfcheckEvidence<'_>) -> SelfcheckVerdict {
    let SelfcheckEvidence {
        summary,
        load,
        diagnostics,
        slip_limit_ns,
        mock_lag_limit_ns,
        validity,
    } = *evidence;
    let lag = |metric: Metric, name: &'static str, limit_ns: u64| {
        let p99 = summary.get(&metric).map(|s| s.p99_ns);
        Criterion {
            name,
            observed: p99.map_or_else(
                || "no samples".to_owned(),
                |ns| format!("{:.2} us", micros(ns)),
            ),
            limit: format!("< {} us", micros(limit_ns)),
            pass: p99.is_some_and(|ns| ns < limit_ns),
        }
    };
    let criteria = vec![
        lag(Metric::EmitLag, "emit_lag_p99", EMIT_LAG_P99_LIMIT_NS),
        lag(
            Metric::MockWriteLag,
            "mock_write_lag_p99",
            mock_lag_limit_ns,
        ),
        Criterion {
            name: "request_slip_p99",
            observed: diagnostics.request_slip.as_ref().map_or_else(
                || "no samples".to_owned(),
                |s| format!("{:.2} us", micros(s.p99_ns)),
            ),
            limit: format!("< {:.0} us", micros(slip_limit_ns)),
            pass: diagnostics
                .request_slip
                .as_ref()
                .is_some_and(|s| s.p99_ns < slip_limit_ns),
        },
        Criterion {
            name: "concurrency_deviation",
            observed: format!(
                "worst {:.2}% over {} intervals",
                load.worst_concurrency_deviation * 100.0,
                load.checked_intervals
            ),
            limit: format!("< {:.0}% in every interval", LOAD_TOLERANCE * 100.0),
            pass: load.checked_intervals > 0 && load.worst_concurrency_deviation < LOAD_TOLERANCE,
        },
        Criterion {
            name: "nominal_concurrency",
            observed: format!(
                "mean {:.1} of {} ({:+.2}%)",
                load.mean_concurrency,
                load.nominal_concurrency,
                load.nominal_deviation * 100.0
            ),
            limit: format!("< {:.0}% from nominal", LOAD_TOLERANCE * 100.0),
            pass: load.checked_intervals > 0 && load.nominal_deviation.abs() < LOAD_TOLERANCE,
        },
        Criterion {
            name: "run_valid",
            observed: if validity.valid {
                "valid".to_owned()
            } else {
                validity.reasons.join("; ")
            },
            limit: "valid".to_owned(),
            pass: validity.valid,
        },
    ];
    SelfcheckVerdict {
        pass: criteria.iter().all(|c| c.pass),
        criteria,
    }
}

/// Nanoseconds as fractional microseconds.
#[expect(clippy::cast_precision_loss, reason = "display only")]
pub(crate) fn micros(ns: u64) -> f64 {
    ns as f64 / 1e3
}

#[cfg(test)]
mod tests {
    use brisk_bench_core::result::ConcurrencyStats;

    use super::*;

    fn interval(index: u64, concurrency: f64, chunks: u64, requests: u64) -> IntervalResult {
        IntervalResult {
            index,
            start_ns: index * SEC_NS,
            duration_ns: SEC_NS,
            histograms: BTreeMap::new(),
            concurrency: ConcurrencyStats {
                samples: 100,
                min: 0,
                max: 0,
                mean: concurrency,
            },
            chunks,
            requests,
            errors: BTreeMap::new(),
            saturated: 0,
            negative: 0,
        }
    }

    fn planned(index: u64, concurrency: f64, chunks: u64) -> PlannedInterval {
        PlannedInterval {
            index,
            concurrency,
            chunks,
        }
    }

    fn summary_tail(
        metric: Metric,
        p99_ns: u64,
        far_tail_ns: u64,
    ) -> BTreeMap<Metric, MetricSummary> {
        BTreeMap::from([(
            metric,
            MetricSummary {
                count: 1000,
                min_ns: 1,
                mean_ns: 1.0,
                p50_ns: 1,
                p90_ns: 1,
                p99_ns,
                p999_ns: far_tail_ns,
                max_ns: far_tail_ns,
            },
        )])
    }

    fn summary_with(metric: Metric, p99_ns: u64) -> BTreeMap<Metric, MetricSummary> {
        summary_tail(metric, p99_ns, p99_ns)
    }

    /// Mock write lag and emit lag just within their limits.
    fn good_summary() -> BTreeMap<Metric, MetricSummary> {
        let mut summary = summary_with(Metric::MockWriteLag, 9_999);
        summary.extend(summary_tail(Metric::EmitLag, 9_999, 999_999));
        summary
    }

    const NO_COUNTERS: Counters = Counters {
        requests_scheduled: 0,
        connections_opened: 0,
        reused_sends: 0,
        stale_retries: 0,
        idle_closed: 0,
        rx_batches: 0,
        rx_batches_without_timestamp: 0,
        open_at_end: 0,
        censored_requests: 0,
        censored_samples: 0,
    };

    const NO_DIAGNOSTICS: Diagnostics = Diagnostics {
        late_wakeups: 0,
        busy_loop_misses: 0,
        max_deadline_miss_ns: 0,
        fresh_conn_sends: 0,
        fresh_conn_ttft: None,
        request_slip: None,
        negative_slips: 0,
    };

    #[test]
    fn load_check_counts_deviating_intervals_after_warmup() {
        let mut intervals: Vec<_> = (0..202).map(|i| interval(i, 1000.0, 30_000, 100)).collect();
        let plan: Vec<_> = (0..202).map(|i| planned(i, 1000.0, 30_000)).collect();
        // Warmup chaos is ignored.
        intervals[0].concurrency.mean = 5.0;
        // One concurrency and one chunk-rate deviation after the warmup.
        intervals[10].concurrency.mean = 1060.0;
        intervals[11].chunks = 28_000;
        // Both in one interval count once.
        intervals[12].concurrency.mean = 900.0;
        intervals[12].chunks = 0;
        // The trailing partial interval is not checked.
        intervals[201].duration_ns = SEC_NS / 2;
        intervals[201].concurrency.mean = 0.0;

        let check = check_load(&intervals, &plan, 2, 1000);
        assert_eq!(check.checked_intervals, 199);
        assert_eq!(check.concurrency_deviating, 2);
        assert_eq!(check.chunk_rate_deviating, 2);
        assert_eq!(check.deviating, 3);
        assert!((check.worst_concurrency_deviation - 0.1).abs() < 1e-12);
        assert!((check.worst_chunk_rate_deviation - 1.0).abs() < 1e-12);
        // 3 of 199 is above 1%.
        assert!(check.too_many_deviating());

        intervals[12] = interval(12, 1000.0, 30_000, 100);
        let check = check_load(&intervals, &plan, 2, 1000);
        // 2 of 199 is above 1% as well; 1 of 199 would not be.
        assert!(check.too_many_deviating());
        intervals[11] = interval(11, 1000.0, 30_000, 100);
        let check = check_load(&intervals, &plan, 2, 1000);
        assert_eq!(check.deviating, 1);
        assert!(!check.too_many_deviating());
        assert!(check.nominal_deviation.abs() < 1e-3);
    }

    #[test]
    fn deviation_handles_zero_plans() {
        assert!(deviation(0.0, 0.0).abs() < f64::EPSILON);
        assert!(deviation(1.0, 0.0).is_infinite());
        assert!((deviation(95.0, 100.0) - 0.05).abs() < 1e-12);
    }

    #[test]
    fn evaluate_applies_every_rule() {
        let intervals: Vec<_> = (0..20).map(|i| interval(i, 10.0, 300, 1000)).collect();
        let good = good_summary();
        let evidence = Evidence {
            intervals: &intervals,
            warmup_intervals: 5,
            summary: &good,
            clock_steps: 0,
            load: None,
            counters: &NO_COUNTERS,
            diagnostics: &NO_DIAGNOSTICS,
            markers_expected: true,
            mock_lag_limit_ns: 10_000,
        };
        assert_eq!(evaluate(&evidence), Validity::default());

        let mut slow = good_summary();
        slow.extend(summary_with(Metric::MockWriteLag, 10_000));
        let v = evaluate(&Evidence {
            summary: &slow,
            clock_steps: 2,
            ..evidence
        });
        assert!(!v.valid);
        assert_eq!(v.reasons.len(), 2, "{v:?}");

        // 16 failures in 15_016 is above 0.1%; stale retries do not count.
        let mut failing = intervals.clone();
        failing[7].errors.insert("reset".into(), 16);
        failing[8].errors.insert(STALE_RETRY.into(), 15);
        let v = evaluate(&Evidence {
            intervals: &failing,
            ..evidence
        });
        assert_eq!(v.reasons.len(), 1, "{v:?}");
        assert!(v.reasons[0].contains("16 of 15016"), "{v:?}");
        failing[7].errors.insert("reset".into(), 15);
        assert!(
            evaluate(&Evidence {
                intervals: &failing,
                ..evidence
            })
            .valid
        );
        // Stale retries beyond 0.1% of the 15_000 requests are no longer
        // keep-alive races.
        failing[8].errors.insert(STALE_RETRY.into(), 16);
        let v = evaluate(&Evidence {
            intervals: &failing,
            ..evidence
        });
        assert_eq!(v.reasons.len(), 1, "{v:?}");
        assert!(
            v.reasons[0].contains("16 stale keep-alive retries"),
            "{v:?}"
        );

        let v = evaluate(&Evidence {
            warmup_intervals: 20,
            ..evidence
        });
        assert!(!v.valid);
    }

    #[test]
    fn mock_write_lag_rule_follows_the_configured_limit() {
        let intervals: Vec<_> = (0..4).map(|i| interval(i, 1.0, 0, 100)).collect();
        let judge = |mock_p99_ns, mock_lag_limit_ns| {
            let mut summary = good_summary();
            summary.extend(summary_with(Metric::MockWriteLag, mock_p99_ns));
            evaluate(&Evidence {
                intervals: &intervals,
                warmup_intervals: 1,
                summary: &summary,
                clock_steps: 0,
                load: None,
                counters: &NO_COUNTERS,
                diagnostics: &NO_DIAGNOSTICS,
                markers_expected: true,
                mock_lag_limit_ns,
            })
        };
        // 12 us fails the contract's 10 us but passes a 25 us limit.
        assert_eq!(
            judge(12_000, 10_000).reasons,
            ["mock write lag p99 12.0 us is not below 10.0 us"]
        );
        assert!(judge(12_000, 25_000).valid);
        assert!(judge(24_999, 25_000).valid);
        assert_eq!(
            judge(25_000, 25_000).reasons,
            ["mock write lag p99 25.0 us is not below 25.0 us"]
        );
        // A stricter limit applies as well.
        assert_eq!(
            judge(6_000, 5_000).reasons,
            ["mock write lag p99 6.0 us is not below 5.0 us"]
        );
        // The emit lag limit stays at 10 us whatever the mock limit.
        let mut summary = good_summary();
        summary.extend(summary_tail(Metric::EmitLag, 12_000, 12_000));
        let v = evaluate(&Evidence {
            intervals: &intervals,
            warmup_intervals: 1,
            summary: &summary,
            clock_steps: 0,
            load: None,
            counters: &NO_COUNTERS,
            diagnostics: &NO_DIAGNOSTICS,
            markers_expected: true,
            mock_lag_limit_ns: 25_000,
        });
        assert_eq!(v.reasons.len(), 1, "{v:?}");
        assert!(v.reasons[0].starts_with("emit lag p99 12.0 us"), "{v:?}");
    }

    #[test]
    fn missing_marker_evidence_invalidates_marked_scenarios_only() {
        let intervals: Vec<_> = (0..4).map(|i| interval(i, 1.0, 0, 100)).collect();
        let summary = summary_with(Metric::EmitLag, 1_000);
        let evidence = Evidence {
            intervals: &intervals,
            warmup_intervals: 1,
            summary: &summary,
            clock_steps: 0,
            load: None,
            counters: &NO_COUNTERS,
            diagnostics: &NO_DIAGNOSTICS,
            markers_expected: true,
            mock_lag_limit_ns: 10_000,
        };
        let v = evaluate(&evidence);
        assert_eq!(
            v.reasons,
            ["no mock write lag samples after the warmup"],
            "{v:?}"
        );
        assert!(
            evaluate(&Evidence {
                markers_expected: false,
                ..evidence
            })
            .valid
        );
    }

    #[test]
    fn a_lagging_load_generator_invalidates_the_run() {
        let intervals: Vec<_> = (0..4).map(|i| interval(i, 1.0, 0, 100)).collect();
        let judge = |p99_ns, far_tail_ns, diagnostics: &Diagnostics| {
            let mut summary = good_summary();
            summary.extend(summary_tail(Metric::EmitLag, p99_ns, far_tail_ns));
            evaluate(&Evidence {
                intervals: &intervals,
                warmup_intervals: 1,
                summary: &summary,
                clock_steps: 0,
                load: None,
                counters: &NO_COUNTERS,
                diagnostics,
                markers_expected: true,
                mock_lag_limit_ns: 10_000,
            })
        };
        assert!(judge(9_999, 999_999, &NO_DIAGNOSTICS).valid);
        let v = judge(10_000, 20_000, &NO_DIAGNOSTICS);
        assert_eq!(
            v.reasons,
            [
                "emit lag p99 10.0 us over 1000 sends is not below 10.0 us: the load generator \
                 fell behind its schedule; no send missed its spin window, so the thread lost \
                 the CPU while spinning or sending"
            ],
            "{v:?}"
        );
        let late = Diagnostics {
            late_wakeups: 9,
            busy_loop_misses: 3,
            max_deadline_miss_ns: 27_140,
            ..NO_DIAGNOSTICS
        };
        let v = judge(5_000, 1_000_000, &late);
        assert_eq!(
            v.reasons,
            [
                "emit lag p99.9 1000.0 us over 1000 sends is not below 1000.0 us: the load \
                 generator fell behind its schedule; 12 of 1000 sends were reached after their \
                 deadline (9 late wake-ups, 3 behind other events, worst by 27.1 us); a longer \
                 --spin-us absorbs late wake-ups"
            ],
            "{v:?}"
        );
    }

    #[test]
    fn request_slip_rule_needs_evidence_below_the_limit() {
        let slip = |p99_ns| Diagnostics {
            request_slip: Some(MetricSummary {
                count: 5000,
                min_ns: 3_000,
                mean_ns: 20_000.0,
                p50_ns: 15_000,
                p90_ns: 40_000,
                p99_ns,
                p999_ns: p99_ns,
                max_ns: p99_ns,
            }),
            ..NO_DIAGNOSTICS
        };
        let judge = |diagnostics: &Diagnostics| {
            let mut v = Validity::default();
            check_slip(&mut v, diagnostics, 200_000);
            v
        };
        assert!(judge(&slip(199_999)).valid);
        assert_eq!(
            judge(&slip(3_125_000)).reasons,
            [
                "request slip p99 3125.0 us over 5000 requests is not below 200.0 us: requests \
                 wait at the mock before it reads them, and TTFT measures that wait"
            ]
        );
        assert_eq!(
            judge(&NO_DIAGNOSTICS).reasons,
            ["no request slip samples after the warmup"]
        );
    }

    #[test]
    fn receive_timestamp_and_clock_conversion_rules() {
        let summary = good_summary();
        let judge = |intervals: &[IntervalResult], counters: &Counters| {
            evaluate(&Evidence {
                intervals,
                warmup_intervals: 1,
                summary: &summary,
                clock_steps: 0,
                load: None,
                counters,
                diagnostics: &NO_DIAGNOSTICS,
                markers_expected: true,
                mock_lag_limit_ns: 10_000,
            })
        };
        let mut intervals: Vec<_> = (0..4).map(|i| interval(i, 1.0, 0, 100)).collect();
        let mut counters = NO_COUNTERS;
        counters.rx_batches = 500;
        counters.rx_batches_without_timestamp = 1;
        // The contract requires kernel receive timestamps on Linux only.
        let v = judge(&intervals, &counters);
        assert_eq!(v.valid, !cfg!(target_os = "linux"), "{v:?}");
        if cfg!(target_os = "linux") {
            assert!(v.reasons[0].starts_with("1 of 500 receives"), "{v:?}");
        }

        // 2000 samples: two negative spans are 0.1%, three are too many.
        intervals[2].negative = 2;
        assert!(judge(&intervals, &NO_COUNTERS).valid);
        intervals[3].negative = 1;
        let v = judge(&intervals, &NO_COUNTERS);
        assert_eq!(v.reasons.len(), 1, "{v:?}");
        assert!(v.reasons[0].starts_with("3 of 2000 spans"), "{v:?}");
        // Negative spans of the warmup do not count.
        intervals[3].negative = 0;
        intervals[0].negative = 50;
        assert!(judge(&intervals, &NO_COUNTERS).valid);
    }

    #[test]
    fn negative_span_rule_denominator_excludes_ttft_reused() {
        // `TtftReused` duplicates a subset of `Ttft`'s samples; the rule's
        // denominator must stay the same, and so must the verdict, whether
        // or not a `TtftReused` summary is present.
        let intervals: Vec<_> = (0..2).map(|i| interval(i, 1.0, 0, 100)).collect();
        let mut with_negative = intervals;
        with_negative[0].negative = 2;
        let judge = |summary: &BTreeMap<Metric, MetricSummary>| {
            evaluate(&Evidence {
                intervals: &with_negative,
                warmup_intervals: 0,
                summary,
                clock_steps: 0,
                load: None,
                counters: &NO_COUNTERS,
                diagnostics: &NO_DIAGNOSTICS,
                markers_expected: false,
                mock_lag_limit_ns: 10_000,
            })
        };

        // 2 of the 1000 `Ttft` samples is 0.2%, above the 0.1% limit.
        let without_reused = summary_with(Metric::Ttft, 500);
        let v = judge(&without_reused);
        assert!(!v.valid, "{v:?}");
        assert!(v.reasons[0].starts_with("2 of 1000 spans"), "{v:?}");

        // Counting `ttft_reused`'s samples too would dilute 2 of 1000 to 2
        // of 2000 (0.1%, at the limit) or lower still with more reused
        // samples; the denominator and verdict must not move.
        let mut with_reused = without_reused.clone();
        with_reused.extend(summary_with(Metric::TtftReused, 500));
        assert_eq!(judge(&with_reused), v);
    }

    #[test]
    fn outcomes_split_failures_and_stale_retries() {
        let mut intervals: Vec<_> = (0..3).map(|i| interval(i, 1.0, 0, 1000)).collect();
        intervals[0].errors.insert("reset".into(), 100);
        intervals[1].errors.insert("timeout".into(), 2);
        intervals[2].errors.insert("http_503".into(), 1);
        intervals[2].errors.insert(STALE_RETRY.into(), 4);
        let outcomes = Outcomes::of(&intervals, 1);
        assert_eq!(
            outcomes,
            Outcomes {
                requests: 2000,
                failures: 3,
                stale_retries: 4
            }
        );
        assert!((outcomes.failure_share() - 3.0 / 2003.0).abs() < 1e-15);
        assert!(Outcomes::default().failure_share().abs() < f64::EPSILON);
    }

    #[test]
    fn selfcheck_needs_all_criteria() {
        let intervals: Vec<_> = (0..10).map(|i| interval(i, 100.0, 3000, 30)).collect();
        let plan: Vec<_> = (0..10).map(|i| planned(i, 100.0, 3000)).collect();
        let load = check_load(&intervals, &plan, 0, 100);
        let mut summary = summary_with(Metric::EmitLag, 4_000);
        summary.extend(summary_with(Metric::MockWriteLag, 6_000));
        let slip = |p99_ns| Diagnostics {
            request_slip: Some(summary_with(Metric::Ttft, p99_ns)[&Metric::Ttft]),
            ..NO_DIAGNOSTICS
        };
        let healthy = slip(80_000);
        let valid = Validity::default();
        let judge = |summary: &BTreeMap<Metric, MetricSummary>,
                     load: &LoadCheck,
                     diagnostics: &Diagnostics| {
            selfcheck(&SelfcheckEvidence {
                summary,
                load,
                diagnostics,
                slip_limit_ns: 200_000,
                mock_lag_limit_ns: 10_000,
                validity: &valid,
            })
        };
        let verdict = judge(&summary, &load, &healthy);
        assert!(verdict.pass, "{verdict:?}");
        assert_eq!(
            verdict.criteria.iter().map(|c| c.name).collect::<Vec<_>>(),
            [
                "emit_lag_p99",
                "mock_write_lag_p99",
                "request_slip_p99",
                "concurrency_deviation",
                "nominal_concurrency",
                "run_valid"
            ]
        );

        // The pilot's saturated mock: write lag in bounds, TTFT p99 3.1 ms.
        let verdict = judge(&summary, &load, &slip(3_125_000));
        assert!(!verdict.pass);
        assert_eq!(verdict.criteria[2].observed, "3125.00 us");
        assert_eq!(verdict.criteria[2].limit, "< 200 us");
        assert!(!verdict.criteria[2].pass);
        assert!(!judge(&summary, &load, &NO_DIAGNOSTICS).criteria[2].pass);

        summary.extend(summary_with(Metric::EmitLag, 12_000));
        let verdict = judge(&summary, &load, &healthy);
        assert!(!verdict.pass);
        assert!(!verdict.criteria[0].pass);

        let mut shaky = intervals.clone();
        shaky[3].concurrency.mean = 94.0;
        let load = check_load(&shaky, &plan, 0, 100);
        summary.extend(summary_with(Metric::EmitLag, 4_000));
        let verdict = judge(&summary, &load, &healthy);
        assert!(!verdict.criteria[3].pass);
        assert!(verdict.criteria[4].pass);
        assert!(!verdict.pass);

        // Schedule and achieved load agree in every interval but both sit 6%
        // below the target: the stream model is off.
        let low: Vec<_> = (0..10).map(|i| interval(i, 94.0, 3000, 30)).collect();
        let low_plan: Vec<_> = (0..10).map(|i| planned(i, 94.0, 3000)).collect();
        let load = check_load(&low, &low_plan, 0, 100);
        let verdict = judge(&summary, &load, &healthy);
        assert!(verdict.criteria[3].pass);
        assert!(!verdict.criteria[4].pass, "{verdict:?}");
        assert!(!verdict.pass);
    }

    #[test]
    fn selfcheck_mock_write_lag_criterion_follows_the_configured_limit() {
        let intervals: Vec<_> = (0..10).map(|i| interval(i, 100.0, 3000, 30)).collect();
        let plan: Vec<_> = (0..10).map(|i| planned(i, 100.0, 3000)).collect();
        let load = check_load(&intervals, &plan, 0, 100);
        let diagnostics = Diagnostics {
            request_slip: Some(summary_with(Metric::Ttft, 80_000)[&Metric::Ttft]),
            ..NO_DIAGNOSTICS
        };
        let valid = Validity::default();
        let mut summary = summary_with(Metric::EmitLag, 4_000);
        summary.extend(summary_with(Metric::MockWriteLag, 14_000));
        let judge = |mock_lag_limit_ns| {
            selfcheck(&SelfcheckEvidence {
                summary: &summary,
                load: &load,
                diagnostics: &diagnostics,
                slip_limit_ns: 200_000,
                mock_lag_limit_ns,
                validity: &valid,
            })
        };

        let verdict = judge(25_000);
        assert!(verdict.pass, "{verdict:?}");
        let mock = &verdict.criteria[1];
        assert_eq!(mock.name, "mock_write_lag_p99");
        assert_eq!(mock.observed, "14.00 us");
        assert_eq!(mock.limit, "< 25 us");
        assert!(mock.pass);
        // The emit lag criterion keeps the contract's bound.
        assert_eq!(verdict.criteria[0].limit, "< 10 us");

        let verdict = judge(10_000);
        assert!(!verdict.pass);
        assert_eq!(verdict.criteria[1].limit, "< 10 us");
        assert!(!verdict.criteria[1].pass);

        let verdict = judge(12_500);
        assert_eq!(verdict.criteria[1].limit, "< 12.5 us");
        assert!(!verdict.criteria[1].pass);
        assert!(judge(14_001).criteria[1].pass);
    }
}
