//! Human-readable summaries on stdout.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use brisk_bench_core::result::{IntervalResult, RunResult};
use brisk_bench_core::stats::Metric;

use crate::output::Extension;
use crate::run::RampOutcome;
use crate::validity::{LoadCheck, SelfcheckVerdict, micros};

/// Appends one formatted line.
macro_rules! put {
    ($out:expr, $($arg:tt)*) => {
        writeln!($out, $($arg)*).expect("writing to a String cannot fail")
    };
}

/// Prints the summary of a finished run.
pub(crate) fn print_run(run: &RunResult, extension: &Extension) {
    print!("{}", render_run(run, extension));
}

/// Prints the `selfcheck` verdict.
pub(crate) fn print_selfcheck(verdict: &SelfcheckVerdict, target: u32, run_at: u32) {
    let mut out = String::new();
    put!(
        out,
        "selfcheck at {run_at} streams (twice the target of {target}): {}",
        if verdict.pass { "PASS" } else { "FAIL" }
    );
    for c in &verdict.criteria {
        put!(
            out,
            "  [{}] {:<22} {} (limit {})",
            if c.pass { "x" } else { " " },
            c.name,
            c.observed,
            c.limit
        );
    }
    print!("{out}");
}

fn render_run(run: &RunResult, extension: &Extension) -> String {
    let mut out = String::new();
    let measured: Vec<&IntervalResult> = run
        .intervals
        .iter()
        .filter(|i| i.index >= run.warmup_intervals)
        .collect();
    header(&mut out, run, extension, measured.len());
    if let Some(load) = &extension.load_check {
        load_line(&mut out, load);
    }
    counts(&mut out, &measured, extension);
    if let Some(ramp) = &extension.ramp {
        ramp_table(&mut out, ramp);
    }
    metric_table(&mut out, run, &measured);
    out
}

fn header(out: &mut String, run: &RunResult, extension: &Extension, measured: usize) {
    put!(
        out,
        "{} {}  label={}{}  target={}{}",
        run.tool,
        run.scenario,
        run.label,
        run.pair_id
            .as_deref()
            .map_or_else(String::new, |id| format!("  pair={id}")),
        extension.target,
        if extension.tls { " (TLS)" } else { "" }
    );
    put!(
        out,
        "duration {:.1} s, warmup {} s, measured intervals {measured}, shards {}",
        extension.duration_s,
        run.warmup_intervals,
        extension.shards
    );
    if run.validity.valid {
        put!(out, "validity: VALID");
    } else {
        put!(out, "validity: INVALID");
        for reason in &run.validity.reasons {
            put!(out, "  - {reason}");
        }
    }
}

fn load_line(out: &mut String, load: &LoadCheck) {
    put!(
        out,
        "concurrency: mean {:.1} (offered {:.1}, nominal {}, {:+.2}% from nominal); \
         chunks/s: mean {:.0} (offered {:.0})",
        load.mean_concurrency,
        load.mean_planned_concurrency,
        load.nominal_concurrency,
        load.nominal_deviation * 100.0,
        load.mean_chunk_rate,
        load.mean_planned_chunk_rate
    );
    put!(
        out,
        "load check: {} of {} intervals off by > 5% (worst concurrency {:.2}%, chunk rate {:.2}%)",
        load.deviating,
        load.checked_intervals,
        load.worst_concurrency_deviation * 100.0,
        load.worst_chunk_rate_deviation * 100.0
    );
}

fn counts(out: &mut String, measured: &[&IntervalResult], extension: &Extension) {
    let requests: u64 = measured.iter().map(|i| i.requests).sum();
    let chunks: u64 = measured.iter().map(|i| i.chunks).sum();
    let mut errors: BTreeMap<&str, u64> = BTreeMap::new();
    for interval in measured {
        for (kind, n) in &interval.errors {
            *errors.entry(kind.as_str()).or_default() += n;
        }
    }
    let errors = if errors.is_empty() {
        "none".to_owned()
    } else {
        errors
            .iter()
            .map(|(k, n)| format!("{k}={n}"))
            .collect::<Vec<_>>()
            .join(" ")
    };
    put!(
        out,
        "after warmup: requests {requests}, chunks {chunks}, errors {errors}"
    );
    let c = &extension.counters;
    put!(
        out,
        "connections opened {}, reused sends {}, stale retries {}, idle closed by peer {}, \
         open at end {}, receives without kernel timestamp {} of {}",
        c.connections_opened,
        c.reused_sends,
        c.stale_retries,
        c.idle_closed,
        c.open_at_end,
        c.rx_batches_without_timestamp,
        c.rx_batches
    );
    let d = &extension.diagnostics;
    let fresh_ttft = d.fresh_conn_ttft.as_ref().map_or_else(String::new, |s| {
        format!(
            " (their TTFT p50 {:.1} us, p99 {:.1} us)",
            micros(s.p50_ns),
            micros(s.p99_ns)
        )
    });
    put!(
        out,
        "after warmup: sends on new connections {}{fresh_ttft}; sends reached after their \
         deadline {} ({} late wake-ups, {} behind other events, worst by {:.1} us)",
        d.fresh_conn_sends,
        d.deadline_misses(),
        d.late_wakeups,
        d.busy_loop_misses,
        micros(d.max_deadline_miss_ns)
    );
    if let Some(s) = &d.request_slip {
        put!(
            out,
            "request slip (send to the mock's receipt, pooled connections): count {}, \
             p50 {:.1} us, p99 {:.1} us, p99.9 {:.1} us, max {:.1} us",
            s.count,
            micros(s.p50_ns),
            micros(s.p99_ns),
            micros(s.p999_ns),
            micros(s.max_ns)
        );
    }
    if d.negative_slips > 0 {
        put!(
            out,
            "note: {} request slip(s) ended before they started: the load generator and the \
             mock do not share a clock",
            d.negative_slips
        );
    }
    if c.censored_requests > 0 {
        put!(
            out,
            "censored: {} timed-out or unfinished request(s) recorded {} latency lower bound(s)",
            c.censored_requests,
            c.censored_samples
        );
    }
}

fn ramp_table(out: &mut String, ramp: &RampOutcome) {
    put!(out, "ramp: {}", ramp.stopped_by);
    for step in &ramp.steps {
        put!(
            out,
            "  step {:>3}  {:>10.1} req/s  p50 {:>9.1} us  p99 {:>9.1} us  errors {}/{}  {}",
            step.step,
            step.rate,
            micros(step.p50_ns),
            micros(step.p99_ns),
            step.errors,
            step.requests + step.errors,
            if step.passed { "ok" } else { "FAIL" }
        );
    }
    match ramp.max_sustainable_rate {
        Some(rate) => put!(out, "max sustainable rate: {rate:.1} req/s"),
        None => put!(out, "max sustainable rate: none (the first step failed)"),
    }
}

fn metric_table(out: &mut String, run: &RunResult, measured: &[&IntervalResult]) {
    put!(
        out,
        "{:<16}{:>10}{:>11}{:>11}{:>11}{:>11}{:>11}   (us, after warmup)",
        "metric",
        "count",
        "p50",
        "p90",
        "p99",
        "p99.9",
        "max"
    );
    for metric in Metric::ALL {
        if let Some(s) = run.summary.get(&metric) {
            put!(
                out,
                "{:<16}{:>10}{:>11.1}{:>11.1}{:>11.1}{:>11.1}{:>11.1}",
                metric.name(),
                s.count,
                micros(s.p50_ns),
                micros(s.p90_ns),
                micros(s.p99_ns),
                micros(s.p999_ns),
                micros(s.max_ns)
            );
        }
    }
    let negative: u64 = measured.iter().map(|i| i.negative).sum();
    let saturated: u64 = measured.iter().map(|i| i.saturated).sum();
    if negative + saturated > 0 {
        put!(
            out,
            "note: {negative} span(s) ended before they started (clock conversion) and \
             {saturated} exceeded the histogram range"
        );
    }
}
