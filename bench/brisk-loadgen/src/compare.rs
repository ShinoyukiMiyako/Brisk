//! `compare`: paired bootstrap comparison of two arms of result files.
//!
//! Every input must be a valid run of the same scenario under the same load
//! (see [`load_params`]); ramp results mix rates and are refused. Failed
//! requests have no latency, so a quantile `q` is only meaningful while the
//! failures stay well inside its tail: each run's failure share must not
//! exceed `(1 − q) / 10` for the highest compared quantile. Runs breaking
//! either the validity or the tail rule are refused unless `--allow-invalid`
//! is given, and are listed in the output either way.
//!
//! The runs of the two arms are matched into repetitions (see [`pair_runs`])
//! and [`stats::compare_with`] builds the interval of Δ from the per-pair
//! deltas and arm A's from whole repetitions, so the run-to-run spread of
//! the load enters both. The M0 baseline criterion is judged per metric;
//! only the gate metrics (see [`gate_metrics`]) decide the gate, and a
//! single repetition, which cannot measure the run-to-run spread, fails it.

use std::path::{Path, PathBuf};

use anyhow::{Context as _, bail, ensure};
use brisk_bench_core::result::RunResult;
use brisk_bench_core::stats::{self, CompareOptions, Comparison, Metric};
use serde::Serialize;
use serde_json::{Map, Value};

use crate::cli::CompareCmd;
use crate::run::RAMP_SCENARIO;
use crate::validity::{Outcomes, micros};

/// M0 exit criterion: half-width of arm A's p99 95% interval relative to
/// its p99.
pub(crate) const BASELINE_CI_LIMIT: f64 = 0.05;
/// Gate metrics when `--gate-metrics` is not given, as far as compared.
const DEFAULT_GATE_METRICS: [Metric; 2] = [Metric::Ttft, Metric::ChunkLatency];
/// Largest failure share relative to the tail `1 − q` of the highest
/// compared quantile.
const TAIL_FAILURE_FRACTION: f64 = 0.1;
/// Parameters that may differ between compared runs: the shared options
/// (target, label, output file, CPU placement, seed, pairing id, timeout),
/// the length of the measurement, and the `bigbody` size list (each result
/// file holds one size, which its scenario names).
const NON_LOAD_PARAMS: [&str; 3] = ["common", "measure_s", "sizes"];

/// The comparison document.
#[derive(Debug, Serialize)]
struct CompareOutput<'a> {
    scenario: &'a str,
    /// Arm A's result files in pair order, which is their input order.
    a_files: &'a [PathBuf],
    /// Arm B's result files in pair order: `b_files[i]` pairs with
    /// `a_files[i]` and with every comparison's `per_pair[i]`.
    b_files: Vec<&'a PathBuf>,
    /// Arm A's labels in pair order.
    a_labels: Vec<&'a str>,
    /// Arm B's labels in pair order.
    b_labels: Vec<&'a str>,
    invalid_runs: Vec<String>,
    /// Largest failure share the highest compared quantile tolerates.
    tail_failure_limit: f64,
    /// Runs whose failure share exceeds it.
    tail_failure_runs: Vec<String>,
    /// What matched the runs of the two arms into repetitions.
    paired_by: PairedBy,
    /// The repetitions, in the order of every comparison's `per_pair`.
    pairs: Vec<PairOutput<'a>>,
    /// Only one repetition was given: the intervals come from resampling
    /// within its runs alone and understate the run-to-run spread.
    single_repetition_fallback: bool,
    /// The baseline criterion's limit on arm A's p99 CI half-width ratio.
    baseline_p99_ci_limit: f64,
    /// Metrics whose baseline criterion decides the gate.
    gate_metrics: &'a [Metric],
    /// One entry per compared metric, in `--metric` order.
    comparisons: Vec<MetricOutput<'a>>,
    /// Every compared metric meets the baseline criterion, gate metric or
    /// not.
    baseline_p99_ci_ok: bool,
    /// Every gate metric meets the baseline criterion.
    gate_pass: bool,
}

/// One repetition of the document.
#[derive(Debug, Serialize)]
struct PairOutput<'a> {
    /// The pairing id, or `seed <n>` for runs paired by their seed.
    key: &'a str,
    /// The schedule seed the two runs share, when recorded.
    seed: Option<u64>,
    /// Result file of the A run.
    a_file: &'a Path,
    /// Result file of the B run.
    b_file: &'a Path,
}

/// One metric's comparison with its baseline verdict.
#[derive(Debug, Serialize)]
struct MetricOutput<'a> {
    #[serde(flatten)]
    comparison: &'a Comparison,
    /// The metric is one of the gate metrics.
    gate: bool,
    /// The baseline criterion can be judged: there are at least two
    /// repetitions, so arm A's interval includes the run-to-run spread.
    baseline_evaluable: bool,
    /// The criterion is evaluable and arm A's p99 CI half-width ratio is
    /// below [`BASELINE_CI_LIMIT`].
    pass: bool,
}

/// What matched the runs of the two arms into repetitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum PairedBy {
    /// The pairing id each run recorded from `--pair-id`.
    PairId,
    /// The schedule seed, for runs recorded without a pairing id.
    Seed,
}

impl PairedBy {
    fn describe(self) -> &'static str {
        match self {
            Self::PairId => "pairing id",
            Self::Seed => "seed",
        }
    }
}

/// One repetition: its key and the positions of its runs in the two arms.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Pair {
    /// The pairing id, or `seed <n>`.
    key: String,
    /// Position of the A run.
    a: usize,
    /// Position of the B run.
    b: usize,
    /// The schedule seed the two runs share, when recorded.
    seed: Option<u64>,
}

/// The parameters that define the offered load: everything recorded except
/// [`NON_LOAD_PARAMS`].
fn load_params(params: &Value) -> Option<Map<String, Value>> {
    params.as_object().map(|object| {
        object
            .iter()
            .filter(|(key, _)| !NON_LOAD_PARAMS.contains(&key.as_str()))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    })
}

/// Keys whose values differ between two parameter sets.
fn differing_keys(a: &Map<String, Value>, b: &Map<String, Value>) -> Vec<String> {
    let mut keys: Vec<String> = a
        .keys()
        .chain(b.keys())
        .filter(|key| a.get(*key) != b.get(*key))
        .cloned()
        .collect();
    keys.sort_unstable();
    keys.dedup();
    keys
}

/// Largest failure share tolerated when comparing up to quantile `q_max`.
fn tail_failure_limit(q_max: f64) -> f64 {
    (1.0 - q_max) * TAIL_FAILURE_FRACTION
}

fn read_arm(files: &[PathBuf]) -> anyhow::Result<Vec<RunResult>> {
    files
        .iter()
        .map(|path| {
            RunResult::read_json(path).with_context(|| format!("reading {}", path.display()))
        })
        .collect()
}

/// One input run with the file it was read from.
type Input<'a> = (&'a RunResult, &'a PathBuf);

/// Requires every input to share the first A file's scenario and load, and
/// refuses ramp results.
fn check_comparable<'a>(
    scenario: &str,
    first: Input<'a>,
    inputs: impl Iterator<Item = Input<'a>>,
) -> anyhow::Result<()> {
    ensure!(
        scenario != RAMP_SCENARIO,
        "{scenario} results mix the latencies of every ramp step; \
         compare fixed-rate nonstream runs (--rate) instead"
    );
    let first_load = load_params(&first.0.params)
        .with_context(|| format!("{}: params is not an object", first.1.display()))?;
    for (run, path) in inputs {
        ensure!(
            run.scenario == scenario,
            "{} is scenario {:?}, the first A file is {:?}; only runs of one scenario compare",
            path.display(),
            run.scenario,
            scenario
        );
        let load = load_params(&run.params)
            .with_context(|| format!("{}: params is not an object", path.display()))?;
        let differing = differing_keys(&first_load, &load);
        ensure!(
            differing.is_empty(),
            "{} ran another load than {} (parameters {}); only runs of one load compare",
            path.display(),
            first.1.display(),
            differing.join(", ")
        );
    }
    Ok(())
}

/// Schedule seed a result was run with (`--seed`).
fn seed_of(run: &RunResult) -> Option<u64> {
    run.params.pointer("/common/seed").and_then(Value::as_u64)
}

/// Matches the runs of the two arms into repetitions, in arm A's order.
///
/// Runs pair by their pairing id when all of them carry one, and by their
/// seed when none does: result files written before `--pair-id` existed
/// carry only the seed, which the M0 session sets per repetition. Each key
/// must name exactly one run per arm, and the two runs of a pair must share
/// their seed, or they did not run the same schedule.
fn pair_runs(a: &[Input<'_>], b: &[Input<'_>]) -> anyhow::Result<(PairedBy, Vec<Pair>)> {
    let runs = a.len() + b.len();
    let with_id = a
        .iter()
        .chain(b)
        .filter(|(run, _)| run.pair_id.is_some())
        .count();
    let paired_by = match with_id {
        0 => PairedBy::Seed,
        n if n == runs => PairedBy::PairId,
        n => bail!(
            "{n} of the {runs} input runs carry a pairing id (--pair-id) and the others do not; \
             runs pair either all by pairing id or all by seed"
        ),
    };
    let keys = |arm: &str, inputs: &[Input<'_>]| -> anyhow::Result<Vec<String>> {
        let keys = inputs
            .iter()
            .map(|(run, path)| match &run.pair_id {
                Some(id) => Ok(id.clone()),
                None => seed_of(run)
                    .map(|seed| format!("seed {seed}"))
                    .with_context(|| {
                        format!(
                            "{} carries neither a pairing id nor a seed (params.common.seed)",
                            path.display()
                        )
                    }),
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        for (i, key) in keys.iter().enumerate() {
            if let Some(j) = keys[..i].iter().position(|other| other == key) {
                bail!(
                    "{} and {} of arm {arm} both belong to repetition {key}; \
                     give every repetition its own --pair-id",
                    inputs[j].1.display(),
                    inputs[i].1.display()
                );
            }
        }
        Ok(keys)
    };
    let (a_keys, b_keys) = (keys("A", a)?, keys("B", b)?);
    let mut pairs = Vec::with_capacity(a.len());
    for (i, key) in a_keys.iter().enumerate() {
        let j = b_keys
            .iter()
            .position(|other| other == key)
            .with_context(|| {
                format!(
                    "repetition {key} ({}) has no run in arm B",
                    a[i].1.display()
                )
            })?;
        let (a_seed, b_seed) = (seed_of(a[i].0), seed_of(b[j].0));
        if let (Some(a_seed), Some(b_seed)) = (a_seed, b_seed) {
            ensure!(
                a_seed == b_seed,
                "repetition {key}: {} ran seed {a_seed} and {} seed {b_seed}; \
                 the two runs of a pair must share their schedule",
                a[i].1.display(),
                b[j].1.display()
            );
        }
        pairs.push(Pair {
            key: key.clone(),
            a: i,
            b: j,
            seed: a_seed.or(b_seed),
        });
    }
    if let Some(j) = b_keys.iter().position(|key| !a_keys.contains(key)) {
        bail!(
            "repetition {} ({}) has no run in arm A",
            b_keys[j],
            b[j].1.display()
        );
    }
    Ok((paired_by, pairs))
}

/// Arm B's runs in pair order; arm A's order already is the pair order.
fn b_in_pair_order<T>(items: Vec<T>, pairs: &[Pair]) -> Vec<T> {
    let mut slots: Vec<Option<T>> = items.into_iter().map(Some).collect();
    pairs
        .iter()
        .map(|pair| {
            slots[pair.b]
                .take()
                .expect("pair_runs matches every B run exactly once")
        })
        .collect()
}

/// The metrics that decide the gate.
///
/// Named ones (`--gate-metrics`) must all be compared. Without the flag the
/// compared ones of [`DEFAULT_GATE_METRICS`] gate, or every compared metric
/// when none of those is, so that a comparison of other metrics (S2's
/// `request_latency`) still runs without the flag and still gates.
fn gate_metrics(metrics: &[Metric], explicit: Option<&[Metric]>) -> anyhow::Result<Vec<Metric>> {
    let Some(gate) = explicit else {
        let defaults: Vec<Metric> = DEFAULT_GATE_METRICS
            .into_iter()
            .filter(|metric| metrics.contains(metric))
            .collect();
        return Ok(if defaults.is_empty() {
            metrics.to_vec()
        } else {
            defaults
        });
    };
    let missing: Vec<&str> = gate
        .iter()
        .filter(|metric| !metrics.contains(metric))
        .map(|metric| metric.name())
        .collect();
    ensure!(
        missing.is_empty(),
        "gate metric(s) {} are not compared (--metric {}); \
         pass --gate-metrics with compared metrics",
        missing.join(","),
        names(metrics)
    );
    Ok(gate.to_vec())
}

/// Judges every comparison against the baseline criterion.
///
/// A single repetition fails it whatever its ratio: its interval comes from
/// within the runs alone, and such intervals excluded 0 in 39 of 108 A/A
/// comparisons of the pilot, so a small half-width there proves nothing.
fn verdicts<'a>(comparisons: &'a [Comparison], gate: &[Metric]) -> Vec<MetricOutput<'a>> {
    comparisons
        .iter()
        .map(|comparison| {
            let baseline_evaluable = !comparison.single_repetition_fallback;
            MetricOutput {
                comparison,
                gate: gate.contains(&comparison.metric),
                baseline_evaluable,
                pass: baseline_evaluable
                    && comparison.baseline_p99_ci_half_width_ratio < BASELINE_CI_LIMIT,
            }
        })
        .collect()
}

/// Inputs that should not be compared, with the reason.
#[derive(Debug)]
struct Screening {
    /// Runs judged invalid.
    invalid: Vec<String>,
    /// Largest failure share the highest quantile tolerates.
    tail_limit: f64,
    /// Runs whose failure share exceeds it.
    tail_failures: Vec<String>,
}

impl Screening {
    fn new<'a>(quantiles: &[f64], inputs: impl Iterator<Item = Input<'a>> + Clone) -> Self {
        let invalid = inputs
            .clone()
            .filter(|(run, _)| !run.validity.valid)
            .map(|(run, path)| format!("{}: {}", path.display(), run.validity.reasons.join("; ")))
            .collect();
        let q_max = quantiles.iter().copied().fold(0.0, f64::max);
        let tail_limit = tail_failure_limit(q_max);
        let tail_failures = inputs
            .filter_map(|(run, path)| {
                let outcomes = Outcomes::of(&run.intervals, run.warmup_intervals);
                (outcomes.failure_share() > tail_limit).then(|| {
                    format!(
                        "{}: {} of {} requests failed, more than {:.3}% for the {} tail",
                        path.display(),
                        outcomes.failures,
                        outcomes.requests + outcomes.failures,
                        tail_limit * 100.0,
                        quantile_label(q_max)
                    )
                })
            })
            .collect();
        Self {
            invalid,
            tail_limit,
            tail_failures,
        }
    }

    /// Reports the flagged runs; refuses them unless `allow` is set.
    fn enforce(&self, allow: bool) -> anyhow::Result<()> {
        for entry in &self.invalid {
            eprintln!("invalid run {entry}");
        }
        for entry in &self.tail_failures {
            eprintln!("too many failures: {entry}");
        }
        if !allow {
            ensure!(
                self.invalid.is_empty(),
                "{} input run(s) are invalid; rerun them or pass --allow-invalid",
                self.invalid.len()
            );
            ensure!(
                self.tail_failures.is_empty(),
                "{} input run(s) failed too many requests for the highest quantile; \
                 compare fewer quantiles, rerun them or pass --allow-invalid",
                self.tail_failures.len()
            );
        }
        Ok(())
    }
}

/// Runs the comparison, prints the table and writes the JSON document.
pub(crate) fn run(cmd: &CompareCmd) -> anyhow::Result<()> {
    crate::output::check_writable(&cmd.out)?;
    let a = read_arm(&cmd.a)?;
    let b = read_arm(&cmd.b)?;
    let scenario = a[0].scenario.as_str();
    let a_inputs: Vec<Input<'_>> = a.iter().zip(&cmd.a).collect();
    let b_inputs: Vec<Input<'_>> = b.iter().zip(&cmd.b).collect();
    let inputs = || a_inputs.iter().chain(&b_inputs).copied();
    check_comparable(scenario, a_inputs[0], inputs())?;
    // After the scenario check, whose refusal (of a ramp, say) names the
    // real problem better than a missing gate metric would.
    let gate = gate_metrics(&cmd.metric, cmd.gate_metrics.as_deref())?;
    let screening = Screening::new(&cmd.quantiles, inputs());
    screening.enforce(cmd.allow_invalid)?;
    let (paired_by, pairs) = pair_runs(&a_inputs, &b_inputs)?;
    let b = b_in_pair_order(b, &pairs);
    let options = CompareOptions {
        resamples: cmd.resamples,
        block_len: cmd.block_len,
        seed: cmd.seed,
    };
    let results = cmd
        .metric
        .iter()
        .map(|&metric| {
            stats::compare_with(&a, &b, metric, &cmd.quantiles, &options)
                .with_context(|| format!("comparing {metric}"))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let reports = verdicts(&results, &gate);
    let document = CompareOutput {
        scenario,
        a_files: &cmd.a,
        b_files: pairs.iter().map(|pair| &cmd.b[pair.b]).collect(),
        a_labels: a.iter().map(|run| run.label.as_str()).collect(),
        b_labels: b.iter().map(|run| run.label.as_str()).collect(),
        tail_failure_limit: screening.tail_limit,
        invalid_runs: screening.invalid,
        tail_failure_runs: screening.tail_failures,
        paired_by,
        pairs: pairs
            .iter()
            .map(|pair| PairOutput {
                key: &pair.key,
                seed: pair.seed,
                a_file: &cmd.a[pair.a],
                b_file: &cmd.b[pair.b],
            })
            .collect(),
        single_repetition_fallback: pairs.len() == 1,
        baseline_p99_ci_limit: BASELINE_CI_LIMIT,
        gate_metrics: &gate,
        baseline_p99_ci_ok: reports.iter().all(|r| r.pass),
        gate_pass: reports.iter().filter(|r| r.gate).all(|r| r.pass),
        comparisons: reports,
    };
    print!("{}", render(&document));
    write_json(&cmd.out, &document)?;
    println!("comparison written to {}", cmd.out.display());
    Ok(())
}

fn write_json<T: Serialize>(path: &Path, value: &T) -> anyhow::Result<()> {
    let json = serde_json::to_vec_pretty(value).context("serializing the comparison")?;
    std::fs::write(path, json).with_context(|| format!("writing {}", path.display()))
}

/// `0.999` as `p99.9`.
fn quantile_label(q: f64) -> String {
    let percent = (q * 100.0 * 1e6).round() / 1e6;
    format!("p{percent}")
}

fn distinct(labels: &[&str]) -> String {
    let mut unique = labels.to_vec();
    unique.sort_unstable();
    unique.dedup();
    unique.join(",")
}

/// Metric names as a comma-separated list.
fn names(metrics: &[Metric]) -> String {
    metrics
        .iter()
        .map(|metric| metric.name())
        .collect::<Vec<_>>()
        .join(",")
}

fn pass_fail(pass: bool) -> &'static str {
    if pass { "PASS" } else { "FAIL" }
}

/// Appends `text` and a line break.
fn push_line(out: &mut String, text: &str) {
    out.push_str(text);
    out.push('\n');
}

fn render(doc: &CompareOutput<'_>) -> String {
    let mut out = String::new();
    push_line(
        &mut out,
        &format!(
            "compare {}: A = {} ({} run(s)), B = {} ({} run(s)){}",
            doc.scenario,
            distinct(&doc.a_labels),
            doc.a_labels.len(),
            distinct(&doc.b_labels),
            doc.b_labels.len(),
            if doc.invalid_runs.is_empty() && doc.tail_failure_runs.is_empty() {
                ""
            } else {
                "  [includes INVALID runs or runs failing too often for the tail]"
            }
        ),
    );
    push_line(
        &mut out,
        &format!(
            "repetitions: {}, paired by {}: {}",
            doc.pairs.len(),
            doc.paired_by.describe(),
            doc.pairs
                .iter()
                .map(|pair| pair.key)
                .collect::<Vec<_>>()
                .join(", ")
        ),
    );
    if doc.single_repetition_fallback {
        push_line(
            &mut out,
            "single repetition: within-run block bootstrap only; the intervals leave out \
             the run-to-run spread and understate the uncertainty, so the baseline \
             criterion is not evaluable and fails (use at least 3 repetitions)",
        );
    } else if let Some(first) = doc.comparisons.first() {
        let c = first.comparison;
        push_line(
            &mut out,
            &format!(
                "intervals: delta = t over the {} per-pair deltas ({} degree(s) of freedom); \
                 A = bootstrap percentile widened x{:.2} for {} repetitions",
                c.pairs,
                c.pairs - 1,
                c.a_interval_scale,
                c.pairs
            ),
        );
    }
    for report in &doc.comparisons {
        render_metric(&mut out, report);
    }
    let failed: Vec<&str> = doc
        .comparisons
        .iter()
        .filter(|report| report.gate && !report.pass)
        .map(|report| report.comparison.metric.name())
        .collect();
    push_line(
        &mut out,
        &format!(
            "gate ({}): {}{}",
            names(doc.gate_metrics),
            pass_fail(doc.gate_pass),
            if failed.is_empty() {
                String::new()
            } else {
                format!(" on {}", failed.join(","))
            }
        ),
    );
    out
}

/// One metric's table, per-pair deltas and baseline verdict.
fn render_metric(out: &mut String, report: &MetricOutput<'_>) {
    let c = report.comparison;
    push_line(
        out,
        &format!(
            "metric {}: A {} intervals / {} samples, B {} intervals / {} samples, \
             {} resamples of {} repetition(s), block {}",
            c.metric,
            c.a_intervals,
            c.a_count,
            c.b_intervals,
            c.b_count,
            c.resamples,
            c.pairs,
            c.block_len
        ),
    );
    push_line(
        out,
        &format!(
            "  {:<9}{:>12}{:>12}{:>12}   {:<27}{:>14}   {}",
            "quantile",
            "A (us)",
            "B (us)",
            "delta (us)",
            "delta 95% CI (us)",
            "A CI half-w.",
            "per-pair delta (us)"
        ),
    );
    for (i, q) in c.quantiles.iter().enumerate() {
        let interval = format!(
            "[{:+.2}, {:+.2}]",
            q.delta_ci_low_ns / 1e3,
            q.delta_ci_high_ns / 1e3
        );
        let per_pair = c
            .per_pair
            .iter()
            .map(|pair| format!("{:+.2}", delta_us(pair.quantiles[i].delta_ns)))
            .collect::<Vec<_>>()
            .join(" ");
        push_line(
            out,
            &format!(
                "  {:<9}{:>12.2}{:>12.2}{:>+12.2}   {:<27}{:>13.2}%   {}",
                quantile_label(q.quantile),
                micros(q.a_ns),
                micros(q.b_ns),
                delta_us(q.delta_ns),
                interval,
                q.a_ci_half_width_ratio * 100.0,
                per_pair
            ),
        );
    }
    push_line(
        out,
        &format!(
            "  A p99 95% CI half-width: {:.2}% of p99 (limit {:.0}%): {}{} ({})",
            c.baseline_p99_ci_half_width_ratio * 100.0,
            BASELINE_CI_LIMIT * 100.0,
            pass_fail(report.pass),
            if report.baseline_evaluable {
                ""
            } else {
                ", not evaluable with one repetition"
            },
            if report.gate {
                "gate metric"
            } else {
                "reported, not gated"
            }
        ),
    );
}

#[expect(clippy::cast_precision_loss, reason = "display only")]
fn delta_us(ns: i64) -> f64 {
    ns as f64 / 1e3
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use brisk_bench_core::fingerprint::Fingerprint;
    use brisk_bench_core::result::{SCHEMA_VERSION, Validity};
    use brisk_bench_core::stats::IntervalRecorder;

    use super::*;

    #[test]
    fn quantile_labels_are_short() {
        assert_eq!(quantile_label(0.5), "p50");
        assert_eq!(quantile_label(0.99), "p99");
        assert_eq!(quantile_label(0.999), "p99.9");
        assert_eq!(quantile_label(0.9999), "p99.99");
    }

    #[test]
    fn load_params_ignore_target_placement_and_length() {
        let a = serde_json::json!({
            "common": {"url": "http://direct:1", "label": "direct-P", "seed": 1},
            "shape": {"concurrency": 1000, "chunk_rate": 30.0},
            "warmup_s": 150,
            "measure_s": 300
        });
        let b = serde_json::json!({
            "common": {"url": "http://floor:2", "label": "floor-P", "seed": 2},
            "shape": {"concurrency": 1000, "chunk_rate": 30.0},
            "warmup_s": 150,
            "measure_s": 600
        });
        let (a, b) = (load_params(&a).unwrap(), load_params(&b).unwrap());
        assert!(differing_keys(&a, &b).is_empty());

        let c = serde_json::json!({
            "common": {},
            "shape": {"concurrency": 2000, "chunk_rate": 30.0},
            "rate": 5.0,
            "sizes": [{"label": "10m", "bytes": 10_485_760}]
        });
        let c = load_params(&c).unwrap();
        assert_eq!(differing_keys(&a, &c), ["rate", "shape", "warmup_s"]);
        assert!(load_params(&serde_json::json!([1])).is_none());
    }

    #[test]
    fn tail_failure_limit_is_a_tenth_of_the_tail() {
        assert!((tail_failure_limit(0.999) - 1e-4).abs() < 1e-12);
        assert!((tail_failure_limit(0.99) - 1e-3).abs() < 1e-12);
        assert!(tail_failure_limit(1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn distinct_labels_join() {
        assert_eq!(distinct(&["direct-P", "direct-P"]), "direct-P");
        assert_eq!(distinct(&["b", "a", "b"]), "a,b");
    }

    /// A run of three one-second intervals whose chunk latencies sit around
    /// `base` ns, recorded with the given seed and pairing id.
    fn run_with(seed: u64, pair_id: Option<&str>, base: u64) -> RunResult {
        let mut recorder = IntervalRecorder::new(0);
        for i in 0..3 {
            for k in 0..100 {
                recorder.record(Metric::ChunkLatency, i * 1_000_000_000 + k, base + k * 10);
            }
        }
        let intervals = recorder.finish(3_000_000_000);
        RunResult {
            schema_version: SCHEMA_VERSION,
            tool: "brisk-loadgen".into(),
            scenario: "stream".into(),
            label: "x".into(),
            pair_id: pair_id.map(str::to_owned),
            params: serde_json::json!({"common": {"seed": seed}}),
            fingerprint: Fingerprint::collect(),
            started_unix_ms: 0,
            warmup_intervals: 0,
            intervals,
            summary: BTreeMap::new(),
            validity: Validity::default(),
        }
    }

    fn paths(arm: &str, n: usize) -> Vec<PathBuf> {
        (0..n)
            .map(|i| PathBuf::from(format!("{arm}{i}.json")))
            .collect()
    }

    fn inputs<'a>(runs: &'a [RunResult], files: &'a [PathBuf]) -> Vec<Input<'a>> {
        runs.iter().zip(files).collect()
    }

    fn pair_error(a: &[RunResult], b: &[RunResult]) -> String {
        let (a_files, b_files) = (paths("a", a.len()), paths("b", b.len()));
        let err = pair_runs(&inputs(a, &a_files), &inputs(b, &b_files)).unwrap_err();
        format!("{err:#}")
    }

    #[test]
    fn runs_without_pairing_ids_pair_by_seed_in_any_order() {
        let a: Vec<_> = [1, 2, 3].map(|s| run_with(s, None, 10_000)).into();
        let b: Vec<_> = [3, 1, 2].map(|s| run_with(s, None, 11_000)).into();
        let (a_files, b_files) = (paths("a", 3), paths("b", 3));
        let (by, pairs) = pair_runs(&inputs(&a, &a_files), &inputs(&b, &b_files)).unwrap();
        assert_eq!(by, PairedBy::Seed);
        let matched: Vec<_> = pairs
            .iter()
            .map(|p| (p.key.as_str(), p.a, p.b, p.seed))
            .collect();
        assert_eq!(
            matched,
            [
                ("seed 1", 0, 1, Some(1)),
                ("seed 2", 1, 2, Some(2)),
                ("seed 3", 2, 0, Some(3))
            ]
        );
        let reordered = b_in_pair_order(b, &pairs);
        assert_eq!(
            reordered.iter().map(seed_of).collect::<Vec<_>>(),
            [Some(1), Some(2), Some(3)]
        );
    }

    #[test]
    fn runs_pair_by_their_pairing_id() {
        let a = vec![
            run_with(7, Some("s1-P-r1"), 10_000),
            run_with(7, Some("s1-P-r2"), 10_000),
        ];
        let b = vec![
            run_with(7, Some("s1-P-r2"), 10_000),
            run_with(7, Some("s1-P-r1"), 10_000),
        ];
        let (a_files, b_files) = (paths("a", 2), paths("b", 2));
        let (by, pairs) = pair_runs(&inputs(&a, &a_files), &inputs(&b, &b_files)).unwrap();
        assert_eq!(by, PairedBy::PairId);
        assert_eq!(
            pairs.iter().map(|p| (p.a, p.b)).collect::<Vec<_>>(),
            [(0, 1), (1, 0)]
        );
        assert_eq!(pairs[0].key, "s1-P-r1");
    }

    #[test]
    fn pairs_with_different_seeds_are_refused() {
        let a = vec![run_with(1, Some("s1-P-r1"), 10_000)];
        let b = vec![run_with(2, Some("s1-P-r1"), 10_000)];
        let err = pair_error(&a, &b);
        assert!(
            err.contains("ran seed 1") && err.contains("seed 2"),
            "{err}"
        );
        assert!(err.contains("share their schedule"), "{err}");
    }

    #[test]
    fn ambiguous_or_incomplete_pairings_are_refused() {
        // Two repetitions run with one seed and no pairing id.
        let same_seed = [run_with(1, None, 10_000), run_with(1, None, 10_000)];
        assert!(pair_error(&same_seed, &same_seed).contains("--pair-id"));
        // Pairing ids on some runs only.
        let mixed_a = [run_with(1, Some("r1"), 10_000)];
        let mixed_b = [run_with(1, None, 10_000)];
        assert!(pair_error(&mixed_a, &mixed_b).contains("all by pairing id"));
        // A repetition missing from one arm.
        let a = [run_with(1, None, 10_000), run_with(2, None, 10_000)];
        let b = [run_with(1, None, 10_000), run_with(3, None, 10_000)];
        assert!(pair_error(&a, &b).contains("seed 2 (a1.json) has no run in arm B"));
        let a = [run_with(1, None, 10_000)];
        assert!(pair_error(&a, &b).contains("seed 3 (b1.json) has no run in arm A"));
        // No seed and no pairing id: nothing declares the repetition.
        let mut bare = run_with(1, None, 10_000);
        bare.params = serde_json::json!({});
        assert!(pair_error(&[bare.clone()], &[bare]).contains("neither a pairing id nor a seed"));
    }

    #[test]
    fn named_gate_metrics_must_be_compared() {
        let compared = [Metric::Ttft, Metric::ChunkLatency, Metric::ChunkWire];
        assert_eq!(
            gate_metrics(&compared, Some(&[Metric::ChunkWire])).unwrap(),
            [Metric::ChunkWire]
        );
        let err = gate_metrics(&[Metric::RequestLatency], Some(&[Metric::Ttft]))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("ttft") && err.contains("request_latency"),
            "{err}"
        );
    }

    #[test]
    fn default_gate_metrics_follow_the_compared_ones() {
        let s1 = [Metric::ChunkLatency, Metric::Ttft, Metric::ChunkWire];
        assert_eq!(
            gate_metrics(&s1, None).unwrap(),
            [Metric::Ttft, Metric::ChunkLatency]
        );
        assert_eq!(
            gate_metrics(&[Metric::ChunkWire, Metric::ChunkLatency], None).unwrap(),
            [Metric::ChunkLatency]
        );
        // Neither default compared: every compared metric gates, as the
        // S2 comparison of request_latency did before the flag existed.
        assert_eq!(
            gate_metrics(&[Metric::RequestLatency], None).unwrap(),
            [Metric::RequestLatency]
        );
        assert_eq!(
            gate_metrics(&[Metric::ChunkWire], None).unwrap(),
            [Metric::ChunkWire]
        );
    }

    fn comparison(metric: Metric, runs: u64, baseline_ratio: f64) -> Comparison {
        let a: Vec<_> = (0..runs).map(|s| run_with(s, None, 10_000)).collect();
        let b: Vec<_> = (0..runs).map(|s| run_with(s, None, 12_000)).collect();
        let options = CompareOptions {
            resamples: 50,
            ..CompareOptions::default()
        };
        let mut c =
            stats::compare_with(&a, &b, Metric::ChunkLatency, &[0.5, 0.99], &options).unwrap();
        c.metric = metric;
        c.baseline_p99_ci_half_width_ratio = baseline_ratio;
        c
    }

    #[test]
    fn only_gate_metrics_decide_the_gate() {
        let comparisons = [
            comparison(Metric::Ttft, 3, 0.049),
            comparison(Metric::ChunkLatency, 3, 0.01),
            comparison(Metric::ChunkWire, 3, 0.09),
        ];
        let gate = [Metric::Ttft, Metric::ChunkLatency];
        let reports = verdicts(&comparisons, &gate);
        let flags: Vec<_> = reports.iter().map(|r| (r.gate, r.pass)).collect();
        assert_eq!(flags, [(true, true), (true, true), (false, false)]);
        assert!(reports.iter().all(|r| r.baseline_evaluable));

        let json = serde_json::to_value(&reports[2]).unwrap();
        assert_eq!(json["metric"], "chunk_wire");
        assert_eq!(json["gate"], false);
        assert_eq!(json["pass"], false);
        assert_eq!(json["baseline_evaluable"], true);
        assert_eq!(json["delta_interval"], "paired_t");
        assert_eq!(json["baseline_p99_ci_half_width_ratio"], 0.09);
        assert_eq!(json["per_pair"].as_array().unwrap().len(), 3);
        assert!(json["per_pair"][0]["quantiles"][0]["delta_ns"].is_i64());
        assert_eq!(json["single_repetition_fallback"], false);
    }

    fn document<'a>(
        comparisons: Vec<MetricOutput<'a>>,
        pairs: Vec<PairOutput<'a>>,
        gate_metrics: &'a [Metric],
    ) -> CompareOutput<'a> {
        CompareOutput {
            scenario: "stream",
            a_files: &[],
            b_files: Vec::new(),
            a_labels: vec!["direct-P"; pairs.len()],
            b_labels: vec!["floor-P"; pairs.len()],
            invalid_runs: Vec::new(),
            tail_failure_limit: 1e-4,
            tail_failure_runs: Vec::new(),
            paired_by: PairedBy::Seed,
            single_repetition_fallback: pairs.len() == 1,
            pairs,
            baseline_p99_ci_limit: BASELINE_CI_LIMIT,
            gate_metrics,
            baseline_p99_ci_ok: comparisons.iter().all(|c| c.pass),
            gate_pass: comparisons.iter().filter(|c| c.gate).all(|c| c.pass),
            comparisons,
        }
    }

    fn pair_output(key: &str) -> PairOutput<'_> {
        PairOutput {
            key,
            seed: None,
            a_file: Path::new("a.json"),
            b_file: Path::new("b.json"),
        }
    }

    #[test]
    fn text_names_each_metric_verdict_and_the_gate() {
        let comparisons = [
            comparison(Metric::Ttft, 3, 0.12),
            comparison(Metric::ChunkWire, 3, 0.01),
        ];
        let gate = [Metric::Ttft];
        let pairs = ["seed 1", "seed 2", "seed 3"].map(pair_output).into();
        let text = render(&document(verdicts(&comparisons, &gate), pairs, &gate));
        assert!(
            text.contains("repetitions: 3, paired by seed: seed 1, seed 2, seed 3"),
            "{text}"
        );
        assert!(!text.contains("single repetition"), "{text}");
        assert!(
            text.contains(
                "intervals: delta = t over the 3 per-pair deltas (2 degree(s) of freedom); \
                 A = bootstrap percentile widened x2.69 for 3 repetitions"
            ),
            "{text}"
        );
        assert!(
            text.contains("12.00% of p99 (limit 5%): FAIL (gate metric)"),
            "{text}"
        );
        assert!(
            text.contains("1.00% of p99 (limit 5%): PASS (reported, not gated)"),
            "{text}"
        );
        assert!(text.contains("gate (ttft): FAIL on ttft"), "{text}");
        // Each quantile row lists the three pairs' deltas: +2.00 us apiece.
        let p50 = text
            .lines()
            .find(|l| l.trim_start().starts_with("p50"))
            .unwrap();
        assert!(p50.ends_with("+2.00 +2.00 +2.00"), "{p50}");
    }

    /// One repetition fails the baseline criterion however narrow its
    /// within-run interval, and says why.
    #[test]
    fn a_single_repetition_fails_the_gate() {
        let comparisons = [comparison(Metric::ChunkLatency, 1, 0.01)];
        let gate = [Metric::ChunkLatency];
        let reports = verdicts(&comparisons, &gate);
        assert!(!reports[0].baseline_evaluable);
        assert!(!reports[0].pass);
        let text = render(&document(reports, vec![pair_output("s1-P-r1")], &gate));
        assert!(
            text.contains("single repetition: within-run block bootstrap only"),
            "{text}"
        );
        assert!(
            text.contains(
                "1.00% of p99 (limit 5%): FAIL, not evaluable with one repetition (gate metric)"
            ),
            "{text}"
        );
        assert!(
            text.contains("gate (chunk_latency): FAIL on chunk_latency\n"),
            "{text}"
        );
    }
}
