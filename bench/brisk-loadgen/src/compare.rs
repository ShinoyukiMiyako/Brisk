//! `compare`: bootstrap comparison of two arms of result files.
//!
//! Every input must be a valid run of the same scenario under the same load
//! (see [`load_params`]); ramp results mix rates and are refused. Failed
//! requests have no latency, so a quantile `q` is only meaningful while the
//! failures stay well inside its tail: each run's failure share must not
//! exceed `(1 − q) / 10` for the highest compared quantile. Runs breaking
//! either the validity or the tail rule are refused unless `--allow-invalid`
//! is given, and are listed in the output either way.

use std::path::{Path, PathBuf};

use anyhow::{Context as _, ensure};
use brisk_bench_core::result::RunResult;
use brisk_bench_core::stats::{self, CompareOptions, Comparison};
use serde::Serialize;
use serde_json::{Map, Value};

use crate::cli::CompareCmd;
use crate::run::RAMP_SCENARIO;
use crate::validity::{Outcomes, micros};

/// M0 exit criterion: half-width of arm A's p99 95% interval relative to
/// its p99.
pub(crate) const BASELINE_CI_LIMIT: f64 = 0.05;
/// Largest failure share relative to the tail `1 − q` of the highest
/// compared quantile.
const TAIL_FAILURE_FRACTION: f64 = 0.1;
/// Parameters that may differ between compared runs: the shared options
/// (target, label, output file, CPU placement, seed, timeout), the length of
/// the measurement, and the `bigbody` size list (each result file holds one
/// size, which its scenario names).
const NON_LOAD_PARAMS: [&str; 3] = ["common", "measure_s", "sizes"];

/// The comparison document.
#[derive(Debug, Serialize)]
struct CompareOutput<'a> {
    scenario: &'a str,
    a_files: &'a [PathBuf],
    b_files: &'a [PathBuf],
    a_labels: Vec<&'a str>,
    b_labels: Vec<&'a str>,
    invalid_runs: Vec<String>,
    /// Largest failure share the highest compared quantile tolerates.
    tail_failure_limit: f64,
    /// Runs whose failure share exceeds it.
    tail_failure_runs: Vec<String>,
    comparisons: &'a [Comparison],
    /// Every comparison's baseline p99 interval is within the limit.
    baseline_p99_ci_ok: bool,
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

    fn is_clean(&self) -> bool {
        self.invalid.is_empty() && self.tail_failures.is_empty()
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
    let inputs = || a.iter().zip(&cmd.a).chain(b.iter().zip(&cmd.b));
    check_comparable(scenario, (&a[0], &cmd.a[0]), inputs())?;
    let screening = Screening::new(&cmd.quantiles, inputs());
    screening.enforce(cmd.allow_invalid)?;
    let options = CompareOptions {
        resamples: cmd.resamples,
        block_len: cmd.block_len,
        seed: cmd.seed,
    };
    let comparisons = cmd
        .metric
        .iter()
        .map(|&metric| {
            stats::compare_with(&a, &b, metric, &cmd.quantiles, &options)
                .with_context(|| format!("comparing {metric}"))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let baseline_p99_ci_ok = comparisons
        .iter()
        .all(|c| c.baseline_p99_ci_half_width_ratio < BASELINE_CI_LIMIT);
    let labels =
        |runs: &'_ [RunResult]| -> Vec<String> { runs.iter().map(|r| r.label.clone()).collect() };
    let (a_labels, b_labels) = (labels(&a), labels(&b));
    print!(
        "{}",
        render(
            scenario,
            &a_labels,
            &b_labels,
            &comparisons,
            !screening.is_clean()
        )
    );
    let document = CompareOutput {
        scenario,
        a_files: &cmd.a,
        b_files: &cmd.b,
        a_labels: a_labels.iter().map(String::as_str).collect(),
        b_labels: b_labels.iter().map(String::as_str).collect(),
        tail_failure_limit: screening.tail_limit,
        invalid_runs: screening.invalid,
        tail_failure_runs: screening.tail_failures,
        comparisons: &comparisons,
        baseline_p99_ci_ok,
    };
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

fn distinct(labels: &[String]) -> String {
    let mut unique: Vec<&str> = labels.iter().map(String::as_str).collect();
    unique.sort_unstable();
    unique.dedup();
    unique.join(",")
}

fn render(
    scenario: &str,
    a_labels: &[String],
    b_labels: &[String],
    comparisons: &[Comparison],
    has_invalid: bool,
) -> String {
    let mut out = String::new();
    let mut line = |text: String| {
        out.push_str(&text);
        out.push('\n');
    };
    line(format!(
        "compare {scenario}: A = {} ({} run(s)), B = {} ({} run(s)){}",
        distinct(a_labels),
        a_labels.len(),
        distinct(b_labels),
        b_labels.len(),
        if has_invalid {
            "  [includes INVALID runs or runs failing too often for the tail]"
        } else {
            ""
        }
    ));
    for c in comparisons {
        line(format!(
            "metric {}: A {} intervals / {} samples, B {} intervals / {} samples, \
             {} resamples, block {}",
            c.metric, c.a_intervals, c.a_count, c.b_intervals, c.b_count, c.resamples, c.block_len
        ));
        line(format!(
            "  {:<9}{:>12}{:>12}{:>12}   {:<27}{:>14}",
            "quantile", "A (us)", "B (us)", "delta (us)", "delta 95% CI (us)", "A CI half-w."
        ));
        for q in &c.quantiles {
            let interval = format!(
                "[{:+.2}, {:+.2}]",
                q.delta_ci_low_ns / 1e3,
                q.delta_ci_high_ns / 1e3
            );
            line(format!(
                "  {:<9}{:>12.2}{:>12.2}{:>+12.2}   {:<27}{:>13.2}%",
                quantile_label(q.quantile),
                micros(q.a_ns),
                micros(q.b_ns),
                delta_us(q.delta_ns),
                interval,
                q.a_ci_half_width_ratio * 100.0
            ));
        }
        let ratio = c.baseline_p99_ci_half_width_ratio;
        line(format!(
            "  A p99 95% CI half-width: {:.2}% of p99 (limit {:.0}%): {}",
            ratio * 100.0,
            BASELINE_CI_LIMIT * 100.0,
            if ratio < BASELINE_CI_LIMIT {
                "PASS"
            } else {
                "FAIL"
            }
        ));
    }
    out
}

#[expect(clippy::cast_precision_loss, reason = "display only")]
fn delta_us(ns: i64) -> f64 {
    ns as f64 / 1e3
}

#[cfg(test)]
mod tests {
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
        let labels = ["direct-P".to_owned(), "direct-P".to_owned()];
        assert_eq!(distinct(&labels), "direct-P");
    }
}
