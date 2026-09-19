//! Scenario orchestration: shard threads, live status, ramp coordination,
//! merging and the result document.

use std::io;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use anyhow::{Context as _, anyhow, bail, ensure};
use brisk_bench_core::clock::{now_ns, realtime_ns};
use brisk_bench_core::cpu;
use brisk_bench_core::fingerprint::Fingerprint;
use brisk_bench_core::result::{IntervalResult, RunResult, SCHEMA_VERSION};
use brisk_bench_core::rlimit;
use brisk_bench_core::stats;
use brisk_bench_core::transport::tls;
use hdrhistogram::Histogram;
use serde::Serialize;

use crate::cli::{
    BigbodyCmd, CommonArgs, CpuSet, NonstreamCmd, SelfcheckCmd, StreamCmd, StreamShape,
};
use crate::diagnostics::{self, Diagnostics};
use crate::output::{self, Extension};
use crate::report;
use crate::request::{BodySpec, RequestSpec, RequestTemplate};
use crate::schedule::{
    FixedArrivals, PlannedInterval, PlannedLoad, RateSegment, SEC_NS, StreamArrivals, StreamPlan,
    merge_planned, ramp_segments, share,
};
use crate::shard::{
    self, Connector, Counters, Live, Mode, RampSpec, Schedule, ShardSpec, StepReport,
};
use crate::target::Target;
use crate::validity::{self, Evidence};

/// Name recorded as the producing tool.
const TOOL: &str = "brisk-loadgen";
/// Time between choosing the origin and the first scheduled request, so
/// every shard thread is up and pinned before it matters.
const STARTUP_LEAD_NS: u64 = 200_000_000;
/// Descriptors kept free beyond the connections.
const FD_HEADROOM: u64 = 256;
/// Ramp steps with more failures than this share fail.
const RAMP_MAX_ERROR_SHARE: f64 = 0.01;
/// Scenario of a `nonstream` ramp. Its summary mixes every step's rate, so
/// it is kept apart from fixed-rate `nonstream` results.
pub(crate) const RAMP_SCENARIO: &str = "nonstream-ramp";

/// Per-run setup shared by all scenarios.
#[derive(Debug)]
struct Setup {
    target: Target,
    connector: Connector,
    shard_cpus: Vec<Option<Vec<usize>>>,
    nofile_limit: Option<u64>,
}

impl Setup {
    fn new(common: &CommonArgs, connections: u64) -> anyhow::Result<Self> {
        output::check_writable(&common.out)?;
        let target = Target::resolve(&common.url)?;
        let tls = match (target.url.tls, &common.tls_ca) {
            (true, Some(ca)) => {
                Some((tls::client_config(ca)?, tls::server_name(&target.url.host)?))
            }
            (true, None) => bail!("an https target needs --tls-ca"),
            (false, Some(_)) => bail!("--tls-ca was given for a plaintext http target"),
            (false, None) => None,
        };
        let shards = usize::from(common.shards);
        let shard_cpus = assign_cpus(common.cpu_list.as_ref(), shards);
        if let Some(cpus) = &common.cpu_list {
            cpu::pin_current_thread(&cpus.0).context("pinning the main thread to --cpu-list")?;
        }
        let nofile_limit = rlimit::raise_nofile_limit().context("raising RLIMIT_NOFILE")?;
        if let Some(limit) = nofile_limit {
            let needed = connections + FD_HEADROOM;
            ensure!(
                limit >= needed,
                "RLIMIT_NOFILE is {limit}, below the {needed} descriptors this run may need; raise the hard limit"
            );
        }
        let connector = Connector {
            addr: target.addr,
            tls,
        };
        Ok(Self {
            target,
            connector,
            shard_cpus,
            nofile_limit,
        })
    }

    fn template(&self, common: &CommonArgs, spec: Template) -> anyhow::Result<RequestTemplate> {
        Ok(RequestTemplate::build(&RequestSpec {
            authority: &self.target.url.authority,
            path: &self.target.url.path,
            headers: &common.headers,
            model: &common.model,
            stream: spec.stream,
            include_usage: spec.include_usage,
            ttft_us: spec.ttft_us,
            interval_us: spec.interval_us,
            chunk_bytes: spec.chunk_bytes,
            resp_bytes: spec.resp_bytes,
            body: spec.body,
        })?)
    }

    fn extension(
        &self,
        common: &CommonArgs,
        template: &RequestTemplate,
        outcome: &PhaseOutcome,
        warmup_intervals: u64,
    ) -> Extension {
        Extension {
            version: env!("CARGO_PKG_VERSION"),
            target: common.url.clone(),
            target_addr: self.target.addr.to_string(),
            tls: self.connector.tls.is_some(),
            shards: usize::from(common.shards),
            request_bytes: template.len(),
            body_bytes: template.body_len(),
            nofile_limit: self.nofile_limit,
            duration_s: seconds(outcome.end_ns.saturating_sub(outcome.origin_ns)),
            counters: outcome.counters,
            clock_steps: outcome.clock_steps,
            diagnostics: Diagnostics::summarize(&outcome.diagnostics, warmup_intervals),
            stream_plan: None,
            load_check: None,
            planned: None,
            ramp: None,
            selfcheck: None,
            body_size: None,
        }
    }
}

/// Request parameters that vary by scenario.
#[derive(Debug, Clone, Copy)]
struct Template {
    stream: bool,
    include_usage: bool,
    ttft_us: u64,
    interval_us: u64,
    chunk_bytes: u32,
    resp_bytes: u32,
    body: BodySpec,
}

/// Splits `--cpu-list` over the shards: one CPU each when the counts
/// match, otherwise the whole set for every shard.
fn assign_cpus(list: Option<&CpuSet>, shards: usize) -> Vec<Option<Vec<usize>>> {
    match list {
        None => vec![None; shards],
        Some(set) if set.0.len() == shards => set.0.iter().map(|&cpu| Some(vec![cpu])).collect(),
        Some(set) => vec![Some(set.0.clone()); shards],
    }
}

/// Seed of shard `index`; distinct per shard and stable across runs.
fn shard_seed(seed: u64, index: usize) -> u64 {
    seed ^ (index as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15)
}

#[expect(clippy::cast_precision_loss, reason = "display and JSON only")]
fn seconds(ns: u64) -> f64 {
    ns as f64 / 1e9
}

#[expect(clippy::cast_precision_loss, reason = "display only")]
fn count(n: u64) -> f64 {
    n as f64
}

/// Converts seconds from the command line to nanoseconds.
fn secs_ns(s: u64) -> u64 {
    s.saturating_mul(SEC_NS)
}

/// Chooses the common origin and the matching wall-clock start.
fn choose_origin() -> (u64, u64) {
    let mono = now_ns();
    let wall = realtime_ns();
    let origin = mono + STARTUP_LEAD_NS;
    (origin, (wall + STARTUP_LEAD_NS) / 1_000_000)
}

/// What the live status line shows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StatusStyle {
    /// Open streams against the offered count, chunk rate.
    Streams,
    /// Requests in flight, request rate.
    Requests,
}

/// The merged result of one phase.
#[derive(Debug)]
struct PhaseOutcome {
    origin_ns: u64,
    end_ns: u64,
    started_unix_ms: u64,
    intervals: Vec<IntervalResult>,
    planned: Option<Vec<PlannedInterval>>,
    counters: Counters,
    clock_steps: u64,
    diagnostics: diagnostics::Intervals,
    ramp: Option<RampOutcome>,
}

/// Runs the shards to completion while printing a status line every second
/// and, in ramp mode, judging steps as they are reported.
fn run_phase(
    specs: Vec<ShardSpec>,
    origin_ns: u64,
    started_unix_ms: u64,
    style: StatusStyle,
    mut ramp: Option<RampCoordinator>,
) -> anyhow::Result<PhaseOutcome> {
    let end_ns = specs.iter().map(|s| s.end_ns).max().unwrap_or(origin_ns);
    let lives: Vec<Arc<Live>> = specs.iter().map(|s| s.live.clone()).collect();
    let handles = specs
        .into_iter()
        .map(|spec| {
            thread::Builder::new()
                .name(format!("shard-{}", spec.index))
                .spawn(move || shard::run(spec))
        })
        .collect::<io::Result<Vec<JoinHandle<io::Result<shard::ShardOutput>>>>>()
        .context("spawning shard threads")?;

    let mut status = Status::new(origin_ns, style);
    let mut next_status = origin_ns + SEC_NS;
    let mut stopping = false;
    while !handles.iter().all(JoinHandle::is_finished) {
        let now = now_ns();
        // A shard that ends well before the others failed; stop the rest so
        // its error surfaces now instead of after the full run.
        if !stopping && now + 100_000_000 < end_ns && handles.iter().any(JoinHandle::is_finished) {
            stop_all(&lives);
            stopping = true;
        }
        let wait = Duration::from_nanos(
            next_status
                .saturating_sub(now)
                .clamp(1_000_000, 100_000_000),
        );
        match &mut ramp {
            Some(coordinator) => match coordinator.reports.recv_timeout(wait) {
                Ok(report) => coordinator.on_report(report, &lives),
                Err(RecvTimeoutError::Timeout) => {}
                // Every shard has dropped its sender and is about to exit.
                Err(RecvTimeoutError::Disconnected) => thread::sleep(Duration::from_millis(1)),
            },
            None => thread::sleep(wait),
        }
        let now = now_ns();
        if now >= next_status {
            status.print(now, &lives);
            next_status += SEC_NS * ((now - next_status) / SEC_NS + 1);
        }
    }

    let mut intervals = Vec::with_capacity(handles.len());
    let mut planned = Vec::new();
    let mut counters = Counters::default();
    let mut clock_steps = 0;
    let mut diagnostics = diagnostics::Intervals::new();
    let mut actual_end = origin_ns;
    for (index, handle) in handles.into_iter().enumerate() {
        let output = handle
            .join()
            .map_err(|_| anyhow!("shard {index} panicked"))?
            .with_context(|| format!("shard {index} failed"))?;
        intervals.push(output.intervals);
        planned.extend(output.planned);
        counters.add(&output.counters);
        clock_steps += output.clock_steps;
        diagnostics::merge_into(&mut diagnostics, output.diagnostics);
        actual_end = actual_end.max(output.end_ns);
    }
    let ramp = ramp.map(|mut coordinator| {
        while let Ok(report) = coordinator.reports.try_recv() {
            coordinator.on_report(report, &lives);
        }
        coordinator.finish()
    });
    Ok(PhaseOutcome {
        origin_ns,
        end_ns: actual_end,
        started_unix_ms,
        intervals: stats::merge_shards(intervals).context("merging shard intervals")?,
        planned: (!planned.is_empty()).then(|| merge_planned(planned)),
        counters,
        clock_steps,
        diagnostics,
        ramp,
    })
}

fn stop_all(lives: &[Arc<Live>]) {
    for live in lives {
        live.stop.store(true, Ordering::Relaxed);
    }
}

/// Live status line on stderr.
#[derive(Debug)]
struct Status {
    origin_ns: u64,
    style: StatusStyle,
    last_ns: u64,
    last_chunks: u64,
    last_requests: u64,
}

impl Status {
    fn new(origin_ns: u64, style: StatusStyle) -> Self {
        Self {
            origin_ns,
            style,
            last_ns: origin_ns,
            last_chunks: 0,
            last_requests: 0,
        }
    }

    fn print(&mut self, now: u64, lives: &[Arc<Live>]) {
        let sum = |f: fn(&Live) -> u64| lives.iter().map(|l| f(l)).sum::<u64>();
        let open = sum(|l| l.open.load(Ordering::Relaxed));
        let planned = sum(|l| l.planned_open.load(Ordering::Relaxed));
        let chunks = sum(|l| l.chunks.load(Ordering::Relaxed));
        let requests = sum(|l| l.requests.load(Ordering::Relaxed));
        let errors = sum(|l| l.errors.load(Ordering::Relaxed));
        let dt = seconds(now.saturating_sub(self.last_ns)).max(1e-9);
        let chunk_rate = count(chunks.saturating_sub(self.last_chunks)) / dt;
        let request_rate = count(requests.saturating_sub(self.last_requests)) / dt;
        let elapsed = seconds(now.saturating_sub(self.origin_ns));
        match self.style {
            StatusStyle::Streams => eprintln!(
                "[{elapsed:>6.0}s] open {open:>6} (offered {planned:>6})  chunks/s {chunk_rate:>9.0}  \
                 done/s {request_rate:>7.1}  errors {errors}"
            ),
            StatusStyle::Requests => eprintln!(
                "[{elapsed:>6.0}s] in flight {open:>6}  req/s {request_rate:>9.1}  errors {errors}"
            ),
        }
        self.last_ns = now;
        self.last_chunks = chunks;
        self.last_requests = requests;
    }
}

/// Result of one ramp step, all shards merged.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct RampStep {
    /// Step number.
    pub(crate) step: u32,
    /// Offered rate per second.
    pub(crate) rate: f64,
    /// Successful requests.
    pub(crate) requests: u64,
    /// Failed requests.
    pub(crate) errors: u64,
    /// Requests still open when judged (recorded as lower bounds).
    pub(crate) censored: u64,
    /// Median latency, nanoseconds.
    pub(crate) p50_ns: u64,
    /// 99th percentile latency, nanoseconds.
    pub(crate) p99_ns: u64,
    /// Within the p99 limit and the error limit.
    pub(crate) passed: bool,
}

/// Outcome of an S2 ramp.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct RampOutcome {
    /// The p99 limit, nanoseconds.
    pub(crate) stop_p99_ns: u64,
    /// Judged steps in order, up to and including the failing one.
    pub(crate) steps: Vec<RampStep>,
    /// Why the ramp ended.
    pub(crate) stopped_by: String,
    /// Rate of the last step before the first failing one.
    pub(crate) max_sustainable_rate: Option<f64>,
}

/// Collects step reports from all shards and stops the run at the first
/// step that fails.
#[derive(Debug)]
struct RampCoordinator {
    reports: Receiver<StepReport>,
    shards: usize,
    stop_p99_ns: u64,
    segments: Vec<RateSegment>,
    pending: Vec<(u32, usize, StepReport)>,
    steps: Vec<RampStep>,
    stopped_by: Option<String>,
}

impl RampCoordinator {
    fn on_report(&mut self, report: StepReport, lives: &[Arc<Live>]) {
        let step = report.step;
        match self.pending.iter_mut().find(|(s, ..)| *s == step) {
            Some((_, count, acc)) => {
                acc.latency
                    .add(&report.latency)
                    .expect("step histograms share the standard bounds");
                acc.requests += report.requests;
                acc.errors += report.errors;
                acc.censored += report.censored;
                *count += 1;
            }
            None => self.pending.push((step, 1, report)),
        }
        while let Some(pos) = self
            .pending
            .iter()
            .position(|(_, count, _)| *count == self.shards)
        {
            let (step, _, acc) = self.pending.remove(pos);
            self.judge(step, &acc, lives);
        }
    }

    fn judge(&mut self, step: u32, acc: &StepReport, lives: &[Arc<Live>]) {
        if self.stopped_by.is_some() {
            return;
        }
        let rate = self
            .segments
            .iter()
            .find(|s| s.step == step)
            .map_or(0.0, |s| s.rate);
        let latency: &Histogram<u64> = &acc.latency;
        let total = acc.requests + acc.errors;
        let p99_ns = latency.value_at_quantile(0.99);
        #[expect(clippy::cast_precision_loss, reason = "request counts are small")]
        let error_share = if total == 0 {
            1.0
        } else {
            acc.errors as f64 / total as f64
        };
        let passed = total > 0 && p99_ns <= self.stop_p99_ns && error_share <= RAMP_MAX_ERROR_SHARE;
        eprintln!(
            "ramp step {step}: {rate:.1} req/s  p99 {:.3} ms  errors {}/{}  -> {}",
            seconds(p99_ns) * 1e3,
            acc.errors,
            total,
            if passed { "ok" } else { "stop" }
        );
        self.steps.push(RampStep {
            step,
            rate,
            requests: acc.requests,
            errors: acc.errors,
            censored: acc.censored,
            p50_ns: latency.value_at_quantile(0.5),
            p99_ns,
            passed,
        });
        if !passed {
            self.stopped_by = Some(if total == 0 {
                format!("step {step} completed no request")
            } else if error_share > RAMP_MAX_ERROR_SHARE {
                format!(
                    "step {step} failed {:.2}% of its requests",
                    error_share * 100.0
                )
            } else {
                format!("step {step} p99 exceeded the limit")
            });
            stop_all(lives);
        }
    }

    fn finish(self) -> RampOutcome {
        let max_sustainable_rate = self
            .steps
            .iter()
            .take_while(|s| s.passed)
            .last()
            .map(|s| s.rate);
        RampOutcome {
            stop_p99_ns: self.stop_p99_ns,
            stopped_by: self
                .stopped_by
                .unwrap_or_else(|| "all steps passed (--ramp-max-steps reached)".to_owned()),
            steps: self.steps,
            max_sustainable_rate,
        }
    }
}

/// Assembles the [`RunResult`] of a phase.
fn build_result(
    scenario: &str,
    common: &CommonArgs,
    params: serde_json::Value,
    warmup_intervals: u64,
    outcome: &PhaseOutcome,
) -> anyhow::Result<RunResult> {
    let summary =
        stats::summarize(&outcome.intervals, warmup_intervals).context("summarizing intervals")?;
    Ok(RunResult {
        schema_version: SCHEMA_VERSION,
        tool: TOOL.to_owned(),
        scenario: scenario.to_owned(),
        label: common.label.clone(),
        pair_id: common.pair_id.clone(),
        params,
        fingerprint: Fingerprint::collect(),
        started_unix_ms: outcome.started_unix_ms,
        warmup_intervals,
        intervals: outcome.intervals.clone(),
        summary,
        validity: brisk_bench_core::result::Validity::default(),
    })
}

/// Runs S1-style streams at `concurrency`; shared by `stream` and
/// `selfcheck`.
fn run_streams(
    common: &CommonArgs,
    shape: &StreamShape,
    concurrency: u32,
    warmup_s: u64,
    measure_s: u64,
    scenario: &str,
    params: serde_json::Value,
) -> anyhow::Result<(RunResult, Extension)> {
    let shards = usize::from(common.shards);
    ensure!(
        u32::try_from(shards).is_ok_and(|s| s <= concurrency),
        "--shards {shards} exceeds the {concurrency} concurrent streams"
    );
    // Open streams fluctuate around the target; allow for twice as many.
    let setup = Setup::new(common, 2 * u64::from(concurrency))?;
    let plan = StreamPlan::new(shape, concurrency)?;
    let template = setup.template(
        common,
        Template {
            stream: true,
            include_usage: shape.include_usage,
            ttft_us: shape.ttft_us,
            interval_us: plan.interval_us,
            chunk_bytes: shape.chunk_bytes,
            resp_bytes: 0,
            body: BodySpec::Prompt(shape.prompt_bytes),
        },
    )?;
    let total_s = warmup_s + measure_s;
    let (origin_ns, started_unix_ms) = choose_origin();
    let end_ns = origin_ns + secs_ns(total_s);
    let intervals = usize::try_from(total_s).context("run too long")?;
    let specs = (0..shards)
        .map(|index| {
            Ok(ShardSpec {
                index,
                origin_ns,
                end_ns,
                cpus: setup.shard_cpus[index].clone(),
                spin_window: Duration::from_micros(common.spin_us),
                connector: setup.connector.clone(),
                template: template.clone(),
                schedule: Schedule::Stream(StreamArrivals::new(
                    plan,
                    share(concurrency, shards, index),
                    shard_seed(common.seed, index),
                    origin_ns,
                    end_ns,
                )?),
                mode: Mode::Stream {
                    ttft_ns: plan.ttft_ns,
                    interval_ns: plan.interval_us * 1_000,
                },
                planned: Some((
                    PlannedLoad::new(origin_ns, SEC_NS, intervals),
                    plan.ttft_ns,
                    plan.interval_us * 1_000,
                )),
                request_timeout_ns: timeout_ns(common),
                live: Arc::new(Live::default()),
                ramp: None,
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    eprintln!(
        "{scenario}: {concurrency} streams against {} ({} shard(s)), arrivals {:.1}/s, \
         mean lifetime {:.2} s, offered {:.0} chunks/s; warmup {warmup_s} s, measure {measure_s} s",
        common.url, shards, plan.arrival_rate, plan.mean_lifetime_s, plan.offered_chunk_rate
    );
    let outcome = run_phase(
        specs,
        origin_ns,
        started_unix_ms,
        StatusStyle::Streams,
        None,
    )?;
    let mut run = build_result(scenario, common, params, warmup_s, &outcome)?;
    let planned = outcome.planned.clone().unwrap_or_default();
    let load = validity::check_load(&run.intervals, &planned, warmup_s, concurrency);
    let mut extension = setup.extension(common, &template, &outcome, warmup_s);
    run.validity = validity::evaluate(&Evidence {
        intervals: &run.intervals,
        warmup_intervals: warmup_s,
        summary: &run.summary,
        clock_steps: outcome.clock_steps,
        load: Some(&load),
        counters: &outcome.counters,
        diagnostics: &extension.diagnostics,
        markers_expected: true,
        mock_lag_limit_ns: common.mock_write_lag_limit_ns(),
    });
    extension.stream_plan = Some(plan);
    extension.load_check = Some(load);
    extension.planned = Some(planned);
    Ok((run, extension))
}

fn timeout_ns(common: &CommonArgs) -> u64 {
    brisk_bench_core::dist::seconds_to_ns(common.request_timeout_s)
}

fn params_of<T: Serialize>(cmd: &T) -> anyhow::Result<serde_json::Value> {
    serde_json::to_value(cmd).context("recording the command-line parameters")
}

fn finish_run(common: &CommonArgs, run: &RunResult, extension: &Extension) -> anyhow::Result<()> {
    output::write_result(&common.out, run, extension)?;
    report::print_run(run, extension);
    println!("result written to {}", common.out.display());
    Ok(())
}

/// `stream` (S1).
pub(crate) fn stream(cmd: &StreamCmd) -> anyhow::Result<()> {
    let (mut run, extension) = run_streams(
        &cmd.common,
        &cmd.shape,
        cmd.shape.concurrency,
        cmd.warmup_s,
        cmd.measure_s,
        "stream",
        params_of(cmd)?,
    )?;
    if let Some(limit_us) = cmd.max_slip_us {
        validity::check_slip(
            &mut run.validity,
            &extension.diagnostics,
            limit_us.saturating_mul(1_000),
        );
    }
    finish_run(&cmd.common, &run, &extension)
}

/// `selfcheck`: S1 streams at twice the target concurrency straight to the
/// mock. Fails (non-zero exit) unless every criterion holds.
pub(crate) fn selfcheck(cmd: &SelfcheckCmd) -> anyhow::Result<()> {
    let doubled = cmd
        .shape
        .concurrency
        .checked_mul(2)
        .context("--concurrency too large to double")?;
    let (run, mut extension) = run_streams(
        &cmd.common,
        &cmd.shape,
        doubled,
        cmd.warmup_s,
        cmd.measure_s,
        "selfcheck",
        params_of(cmd)?,
    )?;
    let load = extension
        .load_check
        .as_ref()
        .expect("stream runs carry a load check");
    let verdict = validity::selfcheck(&validity::SelfcheckEvidence {
        summary: &run.summary,
        load,
        diagnostics: &extension.diagnostics,
        slip_limit_ns: cmd.max_slip_us.saturating_mul(1_000),
        mock_lag_limit_ns: cmd.common.mock_write_lag_limit_ns(),
        validity: &run.validity,
    });
    extension.selfcheck = Some(verdict.clone());
    finish_run(&cmd.common, &run, &extension)?;
    report::print_selfcheck(&verdict, cmd.shape.concurrency, doubled);
    ensure!(verdict.pass, "selfcheck failed");
    Ok(())
}

/// An evenly spaced phase of `nonstream` or `bigbody`.
#[derive(Debug)]
struct FixedPhase<'a> {
    setup: &'a Setup,
    common: &'a CommonArgs,
    template: &'a RequestTemplate,
    segments: &'a [RateSegment],
    origin_ns: u64,
    end_ns: u64,
    mode: Mode,
    chunks: u32,
    /// Ramp reporting: where to send step reports and the p99 limit.
    ramp: Option<(&'a mpsc::Sender<StepReport>, u64)>,
}

impl FixedPhase<'_> {
    fn specs(&self) -> Vec<ShardSpec> {
        let shards = usize::from(self.common.shards);
        let steps: Vec<RateSegment> = self
            .segments
            .iter()
            .copied()
            .filter(|s| s.step > 0)
            .collect();
        (0..shards)
            .map(|index| ShardSpec {
                index,
                origin_ns: self.origin_ns,
                end_ns: self.end_ns,
                cpus: self.setup.shard_cpus[index].clone(),
                spin_window: Duration::from_micros(self.common.spin_us),
                connector: self.setup.connector.clone(),
                template: self.template.clone(),
                schedule: Schedule::Fixed(FixedArrivals::new(
                    self.segments.to_vec(),
                    index,
                    shards,
                    self.chunks,
                )),
                mode: self.mode,
                planned: None,
                request_timeout_ns: timeout_ns(self.common),
                live: Arc::new(Live::default()),
                ramp: self.ramp.map(|(tx, threshold_ns)| RampSpec {
                    steps: steps.clone(),
                    threshold_ns,
                    reports: tx.clone(),
                }),
            })
            .collect()
    }
}

/// The `nonstream` schedule: its segments, its end and a description.
fn nonstream_schedule(
    cmd: &NonstreamCmd,
    origin_ns: u64,
    stop_p99_ns: u64,
) -> (Vec<RateSegment>, u64, String) {
    let warmup_ns = secs_ns(cmd.warmup_s);
    match (cmd.rate, cmd.ramp_start) {
        (Some(rate), None) => {
            let end_ns = origin_ns + warmup_ns + secs_ns(cmd.measure_s);
            let segments = vec![RateSegment {
                start_ns: origin_ns,
                end_ns,
                rate,
                step: 0,
            }];
            (
                segments,
                end_ns,
                format!("{rate} req/s for {} s", cmd.measure_s),
            )
        }
        (None, Some(start)) => {
            let segments = ramp_segments(
                origin_ns,
                warmup_ns,
                start,
                cmd.ramp_step_pct,
                secs_ns(cmd.ramp_step_s),
                cmd.ramp_max_steps,
            );
            let last = segments.last().expect("at least one ramp step").end_ns;
            // Leave room to judge the last step before the shards stop.
            let end_ns = last + stop_p99_ns + 20_000_000;
            let description = format!(
                "ramp from {start} req/s, +{}% every {} s, stop at p99 > {} ms",
                cmd.ramp_step_pct, cmd.ramp_step_s, cmd.stop_p99_ms
            );
            (segments, end_ns, description)
        }
        _ => unreachable!("clap enforces exactly one of --rate and --ramp-start"),
    }
}

/// `nonstream` (S2): fixed rate or ramp.
pub(crate) fn nonstream(cmd: &NonstreamCmd) -> anyhow::Result<()> {
    let common = &cmd.common;
    let setup = Setup::new(common, max_in_flight(cmd.rate.or(cmd.ramp_start), common))?;
    let template = setup.template(
        common,
        Template {
            stream: false,
            include_usage: false,
            ttft_us: cmd.ttft_us,
            interval_us: 0,
            chunk_bytes: brisk_bench_core::wire::MARKER_LEN_U32,
            resp_bytes: cmd.resp_bytes,
            body: BodySpec::Prompt(cmd.prompt_bytes),
        },
    )?;
    let (origin_ns, started_unix_ms) = choose_origin();
    let stop_p99_ns = brisk_bench_core::dist::seconds_to_ns(cmd.stop_p99_ms / 1e3);
    let (segments, end_ns, description) = nonstream_schedule(cmd, origin_ns, stop_p99_ns);
    let ramp = cmd.ramp_start.map(|_| mpsc::channel());
    // The mock embeds a marker in every completion whose content has room.
    let markers_expected = cmd.resp_bytes >= brisk_bench_core::wire::MARKER_LEN_U32;
    let specs = FixedPhase {
        setup: &setup,
        common,
        template: &template,
        segments: &segments,
        origin_ns,
        end_ns,
        mode: Mode::Whole {
            ttft_ns: cmd.ttft_us.saturating_mul(1_000),
            marker: markers_expected,
        },
        chunks: 1,
        ramp: ramp.as_ref().map(|(tx, _)| (tx, stop_p99_ns)),
    }
    .specs();
    let coordinator = ramp.map(|(_, rx)| RampCoordinator {
        reports: rx,
        shards: specs.len(),
        stop_p99_ns,
        segments: segments.iter().copied().filter(|s| s.step > 0).collect(),
        pending: Vec::new(),
        steps: Vec::new(),
        stopped_by: None,
    });
    eprintln!(
        "nonstream: {description} against {} ({} shard(s)); warmup {} s",
        common.url,
        specs.len(),
        cmd.warmup_s
    );
    let outcome = run_phase(
        specs,
        origin_ns,
        started_unix_ms,
        StatusStyle::Requests,
        coordinator,
    )?;
    let scenario = if cmd.ramp_start.is_some() {
        RAMP_SCENARIO
    } else {
        "nonstream"
    };
    let mut run = build_result(scenario, common, params_of(cmd)?, cmd.warmup_s, &outcome)?;
    let mut extension = setup.extension(common, &template, &outcome, cmd.warmup_s);
    run.validity = validity::evaluate(&Evidence {
        intervals: &run.intervals,
        warmup_intervals: cmd.warmup_s,
        summary: &run.summary,
        clock_steps: outcome.clock_steps,
        load: None,
        counters: &outcome.counters,
        diagnostics: &extension.diagnostics,
        markers_expected,
        mock_lag_limit_ns: common.mock_write_lag_limit_ns(),
    });
    extension.ramp.clone_from(&outcome.ramp);
    finish_run(common, &run, &extension)
}

/// Upper bound on concurrent connections of a fixed-rate run: every
/// request of one timeout window in flight at once, capped to keep the
/// descriptor check meaningful.
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "positive, clamped to a small range"
)]
fn max_in_flight(rate: Option<f64>, common: &CommonArgs) -> u64 {
    (rate.unwrap_or(1.0) * common.request_timeout_s.min(10.0)).clamp(1.0, 65_536.0) as u64
}

/// `bigbody` (S3): one run and one result file per size.
pub(crate) fn bigbody(cmd: &BigbodyCmd) -> anyhow::Result<()> {
    let common = &cmd.common;
    let params = params_of(cmd)?;
    for size in &cmd.sizes {
        let out = output::with_suffix(&common.out, &size.label);
        let sized = CommonArgs {
            out: out.clone(),
            ..common.clone()
        };
        let setup = Setup::new(&sized, max_in_flight(Some(cmd.rate), common))?;
        let template = setup.template(
            common,
            Template {
                stream: true,
                include_usage: false,
                ttft_us: cmd.ttft_us,
                interval_us: cmd.interval_us,
                chunk_bytes: cmd.chunk_bytes,
                resp_bytes: 0,
                body: BodySpec::Exact(size.bytes),
            },
        )?;
        let (origin_ns, started_unix_ms) = choose_origin();
        let end_ns = origin_ns + secs_ns(cmd.warmup_s + cmd.measure_s);
        let segments = vec![RateSegment {
            start_ns: origin_ns,
            end_ns,
            rate: cmd.rate,
            step: 0,
        }];
        let specs = FixedPhase {
            setup: &setup,
            common,
            template: &template,
            segments: &segments,
            origin_ns,
            end_ns,
            mode: Mode::Stream {
                ttft_ns: cmd.ttft_us.saturating_mul(1_000),
                interval_ns: cmd.interval_us.saturating_mul(1_000),
            },
            chunks: cmd.chunks,
            ramp: None,
        }
        .specs();
        eprintln!(
            "bigbody {}: {} byte bodies at {} req/s against {}; warmup {} s, measure {} s",
            size.label,
            template.body_len(),
            cmd.rate,
            common.url,
            cmd.warmup_s,
            cmd.measure_s
        );
        let outcome = run_phase(
            specs,
            origin_ns,
            started_unix_ms,
            StatusStyle::Requests,
            None,
        )?;
        let scenario = format!("bigbody-{}", size.label);
        let mut run = build_result(&scenario, &sized, params.clone(), cmd.warmup_s, &outcome)?;
        let mut extension = setup.extension(&sized, &template, &outcome, cmd.warmup_s);
        run.validity = validity::evaluate(&Evidence {
            intervals: &run.intervals,
            warmup_intervals: cmd.warmup_s,
            summary: &run.summary,
            clock_steps: outcome.clock_steps,
            load: None,
            counters: &outcome.counters,
            diagnostics: &extension.diagnostics,
            markers_expected: true,
            mock_lag_limit_ns: sized.mock_write_lag_limit_ns(),
        });
        extension.body_size = Some(size.clone());
        finish_run(&sized, &run, &extension)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cpus_are_split_or_shared() {
        assert_eq!(assign_cpus(None, 2), vec![None, None]);
        let set = CpuSet(vec![2, 3]);
        assert_eq!(
            assign_cpus(Some(&set), 2),
            vec![Some(vec![2]), Some(vec![3])]
        );
        assert_eq!(assign_cpus(Some(&set), 3), vec![Some(vec![2, 3]); 3]);
        let one = CpuSet(vec![3]);
        assert_eq!(assign_cpus(Some(&one), 1), vec![Some(vec![3])]);
    }

    #[test]
    fn shard_seeds_differ() {
        assert_ne!(shard_seed(1, 0), shard_seed(1, 1));
        assert_eq!(shard_seed(1, 0), shard_seed(1, 0));
    }

    fn report(step: u32, values: &[u64], errors: u64) -> StepReport {
        let mut latency = stats::new_histogram();
        for &v in values {
            latency.record(v).unwrap();
        }
        StepReport {
            step,
            latency,
            requests: values.len() as u64,
            errors,
            censored: 0,
        }
    }

    #[test]
    fn ramp_stops_at_the_first_slow_step() {
        let (_tx, rx) = mpsc::channel();
        let segments = ramp_segments(0, 0, 100.0, 10.0, SEC_NS, 3);
        let lives = vec![Arc::new(Live::default()), Arc::new(Live::default())];
        let mut coordinator = RampCoordinator {
            reports: rx,
            shards: 2,
            stop_p99_ns: 2_000_000,
            segments,
            pending: Vec::new(),
            steps: Vec::new(),
            stopped_by: None,
        };
        let fast = vec![500_000; 100];
        let slow = vec![3_000_000; 100];
        coordinator.on_report(report(1, &fast, 0), &lives);
        assert!(coordinator.steps.is_empty(), "waits for every shard");
        coordinator.on_report(report(1, &fast, 0), &lives);
        assert_eq!(coordinator.steps.len(), 1);
        assert!(!lives[0].stop.load(Ordering::Relaxed));
        coordinator.on_report(report(2, &fast, 0), &lives);
        coordinator.on_report(report(2, &slow, 0), &lives);
        assert!(lives.iter().all(|l| l.stop.load(Ordering::Relaxed)));
        coordinator.on_report(report(3, &fast, 0), &lives);
        coordinator.on_report(report(3, &fast, 0), &lives);
        let outcome = coordinator.finish();
        assert_eq!(outcome.steps.len(), 2);
        assert!(outcome.steps[0].passed && !outcome.steps[1].passed);
        assert_eq!(outcome.max_sustainable_rate, Some(100.0));
        assert!(
            outcome.stopped_by.contains("step 2"),
            "{}",
            outcome.stopped_by
        );
    }

    #[test]
    fn ramp_fails_steps_with_errors() {
        let (_tx, rx) = mpsc::channel();
        let lives = vec![Arc::new(Live::default())];
        let mut coordinator = RampCoordinator {
            reports: rx,
            shards: 1,
            stop_p99_ns: 2_000_000,
            segments: ramp_segments(0, 0, 50.0, 10.0, SEC_NS, 2),
            pending: Vec::new(),
            steps: Vec::new(),
            stopped_by: None,
        };
        coordinator.on_report(report(1, &[1_000; 98], 2), &lives);
        let outcome = coordinator.finish();
        assert!(!outcome.steps[0].passed);
        assert_eq!(outcome.max_sustainable_rate, None);
        assert!(
            outcome.stopped_by.contains("failed"),
            "{}",
            outcome.stopped_by
        );
    }
}
