//! Connection warm-up: credential-free requests to each upstream origin at
//! start and on an interval, so the first client requests find pooled
//! connections, and the readiness signal behind `/readyz`.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use reqwest::{Client, Method, Url};
use tokio::sync::mpsc;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{MissedTickBehavior, interval, timeout};

use crate::outcome::OutcomeSink;
use crate::secret::scrub_error;
use crate::spec::WarmupSpec;

/// A GET response body is read up to this many bytes and then dropped; the
/// point is to return the connection to the pool, not to look at the body.
const MAX_WARMUP_BODY: usize = 64 * 1024;

/// One warm-up request, repeated every interval.
#[derive(Debug, Clone)]
pub struct WarmupJob {
    /// The pool to warm; shared with the channels this job serves.
    pub client: Client,
    /// `HEAD` or `GET`.
    pub method: Method,
    /// Built from the base URL and `WarmupTarget::path` with `set_path`;
    /// its origin equals the channel's.
    pub url: Url,
    /// Names of the channels this job serves, for logs.
    pub channels: Box<str>,
}

/// Becomes ready once the first warm-up round finished or `ready_timeout`
/// elapsed, whichever comes first; never becomes unready again.
#[derive(Debug, Clone)]
pub struct Readiness(Arc<AtomicBool>);

impl Readiness {
    /// Whether `/readyz` should answer 200.
    pub fn is_ready(&self) -> bool {
        // Relaxed: the flag publishes no other data.
        self.0.load(Ordering::Relaxed)
    }

    fn set_ready(&self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

/// Spawns the warm-up loop on the current runtime.
///
/// Every job runs in its own task: one request at start, then one per
/// `spec.interval`, each bounded by `spec.request_timeout`. Any HTTP status
/// counts as success because only the pooled connection matters; transport
/// failures and timeouts are counted in `warmup_failures` and logged.
///
/// The returned handle runs until aborted; aborting it also stops every job.
///
/// # Panics
///
/// Outside a tokio runtime, and when `spec.interval` is zero.
pub fn spawn_warmup(
    jobs: Vec<WarmupJob>,
    spec: &WarmupSpec,
    sink: OutcomeSink,
) -> (Readiness, JoinHandle<()>) {
    let readiness = Readiness(Arc::new(AtomicBool::new(false)));
    let ready = readiness.clone();
    let spec = spec.clone();
    let handle = tokio::spawn(async move {
        // Nothing is ever sent: each job drops its sender after its first
        // round, and `recv` returns `None` once every sender is gone.
        let (first_round_tx, mut first_round_rx) = mpsc::channel::<()>(1);
        // Owning the job tasks in a `JoinSet` ties them to this task: aborting
        // the returned handle drops the set, which aborts every job.
        let mut tasks = JoinSet::new();
        for job in jobs {
            tasks.spawn(run_job(
                job,
                spec.interval,
                spec.request_timeout,
                sink.clone(),
                first_round_tx.clone(),
            ));
        }
        drop(first_round_tx);

        // An elapsed `ready_timeout` is the documented fallback, not an error:
        // a hanging upstream must not keep the gateway unready forever.
        let _ = timeout(spec.ready_timeout, first_round_rx.recv()).await;
        ready.set_ready();

        while let Some(joined) = tasks.join_next().await {
            if let Err(err) = joined
                && err.is_panic()
            {
                std::panic::resume_unwind(err.into_panic());
            }
        }
    });
    (readiness, handle)
}

async fn run_job(
    job: WarmupJob,
    every: Duration,
    request_timeout: Duration,
    sink: OutcomeSink,
    first_round: mpsc::Sender<()>,
) {
    let mut first_round = Some(first_round);
    let mut ticker = interval(every);
    // A slow round must not be followed by a burst of catch-up requests.
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        if let Err(failure) = warm_once(&job, request_timeout).await {
            // Relaxed: a report-only counter that orders no other memory.
            sink.counters()
                .warmup_failures
                .fetch_add(1, Ordering::Relaxed);
            match failure {
                WarmupFailure::Timeout => tracing::warn!(
                    channels = %job.channels,
                    timeout_ms = request_timeout.as_millis(),
                    "warm-up request timed out"
                ),
                WarmupFailure::Request(err) => tracing::warn!(
                    channels = %job.channels,
                    error = %scrub_error(Box::new(err)),
                    "warm-up request failed"
                ),
            }
        }
        // Dropping the sender reports the end of this job's first round.
        drop(first_round.take());
    }
}

enum WarmupFailure {
    Timeout,
    Request(reqwest::Error),
}

/// Sends the request without any credential header and, for GET, reads at
/// most [`MAX_WARMUP_BODY`] bytes of the body.
async fn warm_once(job: &WarmupJob, request_timeout: Duration) -> Result<(), WarmupFailure> {
    let exchange = async {
        let mut response = job
            .client
            .request(job.method.clone(), job.url.clone())
            .send()
            .await?;
        if job.method != Method::HEAD {
            let mut read = 0;
            while read < MAX_WARMUP_BODY {
                match response.chunk().await? {
                    Some(chunk) => read += chunk.len(),
                    None => break,
                }
            }
        }
        Ok(())
    };
    match timeout(request_timeout, exchange).await {
        Ok(result) => result.map_err(WarmupFailure::Request),
        Err(_elapsed) => Err(WarmupFailure::Timeout),
    }
}
