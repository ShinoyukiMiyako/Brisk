//! The Tokio runtime the data plane runs on.

use std::io;
use std::num::NonZeroUsize;

use tokio::runtime::{Builder, Runtime};

/// Name of every runtime thread, visible in `top -H` and profilers.
pub(crate) const WORKER_THREAD_NAME: &str = "brisk-worker";

/// Builds the multi-thread runtime with `workers` worker threads, or the
/// available parallelism when `None`, with I/O and time drivers enabled.
/// Returns the worker count actually used, for the startup log.
pub(crate) fn build(workers: Option<NonZeroUsize>) -> io::Result<(Runtime, NonZeroUsize)> {
    let workers = match workers {
        Some(workers) => workers,
        None => std::thread::available_parallelism()?,
    };
    let runtime = Builder::new_multi_thread()
        .worker_threads(workers.get())
        .thread_name(WORKER_THREAD_NAME)
        .enable_all()
        .build()?;
    Ok((runtime, workers))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn uses_the_configured_worker_count_and_thread_name() {
        let workers = NonZeroUsize::new(2).unwrap();
        let (runtime, used) = build(Some(workers)).unwrap();
        assert_eq!(used, workers);
        assert_eq!(runtime.metrics().num_workers(), 2);
        let task = runtime.spawn(async { std::thread::current().name().map(str::to_owned) });
        let name = runtime.block_on(task).unwrap();
        assert_eq!(name.as_deref(), Some(WORKER_THREAD_NAME));
        // The time driver is enabled.
        runtime.block_on(async { tokio::time::sleep(Duration::from_millis(1)).await });
    }

    #[test]
    fn defaults_to_the_available_parallelism() {
        let (runtime, used) = build(None).unwrap();
        assert_eq!(used, std::thread::available_parallelism().unwrap());
        assert_eq!(runtime.metrics().num_workers(), used.get());
    }
}
