//! Shutdown requests from the operating system (2.9): the first `SIGINT` or
//! `SIGTERM` starts the graceful shutdown, a second one abandons the drain.
//! Windows has only Ctrl-C.
//!
//! Same handling as floor-A's, written separately because `brisk` must not
//! depend on the benchmark crates.

use std::io;

/// A source of shutdown requests: the operating system's signals in the
/// binary, a channel in tests.
pub(crate) trait ShutdownRequests {
    /// Waits for the next request and returns its name, for the log.
    async fn next(&mut self) -> &'static str;
}

/// The signals that stop `brisk serve`.
///
/// Handlers are registered eagerly by [`ShutdownSignals::install`], so a
/// failure stops startup instead of leaving a server that cannot be stopped
/// cleanly. Once registered, the default action of the signal no longer
/// applies, which is why the handle keeps listening after the first one.
#[derive(Debug)]
pub(crate) struct ShutdownSignals {
    #[cfg(unix)]
    interrupt: tokio::signal::unix::Signal,
    #[cfg(unix)]
    terminate: tokio::signal::unix::Signal,
    #[cfg(windows)]
    ctrl_c: tokio::signal::windows::CtrlC,
}

impl ShutdownSignals {
    /// Registers `SIGINT` and `SIGTERM` (Unix) or Ctrl-C (Windows). Must be
    /// called inside a Tokio runtime with the I/O driver enabled.
    #[cfg(unix)]
    pub(crate) fn install() -> io::Result<Self> {
        use tokio::signal::unix::{SignalKind, signal};

        Ok(Self {
            interrupt: signal(SignalKind::interrupt())?,
            terminate: signal(SignalKind::terminate())?,
        })
    }

    /// Registers `SIGINT` and `SIGTERM` (Unix) or Ctrl-C (Windows). Must be
    /// called inside a Tokio runtime with the I/O driver enabled.
    #[cfg(windows)]
    pub(crate) fn install() -> io::Result<Self> {
        Ok(Self {
            ctrl_c: tokio::signal::windows::ctrl_c()?,
        })
    }
}

impl ShutdownRequests for ShutdownSignals {
    #[cfg(unix)]
    async fn next(&mut self) -> &'static str {
        // `recv` never returns `None` (tokio documents the `Option` as an
        // accident of its API).
        tokio::select! {
            _ = self.interrupt.recv() => "SIGINT",
            _ = self.terminate.recv() => "SIGTERM",
        }
    }

    #[cfg(windows)]
    async fn next(&mut self) -> &'static str {
        // `recv` never returns `None`, as above.
        self.ctrl_c.recv().await;
        "Ctrl-C"
    }
}

#[cfg(all(test, unix))]
mod tests {
    use std::process::Command;
    use std::time::Duration;

    use tokio::time::timeout;

    use super::*;

    /// Sends `signal` (a name such as `TERM`) to this test process.
    fn raise(signal: &str) {
        let status = Command::new("kill")
            .arg(format!("-{signal}"))
            .arg(std::process::id().to_string())
            .status()
            .expect("run kill");
        assert!(status.success(), "kill -{signal} failed: {status}");
    }

    #[tokio::test]
    async fn every_signal_is_named_and_the_handle_keeps_listening() {
        let mut signals = ShutdownSignals::install().expect("register the handlers");
        // One at a time: tokio coalesces signals that arrive back to back.
        for (signal, name) in [("INT", "SIGINT"), ("TERM", "SIGTERM"), ("TERM", "SIGTERM")] {
            raise(signal);
            let received = timeout(Duration::from_secs(10), signals.next())
                .await
                .unwrap_or_else(|_| panic!("{name} was not delivered"));
            assert_eq!(received, name);
        }
    }
}
