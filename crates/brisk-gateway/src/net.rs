//! Socket and process-level tuning for the data plane.
//!
//! Every socket option is applied through [`socket2`] (and the upstream client
//! builder), so this module contains no FFI. Platform-specific knobs live
//! behind `cfg` gates here and nowhere else in the crate.

use std::io;
use std::time::Duration;

use socket2::{SockRef, TcpKeepalive};
use tokio::net::TcpStream;

/// Interval between keepalive probes once the idle time has elapsed.
///
/// Applied on Linux, macOS and Windows. With [`KEEPALIVE_RETRIES`] probes this
/// detects a dead peer roughly `keepalive + 30s` after the last byte, which is
/// well inside the design's two-phase idle deadlines. Setting it explicitly
/// also matters on Windows, where `SIO_KEEPALIVE_VALS` would otherwise write
/// an interval of 0 ms and fire every probe back to back.
pub const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);

/// Number of unanswered keepalive probes before the kernel drops the
/// connection. Applied on Linux, macOS and Windows.
pub const KEEPALIVE_RETRIES: u32 = 3;

/// Applies the per-connection socket options required by the data plane.
///
/// Sets `TCP_NODELAY` (streamed SSE chunks must not wait for Nagle) and enables
/// TCP keepalive with `keepalive` as the idle time, [`KEEPALIVE_INTERVAL`]
/// between probes and [`KEEPALIVE_RETRIES`] probes.
pub fn tune_accepted(stream: &TcpStream, keepalive: Duration) -> io::Result<()> {
    let sock = SockRef::from(stream);
    sock.set_tcp_nodelay(true)?;
    sock.set_tcp_keepalive(&keepalive_params(keepalive))
}

fn keepalive_params(keepalive: Duration) -> TcpKeepalive {
    let params = TcpKeepalive::new().with_time(keepalive);
    #[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
    let params = params
        .with_interval(KEEPALIVE_INTERVAL)
        .with_retries(KEEPALIVE_RETRIES);
    params
}

/// Raises the `RLIMIT_NOFILE` soft limit to the hard limit and returns the new
/// soft limit (`None` when it is unlimited or the platform has no such limit).
///
/// Rust does not raise this limit at startup the way Go does, and the default
/// soft limit of an interactive session is often 1024, which caps
/// [`ServerConfig::default`](crate::server::ServerConfig::default) at its
/// floor of 256 connections. Binaries must call this *before* building the
/// default `ServerConfig`.
///
/// On macOS the soft limit cannot exceed `OPEN_MAX` (10240) when the hard
/// limit is unlimited, so it is capped there. On Windows this does nothing.
pub fn raise_nofile_soft_limit() -> io::Result<Option<u64>> {
    raise_nofile_impl()
}

#[cfg(unix)]
fn raise_nofile_impl() -> io::Result<Option<u64>> {
    use rustix::process::{Resource, Rlimit, getrlimit, setrlimit};

    let limit = getrlimit(Resource::Nofile);
    let target = nofile_target(limit.maximum);
    if limit.current == target {
        return Ok(target);
    }
    setrlimit(
        Resource::Nofile,
        Rlimit {
            current: target,
            maximum: limit.maximum,
        },
    )?;
    Ok(target)
}

#[cfg(not(unix))]
#[expect(
    clippy::unnecessary_wraps,
    reason = "signature is shared with the Unix implementation"
)]
fn raise_nofile_impl() -> io::Result<Option<u64>> {
    Ok(None)
}

/// `OPEN_MAX` from `<sys/syslimits.h>`: the largest soft `RLIMIT_NOFILE` macOS
/// accepts regardless of the hard limit.
#[cfg(target_os = "macos")]
const MACOS_OPEN_MAX: u64 = 10_240;

#[cfg(all(unix, not(target_os = "macos")))]
fn nofile_target(hard: Option<u64>) -> Option<u64> {
    hard
}

#[cfg(target_os = "macos")]
#[allow(
    clippy::unnecessary_wraps,
    reason = "signature is shared with the other Unix implementation"
)]
fn nofile_target(hard: Option<u64>) -> Option<u64> {
    Some(hard.map_or(MACOS_OPEN_MAX, |hard| hard.min(MACOS_OPEN_MAX)))
}

/// Applies `TCP_USER_TIMEOUT` to the upstream client where the platform has
/// it; elsewhere the builder is returned unchanged.
#[cfg(any(target_os = "android", target_os = "fuchsia", target_os = "linux"))]
pub(crate) fn apply_tcp_user_timeout(
    builder: reqwest::ClientBuilder,
    timeout: Duration,
) -> reqwest::ClientBuilder {
    builder.tcp_user_timeout(timeout)
}

/// Applies `TCP_USER_TIMEOUT` to the upstream client where the platform has
/// it; elsewhere the builder is returned unchanged.
#[cfg(not(any(target_os = "android", target_os = "fuchsia", target_os = "linux")))]
pub(crate) fn apply_tcp_user_timeout(
    builder: reqwest::ClientBuilder,
    _timeout: Duration,
) -> reqwest::ClientBuilder {
    builder
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    async fn connected_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (client, accepted) = tokio::join!(TcpStream::connect(addr), listener.accept());
        (client.unwrap(), accepted.unwrap().0)
    }

    #[tokio::test]
    async fn tune_accepted_sets_nodelay_and_keepalive() {
        let (_client, accepted) = connected_pair().await;
        let sock = SockRef::from(&accepted);
        sock.set_tcp_nodelay(false).unwrap();
        sock.set_keepalive(false).unwrap();
        assert!(!sock.tcp_nodelay().unwrap());
        assert!(!sock.keepalive().unwrap());

        tune_accepted(&accepted, Duration::from_secs(30)).unwrap();

        assert!(
            sock.tcp_nodelay().unwrap(),
            "TCP_NODELAY must be read back as set"
        );
        assert!(
            sock.keepalive().unwrap(),
            "SO_KEEPALIVE must be read back as set"
        );
    }

    /// Linux and macOS expose all three keepalive parameters for reading.
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[tokio::test]
    async fn tune_accepted_sets_keepalive_details() {
        let (_client, accepted) = connected_pair().await;
        tune_accepted(&accepted, Duration::from_secs(45)).unwrap();

        let sock = SockRef::from(&accepted);
        assert_eq!(sock.tcp_keepalive_time().unwrap(), Duration::from_secs(45));
        assert_eq!(sock.tcp_keepalive_interval().unwrap(), KEEPALIVE_INTERVAL);
        assert_eq!(sock.tcp_keepalive_retries().unwrap(), KEEPALIVE_RETRIES);
    }

    /// Idle time and interval are write-only on Windows (`SIO_KEEPALIVE_VALS`,
    /// no socket2 getter), so only the probe count is read back.
    #[cfg(windows)]
    #[tokio::test]
    async fn tune_accepted_sets_keepalive_details() {
        let (_client, accepted) = connected_pair().await;
        tune_accepted(&accepted, Duration::from_secs(45)).unwrap();

        let sock = SockRef::from(&accepted);
        assert!(sock.keepalive().unwrap());
        assert_eq!(sock.tcp_keepalive_retries().unwrap(), KEEPALIVE_RETRIES);
    }

    #[cfg(unix)]
    #[test]
    fn raise_nofile_soft_limit_reaches_target() {
        use rustix::process::{Resource, getrlimit};

        let raised = raise_nofile_soft_limit().unwrap();
        let limit = getrlimit(Resource::Nofile);
        assert_eq!(limit.current, raised);
        assert_eq!(raised, nofile_target(limit.maximum));
        // Idempotent: a second call finds the limit already raised.
        assert_eq!(raise_nofile_soft_limit().unwrap(), raised);
    }

    #[cfg(windows)]
    #[test]
    fn raise_nofile_soft_limit_is_noop_on_windows() {
        assert_eq!(raise_nofile_soft_limit().unwrap(), None);
    }
}
