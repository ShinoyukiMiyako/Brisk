//! Emission policies: how a shard divides the time before a scheduled
//! emission between serving sockets and spinning to it.
//!
//! A shard is one thread and an event handler cannot be interrupted, so a
//! handler still running when an emission comes due makes that emission
//! late. The policies differ in how close to an emission they keep serving
//! sockets:
//!
//! - [`EmitPolicy::FullSpin`] stops as soon as an emission is within the spin
//!   window and spins to it. Nothing is overshot by more than one loop pass,
//!   but at tens of thousands of chunks per second one spin chains into the
//!   next and inbound requests wait for the whole chain.
//! - [`EmitPolicy::Fixed`] serves sockets until the emission is within the
//!   commit window, however long the event takes: a TLS handshake or a large
//!   receive that outlasts the commit window overshoots the emission. The
//!   commit window has to cover at least one pass of the loop, a zero-timeout
//!   poll plus the bookkeeping around it, or the emission would be overshot
//!   while polling; any longer trades request read delay for write precision.

use std::fmt;
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Default commit window of the fixed policy.
///
/// On an 8-vCPU host 5 us let a TLS handshake overshoot emissions by up to
/// 19.7 us at p99, while 10 us kept the worst write-lag p99 at 14.4 us over
/// 12 repetitions; the extra 5 us of request read delay is the cheaper side.
pub const DEFAULT_COMMIT_WINDOW: Duration = Duration::from_micros(10);

/// How a shard divides the time before each emission between socket events
/// and the final spin.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum EmitPolicy {
    /// Commit to spinning for the whole spin window: the best write
    /// precision, while inbound requests wait during chains of emissions.
    FullSpin,
    /// Commit to spinning for the commit window only, whatever the events
    /// served before it cost.
    #[default]
    Fixed,
}

impl EmitPolicy {
    /// The policy's name on the command line and in the statistics.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::FullSpin => "full-spin",
            Self::Fixed => "fixed",
        }
    }
}

impl fmt::Display for EmitPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Emission settings shared by every shard of a server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmitSettings {
    /// How the time before an emission is divided.
    pub policy: EmitPolicy,
    /// Busy-wait window before each emission.
    pub spin_window: Duration,
    /// Commit window of the fixed policy, capped at the spin window.
    pub commit_window: Duration,
}

impl EmitSettings {
    /// The spin window in nanoseconds.
    pub fn spin_window_ns(&self) -> u64 {
        duration_ns(self.spin_window)
    }

    /// The commit window in effect, in nanoseconds: how close the next
    /// emission may come before a shard stops serving sockets and spins to
    /// it. That is the whole spin window for [`EmitPolicy::FullSpin`] and
    /// the configured commit window capped at the spin window for
    /// [`EmitPolicy::Fixed`].
    pub fn commit_window_ns(&self) -> u64 {
        let spin_ns = self.spin_window_ns();
        match self.policy {
            EmitPolicy::FullSpin => spin_ns,
            EmitPolicy::Fixed => duration_ns(self.commit_window).min(spin_ns),
        }
    }
}

/// Converts a duration to whole nanoseconds, saturating at `u64::MAX`.
fn duration_ns(d: Duration) -> u64 {
    u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commit_window_follows_the_policy() {
        let settings = |policy, commit_us| EmitSettings {
            policy,
            spin_window: Duration::from_micros(50),
            commit_window: Duration::from_micros(commit_us),
        };
        assert_eq!(settings(EmitPolicy::FullSpin, 5).commit_window_ns(), 50_000);
        assert_eq!(settings(EmitPolicy::Fixed, 5).commit_window_ns(), 5_000);
        assert_eq!(settings(EmitPolicy::Fixed, 20).commit_window_ns(), 20_000);
        assert_eq!(settings(EmitPolicy::Fixed, 80).commit_window_ns(), 50_000);
        assert_eq!(settings(EmitPolicy::Fixed, 0).commit_window_ns(), 0);
        assert_eq!(settings(EmitPolicy::Fixed, 5).spin_window_ns(), 50_000);
        let huge = EmitSettings {
            policy: EmitPolicy::FullSpin,
            spin_window: Duration::MAX,
            commit_window: Duration::MAX,
        };
        assert_eq!(huge.commit_window_ns(), u64::MAX);
    }

    #[test]
    fn policy_names_match_the_command_line() {
        for (policy, name) in [
            (EmitPolicy::FullSpin, "full-spin"),
            (EmitPolicy::Fixed, "fixed"),
        ] {
            assert_eq!(policy.to_string(), name);
            assert_eq!(
                serde_json::to_value(policy).unwrap(),
                serde_json::Value::from(name)
            );
        }
        assert_eq!(EmitPolicy::default(), EmitPolicy::Fixed);
    }
}
