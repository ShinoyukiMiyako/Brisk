//! Process resource limits.

use std::io;

/// Raises the soft `RLIMIT_NOFILE` to the hard limit (capped at `OPEN_MAX`
/// on macOS), since every open stream holds a socket. Returns the resulting
/// soft limit; `None` means unlimited, or a platform without the limit.
///
/// Rust does not raise this limit at startup, and interactive sessions often
/// default to 1024, which is far below the connection counts of the
/// benchmark scenarios.
pub fn raise_nofile_limit() -> io::Result<Option<u64>> {
    imp::raise_nofile_limit()
}

#[cfg(unix)]
mod imp {
    use std::io;

    use rustix::process::{Resource, Rlimit, getrlimit, setrlimit};

    /// `OPEN_MAX` from `<sys/syslimits.h>`: the largest soft limit macOS
    /// accepts whatever the hard limit says.
    #[cfg(target_os = "macos")]
    const MACOS_OPEN_MAX: u64 = 10_240;

    #[cfg(not(target_os = "macos"))]
    fn target(hard: Option<u64>) -> Option<u64> {
        hard
    }

    #[cfg(target_os = "macos")]
    #[expect(
        clippy::unnecessary_wraps,
        reason = "shares the signature of the other Unix implementation"
    )]
    fn target(hard: Option<u64>) -> Option<u64> {
        Some(hard.map_or(MACOS_OPEN_MAX, |hard| hard.min(MACOS_OPEN_MAX)))
    }

    pub(super) fn raise_nofile_limit() -> io::Result<Option<u64>> {
        let limit = getrlimit(Resource::Nofile);
        let wanted = target(limit.maximum);
        if limit.current != wanted {
            setrlimit(
                Resource::Nofile,
                Rlimit {
                    current: wanted,
                    maximum: limit.maximum,
                },
            )?;
        }
        Ok(wanted)
    }
}

#[cfg(not(unix))]
mod imp {
    #[expect(
        clippy::unnecessary_wraps,
        reason = "shares the signature of the Unix implementation"
    )]
    pub(super) fn raise_nofile_limit() -> std::io::Result<Option<u64>> {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raising_is_idempotent() {
        let first = raise_nofile_limit().unwrap();
        assert_eq!(raise_nofile_limit().unwrap(), first);
        #[cfg(target_os = "linux")]
        assert!(first.is_none_or(|n| n >= 1024), "{first:?}");
    }
}
