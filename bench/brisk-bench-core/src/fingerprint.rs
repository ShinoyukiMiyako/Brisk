//! Host and build fingerprint recorded with every run, so results from
//! different machines, kernels or builds are never mixed unknowingly.

use serde::{Deserialize, Serialize};

/// rustc version the crate was built with, or `unknown`.
pub const RUSTC_VERSION: &str = env!("BRISK_RUSTC_VERSION");
/// Git commit the crate was built from, or `unknown`.
pub const GIT_COMMIT: &str = env!("BRISK_GIT_COMMIT");
/// FNV-1a 64 hash of the workspace `Cargo.lock` (16 hex digits), or `unknown`.
pub const CARGO_LOCK_FNV1A: &str = env!("BRISK_CARGO_LOCK_FNV1A");

/// Environment a run was produced in.
///
/// Host fields are `None` when the platform does not expose them (everything
/// but the OS and architecture is Linux-specific).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Fingerprint {
    /// Host name.
    pub hostname: Option<String>,
    /// `std::env::consts::OS`.
    pub os: String,
    /// `std::env::consts::ARCH`.
    pub arch: String,
    /// Kernel release (`/proc/sys/kernel/osrelease`).
    pub kernel: Option<String>,
    /// CPU model name from `/proc/cpuinfo`.
    pub cpu_model: Option<String>,
    /// Online CPUs (`/sys/devices/system/cpu/online`).
    pub online_cpus: Option<String>,
    /// This process's CPU affinity, as a CPU list.
    pub affinity: Option<String>,
    /// Active clocksource, e.g. `tsc` or `kvm-clock`.
    pub clocksource: Option<String>,
    /// See [`RUSTC_VERSION`].
    pub rustc: String,
    /// See [`GIT_COMMIT`].
    pub git_commit: String,
    /// See [`CARGO_LOCK_FNV1A`].
    pub cargo_lock_fnv1a: String,
}

impl Fingerprint {
    /// Collects the fingerprint of the current process and host.
    pub fn collect() -> Self {
        Self {
            hostname: imp::hostname(),
            os: std::env::consts::OS.to_owned(),
            arch: std::env::consts::ARCH.to_owned(),
            kernel: imp::read_trimmed("/proc/sys/kernel/osrelease"),
            cpu_model: imp::cpu_model(),
            online_cpus: imp::read_trimmed("/sys/devices/system/cpu/online"),
            affinity: crate::cpu::current_affinity()
                .ok()
                .map(|cpus| crate::cpu::format_cpu_list(&cpus)),
            clocksource: imp::read_trimmed(
                "/sys/devices/system/clocksource/clocksource0/current_clocksource",
            ),
            rustc: RUSTC_VERSION.to_owned(),
            git_commit: GIT_COMMIT.to_owned(),
            cargo_lock_fnv1a: CARGO_LOCK_FNV1A.to_owned(),
        }
    }
}

#[cfg(target_os = "linux")]
mod imp {
    pub(super) fn read_trimmed(path: &str) -> Option<String> {
        let text = std::fs::read_to_string(path).ok()?;
        let text = text.trim();
        (!text.is_empty()).then(|| text.to_owned())
    }

    pub(super) fn hostname() -> Option<String> {
        read_trimmed("/proc/sys/kernel/hostname")
    }

    pub(super) fn cpu_model() -> Option<String> {
        let info = std::fs::read_to_string("/proc/cpuinfo").ok()?;
        info.lines()
            .find(|line| line.starts_with("model name"))
            .and_then(|line| line.split_once(':'))
            .map(|(_, model)| model.trim().to_owned())
    }
}

#[cfg(not(target_os = "linux"))]
mod imp {
    pub(super) fn read_trimmed(_path: &str) -> Option<String> {
        None
    }

    pub(super) fn hostname() -> Option<String> {
        ["COMPUTERNAME", "HOSTNAME"]
            .into_iter()
            .find_map(|var| std::env::var(var).ok().filter(|v| !v.is_empty()))
    }

    pub(super) fn cpu_model() -> Option<String> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collect_fills_build_fields() {
        let fp = Fingerprint::collect();
        assert_eq!(fp.os, std::env::consts::OS);
        assert!(!fp.rustc.is_empty());
        assert!(fp.rustc == "unknown" || fp.rustc.starts_with("rustc "));
        assert!(
            fp.cargo_lock_fnv1a == "unknown"
                || (fp.cargo_lock_fnv1a.len() == 16
                    && fp.cargo_lock_fnv1a.bytes().all(|b| b.is_ascii_hexdigit()))
        );
        assert!(!fp.git_commit.is_empty());
        let json = serde_json::to_string(&fp).unwrap();
        assert_eq!(serde_json::from_str::<Fingerprint>(&json).unwrap(), fp);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_host_fields_present() {
        let fp = Fingerprint::collect();
        assert!(fp.hostname.is_some());
        assert!(fp.kernel.is_some());
        assert!(fp.online_cpus.is_some());
        assert!(fp.affinity.is_some());
    }
}
