//! CPU lists and thread pinning.
//!
//! Benchmark shards pin themselves to the cores given on the command line
//! (`--cpu-list 2,4-5`). Pinning and affinity queries use
//! `sched_setaffinity`/`sched_getaffinity` on Linux and report
//! [`io::ErrorKind::Unsupported`] elsewhere, so callers decide explicitly
//! whether running unpinned is acceptable.

use std::io;

/// Largest CPU number representable in the kernel's fixed-size `cpu_set_t`.
pub const MAX_CPUS: usize = 1024;

/// A malformed CPU list.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid CPU list {list:?}: {reason}")]
pub struct CpuListError {
    /// The rejected input.
    pub list: String,
    /// What is wrong with it.
    pub reason: &'static str,
}

/// Parses a Linux-style CPU list such as `0-3,6,8-9` into sorted, distinct
/// CPU numbers.
pub fn parse_cpu_list(list: &str) -> Result<Vec<usize>, CpuListError> {
    let err = |reason| CpuListError {
        list: list.to_owned(),
        reason,
    };
    let mut cpus = Vec::new();
    for part in list.trim().split(',') {
        let part = part.trim();
        let (lo, hi) = match part.split_once('-') {
            Some((lo, hi)) => (lo.trim(), hi.trim()),
            None => (part, part),
        };
        let lo: usize = lo.parse().map_err(|_| err("not a number"))?;
        let hi: usize = hi.parse().map_err(|_| err("not a number"))?;
        if lo > hi {
            return Err(err("range end before start"));
        }
        if hi >= MAX_CPUS {
            return Err(err("CPU number too large"));
        }
        cpus.extend(lo..=hi);
    }
    cpus.sort_unstable();
    cpus.dedup();
    Ok(cpus)
}

/// Formats CPU numbers as a compact list (`0-3,6`). Input need not be sorted.
pub fn format_cpu_list(cpus: &[usize]) -> String {
    let mut sorted = cpus.to_vec();
    sorted.sort_unstable();
    sorted.dedup();
    let mut out = String::new();
    let mut i = 0;
    while i < sorted.len() {
        let start = sorted[i];
        let mut end = start;
        while i + 1 < sorted.len() && sorted[i + 1] == end + 1 {
            i += 1;
            end = sorted[i];
        }
        if !out.is_empty() {
            out.push(',');
        }
        out.push_str(&start.to_string());
        if start != end {
            out.push('-');
            out.push_str(&end.to_string());
        }
        i += 1;
    }
    out
}

/// CPUs the calling thread may run on.
pub fn current_affinity() -> io::Result<Vec<usize>> {
    imp::current_affinity()
}

/// Restricts the calling thread to `cpus`.
pub fn pin_current_thread(cpus: &[usize]) -> io::Result<()> {
    if cpus.is_empty() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "empty CPU set"));
    }
    if let Some(&cpu) = cpus.iter().find(|&&c| c >= MAX_CPUS) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("CPU {cpu} exceeds {MAX_CPUS}"),
        ));
    }
    imp::pin_current_thread(cpus)
}

#[cfg(target_os = "linux")]
mod imp {
    use std::io;
    use std::mem::MaybeUninit;

    use super::MAX_CPUS;

    fn empty_set() -> libc::cpu_set_t {
        // SAFETY: cpu_set_t is a plain bit array; all-zero is the empty set.
        unsafe { MaybeUninit::zeroed().assume_init() }
    }

    pub(super) fn current_affinity() -> io::Result<Vec<usize>> {
        let mut set = empty_set();
        // SAFETY: pid 0 is the calling thread; `set` is a live cpu_set_t of
        // the size passed.
        let rc = unsafe { libc::sched_getaffinity(0, size_of::<libc::cpu_set_t>(), &raw mut set) };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: every index is below CPU_SETSIZE, the bit array's capacity.
        Ok((0..MAX_CPUS)
            .filter(|&cpu| unsafe { libc::CPU_ISSET(cpu, &set) })
            .collect())
    }

    pub(super) fn pin_current_thread(cpus: &[usize]) -> io::Result<()> {
        let mut set = empty_set();
        for &cpu in cpus {
            // SAFETY: the caller checked cpu < MAX_CPUS (CPU_SETSIZE).
            unsafe { libc::CPU_SET(cpu, &mut set) };
        }
        // SAFETY: pid 0 is the calling thread; `set` is a live cpu_set_t of
        // the size passed.
        let rc =
            unsafe { libc::sched_setaffinity(0, size_of::<libc::cpu_set_t>(), &raw const set) };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
}

#[cfg(not(target_os = "linux"))]
mod imp {
    use std::io;

    fn unsupported() -> io::Error {
        io::Error::new(
            io::ErrorKind::Unsupported,
            "CPU affinity is only supported on Linux",
        )
    }

    pub(super) fn current_affinity() -> io::Result<Vec<usize>> {
        Err(unsupported())
    }

    pub(super) fn pin_current_thread(_cpus: &[usize]) -> io::Result<()> {
        Err(unsupported())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_formats_lists() {
        assert_eq!(
            parse_cpu_list("0-3,6, 8-9,2").unwrap(),
            vec![0, 1, 2, 3, 6, 8, 9]
        );
        assert_eq!(parse_cpu_list("2").unwrap(), vec![2]);
        assert!(parse_cpu_list("").is_err());
        assert!(parse_cpu_list("3-1").is_err());
        assert!(parse_cpu_list("a").is_err());
        assert!(parse_cpu_list("0-4096").is_err());
        assert_eq!(format_cpu_list(&[9, 0, 1, 2, 3, 6, 8]), "0-3,6,8-9");
        assert_eq!(format_cpu_list(&[]), "");
    }

    #[test]
    fn rejects_invalid_pin_sets() {
        assert_eq!(
            pin_current_thread(&[]).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(
            pin_current_thread(&[MAX_CPUS]).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn pin_round_trips_on_linux() {
        let handle = std::thread::spawn(|| {
            let allowed = current_affinity().unwrap();
            assert!(!allowed.is_empty());
            let target = allowed[allowed.len() - 1];
            pin_current_thread(&[target]).unwrap();
            assert_eq!(current_affinity().unwrap(), vec![target]);
        });
        handle.join().unwrap();
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn affinity_is_unsupported_elsewhere() {
        assert_eq!(
            current_affinity().unwrap_err().kind(),
            io::ErrorKind::Unsupported
        );
    }
}
