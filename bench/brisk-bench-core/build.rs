//! Embeds build provenance for `fingerprint`: the rustc version, the git
//! commit and an FNV-1a 64 hash of the workspace `Cargo.lock`. Anything that
//! cannot be determined becomes `unknown`; the build never fails over it.
//!
//! The commit gets a `-dirty` suffix when the workspace has uncommitted
//! changes, untracked files included, so results of uncommitted code are
//! never attributed to a clean commit.

use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

const UNKNOWN: &str = "unknown";

fn main() {
    let manifest_dir = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("set by cargo"));

    let rustc = env::var_os("RUSTC").unwrap_or_else(|| "rustc".into());
    let rustc_version = command_stdout(Command::new(rustc).arg("--version"));
    println!(
        "cargo:rustc-env=BRISK_RUSTC_VERSION={}",
        rustc_version.as_deref().unwrap_or(UNKNOWN)
    );

    let workspace_root = find_upwards(&manifest_dir, "Cargo.lock")
        .and_then(|lock| lock.parent().map(Path::to_path_buf));
    let git = |args: &[&str]| {
        let mut cmd = Command::new("git");
        cmd.arg("-C")
            .arg(workspace_root.as_deref().unwrap_or(&manifest_dir))
            .args(args);
        cmd
    };

    let git_commit = command_stdout(&mut git(&["rev-parse", "HEAD"])).map(|commit| {
        // Paths relative to the workspace root; gitignored build output is
        // excluded by git itself.
        let dirty = command_stdout(&mut git(&["status", "--porcelain", "--", "."])).is_some();
        if dirty {
            format!("{commit}-dirty")
        } else {
            commit
        }
    });
    println!(
        "cargo:rustc-env=BRISK_GIT_COMMIT={}",
        git_commit.as_deref().unwrap_or(UNKNOWN)
    );
    if let Some(git_dir) = command_stdout(&mut git(&["rev-parse", "--absolute-git-dir"])) {
        let git_dir = PathBuf::from(git_dir);
        // HEAD changes on checkout, the ref file or packed-refs on commit and
        // the index on staging.
        for file in ["HEAD", "packed-refs", "index"] {
            println!("cargo:rerun-if-changed={}", git_dir.join(file).display());
        }
        if let Some(head_ref) = std::fs::read_to_string(git_dir.join("HEAD"))
            .ok()
            .and_then(|head| head.strip_prefix("ref: ").map(|r| r.trim().to_owned()))
        {
            println!(
                "cargo:rerun-if-changed={}",
                git_dir.join(head_ref).display()
            );
        }
        // Unstaged edits touch neither of the above; watching the source
        // trees (cargo scans directories recursively) keeps `-dirty` current.
        if let Some(root) = &workspace_root {
            for tree in ["crates", "bench"] {
                let dir = root.join(tree);
                if dir.is_dir() {
                    println!("cargo:rerun-if-changed={}", dir.display());
                }
            }
        }
    }

    let lock_hash = find_upwards(&manifest_dir, "Cargo.lock").and_then(|lock| {
        println!("cargo:rerun-if-changed={}", lock.display());
        std::fs::read(&lock)
            .ok()
            .map(|bytes| format!("{:016x}", fnv1a64(&bytes)))
    });
    println!(
        "cargo:rustc-env=BRISK_CARGO_LOCK_FNV1A={}",
        lock_hash.as_deref().unwrap_or(UNKNOWN)
    );
    println!("cargo:rerun-if-changed=build.rs");
}

/// Runs a command and returns its trimmed stdout if it succeeded.
fn command_stdout(cmd: &mut Command) -> Option<String> {
    let output = cmd.output().ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8(output.stdout).ok()?;
    let text = text.trim();
    (!text.is_empty()).then(|| text.to_owned())
}

/// Finds `name` in `start` or its closest ancestor.
fn find_upwards(start: &Path, name: &str) -> Option<PathBuf> {
    start
        .ancestors()
        .map(|dir| dir.join(name))
        .find(|candidate| candidate.is_file())
}

/// 64-bit FNV-1a.
fn fnv1a64(bytes: &[u8]) -> u64 {
    const OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    bytes.iter().fold(OFFSET_BASIS, |hash, &b| {
        (hash ^ u64::from(b)).wrapping_mul(PRIME)
    })
}
