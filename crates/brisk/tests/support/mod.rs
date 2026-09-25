//! Helpers shared by the tests that run the `brisk` binary.

#![allow(dead_code, reason = "each test binary uses a different subset")]

use std::path::{Path, PathBuf};
use std::process::Command;

/// Default model of every test (4.1); the brackets must pass through.
pub(crate) const TEST_MODEL: &str = "grok-4.6(xhigh)";

/// The `brisk` binary with the default log filter: `RUST_LOG` from the
/// environment running the tests is removed.
pub(crate) fn brisk() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_brisk"));
    command.env_remove("RUST_LOG");
    command
}

/// A directory under the system temp dir, removed on drop.
pub(crate) struct TempDir(PathBuf);

impl TempDir {
    /// A fresh directory named after the test and this process.
    pub(crate) fn new(test: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("brisk-bin-{test}-{}", std::process::id()));
        if dir.exists() {
            std::fs::remove_dir_all(&dir).expect("remove a stale test directory");
        }
        std::fs::create_dir_all(&dir).expect("create the test directory");
        Self(dir)
    }

    /// The directory.
    pub(crate) fn path(&self) -> &Path {
        &self.0
    }

    /// Writes `contents` to `name` inside the directory.
    pub(crate) fn write(&self, name: &str, contents: &str) -> PathBuf {
        let path = self.0.join(name);
        std::fs::write(&path, contents).expect("write a test file");
        path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        // Best effort: a leftover directory in the temp dir harms nothing.
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Lowercase hexadecimal of `bytes`.
pub(crate) fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    bytes.iter().fold(String::new(), |mut out, byte| {
        write!(out, "{byte:02x}").expect("writing to a String cannot fail");
        out
    })
}
