//! Helpers shared by the tests that run the `brisk` binary.

#![allow(dead_code, reason = "each test binary uses a different subset")]

use std::path::{Path, PathBuf};
use std::process::Command;

/// Default model of every test (4.1); the brackets must pass through.
pub(crate) const TEST_MODEL: &str = "grok-4.6(xhigh)";

/// Self-signed P-256 end-entity certificate for `localhost`, valid until
/// 2126, the same as in the `tls` module's tests; test material only.
pub(crate) const CERT_PEM: &str = "-----BEGIN CERTIFICATE-----
MIIBuDCCAV+gAwIBAgIUXsfLXsgZaYwGl11TTQjWsMyyqV4wCgYIKoZIzj0EAwIw
FDESMBAGA1UEAwwJbG9jYWxob3N0MCAXDTI2MDkxOTEzMzAxMloYDzIxMjYwODI2
MTMzMDEyWjAUMRIwEAYDVQQDDAlsb2NhbGhvc3QwWTATBgcqhkjOPQIBBggqhkjO
PQMBBwNCAASrldOJF9nKGCFh+pChEbyx+PGi5vJbRR7jOXPqltwRzwDQufBBQKWg
cuYmuVafqOso38OO9iq6f3Zb4RmURh4Ao4GMMIGJMB0GA1UdDgQWBBSDnnfakiQN
pmtLrjzU765j9mMchTAfBgNVHSMEGDAWgBSDnnfakiQNpmtLrjzU765j9mMchTAU
BgNVHREEDTALgglsb2NhbGhvc3QwDAYDVR0TAQH/BAIwADAOBgNVHQ8BAf8EBAMC
B4AwEwYDVR0lBAwwCgYIKwYBBQUHAwEwCgYIKoZIzj0EAwIDRwAwRAIge9OlM9/8
LrXfZOG22zaiM7D/202t8i707sZkljieD7sCIGlhT4LplDyqsoO23EAx0AjOCQ2a
DN/cZ0O6hhVqXyVN
-----END CERTIFICATE-----
";

/// The key of [`CERT_PEM`]; test material only.
pub(crate) const KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgmPbhLbDR/HLa13Fn
X/D6JwJLDAUkp7EMndkpEFCXzcahRANCAASrldOJF9nKGCFh+pChEbyx+PGi5vJb
RR7jOXPqltwRzwDQufBBQKWgcuYmuVafqOso38OO9iq6f3Zb4RmURh4A
-----END PRIVATE KEY-----
";

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

/// Sets the permission bits of `path`, so that which files the loader warns
/// about does not depend on the umask of the machine running the tests.
#[cfg(unix)]
pub(crate) fn set_mode(path: &Path, mode: u32) {
    use std::os::unix::fs::PermissionsExt as _;

    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
        .expect("set the file mode");
}

/// The loader's warning about `path`, a file holding a secret at mode 0644.
pub(crate) fn world_readable_warning(path: &Path) -> String {
    format!(
        "{} holds a secret and is readable by other users (mode 644); consider chmod o-r",
        path.display()
    )
}

/// Lowercase hexadecimal of `bytes`.
pub(crate) fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    bytes.iter().fold(String::new(), |mut out, byte| {
        write!(out, "{byte:02x}").expect("writing to a String cannot fail");
        out
    })
}
