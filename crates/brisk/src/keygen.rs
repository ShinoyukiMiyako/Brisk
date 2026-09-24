//! `brisk keygen`: a fresh virtual key and the `[[keys]]` snippet that stores
//! only its digest (D6).
//!
//! The standard output format is a frozen interface parsed by
//! `scripts/bench/run-m1.sh`: the key on line 1, an empty line 2, then the
//! three snippet lines, and nothing else. Messages for humans go to stderr.
//!
//! ```text
//! bk-<36 base64url characters>
//!
//! [[keys]]
//! name = "<name>"
//! sha256 = "<64 lowercase hex digits>"
//! ```

use std::error::Error as StdError;
use std::fmt;
use std::fmt::Write as _;
use std::io;

use brisk_gateway::secret::Redacted;

/// A new virtual key and its configuration snippet.
#[derive(Debug)]
pub struct GeneratedKey {
    /// Shown once on stdout, never logged.
    pub key: Redacted<String>,
    /// `[[keys]]` table with `name` and `sha256`, ready to paste.
    pub toml_snippet: String,
}

impl GeneratedKey {
    /// Writes the frozen stdout format described in the module documentation.
    pub(crate) fn write_stdout(&self, out: &mut dyn io::Write) -> io::Result<()> {
        write!(out, "{}\n\n{}\n", self.key.expose(), self.toml_snippet)?;
        out.flush()
    }
}

/// Why no key was generated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeygenError {
    /// The system random number generator failed.
    Random,
    /// The name is empty or not printable ASCII.
    Name,
}

impl fmt::Display for KeygenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Random => "the system random number generator failed",
            Self::Name => "key name must be non-empty printable ASCII",
        })
    }
}

impl StdError for KeygenError {}

/// Accepts non-empty printable ASCII (space through `~`), so the name reads
/// the same in logs, the TOML file and a terminal.
pub(crate) fn validate_name(name: &str) -> Result<(), KeygenError> {
    if !name.is_empty() && name.bytes().all(|byte| (b' '..=b'~').contains(&byte)) {
        Ok(())
    } else {
        Err(KeygenError::Name)
    }
}

/// `N` bytes from the operating system's generator, via aws-lc-rs.
pub(crate) fn random_bytes<const N: usize>() -> Result<[u8; N], KeygenError> {
    let mut bytes = [0; N];
    aws_lc_rs::rand::fill(&mut bytes).map_err(|_| KeygenError::Random)?;
    Ok(bytes)
}

/// The three-line `[[keys]]` table, without a trailing newline. `name` must
/// have passed [`validate_name`]; `"` and `\` are escaped so any printable
/// name stays a valid TOML basic string.
pub(crate) fn toml_snippet(name: &str, digest: &[u8; 32]) -> String {
    let mut snippet = String::with_capacity(40 + name.len() + 2 * digest.len());
    snippet.push_str("[[keys]]\nname = \"");
    for c in name.chars() {
        if matches!(c, '"' | '\\') {
            snippet.push('\\');
        }
        snippet.push(c);
    }
    snippet.push_str("\"\nsha256 = \"");
    for byte in digest {
        write!(snippet, "{byte:02x}").expect("writing to a String cannot fail");
    }
    snippet.push('"');
    snippet
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::*;

    #[derive(Debug, Deserialize)]
    struct Snippet {
        keys: Vec<KeyEntry>,
    }

    #[derive(Debug, Deserialize)]
    struct KeyEntry {
        name: String,
        sha256: String,
    }

    #[test]
    fn names_must_be_printable_ascii() {
        for name in [
            "local-dev",
            "a",
            "with space",
            "q\"uote",
            "back\\slash",
            "~",
        ] {
            assert_eq!(validate_name(name), Ok(()), "{name:?}");
        }
        for name in ["", "tab\there", "new\nline", "caf\u{e9}", "\u{7f}", "nul\0"] {
            assert_eq!(validate_name(name), Err(KeygenError::Name), "{name:?}");
        }
    }

    #[test]
    fn snippet_has_the_frozen_layout() {
        let snippet = toml_snippet("local-dev", &[0xab; 32]);
        let lines: Vec<&str> = snippet.lines().collect();
        assert_eq!(
            lines,
            [
                "[[keys]]",
                "name = \"local-dev\"",
                &format!("sha256 = \"{}\"", "ab".repeat(32)),
            ]
        );
        assert!(!snippet.ends_with('\n'));
    }

    #[test]
    fn snippet_parses_back_to_name_and_digest() {
        for name in ["local-dev", "q\"uote", "back\\slash\\", " spaced "] {
            let snippet = toml_snippet(name, &[0x0f; 32]);
            let parsed: Snippet = toml::from_str(&snippet).unwrap();
            assert_eq!(parsed.keys.len(), 1);
            assert_eq!(parsed.keys[0].name, name);
            assert_eq!(parsed.keys[0].sha256, "0f".repeat(32));
        }
    }

    #[test]
    fn stdout_is_key_blank_line_and_snippet() {
        let generated = GeneratedKey {
            key: Redacted::new(format!("bk-{}", "A".repeat(36))),
            toml_snippet: toml_snippet("smoke", &[0; 32]),
        };
        let mut out = Vec::new();
        generated.write_stdout(&mut out).unwrap();
        let text = String::from_utf8(out).unwrap();
        let expected = format!(
            "bk-{}\n\n[[keys]]\nname = \"smoke\"\nsha256 = \"{}\"\n",
            "A".repeat(36),
            "0".repeat(64)
        );
        assert_eq!(text, expected);
        assert!(!format!("{generated:?}").contains(&"A".repeat(36)));
    }

    #[test]
    fn random_bytes_differ_between_calls() {
        let first = random_bytes::<24>().unwrap();
        let second = random_bytes::<24>().unwrap();
        assert_ne!(first, second);
    }

    #[test]
    fn errors_have_the_documented_messages() {
        assert_eq!(
            KeygenError::Random.to_string(),
            "the system random number generator failed"
        );
        assert_eq!(
            KeygenError::Name.to_string(),
            "key name must be non-empty printable ASCII"
        );
    }
}
