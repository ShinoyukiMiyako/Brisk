//! `brisk keygen` and `brisk check-config` run as the binary: the frozen
//! stdout format of `keygen` byte for byte, the `check-config` summary, and
//! that neither ever prints a secret (1.5, 3.1).

mod support;

use std::process::Output;

use brisk_gateway::auth::{is_well_formed, key_digest};

use support::{CERT_PEM, KEY_PEM, TEST_MODEL, TempDir, brisk, hex};

/// Every upstream key in the `check-config` configuration.
const SECRETS: [&str; 3] = ["sk-inline-secret-1", "sk-file-secret-2", "sk-env-secret-3"];

fn text(bytes: &[u8]) -> &str {
    std::str::from_utf8(bytes).expect("UTF-8 output")
}

fn assert_failed(output: &Output) {
    assert!(
        !output.status.success(),
        "exited successfully; stderr:\n{}",
        text(&output.stderr)
    );
}

fn assert_succeeded(output: &Output) {
    assert!(
        output.status.success(),
        "{}; stderr:\n{}",
        output.status,
        text(&output.stderr)
    );
}

#[test]
fn keygen_stdout_is_exactly_the_frozen_format() {
    // Verbose logging must not reach stdout either.
    let output = brisk()
        .args(["keygen", "--name", "smoke"])
        .env("RUST_LOG", "trace")
        .output()
        .expect("run brisk keygen");
    assert_succeeded(&output);

    let stdout = text(&output.stdout);
    let key = stdout.lines().next().expect("the key line");
    assert!(key.starts_with("bk-"), "{key:?}");
    assert_eq!(key.len(), 3 + 36, "{key:?}");
    assert!(
        key[3..]
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_'),
        "{key:?}"
    );
    assert!(is_well_formed(key.as_bytes()), "{key:?}");
    let expected = format!(
        "{key}\n\n[[keys]]\nname = \"smoke\"\nsha256 = \"{}\"\n",
        hex(&key_digest(key.as_bytes()))
    );
    assert_eq!(stdout, expected);

    let stderr = text(&output.stderr);
    assert!(stderr.contains("shown only once"), "{stderr}");
    assert!(!stderr.contains(key), "the key reached stderr:\n{stderr}");
}

#[test]
fn keygen_escapes_the_name_in_the_snippet() {
    let output = brisk()
        .args(["keygen", "--name", r#"ops "night" \ shift"#])
        .output()
        .expect("run brisk keygen");
    assert_succeeded(&output);
    let lines: Vec<&str> = text(&output.stdout).lines().collect();
    assert_eq!(lines.len(), 5, "{lines:?}");
    assert_eq!(lines[3], r#"name = "ops \"night\" \\ shift""#);
}

#[test]
fn keygen_rejects_an_invalid_name_without_printing_a_key() {
    for name in ["", "tab\there"] {
        let output = brisk()
            .args(["keygen", "--name", name])
            .output()
            .expect("run brisk keygen");
        assert_failed(&output);
        assert!(output.stdout.is_empty(), "{:?}", text(&output.stdout));
        let stderr = text(&output.stderr);
        assert!(
            stderr.contains("key name must be non-empty printable ASCII"),
            "{stderr}"
        );
    }
}

#[test]
fn an_invalid_log_filter_fails_before_any_output() {
    let output = brisk()
        .args(["keygen", "--name", "smoke"])
        .env("RUST_LOG", "brisk=loudest")
        .output()
        .expect("run brisk keygen");
    assert_failed(&output);
    assert!(output.stdout.is_empty(), "{:?}", text(&output.stdout));
    assert!(text(&output.stderr).contains("RUST_LOG"));
}

/// A `*.local.toml` with every kind of secret source, on local and private
/// addresses, arranged to raise each kind of channel warning, in a directory
/// of its own per `test` (tests run in parallel). On Unix the configuration,
/// which holds an inline secret, and the key file are both mode 0644, which
/// the loader warns about.
fn check_config_dir(test: &str) -> (TempDir, std::path::PathBuf) {
    let dir = TempDir::new(test);
    dir.write("upstream.key", &format!("{}\n", SECRETS[1]));
    let config = dir.write(
        "check.local.toml",
        &format!(
            r#"
[server]
listen = "127.0.0.1:18080"

[[keys]]
name = "local-dev"
sha256 = "{digest}"

[[channels]]
name = "inline"
base_url = "http://127.0.0.1:18081/v1"
api_key = {{ value = "{inline}" }}
allow_private = true
models = ["{TEST_MODEL}"]

# No path: raises NoVersionPath.
[[channels]]
name = "from-file"
base_url = "http://localhost:18082"
api_key = {{ file = "upstream.key" }}
allow_private = true

# Plain http to a private, non-loopback host: raises PlaintextRemote.
[[channels]]
name = "from-env"
base_url = "http://192.168.10.180:8317/v1"
api_key = {{ env = "BRISK_CHECK_SECRET" }}
allow_private = true

# Same host as "inline" with another client profile: raises SplitPool.
[[channels]]
name = "split"
base_url = "http://127.0.0.1:18081/v1"
api_key = {{ env = "BRISK_CHECK_SECRET" }}
allow_private = true
connect_timeout = "3s"
"#,
            digest = "0".repeat(64),
            inline = SECRETS[0],
        ),
    );
    #[cfg(unix)]
    for path in [dir.path().join("upstream.key"), config.clone()] {
        support::set_mode(&path, 0o644);
    }
    (dir, config)
}

#[test]
fn check_config_prints_the_summary_without_secrets() {
    let (_dir, config) = check_config_dir("check-config-summary");
    let output = brisk()
        .arg("check-config")
        .arg("--config")
        .arg(&config)
        .env("BRISK_CHECK_SECRET", SECRETS[2])
        .output()
        .expect("run brisk check-config");
    assert_succeeded(&output);

    let stdout = text(&output.stdout);
    let stderr = text(&output.stderr);
    for secret in SECRETS {
        assert!(!stdout.contains(secret), "{secret} on stdout:\n{stdout}");
        assert!(!stderr.contains(secret), "{secret} on stderr:\n{stderr}");
    }

    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(
        lines[..7],
        [
            format!("configuration: {}", config.display()).as_str(),
            "listen: 127.0.0.1:18080 (plaintext)",
            "channels: 4",
            r#"  "inline" -> http://127.0.0.1:18081/v1/chat/completions"#,
            r#"  "from-file" -> http://localhost:18082/chat/completions"#,
            r#"  "from-env" -> http://192.168.10.180:8317/v1/chat/completions"#,
            r#"  "split" -> http://127.0.0.1:18081/v1/chat/completions"#,
        ],
        "{stdout}"
    );
    // The loader's warnings come first, in the order it checked the files:
    // the key file while building the channels, then the configuration.
    // Windows ACLs are not inspected, so there are none there.
    #[cfg(unix)]
    let loader_warnings: &[String] = &[
        support::world_readable_warning(&config.with_file_name("upstream.key")),
        support::world_readable_warning(&config),
    ];
    #[cfg(not(unix))]
    let loader_warnings: &[String] = &[];
    let channel_warnings = [
        r#"channel "from-file" has a base_url without a path, so /chat/completions is requested at the root; a version prefix such as /v1 is usually missing"#,
        r#"channel "from-env" uses plain http to a remote host; its key travels in cleartext"#,
        "upstream 127.0.0.1:18081 is reached through several client profiles, so its connections are split across pools",
    ];
    let expected: Vec<String> = loader_warnings
        .iter()
        .map(String::as_str)
        .chain(channel_warnings)
        .map(|warning| format!("  {warning}"))
        .collect();
    assert_eq!(
        lines[7],
        format!("warnings: {}", expected.len()),
        "{stdout}"
    );
    assert_eq!(lines[8..], expected, "{stdout}");

    // `run` also logs each loader warning at warn level once tracing is up
    // (1.5); the channel warnings are the gateway's to log.
    let logged: Vec<&str> = stderr
        .lines()
        .filter(|line| line.contains(" WARN brisk: "))
        .collect();
    assert_eq!(logged.len(), loader_warnings.len(), "{stderr}");
    for (line, warning) in logged.iter().zip(loader_warnings) {
        assert!(line.ends_with(warning.as_str()), "{line:?} vs {warning:?}");
    }
}

#[test]
fn check_config_reports_a_missing_secret_by_name_only() {
    let (_dir, config) = check_config_dir("check-config-missing-env");
    let output = brisk()
        .arg("check-config")
        .arg("--config")
        .arg(&config)
        .env_remove("BRISK_CHECK_SECRET")
        .output()
        .expect("run brisk check-config");
    assert_failed(&output);
    assert!(output.stdout.is_empty(), "{:?}", text(&output.stdout));
    let stderr = text(&output.stderr);
    assert!(
        stderr.contains("environment variable BRISK_CHECK_SECRET for api_key is not set"),
        "{stderr}"
    );
    for secret in SECRETS {
        assert!(!stderr.contains(secret), "{secret} on stderr:\n{stderr}");
    }
}

#[test]
fn check_config_fails_on_a_missing_file() {
    let dir = TempDir::new("check-config-missing");
    let absent = dir.path().join("absent.toml");
    let output = brisk()
        .arg("check-config")
        .arg("--config")
        .arg(&absent)
        .output()
        .expect("run brisk check-config");
    assert_failed(&output);
    assert!(output.stdout.is_empty(), "{:?}", text(&output.stdout));
    let stderr = text(&output.stderr);
    assert!(stderr.contains("absent.toml"), "{stderr}");
}

/// A valid P-256 key that does not match [`CERT_PEM`]; test material only.
const OTHER_KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgjQq3ZPK76VdxUz/C
qF7MmQjyDNIkahf+Fxahp3NxLmmhRANCAASiN1kOnbp8OFPuQSDV+dPDSnMFSv5y
7qoWRHrZoBiHpdqsHVGvwf8cQrxL59HiX+R9m0MGrda04ETsNOzXCPXS
-----END PRIVATE KEY-----
";

/// A TLS listener on every interface with `key` as the private key file.
fn tls_config(dir: &TempDir, key: &str) -> std::path::PathBuf {
    dir.write("cert.pem", CERT_PEM);
    dir.write("key.pem", KEY_PEM);
    dir.write("other-key.pem", OTHER_KEY_PEM);
    dir.write(
        "tls.toml",
        &format!(
            r#"
[server]
listen = "0.0.0.0:18443"

[server.tls]
cert = "cert.pem"
key = "{key}"

[[keys]]
name = "local-dev"
sha256 = "{digest}"

[[channels]]
name = "local"
base_url = "http://127.0.0.1:18081/v1"
api_key = {{ env = "BRISK_CHECK_SECRET" }}
allow_private = true
"#,
            digest = "0".repeat(64),
        ),
    )
}

#[test]
fn check_config_builds_the_inbound_tls_acceptor() {
    let dir = TempDir::new("check-config-tls");
    let output = brisk()
        .arg("check-config")
        .arg("--config")
        .arg(tls_config(&dir, "key.pem"))
        .env("BRISK_CHECK_SECRET", SECRETS[2])
        .output()
        .expect("run brisk check-config");
    assert_succeeded(&output);
    let stdout = text(&output.stdout);
    assert_eq!(
        stdout.lines().nth(1),
        Some("listen: 0.0.0.0:18443 (TLS)"),
        "{stdout}"
    );

    let output = brisk()
        .arg("check-config")
        .arg("--config")
        .arg(tls_config(&dir, "other-key.pem"))
        .env("BRISK_CHECK_SECRET", SECRETS[2])
        .output()
        .expect("run brisk check-config");
    assert_failed(&output);
    assert!(output.stdout.is_empty(), "{:?}", text(&output.stdout));
    let stderr = text(&output.stderr);
    assert!(
        stderr.contains("loading the inbound TLS certificate and key"),
        "{stderr}"
    );
}
