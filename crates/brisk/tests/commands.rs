//! `brisk keygen` and `brisk check-config` run as the binary: the frozen
//! stdout format of `keygen` byte for byte, the `check-config` summary, and
//! that neither ever prints a secret (1.5, 3.1).

mod support;

use std::process::Output;

use brisk_gateway::auth::{is_well_formed, key_digest};

use support::{TEST_MODEL, TempDir, brisk, hex};

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
/// of its own per `test` (tests run in parallel).
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
    let warnings: Vec<&str> = lines[8..].to_vec();
    // On Unix the loader also warns about the world-readable secret files.
    assert_eq!(
        lines[7],
        format!("warnings: {}", warnings.len()),
        "{stdout}"
    );
    let channel_warnings = [
        r#"  channel "from-file" has a base_url without a path, so /chat/completions is requested at the root; a version prefix such as /v1 is usually missing"#,
        r#"  channel "from-env" uses plain http to a remote host; its key travels in cleartext"#,
        "  upstream 127.0.0.1:18081 is reached through several client profiles, so its connections are split across pools",
    ];
    assert_eq!(
        warnings[warnings.len() - channel_warnings.len()..],
        channel_warnings,
        "{stdout}"
    );
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

/// Self-signed P-256 end-entity certificate for `localhost`, valid until
/// 2126, the same as in the `tls` module's tests; test material only.
const CERT_PEM: &str = "-----BEGIN CERTIFICATE-----
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
const KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgmPbhLbDR/HLa13Fn
X/D6JwJLDAUkp7EMndkpEFCXzcahRANCAASrldOJF9nKGCFh+pChEbyx+PGi5vJb
RR7jOXPqltwRzwDQufBBQKWgcuYmuVafqOso38OO9iq6f3Zb4RmURh4A
-----END PRIVATE KEY-----
";

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
