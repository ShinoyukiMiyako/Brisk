//! Configuration loading: the committed examples, defaults and overrides,
//! value syntax, secret sources and rules, and that no error ever carries a
//! secret (3.1, 3.4, 4.3).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use brisk::config::{ConfigError, LoadedConfig, load, load_str};
use brisk_gateway::spec::{
    ForwardingSpec, LimitsSpec, RouterKind, SpliceMode, StreamUsage, Timeouts, WarmupMethod,
    WarmupSpec, WarmupTarget,
};
use brisk_gateway::upstream::UpstreamClientConfig;

const TEST_MODEL: &str = "grok-4.6(xhigh)";

/// The secret every leak assertion looks for.
const LEAK: &str = "sk-leak-123";

const DIGEST: &str = "0000000000000000000000000000000000000000000000000000000000000000";

fn repo_config(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../config")
        .join(name)
}

fn env_with(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
    let map: HashMap<String, String> = pairs
        .iter()
        .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
        .collect();
    move |name| map.get(name).cloned()
}

fn no_env(_: &str) -> Option<String> {
    None
}

/// A minimal valid configuration with `channel` as the body of the only
/// `[[channels]]` table.
fn with_channel(channel: &str) -> String {
    format!(
        r#"
[server]
listen = "127.0.0.1:18080"

[[keys]]
name = "local-dev"
sha256 = "{DIGEST}"

[[channels]]
name = "cpa"
base_url = "http://192.168.10.180:8317/v1"
{channel}
"#
    )
}

fn minimal() -> String {
    with_channel(r#"api_key = { env = "BRISK_CPA_KEY" }"#)
}

fn load_text(text: &str, file_name: &str) -> Result<LoadedConfig, ConfigError> {
    load_str(
        text,
        Path::new("/etc/brisk"),
        file_name,
        &env_with(&[("BRISK_CPA_KEY", "cpa-key")]),
    )
}

fn load_ok(text: &str) -> LoadedConfig {
    load_text(text, "brisk.toml").unwrap_or_else(|err| panic!("{err}"))
}

fn load_err(text: &str) -> ConfigError {
    match load_text(text, "brisk.toml") {
        Ok(config) => panic!("loaded: {config:?}"),
        Err(err) => err,
    }
}

/// Every rendering an operator could see: `Display`, `Debug`, and the chain
/// as `anyhow` prints it in `main`.
fn renderings(err: ConfigError) -> String {
    let display = err.to_string();
    let debug = format!("{err:?}");
    let chain = anyhow::Error::new(err).context("loading configuration");
    format!("{display}\n{debug}\n{chain:?}\n{chain:#}")
}

fn assert_no_leak(text: &str, file_name: &str) -> String {
    let err = match load_text(text, file_name) {
        Ok(config) => panic!("loaded: {config:?}"),
        Err(err) => err,
    };
    let rendered = renderings(err);
    assert!(!rendered.contains(LEAK), "secret leaked:\n{rendered}");
    rendered
}

#[test]
fn cpa_example_loads() {
    let path = repo_config("cpa.example.toml");
    let text = std::fs::read_to_string(&path).unwrap();
    let config = load_str(
        &text,
        path.parent().unwrap(),
        "cpa.example.toml",
        &env_with(&[("BRISK_CPA_KEY", "cpa-key")]),
    )
    .unwrap();

    assert_eq!(
        config.listen,
        "127.0.0.1:18080".parse::<SocketAddr>().unwrap()
    );
    assert_eq!(config.workers.map(std::num::NonZero::get), Some(4));
    assert_eq!(config.tls, None);
    assert_eq!(config.outcome_queue, 16_384);
    assert!(config.warnings.is_empty());
    assert_eq!(config.spec.keys.len(), 1);
    assert_eq!(config.spec.keys[0].name, "local-dev");
    assert_eq!(config.spec.keys[0].sha256, [0; 32]);

    let channels = &config.spec.channels;
    assert_eq!(channels.len(), 2);
    assert_eq!(channels[0].name, "cpa-a");
    assert_eq!(channels[0].weight, 3);
    assert_eq!(channels[1].name, "cpa-b");
    assert_eq!(channels[1].weight, 1);
    for channel in channels {
        assert_eq!(channel.base_url, "http://192.168.10.180:8317/v1");
        assert_eq!(channel.api_key.expose(), "cpa-key");
        assert!(channel.client.allow_private);
        assert_eq!(channel.stream_usage, StreamUsage::Passthrough);
        assert_eq!(channel.models, [TEST_MODEL]);
        assert_eq!(
            channel.warmup,
            WarmupTarget {
                method: WarmupMethod::Head,
                path: "/healthz".to_owned(),
            }
        );
        assert_eq!(channel.timeouts, Timeouts::default());
    }
}

#[test]
fn brisk_example_loads_with_every_default() {
    let config = load(&repo_config("brisk.example.toml"));
    // `load` reads the process environment; the example uses an env source.
    match config {
        Err(ConfigError::MissingEnv { channel, var }) => {
            assert_eq!(channel, "cpa");
            assert_eq!(var, "BRISK_CPA_KEY");
        }
        Ok(_) if std::env::var_os("BRISK_CPA_KEY").is_some() => {}
        other => panic!("unexpected result: {other:?}"),
    }

    let path = repo_config("brisk.example.toml");
    let text = std::fs::read_to_string(&path).unwrap();
    let config = load_str(
        &text,
        path.parent().unwrap(),
        "brisk.example.toml",
        &env_with(&[("BRISK_CPA_KEY", "cpa-key")]),
    )
    .unwrap();
    let spec = &config.spec;
    // The example writes every default explicitly; it must equal the
    // built-in defaults, so the two cannot drift apart.
    let defaults = load_ok(&minimal());
    assert_eq!(spec.limits, defaults.spec.limits);
    assert_eq!(spec.forwarding, defaults.spec.forwarding);
    assert_eq!(spec.warmup, defaults.spec.warmup);
    assert_eq!(spec.experiments, defaults.spec.experiments);
    assert_eq!(config.outcome_queue, defaults.outcome_queue);
    assert_eq!(
        spec.server.header_read_timeout,
        defaults.spec.server.header_read_timeout
    );
    assert_eq!(spec.server.backlog, defaults.spec.server.backlog);
    let channel = &spec.channels[0];
    assert_eq!(channel.timeouts, defaults.spec.channels[0].timeouts);
    assert_eq!(channel.client.connect_timeout, Duration::from_secs(5));
    assert!(channel.model_map.is_empty());
    assert!(!channel.expose_ratelimit_headers);
}

#[test]
fn defaults_match_the_specification() {
    let config = load_ok(&minimal());
    assert_eq!(config.workers, None);
    assert_eq!(config.tls, None);
    assert_eq!(config.outcome_queue, 16_384);
    let spec = &config.spec;
    assert_eq!(spec.limits, LimitsSpec::default());
    assert_eq!(spec.forwarding, ForwardingSpec::default());
    assert_eq!(spec.warmup, WarmupSpec::default());
    assert_eq!(spec.experiments.router, RouterKind::Axum);
    assert_eq!(spec.experiments.splice, SpliceMode::Segments);
    assert_eq!(spec.server.header_read_timeout, Duration::from_secs(30));
    assert_eq!(spec.server.tls_handshake_timeout, Duration::from_secs(10));
    assert_eq!(
        spec.server.graceful_shutdown_timeout,
        Duration::from_secs(30)
    );
    assert_eq!(spec.server.tcp_keepalive, Duration::from_secs(30));
    assert_eq!(spec.server.backlog, 4096);

    let channel = &spec.channels[0];
    assert_eq!(channel.weight, 1);
    assert!(!channel.client.allow_private);
    assert_eq!(channel.stream_usage, StreamUsage::Inject);
    assert!(channel.models.is_empty());
    assert!(channel.model_map.is_empty());
    assert_eq!(channel.warmup, WarmupTarget::default());
    assert_eq!(channel.timeouts, Timeouts::default());
    assert_eq!(channel.client, UpstreamClientConfig::default());
    assert!(!channel.expose_ratelimit_headers);
}

/// Every global setting changed, one channel inheriting them and one
/// overriding every per-channel field.
const OVERRIDES: &str = r#"
[server]
listen = "127.0.0.1:18080"
workers = 3
max_connections = 1000
header_read_timeout = "5s"
backlog = 128

[limits]
max_body = "1MiB"
inflight_body_budget = 8388608
body_read_timeout = "2m"
body_min_rate_bytes = "1KiB"
body_min_rate_window = "500ms"
response_tap_budget = "1GiB"

[forwarding]
max_attempts = 5
commit_hold = "0s"
first_byte_timeout = "1h"
idle_timeout = "30s"
drain_timeout = "1s"
drain_max_bytes = "16KiB"
outcome_queue = 100

[upstream]
connect_timeout = "2s"
pool_idle_timeout = "10s"
tcp_keepalive = "15s"
tcp_user_timeout = "20s"
warmup_interval = "5m"
ready_timeout = "1s"
warmup_request_timeout = "4s"

[experiments]
router = "match"
splice = "concat"

[[keys]]
name = "a"
sha256 = "0000000000000000000000000000000000000000000000000000000000000000"

[[channels]]
name = "global"
base_url = "http://127.0.0.1:9/v1"
api_key = { env = "BRISK_CPA_KEY" }

[[channels]]
name = "override"
base_url = "http://127.0.0.1:9/v1"
api_key = { env = "BRISK_CPA_KEY" }
weight = 7
allow_private = true
stream_usage = "passthrough"
models = ["grok-4.6(xhigh)", "other"]
model_map = { "grok-4.6(xhigh)" = "grok-4.6", other = "other-upstream" }
warmup = { method = "GET", path = "/v1/models" }
commit_hold = "250ms"
first_byte_timeout = "10s"
idle_timeout = "3s"
connect_timeout = "1s"
pool_idle_timeout = "7s"
expose_ratelimit_headers = true
"#;

#[test]
fn globals_apply_to_channels() {
    let config = load_ok(OVERRIDES);
    assert_eq!(config.workers.map(std::num::NonZero::get), Some(3));
    assert_eq!(config.outcome_queue, 100);
    let spec = &config.spec;
    assert_eq!(spec.server.max_connections, 1000);
    assert_eq!(spec.server.header_read_timeout, Duration::from_secs(5));
    assert_eq!(spec.server.backlog, 128);
    assert_eq!(
        spec.limits,
        LimitsSpec {
            max_body: 1 << 20,
            inflight_body_budget: 8 << 20,
            body_read_timeout: Duration::from_secs(120),
            body_min_rate_bytes: 1024,
            body_min_rate_window: Duration::from_millis(500),
            response_tap_budget: 1 << 30,
        }
    );
    assert_eq!(
        spec.forwarding,
        ForwardingSpec {
            max_attempts: 5,
            drain_timeout: Duration::from_secs(1),
            drain_max_bytes: 16 << 10,
        }
    );
    assert_eq!(
        spec.warmup,
        WarmupSpec {
            interval: Duration::from_secs(300),
            ready_timeout: Duration::from_secs(1),
            request_timeout: Duration::from_secs(4),
        }
    );
    assert_eq!(spec.experiments.router, RouterKind::Match);
    assert_eq!(spec.experiments.splice, SpliceMode::Concat);

    let global = &spec.channels[0];
    assert_eq!(
        global.timeouts,
        Timeouts {
            commit_hold: Duration::ZERO,
            first_byte: Duration::from_secs(3600),
            idle: Duration::from_secs(30),
        }
    );
    assert_eq!(
        global.client,
        UpstreamClientConfig {
            connect_timeout: Duration::from_secs(2),
            pool_idle_timeout: Duration::from_secs(10),
            tcp_keepalive: Duration::from_secs(15),
            tcp_user_timeout: Duration::from_secs(20),
            extra_root_certs: Vec::new(),
            allow_private: false,
        }
    );
}

#[test]
fn channels_override_globals() {
    let config = load_ok(OVERRIDES);
    let spec = &config.spec;
    let channel = &spec.channels[1];
    assert_eq!(channel.weight, 7);
    assert_eq!(channel.stream_usage, StreamUsage::Passthrough);
    assert_eq!(channel.models, [TEST_MODEL, "other"]);
    let mut model_map = channel.model_map.clone();
    model_map.sort();
    assert_eq!(
        model_map,
        [
            (TEST_MODEL.to_owned(), "grok-4.6".to_owned()),
            ("other".to_owned(), "other-upstream".to_owned()),
        ]
    );
    assert_eq!(
        channel.warmup,
        WarmupTarget {
            method: WarmupMethod::Get,
            path: "/v1/models".to_owned(),
        }
    );
    assert_eq!(
        channel.timeouts,
        Timeouts {
            commit_hold: Duration::from_millis(250),
            first_byte: Duration::from_secs(10),
            idle: Duration::from_secs(3),
        }
    );
    assert_eq!(
        channel.client,
        UpstreamClientConfig {
            connect_timeout: Duration::from_secs(1),
            pool_idle_timeout: Duration::from_secs(7),
            tcp_keepalive: Duration::from_secs(15),
            tcp_user_timeout: Duration::from_secs(20),
            extra_root_certs: Vec::new(),
            allow_private: true,
        }
    );
    assert!(channel.expose_ratelimit_headers);
}

#[test]
fn bracketed_model_names_pass_through_untouched() {
    let config = load_ok(&with_channel(&format!(
        r#"api_key = {{ env = "BRISK_CPA_KEY" }}
models = ["{TEST_MODEL}"]
model_map = {{ "{TEST_MODEL}" = "{TEST_MODEL}" }}"#
    )));
    let channel = &config.spec.channels[0];
    assert_eq!(channel.models, [TEST_MODEL]);
    assert_eq!(
        channel.model_map,
        [(TEST_MODEL.to_owned(), TEST_MODEL.to_owned())]
    );
}

#[test]
fn relative_paths_resolve_against_the_config_directory() {
    let dir = TempDir::new("relative");
    std::fs::create_dir_all(dir.path().join("secrets")).unwrap();
    std::fs::write(dir.path().join("secrets/cpa.key"), "file-key\r\n\n").unwrap();
    std::fs::write(dir.path().join("tls.key"), "not read at load").unwrap();
    std::fs::write(dir.path().join("ca.pem"), CA_PEM).unwrap();
    let text = format!(
        r#"
[server]
listen = "0.0.0.0:443"

[server.tls]
cert = "tls.crt"
key = "tls.key"

[[keys]]
name = "a"
sha256 = "{DIGEST}"

[[channels]]
name = "file"
base_url = "https://127.0.0.1:19443/v1"
api_key = {{ file = "secrets/cpa.key" }}
ca_file = "ca.pem"
"#
    );
    let config = load_str(&text, dir.path(), "brisk.toml", &no_env).unwrap();
    let tls = config.tls.unwrap();
    assert_eq!(tls.cert, dir.path().join("tls.crt"));
    assert_eq!(tls.key, dir.path().join("tls.key"));
    let channel = &config.spec.channels[0];
    // Trailing newlines of the key file are dropped.
    assert_eq!(channel.api_key.expose(), "file-key");
    assert_eq!(channel.client.extra_root_certs.len(), 1);
}

#[test]
fn durations_accept_every_unit_and_reject_other_forms() {
    for (text, expected) in [
        ("1500ms", Duration::from_millis(1500)),
        ("45s", Duration::from_secs(45)),
        ("3m", Duration::from_secs(180)),
        ("2h", Duration::from_secs(7200)),
    ] {
        let config = load_ok(&format!(
            "[forwarding]\nidle_timeout = \"{text}\"\n{}",
            minimal()
        ));
        assert_eq!(config.spec.channels[0].timeouts.idle, expected, "{text}");
    }
    for bad in [
        "\"1.5s\"", "\"10\"", "\"10 s\"", "\"10d\"", "\"-1s\"", "10", "1.5", "\"\"",
    ] {
        let err = load_err(&format!(
            "[forwarding]\nidle_timeout = {bad}\n{}",
            minimal()
        ));
        assert!(
            matches!(err, ConfigError::Parse { line: Some(2), .. }),
            "{bad}: {err:?}"
        );
    }
}

#[test]
fn zero_durations_are_rejected_except_commit_hold() {
    let config = load_ok(&format!(
        "[forwarding]\ncommit_hold = \"0s\"\n{}",
        minimal()
    ));
    assert_eq!(config.spec.channels[0].timeouts.commit_hold, Duration::ZERO);
    let config = load_ok(&with_channel(
        "api_key = { env = \"BRISK_CPA_KEY\" }\ncommit_hold = \"0ms\"",
    ));
    assert_eq!(config.spec.channels[0].timeouts.commit_hold, Duration::ZERO);

    for (table, field) in [
        ("forwarding", "idle_timeout"),
        ("forwarding", "first_byte_timeout"),
        ("forwarding", "drain_timeout"),
        ("server", "header_read_timeout"),
        ("limits", "body_read_timeout"),
        ("upstream", "warmup_interval"),
    ] {
        let text = if table == "server" {
            minimal().replacen("[server]\n", &format!("[server]\n{field} = \"0s\"\n"), 1)
        } else {
            format!("[{table}]\n{field} = \"0s\"\n{}", minimal())
        };
        match load_err(&text) {
            ConfigError::Invalid { field: got, .. } => {
                assert_eq!(got, format!("{table}.{field}"));
            }
            other => panic!("{field}: {other:?}"),
        }
    }
    match load_err(&with_channel(
        "api_key = { env = \"BRISK_CPA_KEY\" }\nidle_timeout = \"0s\"",
    )) {
        ConfigError::Invalid { field, .. } => assert_eq!(field, "channels[\"cpa\"].idle_timeout"),
        other => panic!("{other:?}"),
    }
}

#[test]
fn sizes_accept_units_and_bare_bytes() {
    for (text, expected) in [
        ("1048576", 1 << 20),
        ("\"1048576\"", 1 << 20),
        ("\"512B\"", 512),
        ("\"2KiB\"", 2048),
        ("\"3MiB\"", 3 << 20),
        ("\"1GiB\"", 1 << 30),
    ] {
        let config = load_ok(&format!("[limits]\nmax_body = {text}\n{}", minimal()));
        assert_eq!(config.spec.limits.max_body, expected, "{text}");
    }
    for bad in ["\"1.5MiB\"", "1.5", "-1", "\"1MB\"", "\"1 MiB\"", "\"MiB\""] {
        let err = load_err(&format!("[limits]\nmax_body = {bad}\n{}", minimal()));
        assert!(matches!(err, ConfigError::Parse { .. }), "{bad}: {err:?}");
    }
    for bad in ["0", "\"4GiB\""] {
        let err = load_err(&format!("[limits]\nmax_body = {bad}\n{}", minimal()));
        assert!(
            matches!(&err, ConfigError::Invalid { field, .. } if field == "limits.max_body"),
            "{bad}: {err:?}"
        );
    }
    let config = load_ok(&format!("[limits]\nmax_body = 4294967295\n{}", minimal()));
    assert_eq!(config.spec.limits.max_body, u32::MAX);
}

#[test]
fn ranges_are_checked() {
    for (text, field) in [
        ("[forwarding]\nmax_attempts = 0", "forwarding.max_attempts"),
        ("[forwarding]\nmax_attempts = 9", "forwarding.max_attempts"),
        (
            "[forwarding]\noutcome_queue = 0",
            "forwarding.outcome_queue",
        ),
        (
            "[forwarding]\noutcome_queue = 16777217",
            "forwarding.outcome_queue",
        ),
    ] {
        match load_err(&format!("{text}\n{}", minimal())) {
            ConfigError::Invalid { field: got, .. } => assert_eq!(got, field),
            other => panic!("{text}: {other:?}"),
        }
    }
    for (line, field) in [
        ("workers = 0", "server.workers"),
        ("max_connections = 0", "server.max_connections"),
        ("backlog = 0", "server.backlog"),
        ("backlog = 2147483648", "server.backlog"),
    ] {
        let text = minimal().replacen("[server]\n", &format!("[server]\n{line}\n"), 1);
        match load_err(&text) {
            ConfigError::Invalid { field: got, .. } => assert_eq!(got, field),
            other => panic!("{line}: {other:?}"),
        }
    }
    for weight in ["0", "-1", "4294967296"] {
        match load_err(&with_channel(&format!(
            "api_key = {{ env = \"BRISK_CPA_KEY\" }}\nweight = {weight}"
        ))) {
            ConfigError::Invalid { field, .. } => assert_eq!(field, "channels[\"cpa\"].weight"),
            other => panic!("{weight}: {other:?}"),
        }
    }
}

#[test]
fn key_digests_must_be_lowercase_hex() {
    for bad in [
        DIGEST.to_uppercase().replace('0', "A"),
        "abc".to_owned(),
        "g".repeat(64),
    ] {
        let text = minimal().replace(DIGEST, &bad);
        match load_err(&text) {
            ConfigError::Invalid { field, .. } => assert_eq!(field, "keys[\"local-dev\"].sha256"),
            other => panic!("{bad}: {other:?}"),
        }
    }
    let digest = "0123456789abcdef".repeat(4);
    let config = load_ok(&minimal().replace(DIGEST, &digest));
    assert_eq!(config.spec.keys[0].sha256[..2], [0x01, 0x23]);
}

#[test]
fn names_must_not_be_empty() {
    match load_err(&minimal().replace("name = \"cpa\"", "name = \"\"")) {
        ConfigError::Invalid { field, .. } => assert_eq!(field, "channels[0].name"),
        other => panic!("{other:?}"),
    }
    match load_err(&minimal().replace("name = \"local-dev\"", "name = \"\"")) {
        ConfigError::Invalid { field, .. } => assert_eq!(field, "keys[0].name"),
        other => panic!("{other:?}"),
    }
}

#[test]
fn unknown_fields_are_rejected_everywhere() {
    for (text, typo) in [
        (
            minimal().replacen("[server]\n", "[server]\nlisten_addr = 1\n", 1),
            "listen_addr",
        ),
        (format!("[limits]\nmax_bdy = 1\n{}", minimal()), "max_bdy"),
        (
            format!("[forwarding]\nretries = 1\n{}", minimal()),
            "retries",
        ),
        (
            format!("[upstream]\ntimeout = \"1s\"\n{}", minimal()),
            "timeout",
        ),
        (
            format!("[experiments]\nrouters = \"axum\"\n{}", minimal()),
            "routers",
        ),
        (format!("[extra]\nx = 1\n{}", minimal()), "extra"),
        (minimal().replace("sha256", "sha265"), "sha265"),
        (
            with_channel("api_key = { env = \"BRISK_CPA_KEY\" }\nwieght = 2"),
            "wieght",
        ),
        (
            with_channel("api_key = { env = \"BRISK_CPA_KEY\" }\nwarmup = { verb = \"GET\" }"),
            "verb",
        ),
    ] {
        match load_err(&text) {
            ConfigError::Parse { message, line, .. } => {
                assert!(message.contains(typo), "{typo}: {message}");
                assert!(line.is_some(), "{typo}: no location");
            }
            other => panic!("{typo}: {other:?}"),
        }
    }
}

#[test]
fn enums_accept_only_documented_values() {
    for (line, expected) in [
        ("stream_usage = \"inject\"", StreamUsage::Inject),
        ("stream_usage = \"passthrough\"", StreamUsage::Passthrough),
    ] {
        let config = load_ok(&with_channel(&format!(
            "api_key = {{ env = \"BRISK_CPA_KEY\" }}\n{line}"
        )));
        assert_eq!(config.spec.channels[0].stream_usage, expected);
    }
    for bad in [
        "stream_usage = \"Inject\"",
        "warmup = { method = \"POST\" }",
        "warmup = { method = \"get\" }",
    ] {
        let err = load_err(&with_channel(&format!(
            "api_key = {{ env = \"BRISK_CPA_KEY\" }}\n{bad}"
        )));
        assert!(matches!(err, ConfigError::Parse { .. }), "{bad}: {err:?}");
    }
}

#[test]
fn parse_errors_report_line_and_column() {
    let text = "[server]\nlisten = \"127.0.0.1:1\"\nworkers = \"four\"\n";
    match load_err(text) {
        ConfigError::Parse {
            line,
            column,
            message,
            path,
        } => {
            assert_eq!(line, Some(3));
            assert_eq!(column, Some(11));
            assert!(message.contains("invalid type"), "{message}");
            assert_eq!(path, Path::new("/etc/brisk").join("brisk.toml"));
        }
        other => panic!("{other:?}"),
    }
    let err = load_err("[server\n");
    assert!(
        matches!(err, ConfigError::Parse { line: Some(1), .. }),
        "{err:?}"
    );
}

#[test]
fn missing_sections_are_parse_errors() {
    for text in [
        "[server]\nlisten = \"127.0.0.1:1\"\n",
        &format!("[[keys]]\nname = \"a\"\nsha256 = \"{DIGEST}\"\n"),
    ] {
        let err = load_err(text);
        assert!(
            matches!(&err, ConfigError::Parse { message, .. } if message.contains("missing field")),
            "{err:?}"
        );
    }
}

#[test]
fn missing_env_names_the_variable_not_a_value() {
    let err = load_str(&minimal(), Path::new("."), "brisk.toml", &no_env).unwrap_err();
    match &err {
        ConfigError::MissingEnv { channel, var } => {
            assert_eq!(channel, "cpa");
            assert_eq!(var, "BRISK_CPA_KEY");
        }
        other => panic!("{other:?}"),
    }
    assert!(err.to_string().contains("BRISK_CPA_KEY"), "{err}");

    let empty = load_str(
        &minimal(),
        Path::new("."),
        "brisk.toml",
        &env_with(&[("BRISK_CPA_KEY", "")]),
    )
    .unwrap_err();
    assert!(
        matches!(&empty, ConfigError::Invalid { field, .. } if field == "channels[\"cpa\"].api_key"),
        "{empty:?}"
    );
}

#[test]
fn secret_file_errors_name_the_path() {
    let dir = TempDir::new("secret-file");
    let err = load_str(
        &with_channel("api_key = { file = \"absent.key\" }"),
        dir.path(),
        "brisk.toml",
        &no_env,
    )
    .unwrap_err();
    match &err {
        ConfigError::SecretFile { channel, path, .. } => {
            assert_eq!(channel, "cpa");
            assert_eq!(path, &dir.path().join("absent.key"));
        }
        other => panic!("{other:?}"),
    }

    std::fs::write(dir.path().join("binary.key"), [0xff, 0xfe, 0x00]).unwrap();
    let err = load_str(
        &with_channel("api_key = { file = \"binary.key\" }"),
        dir.path(),
        "brisk.toml",
        &no_env,
    )
    .unwrap_err();
    assert!(matches!(err, ConfigError::SecretFile { .. }), "{err:?}");
}

#[test]
fn inline_values_only_in_local_files() {
    let text = with_channel(&format!("api_key = {{ value = \"{LEAK}\" }}"));
    let config = load_text(&text, "cpa.local.toml").unwrap();
    assert_eq!(config.spec.channels[0].api_key.expose(), LEAK);
    assert!(!format!("{config:?}").contains(LEAK));

    for file_name in [
        "brisk.toml",
        "cpa.local.toml.bak",
        "local.toml",
        "cpa.local.TOML",
    ] {
        match load_text(&text, file_name) {
            Err(ConfigError::InlineSecretNotAllowed { channel }) => assert_eq!(channel, "cpa"),
            other => panic!("{file_name}: {other:?}"),
        }
        assert_no_leak(&text, file_name);
    }
}

#[test]
fn errors_never_contain_the_secret() {
    let cases = [
        format!("api_key = \"{LEAK}\""),
        format!("api_key = {{ value = \"{LEAK}\", env = \"X\" }}"),
        format!("api_key = {{ env = \"X\", value = \"{LEAK}\" }}"),
        format!("api_key = [\"{LEAK}\"]"),
        format!("api_key = {{ secret = \"{LEAK}\" }}"),
        format!("api_key = {{ value = {{ inner = \"{LEAK}\" }} }}"),
        format!("api_key = {{ value = [\"{LEAK}\"] }}"),
        "api_key = { value = 123 }".to_owned(),
        "api_key = {}".to_owned(),
        format!("api_key = {{ env = \"BRISK_CPA_KEY\" }}\nweight = \"{LEAK}\""),
        format!("api_key = {{ env = \"BRISK_CPA_KEY\" }}\nstream_usage = \"{LEAK}\""),
        format!("api_key = {{ env = \"BRISK_CPA_KEY\" }}\nidle_timeout = \"{LEAK}\""),
        format!("api_key = {{ env = \"BRISK_CPA_KEY\" }}\nmodels = \"{LEAK}\""),
        format!("api_key = {{ env = \"BRISK_CPA_KEY\" }}\nallow_private = \"{LEAK}\""),
        format!("api_key = {LEAK}"),
        format!("api_key = \"{LEAK}"),
        format!("api_key = '{LEAK}'"),
        format!("api_key = \"\"\"{LEAK}\"\"\""),
    ];
    for channel in &cases {
        for file_name in ["brisk.toml", "cpa.local.toml"] {
            let rendered = assert_no_leak(&with_channel(channel), file_name);
            assert!(!rendered.is_empty());
        }
    }

    // The fixed text replaces serde's type error for the common mistake.
    let rendered = assert_no_leak(&with_channel(&cases[0]), "brisk.toml");
    assert!(
        rendered.contains("api_key must be a table with exactly one of env, file, value"),
        "{rendered}"
    );
    // A mistyped weight still says what was wrong, without the value.
    let rendered = assert_no_leak(&with_channel(&cases[9]), "brisk.toml");
    assert!(rendered.contains("invalid type: string"), "{rendered}");
}

#[test]
fn parse_errors_have_no_source_that_could_print_the_line() {
    use std::error::Error as _;

    let err = load_err(&with_channel(&format!("api_key = \"{LEAK}\"")));
    assert!(matches!(err, ConfigError::Parse { .. }));
    assert!(err.source().is_none());
}

#[test]
fn plaintext_listen_needs_loopback_tls_or_explicit_opt_in() {
    let wildcard = minimal().replace("127.0.0.1:18080", "0.0.0.0:18080");
    match load_err(&wildcard) {
        ConfigError::PlaintextListen { listen } => {
            assert_eq!(listen, "0.0.0.0:18080".parse::<SocketAddr>().unwrap());
        }
        other => panic!("{other:?}"),
    }
    let lan = minimal().replace("127.0.0.1:18080", "192.168.10.5:18080");
    assert!(matches!(
        load_err(&lan),
        ConfigError::PlaintextListen { .. }
    ));

    let opted_in = wildcard.replacen("[server]\n", "[server]\nallow_plaintext = true\n", 1);
    load_ok(&opted_in);
    load_ok(&minimal().replace("127.0.0.1:18080", "[::1]:18080"));
    load_ok(&minimal().replace("127.0.0.1:18080", "127.8.9.10:18080"));

    let dir = TempDir::new("plaintext");
    std::fs::write(dir.path().join("tls.key"), "key").unwrap();
    let with_tls = wildcard.replacen(
        "[[keys]]",
        "[server.tls]\ncert = \"tls.crt\"\nkey = \"tls.key\"\n\n[[keys]]",
        1,
    );
    let config = load_str(
        &with_tls,
        dir.path(),
        "brisk.toml",
        &env_with(&[("BRISK_CPA_KEY", "k")]),
    )
    .unwrap();
    assert!(config.tls.is_some());
}

#[test]
fn a_missing_tls_key_fails_at_load() {
    let dir = TempDir::new("tls-missing");
    let text = minimal().replacen(
        "[[keys]]",
        "[server.tls]\ncert = \"tls.crt\"\nkey = \"tls.key\"\n\n[[keys]]",
        1,
    );
    let err = load_str(
        &text,
        dir.path(),
        "brisk.toml",
        &env_with(&[("BRISK_CPA_KEY", "k")]),
    )
    .unwrap_err();
    match err {
        ConfigError::Io { path, .. } => assert_eq!(path, dir.path().join("tls.key")),
        other => panic!("{other:?}"),
    }
}

#[test]
fn bad_ca_files_are_reported() {
    let dir = TempDir::new("ca");
    std::fs::write(dir.path().join("empty.pem"), "").unwrap();
    for file in ["empty.pem", "absent.pem"] {
        let err = load_str(
            &with_channel(&format!(
                "api_key = {{ env = \"K\" }}\nca_file = \"{file}\""
            )),
            dir.path(),
            "brisk.toml",
            &env_with(&[("K", "k")]),
        )
        .unwrap_err();
        match err {
            ConfigError::CaFile { channel, path, .. } => {
                assert_eq!(channel, "cpa");
                assert_eq!(path, dir.path().join(file));
            }
            other => panic!("{file}: {other:?}"),
        }
    }
}

#[test]
fn load_reads_the_file_and_names_it_in_errors() {
    let dir = TempDir::new("load");
    let path = dir.path().join("cpa.local.toml");
    std::fs::write(
        &path,
        with_channel(&format!("api_key = {{ value = \"{LEAK}\" }}")),
    )
    .unwrap();
    let config = load(&path).unwrap();
    assert_eq!(config.spec.channels[0].api_key.expose(), LEAK);

    let absent = dir.path().join("absent.toml");
    match load(&absent).unwrap_err() {
        ConfigError::Io { path, .. } => assert_eq!(path, absent),
        other => panic!("{other:?}"),
    }

    let broken = dir.path().join("broken.toml");
    std::fs::write(&broken, "[server\n").unwrap();
    match load(&broken).unwrap_err() {
        ConfigError::Parse { path, .. } => assert_eq!(path, broken),
        other => panic!("{other:?}"),
    }
}

#[cfg(unix)]
mod unix_permissions {
    use std::os::unix::fs::PermissionsExt as _;

    use super::*;

    fn chmod(path: &Path, mode: u32) {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
    }

    #[test]
    fn world_writable_config_fails() {
        let dir = TempDir::new("perm-config");
        let path = dir.path().join("brisk.toml");
        std::fs::write(&path, minimal()).unwrap();
        chmod(&path, 0o602);
        // `load` reads the process environment, which lacks the key; the
        // permission check must not depend on that, so use a file source.
        std::fs::write(dir.path().join("cpa.key"), "k").unwrap();
        chmod(&dir.path().join("cpa.key"), 0o600);
        std::fs::write(&path, with_channel("api_key = { file = \"cpa.key\" }")).unwrap();
        match load(&path).unwrap_err() {
            ConfigError::InsecurePermissions { path: got, mode } => {
                assert_eq!(got, path);
                assert_eq!(mode, 0o602);
            }
            other => panic!("{other:?}"),
        }
        chmod(&path, 0o644);
        let config = load(&path).unwrap();
        // No inline secret: a world-readable config is fine.
        assert!(config.warnings.is_empty(), "{:?}", config.warnings);
    }

    #[test]
    fn world_readable_config_with_inline_secret_warns() {
        let dir = TempDir::new("perm-inline");
        let path = dir.path().join("cpa.local.toml");
        std::fs::write(
            &path,
            with_channel(&format!("api_key = {{ value = \"{LEAK}\" }}")),
        )
        .unwrap();
        chmod(&path, 0o644);
        let config = load(&path).unwrap();
        assert_eq!(config.warnings.len(), 1, "{:?}", config.warnings);
        assert!(!config.warnings[0].contains(LEAK));
        chmod(&path, 0o600);
        assert!(load(&path).unwrap().warnings.is_empty());
    }

    #[test]
    fn secret_file_permissions() {
        let dir = TempDir::new("perm-secret");
        let key = dir.path().join("cpa.key");
        std::fs::write(&key, "k").unwrap();
        let text = with_channel("api_key = { file = \"cpa.key\" }");

        chmod(&key, 0o666);
        match load_str(&text, dir.path(), "brisk.toml", &no_env).unwrap_err() {
            ConfigError::InsecurePermissions { path, mode } => {
                assert_eq!(path, key);
                assert_eq!(mode, 0o666);
            }
            other => panic!("{other:?}"),
        }
        chmod(&key, 0o644);
        let config = load_str(&text, dir.path(), "brisk.toml", &no_env).unwrap();
        assert_eq!(config.warnings.len(), 1, "{:?}", config.warnings);
        chmod(&key, 0o640);
        let config = load_str(&text, dir.path(), "brisk.toml", &no_env).unwrap();
        assert!(config.warnings.is_empty(), "{:?}", config.warnings);
    }

    #[test]
    fn tls_key_permissions() {
        let dir = TempDir::new("perm-tls");
        let key = dir.path().join("tls.key");
        std::fs::write(&key, "key").unwrap();
        let text = minimal().replacen(
            "[[keys]]",
            "[server.tls]\ncert = \"tls.crt\"\nkey = \"tls.key\"\n\n[[keys]]",
            1,
        );
        let env = env_with(&[("BRISK_CPA_KEY", "k")]);

        chmod(&key, 0o602);
        assert!(matches!(
            load_str(&text, dir.path(), "brisk.toml", &env).unwrap_err(),
            ConfigError::InsecurePermissions { mode: 0o602, .. }
        ));
        chmod(&key, 0o604);
        let config = load_str(&text, dir.path(), "brisk.toml", &env).unwrap();
        assert_eq!(config.warnings.len(), 1, "{:?}", config.warnings);
        chmod(&key, 0o600);
        let config = load_str(&text, dir.path(), "brisk.toml", &env).unwrap();
        assert!(config.warnings.is_empty(), "{:?}", config.warnings);
    }
}

/// A directory under the system temp dir, removed on drop.
struct TempDir(PathBuf);

impl TempDir {
    fn new(test: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("brisk-config-{test}-{}", std::process::id()));
        // A leftover from an aborted run would make assertions on file
        // contents meaningless.
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Self-signed test certificate used as an upstream trust anchor.
const CA_PEM: &str = "-----BEGIN CERTIFICATE-----
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
