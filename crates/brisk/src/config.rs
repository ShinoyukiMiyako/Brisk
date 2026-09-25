//! TOML configuration: parsing, secret resolution, file checks and default
//! merging into a [`GatewaySpec`].
//!
//! The loader owns syntax, types, value ranges, secrets, file permissions,
//! the plaintext-listen rule and the merge of global defaults with channel
//! overrides. Cross-field semantics (unique names, base URL policy, digest
//! uniqueness, budget against `max_body`) belong to `Gateway::new`, which the
//! M2 snapshot compiler will reuse.
//!
//! # Secrets never reach an error message
//!
//! - Upstream keys come from `{ env = "NAME" }`, `{ file = "path" }` or
//!   `{ value = "..." }`; `value` is accepted only in a file whose name ends
//!   with `.local.toml`, so a committed file cannot carry one.
//! - A TOML error is reported as its message plus a line and column computed
//!   from its span. Its `Display` quotes the offending source line, which may
//!   be the one holding the key, so it is never used, and no [`ConfigError`]
//!   holds a toml or serde error as its source.
//! - serde embeds string values in type errors (`invalid type: string
//!   "sk-..."`) and unknown enum variants in variant errors; both are masked
//!   before the message is stored.
//! - `api_key` has a hand-written deserializer whose every error is fixed
//!   text, and the inline value is wrapped in [`Redacted`] as soon as it is
//!   read.
//!
//! # File permissions
//!
//! On Unix, the configuration file, every `api_key.file` and the inbound TLS
//! private key must not be writable by other users: whoever can write them can
//! add virtual keys or redirect a channel and read its prompts. Such a file
//! fails the load with [`ConfigError::InsecurePermissions`]. The same files are
//! only warned about when other users can read them and they hold a secret;
//! group-readable files and the owner are not checked, since root-owned
//! service-group files and Docker secrets (mode 0444) are common deployments.
//! Windows ACLs are not inspected.

use std::fmt;
use std::fs::File;
use std::io::{self, Read as _};
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::time::Duration;

use brisk_gateway::secret::Redacted;
use brisk_gateway::server::ServerConfig;
use brisk_gateway::spec::{
    ChannelSpec, ExperimentSpec, ForwardingSpec, GatewaySpec, KeySpec, LimitsSpec, RouterKind,
    SpliceMode, StreamUsage, Timeouts, WarmupMethod, WarmupSpec, WarmupTarget,
};
use brisk_gateway::upstream::UpstreamClientConfig;
use rustls_pki_types::CertificateDer;
use rustls_pki_types::pem::{self, PemObject as _};
use serde::Deserialize;
use serde::de::{self, Deserializer, EnumAccess, MapAccess, SeqAccess, Visitor};

/// Default capacity of the outcome queue (02, `LogRecord` channel).
const DEFAULT_OUTCOME_QUEUE: usize = 16_384;

/// Largest accepted outcome queue. The queue allocates lazily, but a typo such
/// as an extra digit group would otherwise let a burst hold gigabytes of
/// records before the logger drains them.
const MAX_OUTCOME_QUEUE: usize = 1 << 24;

/// Largest accepted `server.max_connections`: `server::serve` sizes a Tokio
/// semaphore with it and refuses to start above that semaphore's limit, so
/// the loader rejects such a value before `check-config` can pass it.
const MAX_CONNECTIONS: usize = tokio::sync::Semaphore::MAX_PERMITS;

/// The range of [`MAX_CONNECTIONS`] as error text; `ConfigError::Invalid`
/// takes a fixed string, and a unit test keeps the two in step.
#[cfg(target_pointer_width = "64")]
const MAX_CONNECTIONS_RANGE: &str = "must be in 1..=2305843009213693951";
#[cfg(target_pointer_width = "32")]
const MAX_CONNECTIONS_RANGE: &str = "must be in 1..=536870911";

/// Suffix of the only file names allowed to hold inline secrets.
const LOCAL_SUFFIX: &str = ".local.toml";

/// A loaded configuration, ready for `Gateway::new` and the runtime.
#[derive(Debug)]
pub struct LoadedConfig {
    /// Address the data plane listens on.
    pub listen: SocketAddr,
    /// Everything the gateway is built from.
    pub spec: GatewaySpec,
    /// Tokio worker threads; `None` means the available parallelism.
    pub workers: Option<NonZeroUsize>,
    /// Inbound TLS material; `None` serves plaintext.
    pub tls: Option<TlsFiles>,
    /// Capacity of the outcome queue.
    pub outcome_queue: usize,
    /// Warnings found while loading (e.g. a secret file readable by others),
    /// logged by `run` once tracing is up.
    pub warnings: Vec<String>,
}

/// Inbound TLS certificate chain and private key, resolved against the
/// configuration directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsFiles {
    /// PEM certificate chain, leaf first.
    pub cert: PathBuf,
    /// PEM private key.
    pub key: PathBuf,
}

/// Why a configuration could not be loaded.
///
/// No variant carries a secret, a TOML source line or a toml or serde error
/// value, so both `Display` and an `anyhow` chain are safe to print.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// A file could not be opened or read.
    #[error("cannot read {path}")]
    Io {
        /// The file.
        path: PathBuf,
        /// The underlying error.
        source: io::Error,
    },
    /// The TOML is malformed or does not match the schema.
    #[error("{path}{}: {message}", LineColumn(*.line, *.column))]
    Parse {
        /// The configuration file.
        path: PathBuf,
        /// 1-based line of the error, when toml reported a location.
        line: Option<usize>,
        /// 1-based column (in characters) of the error, when toml reported a
        /// location.
        column: Option<usize>,
        /// toml's message with string values masked.
        message: String,
    },
    /// `api_key = { env = "..." }` names an unset variable.
    #[error("channel {channel:?}: environment variable {var} for api_key is not set")]
    MissingEnv {
        /// Channel name.
        channel: String,
        /// Variable name.
        var: String,
    },
    /// `api_key = { file = "..." }` could not be read or is not UTF-8.
    #[error("channel {channel:?}: cannot read api_key file {path}")]
    SecretFile {
        /// Channel name.
        channel: String,
        /// The resolved path.
        path: PathBuf,
        /// The underlying error.
        source: io::Error,
    },
    /// `api_key = { value = "..." }` in a file not named `*.local.toml`.
    #[error(
        "channel {channel:?}: api_key = {{ value = ... }} is only allowed in *{suffix} files; use env or file",
        suffix = LOCAL_SUFFIX
    )]
    InlineSecretNotAllowed {
        /// Channel name.
        channel: String,
    },
    /// A file that controls routing or holds a secret is writable by other
    /// users (Unix only).
    #[error("{path} is writable by other users (mode {mode:o}); remove o+w")]
    InsecurePermissions {
        /// The file.
        path: PathBuf,
        /// Its permission bits.
        mode: u32,
    },
    /// Plaintext listening on a non-loopback address without
    /// `server.allow_plaintext = true` (D28).
    #[error(
        "server.listen {listen} is not a loopback address; configure [server.tls] or set server.allow_plaintext = true"
    )]
    PlaintextListen {
        /// The configured address.
        listen: SocketAddr,
    },
    /// A channel's `ca_file` could not be read or holds no certificate.
    #[error("channel {channel:?}: cannot load CA certificates from {path}")]
    CaFile {
        /// Channel name.
        channel: String,
        /// The resolved path.
        path: PathBuf,
        /// The underlying error.
        source: pem::Error,
    },
    /// A value is out of range.
    #[error("{field}: {reason}")]
    Invalid {
        /// Dotted path of the field, e.g. `channels["cpa-a"].weight`.
        field: String,
        /// Fixed description of the rule; never contains the value.
        reason: &'static str,
    },
}

/// `:line:column` after the path of a [`ConfigError::Parse`], or nothing
/// unless toml reported both.
struct LineColumn(Option<usize>, Option<usize>);

impl fmt::Display for LineColumn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self(Some(line), Some(column)) => write!(f, ":{line}:{column}"),
            _ => Ok(()),
        }
    }
}

/// Reads the file, then [`load_str`].
///
/// Relative paths inside resolve against the file's directory, inline secrets
/// are allowed only when its name ends with `.local.toml`, and environment
/// variables come from the process (a variable that is not valid Unicode
/// counts as unset). On Unix the file itself must not be writable by other
/// users.
pub fn load(path: &Path) -> Result<LoadedConfig, ConfigError> {
    let io_error = |source| ConfigError::Io {
        path: path.to_path_buf(),
        source,
    };
    let mut file = File::open(path).map_err(io_error)?;
    let mut text = String::new();
    file.read_to_string(&mut text).map_err(io_error)?;

    let base_dir = path.parent().unwrap_or_else(|| Path::new(""));
    let file_name = path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    let env = |name: &str| std::env::var_os(name).and_then(|value| value.into_string().ok());
    let (mut config, has_inline_secret) = load_inner(&text, base_dir, &file_name, &env)?;
    check_permissions(&file, path, has_inline_secret, &mut config.warnings)?;
    Ok(config)
}

/// Parses and validates configuration text.
///
/// `file_name` decides whether inline secrets are allowed (`*.local.toml`);
/// `env` looks up environment variables (tests pass a map). Relative paths
/// resolve against `base_dir`.
pub fn load_str(
    text: &str,
    base_dir: &Path,
    file_name: &str,
    env: &dyn Fn(&str) -> Option<String>,
) -> Result<LoadedConfig, ConfigError> {
    load_inner(text, base_dir, file_name, env).map(|(config, _)| config)
}

/// [`load_str`], also reporting whether any channel used an inline secret, so
/// [`load`] knows whether a world-readable file leaks one.
fn load_inner(
    text: &str,
    base_dir: &Path,
    file_name: &str,
    env: &dyn Fn(&str) -> Option<String>,
) -> Result<(LoadedConfig, bool), ConfigError> {
    let raw: RawConfig = toml::from_str(text).map_err(|err| {
        let location = err.span().map(|span| line_column(text, span.start));
        ConfigError::Parse {
            path: base_dir.join(file_name),
            line: location.map(|(line, _)| line),
            column: location.map(|(_, column)| column),
            message: mask_values(err.message()),
        }
    })?;
    let mut builder = Builder {
        base_dir,
        inline_allowed: file_name.ends_with(LOCAL_SUFFIX),
        env,
        warnings: Vec::new(),
        has_inline_secret: false,
    };
    let config = builder.build(raw)?;
    Ok((config, builder.has_inline_secret))
}

/// 1-based line and character column of byte `offset` in `text`. Works for
/// any offset, including one inside a multi-byte character.
fn line_column(text: &str, offset: usize) -> (usize, usize) {
    let before = &text.as_bytes()[..offset.min(text.len())];
    let line_start = before
        .iter()
        .rposition(|&byte| byte == b'\n')
        .map_or(0, |newline| newline + 1);
    let line = before.split(|&byte| byte == b'\n').count();
    // Counting non-continuation bytes counts characters.
    let column = before[line_start..]
        .iter()
        .filter(|&&byte| byte & 0xC0 != 0x80)
        .count()
        + 1;
    (line, column)
}

/// Masks the values serde quotes in its messages: every double-quoted string
/// (`invalid type: string "..."`, written with `Debug` escapes) and the
/// variant in ``unknown variant `...` ``. Field names and expected values
/// stay, since they come from the schema.
fn mask_values(message: &str) -> String {
    const VARIANT: &str = "unknown variant `";
    let mut unquoted = String::with_capacity(message.len());
    let mut rest = message;
    while let Some(start) = rest.find(VARIANT) {
        let value_start = start + VARIANT.len();
        unquoted.push_str(&rest[..value_start]);
        unquoted.push_str("***");
        let tail = &rest[value_start..];
        // The client's text may itself contain backticks; the last terminator
        // serde writes is the real one because schema names never contain it.
        let end = ["`, expected ", "`, there are no variants"]
            .iter()
            .filter_map(|terminator| tail.rfind(terminator))
            .max();
        let Some(end) = end else {
            rest = "";
            break;
        };
        rest = &tail[end..];
    }
    unquoted.push_str(rest);

    let mut masked = String::with_capacity(unquoted.len());
    let mut chars = unquoted.chars().peekable();
    let mut previous = None;
    while let Some(c) = chars.next() {
        masked.push(c);
        let quotes_a_value = c == '"'
            // toml names the quote character itself as "expected `\"`".
            && !(previous == Some('`') && chars.peek() == Some(&'`'));
        previous = Some(c);
        if !quotes_a_value {
            continue;
        }
        masked.push_str("***");
        let mut escaped = false;
        for inner in chars.by_ref() {
            if escaped {
                escaped = false;
            } else if inner == '\\' {
                escaped = true;
            } else if inner == '"' {
                masked.push('"');
                break;
            }
        }
    }
    masked
}

/// Turns the raw TOML tree into a [`LoadedConfig`].
struct Builder<'a> {
    base_dir: &'a Path,
    inline_allowed: bool,
    env: &'a dyn Fn(&str) -> Option<String>,
    warnings: Vec<String>,
    has_inline_secret: bool,
}

impl Builder<'_> {
    fn build(&mut self, raw: RawConfig) -> Result<LoadedConfig, ConfigError> {
        let listen = raw.server.listen;
        let workers = raw
            .server
            .workers
            .map(|workers| {
                usize::try_from(workers)
                    .ok()
                    .and_then(NonZeroUsize::new)
                    .ok_or_else(|| invalid("server.workers", "must be at least 1"))
            })
            .transpose()?;
        let tls = raw
            .server
            .tls
            .as_ref()
            .map(|tls| self.tls_files(tls))
            .transpose()?;
        if tls.is_none() && !raw.server.allow_plaintext && !listen.ip().is_loopback() {
            return Err(ConfigError::PlaintextListen { listen });
        }
        let server = server_config(&raw.server)?;

        let limits = limits_spec(&raw.limits)?;
        let (forwarding, global_timeouts, outcome_queue) = forwarding(&raw.forwarding)?;
        let (base_client, warmup) = upstream(&raw.upstream)?;
        let experiments = ExperimentSpec {
            router: raw.experiments.router.map_or(RouterKind::Axum, Into::into),
            splice: raw
                .experiments
                .splice
                .map_or(SpliceMode::Segments, Into::into),
        };

        let keys = raw
            .keys
            .into_iter()
            .enumerate()
            .map(|(index, key)| key_spec(index, key))
            .collect::<Result<Vec<_>, _>>()?;
        let channels = raw
            .channels
            .into_iter()
            .enumerate()
            .map(|(index, channel)| {
                self.channel_spec(index, channel, global_timeouts, &base_client)
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(LoadedConfig {
            listen,
            spec: GatewaySpec {
                server,
                limits,
                forwarding,
                warmup,
                experiments,
                keys,
                channels,
            },
            workers,
            tls,
            outcome_queue,
            warnings: std::mem::take(&mut self.warnings),
        })
    }

    fn resolve(&self, path: &Path) -> PathBuf {
        self.base_dir.join(path)
    }

    fn tls_files(&mut self, raw: &RawTls) -> Result<TlsFiles, ConfigError> {
        let files = TlsFiles {
            cert: self.resolve(&raw.cert),
            key: self.resolve(&raw.key),
        };
        // The key is read by the TLS setup later; only its permissions are
        // checked here, so a bad key file fails at load time like the others.
        let key = File::open(&files.key).map_err(|source| ConfigError::Io {
            path: files.key.clone(),
            source,
        })?;
        check_permissions(&key, &files.key, true, &mut self.warnings)?;
        Ok(files)
    }

    fn channel_spec(
        &mut self,
        index: usize,
        raw: RawChannel,
        global: Timeouts,
        base_client: &UpstreamClientConfig,
    ) -> Result<ChannelSpec, ConfigError> {
        if raw.name.is_empty() {
            return Err(invalid(
                format!("channels[{index}].name"),
                "must not be empty",
            ));
        }
        let name = raw.name;
        let field = |field: &str| format!("channels[{name:?}].{field}");

        let api_key = self.secret(&name, raw.api_key)?;
        if api_key.expose().is_empty() {
            return Err(invalid(field("api_key"), "resolved to an empty value"));
        }
        let weight = match raw.weight {
            None => 1,
            Some(weight) => u32::try_from(weight)
                .ok()
                .filter(|&weight| weight >= 1)
                .ok_or_else(|| invalid(field("weight"), "must be in 1..=4294967295"))?,
        };
        let timeouts = Timeouts {
            commit_hold: raw.commit_hold.map_or(global.commit_hold, |hold| hold.0),
            first_byte: override_duration(
                field("first_byte_timeout"),
                raw.first_byte_timeout,
                global.first_byte,
            )?,
            idle: override_duration(field("idle_timeout"), raw.idle_timeout, global.idle)?,
        };
        let extra_root_certs = match &raw.ca_file {
            None => Vec::new(),
            Some(ca_file) => {
                let path = self.resolve(ca_file);
                load_ca_certs(&path).map_err(|source| ConfigError::CaFile {
                    channel: name.clone(),
                    path,
                    source,
                })?
            }
        };
        let client = UpstreamClientConfig {
            connect_timeout: override_duration(
                field("connect_timeout"),
                raw.connect_timeout,
                base_client.connect_timeout,
            )?,
            pool_idle_timeout: override_duration(
                field("pool_idle_timeout"),
                raw.pool_idle_timeout,
                base_client.pool_idle_timeout,
            )?,
            extra_root_certs,
            allow_private: raw.allow_private,
            ..base_client.clone()
        };
        let warmup = raw.warmup.map_or_else(WarmupTarget::default, |warmup| {
            let default = WarmupTarget::default();
            WarmupTarget {
                method: warmup.method.map_or(default.method, Into::into),
                path: warmup.path.unwrap_or(default.path),
            }
        });

        Ok(ChannelSpec {
            base_url: raw.base_url,
            api_key,
            weight,
            models: raw.models,
            model_map: raw.model_map.0,
            stream_usage: raw.stream_usage.map_or(StreamUsage::Inject, Into::into),
            timeouts,
            client,
            warmup,
            expose_ratelimit_headers: raw.expose_ratelimit_headers,
            name,
        })
    }

    fn secret(
        &mut self,
        channel: &str,
        source: SecretSource,
    ) -> Result<Redacted<String>, ConfigError> {
        match source {
            SecretSource::Env(var) => match (self.env)(&var) {
                Some(value) => Ok(Redacted::new(value)),
                None => Err(ConfigError::MissingEnv {
                    channel: channel.to_owned(),
                    var,
                }),
            },
            SecretSource::File(path) => {
                let path = self.resolve(&path);
                let secret_error = |source| ConfigError::SecretFile {
                    channel: channel.to_owned(),
                    path: path.clone(),
                    source,
                };
                let mut file = File::open(&path).map_err(secret_error)?;
                check_permissions(&file, &path, true, &mut self.warnings)?;
                let mut value = String::new();
                file.read_to_string(&mut value).map_err(secret_error)?;
                // Files written by `echo` or an editor end with a newline that
                // is not part of the key.
                let len = value.trim_end_matches(['\r', '\n']).len();
                value.truncate(len);
                Ok(Redacted::new(value))
            }
            SecretSource::Value(value) => {
                if !self.inline_allowed {
                    return Err(ConfigError::InlineSecretNotAllowed {
                        channel: channel.to_owned(),
                    });
                }
                self.has_inline_secret = true;
                Ok(value)
            }
        }
    }
}

fn server_config(raw: &RawServer) -> Result<ServerConfig, ConfigError> {
    let mut server = ServerConfig::default();
    if let Some(max) = raw.max_connections {
        server.max_connections = usize::try_from(max)
            .ok()
            .filter(|max| (1..=MAX_CONNECTIONS).contains(max))
            .ok_or_else(|| invalid("server.max_connections", MAX_CONNECTIONS_RANGE))?;
    }
    server.header_read_timeout = override_duration(
        "server.header_read_timeout",
        raw.header_read_timeout,
        server.header_read_timeout,
    )?;
    server.tls_handshake_timeout = override_duration(
        "server.tls_handshake_timeout",
        raw.tls_handshake_timeout,
        server.tls_handshake_timeout,
    )?;
    server.graceful_shutdown_timeout = override_duration(
        "server.graceful_shutdown_timeout",
        raw.graceful_shutdown_timeout,
        server.graceful_shutdown_timeout,
    )?;
    server.tcp_keepalive = override_duration(
        "server.tcp_keepalive",
        raw.tcp_keepalive,
        server.tcp_keepalive,
    )?;
    if let Some(backlog) = raw.backlog {
        server.backlog = i32::try_from(backlog)
            .ok()
            .filter(|&backlog| backlog >= 1)
            .ok_or_else(|| invalid("server.backlog", "must be in 1..=2147483647"))?;
    }
    Ok(server)
}

fn limits_spec(raw: &RawLimits) -> Result<LimitsSpec, ConfigError> {
    let default = LimitsSpec::default();
    Ok(LimitsSpec {
        max_body: size_u32(
            "limits.max_body",
            raw.max_body,
            default.max_body,
            "must be in 1B..=4GiB-1",
        )?,
        inflight_body_budget: size_usize(
            "limits.inflight_body_budget",
            raw.inflight_body_budget,
            default.inflight_body_budget,
        )?,
        body_read_timeout: override_duration(
            "limits.body_read_timeout",
            raw.body_read_timeout,
            default.body_read_timeout,
        )?,
        body_min_rate_bytes: size_u32(
            "limits.body_min_rate_bytes",
            raw.body_min_rate_bytes,
            default.body_min_rate_bytes,
            "must be in 1B..=4GiB-1",
        )?,
        body_min_rate_window: override_duration(
            "limits.body_min_rate_window",
            raw.body_min_rate_window,
            default.body_min_rate_window,
        )?,
        response_tap_budget: size_usize(
            "limits.response_tap_budget",
            raw.response_tap_budget,
            default.response_tap_budget,
        )?,
    })
}

fn forwarding(raw: &RawForwarding) -> Result<(ForwardingSpec, Timeouts, usize), ConfigError> {
    let default = ForwardingSpec::default();
    let max_attempts = match raw.max_attempts {
        None => default.max_attempts,
        Some(attempts) => u8::try_from(attempts)
            .ok()
            .filter(|attempts| (1..=8).contains(attempts))
            .ok_or_else(|| invalid("forwarding.max_attempts", "must be in 1..=8"))?,
    };
    let spec = ForwardingSpec {
        max_attempts,
        drain_timeout: override_duration(
            "forwarding.drain_timeout",
            raw.drain_timeout,
            default.drain_timeout,
        )?,
        drain_max_bytes: size_u32(
            "forwarding.drain_max_bytes",
            raw.drain_max_bytes,
            default.drain_max_bytes,
            "must be in 1B..=4GiB-1",
        )?,
    };

    let default_timeouts = Timeouts::default();
    let timeouts = Timeouts {
        // Zero is meaningful here: commit at the 2xx head (E4).
        commit_hold: raw
            .commit_hold
            .map_or(default_timeouts.commit_hold, |hold| hold.0),
        first_byte: override_duration(
            "forwarding.first_byte_timeout",
            raw.first_byte_timeout,
            default_timeouts.first_byte,
        )?,
        idle: override_duration(
            "forwarding.idle_timeout",
            raw.idle_timeout,
            default_timeouts.idle,
        )?,
    };

    let outcome_queue = match raw.outcome_queue {
        None => DEFAULT_OUTCOME_QUEUE,
        Some(queue) => usize::try_from(queue)
            .ok()
            .filter(|queue| (1..=MAX_OUTCOME_QUEUE).contains(queue))
            .ok_or_else(|| invalid("forwarding.outcome_queue", "must be in 1..=16777216"))?,
    };
    Ok((spec, timeouts, outcome_queue))
}

fn upstream(raw: &RawUpstream) -> Result<(UpstreamClientConfig, WarmupSpec), ConfigError> {
    let client_default = UpstreamClientConfig::default();
    let client = UpstreamClientConfig {
        connect_timeout: override_duration(
            "upstream.connect_timeout",
            raw.connect_timeout,
            client_default.connect_timeout,
        )?,
        pool_idle_timeout: override_duration(
            "upstream.pool_idle_timeout",
            raw.pool_idle_timeout,
            client_default.pool_idle_timeout,
        )?,
        tcp_keepalive: override_duration(
            "upstream.tcp_keepalive",
            raw.tcp_keepalive,
            client_default.tcp_keepalive,
        )?,
        tcp_user_timeout: override_duration(
            "upstream.tcp_user_timeout",
            raw.tcp_user_timeout,
            client_default.tcp_user_timeout,
        )?,
        ..client_default
    };
    let warmup_default = WarmupSpec::default();
    let warmup = WarmupSpec {
        interval: override_duration(
            "upstream.warmup_interval",
            raw.warmup_interval,
            warmup_default.interval,
        )?,
        ready_timeout: override_duration(
            "upstream.ready_timeout",
            raw.ready_timeout,
            warmup_default.ready_timeout,
        )?,
        request_timeout: override_duration(
            "upstream.warmup_request_timeout",
            raw.warmup_request_timeout,
            warmup_default.request_timeout,
        )?,
    };
    Ok((client, warmup))
}

fn key_spec(index: usize, raw: RawKey) -> Result<KeySpec, ConfigError> {
    if raw.name.is_empty() {
        return Err(invalid(format!("keys[{index}].name"), "must not be empty"));
    }
    let sha256 = parse_digest(&raw.sha256).ok_or_else(|| {
        invalid(
            format!("keys[{:?}].sha256", raw.name),
            "must be 64 lowercase hexadecimal digits",
        )
    })?;
    Ok(KeySpec {
        name: raw.name,
        sha256,
    })
}

/// 64 lowercase hex digits into 32 bytes; anything else is `None`.
fn parse_digest(hex: &str) -> Option<[u8; 32]> {
    fn nibble(digit: u8) -> Option<u8> {
        match digit {
            b'0'..=b'9' => Some(digit - b'0'),
            b'a'..=b'f' => Some(digit - b'a' + 10),
            _ => None,
        }
    }
    let bytes = hex.as_bytes();
    if bytes.len() != 64 {
        return None;
    }
    let mut digest = [0; 32];
    let (pairs, _) = bytes.as_chunks::<2>();
    for (out, &[high, low]) in digest.iter_mut().zip(pairs) {
        *out = (nibble(high)? << 4) | nibble(low)?;
    }
    Some(digest)
}

fn load_ca_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, pem::Error> {
    let certs = CertificateDer::pem_file_iter(path)?.collect::<Result<Vec<_>, _>>()?;
    if certs.is_empty() {
        return Err(pem::Error::NoItemsFound);
    }
    Ok(certs)
}

fn invalid(field: impl Into<String>, reason: &'static str) -> ConfigError {
    ConfigError::Invalid {
        field: field.into(),
        reason,
    }
}

/// `value` if given, else `default`; either way it must be positive.
fn override_duration(
    field: impl Into<String>,
    value: Option<ConfigDuration>,
    default: Duration,
) -> Result<Duration, ConfigError> {
    let duration = value.map_or(default, |value| value.0);
    if duration.is_zero() {
        return Err(invalid(field, "must be greater than zero"));
    }
    Ok(duration)
}

fn size_u32(
    field: &str,
    value: Option<Size>,
    default: u32,
    range: &'static str,
) -> Result<u32, ConfigError> {
    match value {
        None => Ok(default),
        Some(Size(bytes)) => u32::try_from(bytes)
            .ok()
            .filter(|&bytes| bytes >= 1)
            .ok_or_else(|| invalid(field, range)),
    }
}

fn size_usize(field: &str, value: Option<Size>, default: usize) -> Result<usize, ConfigError> {
    match value {
        None => Ok(default),
        Some(Size(bytes)) => usize::try_from(bytes)
            .ok()
            .filter(|&bytes| bytes >= 1)
            .ok_or_else(|| invalid(field, "must be at least 1B and fit in memory")),
    }
}

/// Fails on files other users can write; warns on files other users can read
/// when `holds_secret`. A no-op outside Unix (see the module documentation).
#[cfg_attr(
    not(unix),
    allow(
        clippy::unnecessary_wraps,
        clippy::needless_pass_by_ref_mut,
        reason = "the check exists only on Unix"
    )
)]
fn check_permissions(
    file: &File,
    path: &Path,
    holds_secret: bool,
    warnings: &mut Vec<String>,
) -> Result<(), ConfigError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;

        const OTHER_WRITE: u32 = 0o002;
        const OTHER_READ: u32 = 0o004;

        let mode = file
            .metadata()
            .map_err(|source| ConfigError::Io {
                path: path.to_path_buf(),
                source,
            })?
            .permissions()
            .mode()
            & 0o7777;
        if mode & OTHER_WRITE != 0 {
            return Err(ConfigError::InsecurePermissions {
                path: path.to_path_buf(),
                mode,
            });
        }
        if holds_secret && mode & OTHER_READ != 0 {
            warnings.push(format!(
                "{} holds a secret and is readable by other users (mode {mode:o}); consider chmod o-r",
                path.display()
            ));
        }
    }
    #[cfg(not(unix))]
    let _ = (file, path, holds_secret, warnings);
    Ok(())
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    server: RawServer,
    #[serde(default)]
    limits: RawLimits,
    #[serde(default)]
    forwarding: RawForwarding,
    #[serde(default)]
    upstream: RawUpstream,
    #[serde(default)]
    experiments: RawExperiments,
    keys: Vec<RawKey>,
    channels: Vec<RawChannel>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawServer {
    listen: SocketAddr,
    workers: Option<i64>,
    max_connections: Option<i64>,
    header_read_timeout: Option<ConfigDuration>,
    tls_handshake_timeout: Option<ConfigDuration>,
    graceful_shutdown_timeout: Option<ConfigDuration>,
    tcp_keepalive: Option<ConfigDuration>,
    backlog: Option<i64>,
    tls: Option<RawTls>,
    #[serde(default)]
    allow_plaintext: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawTls {
    cert: PathBuf,
    key: PathBuf,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawLimits {
    max_body: Option<Size>,
    inflight_body_budget: Option<Size>,
    body_read_timeout: Option<ConfigDuration>,
    body_min_rate_bytes: Option<Size>,
    body_min_rate_window: Option<ConfigDuration>,
    response_tap_budget: Option<Size>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawForwarding {
    max_attempts: Option<i64>,
    commit_hold: Option<ConfigDuration>,
    first_byte_timeout: Option<ConfigDuration>,
    idle_timeout: Option<ConfigDuration>,
    drain_timeout: Option<ConfigDuration>,
    drain_max_bytes: Option<Size>,
    outcome_queue: Option<i64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawUpstream {
    connect_timeout: Option<ConfigDuration>,
    pool_idle_timeout: Option<ConfigDuration>,
    tcp_keepalive: Option<ConfigDuration>,
    tcp_user_timeout: Option<ConfigDuration>,
    warmup_interval: Option<ConfigDuration>,
    ready_timeout: Option<ConfigDuration>,
    warmup_request_timeout: Option<ConfigDuration>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawExperiments {
    router: Option<RawRouter>,
    splice: Option<RawSplice>,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
enum RawRouter {
    Axum,
    Match,
}

impl From<RawRouter> for RouterKind {
    fn from(raw: RawRouter) -> Self {
        match raw {
            RawRouter::Axum => Self::Axum,
            RawRouter::Match => Self::Match,
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
enum RawSplice {
    Segments,
    Concat,
}

impl From<RawSplice> for SpliceMode {
    fn from(raw: RawSplice) -> Self {
        match raw {
            RawSplice::Segments => Self::Segments,
            RawSplice::Concat => Self::Concat,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawKey {
    name: String,
    sha256: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawChannel {
    name: String,
    base_url: String,
    api_key: SecretSource,
    weight: Option<i64>,
    #[serde(default)]
    allow_private: bool,
    stream_usage: Option<RawStreamUsage>,
    #[serde(default)]
    models: Vec<String>,
    #[serde(default)]
    model_map: ModelMap,
    warmup: Option<RawWarmup>,
    commit_hold: Option<ConfigDuration>,
    first_byte_timeout: Option<ConfigDuration>,
    idle_timeout: Option<ConfigDuration>,
    connect_timeout: Option<ConfigDuration>,
    pool_idle_timeout: Option<ConfigDuration>,
    ca_file: Option<PathBuf>,
    #[serde(default)]
    expose_ratelimit_headers: bool,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
enum RawStreamUsage {
    Inject,
    Passthrough,
}

impl From<RawStreamUsage> for StreamUsage {
    fn from(raw: RawStreamUsage) -> Self {
        match raw {
            RawStreamUsage::Inject => Self::Inject,
            RawStreamUsage::Passthrough => Self::Passthrough,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawWarmup {
    method: Option<RawWarmupMethod>,
    path: Option<String>,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
enum RawWarmupMethod {
    Head,
    Get,
}

impl From<RawWarmupMethod> for WarmupMethod {
    fn from(raw: RawWarmupMethod) -> Self {
        match raw {
            RawWarmupMethod::Head => Self::Head,
            RawWarmupMethod::Get => Self::Get,
        }
    }
}

/// `model_map` table as ordered pairs; toml has already rejected duplicate
/// keys.
#[derive(Debug, Default)]
struct ModelMap(Vec<(String, String)>);

impl<'de> Deserialize<'de> for ModelMap {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ModelMapVisitor;

        impl<'de> Visitor<'de> for ModelMapVisitor {
            type Value = ModelMap;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a table of client model name = upstream model name")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<ModelMap, A::Error> {
                let mut pairs = Vec::with_capacity(map.size_hint().unwrap_or(0));
                while let Some(pair) = map.next_entry::<String, String>()? {
                    pairs.push(pair);
                }
                Ok(ModelMap(pairs))
            }
        }

        deserializer.deserialize_map(ModelMapVisitor)
    }
}

/// A duration written as an integer and a unit: `ms`, `s`, `m` or `h`.
#[derive(Debug, Clone, Copy)]
struct ConfigDuration(Duration);

const DURATION_SYNTAX: &str =
    "invalid duration: expected an integer followed by ms, s, m or h, such as `600s`";

/// Parses `<digits><unit>`; fractions, signs, spaces and overflow are errors.
fn parse_duration(text: &str) -> Option<Duration> {
    let split = text.find(|c: char| !c.is_ascii_digit())?;
    let (digits, unit) = text.split_at(split);
    if digits.is_empty() {
        return None;
    }
    let value: u64 = digits.parse().ok()?;
    match unit {
        "ms" => Some(Duration::from_millis(value)),
        "s" => Some(Duration::from_secs(value)),
        "m" => value.checked_mul(60).map(Duration::from_secs),
        "h" => value.checked_mul(3600).map(Duration::from_secs),
        _ => None,
    }
}

impl<'de> Deserialize<'de> for ConfigDuration {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct DurationVisitor;

        impl Visitor<'_> for DurationVisitor {
            type Value = ConfigDuration;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a duration string such as `600s`")
            }

            fn visit_str<E: de::Error>(self, text: &str) -> Result<ConfigDuration, E> {
                parse_duration(text)
                    .map(ConfigDuration)
                    .ok_or_else(|| E::custom(DURATION_SYNTAX))
            }
        }

        deserializer.deserialize_str(DurationVisitor)
    }
}

/// A byte count: a non-negative integer, or an integer and a unit (`B`,
/// `KiB`, `MiB`, `GiB`) as a string.
#[derive(Debug, Clone, Copy)]
struct Size(u64);

const SIZE_SYNTAX: &str = "invalid size: expected an integer number of bytes, or an integer followed by B, KiB, MiB or GiB, such as `32MiB`";

/// Parses `<digits>[unit]`; fractions, signs, spaces and overflow are errors.
fn parse_size(text: &str) -> Option<u64> {
    let split = text
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(text.len());
    let (digits, unit) = text.split_at(split);
    if digits.is_empty() {
        return None;
    }
    let value: u64 = digits.parse().ok()?;
    let shift = match unit {
        "" | "B" => 0,
        "KiB" => 10,
        "MiB" => 20,
        "GiB" => 30,
        _ => return None,
    };
    value.checked_mul(1 << shift)
}

impl<'de> Deserialize<'de> for Size {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct SizeVisitor;

        impl Visitor<'_> for SizeVisitor {
            type Value = Size;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a byte count or a size string such as `32MiB`")
            }

            fn visit_i64<E: de::Error>(self, value: i64) -> Result<Size, E> {
                u64::try_from(value)
                    .map(Size)
                    .map_err(|_| E::custom("a size must not be negative"))
            }

            fn visit_u64<E: de::Error>(self, value: u64) -> Result<Size, E> {
                Ok(Size(value))
            }

            fn visit_str<E: de::Error>(self, text: &str) -> Result<Size, E> {
                parse_size(text)
                    .map(Size)
                    .ok_or_else(|| E::custom(SIZE_SYNTAX))
            }
        }

        deserializer.deserialize_any(SizeVisitor)
    }
}

/// Where a channel's upstream key comes from.
#[derive(Debug)]
enum SecretSource {
    /// Name of an environment variable.
    Env(String),
    /// Path of a file holding the key, relative to the configuration.
    File(PathBuf),
    /// The key itself; only in `*.local.toml`.
    Value(Redacted<String>),
}

/// The only message any malformed `api_key` produces: a type error would
/// quote the value, which is the secret in the common mistake
/// `api_key = "sk-..."`.
const API_KEY_SHAPE: &str = "api_key must be a table with exactly one of env, file, value";

/// Rejects every non-table form with [`API_KEY_SHAPE`] instead of serde's
/// default `invalid_type`, which would echo the value.
macro_rules! reject_scalars {
    ($($method:ident($ty:ty)),* $(,)?) => {
        $(
            fn $method<E: de::Error>(self, _: $ty) -> Result<SecretSource, E> {
                Err(E::custom(API_KEY_SHAPE))
            }
        )*
    };
}

impl<'de> Deserialize<'de> for SecretSource {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_map(SecretSourceVisitor)
    }
}

struct SecretSourceVisitor;

impl<'de> Visitor<'de> for SecretSourceVisitor {
    type Value = SecretSource;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a table with exactly one of env, file, value")
    }

    reject_scalars!(
        visit_bool(bool),
        visit_i64(i64),
        visit_i128(i128),
        visit_u64(u64),
        visit_u128(u128),
        visit_f64(f64),
        visit_char(char),
        visit_str(&str),
        visit_bytes(&[u8]),
    );

    fn visit_none<E: de::Error>(self) -> Result<SecretSource, E> {
        Err(E::custom(API_KEY_SHAPE))
    }

    fn visit_unit<E: de::Error>(self) -> Result<SecretSource, E> {
        Err(E::custom(API_KEY_SHAPE))
    }

    fn visit_some<D: Deserializer<'de>>(self, _: D) -> Result<SecretSource, D::Error> {
        Err(de::Error::custom(API_KEY_SHAPE))
    }

    fn visit_newtype_struct<D: Deserializer<'de>>(self, _: D) -> Result<SecretSource, D::Error> {
        Err(de::Error::custom(API_KEY_SHAPE))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, _: A) -> Result<SecretSource, A::Error> {
        Err(de::Error::custom(API_KEY_SHAPE))
    }

    fn visit_enum<A: EnumAccess<'de>>(self, _: A) -> Result<SecretSource, A::Error> {
        Err(de::Error::custom(API_KEY_SHAPE))
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<SecretSource, A::Error> {
        let mut source = None;
        while let Some(key) = map.next_key::<String>()? {
            // Every value error is replaced by fixed text: the error of a
            // mistyped `value` could otherwise quote the secret.
            let shape = |_| de::Error::custom(API_KEY_SHAPE);
            let next = match key.as_str() {
                "env" => SecretSource::Env(map.next_value::<String>().map_err(shape)?),
                "file" => SecretSource::File(map.next_value::<PathBuf>().map_err(shape)?),
                "value" => {
                    SecretSource::Value(Redacted::new(map.next_value::<String>().map_err(shape)?))
                }
                _ => return Err(de::Error::custom(API_KEY_SHAPE)),
            };
            if source.replace(next).is_some() {
                return Err(de::Error::custom(API_KEY_SHAPE));
            }
        }
        source.ok_or_else(|| de::Error::custom(API_KEY_SHAPE))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations_accept_integer_and_unit() {
        assert_eq!(parse_duration("0s"), Some(Duration::ZERO));
        assert_eq!(parse_duration("250ms"), Some(Duration::from_millis(250)));
        assert_eq!(parse_duration("600s"), Some(Duration::from_secs(600)));
        assert_eq!(parse_duration("2m"), Some(Duration::from_secs(120)));
        assert_eq!(parse_duration("1h"), Some(Duration::from_secs(3600)));
    }

    #[test]
    fn durations_reject_other_forms() {
        for text in [
            "",
            "s",
            "600",
            "1.5s",
            "-1s",
            "+1s",
            "1 s",
            " 1s",
            "1S",
            "1sec",
            "1d",
            "1us",
            "99999999999999999999s",
        ] {
            assert_eq!(parse_duration(text), None, "{text:?}");
        }
        assert_eq!(parse_duration(&format!("{}h", u64::MAX / 60)), None);
    }

    #[test]
    fn sizes_accept_bytes_and_binary_units() {
        assert_eq!(parse_size("0"), Some(0));
        assert_eq!(parse_size("512"), Some(512));
        assert_eq!(parse_size("512B"), Some(512));
        assert_eq!(parse_size("64KiB"), Some(64 << 10));
        assert_eq!(parse_size("32MiB"), Some(32 << 20));
        assert_eq!(parse_size("4GiB"), Some(4 << 30));
    }

    #[test]
    fn sizes_reject_other_forms() {
        for text in [
            "",
            "MiB",
            "1.5MiB",
            "-1",
            "1 MiB",
            "1MB",
            "1mib",
            "1KB",
            "1TiB",
            "18446744073709551615GiB",
        ] {
            assert_eq!(parse_size(text), None, "{text:?}");
        }
    }

    #[test]
    fn the_connection_limit_range_names_the_semaphore_limit() {
        assert_eq!(
            MAX_CONNECTIONS_RANGE,
            format!("must be in 1..={}", tokio::sync::Semaphore::MAX_PERMITS)
        );
    }

    #[test]
    fn digests_must_be_lowercase_hex() {
        let lower = "00ff".repeat(16);
        let digest = parse_digest(&lower).unwrap();
        assert_eq!(digest[0], 0x00);
        assert_eq!(digest[1], 0xff);
        assert_eq!(parse_digest(&lower.to_uppercase()), None);
        assert_eq!(parse_digest(&lower[..62]), None);
        assert_eq!(parse_digest(&format!("{lower}00")), None);
        assert_eq!(parse_digest(&"g".repeat(64)), None);
    }

    #[test]
    fn line_column_counts_lines_and_characters() {
        let text = "a = 1\nbé = \"x\"\n";
        assert_eq!(line_column(text, 0), (1, 1));
        assert_eq!(line_column(text, 6), (2, 1));
        // `é` is two bytes but one column.
        let quote = text.find('"').unwrap();
        assert_eq!(line_column(text, quote), (2, 6));
        assert_eq!(line_column(text, text.len() + 10), (3, 1));
    }

    #[test]
    fn errors_render_the_documented_messages() {
        use std::error::Error as _;

        let path = || PathBuf::from("brisk.toml");
        let channel = || "cpa".to_owned();
        let cases = [
            (
                ConfigError::Io {
                    path: path(),
                    source: io::ErrorKind::NotFound.into(),
                },
                "cannot read brisk.toml",
                true,
            ),
            (
                ConfigError::Parse {
                    path: path(),
                    line: Some(3),
                    column: Some(7),
                    message: "expected `=`".to_owned(),
                },
                "brisk.toml:3:7: expected `=`",
                false,
            ),
            (
                ConfigError::Parse {
                    path: path(),
                    line: Some(3),
                    column: None,
                    message: "missing field `server`".to_owned(),
                },
                "brisk.toml: missing field `server`",
                false,
            ),
            (
                ConfigError::Parse {
                    path: path(),
                    line: None,
                    column: None,
                    message: "missing field `server`".to_owned(),
                },
                "brisk.toml: missing field `server`",
                false,
            ),
            (
                ConfigError::MissingEnv {
                    channel: channel(),
                    var: "BRISK_CPA_KEY".to_owned(),
                },
                "channel \"cpa\": environment variable BRISK_CPA_KEY for api_key is not set",
                false,
            ),
            (
                ConfigError::SecretFile {
                    channel: channel(),
                    path: PathBuf::from("cpa.key"),
                    source: io::ErrorKind::PermissionDenied.into(),
                },
                "channel \"cpa\": cannot read api_key file cpa.key",
                true,
            ),
            (
                ConfigError::InlineSecretNotAllowed { channel: channel() },
                "channel \"cpa\": api_key = { value = ... } is only allowed in *.local.toml files; use env or file",
                false,
            ),
            (
                ConfigError::InsecurePermissions {
                    path: path(),
                    mode: 0o646,
                },
                "brisk.toml is writable by other users (mode 646); remove o+w",
                false,
            ),
            (
                ConfigError::PlaintextListen {
                    listen: "0.0.0.0:8080".parse().unwrap(),
                },
                "server.listen 0.0.0.0:8080 is not a loopback address; configure [server.tls] or set server.allow_plaintext = true",
                false,
            ),
            (
                ConfigError::CaFile {
                    channel: channel(),
                    path: PathBuf::from("ca.pem"),
                    source: pem::Error::NoItemsFound,
                },
                "channel \"cpa\": cannot load CA certificates from ca.pem",
                true,
            ),
            (
                invalid("forwarding.max_attempts", "must be in 1..=8"),
                "forwarding.max_attempts: must be in 1..=8",
                false,
            ),
        ];
        for (err, message, has_source) in cases {
            assert_eq!(err.to_string(), message);
            assert_eq!(err.source().is_some(), has_source, "{err:?}");
        }
    }

    #[test]
    fn masking_hides_quoted_strings_and_variants() {
        assert_eq!(
            mask_values(r#"invalid type: string "sk-leak-123", expected u32"#),
            r#"invalid type: string "***", expected u32"#
        );
        assert_eq!(
            mask_values(r#"invalid value: string "a\"b\\", expected x"#),
            r#"invalid value: string "***", expected x"#
        );
        assert_eq!(
            mask_values(r#"unterminated "sk-leak"#),
            r#"unterminated "***"#
        );
        assert_eq!(
            mask_values("invalid basic string, expected `\"`"),
            "invalid basic string, expected `\"`"
        );
        assert_eq!(
            mask_values("unknown variant `sk-leak-123`, expected `inject` or `passthrough`"),
            "unknown variant `***`, expected `inject` or `passthrough`"
        );
        assert_eq!(
            mask_values("unknown variant `a`, expected `b`, expected x`, expected `c`"),
            "unknown variant `***`, expected `c`"
        );
        assert_eq!(
            mask_values("unknown variant `sk\"leak`, there are no variants"),
            "unknown variant `***`, there are no variants"
        );
        assert_eq!(
            mask_values("unknown field `wieght`, expected one of `name`, `weight`"),
            "unknown field `wieght`, expected one of `name`, `weight`"
        );
    }
}
