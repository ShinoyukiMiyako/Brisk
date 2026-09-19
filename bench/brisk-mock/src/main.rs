//! `brisk-mock`: paced SSE upstream for Brisk benchmarks.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context as _;
use brisk_bench_core::transport::{certs, tls};
use brisk_bench_core::wire::BenchParams;
use brisk_bench_core::{cpu, rlimit};
use brisk_mock::{EmitPolicy, MockConfig, ServerHandle};
use clap::{Args, Parser, Subcommand, ValueEnum};
use tracing_subscriber::EnvFilter;

/// Paced OpenAI-compatible SSE upstream with in-band timestamps.
#[derive(Debug, Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Serve the mock API.
    Serve(ServeArgs),
    /// Generate a self-signed CA and a server certificate signed by it.
    GenCert(GenCertArgs),
}

#[derive(Debug, Args)]
struct ServeArgs {
    /// Address to listen on, e.g. 0.0.0.0:19080.
    #[arg(long)]
    listen: SocketAddr,
    /// Server certificate chain (PEM); enables TLS together with --tls-key.
    #[arg(long, requires = "tls_key")]
    tls_cert: Option<PathBuf>,
    /// Server private key (PEM).
    #[arg(long, requires = "tls_cert")]
    tls_key: Option<PathBuf>,
    /// Number of shard threads; more than one requires Linux.
    #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u16).range(1..))]
    shards: u16,
    /// Busy-wait window before each emission, in microseconds.
    #[arg(long, default_value_t = 50)]
    spin_us: u64,
    /// How close to an emission a shard keeps serving sockets.
    #[arg(long, value_enum, default_value_t = PolicyArg::Fixed)]
    emit_policy: PolicyArg,
    /// Commit window of the fixed policy, in microseconds: how close the
    /// next emission may come before a shard stops serving sockets and spins
    /// to it. Capped at --spin-us.
    #[arg(long, default_value_t = DEFAULT_COMMIT_US)]
    commit_us: u64,
    /// CPUs to pin shards to (Linux list syntax, e.g. 2 or 2-3); shard i uses
    /// the (i mod n)-th CPU of the list. Pinning is supported on Linux only.
    #[arg(long, value_parser = parse_cpu_list)]
    cpu_list: Option<CpuList>,
    /// Model name reported in responses and by /v1/models.
    #[arg(long, default_value = "brisk-mock")]
    model: String,
    /// Backlog of every listening socket.
    #[arg(long, default_value_t = 4096)]
    backlog: i32,
    #[command(flatten)]
    defaults: DefaultParams,
}

/// Default of `--commit-us`, [`brisk_mock::DEFAULT_COMMIT_WINDOW`].
const DEFAULT_COMMIT_US: u64 = 10;

/// `--emit-policy` values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum PolicyArg {
    /// Stop serving sockets once an emission is within the spin window.
    FullSpin,
    /// Serve sockets until an emission is within the commit window.
    Fixed,
}

impl From<PolicyArg> for EmitPolicy {
    fn from(arg: PolicyArg) -> Self {
        match arg {
            PolicyArg::FullSpin => Self::FullSpin,
            PolicyArg::Fixed => Self::Fixed,
        }
    }
}

/// Parameters for requests that carry no bench:v1 directive.
#[derive(Debug, Args)]
struct DefaultParams {
    /// Default delay from request completion to the first chunk, in microseconds.
    #[arg(long = "default-ttft-us", default_value_t = 0)]
    ttft_us: u64,
    /// Default delay between chunks, in microseconds.
    #[arg(long = "default-interval-us", default_value_t = 33_333)]
    interval_us: u64,
    /// Default number of content chunks per stream.
    #[arg(long = "default-chunks", default_value_t = 16)]
    chunks: u32,
    /// Default content length of each chunk, marker included (78 to 1048576).
    #[arg(long = "default-chunk-bytes", default_value_t = 128)]
    chunk_bytes: u32,
    /// Default content length of a non-streaming response (at most 67108864).
    #[arg(long = "default-resp-bytes", default_value_t = 1024)]
    resp_bytes: u32,
}

impl DefaultParams {
    fn to_params(&self) -> BenchParams {
        BenchParams {
            ttft_us: self.ttft_us,
            interval_us: self.interval_us,
            chunks: self.chunks,
            chunk_bytes: self.chunk_bytes,
            sid: 0,
            resp_bytes: self.resp_bytes,
        }
    }
}

#[derive(Debug, Clone)]
struct CpuList(Vec<usize>);

fn parse_cpu_list(list: &str) -> Result<CpuList, cpu::CpuListError> {
    cpu::parse_cpu_list(list).map(CpuList)
}

#[derive(Debug, Args)]
struct GenCertArgs {
    /// Directory to write ca.pem, server.pem and server.key into.
    #[arg(long)]
    out_dir: PathBuf,
    /// Subject alternative names (DNS names or IP addresses).
    #[arg(long, required = true, num_args = 1..)]
    san: Vec<String>,
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();
    match Cli::parse().command {
        Command::Serve(args) => serve(args),
        Command::GenCert(args) => {
            certs::generate(&args.out_dir, &args.san).with_context(|| {
                format!("generating certificates in {}", args.out_dir.display())
            })?;
            tracing::info!(dir = %args.out_dir.display(), "certificates written");
            Ok(())
        }
    }
}

fn serve(args: ServeArgs) -> anyhow::Result<()> {
    let nofile_limit = rlimit::raise_nofile_limit().context("raising RLIMIT_NOFILE")?;
    tracing::info!(?nofile_limit, "open-file limit");
    let tls = match (&args.tls_cert, &args.tls_key) {
        (Some(cert), Some(key)) => {
            Some(tls::server_config(cert, key).context("loading the TLS certificate and key")?)
        }
        _ => None,
    };
    let config = MockConfig {
        listen: args.listen,
        shards: usize::from(args.shards),
        spin_window: Duration::from_micros(args.spin_us),
        emit_policy: args.emit_policy.into(),
        commit_window: Duration::from_micros(args.commit_us),
        cpus: args.cpu_list.map(|CpuList(cpus)| cpus),
        tls,
        defaults: args.defaults.to_params(),
        model: args.model,
        backlog: args.backlog,
    };
    let tls_enabled = config.tls.is_some();
    let emit_policy = config.emit_policy;
    let handle = ServerHandle::start(config).context("starting the mock server")?;
    tracing::info!(
        addr = %handle.local_addr(),
        shards = args.shards,
        tls = tls_enabled,
        %emit_policy,
        spin_us = args.spin_us,
        commit_us = args.commit_us,
        "brisk-mock listening"
    );
    handle.wait().context("mock server stopped")
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory as _;

    use super::*;

    #[test]
    fn cli_is_consistent() {
        Cli::command().debug_assert();
    }

    #[test]
    fn serve_arguments_parse() {
        let cli = Cli::try_parse_from([
            "brisk-mock",
            "serve",
            "--listen",
            "127.0.0.1:0",
            "--shards",
            "2",
            "--cpu-list",
            "2-3",
            "--default-chunks",
            "3",
        ])
        .unwrap();
        let Command::Serve(args) = cli.command else {
            panic!("expected serve");
        };
        assert_eq!(args.shards, 2);
        assert_eq!(args.cpu_list.unwrap().0, [2, 3]);
        assert_eq!(args.defaults.to_params().chunks, 3);
        assert_eq!(args.spin_us, 50);
        assert_eq!(args.emit_policy, PolicyArg::Fixed);
        assert_eq!(args.commit_us, 10);
        assert_eq!(
            Duration::from_micros(args.commit_us),
            brisk_mock::DEFAULT_COMMIT_WINDOW
        );
    }

    #[test]
    fn emit_policy_and_commit_window_parse() {
        let serve = ["brisk-mock", "serve", "--listen", "127.0.0.1:0"];
        let with = |extra: &[&'static str]| Cli::try_parse_from(serve.iter().chain(extra));
        for (name, policy) in [
            ("full-spin", EmitPolicy::FullSpin),
            ("fixed", EmitPolicy::Fixed),
        ] {
            let cli = with(&["--emit-policy", name, "--commit-us", "3"]).unwrap();
            let Command::Serve(args) = cli.command else {
                panic!("expected serve");
            };
            assert_eq!(EmitPolicy::from(args.emit_policy), policy);
            assert_eq!(policy.as_str(), name);
            assert_eq!(args.commit_us, 3);
        }
        assert!(with(&["--emit-policy", "spin"]).is_err());
        assert!(with(&["--emit-policy", "adaptive"]).is_err());
        assert!(with(&["--commit-us", "-1"]).is_err());
    }

    #[test]
    fn tls_needs_both_files_and_shards_must_be_positive() {
        let serve = ["brisk-mock", "serve", "--listen", "127.0.0.1:0"];
        let with = |extra: &[&'static str]| Cli::try_parse_from(serve.iter().chain(extra));
        assert!(with(&["--tls-cert", "a.pem"]).is_err());
        assert!(with(&["--shards", "0"]).is_err());
        assert!(with(&["--tls-cert", "a.pem", "--tls-key", "a.key"]).is_ok());
    }

    #[test]
    fn gen_cert_takes_several_sans() {
        let cli = Cli::try_parse_from([
            "brisk-mock",
            "gen-cert",
            "--out-dir",
            "x",
            "--san",
            "localhost",
            "127.0.0.1",
        ])
        .unwrap();
        let Command::GenCert(args) = cli.command else {
            panic!("expected gen-cert");
        };
        assert_eq!(args.san, ["localhost", "127.0.0.1"]);
    }
}
