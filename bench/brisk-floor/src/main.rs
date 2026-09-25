//! `brisk-floor`: floor-A and floor-B, the empty blind-forwarding proxies.
//!
//! ```text
//! brisk-floor --listen <addr> --upstream <base-url> [--mode a|b] [--upstream-ca <pem>]
//!             [--tls-cert <pem> --tls-key <pem>] [--workers N] [--cpu-list 0-1]
//! ```
//!
//! Runs until Ctrl-C or `SIGTERM`, then drains in-flight connections.

use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::path::PathBuf;

use anyhow::Context as _;
use brisk_bench_core::cpu;
use brisk_bench_core::transport::tls as bench_tls;
use brisk_floor::forward::{self, Forwarder};
use brisk_floor::same_task::{self, SameTaskForwarder};
use brisk_floor::tls;
use brisk_gateway::net::raise_nofile_soft_limit;
use brisk_gateway::server::{self, ServerConfig};
use brisk_gateway::upstream::{UpstreamClientConfig, build_client};
use clap::Parser;
use tokio::runtime::Runtime;
use tokio::sync::oneshot;
use tokio::task::JoinError;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::filter::LevelFilter;

#[cfg(not(target_os = "macos"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// floor-A and floor-B: forward every request unmodified to one upstream,
/// streaming both bodies, to measure the cost of the gateway's connection
/// layer and an upstream client alone.
#[derive(Debug, Parser)]
#[command(version, about)]
struct Cli {
    /// Address to accept client connections on.
    #[arg(long)]
    listen: SocketAddr,

    /// Upstream base URL (`http://` or `https://`); the inbound path and query
    /// are appended to it.
    #[arg(long)]
    upstream: String,

    /// Upstream client: `a` is floor-A (the gateway's reqwest client), `b` is
    /// floor-B (hyper HTTP/1.1 connections driven in the forwarding task).
    #[arg(long, value_enum, default_value_t = Mode::A)]
    mode: Mode,

    /// PEM file with CA certificates trusted for the upstream: in addition to
    /// the platform roots in mode `a`, as the only roots in mode `b`, where it
    /// is required for an `https` upstream.
    #[arg(long)]
    upstream_ca: Option<PathBuf>,

    /// PEM certificate chain for inbound TLS (ALPN h2 and http/1.1).
    #[arg(long, requires = "tls_key")]
    tls_cert: Option<PathBuf>,

    /// PEM private key for inbound TLS.
    #[arg(long, requires = "tls_cert")]
    tls_key: Option<PathBuf>,

    /// Number of tokio worker threads. Defaults to the size of `--cpu-list`,
    /// or to the available parallelism without it.
    #[arg(long)]
    workers: Option<NonZeroUsize>,

    /// CPUs the runtime threads are pinned to, e.g. `0-1` (Linux only; ignored
    /// with a warning elsewhere).
    #[arg(long, value_parser = parse_cpu_list)]
    cpu_list: Option<CpuList>,
}

/// Which upstream client forwards the requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum Mode {
    /// floor-A: the gateway's reqwest client, which runs each upstream
    /// connection in a task of its own.
    A,
    /// floor-B: pooled hyper HTTP/1.1 connections polled by the task that
    /// forwards the request and its response.
    B,
}

impl Mode {
    fn name(self) -> &'static str {
        match self {
            Self::A => "a",
            Self::B => "b",
        }
    }
}

/// The forwarder of the selected mode.
enum Floor {
    A(Forwarder),
    B(SameTaskForwarder),
}

impl Floor {
    fn build(cli: &Cli) -> anyhow::Result<Self> {
        match cli.mode {
            Mode::A => {
                let mut upstream_config = UpstreamClientConfig {
                    // Every benchmark topology puts the upstream on loopback or
                    // the internal network, and a host name there must resolve.
                    allow_private: true,
                    ..UpstreamClientConfig::default()
                };
                if let Some(ca) = &cli.upstream_ca {
                    upstream_config.extra_root_certs = tls::load_ca_certs(ca)
                        .with_context(|| format!("loading upstream CA {}", ca.display()))?;
                }
                let client = build_client(&upstream_config).context("building upstream client")?;
                Ok(Self::A(Forwarder::new(client, &cli.upstream)?))
            }
            Mode::B => {
                let tls = cli
                    .upstream_ca
                    .as_deref()
                    .map(|ca| {
                        bench_tls::client_config(ca)
                            .with_context(|| format!("loading upstream CA {}", ca.display()))
                    })
                    .transpose()?;
                Ok(Self::B(SameTaskForwarder::new(&cli.upstream, tls)?))
            }
        }
    }

    fn upstream_base(&self) -> &str {
        match self {
            Self::A(forwarder) => forwarder.upstream_base(),
            Self::B(forwarder) => forwarder.upstream_base(),
        }
    }
}

/// A parsed `--cpu-list`, wrapped so clap treats it as one value.
#[derive(Debug, Clone)]
struct CpuList(Vec<usize>);

fn parse_cpu_list(list: &str) -> Result<CpuList, cpu::CpuListError> {
    cpu::parse_cpu_list(list).map(CpuList)
}

fn main() -> anyhow::Result<()> {
    init_tracing()?;
    let cli = Cli::parse();

    // Must precede `ServerConfig::default()`, which derives the connection
    // limit from the current soft limit.
    let nofile = raise_nofile_soft_limit().context("raising RLIMIT_NOFILE")?;
    let server_config = ServerConfig::default();

    let tls = match (&cli.tls_cert, &cli.tls_key) {
        (Some(cert), Some(key)) => {
            Some(tls::acceptor(cert, key).context("loading inbound TLS certificate and key")?)
        }
        _ => None,
    };
    let floor = Floor::build(&cli)?;
    let mode = cli.mode;

    let cpus = cli.cpu_list.map(|list| list.0);
    let workers = match (cli.workers, &cpus) {
        (Some(workers), _) => workers.get(),
        (None, Some(cpus)) => cpus.len(),
        (None, None) => std::thread::available_parallelism()
            .context("querying available parallelism")?
            .get(),
    };
    let runtime = build_runtime(workers, cpus.as_deref())?;

    runtime.block_on(async move {
        let mut signals = ShutdownSignals::install().context("installing signal handlers")?;
        #[cfg(target_os = "linux")]
        if let Some(cpus) = &cpus {
            verify_runtime_affinity(cpus).await?;
        }
        let listener = server::bind(cli.listen, &server_config)
            .with_context(|| format!("binding {}", cli.listen))?;
        tracing::info!(
            listen = %listener.local_addr()?,
            upstream = floor.upstream_base(),
            mode = mode.name(),
            tls = tls.is_some(),
            workers,
            cpus = cpus.as_deref().map(cpu::format_cpu_list),
            nofile,
            "brisk-floor ready"
        );
        let (begin_drain, drain) = oneshot::channel::<()>();
        let stop = async move {
            // A dropped sender means the same as a sent one: stop.
            let _ = drain.await;
        };
        // Spawned so the accept loop runs on the (pinned) workers rather than
        // on the main thread, which only waits here.
        let mut serving = match floor {
            Floor::A(forwarder) => {
                tokio::spawn(forward::run(listener, tls, server_config, forwarder, stop))
            }
            Floor::B(forwarder) => tokio::spawn(same_task::run(
                listener,
                tls,
                server_config,
                forwarder,
                stop,
            )),
        };

        let signal = tokio::select! {
            served = &mut serving => return serve_outcome(served),
            signal = signals.recv() => signal,
        };
        tracing::info!(signal, "shutting down; signal again to abandon the drain");
        // Fails only if serving already ended, which the next select reports.
        let _ = begin_drain.send(());
        tokio::select! {
            served = &mut serving => serve_outcome(served),
            signal = signals.recv() => {
                serving.abort();
                anyhow::bail!("{signal} received during the drain; abandoned in-flight connections")
            }
        }
    })?;
    tracing::info!("brisk-floor stopped");
    Ok(())
}

fn serve_outcome(served: Result<std::io::Result<()>, JoinError>) -> anyhow::Result<()> {
    served.context("serve task panicked")?.context("serving")
}

fn init_tracing() -> anyhow::Result<()> {
    let filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env()
        .context("parsing RUST_LOG")?;
    // Benchmark runs redirect logs to files, where color codes are noise.
    let ansi = std::io::IsTerminal::is_terminal(&std::io::stdout());
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_ansi(ansi)
        .init();
    Ok(())
}

fn build_runtime(workers: usize, cpus: Option<&[usize]>) -> anyhow::Result<Runtime> {
    if let Some(cpus) = cpus {
        pin_creating_thread(cpus)?;
    }
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .thread_name("brisk-floor")
        .enable_all()
        .build()
        .context("building tokio runtime")
}

/// Pins every runtime thread to the whole CPU set by pinning the thread that
/// builds the runtime.
///
/// Only the calling (main) thread is pinned: on Linux a new thread inherits
/// its creator's affinity, the runtime's workers are started from this thread
/// by `build()`, and blocking-pool threads (reqwest resolves DNS there) are
/// started from workers or from this thread. [`verify_runtime_affinity`]
/// checks the result once the runtime runs. The set is not split one CPU per
/// worker: tokio's thread hooks cannot tell workers from blocking threads, and
/// confining all of them to the set is what keeps floor off the cores of the
/// mock and the load generator.
#[cfg(target_os = "linux")]
fn pin_creating_thread(cpus: &[usize]) -> anyhow::Result<()> {
    cpu::pin_current_thread(cpus)
        .with_context(|| format!("pinning to CPUs {}", cpu::format_cpu_list(cpus)))
}

/// Reads the affinity back on a worker and on a blocking-pool thread and fails
/// unless both are exactly `cpus`; a run on the wrong cores must not start.
#[cfg(target_os = "linux")]
async fn verify_runtime_affinity(cpus: &[usize]) -> anyhow::Result<()> {
    let on_worker = tokio::spawn(async { cpu::current_affinity() })
        .await
        .context("affinity probe panicked")?
        .context("reading worker affinity")?;
    let on_blocking = tokio::task::spawn_blocking(cpu::current_affinity)
        .await
        .context("affinity probe panicked")?
        .context("reading blocking-thread affinity")?;
    for (thread, actual) in [("worker", on_worker), ("blocking", on_blocking)] {
        anyhow::ensure!(
            actual == cpus,
            "{thread} thread runs on CPUs {} instead of {}",
            cpu::format_cpu_list(&actual),
            cpu::format_cpu_list(cpus)
        );
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
#[expect(
    clippy::unnecessary_wraps,
    reason = "signature is shared with the Linux implementation"
)]
fn pin_creating_thread(cpus: &[usize]) -> anyhow::Result<()> {
    tracing::warn!(
        cpus = %cpu::format_cpu_list(cpus),
        "--cpu-list is only supported on Linux; running unpinned"
    );
    Ok(())
}

/// The signals that stop floor: the first starts a graceful drain, a second
/// one abandons it.
///
/// Handlers are registered eagerly by [`ShutdownSignals::install`] so a
/// failure stops startup instead of leaving a server that cannot be stopped
/// cleanly. Once registered, the default action of the signal no longer
/// applies, which is why the handle keeps listening after the first one.
#[derive(Debug)]
struct ShutdownSignals {
    #[cfg(unix)]
    interrupt: tokio::signal::unix::Signal,
    #[cfg(unix)]
    terminate: tokio::signal::unix::Signal,
    #[cfg(windows)]
    ctrl_c: tokio::signal::windows::CtrlC,
}

impl ShutdownSignals {
    /// Registers `SIGINT` and `SIGTERM` (Unix) or Ctrl-C (Windows).
    #[cfg(unix)]
    fn install() -> std::io::Result<Self> {
        use tokio::signal::unix::{SignalKind, signal};

        Ok(Self {
            interrupt: signal(SignalKind::interrupt())?,
            terminate: signal(SignalKind::terminate())?,
        })
    }

    /// Registers `SIGINT` and `SIGTERM` (Unix) or Ctrl-C (Windows).
    #[cfg(windows)]
    fn install() -> std::io::Result<Self> {
        Ok(Self {
            ctrl_c: tokio::signal::windows::ctrl_c()?,
        })
    }

    /// Waits for the next signal and returns its name.
    #[cfg(unix)]
    async fn recv(&mut self) -> &'static str {
        tokio::select! {
            _ = self.interrupt.recv() => "SIGINT",
            _ = self.terminate.recv() => "SIGTERM",
        }
    }

    /// Waits for the next signal and returns its name.
    #[cfg(windows)]
    async fn recv(&mut self) -> &'static str {
        self.ctrl_c.recv().await;
        "Ctrl-C"
    }
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory as _;

    use super::*;

    #[test]
    fn cli_definition_is_consistent() {
        Cli::command().debug_assert();
    }

    #[test]
    fn tls_cert_and_key_require_each_other() {
        let base = [
            "brisk-floor",
            "--listen",
            "127.0.0.1:0",
            "--upstream",
            "http://h",
        ];
        for extra in [["--tls-cert", "c.pem"], ["--tls-key", "k.pem"]] {
            let err = Cli::try_parse_from(base.iter().chain(&extra)).unwrap_err();
            assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);
        }
        let both = ["--tls-cert", "c.pem", "--tls-key", "k.pem"];
        let cli = Cli::try_parse_from(base.iter().chain(&both)).unwrap();
        assert!(cli.tls_cert.is_some() && cli.tls_key.is_some());
    }

    #[test]
    fn mode_defaults_to_floor_a() {
        let base = [
            "brisk-floor",
            "--listen",
            "127.0.0.1:0",
            "--upstream",
            "http://h",
        ];
        assert_eq!(Cli::try_parse_from(base).unwrap().mode, Mode::A);
        let cli = Cli::try_parse_from(base.iter().chain(&["--mode", "b"])).unwrap();
        assert_eq!(cli.mode, Mode::B);
        assert!(Cli::try_parse_from(base.iter().chain(&["--mode", "c"])).is_err());
    }

    #[test]
    fn floor_b_refuses_an_https_upstream_without_ca() {
        let cli = Cli::try_parse_from([
            "brisk-floor",
            "--listen",
            "127.0.0.1:0",
            "--upstream",
            "https://127.0.0.1:19443",
            "--mode",
            "b",
        ])
        .unwrap();
        let err = Floor::build(&cli).err().expect("accepted without a CA");
        assert!(err.to_string().contains("--upstream-ca"), "{err:#}");
    }

    #[test]
    fn https_upstream_builds_with_a_ca_in_mode_b_and_without_one_in_mode_a() {
        use brisk_bench_core::transport::certs::{CA_CERT_FILE, CertBundle};

        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "brisk-floor-main-test-{}-{nanos}",
            std::process::id()
        ));
        CertBundle::generate(&["localhost".to_owned()])
            .unwrap()
            .write_to(&dir)
            .unwrap();
        let ca = dir.join(CA_CERT_FILE);
        let base = [
            "brisk-floor",
            "--listen",
            "127.0.0.1:0",
            "--upstream",
            "https://localhost:19443",
        ];

        let with_ca = ["--mode", "b", "--upstream-ca", ca.to_str().unwrap()];
        let cli = Cli::try_parse_from(base.iter().chain(&with_ca)).unwrap();
        let built = Floor::build(&cli);
        // floor-A trusts the platform roots as well and needs no CA.
        let cli = Cli::try_parse_from(base).unwrap();
        let floor_a = Floor::build(&cli);
        std::fs::remove_dir_all(&dir).unwrap();

        assert!(matches!(built.unwrap(), Floor::B(_)));
        assert!(matches!(floor_a.unwrap(), Floor::A(_)));
    }

    #[test]
    fn cpu_list_is_parsed_and_invalid_lists_rejected() {
        let base = [
            "brisk-floor",
            "--listen",
            "127.0.0.1:0",
            "--upstream",
            "http://h",
        ];
        let cli = Cli::try_parse_from(base.iter().chain(&["--cpu-list", "0-1,3"])).unwrap();
        assert_eq!(cli.cpu_list.unwrap().0, [0, 1, 3]);
        assert!(Cli::try_parse_from(base.iter().chain(&["--cpu-list", "3-1"])).is_err());
        assert!(Cli::try_parse_from(base.iter().chain(&["--workers", "0"])).is_err());
    }
}
