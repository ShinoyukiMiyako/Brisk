//! `brisk`: the gateway binary. Sets up the allocator and tracing, parses the
//! command line and hands over to [`brisk::run`]; an error ends the process
//! with a non-zero exit code and its `anyhow` chain on stderr.

use std::env::{self, VarError};
use std::io::{self, IsTerminal as _};

use anyhow::Context as _;
use brisk::cli::Cli;
use clap::Parser as _;
use tracing_subscriber::EnvFilter;

#[cfg(not(target_os = "macos"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Filter used when `RUST_LOG` is unset or empty: per-request outcome records
/// are off, totals and warnings stay on (D27).
const DEFAULT_FILTER: &str = "info,brisk::outcome=off";

fn main() -> anyhow::Result<()> {
    init_tracing()?;
    let cli = Cli::parse();
    brisk::run(cli)
}

/// Logs to stderr: stdout carries only command output (the frozen `keygen`
/// format and the `check-config` summary). An invalid `RUST_LOG` fails
/// startup instead of being ignored.
fn init_tracing() -> anyhow::Result<()> {
    let directives = match env::var(EnvFilter::DEFAULT_ENV) {
        Ok(directives) if !directives.is_empty() => directives,
        Ok(_) | Err(VarError::NotPresent) => DEFAULT_FILTER.to_owned(),
        Err(err @ VarError::NotUnicode(_)) => {
            return Err(err).context("reading RUST_LOG");
        }
    };
    let filter = EnvFilter::builder()
        .parse(&directives)
        .with_context(|| format!("parsing RUST_LOG {directives:?}"))?;
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(io::stderr)
        // Services and benchmark runs redirect stderr to files, where color
        // codes are noise.
        .with_ansi(io::stderr().is_terminal())
        .try_init()
        .map_err(anyhow::Error::from_boxed)
        .context("installing the tracing subscriber")
}
