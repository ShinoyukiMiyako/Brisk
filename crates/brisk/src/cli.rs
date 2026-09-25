//! The command line: `brisk serve`, `brisk check-config` and `brisk keygen`.
//!
//! Every value is a long option (`--config <PATH>`, `--name <NAME>`), never a
//! positional argument, so scripts such as `scripts/bench/run-m1.sh` read the
//! same in any argument order.

use std::path::PathBuf;

use clap::{Parser, Subcommand};

/// Brisk: an OpenAI-compatible Chat Completions gateway that forwards to
/// upstream channels with failover and settles usage per request.
// `about` is left to this doc comment; a bare `#[command(about)]` would show
// the package description instead.
#[derive(Debug, Clone, PartialEq, Eq, Parser)]
#[command(version)]
pub struct Cli {
    /// What to do.
    #[command(subcommand)]
    pub command: Command,
}

/// The subcommands.
#[derive(Debug, Clone, PartialEq, Eq, Subcommand)]
pub enum Command {
    /// Serve the data plane.
    Serve {
        /// TOML configuration file.
        #[arg(long, value_name = "PATH")]
        config: PathBuf,
    },
    /// Load and validate a config, build the gateway, print a summary without
    /// secrets: listen address, each channel's name and `chat_url`, and every
    /// `ChannelWarning`.
    CheckConfig {
        /// TOML configuration file.
        #[arg(long, value_name = "PATH")]
        config: PathBuf,
    },
    /// Print a new virtual key once, plus the `[[keys]]` TOML snippet with its digest.
    Keygen {
        /// Name stored next to the digest in `[[keys]]`; non-empty printable
        /// ASCII.
        #[arg(long, value_name = "NAME")]
        name: String,
    },
}
