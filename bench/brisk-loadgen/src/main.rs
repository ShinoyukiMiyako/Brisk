//! `brisk-loadgen`: the open-loop load generator of the Brisk benchmarks.
//!
//! Each shard is an OS thread with its own `mio` event loop, precise
//! deadline timer (`timerfd` plus a short spin on Linux) and HTTP/1.1
//! keep-alive connection pool. Requests leave at their scheduled time
//! regardless of how the server responds, and every latency is measured
//! from that scheduled time. Streaming responses carry timestamp markers
//! written by `brisk-mock`, which give per-chunk latency, wire time and the
//! mock's own write lag. On Linux every receive is timestamped by the
//! kernel (`SO_TIMESTAMPNS`).
//!
//! Subcommands: `stream` (S1), `nonstream` (S2), `bigbody` (S3),
//! `selfcheck` and `compare`. Each run writes a `RunResult` JSON document
//! and prints a summary.

mod cli;
mod compare;
mod output;
mod report;
mod request;
mod response;
mod run;
mod schedule;
mod shard;
mod target;
mod validity;

use clap::Parser as _;

use crate::cli::{Cli, Command};

fn main() -> anyhow::Result<()> {
    match Cli::parse().command {
        Command::Stream(cmd) => run::stream(&cmd),
        Command::Nonstream(cmd) => run::nonstream(&cmd),
        Command::Bigbody(cmd) => run::bigbody(&cmd),
        Command::Selfcheck(cmd) => run::selfcheck(&cmd),
        Command::Compare(cmd) => compare::run(&cmd),
    }
}
