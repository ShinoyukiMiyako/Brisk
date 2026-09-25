//! The Brisk gateway binary as a library: command line, configuration
//! loading with secret handling, virtual-key generation, runtime assembly,
//! outcome logging and signal handling around `brisk_gateway::Gateway`.
//!
//! It is a library so that the configuration and live upstream tests drive
//! exactly the code the binary runs.

#![forbid(unsafe_code)]

pub mod cli;
pub mod config;
pub mod keygen;
mod logger;
mod runtime;
mod tls;
