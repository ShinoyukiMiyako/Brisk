//! Shared low-level building blocks for the Brisk benchmark tools.
//!
//! `brisk-mock` and `brisk-loadgen` run one OS thread per shard, each with its
//! own `mio::Poll`. This crate supplies everything those event loops share:
//!
//! - [`clock`]: `CLOCK_MONOTONIC` nanoseconds and realtime→monotonic
//!   conversion for kernel receive timestamps;
//! - [`precise`]: microsecond-accurate deadlines inside a poll loop
//!   (`timerfd` plus a short spin) and a blocking `sleep_until`;
//! - [`wire`]: the in-band timestamp marker and the request parameter
//!   directive, plus `OpenAI` Chat request bodies;
//! - [`http1`]: HTTP/1.1 heads, body framing and a zero-copy chunked decoder;
//! - [`transport`]: plaintext/TLS connections on nonblocking sockets with
//!   `SO_TIMESTAMPNS` receive timestamps, listener setup, certificates;
//! - [`dist`]: truncated log-normal stream lifetimes and Poisson arrivals;
//! - [`stats`]: per-interval HDR histograms and block-bootstrap comparison;
//! - [`result`] and [`fingerprint`]: the JSON result document;
//! - [`cpu`]: CPU lists and thread pinning;
//! - [`rlimit`]: raising the open-file limit.
//!
//! Precise timing and kernel timestamps exist only on Linux. Other platforms
//! compile and behave correctly for functional tests, without precision
//! guarantees.

pub mod clock;
pub mod cpu;
pub mod dist;
pub mod fingerprint;
pub mod http1;
pub mod precise;
pub mod result;
pub mod rlimit;
pub mod stats;
pub mod transport;
pub mod wire;
