//! Paced `OpenAI`-compatible upstream for Brisk benchmarks.
//!
//! The mock answers `POST /v1/chat/completions` with Server-Sent Events whose
//! timing is planned up front: chunk `k` of a stream is due at
//! `t0 + ttft + k × interval`, where `t0` is the moment the request body was
//! complete. Every content chunk carries an in-band marker
//! ([`brisk_bench_core::wire`]) with the planned time `t_sched` and the time
//! `t_write` taken immediately before the write call, so the load generator
//! can separate mock lateness from network and gateway latency.
//!
//! Architecture: one OS thread per shard, each with its own `mio::Poll`, its
//! own listener (`SO_REUSEPORT` on Linux when sharded), optional CPU pinning
//! and a min-heap of `(t_sched, stream, seq)` entries waited on with
//! [`brisk_bench_core::precise::DeadlineTimer`]. Requests pick their timing
//! through a `bench:v1;` directive in the first system message; requests
//! without one use the server defaults.
//!
//! A shard serves sockets while it waits for its next emission; how close to
//! the emission it keeps doing so is its emission policy ([`EmitPolicy`]).
//! The default, [`EmitPolicy::Fixed`], keeps serving them until the emission
//! is within a fixed commit window.
//!
//! Endpoints: `POST /v1/chat/completions`, `GET /v1/models`,
//! `GET /__bench/stats` and `POST /__bench/reset`.

mod emit;
mod request;
mod response;
pub mod server;
mod shard;
pub mod stats;

pub use emit::{DEFAULT_COMMIT_WINDOW, EmitPolicy, EmitSettings};
pub use server::{MockConfig, ParamsError, ServerError, ServerHandle};
pub use stats::Snapshot;
