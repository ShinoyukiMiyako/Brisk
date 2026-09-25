//! floor-A and floor-B: the empty proxies that bound what the Brisk data
//! plane can cost.
//!
//! `brisk-floor` puts the gateway's inbound connection layer
//! ([`brisk_gateway::server`]) in front of an upstream HTTP/1.1 client with
//! no business logic in between: every request is forwarded unmodified apart
//! from hop-by-hop headers (a request-target that URL parsing would rewrite
//! is refused instead) and every response body is streamed back unmodified.
//! Measuring it against a direct connection to the mock yields the latency
//! floor that any real handler builds on.
//!
//! - [`forward`]: floor-A, which forwards through the gateway's reqwest
//!   client ([`brisk_gateway::upstream`]), and the [`run`](forward::run)
//!   entry point that serves it;
//! - [`same_task`]: floor-B, which drives pooled hyper HTTP/1.1 connections
//!   in the forwarding task itself (experiment E11);
//! - [`tls`]: inbound TLS acceptor and upstream CA loading.

pub mod forward;
pub mod same_task;
pub mod tls;
