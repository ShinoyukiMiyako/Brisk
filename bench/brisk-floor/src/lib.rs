//! floor-A: the empty proxy that bounds what the Brisk data plane can cost.
//!
//! `brisk-floor` puts the gateway's inbound connection layer
//! ([`brisk_gateway::server`]) in front of its upstream HTTP/1.1 client
//! ([`brisk_gateway::upstream`]) with no business logic in between: every
//! request is forwarded unmodified apart from hop-by-hop headers (a
//! request-target that URL parsing would rewrite is refused instead) and every
//! response body is streamed back unmodified. Measuring it against a direct connection to the mock yields the
//! latency floor that any real handler builds on.
//!
//! - [`forward`]: the blind forwarder and the [`run`](forward::run) entry
//!   point that serves it;
//! - [`tls`]: inbound TLS acceptor and upstream CA loading.

pub mod forward;
pub mod tls;
