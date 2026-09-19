//! Upstream side of the gateway: HTTP client construction, the address policy
//! enforced at resolution time, the channel registry and connection warm-up.

pub mod client;
pub mod registry;
pub mod resolver;
pub mod warmup;

pub use client::{UpstreamClientConfig, UpstreamError, build_client};
