//! Support shared by the property tests. `oracle` and `sse_ref` are also
//! compiled into the fuzz targets (`fuzz/fuzz_targets/*.rs` include them by
//! path), so they depend on nothing but `brisk-proto`, `bytes`, `serde` and
//! `serde_json`.

#![allow(dead_code, reason = "each test binary uses a different subset")]

pub(crate) mod json;
pub(crate) mod oracle;
pub(crate) mod sse_ref;

/// Default model of every Brisk test (contract 05, 4.1).
pub(crate) const TEST_MODEL: &str = "grok-4.6(xhigh)";
