//! Address policy for upstream connections (04, 8.1; R25).
//!
//! Every upstream address is classified as public, private, loopback or
//! always denied (link-local, cloud metadata, multicast, reserved); private
//! and loopback addresses are reachable only for channels that opt in. The
//! policy is applied twice: by a DNS resolver that filters what the system
//! resolver returns, and at configuration time to IP-literal base URLs,
//! which never reach a resolver.
