//! Brisk data-plane gateway library.
//!
//! M0 scope: the inbound connection layer ([`server`], [`net`]) and the
//! upstream client construction ([`upstream`]). Request handlers and body
//! types build on top of these from M1 on.

#![forbid(unsafe_code)]

pub mod net;
pub mod server;
pub mod upstream;
