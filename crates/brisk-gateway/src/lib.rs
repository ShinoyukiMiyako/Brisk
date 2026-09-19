//! Brisk data-plane gateway library.
//!
//! - Inbound: the connection layer ([`server`], [`net`]), virtual-key
//!   authentication ([`auth`]) and request-body intake ([`ingress`]).
//! - Upstream: client construction, address policy, channels and warm-up
//!   ([`upstream`]).
//! - Responses: the streamed and tapped response bodies ([`body`]).
//! - Plain data shared by all of them: the gateway's input ([`spec`]),
//!   settlement records ([`outcome`]), the in-flight byte budget ([`budget`])
//!   and secret handling ([`secret`]).

#![forbid(unsafe_code)]

/// Error type accepted from bodies and services.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

pub mod auth;
pub mod body;
pub mod budget;
mod forward;
mod gateway;
mod headers;
pub mod ingress;
pub mod net;
pub mod outcome;
mod reply;
mod router;
pub mod secret;
mod select;
pub mod server;
pub mod spec;
mod state;
pub mod upstream;
