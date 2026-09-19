//! Construction of the upstream HTTP client.
//!
//! The client is deliberately conservative: HTTP/1.1 only (R12), never follows
//! redirects and never reads proxy settings from the environment (R24), and has
//! no whole-request or read timeout because streamed responses may legitimately
//! stay open for minutes (R13). Plain `http://` upstreams are fully supported,
//! which is what `CLIProxyAPI` (CPA) exposes on the internal network.

use std::time::Duration;

use reqwest::redirect::Policy;
use reqwest::{Certificate, Client};
use rustls_pki_types::CertificateDer;
use tokio_rustls::rustls;

use crate::net;

/// Settings for [`build_client`].
///
/// [`Default`] yields the values from the data-plane design document.
///
/// Also the key of the client registry: channels with equal configurations
/// share one client and therefore one connection pool.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UpstreamClientConfig {
    /// Upper bound for establishing a TCP connection (and TLS handshake).
    pub connect_timeout: Duration,
    /// How long an idle pooled connection is kept before being closed.
    pub pool_idle_timeout: Duration,
    /// TCP keepalive idle time for upstream sockets.
    pub tcp_keepalive: Duration,
    /// `TCP_USER_TIMEOUT`: how long sent data may stay unacknowledged before
    /// the kernel aborts the connection. Only takes effect on Linux (and
    /// Android/Fuchsia); ignored on other platforms.
    pub tcp_user_timeout: Duration,
    /// Additional trust anchors, merged with the platform roots. Used for the
    /// benchmark mock's self-signed CA.
    pub extra_root_certs: Vec<CertificateDer<'static>>,
    /// Private and loopback upstream addresses are allowed (04, 8.1).
    pub allow_private: bool,
}

impl Default for UpstreamClientConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            pool_idle_timeout: Duration::from_secs(90),
            tcp_keepalive: Duration::from_secs(30),
            tcp_user_timeout: Duration::from_secs(60),
            extra_root_certs: Vec::new(),
            allow_private: false,
        }
    }
}

/// Errors produced by [`build_client`].
#[derive(Debug, thiserror::Error)]
pub enum UpstreamError {
    /// An entry of [`UpstreamClientConfig::extra_root_certs`] is not a usable
    /// trust anchor (malformed DER or not an X.509 certificate).
    #[error("invalid extra root certificate #{index}")]
    InvalidRootCert {
        /// Position of the offending certificate in `extra_root_certs`.
        index: usize,
        /// Why the certificate was rejected.
        #[source]
        source: rustls::Error,
    },
    /// The client could not be built, e.g. the TLS verifier failed to load.
    #[error("failed to build upstream HTTP client")]
    Build(#[source] reqwest::Error),
}

/// Builds the upstream [`reqwest::Client`] used for forwarding.
///
/// Guarantees, independent of reqwest defaults:
/// - HTTP/1.1 only;
/// - redirects are returned to the caller, never followed, so upstream
///   credentials cannot leak to a third-party host;
/// - no proxy, including none picked up from `HTTP_PROXY` and friends;
/// - `TCP_NODELAY` on every upstream socket;
/// - no global timeout and no read timeout;
/// - `extra_root_certs` are trusted in addition to the platform roots.
///
/// reqwest itself adds a default `Accept: */*` request header that cannot be
/// removed at this layer; forwarding code must account for it.
pub fn build_client(config: &UpstreamClientConfig) -> Result<Client, UpstreamError> {
    let extra_roots = config
        .extra_root_certs
        .iter()
        .enumerate()
        .map(|(index, der)| to_trust_anchor(index, der))
        .collect::<Result<Vec<_>, _>>()?;

    let builder = Client::builder()
        .http1_only()
        .redirect(Policy::none())
        .no_proxy()
        .tcp_nodelay(true)
        .tcp_keepalive(config.tcp_keepalive)
        .connect_timeout(config.connect_timeout)
        .pool_idle_timeout(config.pool_idle_timeout)
        .tls_certs_merge(extra_roots);
    net::apply_tcp_user_timeout(builder, config.tcp_user_timeout)
        .build()
        .map_err(UpstreamError::Build)
}

/// reqwest only copies the DER bytes and the rustls verifier parses them much
/// later inside `build()`, losing the position; parsing here first keeps the
/// index in the error.
fn to_trust_anchor(
    index: usize,
    der: &CertificateDer<'static>,
) -> Result<Certificate, UpstreamError> {
    rustls::RootCertStore::empty()
        .add(der.clone())
        .map_err(|source| UpstreamError::InvalidRootCert { index, source })?;
    Certificate::from_der(der.as_ref()).map_err(UpstreamError::Build)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_matches_design_values() {
        let config = UpstreamClientConfig::default();
        assert_eq!(config.connect_timeout, Duration::from_secs(5));
        assert_eq!(config.pool_idle_timeout, Duration::from_secs(90));
        assert_eq!(config.tcp_keepalive, Duration::from_secs(30));
        assert_eq!(config.tcp_user_timeout, Duration::from_secs(60));
        assert!(config.extra_root_certs.is_empty());
        assert!(!config.allow_private);
    }

    #[test]
    fn build_client_with_defaults_succeeds() {
        build_client(&UpstreamClientConfig::default()).unwrap();
    }

    #[test]
    fn malformed_root_cert_reports_its_index() {
        let config = UpstreamClientConfig {
            extra_root_certs: vec![CertificateDer::from(vec![0x30, 0x03, 0x02, 0x01, 0x00])],
            ..UpstreamClientConfig::default()
        };
        let err = build_client(&config).unwrap_err();
        assert!(
            matches!(err, UpstreamError::InvalidRootCert { index: 0, .. }),
            "unexpected error: {err:?}"
        );
    }
}
