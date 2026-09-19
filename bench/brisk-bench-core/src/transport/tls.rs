//! rustls configurations for the benchmark tools.
//!
//! Both sides use the aws-lc-rs crypto provider and advertise only
//! `http/1.1` via ALPN. The client trusts exactly the given CA, which is the
//! self-signed CA produced by [`super::certs`].

use std::path::{Path, PathBuf};
use std::sync::Arc;

use rustls::pki_types::pem::PemObject as _;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig, RootCertStore, ServerConfig};

/// The only ALPN protocol either side offers.
pub const ALPN_HTTP11: &[u8] = b"http/1.1";

/// Errors while building TLS configurations.
#[derive(Debug, thiserror::Error)]
pub enum TlsError {
    /// A PEM file could not be read.
    #[error("reading {path}: {source}")]
    Io {
        /// The file that failed.
        path: PathBuf,
        /// The underlying error.
        #[source]
        source: std::io::Error,
    },
    /// PEM data is malformed.
    #[error("parsing PEM {what}: {source}")]
    Pem {
        /// What was being parsed (file name or description).
        what: String,
        /// The underlying error.
        #[source]
        source: rustls::pki_types::pem::Error,
    },
    /// PEM data contains no certificate.
    #[error("no certificate found in {0}")]
    NoCertificates(String),
    /// rustls rejected the configuration or a certificate.
    #[error(transparent)]
    Rustls(#[from] rustls::Error),
    /// The host name is neither a valid DNS name nor an IP address.
    #[error("invalid TLS server name {0:?}")]
    InvalidServerName(String),
}

fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::aws_lc_rs::default_provider())
}

fn read_file(path: &Path) -> Result<Vec<u8>, TlsError> {
    std::fs::read(path).map_err(|source| TlsError::Io {
        path: path.to_path_buf(),
        source,
    })
}

fn parse_certs(pem: &[u8], what: &str) -> Result<Vec<CertificateDer<'static>>, TlsError> {
    let certs = CertificateDer::pem_slice_iter(pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| TlsError::Pem {
            what: what.to_owned(),
            source,
        })?;
    if certs.is_empty() {
        return Err(TlsError::NoCertificates(what.to_owned()));
    }
    Ok(certs)
}

/// Builds a server configuration from PEM files: the certificate chain
/// (leaf first) and its private key.
pub fn server_config(cert_pem: &Path, key_pem: &Path) -> Result<Arc<ServerConfig>, TlsError> {
    let certs = read_file(cert_pem)?;
    let key = read_file(key_pem)?;
    server_config_with(
        &certs,
        &cert_pem.display().to_string(),
        &key,
        &key_pem.display().to_string(),
    )
}

/// Builds a server configuration from in-memory PEM data.
pub fn server_config_from_pem(
    cert_pem: &[u8],
    key_pem: &[u8],
) -> Result<Arc<ServerConfig>, TlsError> {
    server_config_with(cert_pem, "certificate PEM", key_pem, "key PEM")
}

fn server_config_with(
    cert_pem: &[u8],
    cert_what: &str,
    key_pem: &[u8],
    key_what: &str,
) -> Result<Arc<ServerConfig>, TlsError> {
    let certs = parse_certs(cert_pem, cert_what)?;
    let key = PrivateKeyDer::from_pem_slice(key_pem).map_err(|source| TlsError::Pem {
        what: key_what.to_owned(),
        source,
    })?;
    let mut config = ServerConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()?
        .with_no_client_auth()
        .with_single_cert(certs, key)?;
    config.alpn_protocols = vec![ALPN_HTTP11.to_vec()];
    Ok(Arc::new(config))
}

/// Builds a client configuration that trusts only the CA certificates in
/// `ca_pem`.
pub fn client_config(ca_pem: &Path) -> Result<Arc<ClientConfig>, TlsError> {
    let pem = read_file(ca_pem)?;
    client_config_with(&pem, &ca_pem.display().to_string())
}

/// Builds a client configuration from in-memory CA PEM data.
pub fn client_config_from_pem(ca_pem: &[u8]) -> Result<Arc<ClientConfig>, TlsError> {
    client_config_with(ca_pem, "CA PEM")
}

fn client_config_with(ca_pem: &[u8], what: &str) -> Result<Arc<ClientConfig>, TlsError> {
    let mut roots = RootCertStore::empty();
    for cert in parse_certs(ca_pem, what)? {
        roots.add(cert)?;
    }
    let mut config = ClientConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()?
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = vec![ALPN_HTTP11.to_vec()];
    Ok(Arc::new(config))
}

/// Parses a host (DNS name or IP literal) into an owned rustls server name.
pub fn server_name(host: &str) -> Result<ServerName<'static>, TlsError> {
    ServerName::try_from(host)
        .map(|name| name.to_owned())
        .map_err(|_| TlsError::InvalidServerName(host.to_owned()))
}
