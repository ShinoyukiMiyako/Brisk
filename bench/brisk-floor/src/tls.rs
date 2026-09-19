//! TLS material for floor-A: the inbound acceptor and the upstream trust
//! anchors.
//!
//! Certificates and keys are parsed by the shared benchmark helpers, so floor
//! accepts exactly the files `brisk-mock gen-cert` writes.

use std::path::Path;
use std::sync::Arc;

use brisk_bench_core::transport::tls::{self as bench_tls, TlsError};
use rustls_pki_types::CertificateDer;
use rustls_pki_types::pem::PemObject as _;
use tokio_rustls::TlsAcceptor;

/// ALPN protocols offered to inbound clients, in server preference order.
///
/// The gateway's connection layer serves both HTTP/2 and HTTP/1.1, so floor
/// offers both; the shared benchmark config offers only `http/1.1` because
/// the mock and the load generator speak nothing else.
pub const INBOUND_ALPN: [&[u8]; 2] = [b"h2", b"http/1.1"];

/// Builds the inbound TLS acceptor from a PEM certificate chain (leaf first)
/// and its PEM private key, using the aws-lc-rs provider.
pub fn acceptor(cert_pem: &Path, key_pem: &Path) -> Result<TlsAcceptor, TlsError> {
    let mut config = Arc::unwrap_or_clone(bench_tls::server_config(cert_pem, key_pem)?);
    config.alpn_protocols = INBOUND_ALPN.iter().map(|proto| proto.to_vec()).collect();
    Ok(TlsAcceptor::from(Arc::new(config)))
}

/// Reads every certificate from a PEM file, for use as additional upstream
/// trust anchors.
pub fn load_ca_certs(ca_pem: &Path) -> Result<Vec<CertificateDer<'static>>, TlsError> {
    let what = ca_pem.display().to_string();
    let pem_error = |source| TlsError::Pem {
        what: what.clone(),
        source,
    };
    let certs = CertificateDer::pem_file_iter(ca_pem)
        .map_err(pem_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(pem_error)?;
    if certs.is_empty() {
        return Err(TlsError::NoCertificates(what));
    }
    Ok(certs)
}
