//! The inbound TLS acceptor.
//!
//! Same construction as floor-A's (aws-lc-rs provider, safe default protocol
//! versions, no client authentication), written separately because `brisk`
//! must not depend on the benchmark crates.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rustls_pki_types::pem::{self, PemObject as _};
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::{self, ServerConfig};

use crate::config::TlsFiles;

/// ALPN protocols offered to inbound clients, in server preference order; the
/// connection layer serves both.
pub(crate) const INBOUND_ALPN: [&[u8]; 2] = [b"h2", b"http/1.1"];

/// Why the inbound TLS configuration could not be built.
#[derive(Debug, thiserror::Error)]
pub(crate) enum TlsError {
    /// The certificate file is unreadable or malformed.
    #[error("cannot load TLS certificates from {}", path.display())]
    Certificate {
        /// The certificate file.
        path: PathBuf,
        /// The underlying error.
        source: pem::Error,
    },
    /// The certificate file holds no certificate.
    #[error("no certificate found in {}", path.display())]
    NoCertificate {
        /// The certificate file.
        path: PathBuf,
    },
    /// The private key file could not be read.
    #[error("cannot read TLS key {}", path.display())]
    KeyIo {
        /// The key file.
        path: PathBuf,
        /// The underlying error.
        source: io::Error,
    },
    /// The private key file holds no usable key. The PEM parser's own error
    /// is not kept: its base64 diagnostics describe bytes of the key file.
    #[error("TLS key {}: {reason}", path.display())]
    Key {
        /// The key file.
        path: PathBuf,
        /// What was wrong.
        reason: &'static str,
    },
    /// rustls rejected the certificate and key, e.g. they do not match.
    #[error("the TLS certificate and key were rejected")]
    Rustls(#[source] rustls::Error),
}

/// Builds the acceptor from a PEM certificate chain (leaf first) and its PEM
/// private key.
pub(crate) fn acceptor(files: &TlsFiles) -> Result<TlsAcceptor, TlsError> {
    let certs = load_certs(&files.cert)?;
    let key = load_key(&files.key)?;
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let mut config = ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(TlsError::Rustls)?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(TlsError::Rustls)?;
    config.alpn_protocols = INBOUND_ALPN.iter().map(|proto| proto.to_vec()).collect();
    Ok(TlsAcceptor::from(Arc::new(config)))
}

fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, TlsError> {
    let error = |source| TlsError::Certificate {
        path: path.to_path_buf(),
        source,
    };
    let certs = CertificateDer::pem_file_iter(path)
        .map_err(error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(error)?;
    if certs.is_empty() {
        return Err(TlsError::NoCertificate {
            path: path.to_path_buf(),
        });
    }
    Ok(certs)
}

fn load_key(path: &Path) -> Result<PrivateKeyDer<'static>, TlsError> {
    PrivateKeyDer::from_pem_file(path).map_err(|err| match err {
        pem::Error::Io(source) => TlsError::KeyIo {
            path: path.to_path_buf(),
            source,
        },
        pem::Error::NoItemsFound => TlsError::Key {
            path: path.to_path_buf(),
            reason: "no private key found",
        },
        _ => TlsError::Key {
            path: path.to_path_buf(),
            reason: "malformed PEM",
        },
    })
}

#[cfg(test)]
mod tests {
    use rustls_pki_types::ServerName;
    use tokio::net::{TcpListener, TcpStream};
    use tokio_rustls::TlsConnector;
    use tokio_rustls::rustls::{ClientConfig, RootCertStore};

    use super::*;

    /// Self-signed P-256 end-entity certificate for `localhost`, valid until
    /// 2126; test material only.
    const CERT_PEM: &str = "-----BEGIN CERTIFICATE-----
MIIBuDCCAV+gAwIBAgIUXsfLXsgZaYwGl11TTQjWsMyyqV4wCgYIKoZIzj0EAwIw
FDESMBAGA1UEAwwJbG9jYWxob3N0MCAXDTI2MDkxOTEzMzAxMloYDzIxMjYwODI2
MTMzMDEyWjAUMRIwEAYDVQQDDAlsb2NhbGhvc3QwWTATBgcqhkjOPQIBBggqhkjO
PQMBBwNCAASrldOJF9nKGCFh+pChEbyx+PGi5vJbRR7jOXPqltwRzwDQufBBQKWg
cuYmuVafqOso38OO9iq6f3Zb4RmURh4Ao4GMMIGJMB0GA1UdDgQWBBSDnnfakiQN
pmtLrjzU765j9mMchTAfBgNVHSMEGDAWgBSDnnfakiQNpmtLrjzU765j9mMchTAU
BgNVHREEDTALgglsb2NhbGhvc3QwDAYDVR0TAQH/BAIwADAOBgNVHQ8BAf8EBAMC
B4AwEwYDVR0lBAwwCgYIKwYBBQUHAwEwCgYIKoZIzj0EAwIDRwAwRAIge9OlM9/8
LrXfZOG22zaiM7D/202t8i707sZkljieD7sCIGlhT4LplDyqsoO23EAx0AjOCQ2a
DN/cZ0O6hhVqXyVN
-----END CERTIFICATE-----
";

    /// The key of [`CERT_PEM`]; test material only.
    const KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgmPbhLbDR/HLa13Fn
X/D6JwJLDAUkp7EMndkpEFCXzcahRANCAASrldOJF9nKGCFh+pChEbyx+PGi5vJb
RR7jOXPqltwRzwDQufBBQKWgcuYmuVafqOso38OO9iq6f3Zb4RmURh4A
-----END PRIVATE KEY-----
";

    /// A directory under the system temp dir, removed on drop.
    struct TempDir(PathBuf);

    impl TempDir {
        fn new(test: &str) -> Self {
            let dir = std::env::temp_dir().join(format!("brisk-tls-{test}-{}", std::process::id()));
            std::fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }

        fn write(&self, name: &str, contents: &str) -> PathBuf {
            let path = self.0.join(name);
            std::fs::write(&path, contents).unwrap();
            path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// The error of a build that must fail; `TlsAcceptor` has no `Debug`, so
    /// `unwrap_err` is unavailable.
    fn rejection(files: &TlsFiles) -> TlsError {
        match acceptor(files) {
            Ok(_) => panic!("the TLS files were accepted"),
            Err(err) => err,
        }
    }

    fn client_config(alpn: &[&[u8]]) -> ClientConfig {
        let mut roots = RootCertStore::empty();
        roots
            .add(CertificateDer::from_pem_slice(CERT_PEM.as_bytes()).unwrap())
            .unwrap();
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let mut config = ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
        config.alpn_protocols = alpn.iter().map(|proto| proto.to_vec()).collect();
        config
    }

    async fn negotiated_alpn(acceptor: TlsAcceptor, client_alpn: &[&[u8]]) -> Option<Vec<u8>> {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = async {
            let (stream, _) = listener.accept().await.unwrap();
            let tls = acceptor.accept(stream).await.unwrap();
            tls.get_ref().1.alpn_protocol().map(<[u8]>::to_vec)
        };
        let client = async {
            let connector = TlsConnector::from(Arc::new(client_config(client_alpn)));
            let stream = TcpStream::connect(addr).await.unwrap();
            let name = ServerName::try_from("localhost").unwrap();
            connector.connect(name, stream).await.unwrap()
        };
        let (negotiated, _client) = tokio::join!(server, client);
        negotiated
    }

    #[tokio::test]
    async fn handshake_prefers_h2_and_falls_back_to_http1() {
        let dir = TempDir::new("handshake");
        let files = TlsFiles {
            cert: dir.write("cert.pem", CERT_PEM),
            key: dir.write("key.pem", KEY_PEM),
        };
        let acceptor = acceptor(&files).unwrap();
        assert_eq!(
            negotiated_alpn(acceptor.clone(), &[b"http/1.1", b"h2"]).await,
            Some(b"h2".to_vec())
        );
        assert_eq!(
            negotiated_alpn(acceptor, &[b"http/1.1"]).await,
            Some(b"http/1.1".to_vec())
        );
    }

    #[test]
    fn missing_files_name_the_path() {
        let dir = TempDir::new("missing");
        let cert = dir.write("cert.pem", CERT_PEM);
        let files = TlsFiles {
            cert: cert.clone(),
            key: dir.0.join("absent.pem"),
        };
        let err = rejection(&files);
        assert!(matches!(err, TlsError::KeyIo { .. }), "{err:?}");
        assert!(err.to_string().contains("absent.pem"), "{err}");

        let files = TlsFiles {
            cert: dir.0.join("absent-cert.pem"),
            key: cert,
        };
        let err = rejection(&files);
        assert!(matches!(err, TlsError::Certificate { .. }), "{err:?}");
    }

    #[test]
    fn files_without_items_are_rejected() {
        let dir = TempDir::new("empty");
        let empty = dir.write("empty.pem", "");
        let files = TlsFiles {
            cert: empty.clone(),
            key: dir.write("key.pem", KEY_PEM),
        };
        assert!(matches!(rejection(&files), TlsError::NoCertificate { .. }));

        let files = TlsFiles {
            cert: dir.write("cert.pem", CERT_PEM),
            key: empty,
        };
        assert!(matches!(
            rejection(&files),
            TlsError::Key {
                reason: "no private key found",
                ..
            }
        ));
    }

    #[test]
    fn malformed_key_errors_do_not_describe_key_bytes() {
        let dir = TempDir::new("malformed");
        let files = TlsFiles {
            cert: dir.write("cert.pem", CERT_PEM),
            key: dir.write(
                "key.pem",
                "-----BEGIN PRIVATE KEY-----\n!!!!secret-ish!!!!\n-----END PRIVATE KEY-----\n",
            ),
        };
        let err = rejection(&files);
        let chain = format!("{err} {err:?}");
        assert!(chain.contains("malformed PEM"), "{chain}");
        assert!(!chain.contains("secret-ish"), "{chain}");
    }

    #[test]
    fn a_key_that_does_not_match_the_certificate_is_rejected() {
        let dir = TempDir::new("mismatch");
        // A different, valid P-256 key.
        let other_key = "-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgjQq3ZPK76VdxUz/C
qF7MmQjyDNIkahf+Fxahp3NxLmmhRANCAATiN1kOnbp8OFPuQSDV+dPDSnMFSv5y
7qoWRHrZoBiHpdqsHVGvwf8cQrxL59HiX+R9m0MGrda04ETsNOzXCPXS
-----END PRIVATE KEY-----
";
        let files = TlsFiles {
            cert: dir.write("cert.pem", CERT_PEM),
            key: dir.write("key.pem", other_key),
        };
        assert!(matches!(rejection(&files), TlsError::Rustls(_)));
    }

    #[test]
    fn every_variant_keeps_its_message_and_source() {
        use std::error::Error as _;

        let path = PathBuf::from("dir/tls.pem");
        let cases: [(TlsError, &str, bool); 5] = [
            (
                TlsError::Certificate {
                    path: path.clone(),
                    source: pem::Error::NoItemsFound,
                },
                "cannot load TLS certificates from dir/tls.pem",
                true,
            ),
            (
                TlsError::NoCertificate { path: path.clone() },
                "no certificate found in dir/tls.pem",
                false,
            ),
            (
                TlsError::KeyIo {
                    path: path.clone(),
                    source: io::Error::other("denied"),
                },
                "cannot read TLS key dir/tls.pem",
                true,
            ),
            (
                TlsError::Key {
                    path,
                    reason: "malformed PEM",
                },
                "TLS key dir/tls.pem: malformed PEM",
                false,
            ),
            (
                TlsError::Rustls(rustls::Error::NoCertificatesPresented),
                "the TLS certificate and key were rejected",
                true,
            ),
        ];
        for (err, message, has_source) in cases {
            assert_eq!(err.to_string(), message);
            assert_eq!(err.source().is_some(), has_source, "{message}");
        }
    }
}
