//! Self-signed certificate generation for TLS benchmark runs.
//!
//! Produces a throwaway CA (`ca.pem`) and a server certificate signed by it
//! (`server.pem`, `server.key`). The load generator and the gateway trust the
//! CA; the mock serves the server certificate.

use std::path::{Path, PathBuf};

use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose,
};

/// File name of the CA certificate written by [`generate`].
pub const CA_CERT_FILE: &str = "ca.pem";
/// File name of the server certificate written by [`generate`].
pub const SERVER_CERT_FILE: &str = "server.pem";
/// File name of the server private key written by [`generate`].
pub const SERVER_KEY_FILE: &str = "server.key";

/// Errors while generating or writing certificates.
#[derive(Debug, thiserror::Error)]
pub enum CertError {
    /// No subject alternative name was given.
    #[error("at least one subject alternative name is required")]
    NoSans,
    /// rcgen rejected a parameter or failed to sign.
    #[error(transparent)]
    Rcgen(#[from] rcgen::Error),
    /// Writing an output file failed.
    #[error("writing {path}: {source}")]
    Io {
        /// The file that failed.
        path: PathBuf,
        /// The underlying error.
        #[source]
        source: std::io::Error,
    },
}

/// A CA and a server certificate signed by it, all PEM encoded.
#[derive(Clone)]
pub struct CertBundle {
    /// The self-signed CA certificate.
    pub ca_cert_pem: String,
    /// The server certificate, signed by the CA.
    pub server_cert_pem: String,
    /// The server's PKCS#8 private key.
    pub server_key_pem: String,
}

impl std::fmt::Debug for CertBundle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CertBundle")
            .field("ca_cert_pem", &self.ca_cert_pem)
            .field("server_cert_pem", &self.server_cert_pem)
            .field("server_key_pem", &"<redacted>")
            .finish()
    }
}

impl CertBundle {
    /// Generates a fresh CA and a server certificate valid for `sans`, each of
    /// which may be a DNS name or an IP address literal.
    pub fn generate(sans: &[String]) -> Result<Self, CertError> {
        let first = sans.first().ok_or(CertError::NoSans)?;

        let mut ca_params = CertificateParams::new(Vec::new())?;
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params
            .distinguished_name
            .push(DnType::CommonName, "Brisk bench CA");
        ca_params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
        ];
        let ca_key = KeyPair::generate()?;
        let ca_cert = ca_params.self_signed(&ca_key)?;
        let issuer = Issuer::new(ca_params, ca_key);

        let mut server_params = CertificateParams::new(sans.to_vec())?;
        server_params
            .distinguished_name
            .push(DnType::CommonName, first.as_str());
        server_params.use_authority_key_identifier_extension = true;
        server_params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let server_key = KeyPair::generate()?;
        let server_cert = server_params.signed_by(&server_key, &issuer)?;

        Ok(Self {
            ca_cert_pem: ca_cert.pem(),
            server_cert_pem: server_cert.pem(),
            server_key_pem: server_key.serialize_pem(),
        })
    }

    /// Writes [`CA_CERT_FILE`], [`SERVER_CERT_FILE`] and [`SERVER_KEY_FILE`]
    /// into `out_dir`, creating the directory if needed.
    pub fn write_to(&self, out_dir: &Path) -> Result<(), CertError> {
        std::fs::create_dir_all(out_dir).map_err(|source| CertError::Io {
            path: out_dir.to_path_buf(),
            source,
        })?;
        write_file(&out_dir.join(CA_CERT_FILE), &self.ca_cert_pem, false)?;
        write_file(
            &out_dir.join(SERVER_CERT_FILE),
            &self.server_cert_pem,
            false,
        )?;
        write_file(&out_dir.join(SERVER_KEY_FILE), &self.server_key_pem, true)
    }
}

fn write_file(path: &Path, contents: &str, private: bool) -> Result<(), CertError> {
    use std::io::Write as _;

    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    if private {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    #[cfg(not(unix))]
    let _ = private;
    let io_err = |source| CertError::Io {
        path: path.to_path_buf(),
        source,
    };
    let mut file = options.open(path).map_err(io_err)?;
    // The open mode only applies when the file is created; an existing key
    // file keeps its old, possibly world-readable, permissions otherwise.
    // Tightening before writing means the key never sits in a readable file.
    #[cfg(unix)]
    if private {
        use std::os::unix::fs::PermissionsExt as _;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))
            .map_err(io_err)?;
    }
    file.write_all(contents.as_bytes()).map_err(io_err)
}

/// Generates a CA and a server certificate for `sans` and writes them to
/// `out_dir` as `ca.pem`, `server.pem` and `server.key`.
pub fn generate(out_dir: &Path, sans: &[String]) -> Result<(), CertError> {
    CertBundle::generate(sans)?.write_to(out_dir)
}
