//! The target URL: scheme, authority, path and the resolved socket address.

use std::net::{SocketAddr, ToSocketAddrs as _};

/// Path used when the target URL has none.
pub(crate) const DEFAULT_PATH: &str = "/v1/chat/completions";

/// Errors while parsing or resolving the target URL.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub(crate) enum TargetError {
    /// The URL does not start with `http://` or `https://`.
    #[error("target {0:?} must start with http:// or https://")]
    Scheme(String),
    /// The authority is empty or malformed.
    #[error("target {url:?} has an invalid authority: {reason}")]
    Authority {
        /// The URL as given.
        url: String,
        /// What is wrong.
        reason: &'static str,
    },
    /// Name resolution failed or produced no address.
    #[error("cannot resolve {host}:{port}: {reason}")]
    Resolve {
        /// Host part.
        host: String,
        /// Port.
        port: u16,
        /// Resolver message.
        reason: String,
    },
}

/// A parsed target URL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TargetUrl {
    /// `https://`.
    pub(crate) tls: bool,
    /// Host without brackets, for name resolution and TLS server names.
    pub(crate) host: String,
    /// Port, explicit or the scheme default.
    pub(crate) port: u16,
    /// Authority as written, for the `Host` header.
    pub(crate) authority: String,
    /// Request target (path and query).
    pub(crate) path: String,
}

/// A target ready to connect to.
#[derive(Debug, Clone)]
pub(crate) struct Target {
    /// The parsed URL.
    pub(crate) url: TargetUrl,
    /// The address every connection goes to.
    pub(crate) addr: SocketAddr,
}

impl TargetUrl {
    /// Parses `http[s]://host[:port][/path]`. IPv6 hosts must be bracketed.
    pub(crate) fn parse(url: &str) -> Result<Self, TargetError> {
        let (tls, rest) = if let Some(rest) = url.strip_prefix("http://") {
            (false, rest)
        } else if let Some(rest) = url.strip_prefix("https://") {
            (true, rest)
        } else {
            return Err(TargetError::Scheme(url.to_owned()));
        };
        let bad = |reason| TargetError::Authority {
            url: url.to_owned(),
            reason,
        };
        let (authority, path) = match rest.find(['/', '?']) {
            Some(at) => (&rest[..at], &rest[at..]),
            None => (rest, ""),
        };
        if authority.contains('@') {
            return Err(bad("user information is not supported"));
        }
        let (host, port) = if let Some(bracketed) = authority.strip_prefix('[') {
            let (host, after) = bracketed
                .split_once(']')
                .ok_or_else(|| bad("unterminated IPv6 literal"))?;
            match after {
                "" => (host, None),
                _ => (
                    host,
                    Some(
                        after
                            .strip_prefix(':')
                            .ok_or_else(|| bad("garbage after IPv6 literal"))?,
                    ),
                ),
            }
        } else {
            match authority.rsplit_once(':') {
                Some((host, _)) if host.contains(':') => {
                    return Err(bad("IPv6 literals must be bracketed"));
                }
                Some((host, port)) => (host, Some(port)),
                None => (authority, None),
            }
        };
        if host.is_empty() {
            return Err(bad("empty host"));
        }
        let port = match port {
            None => {
                if tls {
                    443
                } else {
                    80
                }
            }
            Some(p) => p
                .parse::<u16>()
                .ok()
                .filter(|&p| p != 0)
                .ok_or_else(|| bad("invalid port"))?,
        };
        let path = match path {
            "" | "/" => DEFAULT_PATH.to_owned(),
            p if p.starts_with('?') => format!("{DEFAULT_PATH}{p}"),
            p => p.to_owned(),
        };
        Ok(Self {
            tls,
            host: host.to_owned(),
            port,
            authority: authority.to_owned(),
            path,
        })
    }
}

impl Target {
    /// Parses and resolves `url`; the first resolved address is used for
    /// every connection.
    pub(crate) fn resolve(url: &str) -> Result<Self, TargetError> {
        let url = TargetUrl::parse(url)?;
        let resolve_err = |reason: String| TargetError::Resolve {
            host: url.host.clone(),
            port: url.port,
            reason,
        };
        let addr = (url.host.as_str(), url.port)
            .to_socket_addrs()
            .map_err(|e| resolve_err(e.to_string()))?
            .next()
            .ok_or_else(|| resolve_err("no address".to_owned()))?;
        Ok(Self { url, addr })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_base_and_full_urls() {
        let t = TargetUrl::parse("http://127.0.0.1:19100").unwrap();
        assert_eq!(
            t,
            TargetUrl {
                tls: false,
                host: "127.0.0.1".into(),
                port: 19100,
                authority: "127.0.0.1:19100".into(),
                path: DEFAULT_PATH.into(),
            }
        );
        let t = TargetUrl::parse("https://mock.local/v1/chat/completions?x=1").unwrap();
        assert!(t.tls);
        assert_eq!(t.port, 443);
        assert_eq!(t.authority, "mock.local");
        assert_eq!(t.path, "/v1/chat/completions?x=1");
        let t = TargetUrl::parse("http://[::1]:8080/").unwrap();
        assert_eq!((t.host.as_str(), t.port), ("::1", 8080));
        assert_eq!(t.authority, "[::1]:8080");
        assert_eq!(t.path, DEFAULT_PATH);
        let t = TargetUrl::parse("http://h?q").unwrap();
        assert_eq!(t.path, "/v1/chat/completions?q");
    }

    #[test]
    fn rejects_malformed_urls() {
        for url in [
            "ftp://h",
            "h:80",
            "http://",
            "http://:80",
            "http://h:0",
            "http://h:99999",
            "http://u@h",
            "http://::1:80",
            "http://[::1",
            "http://[::1]x",
        ] {
            assert!(TargetUrl::parse(url).is_err(), "{url}");
        }
    }

    #[test]
    fn resolves_literal_addresses() {
        let t = Target::resolve("http://127.0.0.1:19100").unwrap();
        assert_eq!(t.addr, "127.0.0.1:19100".parse().unwrap());
    }
}
