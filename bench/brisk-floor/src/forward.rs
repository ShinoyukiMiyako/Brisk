//! The blind forwarder.
//!
//! Each inbound request is sent upstream with the same method, the upstream
//! base URL joined with the original path and query, and the original headers
//! minus hop-by-hop headers and `Host` (reqwest derives `Host` from the URL).
//! Request and response bodies are streamed frame by frame in both directions;
//! nothing is buffered, so the timing of every upstream write reaches the
//! client unchanged apart from the forwarding cost being measured.
//!
//! reqwest only accepts a parsed [`Url`], and WHATWG URL parsing rewrites some
//! request-targets (dot segments are resolved, some characters are
//! percent-encoded). Such a target cannot be forwarded unchanged, so it is
//! refused with `400 Bad Request` instead of being silently rewritten; this
//! also keeps `..` segments from escaping the configured base path.

use std::convert::Infallible;
use std::future::Future;
use std::io;
use std::str::FromStr;
use std::sync::Arc;

use brisk_gateway::server::{ServerConfig, serve};
use http::header::{
    CONNECTION, CONTENT_TYPE, COOKIE, HOST, HeaderName, HeaderValue, PROXY_AUTHENTICATE,
    PROXY_AUTHORIZATION, TE, TRANSFER_ENCODING, UPGRADE,
};
use http::{HeaderMap, Request, Response, StatusCode, Version};
use hyper::body::Incoming;
use hyper::service::service_fn;
use reqwest::{Body, Client, Url};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

/// Body of the response returned when the upstream request fails before any
/// response head arrived.
pub const BAD_GATEWAY_BODY: &str = "brisk-floor: upstream request failed\n";

/// Body of the response returned for a request-target that URL parsing would
/// rewrite, so it cannot be forwarded unchanged.
pub const BAD_TARGET_BODY: &str = "brisk-floor: request-target would be rewritten\n";

/// `Keep-Alive`, which `http` has no constant for.
const KEEP_ALIVE: HeaderName = HeaderName::from_static("keep-alive");

/// `Proxy-Connection`, a non-standard but widely sent hop-by-hop header.
const PROXY_CONNECTION: HeaderName = HeaderName::from_static("proxy-connection");

/// Headers that describe a single transport hop and must never be forwarded:
/// the RFC 9110 (section 7.6.1) set plus the proxy authentication headers,
/// which apply to the next hop only (RFC 9110, sections 11.7.1 and 11.7.2).
///
/// `Trailer` is deliberately absent: it is end-to-end, and hyper's HTTP/1.1
/// encoder only emits the trailer fields a response's `Trailer` header
/// declares.
const HOP_BY_HOP: [HeaderName; 8] = [
    CONNECTION,
    KEEP_ALIVE,
    PROXY_CONNECTION,
    PROXY_AUTHENTICATE,
    PROXY_AUTHORIZATION,
    TE,
    TRANSFER_ENCODING,
    UPGRADE,
];

/// A rejected `--upstream` base URL.
#[derive(Debug, thiserror::Error)]
pub enum UpstreamUrlError {
    /// The value is not an absolute URL.
    #[error("invalid upstream URL {url:?}")]
    Parse {
        /// The rejected input.
        url: String,
        /// Why parsing failed.
        #[source]
        source: <Url as FromStr>::Err,
    },
    /// The URL is well formed but cannot serve as a forwarding base.
    #[error("upstream URL {url:?} {reason}")]
    Unsupported {
        /// The rejected input.
        url: String,
        /// What disqualifies it.
        reason: &'static str,
    },
}

/// Forwards requests to one upstream base URL. Cloning is cheap and shares
/// the underlying connection pool.
#[derive(Debug, Clone)]
pub struct Forwarder {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    client: Client,
    /// Upstream base URL without a trailing slash, so the inbound path (which
    /// always starts with `/`) can be appended directly.
    base: String,
}

impl Forwarder {
    /// Creates a forwarder that sends every request through `client` to
    /// `upstream` joined with the request's path and query.
    ///
    /// `upstream` must be an `http` or `https` URL with a host and without
    /// credentials, a query or a fragment. A path is allowed and becomes a
    /// prefix of every forwarded path.
    pub fn new(client: Client, upstream: &str) -> Result<Self, UpstreamUrlError> {
        let url = Url::parse(upstream).map_err(|source| UpstreamUrlError::Parse {
            url: upstream.to_owned(),
            source,
        })?;
        let unsupported = |reason| UpstreamUrlError::Unsupported {
            url: upstream.to_owned(),
            reason,
        };
        if !matches!(url.scheme(), "http" | "https") {
            return Err(unsupported("must use the http or https scheme"));
        }
        if !url.has_host() {
            return Err(unsupported("must have a host"));
        }
        if url.query().is_some() || url.fragment().is_some() {
            return Err(unsupported("must not have a query or fragment"));
        }
        // `Client::execute` does not turn URL userinfo into `Authorization`
        // the way the request builder does, so credentials there would be
        // silently dropped; clients send their own credentials anyway.
        if !url.username().is_empty() || url.password().is_some() {
            return Err(unsupported("must not contain credentials"));
        }
        let base = url.as_str().trim_end_matches('/').to_owned();
        Ok(Self {
            inner: Arc::new(Inner { client, base }),
        })
    }

    /// The normalized upstream base URL requests are forwarded to.
    pub fn upstream_base(&self) -> &str {
        &self.inner.base
    }

    /// Forwards one request and returns the upstream response with its body
    /// still streaming.
    ///
    /// Only request-targets that URL parsing leaves unchanged are forwarded;
    /// any other (dot segments, characters the URL standard percent-encodes)
    /// gets `400 Bad Request`. A `TE: trailers` from the client is kept so the
    /// upstream may still send trailers, and on an HTTP/2 inbound connection
    /// split `Cookie` fields are joined into one (RFC 9113, section 8.2.3)
    /// because the upstream hop is HTTP/1.1.
    ///
    /// Never fails: an upstream error before the response head (connect
    /// failure, reset, invalid response) becomes `502 Bad Gateway` with a
    /// short text body. An error after the head surfaces as a body error,
    /// which makes hyper abort the inbound response.
    pub async fn forward(&self, req: Request<Incoming>) -> Response<Body> {
        let (parts, body) = req.into_parts();
        let target = parts.uri.path_and_query().map_or("/", |pq| pq.as_str());
        let mut raw = String::with_capacity(self.inner.base.len() + target.len());
        raw.push_str(&self.inner.base);
        raw.push_str(target);
        let url = match Url::parse(&raw) {
            Ok(url) if url.as_str() == raw => url,
            parsed => {
                tracing::warn!(
                    request_target = target,
                    rewritten = parsed.as_ref().ok().map(Url::as_str),
                    "request-target would be rewritten; refusing"
                );
                return text_response(StatusCode::BAD_REQUEST, BAD_TARGET_BODY);
            }
        };

        let mut headers = parts.headers;
        let accepts_trailers = te_accepts_trailers(&headers);
        strip_hop_by_hop(&mut headers);
        headers.remove(HOST);
        if accepts_trailers {
            headers.insert(TE, HeaderValue::from_static("trailers"));
        }
        if parts.version == Version::HTTP_2 {
            join_cookies(&mut headers);
        }

        let mut upstream = reqwest::Request::new(parts.method, url);
        // Moved as a whole: `RequestBuilder::headers` would re-insert every
        // entry into a fresh map.
        *upstream.headers_mut() = headers;
        *upstream.body_mut() = Some(Body::wrap(body));
        match self.inner.client.execute(upstream).await {
            Ok(upstream) => {
                let mut response = Response::<Body>::from(upstream);
                strip_hop_by_hop(response.headers_mut());
                // The upstream's HTTP version describes the upstream hop only;
                // hyper picks the version of the inbound connection itself.
                *response.version_mut() = Version::default();
                response
            }
            Err(err) => {
                tracing::warn!(error = ?err, "upstream request failed");
                text_response(StatusCode::BAD_GATEWAY, BAD_GATEWAY_BODY)
            }
        }
    }
}

/// Removes hop-by-hop headers, including every header nominated by
/// `Connection` (RFC 9110, section 7.6.1).
pub fn strip_hop_by_hop(headers: &mut HeaderMap) {
    // Nominated names must be read before `Connection` itself is removed. The
    // vector stays unallocated in the common case of no `Connection` header.
    let nominated: Vec<HeaderName> = headers
        .get_all(CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .filter_map(|token| HeaderName::from_bytes(token.trim().as_bytes()).ok())
        .collect();
    for name in nominated {
        headers.remove(name);
    }
    for name in HOP_BY_HOP {
        headers.remove(name);
    }
}

/// Whether any `TE` field lists the `trailers` token.
fn te_accepts_trailers(headers: &HeaderMap) -> bool {
    headers
        .get_all(TE)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .any(|member| {
            let token = member.split(';').next().unwrap_or_default().trim();
            token.eq_ignore_ascii_case("trailers")
        })
}

/// Joins multiple `Cookie` fields into one, separated by `"; "`, as RFC 9113
/// (section 8.2.3) requires before forwarding HTTP/2 cookies over HTTP/1.1.
fn join_cookies(headers: &mut HeaderMap) {
    let mut values = headers.get_all(COOKIE).iter();
    let Some(first) = values.next() else {
        return;
    };
    let rest: Vec<&HeaderValue> = values.collect();
    if rest.is_empty() {
        return;
    }
    let mut joined = first.as_bytes().to_vec();
    for value in rest {
        joined.extend_from_slice(b"; ");
        joined.extend_from_slice(value.as_bytes());
    }
    let joined = HeaderValue::from_bytes(&joined)
        .expect("valid field values joined by \"; \" form a valid field value");
    headers.insert(COOKIE, joined);
}

fn text_response(status: StatusCode, body: &'static str) -> Response<Body> {
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = status;
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response
}

/// Serves `forwarder` on `listener` through the gateway's connection layer
/// until `shutdown` completes, then drains in-flight connections as
/// [`brisk_gateway::server::serve`] describes.
pub async fn run(
    listener: TcpListener,
    tls: Option<TlsAcceptor>,
    config: ServerConfig,
    forwarder: Forwarder,
    shutdown: impl Future<Output = ()>,
) -> io::Result<()> {
    let service = service_fn(move |req| {
        let forwarder = forwarder.clone();
        async move { Ok::<_, Infallible>(forwarder.forward(req).await) }
    });
    serve(listener, tls, config, service, shutdown).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client() -> Client {
        brisk_gateway::upstream::build_client(
            &brisk_gateway::upstream::UpstreamClientConfig::default(),
        )
        .unwrap()
    }

    #[test]
    fn strips_standard_and_nominated_hop_by_hop_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(CONNECTION, HeaderValue::from_static("close, X-Hop"));
        headers.append(CONNECTION, HeaderValue::from_static("x-other-hop"));
        headers.insert("x-hop", HeaderValue::from_static("1"));
        headers.insert("x-other-hop", HeaderValue::from_static("1"));
        headers.insert(KEEP_ALIVE, HeaderValue::from_static("timeout=5"));
        headers.insert(TRANSFER_ENCODING, HeaderValue::from_static("chunked"));
        headers.insert(TE, HeaderValue::from_static("trailers"));
        headers.insert(UPGRADE, HeaderValue::from_static("websocket"));
        headers.insert(PROXY_AUTHORIZATION, HeaderValue::from_static("Basic x"));
        headers.insert("trailer", HeaderValue::from_static("x-checksum"));
        headers.insert("authorization", HeaderValue::from_static("Bearer k"));
        headers.append("x-multi", HeaderValue::from_static("a"));
        headers.append("x-multi", HeaderValue::from_static("b"));
        headers.insert(HOST, HeaderValue::from_static("inbound.example"));

        strip_hop_by_hop(&mut headers);

        let mut names: Vec<&str> = headers.keys().map(HeaderName::as_str).collect();
        names.sort_unstable();
        assert_eq!(names, ["authorization", "host", "trailer", "x-multi"]);
        let multi: Vec<_> = headers.get_all("x-multi").iter().collect();
        assert_eq!(multi, ["a", "b"]);
    }

    #[test]
    fn base_url_is_normalized_without_trailing_slash() {
        let fwd = Forwarder::new(client(), "http://127.0.0.1:9000").unwrap();
        assert_eq!(fwd.upstream_base(), "http://127.0.0.1:9000");
        let fwd = Forwarder::new(client(), "https://mock.local:8443/prefix/").unwrap();
        assert_eq!(fwd.upstream_base(), "https://mock.local:8443/prefix");
    }

    #[test]
    fn rejects_unusable_base_urls() {
        for bad in [
            "127.0.0.1:9000",
            "ftp://host/",
            "http://host/?q=1",
            "http://host/#frag",
            "http://user:pw@host/",
            "http://user@host/",
            "not a url",
        ] {
            assert!(Forwarder::new(client(), bad).is_err(), "{bad} was accepted");
        }
    }

    #[test]
    fn te_trailers_token_is_detected() {
        let mut headers = HeaderMap::new();
        assert!(!te_accepts_trailers(&headers));
        headers.insert(TE, HeaderValue::from_static("gzip;q=0.5, Trailers"));
        assert!(te_accepts_trailers(&headers));
        headers.insert(TE, HeaderValue::from_static("gzip, x-trailers"));
        assert!(!te_accepts_trailers(&headers));
    }

    #[test]
    fn cookies_are_joined_only_when_split() {
        let mut headers = HeaderMap::new();
        headers.append(COOKIE, HeaderValue::from_static("a=1"));
        join_cookies(&mut headers);
        assert_eq!(headers.get_all(COOKIE).iter().collect::<Vec<_>>(), ["a=1"]);
        headers.append(COOKIE, HeaderValue::from_static("b=2"));
        headers.append(COOKIE, HeaderValue::from_static("c=3"));
        join_cookies(&mut headers);
        assert_eq!(
            headers.get_all(COOKIE).iter().collect::<Vec<_>>(),
            ["a=1; b=2; c=3"]
        );
    }

    #[test]
    fn text_responses_carry_status_and_content_type() {
        let response = text_response(StatusCode::BAD_GATEWAY, BAD_GATEWAY_BODY);
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        assert_eq!(
            response.headers()[CONTENT_TYPE],
            "text/plain; charset=utf-8"
        );
    }
}
