//! Request routing, in the two implementations experiment E1 compares: an
//! `axum` router and a static method-and-path match. Both remove credential
//! query parameters before any route is matched (D10, R17), and both answer
//! every request identically.

use std::convert::Infallible;
use std::fmt;
use std::future::{Future, ready};
use std::pin::Pin;

use axum::extract::State;
use axum::routing::{get, post};
use http::header::ALLOW;
use http::{HeaderValue, Method, Request, Response};
use hyper::body::Incoming;
use hyper::service::Service;

use crate::auth::strip_credential_query;
use crate::body::ResponseBody;
use crate::forward;
use crate::gateway::Gateway;
use crate::outcome::RejectReason;
use crate::reply::{self, ErrorCode};

/// Chat Completions, the only forwarded endpoint in M1.
pub(crate) const CHAT_COMPLETIONS_PATH: &str = "/v1/chat/completions";
/// Liveness: 200 `{"status":"ok"}` whenever the process serves requests.
pub(crate) const HEALTH_PATH: &str = "/healthz";
/// Readiness: 200 once the first warm-up round finished, 503 before.
pub(crate) const READY_PATH: &str = "/readyz";

/// A matched route of the data plane.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Route {
    /// `POST /v1/chat/completions`.
    ChatCompletions,
    /// `GET /healthz`.
    Health,
    /// `GET /readyz`.
    Ready,
}

/// Why no route matched.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RouteMiss {
    /// 404 `not_found`.
    NotFound,
    /// 405 `method_not_allowed`: the path exists with another method.
    MethodNotAllowed {
        /// The `Allow` header value, spelled as axum spells it.
        allow: &'static str,
    },
}

/// `Allow` of the chat route.
const ALLOW_POST: &str = "POST";
/// `Allow` of the `GET` routes; axum's `get` also answers `HEAD`.
const ALLOW_GET: &str = "GET,HEAD";

/// The static `(method, path)` table of [`RouterKind::Match`].
///
/// `HEAD` is accepted wherever `GET` is, because axum's `get` routes answer
/// `HEAD` too (the body is dropped when the response is written) and E1
/// requires both routers to answer every request identically. Paths compare
/// byte for byte: no trailing-slash or case folding, as in axum. The query
/// string plays no part, so callers pass `uri.path()`.
///
/// [`RouterKind::Match`]: crate::spec::RouterKind::Match
pub(crate) fn match_route(method: &Method, path: &str) -> Result<Route, RouteMiss> {
    let (route, get_like) = match path {
        CHAT_COMPLETIONS_PATH => (Route::ChatCompletions, false),
        HEALTH_PATH => (Route::Health, true),
        READY_PATH => (Route::Ready, true),
        _ => return Err(RouteMiss::NotFound),
    };
    let (allowed, allow) = if get_like {
        (*method == Method::GET || *method == Method::HEAD, ALLOW_GET)
    } else {
        (*method == Method::POST, ALLOW_POST)
    };
    if allowed {
        Ok(route)
    } else {
        Err(RouteMiss::MethodNotAllowed { allow })
    }
}

/// Replaces the request URI by one without credential query parameters
/// (D10), before anything looks at the URI.
fn sanitize<B>(request: &mut Request<B>) {
    if let Some(uri) = strip_credential_query(request.uri()) {
        *request.uri_mut() = uri;
    }
}

/// The answer to a request no route takes, recorded as a rejection.
fn route_miss(gateway: &Gateway, miss: RouteMiss) -> Response<ResponseBody> {
    match miss {
        RouteMiss::NotFound => not_found_response(gateway),
        RouteMiss::MethodNotAllowed { allow } => {
            let mut response = method_not_allowed_response(gateway);
            // RFC 9110, section 15.5.6: a 405 lists the allowed methods.
            response
                .headers_mut()
                .insert(ALLOW, HeaderValue::from_static(allow));
            response
        }
    }
}

fn not_found_response(gateway: &Gateway) -> Response<ResponseBody> {
    forward::reject(gateway, RejectReason::NotFound, ErrorCode::NotFound)
}

fn method_not_allowed_response(gateway: &Gateway) -> Response<ResponseBody> {
    forward::reject(
        gateway,
        RejectReason::MethodNotAllowed,
        ErrorCode::MethodNotAllowed,
    )
}

/// E1 baseline: axum Router with raw-request handlers, no extractors beyond
/// `State` and `Request`, no middleware.
///
/// Route misses go through axum's `fallback` and
/// `method_not_allowed_fallback`, which answer like [`MatchService`]; axum
/// adds the `Allow` header of a 405 itself.
pub(crate) fn axum_router(gateway: Gateway) -> axum::Router {
    axum::Router::new()
        .route(CHAT_COMPLETIONS_PATH, post(chat))
        .route(HEALTH_PATH, get(health))
        .route(READY_PATH, get(readiness))
        // Applies to the routes above, so it must follow them.
        .method_not_allowed_fallback(method_not_allowed)
        .fallback(not_found)
        .with_state(gateway)
}

async fn chat(
    State(gateway): State<Gateway>,
    request: axum::extract::Request,
) -> Response<ResponseBody> {
    forward::chat_completions(&gateway, request).await
}

async fn health() -> Response<ResponseBody> {
    reply::live().map(ResponseBody::from)
}

async fn readiness(State(gateway): State<Gateway>) -> Response<ResponseBody> {
    reply::health(gateway.is_ready()).map(ResponseBody::from)
}

async fn not_found(State(gateway): State<Gateway>) -> Response<ResponseBody> {
    not_found_response(&gateway)
}

async fn method_not_allowed(State(gateway): State<Gateway>) -> Response<ResponseBody> {
    method_not_allowed_response(&gateway)
}

/// E1 alternative: a static `(method, path)` match.
#[derive(Clone)]
pub(crate) struct MatchService {
    gateway: Gateway,
}

impl MatchService {
    /// Routes to `gateway`.
    pub(crate) fn new(gateway: Gateway) -> Self {
        Self { gateway }
    }
}

impl fmt::Debug for MatchService {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MatchService").finish_non_exhaustive()
    }
}

impl Service<Request<Incoming>> for MatchService {
    type Response = Response<ResponseBody>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Infallible>> + Send>>;

    fn call(&self, mut request: Request<Incoming>) -> Self::Future {
        sanitize(&mut request);
        let answer = match match_route(request.method(), request.uri().path()) {
            Ok(Route::ChatCompletions) => {
                // The one clone of the gateway per request (D27).
                let gateway = self.gateway.clone();
                return Box::pin(
                    async move { Ok(forward::chat_completions(&gateway, request).await) },
                );
            }
            Ok(Route::Health) => reply::live().map(ResponseBody::from),
            Ok(Route::Ready) => reply::health(self.gateway.is_ready()).map(ResponseBody::from),
            Err(miss) => route_miss(&self.gateway, miss),
        };
        Box::pin(ready(Ok(answer)))
    }
}

/// Removes credential query parameters before any routing (D10); wraps the
/// axum side, and `MatchService::call` does the same first thing itself.
#[derive(Clone)]
pub(crate) struct Sanitize<S> {
    inner: S,
}

impl<S> Sanitize<S> {
    /// Sanitizes every request before `inner` sees it.
    pub(crate) fn new(inner: S) -> Self {
        Self { inner }
    }
}

impl<S> fmt::Debug for Sanitize<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sanitize").finish_non_exhaustive()
    }
}

impl<S, B> Service<Request<B>> for Sanitize<S>
where
    S: Service<Request<B>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn call(&self, mut request: Request<B>) -> Self::Future {
        sanitize(&mut request);
        self.inner.call(request)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_routes_match_their_method() {
        assert_eq!(
            match_route(&Method::POST, "/v1/chat/completions"),
            Ok(Route::ChatCompletions)
        );
        assert_eq!(match_route(&Method::GET, "/healthz"), Ok(Route::Health));
        assert_eq!(match_route(&Method::GET, "/readyz"), Ok(Route::Ready));
    }

    #[test]
    fn head_is_accepted_where_get_is() {
        assert_eq!(match_route(&Method::HEAD, "/healthz"), Ok(Route::Health));
        assert_eq!(match_route(&Method::HEAD, "/readyz"), Ok(Route::Ready));
        assert_eq!(
            match_route(&Method::HEAD, "/v1/chat/completions"),
            Err(RouteMiss::MethodNotAllowed { allow: "POST" })
        );
    }

    #[test]
    fn wrong_methods_on_known_paths_are_not_allowed() {
        for method in [
            Method::GET,
            Method::PUT,
            Method::DELETE,
            Method::OPTIONS,
            Method::PATCH,
        ] {
            assert_eq!(
                match_route(&method, "/v1/chat/completions"),
                Err(RouteMiss::MethodNotAllowed { allow: "POST" }),
                "{method}"
            );
        }
        for path in ["/healthz", "/readyz"] {
            for method in [Method::POST, Method::PUT, Method::DELETE, Method::OPTIONS] {
                assert_eq!(
                    match_route(&method, path),
                    Err(RouteMiss::MethodNotAllowed { allow: "GET,HEAD" }),
                    "{method} {path}"
                );
            }
        }
    }

    #[test]
    fn unknown_paths_are_not_found_for_every_method() {
        for path in [
            "/",
            "",
            "/healthz/",
            "/Healthz",
            "/v1/chat/completions/",
            "/v1/Chat/Completions",
            "/v1/models",
            "/v1/completions",
            "/v1/responses",
            "/v1beta/models/grok-4.6(xhigh):streamGenerateContent",
            "//v1/chat/completions",
        ] {
            for method in [Method::GET, Method::POST, Method::OPTIONS] {
                assert_eq!(
                    match_route(&method, path),
                    Err(RouteMiss::NotFound),
                    "{method} {path:?}"
                );
            }
        }
    }
}
