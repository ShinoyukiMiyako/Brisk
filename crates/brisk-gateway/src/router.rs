//! Request routing, in the two implementations experiment E1 compares: an
//! `axum` router and a static method-and-path match. Both remove credential
//! query parameters before any route is matched (D10, R17).

use http::Method;

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
    MethodNotAllowed,
}

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
    let allowed = if get_like {
        *method == Method::GET || *method == Method::HEAD
    } else {
        *method == Method::POST
    };
    if allowed {
        Ok(route)
    } else {
        Err(RouteMiss::MethodNotAllowed)
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
            Err(RouteMiss::MethodNotAllowed)
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
                Err(RouteMiss::MethodNotAllowed),
                "{method}"
            );
        }
        for path in ["/healthz", "/readyz"] {
            for method in [Method::POST, Method::PUT, Method::DELETE, Method::OPTIONS] {
                assert_eq!(
                    match_route(&method, path),
                    Err(RouteMiss::MethodNotAllowed),
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
