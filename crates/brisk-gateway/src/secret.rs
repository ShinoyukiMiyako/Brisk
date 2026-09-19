//! Secrets that never reach logs, and scrubbing of errors that may carry
//! upstream URLs (R17).

use std::fmt;

use crate::BoxError;

/// A secret whose `Debug` and `Display` print `***`. No `Deref`, no `Serialize`.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Redacted<T>(T);

impl<T> Redacted<T> {
    /// Wraps `value`.
    pub const fn new(value: T) -> Self {
        Self(value)
    }

    /// The secret itself. Every call is a place where it can leak, which is
    /// why reading it takes an explicit call instead of a `Deref`.
    pub fn expose(&self) -> &T {
        &self.0
    }

    /// Unwraps the secret.
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> fmt::Debug for Redacted<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("***")
    }
}

impl<T> fmt::Display for Redacted<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("***")
    }
}

/// Strips the URL from a `reqwest::Error` inside `err` (`without_url`, R17);
/// other errors pass through unchanged.
///
/// The URL of an upstream request may carry credentials in its query, and
/// reqwest prints it in both `Display` and `Debug`.
pub fn scrub_error(err: BoxError) -> BoxError {
    match err.downcast::<reqwest::Error>() {
        Ok(upstream) => Box::new(upstream.without_url()),
        Err(other) => other,
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use super::*;
    use crate::upstream::{UpstreamClientConfig, build_client};

    const SECRET: &str = "sk-leak-123";

    #[test]
    fn debug_and_display_print_stars() {
        let secret = Redacted::new(String::from(SECRET));
        assert_eq!(format!("{secret:?}"), "***");
        assert_eq!(format!("{secret}"), "***");
        assert_eq!(format!("{secret:#?}"), "***");
        assert_eq!(format!("{:?}", Some(&secret)), "Some(***)");
    }

    #[test]
    fn containers_do_not_print_the_secret() {
        let pair = ("channel", Redacted::new(SECRET));
        assert_eq!(format!("{pair:?}"), r#"("channel", ***)"#);
        let list = vec![Redacted::new(SECRET), Redacted::new(SECRET)];
        assert_eq!(format!("{list:?}"), "[***, ***]");
        assert!(!format!("{list:#?}").contains(SECRET));
    }

    #[test]
    fn expose_and_into_inner_return_the_value() {
        let secret = Redacted::new(String::from(SECRET));
        assert_eq!(secret.expose(), SECRET);
        assert_eq!(secret.clone(), secret);
        assert_eq!(secret.into_inner(), SECRET);
    }

    #[tokio::test]
    async fn scrub_error_removes_the_url_from_reqwest_errors() {
        let client = build_client(&UpstreamClientConfig::default()).unwrap();
        // reqwest rejects the scheme before any I/O and attaches the URL.
        let err = client
            .get(format!("ftp://upstream.invalid/v1?key={SECRET}"))
            .send()
            .await
            .unwrap_err();
        assert!(err.url().is_some());
        assert!(err.to_string().contains(SECRET), "precondition: {err}");

        let scrubbed = scrub_error(Box::new(err));
        assert!(!scrubbed.to_string().contains(SECRET), "{scrubbed}");
        assert!(!format!("{scrubbed:?}").contains(SECRET), "{scrubbed:?}");
        let upstream = scrubbed
            .downcast::<reqwest::Error>()
            .expect("the scrubbed error stays a reqwest::Error");
        assert!(upstream.url().is_none());
    }

    #[test]
    fn scrub_error_passes_other_errors_through() {
        let err: BoxError = Box::new(io::Error::new(io::ErrorKind::TimedOut, "slow upstream"));
        let passed = scrub_error(err);
        assert_eq!(passed.to_string(), "slow upstream");
        let io_err = passed
            .downcast::<io::Error>()
            .expect("non-reqwest errors keep their type");
        assert_eq!(io_err.kind(), io::ErrorKind::TimedOut);
    }
}
