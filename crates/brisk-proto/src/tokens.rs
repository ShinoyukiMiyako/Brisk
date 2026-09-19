//! Token counts that settlement bills on, and the payload-free reason a usage
//! candidate was rejected.
//!
//! They are kept apart from [`crate::usage`] so that settlement code can depend
//! on the counts without depending on the parser that produces them.

/// Token counts Brisk bills on. Fields not listed are ignored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct UsageTokens {
    /// `prompt_tokens`.
    pub input: u64,
    /// `completion_tokens`.
    pub output: u64,
    /// `prompt_tokens_details.cached_tokens`.
    pub cached_input: Option<u64>,
    /// `completion_tokens_details.reasoning_tokens`.
    pub reasoning_output: Option<u64>,
}

impl UsageTokens {
    /// Field-wise maximum of `input` and `output`; the optional details of
    /// `self` are kept.
    #[must_use]
    pub fn max_billable(self, other: Self) -> Self {
        Self {
            input: self.input.max(other.input),
            output: self.output.max(other.output),
            ..self
        }
    }
}

/// Why a usage candidate was rejected. `Copy` and free of payload text, so it
/// can travel inside an `Outcome` and be logged without echoing upstream bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UsageErrorKind {
    /// Malformed JSON, a wrong type, a fraction, a negative number or a duplicate key.
    Json {
        /// `serde_json::Error::classify` of the failure.
        category: serde_json::error::Category,
        /// `serde_json::Error::column` of the failure.
        column: usize,
    },
    /// A required count is absent.
    MissingField(&'static str),
}

#[cfg(test)]
mod tests {
    use super::*;

    const USAGE: UsageTokens = UsageTokens {
        input: 213,
        output: 71,
        cached_input: Some(0),
        reasoning_output: Some(70),
    };

    #[test]
    fn max_billable_takes_the_larger_count_per_field() {
        let estimate = UsageTokens {
            input: 200,
            output: 1071,
            cached_input: None,
            reasoning_output: None,
        };
        let billed = USAGE.max_billable(estimate);
        assert_eq!(billed.input, 213);
        assert_eq!(billed.output, 1071);
    }

    #[test]
    fn max_billable_keeps_the_details_of_self() {
        let other = UsageTokens {
            input: 1,
            output: 1,
            cached_input: Some(128),
            reasoning_output: Some(999),
        };
        assert_eq!(USAGE.max_billable(other), USAGE);

        let without_details = UsageTokens {
            input: 5,
            output: 6,
            cached_input: None,
            reasoning_output: None,
        };
        assert_eq!(
            without_details.max_billable(USAGE),
            UsageTokens {
                input: 213,
                output: 71,
                cached_input: None,
                reasoning_output: None,
            }
        );
    }

    #[test]
    fn max_billable_of_equal_counts_is_identity() {
        assert_eq!(USAGE.max_billable(USAGE), USAGE);
        assert_eq!(
            UsageTokens::default().max_billable(UsageTokens::default()),
            UsageTokens::default()
        );
    }
}
