//! Zero-copy rewriting of a request body for one upstream attempt.
//!
//! The rewritten body is at most five `Bytes` segments that replace the
//! `model` value and set `stream_options.include_usage`; every byte outside
//! the edited value spans reaches the upstream unchanged (R5).
