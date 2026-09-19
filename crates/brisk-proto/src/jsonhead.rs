//! Borrowed parsing of the request-body members Brisk routes and bills on:
//! `model`, `stream` and `stream_options` of an `OpenAI` Chat Completions
//! request, each with its exact byte span in the body.
//!
//! `serde_json` validates the whole body, but only these members are decoded.
//! A repeated member, or a key that equals a recognized member under the case
//! folding Go's `encoding/json` applies, is rejected: an upstream that
//! resolves such keys differently would run a model other than the one Brisk
//! routed and billed (R4, D20).
