//! Usage, `finish_reason` and top-level error detection in streamed
//! `chat.completion.chunk` payloads and non-streaming `chat.completion`
//! bodies.
//!
//! Byte prefilters select candidate payloads, so only the few that can carry
//! these facts are parsed, and only their top-level members are decoded (R8).
