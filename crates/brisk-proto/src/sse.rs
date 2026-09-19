//! Incremental Server-Sent Events framing over arbitrarily split body chunks.
//!
//! Events are found by scanning for line endings only, without parsing their
//! payloads, and a `\r` at the end of one chunk is held until the next chunk
//! shows whether a `\n` completes it (R6, R7).
