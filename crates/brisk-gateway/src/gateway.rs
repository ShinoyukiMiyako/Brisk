//! The gateway handle: validates a spec, builds the upstream clients and the
//! configuration snapshot without network I/O, and serves the data plane
//! until shutdown (R1, R10, R16).
