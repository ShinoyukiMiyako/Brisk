//! The forwarding core for `POST /v1/chat/completions`: authentication, body
//! intake, head parsing, channel selection, request rewriting, failover
//! strictly before commit, and the hand-over of a committed response to its
//! body (R4, R13, R14, R16, R17, R19).
