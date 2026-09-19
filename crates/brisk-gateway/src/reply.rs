//! Responses Brisk generates itself: `OpenAI`-style error bodies with fixed
//! messages, so no upstream detail, header or secret can reach the client
//! through them, and the health and readiness bodies (R17).
