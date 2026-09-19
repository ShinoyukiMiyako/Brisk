//! Channel registry: validates every configured channel, derives its chat
//! endpoint URL from the base URL, and builds one HTTP client per distinct
//! client profile so that channels with equal profiles share a connection
//! pool (R12, R25).
