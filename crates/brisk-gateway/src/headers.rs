//! Request and response header allowlists: which client headers reach the
//! upstream and which upstream headers reach the client. Credentials, cookies,
//! hop-by-hop and forwarding headers never pass in either direction (R19).
