//! Request routing, in the two implementations experiment E1 compares: an
//! `axum` router and a static method-and-path match. Both remove credential
//! query parameters before any route is matched (D10, R17).
