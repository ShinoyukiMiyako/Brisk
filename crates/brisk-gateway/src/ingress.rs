//! Request-body intake under the in-flight byte budget, with a size limit, a
//! total read deadline and a minimum transfer rate, so a slow or oversized
//! body cannot hold memory for long (R13, R20).
