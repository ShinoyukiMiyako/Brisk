//! Connection warm-up: credential-free requests to each upstream origin at
//! start and on an interval, so the first client requests find pooled
//! connections, and the readiness signal behind `/readyz`.
