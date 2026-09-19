//! The immutable configuration snapshot every request reads (keys, channels,
//! model routes), reached only through a single accessor so that swapping it
//! at runtime later touches this module alone (R10, D9).
