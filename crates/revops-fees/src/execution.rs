//! `set_channel_fee` ported as a PURE decision function: clamps, frozen
//! log strings, governor gate. No `setchannel` broadcast in this crate —
//! the side-effecting call is added at fee cutover, behind the
//! per-subsystem flag.
//!
//! Filled in by Phase 4 Task 10 (Wave 3).
