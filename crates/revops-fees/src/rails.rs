//! Frozen ADR-001 rail order:
//! `cooldown(deadband(rate_limit(rails(raw_target))))` with DTS+PID as the
//! authoritative controller. Rail ORDER is load-bearing and pinned by
//! vendored fee goldens.
//!
//! Filled in by Phase 4 Task 5 (Wave 1).
