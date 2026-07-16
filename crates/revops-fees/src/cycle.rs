//! Dry-run fee cycle orchestrator (port of the `_state_lock`-spanning
//! Python fee cycle as a single-owner task; reads the production DB
//! read-only, emits `FeeDecision` records only).
//!
//! Filled in by Phase 4 Task 10 (Wave 3).
