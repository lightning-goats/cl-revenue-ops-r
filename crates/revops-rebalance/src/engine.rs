//! `RebalanceEngine` orchestration + `RebalanceStore` (port of
//! `modules/rebalance_engine_v2.py`): wires the planner, router, executor,
//! EV gate, and cooldowns; owns the unified budget rail interaction and the
//! dry-run journal store.
//!
//! Filled in by Phase 5 Task 7 (Wave 3, serial — wires T2-T6).
