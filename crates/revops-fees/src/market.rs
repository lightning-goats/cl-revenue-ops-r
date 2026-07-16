//! Market intelligence: neighbor-gossip fee statistics + prior selection
//! (port of the market paths in `modules/fee_controller.py`).
//! `_get_market_boundary_fee` returns `None` UNCONDITIONALLY (behavioral
//! contract 1), even with `fee_market_boundary_enabled=true` persisted.
//!
//! Filled in by Phase 4 Task 8 (Wave 2).
