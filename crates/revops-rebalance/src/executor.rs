//! Native route executor: strict validation, `payment_pending` semantics
//! (port of `modules/rebalance_native_executor_v2.py`, `NativeRouteExecutor`).
//!
//! Filled in by Phase 5 Task 5 (Wave 2). Golden parity target:
//! `fixtures/rebalance/executor.json`. `PaymentMode::Live` must be
//! constructed ONLY by the cutover task — a source-scan test in this
//! module's owning task asserts no other construction site.
