//! askrene v3 pricing + v2 policy-lookup helpers + orphan exclude-layer sweep
//! (port of `modules/rebalance_router_v3.py` and the surviving fee/CLTV
//! policy helpers in `modules/rebalance_router_v2.py`).
//!
//! Filled in by Phase 5 Task 4 (Wave 1). Golden parity target:
//! `fixtures/rebalance/router.json` — canned `getroutes` responses driven
//! through the real Python `RebalanceRouterV3`.
