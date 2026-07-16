//! Python-compatible JSON writer for persisted state blobs: reproduces
//! `json.dumps` float formatting (`repr`) and separators so
//! `v2_state_json` round-trips losslessly against Python-written blobs.
//!
//! Filled in by Phase 4 Task 3 (Wave 1).
