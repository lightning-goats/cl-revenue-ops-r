//! Wave 0 scaffold smoke test (Task 1 of
//! `docs/superpowers/plans/2026-07-17-phase3-analytics-budget.md`).
//!
//! This only proves the crate exists, is a valid workspace member, and links
//! — the actual module bodies land task-by-task in Waves 1-2 as stub files
//! are replaced. Waves 1-2 never need to touch this file or `lib.rs` again.

#[test]
fn crate_links_and_names_itself_correctly() {
    assert_eq!(env!("CARGO_PKG_NAME"), "revops-analytics");
}
