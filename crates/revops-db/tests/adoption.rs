use revops_db::{coalesce_msat, open_read_only, table_names};

fn fixture_path() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/fixture.db")
}

#[test]
fn opens_python_initialized_db_read_only() {
    let conn = open_read_only(&fixture_path()).unwrap();
    let tables = table_names(&conn).unwrap();
    // Spot-check the load-bearing tables the observer reads.
    for t in [
        "forwards",
        "rebalance_history",
        "fee_changes",
        "budget_reservations",
    ] {
        assert!(
            tables.iter().any(|x| x == t),
            "missing table {t}; have {tables:?}"
        );
    }
    // Read-only is enforced by sqlite, not convention.
    let err = conn
        .execute("CREATE TABLE should_fail (x)", [])
        .unwrap_err();
    assert!(err.to_string().contains("readonly"), "{err}");
}

#[test]
fn refuses_to_create_missing_db() {
    assert!(open_read_only(std::path::Path::new("/nonexistent/nope.db")).is_err());
}

#[test]
fn coalesce_msat_prefers_msat_column() {
    assert_eq!(coalesce_msat(Some(1500), Some(999)), 1500);
    assert_eq!(coalesce_msat(None, Some(2)), 2000);
    assert_eq!(coalesce_msat(None, None), 0);
}
