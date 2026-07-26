use revops::rpc_status::{build_status, StatusInputs};

#[test]
fn status_shape() {
    let v = build_status(&StatusInputs {
        version: "0.1.0".into(),
        observer: true,
        db_path: Some("/tmp/x.db".into()),
        db_tables: Some(35),
        fee_runway: None,
    });
    assert_eq!(v["status"], "running");
    assert_eq!(v["version"], "0.1.0");
    assert_eq!(v["mode"], "observer");
    assert_eq!(v["db"]["tables"], 35);
}

#[test]
fn status_no_db_configured_is_null() {
    let v = build_status(&StatusInputs {
        version: "0.1.0".into(),
        observer: true,
        db_path: None,
        db_tables: None,
        fee_runway: None,
    });
    assert!(v["db"]["path"].is_null());
    assert!(v["db"]["tables"].is_null());
}

#[test]
fn status_enforcing_mode_when_not_observer() {
    let v = build_status(&StatusInputs {
        version: "0.1.0".into(),
        observer: false,
        db_path: None,
        db_tables: None,
        fee_runway: None,
    });
    assert_eq!(v["mode"], "enforcing");
}

/// Task 5 step 4: `revenue-r-status` exposes the Rust-owned fee-runway
/// identity (latest generation, seed provenance, restart marker) WITHOUT
/// reading any Python state -- `main.rs` resolves it from the observer-db
/// actor and passes it through here.
#[test]
fn status_fee_runway_passthrough() {
    let fee_runway = serde_json::json!({
        "generation": 12,
        "seed": {
            "outcome": "seeded",
            "payload_sha256": "ab".repeat(32),
            "row_count": 47,
        },
        "restart": {
            "hydration_source": "rust_generation:12",
            "prior_generation": 12,
        },
    });
    let v = build_status(&StatusInputs {
        version: "0.1.0".into(),
        observer: true,
        db_path: None,
        db_tables: None,
        fee_runway: Some(fee_runway.clone()),
    });
    assert_eq!(v["fee_runway"], fee_runway);

    let v = build_status(&StatusInputs {
        version: "0.1.0".into(),
        observer: true,
        db_path: None,
        db_tables: None,
        fee_runway: None,
    });
    assert!(
        v["fee_runway"].is_null(),
        "no observer db configured -> null, never absent"
    );
}
