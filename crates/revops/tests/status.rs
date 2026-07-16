use revops::rpc_status::{build_status, StatusInputs};

#[test]
fn status_shape() {
    let v = build_status(&StatusInputs {
        version: "0.1.0".into(),
        observer: true,
        db_path: Some("/tmp/x.db".into()),
        db_tables: Some(35),
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
    });
    assert_eq!(v["mode"], "enforcing");
}
