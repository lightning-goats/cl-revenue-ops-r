//! Parity fix (finding 4 of the 2026-07-29 parity_matrix run): the Boltz
//! transport must be configured from the SAME options Python reads, not
//! from `BoltzCliProcessConfig::default()`.
//!
//! The matrix caught this as 8 simultaneous mismatches: Rust answered
//! "Boltz CLI integration disabled" on every Boltz read while Python --
//! with `revenue-ops-boltz-enabled=true` in its config -- returned real
//! wallet and budget data.

use std::collections::HashMap;

use cln_plugin::options::Value as OptValue;
use revops::boltz_config::resolve_boltz_cfg;

fn python(pairs: &[(&str, OptValue)]) -> HashMap<String, OptValue> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), v.clone()))
        .collect()
}

/// With NO Python options visible, the resolver falls back to Python's own
/// documented defaults -- and `enabled` is false, so a plugin that cannot
/// see the operator's config refuses rather than guessing it is on.
#[tokio::test]
async fn empty_python_snapshot_falls_back_to_disabled() {
    let cfg = resolve_boltz_cfg(None, &HashMap::new()).await;
    assert!(!cfg.enabled, "absent config must NOT be read as enabled");
    assert_eq!(cfg.cli_path, "/usr/local/bin/boltzcli");
    assert_eq!(cfg.datadir, "/var/lib/boltz");
    assert_eq!(cfg.timeout_seconds, 60);
    assert!(!cfg.use_sudo);
    assert_eq!(cfg.sudo_user, "boltz");
}

/// The operator's LIVE Python values win -- this is the parity path. The
/// values below are lnnode's actual config as of 2026-07-29.
#[tokio::test]
async fn python_values_are_adopted() {
    let snapshot = python(&[
        ("revenue-ops-boltz-enabled", OptValue::String("true".into())),
        (
            "revenue-ops-boltz-cli-path",
            OptValue::String("/usr/local/bin/boltzcli".into()),
        ),
        (
            "revenue-ops-boltz-datadir",
            OptValue::String("/var/lib/boltz".into()),
        ),
        (
            "revenue-ops-boltz-use-sudo",
            OptValue::String("false".into()),
        ),
        (
            "revenue-ops-boltz-sudo-user",
            OptValue::String("boltz".into()),
        ),
        (
            "revenue-ops-boltz-timeout-seconds",
            OptValue::String("60".into()),
        ),
        (
            "revenue-ops-boltz-daily-budget-sats",
            OptValue::String("3000".into()),
        ),
        (
            "revenue-ops-boltz-structural-budget-sats",
            OptValue::String("200".into()),
        ),
        (
            "revenue-ops-boltz-auto-cycle-enabled",
            OptValue::String("true".into()),
        ),
        (
            "revenue-ops-boltz-max-withdraw-sats",
            OptValue::String("10000000".into()),
        ),
    ]);
    let cfg = resolve_boltz_cfg(None, &snapshot).await;
    assert!(cfg.enabled, "Python's enabled=true must be adopted");
    assert_eq!(cfg.daily_budget_sats, 3_000);
    assert_eq!(cfg.structural_budget_sats, 200);
    assert!(cfg.auto_cycle_enabled);
    assert_eq!(cfg.max_withdraw_sats, 10_000_000);
}

/// Boolean parsing goes through the PYTHON STARTUP cast, not a tolerant
/// generic parser -- the same layer-aware distinction `resolve_bool`
/// documents for vegas-reflex. A bare "1" must not silently arm Boltz if
/// Python's own startup cast would not.
#[tokio::test]
async fn boolean_parsing_matches_pythons_startup_cast() {
    for raw in ["true", "True"] {
        let cfg = resolve_boltz_cfg(
            None,
            &python(&[("revenue-ops-boltz-enabled", OptValue::String(raw.into()))]),
        )
        .await;
        assert!(cfg.enabled, "{raw:?} should enable");
    }
    for raw in ["false", "False", ""] {
        let cfg = resolve_boltz_cfg(
            None,
            &python(&[("revenue-ops-boltz-enabled", OptValue::String(raw.into()))]),
        )
        .await;
        assert!(!cfg.enabled, "{raw:?} must NOT enable");
    }
    // A native boolean is taken as-is.
    let cfg = resolve_boltz_cfg(
        None,
        &python(&[("revenue-ops-boltz-enabled", OptValue::Boolean(true))]),
    )
    .await;
    assert!(cfg.enabled);
}

/// The resolved config converts into the transport's own type, so there is
/// exactly one place that decides what the transport is configured with.
#[tokio::test]
async fn converts_into_the_transport_config() {
    let cfg = resolve_boltz_cfg(
        None,
        &python(&[
            ("revenue-ops-boltz-enabled", OptValue::String("true".into())),
            (
                "revenue-ops-boltz-datadir",
                OptValue::String("/mnt/boltz".into()),
            ),
        ]),
    )
    .await;
    let transport = cfg.to_process_config();
    assert!(transport.enabled());
    assert_eq!(transport.datadir(), "/mnt/boltz");
    assert_eq!(transport.cli_path(), "/usr/local/bin/boltzcli");
}
