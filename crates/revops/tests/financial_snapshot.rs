//! Task 67 slice 5: the financial-snapshot owner.

use revops::financial_snapshot::{
    plan_financial_snapshot, FinancialDeps, FinancialRefusal, LifetimeStats,
    SNAPSHOT_INTERVAL_SECONDS, STARTUP_DELAY_SECONDS,
};
use serde_json::json;

const NOW: i64 = 1_800_000_000;

fn deps() -> FinancialDeps {
    FinancialDeps {
        tlv_raw: Ok(json!({
            "local_balance_sats": 5_000_000i64,
            "remote_balance_sats": 3_000_000i64,
            "onchain_sats": 1_200_000i64,
            "channel_count": 21i64,
            "tlv_sats": 9_200_000i64,
        })),
        lifetime: Ok(LifetimeStats {
            total_revenue_msat: 12_345_678,
            total_rebalance_cost_sats: 4_321,
        }),
        now: NOW,
        boot_id: "boot-test".to_string(),
    }
}

/// Cadence matches Python: a 300s startup delay then one immediate
/// snapshot, then daily (cl-revenue-ops.py:3491-3551).
#[test]
fn cadence_matches_python() {
    assert_eq!(STARTUP_DELAY_SECONDS, 300);
    assert_eq!(SNAPSHOT_INTERVAL_SECONDS, 86_400);
}

/// The row is assembled with Python's exact arithmetic: capacity is
/// local+remote (NOT a TLV field), and revenue is FLOOR-divided msat.
#[test]
fn assembles_with_pythons_arithmetic() {
    let row = plan_financial_snapshot(deps()).expect("healthy");
    assert_eq!(row.local_balance_sats, 5_000_000);
    assert_eq!(row.remote_balance_sats, 3_000_000);
    assert_eq!(row.onchain_sats, 1_200_000);
    assert_eq!(
        row.capacity_sats, 8_000_000,
        "capacity is local+remote, computed -- not read from tlv_sats"
    );
    assert_eq!(
        row.revenue_accumulated_sats, 12_345,
        "revenue is msat floor-divided by 1000 (12_345_678 -> 12_345)"
    );
    assert_eq!(row.rebalance_cost_accumulated_sats, 4_321);
    assert_eq!(row.channel_count, 21);
    assert_eq!(row.taken_at, NOW);
}

/// Both required sources refuse TYPED. Python logs a warning and silently
/// RETURNS when its components are missing (:3556-3558), recording no
/// snapshot and reporting no failure -- a gap that looks identical to "no
/// snapshot was due". That is not ported.
#[test]
fn missing_sources_refuse_typed_rather_than_silently_skipping() {
    let mut d = deps();
    d.tlv_raw = Err("listfunds rpc timeout".into());
    let err = plan_financial_snapshot(d).expect_err("tlv failure refuses");
    assert_eq!(err.code(), "financial_snapshot_tlv_unavailable");

    let mut d = deps();
    d.lifetime = Err("lifetime stats read failed".into());
    let err = plan_financial_snapshot(d).expect_err("lifetime failure refuses");
    assert_eq!(err.code(), "financial_snapshot_lifetime_unavailable");

    // A TLV reply missing its balance fields is UNUSABLE, not zero.
    let mut d = deps();
    d.tlv_raw = Ok(json!({"channel_count": 21i64}));
    let err = plan_financial_snapshot(d).expect_err("incomplete tlv refuses");
    assert!(
        matches!(err, FinancialRefusal::TlvUnavailable(_)),
        "{err:?}"
    );
}

/// A genuinely zero-balance node is a VALID snapshot, distinct from
/// unusable evidence: explicit zeros are recorded, absent fields refuse.
#[test]
fn explicit_zeros_are_valid_but_absent_fields_are_not() {
    let mut d = deps();
    d.tlv_raw = Ok(json!({
        "local_balance_sats": 0i64,
        "remote_balance_sats": 0i64,
        "onchain_sats": 0i64,
        "channel_count": 0i64,
    }));
    let row = plan_financial_snapshot(d).expect("explicit zeros are a real observation");
    assert_eq!(row.capacity_sats, 0);
    assert_eq!(row.channel_count, 0);
}
