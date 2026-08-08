//! Task 66: `revenue-total-cost-budget` assembly driven against the REAL
//! fixture DB — the exact production path `main.rs` registers (item-214
//! discipline: tests must drive the production functions, never re-assemble
//! the rule locally). Every expected number below is hand-derived from the
//! seeded rows, not read back from the code under test.

use revops::rpc_total_cost_budget::{total_cost_budget_response, TotalCostBudgetSources};
use revops_db::actor::spawn_read_only;
use rusqlite::Connection;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};

const NOW: i64 = 1_800_000_000;

fn fixture_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/fixture.db")
}

fn sources(db: Option<&revops_db::actor::DbHandle>) -> TotalCostBudgetSources<'_> {
    TotalCostBudgetSources {
        db,
        daily_budget_sats: 5_000,
        growth_enabled: false,
        growth_earned_fraction: 0.25,
        growth_experiment_fraction: 0.10,
        growth_max_extra_sats: 0,
        growth_hard_ceiling_sats: 0,
        now: NOW,
    }
}

/// Python `_total_cost_budget_status`'s uninitialized arm
/// (cl-revenue-ops.py:8306-8307): the EXACT 1-key error, "Plugin not
/// initialized" — not the spend-ledger's "Database not initialized".
#[tokio::test]
async fn without_db_returns_pythons_plugin_not_initialized() {
    let v = total_cost_budget_response(24, sources(None)).await;
    assert_eq!(v, json!({"error": "Plugin not initialized"}));
}

/// The full seeded pipeline. Hand-derived expectations:
///
/// revenue: forwards fee_msat 1500+1999 in-window = 3499 msat -> `// 1000`
///   -> 3 sats.
/// open: 250 (recent open; the 90-day-old 500-sat open is outside the
///   24h window but IS total-cost coverage evidence -> "complete").
/// close: 300. rebalance spent: 200; reserved 100 legacy + 40 unified=140;
///   history 2 success + 1 failed -> job 3 / success 2.
/// ledger: spend_events rebalance 60 + channel_open 500 (both excluded as
///   canonically counted) -> counted 0, raw 560; active
///   spend_reservations 40+70=110 total, rebalance 40 -> ledger-only
///   reserved 70.
/// actual: 200+0+250+300+0 = 750. reserved: 140+0+70 = 210.
/// net: 3-750 = -747. growth disabled -> fixed / effective 5000 /
///   remaining max(0, 5000-750-210) = 4040.
#[tokio::test]
async fn seeded_db_produces_hand_derived_budget() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("seed.db");
    std::fs::copy(fixture_path(), &path).unwrap();
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch(&format!(
        "INSERT INTO forwards (in_channel, out_channel, in_msat, out_msat, fee_msat, timestamp, resolved_time) VALUES
           ('1x1x0','2x2x0',1000000,998500,1500,{t1},{t1}),
           ('1x1x0','3x3x0',2000000,1998001,1999,{t2},{t2});
         INSERT INTO channel_costs (channel_id, peer_id, open_cost_sats, capacity_sats, opened_at) VALUES
           ('2x2x0','{p0}',500,1000000,{old_open}),
           ('6x6x0','{p0}',250,2000000,{t3});
         INSERT INTO channel_closure_costs
           (channel_id, peer_id, close_type, closure_fee_sats, htlc_sweep_fee_sats, penalty_fee_sats, total_closure_cost_sats, closed_at, resolution_complete)
           VALUES ('3x3x0','{p1}','mutual',300,0,0,300,{t1},1);
         INSERT INTO rebalance_costs (channel_id, peer_id, cost_sats, cost_msat, amount_sats, timestamp) VALUES
           ('2x2x0','{p1}',200,200000,50000,{t1});
         INSERT INTO rebalance_history (from_channel, to_channel, amount_sats, max_fee_sats, expected_profit_sats, status, timestamp) VALUES
           ('1x1x0','2x2x0',1000,10,5,'success',{t1}),
           ('1x1x0','2x2x0',1000,10,5,'success',{t1}),
           ('1x1x0','2x2x0',1000,10,5,'failed',{t1});
         INSERT INTO budget_reservations (reservation_id, reserved_sats, reserved_at, job_channel_id, status) VALUES
           ('br-1',100,{t3},'2x2x0','active');
         INSERT INTO spend_reservations (reservation_id, category, reserved_sats, reserved_at, status) VALUES
           ('sr-1','rebalance',40,{t4},'active'),
           ('sr-2','channel_open',70,{t4},'active');
         INSERT INTO spend_events (event_id, category, amount_sats, timestamp) VALUES
           ('ev-1','rebalance',60,{t1}),
           ('ev-2','channel_open',500,{t3});",
        t1 = NOW - 3600,
        t2 = NOW - 7200,
        t3 = NOW - 1800,
        t4 = NOW - 900,
        old_open = NOW - 90 * 86400,
        p0 = "0".repeat(66),
        p1 = "1".repeat(66),
    ))
    .unwrap();
    drop(conn);
    let handle = spawn_read_only(&path).await.unwrap();

    let v = total_cost_budget_response(24, sources(Some(&handle))).await;
    assert!(v.get("error").is_none(), "unexpected error: {v}");

    assert_eq!(v["source"], "total_cost_budget");
    assert_eq!(v["timestamp"], NOW);
    assert_eq!(v["generated_at"], NOW);
    assert_eq!(v["ttl_seconds"], 1800);
    assert_eq!(v["window_hours"], 24);
    assert_eq!(v["since_timestamp"], NOW - 86400);

    assert_eq!(v["revenue_sats"], 3, "3499 msat // 1000");
    assert_eq!(
        v["actual_spent_by_category"],
        json!({"rebalance": 200, "boltz": 0, "open": 250, "close": 300, "ledger": 0})
    );
    assert_eq!(v["actual_spent_sats"], 750);
    assert_eq!(
        v["reserved_by_category"],
        json!({"rebalance": 140, "boltz": 0, "ledger": 70})
    );
    assert_eq!(v["reserved_sats"], 210);
    assert_eq!(v["net_profit_sats_after_costs"], -747);

    assert_eq!(v["mode"], "fixed");
    assert_eq!(v["daily_budget_sats"], 5_000);
    assert_eq!(v["effective_budget_sats"], 5_000);
    assert_eq!(v["remaining_sats"], 4_040);
    assert!(!v["growth_budget"].is_null());

    // Total-cost coverage reads the 90-day-old channel_costs row ->
    // complete; the EMBEDDED spend-ledger coverage only sees spend
    // evidence 1h old -> partial 1.0 (the two scans are different scopes).
    assert_eq!(v["covered_hours"], 24);
    assert_eq!(v["coverage_hours"], 24);
    assert_eq!(v["coverage_status"], "complete");
    let ledger = &v["components"]["generic_ledger"];
    assert_eq!(ledger["covered_hours"], 1.0);
    assert_eq!(ledger["coverage_status"], "partial");

    assert_eq!(ledger["raw_spent_24h_sats"], 560);
    assert_eq!(ledger["spent_24h_sats"], 0, "canonical spend excluded");
    assert_eq!(
        ledger["excluded_spent_categories"],
        json!({"channel_open": 500, "rebalance": 60})
    );
    assert_eq!(ledger["reserved_24h_sats"], 110);
    // include_reservations=True parity: the embedded ledger carries the
    // active reservation rows themselves.
    assert_eq!(ledger["active_reservations"].as_array().unwrap().len(), 2);

    let rebalance = &v["components"]["rebalance"];
    assert_eq!(rebalance["available"], json!(true));
    assert_eq!(rebalance["spent_24h_sats"], 200);
    assert_eq!(rebalance["reserved_24h_sats"], 140);
    assert_eq!(rebalance["job_count"], 3);
    assert_eq!(rebalance["success_count"], 2);
    assert!(v["components"].get("boltz").is_none());
    assert_eq!(v["components"]["open_cost_sats"], 250);
    assert_eq!(v["components"]["closure_cost_sats"], 300);

    let visibility = &v["open_close_cost_visibility"];
    assert_eq!(visibility["canonical_open_cost_available"], json!(true));
    assert_eq!(visibility["canonical_close_cost_available"], json!(true));
    // Both canonical costs available -> no event-count pending; the one
    // active channel_open reservation still counts (1 + 0).
    assert_eq!(visibility["pending_open_close_spend_events"], 1);
    assert_eq!(visibility["excluded_open_close_spend_sats"], 500);
    assert_eq!(visibility["reserved_open_close_sats"], 70);

    // Every pipeline genuinely wired: nothing to declare.
    assert_eq!(v["_phase1b_gaps"], json!([]));
}

/// Empty (schema-only) DB: every read genuinely answers zero/empty, the
/// coverage pipeline measures "unknown" — and that is NOT declared as a
/// wiring gap, because the pipeline ran (Python reports the same nulls).
#[tokio::test]
async fn empty_db_reports_zeros_and_honest_unknown_coverage() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("empty.db");
    std::fs::copy(fixture_path(), &path).unwrap();
    let handle = spawn_read_only(&path).await.unwrap();

    let v = total_cost_budget_response(24, sources(Some(&handle))).await;
    assert!(v.get("error").is_none(), "unexpected error: {v}");
    assert_eq!(v["revenue_sats"], 0);
    assert_eq!(v["actual_spent_sats"], 0);
    assert_eq!(v["reserved_sats"], 0);
    assert_eq!(v["net_profit_sats_after_costs"], 0);
    assert_eq!(v["covered_hours"], Value::Null);
    assert_eq!(v["coverage_status"], "unknown");
    assert_eq!(v["_phase1b_gaps"], json!([]));
}
