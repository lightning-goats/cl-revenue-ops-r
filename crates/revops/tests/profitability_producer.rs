//! C71-25: the result-bearing fleet profitability producer.
//!
//! `assemble_fleet` used to hand the frozen classifier four values nobody
//! looked up -- `opener: "local"`, `last_routed: None`, zeroed diagnostics
//! and no fee posterior. Three coincide with Python's no-row defaults and
//! were therefore invisible; the fourth asserts this node paid for the
//! open. These tests drive the real stores so the difference between "we
//! looked and found nothing" and "we never asked" is observable.

use revops::profitability_assembler::{gather_profitability, ProfitabilitySources};
use revops_db::actor::spawn_read_only;
use revops_db::fee_runway::{FeeCycleCommit, FeeStateRow};
use revops_db::owner::{spawn_read_write, ObserverHandle};
use serde_json::json;

const NOW: i64 = 1_800_000_000;
const DAY: i64 = 86_400;

const SCHEMA: &str = "
CREATE TABLE forwards (
    id INTEGER PRIMARY KEY, in_channel TEXT, out_channel TEXT,
    in_msat INTEGER, out_msat INTEGER, fee_msat INTEGER, timestamp INTEGER
);
CREATE TABLE daily_forwarding_stats (
    channel_id TEXT, date INTEGER, forward_count INTEGER,
    total_fee_msat INTEGER, total_out_msat INTEGER
);
CREATE TABLE daily_forwarding_stats_inbound (
    channel_id TEXT, date INTEGER, forward_count INTEGER,
    total_fee_msat INTEGER, total_out_msat INTEGER
);
CREATE TABLE rebalance_history (
    id INTEGER PRIMARY KEY, from_channel TEXT, to_channel TEXT,
    rebalance_type TEXT, status TEXT, timestamp INTEGER
);
CREATE TABLE channel_costs (
    channel_id TEXT PRIMARY KEY, peer_id TEXT, open_cost_sats INTEGER,
    capacity_sats INTEGER, opened_at INTEGER
);
CREATE TABLE rebalance_costs (
    id INTEGER PRIMARY KEY, channel_id TEXT, peer_id TEXT,
    cost_sats INTEGER, timestamp INTEGER
);
";

struct Fixture {
    production: revops_db::actor::DbHandle,
    observer: ObserverHandle,
    _dir: tempfile::TempDir,
}

async fn fixture(seed: &str, fee_rows: Vec<FeeStateRow>) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("prod.db");
    {
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch(SCHEMA).unwrap();
        if !seed.is_empty() {
            conn.execute_batch(seed).unwrap();
        }
    }
    let production = spawn_read_only(&path).await.unwrap();
    let observer = spawn_read_write(&dir.path().join("observer.db"))
        .await
        .unwrap();
    if !fee_rows.is_empty() {
        observer
            .commit_fee_cycle(FeeCycleCommit {
                cycle_id: "c71-25-fixture".to_string(),
                started_at: NOW,
                completed_at: NOW,
                source_commit: "0".repeat(40),
                binary_sha256: "0".repeat(64),
                state_rows: fee_rows,
                ..Default::default()
            })
            .await
            .expect("seeds the fee state");
    }
    Fixture {
        production,
        observer,
        _dir: dir,
    }
}

/// A mature channel that routed yesterday, funded by the REMOTE peer.
fn seed_one_channel() -> String {
    format!(
        "INSERT INTO forwards (in_channel,out_channel,fee_msat,out_msat,timestamp)
         VALUES ('900x1x0','700x1x0', 5000, 1000000, {routed});
         INSERT INTO channel_costs (channel_id,peer_id,open_cost_sats,capacity_sats,opened_at)
         VALUES ('700x1x0','02aa', 1000, 5000000, {opened});",
        routed = NOW - DAY,
        opened = NOW - 400 * DAY,
    )
}

fn channels(scid: &str, opener: &str) -> Vec<serde_json::Value> {
    vec![json!({"short_channel_id": scid, "opener": opener, "peer_id": "02aa"})]
}

/// The whole point: the assembled channel carries facts that were read,
/// not defaults that were assumed.
#[tokio::test]
async fn a_fleet_pass_reports_the_routing_time_and_opener_it_actually_read() {
    let f = fixture(&seed_one_channel(), vec![]).await;
    let chans = channels("700x1x0", "remote");

    let fleet = gather_profitability(ProfitabilitySources {
        production_db: &f.production,
        observer: &f.observer,
        channels: &chans,
        now: NOW,
    })
    .await
    .expect("the stores are healthy");

    let p = fleet
        .profitability
        .get("700x1x0")
        .unwrap_or_else(|| panic!("skipped: {:?}", fleet.skipped));
    assert_eq!(
        p.last_routed,
        Some(NOW - DAY),
        "the fabricated `None` reported this 400-day-old channel as never \
         routed, which the classifier reads as 400 days idle"
    );
    assert_eq!(
        p.opener, "remote",
        "the fabricated \"local\" claimed this node paid the opening fee"
    );
}

/// The fabricated `last_routed: None` was not a cosmetic default: the
/// classifier substitutes `days_open` for `days_inactive` when there is no
/// routing time (py profitability_analyzer.py:2661-2663).
#[tokio::test]
async fn a_channel_that_routed_yesterday_is_not_reported_as_dead_capital() {
    let f = fixture(&seed_one_channel(), vec![]).await;
    let chans = channels("700x1x0", "remote");

    let fleet = gather_profitability(ProfitabilitySources {
        production_db: &f.production,
        observer: &f.observer,
        channels: &chans,
        now: NOW,
    })
    .await
    .expect("the stores are healthy");

    let p = fleet.profitability.get("700x1x0").expect("assembles");
    assert_ne!(
        p.classification.as_value(),
        "stagnant_candidate",
        "a channel that routed yesterday must not be classified from an \
         unconsulted routing time"
    );
}

/// Absence of an opener is a skip, never a `"local"` default -- the figure
/// is reported to the operator as a cost this node paid.
#[tokio::test]
async fn a_channel_absent_from_the_snapshot_is_skipped_not_defaulted_to_local() {
    let f = fixture(&seed_one_channel(), vec![]).await;
    let chans = channels("999x9x9", "local");

    let fleet = gather_profitability(ProfitabilitySources {
        production_db: &f.production,
        observer: &f.observer,
        channels: &chans,
        now: NOW,
    })
    .await
    .expect("the stores are healthy");

    assert!(
        fleet.profitability.is_empty(),
        "no channel may be classified without an opener"
    );
    let (scid, reason) = fleet.skipped.first().expect("the skip is reported");
    assert_eq!(scid, "700x1x0");
    assert!(
        reason.contains("profitability_opener_unavailable"),
        "the skip must name the missing source: {reason}"
    );
}

/// Both SCID spellings must land on one channel here too -- the snapshot
/// folds them, and an unfolded opener map would skip every `:`-spelled
/// channel as having no opener.
#[tokio::test]
async fn the_opener_map_folds_both_scid_spellings() {
    let f = fixture(&seed_one_channel(), vec![]).await;
    let chans = channels("700:1:0", "remote");

    let fleet = gather_profitability(ProfitabilitySources {
        production_db: &f.production,
        observer: &f.observer,
        channels: &chans,
        now: NOW,
    })
    .await
    .expect("the stores are healthy");

    let p = fleet
        .profitability
        .get("700x1x0")
        .unwrap_or_else(|| panic!("skipped: {:?}", fleet.skipped));
    assert_eq!(p.opener, "remote");
}

/// A stored posterior below 2500 widens the classifier's bands. Dropping
/// it makes the port HARSHER than Python on exactly the channels Python
/// protects.
///
/// `ChannelProfitability` does not carry the posterior -- it is an input,
/// not an output -- so the only honest assertion is the verdict it
/// changes. 880 sats earned against 1000 sats of cost is ROI -0.12:
/// underwater on the default band, break-even once a proven posterior
/// widens the underwater threshold to -0.15.
async fn classify_with_posterior(row: Option<FeeStateRow>) -> String {
    let seed = format!(
        "INSERT INTO forwards (in_channel,out_channel,fee_msat,out_msat,timestamp)
         VALUES ('900x1x0','700x1x0', 880000, 1000000, {routed});
         INSERT INTO channel_costs (channel_id,peer_id,open_cost_sats,capacity_sats,opened_at)
         VALUES ('700x1x0','02aa', 1000, 5000000, {opened});",
        routed = NOW - DAY,
        opened = NOW - 400 * DAY,
    );
    let f = fixture(&seed, row.into_iter().collect()).await;
    let chans = channels("700x1x0", "remote");

    let fleet = gather_profitability(ProfitabilitySources {
        production_db: &f.production,
        observer: &f.observer,
        channels: &chans,
        now: NOW,
    })
    .await
    .expect("the stores are healthy");

    fleet
        .profitability
        .get("700x1x0")
        .unwrap_or_else(|| panic!("skipped: {:?}", fleet.skipped))
        .classification
        .as_value()
        .to_string()
}

#[tokio::test]
async fn a_stored_fee_posterior_widens_the_band_the_fabricated_none_left_narrow() {
    assert_eq!(
        classify_with_posterior(None).await,
        "underwater",
        "precondition: this channel is underwater on the default band"
    );
    assert_eq!(
        classify_with_posterior(Some(FeeStateRow {
            channel_id: "700x1x0".to_string(),
            v2_state_json: r#"{"fee_state":{"thompson_state":{"posterior_variance":1200}}}"#
                .to_string(),
            last_update: NOW,
        }))
        .await,
        "break_even",
        "a proven fee posterior must reach the classifier; the fabricated \
         None made this port harsher than Python on exactly the channels \
         Python protects"
    );
}

/// One corrupt row must not blank the surface, and must not pass as a
/// channel that simply has no posterior.
#[tokio::test]
async fn a_corrupt_fee_posterior_skips_only_its_own_channel() {
    let seed = format!(
        "{}
         INSERT INTO channel_costs (channel_id,peer_id,open_cost_sats,capacity_sats,opened_at)
         VALUES ('800x1x0','02bb', 1000, 5000000, {opened});",
        seed_one_channel(),
        opened = NOW - 400 * DAY,
    );
    let f = fixture(
        &seed,
        vec![FeeStateRow {
            channel_id: "800x1x0".to_string(),
            v2_state_json: "{not json".to_string(),
            last_update: NOW,
        }],
    )
    .await;
    let chans = vec![
        json!({"short_channel_id": "700x1x0", "opener": "remote"}),
        json!({"short_channel_id": "800x1x0", "opener": "local"}),
    ];

    let fleet = gather_profitability(ProfitabilitySources {
        production_db: &f.production,
        observer: &f.observer,
        channels: &chans,
        now: NOW,
    })
    .await
    .expect("a corrupt row is a per-channel fact, not a store failure");

    assert!(
        fleet.profitability.contains_key("700x1x0"),
        "the healthy channel must still be classified"
    );
    assert!(!fleet.profitability.contains_key("800x1x0"));
    let reason = fleet
        .skipped
        .iter()
        .find(|(scid, _)| scid == "800x1x0")
        .map(|(_, reason)| reason.clone())
        .expect("the corrupt channel is reported");
    assert!(
        reason.contains("profitability_fee_state_unavailable"),
        "the skip must name the unreadable source: {reason}"
    );
}

/// C71-25 structural: one production-database await, one observer await.
/// Structural because a correct producer offers no seam to interleave a
/// concurrent write into -- the same reason the C71-21 pins are structural.
#[test]
fn the_producer_reads_each_store_exactly_once() {
    let source = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/profitability_assembler.rs"
    ))
    .unwrap();
    let after = source
        .split_once("pub async fn gather_profitability")
        .expect("the producer must exist")
        .1;
    let body = &after[..after.find("\n}\n").expect("a closed top-level item")];

    assert_eq!(
        body.matches(".await").count(),
        2,
        "exactly two awaits: one production snapshot, one fee-state read"
    );
    for split in [
        "per_channel_revenue(",
        "per_channel_costs(",
        "channel_history(",
    ] {
        assert!(
            !body.contains(split),
            "the producer must not reassemble the snapshot from separate \
             reads (found `{split}`)"
        );
    }
    assert!(
        !body.contains("listpeerchannels"),
        "the opener must come from the caller's already-fetched snapshot, \
         not a second query that could disagree with it"
    );
}

/// The zeroed diagnostics were the quietest of the four defaults:
/// `attempt_count: 0` fails the `>= 2` gate, so the ZOMBIE branch could
/// never fire for any channel, on any fleet, ever. Nothing looked wrong --
/// the class simply never appeared.
///
/// ROI -0.2 with two failed diagnostic attempts and ten days idle is
/// Python's ZOMBIE; the same channel with the diagnostics unread is not.
async fn classify_with_diagnostics(seed_diagnostics: bool) -> String {
    let diagnostics = if seed_diagnostics {
        format!(
            "INSERT INTO rebalance_history (from_channel,to_channel,rebalance_type,status,timestamp)
             VALUES ('800x1x0','700x1x0','diagnostic','failed',{recent}),
                    ('800x1x0','700x1x0','diagnostic','failed',{recent});",
            recent = NOW - DAY,
        )
    } else {
        String::new()
    };
    let seed = format!(
        "INSERT INTO forwards (in_channel,out_channel,fee_msat,out_msat,timestamp)
         VALUES ('900x1x0','700x1x0', 800000, 1000000, {routed});
         INSERT INTO channel_costs (channel_id,peer_id,open_cost_sats,capacity_sats,opened_at)
         VALUES ('700x1x0','02aa', 1000, 5000000, {opened});
         {diagnostics}",
        routed = NOW - 10 * DAY,
        opened = NOW - 400 * DAY,
    );
    let f = fixture(&seed, vec![]).await;
    let chans = channels("700x1x0", "remote");

    let fleet = gather_profitability(ProfitabilitySources {
        production_db: &f.production,
        observer: &f.observer,
        channels: &chans,
        now: NOW,
    })
    .await
    .expect("the stores are healthy");

    fleet
        .profitability
        .get("700x1x0")
        .unwrap_or_else(|| panic!("skipped: {:?}", fleet.skipped))
        .classification
        .as_value()
        .to_string()
}

#[tokio::test]
async fn read_diagnostics_let_the_zombie_branch_fire_at_all() {
    assert_ne!(
        classify_with_diagnostics(false).await,
        "zombie",
        "precondition: with no diagnostic rows this channel is not a zombie"
    );
    assert_eq!(
        classify_with_diagnostics(true).await,
        "zombie",
        "two failed diagnostic attempts must reach the classifier; the \
         zeroed default made ZOMBIE unreachable for every channel"
    );
}

/// The observer store's fee rows are keyed by whatever spelling the fee
/// controller wrote. An unfolded posterior map drops the posterior for
/// every `:`-spelled channel -- silently, because a dropped posterior is
/// indistinguishable from a channel that has none, and the only symptom is
/// a band that stays narrower than Python's.
#[tokio::test]
async fn a_colon_spelled_fee_row_still_reaches_its_channel() {
    let seed = format!(
        "INSERT INTO forwards (in_channel,out_channel,fee_msat,out_msat,timestamp)
         VALUES ('900x1x0','700x1x0', 880000, 1000000, {routed});
         INSERT INTO channel_costs (channel_id,peer_id,open_cost_sats,capacity_sats,opened_at)
         VALUES ('700x1x0','02aa', 1000, 5000000, {opened});",
        routed = NOW - DAY,
        opened = NOW - 400 * DAY,
    );
    let f = fixture(
        &seed,
        vec![FeeStateRow {
            channel_id: "700:1:0".to_string(),
            v2_state_json: r#"{"fee_state":{"thompson_state":{"posterior_variance":1200}}}"#
                .to_string(),
            last_update: NOW,
        }],
    )
    .await;
    let chans = channels("700x1x0", "remote");

    let fleet = gather_profitability(ProfitabilitySources {
        production_db: &f.production,
        observer: &f.observer,
        channels: &chans,
        now: NOW,
    })
    .await
    .expect("the stores are healthy");

    assert_eq!(
        fleet
            .profitability
            .get("700x1x0")
            .unwrap_or_else(|| panic!("skipped: {:?}", fleet.skipped))
            .classification
            .as_value(),
        "break_even",
        "a `:`-spelled fee row must widen the `x`-spelled channel's band"
    );
}

// ---------------------------------------------------------------------
// C71-27: the RPC caller's composition.
//
// `main.rs` is a binary no integration test can import, which is exactly
// where the previous fabricated wiring survived. These read its source.
// ---------------------------------------------------------------------

fn profitability_handler() -> String {
    let source = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/main.rs"))
        .expect("main.rs is readable");
    let after = source
        .split_once("&profitability_name,")
        .expect("the profitability RPC must be registered")
        .1;
    // Up to the start of the next registered method.
    after
        .split_once(".rpcmethod(")
        .map(|(handler, _)| handler.to_string())
        .unwrap_or(after.to_string())
}

#[test]
fn the_rpc_takes_one_fresh_bounded_snapshot_and_passes_that_same_one_in() {
    let handler = profitability_handler();
    assert_eq!(
        handler.matches("fetch_channel_snapshot").count(),
        1,
        "exactly one listpeerchannels snapshot per call"
    );
    assert_eq!(
        handler.matches("gather_profitability").count(),
        1,
        "exactly one producer call"
    );
    assert!(
        handler.contains("channels: &channels"),
        "the snapshot the caller took must be the snapshot the producer uses; \
         a second fetch could disagree and nothing downstream could tell"
    );
}

#[test]
fn the_rpc_no_longer_claims_the_pipeline_is_unported() {
    let handler = profitability_handler();
    for marker in [
        "build_profitability_channel_not_wired",
        "build_profitability_summary_not_wired",
    ] {
        assert!(
            !handler.contains(marker),
            "the pipeline is wired; `{marker}` would hide a real answer"
        );
    }
}

#[test]
fn an_unavailable_store_never_answers_with_pythons_unknown_channel_shape() {
    // `build_profitability_channel(id, None)` emits Python's own
    // "No data available". Reaching for it on a store outage would tell
    // the operator to close a channel that is fine. It may appear in the
    // handler exactly once -- the genuine unknown-channel branch, taken
    // only after a successful pass that neither classified nor skipped it.
    // Comments stripped and whitespace normalised: prose ABOUT a call is
    // not a call, and rustfmt's line breaks must not decide whether this
    // pin holds.
    let handler = profitability_handler();
    let code: String = handler
        .lines()
        .map(|line| line.split("//").next().unwrap_or(""))
        .collect::<Vec<_>>()
        .join(" ");
    let flat = code.split_whitespace().collect::<Vec<_>>().join(" ");
    assert_eq!(
        flat.matches("build_profitability_channel( id, None,")
            .count()
            + flat
                .matches("build_profitability_channel(id, None)")
                .count(),
        1,
        "Python's unknown-channel answer belongs to exactly one branch: {flat}"
    );
    assert!(
        handler.contains("build_profitability_channel_unavailable"),
        "a skipped channel needs a shape of its own"
    );
    assert!(
        handler.contains("profitability_store_not_configured"),
        "an unconfigured store must be named, not silently empty"
    );
}

/// C71-29: a channel whose posterior could not be read must never reach a
/// fee multiplier at all.
///
/// The multiplier is computed from an assembled `ChannelProfitability`, and
/// a channel with a malformed posterior is SKIPPED before assembly -- so
/// there is no path on which a null, a fabricated 1.0, or a stale prior
/// value is emitted. The refusal shape carries no `profitability` object,
/// which is what makes that structural rather than incidental.
#[tokio::test]
async fn a_channel_skipped_for_an_unreadable_posterior_emits_no_fee_multiplier() {
    let f = fixture(
        &seed_one_channel(),
        vec![FeeStateRow {
            channel_id: "700x1x0".to_string(),
            v2_state_json: "{not json".to_string(),
            last_update: NOW,
        }],
    )
    .await;
    let chans = channels("700x1x0", "remote");

    let fleet = gather_profitability(ProfitabilitySources {
        production_db: &f.production,
        observer: &f.observer,
        channels: &chans,
        now: NOW,
    })
    .await
    .expect("a corrupt row is a per-channel fact");

    assert!(
        !fleet.profitability.contains_key("700x1x0"),
        "the channel must not be assembled at all"
    );
    let (_, reason) = fleet
        .skipped
        .iter()
        .find(|(scid, _)| scid == "700x1x0")
        .expect("the skip is reported");

    let response =
        revops::rpc_profitability::build_profitability_channel_unavailable("700x1x0", reason);
    assert!(
        response.get("profitability").is_none(),
        "a refusal carries no profitability object, so no multiplier can be \
         read from it: {response:?}"
    );
    assert!(
        response.get("_gaps").is_none(),
        "and no gap marker either: {response:?}"
    );
}
