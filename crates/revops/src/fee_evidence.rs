//! `EvidenceSnapshot`: the live, per-cycle-frozen [`FeeEvidence`]
//! implementation over the production DB (read-only) + prefetched RPC
//! snapshots (Phase 4b Task 2, checklist item 2).
//!
//! ## Two-phase construction
//!
//! 1. **Async half, BEFORE the cycle starts:** [`prefetch_rpc`] fetches
//!    every RPC snapshot the cycle can ever need (`getinfo` node id,
//!    `listpeerchannels`, ONE full `listchannels`, `feerates
//!    style=perkb`) over the `lightning-rpc` unix socket, using the same
//!    `cln_rpc::ClnRpc` + `revops_rpc::call_with_timeout` pattern as
//!    `hydration`/`config_resolve`. Everything comes back owned in an
//!    [`RpcPrefetch`].
//! 2. **Sync half, ON the cycle thread:** [`build_evidence_snapshot`]
//!    opens a direct read-only `rusqlite::Connection`
//!    ([`revops_db::open_read_only`] -- NOT the async actor;
//!    `FeeEvidence` is a sync trait and the snapshot lives on the cycle
//!    thread), then issues `BEGIN` and runs its eager reads. The
//!    production DB is WAL (`database.py` sets `PRAGMA
//!    journal_mode=WAL`), so that open read transaction pins a stable
//!    snapshot for the whole cycle WITHOUT ever blocking Python's writer
//!    -- every later per-channel query observes the DB exactly as it was
//!    at cycle start, satisfying the Global Constraint that the evidence
//!    "must not issue RPC or observe new DB rows after the cycle starts".
//!
//! No method on the snapshot performs RPC -- there is no RPC handle
//! anywhere in the struct, so "one `listchannels` prefetch per cycle" is
//! enforced by construction, not by discipline.
//!
//! ## Python line anchors
//!
//! DB queries are statement-for-statement ports of their
//! `modules/database.py` namesakes in `~/bin/cl_revenue_ops-port` (branch
//! `port`); RPC-shaped evidence ports the `modules/fee_controller.py`
//! `_get_channels_info_live` (8412-8489) / `_get_peer_inbound_channels_live`
//! (3271-3304) / `_get_dynamic_chain_costs_live` (8253-8311) trio. Exact
//! lines are cited per method below.
//!
//! ## Write behavior
//!
//! NONE. Python's read paths occasionally write as a side effect
//! (`get_channel_probe` deletes expired probe rows, `get_policy` deletes
//! expired policies). This surface returns the same VALUES Python would
//! return while never performing those deletes -- the rows' absence from
//! Python's next read is Python's own doing during the dry-run window,
//! and the observable evidence for the current cycle is identical.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use anyhow::{Context, Result};
use cln_rpc::ClnRpc;
use revops_analytics::policy::{FeeStrategy, PeerPolicy, RebalanceMode};
use revops_core::msat::{base_to_sats_ceil, base_to_sats_floor, parse_msat};
use revops_fees::cycle::{ChannelInfo, ChannelStateRow, FeeEvidence, GossipRow, PeerFeeHistory};
use revops_fees::drain::NodeChannel;
use revops_fees::floors::{
    ChainCosts, FlowWindow, PeerLatency, RebalanceCostSample, REBALANCE_FLOOR_MIN_SAMPLES,
    REBALANCE_FLOOR_WINDOW_DAYS,
};
use revops_fees::market::FrozenObservations;
use revops_fees::pyrand::DecisionInputError;
use rusqlite::{Connection, OptionalExtension};
use serde_json::{json, Value};

/// Matches `Config.rpc_timeout_seconds`'s default (modules/config.py:734),
/// same documented constant as `hydration.rs` -- the fee scheduler (T6)
/// may later thread the resolved config value through instead.
const RPC_TIMEOUT_SECONDS: u64 = 15;

/// `FeeController.FLOW_BALANCED_WINDOW_SECONDS` (fee_controller.py:2772):
/// the 7d directional-flow window behind `_get_flow_window_map`.
const FLOW_BALANCED_WINDOW_SECONDS: i64 = 7 * 86_400;

/// `Database.get_channel_probe`'s `max_age_seconds` default
/// (database.py:2144): exploration flags auto-expire after 24h.
const PROBE_MAX_AGE_SECONDS: i64 = 86_400;

// ---------------------------------------------------------------------------
// Async half: RPC prefetch
// ---------------------------------------------------------------------------

/// Everything the cycle needs from lightningd, fetched ONCE before the
/// cycle starts and handed over owned. Fields are raw JSON so tests can
/// can them without a socket; parsing/trimming happens in
/// [`build_evidence_snapshot`].
pub struct RpcPrefetch {
    /// `getinfo`'s `id` (py `_get_our_id`, fee_controller.py:3230-3236).
    pub our_node_id: String,
    /// `listpeerchannels`'s raw `channels` array.
    pub peer_channels: Vec<Value>,
    /// The ONE full `listchannels` `channels` array for this cycle --
    /// grouped by `destination` at build time; never re-fetched, and
    /// never fetched per peer.
    pub gossip_channels: Vec<Value>,
    /// Raw `feerates style=perkb` response; `None` = the RPC failed,
    /// which is exactly Python `_get_dynamic_chain_costs_live`'s
    /// `except -> None` branch (fee_controller.py:8309-8311).
    pub feerates: Option<Value>,
}

/// One RPC call over a fresh `cln_rpc::ClnRpc` connection (same
/// fresh-connection-per-call rationale as `hydration::call_listforwards`).
async fn call_rpc(socket_path: &Path, method: &str, params: Value) -> Result<Value> {
    let mut rpc = ClnRpc::new(socket_path)
        .await
        .with_context(|| format!("connect lightning-rpc socket {}", socket_path.display()))?;
    rpc.call_raw::<Value, Value>(method, &params)
        .await
        .map_err(|e| anyhow::anyhow!("{method} RPC error: {e}"))
}

async fn call_with_timeout(socket_path: &Path, method: &str, params: Value) -> Result<Value> {
    revops_rpc::call_with_timeout(
        method,
        RPC_TIMEOUT_SECONDS,
        call_rpc(socket_path, method, params),
    )
    .await
    .map_err(anyhow::Error::from)
}

fn channels_array(response: &Value) -> Vec<Value> {
    response
        .get("channels")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

/// Fetch ALL RPC snapshots BEFORE the cycle starts. Returns everything
/// owned.
///
/// Failure contract: `getinfo`/`listpeerchannels`/`listchannels` errors
/// are `Err` -- the scheduler skips the cycle rather than running it on
/// evidence Python didn't run on. A `feerates` error maps to
/// `feerates: None` (py `_get_dynamic_chain_costs_live` returns `None` on
/// any exception and the cycle proceeds, fee_controller.py:8309-8311).
pub async fn prefetch_rpc(socket_path: &std::path::Path) -> Result<RpcPrefetch> {
    let info = call_with_timeout(socket_path, "getinfo", json!({})).await?;
    let our_node_id = info
        .get("id")
        .and_then(Value::as_str)
        .context("getinfo response missing 'id'")?
        .to_string();

    let peer_channels =
        channels_array(&call_with_timeout(socket_path, "listpeerchannels", json!({})).await?);

    // The single per-cycle gossip fetch (grouped by destination at build
    // time). Python instead issues `listchannels destination=<peer>` per
    // peer under a TTL cache; one full fetch is this port's per-cycle-
    // frozen equivalent (plan Task 2 contract point 2).
    let gossip_channels =
        channels_array(&call_with_timeout(socket_path, "listchannels", json!({})).await?);

    let feerates = match call_with_timeout(socket_path, "feerates", json!({"style": "perkb"})).await
    {
        Ok(v) => Some(v),
        Err(e) => {
            eprintln!("revops: feerates prefetch failed ({e}); chain costs unavailable this cycle");
            None
        }
    };

    Ok(RpcPrefetch {
        our_node_id,
        peer_channels,
        gossip_channels,
        feerates,
    })
}

// ---------------------------------------------------------------------------
// JSON helpers (Python dict.get semantics)
// ---------------------------------------------------------------------------

/// `container.get(key)` where a JSON `null` value counts as "missing"
/// (Python's `None`).
fn get_non_null<'a>(container: &'a Value, key: &str) -> Option<&'a Value> {
    container.get(key).filter(|v| !v.is_null())
}

/// Python truthiness for the JSON values that appear in RPC responses:
/// `null`/`false`/`0`/`0.0`/`""` are falsy; everything else is truthy.
fn json_truthy(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().map(|f| f != 0.0).unwrap_or(false),
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
    }
}

fn json_i64(container: &Value, key: &str, default: i64) -> i64 {
    match get_non_null(container, key) {
        Some(v) => parse_msat(v),
        None => default,
    }
}

fn json_str(container: &Value, key: &str, default: &str) -> String {
    get_non_null(container, key)
        .and_then(Value::as_str)
        .unwrap_or(default)
        .to_string()
}

/// `utils.normalize_scid` (modules/utils.py:13-20).
fn normalize_scid(scid: &str) -> String {
    scid.replace(':', "x")
}

/// `database._scid_aliases` (database.py:40-52): canonical and legacy SCID
/// spellings for read-side lookups.
fn scid_aliases(channel_id: &str) -> Vec<String> {
    let canonical = normalize_scid(channel_id);
    let mut aliases: Vec<String> = Vec::new();
    for candidate in [canonical.clone(), channel_id.to_string()] {
        if !candidate.is_empty() && !aliases.contains(&candidate) {
            aliases.push(candidate);
        }
    }
    if canonical.matches('x').count() == 2 {
        let legacy_colon = canonical.replace('x', ":");
        if !aliases.contains(&legacy_colon) {
            aliases.push(legacy_colon);
        }
    }
    if aliases.is_empty() {
        aliases.push(String::new());
    }
    aliases
}

fn sql_placeholders(n: usize) -> String {
    vec!["?"; n].join(",")
}

// ---------------------------------------------------------------------------
// RPC-shaped evidence builders
// ---------------------------------------------------------------------------

/// Port of `_get_channels_info_live` (fee_controller.py:8412-8489):
/// CHANNELD_NORMAL rows keyed by canonical scid (falling back to the full
/// channel id), with `updates.local` fee/HTLC precedence and F4 HTLC
/// slot-usage fields.
fn build_channels_info(peer_channels: &[Value]) -> BTreeMap<String, ChannelInfo> {
    let mut channels = BTreeMap::new();
    for channel in peer_channels.iter().filter(|c| c.is_object()) {
        if json_str(channel, "state", "") != "CHANNELD_NORMAL" {
            continue;
        }

        let short_channel_id = normalize_scid(&json_str(channel, "short_channel_id", ""));
        let full_channel_id = json_str(channel, "channel_id", "");
        // py: `canonical_channel_id = short_channel_id or full_channel_id`.
        let canonical = if !short_channel_id.is_empty() {
            short_channel_id.clone()
        } else {
            full_channel_id.clone()
        };
        if canonical.is_empty() {
            continue;
        }

        let spendable_msat = json_i64(channel, "spendable_msat", 0);
        let receivable_msat = json_i64(channel, "receivable_msat", 0);

        // py: `total_msat_raw = channel.get("total_msat") or
        // channel.get("capacity_msat")` -- Python `or` skips FALSY values
        // (null/0/""), then `parse_msat(raw) if raw else spendable +
        // receivable`.
        let total_msat_raw = ["total_msat", "capacity_msat"]
            .iter()
            .filter_map(|k| channel.get(*k))
            .find(|v| json_truthy(v));
        let total_msat = match total_msat_raw {
            Some(raw) => parse_msat(raw),
            None => spendable_msat + receivable_msat,
        };

        // Fee info: updates.local first, top-level fallback (py 8440-8448).
        let empty = json!({});
        let local_updates = channel
            .get("updates")
            .and_then(|u| u.get("local"))
            .filter(|v| v.is_object())
            .unwrap_or(&empty);
        let fee_base_msat = match get_non_null(local_updates, "fee_base_msat") {
            Some(v) => parse_msat(v),
            None => json_i64(channel, "fee_base_msat", 0),
        };
        let fee_proportional_millionths =
            match get_non_null(local_updates, "fee_proportional_millionths") {
                Some(v) => parse_msat(v),
                None => json_i64(channel, "fee_proportional_millionths", 0),
            };

        // `_extract_local_htlc_bounds` (py 7403-7428): first PRESENT key
        // wins (presence, not truthiness), parsed through parse_msat.
        let first_present_msat = |candidates: &[(&Value, &str)]| -> i64 {
            for (container, key) in candidates {
                if let Some(v) = container.get(*key) {
                    return parse_msat(v);
                }
            }
            0
        };
        let htlc_minimum_msat = first_present_msat(&[
            (local_updates, "htlc_minimum_msat"),
            (channel, "minimum_htlc_out_msat"),
            (channel, "htlc_minimum_msat"),
            (channel, "htlc_min_msat"),
        ]);
        let htlc_maximum_msat = first_present_msat(&[
            (local_updates, "htlc_maximum_msat"),
            (channel, "maximum_htlc_out_msat"),
            (channel, "htlc_maximum_msat"),
            (channel, "htlc_max_msat"),
        ]);

        // F4 HTLC slot usage (py 8454-8462): only OUR-direction HTLCs.
        let htlcs = channel.get("htlcs").and_then(Value::as_array);
        let has_htlc_data = htlcs.is_some();
        let our_htlcs_in_flight = htlcs
            .map(|list| {
                list.iter()
                    .filter(|h| h.get("direction").and_then(Value::as_str) == Some("out"))
                    .count() as i64
            })
            .unwrap_or(0);

        // py: `base_to_sats_floor(int(total_msat)) if total_msat else 0`.
        let capacity_sats = if total_msat != 0 {
            base_to_sats_floor(total_msat.max(0) as u64) as i64
        } else {
            0
        };

        channels.insert(
            canonical.clone(),
            ChannelInfo {
                channel_id: canonical,
                short_channel_id,
                full_channel_id: json_str(channel, "channel_id", ""),
                peer_id: json_str(channel, "peer_id", ""),
                capacity_sats,
                spendable_msat,
                receivable_msat,
                fee_base_msat,
                fee_proportional_millionths,
                htlc_minimum_msat,
                htlc_min_msat: htlc_minimum_msat,
                htlc_maximum_msat,
                htlc_max_msat: htlc_maximum_msat,
                opener: json_str(channel, "opener", "local"),
                has_htlc_data,
                max_accepted_htlcs: json_i64(channel, "max_accepted_htlcs", 483),
                our_htlcs_in_flight,
            },
        );
    }
    channels
}

/// The `listpeerchannels`-shaped rows the node-drain-bias aggregate
/// consumes (fee_controller.py:4613-4623 passes the RAW `channels` array;
/// the CHANNELD_NORMAL filter lives inside
/// `drain::compute_node_receivable_ratio`).
fn build_node_channels(peer_channels: &[Value]) -> Vec<NodeChannel> {
    peer_channels
        .iter()
        .filter(|c| c.is_object())
        .map(|c| NodeChannel {
            state: json_str(c, "state", ""),
            to_us_msat: json_i64(c, "to_us_msat", 0),
            total_msat: json_i64(c, "total_msat", 0),
        })
        .collect()
}

/// One trimmed gossip row (py `_GOSSIP_CHANNEL_FIELDS`,
/// fee_controller.py:3242-3251). `base_fee_msat` keeps
/// `_is_cln_default_fee`'s key preference (py 3424-3426):
/// `base_fee_millisatoshi` (pre-24.x) first, `fee_base_msat` (post-24.x)
/// fallback, `None` when both are absent.
fn gossip_row_from_value(ch: &Value) -> GossipRow {
    let base_fee_msat = get_non_null(ch, "base_fee_millisatoshi")
        .or_else(|| get_non_null(ch, "fee_base_msat"))
        .map(parse_msat);
    GossipRow {
        source: json_str(ch, "source", ""),
        active: get_non_null(ch, "active")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        fee_per_millionth: json_i64(ch, "fee_per_millionth", 0),
        satoshis: get_non_null(ch, "satoshis").map(parse_msat),
        amount_msat: get_non_null(ch, "amount_msat").map(parse_msat),
        last_update: json_i64(ch, "last_update", 0),
        base_fee_msat,
    }
}

/// Inverse of [`gossip_row_from_value`] for the per-cycle memo: the same
/// trimmed-dict shape Python's `_neighbor_fee_cache` stores (missing
/// optionals omit their key, exactly like the `{k: ch[k] ... if k in ch}`
/// comprehension at fee_controller.py:3295-3299).
fn gossip_row_to_value(row: &GossipRow) -> Value {
    let mut obj = serde_json::Map::new();
    obj.insert("source".into(), json!(row.source));
    obj.insert("active".into(), json!(row.active));
    obj.insert("fee_per_millionth".into(), json!(row.fee_per_millionth));
    if let Some(s) = row.satoshis {
        obj.insert("satoshis".into(), json!(s));
    }
    if let Some(a) = row.amount_msat {
        obj.insert("amount_msat".into(), json!(a));
    }
    obj.insert("last_update".into(), json!(row.last_update));
    if let Some(b) = row.base_fee_msat {
        obj.insert("base_fee_millisatoshi".into(), json!(b));
    }
    Value::Object(obj)
}

/// Group the single `listchannels` prefetch by `destination` -- the
/// build-time half of contract point 2 ("one listchannels prefetch per
/// cycle, grouped by destination"). Row order within a destination
/// preserves lightningd's response order, as Python's per-destination
/// `listchannels` calls would.
fn group_gossip_by_destination(gossip_channels: &[Value]) -> HashMap<String, Vec<GossipRow>> {
    let mut by_destination: HashMap<String, Vec<GossipRow>> = HashMap::new();
    for ch in gossip_channels.iter().filter(|c| c.is_object()) {
        let destination = json_str(ch, "destination", "");
        if destination.is_empty() {
            continue;
        }
        by_destination
            .entry(destination)
            .or_default()
            .push(gossip_row_from_value(ch));
    }
    by_destination
}

/// Port of `_get_dynamic_chain_costs_live` (fee_controller.py:8253-8311)
/// over the prefetched `feerates style=perkb` response.
///
/// `None` in exactly one case: the feerates RPC failed (prefetch stored
/// `None`), mirroring Python's `except -> None`. A successful response --
/// even an empty one -- ALWAYS produces `Some`: every missing/zero `perkb`
/// candidate falls through Python's `or` chain to the 1000 sat/kvB
/// fallback, and the sanity clamps (`open >= 500`, `close >= 300`) make an
/// empty or all-zero result impossible (py 8293-8295).
fn chain_costs_from_feerates(feerates: Option<&Value>) -> Option<ChainCosts> {
    let fr = feerates?;
    let perkb = fr.get("perkb");

    // py: `perkb.get("opening") or perkb.get("mutual_close") or
    // perkb.get("unilateral_close") or perkb.get("floor") or 1000` --
    // Python `or` skips falsy candidates (missing, null, 0/0.0).
    let sat_per_kvb = ["opening", "mutual_close", "unilateral_close", "floor"]
        .iter()
        .filter_map(|k| perkb.and_then(|p| p.get(*k)))
        .filter(|v| json_truthy(v))
        .find_map(Value::as_f64)
        .unwrap_or(1000.0);

    let sat_per_vbyte = sat_per_kvb / 1000.0;

    const FUNDING_TX_VBYTES: f64 = 140.0;
    const CLOSE_TX_VBYTES: f64 = 200.0;
    // py `int(...)` truncates toward zero; then max(min(...)) bounds.
    let open_cost_sats = ((sat_per_vbyte * FUNDING_TX_VBYTES).trunc() as i64).clamp(500, 50_000);
    let close_cost_sats = ((sat_per_vbyte * CLOSE_TX_VBYTES).trunc() as i64).clamp(300, 50_000);

    Some(ChainCosts {
        open_cost_sats,
        close_cost_sats,
        sat_per_vbyte,
    })
}

// ---------------------------------------------------------------------------
// EvidenceSnapshot
// ---------------------------------------------------------------------------

/// Per-cycle-frozen [`FeeEvidence`]: owned prefetched RPC data + a
/// read-only DB `Connection` held inside an open read transaction (see
/// module docs) + a `RefCell` gossip memo.
pub struct EvidenceSnapshot {
    conn: Connection,
    now: i64,
    our_node_id: String,
    channel_states: Vec<ChannelStateRow>,
    channels_info: BTreeMap<String, ChannelInfo>,
    node_channels: Vec<NodeChannel>,
    chain_costs: Option<ChainCosts>,
    gossip_by_destination: HashMap<String, Vec<GossipRow>>,
    /// Per-cycle compute-once memo, the same `FrozenObservations` the
    /// market layer uses (py `_frozen_observation`, key
    /// `("inbound_channels", peer_id)`); interior mutability because the
    /// trait reads through `&self`.
    gossip_memo: RefCell<FrozenObservations>,
    flow_windows: HashMap<String, FlowWindow>,
    policies: HashMap<String, PeerPolicy>,
    mempool_ma_24h: f64,
}

/// Sync half, called ON the cycle thread: opens the read-only
/// `Connection`, pins the per-cycle snapshot (`BEGIN` + first read), and
/// runs the eager batch reads. `now` is the cycle's single clock read
/// (Global Constraint: clock once per cycle), threaded into every
/// windowed query below in place of Python's per-call `time.time()`.
pub fn build_evidence_snapshot(
    db_path: &std::path::Path,
    rpc: RpcPrefetch,
    now: i64,
) -> Result<EvidenceSnapshot> {
    let conn = revops_db::open_read_only(db_path)?;
    // Pin the frozen view: a deferred read transaction; the immediately
    // following channel_states SELECT establishes the WAL read snapshot.
    // Held for the snapshot's whole life (dropped connection rolls back).
    // WAL readers never block the (Python) writer.
    conn.execute_batch("BEGIN")
        .context("begin snapshot read transaction")?;

    let channel_states = read_channel_states(&conn)?;
    let flow_windows = read_flow_windows(&conn, now - FLOW_BALANCED_WINDOW_SECONDS)?;
    let policies = read_policies(&conn, now)?;
    let mempool_ma_24h = read_mempool_ma(&conn, now - 86_400)?;

    Ok(EvidenceSnapshot {
        now,
        our_node_id: rpc.our_node_id,
        channel_states,
        channels_info: build_channels_info(&rpc.peer_channels),
        node_channels: build_node_channels(&rpc.peer_channels),
        chain_costs: chain_costs_from_feerates(rpc.feerates.as_ref()),
        gossip_by_destination: group_gossip_by_destination(&rpc.gossip_channels),
        gossip_memo: RefCell::new(FrozenObservations::new()),
        flow_windows,
        policies,
        mempool_ma_24h,
        conn,
    })
}

/// Port of `Database.get_all_channel_states` (database.py:1803-1807). The
/// SQL text is VERBATIM Python's -- including its ORDER BY -- and result
/// order is preserved into the `Vec` (contract point 1: `channel_states()`
/// row order):
///
/// ```sql
/// SELECT * FROM channel_states ORDER BY state, flow_ratio DESC
/// ```
///
/// `pub(crate)`: T7's `fee_scheduler::PolicyChanged` handler reuses this
/// exact query (a fresh, unpinned read -- no per-cycle snapshot semantics
/// needed for an out-of-cycle wake) rather than duplicating the SQL.
pub(crate) fn read_channel_states(conn: &Connection) -> Result<Vec<ChannelStateRow>> {
    let mut stmt = conn.prepare("SELECT * FROM channel_states ORDER BY state, flow_ratio DESC")?;
    let rows = stmt
        .query_map([], |row| {
            Ok(ChannelStateRow {
                channel_id: row.get("channel_id")?,
                peer_id: row.get("peer_id")?,
                state: row.get("state")?,
                updated_at: row.get("updated_at")?,
                kalman_flow_ratio: row.get("kalman_flow_ratio")?,
                kalman_velocity: row.get("kalman_velocity")?,
            })
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(rows)
}

/// Port of `Database.get_all_channel_flow_windows` (database.py:5430-5462)
/// at `since = now - FLOW_BALANCED_WINDOW_SECONDS`, the batch behind
/// `_get_flow_window_map` (fee_controller.py:2777-2793). The count element
/// of Python's 3-tuple is dropped -- `FlowWindow` carries the two fields
/// `_is_flow_balanced_router` reads.
fn read_flow_windows(conn: &Connection, since: i64) -> Result<HashMap<String, FlowWindow>> {
    let mut result: HashMap<String, FlowWindow> = HashMap::new();

    let mut out_stmt = conn.prepare(
        "SELECT out_channel AS ch, COALESCE(SUM(out_msat), 0) AS m, COUNT(*) AS c
            FROM forwards
            WHERE timestamp > ?1 AND out_channel IS NOT NULL
            GROUP BY out_channel",
    )?;
    let out_rows = out_stmt.query_map([since], |row| {
        Ok((row.get::<_, String>("ch")?, row.get::<_, i64>("m")?))
    })?;
    for row in out_rows {
        let (ch, msat) = row?;
        result.insert(
            ch,
            FlowWindow {
                out_sats: base_to_sats_floor(msat.max(0) as u64) as i64,
                in_sats: 0,
            },
        );
    }

    let mut in_stmt = conn.prepare(
        "SELECT in_channel AS ch, COALESCE(SUM(in_msat), 0) AS m, COUNT(*) AS c
            FROM forwards
            WHERE timestamp > ?1 AND in_channel IS NOT NULL
            GROUP BY in_channel",
    )?;
    let in_rows = in_stmt.query_map([since], |row| {
        Ok((row.get::<_, String>("ch")?, row.get::<_, i64>("m")?))
    })?;
    for row in in_rows {
        let (ch, msat) = row?;
        let entry = result.entry(ch).or_insert(FlowWindow {
            out_sats: 0,
            in_sats: 0,
        });
        entry.in_sats = base_to_sats_floor(msat.max(0) as u64) as i64;
    }

    Ok(result)
}

/// Port of `PolicyManager._load_cache` + `_row_to_policy`
/// (policy_manager.py:342-440): every `peer_policies` row, parsed with
/// per-row isolation (PM-I13 -- a corrupt row degrades that peer to the
/// default policy instead of poisoning the load), expired policies
/// skipped at load (v2.0). Python's expired-policy DB delete
/// (`get_policy` -> `_delete_expired_policy`) is NOT performed here --
/// read-only surface; the returned VALUE (the default policy) is
/// identical.
fn read_policies(conn: &Connection, now: i64) -> Result<HashMap<String, PeerPolicy>> {
    let mut stmt = conn.prepare("SELECT * FROM peer_policies")?;
    let raw_rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>("peer_id")?,
            row.get::<_, Option<String>>("strategy")?,
            row.get::<_, Option<String>>("rebalance_mode")?,
            row.get::<_, Option<i64>>("fee_ppm_target")?,
            row.get::<_, Option<String>>("tags")?,
            row.get::<_, Option<i64>>("updated_at")?,
            row.get::<_, Option<f64>>("fee_multiplier_min")?,
            row.get::<_, Option<f64>>("fee_multiplier_max")?,
            row.get::<_, Option<i64>>("expires_at")?,
        ))
    })?;

    let mut cache = HashMap::new();
    for row in raw_rows {
        let Ok((
            peer_id,
            strategy,
            rebalance_mode,
            fee_ppm_target,
            tags_json,
            updated_at,
            fee_multiplier_min,
            fee_multiplier_max,
            expires_at,
        )) = row
        else {
            // PM-I13 per-row isolation: skip, peer degrades to default.
            continue;
        };

        // py: `tags = json.loads(row['tags'] or '[]')`, corrupt ->
        // empty; non-list -> []. Non-string elements are dropped
        // (PeerPolicy.tags is Vec<String>; py keeps them but every
        // consumer string-compares, so membership behavior is identical).
        let tags: Vec<String> = tags_json
            .as_deref()
            .filter(|s| !s.is_empty())
            .and_then(|s| serde_json::from_str::<Value>(s).ok())
            .and_then(|v| match v {
                Value::Array(items) => Some(
                    items
                        .into_iter()
                        .filter_map(|i| i.as_str().map(str::to_string))
                        .collect(),
                ),
                _ => None,
            })
            .unwrap_or_default();

        let policy = PeerPolicy {
            peer_id: peer_id.clone(),
            // Invalid enum strings degrade exactly like Python's
            // ValueError branches (policy_manager.py:410-428).
            strategy: strategy
                .as_deref()
                .and_then(FeeStrategy::from_value)
                .unwrap_or(FeeStrategy::Dynamic),
            rebalance_mode: rebalance_mode
                .as_deref()
                .and_then(RebalanceMode::from_value)
                .unwrap_or(RebalanceMode::Enabled),
            fee_ppm_target,
            tags,
            updated_at: updated_at.unwrap_or(0),
            fee_multiplier_min,
            fee_multiplier_max,
            expires_at,
        };
        // v2.0: skip expired policies during cache load (get_policy's
        // second expiry check collapses into this one -- the snapshot's
        // `now` is frozen).
        if policy.is_expired(now) {
            continue;
        }
        cache.insert(peer_id, policy);
    }
    Ok(cache)
}

/// Port of `Database.get_mempool_ma(86400)` (database.py:7696-7712):
/// `SELECT AVG(sat_per_vbyte) FROM mempool_fee_history WHERE timestamp >=
/// ?`, falsy result (`NULL` or `0.0`) -> `1.0`.
fn read_mempool_ma(conn: &Connection, cutoff: i64) -> Result<f64> {
    let avg: Option<f64> = conn.query_row(
        "SELECT AVG(sat_per_vbyte) as avg_fee FROM mempool_fee_history WHERE timestamp >= ?1",
        [cutoff],
        |row| row.get(0),
    )?;
    Ok(match avg {
        Some(v) if v != 0.0 => v,
        _ => 1.0,
    })
}

impl EvidenceSnapshot {
    /// The snapshot's pinned read-only `Connection` (open read
    /// transaction, WAL view frozen at cycle start). T6's scheduler passes
    /// this to `fee_state::rehydrate` so per-cycle hydration observes the
    /// EXACT same DB snapshot as every other evidence read this cycle --
    /// reusing the connection is what makes hydrate-vs-evidence skew
    /// structurally impossible. Read-only by construction
    /// (`revops_db::open_read_only`), so handing it out cannot widen the
    /// write surface.
    pub fn conn(&self) -> &Connection {
        &self.conn
    }

    /// Scalar i64 query over the frozen connection. Trait methods cannot
    /// surface `Result`; a query error here (only reachable via schema
    /// drift -- the SQL is static and the connection is healthy by
    /// construction) logs loudly and returns `default` rather than
    /// panicking the cycle thread.
    fn query_i64(&self, sql: &str, params: impl rusqlite::Params, default: i64) -> i64 {
        match self
            .conn
            .query_row(sql, params, |row| row.get::<_, Option<i64>>(0))
        {
            Ok(v) => v.unwrap_or(default),
            Err(e) => {
                eprintln!("revops: evidence query failed ({e}): {sql}");
                default
            }
        }
    }
}

impl EvidenceSnapshot {
    pub fn our_node_id(&self) -> String {
        self.our_node_id.clone()
    }

    /// Contract point 1: Python's row order (`ORDER BY state, flow_ratio
    /// DESC` -- see [`read_channel_states`] for the verbatim SQL) is
    /// preserved into the returned `Vec`.
    pub fn channel_states(&self) -> Vec<ChannelStateRow> {
        self.channel_states.clone()
    }

    pub fn channels_info(&self) -> BTreeMap<String, ChannelInfo> {
        self.channels_info.clone()
    }

    /// Contract point 3: the FALSINESS mapping. Python's use sites treat
    /// a falsy chain-costs dict as "no chain costs" -- the gate that
    /// matters for RNG parity is `if cfg.enable_vegas_reflex and
    /// chain_costs:` (fee_controller.py:4584): only a TRUTHY dict lets
    /// the Vegas update run, and the Rust cycle draws from the shared
    /// `PyRandom` inside that same branch (`cycle.rs` `run_fee_cycle`,
    /// `vegas::vegas_update(.., deps.rng, ..)`), so Some/None here decides
    /// whether the RNG stream advances this cycle. (The floor path's `if
    /// dynamic_costs:` at py 8158-8160 keys off the same value.)
    ///
    /// The mapping ([`chain_costs_from_feerates`]): `None` exactly when
    /// Python's `if not chain_costs:` would fire -- i.e. ONLY when the
    /// feerates RPC failed (`_get_dynamic_chain_costs_live`'s `except ->
    /// None`, py 8309-8311). A successful response can never be
    /// empty/all-zero: the `or 1000` fallback plus the `max(500,..)`/
    /// `max(300,..)` sanity clamps (py 8270-8295) guarantee a truthy
    /// 3-key dict, so an "empty dict / all-zero row" can only mean
    /// `None` was already the answer.
    pub fn chain_costs(&self) -> Option<ChainCosts> {
        self.chain_costs
    }

    /// Port of `Database.get_volume_since` (database.py:5380-5404):
    /// `SUM(out_msat)` over `out_channel = ? AND timestamp > ?` (strict),
    /// msat -> sats via floor.
    pub fn volume_since(&self, channel_id: &str, since: i64) -> i64 {
        let msat = self.query_i64(
            "SELECT COALESCE(SUM(out_msat), 0) as total_out_msat
            FROM forwards
            WHERE out_channel = ?1 AND timestamp > ?2",
            rusqlite::params![channel_id, since],
            0,
        );
        base_to_sats_floor(msat.max(0) as u64) as i64
    }

    /// Port of `Database.get_forward_count_since` (database.py:5406-5428).
    pub fn forward_count_since(&self, channel_id: &str, since: i64) -> i64 {
        self.query_i64(
            "SELECT COUNT(*) as forward_count
            FROM forwards
            WHERE out_channel = ?1 AND timestamp > ?2",
            rusqlite::params![channel_id, since],
            0,
        )
    }

    /// py `database.get_channel_probe(channel_id) is not None`
    /// (database.py:2144-2164): a `channel_probes` row that has outlived
    /// `PROBE_MAX_AGE_SECONDS` counts as absent. Python DELETEs the stale
    /// row as a side effect of reading it; this read-only surface returns
    /// the same value without the delete (module docs, "Write behavior").
    /// Expiry uses the snapshot's frozen `now`.
    pub fn exploration_flag(&self, channel_id: &str) -> bool {
        let started_at: Option<i64> = match self
            .conn
            .query_row(
                "SELECT started_at FROM channel_probes WHERE channel_id = ?1",
                [channel_id],
                |row| row.get(0),
            )
            .optional()
        {
            Ok(v) => v,
            Err(e) => {
                eprintln!("revops: evidence query failed ({e}): channel_probes");
                None
            }
        };
        match started_at {
            None => false,
            Some(started_at) => {
                !(started_at > 0 && (self.now - started_at) > PROBE_MAX_AGE_SECONDS)
            }
        }
    }

    /// STRICT NO-OP, per the T10 review adjudication (progress-ledger
    /// contract on the `FeeEvidence` trait doc): the per-cycle evidence
    /// surface is read-only and frozen, so "clearing" the probe flag must
    /// neither touch the production DB (Python owns `channel_probes` for
    /// the whole dry-run window -- Global Constraint: the Rust plugin
    /// never writes the production DB) nor change what ANY snapshot
    /// method -- including `exploration_flag` itself -- returns for the
    /// remainder of the cycle. Python's `database.clear_channel_probe`
    /// takes effect for the NEXT cycle's evidence, which re-hydrates from
    /// the DB after Python's own cycle performed the same clear.
    /// `clear_exploration_flag_is_strict_noop` pins this.
    pub fn clear_exploration_flag(&self, _channel_id: &str) {}

    /// Contract point 2: `_get_peer_inbound_channels` (py 3147-3153) --
    /// the ONE `listchannels` prefetch, grouped by destination at build
    /// time, replayed through the same `FrozenObservations` memo pattern
    /// the Python cycle wraps around its gossip reads (key mirrors py's
    /// `("inbound_channels", peer_id)` tuple). Never a per-peer RPC: the
    /// snapshot holds no RPC handle at all.
    pub fn gossip_channels(&self, peer_id: &str) -> Vec<GossipRow> {
        let key = format!("inbound_channels:{peer_id}");
        let mut memo = self.gossip_memo.borrow_mut();
        let value = match memo.get_or_compute(&key, || {
            let rows = self
                .gossip_by_destination
                .get(peer_id)
                .map(|rows| rows.iter().map(gossip_row_to_value).collect())
                .unwrap_or_default();
            Ok::<_, std::convert::Infallible>(Value::Array(rows))
        }) {
            Ok(v) => v,
            Err(never) => match never {},
        };
        value
            .as_array()
            .map(|rows| rows.iter().map(gossip_row_from_value).collect())
            .unwrap_or_default()
    }

    /// Port of `Database.get_peer_latency_stats(peer_id, 86400)`
    /// (database.py:5675-5714): resolution times joined through
    /// `channel_states` on `out_channel`, zero/NULL samples filtered,
    /// mean + SAMPLE stddev (n-1). Python always returns a dict, so this
    /// always returns `Some` (the `{avg: 0.0, std: 0.0}` shape behaves
    /// identically to "no latency data" at every use site).
    pub fn peer_latency(&self, peer_id: &str) -> Option<PeerLatency> {
        let since = self.now - 86_400;
        let times: Vec<f64> = {
            let mut stmt = match self.conn.prepare(
                "SELECT f.resolution_time
            FROM forwards f
            JOIN channel_states cs ON f.out_channel = cs.channel_id
            WHERE cs.peer_id = ?1 AND f.timestamp >= ?2",
            ) {
                Ok(stmt) => stmt,
                Err(e) => {
                    eprintln!("revops: evidence query failed ({e}): peer_latency");
                    return Some(PeerLatency { avg: 0.0, std: 0.0 });
                }
            };
            let rows = stmt.query_map(rusqlite::params![peer_id, since], |row| {
                row.get::<_, Option<f64>>(0)
            });
            match rows {
                Ok(rows) => rows
                    .filter_map(|r| r.ok().flatten())
                    // py: `if row['resolution_time'] and row['resolution_time'] > 0`.
                    .filter(|t| *t > 0.0)
                    .collect(),
                Err(e) => {
                    eprintln!("revops: evidence query failed ({e}): peer_latency");
                    Vec::new()
                }
            }
        };

        let n = times.len();
        if n == 0 {
            return Some(PeerLatency { avg: 0.0, std: 0.0 });
        }
        let avg = times.iter().sum::<f64>() / n as f64;
        if n < 2 {
            return Some(PeerLatency { avg, std: 0.0 });
        }
        let variance = times.iter().map(|x| (x - avg) * (x - avg)).sum::<f64>() / (n as f64 - 1.0);
        Some(PeerLatency {
            avg,
            std: variance.sqrt(),
        })
    }

    /// Port of `Database.get_channel_cost_history(channel_id, since)`
    /// (database.py:5817-5846): scid-alias `IN` lookup, `timestamp >=
    /// since`, `ORDER BY timestamp DESC`, trimmed to the three columns
    /// `RebalanceCostSample` carries.
    pub fn channel_cost_history(&self, channel_id: &str, since: i64) -> Vec<RebalanceCostSample> {
        let aliases = scid_aliases(channel_id);
        let sql = format!(
            "SELECT cost_sats, amount_sats, timestamp FROM rebalance_costs
            WHERE channel_id IN ({})
            AND timestamp >= ?
            ORDER BY timestamp DESC",
            sql_placeholders(aliases.len())
        );
        let mut params: Vec<rusqlite::types::Value> = aliases
            .into_iter()
            .map(rusqlite::types::Value::Text)
            .collect();
        params.push(rusqlite::types::Value::Integer(since));

        let mut stmt = match self.conn.prepare(&sql) {
            Ok(stmt) => stmt,
            Err(e) => {
                eprintln!("revops: evidence query failed ({e}): channel_cost_history");
                return Vec::new();
            }
        };
        let rows = stmt.query_map(rusqlite::params_from_iter(params), |row| {
            Ok(RebalanceCostSample {
                cost_sats: row.get(0)?,
                amount_sats: row.get(1)?,
                timestamp: row.get(2)?,
            })
        });
        match rows {
            Ok(rows) => rows.filter_map(|r| r.ok()).collect(),
            Err(e) => {
                eprintln!("revops: evidence query failed ({e}): channel_cost_history");
                Vec::new()
            }
        }
    }

    /// Port of `Database.get_historical_inbound_fee_ppm`
    /// (database.py:4811-4915) at the fee cycle's call site parameters
    /// (`_get_rebalance_cost_floor`, fee_controller.py:4142-4146):
    /// `window_days=REBALANCE_FLOOR_WINDOW_DAYS` (30),
    /// `min_samples=REBALANCE_FLOOR_MIN_SAMPLES` (4). Only the
    /// `confidence`/`avg_fee_ppm` pair survives into `PeerFeeHistory` --
    /// the median is unread by the cycle. The schema always carries
    /// `actual_fee_msat`, so Python's `sqlite3.OperationalError` legacy
    /// fallback has no equivalent path (same rationale as
    /// `queries::total_rebalance_fees_since`).
    pub fn peer_fee_history(&self, peer_id: &str) -> Option<PeerFeeHistory> {
        // py: channels for this peer; none -> None.
        let channel_ids: Vec<String> = {
            let mut stmt = self
                .conn
                .prepare("SELECT channel_id FROM channel_states WHERE peer_id = ?1")
                .ok()?;
            let rows = stmt
                .query_map([peer_id], |row| row.get::<_, String>(0))
                .ok()?;
            rows.filter_map(|r| r.ok()).collect()
        };
        if channel_ids.is_empty() {
            return None;
        }

        let since = self.now - REBALANCE_FLOOR_WINDOW_DAYS * 86_400;
        let sql = format!(
            "SELECT
                    amount_sats,
                    COALESCE(actual_fee_msat, actual_fee_sats * 1000) as actual_fee_msat
                FROM rebalance_history
                WHERE to_channel IN ({})
                  AND status = 'success'
                  AND COALESCE(actual_fee_msat, actual_fee_sats * 1000) > 0
                  AND amount_sats > 0
                  AND timestamp >= ?
                ORDER BY timestamp DESC",
            sql_placeholders(channel_ids.len())
        );
        let mut params: Vec<rusqlite::types::Value> = channel_ids
            .into_iter()
            .map(rusqlite::types::Value::Text)
            .collect();
        params.push(rusqlite::types::Value::Integer(since));

        let mut stmt = self.conn.prepare(&sql).ok()?;
        let rows: Vec<(i64, i64)> = stmt
            .query_map(rusqlite::params_from_iter(params), |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
            })
            .ok()?
            .filter_map(|r| r.ok())
            .collect();

        if rows.len() < REBALANCE_FLOOR_MIN_SAMPLES {
            return None;
        }
        let total_fees_msat: i64 = rows.iter().map(|(_, fee)| fee).sum();
        let total_volume: i64 = rows.iter().map(|(amount, _)| amount).sum();
        if total_volume == 0 {
            return None;
        }
        // py: `(total_fees_msat * 1000) // total_volume` -- both operands
        // are positive by the WHERE filters, so Rust's truncating `/`
        // equals Python's floor `//` here.
        let avg_fee_ppm = (total_fees_msat * 1000) / total_volume;

        let sample_count = rows.len();
        let confidence = if sample_count >= 10 {
            "high"
        } else if sample_count >= 5 {
            "medium"
        } else {
            "low"
        };
        Some(PeerFeeHistory {
            confidence: confidence.to_string(),
            avg_fee_ppm,
        })
    }

    /// Port of `Database.get_last_forward_time` (database.py:5508-5529):
    /// `MAX(timestamp)` over `out_channel`; Python's falsy check maps a
    /// `NULL`-or-zero max to `None`.
    pub fn last_forward_time(&self, channel_id: &str) -> Option<i64> {
        let ts = self.query_i64(
            "SELECT MAX(timestamp) as last_ts
            FROM forwards
            WHERE out_channel = ?1",
            [channel_id],
            0,
        );
        if ts != 0 {
            Some(ts)
        } else {
            None
        }
    }

    /// py `_get_flow_window_map()[channel_id]`
    /// (fee_controller.py:2777-2793): the batched 7d directional flow,
    /// computed ONCE at build time (the per-cycle cache and this frozen
    /// snapshot coincide); a channel absent from the map is `None`.
    pub fn flow_window(&self, channel_id: &str) -> Option<FlowWindow> {
        self.flow_windows.get(channel_id).copied()
    }

    /// py `policy_manager.get_policy(peer_id)`
    /// (policy_manager.py:446-490): explicit un-expired policy row, else
    /// the default policy. Python's `get_policy` NEVER returns `None` (the
    /// trait's `None` means "no policy manager wired", which the live
    /// plugin always has), so this always returns `Some`. Expired rows
    /// were already dropped at load ([`read_policies`]) -- without
    /// Python's side-effect delete.
    pub fn policy(&self, peer_id: &str) -> Option<PeerPolicy> {
        Some(
            self.policies
                .get(peer_id)
                .cloned()
                .unwrap_or_else(|| PeerPolicy::default_for(peer_id)),
        )
    }

    /// py `profitability.get_profitability(cid).marginal_roi_percent`
    /// (fee_controller.py:5838-5842 -- its ONLY fee-cycle consumer is the
    /// `marginal_roi_info` fragment of the decision reason string, which
    /// the diff harness compares verbatim). This ports exactly the slice
    /// of `ProfitabilityAnalyzer.analyze_channel` that feeds
    /// `marginal_roi_percent` (profitability_analyzer.py:836-846 +
    /// `ChannelProfitability.marginal_roi`, 330-334):
    ///
    /// - `get_channel_pnl(cid, 30)` (database.py:3063-3148): direct
    ///   revenue msat (live forwards + completed daily rollups, current
    ///   partial day excluded) and rebalance cost msat/sats;
    /// - `get_channel_inbound_contribution(cid, 30)`
    ///   (database.py:3149-3232): sourced fee msat;
    /// - `contribution = max(direct, sourced)`;
    ///   `marginal_profit_30d = toward_zero(contribution - cost_msat)`;
    /// - `cost <= 0 -> (profit > 0 ? 1.0 : 0.0)`, else TRUE division
    ///   `profit / max(1, cost)`; `* 100`.
    ///
    /// `None` exactly when Python's cache lookup misses
    /// (`get_profitability` -> `self._profitability_cache.get(...)`,
    /// profitability_analyzer.py:898-913): the cache is keyed by the
    /// analyzer's `listpeerchannels`-derived channel set, mirrored here by
    /// the prefetched `channels_info` key set. The bkpr/datastore parts of
    /// `analyze_channel` feed fields the fee cycle never reads and are
    /// out of this port's scope.
    pub fn marginal_roi_percent(&self, channel_id: &str) -> Option<f64> {
        let canonical = normalize_scid(channel_id);
        if !self.channels_info.contains_key(&canonical) {
            return None;
        }
        let aliases = scid_aliases(&canonical);
        let ph = sql_placeholders(aliases.len());
        let alias_params: Vec<rusqlite::types::Value> = aliases
            .into_iter()
            .map(rusqlite::types::Value::Text)
            .collect();

        let since = self.now - 30 * 86_400;
        let since_day = (since / 86_400) * 86_400;
        // DB-1 boundary (py get_channel_pnl): exclude the current partial
        // day from daily rollups; those forwards are still live rows.
        let today_start = (self.now / 86_400) * 86_400;

        let windowed = |sql: &str| -> i64 {
            let mut params = alias_params.clone();
            params.push(rusqlite::types::Value::Integer(since));
            params.extend(alias_params.clone());
            params.push(rusqlite::types::Value::Integer(since_day));
            params.push(rusqlite::types::Value::Integer(today_start));
            match self
                .conn
                .query_row(sql, rusqlite::params_from_iter(params), |row| row.get(0))
            {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("revops: evidence query failed ({e}): marginal_roi");
                    0
                }
            }
        };

        let revenue_msat = windowed(&format!(
            "SELECT
                (SELECT COALESCE(SUM(fee_msat), 0)
                    FROM forwards
                    WHERE out_channel IN ({ph}) AND timestamp >= ?) +
                (SELECT COALESCE(SUM(total_fee_msat), 0)
                    FROM daily_forwarding_stats
                    WHERE channel_id IN ({ph}) AND date >= ? AND date < ?)"
        ));
        let sourced_fee_msat = windowed(&format!(
            "SELECT
                (SELECT COALESCE(SUM(fee_msat), 0)
                    FROM forwards
                    WHERE in_channel IN ({ph}) AND timestamp >= ?) +
                (SELECT COALESCE(SUM(total_fee_msat), 0)
                    FROM daily_forwarding_stats_inbound
                    WHERE channel_id IN ({ph}) AND date >= ? AND date < ?)"
        ));

        let mut cost_params = alias_params.clone();
        cost_params.push(rusqlite::types::Value::Integer(since));
        let cost_msat = match self.conn.query_row(
            &format!(
                "SELECT COALESCE(SUM(COALESCE(cost_msat, cost_sats * 1000)), 0) as cost_msat
                FROM rebalance_costs
                WHERE channel_id IN ({ph}) AND timestamp >= ?"
            ),
            rusqlite::params_from_iter(cost_params),
            |row| row.get::<_, i64>(0),
        ) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("revops: evidence query failed ({e}): marginal_roi cost");
                0
            }
        };

        let rebalance_cost_sats = base_to_sats_ceil(cost_msat.max(0) as u64) as i64;
        let contribution_msat = revenue_msat.max(sourced_fee_msat);
        // `base_delta_to_sats_toward_zero` (utils.py:91-95): signed msat ->
        // sats rounding toward zero == Rust's truncating i64 division.
        let marginal_profit_sats = (contribution_msat - cost_msat) / 1000;

        let marginal_roi = if rebalance_cost_sats <= 0 {
            if marginal_profit_sats > 0 {
                1.0
            } else {
                0.0
            }
        } else {
            marginal_profit_sats as f64 / rebalance_cost_sats.max(1) as f64
        };
        Some(marginal_roi * 100.0)
    }

    /// py `temporary_fee_overlay_active(channel_id)`: the production
    /// plugin never wires this callable (the `FeeController` constructor
    /// default `None` short-circuits the check at
    /// fee_controller.py:4753-4755; only tests inject one), so `false` is
    /// exact parity, made explicit here rather than inherited from the
    /// trait default.
    pub fn temporary_overlay_active(&self, _channel_id: &str) -> bool {
        false
    }

    /// Port of `Database.get_mempool_ma(86400)` (database.py:7696-7712)
    /// over Python's `mempool_fee_history` table -- see
    /// [`read_mempool_ma`] for the SQL.
    ///
    /// CUTOVER RIDER: this value is only live because *Python's* fee cycle
    /// keeps recording mempool samples during the dry-run window
    /// (`database.record_mempool_fee`, fee_controller.py:4586-4587 --
    /// Python writes one sample per cycle while Vegas is enabled). After
    /// Python unloads at cutover, nothing writes `mempool_fee_history`
    /// and this average degrades toward the stale window (ultimately the
    /// `1.0` fallback) until the Rust recorder exists (checklist item 9's
    /// cutover work). Do NOT ship cutover without that recorder.
    pub fn mempool_ma_24h(&self) -> f64 {
        self.mempool_ma_24h
    }

    /// py `listpeerchannels` rows for the node-drain-bias aggregate
    /// (fee_controller.py:4613-4623) -- the same prefetched snapshot as
    /// `channels_info`, unfiltered (see [`build_node_channels`]).
    pub fn node_channels(&self) -> Vec<NodeChannel> {
        self.node_channels.clone()
    }
}

impl FeeEvidence for EvidenceSnapshot {
    fn our_node_id(&self) -> Result<String, DecisionInputError> {
        Ok(Self::our_node_id(self))
    }
    fn channel_states(&self) -> Result<Vec<ChannelStateRow>, DecisionInputError> {
        Ok(Self::channel_states(self))
    }
    fn channels_info(&self) -> Result<BTreeMap<String, ChannelInfo>, DecisionInputError> {
        Ok(Self::channels_info(self))
    }
    fn chain_costs(&self) -> Result<Option<ChainCosts>, DecisionInputError> {
        Ok(Self::chain_costs(self))
    }
    fn volume_since(&self, channel_id: &str, since: i64) -> Result<i64, DecisionInputError> {
        Ok(Self::volume_since(self, channel_id, since))
    }
    fn forward_count_since(&self, channel_id: &str, since: i64) -> Result<i64, DecisionInputError> {
        Ok(Self::forward_count_since(self, channel_id, since))
    }
    fn exploration_flag(&self, channel_id: &str) -> Result<bool, DecisionInputError> {
        Ok(Self::exploration_flag(self, channel_id))
    }
    fn clear_exploration_flag(&self, channel_id: &str) -> Result<(), DecisionInputError> {
        Self::clear_exploration_flag(self, channel_id);
        Ok(())
    }
    fn gossip_channels(&self, peer_id: &str) -> Result<Vec<GossipRow>, DecisionInputError> {
        Ok(Self::gossip_channels(self, peer_id))
    }
    fn peer_latency(&self, peer_id: &str) -> Result<Option<PeerLatency>, DecisionInputError> {
        Ok(Self::peer_latency(self, peer_id))
    }
    fn channel_cost_history(
        &self,
        channel_id: &str,
        since: i64,
    ) -> Result<Vec<RebalanceCostSample>, DecisionInputError> {
        Ok(Self::channel_cost_history(self, channel_id, since))
    }
    fn peer_fee_history(
        &self,
        peer_id: &str,
    ) -> Result<Option<PeerFeeHistory>, DecisionInputError> {
        Ok(Self::peer_fee_history(self, peer_id))
    }
    fn last_forward_time(&self, channel_id: &str) -> Result<Option<i64>, DecisionInputError> {
        Ok(Self::last_forward_time(self, channel_id))
    }
    fn flow_window(&self, channel_id: &str) -> Result<Option<FlowWindow>, DecisionInputError> {
        Ok(Self::flow_window(self, channel_id))
    }
    fn policy(&self, peer_id: &str) -> Result<Option<PeerPolicy>, DecisionInputError> {
        Ok(Self::policy(self, peer_id))
    }
    fn marginal_roi_percent(&self, channel_id: &str) -> Result<Option<f64>, DecisionInputError> {
        Ok(Self::marginal_roi_percent(self, channel_id))
    }
    fn temporary_overlay_active(&self, channel_id: &str) -> Result<bool, DecisionInputError> {
        Ok(Self::temporary_overlay_active(self, channel_id))
    }
    fn mempool_ma_24h(&self) -> Result<f64, DecisionInputError> {
        Ok(Self::mempool_ma_24h(self))
    }
    fn node_channels(&self) -> Result<Vec<NodeChannel>, DecisionInputError> {
        Ok(Self::node_channels(self))
    }
}
