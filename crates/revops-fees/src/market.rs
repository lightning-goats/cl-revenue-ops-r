//! Market intelligence: neighbor-gossip fee statistics + prior selection
//! (port of the market paths in `modules/fee_controller.py`).
//! `_get_market_boundary_fee` returns `None` UNCONDITIONALLY (behavioral
//! contract 1), even with `fee_market_boundary_enabled=true` persisted.
//!
//! Mirrors `_get_network_fee_prior_live` (py 3179-3231),
//! `_gossip_cache_ttl_seconds` (py 3253-3269),
//! `_get_peer_inbound_channels_live` (py 3271-3304),
//! `_get_market_boundary_fee` (py 3306-3321, stub: always `None`),
//! `_get_neighbor_fee_median_live` (py 3324-3403),
//! `_is_cln_default_fee` (py 3405-3427),
//! `_get_neighbor_fee_percentile_live` (py 3429-3482),
//! `_get_competitive_undercut_pct` (py 3484-3552),
//! `_select_best_fee_prior` (py 7924-7948), and `FrozenObservations`, which
//! mirrors `_frozen_observation` (py 3134-3141) plus its cycle-frozen
//! wrappers (py 3142-3178).
//!
//! Clock injection (Global Constraints): every function that Python
//! computed with `time.time()` takes an explicit `now: i64` here.
//!
//! Interface note: `GossipChannel` slices are the caller's already-fetched,
//! peer-scoped snapshot (the moral equivalent of a
//! `_get_peer_inbound_channels_live(peer_id)` result trimmed to
//! `_GOSSIP_CHANNEL_FIELDS`). The Python `active` gate is a data-fetch
//! concern (whether gossip currently carries an announcement for the
//! edge) rather than fee-math semantics, so it is the fetch layer's job to
//! hand these pure functions only active edges — these functions apply no
//! `active` filter of their own.

use std::collections::HashMap;

/// One gossip-derived channel edge relevant to neighbor-fee market
/// intelligence: `source` -[fee_ppm/base_fee_msat]-> `destination`, with
/// `capacity_sats` and `last_update_ts` (gossip `last_update` timestamp,
/// used for the recency weight).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GossipChannel {
    pub source: String,
    pub destination: String,
    pub fee_ppm: i64,
    pub base_fee_msat: i64,
    pub capacity_sats: i64,
    pub last_update_ts: i64,
}

/// `_get_neighbor_fee_median_live` / `_get_neighbor_fee_percentile_live`'s
/// `neighbor_median_min_competitors` default (py `getattr(cfg,
/// 'neighbor_median_min_competitors', 3)`). Task 8's pure functions take no
/// `cfg` (that plumbing is a Wave-3 orchestrator concern), so the default is
/// baked in here.
pub const MIN_COMPETITORS: usize = 3;

/// Gossip fee-ppm sanity range shared by every neighbor-fee consumer (py
/// `1 <= fee_ppm <= 10000`, checked identically in the median, percentile,
/// and network-prior paths).
const MIN_FEE_PPM: i64 = 1;
const MAX_FEE_PPM: i64 = 10_000;

fn in_fee_range(fee_ppm: i64) -> bool {
    (MIN_FEE_PPM..=MAX_FEE_PPM).contains(&fee_ppm)
}

/// `_is_cln_default_fee` (py 3405-3427): an untouched CLN default (base
/// 1000 msat AND `fee_per_millionth` 10) is not a meaningful competitor —
/// excluded from every neighbor-fee pool. Python falls back across two
/// field-name spellings (`base_fee_millisatoshi` pre-24.x /
/// `fee_base_msat` post-24.x) and treats a missing base as "not default";
/// `GossipChannel::base_fee_msat` is the caller's already-normalized single
/// value, so this is a direct equality check.
pub fn is_cln_default_fee(ch: &GossipChannel) -> bool {
    ch.fee_ppm == 10 && ch.base_fee_msat == 1000
}

/// `_get_neighbor_fee_median_live` core math (py 3324-3403) over an
/// explicit peer-channel snapshot.
///
/// Filters (in order, matching the Python loop body exactly): exclude our
/// own channel (`source == our_id`); fee range `1..=10000`; CLN-default
/// exclusion. Weight = `(capacity_sats / 1_000_000) * (1 / age_days)`,
/// `age_days = max(0.1, max(0, now - last_update_ts) / 86400)` when
/// `last_update_ts > 0`, else a flat `30.0` (stale/unknown-age fallback).
/// Requires `>= 3` surviving competitors (`MIN_COMPETITORS`), else `None`.
/// Weighted median: sort ascending by fee, walk cumulative weight, return
/// the fee at the point cumulative weight first reaches `>= 50%` of total.
pub fn neighbor_fee_median(peer_channels: &[GossipChannel], our_id: &str, now: i64) -> Option<i64> {
    let weighted = collect_weighted_fees(peer_channels, our_id, now);
    weighted_median(&weighted)
}

fn collect_weighted_fees(
    peer_channels: &[GossipChannel],
    our_id: &str,
    now: i64,
) -> Vec<(i64, f64)> {
    let mut weighted_fees: Vec<(i64, f64)> = Vec::new();
    for ch in peer_channels {
        if ch.source == our_id {
            continue;
        }
        if !in_fee_range(ch.fee_ppm) {
            continue;
        }
        if is_cln_default_fee(ch) {
            continue;
        }
        let capacity = ch.capacity_sats.max(1);
        let age_days = if ch.last_update_ts > 0 {
            (0.max(now - ch.last_update_ts) as f64 / 86400.0).max(0.1)
        } else {
            30.0
        };
        let recency_weight = 1.0 / age_days;
        let weight = (capacity as f64 / 1_000_000.0) * recency_weight;
        weighted_fees.push((ch.fee_ppm, weight));
    }
    weighted_fees
}

fn weighted_median(weighted_fees: &[(i64, f64)]) -> Option<i64> {
    if weighted_fees.len() < MIN_COMPETITORS {
        return None;
    }
    // Stable sort by fee ascending — matches Python's
    // `weighted_fees.sort(key=lambda x: x[0])` (Timsort is stable; so is
    // Rust's `sort_by_key`), preserving relative order of equal-fee entries.
    let mut sorted = weighted_fees.to_vec();
    sorted.sort_by_key(|entry| entry.0);

    let total_weight: f64 = sorted.iter().map(|(_, w)| w).sum();
    if total_weight <= 0.0 {
        return None;
    }

    let mut cumulative = 0.0;
    let mut result = sorted[0].0;
    for (fee, w) in &sorted {
        cumulative += w;
        if cumulative >= total_weight * 0.5 {
            result = *fee;
            break;
        }
    }
    Some(result)
}

/// `_get_neighbor_fee_percentile_live` (py 3429-3482): `pct`-th nearest-rank
/// percentile of the same gossip-derived competitor pool (own-channel,
/// fee-range, and CLN-default filters identical to the median path, but
/// UNWEIGHTED — plain sorted fee list). `now` is accepted for interface
/// symmetry with `neighbor_fee_median` (the Python method reads no clock
/// inside its own math; only its caller-side TTL cache does) and is
/// intentionally unused here.
pub fn neighbor_fee_percentile(
    peer_channels: &[GossipChannel],
    our_id: &str,
    pct: f64,
    _now: i64,
) -> Option<i64> {
    let mut fees: Vec<i64> = Vec::new();
    for ch in peer_channels {
        if ch.source == our_id {
            continue;
        }
        if !in_fee_range(ch.fee_ppm) {
            continue;
        }
        if is_cln_default_fee(ch) {
            continue;
        }
        fees.push(ch.fee_ppm);
    }

    if fees.len() < MIN_COMPETITORS {
        return None;
    }

    fees.sort();
    let n = fees.len();
    // Python: `idx = min(len(fees) - 1, max(0, int(round(pct * (len(fees)
    // - 1)))))` — `round()` on a float is CPython round-half-to-even.
    let raw_idx = (pct * (n - 1) as f64).round_ties_even();
    let idx = (raw_idx as i64).clamp(0, (n - 1) as i64) as usize;
    Some(fees[idx])
}

/// `_get_competitive_undercut_pct` (py 3484-3552) distilled to its pure
/// rank/corridor math: `capacity_rank` is the count of competitors with
/// STRICTLY GREATER capacity than ours (`larger_than_us` in the Python —
/// `0` means we're the largest), `rank_count` is the total competitor
/// count. Base undercut scales `5%` (largest, rank 0) to `15%` (smallest,
/// rank == rank_count) by capacity rank; `invert_rank` (premium/markup
/// mode, E-4.1) flips that scaling so capacity strength supports pricing
/// ABOVE the median instead. Corridor adjustment: `median > 300` adds a
/// flat `+5%`; `median < 100` halves the whole base (thin-margin
/// low-fee corridor); `100..=300` is a no-op (both comparisons are
/// strict). Clamped to `[0.03, 0.20]`. `rank_count == 0` (no competitor
/// capacity data) mirrors the Python's `if not competitor_capacities or
/// our_capacity <= 0: return 0.10` early-out.
pub fn competitive_undercut_pct(
    capacity_rank: usize,
    rank_count: usize,
    neighbor_median: i64,
    invert_rank: bool,
) -> f64 {
    if rank_count == 0 {
        return 0.10;
    }

    let our_rank_pct = capacity_rank as f64 / rank_count as f64;
    let rank_weight = if invert_rank {
        1.0 - our_rank_pct
    } else {
        our_rank_pct
    };
    let mut base_undercut = 0.05 + (rank_weight * 0.10);

    if neighbor_median > 300 {
        base_undercut += 0.05;
    } else if neighbor_median < 100 {
        base_undercut *= 0.5;
    }

    // Python: `min(0.20, max(0.03, base_undercut))`.
    0.20_f64.min(0.03_f64.max(base_undercut))
}

/// `_get_market_boundary_fee` (py 3306-3321): deprecated compatibility
/// stub. Production data showed profitable channels whose remote policies
/// sat at 0-1 ppm, so remote peer fees are not a safe lower bound for our
/// local fee — kept for operator config-key compatibility only. ALWAYS
/// returns `None`, even when a persisted `fee_market_boundary_enabled=true`
/// is passed in (behavioral contract 1, ADR-001).
pub fn market_boundary_fee(_cfg_enabled: bool) -> Option<i64> {
    None
}

/// A fee prior: `{"mean", "std", "source"}` (py dict shape, both `mean` and
/// `std` are always integers on the network-gossip path — a
/// capacity-weighted-median fee and an integer-halved spread).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeePrior {
    pub mean: i64,
    pub std: i64,
    pub source: String,
}

/// `_get_network_fee_prior_live` (py 3179-3231): a capacity-weighted-median
/// fee prior from the PEER'S OWN announced gossip channels (every edge the
/// peer itself sources — not the neighbor-competitor pool the median/
/// percentile/undercut functions read). Same `1..=10000` fee-range filter
/// as the neighbor paths, but deliberately NO CLN-default exclusion and NO
/// recency weighting — capacity is the only credibility signal here.
/// `std` floors at 50: half the observed fee spread (>1 data point), or
/// half the median fee itself (a single channel, so there is no spread).
pub fn network_fee_prior(peer_own_channels: &[GossipChannel]) -> Option<FeePrior> {
    let mut weighted: Vec<(i64, f64)> = Vec::new();
    for ch in peer_own_channels {
        if !in_fee_range(ch.fee_ppm) {
            continue;
        }
        let capacity = ch.capacity_sats.max(1);
        let weight = capacity as f64 / 1_000_000.0;
        weighted.push((ch.fee_ppm, weight));
    }

    if weighted.is_empty() {
        return None;
    }

    weighted.sort_by_key(|entry| entry.0);
    let total_weight: f64 = weighted.iter().map(|(_, w)| w).sum();
    if total_weight <= 0.0 {
        return None;
    }

    let mut cumulative = 0.0;
    let mut median_fee = weighted[0].0;
    for (fee, w) in &weighted {
        cumulative += w;
        if cumulative >= total_weight * 0.5 {
            median_fee = *fee;
            break;
        }
    }

    let std = if weighted.len() > 1 {
        let min_fee = weighted[0].0;
        let max_fee = weighted[weighted.len() - 1].0;
        50.max((max_fee - min_fee) / 2)
    } else {
        50.max(median_fee / 2)
    };

    Some(FeePrior {
        mean: median_fee,
        std,
        source: "network".to_string(),
    })
}

/// `_select_best_fee_prior` (py 7924-7948) distilled to a pure priority
/// pick: `candidates` is already in priority order (today: `["network"]`
/// gossip-derived prior only — `_get_network_fee_prior_live`, py
/// 3179-3231 — is the sole source; future sources would be appended after
/// it). Returns the first candidate, or `None` if the caller found no
/// prior source with data at all (an empty slice — mirrors `network_prior
/// is None -> return None`).
pub fn select_best_fee_prior(candidates: &[FeePrior]) -> Option<FeePrior> {
    candidates.first().cloned()
}

/// Initial-prior nudge weight applied when seeding a fresh DTS posterior
/// from a selected `FeePrior` (py `INITIAL_PRIOR_NUDGE_WEIGHT = 0.3`, py
/// 2560). Load-bearing constant, not tunable.
pub const INITIAL_PRIOR_NUDGE_WEIGHT: f64 = 0.3;

/// `_gossip_cache_ttl_seconds` (py 3253-3269): TTL for the peer-inbound
/// gossip fetch. The fee cycle runs every `~fee_interval` seconds; a TTL
/// equal to the interval would expire almost every cycle and re-issue N
/// serial `listchannels` RPCs, so this covers ~2 cycles instead.
/// `fee_interval <= 0` (misconfigured/absent) falls back to a flat 3900s.
pub fn gossip_cache_ttl_seconds(fee_interval: i64) -> i64 {
    if fee_interval > 0 {
        (2 * fee_interval).max(3900)
    } else {
        3900
    }
}

/// Default TTL for cache reads that don't override it — matches
/// `_get_peer_inbound_channels_live`'s own `ttl_seconds: int = 1800`
/// parameter default (distinct from `gossip_cache_ttl_seconds`, which is
/// what real call sites actually pass).
pub const GOSSIP_CACHE_DEFAULT_TTL_SECONDS: i64 = 1800;

/// Eviction sweep threshold (py `if len(self._neighbor_fee_cache) > 500`).
const GOSSIP_CACHE_EVICT_THRESHOLD: usize = 500;

/// Staleness bar for the eviction sweep (py `(now - v["ts"]) > 3600`) —
/// independent of any per-entry TTL; this just bounds unbounded growth.
const GOSSIP_CACHE_STALE_SECONDS: i64 = 3600;

struct CacheEntry {
    value: serde_json::Value,
    ts: i64,
}

/// Generic TTL cache mirroring `self._neighbor_fee_cache` (py 2979): a flat
/// `Dict[str, {"value": ..., "ts": ...}]` shared by the peer-inbound-channel
/// fetch, neighbor-median, and neighbor-percentile caches (Python
/// distinguishes them only by key prefix — `gossip_channels_{peer}` /
/// `neighbor_fee_{peer}` / `neighbor_fee_p{pct}_{peer}`).
///
/// Eviction (py 3332-3344): once the map exceeds 500 entries, sweep
/// entries whose `ts` is more than 3600s stale, iterating a SNAPSHOT of the
/// entries rather than mutating the map in place — the Python comment
/// notes a concurrent `adjust_all_fees` caller may be writing to this dict
/// outside `_state_lock` by design (pre-lock gossip prefetch), and mutating
/// during `items()` would raise `RuntimeError`; Rust's borrow checker makes
/// the equivalent mistake a compile error, so the snapshot-then-remove
/// shape here is a direct, deliberate port of that same discipline rather
/// than a workaround for a live aliasing bug.
pub struct GossipCache {
    entries: HashMap<String, CacheEntry>,
    default_ttl_seconds: i64,
}

impl GossipCache {
    pub fn new(default_ttl_seconds: i64) -> Self {
        Self {
            entries: HashMap::new(),
            default_ttl_seconds,
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Cached value for `key` at `now`, fresh under `ttl_seconds` (falls
    /// back to `default_ttl_seconds` when `None`). `None` on a miss or a
    /// stale hit (py `cached and (time.time() - cached["ts"]) < ttl`).
    pub fn get(&self, key: &str, now: i64, ttl_seconds: Option<i64>) -> Option<&serde_json::Value> {
        let ttl = ttl_seconds.unwrap_or(self.default_ttl_seconds);
        self.entries
            .get(key)
            .filter(|entry| now - entry.ts < ttl)
            .map(|entry| &entry.value)
    }

    /// Insert/overwrite `key` at `now`, running the 500-entry eviction
    /// sweep first if the map has already grown past the threshold —
    /// matching the Python's check-then-write order at the top of
    /// `_get_neighbor_fee_median_live`.
    pub fn insert(&mut self, key: String, value: serde_json::Value, now: i64) {
        self.maybe_evict(now);
        self.entries.insert(key, CacheEntry { value, ts: now });
    }

    /// Run the eviction sweep (py 3337-3344) if the map exceeds 500
    /// entries: drop every entry not touched in the last 3600s. A no-op
    /// under the threshold. Exposed directly so tests can pin the sweep
    /// without needing 500+ real `insert` calls.
    pub fn maybe_evict(&mut self, now: i64) {
        if self.entries.len() <= GOSSIP_CACHE_EVICT_THRESHOLD {
            return;
        }
        let stale_keys: Vec<String> = self
            .entries
            .iter()
            .filter(|(_, entry)| now - entry.ts > GOSSIP_CACHE_STALE_SECONDS)
            .map(|(k, _)| k.clone())
            .collect();
        for key in stale_keys {
            self.entries.remove(&key);
        }
    }
}

impl Default for GossipCache {
    fn default() -> Self {
        Self::new(GOSSIP_CACHE_DEFAULT_TTL_SECONDS)
    }
}

/// Per-cycle compute-once memo (py `_frozen_observation`, py 3134-3141,
/// and its cycle-frozen wrappers, py 3142-3178): the Wave-3 cycle
/// orchestrator owns exactly one `FrozenObservations` per fee cycle so
/// every channel processed within that cycle observes an IDENTICAL gossip
/// snapshot, no matter how many times a wrapper is invoked for the same
/// key over the course of the cycle.
///
/// Python models two states with one field (`self._cycle_observations`):
/// `None` outside an active cycle, where `_frozen_observation` is a bare
/// passthrough — `compute()` runs on every call, nothing is cached — and a
/// `dict` during an active cycle, where it memoizes per key. This struct
/// only models the SECOND state (the active memo): the Wave-3 orchestrator
/// is the only thing that ever constructs one, so "no active cycle" is
/// simply "call the `_live`-equivalent function directly, never through a
/// `FrozenObservations`" on the Rust side — there is no `Option`-wrapped
/// passthrough mode to reproduce here.
///
/// Hit/miss semantics mirror `_frozen_observation` exactly: the first
/// `get_or_compute` for a given `key` invokes `compute` and caches the
/// value it returns; every later call for that SAME key returns the
/// cached value WITHOUT invoking `compute` again. Distinct keys are
/// independent — the "keyed by observation name" contract, ported from
/// Python's per-callsite tuple keys (e.g. `("neighbor_median",
/// peer_id)`) by having each wrapper format its own composite `String` key
/// (e.g. `format!("neighbor_median:{peer_id}")`).
///
/// Error behavior (py: `memo[key] = compute()` — if the right-hand side
/// raises, the assignment never happens, so `key` stays absent and the
/// exception propagates to the caller): `compute` here returns a
/// `Result<serde_json::Value, E>`; on `Err`, NOTHING is cached for `key`
/// and the error is returned to the caller, so the NEXT call for that key
/// retries `compute` from scratch rather than caching or replaying the
/// failure.
#[derive(Debug, Default)]
pub struct FrozenObservations {
    memo: HashMap<String, serde_json::Value>,
}

impl FrozenObservations {
    pub fn new() -> Self {
        Self {
            memo: HashMap::new(),
        }
    }

    /// Compute-once-per-cycle read: returns the cached value for `key` if
    /// present, otherwise calls `compute`, caches its `Ok` result, and
    /// returns it. An `Err` from `compute` is propagated without being
    /// cached, so a subsequent call for the same `key` re-invokes
    /// `compute`.
    pub fn get_or_compute<F, E>(&mut self, key: &str, compute: F) -> Result<serde_json::Value, E>
    where
        F: FnOnce() -> Result<serde_json::Value, E>,
    {
        if let Some(cached) = self.memo.get(key) {
            return Ok(cached.clone());
        }
        let value = compute()?;
        self.memo.insert(key.to_string(), value.clone());
        Ok(value)
    }

    /// Number of distinct keys frozen so far this cycle.
    pub fn len(&self) -> usize {
        self.memo.len()
    }

    pub fn is_empty(&self) -> bool {
        self.memo.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ch(
        source: &str,
        fee_ppm: i64,
        base_fee_msat: i64,
        capacity_sats: i64,
        last_update_ts: i64,
    ) -> GossipChannel {
        GossipChannel {
            source: source.to_string(),
            destination: "peer".to_string(),
            fee_ppm,
            base_fee_msat,
            capacity_sats,
            last_update_ts,
        }
    }

    #[test]
    fn is_cln_default_fee_exact_tuple_only() {
        assert!(is_cln_default_fee(&ch("p", 10, 1000, 1_000_000, 1)));
        assert!(!is_cln_default_fee(&ch("p", 11, 1000, 1_000_000, 1)));
        assert!(!is_cln_default_fee(&ch("p", 10, 999, 1_000_000, 1)));
    }

    #[test]
    fn neighbor_fee_median_none_below_min_competitors() {
        let channels = vec![
            ch("a", 100, 0, 1_000_000, 1_752_400_000 - 3600),
            ch("b", 200, 0, 1_000_000, 1_752_400_000 - 3600),
        ];
        assert_eq!(neighbor_fee_median(&channels, "us", 1_752_400_000), None);
    }

    #[test]
    fn neighbor_fee_median_some_at_exactly_min_competitors() {
        let channels = vec![
            ch("a", 100, 0, 1_000_000, 1_752_400_000 - 3600),
            ch("b", 200, 0, 1_000_000, 1_752_400_000 - 3600),
            ch("c", 300, 0, 1_000_000, 1_752_400_000 - 3600),
        ];
        assert!(neighbor_fee_median(&channels, "us", 1_752_400_000).is_some());
    }

    #[test]
    fn neighbor_fee_median_excludes_our_own_channel() {
        let channels = vec![
            ch("us", 5000, 0, 1_000_000, 1_752_400_000 - 3600),
            ch("a", 100, 0, 1_000_000, 1_752_400_000 - 3600),
            ch("b", 200, 0, 1_000_000, 1_752_400_000 - 3600),
        ];
        // Only 2 non-self competitors survive -> below MIN_COMPETITORS.
        assert_eq!(neighbor_fee_median(&channels, "us", 1_752_400_000), None);
    }

    #[test]
    fn neighbor_fee_median_excludes_out_of_range_fees() {
        let channels = vec![
            ch("a", 0, 0, 1_000_000, 1_752_400_000 - 3600),
            ch("b", 10001, 0, 1_000_000, 1_752_400_000 - 3600),
            ch("c", 100, 0, 1_000_000, 1_752_400_000 - 3600),
            ch("d", 200, 0, 1_000_000, 1_752_400_000 - 3600),
            ch("e", 300, 0, 1_000_000, 1_752_400_000 - 3600),
        ];
        assert!(neighbor_fee_median(&channels, "us", 1_752_400_000).is_some());
        // Drop one of the surviving three -> below MIN_COMPETITORS.
        let channels2 = channels[..4].to_vec();
        assert_eq!(neighbor_fee_median(&channels2, "us", 1_752_400_000), None);
    }

    #[test]
    fn neighbor_fee_median_excludes_cln_default() {
        let channels = vec![
            ch("a", 10, 1000, 1_000_000, 1_752_400_000 - 3600),
            ch("b", 100, 0, 1_000_000, 1_752_400_000 - 3600),
            ch("c", 200, 0, 1_000_000, 1_752_400_000 - 3600),
        ];
        // Only 2 non-default competitors survive.
        assert_eq!(neighbor_fee_median(&channels, "us", 1_752_400_000), None);
    }

    #[test]
    fn neighbor_fee_median_stale_last_update_falls_back_to_30_days() {
        // last_update_ts <= 0 -> age_days = 30.0 flat, not computed from
        // `now`.
        let channels = vec![
            ch("a", 100, 0, 3_000_000, 0),
            ch("b", 200, 0, 3_000_000, 0),
            ch("c", 300, 0, 3_000_000, 0),
        ];
        assert!(neighbor_fee_median(&channels, "us", 1_752_400_000).is_some());
    }

    #[test]
    fn neighbor_fee_percentile_none_below_min_competitors() {
        let channels = vec![
            ch("a", 100, 0, 1_000_000, 1_752_400_000 - 3600),
            ch("b", 200, 0, 1_000_000, 1_752_400_000 - 3600),
        ];
        assert_eq!(
            neighbor_fee_percentile(&channels, "us", 0.25, 1_752_400_000),
            None
        );
    }

    #[test]
    fn neighbor_fee_percentile_p25_nearest_rank() {
        let channels = vec![
            ch("a", 100, 0, 1_000_000, 1),
            ch("b", 200, 0, 1_000_000, 1),
            ch("c", 300, 0, 1_000_000, 1),
            ch("d", 400, 0, 1_000_000, 1),
            ch("e", 500, 0, 1_000_000, 1),
        ];
        // idx = round(0.25 * 4) = round(1.0) = 1 -> fees[1] = 200.
        assert_eq!(
            neighbor_fee_percentile(&channels, "us", 0.25, 1_752_400_000),
            Some(200)
        );
    }

    #[test]
    fn competitive_undercut_pct_rank_zero_largest() {
        // rank 0/5 -> rank_weight 0.0 -> base 0.05, median 200 no corridor.
        assert_eq!(competitive_undercut_pct(0, 5, 200, false), 0.05);
    }

    #[test]
    fn competitive_undercut_pct_rank_last_smallest() {
        // rank 5/5 -> rank_weight 1.0 -> base 0.05 + 1.0*0.10, median 200
        // no corridor. IEEE754 binary64: 0.15000000000000002, not 0.15.
        assert_eq!(
            competitive_undercut_pct(5, 5, 200, false),
            0.15000000000000002
        );
    }

    #[test]
    fn competitive_undercut_pct_high_fee_corridor_adds_five_pct() {
        assert_eq!(competitive_undercut_pct(0, 5, 500, false), 0.10);
    }

    #[test]
    fn competitive_undercut_pct_low_fee_corridor_halves() {
        // base 0.05 -> halved -> 0.025 -> clamped up to floor 0.03.
        assert_eq!(competitive_undercut_pct(0, 5, 50, false), 0.03);
    }

    #[test]
    fn competitive_undercut_pct_invert_rank_flips_scaling() {
        // 0.05 + 1.0 * 0.10 in IEEE754 binary64 is 0.15000000000000002, not
        // the decimal literal 0.15 — same value the Python fixture pins
        // (`tests/market.rs::competitive_undercut_pct_matches_python`).
        assert_eq!(
            competitive_undercut_pct(0, 5, 200, true),
            0.15000000000000002
        );
        assert_eq!(competitive_undercut_pct(5, 5, 200, true), 0.05);
    }

    #[test]
    fn competitive_undercut_pct_no_competitor_data_default() {
        assert_eq!(competitive_undercut_pct(0, 0, 200, false), 0.10);
    }

    #[test]
    fn market_boundary_fee_always_none() {
        assert_eq!(market_boundary_fee(false), None);
        assert_eq!(market_boundary_fee(true), None);
    }

    #[test]
    fn network_fee_prior_none_when_no_channels() {
        assert_eq!(network_fee_prior(&[]), None);
    }

    #[test]
    fn network_fee_prior_single_channel_std_floors_at_50() {
        let channels = vec![ch("peer", 150, 0, 1_000_000, 1)];
        let prior = network_fee_prior(&channels).unwrap();
        assert_eq!(prior.mean, 150);
        assert_eq!(prior.std, 75); // max(50, 150 // 2) = 75
        assert_eq!(prior.source, "network");
    }

    #[test]
    fn network_fee_prior_std_floors_at_50_for_small_spread() {
        let channels = vec![
            ch("peer", 100, 0, 1_000_000, 1),
            ch("peer", 110, 0, 1_000_000, 1),
        ];
        let prior = network_fee_prior(&channels).unwrap();
        assert_eq!(prior.std, 50); // max(50, (110-100)//2=5) = 50
    }

    #[test]
    fn select_best_fee_prior_empty_is_none() {
        assert_eq!(select_best_fee_prior(&[]), None);
    }

    #[test]
    fn select_best_fee_prior_picks_first_in_priority_order() {
        let network = FeePrior {
            mean: 150,
            std: 50,
            source: "network".to_string(),
        };
        let fallback = FeePrior {
            mean: 300,
            std: 100,
            source: "fallback".to_string(),
        };
        assert_eq!(
            select_best_fee_prior(&[network.clone(), fallback]),
            Some(network)
        );
    }

    #[test]
    fn gossip_cache_ttl_seconds_interval_scaling() {
        assert_eq!(gossip_cache_ttl_seconds(0), 3900);
        assert_eq!(gossip_cache_ttl_seconds(-5), 3900);
        assert_eq!(gossip_cache_ttl_seconds(60), 3900);
        assert_eq!(gossip_cache_ttl_seconds(3000), 6000);
    }

    #[test]
    fn gossip_cache_get_respects_ttl_and_default() {
        let mut cache = GossipCache::default();
        cache.insert("k".to_string(), serde_json::json!(42), 1000);
        assert_eq!(cache.get("k", 1000, None), Some(&serde_json::json!(42)));
        assert_eq!(
            cache.get("k", 1000 + GOSSIP_CACHE_DEFAULT_TTL_SECONDS - 1, None),
            Some(&serde_json::json!(42))
        );
        assert_eq!(
            cache.get("k", 1000 + GOSSIP_CACHE_DEFAULT_TTL_SECONDS, None),
            None
        );
        // Explicit override wins over the default: age 500 >= ttl 400.
        assert_eq!(cache.get("k", 1500, Some(400)), None);
    }

    #[test]
    fn gossip_cache_evicts_only_stale_entries_past_threshold_no_wall_clock() {
        let mut cache = GossipCache::default();
        let now = 1_752_400_000;
        for i in 0..GOSSIP_CACHE_EVICT_THRESHOLD + 1 {
            let ts = if i == 0 { now - 4000 } else { now };
            cache.entries.insert(
                format!("k{i}"),
                CacheEntry {
                    value: serde_json::json!(i),
                    ts,
                },
            );
        }
        assert_eq!(cache.len(), GOSSIP_CACHE_EVICT_THRESHOLD + 1);
        cache.maybe_evict(now);
        // Only the one stale (>3600s) entry is dropped.
        assert_eq!(cache.len(), GOSSIP_CACHE_EVICT_THRESHOLD);
        assert!(!cache.entries.contains_key("k0"));
    }

    #[test]
    fn gossip_cache_no_eviction_under_threshold() {
        let mut cache = GossipCache::default();
        let now = 1_752_400_000;
        cache.entries.insert(
            "stale".to_string(),
            CacheEntry {
                value: serde_json::json!(1),
                ts: now - 10_000,
            },
        );
        cache.maybe_evict(now);
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn frozen_observations_compute_once_across_repeated_gets() {
        let calls = std::cell::Cell::new(0u32);
        let mut memo = FrozenObservations::new();

        let first = memo
            .get_or_compute::<_, ()>("neighbor_median:peer_a", || {
                calls.set(calls.get() + 1);
                Ok(serde_json::json!(150))
            })
            .unwrap();
        let second = memo
            .get_or_compute::<_, ()>("neighbor_median:peer_a", || {
                calls.set(calls.get() + 1);
                Ok(serde_json::json!(999)) // must never run: would prove a miss
            })
            .unwrap();
        let third = memo
            .get_or_compute::<_, ()>("neighbor_median:peer_a", || {
                calls.set(calls.get() + 1);
                Ok(serde_json::json!(999))
            })
            .unwrap();

        assert_eq!(
            calls.get(),
            1,
            "compute must run exactly once for a repeated key"
        );
        assert_eq!(first, serde_json::json!(150));
        assert_eq!(second, serde_json::json!(150));
        assert_eq!(third, serde_json::json!(150));
    }

    #[test]
    fn frozen_observations_distinct_keys_are_independent() {
        let mut memo = FrozenObservations::new();

        let a = memo
            .get_or_compute::<_, ()>("neighbor_median:peer_a", || Ok(serde_json::json!(150)))
            .unwrap();
        let b = memo
            .get_or_compute::<_, ()>("neighbor_median:peer_b", || Ok(serde_json::json!(275)))
            .unwrap();
        // Re-reading peer_a must not have been perturbed by freezing peer_b.
        let a_again = memo
            .get_or_compute::<_, ()>("neighbor_median:peer_a", || Ok(serde_json::json!(-1)))
            .unwrap();

        assert_eq!(a, serde_json::json!(150));
        assert_eq!(b, serde_json::json!(275));
        assert_eq!(a_again, serde_json::json!(150));
        assert_eq!(memo.len(), 2);
    }

    #[test]
    fn frozen_observations_error_is_not_cached_and_retries_next_call() {
        // Mirrors Python: `memo[key] = compute()` never assigns when the
        // right-hand side raises, so an erroring key stays a permanent
        // miss until a call finally succeeds.
        let calls = std::cell::Cell::new(0u32);
        let mut memo = FrozenObservations::new();

        let first: Result<serde_json::Value, &str> = memo.get_or_compute("chain_costs", || {
            calls.set(calls.get() + 1);
            Err("rpc unavailable")
        });
        assert_eq!(first, Err("rpc unavailable"));
        assert_eq!(memo.len(), 0, "a failed compute must not be cached");

        let second: Result<serde_json::Value, &str> = memo.get_or_compute("chain_costs", || {
            calls.set(calls.get() + 1);
            Ok(serde_json::json!({"onchain_ppm": 12}))
        });
        assert_eq!(second, Ok(serde_json::json!({"onchain_ppm": 12})));

        let third: Result<serde_json::Value, &str> = memo.get_or_compute("chain_costs", || {
            calls.set(calls.get() + 1);
            Err("must not run: key already frozen")
        });
        assert_eq!(third, Ok(serde_json::json!({"onchain_ppm": 12})));

        assert_eq!(
            calls.get(),
            2,
            "compute retries after an error but never re-runs once frozen"
        );
    }

    #[test]
    fn frozen_observations_wrapper_pattern_matches_live_call() {
        // Demonstrates the shape the Wave-3 cycle orchestrator will use:
        // a thin wrapper closes over `peer_id` and the gossip snapshot,
        // and calls through `FrozenObservations` keyed by observation name
        // + peer, mirroring `_get_neighbor_fee_median` (py 3156-3160).
        fn frozen_neighbor_median(
            memo: &mut FrozenObservations,
            channels: &[GossipChannel],
            our_id: &str,
            now: i64,
            peer_id: &str,
        ) -> Option<i64> {
            let key = format!("neighbor_median:{peer_id}");
            let value = memo
                .get_or_compute::<_, ()>(&key, || {
                    Ok(match neighbor_fee_median(channels, our_id, now) {
                        Some(v) => serde_json::json!(v),
                        None => serde_json::Value::Null,
                    })
                })
                .unwrap();
            value.as_i64()
        }

        let channels = vec![
            ch("a", 100, 0, 1_000_000, 1_752_400_000 - 3600),
            ch("b", 200, 0, 1_000_000, 1_752_400_000 - 3600),
            ch("c", 300, 0, 1_000_000, 1_752_400_000 - 3600),
        ];
        let live = neighbor_fee_median(&channels, "us", 1_752_400_000);

        let mut memo = FrozenObservations::new();
        let frozen_first =
            frozen_neighbor_median(&mut memo, &channels, "us", 1_752_400_000, "peer_a");
        // Second call passes an empty snapshot; if the wrapper were not
        // actually frozen, this would compute `None` instead of replaying
        // the first (non-empty-snapshot) result.
        let frozen_second = frozen_neighbor_median(&mut memo, &[], "us", 1_752_400_000, "peer_a");

        assert_eq!(frozen_first, live);
        assert_eq!(frozen_second, live);
        assert_eq!(memo.len(), 1);
    }
}
