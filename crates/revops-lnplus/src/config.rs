//! The subset of the plugin's global `Config`/`cfg.snapshot()` object this
//! module reads. Python accesses these via `getattr(cfg, name, default)`
//! scattered across the file; this struct collects them with the SAME
//! defaults, each annotated with its Python call site.

#[derive(Debug, Clone, PartialEq)]
pub struct LnPlusConfig {
    /// `cfg.lnplus_swaps_enabled` (py 292) — default `False` (kill switch;
    /// `run_cycle` returns an empty/no-op summary immediately when unset).
    pub lnplus_swaps_enabled: bool,
    /// `cfg.lnplus_apply_feerate_ceiling` (py 341) — no Python default;
    /// required whenever `lnplus_swaps_enabled` is true.
    pub lnplus_apply_feerate_ceiling: i64,
    /// `cfg.planner_min_channel_sats` (py 355) — default `0`.
    pub planner_min_channel_sats: i64,
    /// `cfg.planner_max_channel_sats` (py 362) — default `0` (`0` = no
    /// upper bound, I2(a)).
    pub planner_max_channel_sats: i64,
    /// `cfg.lnplus_max_duration_months` (py 365).
    pub lnplus_max_duration_months: i64,
    /// `cfg.lnplus_max_participants` (py 367).
    pub lnplus_max_participants: i64,
    /// `cfg.lnplus_min_participants` (py 373) — D-3: dual (2-party) swaps
    /// are rejected; default `3`.
    pub lnplus_min_participants: i64,
    /// `cfg.lnplus_min_peer_positive_ratings` (py 413).
    pub lnplus_min_peer_positive_ratings: i64,
    /// `cfg.lnplus_min_peer_rank` (py 420) — D-2: gold-or-better floor.
    pub lnplus_min_peer_rank: i64,
    /// `cfg.lnplus_inbound_credit_factor` (py 555).
    pub lnplus_inbound_credit_factor: f64,
    /// `cfg.lnplus_swap_preference_margin` (py 582).
    pub lnplus_swap_preference_margin: f64,
    /// `cfg.min_wallet_reserve` (py 607) — default `0` (I2(b)).
    pub min_wallet_reserve: i64,
    /// `cfg.lnplus_execute_applications` (py 636) — default `False`
    /// (recommend-only unless explicitly enabled).
    pub lnplus_execute_applications: bool,
    /// `cfg.planner_dry_run` (py 636) — default `False`.
    pub planner_dry_run: bool,
    /// `cfg.lnplus_pending_timeout_days` (py 2069) — default `7`.
    pub lnplus_pending_timeout_days: i64,
}

impl Default for LnPlusConfig {
    fn default() -> Self {
        Self {
            lnplus_swaps_enabled: false,
            lnplus_apply_feerate_ceiling: 0,
            planner_min_channel_sats: 0,
            planner_max_channel_sats: 0,
            lnplus_max_duration_months: i64::MAX,
            lnplus_max_participants: 99,
            lnplus_min_participants: 3,
            lnplus_min_peer_positive_ratings: 0,
            lnplus_min_peer_rank: 0,
            lnplus_inbound_credit_factor: 1.0,
            lnplus_swap_preference_margin: 0.0,
            min_wallet_reserve: 0,
            lnplus_execute_applications: false,
            planner_dry_run: false,
            lnplus_pending_timeout_days: 7,
        }
    }
}
