//! Pure response builder for `revenue-lnplus-status`.
//!
//! Port of `revenue_lnplus_status` (cl-revenue-ops.py:4604-4612) +
//! `LnPlusLifecycle.get_status` (`modules/lnplus_swaps.py:2114-2131`).
//! Every field is a pass-through of an already-fetched DB row / in-memory
//! value (`breaker_tripped()` -> `db.get_config_override`,
//! `db.lnplus_inflight_swaps()`, `db.lnplus_get_swaps_by_status([...])`,
//! `db.get_config_override(_BACKFILL_FLAG)`) -- no scoring or lifecycle
//! decision logic of its own (that lives in `revops_lnplus::breaker`/
//! `::reconcile`/`::evaluator`, invoked elsewhere, not by this read-only
//! status surface).

use serde_json::{json, Value};

/// Evidence `LnPlusLifecycle.get_status` reads, already fetched by the
/// caller, plus the two config overlay flags `revenue_lnplus_status`
/// applies on top (py 4610-4611).
pub struct LnPlusStatusInputs {
    /// `self.breaker_tripped()` (py `lnplus_swaps.py:862-863`):
    /// `db.get_config_override(_BREAKER_KEY)`, a `"<unix_ts>: <reason>"`
    /// string when tripped, `None` when clear.
    pub breaker: Option<String>,
    /// `db.lnplus_inflight_swaps()` rows, already JSON-shaped.
    pub inflight: Vec<Value>,
    /// `db.lnplus_get_swaps_by_status(["active"])` rows.
    pub active: Vec<Value>,
    /// `db.lnplus_get_swaps_by_status(["ended"])[-10:]` -- already sliced
    /// to the last 10 by the caller (py 2115, 2126).
    pub recent_ended: Vec<Value>,
    /// `db.lnplus_get_swaps_by_status(_TERMINAL_PENDING_GHOST_STATUSES)
    /// [-5:]` -- already sliced to the last 5 by the caller (py 2120-2121,
    /// 2127).
    pub recent_failed: Vec<Value>,
    /// `bool(db.get_config_override(_BACKFILL_FLAG))` (py 2128).
    pub backfill_done: bool,
    /// `self._last_watcher_pass` (py 2129): in-memory only, no DB: `None`
    /// before the first watcher pass this process lifetime.
    pub last_watcher_pass: Option<Value>,
    /// `getattr(self, "_recent_notifications", [])` (py 2130): in-memory
    /// only, best-effort LN+ notification poll cache.
    pub recent_notifications: Vec<Value>,
    /// `getattr(config, "lnplus_swaps_enabled", False)` (py 4610):
    /// overlaid onto the status dict's `"enabled"` key regardless of
    /// lifecycle state.
    pub swaps_enabled: bool,
    /// `getattr(config, "lnplus_execute_applications", False)` (py 4611).
    pub execute_applications: bool,
}

/// Port of `revenue_lnplus_status`. `status` is `None` when
/// `lnplus_lifecycle is None` (py 4607-4608: automation never
/// initialized) -- Python returns `{"enabled": False}` and nothing else in
/// that case, which this mirrors exactly (not even `execute_applications`
/// is present).
pub fn build_lnplus_status(status: Option<&LnPlusStatusInputs>) -> Value {
    let Some(i) = status else {
        return json!({"enabled": false});
    };
    json!({
        "breaker": i.breaker,
        "inflight": i.inflight,
        "active": i.active,
        "recent_ended": i.recent_ended,
        "recent_failed": i.recent_failed,
        "backfill_done": i.backfill_done,
        "last_watcher_pass": i.last_watcher_pass,
        "recent_notifications": i.recent_notifications,
        "enabled": i.swaps_enabled,
        "execute_applications": i.execute_applications,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> LnPlusStatusInputs {
        LnPlusStatusInputs {
            breaker: None,
            inflight: vec![],
            active: vec![json!({"swap_id": "s1", "status": "active"})],
            recent_ended: vec![],
            recent_failed: vec![],
            backfill_done: true,
            last_watcher_pass: Some(json!({"ts": 1000, "ok": true})),
            recent_notifications: vec![],
            swaps_enabled: true,
            execute_applications: false,
        }
    }

    #[test]
    fn uninitialized_lifecycle_returns_enabled_false_only() {
        let v = build_lnplus_status(None);
        assert_eq!(v, json!({"enabled": false}));
    }

    #[test]
    fn initialized_lifecycle_overlays_config_flags_over_lifecycle_state() {
        let v = build_lnplus_status(Some(&base()));
        assert_eq!(
            v["enabled"], true,
            "enabled reflects config, not lifecycle presence"
        );
        assert_eq!(v["execute_applications"], false);
        assert_eq!(v["backfill_done"], true);
        assert_eq!(v["active"][0]["swap_id"], "s1");
    }

    #[test]
    fn enabled_key_reflects_config_even_when_swaps_disabled() {
        let mut i = base();
        i.swaps_enabled = false;
        let v = build_lnplus_status(Some(&i));
        assert_eq!(
            v["enabled"], false,
            "a present lifecycle with swaps disabled must still report enabled=false"
        );
    }

    #[test]
    fn tripped_breaker_string_passes_through_verbatim() {
        let mut i = base();
        i.breaker = Some("1700000000: opening ghost swap-42".to_string());
        let v = build_lnplus_status(Some(&i));
        assert_eq!(v["breaker"], "1700000000: opening ghost swap-42");
    }
}
