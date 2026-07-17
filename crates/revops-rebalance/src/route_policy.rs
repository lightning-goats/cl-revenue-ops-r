//! Route-policy classification (port of `modules/rebalance_route_policy.py`).
//!
//! Every candidate uses ordinary market routing — the coordinated policies
//! (peer hints, priority scoring) were retired 2026-07. This module keeps
//! the `RouteDecision` shape and `decide_route_policy` signature frozen so
//! the engine/types layers don't need structural changes if hints are ever
//! reintroduced. Python's `decide_route_policy(pair: Any, *, reason_code:
//! str = "")` takes an unused `pair` argument (never read in the body,
//! `rebalance_route_policy.py:38-47`); this port drops it since no typed
//! candidate needs to flow through this stub yet — a later task can widen
//! the signature if that changes.

/// Port of `rebalance_route_policy.py`'s `RoutePolicy` enum. `MARKET_ONLY`
/// is the only variant (coordinated policies retired).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoutePolicy {
    MarketOnly,
}

/// Port of `rebalance_route_policy.py`'s `RoutePriority` enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoutePriority {
    EvPositive,
    Background,
}

/// Port of `rebalance_route_policy.py:25-35`'s `RouteDecision` frozen
/// dataclass. `hint_id`/`hint_type`/`priority_score` are retained
/// (always-default) so the engine's debug/enrichment dicts keep a stable
/// shape, per the Python docstring — nothing populates these now that
/// hints are gone.
#[derive(Debug, Clone, PartialEq)]
pub struct RouteDecision {
    pub policy: RoutePolicy,
    pub priority: RoutePriority,
    pub reason: String,
    pub allow_market_fallback: bool,
    pub hint_id: String,
    pub hint_type: String,
    pub priority_score: f64,
}

/// Port of `rebalance_route_policy.py:38-47` (`decide_route_policy`):
/// standalone, always plain market routing (EV-positive priority).
pub fn decide_route_policy(reason_code: &str) -> RouteDecision {
    RouteDecision {
        policy: RoutePolicy::MarketOnly,
        priority: RoutePriority::EvPositive,
        reason: if reason_code.is_empty() {
            "ev_positive".to_string()
        } else {
            reason_code.to_string()
        },
        allow_market_fallback: true,
        hint_id: String::new(),
        hint_type: String::new(),
        priority_score: 0.0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decide_route_policy_defaults_to_market_only_ev_positive() {
        let d = decide_route_policy("");
        assert_eq!(d.policy, RoutePolicy::MarketOnly);
        assert_eq!(d.priority, RoutePriority::EvPositive);
        assert_eq!(d.reason, "ev_positive");
        assert!(d.allow_market_fallback);
        assert_eq!(d.hint_id, "");
        assert_eq!(d.hint_type, "");
        assert_eq!(d.priority_score, 0.0);
    }

    #[test]
    fn decide_route_policy_preserves_explicit_reason_code() {
        let d = decide_route_policy("structural_drain");
        assert_eq!(d.reason, "structural_drain");
        assert_eq!(d.policy, RoutePolicy::MarketOnly);
    }
}
