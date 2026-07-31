//! Bleeder-v2 classification ladder (port of `identify_bleeders_v2`'s
//! per-channel verdict, `profitability_analyzer.py:1691-1755`).
//!
//! This module ports ONLY the classification decision — the pure function
//! of one channel's windowed P&L, its rebalance success rate, and its
//! previous verdict. The surrounding machinery (the 5-minute verdict
//! cache `get_bleeder_status` maintains, the batch P&L reads) lives with
//! the caller: the cache is process state, and the P&L windows come from
//! the same per-channel revenue/cost queries the dashboard's bleeder
//! report already uses.
//!
//! Python's `BleederClassification` also carries `reason` /
//! `recommended_action` strings; the capex engine — this port's only
//! consumer — reads `.classification` alone (capex_budget.py:196-203),
//! so only the verdict is ported. `is_hard_bleeder` on the capex side is
//! a string compare against `"hard"`.

/// One channel's windowed P&L inputs, in SATS, shaped exactly like the
/// fields `identify_bleeders_v2` extracts from `get_channel_full_pnl`
/// rows (py 1674-1678).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BleederPnl {
    pub rebalance_cost_30d_sats: i64,
    /// `total_contribution_sats` — ceil(max(direct, sourced)) msat.
    pub revenue_30d_sats: i64,
    /// `net_pnl_sats` — toward-zero(contribution − rebalance msat).
    pub net_profit_30d_sats: i64,
    pub net_profit_7d_sats: i64,
}

/// The `get_channel_rebalance_success_rate` subset the scan reads
/// (py 1680-1687). Callers pass `None` when the channel has no rate row.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BleederSuccess {
    pub total: i64,
    pub success_rate: f64,
}

/// py 1680-1687: with >= 3 samples, effective cost inflates by the
/// success rate (floored at 0.10 so a 2% success rate cannot 50x the
/// cost into absurdity); otherwise the raw cost stands. `int()` on a
/// non-negative float truncates toward zero, which the `as i64` cast
/// reproduces.
fn effective_cost_30d(pnl: &BleederPnl, success: Option<BleederSuccess>) -> i64 {
    match success {
        Some(s) if s.total >= 3 => {
            let sr = s.success_rate.max(0.10);
            (pnl.rebalance_cost_30d_sats as f64 / sr) as i64
        }
        _ => pnl.rebalance_cost_30d_sats,
    }
}

/// The v2 verdict ladder (py 1689-1755), first match wins:
///
/// 1. HARD entry: effective cost > 2x revenue AND 30d net < -1000.
/// 2. Hysteresis hold (audit F4a): previously hard stays hard until the
///    30d net recovers ABOVE -500 (exactly -500 still holds).
/// 3. Materiality floor (audit F8): under 100 sats of RAW 30d rebalance
///    spend (not effective — py 1715 reads `rebalance_cost_30d`) there
///    is not enough evidence to call anything a bleeder.
/// 4. SOFT: 7d negative, 30d positive (short-term loss, long-term gain).
/// 5. Sustained (both windows negative): hard when |30d net| > 1000,
///    soft otherwise.
/// 6. R1: 30d negative with a 7d recovery blip is still soft.
/// 7. Otherwise none.
// The hard-entry and hysteresis arms both yield "hard"; merging them with
// `||` would compile identically but destroy the 1:1 correspondence with
// Python's ladder (py 1697-1712), which is what a reviewer diffs against.
#[allow(clippy::if_same_then_else)]
pub fn classify_bleeder(
    pnl: &BleederPnl,
    success: Option<BleederSuccess>,
    prev_hard: bool,
) -> &'static str {
    let effective = effective_cost_30d(pnl, success);
    let net_30d = pnl.net_profit_30d_sats;
    let net_7d = pnl.net_profit_7d_sats;

    if effective > pnl.revenue_30d_sats * 2 && net_30d < -1000 {
        "hard"
    } else if prev_hard && net_30d <= -500 {
        "hard"
    } else if pnl.rebalance_cost_30d_sats < 100 {
        "none"
    } else if net_7d < 0 && net_30d > 0 {
        "soft"
    } else if net_30d < 0 && net_7d < 0 {
        if net_30d.abs() > 1000 {
            "hard"
        } else {
            "soft"
        }
    } else if net_30d < 0 && net_7d >= 0 {
        "soft"
    } else {
        "none"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pnl(cost: i64, revenue: i64, net_30d: i64, net_7d: i64) -> BleederPnl {
        BleederPnl {
            rebalance_cost_30d_sats: cost,
            revenue_30d_sats: revenue,
            net_profit_30d_sats: net_30d,
            net_profit_7d_sats: net_7d,
        }
    }

    /// py TestHardBleederHysteresis::test_enters_hard_below_minus_1000 /
    /// test_does_not_enter_hard_above_minus_1000: -1002 enters hard; a
    /// FRESH channel at -998 lands on the sustained-minor arm instead.
    #[test]
    fn hard_entry_boundary_at_minus_1000() {
        assert_eq!(
            classify_bleeder(&pnl(1002, 0, -1002, -200), None, false),
            "hard"
        );
        assert_eq!(
            classify_bleeder(&pnl(998, 0, -998, -200), None, false),
            "soft"
        );
        // Discriminates the HARD-ENTRY arm from the sustained-severe arm:
        // with a 7d recovery blip the sustained arm can't fire, so only
        // arm 1 (cost > 2x revenue AND net30 < -1000, py 1697-1704) makes
        // this hard — R1 soft otherwise.
        assert_eq!(
            classify_bleeder(&pnl(5000, 0, -2000, 50), None, false),
            "hard"
        );
    }

    /// py test_oscillation_around_minus_1000_holds_hard /
    /// test_holds_hard_at_exactly_minus_500 / test_exits_hard_above_minus_500.
    #[test]
    fn hysteresis_holds_until_recovery_above_minus_500() {
        assert_eq!(
            classify_bleeder(&pnl(998, 0, -998, -200), None, true),
            "hard"
        );
        assert_eq!(
            classify_bleeder(&pnl(500, 0, -500, -200), None, true),
            "hard"
        );
        // Above -500 releases: -400 falls through to sustained-minor soft.
        assert_eq!(
            classify_bleeder(&pnl(400, 0, -400, -200), None, true),
            "soft"
        );
    }

    /// py TestBleederMaterialityFloor: -80 on 80 sats of spend is "none"
    /// (mentions the floor); the same ratio with material spend is soft;
    /// hard entries are unaffected (they imply cost > 1000).
    #[test]
    fn materiality_floor_at_100_sats_of_raw_spend() {
        assert_eq!(classify_bleeder(&pnl(80, 0, -80, -20), None, false), "none");
        assert_eq!(
            classify_bleeder(&pnl(300, 0, -300, -50), None, false),
            "soft"
        );
        assert_eq!(
            classify_bleeder(&pnl(1500, 0, -1500, -300), None, false),
            "hard"
        );
    }

    /// py TestSoftBleederDetection + R1: 7d loss with 30d gain is soft;
    /// 30d loss with a 7d recovery blip is ALSO soft (R1 fix).
    #[test]
    fn soft_arms_short_term_loss_and_r1_recovery_blip() {
        assert_eq!(
            classify_bleeder(&pnl(200, 500, 500, -200), None, false),
            "soft"
        );
        assert_eq!(
            classify_bleeder(&pnl(300, 0, -300, 10), None, false),
            "soft"
        );
    }

    /// py TestSustainedBleedingDetection: both windows negative -> hard
    /// over |net| 1000, soft under.
    #[test]
    fn sustained_bleeding_splits_on_1000_sats() {
        assert_eq!(
            classify_bleeder(&pnl(2000, 0, -2000, -500), None, false),
            "hard"
        );
        assert_eq!(
            classify_bleeder(&pnl(400, 0, -400, -100), None, false),
            "soft"
        );
    }

    /// py test_zero_rebalance_cost via the floor arm: zero spend is never
    /// a bleeder regardless of net.
    #[test]
    fn zero_cost_is_never_a_bleeder() {
        assert_eq!(
            classify_bleeder(&pnl(0, 100, -500, -100), None, false),
            "none"
        );
    }

    /// py 1680-1687: the success-rate adjustment inflates the EFFECTIVE
    /// cost used by the hard-entry arm. revenue 1000, cost 1900, net30
    /// -1100, net7 +50: raw cost fails 2x-revenue (1900 <= 2000) -> R1
    /// soft; at a 50% success rate the effective cost is 3800 -> hard.
    /// Under 3 samples the adjustment never applies; a 5% rate floors at
    /// 0.10 (19000, still hard — and int() truncation, not rounding).
    #[test]
    fn success_rate_inflates_effective_cost_for_hard_entry() {
        let p = pnl(1900, 1000, -1100, 50);
        assert_eq!(classify_bleeder(&p, None, false), "soft");
        let half = BleederSuccess {
            total: 5,
            success_rate: 0.5,
        };
        assert_eq!(classify_bleeder(&p, Some(half), false), "hard");
        let two_samples = BleederSuccess {
            total: 2,
            success_rate: 0.5,
        };
        assert_eq!(classify_bleeder(&p, Some(two_samples), false), "soft");
        let tiny = BleederSuccess {
            total: 5,
            success_rate: 0.05,
        };
        assert_eq!(classify_bleeder(&p, Some(tiny), false), "hard");
        assert_eq!(effective_cost_30d(&p, Some(tiny)), 19_000, "0.10 floor");
    }
}
