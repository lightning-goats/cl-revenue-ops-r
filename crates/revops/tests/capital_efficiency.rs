//! Task 71 / F71-R4: the capital-efficiency producer that lets discovery
//! take Python's COMMON path (`discover_from_neighbors_capital_efficiency`)
//! instead of the fallback.
//!
//! Port of `modules/capital_efficiency.py`'s ranking core.

use std::collections::BTreeMap;

use revops::capital_efficiency::{efficiency_ranks, percentile_ranks, EfficiencyInput};

fn inputs(rows: &[(&str, i64, i64, Option<i64>)]) -> Vec<EfficiencyInput> {
    rows.iter()
        .map(|(scid, capacity, fees_msat, marginal)| EfficiencyInput {
            scid: (*scid).to_string(),
            capacity_sats: *capacity,
            fees_earned_msat: *fees_msat,
            marginal_profit_30d_sats: *marginal,
        })
        .collect()
}

/// Percentile ranks run 0..1 over the sorted values.
#[test]
fn percentile_ranks_span_zero_to_one() {
    let r = percentile_ranks(&BTreeMap::from([
        ("a".to_string(), 10.0),
        ("b".to_string(), 20.0),
        ("c".to_string(), 30.0),
    ]));
    assert_eq!(r["a"], 0.0);
    assert_eq!(r["b"], 0.5);
    assert_eq!(r["c"], 1.0);
}

/// A single channel ranks 1.0, not 0.0 and not a divide-by-zero (py 189).
/// Ranking it 0.0 would make a one-channel node's only patron look worthless.
#[test]
fn a_single_channel_ranks_top() {
    let r = percentile_ranks(&BTreeMap::from([("solo".to_string(), 42.0)]));
    assert_eq!(r["solo"], 1.0);
}

/// TIES share the AVERAGE of their positions (py 198-203). Assigning them
/// distinct ranks by sort order would make the patron pool depend on scid
/// string ordering rather than on measured efficiency.
#[test]
fn ties_share_the_average_rank() {
    let r = percentile_ranks(&BTreeMap::from([
        ("a".to_string(), 10.0),
        ("b".to_string(), 10.0),
        ("c".to_string(), 10.0),
        ("d".to_string(), 99.0),
    ]));
    // positions 0,1,2 tie -> (0+2)/2 / 3 = 0.3333...
    let tied = r["a"];
    assert!((tied - 1.0 / 3.0).abs() < 1e-12, "got {tied}");
    assert_eq!(r["b"], tied);
    assert_eq!(r["c"], tied);
    assert_eq!(r["d"], 1.0);
}

/// An empty input is an empty map, not a panic.
#[test]
fn empty_input_ranks_nothing() {
    assert!(percentile_ranks(&BTreeMap::new()).is_empty());
}

/// The audit-F7 blend: when EVERY channel exposes 30d marginal profit, the
/// rank is a 50/50 blend of the lifetime-gross and windowed-net ranks.
///
/// Lifetime gross alone is sticky -- a channel that earned well a year ago
/// and has bled since keeps a top rank forever. Here `old` has the best
/// lifetime revenue but the worst 30d profit, so blending must pull it
/// below a currently-productive channel.
///
/// Three channels, not two: with exactly two, the lifetime and windowed
/// rankings are perfect inverses and a 50/50 blend necessarily TIES at 0.5
/// for both. That is correct arithmetic, so a two-channel fixture cannot
/// demonstrate the demotion at all -- `now` needs respectable lifetime
/// revenue as well as the best recent profit.
#[test]
fn the_windowed_blend_demotes_a_stale_earner() {
    let rows = inputs(&[
        ("old", 1_000_000, 900_000_000, Some(-500)),
        ("now", 1_000_000, 800_000_000, Some(5_000)),
        ("weak", 1_000_000, 10_000_000, Some(100)),
    ]);
    let ranks = efficiency_ranks(&rows);
    assert!(
        ranks["now"] > ranks["old"],
        "a currently-productive channel must outrank a stale earner: {ranks:?}"
    );
    // Lifetime-only would have ranked `old` top.
    let lifetime_only = efficiency_ranks(&inputs(&[
        ("old", 1_000_000, 900_000_000, None),
        ("now", 1_000_000, 800_000_000, None),
        ("weak", 1_000_000, 10_000_000, None),
    ]));
    assert!(
        lifetime_only["old"] > lifetime_only["now"],
        "control: without the windowed signal, lifetime gross wins"
    );
}

/// The blend activates ONLY when EVERY channel exposes the windowed signal
/// (py 89-95 breaks and clears the map on the first `None`). A partial
/// blend would rank channels against a metric half of them lack --
/// silently, and the ranking drives which peers become patrons.
#[test]
fn a_single_missing_windowed_signal_disables_the_blend() {
    let blended = efficiency_ranks(&inputs(&[
        ("old", 1_000_000, 900_000_000, Some(-500)),
        ("now", 1_000_000, 800_000_000, Some(5_000)),
        ("weak", 1_000_000, 10_000_000, Some(100)),
    ]));
    let disabled = efficiency_ranks(&inputs(&[
        ("old", 1_000_000, 900_000_000, Some(-500)),
        ("now", 1_000_000, 800_000_000, None),
        ("weak", 1_000_000, 10_000_000, Some(100)),
    ]));
    assert_ne!(
        blended["old"], disabled["old"],
        "one missing signal must disable the blend entirely"
    );
    assert!(
        disabled["old"] > disabled["now"],
        "disabled falls back to pure lifetime ranking"
    );
}

/// Zero capacity yields an rpsd of 0.0 rather than dividing (py 152).
#[test]
fn zero_capacity_is_zero_rpsd_not_a_divide() {
    let ranks = efficiency_ranks(&inputs(&[
        ("empty", 0, 500_000_000, Some(100)),
        ("real", 1_000_000, 100_000_000, Some(100)),
    ]));
    assert!(ranks["real"] > ranks["empty"]);
}
