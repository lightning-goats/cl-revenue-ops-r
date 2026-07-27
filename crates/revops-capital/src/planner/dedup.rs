//! Order-preserving "keep highest score, first-discovery order" dedup —
//! shared by every discovery/candidate-merge step that Python implements
//! via a `dict` (insertion-order-preserving) plus a `score > existing.score`
//! update.
//!
//! Task 47 review, finding 4: a `BTreeMap<peer_id, _>`-based dedup (the
//! pre-correction code in `cycle.rs`'s `discover_peers` and
//! `discovery.rs`'s `discover_from_route_pairs`) reorders EVERY entry into
//! peer-id sort order via `.into_values()`, not just entries that actually
//! collided. Python's `dict` preserves first-INSERTION order regardless of
//! whether a later duplicate replaces a value, and Python's `sorted(...)`
//! is stable, so ties in a later score-sort resolve in first-discovery
//! order. [`upsert_best`] + [`into_ordered_vec`] reproduce exactly that:
//! `if key not in seen or item.score > seen[key].score: seen[key] = item`.

use std::collections::HashMap;

/// Insert `item` under `key` if `key` is new (appending to `order`), or
/// replace the existing entry's VALUE in place — never moving its position
/// in `order` — when `item`'s score is STRICTLY greater than the current
/// occupant's. A tie leaves the earlier-discovered item untouched.
pub fn upsert_best<T>(
    order: &mut Vec<String>,
    map: &mut HashMap<String, T>,
    key: String,
    item: T,
    score_of: impl Fn(&T) -> f64,
) {
    match map.get(&key) {
        None => {
            order.push(key.clone());
            map.insert(key, item);
        }
        Some(existing) if score_of(&item) > score_of(existing) => {
            map.insert(key, item);
        }
        _ => {}
    }
}

/// Drain `order`/`map` (built via repeated [`upsert_best`] calls) into a
/// `Vec<T>` in first-discovery order.
pub fn into_ordered_vec<T>(order: Vec<String>, mut map: HashMap<String, T>) -> Vec<T> {
    order
        .into_iter()
        .map(|k| {
            map.remove(&k)
                .expect("order and map are always built together by upsert_best")
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq)]
    struct Item {
        score: f64,
    }

    #[test]
    fn preserves_first_discovery_order_for_distinct_keys() {
        let mut order = Vec::new();
        let mut map = HashMap::new();
        upsert_best(
            &mut order,
            &mut map,
            "zzz".to_string(),
            Item { score: 1.0 },
            |i| i.score,
        );
        upsert_best(
            &mut order,
            &mut map,
            "aaa".to_string(),
            Item { score: 1.0 },
            |i| i.score,
        );
        let out = into_ordered_vec(order, map);
        // Peer-id sort order would be ["aaa", "zzz"]; discovery order must
        // win instead.
        assert_eq!(out, vec![Item { score: 1.0 }, Item { score: 1.0 }]);
    }

    #[test]
    fn replaces_value_in_place_only_on_strictly_greater_score() {
        let mut order = Vec::new();
        let mut map: HashMap<String, Item> = HashMap::new();
        upsert_best(
            &mut order,
            &mut map,
            "p1".to_string(),
            Item { score: 1.0 },
            |i| i.score,
        );
        // Tie: must NOT replace.
        upsert_best(
            &mut order,
            &mut map,
            "p1".to_string(),
            Item { score: 1.0 },
            |i| i.score,
        );
        assert_eq!(map["p1"], Item { score: 1.0 });
        // Strictly greater: must replace.
        upsert_best(
            &mut order,
            &mut map,
            "p1".to_string(),
            Item { score: 2.0 },
            |i| i.score,
        );
        assert_eq!(map["p1"], Item { score: 2.0 });
        // Position unaffected by the replacement.
        assert_eq!(order, vec!["p1".to_string()]);
    }
}
