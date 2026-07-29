//! Pure response builders for `revenue-r-policy`'s READ-ONLY actions
//! (`list`/`get`/`find`/`changes`) -- port of `revenue_policy`
//! (cl-revenue-ops.py:5325-5573), gated to `READ_ONLY_POLICY_ACTIONS`
//! (policy_manager.py:54). This batch is read-only reporting only: the
//! tactical actions (`set`/`delete`/`tag`/`untag`/`batch`,
//! `TACTICAL_POLICY_ACTIONS`, policy_manager.py:55) are DB writes and
//! stay out of scope -- [`policy_action_gate`] always returns Python's
//! stable authority refusal for them (never applying
//! Python's `internal`/`admin` override, since this port implements no
//! write path for those actions to unlock).
//!
//! Every builder consumes already-fetched [`PeerPolicy`] values (ported in
//! `revops_analytics::policy`, itself a port of `modules/policy_manager
//! .py`'s `PeerPolicy`) -- fetching them from the production DB is the
//! wiring layer's job (see `crates/revops/RPC_BATCH_A.md`: `policy_manager`
//! has no `revops-db` query equivalent yet).

use crate::rpc_params::{is_truthy_py, python_int};
use revops_analytics::policy::PeerPolicy;
use serde_json::{json, Value};

/// Read-only actions this builder supports (policy_manager.py:54's
/// `READ_ONLY_POLICY_ACTIONS`, alphabetized for the "Unknown action"
/// message below).
const READ_ONLY_ACTIONS: [&str; 4] = ["changes", "find", "get", "list"];
/// Tactical (write) actions -- always refused by this read-only port
/// (policy_manager.py:55's `TACTICAL_POLICY_ACTIONS`).
const TACTICAL_ACTIONS: [&str; 5] = ["batch", "delete", "set", "tag", "untag"];

/// `PeerPolicy.to_dict()` (policy_manager.py:135-149) as JSON. `now`
/// stands in for Python's internal `time.time()` read inside `is_expired`.
pub fn peer_policy_to_json(p: &PeerPolicy, now: i64) -> Value {
    json!({
        "peer_id": p.peer_id,
        "strategy": p.strategy.as_value(),
        "rebalance_mode": p.rebalance_mode.as_value(),
        "fee_ppm_target": p.fee_ppm_target,
        "tags": p.tags,
        "updated_at": p.updated_at,
        "fee_multiplier_min": p.fee_multiplier_min,
        "fee_multiplier_max": p.fee_multiplier_max,
        "expires_at": p.expires_at,
        "is_expired": p.is_expired(now),
    })
}

/// Task 50 correction round, F9: `revenue_policy`'s signature default is
/// `action: str = "list"` (cl-revenue-ops.py:5326) -- an ABSENT `action`
/// key binds to the literal string `"list"` before any of Python's own
/// body code runs. An EXPLICIT `action` (present in params, whatever its
/// JSON type) instead goes through `str(action or "").strip().lower()`
/// (cl-revenue-ops.py:5397): `action=null`/`0`/`false`/`""` (Python-falsy)
/// -> `""` -> the unknown-action error; a non-empty string is
/// stripped+lowercased. The OLD Rust wiring collapsed BOTH cases (absent
/// key, and explicit null/non-string) through `Option<&str>`, so an
/// explicit `action: null` silently succeeded as `list` where Python
/// errors. Taking the raw `&Value` here lets the two cases be told apart:
/// `None` (key absent) is the ONLY path that defaults to `"list"`.
///
/// Non-string truthy JSON values (numbers, arrays, objects) are folded to
/// the empty string too, by scope decision -- Python's `str(x)` on those
/// produces exotic reprs (`"5"`, `"[1]"`, `"True"`) that would only ever
/// match `"true"` by coincidence; folding them to `""` still reaches the
/// SAME unknown-action error family Python reaches for all of them except
/// the one exact string `"true"` (an accepted simplification, not a byte-
/// exact port of Python's `str()`).
pub fn normalize_action(raw: Option<&Value>) -> String {
    match raw {
        None => "list".to_string(),
        Some(v) if !is_truthy_py(v) => String::new(),
        Some(Value::String(s)) => s.trim().to_lowercase(),
        Some(_) => String::new(),
    }
}

/// Routes `action` before any DB fetch happens: `Some(error)` for a
/// tactical or unrecognized action (the caller should return it as-is,
/// no query needed); `None` means "one of the 4 read actions -- proceed
/// to fetch and call the matching `build_policy_*` below".
pub fn policy_action_gate(action: &str) -> Option<Value> {
    if TACTICAL_ACTIONS.contains(&action) {
        // Task 65 slice 4: the STABLE authority refusal. Python supports
        // these write actions (set/tag/untag are the documented
        // replacement for revenue-ignore); this plugin refuses them
        // because it holds no production write authority before the
        // Task 69 cutover -- never a fake success, never a misleading
        // deprecation claim.
        return Some(json!({
            "error": {
                "code": "state_writer_authority_absent",
                "message": format!(
                    "revenue-policy {action} is a production-state write; this plugin \
                     holds no write authority before cutover. Use the Python plugin's \
                     revenue-policy for writes; list/get/find/changes serve reads here."
                ),
            }
        }));
    }
    if READ_ONLY_ACTIONS.contains(&action) {
        return None;
    }
    let mut allowed: Vec<&str> = READ_ONLY_ACTIONS
        .iter()
        .chain(TACTICAL_ACTIONS.iter())
        .copied()
        .collect();
    allowed.sort_unstable();
    let allowed_text = allowed
        .iter()
        .map(|n| format!("'{n}'"))
        .collect::<Vec<_>>()
        .join(", ");
    Some(json!({"error": format!("Unknown action: {action}. Use {allowed_text}")}))
}

/// `action=get` usage error (cl-revenue-ops.py:5414): `peer_id` missing.
pub fn get_usage_error() -> Value {
    json!({"error": "Usage: revenue-policy get <peer_id>"})
}

/// `action=find` usage error (cl-revenue-ops.py:5514): `tag` missing.
pub fn find_usage_error() -> Value {
    json!({"error": "Usage: revenue-policy find <tag>"})
}

/// `L-22` peer_id format validation error, shared by `get`
/// (cl-revenue-ops.py:5416-5417) -- also reused by `revenue-ban`/
/// `-unban`/`-hot-channel-protection-peers add`, but those are write RPCs
/// out of this batch's scope.
pub fn invalid_peer_id_error() -> Value {
    json!({"error": "Invalid peer_id format: expected 66-character hex pubkey"})
}

/// `action=changes`'s `since` coercion error (cl-revenue-ops.py:5537-5538).
pub fn invalid_since_error() -> Value {
    json!({"error": "Invalid 'since' timestamp. Must be a Unix timestamp."})
}

/// Task 50 correction round, F6: `action=changes`'s `since` coercion
/// (cl-revenue-ops.py:5524-5529): `since = kwargs.get('since', 0)`, then
/// `since = int(since) if since else 0` inside a `try`, catching
/// `(ValueError, TypeError)` into [`invalid_since_error`]. The OLD Rust
/// wiring was `v.get("since").and_then(as_i64).unwrap_or(0)`, which
/// silently maps ANY non-numeric-JSON `since` (a garbage string like
/// `"abc"`, a numeric-looking string like `"1700000000"`, a float, `null`)
/// to `0` and returns the FULL policy table as "changes since the epoch" --
/// exactly the fabrication the audit flagged.
///
/// This mirrors Python's `if since` truthiness gate FIRST (a Python-falsy
/// `since` -- absent, `null`, `0`, `""`, an empty array/object -- short-
/// circuits to `0` with NO coercion attempt, so it can never error), then
/// [`python_int`] on anything truthy. `Some(0)` on falsy/absent; `Some(n)`
/// on a successful coercion; `None` on garbage (the caller returns
/// [`invalid_since_error`]). `Option`, not `Result<i64, ()>`, since every
/// success case already produces a value and the only thing a caller ever
/// does with the error case is substitute a fixed response -- there is no
/// error payload to carry.
pub fn coerce_since(raw: Option<&Value>) -> Option<i64> {
    let truthy = raw.map(is_truthy_py).unwrap_or(false);
    if !truthy {
        return Some(0);
    }
    python_int(raw.expect("truthy implies Some")).ok()
}

/// `action=list` (cl-revenue-ops.py:5407-5411).
pub fn build_policy_list(policies: &[PeerPolicy], now: i64) -> Value {
    json!({
        "policies": policies.iter().map(|p| peer_policy_to_json(p, now)).collect::<Vec<_>>(),
        "count": policies.len(),
    })
}

/// `action=get <peer_id>` (cl-revenue-ops.py:5418-5420). Caller must have
/// already validated `peer_id` ([`invalid_peer_id_error`]) and fetched the
/// policy (`policy_manager.get_policy` always returns SOME policy --
/// caller-defaults if none exists explicitly, see `PeerPolicy::default_for`).
pub fn build_policy_get(policy: &PeerPolicy, now: i64) -> Value {
    json!({"policy": peer_policy_to_json(policy, now)})
}

/// `action=find <tag>` (cl-revenue-ops.py:5510-5519).
pub fn build_policy_find(tag: &str, policies: &[PeerPolicy], now: i64) -> Value {
    json!({
        "peers": policies.iter().map(|p| peer_policy_to_json(p, now)).collect::<Vec<_>>(),
        "count": policies.len(),
        "tag": tag,
    })
}

/// `action=changes [since=<ts>]` (cl-revenue-ops.py:5524-5535). `changes`
/// must already exclude expired policies -- `PolicyManager
/// .get_policy_changes_since` filters those out before `to_dict()`
/// (policy_manager.py:521-522), so this builder mirrors that by requiring
/// the caller to pre-filter (`PeerPolicy::is_expired`) rather than
/// re-filtering here.
pub fn build_policy_changes(
    since: i64,
    changes: &[PeerPolicy],
    last_change_timestamp: i64,
    now: i64,
) -> Value {
    json!({
        "changes": changes.iter().map(|p| peer_policy_to_json(p, now)).collect::<Vec<_>>(),
        "count": changes.len(),
        "since": since,
        "last_change_timestamp": last_change_timestamp,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use revops_analytics::policy::{FeeStrategy, RebalanceMode};

    fn policy(peer_id: &str) -> PeerPolicy {
        let mut p = PeerPolicy::default_for(peer_id);
        p.strategy = FeeStrategy::Static;
        p.rebalance_mode = RebalanceMode::Disabled;
        p.fee_ppm_target = Some(500);
        p.tags = vec!["whale".to_string()];
        p.updated_at = 1_700_000_000;
        p
    }

    #[test]
    fn peer_policy_to_json_matches_python_to_dict_shape() {
        let peer_id = "02".to_string() + &"a".repeat(64);
        let p = policy(&peer_id);
        let v = peer_policy_to_json(&p, 1_700_000_100);
        assert_eq!(v["strategy"], "static");
        assert_eq!(v["rebalance_mode"], "disabled");
        assert_eq!(v["fee_ppm_target"], 500);
        assert_eq!(v["tags"], json!(["whale"]));
        assert_eq!(v["is_expired"], false);
        assert_eq!(v["expires_at"], Value::Null);
    }

    #[test]
    fn is_expired_reflects_injected_now_not_a_clock() {
        let mut p = policy("peer-a");
        p.expires_at = Some(1000);
        assert_eq!(peer_policy_to_json(&p, 999)["is_expired"], false);
        assert_eq!(peer_policy_to_json(&p, 1001)["is_expired"], true);
    }

    #[test]
    fn action_gate_blocks_writes_with_the_stable_authority_refusal() {
        // Task 65 slice 4: Python SUPPORTS set/tag/untag (they are the
        // recommended replacement for revenue-ignore) -- the old
        // "deprecated" text was a mislabel. The honest refusal is that
        // this plugin holds no production write authority before the
        // Task 69 cutover.
        for action in ["batch", "delete", "set", "tag", "untag"] {
            let v = policy_action_gate(action).unwrap();
            assert_eq!(v["error"]["code"], "state_writer_authority_absent");
            let message = v["error"]["message"].as_str().unwrap();
            assert!(message.contains(action), "{message}");
            assert!(message.contains("write authority"), "{message}");
        }
    }

    #[test]
    fn action_gate_allows_read_actions() {
        for a in ["list", "get", "find", "changes"] {
            assert!(
                policy_action_gate(a).is_none(),
                "expected {a} to be allowed"
            );
        }
    }

    #[test]
    fn action_gate_unknown_action_lists_all_nine_alphabetized() {
        let v = policy_action_gate("bogus").unwrap();
        assert_eq!(
            v["error"],
            "Unknown action: bogus. Use 'batch', 'changes', 'delete', 'find', 'get', 'list', \
             'set', 'tag', 'untag'"
        );
    }

    #[test]
    fn normalize_action_absent_key_defaults_to_list() {
        assert_eq!(normalize_action(None), "list");
    }

    #[test]
    fn normalize_action_string_is_trimmed_and_lowercased() {
        assert_eq!(normalize_action(Some(&json!(" GET "))), "get");
    }

    /// Task 50 correction round, F9: an EXPLICIT `action: null` must NOT
    /// default to `"list"` (that only happens when the key is absent
    /// entirely) -- it normalizes to `""`, which the action gate then
    /// rejects as unknown, matching Python's `str(None or "")` -> `""`.
    #[test]
    fn normalize_action_explicit_null_is_not_list() {
        assert_eq!(normalize_action(Some(&Value::Null)), "");
        assert_ne!(normalize_action(Some(&Value::Null)), "list");
    }

    #[test]
    fn normalize_action_falsy_non_string_values_fold_to_empty() {
        assert_eq!(normalize_action(Some(&json!(0))), "");
        assert_eq!(normalize_action(Some(&json!(false))), "");
        assert_eq!(normalize_action(Some(&json!(""))), "");
    }

    #[test]
    fn normalize_action_explicit_null_reaches_the_unknown_action_error() {
        let action = normalize_action(Some(&Value::Null));
        let err = policy_action_gate(&action).expect("null action must be refused");
        assert!(err["error"]
            .as_str()
            .unwrap()
            .starts_with("Unknown action:"));
    }

    #[test]
    fn coerce_since_absent_and_falsy_values_are_zero_no_error() {
        assert_eq!(coerce_since(None), Some(0));
        assert_eq!(coerce_since(Some(&Value::Null)), Some(0));
        assert_eq!(coerce_since(Some(&json!(0))), Some(0));
        assert_eq!(coerce_since(Some(&json!(""))), Some(0));
    }

    #[test]
    fn coerce_since_numeric_string_coerces_like_python_int() {
        assert_eq!(
            coerce_since(Some(&json!("1700000000"))),
            Some(1_700_000_000)
        );
        assert_eq!(
            coerce_since(Some(&json!(1_700_000_000i64))),
            Some(1_700_000_000)
        );
    }

    #[test]
    fn coerce_since_garbage_is_an_error_not_a_silent_zero() {
        assert_eq!(coerce_since(Some(&json!("abc"))), None);
        // A garbage `since` must NOT silently become 0 (which would fetch
        // the FULL policy table as "changes since the epoch").
        assert_ne!(coerce_since(Some(&json!("abc"))), Some(0));
    }

    #[test]
    fn build_policy_list_reports_count() {
        let policies = vec![policy("a"), policy("b")];
        let v = build_policy_list(&policies, 0);
        assert_eq!(v["count"], 2);
        assert_eq!(v["policies"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn build_policy_find_echoes_tag() {
        let policies = vec![policy("a")];
        let v = build_policy_find("whale", &policies, 0);
        assert_eq!(v["tag"], "whale");
        assert_eq!(v["count"], 1);
    }

    #[test]
    fn build_policy_changes_shape() {
        let policies = vec![policy("a")];
        let v = build_policy_changes(500, &policies, 1_700_000_000, 1_700_000_100);
        assert_eq!(v["since"], 500);
        assert_eq!(v["last_change_timestamp"], 1_700_000_000);
        assert_eq!(v["count"], 1);
    }
}
