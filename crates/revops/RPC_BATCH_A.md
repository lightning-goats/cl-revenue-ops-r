# RPC Batch A — integration guide

Ten read-only response builders, one file each in `crates/revops/src/`:
`rpc_health.rs`, `rpc_profitability.rs`, `rpc_analyze.rs`, `rpc_policy.rs`,
`rpc_list_banned.rs`, `rpc_list_ignored.rs`,
`rpc_hot_channel_protection_peers.rs`, `rpc_capacity_report.rs`,
`rpc_econ_snapshot.rs`, `rpc_spend_ledger.rs`.

Per the task constraints, **`main.rs` and `lib.rs` were left untouched**.
Every builder was verified end-to-end (compiles, `cargo test -p revops`,
`cargo clippy -p revops --all-targets -- -D warnings`, `cargo fmt --all --
--check`) by temporarily adding the `pub mod` lines below to `lib.rs`,
running the gates, then reverting `lib.rs` before committing — so the
snippets in this document are proven to compile and pass, not just
sketched.

## 1. `lib.rs` — add these `pub mod` lines

Insert alphabetically among the existing `pub mod rpc_*` lines:

```rust
pub mod rpc_analyze;
pub mod rpc_capacity_report;
pub mod rpc_dashboard;
pub mod rpc_econ_snapshot;
pub mod rpc_health;
pub mod rpc_history;
pub mod rpc_hot_channel_protection_peers;
pub mod rpc_list_banned;
pub mod rpc_list_ignored;
pub mod rpc_policy;
pub mod rpc_profitability;
pub mod rpc_rebalance;
pub mod rpc_report;
pub mod rpc_spend_ledger;
pub mod rpc_status;
```

(i.e. add `rpc_analyze`, `rpc_capacity_report`, `rpc_econ_snapshot`,
`rpc_health`, `rpc_hot_channel_protection_peers`, `rpc_list_banned`,
`rpc_list_ignored`, `rpc_policy`, `rpc_profitability`, `rpc_spend_ledger`
into the existing alphabetized block.)

## 2. `revops-db` read queries available for wiring

The policy, hot-channel, and spend-ledger fetch functions below now exist in
`crates/revops-db/src/queries.rs`. They execute plain reads through the
single-owner, read-only `DbHandle`; their row contracts live in `revops-db`
and the response modules re-export them.

| Available query | Python source | Consumed by |
|---|---|---|
| `all_policies(handle, now) -> Vec<PeerPolicy>` | `PolicyManager.get_all_policies` (modules/policy_manager.py, backed by `Database.get_all_policies`, database.py:7779) | `revenue-policy list`, `revenue-list-banned`, `revenue-list-ignored` |
| `policy_for_peer(handle, peer_id, now) -> PeerPolicy` (default row when absent or expired) | `PolicyManager.get_policy` (policy_manager.py:446, `Database.get_policy`, database.py:7786) | `revenue-policy get` |
| `policies_by_tag(handle, tag, now) -> Vec<PeerPolicy>` | `PolicyManager.get_peers_by_tag` (policy_manager.py:815) | `revenue-policy find` |
| `policy_changes_since(handle, since, now) -> Vec<PeerPolicy>` + `last_policy_change_timestamp(handle) -> i64` | `Database.get_policy_changes_since` / `.get_last_policy_change_timestamp` (database.py:7865, 7874) | `revenue-policy changes` |
| `hot_channel_protection_override_peers(handle) -> Vec<HotChannelProtectionOverridePeer>` | `Database.list_hot_channel_protection_override_peers` (database.py:7281-7287) | `revenue-hot-channel-protection-peers list` |
| `spend_ledger_aggregates(handle, window_hours, now) -> SpendLedgerAggregates` + `active_spend_reservations(handle, window_hours, limit, now) -> Vec<ActiveReservation>` | `Database.get_spend_ledger_summary` (database.py:4483-4581) | `revenue-spend-ledger` |

Malformed policy tag JSON and unknown enum values follow Python's fallbacks;
expired policy rows are filtered with an injected clock. Spend coverage is
measured from the oldest positive ledger timestamp, including explicit
`null`/`unknown` when no evidence exists.

`revops_db::budget::BudgetDb` remains the plugin's own write rail. It must not
be substituted for these production read-only queries.

`ChannelProfitability`/`FlowMetrics` assembly (for `revenue-profitability`
and `revenue-analyze`'s per-channel results) is intentionally NOT listed
above — producing those requires the full profitability/flow analysis
pipeline (live `listpeerchannels` + forward-history DB rows +
`classify_channel`/Kalman reclassification), which is a substantially
larger wiring task than a single query function. See each builder's
module doc comment.

## 3. `main.rs` — `.rpcmethod(...)` handlers to paste in

Add each `let <x>_name = rpc_name("<suffix>");` beside the existing block
(around `main.rs:689-705`), and each `.rpcmethod(...)` beside the existing
chain (around `main.rs:800-1200`, order does not matter).

### `revenue-health`

```rust
let health_name = rpc_name("health");
```

```rust
.rpcmethod(
    &health_name,
    "consolidated operator health check (Phase: financials.today/.week are \
     DB-backed; annualized_roc_pct and sections 2-9 are gap-marked, see _gaps)",
    |p: Plugin<SharedState>, _v| async move {
        let s = p.state();
        let Some(handle) = &s.db else {
            return Ok(serde_json::json!({"error": "Plugin not initialized"}));
        };
        let now = now_unix();
        let pnl_1d = queries::pnl_summary(handle, 1, now).await?;
        let pnl_7d = queries::pnl_summary(handle, 7, now).await?;
        // total_capacity_sats: a live `listpeerchannels` sum -- omit (pass
        // `None`) until that RPC call is wired; annualized_roc_pct will
        // then show as `null` + gap-listed, per the builder's contract.
        Ok(revops::rpc_health::build_health(now, Some(&pnl_1d), Some(&pnl_7d), None))
    },
)
```

### `revenue-profitability`

```rust
let profitability_name = rpc_name("profitability");
```

```rust
.rpcmethod(
    &profitability_name,
    "channel profitability analysis (single channel_id, or fleet-wide summary)",
    |p: Plugin<SharedState>, v: serde_json::Value| async move {
        // Wiring note: this needs a `ChannelProfitability` assembly
        // pipeline (see RPC_BATCH_A.md section 2) that does not exist
        // yet. Until it does, this handler can only return the
        // "no data" / empty-summary shape below -- do NOT fabricate a
        // `ChannelProfitability` from partial data to make this "work".
        let channel_id = v.get("channel_id").and_then(|c| c.as_str());
        match channel_id {
            Some(id) => Ok(revops::rpc_profitability::build_profitability_channel(id, None)),
            None => Ok(revops::rpc_profitability::build_profitability_summary(&[])),
        }
    },
)
```

### `revenue-analyze`

```rust
let analyze_name = rpc_name("analyze");
```

```rust
.rpcmethod(
    &analyze_name,
    "read-only flow analysis for a single channel_id (SCID); the whole-fleet \
     sweep (no channel_id) is a mutating background job and is NOT ported here",
    |p: Plugin<SharedState>, v: serde_json::Value| async move {
        // Wiring note: `metrics` needs a `FlowMetrics` assembly pipeline
        // (live channel + forward-history evidence through
        // revops_analytics::flow) that does not exist yet -- pass `None`
        // until it does; the builder already returns an honest
        // `{"channel": ..., "analysis": null}` shape for that case.
        Ok(revops::rpc_analyze::build_analyze(v.get("channel_id"), None))
    },
)
```

### `revenue-policy` (read-only actions: list/get/find/changes)

```rust
let policy_name = rpc_name("policy");
```

```rust
.rpcmethod(
    &policy_name,
    "peer policy diagnostics (READ-ONLY in this port: list/get/find/changes; \
     set/delete/tag/untag/batch are refused -- see revops::rpc_policy)",
    |p: Plugin<SharedState>, v: serde_json::Value| async move {
        let s = p.state();
        let action = revops::rpc_policy::normalize_action(
            v.get("action").and_then(|a| a.as_str()),
        );
        if let Some(err) = revops::rpc_policy::policy_action_gate(&action) {
            return Ok(err);
        }
        let Some(handle) = &s.db else {
            return Ok(serde_json::json!({"error": "Plugin not initialized"}));
        };
        let now = now_unix();
        match action.as_str() {
            "list" => {
                let policies = queries::all_policies(handle, now).await?;
                Ok(revops::rpc_policy::build_policy_list(&policies, now))
            }
            "get" => {
                let Some(peer_id) = v.get("peer_id").and_then(|p| p.as_str()) else {
                    return Ok(revops::rpc_policy::get_usage_error());
                };
                if !revops_analytics::policy::is_valid_peer_id(peer_id) {
                    return Ok(revops::rpc_policy::invalid_peer_id_error());
                }
                let policy = queries::policy_for_peer(handle, peer_id, now).await?;
                Ok(revops::rpc_policy::build_policy_get(&policy, now))
            }
            "find" => {
                let Some(tag) = v.get("tag").and_then(|t| t.as_str()) else {
                    return Ok(revops::rpc_policy::find_usage_error());
                };
                let policies = queries::policies_by_tag(handle, tag, now).await?;
                Ok(revops::rpc_policy::build_policy_find(tag, &policies, now))
            }
            "changes" => {
                let since = v.get("since").and_then(|s| s.as_i64()).unwrap_or(0);
                let changes = queries::policy_changes_since(handle, since, now).await?;
                let last_update = queries::last_policy_change_timestamp(handle).await?;
                Ok(revops::rpc_policy::build_policy_changes(
                    since,
                    &changes,
                    last_update,
                    now,
                ))
            }
            _ => unreachable!("policy_action_gate already filtered to the 4 read actions"),
        }
    },
)
```

### `revenue-list-banned`

```rust
let list_banned_name = rpc_name("list-banned");
```

```rust
.rpcmethod(
    &list_banned_name,
    "peers with an operator ban (revenue-ban)",
    |p: Plugin<SharedState>, _v| async move {
        let Some(handle) = &p.state().db else {
            return Ok(serde_json::json!({"error": "Plugin not initialized"}));
        };
        let policies = queries::all_policies(handle, now_unix()).await?;
        Ok(revops::rpc_list_banned::build_list_banned(&policies))
    },
)
```

### `revenue-list-ignored` (deprecated, ported for parity)

```rust
let list_ignored_name = rpc_name("list-ignored");
```

```rust
.rpcmethod(
    &list_ignored_name,
    "DEPRECATED: peers with strategy=passive + rebalance=disabled",
    |p: Plugin<SharedState>, _v| async move {
        let Some(handle) = &p.state().db else {
            return Ok(serde_json::json!({"error": "Plugin not initialized"}));
        };
        let policies = queries::all_policies(handle, now_unix()).await?;
        Ok(revops::rpc_list_ignored::build_list_ignored(&policies))
    },
)
```

### `revenue-hot-channel-protection-peers` (`list` action only)

```rust
let hot_channel_protection_peers_name = rpc_name("hot-channel-protection-peers");
```

```rust
.rpcmethod(
    &hot_channel_protection_peers_name,
    "list persistent hot-channel-protection peer overrides (READ-ONLY in \
     this port: add/remove/clear are DB writes and are refused)",
    |p: Plugin<SharedState>, v: serde_json::Value| async move {
        let action = v.get("action").and_then(|a| a.as_str()).unwrap_or("list");
        if action != "list" {
            return Ok(serde_json::json!({
                "error": format!(
                    "revenue-hot-channel-protection-peers {action} is not available \
                     in this read-only port; use 'list'"
                )
            }));
        }
        let Some(handle) = &p.state().db else {
            return Ok(serde_json::json!({"error": "Plugin not initialized"}));
        };
        let rows = queries::hot_channel_protection_override_peers(handle).await?;
        Ok(
            revops::rpc_hot_channel_protection_peers::
                build_hot_channel_protection_peers_list(&rows),
        )
    },
)
```

### `revenue-capacity-report`

```rust
let capacity_report_name = rpc_name("capacity-report");
```

```rust
.rpcmethod(
    &capacity_report_name,
    "strategic capital redeployment report (Phase: winner/loser \
     identification engine not yet ported -- every field but `timestamp` \
     is gap-marked, see _gaps)",
    |_p: Plugin<SharedState>, _v| async move {
        Ok(revops::rpc_capacity_report::build_capacity_report(now_unix()))
    },
)
```

### `revenue-econ-snapshot`

```rust
let econ_snapshot_name = rpc_name("econ-snapshot");
```

```rust
.rpcmethod(
    &econ_snapshot_name,
    "READ-ONLY preview of the canonical EconomicSnapshot, assembled from \
     live channels + already-computed profitability + budget (requires \
     econ_shadow_enabled)",
    |p: Plugin<SharedState>, _v| async move {
        // Wiring note: `enabled` should come from the same config surface
        // `revenue-r-config` reads (`econ_shadow_enabled`); `false` is a
        // safe placeholder that still exercises the honest "disabled" shape.
        let enabled = false;
        if !enabled {
            return Ok(revops::rpc_econ_snapshot::build_econ_snapshot(false, None, None, None));
        }
        // Live assembly needs: `listpeerchannels` (channels), an
        // already-computed `HashMap<String, ChannelProfitability>` (see
        // section 2's note on the profitability pipeline), and unified
        // budget cap/reserved/spent sats (`revops_db::budget` category
        // sums or the future `_total_cost_budget_status` port). Until all
        // three are wired, report a channel-read failure rather than an
        // empty/fabricated snapshot:
        let assembly = revops::rpc_econ_snapshot::SnapshotAssembly::ChannelReadFailed(
            "listpeerchannels / profitability / budget assembly not yet wired".to_string(),
        );
        Ok(revops::rpc_econ_snapshot::build_econ_snapshot(
            true,
            Some(assembly),
            None,
            None,
        ))
    },
)
```

### `revenue-spend-ledger`

```rust
let spend_ledger_name = rpc_name("spend-ledger");
```

```rust
.rpcmethod(
    &spend_ledger_name,
    "summary of generic spend-ledger events/reservations (opens/closes/etc.)",
    |p: Plugin<SharedState>, v: serde_json::Value| async move {
        let s = p.state();
        let Some(handle) = &s.db else {
            return Ok(serde_json::json!({"error": "Database not initialized"}));
        };
        let window_hours = v
            .get("window_hours")
            .and_then(|w| w.as_i64())
            .unwrap_or(24)
            .max(1);
        let include_reservations = v
            .get("include_reservations")
            .and_then(|b| b.as_bool())
            .unwrap_or(false);
        let reservation_limit = v
            .get("reservation_limit")
            .and_then(|limit| limit.as_i64())
            .unwrap_or(50)
            .max(1);
        let now = now_unix();
        let aggregates = queries::spend_ledger_aggregates(handle, window_hours, now).await?;
        let reservations = if include_reservations {
            Some(
                queries::active_spend_reservations(
                    handle,
                    window_hours,
                    reservation_limit,
                    now,
                )
                .await?,
            )
        } else {
            None
        };
        Ok(revops::rpc_spend_ledger::build_spend_ledger(
            window_hours,
            now,
            &aggregates,
            reservations.as_deref(),
        ))
    },
)
```

## 4. Manifest / capability advertisement

If `crates/revops/src/main.rs`'s plugin manifest declares RPC method names
separately from the `.rpcmethod()` registration (check for a
`tests/manifest.rs`-style black-box test enumerating expected methods,
as referenced in `lib.rs`'s module doc comment), add the 10 new names
there too, or that test will fail once these are wired in.

## 5. Verification status

The response builders are compiled modules and their unit tests run under
`cargo test -p revops`. The DB reads in section 2 are covered by
`crates/revops-db/tests/queries.rs`. The handlers in section 3 remain wiring
instructions until they are registered in `main.rs`; do not advertise these
RPCs as live before manifest/reachability tests exercise the real handlers.
