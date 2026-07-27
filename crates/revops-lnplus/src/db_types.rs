//! Local-ledger row shapes and status vocabulary. Ports the `lnplus_swaps`
//! table's column set (`database.py:7564-7671`, `_LNPLUS_UPDATABLE_FIELDS`
//! / `_LNPLUS_INFLIGHT_STATUSES` / `_LNPLUS_TERMINAL_STATUSES`) and the
//! `lnplus_swaps.py` module-level status constants used to select rows.

use crate::types::Metadata;

/// `SwapLifecycle._LNPLUS_INFLIGHT_STATUSES` (database.py:7563): a row in
/// one of these statuses counts toward `has_inflight()` (the
/// one-application-per-node serialization gate) and — when
/// `channel_funding_txid` is still `NULL` — toward reserved capex budget
/// (`lnplus_reserved_sats`, database.py:7619-7629).
pub const INFLIGHT_STATUSES: [&str; 3] = ["applied", "opening", "opened"];

/// `database.py:7654` `_LNPLUS_TERMINAL_STATUSES` — rows old enough here
/// are eligible for `lnplus_prune_terminal`.
pub const TERMINAL_STATUSES: [&str; 4] = ["ended", "failed", "withdrawn", "cancelled_remote"];

/// `lnplus_swaps.py:680` `_TERMINAL_PENDING_GHOST_STATUSES` (B5b): a local
/// row in one of these statuses that LN+ still lists as `pending` is a
/// stale application we should clean up (`delete_application`), not an
/// untracked ghost worth tripping the breaker over.
pub const TERMINAL_PENDING_GHOST_STATUSES: [&str; 3] = ["failed", "withdrawn", "cancelled_remote"];

pub fn is_inflight_status(status: &str) -> bool {
    INFLIGHT_STATUSES.contains(&status)
}

pub fn is_terminal_status(status: &str) -> bool {
    TERMINAL_STATUSES.contains(&status)
}

/// One row of the local `lnplus_swaps` ledger (`_lnplus_row_to_dict`
/// shape, database.py:7597-7605). All the state the lifecycle machine
/// reads or writes for a single swap.
#[derive(Debug, Clone, PartialEq)]
pub struct SwapRow {
    pub swap_id: String,
    pub status: String,
    pub capacity_sats: i64,
    pub duration_months: i64,
    pub outbound_peer: Option<String>,
    pub incoming_peer: Option<String>,
    pub our_identifier: Option<String>,
    pub applied_at: i64,
    pub opened_at: Option<i64>,
    pub ends_at: Option<i64>,
    pub deadline_at: Option<i64>,
    pub channel_funding_txid: Option<String>,
    pub outcome: Option<String>,
    pub tag_added: Option<bool>,
    pub incoming_tag_added: Option<bool>,
    pub planner_action_id: Option<i64>,
    pub metadata: Option<Metadata>,
}

impl SwapRow {
    /// A fresh row exactly as `lnplus_record_swap` (database.py:7571-7584)
    /// would create it (`applied_at = now`, every optional column unset).
    pub fn new(
        swap_id: impl Into<String>,
        status: impl Into<String>,
        capacity_sats: i64,
        duration_months: i64,
        applied_at: i64,
    ) -> Self {
        Self {
            swap_id: swap_id.into(),
            status: status.into(),
            capacity_sats,
            duration_months,
            outbound_peer: None,
            incoming_peer: None,
            our_identifier: None,
            applied_at,
            opened_at: None,
            ends_at: None,
            deadline_at: None,
            channel_funding_txid: None,
            outcome: None,
            tag_added: None,
            incoming_tag_added: None,
            planner_action_id: None,
            metadata: None,
        }
    }

    pub fn with_outbound_peer(mut self, peer: impl Into<String>) -> Self {
        self.outbound_peer = Some(peer.into());
        self
    }
    pub fn with_incoming_peer(mut self, peer: impl Into<String>) -> Self {
        self.incoming_peer = Some(peer.into());
        self
    }
    pub fn with_our_identifier(mut self, id: impl Into<String>) -> Self {
        self.our_identifier = Some(id.into());
        self
    }
    pub fn with_deadline_at(mut self, ts: i64) -> Self {
        self.deadline_at = Some(ts);
        self
    }
    pub fn with_ends_at(mut self, ts: i64) -> Self {
        self.ends_at = Some(ts);
        self
    }
    pub fn with_channel_funding_txid(mut self, txid: impl Into<String>) -> Self {
        self.channel_funding_txid = Some(txid.into());
        self
    }
    pub fn with_opened_at(mut self, ts: i64) -> Self {
        self.opened_at = Some(ts);
        self
    }
    pub fn with_planner_action_id(mut self, id: i64) -> Self {
        self.planner_action_id = Some(id);
        self
    }
    pub fn apply(&mut self, patch: &SwapPatch) {
        patch.apply_to(self);
    }
}

/// `lnplus_update_swap(sid, **fields)` (database.py:7586-7595) as an
/// explicit, statically-typed field set instead of Python's `**kwargs`
/// bag validated at runtime against `_LNPLUS_UPDATABLE_FIELDS`. Every
/// field is `Option`: `None` means "leave unchanged", matching Python's
/// "only the passed kwargs are in the SET clause" behaviour. There is
/// deliberately no way to express "set this column back to NULL" for most
/// fields (Python's kwargs mechanism could pass `field=None` and it WOULD
/// write NULL — the callers in this module never do that for any column
/// except the ones modeled with a dedicated clear method below), matching
/// every real call site in `lnplus_swaps.py`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SwapPatch {
    pub status: Option<String>,
    pub outbound_peer: Option<String>,
    pub incoming_peer: Option<String>,
    pub our_identifier: Option<String>,
    pub opened_at: Option<i64>,
    pub ends_at: Option<i64>,
    pub deadline_at: Option<i64>,
    pub channel_funding_txid: Option<String>,
    pub outcome: Option<String>,
    pub tag_added: Option<bool>,
    pub incoming_tag_added: Option<bool>,
}

impl SwapPatch {
    pub fn status(mut self, s: impl Into<String>) -> Self {
        self.status = Some(s.into());
        self
    }
    pub fn outbound_peer(mut self, s: impl Into<String>) -> Self {
        self.outbound_peer = Some(s.into());
        self
    }
    pub fn incoming_peer(mut self, s: impl Into<String>) -> Self {
        self.incoming_peer = Some(s.into());
        self
    }
    pub fn our_identifier(mut self, s: impl Into<String>) -> Self {
        self.our_identifier = Some(s.into());
        self
    }
    pub fn opened_at(mut self, ts: i64) -> Self {
        self.opened_at = Some(ts);
        self
    }
    pub fn ends_at(mut self, ts: i64) -> Self {
        self.ends_at = Some(ts);
        self
    }
    pub fn deadline_at(mut self, ts: i64) -> Self {
        self.deadline_at = Some(ts);
        self
    }
    pub fn channel_funding_txid(mut self, txid: impl Into<String>) -> Self {
        self.channel_funding_txid = Some(txid.into());
        self
    }
    pub fn outcome(mut self, s: impl Into<String>) -> Self {
        self.outcome = Some(s.into());
        self
    }
    pub fn tag_added(mut self, v: bool) -> Self {
        self.tag_added = Some(v);
        self
    }
    pub fn incoming_tag_added(mut self, v: bool) -> Self {
        self.incoming_tag_added = Some(v);
        self
    }

    pub fn apply_to(&self, row: &mut SwapRow) {
        if let Some(v) = &self.status {
            row.status = v.clone();
        }
        if let Some(v) = &self.outbound_peer {
            row.outbound_peer = Some(v.clone());
        }
        if let Some(v) = &self.incoming_peer {
            row.incoming_peer = Some(v.clone());
        }
        if let Some(v) = &self.our_identifier {
            row.our_identifier = Some(v.clone());
        }
        if let Some(v) = self.opened_at {
            row.opened_at = Some(v);
        }
        if let Some(v) = self.ends_at {
            row.ends_at = Some(v);
        }
        if let Some(v) = self.deadline_at {
            row.deadline_at = Some(v);
        }
        if let Some(v) = &self.channel_funding_txid {
            row.channel_funding_txid = Some(v.clone());
        }
        if let Some(v) = &self.outcome {
            row.outcome = Some(v.clone());
        }
        if let Some(v) = self.tag_added {
            row.tag_added = Some(v);
        }
        if let Some(v) = self.incoming_tag_added {
            row.incoming_tag_added = Some(v);
        }
    }
}

/// `lnplus_peers` row (`lnplus_get_peer`, database.py:7648-7652).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct PeerRow {
    pub pubkey: String,
    pub swaps_count: i64,
    pub defections: i64,
    pub ratings_given_positive: i64,
    pub ratings_given_negative: i64,
    pub last_swap_at: Option<i64>,
}
