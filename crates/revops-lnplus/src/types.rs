//! DTOs exchanged with the LN+ API and the local ledger. These are the
//! Rust shape of the loosely-typed `Dict`s Python passes around
//! (`lnplus_swaps.py` throughout); every `Option` here mirrors a Python
//! `.get(...)` that can legitimately come back `None`/missing from an
//! untrusted external API.

use std::collections::BTreeMap;

use crate::validation::TsValue;

/// One participant in a swap ring, as returned by `get_applicable_swaps` /
/// `get_swap` / `get_my_swaps`. Field names mirror the LN+ wire shape.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Participant {
    pub participant_identifier: Option<String>,
    pub pubkey: Option<String>,
    pub cancelled: bool,
    pub banned: bool,
    pub address_1: Option<String>,
    pub address_2: Option<String>,
    pub positive_ratings_count: i64,
    pub negative_ratings_count: i64,
    pub lnplus_rank_number: i64,
}

/// A swap listed by `get_applicable_swaps` (pre-application) — the input
/// to [`crate::evaluator`]'s gate chain.
#[derive(Debug, Clone, PartialEq)]
pub struct SwapListing {
    pub id: String,
    pub status: String,
    pub participant_waiting_for_count: i64,
    pub capacity_sats: i64,
    pub duration_months: i64,
    pub participant_max_count: i64,
    pub platform: Option<String>,
    pub participants: Vec<Participant>,
}

/// `get_swap(id)` detail — used by backfill's authoritative outbound-peer
/// derivation (py 1177-1232).
#[derive(Debug, Clone, PartialEq)]
pub struct SwapDetail {
    pub id: String,
    pub capacity_sats: Option<i64>,
    pub duration_months: Option<i64>,
    pub participants: Vec<Participant>,
}

/// One entry from `get_my_swaps`' `pending`/`opening`/`completed` buckets.
/// A single flexible shape (mirrors the loosely-typed Python dict) rather
/// than three structs, since backfill/reconcile/the watcher all read
/// different subsets of the same fields depending on which bucket the
/// entry came from.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct MySwapEntry {
    pub id: String,
    pub capacity_sats: Option<i64>,
    pub duration_months: Option<i64>,
    pub outgoing_peer_pubkey: Option<String>,
    pub outgoing_peer_clearnet_address: Option<String>,
    pub outgoing_peer_tor_address: Option<String>,
    pub incoming_peer_pubkey: Option<String>,
    pub deadline: Option<TsValue>,
    pub ends: Option<TsValue>,
}

/// `get_my_swaps` response (py 169-181), already de-enveloped and with
/// every bucket normalized to a non-null `Vec`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct MySwaps {
    pub pending: Vec<MySwapEntry>,
    pub opening: Vec<MySwapEntry>,
    pub completed: Vec<MySwapEntry>,
}

impl MySwaps {
    pub fn pending_ids(&self) -> std::collections::BTreeSet<String> {
        self.pending.iter().map(|e| e.id.clone()).collect()
    }
    pub fn opening_ids(&self) -> std::collections::BTreeSet<String> {
        self.opening.iter().map(|e| e.id.clone()).collect()
    }
    pub fn completed_ids(&self) -> std::collections::BTreeSet<String> {
        self.completed.iter().map(|e| e.id.clone()).collect()
    }
}

/// `create_rating`'s two allowed values (py 223-229 `rating not in
/// ("positive", "negative")`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rating {
    Positive,
    Negative,
}

impl Rating {
    pub fn as_str(&self) -> &'static str {
        match self {
            Rating::Positive => "positive",
            Rating::Negative => "negative",
        }
    }
}

/// One notification entry (py `get_notifications` / `_poll_notifications`,
/// 2089-2112). Observability-only; no autonomous action is ever taken from
/// this data (py comment, 2093).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct NotificationEntry {
    pub kind: Option<String>,
    pub body: Option<String>,
    pub created_at: Option<String>,
    pub url: Option<String>,
}

/// `metadata` payload attached to `record_planner_action` /
/// `lnplus_record_swap` calls — a small string-keyed bag, mirroring
/// Python's free-form `dict`.
pub type Metadata = BTreeMap<String, String>;
