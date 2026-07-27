//! In-memory fakes for every port in `revops_lnplus::ports`. HARD RULE 2:
//! no test in this crate ever touches the network, a real database, or a
//! real CLN node — every trait is backed by one of these.

#![allow(dead_code)]

use std::cell::RefCell;
use std::collections::BTreeMap;

use revops_lnplus::breaker::BreakerState;
use revops_lnplus::db_types::{PeerRow, SwapPatch, SwapRow};
use revops_lnplus::error::LnPlusError;
use revops_lnplus::ports::{
    ChainPort, ChannelInfo, Feerate, FundChannelResult, IgnorePeerPort, LnPlusApi, LnPlusDb,
    LogLevel, Logger, PeerPolicy, PlannerActionRequest, PlannerPort, PolicyPort, PortError,
    PortResult, ReserveSpendRequest,
};
use revops_lnplus::types::{MySwaps, NotificationEntry, Rating, SwapDetail, SwapListing};

// ============================= Logger =====================================

#[derive(Default)]
pub struct FakeLogger {
    pub lines: RefCell<Vec<(LogLevel, String)>>,
}

impl FakeLogger {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn contains(&self, needle: &str) -> bool {
        self.lines
            .borrow()
            .iter()
            .any(|(_, msg)| msg.contains(needle))
    }
    pub fn count_containing(&self, needle: &str) -> usize {
        self.lines
            .borrow()
            .iter()
            .filter(|(_, msg)| msg.contains(needle))
            .count()
    }
}

impl Logger for FakeLogger {
    fn log(&self, level: LogLevel, message: &str) {
        self.lines.borrow_mut().push((level, message.to_string()));
    }
}

// ============================== DB =========================================

#[derive(Default)]
pub struct FakeDb {
    pub swaps: RefCell<BTreeMap<String, SwapRow>>,
    pub peers: RefCell<BTreeMap<String, PeerRow>>,
    pub config: RefCell<BTreeMap<String, String>>,
    pub breaker: RefCell<Option<BreakerState>>,
    pub next_action_id: RefCell<i64>,
    pub planner_actions: RefCell<Vec<PlannerActionRecord>>,
    pub reservations: RefCell<BTreeMap<String, ReservationRecord>>,
    pub reserve_spend_result: RefCell<Option<PortResult<bool>>>,
    pub release_should_fail: RefCell<bool>,
    pub mark_spent_should_fail: RefCell<Option<u32>>, // Some(n) = fail n times then succeed
}

#[derive(Debug, Clone)]
pub struct PlannerActionRecord {
    pub id: i64,
    pub action_type: String,
    pub peer_id: String,
    pub status: String,
    pub reason: String,
}

#[derive(Debug, Clone)]
pub struct ReservationRecord {
    pub amount_sats: i64,
    pub active: bool,
    pub spent: bool,
}

impl FakeDb {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&self, row: SwapRow) {
        self.swaps.borrow_mut().insert(row.swap_id.clone(), row);
    }

    pub fn action_status(&self, action_type: &str) -> Option<String> {
        self.planner_actions
            .borrow()
            .iter()
            .rev()
            .find(|a| a.action_type == action_type)
            .map(|a| a.status.clone())
    }
}

impl LnPlusDb for FakeDb {
    fn record_swap(&self, row: &SwapRow) {
        self.swaps
            .borrow_mut()
            .insert(row.swap_id.clone(), row.clone());
    }

    fn update_swap(&self, swap_id: &str, patch: &SwapPatch) {
        let mut swaps = self.swaps.borrow_mut();
        if let Some(row) = swaps.get_mut(swap_id) {
            row.apply(patch);
        }
    }

    fn get_swap(&self, swap_id: &str) -> Option<SwapRow> {
        self.swaps.borrow().get(swap_id).cloned()
    }

    fn get_swaps_by_status(&self, statuses: &[&str]) -> Vec<SwapRow> {
        let mut rows: Vec<SwapRow> = self
            .swaps
            .borrow()
            .values()
            .filter(|r| statuses.contains(&r.status.as_str()))
            .cloned()
            .collect();
        rows.sort_by_key(|r| r.applied_at);
        rows
    }

    fn prune_terminal(&self, older_than_days: i64, now: i64) -> usize {
        let cutoff = now - older_than_days * 86_400;
        let mut swaps = self.swaps.borrow_mut();
        let before = swaps.len();
        swaps.retain(|_, r| {
            let terminal = revops_lnplus::db_types::is_terminal_status(&r.status);
            !(terminal && r.applied_at < cutoff && r.ends_at.map(|e| e < cutoff).unwrap_or(true))
        });
        before - swaps.len()
    }

    fn get_peer(&self, pubkey: &str) -> Option<PeerRow> {
        self.peers.borrow().get(pubkey).cloned()
    }

    fn bump_peer(&self, pubkey: &str, defection: bool, rating: Option<Rating>) {
        let mut peers = self.peers.borrow_mut();
        let entry = peers.entry(pubkey.to_string()).or_insert_with(|| PeerRow {
            pubkey: pubkey.to_string(),
            ..Default::default()
        });
        entry.swaps_count += 1;
        if defection {
            entry.defections += 1;
        }
        match rating {
            Some(Rating::Positive) => entry.ratings_given_positive += 1,
            Some(Rating::Negative) => entry.ratings_given_negative += 1,
            None => {}
        }
    }

    fn get_config_override(&self, key: &str) -> Option<String> {
        self.config.borrow().get(key).cloned()
    }
    fn set_config_override(&self, key: &str, value: &str) {
        self.config
            .borrow_mut()
            .insert(key.to_string(), value.to_string());
    }
    fn delete_config_override(&self, key: &str) {
        self.config.borrow_mut().remove(key);
    }

    fn get_breaker(&self) -> Option<BreakerState> {
        self.breaker.borrow().clone()
    }
    fn set_breaker(&self, state: &BreakerState) {
        *self.breaker.borrow_mut() = Some(state.clone());
    }
    fn clear_breaker(&self) {
        *self.breaker.borrow_mut() = None;
    }

    fn record_planner_action(&self, req: &PlannerActionRequest) -> i64 {
        let mut id_cell = self.next_action_id.borrow_mut();
        *id_cell += 1;
        let id = *id_cell;
        self.planner_actions.borrow_mut().push(PlannerActionRecord {
            id,
            action_type: req.action_type.to_string(),
            peer_id: req.peer_id.clone(),
            status: "pending".to_string(),
            reason: req.reason.clone(),
        });
        id
    }

    fn update_planner_action(&self, action_id: i64, status: &str) {
        if let Some(a) = self
            .planner_actions
            .borrow_mut()
            .iter_mut()
            .find(|a| a.id == action_id)
        {
            a.status = status.to_string();
        }
    }

    fn reserve_spend(&self, req: &ReserveSpendRequest) -> PortResult<bool> {
        if let Some(result) = self.reserve_spend_result.borrow().as_ref() {
            return result.clone();
        }
        self.reservations.borrow_mut().insert(
            req.reservation_id.clone(),
            ReservationRecord {
                amount_sats: req.amount_sats,
                active: true,
                spent: false,
            },
        );
        Ok(true)
    }

    fn release_spend_reservation(&self, reservation_id: &str) -> PortResult<()> {
        if *self.release_should_fail.borrow() {
            return Err(PortError::new("release failed"));
        }
        if let Some(r) = self.reservations.borrow_mut().get_mut(reservation_id) {
            r.active = false;
        }
        Ok(())
    }

    fn mark_spend_reservation_spent(
        &self,
        reservation_id: &str,
        _actual_spent_sats: i64,
        _source: &str,
    ) -> PortResult<bool> {
        if let Some(remaining) = *self.mark_spent_should_fail.borrow() {
            if remaining > 0 {
                *self.mark_spent_should_fail.borrow_mut() = Some(remaining - 1);
                return Ok(false);
            }
        }
        if let Some(r) = self.reservations.borrow_mut().get_mut(reservation_id) {
            r.spent = true;
        }
        Ok(true)
    }
}

// ============================== API ========================================

pub struct FakeApi {
    pub applicable_swaps: RefCell<Result<Vec<SwapListing>, LnPlusError>>,
    pub swap_details: RefCell<BTreeMap<String, Result<SwapDetail, LnPlusError>>>,
    pub my_swaps: RefCell<Result<MySwaps, LnPlusError>>,
    pub create_application_result: RefCell<Result<(), LnPlusError>>,
    pub delete_application_results: RefCell<BTreeMap<String, Result<(), LnPlusError>>>,
    pub complete_application_results: RefCell<BTreeMap<String, Result<(), LnPlusError>>>,
    pub notifications: RefCell<Result<Vec<NotificationEntry>, LnPlusError>>,
    pub create_rating_results: RefCell<BTreeMap<String, Result<(), LnPlusError>>>,
    pub delete_application_calls: RefCell<Vec<String>>,
    pub complete_application_calls: RefCell<Vec<String>>,
    pub create_rating_calls: RefCell<Vec<(String, Rating)>>,
    pub create_application_calls: RefCell<Vec<String>>,
    /// Call counters for [`crate::loop_drivers`] short-circuit-ordering
    /// tests: a disabled/blocked pass must not issue these LIVE, signed
    /// calls at all.
    pub get_applicable_swaps_calls: RefCell<u32>,
    pub get_my_swaps_calls: RefCell<u32>,
}

impl Default for FakeApi {
    fn default() -> Self {
        Self {
            applicable_swaps: RefCell::new(Ok(Vec::new())),
            swap_details: RefCell::new(BTreeMap::new()),
            my_swaps: RefCell::new(Ok(MySwaps::default())),
            create_application_result: RefCell::new(Ok(())),
            delete_application_results: RefCell::new(BTreeMap::new()),
            complete_application_results: RefCell::new(BTreeMap::new()),
            notifications: RefCell::new(Ok(Vec::new())),
            create_rating_results: RefCell::new(BTreeMap::new()),
            delete_application_calls: RefCell::new(Vec::new()),
            complete_application_calls: RefCell::new(Vec::new()),
            create_rating_calls: RefCell::new(Vec::new()),
            create_application_calls: RefCell::new(Vec::new()),
            get_applicable_swaps_calls: RefCell::new(0),
            get_my_swaps_calls: RefCell::new(0),
        }
    }
}

impl FakeApi {
    pub fn new() -> Self {
        Self::default()
    }
}

impl LnPlusApi for FakeApi {
    fn get_applicable_swaps(&self) -> Result<Vec<SwapListing>, LnPlusError> {
        *self.get_applicable_swaps_calls.borrow_mut() += 1;
        self.applicable_swaps.borrow().clone()
    }

    fn get_swap(&self, swap_id: &str) -> Result<SwapDetail, LnPlusError> {
        self.swap_details
            .borrow()
            .get(swap_id)
            .cloned()
            .unwrap_or_else(|| {
                Err(LnPlusError::new(format!(
                    "no fixture for get_swap({swap_id})"
                )))
            })
    }

    fn get_my_swaps(&self) -> Result<MySwaps, LnPlusError> {
        *self.get_my_swaps_calls.borrow_mut() += 1;
        self.my_swaps.borrow().clone()
    }

    fn create_application(&self, swap_id: &str) -> Result<(), LnPlusError> {
        self.create_application_calls
            .borrow_mut()
            .push(swap_id.to_string());
        self.create_application_result.borrow().clone()
    }

    fn delete_application(&self, swap_id: &str) -> Result<(), LnPlusError> {
        self.delete_application_calls
            .borrow_mut()
            .push(swap_id.to_string());
        self.delete_application_results
            .borrow()
            .get(swap_id)
            .cloned()
            .unwrap_or(Ok(()))
    }

    fn complete_application(&self, swap_id: &str) -> Result<(), LnPlusError> {
        self.complete_application_calls
            .borrow_mut()
            .push(swap_id.to_string());
        self.complete_application_results
            .borrow()
            .get(swap_id)
            .cloned()
            .unwrap_or(Ok(()))
    }

    fn get_notifications(&self) -> Result<Vec<NotificationEntry>, LnPlusError> {
        self.notifications.borrow().clone()
    }

    fn mark_read_notifications(&self) -> Result<(), LnPlusError> {
        Ok(())
    }

    fn create_rating(&self, swap_id: &str, rating: Rating) -> Result<(), LnPlusError> {
        self.create_rating_calls
            .borrow_mut()
            .push((swap_id.to_string(), rating));
        self.create_rating_results
            .borrow()
            .get(swap_id)
            .cloned()
            .unwrap_or(Ok(()))
    }
}

// ============================== Policy =====================================

#[derive(Default, Clone)]
pub struct FakePeerPolicy {
    pub tags: Vec<String>,
}

impl PeerPolicy for FakePeerPolicy {
    fn has_tag(&self, tag: &str) -> bool {
        self.tags.iter().any(|t| t == tag)
    }
}

#[derive(Default)]
pub struct FakePolicy {
    pub tags: RefCell<BTreeMap<String, Vec<String>>>,
    pub banned: RefCell<Vec<String>>,
    pub get_policy_fails_for: RefCell<Vec<String>>,
    pub ban_lookup_fails_for: RefCell<Vec<String>>,
    pub add_tag_fails_for: RefCell<Vec<String>>,
}

impl FakePolicy {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn ban(&self, pubkey: &str) {
        self.banned.borrow_mut().push(pubkey.to_string());
    }
    pub fn pre_tag(&self, peer: &str, tag: &str) {
        self.tags
            .borrow_mut()
            .entry(peer.to_string())
            .or_default()
            .push(tag.to_string());
    }
    pub fn has_tag(&self, peer: &str, tag: &str) -> bool {
        self.tags
            .borrow()
            .get(peer)
            .map(|t| t.iter().any(|x| x == tag))
            .unwrap_or(false)
    }
}

impl PolicyPort for FakePolicy {
    fn get_policy(&self, peer: &str) -> PortResult<Option<Box<dyn PeerPolicy>>> {
        if self.get_policy_fails_for.borrow().iter().any(|p| p == peer) {
            return Err(PortError::new("policy lookup failed"));
        }
        let tags = self.tags.borrow();
        match tags.get(peer) {
            Some(t) => Ok(Some(Box::new(FakePeerPolicy { tags: t.clone() }))),
            None => Ok(None),
        }
    }

    fn add_tag(&self, peer: &str, tag: &str) -> PortResult<()> {
        if self.add_tag_fails_for.borrow().iter().any(|p| p == peer) {
            return Err(PortError::new("add_tag failed"));
        }
        self.tags
            .borrow_mut()
            .entry(peer.to_string())
            .or_default()
            .push(tag.to_string());
        Ok(())
    }

    fn remove_tag(&self, peer: &str, tag: &str) -> PortResult<()> {
        if let Some(tags) = self.tags.borrow_mut().get_mut(peer) {
            tags.retain(|t| t != tag);
        }
        Ok(())
    }

    fn is_peer_banned(&self, pubkey: &str) -> PortResult<bool> {
        if self
            .ban_lookup_fails_for
            .borrow()
            .iter()
            .any(|p| p == pubkey)
        {
            return Err(PortError::new("ban lookup failed"));
        }
        Ok(self.banned.borrow().iter().any(|p| p == pubkey))
    }
}

// ============================== Planner ====================================

#[derive(Default)]
pub struct FakePlanner {
    pub open_ev: RefCell<BTreeMap<String, f64>>,
    pub default_ev: RefCell<f64>,
    pub open_cost: RefCell<i64>,
    pub scores: RefCell<BTreeMap<String, f64>>,
    pub default_score: RefCell<f64>,
    pub capex_budget: RefCell<Option<i64>>,
}

impl FakePlanner {
    pub fn new() -> Self {
        Self {
            default_score: RefCell::new(1.0),
            open_cost: RefCell::new(2000),
            capex_budget: RefCell::new(None),
            ..Default::default()
        }
    }
    pub fn set_ev(&self, peer: &str, ev: f64) {
        self.open_ev.borrow_mut().insert(peer.to_string(), ev);
    }
    pub fn set_score(&self, peer: &str, score: f64) {
        self.scores.borrow_mut().insert(peer.to_string(), score);
    }
}

impl PlannerPort for FakePlanner {
    fn calculate_open_ev(&self, peer: Option<&str>, _capacity_sats: i64) -> f64 {
        match peer {
            Some(p) => self
                .open_ev
                .borrow()
                .get(p)
                .copied()
                .unwrap_or(*self.default_ev.borrow()),
            None => *self.default_ev.borrow(),
        }
    }
    fn estimate_open_cost(&self) -> i64 {
        *self.open_cost.borrow()
    }
    fn score_candidate(&self, pubkey: &str, base: f64) -> f64 {
        self.scores
            .borrow()
            .get(pubkey)
            .copied()
            .unwrap_or(*self.default_score.borrow() * base)
    }
    fn capex_fleet_exploration_budget(&self) -> Option<i64> {
        *self.capex_budget.borrow()
    }
}

// ============================== Chain ======================================

pub struct FakeChain {
    pub our_id: RefCell<PortResult<String>>,
    pub channels: RefCell<Vec<ChannelInfo>>,
    pub list_peer_channels_fails: RefCell<bool>,
    pub opening_feerate: RefCell<PortResult<i64>>,
    pub confirmed_sats: RefCell<PortResult<i64>>,
    pub connect_calls: RefCell<Vec<String>>,
    pub connect_result: RefCell<PortResult<()>>,
    pub fund_channel_result: RefCell<PortResult<FundChannelResult>>,
    pub fund_channel_calls: RefCell<Vec<(String, i64, Feerate)>>,
    /// Call counters for [`crate::loop_drivers`] short-circuit-ordering
    /// tests.
    pub opening_feerate_calls: RefCell<u32>,
    pub confirmed_sats_calls: RefCell<u32>,
}

impl Default for FakeChain {
    fn default() -> Self {
        Self {
            our_id: RefCell::new(Ok(pubkey(9))),
            channels: RefCell::new(Vec::new()),
            list_peer_channels_fails: RefCell::new(false),
            opening_feerate: RefCell::new(Ok(1000)),
            confirmed_sats: RefCell::new(Ok(1_000_000_000)),
            connect_calls: RefCell::new(Vec::new()),
            connect_result: RefCell::new(Ok(())),
            fund_channel_result: RefCell::new(Ok(FundChannelResult {
                txid: Some("txid1".to_string()),
            })),
            fund_channel_calls: RefCell::new(Vec::new()),
            opening_feerate_calls: RefCell::new(0),
            confirmed_sats_calls: RefCell::new(0),
        }
    }
}

impl FakeChain {
    pub fn new() -> Self {
        Self::default()
    }
}

impl ChainPort for FakeChain {
    fn our_node_id(&self) -> PortResult<String> {
        self.our_id.borrow().clone()
    }
    fn list_peer_channels(&self, peer: Option<&str>) -> PortResult<Vec<ChannelInfo>> {
        if *self.list_peer_channels_fails.borrow() {
            return Err(PortError::new("listpeerchannels failed"));
        }
        let channels = self.channels.borrow();
        Ok(match peer {
            Some(p) => channels
                .iter()
                .filter(|c| c.peer_id == p)
                .cloned()
                .collect(),
            None => channels.clone(),
        })
    }
    fn opening_feerate_perkw(&self) -> PortResult<i64> {
        *self.opening_feerate_calls.borrow_mut() += 1;
        self.opening_feerate.borrow().clone()
    }
    fn confirmed_unreserved_sats(&self) -> PortResult<i64> {
        *self.confirmed_sats_calls.borrow_mut() += 1;
        self.confirmed_sats.borrow().clone()
    }
    fn connect(&self, target: &str) -> PortResult<()> {
        self.connect_calls.borrow_mut().push(target.to_string());
        self.connect_result.borrow().clone()
    }
    fn fund_channel(
        &self,
        peer: &str,
        amount_sats: i64,
        feerate: Feerate,
    ) -> PortResult<FundChannelResult> {
        self.fund_channel_calls
            .borrow_mut()
            .push((peer.to_string(), amount_sats, feerate));
        self.fund_channel_result.borrow().clone()
    }
}

// ============================== Ignore-peer =================================

#[derive(Default)]
pub struct FakeIgnorePeer {
    pub calls: RefCell<Vec<(String, String)>>,
    pub should_fail: RefCell<bool>,
}

impl IgnorePeerPort for FakeIgnorePeer {
    fn ignore_peer(&self, pubkey: &str, reason: &str) -> PortResult<()> {
        self.calls
            .borrow_mut()
            .push((pubkey.to_string(), reason.to_string()));
        if *self.should_fail.borrow() {
            return Err(PortError::new("ignore_peer failed"));
        }
        Ok(())
    }
}

// ============================== builders ===================================

pub fn pubkey(seed: u8) -> String {
    format!("02{:064x}", seed as u128)
}

pub fn participant(identifier: &str, pk: &str) -> revops_lnplus::types::Participant {
    revops_lnplus::types::Participant {
        participant_identifier: Some(identifier.to_string()),
        pubkey: Some(pk.to_string()),
        cancelled: false,
        banned: false,
        address_1: Some("203.0.113.1:9735".to_string()),
        address_2: None,
        positive_ratings_count: 10,
        negative_ratings_count: 0,
        lnplus_rank_number: 5,
    }
}

pub fn listing(id: &str, participants: Vec<revops_lnplus::types::Participant>) -> SwapListing {
    let max = participants.len() as i64 + 1;
    SwapListing {
        id: id.to_string(),
        status: "pending".to_string(),
        participant_waiting_for_count: 1,
        capacity_sats: 2_000_000,
        duration_months: 6,
        participant_max_count: max,
        platform: Some("cln".to_string()),
        participants,
    }
}

// ============================ HTTP transport / signer =======================
// Fakes for `revops_lnplus::http` — HARD RULE: no test anywhere in this
// crate ever performs a real network call. `FakeHttpTransport` is an
// in-memory queue of canned responses; nothing here ever opens a socket.

use std::collections::VecDeque;

use revops_lnplus::http::{
    HttpMethod, HttpResponse, HttpTransport, SignError, Signer, TransportError,
};

#[derive(Debug, Clone)]
pub struct RecordedRequest {
    pub method: HttpMethod,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: Option<Vec<u8>>,
}

#[derive(Default)]
pub struct FakeHttpTransport {
    pub responses: RefCell<VecDeque<Result<HttpResponse, TransportError>>>,
    pub calls: RefCell<Vec<RecordedRequest>>,
}

impl FakeHttpTransport {
    pub fn new() -> Self {
        Self::default()
    }

    /// Queue a 200 OK JSON response.
    pub fn push_json(&self, body: serde_json::Value) {
        self.responses.borrow_mut().push_back(Ok(HttpResponse {
            status: 200,
            body: serde_json::to_vec(&body).unwrap(),
        }));
    }

    pub fn push_status(&self, status: u16, body: serde_json::Value) {
        self.responses.borrow_mut().push_back(Ok(HttpResponse {
            status,
            body: serde_json::to_vec(&body).unwrap(),
        }));
    }

    pub fn push_raw_status(&self, status: u16, body: &[u8]) {
        self.responses.borrow_mut().push_back(Ok(HttpResponse {
            status,
            body: body.to_vec(),
        }));
    }

    pub fn push_transport_err(&self, msg: &str) {
        self.responses
            .borrow_mut()
            .push_back(Err(TransportError(msg.to_string())));
    }

    pub fn call_count(&self) -> usize {
        self.calls.borrow().len()
    }
}

impl HttpTransport for FakeHttpTransport {
    fn request(
        &self,
        method: HttpMethod,
        url: &str,
        headers: &[(String, String)],
        body: Option<Vec<u8>>,
    ) -> Result<HttpResponse, TransportError> {
        self.calls.borrow_mut().push(RecordedRequest {
            method,
            url: url.to_string(),
            headers: headers.to_vec(),
            body: body.clone(),
        });
        self.responses
            .borrow_mut()
            .pop_front()
            .unwrap_or_else(|| Err(TransportError("no fixture queued".to_string())))
    }
}

pub struct FakeSigner {
    pub result: RefCell<Result<String, SignError>>,
    pub calls: RefCell<Vec<String>>,
}

impl Default for FakeSigner {
    fn default() -> Self {
        Self {
            result: RefCell::new(Ok("zbase-signature".to_string())),
            calls: RefCell::new(Vec::new()),
        }
    }
}

impl FakeSigner {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Signer for FakeSigner {
    fn signmessage(&self, message: &str) -> Result<String, SignError> {
        self.calls.borrow_mut().push(message.to_string());
        self.result.borrow().clone()
    }
}
