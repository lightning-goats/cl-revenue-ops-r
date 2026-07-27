//! Safety wrappers around the two ports whose mutating methods can move
//! money or make an irreversible commitment on our behalf:
//! [`LnPlusApi`] (applying to / withdrawing from / completing a swap,
//! rating a counterparty) and [`ChainPort`] (connecting to a peer, funding
//! a channel).
//!
//! Every kernel call site in this crate that reaches one of these mutating
//! methods — `evaluator::select_and_apply`'s `api.create_application` (py
//! 636-661), `open::execute_swap_open`'s `chain.connect` /
//! `chain.fund_channel` (py 1583-1657), `withdrawal::handle_pending_timeouts`'s
//! `api.delete_application`, `open::complete_and_mark_opened`'s
//! `api.complete_application`, `finalize::try_create_rating`'s
//! `api.create_rating` — is reachable ONLY through whatever `&dyn LnPlusApi`
//! / `&dyn ChainPort` [`crate::loop_drivers::evaluator_pass`] /
//! [`crate::loop_drivers::watcher_pass`] hand to the kernel, and those two
//! functions never hand over the raw, ungated port: they always build
//! [`GatedLnPlusApi`] / [`GatedChainPort`] first, from the caller's
//! [`crate::exec_mode::ExecutionMode`].
//!
//! Read-only methods on both traits (`get_applicable_swaps`, `get_swap`,
//! `get_my_swaps`, `get_notifications`, `our_node_id`, `list_peer_channels`,
//! `opening_feerate_perkw`, `confirmed_unreserved_sats`) pass straight
//! through in every mode. Everything else — including
//! `complete_application` / `create_rating` / `mark_read_notifications`,
//! which are the same class of irreversible-on-LN+'s-side call even though
//! the task spec's exact phrase ("applies to a swap, withdraws an
//! application, or opens a channel") doesn't name them individually — is
//! gated.

use crate::error::LnPlusError;
use crate::exec_mode::ExecutionMode;
use crate::ports::{
    ChainPort, ChannelInfo, Feerate, FundChannelResult, LnPlusApi, LogLevel, Logger, PortError,
    PortResult,
};
use crate::types::{MySwaps, NotificationEntry, Rating, SwapDetail, SwapListing};

fn dry_run_log(logger: &dyn Logger, what: &str) {
    logger.log(
        LogLevel::Warn,
        &format!(
            "LNPLUS: [DRY RUN] suppressed live call: {what} — ExecutionMode::DryRun; pass ExecutionMode::Armed to allow"
        ),
    );
}

/// Wraps a real [`LnPlusApi`] so every mutating method is a no-op (returns
/// an `Err`, never reaching `inner`) unless constructed with
/// [`ExecutionMode::Armed`].
pub struct GatedLnPlusApi<'a> {
    inner: &'a dyn LnPlusApi,
    mode: ExecutionMode,
    logger: &'a dyn Logger,
}

impl<'a> GatedLnPlusApi<'a> {
    pub fn new(inner: &'a dyn LnPlusApi, mode: ExecutionMode, logger: &'a dyn Logger) -> Self {
        Self {
            inner,
            mode,
            logger,
        }
    }

    /// `Ok(())` iff armed; otherwise logs and returns the suppression error
    /// WITHOUT calling `inner` — the safety property this whole module
    /// exists for.
    fn guard(&self, what: &str) -> Result<(), LnPlusError> {
        if self.mode.is_armed() {
            return Ok(());
        }
        dry_run_log(self.logger, what);
        Err(LnPlusError::new(format!(
            "execution mode is DryRun — refusing to {what}"
        )))
    }
}

impl<'a> LnPlusApi for GatedLnPlusApi<'a> {
    fn get_applicable_swaps(&self) -> Result<Vec<SwapListing>, LnPlusError> {
        self.inner.get_applicable_swaps()
    }

    fn get_swap(&self, swap_id: &str) -> Result<SwapDetail, LnPlusError> {
        self.inner.get_swap(swap_id)
    }

    fn get_my_swaps(&self) -> Result<MySwaps, LnPlusError> {
        self.inner.get_my_swaps()
    }

    fn create_application(&self, swap_id: &str) -> Result<(), LnPlusError> {
        self.guard(&format!("create_application({swap_id})"))?;
        self.inner.create_application(swap_id)
    }

    fn delete_application(&self, swap_id: &str) -> Result<(), LnPlusError> {
        self.guard(&format!("delete_application({swap_id})"))?;
        self.inner.delete_application(swap_id)
    }

    fn complete_application(&self, swap_id: &str) -> Result<(), LnPlusError> {
        self.guard(&format!("complete_application({swap_id})"))?;
        self.inner.complete_application(swap_id)
    }

    fn get_notifications(&self) -> Result<Vec<NotificationEntry>, LnPlusError> {
        self.inner.get_notifications()
    }

    fn mark_read_notifications(&self) -> Result<(), LnPlusError> {
        self.guard("mark_read_notifications")?;
        self.inner.mark_read_notifications()
    }

    fn create_rating(&self, swap_id: &str, rating: Rating) -> Result<(), LnPlusError> {
        self.guard(&format!("create_rating({swap_id}, {})", rating.as_str()))?;
        self.inner.create_rating(swap_id, rating)
    }
}

/// Wraps a real [`ChainPort`] so `connect`/`fund_channel` are no-ops
/// (return an `Err`, never reaching `inner`) unless constructed with
/// [`ExecutionMode::Armed`].
pub struct GatedChainPort<'a> {
    inner: &'a dyn ChainPort,
    mode: ExecutionMode,
    logger: &'a dyn Logger,
}

impl<'a> GatedChainPort<'a> {
    pub fn new(inner: &'a dyn ChainPort, mode: ExecutionMode, logger: &'a dyn Logger) -> Self {
        Self {
            inner,
            mode,
            logger,
        }
    }

    fn guard(&self, what: &str) -> PortResult<()> {
        if self.mode.is_armed() {
            return Ok(());
        }
        dry_run_log(self.logger, what);
        Err(PortError::new(format!(
            "execution mode is DryRun — refusing to {what}"
        )))
    }
}

impl<'a> ChainPort for GatedChainPort<'a> {
    fn our_node_id(&self) -> PortResult<String> {
        self.inner.our_node_id()
    }

    fn list_peer_channels(&self, peer: Option<&str>) -> PortResult<Vec<ChannelInfo>> {
        self.inner.list_peer_channels(peer)
    }

    fn opening_feerate_perkw(&self) -> PortResult<i64> {
        self.inner.opening_feerate_perkw()
    }

    fn confirmed_unreserved_sats(&self) -> PortResult<i64> {
        self.inner.confirmed_unreserved_sats()
    }

    fn connect(&self, target: &str) -> PortResult<()> {
        self.guard(&format!("connect({target})"))?;
        self.inner.connect(target)
    }

    fn fund_channel(
        &self,
        peer: &str,
        amount_sats: i64,
        feerate: Feerate,
    ) -> PortResult<FundChannelResult> {
        self.guard(&format!("fund_channel({peer}, {amount_sats} sats)"))?;
        self.inner.fund_channel(peer, amount_sats, feerate)
    }
}
