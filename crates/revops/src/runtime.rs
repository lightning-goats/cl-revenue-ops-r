use crate::fee_mode::ObserverMode;
use crate::loop_health::{spawn_loop, LoopHandle, LoopHealthPersistence, ObserverPass};
use anyhow::Result;
use revops_db::loop_health::{LoopId, WiringStatus};
use std::collections::BTreeMap;
use std::sync::Arc;

pub use revops_db::loop_health::REQUIRED_LOOPS;

pub enum AuthorityRuntime {
    Observer(ObserverRuntime),
    Live(LiveRuntime),
}

pub struct ObserverRuntime {
    fee: Option<LoopHandle>,
    rebalance: Option<LoopHandle>,
    planner: Option<LoopHandle>,
    lnplus: Option<LoopHandle>,
    boltz: Option<LoopHandle>,
    // Task 67: the three analytics/startup loops.
    flow_analysis: Option<LoopHandle>,
    startup_snapshot: Option<LoopHandle>,
    financial_snapshot: Option<LoopHandle>,
}

/// Vetted observer passes accepted by production composition. Fields are
/// private: future subsystem ports must add their own concrete observer type
/// and constructor instead of injecting an arbitrary trait object.
///
/// ```
/// let _empty = revops::runtime::ObserverPassSet::empty();
/// ```
///
/// ```compile_fail,E0451
/// // External code cannot open the set and insert an action-bearing fake.
/// let forged = revops::runtime::ObserverPassSet { fee: None };
/// ```
pub struct ObserverPassSet {
    fee: Option<Arc<crate::fee_scheduler::FeeObserverPass>>,
    lnplus: Option<Arc<crate::lnplus_runtime::LnPlusObserverPass>>,
}

impl ObserverPassSet {
    pub fn empty() -> Self {
        Self {
            fee: None,
            lnplus: None,
        }
    }

    pub fn with_fee(mut self, pass: Arc<crate::fee_scheduler::FeeObserverPass>) -> Self {
        self.fee = Some(pass);
        self
    }

    /// Task 61 4D: the REAL LN+ observer pass (watcher-only; observer
    /// adapter types with pure action refusals — see `lnplus_runtime`).
    pub fn with_lnplus(mut self, pass: Arc<crate::lnplus_runtime::LnPlusObserverPass>) -> Self {
        self.lnplus = Some(pass);
        self
    }
}

pub async fn register_unwired_loops(store: Arc<dyn LoopHealthPersistence>) -> Result<()> {
    let now = crate::now_unix();
    for id in REQUIRED_LOOPS {
        store.register(id, WiringStatus::NotWired, now).await?;
    }
    store.reconcile(now).await?;
    Ok(())
}

impl ObserverRuntime {
    pub fn unavailable(_mode: ObserverMode) -> Self {
        Self {
            fee: None,
            rebalance: None,
            planner: None,
            lnplus: None,
            boltz: None,
            flow_analysis: None,
            startup_snapshot: None,
            financial_snapshot: None,
        }
    }
    pub async fn start(
        mode: ObserverMode,
        store: Arc<dyn LoopHealthPersistence>,
        passes: ObserverPassSet,
    ) -> Result<Self> {
        if passes.fee.is_some() && !mode.autonomous_shadow() {
            anyhow::bail!("passive observer cannot start the autonomous fee pass");
        }
        if passes.lnplus.is_some() && !mode.autonomous_shadow() {
            anyhow::bail!("passive observer cannot start the LN+ observer pass");
        }
        let mut generic: BTreeMap<LoopId, Arc<dyn ObserverPass>> = BTreeMap::new();
        if let Some(fee) = passes.fee {
            generic.insert(LoopId::Fee, fee);
        }
        if let Some(lnplus) = passes.lnplus {
            generic.insert(LoopId::LnPlus, lnplus);
        }
        Self::start_internal(store, generic).await
    }

    async fn start_internal(
        store: Arc<dyn LoopHealthPersistence>,
        mut passes: BTreeMap<LoopId, Arc<dyn ObserverPass>>,
    ) -> Result<Self> {
        let now = crate::now_unix();
        for id in REQUIRED_LOOPS {
            let wiring = if passes.contains_key(&id) {
                WiringStatus::Ready
            } else {
                WiringStatus::NotWired
            };
            store.register(id, wiring, now).await?;
        }
        store.reconcile(now).await?;
        let mut take = |id| {
            passes
                .remove(&id)
                .map(|pass| spawn_loop(id, store.clone(), pass))
        };
        Ok(Self {
            fee: take(LoopId::Fee),
            rebalance: take(LoopId::Rebalance),
            planner: take(LoopId::Planner),
            lnplus: take(LoopId::LnPlus),
            boltz: take(LoopId::Boltz),
            flow_analysis: take(LoopId::FlowAnalysis),
            startup_snapshot: take(LoopId::StartupSnapshot),
            financial_snapshot: take(LoopId::FinancialSnapshot),
        })
    }

    #[cfg(test)]
    pub(crate) async fn start_for_tests(
        store: Arc<dyn LoopHealthPersistence>,
        passes: BTreeMap<LoopId, Arc<dyn ObserverPass>>,
    ) -> Result<Self> {
        Self::start_internal(store, passes).await
    }
    pub fn handle(&self, id: LoopId) -> Option<LoopHandle> {
        match id {
            LoopId::Fee => self.fee.clone(),
            LoopId::Rebalance => self.rebalance.clone(),
            LoopId::Planner => self.planner.clone(),
            LoopId::LnPlus => self.lnplus.clone(),
            LoopId::Boltz => self.boltz.clone(),
            LoopId::FlowAnalysis => self.flow_analysis.clone(),
            LoopId::StartupSnapshot => self.startup_snapshot.clone(),
            LoopId::FinancialSnapshot => self.financial_snapshot.clone(),
        }
    }
}

pub struct LiveRuntime {
    _fee_broadcaster: crate::fee_execution::ClnFeeBroadcaster,
}
impl LiveRuntime {
    pub fn new(fee_broadcaster: crate::fee_execution::ClnFeeBroadcaster) -> Self {
        Self {
            _fee_broadcaster: fee_broadcaster,
        }
    }
}
