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
    // Task 71 / F71-R16: the three analytics owners. Each is its OWN
    // concrete type (F71-R17) -- deliberately NOT `Arc<dyn ObserverPass>`,
    // which would let any external crate hand the observer runtime an
    // arbitrary action-bearing pass.
    flow_analysis: Option<Arc<crate::analytics_passes::FlowAnalysisPass>>,
    startup_snapshot: Option<Arc<crate::analytics_passes::StartupSnapshotPass>>,
    financial_snapshot: Option<Arc<crate::analytics_passes::FinancialSnapshotPass>>,
}

impl ObserverPassSet {
    pub fn empty() -> Self {
        Self {
            fee: None,
            flow_analysis: None,
            startup_snapshot: None,
            financial_snapshot: None,
        }
    }

    /// Task 71: the flow-analysis observer pass. Observation-only -- it
    /// reads `listpeerchannels`, runs the frozen analytics kernels, and
    /// writes only to the plugin's own store, so unlike `with_fee` and
    /// `with_lnplus` it is NOT gated on autonomous-shadow authority.
    pub fn with_flow_analysis(
        mut self,
        pass: Arc<crate::analytics_passes::FlowAnalysisPass>,
    ) -> Self {
        self.flow_analysis = Some(pass);
        self
    }

    /// Task 71: the one-shot startup peer snapshot. Observation-only.
    pub fn with_startup_snapshot(
        mut self,
        pass: Arc<crate::analytics_passes::StartupSnapshotPass>,
    ) -> Self {
        self.startup_snapshot = Some(pass);
        self
    }

    /// Task 71: the daily financial snapshot. Observation-only.
    pub fn with_financial_snapshot(
        mut self,
        pass: Arc<crate::analytics_passes::FinancialSnapshotPass>,
    ) -> Self {
        self.financial_snapshot = Some(pass);
        self
    }

    pub fn with_fee(mut self, pass: Arc<crate::fee_scheduler::FeeObserverPass>) -> Self {
        self.fee = Some(pass);
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
        let mut generic: BTreeMap<LoopId, Arc<dyn ObserverPass>> = BTreeMap::new();
        if let Some(fee) = passes.fee {
            generic.insert(LoopId::Fee, fee);
        }
        if let Some(flow) = passes.flow_analysis {
            generic.insert(LoopId::FlowAnalysis, flow);
        }
        if let Some(snapshot) = passes.startup_snapshot {
            generic.insert(LoopId::StartupSnapshot, snapshot);
        }
        if let Some(financial) = passes.financial_snapshot {
            generic.insert(LoopId::FinancialSnapshot, financial);
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
            LoopId::Planner | LoopId::LnPlus | LoopId::Boltz => None,
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
