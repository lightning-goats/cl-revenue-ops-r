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
}

impl ObserverRuntime {
    pub fn unavailable() -> Self {
        Self {
            fee: None,
            rebalance: None,
            planner: None,
            lnplus: None,
            boltz: None,
        }
    }
    pub async fn start(
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
        })
    }
    pub fn handle(&self, id: LoopId) -> Option<LoopHandle> {
        match id {
            LoopId::Fee => self.fee.clone(),
            LoopId::Rebalance => self.rebalance.clone(),
            LoopId::Planner => self.planner.clone(),
            LoopId::LnPlus => self.lnplus.clone(),
            LoopId::Boltz => self.boltz.clone(),
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
