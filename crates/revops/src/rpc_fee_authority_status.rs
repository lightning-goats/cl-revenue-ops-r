//! Python-parity response for the fixed-at-startup fee-authority gate.
//!
//! Python constructs its gate enabled at generation 0 with reason initial,
//! then applies the startup config once. Disabling it is the only startup
//! transition: generation 1 with reason init. The Rust operating mode is
//! likewise fixed for the process lifetime, so the snapshot is immutable
//! and only observed_at changes per request.

use serde_json::{json, Value};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FeeAuthorityStatusSnapshot {
    enabled: bool,
    generation: u64,
    transitioned_at: i64,
    reason: &'static str,
}

impl FeeAuthorityStatusSnapshot {
    pub fn from_startup_mode(live_authority: bool, transitioned_at: i64) -> Self {
        if live_authority {
            Self {
                enabled: true,
                generation: 0,
                transitioned_at,
                reason: "initial",
            }
        } else {
            Self {
                enabled: false,
                generation: 1,
                transitioned_at,
                reason: "init",
            }
        }
    }

    pub fn response(&self, observed_at: i64) -> Value {
        build_fee_authority_status(
            self.enabled,
            self.generation,
            self.transitioned_at,
            observed_at,
            self.reason,
        )
    }

    /// Python-parity denial for the immediate fee-cycle RPC. Startup mode
    /// is immutable, so no authority transition can race this observation.
    pub fn fee_cycle_denial_response(&self) -> Option<Value> {
        (!self.enabled).then(|| {
            json!({
                "ok": false,
                "adjusted_channels": 0,
                "fee_debug": {},
                "status": "blocked",
                "reason": "fee_authority_disabled",
                "operation": "revenue-fee-cycle",
                "generation": self.generation,
                "transitioned_at": self.transitioned_at,
            })
        })
    }
}

pub fn build_fee_authority_status(
    enabled: bool,
    generation: u64,
    transitioned_at: i64,
    observed_at: i64,
    reason: &str,
) -> Value {
    json!({
        "schema": "revenue_ops_fee_authority/v1",
        "enabled": enabled,
        "generation": generation,
        "transitioned_at": transitioned_at,
        "observed_at": observed_at,
        "reason": reason,
    })
}
