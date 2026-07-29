//! Boltz configuration resolution — the parity fix for finding 4 of the
//! 2026-07-29 `parity_matrix.py` run.
//!
//! `main.rs` previously built the Boltz query transport from
//! `BoltzCliProcessConfig::default()`, which ships `enabled=false`. The
//! 17 `revops-r-boltz-*` options were registered but never read, so eight
//! Boltz RPCs answered "Boltz CLI integration disabled" while Python --
//! with `revenue-ops-boltz-enabled=true` — returned real wallet and
//! budget data. One bug, eight mismatches.
//!
//! Resolution reuses [`crate::fee_config`]'s layer-aware helpers so the
//! precedence and the CASTS are identical to every other subsystem:
//! (a) DB override, (b) live Python `listconfigs` value, (c) the
//! documented default. Reimplementing the boolean parse here would
//! reintroduce exactly the `'1'`/`'yes'`/`'on'` divergence
//! `fee_config::resolve_bool` exists to prevent.

use std::collections::HashMap;
use std::sync::atomic::AtomicU64;

use cln_plugin::options::Value as OptValue;
use revops_boltz::process::BoltzCliProcessConfig;
use revops_db::actor::DbHandle;

use crate::fee_config::{resolve_bool, resolve_int, resolve_string_opt};

/// Python's own defaults (`boltz_manager.py` `BoltzCliConfig`, plus the
/// budget/auto-cycle option defaults in `cl-revenue-ops.py`). `enabled`
/// defaults FALSE: a plugin that cannot see the operator's config must
/// refuse rather than assume Boltz is on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoltzCfgSnapshot {
    pub enabled: bool,
    pub cli_path: String,
    pub datadir: String,
    pub use_sudo: bool,
    pub sudo_user: String,
    pub timeout_seconds: u64,
    pub daily_budget_sats: i64,
    pub structural_budget_sats: i64,
    pub auto_cycle_enabled: bool,
    pub max_withdraw_sats: i64,
    // The auto-cycle + treasury block Python's
    // revenue-boltz-auto-cycle-status reports. Resolved, never
    // defaulted: on lnnode Python has treasury enabled with a 1M target
    // while the Rust fixture defaults are false / 5M.
    pub auto_cycle_interval_minutes: i64,
    pub auto_cycle_max_actions: i64,
    pub auto_cycle_startup_delay_seconds: i64,
    pub expansion_treasury_enabled: bool,
    pub expansion_treasury_onchain_target_sats: i64,
    pub expansion_treasury_min_deficit_sats: i64,
}

impl Default for BoltzCfgSnapshot {
    fn default() -> Self {
        Self {
            enabled: false,
            cli_path: "/usr/local/bin/boltzcli".to_string(),
            datadir: "/var/lib/boltz".to_string(),
            use_sudo: false,
            sudo_user: "boltz".to_string(),
            timeout_seconds: 60,
            daily_budget_sats: 3_000,
            structural_budget_sats: 0,
            auto_cycle_enabled: false,
            max_withdraw_sats: 10_000_000,
            auto_cycle_interval_minutes: 15,
            auto_cycle_max_actions: 1,
            auto_cycle_startup_delay_seconds: 120,
            expansion_treasury_enabled: false,
            expansion_treasury_onchain_target_sats: 5_000_000,
            expansion_treasury_min_deficit_sats: 250_000,
        }
    }
}

impl BoltzCfgSnapshot {
    /// The single place that decides what the transport is configured
    /// with, so `main.rs` cannot drift from the resolved values.
    pub fn to_process_config(&self) -> BoltzCliProcessConfig {
        BoltzCliProcessConfig::new(
            self.enabled,
            self.cli_path.clone(),
            self.datadir.clone(),
            self.use_sudo,
            self.sudo_user.clone(),
            self.timeout_seconds,
        )
    }
}

/// Resolve the Boltz config through the standard three layers.
pub async fn resolve_boltz_cfg(
    db: Option<&DbHandle>,
    python_option_values: &HashMap<String, OptValue>,
) -> BoltzCfgSnapshot {
    let failures = &AtomicU64::new(0);
    let default = BoltzCfgSnapshot::default();
    BoltzCfgSnapshot {
        enabled: resolve_bool(
            db,
            python_option_values,
            failures,
            "boltz-enabled",
            default.enabled,
        )
        .await,
        cli_path: resolve_string_opt(
            db,
            python_option_values,
            failures,
            "boltz-cli-path",
            Some(default.cli_path.clone()),
        )
        .await
        .unwrap_or(default.cli_path),
        datadir: resolve_string_opt(
            db,
            python_option_values,
            failures,
            "boltz-datadir",
            Some(default.datadir.clone()),
        )
        .await
        .unwrap_or(default.datadir),
        use_sudo: resolve_bool(
            db,
            python_option_values,
            failures,
            "boltz-use-sudo",
            default.use_sudo,
        )
        .await,
        sudo_user: resolve_string_opt(
            db,
            python_option_values,
            failures,
            "boltz-sudo-user",
            Some(default.sudo_user.clone()),
        )
        .await
        .unwrap_or(default.sudo_user),
        timeout_seconds: resolve_int(
            db,
            python_option_values,
            failures,
            "boltz-timeout-seconds",
            default.timeout_seconds as i64,
        )
        .await
        .max(1) as u64,
        daily_budget_sats: resolve_int(
            db,
            python_option_values,
            failures,
            "boltz-daily-budget-sats",
            default.daily_budget_sats,
        )
        .await
        .max(0),
        structural_budget_sats: resolve_int(
            db,
            python_option_values,
            failures,
            "boltz-structural-budget-sats",
            default.structural_budget_sats,
        )
        .await
        .max(0),
        auto_cycle_enabled: resolve_bool(
            db,
            python_option_values,
            failures,
            "boltz-auto-cycle-enabled",
            default.auto_cycle_enabled,
        )
        .await,
        max_withdraw_sats: resolve_int(
            db,
            python_option_values,
            failures,
            "boltz-max-withdraw-sats",
            default.max_withdraw_sats,
        )
        .await
        .max(0),
        auto_cycle_interval_minutes: resolve_int(
            db,
            python_option_values,
            failures,
            "boltz-auto-cycle-interval-minutes",
            default.auto_cycle_interval_minutes,
        )
        .await
        .max(1),
        auto_cycle_max_actions: resolve_int(
            db,
            python_option_values,
            failures,
            "boltz-auto-cycle-max-actions",
            default.auto_cycle_max_actions,
        )
        .await
        .max(0),
        auto_cycle_startup_delay_seconds: resolve_int(
            db,
            python_option_values,
            failures,
            "boltz-auto-cycle-startup-delay-seconds",
            default.auto_cycle_startup_delay_seconds,
        )
        .await
        .max(0),
        expansion_treasury_enabled: resolve_bool(
            db,
            python_option_values,
            failures,
            "expansion-treasury-enabled",
            default.expansion_treasury_enabled,
        )
        .await,
        expansion_treasury_onchain_target_sats: resolve_int(
            db,
            python_option_values,
            failures,
            "expansion-treasury-onchain-target-sats",
            default.expansion_treasury_onchain_target_sats,
        )
        .await
        .max(0),
        expansion_treasury_min_deficit_sats: resolve_int(
            db,
            python_option_values,
            failures,
            "expansion-treasury-min-deficit-sats",
            default.expansion_treasury_min_deficit_sats,
        )
        .await
        .max(0),
    }
}
