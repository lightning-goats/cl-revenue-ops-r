//! C71-34/C71-35: the `revenue-r-econ-snapshot` assembly, as a function an
//! integration test can drive.
//!
//! This lives outside `main.rs` on purpose. `main.rs` is a binary no test
//! can import, so a handler written inline there can only be checked by
//! reading its source -- and a source-text assertion proves the call is
//! WRITTEN, never that it BEHAVES. Every gate below is exercised through
//! real temp stores and a fake CLN socket instead.

use std::collections::HashMap;

use serde_json::Value;

use crate::rpc_econ_snapshot as econ;

/// Everything the assembly consults. Config values arrive already resolved
/// through the shared three-layer path (`DB override > live Python option >
/// fixture default`) so this never becomes a second, subtly different
/// configuration surface.
pub struct EconSources<'a> {
    pub production_db: Option<&'a revops_db::actor::DbHandle>,
    pub observer: Option<&'a revops_db::owner::ObserverHandle>,
    pub socket_path: &'a std::path::Path,
    /// `Err` = the config surface itself could not be read.
    pub receivable_ratio_target: Result<f64, String>,
    pub daily_budget_sats: Result<i64, String>,
    /// `Err` = the `econ_shadow_enabled` gate could not be read. That is
    /// NOT a disabled shadow.
    pub enabled: Result<bool, String>,
    pub now: i64,
}

/// py `revenue_econ_snapshot` + `_assemble_econ_snapshot`.
pub async fn econ_snapshot_response(sources: EconSources<'_>) -> Value {
    let enabled = match sources.enabled {
        Ok(enabled) => enabled,
        Err(detail) => {
            return econ::build_econ_snapshot_unavailable("econ_shadow_config_unavailable", &detail)
        }
    };
    if !enabled {
        // py's exact two-key disabled shape.
        return econ::build_econ_snapshot(false, None, None, None);
    }

    let Some(db) = sources.production_db else {
        return econ::build_econ_snapshot_unavailable(
            "econ_store_not_configured",
            "the production database is not configured, so the budget position \
             cannot be read",
        );
    };
    let receivable_ratio_target = match sources.receivable_ratio_target {
        Ok(value) => value,
        Err(detail) => {
            return econ::build_econ_snapshot_unavailable("econ_config_unavailable", &detail)
        }
    };
    let cap_sats = match sources.daily_budget_sats {
        Ok(value) => value,
        Err(detail) => {
            return econ::build_econ_snapshot_unavailable("econ_config_unavailable", &detail)
        }
    };

    // ONE channel fetch, shared by the snapshot assembly and the
    // profitability pass, so both describe the same node state.
    let channels =
        match crate::profitability_assembler::fetch_channel_snapshot(sources.socket_path).await {
            Ok(channels) => channels,
            Err(detail) => {
                return econ::build_econ_snapshot(
                    true,
                    Some(econ::SnapshotAssembly::ChannelReadFailed(detail)),
                    None,
                    None,
                )
            }
        };

    // py `get_budget_status(now - 24 * 3600)`.
    //
    // DISCLOSED DIVERGENCE: py wraps this in `except Exception: budget =
    // {}`, so an unreadable budget becomes ZEROS -- which reads as
    // "nothing spent, nothing reserved". This refuses instead;
    // understating committed budget is the one direction that can
    // authorise spend.
    let budget = match db.budget_status(sources.now - 24 * 3600).await {
        Ok(status) => econ::BudgetPreviewInputs {
            cap_sats,
            reserved_sats: status.reserved_sats,
            spent_sats: status.spent_sats,
        },
        Err(error) => {
            return econ::build_econ_snapshot_unavailable(
                "econ_budget_unavailable",
                &format!("{error:#}"),
            )
        }
    };

    // Profitability against the SAME channel snapshot. A failure DECLARES
    // itself and contributes no evidence -- never a fabricated empty fleet.
    let mut evidence_notes: Vec<String> = Vec::new();
    let profitability = match sources.observer {
        Some(observer) => {
            match crate::profitability_assembler::gather_profitability(
                crate::profitability_assembler::ProfitabilitySources {
                    production_db: db,
                    observer,
                    channels: &channels,
                    now: sources.now,
                },
            )
            .await
            {
                Ok(fleet) => {
                    // C71-36: a fleet-level `Ok` still carries PER-CHANNEL
                    // refusals. Dropping them would leave those channels
                    // showing zero economics with nothing to distinguish
                    // them from channels that genuinely earned nothing --
                    // the same silent-absence failure this whole slice
                    // exists to remove. Sorted so the response is
                    // deterministic across runs.
                    let mut skipped = fleet.skipped.clone();
                    skipped.sort();
                    for (scid, reason) in skipped {
                        evidence_notes.push(format!(
                            "{}: {scid}: {reason}",
                            econ::CHANNEL_EVIDENCE_SKIPPED
                        ));
                    }
                    fleet.profitability
                }
                Err(refusal) => {
                    evidence_notes.push(format!(
                        "{}: {}",
                        econ::PROFITABILITY_UNAVAILABLE,
                        refusal.detail()
                    ));
                    HashMap::new()
                }
            }
        }
        None => {
            evidence_notes.push(format!(
                "{}: the observer store is not configured",
                econ::PROFITABILITY_UNAVAILABLE
            ));
            HashMap::new()
        }
    };

    econ::build_econ_snapshot(
        true,
        Some(econ::SnapshotAssembly::Ready {
            channels: &channels,
            profitability: &profitability,
            budget: &budget,
            now: sources.now,
            receivable_ratio_target,
            evidence_notes: &evidence_notes,
        }),
        // C71-32: shadow-mode nulls, each declared by the builder. Task 69
        // supplies production-owner counters.
        None,
        None,
    )
}
