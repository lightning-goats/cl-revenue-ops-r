//! Governor facade + authority ladder (port of `modules/governor_facade.py`).
//!
//! The governor is the SOLE authority for action permissions and spending:
//! every money-moving execution passes through [`GovernorFacade::authorize`]
//! first. It adds no new authority and removes none of its own — it
//! DELEGATES every check to callables injected by the caller (reservation
//! store, pause flag, authority gate) plus the two in-process collaborators
//! ([`crate::ledger::EconLedger`], [`crate::arbiter::ActiveIntentRegistry`]).
//!
//! Decision order is FROZEN, exactly mirroring the Python docstring:
//!
//! 1. paused             -> `PAUSED`
//! 2. authority           -> `AUTHORITY_LEVEL_BLOCKED` (fail CLOSED on error)
//! 3. intent expired      -> `INTENT_STALE`
//! 4. registry conflict   -> the conflict's own reason code
//! 5. budget reserve      -> `BUDGET_EXHAUSTED` on delegate refusal
//!
//! The worst-case authorized cost (`max_cost_msat`) is reserved BEFORE any
//! execution, converted msat->sat with CEILING (budgets never undercount).
//! Zero-worst-case intents (reversible fee/HTLC changes) are authorized
//! without a reservation and without a `budget_reserved` ledger event. When
//! a ledger is attached, authorizations and reservations are recorded as
//! events.
//!
//! Live finding 2026-07-13 (phantom-reservation fix): governed paths reserve
//! under their CALLER's `reservation_id` (legacy finish-path compatibility)
//! while the facade must ledger under that SAME actual reservation id for
//! `budget_reserved` — ledgering under the envelope key instead created a
//! phantom ledger reservation the hourly sweep then auto-corrected as
//! `db_missing`. `intent_authorized` stays keyed by the envelope's own
//! `idempotency_key` (it records the proposal, not the reservation); only
//! `budget_reserved` (and the token) key off the effective reservation id.

use std::collections::BTreeMap;

use serde_json::json;

use crate::arbiter::ActiveIntentRegistry;
use crate::intents::{is_expired, IntentEnvelope};
use crate::ledger::EconLedger;
use crate::reason::Code;
use crate::types::{EconError, EconResult, UnixTime};

/// Phase 4 (Workstream I): the global authority ladder. Governed actions
/// whose required level exceeds the configured level are rejected with
/// `AUTHORITY_LEVEL_BLOCKED`.
const AUTHORITY_LEVELS: [(&str, i64); 4] = [
    ("observe", 0),
    ("fees", 1),
    ("liquidity", 2),
    ("capital", 3),
];

fn authority_level_of(level: &str) -> Option<i64> {
    AUTHORITY_LEVELS
        .iter()
        .find(|(name, _)| *name == level)
        .map(|(_, v)| *v)
}

/// `configured_level` is trimmed + lowercased before lookup; an unknown or
/// missing value fails CLOSED to `observe` (0) — a typo must never grant
/// authority. `required_level` similarly trims+lowercases; an unknown
/// required level defaults to the STRICTEST level, `capital` (3), so a
/// misspelled requirement can never be accidentally satisfied. Returns
/// `configured >= required`.
pub fn authority_allows(configured_level: Option<&str>, required_level: &str) -> bool {
    let configured = configured_level
        .map(|s| s.trim().to_lowercase())
        .and_then(|s| authority_level_of(&s))
        .unwrap_or(0);
    let required = authority_level_of(&required_level.trim().to_lowercase()).unwrap_or(3);
    configured >= required
}

/// Proof of a governor grant: the caller executes only while holding one of
/// these, and must present it back to [`GovernorFacade::release`] when the
/// reservation is no longer needed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorizationToken {
    pub token_id: String,
    pub intent_id: String,
    pub reservation_id: String,
    pub reserved_msat: i64,
    pub budget_bucket: String,
    pub issued_at: i64,
    /// Envelope idempotency key — the arbitration registry's slot key (may
    /// differ from `reservation_id` on legacy-compat paths).
    pub arbitration_key: String,
}

/// The outcome of one [`GovernorFacade::authorize`] call. `reason_code` is
/// `""` on success (never a valid reason code), mirroring the Python
/// dataclass's `str` field paired with `_decision_wire`'s
/// `str(decision.reason_code or "")`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GovernorDecision {
    pub authorized: bool,
    pub token: Option<AuthorizationToken>,
    pub reason_code: String,
}

impl GovernorDecision {
    fn refused(reason_code: &str) -> Self {
        GovernorDecision {
            authorized: false,
            token: None,
            reason_code: reason_code.to_string(),
        }
    }
}

/// The governor facade. Every dependency is injected so the facade adds no
/// authority of its own — it only sequences existing checks in the frozen
/// order above.
pub struct GovernorFacade<'a> {
    /// `(reservation_id, amount_sats, category) -> granted`. An `Err` here
    /// is a hard Rust-level error propagated out of `authorize` (mirrors an
    /// uncaught Python exception escaping the un-guarded delegate call) —
    /// NOT a fail-closed `false` decision.
    pub reserve_spend: &'a dyn Fn(&str, i64, &str) -> EconResult<bool>,
    /// `(reservation_id) -> released`. Same propagation contract as
    /// `reserve_spend`.
    pub release_spend: &'a dyn Fn(&str) -> EconResult<bool>,
    pub is_paused: &'a dyn Fn() -> bool,
    pub ledger: Option<&'a EconLedger>,
    pub registry: Option<&'a ActiveIntentRegistry>,
    /// `Err` => fail CLOSED (mirrors Python's `try: ... except Exception:
    /// allowed = False`) — the ONE check in this facade whose errors are
    /// swallowed rather than propagated, because an authority-check failure
    /// must never accidentally propagate into "unauthorized behavior" via a
    /// crash; it must always resolve to the safe, restrictive answer.
    pub authority_check: Option<&'a dyn Fn() -> EconResult<bool>>,
}

impl GovernorFacade<'_> {
    /// Decision order FROZEN: paused -> authority (fail closed on error) ->
    /// expired -> registry conflict (ledger `intent_rejected` best-effort,
    /// swallowed) -> reserve `max_cost_msat` msat->sat CEIL under the
    /// CALLER's `reservation_id` (2026-07-13 phantom-reservation fix) ->
    /// `BUDGET_EXHAUSTED` on refusal. `reserve_sats == 0` (reversible
    /// fee/HTLC change): authorize WITHOUT reservation and WITHOUT a
    /// `budget_reserved` event. Ledger keying: `intent_authorized` under
    /// `env.idempotency_key`; `budget_reserved` under
    /// `effective_reservation_id`. `token_id` = `"auth-" + key.get(..16)`
    /// (defensive: falls back to the whole key on a short/multibyte-boundary
    /// key rather than panicking — see the inline comment at the slice
    /// site); `reserved_msat` = checked `reserve_sats * 1000` (`Err` on
    /// overflow rather than wrapping).
    pub fn authorize(
        &self,
        env: &IntentEnvelope,
        now: i64,
        reservation_id: Option<&str>,
    ) -> EconResult<GovernorDecision> {
        if (self.is_paused)() {
            return Ok(GovernorDecision::refused(Code::Paused.as_str()));
        }

        if let Some(check) = self.authority_check {
            let allowed = check().unwrap_or(false);
            if !allowed {
                return Ok(GovernorDecision::refused(
                    Code::AuthorityLevelBlocked.as_str(),
                ));
            }
        }

        if is_expired(env, UnixTime::new(now)?) {
            return Ok(GovernorDecision::refused(Code::IntentStale.as_str()));
        }

        if let Some(registry) = self.registry {
            if let Some(conflict) = registry.check_and_register(env, now) {
                if let Some(ledger) = self.ledger {
                    let details = json!({"reason_code": conflict, "arbitration": true});
                    // Best-effort: mirrors Python's `try: ... except
                    // Exception: pass` around this append.
                    let _ = ledger.append(
                        "intent_rejected",
                        env.intent_id.as_str(),
                        &env.idempotency_key,
                        &env.snapshot_id,
                        now,
                        &BTreeMap::new(),
                        &details,
                    );
                }
                return Ok(GovernorDecision::refused(conflict));
            }
        }

        let effective_reservation_id = reservation_id
            .filter(|s| !s.is_empty())
            .unwrap_or(&env.idempotency_key)
            .to_string();
        let reserve_sats = env.max_cost_msat.to_sats_ceil().value();
        if reserve_sats > 0 {
            let granted =
                (self.reserve_spend)(&effective_reservation_id, reserve_sats, &env.budget_bucket)?;
            if !granted {
                return Ok(GovernorDecision::refused(Code::BudgetExhausted.as_str()));
            }
        }

        // `reserve_sats * 1000` (msat) is checked rather than a bare `*`:
        // `max_cost_msat` is only bounded to u63 (`[0, 2**63-1]`), and
        // `to_sats_ceil` can push `reserve_sats` just far enough that
        // re-expanding to msat overflows i64 (e.g. `max_cost_msat ==
        // i64::MAX` ceils to a sat count whose *1000 exceeds i64::MAX by
        // 193). This is reachable with a legitimately-constructed
        // max-range envelope, not just adversarial input — fail closed
        // with `Err` rather than wrapping.
        let reserved_msat = reserve_sats.checked_mul(1000).ok_or_else(|| EconError {
            msg: format!("governor authorize: reserve_sats * 1000 overflow: {reserve_sats}"),
        })?;

        // `env.idempotency_key[..16]` panics if the key arrives (via
        // `from_wire`) shorter than 16 bytes or with a multibyte
        // codepoint straddling the byte-16 boundary. Python's
        // `IntentEnvelope.__post_init__` (see `modules/econ_intents.py`)
        // does NOT validate `idempotency_key` shape at all — Python
        // string slicing (`env.idempotency_key[:16]`) never raises
        // regardless of length or content. Parity therefore means: do
        // not add a validation Python lacks; instead make the Rust byte
        // slice defensive with `.get(..16)`, which returns `None` (falling
        // back to the whole key) on both "too short" and "not a char
        // boundary" rather than panicking.
        let token_id_key = env
            .idempotency_key
            .get(..16)
            .unwrap_or(&env.idempotency_key);
        let token = AuthorizationToken {
            token_id: format!("auth-{token_id_key}"),
            intent_id: env.intent_id.as_str().to_string(),
            reservation_id: effective_reservation_id.clone(),
            reserved_msat,
            budget_bucket: env.budget_bucket.clone(),
            issued_at: now,
            arbitration_key: env.idempotency_key.clone(),
        };

        if let Some(ledger) = self.ledger {
            let mut authorized_amounts = BTreeMap::new();
            authorized_amounts.insert("max_cost_msat".to_string(), env.max_cost_msat.value());
            ledger.append(
                "intent_authorized",
                env.intent_id.as_str(),
                &env.idempotency_key,
                &env.snapshot_id,
                now,
                &authorized_amounts,
                &json!({}),
            )?;

            if token.reserved_msat > 0 {
                // Zero-cost intents (reversible fee/HTLC changes) are
                // authorized without a reservation — recording one would
                // misstate the ledger.
                let mut reserved_amounts = BTreeMap::new();
                reserved_amounts.insert("reserved_msat".to_string(), token.reserved_msat);
                ledger.append(
                    "budget_reserved",
                    &token.intent_id,
                    &effective_reservation_id,
                    &env.snapshot_id,
                    now,
                    &reserved_amounts,
                    &json!({}),
                )?;
            }
        }

        Ok(GovernorDecision {
            authorized: true,
            token: Some(token),
            reason_code: String::new(),
        })
    }

    /// Registry release (`arbitration_key` else `reservation_id`,
    /// best-effort — the registry API itself is infallible, so there is
    /// nothing to swallow beyond "key not present"), then `release_spend`,
    /// then a `reservation_released` ledger event (`cycle_id` `"release"`).
    pub fn release(&self, token: &AuthorizationToken, now: i64) -> EconResult<bool> {
        if let Some(registry) = self.registry {
            let key = if !token.arbitration_key.is_empty() {
                &token.arbitration_key
            } else {
                &token.reservation_id
            };
            registry.release(key);
        }

        let released = (self.release_spend)(&token.reservation_id)?;

        if let Some(ledger) = self.ledger {
            let mut amounts = BTreeMap::new();
            amounts.insert("released_msat".to_string(), token.reserved_msat);
            ledger.append(
                "reservation_released",
                &token.intent_id,
                &token.reservation_id,
                "release",
                now,
                &amounts,
                &json!({}),
            )?;
        }

        Ok(released)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- authority_allows ---
    //
    // Pure-function unit tests live here (module-owned, no cross-module
    // envelope construction needed). `GovernorFacade::authorize`/`release`
    // integration tests — including the corpus replays — live in
    // `tests/governor.rs`, mirroring the split already established by
    // `arbiter.rs` (module-owned `ActiveIntentRegistry` unit tests here,
    // batch `arbitrate` corpus replays in `tests/arbiter.rs`).

    #[test]
    fn authority_allows_orders_ladder_correctly() {
        assert!(authority_allows(Some("capital"), "capital"));
        assert!(authority_allows(Some("capital"), "observe"));
        assert!(!authority_allows(Some("observe"), "capital"));
        assert!(!authority_allows(Some("fees"), "liquidity"));
        assert!(authority_allows(Some("liquidity"), "fees"));
    }

    #[test]
    fn authority_allows_trims_and_lowercases_configured() {
        assert!(authority_allows(Some("  Capital \n"), "capital"));
        assert!(authority_allows(Some("FEES"), "fees"));
    }

    #[test]
    fn authority_allows_unknown_configured_fails_closed_to_observe() {
        // A typo must never grant authority: unknown configured -> 0, which
        // satisfies only a required level of "observe" (0) itself.
        assert!(!authority_allows(Some("typo-level"), "fees"));
        assert!(authority_allows(Some("typo-level"), "observe"));
        assert!(authority_allows(Some("observe"), "observe"));
        assert!(!authority_allows(None, "fees"));
    }

    #[test]
    fn authority_allows_unknown_required_defaults_to_capital() {
        // Unknown required -> 3 (capital, the strictest level).
        assert!(!authority_allows(Some("liquidity"), "not-a-level"));
        assert!(authority_allows(Some("capital"), "not-a-level"));
    }
}
