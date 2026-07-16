//! Stable machine-readable reason codes (Rust port of
//! `modules/reason_codes.py`).
//!
//! Codes are the conformance contract; human-readable wording may evolve
//! freely. Each code declares its owning layer and decision kind. The
//! catalog is wire-frozen at exactly 18 codes — see `reason_codes.py` lines
//! 26-46 in the Python source of truth.

/// Wire-frozen reason code. Variant names ARE the wire strings (rendered by
/// [`Code::as_str`] in `UPPER_SNAKE`, matching the Python `code` field
/// verbatim).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Code {
    BudgetExhausted,
    AuthorityLevelBlocked,
    Paused,
    IntentStale,
    IntentSuperseded,
    ChannelProtected,
    ContractObligation,
    EvBelowHoldMargin,
    InsufficientConfidence,
    FeeRailClamped,
    CooldownActive,
    ConflictCloseRebalance,
    ConflictDuplicateOpen,
    ConflictRebalanceSwap,
    ExternalCircuitBreaker,
    ExternalOutcomeUnknown,
    ArithmeticOverflow,
    SchemaInvalid,
}

/// All 18 codes, in the exact order of the Python `_CODES` tuple.
pub const ALL: [Code; 18] = [
    Code::BudgetExhausted,
    Code::AuthorityLevelBlocked,
    Code::Paused,
    Code::IntentStale,
    Code::IntentSuperseded,
    Code::ChannelProtected,
    Code::ContractObligation,
    Code::EvBelowHoldMargin,
    Code::InsufficientConfidence,
    Code::FeeRailClamped,
    Code::CooldownActive,
    Code::ConflictCloseRebalance,
    Code::ConflictDuplicateOpen,
    Code::ConflictRebalanceSwap,
    Code::ExternalCircuitBreaker,
    Code::ExternalOutcomeUnknown,
    Code::ArithmeticOverflow,
    Code::SchemaInvalid,
];

impl Code {
    /// The wire string, e.g. `"BUDGET_EXHAUSTED"`.
    pub const fn as_str(self) -> &'static str {
        match self {
            Code::BudgetExhausted => "BUDGET_EXHAUSTED",
            Code::AuthorityLevelBlocked => "AUTHORITY_LEVEL_BLOCKED",
            Code::Paused => "PAUSED",
            Code::IntentStale => "INTENT_STALE",
            Code::IntentSuperseded => "INTENT_SUPERSEDED",
            Code::ChannelProtected => "CHANNEL_PROTECTED",
            Code::ContractObligation => "CONTRACT_OBLIGATION",
            Code::EvBelowHoldMargin => "EV_BELOW_HOLD_MARGIN",
            Code::InsufficientConfidence => "INSUFFICIENT_CONFIDENCE",
            Code::FeeRailClamped => "FEE_RAIL_CLAMPED",
            Code::CooldownActive => "COOLDOWN_ACTIVE",
            Code::ConflictCloseRebalance => "CONFLICT_CLOSE_REBALANCE",
            Code::ConflictDuplicateOpen => "CONFLICT_DUPLICATE_OPEN",
            Code::ConflictRebalanceSwap => "CONFLICT_REBALANCE_SWAP",
            Code::ExternalCircuitBreaker => "EXTERNAL_CIRCUIT_BREAKER",
            Code::ExternalOutcomeUnknown => "EXTERNAL_OUTCOME_UNKNOWN",
            Code::ArithmeticOverflow => "ARITHMETIC_OVERFLOW",
            Code::SchemaInvalid => "SCHEMA_INVALID",
        }
    }

    /// Owning layer: one of `policy`, `arbiter`, `governor`, `executor`,
    /// `reconciliation`, `any`.
    pub const fn layer(self) -> &'static str {
        match self {
            Code::BudgetExhausted => "governor",
            Code::AuthorityLevelBlocked => "governor",
            Code::Paused => "governor",
            Code::IntentStale => "arbiter",
            Code::IntentSuperseded => "arbiter",
            Code::ChannelProtected => "governor",
            Code::ContractObligation => "arbiter",
            Code::EvBelowHoldMargin => "policy",
            Code::InsufficientConfidence => "governor",
            Code::FeeRailClamped => "policy",
            Code::CooldownActive => "policy",
            Code::ConflictCloseRebalance => "arbiter",
            Code::ConflictDuplicateOpen => "arbiter",
            Code::ConflictRebalanceSwap => "arbiter",
            Code::ExternalCircuitBreaker => "executor",
            Code::ExternalOutcomeUnknown => "reconciliation",
            Code::ArithmeticOverflow => "any",
            Code::SchemaInvalid => "any",
        }
    }

    /// Decision kind: one of `hold`, `rejection`, `deferral`, `clamp`,
    /// `failure`, `unknown`.
    pub const fn kind(self) -> &'static str {
        match self {
            Code::BudgetExhausted => "rejection",
            Code::AuthorityLevelBlocked => "rejection",
            Code::Paused => "rejection",
            Code::IntentStale => "deferral",
            Code::IntentSuperseded => "rejection",
            Code::ChannelProtected => "rejection",
            Code::ContractObligation => "rejection",
            Code::EvBelowHoldMargin => "hold",
            Code::InsufficientConfidence => "hold",
            Code::FeeRailClamped => "clamp",
            Code::CooldownActive => "hold",
            Code::ConflictCloseRebalance => "rejection",
            Code::ConflictDuplicateOpen => "rejection",
            Code::ConflictRebalanceSwap => "rejection",
            Code::ExternalCircuitBreaker => "deferral",
            Code::ExternalOutcomeUnknown => "unknown",
            Code::ArithmeticOverflow => "failure",
            Code::SchemaInvalid => "failure",
        }
    }
}

/// Mirrors Python's `is_valid_code`: true iff `code` is one of the 18 wire
/// strings.
pub fn is_valid_code(code: &str) -> bool {
    ALL.iter().any(|c| c.as_str() == code)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exactly_eighteen_codes() {
        assert_eq!(ALL.len(), 18);
    }

    #[test]
    fn all_wire_strings_unique() {
        let mut strs: Vec<&str> = ALL.iter().map(|c| c.as_str()).collect();
        strs.sort_unstable();
        strs.dedup();
        assert_eq!(strs.len(), 18);
    }

    /// Transcribed verbatim from `reason_codes.py`'s `_CODES` tuple.
    #[test]
    fn layer_and_kind_match_python_catalog() {
        let expected: &[(&str, &str, &str)] = &[
            ("BUDGET_EXHAUSTED", "governor", "rejection"),
            ("AUTHORITY_LEVEL_BLOCKED", "governor", "rejection"),
            ("PAUSED", "governor", "rejection"),
            ("INTENT_STALE", "arbiter", "deferral"),
            ("INTENT_SUPERSEDED", "arbiter", "rejection"),
            ("CHANNEL_PROTECTED", "governor", "rejection"),
            ("CONTRACT_OBLIGATION", "arbiter", "rejection"),
            ("EV_BELOW_HOLD_MARGIN", "policy", "hold"),
            ("INSUFFICIENT_CONFIDENCE", "governor", "hold"),
            ("FEE_RAIL_CLAMPED", "policy", "clamp"),
            ("COOLDOWN_ACTIVE", "policy", "hold"),
            ("CONFLICT_CLOSE_REBALANCE", "arbiter", "rejection"),
            ("CONFLICT_DUPLICATE_OPEN", "arbiter", "rejection"),
            ("CONFLICT_REBALANCE_SWAP", "arbiter", "rejection"),
            ("EXTERNAL_CIRCUIT_BREAKER", "executor", "deferral"),
            ("EXTERNAL_OUTCOME_UNKNOWN", "reconciliation", "unknown"),
            ("ARITHMETIC_OVERFLOW", "any", "failure"),
            ("SCHEMA_INVALID", "any", "failure"),
        ];
        assert_eq!(expected.len(), 18);
        for (code, layer, kind) in expected {
            let found = ALL
                .iter()
                .find(|c| c.as_str() == *code)
                .unwrap_or_else(|| panic!("code {code} missing from ALL"));
            assert_eq!(found.layer(), *layer, "layer mismatch for {code}");
            assert_eq!(found.kind(), *kind, "kind mismatch for {code}");
        }
    }

    #[test]
    fn is_valid_code_accepts_known_and_rejects_unknown() {
        assert!(is_valid_code("BUDGET_EXHAUSTED"));
        assert!(is_valid_code("SCHEMA_INVALID"));
        assert!(!is_valid_code("NOT_A_CODE"));
        assert!(!is_valid_code(""));
        assert!(!is_valid_code("budget_exhausted"));
    }
}
