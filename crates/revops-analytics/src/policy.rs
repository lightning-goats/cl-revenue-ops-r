//! Policy core: `FeeStrategy`, `RebalanceMode`, `PeerPolicy`, and the
//! `set_policy` VALIDATION core — a pure port of `modules/policy_manager.py`
//! (cl_revenue_ops-port).
//!
//! Only the pure decision surface ports here: given an existing
//! [`PeerPolicy`] snapshot and a proposed [`PolicyUpdate`], produce either a
//! validated new `PeerPolicy` or one of the frozen `ValueError` strings from
//! the Python original. Rate limiting (`_check_rate_limit` /
//! `_record_rate_limit_change`), the SQLite write (`database.upsert_policy`),
//! the write-through cache, and change-callback notification are all
//! plugin-orchestration concerns that stay with the future Phase 3b wiring
//! layer — this module has no clock, no DB, no I/O; `now` is always an
//! injected value, never `time.time()`.

use revops_econ::pyfloat::py_repr;

/// Per-policy fee multiplier bounds (security limits), verbatim from
/// `policy_manager.py` lines 38-39.
pub const GLOBAL_MIN_FEE_MULTIPLIER: f64 = 0.1;
pub const GLOBAL_MAX_FEE_MULTIPLIER: f64 = 5.0;

/// Maximum time-limited policy expiry, in days (line 42).
pub const MAX_POLICY_EXPIRY_DAYS: i64 = 30;

/// Fee control strategy for a peer (`policy_manager.py` `FeeStrategy`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FeeStrategy {
    /// DTS+PID fee optimization (default).
    Dynamic,
    /// Fixed fee (user override).
    Static,
    /// Do nothing (manual control).
    Passive,
}

impl FeeStrategy {
    /// All variants, in Python `Enum` declaration order — the order that
    /// backs `[s.value for s in FeeStrategy]` in the frozen "Invalid
    /// strategy" error message.
    pub const ALL: [FeeStrategy; 3] = [
        FeeStrategy::Dynamic,
        FeeStrategy::Static,
        FeeStrategy::Passive,
    ];

    /// Python `.value` (wire/DB representation).
    pub fn as_value(&self) -> &'static str {
        match self {
            FeeStrategy::Dynamic => "dynamic",
            FeeStrategy::Static => "static",
            FeeStrategy::Passive => "passive",
        }
    }

    /// Python `.name` (enum member name).
    pub fn as_name(&self) -> &'static str {
        match self {
            FeeStrategy::Dynamic => "DYNAMIC",
            FeeStrategy::Static => "STATIC",
            FeeStrategy::Passive => "PASSIVE",
        }
    }

    /// `FeeStrategy(value)` — case-sensitive; callers lowercase first
    /// (mirrors `FeeStrategy(strategy.lower())`).
    pub fn from_value(value: &str) -> Option<FeeStrategy> {
        match value {
            "dynamic" => Some(FeeStrategy::Dynamic),
            "static" => Some(FeeStrategy::Static),
            "passive" => Some(FeeStrategy::Passive),
            _ => None,
        }
    }
}

/// Rebalancing behavior for a peer (`policy_manager.py` `RebalanceMode`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RebalanceMode {
    /// Full rebalancing allowed.
    Enabled,
    /// No rebalancing.
    Disabled,
    /// Can drain, cannot fill.
    SourceOnly,
    /// Can fill, cannot drain.
    SinkOnly,
}

impl RebalanceMode {
    /// All variants, in Python `Enum` declaration order.
    pub const ALL: [RebalanceMode; 4] = [
        RebalanceMode::Enabled,
        RebalanceMode::Disabled,
        RebalanceMode::SourceOnly,
        RebalanceMode::SinkOnly,
    ];

    pub fn as_value(&self) -> &'static str {
        match self {
            RebalanceMode::Enabled => "enabled",
            RebalanceMode::Disabled => "disabled",
            RebalanceMode::SourceOnly => "source_only",
            RebalanceMode::SinkOnly => "sink_only",
        }
    }

    pub fn as_name(&self) -> &'static str {
        match self {
            RebalanceMode::Enabled => "ENABLED",
            RebalanceMode::Disabled => "DISABLED",
            RebalanceMode::SourceOnly => "SOURCE_ONLY",
            RebalanceMode::SinkOnly => "SINK_ONLY",
        }
    }

    pub fn from_value(value: &str) -> Option<RebalanceMode> {
        match value {
            "enabled" => Some(RebalanceMode::Enabled),
            "disabled" => Some(RebalanceMode::Disabled),
            "source_only" => Some(RebalanceMode::SourceOnly),
            "sink_only" => Some(RebalanceMode::SinkOnly),
            _ => None,
        }
    }
}

/// Immutable policy snapshot for a peer (`policy_manager.py` `PeerPolicy`,
/// fields lines 95-104).
#[derive(Debug, Clone, PartialEq)]
pub struct PeerPolicy {
    /// 66-character hex public key.
    pub peer_id: String,
    pub strategy: FeeStrategy,
    pub rebalance_mode: RebalanceMode,
    /// Target fee for static strategy.
    pub fee_ppm_target: Option<i64>,
    /// Tags for grouping/filtering.
    pub tags: Vec<String>,
    /// Unix timestamp of last update.
    pub updated_at: i64,
    /// v2.0: override minimum flow multiplier for this peer.
    pub fee_multiplier_min: Option<f64>,
    /// v2.0: override maximum flow multiplier for this peer.
    pub fee_multiplier_max: Option<f64>,
    /// Unix timestamp when policy auto-reverts (`None` = permanent).
    pub expires_at: Option<i64>,
}

impl PeerPolicy {
    /// Default policy for peers without explicit configuration: dynamic
    /// strategy, rebalancing enabled, no tags, never expires.
    pub fn default_for(peer_id: impl Into<String>) -> PeerPolicy {
        PeerPolicy {
            peer_id: peer_id.into(),
            strategy: FeeStrategy::Dynamic,
            rebalance_mode: RebalanceMode::Enabled,
            fee_ppm_target: None,
            tags: Vec::new(),
            updated_at: 0,
            fee_multiplier_min: None,
            fee_multiplier_max: None,
            expires_at: None,
        }
    }

    /// `PeerPolicy.has_tag`.
    pub fn has_tag(&self, tag: &str) -> bool {
        self.tags.iter().any(|t| t == tag)
    }

    /// `PeerPolicy.is_expired`. `ENABLE_AUTO_EXPIRY` is a Python module
    /// constant fixed at `True` in the original, so it never actually
    /// changes this: `expires_at is None` -> false; else strict
    /// `now > expires_at`.
    pub fn is_expired(&self, now: i64) -> bool {
        match self.expires_at {
            None => false,
            Some(expires_at) => now > expires_at,
        }
    }

    /// `PeerPolicy.get_fee_multiplier_bounds`: per-peer override with
    /// global-limit clamp, then swap-if-inverted.
    pub fn fee_multiplier_bounds(&self) -> (f64, f64) {
        let mut min_mult = self.fee_multiplier_min.unwrap_or(GLOBAL_MIN_FEE_MULTIPLIER);
        let mut max_mult = self.fee_multiplier_max.unwrap_or(GLOBAL_MAX_FEE_MULTIPLIER);

        // Enforce global security bounds: max(MIN, min(x, MAX)).
        min_mult = min_mult.clamp(GLOBAL_MIN_FEE_MULTIPLIER, GLOBAL_MAX_FEE_MULTIPLIER);
        max_mult = max_mult.clamp(GLOBAL_MIN_FEE_MULTIPLIER, GLOBAL_MAX_FEE_MULTIPLIER);

        // Ensure min <= max.
        if min_mult > max_mult {
            std::mem::swap(&mut min_mult, &mut max_mult);
        }

        (min_mult, max_mult)
    }
}

/// Peer-id validation pattern, ported from `policy_manager.py`'s
/// `PEER_ID_PATTERN = re.compile(r'\A[0-9a-fA-F]{66}\Z')` (PM-I1: anchored
/// with `\A...\Z`, not `^...$` — Python's `$` matches before a trailing
/// newline, which historically let a 67-char `...\n` peer_id through).
///
/// No `regex` crate is available in this workspace (Global Constraints:
/// "no new external crates"), so this is a manual character scan. That
/// scan is *equivalent* to the anchored-with-`\A...\Z` regex (not the
/// buggy `^...$` one): requiring `s.len() == 66` already rejects the
/// 67-char trailing-newline case that the `$`-anchored bug let through, so
/// this reproduces the FIXED behavior, not the historical bug.
pub fn is_valid_peer_id(s: &str) -> bool {
    s.len() == 66 && s.bytes().all(|b| b.is_ascii_hexdigit())
}

/// Frozen `ValueError` strings from `PolicyManager.set_policy`
/// (`policy_manager.py` lines 557-720). `Display` renders the EXACT
/// message text; wording changes are a conformance failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct PolicyError(pub String);

impl PolicyError {
    fn new(msg: impl Into<String>) -> PolicyError {
        PolicyError(msg.into())
    }
}

/// Proposed policy changes for [`validate_policy_update`] — the pure-ified
/// keyword arguments of `PolicyManager.set_policy`. Only provided
/// (`Some`) fields are changed; the rest retain the existing policy's
/// values. `tags`, when present, REPLACES the existing tag list entirely
/// (mirrors Python: `new_tags = tags if tags is not None else
/// existing.tags`).
#[derive(Debug, Clone, Default)]
pub struct PolicyUpdate {
    /// Case-insensitive: lowercased before matching against
    /// [`FeeStrategy::from_value`] (Python `FeeStrategy(strategy.lower())`).
    pub strategy: Option<String>,
    /// Case-insensitive, same lowering rule as `strategy`.
    pub rebalance_mode: Option<String>,
    pub fee_ppm_target: Option<i64>,
    pub tags: Option<Vec<String>>,
    pub fee_multiplier_min: Option<f64>,
    pub fee_multiplier_max: Option<f64>,
    /// Hours until policy auto-reverts. `Some(0)` or negative clears any
    /// existing expiry (`None` result); `None` here means "leave
    /// `expires_at` untouched".
    pub expires_in_hours: Option<i64>,
}

/// The `set_policy` VALIDATION core: existing policy + proposed update ->
/// validated new [`PeerPolicy`], or the first frozen error string
/// encountered (Python's validation order is preserved exactly). Rate
/// limiting, the DB write, and cache/callback notification are NOT part of
/// this function — those are the future wiring layer's job; this only
/// validates and produces the value that would be persisted.
///
/// `now` stands in for Python's `int(time.time())` calls inside
/// `set_policy` (both the `updated_at` stamp and the `expires_in_hours`
/// base) — always injected, never read from a clock.
pub fn validate_policy_update(
    existing: &PeerPolicy,
    update: &PolicyUpdate,
    now: i64,
) -> Result<PeerPolicy, PolicyError> {
    // --- strategy ---
    let new_strategy = match &update.strategy {
        None => existing.strategy,
        Some(strategy) => FeeStrategy::from_value(&strategy.to_lowercase()).ok_or_else(|| {
            let valid: Vec<&str> = FeeStrategy::ALL.iter().map(FeeStrategy::as_value).collect();
            PolicyError::new(format!(
                "Invalid strategy '{strategy}'. Valid: {}",
                py_list_repr(&valid)
            ))
        })?,
    };

    // --- rebalance_mode ---
    let new_rebalance_mode = match &update.rebalance_mode {
        None => existing.rebalance_mode,
        Some(mode) => RebalanceMode::from_value(&mode.to_lowercase()).ok_or_else(|| {
            let valid: Vec<&str> = RebalanceMode::ALL
                .iter()
                .map(RebalanceMode::as_value)
                .collect();
            PolicyError::new(format!(
                "Invalid rebalance_mode '{mode}'. Valid: {}",
                py_list_repr(&valid)
            ))
        })?,
    };

    // --- fee_ppm_target ---
    let new_fee_ppm = update.fee_ppm_target.or(existing.fee_ppm_target);
    if let Some(fee_ppm) = new_fee_ppm {
        if fee_ppm < 0 {
            return Err(PolicyError::new(format!(
                "fee_ppm_target must be a non-negative integer, got {fee_ppm}"
            )));
        }
        if fee_ppm > 100_000 {
            return Err(PolicyError::new("fee_ppm_target cannot exceed 100000 PPM"));
        }
    }
    if new_strategy == FeeStrategy::Static && new_fee_ppm.is_none() {
        // Without a target the fee controller silently falls through to
        // dynamic management — the opposite of what 'static' promises.
        return Err(PolicyError::new("strategy=static requires fee_ppm_target"));
    }

    // --- tags (replace-if-present; Rust's `Vec<String>` already
    // guarantees Python's "must be a list of strings" type check) ---
    let new_tags = match &update.tags {
        Some(tags) => tags.clone(),
        None => existing.tags.clone(),
    };

    // --- v2.0 fee multiplier bounds ---
    let new_mult_min = update.fee_multiplier_min.or(existing.fee_multiplier_min);
    let new_mult_max = update.fee_multiplier_max.or(existing.fee_multiplier_max);

    if let Some(mult_min) = new_mult_min {
        if mult_min < GLOBAL_MIN_FEE_MULTIPLIER {
            return Err(PolicyError::new(format!(
                "fee_multiplier_min must be >= {}",
                py_repr(GLOBAL_MIN_FEE_MULTIPLIER)
            )));
        }
        if mult_min > GLOBAL_MAX_FEE_MULTIPLIER {
            return Err(PolicyError::new(format!(
                "fee_multiplier_min must be <= {}",
                py_repr(GLOBAL_MAX_FEE_MULTIPLIER)
            )));
        }
    }
    if let Some(mult_max) = new_mult_max {
        if mult_max < GLOBAL_MIN_FEE_MULTIPLIER {
            return Err(PolicyError::new(format!(
                "fee_multiplier_max must be >= {}",
                py_repr(GLOBAL_MIN_FEE_MULTIPLIER)
            )));
        }
        if mult_max > GLOBAL_MAX_FEE_MULTIPLIER {
            return Err(PolicyError::new(format!(
                "fee_multiplier_max must be <= {}",
                py_repr(GLOBAL_MAX_FEE_MULTIPLIER)
            )));
        }
    }
    // L-R5-8 FIX: cross-validate multiplier min/max instead of silently
    // storing inverted bounds that get swapped at read time.
    if let (Some(mult_min), Some(mult_max)) = (new_mult_min, new_mult_max) {
        if mult_min > mult_max {
            return Err(PolicyError::new(format!(
                "fee_multiplier_min ({}) cannot exceed fee_multiplier_max ({})",
                py_repr(mult_min),
                py_repr(mult_max)
            )));
        }
    }

    // --- v2.0 expiry ---
    let new_expires_at = match update.expires_in_hours {
        None => existing.expires_at,
        Some(hours) if hours <= 0 => None,
        Some(hours) => {
            let max_hours = MAX_POLICY_EXPIRY_DAYS * 24;
            if hours > max_hours {
                return Err(PolicyError::new(format!(
                    "expires_in_hours cannot exceed {max_hours} ({MAX_POLICY_EXPIRY_DAYS} days)"
                )));
            }
            Some(now + hours * 3600)
        }
    };

    Ok(PeerPolicy {
        peer_id: existing.peer_id.clone(),
        strategy: new_strategy,
        rebalance_mode: new_rebalance_mode,
        fee_ppm_target: new_fee_ppm,
        tags: new_tags,
        updated_at: now,
        fee_multiplier_min: new_mult_min,
        fee_multiplier_max: new_mult_max,
        expires_at: new_expires_at,
    })
}

/// Python `str(list_of_str)` — `['dynamic', 'static', 'passive']`, single
/// quotes, `, ` separator. Used only to reproduce the "Valid: {valid}"
/// tail of the frozen strategy/rebalance_mode error messages.
fn py_list_repr(items: &[&str]) -> String {
    let inner = items
        .iter()
        .map(|s| format!("'{s}'"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("[{inner}]")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fee_strategy_values_and_names() {
        assert_eq!(FeeStrategy::Dynamic.as_value(), "dynamic");
        assert_eq!(FeeStrategy::Static.as_value(), "static");
        assert_eq!(FeeStrategy::Passive.as_value(), "passive");
        assert_eq!(FeeStrategy::Dynamic.as_name(), "DYNAMIC");
        assert_eq!(FeeStrategy::Static.as_name(), "STATIC");
        assert_eq!(FeeStrategy::Passive.as_name(), "PASSIVE");
    }

    #[test]
    fn rebalance_mode_values_and_names() {
        assert_eq!(RebalanceMode::Enabled.as_value(), "enabled");
        assert_eq!(RebalanceMode::Disabled.as_value(), "disabled");
        assert_eq!(RebalanceMode::SourceOnly.as_value(), "source_only");
        assert_eq!(RebalanceMode::SinkOnly.as_value(), "sink_only");
        assert_eq!(RebalanceMode::Enabled.as_name(), "ENABLED");
        assert_eq!(RebalanceMode::SourceOnly.as_name(), "SOURCE_ONLY");
    }

    #[test]
    fn has_tag_checks_membership() {
        let mut p = PeerPolicy::default_for("peer");
        p.tags = vec!["protect".to_string(), "no_close".to_string()];
        assert!(p.has_tag("protect"));
        assert!(!p.has_tag("banned"));
    }

    #[test]
    fn is_expired_none_expiry_never_expires() {
        let p = PeerPolicy::default_for("peer");
        assert!(!p.is_expired(9_999_999_999));
    }

    #[test]
    fn is_expired_strict_greater_than() {
        let mut p = PeerPolicy::default_for("peer");
        p.expires_at = Some(1000);
        assert!(
            !p.is_expired(1000),
            "now == expires_at must NOT be expired (strict >)"
        );
        assert!(p.is_expired(1001));
        assert!(!p.is_expired(999));
    }

    #[test]
    fn fee_multiplier_bounds_default_is_global() {
        let p = PeerPolicy::default_for("peer");
        assert_eq!(
            p.fee_multiplier_bounds(),
            (GLOBAL_MIN_FEE_MULTIPLIER, GLOBAL_MAX_FEE_MULTIPLIER)
        );
    }

    #[test]
    fn fee_multiplier_bounds_clamped_to_global_limits() {
        let mut p = PeerPolicy::default_for("peer");
        p.fee_multiplier_min = Some(0.0); // below global min
        p.fee_multiplier_max = Some(10.0); // above global max
        assert_eq!(
            p.fee_multiplier_bounds(),
            (GLOBAL_MIN_FEE_MULTIPLIER, GLOBAL_MAX_FEE_MULTIPLIER)
        );
    }

    #[test]
    fn fee_multiplier_bounds_swapped_if_inverted() {
        let mut p = PeerPolicy::default_for("peer");
        p.fee_multiplier_min = Some(3.0);
        p.fee_multiplier_max = Some(1.0);
        assert_eq!(p.fee_multiplier_bounds(), (1.0, 3.0));
    }

    // --- is_valid_peer_id ---

    const VALID_ID: &str = "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    #[test]
    fn peer_id_valid_66_hex() {
        assert_eq!(VALID_ID.len(), 66);
        assert!(is_valid_peer_id(VALID_ID));
    }

    #[test]
    fn peer_id_rejects_trailing_newline() {
        // PM-I1: 66 hex chars + '\n' is 67 bytes — must be rejected, not
        // waved through by a Python-`$`-style "matches before trailing
        // newline" bug.
        let with_newline = format!("{VALID_ID}\n");
        assert!(!is_valid_peer_id(&with_newline));
    }

    #[test]
    fn peer_id_rejects_wrong_length_and_non_hex() {
        assert!(!is_valid_peer_id(""));
        assert!(!is_valid_peer_id(&VALID_ID[..65]));
        assert!(!is_valid_peer_id(&format!("{VALID_ID}a")));
        let mut bad = VALID_ID.to_string();
        bad.replace_range(0..1, "g");
        assert!(!is_valid_peer_id(&bad));
    }

    #[test]
    fn peer_id_accepts_uppercase_hex() {
        let upper = VALID_ID.to_uppercase();
        assert!(is_valid_peer_id(&upper));
    }

    // --- validate_policy_update ---

    fn existing() -> PeerPolicy {
        PeerPolicy::default_for(VALID_ID)
    }

    #[test]
    fn invalid_strategy_message_frozen() {
        let update = PolicyUpdate {
            strategy: Some("bogus".to_string()),
            ..Default::default()
        };
        let err = validate_policy_update(&existing(), &update, 1000).unwrap_err();
        assert_eq!(
            err.0,
            "Invalid strategy 'bogus'. Valid: ['dynamic', 'static', 'passive']"
        );
    }

    #[test]
    fn invalid_rebalance_mode_message_frozen() {
        let update = PolicyUpdate {
            rebalance_mode: Some("bogus".to_string()),
            ..Default::default()
        };
        let err = validate_policy_update(&existing(), &update, 1000).unwrap_err();
        assert_eq!(
            err.0,
            "Invalid rebalance_mode 'bogus'. Valid: ['enabled', 'disabled', 'source_only', 'sink_only']"
        );
    }

    #[test]
    fn strategy_lowercased_before_match_but_message_keeps_original_case() {
        let update = PolicyUpdate {
            strategy: Some("DYNAMIC".to_string()),
            fee_ppm_target: None,
            ..Default::default()
        };
        let out = validate_policy_update(&existing(), &update, 1000).unwrap();
        assert_eq!(out.strategy, FeeStrategy::Dynamic);

        // Same casing rule applies to the invalid-input error: message
        // must echo the ORIGINAL string, not the lowercased one (Python:
        // `f"Invalid strategy '{strategy}'..."` uses the un-lowered var).
        let bad = PolicyUpdate {
            strategy: Some("BOGUS".to_string()),
            ..Default::default()
        };
        let err = validate_policy_update(&existing(), &bad, 1000).unwrap_err();
        assert!(
            err.0.starts_with("Invalid strategy 'BOGUS'."),
            "got: {}",
            err.0
        );
    }

    #[test]
    fn static_strategy_requires_fee_ppm_target() {
        let update = PolicyUpdate {
            strategy: Some("static".to_string()),
            ..Default::default()
        };
        let err = validate_policy_update(&existing(), &update, 1000).unwrap_err();
        assert_eq!(err.0, "strategy=static requires fee_ppm_target");
    }

    #[test]
    fn static_strategy_with_target_succeeds() {
        let update = PolicyUpdate {
            strategy: Some("static".to_string()),
            fee_ppm_target: Some(500),
            ..Default::default()
        };
        let out = validate_policy_update(&existing(), &update, 1000).unwrap();
        assert_eq!(out.strategy, FeeStrategy::Static);
        assert_eq!(out.fee_ppm_target, Some(500));
    }

    #[test]
    fn fee_ppm_target_negative_rejected() {
        let update = PolicyUpdate {
            fee_ppm_target: Some(-1),
            ..Default::default()
        };
        let err = validate_policy_update(&existing(), &update, 1000).unwrap_err();
        assert_eq!(
            err.0,
            "fee_ppm_target must be a non-negative integer, got -1"
        );
    }

    #[test]
    fn fee_ppm_target_over_cap_rejected() {
        let update = PolicyUpdate {
            fee_ppm_target: Some(100_001),
            ..Default::default()
        };
        let err = validate_policy_update(&existing(), &update, 1000).unwrap_err();
        assert_eq!(err.0, "fee_ppm_target cannot exceed 100000 PPM");
    }

    #[test]
    fn fee_ppm_target_at_cap_accepted() {
        let update = PolicyUpdate {
            fee_ppm_target: Some(100_000),
            ..Default::default()
        };
        let out = validate_policy_update(&existing(), &update, 1000).unwrap();
        assert_eq!(out.fee_ppm_target, Some(100_000));
    }

    #[test]
    fn tags_replace_existing_when_present() {
        let mut base = existing();
        base.tags = vec!["old".to_string()];
        let update = PolicyUpdate {
            tags: Some(vec!["new".to_string()]),
            ..Default::default()
        };
        let out = validate_policy_update(&base, &update, 1000).unwrap();
        assert_eq!(out.tags, vec!["new".to_string()]);
    }

    #[test]
    fn tags_retained_when_absent() {
        let mut base = existing();
        base.tags = vec!["kept".to_string()];
        let out = validate_policy_update(&base, &PolicyUpdate::default(), 1000).unwrap();
        assert_eq!(out.tags, vec!["kept".to_string()]);
    }

    #[test]
    fn fee_multiplier_min_below_global_rejected() {
        let update = PolicyUpdate {
            fee_multiplier_min: Some(0.05),
            ..Default::default()
        };
        let err = validate_policy_update(&existing(), &update, 1000).unwrap_err();
        assert_eq!(err.0, "fee_multiplier_min must be >= 0.1");
    }

    #[test]
    fn fee_multiplier_min_above_global_rejected() {
        let update = PolicyUpdate {
            fee_multiplier_min: Some(5.5),
            ..Default::default()
        };
        let err = validate_policy_update(&existing(), &update, 1000).unwrap_err();
        assert_eq!(err.0, "fee_multiplier_min must be <= 5.0");
    }

    #[test]
    fn fee_multiplier_max_below_global_rejected() {
        let update = PolicyUpdate {
            fee_multiplier_max: Some(0.05),
            ..Default::default()
        };
        let err = validate_policy_update(&existing(), &update, 1000).unwrap_err();
        assert_eq!(err.0, "fee_multiplier_max must be >= 0.1");
    }

    #[test]
    fn fee_multiplier_max_above_global_rejected() {
        let update = PolicyUpdate {
            fee_multiplier_max: Some(5.5),
            ..Default::default()
        };
        let err = validate_policy_update(&existing(), &update, 1000).unwrap_err();
        assert_eq!(err.0, "fee_multiplier_max must be <= 5.0");
    }

    #[test]
    fn l_r5_8_min_exceeds_max_rejected() {
        let update = PolicyUpdate {
            fee_multiplier_min: Some(4.0),
            fee_multiplier_max: Some(2.0),
            ..Default::default()
        };
        let err = validate_policy_update(&existing(), &update, 1000).unwrap_err();
        assert_eq!(
            err.0,
            "fee_multiplier_min (4.0) cannot exceed fee_multiplier_max (2.0)"
        );
    }

    #[test]
    fn expires_in_hours_zero_or_negative_clears_expiry() {
        let mut base = existing();
        base.expires_at = Some(5000);
        let update = PolicyUpdate {
            expires_in_hours: Some(0),
            ..Default::default()
        };
        let out = validate_policy_update(&base, &update, 1000).unwrap();
        assert_eq!(out.expires_at, None);

        let update_neg = PolicyUpdate {
            expires_in_hours: Some(-5),
            ..Default::default()
        };
        let out_neg = validate_policy_update(&base, &update_neg, 1000).unwrap();
        assert_eq!(out_neg.expires_at, None);
    }

    #[test]
    fn expires_in_hours_computes_from_injected_now() {
        let update = PolicyUpdate {
            expires_in_hours: Some(2),
            ..Default::default()
        };
        let out = validate_policy_update(&existing(), &update, 1_000_000).unwrap();
        assert_eq!(out.expires_at, Some(1_000_000 + 2 * 3600));
    }

    #[test]
    fn expires_in_hours_over_max_rejected() {
        let update = PolicyUpdate {
            expires_in_hours: Some(30 * 24 + 1),
            ..Default::default()
        };
        let err = validate_policy_update(&existing(), &update, 1000).unwrap_err();
        assert_eq!(err.0, "expires_in_hours cannot exceed 720 (30 days)");
    }

    #[test]
    fn expires_in_hours_at_max_accepted() {
        let update = PolicyUpdate {
            expires_in_hours: Some(30 * 24),
            ..Default::default()
        };
        let out = validate_policy_update(&existing(), &update, 1000).unwrap();
        assert_eq!(out.expires_at, Some(1000 + 720 * 3600));
    }

    #[test]
    fn updated_at_stamped_from_injected_now() {
        let out = validate_policy_update(&existing(), &PolicyUpdate::default(), 42).unwrap();
        assert_eq!(out.updated_at, 42);
    }

    #[test]
    fn unset_fields_retain_existing_values() {
        let mut base = existing();
        base.strategy = FeeStrategy::Static;
        base.fee_ppm_target = Some(777);
        base.rebalance_mode = RebalanceMode::SourceOnly;
        let out = validate_policy_update(&base, &PolicyUpdate::default(), 1000).unwrap();
        assert_eq!(out.strategy, FeeStrategy::Static);
        assert_eq!(out.fee_ppm_target, Some(777));
        assert_eq!(out.rebalance_mode, RebalanceMode::SourceOnly);
    }
}
