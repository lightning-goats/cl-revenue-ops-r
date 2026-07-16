//! Fee profiles: `FEE_PROFILES` tables + `EXPLORATION_*` constants, ported
//! verbatim from `modules/fee_controller.py:2502-2552` and the profile
//! resolution in `_resolve_fee_profile`/`get_fee_profile_settings`
//! (py 3049-3068).
//!
//! Every constant here is load-bearing (Phase 4 Global Constraints):
//! transcribed verbatim from the Python class body, pinned by
//! `fixtures/fees/rails/profiles.json` (generated from the real
//! `FeeProfileSettings.to_dict()`).

/// Runtime aggressiveness knobs for the fee controller (py `FeeProfileSettings`,
/// a frozen dataclass at py 2439-2467).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FeeProfileSettings {
    pub min_observation_hours: f64,
    pub min_forwards_for_signal: i64,
    pub dts_discount_gamma: f64,
    pub dts_sparse_discount_gamma: f64,
    pub normal_target_blend_ratio: f64,
    pub wake_target_blend_ratio: f64,
    pub sparse_target_blend_ratio: f64,
    pub normal_cycle_max_delta_ratio: f64,
    pub normal_cycle_min_delta_ppm: i64,
    pub wake_cycle_max_delta_ratio: f64,
    pub wake_cycle_min_delta_ppm: i64,
}

/// `FEE_PROFILES["active"]` (py 2502-2534): built from the module-level
/// constants `MIN_OBSERVATION_HOURS=0.25`, `MIN_FORWARDS_FOR_SIGNAL=3`,
/// `DTS_DISCOUNT_GAMMA=0.98`, `DTS_SPARSE_DISCOUNT_GAMMA=0.992`,
/// `NORMAL_TARGET_BLEND_RATIO=0.35`, `WAKE_TARGET_BLEND_RATIO=0.15`,
/// `SPARSE_TARGET_BLEND_RATIO=0.20`, `NORMAL_CYCLE_MAX_DELTA_RATIO=0.50`,
/// `NORMAL_CYCLE_MIN_DELTA_PPM=100`, `WAKE_CYCLE_MAX_DELTA_RATIO=0.20`,
/// `WAKE_CYCLE_MIN_DELTA_PPM=50`.
pub static ACTIVE_PROFILE: FeeProfileSettings = FeeProfileSettings {
    min_observation_hours: 0.25,
    min_forwards_for_signal: 3,
    dts_discount_gamma: 0.98,
    dts_sparse_discount_gamma: 0.992,
    normal_target_blend_ratio: 0.35,
    wake_target_blend_ratio: 0.15,
    sparse_target_blend_ratio: 0.20,
    normal_cycle_max_delta_ratio: 0.50,
    normal_cycle_min_delta_ppm: 100,
    wake_cycle_max_delta_ratio: 0.20,
    wake_cycle_min_delta_ppm: 50,
};

/// `FEE_PROFILES["conservative"]` (py 2535-2547), verbatim literal table.
pub static CONSERVATIVE_PROFILE: FeeProfileSettings = FeeProfileSettings {
    min_observation_hours: 1.0,
    min_forwards_for_signal: 6,
    dts_discount_gamma: 0.992,
    dts_sparse_discount_gamma: 0.996,
    normal_target_blend_ratio: 0.20,
    wake_target_blend_ratio: 0.10,
    sparse_target_blend_ratio: 0.10,
    normal_cycle_max_delta_ratio: 0.25,
    normal_cycle_min_delta_ppm: 25,
    wake_cycle_max_delta_ratio: 0.10,
    wake_cycle_min_delta_ppm: 10,
};

/// `EXPLORATION_FEE_MULTIPLIER` (py 2549).
pub const EXPLORATION_FEE_MULTIPLIER: f64 = 1.25;
/// `EXPLORATION_MAX_DISCOUNT_RATIO` (py 2550).
pub const EXPLORATION_MAX_DISCOUNT_RATIO: f64 = 0.50;
/// `EXPLORATION_HEADROOM_RATIO` (py 2551).
pub const EXPLORATION_HEADROOM_RATIO: f64 = 0.35;
/// `EXPLORATION_SPARSE_HEADROOM_RATIO` (py 2552).
pub const EXPLORATION_SPARSE_HEADROOM_RATIO: f64 = 0.50;

/// Resolve a fee-profile name to its canonical name + settings table
/// (`_resolve_fee_profile`, py 3056-3065). Python lower-cases the raw
/// config value before the membership check; anything not in
/// `FEE_PROFILES` (including the empty string) falls back to `"active"`.
pub fn fee_profile(name: &str) -> (&'static str, &'static FeeProfileSettings) {
    match name.to_lowercase().as_str() {
        "conservative" => ("conservative", &CONSERVATIVE_PROFILE),
        _ => ("active", &ACTIVE_PROFILE),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_name_falls_back_to_active() {
        let (name, settings) = fee_profile("bogus");
        assert_eq!(name, "active");
        assert_eq!(*settings, ACTIVE_PROFILE);
    }

    #[test]
    fn resolution_is_case_insensitive() {
        assert_eq!(fee_profile("CONSERVATIVE").0, "conservative");
        assert_eq!(fee_profile("Active").0, "active");
    }

    #[test]
    fn empty_name_falls_back_to_active() {
        assert_eq!(fee_profile("").0, "active");
    }
}
