//! Python-parity profile preview helpers.

pub use crate::rpc_state_mutators::{build_profile_preview, profile_bundles};

/// Freeze the validated risk profile that Python applies during startup.
/// Invalid persisted enum values are skipped, preserving the default.
pub fn startup_active_profile(persisted: Result<Option<String>, String>) -> Result<String, String> {
    persisted.map(|value| {
        value
            .and_then(|raw| crate::config_resolve::validate_override("risk_profile", raw.trim()))
            .unwrap_or_else(|| "custom".to_string())
    })
}

/// Apply the startup profile below explicit database overrides.
pub fn apply_active_profile(
    current: &mut serde_json::Map<String, serde_json::Value>,
    active_profile: &str,
    explicit_keys: &std::collections::BTreeSet<String>,
) {
    let bundles = profile_bundles();
    let Some(bundle) = bundles.get(active_profile) else {
        return;
    };
    for (key, value) in bundle {
        if !explicit_keys.contains(key) {
            current.insert(key.clone(), value.clone());
        }
    }
}
