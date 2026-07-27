//! Pure wallet-selection logic over an already-fetched `boltzcli wallet
//! list --json` payload.
//!
//! Ports py `_resolve_wallet`/`_resolve_wallet_name` (boltz_manager.py:
//! 559-585), minus the `wallet list` subprocess call itself (I/O — the live
//! adapter's job, per `ENTRYPOINTS.md`'s "argv-construction glue is not
//! decision logic" split). Selection order, matching Python exactly:
//! 1. an explicit wallet name, matched verbatim (no currency filter);
//! 2. a configured preferred name for the currency, if it exists, is
//!    writable-currency-matched;
//! 3. the first writable (non-readonly) wallet whose `currency` matches.

use serde_json::Value;

/// py `_resolve_wallet` (boltz_manager.py:559-579). `wallets` is the
/// `"wallets"` array from `wallet list --json`. `currency` should already be
/// normalized (see `argv::normalize_currency`).
pub fn resolve_wallet<'a>(
    wallets: &'a [Value],
    currency: &str,
    explicit_name: Option<&str>,
    preferred_name: Option<&str>,
) -> Option<&'a Value> {
    if let Some(name) = explicit_name {
        return wallets.iter().find(|w| wallet_name(w) == Some(name));
    }
    if let Some(preferred) = preferred_name {
        if let Some(w) = wallets.iter().find(|w| {
            wallet_name(w) == Some(preferred) && wallet_currency(w).as_deref() == Some(currency)
        }) {
            return Some(w);
        }
    }
    wallets
        .iter()
        .find(|w| wallet_currency(w).as_deref() == Some(currency) && !wallet_readonly(w))
}

/// py `_resolve_wallet_name` (boltz_manager.py:581-585): the resolved
/// wallet's `name` field, or `None` when no writable wallet matches (py
/// raises `BoltzCliError("No writable {currency} wallet found in
/// boltzd")` — the live adapter's job to turn `None` into that error).
pub fn resolve_wallet_name(
    wallets: &[Value],
    currency: &str,
    explicit_name: Option<&str>,
    preferred_name: Option<&str>,
) -> Option<String> {
    resolve_wallet(wallets, currency, explicit_name, preferred_name)
        .and_then(wallet_name)
        .map(|s| s.to_string())
}

fn wallet_name(w: &Value) -> Option<&str> {
    w.get("name").and_then(|v| v.as_str())
}

fn wallet_currency(w: &Value) -> Option<String> {
    w.get("currency")
        .and_then(|v| v.as_str())
        .map(|s| s.to_uppercase())
}

fn wallet_readonly(w: &Value) -> bool {
    w.get("readonly").and_then(|v| v.as_bool()).unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn wallets() -> Vec<Value> {
        vec![
            json!({"name": "main-btc", "currency": "BTC", "readonly": false}),
            json!({"name": "readonly-btc", "currency": "BTC", "readonly": true}),
            json!({"name": "main-lbtc", "currency": "LBTC", "readonly": false}),
            json!({"name": "preferred-lbtc", "currency": "LBTC", "readonly": false}),
        ]
    }

    #[test]
    fn explicit_name_matched_verbatim_ignores_currency() {
        let w = wallets();
        let resolved = resolve_wallet(&w, "BTC", Some("main-lbtc"), None);
        assert_eq!(resolved.unwrap().get("name").unwrap(), "main-lbtc");
    }

    #[test]
    fn explicit_name_not_found_returns_none() {
        let w = wallets();
        assert!(resolve_wallet(&w, "BTC", Some("nope"), None).is_none());
    }

    #[test]
    fn preferred_name_wins_when_currency_matches() {
        let w = wallets();
        let resolved = resolve_wallet(&w, "LBTC", None, Some("preferred-lbtc"));
        assert_eq!(resolved.unwrap().get("name").unwrap(), "preferred-lbtc");
    }

    #[test]
    fn preferred_name_ignored_when_currency_mismatches() {
        // Control: a preferred name that exists but under the WRONG
        // currency must not be selected — falls through to the generic
        // writable-wallet-for-currency search instead.
        let w = wallets();
        let resolved = resolve_wallet(&w, "LBTC", None, Some("main-btc"));
        assert_eq!(resolved.unwrap().get("name").unwrap(), "main-lbtc");
    }

    #[test]
    fn falls_back_to_first_writable_wallet_for_currency() {
        let w = wallets();
        let resolved = resolve_wallet(&w, "BTC", None, None);
        assert_eq!(resolved.unwrap().get("name").unwrap(), "main-btc");
    }

    #[test]
    fn readonly_wallet_never_selected_by_fallback() {
        // Control: a readonly wallet exists for BTC but must never be
        // picked by the currency-only fallback path.
        let w = vec![json!({"name": "ro", "currency": "BTC", "readonly": true})];
        assert!(resolve_wallet(&w, "BTC", None, None).is_none());
    }

    #[test]
    fn no_matching_wallet_returns_none() {
        let w = wallets();
        assert!(resolve_wallet(&w, "XYZ", None, None).is_none());
    }

    #[test]
    fn resolve_wallet_name_extracts_name_string() {
        let w = wallets();
        assert_eq!(
            resolve_wallet_name(&w, "BTC", None, None),
            Some("main-btc".to_string())
        );
    }

    #[test]
    fn resolve_wallet_name_none_when_unresolved() {
        let w = wallets();
        assert_eq!(resolve_wallet_name(&w, "XYZ", None, None), None);
    }
}
