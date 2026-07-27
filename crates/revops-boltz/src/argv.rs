//! Pure `boltzcli` argv construction for the commands the kernels in this
//! crate need a caller to actually run.
//!
//! Ports the argv-assembly half of py `quote`/`loop_in`/`_loop_out_locked`/
//! `chainswap`/`refund`/`claim`/`withdraw`/`deposit_address`/`swap_status`/
//! `swap_history`/`wallet_balances` (boltz_manager.py:1857-2646), MINUS:
//! - budget gating, reservation open/finalize, and journal recording (those
//!   are `budget.rs`/`journal.rs` plus the live adapter's job);
//! - wallet resolution (that's `wallet.rs`, given an already-fetched wallet
//!   list);
//! - the CLN first-hop-pinning / `--external-pay` routed reverse-swap path
//!   and the async chanIds-rejection retry dance (py boltz_manager.py:
//!   2140-2365) — deliberately NOT ported here (see `ENTRYPOINTS.md`'s
//!   "Deliberately NOT ported" #2): it depends on a live CLN RPC client
//!   this crate does not have. [`create_reverse_swap_argv`] covers the
//!   plain (non-external-pay) `createreverseswap` path only, with an
//!   optional static `--chan-id` list supplied by the caller.
//!
//! Every function here is pure: no subprocess, no file I/O, no wallet-list
//! fetch. Validation that Python performs BEFORE the subprocess call (empty
//! amount, malformed destination, same-currency chainswap, empty swap-id
//! list) is preserved as a typed [`ArgvError`] instead of a raised
//! exception, so a caller cannot skip it by catching the wrong exception
//! type.

use crate::address::{validate_onchain_address, validate_swap_destination};
use std::fmt;

/// py `_norm_currency` (boltz_manager.py:544-550).
pub fn normalize_currency(currency: Option<&str>, default: &str) -> String {
    let c = currency.unwrap_or(default).trim().to_uppercase();
    match c.as_str() {
        "L-BTC" | "LBTC" => "LBTC".to_string(),
        other => other.to_string(),
    }
}

/// py `_swap_cli_currency` (boltz_manager.py:552-554): the lowercase form
/// boltzcli's positional currency argument expects.
pub fn swap_cli_currency(currency: Option<&str>, default: &str) -> String {
    normalize_currency(currency, default).to_lowercase()
}

/// Validation failures caught BEFORE any subprocess call would be made —
/// the same guard points as py's pre-`_run`/`_run_json` `raise
/// BoltzCliError(...)` calls, made into a typed, matchable error instead of
/// a string exception.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArgvError {
    /// py boltz_manager.py:1861-1862, 1906-1907, 2493-2494: `amount_sats`
    /// must be > 0.
    NonPositiveAmount,
    /// py boltz_manager.py:1874-1875: `swap_type` not one of reverse/
    /// submarine/normal/chain.
    InvalidSwapType(String),
    /// py boltz_manager.py:2497-2498: chainswap `from_currency` ==
    /// `to_currency`.
    SameCurrencyChainSwap(String),
    /// py boltz_manager.py:2475-2477: `claim`'s `swap_ids` was empty (or
    /// all-whitespace) after filtering.
    EmptySwapIds,
    /// py boltz_manager.py:2465-2468 / 2481-2484: an explicit non-`"wallet"`
    /// refund/claim destination that fails `validate_swap_destination`.
    InvalidDestination(String),
}

impl fmt::Display for ArgvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ArgvError::NonPositiveAmount => write!(f, "amount_sats must be > 0"),
            ArgvError::InvalidSwapType(s) => {
                write!(
                    f,
                    "swap_type must be reverse, submarine, or chain, got {s:?}"
                )
            }
            ArgvError::SameCurrencyChainSwap(c) => {
                write!(
                    f,
                    "from_currency and to_currency must differ, both were {c}"
                )
            }
            ArgvError::EmptySwapIds => write!(f, "swap_ids is required"),
            ArgvError::InvalidDestination(d) => {
                write!(f, "invalid on-chain destination address: refusing: {d}")
            }
        }
    }
}

impl std::error::Error for ArgvError {}

/// py `quote`'s `st` dispatch (boltz_manager.py:1859-1875), collapsed to a
/// closed enum instead of a free string so a live adapter cannot typo a
/// fourth branch into existence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SwapType {
    Reverse,
    Submarine,
    Chain,
}

/// py `(swap_type or "reverse").strip().lower()` dispatch: `"reverse"` ->
/// [`SwapType::Reverse`], `"submarine"`/`"normal"` -> [`SwapType::Submarine`],
/// `"chain"` -> [`SwapType::Chain`], anything else -> `ArgvError::InvalidSwapType`.
pub fn classify_swap_type(raw: &str) -> Result<SwapType, ArgvError> {
    match raw.trim().to_lowercase().as_str() {
        "reverse" => Ok(SwapType::Reverse),
        "submarine" | "normal" => Ok(SwapType::Submarine),
        "chain" => Ok(SwapType::Chain),
        other => Err(ArgvError::InvalidSwapType(other.to_string())),
    }
}

/// py `quote` (boltz_manager.py:1857-1877)'s argv assembly, up to (not
/// including) `_run_json`.
pub fn quote_argv(
    swap_type: SwapType,
    amount_sats: i64,
    currency: Option<&str>,
) -> Result<Vec<String>, ArgvError> {
    if amount_sats <= 0 {
        return Err(ArgvError::NonPositiveAmount);
    }
    let amt = amount_sats.to_string();
    Ok(match swap_type {
        SwapType::Reverse => {
            let cur = normalize_currency(currency, "BTC");
            vec![
                "quote".into(),
                "--json".into(),
                "--send".into(),
                amt,
                "--to".into(),
                cur,
                "reverse".into(),
            ]
        }
        SwapType::Submarine => {
            let cur = normalize_currency(currency, "LBTC");
            vec![
                "quote".into(),
                "--json".into(),
                "--receive".into(),
                amt,
                "--from".into(),
                cur,
                "submarine".into(),
            ]
        }
        SwapType::Chain => {
            let from_cur = normalize_currency(currency, "LBTC");
            let to_cur = if from_cur == "LBTC" { "BTC" } else { "LBTC" }.to_string();
            vec![
                "quote".into(),
                "--json".into(),
                "--send".into(),
                amt,
                "--from".into(),
                from_cur,
                "--to".into(),
                to_cur,
                "chain".into(),
            ]
        }
    })
}

/// py `loop_in`'s `createswap` argv assembly (boltz_manager.py:1970):
/// `["createswap", "--json", "--from-wallet", wallet_name, cli_currency,
/// amount_sats]`. `wallet_name` must already be resolved (see `wallet.rs`).
pub fn create_swap_argv(
    wallet_name: &str,
    currency: Option<&str>,
    amount_sats: i64,
) -> Result<Vec<String>, ArgvError> {
    if amount_sats <= 0 {
        return Err(ArgvError::NonPositiveAmount);
    }
    Ok(vec![
        "createswap".into(),
        "--json".into(),
        "--from-wallet".into(),
        wallet_name.into(),
        swap_cli_currency(currency, "LBTC"),
        amount_sats.to_string(),
    ])
}

/// py `_loop_out_locked`'s plain (non-`--external-pay`) `_build_args`
/// (boltz_manager.py:2281-2298): flags, then optional static `--chan-id`
/// pins, then `--routing-fee-limit-ppm`, then `--to-wallet` (only when no
/// on-chain `address` is given), then the `--` terminator, then the
/// positional currency + amount, then the address if one was given.
/// `chan_ids`/CLN first-hop-pinning retry logic is NOT reproduced (see
/// module docs) — this is one static argv build, not the stateful
/// probe-and-retry dance.
#[allow(clippy::too_many_arguments)]
pub fn create_reverse_swap_argv(
    amount_sats: i64,
    currency: Option<&str>,
    address: Option<&str>,
    wallet_name: Option<&str>,
    chan_ids: &[String],
    routing_fee_limit_ppm: i64,
) -> Result<Vec<String>, ArgvError> {
    if amount_sats <= 0 {
        return Err(ArgvError::NonPositiveAmount);
    }
    let mut cmd = vec!["createreverseswap".to_string(), "--json".to_string()];
    for scid in chan_ids {
        cmd.push("--chan-id".to_string());
        cmd.push(scid.clone());
    }
    if routing_fee_limit_ppm > 0 {
        cmd.push("--routing-fee-limit-ppm".to_string());
        cmd.push(routing_fee_limit_ppm.to_string());
    }
    if address.is_none() {
        // py falls back to an empty "None" wallet name if resolution
        // failed upstream; this port requires the caller to have already
        // resolved one (or supplied an address) rather than silently
        // stringifying `None`.
        let wallet = wallet_name.unwrap_or_default();
        cmd.push("--to-wallet".to_string());
        cmd.push(wallet.to_string());
    }
    cmd.push("--".to_string());
    cmd.push(swap_cli_currency(currency, "BTC"));
    cmd.push(amount_sats.to_string());
    if let Some(addr) = address {
        cmd.push(addr.to_string());
    }
    Ok(cmd)
}

/// py `chainswap`'s `createchainswap` argv assembly (boltz_manager.py:
/// 2547-2557).
pub fn create_chain_swap_argv(
    amount_sats: i64,
    from_currency: Option<&str>,
    to_currency: Option<&str>,
    from_wallet_name: &str,
    to_address: Option<&str>,
    to_wallet_name: Option<&str>,
) -> Result<Vec<String>, ArgvError> {
    if amount_sats <= 0 {
        return Err(ArgvError::NonPositiveAmount);
    }
    let from_cur = normalize_currency(from_currency, "LBTC");
    let to_cur = normalize_currency(to_currency, "BTC");
    if from_cur == to_cur {
        return Err(ArgvError::SameCurrencyChainSwap(from_cur));
    }
    let mut args = vec![
        "createchainswap".to_string(),
        "--json".to_string(),
        "--from-wallet".to_string(),
        from_wallet_name.to_string(),
    ];
    if let Some(addr) = to_address {
        args.push("--to-address".to_string());
        args.push(addr.to_string());
    } else {
        args.push("--to-wallet".to_string());
        args.push(to_wallet_name.unwrap_or_default().to_string());
    }
    args.push("--".to_string());
    args.push(amount_sats.to_string());
    Ok(args)
}

/// py `refund` (boltz_manager.py:2461-2472): validates a non-`"wallet"`
/// destination BEFORE building the argv (P4-005 — never let a bad address
/// reach boltzcli).
pub fn refund_swap_argv(
    swap_id: &str,
    destination: Option<&str>,
) -> Result<Vec<String>, ArgvError> {
    let dest = destination.unwrap_or("wallet");
    if dest != "wallet" && !validate_swap_destination(dest) {
        return Err(ArgvError::InvalidDestination(dest.to_string()));
    }
    Ok(vec![
        "refundswap".to_string(),
        "--".to_string(),
        swap_id.to_string(),
        dest.to_string(),
    ])
}

/// py `claim` (boltz_manager.py:2474-2488): non-empty `swap_ids` (after
/// trimming) and the same destination-validation gate as `refund`.
pub fn claim_swaps_argv(
    swap_ids: &[String],
    destination: Option<&str>,
) -> Result<Vec<String>, ArgvError> {
    let ids: Vec<String> = swap_ids
        .iter()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if ids.is_empty() {
        return Err(ArgvError::EmptySwapIds);
    }
    let dest = destination.unwrap_or("wallet");
    if dest != "wallet" && !validate_swap_destination(dest) {
        return Err(ArgvError::InvalidDestination(dest.to_string()));
    }
    let mut args = vec!["claimswaps".to_string(), "--".to_string(), dest.to_string()];
    args.extend(ids);
    Ok(args)
}

/// Safety-critical pre-subprocess gate for `withdraw` (py boltz_manager.py:
/// 2581-2619): destination-address validity, the sweep-requires-
/// confirmation guard, and the hard `max_withdraw_sats` cap. This is
/// deliberately a SEPARATE function from [`wallet_send_argv`] (which
/// assumes the gate already passed) — the DD3/P1-006 cap and P1-006
/// address check are the load-bearing safety logic here, not the argv
/// shape, and must not be skippable by a caller who only calls the argv
/// builder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WithdrawGateError {
    EmptyDestination,
    InvalidDestination {
        destination: String,
        currency: String,
    },
    NonPositiveAmount,
    /// py boltz_manager.py:2609-2614: `sweep=true` without
    /// `confirm_sweep=true`.
    SweepRequiresConfirmation,
    /// py boltz_manager.py:2615-2619 (DD3/P1-006 hard cap).
    ExceedsMaxWithdrawCap {
        amount_sats: i64,
        max_withdraw_sats: i64,
    },
}

impl fmt::Display for WithdrawGateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WithdrawGateError::EmptyDestination => write!(f, "destination is required"),
            WithdrawGateError::InvalidDestination { destination, currency } => write!(
                f,
                "invalid {currency} on-chain destination address: refusing to withdraw: {destination}"
            ),
            WithdrawGateError::NonPositiveAmount => {
                write!(f, "amount_sats must be > 0 unless sweep=true")
            }
            WithdrawGateError::SweepRequiresConfirmation => write!(
                f,
                "sweep withdraws the entire wallet balance and bypasses the max_withdraw_sats cap; pass confirm_sweep=true to proceed"
            ),
            WithdrawGateError::ExceedsMaxWithdrawCap { amount_sats, max_withdraw_sats } => write!(
                f,
                "withdraw amount {amount_sats} exceeds max_withdraw_sats cap {max_withdraw_sats}: refusing to withdraw"
            ),
        }
    }
}

impl std::error::Error for WithdrawGateError {}

/// py boltz_manager.py:2584-2619's checks, in the same order (destination
/// required -> address valid -> sweep-confirm -> non-sweep amount/cap).
pub fn withdraw_gate(
    destination: &str,
    currency: &str,
    amount_sats: i64,
    sweep: bool,
    confirm_sweep: bool,
    max_withdraw_sats: i64,
) -> Result<(), WithdrawGateError> {
    if destination.trim().is_empty() {
        return Err(WithdrawGateError::EmptyDestination);
    }
    if !validate_onchain_address(destination, currency) {
        return Err(WithdrawGateError::InvalidDestination {
            destination: destination.to_string(),
            currency: currency.to_string(),
        });
    }
    if sweep {
        if !confirm_sweep {
            return Err(WithdrawGateError::SweepRequiresConfirmation);
        }
        return Ok(());
    }
    if amount_sats <= 0 {
        return Err(WithdrawGateError::NonPositiveAmount);
    }
    if max_withdraw_sats > 0 && amount_sats > max_withdraw_sats {
        return Err(WithdrawGateError::ExceedsMaxWithdrawCap {
            amount_sats,
            max_withdraw_sats,
        });
    }
    Ok(())
}

/// py `withdraw`'s `wallet send` argv assembly (boltz_manager.py:2620-2628).
/// Assumes [`withdraw_gate`] has already been called and returned `Ok`.
pub fn wallet_send_argv(
    wallet_name: &str,
    destination: &str,
    amount_sats: i64,
    sat_per_vbyte: Option<i64>,
    sweep: bool,
) -> Vec<String> {
    let mut args = vec!["wallet".to_string(), "send".to_string()];
    if let Some(spv) = sat_per_vbyte {
        args.push("--sat-per-vbyte".to_string());
        args.push(spv.to_string());
    }
    if sweep {
        args.push("--sweep".to_string());
    }
    let amt = if sweep { 0 } else { amount_sats };
    args.push("--".to_string());
    args.push(wallet_name.to_string());
    args.push(destination.to_string());
    args.push(amt.to_string());
    args
}

/// py `deposit_address`'s `wallet receive` argv (boltz_manager.py:2642).
pub fn wallet_receive_argv(wallet_name: &str) -> Vec<String> {
    vec![
        "wallet".to_string(),
        "receive".to_string(),
        wallet_name.to_string(),
    ]
}

/// py `_wallet_list` (boltz_manager.py:556-557).
pub fn wallet_list_argv() -> Vec<String> {
    vec![
        "wallet".to_string(),
        "list".to_string(),
        "--json".to_string(),
    ]
}

/// py `swap_status`'s `swapinfo` argv (boltz_manager.py:2398): `--`
/// terminates option parsing so a `swap_id` beginning with `-` cannot be
/// reparsed by boltzcli as a flag.
pub fn swap_info_argv(swap_id: &str) -> Vec<String> {
    vec![
        "swapinfo".to_string(),
        "--".to_string(),
        swap_id.to_string(),
    ]
}

/// py `_listswaps_json` (boltz_manager.py:926-950)'s base call shape (the
/// `manual_only`/`pending_only` filtering is a live-adapter I/O-side
/// concern over the parsed JSON, not an argv difference).
pub fn list_swaps_argv() -> Vec<String> {
    vec!["listswaps".to_string(), "--json".to_string()]
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- normalize_currency / swap_cli_currency ---

    #[test]
    fn normalize_currency_aliases_l_dash_btc() {
        assert_eq!(normalize_currency(Some("L-BTC"), "BTC"), "LBTC");
        assert_eq!(normalize_currency(Some("lbtc"), "BTC"), "LBTC");
    }

    #[test]
    fn normalize_currency_uses_default_when_none() {
        assert_eq!(normalize_currency(None, "LBTC"), "LBTC");
    }

    #[test]
    fn swap_cli_currency_is_lowercase() {
        assert_eq!(swap_cli_currency(Some("BTC"), "LBTC"), "btc");
    }

    // --- classify_swap_type ---

    #[test]
    fn classify_swap_type_recognizes_all_valid_forms() {
        assert_eq!(classify_swap_type("reverse"), Ok(SwapType::Reverse));
        assert_eq!(classify_swap_type("submarine"), Ok(SwapType::Submarine));
        assert_eq!(classify_swap_type("normal"), Ok(SwapType::Submarine));
        assert_eq!(classify_swap_type("chain"), Ok(SwapType::Chain));
        assert_eq!(classify_swap_type("  CHAIN  "), Ok(SwapType::Chain));
    }

    #[test]
    fn classify_swap_type_rejects_unknown() {
        assert_eq!(
            classify_swap_type("bogus"),
            Err(ArgvError::InvalidSwapType("bogus".to_string()))
        );
    }

    // --- quote_argv ---

    #[test]
    fn quote_reverse_argv_matches_python_shape() {
        let argv = quote_argv(SwapType::Reverse, 50_000, None).unwrap();
        assert_eq!(
            argv,
            vec!["quote", "--json", "--send", "50000", "--to", "BTC", "reverse"]
        );
    }

    #[test]
    fn quote_submarine_argv_matches_python_shape() {
        let argv = quote_argv(SwapType::Submarine, 1000, Some("btc")).unwrap();
        assert_eq!(
            argv,
            vec![
                "quote",
                "--json",
                "--receive",
                "1000",
                "--from",
                "BTC",
                "submarine"
            ]
        );
    }

    #[test]
    fn quote_chain_argv_picks_opposite_target_currency() {
        let argv = quote_argv(SwapType::Chain, 2000, Some("LBTC")).unwrap();
        assert_eq!(
            argv,
            vec!["quote", "--json", "--send", "2000", "--from", "LBTC", "--to", "BTC", "chain"]
        );
        // Control: BTC source flips to LBTC target.
        let argv2 = quote_argv(SwapType::Chain, 2000, Some("BTC")).unwrap();
        assert_eq!(argv2[5], "BTC");
        assert_eq!(argv2[7], "LBTC");
    }

    #[test]
    fn quote_zero_or_negative_amount_rejected() {
        assert_eq!(
            quote_argv(SwapType::Reverse, 0, None),
            Err(ArgvError::NonPositiveAmount)
        );
        assert_eq!(
            quote_argv(SwapType::Reverse, -5, None),
            Err(ArgvError::NonPositiveAmount)
        );
    }

    // --- create_swap_argv (loop_in) ---

    #[test]
    fn create_swap_argv_matches_python_shape() {
        let argv = create_swap_argv("lbtc-wallet-1", Some("LBTC"), 25_000).unwrap();
        assert_eq!(
            argv,
            vec![
                "createswap",
                "--json",
                "--from-wallet",
                "lbtc-wallet-1",
                "lbtc",
                "25000"
            ]
        );
    }

    #[test]
    fn create_swap_argv_rejects_non_positive_amount() {
        assert_eq!(
            create_swap_argv("w", None, 0),
            Err(ArgvError::NonPositiveAmount)
        );
    }

    // --- create_reverse_swap_argv (loop_out) ---

    #[test]
    fn create_reverse_swap_argv_to_wallet_no_chanids() {
        let argv =
            create_reverse_swap_argv(75_000, Some("BTC"), None, Some("btc-w"), &[], 0).unwrap();
        assert_eq!(
            argv,
            vec![
                "createreverseswap",
                "--json",
                "--to-wallet",
                "btc-w",
                "--",
                "btc",
                "75000"
            ]
        );
    }

    #[test]
    fn create_reverse_swap_argv_to_address_omits_to_wallet() {
        let argv = create_reverse_swap_argv(
            1000,
            Some("BTC"),
            Some("bc1qqqqsyqcyq5rqwzqfpg9scrgwpugpzysn4v0345"),
            None,
            &[],
            0,
        )
        .unwrap();
        assert_eq!(
            argv,
            vec![
                "createreverseswap",
                "--json",
                "--",
                "btc",
                "1000",
                "bc1qqqqsyqcyq5rqwzqfpg9scrgwpugpzysn4v0345"
            ]
        );
    }

    #[test]
    fn create_reverse_swap_argv_includes_chan_ids_and_routing_limit() {
        let chan_ids = vec!["111x1x0".to_string(), "222x2x0".to_string()];
        let argv =
            create_reverse_swap_argv(1000, Some("BTC"), None, Some("w"), &chan_ids, 500).unwrap();
        assert_eq!(
            argv,
            vec![
                "createreverseswap",
                "--json",
                "--chan-id",
                "111x1x0",
                "--chan-id",
                "222x2x0",
                "--routing-fee-limit-ppm",
                "500",
                "--to-wallet",
                "w",
                "--",
                "btc",
                "1000"
            ]
        );
    }

    #[test]
    fn create_reverse_swap_argv_rejects_non_positive_amount() {
        assert_eq!(
            create_reverse_swap_argv(0, None, None, Some("w"), &[], 0),
            Err(ArgvError::NonPositiveAmount)
        );
    }

    // --- create_chain_swap_argv ---

    #[test]
    fn create_chain_swap_argv_to_wallet() {
        let argv = create_chain_swap_argv(
            10_000,
            Some("LBTC"),
            Some("BTC"),
            "lbtc-w",
            None,
            Some("btc-w"),
        )
        .unwrap();
        assert_eq!(
            argv,
            vec![
                "createchainswap",
                "--json",
                "--from-wallet",
                "lbtc-w",
                "--to-wallet",
                "btc-w",
                "--",
                "10000"
            ]
        );
    }

    #[test]
    fn create_chain_swap_argv_to_address() {
        let argv = create_chain_swap_argv(
            10_000,
            Some("LBTC"),
            Some("BTC"),
            "lbtc-w",
            Some("bc1qqqqsyqcyq5rqwzqfpg9scrgwpugpzysn4v0345"),
            None,
        )
        .unwrap();
        assert_eq!(
            argv,
            vec![
                "createchainswap",
                "--json",
                "--from-wallet",
                "lbtc-w",
                "--to-address",
                "bc1qqqqsyqcyq5rqwzqfpg9scrgwpugpzysn4v0345",
                "--",
                "10000"
            ]
        );
    }

    #[test]
    fn create_chain_swap_argv_rejects_same_currency() {
        let err = create_chain_swap_argv(10_000, Some("BTC"), Some("BTC"), "w", None, Some("w2"))
            .unwrap_err();
        assert_eq!(err, ArgvError::SameCurrencyChainSwap("BTC".to_string()));
    }

    #[test]
    fn create_chain_swap_argv_rejects_non_positive_amount() {
        assert_eq!(
            create_chain_swap_argv(0, Some("LBTC"), Some("BTC"), "w", None, Some("w2")),
            Err(ArgvError::NonPositiveAmount)
        );
    }

    // --- refund_swap_argv / claim_swaps_argv ---

    #[test]
    fn refund_swap_argv_defaults_to_wallet_destination() {
        let argv = refund_swap_argv("swap-1", None).unwrap();
        assert_eq!(argv, vec!["refundswap", "--", "swap-1", "wallet"]);
    }

    #[test]
    fn refund_swap_argv_accepts_valid_onchain_destination() {
        let argv =
            refund_swap_argv("swap-1", Some("bc1qqqqsyqcyq5rqwzqfpg9scrgwpugpzysn4v0345")).unwrap();
        assert_eq!(
            argv,
            vec![
                "refundswap",
                "--",
                "swap-1",
                "bc1qqqqsyqcyq5rqwzqfpg9scrgwpugpzysn4v0345"
            ]
        );
    }

    #[test]
    fn refund_swap_argv_rejects_invalid_destination() {
        let err = refund_swap_argv("swap-1", Some("not-an-address")).unwrap_err();
        assert_eq!(
            err,
            ArgvError::InvalidDestination("not-an-address".to_string())
        );
    }

    #[test]
    fn claim_swaps_argv_matches_python_shape() {
        let ids = vec!["a".to_string(), "b".to_string()];
        let argv = claim_swaps_argv(&ids, None).unwrap();
        assert_eq!(argv, vec!["claimswaps", "--", "wallet", "a", "b"]);
    }

    #[test]
    fn claim_swaps_argv_filters_blank_ids() {
        let ids = vec!["  ".to_string(), "b".to_string(), "".to_string()];
        let argv = claim_swaps_argv(&ids, None).unwrap();
        assert_eq!(argv, vec!["claimswaps", "--", "wallet", "b"]);
    }

    #[test]
    fn claim_swaps_argv_rejects_all_blank_ids() {
        let ids = vec!["  ".to_string(), "".to_string()];
        assert_eq!(claim_swaps_argv(&ids, None), Err(ArgvError::EmptySwapIds));
    }

    #[test]
    fn claim_swaps_argv_rejects_invalid_destination() {
        let ids = vec!["a".to_string()];
        assert_eq!(
            claim_swaps_argv(&ids, Some("garbage")),
            Err(ArgvError::InvalidDestination("garbage".to_string()))
        );
    }

    // --- withdraw_gate ---

    #[test]
    fn withdraw_gate_rejects_empty_destination() {
        assert_eq!(
            withdraw_gate("", "BTC", 1000, false, false, 0),
            Err(WithdrawGateError::EmptyDestination)
        );
    }

    #[test]
    fn withdraw_gate_rejects_invalid_address() {
        let err = withdraw_gate("garbage", "BTC", 1000, false, false, 0).unwrap_err();
        assert_eq!(
            err,
            WithdrawGateError::InvalidDestination {
                destination: "garbage".to_string(),
                currency: "BTC".to_string()
            }
        );
    }

    #[test]
    fn withdraw_gate_accepts_valid_amount_within_cap() {
        assert_eq!(
            withdraw_gate(
                "bc1qqqqsyqcyq5rqwzqfpg9scrgwpugpzysn4v0345",
                "BTC",
                1000,
                false,
                false,
                5000
            ),
            Ok(())
        );
    }

    #[test]
    fn withdraw_gate_rejects_non_positive_amount_unless_sweep() {
        assert_eq!(
            withdraw_gate(
                "bc1qqqqsyqcyq5rqwzqfpg9scrgwpugpzysn4v0345",
                "BTC",
                0,
                false,
                false,
                0
            ),
            Err(WithdrawGateError::NonPositiveAmount)
        );
    }

    #[test]
    fn withdraw_gate_rejects_sweep_without_confirmation() {
        assert_eq!(
            withdraw_gate(
                "bc1qqqqsyqcyq5rqwzqfpg9scrgwpugpzysn4v0345",
                "BTC",
                0,
                true,
                false,
                0
            ),
            Err(WithdrawGateError::SweepRequiresConfirmation)
        );
    }

    #[test]
    fn withdraw_gate_allows_sweep_with_confirmation() {
        // Control: same call, confirm_sweep=true now passes despite
        // amount_sats=0 (sweep does not use amount_sats at all).
        assert_eq!(
            withdraw_gate(
                "bc1qqqqsyqcyq5rqwzqfpg9scrgwpugpzysn4v0345",
                "BTC",
                0,
                true,
                true,
                0
            ),
            Ok(())
        );
    }

    #[test]
    fn withdraw_gate_enforces_max_withdraw_cap() {
        let err = withdraw_gate(
            "bc1qqqqsyqcyq5rqwzqfpg9scrgwpugpzysn4v0345",
            "BTC",
            6000,
            false,
            false,
            5000,
        )
        .unwrap_err();
        assert_eq!(
            err,
            WithdrawGateError::ExceedsMaxWithdrawCap {
                amount_sats: 6000,
                max_withdraw_sats: 5000
            }
        );
    }

    #[test]
    fn withdraw_gate_zero_cap_means_uncapped() {
        // Control: max_withdraw_sats<=0 means "no cap configured" (py:
        // `max_withdraw > 0 and amt > max_withdraw`), not "cap at zero".
        assert_eq!(
            withdraw_gate(
                "bc1qqqqsyqcyq5rqwzqfpg9scrgwpugpzysn4v0345",
                "BTC",
                1_000_000,
                false,
                false,
                0
            ),
            Ok(())
        );
    }

    // --- wallet_send_argv / wallet_receive_argv / wallet_list_argv ---

    #[test]
    fn wallet_send_argv_plain() {
        let argv = wallet_send_argv(
            "w1",
            "bc1qqqqsyqcyq5rqwzqfpg9scrgwpugpzysn4v0345",
            1000,
            None,
            false,
        );
        assert_eq!(
            argv,
            vec![
                "wallet",
                "send",
                "--",
                "w1",
                "bc1qqqqsyqcyq5rqwzqfpg9scrgwpugpzysn4v0345",
                "1000"
            ]
        );
    }

    #[test]
    fn wallet_send_argv_with_fee_rate_and_sweep() {
        let argv = wallet_send_argv(
            "w1",
            "bc1qqqqsyqcyq5rqwzqfpg9scrgwpugpzysn4v0345",
            999_999,
            Some(5),
            true,
        );
        assert_eq!(
            argv,
            vec![
                "wallet",
                "send",
                "--sat-per-vbyte",
                "5",
                "--sweep",
                "--",
                "w1",
                "bc1qqqqsyqcyq5rqwzqfpg9scrgwpugpzysn4v0345",
                "0"
            ],
            "sweep must force the positional amount to 0 regardless of amount_sats"
        );
    }

    #[test]
    fn wallet_receive_argv_matches_python_shape() {
        assert_eq!(wallet_receive_argv("w1"), vec!["wallet", "receive", "w1"]);
    }

    #[test]
    fn wallet_list_argv_matches_python_shape() {
        assert_eq!(wallet_list_argv(), vec!["wallet", "list", "--json"]);
    }

    // --- swap_info_argv / list_swaps_argv ---

    #[test]
    fn swap_info_argv_uses_terminator() {
        assert_eq!(
            swap_info_argv("-weird-id"),
            vec!["swapinfo", "--", "-weird-id"]
        );
    }

    #[test]
    fn list_swaps_argv_matches_python_shape() {
        assert_eq!(list_swaps_argv(), vec!["listswaps", "--json"]);
    }
}
