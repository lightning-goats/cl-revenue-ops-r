//! `gated.rs` — the safety wrappers execution-mode gating rides on. Every
//! test proves the SAME shape: in `DryRun`, the wrapped mutating method
//! returns `Err` and the underlying fake's call log stays EMPTY (the real
//! port was never touched, not just "touched but told no"); in `Armed`,
//! the call goes through and the fake records it. Read-only methods pass
//! through in both modes (control: proves the gate is selective, not a
//! blanket no-op).

mod common;

use common::*;
use revops_lnplus::exec_mode::ExecutionMode;
use revops_lnplus::gated::{GatedChainPort, GatedLnPlusApi};
use revops_lnplus::ports::{ChainPort, Feerate, LnPlusApi};
use revops_lnplus::types::Rating;

// ------------------------------------------------------------- LnPlusApi --

#[test]
fn dry_run_blocks_create_application_and_never_calls_inner() {
    let api = FakeApi::new();
    let logger = FakeLogger::new();
    let gated = GatedLnPlusApi::new(&api, ExecutionMode::DryRun, &logger);

    let result = gated.create_application("swap-1");

    assert!(result.is_err(), "DryRun must refuse create_application");
    assert!(
        api.create_application_calls.borrow().is_empty(),
        "the real port must never be called in DryRun"
    );
    assert!(logger.contains("DRY RUN"));
}

#[test]
fn armed_allows_create_application_and_calls_inner() {
    // CONTROL for the test above: same call, `Armed` instead — proves the
    // suppression above is due to the mode, not some unrelated bug that
    // would make `create_application` always fail.
    let api = FakeApi::new();
    let logger = FakeLogger::new();
    let gated = GatedLnPlusApi::new(&api, ExecutionMode::Armed, &logger);

    let result = gated.create_application("swap-1");

    assert!(
        result.is_ok(),
        "Armed must allow create_application through"
    );
    assert_eq!(api.create_application_calls.borrow().as_slice(), ["swap-1"]);
}

#[test]
fn dry_run_blocks_delete_application() {
    let api = FakeApi::new();
    let logger = FakeLogger::new();
    let gated = GatedLnPlusApi::new(&api, ExecutionMode::DryRun, &logger);
    assert!(gated.delete_application("swap-1").is_err());
    assert!(api.delete_application_calls.borrow().is_empty());
}

#[test]
fn dry_run_blocks_complete_application() {
    let api = FakeApi::new();
    let logger = FakeLogger::new();
    let gated = GatedLnPlusApi::new(&api, ExecutionMode::DryRun, &logger);
    assert!(gated.complete_application("swap-1").is_err());
    assert!(api.complete_application_calls.borrow().is_empty());
}

#[test]
fn dry_run_blocks_create_rating() {
    let api = FakeApi::new();
    let logger = FakeLogger::new();
    let gated = GatedLnPlusApi::new(&api, ExecutionMode::DryRun, &logger);
    assert!(gated.create_rating("swap-1", Rating::Positive).is_err());
    assert!(api.create_rating_calls.borrow().is_empty());
}

#[test]
fn dry_run_blocks_mark_read_notifications() {
    let api = FakeApi::new();
    let logger = FakeLogger::new();
    let gated = GatedLnPlusApi::new(&api, ExecutionMode::DryRun, &logger);
    assert!(gated.mark_read_notifications().is_err());
}

#[test]
fn dry_run_still_allows_reads() {
    // Control: the gate is selective. Reads must pass through in EVERY
    // mode, including DryRun — otherwise a dry-run evaluator pass could
    // never even see what it would have applied to.
    let api = FakeApi::new();
    let _ = api
        .applicable_swaps
        .replace(Ok(vec![listing("s1", vec![])]));
    let logger = FakeLogger::new();
    let gated = GatedLnPlusApi::new(&api, ExecutionMode::DryRun, &logger);

    let swaps = gated.get_applicable_swaps().unwrap();
    assert_eq!(swaps.len(), 1);
    assert!(gated.get_my_swaps().is_ok());
    assert!(gated.get_notifications().is_ok());
}

// ------------------------------------------------------------- ChainPort --

#[test]
fn dry_run_blocks_connect_and_never_calls_inner() {
    let chain = FakeChain::new();
    let logger = FakeLogger::new();
    let gated = GatedChainPort::new(&chain, ExecutionMode::DryRun, &logger);

    let result = gated.connect("02aa@1.2.3.4:9735");

    assert!(result.is_err(), "DryRun must refuse connect");
    assert!(
        chain.connect_calls.borrow().is_empty(),
        "the real chain port must never be called in DryRun"
    );
}

#[test]
fn armed_allows_connect_and_calls_inner() {
    let chain = FakeChain::new();
    let logger = FakeLogger::new();
    let gated = GatedChainPort::new(&chain, ExecutionMode::Armed, &logger);

    let result = gated.connect("02aa@1.2.3.4:9735");

    assert!(result.is_ok());
    assert_eq!(
        chain.connect_calls.borrow().as_slice(),
        ["02aa@1.2.3.4:9735"]
    );
}

#[test]
fn dry_run_blocks_fund_channel_and_never_calls_inner() {
    let chain = FakeChain::new();
    let logger = FakeLogger::new();
    let gated = GatedChainPort::new(&chain, ExecutionMode::DryRun, &logger);

    let result = gated.fund_channel(&pubkey(1), 500_000, Feerate::Normal);

    assert!(result.is_err(), "DryRun must refuse fund_channel");
    assert!(
        chain.fund_channel_calls.borrow().is_empty(),
        "the real chain port must never be called in DryRun"
    );
}

#[test]
fn armed_allows_fund_channel_and_calls_inner() {
    let chain = FakeChain::new();
    let logger = FakeLogger::new();
    let gated = GatedChainPort::new(&chain, ExecutionMode::Armed, &logger);

    let result = gated.fund_channel(&pubkey(1), 500_000, Feerate::Normal);

    assert!(result.is_ok());
    assert_eq!(chain.fund_channel_calls.borrow().len(), 1);
}

#[test]
fn dry_run_still_allows_chain_reads() {
    let chain = FakeChain::new();
    let logger = FakeLogger::new();
    let gated = GatedChainPort::new(&chain, ExecutionMode::DryRun, &logger);

    assert!(gated.our_node_id().is_ok());
    assert!(gated.list_peer_channels(None).is_ok());
    assert!(gated.opening_feerate_perkw().is_ok());
    assert!(gated.confirmed_unreserved_sats().is_ok());
}
