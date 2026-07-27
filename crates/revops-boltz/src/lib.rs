//! Rust port of the Python `cl_revenue_ops` Boltz swap subsystem
//! (`modules/boltz_manager.py`'s `BoltzCliManager`, ~2,670 LOC, and
//! `cl-revenue-ops.py`'s `BoltzAutoCycle` orchestration, ~1,400 LOC).
//!
//! Boltz performs ON-CHAIN swaps and moves real funds via a `boltzcli`
//! subprocess against a local `boltzd`. This is the highest-risk
//! subsystem in the plugin: swap creation is irreversible, unlike a fee
//! change. Per the port brief's HARD RULES, this crate keeps ALL decision
//! logic PURE (no subprocess, no file I/O, no clock reads, no RNG) and
//! models every external boundary as an injected trait or an explicit
//! parameter:
//!
//! - the `boltzcli` subprocess -> [`cli::BoltzCli`] (tests use
//!   [`cli::FakeBoltzCli`] exclusively — nothing in this crate's test
//!   suite touches a real process, wallet, or daemon);
//! - the swap journal file / ignored-external-swaps file -> pure
//!   [`journal`] functions operating on `Vec<Map<String, Value>>`, with
//!   the actual `open`/`read`/`os::replace` calls left to the live
//!   adapter;
//! - the capex budget engine's `reserve_boltz_swap_budget` /
//!   `settle_boltz_swap_reservation` / `release_boltz_swap_reservation` /
//!   `get_channel_budget` / `get_tactical_budget` -> plain parameters into
//!   [`budget`]'s gate/finalize functions, never a callback the kernel
//!   invokes itself;
//! - wall-clock time -> an explicit `now: i64` parameter everywhere.
//!
//! See `ENTRYPOINTS.md` for the history of what was NOT wired, and
//! `REGISTER.md` for the exact `pub mod`/`.rpcmethod(...)` snippets a
//! maintainer should paste into `crates/revops` to reach the wiring layer
//! below from a running plugin (this crate still does not modify
//! `crates/revops/src/main.rs` itself — see `REGISTER.md`).
//!
//! ## The wiring layer (`process`/`argv`/`wallet`/`commands`/`driver`/`rpc`/`execution`)
//!
//! On top of the pure decision kernels (`address`/`autocycle`/`budget`/
//! `cli`/`error`/`fee`/`journal`/`parsing`/`state`), this crate also
//! provides:
//! - [`execution::ExecutionMode`]: the crate-wide, `Default`-is-safe gate
//!   every fund-moving entry point requires;
//! - [`process::ProcessBoltzCli`]: a real [`cli::BoltzCli`] backed by
//!   `std::process::Command`, honouring the configured binary path,
//!   datadir, sudo user, and timeout;
//! - [`argv`]: pure per-command argv construction (quote/create-swap/
//!   create-reverse-swap/create-chain-swap/refund/claim/wallet-send/
//!   wallet-receive/wallet-list/swapinfo/listswaps) plus the
//!   safety-critical `withdraw_gate`;
//! - [`wallet`]: pure wallet-selection logic over an already-fetched
//!   `wallet list --json` payload;
//! - [`commands`]: [`execution::ExecutionMode`]-gated wrappers that are the
//!   ONLY place in this crate allowed to call `BoltzCli::run` for a
//!   fund-moving command;
//! - [`driver::run_balance_cycle_pass`]: one autocycle balance-cycle pass,
//!   composed purely from this crate's own kernels plus an injected
//!   [`cli::BoltzCli`];
//! - [`rpc`]: pure `serde_json::Value` response builders for the
//!   `status`/`budget`/`history`/`quote` operator RPCs.
//!
//! None of this is called from `crates/revops` yet — `REGISTER.md` is the
//! paste-in for a maintainer to do that outside this task's scope (this
//! crate does not touch `crates/revops/src/main.rs`).
//!
//! ## What this crate does NOT port (see `ENTRYPOINTS.md` for the full,
//! honest list)
//!
//! - The CLN first-hop-pinning / `--external-pay` routed reverse-swap path
//!   and its async chanIds-rejection retry dance (py boltz_manager.py:
//!   587-871, 2140-2365) — depends on a live CLN RPC client
//!   (`listpeerchannels`/`pay`/`decode`/`decodepay`) this crate does not
//!   have. [`argv::create_reverse_swap_argv`] covers the plain
//!   (non-`--external-pay`) `createreverseswap` path only, with a static
//!   caller-supplied `--chan-id` list (no probe-and-retry).
//! - `BoltzAutoCycle`'s plan BUILDERS (`_build_boltz_expansion_treasury_plan`,
//!   `_build_boltz_balance_plan`) and the candidate-scoring heuristics
//!   inside them — those depend on `CapacityPlanner`/`profitability_analyzer`
//!   output that has no Rust port yet (see `docs/port/PARITY-CHECKLIST.md`
//!   Lens 4). This crate ports the MODE SELECTION and STATE MACHINE that
//!   consume a plan's shape (status/executable-count/recommendation-count),
//!   not the plan construction itself.
//! - Phase 2G governor-facade integration (`_governed_open_reservation`,
//!   py boltz_manager.py:1636-1741) — flag-gated, cross-module wiring into
//!   `revops-econ`'s `GovernorFacade`/`econ_shadow`, out of scope for this
//!   pass. The un-governed reservation path (`reservation_gate`/
//!   `finalize_reservation_attempt` in [`budget`]) IS ported; the governed
//!   variant is not.
//! - LN+ swap automation (a separate, ~2,099 LOC component per the port
//!   map) — not part of this task's scope.

pub mod address;
pub mod argv;
pub mod autocycle;
pub mod budget;
pub mod cli;
pub mod commands;
pub mod driver;
pub mod error;
pub mod execution;
pub mod fee;
pub mod journal;
pub mod parsing;
pub mod process;
pub mod rpc;
pub mod state;
pub mod wallet;
