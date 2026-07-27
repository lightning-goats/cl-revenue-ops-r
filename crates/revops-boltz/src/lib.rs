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
//! See `ENTRYPOINTS.md` for what still needs to be wired into the plugin
//! binary (this crate is not called from `crates/revops/src/main.rs` —
//! per the port brief, that wiring is out of scope for this task and is
//! itself the #1 failure mode this brief calls out).
//!
//! ## What this crate does NOT port (see `ENTRYPOINTS.md` for the full,
//! honest list)
//!
//! - The `boltzcli` argv-construction glue for every individual command
//!   (loop_in/loop_out/chainswap/refund/claim/withdraw/wallet ops) and the
//!   CLN first-hop-pinning excludes-list logic (py boltz_manager.py:
//!   604-871). These are orchestration, not decision logic; they belong
//!   in the live adapter that holds a real [`cli::BoltzCli`] and a real
//!   CLN RPC client.
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
pub mod autocycle;
pub mod budget;
pub mod cli;
pub mod error;
pub mod fee;
pub mod journal;
pub mod parsing;
pub mod state;
