# Boltz Process Fake-Executable Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove the real `ProcessBoltzCli` spawn, output, failure, timeout, child-termination, and ambiguous-create behavior without contacting a live Boltz service.

**Architecture:** Integration tests create chmod-executable shell programs inside `tempfile::TempDir` and point `BoltzCliProcessConfig::cli_path` at them. Tests enter through the public `BoltzCli::run` and `commands::execute_loop_in` APIs so the production `Command` boundary, timeout loop, and create-outcome classifier are exercised without adding a mock or a test-only production seam.

**Tech Stack:** Rust 2021, `std::process`, Unix executable permissions, Linux `/proc` PID liveness evidence, existing `tempfile` dev dependency.

## Global Constraints

- Never execute a real `boltzcli`, contact `boltzd`, CLN, LN+, or the network.
- Never read or write the production Boltz datadir.
- Preserve default-off `enabled=false` and the independent `ExecutionMode::Armed` gate.
- A timed-out create remains `CreateOutcome::Unknown`; it is never downgraded to a retryable rejection.
- Work only in the isolated Task 54 worktree and do not modify Task 44 scheduler/main/database files.

---

### Task 1: Real process success and deterministic failures

**Files:**
- Create: `crates/revops-boltz/tests/process_fake_executable.rs`
- Modify: `crates/revops-boltz/src/process.rs`

**Interfaces:**
- Consumes: `ProcessBoltzCli::new(BoltzCliProcessConfig)` and `BoltzCli::run(&[&str], u64)`.
- Produces: sandbox integration coverage for the existing public process adapter; no new production API.

- [x] **Step 1: Write failing fake-executable integration tests**

  Add a test helper that writes `#!/bin/sh` plus a supplied body into a `TempDir`, sets mode `0o700`, and builds an enabled adapter whose datadir is another temporary path. Add separate tests for exact `--datadir` and caller-argument propagation with trimmed stdout; stderr-preferred nonzero failure; stdout-fallback nonzero failure; missing executable as `CliError::NotFound`; and simultaneous stdout/stderr larger than a pipe buffer.

- [x] **Step 2: Run the focused test and record RED**

  Run: `cargo test -p revops-boltz --test process_fake_executable -- --nocapture`

  Expected: RED because the source still declares the production subprocess seam intentionally untested and at least one real boundary assertion exposes missing/incorrect behavior.

- [x] **Step 3: Make only the minimal production correction required by the observed RED**

  Keep `base_argv` and the public trait unchanged. Correct only `run_with_timeout` output/error handling if the real fake process demonstrates divergence; do not introduce a generic command-runner abstraction.

- [x] **Step 4: Re-run focused tests GREEN**

  Run: `cargo test -p revops-boltz --test process_fake_executable -- --nocapture`

  Expected: all Task 1 tests pass.

### Task 2: Timeout, reap, and ambiguity proof

**Files:**
- Modify: `crates/revops-boltz/tests/process_fake_executable.rs`
- Modify only if RED requires it: `crates/revops-boltz/src/process.rs`

**Interfaces:**
- Consumes: configured timeout fallback (`run(..., 0)`), explicit override (`run(..., N)`), `commands::execute_loop_in`, `ExecutionMode::Armed`.
- Produces: evidence that the exact fake child is gone when `Timeout` returns and that a real process timeout becomes `ActionOutcome::Executed(CreateOutcome::Unknown { .. })`.

- [x] **Step 1: Write timeout tests before production changes**

  The fake executable writes `$$` to a test-owned PID file and then `exec sleep 30`. Assert configured timeout fallback and explicit override report the effective timeout; after each return, poll `/proc/<pid>` for a bounded interval and assert it is absent. Add an armed `execute_loop_in` test using the same real adapter and assert `Unknown`, including the actual command string.

- [x] **Step 2: Run the focused test and record RED**

  Run: `cargo test -p revops-boltz --test process_fake_executable -- --nocapture`

  Expected: a behavioral failure if timeout reporting, kill/reap, or create classification is not exact; a passing assertion must be mutation-tested in Step 4.

- [x] **Step 3: Implement the minimum timeout correction**

  Preserve the one-second minimum and `kill` followed by `wait`. If the test exposes a leak or wrong timeout value, fix that exact path without adding retries or treating ambiguity as failure.

- [x] **Step 4: Demonstrate revert-discriminating controls**

  Temporarily bypass the `child.wait()` call; show both exact-child PID tests RED, then restore production bytes exactly and rerun GREEN. Preserve the original focused RED proving that the missing-program assertion traverses the real process boundary and discriminates the production correction.

### Task 3: Documentation and repository gates

**Files:**
- Modify: `crates/revops-boltz/src/process.rs` module documentation
- Modify: `crates/revops-boltz/ENTRYPOINTS.md`
- Modify: `crates/revops-boltz/TASK54-REPORT.md`

**Interfaces:**
- Produces: an honest transport-proven claim limited to sandbox execution; live transport and plugin reachability remain separate gaps.

- [x] **Step 1: Replace obsolete no-subprocess-test claims**

  State that real subprocess behavior is tested only with sandbox-owned fake executables, never a real Boltz binary, service, datadir, node, or network. Do not mark plugin registration, budget rails, or live transport complete.

- [x] **Step 2: Record evidence**

  In `TASK54-REPORT.md`, record RED outputs, mutation controls, exact changed files, and the fact that no live boundary was contacted.

- [x] **Step 3: Run all gates**

  Run in order:

  - `cargo test -p revops-boltz`
  - `cargo test --workspace`
  - `cargo test --workspace --release`
  - `cargo fmt --all -- --check`
  - `cargo clippy --workspace --all-targets -- -D warnings`
  - `git diff --check`

  Expected: all green and no warnings.

- [x] **Step 4: Commit one logical checkpoint**

  Stage only the Task 54 plan, process boundary, tests, entrypoint documentation, and report. Commit as `test(boltz): prove real subprocess boundary with fake executable`, verify a clean tree, mark only Hexmem Task 54 `impl`, and notify the Rust verifier with the pinned SHA.
