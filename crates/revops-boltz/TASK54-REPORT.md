# Task 54 — ProcessBoltzCli fake-executable proof

## Scope and safety

This checkpoint exercises the real `ProcessBoltzCli` `std::process::Command` boundary exclusively against chmod-executable shell programs created inside `tempfile::TempDir`. It never invokes a real `boltzcli`, contacts `boltzd`, CLN, LN+, or the network, or reads/writes a production datadir. Plugin registration, budget rails, journal persistence, and live transport remain separate gaps.

## RED and GREEN evidence

The first focused run executed eight real-boundary tests. Seven passed; the missing-executable test failed because `CliError::NotFound` contained only `No such file or directory (os error 2)` and discarded the exact attempted program. The minimal production correction includes `program` in that message. The same focused run then passed 8/8.

The suite proves:

- exact `--datadir` and caller argv propagation plus trimmed stdout;
- stderr-preferred and stdout-fallback nonzero failures with exact exit codes;
- exact missing executable identity in `NotFound`;
- simultaneous stdout/stderr larger than a pipe buffer without deadlock;
- configured timeout fallback and explicit timeout override;
- kill and reap of the exact fake child, proved by PID disappearance in `/proc`;
- a real process timeout on an armed create maps to `CreateOutcome::Unknown`, never `Rejected`.

## Mutation evidence

Before mutation, `crates/revops-boltz/src/process.rs` SHA-256 was `d7334343520292e51df1c723e65d2ac450c3fb2f1dfba70a1df8847aec42d959`. Temporarily removing `child.wait()` made both timeout PID tests RED because the killed child survived as an unreaped process. Restoring the line returned the file to the exact same SHA-256 and the focused suite to GREEN.

## Harness race investigation

Normal parallel test execution intermittently returned Linux `ETXTBSY` while executing a just-created temporary shell program. The suspect test passed 5/5 in isolation and the full integration binary passed 3/3 with one test thread; normal parallel execution reproduced the failure in 2/3 runs. `strace` changed the timing and produced 3/3 green runs. The fake-process tests therefore use a test-only, poison-tolerant mutex around each create/execute lifecycle. Production concurrency is unchanged. Five subsequent normal Cargo runs passed 5/5.

## Verification

The completed gate outputs and exact changed-file inventory follow.

Final gates:

- focused fake-executable integration: 8 passed / 0 failed;
- `revops-boltz`: 222 passed / 0 failed;
- workspace debug: 2,216 listed tests, all passed;
- workspace release: 2,216 listed tests, all passed;
- `cargo fmt --all -- --check`: clean;
- `cargo clippy --workspace --all-targets -- -D warnings`: clean;
- `git diff --check`: clean.

Exact changed files:

- `crates/revops-boltz/src/process.rs`
- `crates/revops-boltz/tests/process_fake_executable.rs`
- `crates/revops-boltz/ENTRYPOINTS.md`
- `crates/revops-boltz/TASK54-REPORT.md`
- `docs/superpowers/plans/2026-07-27-boltz-process-fake-executable.md`
