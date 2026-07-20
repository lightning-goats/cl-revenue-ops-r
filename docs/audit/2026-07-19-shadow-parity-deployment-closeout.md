# Shadow Parity Deployment Closeout

**Closeout date:** 2026-07-20

**Scope:** Python fee authority, Rust shadow observer, exact replay parity, and deployed artifact provenance on `lnnode`

**Result:** Accepted for continued shadow observation only

## Outcome

Python remains the sole live fee authority. Rust is active only as an observer
with fee execution held in dry-run mode. The final deployed Rust release
replayed six untouched, naturally scheduled Python authority cycles exactly:

- 6 complete cycles;
- 252 evaluated channels and 252 terminal outcomes;
- 54 adjustments;
- 0 replay mismatches;
- 0 failed captures;
- 0 dropped captures.

This proves exact fee-decision parity for the selected window. It does not
authorize Rust cutover or live fee execution.

## Source and verification

- Python authority commit:
  `364db66d500a47d259ca9ec024569d8f0b58fc5d`
- Rust observer commit:
  `7d8e79ec307fd10bd1a775a236148a642a0a506f`
- Both local histories and the remote Python checkout matched `origin/main`
  and were clean at verification.
- Rust gates passed:
  - `cargo fmt --all -- --check`
  - `cargo clippy --workspace --all-targets -- -D warnings`
  - `cargo test --workspace`
  - `cargo test --workspace --release`
  - all three diff-harness self-tests
  - `cargo test -p revops no_setchannel_symbol_in_crate`

The first frozen authority window
(`202314cb42e84ecb9247a782be8a8eac`) exposed two real strict-replay
divergences:

1. A captured effective neighbor-fee median cache hit of `None` was followed
   by an extra Rust gossip read that Python did not perform.
2. High-fee channels with a nonzero prior-forward timestamp omitted Python's
   semantic `flow_ceiling.last_forward_age` clock read.

The Rust fix lazily reads gossip only on the same Python cache-miss and market
paths, and consumes the conditional flow-ceiling clock before Thompson
sampling. Both defects have focused regression tests. The untouched first
window then replayed exactly across 252 evaluated channels and 56 adjustments
before the source was published and redeployed.

## Artifact attestation and rollback

- Local, staged, installed, and running SHA-256:
  `ff648376758b9a97de7642adbf1c258494744c54e33c31a712dcc8c742d1428c`
- Size: 5,794,016 bytes
- Installed path: `/home/lightningd/revops-r-deploy/revops`
- Format: x86-64 ELF PIE, dynamically linked, not stripped
- Resolved runtime dependencies:
  - `libsqlite3.so.0`
  - `libgcc_s.so.1`
  - `libm.so.6`
  - `libc.so.6`
- Previous release SHA-256:
  `09dd2d859b60b337b7335fcdc2b7254dbaf11005d7421e9c302f6f230548ffa0`
- Rollback path:
  `/home/lightningd/revops-r-deploy/revops.rollback.09dd2d859b60b337b7335fcdc2b7254dbaf11005d7421e9c302f6f230548ffa0`
- Previous dry-run journal archived as:
  `/data/lightningd/.lightning/fee_dryrun_journal.pre-fix-20260720T153356Z.jsonl`

The release was staged and attested before stopping Rust. Only the Rust plugin
was stopped for the atomic replacement; Python remained active.

## Live safety posture

- Python plugin:
  `/data/lightningd/plugins/cl_revenue_ops/cl-revenue-ops.py`
  - active and dynamic;
  - health `all_alive=true`;
  - no stalled loops.
- Rust plugin:
  `/home/lightningd/revops-r-deploy/revops`
  - active and dynamic;
  - status `running`;
  - mode `observer`.
- `revops-r-observer=true`, source `pluginstart`.
- `revops-r-fee-dryrun=true`, source `pluginstart`.
- Python capture readback is `false`.
- Rust production DB:
  `/data/lightningd/.lightning/revenue_ops.db`
  - main database descriptors had access mode `O_RDONLY`;
  - SQLite WAL/SHM coordination sidecars were read-write as expected for a
    read-only WAL client.
- Rust writable DB:
  `/data/lightningd/.lightning/revops-r-observer.db`.
- The exact release binary contains no `setchannel` or `revenue-set-fee`
  symbol, and the structural no-`setchannel` test passes.
- The final Rust journal epoch contains 252 rows across six cycle IDs,
  `would_broadcast=true` count 0, and non-null execution-result count 0.
- No validation command invoked a live fee, payment, rebalance, planner,
  Boltz, channel, or on-chain action RPC.

## Final natural capture and exact replay

- Run ID: `0acdea4b7a9d49fa9d66cb944d7421da`
- Started: `2026-07-20T15:35:55.006538+00:00`
- Collection method: dynamic observational capture plus natural scheduling;
  no manual fee cycle
- Manifest:
  - state `closed`;
  - writer health `healthy`;
  - attempted/completed `6/6`;
  - failed/dropped `0/0`;
  - sequences `1..6`;
  - queue drained `true`.
- Per-cycle evaluated channels: 42, 42, 42, 42, 42, 42
- Per-cycle adjustments: 11, 8, 14, 6, 8, 7
- Strict replay status: `exact`
- Strict replay mismatch count: 0
- Frozen evidence:
  `/tmp/revenue_ops_fee_replay-2026-07-20-final-deployed-0acdea4b7a9d49fa9d66cb944d7421da`

Frozen file SHA-256 values:

| File | SHA-256 |
| --- | --- |
| sequence 1 | `cccd299bdf67d618e217afca57611294d63dfd1e0261e9894e81f42331eb2fd7` |
| sequence 2 | `d5697bd4641f0cc72e3a60f730ebb80e0a06a579f1515e884f7ce146cee8b0da` |
| sequence 3 | `3e4b6e90003d33d2da23903b8ae196d15a47367b93bfb7ae111f7eedc3aaf50b` |
| sequence 4 | `8106736cff5e44fcf52db114ebc532a73d941decd8ae6099c279be7176e40dbc` |
| sequence 5 | `c3ccbbbe83682d9fbfdfeba4639d60081595ad782fa923c602e32ef04ce7bc76` |
| sequence 6 | `0a84918dfd41ed680121f0be58570104879ee0eff32aaf3545d90776383017cc` |
| manifest | `98e3d7da838d62273fdcac49a4a143896ea60863114bb57edfb03c2648397a95` |

## Final live parity gates

- Configuration: 118 comparable keys identical, including 12 constructor
  option-surface checks; 0 skipped.
- `revenue-history`: 17 implemented fields identical.
- `revenue-report costs`: 7 fields identical; only volatile `generated_at`
  skipped.
- `revenue-dashboard`: 9 implemented fields identical; 5 explicitly declared
  Phase-1b gaps skipped.
- Forward ingestion over the shared seven-day window: Python 201, Rust 201,
  difference 0 with tolerance 2.

The declared skips are not fee-decision mismatches and are outside the
implemented read-RPC surface.

## Reboot note

`lnnode` had booted at `2026-07-20 03:19:39` before this deployment window.
The earlier host reboot and mount recovery were not caused by this release.
This closeout did not reboot the host or `lightningd`; it restarted only the
Rust dynamic plugin during the checksummed swap.

## Limitation

Parity is proven only for the selected closed replay window and the stated
implemented live comparison surfaces. Rust remains non-authoritative, and
these results do not authorize cutover, fee broadcasts, or any other live
decision execution.
