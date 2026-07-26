# `fixtures/cutover/`

Deliberately empty of fixture data.

The fee-cutover rehearsal harness
(`crates/revops/src/bin/rehearse_fee_cutover.rs`) **synthesises everything it
needs at run time, under the `--rehearsal-root` it is given.** There is nothing
to check in here, and that is the point.

## Why no checked-in fixtures

A committed cutover arm would be a committed *capability*. The arm file is the
one-time authorisation to take live fee authority: it binds a node id, a source
commit, a binary hash and a time window, and consuming it is what mints a
`LiveMode` — the only way to construct the broadcaster that can send
`setchannel`. Storing one in the repository would mean:

- a real-looking arm sitting in every clone and CI checkout, and
- a fixture that must be kept in sync with the running binary's hash, so it
  would either rot or tempt someone to relax the identity checks to make it
  pass.

Instead the harness mints arms for a **synthetic node** (`node_id` is
deliberately *not* the real node) with a zeroed commit and binary hash. Such an
arm cannot validate against production even if it escaped: the identity gates
compare against the running process's real values.

## What the harness creates per run

Under `<rehearsal-root>/`:

| Path | Purpose |
|---|---|
| `sources/` | Synthetic Python and Rust SQLite databases, created here — never read from production |
| `copies/` | The copies actually opened. `fs::copy` opens each source read-only |
| `arms/` | Freshly minted synthetic arms, mode `0600` |
| `consumed/` | Where a validated arm is atomically renamed — the nonce-replay ledger |
| `fake-cln.sock` | The fake CLN socket this process binds itself |
| `rollback/` | Individually reversible step markers for the ordered-rollback scenario |

Deliberately **not** named `lightning-rpc`: that string is a production marker
the harness refuses, so the fake must be unmistakable.

## Running it

```bash
cargo test -p revops --test fee_cutover_rehearsal
cargo run -p revops --bin rehearse_fee_cutover -- --help
cargo run -p revops --bin rehearse_fee_cutover -- --list-scenarios
```

Each run emits exactly one versioned JSON object
(`revops_fee_cutover_rehearsal/v1`) on stdout. A scenario that is not
implemented exits non-zero and says so, rather than emitting an outcome that
could be mistaken for a rehearsed pass.
