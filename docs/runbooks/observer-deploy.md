# Observer Deployment Runbook — Rust Shadow Plugin on lnnode

**Scope:** Deploying the Phase 1b `revops` binary as a read-only shadow observer
alongside the production Python `cl-revenue-ops.py` plugin on `lnnode`, for the
expedited Jul-19 comparison window ("field-for-field RPC parity and ingestion
parity vs Python via the diff harness" — `docs/superpowers/specs/2026-07-16-rust-port-design.md`).

**Node facts, verified live on 2026-07-16** (do not assume these carry forward —
re-check anything marked "verify" before a real deploy if time has passed):

| Fact | Value | How verified |
|---|---|---|
| SSH host | `lnnode` | `~/.ssh/config` alias, already in use |
| lightningd user | `lightningd`, `$HOME=/home/lightningd` | `ssh lnnode whoami; echo $HOME` |
| CLN version | v26.06.1 | `lightning-cli --version` |
| `--lightning-dir` | `/data/lightningd` (network `bitcoin`, so effective dir `/data/lightningd/bitcoin`) | `systemctl cat lightningd`, `/data/lightningd/config:6` |
| Global config file | `/data/lightningd/config` (NOT `/data/lightningd/bitcoin/config` — that's the per-network file) | `grep revenue-ops /data/lightningd/config` |
| Production `db_path` | `/data/lightningd/.lightning/revenue_ops.db` | `lightning-cli revenue-config get db_path` → `{"value": "/data/lightningd/.lightning/revenue_ops.db", "version": 82, "classification": "internal", ...}`, cross-checked against `/data/lightningd/config:47` and the file's existence (55MB, actively modified) |
| Python plugin location | `/data/lightningd/plugins/cl_revenue_ops/cl-revenue-ops.py` (a directory-based checkout, not a single-file plugin) | `ps aux \| grep lightningd`, `ls -la /data/lightningd/plugins/` |
| Plugin autoload | No explicit `plugin=`/`important-plugin=` line in config — Python loads via lightningd's default plugin-dir scan of `/data/lightningd/plugins` at daemon startup | `grep plugin /data/lightningd/config` (no match); `lightning-cli listconfigs plugin-dir` (empty = using built-in default) |
| OS / arch | Ubuntu 25.04, x86_64, glibc 2.41 (`ldd (Ubuntu GLIBC 2.41-6ubuntu1.2) 2.41`) | `uname -m`, `cat /etc/os-release`, `ldd --version` |
| `libsqlite3` on target | Present: `/lib/x86_64-linux-gnu/libsqlite3.so.0` | `ldconfig -p \| grep sqlite` |
| Rust toolchain on lnnode | **Not installed** (`which cargo rustc` → nothing) | `ssh lnnode which cargo rustc` |
| `$HOME/.lightning` symlink | `/home/lightningd/.lightning` → `/data/lightningd` (a symlink to the **top-level** data dir, not to the `bitcoin/` network subdir, and not the same node as the real nested `/data/lightningd/.lightning/` directory that holds `revenue_ops.db`) | `ls -la /home/lightningd/.lightning` |
| Disk | `/data` at 92% used, 7.3G free | `df -h /data` |

**Corrections to the plan's working assumptions**, made after checking the
actual repo and lnnode, both flagged in the Phase 1b plan text:

- The plan step 2 assumed dynamic `plugin start` "can't pass CLI options —
  options must be in the config file." **This is false for CLN v26.06.1.**
  `lightning-cli help plugin` on lnnode documents explicitly: *"Additional
  options may be passed to the plugin, but requires all parameters to be
  passed as keyword=value pairs using the `-k`/`--keyword` option"* — with a
  worked example (`lightning-cli -k plugin subcommand=start plugin=... greeting='A crazy'`).
  This is unrelated to whether an option is declared `dynamic` in
  `getmanifest` (that flag governs live `setconfig` after the plugin is
  already running, a separate concept) — start-time keyword args work for any
  option the plugin registers. Section 2 below uses this path as primary.
- The task brief's guessed production path (`/data/lightningd/.lightning/revenue_ops.db`)
  is **confirmed correct** by live `revenue-config get db_path`.

---

## 1. Build

The observer binary is `revops`, produced from the `crates/revops` package
(`cargo build --release -p revops` → `target/release/revops`), pinned to Rust
1.97 (`rust-toolchain.toml`). Rust is **not installed on lnnode** as of this
check, so path (b) is the default; path (a) is documented in case that
changes.

### Path (a) — build on lnnode (only if a toolchain shows up later)

```sh
ssh lnnode 'which cargo rustc'          # re-check; was empty on 2026-07-16
# If present:
ssh lnnode
cd ~/cl-revenue-ops-r                    # wherever the repo is checked out on lnnode
cargo build --release -p revops
```

### Path (b) — cross-... actually same-arch local build + scp (current default)

lnnode is x86_64 Ubuntu 25.04/glibc 2.41 — build on any x86_64 Linux box with
a matching or older glibc (glibc is forward-compatible: a binary built
against an older glibc runs fine against 2.41; the reverse is not
guaranteed). This repo's dev machine is x86_64 Linux, so a plain native
`cargo build --release` is sufficient — no actual cross-compilation toolchain
needed.

```sh
# Locally, in the repo:
rustup show                              # confirm channel matches rust-toolchain.toml (1.97)
cargo build --release -p revops
file target/release/revops               # expect: ELF 64-bit LSB pie executable, x86-64 ...

# Compat check against the target BEFORE shipping:
ldd target/release/revops
# Expect: linux-vdso, libsqlite3.so.0, libgcc_s.so.1, libc.so.6, ld-linux-x86-64.so.2
# (rusqlite is system-linked per this repo's Global Constraint — no `bundled`
# feature — so libsqlite3.so.0 MUST resolve on lnnode; it does, per the node
# facts table above. If `ldd` on your build box shows a NEWER libsqlite3/libc
# soname than lnnode's, or lists a library lnnode's ldconfig doesn't have,
# stop and rebuild on an older base image / lnnode-equivalent container
# rather than guessing.)

scp target/release/revops lnnode:/home/lightningd/revops-r-deploy/revops
ssh lnnode 'chmod +x /home/lightningd/revops-r-deploy/revops && file /home/lightningd/revops-r-deploy/revops'
```

Deploy to `/home/lightningd/revops-r-deploy/` (or any path **outside**
`/data/lightningd/plugins/`), not the default plugin directory. lightningd
scans `/data/lightningd/plugins/` at its own daemon startup with no `plugin=`
directive gating it (see node-facts table) — dropping the binary in there
would make it auto-load on any future `lightningd` restart, defeating the
"shadow observer, started deliberately, stopped deliberately" intent of this
runbook. `lightning-cli plugin start` takes an absolute path, so the binary
does not need to live under the plugin directory at all.

---

## 2. Start (shadow mode)

Shadow mode = both plugins loaded, Python authoritative, Rust observing only.
Do **not** set `REVOPS_CANONICAL_NAMES=1` for this (see §6).

Because `REVOPS_CANONICAL_NAMES` is a process environment variable read at
Rust binary startup (`std::env::var("REVOPS_CANONICAL_NAMES")` in
`crates/revops/src/main.rs`), and `lightning-cli plugin start` has no
mechanism to inject a subprocess environment variable, leaving it **unset**
in lightningd's own environment is what shadow mode already relies on — no
action needed here beyond not exporting it anywhere lightningd's process
tree would inherit it.

Start with explicit options passed as start-time keyword args (verified
supported syntax, `lightning-cli help plugin` on lnnode, §"Corrections" above):

```sh
ssh lnnode
lightning-cli -k plugin subcommand=start \
  plugin=/home/lightningd/revops-r-deploy/revops \
  revops-r-db-path=/data/lightningd/.lightning/revenue_ops.db \
  revops-r-observer-db-path=/data/lightningd/.lightning/revops-r-observer.db
```

Notes on the two paths:

- `revops-r-db-path` — production DB, **opened read-only** by
  `revops_db::actor::spawn_read_only` (Task 1). Use the exact live value
  confirmed above; do not rely on the option's built-in default, which is
  the empty string in shadow mode by design (`crates/revops/src/main.rs`:
  *"in shadow mode ... the default stays '' — no accidental DB probe just
  because this plugin loaded alongside Python"*). If this option is left
  unset, the plugin still starts fine (observer mode, `db: null` in
  `revenue-r-status`) — it just can't do anything DB-backed.
- `revops-r-observer-db-path` — the Rust plugin's own read-write file
  (Task 2's `ingested_forwards`/`peer_connection_events`/
  `channel_closure_events` schema — deliberately **not** a clone of
  production's schema). The registered default is
  `~/.lightning/revops-r-observer.db`, but on lnnode `~/.lightning` is a
  symlink to `/data/lightningd` (the top-level data dir), which is a
  different, more confusing location than production's real
  `/data/lightningd/.lightning/` directory. Pass it explicitly (as above)
  so the observer's own file sits next to `revenue_ops.db` where it's easy
  to find, rather than relying on the symlink-relative default. The code
  refuses (logs and disables ingestion only, does not fail plugin start) if
  this path resolves to the same file as `db-path` — confirm the two paths
  above are in fact distinct, which they are.

If, contrary to the verified man-page behavior, keyword-passing turns out
not to take effect for these two options once actually exercised (worth
confirming — see §3's first check), fall back to putting the values in
`/data/lightningd/config` (the confirmed live global config file, NOT
`/data/lightningd/bitcoin/config`) under a new `# revops-r shadow observer`
section, then `lightning-cli plugin start` with no keyword args. This
requires no lightningd restart — dynamic plugin start reads the config file
alongside CLI keyword args at getmanifest/init time.

---

## 3. Verify

**Spot checks**, immediately after start:

```sh
lightning-cli revenue-r-ping
# expect: {"pong": true, "version": "0.1.0"}

lightning-cli revenue-r-status
# expect: {"status": "running", "version": "0.1.0", "mode": "observer",
#          "db": {"path": "/data/lightningd/.lightning/revenue_ops.db", "tables": <N>}}
# db.tables should be a positive integer close to the schema's known table
# count (fixtures/schema.sql lists ~19 CREATE TABLE statements) -- this is
# the first confirmation the read-only actor actually attached to the real
# production file rather than silently getting an empty/new db.

lightning-cli -k revenue-r-config key=db-path
lightning-cli -k revenue-r-config key=fee-interval
# spot-check a couple of keys against:
lightning-cli revenue-config get fee_interval
```

**Hydration smoke test** (Task 2's own db file, never production):

```sh
ls -la /data/lightningd/.lightning/revops-r-observer.db
# should appear within seconds of plugin start (init_schema runs at spawn)

sqlite3 /data/lightningd/.lightning/revops-r-observer.db \
  "SELECT COUNT(*), MAX(timestamp) FROM ingested_forwards;"
# run again a minute or two later -- COUNT should tick up if live
# forward_event traffic is happening (compare against production's own
# forwarding activity: `lightning-cli listforwards | jq '.forwards | length'`
# rate over the same window is a rough sanity cross-check, not exact parity).
```

**Diff harness — config parity** (run from this repo, not on lnnode):

```sh
python3 tools/diff-harness/diff_config.py --node lnnode --strict
```

Phase 1b's Task 3/4 (typed config values, canonical db-path default; both
already merged per `git log` — `20dc0ed`/`494cc09`) means `--strict` should
now be the default invocation rather than Phase 1a's normalized/string
fallback — pass it explicitly every time from here on. `db-path` is already
in the script's `SKIP_KEYS` (it's deliberately not shadow-registered from
the fixture; the plugin registers its own `revops-r-db-path` under a
different contract, per `register_python_options`'s doc comment in
`crates/revops/src/main.rs`).

**Diff harness — read RPCs.** `tools/diff-harness/diff_read_rpcs.py`
compares `revenue-history` vs `revenue-r-history`, `revenue-report costs`
vs `revenue-r-report report_type=costs`, and `revenue-dashboard` vs
`revenue-r-dashboard` field-by-field, skipping whatever the Rust
response's own `_phase1b_gaps` array declares, plus an ingestion
cross-check (`ingested_forwards` vs Python's `forwards` row count). It
follows `diff_config.py`'s exact ssh + `lightning-cli` + JSON pattern
(same `cli()`/transport/error/mismatch classification, same exit codes
0/1/2). Invoke it as:

```sh
python3 tools/diff-harness/diff_read_rpcs.py --node lnnode
```

(there is no `--strict` flag on this script — `diff_config.py`'s
`--strict` is specific to that script's normalized/string fallback mode,
which `diff_read_rpcs.py` has no equivalent of). Run `--self-test` first
to confirm the harness itself is sound before pointing it at lnnode:

```sh
python3 tools/diff-harness/diff_read_rpcs.py --self-test
```

---

## 4. Jul-19 comparison-window exit checklist

All five must be true before calling the comparison window closed:

- [ ] **(a) Config parity** — `diff_config.py --node lnnode --strict` exits 0
      over the full option surface (119 keys minus `db-path` in `SKIP_KEYS`).
- [ ] **(b) Read-RPC parity** — `diff_read_rpcs.py --node lnnode`
      exits 0 modulo keys listed in each Rust response's own
      `_phase1b_gaps` array (as of this check: `revenue-r-report`'s
      `summary`/`policies`/`peer` types are wholesale gap-marked, and
      `revenue-r-dashboard` gaps `financial_health.tlv_sats`,
      `financial_health.annualized_roc_pct`, `warnings`, `bleeder_count` —
      see `crates/revops/src/rpc_report.rs` and `rpc_dashboard.rs`). This
      also runs the script's own ingestion cross-check (item (c) below),
      so a manual spot-check is a supplementary sanity check, not the only
      way to confirm this.
- [ ] **(c) Ingestion row-count cross-check** — NOT a diff-harness
      *config/RPC* comparison (the two tables' schemas intentionally
      differ), but `diff_read_rpcs.py` does run it automatically (see
      above). Production's `forwards` table stores CLN's `received_time`
      under the column name `timestamp` (`fixtures/schema.sql`'s
      `CREATE TABLE forwards` — there is no `received_time` column on
      that table), so a manual spot-check must query `timestamp`, not
      `received_time`:
      ```sh
      ssh lnnode 'sqlite3 /data/lightningd/.lightning/revenue_ops.db \
        "SELECT COUNT(*) FROM forwards WHERE timestamp >= <window_start>;"'
      ssh lnnode 'sqlite3 /data/lightningd/.lightning/revops-r-observer.db \
        "SELECT COUNT(*) FROM ingested_forwards WHERE timestamp >= <window_start>;"'
      ```
      Row counts should match over the same shared observation window (an
      exact 1:1 dedup-key match, not just a count coincidence, is stronger
      evidence but requires joining on the shared unique-index columns —
      `in_channel, out_channel, in_msat, out_msat, fee_msat, timestamp,
      resolved_time` — across both files if the counts alone leave any doubt).
- [ ] **(d) WAL/cold-start integration tests green** —
      `cargo test -p revops-db --test actor_wal` in CI (Task 1's
      `cold_start_before_writer_fails_gracefully` and
      `reader_sees_only_committed_data_while_writer_holds_open_transaction`).
- [ ] **(e) Python unaffected for one full cycle of each of its 8 loops**
      — manual confirmation, e.g. `lightning-cli revenue-status` before and
      after, watching `cln.log` (`/data/lightningd/cln.log`, ~1.1GB — `tail
      -f`, don't `cat` it) for Python's own cycle-completion log lines with
      the Rust plugin loaded alongside, and confirming no new errors/warnings
      correlate with the Rust plugin's start.

---

## 5. Rollback

```sh
lightning-cli plugin stop /home/lightningd/revops-r-deploy/revops
```

(or the exact path/name `lightning-cli plugin list` reports it under, if
started via a relative name).

**Zero production impact by construction.** The observer never holds write
authority over anything Python depends on:

- `revops-r-db-path` is opened strictly **read-only**
  (`revops_db::open_read_only` under the hood of `spawn_read_only`) — WAL
  mode means the Rust reader coexists safely with Python's writer even
  concurrently (Task 1's own integration tests exercise exactly this).
  Stopping the observer removes a reader; it never had a lock Python was
  waiting on.
- `revops-r-observer-db-path` is a completely separate file the observer
  itself created and owns; stopping the plugin just leaves that file on
  disk (harmless — delete it manually if you want a clean slate for the
  next run, e.g. `rm /data/lightningd/.lightning/revops-r-observer.db`).
- Python was never paused, restarted, or reconfigured to accommodate the
  observer. There is no "revert Python's state" step, because Python's
  state was never touched.

Unlike a fee/rebalance cutover (where reversibility is a real, load-bearing
concern), there is **no reversibility concern here at all** — the observer
has no write authority over any file or in-memory state Python or
lightningd itself relies on.

---

## 6. Risk notes

- **This is a production node with a live production plugin.** `lnnode` runs
  the real revenue-generating Python plugin (`/data/lightningd/plugins/
  cl_revenue_ops/cl-revenue-ops.py`), which stays loaded and authoritative
  throughout this entire exercise. Nothing in this runbook pauses, restarts,
  or reconfigures Python.
- **Never start the observer with `REVOPS_CANONICAL_NAMES=1` while Python is
  loaded.** Canonical mode makes the Rust plugin register the exact same
  option names (`revenue-ops-*`) and RPC method names (`revenue-*`) that
  Python already owns. lightningd's plugin registration rejects a second
  plugin claiming an already-registered option or RPC method name — the
  Rust plugin would fail to load (or, depending on registration order and
  CLN's exact collision handling, could contend with or shadow Python's own
  registrations in a way this design was explicitly built to avoid).
  Canonical mode is for a future full cutover *after* Python is unloaded,
  never for coexistence testing. Shadow mode (`revops-r-*` / `revenue-r-*`,
  the unset-env default) is the only mode this runbook uses.
- **The plugin directory auto-scan is a standing footgun independent of this
  deploy.** `/data/lightningd/plugins/` has no explicit `plugin=` allowlist
  in `/data/lightningd/config` — anything executable dropped there gets
  auto-loaded on the next `lightningd` restart (not on `plugin start`, which
  is explicit and one-shot). Keep the observer binary outside that directory
  (§1) so an unrelated lightningd restart during the comparison window
  doesn't silently promote it to an auto-loaded, unconfigured plugin.
- **`$HOME/.lightning` is a symlink to `/data/lightningd`, not to the network
  subdir or to the real nested `.lightning` directory holding
  `revenue_ops.db`.** Any option relying on `~`-expansion (both the
  Python and Rust default paths use it) resolves through this symlink;
  always pass explicit absolute paths for both `revops-r-db-path` and
  `revops-r-observer-db-path` on this node rather than trusting defaults.
