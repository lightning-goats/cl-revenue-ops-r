# Shadow Runway Timers and Deployment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Install a bounded, versioned runway controller and two `lightningd` user-level systemd timers that continuously preserve shadow evidence, detect divergence and safety failures, roll up exact daily reports, and keep the candidate on a two-week gate schedule without possessing cutover authority.

**Architecture:** One Python-standard-library controller owns schema, candidate identity, capture rotation, read-only health collection, gate classification, retention, soak state, and atomic reports. A ten-minute watcher performs cheap safety/liveness checks and freezes capture runs before Python's 32-envelope retention can erase evidence. A daily rollup closes any partial window, invokes the deployed strict replay tools, aggregates autonomous Rust evidence, updates a restart-persistent schedule, and emits JSON plus Markdown. systemd supplies timing, locking, and hardening only.

**Tech Stack:** Python 3 standard library and pytest; existing `replay_fee_capture` Rust binary and `tools/replay_fee_capture_window.py`; Core Lightning read-only/status RPCs plus transient capture toggle; systemd user units; SHA-256; atomic filesystem operations.

## Global Constraints

- Work from `docs/superpowers/specs/2026-07-20-rust-fee-cutover-runway-design.md` after the Python status RPC and Rust runway status schema are reviewed.
- Use `superpowers:test-driven-development`; every controller behavior starts with a failing pure or fake-command test.
- Use `superpowers:systematic-debugging` on unexpected timer, capture, report, or live-state results.
- Use `superpowers:verification-before-completion` before commits, staging, timer enablement, and any green/go claim.
- Timers may call only read-only/status RPCs and transiently toggle `revenue-ops-fee-replay-capture-enabled` to close/reopen observational capture. They never call a fee cycle or action RPC.
- Timers cannot create, install, refresh, read, or consume a cutover arm; cannot disable Python fee authority; cannot enable Rust broadcasting; and cannot deploy or patch binaries.
- Shadow requires Python authority `enabled=true` and Rust `observer=true`, `fee-dryrun=true`, `fee-broadcast=false`. Any conflict is red.
- Capture toggles use `setconfig ... transient=true`; persistent `setconfig` is reserved for the manual cutover runbook.
- The controller never triggers `revenue-fee-cycle`, `revenue-wake-all`, `revenue-set-fee`, `setchannel`, payment, rebalance, channel, planner, Boltz, LN+, or on-chain action RPCs.
- Capture rotation occurs immediately after six completed natural cycles and before the existing 32-envelope retention can overwrite the selected window.
- Evidence has a 512-MiB hard cap and must preserve at least 3 GiB free on the shared filesystem. Retention pressure is red; it is not permission to delete the active failure evidence.
- Candidate commit or binary SHA change resets clean soak. Yellow time never counts as green. Any red gate resets promotion.
- Timers report `NO-GO`; they never perform cutover automatically.
- User units run only as `lightningd`. Enabling linger is the one privileged prerequisite and must be verified after logout.
- Commit each green logical unit separately.

---

### Task 1: Define controller schemas, state, and gate semantics

**Files:**

- Create: `tools/cutover_runway.py`
- Create: `tools/tests/test_cutover_runway.py`
- Create: `schemas/cutover_runway_snapshot.v1.schema.json`
- Create: `schemas/cutover_runway_report.v1.schema.json`

- [ ] **Step 1: Add failing argument and schema tests.**

  Pin commands:

  ```text
  cutover_runway.py watch --config <json>
  cutover_runway.py rollup --config <json>
  cutover_runway.py status --config <json>
  cutover_runway.py prune --config <json> --dry-run
  ```

  Unknown/duplicate arguments and missing local files exit 2 with one JSON error on stdout and no traceback. Configuration schema is closed and includes explicit paths, candidate identity sources, timeouts, byte cap, free-space reserve, capture-cycle target, timezone, and schedule dates.

  ```bash
  pytest -q tools/tests/test_cutover_runway.py -k 'args or config or schema'
  ```

  Expected before implementation: import or command failures.

- [ ] **Step 2: Implement immutable configuration and versioned state.**

  ```python
  @dataclass(frozen=True)
  class RunwayConfig:
      lightning_cli: Path
      capture_dir: Path
      evidence_dir: Path
      report_dir: Path
      replay_bin: Path
      replay_window_tool: Path
      rust_binary: Path
      rust_observer_db: Path
      python_db: Path
      lock_path: Path
      evidence_cap_bytes: int = 512 * 1024 * 1024
      min_free_bytes: int = 3 * 1024 * 1024 * 1024
      capture_cycles: int = 6
      command_timeout_seconds: int = 60
      timezone: str = "America/Denver"
  ```

  Persistent state uses schema `revops_cutover_runway_state/v1`, stores candidate commit/hash, clean-soak start, latest snapshot/report IDs, last completed capture sequence, last timer timestamps, evidence inventory, milestone, and `GO`/`NO-GO`. Write through `fsync` + same-directory `os.replace`.

- [ ] **Step 3: Add failing gate-classification tests.**

  Implement `GREEN`, `YELLOW`, `RED` severity ordering and stable gate codes from the approved design. Red resets soak; yellow preserves the prior reset but adds no green duration; candidate change preserves prior evidence and starts a new candidate at zero soak.

  ```python
  def classify_candidate(previous, snapshot, now):
      if snapshot.candidate_id != previous.candidate_id:
          return CandidateState.start(snapshot.candidate_id, now, "candidate_changed")
      if snapshot.has_red:
          return previous.reset(now, snapshot.red_codes)
      return previous.advance_green(now) if snapshot.all_green else previous.pause_yellow(now)
  ```

- [ ] **Step 4: Verify and commit.**

  ```bash
  pytest -q tools/tests/test_cutover_runway.py -k 'args or config or schema or gate or candidate'
  python3 -m py_compile tools/cutover_runway.py
  python3 -m json.tool schemas/cutover_runway_snapshot.v1.schema.json >/dev/null
  python3 -m json.tool schemas/cutover_runway_report.v1.schema.json >/dev/null
  git diff --check
  git add tools/cutover_runway.py tools/tests/test_cutover_runway.py schemas
  git commit -m "feat(runway): define controller state and gates"
  ```

---

### Task 2: Implement safe command execution and live health collection

**Files:**

- Modify: `tools/cutover_runway.py`
- Modify: `tools/tests/test_cutover_runway.py`

- [ ] **Step 1: Add a fake command runner and failing timeout/output tests.**

  Cover argv-only subprocess invocation, bounded output, timeout, nonzero exit, malformed JSON, missing method, and command provenance. Shell execution is forbidden.

  ```python
  def run_json(argv: Sequence[str], timeout: int, max_bytes: int) -> dict[str, Any]:
      completed = subprocess.run(
          list(argv), capture_output=True, check=False, timeout=timeout
      )
      ...
  ```

- [ ] **Step 2: Add failing snapshot tests for every cheap gate.**

  Fake these observations:

  - `lightning-cli getinfo` node identity;
  - `revenue-fee-authority-status` effective Python authority;
  - `revops-fee-runway-status` Rust mode/state/trigger/mempool/governor/ledger/quarantine/mutation data;
  - local source marker and exact installed Rust binary SHA-256;
  - production and observer DB descriptor modes from a narrowly scoped process-fd probe;
  - filesystem bytes and evidence bytes;
  - systemd unit/timer liveness supplied by the service environment or status command.

  Missing/malformed/stale fields are red when safety-relevant and yellow only for explicitly insufficient natural occurrences.

- [ ] **Step 3: Implement read-only snapshot collection.**

  The controller stores compact raw evidence plus normalized gates. It must not use UI status, log silence, or plugin presence as proof of authority. Candidate identity is `(source_commit, sha256(rust_binary))` and must match Rust status.

  Centralize every permitted Lightning command in one closed dispatcher. The
  allowlist contains only `getinfo`, `revenue-fee-authority-status`,
  `revops-fee-runway-status`, implemented read-parity RPCs, and the two exact
  transient capture toggles from Task 3. A method not in the allowlist is an
  input error before a subprocess is started.

- [ ] **Step 4: Add an action-command denylist test.**

  Scan controller argv construction and source. Reject `revenue-fee-cycle`, `revenue-wake-all`, `revenue-set-fee`, `setchannel`, `pay`, `keysend`, `fundchannel`, `close`, `withdraw`, `rebalance`, and cutover-arm paths. Permit only the exact capture `setconfig` command in Task 3.

- [ ] **Step 5: Verify and commit.**

  ```bash
  pytest -q tools/tests/test_cutover_runway.py -k 'command or snapshot or health or denylist'
  python3 -m py_compile tools/cutover_runway.py
  git diff --check
  git add tools/cutover_runway.py tools/tests/test_cutover_runway.py
  git commit -m "feat(runway): collect fail-closed shadow health"
  ```

---

### Task 3: Rotate and freeze natural Python capture windows

**Files:**

- Modify: `tools/cutover_runway.py`
- Modify: `tools/tests/test_cutover_runway.py`

- [ ] **Step 1: Add failing manifest-discovery tests.**

  Discover only `manifest-<run-id>.v0.json` inside the configured capture directory. Validate bounded strict JSON, schema/version, state, queue status, counts, consecutive attempts, safe basenames, and completion count. Symlinks/path escapes are rejected.

- [ ] **Step 2: Add failing rotation-command tests.**

  The only allowed mutations are exactly:

  ```bash
  lightning-cli setconfig \
    config=revenue-ops-fee-replay-capture-enabled val=false transient=true
  lightning-cli setconfig \
    config=revenue-ops-fee-replay-capture-enabled val=true transient=true
  ```

  Assert no rotation calls a fee cycle. The controller waits up to the configured deadline for `state=closed` and `queue_drained=true`; timeout/failure is red and it does not copy an incomplete window as valid.

- [ ] **Step 3: Implement atomic freeze.**

  After six complete natural cycles, disable capture transiently, wait for durable close, copy the manifest and every referenced envelope into a candidate/run-id staging directory using exclusive creation, verify byte counts and SHA-256, fsync files/directory, then atomically rename to `frozen/<candidate>/<run-id>`. Re-enable capture transiently in a bounded `finally` path and verify a new active run appears.

- [ ] **Step 4: Cover delayed watcher and partial daily rotation.**

  If more than six cycles exist, freeze all retained consecutive attempts and mark `rotation_late` yellow or red if sequence loss is possible. Daily rollup may close/freeze a partial window; it labels it insufficient rather than replay-exact and immediately opens the next run.

- [ ] **Step 5: Prove the 32-envelope boundary.**

  Add fixtures at 6, 31, and 32 completed envelopes. At 31/32 without a successful freeze, classification is red. Freezing never deletes the live capture directory; only the Python manager owns its retention.

- [ ] **Step 6: Verify and commit.**

  ```bash
  pytest -q tools/tests/test_cutover_runway.py -k 'manifest or capture or rotate or freeze or envelope'
  python3 -m py_compile tools/cutover_runway.py
  git diff --check
  git add tools/cutover_runway.py tools/tests/test_cutover_runway.py
  git commit -m "feat(runway): freeze natural parity windows"
  ```

---

### Task 4: Implement the ten-minute watcher

**Files:**

- Modify: `tools/cutover_runway.py`
- Modify: `tools/tests/test_cutover_runway.py`

- [ ] **Step 1: Add failing idempotence, lock, and publication tests.**

  A non-blocking `flock` prevents overlap. Repeating the same observation writes a new uniquely identified snapshot but does not duplicate frozen runs or alter prior files. Interrupted writes leave the previous `latest-watch.json` intact.

- [ ] **Step 2: Implement `watch`.**

  The command:

  1. acquires the shared lock or exits with stable `already_running` status;
  2. collects the cheap snapshot;
  3. rotates a six-cycle capture if needed;
  4. classifies gates and candidate continuity;
  5. writes append-only `watch/<UTC timestamp>-<id>.json`;
  6. atomically replaces `latest-watch.json`;
  7. exits 0 for green/yellow and 1 for a hard red safety gate.

- [ ] **Step 3: Keep output rolled up.**

  stdout is exactly one compact JSON summary. Routine green results do not invoke notification tools. systemd journal retains the one-line result; daily Markdown carries the readable aggregation.

- [ ] **Step 4: Verify and commit.**

  ```bash
  pytest -q tools/tests/test_cutover_runway.py -k 'watch or lock or atomic or idempotent'
  python3 tools/cutover_runway.py --help
  git diff --check
  git add tools/cutover_runway.py tools/tests/test_cutover_runway.py
  git commit -m "feat(runway): add ten-minute shadow watcher"
  ```

---

### Task 5: Implement strict daily replay and parity rollup

**Files:**

- Modify: `tools/cutover_runway.py`
- Modify: `tools/tests/test_cutover_runway.py`
- Modify if needed: `tools/replay_fee_capture_window.py`
- Modify if needed: `tools/tests/test_replay_fee_capture_window.py`

- [ ] **Step 1: Add failing replay-wrapper tests.**

  Invoke the existing runner exactly as:

  ```bash
  python3 tools/replay_fee_capture_window.py \
    --manifest <frozen>/manifest-<run-id>.v0.json \
    --capture-dir <frozen> \
    --replay-bin <release>/replay_fee_capture
  ```

  Accept only its one-line strict JSON and exit codes 0 exact, 1 gate failed, 2 input error. Pin commit equality with the candidate and aggregate cycle/evaluation/adjustment/mismatch totals without hiding per-window failures.

- [ ] **Step 2: Add failing autonomous-divergence tests.**

  Compare oracle outcomes with Rust `rust_fee_cycles`, state generations, prepared intents, mempool 24-hour average, triggers, governor, ledger, and restart markers for the same report interval. Oracle mismatch is red. Autonomous divergence is separately quantified and classified by stable policy; missing required autonomous evidence is red.

- [ ] **Step 3: Add configuration and read-RPC parity adapters.**

  Reuse the already-tested diff harnesses in machine-readable mode. Pin exact comparable counts and exclude intentional Rust-only/Python-only surfaces through checked-in allowlists. Harness errors or unexpected exclusions are red.

- [ ] **Step 4: Implement `rollup`.**

  The command closes/freezes a partial active capture, reopens capture, processes every unreported frozen window in order, aggregates watcher samples/gaps, performs parity and state checks, updates soak/milestones, then atomically publishes:

  ```text
  reports/<candidate>/<date>-<report-id>.json
  reports/<candidate>/<date>-<report-id>.md
  reports/latest.json
  reports/latest.md
  ```

  JSON uses `revops_cutover_runway_report/v1`. Markdown is generated only from the validated JSON object.

- [ ] **Step 5: Pin the two-week schedule in tests.**

  Use `America/Denver` and these milestones: Jul 20-22 implementation; Jul 22-25 first shadow; Jul 26 rehearsal 1; Jul 27-29 bug-fix soak; Jul 30 rehearsal 2; Jul 31-Aug 2 frozen 72-hour candidate; Aug 3 manual go/no-go. Late work moves the effective cutover; it never compresses a required 24/72-hour soak.

- [ ] **Step 6: Verify and commit.**

  ```bash
  pytest -q tools/tests/test_replay_fee_capture_window.py
  pytest -q tools/tests/test_cutover_runway.py -k 'replay or rollup or parity or divergence or schedule or report'
  python3 -m py_compile tools/cutover_runway.py tools/replay_fee_capture_window.py
  git diff --check
  git add tools/cutover_runway.py tools/tests/test_cutover_runway.py \
    tools/replay_fee_capture_window.py tools/tests/test_replay_fee_capture_window.py
  git commit -m "feat(runway): publish strict daily parity rollups"
  ```

---

### Task 6: Enforce bounded evidence retention

**Files:**

- Modify: `tools/cutover_runway.py`
- Modify: `tools/tests/test_cutover_runway.py`

- [ ] **Step 1: Add failing retention-selection tests.**

  Given green, yellow, red, reported, unreported, active-candidate, and prior-candidate evidence, assert:

  - never exceed 512 MiB;
  - preserve at least 3 GiB filesystem free;
  - never delete unreported, active, quarantined, or sole failure reproducer evidence;
  - prune oldest reported green windows first;
  - retain JSON/Markdown/checksum identity after envelope pruning;
  - return red when safe pruning cannot restore both limits.

- [ ] **Step 2: Implement deterministic plan/apply pruning.**

  `prune --dry-run` emits the exact ordered candidate list and projected bytes. Apply mode revalidates inode, size, checksum, report reference, and current free space immediately before unlink. It fsyncs affected directories and records every removal in state.

- [ ] **Step 3: Integrate retention after successful report publication only.**

  A failed report never triggers pruning. A red retention result is included in the report and resets soak.

- [ ] **Step 4: Verify and commit.**

  ```bash
  pytest -q tools/tests/test_cutover_runway.py -k 'retention or prune or free_space'
  git diff --check
  git add tools/cutover_runway.py tools/tests/test_cutover_runway.py
  git commit -m "feat(runway): bound retained shadow evidence"
  ```

---

### Task 7: Add hardened `lightningd` user units and installer

**Files:**

- Create: `deploy/systemd/user/revops-shadow-watch.service`
- Create: `deploy/systemd/user/revops-shadow-watch.timer`
- Create: `deploy/systemd/user/revops-shadow-rollup.service`
- Create: `deploy/systemd/user/revops-shadow-rollup.timer`
- Create: `deploy/runway/config.example.json`
- Create: `tools/install_cutover_runway.py`
- Create: `tools/tests/test_install_cutover_runway.py`

- [ ] **Step 1: Add failing unit-contract tests.**

  Parse the unit files and require:

  - `Type=oneshot`, explicit absolute `ExecStart`, `TimeoutStartSec`, `UMask=0077`;
  - `NoNewPrivileges=true`, `PrivateTmp=true`, `ProtectSystem=strict`, `ProtectHome=read-only` with narrow `ReadWritePaths`;
  - no root user, sudo, SSH, arm path, authority control, or broadcast option;
  - watcher `OnCalendar=*:0/10`, rollup a pinned daily local time, randomized delay bounded below ten minutes, and `Persistent=true`;
  - both services use the controller's shared non-blocking lock.

- [ ] **Step 2: Write the user units.**

  Use deployed paths rooted at `/home/lightningd/revops-r-deploy/current` and writable state at `/home/lightningd/revops-r-deploy/runway`. The timer schedule is declarative; all policy stays in the controller/config.

- [ ] **Step 3: Add a checksummed, idempotent user installer.**

  The installer stages into a versioned release directory, verifies supplied SHA-256 manifest, creates the runway layout with mode `0700`, atomically switches `current`, installs units under `/home/lightningd/.config/systemd/user`, and runs only `systemctl --user daemon-reload`. Enabling timers is a separate explicit flag. It cannot edit lightning configuration or create arms.

- [ ] **Step 4: Test against a temporary HOME and fake systemctl.**

  Verify first install, repeated install, checksum mismatch rollback, partial copy failure, unit diff, enable flag, and prior `current` preservation.

- [ ] **Step 5: Verify unit syntax and commit.**

  ```bash
  pytest -q tools/tests/test_install_cutover_runway.py
  systemd-analyze verify \
    deploy/systemd/user/revops-shadow-watch.service \
    deploy/systemd/user/revops-shadow-watch.timer \
    deploy/systemd/user/revops-shadow-rollup.service \
    deploy/systemd/user/revops-shadow-rollup.timer
  python3 -m py_compile tools/install_cutover_runway.py
  git diff --check
  git add deploy tools/install_cutover_runway.py tools/tests/test_install_cutover_runway.py
  git commit -m "feat(runway): add lightningd user timers"
  ```

---

### Task 8: Write the operational runbook and rehearsal schedule

**Files:**

- Create: `docs/runbooks/fee-cutover-runway.md`
- Create: `docs/runbooks/fee-cutover-go-no-go.md`

- [ ] **Step 1: Document prerequisites and exact ownership.**

  Include current `Linger=no`, one-time privileged linger activation, same-filesystem capacity, 512-MiB cap, 3-GiB reserve, `lightningd` user manager verification, artifact manifest, unit paths, logs, status commands, and rollback of timers/controller.

- [ ] **Step 2: Document the runway response policy.**

  Green is rolled up. Yellow creates an engineering review item but no soak credit. Red resets soak, preserves the reproducer, and is `NO-GO`. Timers do not fix or deploy; bug fixes follow TDD, exact release rebuild, redeploy, and 24/72-hour reset rules.

- [ ] **Step 3: Pin both rehearsals.**

  Rehearsal 1 on Jul 26 uses copied production state and fake CLN normal handoff/rollback. Rehearsal 2 on Jul 30 adds restart, stale/early/wrong arm, wrong SHA, Python still authoritative, flush/governor/ledger failures, explicit rejection, ambiguous outcome, quarantine restoration, reconciliation, and rollback.

- [ ] **Step 4: Keep manual cutover commands outside timer assets.**

  The go/no-go runbook may describe the operator sequence, but no automated script contains the persistent Python-authority disable or arm creation. The final commands are filled with exact reviewed option/RPC names and require a fresh operator approval at execution time.

- [ ] **Step 5: Verify and commit.**

  ```bash
  rg -n 'setchannel|revenue-fee-cycle|revenue-wake-all|revenue-set-fee|cutover.arm|fee-authority-enabled.*false' \
    tools deploy/systemd
  git diff --check
  git add docs/runbooks/fee-cutover-runway.md docs/runbooks/fee-cutover-go-no-go.md
  git commit -m "docs: publish fee cutover runway operations"
  ```

  Expected: action terms are absent from executable timer assets except explicit denylist tests; documentation clearly labels manual-only actions.

---

### Task 9: Full local verification, review, and release manifest

**Files:**

- Create: `deploy/runway/RELEASE-MANIFEST.example.json`

- [ ] **Step 1: Run all controller and existing replay tests.**

  ```bash
  pytest -q tools/tests/test_cutover_runway.py
  pytest -q tools/tests/test_install_cutover_runway.py
  pytest -q tools/tests/test_replay_fee_capture_window.py
  python3 -m py_compile \
    tools/cutover_runway.py \
    tools/install_cutover_runway.py \
    tools/replay_fee_capture_window.py
  cargo test --workspace
  cargo test --workspace --release
  cargo clippy --workspace --all-targets -- -D warnings
  cargo fmt --all -- --check
  ```

- [ ] **Step 2: Run pure end-to-end timer simulations.**

  With temporary directories, fake `lightning-cli`, fake Rust status, fixture manifests, and the real replay binary, simulate 36 watcher invocations and three rollups across candidate change, missed timer, partial window, exact replay, mismatch, disk pressure, and recovery. Assert no fake action RPC call.

- [ ] **Step 3: Obtain review and resolve findings with regression tests.**

  Follow `superpowers:requesting-code-review`, rerun every gate, and ensure no unfinished marker remains in executable paths.

- [ ] **Step 4: Build the exact release bundle and manifest.**

  The versioned bundle contains controller, installer, replay-window tool, strict replay binary, Rust plugin binary, unit files, schemas, config, and runbooks. The manifest pins source commit, candidate binary SHA, every file SHA/size/mode, creation time, and schema version. It contains no secrets or arm.

- [ ] **Step 5: Commit and publish.**

  ```bash
  git diff --check
  git add deploy/runway/RELEASE-MANIFEST.example.json
  git commit -m "build(runway): pin deployment manifest"
  git status --short --branch
  git push -u origin codex/stateful-shadow-cutover
  ```

---

### Task 10: Stage, enable, and verify node-local timers

**Files on `lnnode`:**

- `/home/lightningd/revops-r-deploy/releases/<candidate>/`
- `/home/lightningd/revops-r-deploy/current`
- `/home/lightningd/revops-r-deploy/runway/`
- `/home/lightningd/.config/systemd/user/revops-shadow-*.{service,timer}`

- [ ] **Step 1: Recheck live prerequisites immediately before changes.**

  ```bash
  ssh lnnode 'loginctl show-user lightningd -p Linger -p State'
  ssh lnnode 'df -B1 /data /home/lightningd'
  ssh lnnode 'uid=$(id -u lightningd); sudo -u lightningd env XDG_RUNTIME_DIR=/run/user/$uid DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/$uid/bus systemctl --user is-system-running'
  ssh lnnode 'lightning-cli revenue-fee-authority-status'
  ssh lnnode 'lightning-cli revops-fee-runway-status'
  ```

  Expected: Python authority true; Rust shadow/no-broadcast; at least the required free-space reserve plus staging headroom. Stop on drift.

- [ ] **Step 2: Enable lingering with explicit privileged approval.**

  ```bash
  ssh lnnode 'sudo loginctl enable-linger lightningd'
  ssh lnnode 'loginctl show-user lightningd -p Linger'
  ```

  Expected: `Linger=yes`. This is the only root-level change in the timer deployment.

- [ ] **Step 3: Stage and checksum the exact reviewed bundle.**

  Copy to a new candidate release directory, verify every manifest entry as `lightningd`, run installer without `--enable`, and inspect the unit/config diff. Preserve the prior `current` release and checksum as rollback.

- [ ] **Step 4: Run each service once manually before enabling timers.**

  ```bash
  ssh lnnode 'uid=$(id -u lightningd); sudo -u lightningd env XDG_RUNTIME_DIR=/run/user/$uid DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/$uid/bus systemctl --user start revops-shadow-watch.service'
  ssh lnnode 'uid=$(id -u lightningd); sudo -u lightningd env XDG_RUNTIME_DIR=/run/user/$uid DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/$uid/bus systemctl --user status --no-pager revops-shadow-watch.service'
  ssh lnnode 'uid=$(id -u lightningd); sudo -u lightningd env XDG_RUNTIME_DIR=/run/user/$uid DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/$uid/bus systemctl --user start revops-shadow-rollup.service'
  ssh lnnode 'uid=$(id -u lightningd); sudo -u lightningd env XDG_RUNTIME_DIR=/run/user/$uid DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/$uid/bus systemctl --user status --no-pager revops-shadow-rollup.service'
  ```

  Expected: valid reports even if initial natural-occurrence gates are yellow; no hard red safety gate; capture reopened; Python authority still true; Rust mutation count zero.

- [ ] **Step 5: Enable timers as the `lightningd` user.**

  ```bash
  ssh lnnode 'uid=$(id -u lightningd); sudo -u lightningd env XDG_RUNTIME_DIR=/run/user/$uid DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/$uid/bus systemctl --user enable --now revops-shadow-watch.timer revops-shadow-rollup.timer'
  ssh lnnode 'uid=$(id -u lightningd); sudo -u lightningd env XDG_RUNTIME_DIR=/run/user/$uid DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/$uid/bus systemctl --user list-timers --all --no-pager'
  ```

  Expected: both timers loaded/active with future next-run timestamps.

- [ ] **Step 6: Verify persistence after logout without rebooting the node.**

  End the deployment SSH session, reconnect, and verify `Linger=yes`, the `lightningd` user manager is running, both timers remain active, and the next watcher executes. Do not reboot `lnnode` for this deployment.

- [ ] **Step 7: Verify the safety boundary after timer execution.**

  Confirm Python authority is still true, Rust mode remains observer/dry-run/no-broadcast, no arm exists, Rust mutation count remains zero, production DB descriptors are read-only, evidence usage is bounded, capture windows rotate after six natural cycles, and `latest` report links resolve to checksummed regular files.

- [ ] **Step 8: Hand polling over to the timers.**

  Record the next scheduled watcher/rollup times and stop manual polling. Review one compact daily report unless a red service exit requires immediate diagnosis.

## Completion Evidence

Record controller and candidate source commits, complete artifact manifest, installed hashes, linger readback before/after logout, unit contents and enablement state, next-run timestamps, manual watch/rollup outputs, first automatically generated snapshot, authority/mode readbacks, Rust mutation count, DB descriptor modes, disk/evidence usage, capture rotation identity, and rollback release checksum. The final controller state remains `NO-GO` until the separate rehearsal and 72-hour frozen-candidate gates pass and the operator explicitly approves manual cutover.
