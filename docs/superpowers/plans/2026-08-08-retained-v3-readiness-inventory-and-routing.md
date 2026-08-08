# Retained-v3 Readiness Inventory and Routing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce a deterministic, machine-verifiable retained-v3 cutover-readiness baseline and replace stale whole-plugin task routing without creating Rust mutation authority.

**Architecture:** Extend the existing pinned Python-to-Rust inventory generator rather than create a second tracker. The generated inventory computes typed blockers for the exact 39 RPCs, five loops, and 17 retained external boundaries; the active checklist summarizes that artifact and marks old 69-RPC/LN+/Boltz material historical. After the repository evidence is reviewed and published, correct the Hexmem task graph while keeping an independent Python tier-1 review gate.

**Tech Stack:** Python 3 standard library (`ast`, `argparse`, `json`), pytest, canonical generated JSON, Markdown, Git, Hexmem MCP, Codex Security diff scan.

## Global Constraints

- Pin Python source commit `a5c2e2f65019df5cefe4e1261b7de2823a03e448`: exactly 39 retained RPCs, 71 options, five loops, and 17 external boundary classes.
- Retired authority remains absent: Boltz, LN+, CapacityPlanner, automatic channel open/close, planner defibrillation, and their RPCs, options, loops, transports, and capabilities.
- Retain reporting, fee control, profitability, policy/configuration, capex/budget accounting, and budget-constrained ordinary circular rebalancing.
- Python remains sole production mutation authority. Do not construct a Rust live capability, issue/consume an arm, stop Python, deploy, contact `lnnode`, or invoke a CLN action RPC.
- No Sling, Hive, Mycelium, fleet coordinator, external coordinator, or advisory authorization path may return.
- Generate `fixtures/port/plugin_inventory.json` only with `tools/port/gen_plugin_inventory.py`; never hand-edit it.
- Missing, malformed, stale, ambiguous, or unreviewed evidence produces a typed blocker and non-ready verdict.
- Tests invoke no `revenue-*` action RPC, CLN mutation RPC, network service, or production database.
- Run one compiler/test process at a time and verify no task-owned `cargo`, `rustc`, `pytest`, or security worker remains after gates.

---

## File Map

- Modify `tools/port/gen_plugin_inventory.py`: boundary requirements, deterministic readiness, schema v2, fail-closed CLI gate.
- Modify `tools/tests/test_gen_plugin_inventory.py`: schema/count/blocker/determinism/exit-code contracts.
- Regenerate `fixtures/port/plugin_inventory.json`: machine-readable truth.
- Modify `docs/port/PARITY-CHECKLIST.md`: one active retained-v3 section; old scope clearly historical.
- Modify `docs/superpowers/specs/2026-08-08-retained-v3-whole-plugin-cutover-design.md`: operator-approved status.
- Correct Hexmem tasks 69, 80, and 88 and create retained-only replacements after repository evidence is published.

### Task 1: Define the Readiness Contract RED-First

**Files:**
- Modify: `tools/tests/test_gen_plugin_inventory.py`
- Test: `tools/tests/test_gen_plugin_inventory.py`

**Interfaces:**
- Consumes: `generated_inventory() -> dict[str, Any]` and the existing generator CLI.
- Produces: executable contract for `inventory["readiness"]`, boundary `requirement`, schema versions, blocker ordering, and CLI exit `3`.

- [ ] **Step 1: Add the exact baseline test**

```python
def test_retained_v3_readiness_is_exact_and_fails_closed():
    inventory = generated_inventory()["fixtures/port/plugin_inventory.json"]
    readiness = inventory["readiness"]
    assert inventory["schema_version"] == 2
    assert readiness["schema_version"] == 1
    assert readiness["canonical_rpcs"] == {
        "total": 39, "full": 7, "reviewed_full": 4, "promotion_ready": 0,
    }
    assert readiness["retained_loops"] == {
        "total": 5, "effective": 5, "reviewed": 1, "soaked": 1,
    }
    assert readiness["retained_external_boundaries"] == {
        "total": 17, "transport_proven": 2, "missing": 15,
    }
    assert readiness["promotion_ready"] is False
    assert readiness["blockers"] == sorted(
        readiness["blockers"], key=lambda row: (row["kind"], row["id"])
    )
```

- [ ] **Step 2: Add boundary and blocker tests**

```python
def test_all_pinned_external_boundaries_are_required_by_retained_v3():
    inventory = generated_inventory()["fixtures/port/plugin_inventory.json"]
    assert len(inventory["external_boundaries"]) == 17
    assert {row["requirement"] for row in inventory["external_boundaries"]} == {
        "retained_required"
    }
    assert not {
        "boltzcli", "close", "connect", "fundchannel", "lnplus_https",
        "pay", "signmessage",
    } & {row["id"] for row in inventory["external_boundaries"]}


def test_readiness_blockers_name_real_missing_evidence():
    readiness = generated_inventory()["fixtures/port/plugin_inventory.json"]["readiness"]
    blockers = {(row["kind"], row["id"]) for row in readiness["blockers"]}
    assert ("rpc_effective", "revenue-config") in blockers
    assert ("rpc_review", "revenue-config") in blockers
    assert ("loop_review", "rebalance-check") in blockers
    assert ("loop_soak", "rebalance-check") in blockers
    assert ("boundary_transport", "sendpay_waitsendpay") in blockers
    assert ("release_evidence", "exact_binary_72h") in blockers
```

- [ ] **Step 3: Add the fail-closed CLI test**

```python
def test_require_retained_v3_ready_exits_three_with_typed_blockers():
    completed = subprocess.run(
        [sys.executable, str(SCRIPT), "--python-repo", str(PYTHON_REPO),
         "--python-commit", PYTHON_COMMIT, "--repo-root", str(ROOT),
         "--check", "--require-retained-v3-ready"],
        check=False, capture_output=True, text=True,
    )
    assert completed.returncode == 3
    assert '"promotion_ready": false' in completed.stderr
    assert '"exact_binary_72h"' in completed.stderr
```

- [ ] **Step 4: Verify RED**

Run:

```bash
pytest -q \
  tools/tests/test_gen_plugin_inventory.py::test_retained_v3_readiness_is_exact_and_fails_closed \
  tools/tests/test_gen_plugin_inventory.py::test_all_pinned_external_boundaries_are_required_by_retained_v3 \
  tools/tests/test_gen_plugin_inventory.py::test_readiness_blockers_name_real_missing_evidence \
  tools/tests/test_gen_plugin_inventory.py::test_require_retained_v3_ready_exits_three_with_typed_blockers
```

Expected: four failures because readiness, requirement, and the CLI option do not exist.

- [ ] **Step 5: Commit the RED contract**

```bash
git add tools/tests/test_gen_plugin_inventory.py
git commit -m "test: define retained v3 readiness contract"
```

### Task 2: Implement Deterministic Readiness and CLI Semantics

**Files:**
- Modify: `tools/port/gen_plugin_inventory.py`
- Test: `tools/tests/test_gen_plugin_inventory.py`

**Interfaces:**
- Consumes: `rpc_state()`, `loop_state()`, `external_boundaries()`, inventory dictionary.
- Produces: `readiness_summary(inventory: dict[str, Any]) -> dict[str, Any]`, boundary `requirement`, CLI `--require-retained-v3-ready`, and exit `3` only for current-but-not-ready artifacts.

- [ ] **Step 1: Declare each pinned boundary retained**

```python
def boundary(
    boundary_id: str,
    evidence: list[dict[str, Any]],
    owner_task: str,
    rust_adapter: str | None = None,
    rust_transport: str = "missing",
    requirement: str = "retained_required",
) -> dict[str, Any]:
    return {
        "id": boundary_id,
        "requirement": requirement,
        "python_evidence": evidence,
        "rust_adapter": rust_adapter,
        "rust_transport": rust_transport,
        "owner_task": owner_task,
    }
```

- [ ] **Step 2: Add pure readiness helpers before `generate()`**

```python
def _blocker(kind: str, item_id: str, actual: str, required: str) -> dict[str, str]:
    return {"kind": kind, "id": item_id, "actual": actual, "required": required}


def readiness_summary(inventory: dict[str, Any]) -> dict[str, Any]:
    rpcs = inventory["python_rpcs"]
    loops = inventory["loops"]
    boundaries = [row for row in inventory["external_boundaries"]
                  if row["requirement"] == "retained_required"]
    blockers: list[dict[str, str]] = []
    for rpc in rpcs:
        state = rpc["state"]
        if state["effective"] != "full":
            blockers.append(_blocker("rpc_effective", rpc["name"], state["effective"], "full"))
        if state["review"] != "passed":
            blockers.append(_blocker("rpc_review", rpc["name"], state["review"], "passed"))
    for loop in loops:
        state = loop["rust_state"]
        if state["effective"] != "full":
            blockers.append(_blocker("loop_effective", loop["name"], state["effective"], "full"))
        if state["review"] != "passed":
            blockers.append(_blocker("loop_review", loop["name"], state["review"], "passed"))
        if state["soak"] != "passed":
            blockers.append(_blocker("loop_soak", loop["name"], state["soak"], "passed"))
    for row in boundaries:
        if row["rust_transport"] != "local_fake_proven":
            blockers.append(_blocker(
                "boundary_transport", row["id"], row["rust_transport"], "local_fake_proven"
            ))
    blockers.append(_blocker("release_evidence", "exact_binary_72h", "missing", "passed"))
    blockers.sort(key=lambda row: (row["kind"], row["id"]))
    full = sum(row["state"]["effective"] == "full" for row in rpcs)
    reviewed_full = sum(
        row["state"]["effective"] == "full" and row["state"]["review"] == "passed"
        for row in rpcs
    )
    return {
        "schema_version": 1,
        "canonical_rpcs": {"total": len(rpcs), "full": full,
                           "reviewed_full": reviewed_full, "promotion_ready": 0},
        "retained_loops": {
            "total": len(loops),
            "effective": sum(row["rust_state"]["effective"] == "full" for row in loops),
            "reviewed": sum(row["rust_state"]["review"] == "passed" for row in loops),
            "soaked": sum(row["rust_state"]["soak"] == "passed" for row in loops),
        },
        "retained_external_boundaries": {
            "total": len(boundaries),
            "transport_proven": sum(row["rust_transport"] == "local_fake_proven" for row in boundaries),
            "missing": sum(row["rust_transport"] == "missing" for row in boundaries),
        },
        "promotion_ready": not blockers,
        "blockers": blockers,
    }
```

The fixed `exact_binary_72h` blocker is intentional: soak is not yet a receipt-bound generator input, so source or old reports cannot imply it. The runway-evidence project later replaces this fixed blocker with exact release evidence.

- [ ] **Step 3: Emit schema v2**

Increment `GENERATOR_VERSION` from `5` to `6`, update its test, set inventory `schema_version` to `2`, and attach the summary only after all existing fields exist:

```python
inventory["readiness"] = readiness_summary(inventory)
```

- [ ] **Step 4: Add distinct CLI exits**

Add:

```python
parser.add_argument("--require-retained-v3-ready", action="store_true")
```

After the stale-artifact return and before `return 0`:

```python
if args.require_retained_v3_ready:
    readiness = artifacts["fixtures/port/plugin_inventory.json"]["readiness"]
    if not readiness["promotion_ready"]:
        print(json.dumps(readiness, sort_keys=True), file=sys.stderr)
        return 3
```

Exit meanings: `1` stale artifacts; `2` invalid source/provenance; `3` current artifacts with cutover blockers.

- [ ] **Step 5: Verify focused GREEN and expected fixture drift**

Run Task 1’s four-test command; expect all pass. Then run:

```bash
pytest -q tools/tests/test_gen_plugin_inventory.py
```

Expected: only `test_checked_in_artifacts_are_exact_generator_output` fails, naming stale generated artifacts.

- [ ] **Step 6: Commit**

```bash
git add tools/port/gen_plugin_inventory.py tools/tests/test_gen_plugin_inventory.py
git commit -m "feat: compute retained v3 readiness blockers"
```

### Task 3: Regenerate Truth and Correct the Active Checklist

**Files:**
- Regenerate: `fixtures/port/plugin_inventory.json`
- Modify: `docs/port/PARITY-CHECKLIST.md`
- Modify: `docs/superpowers/specs/2026-08-08-retained-v3-whole-plugin-cutover-design.md`
- Test: `tools/tests/test_gen_plugin_inventory.py`

**Interfaces:**
- Consumes: schema-v2 generator and `inventory["readiness"]`.
- Produces: canonical JSON and one human-readable current status derived from it.

- [ ] **Step 1: Regenerate generator-owned artifacts**

```bash
python tools/port/gen_plugin_inventory.py \
  --python-repo /home/sat/bin/cl_revenue_ops \
  --python-commit a5c2e2f65019df5cefe4e1261b7de2823a03e448 \
  --repo-root "$PWD"
```

Expected: exit `0`; fixture diff contains only generator/schema/readiness/requirement changes.

- [ ] **Step 2: Add the active retained-v3 status near the checklist top**

```markdown
## Active retained-v3 readiness

The generated source of truth is `fixtures/port/plugin_inventory.json` schema
2. Current baseline: **7/39 full RPC contracts, 4/39 independently reviewed
full contracts, 0/39 promotion-ready; 5/5 loops effective, 1/5 reviewed and
soaked; 2/17 retained external boundaries local-fake proven, 15/17 missing.**
The release is not promotion-ready. The generator's readiness-required check
exits 3 and prints typed blockers until every code, review, transport, and
exact-binary soak gate closes.

Boltz, LN+, CapacityPlanner, automatic channel open/close, and planner
defibrillation are retired. Sections using the former 69-RPC denominator or
those subsystems are historical evidence only and cannot authorize work.
```

Make the current authority link point to `docs/superpowers/specs/2026-08-08-retained-v3-whole-plugin-cutover-design.md`. Replace section 3b's live “Current conclusion” with a historical label: schema-v2 inventory is authoritative and removed subsystem work must not return.

- [ ] **Step 3: Verify generated and human artifacts**

```bash
pytest -q tools/tests/test_gen_plugin_inventory.py
python tools/port/gen_plugin_inventory.py --python-repo /home/sat/bin/cl_revenue_ops \
  --python-commit a5c2e2f65019df5cefe4e1261b7de2823a03e448 --repo-root "$PWD" --check
python tools/port/gen_plugin_inventory.py --python-repo /home/sat/bin/cl_revenue_ops \
  --python-commit a5c2e2f65019df5cefe4e1261b7de2823a03e448 --repo-root "$PWD" \
  --check --require-retained-v3-ready
```

Expected: pytest and plain check exit `0`; readiness-required check exits `3` with typed blockers. Exit `3` is the expected safe baseline.

- [ ] **Step 4: Scan active targets for retired authority/coordinators**

```bash
rg -n "Boltz|LN\+|CapacityPlanner|fundchannel|planner defibrillation|Sling|cl-hive|cl-mycelium" \
  tools/port/gen_plugin_inventory.py fixtures/port/plugin_inventory.json \
  docs/port/PARITY-CHECKLIST.md \
  docs/superpowers/specs/2026-08-08-retained-v3-whole-plugin-cutover-design.md
```

Expected: negative retirement statements or historical sections only; no generated boundary/owner/active target names retired authority or a coordinator.

- [ ] **Step 5: Commit**

```bash
git add fixtures/port/plugin_inventory.json docs/port/PARITY-CHECKLIST.md \
  docs/superpowers/specs/2026-08-08-retained-v3-whole-plugin-cutover-design.md
git commit -m "docs: publish retained v3 readiness baseline"
```

### Task 4: Security and Regression Gates

**Files:**
- Review: all files changed after `c3d8ad448c12f408a8a08b1c710f99e0f29a629f`.
- Generate locally: Codex Security artifacts in the plugin-selected scan directory; do not add unrelated scan output to the product commit.

**Interfaces:**
- Consumes: committed readiness changes.
- Produces: complete diff-scan report, serial green verification, and proof no worker remains.

- [ ] **Step 1: Resolve exact diff**

```bash
git diff --check c3d8ad448c12f408a8a08b1c710f99e0f29a629f..HEAD
git diff --name-only c3d8ad448c12f408a8a08b1c710f99e0f29a629f..HEAD
```

Expected: only plan-listed files; diff check silent.

- [ ] **Step 2: Run Codex Security diff scan**

Use `codex-security:security-diff-scan` for `c3d8ad448c12f408a8a08b1c710f99e0f29a629f..HEAD`. Compile guidance from `SECURITY.md`; threat model repository scope, later phases diff scope. Every changed file needs a coverage receipt. Fix and re-scan any reportable finding before publication.

- [ ] **Step 3: Run serial repository gates**

```bash
pytest -q tools/tests/test_gen_plugin_inventory.py
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --all-targets
```

Expected: each exits `0`; never overlap Cargo processes.

- [ ] **Step 4: Re-run absence checks**

```bash
rg -n "Sling|cl-hive|cl-mycelium|fundchannel|boltzcli|lnplus" \
  crates tools fixtures/port/plugin_inventory.json
```

Expected: no executable coordinator, fundchannel, Boltz, or LN+ transport. Inspect any historical/test-name match individually.

- [ ] **Step 5: Prove no task process remains**

```bash
ps -eo pid,ppid,stat,etimes,pcpu,pmem,comm,args --sort=-pcpu
```

Expected: no task-owned `cargo`, `rustc`, `pytest`, Codex Security, or scan-helper process. Do not stop unrelated node services.

- [ ] **Step 6: Push and verify**

```bash
git push origin agent/retained-v3-cutover-design-20260808
git rev-parse HEAD
git rev-parse @{upstream}
```

Expected: hashes identical; worktree clean.

### Task 5: Correct Hexmem Routing From Published Evidence

**Files:**
- Durable state: Hexmem tasks 69, 80, 88, plus retained-only replacement tasks.
- No repository files.

**Interfaces:**
- Consumes: published hash, security report, schema-v2 blockers, current Hexmem records.
- Produces: non-duplicative tier-1 contracts with distinct owner/verifier and routing observations.

- [ ] **Step 1: Publish a fresh team capacity view**

Follow `/home/sat/.claude/skills/superagent/SKILL.md`: capture Codex, Python, Rust, shared-pool, and verifier reserve as bounded categories or explicit unknown. Deliver the same view to active workers and require acknowledgement. No bare percentages or durable exact quota values.

- [ ] **Step 2: Search before durable writes**

Use `hexmem_search` for `retained v3 readiness inventory`, `Task 69 whole-plugin authority handoff`, `Task 80 pending settlement rebalance`, and `Task 88 Rust authority retirement`. Mark exact existing records helpful; do not duplicate them.

- [ ] **Step 3: Resolve Task 88 through Python review**

Resolve the Task 88 follow-up branch before requesting review. Require `git rev-parse task88-merged-followup` to equal `b26d0d33f336efc30a1d0b378a68ae82848764d7`, run `git diff --check 742b39a5503d93dd65c1b6ed538b2b6dfd666d9a..b26d0d33f336efc30a1d0b378a68ae82848764d7`, and inspect `git branch -r --contains b26d0d33f336efc30a1d0b378a68ae82848764d7`. If no remote branch contains it, push exactly `task88-merged-followup` and verify the remote-tracking branch resolves to that hash; do not merge or deploy it in this task. Then give Python the exact published commit. Python independently verifies retired source/RPC/option/loop/transport absence; retained reports/fees/budgets/ordinary rebalancing; pinned 39/5/17 inventory; no Sling/Hive/Mycelium; no production/action contact. Only Python calls `hexmem_task_verify` with `task_id=88`, `criterion_id="review"`, the independently derived `status`, and evidence-bearing `notes`. Record the current criterion/context inconsistency in those notes; Codex never self-passes.

- [ ] **Step 4: Close or supersede Task 69**

Task 69 has `design=pass`. Python reviews the approved retained-v3 specification at the published commit against receipt/preflight/capability/rollback requirements. On pass, Python marks `review=pass`. If removed-subsystem wording prevents a truthful pass, cancel Task 69 with the superseding design hash and create a new design-review task; never silently reinterpret the old pass.

- [ ] **Step 5: Cancel old Task 80 and create the retained correction task**

Cancel Task 80 noting LN+, Boltz, fundchannel, and close uncertainty are retired. Create:

```text
Title: Port retained rebalance ambiguity and pending-settlement safety into Rust
Context: owner=rust; verifier=python; tier=1
Description: Port af7fc96 item 3: rotating-offset pending_settlement paging; full page plus zero resolved advances, otherwise reset, off-end wraps; over-14-day pending rows emit error visibility without autonomous resolution. Also own the retained ordinary circular-rebalance unknown-outcome contract required by the approved design: a timeout, disconnect, malformed result, or RPC error after sendpay submission preserves the reservation, records ambiguity/quarantine, reconciles through listsendpays before any determinate cleanup, and never blindly retries. Preserve reservation/budget/idempotency semantics and fail closed on malformed, empty, and RPC-error inputs. Explicitly discard af7fc96 item 2 branches for retired fundchannel, close, and Boltz execution. No LN+, Boltz, fundchannel, close, planner, channel lifecycle, Sling, Hive, Mycelium, production contact, or action RPC in tests.
Criteria:
  impl automated: RED-first paging/wrap/reset/age/error tests plus sendpay/waitsendpay timeout/disconnect/malformed/duplicate/restart tests prove reservation retention, reconciliation-before-cleanup, no blind retry, and retired-authority absence; focused and workspace gates pass.
  review review: Python independently verifies af7fc96 paging parity, the approved retained rebalance ambiguity contract, retained-only scope, and no live action.
```

- [ ] **Step 6: Create the next non-live canonical-contract task**

```text
Title: Complete retained-v3 canonical RPC contracts without live authority
Context: owner=rust; verifier=python; tier=1
Description: Close schema-v2 rpc_effective/rpc_review blockers for exactly 39 canonical methods in observer, autonomous-shadow, or canonical-inert modes only. Retired RPCs/transports stay absent. No arm, capability, Python stop, deployment, production contact, or action RPC in tests. Every slice names exact RPCs, adds revert-discriminating contract tests, and preserves malformed/absent/error fail-closed behavior.
Criteria:
  impl automated: exact 39-method contract and generator tests pass with no success-shaped placeholder.
  security automated: complete Codex Security diff coverage and no unresolved reportable finding.
  review review: Python independently verifies Python-v3 parity and no mutation authority.
```

- [ ] **Step 7: Record routing observations**

For Tasks 88, 69, 80, and each replacement, call `hexmem_observation_add` with `category="routing"`, `action_type="task_assignment"`, an `action_summary` naming the task and routing, the actual `outcome`, evidence-bearing `outcome_details`, and `outcome_source="explicit"`. Do not create a generic duplicate lesson.

### Task 6: Final Verification and Handoff

**Files:**
- Review: worktree and Hexmem readback.
- No production files.

**Interfaces:**
- Consumes: Tasks 1-5.
- Produces: truthful readiness status and exact next owner.

- [ ] **Step 1: Verify Git state**

```bash
git status --short --branch
git log -5 --oneline --decorate
git rev-parse HEAD
git rev-parse @{upstream}
```

Expected: clean; intended commits only; local/upstream identical.

- [ ] **Step 2: Read back Hexmem**

Confirm Task 88 is independently reviewed or explicitly failed; Task 69 reviewed or superseded; Task 80 cancelled with retired clauses named; replacement tasks use `owner=rust; verifier=python; tier=1` and review criteria; no task authorizes production/live authority.

- [ ] **Step 3: Re-run the machine verdict**

Run Task 3's readiness-required command. Expected: exit `3` until actual reviewed code/transport/soak evidence closes blockers. Task creation and documentation never reduce readiness.

- [ ] **Step 4: Confirm host idle**

Run Task 4's process command and inspect load. No compiler, test, scan helper, or abandoned task agent may remain.

- [ ] **Step 5: Report in project format**

Report exact files/hashes; tests and security result; no-Sling/Hive/Mycelium; no action RPC/production/deploy/authority change; Python sole-authority compatibility; follow-up risks (32 partial RPCs, four unreviewed/unsoaked loops, 15 missing transports, absent exact-binary 72-hour evidence, unresolved Task 88 items).

Do not call this cutover completion. It creates the truthful baseline and reviewed work queue for later projects.
