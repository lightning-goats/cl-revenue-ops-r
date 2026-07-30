#!/usr/bin/env python3
"""Task 67c mutation matrix C1-C8.

Each entry mutates ONE production behaviour that a slice claims to pin,
then runs the pinned test. A mutation that does not compile is INVALID,
not killed, and is reported as such. The tree is reverted after every
case; killing tests are already committed (task 67 A10 lesson).
"""
import subprocess, sys, pathlib

ROOT = pathlib.Path("/home/sat/bin/cl-revenue-ops-r/.worktrees/task67c-discovery")

MUTATIONS = [
    ("C1", "discovery groups edges by DESTINATION instead of source",
     "crates/revops/src/discovery_evidence.rs",
     None, None, "discovery_evidence"),
    ("C2a", "scid ':' form is no longer normalized to 'x'",
     "crates/revops/src/discovery_evidence.rs",
     'scid.replace(\':\', "x")',
     "scid.to_string()",
     "discovery_evidence"),
    ("C2b", "channel_to_peer is rebuilt from gossip destination again",
     "crates/revops/src/discovery_evidence.rs",
     ".map(|(scid, peer)| (normalize_scid(scid), peer.clone()))",
     ".map(|(scid, _peer)| (normalize_scid(scid), sources.our_node_id.clone()))",
     "discovery_evidence"),
    ("C9", "F71-R1: dest capacities parsed with as_i64 again (drops string msat)",
     "crates/revops/src/enrichment_evidence.rs",
     '.map(|c| parse_msat(c.get("amount_msat").unwrap_or(&Value::Null)))',
     '.filter_map(|c| c.get("amount_msat").and_then(Value::as_i64))',
     "enrichment_evidence"),
    ("C10", "discovery msat helper reverts to as_i64-only",
     "crates/revops/src/discovery_evidence.rs",
     "    v.map(revops_core::msat::parse_msat).unwrap_or(0)",
     '    match v {\n        Some(Value::Number(n)) => n.as_i64().unwrap_or(0),\n        _ => 0,\n    }',
     "discovery_evidence"),
    ("C11", "F71-R3: absent outputs array defaults to empty instead of refusing",
     "crates/revops/src/econ_evidence.rs",
     '        .ok_or_else(|| malformed("reply has no outputs array"))?\n        .as_array()\n        .ok_or_else(|| malformed("outputs is not an array"))?;',
     '        .map(|v| v.as_array().cloned().unwrap_or_default())\n        .unwrap_or_default();\n    let outputs = &outputs;',
     "econ_evidence"),
    ("C12", "F71-R3: impossible balance (ours > total) no longer refuses",
     "crates/revops/src/econ_evidence.rs",
     "        if ours > total {",
     "        if false {",
     "econ_evidence"),
    ("C13", "F71-R2: ROC regrows its own margin arithmetic",
     "crates/revops/src/econ_evidence.rs",
     "pub fn calculate_roc(pnl: PnlSummary, total_capacity_sats: i64, window_days: i64) -> RocSummary {",
     "pub fn calculate_roc(pnl: PnlSummary, total_capacity_sats: i64, window_days: i64) -> RocSummary {\n    let _operating_margin_pct = 0.0;",
     "econ_evidence"),
    ("C14", "F71-R6: amount validation dropped, permissive parse fabricates zero",
     "crates/revops/src/econ_evidence.rs",
     "    if !acceptable {",
     "    if false {",
     "econ_evidence"),
    ("C15", "F71-R7: negatives/u64-overflow accepted again",
     "crates/revops/src/econ_evidence.rs",
     "        Value::Number(n) => n.as_u64().is_some_and(|u| i64::try_from(u).is_ok()),",
     "        Value::Number(_) => true,",
     "econ_evidence"),
    ("C16", "F71-R10: winner EV no longer repriced per loser capacity",
     "crates/revops-capital/src/planner/ev.rs",
     "        inputs.channel_size_sats = loser_capacity_sats;",
     "        inputs.channel_size_sats = 1_000_000;",
     "planner_recycle"),
    ("C17", "F71-R14: winner derivation deleted, redeployment templates always empty",
     "crates/revops/src/capital_producers.rs",
     "    let winners = identify_winners(&sources.winner_channels);",
     "    let winners = identify_winners(&[]);",
     "capital_producers"),
    ("C18", "F71-R12(b): malformed listnodes nodes reads as an empty measured set",
     "crates/revops/src/capital_producers.rs",
     '        if !n.get("nodeid").is_some_and(Value::is_string) {',
     '        if false {',
     "capital_producers"),
    ("C19", "F71-R13: bundle carries a DIFFERENT discovery instance than discover_peers used",
     "crates/revops/src/capital_producers.rs",
     "        discovery: sources.discovery,",
     "        discovery: Default::default(),",
     "capital_producers"),
    ("C20", "F71-R15: bundle carries a DIFFERENT winner snapshot than it derived from",
     "crates/revops/src/capital_producers.rs",
     "        winner_channels: sources.winner_channels,",
     "        winner_channels: Vec::new(),",
     "capital_producers"),
    ("C21", "F71-R4: capital efficiency forced back to None (fallback strategy)",
     "crates/revops/src/discovery_evidence.rs",
     "        neighbor_capital_efficiency: sources.neighbor_capital_efficiency,",
     "        neighbor_capital_efficiency: None,",
     "discovery_evidence"),
    ("C22", "F71-R4: windowed blend applied even when a signal is missing",
     "crates/revops/src/capital_efficiency.rs",
     "                windowed.clear();\n                break;",
     "                continue;",
     "capital_efficiency"),
    ("C23", "F71-R4: percentile ties get distinct ranks by sort order",
     "crates/revops/src/capital_efficiency.rs",
     "        let avg_rank = ((index + end) as f64 / 2.0) / denominator;",
     "        let avg_rank = index as f64 / denominator;",
     "capital_efficiency"),
    ("C24", "F71-R9: channel row fields default instead of refusing",
     "crates/revops/src/capital_evidence.rs",
     '        let peer_id = required_str(channel, "peer_id", &context).map_err(malformed)?;',
     '        let peer_id = channel.get("peer_id").and_then(Value::as_str).unwrap_or_default().to_string();',
     "capital_evidence"),
    ("C25", "F71-R9: impossible to_us_msat > total_msat no longer refuses",
     "crates/revops/src/capital_evidence.rs",
     "        if to_us_msat > total_msat {",
     "        if false {",
     "capital_evidence"),
    ("C3", "cold-start uptime becomes 0.0 instead of 100.0",
     "crates/revops/src/enrichment_evidence.rs",
     "    if prior.is_none() && in_window.is_empty() {\n        return 100.0;\n    }",
     "    if prior.is_none() && in_window.is_empty() {\n        return 0.0;\n    }",
     "enrichment_evidence"),
    ("C4", "inbound fee accepts a single sample",
     "crates/revops/src/enrichment_evidence.rs",
     "pub const INBOUND_FEE_MIN_SAMPLES: usize = 3;",
     "pub const INBOUND_FEE_MIN_SAMPLES: usize = 1;",
     "enrichment_evidence"),
    ("C5", "observed node daily ppm returns Some(0.0) instead of None",
     "crates/revops/src/open_ev_evidence.rs",
     "    if rates.is_empty() {\n        return None;\n    }",
     "    if rates.is_empty() {\n        return Some(0.0);\n    }",
     "open_ev_evidence"),
    ("C6", "close cost falls back to the OPENING feerate (undoes py audit E-4.6)",
     "crates/revops/src/open_ev_evidence.rs",
     'match perkb(feerates, "mutual_close").or_else(|| perkb(feerates, "unilateral_close"))',
     'match perkb(feerates, "opening")',
     "open_ev_evidence"),
    ("C7", "failed policy source becomes an EMPTY set (undoes py audit F1)",
     "crates/revops/src/recycle_evidence.rs",
     "        PolicySource::Unavailable(_) => None,",
     "        PolicySource::Unavailable(_) => Some(BTreeSet::new()),",
     "recycle_evidence"),
    ("C8", "a wired gap field silently reverts to empty",
     "crates/revops/src/capital_evidence.rs",
     "        dual_fund_peers: open_side.dual_fund_peers,",
     "        dual_fund_peers: Default::default(),",
     "capital_evidence"),
]

# C1 needs a bespoke edit; resolve it against the real source.
def c1_patch(text):
    old = 'ch.get("source").and_then(Value::as_str)'
    new = 'ch.get("destination").and_then(Value::as_str)'
    return text.replace(old, new, 1) if old in text else None

def pkg(testname):
    return "revops-capital" if testname in ("planner_recycle", "planner_ev") else "revops"

def run(cmd):
    return subprocess.run(cmd, cwd=ROOT, shell=True,
                          capture_output=True, text=True)

def revert(snapshots):
    """RC71-1: restore EXACTLY what we touched, by content. `git checkout --`
    cannot restore an untracked file and destroys uncommitted tracked edits,
    so it is never used here."""
    for path, original in snapshots.items():
        pathlib.Path(path).write_text(original)

# HARNESS GUARD. `git checkout -- crates/` cannot revert an UNTRACKED file,
# so a mutation applied to a new module stays applied; and it DESTROYS
# uncommitted work on tracked files. On 2026-07-30 that combination silently
# reverted an hour of R5/R11/R12/R13 edits while leaving three mutations
# baked into the new producer module. The matrix is only meaningful against a
# clean, fully-committed tree -- refuse otherwise.
dirty = run("git status --porcelain").stdout.strip()
if dirty:
    print("REFUSING: working tree is not clean. Commit first.\n" + dirty)
    sys.exit(2)

results = []
for mid, desc, relpath, old, new, testname in MUTATIONS:
    p = ROOT / relpath
    src = p.read_text()
    snapshots = {str(p): src}
    if mid == "C1":
        mutated = c1_patch(src)
    else:
        mutated = src.replace(old, new, 1) if old in src else None

    if mutated is None or mutated == src:
        results.append((mid, "INVALID(anchor-not-found)", desc))
        revert(snapshots)
        continue

    p.write_text(mutated)
    # Compile first: a mutation that does not build is INVALID, not killed.
    build = run(f"cargo test -p {pkg(testname)} --test {testname} --no-run")
    if build.returncode != 0:
        results.append((mid, "INVALID(does-not-compile)", desc))
        revert(snapshots)
        continue

    # NOT chained with && -- this command is EXPECTED to fail (lessons:728).
    res = run(f"cargo test -p {pkg(testname)} --test {testname}")
    verdict = "KILLED" if res.returncode != 0 else "SURVIVED"
    results.append((mid, verdict, desc))
    revert(snapshots)

print("\n=== TASK 67C MUTATION MATRIX ===")
for mid, verdict, desc in results:
    print(f"{mid}  {verdict:28s}  {desc}")
survived = [r for r in results if r[1] == "SURVIVED"]
invalid = [r for r in results if r[1].startswith("INVALID")]
print(f"\nkilled={len(results)-len(survived)-len(invalid)} "
      f"survived={len(survived)} invalid={len(invalid)}")
sys.exit(1 if survived or invalid else 0)
