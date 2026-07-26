//! Embeds the exact Rust source-commit provenance into the compiled binary
//! (Task 7 step 2; see
//! `docs/superpowers/specs/2026-07-20-rust-fee-cutover-runway-design.md`,
//! "Cutover authorizer": a cutover arm binds `(source_commit,
//! sha256(binary))` and must match the RUNNING binary's own identity).
//!
//! `fee_scheduler::source_commit()` reads the value this script bakes in
//! through `option_env!("REVOPS_SOURCE_COMMIT")` at COMPILE time (build
//! scripts only affect the compiled binary via `cargo:rustc-env`, they
//! cannot change the runtime process environment). When this script cannot
//! determine a commit at all, `source_commit()` falls back to its own
//! `cargo:<version>` placeholder.
//!
//! Deliberately no special-cased "unavailable" or "dirty" handling lives in
//! the cutover-arm validator: a `-dirty`-suffixed commit and the
//! `cargo:<version>` placeholder can never equal a real arm's plain,
//! reviewed commit hash, so both are already denied by the ordinary
//! commit-mismatch comparison in `cutover_arm::validate_and_consume`. Both
//! remain fully reportable (visible in status output and local test
//! assertions) — only the *equality with an issued arm* is refused, per the
//! design doc: "Dirty/unavailable provenance is rejected for live mode but
//! remains reportable in local tests."
//!
//! A caller that already pins `REVOPS_SOURCE_COMMIT` in the environment
//! (the release build pipeline staging the exact artifact to be deployed)
//! wins outright — this script never overrides an externally supplied
//! value, it only re-exports it so `option_env!` can see it.
//!
//! ## Deliberately NO `cargo:rerun-if-*` directives (tradeoff, pinned)
//!
//! This script emits no `cargo:rerun-if-changed` or
//! `cargo:rerun-if-env-changed` directive at all. The moment a build
//! script emits ANY such directive, cargo stops treating "always re-run"
//! as the default and instead skips re-running the script unless one of
//! the watched paths/env-vars actually changed. That is exactly wrong for
//! a `-dirty` provenance stamp: editing a tracked source file after a
//! clean build changes `git status --porcelain`'s output but touches none
//! of the paths a plausible watch list would name (`.git/HEAD`,
//! `packed-refs`, a specific ref file) until the NEXT commit or checkout,
//! so a previously-clean build's stale `REVOPS_SOURCE_COMMIT` would keep
//! being embedded into every following binary — silently claiming a clean
//! commit for a binary that no longer matches it. Emitting no directives
//! at all keeps cargo's un-opted-in default: this build script re-runs on
//! EVERY build, so the provenance stamp can never go stale. The cost is
//! literally running this script (a couple of `git` subprocess calls)
//! once per build, which is trivial next to what "guaranteed-fresh
//! provenance for a live-authority cutover gate" is worth.

use std::process::Command;

fn main() {
    if let Ok(pinned) = std::env::var("REVOPS_SOURCE_COMMIT") {
        if !pinned.is_empty() {
            println!("cargo:rustc-env=REVOPS_SOURCE_COMMIT={pinned}");
            return;
        }
    }

    let Some(commit) = run_git(&["rev-parse", "HEAD"]).filter(|s| !s.is_empty()) else {
        // No git checkout available (e.g. a source tarball build outside a
        // repo). Leave REVOPS_SOURCE_COMMIT unset -- `source_commit()`'s
        // `cargo:<version>` fallback is the reportable-but-never-matching
        // placeholder described above.
        return;
    };

    // `git status --porcelain` failing outright (not just reporting no
    // output) is itself ambiguous provenance -- treat it as dirty rather
    // than silently claiming a clean commit.
    let dirty = run_git(&["status", "--porcelain"])
        .map(|out| !out.is_empty())
        .unwrap_or(true);

    let value = if dirty {
        format!("{commit}-dirty")
    } else {
        commit
    };
    println!("cargo:rustc-env=REVOPS_SOURCE_COMMIT={value}");
}

fn run_git(args: &[&str]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout)
        .ok()
        .map(|s| s.trim().to_string())
}
