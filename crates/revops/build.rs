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

use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    if let Ok(pinned) = std::env::var("REVOPS_SOURCE_COMMIT") {
        if !pinned.is_empty() {
            println!("cargo:rustc-env=REVOPS_SOURCE_COMMIT={pinned}");
            println!("cargo:rerun-if-env-changed=REVOPS_SOURCE_COMMIT");
            return;
        }
    }
    println!("cargo:rerun-if-env-changed=REVOPS_SOURCE_COMMIT");

    let Some(commit) = run_git(&["rev-parse", "HEAD"]).filter(|s| !s.is_empty()) else {
        // No git checkout available (e.g. a source tarball build outside a
        // repo). Leave REVOPS_SOURCE_COMMIT unset -- `source_commit()`'s
        // `cargo:<version>` fallback is the reportable-but-never-matching
        // placeholder described above. Re-run only if this script changes;
        // there is no git state to watch.
        println!("cargo:rerun-if-changed=build.rs");
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

    watch_git_refs();
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

/// Best-effort rebuild triggers so a later commit or branch switch is
/// picked up without a full `cargo clean`. Missing paths are harmless --
/// cargo re-runs the build script whenever a watched path does not (yet)
/// exist, which only makes provenance MORE likely to be freshly recomputed,
/// never less.
fn watch_git_refs() {
    if let Some(git_dir) = run_git(&["rev-parse", "--git-dir"]) {
        rerun_if_exists(&Path::new(&git_dir).join("HEAD"));
    }
    // Worktrees keep HEAD private but share refs/heads through the common
    // dir, so branch-tip advances have to be watched there instead.
    if let Some(common_dir) = run_git(&["rev-parse", "--git-common-dir"]) {
        let common_dir = PathBuf::from(common_dir);
        rerun_if_exists(&common_dir.join("packed-refs"));
        if let Some(branch) = run_git(&["rev-parse", "--abbrev-ref", "HEAD"]) {
            if branch != "HEAD" {
                rerun_if_exists(&common_dir.join("refs").join("heads").join(branch));
            }
        }
    }
}

fn rerun_if_exists(path: &Path) {
    println!("cargo:rerun-if-changed={}", path.display());
}
