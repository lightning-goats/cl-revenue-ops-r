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
//! ## Freshness (task 41): forced re-run + Git-metadata watches
//!
//! An earlier revision emitted NO `cargo:rerun-if-*` directives, believing
//! cargo's un-opted-in default was "re-run the build script on every
//! build". It is not: with no directives, cargo re-runs the script only
//! when a file INSIDE THE PACKAGE changes. Git metadata (`.git/`) and
//! everything outside `crates/revops` are not part of that scan, so a
//! docs-only commit advanced HEAD while incremental rebuilds kept
//! embedding the previous commit until `cargo clean` — a stale provenance
//! stamp on a surface whose whole job is binary identity.
//!
//! The fix is layered, and `tests/build_provenance.rs` enforces the
//! end-to-end property (HEAD advance, dirty tree, env unpinning — each
//! must refresh a plain incremental rebuild, no clean):
//!
//! 1. `rerun-if-changed` on a path under `OUT_DIR` that is never created.
//!    Cargo treats a missing watched path as always-out-of-date, so the
//!    script re-runs on EVERY build — which also keeps the `-dirty`
//!    suffix honest for working-tree edits that touch no watched path.
//! 2. `rerun-if-changed` on the Git metadata that determines the stamp
//!    (`HEAD`, the checked-out branch's loose ref, `packed-refs`, the
//!    index; worktree-aware via `--git-common-dir`). Documented cargo
//!    behavior, covering ref/HEAD/index staleness even if (1)'s
//!    missing-path semantics ever changed.
//! 3. `rerun-if-env-changed=REVOPS_SOURCE_COMMIT`, so pinning or
//!    unpinning the release pipeline's override is itself a re-run
//!    trigger.
//!
//! The cost is running this script (a few `git` subprocess calls) once per
//! build — trivial next to what "guaranteed-fresh provenance for a
//! live-authority cutover gate" is worth. The script's OUTPUT is stable
//! for an unchanged tree, so the forced re-run does not cascade into
//! recompiles.

use std::path::PathBuf;
use std::process::Command;

fn main() {
    // Layer 3: react to the release pipeline pinning/unpinning the
    // override.
    println!("cargo:rerun-if-env-changed=REVOPS_SOURCE_COMMIT");

    // Layer 1: a watched path that never exists forces this script to
    // re-run on every build (missing input = always out-of-date).
    if let Ok(out_dir) = std::env::var("OUT_DIR") {
        println!("cargo:rerun-if-changed={out_dir}/revops-provenance-never-created-force-rerun");
    }

    // Layer 2: watch the Git metadata the stamp is derived from.
    emit_git_metadata_watches();

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

/// Watch `HEAD`, the checked-out branch's loose ref file, `packed-refs`,
/// and the index. In a linked worktree `HEAD`/`index` live in the
/// per-worktree git dir while refs and `packed-refs` live in the common
/// dir, hence both lookups. Watching a ref file that does not exist yet
/// (ref only in `packed-refs`) is harmless — it just forces re-runs, which
/// layer 1 does anyway.
fn emit_git_metadata_watches() {
    let Some(git_dir) = run_git(&["rev-parse", "--absolute-git-dir"]).map(PathBuf::from) else {
        return;
    };
    let common_dir = run_git(&["rev-parse", "--git-common-dir"])
        .map(|dir| absolutize(PathBuf::from(dir)))
        .unwrap_or_else(|| git_dir.clone());

    println!("cargo:rerun-if-changed={}", git_dir.join("HEAD").display());
    println!("cargo:rerun-if-changed={}", git_dir.join("index").display());
    println!(
        "cargo:rerun-if-changed={}",
        common_dir.join("packed-refs").display()
    );
    if let Some(branch_ref) = run_git(&["symbolic-ref", "-q", "HEAD"]).filter(|s| !s.is_empty()) {
        println!(
            "cargo:rerun-if-changed={}",
            common_dir.join(branch_ref).display()
        );
    }
}

/// `--git-common-dir` may answer relative to the build script's cwd (the
/// package root); anchor it there so the watch points at a real path.
fn absolutize(path: PathBuf) -> PathBuf {
    if path.is_absolute() {
        return path;
    }
    match std::env::current_dir() {
        Ok(cwd) => cwd.join(path),
        Err(_) => path,
    }
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
