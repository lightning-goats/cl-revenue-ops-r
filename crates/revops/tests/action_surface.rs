//! Task 10 Step 4: structural action allowlist. Replaces the removed
//! `tests/fee_scheduler.rs::no_setchannel_symbol_in_crate` guard (which
//! only scanned this one crate -- see that file's own historical note)
//! with a workspace-wide scan: Task 9's `crate::fee_execution` module is
//! now the ONE sanctioned call site for the literal broadcast RPC method
//! name, and this test recursively confirms the literal appears nowhere
//! else in non-test Rust source, workspace-wide.
//!
//! Reads the source tree relative to `CARGO_MANIFEST_DIR` (this crate's
//! own manifest directory, an absolute path baked in at compile time), so
//! this test's result never depends on the process's current working
//! directory.

use std::path::{Path, PathBuf};

/// Files (workspace-relative, forward-slash-separated) allowed to contain
/// the literal `setchannel`:
///
/// - `crates/revops/src/fee_execution.rs` -- the one guarded action call
///   site (`ClnFeeBroadcaster::attempt_send`), behind the
///   `crate::fee_mode::LiveMode` capability every other module in this
///   workspace is structurally unable to construct or hold.
/// - `crates/revops-fees/src/execution.rs` -- the typed serializer that
///   defines and validates `SetChannelRequest` (the exact wire params the
///   guarded call site sends); its doc comments and validation-error
///   strings legitimately name the CLN RPC method it serializes for.
const ALLOWED_FILES: &[&str] = &[
    "crates/revops/src/fee_execution.rs",
    "crates/revops-fees/src/execution.rs",
];

/// Files (workspace-relative) exempt from the `ClnFeeBroadcaster` mention
/// check: the type's own defining module, and `main.rs`, which is the
/// ONLY legitimate construction site (the live-authority mode gate) per
/// Task 10's wiring.
const CLN_FEE_BROADCASTER_ALLOWED_FILES: &[&str] = &[
    "crates/revops/src/fee_execution.rs",
    "crates/revops/src/main.rs",
];

/// This crate's own manifest directory (`crates/revops`), two levels
/// below the workspace root -- `CARGO_MANIFEST_DIR` is an absolute path
/// injected at compile time, so this never depends on the process's
/// current working directory.
fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root canonicalizes from CARGO_MANIFEST_DIR")
}

/// Every `.rs` file under every workspace crate's `src/` tree -- "non-test
/// Rust sources": a crate's `tests/` integration-test directory (and any
/// `benches`/`examples`, though none exist today) is never walked, only
/// `src/`.
fn non_test_rust_sources(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let crates_dir = root.join("crates");
    let mut crate_dirs: Vec<PathBuf> = std::fs::read_dir(&crates_dir)
        .unwrap_or_else(|e| panic!("read {}: {e}", crates_dir.display()))
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|p| p.is_dir())
        .collect();
    crate_dirs.sort();
    for crate_dir in crate_dirs {
        let src_dir = crate_dir.join("src");
        if src_dir.is_dir() {
            walk_rs_files(&src_dir, &mut out);
        }
    }
    out
}

fn walk_rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let mut entries: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("read dir {}: {e}", dir.display()))
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .collect();
    entries.sort();
    for path in entries {
        if path.is_dir() {
            walk_rs_files(&path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            out.push(path);
        }
    }
}

/// `path` (absolute, under `root`) as a workspace-relative,
/// forward-slash-separated string (stable across the test running on any
/// platform path-separator convention, and directly comparable against
/// the literal strings in [`ALLOWED_FILES`]).
fn relative_to(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or_else(|_| {
            panic!(
                "{} is not under workspace root {}",
                path.display(),
                root.display()
            )
        })
        .to_string_lossy()
        .replace('\\', "/")
}

/// The core allowlist assertion: the literal `setchannel` (case-
/// insensitive, so `SetChannel`/`SETCHANNEL` can't sneak past a
/// case-sensitive scan) appears in non-test Rust source ONLY in
/// [`ALLOWED_FILES`].
#[test]
fn setchannel_literal_confined_to_the_allowlist() {
    let root = workspace_root();
    let mut violations = Vec::new();
    for path in non_test_rust_sources(&root) {
        let rel = relative_to(&root, &path);
        if ALLOWED_FILES.contains(&rel.as_str()) {
            continue;
        }
        let contents = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        if contents.to_ascii_lowercase().contains("setchannel") {
            violations.push(rel);
        }
    }
    assert!(
        violations.is_empty(),
        "the literal 'setchannel' must appear only in {ALLOWED_FILES:?} -- found it (also) in: \
         {violations:?}"
    );
}

/// Shadow/autonomous construction never mentions `ClnFeeBroadcaster` --
/// the type with the ONE call site above. If this type's name shows up
/// anywhere outside its own module and `main.rs`'s live-authority gate,
/// something (most likely the scheduler/shadow construction graph) has
/// started threading a mutation-capable object through a path that must
/// stay structurally connection-free.
#[test]
fn shadow_constructors_never_mention_cln_fee_broadcaster() {
    let root = workspace_root();
    let mut violations = Vec::new();
    for path in non_test_rust_sources(&root) {
        let rel = relative_to(&root, &path);
        if CLN_FEE_BROADCASTER_ALLOWED_FILES.contains(&rel.as_str()) {
            continue;
        }
        let contents = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        if contents.contains("ClnFeeBroadcaster") {
            violations.push(rel);
        }
    }
    assert!(
        violations.is_empty(),
        "ClnFeeBroadcaster must not be mentioned outside {CLN_FEE_BROADCASTER_ALLOWED_FILES:?} \
         (every shadow/autonomous construction path must stay capability-free) -- found it in: \
         {violations:?}"
    );
}

/// Sanity check on the scan itself: it must actually find files (a scan
/// that silently walked zero files would make both assertions above
/// vacuously true).
#[test]
fn scan_finds_a_realistic_number_of_source_files() {
    let root = workspace_root();
    let files = non_test_rust_sources(&root);
    assert!(
        files.len() > 20,
        "expected the workspace-wide src scan to find dozens of files, found {}: is \
         workspace_root() resolving correctly?",
        files.len()
    );
    assert!(
        files
            .iter()
            .any(|p| relative_to(&root, p) == "crates/revops/src/fee_execution.rs"),
        "scan must include the one guarded call site's own file"
    );
}
