#![cfg(unix)]

//! Regression tests for the `REVOPS_SOURCE_COMMIT` provenance stamp
//! (task 41, from lessons learned in Task 30 staging): a release binary
//! built after a docs-only commit kept embedding the PREVIOUS commit until
//! `cargo clean`, because with no `cargo:rerun-if-*` directives cargo only
//! re-runs a build script when a file inside the PACKAGE changes — Git
//! metadata and files outside `crates/revops` are not part of that scan.
//!
//! Each test builds a minimal fixture crate whose `build.rs` is a
//! byte-for-byte copy of the real `crates/revops/build.rs`, inside a
//! throwaway git repository whose package lives in `crates/app/` — so a
//! docs-only change at the repo root mirrors the incident exactly. The
//! target dir lives OUTSIDE the repo so build artifacts never make the
//! fixture repo dirty.

use std::fs;
use std::path::PathBuf;
use std::process::Command;
use tempfile::TempDir;

struct Fixture {
    _tmp: TempDir,
    repo: PathBuf,
    target: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let tmp = TempDir::new().unwrap();
        let repo = tmp.path().join("repo");
        let target = tmp.path().join("target-outside-repo");
        fs::create_dir_all(repo.join("docs")).unwrap();
        fs::create_dir_all(repo.join("crates/app/src")).unwrap();
        fs::create_dir_all(&target).unwrap();

        fs::write(repo.join("docs/README.md"), "docs v1\n").unwrap();
        fs::write(repo.join(".gitignore"), "Cargo.lock\n").unwrap();
        fs::write(
            repo.join("crates/app/Cargo.toml"),
            "[package]\nname = \"app\"\nversion = \"0.0.0\"\nedition = \"2021\"\n\n[workspace]\n",
        )
        .unwrap();
        fs::write(
            repo.join("crates/app/src/main.rs"),
            "fn main() { println!(\"{}\", option_env!(\"REVOPS_SOURCE_COMMIT\").unwrap_or(\"UNSET\")); }\n",
        )
        .unwrap();
        // The system under test: the REAL build script, not a copy that
        // could drift.
        fs::copy(
            concat!(env!("CARGO_MANIFEST_DIR"), "/build.rs"),
            repo.join("crates/app/build.rs"),
        )
        .unwrap();

        let fixture = Fixture {
            _tmp: tmp,
            repo,
            target,
        };
        fixture.git(&["init", "-q", "-b", "main"]);
        fixture.git(&["add", "-A"]);
        fixture.git(&["commit", "-q", "-m", "initial"]);
        // Make every git path the build script watches actually EXIST
        // (`packed-refs` via pack-refs, then a second commit to recreate
        // the loose branch ref), matching a real long-lived repo. A
        // missing watched path makes cargo re-run the script every build,
        // which would let these tests pass for the wrong reason.
        fixture.git(&["pack-refs", "--all"]);
        fs::write(fixture.repo.join("docs/README.md"), "docs v1 baseline\n").unwrap();
        fixture.git(&["add", "-A"]);
        fixture.git(&["commit", "-q", "-m", "baseline"]);
        let git_dir = PathBuf::from(fixture.git(&["rev-parse", "--absolute-git-dir"]));
        for watched in ["HEAD", "index", "packed-refs", "refs/heads/main"] {
            assert!(
                git_dir.join(watched).exists(),
                "fixture must have every watched git path present: {watched}"
            );
        }
        fixture
    }

    fn git(&self, args: &[&str]) -> String {
        let output = Command::new("git")
            .args(args)
            .current_dir(&self.repo)
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_AUTHOR_NAME", "fixture")
            .env("GIT_AUTHOR_EMAIL", "fixture@invalid")
            .env("GIT_COMMITTER_NAME", "fixture")
            .env("GIT_COMMITTER_EMAIL", "fixture@invalid")
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .env_remove("GIT_INDEX_FILE")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap().trim().to_string()
    }

    fn head(&self) -> String {
        self.git(&["rev-parse", "HEAD"])
    }

    /// Plain incremental `cargo build` — deliberately NEVER `cargo clean`:
    /// stale-stamp survival across exactly this call is the defect.
    fn build(&self, pinned_env: Option<&str>) {
        let mut cmd = Command::new(env!("CARGO"));
        cmd.args(["build", "-q", "--manifest-path"])
            .arg(self.repo.join("crates/app/Cargo.toml"))
            .env("CARGO_TARGET_DIR", &self.target)
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .env_remove("GIT_INDEX_FILE")
            .env_remove("REVOPS_SOURCE_COMMIT");
        if let Some(pinned) = pinned_env {
            cmd.env("REVOPS_SOURCE_COMMIT", pinned);
        }
        let output = cmd.output().unwrap();
        assert!(
            output.status.success(),
            "fixture cargo build failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    /// What the binary actually claims as its provenance.
    fn stamp(&self) -> String {
        let output = Command::new(self.target.join("debug/app"))
            .output()
            .unwrap();
        assert!(output.status.success());
        String::from_utf8(output.stdout).unwrap().trim().to_string()
    }
}

#[test]
fn head_advance_by_docs_only_commit_refreshes_stamp_without_clean() {
    let fixture = Fixture::new();
    let first = fixture.head();

    fixture.build(None);
    assert_eq!(fixture.stamp(), first, "clean build must stamp HEAD");

    fs::write(fixture.repo.join("docs/README.md"), "docs v2\n").unwrap();
    fixture.git(&["add", "-A"]);
    fixture.git(&["commit", "-q", "-m", "docs-only"]);
    let second = fixture.head();
    assert_ne!(first, second);

    fixture.build(None);
    assert_eq!(
        fixture.stamp(),
        second,
        "rebuild after a docs-only HEAD advance kept the STALE commit \
         (the Task 30 staging incident: c7158d4 embedded after e7996fb \
         until cargo clean)"
    );
}

#[test]
fn working_tree_edit_after_clean_build_stamps_dirty_without_clean() {
    let fixture = Fixture::new();
    let head = fixture.head();

    fixture.build(None);
    assert_eq!(fixture.stamp(), head, "clean build must stamp HEAD");

    // Dirty the repo OUTSIDE the package directory: no package file
    // changes, so only a genuinely re-run build script can notice.
    fs::write(fixture.repo.join("docs/README.md"), "uncommitted edit\n").unwrap();

    fixture.build(None);
    assert_eq!(
        fixture.stamp(),
        format!("{head}-dirty"),
        "rebuild in a dirty tree must stamp -dirty, not keep claiming a \
         clean commit"
    );
}

#[test]
fn pinned_env_wins_and_unpinning_refreshes_to_git_without_clean() {
    let fixture = Fixture::new();
    let head = fixture.head();

    fixture.build(Some("pinned-by-release-pipeline"));
    assert_eq!(
        fixture.stamp(),
        "pinned-by-release-pipeline",
        "an externally pinned REVOPS_SOURCE_COMMIT must win outright"
    );

    fixture.build(None);
    assert_eq!(
        fixture.stamp(),
        head,
        "unpinning the env var must refresh the stamp back to git \
         provenance without cargo clean"
    );
}
