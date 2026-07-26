//! Tests for Task 7: release-bound cutover-arm validation and consumption
//! (`revops::cutover_arm`). See the module's own doc comment and
//! `docs/superpowers/specs/2026-07-20-rust-fee-cutover-runway-design.md`
//! ("Cutover authorizer") for the full contract this pins.

use revops::cutover_arm::{
    validate_and_consume, CutoverArmDenyReason, RunningIdentity, CUTOVER_ARM_SCHEMA,
    CUTOVER_SUBSYSTEM_FEES,
};
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};

const NODE_ID: &str = "lnnode";
const SOURCE_COMMIT: &str = "7d8e79ec307fd10bd1a775a236148a642a0a506f";
const BINARY_SHA256: &str = "ff648376758b9a97de7642adbf1c258494744c54e33c31a712dcc8c742d1428c";

/// The running identity every "correct" fixture is built to satisfy.
/// `owner_uid` is resolved from a just-created file rather than hardcoded
/// so the suite runs unprivileged under any test user.
fn matching_identity(owner_uid: u32) -> RunningIdentity {
    RunningIdentity {
        node_id: NODE_ID.to_string(),
        subsystem: CUTOVER_SUBSYSTEM_FEES.to_string(),
        source_commit: SOURCE_COMMIT.to_string(),
        binary_sha256: BINARY_SHA256.to_string(),
        owner_uid,
        now: 1_000_000,
    }
}

fn valid_arm_json(nonce: &str) -> String {
    format!(
        r#"{{
            "schema": "{schema}",
            "node_id": "{node}",
            "subsystem": "{subsystem}",
            "source_commit": "{commit}",
            "binary_sha256": "{hash}",
            "not_before": 999900,
            "expires_at": 1000100,
            "nonce": "{nonce}"
        }}"#,
        schema = CUTOVER_ARM_SCHEMA,
        node = NODE_ID,
        subsystem = CUTOVER_SUBSYSTEM_FEES,
        commit = SOURCE_COMMIT,
        hash = BINARY_SHA256,
        nonce = nonce,
    )
}

/// Writes `json` to a fresh file at exactly mode `0600` FROM CREATION (no
/// separate chmod step, so the file is never transiently wider).
fn write_arm(dir: &Path, name: &str, json: &str) -> PathBuf {
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;

    let path = dir.join(name);
    OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(&path)
        .expect("create arm file")
        .write_all(json.as_bytes())
        .expect("write arm json");
    path
}

fn write_arm_with_mode(dir: &Path, name: &str, json: &str, mode: u32) -> PathBuf {
    let path = write_arm(dir, name, json);
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode))
        .expect("relax arm file mode");
    path
}

/// `owner_uid` for a `matching_identity()` in this process: whatever uid
/// this test process itself creates files as.
fn current_owner_uid(sample_path: &Path) -> u32 {
    std::fs::metadata(sample_path)
        .expect("stat sample file for uid")
        .uid()
}

#[test]
fn valid_arm_is_validated_and_consumed() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let arm_path = write_arm(tmp.path(), "arm.json", &valid_arm_json("nonce-1"));
    let owner_uid = current_owner_uid(&arm_path);
    let consumed_dir = tmp.path().join("consumed");

    let identity = matching_identity(owner_uid);
    let session = validate_and_consume(&arm_path, &consumed_dir, &identity)
        .expect("a correctly bound, in-window arm must validate");

    assert_eq!(session.node_id, NODE_ID);
    assert_eq!(session.subsystem, CUTOVER_SUBSYSTEM_FEES);
    assert_eq!(session.source_commit, SOURCE_COMMIT);
    assert_eq!(session.binary_sha256, BINARY_SHA256);
    assert_eq!(session.nonce, "nonce-1");
    assert_eq!(session.consumed_path, consumed_dir.join("nonce-1"));

    // The arm is gone from its original path -- nothing left to re-read.
    assert!(
        std::fs::symlink_metadata(&arm_path).is_err(),
        "consumption must remove the arm from its original path"
    );
}

#[test]
fn atomic_consumption_preserves_bytes_and_mode_moves_original() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let json = valid_arm_json("nonce-atomic");
    let arm_path = write_arm(tmp.path(), "arm.json", &json);
    let owner_uid = current_owner_uid(&arm_path);
    let consumed_dir = tmp.path().join("consumed");

    let identity = matching_identity(owner_uid);
    let session =
        validate_and_consume(&arm_path, &consumed_dir, &identity).expect("valid arm consumes");

    let consumed_bytes = std::fs::read(&session.consumed_path).expect("read consumed arm");
    assert_eq!(
        consumed_bytes,
        json.as_bytes(),
        "rename must preserve the exact original bytes, not rewrite them"
    );
    let consumed_mode = std::fs::metadata(&session.consumed_path)
        .expect("stat consumed arm")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(consumed_mode, 0o600, "rename must preserve the 0600 mode");

    let entries: Vec<_> = std::fs::read_dir(&consumed_dir)
        .expect("read consumed dir")
        .collect();
    assert_eq!(entries.len(), 1, "exactly one consumed artifact must exist");
}

#[test]
fn restart_cannot_reacquire_authority_from_the_same_arm_path() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let arm_path = write_arm(tmp.path(), "arm.json", &valid_arm_json("nonce-restart"));
    let owner_uid = current_owner_uid(&arm_path);
    let consumed_dir = tmp.path().join("consumed");
    let identity = matching_identity(owner_uid);

    validate_and_consume(&arm_path, &consumed_dir, &identity)
        .expect("first validation consumes the arm");

    // Simulate a process restart: the SAME code path, against the SAME
    // original path, must fail closed rather than reconstructing
    // authority from whatever the consumption left behind.
    let retry = validate_and_consume(&arm_path, &consumed_dir, &identity);
    assert_eq!(retry.unwrap_err(), CutoverArmDenyReason::NotFound);
}

#[test]
fn malformed_json_is_denied() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let arm_path = write_arm(tmp.path(), "arm.json", "{ not json");
    let owner_uid = current_owner_uid(&arm_path);
    let identity = matching_identity(owner_uid);

    let result = validate_and_consume(&arm_path, &tmp.path().join("consumed"), &identity);
    assert!(matches!(
        result.unwrap_err(),
        CutoverArmDenyReason::Malformed(_)
    ));
}

#[test]
fn wrong_schema_is_denied() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let json = valid_arm_json("nonce-schema")
        .replace("revops_fee_cutover_arm/v1", "revops_fee_cutover_arm/v2");
    let arm_path = write_arm(tmp.path(), "arm.json", &json);
    let owner_uid = current_owner_uid(&arm_path);
    let identity = matching_identity(owner_uid);

    let result = validate_and_consume(&arm_path, &tmp.path().join("consumed"), &identity);
    assert_eq!(
        result.unwrap_err(),
        CutoverArmDenyReason::WrongSchema("revops_fee_cutover_arm/v2".to_string())
    );
}

#[test]
fn wrong_node_is_denied() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let json = valid_arm_json("nonce-node").replace(NODE_ID, "some-other-node");
    let arm_path = write_arm(tmp.path(), "arm.json", &json);
    let owner_uid = current_owner_uid(&arm_path);
    let identity = matching_identity(owner_uid);

    let result = validate_and_consume(&arm_path, &tmp.path().join("consumed"), &identity);
    assert_eq!(
        result.unwrap_err(),
        CutoverArmDenyReason::WrongNode("some-other-node".to_string())
    );
}

#[test]
fn wrong_subsystem_is_denied() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let json = valid_arm_json("nonce-subsystem").replace(
        &format!("\"subsystem\": \"{CUTOVER_SUBSYSTEM_FEES}\""),
        "\"subsystem\": \"rebalance\"",
    );
    let arm_path = write_arm(tmp.path(), "arm.json", &json);
    let owner_uid = current_owner_uid(&arm_path);
    let identity = matching_identity(owner_uid);

    let result = validate_and_consume(&arm_path, &tmp.path().join("consumed"), &identity);
    assert_eq!(
        result.unwrap_err(),
        CutoverArmDenyReason::WrongSubsystem("rebalance".to_string())
    );
}

#[test]
fn wrong_commit_is_denied() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let other_commit = "0000000000000000000000000000000000000000";
    let json = valid_arm_json("nonce-commit").replace(SOURCE_COMMIT, other_commit);
    let arm_path = write_arm(tmp.path(), "arm.json", &json);
    let owner_uid = current_owner_uid(&arm_path);
    let identity = matching_identity(owner_uid);

    let result = validate_and_consume(&arm_path, &tmp.path().join("consumed"), &identity);
    assert_eq!(
        result.unwrap_err(),
        CutoverArmDenyReason::WrongCommit(other_commit.to_string())
    );
}

#[test]
fn wrong_binary_hash_is_denied() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let other_hash = "0".repeat(64);
    let json = valid_arm_json("nonce-hash").replace(BINARY_SHA256, &other_hash);
    let arm_path = write_arm(tmp.path(), "arm.json", &json);
    let owner_uid = current_owner_uid(&arm_path);
    let identity = matching_identity(owner_uid);

    let result = validate_and_consume(&arm_path, &tmp.path().join("consumed"), &identity);
    assert_eq!(
        result.unwrap_err(),
        CutoverArmDenyReason::WrongBinaryHash(other_hash)
    );
}

#[test]
fn not_yet_valid_arm_is_denied() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let json = valid_arm_json("nonce-early").replace("999900", "1000001");
    let arm_path = write_arm(tmp.path(), "arm.json", &json);
    let owner_uid = current_owner_uid(&arm_path);
    let identity = matching_identity(owner_uid); // now == 1_000_000

    let result = validate_and_consume(&arm_path, &tmp.path().join("consumed"), &identity);
    assert_eq!(result.unwrap_err(), CutoverArmDenyReason::NotYetValid);
}

#[test]
fn expired_arm_is_denied() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let json = valid_arm_json("nonce-expired").replace("1000100", "1000000");
    let arm_path = write_arm(tmp.path(), "arm.json", &json);
    let owner_uid = current_owner_uid(&arm_path);
    let identity = matching_identity(owner_uid); // now == expires_at: already expired

    let result = validate_and_consume(&arm_path, &tmp.path().join("consumed"), &identity);
    assert_eq!(result.unwrap_err(), CutoverArmDenyReason::Expired);
}

#[test]
fn empty_nonce_is_denied() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let json = valid_arm_json("");
    let arm_path = write_arm(tmp.path(), "arm.json", &json);
    let owner_uid = current_owner_uid(&arm_path);
    let identity = matching_identity(owner_uid);

    let result = validate_and_consume(&arm_path, &tmp.path().join("consumed"), &identity);
    assert_eq!(result.unwrap_err(), CutoverArmDenyReason::EmptyNonce);
}

#[test]
fn reused_nonce_is_denied_and_leaves_prior_evidence_and_the_second_file_intact() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let consumed_dir = tmp.path().join("consumed");

    let first_path = write_arm(tmp.path(), "first.json", &valid_arm_json("shared-nonce"));
    let owner_uid = current_owner_uid(&first_path);
    let identity = matching_identity(owner_uid);
    let first = validate_and_consume(&first_path, &consumed_dir, &identity)
        .expect("first arm with this nonce consumes cleanly");

    let second_path = write_arm(tmp.path(), "second.json", &valid_arm_json("shared-nonce"));
    let result = validate_and_consume(&second_path, &consumed_dir, &identity);
    assert_eq!(result.unwrap_err(), CutoverArmDenyReason::ReusedNonce);

    // The second arm's own file must be untouched -- the atomic rename
    // failed before ever moving it.
    assert!(
        second_path.exists(),
        "a denied second arm must not be consumed"
    );
    // The first consumption's evidence must be untouched too.
    let consumed_bytes = std::fs::read(&first.consumed_path).expect("first consumption intact");
    assert_eq!(consumed_bytes, valid_arm_json("shared-nonce").as_bytes());
}

#[test]
fn non_regular_file_is_denied() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir_path = tmp.path().join("not-a-file");
    std::fs::create_dir(&dir_path).expect("create directory arm target");
    let owner_uid = current_owner_uid(tmp.path());
    let identity = matching_identity(owner_uid);

    let result = validate_and_consume(&dir_path, &tmp.path().join("consumed"), &identity);
    assert_eq!(result.unwrap_err(), CutoverArmDenyReason::NotRegularFile);
}

#[test]
fn symlink_is_denied_and_leaves_target_untouched() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let json = valid_arm_json("nonce-symlink");
    let real_path = write_arm(tmp.path(), "real.json", &json);
    let owner_uid = current_owner_uid(&real_path);
    let identity = matching_identity(owner_uid);

    let link_path = tmp.path().join("link.json");
    std::os::unix::fs::symlink(&real_path, &link_path).expect("create symlink");

    let result = validate_and_consume(&link_path, &tmp.path().join("consumed"), &identity);
    assert_eq!(result.unwrap_err(), CutoverArmDenyReason::Symlink);

    // The real target must be untouched -- the arm was never even opened.
    let bytes = std::fs::read(&real_path).expect("target arm intact");
    assert_eq!(bytes, json.as_bytes());
}

#[test]
fn wrong_mode_is_denied() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let arm_path =
        write_arm_with_mode(tmp.path(), "arm.json", &valid_arm_json("nonce-mode"), 0o644);
    let owner_uid = current_owner_uid(&arm_path);
    let identity = matching_identity(owner_uid);

    let result = validate_and_consume(&arm_path, &tmp.path().join("consumed"), &identity);
    assert_eq!(result.unwrap_err(), CutoverArmDenyReason::WrongMode(0o644));
}

#[test]
fn wrong_owner_is_denied() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let arm_path = write_arm(tmp.path(), "arm.json", &valid_arm_json("nonce-owner"));
    let real_uid = current_owner_uid(&arm_path);
    // Deliberately mismatched: some uid this test process does not run as.
    let mut identity = matching_identity(real_uid);
    identity.owner_uid = real_uid.wrapping_add(1);

    let result = validate_and_consume(&arm_path, &tmp.path().join("consumed"), &identity);
    assert_eq!(
        result.unwrap_err(),
        CutoverArmDenyReason::NotOwnedByCurrentUser
    );
}

#[test]
fn hash_running_binary_returns_a_64_char_hex_digest() {
    let hash = revops::cutover_arm::hash_running_binary().expect("hash own test binary");
    assert_eq!(hash.len(), 64);
    assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
}
