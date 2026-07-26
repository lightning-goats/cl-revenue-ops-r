//! Release-bound cutover-arm validation and one-time consumption (Task 7).
//!
//! See `docs/superpowers/specs/2026-07-20-rust-fee-cutover-runway-design.md`,
//! section "Cutover authorizer": entering a live-authority process session
//! requires (among other independent gates layered by a later task) "a
//! valid cutover arm." The arm is a mode-`0600` JSON file the OPERATOR
//! writes by hand right before cutover; it is never generated, refreshed,
//! or installed by a timer. This module owns exactly the arm's own
//! contract:
//!
//! - parsing and validating every field against the RUNNING process's own
//!   identity and the current time;
//! - the filesystem hardening a root-adjacent authority artifact needs
//!   (no symlink-follow, exact `0600` mode, owned by the running user);
//! - atomic, one-time consumption, so a restart cannot reconstruct the
//!   in-memory authority capability from the file that authorized it.
//!
//! Everything past that -- Python authority readback, scheduler evidence,
//! governor/ledger health, quarantine state, and actually flipping the
//! plugin from shadow to live -- belongs to the `CutoverAuthorizer`
//! integration a later task wires up; this module is deliberately a
//! self-contained, fully offline-testable primitive with no CLN RPC, no DB
//! handle, and no process-wide mutable state of its own.
//!
//! ## Fail-closed by construction
//!
//! Every rejection path returns a [`CutoverArmDenyReason`] -- there is no
//! "best effort" or partial-success outcome. Field checks run in a fixed
//! order (file hardening, then read, then parse, then schema/identity/time
//! fields, then nonce consumption) so a given bad arm always reports the
//! SAME reason, which rehearsal harnesses and operators can match on by its
//! stable [`CutoverArmDenyReason::code`].
//!
//! ## One-session consumption
//!
//! [`validate_and_consume`] renames the arm file into `consumed_dir` using
//! Linux's `RENAME_NOREPLACE` (`rustix::fs::renameat_with`) rather than a
//! plain `rename`: the destination name is the arm's own nonce, so a second
//! arm (from any source path) carrying the SAME nonce hits `EEXIST`
//! atomically and is denied as [`CutoverArmDenyReason::ReusedNonce`] --
//! without ever touching (or losing) the first consumption's evidence.
//! Because `rename` never crosses filesystems, an operator pointing
//! `consumed_dir` at a different filesystem than the arm file fails the
//! same way (surfaced as [`CutoverArmDenyReason::ConsumeFailed`]) rather
//! than silently falling back to copy+delete.
//!
//! The returned [`LiveSessionArm`] is the capability that consumption
//! succeeded. It deliberately implements neither `Serialize` nor
//! `Deserialize` and has no public constructor other than
//! [`validate_and_consume`] -- nothing can write it to disk and read it
//! back, so a process restart has no path to reacquire authority except a
//! FRESH arm file at a path that still exists (the consumed one does not:
//! it was renamed away).
//!
//! ## Residual TOCTOU (accepted)
//!
//! The final rename step is by PATH, not by the file descriptor this
//! module read the content from (POSIX has no fd-only rename). A local
//! attacker who already has write access to the arm file's own directory
//! -- i.e. is already the same trusted operator account that authored the
//! arm -- could in principle swap the file's content between the read and
//! the rename. That is an operator-trust-boundary self-attack, not a
//! privilege escalation, and is out of scope here.

use std::fs::File;
use std::io::Read as _;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};

use serde::Deserialize;

/// Pinned schema identity. Any other value -- including a same-shape
/// document at a different version -- is refused outright; there is no
/// migration path inside the validator, only a new pinned constant in a
/// future release.
pub const CUTOVER_ARM_SCHEMA: &str = "revops_fee_cutover_arm/v1";

/// The one subsystem this arm authorizes. Kept as a named constant (rather
/// than inlined into callers) so a future second subsystem cannot
/// accidentally reuse a hand-typed string that drifts from this one.
pub const CUTOVER_SUBSYSTEM_FEES: &str = "fees";

/// Required file mode. Anything group- or world-readable/writable is
/// refused -- the arm's nonce and identity are not secret-strength, but a
/// looser mode is loud evidence the operator's deployment procedure was not
/// followed, and the design doc pins `0600` explicitly.
const REQUIRED_MODE: u32 = 0o600;

/// The on-disk cutover-arm document (design doc "Cutover authorizer": "The
/// cutover arm is a mode-`0600` JSON object containing: schema name and
/// version; node identity; subsystem name `fees`; Rust source commit; exact
/// running-binary SHA-256; activation and expiry timestamps; a unique
/// operator-generated nonce.").
#[derive(Debug, Clone, Deserialize)]
pub struct CutoverArm {
    pub schema: String,
    pub node_id: String,
    pub subsystem: String,
    pub source_commit: String,
    pub binary_sha256: String,
    pub not_before: i64,
    pub expires_at: i64,
    pub nonce: String,
}

/// Everything the running process IS, for arm binding. Constructed by the
/// caller from live process state (never derived from the arm file
/// itself -- that would make every check tautological). `owner_uid` is
/// threaded in rather than read internally via a `getuid()` syscall so this
/// module stays a pure function of its inputs and is fully testable without
/// root (a test can assert a mismatch by simply passing an `owner_uid` the
/// test process does not have).
#[derive(Debug, Clone)]
pub struct RunningIdentity {
    pub node_id: String,
    pub subsystem: String,
    pub source_commit: String,
    pub binary_sha256: String,
    pub owner_uid: u32,
    pub now: i64,
}

/// Every fail-closed reason, each stable and mutually exclusive. Field
/// order matches the check order in [`validate_and_consume`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CutoverArmDenyReason {
    /// The arm path does not exist (already consumed, never installed, or
    /// removed by the operator).
    NotFound,
    /// The path resolves to something other than a regular file (a
    /// directory, FIFO, socket, or device).
    NotRegularFile,
    /// The path's final component is a symlink; never followed.
    Symlink,
    /// The file's mode is not exactly `0600`. Carries the observed mode
    /// (masked to the permission bits) for operator diagnosis.
    WrongMode(u32),
    /// The file is not owned by the running process's own user.
    NotOwnedByCurrentUser,
    /// The file could not be opened, `fstat`ed, or read (I/O error other
    /// than not-found).
    ReadFailed(String),
    /// The file's bytes are not a valid `CutoverArm` JSON document.
    Malformed(String),
    /// `schema` did not match [`CUTOVER_ARM_SCHEMA`]. Carries the value
    /// found.
    WrongSchema(String),
    /// `node_id` did not match the running identity. Carries the value
    /// found (never the expected one, to avoid encouraging a caller to
    /// build a matching arm from an error message read off a shared log).
    WrongNode(String),
    /// `subsystem` did not match the running identity.
    WrongSubsystem(String),
    /// `source_commit` did not match the running binary's own commit --
    /// this is also how a dirty or unavailable build's placeholder commit
    /// (see `build.rs`) is refused, with no special-cased branch needed.
    WrongCommit(String),
    /// `binary_sha256` did not match the running executable's own hash.
    WrongBinaryHash(String),
    /// `now` is before `not_before`.
    NotYetValid,
    /// `now` is at or after `expires_at`.
    Expired,
    /// `nonce` is the empty string.
    EmptyNonce,
    /// `nonce` is non-empty but is not a bare, safe filesystem path
    /// component: it contains a character outside `[A-Za-z0-9_-]`, or it
    /// is longer than 64 characters. Checked -- and denied -- BEFORE the
    /// nonce is ever used to build a filesystem path (`consumed_dir.join`
    /// in [`consume`]), because an unconstrained nonce (an absolute path,
    /// a `../` traversal segment, or an oversized value) could otherwise
    /// make the one-time-consumption rename land outside `consumed_dir`
    /// entirely, scattering or overwriting things the single-use replay
    /// ledger depends on. Carries the offending value found.
    InvalidNonce(String),
    /// A DIFFERENT arm (any source path) already consumed this exact
    /// nonce. The original file is left untouched -- the reuse is detected
    /// entirely inside the atomic consumption step, before any rename of
    /// the second arm occurs.
    ReusedNonce,
    /// The atomic consuming rename (or its parent-directory fsync) failed.
    ConsumeFailed(String),
}

impl CutoverArmDenyReason {
    /// Stable, machine-matchable code -- logged and reported verbatim.
    /// Never reword an existing code; add a new variant instead.
    pub fn code(&self) -> &'static str {
        match self {
            Self::NotFound => "not_found",
            Self::NotRegularFile => "not_regular_file",
            Self::Symlink => "symlink",
            Self::WrongMode(_) => "wrong_mode",
            Self::NotOwnedByCurrentUser => "not_owned_by_current_user",
            Self::ReadFailed(_) => "read_failed",
            Self::Malformed(_) => "malformed",
            Self::WrongSchema(_) => "wrong_schema",
            Self::WrongNode(_) => "wrong_node",
            Self::WrongSubsystem(_) => "wrong_subsystem",
            Self::WrongCommit(_) => "wrong_commit",
            Self::WrongBinaryHash(_) => "wrong_binary_hash",
            Self::NotYetValid => "not_yet_valid",
            Self::Expired => "expired",
            Self::EmptyNonce => "empty_nonce",
            Self::InvalidNonce(_) => "invalid_nonce",
            Self::ReusedNonce => "reused_nonce",
            Self::ConsumeFailed(_) => "consume_failed",
        }
    }
}

impl std::fmt::Display for CutoverArmDenyReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WrongMode(mode) => write!(f, "{}: {mode:#o}", self.code()),
            Self::ReadFailed(detail) | Self::Malformed(detail) | Self::ConsumeFailed(detail) => {
                write!(f, "{}: {detail}", self.code())
            }
            Self::WrongSchema(found)
            | Self::WrongNode(found)
            | Self::WrongSubsystem(found)
            | Self::WrongCommit(found)
            | Self::WrongBinaryHash(found)
            | Self::InvalidNonce(found) => write!(f, "{}: {found}", self.code()),
            _ => write!(f, "{}", self.code()),
        }
    }
}

impl std::error::Error for CutoverArmDenyReason {}

/// The capability that one arm was validated and atomically consumed in
/// THIS process session. See the module doc's "One-session consumption"
/// for why this deliberately carries no `Serialize`/`Deserialize` impl.
///
/// Every field is private and there is no public constructor other than
/// [`validate_and_consume`] -- this is what makes the type an actual
/// capability rather than a plain data bag: possessing a value of this
/// type is proof that a real arm file was validated and atomically
/// consumed in this process, because there is no other way to build one.
/// If the fields were `pub`, any code anywhere could mint
/// `LiveSessionArm { .. }` by hand and hand it to a caller expecting proof
/// of a real cutover, defeating the whole point of the type. This is
/// pinned by a `compile_fail` doctest below so a future edit that widens
/// visibility is caught at documentation-test time, not just by code
/// review.
///
/// ```compile_fail
/// // Fields are private outside this module -- this must NOT compile.
/// let forged = revops::cutover_arm::LiveSessionArm {
///     node_id: "lnnode".to_string(),
///     subsystem: "fees".to_string(),
///     source_commit: "deadbeef".to_string(),
///     binary_sha256: "0".repeat(64),
///     nonce: "forged-nonce".to_string(),
///     consumed_path: std::path::PathBuf::from("/tmp/forged"),
/// };
/// ```
#[derive(Debug)]
pub struct LiveSessionArm {
    node_id: String,
    subsystem: String,
    source_commit: String,
    binary_sha256: String,
    nonce: String,
    /// Where the consumed arm now lives (`consumed_dir/<nonce>`), kept for
    /// audit logging -- never re-read to reconstruct authority.
    consumed_path: PathBuf,
}

impl LiveSessionArm {
    /// The node identity the consumed arm was bound to.
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// The subsystem the consumed arm authorized (always
    /// [`CUTOVER_SUBSYSTEM_FEES`] today).
    pub fn subsystem(&self) -> &str {
        &self.subsystem
    }

    /// The Rust source commit the consumed arm was bound to.
    pub fn source_commit(&self) -> &str {
        &self.source_commit
    }

    /// The running binary's SHA-256 the consumed arm was bound to.
    pub fn binary_sha256(&self) -> &str {
        &self.binary_sha256
    }

    /// The arm's unique, now-consumed nonce.
    pub fn nonce(&self) -> &str {
        &self.nonce
    }

    /// Where the consumed arm now lives (`consumed_dir/<nonce>`) -- for
    /// audit logging only, never re-read to reconstruct authority.
    pub fn consumed_path(&self) -> &Path {
        &self.consumed_path
    }
}

/// SHA-256 (lowercase hex) of the CURRENTLY RUNNING executable's bytes,
/// read fresh via `std::env::current_exe()` (design doc: "The running
/// process hashes its own executable and verifies every arm field.").
/// Fallible on purpose -- unlike `fee_scheduler::binary_sha256()`'s
/// infallible provenance-logging sibling (which degrades to the string
/// `"unavailable"` for a non-security-critical DB column), a security
/// comparison must never risk that placeholder coincidentally matching a
/// miswritten arm; callers that cannot compute this MUST refuse to attempt
/// live authority rather than substitute a sentinel.
pub fn hash_running_binary() -> std::io::Result<String> {
    let path = std::env::current_exe()?;
    let bytes = std::fs::read(path)?;
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(&bytes);
    let mut hex = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(hex, "{byte:02x}");
    }
    Ok(hex)
}

/// Validate `arm_path` against `identity`, then atomically consume it into
/// `consumed_dir`. See the module doc for the full contract; the check
/// order below is the stable order callers and tests can rely on.
pub fn validate_and_consume(
    arm_path: &Path,
    consumed_dir: &Path,
    identity: &RunningIdentity,
) -> Result<LiveSessionArm, CutoverArmDenyReason> {
    // 1. Symlink / file-type check BEFORE opening anything (an `lstat`,
    //    never a `stat`): a directory, FIFO, socket, or device is refused
    //    here without ever being opened.
    let link_meta = std::fs::symlink_metadata(arm_path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            CutoverArmDenyReason::NotFound
        } else {
            CutoverArmDenyReason::ReadFailed(error.to_string())
        }
    })?;
    if link_meta.file_type().is_symlink() {
        return Err(CutoverArmDenyReason::Symlink);
    }
    if !link_meta.is_file() {
        return Err(CutoverArmDenyReason::NotRegularFile);
    }

    // 2. Open with O_NOFOLLOW as defense-in-depth against a TOCTOU swap
    //    between the lstat above and this open landing on a symlink.
    let owned_fd = rustix::fs::open(
        arm_path,
        rustix::fs::OFlags::RDONLY | rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )
    .map_err(|errno| match errno {
        rustix::io::Errno::LOOP => CutoverArmDenyReason::Symlink,
        rustix::io::Errno::NOENT => CutoverArmDenyReason::NotFound,
        other => CutoverArmDenyReason::ReadFailed(std::io::Error::from(other).to_string()),
    })?;
    let mut file = File::from(owned_fd);

    // 3. `fstat` through the already-open fd is authoritative (immune to
    //    any further path-based race) for regular-file-ness, mode, and
    //    ownership.
    let metadata = file
        .metadata()
        .map_err(|error| CutoverArmDenyReason::ReadFailed(error.to_string()))?;
    if !metadata.is_file() {
        return Err(CutoverArmDenyReason::NotRegularFile);
    }
    // Masked with `0o7777`, not `0o777`: setuid/setgid/sticky are real mode
    // bits that a bare `0o777` mask would silently discard, letting e.g.
    // `0o4600` (setuid + owner rw) coincidentally read as the required
    // `0600`. Masking in the extra bits means they must be exactly absent.
    let mode = metadata.permissions().mode() & 0o7777;
    if mode != REQUIRED_MODE {
        return Err(CutoverArmDenyReason::WrongMode(mode));
    }
    if metadata.uid() != identity.owner_uid {
        return Err(CutoverArmDenyReason::NotOwnedByCurrentUser);
    }

    // 4. Read once.
    let mut contents = String::new();
    file.read_to_string(&mut contents)
        .map_err(|error| CutoverArmDenyReason::ReadFailed(error.to_string()))?;
    drop(file);

    // 5. Parse.
    let arm: CutoverArm = serde_json::from_str(&contents)
        .map_err(|error| CutoverArmDenyReason::Malformed(error.to_string()))?;

    // 6. Validate every field against the running identity and clock.
    validate_fields(&arm, identity)?;

    // 7. Atomic, one-time consumption.
    consume(arm_path, consumed_dir, &arm)?;

    let consumed_path = consumed_dir.join(&arm.nonce);
    Ok(LiveSessionArm {
        node_id: arm.node_id,
        subsystem: arm.subsystem,
        source_commit: arm.source_commit,
        binary_sha256: arm.binary_sha256,
        nonce: arm.nonce,
        consumed_path,
    })
}

fn validate_fields(
    arm: &CutoverArm,
    identity: &RunningIdentity,
) -> Result<(), CutoverArmDenyReason> {
    if arm.schema != CUTOVER_ARM_SCHEMA {
        return Err(CutoverArmDenyReason::WrongSchema(arm.schema.clone()));
    }
    if arm.node_id != identity.node_id {
        return Err(CutoverArmDenyReason::WrongNode(arm.node_id.clone()));
    }
    if arm.subsystem != identity.subsystem {
        return Err(CutoverArmDenyReason::WrongSubsystem(arm.subsystem.clone()));
    }
    if arm.source_commit != identity.source_commit {
        return Err(CutoverArmDenyReason::WrongCommit(arm.source_commit.clone()));
    }
    if arm.binary_sha256 != identity.binary_sha256 {
        return Err(CutoverArmDenyReason::WrongBinaryHash(
            arm.binary_sha256.clone(),
        ));
    }
    if arm.nonce.is_empty() {
        return Err(CutoverArmDenyReason::EmptyNonce);
    }
    // Gate the nonce's shape BEFORE it is ever used to build a filesystem
    // path (`consume` joins it onto `consumed_dir` verbatim). An absolute
    // path, a `../` traversal segment, or an oversized value must never
    // reach that join.
    if !is_safe_nonce(&arm.nonce) {
        return Err(CutoverArmDenyReason::InvalidNonce(arm.nonce.clone()));
    }
    // Activation is inclusive (`now == not_before` is valid); expiry is
    // exclusive (`now == expires_at` is already expired) -- a zero-width
    // valid window is never accidentally valid.
    if identity.now < arm.not_before {
        return Err(CutoverArmDenyReason::NotYetValid);
    }
    if identity.now >= arm.expires_at {
        return Err(CutoverArmDenyReason::Expired);
    }
    Ok(())
}

/// A nonce is safe to use as a single filesystem path COMPONENT once
/// joined onto `consumed_dir`: 1..=64 ASCII characters, each an
/// alphanumeric, `_`, or `-`. This rules out `/` (so an absolute nonce or a
/// `../` traversal segment can never be built), any other path separator,
/// and unbounded length, without needing a platform-specific path parser --
/// the charset alone makes `Path::join` behave exactly like appending a
/// plain filename.
fn is_safe_nonce(nonce: &str) -> bool {
    !nonce.is_empty()
        && nonce.len() <= 64
        && nonce
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

/// Rename `arm_path` to `consumed_dir/<nonce>` with `RENAME_NOREPLACE`, so
/// a reused nonce fails atomically (`EEXIST`) instead of silently
/// overwriting the first consumption's evidence, then `fsync`s
/// `consumed_dir` so the rename survives a crash immediately after this
/// call returns. A failure past this point (including the fsync) still
/// leaves the arm consumed -- never re-usable -- which is the fail-closed
/// direction: losing a live-session grant to a durability-reporting error
/// is acceptable, silently permitting nonce replay is not.
fn consume(
    arm_path: &Path,
    consumed_dir: &Path,
    arm: &CutoverArm,
) -> Result<(), CutoverArmDenyReason> {
    std::fs::create_dir_all(consumed_dir)
        .map_err(|error| CutoverArmDenyReason::ConsumeFailed(format!("consumed dir: {error}")))?;
    let consumed_path = consumed_dir.join(&arm.nonce);

    rustix::fs::renameat_with(
        rustix::fs::CWD,
        arm_path,
        rustix::fs::CWD,
        &consumed_path,
        rustix::fs::RenameFlags::NOREPLACE,
    )
    .map_err(|errno| match errno {
        rustix::io::Errno::EXIST => CutoverArmDenyReason::ReusedNonce,
        other => CutoverArmDenyReason::ConsumeFailed(std::io::Error::from(other).to_string()),
    })?;

    let dir_fd = rustix::fs::open(
        consumed_dir,
        rustix::fs::OFlags::RDONLY | rustix::fs::OFlags::DIRECTORY | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )
    .map_err(|errno| {
        CutoverArmDenyReason::ConsumeFailed(format!(
            "consumed dir reopen for fsync: {}",
            std::io::Error::from(errno)
        ))
    })?;
    rustix::fs::fsync(&dir_fd).map_err(|errno| {
        CutoverArmDenyReason::ConsumeFailed(format!(
            "consumed dir fsync: {}",
            std::io::Error::from(errno)
        ))
    })?;

    Ok(())
}
