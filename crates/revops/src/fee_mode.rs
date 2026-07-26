//! Operating-mode validation (stateful-shadow revision, Task 8): the ONLY
//! gate that decides which of the three accepted operating modes this
//! process runs in, and the type-level guarantee that live fee authority
//! is structurally unreachable except through a validated
//! [`crate::cutover_arm::LiveSessionArm`].
//!
//! ## The mode matrix (Task 1, encoded verbatim)
//!
//! | Mode | observer | fee-dryrun | fee-broadcast | fee-stateful-shadow | arm |
//! | --- | --- | --- | --- | --- | --- |
//! | passive observer | true | false | false | false | absent |
//! | autonomous fee shadow | true | true | false | true | absent |
//! | live fee authority | false | false | true | false | valid and consumed |
//!
//! Every other combination of the four booleans -- including an arm
//! present alongside a non-live combination -- fails initialization with a
//! stable, machine-matchable [`FeeModeDenyReason`]. There is no partial
//! acceptance and no default fallback mode: [`validate_fee_mode`] either
//! returns exactly one of the three [`ValidatedFeeMode`] variants or an
//! `Err`.
//!
//! ## Amendment (Task R8 item 4, binding): stateful-shadow seed provenance
//!
//! `fee-stateful-shadow=true` requires a recorded seed-provenance event
//! UNLESS the Rust-owned state store is still virgin (no state generation
//! has ever committed -- seeding is expected to happen on the very first
//! cycle, `crate::fee_state::seed_once_from_python`, per
//! `StateLifecycle::SeedOnce`'s first-cycle-not-spawn-time contract). A
//! store that already holds committed state but carries NO seed-event row
//! is the "never seeded" misconfiguration the amendment targets, and fails
//! startup with [`FeeModeDenyReason::NeverSeeded`].
//!
//! The caller supplies this as a [`revops_db::fee_runway::FeeStateSnapshot`]
//! (from `load_latest_state`) and an `Option<&FeeSeedEventRow>` (from
//! `latest_seed_event`) -- this module reads only `generation`/`rows` and
//! presence, never reopening a connection itself (Task 8 is a pure
//! validator, not an I/O module).
//!
//! ## `LiveAuthority` cannot be forged
//!
//! [`LiveMode`]'s only field is a private [`crate::cutover_arm::LiveSessionArm`]
//! and its only constructor is [`validate_fee_mode`] itself (which in turn
//! only accepts an `Option<LiveSessionArm>` -- a type with no public
//! constructor other than `cutover_arm::validate_and_consume`). There is no
//! path from flags alone to a `LiveAuthority` value: an operator (or a bug)
//! setting the four booleans to the live row without ALSO possessing a
//! genuine consumed arm gets [`FeeModeDenyReason::LiveModeRequiresArm`],
//! never a live capability. See the `compile_fail` doctest below.
//!
//! ## Shadow mode cannot contain a live broadcaster
//!
//! [`ShadowMode`] carries only a [`ShadowSeedStatus`] marker -- there is no
//! field of any kind that could hold a broadcaster, executor, or arm. This
//! is a structural guarantee (nothing to leak), not a runtime check.

use revops_db::fee_runway::FeeSeedEventRow;
use revops_db::fee_runway::FeeStateSnapshot;

use crate::cutover_arm::LiveSessionArm;

/// The four operating-mode booleans this validator matches against the
/// Task 1 table. Field order matches the table's column order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ModeFlags {
    pub observer: bool,
    pub fee_dryrun: bool,
    pub fee_broadcast: bool,
    pub fee_stateful_shadow: bool,
}

/// Which committed-state-vs-seed-provenance case applied for an
/// [`ValidatedFeeMode::AutonomousShadow`] result. Diagnostic only -- both
/// variants are equally "valid shadow mode."
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShadowSeedStatus {
    /// The Rust-owned state store is virgin (no generation has ever
    /// committed): `SeedOnce` will import from Python on the upcoming
    /// first cycle.
    PendingFirstCycle,
    /// A seed-provenance event (`seeded` or `seed_refused`) is already on
    /// record: seeding already ran (or was fail-closed refused) in a prior
    /// process lifetime.
    AlreadySeeded,
}

/// The autonomous-fee-shadow mode capability. Deliberately carries nothing
/// but the diagnostic [`ShadowSeedStatus`] -- there is no field through
/// which a live broadcaster, executor, or arm could ever reach this
/// variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShadowMode {
    seed_status: ShadowSeedStatus,
}

impl ShadowMode {
    pub fn seed_status(&self) -> ShadowSeedStatus {
        self.seed_status
    }
}

/// The live-fee-authority mode capability. Its only field is a private
/// [`LiveSessionArm`], and its only constructor is [`validate_fee_mode`] --
/// see the module doc's "`LiveAuthority` cannot be forged".
#[derive(Debug)]
pub struct LiveMode {
    arm: LiveSessionArm,
}

impl LiveMode {
    /// The consumed arm that authorized this session.
    pub fn arm(&self) -> &LiveSessionArm {
        &self.arm
    }

    /// Unwrap into the consumed arm, e.g. for audit logging at shutdown.
    pub fn into_arm(self) -> LiveSessionArm {
        self.arm
    }
}

/// The result of a successful mode validation: exactly one of the three
/// accepted operating modes (Task 1 table).
///
/// ```compile_fail
/// // LiveMode has no public field and no public constructor other than
/// // `validate_fee_mode` -- this must NOT compile.
/// let forged = revops::fee_mode::LiveMode { arm: unimplemented!() };
/// ```
#[derive(Debug)]
pub enum ValidatedFeeMode {
    PassiveObserver,
    AutonomousShadow(ShadowMode),
    LiveAuthority(LiveMode),
}

/// Every fail-closed reason [`validate_fee_mode`] can return. Each is
/// stable and mutually exclusive; never reword an existing variant's
/// [`FeeModeDenyReason::code`] -- add a new variant instead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FeeModeDenyReason {
    /// The four booleans match none of the Task 1 table's three rows.
    InvalidCombination(ModeFlags),
    /// A cutover arm was supplied (`Some`) but the resolved flag
    /// combination is passive-observer or autonomous-shadow, neither of
    /// which the table permits an arm for.
    ArmPresentInNonLiveMode,
    /// The flags matched the live-fee-authority row but no arm was
    /// supplied -- the table requires "valid and consumed".
    LiveModeRequiresArm,
    /// Amendment (Task R8 item 4): `fee-stateful-shadow=true`, the
    /// Rust-owned store already holds committed state (not virgin), and no
    /// seed-provenance event is on record.
    NeverSeeded,
    /// Fix round 1 (coordinator ruling I-6): the flags matched the
    /// live-fee-authority row and a valid arm was supplied, but the
    /// Rust-owned store is either virgin (generation 0) or carries no
    /// seed-provenance event. The cutover sequence guarantees the
    /// autonomous shadow seeds and accumulates state BEFORE an arm is ever
    /// minted -- "healthy persisted state" means seeded, never virgin. A
    /// live process must never be the first thing to ever touch the
    /// Rust-owned store.
    LiveModeRequiresSeededState,
}

impl FeeModeDenyReason {
    /// Stable, machine-matchable code -- logged and reported verbatim.
    pub fn code(&self) -> &'static str {
        match self {
            Self::InvalidCombination(_) => "invalid_mode_combination",
            Self::ArmPresentInNonLiveMode => "arm_present_in_non_live_mode",
            Self::LiveModeRequiresArm => "live_mode_requires_arm",
            Self::NeverSeeded => "stateful_shadow_never_seeded",
            Self::LiveModeRequiresSeededState => "live_mode_requires_seeded_state",
        }
    }
}

impl std::fmt::Display for FeeModeDenyReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidCombination(flags) => write!(
                f,
                "{}: observer={} fee-dryrun={} fee-broadcast={} fee-stateful-shadow={} \
                 matches no accepted operating mode; accepted combinations (option names as \
                 registered by the plugin): passive observer (revops-r-observer=true \
                 revops-r-fee-dryrun=false revops-r-fee-broadcast=false \
                 revops-r-fee-stateful-shadow=false, arm absent) | autonomous fee shadow \
                 (revops-r-observer=true revops-r-fee-dryrun=true revops-r-fee-broadcast=false \
                 revops-r-fee-stateful-shadow=true, arm absent) | live fee authority \
                 (revops-r-observer=false revops-r-fee-dryrun=false revops-r-fee-broadcast=true \
                 revops-r-fee-stateful-shadow=false, arm valid and consumed)",
                self.code(),
                flags.observer,
                flags.fee_dryrun,
                flags.fee_broadcast,
                flags.fee_stateful_shadow,
            ),
            Self::ArmPresentInNonLiveMode => write!(
                f,
                "{}: a cutover arm was supplied but the resolved operating mode is not live \
                 fee authority",
                self.code(),
            ),
            Self::LiveModeRequiresArm => write!(
                f,
                "{}: live fee authority mode requires a valid, consumed cutover arm",
                self.code(),
            ),
            Self::NeverSeeded => write!(
                f,
                "{}: fee-stateful-shadow=true but the Rust-owned state store already holds \
                 committed state with no recorded seed-provenance event -- a stateful shadow \
                 that never seeded is a misconfiguration",
                self.code(),
            ),
            Self::LiveModeRequiresSeededState => write!(
                f,
                "{}: live fee authority mode requires the Rust-owned store to already hold \
                 seeded state (generation > 0 and a recorded seed-provenance event) -- a live \
                 process must never be the first thing to ever touch the Rust-owned store",
                self.code(),
            ),
        }
    }
}

impl std::error::Error for FeeModeDenyReason {}

/// True iff the Rust-owned state store has never committed any state
/// generation (`revops_db::fee_runway::load_latest_state` returned
/// `generation == 0` with an empty row set) -- the amendment's "virgin
/// store" case, where seeding is still pending on the first cycle.
fn store_is_virgin(state: &FeeStateSnapshot) -> bool {
    state.generation == 0 && state.rows.is_empty()
}

/// Validate the four operating-mode flags plus the arm/seed-provenance
/// evidence against the Task 1 mode matrix (and its Task R8 amendment),
/// returning exactly one of the three accepted [`ValidatedFeeMode`]
/// variants or a stable [`FeeModeDenyReason`].
///
/// `state` and `seed_event` are read for `fee-stateful-shadow=true` (the
/// autonomous-shadow row) AND for the live-fee-authority row (fix round 1,
/// coordinator ruling I-6: live authority requires the store to already be
/// seeded -- see [`FeeModeDenyReason::LiveModeRequiresSeededState`]); they
/// are ignored only for passive observer, which has no seed-provenance
/// requirement of its own.
pub fn validate_fee_mode(
    flags: ModeFlags,
    arm: Option<LiveSessionArm>,
    state: &FeeStateSnapshot,
    seed_event: Option<&FeeSeedEventRow>,
) -> Result<ValidatedFeeMode, FeeModeDenyReason> {
    match (
        flags.observer,
        flags.fee_dryrun,
        flags.fee_broadcast,
        flags.fee_stateful_shadow,
    ) {
        (true, false, false, false) => {
            if arm.is_some() {
                return Err(FeeModeDenyReason::ArmPresentInNonLiveMode);
            }
            Ok(ValidatedFeeMode::PassiveObserver)
        }
        (true, true, false, true) => {
            if arm.is_some() {
                return Err(FeeModeDenyReason::ArmPresentInNonLiveMode);
            }
            let seed_status = if store_is_virgin(state) {
                ShadowSeedStatus::PendingFirstCycle
            } else if seed_event.is_some() {
                ShadowSeedStatus::AlreadySeeded
            } else {
                return Err(FeeModeDenyReason::NeverSeeded);
            };
            Ok(ValidatedFeeMode::AutonomousShadow(ShadowMode {
                seed_status,
            }))
        }
        (false, false, true, false) => {
            let Some(arm) = arm else {
                return Err(FeeModeDenyReason::LiveModeRequiresArm);
            };
            // Fix round 1 (coordinator ruling I-6): "healthy persisted
            // state" for live authority means SEEDED, never virgin. The
            // cutover sequence guarantees the autonomous shadow seeds and
            // accumulates state before an arm is ever minted, so a virgin
            // store (or committed state with no seed-provenance event --
            // the same corruption/misconfiguration `NeverSeeded` catches
            // for the shadow row) means this would be the FIRST thing to
            // ever touch the Rust-owned store, which must never happen for
            // a live process.
            if state.generation == 0 || seed_event.is_none() {
                return Err(FeeModeDenyReason::LiveModeRequiresSeededState);
            }
            Ok(ValidatedFeeMode::LiveAuthority(LiveMode { arm }))
        }
        _ => Err(FeeModeDenyReason::InvalidCombination(flags)),
    }
}
