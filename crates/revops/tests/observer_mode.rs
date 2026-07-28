use revops::fee_mode::{validate_fee_mode, AuthorityPlan, ModeFlags, ValidatedFeeMode};
use revops_db::fee_runway::{FeeStateSnapshot, SeedBindingState};

fn passive_mode() -> ValidatedFeeMode {
    validate_fee_mode(
        ModeFlags {
            observer: true,
            fee_dryrun: false,
            fee_broadcast: false,
            fee_stateful_shadow: false,
        },
        None,
        &FeeStateSnapshot::default(),
        &SeedBindingState::VirginStore,
    )
    .expect("passive mode validates")
}

fn autonomous_shadow_mode() -> ValidatedFeeMode {
    validate_fee_mode(
        ModeFlags {
            observer: true,
            fee_dryrun: true,
            fee_broadcast: false,
            fee_stateful_shadow: true,
        },
        None,
        &FeeStateSnapshot::default(),
        &SeedBindingState::VirginStore,
    )
    .expect("autonomous shadow mode validates")
}

#[test]
fn both_observer_modes_construct_without_touching_the_action_factory() {
    for (mode, autonomous) in [(passive_mode(), false), (autonomous_shadow_mode(), true)] {
        let plan = mode.into_authority_plan(|_| -> () {
            panic!("observer construction touched the live action factory")
        });
        match plan {
            AuthorityPlan::Observer(token) => {
                assert_eq!(token.autonomous_shadow(), autonomous);
            }
            AuthorityPlan::Live(()) => panic!("observer validation produced live authority"),
        }
    }
}
