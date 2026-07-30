//! F71-R23 slice 2 (RED): the per-channel evidence profitability needs
//! before `assemble_channel_profitability` may be called.
//!
//! `assemble_fleet` currently hands the frozen classifier four values it
//! never looked up: `last_routed: None`, `diag_attempt_count: 0`,
//! `diag_last_success_time: 0`, `posterior_variance: None`. Three of those
//! coincide with Python's OWN defaults for "the query returned no row", so
//! they are invisible in every existing test. They are not equivalent to
//! Python, because Python ran the query first.
//!
//! The distinction these tests exist to hold is one level down from the
//! analyze slice's: CONSULTED-AND-ABSENT is a real answer and keeps
//! Python's default; COULD-NOT-CONSULT is a refusal.

use revops_analytics::profitability::{classify_channel, ClassifyEvidence, DiagStats};

use crate::profitability_evidence::{
    channel_evidence, ChannelEvidence, ConsultedSources, ProfitabilityEvidenceRefusal,
};

const NOW: i64 = 1_800_000_000;
const DAY: i64 = 86_400;
const SCID: &str = "700x1x0";

/// Every source consulted successfully, every one holding a row.
fn all_present() -> ConsultedSources {
    ConsultedSources {
        last_routed: Ok(Some(NOW - DAY)),
        diag: Ok(Some(DiagStats {
            attempt_count: 3,
            last_success_time: NOW - 3 * DAY,
        })),
        posterior_variance: Ok(Some(1_000.0)),
        opener: Ok(Some("remote".to_string())),
    }
}

/// Every source consulted successfully, none holding a row.
fn all_consulted_empty() -> ConsultedSources {
    ConsultedSources {
        last_routed: Ok(None),
        diag: Ok(None),
        posterior_variance: Ok(None),
        opener: Ok(Some("local".to_string())),
    }
}

// ---------------------------------------------------------------------
// Why this slice exists at all: the fabricated `last_routed`.
// ---------------------------------------------------------------------

/// The severity case, stated as behaviour rather than as an opinion.
///
/// `last_routed: None` is not a harmless placeholder. The classifier reads
/// it as "never routed" and substitutes `days_open` for `days_inactive`
/// (py profitability_analyzer.py:2661-2663). So a mature channel that
/// routed yesterday is judged as having been idle for its entire life, and
/// falls straight through the STAGNANT_CANDIDATE branch.
///
/// This is not underdetection. It is a live, busy channel reported as dead
/// capital -- and downstream that is close/redeploy evidence.
#[test]
fn fabricating_last_routed_reports_a_busy_channel_as_dead_capital() {
    let busy = ClassifyEvidence {
        now: NOW,
        diag_stats: None,
        posterior_variance: None,
        contribution_30d_msat: Some(50_000),
    };
    // Same channel, same ROI, same age. Only `last_routed` differs.
    let sourced = classify_channel(-0.5, -1_000, Some(NOW - DAY), 400, 10, &busy);
    let fabricated = classify_channel(-0.5, -1_000, None, 400, 10, &busy);

    assert_eq!(
        sourced.as_value(),
        "underwater",
        "a channel that routed yesterday is underwater, not stagnant"
    );
    assert_eq!(
        fabricated.as_value(),
        "stagnant_candidate",
        "the fabricated None must be shown to change the verdict; if this ever \
         stops differing, the fabrication is no longer load-bearing and this \
         whole slice needs re-deriving"
    );
}

// ---------------------------------------------------------------------
// Could-not-consult is a refusal.
// ---------------------------------------------------------------------

#[test]
fn an_unreadable_last_routed_source_refuses_rather_than_defaulting_to_never_routed() {
    // DISCLOSED DIVERGENCE from Python. `_get_last_routing_time` wraps its
    // query in `except Exception: return None`
    // (profitability_analyzer.py:2585-2588), so on a failed read Python
    // silently classifies every channel as never-routed. That is the
    // dead-capital verdict above, produced by an I/O error. This port
    // refuses instead, for the same reason `econ_evidence` refuses on a
    // failed `listfunds` rather than reporting Python's zeros.
    let sources = ConsultedSources {
        last_routed: Err("database is locked".to_string()),
        ..all_present()
    };
    let refusal = channel_evidence(SCID, sources)
        .expect_err("an unreadable routing history is not evidence of no routing");
    assert_eq!(
        refusal,
        ProfitabilityEvidenceRefusal::LastRoutedUnavailable {
            scid: SCID.to_string(),
            detail: "database is locked".to_string(),
        }
    );
}

#[test]
fn an_unreadable_diagnostic_source_refuses_rather_than_defaulting_to_no_attempts() {
    let sources = ConsultedSources {
        diag: Err("no such table: rebalance_history".to_string()),
        ..all_present()
    };
    let refusal = channel_evidence(SCID, sources)
        .expect_err("an unreadable diagnostic history is not evidence of no attempts");
    assert!(matches!(
        refusal,
        ProfitabilityEvidenceRefusal::DiagnosticsUnavailable { .. }
    ));
}

#[test]
fn an_unreadable_fee_state_refuses_rather_than_defaulting_to_no_widening() {
    let sources = ConsultedSources {
        posterior_variance: Err("v2_state_json is not valid JSON".to_string()),
        ..all_present()
    };
    let refusal = channel_evidence(SCID, sources)
        .expect_err("an unreadable fee state is not evidence of an unproven channel");
    assert!(matches!(
        refusal,
        ProfitabilityEvidenceRefusal::FeeStateUnavailable { .. }
    ));
}

#[test]
fn a_missing_opener_refuses_rather_than_assuming_we_paid_for_the_channel() {
    // `opener` decides who paid the opening fee. Defaulting it to "local"
    // asserts this node funded a channel it may not have funded, which is
    // a false statement about the operator's costs -- and unlike the three
    // above it is reported directly, not merely fed to the classifier.
    let sources = ConsultedSources {
        opener: Ok(None),
        ..all_present()
    };
    let refusal = channel_evidence(SCID, sources)
        .expect_err("an absent opener must not be defaulted to local");
    assert!(matches!(
        refusal,
        ProfitabilityEvidenceRefusal::OpenerUnavailable { .. }
    ));
}

#[test]
fn every_refusal_names_the_channel_it_is_about() {
    // A fleet pass skips per channel; a refusal that does not say which
    // channel cannot be acted on.
    let sources = ConsultedSources {
        last_routed: Err("io".to_string()),
        ..all_present()
    };
    let refusal = channel_evidence(SCID, sources).expect_err("refuses");
    assert!(
        refusal.scid() == SCID,
        "refusal must carry its channel, got {refusal:?}"
    );
    assert!(
        !refusal.code().is_empty() && refusal.code() != "not_yet_ported",
        "refusals are live conditions, not unported markers"
    );
}

// ---------------------------------------------------------------------
// Consulted-and-absent keeps Python's own defaults.
// ---------------------------------------------------------------------

#[test]
fn a_consulted_empty_source_keeps_pythons_defaults_and_does_not_refuse() {
    // This is the half that must NOT become a refusal. Python's queries
    // legitimately return no row for a channel that has never routed, has
    // no diagnostic attempts, and has no stored fee posterior -- a new
    // channel is exactly that. Refusing here would make every new channel
    // unevaluable.
    let evidence = channel_evidence(SCID, all_consulted_empty())
        .expect("consulted-and-empty is a real answer, not a failure");
    assert_eq!(
        evidence,
        ChannelEvidence {
            last_routed: None,
            diag: DiagStats {
                attempt_count: 0,
                last_success_time: 0,
            },
            posterior_variance: None,
            opener: "local".to_string(),
        },
        "py: attempt_count defaults to 0 (database.py:2811), last_success_time \
         to 0 via `or 0` (profitability_analyzer.py:2674), and posterior_variance \
         to 10000 which is >= 2500 and so widens nothing -- None is exactly that"
    );
}

#[test]
fn present_sources_are_passed_through_unchanged() {
    let evidence = channel_evidence(SCID, all_present()).expect("all sources present");
    assert_eq!(evidence.last_routed, Some(NOW - DAY));
    assert_eq!(evidence.diag.attempt_count, 3);
    assert_eq!(evidence.diag.last_success_time, NOW - 3 * DAY);
    assert_eq!(evidence.posterior_variance, Some(1_000.0));
    assert_eq!(evidence.opener, "remote");
}

#[test]
fn a_sourced_posterior_variance_actually_widens_the_thresholds() {
    // Guards the pass-through above from becoming decorative: a variance
    // below 2500 must reach the classifier and change the verdict, or
    // reading fee state was pointless.
    let ev = |variance: Option<f64>| ClassifyEvidence {
        now: NOW,
        diag_stats: None,
        posterior_variance: variance,
        contribution_30d_msat: None,
    };
    // ROI -0.12: underwater on the default band, break-even once the
    // underwater threshold widens to -0.15.
    assert_eq!(
        classify_channel(-0.12, -1, Some(NOW - DAY), 400, 10, &ev(None)).as_value(),
        "underwater"
    );
    assert_eq!(
        classify_channel(-0.12, -1, Some(NOW - DAY), 400, 10, &ev(Some(1_000.0))).as_value(),
        "break_even",
        "a proven fee posterior must widen the band; dropping it makes the port \
         harsher than Python on exactly the channels Python protects"
    );
}
