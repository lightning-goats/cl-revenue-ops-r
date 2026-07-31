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
        refusal.scid() == Some(SCID),
        "refusal must carry its channel, got {refusal:?}"
    );
    // The store-level variants name no channel on purpose: pinning a
    // whole-store failure on one channel would imply the rest of the fleet
    // was evaluated.
    assert_eq!(
        ProfitabilityEvidenceRefusal::SnapshotUnavailable {
            detail: "io".to_string()
        }
        .scid(),
        None
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

// ---------------------------------------------------------------------
// C71-25: the fee posterior, parsed from the observer store's envelope.
//
// py `_classify_channel` (profitability_analyzer.py:2705-2729) reads
// `v2_state_json`, prefers `fee_state.thompson_state` and falls back to a
// flat `thompson_state`, then takes `posterior_variance` defaulting to
// 10000 -- a value at/above the 2500 widening threshold, so the default
// widens nothing. `None` here IS that default.
//
// Everything in that block is wrapped in `except Exception: pass`, so
// Python cannot tell "no stored posterior" from "the stored posterior is
// unreadable". This port keeps them distinct: absent is Python's answer,
// unreadable is a refusal.
// ---------------------------------------------------------------------

use crate::profitability_evidence::posterior_variance_from_v2_json as parse_posterior;

#[test]
fn a_nested_thompson_posterior_is_the_preferred_source() {
    assert_eq!(
        parse_posterior(r#"{"fee_state":{"thompson_state":{"posterior_variance":1200}}}"#),
        Ok(Some(1200.0))
    );
}

#[test]
fn a_flat_thompson_posterior_is_the_fallback_for_pre_mirror_rows() {
    // py's `or` chain falls back to a top-level `thompson_state` for rows
    // written before the mirror removal. Dropping the fallback silently
    // un-widens every legacy channel's band.
    assert_eq!(
        parse_posterior(r#"{"thompson_state":{"posterior_variance":1200}}"#),
        Ok(Some(1200.0))
    );
}

#[test]
fn an_empty_nested_thompson_state_falls_through_to_the_flat_one() {
    // `(x or {}).get('thompson_state') or v2.get('thompson_state', {})`:
    // an EMPTY nested dict is falsy, so Python falls back. Treating the
    // nested key's mere presence as authoritative diverges here.
    assert_eq!(
        parse_posterior(
            r#"{"fee_state":{"thompson_state":{}},"thompson_state":{"posterior_variance":1200}}"#
        ),
        Ok(Some(1200.0))
    );
}

#[test]
fn a_non_empty_nested_thompson_state_wins_even_when_it_lacks_the_posterior() {
    // The other side of the same `or`: once the nested dict is truthy it
    // IS `ts`, and a missing `posterior_variance` takes the 10000 default
    // rather than reaching for the flat row's value. Falling through here
    // would resurrect a stale legacy posterior and widen a band Python
    // leaves narrow.
    assert_eq!(
        parse_posterior(
            r#"{"fee_state":{"thompson_state":{"alpha":3}},"thompson_state":{"posterior_variance":1200}}"#
        ),
        Ok(None)
    );
}

#[test]
fn a_row_with_no_thompson_state_at_all_is_pythons_own_default() {
    assert_eq!(parse_posterior(r#"{"fee_state":{}}"#), Ok(None));
    assert_eq!(parse_posterior("{}"), Ok(None));
}

#[test]
fn an_empty_envelope_string_is_pythons_own_default() {
    // py: `fee_state.get('v2_state_json', '{}') or '{}'` -- an empty or
    // NULL column reads as `{}`, not as an error.
    assert_eq!(parse_posterior(""), Ok(None));
}

#[test]
fn a_json_null_posterior_is_absent_not_malformed() {
    // py takes the value, then guards `isinstance(variance, (int, float))`.
    // A JSON null fails that guard and widens nothing -- a real answer, and
    // a common one for rows written before the posterior existed. Refusing
    // here would make those channels unevaluable.
    assert_eq!(
        parse_posterior(r#"{"thompson_state":{"posterior_variance":null}}"#),
        Ok(None)
    );
}

#[test]
fn a_non_numeric_posterior_is_a_refusal_not_a_silent_no_widening() {
    // Disclosed divergence. Python's isinstance guard turns a corrupt
    // value into "no widening", indistinguishable from a channel that
    // simply has no posterior. A string where a variance belongs means the
    // fee controller wrote something wrong, and the widening decision would
    // then rest on state nobody can read.
    let refusal = parse_posterior(r#"{"thompson_state":{"posterior_variance":"1200"}}"#)
        .expect_err("a non-numeric posterior must not read as absent");
    assert!(
        refusal.contains("posterior_variance"),
        "the refusal must name the field: {refusal}"
    );
}

#[test]
fn an_unparseable_envelope_is_a_refusal_not_an_absent_posterior() {
    // py's `except Exception: pass` swallows the JSONDecodeError and
    // classifies with an unwidened band. That is could-not-consult being
    // reported as consulted-and-empty.
    let refusal =
        parse_posterior("{not json").expect_err("an unparseable envelope must not read as absent");
    assert!(
        !refusal.is_empty(),
        "the refusal must carry the parse detail"
    );
}

#[test]
fn a_bare_number_envelope_is_a_refusal() {
    // `json.loads("5")` succeeds and returns an int; py's `.get` then
    // raises AttributeError, caught by the same blanket handler. The
    // envelope is not an object, so nothing was consulted.
    assert!(parse_posterior("5").is_err());
}

#[test]
fn an_integer_posterior_is_read_as_a_variance() {
    // The store writes whatever serde produced; an exact integer must not
    // read as malformed.
    assert_eq!(
        parse_posterior(r#"{"thompson_state":{"posterior_variance":2500}}"#),
        Ok(Some(2500.0))
    );
}

#[test]
fn a_thompson_state_that_is_not_an_object_is_a_refusal() {
    // py's `.get` on a non-dict raises AttributeError into the blanket
    // handler, so this too arrives as an unwidened band indistinguishable
    // from a new channel. Structural corruption is the same class of fact
    // as a corrupt value.
    assert!(parse_posterior(r#"{"thompson_state":7}"#).is_err());
    assert!(parse_posterior(r#"{"fee_state":{"thompson_state":7}}"#).is_err());
}

// ---------------------------------------------------------------------
// C71-27: the `or` chain's falsy set, derived by EXECUTING Python.
//
//   ts = ((v2.get('fee_state') or {}).get('thompson_state')
//         or v2.get('thompson_state', {}))
//   variance = ts.get('posterior_variance', 10000)
//
// Two `or`s, each with Python's full falsy set (None/False/0/""/[]/{}),
// and two places that raise into the blanket handler. Verified case by
// case against the interpreter:
//
//   fee_state null/false/0/[]        -> falls through to flat  (`or {}`)
//   fee_state 7 (truthy non-dict)    -> AttributeError
//   nested null/false/0/""/[]/{}     -> falls through to flat
//   nested 7 (truthy non-dict)       -> AttributeError
//   flat present but null            -> AttributeError
//   flat absent, or an empty dict    -> the 10000 default
//
// A falsy `fee_state` is NOT a raise: `None or {}` yields `{}`, so Python
// really does reach the flat row. Refusing there would deny a posterior
// Python honours.
// ---------------------------------------------------------------------

const FLAT_1200: &str = r#""thompson_state":{"posterior_variance":1200}"#;

#[test]
fn every_python_falsy_nested_thompson_state_falls_through_to_the_flat_row() {
    for falsy in ["null", "false", "0", "0.0", r#""""#, "[]", "{}"] {
        let envelope = format!(r#"{{"fee_state":{{"thompson_state":{falsy}}},{FLAT_1200}}}"#);
        assert_eq!(
            parse_posterior(&envelope),
            Ok(Some(1200.0)),
            "nested thompson_state `{falsy}` is Python-falsy and must fall \
             through to the flat pre-mirror row"
        );
    }
}

#[test]
fn every_python_falsy_fee_state_falls_through_to_the_flat_row() {
    // `(x or {})` -- a falsy fee_state becomes an empty dict and the chain
    // continues. This is NOT one of Python's raising cases.
    for falsy in ["null", "false", "0", r#""""#, "[]", "{}"] {
        let envelope = format!(r#"{{"fee_state":{falsy},{FLAT_1200}}}"#);
        assert_eq!(
            parse_posterior(&envelope),
            Ok(Some(1200.0)),
            "fee_state `{falsy}` is Python-falsy; `or {{}}` means Python still \
             reads the flat row, so refusing here would deny a posterior \
             Python honours"
        );
    }
}

#[test]
fn a_truthy_non_object_fee_state_refuses_rather_than_resurrecting_the_flat_row() {
    // Python raises AttributeError here and the blanket handler turns it
    // into "no widening". Falling through to the flat row instead would be
    // strictly worse than either: it would widen a band from a stale
    // pre-mirror posterior on the strength of corrupt state.
    for truthy in ["7", r#""x""#, "[1]", "true"] {
        let envelope = format!(r#"{{"fee_state":{truthy},{FLAT_1200}}}"#);
        assert!(
            parse_posterior(&envelope).is_err(),
            "a truthy non-object fee_state (`{truthy}`) must refuse, not fall \
             through to the flat row"
        );
    }
}

#[test]
fn a_flat_thompson_state_present_but_null_refuses() {
    // `v2.get('thompson_state', {})` returns None when the key is present
    // with a null value -- the default only applies to an ABSENT key -- and
    // `None.get(...)` raises.
    assert!(parse_posterior(r#"{"thompson_state":null}"#).is_err());
}

#[test]
fn an_absent_or_empty_flat_thompson_state_is_the_ten_thousand_default() {
    assert_eq!(parse_posterior("{}"), Ok(None));
    assert_eq!(parse_posterior(r#"{"thompson_state":{}}"#), Ok(None));
}
