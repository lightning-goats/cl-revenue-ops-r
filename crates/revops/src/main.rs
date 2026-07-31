#![forbid(unsafe_code)]

use anyhow::Result;
use cln_plugin::options::{
    BooleanConfigOption, DefaultBooleanConfigOption, DefaultIntegerConfigOption,
    DefaultStringConfigOption, FlagConfigOption, IntegerConfigOption, StringConfigOption,
};
use cln_plugin::{Builder, Plugin};
use revops::config_types;
use revops::cutover_arm::{self, RunningIdentity};
use revops::fee_evidence::MEMPOOL_MA_WINDOW_SECONDS;
use revops::fee_execution::ClnFeeBroadcaster;
use revops::fee_mode::{self, ModeFlags, ValidatedFeeMode};
use revops::options_table::{self, OptDef};
use revops::rpc_dashboard::{build_dashboard, parse_window_days};
use revops::rpc_history::build_history;
use revops::rpc_report::build_report;
use revops::rpc_status::{build_config_response, build_status, StatusInputs};
use revops::{as_bool_default, as_int_default, as_string_default, now_unix};
use revops::{hydration, notify};
use revops_db::fee_runway::FeeStateSnapshot;
use revops_db::queries;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Timeout budget for the guarded live broadcaster's own RPC calls (its
/// restart quarantine reconciliation touches only the Rust-owned observer
/// db, not a socket; this timeout is for the broadcaster's own future live
/// dispatch, per-call).
const LIVE_BROADCASTER_TIMEOUT_SECONDS: u64 = 30;

/// Shared plugin state, resolved once at init (option values, and — if
/// `revops-r-db-path` is set — a persistent read-only DB actor). See
/// `revops::rpc_status` for the pure response builders that consume this.
struct State {
    version: String,
    observer: bool,
    db_path: Option<String>,
    db: Option<revops_db::actor::DbHandle>,
    /// The Rust plugin's OWN read-write notification-ingestion db (never
    /// the production DB — see `revops_db::owner`). `None` when
    /// `observer-db-path` is unset/empty or failed to open; every
    /// subscription handler and startup hydration treat that as a no-op,
    /// never falling back to the read-only `db` connection above.
    observer_db: Option<revops_db::owner::ObserverHandle>,
    /// Real fee-owner handle for diagnostic and notification messages. Every
    /// producer uses the scheduler's bounded ingress; full-cycle cadence also
    /// routes through `AuthorityRuntime::Observer`'s bounded `LoopHandle`.
    scheduler: std::sync::OnceLock<revops::fee_scheduler::SchedulerHandle>,
    /// Async prefetch half retained for the immediate fee-cycle RPC. The
    /// pass owns no broadcaster; all state evolution remains serialized by
    /// the scheduler owner. Live construction is completed by Task69.
    fee_pass: Option<std::sync::Arc<revops::fee_scheduler::FeeObserverPass>>,
    /// Empty until Task69 consumes the whole-plugin live capability and
    /// assembles the sealed core-state mutator owner.
    core_mutations: std::sync::OnceLock<revops::rpc_state_mutators::CoreStateMutationOwner>,
    /// suffix (as accepted by `revenue-r-config`'s `key` param) -> the full
    /// registered option name (shadow- or canonical-mapped).
    config_names: HashMap<String, String>,
    /// Refreshable `listconfigs` snapshot of every `revenue-ops-*`
    /// (Python plugin) option's live resolved value, keyed by the FULL
    /// Python option name (e.g. `revenue-ops-min-fee-ppm`). Fetched at
    /// init and re-fetched before every dispatched fee cycle (2026-07-22
    /// audit M3 — mirroring Python's per-cycle `_refresh_dynamic_config`),
    /// so `setconfig` on dynamic options takes effect without a restart
    /// and an init-time listconfigs outage heals. See
    /// `revops::config_resolve` for the (a) DB override > (b) this cache >
    /// (c) fixture-default precedence `revenue-r-config` resolves through.
    python_options: revops::config_resolve::PythonOptionCache,
    // Validated startup risk profile. Frozen because Python applies profile bundles only at restart.
    active_profile: Result<String, String>,
    /// Task 10: the stable label for the [`ValidatedFeeMode`] this process
    /// resolved at startup (`resolve_startup_mode`) -- one of
    /// `"passive_observer"`, `"autonomous_shadow"`, `"live_authority"`.
    /// Surfaced by the runway status RPC; never changes for the process's
    /// whole lifetime (the mode-matrix options are not `.dynamic()`).
    mode_label: &'static str,
    /// Python-equivalent fee-authority gate state, fixed at startup just
    /// like `mode_label`; only the response's observation time changes.
    fee_authority_status: revops::rpc_fee_authority_status::FeeAuthorityStatusSnapshot,
    /// Whole-plugin authority split. Observer variants hold only observer loop handles; the guarded action adapter exists only inside `LiveRuntime`.
    #[allow(dead_code)]
    authority_runtime: revops::runtime::AuthorityRuntime,
    /// Task 60: the serialized rebalance owner (engine unassembled until
    /// cutover -- the RPCs sit on the Python-parity uninitialized arm) and
    /// the manual RPC's rate limiter + hard cap.
    rebalance_owner: Option<revops::rebalance_owner::RebalanceOwnerHandle>,
    rebalance_rate_limiter: revops::rpc_rebalance_ops::ForceRateLimiter,
    rebalance_hard_cap_sats: i64,
    /// Task 62: the serialized capital owner (adapters/governor
    /// unassembled until Task 69's authority assembly -- planner-execute
    /// sits on the Python-parity uninitialized arm); restart
    /// reconciliation of orphan capital intents still runs.
    capital_owner: Option<revops::capital_owner::CapitalOwnerHandle>,
    /// Task 63: the serialized Boltz owner (action capability
    /// unassembled until Task 69 -- every fund-moving Boltz RPC sits on
    /// the Python-parity uninitialized arm) plus the read-only QUERY
    /// transport its read RPCs use.
    boltz_owner: Option<revops::boltz_owner::BoltzOwnerHandle>,
    boltz_query: std::sync::Arc<dyn revops_boltz::cli::BoltzCli + Send + Sync>,
    /// The resolved Boltz config the transport above was built from.
    boltz_cfg: revops::boltz_config::BoltzCfgSnapshot,
    /// Task 67: THIS process's boot identity. Loop health is judged
    /// against it so a prior boot's pass is never inherited.
    boot_id: String,
    /// Task 44 / A3: the `lightning-rpc` socket path, resolved once at
    /// init (same value the fee-cycle scheduler's `SchedulerConfig` uses)
    /// -- so the `channel_state_changed` subscription's async preparation
    /// half can issue its own fresh RPC prefetch without re-deriving this
    /// path per notification.
    socket_path: PathBuf,
    /// Task 44 / A3: the expanded production DB path (same value the
    /// fee-cycle scheduler's `SchedulerConfig.db_path` uses), for the
    /// async preparation half's out-of-cycle policy read. `None` under
    /// the exact same conditions the scheduler itself would not start.
    production_db_path: Option<PathBuf>,
    /// Task 61 4E: the LN+ observer owner, when this process spawned one
    /// (autonomous shadow with an observer DB). The four operator RPCs go
    /// through its serialization lock for completion acknowledgements;
    /// `None` yields the Python-equivalent "not initialized" responses.
    lnplus: Option<std::sync::Arc<revops::lnplus_runtime::LnPlusObserverPass>>,
}

/// `cln-plugin` clones the state per request; keep it cheap to clone by
/// Arc'ing the actual data. Does NOT hold a DB `Connection` directly (that
/// type is `!Sync`) — `db` is a [`revops_db::actor::DbHandle`], a cheap
/// `Clone`-able `mpsc::Sender` to the single-owner task that actually holds
/// the `Connection` (see `revops_db::actor`).
type SharedState = Arc<State>;

/// suffix -> full registered option name, for every option this plugin
/// exposes: our own (`observer`, `db-path`, `observer-db-path`) plus the
/// entire shadowed Python option surface from `fixtures/options.json`.
///
/// **`observer-db-path` (MINOR b)**: without this entry, `revenue-r-config
/// key=observer-db-path` returned `{"exists": false}` even though the
/// option is registered and resolvable via `p.option_str` -- the only way
/// to introspect the observer's own ingestion-db path was reading
/// lightningd's config/CLI args directly. Same registration pattern as
/// `observer`/`db-path` above.
/// Task 63: the Boltz RPC handlers' dependency bundle, read out of the
/// shared state per call.
fn boltz_rpc_deps(p: &Plugin<SharedState>) -> revops::rpc_boltz_ops::BoltzRpcDeps {
    revops::rpc_boltz_ops::BoltzRpcDeps {
        owner: p.state().boltz_owner.clone(),
        query: p.state().boltz_query.clone(),
        now: now_unix(),
        cfg: p.state().boltz_cfg.clone(),
    }
}

fn config_name_map() -> HashMap<String, String> {
    let mut map = HashMap::new();
    map.insert("observer".to_string(), opt_name("observer"));
    map.insert("db-path".to_string(), opt_name("db-path"));
    map.insert("observer-db-path".to_string(), opt_name("observer-db-path"));
    map.insert("journal-dir".to_string(), opt_name("journal-dir"));
    map.insert("fee-dryrun".to_string(), opt_name("fee-dryrun"));
    // Task 10: the stateful-shadow mode-matrix options -- no Python
    // analogs, same registration pattern as every other Rust-only option
    // above.
    map.insert(
        "fee-stateful-shadow".to_string(),
        opt_name("fee-stateful-shadow"),
    );
    map.insert("fee-broadcast".to_string(), opt_name("fee-broadcast"));
    map.insert("cutover-arm-path".to_string(), opt_name("cutover-arm-path"));
    for opt in options_table::load() {
        let suffix = opt
            .name
            .strip_prefix("revenue-ops-")
            .unwrap_or(&opt.name)
            .to_string();
        map.insert(suffix.clone(), opt_name(&suffix));
    }
    map
}

/// Shadow-vs-canonical naming per design spec (coexistence collision rule).
fn canonical_names() -> bool {
    std::env::var("REVOPS_CANONICAL_NAMES")
        .map(|v| v == "1")
        .unwrap_or(false)
}

/// Expand a leading `~` (bare, or `~/...`) against `$HOME`, mirroring
/// Python's `os.path.expanduser` as used on this exact option
/// (`os.path.expanduser(options['revenue-ops-db-path'])` in
/// `cl-revenue-ops.py`, and again in `Database.__init__`,
/// `modules/database.py:308`). Only the `~`/`~/...` forms are handled (no
/// `~user/...` lookup) -- that is the only form Python's own config ever
/// produces or that this plugin's fixture default uses. No new
/// dependency: `std::env::var("HOME")` only. If `HOME` isn't set, the
/// input is returned unexpanded (same fallback shape as
/// `os.path.expanduser`, which also leaves the string untouched when it
/// can't resolve a home directory).
fn expand_tilde(raw: &str) -> PathBuf {
    if raw == "~" {
        if let Ok(home) = std::env::var("HOME") {
            return PathBuf::from(home);
        }
    } else if let Some(rest) = raw.strip_prefix("~/") {
        if let Ok(home) = std::env::var("HOME") {
            return PathBuf::from(home).join(rest);
        }
    }
    PathBuf::from(raw)
}

/// Task 3 interface (`docs/superpowers/plans/2026-07-17-phase4b-wiring.md`,
/// Task 3): resolve the effective journal directory for the fee
/// controller's dry-run JSONL output. `journal_dir_opt` is the raw,
/// not-yet-expanded `revops-r-journal-dir` option value.
///
/// - **Empty** (`""`, the registered default): resolves to the PARENT
///   directory of `observer_db_path` -- the caller passes the already
///   `~`-expanded `observer-db-path` value here, so no further expansion is
///   needed on that branch. `None` if `observer_db_path` is itself `None`
///   (nothing to derive a location from), matching the doc comment on the
///   registered option ("Empty = parent of observer-db-path").
/// - **Non-empty**: used as-is after [`expand_tilde`] (same tilde-expansion
///   every other path-shaped option in this file goes through), regardless
///   of whether `observer_db_path` is set.
pub fn resolve_journal_dir(
    journal_dir_opt: &str,
    observer_db_path: Option<&std::path::Path>,
) -> Option<PathBuf> {
    if journal_dir_opt.is_empty() {
        observer_db_path.and_then(|p| p.parent().map(std::path::Path::to_path_buf))
    } else {
        Some(expand_tilde(journal_dir_opt))
    }
}

/// True if the observer's own db path (`observer-db-path`, already
/// `~`-expanded) refers to the exact same file as the production db path
/// (`db-path`, also already `~`-expanded, or `None` if production db-path
/// isn't set).
///
/// **Canonicalizes when both files exist.** Pure string equality on the
/// expanded forms misses the realistic case of a symlinked lightning-dir
/// (e.g. lnnode's own `~/.lightning -> /data/lightningd`): an operator can
/// spell the observer's path through the symlink and the production path
/// directly (or vice versa), landing on two textually-different paths that
/// resolve to the exact same underlying file. A bypass here means opening
/// the production DB in the observer's READ-WRITE actor -- not a cosmetic
/// bug. When either path doesn't exist yet (the common case: this check
/// runs before the observer db file is created), `std::fs::canonicalize`
/// can't resolve it, so this falls back to the same string comparison as
/// before -- neither path is required to exist for this function to be
/// callable.
fn observer_db_path_collides_with_production(
    observer_path: &std::path::Path,
    production_path: Option<&std::path::Path>,
) -> bool {
    let Some(production_path) = production_path else {
        return false;
    };
    if observer_path.exists() && production_path.exists() {
        if let (Ok(observer_canon), Ok(production_canon)) = (
            std::fs::canonicalize(observer_path),
            std::fs::canonicalize(production_path),
        ) {
            return observer_canon == production_canon;
        }
    }
    observer_path == production_path
}

/// MINOR (a): each of the four subscription handlers logs once, the FIRST
/// time it sees a notification while `observer_db` is unconfigured, then
/// falls silent for the rest of this process's lifetime. Without this, a
/// live routing node firing `forward_event`/`connect`/`disconnect` at its
/// normal traffic rate with `observer-db-path` unset (or failed to open)
/// would spam one `eprintln!` per notification forever -- pure log noise
/// for a condition that, once true, stays true for the process's whole
/// life (there is no live-reconfiguration path that would later set
/// `observer_db` to `Some`). One `AtomicBool` per subscription topic (not
/// one shared flag) so each topic's own first-drop is still visible in the
/// log, rather than only ever logging whichever topic happens to notify
/// first.
static FORWARD_EVENT_DROP_LOGGED: AtomicBool = AtomicBool::new(false);
static CONNECT_DROP_LOGGED: AtomicBool = AtomicBool::new(false);
static DISCONNECT_DROP_LOGGED: AtomicBool = AtomicBool::new(false);
static CHANNEL_STATE_CHANGED_DROP_LOGGED: AtomicBool = AtomicBool::new(false);

/// Log `revops: debug: {topic} dropped (observer_db not configured)` at
/// most once per `topic`'s `logged` flag (see the flags' doc comment).
/// `Ordering::Relaxed` is sufficient: this only gates whether a debug
/// `eprintln!` happens, never anything else's correctness, so no
/// synchronization-with-other-memory-operations guarantee is needed --
/// just "don't print more than once, near enough."
fn log_observer_db_drop_once(logged: &AtomicBool, topic: &str) {
    if !logged.swap(true, Ordering::Relaxed) {
        eprintln!(
            "revops: debug: {topic} dropped (observer_db not configured); \
             further {topic} drops will not be logged"
        );
    }
}

fn opt_name(suffix: &str) -> String {
    if canonical_names() {
        format!("revenue-ops-{suffix}")
    } else {
        format!("revops-r-{suffix}")
    }
}

fn rpc_name(suffix: &str) -> String {
    if canonical_names() {
        format!("revenue-{suffix}")
    } else {
        format!("revenue-r-{suffix}")
    }
}

/// Register a single Python option under `name` (already shadow- or
/// canonical-mapped by the caller), mapping the table's `opt_type` to the
/// matching cln-plugin 0.7 option constructor. A `null` default registers a
/// valueless/optional variant of the same type.
///
/// **Fail-closed**: if a non-null default fails to parse for its declared
/// `opt_type`, this panics with a clear error message naming the option,
/// type, and bad default. This prevents silent loss of configuration defaults.
fn register_option<S, I, O>(builder: Builder<S, I, O>, name: &str, opt: &OptDef) -> Builder<S, I, O>
where
    O: Send + AsyncWrite + Unpin + 'static,
    S: Clone + Sync + Send + 'static,
    I: AsyncRead + Send + Unpin + 'static,
{
    match opt.opt_type.as_str() {
        "int" => match as_int_default(&opt.default) {
            Some(default) => {
                let mut c = DefaultIntegerConfigOption::new_i64_with_default(
                    name,
                    default,
                    &opt.description,
                );
                if opt.dynamic {
                    c = c.dynamic();
                }
                builder.option(c)
            }
            None => {
                // Only allow None (no default) if the original default was null.
                // Non-null defaults that fail to parse are a configuration error.
                if !opt.default.is_null() {
                    panic!(
                        "option '{}' (type: int) has non-null default that fails to parse as i64: {:?}",
                        opt.name, opt.default
                    );
                }
                let mut c = IntegerConfigOption::new_i64_no_default(name, &opt.description);
                if opt.dynamic {
                    c = c.dynamic();
                }
                builder.option(c)
            }
        },
        "bool" => match as_bool_default(&opt.default) {
            Some(default) => {
                let mut c = DefaultBooleanConfigOption::new_bool_with_default(
                    name,
                    default,
                    &opt.description,
                );
                if opt.dynamic {
                    c = c.dynamic();
                }
                builder.option(c)
            }
            None => {
                // Only allow None (no default) if the original default was null.
                if !opt.default.is_null() {
                    panic!(
                        "option '{}' (type: bool) has non-null default that fails to parse as bool: {:?}",
                        opt.name, opt.default
                    );
                }
                let mut c = BooleanConfigOption::new_bool_no_default(name, &opt.description);
                if opt.dynamic {
                    c = c.dynamic();
                }
                builder.option(c)
            }
        },
        "flag" => {
            let mut c = FlagConfigOption::new_flag(name, &opt.description);
            if opt.dynamic {
                c = c.dynamic();
            }
            builder.option(c)
        }
        // "string" and anything unrecognized: treat as string (matches the
        // extractor's own `opt_type = ... or "string"` fallback).
        _ => match as_string_default(&opt.default) {
            Some(default) => {
                let mut c = DefaultStringConfigOption::new_str_with_default(
                    name,
                    &default,
                    &opt.description,
                );
                if opt.dynamic {
                    c = c.dynamic();
                }
                builder.option(c)
            }
            None => {
                // Only allow None (no default) if the original default was null.
                if !opt.default.is_null() {
                    panic!(
                        "option '{}' (type: string) has non-null default that fails to parse as string: {:?}",
                        opt.name, opt.default
                    );
                }
                let mut c = StringConfigOption::new_str_no_default(name, &opt.description);
                if opt.dynamic {
                    c = c.dynamic();
                }
                builder.option(c)
            }
        },
    }
}

/// Register the full Python option surface (`fixtures/options.json`) under
/// the shadow prefix, or under the original canonical names when
/// `REVOPS_CANONICAL_NAMES=1`.
///
/// **`revenue-ops-db-path` is deliberately skipped here.** Its shadow name
/// (`revops-r-db-path`) is *exactly* the name Task 8 registers directly in
/// `main` for the new DB-probe option — same underlying concept (the sqlite
/// path), but Phase 1 wants an empty-string default (DB probing disabled)
/// rather than the Python plugin's live default of
/// `~/.lightning/revenue_ops.db`. Registering both under the same name would
/// silently collide in cln-plugin's name-keyed option map (last registration
/// wins), so we register it exactly once, under our own definition, instead
/// of double-registering with conflicting defaults. This does not change the
/// total registered-option count (`fixture_len + 1`): the skip here is
/// offset by `main`'s own registration of the same name.
fn resolved_profile_config_json(
    p: &Plugin<SharedState>,
    key: &str,
    overrides: &std::collections::BTreeMap<String, String>,
    python_options: &HashMap<String, cln_plugin::options::Value>,
) -> Result<Option<serde_json::Value>> {
    let state = p.state();
    let Some(full_name) = state.config_names.get(key) else {
        return Ok(None);
    };
    let fixture_value = p.option_str(full_name)?;
    let db_key = revops::config_resolve::db_override_key(key);
    let field_type = config_types::field_type_for(&db_key);
    let db_override = if revops::config_resolve::is_immutable_key(key) {
        None
    } else {
        overrides
            .get(&db_key)
            .and_then(|raw| revops::config_resolve::validate_override(&db_key, raw))
            .map(cln_plugin::options::Value::String)
    };
    let python_value = revops::config_resolve::python_option_name(key)
        .and_then(|python_name| python_options.get(&python_name).cloned())
        .map(|value| match value {
            cln_plugin::options::Value::String(raw)
                if field_type == Some(config_types::FieldType::Bool) =>
            {
                cln_plugin::options::Value::Boolean(config_types::python_startup_bool(
                    &db_key, &raw,
                ))
            }
            other => other,
        });
    Ok(
        revops::config_resolve::resolve_option_value(db_override, python_value, fixture_value)
            .as_ref()
            .map(|raw| config_types::convert_value(field_type, raw)),
    )
}

fn register_profile_preview(
    builder: Builder<SharedState, tokio::io::Stdin, tokio::io::Stdout>,
    name: &str,
    spec: revops::rpc_params::RpcMethodSpec,
) -> Builder<SharedState, tokio::io::Stdin, tokio::io::Stdout> {
    builder.rpcmethod(
        name,
        "preview risk-profile bundle changes without mutating configuration",
        move |p: Plugin<SharedState>, raw: serde_json::Value| {
            let spec = spec.clone();
            async move {
                let decoded = match revops::rpc_params::decode_params(
                    &spec,
                    &raw,
                    revops::rpc_params::ParamBinding::PositionalOrNamed,
                ) {
                    Ok(decoded) => decoded,
                    Err(error) => return Ok(serde_json::json!({"error": error.to_string()})),
                };
                let state = p.state();
                let Some(handle) = state.db.as_ref() else {
                    return Ok(serde_json::json!({"error": "Plugin not fully initialized"}));
                };
                let overrides = match queries::all_config_overrides(handle).await {
                    Ok(overrides) => overrides,
                    Err(error) => return Ok(serde_json::json!({"error": error.to_string()})),
                };
                let mut current = serde_json::Map::new();
                let python_options = state.python_options.snapshot();
                let bundle_keys = revops::rpc_profile_preview::profile_bundles()
                    .values()
                    .flat_map(|bundle| bundle.keys().cloned())
                    .collect::<std::collections::BTreeSet<_>>();
                for key in bundle_keys {
                    let suffix = key.replace("_", "-");
                    match resolved_profile_config_json(&p, &suffix, &overrides, &python_options) {
                        Ok(Some(value)) => {
                            current.insert(key, value);
                        }
                        Ok(None) => {
                            return Ok(serde_json::json!({
                                "error": format!("config value unavailable: {key}")
                            }));
                        }
                        Err(error) => {
                            return Ok(serde_json::json!({"error": format!("{error:#}")}));
                        }
                    }
                }
                let active_profile = match &state.active_profile {
                    Ok(profile) => profile,
                    Err(error) => return Ok(serde_json::json!({"error": error})),
                };
                let explicit_keys = overrides.keys().cloned().collect();
                revops::rpc_profile_preview::apply_active_profile(
                    &mut current,
                    active_profile,
                    &explicit_keys,
                );
                let override_values = overrides
                    .into_iter()
                    .map(|(key, value)| (key, serde_json::Value::String(value)))
                    .collect::<serde_json::Map<_, _>>();
                Ok(revops::rpc_profile_preview::build_profile_preview(
                    &current,
                    active_profile,
                    &override_values,
                    decoded.get("profile"),
                ))
            }
        },
    )
}

fn register_fee_authority_status(
    builder: Builder<SharedState, tokio::io::Stdin, tokio::io::Stdout>,
    name: &str,
    spec: revops::rpc_params::RpcMethodSpec,
) -> Builder<SharedState, tokio::io::Stdin, tokio::io::Stdout> {
    builder.rpcmethod(
        name,
        "report the fixed-at-startup fee-authority gate state",
        move |p: Plugin<SharedState>, raw: serde_json::Value| {
            let spec = spec.clone();
            async move {
                if let Err(error) = revops::rpc_params::decode_params(
                    &spec,
                    &raw,
                    revops::rpc_params::ParamBinding::PositionalOrNamed,
                ) {
                    return Ok(serde_json::json!({"error": error.to_string()}));
                }
                Ok(p.state().fee_authority_status.response(now_unix()))
            }
        },
    )
}

fn register_fee_cycle(
    builder: Builder<SharedState, tokio::io::Stdin, tokio::io::Stdout>,
    name: &str,
    spec: revops::rpc_params::RpcMethodSpec,
) -> Builder<SharedState, tokio::io::Stdin, tokio::io::Stdout> {
    builder.rpcmethod(
        name,
        "run one complete fee adjustment cycle immediately",
        move |p: Plugin<SharedState>, raw: serde_json::Value| {
            let spec = spec.clone();
            async move {
                if let Err(error) = revops::rpc_params::decode_params(
                    &spec,
                    &raw,
                    revops::rpc_params::ParamBinding::PositionalOrNamed,
                ) {
                    return Ok(serde_json::json!({"error": error.to_string()}));
                }
                let state = p.state();
                if let Some(denial) = state.fee_authority_status.fee_cycle_denial_response() {
                    return Ok(denial);
                }
                let Some(pass) = state.fee_pass.as_ref() else {
                    return Ok(serde_json::json!({"error": "Plugin not fully initialized"}));
                };
                match pass.run_with_completion().await {
                    Ok(completed) => {
                        Ok(revops::fee_scheduler::build_fee_cycle_response(&completed))
                    }
                    Err(error) => Ok(serde_json::json!({"error": format!("{error:#}")})),
                }
            }
        },
    )
}

fn register_core_mutator(
    builder: Builder<SharedState, tokio::io::Stdin, tokio::io::Stdout>,
    name: &str,
    spec: revops::rpc_params::RpcMethodSpec,
    action: revops::rpc_state_mutators::CoreStateMutationAction,
) -> Builder<SharedState, tokio::io::Stdin, tokio::io::Stdout> {
    builder.rpcmethod(
        name,
        "apply a completed core-state mutation through the sealed live state writer",
        move |p: Plugin<SharedState>, raw: serde_json::Value| {
            let spec = spec.clone();
            async move {
                let params = match revops::rpc_params::decode_params(
                    &spec,
                    &raw,
                    revops::rpc_params::ParamBinding::PositionalOrNamed,
                ) {
                    Ok(params) => params,
                    Err(error) => return Ok(serde_json::json!({"error": error.to_string()})),
                };
                let Some(owner) = p.state().core_mutations.get() else {
                    return Ok(serde_json::json!({"error": "Plugin not initialized"}));
                };
                Ok(owner.handle(action, &params).await)
            }
        },
    )
}

fn register_rust_diagnostics(
    builder: Builder<SharedState, tokio::io::Stdin, tokio::io::Stdout>,
    ping_name: &str,
    rebalance_plan_name: &str,
) -> Builder<SharedState, tokio::io::Stdin, tokio::io::Stdout> {
    if canonical_names() {
        return builder;
    }
    builder
        .rpcmethod(
            ping_name,
            "liveness probe for the Rust port",
            |_p, _v| async move { Ok(serde_json::json!({"pong": true, "version": VERSION})) },
        )
        .rpcmethod(
            rebalance_plan_name,
            "read-only rebalance plan: what the ported planner WOULD pair (sends nothing)",
            |p: Plugin<SharedState>, _v: serde_json::Value| async move {
                let cfg = p.configuration();
                let socket = PathBuf::from(&cfg.lightning_dir).join(&cfg.rpc_file);
                let mut rpc = match cln_rpc::ClnRpc::new(&socket).await {
                    Ok(rpc) => rpc,
                    Err(error) => {
                        return Ok(serde_json::json!({
                            "error": format!("connect {}: {error}", socket.display())
                        }));
                    }
                };
                let response: serde_json::Value = match rpc
                    .call_raw("listpeerchannels", &serde_json::json!({}))
                    .await
                {
                    Ok(value) => value,
                    Err(error) => {
                        return Ok(serde_json::json!({
                            "error": format!("listpeerchannels: {error}")
                        }));
                    }
                };
                let channels = response
                    .get("channels")
                    .and_then(|value| value.as_array())
                    .map(|values| values.as_slice())
                    .unwrap_or(&[])
                    .iter()
                    .filter_map(|channel| {
                        revops::rpc_rebalance::planner_channel_from_rpc(
                            channel,
                            revops::rpc_rebalance::DEFAULT_BAND_LOW,
                            revops::rpc_rebalance::DEFAULT_BAND_HIGH,
                        )
                    })
                    .collect::<Vec<_>>();
                Ok(revops::rpc_rebalance::build_rebalance_plan(
                    &channels, 200_000, 8, 1_000,
                ))
            },
        )
}

fn register_python_options<S, I, O>(
    mut builder: Builder<S, I, O>,
    canonical: bool,
) -> Builder<S, I, O>
where
    O: Send + AsyncWrite + Unpin + 'static,
    S: Clone + Sync + Send + 'static,
    I: AsyncRead + Send + Unpin + 'static,
{
    for opt in options_table::load() {
        if opt.name == "revenue-ops-db-path" {
            continue;
        }
        let name = if canonical {
            opt.name.clone()
        } else {
            options_table::shadow_name(&opt.name)
        };
        builder = register_option(builder, &name, &opt);
    }
    builder
}

// ---------------------------------------------------------------------------
// Task 10: capability-separated startup mode resolution.
// ---------------------------------------------------------------------------

/// Everything [`resolve_startup_mode`] needs to decide (and, for the live
/// row, atomically consume) which of the three accepted operating modes
/// this process may run in. Every field is caller-resolved -- this
/// function performs no RPC and opens no DB connection itself -- so it is
/// unit-testable without a live plugin process; `main` is the only
/// production caller and is responsible for actually gathering these
/// inputs (a live `getinfo` for the node id, the Rust-owned observer db's
/// read paths for `state`/`seed_event`, `cutover_arm::hash_running_binary`
/// for the running binary's own hash).
pub struct StartupModeInputs<'a> {
    pub flags: ModeFlags,
    /// `Some((arm_path, identity))` iff `revops-r-cutover-arm-path` is a
    /// non-empty, `~`-expanded path -- the arm path and the running
    /// process's identity are resolved TOGETHER (never one without the
    /// other) so this function can never be called in a state where an arm
    /// path exists but there is no identity to validate it against.
    pub cutover_arm: Option<(&'a Path, RunningIdentity)>,
    /// Where a successfully-validated arm is atomically consumed
    /// (`cutover_arm::validate_and_consume`'s `consumed_dir`).
    pub consumed_arm_dir: &'a Path,
    /// The Rust-owned state store's latest snapshot -- read ONLY by
    /// [`fee_mode::validate_fee_mode`]'s autonomous-shadow row (see that
    /// function's own doc comment). The caller passes
    /// [`FeeStateSnapshot::default`] when no Rust-owned store is
    /// configured; combined with `rust_state_store_configured: false` that
    /// case is refused by the mandatory-writable-state gate below before
    /// this default value is ever consulted.
    pub state: &'a FeeStateSnapshot,
    /// Task 42 correction F1: the DERIVED verified seed-binding state
    /// (`fee_runway::verified_seed_binding`), never a raw latest event.
    pub seed_binding: &'a revops_db::fee_runway::SeedBindingState,
    /// Whether the Rust-owned observer db (`revops-r-observer-db-path`) is
    /// configured and successfully opened THIS process lifetime.
    pub rust_state_store_configured: bool,
    /// Task 59 §5.3: the durable cutover-arm nonce deny ledger (the
    /// observer db's owner handle). REQUIRED whenever `cutover_arm` is
    /// `Some` -- consumption is DB-first, and an arm with nowhere durable
    /// to burn its nonce is refused (`ConsumeFailed`) with the file
    /// untouched.
    pub nonce_ledger: Option<&'a revops_db::owner::ObserverHandle>,
    /// The startup clock reading the ledger row records as
    /// `consumed_at`.
    pub now: i64,
}

/// Every fail-closed reason [`resolve_startup_mode`] can return, each
/// naming which gate refused (per the stateful-shadow revision's ruling:
/// "a stable error message states which gate refused").
#[derive(Debug)]
pub enum StartupModeDenyReason {
    /// Autonomous-shadow or live-authority rows require the Rust-owned
    /// observer db to be configured and open -- checked BEFORE any
    /// cutover arm is even read, so a misconfigured deploy never burns a
    /// one-time arm on an environment that could never have supported the
    /// requested mode.
    MissingRustState { mode: &'static str },
    /// The cutover arm itself failed validation (wrong node/commit/hash,
    /// expired, reused nonce, bad file mode, ...).
    ArmInvalid(cutover_arm::CutoverArmDenyReason),
    /// The resolved flags (plus arm presence/absence) matched no accepted
    /// mode-matrix row, or `fee-stateful-shadow=true` with committed state
    /// but no seed-provenance event (see [`fee_mode::FeeModeDenyReason`]).
    Mode(fee_mode::FeeModeDenyReason),
    /// Task 59 §5.4 (F7): this process already resolved its startup mode
    /// once -- [`StartupResolutionToken::take`] returned `None`. A second
    /// in-process resolution is refused regardless of fresh arms.
    AlreadyResolved,
}

impl std::fmt::Display for StartupModeDenyReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingRustState { mode } => write!(
                f,
                "missing_rust_state: {mode} requires revops-r-observer-db-path to be configured \
                 and successfully opened (a writable Rust-owned state store)"
            ),
            Self::ArmInvalid(reason) => write!(f, "cutover_arm_invalid: {reason}"),
            Self::Mode(reason) => write!(f, "{reason}"),
            Self::AlreadyResolved => write!(
                f,
                "already_resolved: this process already resolved its startup mode once; a \
                 second in-process resolution is refused regardless of fresh arms (restart \
                 for a fresh resolution)"
            ),
        }
    }
}

impl std::error::Error for StartupModeDenyReason {}

/// Task 42 correction F1: render the DERIVED verified seed-binding state
/// for the status surfaces -- raw generation/seed fields remain for
/// diagnostics, but THIS field is the one that means anything.
fn seed_binding_json(
    binding: anyhow::Result<revops_db::fee_runway::SeedBindingState>,
) -> serde_json::Value {
    use revops_db::fee_runway::SeedBindingState;
    match binding {
        Ok(SeedBindingState::VirginStore) => serde_json::json!({
            "verified": false, "state": "virgin_store"
        }),
        Ok(SeedBindingState::VerifiedBound { cycle_id }) => serde_json::json!({
            "verified": true, "state": "verified_bound", "cycle_id": cycle_id
        }),
        Ok(SeedBindingState::Invalid { reason }) => serde_json::json!({
            "verified": false, "state": "invalid", "reason": reason
        }),
        Err(e) => serde_json::json!({
            "verified": false, "state": "read_failed", "reason": format!("{e:#}")
        }),
    }
}

/// Resolve which operating mode this process may run in (Task 10 Step 1/2
/// wiring around Task 8's [`fee_mode::validate_fee_mode`] and Task 7's
/// [`cutover_arm::validate_and_consume`]).
///
/// Check order (each gate runs to completion before the next; see this
/// module's tests for the ordering guarantee):
///
/// 1. **Mandatory writable Rust state**: `fee-stateful-shadow=true` or
///    `fee-broadcast=true` without a configured, open Rust-owned observer
///    db is refused HERE, before the cutover arm file (if any) is ever
///    touched -- a broken deploy must never burn a one-time arm.
/// 2. **Cutover arm validation + one-time consumption**: only when
///    `inputs.cutover_arm` is `Some`. This is the only step with a
///    filesystem side effect (the arm is renamed into `consumed_arm_dir`
///    on success) -- run UNCONDITIONALLY on the resolved flags, exactly
///    mirroring `fee_mode`'s own `ArmPresentInNonLiveMode` gate (an arm
///    present alongside a non-live flag combination is itself a
///    misconfiguration the arm is consumed to prove, per that module's
///    doc comment).
/// 3. **The Task 8 mode matrix**: [`fee_mode::validate_fee_mode`] over the
///    resolved flags, the (now possibly `Some`) consumed arm, and the
///    state/seed-event evidence.
///
/// Task 59 §5.4: the PRIVATE pure kernel -- no global state, no token,
/// so the mode-matrix tests exercise it directly and stay
/// order-independent. Arm consumption is DB-FIRST (§5.3): validate
/// (pure), await the durable nonce burn, THEN rename -- a crash between
/// the two leaves the nonce burned and the file present, denied
/// `ReusedNonce` on retry.
async fn resolve_startup_mode_kernel(
    inputs: StartupModeInputs<'_>,
) -> Result<ValidatedFeeMode, StartupModeDenyReason> {
    if (inputs.flags.fee_stateful_shadow || inputs.flags.fee_broadcast)
        && !inputs.rust_state_store_configured
    {
        let mode = if inputs.flags.fee_broadcast {
            "live fee authority mode"
        } else {
            "autonomous fee shadow mode"
        };
        return Err(StartupModeDenyReason::MissingRustState { mode });
    }

    let arm = match inputs.cutover_arm {
        Some((arm_path, identity)) => {
            let validated = cutover_arm::validate(arm_path, &identity)
                .map_err(StartupModeDenyReason::ArmInvalid)?;
            let Some(ledger) = inputs.nonce_ledger else {
                // No durable deny ledger to burn the nonce in: refuse
                // with the arm file untouched rather than consume
                // filesystem-only (the F12 single-loss pair needs both).
                return Err(StartupModeDenyReason::ArmInvalid(
                    cutover_arm::CutoverArmDenyReason::ConsumeFailed(
                        "no durable nonce ledger (observer db) available to burn the arm \
                         nonce in; the arm file is untouched"
                            .to_string(),
                    ),
                ));
            };
            match ledger
                .insert_consumed_arm_nonce(
                    validated.nonce().to_string(),
                    inputs.now,
                    validated.source_commit().to_string(),
                    validated.binary_sha256().to_string(),
                    validated.expires_at(),
                )
                .await
            {
                Ok(true) => {}
                Ok(false) => {
                    return Err(StartupModeDenyReason::ArmInvalid(
                        cutover_arm::CutoverArmDenyReason::ReusedNonce,
                    ));
                }
                Err(e) => {
                    return Err(StartupModeDenyReason::ArmInvalid(
                        cutover_arm::CutoverArmDenyReason::ConsumeFailed(format!(
                            "durable nonce-ledger insert failed ({e:#}); the arm file is \
                             untouched and may be retried by the operator"
                        )),
                    ));
                }
            }
            Some(
                cutover_arm::consume_validated(validated, inputs.consumed_arm_dir)
                    .map_err(StartupModeDenyReason::ArmInvalid)?,
            )
        }
        None => None,
    };

    fee_mode::validate_fee_mode(inputs.flags, arm, inputs.state, inputs.seed_binding)
        .map_err(StartupModeDenyReason::Mode)
}

/// Task 59 §5.4 (F7): minted at most once per process. Private field, no
/// `Clone`, and deliberately NO reset/factory/`cfg(test)` constructor of
/// any kind -- the action-surface scan pins that absence. Exactly one
/// test (`same_process_second_resolution_refuses`) may touch this
/// process-global.
pub struct StartupResolutionToken {
    _private: (),
}

impl StartupResolutionToken {
    /// `Some` exactly once per process, `None` forever after.
    pub fn take() -> Option<Self> {
        static TAKEN: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
        (!TAKEN.swap(true, std::sync::atomic::Ordering::SeqCst)).then_some(Self { _private: () })
    }
}

/// The ONLY public resolution path (Task 59 §5.4): consumes the
/// process-global token by value and delegates to the private kernel.
/// Production has exactly one call site; a second in-process resolution
/// cannot obtain a token and is refused
/// [`StartupModeDenyReason::AlreadyResolved`] at that call site.
pub async fn resolve_startup_mode(
    token: StartupResolutionToken,
    inputs: StartupModeInputs<'_>,
) -> Result<ValidatedFeeMode, StartupModeDenyReason> {
    let _consumed = token;
    resolve_startup_mode_kernel(inputs).await
}

/// A short, stable label for a [`ValidatedFeeMode`] -- surfaced in
/// `State::mode_label` for the status/runway RPCs and startup logging.
pub fn mode_label(mode: &ValidatedFeeMode) -> &'static str {
    match mode {
        ValidatedFeeMode::PassiveObserver(_) => "passive_observer",
        ValidatedFeeMode::AutonomousShadow(_) => "autonomous_shadow",
        ValidatedFeeMode::LiveAuthority(_) => "live_authority",
    }
}

/// One `getinfo` RPC call over a fresh connection -- ONLY made when
/// `revops-r-cutover-arm-path` is non-empty (the common passive-observer/
/// autonomous-shadow startup path never dials this). Mirrors the
/// fresh-connection-per-call rationale every other read-only RPC helper in
/// this crate uses (`fee_evidence::call_rpc`, `hydration::call_listforwards`,
/// `python_authority::call_status_rpc`).
async fn resolve_running_node_id(socket_path: &Path) -> anyhow::Result<String> {
    use anyhow::Context;
    let mut rpc = cln_rpc::ClnRpc::new(socket_path)
        .await
        .with_context(|| format!("connect lightning-rpc socket {}", socket_path.display()))?;
    let info: serde_json::Value = rpc
        .call_raw("getinfo", &serde_json::json!({}))
        .await
        .map_err(|e| anyhow::anyhow!("getinfo RPC error: {e}"))?;
    info.get("id")
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .context("getinfo response missing 'id'")
}

/// Resolve one Python-equivalent configuration suffix through the same
/// three layers as `revenue-r-config`: validated DB override, live Python
/// `listconfigs` snapshot, then this plugin's registered fixture value.
/// Returning typed JSON here keeps planner-status from inventing a second,
/// subtly different configuration path.
async fn resolved_config_json(
    p: &Plugin<SharedState>,
    key: &str,
) -> Result<Option<serde_json::Value>> {
    let s = p.state();
    let Some(full_name) = s.config_names.get(key) else {
        return Ok(None);
    };
    let fixture_value = p.option_str(full_name)?;
    let db_key = revops::config_resolve::db_override_key(key);
    let field_type = config_types::field_type_for(&db_key);
    let (db_override, python_value) = match revops::config_resolve::python_option_name(key) {
        Some(python_name) => {
            // Task 65 slice 3 (W10): a failed layer-(a) read surfaces as an
            // error -- never as "no override".
            let db_override = revops::config_resolve::read_db_override(s.db.as_ref(), key)
                .await
                .map_err(|detail| anyhow::anyhow!("config_override_read_failed: {detail}"))?
                .map(cln_plugin::options::Value::String);
            let python_value = s
                .python_options
                .snapshot()
                .get(&python_name)
                .cloned()
                .map(|v| match v {
                    cln_plugin::options::Value::String(raw)
                        if field_type == Some(config_types::FieldType::Bool) =>
                    {
                        cln_plugin::options::Value::Boolean(config_types::python_startup_bool(
                            &db_key, &raw,
                        ))
                    }
                    other => other,
                });
            (db_override, python_value)
        }
        None => (None, None),
    };
    Ok(
        revops::config_resolve::resolve_option_value(db_override, python_value, fixture_value)
            .as_ref()
            .map(|raw| config_types::convert_value(field_type, raw)),
    )
}

#[tokio::main]
async fn main() -> Result<()> {
    let observer_name = opt_name("observer");
    let db_path_name = opt_name("db-path");
    let observer_opt = DefaultBooleanConfigOption::new_bool_with_default(
        &observer_name,
        true,
        "Run in observer (read-only) mode",
    );
    // Per the design spec's db-path ruling (docs/superpowers/specs/
    // 2026-07-16-rust-port-design.md lines 78-87): in shadow mode (both
    // plugins loaded) the default stays "" -- no accidental DB probe just
    // because this plugin loaded alongside Python. In canonical mode
    // (REVOPS_CANONICAL_NAMES=1, Python unloaded), this Rust plugin IS the
    // only plugin, so the default must be Python's own live default
    // (`fixtures/options.json`'s `revenue-ops-db-path` entry) or an
    // operator relying on the option's default silently loses DB access.
    let db_path_default: String = if canonical_names() {
        options_table::load()
            .into_iter()
            .find(|o| o.name == "revenue-ops-db-path")
            .and_then(|o| as_string_default(&o.default))
            .unwrap_or_default()
    } else {
        String::new()
    };
    let db_path_opt = DefaultStringConfigOption::new_str_with_default(
        &db_path_name,
        &db_path_default,
        "Path to the revops sqlite database, opened read-only at init (empty = disabled)",
    );

    // The Rust plugin's OWN writable sqlite file (Task 2) -- no Python
    // analog, so no shadow/canonical collision risk; `opt_name` is reused
    // purely for naming-prefix consistency with every other option here.
    let observer_db_name = opt_name("observer-db-path");
    let observer_db_opt = DefaultStringConfigOption::new_str_with_default(
        &observer_db_name,
        "~/.lightning/revops-r-observer.db",
        "Path to the Rust plugin's OWN sqlite file (read-write). Never the production DB.",
    );

    // Rust plugin's journal-dir for dry-run JSONL output (Task 3) -- empty
    // default resolves to parent of observer-db-path at scheduler start.
    let journal_dir_name = opt_name("journal-dir");
    let journal_dir_opt = DefaultStringConfigOption::new_str_with_default(
        &journal_dir_name,
        "",
        "Directory for fee-controller dry-run journal (JSONL). Empty = parent of observer-db-path.",
    );

    // T6 (Phase 4b): opt-in switch for the fee-cycle scheduler. Default
    // FALSE so a deploy/restart without explicit opt-in changes nothing
    // (Global Constraint).
    //
    // NOT `.dynamic()` -- final-review finding I4 (2026-07-26). T6
    // advertised it dynamic on the assumption that live-toggle handling
    // would follow, but under the Task 10 mode matrix this option is one
    // of the four inputs to a mode that is validated exactly ONCE, at
    // init, and never re-read. `setconfig revops-r-fee-dryrun false`
    // therefore returned success and changed nothing: a fake off-switch on
    // a live node, which is worse than an absent one. It is now fixed for
    // the process's whole lifetime, exactly like the three mode-matrix
    // options below.
    let fee_dryrun_name = opt_name("fee-dryrun");
    let fee_dryrun_opt = DefaultBooleanConfigOption::new_bool_with_default(
        &fee_dryrun_name,
        false,
        "Run the ported fee controller in dry-run: journal decisions next to the observer db, \
         never broadcast. One of the four operating-mode inputs, validated once at startup: \
         a runtime setconfig has no effect, restart the plugin to change it.",
    );

    // Task 10 (stateful-shadow revision plan): the mode-matrix options.
    // NOT `.dynamic()` -- the operating mode is validated exactly ONCE, at
    // init, before the plugin starts; a runtime `setconfig` flip would be
    // silently ineffective (worse than merely undocumented), so these are
    // deliberately fixed for the process's whole lifetime.
    let fee_stateful_shadow_name = opt_name("fee-stateful-shadow");
    let fee_stateful_shadow_opt = DefaultBooleanConfigOption::new_bool_with_default(
        &fee_stateful_shadow_name,
        false,
        "Run the fee controller as an autonomous Rust-owned shadow: SeedOnce state lifecycle, \
         fixed-interval cycles, Rust mempool evidence, a capability-free recording executor, \
         and governor/ledger auditing -- never a live broadcaster. Requires observer=true, \
         revops-r-fee-dryrun=true, revops-r-fee-broadcast=false, and revops-r-observer-db-path \
         configured.",
    );
    let fee_broadcast_name = opt_name("fee-broadcast");
    let fee_broadcast_opt = DefaultBooleanConfigOption::new_bool_with_default(
        &fee_broadcast_name,
        false,
        "Enable live fee authority. Requires observer=false, revops-r-fee-dryrun=false, \
         revops-r-fee-stateful-shadow=false, revops-r-observer-db-path configured, and a valid, \
         one-time cutover arm at revops-r-cutover-arm-path.",
    );
    let cutover_arm_path_name = opt_name("cutover-arm-path");
    let cutover_arm_path_opt = DefaultStringConfigOption::new_str_with_default(
        &cutover_arm_path_name,
        "",
        "Path to a one-time, mode-0600 cutover-arm JSON file authorizing live fee authority \
         (empty = no arm supplied). Consumed exactly once at startup into <journal-dir>/ \
         cutover-consumed -- the arm file MUST be on the same filesystem as journal-dir (a \
         rename never crosses mounts; a cross-filesystem arm fails consumption with EXDEV) -- \
         and requires journal-dir (or observer-db-path, which journal-dir defaults from) to be \
         resolved at all, or the plugin refuses to start rather than guess a fallback location.",
    );

    let ping_name = rpc_name("ping");
    let status_name = rpc_name("status");
    let config_name = rpc_name("config");
    let profile_preview_name = rpc_name("profile-preview");
    let profile_preview_spec = revops::rpc_params::method_spec(
        &revops::rpc_params::load_rpc_contract(),
        "revenue-profile-preview",
    );
    let fee_authority_status_name = rpc_name("fee-authority-status");
    let fee_authority_status_spec = revops::rpc_params::method_spec(
        &revops::rpc_params::load_rpc_contract(),
        "revenue-fee-authority-status",
    );
    let fee_cycle_name = rpc_name("fee-cycle");
    let fee_cycle_spec = revops::rpc_params::method_spec(
        &revops::rpc_params::load_rpc_contract(),
        "revenue-fee-cycle",
    );
    let ignore_name = rpc_name("ignore");
    let ignore_spec =
        revops::rpc_params::method_spec(&revops::rpc_params::load_rpc_contract(), "revenue-ignore");
    let unignore_name = rpc_name("unignore");
    let unignore_spec = revops::rpc_params::method_spec(
        &revops::rpc_params::load_rpc_contract(),
        "revenue-unignore",
    );
    let ban_name = rpc_name("ban");
    let ban_spec =
        revops::rpc_params::method_spec(&revops::rpc_params::load_rpc_contract(), "revenue-ban");
    let unban_name = rpc_name("unban");
    let unban_spec =
        revops::rpc_params::method_spec(&revops::rpc_params::load_rpc_contract(), "revenue-unban");
    let clear_reservations_name = rpc_name("clear-reservations");
    let clear_reservations_spec = revops::rpc_params::method_spec(
        &revops::rpc_params::load_rpc_contract(),
        "revenue-clear-reservations",
    );
    let spend_release_name = rpc_name("spend-release");
    let spend_release_spec = revops::rpc_params::method_spec(
        &revops::rpc_params::load_rpc_contract(),
        "revenue-spend-release",
    );
    let spend_settle_name = rpc_name("spend-settle");
    let spend_settle_spec = revops::rpc_params::method_spec(
        &revops::rpc_params::load_rpc_contract(),
        "revenue-spend-settle",
    );
    let rebalance_plan_name = rpc_name("rebalance-plan");
    let rebalance_cycle_name = rpc_name("rebalance-cycle");
    let rebalance_debug_name = rpc_name("rebalance-debug");
    let rebalance_manual_name = rpc_name("rebalance");
    let history_name = rpc_name("history");
    let report_name = rpc_name("report");
    let dashboard_name = rpc_name("dashboard");
    // Phase 4b Task 7: fee-controller diagnostic + manual wake, both
    // gated on the fee-cycle scheduler actually running (see the
    // `s.scheduler.get()` checks in each handler below).
    let fee_debug_name = rpc_name("fee-debug");
    let fee_wake_name = if canonical_names() {
        rpc_name("wake-all")
    } else {
        rpc_name("fee-wake")
    };
    // Task 10: the read-only runway status RPC. Deliberately NOT run
    // through `rpc_name()` -- this is a new capability with no Python
    // analog to shadow/canonicalize, so it keeps one fixed name in every
    // mode (per the plan's own literal naming).
    let fee_runway_status_name = "revops-fee-runway-status";

    // Task 49 (Wave 2 / RPC Batch A): ten more read-only response
    // builders, already compiled (see `lib.rs`) but previously
    // unreachable -- nothing registered them. See `RPC_BATCH_A.md` for
    // the full wiring contract each handler below follows.
    let health_name = rpc_name("health");
    let profitability_name = rpc_name("profitability");
    let analyze_name = rpc_name("analyze");
    let policy_name = rpc_name("policy");
    let list_banned_name = rpc_name("list-banned");
    let list_ignored_name = rpc_name("list-ignored");
    let hot_channel_protection_peers_name = rpc_name("hot-channel-protection-peers");
    let capacity_report_name = rpc_name("capacity-report");
    let econ_snapshot_name = rpc_name("econ-snapshot");
    let spend_ledger_name = rpc_name("spend-ledger");

    // Task 56: the read-only planner quartet. These builders were already
    // ported, but were unreachable until their production DB/config seams
    // were wired here.
    let planner_candidate_sources_name = rpc_name("planner-candidate-sources");
    let planner_candidates_name = rpc_name("planner-candidates");
    let planner_history_name = rpc_name("planner-history");
    let planner_status_name = rpc_name("planner-status");
    // Task 62: the write-shaped planner cycle RPC (Python-parity
    // uninitialized arm until Task 69 assembles the capital adapters).
    let planner_execute_name = rpc_name("planner-execute");

    // Task 63: the exact 22 Python-equivalent Boltz RPCs (owner-backed;
    // Python-parity uninitialized arm until Task 69 assembles the action
    // capability).
    let boltz_quote_name = rpc_name("boltz-quote");
    let boltz_loop_out_name = rpc_name("boltz-loop-out");
    let boltz_loop_in_name = rpc_name("boltz-loop-in");
    let boltz_status_name = rpc_name("boltz-status");
    let boltz_history_name = rpc_name("boltz-history");
    let boltz_external_pay_ignores_name = rpc_name("boltz-external-pay-ignores");
    let boltz_budget_name = rpc_name("boltz-budget");
    let boltz_wallet_name = rpc_name("boltz-wallet");
    let boltz_refund_name = rpc_name("boltz-refund");
    let boltz_claim_name = rpc_name("boltz-claim");
    let boltz_chainswap_name = rpc_name("boltz-chainswap");
    let boltz_withdraw_name = rpc_name("boltz-withdraw");
    let boltz_deposit_name = rpc_name("boltz-deposit");
    let boltz_backup_name = rpc_name("boltz-backup");
    let boltz_backup_verify_name = rpc_name("boltz-backup-verify");
    let boltz_balance_recommendations_name = rpc_name("boltz-balance-recommendations");
    let boltz_auto_cycle_status_name = rpc_name("boltz-auto-cycle-status");
    let boltz_auto_cycle_run_now_name = rpc_name("boltz-auto-cycle-run-now");
    let boltz_balance_cycle_name = rpc_name("boltz-balance-cycle");
    let boltz_expansion_treasury_status_name = rpc_name("boltz-expansion-treasury-status");
    let boltz_expansion_treasury_recommendations_name =
        rpc_name("boltz-expansion-treasury-recommendations");
    let boltz_expansion_treasury_cycle_name = rpc_name("boltz-expansion-treasury-cycle");

    // Task 61 4E: the exact four Python-equivalent LN+ operator RPCs
    // (cl-revenue-ops.py:4604-4676), each a completion acknowledgement
    // through the LN+ owner's serialization lock.
    let lnplus_status_name = rpc_name("lnplus-status");
    let lnplus_breaker_clear_name = rpc_name("lnplus-breaker-clear");
    let lnplus_abandon_name = rpc_name("lnplus-abandon");
    let lnplus_backfill_name = rpc_name("lnplus-backfill");

    let builder = Builder::new(tokio::io::stdin(), tokio::io::stdout())
        // Whole-plugin dynamic flag (distinct from per-option `dynamic`):
        // lightningd only allows `plugin start`/`plugin stop` at runtime when
        // the manifest advertises dynamic=true. The shadow-observer deploy
        // model starts and stops this plugin deliberately on a live node.
        .dynamic()
        .option(observer_opt.clone())
        .option(db_path_opt.clone())
        .option(observer_db_opt.clone())
        .option(journal_dir_opt.clone())
        .option(fee_dryrun_opt.clone())
        .option(fee_stateful_shadow_opt.clone())
        .option(fee_broadcast_opt.clone())
        .option(cutover_arm_path_opt.clone())
        .subscribe(
            "forward_event",
            |p: Plugin<SharedState>, v: serde_json::Value| async move {
                match p.state().observer_db.clone() {
                    Some(handle) => notify::on_forward_event(&handle, &v).await,
                    None => log_observer_db_drop_once(&FORWARD_EVENT_DROP_LOGGED, "forward_event"),
                }
                // Fix round 1 (review finding 2): additionally offer this
                // occurrence to the bounded trigger queue (recording-only
                // -- see `CycleOwner::handle_forward_event`), independent
                // of whether the dedup-insert above ran. A `None`
                // scheduler (dry-run off, or it failed to start) is a
                // silent no-op, same as every other trigger source that
                // predates the scheduler existing.
                if let Some(handle) = p.state().scheduler.get() {
                    let channel_id = notify::forward_trigger_channel_id(&v);
                    if handle
                        .tx
                        .send(revops::fee_scheduler::CycleMsg::ForwardEvent { channel_id })
                        .await
                        .is_err()
                    {
                        eprintln!("revops: forward_event scheduler ingress closed");
                    }

                    // Task 44: the fee-relevant FAILURE path (py
                    // cl-revenue-ops.py:6911-6941). Separate from the
                    // generic ForwardEvent trigger above, which stays
                    // recording-only.
                    //
                    // CLN carries failcode/failreason ONLY for
                    // `local_failed` (our node rejected the HTLC); a plain
                    // `failed` is a downstream error inside an onion we
                    // cannot decrypt. `is_fee_relevant_failure` drops both
                    // that and every liquidity failure -- a misdirected
                    // systematic signal is worse than none.
                    //
                    // OUTGOING channel only (audit DTS-4a): per BOLT 7 the
                    // fee a sender pays to traverse us is our policy on the
                    // out channel; the in channel's fee belongs to our peer.
                    if let Some(signal) = notify::failed_forward_signal(&v, revops::now_unix()) {
                        if handle
                            .tx
                            .send(revops::fee_scheduler::CycleMsg::FailedForward(Box::new(
                                signal,
                            )))
                            .await
                            .is_err()
                        {
                            eprintln!("revops: failed-forward scheduler ingress closed");
                        }
                    }
                }
                Ok(())
            },
        )
        .subscribe(
            "connect",
            |p: Plugin<SharedState>, v: serde_json::Value| async move {
                match p.state().observer_db.clone() {
                    Some(handle) => notify::on_connect(&handle, &v).await,
                    None => log_observer_db_drop_once(&CONNECT_DROP_LOGGED, "connect"),
                }
                Ok(())
            },
        )
        .subscribe(
            "disconnect",
            |p: Plugin<SharedState>, v: serde_json::Value| async move {
                match p.state().observer_db.clone() {
                    Some(handle) => notify::on_disconnect(&handle, &v).await,
                    None => log_observer_db_drop_once(&DISCONNECT_DROP_LOGGED, "disconnect"),
                }
                Ok(())
            },
        )
        .subscribe(
            "channel_state_changed",
            |p: Plugin<SharedState>, v: serde_json::Value| async move {
                match p.state().observer_db.clone() {
                    Some(handle) => notify::on_channel_state_changed(&handle, &v).await,
                    None => log_observer_db_drop_once(
                        &CHANNEL_STATE_CHANGED_DROP_LOGGED,
                        "channel_state_changed",
                    ),
                }
                // Task 44 / A3: the new-channel initial-fee producer (py
                // `_handle_channel_open`, cl-revenue-ops.py:7152-7165) --
                // pure parse here, async preparation off the owner thread,
                // then ONE message to the owner (contract §3.1's
                // three-stage boundary). A `None` scheduler (dry-run off,
                // or it failed to start) is a silent no-op, same as every
                // other trigger source that predates the scheduler
                // existing.
                if let Some(signal) = notify::new_channel_signal(&v, revops::now_unix()) {
                    let s = p.state();
                    match (s.scheduler.get(), s.production_db_path.as_ref()) {
                        (Some(handle), Some(db_path)) => {
                            let socket_path = s.socket_path.clone();
                            let db_path = db_path.clone();
                            let db = s.db.clone();
                            let python_options = s.python_options.clone();
                            let tx = handle.tx.clone();
                            tokio::spawn(async move {
                                let preparation = revops::fee_scheduler::prepare_new_channel(
                                    &socket_path,
                                    &db_path,
                                    db.as_ref(),
                                    &python_options,
                                    signal,
                                )
                                .await;
                                if tx
                                    .send(revops::fee_scheduler::CycleMsg::NewChannel(Box::new(
                                        preparation,
                                    )))
                                    .await
                                    .is_err()
                                {
                                    eprintln!("revops: new-channel scheduler ingress closed");
                                }
                            });
                        }
                        _ => {
                            // No scheduler (dry-run off) or no production
                            // DB path -- same silent-no-op posture every
                            // other trigger source has for a `None`
                            // scheduler; A3 cannot prepare without a
                            // production DB to read policy from either.
                        }
                    }
                }
                Ok(())
            },
        )
        .rpcmethod(
            &status_name,
            "status snapshot for the Rust port",
            |p: Plugin<SharedState>, _v| async move {
                let s = p.state();
                // Resolved live via the actor at request time (not an
                // init-time snapshot) so `revenue-r-status` always
                // reflects the DB's current table count.
                let db_tables = match &s.db {
                    Some(handle) => handle.table_count().await.ok(),
                    None => None,
                };
                // Task 5 step 4: latest generation, seed provenance, and
                // restart marker -- resolved live from the Rust-owned
                // observer db, NEVER from Python state.
                let fee_runway = match &s.observer_db {
                    Some(handle) => {
                        let generation = handle
                            .load_latest_fee_state()
                            .await
                            .ok()
                            .map(|snap| snap.generation);
                        let seed = handle.latest_fee_seed_event().await.ok().flatten();
                        let seed_binding = seed_binding_json(
                            handle
                                .verified_seed_binding().await,
                        );
                        let restart = handle.latest_fee_restart_marker().await.ok().flatten();
                        // Task 6: Rust-owned mempool recorder freshness --
                        // resolved live from the observer db, same as
                        // every other `fee_runway` field on this path
                        // (never Python state). Gives operators visibility
                        // into whether the recorder is actually running
                        // ahead of cutover (checklist item 9).
                        let mempool_samples_24h = handle
                            .query_mempool_samples_since(now_unix() - MEMPOOL_MA_WINDOW_SECONDS)
                            .await
                            .ok();
                        let mempool = mempool_samples_24h.map(|rows| {
                            serde_json::json!({
                                "sample_count_24h": rows.len(),
                                "latest_sampled_at": rows.last().map(|r| r.sampled_at),
                            })
                        });
                        Some(serde_json::json!({
                            "generation": generation,
                            "seed_binding": seed_binding,
                            "mempool": mempool,
                            "seed": seed.map(|e| serde_json::json!({
                                "outcome": e.outcome,
                                "seeded_at": e.seeded_at,
                                "source_db_path": e.source_db_path,
                                "source_max_last_update": e.source_max_last_update,
                                "row_count": e.row_count,
                                "payload_sha256": e.payload_sha256,
                                "source_commit": e.source_commit,
                                "refused_channel": e.refused_channel,
                                "refused_field": e.refused_field,
                                "detail": e.detail,
                            })),
                            "restart": restart.map(|m| serde_json::json!({
                                "started_at": m.started_at,
                                "process_id": m.process_id,
                                "prior_generation": m.prior_generation,
                                "hydration_source": m.hydration_source,
                                "source_commit": m.source_commit,
                            })),
                        }))
                    }
                    None => None,
                };
                Ok(build_status(&StatusInputs {
                    version: s.version.clone(),
                    observer: s.observer,
                    db_path: s.db_path.clone(),
                    db_tables,
                    fee_runway,
                }))
            },
        )
        .rpcmethod(
            &rebalance_cycle_name,
            "run one rebalance owner cycle (Python-parity revenue-rebalance-cycle; \
             uninitialized until the engine is assembled at cutover)",
            |p: Plugin<SharedState>, _v: serde_json::Value| async move {
                let s = p.state();
                Ok(revops::rpc_rebalance_ops::handle_rebalance_cycle(
                    s.rebalance_owner.as_ref(),
                )
                .await)
            },
        )
        .rpcmethod(
            &rebalance_debug_name,
            "rebalance diagnostic state (Python-parity revenue-rebalance-debug filters; \
             uninitialized until the engine is assembled at cutover)",
            |p: Plugin<SharedState>, v: serde_json::Value| async move {
                let s = p.state();
                let summary_only = v.get("summary_only").and_then(|x| x.as_bool()).unwrap_or(false);
                let include_hot_markers = v
                    .get("include_hot_markers")
                    .and_then(|x| x.as_bool())
                    .unwrap_or(true);
                Ok(revops::rpc_rebalance_ops::handle_rebalance_debug(
                    s.rebalance_owner.as_ref(),
                    v.get("channel_id"),
                    v.get("peer_id"),
                    summary_only,
                    include_hot_markers,
                    v.get("max_candidates"),
                )
                .await)
            },
        )
        .rpcmethod(
            &rebalance_manual_name,
            "manually trigger a rebalance with profit/budget constraints (Python-parity \
             revenue-rebalance validation; uninitialized until the engine is assembled)",
            |p: Plugin<SharedState>, v: serde_json::Value| async move {
                let s = p.state();
                let force = v.get("force").and_then(|x| x.as_bool()).unwrap_or(false);
                Ok(revops::rpc_rebalance_ops::handle_manual_rebalance(
                    s.rebalance_owner.as_ref(),
                    &s.rebalance_rate_limiter,
                    s.rebalance_hard_cap_sats,
                    now_unix() as f64,
                    v.get("from_channel"),
                    v.get("to_channel"),
                    v.get("amount_sats"),
                    v.get("max_fee_sats"),
                    force,
                )
                .await)
            },
        )
        .rpcmethod(
            &config_name,
            "read a registered option's current resolved value",
            |p: Plugin<SharedState>, v: serde_json::Value| async move {
                let Some(key) = v.get("key").and_then(|k| k.as_str()) else {
                    return Ok(serde_json::json!({"error": "missing 'key' param"}));
                };
                let s = p.state();
                match s.config_names.get(key) {
                    Some(full_name) => {
                        let fixture_value = p.option_str(full_name)?;
                        // `db_key` is the Python `Config` field name (used
                        // for both the override lookup below AND the typed
                        // JSON conversion) -- for the 4 keys in
                        // `config_resolve::FIELD_NAME_OVERRIDES` this
                        // differs from `key.replace('-', "_")`, which is why
                        // this now goes through `db_override_key` rather
                        // than passing `key` straight to `field_type_for`
                        // (CRITICAL 2).
                        let db_key = revops::config_resolve::db_override_key(key);
                        let field_type = config_types::field_type_for(&db_key);
                        // (a) DB override / (b) listconfigs live Python-option
                        // value -- both meaningless for the three Rust-only
                        // keys (see `config_resolve::python_option_name`), so
                        // both stay `None` for those and this falls straight
                        // through to (c) `fixture_value`, unchanged from
                        // before this resolution order existed.
                        let (db_override, python_value) =
                            match revops::config_resolve::python_option_name(key) {
                                Some(python_name) => {
                                    // CRITICAL 4 / `IMMUTABLE_CONFIG_KEYS`
                                    // (modules/config.py:22-25): `dry-run`
                                    // never receives a DB override even if a
                                    // row exists for it -- Python's
                                    // `load_overrides` structurally skips
                                    // applying one, so this skips the query
                                    // entirely rather than fetching-then-
                                    // discarding.
                                    // Task 65 slice 3 (W10): a failed
                                    // layer-(a) read is a typed error --
                                    // never "no override". (The immutable
                                    // skip and CRITICAL-1 validate gate
                                    // live inside read_db_override.)
                                    let db_override = match revops::config_resolve::read_db_override(
                                        s.db.as_ref(),
                                        key,
                                    )
                                    .await
                                    {
                                        Ok(value) => {
                                            value.map(cln_plugin::options::Value::String)
                                        }
                                        Err(detail) => {
                                            return Ok(serde_json::json!({"error": {
                                                "code": "config_override_read_failed",
                                                "message": detail,
                                            }}));
                                        }
                                    };
                                    let python_value = s
                                        .python_options
                                        .snapshot()
                                        .get(&python_name)
                                        .cloned()
                                        // 2026-07-22 audit M2: a layer-(b)
                                        // bool STRING goes through the
                                        // field's Python STARTUP cast, not
                                        // `_apply_override`'s tolerant
                                        // parser (that one is layer-(a)
                                        // only) -- pre-cast here so the
                                        // downstream generic conversion
                                        // passes the native bool through.
                                        .map(|v| match v {
                                            cln_plugin::options::Value::String(raw)
                                                if config_types::field_type_for(&db_key)
                                                    == Some(config_types::FieldType::Bool) =>
                                            {
                                                cln_plugin::options::Value::Boolean(
                                                    config_types::python_startup_bool(
                                                        &db_key, &raw,
                                                    ),
                                                )
                                            }
                                            other => other,
                                        });
                                    (db_override, python_value)
                                }
                                None => (None, None),
                            };
                        let effective = revops::config_resolve::resolve_option_value(
                            db_override,
                            python_value,
                            fixture_value,
                        );
                        // Phase 1b has no DB-backed config-override-write
                        // path yet, so there is no live per-key version to
                        // report; build_config_response documents this
                        // placeholder in its `_phase1b_gaps` array.
                        Ok(build_config_response(
                            key,
                            true,
                            effective.as_ref(),
                            field_type,
                            0,
                        ))
                    }
                    None => Ok(build_config_response(key, false, None, None, 0)),
                }
            },
        )
        .rpcmethod(
            &history_name,
            "lifetime financial history (Phase 1b: fully DB-backed)",
            |p: Plugin<SharedState>, _v| async move {
                let s = p.state();
                let Some(handle) = &s.db else {
                    return Ok(serde_json::json!({"error": "Plugin not initialized"}));
                };
                let now = now_unix();
                let stats = queries::lifetime_stats(handle, now).await?;
                let closed = queries::closed_channels_summary(handle).await?;
                Ok(build_history(&stats, &closed))
            },
        )
        .rpcmethod(
            &report_name,
            "financial/policy reports (Phase 1b: 'costs' is DB-backed; \
             'summary'/'policies'/'peer' are gap-marked, see _phase1b_gaps)",
            |p: Plugin<SharedState>, v: serde_json::Value| async move {
                let s = p.state();
                let report_type = v
                    .get("report_type")
                    .and_then(|t| t.as_str())
                    .unwrap_or("summary");
                if report_type != "costs" {
                    return Ok(build_report(report_type, None, 0));
                }
                let Some(handle) = &s.db else {
                    return Ok(serde_json::json!({"error": "Plugin not initialized"}));
                };
                let now = now_unix();
                let costs = queries::closure_costs_windows(handle, now).await?;
                Ok(build_report(report_type, Some(&costs), now))
            },
        )
        .rpcmethod(
            &dashboard_name,
            "P&L dashboard: period/net_profit/margin from the production DB, \
             TLV from listfunds, annualized ROC from live channel capacity, \
             and bleeder warnings from the windowed profitability snapshot",
            |p: Plugin<SharedState>, v: serde_json::Value| async move {
                let s = p.state();
                let Some(handle) = &s.db else {
                    return Ok(serde_json::json!({"error": "Database not initialized"}));
                };
                let window_days = match parse_window_days(v.get("window_days")) {
                    Ok(w) => w,
                    Err(e) => return Ok(e),
                };
                let now = now_unix();

                // C71-28. These four fields used to be `null`/`[]` under a
                // `_phase1b_gaps` marker. `warnings: []` was the dangerous
                // one: an empty array is a well-formed Python answer meaning
                // "nothing is bleeding", so a node losing money on every
                // channel reported exactly what a healthy one reports.
                let funds = match revops::profitability_assembler::fetch_read_rpc(
                    &s.socket_path,
                    "listfunds",
                )
                .await
                {
                    Ok(funds) => funds,
                    Err(detail) => {
                        return Ok(revops::rpc_dashboard::build_dashboard_unavailable(
                            "dashboard_funds_unavailable",
                            &detail,
                        ))
                    }
                };
                let tlv = match revops::dashboard_evidence::total_liquidating_value(&funds) {
                    Ok(tlv) => tlv,
                    Err(detail) => {
                        return Ok(revops::rpc_dashboard::build_dashboard_unavailable(
                            "dashboard_funds_unavailable",
                            &detail,
                        ))
                    }
                };
                let channels =
                    match revops::profitability_assembler::fetch_channel_snapshot(&s.socket_path)
                        .await
                    {
                        Ok(channels) => channels,
                        Err(detail) => {
                            return Ok(revops::rpc_dashboard::build_dashboard_unavailable(
                                "dashboard_channels_unavailable",
                                &detail,
                            ))
                        }
                    };
                let snapshot = match handle
                    .profitability_snapshot(
                        now,
                        window_days,
                        revops::profitability_assembler::DIAGNOSTIC_WINDOW_DAYS,
                    )
                    .await
                {
                    Ok(snapshot) => snapshot,
                    Err(error) => {
                        return Ok(revops::rpc_dashboard::build_dashboard_unavailable(
                            "dashboard_snapshot_unavailable",
                            &format!("{error:#}"),
                        ))
                    }
                };

                let pnl = queries::pnl_summary(handle, window_days, now).await?;
                let bleeders = revops::dashboard_evidence::bleeder_warnings(
                    &channels,
                    &snapshot.revenue_30d,
                    &snapshot.costs,
                );
                let evidence = revops::rpc_dashboard::DashboardEvidence {
                    tlv_sats: tlv.tlv_sats,
                    annualized_roc_pct: revops::dashboard_evidence::annualized_roc_pct(
                        pnl.net_profit_sats,
                        revops::dashboard_evidence::total_capacity_sats(&channels),
                        window_days,
                    ),
                    warnings: bleeders.warnings,
                    bleeder_count: bleeders.bleeder_count,
                };
                Ok(build_dashboard(&pnl, &evidence))
            },
        )
        .rpcmethod(
            &fee_debug_name,
            "fee-controller diagnostic: one channel's DTS/cycle summary \
             (channel_id param) or the controller-wide summary \
             (last_decision_summary + a per-channel map); requires the \
             fee-cycle scheduler running (autonomous shadow: \
             revops-r-fee-stateful-shadow=true)",
            |p: Plugin<SharedState>, v: serde_json::Value| async move {
                let s = p.state();
                let Some(handle) = s.scheduler.get() else {
                    return Ok(serde_json::json!({
                        "error": "fee-cycle scheduler not running \
                                  (revops-r-fee-stateful-shadow=false, or it failed to start \
                                  -- see plugin log)"
                    }));
                };
                let query = match v.get("channel_id").and_then(|c| c.as_str()) {
                    Some(id) => revops::fee_scheduler::FeeDebugQuery::Channel(id.to_string()),
                    None => revops::fee_scheduler::FeeDebugQuery::Summary,
                };
                // Task 59 §3.3: two-phase bounded bridge -- typed
                // `owner_queue_saturated` (refused admission, nothing
                // enqueued) and `owner_response_timeout` (admitted,
                // response expired) replace the unbounded admission +
                // `recv()` waits. Both are retryable read failures.
                Ok(revops::fee_scheduler::query_owner_bounded(
                    &handle.tx,
                    query,
                    revops::fee_scheduler::RPC_BRIDGE_RECV_TIMEOUT,
                )
                .await)
            },
        )
        .rpcmethod(
            &fee_wake_name,
            "operator/diagnostic: wake every sleeping channel immediately \
             (canonical mode mirrors Python's completed revenue-wake-all response; \
             shadow mode uses the same completed owner path); requires the \
             fee-cycle scheduler running (autonomous shadow: \
             revops-r-fee-stateful-shadow=true)",
            |p: Plugin<SharedState>, _v| async move {
                let s = p.state();
                let Some(handle) = s.scheduler.get() else {
                    return Ok(serde_json::json!({
                        "error": "fee-cycle scheduler not running \
                                  (revops-r-fee-stateful-shadow=false, or it failed to start \
                                  -- see plugin log)"
                    }));
                };
                match handle.wake_all().await {
                    Ok(completed) => Ok(revops::fee_scheduler::build_wake_all_response(
                        &completed,
                    )),
                    Err(error) => Ok(serde_json::json!({"error": error})),
                }
            },
        )
        .rpcmethod(
            fee_runway_status_name,
            "read-only runway status for the stateful-shadow candidate: schema/version, mode, \
             candidate source commit/binary hash, Rust-owned state generation, hydration \
             source, last cycle, trigger queue/drop counters, mempool freshness, \
             governor/ledger health, quarantine, prepared-request count, and mutation-call \
             count. Performs no mutations and never blocks the cycle loop.",
            |p: Plugin<SharedState>, _v| async move {
                let s = p.state();

                // In-memory counters, from the owner thread itself (never
                // blocks the cycle loop -- the owner answers synchronously
                // off its own fields, see `CycleOwner::fee_debug`'s
                // `RunwayCounters` arm). `null` when the scheduler isn't
                // running (passive-observer mode, or it failed to start).
                let mut counters = serde_json::Value::Null;
                if let Some(handle) = s.scheduler.get() {
                    // Task 59 §3.3: same bounded two-phase bridge as
                    // revenue-r-fee-debug (typed, retryable errors).
                    let value = revops::fee_scheduler::query_owner_bounded(
                        &handle.tx,
                        revops::fee_scheduler::FeeDebugQuery::RunwayCounters,
                        revops::fee_scheduler::RPC_BRIDGE_RECV_TIMEOUT,
                    )
                    .await;
                    if value.get("error").is_some() {
                        return Ok(value);
                    }
                    counters = value;
                }

                // Rust-owned store reads: all read-only, resolved live at
                // request time via the actor's async request/reply
                // methods (never a blocking call, never a lock shared
                // with the cycle loop). Fix round 1 (I-5): scalar-only
                // queries (`current_state_generation`/
                // `mempool_sample_stats`) replace the prior
                // `load_latest_fee_state`/`query_mempool_samples_since`
                // calls here -- this RPC only ever needed a number, not
                // every channel's `v2_state_json` row or the 24h mempool
                // row set, and the single-owner actor is the SAME one the
                // cycle loop writes through (head-of-line blocking risk
                // on a busy store). `null` fields when no observer db is
                // configured, matching every other optional field on this
                // path.
                let (
                    state_generation,
                    seed_provenance,
                    seed_binding,
                    quarantine,
                    prepared_request_count,
                    mutation_call_count,
                    mempool,
                ) = match &s.observer_db {
                    Some(handle) => {
                        let generation = handle.current_state_generation().await.ok();
                        // Fix round 1 (I-1): the runway controller consumes
                        // this exact shape (source db path, MAX(last_update),
                        // row count, payload sha256, source commit), mirroring
                        // the full `FeeSeedEventRow` shape `revenue-r-status`
                        // already reports (see that RPC's `fee_runway.seed`
                        // field) for consistency across both.
                        let seed_binding = seed_binding_json(
                            handle
                                .verified_seed_binding().await,
                        );
                        let seed_provenance = handle
                            .latest_fee_seed_event()
                            .await
                            .ok()
                            .flatten()
                            .map(|e| {
                                serde_json::json!({
                                    "outcome": e.outcome,
                                    "seeded_at": e.seeded_at,
                                    "source_db_path": e.source_db_path,
                                    "source_max_last_update": e.source_max_last_update,
                                    "row_count": e.row_count,
                                    "payload_sha256": e.payload_sha256,
                                    "source_commit": e.source_commit,
                                    "refused_channel": e.refused_channel,
                                    "refused_field": e.refused_field,
                                    "detail": e.detail,
                                })
                            });
                        let quarantine = handle
                            .active_execution_quarantine()
                            .await
                            .ok()
                            .flatten()
                            .map(|q| {
                                serde_json::json!({
                                    "id": q.id,
                                    "reason": q.reason,
                                    "cycle_id": q.cycle_id,
                                    "channel_id": q.channel_id,
                                    "request_id": q.request_id,
                                    "entered_at": q.entered_at,
                                })
                            });
                        let prepared_request_count = handle.fee_mutation_count().await.ok();
                        let mutation_call_count = handle.fee_broadcast_attempt_count().await.ok();
                        let mempool_stats = handle
                            .mempool_sample_stats(now_unix() - MEMPOOL_MA_WINDOW_SECONDS)
                            .await
                            .ok();
                        let mempool = mempool_stats.map(|stats| {
                            serde_json::json!({
                                "sample_count_24h": stats.count,
                                "latest_sampled_at": stats.latest_sampled_at,
                            })
                        });
                        (
                            generation,
                            seed_provenance,
                            Some(seed_binding),
                            quarantine,
                            prepared_request_count,
                            mutation_call_count,
                            mempool,
                        )
                    }
                    None => (None, None, None, None, None, None, None),
                };

                Ok(serde_json::json!({
                    "schema_version": "revops_fee_runway_status/v1",
                    "plugin_version": s.version,
                    "mode": s.mode_label,
                    "candidate": {
                        "source_commit": revops::fee_scheduler::source_commit(),
                        "binary_sha256": revops::fee_scheduler::binary_sha256(),
                    },
                    "state_generation": state_generation,
                    "seed_provenance": seed_provenance,
                    "seed_binding": seed_binding,
                    "counters": counters,
                    "mempool": mempool,
                    "quarantine": quarantine,
                    "prepared_request_count": prepared_request_count,
                    "mutation_call_count": mutation_call_count,
                }))
            },
        )
        .rpcmethod(
            &planner_candidate_sources_name,
            "planner candidate pool grouped by source (read-only DB evidence)",
            |p: Plugin<SharedState>, v: serde_json::Value| async move {
                if let Some(err) = revops::rpc_params::reject_positional_params(&v) {
                    return Ok(err);
                }
                let Some(handle) = &p.state().db else {
                    return Ok(serde_json::json!({"error": "Database not initialized"}));
                };
                match queries::planner_candidates(handle, -999.0, None, 100).await {
                    Ok(rows) => {
                        let candidates: Vec<_> = rows
                            .into_iter()
                            .map(|row| revops::rpc_planner_candidate_sources::CandidateRow {
                                peer_id: row.peer_id,
                                score: row.score,
                                source: row.source,
                            })
                            .collect();
                        Ok(
                            revops::rpc_planner_candidate_sources::build_planner_candidate_sources(
                                &candidates,
                            ),
                        )
                    }
                    Err(e) => Ok(serde_json::json!({"error": e.to_string()})),
                }
            },
        )
        .rpcmethod(
            &planner_candidates_name,
            "ranked planner candidates (read-only DB evidence)",
            |p: Plugin<SharedState>, v: serde_json::Value| async move {
                if let Some(err) = revops::rpc_params::reject_positional_params(&v) {
                    return Ok(err);
                }
                let limit = match revops::rpc_planner_candidates::parse_query_limit(
                    v.get("limit"),
                    20,
                    1,
                    1000,
                ) {
                    Ok(limit) => limit,
                    Err(err) => return Ok(err),
                };
                let Some(handle) = &p.state().db else {
                    return Ok(serde_json::json!({"error": "Database not initialized"}));
                };
                match queries::planner_candidates(handle, -999.0, None, limit).await {
                    Ok(rows) => Ok(revops::rpc_planner_candidates::build_planner_candidates(
                        rows.into_iter().map(|row| row.to_json()).collect(),
                    )),
                    Err(e) => Ok(serde_json::json!({"error": e.to_string()})),
                }
            },
        )
        .rpcmethod(
            &boltz_quote_name,
            "quote a Boltz swap (read-only through the query transport)",
            |p: Plugin<SharedState>, v: serde_json::Value| async move {
                if let Some(err) = revops::rpc_params::reject_positional_params(&v) {
                    return Ok(err);
                }
                let deps = boltz_rpc_deps(&p);
                Ok(revops::rpc_boltz_ops::handle_quote(&deps, v.get("amount_sats"), v.get("swap_type"), v.get("currency")).await)
            },
        )
        .rpcmethod(
            &boltz_loop_out_name,
            "submit a Boltz loop-out through the serialized owner",
            |p: Plugin<SharedState>, v: serde_json::Value| async move {
                if let Some(err) = revops::rpc_params::reject_positional_params(&v) {
                    return Ok(err);
                }
                let deps = boltz_rpc_deps(&p);
                Ok(revops::rpc_boltz_ops::handle_loop_out(&deps, v.get("amount_sats"), v.get("address"), v.get("channel_id"), v.get("peer_id"), v.get("currency"), v.get("routing_fee_limit_ppm")).await)
            },
        )
        .rpcmethod(
            &boltz_loop_in_name,
            "submit a Boltz loop-in through the serialized owner",
            |p: Plugin<SharedState>, v: serde_json::Value| async move {
                if let Some(err) = revops::rpc_params::reject_positional_params(&v) {
                    return Ok(err);
                }
                let deps = boltz_rpc_deps(&p);
                Ok(revops::rpc_boltz_ops::handle_loop_in(&deps, v.get("amount_sats"), v.get("channel_id"), v.get("peer_id"), v.get("currency")).await)
            },
        )
        .rpcmethod(
            &boltz_status_name,
            "per-swap Boltz status",
            |p: Plugin<SharedState>, v: serde_json::Value| async move {
                if let Some(err) = revops::rpc_params::reject_positional_params(&v) {
                    return Ok(err);
                }
                let deps = boltz_rpc_deps(&p);
                Ok(revops::rpc_boltz_ops::handle_status(&deps, v.get("swap_id")).await)
            },
        )
        .rpcmethod(
            &boltz_history_name,
            "recent Boltz swaps with a cost summary",
            |p: Plugin<SharedState>, v: serde_json::Value| async move {
                if let Some(err) = revops::rpc_params::reject_positional_params(&v) {
                    return Ok(err);
                }
                let deps = boltz_rpc_deps(&p);
                Ok(revops::rpc_boltz_ops::handle_history(&deps, v.get("limit")).await)
            },
        )
        .rpcmethod(
            &boltz_external_pay_ignores_name,
            "operator-ignored external swaps",
            |p: Plugin<SharedState>, v: serde_json::Value| async move {
                if let Some(err) = revops::rpc_params::reject_positional_params(&v) {
                    return Ok(err);
                }
                let deps = boltz_rpc_deps(&p);
                Ok(revops::rpc_boltz_ops::handle_external_pay_ignores(&deps).await)
            },
        )
        .rpcmethod(
            &boltz_budget_name,
            "Boltz fee budget status",
            |p: Plugin<SharedState>, v: serde_json::Value| async move {
                if let Some(err) = revops::rpc_params::reject_positional_params(&v) {
                    return Ok(err);
                }
                let deps = boltz_rpc_deps(&p);
                Ok(revops::rpc_boltz_ops::handle_budget(&deps).await)
            },
        )
        .rpcmethod(
            &boltz_wallet_name,
            "Boltz wallet list",
            |p: Plugin<SharedState>, v: serde_json::Value| async move {
                if let Some(err) = revops::rpc_params::reject_positional_params(&v) {
                    return Ok(err);
                }
                let deps = boltz_rpc_deps(&p);
                Ok(revops::rpc_boltz_ops::handle_wallet(&deps).await)
            },
        )
        .rpcmethod(
            &boltz_refund_name,
            "refund a Boltz swap through the serialized owner",
            |p: Plugin<SharedState>, v: serde_json::Value| async move {
                if let Some(err) = revops::rpc_params::reject_positional_params(&v) {
                    return Ok(err);
                }
                let deps = boltz_rpc_deps(&p);
                Ok(revops::rpc_boltz_ops::handle_refund(&deps, v.get("swap_id"), v.get("destination")).await)
            },
        )
        .rpcmethod(
            &boltz_claim_name,
            "claim Boltz swaps through the serialized owner",
            |p: Plugin<SharedState>, v: serde_json::Value| async move {
                if let Some(err) = revops::rpc_params::reject_positional_params(&v) {
                    return Ok(err);
                }
                let deps = boltz_rpc_deps(&p);
                Ok(revops::rpc_boltz_ops::handle_claim(&deps, v.get("swap_ids"), v.get("destination")).await)
            },
        )
        .rpcmethod(
            &boltz_chainswap_name,
            "submit a Boltz chain swap through the serialized owner",
            |p: Plugin<SharedState>, v: serde_json::Value| async move {
                if let Some(err) = revops::rpc_params::reject_positional_params(&v) {
                    return Ok(err);
                }
                let deps = boltz_rpc_deps(&p);
                Ok(revops::rpc_boltz_ops::handle_chainswap(&deps, v.get("amount_sats"), v.get("from_currency"), v.get("to_currency"), v.get("to_address"), v.get("to_wallet_name")).await)
            },
        )
        .rpcmethod(
            &boltz_withdraw_name,
            "withdraw from a Boltz wallet through the serialized owner",
            |p: Plugin<SharedState>, v: serde_json::Value| async move {
                if let Some(err) = revops::rpc_params::reject_positional_params(&v) {
                    return Ok(err);
                }
                let deps = boltz_rpc_deps(&p);
                Ok(revops::rpc_boltz_ops::handle_withdraw(&deps, v.get("wallet_name"), v.get("destination"), v.get("currency"), v.get("amount_sats"), v.get("sat_per_vbyte"), v.get("sweep").and_then(serde_json::Value::as_bool).unwrap_or(false), v.get("confirm_sweep").and_then(serde_json::Value::as_bool).unwrap_or(false)).await)
            },
        )
        .rpcmethod(
            &boltz_deposit_name,
            "Boltz wallet deposit address",
            |p: Plugin<SharedState>, v: serde_json::Value| async move {
                if let Some(err) = revops::rpc_params::reject_positional_params(&v) {
                    return Ok(err);
                }
                let deps = boltz_rpc_deps(&p);
                Ok(revops::rpc_boltz_ops::handle_deposit(&deps, v.get("wallet_name")).await)
            },
        )
        .rpcmethod(
            &boltz_backup_name,
            "Boltz wallet backup (mnemonic omitted by default)",
            |p: Plugin<SharedState>, v: serde_json::Value| async move {
                if let Some(err) = revops::rpc_params::reject_positional_params(&v) {
                    return Ok(err);
                }
                let deps = boltz_rpc_deps(&p);
                Ok(revops::rpc_boltz_ops::handle_backup(&deps, v.get("include_mnemonic").and_then(serde_json::Value::as_bool).unwrap_or(false)).await)
            },
        )
        .rpcmethod(
            &boltz_backup_verify_name,
            "verify a Boltz backup mnemonic",
            |p: Plugin<SharedState>, v: serde_json::Value| async move {
                if let Some(err) = revops::rpc_params::reject_positional_params(&v) {
                    return Ok(err);
                }
                let deps = boltz_rpc_deps(&p);
                Ok(revops::rpc_boltz_ops::handle_backup_verify(&deps, v.get("swap_mnemonic")).await)
            },
        )
        .rpcmethod(
            &boltz_balance_recommendations_name,
            "Boltz balance-cycle recommendations",
            |p: Plugin<SharedState>, v: serde_json::Value| async move {
                if let Some(err) = revops::rpc_params::reject_positional_params(&v) {
                    return Ok(err);
                }
                let deps = boltz_rpc_deps(&p);
                Ok(revops::rpc_boltz_ops::handle_balance_recommendations(&deps).await)
            },
        )
        .rpcmethod(
            &boltz_auto_cycle_status_name,
            "Boltz auto-cycle state",
            |p: Plugin<SharedState>, v: serde_json::Value| async move {
                if let Some(err) = revops::rpc_params::reject_positional_params(&v) {
                    return Ok(err);
                }
                let deps = boltz_rpc_deps(&p);
                Ok(revops::rpc_boltz_ops::handle_auto_cycle_status(&deps).await)
            },
        )
        .rpcmethod(
            &boltz_auto_cycle_run_now_name,
            "run one Boltz auto-cycle pass",
            |p: Plugin<SharedState>, v: serde_json::Value| async move {
                if let Some(err) = revops::rpc_params::reject_positional_params(&v) {
                    return Ok(err);
                }
                let deps = boltz_rpc_deps(&p);
                Ok(revops::rpc_boltz_ops::handle_auto_cycle_run_now(&deps, v.get("force").and_then(serde_json::Value::as_bool).unwrap_or(false), v.get("dry_run").and_then(serde_json::Value::as_bool).unwrap_or(false)).await)
            },
        )
        .rpcmethod(
            &boltz_balance_cycle_name,
            "run one Boltz balance cycle",
            |p: Plugin<SharedState>, v: serde_json::Value| async move {
                if let Some(err) = revops::rpc_params::reject_positional_params(&v) {
                    return Ok(err);
                }
                let deps = boltz_rpc_deps(&p);
                Ok(revops::rpc_boltz_ops::handle_balance_cycle(&deps, v.get("dry_run").and_then(serde_json::Value::as_bool).unwrap_or(false)).await)
            },
        )
        .rpcmethod(
            &boltz_expansion_treasury_status_name,
            "Boltz expansion treasury status",
            |p: Plugin<SharedState>, v: serde_json::Value| async move {
                if let Some(err) = revops::rpc_params::reject_positional_params(&v) {
                    return Ok(err);
                }
                let deps = boltz_rpc_deps(&p);
                Ok(revops::rpc_boltz_ops::handle_expansion_treasury_status(&deps).await)
            },
        )
        .rpcmethod(
            &boltz_expansion_treasury_recommendations_name,
            "Boltz expansion treasury recommendations",
            |p: Plugin<SharedState>, v: serde_json::Value| async move {
                if let Some(err) = revops::rpc_params::reject_positional_params(&v) {
                    return Ok(err);
                }
                let deps = boltz_rpc_deps(&p);
                Ok(revops::rpc_boltz_ops::handle_expansion_treasury_recommendations(&deps).await)
            },
        )
        .rpcmethod(
            &boltz_expansion_treasury_cycle_name,
            "run one Boltz expansion treasury cycle",
            |p: Plugin<SharedState>, v: serde_json::Value| async move {
                if let Some(err) = revops::rpc_params::reject_positional_params(&v) {
                    return Ok(err);
                }
                let deps = boltz_rpc_deps(&p);
                Ok(revops::rpc_boltz_ops::handle_expansion_treasury_cycle(&deps, v.get("dry_run").and_then(serde_json::Value::as_bool).unwrap_or(true)).await)
            },
        )
        .rpcmethod(
            &planner_execute_name,
            "run one capacity-planner cycle through the capital owner",
            |p: Plugin<SharedState>, v: serde_json::Value| async move {
                if let Some(err) = revops::rpc_params::reject_positional_params(&v) {
                    return Ok(err);
                }
                Ok(revops::rpc_planner_execute::handle_planner_execute(
                    p.state().capital_owner.as_ref(),
                    // Pre-cutover the adapters gate above always fires
                    // first; this closure is unreachable until Task 69
                    // assembles real evidence deps.
                    || {
                        Err(
                            revops::capital_evidence::EvidenceRefusal::PeerChannelsUnavailable(
                                "capital evidence not assembled (pre-cutover)".to_string(),
                            ),
                        )
                    },
                )
                .await)
            },
        )
        .rpcmethod(
            &planner_history_name,
            "recent planner actions (read-only DB evidence)",
            |p: Plugin<SharedState>, v: serde_json::Value| async move {
                if let Some(err) = revops::rpc_params::reject_positional_params(&v) {
                    return Ok(err);
                }
                let limit = match revops::rpc_planner_candidates::parse_query_limit(
                    v.get("limit"),
                    20,
                    1,
                    1000,
                ) {
                    Ok(limit) => limit,
                    Err(err) => return Ok(err),
                };
                let Some(handle) = &p.state().db else {
                    return Ok(serde_json::json!({"error": "Database not initialized"}));
                };
                match queries::planner_actions(handle, None, limit).await {
                    Ok(rows) => Ok(revops::rpc_planner_history::build_planner_history(
                        rows.into_iter().map(|row| row.to_json()).collect(),
                    )),
                    Err(e) => Ok(serde_json::json!({"error": e.to_string()})),
                }
            },
        )
        .rpcmethod(
            &planner_status_name,
            "planner configuration and recent read-only DB evidence",
            |p: Plugin<SharedState>, v: serde_json::Value| async move {
                if let Some(err) = revops::rpc_params::reject_positional_params(&v) {
                    return Ok(err);
                }
                let Some(handle) = &p.state().db else {
                    return Ok(serde_json::json!({"error": "Database not initialized"}));
                };
                let candidates = match queries::planner_candidates(handle, -999.0, None, 32).await {
                    Ok(rows) => rows,
                    Err(e) => return Ok(serde_json::json!({"error": e.to_string()})),
                };
                let actions = match queries::planner_actions(handle, None, 5).await {
                    Ok(rows) => rows,
                    Err(e) => return Ok(serde_json::json!({"error": e.to_string()})),
                };
                let enabled = resolved_config_json(&p, "planner-enabled")
                    .await?
                    .and_then(|value| value.as_bool())
                    .unwrap_or(false);
                let dry_run = resolved_config_json(&p, "planner-dry-run")
                    .await?
                    .and_then(|value| value.as_bool())
                    .unwrap_or(false);
                let execute_closes = resolved_config_json(&p, "planner-execute-closes")
                    .await?
                    .and_then(|value| value.as_bool())
                    .unwrap_or(false);
                let max_closes_per_cycle =
                    resolved_config_json(&p, "planner-max-closes-per-cycle")
                        .await?
                        .and_then(|value| value.as_i64())
                        .unwrap_or(0)
                        .max(0);
                Ok(revops::rpc_planner_status::build_planner_status(
                    &revops::rpc_planner_status::PlannerStatusInputs {
                        enabled,
                        dry_run,
                        execute_closes,
                        max_closes_per_cycle,
                        candidate_pool_size: candidates.len() as i64,
                        recent_actions: actions
                            .into_iter()
                            .map(|row| row.to_json())
                            .collect(),
                    },
                ))
            },
        )
        .rpcmethod(
            &lnplus_status_name,
            "LN+ swap automation status: breaker, in-flight, active contracts",
            |p: Plugin<SharedState>, v: serde_json::Value| async move {
                if let Some(err) = revops::rpc_params::reject_positional_params(&v) {
                    return Ok(err);
                }
                let Some(pass) = p.state().lnplus.clone() else {
                    // py 4607-4608: automation never initialized.
                    return Ok(serde_json::json!({"enabled": false}));
                };
                match tokio::task::spawn_blocking(move || pass.operator_status()).await {
                    Ok(value) => Ok(value),
                    Err(join) => Ok(serde_json::json!({"error": format!("status task failed: {join}")})),
                }
            },
        )
        .rpcmethod(
            &lnplus_breaker_clear_name,
            "clear the LN+ circuit breaker (operator acknowledgment of the failure)",
            |p: Plugin<SharedState>, v: serde_json::Value| async move {
                if let Some(err) = revops::rpc_params::reject_positional_params(&v) {
                    return Ok(err);
                }
                let Some(pass) = p.state().lnplus.clone() else {
                    return Ok(serde_json::json!({"error": "LN+ automation not initialized"}));
                };
                match tokio::task::spawn_blocking(move || pass.operator_breaker_clear()).await {
                    Ok(value) => Ok(value),
                    Err(join) => Ok(serde_json::json!({"error": format!("breaker-clear task failed: {join}")})),
                }
            },
        )
        .rpcmethod(
            &lnplus_abandon_name,
            "EMERGENCY: abandon an in-flight LN+ swap obligation (defects on a commitment)",
            |p: Plugin<SharedState>, v: serde_json::Value| async move {
                if let Some(err) = revops::rpc_params::reject_positional_params(&v) {
                    return Ok(err);
                }
                let swap_id = match v.get("swap_id").and_then(serde_json::Value::as_str) {
                    Some(swap_id) if !swap_id.is_empty() => swap_id.to_string(),
                    // py 4636-4637: exact usage error for a missing or
                    // non-string swap_id.
                    _ => {
                        return Ok(serde_json::json!({
                            "error": "Usage: revenue-lnplus-abandon <swap_id>"
                        }))
                    }
                };
                let Some(pass) = p.state().lnplus.clone() else {
                    return Ok(serde_json::json!({"error": "LN+ automation not initialized"}));
                };
                match tokio::task::spawn_blocking(move || pass.operator_abandon(&swap_id)).await {
                    Ok(value) => Ok(value),
                    Err(join) => Ok(serde_json::json!({"error": format!("abandon task failed: {join}")})),
                }
            },
        )
        .rpcmethod(
            &lnplus_backfill_name,
            "adopt pre-existing LN+ swaps into the local ledger (safe to repeat)",
            |p: Plugin<SharedState>, v: serde_json::Value| async move {
                if let Some(err) = revops::rpc_params::reject_positional_params(&v) {
                    return Ok(err);
                }
                let Some(pass) = p.state().lnplus.clone() else {
                    return Ok(serde_json::json!({"error": "LN+ automation not initialized"}));
                };
                match tokio::task::spawn_blocking(move || pass.operator_backfill()).await {
                    Ok(value) => Ok(value),
                    Err(join) => Ok(serde_json::json!({"error": format!("backfill task failed: {join}")})),
                }
            },
        )
        .rpcmethod(
            &health_name,
            "consolidated operator health check (Phase: financials.today/.week are \
             DB-backed; annualized_roc_pct and sections 2-9 are gap-marked, see _gaps)",
            |p: Plugin<SharedState>, v: serde_json::Value| async move {
                if let Some(err) = revops::rpc_params::reject_positional_params(&v) {
                    return Ok(err);
                }
                let s = p.state();
                let now = now_unix();
                let loop_rows: Result<Vec<revops_db::loop_health::LoopHealthRow>, String> = match &s.observer_db {
                    Some(store) => store.list_loop_health().await.map_err(|error| format!("{error:#}")),
                    None => Err("Rust-owned observer DB unavailable".to_string()),
                };
                // Round-2 correction, P1 (codex re-review): `db=None` is a
                // REAL, reachable degraded state -- the plugin deliberately
                // comes up running with `db=None` when the DEFAULT db-path
                // misses (see the "default-path miss" ruling in this
                // file's init wiring), not just in some theoretical
                // never-happens branch. Python's `revenue_health` NEVER
                // carries a top-level `error` key; every section is
                // independently populated or gap-marked -- the OLD
                // `{"error": "Plugin not initialized"}` short-circuit here
                // collapsed a PARTIAL evidence-loss condition (no DB, but
                // every other section's honest gap/computed shape is still
                // knowable) into a whole-call failure, discarding
                // generated_at, the honest boltz={"enabled": false}
                // answer, and every gap-section shape a caller could
                // otherwise rely on. `build_health(now, None, None, None)`
                // is exactly what a live `pnl_summary` failure already
                // falls back to below (F11) -- reusing it here for the
                // upfront no-DB case keeps both degraded paths consistent.
                let Some(handle) = &s.db else {
                    return Ok(revops::rpc_health::build_health_with_loops(now, None, None, None, loop_rows.as_ref().map(|rows| rows.as_slice()).map_err(|error| error.clone()), &p.state().boot_id));
                };
                // Task 50 correction round, F11: Python's `revenue_health`
                // try/excepts EACH section independently (cl-revenue-ops.py:
                // 6217-6218) -- a `pnl_summary` DB failure becomes an
                // in-band `financials: {"error": ...}` with the other
                // eight sections still present. The OLD `?` on
                // `pnl_summary(...).await?` instead turned any DB failure
                // into a whole-call JSON-RPC error, losing every section.
                let pnl = async {
                    let pnl_1d = queries::pnl_summary(handle, 1, now).await?;
                    let pnl_7d = queries::pnl_summary(handle, 7, now).await?;
                    Ok::<_, anyhow::Error>((pnl_1d, pnl_7d))
                }
                .await;
                // total_capacity_sats: a live `listpeerchannels` sum -- omit
                // (pass `None`) until that RPC call is wired; annualized_roc_pct
                // will then show as `null` + gap-listed, per the builder's
                // contract.
                match pnl {
                    Ok((pnl_1d, pnl_7d)) => Ok(revops::rpc_health::build_health_with_loops(
                        now, Some(&pnl_1d), Some(&pnl_7d), None,
                        loop_rows.as_ref().map(|rows| rows.as_slice()).map_err(|error| error.clone()),
                        &p.state().boot_id,
                    )),
                    Err(e) => {
                        let mut out = revops::rpc_health::build_health_with_loops(now, None, None, None, loop_rows.as_ref().map(|rows| rows.as_slice()).map_err(|error| error.clone()), &p.state().boot_id);
                        out["financials"] = serde_json::json!({"error": e.to_string()});
                        // This is a LIVE failure, not a declared "not
                        // wired yet" gap -- `_gaps` must not carry it (a
                        // `_gaps` entry tells the harness to skip the
                        // field, which would hide this real failure).
                        if let Some(gaps) = out["_gaps"].as_array_mut() {
                            gaps.retain(|g| g != "financials");
                        }
                        Ok(out)
                    }
                }
            },
        )
        .rpcmethod(
            &profitability_name,
            "channel profitability analysis (single channel_id, or fleet-wide summary), \
             served from one production-DB snapshot, one observer read, and one fresh \
             bounded listpeerchannels snapshot (see revops::profitability_assembler)",
            |p: Plugin<SharedState>, v: serde_json::Value| async move {
                if let Some(err) = revops::rpc_params::reject_positional_params(&v) {
                    return Ok(err);
                }
                // C71-25/C71-27. This used to return `not_yet_ported`
                // because the assembly pipeline did not exist. It exists
                // now, and every input it feeds the frozen classifier was
                // read rather than assumed.
                //
                // The three unavailable branches below are deliberately
                // NOT `build_profitability_channel(id, None)`: that shape
                // is byte-identical to Python's "channel doesn't exist"
                // answer, and returning it for a store outage would tell
                // the operator to close a channel that is fine.
                let s = p.state();
                let channel_id = v.get("channel_id").and_then(|c| c.as_str());
                let unavailable = |code: &str, detail: String| match channel_id {
                    Some(id) => {
                        revops::rpc_profitability::build_profitability_channel_unavailable(
                            id, &detail,
                        )
                    }
                    None => revops::rpc_profitability::build_profitability_unavailable(
                        code, &detail,
                    ),
                };

                let (Some(db), Some(observer)) = (s.db.as_ref(), s.observer_db.as_ref()) else {
                    return Ok(unavailable(
                        "profitability_store_not_configured",
                        "the production database and the observer store must both be \
                         configured before profitability can be evaluated"
                            .to_string(),
                    ));
                };

                // ONE fresh bounded snapshot, taken here and handed to the
                // producer, so the opener every verdict is built from is
                // the opener this call actually saw.
                let channels =
                    match revops::profitability_assembler::fetch_channel_snapshot(&s.socket_path)
                        .await
                    {
                        Ok(channels) => channels,
                        Err(detail) => {
                            return Ok(unavailable("profitability_channels_unavailable", detail))
                        }
                    };

                let fleet = match revops::profitability_assembler::gather_profitability(
                    revops::profitability_assembler::ProfitabilitySources {
                        production_db: db,
                        observer,
                        channels: &channels,
                        now: revops::now_unix(),
                    },
                )
                .await
                {
                    Ok(fleet) => fleet,
                    Err(refusal) => {
                        return Ok(unavailable(refusal.code(), refusal.detail().to_string()))
                    }
                };

                match channel_id {
                    Some(id) => {
                        let scid = id.replace(':', "x");
                        if let Some(result) = fleet.profitability.get(&scid) {
                            return Ok(revops::rpc_profitability::build_profitability_channel(
                                id,
                                Some(result),
                            ));
                        }
                        // A channel this pass skipped is NOT an unknown
                        // channel; only a channel with no costs row at all
                        // is Python's own "No data available".
                        match fleet.skipped.iter().find(|(s, _)| *s == scid) {
                            Some((_, reason)) => Ok(
                                revops::rpc_profitability::build_profitability_channel_unavailable(
                                    id, reason,
                                ),
                            ),
                            None => Ok(revops::rpc_profitability::build_profitability_channel(
                                id, None,
                            )),
                        }
                    }
                    None => {
                        let mut results: Vec<_> =
                            fleet.profitability.values().cloned().collect();
                        results.sort_by(|a, b| a.channel_id.cmp(&b.channel_id));
                        Ok(
                            revops::rpc_profitability::build_profitability_summary_with_skips(
                                &results,
                                &fleet.skipped,
                            ),
                        )
                    }
                }
            },
        )
        .rpcmethod(
            &analyze_name,
            "read-only flow analysis for a single channel_id (SCID); the whole-fleet \
             sweep (no channel_id) is a mutating background job and is NOT ported here",
            |p: Plugin<SharedState>, v: serde_json::Value| async move {
                if let Some(err) = revops::rpc_params::reject_positional_params(&v) {
                    return Ok(err);
                }
                // F71-R23: served from the flow pass's own persisted
                // state, gated on THIS boot having completed a pass. The
                // old wiring answered `NotWired` unconditionally; the
                // store has held real rows since F71-R22, so the marker
                // had become a false statement about this port rather
                // than an honest gap.
                //
                // A malformed/absent `channel_id` never reaches the
                // store: its verdict is a pure function of the parameter.
                let s = p.state();
                let Some(scid) = revops::rpc_analyze::analyze_target_scid(v.get("channel_id"))
                else {
                    return Ok(revops::rpc_analyze::build_analyze(
                        v.get("channel_id"),
                        revops::rpc_analyze::MetricsLookup::Ready(None),
                    ));
                };
                let evidence = revops::flow_evidence::current_boot_flow_evidence(
                    s.observer_db.as_ref(),
                    &scid,
                    &s.boot_id,
                )
                .await;
                Ok(revops::rpc_analyze::build_analyze_from_evidence(
                    v.get("channel_id"),
                    evidence.as_ref(),
                ))
            },
        )
        .rpcmethod(
            &policy_name,
            "peer policy diagnostics (READ-ONLY in this port: list/get/find/changes; \
             set/delete/tag/untag/batch are refused -- see revops::rpc_policy)",
            |p: Plugin<SharedState>, v: serde_json::Value| async move {
                if let Some(err) = revops::rpc_params::reject_positional_params(&v) {
                    return Ok(err);
                }
                let s = p.state();
                // Task 50 correction round, F9: pass the raw `Value` (not
                // `.and_then(as_str)`) so an ABSENT `action` key (Python's
                // signature default, "list") can be told apart from an
                // EXPLICIT `action: null`/non-string (Python's
                // `str(x or "")` -> `""` -> unknown-action error) -- the
                // OLD wiring collapsed both to `None` -> "list".
                let action = revops::rpc_policy::normalize_action(v.get("action"));
                if let Some(err) = revops::rpc_policy::policy_action_gate(&action) {
                    return Ok(err);
                }
                let Some(handle) = &s.db else {
                    return Ok(serde_json::json!({"error": "Plugin not initialized"}));
                };
                let now = now_unix();
                // Task 50 correction round, F11: every DB read below is
                // wrapped so an actor/SQL failure comes back as Python's
                // own in-band shape (`{"status":"error","error":
                // "Unexpected error: {e}"}`, cl-revenue-ops.py's
                // catch-all) instead of a `?`-propagated JSON-RPC error
                // envelope that a `result.get("error")` caller never sees.
                let result: anyhow::Result<serde_json::Value> = async {
                    match action.as_str() {
                        "list" => {
                            let policies = queries::all_policies(handle, now).await?;
                            Ok(revops::rpc_policy::build_policy_list(&policies, now))
                        }
                        "get" => {
                            let Some(peer_id) = v.get("peer_id").and_then(|p| p.as_str()) else {
                                return Ok(revops::rpc_policy::get_usage_error());
                            };
                            if !revops_analytics::policy::is_valid_peer_id(peer_id) {
                                return Ok(revops::rpc_policy::invalid_peer_id_error());
                            }
                            let policy = queries::policy_for_peer(handle, peer_id, now).await?;
                            Ok(revops::rpc_policy::build_policy_get(&policy, now))
                        }
                        "find" => {
                            let Some(tag) = v.get("tag").and_then(|t| t.as_str()) else {
                                return Ok(revops::rpc_policy::find_usage_error());
                            };
                            let policies = queries::policies_by_tag(handle, tag, now).await?;
                            Ok(revops::rpc_policy::build_policy_find(tag, &policies, now))
                        }
                        "changes" => {
                            // Task 50 correction round, F6: Python coerces
                            // `since` via `int(since) if since else 0`
                            // (falsy -> 0, no error) inside a try/except
                            // that returns the exact `invalid_since_error`
                            // string on garbage -- the OLD wiring silently
                            // mapped ANY non-numeric-JSON `since` to `0`
                            // and returned the full table.
                            let since = match revops::rpc_policy::coerce_since(v.get("since")) {
                                Some(since) => since,
                                None => {
                                    return Ok(revops::rpc_policy::invalid_since_error())
                                }
                            };
                            let changes =
                                queries::policy_changes_since(handle, since, now).await?;
                            let last_update =
                                queries::last_policy_change_timestamp(handle).await?;
                            Ok(revops::rpc_policy::build_policy_changes(
                                since,
                                &changes,
                                last_update,
                                now,
                            ))
                        }
                        _ => unreachable!(
                            "policy_action_gate already filtered to the 4 read actions"
                        ),
                    }
                }
                .await;
                Ok(result.unwrap_or_else(|e| {
                    serde_json::json!({"status": "error", "error": format!("Unexpected error: {e}")})
                }))
            },
        )
        .rpcmethod(
            &list_banned_name,
            "peers with an operator ban (revenue-ban)",
            |p: Plugin<SharedState>, v: serde_json::Value| async move {
                if let Some(err) = revops::rpc_params::reject_positional_params(&v) {
                    return Ok(err);
                }
                let Some(handle) = &p.state().db else {
                    return Ok(serde_json::json!({"error": "Plugin not initialized"}));
                };
                // Task 50 correction round, F11: an actor/SQL failure
                // comes back in-band rather than a `?`-propagated
                // JSON-RPC error envelope.
                match queries::all_policies(handle, now_unix()).await {
                    Ok(policies) => Ok(revops::rpc_list_banned::build_list_banned(&policies)),
                    Err(e) => Ok(serde_json::json!({"error": e.to_string()})),
                }
            },
        )
        .rpcmethod(
            &list_ignored_name,
            "DEPRECATED: peers with strategy=passive + rebalance=disabled",
            |p: Plugin<SharedState>, v: serde_json::Value| async move {
                if let Some(err) = revops::rpc_params::reject_positional_params(&v) {
                    return Ok(err);
                }
                let Some(handle) = &p.state().db else {
                    return Ok(serde_json::json!({"error": "Plugin not initialized"}));
                };
                match queries::all_policies(handle, now_unix()).await {
                    Ok(policies) => Ok(revops::rpc_list_ignored::build_list_ignored(&policies)),
                    Err(e) => Ok(serde_json::json!({"error": e.to_string()})),
                }
            },
        )
        .rpcmethod(
            &hot_channel_protection_peers_name,
            "list persistent hot-channel-protection peer overrides (READ-ONLY in \
             this port: add/remove/clear are DB writes and are refused)",
            |p: Plugin<SharedState>, v: serde_json::Value| async move {
                if let Some(err) = revops::rpc_params::reject_positional_params(&v) {
                    return Ok(err);
                }
                // Task 50 correction round, F8: `str(action or
                // "list").lower()`, NO `.strip()` -- the OLD wiring
                // compared the raw string directly against `"list"` with
                // no lowercasing, so `action="LIST"` was wrongly refused
                // and `action=""`/`null` were also wrongly refused
                // (Python defaults both to `list`).
                let action =
                    revops::rpc_hot_channel_protection_peers::normalize_action(v.get("action"));
                if revops::rpc_hot_channel_protection_peers::WRITE_ACTIONS.contains(&action.as_str())
                {
                    // H6: a REAL write action (a genuine scope boundary),
                    // distinct from the unknown-action message below.
                    return Ok(
                        revops::rpc_hot_channel_protection_peers::write_action_refused_error(
                            &action,
                        ),
                    );
                }
                if action != "list" {
                    return Ok(
                        revops::rpc_hot_channel_protection_peers::unknown_action_error(&action),
                    );
                }
                let Some(handle) = &p.state().db else {
                    return Ok(serde_json::json!({"error": "Plugin not initialized"}));
                };
                match queries::hot_channel_protection_override_peers(handle).await {
                    Ok(rows) => Ok(
                        revops::rpc_hot_channel_protection_peers::build_hot_channel_protection_peers_list(
                            &rows,
                        ),
                    ),
                    Err(e) => Ok(serde_json::json!({"error": e.to_string()})),
                }
            },
        )
        .rpcmethod(
            &capacity_report_name,
            "strategic capital redeployment report (Phase: no capacity planner exists \
             yet -- returns Python's own exact \"not initialized\" error, cl-revenue-ops.py:4586-4587)",
            |_p: Plugin<SharedState>, v: serde_json::Value| async move {
                if let Some(err) = revops::rpc_params::reject_positional_params(&v) {
                    return Ok(err);
                }
                // Task 50 correction round, F2: no capacity planner exists
                // in this port. The OLD wiring called
                // `build_capacity_report(now_unix())` unconditionally --
                // a success-shaped 6-key object that hides the
                // "if error in resp" guard every real caller uses. Python's
                // own answer for this exact condition is the 1-key error
                // below (no `timestamp`), so return that instead.
                Ok(revops::rpc_capacity_report::capacity_planner_not_initialized_error())
            },
        )
        .rpcmethod(
            &econ_snapshot_name,
            "READ-ONLY preview of the canonical EconomicSnapshot, assembled from one \
             fresh bounded listpeerchannels snapshot, the profitability evidence \
             gathered against that same snapshot, and a one-transaction budget \
             position (requires econ_shadow_enabled)",
            |p: Plugin<SharedState>, v: serde_json::Value| async move {
                if let Some(err) = revops::rpc_params::reject_positional_params(&v) {
                    return Ok(err);
                }
                // C71-35: the assembly itself lives in
                // `revops::econ_producer` so integration tests can drive
                // every gate against real stores and a fake CLN socket. A
                // handler written inline here could only be checked by
                // reading its source, which proves a call is WRITTEN, not
                // that it BEHAVES.
                let s = p.state();
                let config_error = |error: anyhow::Error| format!("{error:#}");
                Ok(revops::econ_producer::econ_snapshot_response(
                    revops::econ_producer::EconSources {
                        production_db: s.db.as_ref(),
                        observer: s.observer_db.as_ref(),
                        socket_path: &s.socket_path,
                        receivable_ratio_target: resolved_config_json(
                            &p,
                            "receivable-ratio-target",
                        )
                        .await
                        .map(|value| value.and_then(|v| v.as_f64()).unwrap_or(0.0))
                        .map_err(config_error),
                        daily_budget_sats: resolved_config_json(&p, "daily-budget-sats")
                            .await
                            .map(|value| value.and_then(|v| v.as_i64()).unwrap_or(0))
                            .map_err(config_error),
                        enabled: revops::config_resolve::econ_shadow_enabled(s.db.as_ref()).await,
                        now: now_unix(),
                    },
                )
                .await)
            },
        )
        .rpcmethod(
            &spend_ledger_name,
            "summary of generic spend-ledger events/reservations (opens/closes/etc.)",
            |p: Plugin<SharedState>, v: serde_json::Value| async move {
                if let Some(err) = revops::rpc_params::reject_positional_params(&v) {
                    return Ok(err);
                }
                let s = p.state();
                let Some(handle) = &s.db else {
                    return Ok(serde_json::json!({"error": "Database not initialized"}));
                };
                // Task 50 correction round, F7: `window_hours`/
                // `reservation_limit` coerce like Python's `int()`
                // (numeric strings included, garbage errors -- see
                // `rpc_spend_ledger::parse_window_hours`'s doc comment for
                // why this deliberately has NO upper clamp, unlike
                // `_total_cost_budget_status`'s [1,168]);
                // `include_reservations` matches Python truthiness
                // (`bool("false")` is `True`). The OLD wiring silently
                // substituted defaults for anything that wasn't already a
                // JSON number/bool, instead of coercing or erroring.
                let window_hours =
                    match revops::rpc_spend_ledger::parse_window_hours(v.get("window_hours")) {
                        Ok(w) => w,
                        Err(message) => return Ok(serde_json::json!({"error": message})),
                    };
                let include_reservations = revops::rpc_spend_ledger::parse_include_reservations(
                    v.get("include_reservations"),
                );
                let reservation_limit = match revops::rpc_spend_ledger::parse_reservation_limit(
                    v.get("reservation_limit"),
                ) {
                    Ok(n) => n,
                    Err(message) => return Ok(serde_json::json!({"error": message})),
                };
                let now = now_unix();
                // Task 50 correction round, F11: an actor/SQL failure
                // comes back in-band (matching the existing db-None
                // string's style) rather than a `?`-propagated JSON-RPC
                // error envelope.
                let result: anyhow::Result<serde_json::Value> = async {
                    let aggregates =
                        queries::spend_ledger_aggregates(handle, window_hours, now).await?;
                    let reservations = if include_reservations {
                        Some(
                            queries::active_spend_reservations(
                                handle,
                                window_hours,
                                reservation_limit,
                                now,
                            )
                            .await?,
                        )
                    } else {
                        None
                    };
                    Ok(revops::rpc_spend_ledger::build_spend_ledger(
                        window_hours,
                        now,
                        &aggregates,
                        reservations.as_deref(),
                    ))
                }
                .await;
                Ok(result.unwrap_or_else(|e| serde_json::json!({"error": e.to_string()})))
            },
        );
    let builder = register_profile_preview(builder, &profile_preview_name, profile_preview_spec);
    let builder = register_fee_authority_status(
        builder,
        &fee_authority_status_name,
        fee_authority_status_spec,
    );
    let builder = register_fee_cycle(builder, &fee_cycle_name, fee_cycle_spec);
    let builder = register_core_mutator(
        builder,
        &ignore_name,
        ignore_spec,
        revops::rpc_state_mutators::CoreStateMutationAction::Ignore,
    );
    let builder = register_core_mutator(
        builder,
        &unignore_name,
        unignore_spec,
        revops::rpc_state_mutators::CoreStateMutationAction::Unignore,
    );
    let builder = register_core_mutator(
        builder,
        &ban_name,
        ban_spec,
        revops::rpc_state_mutators::CoreStateMutationAction::Ban,
    );
    let builder = register_core_mutator(
        builder,
        &unban_name,
        unban_spec,
        revops::rpc_state_mutators::CoreStateMutationAction::Unban,
    );
    let builder = register_core_mutator(
        builder,
        &clear_reservations_name,
        clear_reservations_spec,
        revops::rpc_state_mutators::CoreStateMutationAction::ClearReservations,
    );
    let builder = register_core_mutator(
        builder,
        &spend_release_name,
        spend_release_spec,
        revops::rpc_state_mutators::CoreStateMutationAction::SpendRelease,
    );
    let builder = register_core_mutator(
        builder,
        &spend_settle_name,
        spend_settle_spec,
        revops::rpc_state_mutators::CoreStateMutationAction::SpendSettle,
    );
    let builder = register_rust_diagnostics(builder, &ping_name, &rebalance_plan_name);
    let builder = register_python_options(builder, canonical_names());

    let Some(configured) = builder.configure().await? else {
        return Ok(()); // lightningd disabled us (or --help) at manifest time
    };

    let observer = configured.option(&observer_opt)?;
    let db_path_raw = configured.option(&db_path_opt)?;
    // Whether the resolved value is exactly the registered default
    // (i.e. the operator never overrode db-path) -- see the
    // default-path-miss ruling below. Computed before db_path_raw is
    // consumed by `then_some`.
    let db_path_is_default = db_path_raw == db_path_default;
    let db_path_setting = (!db_path_raw.is_empty()).then_some(db_path_raw);

    // Spawn the persistent read-only DB actor (`revops_db::actor`) once at
    // init. The actor owns the `Connection` for the plugin's whole
    // lifetime (it is not `Sync` so it cannot live directly in plugin
    // state — only the cheap, `Clone`-able `DbHandle` does); this replaces
    // Phase 1a's Task 8 probe-and-drop (open once, count tables, drop the
    // connection).
    //
    // **Deviation from "any DB-open failure disables the plugin":**
    // Python's `Database.__init__` (modules/database.py:308,338-350)
    // expands `~` and then *creates* the sqlite file (and its parent
    // directory) if it doesn't exist yet — `os.makedirs(..., exist_ok=True)`
    // followed by `sqlite3.connect(...)`, which creates the file, then
    // `CREATE TABLE IF NOT EXISTS` runs unconditionally at startup. This
    // Rust plugin is a read-only *observer* by design (Phase 1a
    // convention: it never writes to or creates the DB). That means a
    // fresh machine that has never run the Python plugin — or an
    // operator who simply hasn't pointed db-path anywhere yet — will
    // always miss on the *default* path, through no misconfiguration of
    // their own. Disabling the whole plugin over that would be a
    // self-inflicted outage for a purely cosmetic gap (no DB-backed
    // status fields). So:
    //   - **default-path miss** (db-path left at the fixture default) →
    //     log a warning to stderr (picked up by lightningd as the
    //     plugin's log, since `cln-plugin`'s default logging redirects
    //     the `log` crate, but we're pre-`start()` here so stderr is the
    //     simplest available channel) and continue with `db=None` — the
    //     plugin still comes up and serves `ping`/`status`/`config`.
    //   - **explicit-path miss** (operator set db-path to something
    //     other than the default and it still doesn't open) → keep the
    //     existing Phase 1a probe-and-disable behavior: a bad *explicit*
    //     path is a real misconfiguration worth surfacing loudly.
    let (db_path, db) = match db_path_setting {
        Some(raw) => {
            let path = expand_tilde(&raw);
            match revops_db::actor::spawn_read_only(&path).await {
                Ok(handle) => (Some(raw), Some(handle)),
                Err(e) if db_path_is_default => {
                    eprintln!(
                        "revops: {db_path_name} default path {} not usable ({e}); continuing \
                         without DB (observer mode, no explicit db-path set)",
                        path.display()
                    );
                    (None, None)
                }
                Err(e) => {
                    configured
                        .disable(&format!(
                            "{db_path_name} set but DB actor spawn failed: {e}"
                        ))
                        .await?;
                    return Ok(());
                }
            }
        }
        None => (None, None),
    };

    // The observer's OWN read-write db (Task 2 -- never the production
    // `db` connection above). Unlike production db-path, a spawn failure
    // here never disables the plugin: it only means notification
    // ingestion is a no-op (per the plan's Global Constraint) while
    // `ping`/`status`/`config` keep working.
    //
    // Path equality (after `~` expansion) against the production db-path
    // is checked first and refused outright: the production connection is
    // a read-only single-owner actor (`revops_db::actor`) and the
    // observer's is a read-write single-owner actor (`revops_db::owner`)
    // -- pointing both at one file breaks the single-owner invariant
    // either actor relies on (and would hand the observer write access to
    // the production DB, which this plugin is never supposed to touch).
    let production_db_path_expanded = db_path.as_deref().map(expand_tilde);
    let observer_db_raw = configured.option(&observer_db_opt)?;
    // Captured by reference (not moved) BEFORE the match below consumes
    // `observer_db_raw` -- Task 3's `resolve_journal_dir` needs the
    // resolved observer-db-path regardless of whether the observer actor
    // itself went on to open successfully (a failed/refused open still
    // names a Rust-owned, writable directory by construction).
    let observer_db_path_expanded: Option<PathBuf> =
        (!observer_db_raw.is_empty()).then(|| expand_tilde(&observer_db_raw));
    let observer_db = match (!observer_db_raw.is_empty()).then_some(observer_db_raw) {
        Some(raw) => {
            let path = expand_tilde(&raw);
            if observer_db_path_collides_with_production(
                &path,
                production_db_path_expanded.as_deref(),
            ) {
                eprintln!(
                    "revops: {observer_db_name} ({}) resolves to the same file as \
                     {db_path_name}; refusing to open it as the observer's own \
                     read-write db (notification ingestion disabled)",
                    path.display()
                );
                None
            } else {
                match revops_db::owner::spawn_read_write(&path).await {
                    Ok(handle) => Some(handle),
                    Err(e) => {
                        eprintln!(
                            "revops: {observer_db_name} spawn failed ({e}); notification \
                             ingestion disabled (never falls back to the production \
                             db-path connection)"
                        );
                        None
                    }
                }
            }
        }
        None => None,
    };

    // Layer (b) of `revenue-r-config`'s resolution order (see
    // `revops::config_resolve`'s doc comment): one `listconfigs` RPC call,
    // cached for the plugin's whole lifetime. Uses the SAME socket-path
    // derivation hydration uses below (`lightning_dir`/`rpc_file` off
    // `Configuration`) -- `ConfiguredPlugin::configuration()` exposes it
    // before `start()`, so this can run synchronously here rather than as
    // a deferred background task like hydration: `listconfigs` is a single
    // fast, local config-file read (no wallet/chain/history paging), so it
    // doesn't carry hydration's "could be slow on a big history" risk that
    // motivated deferring that call instead.
    let init_cfg = configured.configuration();
    let init_socket_path = PathBuf::from(&init_cfg.lightning_dir).join(&init_cfg.rpc_file);
    let python_options = revops::config_resolve::PythonOptionCache::empty();
    // Failure logs and leaves the cache empty; the per-cycle refresh in
    // the fee scheduler (audit M3) retries until lightningd answers.
    let _ = python_options.refresh(&init_socket_path).await;

    // Task 3: resolve `revops-r-journal-dir` once at init (empty default
    // falls back to the parent of the resolved `observer-db-path`; see
    // `resolve_journal_dir`'s doc comment). T6 consumes `State::journal_dir`
    // to build the fee-cycle scheduler's `SchedulerConfig`.
    let journal_dir_raw = configured.option(&journal_dir_opt)?;
    let journal_dir = resolve_journal_dir(&journal_dir_raw, observer_db_path_expanded.as_deref());

    let fee_dryrun = configured.option(&fee_dryrun_opt)?;

    // ------------------------------------------------------------------
    // Task 10: resolve the operating mode BEFORE the plugin ever starts.
    // Every gate here either disables the plugin (loudly, naming which
    // gate refused) or falls through with a validated `ValidatedFeeMode`.
    // ------------------------------------------------------------------
    let fee_stateful_shadow = configured.option(&fee_stateful_shadow_opt)?;
    let fee_broadcast = configured.option(&fee_broadcast_opt)?;
    let cutover_arm_path_raw = configured.option(&cutover_arm_path_opt)?;
    let cutover_arm_path_expanded: Option<PathBuf> =
        (!cutover_arm_path_raw.is_empty()).then(|| expand_tilde(&cutover_arm_path_raw));

    let flags = ModeFlags {
        observer,
        fee_dryrun,
        fee_broadcast,
        fee_stateful_shadow,
    };
    let rust_state_store_configured = observer_db.is_some();

    // Rust-owned state/seed-provenance evidence, read ONLY here at init,
    // straight off the observer db actor (never Python state). A read
    // FAILURE must fail closed -- it must never be treated as "virgin
    // store" (that would let a broken store slip through as
    // `PendingFirstCycle`).
    let (fee_state_snapshot, fee_seed_binding) = match &observer_db {
        Some(handle) => {
            let snap = match handle.load_latest_fee_state().await {
                Ok(snap) => snap,
                Err(e) => {
                    configured
                        .disable(&format!(
                            "stateful-shadow mode gate: failed to read Rust-owned state \
                             generation: {e:#}"
                        ))
                        .await?;
                    return Ok(());
                }
            };
            // Task 42 correction F1: derive the VERIFIED binding state; a
            // read/decode failure fails closed exactly like a state-read
            // failure (never treated as virgin).
            let seed = match handle.verified_seed_binding().await {
                Ok(binding) => binding,
                Err(e) => {
                    configured
                        .disable(&format!(
                            "stateful-shadow mode gate: failed to derive Rust-owned seed \
                             binding: {e:#}"
                        ))
                        .await?;
                    return Ok(());
                }
            };
            (snap, seed)
        }
        None => (
            FeeStateSnapshot::default(),
            revops_db::fee_runway::SeedBindingState::VirginStore,
        ),
    };

    // Mirrors `resolve_startup_mode`'s OWN check order (mandatory writable
    // Rust state before any arm handling): if that gate is going to refuse
    // anyway, skip resolving the cutover identity entirely here too --
    // otherwise a broken deploy (no Rust-owned store) that also happens to
    // have no live `lightning-rpc` socket would report the WRONG gate
    // (a `getinfo` failure) instead of the more fundamental
    // `missing_rust_state` refusal.
    let mandatory_state_gate_would_fail =
        (fee_stateful_shadow || fee_broadcast) && !rust_state_store_configured;

    // Fix round 1 (coordinator ruling I-7): `consumed_arm_dir` (below) IS
    // the nonce-replay ledger -- the only defence against an operator
    // re-consuming a copy of an already-consumed arm. It is pinned to
    // `journal_dir` UNCONDITIONALLY, with NO fallback: a cutover arm
    // supplied with no journal_dir resolved has nowhere safe to be
    // consumed into (falling back to the arm file's own parent directory
    // would silently create a SECOND, different consumption ledger,
    // letting the very same nonce be consumed twice across the two
    // different fallback locations). Refuse outright, before ever
    // touching the arm file -- and before the `getinfo` call just below,
    // which would otherwise run first and report a less fundamental
    // failure.
    if cutover_arm_path_expanded.is_some() && journal_dir.is_none() {
        configured
            .disable(&format!(
                "cutover arm gate: {cutover_arm_path_name} is set but no journal-dir resolved \
                 to derive the one-time-consumption ledger location from (set \
                 {journal_dir_name} or {observer_db_name}); refusing to touch the arm file"
            ))
            .await?;
        return Ok(());
    }

    // The cutover arm and the running process's identity are resolved
    // TOGETHER (see `StartupModeInputs::cutover_arm`'s doc comment): a
    // `getinfo` call and a fresh self-hash, made ONLY when an arm path was
    // actually supplied AND the mandatory-state gate won't refuse first --
    // the common passive-observer/autonomous-shadow startup path never
    // dials either.
    let cutover_identity: Option<RunningIdentity> = match &cutover_arm_path_expanded {
        Some(_) if !mandatory_state_gate_would_fail => {
            let node_id = match resolve_running_node_id(&init_socket_path).await {
                Ok(id) => id,
                Err(e) => {
                    configured
                        .disable(&format!(
                            "cutover arm gate: failed to resolve running node identity via \
                             getinfo: {e:#}"
                        ))
                        .await?;
                    return Ok(());
                }
            };
            let binary_sha256 = match cutover_arm::hash_running_binary() {
                Ok(hash) => hash,
                Err(e) => {
                    configured
                        .disable(&format!(
                            "cutover arm gate: failed to hash the running binary: {e}"
                        ))
                        .await?;
                    return Ok(());
                }
            };
            Some(RunningIdentity {
                node_id,
                subsystem: cutover_arm::CUTOVER_SUBSYSTEM_FEES.to_string(),
                source_commit: revops::fee_scheduler::source_commit().to_string(),
                binary_sha256,
                owner_uid: rustix::process::getuid().as_raw(),
                now: now_unix(),
            })
        }
        _ => None,
    };

    // Where a successfully-validated arm is atomically consumed -- pinned
    // to `journal_dir` UNCONDITIONALLY (fix round 1, I-7): the gate above
    // already guarantees `journal_dir.is_some()` whenever an arm was
    // supplied, so `unwrap_or_default()` here only ever produces a
    // placeholder value when NO arm was supplied (never consulted in that
    // case). No fallback to the arm file's own parent directory -- see the
    // gate's own doc comment for why that would be unsafe.
    let consumed_arm_dir: PathBuf = journal_dir
        .clone()
        .unwrap_or_default()
        .join("cutover-consumed");

    // Task 59 §5.4: the one-per-process resolution token -- production's
    // single call site. A `None` here would mean a second in-process
    // resolution attempt: refused typed, never re-run.
    let resolution = match StartupResolutionToken::take() {
        Some(token) => {
            resolve_startup_mode(
                token,
                StartupModeInputs {
                    flags,
                    cutover_arm: cutover_arm_path_expanded.as_deref().zip(cutover_identity),
                    consumed_arm_dir: &consumed_arm_dir,
                    state: &fee_state_snapshot,
                    seed_binding: &fee_seed_binding,
                    rust_state_store_configured,
                    nonce_ledger: observer_db.as_ref(),
                    now: now_unix(),
                },
            )
            .await
        }
        None => Err(StartupModeDenyReason::AlreadyResolved),
    };
    let mode = match resolution {
        Ok(mode) => mode,
        Err(reason) => {
            configured
                .disable(&format!("stateful-shadow mode gate refused: {reason}"))
                .await?;
            return Ok(());
        }
    };

    let resolved_mode_label = mode_label(&mode);
    let scheduler = std::sync::OnceLock::new();
    let core_mutations = std::sync::OnceLock::new();
    let authority_plan = mode.into_authority_plan(|live_mode| {
        let store = observer_db
            .clone()
            .expect("live authority mode gate guarantees observer_db is Some");
        ClnFeeBroadcaster::new(
            init_socket_path.clone(),
            store,
            LIVE_BROADCASTER_TIMEOUT_SECONDS,
            live_mode,
        )
    });
    let mut fee_cadence = None;
    let mut fee_rpc_pass: Option<std::sync::Arc<revops::fee_scheduler::FeeObserverPass>> = None;
    let mut lnplus_cadence = None;
    // Task 71 / R26: the three analytics cadences. Built during
    // composition, started only after `configured.start()` returns.
    let mut flow_cadence = None;
    let mut startup_snapshot_cadence = None;
    let mut financial_cadence = None;
    let mut lnplus_rpc_pass: Option<std::sync::Arc<revops::lnplus_runtime::LnPlusObserverPass>> =
        None;
    // Task 67: ONE boot identity per process, minted before any loop can
    // record a pass. Loop health is judged against this id, so a prior
    // boot's terminal evidence can never be inherited by this process.
    let boot_identity = revops_db::loop_health::BootIdentity {
        boot_id: format!("{}-{}", now_unix(), std::process::id()),
        process_id: i64::from(std::process::id()),
        source_commit: Some(revops::fee_scheduler::source_commit().to_string()),
        binary_sha256: Some(revops::fee_scheduler::binary_sha256().to_string()),
        started_at: now_unix(),
    };
    let boot_id = boot_identity.boot_id.clone();
    if let Some(handle) = observer_db.clone() {
        if let Err(error) = handle.record_boot_session(boot_identity.clone()).await {
            eprintln!("revops: boot session record failed: {error:#}");
        }
    }

    let authority_runtime = match authority_plan {
        revops::fee_mode::AuthorityPlan::Live(broadcaster) => {
            if let Some(handle) = observer_db.clone() {
                let store: Arc<dyn revops::loop_health::LoopHealthPersistence> = Arc::new(
                    revops::loop_health::LoopHealthStore::new(handle, boot_id.clone()),
                );
                if let Err(error) = revops::runtime::register_unwired_loops(store).await {
                    configured
                        .disable(&format!("loop-health registration failed: {error:#}"))
                        .await?;
                    return Ok(());
                }
            }
            let broadcaster = match broadcaster.await {
                Ok(broadcaster) => broadcaster,
                Err(error) => {
                    configured.disable(&format!("live authority gate: restart quarantine reconciliation failed: {error}")).await?;
                    return Ok(());
                }
            };
            revops::runtime::AuthorityRuntime::Live(revops::runtime::LiveRuntime::new(broadcaster))
        }
        revops::fee_mode::AuthorityPlan::Observer(observer_mode) => {
            let autonomous_shadow = observer_mode.autonomous_shadow();
            match observer_db.clone() {
                None => revops::runtime::AuthorityRuntime::Observer(
                    revops::runtime::ObserverRuntime::unavailable(observer_mode),
                ),
                Some(observer_handle) => {
                    let store: Arc<dyn revops::loop_health::LoopHealthPersistence> =
                        Arc::new(revops::loop_health::LoopHealthStore::new(
                            observer_handle.clone(),
                            boot_id.clone(),
                        ));
                    let mut passes = revops::runtime::ObserverPassSet::empty();
                    let mut fee_pass = None;
                    let mut lnplus_pass = None;
                    // Task 71 / R26: the three analytics owners. Unlike the
                    // fee and LN+ passes these are NOT gated on
                    // autonomous-shadow authority: they issue read-only
                    // RPCs, run the frozen kernels, and write only to the
                    // Rust-owned observer store, so they hold no action
                    // capability for a passive observer to escalate.
                    //
                    // `db` is the READ-ONLY production handle. Passing it
                    // as `Some` is what makes `config_overrides` a readable
                    // tier; `None` makes the flow resolver refuse rather
                    // than silently run on defaults an operator replaced.
                    let flow_pass = Arc::new(revops::analytics_passes::FlowAnalysisPass::live(
                        init_socket_path.clone(),
                        observer_handle.clone(),
                        boot_id.clone(),
                        db.clone(),
                        python_options.clone(),
                    ));
                    passes = passes.with_flow_analysis(flow_pass.clone());
                    passes = passes.with_startup_snapshot(Arc::new(
                        revops::analytics_passes::StartupSnapshotPass::live(
                            init_socket_path.clone(),
                            observer_handle.clone(),
                        ),
                    ));
                    passes = passes.with_financial_snapshot(Arc::new(
                        revops::analytics_passes::FinancialSnapshotPass::live(
                            observer_handle.clone(),
                            boot_id.clone(),
                            init_socket_path.clone(),
                            db.clone(),
                        ),
                    ));
                    if autonomous_shadow {
                        // Task 61 4D: the REAL LN+ observer pass, against
                        // the Rust observer parallel-state DB (collision
                        // with production already vetoed upstream).
                        // Watcher-only + DryRun + read-side observer
                        // adapter types; disabled-by-default via the LN+
                        // store's own config until an operator enables it.
                        match observer_db_path_expanded.as_ref() {
                            Some(lnplus_store_path) => {
                                match revops::lnplus_runtime::LnPlusObserverPass::observer(
                                    revops::lnplus_runtime::LnPlusRuntimeConfig {
                                        store_path: lnplus_store_path.clone(),
                                        socket_path: init_socket_path.clone(),
                                        base_url: revops_lnplus::http::BASE_URL.to_string(),
                                        http_timeout: std::time::Duration::from_secs(20),
                                        rpc_timeout: revops::lnplus_adapters::DEFAULT_RPC_TIMEOUT,
                                    },
                                ) {
                                    Ok(pass) => {
                                        passes = passes.with_lnplus(pass.clone());
                                        lnplus_rpc_pass = Some(pass.clone());
                                        lnplus_pass = Some(pass);
                                    }
                                    Err(error) => eprintln!(
                                        "revops: LN+ observer pass FAILED to build: {error:#}; \
                                         LN+ loop remains not_wired"
                                    ),
                                }
                            }
                            None => eprintln!(
                                "revops: LN+ loop not wired: observer DB path unavailable"
                            ),
                        }
                    }
                    if autonomous_shadow {
                        match (production_db_path_expanded.as_ref(), journal_dir.as_ref()) {
                            (Some(prod_db_path), Some(journal_dir)) => {
                                let cfg = revops::fee_scheduler::SchedulerConfig {
                                    db_path: prod_db_path.clone(),
                                    socket_path: init_socket_path.clone(),
                                    journal_dir: journal_dir.clone(),
                                    lifecycle: revops::fee_scheduler::StateLifecycle::SeedOnce,
                                    trigger: revops::fee_scheduler::TriggerMode::ExternalOnly,
                                };
                                match revops::fee_scheduler::spawn_owner_for_runtime(cfg, Some(Box::new(observer_handle.clone()) as Box<dyn revops::fee_state::RunwayStateStore>)) {
                            Ok(handle) => {
                                let initial_interval = revops::fee_config::resolve_fee_cfg(db.as_ref(), &python_options.snapshot()).await.fee_interval.max(1) as u64;
                                let pass = Arc::new(revops::fee_scheduler::FeeObserverPass::new(init_socket_path.clone(), db.clone(), python_options.clone(), handle.tx.clone(), initial_interval));
                                passes = passes.with_fee(pass.clone());
                                fee_rpc_pass = Some(pass.clone());
                                fee_pass = Some(pass);
                                let _ = scheduler.set(handle);
                            }
                            Err(error) => eprintln!("revops: fee-cycle owner FAILED to start: {error:#}; fee loop remains not_wired"),
                        }
                            }
                            (None, _) => {
                                eprintln!("revops: fee loop not wired: production DB unavailable")
                            }
                            (_, None) => eprintln!(
                                "revops: fee loop not wired: journal directory unavailable"
                            ),
                        }
                    }
                    let runtime =
                        match revops::runtime::ObserverRuntime::start(observer_mode, store, passes)
                            .await
                        {
                            Ok(runtime) => runtime,
                            Err(error) => {
                                configured
                                    .disable(&format!(
                                    "observer runtime loop-health initialization failed: {error:#}"
                                ))
                                    .await?;
                                return Ok(());
                            }
                        };
                    if let (Some(pass), Some(handle)) = (
                        fee_pass,
                        runtime.handle(revops_db::loop_health::LoopId::Fee),
                    ) {
                        fee_cadence = Some(revops::fee_scheduler::FeeCadenceActivation::new(
                            handle,
                            pass,
                            revops::fee_scheduler::TICK_PHASE_OFFSET_SECS,
                        ));
                    }
                    if let (Some(pass), Some(handle)) = (
                        lnplus_pass,
                        runtime.handle(revops_db::loop_health::LoopId::LnPlus),
                    ) {
                        lnplus_cadence = Some(
                            revops::lnplus_runtime::LnPlusCadenceActivation::new(handle, pass),
                        );
                    }
                    // Task 71 / R26. Every one of these is INERT until
                    // `activate()` below, which runs only after
                    // `configured.start()` has returned: a flow pass that
                    // landed mid-handshake would read a socket lightningd
                    // has not finished answering on.
                    if let Some(handle) =
                        runtime.handle(revops_db::loop_health::LoopId::FlowAnalysis)
                    {
                        flow_cadence = Some(revops::analytics_cadence::FlowCadenceActivation::new(
                            handle, flow_pass,
                        ));
                    }
                    if let Some(handle) =
                        runtime.handle(revops_db::loop_health::LoopId::StartupSnapshot)
                    {
                        startup_snapshot_cadence = Some(
                            revops::analytics_cadence::StartupSnapshotActivation::new(handle),
                        );
                    }
                    if let Some(handle) =
                        runtime.handle(revops_db::loop_health::LoopId::FinancialSnapshot)
                    {
                        financial_cadence = Some(
                            revops::analytics_cadence::FinancialCadenceActivation::new(handle),
                        );
                    }
                    revops::runtime::AuthorityRuntime::Observer(runtime)
                }
            }
        }
    };

    // Task 60: the rebalance owner exists whenever the Rust-owned store
    // does. The engine stays UNASSEMBLED pre-cutover (submissions refuse
    // on the Python-parity uninitialized arm); restart reconciliation of
    // unresolved rebalance attempts still runs -- store + one read-only
    // listsendpays lookup per orphan.
    let rebalance_hard_cap_sats = 5_000_000; // py rebalance_max_amount default
    let rebalance_owner = observer_db.clone().map(|store| {
        revops::rebalance_owner::spawn_rebalance_owner(
            revops::rebalance_owner::RebalanceOwnerDeps {
                engine: None,
                evidence: std::sync::Arc::new(revops::rebalance_owner::UnassembledEvidence),
                store,
                reconcile: std::sync::Arc::new(revops::rebalance_adapters::ClnReconcileRpc::new(
                    init_socket_path.clone(),
                    30,
                )),
                config: revops::rebalance_owner::RebalanceOwnerConfig {
                    daily_budget_sats: 5_000, // py daily_budget_sats default
                    budget_window_hours: 24,
                    rebalance_max_amount: rebalance_hard_cap_sats,
                    pair_cooldown_seconds: 3_600,
                },
                clock: Box::new(now_unix),
            },
        )
    });
    if let Some(owner) = rebalance_owner.clone() {
        tokio::spawn(async move {
            match owner.reconcile_on_start().await {
                Ok(summary) => {
                    if summary.settled_success + summary.settled_failed + summary.quarantined > 0 {
                        eprintln!(
                            "revops: rebalance restart reconciliation: {} settled complete,                              {} settled failed, {} quarantined",
                            summary.settled_success, summary.settled_failed, summary.quarantined
                        );
                    }
                }
                Err(e) => eprintln!("revops: rebalance restart reconciliation failed: {e:?}"),
            }
        });
    }

    // Task 62: the capital owner exists whenever the Rust-owned store
    // does. Adapters/governor stay UNASSEMBLED until Task 69's authority
    // assembly (planner-execute refuses on the Python-parity
    // uninitialized arm); restart reconciliation of orphan capital
    // intents still runs -- store plus read-only
    // listfunds/listclosedchannels lookups.
    let capital_owner = observer_db.clone().map(|store| {
        revops::capital_owner::spawn_capital_owner(revops::capital_owner::CapitalOwnerDeps {
            adapters: None,
            governor: None,
            budget: std::sync::Arc::new(revops::capital_owner::UnassembledCapitalBudget),
            evidence: std::sync::Arc::new(revops::capital_owner::UnassembledCapitalEvidence),
            store,
            reconcile: std::sync::Arc::new(revops::capital_adapters::ClnCapitalReconcileRpc::new(
                init_socket_path.clone(),
                30,
            )),
            clock: Box::new(now_unix),
        })
    });
    if let Some(owner) = capital_owner.clone() {
        tokio::spawn(async move {
            match owner.reconcile_on_start().await {
                Ok(summary) => {
                    if summary.settled_success + summary.quarantined > 0 {
                        eprintln!(
                            "revops: capital restart reconciliation: {} settled, {} quarantined",
                            summary.settled_success, summary.quarantined
                        );
                    }
                }
                Err(e) => eprintln!("revops: capital restart reconciliation failed: {e:?}"),
            }
        });
    }

    // Task 63: the Boltz owner exists whenever the Rust-owned store does.
    // The ACTION CAPABILITY stays unassembled until Task 69's authority
    // assembly (every fund-moving Boltz RPC refuses on the Python-parity
    // uninitialized arm); the read-only QUERY transport is
    // production-constructible but ships DISABLED (py cfg.enabled
    // default false), so it too refuses until an operator turns it on.
    // Parity fix (parity_matrix finding 4): resolve the Boltz config from
    // the SAME three layers every other subsystem uses -- DB override,
    // the operator's live Python value, then the documented default.
    // Building the transport from `BoltzCliProcessConfig::default()` made
    // eight Boltz RPCs answer "integration disabled" while Python
    // returned real data.
    let boltz_cfg =
        revops::boltz_config::resolve_boltz_cfg(db.as_ref(), &python_options.snapshot()).await;
    let boltz_query: std::sync::Arc<dyn revops_boltz::cli::BoltzCli + Send + Sync> =
        std::sync::Arc::new(revops_boltz::process::ProcessBoltzCli::new(
            boltz_cfg.to_process_config(),
        ));
    let boltz_owner = observer_db.clone().map(|store| {
        revops::boltz_owner::spawn_boltz_owner(revops::boltz_owner::BoltzOwnerDeps {
            capability: None,
            governor: None,
            query: boltz_query.clone(),
            structural: std::sync::Arc::new(revops::boltz_owner::UnassembledStructuralSpend),
            store,
            config: revops::boltz_owner::BoltzOwnerConfig {
                daily_budget_sats: boltz_cfg.daily_budget_sats,
                budget_window_hours: 24,
                structural_envelope_sats: boltz_cfg.structural_budget_sats,
                allow_concurrent_swaps: false,
                default_cooldown_seconds: 3_600,
                auto_cycle_enabled: boltz_cfg.auto_cycle_enabled,
                create_timeout_secs: revops::boltz_owner::CREATE_TIMEOUT_FLOOR_SECS,
            },
            clock: Box::new(now_unix),
        })
    });
    if let Some(owner) = boltz_owner.clone() {
        tokio::spawn(async move {
            match owner.reconcile_on_start().await {
                Ok(summary) => {
                    if summary.quarantined > 0 {
                        eprintln!(
                            "revops: boltz restart reconciliation: {} quarantined",
                            summary.quarantined
                        );
                    }
                }
                Err(e) => eprintln!("revops: boltz restart reconciliation failed: {e:?}"),
            }
        });
    }

    let active_profile = match db.as_ref() {
        Some(handle) => revops::rpc_profile_preview::startup_active_profile(
            queries::config_override(handle, "risk_profile")
                .await
                .map_err(|error| format!("{error:#}")),
        ),
        None => revops::rpc_profile_preview::startup_active_profile(Ok(None)),
    };

    let fee_authority_status =
        revops::rpc_fee_authority_status::FeeAuthorityStatusSnapshot::from_startup_mode(
            resolved_mode_label == "live_authority",
            boot_identity.started_at,
        );

    let state: SharedState = Arc::new(State {
        version: VERSION.to_string(),
        observer,
        db_path,
        db,
        observer_db,
        config_names: config_name_map(),
        python_options,
        active_profile,
        scheduler,
        fee_pass: fee_rpc_pass,
        core_mutations,
        mode_label: resolved_mode_label,
        fee_authority_status,
        authority_runtime,
        socket_path: init_socket_path.clone(),
        production_db_path: production_db_path_expanded.clone(),
        lnplus: lnplus_rpc_pass,
        rebalance_owner,
        rebalance_rate_limiter: revops::rpc_rebalance_ops::ForceRateLimiter::production(),
        rebalance_hard_cap_sats,
        capital_owner,
        boltz_owner,
        boltz_query,
        boltz_cfg,
        boot_id: boot_id.clone(),
    });

    let plugin = configured.start(state).await?;

    if let Some(fee_cadence) = fee_cadence {
        fee_cadence.activate();
    }
    if let Some(lnplus_cadence) = lnplus_cadence {
        lnplus_cadence.activate();
    }
    // Task 71 / R26: py starts its analytics threads at the very end of
    // `init`, after the plugin is serving (cl-revenue-ops.py:3588-3600).
    // Activation AFTER `start()` reproduces that ordering exactly, and it
    // is the reason each activation is inert until this point.
    if let Some(flow_cadence) = flow_cadence {
        flow_cadence.activate();
    }
    if let Some(startup_snapshot_cadence) = startup_snapshot_cadence {
        startup_snapshot_cadence.activate();
    }
    if let Some(financial_cadence) = financial_cadence {
        financial_cadence.activate();
    }

    // Startup hydration runs as a background task, off the init-handshake
    // path: paging `listforwards` over a live socket could be slow on a
    // node with a large forwards history, and lightningd's own init
    // handshake must not wait on it (see the plan's Task 2 self-review
    // note on splitting hydration into a post-start spawned task).
    {
        let hydration_plugin = plugin.clone();
        tokio::spawn(async move {
            let Some(observer_db) = hydration_plugin.state().observer_db.clone() else {
                return;
            };
            let cfg = hydration_plugin.configuration();
            let socket_path = PathBuf::from(cfg.lightning_dir).join(cfg.rpc_file);
            // `flow_window_days` must be read LIVE from the resolved
            // option (not a hardcoded default) so an operator running a
            // non-default flow window still gets the correct backfill
            // bounds (plan Task 2 self-review, second-order risk).
            let flow_window_days = hydration_plugin
                .option_str(&opt_name("flow-window-days"))
                .ok()
                .flatten()
                .and_then(|v| v.as_str().and_then(|s| s.trim().parse::<i64>().ok()))
                .unwrap_or(7);
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);
            let inserted =
                hydration::run_startup_hydration(&observer_db, &socket_path, flow_window_days, now)
                    .await;
            if inserted > 0 {
                eprintln!(
                    "revops: startup hydration inserted {inserted} forwards into the observer db"
                );
            }
        });
    }

    plugin.join().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::MetadataExt;

    /// Serializes every test that mutates process-global environment
    /// (`HOME`): the default test runner executes `#[test]`s on parallel
    /// threads within this one process, so two HOME-mutating tests (or a
    /// mutator racing a reader mid-`expand_tilde`) can interleave
    /// set/restore and flake -- observed twice in release-leg CI runs.
    /// Every HOME mutation must go through [`set_home`], which holds this
    /// lock for the guard's lifetime.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// RAII guard from [`set_home`]: restores the previous `HOME` (or
    /// removes it) on drop -- INCLUDING on assert-panic unwind, so a
    /// failing test can't leak its fake `HOME` into later tests. Holds
    /// [`ENV_LOCK`] for its whole lifetime; a poisoned lock (an earlier
    /// panicking holder) is recovered via `into_inner` -- the guard's own
    /// Drop restored `HOME`, so the "poisoned" state is already clean.
    struct HomeGuard {
        prev: Option<String>,
        _lock: std::sync::MutexGuard<'static, ()>,
    }

    impl Drop for HomeGuard {
        fn drop(&mut self) {
            match self.prev.take() {
                Some(v) => std::env::set_var("HOME", v),
                None => std::env::remove_var("HOME"),
            }
        }
    }

    fn set_home(home: &str) -> HomeGuard {
        let lock = ENV_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let prev = std::env::var("HOME").ok();
        std::env::set_var("HOME", home);
        HomeGuard { prev, _lock: lock }
    }

    fn bad_default_opt(opt_type: &str, default: serde_json::Value) -> OptDef {
        OptDef {
            name: "revops-r-test-bad".to_string(),
            opt_type: opt_type.to_string(),
            default,
            description: "synthetic option for panic-path coverage".to_string(),
            dynamic: false,
        }
    }

    /// Negative test: `register_option` must panic (not silently degrade to
    /// a valueless option) when a non-null `int` default fails to parse.
    /// This directly exercises the failure branch the guard test in
    /// `options_table.rs` cannot reach (that test only walks known-good
    /// fixture data).
    #[test]
    #[should_panic(expected = "has non-null default that fails to parse as i64")]
    fn register_option_panics_on_unparseable_int_default() {
        let builder = Builder::<(), _, _>::new(tokio::io::empty(), tokio::io::sink());
        let opt = bad_default_opt("int", serde_json::json!("not-a-number"));
        let _ = register_option(builder, "revops-r-test-bad", &opt);
    }

    /// Same, for the `bool` failure branch.
    #[test]
    #[should_panic(expected = "has non-null default that fails to parse as bool")]
    fn register_option_panics_on_unparseable_bool_default() {
        let builder = Builder::<(), _, _>::new(tokio::io::empty(), tokio::io::sink());
        let opt = bad_default_opt("bool", serde_json::json!("not-a-bool"));
        let _ = register_option(builder, "revops-r-test-bad", &opt);
    }

    /// `expand_tilde` on the fixture's own default
    /// (`~/.lightning/revenue_ops.db`) against a synthetic `$HOME` --
    /// mirrors `os.path.expanduser("~/.lightning/revenue_ops.db")`.
    #[test]
    fn expand_tilde_expands_leading_tilde_slash() {
        let _home = set_home("/home/testuser");
        assert_eq!(
            expand_tilde("~/.lightning/revenue_ops.db"),
            PathBuf::from("/home/testuser/.lightning/revenue_ops.db")
        );
    }

    /// Bare `~` (no trailing slash) expands to exactly `$HOME`.
    #[test]
    fn expand_tilde_expands_bare_tilde() {
        let _home = set_home("/home/testuser");
        assert_eq!(expand_tilde("~"), PathBuf::from("/home/testuser"));
    }

    /// A path with no leading `~` passes through unchanged.
    #[test]
    fn expand_tilde_leaves_absolute_path_unchanged() {
        assert_eq!(
            expand_tilde("/var/lib/revops/revenue_ops.db"),
            PathBuf::from("/var/lib/revops/revenue_ops.db")
        );
    }

    /// `~user/...` (not the plain `~/...` this plugin's config ever
    /// produces) is deliberately left unexpanded -- documented
    /// limitation, matches this function's own doc comment.
    #[test]
    fn expand_tilde_does_not_expand_tilde_user_form() {
        assert_eq!(
            expand_tilde("~alice/db.sqlite"),
            PathBuf::from("~alice/db.sqlite")
        );
    }

    /// Same expanded path -> collision, refuse.
    #[test]
    fn observer_db_path_collides_with_production_same_path() {
        let production = PathBuf::from("/home/testuser/.lightning/revenue_ops.db");
        assert!(observer_db_path_collides_with_production(
            &production,
            Some(&production),
        ));
    }

    /// Different paths -> no collision.
    #[test]
    fn observer_db_path_collides_with_production_different_paths() {
        let observer = PathBuf::from("/home/testuser/.lightning/revops-r-observer.db");
        let production = PathBuf::from("/home/testuser/.lightning/revenue_ops.db");
        assert!(!observer_db_path_collides_with_production(
            &observer,
            Some(&production),
        ));
    }

    /// Production db-path unset (`None`) -> never a collision.
    #[test]
    fn observer_db_path_collides_with_production_no_production_path() {
        let observer = PathBuf::from("/home/testuser/.lightning/revops-r-observer.db");
        assert!(!observer_db_path_collides_with_production(&observer, None));
    }

    /// IMPORTANT 4 regression: two textually-DIFFERENT paths that resolve
    /// to the SAME real file via a symlink (mirroring lnnode's own
    /// `~/.lightning -> /data/lightningd`) must be caught as a collision.
    /// Pure string equality misses this entirely -- the exact bypass that
    /// would hand the observer's read-write actor the production DB.
    #[test]
    fn observer_db_path_collides_with_production_via_symlink() {
        let dir = tempfile::tempdir().unwrap();

        // The "real" data directory and its one true db file.
        let real_dir = dir.path().join("data/lightningd");
        std::fs::create_dir_all(&real_dir).unwrap();
        let production_path = real_dir.join("revenue_ops.db");
        std::fs::write(&production_path, b"").unwrap();

        // A symlink pointing at that same real directory, under a
        // different parent -- e.g. `$HOME/.lightning`.
        let home_dir = dir.path().join("home");
        std::fs::create_dir_all(&home_dir).unwrap();
        let symlinked_dir = home_dir.join(".lightning");
        std::os::unix::fs::symlink(&real_dir, &symlinked_dir).unwrap();

        // Observer path spelled THROUGH the symlink, at the exact same
        // real file the production path points at directly.
        let observer_path = symlinked_dir.join("revenue_ops.db");

        assert_ne!(
            observer_path, production_path,
            "the two spellings must be textually different for this test to mean anything"
        );
        assert!(observer_db_path_collides_with_production(
            &observer_path,
            Some(&production_path),
        ));
    }

    /// When files don't exist (the common real-world case -- this check
    /// runs before the observer creates its own file), collision detection
    /// still falls back to string equality rather than silently reporting
    /// "no collision" just because canonicalize can't resolve anything.
    #[test]
    fn observer_db_path_collides_with_production_falls_back_when_files_missing() {
        let dir = tempfile::tempdir().unwrap();
        let same_path = dir.path().join("does-not-exist-yet.db");
        assert!(observer_db_path_collides_with_production(
            &same_path,
            Some(&same_path),
        ));
    }

    /// MINOR (a) regression: the once-only drop-log flag latches after its
    /// first flip and stays latched -- confirms the swap-gate itself
    /// (`log_observer_db_drop_once`'s `AtomicBool::swap`), even though a
    /// unit test can't directly assert on `eprintln!`'s stderr output.
    #[test]
    fn log_observer_db_drop_once_latches_after_first_call() {
        static TEST_DROP_LOGGED: AtomicBool = AtomicBool::new(false);
        assert!(!TEST_DROP_LOGGED.load(Ordering::Relaxed));
        log_observer_db_drop_once(&TEST_DROP_LOGGED, "test_topic");
        assert!(TEST_DROP_LOGGED.load(Ordering::Relaxed));
        // Repeated calls are no-ops on the flag -- it was already true.
        log_observer_db_drop_once(&TEST_DROP_LOGGED, "test_topic");
        assert!(TEST_DROP_LOGGED.load(Ordering::Relaxed));
    }

    /// `config_name_map` must expose `observer-db-path` (MINOR b) so
    /// `revenue-r-config key=observer-db-path` can resolve it, same as the
    /// pre-existing `observer`/`db-path` entries.
    #[test]
    fn config_name_map_includes_observer_db_path() {
        let map = config_name_map();
        assert!(
            map.contains_key("observer-db-path"),
            "observer-db-path missing from config_name_map: {map:?}"
        );
    }

    /// Checklist-mandated mirror of `config_name_map_includes_observer_db_path`:
    /// `config_name_map` must also expose `journal-dir` (Task 3) so
    /// `revenue-r-config key=journal-dir` can resolve it.
    #[test]
    fn config_name_map_includes_journal_dir() {
        let map = config_name_map();
        assert!(
            map.contains_key("journal-dir"),
            "journal-dir missing from config_name_map: {map:?}"
        );
    }

    /// Task 6 mirror of the two tests above: `revenue-r-config
    /// key=fee-dryrun` must resolve the new dry-run switch.
    #[test]
    fn config_name_map_includes_fee_dryrun() {
        let map = config_name_map();
        assert!(
            map.contains_key("fee-dryrun"),
            "fee-dryrun missing from config_name_map: {map:?}"
        );
    }

    /// Task 3, branch 1: an explicit, non-empty `revops-r-journal-dir`
    /// value is used as-is after `expand_tilde`, regardless of what
    /// `observer_db_path` is (even `Some`, to prove the explicit value
    /// wins rather than being ignored).
    #[test]
    fn resolve_journal_dir_explicit_value_is_tilde_expanded() {
        let _home = set_home("/home/testuser");
        let resolved = resolve_journal_dir(
            "~/journal",
            Some(&PathBuf::from("/var/lib/revops/observer.db")),
        );
        assert_eq!(resolved, Some(PathBuf::from("/home/testuser/journal")));
    }

    /// Task 3, branch 2: empty option value + a resolved `observer_db_path`
    /// resolves to that path's PARENT directory.
    #[test]
    fn resolve_journal_dir_empty_with_observer_db_uses_parent_dir() {
        let resolved = resolve_journal_dir(
            "",
            Some(&PathBuf::from(
                "/home/testuser/.lightning/revops-r-observer.db",
            )),
        );
        assert_eq!(resolved, Some(PathBuf::from("/home/testuser/.lightning")));
    }

    /// Task 3, branch 3: empty option value AND no `observer_db_path` ->
    /// nothing to derive a journal location from -> `None`.
    #[test]
    fn resolve_journal_dir_both_unset_yields_none() {
        let resolved = resolve_journal_dir("", None);
        assert_eq!(resolved, None);
    }

    // -----------------------------------------------------------------
    // Task 10: `resolve_startup_mode` -- the mode matrix, the mandatory-
    // writable-Rust-state gate, and cutover-arm consumption, all wired
    // together exactly as `main()` calls them.
    // -----------------------------------------------------------------

    const TEST_NODE_ID: &str = "lnnode";
    const TEST_SOURCE_COMMIT: &str = "7d8e79ec307fd10bd1a775a236148a642a0a506f";
    const TEST_BINARY_SHA256: &str =
        "ff648376758b9a97de7642adbf1c258494744c54e33c31a712dcc8c742d1428c";

    fn test_identity(owner_uid: u32) -> RunningIdentity {
        RunningIdentity {
            node_id: TEST_NODE_ID.to_string(),
            subsystem: cutover_arm::CUTOVER_SUBSYSTEM_FEES.to_string(),
            source_commit: TEST_SOURCE_COMMIT.to_string(),
            binary_sha256: TEST_BINARY_SHA256.to_string(),
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
            schema = cutover_arm::CUTOVER_ARM_SCHEMA,
            node = TEST_NODE_ID,
            subsystem = cutover_arm::CUTOVER_SUBSYSTEM_FEES,
            commit = TEST_SOURCE_COMMIT,
            hash = TEST_BINARY_SHA256,
            nonce = nonce,
        )
    }

    fn write_arm(dir: &std::path::Path, name: &str, json: &str) -> PathBuf {
        use std::fs::OpenOptions;
        use std::io::Write as _;
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

    fn virgin_state() -> revops_db::fee_runway::FeeStateSnapshot {
        revops_db::fee_runway::FeeStateSnapshot::default()
    }

    fn non_virgin_state() -> revops_db::fee_runway::FeeStateSnapshot {
        revops_db::fee_runway::FeeStateSnapshot {
            generation: 3,
            rows: vec![],
        }
    }

    /// Coordinator ruling I-6: live authority requires a SEEDED store
    /// (generation > 0 AND a recorded seed event) -- this pairs with
    /// [`some_seed_event`] wherever a test needs a valid live row.
    fn passive_flags() -> ModeFlags {
        ModeFlags {
            observer: true,
            fee_dryrun: false,
            fee_broadcast: false,
            fee_stateful_shadow: false,
        }
    }

    fn shadow_flags() -> ModeFlags {
        ModeFlags {
            observer: true,
            fee_dryrun: true,
            fee_broadcast: false,
            fee_stateful_shadow: true,
        }
    }

    fn live_flags() -> ModeFlags {
        ModeFlags {
            observer: false,
            fee_dryrun: false,
            fee_broadcast: true,
            fee_stateful_shadow: false,
        }
    }

    /// Full mode matrix: passive observer needs no Rust state and no arm.
    #[tokio::test]
    async fn resolve_startup_mode_passive_observer_delegates_to_fee_mode() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let consumed_dir = tmp.path().join("consumed");
        let result = resolve_startup_mode_kernel(StartupModeInputs {
            flags: passive_flags(),
            cutover_arm: None,
            consumed_arm_dir: &consumed_dir,
            state: &virgin_state(),
            seed_binding: &revops_db::fee_runway::SeedBindingState::VirginStore,
            rust_state_store_configured: false,
            nonce_ledger: None,
            now: 1_800_000_000,
        })
        .await
        .expect("passive observer needs no Rust state");
        assert!(matches!(result, ValidatedFeeMode::PassiveObserver(_)));
        assert_eq!(mode_label(&result), "passive_observer");
    }

    /// Mandatory writable Rust state (Step 1): autonomous shadow without a
    /// configured Rust-owned store is refused BEFORE the mode matrix ever
    /// runs -- no arm is involved here, so this proves the gate exists
    /// independent of arm handling.
    #[tokio::test]
    async fn resolve_startup_mode_shadow_requires_rust_state_store() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let consumed_dir = tmp.path().join("consumed");
        let err = resolve_startup_mode_kernel(StartupModeInputs {
            flags: shadow_flags(),
            cutover_arm: None,
            consumed_arm_dir: &consumed_dir,
            state: &virgin_state(),
            seed_binding: &revops_db::fee_runway::SeedBindingState::VirginStore,
            rust_state_store_configured: false,
            nonce_ledger: None,
            now: 1_800_000_000,
        })
        .await
        .unwrap_err();
        assert!(matches!(
            err,
            StartupModeDenyReason::MissingRustState {
                mode: "autonomous fee shadow mode"
            }
        ));
    }

    /// Same gate, live-authority row -- AND proves the ordering guarantee:
    /// the cutover arm file is left COMPLETELY untouched (not even opened)
    /// when the mandatory-state gate refuses first. A broken deploy must
    /// never burn a one-time arm.
    #[tokio::test]
    async fn resolve_startup_mode_live_requires_rust_state_store_before_touching_arm() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let consumed_dir = tmp.path().join("consumed");
        let arm_path = write_arm(
            tmp.path(),
            "arm.json",
            &valid_arm_json("live-nonce-untouched"),
        );
        let owner_uid = std::fs::metadata(&arm_path).expect("stat arm").uid();

        let err = resolve_startup_mode_kernel(StartupModeInputs {
            flags: live_flags(),
            cutover_arm: Some((&arm_path, test_identity(owner_uid))),
            consumed_arm_dir: &consumed_dir,
            state: &virgin_state(),
            seed_binding: &revops_db::fee_runway::SeedBindingState::VirginStore,
            rust_state_store_configured: false,
            nonce_ledger: None,
            now: 1_800_000_000,
        })
        .await
        .unwrap_err();
        assert!(matches!(
            err,
            StartupModeDenyReason::MissingRustState {
                mode: "live fee authority mode"
            }
        ));
        assert!(
            arm_path.exists(),
            "arm must be left untouched when the mandatory-state gate refuses first"
        );
        assert!(!consumed_dir.exists(), "nothing should have been consumed");
    }

    /// Arm absence in shadow (Step 1): a configured store, no arm supplied,
    /// virgin state -- accepted, deferring seeding to the first cycle.
    #[tokio::test]
    async fn resolve_startup_mode_shadow_without_arm_when_store_configured() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let consumed_dir = tmp.path().join("consumed");
        let result = resolve_startup_mode_kernel(StartupModeInputs {
            flags: shadow_flags(),
            cutover_arm: None,
            consumed_arm_dir: &consumed_dir,
            state: &virgin_state(),
            seed_binding: &revops_db::fee_runway::SeedBindingState::VirginStore,
            rust_state_store_configured: true,
            nonce_ledger: None,
            now: 1_800_000_000,
        })
        .await
        .expect("shadow row with a configured store and no arm is valid");
        match result {
            ValidatedFeeMode::AutonomousShadow(shadow) => assert_eq!(
                shadow.seed_status(),
                revops::fee_mode::ShadowSeedStatus::PendingFirstCycle
            ),
            other => panic!("expected AutonomousShadow, got {other:?}"),
        }
    }

    /// Arm consumption in live mode (Step 1): a valid arm, matching
    /// identity, and a configured store -- accepted, AND the arm file is
    /// actually gone from its original path (moved into `consumed_dir`).
    #[tokio::test]
    async fn resolve_startup_mode_live_consumes_a_valid_arm() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let consumed_dir = tmp.path().join("consumed");
        let arm_path = write_arm(
            tmp.path(),
            "arm.json",
            &valid_arm_json("live-nonce-consumed"),
        );
        let owner_uid = std::fs::metadata(&arm_path).expect("stat arm").uid();
        let ledger = revops_db::owner::spawn_read_write(&tmp.path().join("ledger.db"))
            .await
            .expect("spawn test nonce ledger");
        let seed_binding = revops_db::fee_runway::SeedBindingState::VerifiedBound {
            cycle_id: "live-startup-seed-cycle".to_string(),
        };

        let result = resolve_startup_mode_kernel(StartupModeInputs {
            flags: live_flags(),
            cutover_arm: Some((&arm_path, test_identity(owner_uid))),
            consumed_arm_dir: &consumed_dir,
            state: &non_virgin_state(),
            seed_binding: &seed_binding,
            rust_state_store_configured: true,
            nonce_ledger: Some(&ledger),
            now: 1_800_000_000,
        })
        .await
        .expect("live row with a valid arm, a configured store, and seeded state is valid");
        match result {
            ValidatedFeeMode::LiveAuthority(live) => {
                assert_eq!(live.arm().nonce(), "live-nonce-consumed");
            }
            other => panic!("expected LiveAuthority, got {other:?}"),
        }
        assert!(
            !arm_path.exists(),
            "the arm must be consumed (moved away) on success"
        );
        assert!(
            consumed_dir.join("live-nonce-consumed").exists(),
            "the consumed arm must land at consumed_dir/<nonce>"
        );
    }

    /// An invalid arm (wrong node id) is refused with `ArmInvalid`, never
    /// silently treated as "no arm supplied".
    #[tokio::test]
    async fn resolve_startup_mode_invalid_arm_is_refused() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let consumed_dir = tmp.path().join("consumed");
        let arm_path = write_arm(tmp.path(), "arm.json", &valid_arm_json("live-nonce-bad"));
        let owner_uid = std::fs::metadata(&arm_path).expect("stat arm").uid();
        let mut identity = test_identity(owner_uid);
        identity.node_id = "some-other-node".to_string();

        let err = resolve_startup_mode_kernel(StartupModeInputs {
            flags: live_flags(),
            cutover_arm: Some((&arm_path, identity)),
            consumed_arm_dir: &consumed_dir,
            state: &virgin_state(),
            seed_binding: &revops_db::fee_runway::SeedBindingState::VirginStore,
            rust_state_store_configured: true,
            nonce_ledger: None,
            now: 1_800_000_000,
        })
        .await
        .unwrap_err();
        assert!(matches!(
            err,
            StartupModeDenyReason::ArmInvalid(cutover_arm::CutoverArmDenyReason::WrongNode(_))
        ));
        assert!(arm_path.exists(), "a rejected arm is never consumed");
    }

    /// An arm present alongside the shadow row is consumed (per
    /// `fee_mode`'s own `ArmPresentInNonLiveMode` contract) and THEN the
    /// mode matrix denies it -- proving the arm-handling step runs
    /// unconditionally on the resolved flags, not only for the live row.
    #[tokio::test]
    async fn resolve_startup_mode_arm_present_in_shadow_row_is_denied_after_consumption() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let consumed_dir = tmp.path().join("consumed");
        let arm_path = write_arm(tmp.path(), "arm.json", &valid_arm_json("stray-in-shadow"));
        let owner_uid = std::fs::metadata(&arm_path).expect("stat arm").uid();
        let ledger = revops_db::owner::spawn_read_write(&tmp.path().join("ledger.db"))
            .await
            .expect("spawn test nonce ledger");

        let err = resolve_startup_mode_kernel(StartupModeInputs {
            flags: shadow_flags(),
            cutover_arm: Some((&arm_path, test_identity(owner_uid))),
            consumed_arm_dir: &consumed_dir,
            state: &virgin_state(),
            seed_binding: &revops_db::fee_runway::SeedBindingState::VirginStore,
            rust_state_store_configured: true,
            nonce_ledger: Some(&ledger),
            now: 1_800_000_000,
        })
        .await
        .unwrap_err();
        assert!(matches!(
            err,
            StartupModeDenyReason::Mode(
                revops::fee_mode::FeeModeDenyReason::ArmPresentInNonLiveMode
            )
        ));
        assert!(
            !arm_path.exists(),
            "the arm is consumed even though this mode ultimately denies it"
        );
    }

    /// Shadow row, a non-virgin store with no seed event, delegates the
    /// `NeverSeeded` misconfiguration straight through from `fee_mode`.
    #[tokio::test]
    async fn resolve_startup_mode_never_seeded_delegates_from_fee_mode() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let consumed_dir = tmp.path().join("consumed");
        let err = resolve_startup_mode_kernel(StartupModeInputs {
            flags: shadow_flags(),
            cutover_arm: None,
            consumed_arm_dir: &consumed_dir,
            state: &non_virgin_state(),
            seed_binding: &revops_db::fee_runway::SeedBindingState::VirginStore,
            rust_state_store_configured: true,
            nonce_ledger: None,
            now: 1_800_000_000,
        })
        .await
        .unwrap_err();
        assert!(matches!(
            err,
            StartupModeDenyReason::Mode(revops::fee_mode::FeeModeDenyReason::NeverSeeded)
        ));
    }

    /// Observer-DB collision guard, end to end: the collision guard yields
    /// `observer_db = None` (existing Task 2 behavior), and feeding that
    /// straight into `rust_state_store_configured` refuses autonomous
    /// shadow with the SAME `MissingRustState` gate a never-configured
    /// store would -- the collision guard's downstream effect on mode
    /// resolution is exactly "no writable Rust state", not a distinct
    /// error path.
    #[tokio::test]
    async fn observer_db_collision_guard_feeds_missing_rust_state_gate() {
        let observer_path = PathBuf::from("/home/testuser/.lightning/revenue_ops.db");
        let production_path = PathBuf::from("/home/testuser/.lightning/revenue_ops.db");
        assert!(observer_db_path_collides_with_production(
            &observer_path,
            Some(&production_path),
        ));
        // The collision guard's real effect: `observer_db` stays `None`.
        let rust_state_store_configured = false;

        let tmp = tempfile::tempdir().expect("tempdir");
        let consumed_dir = tmp.path().join("consumed");
        let err = resolve_startup_mode_kernel(StartupModeInputs {
            flags: shadow_flags(),
            cutover_arm: None,
            consumed_arm_dir: &consumed_dir,
            state: &virgin_state(),
            seed_binding: &revops_db::fee_runway::SeedBindingState::VirginStore,
            rust_state_store_configured,
            nonce_ledger: None,
            now: 1_800_000_000,
        })
        .await
        .unwrap_err();
        assert!(matches!(
            err,
            StartupModeDenyReason::MissingRustState {
                mode: "autonomous fee shadow mode"
            }
        ));
    }

    /// Coordinator ruling I-6: `resolve_startup_mode` itself (not just
    /// `fee_mode::validate_fee_mode` directly) must refuse a live row over
    /// a virgin store, even with an otherwise-perfectly-valid arm and a
    /// configured Rust-owned store (`rust_state_store_configured: true` is
    /// NOT sufficient on its own -- the store must also already be
    /// seeded). The arm is still consumed (this is the arm-handling step
    /// running unconditionally, same posture as
    /// `resolve_startup_mode_arm_present_in_shadow_row_is_denied_after_consumption`).
    #[tokio::test]
    async fn resolve_startup_mode_live_denies_virgin_store_even_with_valid_arm() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let consumed_dir = tmp.path().join("consumed");
        let arm_path = write_arm(tmp.path(), "arm.json", &valid_arm_json("live-virgin-nonce"));
        let owner_uid = std::fs::metadata(&arm_path).expect("stat arm").uid();
        let ledger = revops_db::owner::spawn_read_write(&tmp.path().join("ledger.db"))
            .await
            .expect("spawn test nonce ledger");

        let err = resolve_startup_mode_kernel(StartupModeInputs {
            flags: live_flags(),
            cutover_arm: Some((&arm_path, test_identity(owner_uid))),
            consumed_arm_dir: &consumed_dir,
            state: &virgin_state(),
            seed_binding: &revops_db::fee_runway::SeedBindingState::VirginStore,
            rust_state_store_configured: true,
            nonce_ledger: Some(&ledger),
            now: 1_800_000_000,
        })
        .await
        .unwrap_err();
        assert!(matches!(
            err,
            StartupModeDenyReason::Mode(fee_mode::FeeModeDenyReason::LiveModeRequiresSeededState)
        ));
        assert!(
            !arm_path.exists(),
            "the arm is consumed even though the seeded-state gate ultimately denies it"
        );
    }

    /// Task 59 §5.3 test plumbing: live-mode inputs around one arm --
    /// the shape every A-matrix test drives the kernel with.
    async fn consume_arm_via_kernel(
        arm_path: &std::path::Path,
        consumed_dir: &std::path::Path,
        ledger: &revops_db::owner::ObserverHandle,
        owner_uid: u32,
    ) -> Result<ValidatedFeeMode, StartupModeDenyReason> {
        let seed_binding = revops_db::fee_runway::SeedBindingState::VerifiedBound {
            cycle_id: "a-matrix-seed-cycle".to_string(),
        };
        resolve_startup_mode_kernel(StartupModeInputs {
            flags: live_flags(),
            cutover_arm: Some((arm_path, test_identity(owner_uid))),
            consumed_arm_dir: consumed_dir,
            state: &non_virgin_state(),
            seed_binding: &seed_binding,
            rust_state_store_configured: true,
            nonce_ledger: Some(ledger),
            now: 1_800_000_000,
        })
        .await
    }

    /// A1 (F12 single-loss pair, DB side): wiping/repointing the
    /// consumed-arm DIRECTORY does not permit replay -- the durable DB
    /// ledger still denies the burned nonce.
    #[tokio::test]
    async fn wiped_consumed_dir_does_not_permit_replay() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let consumed_dir = tmp.path().join("consumed");
        let ledger = revops_db::owner::spawn_read_write(&tmp.path().join("ledger.db"))
            .await
            .expect("spawn test nonce ledger");
        let arm_path = write_arm(tmp.path(), "arm.json", &valid_arm_json("a1-replay-nonce"));
        let owner_uid = std::fs::metadata(&arm_path).expect("stat arm").uid();

        consume_arm_via_kernel(&arm_path, &consumed_dir, &ledger, owner_uid)
            .await
            .expect("first consumption is valid");

        // Wipe the filesystem ledger entirely and re-mint the same nonce.
        std::fs::remove_dir_all(&consumed_dir).expect("wipe consumed dir");
        let arm_path = write_arm(tmp.path(), "arm2.json", &valid_arm_json("a1-replay-nonce"));
        let err = consume_arm_via_kernel(&arm_path, &consumed_dir, &ledger, owner_uid)
            .await
            .expect_err("a burned nonce must deny even with the consumed dir gone");
        assert!(
            matches!(
                err,
                StartupModeDenyReason::ArmInvalid(cutover_arm::CutoverArmDenyReason::ReusedNonce)
            ),
            "{err:?}"
        );
        assert!(arm_path.exists(), "a denied replay arm is never consumed");
    }

    /// A1b (F12 single-loss pair, filesystem side): replacing the observer
    /// DB with a fresh one does not permit replay -- the consumed-dir
    /// rename hits `EEXIST` and denies.
    #[tokio::test]
    async fn fresh_observer_db_does_not_permit_replay() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let consumed_dir = tmp.path().join("consumed");
        let ledger = revops_db::owner::spawn_read_write(&tmp.path().join("ledger.db"))
            .await
            .expect("spawn test nonce ledger");
        let arm_path = write_arm(tmp.path(), "arm.json", &valid_arm_json("a1b-replay-nonce"));
        let owner_uid = std::fs::metadata(&arm_path).expect("stat arm").uid();

        consume_arm_via_kernel(&arm_path, &consumed_dir, &ledger, owner_uid)
            .await
            .expect("first consumption is valid");

        // A FRESH observer db (the DB ledger is gone) but the consumed
        // dir survives: the rename target already exists.
        let fresh_ledger = revops_db::owner::spawn_read_write(&tmp.path().join("fresh-ledger.db"))
            .await
            .expect("spawn fresh ledger");
        let arm_path = write_arm(tmp.path(), "arm2.json", &valid_arm_json("a1b-replay-nonce"));
        let err = consume_arm_via_kernel(&arm_path, &consumed_dir, &fresh_ledger, owner_uid)
            .await
            .expect_err("the surviving filesystem ledger must deny the replay");
        assert!(
            matches!(
                err,
                StartupModeDenyReason::ArmInvalid(cutover_arm::CutoverArmDenyReason::ReusedNonce)
            ),
            "{err:?}"
        );
    }

    /// A2 (§5.3 crash window): nonce burned in the DB, rename never
    /// performed (crash between) -- the next attempt denies `ReusedNonce`
    /// and the arm file survives for the audit trail.
    #[tokio::test]
    async fn nonce_insert_before_rename_survives_crash_between() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let consumed_dir = tmp.path().join("consumed");
        let ledger = revops_db::owner::spawn_read_write(&tmp.path().join("ledger.db"))
            .await
            .expect("spawn test nonce ledger");
        let arm_path = write_arm(tmp.path(), "arm.json", &valid_arm_json("a2-crash-nonce"));
        let owner_uid = std::fs::metadata(&arm_path).expect("stat arm").uid();

        // Simulate the crash window: validate + durable burn, NO rename.
        let validated =
            cutover_arm::validate(&arm_path, &test_identity(owner_uid)).expect("arm validates");
        assert!(ledger
            .insert_consumed_arm_nonce(
                validated.nonce().to_string(),
                1_800_000_000,
                validated.source_commit().to_string(),
                validated.binary_sha256().to_string(),
                validated.expires_at(),
            )
            .await
            .expect("burn nonce"));
        drop(validated); // the "crash": no consume_validated ever runs

        let err = consume_arm_via_kernel(&arm_path, &consumed_dir, &ledger, owner_uid)
            .await
            .expect_err("the burned nonce must deny the retry");
        assert!(
            matches!(
                err,
                StartupModeDenyReason::ArmInvalid(cutover_arm::CutoverArmDenyReason::ReusedNonce)
            ),
            "{err:?}"
        );
        assert!(
            arm_path.exists(),
            "the arm file is preserved when the DB ledger denies"
        );
    }

    /// §5.3: a ledger INSERT failure (not a conflict) refuses
    /// `ConsumeFailed` with the arm file untouched -- operator-retryable.
    #[tokio::test]
    async fn ledger_insert_failure_preserves_the_arm_file() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let consumed_dir = tmp.path().join("consumed");
        let ledger_path = tmp.path().join("ledger.db");
        let ledger = revops_db::owner::spawn_read_write(&ledger_path)
            .await
            .expect("spawn test nonce ledger");
        {
            let raw = rusqlite::Connection::open(&ledger_path).expect("raw open");
            raw.execute_batch("DROP TABLE rust_consumed_arm_nonces;")
                .expect("sabotage the ledger table");
        }
        let arm_path = write_arm(tmp.path(), "arm.json", &valid_arm_json("a3-dbfail-nonce"));
        let owner_uid = std::fs::metadata(&arm_path).expect("stat arm").uid();

        let err = consume_arm_via_kernel(&arm_path, &consumed_dir, &ledger, owner_uid)
            .await
            .expect_err("a failed ledger insert must refuse");
        assert!(
            matches!(
                err,
                StartupModeDenyReason::ArmInvalid(
                    cutover_arm::CutoverArmDenyReason::ConsumeFailed(_)
                )
            ),
            "{err:?}"
        );
        assert!(
            arm_path.exists(),
            "the arm file is untouched when the ledger insert fails"
        );
        assert!(!consumed_dir.exists(), "nothing was consumed");
    }

    /// A6 (F6): the FULL async consumption path on a current-thread
    /// runtime -- red under the revision-1 blocking-bridge shape (a
    /// "cannot block the current thread" panic), green proves no blocking
    /// bridge remains anywhere on this path.
    #[tokio::test(flavor = "current_thread")]
    async fn current_thread_runtime_consumption_no_panic() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let consumed_dir = tmp.path().join("consumed");
        let ledger = revops_db::owner::spawn_read_write(&tmp.path().join("ledger.db"))
            .await
            .expect("spawn test nonce ledger");
        let arm_path = write_arm(tmp.path(), "arm.json", &valid_arm_json("a6-ct-nonce"));
        let owner_uid = std::fs::metadata(&arm_path).expect("stat arm").uid();

        let mode = consume_arm_via_kernel(&arm_path, &consumed_dir, &ledger, owner_uid)
            .await
            .expect("full async consumption on a current-thread runtime");
        assert!(matches!(mode, ValidatedFeeMode::LiveAuthority(_)));
        assert!(consumed_dir.join("a6-ct-nonce").exists());
    }

    /// A5 (§5.4, R2-F3): the ONE test that touches the process-global
    /// guard. First take succeeds and resolves; the second take returns
    /// `None`, which production's single call site maps to the typed
    /// `AlreadyResolved` refusal -- regardless of fresh arms.
    #[tokio::test]
    async fn same_process_second_resolution_refuses() {
        let token = StartupResolutionToken::take()
            .expect("the first take in this process must mint the token");

        let tmp = tempfile::tempdir().expect("tempdir");
        let consumed_dir = tmp.path().join("consumed");
        let mode = resolve_startup_mode(
            token,
            StartupModeInputs {
                flags: passive_flags(),
                cutover_arm: None,
                consumed_arm_dir: &consumed_dir,
                state: &virgin_state(),
                seed_binding: &revops_db::fee_runway::SeedBindingState::VirginStore,
                rust_state_store_configured: false,
                nonce_ledger: None,
                now: 1_800_000_000,
            },
        )
        .await
        .expect("first resolution succeeds");
        assert!(matches!(mode, ValidatedFeeMode::PassiveObserver(_)));

        // A second in-process resolution cannot obtain a token -- even a
        // fresh arm changes nothing (the round-1 F7 red: two calls with
        // two fresh arms both succeeded).
        assert!(
            StartupResolutionToken::take().is_none(),
            "the token is minted at most once per process"
        );
        let refusal = StartupModeDenyReason::AlreadyResolved;
        assert!(format!("{refusal}").starts_with("already_resolved:"));
    }

    #[test]
    fn mode_label_matches_every_variant() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let arm_path = write_arm(tmp.path(), "arm.json", &valid_arm_json("label-nonce"));
        let owner_uid = std::fs::metadata(&arm_path).expect("stat arm").uid();
        let consumed_dir = tmp.path().join("consumed");
        let arm =
            cutover_arm::validate_and_consume(&arm_path, &consumed_dir, &test_identity(owner_uid))
                .expect("valid arm consumes");

        let passive = fee_mode::validate_fee_mode(
            passive_flags(),
            None,
            &virgin_state(),
            &revops_db::fee_runway::SeedBindingState::VirginStore,
        )
        .expect("passive row valid");
        assert_eq!(mode_label(&passive), "passive_observer");
        let shadow = fee_mode::validate_fee_mode(
            shadow_flags(),
            None,
            &virgin_state(),
            &revops_db::fee_runway::SeedBindingState::VirginStore,
        )
        .expect("shadow row valid");
        assert_eq!(mode_label(&shadow), "autonomous_shadow");
        let live = fee_mode::validate_fee_mode(
            live_flags(),
            Some(arm),
            &non_virgin_state(),
            &revops_db::fee_runway::SeedBindingState::VerifiedBound {
                cycle_id: "label-seed-cycle".to_string(),
            },
        )
        .expect("live row valid");
        assert_eq!(mode_label(&live), "live_authority");
    }
}
