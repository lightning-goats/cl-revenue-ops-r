#![forbid(unsafe_code)]

use anyhow::Result;
use cln_plugin::options::{
    BooleanConfigOption, DefaultBooleanConfigOption, DefaultIntegerConfigOption,
    DefaultStringConfigOption, FlagConfigOption, IntegerConfigOption, StringConfigOption,
};
use cln_plugin::{Builder, Plugin};
use revops::config_types;
use revops::options_table::{self, OptDef};
use revops::rpc_dashboard::{build_dashboard, parse_window_days};
use revops::rpc_history::build_history;
use revops::rpc_report::build_report;
use revops::rpc_status::{build_config_response, build_status, StatusInputs};
use revops::{as_bool_default, as_int_default, as_string_default, now_unix};
use revops::{hydration, notify};
use revops_db::queries;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};

const VERSION: &str = env!("CARGO_PKG_VERSION");

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
    /// Resolved once at init via [`resolve_journal_dir`] (Task 3): `None`
    /// when `revops-r-journal-dir` is empty AND `observer-db-path` is also
    /// unset/unresolved -- there is nothing to derive a dry-run journal
    /// location from in that case. T6's scheduler consumes this to build
    /// `SchedulerConfig.journal_dir` (a required `PathBuf`) and skips
    /// spawning the fee-cycle scheduler entirely when this is `None`.
    journal_dir: Option<PathBuf>,
    /// The running fee-cycle scheduler's handle (T6) -- T7's fee-debug
    /// RPC and wake triggers send through this. Set at most once, AFTER
    /// `configured.start` (the scheduler spawns post-start, but `State`
    /// is built pre-start -- hence `OnceLock` rather than an
    /// `Option` field). Unset whenever the scheduler is off
    /// (`revops-r-fee-dryrun=false`, or a missing db-path/journal-dir --
    /// each case logged explicitly at init).
    scheduler: std::sync::OnceLock<revops::fee_scheduler::SchedulerHandle>,
    /// suffix (as accepted by `revenue-r-config`'s `key` param) -> the full
    /// registered option name (shadow- or canonical-mapped).
    config_names: HashMap<String, String>,
    /// Cached `listconfigs` snapshot of every `revenue-ops-*` (Python
    /// plugin) option's live resolved value, keyed by the FULL Python
    /// option name (e.g. `revenue-ops-min-fee-ppm`). Fetched ONCE at init
    /// via `revops::config_resolve::fetch_python_option_values` -- see that
    /// module's doc comment for the full (a) DB override > (b) this map >
    /// (c) fixture-default precedence `revenue-r-config` resolves through.
    python_option_values: HashMap<String, cln_plugin::options::Value>,
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
fn config_name_map() -> HashMap<String, String> {
    let mut map = HashMap::new();
    map.insert("observer".to_string(), opt_name("observer"));
    map.insert("db-path".to_string(), opt_name("db-path"));
    map.insert("observer-db-path".to_string(), opt_name("observer-db-path"));
    map.insert("journal-dir".to_string(), opt_name("journal-dir"));
    map.insert("fee-dryrun".to_string(), opt_name("fee-dryrun"));
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
    // (Global Constraint); `.dynamic()` per the plan so a later
    // `setconfig` can flip it without a manifest change (T6 itself only
    // reads it once at init -- the scheduler does not start unless it
    // resolves true THERE; live-toggle handling is future work).
    let fee_dryrun_name = opt_name("fee-dryrun");
    let fee_dryrun_opt = DefaultBooleanConfigOption::new_bool_with_default(
        &fee_dryrun_name,
        false,
        "Run the ported fee controller in dry-run: journal decisions next to the observer db, \
         never broadcast. The fee-cycle scheduler starts ONLY when this is true.",
    )
    .dynamic();

    let ping_name = rpc_name("ping");
    let status_name = rpc_name("status");
    let config_name = rpc_name("config");
    let history_name = rpc_name("history");
    let report_name = rpc_name("report");
    let dashboard_name = rpc_name("dashboard");

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
        .subscribe(
            "forward_event",
            |p: Plugin<SharedState>, v: serde_json::Value| async move {
                match p.state().observer_db.clone() {
                    Some(handle) => notify::on_forward_event(&handle, &v).await,
                    None => log_observer_db_drop_once(&FORWARD_EVENT_DROP_LOGGED, "forward_event"),
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
                Ok(())
            },
        )
        .rpcmethod(
            &ping_name,
            "liveness probe for the Rust port",
            |_p, _v| async move { Ok(serde_json::json!({"pong": true, "version": VERSION})) },
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
                Ok(build_status(&StatusInputs {
                    version: s.version.clone(),
                    observer: s.observer,
                    db_path: s.db_path.clone(),
                    db_tables,
                }))
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
                                    let db_override =
                                        if revops::config_resolve::is_immutable_key(key) {
                                            None
                                        } else {
                                            match &s.db {
                                                Some(handle) => {
                                                    queries::config_override(handle, &db_key)
                                                    .await
                                                    .unwrap_or_else(|e| {
                                                        eprintln!(
                                                            "revops: config_override query \
                                                             failed for {db_key}: {e}"
                                                        );
                                                        None
                                                    })
                                                    // CRITICAL 1: mirror
                                                    // `Config._apply_override`'s
                                                    // type/range/enum gate --
                                                    // an override that fails
                                                    // any check is skipped
                                                    // (never surfaced), so
                                                    // resolution falls
                                                    // through to (b)/(c).
                                                    .and_then(|raw| {
                                                        revops::config_resolve::validate_override(
                                                            &db_key, &raw,
                                                        )
                                                    })
                                                }
                                                None => None,
                                            }
                                        }
                                        .map(cln_plugin::options::Value::String);
                                    let python_value =
                                        s.python_option_values.get(&python_name).cloned();
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
            "P&L dashboard (Phase 1b: period.*/net_profit/margin are \
             DB-backed; tlv/roc/warnings/bleeders are gap-marked)",
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
                let pnl = queries::pnl_summary(handle, window_days, now).await?;
                Ok(build_dashboard(&pnl))
            },
        );
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
    let python_option_values =
        revops::config_resolve::fetch_python_option_values(&init_socket_path).await;

    // Task 3: resolve `revops-r-journal-dir` once at init (empty default
    // falls back to the parent of the resolved `observer-db-path`; see
    // `resolve_journal_dir`'s doc comment). T6 consumes `State::journal_dir`
    // to build the fee-cycle scheduler's `SchedulerConfig`.
    let journal_dir_raw = configured.option(&journal_dir_opt)?;
    let journal_dir = resolve_journal_dir(&journal_dir_raw, observer_db_path_expanded.as_deref());

    let fee_dryrun = configured.option(&fee_dryrun_opt)?;

    let state: SharedState = Arc::new(State {
        version: VERSION.to_string(),
        observer,
        db_path,
        db,
        observer_db,
        journal_dir,
        config_names: config_name_map(),
        python_option_values,
        scheduler: std::sync::OnceLock::new(),
    });

    let plugin = configured.start(state).await?;

    // T6: spawn the single-owner fee-cycle scheduler -- ONLY IF the
    // operator opted in AND both required paths resolved; otherwise say
    // exactly why the fee cycle is off (plan requirement: never silent).
    {
        let s = plugin.state();
        if !fee_dryrun {
            eprintln!(
                "revops: fee-cycle scheduler off: {fee_dryrun_name}=false (dry-run not \
                 requested; default is off)"
            );
        } else {
            match (production_db_path_expanded.as_ref(), s.journal_dir.as_ref()) {
                (None, _) => eprintln!(
                    "revops: fee-cycle scheduler off: {fee_dryrun_name}=true but \
                     {db_path_name} is unset/unusable (no production DB to read evidence from)"
                ),
                (_, None) => eprintln!(
                    "revops: fee-cycle scheduler off: {fee_dryrun_name}=true but no journal \
                     dir resolved (set {journal_dir_name} or {observer_db_name})"
                ),
                (Some(prod_db_path), Some(journal_dir)) => {
                    match revops::fee_scheduler::spawn(
                        revops::fee_scheduler::SchedulerConfig {
                            db_path: prod_db_path.clone(),
                            socket_path: init_socket_path.clone(),
                            journal_dir: journal_dir.clone(),
                            // Design Note 1: re-hydrate-per-cycle for the
                            // whole dry-run window; SeedOnce is the
                            // cutover flip.
                            lifecycle: revops::fee_scheduler::StateLifecycle::RehydratePerCycle,
                            // T6b: cycles keyed off Python's observed
                            // end-of-cycle state flush (FixedInterval is
                            // the cutover flip, alongside SeedOnce).
                            trigger: revops::fee_scheduler::TriggerMode::default(),
                        },
                        s.db.clone(),
                        s.python_option_values.clone(),
                    ) {
                        Ok(handle) => {
                            let _ = s.scheduler.set(handle);
                            eprintln!(
                                "revops: fee-cycle scheduler started (dry-run; journal dir {}, \
                                 flush-triggered: poll {}s / settle {}s off Python's \
                                 fee_strategy_state flush marker)",
                                journal_dir.display(),
                                revops::fee_scheduler::DEFAULT_FLUSH_POLL_SECS,
                                revops::fee_scheduler::DEFAULT_FLUSH_SETTLE_SECS
                            );
                        }
                        Err(e) => eprintln!(
                            "revops: fee-cycle scheduler FAILED to start: {e:#}; fee dry-run \
                             disabled for this plugin lifetime"
                        ),
                    }
                }
            }
        }
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
}
