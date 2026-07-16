#![forbid(unsafe_code)]

use anyhow::Result;
use cln_plugin::options::{
    BooleanConfigOption, DefaultBooleanConfigOption, DefaultIntegerConfigOption,
    DefaultStringConfigOption, FlagConfigOption, IntegerConfigOption, StringConfigOption,
};
use cln_plugin::{Builder, Plugin};
use revops::config_types;
use revops::options_table::{self, OptDef};
use revops::rpc_status::{build_config_response, build_status, StatusInputs};
use revops::{as_bool_default, as_int_default, as_string_default};
use revops::{hydration, notify};
use std::collections::HashMap;
use std::path::PathBuf;
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
    /// suffix (as accepted by `revenue-r-config`'s `key` param) -> the full
    /// registered option name (shadow- or canonical-mapped).
    config_names: HashMap<String, String>,
}

/// `cln-plugin` clones the state per request; keep it cheap to clone by
/// Arc'ing the actual data. Does NOT hold a DB `Connection` directly (that
/// type is `!Sync`) — `db` is a [`revops_db::actor::DbHandle`], a cheap
/// `Clone`-able `mpsc::Sender` to the single-owner task that actually holds
/// the `Connection` (see `revops_db::actor`).
type SharedState = Arc<State>;

/// suffix -> full registered option name, for every option this plugin
/// exposes: our own (`observer`, `db-path`) plus the entire shadowed Python
/// option surface from `fixtures/options.json`.
fn config_name_map() -> HashMap<String, String> {
    let mut map = HashMap::new();
    map.insert("observer".to_string(), opt_name("observer"));
    map.insert("db-path".to_string(), opt_name("db-path"));
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

    let ping_name = rpc_name("ping");
    let status_name = rpc_name("status");
    let config_name = rpc_name("config");

    let builder = Builder::new(tokio::io::stdin(), tokio::io::stdout())
        .option(observer_opt.clone())
        .option(db_path_opt.clone())
        .option(observer_db_opt.clone())
        .subscribe(
            "forward_event",
            |p: Plugin<SharedState>, v: serde_json::Value| async move {
                if let Some(handle) = p.state().observer_db.clone() {
                    notify::on_forward_event(&handle, &v).await;
                }
                Ok(())
            },
        )
        .subscribe(
            "connect",
            |p: Plugin<SharedState>, v: serde_json::Value| async move {
                if let Some(handle) = p.state().observer_db.clone() {
                    notify::on_connect(&handle, &v).await;
                }
                Ok(())
            },
        )
        .subscribe(
            "disconnect",
            |p: Plugin<SharedState>, v: serde_json::Value| async move {
                if let Some(handle) = p.state().observer_db.clone() {
                    notify::on_disconnect(&handle, &v).await;
                }
                Ok(())
            },
        )
        .subscribe(
            "channel_state_changed",
            |p: Plugin<SharedState>, v: serde_json::Value| async move {
                if let Some(handle) = p.state().observer_db.clone() {
                    notify::on_channel_state_changed(&handle, &v).await;
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
                        let value = p.option_str(full_name)?;
                        let field_type = config_types::field_type_for(key);
                        // Phase 1b has no DB-backed config-override-write
                        // path yet, so there is no live per-key version to
                        // report; build_config_response documents this
                        // placeholder in its `_phase1b_gaps` array.
                        Ok(build_config_response(
                            key,
                            true,
                            value.as_ref(),
                            field_type,
                            0,
                        ))
                    }
                    None => Ok(build_config_response(key, false, None, None, 0)),
                }
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
    let observer_db_raw = configured.option(&observer_db_opt)?;
    let observer_db = match (!observer_db_raw.is_empty()).then_some(observer_db_raw) {
        Some(raw) => {
            let path = expand_tilde(&raw);
            match revops_db::owner::spawn_read_write(&path).await {
                Ok(handle) => Some(handle),
                Err(e) => {
                    eprintln!(
                        "revops: {observer_db_name} spawn failed ({e}); notification ingestion \
                         disabled (never falls back to the production db-path connection)"
                    );
                    None
                }
            }
        }
        None => None,
    };

    let state: SharedState = Arc::new(State {
        version: VERSION.to_string(),
        observer,
        db_path,
        db,
        observer_db,
        config_names: config_name_map(),
    });

    let plugin = configured.start(state).await?;

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
        // SAFETY: test-only env mutation; not run concurrently with other
        // HOME-reading tests in this process (cargo runs unit tests in
        // this crate's own process, but each #[test] here is
        // self-contained and doesn't read HOME elsewhere).
        let prev = std::env::var("HOME").ok();
        std::env::set_var("HOME", "/home/testuser");
        let expanded = expand_tilde("~/.lightning/revenue_ops.db");
        match prev {
            Some(v) => std::env::set_var("HOME", v),
            None => std::env::remove_var("HOME"),
        }
        assert_eq!(
            expanded,
            PathBuf::from("/home/testuser/.lightning/revenue_ops.db")
        );
    }

    /// Bare `~` (no trailing slash) expands to exactly `$HOME`.
    #[test]
    fn expand_tilde_expands_bare_tilde() {
        let prev = std::env::var("HOME").ok();
        std::env::set_var("HOME", "/home/testuser");
        let expanded = expand_tilde("~");
        match prev {
            Some(v) => std::env::set_var("HOME", v),
            None => std::env::remove_var("HOME"),
        }
        assert_eq!(expanded, PathBuf::from("/home/testuser"));
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
}
