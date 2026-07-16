#![forbid(unsafe_code)]

use anyhow::Result;
use cln_plugin::options::{
    BooleanConfigOption, DefaultBooleanConfigOption, DefaultIntegerConfigOption,
    DefaultStringConfigOption, FlagConfigOption, IntegerConfigOption, StringConfigOption,
};
use cln_plugin::{Builder, Plugin};
use revops::options_table::{self, OptDef};
use revops::rpc_status::{build_config_response, build_status, StatusInputs};
use revops::{as_bool_default, as_int_default, as_string_default};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Shared plugin state, resolved once at init (option values, and — if
/// `revops-r-db-path` is set — a one-shot read-only DB probe). See
/// `revops::rpc_status` for the pure response builders that consume this.
struct State {
    version: String,
    observer: bool,
    db_path: Option<String>,
    db_tables: Option<usize>,
    /// suffix (as accepted by `revenue-r-config`'s `key` param) -> the full
    /// registered option name (shadow- or canonical-mapped).
    config_names: HashMap<String, String>,
}

/// `cln-plugin` clones the state per request; keep it cheap to clone by
/// Arc'ing the actual data. Does NOT hold a DB `Connection` (that type is
/// `!Sync`) — the DB is opened, probed, and dropped once at init. Phase 1b
/// brings a persistent-connection actor.
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
    let db_path_opt = DefaultStringConfigOption::new_str_with_default(
        &db_path_name,
        "",
        "Path to the revops sqlite database, opened read-only at init (empty = disabled)",
    );

    let ping_name = rpc_name("ping");
    let status_name = rpc_name("status");
    let config_name = rpc_name("config");

    let builder = Builder::new(tokio::io::stdin(), tokio::io::stdout())
        .option(observer_opt.clone())
        .option(db_path_opt.clone())
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
                Ok(build_status(&StatusInputs {
                    version: s.version.clone(),
                    observer: s.observer,
                    db_path: s.db_path.clone(),
                    db_tables: s.db_tables,
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
                        Ok(build_config_response(key, true, value.as_ref()))
                    }
                    None => Ok(build_config_response(key, false, None)),
                }
            },
        );
    let builder = register_python_options(builder, canonical_names());

    let Some(configured) = builder.configure().await? else {
        return Ok(()); // lightningd disabled us (or --help) at manifest time
    };

    let observer = configured.option(&observer_opt)?;
    let db_path_raw = configured.option(&db_path_opt)?;
    let db_path_setting = (!db_path_raw.is_empty()).then_some(db_path_raw);

    // Open the DB read-only once at init, count its tables, and drop the
    // connection — it is not `Sync` so it cannot live in plugin state.
    // Phase 1b brings a persistent-connection actor.
    let (db_path, db_tables) = match db_path_setting {
        Some(raw) => {
            let path = PathBuf::from(&raw);
            match revops_db::open_read_only(&path) {
                Ok(conn) => match revops_db::table_names(&conn) {
                    Ok(tables) => {
                        let count = tables.len();
                        drop(conn);
                        (Some(raw), Some(count))
                    }
                    Err(e) => {
                        configured
                            .disable(&format!(
                                "{db_path_name} set but listing tables failed: {e}"
                            ))
                            .await?;
                        return Ok(());
                    }
                },
                Err(e) => {
                    configured
                        .disable(&format!("{db_path_name} set but DB open failed: {e}"))
                        .await?;
                    return Ok(());
                }
            }
        }
        None => (None, None),
    };

    let state: SharedState = Arc::new(State {
        version: VERSION.to_string(),
        observer,
        db_path,
        db_tables,
        config_names: config_name_map(),
    });

    let plugin = configured.start(state).await?;
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
}
