#![forbid(unsafe_code)]

use anyhow::Result;
use cln_plugin::options::DefaultBooleanConfigOption;
use cln_plugin::Builder;

const VERSION: &str = env!("CARGO_PKG_VERSION");

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

#[tokio::main]
async fn main() -> Result<()> {
    let observer_name = opt_name("observer");
    let observer_opt = DefaultBooleanConfigOption::new_bool_with_default(
        &observer_name,
        true,
        "Run in observer (read-only) mode",
    );
    let ping_name = rpc_name("ping");
    let Some(plugin) = Builder::new(tokio::io::stdin(), tokio::io::stdout())
        .option(observer_opt)
        .rpcmethod(
            &ping_name,
            "liveness probe for the Rust port",
            |_p, _v| async move { Ok(serde_json::json!({"pong": true, "version": VERSION})) },
        )
        .start(())
        .await?
    else {
        return Ok(()); // lightningd disabled us at manifest time
    };
    plugin.join().await
}
