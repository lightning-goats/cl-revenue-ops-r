#![forbid(unsafe_code)]

use std::future::Future;
use std::time::Duration;

#[derive(Debug, thiserror::Error)]
pub enum RpcProxyError {
    /// Exact Python contract string — matched by callers and log greps.
    #[error("RPC timeout after {seconds}s on {method}")]
    Timeout { method: String, seconds: u64 },
    #[error(transparent)]
    Rpc(#[from] anyhow::Error),
}

pub async fn call_with_timeout<F, T>(method: &str, seconds: u64, fut: F) -> Result<T, RpcProxyError>
where
    F: Future<Output = Result<T, anyhow::Error>>,
{
    match tokio::time::timeout(Duration::from_secs(seconds), fut).await {
        Ok(inner) => inner.map_err(RpcProxyError::from),
        Err(_) => Err(RpcProxyError::Timeout {
            method: method.to_string(),
            seconds,
        }),
    }
}

// Unit-test coverage for `call_with_timeout` lives in the integration test
// `crates/revops-rpc/tests/timeout.rs` (`timeout_error_string_matches_python`,
// `passthrough_on_success`) -- those duplicated this module's former inline
// `#[cfg(test)]` bodies verbatim, so the inline copies were removed.
