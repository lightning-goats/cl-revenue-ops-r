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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn timeout_basic() {
        let slow = async {
            tokio::time::sleep(Duration::from_secs(5)).await;
            Ok::<u32, anyhow::Error>(42)
        };
        let err = call_with_timeout("listchannels", 0, slow)
            .await
            .unwrap_err();
        assert_eq!(err.to_string(), "RPC timeout after 0s on listchannels");
        assert!(matches!(err, RpcProxyError::Timeout { .. }));
    }

    #[tokio::test]
    async fn passthrough_on_success() {
        let fast = async { Ok::<u32, anyhow::Error>(7) };
        assert_eq!(call_with_timeout("getinfo", 15, fast).await.unwrap(), 7);
    }
}
