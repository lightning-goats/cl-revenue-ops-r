use revops_rpc::{call_with_timeout, RpcProxyError};
use std::time::Duration;

#[tokio::test]
async fn timeout_error_string_matches_python() {
    let slow = async {
        tokio::time::sleep(Duration::from_secs(5)).await;
        Ok::<u32, anyhow::Error>(42)
    };
    // 0-second budget forces immediate timeout without slowing the suite.
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
