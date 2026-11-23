//! async-enabled 面（`tokio` feature）：fx 取消可见 + scope 停机净。

use skeleton_infra::fx::{FxScope, cancel_pair};
use tokio::sync::oneshot;

/// 取消探针：任务先进入等待点（oneshot 握手，无 sleep），宿主触发取消，
/// 断言 token 立即可见、任务在下一让渡点结束、scope 停机后无在途。
///
/// ⚠️ 消费方视角缺口（Stage C 报告原料；**框架侧已修**）：`CancelSource`
/// 曾非 `Clone` 且 [`FxScope::with_source`] 按值取走 ⇒ 接线后宿主手里再无
/// 可触发取消的句柄。现 `CancelSource: Clone`（Clone = **同一取消态**的第二
/// 句柄，不是独立源）⇒ 宿主可预先 `let handle = source.clone()`，把源按值
/// 交给 `with_source` 后用 `handle.cancel()` 触发。框架侧回归见
/// `skeleton-infra/tests/fx_cancel_source_clone.rs`（2 例）。本探针仍保留
/// `cancel_pair` 的 source 在宿主侧触发的既有形态（不改变已入库证据）。
pub async fn run_cancellation_probe() -> Result<String, String> {
    let (source, token) = cancel_pair();
    let scope = FxScope::new();

    let task_token = token.clone();
    let (started_tx, started_rx) = oneshot::channel::<()>();
    let spawned = scope
        .spawn_tracked(async move {
            let _ = started_tx.send(());
            task_token.cancelled().await;
        })
        .map_err(|closed| format!("spawn 被拒（scope 已关）: {closed:?}"))?;

    started_rx
        .await
        .map_err(|_| String::from("任务未进入等待点就结束"))?;
    if token.is_cancelled() {
        return Err(String::from("取消在探测前已发生"));
    }

    source.cancel();
    if !token.is_cancelled() {
        return Err(String::from("取消未对 token 可见"));
    }

    spawned
        .join()
        .await
        .map_err(|err| format!("任务取消后未正常结束: {err:?}"))?;
    let report = scope.shutdown().await;
    if scope.in_flight() != 0 {
        return Err(format!("停机后仍有在途任务: {}", scope.in_flight()));
    }
    Ok(format!(
        "cancel-async: token_cancelled=true completed={} cancelled={} timed_out={} still_running={}",
        report.completed, report.cancelled, report.timed_out, report.still_running
    ))
}
