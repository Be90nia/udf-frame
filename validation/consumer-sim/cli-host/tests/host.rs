//! Stage B 集成测试：真实并发 Full / 取消序 / 停机 typed 拒绝 / 组合快照 /
//! `Arc::make_mut` 克隆分支观测 / CLI 入口冒烟。
//!
//! 并发用例用 [`DrainGate`] 的 condvar 握手（非 sleep）钉住排空租约后
//! 再发准入——Full 是真实时序结果，不是编译级断言。

use std::sync::Arc;
use std::time::Duration;

use cli_host::{
    DrainGate, Executed, GATE_JOB, HostAction, HostFeature, JobAction, RecordingFx, run_cli,
};
use skeleton::Store as _;
use skeleton_infra::runtime::{AdmissionOutcome, CloseReportOutcome, CloseWaitOutcome, SyncStore};

fn submit(job: u64) -> HostAction {
    HostAction::Job(JobAction::Submit { job })
}

fn cancel(job: u64) -> HostAction {
    HostAction::Job(JobAction::Cancel { job })
}

#[test]
fn bounded_admission_reports_full_under_real_concurrent_drain() {
    let gate = Arc::new(DrainGate::default());
    let fx = RecordingFx::gated(Arc::clone(&gate));
    let observed = fx.clone();
    let store = SyncStore::new(HostFeature, fx);
    store.set_capacity(1);

    std::thread::scope(|scope| {
        let drainer = scope.spawn(|| {
            assert_eq!(
                store.try_dispatch(submit(GATE_JOB)),
                AdmissionOutcome::Accepted
            );
        });

        // 解释器已钉住排空租约：此后外部准入只入队、不夺排空权。
        gate.wait_until_entered();
        assert_eq!(store.try_dispatch(submit(1)), AdmissionOutcome::Accepted);
        assert_eq!(store.try_dispatch(submit(2)), AdmissionOutcome::Full);
        assert_eq!(store.admission_full_count(), 1);

        gate.release();
        drainer.join().expect("drainer 线程");
    });

    let snapshot = store.snapshot();
    assert_eq!(
        snapshot.queue.submitted,
        vec![GATE_JOB, 1],
        "Full 的 action 不得入队"
    );
    assert_eq!(
        observed.executed(),
        vec![
            Executed::Run {
                id: GATE_JOB,
                job: GATE_JOB,
            },
            Executed::Run { id: 1, job: 1 },
        ],
        "执行序 = 准入序，被拒的 job 2 无效果"
    );
    assert_eq!(store.close_wait(), CloseWaitOutcome::Drained);
}

#[test]
fn interpreter_records_run_then_cancel_in_order() {
    let fx = RecordingFx::new();
    let observed = fx.clone();
    let store = SyncStore::new(HostFeature, fx);
    store.set_capacity(8);

    assert_eq!(store.try_dispatch(submit(5)), AdmissionOutcome::Accepted);
    assert_eq!(store.try_dispatch(cancel(5)), AdmissionOutcome::Accepted);

    assert_eq!(
        observed.executed(),
        vec![Executed::Run { id: 5, job: 5 }, Executed::Cancel { id: 5 },]
    );
    assert_eq!(
        store.cancel_unknown_id(),
        0,
        "Cancel 指向已登记的 Run 不得计 unknown"
    );
    let snapshot = store.snapshot();
    assert_eq!(snapshot.queue.submitted, vec![5]);
    assert_eq!(snapshot.queue.cancelled, vec![5]);
    assert_eq!(store.close_wait(), CloseWaitOutcome::Drained);
}

#[test]
fn shutdown_close_wait_reports_drained_and_types_later_admissions() {
    let fx = RecordingFx::new();
    let observed = fx.clone();
    let store = SyncStore::new(HostFeature, fx);
    store.set_capacity(4);
    assert_eq!(store.try_dispatch(submit(9)), AdmissionOutcome::Accepted);

    let report = match store.close_wait_with_report(Duration::from_secs(5)) {
        CloseReportOutcome::Drained(report) => report,
        other => panic!("静止 store 的停机必须报 Drained: {other:?}"),
    };
    assert_eq!(report.queued_actions, 0);
    assert_eq!(report.pending_effects.effects, 0);
    assert!(report.interrupted_effect.is_none());
    assert!(store.is_closed());

    assert_eq!(
        store.try_dispatch(HostAction::Job(JobAction::Finished { job: 9 })),
        AdmissionOutcome::Closed
    );
    assert_eq!(store.rejected_after_close(), 1);
    assert_eq!(observed.executed(), vec![Executed::Run { id: 9, job: 9 }]);

    // 停机幂等：重复 close / close_wait 不改终态。
    store.close();
    assert_eq!(store.close_wait(), CloseWaitOutcome::Drained);
    assert!(store.is_closed());
}

#[test]
fn composed_root_snapshot_freezes_held_arc_and_walks_make_mut_clone() {
    let store = SyncStore::new(HostFeature, RecordingFx::new());
    store.set_capacity(8);
    assert_eq!(store.try_dispatch(submit(1)), AdmissionOutcome::Accepted);

    let held = store.snapshot();
    assert_eq!(Arc::strong_count(&held), 2, "store 内部 + 外部持有");

    assert_eq!(store.try_dispatch(submit(2)), AdmissionOutcome::Accepted);
    assert_eq!(
        Arc::strong_count(&held),
        1,
        "store 已放手旧 Arc ⇒ 本轮落位走 Arc::make_mut 克隆分支"
    );
    assert_eq!(held.queue.submitted, vec![1], "旧快照冻结（CoW）");
    assert_eq!(held.heartbeats, 0);

    let live = store.snapshot();
    assert_eq!(live.queue.submitted, vec![1, 2]);
    assert_eq!(live.heartbeats, 0);
    drop(live);

    assert_eq!(
        store.try_dispatch(HostAction::Heartbeat),
        AdmissionOutcome::Accepted
    );
    let live = store.snapshot();
    assert_eq!(live.heartbeats, 1);
    assert_eq!(live.queue.submitted, vec![1, 2], "父层分支不动子切片");
    assert_eq!(held.heartbeats, 0, "父层与子切片同属一份冻结快照");
    assert_eq!(store.close_wait(), CloseWaitOutcome::Drained);
}

#[test]
fn heartbeat_parent_branch_leaves_child_slice_untouched() {
    let store = SyncStore::new(HostFeature, RecordingFx::new());
    assert_eq!(store.try_dispatch(submit(7)), AdmissionOutcome::Accepted);
    assert_eq!(
        store.try_dispatch(HostAction::Heartbeat),
        AdmissionOutcome::Accepted
    );

    let snapshot = store.snapshot();
    assert_eq!(snapshot.queue.submitted, vec![7]);
    assert_eq!(snapshot.heartbeats, 1);
    assert_eq!(store.close_wait(), CloseWaitOutcome::Drained);
}

#[test]
fn cli_entry_runs_submit_cancel_and_shutdown() {
    let submit_args = ["submit".to_string(), "4".to_string()];
    let submitted = run_cli(&submit_args).expect("submit 段");
    assert!(submitted.contains("Run { id: 4, job: 4 }"), "{submitted}");

    let cancel_args = ["cancel".to_string(), "4".to_string()];
    let cancelled = run_cli(&cancel_args).expect("cancel 段");
    assert!(cancelled.contains("Cancel { id: 4 }"), "{cancelled}");

    let shutdown_args = ["shutdown".to_string()];
    let shutdown = run_cli(&shutdown_args).expect("shutdown 段");
    assert!(
        shutdown.contains("post_close_admission=Closed"),
        "{shutdown}"
    );

    assert!(run_cli(&["bogus".to_string()]).is_err());
}

#[cfg(feature = "tokio")]
mod async_face {
    use skeleton_infra::fx::{FxScope, cancel_pair};
    use tokio::sync::oneshot;

    /// G1 修复后的消费方实证：`CancelSource: Clone`（同一取消态第二句柄）⇒
    /// 预先 clone 的 handle 可在 `with_source` 接线后触发取消，scope token 与
    /// 任务同拍可见。oneshot 握手（无 sleep）保证取消发生在任务启动之后。
    #[tokio::test]
    async fn cloned_source_cancels_attached_scope_during_lifetime() {
        let (source, token) = cancel_pair();
        let handle = source.clone();
        let scope = FxScope::new().with_source(source);

        let scope_token = scope.token();
        assert!(!scope_token.is_cancelled(), "接线时未取消");

        let task_token = scope_token.clone();
        let (started_tx, started_rx) = oneshot::channel::<()>();
        let spawned = scope
            .spawn_tracked(async move {
                let _ = started_tx.send(());
                task_token.cancelled().await;
            })
            .expect("spawn 应准入");
        started_rx.await.expect("任务未进入等待点就结束");

        // scope 存续期，用接线前 clone 的句柄触发取消。
        handle.cancel();
        assert!(handle.is_cancelled(), "句柄自身可见");
        assert!(token.is_cancelled(), "原 token 可见");
        assert!(scope.token().is_cancelled(), "scope token 可见");

        spawned.join().await.expect("任务应在取消后下一让渡点结束");
        let report = scope.shutdown().await;
        assert_eq!(scope.in_flight(), 0, "停机后无在途");
        assert_eq!(
            report.completed, 1,
            "任务正常终局（取消是协作式返回，非 abort）"
        );
    }

    #[tokio::test]
    async fn cancellation_is_visible_and_scope_drains() {
        let line = cli_host::async_host::run_cancellation_probe()
            .await
            .expect("取消探针");
        assert!(line.contains("token_cancelled=true"), "{line}");
        assert!(line.contains("completed=1"), "{line}");
    }

    #[tokio::test]
    async fn cancel_before_subscribe_is_sticky() {
        let (source, _token) = cancel_pair();
        source.cancel();
        let scope = FxScope::new().with_source(source);
        assert!(
            scope.token().is_cancelled(),
            "取消是粘性的：订阅前取消也必须可见"
        );
        let report = scope.shutdown().await;
        assert_eq!(scope.in_flight(), 0);
        assert_eq!(report.completed, 0);
    }
}
