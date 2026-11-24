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

#[cfg(feature = "tokio")]
#[path = "../../../../skeleton-infra/src/test_support/guard.rs"]
mod guards;

/// multi-thread profile（issue#97）：Consumer 自己建/选 tokio runtime（UDF 不创建
/// runtime），复用 `FxScope` / `CancelSource` / `SyncStore` 同一装配，只验证多
/// worker 调度下消费者关心的五类契约：外部取消可见、任务并行推进、并发停机的
/// owner/waiter 分工、waiter 提前退出不改写 owner 的排空、`join_timeout` 与任务体
/// 真正析构之间的计数窗口。
///
/// 与 `async_face`（current-thread profile）并存：两者是不同执行环境，互不替代。
/// 并发证据走握手（oneshot / `Barrier` / 原子闸）+ OS 时钟护栏：不用固定 sleep 作
/// 先后条件，也不以 thread id 不同推断并发（worker 窃取不保证放置）。
#[cfg(feature = "tokio")]
mod async_multi_thread {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use cli_host::{Executed, HostFeature, JobId, RecordingFx};
    use skeleton::Store as _;
    use skeleton_infra::fx::{FxScope, Spawned, TaskError, cancel_pair};
    use skeleton_infra::runtime::{AdmissionOutcome, CloseWaitOutcome, SyncStore};
    use tokio::sync::oneshot;

    use super::guards::{bounded, kick_scheduler, wait_until};
    use super::{cancel, submit};

    /// 自旋放行闸的 RAII 兜底：断言 panic 时也必须放行——worker 滞留在无 await 的
    /// poll 里会让运行时拆除期等它（挂死比一条红更难诊断）。
    struct ReleaseOnDrop(Arc<AtomicBool>);

    impl Drop for ReleaseOnDrop {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    /// 消费者侧的阻塞工作闸：任务体进入自旋段（无 await 点）后 abort 只能等放行，
    /// 正好把「已请求 abort」与「任务体已析构」之间的窗口固定住。
    /// 自旋段内不能 await ⇒ 「已进入」握手走 oneshot、放行走原子量。
    struct BlockGate {
        release: Arc<AtomicBool>,
        _release_on_drop: ReleaseOnDrop,
    }

    impl BlockGate {
        fn new() -> (Self, oneshot::Sender<()>, oneshot::Receiver<()>) {
            let (entered_tx, entered_rx) = oneshot::channel();
            let release = Arc::new(AtomicBool::new(false));
            let gate = Self {
                release: Arc::clone(&release),
                _release_on_drop: ReleaseOnDrop(release),
            };
            (gate, entered_tx, entered_rx)
        }

        fn release_flag(&self) -> Arc<AtomicBool> {
            Arc::clone(&self.release)
        }
    }

    /// 自旋段占住原 driver 持有者 ⇒ 唤醒一台普通休眠 worker 重新 park，让它接手
    /// 时间轮（否则截止永不到期，issue#102 机理）。
    async fn kick_scheduler_for_spin() {
        kick_scheduler(
            "唤醒在 driver 上 park 的 worker（自旋段占住原 driver 持有者）",
            FxScope::new()
                .spawn_detached(async {})
                .expect("一次性 kick scope 新开 ⇒ 闸必开"),
        )
        .await;
    }

    /// 造一条停在无 await 自旋段里的工作任务：阻塞工作误留 executor 的最坏形态，
    /// abort 只能等闸门放行才生效。
    ///
    /// 返回 (句柄, 闸门, 「已进入自旋段」握手接收端)——握手 await 完即证明任务体确
    /// 已进入自旋段（否则后面「超时返回时任务体仍在跑」的断言无意义）。
    fn spawn_blocked_job(
        scope: &FxScope,
        job: JobId,
    ) -> (Spawned<JobId>, BlockGate, oneshot::Receiver<()>) {
        let (gate, entered_tx, entered_rx) = BlockGate::new();
        let release = gate.release_flag();
        let handle = scope
            .spawn_tracked(async move {
                entered_tx
                    .send(())
                    .expect("任务体须能把「已进入自旋段」信号送达测试侧");
                while !release.load(Ordering::Acquire) {
                    std::hint::spin_loop();
                }
                job
            })
            .expect("闸开 ⇒ 阻塞任务可准入");
        (handle, gate, entered_rx)
    }

    /// 场景①外部取消：Consumer 预先 clone `CancelSource`，接线后由 clone 句柄在多
    /// worker profile 下触发取消；两条已同时挂起在等待点的托管任务各自观察到，并把
    /// 后续取消记账落进共享 `SyncStore`（消费方的动作走同一 store 装配）。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn external_cancel_reaches_waiting_tasks_on_multi_worker_runtime() {
        let (source, token) = cancel_pair();
        let handle = source.clone();
        let scope = FxScope::new().with_source(source);

        let fx = RecordingFx::new();
        let observed = fx.clone();
        let store = Arc::new(SyncStore::new(HostFeature, fx));
        store.set_capacity(8);

        let mut entered = Vec::new();
        let mut handles = Vec::new();
        for job in [11u64, 12u64] {
            let (entered_tx, entered_rx) = oneshot::channel::<()>();
            entered.push(entered_rx);
            let task_token = scope.token();
            let store = Arc::clone(&store);
            handles.push(
                scope
                    .spawn_tracked(async move {
                        entered_tx
                            .send(())
                            .expect("任务体须能把「已进入」信号送达测试侧");
                        task_token.cancelled().await;
                        // 观察到取消之后才做的消费方记账（同一 store 跨 worker 准入）。
                        (job, store.try_dispatch(cancel(job)))
                    })
                    .expect("闸开 ⇒ 两任务都准入"),
            );
        }
        // 两任务都已挂起在取消等待点：同时挂起 = 真并发，不以 thread id 推断。
        for entered_rx in entered {
            entered_rx.await.expect("任务体须进入取消等待点");
        }
        assert!(!token.is_cancelled(), "触发前不得已取消");
        assert_eq!(scope.in_flight(), 2, "两任务须同时在途");

        handle.cancel();
        assert!(handle.is_cancelled(), "clone 句柄自身可见");
        assert!(token.is_cancelled(), "原 token 可见");
        assert!(scope.token().is_cancelled(), "scope token 可见");

        for spawned in handles {
            let (job, outcome) = spawned.join().await.expect("取消是协作式返回，非 abort");
            assert_eq!(
                outcome,
                AdmissionOutcome::Accepted,
                "job {job} 的取消记账被拒"
            );
        }

        let report = scope.shutdown().await;
        assert_eq!(report.completed, 2, "两任务皆协作式终局");
        assert_eq!(report.cancelled, 0);
        assert!(report.panicked.is_empty(), "{:?}", report.panicked);
        assert_eq!(scope.in_flight(), 0);
        scope.expect_body_idle();
        scope.expect_fully_reaped_idle();

        let snapshot = store.snapshot();
        let mut cancelled: Vec<JobId> = snapshot.queue.cancelled.clone();
        cancelled.sort_unstable();
        assert_eq!(cancelled, vec![11, 12], "两任务各记一次移除：不丢不重");
        let executed = observed.executed();
        assert_eq!(executed.len(), 2, "{executed:?}");
        for entry in &executed {
            assert!(
                matches!(entry, Executed::Cancel { id } if *id == 11 || *id == 12),
                "取消记账落成非取消形: {entry:?}"
            );
        }
    }

    /// 场景②并行推进：两条任务经三方 `Barrier`（两任务 + 测试侧）同时进入受控等待
    /// 区，证明它们在同一时刻都在途挂起；放行后各自从自己的 worker 并发准入同一
    /// `SyncStore`——并发准入不丢 action。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tasks_advance_in_parallel_and_admit_concurrently() {
        let store = Arc::new(SyncStore::new(HostFeature, RecordingFx::new()));
        store.set_capacity(8);
        let scope = FxScope::new();
        let barrier = Arc::new(tokio::sync::Barrier::new(3));

        let mut entered = Vec::new();
        let mut handles = Vec::new();
        for job in [21u64, 22u64] {
            let (entered_tx, entered_rx) = oneshot::channel::<()>();
            entered.push(entered_rx);
            let barrier = Arc::clone(&barrier);
            let store = Arc::clone(&store);
            handles.push(
                scope
                    .spawn_tracked(async move {
                        entered_tx
                            .send(())
                            .expect("任务体须能把「已进入」信号送达测试侧");
                        barrier.wait().await;
                        store.try_dispatch(submit(job))
                    })
                    .expect("闸开 ⇒ 两任务都准入"),
            );
        }
        for entered_rx in entered {
            entered_rx.await.expect("任务体须进入受控等待区");
        }
        // 同时挂起且各持一个并发槽位 ⇒ 两者都在途（并行推进的观测面）。
        assert_eq!(scope.in_flight(), 2);
        assert_eq!(scope.counts().active, 2);

        barrier.wait().await; // 测试侧到位 ⇒ 三方同时放行
        for spawned in handles {
            assert_eq!(
                spawned.join().await.expect("正常终局"),
                AdmissionOutcome::Accepted
            );
        }

        let snapshot = store.snapshot();
        let mut submitted: Vec<JobId> = snapshot.queue.submitted.clone();
        submitted.sort_unstable();
        assert_eq!(submitted, vec![21, 22], "并发准入不得丢 action");
        assert_eq!(store.close_wait(), CloseWaitOutcome::Drained);

        let report = scope.shutdown().await;
        assert_eq!(report.completed, 2, "两任务皆正常终局");
        scope.expect_body_idle();
        scope.expect_fully_reaped_idle();
    }

    /// 场景③④并发停机 + waiter 提前退出：owner（本任务）的排空被非让渡段钉住期间，
    /// 另一处（`tokio::spawn` 出去的 waiter）到达 `shutdown_timeout`——单 owner、
    /// waiter 共享 owner 的 canonical 报告；waiter 自己的截止到期**只**停止它自己
    /// 的等待，不取消、不改写 owner 的排空（owner 随后照常发布等净结果）。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_shutdown_waiter_exits_early_without_touching_owner_drain() {
        let scope = Arc::new(FxScope::new());
        let (blocked, gate, entered_rx) = spawn_blocked_job(&scope, 31);
        entered_rx.await.expect("阻塞任务须进入无让渡段");
        kick_scheduler_for_spin().await;

        let release = gate.release_flag();
        let (waiter_tx, waiter_rx) = oneshot::channel();
        let waiter_scope = Arc::clone(&scope);
        let waiter = tokio::spawn(async move {
            // 等 owner 已发布排空（闸已关）再到达 ⇒ 角色确定是 waiter 而非 owner。
            let closed = Arc::clone(&waiter_scope);
            wait_until("owner 已进入排空（scope 已关）", move || {
                closed.is_closed()
            })
            .await;
            let report = bounded(
                "waiter 的 shutdown_timeout 有界返回",
                waiter_scope.shutdown_timeout(Duration::from_millis(50)),
            )
            .await;
            // 放行的是测试侧的阻塞工作闸（消费者自己的 worker），与 owner 的排空无关
            // ——owner 等的是任务体终局，不是 waiter 的读数。
            release.store(true, Ordering::Release);
            waiter_tx.send(report).expect("waiter 须回送读数给测试侧");
        });

        let owner_report = bounded("owner 的 shutdown 有界返回", scope.shutdown()).await;
        let waiter_report = waiter_rx.await.expect("waiter 须回送自己的到期读数");
        waiter.await.expect("waiter 任务正常结束");

        // waiter 的读数只含自己的截止：终局桶恒空，非排空中断。
        assert_eq!(waiter_report.timed_out, 1, "截止时仍有 1 条在途");
        assert_eq!(waiter_report.still_running, 1, "终止未确认");
        assert_eq!(waiter_report.completed, 0, "waiter 不得接管 owner 的终局账");
        assert_eq!(waiter_report.cancelled, 0);
        assert!(waiter_report.panicked.is_empty());
        assert!(!waiter_report.drain_interrupted, "waiter 退出不是排空中断");

        // owner 的排空没被改写：等净至空集，终局恰落一桶。
        assert_eq!(owner_report.completed + owner_report.cancelled, 1);
        assert_eq!(owner_report.timed_out, 0);
        assert_eq!(owner_report.still_running, 0);
        assert!(!owner_report.drain_interrupted);
        assert_ne!(
            owner_report, waiter_report,
            "waiter 读数不是 canonical 报告"
        );

        // 晚到者（顺序语义）拿到同一次排空的 canonical 副本。
        let canonical = scope.shutdown().await;
        assert_eq!(canonical, owner_report, "晚到者共享同一 canonical 报告");
        assert_eq!(scope.in_flight(), 0);
        scope.expect_body_idle();
        scope.expect_fully_reaped_idle();

        // 阻塞任务的终局：正常返回或被排空 abort——两形都合法（桶已归集）。
        match blocked.join().await {
            Ok(job) => assert_eq!(job, 31),
            Err(TaskError::Cancelled) => {}
            other => panic!("阻塞任务终局只能是正常返回或被 abort: {other:?}"),
        }
    }

    /// 场景⑤计数窗口：`Spawned::join_timeout` 只表达「截止已过 + 已请求 abort」；
    /// 任务体停在无让渡段时 `in_flight` 与并发槽位都**不**回落，直到任务体真正
    /// 析构。句柄被消费也不取消 scope 的生命周期跟踪（条目留到停机收割）。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn join_timeout_leaves_counting_window_open_until_body_drops() {
        let scope = FxScope::new();
        let (blocked, gate, entered_rx) = spawn_blocked_job(&scope, 41);
        entered_rx.await.expect("阻塞任务须进入无让渡段");
        kick_scheduler_for_spin().await;

        let outcome = bounded(
            "join_timeout 有界返回",
            blocked.join_timeout(Duration::from_millis(50)),
        )
        .await;
        assert_eq!(outcome, Err(TaskError::TimedOut), "截止已过 + 已请求 abort");
        // 窗口：abort 只是请求 ⇒ 任务体仍在自旋段里跑，两个计数都不能回落。
        assert_eq!(scope.in_flight(), 1, "任务体未析构 ⇒ in_flight 不回落");
        assert_eq!(scope.counts().active, 1, "槽位随任务体析构才归还");

        gate.release_flag().store(true, Ordering::Release);
        wait_until(
            "任务体真正析构（在途归零 + 终局记账已落）",
            || scope.in_flight() == 0 && scope.counts().completed_unreaped == 1,
        )
        .await;
        scope.expect_body_idle();
        assert_eq!(scope.counts().active, 0, "析构后槽位归零");

        let report = scope.shutdown().await;
        assert_eq!(
            report.completed + report.cancelled,
            1,
            "滞留条目在停机排空时收割（abort 与任务体返回的竞态两形都合法）"
        );
        assert_eq!(report.timed_out, 0);
        scope.expect_fully_reaped_idle();
    }
}
