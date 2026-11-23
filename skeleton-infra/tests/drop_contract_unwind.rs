#![cfg(feature = "runtime")]
//! #66：unwind 清理期 detached payload 的析构合同。
//!
//! 契约正文 = `SyncStore` 类型文档「Action 析构义务（unwind 清理期）」+
//! `skeleton::Store` 语义契约 7。本测钉三件事：
//!
//! 1. **锁外析构**：`DrainGuard::drop` 摘队结算后，残留载荷的 `Drop` 里调用
//!    只读面（`pending_actions` / `snapshot` / `subscriber_count` /
//!    `rejected_after_close`）必须成功——若析构发生在某把内部锁内，同线程重取
//!    即自死锁；父线程有界超时把「挂死」变成「判红」（禁 sleep，用
//!    `recv_timeout`）。变异形态（把载荷析构挪进 queue 临界区）会让本测超时。
//! 2. **真的析构了**：断言析构次数与读数——断言非恒真（漏析构 / 重放即红）。
//! 3. **重入边界无框架防护**：放旗在结算前已完成 ⇒ 析构期的一次准入会成功并
//!    同步排空。「Drop 不得重入同一 Store」因此是**宿主义务**。本测只演示边界
//!    存在（生产代码这么写是契约禁止的，见上述契约段）。

use skeleton::{EffectBuf, Infallible, NoFx, Reducer, Store as _};
use skeleton_infra::runtime::SyncStore;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

/// 有界等待：锁内析构的退化形态必须「判红」而不是挂死 CI。
const BOUNDED_WAIT: Duration = Duration::from_secs(10);

type ProbeStore = SyncStore<u32, Probe, Infallible>;

fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "<non-string panic>".to_string()
    }
}

#[derive(Clone, Copy)]
enum Kind {
    /// reduce 内先同栈回灌一条 [`Kind::Nested`]，再 panic。
    Boom,
    /// 留在队列里、被 unwind 清理整批摘下的残留。
    Nested,
    /// 析构期重入 / 展开后正常 dispatch 用的良性 action。
    Benign,
}

/// 析构期观测面：全原子（`Drop` 内禁 panic，断言统一放父线程）。
#[derive(Default)]
struct Obs {
    boom_drops: AtomicUsize,
    nested_drops: AtomicUsize,
    nested_reduced: AtomicUsize,
    benign_reduced: AtomicUsize,
    pending_at_drop: AtomicUsize,
    snapshot_at_drop: AtomicUsize,
    subs_at_drop: AtomicUsize,
    rejected_at_drop: AtomicUsize,
    reentrant_admitted: AtomicBool,
}

struct Probe {
    kind: Kind,
    obs: Arc<Obs>,
    /// 只读面句柄。用 `Arc` 而不是 `SyncStore` 克隆：载荷析构只减引用计数，
    /// 不触发 `SyncStore::drop` 的弃队记账。
    store: Option<Arc<ProbeStore>>,
}

impl Probe {
    fn boom(store: Arc<ProbeStore>, obs: Arc<Obs>) -> Self {
        Self {
            kind: Kind::Boom,
            obs,
            store: Some(store),
        }
    }

    fn nested(store: Arc<ProbeStore>, obs: Arc<Obs>) -> Self {
        Self {
            kind: Kind::Nested,
            obs,
            store: Some(store),
        }
    }

    fn benign(obs: Arc<Obs>) -> Self {
        Self {
            kind: Kind::Benign,
            obs,
            store: None,
        }
    }
}

impl Drop for Probe {
    // 本 Drop 本身就是被钉的契约：panic-free + 只碰只读面 / 准入面。
    fn drop(&mut self) {
        match self.kind {
            Kind::Boom => {
                self.obs.boom_drops.fetch_add(1, Ordering::SeqCst);
            }
            Kind::Benign => {}
            Kind::Nested => {
                self.obs.nested_drops.fetch_add(1, Ordering::SeqCst);
                let Some(store) = &self.store else {
                    return;
                };
                // 只读面：任何一把内部锁被持有 ⇒ 同线程自死锁 ⇒ 父线程超时判红。
                self.obs
                    .pending_at_drop
                    .store(store.pending_actions(), Ordering::SeqCst);
                self.obs
                    .snapshot_at_drop
                    .store(*store.snapshot() as usize, Ordering::SeqCst);
                self.obs
                    .subs_at_drop
                    .store(store.subscriber_count(), Ordering::SeqCst);
                self.obs
                    .rejected_at_drop
                    .store(store.rejected_after_close() as usize, Ordering::SeqCst);
                // 重入边界：排空旗已放 ⇒ 这里的准入被接受、且同步排空至 idle。
                let admitted = store
                    .sender()
                    .dispatch(Probe::benign(Arc::clone(&self.obs)));
                self.obs
                    .reentrant_admitted
                    .store(admitted, Ordering::SeqCst);
            }
        }
    }
}

struct ProbeReducer;

impl Reducer for ProbeReducer {
    type State = u32;
    type Action = Probe;
    type Op = Infallible;

    fn initial(&self) -> u32 {
        0
    }

    fn reduce(&self, state: &u32, action: Probe, _out: &mut EffectBuf<Probe, Infallible>) -> u32 {
        match action.kind {
            Kind::Boom => {
                // 同栈回灌：本线程正在排空 ⇒ 只入队即返（不入排空环、不自死锁）。
                // 该记录随后在 panic 展开时被整批摘下 = detached 载荷。
                let Some(store) = &action.store else {
                    panic!("reducer boom (drill): missing store handle");
                };
                let admitted = store
                    .sender()
                    .dispatch(Probe::nested(Arc::clone(store), Arc::clone(&action.obs)));
                assert!(
                    admitted,
                    "同栈回灌必须被准入（否则残留不存在，测试前提失效）"
                );
                panic!("reducer boom (drill)");
            }
            // 命中即「被中断的 action 被重放」——契约禁止（#45 at-most-once）。
            Kind::Nested => {
                action.obs.nested_reduced.fetch_add(1, Ordering::SeqCst);
                *state
            }
            Kind::Benign => {
                action.obs.benign_reduced.fetch_add(1, Ordering::SeqCst);
                state + 1
            }
        }
    }
}

/// 展开清理期的残留载荷：析构在锁外、恰好一次；重入边界无框架防护。
#[test]
fn unwind_cleanup_drops_detached_payload_outside_internal_locks() {
    let (tx, rx) = mpsc::channel();
    let worker = thread::spawn(move || {
        let obs = Arc::new(Obs::default());
        let store = Arc::new(SyncStore::new(ProbeReducer, NoFx));
        let boom = catch_unwind(AssertUnwindSafe(|| {
            store.dispatch(Probe::boom(Arc::clone(&store), Arc::clone(&obs)));
        }));
        let panic_msg = boom
            .as_ref()
            .err()
            .map(|p| panic_message(p.as_ref()))
            .unwrap_or_default();
        // 展开清理期已结算终局回执并放旗 ⇒ 实例照常可用（#41 语义不因 #66 变形）。
        let state_after_unwind = *store.snapshot();
        let pending_after_unwind = store.pending_actions();
        store.dispatch(Probe::benign(Arc::clone(&obs)));
        let state_after_post_dispatch = *store.snapshot();
        // 接收端已消失 = 父线程已判红，此时无额外信息可报。
        let _ = tx.send((
            Arc::clone(&obs),
            boom.is_err(),
            panic_msg,
            state_after_unwind,
            pending_after_unwind,
            state_after_post_dispatch,
        ));
    });

    let (obs, panicked, panic_msg, state_after_unwind, pending_after_unwind, state_after_post) =
        rx.recv_timeout(BOUNDED_WAIT).expect(
            "10s 内未完成：detached 载荷的析构被内部锁挡住（同线程自死锁形态）——\
             载荷析构必须在锁外（DrainGuard::drop 的摘队临界区之外）",
        );
    worker
        .join()
        .expect("场景线程非预期 panic（断言全部在父线程）");

    // 前提：reducer panic 原样上浮（fail-visible），不是被吞。
    assert!(panicked, "reducer panic 必须如实传播（fail-visible）");
    assert!(
        panic_msg.contains("reducer boom (drill)"),
        "panic 必须是 reducer 那次（实际：{panic_msg}）"
    );

    // ① 两个载荷各析构恰好一次：被中断的 action（reducer 按值消费时展开析构）
    //    与 detached 残留（DrainGuard::drop 的结算后析构）。
    assert_eq!(
        obs.boom_drops.load(Ordering::SeqCst),
        1,
        "被中断 action 的载荷必须在展开中恰好析构一次"
    );
    assert_eq!(
        obs.nested_drops.load(Ordering::SeqCst),
        1,
        "detached 队列残留的载荷必须在 unwind 清理期被析构（漏析构 = 泄漏）"
    );
    assert_eq!(
        obs.nested_reduced.load(Ordering::SeqCst),
        0,
        "被中断的 action 禁重放（它从未被 reduce）"
    );

    // ② 析构期只读面全部可达（锁已释放），且读数对得上当时的真实状态。
    assert_eq!(
        obs.pending_at_drop.load(Ordering::SeqCst),
        0,
        "析构发生在摘队之后：队列已空"
    );
    assert_eq!(
        obs.snapshot_at_drop.load(Ordering::SeqCst),
        0,
        "Boom 未提交：状态不前进（失败是提交前的）"
    );
    assert_eq!(obs.subs_at_drop.load(Ordering::SeqCst), 0, "无订阅者");
    assert_eq!(
        obs.rejected_at_drop.load(Ordering::SeqCst),
        0,
        "未闭门：无拒绝"
    );

    // ③ 重入边界：放旗在前 ⇒ 析构期准入仍被接受并同步排空。框架没有防护
    //    ⇒「不得重入」只能靠契约（本测不主张生产代码这么写）。
    assert!(
        obs.reentrant_admitted.load(Ordering::SeqCst),
        "排空旗已放 ⇒ 析构期准入被接受（边界靠契约不靠框架）"
    );
    assert_eq!(
        obs.benign_reduced.load(Ordering::SeqCst),
        2,
        "1 次来自析构期重入、1 次来自展开后的正常 dispatch"
    );

    // ④ #41 语义不变：中断后队列不搁浅、实例可用。
    assert_eq!(state_after_unwind, 1, "重入那条 benign 已在析构期落账");
    assert_eq!(pending_after_unwind, 0, "残留被摘走：无孤儿 action");
    assert_eq!(state_after_post, 2, "展开后正常 dispatch 照常生效");
}
