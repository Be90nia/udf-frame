#![cfg(feature = "journal")]
//! JournalPort 契约端口验收：`JsonlJournal` 兑现 `skeleton::JournalPort`——
//! 后端自换路径成立（宿主面向零依赖端口编程，JSONL 只是参考实现）。

use serde::{Deserialize, Serialize};
use skeleton::{JournalPort, Restore};
use skeleton_infra::journal::JsonlJournal;
use std::path::{Path, PathBuf};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct Act {
    id: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct St {
    n: u64,
}

/// 独立子目录；通过用例收尾 remove_dir_all（同 journal.rs case_dir 惯例）。
fn case_dir(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target-iso/journal-port-tests")
        .join(name);
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// impl 断言（编译期）：JsonlJournal 实现契约端口，且端口无 A/S 侧约束
///（零依赖端口对实现词表零假设）。
#[test]
fn jsonl_journal_impls_journal_port() {
    fn assert_impl<T: JournalPort<Act, State = St, Err = skeleton_infra::journal::JournalError>>() {
    }
    assert_impl::<JsonlJournal<Act, St>>();
}

/// 三态恢复语义：Empty（空账本）/ FromInitial（无快照全量流）/
/// FromSnapshot（快照+增量）。读侧可见性=已 flush 前缀（BufWriter 纪律）。
#[test]
fn journal_port_restore_three_shapes() {
    let dir = case_dir("three-shapes");
    let mut j = JsonlJournal::<Act, St>::create(dir.join("j.jsonl")).unwrap();

    // dyn 焊接面（批 6A 关联类型化后仍一线可达）：`State =` 把状态面焊进
    // 对象类型，dyn 上方法调用零标注——后端可换性面在 trait object 下成立。
    type Port = dyn JournalPort<Act, State = St, Err = skeleton_infra::journal::JournalError>;

    let empty: Restore<Act, St> = j.restore().unwrap();
    assert_eq!(empty, Restore::Empty, "空账本=Empty");

    Port::append(&mut j, &Act { id: 1 }).unwrap();
    Port::append(&mut j, &Act { id: 2 }).unwrap();
    j.flush().unwrap(); // 未 flush 的尾部对读侧不可见
    let full: Restore<Act, St> = j.restore().unwrap();
    assert_eq!(
        full,
        Restore::FromInitial(vec![Act { id: 1 }, Act { id: 2 }]),
        "无快照行=自 initial 全量播"
    );

    Port::persist_snapshot(&mut j, &St { n: 2 }).unwrap(); // 快照即写即刷
    Port::append(&mut j, &Act { id: 3 }).unwrap();
    j.flush().unwrap();
    let delta: Restore<Act, St> = j.restore().unwrap();
    assert_eq!(
        delta,
        Restore::FromSnapshot(St { n: 2 }, vec![Act { id: 3 }]),
        "快照后动作=增量"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// E0283/E0034 治理证明：两侧 trait 同屏 import，直呼（方法语法，零
/// turbofish/零 UFCS/零 DynJournal 别名）编译通过——旧世界（实现层与端口
/// 三同名方法）本用例在 `j.restore()` 处双候选拒编译；改名消重后每名唯一
/// 候选。（批 6A 关联类型化后返回类型标注也不再必需——`Self` 钉死状态面；
/// 此处保留标注以钉旧世界行为基线。）
#[test]
fn direct_method_calls_resolve_uniquely_with_both_traits_in_scope() {
    use skeleton_infra::journal::Journal as _;
    use skeleton_infra::journal::{Replay, ReplayError}; // 实现层全功能面同屏

    let dir = case_dir("direct-calls");
    let j = JsonlJournal::<Act, St>::create(dir.join("j.jsonl")).unwrap();

    // 直呼 1（端口侧 restore）：返回类型标注钉 (A, S)，唯一候选直达
    let restored: Restore<Act, St> = j.restore().unwrap();
    assert_eq!(restored, Restore::Empty, "空账本=Empty");

    // 直呼 2（实现侧 restore_stream）：同屏双 trait 下唯一候选直达
    let streamed: Option<(St, Replay<'_, Act>)> = j.restore_stream().unwrap();
    assert!(
        streamed.is_none(),
        "无快照行=None（调用方回退 initial 全量回放）"
    );
    let rows: Vec<Result<Act, ReplayError>> = match streamed {
        Some((_, replay)) => replay.collect(),
        None => Vec::new(),
    };
    assert!(rows.is_empty());

    let _ = std::fs::remove_dir_all(&dir);
}

/// 批 6A reshape 验收：`MutexGuard` 上直呼实现层方法——零 turbofish、零 UFCS、
/// 零显式 deref 注记、零绑定标注（`A` 从 action 参数自然推断，`State` 被
/// `Self` 钉死）。旧世界（`Journal<A, S>` 的 `S` 不被方法参数约束）本用例
/// E0283 拒编译，须 `Journal::<A, S>::` 全限定 + `&mut *guard` 显式 deref。
#[test]
fn mutex_guard_direct_append_zero_turbofish() {
    use skeleton_infra::journal::{DynJournal, Journal as _};

    let dir = case_dir("guard-direct");
    let m = std::sync::Mutex::new(JsonlJournal::<Act, St>::create(dir.join("j.jsonl")).unwrap());
    let mut g = m.lock().unwrap();

    // 直呼：guard 自动 deref 为接收者，A 从 action 参数推断，State 由 Self 归一。
    g.append_action(&Act { id: 1 }).unwrap();
    g.append_snapshot(&St { n: 1 }).unwrap();
    g.flush().unwrap();

    // 读面直呼同样零标注：replay/restore_stream 全推断（Self 钉死双面）。
    assert_eq!(g.replay().unwrap().count(), 1, "快照行不计入 action 回放");
    let streamed = g.restore_stream().unwrap();
    assert!(
        streamed.is_some(),
        "有快照行=Some((快照态, 增量回放))，绑定零标注拿到全型"
    );

    // dyn 焊接面：别名 `DynJournal<A, S>`（= dyn Journal<A, State = S>）钉死后直呼。
    let obj: &mut DynJournal<Act, St> = &mut *g;
    obj.append_action(&Act { id: 2 }).unwrap();

    drop(g);
    let _ = std::fs::remove_dir_all(&dir);
}

// ── Emit 回灌的账本面语义（可观测 R1 清零）：确定性派生不入账，重放再生 ─────

/// Emit 回灌动作由 store 核心直接入队（不经解释器，无挂点可设）——账本不入账。
/// 语义对齐的锚点：Emit 派生动作是**已落盘 action 经纯 reducer 的确定性派生**
///（同 (state, action) 恒同输出，Reducer 契约 #1），重放协议 = Restore 动作流
/// 经同一装配的 store 逐条 dispatch（派生链由排空环再生）。三方可查：
/// live 终态 == 重放终态 == 审计流深度链重建。
#[cfg(feature = "runtime")]
mod emit_journal_convergence {
    use serde::{Deserialize, Serialize};
    use skeleton::{Effect, EffectBuf, NoFx, Reducer, Restore, Store as _};
    use skeleton_infra::audit::AuditEventKind;
    use skeleton_infra::journal::JsonlJournal;
    use skeleton_infra::runtime::SyncStore as Store;
    use std::convert::Infallible;
    use std::path::{Path, PathBuf};

    /// Begin：状态落 1 并派生一次 Emit(Stroke)（回灌链有界：Stroke 零效果）。
    /// Stroke：状态 +10。live 一次 dispatch(Begin) 实际 apply 两个动作
    ///（Begin + 派生 Stroke），账本只有 Begin 一行。
    /// `tag = "kind"`：账本行要求对象 payload（行 tag 平铺约束），unit enum
    /// 默认序列化成字符串不合法。
    #[derive(Serialize, Deserialize, Debug)]
    #[serde(tag = "kind")]
    enum Draw {
        Begin,
        Stroke,
    }

    struct DrawFeature;

    impl Reducer for DrawFeature {
        type State = u64;
        type Action = Draw;
        type Op = Infallible;
        fn initial(&self) -> u64 {
            0
        }
        fn reduce(&self, s: &u64, a: Draw, out: &mut EffectBuf<Draw, Infallible>) -> u64 {
            match a {
                Draw::Begin => {
                    out.push(Effect::Emit(Draw::Stroke));
                    1
                }
                Draw::Stroke => s + 10,
            }
        }
    }

    fn case_dir(name: &str) -> PathBuf {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target-iso/emit-convergence")
            .join(name);
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// 重放协议深度链快照：Enqueued(0) → Reduced → Enqueued(1) → Reduced。
    fn assert_feedback_chain(events: &[skeleton_infra::audit::AuditEvent]) {
        let kinds: Vec<_> = events.iter().map(|e| e.kind).collect();
        assert_eq!(
            kinds,
            vec![
                AuditEventKind::Enqueued { feedback_depth: 0 },
                AuditEventKind::Reduced,
                AuditEventKind::Enqueued { feedback_depth: 1 },
                AuditEventKind::Reduced,
            ],
            "审计流可重建 Emit 派生链（depth 0→1），实际: {kinds:?}"
        );
    }

    #[test]
    fn emit_feedback_replay_converges_with_live() {
        let dir = case_dir("converge");
        let path = dir.join("j.jsonl");

        // live：Begin 落盘后 dispatch——派生 Stroke 经 Emit 回灌被 apply（不入账）。
        let live_final = {
            let mut j = JsonlJournal::<Draw, u64>::create(&path).unwrap();
            use skeleton_infra::journal::Journal as _;
            j.append_action(&Draw::Begin).unwrap();
            j.flush().unwrap();
            let store = Store::new(DrawFeature, NoFx);
            let _ticket = store.subscribe_events(64, |_| {});
            store.dispatch(Draw::Begin);
            assert_eq!(*store.snapshot(), 11, "live：Begin(→1) + 派生 Stroke(→11)");
            assert_feedback_chain(&store.take_events());
            // 账本侧核对：恰一行 Begin（派生动作不入账——派生即已落盘 action 的
            // 纯函数像，重放可精确再生，重复记录=账本噪声）。
            use skeleton_infra::journal::ReplayError;
            let rows: Vec<Result<Draw, ReplayError>> = j.replay().unwrap().collect();
            assert_eq!(rows.len(), 1, "账本只有外部动作一行");
            j.flush().unwrap();
            *store.snapshot()
        };

        // 重放：Restore 动作流经同一装配的 store 逐条 dispatch（非裸 reduce）——
        // Emit 派生链由排空环再生，终态必收敛。
        let replay_final = {
            let j = JsonlJournal::<Draw, u64>::open(&path, 1).unwrap();
            let store = Store::new(DrawFeature, NoFx);
            let _ticket = store.subscribe_events(64, |_| {});
            // 关联类型化后零 turbofish：Self 钉死 (A, State)，路径限定即可达。
            match skeleton::JournalPort::restore(&j).unwrap() {
                Restore::FromInitial(actions) => {
                    for a in actions {
                        store.dispatch(a);
                    }
                }
                other => panic!("无快照行应为 FromInitial，实际: {other:?}"),
            }
            assert_eq!(*store.snapshot(), 11, "重放：派生 Stroke 再生");
            assert_feedback_chain(&store.take_events());
            *store.snapshot()
        };

        assert_eq!(live_final, replay_final, "同一 action 序列 live==重放终态");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// F1（接口维 R1）：`Restore::FromSnapshot` 的第一类落点——
    /// `SyncStore::restore_from` 播种构造器。四面断言：
    /// ①快照态直入（不经 `initial`：快照值 11 ≠ initial 0）；②增量重放经
    /// dispatch——派生 Emit 链由排空环再生（裸 reduce 终态=1，必挂）；
    /// ③审计深度链逐 action 交错可重建（F2 生命周期口径的实证锚）；
    /// ④重建后 store 照常 live（续 dispatch 正常推进）。
    /// （账本→`Restore` 的产出面由 `journal_port_restore_three_shapes` 锚定；
    /// 本模块 Draw fixture 状态为裸 u64，不满足账本行对象 payload 约束，
    /// 故手构 `Restore` 值直测播种面——pass-through 组合是类型安全的一行。）
    #[test]
    fn snapshot_restore_seeds_state_and_replays_deltas_through_dispatch() {
        // live 基准：Begin→1 派生 Stroke→11（快照点）；增量 Begin 再走 1→11。
        let live = Store::new(DrawFeature, NoFx);
        live.dispatch(Draw::Begin);
        assert_eq!(
            *live.snapshot(),
            11,
            "live 前半：Begin(→1) + 派生 Stroke(→11)"
        );
        let snap = *live.snapshot();
        live.dispatch(Draw::Begin);
        assert_eq!(*live.snapshot(), 11, "live 后半：增量 Begin 再走 1→11");

        // 恢复：FromSnapshot(11, [Begin]) 直传播种构造——快照态直入 + 增量经
        // dispatch 重放。
        let store = Store::restore_from(
            DrawFeature,
            NoFx,
            Restore::FromSnapshot(snap, vec![Draw::Begin]),
        );
        let _ticket = store.subscribe_events(64, |_| {});
        assert_eq!(
            *store.snapshot(),
            11,
            "快照播种 + 增量重放收敛（裸 reduce 无派生链只会到 1）"
        );
        assert_eq!(*live.snapshot(), *store.snapshot(), "live==快照恢复终态");
        // 构造期增量重放的派生链再生已由状态断言锚定（11 ≠ 裸 reduce 的 1）；
        // 其事件发生在订阅开闸前（构造先于订阅，取证窗不可覆盖）——交错链断言
        // 挂在重建后订阅窗内的续 dispatch 上（同为 F2 生命周期口径的实证锚）：
        // `Notified` 记账的是状态快照订阅者的通知（事件订阅不开此闸——对照
        // emit_feedback_replay_converges_with_live 的无 Notified 链），挂一个
        // 状态订阅者让 F2 交错口径全链可观测：
        let _sub = store.subscribe(|_| {});
        store.dispatch(Draw::Begin);
        let kinds: Vec<_> = store.take_events().iter().map(|e| e.kind).collect();
        assert_eq!(
            kinds,
            vec![
                AuditEventKind::Enqueued { feedback_depth: 0 },
                AuditEventKind::Reduced,
                AuditEventKind::Enqueued { feedback_depth: 1 },
                AuditEventKind::Notified,
                AuditEventKind::Reduced,
                AuditEventKind::Notified,
            ],
            "续 dispatch：Emit 派生链+逐 action 交错通知可重建，实际: {kinds:?}"
        );
        // 重建后照常 live：续 dispatch 正常推进（11→1→11）。
        assert_eq!(*store.snapshot(), 11, "重建后续 dispatch 行为正常");
    }

    /// F1 纯播种面：零增量动作 = 快照态原样重建，且零 dispatch（审计流空）。
    #[test]
    fn snapshot_restore_with_no_deltas_seeds_state_without_dispatch() {
        let store = Store::restore_from(DrawFeature, NoFx, Restore::FromSnapshot(42u64, vec![]));
        let _ticket = store.subscribe_events(64, |_| {});
        assert_eq!(
            *store.snapshot(),
            42,
            "快照态直入（42 ≠ initial 0：未经 initial、未套用任何动作）"
        );
        assert!(
            store.take_events().is_empty(),
            "零增量=零 dispatch（播种不是重放流量）"
        );
    }
}
