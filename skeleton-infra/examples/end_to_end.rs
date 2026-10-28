//! 端到端小例：核心五件套 + `combine` + `TestStore`
//! + fx/journal 解释器标准形——采用方起步链路的可运行样本。
//!
//! 跑法：`cargo run -p udf-skeleton-infra --features tokio,journal,testing,compose --example end_to_end`

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use skeleton::{
    ActionSender, Driver, Effect, EffectBuf, EffectId, EffectRuntime, Reducer, Store as _,
};
use skeleton_infra::compose::combine;
use skeleton_infra::runtime::SyncStore as Store;
use skeleton_infra::testing::{ExpectedEffect, TestStore};
use skeleton_infra::{expect_idle, FxScope, Journal as _, JsonlJournal, Spawned};

// ---- 领域：父 Board 挂一个 Tally 子 feature（combine 显式结构）----

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Board {
    tally: TallyState,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct TallyState {
    count: u64,
    last_notified: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
enum BoardAct {
    Tally(TallyAct),
    /// 父层分支自理的动作：本例组合根直用 combine，unwrap=None → 无操作
    /// （零效果、状态不动）——「禁字符串路由」的显式结构形态。
    Ping,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum TallyAct {
    Increment(u64),
    /// 解释器回灌动作（环闭合）：确认外部已收到通知，不再发效果（环终止条件）
    Notified(u64),
}

#[derive(Debug, PartialEq)]
enum BoardFx {
    Tally(TallyFx),
}

#[derive(Debug, PartialEq)]
enum TallyFx {
    /// 描述式副作用：请外部世界通知 n（宿主解释，feature 不执行）
    Notify(u64),
}

/// 子 feature：Tally——增量计数 + 一次性通知（只在 Increment 发效果，Notified
/// 不发 → 回灌链必收敛，Emit/效果环自查条目的正面样本）。
struct TallyFeature;

impl Reducer for TallyFeature {
    type State = TallyState;
    type Action = TallyAct;
    type Op = TallyFx;

    fn initial(&self) -> TallyState {
        TallyState::default()
    }

    fn reduce(
        &self,
        s: &TallyState,
        a: TallyAct,
        out: &mut EffectBuf<TallyAct, TallyFx>,
    ) -> TallyState {
        match a {
            TallyAct::Increment(n) => {
                out.push(Effect::Run {
                    id: EffectId(s.count + n),
                    op: TallyFx::Notify(n),
                });
                TallyState {
                    count: s.count + n,
                    last_notified: s.last_notified,
                }
            }
            TallyAct::Notified(n) => TallyState {
                count: s.count,
                last_notified: Some(n),
            },
        }
    }
}

/// 子切片分片（combine 六参，零 dyn 零装箱）——产物是状态分片不是根：
/// `initial` 恒 panic（fail-closed），根状态归 [`BoardFeature`] 显式构造。
fn make_fragment() -> impl Reducer<State = Board, Action = BoardAct, Op = BoardFx> {
    combine(
        |b: &Board| &b.tally,
        |b: &mut Board, t: TallyState| b.tally = t,
        BoardAct::Tally,
        |a: &BoardAct| match a {
            BoardAct::Tally(t) => Some(t.clone()),
            BoardAct::Ping => None,
        },
        TallyFeature,
        |fx: Effect<TallyAct, TallyFx>, out: &mut EffectBuf<BoardAct, BoardFx>| match fx {
            // combine 契约：Emit 由核心经 wrap 上浮，lift_fx 只收 Run/Cancel
            Effect::Emit(_) => unreachable!("Emit is up-floated by core via wrap"),
            Effect::Run { id, op } => out.push(Effect::Run {
                id,
                op: BoardFx::Tally(op),
            }),
            Effect::Cancel { id } => out.push(Effect::Cancel { id }),
        },
    )
}

/// 根 Feature：initial 显式构造父状态（含子切片初值）；子路由动作委托分片，
/// 父层动作（Ping）分支自理（本例=零迁移零效果）。
struct BoardFeature;

impl Reducer for BoardFeature {
    type State = Board;
    type Action = BoardAct;
    type Op = BoardFx;

    fn initial(&self) -> Board {
        Board {
            tally: TallyFeature.initial(),
        }
    }

    fn reduce(&self, s: &Board, a: BoardAct, out: &mut EffectBuf<BoardAct, BoardFx>) -> Board {
        match a {
            BoardAct::Tally(_) => make_fragment().reduce(s, a, out),
            BoardAct::Ping => s.clone(),
        }
    }
}

// ---- 第一半：TestStore 验收面（脚本化解释器，宿主逻辑层零 IO 全测）----

fn test_store_face() {
    let mut ts = TestStore::new(BoardFeature);
    ts.dispatch(BoardAct::Tally(TallyAct::Increment(2)));
    // 效果回填断言：id: None = 不校验寻址 id（同义恒同值纪律见 TestStore 文档）
    ts.receive(ExpectedEffect::Run {
        id: None,
        op: BoardFx::Tally(TallyFx::Notify(2)),
    });
    assert_eq!(ts.state().tally.count, 2);
    ts.dispatch(BoardAct::Tally(TallyAct::Notified(2)));
    assert_eq!(ts.state().tally.last_notified, Some(2));
    println!("[1/2] TestStore 验收面：dispatch/receive/state 全过");
}

// ---- 第二半：真 Store + fx/journal 解释器标准形（端到端环）----

/// 解释器标准形宿主：效果 spawn 后即返（dispatch 同步排空，解释器禁阻塞）；
/// 外部交互超时必填与一 Effect 一 span 的真实宿主接 log/tracing。
/// 回灌句柄由核心经 run 的 back 参数传入（克隆进任务即闭环），宿主无需自存。
struct HostRuntime {
    scope: Arc<FxScope>,
    journal: Arc<Mutex<JsonlJournal<BoardAct, Board>>>,
    pending: Arc<Mutex<Vec<Spawned<()>>>>,
}

impl EffectRuntime<BoardAct, BoardFx> for HostRuntime {
    fn run(&mut self, effect: Effect<BoardAct, BoardFx>, back: &ActionSender<BoardAct>) {
        let next = match effect {
            Effect::Run {
                id: _,
                op: BoardFx::Tally(TallyFx::Notify(n)),
            } => BoardAct::Tally(TallyAct::Notified(n)),
            Effect::Cancel { .. } => return, // 本例无在途取消语义
            // Emit 由核心折叠入队，永不进入解释器转发路径
            Effect::Emit(_) => unreachable!("Emit never reaches the interpreter"),
        };
        let back = back.clone();
        let journal = self.journal.clone();
        let handle = self
            .scope
            .spawn_tracked(async move {
                tokio::time::sleep(Duration::from_millis(5)).await; // 模拟外部 IO（真实宿主超时必填）
                                                                    // 回灌挂点（append-before-send）：账本写成功才回灌；
                                                                    // append Err ⇒ 禁 send（fail-closed，禁静默丢）
                let appended = journal
                    .lock()
                    .expect("journal poisoned")
                    .append_action(&next)
                    .is_ok();
                assert!(
                    appended,
                    "journal append failed: refusing to re-enter (fail-closed)"
                );
                assert!(back.dispatch(next), "store closed: re-entry rejected");
            })
            .expect("scope open");
        self.pending.lock().expect("pending poisoned").push(handle);
    }
}

/// 回灌挂点·外部动作：组合根包装层 append→dispatch（先落盘后 apply）。
fn journaled_dispatch(
    store: &Store<Board, BoardAct, BoardFx>,
    journal: &mut JsonlJournal<BoardAct, Board>,
    a: BoardAct,
) {
    journal.append_action(&a).expect("journal append");
    store.dispatch(a);
}

async fn runtime_face() {
    // 临时账本归 crate 构建隔离目录（禁系统 TEMP）；残留先清（create_new 撞残留=Err）
    let dir = Path::new("../../target-iso/example-journal");
    std::fs::create_dir_all(dir).expect("mkdir target-iso/example-journal");
    let path = dir.join("board.jsonl");
    let _ = std::fs::remove_file(&path);

    let journal = Arc::new(Mutex::new(
        JsonlJournal::<BoardAct, Board>::create(&path).expect("create journal"),
    ));
    let scope = Arc::new(FxScope::new());
    let pending = Arc::new(Mutex::new(Vec::new()));

    let store = Store::new(
        BoardFeature,
        HostRuntime {
            scope: scope.clone(),
            journal: journal.clone(),
            pending: pending.clone(),
        },
    );

    journaled_dispatch(
        &store,
        &mut journal.lock().expect("journal poisoned"),
        BoardAct::Tally(TallyAct::Increment(2)),
    );
    // dispatch 已返回（解释器 spawn 后即返）；效果任务在途——join 全部在途回灌
    let handles: Vec<_> = pending
        .lock()
        .expect("pending poisoned")
        .drain(..)
        .collect();
    for h in handles {
        h.join().await.expect("effect task ok");
    }

    let snap = store.snapshot();
    assert_eq!(snap.tally.count, 2, "count via combine lens");
    assert_eq!(
        snap.tally.last_notified,
        Some(2),
        "re-entry closed the loop"
    );
    // 回放可见性=已 flush 前缀（flush 点=持久点）——断言前显式冲刷
    journal
        .lock()
        .expect("journal poisoned")
        .flush()
        .expect("journal flush");
    let replay = journal
        .lock()
        .expect("journal poisoned")
        .replay()
        .expect("replay")
        .filter_map(|r| r.ok())
        .count();
    assert_eq!(replay, 2, "journal = truth source: 1 external + 1 re-entry");

    // 停机拼装序：fx.close（拒新）→ 在途净 → journal.flush（持久点）→ store drop
    scope.close();
    store.close();
    store.dispatch(BoardAct::Ping); // 闭门后 no-op
    assert_eq!(store.rejected_after_close(), 1, "fail-visible counter");
    scope.shutdown().await;
    expect_idle(&scope);
    journal
        .lock()
        .expect("journal poisoned")
        .flush()
        .expect("journal flush");
    drop(store); // append 源归零

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_dir(dir);
    println!("[2/2] Store+combine+fx/journal 端到端环：账本 2 行、停机序全过");
}

// Driver 槽位展示（headless 例不接 UI，仅证明五件套之 Driver 在签名面可用）：
// `bind`+`render` 两成员即全部平台概念，headless 宿主不接（Elm worker 形态学）。
#[allow(dead_code)]
struct NoDriver;
impl Driver<Board, BoardAct> for NoDriver {
    fn bind(&mut self, _sender: ActionSender<BoardAct>) {}
    fn render(&mut self, _state: &Board) {}
}

#[tokio::main]
async fn main() {
    test_store_face();
    runtime_face().await;
}
