//! 测试替身域：脚本化解释器 + TestStore 验收面——逻辑层零 IO 全测 + panic/挂起面注入。
//!
//! feature 门 `testing`（连带启用 `runtime`/`tokio` 底座）。绑契约 trait
//! `EffectRuntime`/`ActionSender`/`Effect` 与实现 `SyncStore`（单向依赖 infra 内
//! runtime→skeleton 契约）。
//!
//! 实现注记（其余签名/行为与契约对齐）：
//! ① `skeleton` 依赖走 `runtime` feature 门（`runtime = ["dep:skeleton"]`），
//!    `testing` 连带启用——守「零三方依赖」cargo tree 证明
//!    （同 fx.rs 先例：引用核心类型的域才引核心依赖）。
//! ② 测试构建（cfg(test)）私有留存 `Spawned` 句柄：「scope join=
//!    Panicked(msg)」测试要求可 join 的任务句柄，而 `run()` 不外借句柄——
//!    采用方构建不含此字段，struct 公开面不变。
//! ③ scope 关闸时 spawn 失败 → `expect` fail-fast（crate 纪律
//!    「禁静默丢弃 future」，测试替身里 fail-fast 即最清晰的失败信号）。

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use skeleton::{ActionSender, Effect, EffectId, EffectRuntime, Infallible, Reducer, Store};

use crate::fx::FxScope;
#[cfg(test)]
use crate::fx::Spawned;
use crate::runtime::{lock, SyncStore};

/// 脚本步：`run` 每次出队一步（FIFO），行为完全由脚本驱动、与到达的 Effect 值无关。
pub enum Script<A, Op> {
    /// 回灌一个 Action（经 ActionSender）。
    Emit(A),
    /// 任务内 panic!（scope 捕获→TaskError::Panicked——验证 fail-closed 链）。
    Panic(&'static str),
    /// 永不完成（std::future::pending——挂起面/取消竞窗用）。
    Never,
    /// 占位透传：Op 语义由采用方在任务内断言（不改行为）。
    Run(Op),
}

/// 脚本化效果运行时：`SyncStore` 的解释器替身（TestStore 走自带的
/// `CollectRuntime` 回收面，见本文件下节）。
///
/// # 标准形模板（逻辑层零 IO 全测）
///
/// reducer 出描述式效果 → `ScriptedRuntime` 按脚本解释 → `Emit` 经回灌通道闭环保环：
///
/// ```
/// use std::sync::Arc;
///
/// use skeleton::{Effect, EffectBuf, EffectId, Reducer, Store as _};
/// use skeleton_infra::fx::FxScope;
/// use skeleton_infra::runtime::SyncStore;
/// use skeleton_infra::testing::{Script, ScriptedRuntime};
///
/// enum Act {
///     Start,
///     Bump,
/// }
///
/// struct Counter;
///
/// impl Reducer for Counter {
///     type State = i32;
///     type Action = Act;
///     type Op = ();
///     fn initial(&self) -> i32 {
///         0
///     }
///     fn reduce(&self, s: &i32, a: Act, out: &mut EffectBuf<Act, ()>) -> i32 {
///         match a {
///             Act::Start => {
///                 out.push(Effect::Run { id: EffectId::new(0), op: () });
///                 *s
///             }
///             Act::Bump => s + 1,
///         }
///     }
/// }
///
/// # fn main() {
/// let rt = tokio::runtime::Builder::new_current_thread()
///     .enable_time()
///     .build()
///     .unwrap();
/// rt.block_on(async {
///     let scope = Arc::new(FxScope::new());
///     let mut scripted = ScriptedRuntime::<Act, ()>::new(Arc::clone(&scope));
///     scripted.feed([Script::Emit(Act::Bump)]);
///     let store = SyncStore::new(Counter, scripted);
///
///     store.dispatch(Act::Start); // reducer 出 Run 效果 → 脚本解释器接管
///     while scope.in_flight() > 0 {
///         tokio::task::yield_now().await;
///     }
///     assert_eq!(*store.snapshot(), 1, "脚本 Emit 经回灌推进 store");
///     let _ = scope.shutdown().await; // 收尾净空（泄漏断言锚见 fx::expect_idle）
/// });
/// # }
/// ```
///
/// 本件不实现 clock——取消竞窗测试用 `tokio::time::start_paused`。竞窗三断言形
/// （就地模板）：①取消后零副作用（效果体未产生任何可观测变更）；②显式
/// `Cancelled`（join 归一 TaskError::Cancelled，非静默丢弃）；③journal 显式行
/// （取消进审计/账本时间线，可事后归因）。
pub struct ScriptedRuntime<A, Op> {
    scope: Arc<FxScope>,
    scripts: VecDeque<Script<A, Op>>,
    /// 测试观测面（仅 cfg(test) 构建，见头注②）：run() 产出句柄供单测 join/abort。
    #[cfg(test)]
    handles: Vec<Spawned<()>>,
}

impl<A, Op> ScriptedRuntime<A, Op> {
    /// 绑定解释作用域（效果任务在此 spawn；停机面归 `FxScope`）。
    pub fn new(scope: Arc<FxScope>) -> Self {
        Self {
            scope,
            scripts: VecDeque::new(),
            #[cfg(test)]
            handles: Vec::new(),
        }
    }

    /// 追加脚本序列（FIFO 消费）。
    pub fn feed(&mut self, scripts: impl IntoIterator<Item = Script<A, Op>>) {
        self.scripts.extend(scripts);
    }

    /// 作用域读侧（测试收尾 expect_idle 断言用）。
    pub fn scope(&self) -> &Arc<FxScope> {
        &self.scope
    }
}

impl<A, Op> EffectRuntime<A, Op> for ScriptedRuntime<A, Op>
where
    A: Send + 'static,
    Op: Send + 'static,
{
    /// 每次效果转发出队一脚本，经 scope.spawn_tracked 执行（对齐点：回灌调用=
    /// `ActionSender::dispatch` 公开方法；Panic/Never 在任务内展开）。
    fn run(&mut self, _effect: Effect<A, Op>, back: &ActionSender<A>) {
        let Some(script) = self.scripts.pop_front() else {
            // 脚本耗尽=no-op——TestStore.receive 超时即自然失败信号
            return;
        };
        let back = back.clone();
        let spawned = self
            .scope
            .spawn_tracked(async move {
                match script {
                    Script::Emit(a) => {
                        // 回灌即环闭合；false=store 已关（契约显式拒绝语义，采用方断言面）。
                        // 测试替身在停机语义上禁吞投递结果——闭门下 Emit
                        // 被静默吞会让 fail-closed 链的测试照绿，升格为显式断言。
                        assert!(back.dispatch(a), "scripted Emit rejected: store closed");
                    }
                    // 任务内 panic!→scope catch_unwind 归一 TaskError::Panicked（fail-closed 链）
                    Script::Panic(msg) => panic!("{}", msg),
                    // 永不完成：挂起面/取消竞窗锚（start_paused 测试模板见本类型文档）
                    Script::Never => std::future::pending::<()>().await,
                    // 占位透传：任务内持 op，零行为（采用方断言锚）
                    Script::Run(op) => {
                        let _op = op;
                    }
                }
            })
            .expect("scripted runtime: fx scope closed while expanding script");
        #[cfg(test)]
        self.handles.push(spawned);
        #[cfg(not(test))]
        drop(spawned);
    }
}

// ── TestStore 验收面：脚本化效果回收 + 逐动作断言 + 效果回填断言 ─────────────

/// 测试面：脚本化效果 + 逐动作断言 + 效果回填断言。
///
/// 验收纪律四条：逐动作断言 /
/// 效果断言 / 非法迁移负例 / tests 先行。
pub struct TestStore<S, A, Op = Infallible> {
    store: SyncStore<S, A, Op>,
    /// 脚本化解释器的效果回收队列（FIFO）：[`TestStore::receive`] 逐条弹出比对
    effects: Arc<Mutex<VecDeque<Effect<A, Op>>>>,
    /// 最近一次 dispatch 后的 idle 快照：[`TestStore::state`] 直接借用免锁
    current: Arc<S>,
    /// 最近一次 dispatch 期间的回灌 Emit 计数：[`TestStore::receive`] 空队列
    /// 时区分「真没产效果」与「产了但走了回灌通道」。
    last_dispatch_feedback: u64,
}

/// [`TestStore`] 专用脚本化解释器：效果原样回收、永不解释——宿主逻辑层
/// 无 IO 全测（通用性基准 L1）。与 fx 侧 [`ScriptedRuntime`] 分工：本件只
/// 回收，不解释不 spawn。
struct CollectRuntime<A, Op> {
    sink: Arc<Mutex<VecDeque<Effect<A, Op>>>>,
}

impl<A, Op> EffectRuntime<A, Op> for CollectRuntime<A, Op> {
    fn run(&mut self, effect: Effect<A, Op>, _back: &ActionSender<A>) {
        lock(&self.sink).push_back(effect);
    }
}

/// 断言用效果期望值（[`TestStore::receive`] 入参）。`id: None` = 不校验寻址
/// id；`Some(id)` = 必须同值（[`EffectId`] 同义恒同值）。`Emit` 不在此列：
/// Emit 回灌由 store 核心以 Action 形态重入队列，从不进入效果转发路径——
/// 回环语义用 `dispatch` + [`TestStore::state`] 断言。
#[derive(Debug, PartialEq)]
pub enum ExpectedEffect<Op = Infallible> {
    /// 断言下一条效果为 [`Effect::Run`]
    Run {
        /// `None` = 不校验寻址 id；`Some(id)` = 必须同值（[`EffectId`] 同义恒同值）
        id: Option<EffectId>,
        /// 副作用载荷逐字比对（`PartialEq`）
        op: Op,
    },
    /// 断言下一条效果为 [`Effect::Cancel`]
    Cancel {
        /// `None` = 不校验寻址 id；`Some(id)` = 必须同值
        id: Option<EffectId>,
    },
}

/// `None` = 不校验 id；`Some` = 必须同值。
fn id_matches(expected: Option<EffectId>, actual: EffectId) -> bool {
    match expected {
        Some(id) => id == actual,
        None => true,
    }
}

impl<S: Clone + Send + Sync + 'static, A: Send + 'static, Op: Send + 'static> TestStore<S, A, Op> {
    /// 以脚本化解释器装配 store：效果不解释、原样回收，等 `receive` 逐条断言。
    pub fn new(root: impl Reducer<State = S, Action = A, Op = Op> + Send + Sync + 'static) -> Self {
        let effects = Arc::new(Mutex::new(VecDeque::new()));
        let store = SyncStore::new(
            root,
            CollectRuntime {
                sink: Arc::clone(&effects),
            },
        );
        let current = store.snapshot();
        Self {
            store,
            effects,
            current,
            last_dispatch_feedback: 0,
        }
    }

    /// 逐动作断言入口：dispatch 并刷新 idle 快照（同步排空至 idle 后取，
    /// Emit 回灌链已折叠完毕）。
    pub fn dispatch(&mut self, action: A) {
        let feedback_before = self.store.feedback_emitted_total();
        self.store.dispatch(action);
        self.current = self.store.snapshot();
        // 本轮回灌 Emit 快照——Emit 不进效果队列，receive 空队列时据此
        // 如实归因（禁再报「no effect produced」文不对题）。
        self.last_dispatch_feedback = self.store.feedback_emitted_total() - feedback_before;
    }

    /// 效果回填断言：弹出最早一条未消费效果与期望值比对。队列空或值不符 =
    /// panic（测试失败即验收失败），fail-closed 禁静默吞。
    /// `#[track_caller]`：panic 位置指采用方调用点而非库内 assert 站。
    #[track_caller]
    pub fn receive(&mut self, expected: ExpectedEffect<Op>)
    where
        A: std::fmt::Debug,
        Op: std::fmt::Debug + PartialEq,
    {
        let actual = lock(&self.effects).pop_front();
        let Some(actual) = actual else {
            // Emit 永不进 receive 队列——本轮
            // 产过回灌 Emit 时如实归因并指正解（回环语义 dispatch + state 断言）。
            if self.last_dispatch_feedback > 0 {
                panic!(
                    "receive: no effect for consumer ({} emitted action(s) were re-enqueued as feedback; Emit never enters the receive queue — assert the round-trip via dispatch + state())",
                    self.last_dispatch_feedback
                );
            }
            panic!("receive: no effect produced (queue empty); expected {expected:?}");
        };
        let matched = match (&expected, &actual) {
            (
                ExpectedEffect::Run { id, op },
                Effect::Run {
                    id: actual_id,
                    op: actual_op,
                },
            ) => op == actual_op && id_matches(*id, *actual_id),
            (ExpectedEffect::Cancel { id }, Effect::Cancel { id: actual_id }) => {
                id_matches(*id, *actual_id)
            }
            _ => false,
        };
        assert!(
            matched,
            "receive: effect mismatch\n  expected: {expected:?}\n  actual: {actual:?}"
        );
    }

    /// 逐动作断言读侧：最近一次 `dispatch` 后的 idle 状态。
    pub fn state(&self) -> &S {
        &self.current
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fx::{expect_idle, TaskError};
    use skeleton::{EffectBuf, EffectId, Reducer};
    use std::convert::Infallible;

    use crate::runtime::SyncStore;

    enum Act {
        Bump,
    }

    struct Counter;

    impl Reducer for Counter {
        type State = i32;
        type Action = Act;
        type Op = Infallible;
        fn initial(&self) -> i32 {
            0
        }
        fn reduce(&self, s: &i32, _: Act, _out: &mut EffectBuf<Act, Infallible>) -> i32 {
            s + 1
        }
    }

    /// store 自带 no-op 运行时（回灌测试不走它），经 `sender()` 取真回灌句柄
    /// （走实现方装配接线，与采用方同一来源）。
    struct NopRuntime;

    impl EffectRuntime<Act, Infallible> for NopRuntime {
        fn run(&mut self, _: Effect<Act, Infallible>, _: &ActionSender<Act>) {}
    }

    fn fixture() -> (
        Arc<FxScope>,
        SyncStore<i32, Act, Infallible>,
        ActionSender<Act>,
    ) {
        let scope = Arc::new(FxScope::new());
        let store = SyncStore::new(Counter, NopRuntime);
        let back = store.sender();
        (scope, store, back)
    }

    /// abort 的取消 poll 异步生效：让渡至计数归零（fx.rs 同款有界让渡，防挂死）。
    async fn wait_idle(scope: &FxScope) {
        for _ in 0..10_000 {
            if scope.in_flight() == 0 {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!(
            "fx scope not idle: {} task(s) still in flight",
            scope.in_flight()
        );
    }

    /// ① Emit 脚本→经 ActionSender 回灌收到（store 状态推进为证）。
    #[tokio::test]
    async fn emit_script_feeds_action_back() {
        let (scope, store, back) = fixture();
        let mut rt = ScriptedRuntime::<Act, Infallible>::new(Arc::clone(&scope));
        rt.feed([Script::Emit(Act::Bump)]);
        rt.run(
            Effect::Cancel {
                id: EffectId::new(0),
            },
            &back,
        );
        let handle = rt.handles.pop().expect("Emit 脚本必须产出任务");
        assert_eq!(handle.join().await, Ok(()));
        assert_eq!(*store.snapshot(), 1, "回灌 Action 必须推进 store");
        expect_idle(&scope);
    }

    /// ② Panic→任务内 panic!，scope 捕获归一 TaskError::Panicked(msg)（fail-closed 链）。
    #[tokio::test]
    async fn panic_script_yields_panicked() {
        let (scope, _store, back) = fixture();
        let mut rt = ScriptedRuntime::<Act, Infallible>::new(Arc::clone(&scope));
        rt.feed([Script::Panic("scripted-boom")]);
        rt.run(
            Effect::Cancel {
                id: EffectId::new(1),
            },
            &back,
        );
        let handle = rt.handles.pop().unwrap();
        assert_eq!(
            handle.join().await,
            Err(TaskError::Panicked("scripted-boom".to_owned()))
        );
        wait_idle(&scope).await;
        expect_idle(&scope);
    }

    /// ③ Never→任务挂起（in_flight=1），abort 后归零（挂起面/取消竞窗锚）。
    #[tokio::test]
    async fn never_script_holds_until_abort() {
        let (scope, _store, back) = fixture();
        let mut rt = ScriptedRuntime::<Act, Infallible>::new(Arc::clone(&scope));
        rt.feed([Script::Never]);
        rt.run(
            Effect::Cancel {
                id: EffectId::new(2),
            },
            &back,
        );
        assert_eq!(scope.in_flight(), 1);
        let handle = rt.handles.pop().unwrap();
        handle.abort();
        assert_eq!(handle.join().await, Err(TaskError::Cancelled));
        wait_idle(&scope).await;
        expect_idle(&scope);
    }

    /// ④ 脚本耗尽=no-op：不 spawn 不 panic（TestStore.receive 超时即自然失败信号）。
    #[test]
    fn exhausted_scripts_are_noop() {
        let (scope, _store, back) = fixture();
        let mut rt = ScriptedRuntime::<Act, Infallible>::new(Arc::clone(&scope));
        rt.run(
            Effect::Cancel {
                id: EffectId::new(3),
            },
            &back,
        );
        assert_eq!(scope.in_flight(), 0);
        assert!(rt.handles.is_empty());
        expect_idle(&scope);
    }

    /// ⑤ FIFO 消费序：句柄序=脚本序（Emit→Never→Panic 逐段核验，乱序即错位）。
    #[tokio::test]
    async fn scripts_consume_fifo() {
        let (scope, store, back) = fixture();
        let mut rt = ScriptedRuntime::<Act, Infallible>::new(Arc::clone(&scope));
        rt.feed([
            Script::Emit(Act::Bump),
            Script::Never,
            Script::Panic("third"),
        ]);
        rt.run(
            Effect::Cancel {
                id: EffectId::new(4),
            },
            &back,
        );
        rt.run(
            Effect::Cancel {
                id: EffectId::new(5),
            },
            &back,
        );
        rt.run(
            Effect::Cancel {
                id: EffectId::new(6),
            },
            &back,
        );
        let mut handles = std::mem::take(&mut rt.handles);
        assert_eq!(handles.len(), 3);
        assert_eq!(handles.remove(0).join().await, Ok(())); // 首脚本=Emit：回灌落地
        assert_eq!(*store.snapshot(), 1);
        let never = handles.remove(0);
        never.abort(); // 次脚本=Never：挂起可取消
        assert_eq!(never.join().await, Err(TaskError::Cancelled));
        assert_eq!(
            handles.remove(0).join().await,
            Err(TaskError::Panicked("third".to_owned()))
        ); // 末脚本=Panic
        wait_idle(&scope).await;
        expect_idle(&scope);
    }

    /// 零覆盖 #12：Run(op) 占位透传——任务携 op 正常完成（join=Ok(())），零行为
    /// 零回灌（store 不动）；op 语义由采用方在任务内断言（文档契约的可达性面）。
    /// 变异检出：Run 臂若误回灌/误 panic/漏 spawn，此处立即红。
    #[tokio::test]
    async fn run_script_carries_op_through_without_side_effects() {
        let (fxscope_scope, store, back) = fixture();
        let mut rt = ScriptedRuntime::<Act, u8>::new(Arc::clone(&fxscope_scope));
        rt.feed([Script::Run(7u8), Script::Run(9u8)]);
        rt.run(
            Effect::Cancel {
                id: EffectId::new(1),
            },
            &back,
        );
        rt.run(
            Effect::Cancel {
                id: EffectId::new(2),
            },
            &back,
        );
        let mut handles = std::mem::take(&mut rt.handles);
        assert_eq!(handles.len(), 2, "两条 Run 脚本各产一个任务");
        assert_eq!(handles.remove(0).join().await, Ok(()));
        assert_eq!(handles.remove(0).join().await, Ok(()));
        assert_eq!(*store.snapshot(), 0, "透传零回灌：store 不动");
        wait_idle(&fxscope_scope).await;
        expect_idle(&fxscope_scope);
    }
}
