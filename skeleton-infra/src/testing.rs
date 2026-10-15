//! 脚本化解释器测试替身：逻辑层零 IO 全测 + panic/挂起面注入。
//!
//! feature 门 `testing`（连带启用 `tokio`/`fx` 底座）。绑核心
//! `EffectRuntime`/`ActionSender`/`Effect`（单向依赖 infra→核心）。
//!
//! 实现注记（其余签名/行为与核心契约对齐）：
//! ① `skeleton` 依赖走 `testing` feature 门（`testing = ["tokio", "dep:skeleton"]`）
//!    而非全局依赖——守「零 feature = 零依赖」
//!    cargo tree 单行证明（同 fx.rs 先例：引用核心类型的域才引核心依赖）。
//! ② 测试构建（cfg(test)）私有留存 `Spawned` 句柄：「scope join=
//!    Panicked(msg)」测试要求可 join 的任务句柄，而 `run()` 不外借句柄——
//!    采用方构建不含此字段，struct 公开面不变。
//! ③ scope 关闸时 spawn 失败 → `expect` fail-fast（crate 纪律
//!    「禁静默丢弃 future」，测试替身里 fail-fast 即最清晰的失败信号）。

use std::collections::VecDeque;
use std::sync::Arc;

use skeleton::{ActionSender, Effect, EffectRuntime};

use crate::fx::FxScope;
#[cfg(test)]
use crate::fx::Spawned;

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

/// 脚本化效果运行时：`Store` 的解释器替身——本件只做解释器替身，核心 TestStore
/// 本体归核心 crate。
///
/// # 标准形模板（逻辑层零 IO 全测）
///
/// reducer 出描述式效果 → `ScriptedRuntime` 按脚本解释 → `Emit` 经回灌通道闭环保环：
///
/// ```
/// use std::sync::Arc;
///
/// use skeleton::{Effect, EffectBuf, EffectId, Reducer, Store};
/// use skeleton_infra::fx::FxScope;
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
///                 out.push(Effect::Run { id: EffectId(0), op: () });
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
///     let store = Store::new(Counter, scripted);
///
///     store.dispatch(Act::Start); // reducer 出 Run 效果 → 脚本解释器接管
///     while scope.in_flight() > 0 {
///         tokio::task::yield_now().await;
///     }
///     assert_eq!(*store.snapshot(), 1, "脚本 Emit 经回灌推进 store");
///     scope.shutdown().await; // 收尾净空（泄漏断言锚见 fx::expect_idle）
/// });
/// # }
/// ```
///
/// 本件不实现 clock——取消竞窗测试用 `tokio::time::start_paused`（三断言形：
/// 取消后零副作用 / 显式 Cancelled / journal 显式行，模板归 CONCURRENCY.md）。
pub struct ScriptedRuntime<A, Op> {
    scope: Arc<FxScope>,
    scripts: VecDeque<Script<A, Op>>,
    /// 测试观测面（仅 cfg(test) 构建，见头注②）：run() 产出句柄供单测 join/abort。
    #[cfg(test)]
    handles: Vec<Spawned<()>>,
}

impl<A, Op> ScriptedRuntime<A, Op> {
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

    pub fn scope(&self) -> &Arc<FxScope> {
        &self.scope
    }
}

impl<A, Op> EffectRuntime<A, Op> for ScriptedRuntime<A, Op>
where
    A: Send + 'static,
    Op: Send + 'static,
{
    /// 每次效果转发出队一脚本，经 scope.spawn 执行（对齐点：回灌调用=核心
    /// ActionSender 公开方法，实名 `send`；Panic/Never 在任务内展开）。
    fn run(&mut self, _effect: Effect<A, Op>, back: &ActionSender<A>) {
        let Some(script) = self.scripts.pop_front() else {
            // 脚本耗尽=no-op——TestStore.receive 超时即自然失败信号
            return;
        };
        let back = back.clone();
        let spawned = self
            .scope
            .spawn(async move {
                match script {
                    Script::Emit(a) => {
                        // 回灌即环闭合；false=store 已关（核心显式拒绝语义，采用方断言面）。
                        // 测试替身在停机语义上禁吞投递结果——闭门下 Emit
                        // 被静默吞会让 fail-closed 链的测试照绿，升格为显式断言。
                        assert!(back.send(a), "scripted Emit rejected: store closed");
                    }
                    // 任务内 panic!→scope catch_unwind 归一 TaskError::Panicked（fail-closed 链）
                    Script::Panic(msg) => panic!("{}", msg),
                    // 永不完成：挂起面/取消竞窗锚（start_paused 模板归 CONCURRENCY.md）
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fx::{expect_idle, TaskError};
    use skeleton::{EffectBuf, EffectId, Reducer, Store};
    use std::convert::Infallible;

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
    /// （ActionSender 无公开构造，Store 为唯一合法来源）。
    struct NopRuntime;

    impl EffectRuntime<Act, Infallible> for NopRuntime {
        fn run(&mut self, _: Effect<Act, Infallible>, _: &ActionSender<Act>) {}
    }

    fn fixture() -> (Arc<FxScope>, Store<i32, Act, Infallible>, ActionSender<Act>) {
        let scope = Arc::new(FxScope::new());
        let store = Store::new(Counter, NopRuntime);
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
        panic!("fx scope not idle: {} task(s) still in flight", scope.in_flight());
    }

    /// ① Emit 脚本→经 ActionSender 回灌收到（store 状态推进为证）。
    #[tokio::test]
    async fn emit_script_feeds_action_back() {
        let (scope, store, back) = fixture();
        let mut rt = ScriptedRuntime::<Act, Infallible>::new(Arc::clone(&scope));
        rt.feed([Script::Emit(Act::Bump)]);
        rt.run(Effect::Cancel { id: EffectId(0) }, &back);
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
        rt.run(Effect::Cancel { id: EffectId(1) }, &back);
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
        rt.run(Effect::Cancel { id: EffectId(2) }, &back);
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
        rt.run(Effect::Cancel { id: EffectId(3) }, &back);
        assert_eq!(scope.in_flight(), 0);
        assert!(rt.handles.is_empty());
        expect_idle(&scope);
    }

    /// ⑤ FIFO 消费序：句柄序=脚本序（Emit→Never→Panic 逐段核验，乱序即错位）。
    #[tokio::test]
    async fn scripts_consume_fifo() {
        let (scope, store, back) = fixture();
        let mut rt = ScriptedRuntime::<Act, Infallible>::new(Arc::clone(&scope));
        rt.feed([Script::Emit(Act::Bump), Script::Never, Script::Panic("third")]);
        rt.run(Effect::Cancel { id: EffectId(4) }, &back);
        rt.run(Effect::Cancel { id: EffectId(5) }, &back);
        rt.run(Effect::Cancel { id: EffectId(6) }, &back);
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
        rt.run(Effect::Cancel { id: EffectId(1) }, &back);
        rt.run(Effect::Cancel { id: EffectId(2) }, &back);
        let mut handles = std::mem::take(&mut rt.handles);
        assert_eq!(handles.len(), 2, "两条 Run 脚本各产一个任务");
        assert_eq!(handles.remove(0).join().await, Ok(()));
        assert_eq!(handles.remove(0).join().await, Ok(()));
        assert_eq!(*store.snapshot(), 0, "透传零回灌：store 不动");
        wait_idle(&fxscope_scope).await;
        expect_idle(&fxscope_scope);
    }

}
