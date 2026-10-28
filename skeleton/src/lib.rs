//! 最小单向数据流（UDF / Elm 式架构）骨架——**契约层**。
//!
//! 本 crate 只承载「约束后面代码长法的形状」：trait（谁变形 / 谁解释 / 谁存储 /
//! 谁呈现 / 谁持久化 / 什么算一个 store）、数据词汇（Effect/EffectBuf/EffectId/ActionSender/
//! Subscription/NoFx）与语义契约文本（各 trait 文档中的承诺）。零
//! `[dependencies]`——核心无 IO/运行时依赖，reducer「禁 IO」从评审约束升级为
//! 编译期不可能。
//!
//! 基础端口域（契约+参考实现+测试替身同模块，`skeleton_infra` 承载行为件）：
//! [`clock`]（时间源双口分流）、[`config`]（env 覆盖→校验）、[`logpath`]
//! （日志根目录解析）——被多域消费的核心契约住契约层（批 1 契约真下沉）。
//!
//! 可运行的实现（同步排空环、订阅/通知机器、审计事件流、组合器、测试替身）
//! 在姊妹 crate `skeleton_infra`（仓库内 `skeleton-infra/`——0.1.0 未发布，
//! docs.rs 页面随首次发布生效；`skeleton_infra::runtime::SyncStore` 等，按
//! feature 取用）。
//!
//! # 数据链路（只看名字即可复述）
//!
//! | 问 | 依据名 |
//! |---|---|
//! | 数据从哪进 | [`Store::dispatch`]；远处经 [`Store::sender`] 的 [`ActionSender::dispatch`] 回灌 |
//! | 谁变形 | [`Reducer::reduce`]（纯函数：状态进出，效果写 [`EffectBuf`]） |
//! | 谁存储 | [`Store`]（[`Store::snapshot`] 取值语义快照） |
//! | 谁消费 | [`Driver::render`] 下行呈现；[`Subscription`] 订阅票据收变更通知 |
//! | 谁呈现给 UI | [`view::ViewMap`] 投影 + [`view::IntentMap`] 意图翻译（语言无关出站/入站；参考实现 `skeleton_infra::view::ViewBridge`） |
//! | 效果从哪出 | [`Effect::Run`] 描述值 → [`EffectRuntime::run`] 解释，经 [`ActionSender`] 回灌闭环 |
//! | 谁持久化 | [`JournalPort`] 端口（后端自换；参考实现 `skeleton_infra::journal::JsonlJournal`） |
//!
//! # 契约即可实现（toy 最小环）
//!
//! 实现本 crate 的 trait 不需要任何机器——一个十几行的玩具 store 即可演示
//! 契约的可实现性（真实实现见 `skeleton_infra::runtime::SyncStore`）：
//!
//! ```
//! use skeleton::{ActionSender, Store, Subscription};
//! use std::sync::atomic::{AtomicU64, Ordering};
//! use std::sync::Arc;
//!
//! struct Toy {
//!     count: AtomicU64,
//!     sender: ActionSender<u64>,
//! }
//!
//! impl Store for Toy {
//!     type State = u64;
//!     type Action = u64;
//!     type Op = std::convert::Infallible;
//!     fn dispatch(&self, action: u64) {
//!         self.count.fetch_add(action, Ordering::SeqCst);
//!     }
//!     fn snapshot(&self) -> Arc<u64> {
//!         Arc::new(self.count.load(Ordering::SeqCst))
//!     }
//!     fn subscribe(&self, _f: impl FnMut(&u64) + Send + 'static) -> Subscription<u64> {
//!         Subscription::new(|| {})
//!     }
//!     fn sender(&self) -> ActionSender<u64> {
//!         self.sender.clone()
//!     }
//!     fn close(&self) {}
//!     fn is_closed(&self) -> bool {
//!         false
//!     }
//! }
//!
//! let toy = Toy { count: AtomicU64::new(0), sender: ActionSender::new(|_| false) };
//! toy.dispatch(2);
//! toy.dispatch(3);
//! assert_eq!(*toy.snapshot(), 5); // 三分钟读完契约即可写出最小环
//! ```

pub mod clock;
pub mod config;
pub mod logpath;
pub mod parse;
pub mod pool;
pub mod view;

pub use std::convert::Infallible;
use std::sync::Arc;

/// State：值语义只读快照的形状约束。契约：禁内部可变性（无 Mutex/Cell 字段）、
/// 非法状态进类型（互斥态用嵌套 enum，禁 bool 旗子）。
///
/// [`Reducer`] 是一个 Feature 唯一的状态迁移函数：同 `(state, action)` 恒产同
/// 输出（确定性）；IO 在结构上不可触——本 crate 无 IO 依赖。
pub trait Reducer {
    /// `Clone` 语义（值语义快照的前提）；panic-free 要求由实现方按其锁形态
    /// 声明（见实现方文档）。
    type State: Clone + Send;
    /// enum；serde feature 下要求 Serialize + Deserialize（规划中）
    type Action: Send;
    /// 副作用词表（宿主定义的描述式 enum）；零副作用 = [`Infallible`]。
    /// Rust 稳定版无关联类型缺省，零效果 Feature 显式写 `type Op = Infallible;`。
    type Op;
    /// 根状态构造：完整根状态的唯一合法来源（实现方装配时调用一次）。
    /// 状态分片（子 Feature 委托件）没有合法实现——根状态由父 Feature 的
    /// `initial` 显式构造（含子切片初值），分片只承载 reduce 委托。
    ///
    /// `#[track_caller]`：装配期 panic（如分片误用作根）经实现方的 `new`
    /// 转发指采用方装配行，禁指库内 panic 站。
    #[track_caller]
    fn initial(&self) -> Self::State;
    /// 纯函数：禁 IO/时钟/随机/写共享态（结构性：本 crate 无此依赖）。
    /// 效果写入 `out`（写入序 = 转发序），状态经返回值给出（效果出参化）。
    fn reduce(
        &self,
        state: &Self::State,
        action: Self::Action,
        out: &mut EffectBuf<Self::Action, Self::Op>,
    ) -> Self::State;
}

/// 效果句柄：reducer 按语义指派（同义恒同值），解释器寻址用（[`Effect::Cancel`]）。
/// 值语义小句柄（Copy+Eq+Hash）：效果断言、`HashMap`/`HashSet` 键等宿主寻址表
/// 都用值比对。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EffectId(pub u64);

/// Effect：纯描述值。解释器（宿主适配层）执行后经回灌通道把 Action 重入 store（环闭合）。
/// Debug 供测试替身的断言失败消息如实回放实际效果。
#[derive(Debug)]
pub enum Effect<A, Op = Infallible> {
    /// 零副作用回灌：环闭合/表单回写的唯一形态。
    /// 账本语义：Emit 派生动作由 store 核心直接回灌入队（不经解释器，无挂点
    /// 可设），**不入账本**——它是已落盘 action 经纯 reducer 的确定性派生，
    /// 重放经 store 逐条 dispatch 再生、终态收敛（挂点矩阵见实现方
    /// `skeleton_infra::journal` 的 `Journal::append` 文档）。
    Emit(A),
    /// 描述式副作用：`id` 供 [`Effect::Cancel`] 寻址；`op` 由解释器解释
    Run {
        /// 解释器寻址句柄（reducer 按语义指派，同义恒同值）
        id: EffectId,
        /// 副作用载荷：宿主词表项，交 [`EffectRuntime`] 解释执行
        op: Op,
    },
    /// 取消在途效果（ping-pong 显式原语）
    Cancel {
        /// 要取消的在途效果句柄（与 `Run { id }` 写入的同值比对）
        id: EffectId,
    },
}

/// 效果缓冲：reducer 的效果出参位。
///
/// 阅读面口径：reducer 只用 [`EffectBuf::push`]；`new`/`clear`/`is_empty`/
/// `pop_front` 是跨 crate 实现面便利（实现方排空循环构造/复用/逐条转发），
/// 不进十分钟阅读面。
pub struct EffectBuf<A, Op = Infallible> {
    effects: Vec<Effect<A, Op>>,
}

impl<A, Op> EffectBuf<A, Op> {
    /// 空缓冲（实现面便利：排空循环构造一次，此后 clear 复用）。
    pub fn new() -> Self {
        Self {
            effects: Vec::new(),
        }
    }

    /// reducer 写入一条效果；写入顺序 = 解释器转发顺序。
    pub fn push(&mut self, effect: Effect<A, Op>) {
        self.effects.push(effect);
    }

    /// 清空保容量（实现面便利：容量留给下一次 reduce 复用）。
    pub fn clear(&mut self) {
        self.effects.clear();
    }

    /// 缓冲是否为空（实现面便利）。
    pub fn is_empty(&self) -> bool {
        self.effects.is_empty()
    }

    /// 摘取最早一条效果（实现面便利：实现方转发环逐条摘除——panic 时未转发
    /// 效果原地留缓冲；容量语义与 `Vec::remove(0)` 同）。
    pub fn pop_front(&mut self) -> Option<Effect<A, Op>> {
        if self.effects.is_empty() {
            None
        } else {
            Some(self.effects.remove(0))
        }
    }
}

impl<A, Op> Default for EffectBuf<A, Op> {
    fn default() -> Self {
        Self::new()
    }
}

/// 效果运行时：解释器契约——核心只转发，永不解释。同步实现立即
/// `back.dispatch(a)`；异步实现（适配层）spawn 后持句柄回灌。
///
/// `run` 的 panic 如实传播（fail-visible），实现方可自选恢复策略并留痕；
/// panic 后 `self` 的一致性由实现自证（核心不为中断在任意中间态的解释器兜底）。
///
/// # 回灌环责任边界
///
/// `back.dispatch` 与 dispatch 同栈（同步实现）时计入回灌深度预算
/// （见 [`Store`] 契约「回灌有界」）——自指回灌环有界返回。异步实现 spawn 后
/// 从他线程回灌：那不是 dispatch 调用栈的一部分，按外部流量计——跨线程回灌环
/// 的环防护责任在解释器自身，核心无从关联无栈联系的入队。
pub trait EffectRuntime<A, Op> {
    /// 解释执行一条效果。只收 [`Effect::Run`]/[`Effect::Cancel`]（[`Effect::Emit`]
    /// 由 store 直接回灌入队，不经此处）；转发顺序 = reducer 在 [`EffectBuf`]
    /// 的写入顺序。`back` 是回灌句柄：异步实现 spawn 后持它把 Action 重入队列。
    fn run(&mut self, effect: Effect<A, Op>, back: &ActionSender<A>);
}

/// 零效果程序（`Op = Infallible`）的缺省解释器：`Effect<A, Infallible>` 无
/// `Run` 形可构造，本实现至多收到 `Cancel`——语义即「无注册，空转」
/// （[`Effect::Cancel`]），丢弃即正确。消灭手写含静态死分支的空运行时样板。
/// 不提供跨 `Op` 泛化：非零效果词表下静默丢弃 `Run` 是危险的缺省行为。
///
/// 用法见 crate 级文档的 toy 环与 `skeleton-infra` 的 quickstart 示例。
pub struct NoFx;

impl<A> EffectRuntime<A, Infallible> for NoFx {
    fn run(&mut self, _: Effect<A, Infallible>, _: &ActionSender<A>) {}
}

/// 可克隆回灌句柄：解释器异步回灌的唯一通道（[`EffectRuntime::run`] 的 `back`）。
/// 内部持入队闭包（实现方以弱引用接线，不构成引用环）；store 丢弃或已
/// [`Store::close`] 后 [`ActionSender::dispatch`] 返回 `false`——停机窗显式化，
/// 禁静默丢弃（停机闸契约，对照表见 [`Store`] 类型文档）。
pub struct ActionSender<A> {
    push: Arc<dyn Fn(A) -> bool + Send + Sync>,
}

impl<A> Clone for ActionSender<A> {
    fn clone(&self) -> Self {
        Self {
            push: Arc::clone(&self.push),
        }
    }
}

impl<A> ActionSender<A> {
    /// 实现面便利：由 store 实现方在装配时构造（闭包返回 `false` = 已闭门或
    /// 已丢弃）。采用方经 [`Store::sender`] 取句柄，不直接调本构造。
    pub fn new(push: impl Fn(A) -> bool + Send + Sync + 'static) -> Self {
        Self {
            push: Arc::new(push),
        }
    }

    /// 回灌 Action（入队并同步排空至 idle，与 [`Store::dispatch`] 同语义）。
    /// 返回 `false` = store 已丢弃或已 close，action 被显式拒绝（停机静默窗
    /// 收口；闭门拒绝计入实现方的拒绝计数）。
    ///
    /// # Examples
    ///
    /// 停机窗显式化由类型层强制：裸调用（丢弃返回值）会被 `unused_must_use`
    /// lint 拦下——
    ///
    /// ```compile_fail
    /// # #![deny(unused_must_use)]
    /// use skeleton::{ActionSender, Store, Subscription};
    /// use std::sync::atomic::{AtomicBool, Ordering};
    ///
    /// struct Toy {
    ///     closed: AtomicBool,
    ///     sender: ActionSender<u64>,
    /// }
    /// impl Store for Toy {
    ///     type State = u64; type Action = u64; type Op = skeleton::Infallible;
    ///     fn dispatch(&self, _: u64) {}
    ///     fn snapshot(&self) -> std::sync::Arc<u64> { std::sync::Arc::new(0) }
    ///     fn subscribe(&self, _: impl FnMut(&u64) + Send + 'static) -> Subscription<u64> { Subscription::new(|| {}) }
    ///     fn sender(&self) -> ActionSender<u64> { self.sender.clone() }
    ///     fn close(&self) { self.closed.store(true, Ordering::SeqCst) }
    ///     fn is_closed(&self) -> bool { self.closed.load(Ordering::SeqCst) }
    /// }
    /// let toy = Toy { closed: AtomicBool::new(false), sender: ActionSender::new(|_| false) };
    /// toy.close();
    /// toy.sender().dispatch(42); // ERROR: unused `bool` that must be used——闭门拒绝禁静默丢弃
    /// ```
    ///
    /// 接收返回值即合规：
    ///
    /// ```
    /// # use skeleton::{ActionSender, Store, Subscription};
    /// # use std::sync::atomic::{AtomicBool, Ordering};
    /// # struct Toy {
    /// #     closed: AtomicBool,
    /// #     sender: ActionSender<u64>,
    /// # }
    /// # impl Store for Toy {
    /// #     type State = u64; type Action = u64; type Op = skeleton::Infallible;
    /// #     fn dispatch(&self, _: u64) {}
    /// #     fn snapshot(&self) -> std::sync::Arc<u64> { std::sync::Arc::new(0) }
    /// #     fn subscribe(&self, _: impl FnMut(&u64) + Send + 'static) -> Subscription<u64> { Subscription::new(|| {}) }
    /// #     fn sender(&self) -> ActionSender<u64> { self.sender.clone() }
    /// #     fn close(&self) { self.closed.store(true, Ordering::SeqCst) }
    /// #     fn is_closed(&self) -> bool { self.closed.load(Ordering::SeqCst) }
    /// # }
    /// # let toy = Toy { closed: AtomicBool::new(false), sender: ActionSender::new(|_| false) };
    /// # toy.close();
    /// let accepted = toy.sender().dispatch(42);
    /// assert!(!accepted, "闭门后 dispatch 必须显式 false");
    /// ```
    #[must_use = "返回 false = action 被闭门拒绝（store 已 drop 或已 close）；静默丢弃将掩盖停机窗——接收返回值或 `let _ =` 显式弃置"]
    pub fn dispatch(&self, action: A) -> bool {
        (self.push)(action)
    }
}

/// 订阅票据：Drop = 注销。漏注销不可表达——票据被丢弃即立即注销（死订阅），
/// 类型层以 `#[must_use]` 拦截裸调用。注销即时生效：注销后不再收到任何通知，
/// 实现方的订阅计数读侧同步回落。
///
/// 票据对采用方不透明：字段私有，经实现方 [`Store::subscribe`] 取得。
#[must_use = "订阅票据被丢弃即立即注销（Drop=注销）——`store.subscribe(..);` 裸调用 = 静默死订阅；绑定持有才持续收通知"]
pub struct Subscription<S> {
    unsubscribe: Option<Box<dyn FnOnce() + Send + Sync>>,
    _state: std::marker::PhantomData<fn() -> S>,
}

impl<S> Subscription<S> {
    /// 实现面便利：由 store 实现方构造——`unsubscribe` 在票据 Drop 时执行一次
    /// （摘表 + 计数回落等注销语义由实现方定义）。
    pub fn new(unsubscribe: impl FnOnce() + Send + Sync + 'static) -> Self {
        Self {
            unsubscribe: Some(Box::new(unsubscribe)),
            _state: std::marker::PhantomData,
        }
    }
}

impl<S> Drop for Subscription<S> {
    fn drop(&mut self) {
        if let Some(unsubscribe) = self.unsubscribe.take() {
            unsubscribe();
        }
    }
}

/// 下行呈现端口（单向输出）：两职责两成员——上行通道一条（bind）、呈现入口
/// 一条（render），签名里没有平台类型，无第三成员的位置。
///
/// # 宿主主循环接缝
///
/// Driver 是宿主主循环与 store 之间的接缝，**驱动权在宿主**：本 crate 不拥有
/// 循环，宿主在自己的循环里反复「[`Self::render`] 最新状态 → 经 [`Self::bind`]
/// 得到的句柄 [`ActionSender::dispatch`] 输入」（headless 无循环亦无碍，本端口
/// 可不用）。两种典型宿主形态：
///
/// - **帧循环宿主**（如 Unity 每帧 Update step）：一帧一拍——`driver.render(&state);`
///   刷新画面，随后把本帧输入 `sender.dispatch(input)` 上行，下一帧见新状态。
/// - **事件循环宿主**（tokio select 每唤醒 step）：一事件一拍——select 命中
///   时 `driver.render(&store.snapshot())` 刷新呈现，效果回灌由解释器持句柄
///   异步入队。
pub trait Driver<S, A> {
    /// 唯一上行通道
    fn bind(&mut self, sender: ActionSender<A>);
    /// 唯一呈现入口（render(state) 纪律）
    fn render(&mut self, state: &S);
}

/// Store：单向数据流的存储与排空契约——「谁存储 + 数据从哪进」。
///
/// 六方法即全契约（`subscribe` 含 `impl FnMut` 泛型参数 = 非 dyn-safe；宿主
/// 若需 `Box<dyn Store>` 需改参数装箱，出现采用方前不预付）。trait 不设构造：
/// 装配（根 Reducer + 解释器接线）是实现方的 `new`（组合根约定见实现方文档）。
///
/// # 语义契约（实现必须兑现）
///
/// 1. **同步排空**：`dispatch` 返回 = 队列 idle，效果已转发、Emit 回灌已折叠；
///    同序 action 序列恒同终态（确定性）。
/// 2. **快照一致性**：[`Store::snapshot`] 返回值语义 `Arc<State>`；同拍通知
///    共享同一 Arc（订阅者与快照持有者永见自洽状态）。
/// 3. **订阅**：回调在 dispatch 线程同步执行；票据 Drop 即注销；`#[must_use]`
///    拦裸调用。
/// 4. **停机闸**：[`Store::close`] 单向；闭门后 dispatch no-op、
///    `sender().dispatch` 返 false、订阅照常登记但永无通知（已知取舍，见下表）。
/// 5. **fail-visible**：订阅回调 panic 摘除留痕、解释器 panic 计数不报废实例、
///    reducer panic 计数留痕（传播语义不变）、状态锁中毒 fail-fast 实例报废
///    ——四条都有读侧/异常可见面，禁静默吞。
/// 6. **回灌有界**：同栈回灌环有界（预算 64 止步），越界止步且计数留痕、
///    禁静默弃队。
///
/// # 闭门后的入口对照（停机闸）
///
/// | 入口 | 闭门后行为 | fail-visible 面 |
/// |---|---|---|
/// | [`Self::dispatch`] | 恒 no-op（签名不引入 Err） | 计入实现方拒绝计数 |
/// | [`ActionSender::dispatch`] | 返回 `false`（显式拒绝，`#[must_use]` 兜底） | 同一计数 |
/// | [`Self::subscribe`] | 照常登记、票据照常注销，但闭门后永无 dispatch 触发——通知静默（已知取舍） | 计入实现方「闭门登记」计数 |
///
/// dispatch 的静默 no-op 与 sender 的显式 `false` 是**保签名**与**停机显式化**
/// 之间的显式取舍：dispatch 面向动作源（不感染调用面），sender 面向异步回灌
/// （停机窗必须可观测）。闭门 subscribe 照常登记是「票据即注销」模型的自然
/// 推论——通知静默可经实现方的计数读侧交叉查证。
pub trait Store {
    /// 值语义快照的状态（`Clone` 纪律见 [`Reducer::State`]）
    type State: Clone + Send;
    /// action 词表
    type Action: Send;
    /// 效果词表（store 携带其解释器的词表；零效果 = [`Infallible`]）
    type Op;
    /// 数据从哪进（唯一写入口）：同步排空至 idle。
    /// 闭门后恒 no-op（取舍论证见类型文档「闭门后的入口对照」）。
    fn dispatch(&self, action: Self::Action);
    /// 谁存储：拉取式值语义快照（快照一致性承诺见类型文档②）。
    fn snapshot(&self) -> std::sync::Arc<Self::State>;
    /// 谁消费：订阅状态通知；Drop = 注销（契约③）。
    ///
    /// 丢弃票据 = 立即注销（死订阅），类型层拦截（[`Subscription`] 恒
    /// `#[must_use]`）：
    ///
    /// ```compile_fail
    /// # #![deny(unused_must_use)]
    /// use skeleton::{ActionSender, Store, Subscription};
    ///
    /// struct Toy;
    /// impl Store for Toy {
    ///     type State = u64; type Action = u64; type Op = skeleton::Infallible;
    ///     fn dispatch(&self, _: u64) {}
    ///     fn snapshot(&self) -> std::sync::Arc<u64> { std::sync::Arc::new(0) }
    ///     fn subscribe(&self, _: impl FnMut(&u64) + Send + 'static) -> Subscription<u64> { Subscription::new(|| {}) }
    ///     fn sender(&self) -> ActionSender<u64> { ActionSender::new(|_| false) }
    ///     fn close(&self) {}
    ///     fn is_closed(&self) -> bool { false }
    /// }
    /// let toy = Toy;
    /// toy.subscribe(|_| {}); // ERROR: unused `Subscription` that must be used——丢弃即死订阅
    /// ```
    fn subscribe(&self, f: impl FnMut(&Self::State) + Send + 'static) -> Subscription<Self::State>;
    /// 回灌缝出口：可克隆句柄（解释器异步回灌的唯一通道）。
    fn sender(&self) -> ActionSender<Self::Action>;
    /// 停机闸：单向（幂等），置位后排空至 idle（drop 不静默弃队）。
    fn close(&self);
    /// 停机闸状态。
    fn is_closed(&self) -> bool;
}

/// 状态持久化端口（框架留口、后端自换）：快照持久化 + 动作追加 + 恢复的
/// 最小面。真相源「在后端怎么存」完全委托给实现方——JSONL/SQLite/网络皆可，
/// 序列化词表由实现自选，本 crate 不绑格式（零依赖纪律）。参考实现：
/// `skeleton_infra::journal::JsonlJournal`（`journal` feature）。
///
/// 参考实现的行格式约束（细节归实现方）：action/state 须可序列化为
/// **JSON 对象**（行 tag 与载荷平铺同层）——非对象载荷（如 `A = u64` 序列化
/// 为裸数字）首条 [`Self::append`] 即 `Err`；先落盘后 apply ⇒ 零副作用，
/// 换对象形状重试即可。
///
/// # 语义契约（实现必须兑现）
///
/// 1. **先落盘后 apply**：[`Self::append`] 返回 `Err` ⇒ 调用方禁 apply 该
///    action——内存态永不领先已持久前缀。
/// 2. **恢复完备**：[`Self::restore`] 返回三态 [`Restore`]，调用方按其语义
///    重建当前态——`Empty` 取 [`Reducer::initial`] 零 apply；`FromInitial`
///    自 initial 起按序 apply 全量动作；`FromSnapshot` 自快照态起按序
///    apply 增量动作。重放协议：动作流**经同一装配的 store 逐条
///    [`Store::dispatch`]**（效果环照常运转）——[`Effect::Emit`] 回灌等派生
///    动作由排空环再生，终态与 live 收敛；裸 reduce 逐条套用会丢派生链
///    （挂点矩阵与收敛回归见实现方 `skeleton_infra::journal`）。
/// 3. **fail-visible**：持久化失败如实 `Err`，禁静默丢弃——静默丢 action =
///    真相源丢史。
/// 4. **单写者**：本端口不设跨进程锁；多进程挂载同一账本的编排归上层。
///
/// # 与实现层 `Journal` trait 的分工
///
/// 实现层全功能面 trait `skeleton_infra::journal::Journal` 的同名方法已改名
/// （`append_action`/`restore_stream`/`append_snapshot`；`replay` 为实现层
/// 独有），本端口独占契约词表（`append`/`restore`/`persist_snapshot`）——两
/// trait 同屏 import 直呼无歧义（旧三同名双候选拒编译已结构性消灭，批 2A）。
/// 状态面同款关联类型化（批 6A，与实现层同步）：[`JournalPort::State`] 被
/// `Self` 钉死，端口方法直呼零 turbofish 零绑定标注（`j.append(&a)` /
/// `let r = j.restore()`）。
///
/// # Examples
///
/// 后端自换即实现本 trait——一个内存账本（无任何序列化依赖）同样合法：
///
/// ```
/// use skeleton::{JournalPort, Restore};
///
/// struct MemJournal {
///     applied: Vec<String>,
/// }
///
/// impl JournalPort<String> for MemJournal {
///     type State = u64;
///     type Err = std::convert::Infallible;
///     fn append(&mut self, action: &String) -> Result<(), Self::Err> {
///         self.applied.push(action.clone());
///         Ok(())
///     }
///     fn persist_snapshot(&mut self, _state: &u64) -> Result<(), Self::Err> {
///         Ok(()) // 快照可选：真相源恒为动作流
///     }
///     fn restore(&self) -> Result<Restore<String, u64>, Self::Err> {
///         Ok(Restore::FromInitial(self.applied.clone())) // 无快照：自 initial 全量播
///     }
/// }
///
/// let mut j = MemJournal { applied: Vec::new() };
/// j.append(&"inc".to_string()).unwrap(); // 直呼：写面零标注
/// assert_eq!(j.restore().unwrap(), Restore::FromInitial(vec!["inc".to_string()]));
/// ```
pub trait JournalPort<A> {
    /// 持久化失败类型（实现方定义；`Err` ⇒ 契约①「先落盘后 apply」生效）。
    type Err;
    /// 账本状态面（快照与恢复语义的载荷类型；实现方定义）。
    type State;
    /// 动作追加（真相源的增量单位）。
    ///
    /// payload 契约：`A` 序列化结果**必须是 JSON object**（实现层按
    /// `#[serde(tag)]` 事件行校验）——unit variant 序列化为裸字符串，
    /// 运行时 `Err`（fail-visible）；给 enum 挂 `#[serde(tag = "t")]` 即免。
    fn append(&mut self, action: &A) -> Result<(), Self::Err>;
    /// 快照持久化（加速恢复的可选位；真相源恒为动作流）。动名
    /// `persist_snapshot` 与 [`Store::snapshot`] 的读侧快照跨层同名反向消歧
    /// （接口维 R1：账本侧=持久化写入，store 侧=拉取读取）。
    fn persist_snapshot(&mut self, state: &Self::State) -> Result<(), Self::Err>;
    /// 恢复：三态 [`Restore`]，调用方按其语义重建当前态。参考落点（实现层）：
    /// `skeleton_infra::runtime::SyncStore::restore_from` 直收本返回值播种
    /// 新 store——`FromSnapshot` 的第一类落点（接口维 R1）。
    fn restore(&self) -> Result<Restore<A, Self::State>, Self::Err>;
}

/// 恢复点三态（互斥态进类型，无双 `Option` 同义形）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Restore<A, S> {
    /// 空账本：取 [`Reducer::initial`]，零 apply。
    Empty,
    /// 无快照行：自 [`Reducer::initial`] 起按序 apply 全量动作。
    FromInitial(Vec<A>),
    /// 有快照：自快照态起按序 apply 增量动作。
    FromSnapshot(S, Vec<A>),
}

#[cfg(test)]
mod tests {
    use super::*;

    enum Tick {
        Tick,
    }

    /// 效果缓冲 clear 保容量——实现方「跨 dispatch 复用常驻缓冲」承诺的数据
    /// 结构前提（容量语义是本 crate 自有类型的属性，契约面单元测试）。
    #[test]
    fn effect_buf_clear_preserves_capacity() {
        let mut buf: EffectBuf<Tick, Infallible> = EffectBuf::new();
        for _ in 0..8 {
            buf.push(Effect::Emit(Tick::Tick));
        }
        assert!(!buf.is_empty());
        let cap = buf.effects.capacity();
        assert!(cap >= 8);
        buf.clear();
        assert!(buf.is_empty());
        assert_eq!(buf.effects.capacity(), cap, "clear 必须保容量");
    }
}
