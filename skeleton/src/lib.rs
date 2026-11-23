//! 最小单向数据流（UDF / Elm 式架构）骨架——**契约层**。
//!
//! 本 crate 只承载「约束后面代码长法的形状」：trait（谁变形 / 谁解释 / 谁存储 /
//! 谁呈现 / 谁持久化 / 谁发异步变更信号 / 什么算一个 store）、数据词汇
//! （Effect/EffectBuf/EffectId/ActionSender/Subscription/NoFx）与语义契约文本
//! （各 trait 文档中的承诺）。零
//! `[dependencies]`——核心无 IO/运行时依赖，reducer「禁 IO」从评审约束升级为
//! 编译期不可能。
//!
//! 基础端口域（契约+参考实现+测试替身同模块，`skeleton_infra` 承载行为件）：
//! [`clock`]（时间源双口分流）、[`config`]（env 覆盖→校验）、[`logpath`]
//! （日志根目录解析）——被多域消费的核心契约住契约层（批 1 契约真下沉）。
//!
//! 启动期编排件：[`lint`]（配置 lint 阶段表——注册期登记，`post_process` 快照后
//! 锁外按序执行、稳态零堆分配；纯 std 零依赖，采用方不必为配置校验把运行时拖进树）。
//!
//! 可观测值面：[`fields`]（一组可观测字段（名+值）的纯值形状——插入序稳定、
//! 同名覆盖、克隆 O(1)；只做值面，无导出/上报）。
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
//! | 谁异步收变更信号 | [`notify::NotifySink`] 投递口收单调版本号水位（[`notify::Notify`] 登记面；见版本拉取现态，合并合法、丢拍可检出） |
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
pub mod fields;
pub mod lint;
pub mod logpath;
pub mod notify;
pub mod parse;
pub mod pool;
pub mod view;

use std::collections::VecDeque;
pub use std::convert::Infallible;
use std::sync::Arc;

/// State：值语义只读快照的形状约束。契约：禁内部可变性（无 Mutex/Cell 字段）、
/// 非法状态进类型（互斥态用嵌套 enum，禁 bool 旗子）。
///
/// **根契约**：[`Reducer`] 是一个 Feature 的唯一状态迁移函数——`initial` 给出
/// 根状态、`reduce` 同 `(state, action)` 恒产同输出（确定性）；IO 在结构上
/// 不可触——本 crate 无 IO 依赖。子件（`combine` 产物）只需 [`Fragment`]：
/// 每个 `Reducer` 都是合法子件（blanket impl），反之不成立——分片无根状态
/// 工厂，作根 = E0277 编译期拒绝。
#[diagnostic::on_unimplemented(
    message = "`{Self}` 不是可挂载的根：根契约 `Reducer` 未实现",
    note = "combine!/combine 产物是状态分片（`Fragment`）：没有 `initial`，不能作根；根状态由父 Feature 的 `initial` 显式构造（含子切片初值）"
)]
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
    /// 状态分片没有本方法（[`Fragment`] 是 reduce-only 契约）——根状态由父
    /// Feature 的 `initial` 显式构造（含子切片初值），分片只承载 reduce 委托。
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

/// 状态分片：只承载 reduce 委托，无根状态工厂——不能作根挂载（根契约是
/// [`Reducer`]）。树内唯一非 blanket 实现：`compose` 域 `combine` 的产物。
///
/// 每个根都是合法子件（完整 Feature 可作 `combine` 的 child），blanket 单向：
/// `impl<T: Reducer> Fragment for T`——反向「每个分片都是根」正是本拆分封死
/// 的错误（分片作根 = E0277）。
///
/// 罕见二义：同文件 import 本 trait 与 [`Reducer`] 且裸调**完整 Feature** 的
/// `.reduce()` 会报 E0034（两个候选）——用 `Reducer::reduce(&feature, …)` 显式
/// 分发（树内零此形态：直接裸调的都是分片）。
pub trait Fragment {
    /// 同 [`Reducer::State`]
    type State: Clone + Send;
    /// 同 [`Reducer::Action`]
    type Action: Send;
    /// 同 [`Reducer::Op`]
    type Op;
    /// 纯函数：禁 IO/时钟/随机/写共享态（结构性：本 crate 无此依赖）。
    /// 效果写入 `out`（写入序 = 转发序），状态经返回值给出（效果出参化）。
    fn reduce(
        &self,
        state: &Self::State,
        action: Self::Action,
        out: &mut EffectBuf<Self::Action, Self::Op>,
    ) -> Self::State;
}

/// 每个根都是合法子件。
impl<T: Reducer> Fragment for T {
    type State = T::State;
    type Action = T::Action;
    type Op = T::Op;

    fn reduce(
        &self,
        state: &Self::State,
        action: Self::Action,
        out: &mut EffectBuf<Self::Action, Self::Op>,
    ) -> Self::State {
        Reducer::reduce(self, state, action, out)
    }
}

/// 效果句柄：reducer 按语义指派（同义恒同值），解释器寻址用（[`Effect::Cancel`]）。
/// 值语义小句柄（Copy+Eq+Hash）：效果断言、`HashMap`/`HashSet` 键等宿主寻址表
/// 都用值比对。
///
/// # 受控构造
///
/// 内部值私有：裸元组语法 `EffectId(n)` 编译期不可用，构造唯一入口是
/// [`EffectId::new`]，解释器读值走 [`EffectId::get`]——id 的产生点在代码里
/// 可 grep 可审计，不再有「任何整数随手当句柄」的裸通道。
///
/// # 唯一性纪律
///
/// 一个 store 内 id **全局唯一**：reducer 侧自持单调计数器（State 承载），
/// 多效果域（如 timer 与 http）用不重叠的 id 区间或语义派生值——各自从 0
/// 计数必撞 id，撞车后 [`Effect::Cancel`] 取消错对象且现象是「另一个效果莫名
/// 被取消」，极难定位。撞车的运行期观测面：参考实现 `SyncStore` 的
/// `cancel_unknown_id` 读侧（`Cancel` 引用从未发出的 `Run` id 即计数留痕）。
///
/// 裸元组构造已封死（doctest 即外部 crate 视角编译验证）：
///
/// ```compile_fail
/// use skeleton::EffectId;
/// let _id = EffectId(1); // 字段私有：元组构造器不存在
/// ```
///
/// 受控构造与读值：
///
/// ```
/// use skeleton::EffectId;
/// let id = EffectId::new(7);
/// assert_eq!(id.get(), 7);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EffectId(u64);

impl EffectId {
    /// 受控构造入口。唯一性纪律（store 内全局唯一、多效果域用不重叠区间）
    /// 见类型文档——构造可审计，纪律靠此文档与读侧计数共同守。
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    /// 读侧：解释器寻址表取键用（std `NonZeroU64::get` 同形纪律）。
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Effect：纯描述值。解释器（宿主适配层）执行后经回灌通道把 Action 重入 store（环闭合）。
/// Debug 供测试替身的断言失败消息如实回放实际效果。
///
/// 词表按真实采用面渐进扩：`#[non_exhaustive]` 使加变体属非破坏演进——
/// 跨 crate 的 match 必须带通配臂（同 crate 豁免），采用方在编译期被强制
/// 面对「未知效果形态」的分支而非静默漏配（纪律同 [`parse`]
/// 词表，F-07 封口）。
///
/// 下游穷尽 match 被编译期拒绝（doctest 即下游 crate 视角编译验证，纪律同
/// [`EffectId`] 的元组构造封口）：
///
/// ```compile_fail
/// use skeleton::Effect;
/// match Effect::<u8>::Emit(1) {
///     Effect::Emit(_) => {}
///     Effect::Run { .. } => {}
///     Effect::Cancel { .. } => {}
/// } // ERROR E0004: non-exhaustive patterns，`_` 臂缺失
/// ```
///
/// 带通配臂即放行，且已知变体照常命中自身臂：
///
/// ```
/// use skeleton::Effect;
/// let described = match Effect::<u8>::Emit(7) {
///     Effect::Emit(a) => format!("emit {a}"),
///     Effect::Run { id, .. } => format!("run {}", id.get()),
///     Effect::Cancel { id } => format!("cancel {}", id.get()),
///     _ => String::from("future variant"),
/// };
/// assert_eq!(described, "emit 7");
/// ```
#[derive(Debug)]
#[non_exhaustive]
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

/// 效果种类：解释器可见的效果分类（`Effect` 的可执行子集）。
///
/// [`Effect::Emit`] **不经解释器**（由 store 核心直接回灌入队），故不在本
/// 词表内；`Run` 与 `Cancel` 是当前仅有的两种解释器可见种类。
///
/// 词表按真实采用面渐进扩：`#[non_exhaustive]` 使加变体属非破坏演进——
/// 跨 crate 的 match 必须带通配臂（同 crate 豁免），采用方在编译期被强制
/// 面对「未知效果种类」的分支而非静默漏配（纪律同 [`Effect`] 词表）。
///
/// 与 [`EffectId`] 的分工：`EffectId` 是 reducer 分配的**寻址键**，**一个
/// store 内全局唯一**（见 [`EffectId`] 的唯一性纪律）——它与 `EffectKind`
/// 正交。`EffectKind` 是**效果形态**。
///
/// ⚠️ **句柄的语义随种类而变**：审计事件里的 `effect_id` 对 `Run` 是**该效果
/// 自己的**句柄，对 `Cancel` 是**被取消目标**的句柄（`Cancel` 效果自身没有
/// 句柄）⇒ `effect_id` **不可**单独当作「效果身份」用——同一 store 内
/// `Run(id=N)` 与 `Cancel(id=N)` 会发出**相同**的 `effect_id`；两条指向同一
/// 目标的 `Cancel` 效果也会发出**相同**的 `effect_id`（合法时序，非误用）。
/// **唯一标识一个效果的作用域**：在**同一个产出 action 内**，`(effect_kind,
/// effect_id, effect_index)` 三元组唯一标识一个效果；**跨 action** 时唯一
/// 标识一个效果的是 **`(origin_action_seq, effect_index)`**（=
/// 哪个 action 产出的第几条）——`effect_index` 是**产出 action 内**的序号，
/// 不跨 action 唯一。
///
/// ```
/// use skeleton::{Effect, EffectId, EffectKind};
/// let effect = Effect::<(), u8>::Run { id: EffectId::new(3), op: 1 };
/// let (kind, id) = EffectKind::of(&effect).expect("Run 经解释器");
/// assert_eq!(kind, EffectKind::Run);
/// assert_eq!(id, Some(EffectId::new(3)));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum EffectKind {
    /// 描述式副作用（对应 [`Effect::Run`]）：携带逻辑效果句柄。
    Run,
    /// 取消在途效果（对应 [`Effect::Cancel`]）：携带被取消效果的句柄。
    Cancel,
}

impl EffectKind {
    /// 效果的解释器侧身份：`(种类, 逻辑句柄)`。
    ///
    /// [`Effect::Emit`] 不经解释器 ⇒ `None`；其余种类恒 `Some`。
    /// 逻辑句柄缺失时内层为 `None`（未来无句柄的效果种类）。
    ///
    /// 匹配在本 crate 内对 [`Effect`] 词表**穷尽**——词表演进（新增效果
    /// 种类）时本函数编译期强制同步，下游 crate 免于各自补通配语义。
    ///
    /// ⚠️ **演进约束（双层 `Option` 的已知限制）**：本函数的穷尽匹配保证的是
    /// 「**覆盖 `Effect` 词表的所有变体**」，**不**保证「所有非 [`Effect::Emit`]
    /// 变体都必经解释器」⇒ 若将来新增一个**不经解释器**的效果种类并让它返回
    /// `None`，实现侧那条「非 `Emit` 效果必经解释器」的 `expect` 不变式将在
    /// **运行时 panic**（编译器**不会**报错——编译期强制同步只强制**改本函数**，
    /// 不强制**改成 `Some`**）⇒ **新增不经解释器的效果种类时，实现侧必须同步
    /// 处理本函数返回 `None` 的分支，不得依赖 `expect`**。
    pub fn of<A, Op>(effect: &Effect<A, Op>) -> Option<(EffectKind, Option<EffectId>)> {
        match effect {
            Effect::Emit(_) => None,
            Effect::Run { id, .. } => Some((EffectKind::Run, Some(*id))),
            Effect::Cancel { id } => Some((EffectKind::Cancel, Some(*id))),
        }
    }
}

/// 效果缓冲：reducer 的效果出参位。
///
/// 阅读面口径：reducer 只用 [`EffectBuf::push`]；`new`/`clear`/`is_empty`/
/// `pop_front` 是跨 crate 实现面便利（实现方排空循环构造/复用/逐条转发），
/// 不进十分钟阅读面。
///
/// **内部为前向可读的 FIFO 环形缓冲**（`VecDeque`）：追加在尾、摘取在头，摘队首
/// **不搬动**其余效果——多效果批量转发的元素搬移从 O(k²) 降为 O(k)（issue#43）。
/// 公共 API、语义与容量行为逐字不变（`clear` 仍保容量）。
pub struct EffectBuf<A, Op = Infallible> {
    effects: VecDeque<Effect<A, Op>>,
}

impl<A, Op> EffectBuf<A, Op> {
    /// 空缓冲（实现面便利：排空循环构造一次，此后 clear 复用）。
    pub fn new() -> Self {
        Self {
            effects: VecDeque::new(),
        }
    }

    /// reducer 写入一条效果；写入顺序 = 解释器转发顺序。
    pub fn push(&mut self, effect: Effect<A, Op>) {
        self.effects.push_back(effect);
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
    /// 效果原地留缓冲）。
    ///
    /// **O(1) 零搬移**：内部为环形缓冲，摘队首只推进头指针，其余效果原地不动
    /// ——旧实现每次把尾部整体前移一格 ⇒ 排空 k 条效果共 O(k²) 次元素搬移
    /// （issue#43）。
    pub fn pop_front(&mut self) -> Option<Effect<A, Op>> {
        if self.effects.is_empty() {
            None
        } else {
            self.effects.pop_front()
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
/// # 执行两档（同步档 / 托管档）
///
/// 同一个 `run` 契约下两档并存：`dispatch` 的**时序**在两档上不同，**语义**
/// 相同（状态只由 reducer 推进，效果结果一律经 [`ActionSender`] 回灌）：
///
/// - **同步档**：`run` 在排空环内联执行完（有界、非阻塞、不 `await`）。返回时
///   效果已执行；`back.dispatch` 与 dispatch 同栈，计入回灌深度预算。
/// - **托管档**：`run` 把工作交给适配层的异步托管（`skeleton-infra` 的
///   `fx::FxScope::spawn_tracked` 是先例），持 `back` 句柄立即返回——**返回 ≠
///   效果完成**。结果由任务经 `back.dispatch` 回灌成普通 action（显式消息
///   边界）；失败/取消面由托管层自报（如任务句柄的 `TaskError`、停机排空报告）。
///
/// 边界铁律（两档共用）：核心永不阻塞等待异步完成；异步结果只经
/// [`ActionSender`] 回灌；同步档内禁 `await`、禁阻塞 IO——`run` 在排空环内执行，
/// 阻塞它即阻塞整个状态机（后续 action 与回灌链全部排队）。**另禁 join 会回调
/// 同一 store 的线程**——`run` 内 join 一个用阻塞式 [`Store::dispatch`] 回调本
/// store 的线程 ⇒ 排空环被 join 卡住，而回执只由排空环兑现 ⇒ 确定性互等死锁
/// （详见 [`Store::dispatch`]「等待语义与宿主意务」）。**回调派生的 worker 用框架
/// 提供的 sender**（本回调收到的 `back`；本回调运行在排空环内 ⇒ 该 sender 必处
/// **竞争态** = **纯准入**、不等回执 ⇒ 可被 join）提交——排队的 action 在
/// **回调返回后**按 FIFO 处理。禁为「API 对称」把 `run` 改成
/// async：reducer 与核心恒同步，异步只住在边沿（适配层暴露自己的惯用异步形）。
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
    ///
    /// **panic 语义：至多一次调用尝试（at-most-once attempt）· 外部结果不确定**：
    /// `run` panic ⇒ 本次调用**退出自动重放**——框架**不会**再次调用它。
    /// 但**不得**据此认为该效果「未执行」：`run` 可能在 panic 之前**已产生外部
    /// 副作用**；框架按值取走效果，**无法区分**「panic 前未执行 / 执行中 panic /
    /// 执行完成后在本地清理或汇报时 panic」⇒ 该效果的**外部结果不确定
    /// （indeterminate）**。
    /// **不承诺** exactly-once 投递或 exactly-once 副作用，也**不承诺**「未执行」。
    ///
    /// **中断可见**：同一路径发出 `EffectInterrupted` 审计事件（`skeleton_infra`
    /// 的 `AuditEventKind`），转发失败回执仍报 `Failed`（失败阶段 =
    /// `ForwardingEffects`——注意与**成功**转发完毕的 `EffectsForwarded` 词序相反）
    /// + `state_committed=true`。
    ///
    /// **既有保证不变**：已成功转发的**前缀不重放**；已提交但未调用的**后缀保留**
    /// 并按 FIFO 续转（恰好一次）。
    ///
    /// **幂等性属消费方责任**：框架不替消费方重试一个可能已执行的效果。若将来
    /// 需要可重试投递，应作为**显式 opt-in 能力**引入（稳定的 delivery identity
    /// **并且**适配器侧提供持久幂等/去重、结果为三态（acknowledged /
    /// definitely-not-started / indeterminate），而非改写本默认契约
    /// （GitHub issue#45，2026-11-20 拍板：Option D）。
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
///
/// **两种来源，语义不同**：
///
/// - **框架提供的** sender（经 [`Store::sender`] 取到的、解释器回调收到的 `back`）
///   的语义**按调用瞬间有无在任排空者分两条路径**（逐条规格见
///   [`ActionSender::dispatch`]）：**竞争态**（已有在任排空者）= **纯准入**——
///   框架**只保证**该 action 已入队、**不等**它的完成边界；**返回瞬间该 action
///   是否已被 reduce，框架不作任何保证**（取决于在任排空者的推进速度：可能尚未
///   被 reduce，也可能已被 reduce 甚至已完成）⇒ 下游**不得**据「竞争态」推断
///   「此刻读状态仍是旧值」。**静默态**（无在任排空者）= 本线程**夺得排空权并
///   同步排空至 idle** ⇒ 返回时该 action **已被 reduce、其完成边界必然已达成**；
///   此路径**强于** [`Store::dispatch`] 的 per-action 完成边界契约——后者**不**
///   承诺队列为空（并发生产者下队列非空是正常态，不是未排空证据）。
///   **两条路径共同的例外**：若该 action 的某个因果后代撞上回灌深度预算，根
///   回执以 `Deferred` 收口 ⇒ 「完成边界已达成」**不含**被停放的子树（见
///   [`Store`] 类型文档①的结果面说明）。
/// - **自建** sender（经 [`ActionSender::new`] 装配）语义**由实现方决定**：闭包
///   是阻塞还是即时返回、`true` 意味什么，全由闭包负责；调用方须知其语义，
///   **不可假定**框架提供的 sender 的任一条路径语义。
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
    ///
    /// **闭包的语义即本句柄的语义**（是否阻塞、`true` 意味什么，由闭包决定）——
    /// 框架提供的 sender 的语义**按排空状态分两条路径**（竞争态 = 纯准入；
    /// 静默态 = 同步排空，见 [`ActionSender`] 类型文档）；**自建** sender 的语义由
    /// 实现方自证，调用方须知其语义、**不可假定**框架提供的 sender 的任一条路径语义。
    pub fn new(push: impl Fn(A) -> bool + Send + Sync + 'static) -> Self {
        Self {
            push: Arc::new(push),
        }
    }

    /// 回灌 Action。**框架提供的** sender 的语义**按调用瞬间的排空状态分两条路径**：
    ///
    /// - **竞争态**（调用瞬间已有在任排空者）：**纯准入**——框架**只保证**该 action
    ///   已入队、**不等**它的完成边界。**返回瞬间该 action 是否已被 reduce，框架
    ///   不作任何保证**——取决于在任排空者的推进速度：它可能尚未被 reduce，也可能
    ///   已被 reduce（甚至已完成）。下游**不得**据「竞争态」推断「此刻读状态仍是
    ///   旧值」。
    /// - **静默态**（调用瞬间无在任排空者）：本线程**夺得排空权并同步排空至 idle**
    ///   ⇒ 返回时该 action **已被 reduce、其完成边界必然已达成**（其效果已转发、
    ///   通知已发）。此路径**强于** [`Store::dispatch`] 的 per-action 完成边界
    ///   契约——后者**不**承诺队列为空（并发生产者下队列非空是正常态，不是未排空
    ///   证据）。**例外**：某个因果后代撞上回灌深度预算时根回执以 `Deferred`
    ///   收口 ⇒ 被停放的子树没跑（见 [`Store`] 类型文档①）。
    ///
    /// **返回的 `bool` 只承载准入结果**（被 store 接受/已入队 = `true`，被拒 =
    /// `false`），**不承载完成状态**：竞争态下是否已被 reduce、静默态下是否已达成
    /// 完成边界，均**不可**由 `bool` 推断。需要「等完成」的调用方用
    /// [`Store::dispatch`]。**自建** sender 的语义由实现方决定，见 [`ActionSender`]
    /// 类型文档「两种来源，语义不同」。
    ///
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
    /// **框架提供的** sender：返回 `true` = **被 store 接受**（已入队），
    /// `false` = 被拒（停机窗显式化）。**`bool` 不承载完成状态**：竞争态下该
    /// action 是否已被 reduce **不作任何保证**（取决于在任排空者的推进速度），
    /// 静默态下返回时它**必已被 reduce**（见上）。需要「等完成」的调用方用
    /// [`Store::dispatch`]；两者语义不同、**不可互换**。（**自建**
    /// sender 的返回值含义由其闭包决定，见 [`ActionSender`] 类型文档「两种来源，
    /// 语义不同」。）
    ///
    /// **drain 回调里派生并 join 的 worker ⇒ 用框架提供的 sender**（回调收到的
    /// `back` 或 [`Store::sender`]）：回调运行在排空环内 ⇒ 该 sender 必处
    /// **竞争态**（**纯准入**、不等回执）——若 worker 改用阻塞式 [`Store::dispatch`]
    /// 回调同一 store，则 worker 等回执、排空环等 worker ⇒ 确定性互等死锁（见
    /// [`Store::dispatch`]「等待语义与宿主意务」）。worker 用它只提交、不等回执 ⇒
    /// 回调可以 join 它（不与排空环互等），排队的 action 在**回调返回后**按 FIFO
    /// 处理。（**回调之外**调用同一 sender 时语义按排空状态分两条路径，见上。）
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
/// 1. **同步边界**：`dispatch` 返回 = **本 action 的完成边界**（状态提交 → 效果转发
///    → 通知返回，且其全部**因果后代**终局）；并发生产者场景下队列可能仍非空
///    （正常态，**非**未排空证据）；同序 action 序列恒同终态（确定性）。
///    **例外（回灌预算停放）**：某个因果后代撞上回灌深度预算时，其根回执以
///    `Deferred` 收口 ⇒ 该 action 的状态与已转发效果都已生效，但**被停放的子树
///    没跑**——`dispatch` 的 void 返回因此**不证明完整因果完成**。参考实现把
///    这条信息放在加法面上（`SyncStore::dispatch_with_receipt` 的
///    `DispatchOutcome`），本 trait 签名不变、行为不变。
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
/// 7. **Action 析构（unwind 清理期）**：实现方按值持有 action（队列/在途记录）
///    ⇒ 排空环 panic 展开时，残留载荷会在**展开栈上**被析构（框架拥有并按值
///    丢弃，无法阻止）。宿主义务：这类 `Drop` **必须 panic-free**（展开中二次
///    panic = 进程 abort），且**不得重入同一 store 的写入口**（此刻排空租约
///    已释放，重入会夺得新租约并在展开栈上再跑排空环）；只读面（快照、计数
///    读侧）安全。正常运行期的 `A` 析构同样在实现方内部锁之外——不因此对一般
///    路径提出 panic-free 要求。被中断的 action **禁重放**。
///
/// # 闭门后的入口对照（停机闸）
///
/// | 入口 | 闭门后行为 | fail-visible 面 |
/// |---|---|---|
/// | [`Self::dispatch`] | 恒 no-op（签名不引入 Err） | 计入实现方拒绝计数 |
/// | [`ActionSender::dispatch`] | 返回 `false`（显式拒绝，`#[must_use]` 兜底） | 同一计数 |
/// | [`Self::subscribe`] | 照常登记、票据照常注销，但闭门后永无 dispatch 触发——通知静默（已知取舍；显式拒绝面用实现方 `try_subscribe`） | 计入实现方「闭门登记」计数 |
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
    /// 数据从哪进（唯一写入口）：同步至**本 action 的完成边界**（口径见类型文档①；
    /// 并发生产者下队列可能仍非空，非未排空证据）。
    /// 闭门后恒 no-op（取舍论证见类型文档「闭门后的入口对照」）。
    ///
    /// **`Deferred` 例外（不证明完整因果完成）**：某个因果后代撞上回灌深度预算
    /// 时根回执以 `Deferred` 收口——状态与已转发效果都已生效，但被停放的子树
    /// 没跑。void 签名承载不了这条信息（本 trait 不为此改签名）：实现方若提供
    /// 结果面（参考实现 = `SyncStore::dispatch_with_receipt` 的
    /// `DispatchOutcome`），要区分「完整完成」与「子树被停放」的调用方必须用它。
    ///
    /// **等待语义与宿主义务**：外部调用者的 `dispatch` **阻塞**至自己的完成边界
    /// （含其全部因果后代终局）。由此产生两条宿主义务：
    ///
    /// - **同 drain 栈 / 同线程重入**：仅入队、不等待（否则自死锁）。
    /// - **drain 回调内的跨线程阻塞**：drain 回调（[`EffectRuntime::run`]、订阅回调）
    ///   **运行在排空环内**——宿主在其中 **join 一个会回调同一 store 的线程**、而该
    ///   线程用阻塞式 `dispatch` ⇒ **确定性互等死锁**（排空环被 join 卡住，而回执
    ///   只由排空环兑现）。**安全写法**：回调派生的 worker 必须走**框架提供的**
    ///   sender（回调收到的 `back` 或 [`Store::sender`]）的
    ///   [`ActionSender::dispatch`]——**在 drain 回调内该 sender 必处竞争态**
    ///   （排空权在本线程）⇒ **纯准入、不等回执**（其 `bool` = 被接受，非完成）
    ///   ——排队的 action 在**回调返回后**按 FIFO 处理。若 worker 确实需要该 action
    ///   的**完成结果**才能返回，则它**不得**被跑在同一 store 排空环上的回调同步
    ///   join。（**回调之外**调用同一 sender 时语义**按排空状态分两条路径**：静默态
    ///   下它会夺得排空权并同步排空 ⇒ 调用方线程承担该次排空，见
    ///   [`ActionSender::dispatch`]。）
    ///   （**自建** sender 的语义由实现方决定，见 [`ActionSender`] 类型文档「两种
    ///   来源，语义不同」——不可假定框架提供的 sender 的任一条路径语义。框架**无法**检测宿主侧的跨线程
    ///   join ⇒ 本契约只规定**义务与规避路径**，**不提供**框架侧防护或超时；复现
    ///   与场景矩阵见 `verify/2026-11-20-residual-join-risk/`。）
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
    /// 停机闸：单向（幂等），置位后 best-effort 排空（drop 不静默弃队）。
    ///
    /// **不承诺 quiescence**：本线程夺得排空租约时同步排至 idle；他线程已持
    /// 租约时只完成交接即返（排空仍在进行）。要显式等静止见
    /// `skeleton_infra::runtime::SyncStore::close_wait` / `close_wait_timeout`。
    ///
    /// **不是效果 flush**：不承诺终态 pending 效果快照（解释器 panic 后
    /// 已提交未转发的效果后缀仍归 store 所有）。需要终态报告用
    /// `SyncStore::close_wait_with_report`，显式丢弃保留后缀用
    /// `SyncStore::discard_pending_effects`（参考实现面）。
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
///
/// 词表按真实采用面渐进扩：`#[non_exhaustive]` 使加态属非破坏演进——跨
/// crate 的 match 必须带通配臂（同 crate 豁免），恢复方在编译期被强制面对
/// 「未知恢复形态」的分支（fail-closed）而非静默漏配（纪律同 [`Effect`]，
/// F-07 封口）。
///
/// 下游穷尽 match 被编译期拒绝（doctest 即下游 crate 视角编译验证）：
///
/// ```compile_fail
/// use skeleton::Restore;
/// match Restore::<u8, u8>::Empty {
///     Restore::Empty => {}
///     Restore::FromInitial(_) => {}
///     Restore::FromSnapshot(_, _) => {}
/// } // ERROR E0004: non-exhaustive patterns，`_` 臂缺失
/// ```
///
/// 带通配臂即放行，且已知变体照常命中自身臂：
///
/// ```
/// use skeleton::Restore;
/// let described = match Restore::<u8, u8>::FromInitial(vec![1, 2]) {
///     Restore::Empty => String::from("empty"),
///     Restore::FromInitial(actions) => format!("initial {}", actions.len()),
///     Restore::FromSnapshot(_, actions) => format!("snapshot {}", actions.len()),
///     _ => String::from("future variant"),
/// };
/// assert_eq!(described, "initial 2");
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
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

    /// 摘队首**不得搬动其余效果**——排空 k 条效果时每次摘除只允许 O(1) 搬移。
    ///
    /// 判据取**地址不变**（其余效果原地不动）而非计时：计时被布局噪声主导，
    /// 不可作证据（issue#43 测量纪律）。旧实现 `Vec::remove(0)` 每次把尾部
    /// 全部元素前移一格 ⇒ 其余效果地址改变 ⇒ 此测在旧实现下为红。
    #[test]
    fn effect_buf_pop_front_does_not_relocate_remaining_effects() {
        let mut buf: EffectBuf<Tick, Infallible> = EffectBuf::new();
        for _ in 0..8 {
            buf.push(Effect::Emit(Tick::Tick));
        }
        // 第 2 条效果当前所在地址（摘队首后它应原地不动，只换逻辑序号）。
        let tail_before = &buf.effects[1] as *const Effect<Tick, Infallible>;
        buf.pop_front();
        let tail_after = &buf.effects[0] as *const Effect<Tick, Infallible>;
        assert_eq!(
            tail_before, tail_after,
            "pop_front 只摘队首（其余效果原地不动）——否则排空退化为 O(k²) 元素搬移"
        );
    }
}
