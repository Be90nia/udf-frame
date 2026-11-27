//! 最小单向数据流（UDF / Elm 式架构）骨架——**契约层**。
//!
//! 本 crate 只承载「约束后面代码长法的形状」：trait（谁变形 / 谁解释 / 谁存储 /
//! 谁呈现 / 谁持久化 / 谁发异步变更信号 / 什么算一个 store）、数据词汇
//! （Effect/EffectBuf/EffectId/ActionSender/Subscription/NoFx）与语义契约文本
//! （各 trait 文档中的承诺）。零
//! `[dependencies]`——核心无 IO/运行时依赖，且框架不向 reducer 参数暴露 IO/运行时
//! 能力；但「reducer 禁 IO」是**实现/架构不变式，不是 Rust 编译期保证**——下游实现
//! 仍可直接调用 `std::fs`/时钟/全局状态或自加依赖（issue#136）。
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
//! | 谁持久化 | [`JournalPort`] 端口（`append`=缓冲接受、[`JournalPort::flush`]=持久点；后端自换；参考实现 `skeleton_infra::journal::JsonlJournal`） |
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
pub mod sink;
pub mod view;

pub use sink::{Buffered, EffectSink, FragmentInto, SinkLease};

use std::collections::VecDeque;
pub use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// State：值语义只读快照的形状约束。契约：禁内部可变性（无 Mutex/Cell 字段）、
/// 非法状态进类型（互斥态用嵌套 enum，禁 bool 旗子）。
///
/// **根契约**：[`Reducer`] 是一个 Feature 的唯一状态迁移函数——`initial` 给出
/// 根状态、`reduce` 同 `(state, action)` 恒产同输出（确定性）。**纯度边界**：
/// 框架不通过 reducer 参数暴露 IO/运行时能力（trait 签名即边界，`Op = Infallible`
/// 阻止用框架效果词汇描述效果）；但「禁 IO/时钟/随机」与状态不可变是**实现/架构
/// 不变式，不是编译期保证**——下游实现仍可直接调用 std/OS，`Clone + Send + Sync`
/// 也不排除内部可变性（issue#136）。子件（`combine` 产物）只需 [`Fragment`]：
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
    /// 纯函数：禁 IO/时钟/随机/写共享态（实现纪律——本 crate 无此依赖，但编译器不强制下游实现）。
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
    /// 纯函数：禁 IO/时钟/随机/写共享态（实现纪律——本 crate 无此依赖，但编译器不强制下游实现）。
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
/// # 组合纪律（issue#163）：独立子件不自动可组合
///
/// `combine` 只做 reduce 委托 + 效果原值转发，**不改写**子件发出的 `Run { id }` /
/// `Cancel { id }`：两个各自从 1 计数的子件装上同一 store 就会撞 id，
/// `Cancel(1)` 取消到**另一个**子件的在途效果——`cancel_unknown_id` 读侧抓不到
/// （id 不未知，只是归属错了），现象是「另一个效果莫名被取消」。
///
/// ⇒ **组合方必须显式声明**各子件的效果 ID 命名空间/范围，并在装配期校验重叠：
/// [`EffectNamespace`] + [`validate_effect_namespaces`]（`combine!` 的
/// `namespaces:` 臂在**编译期**断言同一判定）。**不声明 = 不校验 = 保持现状**：
/// 加新子件可能静默作废既有子件的 id 命名空间，这条路只在组合方明确放弃校验时
/// 才该走。
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

/// 效果 ID 命名空间声明（组合面，issue#163）：一个子件独占 `[lo, hi]`（**含两端**）。
///
/// 单个 `Fragment` 只保证自己内部 id 唯一；**独立子件的命名空间互不相同不是自动
/// 的**——组合方必须显式声明各子件占用哪段 id，并在装配期校验
/// （[`validate_effect_namespaces`]）。不自报声明 = 不校验 = 保持 issue#163 之前的
/// 行为（见 [`EffectId`] 文档「组合纪律」段）。
///
/// [`Effect::Run`] 与 [`Effect::Cancel`] 共用同一 id 空间（`Cancel` 寻址的就是
/// `Run` 写出的同值句柄），所以**每个子件只声明一次**区间，不按效果种类分开声明。
///
/// 声明列表是**每个装配点一份（扁平）**：嵌套组合（父装配点里含一个子组合分片）
/// 时，子组合整体在父装配点上按一整段区间声明，父级不自动继承/推导子件的内部分配
/// ——「显式」正是本方案的全部内容。
///
/// ```
/// use skeleton::{EffectNamespace, validate_effect_namespaces};
/// let decls = [
///     EffectNamespace::new("timer", 0, 999),
///     EffectNamespace::new("http", 1000, 1999),
/// ];
/// assert_eq!(validate_effect_namespaces(&decls), Ok(()));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EffectNamespace {
    name: &'static str,
    lo: u64,
    hi: u64,
}

impl EffectNamespace {
    /// 声明子件 `name` 占用 `lo..=hi`。
    ///
    /// **不在此校验** `lo > hi`：单条声明的合法性由
    /// [`validate_effect_namespaces`] 统一判定（一处语义），构造保持 `const` 以便
    /// 写进 `const` 数组——`combine!` 的 `namespaces:` 臂正是靠这一点在**编译期**
    /// 断言重叠。
    pub const fn new(name: &'static str, lo: u64, hi: u64) -> Self {
        Self { name, lo, hi }
    }

    /// 子件名（诊断文案用）。
    pub const fn name(self) -> &'static str {
        self.name
    }

    /// 占用区间 `(lo, hi)`（含两端）。
    pub const fn range(self) -> (u64, u64) {
        (self.lo, self.hi)
    }
}

/// 命名空间声明被拒的 typed 原因（[`validate_effect_namespaces`]）。
///
/// `Display` 含子件名 + 区间 + 首个重叠 id，可直接打印进装配期诊断；本类型不
/// panic（拒绝经返回值给出）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EffectNamespaceConflict {
    /// 单条声明自身无效：`lo > hi`（区间空置）。**必须拒绝**——空区间永不与任何
    /// 声明相交，会把重叠校验对全部 id 静默失效。
    Inverted {
        /// 声明方子件名。
        name: &'static str,
        /// 声明的下界。
        lo: u64,
        /// 声明的上界（在 `lo` 之前 ⇒ 空区间）。
        hi: u64,
    },
    /// 两个子件的区间相交（含一端完全包含另一端——嵌套组合的常见形态）：它们会
    /// 发出同一个 id，运行时 [`Effect::Cancel`] 可能取消到另一个子件的在途效果。
    Overlap {
        /// 声明序在前的一方。
        left: EffectNamespace,
        /// 声明序在后的一方。
        right: EffectNamespace,
        /// 首个重叠 id（`max(left.lo, right.lo)`）。
        at: u64,
    },
}

impl std::fmt::Display for EffectNamespaceConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Inverted { name, lo, hi } => write!(
                f,
                "effect id namespace rejected: `{name}` declares {lo}..={hi} with lo > hi \
                 (an empty range would silently disable overlap checking)"
            ),
            Self::Overlap { left, right, at } => write!(
                f,
                "effect id namespace overlap: `{}` {}..={} and `{}` {}..={} share id {at} \
                 (independently counted fragments collide; declare disjoint ranges)",
                left.name, left.lo, left.hi, right.name, right.lo, right.hi
            ),
        }
    }
}

impl std::error::Error for EffectNamespaceConflict {}

/// 装配期命名空间校验：`lo > hi` 或任意两声明相交 ⇒ typed `Err`；空声明 ⇒ `Ok`
/// （**不声明 = 不校验 = 现状**）。
///
/// `const fn`：`combine!` 的 `namespaces:` 臂在**编译期**断言它（重叠 = 编译错误，
/// 比运行期 panic 早且强）。零依赖、零分配、O(n²)（声明数 = 子件数，个位数），报
/// **声明序上的第一处**冲突（确定性）。相交判定按闭区间：
/// `a.lo <= b.hi && b.lo <= a.hi`。
///
/// 重叠在编译期即被拒绝（`combine!` 的 `namespaces:` 臂用的是同一个函数）：
///
/// ```compile_fail
/// use skeleton::{EffectNamespace, validate_effect_namespaces};
/// const _: () = assert!(matches!(
///     validate_effect_namespaces(&[
///         EffectNamespace::new("timer", 0, 999),
///         EffectNamespace::new("http", 500, 1499),
///     ]),
///     Ok(())
/// )); // ERROR: `http` 与 `timer` 相交 ⇒ 编译期断言失败
/// ```
pub const fn validate_effect_namespaces(
    decls: &[EffectNamespace],
) -> Result<(), EffectNamespaceConflict> {
    let n = decls.len();
    let mut i = 0;
    while i < n {
        let a = decls[i];
        if a.lo > a.hi {
            return Err(EffectNamespaceConflict::Inverted {
                name: a.name,
                lo: a.lo,
                hi: a.hi,
            });
        }
        let mut j = 0;
        while j < i {
            let b = decls[j];
            if a.lo <= b.hi && b.lo <= a.hi {
                return Err(EffectNamespaceConflict::Overlap {
                    left: b,
                    right: a,
                    at: if a.lo > b.lo { a.lo } else { b.lo },
                });
            }
            j += 1;
        }
        i += 1;
    }
    Ok(())
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
/// `clear` 仍保容量。
///
/// **有界暂存（ADR-0011 §2.1，issue#233）**：[`Self::push`] 签名不变、仍不可
/// 失败——条数到达本段生效上限（[`Self::set_effect_cap`]，`0` = 不限）后**拒绝
/// 继续插入**并置溢出标志（[`Self::overflowed`]）。实现方在 reducer 返回后读该
/// 标志，把整次转换按全有或全无处理（不发布状态、不转发任何效果）⇒ 拒绝是
/// 「整段作废」的一部分，不是静默丢弃。溢出后继续 `push` 不增长条数、不 panic；
/// [`Self::clear`] 复位溢出标志（复用前必清零），上限值由实现方在每次 reduce
/// 入口重设（禁沿用上一拍）。
///
/// **批量入口（opt-in，issue#237）**：[`Self::push_batch`] 一次性快照剩余额度
/// 后成批追加，循环内不再逐条判断预算——收益只属于显式采用该 API 的写入方，
/// 旧 [`Self::push`] 的即时硬界不变（单条 push 无法在没有可信批量信息时省掉
/// 边界判断）。`size_hint` 只作预留提示，不是安全证明。
pub struct EffectBuf<A, Op = Infallible> {
    effects: VecDeque<Effect<A, Op>>,
    /// 本段生效上限：`None` = 不限、`Some(n)` = 至多 n 条。**`Some(0)` 是精确零
    /// 额度**（组合链借出额度时父缓冲可暂时不可写）——公共约定仍是
    /// [`Self::set_effect_cap`] 的 `0` = 不限，两者不是一回事。
    cap: Option<usize>,
    /// 到界后是否已拒绝过插入（`clear` 复位）。
    overflowed: bool,
}

impl<A, Op> EffectBuf<A, Op> {
    /// 空缓冲（实现面便利：排空循环构造一次，此后 clear 复用）。
    pub fn new() -> Self {
        Self {
            effects: VecDeque::new(),
            cap: None,
            overflowed: false,
        }
    }

    /// reducer 写入一条效果；写入顺序 = 解释器转发顺序。
    ///
    /// 到界（[`Self::set_effect_cap`]）后**本条不入缓冲**并置溢出标志：调用方
    /// 据此整段作废，不得把本条当作已接受（见类型文档「有界暂存」）。
    pub fn push(&mut self, effect: Effect<A, Op>) {
        if self.cap.is_some_and(|cap| self.effects.len() >= cap) {
            self.overflowed = true;
            return;
        }
        self.effects.push_back(effect);
    }

    /// 本段剩余额度（实现面读侧，供批量入口与组合链预算协作）。
    ///
    /// `None` = **不限**；`Some(n)` = 还可再接纳 n 条（`Some(0)` = **精确零
    /// 额度**）。注意 `Some(0)` 与公共约定「[`Self::set_effect_cap`] 的 `0` =
    /// 不限」不是一回事：不限额度在本方法返回 `None`，绝不返回 `Some(0)`。
    pub fn remaining_limit(&self) -> Option<usize> {
        self.cap.map(|cap| cap.saturating_sub(self.effects.len()))
    }

    /// 预留 `want` 条额度（实现面预算协作，见 [`sink::SinkLease`]）。返回**实际
    /// 借到**的条数：有限额度下 = min(剩余, `want`)；不限额度时恒等于 `want`
    /// （不记账）。
    pub fn reserve_staging(&mut self, want: usize) -> usize {
        match self.cap {
            None => want,
            Some(cap) => {
                let free = cap.saturating_sub(self.effects.len());
                let granted = free.min(want);
                self.cap = Some(cap - granted);
                granted
            }
        }
    }

    /// 归还 `n` 条额度（与 [`Self::reserve_staging`] 配对；不限额度时是空操作）。
    pub fn release_staging(&mut self, n: usize) {
        if let Some(cap) = self.cap {
            self.cap = Some(cap + n);
        }
    }

    /// 批量写入（opt-in；issue#237）。一次性快照剩余额度 R 后成批追加，追加
    /// 循环内**不再逐条重复预算判断**——批量路径的收益来自这里，旧 [`Self::push`]
    /// 的即时硬界**不变**（对任意旧 reducer 的单条 push，写入前仍必须判断额度；
    /// 把检查挪到 reduce 返回后、每 N 次检查一次、或让 `VecDeque` 自行扩容后
    /// 再裁决，都会破坏 ADR-0011 的暂存期硬界）。
    ///
    /// 语义（ADR-0011 §6「批量入口」）：
    /// - **不信任** `size_hint` / `ExactSizeIterator::len`：只作 `reserve` 提示，
    ///   实际消费严格受 R 限制；R 条后再探测一次，迭代器仍能产出一条 ⇒ 置
    ///   [`Self::overflowed`]，**超出项不写入**（条数界绝不破）。
    /// - 不限额度时直接 `extend`（无边界判断）。
    /// - 已溢出时直接短路：本段已注定整段拒绝，继续写入只是无谓工作。
    /// - 迭代器 panic 如实传播——本段 staging 由实现方既有的 reducer panic 路径
    ///   整体清理，本方法不 `catch_unwind`。
    ///
    /// 条数界不是字节界：`reserve` 只影响容量，分配器可以给得更多，不能据此
    /// 声称字节级硬上限。
    pub fn push_batch<I>(&mut self, effects: I)
    where
        I: IntoIterator<Item = Effect<A, Op>>,
    {
        if self.overflowed {
            return;
        }
        let mut iter = effects.into_iter();
        match self.remaining_limit() {
            None => self.effects.extend(iter),
            Some(remaining) => {
                let hint = iter.size_hint().0.min(remaining);
                self.effects.reserve(hint);
                for _ in 0..remaining {
                    match iter.next() {
                        Some(effect) => self.effects.push_back(effect),
                        None => return,
                    }
                }
                if iter.next().is_some() {
                    self.overflowed = true;
                }
            }
        }
    }

    /// 清空保容量并复位溢出标志（实现面便利：容量留给下一次 reduce 复用）。
    ///
    /// **实现面专用**：框架在每次 reduce 入口自行调用（并随即重设本段上限）。
    /// reducer 在 `reduce` 里调用它会**削弱拒绝显形**——溢出标志被复位，本条输出
    /// 的「已溢出」事实消失，框架因此不会整次拒绝该转换（条数界本身不破：被拒的
    /// 插入一条也没进缓冲）。不要在自己的 reducer 里调用。
    pub fn clear(&mut self) {
        self.effects.clear();
        self.overflowed = false;
    }

    /// 本段暂存的条数上限（`0` = 不限）。实现面在每次 reduce 入口重设。
    pub fn set_effect_cap(&mut self, cap: usize) {
        self.cap = (cap != 0).then_some(cap);
    }

    /// 自上次 [`Self::clear`] 以来是否有过被拒绝的插入（= 本段已溢出）。
    ///
    /// 实现面在 reducer 返回后读它：`true` ⇒ 整次转换被拒（全有或全无）。
    pub fn overflowed(&self) -> bool {
        self.overflowed
    }

    /// 暂存条数（实现面便利：组合链按它归还借出的额度）。
    pub fn len(&self) -> usize {
        self.effects.len()
    }

    /// 把本缓冲标记为已溢出（实现面专用）。组合链用它把**子缓冲**的溢出事实
    /// 沿父链传回根——否则「子缓冲截断」会退化成静默丢弃，违反 ADR-0011 §2.2
    /// 的全有或全无。
    pub fn mark_overflowed(&mut self) {
        self.overflowed = true;
    }

    /// 按 `Option` 语义设置上限（实现面专用）：`None` = 不限、`Some(0)` =
    /// **精确零额度**。组合链用 [`StagingLease::capacity`] 给子缓冲设上限——
    /// **不得**改用 [`Self::set_effect_cap`]，那条公开约定把 `0` 当「不限」，
    /// 会把「零额度」误设成无界。
    pub fn set_staging_cap(&mut self, cap: Option<usize>) {
        self.cap = cap;
    }

    /// 借出本段全部剩余额度（组合链预算协作，ADR-0011 §2.2 / issue#235）。
    ///
    /// 借出期间**本缓冲的有效上限收缩为当前条数**（新的 `push` 一律被拒，直到
    /// 借出的 credit 被归还）；不限额度时是空操作。配合 [`StagingLease::release_one`]
    /// 逐条归还，可保证「父缓冲条数 + 子缓冲仍存活条数 ≤ 本段总预算」——嵌套组
    /// 合递归成立，即全链 live staging 总量不超过预算。
    ///
    /// 租约是 **RAII**：`Drop` 归还全部未释放的 credits 并恢复原上限，故子
    /// reducer / mapper panic 展开时也不会留下被永久缩小的缓冲。
    pub fn reserve_all_remaining(&mut self) -> StagingLease<'_, A, Op> {
        let original = self.cap;
        let outstanding = self.remaining_limit();
        if outstanding.is_some() {
            self.cap = Some(self.effects.len());
        }
        StagingLease {
            buf: self,
            original,
            outstanding,
        }
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

/// 组合链预算租约（ADR-0011 §2.2 / issue#235）：由
/// [`EffectBuf::reserve_all_remaining`] 借出父缓冲的全部剩余额度，逐条归还。
///
/// 不变式（每步由 `sync` 维持）：父缓冲的有效上限恒为「原上限 − 未归还
/// credits」，于是「父缓冲条数 + 子缓冲仍存活条数 ≤ 原上限」；嵌套组合时父层
/// 上限本身已扣除祖先存活缓冲 ⇒ 递归 ⇒ **全链 live staging 总量不超过预算**。
///
/// **RAII**：`Drop` 归还全部未释放 credits 并恢复原上限（panic 展开也安全）。
pub struct StagingLease<'p, A, Op> {
    buf: &'p mut EffectBuf<A, Op>,
    /// 借出前的原上限（`None` = 不限）。
    original: Option<usize>,
    /// 尚未归还的 credits（`None` = 不限额度：租约是空操作）。
    outstanding: Option<usize>,
}

impl<A, Op> StagingLease<'_, A, Op> {
    /// 借出的额度：子缓冲应以此为自己的上限（`None` = 不限、`Some(0)` =
    /// 精确零额度，见 [`EffectBuf::set_staging_cap`]）。
    pub fn capacity(&self) -> Option<usize> {
        self.outstanding
    }

    /// 子 reduce 返回后调用：子缓冲只用了 `used` 条 ⇒ 归还多余额度。
    pub fn release_unused(&mut self, used: usize) {
        if let Some(outstanding) = self.outstanding {
            debug_assert!(
                used <= outstanding,
                "子缓冲用了 {used} 条，超出借出额度 {outstanding}"
            );
            self.outstanding = Some(used.min(outstanding));
            self.sync();
        }
    }

    /// 从子缓冲转交一条之前调用：归还一个 credit（父缓冲可写额度 +1）。
    pub fn release_one(&mut self) {
        if let Some(outstanding) = self.outstanding {
            self.outstanding = Some(outstanding.saturating_sub(1));
            self.sync();
        }
    }

    /// 逐条把子缓冲转交给父缓冲：每 pop 一条先 [`Self::release_one`]，再调
    /// `mapper`（拿到的是父缓冲的可写借用）。mapper 可按旧契约产生 0/1/多条，
    /// 全部受父缓冲当前上限约束 ⇒ **fan-out 不能借用子缓冲尚未释放的槽位**。
    ///
    /// 子缓冲曾溢出 ⇒ 把溢出事实沿父链传回（整段由框架原子拒绝，不静默截断）。
    pub fn transfer_from<CA, CO, F>(&mut self, child: &mut EffectBuf<CA, CO>, mut mapper: F)
    where
        F: FnMut(Effect<CA, CO>, &mut EffectBuf<A, Op>),
    {
        while let Some(effect) = child.pop_front() {
            self.release_one();
            mapper(effect, self.buf);
        }
        if child.overflowed() {
            self.buf.mark_overflowed();
        }
    }

    /// 父缓冲有效上限 = 原上限 − 未归还 credits。
    fn sync(&mut self) {
        if let Some(outstanding) = self.outstanding {
            self.buf.cap = self.original.map(|b| b.saturating_sub(outstanding));
        }
    }
}

impl<A, Op> Drop for StagingLease<'_, A, Op> {
    fn drop(&mut self) {
        // RAII：panic 展开也必须归还全部 credits 并恢复原上限。
        self.buf.cap = self.original;
        self.outstanding = None;
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
/// # 注销回调契约（实现方必须兑现，#137）
///
/// 注销回调在**票据 Drop 的线程上同步执行**（包含正在 panic unwind 的时刻），
/// 故必须是最小的「摘表 + 失活」动作：
///
/// - **禁 panic**：Drop 面用 `catch_unwind` 就地 contain（unwind 期逃逸的 panic
///   = 进程 abort），失败计入 [`subscription_teardown_panic_count`]——不静默，
///   计数器就是可观察失败通道。要拿原始 panic 载荷请显式调
///   [`Subscription::unsubscribe`]。
/// - **禁阻塞、禁等待在途工作**：Drop 只尽最大努力（best-effort）摘表，不等待
///   任何在途通知/别的注销完成——尤其**不得等待「本次回调返回才能推进」的工作**
///   （那就是自等死锁）。
/// - **禁重入等待**：回调里注销别的票据合法，但不得等待当前 Drop（或通知栈）
///   返回。
///
/// **订阅回调内注销自己**（持有票据的回调在自己的通知里 drop 它）合法：注销 =
/// 失活 + 摘表，参考实现不等待正在跑的回调，本次通知照常完成（in-flight 语义，
/// **不承诺** quiescent 退订）；此后的通知不再触达该订阅。
///
/// 同族票据（[`pool::PoolAuditTicket`]）已持同一套 Drop 契约（contain + 有界
/// 计数器 + 显式关闭面）；两者生命周期语义各自独立，不互相代理。
///
/// 票据对采用方不透明：字段私有，经实现方 [`Store::subscribe`] 取得。
#[must_use = "订阅票据被丢弃即立即注销（Drop=注销）——`store.subscribe(..);` 裸调用 = 静默死订阅；绑定持有才持续收通知"]
pub struct Subscription<S> {
    unsubscribe: Option<Box<dyn FnOnce() + Send + Sync>>,
    _state: std::marker::PhantomData<fn() -> S>,
}

static SUBSCRIPTION_TEARDOWN_PANICS: AtomicU64 = AtomicU64::new(0);

/// Number of unsubscribe callbacks that panicked while being invoked by
/// [`Subscription`]'s `Drop`.
///
/// Drop 面无法返回错误 ⇒ 本计数器是可观察的失败通道。显式
/// [`Subscription::unsubscribe`] 把 panic 作为 `Err` 原样交给调用方、不计入。
/// 值饱和于 `u64::MAX`，不回绕。
pub fn subscription_teardown_panic_count() -> u64 {
    SUBSCRIPTION_TEARDOWN_PANICS.load(Ordering::Relaxed)
}

fn record_subscription_teardown_panic() {
    let _ =
        SUBSCRIPTION_TEARDOWN_PANICS.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
            Some(count.saturating_add(1))
        });
}

impl<S> Subscription<S> {
    /// 实现面便利：由 store 实现方构造——`unsubscribe` 在票据 Drop 时执行一次
    /// （摘表 + 计数回落等注销语义由实现方定义）。
    ///
    /// 回调契约见 [`Subscription`] 类型文档「注销回调契约」：**不 panic、有界、
    /// 非阻塞、不等待当前 Drop/通知栈**。
    pub fn new(unsubscribe: impl FnOnce() + Send + Sync + 'static) -> Self {
        Self {
            unsubscribe: Some(Box::new(unsubscribe)),
            _state: std::marker::PhantomData,
        }
    }

    /// 显式注销（消费票据）：注销回调的 panic 以 `Err(原始载荷)` 返回，不吞、
    /// 不 abort。回调恰好执行一次（panic 亦然），此后再 Drop 本票据不会重跑。
    ///
    /// 需要「阻塞式/可失败 teardown」的调用方用本入口自己组合；Drop 面只保留
    /// 最小非阻塞 best-effort 兜底（见类型文档）。
    pub fn unsubscribe(mut self) -> Result<(), Box<dyn std::any::Any + Send>> {
        let Some(unsubscribe) = self.unsubscribe.take() else {
            return Ok(());
        };
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(unsubscribe))
    }
}

impl<S> Drop for Subscription<S> {
    fn drop(&mut self) {
        let Some(unsubscribe) = self.unsubscribe.take() else {
            return;
        };
        // 恰好一次；panic 就地 contain（Drop 里逃逸的 panic 在 unwind 期 = abort）。
        // 失败不静默：计入可观察计数器。Drop 不等待、不重试。
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(unsubscribe)).is_err() {
            record_subscription_teardown_panic();
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
///    **例外（资源保护拒绝）**：实现方可用资源保护策略**拒绝整次转换**（例如单次
///    reducer 输出超过实现方配置的条数界）。此时 `dispatch` 返回时该 transition
///    **什么都没提交**（next state 未发布、它产出的效果零转发、超限 work 无部分
///    进展）——返回**不代表** transition 已提交，也**不代表**失败。本 trait 不
///    规定拒绝的判定、数值与观测面：判定与数值由实现方配置，观测手段也由实现
///    定义（计数读侧 / 审计事件 / 结果面）。
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
    /// **资源保护拒绝例外（返回不代表 transition 已提交）**：实现方可用资源保护
    /// 策略**拒绝整次转换**（例如单次 reducer 输出超过实现方配置的条数界）。被拒
    /// 时本方法返回**什么都没提交**：该 transition 的 next state 未发布、它产出的
    /// 效果零转发、超限 work 无部分进展——本方法**不 panic**（拒绝是资源保护，
    /// 不是失败），void 返回也不构成「已提交」的证据。判定与数值由实现定义；
    /// 需要同步获知该结局、或从诊断面观测（计数读侧 / 审计事件 / 结果面）的
    /// 调用方见实现方文档。
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

/// 「后端没有持久化位」的 typed 错误（[`JournalPort::flush`] 不支持面）。
///
/// 没有持久化边界、无法承诺任何跨崩溃可见性的实现把它内嵌进自己的
/// [`JournalPort::Err`]，从 `flush` 显式返回——typed 拒绝优于返回 `Ok`
/// 静默谎报持久（`flush` 刻意没有默认实现）。进程内后端若把「已接受即可见」
/// 当作自己的边界，也可以合法返回 `Ok`，但必须在实现文档里如实声明该边界。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlushUnsupported;

impl std::fmt::Display for FlushUnsupported {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("journal backend has no flush/durability boundary")
    }
}

impl std::error::Error for FlushUnsupported {}

/// 状态持久化端口（框架留口、后端自换）：动作追加 + 显式持久化位 + 快照 +
/// 恢复的最小面。真相源「在后端怎么存」完全委托给实现方——JSONL/SQLite/网络
/// 皆可，序列化词表由实现自选，本 crate 不绑格式（零依赖纪律）。参考实现：
/// `skeleton_infra::journal::JsonlJournal`（`journal` feature）。
///
/// 参考实现的行格式约束（细节归实现方）：action/state 须可序列化为
/// **JSON 对象**（行 tag 与载荷平铺同层）——非对象载荷（如 `A = u64` 序列化
/// 为裸数字）首条 [`Self::append`] 即 `Err`；`Err` 发生在任何字节进入持久化
/// 管线之前 ⇒ 零副作用，换对象形状重试即可。
///
/// # 语义契约（实现必须兑现）
///
/// 1. **`append` = 缓冲接受，不是已持久**：[`Self::append`] 的 `Ok` 只表示整条
///    记录已被本实现接受（参考实现=写进 64 KiB `BufWriter`），**不承诺**崩溃后
///    仍可恢复；`Err` ⇒ 调用方禁 apply 该 action。
/// 2. **持久化边界显式**：[`Self::flush`] 把此前 append 的全部记录推过实现的
///    持久化边界（参考实现=`BufWriter` flush + `sync_all`，掉电真持久）。
///    「apply 后崩溃不丢史」的调用方必须在 apply 前调 `flush` 并见 `Ok`——本端口
///    **不**逐条 fsync，批次/窗口由调用方决定（#106：缓冲接受不得描述成已持久
///    的 write-ahead 日志）。
/// 3. **恢复完备**：[`Self::restore`] 返回三态 [`Restore`]，调用方按其语义
///    重建当前态——`Empty` 取 [`Reducer::initial`] 零 apply；`FromInitial`
///    自 initial 起按序 apply 全量动作；`FromSnapshot` 自快照态起按序
///    apply 增量动作。重放协议：动作流**经同一装配的 store 逐条
///    [`Store::dispatch`]**（效果环照常运转）——[`Effect::Emit`] 回灌等派生
///    动作由排空环再生，终态与 live 收敛；裸 reduce 逐条套用会丢派生链
///    （挂点矩阵与收敛回归见实现方 `skeleton_infra::journal`）。
/// 4. **fail-visible**：持久化失败如实 `Err`，禁静默丢弃——静默丢 action =
///    真相源丢史。
/// 5. **单写者**：本端口不设跨进程锁；多进程挂载同一账本的编排归上层。
///
/// # 与实现层 `Journal` trait 的分工
///
/// 实现层全功能面 trait `skeleton_infra::journal::Journal` 的方法名带实现层
/// 词形（`append_action`/`restore_stream`/`append_snapshot`；`replay` 为实现层
/// 独有），本端口独占契约词表（`append`/`flush`/`restore`/`persist_snapshot`）
/// ——两 trait 同屏 import 直呼无歧义（旧三同名双候选拒编译已结构性消灭，批
/// 2A）。唯一同名项 `flush` 是参考实现的**固有方法**（非 trait 方法）：方法
/// 调用语法下固有方法优先且语义相同（端口实现直接转调），UFCS
/// `JournalPort::flush(&mut j)` 亦无第二候选。
/// 状态面同款关联类型化（批 6A，与实现层同步）：[`JournalPort::State`] 被
/// `Self` 钉死，端口方法直呼零 turbofish 零绑定标注（`j.append(&a)` /
/// `let r = j.restore()`）。
///
/// # Examples
///
/// 后端自换即实现本 trait——一个内存账本（无任何序列化依赖）同样合法；它的
/// `flush` 只能声明**进程内**语义（崩溃即全丢），不得声称 OS 持久：
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
///     fn flush(&mut self) -> Result<(), Self::Err> {
///         Ok(()) // 进程内账本：记录已在 Vec 里可见，没有更外层的持久位
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
/// j.flush().unwrap(); // 持久化位：进程内语义=已接受即可见
/// assert_eq!(j.restore().unwrap(), Restore::FromInitial(vec!["inc".to_string()]));
/// ```
pub trait JournalPort<A> {
    /// 持久化失败类型（实现方定义；`Err` ⇒ 契约①生效：调用方禁 apply 该 action）。
    ///
    /// 没有持久化位的后端把 [`FlushUnsupported`] 内嵌进本类型，从
    /// [`Self::flush`] 显式返回它。
    type Err;
    /// 账本状态面（快照与恢复语义的载荷类型；实现方定义）。
    type State;
    /// 动作追加（真相源的增量单位）。`Ok` = 记录已被本实现接受（参考实现=
    /// 进入写缓冲），**不是**已持久（契约①）。
    ///
    /// payload 契约：`A` 序列化结果**必须是 JSON object**（实现层按
    /// `#[serde(tag)]` 事件行校验）——unit variant 序列化为裸字符串，
    /// 运行时 `Err`（fail-visible）；给 enum 挂 `#[serde(tag = "t")]` 即免。
    fn append(&mut self, action: &A) -> Result<(), Self::Err>;
    /// 持久化位：把此前 [`Self::append`] 已接受的记录推过实现的持久化边界
    /// （参考实现=`BufWriter` flush + `sync_all`，掉电真持久）。
    ///
    /// `Ok` ⇒ 这些记录已到达实现的持久化边界（参考实现=崩溃后重开仍可读到；
    /// 边界由实现方文档声明：进程内后端只有进程内语义，**不得**声称 OS 持久）。
    /// `Err` ⇒ fail-visible，调用方不得把依赖该批次的动作当作已持久。
    ///
    /// **刻意没有默认实现**：默认 `Ok(())` 会让没有持久化位的后端静默谎报持久
    /// （#106）。每个实现必须显式表态——真持久、进程内语义，或返回内嵌
    /// [`FlushUnsupported`] 的 typed 错误。
    fn flush(&mut self) -> Result<(), Self::Err>;
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

    /// #106 finding 1：没有持久化位的后端必须从 `flush` 返回 typed
    /// [`FlushUnsupported`]，而不是靠默认实现返回 `Ok` 静默谎报持久——
    /// `JournalPort::flush` 无默认实现正是为此（删掉 trait 里的方法声明本测
    /// 即编译失败，不再有「不实现也能编译且说谎」的形状）。
    #[test]
    fn journal_port_without_durability_flushes_via_typed_unsupported() {
        struct NoDurability;

        impl JournalPort<u8> for NoDurability {
            type State = u8;
            type Err = FlushUnsupported;

            fn append(&mut self, _action: &u8) -> Result<(), Self::Err> {
                Ok(())
            }

            fn flush(&mut self) -> Result<(), Self::Err> {
                Err(FlushUnsupported)
            }

            fn persist_snapshot(&mut self, _state: &u8) -> Result<(), Self::Err> {
                Ok(())
            }

            fn restore(&self) -> Result<Restore<u8, u8>, Self::Err> {
                Ok(Restore::Empty)
            }
        }

        let mut j = NoDurability;
        assert_eq!(j.append(&1), Ok(()), "append 仍可缓冲接受");
        assert_eq!(j.flush(), Err(FlushUnsupported), "无持久位=typed 拒绝");
        assert_eq!(
            FlushUnsupported.to_string(),
            "journal backend has no flush/durability boundary"
        );
    }

    /// 有界暂存（ADR-0011 §2.1）：条数到本段上限后拒绝继续插入并置溢出标志——
    /// `push` 签名不变，但暂存条数**不可能**超过上限。判别：把上限检查去掉即红
    /// （第 4 条入缓冲、`overflowed()` 为假）。
    #[test]
    fn effect_buf_refuses_pushes_past_the_effect_cap() {
        let mut buf: EffectBuf<Tick, Infallible> = EffectBuf::new();
        buf.set_effect_cap(3);
        for _ in 0..3 {
            buf.push(Effect::Emit(Tick::Tick));
        }
        assert!(!buf.overflowed(), "到界但未超界：尚未溢出");
        assert_eq!(buf.effects.len(), 3);
        buf.push(Effect::Emit(Tick::Tick));
        assert!(buf.overflowed(), "第 4 条被拒 ⇒ 置溢出标志");
        assert_eq!(buf.effects.len(), 3, "超界插入不得增长暂存条数");
        // 溢出后继续 push 不增长、不 panic。
        for _ in 0..8 {
            buf.push(Effect::Emit(Tick::Tick));
        }
        assert_eq!(buf.effects.len(), 3);
    }

    /// `0` = 不限；`clear` 复位溢出标志（实现面复用缓冲前必清零）；上限可按拍重设
    /// 且**只影响之后的插入**。判别：`clear` 不清标志（复用后立刻再判溢出）即红。
    #[test]
    fn effect_buf_cap_zero_is_unbounded_and_clear_resets_overflow() {
        let mut buf: EffectBuf<Tick, Infallible> = EffectBuf::new();
        for _ in 0..64 {
            buf.push(Effect::Emit(Tick::Tick));
        }
        assert!(!buf.overflowed(), "cap 0 = 不限");
        assert_eq!(buf.effects.len(), 64);

        buf.clear();
        buf.set_effect_cap(2);
        for _ in 0..5 {
            buf.push(Effect::Emit(Tick::Tick));
        }
        assert!(buf.overflowed());
        assert_eq!(buf.effects.len(), 2);

        buf.clear();
        assert!(!buf.overflowed(), "clear 必须复位溢出标志");
        assert!(buf.is_empty());
        // 上一拍的上限随下一拍重设变化（不得沿用到新上限之外）。
        buf.set_effect_cap(0);
        for _ in 0..8 {
            buf.push(Effect::Emit(Tick::Tick));
        }
        assert!(!buf.overflowed());
        assert_eq!(buf.effects.len(), 8);
    }

    /// 批量入口（issue#237）：一次性快照剩余额度后成批追加，循环内不再逐条
    /// 判断预算。少产出正常结束、恰好到界不溢出、超一条 ⇒ 置溢出且**只写入
    /// 快照额度条**（超出项不入暂存）。判别：把 `for _ in 0..remaining` 的
    /// 上界改成 `remaining + 1` 即红。
    #[test]
    fn effect_buf_push_batch_writes_at_most_the_remaining_quota() {
        // 少产出（2 < 5）：正常结束、不溢出。
        let mut buf: EffectBuf<Tick, Infallible> = EffectBuf::new();
        buf.set_effect_cap(5);
        buf.push_batch((0..2).map(|_| Effect::Emit(Tick::Tick)));
        assert_eq!(buf.effects.len(), 2);
        assert!(!buf.overflowed(), "少产出不是溢出");

        // 恰好到界：边界额外探测一次取不到 ⇒ 不置位。
        let mut buf: EffectBuf<Tick, Infallible> = EffectBuf::new();
        buf.set_effect_cap(3);
        buf.push_batch((0..3).map(|_| Effect::Emit(Tick::Tick)));
        assert_eq!(buf.effects.len(), 3);
        assert!(!buf.overflowed(), "恰好到界但未超界：不溢出");

        // 超一条：置溢出，且只写入 3 条（超出项不写入）。
        let mut buf: EffectBuf<Tick, Infallible> = EffectBuf::new();
        buf.set_effect_cap(3);
        buf.push_batch((0..4).map(|_| Effect::Emit(Tick::Tick)));
        assert!(buf.overflowed(), "额度用尽后仍能产出一条 ⇒ 溢出");
        assert_eq!(buf.effects.len(), 3, "超出项不得写入暂存");
    }

    /// 剩余额度读侧：`None` = 不限、`Some(n)` = 还可接纳 n 条、`Some(0)` =
    /// **精确零额度**——不得与公共约定「`set_effect_cap(0)` = 不限」混淆。
    #[test]
    fn effect_buf_remaining_limit_distinguishes_unlimited_from_zero() {
        let mut buf: EffectBuf<Tick, Infallible> = EffectBuf::new();
        assert_eq!(buf.remaining_limit(), None, "不限额度必须是 None");
        buf.set_effect_cap(2);
        assert_eq!(buf.remaining_limit(), Some(2));
        buf.push(Effect::Emit(Tick::Tick));
        assert_eq!(buf.remaining_limit(), Some(1));
        buf.push(Effect::Emit(Tick::Tick));
        assert_eq!(buf.remaining_limit(), Some(0), "额度用尽 = 精确零额度");
        // 精确零额度下批量入口一条也不写，且仍能判出溢出。
        buf.push_batch((0..1).map(|_| Effect::Emit(Tick::Tick)));
        assert!(buf.overflowed());
        assert_eq!(buf.effects.len(), 2);
    }

    /// 精确零额度：空迭代器正常结束不溢出；非空迭代器一条也写不进 ⇒ 溢出。
    #[test]
    fn effect_buf_push_batch_with_zero_remaining_writes_nothing() {
        let mut buf: EffectBuf<Tick, Infallible> = EffectBuf::new();
        buf.set_effect_cap(1);
        buf.push(Effect::Emit(Tick::Tick));
        assert_eq!(buf.remaining_limit(), Some(0));
        buf.push_batch(core::iter::empty::<Effect<Tick, Infallible>>());
        assert!(!buf.overflowed(), "空批次不是溢出");
        buf.push_batch(core::iter::once(Effect::Emit(Tick::Tick)));
        assert!(buf.overflowed(), "零额度下仍有产出 ⇒ 溢出");
        assert_eq!(buf.effects.len(), 1);
    }

    /// 不限额度：全部写入、不溢出（批量入口走 `extend`，无边界判断）。
    #[test]
    fn effect_buf_push_batch_unlimited_writes_everything() {
        let mut buf: EffectBuf<Tick, Infallible> = EffectBuf::new();
        buf.push_batch((0..1000).map(|_| Effect::Emit(Tick::Tick)));
        assert_eq!(buf.effects.len(), 1000);
        assert!(!buf.overflowed());
    }

    /// `size_hint` 只作 `reserve` 提示、**不是**安全证明：谎报过多不能据此判
    /// 溢出；谎报过少不得突破上限。
    #[test]
    fn effect_buf_push_batch_does_not_trust_size_hint() {
        struct Lying {
            inner: std::vec::IntoIter<Effect<Tick, Infallible>>,
            hint: (usize, Option<usize>),
        }
        impl Iterator for Lying {
            type Item = Effect<Tick, Infallible>;
            fn next(&mut self) -> Option<Self::Item> {
                self.inner.next()
            }
            fn size_hint(&self) -> (usize, Option<usize>) {
                self.hint
            }
        }

        // 谎报下界 1000 而实际只产 2 条：不溢出、写 2 条。
        let mut buf: EffectBuf<Tick, Infallible> = EffectBuf::new();
        buf.set_effect_cap(5);
        buf.push_batch(Lying {
            inner: (0..2)
                .map(|_| Effect::Emit(Tick::Tick))
                .collect::<Vec<_>>()
                .into_iter(),
            hint: (1000, Some(1000)),
        });
        assert_eq!(buf.effects.len(), 2);
        assert!(!buf.overflowed(), "size_hint 过报不能当溢出证明");

        // 谎报下界 0 而实际产 8 条：仍严格受上限约束并判溢出。
        let mut buf: EffectBuf<Tick, Infallible> = EffectBuf::new();
        buf.set_effect_cap(3);
        buf.push_batch(Lying {
            inner: (0..8)
                .map(|_| Effect::Emit(Tick::Tick))
                .collect::<Vec<_>>()
                .into_iter(),
            hint: (0, None),
        });
        assert_eq!(buf.effects.len(), 3, "size_hint 少报不得突破上限");
        assert!(buf.overflowed());
    }

    /// 已溢出时批量入口直接短路：本段已注定整段拒绝，继续写入只是无谓工作。
    #[test]
    fn effect_buf_push_batch_short_circuits_after_overflow() {
        let mut buf: EffectBuf<Tick, Infallible> = EffectBuf::new();
        buf.set_effect_cap(2);
        buf.push_batch((0..5).map(|_| Effect::Emit(Tick::Tick)));
        assert!(buf.overflowed());
        assert_eq!(buf.effects.len(), 2);
        buf.push_batch((0..5).map(|_| Effect::Emit(Tick::Tick)));
        assert_eq!(buf.effects.len(), 2, "溢出后批量入口不得再写入");
    }

    /// 迭代器 panic 如实传播（不 catch）；已接受的前缀留在缓冲里，由实现方
    /// 既有的 reducer panic 路径整体清理本段 staging。
    #[test]
    fn effect_buf_push_batch_propagates_iterator_panic() {
        let mut buf: EffectBuf<Tick, Infallible> = EffectBuf::new();
        buf.set_effect_cap(8);
        let mut produced = 0;
        let iter = core::iter::from_fn(|| {
            produced += 1;
            if produced == 3 {
                panic!("iterator blew up");
            }
            Some(Effect::Emit(Tick::Tick))
        });
        let prev_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            buf.push_batch(iter);
        }));
        std::panic::set_hook(prev_hook);
        assert!(
            caught.is_err(),
            "迭代器 panic 必须如实传播（禁 catch_unwind）"
        );
        assert_eq!(buf.effects.len(), 2, "panic 前已接受的前缀留在缓冲");
        assert!(!buf.overflowed(), "panic 不是溢出");
    }

    /// 组合链预算租约（#235 / ADR-0011 §2.2）：借出期间父缓冲不可写；每转交一条
    /// 归还一个 credit ⇒ **任一时刻「父条数 + 子存活条数」≤ 总预算**（fan-out 不得
    /// 借用子缓冲尚未释放的槽位）。判别：去掉 `release_one` 的 sync、或让父上限
    /// 不收缩 ⇒ 本测红。
    #[test]
    fn staging_lease_keeps_parent_and_child_within_the_budget() {
        const B: usize = 8;
        let mut parent: EffectBuf<Tick, Infallible> = EffectBuf::new();
        parent.set_effect_cap(B);
        for _ in 0..3 {
            parent.push(Effect::Emit(Tick::Tick)); // p = 3
        }

        let mut lease = parent.reserve_all_remaining();
        assert_eq!(lease.capacity(), Some(5), "借出 = 剩余额度 B - p");
        let mut child: EffectBuf<Tick, Infallible> = EffectBuf::new();
        child.set_staging_cap(lease.capacity());
        child.push(Effect::Emit(Tick::Tick));
        child.push(Effect::Emit(Tick::Tick)); // k = 2
        lease.release_unused(child.len());

        // 每次 mapper 调用前记录（父条数, 父剩余额度, 子仍存活条数），fan-out 出 2 条。
        let total_child = child.len();
        let mut popped = 0usize;
        let mut observed = Vec::new();
        lease.transfer_from(&mut child, |_effect, out| {
            popped += 1;
            observed.push((out.len(), out.remaining_limit(), total_child - popped));
            for _ in 0..2 {
                out.push(Effect::Emit(Tick::Tick));
            }
        });
        assert_eq!(
            observed,
            vec![(3, Some(4), 1), (5, Some(3), 0)],
            "父上限必须恒为「B − 未归还 credits」"
        );
        for (parent_len, _remaining, child_left) in &observed {
            assert!(
                parent_len + child_left <= B,
                "全链 live staging 超预算：父 {parent_len} + 子 {child_left} > {B}"
            );
        }
        drop(lease);
        // 归还后上限恢复：父可继续写到总预算。
        while parent.len() < B {
            parent.push(Effect::Emit(Tick::Tick));
        }
        assert_eq!(parent.len(), B);
        assert!(!parent.overflowed(), "未超界不得置溢出");
    }

    /// 租约是 RAII：panic 展开时 `Drop` 必须归还 credits 并恢复原上限（否则缓冲
    /// 被永久缩小，后续 reduce 静默少写效果）。
    #[test]
    fn staging_lease_drop_restores_cap_on_panic() {
        let mut parent: EffectBuf<Tick, Infallible> = EffectBuf::new();
        parent.set_effect_cap(4);
        parent.push(Effect::Emit(Tick::Tick));
        let prev_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _lease = parent.reserve_all_remaining();
            panic!("mapper blew up");
        }));
        std::panic::set_hook(prev_hook);
        assert!(caught.is_err());
        for _ in 0..3 {
            parent.push(Effect::Emit(Tick::Tick));
        }
        assert_eq!(parent.len(), 4, "上限必须已恢复为原值");
        assert!(!parent.overflowed());
        parent.push(Effect::Emit(Tick::Tick));
        assert!(parent.overflowed(), "恢复后仍受原上限约束");
    }

    /// 不限额度时租约是空操作：子缓冲同样不限、全部转交。
    #[test]
    fn staging_lease_is_a_noop_when_unlimited() {
        let mut parent: EffectBuf<Tick, Infallible> = EffectBuf::new();
        let mut lease = parent.reserve_all_remaining();
        assert_eq!(lease.capacity(), None, "不限额度 → None（绝不是 Some(0)）");
        let mut child: EffectBuf<Tick, Infallible> = EffectBuf::new();
        child.set_staging_cap(lease.capacity());
        child.push_batch((0..100).map(|_| Effect::Emit(Tick::Tick)));
        lease.release_unused(child.len());
        lease.transfer_from(&mut child, |effect, out| out.push(effect));
        drop(lease);
        assert_eq!(parent.len(), 100);
        assert!(!parent.overflowed());
    }

    /// 精确零额度：父已用满 ⇒ 子上限 `Some(0)`（**不是**「不限」）；子一 push 即
    /// 溢出，转交时把溢出事实沿父链传回 ⇒ 整段由框架拒绝，不静默截断。
    #[test]
    fn staging_lease_exact_zero_propagates_child_overflow() {
        let mut parent: EffectBuf<Tick, Infallible> = EffectBuf::new();
        parent.set_effect_cap(2);
        parent.push(Effect::Emit(Tick::Tick));
        parent.push(Effect::Emit(Tick::Tick));
        let mut lease = parent.reserve_all_remaining();
        assert_eq!(lease.capacity(), Some(0), "用满 = 精确零额度");
        let mut child: EffectBuf<Tick, Infallible> = EffectBuf::new();
        child.set_staging_cap(lease.capacity());
        child.push(Effect::Emit(Tick::Tick));
        assert!(
            child.overflowed(),
            "零额度下 push 必须被拒（不得退化成不限）"
        );
        lease.release_unused(child.len());
        lease.transfer_from(&mut child, |effect, out| out.push(effect));
        drop(lease);
        assert!(parent.overflowed(), "子缓冲溢出必须沿父链传回");
        assert_eq!(parent.len(), 2, "被拒的效果一条也没进父缓冲");
    }
}
