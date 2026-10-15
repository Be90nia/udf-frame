//! 最小单向数据流（UDF）骨架核心——五件套 signature 级壳（壳+签名+最小环接线）。
//!
//! 零依赖纪律：本 crate 无 \[dependencies\]——核心无 IO/运行时依赖，reducer「禁 IO」
//! 从评审约束升级为编译期不可能。
//! 已落功能：`combine` 组合器与 `TestStore` 测试面；待填余项：`combine!`/`binding!`
//! 宏、异步 EffectRuntime、serde Action（可回放）。

use std::cell::{Cell, RefCell, UnsafeCell};
use std::collections::VecDeque;
pub use std::convert::Infallible;
use std::marker::PhantomData;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};

/// std Mutex 取锁：中毒 = 持锁方已 panic、状态不可信——按 fail-fast 纪律传播
/// panic，禁静默吞掉毒锁继续跑。
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().expect(
        "skeleton Store state lock poisoned（S::Clone/守卫 panic 违约——实例整体报废且状态不可取出，见 Store 文档「Panic 契约」节）: prior panic while holding the lock",
    )
}

/// State：值语义只读快照。契约：禁内部可变性（无 Mutex/Cell 字段）、
/// 非法状态进类型（互斥态用嵌套 enum，禁 bool 旗子）。
///
/// [`Reducer`] 是一个 Feature 唯一的状态迁移函数：同 `(state, action)` 恒产同
/// 输出（确定性）；IO 在结构上不可触——本 crate 无 IO 依赖。
pub trait Reducer {
    /// **`Clone` 必须 panic-free**：外部持有快照时状态落位走
    /// `Arc::make_mut` 克隆分支，`S::clone` 在 state 锁内执行——clone panic
    /// 即毒锁，实例整体报废（见 [`Store`] 文档「Panic 契约」节）。
    type State: Clone + Send;
    /// enum；serde feature 下要求 Serialize + Deserialize（规划中）
    type Action: Send;
    /// 副作用词表（宿主定义的描述式 enum）；零副作用 = [`Infallible`]。
    /// Rust 稳定版无关联类型缺省，零效果 Feature 显式写 `type Op = Infallible;`。
    type Op;
    /// 根状态构造：完整根状态的唯一合法来源（[`Store::new`] 装配时调用一次）。
    /// [`combine`] 产物没有可用实现（恒 panic）——根状态由父 Feature 的
    /// `initial` 显式构造（含子切片初值），分片只承载 reduce 委托。
    // `#[track_caller]`：分片误用作根的
    // panic 经 [`Store::new`] 转发指采用方装配行，禁指库内 panic 站。
    #[track_caller]
    fn initial(&self) -> Self::State;
    /// 纯函数：禁 IO/时钟/随机/写共享态（结构性：本 crate 无此依赖）。
    /// 效果写入 `out`（写入序 = 转发序），状态经返回值给出（效果出参化：
    /// `EffectBuf` 复用缓冲 clear 保容量，效果路径稳态零新增分配——替代
    /// `-> Vec<Effect>` 每 dispatch 一次堆分配的旧形态）。
    fn reduce(
        &self,
        state: &Self::State,
        action: Self::Action,
        out: &mut EffectBuf<Self::Action, Self::Op>,
    ) -> Self::State;
}

/// 效果句柄：reducer 按语义指派（同义恒同值），解释器寻址用（[`Effect::Cancel`]）。
/// 值语义小句柄（Copy+Eq+Hash）：TestStore 效果断言、`HashMap`/`HashSet` 键等
/// 宿主寻址表都用值比对。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EffectId(pub u64);

/// Effect：纯描述值。解释器（宿主适配层）执行后经回灌通道把 Action 重入 store（环闭合）。
/// Debug 供 [`TestStore::receive`] 断言失败消息如实回放实际效果。
#[derive(Debug)]
pub enum Effect<A, Op = Infallible> {
    /// 零副作用回灌：环闭合/表单回写的唯一形态
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

/// 效果缓冲：reducer 的效果出参。复用 [`Vec`]
/// 包装，排空循环 clear 保容量、跨 dispatch 复用同一常驻缓冲——效果路径稳态
/// 零新增分配（快照本体分配已由 make_mut CoW 消除：稳态原地
/// 写入零 malloc，全路径稳态零新增分配）。
pub struct EffectBuf<A, Op = Infallible> {
    effects: Vec<Effect<A, Op>>,
}

impl<A, Op> EffectBuf<A, Op> {
    /// 空缓冲（排空循环构造一次，此后 clear 复用）。
    pub fn new() -> Self {
        Self { effects: Vec::new() }
    }

    /// reducer 写入一条效果；写入顺序 = 解释器转发顺序。
    pub fn push(&mut self, effect: Effect<A, Op>) {
        self.effects.push(effect);
    }

    /// 清空保容量：容量留给下一次 reduce 复用（稳态零新增分配的前提）。
    pub fn clear(&mut self) {
        self.effects.clear();
    }

    /// 缓冲是否为空。
    pub fn is_empty(&self) -> bool {
        self.effects.is_empty()
    }
}

impl<A, Op> Default for EffectBuf<A, Op> {
    fn default() -> Self {
        Self::new()
    }
}

/// 效果运行时：核心只转发，永不解释。同步实现立即 `back(a)`；异步实现（适配层）
/// spawn 后持句柄回灌。
///
/// `run` 的 panic 如实传播（fail-visible），但 runtime 锁由核心恢复+计数
/// （[`Store::runtime_panic_count`]），实例不报废——panic 后 `self` 的一致性
/// 由实现自证（核心不为中断在任意中间态的解释器兜底）。
///
/// # 回灌环责任边界
///
/// `back.send` 与 dispatch 同栈（同步实现）时计入回灌深度预算
/// （[`Store::feedback_budget_exceeded`]，64 层止步）——自指回灌环有界返回。
/// 异步实现 spawn 后从他线程回灌：那不是 dispatch 调用栈的一部分，按外部流量
/// 计（深度清零）——跨线程回灌环的环防护责任在解释器自身，核心无从关联无栈
/// 联系的入队。
pub trait EffectRuntime<A, Op> {
    /// 解释执行一条效果。只收 [`Effect::Run`]/[`Effect::Cancel`]（[`Effect::Emit`]
    /// 由 store 核心直接回灌入队，不经此处）；转发顺序 = reducer 在 [`EffectBuf`]
    /// 的写入顺序。`back` 是回灌句柄：异步实现 spawn 后持它把 Action 重入队列。
    fn run(&mut self, effect: Effect<A, Op>, back: &ActionSender<A>);
}

/// 零效果程序（`Op = Infallible`）的缺省解释器：`Effect<A, Infallible>` 无
/// `Run` 形可构造，本实现至多收到 `Cancel`——语义即「无注册，空转」
/// （[`Effect::Cancel`]），丢弃即正确。消灭手写含静态死分支的空运行时样板。
/// 不提供跨 `Op` 泛化：非零效果词表下静默丢弃 `Run` 是危险的缺省行为。
///
/// # Examples
///
/// ```
/// use skeleton::{EffectBuf, Infallible, NoFx, Reducer, Store};
///
/// struct Counter;
///
/// impl Reducer for Counter {
///     type State = u32;
///     type Action = u32;
///     type Op = Infallible;
///     fn initial(&self) -> u32 { 0 }
///     fn reduce(&self, s: &u32, a: u32, _out: &mut EffectBuf<u32, Infallible>) -> u32 {
///         s + a
///     }
/// }
///
/// let store = Store::new(Counter, NoFx);
/// store.dispatch(2);
/// store.dispatch(3);
/// assert_eq!(*store.snapshot(), 5); // 同步排空：dispatch 返回即 idle
/// store.close();
/// ```
pub struct NoFx;

impl<A> EffectRuntime<A, Infallible> for NoFx {
    fn run(&mut self, _: Effect<A, Infallible>, _: &ActionSender<A>) {}
}

/// 可克隆回灌句柄：解释器异步回灌的唯一通道（[`EffectRuntime::run`] 的 `back`）。
/// 持入队+排空闭包（内部 Weak 指向 store，不构成引用环）；store 丢弃或已
/// [`Store::close`] 后 [`ActionSender::send`] 返回 `false`——停机窗显式化，
/// 禁静默丢弃（停机闸契约，对照表见 [`Store`] 类型文档）。
pub struct ActionSender<A> {
    push: Arc<dyn Fn(A) -> bool + Send + Sync>,
}

impl<A> Clone for ActionSender<A> {
    fn clone(&self) -> Self {
        Self { push: Arc::clone(&self.push) }
    }
}

impl<A> ActionSender<A> {
    /// 回灌 Action（入队并同步排空至 idle，与 [`Store::dispatch`] 同语义）。
    /// 返回 `false` = store 已丢弃或已 close，action 被显式拒绝（停机静默窗
    /// 收口；闭门拒绝计入 [`Store::rejected_after_close`]）。
    ///
    /// # Examples
    ///
    /// 停机窗显式化由类型层强制：裸调用（丢弃返回值）会被 `unused_must_use`
    /// lint 拦下——
    ///
    /// ```compile_fail
    /// # #![deny(unused_must_use)]
    /// # use skeleton::{EffectBuf, Infallible, NoFx, Reducer, Store};
    /// # struct Counter;
    /// # impl Reducer for Counter {
    /// #     type State = u32; type Action = u32; type Op = Infallible;
    /// #     fn initial(&self) -> u32 { 0 }
    /// #     fn reduce(&self, s: &u32, a: u32, _out: &mut EffectBuf<u32, Infallible>) -> u32 { s + a }
    /// # }
    /// # let store = Store::new(Counter, NoFx);
    /// # let sender = store.sender();
    /// # store.close();
    /// sender.send(42); // ERROR: unused `bool` that must be used——闭门拒绝禁静默丢弃
    /// ```
    ///
    /// 接收返回值即合规：
    ///
    /// ```
    /// # use skeleton::{EffectBuf, Infallible, NoFx, Reducer, Store};
    /// # struct Counter;
    /// # impl Reducer for Counter {
    /// #     type State = u32; type Action = u32; type Op = Infallible;
    /// #     fn initial(&self) -> u32 { 0 }
    /// #     fn reduce(&self, s: &u32, a: u32, _out: &mut EffectBuf<u32, Infallible>) -> u32 { s + a }
    /// # }
    /// # let store = Store::new(Counter, NoFx);
    /// # let sender = store.sender();
    /// # store.close();
    /// let accepted = sender.send(42);
    /// assert!(!accepted, "闭门后 send 必须显式 false");
    /// assert_eq!(store.rejected_after_close(), 1);
    /// ```
    #[must_use = "返回 false = action 被闭门拒绝（store 已 drop 或已 close）；静默丢弃将掩盖停机窗——接收返回值或 `let _ =` 显式弃置"]
    pub fn send(&self, action: A) -> bool {
        (self.push)(action)
    }
}

/// 构造回灌句柄：仅 [`Store::new`] 经
/// [`Arc::new_cyclic`] 调用一次，产物缓存为 [`StoreInner::back`]——`sender()`
/// 克隆复用、排空循环借用，禁每 action 新建 Arc 闭包（稳态零新增分配的构成部分）。
fn back_sender<S: Clone + Send + Sync + 'static, A: Send + 'static, Op: Send + 'static>(
    weak: &Weak<StoreInner<S, A, Op>>,
) -> ActionSender<A> {
    let weak = weak.clone();
    ActionSender {
        push: Arc::new(move |action| match weak.upgrade() {
            Some(inner) => {
                // 与 dispatch 同闸——闭门 send 显式 false 并计数；
                // push/夺权/首 pop 同临界区（锁消融，论证同 enqueue/dispatch）。
                // 同栈判定经 reentrant_origin——排空线程在 run 内回灌计深，
                // 他线程异步回灌=外部流量（环防护责任边界见 EffectRuntime::run）。
                let origin = reentrant_origin(inner.store_id);
                match enqueue(&inner, action, origin) {
                    Enqueue::Rejected => {
                        audit_emit(&inner, AuditEventKind::Rejected);
                        false
                    }
                    Enqueue::Queued => {
                        audit_enqueued(&inner, origin);
                        true
                    }
                    Enqueue::Draining(first) => {
                        audit_enqueued(&inner, origin);
                        drain_owned(&inner, first);
                        true
                    }
                }
            }
            None => false,
        }),
    }
}

/// 订阅回调闭包（私有别名收口 type_complexity，零公共面增量）
type SubscriberCallback<S> = Box<dyn FnMut(&S) + Send>;

struct SubscriptionSlot<S> {
    notify: Mutex<SubscriberCallback<S>>,
}

/// 排空权独占字段：仅持排空权（[`StoreInner::draining`] 旗互斥下的 drain 循环）
/// 触及的可变态。锁消融的载体——`effect_buf` 与订阅集快照缓存全 dispatch 生命
/// 周期内只有排空者读写（重入 dispatch 只入队即返，到不了这两处），互斥由排空
/// 旗外部同步，无需各自上锁：原 `Mutex` 的 lock/unlock（每 action 3 对）与毒锁
/// 面一并消融。
struct DrainExclusive<T> {
    value: UnsafeCell<T>,
}

// SAFETY: DrainExclusive<T> 的安全纪要是**外部同步**——所有 `get_mut` 调用点
// 必须位于排空权持有区间（drain 内 DrainGuard 活跃期）。排空旗 swap 互斥保证
// 同一时刻至多一个排空者，故 &mut 独占成立；T: Send 使该独占访问可跨线程交接。
unsafe impl<T: Send> Sync for DrainExclusive<T> {}

impl<T> DrainExclusive<T> {
    fn new(value: T) -> Self {
        Self { value: UnsafeCell::new(value) }
    }

    /// SAFETY: 调用方必须持排空权（drain 内，DrainGuard 未 disarm），且不得让
    /// 返回的 `&mut T` 活到排空权释放之后。重入路径（reducer/回调里 dispatch/
    /// send）只入队即返、到不了此处，无别名风险。
    // clippy::mut_from_ref：本函数的不安全**就是**设计本体——外部同步（排空旗
    // 互斥）替代互斥锁，见类型级文档。allow 收口在唯一不安全面上。
    #[allow(clippy::mut_from_ref)]
    unsafe fn get_mut(&self) -> &mut T {
        &mut *self.value.get()
    }
}

/// 订阅集快照缓存（锁消融：稳态 dispatch 对订阅广播零锁读）。
/// `version` 与 [`StoreInner::subs_version`] 对账：订阅增删/票据注销/回调 panic
/// 摘除都自增版本，排空者版本不匹配才冷重建（重建顺带代答回收死 Weak）；
/// 匹配则直接复用 `slots`——稳态（订阅集不变）单次 Acquire load + 遍历，零锁。
struct NotifyCache<S> {
    version: usize,
    slots: Vec<Arc<SubscriptionSlot<S>>>,
}

struct StoreInner<S, A, Op> {
    reducer: Arc<dyn Reducer<State = S, Action = A, Op = Op> + Send + Sync>,
    state: Mutex<Arc<S>>,
    runtime: Mutex<Box<dyn EffectRuntime<A, Op> + Send>>,
    queue: Mutex<VecDeque<(A, ActionOrigin)>>,
    /// 订阅集（Arc 包裹：票据 Drop 经共享句柄即时摘表）
    subs: Arc<Mutex<Vec<Weak<SubscriptionSlot<S>>>>>,
    /// 回灌句柄缓存：每 store 构造一次；`sender()` 克隆、排空循环借用。
    back: ActionSender<A>,
    /// 效果缓冲（锁消融修订）：reduce 出参，clear 保容量跨 dispatch 复用。
    /// 排空权独占（[`DrainExclusive`]）：原 `Mutex` 的 take/还 两对锁消融——
    /// reducer 期间不持任何锁的契约不变（UnsafeCell 无锁可毒），panic 路径
    /// 连容量都不再丢（缓冲留在原位，下次 clear 复用）。
    effect_buf: DrainExclusive<EffectBuf<A, Op>>,
    /// 订阅集快照缓存（锁消融修订，见 [`NotifyCache`]）：稳态零锁读，
    /// 冷路径（订阅集变更后的首个 dispatch）才锁 `subs` 重建+代答回收死 Weak。
    notify_cache: DrainExclusive<NotifyCache<S>>,
    /// 订阅集版本号：订阅增删（[`Store::subscribe`]）、票据注销（[`Subscription`] Drop）、
    /// 回调 panic 摘除三路自增，快照缓存失效依据。Arc 装载使注销票据持句柄可 bump。
    subs_version: Arc<AtomicUsize>,
    /// 本地订阅闸（[`Store::subscribe_local`]）：零本地订阅时通知路径一次
    /// relaxed 读即短路，不触达任何线程本地表。只作闸，不作精确计数读侧。
    local_subs: Arc<AtomicUsize>,
    /// store 身份号（本地订阅在注册线程 TLS 表中的键）：全局单调，进程内唯一。
    store_id: u64,
    /// 单写者排空守卫（确定性：同序 action 序列恒同终态；重入回灌只入队）
    draining: AtomicBool,
    /// 停机闸：置位后 push 路径（dispatch 与 sender().send 同闸）恒拒——
    /// dispatch no-op、send 显式 false，拒绝计数 fail-visible。Emit 回灌是已
    /// 接受 action 的内部延续（确定性迁移的收尾），不过闸。push
    /// 复检在 queue 锁内，置位与 push 先后经锁线性化——闭门后零静默执行。
    closed: AtomicBool,
    rejected_after_close: AtomicU64,
    /// 订阅回调 panic 摘除的成因留痕计数（摘除留痕+显式计数读侧可见）：`subscriber_count` 下跌只表明有人没了，本计数回答「为什么」。
    /// 毒订阅者摘除即 +1，已摘除者不重复计入。
    subscriber_panics: AtomicU64,
    /// 最近一次订阅回调 panic 的成因摘要：
    /// payload 为 `&str`/`String` 时存原文（200 字符截断），其余载荷类型不可
    /// 静态提取，存占位说明。只存最近一次；跨线程与本地两条摘除路径同一纪律。
    subscriber_panic_cause: Mutex<Option<String>>,
    /// 闭门后登记的订阅笔数：
    /// 登记照常、永不触发——本计数让该状态显形。与 [`StoreInner::rejected_after_close`]
    /// 分立：登记不是拒绝（票据有效、可注销、count 可查），并数会污染拒绝语义。
    subscriptions_after_close: AtomicU64,
    /// 解释器 panic 毒锁恢复计数：每次 into_inner 恢复自增（= 此前恰一次
    /// 解释器 panic，恢复后 clear_poison 复位毒位）。读侧 [`Store::runtime_panic_count`]。
    runtime_panics: AtomicU64,
    /// 回灌深度预算触发计数：单任排空连续回灌超 [`FEEDBACK_DEPTH_BUDGET`]
    /// 即停排+自增。读侧 [`Store::feedback_budget_exceeded`]。
    feedback_exceeded: AtomicU64,
    /// Emit 回灌累计重入计数：`Effect::Emit` 永不
    /// 进效果转发路径（回环以 Action 重入队列），本计数供 [`TestStore::receive`]
    /// 空队列时如实归因「本轮产过 N 个回灌 Emit」——区别于「真没产效果」。
    /// 白盒读侧（TestStore 同模块、同线程读），零公共面增量。
    feedback_emitted: AtomicU64,
    /// 审计事件流闸（时序事后取证面）：false = 默认关闭——emit 点一次
    /// Acquire load 即返，零分配零锁（dispatch 锚不回退的依据）。Release/Acquire
    /// 配对 subscribe_events 的开闸与票据 Drop 的落闸。
    audit_gate: Arc<AtomicBool>,
    /// 审计汇聚槽（唯一订阅点的单槽内容）：有界环 + 实时回调 + 订阅代次。
    /// None = 未订阅。锁内只做数据搬运、回调锁外调用（禁持锁重入）。
    audit: Arc<Mutex<Option<AuditSink>>>,
    /// 审计事件单调序号计数器（store 级，跨订阅窗不重复；空洞 = 环溢出证据）。
    audit_seq: AtomicU64,
    /// 审计订阅代次计数器（票据 Drop 对账键，防旧票据误杀后继订阅流）。
    audit_gen: AtomicU64,
    /// 测试专用：在途 dispatch 计数。0=无任何
    /// dispatch 在执行，此刻「draining=false 且队列非空」即孤儿 action（无人
    /// 排空、无人即将排空）。push 方先 +1 再入队、返回前 -1，使监视线程可用
    /// in_flight==0 排除「push 已入队、swap 未达」的正常瞬态窗。非 Drop 守卫：
    /// reducer panic 路径会残留计数，但仅影响同实例后续测试监视精度，零发布
    /// 产物开销。
    #[cfg(test)]
    in_flight: AtomicUsize,
}

/// Store：单写者队列 + 快照广播。`Op` 缺省 [`Infallible`] → [`Effect::Run`] 不可
/// 构造（[`Effect::Cancel`] 恒可构造、无注册时空转），零效果程序编译期证明无
/// 副作用（最小环直接配 [`NoFx`]，见其文档示例）。
///
/// # 闭门后的入口对照（停机闸）
///
/// | 入口 | 闭门后行为 | fail-visible 面 |
/// |---|---|---|
/// | [`Self::dispatch`] | 恒 no-op（签名不引入 Err） | 计入 [`Self::rejected_after_close`] |
/// | [`ActionSender::send`] | 返回 `false`（显式拒绝，`#[must_use]` 兜底） | 同一计数 |
/// | [`Self::subscribe`] / [`Self::subscribe_local`] | 照常登记、票据照常注销，但闭门后永无 dispatch 触发——通知静默（已知取舍，见下） | 计入 [`Self::subscriptions_after_close`]；`subscriber_count` 仍可查 |
///
/// dispatch 的静默 no-op 与 send 的显式 `false` 是**保签名**与**停机显式化**
/// 之间的显式取舍：dispatch 面向动作源（不感染调用面），send 面向异步回灌
/// （停机窗必须可观测）；两者闭门后共用同一拒绝计数，无观测死角。闭门
/// subscribe 照常登记是「票据即注销」模型的自然推论——通知静默可经
/// `subscriber_count` 交叉查证（已知语义，非缺陷）。
///
/// # Panic 契约
///
/// `S::Clone` 必须 panic-free：外部持有快照（强计数 ≥2）时，状态落位走
/// `Arc::make_mut` 克隆分支，`S::clone` 在 state 锁内执行——clone panic 即毒锁，
/// snapshot/dispatch 从此恒 panic，**实例整体报废且状态不可取出**。这是文档化
/// 显式契约而非可恢复路径：state 锁内「只剩 memcpy 与计数」的形态纪律不为用户
/// State 的 panic-free 性兜底。对照：`S::Drop` 的 panic 已由「旧快照锁外析构」
/// 排除（见 `drain_owned` 落位段）；解释器 panic 不报废实例（毒锁恢复+计数，
/// 见 [`Self::runtime_panic_count`]）。
pub struct Store<S, A, Op = Infallible> {
    inner: Arc<StoreInner<S, A, Op>>,
}

/// 回灌深度预算（const 64）：单任排空内连续处理的回灌链深度上限。
/// 有界链（Undo→Replay、表单回写）深度个位数；自指回灌环（reducer 恒 Emit
/// 自身、解释器经 [`ActionSender::send`] 同栈回灌）属用户程序病理，预算点
/// 止步+计数上浮（[`Store::feedback_budget_exceeded`]）。
const FEEDBACK_DEPTH_BUDGET: u64 = 64;

/// 入队来源标签：外部流量 = [`ActionOrigin::External`]（深度清零）；
/// 同栈回灌重入 = [`ActionOrigin::Feedback`]（携带入队时深度）。
///
/// 「同栈」判定不看调用者身份，只看 [`REENTRANT_DEPTH`] 线程局部：排空线程
/// 处理某 action 期间（store_id 匹配），该线程上发生的一切入队——Emit 回灌、
/// 解释器 `back.send`、订阅回调重入 dispatch——一律计深（预算按
/// dispatch 调用栈深度统一计数，不分来源标签，back 通道不再绕预算）；他线程
/// 或静止态的入队 = 外部流量，永不计深。
#[derive(Clone, Copy, PartialEq, Eq)]
enum ActionOrigin {
    External,
    /// 同栈回灌重入，值为入队时的回灌深度（= 产生本回灌的 action 深度 + 1）
    Feedback(u64),
}

thread_local! {
    /// 排空线程的在途回灌深度：键 = store_id，值 = 当前 action 处理期间
    /// 产生的回灌应计的深度（= 该 action 深度 + 1）。由 [`ReentrantDepthGuard`]
    /// 置位/恢复，嵌套排空（回调内 dispatch 另一 store）按栈配对。
    static REENTRANT_DEPTH: Cell<Option<(u64, u64)>> = const { Cell::new(None) };
}

/// 在途回灌深度守卫：进入某 action 处理前置位，迭代末（或 panic 栈展开）恢复
/// 原值——同线程嵌套排空按栈配对，TLS 不跨 action 泄漏。
struct ReentrantDepthGuard {
    prev: Option<(u64, u64)>,
}

impl ReentrantDepthGuard {
    fn enter(store_id: u64, feedback_depth: u64) -> Self {
        let prev = REENTRANT_DEPTH.with(|cell| cell.replace(Some((store_id, feedback_depth))));
        Self { prev }
    }
}

impl Drop for ReentrantDepthGuard {
    fn drop(&mut self) {
        REENTRANT_DEPTH.with(|cell| cell.set(self.prev.take()));
    }
}

/// 入队来源判定：本线程正在排空同 store → 同栈回灌（计深）；
/// 否则 → 外部流量（清零）。
fn reentrant_origin(store_id: u64) -> ActionOrigin {
    REENTRANT_DEPTH.with(|cell| match cell.get() {
        Some((id, depth)) if id == store_id => ActionOrigin::Feedback(depth),
        _ => ActionOrigin::External,
    })
}

/// push 三态结果（锁消融：push/夺权/首 pop 同临界区一次获取）。
enum Enqueue<A> {
    /// 闭门拒绝（已计入 [`Store::rejected_after_close`]）
    Rejected,
    /// 已入队，排空权归在任排空者（其循环必覆盖本 push，论证见 [`enqueue`]）
    Queued,
    /// 已入队且夺得排空权，`Some` 为同临界区内 pop 出的首个 action（带来源标签）
    Draining(Option<(A, ActionOrigin)>),
}

/// 共享 push 路径（[`Store::dispatch`] 与 [`ActionSender::send`] 同闸同窗）。
/// 临界区一次获取：push、排空权 swap、首 pop 在同一 queue 锁内完成——
/// 赢家路径免「push 锁→drain 入口锁」的二次加锁（-1 锁对/动作）。`origin`
/// 由调用方按 [`reentrant_origin`] 判定传入（dispatch 与 back 通道同一收口）。
///
/// 交接论证（M1 语义保持）：swap 在 queue 锁内判定——
/// - 输给在任排空者：其尚未到出口（出口=持锁 pop None 后放旗），本 push 或被其
///   后续循环 pop 覆盖，或在其持锁出口判定之后才入队（此时本线程 swap 读到的是
///   disarm 后的 false，走赢家路径自排空）。零孤儿。
/// - 赢家 pop_front 取的是队首（可能非本 action）：全局 FIFO 由单队列单排空者
///   保证，先入先出序不因合并而变。
fn enqueue<S: Clone + Send + Sync + 'static, A: Send + 'static, Op: Send + 'static>(
    inner: &Arc<StoreInner<S, A, Op>>,
    action: A,
    origin: ActionOrigin,
) -> Enqueue<A> {
    let mut queue = lock(&inner.queue);
    // 持 queue 锁后复检 closed——close 的 store 与本复检经 queue 锁
    // 线性化合拢：复检先于 store 则 close 的 drain 必覆盖本 push；后于 store 则
    // 显式拒绝计数。封死「过闸后在锁等待窗被 close 抢跑、闭门后静默执行」的
    // TOCTOU。序论证：closed 单调，stale false 是唯一可能的方向偏（锁+drain
    // 兜底，见 close 注释），Acquire 配 close 的 Release 即足。
    if inner.closed.load(Ordering::Acquire) {
        inner.rejected_after_close.fetch_add(1, Ordering::SeqCst);
        return Enqueue::Rejected;
    }
    queue.push_back((action, origin));
    // AcqRel：RMW 恒见修改序最新值；Acquire 侧同步前任 Release 放旗，Release 侧
    // 发布本任排空权。
    if inner.draining.swap(true, Ordering::AcqRel) {
        return Enqueue::Queued;
    }
    Enqueue::Draining(queue.pop_front())
}

// S: Sync：ActionSender 的回灌闭包跨线程共享（Send+Sync），Weak<StoreInner> 的
// Send/Sync 要求 StoreInner 整体 Send+Sync——其中 Mutex<Arc<S>> 传导出 S: Sync。
impl<S: Clone + Send + Sync + 'static, A: Send + 'static, Op: Send + 'static> Store<S, A, Op> {
    /// 装配最小运转环：`root` 是**根** Reducer（[`Reducer::initial`] 产出完整根
    /// 状态），`fx` 是效果解释器（核心只转发，永不解释）。
    ///
    /// 与 [`combine`] 的关系：`combine` 产物是**状态分片不是根**（无合法
    /// `initial`），不能直接作 `root` 传入——分片作根会在装配首次 `initial()`
    /// 即 fail-closed panic（组合根误用显式崩溃）。正确用法：父 Feature
    /// 的 `initial` 显式构造含子切片初值的根状态、`reduce` 内委托分片，父 Feature
    /// 本体作 `root`（完整形态见 `tests/combine_teststore.rs` 固定装置）。
    ///
    /// 构造即接线：回灌句柄经 `Arc::new_cyclic` 缓存（[`Store::sender`] 克隆复用），
    /// 初始状态 = `root.initial()`；此后 [`Store::dispatch`] / `sender().send` 驱动。
    // `#[track_caller]`：把采用方装配行转发给 `Reducer::initial` 的 panic 位置
    // （分片误用作根，O 小项）。
    #[track_caller]
    pub fn new(
        root: impl Reducer<State = S, Action = A, Op = Op> + Send + Sync + 'static,
        fx: impl EffectRuntime<A, Op> + Send + 'static,
    ) -> Self {
        let initial = root.initial();
        // new_cyclic：back 缓存句柄持 Weak 指回 inner，环构造一步完成
        let inner = Arc::new_cyclic(|weak| StoreInner {
            reducer: Arc::new(root),
            state: Mutex::new(Arc::new(initial)),
            runtime: Mutex::new(Box::new(fx)),
            queue: Mutex::new(VecDeque::new()),
            subs: Arc::new(Mutex::new(Vec::new())),
            back: back_sender(weak),
            effect_buf: DrainExclusive::new(EffectBuf::new()),
            notify_cache: DrainExclusive::new(NotifyCache { version: 0, slots: Vec::new() }),
            subs_version: Arc::new(AtomicUsize::new(0)),
            local_subs: Arc::new(AtomicUsize::new(0)),
            store_id: NEXT_STORE_ID.fetch_add(1, Ordering::Relaxed),
            draining: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            rejected_after_close: AtomicU64::new(0),
            subscriber_panics: AtomicU64::new(0),
            subscriber_panic_cause: Mutex::new(None),
            subscriptions_after_close: AtomicU64::new(0),
            runtime_panics: AtomicU64::new(0),
            feedback_exceeded: AtomicU64::new(0),
            feedback_emitted: AtomicU64::new(0),
            audit_gate: Arc::new(AtomicBool::new(false)),
            audit: Arc::new(Mutex::new(None)),
            audit_seq: AtomicU64::new(0),
            audit_gen: AtomicU64::new(0),
            #[cfg(test)]
            in_flight: AtomicUsize::new(0),
        });
        Store { inner }
    }

    /// 唯一写入口；同步排空至 idle（同序 action 序列恒同终态）。
    /// 闭门后恒 no-op 并计入 [`Self::rejected_after_close`]，签名不变（取舍
    /// 对照 [`ActionSender::send`] 的显式 `false`——见类型文档「闭门后的
    /// 入口对照」表）。
    pub fn dispatch(&self, action: A) {
        // 闸序（锁消融批论证）：closed 单调 false→true，load Acquire——见
        // [`Store::close`] 的 Release 配对与锁内复检论证。
        if self.is_closed() {
            self.inner.rejected_after_close.fetch_add(1, Ordering::SeqCst);
            // 取证面：早闸拒绝与 enqueue 复检拒绝同一事件节点（两条 push
            // 路径 + 两个闸位的拒绝在时间线上同形）。
            audit_emit(&self.inner, AuditEventKind::Rejected);
            return;
        }
        #[cfg(test)]
        self.inner.in_flight.fetch_add(1, Ordering::SeqCst);
        let origin = reentrant_origin(self.inner.store_id);
        match enqueue(&self.inner, action, origin) {
            Enqueue::Rejected => audit_emit(&self.inner, AuditEventKind::Rejected),
            Enqueue::Queued => audit_enqueued(&self.inner, origin), // 在任排空者覆盖（交接论证见 enqueue）
            Enqueue::Draining(first) => {
                audit_enqueued(&self.inner, origin);
                drain_owned(&self.inner, first)
            }
        }
        #[cfg(test)]
        self.inner.in_flight.fetch_sub(1, Ordering::SeqCst);
    }

    /// 拉取式快照。
    pub fn snapshot(&self) -> Arc<S> {
        Arc::clone(&lock(&self.inner.state))
    }

    /// 订阅状态通知；Drop=注销，漏注销不可表达（订阅表存 Weak，票据丢弃即失效）。
    /// 「失效」的精确语义：票据 drop 后**即时生效**——表项即时摘除（广播可见性：
    /// 注销票据不再收到通知；计数读侧 [`Self::subscriber_count`] 实时判活、读数
    /// 立即回落，无一拍滞后，见其时序节）。
    /// 通知缓存失效依据：push 后 Release 自增订阅集版本——稳态广播零锁读
    /// （顺序：push 先于 bump；快照重建载入版本 v 即保证 ≥v 前的全部入表可见，
    /// 论证见 drain 通知段）。票据持版本句柄，注销时自增（见 [`Subscription`]）。
    ///
    /// # Examples
    ///
    /// 丢弃票据 = 立即注销（死订阅），类型层拦截（[`Subscription`] 恒
    /// `#[must_use]`）：
    ///
    /// ```compile_fail
    /// # #![deny(unused_must_use)]
    /// # use skeleton::{EffectBuf, Infallible, NoFx, Reducer, Store};
    /// # struct Counter;
    /// # impl Reducer for Counter {
    /// #     type State = u32; type Action = u32; type Op = Infallible;
    /// #     fn initial(&self) -> u32 { 0 }
    /// #     fn reduce(&self, s: &u32, a: u32, _out: &mut EffectBuf<u32, Infallible>) -> u32 { s + a }
    /// # }
    /// # let store = Store::new(Counter, NoFx);
    /// store.subscribe(|_| {}); // ERROR: unused `Subscription` that must be used——丢弃即死订阅
    /// ```
    ///
    /// 绑定持有即持续收通知：
    ///
    /// ```
    /// # use skeleton::{EffectBuf, Infallible, NoFx, Reducer, Store};
    /// # struct Counter;
    /// # impl Reducer for Counter {
    /// #     type State = u32; type Action = u32; type Op = Infallible;
    /// #     fn initial(&self) -> u32 { 0 }
    /// #     fn reduce(&self, s: &u32, a: u32, _out: &mut EffectBuf<u32, Infallible>) -> u32 { s + a }
    /// # }
    /// # let store = Store::new(Counter, NoFx);
    /// let _ticket = store.subscribe(|_| {}); // 绑定即持有，Drop 才注销
    /// store.dispatch(1);
    /// store.close();
    /// ```
    pub fn subscribe(&self, f: impl FnMut(&S) + Send + 'static) -> Subscription<S> {
        // 闭门登记显形（登记照常生效，但永无通知——见类型文档对照表）
        if self.is_closed() {
            self.inner.subscriptions_after_close.fetch_add(1, Ordering::SeqCst);
        }
        let slot = Arc::new(SubscriptionSlot { notify: Mutex::new(Box::new(f)) });
        lock(&self.inner.subs).push(Arc::downgrade(&slot));
        self.inner.subs_version.fetch_add(1, Ordering::Release);
        Subscription { _slot: slot, subs: Arc::clone(&self.inner.subs), _version: Arc::clone(&self.inner.subs_version) }
    }

    /// 单线程订阅：回调免 `Send` 约束——闭包体内可用 `Rc`/`RefCell` 等线程
    /// 本地设施（跨线程档逼着采用方 `Arc<AtomicU64>`/`Mutex` 的痛点在此收口）。
    /// 与 [`Self::subscribe`] 的三点差异：
    ///
    /// 1. **只在注册线程触发**：本地回调登记在注册线程的 thread-local 表，
    ///    store 侧从不持有闭包。他线程 dispatch 不触达本地订阅（其线程表中
    ///    无此 store）；注册线程的 dispatch / `sender().send` 正常触发；
    /// 2. **通知路径免锁免原子量**：回调经 thread-local 直达；store 侧只存一个
    ///    计数闸，零本地订阅时一次 relaxed 读即短路；
    /// 3. **不计入 [`Self::subscriber_count`]**（该计数只看跨线程订阅）；回调
    ///    panic 与跨线程订阅同一纪律：fail-closed 摘除 + [`Self::subscriber_panics`]
    ///    计数。
    ///
    /// 两 API 的关系：`subscribe` 是跨线程档（回调需 `Send`，槽位 `Mutex`
    /// 串行化）；本 API 是单线程档（回调免 `Send`，线程亲和即安全依据）。
    /// 闭门（[`Self::close`]）后登记照常、永不触发——与 `subscribe` 一致，
    /// 对照见 [`Store`] 类型文档「闭门后的入口对照」。同一 dispatch 内通知序：
    /// 本地订阅先行，跨线程订阅随后（确定性；本地闸先判读，零共享订阅不短路
    /// 本地通知）。
    ///
    /// 票据 [`LocalSubscription`]：Drop = 注销（从注册线程表摘除 + 计数回落）；
    /// 票据本身 `!Send + !Sync`（线程亲和的类型化表达），跨线程移动即编译错。
    /// 丢弃票据 = 立即注销（死订阅），类型层拦截（与 [`Self::subscribe`] 同款）：
    ///
    /// ```compile_fail
    /// # #![deny(unused_must_use)]
    /// # use skeleton::{EffectBuf, Infallible, NoFx, Reducer, Store};
    /// # struct Counter;
    /// # impl Reducer for Counter {
    /// #     type State = u32; type Action = u32; type Op = Infallible;
    /// #     fn initial(&self) -> u32 { 0 }
    /// #     fn reduce(&self, s: &u32, a: u32, _out: &mut EffectBuf<u32, Infallible>) -> u32 { s + a }
    /// # }
    /// # let store = Store::new(Counter, NoFx);
    /// store.subscribe_local(|_| {}); // ERROR: unused `LocalSubscription` that must be used——丢弃即死订阅
    /// ```
    pub fn subscribe_local(&self, f: impl FnMut(&S) + 'static) -> LocalSubscription {
        // 闭门登记显形（与 subscribe 同一纪律）
        if self.is_closed() {
            self.inner.subscriptions_after_close.fetch_add(1, Ordering::SeqCst);
        }
        let sub_id = NEXT_LOCAL_SUB_ID.fetch_add(1, Ordering::Relaxed);
        let store_id = self.inner.store_id;
        local_sub_register(store_id, sub_id, Box::new(LocalSlot { notify: Box::new(f) }));
        self.inner.local_subs.fetch_add(1, Ordering::Release);
        LocalSubscription {
            store_id,
            sub_id,
            count: Arc::clone(&self.inner.local_subs),
            _not_send: PhantomData,
        }
    }

    /// 活跃订阅者数（跨线程订阅；本地订阅不入此表，见 [`Self::subscribe_local`]）。
    /// 只读：不触发回收；订阅回调 panic 被摘除后计数可观测下降（成因另见
    /// [`Self::subscriber_panics`] 与 [`Self::subscriber_panic_last_cause`]）——
    /// 订阅者丢失不再纯静默。
    ///
    /// # 时序（如实）
    ///
    /// 读数即时精确：票据 drop 即时摘表（[`Subscription`] Drop 经共享订阅表
    /// 句柄 retain 摘除本表项），本读侧直查订阅表 Weak 判活——无
    /// 「下一次 dispatch 的一拍滞后」（旧文档曾声称滞后：旧实现依赖排空冷重建
    /// 代笔回收，通知快照缓存持强引用为死槽「续命」，实测证伪后修复为即时摘表；
    /// 「先 dispatch 后 drop 再读」序下滞后实测可复现，见
    /// tests/store_behavior_gaps.rs 的即时性回归）。
    pub fn subscriber_count(&self) -> usize {
        lock(&self.inner.subs)
            .iter()
            .filter(|w| w.upgrade().is_some())
            .count()
    }

    /// 可克隆回灌句柄（解释器异步回灌的唯一通道）。克隆 StoreInner 缓存句柄
    /// （Arc 引用计数），不重建闭包。
    pub fn sender(&self) -> ActionSender<A> {
        self.inner.back.clone()
    }

    /// 停机闸：置 closed 后同步排空至 idle（与宿主装配序「解释器 close→排空
    /// idle→flush→drop」对齐）——drop 不再静默弃队。
    /// 在途他线程 drain 持排空权时本调用交由其排至 idle（闭门后无新入队，其
    /// 循环必达空，自指回灌环受回灌深度预算止步），交接语义同 drain 出口协议。
    /// 幂等。闭门后 push 恒拒：dispatch no-op、send 显式 `false`、subscribe
    /// 照常登记但永不触发——三入口对照见类型文档「闭门后的入口对照」表。
    pub fn close(&self) {
        // Release 配对 push 路径的 Acquire 复检。线性化不靠原子序：close 的
        // drain 与 dispatch 的 push 在 queue 锁上串行——stale false（迟见的
        // 闭门）只会让 push 照常入队，随后 close 的 drain 必经锁覆盖该 push；
        // 反向（未见 store 即读到 true）单变量一致性上不可能。Release 只保证
        // 「读到 true 时闸前所有写可见」，单调闸下这是唯一的错误方向防护。
        self.inner.closed.store(true, Ordering::Release);
        drain(&self.inner);
    }

    /// 停机闸状态。
    pub fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::Acquire)
    }

    /// 闭门后拒绝计数（fail-visible）：dispatch 拒收与 send false 均计入。
    /// store 已丢弃后的 send false 不可计（计数器随实例消亡），停机窗语义不变。
    pub fn rejected_after_close(&self) -> u64 {
        self.inner.rejected_after_close.load(Ordering::SeqCst)
    }

    /// 未处理 action 的队列深（状态可查读侧）。
    /// 同步排空语义下 dispatch 返回即 idle（恒 0）；非 0 = 重入回灌在途或排空者
    /// 尚未夺权的瞬态窗。采样即时值（queue 锁内 len）。
    pub fn pending_actions(&self) -> usize {
        lock(&self.inner.queue).len()
    }

    /// 订阅回调 panic 摘除的累计次数（成因留痕读侧：`subscriber_count` 下跌
    /// 只表明订阅者消失，本计数回答「因 panic 被摘」）。摘除留痕
    /// +显式计数读侧可见的成因面；只读，不触发回收。
    pub fn subscriber_panics(&self) -> u64 {
        self.inner.subscriber_panics.load(Ordering::Acquire)
    }

    /// 最近一次订阅回调 panic 的成因摘要（成因面读侧；摘除计数见
    /// [`Self::subscriber_panics`]——计数回答「谁死了」，本读侧回答「为什么」）。
    /// `None` = 从无 panic 摘除；`Some` = payload 为 `&str`/`String` 的原文
    /// （200 字符截断）或非字符串载荷的占位说明。只保留最近一次，多次摘除以
    /// 最后为准；跨线程与本地订阅两条摘除路径同一纪律。
    pub fn subscriber_panic_last_cause(&self) -> Option<String> {
        lock(&self.inner.subscriber_panic_cause).clone()
    }

    /// 闭门后登记的订阅笔数：登记照常生效（票据可注销、
    /// [`Self::subscriber_count`] 可查）但闭门后永无 dispatch 触发——本计数让
    /// 该状态从观测死角显形。与 [`Self::rejected_after_close`] 分立：登记不是
    /// 拒绝，并数会污染拒绝语义（闭门对照见类型文档）。
    pub fn subscriptions_after_close(&self) -> u64 {
        self.inner.subscriptions_after_close.load(Ordering::SeqCst)
    }

    /// 解释器 panic 毒锁恢复计数（fail-visible 上浮）。每次 into_inner 恢复自增 = 此前恰
    /// 发生一次解释器 panic（毒位随即 clear_poison 复位，实例不报废）；读数不含
    /// 尚未恢复的最近一次 panic——它在下一次效果转发恢复时才计入。持续上升的
    /// 读数 = 解释器 `run` 存在 panic 路径，需在宿主侧修复。
    pub fn runtime_panic_count(&self) -> u64 {
        self.inner.runtime_panics.load(Ordering::SeqCst)
    }

    /// 回灌深度预算触发计数（fail-visible 上浮）：单任排空内连续处理的
    /// 回灌链深度超过 `FEEDBACK_DEPTH_BUDGET`（64）即停排并自增。触发点回灌
    /// action 原值退回队尾（零丢失），本任 dispatch 随即返回——非 panic 非静默
    /// no-op。持续上升的读数 = 用户程序存在自指回灌环（reducer 恒 Emit 自身、
    /// 解释器经 `back.send` 同栈回灌），需在宿主侧修复；读数不升的正常流量不受
    /// 预算影响（外部 action 深度恒 0）。store drop 时若队列仍滞留（预算止步
    /// 残留），弃队数一并计入本计数留痕（禁静默弃队）。
    pub fn feedback_budget_exceeded(&self) -> u64 {
        self.inner.feedback_exceeded.load(Ordering::SeqCst)
    }

    /// 唯一订阅点：开启审计事件流。默认关闭零开销；
    /// 本调用是事件流的唯一开关（再次订阅覆盖前订，单槽）。
    ///
    /// - `capacity`：有界环容量（0 视作 1）。溢出丢最旧——最新生命周期优先；
    ///   丢失无新计数，`seq` 空洞即显式证据（见 [`AuditEvent`]）。
    /// - `on_event`：实时回调（每事件锁外调用一次；纯环录模式传 `|_| {}`）。
    ///   回调 panic = fail-closed 摘除（环照录、dispatch 行为不变——观测面禁
    ///   改变被观测者），成因计入 [`Self::subscriber_panics`] /
    ///   [`Self::subscriber_panic_last_cause`]。回调内重入 dispatch/send 的
    ///   事件只录环不再回调（TLS 在途标记封自指递归）。
    /// - 票据 Drop = 关闭；被覆盖的旧票据 Drop 凭代次对账，禁误杀后继流。
    ///
    /// # Examples
    ///
    /// 一笔 dispatch 的生命周期从事件流重建（取证锚）：
    ///
    /// ```
    /// use skeleton::{AuditEventKind, Effect, EffectBuf, NoFx, Reducer, Store};
    ///
    /// struct PingOnce;
    /// impl Reducer for PingOnce {
    ///     type State = u32;
    ///     type Action = u32;
    ///     type Op = skeleton::Infallible;
    ///     fn initial(&self) -> u32 { 0 }
    ///     fn reduce(&self, s: &u32, a: u32, out: &mut EffectBuf<u32>) -> u32 {
    ///         let n = s + a;
    ///         if n == 8 { out.push(Effect::Emit(4)); } // 单次回灌
    ///         n
    ///     }
    /// }
    ///
    /// let store = Store::new(PingOnce, NoFx);
    /// let _ticket = store.subscribe_events(64, |_| {});
    /// store.dispatch(8);
    /// let kinds: Vec<_> = store.take_events().iter().map(|e| e.kind).collect();
    /// // 外部入队 → reduce → 回灌入队(深 1) → reduce
    /// assert_eq!(kinds, [
    ///     AuditEventKind::Enqueued { feedback_depth: 0 },
    ///     AuditEventKind::Reduced,
    ///     AuditEventKind::Enqueued { feedback_depth: 1 },
    ///     AuditEventKind::Reduced,
    /// ]);
    /// # store.close();
    /// ```
    ///
    /// 票据丢弃即静默关闭，类型层拦截：
    ///
    /// ```compile_fail
    /// # #![deny(unused_must_use)]
    /// # use skeleton::{EffectBuf, Infallible, NoFx, Reducer, Store};
    /// # struct T;
    /// # impl Reducer for T {
    /// #     type State = u32; type Action = u32; type Op = Infallible;
    /// #     fn initial(&self) -> u32 { 0 }
    /// #     fn reduce(&self, s: &u32, a: u32, _o: &mut EffectBuf<u32>) -> u32 { s + a }
    /// # }
    /// # let store = Store::new(T, NoFx);
    /// store.subscribe_events(16, |_| {}); // ERROR: unused `AuditTicket`——丢弃即静默关闭事件流
    /// ```
    pub fn subscribe_events(
        &self,
        capacity: usize,
        on_event: impl FnMut(&AuditEvent) + Send + 'static,
    ) -> AuditTicket {
        let gen = self.inner.audit_gen.fetch_add(1, Ordering::AcqRel) + 1;
        let cap = capacity.max(1);
        *lock(&self.inner.audit) = Some(AuditSink {
            ring: VecDeque::with_capacity(cap),
            cap,
            gen,
            notify: Some(Box::new(on_event)),
        });
        self.inner.audit_gate.store(true, Ordering::Release);
        AuditTicket {
            gate: Arc::clone(&self.inner.audit_gate),
            audit: Arc::clone(&self.inner.audit),
            gen,
        }
    }

    /// 读侧：整体取走缓冲事件（取证消费；环容量保留复用）。未订阅 = 空 Vec。
    pub fn take_events(&self) -> Vec<AuditEvent> {
        let mut guard = lock(&self.inner.audit);
        match guard.as_mut() {
            Some(sink) => sink.ring.drain(..).collect(),
            None => Vec::new(),
        }
    }
}

/// 残留弃队留痕：正常路径 drop 恒 idle（dispatch/close 同步排空至空），
/// drop 时队列非空只可能是预算止步的残留回灌链——弃队数并入预算计数留痕，
/// 禁静默弃队。读窗 = inner Arc 存续期（Store 已弃而 inner 仍被持有，如测试
/// 白盒），与 [`Store::rejected_after_close`] 的停机窗同类语义。
impl<S, A, Op> Drop for Store<S, A, Op> {
    fn drop(&mut self) {
        let discarded = match self.inner.queue.lock() {
            Ok(queue) => queue.len(),
            // 毒锁 = 此前已有 panic 上浮；取内层只读长度，drop 内不二次爆炸
            Err(poisoned) => poisoned.into_inner().len(),
        };
        if discarded > 0 {
            self.inner.feedback_exceeded.fetch_add(discarded as u64, Ordering::SeqCst);
        }
    }
}

/// 排空权守卫：Drop 复位 `draining` 旗——reducer/解释器/订阅回调任一
/// panic 栈展开时旗必复位，杜绝「旗恒 true → dispatch 只入队不执行、零报错」的
/// 静默变砖。`armed=false`（disarm 后）Drop 不动旗：旗已显式放行。
/// 序：disarm/Drop 用 Release——旗的消费者只有后续排空者的 AcqRel swap（RMW 恒
/// 读改写序最新值，无 stale 风险），Release 发布旗变更即足；旗保护的数据交接
/// 经 queue 锁 happens-before 传导，不依赖旗本身承载同步。
struct DrainGuard<'a> {
    flag: &'a AtomicBool,
    armed: bool,
}

impl DrainGuard<'_> {
    fn disarm(&mut self) {
        self.flag.store(false, Ordering::Release);
        self.armed = false;
    }
}

impl Drop for DrainGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.flag.store(false, Ordering::Release);
        }
    }
}

/// 排空入口（close 路径与重入兜底）：夺排空权后交 [`drain_owned`]。
/// dispatch/send 的常规路径不经过此函数——它们的夺权已并入 push 临界区
/// （[`enqueue`]），同窗免二次加锁。
fn drain<S: Clone + Send + Sync + 'static, A: Send + 'static, Op: Send + 'static>(
    inner: &Arc<StoreInner<S, A, Op>>,
) {
    // 已在排空（重入回灌）：只入队，最外层循环排空。AcqRel：RMW 恒见修改序最新
    // 值（无 stale 可言）；Acquire 侧同步前任 Release 放旗，Release 侧发布本任。
    if inner.draining.swap(true, Ordering::AcqRel) {
        return;
    }
    drain_owned(inner, None);
}

/// 持排空权排空至 idle：出队→reduce→快照替换→效果转发（Emit 回灌入队，
/// Run/Cancel 交解释器）→通知订阅。
///
/// 交接语义（锁消融批修订）：持有排空权=同步排空至队列空；否则=入队保证
/// 序。出口协议为**持锁放旗**：pop 得 None 时仍持 queue 锁内完成放旗再返回——
/// push 全部被挡在锁外，此刻的空判定即终局判定（原「放旗→复检」两步的交错窗在锁
/// 内不可插入，复检退化为同锁内的已见空）；晚到的 push 由后到者自己的夺权排空。
/// 「重夺排空权」分支随窗口封死而不可达，rearm 机制随之删除（不变量由孤儿
/// 监视测试与守恒竞速持续咬住）。
fn drain_owned<S: Clone + Send + Sync + 'static, A: Send + 'static, Op: Send + 'static>(
    inner: &Arc<StoreInner<S, A, Op>>,
    first: Option<(A, ActionOrigin)>,
) {
    let mut guard = DrainGuard { flag: &inner.draining, armed: true };
    let mut first = first;
    loop {
        let (action, origin) = match first.take() {
            Some(pair) => pair,
            None => {
                // let-else 绑定：queue 守卫先于 action 使用释放，Emit 回灌再锁
                // queue 不会自死锁
                let mut queue = lock(&inner.queue);
                let Some(pair) = queue.pop_front() else {
                    // 出口交接（M1）：持锁放旗——push 被锁外挡，锁内空即终局空；
                    // queue 守卫随 return 释放，晚到 push 由其排空者自排空
                    guard.disarm();
                    return;
                };
                drop(queue);
                pair
            }
        };
        // 回灌深度随 action 携带（入队时定深，见 [`ActionOrigin`]）——
        // 外部 action 深度 0，同栈回灌 = 产生它的 action 深度 + 1。自指回灌环
        // 在预算点止步，dispatch 必返回（修复前排空环永不达空，dispatch 永挂；
        // 修复前 back 通道同栈回灌标 External 深度清零，预算永不触发）。
        let feedback_depth = match origin {
            ActionOrigin::External => 0,
            ActionOrigin::Feedback(depth) => depth,
        };
        if feedback_depth > FEEDBACK_DEPTH_BUDGET {
            // 超限：结构化错误上浮（计数读侧可见），当前 action 原值退回队尾
            //（零丢失、无半执行态；队尾重排让位外部流量——队首退回会让深度
            // 饱和的残留卡住后续一切 action），本任排空停止——非 panic 非静默
            // no-op。残留链由后续 dispatch 有界重放（每次再计再浮）。
            lock(&inner.queue).push_back((action, origin));
            inner.feedback_exceeded.fetch_add(1, Ordering::SeqCst);
            // 取证时间线的病理终点（R1-2）：预算止步以事件收尾，环结构可重建。
            audit_emit(inner, AuditEventKind::FeedbackBudgetExceeded);
            return;
        }
        // 本 action 处理期间（reduce/效果转发/订阅回调）本线程的一切入队
        // 一律计深 feedback_depth+1；迭代末守卫恢复原值，panic 栈展开亦然。
        let _reentrant = ReentrantDepthGuard::enter(inner.store_id, feedback_depth + 1);
        let current = Arc::clone(&lock(&inner.state));
        // 效果缓冲排空权独占（DrainExclusive）：take/还
        // 两对锁消融——clear 后原地交 reducer，转发完随下轮 clear 复用。reducer
        // 期间不持任何锁的契约不变：UnsafeCell 无锁可毒，reducer panic 时
        // 栈展开零残留，缓冲容量连丢一次都不丢。
        // SAFETY: 持排空权（DrainGuard armed 区间）；重入 dispatch 只入队即返，
        // 到不了此处——独占访问成立，见 DrainExclusive 安全纪要
        let buf = unsafe { inner.effect_buf.get_mut() };
        // 缓冲不再迭代头 clear——
        // 转发改为逐条摘除（见下方转发环注释），panic 时未转发的效果原地留缓冲，
        // 下一任排空的 reduce 向其追加、转发时先续转（panic 前已产效果禁吞，
        // fail-visible；迭代头 clear 会无声吞掉残留）。成功路径转发即摘除，缓冲
        // 照常回空，保容量语义不变，稳态零新增分配不受影响。
        let next_val = inner.reducer.reduce(&current, action, buf);
        audit_emit(inner, AuditEventKind::Reduced);
        // CoW 前置：reduce 后立即放手 current——不放
        // 手则 state 强计数恒 2，下方 make_mut 永走伪克隆分支，稳态零分配落空。
        drop(current);
        // 订阅广播（锁消融修订）：稳态版本匹配=单次 Acquire load 直接
        // 复用快照，零锁零 upgrade；版本不匹配才冷重建（锁 subs 单遍 upgrade
        // 收集+代笔回收死 Weak，Stamp 用载入时版本——竞态多出的重建无害）。
        // 快照落位（消融：原「再锁 state 再 Arc::clone」的快照预取一对锁+一对
        // 引用计数整对删除）：快照即 `next` 本体。零订阅稳态把 `next` 直接移入
        // state（省 clone 对）；有订阅者才 clone 留本体给回调。管线序：
        // reduce→快照替换→效果转发→通知。
        // SAFETY: 持排空权（DrainGuard armed 区间）；通知段仅在排空权下进入，
        // 独占访问成立，见 DrainExclusive 安全纪要
        let cache = unsafe { inner.notify_cache.get_mut() };
        let version = inner.subs_version.load(Ordering::Acquire);
        if cache.version != version {
            let mut subs = lock(&inner.subs);
            cache.slots.clear();
            subs.retain(|w| match w.upgrade() {
                Some(slot) => {
                    cache.slots.push(slot);
                    true
                }
                None => false,
            });
            cache.version = version;
        }
        // CoW 快照落位（快照分配税消除）：稳态
        // （current 已放手、无外部 snapshot() 持有者）强计数=1 → make_mut 原地
        // 写入零分配；有外部持有者时自动克隆新分配、旧分配冻结——持有者永见旧
        // 快照，快照一致性逐比特保持（CoW 即快照语义的业界标准实现）。
        // **mem::replace 把旧 S 挪出锁外析构是本形态最大风险点**：原地赋值会在
        // state 锁内 drop 旧值，S:Drop panic 即毒锁变砖——锁内必须
        // 只剩 memcpy 与计数原子，析构在 drop(old_snapshot) 处（锁外）发生。
        let old_snapshot = {
            let mut slot = lock(&inner.state);
            std::mem::replace(Arc::make_mut(&mut slot), next_val)
        };
        drop(old_snapshot);
        // 通知句柄：订阅态在落位后补一次 state 锁克隆新快照（回调锁外借用）；
        // 句柄随迭代末消亡，稳态计数回落 1，下一 action 的 make_mut 仍原地命中。
        let notified_next = if cache.slots.is_empty() {
            None
        } else {
            Some(Arc::clone(&lock(&inner.state)))
        };
        // 转发环：逐条借出转发，
        // 成功才从缓冲摘除——panic 时未转发的效果原地留在缓冲，由下一任排空的
        // reduce 追加、转发时先续转（禁吞：panic 前已产效果零丢失）。不能用
        // `drain(..)`：其 Drop 语义是「完成排空」（未消费元素被 drop，drain_keep_rest
        // 才回填且 unstable）——`drain(..)` 的静默蒸发本体在此，「clear 吞掉」
        // 是次生路径。正在转发的一条随 panic 丢弃：其失败已显式传播+计数，重投
        // 会恒 panic 死循环。remove(0) 稳态（单效果）零搬移，保容量。
        // 回灌句柄为 StoreInner 缓存字段，禁每 action 新建 Arc 闭包
        while !buf.effects.is_empty() {
            let effect = buf.effects.remove(0);
            match effect {
                Effect::Emit(a) => {
                    inner.feedback_emitted.fetch_add(1, Ordering::Relaxed);
                    lock(&inner.queue).push_back((a, ActionOrigin::Feedback(feedback_depth + 1)));
                    // 回灌入队与外部入队同一事件节点（深度链 = 环结构重建键）
                    audit_emit(inner, AuditEventKind::Enqueued { feedback_depth: feedback_depth + 1 });
                }
                effect => {
                    // 解释器 panic 毒化 runtime 锁，此后每个产效果 dispatch
                    // 恒 panic（半砖：reduce 照常执行、状态持续前进，效果路径
                    // 原地无恢复 API）。修法 = PoisonError into_inner 恢复守卫 +
                    // clear_poison 复位毒位 + 计数上浮（runtime_panic_count），
                    // panic 本体照常传播（禁 catch_unwind，fail-visible）。恢复
                    // 的 Box 可能已被 panic 打断在任意中间态——run panic 后解释器
                    // 自身一致性自证（宿主适配层责任，见 EffectRuntime 契约注）。
                    let mut runtime = match inner.runtime.lock() {
                        Ok(guard) => guard,
                        Err(poisoned) => {
                            inner.runtime_panics.fetch_add(1, Ordering::SeqCst);
                            let guard = poisoned.into_inner();
                            inner.runtime.clear_poison();
                            guard
                        }
                    };
                    // 解释器 panic 在时间线留痕
                    // （EffectInterrupted）——catch_unwind 只观察不吞：发完事件
                    // 原样 resume（payload/传播面不变；runtime 守卫在闭包外存活，
                    // 随外层展开照常毒化，恢复路径与 fail-visible 均不受影响）。
                    let run = catch_unwind(AssertUnwindSafe(|| runtime.run(effect, &inner.back)));
                    if let Err(payload) = run {
                        audit_emit(inner, AuditEventKind::EffectInterrupted);
                        std::panic::resume_unwind(payload);
                    }
                    audit_emit(inner, AuditEventKind::EffectForwarded);
                }
            }
        }
        // 本地订阅（单线程档）：先于跨线程广播判读——零本地订阅一次 relaxed
        // 读即短路；有本地订阅时零跨线程订阅也必触发（快照按需取，本地闭包
        // panic 摘除计入 subscriber_panics，fail-closed）。同一 dispatch 通知
        // 序：本地先行、跨线程随后（确定性）。
        let local_notified = inner.local_subs.load(Ordering::Relaxed) > 0;
        if local_notified {
            let snapshot = Arc::clone(&lock(&inner.state));
            let (poisoned, cause) = local_notify(inner.store_id, &*snapshot);
            if poisoned > 0 {
                inner.subscriber_panics.fetch_add(poisoned, Ordering::Release);
                *lock(&inner.subscriber_panic_cause) = cause;
            }
        }
        if let Some(next) = &notified_next {
            for slot in cache.slots.iter() {
                // 订阅回调 panic 归一：摘除该订阅者后继续——中毒 slot.notify
                // 永久再 panic，留表即「每次 dispatch 必 panic」的双重变砖。摘除自增
                // 版本，快照缓存下一次 dispatch 冷重建时收敛。
                let notified = catch_unwind(AssertUnwindSafe(|| {
                    let mut notify = lock(&slot.notify);
                    notify(next);
                }));
                if let Err(payload) = notified {
                    let dead = Arc::downgrade(slot);
                    lock(&inner.subs).retain(|w| !Weak::ptr_eq(w, &dead));
                    inner.subs_version.fetch_add(1, Ordering::Release);
                    inner.subscriber_panics.fetch_add(1, Ordering::Release);
                    // 成因摘要留痕（回答「为什么」，计数只答「谁死了」）
                    *lock(&inner.subscriber_panic_cause) = Some(panic_cause_summary(&*payload));
                }
            }
        }
        // 通知节点（R1-2）：只在当拍确有订阅者收到快照时记录——零订阅者的
        // dispatch 无 Notified 事件（时间线如实，不造心跳噪声）。
        if local_notified || notified_next.is_some() {
            audit_emit(inner, AuditEventKind::Notified);
        }
    }
}

/// 订阅票据：Drop=注销——经共享订阅表句柄**即时摘除**本表项（订阅表存 Weak，
/// 漏注销不可表达），广播可见性与 [`Store::subscriber_count`] 计数读侧同时即时
/// 生效、无一拍滞后。
/// Drop 时额外 Release 自增订阅集版本，失效通知快照缓存（其持槽位强引用，不
/// 失效则下一次 dispatch 会照旧通知已注销者）——注销的广播可见性由版本失效
/// 精确保证。
#[must_use = "订阅票据被丢弃即立即注销（Drop=注销）——`store.subscribe(..);` 裸调用 = 静默死订阅；绑定持有才持续收通知"]
pub struct Subscription<S> {
    /// 存活令牌：持住 slot 强引用，Weak 订阅表项即有效
    _slot: Arc<SubscriptionSlot<S>>,
    /// 订阅表句柄：Drop 时即时摘除本表项（`subscriber_count`
    /// 读侧实时判活，不等排空冷重建代笔回收）
    subs: Arc<Mutex<Vec<Weak<SubscriptionSlot<S>>>>>,
    /// 订阅集版本句柄（与 store 共享计数器），Drop 时 bump 失效快照缓存
    _version: Arc<AtomicUsize>,
}

impl<S> Drop for Subscription<S> {
    fn drop(&mut self) {
        let dead = Arc::downgrade(&self._slot);
        lock(&self.subs).retain(|w| !Weak::ptr_eq(w, &dead));
        // 版本 bump 仍需：通知快照缓存（NotifyCache）持槽位强引用——不失效则
        // 下一次 dispatch 会照旧通知已注销者（广播可见性由版本失效精确保证）
        self._version.fetch_add(1, Ordering::Release);
    }
}

// ── 本地订阅（[`Store::subscribe_local`]）的线程本地登记机制 ─────────────
//
// 回调只登记在注册线程的 thread-local 表——store 侧从不持有本地闭包，跨线程
// dispatch 在自己的表里查不到该 store，本地回调「只在注册线程触发」与「免
// Send」的安全性全部由线程亲和保证，零 unsafe（RefCell 作用域即单线程）。

/// store 身份号：本地订阅 TLS 表的键。全局单调、进程内唯一。
static NEXT_STORE_ID: AtomicU64 = AtomicU64::new(1);

/// 本地订阅序号：注销定位键（与 store_id 联合唯一定位一条登记）。
static NEXT_LOCAL_SUB_ID: AtomicU64 = AtomicU64::new(1);

thread_local! {
    static LOCAL_SUBS: RefCell<Vec<LocalSubEntry>> = const { RefCell::new(Vec::new()) };
}

struct LocalSubEntry {
    store_id: u64,
    sub_id: u64,
    notify: Box<dyn LocalNotify>,
}

/// 状态擦除的本地回调：TLS 容器无泛型，快照经 `dyn Any` 下发——store_id
/// 唯一性保证下发必是本 store 的 `S`（downcast 失败属 TLS 键碰撞，不可达）。
trait LocalNotify {
    fn notify(&mut self, state: &dyn std::any::Any);
}

struct LocalSlot<S> {
    notify: Box<dyn FnMut(&S)>,
}

impl<S: 'static> LocalNotify for LocalSlot<S> {
    fn notify(&mut self, state: &dyn std::any::Any) {
        let snapshot = state
            .downcast_ref::<S>()
            .expect("local subscription: state type mismatch (TLS store_id collision)");
        (self.notify)(snapshot);
    }
}

/// 登记（注册线程执行）：TLS 插入先于闸自增——注册线程上程序序保证
/// 「表里有 → 闸 >0」，本线程的 dispatch 必能查到自己的回调。
fn local_sub_register(store_id: u64, sub_id: u64, notify: Box<dyn LocalNotify>) {
    LOCAL_SUBS.with(|table| {
        table.borrow_mut().push(LocalSubEntry { store_id, sub_id, notify });
    });
}

/// 注销（注册线程执行——票据 `!Send`，Drop 只会发生在注册线程）。
fn local_sub_unregister(store_id: u64, sub_id: u64) {
    LOCAL_SUBS.with(|table| {
        table.borrow_mut().retain(|e| !(e.store_id == store_id && e.sub_id == sub_id));
    });
}

/// panic payload 成因摘要：str/String 原文截断 200 字符；其余
/// 载荷类型不可静态提取（`Box<dyn Any>` 无 Debug 面），存占位说明。
fn panic_cause_summary(payload: &(dyn std::any::Any + Send)) -> String {
    let raw = payload
        .downcast_ref::<&str>()
        .map(|s| (*s).to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "<non-string panic payload>".to_string());
    if raw.chars().count() > 200 {
        format!("{}…", raw.chars().take(200).collect::<String>())
    } else {
        raw
    }
}

/// 触发本线程登记的某 store 全部本地订阅；返回 (panic 摘除数, 最近一次成因
/// 摘要)（调用方计入 `subscriber_panics` + 留痕 `subscriber_panic_last_cause`，
/// 摘除即从此不再触发——fail-closed）。回调内重入
/// dispatch 只入队即返（排空权互斥），不会重入本函数，RefCell 不可重入
/// 借用天然满足。通知序 = 登记序（确定性）。
fn local_notify(store_id: u64, state: &dyn std::any::Any) -> (u64, Option<String>) {
    LOCAL_SUBS.with(|table| {
        let mut table = table.borrow_mut();
        let mut dead = Vec::new();
        let mut last_cause = None;
        for entry in table.iter_mut() {
            if entry.store_id != store_id {
                continue;
            }
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| entry.notify.notify(state))) {
                dead.push(entry.sub_id);
                last_cause = Some(panic_cause_summary(&*payload));
            }
        }
        let removed = dead.len() as u64;
        if !dead.is_empty() {
            table.retain(|e| e.store_id != store_id || !dead.contains(&e.sub_id));
        }
        (removed, last_cause)
    })
}

/// 本地订阅票据（[`Store::subscribe_local`]）：Drop = 注销（TLS 摘除 + 闸计数
/// 回落）。`PhantomData<*const ()>` 即 `!Send + !Sync` 的类型化表达——票据
/// 不可跨线程移动，注销只会发生在注册线程，与本地登记的线程亲和语义互锁。
#[must_use = "本地订阅票据被丢弃即立即注销（Drop=注销）——`store.subscribe_local(..);` 裸调用 = 静默死订阅；绑定持有才持续收通知"]
pub struct LocalSubscription {
    store_id: u64,
    sub_id: u64,
    count: Arc<AtomicUsize>,
    _not_send: PhantomData<*const ()>,
}

impl Drop for LocalSubscription {
    fn drop(&mut self) {
        local_sub_unregister(self.store_id, self.sub_id);
        self.count.fetch_sub(1, Ordering::Release);
    }
}

// ── 审计事件流（时序事后取证面）────────────────────────
//
// 最小 API 面：一订阅点（[`Store::subscribe_events`]）+ 一事件
// 枚举（[`AuditEvent`]/[`AuditEventKind`]）+ 一读侧（[`Store::take_events`]）。
// 默认关闭零开销：未订阅路径 = 每 emit 点一次 gate load 即返，零分配零锁；
// 不搞 serde/异步/过滤 DSL。

/// 审计事件：单笔 dispatch 生命周期的一个节点。
///
/// 生命周期重建协议：一笔 dispatch = `Enqueued(depth) → Reduced →
/// [EffectForwarded* | EffectInterrupted | Enqueued(depth+1) → …] → Notified?`；`Notified` 仅当拍
/// 确有订阅者收到快照时记录。`seq` 单调；订阅窗内**连续**，空洞 = 有界环溢出
/// 丢最旧的显式证据（禁静默重写历史的对账键）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuditEvent {
    /// 进程内单调序号（store 级计数器，跨订阅窗不重复）。
    pub seq: u64,
    /// 生命周期节点。
    pub kind: AuditEventKind,
}

/// 生命周期节点枚举（七节点：入队/reduce 完成/效果转发/效果中断/通知/拒绝/回灌超限）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditEventKind {
    /// action 入队（dispatch / [`ActionSender::send`] / Emit 回灌三路同闸）。
    /// `feedback_depth`: 0 = 外部流量；>0 = 同栈回灌链深（链上逐级 +1，回灌
    /// 环结构与预算止步点可重建，见 [`AuditEventKind::FeedbackBudgetExceeded`]）。
    Enqueued {
        /// 回灌深度（0 = 外部流量）
        feedback_depth: u64,
    },
    /// reduce 完成（状态迁移已产出，效果在缓冲）。
    Reduced,
    /// 一条 `Run`/`Cancel` 效果已转发解释器（[`Effect::Emit`] 不经此处——
    /// 回灌以 [`AuditEventKind::Enqueued`] 入账）。
    EffectForwarded,
    /// 一条效果转发解释器时解释器 panic：本条效果随转发丢弃，未转发的残留由
    /// 下一任排空续转（续转仍以 [`AuditEventKind::EffectForwarded`] 入账）——
    /// 时间线上「谁 panic、什么被续转」可归因。
    EffectInterrupted,
    /// 状态通知已发出（本地或跨线程至少一个订阅者收到当拍快照）。
    Notified,
    /// 闭门拒绝（dispatch no-op / send `false` 的共享计数路径）。
    Rejected,
    /// 回灌深度预算止步（自指回灌环在时间线的显式终点）。
    FeedbackBudgetExceeded,
}

/// 审计回调闭包（私有别名收口 type_complexity，零公共面增量——同
/// [`SubscriberCallback`] 先例）。
type AuditCallback = Box<dyn FnMut(&AuditEvent) + Send>;

/// 审计汇聚槽（唯一订阅点的单槽内容）：有界环 + 实时回调 + 订阅代次。
struct AuditSink {
    ring: VecDeque<AuditEvent>,
    cap: usize,
    /// 订阅代次：票据 Drop 凭 gen 对账，禁误杀后继订阅流。
    gen: u64,
    notify: Option<AuditCallback>,
}

/// 审计订阅票据：Drop = 关闭事件流（落闸 + 清槽，零开销恢复）。
/// `#[must_use]` 拦截裸调用（丢弃即静默关闭，与状态订阅票据同一纪律）。
#[must_use = "审计票据被丢弃即立即关闭事件流——`store.subscribe_events(..);` 裸调用 = 静默关闭；绑定持有才持续记录"]
pub struct AuditTicket {
    gate: Arc<AtomicBool>,
    audit: Arc<Mutex<Option<AuditSink>>>,
    gen: u64,
}

impl Drop for AuditTicket {
    fn drop(&mut self) {
        // 代次对账在锁内完成：现役票据才落闸清槽；旧票据（已被后订覆盖）禁动。
        let mut guard = lock(&self.audit);
        if guard.as_ref().is_some_and(|s| s.gen == self.gen) {
            *guard = None;
            drop(guard);
            self.gate.store(false, Ordering::Release);
        }
    }
}

thread_local! {
    /// 审计回调在途标记（键 = store_id）：回调内 dispatch/send 的再入事件只录
    /// 环不再进回调——封死「回调→dispatch→回调」自指递归（栈爆），与回灌深度
    /// 预算同一病理类别。嵌套回调（另一 store）按栈配对（prev 恢复）。
    static AUDIT_CB_IN_FLIGHT: Cell<Option<u64>> = const { Cell::new(None) };
}

/// 审计回调在途守卫：进入置位、离开（含 panic 栈展开）恢复原值。
struct AuditCallbackGuard {
    prev: Option<u64>,
}

impl AuditCallbackGuard {
    fn enter(store_id: u64) -> Self {
        Self { prev: AUDIT_CB_IN_FLIGHT.with(|cell| cell.replace(Some(store_id))) }
    }
}

impl Drop for AuditCallbackGuard {
    fn drop(&mut self) {
        AUDIT_CB_IN_FLIGHT.with(|cell| cell.set(self.prev.take()));
    }
}

/// 入队事件发射（[`audit_emit`] 的 Enqueued 收口：来源标签 → 回灌深度）。
fn audit_enqueued<S, A, Op>(inner: &StoreInner<S, A, Op>, origin: ActionOrigin) {
    let feedback_depth = match origin {
        ActionOrigin::External => 0,
        ActionOrigin::Feedback(depth) => depth,
    };
    audit_emit(inner, AuditEventKind::Enqueued { feedback_depth });
}

/// 审计发射唯一收口：未订阅 = 一次 gate load 即返；订阅态 = 有界环入账
/// （溢出丢最旧）+ 实时回调（锁外调用——回调可重入本 store 的一切入口，
/// 持锁即死锁；take/归还窗口内订阅被替换则宁弃旧回调禁覆盖新槽）。
fn audit_emit<S, A, Op>(inner: &StoreInner<S, A, Op>, kind: AuditEventKind) {
    if !inner.audit_gate.load(Ordering::Acquire) {
        return;
    }
    let event = AuditEvent { seq: inner.audit_seq.fetch_add(1, Ordering::Relaxed) + 1, kind };
    let mut notify = {
        let mut guard = lock(&inner.audit);
        match guard.as_mut() {
            Some(sink) => {
                if sink.ring.len() >= sink.cap {
                    sink.ring.pop_front();
                }
                sink.ring.push_back(event);
                sink.notify.take()
            }
            None => return,
        }
    };
    if let Some(f) = notify.as_mut() {
        if AUDIT_CB_IN_FLIGHT.with(Cell::get) != Some(inner.store_id) {
            let _guard = AuditCallbackGuard::enter(inner.store_id);
            match catch_unwind(AssertUnwindSafe(|| f(&event))) {
                Ok(()) => {}
                // 回调 panic：fail-closed 摘除（环照录、dispatch 行为不变——观测
                // 面禁改变被观测者），成因留痕与订阅回调同一纪律（审计回调也是
                // 订阅回调：subscriber_panics / last_cause）。
                Err(payload) => {
                    inner.subscriber_panics.fetch_add(1, Ordering::Release);
                    *lock(&inner.subscriber_panic_cause) = Some(panic_cause_summary(&*payload));
                    notify = None;
                }
            }
        }
    }
    if let Some(f) = notify {
        let mut guard = lock(&inner.audit);
        if let Some(sink) = guard.as_mut() {
            if sink.notify.is_none() {
                sink.notify = Some(f);
            }
        }
    }
}

/// Driver 端口（单向输出）：两职责两成员——上行通道一条（bind）、呈现入口
/// 一条（render），签名里没有平台类型，无第三成员的位置。
pub trait Driver<S, A> {
    /// 唯一上行通道
    fn bind(&mut self, sender: ActionSender<A>);
    /// 唯一呈现入口（render(state) 纪律）
    fn render(&mut self, state: &S);
}

/// 组合器：父子分片，协作=显式结构（字符串路由组合的封死形态）。
///
/// - `get`/`set`：父状态内子切片的读写镜头
/// - `wrap`/`unwrap`：子 action 上浮 / 父 action 下沉路由；`unwrap` 返回
///   `None` = 本层不路由，状态克隆原样返回、零效果（父层分支自理）。
///   子效果里的 `Emit(CA)` 由核心经 `wrap` 自动上浮为 `Emit(PA)`——`lift_fx`
///   只会收到 `Run`/`Cancel` 两形（词表上提与 action 上浮分工，参数零死位）
/// - `lift_fx`：子效果词表到父词表的显式上提（同形出参：写父 `out`）
///
/// # Examples
///
/// 父子分片最小例（零效果词表，`lift_fx` 无可上提即空转）：子切片承载计数，
/// 子 action 经 `wrap`/`unwrap` 上浮/下沉路由；根状态由父 Feature 的
/// `initial` 显式构造（含子初值），路由分支整条委托分片（完整含效果词表形态见
/// `tests/combine_teststore.rs`）：
///
/// ```
/// use skeleton::{combine, EffectBuf, Infallible, NoFx, Reducer, Store};
///
/// #[derive(Clone, Debug, PartialEq)]
/// struct Parent { total: u32, child: u32 }
///
/// #[derive(Clone, Debug, PartialEq)]
/// enum PAction { Child(CAction), Touch }
///
/// #[derive(Clone, Debug, PartialEq)]
/// enum CAction { Add(u32) }
///
/// struct ChildCounter;
///
/// impl Reducer for ChildCounter {
///     type State = u32;
///     type Action = CAction;
///     type Op = Infallible;
///     fn initial(&self) -> u32 { 0 }
///     fn reduce(&self, s: &u32, a: CAction, _out: &mut EffectBuf<CAction, Infallible>) -> u32 {
///         match a { CAction::Add(n) => s + n }
///     }
/// }
///
/// /// 子切片分片：get/set 镜头 + wrap/unwrap 路由 + lift_fx 效果上提（12 参具名）
/// fn counter_fragment() -> impl Reducer<State = Parent, Action = PAction, Op = Infallible> {
///     combine(
///         |p: &Parent| &p.child,                 // get：子切片读镜头
///         |p: &mut Parent, c: u32| p.child = c,  // set：子切片写回
///         PAction::Child,                        // wrap：子 action 上浮
///         |p: &PAction| match p {                // unwrap：父 action 下沉路由
///             PAction::Child(c) => Some(c.clone()),
///             PAction::Touch => None,
///         },
///         ChildCounter,                          // child：子 reducer
///         |_, _| {},                             // lift_fx：子零效果，无可上提
///     )
/// }
///
/// struct ParentFeature;
///
/// impl Reducer for ParentFeature {
///     type State = Parent;
///     type Action = PAction;
///     type Op = Infallible;
///     fn initial(&self) -> Parent { Parent { total: 100, child: 0 } }
///     fn reduce(&self, s: &Parent, a: PAction, out: &mut EffectBuf<PAction, Infallible>) -> Parent {
///         match a {
///             // 路由命中的子 action：整条委托分片（子迁移+子切片写回）
///             PAction::Child(_) => counter_fragment().reduce(s, a, out),
///             PAction::Touch => Parent { total: s.total + 1, child: s.child },
///         }
///     }
/// }
///
/// let store = Store::new(ParentFeature, NoFx);
/// store.dispatch(PAction::Child(CAction::Add(2))); // wrap 下沉 → 子切片迁移
/// store.dispatch(PAction::Child(CAction::Add(3))); // → 5
/// store.dispatch(PAction::Touch);                  // 父自有分支，子切片不动
/// assert_eq!(*store.snapshot(), Parent { total: 101, child: 5 });
/// store.close();
/// ```
///
/// 产物是**状态分片不是根**：签名无父状态工厂，`initial` 恒 panic——根状态
/// 由父 Feature 的 `initial` 显式构造（含子切片初值），分片只承载 reduce
/// 委托；用作根 = 组合根误用，fail-closed 显式崩溃（禁静默空状态，
/// 亦不以 `PS: Default` 界逼「非法状态进类型」的 State 写退化初值）。
pub fn combine<PS, PA, PO, CS, CA, CO, G, Se, W, U, C, L>(
    get: G,
    set: Se,
    wrap: W,
    unwrap: U,
    child: C,
    lift_fx: L,
) -> Combine<PS, PA, PO, CS, CA, CO, G, Se, W, U, C, L>
where
    PS: Clone + Send,
    PA: Send,
    G: Fn(&PS) -> &CS + 'static,
    Se: Fn(&mut PS, CS) + 'static,
    W: Fn(CA) -> PA + 'static,
    U: Fn(&PA) -> Option<CA> + 'static,
    C: Reducer<State = CS, Action = CA, Op = CO>,
    L: Fn(Effect<CA, CO>, &mut EffectBuf<PA, PO>) + 'static,
{
    Combine { get, set, wrap, unwrap, child, lift_fx, _slices: PhantomData }
}

/// [`combine`] 产物（具名类型，字段私有、只经 [`combine`] 构造）：镜头/路由
/// 闭包 + 子 reducer + 效果上提闭包，零 dyn 零装箱；六个状态/词表参数与六个
/// 闭包参数经泛型记名——采用方可用 `Combine<_, _, …>` 注解或别名持有产物，
/// 匿名 `impl Reducer` 形态做不到。
pub struct Combine<PS, PA, PO, CS, CA, CO, G, Se, W, U, C, L> {
    get: G,
    set: Se,
    wrap: W,
    unwrap: U,
    child: C,
    lift_fx: L,
    _slices: PhantomData<(PS, PA, PO, CS, CA, CO)>,
}

impl<PS, PA, PO, CS, CA, CO, G, Se, W, U, C, L> Reducer
    for Combine<PS, PA, PO, CS, CA, CO, G, Se, W, U, C, L>
where
    G: Fn(&PS) -> &CS,
    Se: Fn(&mut PS, CS),
    W: Fn(CA) -> PA,
    U: Fn(&PA) -> Option<CA>,
    C: Reducer<State = CS, Action = CA, Op = CO>,
    L: Fn(Effect<CA, CO>, &mut EffectBuf<PA, PO>),
    PS: Clone + Send,
    PA: Send,
{
    type State = PS;
    type Action = PA;
    type Op = PO;

    // `#[track_caller]`（O 小项）：与 trait 声明同标——误用 panic 位置指采用方装配行。
    #[track_caller]
    fn initial(&self) -> PS {
        panic!(
            "combine 产物是状态分片不是根：根状态由父 Feature 的 initial 显式构造（含子切片初值），分片只承载 reduce 委托"
        );
    }

    fn reduce(&self, state: &PS, action: PA, out: &mut EffectBuf<PA, PO>) -> PS {
        let Some(child_action) = (self.unwrap)(&action) else {
            return state.clone();
        };
        // 子效果缓冲：Vec::new 空态零分配，子 action 实际产效果时才分配一次
        // （稳态零新增分配纪律约束的是 store 效果转发路径，见 EffectBuf 头注；
        // reducer 禁写共享态——本地缓冲进出即弃，不违纯函数契约）
        let mut child_out = EffectBuf::new();
        let child_next = self.child.reduce((self.get)(state), child_action, &mut child_out);
        for effect in child_out.effects.drain(..) {
            match effect {
                // Emit 回灌是子 action 上浮（wrap 的职责），核心代 lifted；
                // Run/Cancel 交 lift_fx 显式上提词表（lift_fx 只会收到这两形）
                Effect::Emit(a) => out.push(Effect::Emit((self.wrap)(a))),
                effect => (self.lift_fx)(effect, out),
            }
        }
        let mut next = state.clone();
        (self.set)(&mut next, child_next);
        next
    }
}

/// 测试面：脚本化效果 + 逐动作断言 + 效果回填断言。恒导出（非 feature 门控）——
/// 零依赖小装置，免 feature 矩阵、验收命令零额外旗标。
///
/// 验收纪律四条：逐动作断言 /
/// 效果断言 / 非法迁移负例 / tests 先行。
pub struct TestStore<S, A, Op = Infallible> {
    store: Store<S, A, Op>,
    /// 脚本化解释器的效果回收队列（FIFO）：[`TestStore::receive`] 逐条弹出比对
    effects: Arc<Mutex<VecDeque<Effect<A, Op>>>>,
    /// 最近一次 send 后的 idle 快照：[`TestStore::state`] 直接借用免锁
    current: Arc<S>,
    /// 最近一次 send 期间的回灌 Emit 计数：[`TestStore::receive`] 空队列
    /// 时区分「真没产效果」与「产了但走了回灌通道」。
    last_send_feedback: u64,
}

/// [`TestStore`] 专用脚本化解释器：效果原样回收、永不解释——宿主逻辑层
/// 无 IO 全测（通用性基准 L1）。
struct ScriptedRuntime<A, Op> {
    sink: Arc<Mutex<VecDeque<Effect<A, Op>>>>,
}

impl<A, Op> EffectRuntime<A, Op> for ScriptedRuntime<A, Op> {
    fn run(&mut self, effect: Effect<A, Op>, _back: &ActionSender<A>) {
        lock(&self.sink).push_back(effect);
    }
}

/// 断言用效果期望值（[`TestStore::receive`] 入参）。`id: None` = 不校验寻址
/// id；`Some(id)` = 必须同值（[`EffectId`] 同义恒同值）。`Emit` 不在此列：
/// Emit 回灌由 store 核心以 Action 形态重入队列，从不进入效果转发路径——
/// 回环语义用 `send` + [`TestStore::state`] 断言。
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
    // `#[track_caller]`：分片误用作根的 initial panic 指采用方行（同 [`Store::new`]）。
    #[track_caller]
    pub fn new(root: impl Reducer<State = S, Action = A, Op = Op> + Send + Sync + 'static) -> Self {
        let effects = Arc::new(Mutex::new(VecDeque::new()));
        let store = Store::new(root, ScriptedRuntime { sink: Arc::clone(&effects) });
        let current = store.snapshot();
        Self { store, effects, current, last_send_feedback: 0 }
    }

    /// 逐动作断言入口：dispatch 并刷新 idle 快照（同步排空至 idle 后取，
    /// Emit 回灌链已折叠完毕）。
    pub fn send(&mut self, action: A) {
        let feedback_before = self.store.inner.feedback_emitted.load(Ordering::Relaxed);
        self.store.dispatch(action);
        self.current = self.store.snapshot();
        // 本轮回灌 Emit 快照——Emit 不进效果队列，receive 空队列时据此
        // 如实归因（禁再报「no effect produced」文不对题）。
        self.last_send_feedback =
            self.store.inner.feedback_emitted.load(Ordering::Relaxed) - feedback_before;
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
            // 产过回灌 Emit 时如实归因并指正解（回环语义 send + state 断言）。
            if self.last_send_feedback > 0 {
                panic!(
                    "receive: no effect for consumer ({} emitted action(s) were re-enqueued as feedback; Emit never enters the receive queue — assert the round-trip via send + state())",
                    self.last_send_feedback
                );
            }
            panic!("receive: no effect produced (queue empty); expected {expected:?}");
        };
        let matched = match (&expected, &actual) {
            (
                ExpectedEffect::Run { id, op },
                Effect::Run { id: actual_id, op: actual_op },
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

    /// 逐动作断言读侧：最近一次 `send` 后的 idle 状态。
    pub fn state(&self) -> &S {
        &self.current
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── 所有权分界：本 mod 为白盒回归监视区；产品面与新 API 测试归
    // tests/ 下各文件，互不越界。──

    enum Tick {
        Tick,
    }

    struct TickFeature;

    impl Reducer for TickFeature {
        type State = u32;
        type Action = Tick;
        type Op = Infallible;
        fn initial(&self) -> u32 {
            0
        }
        fn reduce(&self, s: &u32, _: Tick, _out: &mut EffectBuf<Tick, Infallible>) -> u32 {
            s + 1
        }
    }

    struct NopRuntime;

    impl EffectRuntime<Tick, Infallible> for NopRuntime {
        fn run(&mut self, _: Effect<Tick, Infallible>, _: &ActionSender<Tick>) {}
    }

    /// 死 Weak 就地回收：票据丢弃后下一次排空的通知收集阶段摘除，
    /// 订阅表不随订阅增删无界增长（修复前表长只增不减）。内部状态断言，须与
    /// 实现同模块。
    #[test]
    fn dead_subscription_slots_pruned_on_drain() {
        let store = Store::new(TickFeature, NopRuntime);
        let _keep = store.subscribe(|_| {});
        let dropped = store.subscribe(|_| {});
        drop(dropped);
        store.dispatch(Tick::Tick);
        assert_eq!(lock(&store.inner.subs).len(), 1);
    }

    /// 多线程竞争回归：自由竞争 dispatch +
    /// 不变量监视线程。原版在 tests/ 且 barrier 每轮同步，push 永远落在退出
    /// 窗口（放旗→复检）之外，修复前代码上也绿（green theater）。强化三件：
    /// ①挪入 src 暴露私有态；②去 barrier 让 push 可落进退出窗口；③监视线程
    /// 连续断言「无在途 dispatch ⇒ 队列空」。
    ///
    /// 判孤儿取持锁全程核查：监视线程握住 queue 锁的整个核查期间，任何在途
    /// dispatch 都无法完成排空或返回（pop/出口复检都要这把锁），因此
    /// `draining=false && 队列非空 && in_flight==0`（push 方先 +1 再入队、返回
    /// 前 -1）在修法代码中不可达——swap 失败有在任排空者出口复检兜底；修复前
    /// 出口 store(false) 无复检，孤儿可滞留，即被咬住。检出置旗标不 panic，
    /// 免毒锁噪声；终态由测试线程显式断言。
    #[test]
    fn concurrent_dispatch_never_orphans_actions() {
        const THREADS: usize = 8;
        const ITERS: usize = 10000;
        let store = Arc::new(Store::new(TickFeature, NopRuntime));
        let inner = Arc::clone(&store.inner);
        let done = Arc::new(AtomicBool::new(false));
        let orphan = Arc::new(AtomicBool::new(false));
        let monitor = {
            let inner = Arc::clone(&inner);
            let done = Arc::clone(&done);
            let orphan = Arc::clone(&orphan);
            std::thread::spawn(move || {
                let mut iter = 0usize;
                while !done.load(Ordering::SeqCst) && !orphan.load(Ordering::SeqCst) {
                    let queue = lock(&inner.queue);
                    if !queue.is_empty()
                        && !inner.draining.load(Ordering::SeqCst)
                        && inner.in_flight.load(Ordering::SeqCst) == 0
                    {
                        orphan.store(true, Ordering::SeqCst);
                    }
                    drop(queue);
                    iter += 1;
                    if iter.is_multiple_of(64) {
                        std::thread::yield_now(); // 防独占：周期性让出，其余迭代贴身采样
                    }
                }
            })
        };
        let mut handles = Vec::new();
        for _ in 0..THREADS {
            let store = Arc::clone(&store);
            handles.push(std::thread::spawn(move || {
                for _ in 0..ITERS {
                    store.dispatch(Tick::Tick);
                }
            }));
        }
        for h in handles {
            h.join().expect("dispatch 线程不得 panic");
        }
        // 静止态终检（确定性判据）：全部 dispatch 已返回，滞留孤儿必在此现形
        assert!(!orphan.load(Ordering::SeqCst), "孤儿 action：无在途 dispatch 且排空旗已放，队列仍滞留 action（监视线程持锁核实）");
        assert_eq!(inner.in_flight.load(Ordering::SeqCst), 0);
        assert!(!inner.draining.load(Ordering::SeqCst), "静止态排空旗必须已放");
        assert!(lock(&inner.queue).is_empty(), "静止态队列必须空：滞留即孤儿");
        // 全部 dispatch 返回 = 排空至 idle：滞留即少计
        assert_eq!(*store.snapshot(), (THREADS * ITERS) as u32);
        done.store(true, Ordering::SeqCst);
        monitor.join().expect("监视线程不得 panic");
    }

    // ── 停机闸（close 语义）──────────

    /// close 后 dispatch 恒 no-op——状态不再前进；dispatch 签名不变，
    /// 无 Err 感染调用面。
    #[test]
    fn close_makes_dispatch_noop() {
        let store = Store::new(TickFeature, NopRuntime);
        store.close();
        assert!(store.is_closed());
        store.dispatch(Tick::Tick);
        store.dispatch(Tick::Tick);
        assert_eq!(*store.snapshot(), 0);
    }

    /// 闸在共享 push 路径——闭门后 send 返回 false 且计数，
    /// bool 契约不撒谎（不报「已入队」实为弃）。
    #[test]
    fn closed_sender_returns_false_and_counts() {
        let store = Store::new(TickFeature, NopRuntime);
        let sender = store.sender();
        assert!(sender.send(Tick::Tick), "闭门前 send 照常入队");
        store.close();
        assert!(!sender.send(Tick::Tick), "闭门后 send 必须显式 false");
        assert_eq!(store.rejected_after_close(), 1);
    }

    /// 闭门计数 fail-visible，dispatch 与 send 两条 push 路径的拒绝都计入。
    #[test]
    fn rejection_counter_covers_both_push_paths() {
        let store = Store::new(TickFeature, NopRuntime);
        let sender = store.sender();
        store.close();
        store.dispatch(Tick::Tick);
        store.dispatch(Tick::Tick);
        let _ = sender.send(Tick::Tick);
        let _ = sender.send(Tick::Tick);
        let _ = sender.send(Tick::Tick);
        assert_eq!(store.rejected_after_close(), 5);
        assert_eq!(*store.snapshot(), 0, "被拒 action 不得推进状态");
    }

    /// close() 内同步排空至 idle——关门时点的存量队列被执行而非
    /// 随 drop 静默弃队（与 fx 停机序「fx.close→await idle→flush→drop」对齐）。
    /// 白盒注入：单线程下队列仅在排空中非空，直推 queue 复现「存量未排」瞬态。
    #[test]
    fn close_drains_stock_queue_to_idle() {
        let store = Store::new(TickFeature, NopRuntime);
        for _ in 0..3 {
            // 白盒注入：直推队列须带来源标签（回灌深度预算的入队纪律）
            lock(&store.inner.queue).push_back((Tick::Tick, ActionOrigin::External));
        }
        store.close();
        assert_eq!(*store.snapshot(), 3, "存量 action 必须被 close 排空执行");
        assert!(lock(&store.inner.queue).is_empty(), "close 返回即 idle");
        assert_eq!(store.rejected_after_close(), 0, "存量执行不是拒绝");
    }

    /// close 幂等——重复关门只多一次空排空，状态与计数不受扰。
    #[test]
    fn close_is_idempotent() {
        let store = Store::new(TickFeature, NopRuntime);
        store.close();
        store.close();
        assert!(store.is_closed());
        store.dispatch(Tick::Tick);
        assert_eq!(*store.snapshot(), 0);
        assert_eq!(store.rejected_after_close(), 1);
    }

    // ── 效果出参化（EffectBuf）──────────────

    /// EffectBuf clear 保容量——效果路径稳态零新增分配的前提。
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

    // ── make_mut CoW 快照落位 ────────

    /// CoW 分支语义（快照一致性自证）：外部持有者存续期间旧快照冻结——持有者
    /// 永见其取得时点的自洽状态，后续 action 的落位不得波及（快照即模型）。
    #[test]
    fn make_mut_cow_freezes_held_snapshot() {
        let store = Store::new(TickFeature, NopRuntime);
        store.dispatch(Tick::Tick);
        let held = store.snapshot();
        store.dispatch(Tick::Tick);
        assert_eq!(*held, 1, "外部持有者必须永见旧快照（CoW 冻结）");
        assert_eq!(*store.snapshot(), 2, "新快照照常前进");
    }

    /// 原地分支命中（稳态零分配自证）：无外部持有者时连续 dispatch 复用同一
    /// 分配（指针不变）且稳态强计数恒 1——快照分配税消除的直接断言。
    #[test]
    fn make_mut_inplace_steady_state_reuses_allocation() {
        let store = Store::new(TickFeature, NopRuntime);
        store.dispatch(Tick::Tick);
        let p1 = Arc::as_ptr(&*lock(&store.inner.state));
        store.dispatch(Tick::Tick);
        let p2 = Arc::as_ptr(&*lock(&store.inner.state));
        assert_eq!(p1, p2, "稳态无外部持有者：make_mut 必须原地写入，禁每 action 换分配");
        assert_eq!(Arc::strong_count(&*lock(&store.inner.state)), 1, "稳态强计数必须回落 1");
    }

    /// 旧 S 析构必须发生在 state 锁外（锁消融形态回归）：
    /// ——S:Drop panic 不得毒化 state mutex（毒锁 = store 永久变砖）。
    #[derive(Clone)]
    struct PanicOnDrop {
        armed: Arc<AtomicBool>,
    }
    impl Drop for PanicOnDrop {
        fn drop(&mut self) {
            // 首次析构（=落位时被换出的旧快照）panic；其后实例静默，保测试
            // 自身与 store drop 不被噪声引爆
            if !self.armed.swap(true, Ordering::SeqCst) {
                panic!("S:Drop drill：旧快照析构必须发生在 state 锁外（毒锁契约）");
            }
        }
    }

    struct PanicDropFeature {
        armed: Arc<AtomicBool>,
    }
    impl Reducer for PanicDropFeature {
        type State = PanicOnDrop;
        type Action = Tick;
        type Op = Infallible;
        fn initial(&self) -> PanicOnDrop {
            PanicOnDrop { armed: Arc::clone(&self.armed) }
        }
        fn reduce(
            &self,
            s: &PanicOnDrop,
            _: Tick,
            _out: &mut EffectBuf<Tick, Infallible>,
        ) -> PanicOnDrop {
            PanicOnDrop { armed: Arc::clone(&s.armed) }
        }
    }

    #[test]
    fn snapshot_replacement_drops_old_value_outside_state_lock() {
        let store = Store::new(
            PanicDropFeature { armed: Arc::new(AtomicBool::new(false)) },
            NopRuntime,
        );
        let first = std::panic::catch_unwind(AssertUnwindSafe(|| store.dispatch(Tick::Tick)));
        assert!(first.is_err(), "旧快照析构 panic 必须如实传播（fail-visible）");
        // 析构在锁外 ⇒ state mutex 未中毒 ⇒ store 继续可用（毒锁形态下此处必 panic）
        store.dispatch(Tick::Tick);
        store.dispatch(Tick::Tick);
    }

    // ── 残留弃队留痕 ────────────────────

    enum DropAction {
        Spin,
    }

    struct DropSpinFeature;

    impl Reducer for DropSpinFeature {
        type State = u32;
        type Action = DropAction;
        type Op = Infallible;
        fn initial(&self) -> u32 {
            0
        }
        fn reduce(&self, s: &u32, _: DropAction, out: &mut EffectBuf<DropAction, Infallible>) -> u32 {
            out.push(Effect::Emit(DropAction::Spin));
            *s
        }
    }

    struct DropNopFx;

    impl EffectRuntime<DropAction, Infallible> for DropNopFx {
        fn run(&mut self, _: Effect<DropAction, Infallible>, _: &ActionSender<DropAction>) {}
    }

    /// 预算止步残留 × 直接 drop：弃队必须留痕（并入预算计数），禁静默。
    /// 读窗 = inner Arc 存续期（与 rejected_after_close 的停机窗同类语义）——
    /// 白盒持 inner 引用跨越 Store drop 验证计数上浮。
    #[test]
    fn budget_residual_discard_is_counted_on_store_drop() {
        let store = Arc::new(Store::new(DropSpinFeature, DropNopFx));
        store.dispatch(DropAction::Spin);
        assert_eq!(store.feedback_budget_exceeded(), 1, "前置：预算已止步");
        assert_eq!(store.pending_actions(), 1, "前置：残留尚在队列");
        let inner = Arc::clone(&store.inner);
        drop(store);
        assert_eq!(
            inner.feedback_exceeded.load(Ordering::SeqCst),
            2,
            "drop 弃队必须留痕（1 预算触发 + 1 弃队），禁静默"
        );
    }

    // ── 可观测读侧 ─────────────────────────────────────────────────────

    /// 队列深公开读侧：状态可查。同步排空语义下 dispatch
    /// 返回即 idle（恒 0），非 0 值只在排空循环内可见：经订阅回调重入 dispatch
    /// 验证「入队未排空」的窗口深度可读。
    #[test]
    fn pending_actions_visible_from_reentrant_dispatch() {
        let store = Arc::new(Store::new(TickFeature, NopRuntime));
        let observed = Arc::new(AtomicUsize::new(0));
        let observer = Arc::clone(&observed);
        let reentrant = Arc::clone(&store);
        let fired = Arc::new(AtomicBool::new(false));
        let guard = Arc::clone(&fired);
        let _keep = store.subscribe(move |_| {
            // 只在首个通知里重入一次，否则重入 action 的通知再触发重入 = 无限环
            if !guard.swap(true, Ordering::SeqCst) {
                reentrant.dispatch(Tick::Tick); // 本任排空循环持旗，重入只入队
                observer.store(reentrant.pending_actions(), Ordering::SeqCst);
            }
        });
        assert_eq!(store.pending_actions(), 0, "静止态队列深为 0");
        store.dispatch(Tick::Tick);
        assert_eq!(
            observed.load(Ordering::SeqCst),
            1,
            "回调内重入入队后队列深必须可见为 1"
        );
        assert_eq!(store.pending_actions(), 0, "dispatch 返回即同步排空至 idle");
        assert_eq!(*store.snapshot(), 2, "重入 action 被同一排空循环执行");
    }

    /// 订阅回调 panic 摘除的成因留痕计数（摘除留痕+显式
    /// 计数读侧可见）：摘除只剩 subscriber_count 下跌可查时无成因，
    /// 本计数补齐成因面。摘除一次计 1；已摘除者不再重复计入。
    #[test]
    fn subscriber_panic_removal_is_counted() {
        let store = Store::new(TickFeature, NopRuntime);
        assert_eq!(store.subscriber_panics(), 0);
        let _poison = store.subscribe(|_| panic!("subscriber bug"));
        let _healthy = store.subscribe(|_| {});
        store.dispatch(Tick::Tick); // 毒订阅者 panic → 摘除 + 留痕
        assert_eq!(store.subscriber_panics(), 1, "panic 摘除必须显式留痕");
        assert_eq!(store.subscriber_count(), 1, "毒订阅者已摘除");
        store.dispatch(Tick::Tick); // 健康者照常通知
        assert_eq!(store.subscriber_panics(), 1, "已摘除者不得重复计数");
    }
}
