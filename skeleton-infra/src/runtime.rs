//! # skeleton-infra :: runtime
//!
//! [`SyncStore`]：契约 trait `skeleton::Store` 的 std-only 参考实现（单写者
//! 队列 + 快照广播 + 同步排空环）。契约承诺（同步排空/快照一致性/订阅/停机闸/
//! fail-visible/回灌有界）见 `skeleton::Store` 类型文档；本模块承载兑现承诺的
//! 机器：CoW 快照落位、锁消融排空环、回灌深度预算、本地订阅档、异步信号
//! 通知档（ADR 0008）、审计事件流发射点（值面在 [`crate::audit`]）与取证八读数。
//!
//! std-only：`runtime` feature 零三方依赖（cargo tree 证明口径见 crate 文档）。

use std::cell::{Cell, UnsafeCell};
use std::collections::{HashSet, VecDeque};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};

use skeleton::notify::{Notify, NotifySink};
use skeleton::{
    ActionSender, Effect, EffectBuf, EffectRuntime, Infallible, Reducer, Restore, Store,
    Subscription,
};

use crate::audit::{AuditEvent, AuditEventKind, AuditSink, AuditTicket};

// 关注面切缝（可维护性 R1）：排空环主文件只留 store 机器；本地订阅 TLS 机制
// 与审计发射沿自然缝独立成子模块（对内 pub(super)，公共面经本模块 re-export
// 原路不变）。
mod audit_emit;
mod local_subs;
mod notify;

use audit_emit::{audit_emit, audit_enqueued};
pub use local_subs::LocalSubscription;
use local_subs::{local_notify, next_local_sub_id};
use notify::{notify_async, NotifySinkSlot};

/// std Mutex 取锁：中毒 = 持锁方已 panic、状态不可信——按 fail-fast 纪律传播
/// panic，禁静默吞掉毒锁继续跑。（crate 内共享：testing 域 TestStore 同纪律取锁。）
pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().expect(
        "skeleton Store state lock poisoned（S::Clone/守卫 panic 违约——实例整体报废且状态不可取出，见 Store 文档「Panic 契约」节）: prior panic while holding the lock",
    )
}

/// 构造回灌句柄：仅装配构造器（[`SyncStore::new`] / [`SyncStore::restore_from`]
/// 共用装配段）经
/// [`Arc::new_cyclic`] 调用一次，产物缓存为 [`StoreInner::back`]——`sender()`
/// 克隆复用、排空循环借用，禁每 action 新建 Arc 闭包（稳态零新增分配的构成部分）。
fn back_sender<S: Clone + Send + Sync + 'static, A: Send + 'static, Op: Send + 'static>(
    weak: &Weak<StoreInner<S, A, Op>>,
) -> ActionSender<A> {
    let weak = weak.clone();
    ActionSender::new(move |action| match weak.upgrade() {
        Some(inner) => {
            // 与 dispatch 同闸——闭门 sender dispatch 显式 false 并计数；
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
    })
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
        Self {
            value: UnsafeCell::new(value),
        }
    }

    /// SAFETY: 调用方必须持排空权（drain 内，DrainGuard 未 disarm），且不得让
    /// 返回的 `&mut T` 活到排空权释放之后。重入路径（reducer/回调里 dispatch/
    /// sender dispatch）只入队即返、到不了此处，无别名风险。
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
    /// 订阅集版本号：订阅增删（[`SyncStore::subscribe`]）、票据注销（[`Subscription`] Drop）、
    /// 回调 panic 摘除三路自增，快照缓存失效依据。Arc 装载使注销票据持句柄可 bump。
    subs_version: Arc<AtomicUsize>,
    /// 本地订阅闸（[`SyncStore::subscribe_local`]）：零本地订阅时通知路径一次
    /// relaxed 读即短路，不触达任何线程本地表。只作闸，不作精确计数读侧。
    local_subs: Arc<AtomicUsize>,
    /// 本地订阅跨线程 miss 计数（读侧 [`SyncStore::local_missed_notifications`]）：
    /// 通知段闸>0 但本线程表查无该 store 登记即 +1——「本地档挂注册线程、
    /// dispatch 来自他线程」的显形。
    local_missed: AtomicU64,
    /// 异步投递口注册表（ADR 0008）：表存 Weak、票据 Drop 经共享句柄即时摘表
    /// （与 `subs` 同构）；槽位与投递机制在 `notify` 子模块。
    notify_sinks: Arc<Mutex<Vec<Weak<NotifySinkSlot>>>>,
    /// 异步订阅闸（门控快路径，投递段一次 Acquire load 即返）：零异步订阅者时
    /// `state_version` 不自增——77ns 无订阅锚的守卫。`NotifySinkSlot::alive`
    /// 裁决保证摘除（panic 摘除 / 票据 Drop 两路竞争）只回落一次。
    notify_subs: Arc<AtomicUsize>,
    /// 状态版本水位（ADR 0008 §5.2 门控语义）：仅在异步订阅在册的落位拍自增
    /// （版本号 = 状态代数），无人订阅期间冻结、增减订不回退。经 publish 出站，
    /// 无独立读侧（本 ADR 唯一新读侧是 `notify_published`）。
    state_version: AtomicU64,
    /// 异步投递累计次数（成功投递逐口逐拍 +1，成本可见）。读侧
    /// [`SyncStore::notify_published`]。
    notify_published: AtomicU64,
    /// store 身份号（本地订阅在注册线程 TLS 表中的键）：全局单调，进程内唯一。
    store_id: u64,
    /// 单写者排空守卫（确定性：同序 action 序列恒同终态；重入回灌只入队）
    draining: AtomicBool,
    /// 停机闸：置位后 push 路径（dispatch 与 sender().dispatch 同闸）恒拒——
    /// dispatch no-op、sender dispatch 显式 false，拒绝计数 fail-visible。Emit 回灌是已
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
    /// 解释器 panic，恢复后 clear_poison 复位毒位）。读侧 [`SyncStore::runtime_panic_count`]。
    runtime_panics: AtomicU64,
    /// reducer panic 计数：排空环 catch_unwind 观察点计数后原样 resume（传播
    /// 语义不变，只加留痕）。读侧 [`SyncStore::reducer_panic_count`]——四条
    /// panic/异常路径（解释器/订阅回调/回灌超限/reducer）的最后一处读侧收口。
    reducer_panics: AtomicU64,
    /// 回灌深度预算触发计数：单任排空连续回灌超 [`FEEDBACK_DEPTH_BUDGET`]
    /// 即停排+自增。读侧 [`SyncStore::feedback_budget_exceeded`]。
    feedback_exceeded: AtomicU64,
    /// Cancel 寻址校验：本 store 见过的 `Run` id 登记（reducer 发出即记，
    /// 摘除不可行——Run 的自然完成核心不可见，见 `SyncStore::cancel_unknown_id`
    /// 文档）。只在 Cancel/Run 效果流经转发环时触达，零效果路径零开销。
    run_ids: Mutex<HashSet<u64>>,
    /// Cancel 寻址校验计数：`Effect::Cancel` 引用从未发出的 `Run` id 即 +1
    /// （id 纪律漂移的直接证据：打错数字、撞车后引用了他域 id、Cancel 早于
    /// Run 的时序 bug）。读侧 [`SyncStore::cancel_unknown_id`]。
    cancel_unknown_id: AtomicU64,
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
    /// 审计事件流前订被覆盖计数（读侧 [`SyncStore::audit_preempted`]）：
    /// `subscribe_events` 落槽时槽内已有订阅即 +1——单槽二次订阅静默杀掉
    /// 前订的显形。
    audit_preempted: AtomicU64,
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
/// 副作用（最小环直接配 [`NoFx`](skeleton::NoFx)，见其文档示例）。
///
/// # 闭门后的入口对照（停机闸）
///
/// | 入口 | 闭门后行为 | fail-visible 面 |
/// |---|---|---|
/// | [`Self::dispatch`] | 恒 no-op（签名不引入 Err） | 计入 [`Self::rejected_after_close`] |
/// | [`ActionSender::dispatch`] | 返回 `false`（显式拒绝，`#[must_use]` 兜底） | 同一计数 |
/// | [`Self::subscribe`] / [`Self::subscribe_local`] | 照常登记、票据照常注销，但闭门后永无 dispatch 触发——通知静默（已知取舍，见下） | 计入 [`Self::subscriptions_after_close`]；`subscriber_count` 仍可查 |
/// | [`Self::try_subscribe`] | 返回 `Err(StoreClosed)`——显式拒绝、不登记（推荐停机序主路径） | 返回值即信号；登记未发生，[`Self::subscriptions_after_close`] 不计 |
/// | [`Notify::subscribe_notify`] | 同上行：登记照常、闭门后永无落位拍——信号静默 | 计入 [`Self::subscriptions_after_close`] |
///
/// dispatch 的静默 no-op 与 sender dispatch 的显式 `false` 是**保签名**与**停机显式化**
/// 之间的显式取舍：dispatch 面向动作源（不感染调用面），sender dispatch 面向异步回灌
/// （停机窗必须可观测）；两者闭门后共用同一拒绝计数，无观测死角。闭门
/// subscribe 照常登记是「票据即注销」模型的自然推论——通知静默可经
/// `subscriber_count` 交叉查证（已知语义，非缺陷）。
///
/// # 订阅面三档（ADR 0008）
///
/// - **跨线程同步**：[`Self::subscribe`]——回调在 dispatch 线程同步执行，需 `Send`；
/// - **本地线程亲和**：[`Self::subscribe_local`]——免 `Send`，只在注册线程触发；
/// - **异步信号**：[`Notify::subscribe_notify`]——投递口每拍收一个单调版本号
///   （水位信号，非事件流），消费侧见版本即 [`Store::snapshot`] 拉取现态。
///
/// 投递序：**异步先行、同步回调随后**（异步投递排在通知段开头，同步档内部
/// 既有顺序不动）。**异步档不救同步档**：慢同步订阅者的耗时照常计入 dispatch；
/// 慢消费者靠投递口内自备通道/工作线程解耦（O(1) 是宿主契约义务，框架无法
/// 检测也无法阻止阻塞）。
///
/// # Panic 契约
///
/// `S::Clone` 必须 panic-free：外部持有快照（强计数 ≥2）时，状态落位走
/// `Arc::make_mut` 克隆分支，`S::clone` 在 state 锁内执行——clone panic 即毒锁，
/// snapshot/dispatch 从此恒 panic，**实例整体报废且状态不可取出**。这是文档化
/// 显式契约而非可恢复路径：state 锁内「只剩 memcpy 与计数」的形态纪律不为用户
/// State 的 panic-free 性兜底。对照：`S::Drop` 的 panic 已由「旧快照锁外析构」
/// 排除（见 `drain_owned` 落位段）；解释器 panic 不报废实例（毒锁恢复+计数，
/// 见 [`Self::runtime_panic_count`]）；reducer panic 同不报废（计数+审计现场，
/// 见 [`Self::reducer_panic_count`]——action 本体已出队随栈展开丢失，状态不前进）。
pub struct SyncStore<S, A, Op = Infallible> {
    inner: Arc<StoreInner<S, A, Op>>,
}

/// 停机闸已闭：订阅登记窗口已关（[`SyncStore::try_subscribe`] 的显式失败面）。
/// 与 [`crate::fx::ScopeClosed`]、`RegisterError::Closed` 同族停机语义——
/// 框架其余停机入口全都显式失败，本类型补齐 subscribe 面的缺口。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreClosed;

impl std::fmt::Display for StoreClosed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("store closed for new subscriptions (accepting gate)")
    }
}

impl std::error::Error for StoreClosed {}

/// 回灌深度预算（const 64）：单任排空内连续处理的回灌链深度上限。
/// 有界链（Undo→Replay、表单回写）深度个位数；自指回灌环（reducer 恒 Emit
/// 自身、解释器经 [`ActionSender::dispatch`] 同栈回灌）属用户程序病理，预算点
/// 止步+计数上浮（[`SyncStore::feedback_budget_exceeded`]）。
const FEEDBACK_DEPTH_BUDGET: u64 = 64;

/// 入队来源标签：外部流量 = [`ActionOrigin::External`]（深度清零）；
/// 同栈回灌重入 = [`ActionOrigin::Feedback`]（携带入队时深度）。
///
/// 「同栈」判定不看调用者身份，只看 [`REENTRANT_DEPTH`] 线程局部：排空线程
/// 处理某 action 期间（store_id 匹配），该线程上发生的一切入队——Emit 回灌、
/// 解释器 `back.dispatch`、订阅回调重入 dispatch——一律计深（预算按
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
    /// 闭门拒绝（已计入 [`SyncStore::rejected_after_close`]）
    Rejected,
    /// 已入队，排空权归在任排空者（其循环必覆盖本 push，论证见 [`enqueue`]）
    Queued,
    /// 已入队且夺得排空权，`Some` 为同临界区内 pop 出的首个 action（带来源标签）
    Draining(Option<(A, ActionOrigin)>),
}

/// 共享 push 路径（[`SyncStore::dispatch`] 与 [`ActionSender::dispatch`] 同闸同窗）。
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
impl<S: Clone + Send + Sync + 'static, A: Send + 'static, Op: Send + 'static> Store
    for SyncStore<S, A, Op>
{
    type State = S;
    type Action = A;
    type Op = Op;

    /// 唯一写入口；同步排空至 idle（同序 action 序列恒同终态）。
    /// 闭门后恒 no-op 并计入 [`Self::rejected_after_close`]，签名不变（取舍
    /// 对照 [`ActionSender::dispatch`] 的显式 `false`——见类型文档「闭门后的
    /// 入口对照」表）。
    fn dispatch(&self, action: A) {
        // 闸序（锁消融批论证）：closed 单调 false→true，load Acquire——见
        // [`SyncStore::close`] 的 Release 配对与锁内复检论证。
        if self.is_closed() {
            self.inner
                .rejected_after_close
                .fetch_add(1, Ordering::SeqCst);
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
    fn snapshot(&self) -> Arc<S> {
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
    /// # use skeleton::{EffectBuf, Infallible, NoFx, Reducer, Store as _};
    /// # use skeleton_infra::runtime::SyncStore;
    /// # struct Counter;
    /// # impl Reducer for Counter {
    /// #     type State = u32; type Action = u32; type Op = Infallible;
    /// #     fn initial(&self) -> u32 { 0 }
    /// #     fn reduce(&self, s: &u32, a: u32, _out: &mut EffectBuf<u32, Infallible>) -> u32 { s + a }
    /// # }
    /// # let store = SyncStore::new(Counter, NoFx);
    /// store.subscribe(|_| {}); // ERROR: unused `Subscription` that must be used——丢弃即死订阅
    /// ```
    ///
    /// 绑定持有即持续收通知：
    ///
    /// ```
    /// # use skeleton::{EffectBuf, Infallible, NoFx, Reducer, Store as _};
    /// # use skeleton_infra::runtime::SyncStore;
    /// # struct Counter;
    /// # impl Reducer for Counter {
    /// #     type State = u32; type Action = u32; type Op = Infallible;
    /// #     fn initial(&self) -> u32 { 0 }
    /// #     fn reduce(&self, s: &u32, a: u32, _out: &mut EffectBuf<u32, Infallible>) -> u32 { s + a }
    /// # }
    /// # let store = SyncStore::new(Counter, NoFx);
    /// let _ticket = store.subscribe(|_| {}); // 绑定即持有，Drop 才注销
    /// store.dispatch(1);
    /// store.close();
    /// ```
    ///
    /// 停机序推荐走 [`SyncStore::try_subscribe`]（闭门显式 `Err`，见其文档）；
    /// 本方法保留签名作迁移期兼容面——闭门登记照常 + 计数
    /// （[`SyncStore::subscriptions_after_close`]），不静默。
    fn subscribe(&self, f: impl FnMut(&S) + Send + 'static) -> Subscription<S> {
        // 闭门登记显形（登记照常生效，但永无通知——见类型文档对照表）
        if self.is_closed() {
            self.inner
                .subscriptions_after_close
                .fetch_add(1, Ordering::SeqCst);
        }
        let slot = Arc::new(SubscriptionSlot {
            notify: Mutex::new(Box::new(f)),
        });
        lock(&self.inner.subs).push(Arc::downgrade(&slot));
        self.inner.subs_version.fetch_add(1, Ordering::Release);
        let dead = Arc::downgrade(&slot);
        let subs = Arc::clone(&self.inner.subs);
        let version = Arc::clone(&self.inner.subs_version);
        // 存活令牌随闭包持有：表项存 Weak，强引用在票据 Drop（闭包释放）前
        // 保 slot 存活——旧形态 `_slot: Arc<SubscriptionSlot>` 字段的等价物。
        let keep_alive = Arc::clone(&slot);
        Subscription::new(move || {
            lock(&subs).retain(|w| !Weak::ptr_eq(w, &dead));
            version.fetch_add(1, Ordering::Release);
            drop(keep_alive);
        })
    }

    /// 可克隆回灌句柄（解释器异步回灌的唯一通道）。克隆 StoreInner 缓存句柄
    /// （Arc 引用计数），不重建闭包。
    fn sender(&self) -> ActionSender<A> {
        self.inner.back.clone()
    }

    /// 停机闸：置 closed 后同步排空至 idle（与宿主装配序「解释器 close→排空
    /// idle→flush→drop」对齐）——drop 不再静默弃队。
    /// 在途他线程 drain 持排空权时本调用交由其排至 idle（闭门后无新入队，其
    /// 循环必达空，自指回灌环受回灌深度预算止步），交接语义同 drain 出口协议。
    /// 幂等。闭门后 push 恒拒：dispatch no-op、send 显式 `false`、subscribe
    /// 照常登记但永不触发——三入口对照见类型文档「闭门后的入口对照」表。
    fn close(&self) {
        // Release 配对 push 路径的 Acquire 复检。线性化不靠原子序：close 的
        // drain 与 dispatch 的 push 在 queue 锁上串行——stale false（迟见的
        // 闭门）只会让 push 照常入队，随后 close 的 drain 必经锁覆盖该 push；
        // 反向（未见 store 即读到 true）单变量一致性上不可能。Release 只保证
        // 「读到 true 时闸前所有写可见」，单调闸下这是唯一的错误方向防护。
        self.inner.closed.store(true, Ordering::Release);
        drain(&self.inner);
    }

    /// 停机闸状态。
    fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::Acquire)
    }
}

/// 异步（信号式）通知档的登记面实现（ADR 0008）：与 [`Store`] 正交的独立端口
/// ——「六方法即全契约」纪律不因此扩员。语义细则（版本号水位/门控/失败面）见
/// `skeleton::notify` 模块文档；投递在排空环通知段、同步回调**之前**（异步档
/// 不救同步档：同步订阅者耗时照常计入 dispatch）。
impl<S: Clone + Send + Sync + 'static, A: Send + 'static, Op: Send + 'static> Notify<S>
    for SyncStore<S, A, Op>
{
    /// 登记投递口（信号式异步档）。载荷 = 单调状态版本号：在册期间逐拍 +1、
    /// 无人订阅期间冻结、增减订不回退；消费侧见版本变化即 [`Store::snapshot`]
    /// 拉取现态，版本跳跃 = 中间拍被合并（重取现态自愈，撕裂判定依据）。
    ///
    /// 失败面（复用既有读侧，ADR §5.5，不造第二套）：投递口 panic ⇒
    /// `catch_unwind` 捕获 + fail-closed 摘除 + [`Self::subscriber_panics`] /
    /// [`Self::subscriber_panic_last_cause`] 留痕（同订阅回调纪律）；闭门后
    /// 登记照常、永不发信号 ⇒ [`Self::subscriptions_after_close`]；投递成本 ⇒
    /// [`Self::notify_published`]。投递口 O(1) 是宿主契约义务（
    /// `skeleton::notify::NotifySink` 文档），框架无法检测也无法阻止阻塞
    /// ——重活请在投递口内自备通道/工作线程移出。
    ///
    /// # Examples
    ///
    /// 合并式投递口挂载后，dispatch 每拍投递一位版本号（水位从 1 起步）；
    /// 成本读侧同步累计：
    ///
    /// ```
    /// use skeleton::notify::{Notify, NotifySink};
    /// use skeleton::{EffectBuf, Infallible, NoFx, Reducer, Store as _};
    /// use skeleton_infra::runtime::SyncStore;
    /// use std::sync::atomic::{AtomicU64, Ordering};
    /// use std::sync::Arc;
    ///
    /// struct Counter;
    /// impl Reducer for Counter {
    ///     type State = u64; type Action = u64; type Op = Infallible;
    ///     fn initial(&self) -> u64 { 0 }
    ///     fn reduce(&self, s: &u64, a: u64, _out: &mut EffectBuf<u64, Infallible>) -> u64 { s + a }
    /// }
    ///
    /// #[derive(Clone)]
    /// struct Latest(Arc<AtomicU64>);
    /// impl NotifySink for Latest {
    ///     fn publish(&self, version: u64) { self.0.store(version, Ordering::Relaxed); }
    /// }
    ///
    /// let store = SyncStore::new(Counter, NoFx);
    /// let sink = Latest(Arc::new(AtomicU64::new(0)));
    /// let _ticket = store.subscribe_notify(sink.clone()); // 绑定持有，Drop 才注销
    /// store.dispatch(7);
    /// assert_eq!(sink.0.load(Ordering::Relaxed), 1); // 首拍版本 = 1
    /// store.dispatch(2);
    /// assert_eq!(*store.snapshot(), 9);
    /// assert_eq!(store.notify_published(), 2);
    /// # store.close();
    /// ```
    fn subscribe_notify(&self, sink: impl NotifySink) -> Subscription<S> {
        // 闭门登记显形（与 subscribe 同一纪律）：登记照常生效，但闭门后
        // 排空环不再产生落位拍——永无信号。
        if self.is_closed() {
            self.inner
                .subscriptions_after_close
                .fetch_add(1, Ordering::SeqCst);
        }
        notify::register(
            &self.inner.notify_sinks,
            &self.inner.notify_subs,
            Box::new(sink),
        )
    }
}

impl<S: Clone + Send + Sync + 'static, A: Send + 'static, Op: Send + 'static> SyncStore<S, A, Op> {
    /// 装配最小运转环：`root` 是**根** Reducer（[`Reducer::initial`] 产出完整根
    /// 状态），`fx` 是效果解释器（核心只转发，永不解释）。
    ///
    /// 与 `compose` 域的 `combine` 的关系：`combine` 产物是**状态分片不是根**
    /// （只实现 `Fragment`，无 `initial`），不能作 `root` 传入——分片作根即
    /// **E0277 编译期拒绝**（`Fragment` ≠ `Reducer`，错误信息自带修复指引）。
    /// 正确用法：父 Feature 的 `initial` 显式构造含子切片初值的根状态、
    /// `reduce` 内委托分片，父 Feature 本体作 `root`（完整形态见
    /// `tests/combine_teststore.rs` 固定装置）。
    ///
    /// 构造即接线：回灌句柄经 `Arc::new_cyclic` 缓存（[`SyncStore::sender`] 克隆复用），
    /// 初始状态 = `root.initial()`；此后 [`SyncStore::dispatch`] / `sender().dispatch` 驱动。
    ///
    /// # 界不对称（有意，接口维 R1）
    ///
    /// 根 Reducer 要 `Send + Sync`、解释器只要求 `Send`，依据在 std 类型律：
    /// store 部件经 `Arc<StoreInner>` 跨线程共享，`Arc<T>: Sync` 要求 `T: Sync`，
    /// 故 reducer 的 trait 对象必须挂全 `Send + Sync`；解释器装箱在 `Mutex`
    /// 之后（`Mutex<T>: Sync` 只要求 `T: Send`）——锁后独占访问、从不并发
    /// 共享，能安全移动即足，多要 `Sync` 是白缴的税。
    ///
    /// # 解释器观测的惯用形（fx 按值收走后）
    ///
    /// `new` 按值拿走解释器——产品代码要观测解释器行为（处理过哪些 op、
    /// 何时处理）时，把观测面放进共享句柄：解释器持 `Arc<Mutex<…>>` 克隆份，
    /// 宿主持原份，装配后随处可读（测试语境的收口见 `testing::TestStore`；
    /// 本例是产品语境的手写形）：
    ///
    /// ```
    /// use skeleton::{ActionSender, Effect, EffectBuf, EffectId, EffectRuntime, Reducer, Store as _};
    /// use skeleton_infra::runtime::SyncStore;
    /// use std::convert::Infallible;
    /// use std::sync::{Arc, Mutex};
    ///
    /// #[derive(Debug, PartialEq)]
    /// enum Op { Ping }
    ///
    /// struct Pinger;
    /// impl Reducer for Pinger {
    ///     type State = u32;
    ///     type Action = u32;
    ///     type Op = Op;
    ///     fn initial(&self) -> u32 { 0 }
    ///     fn reduce(&self, s: &u32, _a: u32, out: &mut EffectBuf<u32, Op>) -> u32 {
    ///         out.push(Effect::Run { id: EffectId::new(1), op: Op::Ping });
    ///         *s
    ///     }
    /// }
    ///
    /// /// 解释器外壳：执行逻辑外的观测面（此处记录处理序；真实宿主在同一位置执行）。
    /// struct Observed {
    ///     seen: Arc<Mutex<Vec<Op>>>,
    /// }
    /// impl EffectRuntime<u32, Op> for Observed {
    ///     fn run(&mut self, effect: Effect<u32, Op>, _back: &ActionSender<u32>) {
    ///         if let Effect::Run { op, .. } = effect {
    ///             self.seen.lock().unwrap().push(op); // 解释后记录（时机自选）
    ///         }
    ///     }
    /// }
    ///
    /// let seen = Arc::new(Mutex::new(Vec::new()));
    /// let store = SyncStore::new(Pinger, Observed { seen: Arc::clone(&seen) });
    /// store.dispatch(1);
    /// assert_eq!(*seen.lock().unwrap(), vec![Op::Ping]); // 装配后随处可读
    /// # store.close();
    /// ```
    pub fn new(
        root: impl Reducer<State = S, Action = A, Op = Op> + Send + Sync + 'static,
        fx: impl EffectRuntime<A, Op> + Send + 'static,
    ) -> Self {
        Self::restore_from(root, fx, Restore::Empty)
    }

    /// 恢复装配（接口维 R1 落点）：自 [`Restore`] 三态播种新 store——
    /// [`skeleton::JournalPort::restore`] 的返回值直传本构造，`FromSnapshot`
    /// 由此获得第一类落点（采用方不再自造协议级合成 action 污染词表）。
    ///
    /// 播种语义（契约②「恢复完备」的兑现）：
    ///
    /// - [`Restore::Empty`]：取 [`Reducer::initial`]，零 apply（[`Self::new`] 即本态）；
    /// - [`Restore::FromInitial`]：自 initial 起步，动作全量经 [`Store::dispatch`] 重放；
    /// - [`Restore::FromSnapshot`]：**快照态直入**（不经 `initial`——快照即真相），
    ///   增量动作经 [`Store::dispatch`] 重放。
    ///
    /// 重放走 dispatch 而非裸 reduce 是契约而非实现细节：效果环照常运转，
    /// `Effect::Emit` 回灌等派生动作由排空环再生，终态与 live 收敛（裸 reduce
    /// 丢派生链）；收敛回归锚见 `tests/journal_port.rs`。
    ///
    /// # Examples
    ///
    /// 快照态 100 + 增量 [10, 5]：快照直入、增量重放，重建后照常 live：
    ///
    /// ```
    /// use skeleton::{EffectBuf, NoFx, Reducer, Restore, Store as _};
    /// use skeleton_infra::runtime::SyncStore;
    /// use std::convert::Infallible;
    ///
    /// struct Counter;
    /// impl Reducer for Counter {
    ///     type State = u64;
    ///     type Action = u64;
    ///     type Op = Infallible;
    ///     fn initial(&self) -> u64 { 0 }
    ///     fn reduce(&self, s: &u64, a: u64, _out: &mut EffectBuf<u64, Infallible>) -> u64 { s + a }
    /// }
    ///
    /// let store = SyncStore::restore_from(
    ///     Counter,
    ///     NoFx,
    ///     Restore::FromSnapshot(100u64, vec![10u64, 5]),
    /// );
    /// assert_eq!(*store.snapshot(), 115);
    /// store.dispatch(1);
    /// assert_eq!(*store.snapshot(), 116);
    /// # store.close();
    /// ```
    // `#[track_caller]`：播种期 `initial()`（`Restore::Empty`/`FromInitial` 两态）的
    // panic 归采用方装配行——分片误用作根已在类型层拒绝（E0277），此处兜的是
    // 用户 `initial` 自身的 panic。
    #[track_caller]
    pub fn restore_from(
        root: impl Reducer<State = S, Action = A, Op = Op> + Send + Sync + 'static,
        fx: impl EffectRuntime<A, Op> + Send + 'static,
        restore: Restore<A, S>,
    ) -> Self {
        let (state, actions) = match restore {
            Restore::Empty => (root.initial(), Vec::new()),
            Restore::FromInitial(actions) => (root.initial(), actions),
            // 快照态直入：不经 initial（快照即真相），增量留给下方 dispatch 重放
            Restore::FromSnapshot(state, actions) => (state, actions),
            // Restore 词表 non_exhaustive（跨 crate match 须通配）：未知恢复
            // 形态 fail-closed 崩溃带因——恢复语义核心不懂的态不能静默当
            // Empty/FromInitial 处理（吞掉快照或重放即状态损坏）
            _ => panic!(
                "skeleton::Restore 新增变体未适配：SyncStore::restore_from 无法 \
                 解释该恢复形态（fail-closed），须在实现侧补臂定义其播种语义"
            ),
        };
        // new_cyclic：back 缓存句柄持 Weak 指回 inner，环构造一步完成
        let inner = Arc::new_cyclic(|weak| StoreInner {
            reducer: Arc::new(root),
            state: Mutex::new(Arc::new(state)),
            runtime: Mutex::new(Box::new(fx)),
            queue: Mutex::new(VecDeque::new()),
            subs: Arc::new(Mutex::new(Vec::new())),
            back: back_sender(weak),
            effect_buf: DrainExclusive::new(EffectBuf::new()),
            notify_cache: DrainExclusive::new(NotifyCache {
                version: 0,
                slots: Vec::new(),
            }),
            subs_version: Arc::new(AtomicUsize::new(0)),
            local_subs: Arc::new(AtomicUsize::new(0)),
            local_missed: AtomicU64::new(0),
            notify_sinks: Arc::new(Mutex::new(Vec::new())),
            notify_subs: Arc::new(AtomicUsize::new(0)),
            state_version: AtomicU64::new(0),
            notify_published: AtomicU64::new(0),
            store_id: NEXT_STORE_ID.fetch_add(1, Ordering::Relaxed),
            draining: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            rejected_after_close: AtomicU64::new(0),
            subscriber_panics: AtomicU64::new(0),
            subscriber_panic_cause: Mutex::new(None),
            subscriptions_after_close: AtomicU64::new(0),
            runtime_panics: AtomicU64::new(0),
            reducer_panics: AtomicU64::new(0),
            feedback_exceeded: AtomicU64::new(0),
            feedback_emitted: AtomicU64::new(0),
            run_ids: Mutex::new(HashSet::new()),
            cancel_unknown_id: AtomicU64::new(0),
            audit_gate: Arc::new(AtomicBool::new(false)),
            audit: Arc::new(Mutex::new(None)),
            audit_seq: AtomicU64::new(0),
            audit_gen: AtomicU64::new(0),
            audit_preempted: AtomicU64::new(0),
            #[cfg(test)]
            in_flight: AtomicUsize::new(0),
        });
        let store = SyncStore { inner };
        // 重放协议（契约②）：动作流经同一装配的 store 逐条 dispatch——效果环
        // 照常运转，Emit 回灌等派生动作由排空环再生；裸 reduce 会丢派生链。
        for action in actions {
            store.dispatch(action);
        }
        store
    }

    /// 登记跨线程订阅，闭门显式拒绝（推荐停机序主路径）：登记路径与
    /// [`Store::subscribe`] 完全一致，唯 `close()` 之后返回 `Err(StoreClosed)`
    /// 而非登记一张永不触发的死票据——停机序写反（先 close 后 subscribe）
    /// 在调用点即显形。与 `ActionSender::dispatch` 的显式 `false`、
    /// [`crate::fx::FxScope::spawn_tracked`] 的 `Err(ScopeClosed)` 同族：其余
    /// 停机入口全都显式失败，本方法补齐 subscribe 面的缺口。
    ///
    /// 旧 [`Store::subscribe`] 保持签名不变（迁移期兼容面）：照常登记 +
    /// [`Self::subscriptions_after_close`] 计数，不静默。Err 路径登记未发生，
    /// 不计入任何计数（返回值即信号，`subscriptions_after_close` 保持
    /// 「已登记的死订阅笔数」口径不掺水）。
    ///
    /// # Examples
    ///
    /// ```
    /// # use skeleton::{EffectBuf, Infallible, NoFx, Reducer, Store as _};
    /// # use skeleton_infra::runtime::{StoreClosed, SyncStore};
    /// # struct Counter;
    /// # impl Reducer for Counter {
    /// #     type State = u32; type Action = u32; type Op = Infallible;
    /// #     fn initial(&self) -> u32 { 0 }
    /// #     fn reduce(&self, s: &u32, a: u32, _out: &mut EffectBuf<u32, Infallible>) -> u32 { s + a }
    /// # }
    /// let store = SyncStore::new(Counter, NoFx);
    /// let ticket = store.try_subscribe(|_| {}).expect("未闭门登记必成功");
    /// store.close();
    /// assert!(matches!(store.try_subscribe(|_| {}), Err(StoreClosed)));
    /// drop(ticket);
    /// ```
    pub fn try_subscribe(
        &self,
        f: impl FnMut(&S) + Send + 'static,
    ) -> Result<Subscription<S>, StoreClosed> {
        if self.is_closed() {
            return Err(StoreClosed);
        }
        Ok(self.subscribe(f))
    }

    /// 单线程订阅：回调免 `Send` 约束——闭包体内可用 `Rc`/`RefCell` 等线程
    /// 本地设施（跨线程档逼着采用方 `Arc<AtomicU64>`/`Mutex` 的痛点在此收口）。
    /// 与 [`Self::subscribe`] 的四点差异：
    ///
    /// 1. **只在注册线程触发**：本地回调登记在注册线程的 thread-local 表，
    ///    store 侧从不持有闭包。他线程 dispatch 不触达本地订阅（其线程表中
    ///    无此 store）；注册线程的 dispatch / `sender().dispatch` 正常触发；
    /// 2. **通知路径免锁免原子量**：回调经 thread-local 直达；store 侧只存一个
    ///    计数闸，零本地订阅时一次 relaxed 读即短路；
    /// 3. **不计入 [`Self::subscriber_count`]**（该计数只看跨线程订阅）；回调
    ///    panic 与跨线程订阅同一纪律：fail-closed 摘除 + [`Self::subscriber_panics`]
    ///    计数。
    /// 4. **跨线程 miss 显形**：他线程 dispatch 不触达本档——每次「闸>0 而
    ///    dispatch 线程表查无本 store 登记」计入 [`Self::local_missed_notifications`]
    ///    （「挂错档」检测；触发语义不变，只留痕）。
    ///
    /// 两 API 的关系：`subscribe` 是跨线程档（回调需 `Send`，槽位 `Mutex`
    /// 串行化）；本 API 是单线程档（回调免 `Send`，线程亲和即安全依据）。
    /// 闭门（[`Self::close`]）后登记照常、永不触发——与 `subscribe` 一致，
    /// 对照见 [`SyncStore`] 类型文档「闭门后的入口对照」。同一 dispatch 内通知序：
    /// 本地订阅先行，跨线程订阅随后（确定性；本地闸先判读，零共享订阅不短路
    /// 本地通知）。
    ///
    /// 票据 [`LocalSubscription`]：Drop = 注销（从注册线程表摘除 + 计数回落）；
    /// 票据本身 `!Send + !Sync`（线程亲和的类型化表达），跨线程移动即编译错。
    /// 丢弃票据 = 立即注销（死订阅），类型层拦截（与 [`Self::subscribe`] 同款）：
    ///
    /// ```compile_fail
    /// # #![deny(unused_must_use)]
    /// # use skeleton::{EffectBuf, Infallible, NoFx, Reducer, Store as _};
    /// # use skeleton_infra::runtime::SyncStore;
    /// # struct Counter;
    /// # impl Reducer for Counter {
    /// #     type State = u32; type Action = u32; type Op = Infallible;
    /// #     fn initial(&self) -> u32 { 0 }
    /// #     fn reduce(&self, s: &u32, a: u32, _out: &mut EffectBuf<u32, Infallible>) -> u32 { s + a }
    /// # }
    /// # let store = SyncStore::new(Counter, NoFx);
    /// store.subscribe_local(|_| {}); // ERROR: unused `LocalSubscription` that must be used——丢弃即死订阅
    /// ```
    pub fn subscribe_local(&self, f: impl FnMut(&S) + 'static) -> LocalSubscription {
        // 闭门登记显形（与 subscribe 同一纪律）
        if self.is_closed() {
            self.inner
                .subscriptions_after_close
                .fetch_add(1, Ordering::SeqCst);
        }
        let sub_id = next_local_sub_id();
        let store_id = self.inner.store_id;
        local_subs::register(store_id, sub_id, Box::new(f));
        self.inner.local_subs.fetch_add(1, Ordering::Release);
        LocalSubscription::new(store_id, sub_id, Arc::clone(&self.inner.local_subs))
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
    ///
    /// 口径：**仅计 `subscribe` 订阅**——`subscribe_local`（TLS 亲和档）不在
    /// 本计数内（两档订阅面不对称，设计见 subscribe_local 文档）。
    pub fn subscriber_count(&self) -> usize {
        lock(&self.inner.subs)
            .iter()
            .filter(|w| w.upgrade().is_some())
            .count()
    }

    /// 闭门后拒绝计数（fail-visible）：dispatch 拒收与 sender dispatch false 均计入。
    /// store 已丢弃后的 sender dispatch false 不可计（计数器随实例消亡），停机窗语义不变。
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

    /// 本地订阅跨线程 miss 计数（fail-visible 读侧）：dispatch 通知段「本地
    /// 订阅闸>0 但 dispatch 线程的 TLS 表查无本 store 登记」即 +1——本地订阅
    /// 挂在 A 线程、dispatch 来自 B 线程时，A 的订阅者收不到 B 的任何 dispatch
    /// 通知（触发语义不变，只从「零触发零信号」变「miss 几次看得见」）。
    /// 非零读数 = 本地档挂在非 dispatch 线程（「挂错档」）：改用
    /// [`Self::subscribe`] 跨线程档，或把 dispatch 收敛到注册线程。口径按
    /// dispatch 时点的闸快照：票据注销与闸回落之间的瞬态窗可能多计一次
    /// （无害偏粗）。
    pub fn local_missed_notifications(&self) -> u64 {
        self.inner.local_missed.load(Ordering::Relaxed)
    }

    /// 审计事件流前订被覆盖计数（fail-visible 读侧）：[`Self::subscribe_events`]
    /// 落槽时槽内已有订阅即 +1——单槽二次订阅会终止前订的事件流（典型：
    /// 框架自带 tracing 桥被自定义消费者顶掉且无任何信号），本计数让覆盖
    /// 显形。旧票据 Drop 照常安全（代次对账不误杀后继流），但其事件流已被
    /// 覆盖终止。
    pub fn audit_preempted(&self) -> u64 {
        self.inner.audit_preempted.load(Ordering::Relaxed)
    }

    /// 异步投递累计次数（ADR 0008 唯一新增读侧，成本可见）：投递口成功收到
    /// 信号的累计次数——逐口逐拍累计（panic 被摘除的投递不计；闭门/票 Drop 后
    /// 不再产生）。乘以单次投递成本即异步档对 dispatch 的总税额，是投递口
    /// 「O(1)」契约义务的核对依据。只读。
    pub fn notify_published(&self) -> u64 {
        self.inner.notify_published.load(Ordering::Acquire)
    }

    /// 解释器 panic 毒锁恢复计数（fail-visible 上浮）。每次 into_inner 恢复自增 = 此前恰
    /// 发生一次解释器 panic（毒位随即 clear_poison 复位，实例不报废）；读数不含
    /// 尚未恢复的最近一次 panic——它在下一次效果转发恢复时才计入。持续上升的
    /// 读数 = 解释器 `run` 存在 panic 路径，需在宿主侧修复。
    pub fn runtime_panic_count(&self) -> u64 {
        self.inner.runtime_panics.load(Ordering::SeqCst)
    }

    /// reducer panic 计数（fail-visible 留痕读侧）：reduce 的 panic 在排空环
    /// catch_unwind 观察点计数后 `resume_unwind` 原样重抛——传播语义不变，
    /// panic 照常在 dispatch 调用点上浮。动作本体已出队、以值传入 reduce，
    /// panic 即丢失（状态不前进、实例可复用、已产效果原地留缓冲先续转）；
    /// 审计时间线现场 = [`AuditEventKind::ReducerInterrupted`]。至此四条
    /// panic/异常路径读侧齐全：解释器 [`Self::runtime_panic_count`]、订阅回调
    /// [`Self::subscriber_panics`]、回灌超限 [`Self::feedback_budget_exceeded`]、
    /// reducer 本计数。持续上升的读数 = 用户 reducer 存在 panic 路径。
    pub fn reducer_panic_count(&self) -> u64 {
        self.inner.reducer_panics.load(Ordering::SeqCst)
    }

    /// 回灌深度预算触发计数（fail-visible 上浮）：单任排空内连续处理的
    /// 回灌链深度超过 `FEEDBACK_DEPTH_BUDGET`（64）即停排并自增。触发点回灌
    /// action 原值退回队尾（零丢失），本任 dispatch 随即返回——非 panic 非静默
    /// no-op。持续上升的读数 = 用户程序存在自指回灌环（reducer 恒 Emit 自身、
    /// 解释器经 `back.send` 同栈回灌），需在宿主侧修复；读数不升的正常流量不受
    /// 预算影响（外部 action 深度恒 0）。store drop 时若队列仍滞留（预算止步
    /// 残留），弃队数一并计入本计数留痕（禁静默弃队）。时间线现场 =
    /// [`AuditEventKind::FeedbackBudgetExceeded`]（`subscribe_events` 订阅窗内
    /// 可查——环结构重建走事件流，快速巡检走本计数）。
    pub fn feedback_budget_exceeded(&self) -> u64 {
        self.inner.feedback_exceeded.load(Ordering::SeqCst)
    }

    /// Cancel 寻址校验读数：[`Effect::Cancel`](skeleton::Effect::Cancel) 引用
    /// 从未发出的 `Run` id 的累计次数（转发环留痕，只计数不拦截——迟到
    /// Cancel 是合法时序，见 `StoreInner::cancel_unknown_id` 文档）。
    ///
    /// 非零读数 = id 纪律漂移的直接证据：Cancel 打错数字、多效果域撞 id 后
    /// 引用了他域 id、或 Cancel 早于 Run 的时序 bug。持续上升的读数需在
    /// reducer 侧修 id 指派（唯一性纪律见 [`EffectId`](skeleton::EffectId)
    /// 文档）。
    pub fn cancel_unknown_id(&self) -> u64 {
        self.inner.cancel_unknown_id.load(Ordering::SeqCst)
    }

    /// Emit 回灌累计重入计数（crate 内读侧：testing 域 TestStore 归因用，
    /// 零公共面增量）。
    #[cfg(feature = "testing")]
    pub(crate) fn feedback_emitted_total(&self) -> u64 {
        self.inner.feedback_emitted.load(Ordering::Relaxed)
    }

    /// 唯一订阅点：开启审计事件流。默认关闭零开销；
    /// 本调用是事件流的唯一开关（再次订阅覆盖前订，单槽；覆盖留痕
    /// [`Self::audit_preempted`]——前订被终止从此可查）。
    ///
    /// - `capacity`：有界环容量（0 视作 1）。溢出丢最旧——最新生命周期优先；
    ///   丢失无新计数，`seq` 空洞即显式证据（见 [`AuditEvent`]）。
    /// - `on_event`：实时回调（每事件锁外调用一次；纯环录模式传 `|_| {}`）。
    ///   回调 panic = fail-closed 摘除（环照录、dispatch 行为不变——观测面禁
    ///   改变被观测者），成因计入 [`Self::subscriber_panics`] /
    ///   [`Self::subscriber_panic_last_cause`]。回调内重入 dispatch 的
    ///   事件只录环不再回调（TLS 在途标记封自指递归）。
    /// - 票据 Drop = 关闭；被覆盖的旧票据 Drop 凭代次对账，禁误杀后继流。
    ///
    /// # Examples
    ///
    /// 一笔 dispatch 的生命周期从事件流重建（取证锚）：
    ///
    /// ```
    /// use skeleton::{Effect, EffectBuf, NoFx, Reducer, Store as _};
    /// use skeleton_infra::audit::AuditEventKind;
    /// use skeleton_infra::runtime::SyncStore;
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
    /// let store = SyncStore::new(PingOnce, NoFx);
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
    /// # use skeleton::{EffectBuf, Infallible, NoFx, Reducer, Store as _};
    /// # use skeleton_infra::runtime::SyncStore;
    /// # struct T;
    /// # impl Reducer for T {
    /// #     type State = u32; type Action = u32; type Op = Infallible;
    /// #     fn initial(&self) -> u32 { 0 }
    /// #     fn reduce(&self, s: &u32, a: u32, _o: &mut EffectBuf<u32>) -> u32 { s + a }
    /// # }
    /// # let store = SyncStore::new(T, NoFx);
    /// store.subscribe_events(16, |_| {}); // ERROR: unused `AuditTicket`——丢弃即静默关闭事件流
    /// ```
    pub fn subscribe_events(
        &self,
        capacity: usize,
        on_event: impl FnMut(&AuditEvent) + Send + 'static,
    ) -> AuditTicket {
        let gen = self.inner.audit_gen.fetch_add(1, Ordering::AcqRel) + 1;
        let cap = capacity.max(1);
        {
            let mut guard = lock(&self.inner.audit);
            // 前订被覆盖留痕（读侧 audit_preempted）：单槽二次订阅会终止前订
            // 的事件流（典型：tracing 桥被自定义消费者顶掉），覆盖必须显形——
            // 语义不变（仍覆盖），只留痕。
            if guard.is_some() {
                self.inner.audit_preempted.fetch_add(1, Ordering::Relaxed);
            }
            *guard = Some(AuditSink {
                ring: VecDeque::with_capacity(cap),
                cap,
                gen,
                notify: Some(Box::new(on_event)),
            });
        }
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
/// 禁静默弃队。读窗 = inner Arc 存续期（SyncStore 已弃而 inner 仍被持有，如测试
/// 白盒），与 [`SyncStore::rejected_after_close`] 的停机窗同类语义。
impl<S, A, Op> Drop for SyncStore<S, A, Op> {
    fn drop(&mut self) {
        let discarded = match self.inner.queue.lock() {
            Ok(queue) => queue.len(),
            // 毒锁 = 此前已有 panic 上浮；取内层只读长度，drop 内不二次爆炸
            Err(poisoned) => poisoned.into_inner().len(),
        };
        if discarded > 0 {
            self.inner
                .feedback_exceeded
                .fetch_add(discarded as u64, Ordering::SeqCst);
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
/// dispatch 的常规路径不经过此函数——其夺权已并入 push 临界区
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
    let mut guard = DrainGuard {
        flag: &inner.draining,
        armed: true,
    };
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
        // reducer panic 与解释器 panic 同形留痕（与全链计数纪律对称）：catch 只
        // 观察（计数+审计现场）即 resume_unwind 原样重抛——传播语义不变，
        // panic 照常在 dispatch 调用点上浮；动作本体已出队随栈展开丢失（已知
        // 契约），缓冲已产效果原地留（续转不受观察影响）。
        let next_val = match catch_unwind(AssertUnwindSafe(|| {
            inner.reducer.reduce(&current, action, buf)
        })) {
            Ok(next) => next,
            Err(payload) => {
                inner.reducer_panics.fetch_add(1, Ordering::SeqCst);
                audit_emit(inner, AuditEventKind::ReducerInterrupted);
                std::panic::resume_unwind(payload);
            }
        };
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
        // reduce→快照替换→效果转发→通知（异步投递先行、同步回调随后）。
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
        // CoW 快照落位（快照分配税消除；析构在锁外——契约细节见 helper 文档）。
        place_snapshot(inner, next_val);
        // 通知句柄：订阅态在落位后补一次 state 锁克隆新快照（回调锁外借用）；
        // 句柄随迭代末消亡，稳态计数回落 1，下一 action 的 make_mut 仍原地命中。
        let notified_next = if cache.slots.is_empty() {
            None
        } else {
            Some(Arc::clone(&lock(&inner.state)))
        };
        // 转发环（禁吞语义见 helper 文档）：Emit 回灌入队、Run/Cancel 交解释器。
        forward_effects(inner, buf, feedback_depth);
        // 异步投递段（ADR 0008）：排在同步回调之前——否则同步档慢订阅者会把
        // 异步消费者的可见延迟一并拖后。投递只发版本号：O(1)、门控短路
        // （零异步订阅者一次 Acquire load 即返）、锁外调用宿主代码。
        notify_async(inner);
        // 通知段（本地先行、跨线程随后；返回当拍是否有订阅者收到快照）。
        let snapshot_notified = notify_snapshot_consumers(inner, cache, &notified_next);
        // 通知节点（R1-2）：只在当拍确有订阅者收到快照时记录——零订阅者的
        // dispatch 无 Notified 事件（时间线如实，不造心跳噪声）。
        if snapshot_notified {
            audit_emit(inner, AuditEventKind::Notified);
        }
    }
}

/// CoW 快照落位（快照分配税消除）：稳态
/// （current 已放手、无外部 snapshot() 持有者）强计数=1 → make_mut 原地
/// 写入零分配；有外部持有者时自动克隆新分配、旧分配冻结——持有者永见旧
/// 快照，快照一致性逐比特保持（CoW 即快照语义的业界标准实现）。
/// **mem::replace 把旧 S 挪出锁外析构是本形态最大风险点**：原地赋值会在
/// state 锁内 drop 旧值，S:Drop panic 即毒锁变砖——锁内必须
/// 只剩 memcpy 与计数原子，析构在本函数返回后的 drop(old_snapshot) 处（锁外）发生。
fn place_snapshot<S: Clone + Send + Sync + 'static, A: Send + 'static, Op: Send + 'static>(
    inner: &Arc<StoreInner<S, A, Op>>,
    next_val: S,
) {
    let old_snapshot = {
        let mut slot = lock(&inner.state);
        std::mem::replace(Arc::make_mut(&mut slot), next_val)
    };
    drop(old_snapshot);
}

/// 转发环：逐条借出转发，
/// 成功才从缓冲摘除——panic 时未转发的效果原地留在缓冲，由下一任排空的
/// reduce 追加、转发时先续转（禁吞：panic 前已产效果零丢失）。不能用
/// `drain(..)`：其 Drop 语义是「完成排空」（未消费元素被 drop，drain_keep_rest
/// 才回填且 unstable）——`drain(..)` 的静默蒸发本体在此，「clear 吞掉」
/// 是次生路径。正在转发的一条随 panic 丢弃：其失败已显式传播+计数，重投
/// 会恒 panic 死循环。remove(0) 稳态（单效果）零搬移，保容量。
/// 回灌句柄为 StoreInner 缓存字段，禁每 action 新建 Arc 闭包。
fn forward_effects<S: Clone + Send + Sync + 'static, A: Send + 'static, Op: Send + 'static>(
    inner: &Arc<StoreInner<S, A, Op>>,
    buf: &mut EffectBuf<A, Op>,
    feedback_depth: u64,
) {
    while let Some(effect) = buf.pop_front() {
        match effect {
            Effect::Emit(a) => {
                inner.feedback_emitted.fetch_add(1, Ordering::Relaxed);
                lock(&inner.queue).push_back((a, ActionOrigin::Feedback(feedback_depth + 1)));
                // 回灌入队与外部入队同一事件节点（深度链 = 环结构重建键）
                audit_emit(
                    inner,
                    AuditEventKind::Enqueued {
                        feedback_depth: feedback_depth + 1,
                    },
                );
            }
            effect => {
                // Cancel 寻址校验（fail-visible 留痕，不改转发语义）：
                // Run 发出即登记 id；Cancel 引用未见过的 id 只计数不拦截——
                // 拦截（panic/丢弃）会把「Cancel 语义由解释器自证」的契约
                // （EffectRuntime 责任边界注）改写成核心语义：迟到 Cancel
                // （Run 已自然完成）是合法时序，核心无生命周期汇报面，无法
                // 区分「迟到」与「漂移」。读数 = SyncStore::cancel_unknown_id。
                match &effect {
                    Effect::Run { id, .. } => {
                        lock(&inner.run_ids).insert(id.get());
                    }
                    // guard 形态（clippy collapsible_match）：Cancel 命中已见
                    // id 时落 `_`，与未知新变体同路——均无需留痕
                    Effect::Cancel { id } if !lock(&inner.run_ids).contains(&id.get()) => {
                        inner.cancel_unknown_id.fetch_add(1, Ordering::Relaxed);
                    }
                    // Effect 词表 non_exhaustive（跨 crate match 须通配）：
                    // 未知新变体与寻址校验无关，原样落下方解释器转发
                    _ => {}
                }
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
}

/// 通知段：本地订阅先行、跨线程订阅随后（确定性），返回当拍是否有订阅者
/// 收到快照（审计 `Notified` 节点的判据）。
fn notify_snapshot_consumers<
    S: Clone + Send + Sync + 'static,
    A: Send + 'static,
    Op: Send + 'static,
>(
    inner: &Arc<StoreInner<S, A, Op>>,
    cache: &NotifyCache<S>,
    notified_next: &Option<Arc<S>>,
) -> bool {
    // 本地订阅（单线程档）：先于跨线程广播判读——零本地订阅一次 relaxed
    // 读即短路；有本地订阅时零跨线程订阅也必触发（快照按需取，本地闭包
    // panic 摘除计入 subscriber_panics，fail-closed）。同一 dispatch 通知
    // 序：本地先行、跨线程随后（确定性）。
    let local_notified = inner.local_subs.load(Ordering::Relaxed) > 0;
    if local_notified {
        let snapshot = Arc::clone(&lock(&inner.state));
        let (matched, poisoned, cause) = local_notify(inner.store_id, &*snapshot);
        // 跨线程 miss 留痕（读侧 local_missed_notifications）：闸>0 说明有线程
        // 持有本地订阅，本线程表却查无该 store——本拍 dispatch 到不了任何
        // 本地订阅（触发语义不变，只从「零触发零信号」变「miss 看得见」）。
        if matched == 0 {
            inner.local_missed.fetch_add(1, Ordering::Relaxed);
        }
        if poisoned > 0 {
            inner
                .subscriber_panics
                .fetch_add(poisoned, Ordering::Release);
            *lock(&inner.subscriber_panic_cause) = cause;
        }
    }
    if let Some(next) = notified_next {
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
    local_notified || notified_next.is_some()
}

// ── 本地订阅：线程本地登记机制在 runtime/local_subs.rs（关注面切缝）────────
// 排空环的通知段经 local_notify 触发本线程登记的回调；store 侧只持闸计数。

/// store 身份号：本地订阅 TLS 表的键。全局单调、进程内唯一。
static NEXT_STORE_ID: AtomicU64 = AtomicU64::new(1);

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

// ── 审计事件流发射面：runtime/audit_emit.rs（关注面切缝）────────────────────
// 发射点（值面 crate::audit）与 gate 快路径随排空环走，本文件只持调用点。

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
        let store = SyncStore::new(TickFeature, NopRuntime);
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
        let store = Arc::new(SyncStore::new(TickFeature, NopRuntime));
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
        assert!(
            !orphan.load(Ordering::SeqCst),
            "孤儿 action：无在途 dispatch 且排空旗已放，队列仍滞留 action（监视线程持锁核实）"
        );
        assert_eq!(inner.in_flight.load(Ordering::SeqCst), 0);
        assert!(
            !inner.draining.load(Ordering::SeqCst),
            "静止态排空旗必须已放"
        );
        assert!(
            lock(&inner.queue).is_empty(),
            "静止态队列必须空：滞留即孤儿"
        );
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
        let store = SyncStore::new(TickFeature, NopRuntime);
        store.close();
        assert!(store.is_closed());
        store.dispatch(Tick::Tick);
        store.dispatch(Tick::Tick);
        assert_eq!(*store.snapshot(), 0);
    }

    /// 闸在共享 push 路径——闭门后 dispatch 返回 false 且计数，
    /// bool 契约不撒谎（不报「已入队」实为弃）。
    #[test]
    fn closed_sender_returns_false_and_counts() {
        let store = SyncStore::new(TickFeature, NopRuntime);
        let sender = store.sender();
        assert!(sender.dispatch(Tick::Tick), "闭门前 dispatch 照常入队");
        store.close();
        assert!(
            !sender.dispatch(Tick::Tick),
            "闭门后 dispatch 必须显式 false"
        );
        assert_eq!(store.rejected_after_close(), 1);
    }

    /// 闭门计数 fail-visible，dispatch 与 send 两条 push 路径的拒绝都计入。
    #[test]
    fn rejection_counter_covers_both_push_paths() {
        let store = SyncStore::new(TickFeature, NopRuntime);
        let sender = store.sender();
        store.close();
        store.dispatch(Tick::Tick);
        store.dispatch(Tick::Tick);
        let _ = sender.dispatch(Tick::Tick);
        let _ = sender.dispatch(Tick::Tick);
        let _ = sender.dispatch(Tick::Tick);
        assert_eq!(store.rejected_after_close(), 5);
        assert_eq!(*store.snapshot(), 0, "被拒 action 不得推进状态");
    }

    /// close() 内同步排空至 idle——关门时点的存量队列被执行而非
    /// 随 drop 静默弃队（与 fx 停机序「fx.close→await idle→flush→drop」对齐）。
    /// 白盒注入：单线程下队列仅在排空中非空，直推 queue 复现「存量未排」瞬态。
    #[test]
    fn close_drains_stock_queue_to_idle() {
        let store = SyncStore::new(TickFeature, NopRuntime);
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
        let store = SyncStore::new(TickFeature, NopRuntime);
        store.close();
        store.close();
        assert!(store.is_closed());
        store.dispatch(Tick::Tick);
        assert_eq!(*store.snapshot(), 0);
        assert_eq!(store.rejected_after_close(), 1);
    }

    // ── make_mut CoW 快照落位 ────────

    /// CoW 分支语义（快照一致性自证）：外部持有者存续期间旧快照冻结——持有者
    /// 永见其取得时点的自洽状态，后续 action 的落位不得波及（快照即模型）。
    #[test]
    fn make_mut_cow_freezes_held_snapshot() {
        let store = SyncStore::new(TickFeature, NopRuntime);
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
        let store = SyncStore::new(TickFeature, NopRuntime);
        store.dispatch(Tick::Tick);
        let p1 = Arc::as_ptr(&*lock(&store.inner.state));
        store.dispatch(Tick::Tick);
        let p2 = Arc::as_ptr(&*lock(&store.inner.state));
        assert_eq!(
            p1, p2,
            "稳态无外部持有者：make_mut 必须原地写入，禁每 action 换分配"
        );
        assert_eq!(
            Arc::strong_count(&*lock(&store.inner.state)),
            1,
            "稳态强计数必须回落 1"
        );
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
            PanicOnDrop {
                armed: Arc::clone(&self.armed),
            }
        }
        fn reduce(
            &self,
            s: &PanicOnDrop,
            _: Tick,
            _out: &mut EffectBuf<Tick, Infallible>,
        ) -> PanicOnDrop {
            PanicOnDrop {
                armed: Arc::clone(&s.armed),
            }
        }
    }

    #[test]
    fn snapshot_replacement_drops_old_value_outside_state_lock() {
        let store = SyncStore::new(
            PanicDropFeature {
                armed: Arc::new(AtomicBool::new(false)),
            },
            NopRuntime,
        );
        let first = std::panic::catch_unwind(AssertUnwindSafe(|| store.dispatch(Tick::Tick)));
        assert!(
            first.is_err(),
            "旧快照析构 panic 必须如实传播（fail-visible）"
        );
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
        fn reduce(
            &self,
            s: &u32,
            _: DropAction,
            out: &mut EffectBuf<DropAction, Infallible>,
        ) -> u32 {
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
        let store = Arc::new(SyncStore::new(DropSpinFeature, DropNopFx));
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
        let store = Arc::new(SyncStore::new(TickFeature, NopRuntime));
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
        let store = SyncStore::new(TickFeature, NopRuntime);
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
