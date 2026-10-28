//! 异步（信号式）通知档（ADR 0008）：投递口注册表 + 状态版本水位 + 门控投递。
//!
//! 从 runtime 主文件切出的关注面缝（与 local_subs/audit_emit 同体例）：排空环
//! 通知段经 `notify_async` 在同步回调**之前**投递（ADR §4 决策 4——否则同步档
//! 慢订阅者会把异步消费者的可见延迟一并拖后）。零异步订阅者一次 Acquire load
//! 即返（77ns 无订阅锚的守卫）；有订阅者时「取出 → 锁外调用 → 归还」（照抄
//! audit_emit 范式：publish 是宿主代码，持锁调用即死锁）。载荷 = 单调版本号
//! （拉取式，M1 无事件流 / M5 水位可检出丢拍）；内存 O(异步订阅数)，无队列
//! 无缓冲。契约面在 `skeleton::notify`。

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};

use skeleton::notify::NotifySink;
use skeleton::Subscription;

use super::lock;
use super::panic_cause_summary;
use super::StoreInner;

/// 投递口槽位：注册表表项。表存 Weak、强引用随票据存活（与跨线程订阅表同构）；
/// `publish` 走 `&self`，槽位免锁。`alive` 是闸回落的一次性裁决：panic 摘除与
/// 票据 Drop 两条摘除路径竞争时，闸计数恰好回落一次（谁先 swap 谁回落）——
/// 没有它，摘除后再 Drop 票据会把闸打到下溢。
pub(super) struct NotifySinkSlot {
    sink: Box<dyn NotifySink>,
    alive: AtomicBool,
}

/// 摘除（panic fail-closed 与票据 Drop 两路径共用）：闸回落恰好一次 + 表 retain。
/// 摘除即时生效：此后不再收到任何投递（同 [`super::Subscription`] 注销语义）。
fn remove_sink(
    slot: &Arc<NotifySinkSlot>,
    table: &Mutex<Vec<Weak<NotifySinkSlot>>>,
    gate: &AtomicUsize,
) {
    if slot.alive.swap(false, Ordering::AcqRel) {
        gate.fetch_sub(1, Ordering::Release);
    }
    let dead = Arc::downgrade(slot);
    lock(table).retain(|w| !Weak::ptr_eq(w, &dead));
}

/// 登记入口（`Notify for SyncStore` 专用）：槽位入表先于闸自增——投递侧闸
/// 判读通过后必经表锁收集，登记序与投递可见序一致。返回票据（Drop = 摘除）。
pub(super) fn register<S>(
    table: &Arc<Mutex<Vec<Weak<NotifySinkSlot>>>>,
    gate: &Arc<AtomicUsize>,
    sink: Box<dyn NotifySink>,
) -> Subscription<S> {
    let slot = Arc::new(NotifySinkSlot {
        sink,
        alive: AtomicBool::new(true),
    });
    lock(table).push(Arc::downgrade(&slot));
    gate.fetch_add(1, Ordering::Release);
    let table = Arc::clone(table);
    let gate = Arc::clone(gate);
    // 存活令牌随闭包持有：表项存 Weak，强引用在票据 Drop（闭包释放）前保 slot
    // 存活——与跨线程订阅票据同一形态。
    let keep_alive = Arc::clone(&slot);
    Subscription::new(move || {
        remove_sink(&slot, &table, &gate);
        drop(keep_alive);
    })
}

/// 排空环异步投递段：有在册投递口时版本推进一位并逐口投递；零订阅一次
/// Acquire load 即返（门控，ADR §5.2：无人订阅期间版本冻结、增减订不回退）。
pub(super) fn notify_async<S, A, Op>(inner: &StoreInner<S, A, Op>) {
    if inner.notify_subs.load(Ordering::Acquire) == 0 {
        return;
    }
    let slots: Vec<Arc<NotifySinkSlot>> = {
        let sinks = lock(&inner.notify_sinks);
        sinks.iter().filter_map(|w| w.upgrade()).collect()
    };
    if slots.is_empty() {
        // 闸 stale 的并发摘除窗：表已空——门控语义（无人订阅不推进版本）成立
        return;
    }
    // 版本推进与落位一一对应（版本号 = 状态代数）；Relaxed 足够：版本值经
    // 调用参数出站，消费侧拉取现态自走 state 锁序。
    let version = inner.state_version.fetch_add(1, Ordering::Relaxed) + 1;
    for slot in &slots {
        // 锁外调用（取出→调用→归还）：投递口 panic 归一——fail-closed 摘除后
        // 继续（中毒口每次 dispatch 必再 panic，留表即「每拍必 panic」双重变砖）。
        // 成因留痕与订阅回调 panic 同一纪律（subscriber_panics / last_cause）；
        // panic 的投递不计入 notify_published（成本只计成功投递）。
        let published = catch_unwind(AssertUnwindSafe(|| slot.sink.publish(version)));
        match published {
            Ok(()) => {
                inner.notify_published.fetch_add(1, Ordering::Release);
            }
            Err(payload) => {
                remove_sink(slot, &inner.notify_sinks, &inner.notify_subs);
                inner.subscriber_panics.fetch_add(1, Ordering::Release);
                *lock(&inner.subscriber_panic_cause) = Some(panic_cause_summary(&*payload));
            }
        }
    }
}
