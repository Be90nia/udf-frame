//! 本地订阅（[`super::SyncStore::subscribe_local`]）的线程本地登记机制。
//!
//! 从 runtime 主文件切出的关注面缝：回调只登记在注册线程的 thread-local 表
//! ——store 侧从不持有本地闭包，跨线程 dispatch 在自己的表里查不到该 store，
//! 本地回调「只在注册线程触发」与「免 Send」的安全性全部由线程亲和保证，
//! 零 unsafe（RefCell 作用域即单线程）。store 侧闸计数与排空环通知点仍在
//! [`super`]（drain 的通知段经 [`local_notify`] 触发本域）。

use std::cell::RefCell;
use std::marker::PhantomData;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use super::panic_cause_summary;

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

/// 分配本地订阅序号（注册流程第一步；登记与票据构造归 [`super`] 装配）。
pub(super) fn next_local_sub_id() -> u64 {
    NEXT_LOCAL_SUB_ID.fetch_add(1, Ordering::Relaxed)
}

/// 登记入口（泛型收口，[`super::SyncStore::subscribe_local`] 专用）：宿主闭包
/// → 状态擦除槽——`LocalSlot`/`LocalNotify` 对本域私有，TLS 容器无泛型的
/// 擦除细节不出本模块。
pub(super) fn register<S: 'static>(store_id: u64, sub_id: u64, notify: Box<dyn FnMut(&S)>) {
    local_sub_register(store_id, sub_id, Box::new(LocalSlot { notify }));
}

/// 登记（注册线程执行）：TLS 插入先于闸自增——注册线程上程序序保证
/// 「表里有 → 闸 >0」，本线程的 dispatch 必能查到自己的回调。
fn local_sub_register(store_id: u64, sub_id: u64, notify: Box<dyn LocalNotify>) {
    LOCAL_SUBS.with(|table| {
        table.borrow_mut().push(LocalSubEntry {
            store_id,
            sub_id,
            notify,
        });
    });
}

/// 注销（注册线程执行——票据 `!Send`，Drop 只会发生在注册线程）。
fn local_sub_unregister(store_id: u64, sub_id: u64) {
    LOCAL_SUBS.with(|table| {
        table
            .borrow_mut()
            .retain(|e| !(e.store_id == store_id && e.sub_id == sub_id));
    });
}

/// 触发本线程登记的某 store 全部本地订阅；返回 (命中数, panic 摘除数,
/// 最近一次成因摘要)。命中数 = 本线程表内该 store 的登记笔数（含本轮被摘除
/// 的毒回调）——闸>0 而命中=0 即本线程 dispatch 未触达任何本地订阅（跨线程
/// miss，调用方计入 `local_missed` 留痕）。摘除数计入 `subscriber_panics` +
/// 留痕 `subscriber_panic_last_cause`，摘除即从此不再触发——fail-closed。
/// 回调内重入 dispatch 只入队即返（排空权互斥），不会重入本函数，RefCell
/// 不可重入借用天然满足。通知序 = 登记序（确定性）。
pub(super) fn local_notify(store_id: u64, state: &dyn std::any::Any) -> (u64, u64, Option<String>) {
    LOCAL_SUBS.with(|table| {
        let mut table = table.borrow_mut();
        let mut matched = 0u64;
        let mut dead = Vec::new();
        let mut last_cause = None;
        for entry in table.iter_mut() {
            if entry.store_id != store_id {
                continue;
            }
            matched += 1;
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| entry.notify.notify(state))) {
                dead.push(entry.sub_id);
                last_cause = Some(panic_cause_summary(&*payload));
            }
        }
        let removed = dead.len() as u64;
        if !dead.is_empty() {
            table.retain(|e| e.store_id != store_id || !dead.contains(&e.sub_id));
        }
        (matched, removed, last_cause)
    })
}

/// 本地订阅票据（[`super::SyncStore::subscribe_local`]）：Drop = 注销（TLS 摘除 +
/// 闸计数回落）。`PhantomData<*const ()>` 即 `!Send + !Sync` 的类型化表达——
/// 票据不可跨线程移动，注销只会发生在注册线程，与本地登记的线程亲和语义互锁。
#[must_use = "本地订阅票据被丢弃即立即注销（Drop=注销）——`store.subscribe_local(..);` 裸调用 = 静默死订阅；绑定持有才持续收通知"]
pub struct LocalSubscription {
    store_id: u64,
    sub_id: u64,
    count: Arc<AtomicUsize>,
    _not_send: PhantomData<*const ()>,
}

impl LocalSubscription {
    /// 构造收口（[`super::SyncStore::subscribe_local`] 专用）：字段对本域私有，
    /// 经构造器交接——闸计数的 Arc 由 store 侧持有，票据 Drop 回落同闸。
    pub(super) fn new(store_id: u64, sub_id: u64, count: Arc<AtomicUsize>) -> Self {
        Self {
            store_id,
            sub_id,
            count,
            _not_send: PhantomData,
        }
    }
}

impl Drop for LocalSubscription {
    fn drop(&mut self) {
        local_sub_unregister(self.store_id, self.sub_id);
        self.count.fetch_sub(1, Ordering::Release);
    }
}
