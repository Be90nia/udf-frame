//! 本地订阅（[`super::SyncStore::subscribe_local`]）的线程本地登记机制。
//!
//! 从 runtime 主文件切出的关注面缝：回调只登记在注册线程的 thread-local 表
//! ——store 侧从不持有本地闭包，跨线程 dispatch 在自己的表里查不到该 store，
//! 本地回调「只在注册线程触发」与「免 Send」的安全性全部由线程亲和保证，
//! 零 unsafe（RefCell 作用域即单线程）。store 侧闸计数与排空环通知点仍在
//! [`super`]（drain 的通知段经 [`local_notify`] 触发本域）。

use std::cell::RefCell;
use std::marker::PhantomData;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use super::panic_cause_summary;

/// 本地订阅序号：注销定位键（与 store_id 联合唯一定位一条登记）。
static NEXT_LOCAL_SUB_ID: AtomicU64 = AtomicU64::new(1);

thread_local! {
    static LOCAL_SUBS: RefCell<Vec<LocalSubEntry>> = const { RefCell::new(Vec::new()) };
}

struct LocalSubEntry {
    store_id: u64,
    sub_id: u64,
    /// 回调句柄走 `Rc`：通知阶段快照句柄、释放表借用后再调用户代码
    /// （回调重入不再撞表借用）；回调中途注销自身/他者时，快照的强引用
    /// 保住槽体活到调用返回。
    notify: LocalNotifyHandle,
}

/// 通知回调句柄：`Rc` 供通知阶段快照（强引用保槽体活到调用返回），
/// `RefCell` 供 `&mut` 下发（TLS 单线程内安全）。
type LocalNotifyHandle = Rc<RefCell<Box<dyn LocalNotify>>>;

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
            notify: Rc::new(RefCell::new(notify)),
        });
    });
}

/// 注销（注册线程执行——票据 `!Send`，Drop 只会发生在注册线程）。
///
/// 整条登记移出表、在 `with` 之外析构（issue#90）：entry 持有最后一个
/// 回调句柄时，`Rc` 归零会同步跑用户闭包与捕获对象的 `Drop`，而它们可能
/// 重入本地订阅（注册/注销）——若此刻仍持 `RefCell` 的表借用，重入即炸
/// `BorrowMutError`。
fn local_sub_unregister(store_id: u64, sub_id: u64) {
    let retired = LOCAL_SUBS.with(|table| {
        let mut table = table.borrow_mut();
        let index = table
            .iter()
            .position(|e| e.store_id == store_id && e.sub_id == sub_id);
        index.map(|index| table.remove(index))
    });
    drop(retired);
}

/// 触发本线程登记的某 store 全部本地订阅；返回 (命中数, panic 摘除数,
/// 最近一次成因摘要)。命中数 = 本轮开始时表内该 store 的登记笔数（含本轮
/// 被摘除的毒回调与回调期间被注销而跳过的登记）——闸>0 而命中=0 即本线程
/// dispatch 未触达任何本地订阅（跨线程 miss，调用方计入 `local_missed`
/// 留痕）。摘除数计入 `subscriber_panics` + 留痕
/// `subscriber_panic_last_cause`，摘除即从此不再触发——fail-closed。
///
/// 两阶段防重入（issue#42）：阶段一在表借用下只做快照（克隆回调句柄的
/// `Rc`，登记序即通知序），借用随即释放；阶段二**不持表借用**逐个调回调
/// ——回调内再入（跨 store dispatch 的嵌套通知、callback-time
/// subscribe/unregister）都能正常拿表借用，不再炸 `BorrowMutError` 被误判
/// 成用户 panic。同 store 重入 dispatch 只入队即返（排空权互斥），同一
/// 句柄的内部 `RefCell` 不可能双重借用。回调期间新注册的本轮不可见
/// （快照确定性，下一轮可见）；本轮开始时在册、被调到之前已注销的登记
/// 跳过（Drop=退订即时生效，不复活死订阅）。真用户 panic 照旧
/// fail-closed 摘除。
pub(super) fn local_notify(store_id: u64, state: &dyn std::any::Any) -> (u64, u64, Option<String>) {
    LOCAL_SUBS.with(|table| {
        let snapshot: Vec<(u64, LocalNotifyHandle)> = {
            let table = table.borrow();
            table
                .iter()
                .filter(|e| e.store_id == store_id)
                .map(|e| (e.sub_id, Rc::clone(&e.notify)))
                .collect()
        };
        let matched = snapshot.len() as u64;
        let mut dead = Vec::new();
        let mut last_cause = None;
        for (sub_id, handle) in snapshot {
            // 本轮内已被注销（回调中途 drop 了排位靠后的票据）→ 跳过。
            let alive = table
                .borrow()
                .iter()
                .any(|e| e.store_id == store_id && e.sub_id == sub_id);
            if !alive {
                continue;
            }
            // 句柄内部借用拿在 catch_unwind 外：RefCell 若真被双重借用
            // （不变式外路径），panic 原样传播，不误记到用户回调头上。
            let mut slot = handle.borrow_mut();
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| slot.notify(state))) {
                dead.push(sub_id);
                last_cause = Some(panic_cause_summary(&*payload));
            }
        }
        let removed = dead.len() as u64;
        // 毒回调整条移出表、在借用之外析构（issue#90）：句柄归零会跑用户
        // 闭包的 `Drop`，其重入本地订阅不得撞上表借用。
        let mut retired: Vec<LocalSubEntry> = Vec::new();
        if !dead.is_empty() {
            let mut table = table.borrow_mut();
            let mut index = 0;
            while index < table.len() {
                if table[index].store_id == store_id && dead.contains(&table[index].sub_id) {
                    retired.push(table.remove(index));
                } else {
                    index += 1;
                }
            }
        }
        drop(retired);
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    /// 析构时重入本地订阅注册/注销的捕获对象：issue#90 的可达路径就是用户
    /// 闭包 `Drop` 在注销/摘除期间运行——那时若仍持 `LOCAL_SUBS` 的表借用，
    /// 重入即抛 `BorrowMutError`。探针不吞 panic，让宿主析构路径原样传播。
    struct DropProbe {
        store_id: u64,
    }

    impl Drop for DropProbe {
        fn drop(&mut self) {
            let sub_id = next_local_sub_id();
            register::<u8>(self.store_id, sub_id, Box::new(|_state: &u8| {}));
            local_sub_unregister(self.store_id, sub_id);
        }
    }

    /// 登记一条捕获 [`DropProbe`] 的本地订阅：探针随回调句柄一起析构。
    fn register_with_probe(store_id: u64, sub_id: u64) {
        let probe = DropProbe {
            store_id: store_id + 1,
        };
        register::<u8>(
            store_id,
            sub_id,
            Box::new(move |_state: &u8| {
                let _ = &probe;
            }),
        );
    }

    /// 普通退订（无通知快照持额外句柄）：整条登记移出表后才析构，探针能在自身
    /// `Drop` 里成功注册并注销另一条本地订阅；注销即时可见、闸计数照旧回落。
    #[test]
    fn unregister_drops_callback_outside_table_borrow() {
        const STORE: u64 = u64::MAX - 90;
        let sub_id = next_local_sub_id();
        register_with_probe(STORE, sub_id);
        let count = Arc::new(AtomicUsize::new(1));
        let ticket = LocalSubscription::new(STORE, sub_id, Arc::clone(&count));
        let dropped = catch_unwind(AssertUnwindSafe(move || drop(ticket)));
        assert!(
            dropped.is_ok(),
            "注销时用户闭包析构重入本地订阅不得炸 RefCell 借用"
        );
        assert_eq!(count.load(Ordering::Acquire), 0, "票据 Drop 的闸计数回落");
        assert_eq!(local_notify(STORE, &0u8).0, 0, "注销后不再命中");
    }

    /// 通知中途退订后续订阅：快照强引用只保槽体活到调用收尾，最终析构不在表
    /// 借用期；被注销的登记本轮跳过、下一轮不复活。
    #[test]
    fn notify_mid_flight_unregister_skips_later_sub() {
        const STORE: u64 = u64::MAX - 91;
        let first = next_local_sub_id();
        let second = next_local_sub_id();
        // 登记序即通知序：`first` 排在 `second` 之前，才能在后者被触达前注销它。
        let first_hits = Rc::new(Cell::new(0));
        register::<u8>(
            STORE,
            first,
            Box::new({
                let first_hits = Rc::clone(&first_hits);
                move |_state: &u8| {
                    first_hits.set(first_hits.get() + 1);
                    local_sub_unregister(STORE, second);
                }
            }),
        );
        let second_hits = Rc::new(Cell::new(0));
        register::<u8>(
            STORE,
            second,
            Box::new({
                let second_hits = Rc::clone(&second_hits);
                let probe = DropProbe {
                    store_id: STORE + 1,
                };
                move |_state: &u8| {
                    let _ = &probe;
                    second_hits.set(second_hits.get() + 1);
                }
            }),
        );
        let round = catch_unwind(AssertUnwindSafe(|| local_notify(STORE, &0u8)));
        let (matched, removed, cause) = round.expect("中途退订的闭包析构重入不得炸 RefCell 借用");
        assert_eq!(
            (matched, removed),
            (2, 0),
            "本轮开始时两笔在册、无用户 panic"
        );
        assert!(cause.is_none(), "无用户 panic 即无成因摘要");
        assert_eq!(
            (first_hits.get(), second_hits.get()),
            (1, 0),
            "被注销的后续订阅本轮不再命中"
        );
        assert_eq!(local_notify(STORE, &0u8).0, 1, "下一轮只剩仍持有的登记");
        assert_eq!(second_hits.get(), 0, "死订阅不复活");
    }

    /// 回调 panic 摘除路径：被摘除的登记同样整条移出、在表借用之外析构。
    #[test]
    fn notify_panic_removal_drops_callback_outside_table_borrow() {
        const STORE: u64 = u64::MAX - 92;
        let sub_id = next_local_sub_id();
        let probe = DropProbe {
            store_id: STORE + 1,
        };
        register::<u8>(
            STORE,
            sub_id,
            Box::new(move |_state: &u8| {
                let _ = &probe;
                panic!("issue#90 探针：用户回调 panic 走 fail-closed 摘除");
            }),
        );
        let round = catch_unwind(AssertUnwindSafe(|| local_notify(STORE, &0u8)));
        let (matched, removed, cause) = round.expect("摘除路径的闭包析构重入不得炸 RefCell 借用");
        assert_eq!((matched, removed), (1, 1), "毒回调按 fail-closed 摘除");
        assert!(cause.is_some(), "摘除留痕成因摘要");
        assert_eq!(local_notify(STORE, &0u8).0, 0, "摘除后不再触发");
    }
}
