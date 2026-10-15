//! 内存治理：有界容器族 + counting allocator + 泄漏断言（结构性保证不靠自觉）。
//!
//! feature `memgov`（零外部依赖纯 std）；由 lib.rs 以
//! `#[cfg(feature = "memgov")] pub mod memgov;` 接线。

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};

/// 淘汰策略（②档必选参数）：Reject=满则显式错（fail-closed，默认推荐）；
/// EvictOldest=按 BTreeMap 首键淘汰。
///
/// K 契约：K 须单调指派（序号/时间戳键），此时语义=逐最旧；
/// K 乱序时语义如实退化为 EvictMinKey——调用方纪律，本件不静默补偿。
/// LRU/TTL→moka 对表，不在此实现。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Eviction {
    /// 满=Err(BoundError)（fail-closed）。
    Reject,
    /// 满=先 pop_first（首键）再插。
    EvictOldest,
}

/// [`BoundedRegistry::insert`] 在满且策略为 [`Eviction::Reject`] 时的显式错误。
///
/// 零依赖纪律（同 text.rs/journal.rs 先例）：thiserror 不入 manifest，
/// 手写同形 `Display`/`Error` impl（文案与 thiserror 派生形态逐字一致），
/// 引入 thiserror 依赖时可换回派生。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BoundError {
    pub capacity: usize,
}

impl std::fmt::Display for BoundError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "bounded registry full (capacity={}, eviction=Reject)",
            self.capacity
        )
    }
}

impl std::error::Error for BoundError {}

/// 有界注册表：上限+策略必填（无默认无界构造——②档门面强制）。
/// 迭代恒升序确定序；缓存形态别名 BoundedCache=本类型（文档级别名，不另造类型）。
pub struct BoundedRegistry<K: Ord, V> {
    map: BTreeMap<K, V>,
    max: usize,
    evict: Eviction,
}

impl<K: Ord, V> BoundedRegistry<K, V> {
    /// 构造有界注册表。`max == 0` 构造期 panic（assert fail-fast：恒满注册表必是 bug）。
    pub fn bounded(max: usize, evict: Eviction) -> Self {
        assert!(
            max > 0,
            "BoundedRegistry::bounded: max == 0 is a bug (always-full registry)"
        );
        Self {
            map: BTreeMap::new(),
            max,
            evict,
        }
    }

    /// 插入。已存在 key=覆盖（len 不变，不触发淘汰）。
    /// 满：Reject→Err(BoundError)（原内容不动）；EvictOldest→先 pop_first 再插。
    /// Err ⇒ 调用方显式处理（fail-closed 回灌）。
    pub fn insert(&mut self, k: K, v: V) -> Result<(), BoundError> {
        if let Some(slot) = self.map.get_mut(&k) {
            *slot = v;
            return Ok(());
        }
        if self.map.len() >= self.max {
            match self.evict {
                Eviction::Reject => return Err(BoundError { capacity: self.max }),
                Eviction::EvictOldest => {
                    self.map.pop_first();
                }
            }
        }
        self.map.insert(k, v);
        Ok(())
    }

    pub fn get(&self, k: &K) -> Option<&V> {
        self.map.get(k)
    }

    pub fn remove(&mut self, k: &K) -> Option<V> {
        self.map.remove(k)
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn capacity(&self) -> usize {
        self.max
    }

    /// 确定序升序迭代（泄漏断言面）。
    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> + '_ {
        self.map.iter()
    }
}

static LIVE_BYTES: AtomicUsize = AtomicUsize::new(0);
static LIVE_ALLOCS: AtomicUsize = AtomicUsize::new(0);
static PEAK_BYTES: AtomicUsize = AtomicUsize::new(0);

/// counting allocator：System 包装+三计数器（live_bytes/live_allocs/peak_bytes，Relaxed 序）。
/// **仅测试/基准侧声明**：`#[global_allocator] static A: CountingAllocator = CountingAllocator;`
/// 发布构建不声明=零开销（内存红线铁律三的配置面）。
pub struct CountingAllocator;

impl CountingAllocator {
    /// 当前存活字节数（O(1)；G 类超阈告警钩子的数据源）。
    pub fn live_bytes() -> usize {
        LIVE_BYTES.load(Ordering::Relaxed)
    }

    /// 当前存活分配次数（O(1)）。
    pub fn live_allocs() -> usize {
        LIVE_ALLOCS.load(Ordering::Relaxed)
    }

    /// 历史峰值存活字节数（fetch_max 维护）。
    /// 计数器全局共享：**仅串行上下文有效**（并行测试的他测分配计入读数，无归因）。
    pub fn peak_bytes() -> usize {
        PEAK_BYTES.load(Ordering::Relaxed)
    }

    /// 清零峰值（同样仅串行上下文有意义）。
    pub fn reset_peak() {
        PEAK_BYTES.store(0, Ordering::Relaxed);
    }
}

/// 成功分配的记账三连：bytes 增、峰值刷新、allocs 增（alloc/alloc_zeroed 共用，防漂移）。
fn record_alloc(layout: Layout) {
    let live = LIVE_BYTES.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
    PEAK_BYTES.fetch_max(live, Ordering::Relaxed);
    LIVE_ALLOCS.fetch_add(1, Ordering::Relaxed);
}

// SAFETY: 计数器增减仅在 System 分配成功/释放后发生，不改变分配器本身的内存安全性；
// 计数为尽力而为观测面（Relaxed），不参与内存安全论证。
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: System 分配器对任意合法 Layout 安全；null 按契约原样返回，不产出悬垂指针。
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            record_alloc(layout);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: ptr 须来自本分配器成功分配且未释放——GlobalAlloc 契约由调用方保证。
        unsafe { System.dealloc(ptr, layout) };
        LIVE_BYTES.fetch_sub(layout.size(), Ordering::Relaxed);
        LIVE_ALLOCS.fetch_sub(1, Ordering::Relaxed);
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: 同 alloc；null 不计数。
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            record_alloc(layout);
        }
        ptr
    }

    unsafe fn realloc(&self, ptr: *mut u8, old_layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: ptr/old_layout 配对由 GlobalAlloc 契约保证；null 原样返回不计数。
        let new_ptr = unsafe { System.realloc(ptr, old_layout, new_size) };
        if !new_ptr.is_null() {
            if new_size >= old_layout.size() {
                let grown = LIVE_BYTES.fetch_add(new_size - old_layout.size(), Ordering::Relaxed)
                    + (new_size - old_layout.size());
                PEAK_BYTES.fetch_max(grown, Ordering::Relaxed);
            } else {
                LIVE_BYTES.fetch_sub(old_layout.size() - new_size, Ordering::Relaxed);
            }
        }
        new_ptr
    }
}

/// 泄漏断言 guard（三类判据之一：allocs/bytes 回基线）。
/// Drop 兜底尽力而为——panic-in-drop 会被吞，**规范用法=测试尾显式 [`LeakGuard::check`]**
/// /`expect_empty`/fx 侧 `expect_idle`。
pub struct LeakGuard {
    baseline_allocs: usize,
    baseline_bytes: usize,
    enabled: bool,
}

impl LeakGuard {
    /// 记当前基线。global_allocator 未声明 [`CountingAllocator`] 时 enabled=false
    /// 全 no-op（诚实降级不虚报）。
    pub fn arm() -> Self {
        // 探测：分配一枚探针看计数器动不动——动=计数分配器在岗；不动=裸 System（或其他
        // 分配器），降级。black_box 防 LLVM 消除 malloc/free 对造成假阴性。
        let before = LIVE_ALLOCS.load(Ordering::Relaxed);
        let probe = Vec::<u8>::with_capacity(64);
        std::hint::black_box(&probe);
        let moved = LIVE_ALLOCS.load(Ordering::Relaxed) > before;
        drop(probe);
        Self {
            baseline_allocs: LIVE_ALLOCS.load(Ordering::Relaxed),
            baseline_bytes: LIVE_BYTES.load(Ordering::Relaxed),
            enabled: moved,
        }
    }

    /// allocs/bytes 回基线=Ok；否则 Err(LeakReport)。
    pub fn check(&self) -> Result<(), LeakReport> {
        if !self.enabled {
            return Ok(());
        }
        let allocs_delta =
            LIVE_ALLOCS.load(Ordering::Relaxed) as isize - self.baseline_allocs as isize;
        let bytes_delta =
            LIVE_BYTES.load(Ordering::Relaxed) as isize - self.baseline_bytes as isize;
        if allocs_delta == 0 && bytes_delta == 0 {
            Ok(())
        } else {
            Err(LeakReport {
                allocs_delta,
                bytes_delta,
            })
        }
    }
}

impl Drop for LeakGuard {
    fn drop(&mut self) {
        if let Err(report) = self.check() {
            eprintln!("LeakGuard drop 兜底（尽力而为，请改用显式 check()）: {report:?}");
        }
    }
}

/// 泄漏报告：相对基线的净增量。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeakReport {
    pub allocs_delta: isize,
    pub bytes_delta: isize,
}

/// 判据二：注册表排空断言——非空即 panic，带 len/capacity 现场。
/// 判据三（任务面 expect_idle）在 fx.rs——依赖 FxScope 类型故放彼处。
pub fn expect_empty<K: Ord, V>(reg: &BoundedRegistry<K, V>) {
    assert!(
        reg.is_empty(),
        "registry not drained: len={} capacity={}",
        reg.len(),
        reg.capacity()
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    /// ① insert/get/remove/len 恒等。
    #[test]
    fn insert_get_remove_len_roundtrip() {
        let mut reg = BoundedRegistry::bounded(4, Eviction::Reject);
        assert!(reg.is_empty());
        assert_eq!(reg.capacity(), 4);
        reg.insert(1, "a").expect("within capacity");
        reg.insert(2, "b").expect("within capacity");
        assert_eq!(reg.len(), 2);
        assert_eq!(reg.get(&1), Some(&"a"));
        assert_eq!(reg.get(&9), None);
        assert_eq!(reg.remove(&1), Some("a"));
        assert_eq!(reg.remove(&1), None);
        assert_eq!(reg.len(), 1);
        assert_eq!(reg.remove(&2), Some("b"));
        assert!(reg.is_empty());
    }

    /// ② 满+Reject=Err(BoundError)，原内容不动。
    #[test]
    fn reject_at_capacity_leaves_registry_untouched() {
        let mut reg = BoundedRegistry::bounded(2, Eviction::Reject);
        reg.insert(1, "a").expect("within capacity");
        reg.insert(2, "b").expect("within capacity");
        let err = reg.insert(3, "c").expect_err("full + Reject");
        assert_eq!(err.capacity, 2);
        assert_eq!(
            err.to_string(),
            "bounded registry full (capacity=2, eviction=Reject)"
        );
        assert_eq!(reg.len(), 2);
        assert_eq!(reg.get(&1), Some(&"a"));
        assert_eq!(reg.get(&2), Some(&"b"));
        assert_eq!(reg.get(&3), None);
    }

    /// ③ 满+EvictOldest=首键淘汰+新键入。
    #[test]
    fn evict_oldest_pops_min_key_then_inserts() {
        // K 契约实证：乱序插入（10 后插 5）时淘汰的是首键 5——如实退化 EvictMinKey。
        let mut reg = BoundedRegistry::bounded(2, Eviction::EvictOldest);
        reg.insert(10, "ten").expect("within capacity");
        reg.insert(5, "five").expect("within capacity");
        reg.insert(7, "seven").expect("eviction makes room");
        assert_eq!(reg.len(), 2);
        assert_eq!(reg.get(&5), None);
        assert_eq!(reg.get(&10), Some(&"ten"));
        assert_eq!(reg.get(&7), Some(&"seven"));
    }

    /// ④ 同 key 覆盖不触发淘汰。
    #[test]
    fn overwrite_existing_key_neither_evicts_nor_grows() {
        let mut reg = BoundedRegistry::bounded(1, Eviction::EvictOldest);
        reg.insert(1, "a").expect("within capacity");
        reg.insert(1, "b").expect("overwrite keeps len");
        assert_eq!(reg.len(), 1);
        assert_eq!(reg.get(&1), Some(&"b"));
        reg.insert(2, "c").expect("evict oldest to fit new key");
        assert_eq!(reg.get(&1), None);
        assert_eq!(reg.get(&2), Some(&"c"));

        let mut rej = BoundedRegistry::bounded(2, Eviction::Reject);
        rej.insert(1, "a").expect("within capacity");
        rej.insert(1, "b").expect("overwrite keeps len");
        assert_eq!(rej.len(), 1);
        rej.insert(2, "c").expect("still one slot free after overwrite");
        rej.insert(3, "d").expect_err("full + Reject, overwrite did not grow");
    }

    /// ⑤ iter 升序确定序（乱序插入断言序）。
    #[test]
    fn iter_is_ascending_deterministic_order() {
        let mut reg = BoundedRegistry::bounded(8, Eviction::Reject);
        for k in [5, 1, 3] {
            reg.insert(k, k * 10).expect("within capacity");
        }
        let keys: Vec<_> = reg.iter().map(|(k, _)| *k).collect();
        assert_eq!(keys, vec![1, 3, 5]);
        let pairs: Vec<_> = reg.iter().map(|(k, v)| (*k, *v)).collect();
        assert_eq!(pairs, vec![(1, 10), (3, 30), (5, 50)]);
    }

    /// ⑩ 未启用 CountingAllocator 时 arm→check=Ok（降级 no-op 诚实）。
    /// 本断言同时是绊线：若将来有人给 lib 测试二进制挂了计数分配器，此测试会立即红。
    #[test]
    fn leak_guard_degrades_to_noop_without_counting_allocator() {
        let guard = LeakGuard::arm();
        assert!(
            !guard.enabled,
            "lib test binary must NOT declare a CountingAllocator global allocator"
        );
        let _live = Vec::<u8>::with_capacity(64);
        assert!(guard.check().is_ok());
    }

    /// 零覆盖 #8：bounded(0) 构造期 panic——恒满注册表必是 bug，fail-fast
    /// 不给「永不可能插入成功」的僵尸实例留活口（构造面 fail-closed）。
    #[test]
    #[should_panic(expected = "max == 0 is a bug")]
    fn bounded_zero_capacity_panics_fail_fast() {
        let _ = BoundedRegistry::<u32, u32>::bounded(0, Eviction::Reject);
    }

}
