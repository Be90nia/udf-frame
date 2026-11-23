//! cache 域集成测试：共享缓存的**用户 `K::clone` 在锁外**执行（GitHub issue#80）。
//!
//! 契约（issue#80 维护者拍板）：共享面（`SyncLruCache`/`SyncTtlCache`）的插入
//! 路径**不得在持有 cache 锁时执行用户 `K::clone`**——
//! - `K::clone` 可**重入**同一缓存（非重入 Mutex ⇒ 自死锁）；
//! - `K::clone` 可 **panic**（投毒 cache 锁 ⇒ 后续访问全 panic）。
//!
//! 实施形态（两阶段 + 重验证插入，维护者拍板）：
//! ① 持锁查存在 → 存在：更新值 + touch（**不 clone**，覆盖语义不变）；
//! ② 不存在：**放锁** → clone map 侧键副本（此刻无锁、无任何变更）→ **重取锁
//!    再查**（第二次锁定检查 = 操作线性化点）→ 仍不存在则用备好的键副本插入
//!    （锁内不调 `K::clone`）；已存在则走普通覆盖路径，**未被采用**的候选键
//!    **解锁后**才析构。
//!
//! 断言口径（禁 sleep，全部确定性）：
//! - **重入不死锁**：clone 内回调 cache（`len()`）必须正常返回——带上限的通道
//!   等待判定「不死锁」，超时即判红（不挂死 CI）；
//! - **panic 不投毒**：clone panic 后缓存仍可用，既有条目/LRU 序/计数均不变；
//! - **覆盖不 clone**：覆盖已有键不调 `K::clone`（克隆计数器钉死）；
//! - **并发首插同键**：两线程并发首次插入同一键 ⇒ 只剩一条记录，未被采用的
//!   候选键**锁外**析构（析构回调重入 cache ⇒ 锁内析构即死锁）；
//! - **TTL 早于清扫**：clone panic 发生在过期清扫/容量淘汰**之前**（僵尸条目与
//!   活条目都不得被提前动过）。
//!
//! 已知缺口（issue#80 只覆盖 clone 边界，另票跟踪）：`HashMap` 内部调用的用户
//! `Hash`/`Eq`、读面 `V::clone` 仍在共享锁内 —— 本文件不覆盖这两面。

use std::hash::{Hash, Hasher};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex, Weak};
use std::thread;
use std::time::Duration;

use skeleton::clock::MockClock;
use std_ext::cache::{SyncLruCache, SyncTtlCache};

/// 握手/等待上限（通道等待，非 sleep）。
const WAIT_LIMIT: Duration = Duration::from_secs(5);
/// 并发首插的会合上限（通道等待，非 sleep）。
const RENDEZVOUS_LIMIT: Duration = Duration::from_secs(2);

/// 探针计数（克隆次数 / 析构次数）。
#[derive(Default)]
struct ProbeState {
    clones: AtomicUsize,
    drops: AtomicUsize,
}

/// 克隆探针键：`Clone` 手动实现（计次 + 挂回调），`id` 参与 Eq/Hash（身份）。
///
/// `on_clone` 在 `Clone::clone` 内执行（重入 / panic 探针）；`on_drop` 在析构
/// 时执行（锁外析构探针）。两者都持 `Arc` 闭包（可重入、跨线程）。
struct CloneProbe {
    id: u32,
    state: Arc<ProbeState>,
    on_clone: Option<Arc<dyn Fn() + Send + Sync>>,
    on_drop: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl CloneProbe {
    /// 新探针（自带计数状态）。
    fn new(id: u32) -> Self {
        Self {
            id,
            state: Arc::new(ProbeState::default()),
            on_clone: None,
            on_drop: None,
        }
    }

    /// 与外部共享计数状态的探针（测试侧据此读克隆/析构次数）。
    fn with_state(id: u32, state: Arc<ProbeState>) -> Self {
        Self {
            id,
            state,
            on_clone: None,
            on_drop: None,
        }
    }

    /// 挂 clone 回调（重入 / panic 探针）。
    fn with_on_clone(mut self, f: impl Fn() + Send + Sync + 'static) -> Self {
        self.on_clone = Some(Arc::new(f));
        self
    }

    /// 挂析构回调（锁外析构探针）。
    fn with_on_drop(mut self, f: impl Fn() + Send + Sync + 'static) -> Self {
        self.on_drop = Some(Arc::new(f));
        self
    }
}

impl Clone for CloneProbe {
    fn clone(&self) -> Self {
        self.state.clones.fetch_add(1, Ordering::SeqCst);
        if let Some(cb) = &self.on_clone {
            cb();
        }
        Self {
            id: self.id,
            state: self.state.clone(),
            on_clone: self.on_clone.clone(),
            on_drop: self.on_drop.clone(),
        }
    }
}

impl Drop for CloneProbe {
    fn drop(&mut self) {
        self.state.drops.fetch_add(1, Ordering::SeqCst);
        if let Some(cb) = &self.on_drop {
            cb();
        }
    }
}

impl PartialEq for CloneProbe {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for CloneProbe {}

impl Hash for CloneProbe {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

/// 带上限等待 `op` 完成（死锁 ⇒ 超时判红，不挂死 CI）。
fn must_complete_within(op: impl FnOnce() + Send + 'static) {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        op();
        let _ = tx.send(());
    });
    rx.recv_timeout(WAIT_LIMIT)
        .expect("操作不得死锁（超时即死锁：用户 clone/析构跑在了 cache 锁内）");
}

// ═══ 重入不死锁：clone 内回调同一缓存 ═══

/// 新键插入：clone 重入 `cache.len()` 必须完成（锁内 clone ⇒ 非重入 Mutex 自死锁）。
#[test]
fn lru_new_key_clone_reentering_cache_completes() {
    let cache = Arc::new(SyncLruCache::new(4));
    let weak: Weak<SyncLruCache<CloneProbe, i32>> = Arc::downgrade(&cache);
    let reentered = Arc::new(AtomicBool::new(false));
    let flag = reentered.clone();
    let key = CloneProbe::new(1).with_on_clone(move || {
        if let Some(c) = weak.upgrade() {
            let _ = c.len();
            flag.store(true, Ordering::SeqCst);
        }
    });
    let c2 = cache.clone();
    must_complete_within(move || {
        c2.insert(key, 7);
    });
    assert!(
        reentered.load(Ordering::SeqCst),
        "clone 回调必须真的重入过 cache（否则本用例无鉴别力）"
    );
    assert_eq!(cache.len(), 1, "新键必须插入成功（clone 在锁外执行）");
    assert_eq!(cache.get(&CloneProbe::new(1)), Some(7), "值必须可读");
}

/// 新键插入（TTL 面）：clone 重入 `cache.len()` 必须完成。
#[test]
fn ttl_new_key_clone_reentering_cache_completes() {
    let cache = Arc::new(SyncTtlCache::new(Arc::new(MockClock::new()), 4));
    let weak: Weak<SyncTtlCache<CloneProbe, i32>> = Arc::downgrade(&cache);
    let reentered = Arc::new(AtomicBool::new(false));
    let flag = reentered.clone();
    let key = CloneProbe::new(1).with_on_clone(move || {
        if let Some(c) = weak.upgrade() {
            let _ = c.len();
            flag.store(true, Ordering::SeqCst);
        }
    });
    let c2 = cache.clone();
    must_complete_within(move || {
        c2.insert(key, 7, Duration::from_secs(60));
    });
    assert!(
        reentered.load(Ordering::SeqCst),
        "clone 回调必须真的重入过 cache（否则本用例无鉴别力）"
    );
    assert_eq!(cache.len(), 1, "新键必须插入成功（clone 在锁外执行）");
    assert_eq!(cache.get(&CloneProbe::new(1)), Some(7), "值必须可读");
}

// ═══ clone panic：缓存仍可用且未投毒，既有状态不变 ═══

/// clone panic 后：缓存仍可用（未投毒），既有条目/LRU 序/计数均不变。
#[test]
fn lru_panicking_clone_leaves_cache_usable_and_state_intact() {
    let cache = SyncLruCache::new(2);
    cache.insert(CloneProbe::new(1), 1);
    cache.insert(CloneProbe::new(2), 2);
    let before_len = cache.len();
    let before_hits = cache.hits();
    let before_evictions = cache.evictions();

    let key = CloneProbe::new(3).with_on_clone(|| panic!("clone 故意 panic（issue#80 探针）"));
    let result = catch_unwind(AssertUnwindSafe(|| cache.insert(key, 7)));
    assert!(result.is_err(), "clone panic 必须传播给调用方（不得被吞）");

    assert_eq!(
        cache.len(),
        before_len,
        "clone panic 不得改变条目数（未发生任何变更）"
    );
    assert_eq!(cache.hits(), before_hits, "clone panic 不得改变命中计数");
    assert_eq!(
        cache.evictions(),
        before_evictions,
        "clone panic 不得改变淘汰计数"
    );
    // LRU 序未变：再插一条触发淘汰，被逐的必须是更旧的 1（2 更新）。
    cache.insert(CloneProbe::new(4), 4);
    assert_eq!(
        cache.get(&CloneProbe::new(1)),
        None,
        "clone panic 不得扰动 LRU 序（更旧的 1 应先被淘汰）"
    );
    assert_eq!(cache.get(&CloneProbe::new(2)), Some(2), "较新的 2 应仍在");
}

/// clone panic 后（TTL 面）：缓存仍可用（未投毒），既有条目/计数均不变。
#[test]
fn ttl_panicking_clone_leaves_cache_usable_and_state_intact() {
    let clock = Arc::new(MockClock::new());
    let cache = SyncTtlCache::new(clock.clone(), 2);
    cache.insert(CloneProbe::new(1), 1, Duration::from_secs(60));
    cache.insert(CloneProbe::new(2), 2, Duration::from_secs(60));
    let before_len = cache.len();
    let before_hits = cache.hits();
    let before_evictions = cache.evictions();
    let before_expired = cache.expired();

    let key = CloneProbe::new(3).with_on_clone(|| panic!("clone 故意 panic（issue#80 探针）"));
    let result = catch_unwind(AssertUnwindSafe(|| {
        cache.insert(key, 7, Duration::from_secs(60));
    }));
    assert!(result.is_err(), "clone panic 必须传播给调用方（不得被吞）");

    assert_eq!(cache.len(), before_len, "clone panic 不得改变条目数");
    assert_eq!(cache.hits(), before_hits, "clone panic 不得改变命中计数");
    assert_eq!(
        cache.evictions(),
        before_evictions,
        "clone panic 不得改变淘汰计数"
    );
    assert_eq!(
        cache.expired(),
        before_expired,
        "clone panic 不得触发过期回收"
    );
    assert_eq!(
        cache.get(&CloneProbe::new(1)),
        Some(1),
        "既有条目仍可读（锁未被投毒）"
    );
    assert_eq!(
        cache.get(&CloneProbe::new(2)),
        Some(2),
        "既有条目仍可读（锁未被投毒）"
    );
}

// ═══ 覆盖已有键：不调 `K::clone` ═══

/// 覆盖已有键不得调 `K::clone`（新键才需要 map 侧键副本）。
#[test]
fn lru_overwrite_existing_key_does_not_clone() {
    let cache = SyncLruCache::new(4);
    let state = Arc::new(ProbeState::default());
    cache.insert(CloneProbe::with_state(1, state.clone()), 1);
    assert_eq!(
        state.clones.load(Ordering::SeqCst),
        1,
        "新键插入需一次 clone（map 侧键副本）"
    );
    cache.insert(CloneProbe::with_state(1, state.clone()), 2);
    assert_eq!(
        state.clones.load(Ordering::SeqCst),
        1,
        "覆盖已有键不得调 K::clone（现有可观察语义：覆盖不 clone）"
    );
    assert_eq!(cache.len(), 1, "覆盖不新增条目");
    assert_eq!(cache.get(&CloneProbe::new(1)), Some(2), "覆盖后值更新");
}

/// 覆盖已有键不得调 `K::clone`（TTL 面；覆盖同时重置过期时刻）。
#[test]
fn ttl_overwrite_existing_key_does_not_clone() {
    let clock = Arc::new(MockClock::new());
    let cache = SyncTtlCache::new(clock.clone(), 4);
    let state = Arc::new(ProbeState::default());
    cache.insert(
        CloneProbe::with_state(1, state.clone()),
        1,
        Duration::from_secs(60),
    );
    assert_eq!(
        state.clones.load(Ordering::SeqCst),
        1,
        "新键插入需一次 clone（map 侧键副本）"
    );
    cache.insert(
        CloneProbe::with_state(1, state.clone()),
        2,
        Duration::from_secs(60),
    );
    assert_eq!(
        state.clones.load(Ordering::SeqCst),
        1,
        "覆盖已有键不得调 K::clone（现有可观察语义：覆盖不 clone）"
    );
    assert_eq!(cache.len(), 1, "覆盖不新增条目");
    assert_eq!(cache.get(&CloneProbe::new(1)), Some(2), "覆盖后值更新");
}

// ═══ 并发首插同键：一条记录 + 未采用的候选键锁外析构 ═══

/// 两线程并发首次插入同一键 ⇒ 只剩一条记录；未被采用的候选键在**锁外**析构
/// （析构回调重入 cache ⇒ 锁内析构即自死锁）。
#[test]
fn concurrent_first_inserts_of_same_key_leave_one_entry() {
    let cache = Arc::new(SyncLruCache::new(4));
    let weak: Weak<SyncLruCache<CloneProbe, i32>> = Arc::downgrade(&cache);
    let drops = Arc::new(AtomicUsize::new(0));
    let clones = Arc::new(AtomicUsize::new(0));

    // 会合：两线程的 clone 回调互等（确定性并发首插，禁 sleep）。
    let (tx_a, rx_a) = mpsc::channel::<()>();
    let (tx_b, rx_b) = mpsc::channel::<()>();

    let spawn = |tx: Sender<()>, rx: Arc<Mutex<Receiver<()>>>, value: i32| {
        let cache = cache.clone();
        let weak = weak.clone();
        let drops = drops.clone();
        let clones = clones.clone();
        thread::spawn(move || {
            let key = CloneProbe::new(1)
                .with_on_clone(move || {
                    clones.fetch_add(1, Ordering::SeqCst);
                    let _ = tx.send(());
                    let _ = rx
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .recv_timeout(RENDEZVOUS_LIMIT);
                })
                .with_on_drop(move || {
                    drops.fetch_add(1, Ordering::SeqCst);
                    if let Some(c) = weak.upgrade() {
                        let _ = c.len();
                    }
                });
            cache.insert(key, value);
        })
    };

    let t_a = spawn(tx_a, Arc::new(Mutex::new(rx_b)), 1);
    let t_b = spawn(tx_b, Arc::new(Mutex::new(rx_a)), 2);
    must_complete_within(move || {
        t_a.join().expect("线程 A 不得 panic");
        t_b.join().expect("线程 B 不得 panic");
    });

    assert_eq!(
        clones.load(Ordering::SeqCst),
        2,
        "两线程各备一份 map 侧键副本（clone 在锁外发生）"
    );
    assert_eq!(cache.len(), 1, "并发首插同键 ⇒ 只剩一条记录");
    assert_eq!(
        drops.load(Ordering::SeqCst),
        2,
        "未被采用的候选键（传入键 + 备好的键副本）在锁外各析构一次"
    );
    let value = cache.get(&CloneProbe::new(1));
    assert!(
        matches!(value, Some(1) | Some(2)),
        "值必须是两者之一（线性化结果自洽）：{value:?}"
    );
    assert_eq!(cache.capacity(), 4, "容量语义不变");
}

// ═══ TTL：clone panic 早于过期清扫 / 容量淘汰 ═══

/// TTL 未满：clone panic 必须发生在过期清扫**之前**（僵尸条目仍在、计数不变）。
#[test]
fn ttl_clone_panic_precedes_expiry_sweep_when_not_full() {
    let clock = Arc::new(MockClock::new());
    let cache = SyncTtlCache::new(clock.clone(), 4);
    cache.insert(CloneProbe::new(1), 1, Duration::from_millis(100));
    clock.advance(Duration::from_millis(150));
    assert_eq!(cache.len(), 1, "过期条目作为僵尸仍占容（惰性过期语义）");

    let key = CloneProbe::new(2).with_on_clone(|| panic!("clone 故意 panic（issue#80 探针）"));
    let result = catch_unwind(AssertUnwindSafe(|| {
        cache.insert(key, 7, Duration::from_secs(60));
    }));
    assert!(result.is_err(), "clone panic 必须传播给调用方（不得被吞）");
    assert_eq!(
        cache.len(),
        1,
        "clone panic 发生在过期清扫之前 ⇒ 僵尸条目不得被提前回收"
    );
    assert_eq!(
        cache.expired(),
        0,
        "clone panic 不得触发过期清扫计数（清扫在 panic 之后才该发生）"
    );
    assert_eq!(cache.evictions(), 0, "clone panic 不得触发容量淘汰");
    assert_eq!(
        cache.get(&CloneProbe::new(1)),
        None,
        "过期条目按惰性过期语义读为 miss"
    );
}

/// TTL 已满：clone panic 必须发生在容量淘汰**之前**（活条目不得被提前逐出）。
#[test]
fn ttl_clone_panic_precedes_eviction_when_full() {
    let clock = Arc::new(MockClock::new());
    let cache = SyncTtlCache::new(clock.clone(), 1);
    cache.insert(CloneProbe::new(1), 1, Duration::from_secs(60));

    let key = CloneProbe::new(2).with_on_clone(|| panic!("clone 故意 panic（issue#80 探针）"));
    let result = catch_unwind(AssertUnwindSafe(|| {
        cache.insert(key, 7, Duration::from_secs(60));
    }));
    assert!(result.is_err(), "clone panic 必须传播给调用方（不得被吞）");
    assert_eq!(
        cache.len(),
        1,
        "clone panic 发生在容量淘汰之前 ⇒ 活条目不得被提前逐出"
    );
    assert_eq!(cache.evictions(), 0, "clone panic 不得触发容量淘汰计数");
    assert_eq!(cache.expired(), 0, "clone panic 不得触发过期清扫");
    assert_eq!(
        cache.get(&CloneProbe::new(1)),
        Some(1),
        "活条目仍可读（锁未被投毒）"
    );
}
