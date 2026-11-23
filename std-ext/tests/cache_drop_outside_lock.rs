//! cache 域集成测试：共享缓存的**用户析构在锁外**执行（GitHub issue#71）。
//!
//! 契约（#71 `## Proposed design`）：共享面（`SyncLruCache`/`SyncTtlCache`）
//! 的临界区内**只做簿记与所有权转移**——被覆盖/被淘汰/被过期回收/被摘除的
//! **键（map 侧键副本 + arena 侧槽键）与值**的所有权保留到**解锁之后**才
//! 析构（map 侧键用 `HashMap::remove_entry` 取回，不在锁内析构）。
//!
//! 断言口径（禁 sleep）：
//! - **重入不死锁**：析构内回调 cache（`len()`）必须正常返回——锁内析构 =
//!   非重入 Mutex 自死锁；带上限的通道等待判定「不死锁」，超时即判红
//!   （不挂死 CI）。
//! - **锁确已释放**：析构进行中旁线程能取到同一把锁（确定性握手：析构方报
//!   「我进了析构」→ 旁线程取锁 → 取到即证明临界区已释放）。
//! - **恰好一次**：每份所有权（map 键副本 / arena 键 / 值）各析构一次——
//!   计数精确值钉死泄漏与双析构。
//!
//! 已知缺口（#71 只覆盖析构面，另票跟踪）：`LruCore::insert` 的 `k.clone()`
//! （map 侧键副本的**克隆**）仍在锁内执行——`K::clone` 是用户代码，重入同样会
//! 自锁死。本文件不覆盖该面。

use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex, Weak};
use std::thread;
use std::time::Duration;

use skeleton::clock::MockClock;
use std_ext::cache::{SyncLruCache, SyncTtlCache};

/// 握手/等待上限（通道等待，非 sleep）。
const WAIT_LIMIT: Duration = Duration::from_secs(5);
/// 析构握手自身的等待上限（旁线程取锁确认）。
const HANDSHAKE_LIMIT: Duration = Duration::from_secs(3);

/// 析构探针（值侧）：析构时执行 `on_drop`（重入缓存面 / 计数面用）。
///
/// 回调持 `Arc`（非 `Box`）：`SyncTtlCache::get` 返 owned 值要求 `V: Clone`。
#[derive(Clone)]
struct ValueProbe {
    on_drop: Arc<dyn Fn() + Send + Sync>,
}

impl ValueProbe {
    fn new(on_drop: impl Fn() + Send + Sync + 'static) -> Self {
        Self {
            on_drop: Arc::new(on_drop),
        }
    }

    /// 空探针（对照臂：不做事）。
    fn noop() -> Self {
        Self::new(|| {})
    }
}

impl Drop for ValueProbe {
    fn drop(&mut self) {
        (self.on_drop)();
    }
}

/// 键探针（键侧）：析构时执行 `on_drop`；`id` 参与 Eq/Hash（身份）。
#[derive(Clone)]
struct KeyProbe {
    id: u32,
    on_drop: Arc<dyn Fn() + Send + Sync>,
}

impl KeyProbe {
    fn new(id: u32, on_drop: impl Fn() + Send + Sync + 'static) -> Self {
        Self {
            id,
            on_drop: Arc::new(on_drop),
        }
    }
}

impl PartialEq for KeyProbe {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for KeyProbe {}

impl Hash for KeyProbe {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl Drop for KeyProbe {
    fn drop(&mut self) {
        (self.on_drop)();
    }
}

/// 析构握手闭包：进入析构时报信 + 等旁线程确认它取到了 cache 锁。
///
/// 旁线程能在**析构进行中**取到同一把锁 ⇒ 临界区已释放（锁外析构）。
/// 只跑一次（一个条目的键有两份所有权，第二份直接跳过，避免二次等待）。
fn handshake_on_drop(
    exited: Arc<AtomicBool>,
    tx: Sender<()>,
    ack: Mutex<Receiver<()>>,
) -> impl Fn() + Send + Sync + 'static {
    let once = Arc::new(AtomicBool::new(false));
    move || {
        if once.swap(true, Ordering::SeqCst) {
            return;
        }
        let _ = tx.send(());
        let _ = ack
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .recv_timeout(HANDSHAKE_LIMIT);
        exited.store(true, Ordering::SeqCst);
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
        .expect("操作不得死锁（超时即死锁：用户析构跑在了 cache 锁内）");
}

// ═══ SyncLruCache：值侧析构 ═══

/// 容量淘汰：被逐值的析构重入 cache 不得死锁（锁内析构 = 非重入 Mutex 自死锁）。
#[test]
fn evicted_value_drop_reentering_cache_does_not_deadlock() {
    let cache = Arc::new(SyncLruCache::new(1));
    let weak: Weak<SyncLruCache<String, ValueProbe>> = Arc::downgrade(&cache);
    let reentered = Arc::new(AtomicBool::new(false));
    let flag = reentered.clone();
    let probe = ValueProbe::new(move || {
        if let Some(c) = weak.upgrade() {
            let _ = c.len();
            flag.store(true, Ordering::SeqCst);
        }
    });
    cache.insert("a".to_string(), probe);
    let c2 = cache.clone();
    must_complete_within(move || {
        c2.insert("b".to_string(), ValueProbe::noop());
    });
    assert!(
        reentered.load(Ordering::SeqCst),
        "被淘汰值的析构必须执行且重入成功（锁内析构 ⇒ 自死锁）"
    );
}

/// 覆盖（替换）：旧值的析构重入 cache 不得死锁。
#[test]
fn replaced_value_drop_reentering_cache_does_not_deadlock() {
    let cache = Arc::new(SyncLruCache::new(4));
    let weak: Weak<SyncLruCache<String, ValueProbe>> = Arc::downgrade(&cache);
    let reentered = Arc::new(AtomicBool::new(false));
    let flag = reentered.clone();
    let probe = ValueProbe::new(move || {
        if let Some(c) = weak.upgrade() {
            let _ = c.len();
            flag.store(true, Ordering::SeqCst);
        }
    });
    cache.insert("a".to_string(), probe);
    let c2 = cache.clone();
    must_complete_within(move || {
        c2.insert("a".to_string(), ValueProbe::noop());
    });
    assert!(
        reentered.load(Ordering::SeqCst),
        "被覆盖旧值的析构必须执行且重入成功"
    );
}

/// 摘除：返回值交还调用方后析构，重入 cache 不得死锁。
#[test]
fn removed_value_drop_reentering_cache_does_not_deadlock() {
    let cache = Arc::new(SyncLruCache::new(4));
    let weak: Weak<SyncLruCache<String, ValueProbe>> = Arc::downgrade(&cache);
    let reentered = Arc::new(AtomicBool::new(false));
    let flag = reentered.clone();
    let probe = ValueProbe::new(move || {
        if let Some(c) = weak.upgrade() {
            let _ = c.len();
            flag.store(true, Ordering::SeqCst);
        }
    });
    cache.insert("a".to_string(), probe);
    let c2 = cache.clone();
    must_complete_within(move || {
        let removed = c2.remove(&"a".to_string());
        drop(removed);
    });
    assert!(
        reentered.load(Ordering::SeqCst),
        "被摘除值的析构必须执行且重入成功"
    );
}

// ═══ SyncLruCache：键侧析构 ═══

/// 容量淘汰：被逐键的析构重入 cache 不得死锁（键有两份所有权：map 副本 + arena 键）。
#[test]
fn evicted_key_drop_reentering_cache_does_not_deadlock() {
    let cache = Arc::new(SyncLruCache::new(1));
    let weak: Weak<SyncLruCache<KeyProbe, i32>> = Arc::downgrade(&cache);
    let reentered = Arc::new(AtomicBool::new(false));
    let flag = reentered.clone();
    let key = KeyProbe::new(1, move || {
        if let Some(c) = weak.upgrade() {
            let _ = c.len();
            flag.store(true, Ordering::SeqCst);
        }
    });
    cache.insert(key, 7);
    let c2 = cache.clone();
    must_complete_within(move || {
        c2.insert(KeyProbe::new(2, || {}), 8);
    });
    assert!(
        reentered.load(Ordering::SeqCst),
        "被淘汰键的析构必须执行且重入成功"
    );
}

/// 摘除：键的两份所有权析构重入 cache 不得死锁。
#[test]
fn removed_key_drop_reentering_cache_does_not_deadlock() {
    let cache = Arc::new(SyncLruCache::new(4));
    let weak: Weak<SyncLruCache<KeyProbe, i32>> = Arc::downgrade(&cache);
    let reentered = Arc::new(AtomicBool::new(false));
    let flag = reentered.clone();
    let key = KeyProbe::new(1, move || {
        if let Some(c) = weak.upgrade() {
            let _ = c.len();
            flag.store(true, Ordering::SeqCst);
        }
    });
    cache.insert(key, 7);
    // 查找副本：回调空转（查找用的临时键不污染计数/重入断言）。
    let lookup = KeyProbe::new(1, || {});
    let c2 = cache.clone();
    must_complete_within(move || {
        assert!(c2.remove(&lookup).is_some(), "摘除必须命中");
    });
    assert!(
        reentered.load(Ordering::SeqCst),
        "被摘除键的析构必须执行且重入成功"
    );
}

// ═══ 确定性握手：析构进行中锁确已释放 ═══

/// 容量淘汰：旁线程能在被逐值析构进行中取到 cache 锁 ⇒ 析构在锁外。
#[test]
fn evicted_value_drop_runs_after_lock_released() {
    let cache = Arc::new(SyncLruCache::new(1));
    let exited = Arc::new(AtomicBool::new(false));
    let (entered_tx, entered_rx) = mpsc::channel();
    let (ack_tx, ack_rx) = mpsc::channel();
    let probe = ValueProbe::new(handshake_on_drop(
        exited.clone(),
        entered_tx,
        Mutex::new(ack_rx),
    ));
    cache.insert("a".to_string(), probe);

    let helper_cache = cache.clone();
    let helper_exited = exited.clone();
    let helper = thread::spawn(move || {
        entered_rx
            .recv_timeout(WAIT_LIMIT)
            .expect("被淘汰值必须析构");
        let _len = helper_cache.len(); // 取锁：锁内析构 ⇒ 这里阻塞到握手超时
        let still_dropping = !helper_exited.load(Ordering::SeqCst);
        let _ = ack_tx.send(());
        still_dropping
    });
    cache.insert("b".to_string(), ValueProbe::noop());
    assert!(
        helper.join().unwrap(),
        "被淘汰值的析构必须在解锁之后（旁线程应能在析构进行中取到 cache 锁）"
    );
}

/// 摘除：旁线程能在被摘除键析构进行中取到 cache 锁 ⇒ 析构在锁外。
#[test]
fn removed_key_drop_runs_after_lock_released() {
    let cache = Arc::new(SyncLruCache::new(4));
    let exited = Arc::new(AtomicBool::new(false));
    let (entered_tx, entered_rx) = mpsc::channel();
    let (ack_tx, ack_rx) = mpsc::channel();
    let key = KeyProbe::new(
        1,
        handshake_on_drop(exited.clone(), entered_tx, Mutex::new(ack_rx)),
    );
    cache.insert(key, 7);

    let helper_cache = cache.clone();
    let helper_exited = exited.clone();
    let helper = thread::spawn(move || {
        entered_rx
            .recv_timeout(WAIT_LIMIT)
            .expect("被摘除键必须析构");
        let _len = helper_cache.len();
        let still_dropping = !helper_exited.load(Ordering::SeqCst);
        let _ = ack_tx.send(());
        still_dropping
    });
    let lookup = KeyProbe::new(1, || {});
    assert!(cache.remove(&lookup).is_some(), "摘除必须命中");
    assert!(
        helper.join().unwrap(),
        "被摘除键的析构必须在解锁之后（旁线程应能在析构进行中取到 cache 锁）"
    );
}

// ═══ SyncTtlCache：过期面析构 ═══

/// 惰性过期（触碰即回收）：被回收值的析构重入 cache 不得死锁。
#[test]
fn ttl_expired_on_get_drops_value_outside_lock() {
    let clock = Arc::new(MockClock::new());
    let cache = Arc::new(SyncTtlCache::new(clock.clone(), 4));
    let weak: Weak<SyncTtlCache<String, ValueProbe>> = Arc::downgrade(&cache);
    let reentered = Arc::new(AtomicBool::new(false));
    let flag = reentered.clone();
    let probe = ValueProbe::new(move || {
        if let Some(c) = weak.upgrade() {
            let _ = c.len();
            flag.store(true, Ordering::SeqCst);
        }
    });
    cache.insert("k".to_string(), probe, Duration::from_millis(100));
    clock.advance(Duration::from_millis(150));
    let c2 = cache.clone();
    must_complete_within(move || {
        assert!(c2.get("k").is_none(), "过期触碰按 miss");
    });
    assert!(
        reentered.load(Ordering::SeqCst),
        "过期回收值的析构必须执行且重入成功"
    );
}

/// 满插清扫（先清过期）：被回收值的析构重入 cache 不得死锁。
#[test]
fn ttl_full_insert_sweep_drops_value_outside_lock() {
    let clock = Arc::new(MockClock::new());
    let cache = Arc::new(SyncTtlCache::new(clock.clone(), 1));
    let weak: Weak<SyncTtlCache<String, ValueProbe>> = Arc::downgrade(&cache);
    let reentered = Arc::new(AtomicBool::new(false));
    let flag = reentered.clone();
    let probe = ValueProbe::new(move || {
        if let Some(c) = weak.upgrade() {
            let _ = c.len();
            flag.store(true, Ordering::SeqCst);
        }
    });
    cache.insert("a".to_string(), probe, Duration::from_millis(100));
    clock.advance(Duration::from_millis(150));
    let c2 = cache.clone();
    must_complete_within(move || {
        c2.insert("b".to_string(), ValueProbe::noop(), Duration::from_secs(60));
    });
    assert!(
        reentered.load(Ordering::SeqCst),
        "满插清扫回收值的析构必须执行且重入成功"
    );
}

/// 显式清扫（`purge_expired`）：被回收键的析构重入 cache 不得死锁。
#[test]
fn ttl_purge_expired_drops_key_outside_lock() {
    let clock = Arc::new(MockClock::new());
    let cache = Arc::new(SyncTtlCache::new(clock.clone(), 4));
    let weak: Weak<SyncTtlCache<KeyProbe, i32>> = Arc::downgrade(&cache);
    let reentered = Arc::new(AtomicBool::new(false));
    let flag = reentered.clone();
    let key = KeyProbe::new(1, move || {
        if let Some(c) = weak.upgrade() {
            let _ = c.len();
            flag.store(true, Ordering::SeqCst);
        }
    });
    cache.insert(key, 7, Duration::from_millis(100));
    clock.advance(Duration::from_millis(150));
    let c2 = cache.clone();
    must_complete_within(move || {
        assert_eq!(c2.purge_expired(), 1, "显式清扫回收 1 条");
    });
    assert!(
        reentered.load(Ordering::SeqCst),
        "显式清扫回收键的析构必须执行且重入成功"
    );
}

// ═══ 恰好一次：每份所有权各析构一次（钉死泄漏与双析构） ═══

/// 淘汰/摘除/覆盖三路的析构计数：map 键副本 + arena 键各一次，值一次。
#[test]
fn retired_entries_are_destroyed_exactly_once() {
    let key_drops = Arc::new(AtomicUsize::new(0));
    let value_drops = Arc::new(AtomicUsize::new(0));

    let cache: SyncLruCache<KeyProbe, ValueProbe> = SyncLruCache::new(1);
    {
        let kd = key_drops.clone();
        let key = KeyProbe::new(1, move || {
            kd.fetch_add(1, Ordering::SeqCst);
        });
        let vd = value_drops.clone();
        let value = ValueProbe::new(move || {
            vd.fetch_add(1, Ordering::SeqCst);
        });
        cache.insert(key, value);
        assert_eq!(key_drops.load(Ordering::SeqCst), 0, "插入不析构");
        assert_eq!(value_drops.load(Ordering::SeqCst), 0, "插入不析构");

        // 满插 ⇒ 淘汰：键两份所有权（map 副本 + arena 键）+ 值各析构一次。
        // 新条目挂「计数值」——下一步覆盖它即验证覆盖路径的析构计数。
        let vd2 = value_drops.clone();
        cache.insert(
            KeyProbe::new(2, || {}),
            ValueProbe::new(move || {
                vd2.fetch_add(1, Ordering::SeqCst);
            }),
        );
        assert_eq!(
            key_drops.load(Ordering::SeqCst),
            2,
            "被淘汰键的两份所有权各析构一次（map 副本 + arena 键）"
        );
        assert_eq!(value_drops.load(Ordering::SeqCst), 1, "被淘汰值析构一次");

        // 覆盖 ⇒ 旧值析构一次（键不析构：仍在缓存里）。
        cache.insert(KeyProbe::new(2, || {}), ValueProbe::noop());
        assert_eq!(
            value_drops.load(Ordering::SeqCst),
            2,
            "被覆盖旧值析构一次（累计）"
        );
        assert_eq!(key_drops.load(Ordering::SeqCst), 2, "覆盖不析构键");
    }
    // 缓存析构（作用域末）后计数继续增长——本断言只覆盖「退休批」路径。
}
