//! cache 域集成测试：坑面 armor 逐条钉死。
//!
//! 时间线全部经 [`MockClock`] 虚拟推进（TTL 零真睡）：若实现误走真实钟，
//! 虚拟钟冻结语义使「1ns TTL 即刻活」断言红——确定性不等式是真实钟通路
//! 的证伪器（ratelimit 同款手法）。

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;
use std::time::Duration;

use skeleton::clock::{Clock, MockClock, SystemClock};
use std_ext::cache::{LruCache, TtlCache};

// ═══ LruCache：LRU 精确语义 ═══

// ---- 基本往返 ----

#[test]
fn insert_get_roundtrip_and_miss() {
    let mut c = LruCache::new(4);
    assert!(c.is_empty());
    c.insert("a", 1);
    assert_eq!(c.get(&"a"), Some(&1));
    assert_eq!(c.get(&"missing"), None);
    assert_eq!(c.len(), 1);
    assert_eq!(c.capacity(), 4);
}

// ---- 坑面「get 是否算 touch」：get=touch，命中刷新 recency ----

#[test]
fn get_touches_recency() {
    let mut c = LruCache::new(2);
    c.insert("a", 1);
    c.insert("b", 2);
    assert_eq!(c.get(&"a"), Some(&1)); // a 变最新，b 成尾
    c.insert("c", 3); // 满 → 逐 b（非 a）
    assert_eq!(c.get(&"b"), None, "get=touch：a 被命中后 b 才是 LRU 尾");
    assert_eq!(c.get(&"a"), Some(&1));
    assert_eq!(c.get(&"c"), Some(&3));
}

// ---- 坑面「覆盖是否 touch / 是否淘汰」：覆盖=更新+touch，len 不变 ----

#[test]
fn overwrite_bumps_and_updates_without_eviction() {
    let mut c = LruCache::new(2);
    c.insert("a", 1);
    c.insert("b", 2);
    c.insert("a", 10); // 覆盖：更新值+touch，不触发淘汰
    assert_eq!(c.len(), 2);
    assert_eq!(c.get(&"a"), Some(&10));
    c.insert("c", 3); // 满 → 逐 b（a 刚被覆盖 touch）
    assert_eq!(c.get(&"b"), None);
    assert_eq!(c.get(&"a"), Some(&10));
}

// ---- 坑面「peek 是否 touch」：peek 纯读，不扰动淘汰序 ----

#[test]
fn peek_does_not_touch() {
    let mut c = LruCache::new(2);
    c.insert("a", 1);
    c.insert("b", 2);
    assert_eq!(c.peek(&"a"), Some(&1)); // peek 不 touch：a 仍是尾
    c.insert("c", 3);
    assert_eq!(c.get(&"a"), None, "peek 不刷新 recency：a 未被保护");
    assert_eq!(c.get(&"b"), Some(&2));
    assert_eq!(c.get(&"c"), Some(&3));
}

// ---- 坑面「peek 是否 touch」：contains 同为纯读 ----

#[test]
fn contains_does_not_touch() {
    let mut c = LruCache::new(2);
    c.insert("a", 1);
    c.insert("b", 2);
    assert!(c.contains(&"a"));
    c.insert("c", 3);
    assert!(!c.contains(&"a"), "contains 不刷新 recency");
}

// ---- 坑面「更新值不刷新 recency」：peek_mut 显式面 ----

#[test]
fn peek_mut_updates_without_touch() {
    let mut c = LruCache::new(2);
    c.insert("a", 1);
    c.insert("b", 2);
    if let Some(v) = c.peek_mut(&"a") {
        *v = 10;
    }
    assert_eq!(c.peek(&"a"), Some(&10), "值已就地更新");
    c.insert("c", 3); // a 未被 touch → 仍是尾 → 被逐
    assert_eq!(c.get(&"a"), None, "peek_mut 不刷新 recency");
    assert_eq!(c.get(&"b"), Some(&2));
}

// ---- 淘汰序全链验证 + pop_lru ----

#[test]
fn eviction_order_matches_recency_chain() {
    let mut c = LruCache::new(3);
    c.insert(1, 'a');
    c.insert(2, 'b');
    c.insert(3, 'c');
    assert_eq!(c.get(&1), Some(&'a')); // recency: 1,3,2
    c.insert(4, 'd'); // 逐 2（此时已不在缓存）→ recency: 4,1,3
    let drained: Vec<_> = std::iter::from_fn(|| c.pop_lru()).collect();
    assert_eq!(drained, vec![(3, 'c'), (1, 'a'), (4, 'd')]);
    assert!(c.is_empty());
}

// ---- remove 摘除 + arena 槽复用 ----

#[test]
fn remove_frees_slot_and_returns_value() {
    let mut c = LruCache::new(2);
    c.insert("a", 1);
    c.insert("b", 2);
    assert_eq!(c.remove(&"a"), Some(1));
    assert_eq!(c.remove(&"a"), None, "重复摘除 None");
    assert_eq!(c.len(), 1);
    c.insert("c", 3); // 复用 free 槽
    assert_eq!(c.len(), 2);
    assert_eq!(c.get(&"c"), Some(&3));
    assert_eq!(c.get(&"b"), Some(&2));
}

// ---- 构造不变量：cap=0 fail-fast ----

#[test]
fn cap_zero_panics() {
    let r = catch_unwind(AssertUnwindSafe(|| LruCache::<u8, u8>::new(0)));
    assert!(r.is_err(), "cap=0 必须构造期拒绝（恒满缓存必是 bug）");
}

// ---- iter recency 序确定 ----

#[test]
fn iter_recency_order_newest_first() {
    let mut c = LruCache::new(3);
    c.insert(1, 'a');
    c.insert(2, 'b');
    c.insert(3, 'c');
    let _ = c.get(&1); // recency: 1,3,2
    let order: Vec<_> = c.iter().map(|(k, v)| (*k, *v)).collect();
    assert_eq!(order, vec![(1, 'a'), (3, 'c'), (2, 'b')]);
}

// ---- arena/free-list 搅动正确性（确定性 PRNG，独立第二实现对账）----

#[test]
fn churn_reuses_slots_with_model_agreement() {
    /// 独立第二实现：VecDeque 前端=最新（O(n) 扫描换 oracle 简单性——
    /// 与 arena 内核零共享结构，交叉对账才有意义）。
    struct Oracle {
        cap: usize,
        dq: std::collections::VecDeque<(u64, u64)>,
    }
    impl Oracle {
        fn get(&mut self, k: u64) -> Option<u64> {
            let pos = self.dq.iter().position(|item| item.0 == k)?;
            let item = self.dq.remove(pos).unwrap();
            self.dq.push_front(item);
            Some(item.1)
        }
        fn insert(&mut self, k: u64, v: u64) {
            self.dq.retain(|item| item.0 != k); // 覆盖=移旧
            if self.dq.len() >= self.cap {
                self.dq.pop_back();
            }
            self.dq.push_front((k, v));
        }
        fn remove(&mut self, k: u64) -> Option<u64> {
            let pos = self.dq.iter().position(|item| item.0 == k)?;
            self.dq.remove(pos).map(|item| item.1)
        }
    }

    let mut c = LruCache::new(8);
    let mut oracle = Oracle {
        cap: 8,
        dq: std::collections::VecDeque::new(),
    };
    let mut rng: u64 = 0x9E3779B97F4A7C15; // xorshift 确定性
    let mut next = || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };
    for step in 0..2000u64 {
        let k = next() % 16; // 16 键挤 8 容量：淘汰路径高频入场
        match next() % 4 {
            0 | 1 => {
                let v = step;
                c.insert(k, v);
                oracle.insert(k, v);
            }
            2 => {
                assert_eq!(
                    c.get(&k).copied(),
                    oracle.get(k),
                    "step {step} 键 {k} 读漂移"
                );
            }
            _ => {
                assert_eq!(
                    c.remove(&k),
                    oracle.remove(k),
                    "step {step} 键 {k} 摘除漂移"
                );
            }
        }
        assert_eq!(c.len(), oracle.dq.len(), "step {step} 长度漂移");
        // 全内容对账（集合序，经 iter 面）
        let mut cache_items: Vec<_> = c.iter().map(|(k, v)| (*k, *v)).collect();
        let mut oracle_items = oracle.dq.iter().copied().collect::<Vec<_>>();
        cache_items.sort_unstable();
        oracle_items.sort_unstable();
        assert_eq!(cache_items, oracle_items, "step {step} 内容漂移");
    }
}

// ═══ TtlCache：MockClock 确定性 TTL 时间线 ═══

// ---- 基本往返 + 冻结钟不过期（真实钟通路证伪器）----

#[test]
fn frozen_mock_clock_never_expires() {
    let clock = Arc::new(MockClock::new());
    let mut c = TtlCache::new(clock.clone(), 4);
    c.insert("k", 1, Duration::from_nanos(1)); // 1ns TTL
    assert_eq!(
        c.get(&"k"),
        Some(&1),
        "仿钟冻结：无 advance 即无时间流逝（真钟通路必红）"
    );
    assert_eq!(clock.unix_offset(), Duration::ZERO);
}

#[test]
fn ttl_roundtrip_before_expiry() {
    let clock = Arc::new(MockClock::new());
    let mut c = TtlCache::new(clock.clone(), 4);
    c.insert("k", 1, Duration::from_millis(100));
    clock.advance(Duration::from_millis(99));
    assert_eq!(c.get(&"k"), Some(&1));
    clock.advance(Duration::from_millis(1));
    assert_eq!(c.get(&"k"), None, "恰 100ms 过期");
    assert_eq!(c.expired(), 1);
}

// ---- 惰性过期：触碰即回收 + fail-visible 计数 ----

#[test]
fn expired_get_returns_none_and_counts_once() {
    let clock = Arc::new(MockClock::new());
    let mut c = TtlCache::new(clock.clone(), 4);
    c.insert("k", 1, Duration::from_millis(100));
    clock.advance(Duration::from_millis(200));
    assert_eq!(c.get(&"k"), None);
    assert_eq!(c.get(&"k"), None, "键已回收：重复读不重复计数");
    assert_eq!(c.expired(), 1);
    assert_eq!(c.len(), 0, "惰性回收后物理条数回落");
}

#[test]
fn peek_expires_lazily_too() {
    let clock = Arc::new(MockClock::new());
    let mut c = TtlCache::new(clock.clone(), 4);
    c.insert("k", 1, Duration::from_millis(100));
    clock.advance(Duration::from_millis(150));
    assert_eq!(
        c.peek(&"k"),
        None,
        "peek 同样惰性过期（否则僵尸借 peek 复活）"
    );
    assert_eq!(c.expired(), 1);
}

// ---- purge_expired：显式清扫 ----

#[test]
fn purge_expired_count_and_len() {
    let clock = Arc::new(MockClock::new());
    let mut c = TtlCache::new(clock.clone(), 8);
    c.insert("a", 1, Duration::from_millis(50));
    c.insert("b", 2, Duration::from_millis(150));
    c.insert("c", 3, Duration::from_millis(250));
    clock.advance(Duration::from_millis(200)); // a、b 过期，c 活
    assert_eq!(c.purge_expired(), 2);
    assert_eq!(c.expired(), 2);
    assert_eq!(c.len(), 1);
    assert_eq!(c.peek(&"c"), Some(&3));
    assert_eq!(c.purge_expired(), 0, "二次清扫幂等");
    assert_eq!(c.expired(), 2, "无新增不重复计数");
}

// ---- 坑面「TTL+容量双界交互」：先过期先淘汰，再逐 LRU ----

#[test]
fn full_insert_purges_expired_before_lru() {
    let clock = Arc::new(MockClock::new());
    let mut c = TtlCache::new(clock.clone(), 2);
    c.insert("a", 1, Duration::from_millis(50)); // a 将过期
    c.insert("b", 2, Duration::from_millis(500));
    let _ = c.get(&"a"); // touch a：recency 尾=b（LRU 若先行会逐 b）
    clock.advance(Duration::from_millis(100)); // a 过期，b 活
    c.insert("c", 3, Duration::from_millis(100)); // 满插入
    assert_eq!(c.len(), 2);
    assert_eq!(
        c.peek(&"b"),
        Some(&2),
        "先过期先淘汰：过期 a 被清，活条目 b 不因 LRU 序陪葬"
    );
    assert_eq!(c.peek(&"c"), Some(&3));
    assert_eq!(c.expired(), 1);
}

#[test]
fn full_insert_evicts_lru_when_nothing_expired() {
    let clock = Arc::new(MockClock::new());
    let mut c = TtlCache::new(clock.clone(), 2);
    c.insert("a", 1, Duration::from_secs(100));
    c.insert("b", 2, Duration::from_secs(100));
    clock.advance(Duration::from_millis(1)); // 无一过期
    c.insert("c", 3, Duration::from_secs(100));
    assert_eq!(c.len(), 2);
    assert_eq!(c.peek(&"a"), None, "全活时按 LRU 逐尾 a");
    assert_eq!(c.peek(&"b"), Some(&2));
    assert_eq!(c.expired(), 0);
}

// ---- 坑面「覆盖重置计时」（expire-after-write）----

#[test]
fn overwrite_resets_expiry_clock() {
    let clock = Arc::new(MockClock::new());
    let mut c = TtlCache::new(clock.clone(), 4);
    c.insert("k", 1, Duration::from_millis(100));
    clock.advance(Duration::from_millis(60));
    c.insert("k", 2, Duration::from_millis(100)); // 重置计时（新限期 160）
    clock.advance(Duration::from_millis(60)); // =120，旧限期(100)已过
    assert_eq!(c.get(&"k"), Some(&2), "expire-after-write：覆盖重置计时");
    clock.advance(Duration::from_millis(41)); // =161 > 160
    assert_eq!(c.get(&"k"), None);
    assert_eq!(c.expired(), 1);
}

#[test]
fn overwrite_at_capacity_no_eviction() {
    let clock = Arc::new(MockClock::new());
    let mut c = TtlCache::new(clock.clone(), 2);
    c.insert("a", 1, Duration::from_secs(10));
    c.insert("b", 2, Duration::from_secs(10));
    c.insert("b", 20, Duration::from_secs(10)); // 覆盖：len 不变
    assert_eq!(c.len(), 2);
    assert_eq!(c.peek(&"a"), Some(&1));
    assert_eq!(c.peek(&"b"), Some(&20));
}

// ---- peek_mut 惰性过期面 ----

#[test]
fn peek_mut_expires_lazily_and_updates_in_place() {
    let clock = Arc::new(MockClock::new());
    let mut c = TtlCache::new(clock.clone(), 4);
    c.insert("k", 1, Duration::from_millis(100));
    if let Some(v) = c.peek_mut(&"k") {
        *v = 10;
    }
    assert_eq!(c.peek(&"k"), Some(&10));
    clock.advance(Duration::from_millis(150));
    assert_eq!(c.peek_mut(&"k"), None, "过期经 peek_mut 同样回收");
    assert_eq!(c.expired(), 1);
}

// ---- pop_lru/remove 物理面：不判过期 ----

#[test]
fn pop_lru_is_physical_no_expiry_check() {
    let clock = Arc::new(MockClock::new());
    let mut c = TtlCache::new(clock.clone(), 2);
    c.insert("a", 1, Duration::from_millis(10));
    c.insert("b", 2, Duration::from_millis(10));
    clock.advance(Duration::from_millis(50)); // 双双过期但未回收
    let victim = c.pop_lru();
    assert_eq!(
        victim,
        Some(("a", 1)),
        "pop_lru 物理面：过期与否不改变逐出对象"
    );
    assert_eq!(c.expired(), 0, "物理逐出不记入过期计数");
    assert_eq!(c.remove(&"b"), Some(2));
    assert!(c.is_empty());
}

// ---- 构造不变量与 fail-closed ----

#[test]
fn ttl_cache_cap_zero_panics() {
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let r = catch_unwind(AssertUnwindSafe(|| TtlCache::<u8, u8>::new(clock, 0)));
    assert!(r.is_err());
}

#[test]
fn ttl_zero_panics() {
    let clock = Arc::new(MockClock::new());
    let mut c = TtlCache::new(clock, 4);
    let r = catch_unwind(AssertUnwindSafe(|| {
        c.insert("k", 1, Duration::ZERO);
    }));
    assert!(r.is_err(), "ttl=0 是恒过期条目（fail-fast）");
}

#[test]
fn ttl_overflow_panics_state_intact() {
    let clock = Arc::new(MockClock::new());
    let mut c = TtlCache::new(clock, 4);
    let r = catch_unwind(AssertUnwindSafe(|| {
        c.insert("k", 1, Duration::MAX);
    }));
    assert!(r.is_err(), "now+ttl 溢出 fail-closed panic");
    assert_eq!(c.len(), 0, "panic 前未污染容器");
    c.insert("ok", 1, Duration::from_secs(1)); // 容器仍可用
    assert_eq!(c.get(&"ok"), Some(&1));
}

// ── log-embed 埋点（cache 事件点红绿；键身份以 DefaultHasher 定长种子指纹
// 入字段——K 无 Debug/Display bound，加 bound=公共 API 破坏性变更）──

#[cfg(feature = "log-embed")]
#[path = "common/log_capture.rs"]
mod log_capture;

#[cfg(feature = "log-embed")]
mod log_embed {
    use super::*;
    use log_capture::Capture;
    use std::hash::{Hash, Hasher};
    use tracing::Level;

    /// 与实现同构的键指纹（`DefaultHasher::new` 定长种子=进程内确定）：
    /// key_hash 字段断言的期望值来源。
    fn fingerprint(k: &str) -> String {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        k.hash(&mut h);
        h.finish().to_string()
    }

    // 埋点「TTL 过期驱逐（惰性单条）」：过期触碰回收 → debug（键指纹+累计数）。
    #[test]
    fn lazy_ttl_expire_emits_debug_event() {
        let clock = Arc::new(MockClock::new());
        let mut c = TtlCache::new(clock.clone(), 4);
        c.insert("k", 1, Duration::from_millis(100));
        clock.advance(Duration::from_millis(200));

        let capture = Capture::new();
        tracing::subscriber::with_default(capture.subscriber(), || {
            assert_eq!(c.get(&"k"), None);
        });

        let e = log_capture::only(&capture, "skeleton::cache::evict");
        assert_eq!(e.level, Level::DEBUG);
        assert_eq!(e.field("kind"), Some("ttl"));
        let expected = fingerprint("k");
        assert_eq!(e.field("key_hash"), Some(expected.as_str()), "键指纹入字段");
        assert_eq!(e.field("expired_total"), Some("1"), "过期累计入字段");
    }

    // 活键触碰零事件（埋点只挂边界，正常 get 零事件面）。
    #[test]
    fn live_get_emits_no_evict_event() {
        let clock = Arc::new(MockClock::new());
        let mut c = TtlCache::new(clock.clone(), 4);
        c.insert("k", 1, Duration::from_millis(100));
        let capture = Capture::new();
        tracing::subscriber::with_default(capture.subscriber(), || {
            assert_eq!(c.get(&"k"), Some(&1));
        });
        assert!(
            capture.events().is_empty(),
            "活键触碰零事件: {:?}",
            capture.events()
        );
    }

    // 埋点「TTL 过期驱逐（满插清扫）」：僵尸计数入字段（清扫 0 僵尸不发事件，
    // 该路径由容量淘汰事件承载）。
    #[test]
    fn full_insert_sweep_emits_ttl_sweep_with_zombie_count() {
        let clock = Arc::new(MockClock::new());
        let mut c = TtlCache::new(clock.clone(), 2);
        c.insert("a", 1, Duration::from_millis(50)); // 将成僵尸
        c.insert("b", 2, Duration::from_millis(500));
        clock.advance(Duration::from_millis(100));

        let capture = Capture::new();
        tracing::subscriber::with_default(capture.subscriber(), || {
            c.insert("c", 3, Duration::from_secs(10)); // 满插：清扫僵尸 a，清后不满无需 LRU
        });

        let e = log_capture::only(&capture, "skeleton::cache::evict");
        assert_eq!(e.level, Level::DEBUG);
        assert_eq!(e.field("kind"), Some("ttl_sweep"));
        assert_eq!(e.field("zombies"), Some("1"), "僵尸计数入字段");
        let expected = fingerprint("c");
        assert_eq!(
            e.field("key_hash"),
            Some(expected.as_str()),
            "触发键指纹入字段"
        );
    }

    // 埋点「容量淘汰（LRU 尾）」：LruCache 满插 → debug（被逐者指纹/LRU 序/残留）。
    #[test]
    fn lru_capacity_evict_emits_debug_event() {
        let mut c = LruCache::new(2);
        c.insert("x", 1);
        c.insert("y", 2);
        let capture = Capture::new();
        tracing::subscriber::with_default(capture.subscriber(), || {
            c.insert("z", 3); // 满：逐 LRU 尾 x
        });

        let e = log_capture::only(&capture, "skeleton::cache::evict");
        assert_eq!(e.level, Level::DEBUG);
        assert_eq!(e.field("kind"), Some("capacity"));
        assert_eq!(e.field("order"), Some("lru_tail"), "LRU 序选择策略入字段");
        let expected = fingerprint("x");
        assert_eq!(
            e.field("key_hash"),
            Some(expected.as_str()),
            "被逐者=x（LRU 尾）"
        );
        assert_eq!(
            e.field("len_at_evict"),
            Some("1"),
            "发事件瞬间=驱逐后、新键落位前"
        );
    }

    // 同上（TtlCache 站点）：全活满插的容量淘汰（清扫 0 僵尸 → 逐 LRU 尾）。
    #[test]
    fn ttl_capacity_evict_emits_debug_event() {
        let clock = Arc::new(MockClock::new());
        let mut c = TtlCache::new(clock.clone(), 2);
        c.insert("a", 1, Duration::from_secs(100));
        c.insert("b", 2, Duration::from_secs(100));
        let capture = Capture::new();
        tracing::subscriber::with_default(capture.subscriber(), || {
            c.insert("c", 3, Duration::from_secs(100));
        });

        let e = log_capture::only(&capture, "skeleton::cache::evict");
        assert_eq!(e.field("kind"), Some("capacity"));
        let expected = fingerprint("a");
        assert_eq!(
            e.field("key_hash"),
            Some(expected.as_str()),
            "被逐者=a（LRU 尾）"
        );
    }

    // 埋点「purge」：显式清扫 → info（回收数/残留入字段）。
    #[test]
    fn purge_expired_emits_info_event() {
        let clock = Arc::new(MockClock::new());
        let mut c = TtlCache::new(clock.clone(), 8);
        c.insert("a", 1, Duration::from_millis(10));
        c.insert("b", 2, Duration::from_millis(10));
        clock.advance(Duration::from_millis(50));
        let capture = Capture::new();
        tracing::subscriber::with_default(capture.subscriber(), || {
            assert_eq!(c.purge_expired(), 2);
        });

        let e = log_capture::only(&capture, "skeleton::cache::purge");
        assert_eq!(e.level, Level::INFO);
        assert_eq!(e.field("purged"), Some("2"));
        assert_eq!(e.field("len_after"), Some("0"));
    }
}
