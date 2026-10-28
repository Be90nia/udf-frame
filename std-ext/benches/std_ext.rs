//! std-ext bench：cache get/insert / retry_async 首试 / RandomId v4。
//!
//! 跑法：`cargo bench -p udf-std-ext --bench std_ext`（RandomId 组需 `--features
//! random`，默认关闭时本组空转不报错）。
//!
//! 取数口径对齐 PerfProbe（`verify/perf-probe/` 的 `cache`/`retry`/`id` 组）：
//! criterion 默认采样取中位，与 probe 的 7 轮中位量级可比；绝对值随机器漂移。
//! 分配面由集成测试钉住（`tests/alloc_anchors.rs`），bench 只测时间。
//!
//! 本机实测（i9-9900K@3.60GHz / Win 10.0.26200，criterion 默认采样中位）：
//! lru_get_hit 28.1 ns / lru_insert_existing_key 24.7 ns / ttl_get_hit 55.1 ns /
//! ttl_insert_existing_key 51.6 ns（TTL 多一次注入钟读）/ retry_async_first_attempt
//! 31.4 ns / random_id_v4 60.0 ns / random_fill_8x8 364.9 ns（8 次 8 字节 fill）。

use std::future::{ready, Future};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, Criterion};

use skeleton::clock::MockClock;
use std_ext::cache::{SyncLruCache, SyncTtlCache};
use std_ext::retry::{retry_async, AsyncSleeper, RetryPolicy};

// ---- cache：get 命中 + insert（已有键覆盖）----

fn cache_get_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("sync_cache");

    let lru: SyncLruCache<u64, u64> = SyncLruCache::new(64);
    for k in 0..64u64 {
        lru.insert(k, k);
    }
    group.bench_function("lru_get_hit", |b| {
        let mut k = 0u64;
        b.iter(|| {
            k = (k + 1) % 64;
            lru.get(black_box(&k))
        })
    });
    group.bench_function("lru_insert_existing_key", |b| {
        let mut k = 0u64;
        b.iter(|| {
            k = (k + 1) % 64;
            lru.insert(black_box(k), k + 1)
        })
    });

    let clock = Arc::new(MockClock::new());
    let ttl: SyncTtlCache<u64, u64> = SyncTtlCache::new(clock, 64);
    let ttl_dur = Duration::from_secs(60);
    for k in 0..64u64 {
        ttl.insert(k, k, ttl_dur);
    }
    group.bench_function("ttl_get_hit", |b| {
        let mut k = 0u64;
        b.iter(|| {
            k = (k + 1) % 64;
            ttl.get(black_box(&k))
        })
    });
    group.bench_function("ttl_insert_existing_key", |b| {
        let mut k = 0u64;
        b.iter(|| {
            k = (k + 1) % 64;
            ttl.insert(black_box(k), k + 1, ttl_dur)
        })
    });

    group.finish();
}

// ---- retry：retry_async 首试成功（ReadySleeper，排除 sleep 成本）----

/// 即刻就绪的睡眠口：首试成功路径不会走到它。
struct ReadySleeper;

impl AsyncSleeper for ReadySleeper {
    fn sleep(&self, _d: Duration) -> impl Future<Output = ()> + Send {
        ready(())
    }
}

/// 最小阻塞驱动：ReadySleeper 下 future 必然一步内就绪；有界循环防挂死。
fn run_to_ready<F: Future>(fut: F) -> F::Output {
    let mut fut = std::pin::pin!(fut);
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..64 {
        if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
            return v;
        }
    }
    panic!("future 在 64 次 poll 内未就绪（ReadySleeper 应立即完成）");
}

fn retry_first_attempt(c: &mut Criterion) {
    let policy = RetryPolicy::new(3, Duration::from_millis(10), Duration::from_secs(1));
    c.bench_function("retry_async_first_attempt", |b| {
        b.iter(|| {
            run_to_ready(retry_async(
                &policy,
                |_attempt| async { Ok::<u32, &'static str>(7) },
                ReadySleeper,
            ))
            .expect("首试成功")
        })
    });
}

// ---- random：RandomId v4 单枚 / fill 批量（random feature 门）----

fn random_id(c: &mut Criterion) {
    #[cfg(feature = "random")]
    {
        use std_ext::id::RandomId;

        c.bench_function("random_id_v4", |b| {
            b.iter(|| RandomId::new().expect("OS CSPRNG 可用"))
        });

        let mut buf = [0u8; 64];
        c.bench_function("random_fill_8x8", |b| {
            b.iter(|| {
                for chunk in buf.chunks_mut(8) {
                    RandomId::fill(chunk).expect("OS CSPRNG 可用");
                }
                black_box(&buf);
            })
        });
    }
    // 默认 feature 集下 random 门关闭：本组空转（criterion 照常跑其余组）。
    #[cfg(not(feature = "random"))]
    black_box(c);
}

criterion_group!(benches, cache_get_insert, retry_first_attempt, random_id);
criterion_main!(benches);
