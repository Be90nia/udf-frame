#![cfg(feature = "pool")]
//! [`ByteBudget`] 并发账本契约：全局硬上限、对称回卷（下溢 fail-fast 且不留脏账）、
//! 耗尽拒绝不伪造成功、无碎片浪费/饿死（单字账本不分片，故 `held < total` 时
//! 任何 `n <= total - held` 都认领得到）。
//!
//! 并发面用「采样线程 + churn 线程」的双向夹逼：churn 只做合法借还，采样线程
//! 断言**任一时刻** `held <= total`；churn 结束后账目必须**精确**归零（漂移即失
//! 败）。耗尽面用确定性容量（`total / n` 件恰好可认领）钉住「既不多给也不少给」。

use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, Barrier,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use skeleton::clock::SystemClock;
use skeleton_infra::objpool::{ByteBudget, ObjectPool};

/// 耗尽态读数精确：认领到最后一字节才拒绝，拒绝不伪造成功。
#[test]
fn exhaustion_is_exact_and_refusals_are_real() {
    let budget = ByteBudget::new(1024);
    for taken in 1..=1024 {
        assert!(budget.try_take(1), "第 {taken} 字节应可认领");
        assert_eq!(budget.held(), taken);
    }
    assert_eq!(budget.available(), 0);
    assert!(!budget.try_take(1), "满额后必须拒绝");
    assert_eq!(budget.held(), 1024, "拒绝不得改动账目");

    budget.give_back(1024);
    assert_eq!(budget.held(), 0);
    assert_eq!(budget.available(), 1024);
}

/// 单字账本无碎片：一堆小件占满前仍能整额认领剩余额度（分片账本会在这里露出
/// 「某片满、他片空」的浪费面）。
#[test]
fn no_fragmentation_when_many_small_pieces_are_held() {
    let budget = ByteBudget::new(4096);
    for _ in 0..4095 {
        assert!(budget.try_take(1));
    }
    assert!(!budget.try_take(2), "只剩 1 字节，要 2 字节必须拒绝");
    assert!(budget.try_take(1), "剩余 1 字节必须认领得到（无碎片浪费）");
    budget.give_back(4096);
    assert!(budget.try_take(4096), "全归还后整额认领必须成功");
}

/// 超大请求按 `checked_add` 拒绝，不靠回绕「看起来有余量」。
///
/// **必须带非零前置状态**：空账本时 `wrapping_add(0, MAX) = MAX > total`，回绕
/// 实现也会拒绝（旧版测试因此在变异下照样绿）。`held == 1` 时
/// `wrapping_add(1, MAX) = 0 <= total` ⇒ 回绕实现会受理并把账本清零，这条才钉得住。
#[test]
fn huge_request_is_refused_without_wrapping() {
    let budget = ByteBudget::new(1024);

    // 空账本：超大请求被拒（回绕实现同样拒绝 —— 单靠这条没有鉴别力）
    assert!(!budget.try_take(usize::MAX), "空账本也必须拒收 usize::MAX");
    assert_eq!(budget.held(), 0);

    // 非零前置：held == 1（wrapping_add(1, MAX) 回绕到 0，正是变异会受理的那一点）
    assert!(budget.try_take(1));
    let held_before = budget.held();
    let available_before = budget.available();
    assert_eq!(held_before, 1);

    assert!(
        !budget.try_take(usize::MAX),
        "held == 1 时 usize::MAX 会回绕到 0（看起来有余量），必须拒收"
    );
    assert_eq!(budget.held(), held_before, "拒收不得改动账本");
    assert_eq!(budget.available(), available_before, "拒收不得改动余额");
    assert!(!budget.try_take(1024), "剩余 1023 字节，要 1024 必须拒收");

    budget.give_back(1);
    assert_eq!(budget.held(), 0);
}

/// `n == 0` 恒免账（未启用计量面的调用点短路契约），满额时同样成立。
#[test]
fn zero_is_free_even_when_full() {
    let budget = ByteBudget::new(8);
    assert!(budget.try_take(8));
    assert!(budget.try_take(0));
    assert_eq!(budget.held(), 8);
    budget.give_back(0);
    assert_eq!(budget.held(), 8);
}

/// 下溢 fail-fast：panic 且**不留脏账**（wrap 后的巨值会被后续 `try_take` 读走，
/// 等于静默钳位的反面教材）。
#[test]
fn give_back_underflow_panics_and_leaves_ledger_intact() {
    let budget = ByteBudget::new(64);
    assert!(budget.try_take(8));

    let caught = catch_unwind(AssertUnwindSafe(|| budget.give_back(9)));
    let payload = caught.expect_err("下溢必须 panic");
    let message = payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or_default();
    assert!(
        message.contains("give_back would go negative"),
        "panic 消息应指明记账 bug，实际：{message}"
    );

    assert_eq!(budget.held(), 8, "fail-fast 不得改动账目");
    budget.give_back(8);
    assert_eq!(budget.held(), 0);
}

/// 并发 churn：8 线程交错借还，采样线程断言 `held <= total` 恒成立；结束后
/// 账目精确归零（不漂移、不剩残额）。
#[test]
fn concurrent_churn_never_breaches_total_and_settles_to_zero() {
    const TOTAL: usize = 4096;
    const CHURN_THREADS: usize = 8;
    const ROUNDS: usize = 20_000;

    let budget = Arc::new(ByteBudget::new(TOTAL));
    let stop = Arc::new(AtomicBool::new(false));
    let breaches = Arc::new(AtomicUsize::new(0));
    let samples = Arc::new(AtomicUsize::new(0));

    thread::scope(|scope| {
        let sampler = {
            let budget = Arc::clone(&budget);
            let stop = Arc::clone(&stop);
            let breaches = Arc::clone(&breaches);
            let samples = Arc::clone(&samples);
            scope.spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    if budget.held() > TOTAL {
                        breaches.fetch_add(1, Ordering::Relaxed);
                    }
                    samples.fetch_add(1, Ordering::Relaxed);
                }
            })
        };

        let churn: Vec<_> = (0..CHURN_THREADS)
            .map(|worker| {
                let budget = Arc::clone(&budget);
                scope.spawn(move || {
                    let mut held = Vec::new();
                    for round in 0..ROUNDS {
                        let n = 1 + (worker * 7 + round) % 64;
                        if budget.try_take(n) {
                            held.push(n);
                        }
                        // 周期性全归还：账目必须能回零（借还对称的并发实证）。
                        if held.len() >= 8 {
                            for n in held.drain(..) {
                                budget.give_back(n);
                            }
                        }
                    }
                    for n in held.drain(..) {
                        budget.give_back(n);
                    }
                })
            })
            .collect();

        for handle in churn {
            handle.join().expect("churn 线程不得 panic");
        }
        stop.store(true, Ordering::Relaxed);
        sampler.join().expect("采样线程不得 panic");
    });

    assert!(samples.load(Ordering::Relaxed) > 0, "采样线程必须真的跑过");
    assert_eq!(
        breaches.load(Ordering::Relaxed),
        0,
        "任一采样点 held 都不得越过 total"
    );
    assert_eq!(budget.held(), 0, "全归还后账目必须精确归零");
}

/// 并发耗尽：容量确定（1024 / 7 = 146 件恰好），故成功次数必须**恰好** 146——
/// 多一件 = 假成功击穿硬上限，少一件 = 额度被浪费/饿死。
#[test]
fn concurrent_exhaustion_grants_exactly_the_capacity() {
    const TOTAL: usize = 1024;
    const PIECE: usize = 7;
    const THREADS: usize = 8;
    const ATTEMPTS: usize = 64;

    let budget = Arc::new(ByteBudget::new(TOTAL));
    let granted = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(THREADS));

    thread::scope(|scope| {
        for _ in 0..THREADS {
            let budget = Arc::clone(&budget);
            let granted = Arc::clone(&granted);
            let barrier = Arc::clone(&barrier);
            scope.spawn(move || {
                barrier.wait();
                let mut mine = Vec::new();
                for _ in 0..ATTEMPTS {
                    if budget.try_take(PIECE) {
                        mine.push(PIECE);
                        assert!(budget.held() <= TOTAL, "认领后 held 越过 total");
                    }
                }
                granted.fetch_add(mine.len(), Ordering::Relaxed);
                // 全部线程认领完成后才归还——容量不得跨相位复用，否则成功件数
                // 会随线程调度漂移，钉不住「恰好等于容量」。
                barrier.wait();
                for piece in mine.drain(..) {
                    budget.give_back(piece);
                }
            });
        }
    });

    assert_eq!(
        granted.load(Ordering::Relaxed),
        TOTAL / PIECE,
        "成功认领件数必须恰好等于容量"
    );
    assert_eq!(budget.held(), 0, "全归还后账目必须精确归零");
}

/// 账本对**对象大小没有隐藏上限**、也没有碎片浪费：只要剩余额度够就收下。
///
/// 1024B 预算下每件 200B：必须收满 5 件（1000B），第 6 件（1200B）才拒——
/// 若账本按片切份额（`1024 / 8 = 128B`）就会在第一件就拒（语义变化）。
#[test]
fn oversized_object_is_admitted_while_capacity_remains() {
    let budget = Arc::new(ByteBudget::new(1024));
    let pool = ObjectPool::with_shared_byte_quota(
        64,
        8,
        || Vec::<u8>::with_capacity(200),
        Arc::clone(&budget),
        |buf: &Vec<u8>| buf.capacity(),
    );

    for admitted in 1..=5 {
        assert!(
            pool.insert(Vec::with_capacity(200)).is_none(),
            "第 {admitted} 件（200B）应入池：账本只认总量、不设对象大小上限"
        );
        assert_eq!(budget.held(), admitted * 200);
    }
    assert!(
        pool.insert(Vec::with_capacity(200)).is_some(),
        "第 6 件越过 1024B 硬上限，必须拒收"
    );
    assert_eq!(budget.held(), 1000, "拒收不得改动账目");
    assert_eq!(budget.available(), 24);
}

/// 共享预算的额度**跨池可用**：前一个池认领的字节不锁死在后者的「片」里，
/// 第二个池照样能在剩余额度内准入（无按池/按片的额度浪费）。
#[test]
fn shared_quota_stays_usable_from_a_second_pool() {
    let budget = Arc::new(ByteBudget::new(1024));
    let make = |budget: &Arc<ByteBudget>| {
        ObjectPool::with_shared_byte_quota(
            64,
            8,
            || Vec::<u8>::with_capacity(200),
            Arc::clone(budget),
            |buf: &Vec<u8>| buf.capacity(),
        )
    };
    let first = make(&budget);
    let second = make(&budget);

    // 第一个池先认领：3 件 = 600B。
    for _ in 0..3 {
        assert!(first.insert(Vec::with_capacity(200)).is_none());
    }
    assert_eq!(budget.held(), 600);

    // 第二个池在剩余 424B 里仍能收下 2 件（400B）。
    for _ in 0..2 {
        assert!(
            second.insert(Vec::with_capacity(200)).is_none(),
            "共享预算的剩余额度必须对第二个池可用"
        );
    }
    assert_eq!(budget.held(), 1000);
    assert!(
        second.insert(Vec::with_capacity(200)).is_some(),
        "合计 1200B 越过硬上限，必须拒收"
    );
}

// ── 池析构的字节回卷（bd udf-frame-m34）────────────────────────────────────
//
// 池 drop 时 idle 槽里的对象被析构，但预算额度必须随之回卷：共享
// `Arc<ByteBudget>` 场景下否则那部分字节**永久不可用**（直到进程结束）。

/// 装配：`idle_cap = 4 ⇒ 4 片 × 1 槽`；权重按 `len()` 计（确定性 100B）。
fn budget_pool(budget: &Arc<ByteBudget>) -> ObjectPool<Vec<u8>> {
    ObjectPool::with_shared_byte_quota(8, 4, Vec::<u8>::new, Arc::clone(budget), |buf: &Vec<u8>| {
        buf.len()
    })
}

/// drop 一个池必须把它 idle 占住的字节还给共享预算。
#[test]
fn dropping_a_pool_returns_its_idle_bytes_to_the_shared_budget() {
    let budget = Arc::new(ByteBudget::new(1024));
    let first = budget_pool(&budget);
    let second = budget_pool(&budget);
    for _ in 0..2 {
        assert!(first.insert(vec![0u8; 100]).is_none());
        assert!(second.insert(vec![0u8; 100]).is_none());
    }
    assert_eq!(budget.held(), 400, "两池各 200B 入 idle");

    drop(first);
    assert_eq!(budget.held(), 200, "drop 的池必须回卷自己的 idle 字节");
    assert_eq!(budget.available(), 824);

    drop(second);
    assert_eq!(budget.held(), 0, "最后一个池 drop 后额度全数归还");
    assert_eq!(budget.available(), 1024);
}

/// 回卷出去的额度受**全局硬上限**约束：回收后别的池能用满，但不得超发。
#[test]
fn released_quota_is_still_bounded_by_the_global_cap() {
    let budget = Arc::new(ByteBudget::new(300));
    let holder = budget_pool(&budget);
    let newcomer = budget_pool(&budget);

    for _ in 0..3 {
        assert!(holder.insert(vec![0u8; 100]).is_none());
    }
    assert_eq!(budget.available(), 0, "300B 全被占满");
    assert!(
        newcomer.insert(vec![0u8; 100]).is_some(),
        "满额时必须拒收，不得超发"
    );

    drop(holder);
    assert_eq!(budget.available(), 300, "drop 后额度回到全局池");

    for admitted in 1..=3 {
        assert!(
            newcomer.insert(vec![0u8; 100]).is_none(),
            "第 {admitted} 件应可用回收额度入池"
        );
        assert_eq!(budget.held(), admitted * 100);
    }
    assert!(
        newcomer.insert(vec![0u8; 100]).is_some(),
        "再用到 400B 就越过 300B 硬上限，必须拒收"
    );
    assert_eq!(budget.held(), 300);
}

/// 回归：私有预算池与无预算池的析构行为不变（不掉别人的账、不 panic）。
#[test]
fn private_and_plain_pool_drop_leave_shared_budgets_untouched() {
    let shared = Arc::new(ByteBudget::new(1024));

    let private = ObjectPool::with_byte_quota(8, 4, Vec::<u8>::new, 512, |buf: &Vec<u8>| buf.len());
    for _ in 0..2 {
        assert!(private.insert(vec![0u8; 100]).is_none());
    }
    assert_eq!(shared.held(), 0, "私有预算池不碰共享账本");
    drop(private);
    assert_eq!(shared.held(), 0, "私有预算池 drop 不得改动共享账本");

    let plain = ObjectPool::new(8, 4, Vec::<u8>::new);
    for _ in 0..2 {
        assert!(plain.insert(vec![0u8; 100]).is_none());
    }
    drop(plain);
    assert_eq!(shared.held(), 0, "无预算池 drop 零副作用");
}

/// 并发：池析构与其他池的 `try_take` 交错时，采样 `held <= total` 恒成立，
/// 且全部池 drop 后账目精确归零。
#[test]
fn concurrent_drop_and_take_never_breach_the_cap() {
    const TOTAL: usize = 1024;
    const PIECES: usize = 4;
    const PER_POOL: usize = 2;

    let budget = Arc::new(ByteBudget::new(TOTAL));
    let mut pools: Vec<ObjectPool<Vec<u8>>> = (0..PIECES).map(|_| budget_pool(&budget)).collect();
    for pool in &pools {
        for _ in 0..PER_POOL {
            assert!(pool.insert(vec![0u8; 100]).is_none());
        }
    }
    assert_eq!(budget.held(), PIECES * PER_POOL * 100);

    let stop = Arc::new(AtomicBool::new(false));
    let breaches = Arc::new(AtomicUsize::new(0));
    let samples = Arc::new(AtomicUsize::new(0));
    let churn_claims = Arc::new(AtomicUsize::new(0));

    thread::scope(|scope| {
        let sampler = {
            let budget = Arc::clone(&budget);
            let stop = Arc::clone(&stop);
            let breaches = Arc::clone(&breaches);
            let samples = Arc::clone(&samples);
            scope.spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    if budget.held() > TOTAL {
                        breaches.fetch_add(1, Ordering::Relaxed);
                    }
                    // 采样计数：没有它，「未越界」可能只是「一次都没采」的空转。
                    samples.fetch_add(1, Ordering::Relaxed);
                }
            })
        };
        let churn = {
            let budget = Arc::clone(&budget);
            let stop = Arc::clone(&stop);
            let churn_claims = Arc::clone(&churn_claims);
            scope.spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    if budget.try_take(50) {
                        churn_claims.fetch_add(1, Ordering::Relaxed);
                        budget.give_back(50);
                    }
                }
            })
        };
        // ① 采样线程必须真的先跑起来：否则「未越界」可能一次都没采（空转假绿）。
        let started = Instant::now();
        while samples.load(Ordering::Relaxed) == 0 {
            assert!(
                started.elapsed() < Duration::from_secs(5),
                "采样线程未能启动"
            );
            thread::yield_now();
        }
        let before_drop = samples.load(Ordering::Relaxed);

        // ② 两个池在与 churn 交错的另一个线程里析构。
        let doomed: Vec<ObjectPool<Vec<u8>>> = pools.drain(..2).collect();
        scope
            .spawn(move || drop(doomed))
            .join()
            .expect("析构线程不得 panic");

        // ③ 让并发窗口覆盖「析构刚完成」这一段（churn 仍在借还、采样仍在采样）。
        let tail = Instant::now();
        while tail.elapsed() < Duration::from_millis(20) {
            thread::yield_now();
        }
        let after_drop = samples.load(Ordering::Relaxed);

        stop.store(true, Ordering::Relaxed);
        churn.join().expect("churn 线程不得 panic");
        sampler.join().expect("采样线程不得 panic");
        assert!(
            after_drop > before_drop,
            "采样必须跨过析构窗口继续发生（{before_drop} → {after_drop}）"
        );
    });

    drop(pools);
    assert!(
        samples.load(Ordering::Relaxed) > 0,
        "采样线程必须真的采到过样本（否则「未越界」是空转假绿）"
    );
    assert_eq!(
        breaches.load(Ordering::Relaxed),
        0,
        "任一采样点都不得越过 total"
    );
    assert!(
        churn_claims.load(Ordering::Relaxed) > 0,
        "churn 线程必须真的认领过（否则测试空转）"
    );
    assert_eq!(budget.held(), 0, "全部池 drop 后账目必须精确归零");
    assert_eq!(budget.available(), TOTAL);
}

/// 片锁中毒后析构：**不得 panic**，且中毒片里剩余的 idle 字节照样回卷。
///
/// 中毒怎么来（本 crate 自己的 fail-fast 造出来的）：片锁内跑用户 `weight` 钩子，
/// 钩子 panic ⇒ 守卫 unwind ⇒ 该片锁中毒。此后任何 `lock()` 都会 panic；析构期
/// 若沿用 `lock()`，则 ①`Drop` 中途 panic（若本就在 unwind 中 ⇒ **进程 abort**），
/// ②本次要修的那个「额度永久不可用」在异常路径上复现。
#[test]
fn drop_survives_a_poisoned_shard_and_returns_its_idle_bytes() {
    let budget = Arc::new(ByteBudget::new(1024));
    let armed = Arc::new(AtomicBool::new(false));
    let pool = {
        let armed = Arc::clone(&armed);
        ObjectPool::with_shared_byte_quota(
            16,
            16,
            || vec![0u8; 100],
            Arc::clone(&budget),
            move |buf: &Vec<u8>| {
                if armed.load(Ordering::Relaxed) {
                    panic!("weight hook boom");
                }
                buf.len()
            },
        )
    };

    // 两件都落在 main 的 home 片（idle_cap = 16 ⇒ 8 片 × 每片 2 槽）。
    assert!(pool.insert(vec![0u8; 100]).is_none());
    assert!(pool.insert(vec![0u8; 100]).is_none());
    assert_eq!(budget.held(), 200);

    // 让钩子在**持片锁时**panic：`prune_idle` 先移出一件再计量 ⇒ 该片锁中毒，
    // 且片里还剩 1 件（100B）待回卷。
    armed.store(true, Ordering::Relaxed);
    let poisoned = catch_unwind(AssertUnwindSafe(|| {
        pool.prune_idle_with_clock(Duration::ZERO, &SystemClock);
    }));
    assert!(
        poisoned.is_err(),
        "钩子 panic 必须照常传播（禁 catch_unwind 吞掉）"
    );
    assert_eq!(
        budget.held(),
        200,
        "panic 路径上已移出的那件不释放（既有行为，本单不动）"
    );

    // 中毒后析构：只处理 PoisonError（取回槽位），其余 panic 照常传播。
    armed.store(false, Ordering::Relaxed); // 场景是「锁中毒」，不是「钩子永远炸」
    let dropped = catch_unwind(AssertUnwindSafe(move || drop(pool)));
    assert!(dropped.is_ok(), "片锁中毒时 Drop 不得 panic");
    assert_eq!(
        budget.held(),
        100,
        "中毒片里剩余的 idle 字节必须回卷（取锁失败也要计入）"
    );
    assert_eq!(budget.available(), 924);
}
