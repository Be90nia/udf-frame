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
};

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
#[test]
fn huge_request_is_refused_without_wrapping() {
    let budget = ByteBudget::new(1024);
    assert!(!budget.try_take(usize::MAX));
    assert!(!budget.try_take(1025));
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
