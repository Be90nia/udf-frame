#![cfg(all(feature = "memgov", feature = "pool"))]
//! PooledThreadPool 组合件端到端：互喂时序（借还发生在任务闭包内，RAII 覆盖
//! panic unwind）+ 借空 Discard 等价语义（计数留痕）+ resize 缩容溢出 + 提交侧
//! 包装开销锚（counting allocator：与手写借还同款恰一次 Box，零额外分配）。
//!
//! 分工：双池本体行为已在 `tests/memory_pool.rs` / `tests/thread_pool.rs` /
//! `tests/pool_base.rs` 覆盖——本文件只测**组合层**（借还时序/借用超时/包装
//! 开销），七维性能矩阵不在此重复（见 examples/thread_pool_multidim.rs 等既有
//! 采集器与 reports/2026-10-16-pool-wave25.md）。

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Barrier, Mutex, MutexGuard};
use std::thread;
use std::time::Duration;

use skeleton_infra::memgov::{CountingAllocator, MemoryPool, MemoryTuning};
use skeleton_infra::pool::{ThreadPool, ThreadPoolTuning};
use skeleton_infra::pooled_thread_pool::PooledThreadPool;

/// 计数分配器挂全局（本二进制专用；同 tests/memory_pool.rs 纪律——lib 不挂）。
#[global_allocator]
static COUNTING: CountingAllocator = CountingAllocator;

/// 分配读数窗口隔离（同 tests/memory_pool.rs：libtest 打印分配窗与读数窗物理
/// 重叠——静止检测收敛真实排空时刻，不赌固定时长）。
static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    match SERIAL.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    }
}

fn settle() {
    let mut last = CountingAllocator::live_allocs();
    for _ in 0..500 {
        thread::sleep(Duration::from_millis(1));
        let now = CountingAllocator::live_allocs();
        if now == last {
            return;
        }
        last = now;
    }
}

/// 单 worker 组合件（FIFO 顺序断言用：sentinel 任务必在先行任务完全落账后跑）。
fn single<F>(mem: Arc<MemoryPool<Vec<u8>, F>>) -> PooledThreadPool<Vec<u8>, F>
where
    F: Fn() -> Vec<u8> + Send + Sync + 'static,
{
    PooledThreadPool::new(
        mem,
        ThreadPool::new(ThreadPoolTuning::new(1, 1, 16)),
        Duration::from_millis(100),
    )
}

// ── 正常借还循环：任务闭包内借出→跑 f→RAII 归还，同一件对象复用 ──────────

#[test]
fn submit_borrows_runs_and_returns_buffer() {
    let _g = serial();
    let created = Arc::new(AtomicUsize::new(0));
    let created2 = Arc::clone(&created);
    let mem = Arc::new(MemoryPool::new(2, 2, move || {
        created2.fetch_add(1, Ordering::SeqCst);
        Vec::<u8>::new()
    }));
    let ptp = single(mem);

    ptp.submit(|buf: &mut Vec<u8>| buf.push(7)).unwrap();
    let (tx, rx) = mpsc::channel();
    ptp.submit(move |_| tx.send(()).unwrap()).unwrap();
    rx.recv_timeout(Duration::from_secs(5)).unwrap();

    // sentinel 只证明任务2已跑到 send，不证明其包装体结束（任务2 自身的
    // guard 仍在借）——归还记账以 worker 全停为界，join 后零在借=确定口径，
    // 跨平台调度差异免疫（Linux 快调度曾致断言抢跑 left:1）。
    ptp.threads().shutdown();
    ptp.threads().join();

    let m = ptp.mem_pool().metrics();
    assert_eq!(m.core.borrowed, 0, "任务结束缓冲必已归还");
    assert_eq!(m.core.idle, 1, "归还件留存闲槽");
    assert_eq!(created.load(Ordering::SeqCst), 1, "工厂只调一次");

    // 同一件对象复用（状态随件保留）——组合层不破坏 MemoryPool 语义。
    let g = ptp.mem_pool().acquire(Duration::from_millis(100)).unwrap();
    assert_eq!(&*g, &[7]);
    drop(g);
    ptp.threads().shutdown();
    ptp.threads().join();
}

// ── panic 路径：unwind 中 guard Drop 自动归还，池可继续用 ────────────────

#[test]
fn task_panic_returns_buffer_and_pool_stays_usable() {
    let _g = serial();
    let mem = Arc::new(MemoryPool::<Vec<u8>>::new(2, 2, Vec::new));
    let ptp = single(mem);

    ptp.submit(|_: &mut Vec<u8>| panic!("boom in pooled task"))
        .unwrap();
    let (tx, rx) = mpsc::channel();
    ptp.submit(move |_| tx.send(()).unwrap()).unwrap();
    rx.recv_timeout(Duration::from_secs(5)).unwrap();

    // 池可继续用：后续提交正常借还。（shutdown 是单向闸，活性验证必须在其前）
    let hit = Arc::new(AtomicUsize::new(0));
    let hit2 = Arc::clone(&hit);
    ptp.submit(move |_| {
        hit2.fetch_add(1, Ordering::SeqCst);
    })
    .unwrap();
    let (tx, rx) = mpsc::channel();
    ptp.submit(move |_| tx.send(()).unwrap()).unwrap();
    rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(hit.load(Ordering::SeqCst), 1);

    // 归还记账断言：全部任务结束 + worker 全停后（join 后零在借=确定口径；
    // sentinel 只证明任务跑到 send，不证包装体结束——Linux 快调度曾致断言
    // 抢跑 borrowed=1，那是哨兵任务自己的缓冲）。
    ptp.threads().shutdown();
    ptp.threads().join();
    let m = ptp.mem_pool().metrics();
    assert_eq!(m.core.borrowed, 0, "panic unwind 中 guard Drop 必归还缓冲");
    assert_eq!(m.core.idle, 1, "归还件照常留存");
    assert_eq!(
        ptp.threads().metrics().task_panics,
        1,
        "panic 另册留痕（fail-visible）"
    );
    assert_eq!(ptp.borrow_rejected(), 0);
}

// ── 并发提交：N 任务全归还，dropped_overflow 零 ──────────────────────────

#[test]
fn concurrent_submits_all_return_with_zero_overflow() {
    let _g = serial();
    let mem = Arc::new(MemoryPool::<Vec<u8>>::new(16, 16, Vec::new));
    let ptp = PooledThreadPool::new(
        mem,
        ThreadPool::new(ThreadPoolTuning::new(4, 4, 64)),
        Duration::from_millis(100),
    );

    let (tx, rx) = mpsc::channel();
    const N: usize = 64;
    for i in 0..N {
        let tx = tx.clone();
        ptp.submit(move |buf: &mut Vec<u8>| {
            buf.push(i as u8);
            tx.send(()).unwrap();
        })
        .unwrap();
    }
    drop(tx);
    for _ in 0..N {
        rx.recv_timeout(Duration::from_secs(5)).unwrap();
    }
    // completed/guard-Drop 落账在任务体返回之后——recv 只证明任务体跑完，
    // 指标断言等确定性排空（shutdown+join）后读终态。
    ptp.threads().shutdown();
    ptp.threads().join();

    let m = ptp.mem_pool().metrics();
    assert_eq!(m.core.borrowed, 0, "N 任务全归还");
    assert_eq!(m.core.dropped_overflow, 0, "池深≥并发，归还零溢出丢弃");
    assert_eq!(ptp.borrow_rejected(), 0, "池深≥并发，零借空");
    assert!(m.core.idle <= 16 && m.core.idle >= 1);
    assert_eq!(ptp.threads().metrics().completed, N as u64);
}

// ── 借空语义：Discard 等价（任务体丢弃 + borrow_rejected 计数留痕）────────

#[test]
fn borrow_exhaustion_discards_task_with_counted_rejection() {
    let _g = serial();
    let mem = Arc::new(MemoryPool::<Vec<u8>>::new(1, 1, Vec::new));
    let ptp = PooledThreadPool::new(
        mem,
        ThreadPool::new(ThreadPoolTuning::new(1, 1, 16)),
        Duration::from_millis(30),
    );

    // 测试线程持走唯一缓冲，worker 借空。
    let _holder = ptp.mem_pool().acquire(Duration::from_millis(100)).unwrap();

    let ran = Arc::new(AtomicUsize::new(0));
    let ran2 = Arc::clone(&ran);
    ptp.submit(move |_| {
        ran2.fetch_add(1, Ordering::SeqCst);
    })
    .unwrap();
    // sentinel 走裸 execute（不借缓冲）：1 worker FIFO 保证它在借空任务完全
    // 落账后跑。若走 submit 会同样借空超时被弃——正好反向钉死语义。
    let (tx, rx) = mpsc::channel();
    ptp.threads().execute(move || tx.send(()).unwrap()).unwrap();
    rx.recv_timeout(Duration::from_secs(5)).unwrap();

    assert_eq!(ran.load(Ordering::SeqCst), 0, "借空任务体不得执行");
    // completed 落账在任务体返回之后——指标断言等确定性排空后读终态。
    drop(_holder);
    ptp.threads().shutdown();
    ptp.threads().join();

    assert_eq!(ptp.borrow_rejected(), 1, "借空=Discard 等价，计数留痕");
    let tm = ptp.threads().metrics();
    assert_eq!(tm.rejected, 0, "队列未满——与提交拒绝是两个计数，不得混账");
    assert_eq!(tm.task_panics, 0);
    assert_eq!(
        tm.completed, 2,
        "借空任务按正常完成落账（丢弃发生在任务体内）"
    );
}

// ── resize 缩容后提交：归还超新闲槽上限=溢出丢弃计数 ─────────────────────

#[test]
fn resize_shrink_overflow_semantics() {
    let _g = serial();
    let mem = Arc::new(MemoryPool::<Vec<u8>>::new(2, 2, Vec::new));
    let ptp = PooledThreadPool::new(
        mem,
        ThreadPool::new(ThreadPoolTuning::new(2, 2, 16)),
        Duration::from_millis(100),
    );
    // 缩容：闲槽上限 2→1。
    ptp.mem_pool()
        .resize(MemoryTuning {
            max: 2,
            idle_cap: 1,
        })
        .unwrap();

    // 两任务并发各持一件（barrier 同步双持），释放后先后归还：
    // 第一件留存（idle 0→1），第二件超 idle_cap=1 溢出丢弃。
    let barrier = Arc::new(Barrier::new(2));
    let (tx, rx) = mpsc::channel();
    for i in 0..2 {
        let (barrier, tx) = (Arc::clone(&barrier), tx.clone());
        ptp.submit(move |buf: &mut Vec<u8>| {
            buf.push(i as u8);
            barrier.wait();
            tx.send(()).unwrap();
        })
        .unwrap();
    }
    drop(tx);
    for _ in 0..2 {
        rx.recv_timeout(Duration::from_secs(5)).unwrap();
    }

    // 池可继续用（先跑完使用面，指标终态断言统一放 join 后）。
    let (tx, rx) = mpsc::channel();
    ptp.submit(move |buf: &mut Vec<u8>| {
        buf.push(9);
        tx.send(()).unwrap();
    })
    .unwrap();
    rx.recv_timeout(Duration::from_secs(5)).unwrap();

    ptp.threads().shutdown();
    ptp.threads().join();

    let m = ptp.mem_pool().metrics();
    assert_eq!(m.core.borrowed, 0);
    assert_eq!(m.core.idle, 1, "缩容后闲槽上限=1，只留一件");
    assert_eq!(m.core.dropped_overflow, 1, "超限件溢出丢弃计数=1");
    assert_eq!(ptp.borrow_rejected(), 0);
    assert_eq!(ptp.threads().metrics().completed, 3);
}

// ── 包装开销锚：提交侧恰一次 Box（同手写），零额外分配 + ns 级 delta ──────

/// 单轮测量：全新池同构对比。返回 None=分配读数窗被污染，调用方换全新池重试。
/// 硬判据：blocker 占住唯一 worker（FIFO 首位，窗口内零任务完成）时 live
/// delta=恰 N 次 Box；排空后回基线=零滞留。
/// 窗口纯化两件：①预热——idle Vec 容量先行长成（首次归还 0→1 的 buffer 滞留
/// 属池内部状态，须落在窗前）；②blocker 用 Mutex/Condvar 闸门（零分配；mpsc
/// 的 Packet 会在窗内释放打脏读数）。
fn measure_once(n: u64, borrow_to: Duration) -> Option<(f64, f64)> {
    let hand_mem = Arc::new(MemoryPool::<Vec<u8>>::new(16, 16, Vec::new));
    let hand = ThreadPool::new(ThreadPoolTuning::new(1, 1, 20_000));
    let wrapped = PooledThreadPool::new(
        Arc::new(MemoryPool::<Vec<u8>>::new(16, 16, Vec::new)),
        ThreadPool::new(ThreadPoolTuning::new(1, 1, 20_000)),
        borrow_to,
    );

    // 预热：两池各过 16 件（idle Vec 容量长满、created 到顶），排空后进窗。
    for _ in 0..16u64 {
        let mem = Arc::clone(&hand_mem);
        hand.execute(move || {
            if let Ok(g) = mem.acquire(borrow_to) {
                drop(g);
            }
        })
        .unwrap();
        wrapped.submit(|_| {}).unwrap();
    }
    settle();

    let gate = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    // 锁中毒=此前持锁方 panic——测试语境恢复续走（同 tests/memory_pool.rs
    // serial() 纪律；crate 零依赖，parking_lot 不入 manifest）。
    fn lock_gate(m: &std::sync::Mutex<bool>) -> std::sync::MutexGuard<'_, bool> {
        match m.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        }
    }
    let open = |gate: &Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>| {
        let (m, cv) = &**gate;
        *lock_gate(m) = true;
        cv.notify_all();
    };
    let block = |gate: &Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>| {
        let (m, cv) = &**gate;
        let mut g = lock_gate(m);
        while !*g {
            g = cv.wait(g).unwrap_or_else(|p| p.into_inner());
        }
    };

    // ── 手写基线 ──
    let g2 = Arc::clone(&gate);
    hand.execute(move || block(&g2)).unwrap();
    settle();
    let before = CountingAllocator::live_allocs();
    let t0 = std::time::Instant::now();
    for _ in 0..n {
        let mem = Arc::clone(&hand_mem);
        hand.execute(move || {
            if let Ok(g) = mem.acquire(borrow_to) {
                drop(g);
            }
        })
        .unwrap();
    }
    let hand_submit = t0.elapsed();
    let hand_delta = CountingAllocator::live_allocs() as i64 - before as i64;
    open(&gate);
    settle();
    // 排空会计：N 个任务 Box 窗内+N/窗内释放 −N 净 0；blocker Box 窗前分配、
    // 窗内释放 → 恰 −1（多一分少一分都是窗被污染）。
    let hand_drained = CountingAllocator::live_allocs() as i64 - before as i64;
    hand.shutdown();
    hand.join();
    if hand_delta != n as i64 || hand_drained != -1 || hand.metrics().completed != n + 1 + 16 {
        eprintln!("attempt dirty: hand_delta={hand_delta} hand_drained={hand_drained}");
        return None;
    }
    let hand_ns = hand_submit.as_nanos() as f64 / n as f64;

    // ── 包装 ──
    let g2 = Arc::clone(&gate);
    *lock_gate(&gate.0) = false;
    wrapped.threads().execute(move || block(&g2)).unwrap();
    settle();
    let before = CountingAllocator::live_allocs();
    let t0 = std::time::Instant::now();
    for _ in 0..n {
        wrapped.submit(|_| {}).unwrap();
    }
    let wrapped_submit = t0.elapsed();
    let wrapped_delta = CountingAllocator::live_allocs() as i64 - before as i64;
    open(&gate);
    settle();
    // 同手写侧会计：blocker Box 窗前分配窗内释放 → 恰 −1。
    let wrapped_drained = CountingAllocator::live_allocs() as i64 - before as i64;
    wrapped.threads().shutdown();
    wrapped.threads().join();
    if wrapped_delta != n as i64
        || wrapped_drained != -1
        || wrapped.threads().metrics().completed != n + 1 + 16
        || wrapped.borrow_rejected() != 0
    {
        eprintln!("attempt dirty: wrapped_delta={wrapped_delta} wrapped_drained={wrapped_drained}");
        return None;
    }
    let wrapped_ns = wrapped_submit.as_nanos() as f64 / n as f64;
    Some((hand_ns, wrapped_ns))
}

#[test]
fn wrapper_overhead_matches_hand_written_allocation() {
    let _g = serial();

    const N: u64 = 5_000;
    let borrow_to = Duration::from_millis(100);

    let mut outcome = None;
    for attempt in 0..8 {
        if let Some(r) = measure_once(N, borrow_to) {
            println!("clean allocation window at attempt {attempt}");
            outcome = Some(r);
            break;
        }
    }
    let (hand_ns, wrapped_ns) =
        outcome.expect("分配读数窗连续 8 轮被污染——环境噪音异常，禁带脏数据放行");

    // 分配锚已在 measure_once 内硬断言（两路径各恰 N 次 Box + 零滞留）。
    // 开销锚（宽松上限防 flaky；ns 级实测数字打印进报告，release 复跑取数）。
    assert!(
        wrapped_ns <= hand_ns + 2_000.0,
        "包装开销须 ns 级：wrapped {wrapped_ns:.0} ns/op vs hand {hand_ns:.0} ns/op"
    );
    println!("handwritten: {hand_ns:.0} ns/op submit-side");
    println!("wrapped: {wrapped_ns:.0} ns/op submit-side");
    println!("wrapper overhead delta: {:.0} ns/op", wrapped_ns - hand_ns);
}
