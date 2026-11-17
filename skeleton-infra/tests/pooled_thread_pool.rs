#![cfg(all(feature = "memgov", feature = "pool"))]
//! PooledThreadPool 组合件端到端：互喂时序（借还发生在任务闭包内，RAII 覆盖
//! panic unwind）+ 借空降级直构（永不丢任务，`degraded`/`constructed` 留痕）+
//! 提交侧 CallerRuns 档（池饱和当代跑）+ resize 缩容溢出 + 提交侧包装开销锚
//! （counting allocator：与手写借还同款恰一次 Box，零额外分配）。
//!
//! 分工：池本体行为已在 `tests/object_pool.rs` / `tests/thread_pool.rs` /
//! `tests/pool_base.rs` 覆盖——本文件只测**组合层**（借还时序/借空降级/
//! CallerRuns 档/包装开销），七维性能矩阵不在此重复（见
//! `examples/pooled_thread_pool_multidim.rs` 采集器）。

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Condvar, Mutex, MutexGuard, mpsc};
use std::thread;
use std::time::Duration;

use skeleton_infra::memgov::CountingAllocator;
use skeleton_infra::objpool::{ObjectPool, ObjectPoolTuning};
use skeleton_infra::pool::{ThreadPool, ThreadPoolTuning};
use skeleton_infra::pooled_thread_pool::PooledThreadPool;

/// 计数分配器挂全局（本二进制专用；同 tests/memory_pool.rs 纪律——lib 不挂）。
#[global_allocator]
static COUNTING: CountingAllocator = CountingAllocator;

/// 分配读数窗口隔离（同 tests/memory_pool.rs：libtest 打印分配窗与读数窗物理
/// 重叠——静止检测收敛真实排空时刻，不赌固定时长）。
static SERIAL: Mutex<()> = Mutex::new(());

/// 锁中毒=此前持锁方 panic——测试语境恢复续走（同 tests/memory_pool.rs 纪律；
/// crate 零依赖，parking_lot 不入 manifest）。
fn lock_or_recover<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    match m.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    }
}

fn serial() -> MutexGuard<'static, ()> {
    lock_or_recover(&SERIAL)
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

/// 闸门（零分配）：`open` 放闸、`block` 等到放闸——占住 worker 而不引入 mpsc
/// Packet 的窗内释放噪声（包装开销锚用），也用于 CallerRuns 档占满 worker。
type Gate = Arc<(Mutex<bool>, Condvar)>;

fn open(gate: &Gate) {
    let (m, cv) = &**gate;
    *lock_or_recover(m) = true;
    cv.notify_all();
}

fn block(gate: &Gate) {
    let (m, cv) = &**gate;
    let mut opened = lock_or_recover(m);
    while !*opened {
        opened = match cv.wait(opened) {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
    }
}

/// 单 worker 组合件（FIFO 顺序断言用：sentinel 任务必在先行任务完全落账后跑）。
fn single(buffers: Arc<ObjectPool<Vec<u8>>>) -> PooledThreadPool<Vec<u8>> {
    PooledThreadPool::new(buffers, ThreadPool::new(ThreadPoolTuning::new(1, 1, 16)))
}

// ── 正常借还循环：任务闭包内借出→跑 f→RAII 归还，同一件对象复用 ──────────

#[test]
fn submit_borrows_runs_and_returns_buffer() {
    let _g = serial();
    let created = Arc::new(AtomicUsize::new(0));
    let created2 = Arc::clone(&created);
    let buffers = Arc::new(ObjectPool::new(2, 2, move || {
        created2.fetch_add(1, Ordering::SeqCst);
        Vec::<u8>::new()
    }));
    let ptp = single(buffers);

    ptp.submit(|buf: &mut Vec<u8>| buf.push(7)).unwrap();
    let (tx, rx) = mpsc::channel();
    ptp.submit(move |_| tx.send(()).unwrap()).unwrap();
    rx.recv_timeout(Duration::from_secs(5)).unwrap();

    // sentinel 只证明任务2已跑到 send，不证明其包装体结束（任务2 自身的
    // 句柄仍在借）——归还记账以 worker 全停为界，join 后零在借=确定口径，
    // 跨平台调度差异免疫（Linux 快调度曾致断言抢跑 left:1）。
    ptp.threads().shutdown();
    ptp.threads().join();

    let m = ptp.buffer_pool().metrics();
    assert_eq!(m.core.borrowed, 0, "任务结束缓冲必已归还");
    assert_eq!(m.core.idle, 1, "归还件留存闲槽");
    assert_eq!(created.load(Ordering::SeqCst), 1, "工厂只调一次");

    // 同一件对象复用（状态随件保留）——组合层不破坏 ObjectPool 语义。
    let g = ptp.buffer_pool().acquire();
    assert_eq!(&*g, &[7]);
    drop(g);
    ptp.threads().shutdown();
    ptp.threads().join();
}

// ── panic 路径：unwind 中句柄 Drop 回卷宇宙记账，池可继续用 ──────────────

#[test]
fn task_panic_returns_buffer_and_pool_stays_usable() {
    let _g = serial();
    let buffers = Arc::new(ObjectPool::<Vec<u8>>::new(2, 2, Vec::new));
    let ptp = single(Arc::clone(&buffers));

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
    let m = ptp.buffer_pool().metrics();
    assert_eq!(
        m.core.borrowed, 0,
        "panic unwind 中句柄 Drop 必回卷宇宙记账"
    );
    assert_eq!(
        m.panic_dropped, 1,
        "unwind 归还走销毁路径（r2d2 #31 防御：panic 中对象不变量可能已破，禁回池）"
    );
    assert_eq!(
        m.core.idle, 1,
        "销毁件不回池；后续任务新建件照常留存（池可继续用）"
    );
    assert_eq!(m.degraded, 0);
    assert_eq!(
        ptp.threads().metrics().task_panics,
        1,
        "panic 另册留痕（fail-visible）"
    );
}

// ── 并发提交：N 任务全归还，dropped_overflow 零 ──────────────────────────

#[test]
fn concurrent_submits_all_return_with_zero_overflow() {
    let _g = serial();
    let buffers = Arc::new(ObjectPool::<Vec<u8>>::new(16, 16, Vec::new));
    let ptp = PooledThreadPool::new(
        Arc::clone(&buffers),
        ThreadPool::new(ThreadPoolTuning::new(4, 4, 64)),
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
    // completed/句柄-Drop 落账在任务体返回之后——recv 只证明任务体跑完，
    // 指标断言等确定性排空（shutdown+join）后读终态。
    ptp.threads().shutdown();
    ptp.threads().join();

    let m = buffers.metrics();
    assert_eq!(m.core.borrowed, 0, "N 任务全归还");
    assert_eq!(m.core.dropped_overflow, 0, "池深≥并发，归还零溢出丢弃");
    assert_eq!(m.degraded, 0, "池深≥并发，零借空降级");
    assert!(m.core.idle <= 16 && m.core.idle >= 1);
    assert_eq!(ptp.threads().metrics().completed, N as u64);
}

// ── 借空不丢任务：宇宙满=降级直构（任务照跑，degraded/constructed 留痕）───

#[test]
fn borrow_exhaustion_degrades_and_task_runs() {
    let _g = serial();
    let buffers = Arc::new(ObjectPool::<Vec<u8>>::new(1, 1, Vec::new));
    let ptp = single(Arc::clone(&buffers));

    // 测试线程持走唯一缓冲（宇宙满）：worker 借空必须降级直构，不得丢任务。
    let holder = ptp.buffer_pool().acquire();

    let ran = Arc::new(AtomicUsize::new(0));
    let ran2 = Arc::clone(&ran);
    ptp.submit(move |_: &mut Vec<u8>| {
        ran2.fetch_add(1, Ordering::SeqCst);
    })
    .unwrap();
    // sentinel 走裸 execute（不借缓冲）：1 worker FIFO 保证它在任务完全落账后跑。
    let (tx, rx) = mpsc::channel();
    ptp.threads().execute(move || tx.send(()).unwrap()).unwrap();
    rx.recv_timeout(Duration::from_secs(5)).unwrap();

    assert_eq!(ran.load(Ordering::SeqCst), 1, "借空降级：任务体必须执行");
    // completed 落账在任务体返回之后——指标断言等确定性排空后读终态。
    drop(holder);
    ptp.threads().shutdown();
    ptp.threads().join();

    let m = buffers.metrics();
    assert_eq!(m.degraded, 1, "宇宙满借空=降级直构，计数留痕");
    assert_eq!(m.constructed, 2, "holder 一件 + 降级一件（工厂调用留痕）");
    assert_eq!(m.core.borrowed, 0);
    assert_eq!(m.core.idle, 1, "holder 的件归还入闲槽；降级件归还即释放");
    assert_eq!(m.core.dropped_overflow, 0);
    let tm = ptp.threads().metrics();
    assert_eq!(tm.rejected, 0, "队列未满——与提交拒绝是两个计数，不得混账");
    assert_eq!(tm.task_panics, 0);
    assert_eq!(tm.completed, 2, "降级任务与 sentinel 都正常完成");
}

// ── 饱和场景：缓冲池容量 << 并发任务数，全部任务仍执行完成（零丢弃）────────

#[test]
fn saturation_keeps_every_task_alive() {
    let _g = serial();
    let buffers = Arc::new(ObjectPool::<Vec<u8>>::new(2, 2, Vec::new));
    let ptp = PooledThreadPool::new(
        Arc::clone(&buffers),
        ThreadPool::new(ThreadPoolTuning::new(8, 8, 64)),
    );

    // 容量 2 vs 每波 8 件并发持有：barrier 钉住「同一时刻 8 件都在借」，宇宙只
    // 容 2 ⇒ 每波至少 6 件走降级直构（旧语义在这里丢件，barrier 永不 trip）。
    const N: usize = 16;
    let barrier = Arc::new(Barrier::new(8));
    let ran = Arc::new(AtomicUsize::new(0));
    let (tx, rx) = mpsc::channel();
    for _ in 0..N {
        let (barrier, ran, tx) = (Arc::clone(&barrier), Arc::clone(&ran), tx.clone());
        ptp.submit(move |buf: &mut Vec<u8>| {
            buf.push(1);
            barrier.wait();
            ran.fetch_add(1, Ordering::SeqCst);
            tx.send(()).unwrap();
        })
        .unwrap();
    }
    drop(tx);
    for _ in 0..N {
        rx.recv_timeout(Duration::from_secs(5))
            .expect("饱和场景下任务不得被丢弃");
    }
    ptp.threads().shutdown();
    ptp.threads().join();

    assert_eq!(ran.load(Ordering::SeqCst), N, "饱和下零丢弃");
    assert_eq!(ptp.threads().metrics().completed, N as u64);
    let m = buffers.metrics();
    assert!(
        m.degraded >= 12,
        "两波各 ≥6 件降级直构，实际 degraded={}",
        m.degraded
    );
    assert_eq!(
        m.constructed,
        m.degraded + 2,
        "宇宙 2 件 + 降级件（工厂调用留痕）"
    );
    assert_eq!(m.core.borrowed, 0);
    assert_eq!(m.core.idle, 2, "宇宙两件全数留存");
    assert_eq!(m.core.dropped_overflow, 0, "降级件归还即释放，不占闲槽");
    println!(
        "饱和 e2e: {N} 任务全完成（零丢弃），degraded={} constructed={} idle={}",
        m.degraded, m.constructed, m.core.idle
    );
}

// ── CallerRuns 档：池饱和时提交线程代跑（不丢、当代跑）──────────────────

#[test]
fn caller_runs_policy_runs_task_on_submitting_thread() {
    let _g = serial();
    let buffers = Arc::new(ObjectPool::<Vec<u8>>::new(1, 1, Vec::new));
    // core=max=1、queue_cap=1：worker 占住 + 队列 1 件之后，第三次提交必走拒绝策略。
    let ptp =
        PooledThreadPool::with_caller_runs(Arc::clone(&buffers), ThreadPoolTuning::new(1, 1, 1));

    let gate: Gate = Arc::new((Mutex::new(false), Condvar::new()));
    let ran = Arc::new(AtomicUsize::new(0));
    let (started_tx, started_rx) = mpsc::channel();
    let (g, r) = (Arc::clone(&gate), Arc::clone(&ran));
    ptp.submit(move |_: &mut Vec<u8>| {
        started_tx.send(()).unwrap();
        r.fetch_add(1, Ordering::SeqCst);
        block(&g);
    })
    .unwrap();
    started_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("worker 必须先认领 blocker 并占住唯一缓冲");

    // 队列位占满（cap=1）：下一次提交触发拒绝策略 → CallerRuns 在提交线程内联。
    let (queued_tx, queued_rx) = mpsc::channel();
    let r = Arc::clone(&ran);
    ptp.submit(move |_: &mut Vec<u8>| {
        r.fetch_add(1, Ordering::SeqCst);
        queued_tx.send(()).unwrap();
    })
    .unwrap();
    let main_thread = thread::current().id();
    let inline_on = Arc::new(Mutex::new(None));
    let (seen, r) = (Arc::clone(&inline_on), Arc::clone(&ran));
    ptp.submit(move |_: &mut Vec<u8>| {
        r.fetch_add(1, Ordering::SeqCst);
        *lock_or_recover(&seen) = Some(thread::current().id());
    })
    .unwrap();
    assert_eq!(
        *lock_or_recover(&inline_on),
        Some(main_thread),
        "CallerRuns：被拒任务由提交线程代跑"
    );

    open(&gate);
    queued_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("队列中的任务必须被 worker 认领执行（shutdown 会丢弃积压）");
    ptp.threads().shutdown();
    ptp.threads().join();

    assert_eq!(
        ran.load(Ordering::SeqCst),
        3,
        "三件任务全部执行——提交侧饱和不丢工作"
    );
    let tm = ptp.threads().metrics();
    assert_eq!(
        tm.rejected, 1,
        "策略触发留痕（CallerRuns 内联计入 rejected，不计入 completed）"
    );
    assert_eq!(tm.completed, 2, "worker 侧执行两件");
    let m = buffers.metrics();
    assert_eq!(m.degraded, 1, "内联任务遇宇宙满=降级直构（读数可见）");
    assert_eq!(m.constructed, 2, "宇宙一件 + 降级一件");
    assert_eq!(m.core.borrowed, 0);
    assert_eq!(m.core.idle, 1, "宇宙一件归还入闲槽；降级件归还即释放");
}

// ── resize 缩容后提交：归还超新闲槽上限=溢出丢弃计数 ─────────────────────

#[test]
fn resize_shrink_overflow_semantics() {
    let _g = serial();
    let buffers = Arc::new(ObjectPool::<Vec<u8>>::new(2, 2, Vec::new));
    let ptp = PooledThreadPool::new(
        Arc::clone(&buffers),
        ThreadPool::new(ThreadPoolTuning::new(2, 2, 16)),
    );
    // 缩容：闲槽上限 2→1。
    ptp.buffer_pool()
        .resize(ObjectPoolTuning {
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

    let m = buffers.metrics();
    assert_eq!(m.core.borrowed, 0);
    assert_eq!(m.core.idle, 1, "缩容后闲槽上限=1，只留一件");
    assert_eq!(m.core.dropped_overflow, 1, "超限件溢出丢弃计数=1");
    assert_eq!(m.degraded, 0);
    assert_eq!(ptp.threads().metrics().completed, 3);
}

// ── 包装开销锚：提交侧恰一次 Box（同手写），零额外分配 + ns 级 delta ──────

/// 单轮测量：全新池同构对比。返回 None=分配读数窗被污染，调用方换全新池重试。
/// 硬判据：blocker 占住唯一 worker（FIFO 首位，窗口内零任务完成）时 live
/// delta=恰 N 次 Box；排空后回基线=零滞留。
/// 窗口纯化两件：①预热——idle 槽位与池内部容量先行长成（首次归还 0→1 的缓冲
/// 滞留属池内部状态，须落在窗前）；②blocker 用 Mutex/Condvar 闸门（零分配；
/// mpsc 的 Packet 会在窗内释放打脏读数）。
fn measure_once(n: u64) -> Option<(f64, f64)> {
    let hand_pool = Arc::new(ObjectPool::<Vec<u8>>::new(16, 16, Vec::new));
    let hand = ThreadPool::new(ThreadPoolTuning::new(1, 1, 20_000));
    let wrapped_pool = Arc::new(ObjectPool::<Vec<u8>>::new(16, 16, Vec::new));
    let wrapped = PooledThreadPool::new(
        Arc::clone(&wrapped_pool),
        ThreadPool::new(ThreadPoolTuning::new(1, 1, 20_000)),
    );

    // 预热：两池各过 16 件（idle 槽位/池内部容量长满），排空后进窗。
    for _ in 0..16u64 {
        let pool = Arc::clone(&hand_pool);
        hand.execute(move || drop(pool.acquire())).unwrap();
        wrapped.submit(|_| {}).unwrap();
    }
    settle();

    let gate: Gate = Arc::new((Mutex::new(false), Condvar::new()));

    // ── 手写基线 ──
    let g2 = Arc::clone(&gate);
    hand.execute(move || block(&g2)).unwrap();
    settle();
    let before = CountingAllocator::live_allocs();
    let t0 = std::time::Instant::now();
    for _ in 0..n {
        let pool = Arc::clone(&hand_pool);
        hand.execute(move || drop(pool.acquire())).unwrap();
    }
    let hand_submit = t0.elapsed();
    let hand_delta = CountingAllocator::live_allocs() as i64 - before as i64;
    open(&gate);
    settle();
    // 排空会计：N 个任务 Box 窗内分配 + N 窗内释放 −N 净 0；blocker Box 窗前分配、
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
    *lock_or_recover(&gate.0) = false;
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
        || wrapped_pool.metrics().degraded != 0
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

    let mut outcome = None;
    for attempt in 0..8 {
        if let Some(r) = measure_once(N) {
            println!("clean allocation window at attempt {attempt}");
            outcome = Some(r);
            break;
        }
    }
    let (hand_ns, wrapped_ns) =
        outcome.expect("分配读数窗连续 8 轮被污染——环境噪音异常，禁带脏数据放行");

    // 分配锚已在 measure_once 内硬断言（两路径各恰 N 次 Box + 零滞留）。
    // 开销读数**只打印、不设断言**：墙钟判据属 bench 面（perf.yml 滚动基线 +
    // artifacts），功能测试里的时间阈值在 CI 并发负载下只会制造 flaky 红。
    println!("handwritten: {hand_ns:.0} ns/op submit-side");
    println!("wrapped: {wrapped_ns:.0} ns/op submit-side");
    println!("wrapper overhead delta: {:.0} ns/op", wrapped_ns - hand_ns);
}
