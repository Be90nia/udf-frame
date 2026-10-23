//! dispatch 热路径分段归因基准。
//!
//! 口径与 multidim_eval 一致（batch_mean + black_box，Instant 批均摊薄时钟地板）。
//! 各段为**独立代理测量**（同型原语单测），非插桩——插桩会改变代码布局，归因看
//! 量级不看个位数。段清单对应契约：闸检查 / queue push / drain 循环内 reduce dyn、
//! 快照 Arc::new / notify 段（notify_slots 锁+遍历）/ EffectBuf take-还 / back 克隆。

use skeleton::{Effect, EffectBuf, EffectRuntime, Reducer, Store as _};
use skeleton_infra::runtime::SyncStore as Store;
use std::cell::UnsafeCell;
use std::collections::VecDeque;
use std::convert::Infallible;
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Instant;

fn batch_mean<T>(mut op: impl FnMut() -> T, n: u64) -> f64 {
    let t = Instant::now();
    for _ in 0..n {
        black_box(op());
    }
    t.elapsed().as_nanos() as f64 / n as f64
}

/// 三连批取中位，压单批噪声。
fn median3<T>(mut op: impl FnMut() -> T, n: u64) -> f64 {
    let mut xs = [
        batch_mean(&mut op, n),
        batch_mean(&mut op, n),
        batch_mean(&mut op, n),
    ];
    xs.sort_by(|a, b| a.partial_cmp(b).unwrap());
    xs[1]
}

enum Tick {
    Tick,
}

struct Count;

impl Reducer for Count {
    type State = u64;
    type Action = Tick;
    type Op = Infallible;

    fn initial(&self) -> u64 {
        0
    }

    fn reduce(&self, state: &u64, action: Tick, _out: &mut EffectBuf<Tick>) -> u64 {
        match action {
            Tick::Tick => state + 1,
        }
    }
}

struct Nop;

impl EffectRuntime<Tick, Infallible> for Nop {
    fn run(&mut self, _: Effect<Tick, Infallible>, _: &skeleton::ActionSender<Tick>) {}
}

fn main() {
    const N: u64 = 2_000_000;
    println!("== dispatch_profile 分段归因（batch mean ns，三批中位）==");

    // ── 闸与排空旗的原子原语 ──
    let closed = Arc::new(AtomicBool::new(false));
    let c2 = Arc::clone(&closed);
    println!(
        "gate_load_sc={:.1} gate_load_acq={:.1}",
        median3(|| c2.load(Ordering::SeqCst), N),
        median3(|| c2.load(Ordering::Acquire), N)
    );
    let flag = Arc::new(AtomicBool::new(false));
    let f2 = Arc::clone(&flag);
    let f3 = Arc::clone(&flag);
    println!(
        "drain_swap_sc={:.1} drain_swap_acqrel={:.1}",
        median3(|| f2.swap(true, Ordering::SeqCst), N),
        median3(|| f3.swap(false, Ordering::AcqRel), N)
    );
    println!(
        "guard_store_sc={:.1} guard_store_rel={:.1}",
        median3(|| flag.store(false, Ordering::SeqCst), N),
        median3(|| flag.store(true, Ordering::Release), N)
    );

    // ── 锁原语：queue 锁 / push+pop 对 ──
    let m = Arc::new(Mutex::new(()));
    let m2 = Arc::clone(&m);
    println!("mutex_pair={:.1}", median3(|| drop(m2.lock().unwrap()), N));

    let q = Arc::new(Mutex::new(VecDeque::with_capacity(16)));
    let q2 = Arc::clone(&q);
    println!(
        "queue_push_pop={:.1}",
        median3(
            || {
                q2.lock().unwrap().push_back(0u64);
                black_box(q2.lock().unwrap().pop_front());
            },
            N
        )
    );

    // std::sync::mpsc 对照（锁消融备选：Sender::send + try_iter 排空）
    let (tx, rx) = std::sync::mpsc::channel::<u64>();
    let rx_cell = UnsafeCell::new(rx);
    println!(
        "mpsc_send_recv={:.1}",
        median3(
            || {
                tx.send(0u64).unwrap();
                black_box(unsafe { (*rx_cell.get()).try_iter().next() });
            },
            N
        )
    );

    // ── 快照面：Arc::new / 克隆 ──
    println!(
        "arc_new_drop={:.1} arc_clone_drop={:.1}",
        median3(|| drop(Arc::new(7u64)), N),
        {
            let a = Arc::new(7u64);
            median3(|| drop(Arc::clone(&a)), N)
        }
    );

    // 状态替换对（current 克隆锁 + next 写锁）——与 reduce 组合近似现 drain 内核
    let st = Arc::new(Mutex::new(Arc::new(0u64)));
    let st2 = Arc::clone(&st);
    println!(
        "state_two_lock_swap={:.1}",
        median3(
            || {
                let current = Arc::clone(&st2.lock().unwrap());
                let next = Arc::new(black_box(*current) + 1);
                *st2.lock().unwrap() = Arc::clone(&next);
            },
            N
        )
    );

    // ── reduce dyn 调用 ──
    let reducer: Arc<dyn Reducer<State = u64, Action = Tick, Op = Infallible> + Send + Sync> =
        Arc::new(Count);
    let buf_cell = UnsafeCell::new(EffectBuf::new());
    println!(
        "reduce_dyn_only={:.1}",
        median3(
            || black_box(reducer.reduce(black_box(&7u64), Tick::Tick, unsafe {
                &mut *buf_cell.get()
            })),
            N
        )
    );

    // ── EffectBuf take-还（现行模式：两对锁） vs 直接清用（消锁后模式） ──
    let ebuf = Mutex::new(EffectBuf::<Tick>::new());
    println!(
        "effectbuf_take_put_locked={:.1}",
        median3(
            || {
                let taken = {
                    let mut g = ebuf.lock().unwrap();
                    g.clear();
                    std::mem::take(&mut *g)
                };
                black_box(taken.is_empty());
                *ebuf.lock().unwrap() = taken;
            },
            N
        )
    );
    let ebuf2 = UnsafeCell::new(EffectBuf::<Tick>::new());
    println!(
        "effectbuf_clear_inplace={:.1}",
        median3(
            || {
                let buf = unsafe { &mut *ebuf2.get() };
                buf.clear();
                black_box(buf.is_empty());
            },
            N
        )
    );

    // ── notify 段代理（零订阅）：现行双锁收集 vs 版本快照读 ──
    let slots: Mutex<Vec<Weak<()>>> = Mutex::new(Vec::new());
    let stage: Mutex<Vec<Arc<()>>> = Mutex::new(Vec::new());
    println!(
        "notify_collect_zero_subs={:.1}",
        median3(
            || {
                let mut s = stage.lock().unwrap();
                s.clear();
                let mut subs = slots.lock().unwrap();
                subs.retain(|w| w.upgrade().is_some());
                s.extend(subs.iter().filter_map(|w| w.upgrade()));
            },
            N
        )
    );
    let version = AtomicUsize::new(0);
    println!(
        "notify_version_read={:.1}",
        median3(|| version.load(Ordering::Acquire), N)
    );

    let weak_holder = Arc::new(());
    let weak = Arc::downgrade(&weak_holder);
    println!(
        "weak_upgrade_drop={:.1}",
        median3(|| drop(weak.upgrade()), N)
    );

    println!(
        "catch_unwind_noop={:.1}",
        median3(
            || std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| black_box(1u64))),
            N
        )
    );

    // ── 端到端交叉核对（应与 multidim skeleton_dispatch 同量级） ──
    let store = Store::new(Count, Nop);
    for _ in 0..10_000 {
        store.dispatch(Tick::Tick);
    }
    let e2e = median3(|| store.dispatch(Tick::Tick), 200_000);
    println!(
        "end_to_end_dispatch={e2e:.1} final={}",
        black_box(*store.snapshot())
    );

    // ── 审计事件流零开销双证（observability 批，R1-2 收口）────────────────
    // 未订阅：上行 end_to_end_dispatch 即默认关闭锚（emit 点仅一次 gate load，
    // 零分配零锁）。订阅态对照：同型 store 挂 no-op 回调 + 有界环——订阅是
    // opt-in 的显式成本（gate load + 环锁 + 回调），非锚回退面。
    let audit_store = Store::new(Count, Nop);
    let _ticket = audit_store.subscribe_events(4096, |_| {});
    for _ in 0..10_000 {
        audit_store.dispatch(Tick::Tick);
    }
    let e2e_audit = median3(|| audit_store.dispatch(Tick::Tick), 200_000);
    println!("audit_subscribed_dispatch={e2e_audit:.1}");
}
