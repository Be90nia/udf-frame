#![cfg(feature = "runtime")]
//! issue#63：`try_subscribe` 登记与 `close` 闭门翻转的线性化——闭门先线性化时
//! 绝不能返回 `Ok`（否则「已登记但 store 已关」：票据在闭门后登记，永不触发，
//! 却对外表现为登记成功）。
//!
//! 竞态窗口的构造（无 sleep、握手驱动）：
//!
//! - 竞态线程的闭包携带**大数组**（`[u8; 256KB]` 按值移动）——buggy 版里
//!   `try_subscribe` 的第一道 closed 检查与 legacy `subscribe` 内部的第二道检查
//!   之间隔着闭包的**按值 memcpy**，该拷贝把「检查 → 登记」窗口从纳秒级放大
//!   到几十微秒级 ⇒ `close()` 的 `closed=true` 翻转可落进窗口。
//! - 多个竞态线程持续登记 + 关闭线程在握手后翻转 ⇒ 翻转瞬间几乎必然落在某个
//!   竞态线程的窗口内（窗口占单次迭代的相当比例，线程数越多越确定）。
//!
//! 断言取**能失败的输入**：窗口命中时（buggy 版）`try_subscribe` 内部第二道
//! closed 检查看到 `closed=true` ⇒ 计入 `subscriptions_after_close` 且仍登记一张
//! 死票据 ⇒ 红；修复版登记与闭门翻转共用一把订阅闸 ⇒ 二者互斥 ⇒ 计数恒 0 ⇒ 绿。

use std::convert::Infallible;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;

use skeleton::{Effect, EffectBuf, EffectRuntime, Reducer, Store as _};
use skeleton_infra::runtime::{StoreClosed, SyncStore as Store};

// ── 固定装置：计数器 reducer + 空解释器 ─────────────────────────────────────

struct Counter;

impl Reducer for Counter {
    type State = u64;
    type Action = u64;
    type Op = Infallible;
    fn initial(&self) -> u64 {
        0
    }
    fn reduce(&self, s: &u64, a: u64, _out: &mut EffectBuf<u64, Infallible>) -> u64 {
        s + a
    }
}

struct NoFx;

impl EffectRuntime<u64, Infallible> for NoFx {
    fn run(&mut self, _: Effect<u64, Infallible>, _: &skeleton::ActionSender<u64>) {}
}

/// 竞态窗口放大器：按值捕获的大数组——闭包移动（memcpy）落在 `try_subscribe`
/// 的两道 closed 检查之间，把纳秒窗口放大到几十微秒级。
const WINDOW_PAYLOAD: usize = 256 * 1024;
/// 竞态线程数：翻转瞬间落在某个线程窗口内的概率随线程数上升。
const RACERS: usize = 8;
/// 单线程登记迭代上限（防挂死；正常路径在 close 翻转后立即收到 Err 退出）。
const MAX_ITERS: usize = 20_000;

/// #63 主验收：竞态窗口命中时也不得「已登记但 store 已关」。
///
/// buggy 版命中 ⇒ `try_subscribe` 内部第二道检查见 `closed=true` ⇒
/// `subscriptions_after_close` 自增（且仍登记一张死票据）⇒ 断言红。
/// 修复版（登记与闭门翻转同闸）⇒ 计数恒 0 ⇒ 绿。
#[test]
fn try_subscribe_racing_close_never_registers_after_close() {
    const ROUNDS: usize = 4;
    for round in 0..ROUNDS {
        let store = Arc::new(Store::new(Counter, NoFx));
        let (started_tx, started_rx) = mpsc::channel();
        let ok_total = Arc::new(AtomicUsize::new(0));

        let mut racers = Vec::with_capacity(RACERS);
        for _ in 0..RACERS {
            let store = Arc::clone(&store);
            let started_tx = started_tx.clone();
            let ok_total = Arc::clone(&ok_total);
            racers.push(thread::spawn(move || {
                // 握手信号在**进入循环前**发出 ⇒ close() 的翻转落在竞态循环
                // 运行期间（而非循环跑完之后）。
                let _ = started_tx.send(());
                for _ in 0..MAX_ITERS {
                    // 闭包按值捕获大数组 ⇒ 每次登记都发生一次大 memcpy
                    // （窗口放大：buggy 版该 memcpy 落在 try_subscribe 的
                    // closed 检查与 subscribe 内部的 closed 检查之间）。
                    let payload = [0u8; WINDOW_PAYLOAD];
                    match store.try_subscribe(move |_: &u64| {
                        let _ = &payload;
                    }) {
                        Ok(ticket) => {
                            ok_total.fetch_add(1, Ordering::Relaxed);
                            drop(ticket);
                        }
                        Err(StoreClosed) => break,
                    }
                }
            }));
        }
        drop(started_tx);

        // 握手：所有竞态线程都已进入登记循环 ⇒ 此刻 close() 的翻转落在某个
        // 竞态线程的「检查 → 登记」窗口内（窗口占迭代的相当比例）。
        for _ in 0..RACERS {
            started_rx
                .recv_timeout(std::time::Duration::from_secs(10))
                .expect("竞态线程未进入登记循环");
        }
        store.close();
        for racer in racers {
            racer.join().expect("竞态线程 panic");
        }

        assert_eq!(
            store.subscriptions_after_close(),
            0,
            "闭门后登记的订阅笔数必须为 0（issue#63，第 {round} 轮，\
             try_subscribe 成功 {} 次）——竞态窗口命中即红",
            ok_total.load(Ordering::Relaxed)
        );
    }
}

/// #63 验收（闭门后语义）：`close` 返回后每次 `try_subscribe` 都显式 `Err`，
/// 且 `subscriber_count` 不增（登记未发生）。
#[test]
fn try_subscribe_after_close_is_err_and_does_not_register() {
    let store = Store::new(Counter, NoFx);
    let before = store.subscriber_count();
    store.close();
    for _ in 0..8 {
        assert!(
            matches!(store.try_subscribe(|_| {}), Err(StoreClosed)),
            "闭门后 try_subscribe 必须显式 Err(StoreClosed)"
        );
    }
    assert_eq!(
        store.subscriber_count(),
        before,
        "闭门后 try_subscribe 不得登记（subscriber_count 不增）"
    );
    assert_eq!(
        store.subscriptions_after_close(),
        0,
        "Err 路径登记未发生，死订阅计数不掺水"
    );
}

/// #63 验收（legacy 面语义不变）：闭门后 legacy `subscribe` 仍返回可用票据
/// （可 drop、注销即时生效）并计入 `subscriptions_after_close`。
#[test]
fn legacy_subscribe_after_close_still_registers_and_counts() {
    let store = Store::new(Counter, NoFx);
    store.close();
    let ticket = store.subscribe(|_| {});
    assert_eq!(
        store.subscriptions_after_close(),
        1,
        "legacy 闭门登记照常计数"
    );
    assert_eq!(
        store.subscriber_count(),
        1,
        "legacy 闭门登记照常登记（票据在闭门后仍可 drop）"
    );
    drop(ticket);
    assert_eq!(
        store.subscriber_count(),
        0,
        "票据 drop 即时注销（表项即时摘除）"
    );
}

// ── issue#63：闭门排空/回调期间不持新闸（锁序纪律）────────────────────────

/// 阻塞 reducer：dispatch 的 reduce 阶段在此挂起（等主线程放行）。
struct BlockingReducer {
    entered: mpsc::Sender<()>,
    release: Mutex<mpsc::Receiver<()>>,
}

impl Reducer for BlockingReducer {
    type State = u64;
    type Action = u64;
    type Op = Infallible;
    fn initial(&self) -> u64 {
        0
    }
    fn reduce(&self, s: &u64, a: u64, _out: &mut EffectBuf<u64, Infallible>) -> u64 {
        let _ = self.entered.send(());
        let _ = self.release.lock().expect("release 通道未毒").recv();
        s + a
    }
}

/// #63 验收（顺序用例｜确定性拒绝）：**`close` 完成后**订阅必被拒。
///
/// 语义边界（本文件两条用例的分工，勿混读）：
///
/// - **本用例（顺序）**：`close()` 已**完成** ⇒ 闭门翻转已可见 ⇒ 之后的
///   `try_subscribe` 必 `Err(StoreClosed)`。同步点是显式的：close 线程在
///   `close()` 返回后发信号，主线程等到该信号后才发起登记 ⇒ 断言不依赖调度
///   运气（修复前本用例 5 次跑红 1 次：probe 与闭门翻转之间无同步点，probe
///   偶尔先跑赢拿到 `Ok`）。
/// - **竞态用例**（`try_subscribe_racing_close_never_registers_after_close`）：
///   登记与 `close` **同时起跑** ⇒ 两种线性化都合法（「Ok 且登记线性化在
///   close 之前」或「Err 且 close 已线性化」），唯一非法的是「既登记成功又
///   计入闭门后」⇒ 该用例断言 `subscriptions_after_close` 恒 0。
///
/// 附带守 issue#63 的锁序纪律（禁持闸跑用户回调 / 禁持闸排空队列）：本用例
/// 让排空**在途**（reducer 阻塞、排空租约被占）时发起并发 `try_subscribe`，
/// 只断言它**在有界等待内能返回**（不被闸挡住），**不断言其竞态结果值**
/// （结果值属竞态用例的地盘）——闸被持有跨越排空 ⇒ 探针被挡 ⇒ 超时 ⇒ 红。
#[test]
fn close_completed_then_subscription_is_rejected() {
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let store = Arc::new(Store::new(
        BlockingReducer {
            entered: entered_tx,
            release: Mutex::new(release_rx),
        },
        NoFx,
    ));

    // dispatch 线程：reduce 阶段挂起 ⇒ 排空在途（排空租约被占，reducer 未放行）。
    let dispatch_thread = {
        let store = Arc::clone(&store);
        thread::spawn(move || {
            store.dispatch(1);
        })
    };
    entered_rx
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("reducer 未进入阻塞点（排空未在途）");

    // close 线程：`close()` 返回后发信号 ⇒「close 已完成」成为显式可观测同步点。
    let (closed_tx, closed_rx) = mpsc::channel();
    let close_thread = {
        let store = Arc::clone(&store);
        thread::spawn(move || {
            store.close();
            let _ = closed_tx.send(());
        })
    };

    // 闸纪律探针：排空仍在途时并发登记必须能**完成**（不被闸挡住）。
    // 只断言「有界等待内返回」——不追究其结果值（结果值由竞态用例守）。
    let (probe_tx, probe_rx) = mpsc::channel();
    let probe = {
        let store = Arc::clone(&store);
        thread::spawn(move || {
            let _ = probe_tx.send(store.try_subscribe(|_| {}).is_ok());
        })
    };
    probe_rx
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("并发 try_subscribe 被订阅闸挡住（闸不得跨越排空持有）");

    // 顺序断言：等 close 完成信号 ⇒ 闭门翻转已可见 ⇒ 登记必被拒。
    closed_rx
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("close 未在期限内完成");
    assert!(
        matches!(store.try_subscribe(|_| {}), Err(StoreClosed)),
        "close 完成后 try_subscribe 必须 Err(StoreClosed)"
    );

    // 放行 reducer ⇒ 排空收尾。
    release_tx.send(()).expect("放行失败");
    probe.join().expect("探针线程 panic");
    dispatch_thread.join().expect("dispatch 线程 panic");
    close_thread.join().expect("close 线程 panic");
}
