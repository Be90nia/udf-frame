#![cfg(feature = "runtime")]
//! 账本复用所有权（GitHub issue#84）：非等待型根（`try_dispatch` /
//! `ActionSender::dispatch`）的账本**只能在其终局发布之后**入复用池。Emit 后代
//! 仍排队（`outstanding > 0`）时入池 ⇒ 并发外部准入 pop 后 `reset()` 清空活
//! 账本 ⇒ 后代结算记到别人的账本上 ⇒ 跨归属（另一个根提前发布终局 / 携带别人
//! 的失败）。
//!
//! 定稿口径（issue#84 维护者）：**终局发布是唯一可复用边界**；等待型根由
//! waiter 消费 `Outcome` 后归还；后代结算绝不把等待型根的账本放进池子。
//!
//! 编排纪律：全部用两段闸 + 队列深轮询做确定性握手（禁 sleep 竞速）；同一
//! store 上「持排空权者闸在 reducer 内」⇒ 其他线程的入队只入队、不排空。

use skeleton::{Effect, EffectBuf, NoFx, Reducer, Store as _};
use skeleton_infra::runtime::{AdmissionOutcome, SyncStore};
use std::convert::Infallible;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::thread;
use std::time::Duration;

/// 两段闸：进入点 + 放行点（确定性并发编排，禁 sleep 竞速）。
struct Gate {
    entered: (Mutex<bool>, Condvar),
    release: (Mutex<bool>, Condvar),
}

impl Gate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: (Mutex::new(false), Condvar::new()),
            release: (Mutex::new(false), Condvar::new()),
        })
    }

    fn mark_entered(&self) {
        let mut g = self.entered.0.lock().unwrap();
        *g = true;
        self.entered.1.notify_all();
    }

    fn wait_entered(&self) {
        let mut g = self.entered.0.lock().unwrap();
        while !*g {
            g = self.entered.1.wait(g).unwrap();
        }
    }

    fn open(&self) {
        let mut g = self.release.0.lock().unwrap();
        *g = true;
        self.release.1.notify_all();
    }

    fn wait_release(&self) {
        let mut g = self.release.0.lock().unwrap();
        while !*g {
            g = self.release.1.wait(g).unwrap();
        }
    }
}

/// 有界等待队列深度达 `want`（握手点：入队已完成 ⇒ 根账本已取用并复位）。
fn wait_pending(store: &SyncStore<u32, Act, Infallible>, want: usize) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while store.pending_actions() < want {
        assert!(
            std::time::Instant::now() < deadline,
            "队列深在 5s 内未达 {want}（编排前提不成立）"
        );
        thread::yield_now();
    }
}

/// panic 载荷取文本（断言失败消息用）。
fn panic_text(payload: Box<dyn std::any::Any + Send>) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|s| (*s).to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "<non-string panic payload>".to_string())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Act {
    /// 占住排空权（reducer 内等闸），放行后返回。
    Hold,
    /// 非等待型根（`try_dispatch`）：产出一条 Emit 后代。
    Root,
    /// `Root` 的因果后代：闸住，放行后产出一条更晚的后代。
    Desc,
    /// 更晚的后代（排在第二个根之后 ⇒ 可把结算错记到第二个根的账本上）。
    Late,
    /// 第二个独立根（等待型）：产出一条 Emit 后代。
    SecondRoot,
    /// `SecondRoot` 的因果后代：闸住，放行后计 1。
    SecondChild,
    /// 非等待型根：产出一条会失败的 Emit 后代。
    RootFail,
    /// `RootFail` 的因果后代：闸住，放行后 panic（reduce 期失败）。
    DescFail,
    /// 不产效果的动作（收尾用）。
    Plain,
}

struct Fixture {
    hold: Arc<Gate>,
    desc: Arc<Gate>,
    tail: Arc<Gate>,
    tail_ran: Arc<AtomicU32>,
}

impl Reducer for Fixture {
    type State = u32;
    type Action = Act;
    type Op = Infallible;

    fn initial(&self) -> u32 {
        0
    }

    fn reduce(&self, s: &u32, a: Act, out: &mut EffectBuf<Act, Infallible>) -> u32 {
        match a {
            Act::Hold => {
                self.hold.mark_entered();
                self.hold.wait_release();
                s + 1
            }
            Act::Root => {
                out.push(Effect::Emit(Act::Desc));
                s + 1
            }
            Act::Desc => {
                self.desc.mark_entered();
                self.desc.wait_release();
                out.push(Effect::Emit(Act::Late));
                s + 1
            }
            Act::Late => s + 1,
            Act::SecondRoot => {
                out.push(Effect::Emit(Act::SecondChild));
                s + 1
            }
            Act::SecondChild => {
                self.tail.mark_entered();
                self.tail.wait_release();
                self.tail_ran.fetch_add(1, Ordering::SeqCst);
                s + 1
            }
            Act::RootFail => {
                out.push(Effect::Emit(Act::DescFail));
                s + 1
            }
            Act::DescFail => {
                self.desc.mark_entered();
                self.desc.wait_release();
                panic!("descendant detonation (ledger ownership test)");
            }
            Act::Plain => s + 1,
        }
    }
}

fn fixture() -> (Fixture, Arc<Gate>, Arc<Gate>, Arc<Gate>, Arc<AtomicU32>) {
    let hold = Gate::new();
    let desc = Gate::new();
    let tail = Gate::new();
    let tail_ran = Arc::new(AtomicU32::new(0));
    let fx = Fixture {
        hold: Arc::clone(&hold),
        desc: Arc::clone(&desc),
        tail: Arc::clone(&tail),
        tail_ran: Arc::clone(&tail_ran),
    };
    (fx, hold, desc, tail, tail_ran)
}

/// issue#84 主回归：**非等待型根 + 在途后代 + 强制取池 ⇒ 无跨归属**。
///
/// 构造（跨线程，禁 sleep 竞速）：
/// - A 线程 `dispatch(Hold)`：夺排空权后在 reducer 内等闸；
/// - `try_dispatch(Root)`（非等待型根，`waiter=false`）：Root 结算时它的
///   `Emit(Desc)` 后代仍在途（`outstanding == 1`）；
/// - 后端 `Desc` 闸住 ⇒ 外部线程 `dispatch(SecondRoot)` 入队；队列深握手确证
///   它已取得账本（旧实现取得的是 **Root 的活账本**并 `reset`）；
/// - 放行 `Desc` ⇒ 它产出更晚的后代 `Late`（排在 SecondRoot **之后**）。
///
/// 判据：`SecondRoot` 的 dispatch 只能在自己的后代 `SecondChild` 终局后返回。
/// 旧实现下 `Late` 的结算错误清零了第二根的未终局后代计数 ⇒ 第二根提前发布
/// 终局 ⇒ dispatch 早于自己的后代返回（`tail_ran == 0`）⇒ 判红。
#[test]
fn non_waiting_root_ledger_not_reused_while_descendant_outstanding() {
    let (fx, hold, desc, tail, tail_ran) = fixture();
    let store = Arc::new(SyncStore::new(fx, NoFx));

    // A：夺排空权并闸在 reducer 内（其余入队只入队、不排空）。
    let store_a = Arc::clone(&store);
    let handle_a = thread::spawn(move || store_a.dispatch(Act::Hold));
    hold.wait_entered();

    // 非等待型根：只入队（A 持排空权）。
    assert_eq!(
        store.try_dispatch(Act::Root),
        AdmissionOutcome::Accepted,
        "前置：非等待型根必须被接受"
    );

    // 放行 A：A 处理 Root（产出 Emit(Desc)）→ Root 结算 → 处理 Desc（闸住）。
    hold.open();
    desc.wait_entered();

    // 第二个独立根（等待型）：旧实现会从池里取到 Root 的活账本并 reset。
    let store_b = Arc::clone(&store);
    let tail_ran_b = Arc::clone(&tail_ran);
    let (tx, rx) = mpsc::channel();
    let handle_b = thread::spawn(move || {
        store_b.dispatch(Act::SecondRoot);
        tx.send(tail_ran_b.load(Ordering::SeqCst)).unwrap();
    });

    // 入队 + 账本取用已完成的确定性握手点（队列深是可观测读侧）。
    wait_pending(&store, 1);

    // 放行后代：产出更晚的后代 Late（排队位在 SecondRoot 之后）并结算。
    desc.open();

    // 旧实现：第二根提前发布终局 ⇒ B 在 SecondChild 处理前返回。
    match rx.recv_timeout(Duration::from_secs(2)) {
        Ok(observed) => {
            // 提前返回：先放开 tail 免 A 永久阻塞，再判红并给出现场。
            tail.open();
            let _ = handle_a.join();
            let _ = handle_b.join();
            assert_eq!(
                observed, 1,
                "第二个根的终局必须只由**自己的**后代决定：它在自己的后代结算前就返回了\
                 （跨归属：Root 的后代结算清零了 SecondRoot 的未终局后代计数）"
            );
        }
        Err(_) => {
            // 正确实现：第二根的终局等 SecondChild 终局（收尾放行）。
            tail.open();
            let observed = rx
                .recv_timeout(Duration::from_secs(5))
                .expect("第二根的 dispatch 必须返回（收尾放行后）");
            handle_a.join().expect("A 不得 panic");
            handle_b.join().expect("B 不得 panic");
            assert_eq!(observed, 1, "第二根返回时其因果后代必须已终局");
        }
    }
}

/// issue#84 失败归属回归：在途**失败**后代不得把失败记到并发根的账本上。
///
/// 构造：非等待型根 `RootFail` 产出 `Emit(DescFail)`；`DescFail` 闸住后 panic
/// ⇒ 排空中断展开（当前记录 unwind 路径）。旧实现下 RootFail 的账本在
/// `finish_root` 后即入池（`outstanding == 1`），被并发的 `SecondRoot` 取走 ⇒
/// `DescFail` 的失败（`phase=Reducing`）记到第二根的账本上；摘队 sweep 随后
/// 结算第二根自己的排队记录（`phase=Queued`）也只能「只计数」——第二根的终局
/// 携带**别人的**失败。
///
/// 判据：第二根收到的失败必须是它自己的中断结算（`phase=Queued`），不得是
/// 另一个因果链的 reduce 期失败（`phase=Reducing`）。
#[test]
fn descendant_failure_not_attributed_to_concurrent_root() {
    let (fx, hold, desc, tail, _tail_ran) = fixture();
    let store = Arc::new(SyncStore::new(fx, NoFx));

    let store_a = Arc::clone(&store);
    let handle_a = thread::spawn(move || store_a.dispatch(Act::Hold));
    hold.wait_entered();

    assert_eq!(
        store.try_dispatch(Act::RootFail),
        AdmissionOutcome::Accepted,
        "前置：非等待型根必须被接受"
    );
    hold.open();
    desc.wait_entered();

    // 并发外部根：旧实现取到 RootFail 的活账本（尚未终局）。
    let store_b = Arc::clone(&store);
    let handle_b = thread::spawn(move || store_b.dispatch(Act::SecondRoot));
    wait_pending(&store, 1);

    // 放行后代 ⇒ 它在 reduce 期 panic ⇒ 排空展开（当前记录 unwind 路径）。
    desc.open();
    let panic_a = handle_a
        .join()
        .expect_err("后代 panic 必须照常上浮到排空者（禁吞）");
    assert!(
        panic_text(panic_a).contains("descendant detonation"),
        "A 必须收到后代 reducer 的原始 panic 载荷"
    );

    let panic_b = handle_b
        .join()
        .expect_err("第二根的排队记录被摘队结算 ⇒ 其 dispatch 必须显式失败");
    let text_b = panic_text(panic_b);
    assert!(
        text_b.contains("phase=Queued"),
        "第二根只能收到**自己**的摘队中断失败（phase=Queued）：{text_b}"
    );
    assert!(
        !text_b.contains("phase=Reducing"),
        "第二根不得携带另一个因果链的 reduce 期失败（跨归属）：{text_b}"
    );

    // 收尾：后续根不得携带前一因果链的失败（池复用前终局已发布 ⇒ reset 干净）。
    tail.open();
    let before = *store.snapshot();
    store.dispatch(Act::Plain);
    assert_eq!(*store.snapshot(), before + 1, "收尾动作必须正常生效");
}

/// 等待型根对照（⑤）：根已终局也必须等 waiter 消费 `Outcome` 才可复用。
///
/// 判据：后代结算不得把等待型根的账本入池——若入了池并被 reset，waiter 醒来
/// 只见 `terminal = None` 而永久挂起。本测以有界超时把「挂起」变成失败而非
/// 挂死整个测试进程。
#[test]
fn waiting_root_outcome_survives_descendant_settlement() {
    let (fx, _hold, _desc, tail, tail_ran) = fixture();
    tail.open();
    let store = Arc::new(SyncStore::new(fx, NoFx));

    let store_b = Arc::clone(&store);
    let tail_ran_b = Arc::clone(&tail_ran);
    let (tx, rx) = mpsc::channel();
    let handle_b = thread::spawn(move || {
        store_b.dispatch(Act::SecondRoot);
        tx.send(tail_ran_b.load(Ordering::SeqCst)).unwrap();
    });

    let observed = rx
        .recv_timeout(Duration::from_secs(5))
        .expect("等待型根的 dispatch 必须返回终局（后代结算不得毁掉 waiter 的终局面）");
    handle_b.join().expect("等待型根的 dispatch 不得 panic");
    assert_eq!(observed, 1, "返回时其因果后代必须已终局");
    assert_eq!(store.pending_actions(), 0, "队列不得残留");
}

/// 回灌预算脱钩（Deferred）对照（⑥）：脱钩把旧账本以 `Deferred` 收口、后续
/// 记录迁到续接账本；旧账本**不得**在终局前入池，续接账本也不必随旧账本一起
/// 入池。
///
/// 判据：脱钩链停下（预算止步可见）后，store 仍能正常服务，且新根不携带前一
/// 因果链的终局面/失败；停放记录零丢失。
#[test]
fn deferred_detach_keeps_old_ledger_until_terminal() {
    // 深层回灌链：深度 > 预算（64）即脱钩。
    struct Chain;

    impl Reducer for Chain {
        type State = u32;
        type Action = u32;
        type Op = Infallible;

        fn initial(&self) -> u32 {
            0
        }

        fn reduce(&self, s: &u32, depth: u32, out: &mut EffectBuf<u32, Infallible>) -> u32 {
            if depth <= 70 {
                out.push(Effect::Emit(depth + 1));
            }
            s + 1
        }
    }

    let store = SyncStore::new(Chain, NoFx);
    // 非等待型根：预算止步与脱钩都在它的因果链里发生。
    assert_eq!(
        store.try_dispatch(0),
        AdmissionOutcome::Accepted,
        "前置：根必须被接受"
    );
    assert!(
        store.feedback_budget_exceeded() >= 1,
        "深度超预算必须脱钩留痕（Deferred 收口）"
    );
    assert!(
        store.pending_actions() >= 1,
        "被脱钩的记录必须停放（零丢失），不得静默蒸发"
    );

    // 脱钩后 store 仍可服务：新根（等待型）拿到的是空闲账本（不是旧的活账本），
    // 其终局面只由自己的链决定（深度超预算 ⇒ 不产效果 ⇒ 直接 Completed）。
    let before = *store.snapshot();
    store.dispatch(u32::MAX);
    assert_eq!(*store.snapshot(), before + 1, "新根必须正常生效");
}
