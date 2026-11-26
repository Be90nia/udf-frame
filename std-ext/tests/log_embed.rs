//! log-embed 埋点验收（std-ext 域切片：backoff/fsx/iter + 批 5-2 scheduler/
//! ratelimit/retry/cache，共七域 47 调用点（口径=`embed_event!` 宏调用点；
//! issue#202 把 backoff 的原 1 处移到 retry 编排层——纯算术件零调用点，
//! 见仓库根 `docs/reference.md` 的 log-embed 段）；`--features log-embed`
//! 开跑。shutdown 一域留在 skeleton-infra/tests/log_embed.rs，两文件捕获
//! 订阅器同款各一份）：
//!
//! ① feature 开：七域事件断言（target 精确 + level 精确 + 字段子串）——
//!    本文件 `#![cfg(feature = "log-embed")]` 门控，feature 关时整文件为空，
//!    `cargo test` 默认面零负担；
//! ② feature 关：零 tracing 依赖是构建面事实（cargo tree 证明），不进
//!    单测（测试无法观测「依赖不存在」）。
//!
//! 捕获订阅器：手写最小 `Subscriber`（skeleton-infra 同款形态），线程作用域
//! `set_default` 挂载不动全局默认；唯 scheduler worker 线程退出事件跨线程，
//! 独立单测用 `set_global_default` 捕获（本二进制内唯一全局挂载点，其余
//! 测试的线程局部订阅器按线程优先，互不串扰）。

#![cfg(feature = "log-embed")]

use std::fmt::Write as _;
use std::fs;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use skeleton::clock::{Clock, MockClock, SystemClock};
use std_ext::backoff::backoff_delay;
use std_ext::cache::{LruCache, TtlCache};
use std_ext::fsx::{Sort, SymlinkPolicy, TempDir, copy_tree, temp_dir_in, walk};
use std_ext::iter::process_results;
use std_ext::ratelimit::TokenBucket;
use std_ext::retry::{RetryPolicy, Sleeper, retry_if};
use std_ext::scheduler::{AdmissionError, Scheduler};

use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Record};
use tracing::{Event, Id, Level, Metadata, Subscriber};

// ---- 捕获订阅器 ----

#[derive(Debug)]
struct Captured {
    target: &'static str,
    level: Level,
    fields: String,
    /// 发射线程名（`None` = 无名线程）：worker 收尾路径不得同步调用宿主订阅器
    /// ——#221 §2.5 只能按线程归因断言（进程级/线程级捕获面都与他测共享）。
    thread: Option<String>,
}

#[derive(Default)]
struct Capture {
    events: Mutex<Vec<Captured>>,
}

impl Capture {
    /// 断言存在一条 target/level 精确匹配且字段全部命中的事件，否则
    /// panic 并附实际捕获面（测试自证失败原因，不留「没断到」悬案）。
    /// 匹配三分（审计 F5 消除裸 contains 的 `seq=1`⊂`seq=12` 假阳性面，
    /// 同时保留动态值字段的存在性断言）：f 含空格=多词片段 contains；
    /// f 以 `=` 结尾=字段存在性（token 前缀，值任意）；否则 token 精确等值。
    fn assert_event(&self, target: &str, level: Level, fields_contains: &[&str]) {
        let hit = |fields: &str, f: &str| {
            if f.contains(' ') {
                fields.contains(f)
            } else if f.ends_with('=') {
                fields.split(' ').any(|tok| tok.starts_with(f))
            } else {
                fields.split(' ').any(|tok| tok == f)
            }
        };
        let events = self.events.lock().expect("capture mutex").iter().any(|e| {
            e.target == target
                && e.level == level
                && fields_contains.iter().all(|f| hit(&e.fields, f))
        });
        assert!(
            events,
            "未捕获 target={target} level={level:?} fields~{fields_contains:?}；实际={:?}",
            self.events.lock().expect("capture mutex")
        );
    }

    fn len(&self) -> usize {
        self.events.lock().expect("capture mutex").len()
    }

    /// 只数 target+level 精确匹配的事件。
    ///
    /// 本文件那个 `set_global_default` 的捕获面是**进程级**的（同二进制其它
    /// 测试/线程的事件也会进来）⇒ 断言精确条数时必须先按 target/level 收敛，
    /// 裸 `len()` 会被无关事件污染（`worker_teardown_*` 曾因此偶红）。
    fn count_target(&self, target: &str, level: Level) -> usize {
        self.events
            .lock()
            .expect("capture mutex")
            .iter()
            .filter(|e| e.target == target && e.level == level)
            .count()
    }

    /// 按发射线程名计数（worker 收尾路径的归因判据）。
    fn count_from_thread(&self, thread_name: &str) -> usize {
        self.events
            .lock()
            .expect("capture mutex")
            .iter()
            .filter(|e| e.thread.as_deref() == Some(thread_name))
            .count()
    }
}

struct FieldWriter(String);

impl Visit for FieldWriter {
    // 静态串字段原样落（默认 record_str 转发 record_debug 会被 Debug 加引号）。
    fn record_str(&mut self, field: &Field, value: &str) {
        if !self.0.is_empty() {
            self.0.push(' ');
        }
        let _ = write!(&mut self.0, "{}={}", field.name(), value);
    }

    // 数值/布尔字段经 Visit 默认实现转发到 record_debug，字符串化统一在此。
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if !self.0.is_empty() {
            self.0.push(' ');
        }
        let _ = write!(&mut self.0, "{}={:?}", field.name(), value);
    }
}

impl Subscriber for Capture {
    fn enabled(&self, _: &Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }
    fn record(&self, _: &Id, _: &Record<'_>) {}
    fn record_follows_from(&self, _: &Id, _: &Id) {}
    fn event(&self, event: &Event<'_>) {
        let mut writer = FieldWriter(String::new());
        event.record(&mut writer);
        self.events.lock().expect("capture mutex").push(Captured {
            target: event.metadata().target(),
            level: *event.metadata().level(),
            fields: writer.0,
            thread: std::thread::current().name().map(str::to_owned),
        });
    }
    fn enter(&self, _: &Id) {}
    fn exit(&self, _: &Id) {}
}

type SharedCapture = Arc<Capture>;

fn capture() -> SharedCapture {
    Arc::new(Capture::default())
}

// ---- backoff 域（issue#202：纯算术件零事件；计算入参的观测面移到 retry 调用点）----

#[test]
fn backoff_delay_emits_no_event() {
    let cap = capture();
    let _guard = tracing::subscriber::set_default(cap.clone());
    let d = backoff_delay(3, Duration::from_millis(100), Duration::from_millis(250));
    assert_eq!(d, Duration::from_millis(250), "线性 300ms 截平到上限");
    assert_eq!(cap.len(), 0, "纯算术件不得调用宿主订阅者（issue#202）");
}

// ---- 数据域 scratch 守卫（同 tests/fsx.rs：dogfood temp_dir_in，工作区
// CARGO_TARGET_TMPDIR 落点，Drop 兜底自清，禁系统 TEMP）----

fn data_guard(tag: &str) -> TempDir {
    temp_dir_in(env!("CARGO_TARGET_TMPDIR"), &format!("logembed-{tag}-"))
        .expect("scratch root under CARGO_TARGET_TMPDIR")
}

// ---- fsx 域（target skeleton::fsx::*；流转=debug / 异常=warn）----

#[test]
fn fsx_walk_frame_advance_is_debug() {
    let g = data_guard("walk");
    fs::create_dir(g.path().join("sub")).unwrap();
    fs::write(g.path().join("sub").join("f.txt"), b"x").unwrap();
    let cap = capture();
    let _guard = tracing::subscriber::set_default(cap.clone());

    let n = walk(g.path()).sort(Sort::ByName).count();
    assert_eq!(n, 3, "根+sub+f.txt 全产出");
    cap.assert_event(
        "skeleton::fsx::walk",
        Level::DEBUG,
        &["depth=1", "sort=ByName"],
    );
    assert_eq!(cap.len(), 1, "根帧零事件：下钻目录每帧恰一条");
}

#[test]
fn fsx_copy_per_item_debug_events() {
    let g = data_guard("copy");
    let src = g.path().join("src");
    fs::create_dir(&src).unwrap();
    fs::write(src.join("a.txt"), b"good").unwrap();
    fs::create_dir(src.join("d1")).unwrap();
    fs::write(src.join("d1").join("inner.txt"), b"inner").unwrap();
    let cap = capture();
    let _guard = tracing::subscriber::set_default(cap.clone());

    // ByName 序：a.txt(file) → d1(dir) → d1/inner.txt(file)；items 含 dst 根
    //（report 初始 dirs=1），bytes 逐文件累计。
    let report = copy_tree(&src, g.path().join("dst"), SymlinkPolicy::Preserve).unwrap();
    assert_eq!((report.dirs, report.files, report.bytes), (2, 2, 9));
    cap.assert_event(
        "skeleton::fsx::copy",
        Level::DEBUG,
        &["kind=dir", "items=3", "bytes=4"],
    );
    cap.assert_event(
        "skeleton::fsx::copy",
        Level::DEBUG,
        &["kind=file", "items=4", "bytes=9"],
    );
}

// symlink 逐项事件：无特权环境自动跳过（tests/fsx.rs try_dir_link 同纪律）。
#[test]
fn fsx_copy_symlink_item_debug_event() {
    let g = data_guard("symlink");
    let src = g.path().join("src");
    fs::create_dir(&src).unwrap();
    fs::write(src.join("t.txt"), b"hi").unwrap();
    let link = src.join("z-link");
    #[cfg(unix)]
    let ok = std::os::unix::fs::symlink("t.txt", &link).is_ok();
    #[cfg(windows)]
    let ok = {
        let ok = std::os::windows::fs::symlink_file("t.txt", &link).is_ok();
        if !ok {
            eprintln!("SKIP（无 symlink 特权）: {link:?}");
        }
        ok
    };
    if !ok {
        return;
    }
    let cap = capture();
    let _guard = tracing::subscriber::set_default(cap.clone());

    // ByName 序：t.txt(file, items=2) → z-link(symlink, items=3)。
    let report = copy_tree(&src, g.path().join("dst"), SymlinkPolicy::Preserve).unwrap();
    assert_eq!(report.symlinks, 1);
    cap.assert_event(
        "skeleton::fsx::copy",
        Level::DEBUG,
        &["kind=symlink", "items=3", "bytes=2"],
    );
}

// 埋点「部分失败」：Partial → warn（copied 已复制计数/path 定位/error 原文；
// 触发面同 tests/fsx.rs 先例：unix=chmod 000，windows=排他句柄）。
#[cfg(unix)]
#[test]
fn fsx_copy_partial_failure_warns() {
    use std::os::unix::fs::PermissionsExt;
    let g = data_guard("partial");
    let src = g.path().join("src");
    fs::create_dir(&src).unwrap();
    fs::write(src.join("a-ok.txt"), b"good").unwrap();
    fs::write(src.join("b-noperm.txt"), b"secret").unwrap();
    fs::set_permissions(src.join("b-noperm.txt"), fs::Permissions::from_mode(0o000)).unwrap();
    let cap = capture();
    let _guard = tracing::subscriber::set_default(cap.clone());

    assert!(
        copy_tree(&src, g.path().join("dst"), SymlinkPolicy::Preserve).is_err(),
        "无权限文件首错即停"
    );
    // copied=dst 根目录（report 初始 dirs=1）+ 1 文件。
    cap.assert_event(
        "skeleton::fsx::copy",
        Level::WARN,
        &["copied=2", "path=", "error="],
    );
    fs::set_permissions(src.join("b-noperm.txt"), fs::Permissions::from_mode(0o644)).unwrap();
}

// Windows 同款：排他句柄（share_mode=0）致 sharing violation → Partial。
#[cfg(windows)]
#[test]
fn fsx_copy_partial_failure_warns() {
    use std::os::windows::fs::OpenOptionsExt;
    let g = data_guard("partial");
    let src = g.path().join("src");
    fs::create_dir(&src).unwrap();
    fs::write(src.join("a-ok.txt"), b"good").unwrap();
    fs::write(src.join("b-locked.txt"), b"locked").unwrap();
    let _held = fs::OpenOptions::new()
        .read(true)
        .share_mode(0)
        .open(src.join("b-locked.txt"))
        .unwrap();
    let cap = capture();
    let _guard = tracing::subscriber::set_default(cap.clone());

    assert!(copy_tree(&src, g.path().join("dst"), SymlinkPolicy::Preserve).is_err());
    cap.assert_event(
        "skeleton::fsx::copy",
        Level::WARN,
        &["copied=2", "path=", "error="],
    );
}

#[test]
fn fsx_tempdir_drop_cleanup_failure_does_not_call_host_subscriber() {
    let g = data_guard("tmpdrop");
    let tmp = temp_dir_in(g.path(), "victim-").unwrap();
    fs::remove_dir_all(tmp.path()).unwrap(); // 树被外部先删 → Drop 内 rm_all 必败
    let cap = capture();
    let _guard = tracing::subscriber::set_default(cap.clone());
    drop(tmp);
    assert_eq!(
        cap.len(),
        0,
        "TempDir::drop must use only its fallible best-effort reporter"
    );

    // 对照面：清理成功同样不发事件。
    let clean = temp_dir_in(g.path(), "clean-").unwrap();
    drop(clean);
    assert_eq!(cap.len(), 0, "Drop paths never invoke the host subscriber");
}

// ---- iter 域（target skeleton::iter::process_results；首 Err 短路=异常路径
// warn，pos=已产出 Ok 数）----

#[test]
fn iter_process_results_short_circuit_warns_with_pos() {
    let cap = capture();
    let _guard = tracing::subscriber::set_default(cap.clone());

    // next 通路：for 循环逐个拉取，两 Ok 后遇 Err（pos=2）。
    let got = process_results([Ok(1u8), Ok(2), Err("boom"), Ok(3)], |it| {
        let mut got = Vec::new();
        for x in it {
            got.push(x);
        }
        got
    });
    assert_eq!(got, Err("boom"));
    cap.assert_event("skeleton::iter::process_results", Level::WARN, &["pos=2"]);

    // fold 快路径：首项即 Err（pos=0）。fold 本身是被测通路（iter 域 fold
    // 装配形态），不用 sum() 替换——clippy unnecessary_fold 在此 allow。
    #[allow(clippy::unnecessary_fold)]
    let folded: Result<i32, &str> =
        process_results([Err::<i32, _>("early")], |it| it.fold(0, |a, x| a + x));
    assert_eq!(folded, Err("early"));
    cap.assert_event("skeleton::iter::process_results", Level::WARN, &["pos=0"]);
}

// ---- scheduler 域（target skeleton::scheduler::*；批 5-2 自 skeleton-infra 迁入）----

#[test]
fn scheduler_register_cancel_fire_events() {
    let cap = capture();
    let clock = Arc::new(MockClock::new());
    let sched = Scheduler::new(clock.clone());
    let _guard = tracing::subscriber::set_default(cap.clone());

    let h = sched.after(Duration::from_millis(100), || {}).unwrap();
    cap.assert_event(
        "skeleton::scheduler::register",
        Level::DEBUG,
        &["seq=0", "kind=after", "delay_ms=100"],
    );

    assert!(h.cancel(), "首次取消成功");
    cap.assert_event(
        "skeleton::scheduler::cancel",
        Level::DEBUG,
        &["cancelled=true"],
    );
    assert!(!h.cancel(), "cancel 幂等");
    cap.assert_event(
        "skeleton::scheduler::cancel",
        Level::DEBUG,
        &["cancelled=false"],
    );

    let _h2 = sched.every(Duration::from_millis(50), || {}).unwrap();
    cap.assert_event(
        "skeleton::scheduler::register",
        Level::DEBUG,
        &["seq=1", "kind=every", "delay_ms=50"],
    );
    clock.advance(Duration::from_millis(50));
    assert_eq!(sched.tick(), 1, "周期任务首火");
    cap.assert_event(
        "skeleton::scheduler::fire",
        Level::DEBUG,
        &["seq=1", "kind=every"],
    );
}

/// Drop 走静默取消路径（#221 §2.5）：析构不发 cancel 事件，取消语义不变。
#[test]
fn scheduler_drop_cancel_is_silent() {
    let cap = capture();
    let clock = Arc::new(MockClock::new());
    let sched = Scheduler::new(clock.clone());
    let _guard = tracing::subscriber::set_default(cap.clone());

    // 对照臂：显式 cancel 仍发一条。
    let h = sched.after(Duration::from_millis(10), || {}).unwrap();
    assert!(h.cancel(), "首次取消成功");
    cap.assert_event(
        "skeleton::scheduler::cancel",
        Level::DEBUG,
        &["cancelled=true"],
    );

    // armed 句柄直接 Drop：零事件（宿主回调不得在析构里同步执行）。
    let h2 = sched.after(Duration::from_millis(10), || {}).unwrap();
    let before_drop = cap.len();
    drop(h2);
    assert_eq!(
        cap.len(),
        before_drop,
        "Drop 发了事件（events len {} → {}）",
        before_drop,
        cap.len()
    );

    // 但取消语义照旧：到期不点火（`h` 也是取消态）。
    clock.advance(Duration::from_millis(10));
    assert_eq!(sched.tick(), 0, "Drop 已静默取消该任务");
}

#[test]
fn scheduler_register_after_close_warns() {
    let cap = capture();
    // 订阅器先装再动调度器：本文件另有一处 `set_global_default`（跨线程捕获
    // 用），先发事件会漏进那个全局捕获面并污染它的精确条数断言。
    let _guard = tracing::subscriber::set_default(cap.clone());
    let sched = Scheduler::new(Arc::new(MockClock::new()));
    sched.close();
    assert!(sched.after(Duration::from_millis(1), || {}).is_err());
    cap.assert_event(
        "skeleton::scheduler::register",
        Level::WARN,
        &["kind=after", "reason=closed"],
    );
}

#[test]
fn scheduler_tombstone_skip_is_debug() {
    let cap = capture();
    let clock = Arc::new(MockClock::new());
    let sched = Scheduler::new(clock.clone());
    let _guard = tracing::subscriber::set_default(cap.clone());
    let h = sched.after(Duration::from_millis(50), || {}).unwrap();
    h.cancel();
    clock.advance(Duration::from_millis(50));
    assert_eq!(sched.tick(), 0, "出堆前已 cancel：墓碑静默丢弃不执行");
    cap.assert_event("skeleton::scheduler::fire", Level::DEBUG, &["skipped=true"]);
}

#[test]
fn scheduler_overload_rearm_warns() {
    let cap = capture();
    let clock = Arc::new(MockClock::new());
    let sched = Scheduler::new(clock.clone());
    let _guard = tracing::subscriber::set_default(cap.clone());
    // 回调内推进虚拟钟越过下锚（完成时刻已越过 prev+period）⇒ 超载退化重臂。
    let c2 = Arc::clone(&clock);
    sched
        .every(Duration::from_millis(100), move || {
            c2.advance(Duration::from_millis(250));
        })
        .unwrap()
        .detach();
    clock.advance(Duration::from_millis(100));
    assert_eq!(sched.tick(), 1);
    cap.assert_event(
        "skeleton::scheduler::fire",
        Level::WARN,
        &["kind=every", "reason=overload_rearm", "period_ms=100"],
    );
}

/// issue#149：`delay_ms`/`period_ms` 越过 `u64::MAX` 毫秒 ⇒ 饱和到 `u64::MAX`
/// （裸 `as u64` 会把 2^64 ms 回绕成 0，遥测谎报「零延迟」）。
#[test]
fn scheduler_duration_fields_saturate_at_u64_max() {
    let cap = capture();
    let clock = Arc::new(MockClock::new());
    let sched = Scheduler::new(clock.clone());
    let _guard = tracing::subscriber::set_default(cap.clone());
    // 2^64 ms：恰越 u64::MAX（裸转换回绕成 0），仍远在 Instant 表示范围内。
    let huge = Duration::from_millis(u64::MAX).saturating_add(Duration::from_millis(1));
    let delay_field = format!("delay_ms={}", u64::MAX);
    let period_field = format!("period_ms={}", u64::MAX);

    let pending = sched.after(huge, || {}).unwrap();
    cap.assert_event(
        "skeleton::scheduler::register",
        Level::DEBUG,
        &["kind=after", delay_field.as_str()],
    );
    assert!(pending.cancel(), "清掉堆内条目，下轮 tick 只点火周期任务");

    // 超载退化重臂：回调内推进虚拟钟越过下锚 ⇒ 同一 huge 周期落进 period_ms。
    let c2 = Arc::clone(&clock);
    sched
        .every(huge, move || c2.advance(huge))
        .unwrap()
        .detach();
    clock.advance(huge);
    assert_eq!(sched.tick(), 1, "周期任务首火");
    cap.assert_event(
        "skeleton::scheduler::fire",
        Level::WARN,
        &["kind=every", "reason=overload_rearm", period_field.as_str()],
    );
}

#[test]
fn scheduler_callback_panic_warns() {
    let cap = capture();
    let sched = Scheduler::new(Arc::new(MockClock::new()));
    let _guard = tracing::subscriber::set_default(cap.clone());
    sched
        .after(Duration::ZERO, || panic!("boom"))
        .unwrap()
        .detach();
    assert_eq!(sched.tick(), 1, "panic 已被摘除，点火计数照记");
    assert_eq!(sched.panic_count(), 1);
    cap.assert_event(
        "skeleton::scheduler::fire",
        Level::WARN,
        &["kind=after", "reason=panic"],
    );
}

#[test]
fn scheduler_close_is_info_with_cleared_count() {
    let cap = capture();
    // 订阅器先装再动调度器：先发事件会漏进另一处 `set_global_default` 的
    // 全局捕获面（同 `scheduler_register_after_close_warns`）。
    let _guard = tracing::subscriber::set_default(cap.clone());
    let sched = Scheduler::new(Arc::new(SystemClock));
    sched
        .after(Duration::from_secs(3600), || {})
        .unwrap()
        .detach();
    sched.close();
    cap.assert_event("skeleton::scheduler::close", Level::INFO, &["cleared=1"]);
}

// ---- scheduler 域：宿主订阅器 panic 不得打断状态机（#194 / #221 §3.1）----

/// 命中指定 target 的订阅器：`event` 直接 panic，模拟宿主遥测故障。
struct PanicOnTarget {
    targets: &'static [&'static str],
    panicked: Arc<AtomicUsize>,
}

impl Subscriber for PanicOnTarget {
    fn enabled(&self, _: &Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }
    fn record(&self, _: &Id, _: &Record<'_>) {}
    fn record_follows_from(&self, _: &Id, _: &Id) {}
    fn event(&self, event: &Event<'_>) {
        if self.targets.contains(&event.metadata().target()) {
            self.panicked.fetch_add(1, Ordering::SeqCst);
            panic!(
                "injected host subscriber panic on {}",
                event.metadata().target()
            );
        }
    }
    fn enter(&self, _: &Id) {}
    fn exit(&self, _: &Id) {}
}

/// #194 item 5：装一个在 register/fire/close/cancel 事件上 panic 的订阅器，
/// 断言任务回调/退役/重臂/取消结果/唤醒前提（额度释放）语义全不变——
/// 遥测只许是观测面，不得成为状态转移的权威。
#[test]
fn scheduler_state_machine_survives_panicking_subscriber() {
    let clock = Arc::new(MockClock::new());
    let sched = Scheduler::new(clock.clone());
    sched.set_capacity(1); // 有界准入：额度账目同时被本测试钉住
    let panicked = Arc::new(AtomicUsize::new(0));
    let before = std_ext::embedded_event_panic_count();
    let _guard = tracing::subscriber::set_default(PanicOnTarget {
        targets: &[
            "skeleton::scheduler::register",
            "skeleton::scheduler::fire",
            "skeleton::scheduler::close",
            "skeleton::scheduler::cancel",
        ],
        panicked: Arc::clone(&panicked),
    });

    // register：事件 panic 不得把已提交的注册变成错误，也不得吞掉句柄。
    let hits = Arc::new(AtomicUsize::new(0));
    let hits2 = Arc::clone(&hits);
    let handle = sched
        .after(Duration::from_millis(10), move || {
            hits2.fetch_add(1, Ordering::SeqCst);
        })
        .expect("订阅器 panic 不得把已受理的注册变成错误");
    assert_eq!(sched.live_registrations(), 1, "注册已提交且额度恰占一条");
    assert_eq!(
        sched.try_after(Duration::from_millis(10), || {}).err(),
        Some(AdmissionError::Full),
        "触顶判定不受订阅器 panic 影响"
    );

    // fire：事件不得插进出堆与回调之间 ⇒ 回调照跑、终态照落、额度恰好一次。
    clock.advance(Duration::from_millis(10));
    assert_eq!(sched.tick(), 1, "被弹出的任务必须照常点火");
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    assert_eq!(sched.live_registrations(), 0, "退役后额度恰好释放一次");
    assert_eq!(
        sched.panic_count(),
        0,
        "回调没 panic：计数只记真实回调 panic"
    );
    assert!(!handle.cancel(), "已点火（DONE 终态）取消无效");
    drop(handle);

    // 周期重臂：超载 warn 的订阅器 panic 不得阻止重臂（下锚已先发布）。
    let c2 = Arc::clone(&clock);
    let every = sched
        .every(Duration::from_millis(100), move || {
            c2.advance(Duration::from_millis(250));
        })
        .expect("额度已释放 ⇒ cap=1 仍可接纳（无容量滞留）");
    clock.advance(Duration::from_millis(100));
    assert_eq!(sched.tick(), 1, "周期首火");
    clock.advance(Duration::from_millis(250));
    assert_eq!(sched.tick(), 1, "订阅器 panic 后周期任务仍已重臂");
    assert_eq!(sched.live_registrations(), 1, "重臂只占原有一条注册");

    // cancel：事件 panic 不得替换返回的取消结果与幂等语义。
    assert!(every.cancel(), "取消必须返回真实结果");
    assert!(!every.cancel(), "幂等语义不变");
    assert_eq!(sched.live_registrations(), 0);
    drop(every);

    // close：事件 panic 不得跳过退役/唤醒 —— 关闸仍清空 pending 并释放额度。
    sched.close();
    assert!(sched.is_closed());
    assert_eq!(sched.live_registrations(), 0, "关闸后零残留");
    assert!(
        sched.after(Duration::from_millis(1), || {}).is_err(),
        "关闸后注册显式拒绝"
    );

    let hits_of_panic = panicked.load(Ordering::SeqCst);
    assert!(
        hits_of_panic >= 5,
        "订阅器确实在五类事件上 panic 过（实际 {hits_of_panic} 次）"
    );
    let after = std_ext::embedded_event_panic_count();
    assert!(
        after > before,
        "containment 计数器必须前进（{before} → {after}）"
    );
}
/// #194 item 2 / #221 §3.1：fire 事件不得插在出堆与回调执行之间——事件到达
/// 时回调必须已经跑完（时序订阅方据此推断状态，不能看到"已出堆未执行"）。
#[test]
fn scheduler_fire_event_arrives_after_callback() {
    struct FireOrderProbe {
        hits: Arc<AtomicUsize>,
        seen_at_hits: Arc<Mutex<Vec<usize>>>,
    }

    impl Subscriber for FireOrderProbe {
        fn enabled(&self, _: &Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _: &Attributes<'_>) -> Id {
            Id::from_u64(1)
        }
        fn record(&self, _: &Id, _: &Record<'_>) {}
        fn record_follows_from(&self, _: &Id, _: &Id) {}
        fn event(&self, event: &Event<'_>) {
            if event.metadata().target() == "skeleton::scheduler::fire"
                && *event.metadata().level() == Level::DEBUG
            {
                self.seen_at_hits
                    .lock()
                    .expect("probe mutex")
                    .push(self.hits.load(Ordering::SeqCst));
            }
        }
        fn enter(&self, _: &Id) {}
        fn exit(&self, _: &Id) {}
    }

    let clock = Arc::new(MockClock::new());
    let sched = Scheduler::new(clock.clone());
    let hits = Arc::new(AtomicUsize::new(0));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let _guard = tracing::subscriber::set_default(FireOrderProbe {
        hits: Arc::clone(&hits),
        seen_at_hits: Arc::clone(&seen),
    });
    let hits2 = Arc::clone(&hits);
    sched
        .after(Duration::from_millis(10), move || {
            hits2.fetch_add(1, Ordering::SeqCst);
        })
        .expect("未关闸注册必成")
        .detach();

    clock.advance(Duration::from_millis(10));
    assert_eq!(sched.tick(), 1);
    assert_eq!(
        *seen.lock().expect("probe mutex"),
        vec![1],
        "fire 事件到达时回调必已跑完（否则事件插在出堆与执行之间）"
    );
}

#[test]
fn worker_teardown_does_not_emit_synchronous_host_event() {
    let cap = capture();
    // 本测试二进制唯一全局挂载点：close 的调用方事件落全局。
    tracing::subscriber::set_global_default(cap.clone()).expect("全局订阅器只设一次");
    let sched = Arc::new(Scheduler::new(Arc::new(SystemClock)));
    let worker = sched.spawn_worker();
    worker.join();
    cap.assert_event("skeleton::scheduler::close", Level::INFO, &["cleared=0"]);
    // 断言面**不能**用进程级 `len()`：全局订阅器会收到同二进制其它测试/线程的
    // 事件（曾因此偶红 left:2）。收敛到本测试关心的事件（target+level 恰一条
    // = 调用方 close），再按发射线程归因判「worker 收尾零事件」——后者才是
    // #221 §2.5 的原意，且对任何 target 的新增事件都有效。
    assert_eq!(
        cap.count_target("skeleton::scheduler::close", Level::INFO),
        1,
        "只应有调用方 close 一条事件（worker 收尾零事件）"
    );
    assert_eq!(
        cap.count_from_thread("scheduler-worker"),
        0,
        "worker teardown must not synchronously invoke the host subscriber"
    );
}

// ---- ratelimit 域（target skeleton::ratelimit::*；批 5-2 自 skeleton-infra 迁入）----

#[test]
fn ratelimit_try_acquire_rejections_warn_and_success_silent() {
    let cap = capture();
    let clock = Arc::new(MockClock::new());
    let bucket = TokenBucket::new(clock.clone(), 2.0, 1.0);
    let _guard = tracing::subscriber::set_default(cap.clone());

    assert!(bucket.try_acquire(5).is_err(), "超容量立即拒绝");
    cap.assert_event(
        "skeleton::ratelimit::try_acquire",
        Level::WARN,
        &["permits=5", "reason=exceeds_capacity"],
    );

    assert!(bucket.try_acquire(2).is_ok(), "满桶突发 2");
    let err = bucket.try_acquire(1).unwrap_err();
    assert_eq!(err.wait(), Duration::from_secs(1), "缺 1 个 ÷ 1/s");
    cap.assert_event(
        "skeleton::ratelimit::try_acquire",
        Level::WARN,
        &["permits=1", "reason=insufficient", "available=0.0"],
    );

    clock.advance(Duration::from_secs(1));
    let before = cap.len();
    assert!(bucket.try_acquire(1).is_ok(), "回填后成功");
    assert_eq!(cap.len(), before, "成功路径零埋点事件（只埋决策点）");
}

#[test]
fn ratelimit_acquire_blocks_with_round_debug() {
    let cap = capture();
    let clock = Arc::new(MockClock::new());
    let bucket = TokenBucket::new(clock.clone(), 1.0, 1.0);
    let _guard = tracing::subscriber::set_default(cap.clone());
    assert!(bucket.try_acquire(1).is_ok(), "清空满桶");
    bucket
        .acquire(1)
        .expect("阻塞睡到回填足额（MockClock 虚拟推进）");
    assert_eq!(clock.unix_offset(), Duration::from_secs(1));
    cap.assert_event(
        "skeleton::ratelimit::acquire",
        Level::DEBUG,
        &["permits=1", "round=1", "deficit=1.0"],
    );

    assert!(
        bucket.acquire(5).is_err(),
        "超容量立即拒绝（等待也无法满足）"
    );
    cap.assert_event(
        "skeleton::ratelimit::acquire",
        Level::WARN,
        &["permits=5", "reason=exceeds_capacity"],
    );
}

// ---- retry 域（target skeleton::retry::*；批 5-2 自 skeleton-infra 迁入）----

#[test]
fn retry_attempt_start_and_retry_verdict() {
    let cap = capture();
    let clock = MockClock::new();
    let policy = RetryPolicy::new(3, Duration::from_millis(100), Duration::from_secs(10));
    let mut remaining = 2u32;
    let _guard = tracing::subscriber::set_default(cap.clone());
    let res = retry_if(
        || {
            if remaining > 0 {
                remaining -= 1;
                Err("transient")
            } else {
                Ok(7u32)
            }
        },
        &policy,
        &clock,
        |_| true,
    );
    assert_eq!(res.expect("两次失败后成功"), 7);
    cap.assert_event("skeleton::retry::attempt", Level::DEBUG, &["attempt=1"]);
    cap.assert_event("skeleton::retry::attempt", Level::DEBUG, &["attempt=3"]);
    // 原 `skeleton::backoff` 的计算入参在这里可观测（issue#202：观测面移到调用点）。
    cap.assert_event(
        "skeleton::retry::verdict",
        Level::WARN,
        &[
            "attempt=1",
            "decision=retry",
            "base_ms=100",
            "max_ms=10000",
            "backoff_ms=100",
        ],
    );
    cap.assert_event(
        "skeleton::retry::verdict",
        Level::WARN,
        &["attempt=2", "decision=retry", "backoff_ms=200"],
    );
}

/// 不推进时间的 Sleeper：遥测值域断言不关心时间线（MAX 量级退避在 MockClock
/// 上会因 `Instant` 表示范围 fail-closed panic，此处只吞掉睡眠请求）。
struct FrozenClock(MockClock);

impl Clock for FrozenClock {
    fn now_instant(&self) -> Instant {
        self.0.now_instant()
    }
    fn now_system(&self) -> SystemTime {
        self.0.now_system()
    }
}

impl Sleeper for FrozenClock {
    fn sleep(&self, _d: Duration) {}
}

// issue#149：duration→u64 遥测字段饱和到 u64::MAX，不回绕成「小值/零延迟」。
#[test]
fn retry_telemetry_saturates_beyond_u64() {
    const U64_MAX: u64 = u64::MAX;
    let cap = capture();
    // base = u64::MAX 秒（毫秒量级远超 u64::MAX）> max = 1s ⇒ 既触发截平事件，
    // 又让 base_ms/max_ms/backoff_ms/linear_ms/capped_ms 全部走到饱和面。
    let policy = RetryPolicy::new(2, Duration::from_secs(U64_MAX), Duration::from_secs(1))
        .with_jitter(|d| d);
    let _guard = tracing::subscriber::set_default(cap.clone());
    let res = retry_if(
        || Err::<(), &str>("x"),
        &policy,
        &FrozenClock(MockClock::new()),
        |_| true,
    );
    assert!(res.is_err());
    cap.assert_event(
        "skeleton::retry::verdict",
        Level::WARN,
        &[
            "decision=retry",
            &format!("base_ms={U64_MAX}"),
            "max_ms=1000",
            "backoff_ms=1000",
        ],
    );
    cap.assert_event(
        "skeleton::retry::backoff",
        Level::WARN,
        &[&format!("linear_ms={U64_MAX}"), "capped_ms=1000"],
    );
}

// issue#149 / #148：ratelimit 的 wait_ns 同样饱和（饱和等待 = u64::MAX 纳秒）。
#[test]
fn ratelimit_wait_ns_saturates() {
    const U64_MAX: u64 = u64::MAX;
    let cap = capture();
    let clock = Arc::new(MockClock::new());
    let bucket = TokenBucket::new(clock, 1.0, 1e-30);
    let _guard = tracing::subscriber::set_default(cap.clone());
    bucket.try_acquire(1).expect("清空满桶");
    let err = bucket.try_acquire(1).expect_err("空桶必拒");
    assert_eq!(err.wait(), Duration::MAX);
    cap.assert_event(
        "skeleton::ratelimit::try_acquire",
        Level::WARN,
        &["reason=insufficient", &format!("wait_ns={U64_MAX}")],
    );
}

#[test]
fn retry_backoff_capped_warns() {
    let cap = capture();
    let clock = MockClock::new();
    // base(300ms) > max(250ms)：首次退避即截平（线性 300 → 截 250）。
    let policy = RetryPolicy::new(2, Duration::from_millis(300), Duration::from_millis(250));
    let mut remaining = 1u32;
    let _guard = tracing::subscriber::set_default(cap.clone());
    let res = retry_if(
        || {
            if remaining > 0 {
                remaining -= 1;
                Err("x")
            } else {
                Ok(())
            }
        },
        &policy,
        &clock,
        |_| true,
    );
    assert!(res.is_ok());
    cap.assert_event(
        "skeleton::retry::backoff",
        Level::WARN,
        &["attempt=1", "linear_ms=300", "capped_ms=250"],
    );
}

#[test]
fn retry_stop_verdicts_are_info() {
    let cap = capture();
    let clock = MockClock::new();
    let _guard = tracing::subscriber::set_default(cap.clone());

    // 判定器拒绝：attempt=1 即停（stop 终态 info，零睡眠）。
    let policy = RetryPolicy::new(5, Duration::from_millis(10), Duration::from_secs(1));
    let res = retry_if(
        || Err::<u32, &str>("fatal"),
        &policy,
        &clock,
        |e: &&str| *e == "transient",
    );
    assert!(res.is_err());
    cap.assert_event(
        "skeleton::retry::verdict",
        Level::INFO,
        &["attempt=1", "decision=stop_non_retryable"],
    );

    // 次数到顶：attempt=2 停。
    let policy2 = RetryPolicy::new(2, Duration::from_millis(10), Duration::from_secs(1));
    let res2 = retry_if(|| Err::<u32, &str>("e"), &policy2, &clock, |_| true);
    assert!(res2.is_err());
    cap.assert_event(
        "skeleton::retry::verdict",
        Level::INFO,
        &["attempt=2", "decision=stop_max_attempts"],
    );

    // 预算耗尽：带 elapsed 字段（退避线性 100+200=300ms 越过 150ms 预算，
    // 第 3 次失败后停）。
    let policy3 = RetryPolicy::new(10, Duration::from_millis(100), Duration::from_secs(10))
        .with_budget(Duration::from_millis(150))
        .with_jitter(|d| d); // bd 514 授权边界：恒等源保 elapsed_ms 确定断言
    let res3 = retry_if(|| Err::<u32, &str>("e"), &policy3, &clock, |_| true);
    assert!(res3.is_err());
    cap.assert_event(
        "skeleton::retry::verdict",
        Level::INFO,
        &["decision=stop_out_of_budget", "elapsed_ms=300"],
    );
}

// ---- cache 域（target skeleton::cache[::phase]；流转=debug / 显式清扫=info；
// 事件不带键身份字段（issue#203：遥测不得为已提交变更再跑用户 Hash）；
// 批 5-2 自 skeleton-infra 迁入）----

#[test]
fn cache_lru_capacity_evict_is_debug_with_lru_tail() {
    let cap = capture();
    let _guard = tracing::subscriber::set_default(cap.clone());
    let mut c = LruCache::new(2);
    c.insert("a", 1);
    c.insert("b", 2);
    assert_eq!(cap.len(), 0, "未满插入零事件");

    c.insert("c", 3); // 满：逐 LRU 尾 a（b 更新）
    cap.assert_event(
        "skeleton::cache::evict",
        Level::DEBUG,
        &["kind=capacity", "order=lru_tail", "len_at_evict=1"],
    );
    assert_eq!(cap.len(), 1, "容量淘汰恰一条事件");
    assert_eq!(c.get(&"a"), None, "被逐者恒 LRU 尾");
    assert_eq!(c.get(&"b"), Some(&2));
}

#[test]
fn cache_ttl_lazy_and_sweep_events() {
    let cap = capture();
    let clock = Arc::new(MockClock::new());
    let mut c = TtlCache::new(clock.clone(), 2);
    let _guard = tracing::subscriber::set_default(cap.clone());

    // 惰性单条：过期触碰即回收（键指纹+过期累计）。
    c.insert("k", 1, Duration::from_millis(100));
    clock.advance(Duration::from_millis(100));
    assert_eq!(c.get(&"k"), None, "过期读=回收+None");
    cap.assert_event(
        "skeleton::cache::evict",
        Level::DEBUG,
        &["kind=ttl", "expired_total=1"],
    );

    // 满插清扫：双僵尸一次清空，真回收才发（zombies=2；本次零容量淘汰）。
    c.insert("x", 1, Duration::from_millis(50));
    c.insert("y", 2, Duration::from_millis(50));
    clock.advance(Duration::from_millis(50));
    let before = cap.len();
    c.insert("z", 3, Duration::from_millis(50));
    cap.assert_event(
        "skeleton::cache::evict",
        Level::DEBUG,
        &["kind=ttl_sweep", "zombies=2"],
    );
    assert_eq!(
        cap.len(),
        before + 1,
        "清扫+插入共两事件（容量未满不再逐出）"
    );
}

#[test]
fn cache_ttl_site_capacity_and_purge_events() {
    let cap = capture();
    let clock = Arc::new(MockClock::new());
    let mut c = TtlCache::new(clock.clone(), 2);
    let _guard = tracing::subscriber::set_default(cap.clone());

    // TtlCache 站点容量淘汰：无一过期可清 → 逐 LRU 尾（len_at_evict=残留）。
    c.insert("a", 1, Duration::from_secs(3600));
    c.insert("b", 2, Duration::from_secs(3600));
    clock.advance(Duration::from_millis(1));
    c.insert("c", 3, Duration::from_secs(3600));
    cap.assert_event(
        "skeleton::cache::evict",
        Level::DEBUG,
        &["kind=capacity", "order=lru_tail", "len_at_evict=1"],
    );
    assert_eq!(c.get(&"a"), None, "被逐者恒 LRU 尾（a 最旧）");

    // purge：显式清扫 info（回收数/残留）。
    clock.advance(Duration::from_secs(3600)); // b、c 全过期成僵尸
    assert_eq!(c.purge_expired(), 2);
    cap.assert_event(
        "skeleton::cache::purge",
        Level::INFO,
        &["purged=2", "len_after=0"],
    );
}
