//! paths 分配事件锚回归（B1 盲评卡点：doc 分配声明与实测逐字一致）。
//!
//! 本文件自带**事件计数**分配器（alloc/realloc/alloc_zeroed 各计 1 次事件，
//! 只挂本集成测试二进制——lib 测试二进制禁挂全局分配器，tests/memgov.rs
//! 纪律）。事件口径是硬约束：`memgov::CountingAllocator` 的 live 净计数测
//! 不到窗内生灭的瞬时分配——simplify 保留名判定里的 String 瞬时件恰属此类
//! （B1 盲评卡点 1 的实测口径即 events）。测试函数内同步单线程读数窗。

use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard};

use std_ext::paths::{cache_dir, config_dir, data_dir, home_dir, normalize_lexical, simplify};

static EVENTS: AtomicUsize = AtomicUsize::new(0);

/// 文件内串行闸（tests/memgov.rs 同款：libtest 并行线程共享全局事件计数，
/// 窗被同二进制并发分配污染 = flaky-by-construction）。
///
/// 注意（终验波 B1）：本 Mutex 只串行**本文件**测试；真污染源=同二进制
/// libtest 主线程（结果记录/输出分配）落在计数窗内——Mutex 拦不住主线程
/// （它不持锁），并行整跑仍可 flake（实测 5 跑 2 红；其他 --test 目标是
/// 独立进程，计数器不共享，非污染源）。
/// 确定口径=CI 单列步 `--test-threads=1`（ci.yml "paths_alloc serial"），
/// 本机复跑同口径：`cargo test -p udf-skeleton-infra --all-features --test
/// paths_alloc -- --test-threads=1`。
static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

struct EventCounting;

// SAFETY: 纯转发 System，仅在转发前累加事件计数；null 原样透传（alloc 契约）。
unsafe impl GlobalAlloc for EventCounting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        EVENTS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        EVENTS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        EVENTS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static COUNTING: EventCounting = EventCounting;

/// op 执行期的分配事件数（瞬时生灭也计入；结果活到函数尾，drop 不进窗）。
fn allocs<T>(op: impl FnOnce() -> T) -> usize {
    let before = EVENTS.load(Ordering::Relaxed);
    let out = op();
    black_box(&out);
    EVENTS.load(Ordering::Relaxed) - before
}

/// doc 分配声明逐字背书：simplify legacy 直通零事件 + normalize 事件数。
#[test]
fn allocation_anchors_match_doc() {
    let _serial = serial();
    // 预热：std/测试框架懒初始化出窗
    black_box(simplify(Path::new(r"C:\warmup")));
    black_box(normalize_lexical(Path::new("warm/up")));

    assert_eq!(
        allocs(|| simplify(Path::new(r"C:\foo\bar\baz"))),
        0,
        "simplify legacy 直通须零分配（doc：恒返借用切片）"
    );
    assert_eq!(
        allocs(|| normalize_lexical(Path::new("a/./b/../c"))),
        2,
        "normalize 脏输入=2 次分配（组件栈 Vec + 输出 PathBuf）"
    );
    assert_eq!(
        allocs(|| normalize_lexical(Path::new("a/b/c"))),
        2,
        "已归一输入同价（无快路径，如实声明）"
    );
    assert_eq!(
        allocs(|| normalize_lexical(Path::new(""))),
        1,
        "空输入 1 次（组件栈容量预留）"
    );
    assert_eq!(
        allocs(|| normalize_lexical(Path::new("."))),
        2,
        "`.`→`.` 2 次（组件栈+标记 PathBuf）"
    );
}

/// verbatim 全链零分配（Windows 面）：逐组件过保留名判定——原
/// `to_ascii_uppercase` 每组件 2 次瞬时 String 的证伪现场（2/4/8 组件线性），
/// 修后归零。
#[cfg(windows)]
#[test]
fn verbatim_hot_path_zero_alloc() {
    let _serial = serial();
    // 预热：懒初始化出窗
    black_box(simplify(Path::new(r"\\?\C:\warmup")));

    // 可剥离 verbatim：1/2/4/8 组件梯度（盲评卡点 1 的实测形状）
    for p in [
        r"\\?\C:\a",
        r"\\?\C:\a\b",
        r"\\?\C:\a\b\c\d",
        r"\\?\C:\a\b\c\d\e\f\g\h",
    ] {
        assert_eq!(
            allocs(|| simplify(Path::new(p))),
            0,
            "verbatim 可剥离零分配: {p}"
        );
    }
    // 保留名早退（卡点 432/437 现场：CON 首段命中 / COM±数字前缀命中）
    assert_eq!(
        allocs(|| simplify(Path::new(r"\\?\C:\con.tar.gz"))),
        0,
        "CON 早退零分配"
    );
    assert_eq!(
        allocs(|| simplify(Path::new(r"\\?\C:\lpt4"))),
        0,
        "LPT±数字早退零分配"
    );
    assert_eq!(
        allocs(|| simplify(Path::new(r"\\?\C:\com1"))),
        0,
        "小写 COM 早退零分配"
    );
    // normalize verbatim 透传 1 次（to_path_buf，doc 如实声明）
    assert_eq!(
        allocs(|| normalize_lexical(Path::new(r"\\?\C:\a\..\b"))),
        1,
        "verbatim 透传 1 次"
    );
}

// ===== home/XDG 域分配锚（env 注入臂；事件口径同上） =====

/// env 守卫：var_os 无损存还原（tests/paths_xdg.rs 同形）。
struct XdgEnvGuard {
    var: &'static str,
    saved: Option<std::ffi::OsString>,
}

impl XdgEnvGuard {
    fn save(var: &'static str) -> Self {
        Self {
            var,
            saved: std::env::var_os(var),
        }
    }
}

impl Drop for XdgEnvGuard {
    fn drop(&mut self) {
        match self.saved.take() {
            Some(v) => std::env::set_var(self.var, v),
            None => std::env::remove_var(self.var),
        }
    }
}

#[cfg(unix)]
const HOME_VAR: &str = "HOME";
#[cfg(windows)]
const HOME_VAR: &str = "USERPROFILE";

/// doc 分配声明逐字背书：home_dir env 优先臂=Unix 1 次（var_os 字节直拷
/// OsString）／Windows 2 次（env 块 UTF-16→WTF-8 转换多一次分配）；
/// PathBuf::from 移动零增；XDG 三函数 Windows 面恒 None 零事件。
#[test]
fn home_xdg_alloc_anchors_match_doc() {
    let _serial = serial();
    let _g = XdgEnvGuard::save(HOME_VAR);
    std::env::set_var(HOME_VAR, "C:\\xdg-alloc-home");
    // 预热：std env 懒初始化出窗
    black_box(home_dir());
    #[cfg(unix)]
    assert_eq!(allocs(home_dir), 1, "home_dir env 臂 1 次");
    #[cfg(windows)]
    assert_eq!(allocs(home_dir), 2, "home_dir env 臂 2 次（UTF-16 转换）");
    #[cfg(windows)]
    {
        // Windows 面：xdg_base_dir 忽略入参恒返 None，零事件
        assert_eq!(allocs(config_dir), 0, "Windows XDG 面零分配");
        assert_eq!(allocs(cache_dir), 0, "Windows XDG 面零分配");
        assert_eq!(allocs(data_dir), 0, "Windows XDG 面零分配");
    }
}

/// Unix 面 doc 分配声明逐字背书：XDG 绝对值臂 3 次（env OsString 1 +
/// normalize_lexical 2）、默认臂 4 次（home 1 + join 1 + normalize 2）。
/// 本测试在 Unix 面自证（Windows 开发机不可达，CI 覆盖）；env 臂 1 次的
/// 假设已由 home_xdg_alloc_anchors_match_doc 在 Windows 面实测背书。
#[cfg(not(windows))]
#[test]
fn xdg_unix_alloc_anchors_match_doc() {
    let _serial = serial();
    let _hg = XdgEnvGuard::save(HOME_VAR);
    let _cg = XdgEnvGuard::save("XDG_CONFIG_HOME");
    std::env::set_var(HOME_VAR, "/xdg-alloc-home");
    black_box(home_dir()); // 预热
    black_box(config_dir());
    std::env::set_var("XDG_CONFIG_HOME", "/xdg-alloc/cfg");
    assert_eq!(allocs(|| config_dir()), 3, "XDG 绝对值臂 3 次");
    std::env::remove_var("XDG_CONFIG_HOME");
    assert_eq!(allocs(|| config_dir()), 4, "默认臂 4 次");
}
