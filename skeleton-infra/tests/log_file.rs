//! log 文件落盘端到端验收（TDD；`--features log` 开跑）：
//!
//! ① init_with_file 生命周期：首调 Ok、二调 Err(AlreadyInit)；与既有
//!    init_tracing_from_env 并存（同进程二次初始化同样被拒，禁静默覆盖）；
//! ② 默认行格式 `[UTC 时间 LEVEL target] message key=value` 经真 fmt 层
//!    落盘断言；`skeleton=off` 预设过滤框架域（文件仅含 app 域行）；
//! ③ release 态 DefaultPathResolver：mock USERPROFILE/HOME → `.<project>/log`，
//!    且解析结果不得含 CARGO_MANIFEST_DIR（开发者路径泄漏禁；仅 `--release`
//!    编译执行——debug 构建走编译期锚分支，与本测试无关）。
//!
//! 全局订阅者单例：本文件只允许一个 init 测试（行为面细粒度断言在
//! src/logx.rs 单元测试）。

#![cfg(feature = "log")]

use std::path::PathBuf;

use skeleton::logpath::LogPathResolver;
#[cfg(not(debug_assertions))]
use skeleton_infra::logx::DefaultPathResolver;
use skeleton_infra::logx::{init_tracing_from_env, init_with_file, AlreadyInit, LogInitError};
use std_ext::fsx::{temp_dir_in, TempDir};

/// 契约测试替身：任意根目录注入。
struct FixedRoot(PathBuf);

impl LogPathResolver for FixedRoot {
    fn log_root(&self) -> PathBuf {
        self.0.clone()
    }
}

/// 工作区内临时根（禁系统 TEMP 红线；TempDir RAII 自清，清理失败会打 stderr）。
fn guard(tag: &str) -> TempDir {
    temp_dir_in(env!("CARGO_TARGET_TMPDIR"), &format!("logfile-{tag}-"))
        .expect("temp root under CARGO_TARGET_TMPDIR")
}

/// env 守卫：构造留存原值、Drop 恢复（panic 展开路径同样恢复；与
/// src/logx.rs 单测守卫同形——env 是进程单例，禁自改自清）。
#[cfg(not(debug_assertions))]
struct EnvGuard {
    var: &'static str,
    saved: Option<String>,
}

#[cfg(not(debug_assertions))]
impl EnvGuard {
    fn save(var: &'static str) -> Self {
        Self {
            var,
            saved: std::env::var(var).ok(),
        }
    }
}

#[cfg(not(debug_assertions))]
impl Drop for EnvGuard {
    fn drop(&mut self) {
        match self.saved.take() {
            Some(v) => std::env::set_var(self.var, v),
            None => std::env::remove_var(self.var),
        }
    }
}

#[test]
fn init_with_file_lifecycle_line_format_and_skeleton_off() {
    let scratch = guard("e2e");
    let root = scratch.path().join("logs");
    let resolver = FixedRoot(root.clone());

    init_with_file(&resolver, log::LevelFilter::Info).expect("first init must succeed");

    // 框架域事件必须被 `skeleton=off` 预设过滤（不落文件）；
    // app 域事件按默认行格式落盘。
    tracing::info!(target: "skeleton::probe", "framework domain must stay silent");
    tracing::info!(target: "test_app::worker", key = 42, "hello {}", "world");

    // 每行 flush 语义 → 无需等待即可读。
    let mut files: Vec<PathBuf> = std::fs::read_dir(&root)
        .expect("log root exists")
        .map(|e| e.expect("dir entry").path())
        .collect();
    assert_eq!(files.len(), 1, "按日滚动：首日恰一个文件");
    let content = std::fs::read_to_string(files.remove(0)).expect("readable");
    let mut lines = content.lines();
    let line = lines.next().expect("at least one line");
    assert!(line.starts_with('['), "行以 [ 开头: {line}");
    let date = &line[1..11]; // YYYY-MM-DD
    assert!(
        date.bytes().enumerate().all(|(i, b)| match i {
            4 | 7 => b == b'-',
            _ => b.is_ascii_digit(),
        }),
        "UTC 日期形状: {line}"
    );
    assert!(
        line.contains(" INFO test_app::worker] hello world key=42"),
        "默认行格式不匹配: {line}"
    );
    assert!(
        lines.next().is_none(),
        "skeleton 域必须被预设过滤，文件只有 app 行；实际: {content}"
    );

    // 全局单例：二次 init 显式 Err；既有入口并存同样被拒。
    assert!(matches!(
        init_with_file(&resolver, log::LevelFilter::Info),
        Err(LogInitError::AlreadyInit)
    ));
    assert!(matches!(
        init_tracing_from_env(log::LevelFilter::Info),
        Err(AlreadyInit)
    ));
}

#[cfg(all(not(debug_assertions), windows))]
#[test]
fn release_resolver_roots_at_mock_userprofile_never_manifest() {
    let scratch = guard("relw");
    let _env = EnvGuard::save("USERPROFILE");
    std::env::set_var("USERPROFILE", scratch.path());
    let resolver = DefaultPathResolver::new("agentwiki").expect("mock home 已设");
    let root = resolver.log_root();
    assert_eq!(root, scratch.path().join(".agentwiki").join("log"));
    // release 禁用编译期锚：解析结果不得是开发者机器路径
    assert!(!root.starts_with(env!("CARGO_MANIFEST_DIR")));
}

#[cfg(all(not(debug_assertions), unix))]
#[test]
fn release_resolver_roots_at_mock_home_never_manifest() {
    let scratch = guard("relu");
    let _env = EnvGuard::save("HOME");
    std::env::set_var("HOME", scratch.path());
    let resolver = DefaultPathResolver::new("agentwiki").expect("mock home 已设");
    let root = resolver.log_root();
    assert_eq!(root, scratch.path().join(".agentwiki").join("log"));
    assert!(!root.starts_with(env!("CARGO_MANIFEST_DIR")));
}

// F2（审计 2026-10-17）处置注：release 臂 home 不可解析→Err(HomeDirUnresolvable)
// 的 fail-closed 分支**经 env 注入不可测**——清 $HOME/$USERPROFILE 后回退链
// 仍命中（Windows=GetUserProfileDirectory OS API、Unix=passwd 当前用户条目，
// std::env::home_dir 两平台皆非纯 env 面）。真触发面=UWP（Windows 恒 None）/
// 无 passwd 条目环境，CI 不可造。分支本体 2 行直白（ok_or+Err），注释即覆盖。
