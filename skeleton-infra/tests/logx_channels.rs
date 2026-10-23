//! logx 多通道扇出端到端验收（P1-10-a 前置件；`--features log` 开跑）：
//!
//! ① 三通道独立 sink 路由（消息不串道）；
//! ② 每通道独立 level 过滤（低于本通道 level 丢弃且**不**计丢弃数）；
//! ③ 重复注册 fail-visible（Err，原 sink 不被覆盖）；
//! ④ 未注册通道默认静默（无输出/无 panic/无计数）；
//! ⑤ 丢弃计数可读（失败 sink 自计数 + registry 总和跨通道求和）；
//! ⑥ 掩码唯一布点（set_secrets 后各通道写前统一过 mask_secrets）；
//! ⑦ PerChannelPathResolver + ChannelFileSink：每通道独立子目录真落盘，
//!    行格式与 LineFormat 同形，内容互不污染。
//!
//! 本文件零全局单例依赖（ChannelRegistry 为普通值），测试可并行。

#![cfg(feature = "log")]

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use skeleton::logpath::LogPathResolver;
use skeleton_infra::logx::{
    Channel, ChannelError, ChannelFileSink, ChannelRegistry, ChannelSink, PerChannelPathResolver,
};
use std_ext::fsx::{temp_dir_in, TempDir};

/// 契约测试替身：任意根目录注入（同 tests/log_file.rs FixedRoot）。
struct FixedRoot(PathBuf);

impl LogPathResolver for FixedRoot {
    fn log_root(&self) -> PathBuf {
        self.0.clone()
    }
}

/// 工作区内临时根（禁系统 TEMP 红线；TempDir RAII 自清）。
fn guard(tag: &str) -> TempDir {
    temp_dir_in(env!("CARGO_TARGET_TMPDIR"), &format!("logx-ch-{tag}-"))
        .expect("temp root under CARGO_TARGET_TMPDIR")
}

/// 捕获 sink：行记录进内存；`fail=true` 时写恒失败并自计数（失败记账在
/// sink 内的契约面）。
struct CaptureSink {
    channel: Channel,
    level: log::LevelFilter,
    fail: bool,
    lines: Mutex<Vec<String>>,
    dropped: AtomicU64,
}

impl CaptureSink {
    fn new(channel: Channel, level: log::LevelFilter) -> Self {
        Self {
            channel,
            level,
            fail: false,
            lines: Mutex::new(Vec::new()),
            dropped: AtomicU64::new(0),
        }
    }

    fn failing(channel: Channel) -> Self {
        Self {
            fail: true,
            ..Self::new(channel, log::LevelFilter::Trace)
        }
    }

    fn lines(&self) -> Vec<String> {
        self.lines
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

impl ChannelSink for CaptureSink {
    fn channel(&self) -> Channel {
        self.channel
    }

    fn level(&self) -> log::LevelFilter {
        self.level
    }

    fn write(&self, level: log::Level, target: &str, message: &str) {
        if self.fail {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        self.lines
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(format!("{level} {target} {message}"));
    }

    fn dropped_lines(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

// ① 三通道各自 sink 独立：三条消息各回各通道，不串道。
#[test]
fn channels_route_to_their_own_sink_only() {
    let registry = ChannelRegistry::new();
    let general = Arc::new(CaptureSink::new(Channel::General, log::LevelFilter::Trace));
    let access = Arc::new(CaptureSink::new(Channel::Access, log::LevelFilter::Trace));
    let dns = Arc::new(CaptureSink::new(Channel::Dns, log::LevelFilter::Trace));
    registry.register(general.clone()).expect("general 首注");
    registry.register(access.clone()).expect("access 首注");
    registry.register(dns.clone()).expect("dns 首注");

    registry.emit(Channel::General, log::Level::Info, "app::t", "to-general");
    registry.emit(Channel::Access, log::Level::Info, "app::t", "to-access");
    registry.emit(Channel::Dns, log::Level::Info, "app::t", "to-dns");

    assert_eq!(general.lines(), vec!["INFO app::t to-general"]);
    assert_eq!(access.lines(), vec!["INFO app::t to-access"]);
    assert_eq!(dns.lines(), vec!["INFO app::t to-dns"]);
    assert_eq!(registry.dropped_total(), 0);
}

// ② 每通道独立 level：同一条 Info 在 Warn 通道被滤、在 Debug 通道落；
//    级别过滤不计丢弃（dropped_lines 与 dropped_total 均不动）。
#[test]
fn per_channel_level_filters_independently_without_counting_drops() {
    let registry = ChannelRegistry::new();
    let general = Arc::new(CaptureSink::new(Channel::General, log::LevelFilter::Warn));
    let dns = Arc::new(CaptureSink::new(Channel::Dns, log::LevelFilter::Debug));
    registry.register(general.clone()).expect("general 首注");
    registry.register(dns.clone()).expect("dns 首注");

    registry.emit(Channel::General, log::Level::Info, "app::t", "below-warn");
    registry.emit(Channel::General, log::Level::Error, "app::t", "above-warn");
    registry.emit(Channel::Dns, log::Level::Debug, "app::t", "at-debug");

    assert_eq!(general.lines(), vec!["ERROR app::t above-warn"]);
    assert_eq!(dns.lines(), vec!["DEBUG app::t at-debug"]);
    assert_eq!(general.dropped_lines(), 0, "级别过滤不计丢弃");
    assert_eq!(
        registry.dropped_total(),
        0,
        "级别过滤不计丢弃（总和面同口径）"
    );
}

// ③ 重复注册 fail-visible：Err(AlreadyRegistered) 且原 sink 保持不变。
#[test]
fn duplicate_registration_fails_and_keeps_first_sink() {
    let registry = ChannelRegistry::new();
    let first = Arc::new(CaptureSink::new(Channel::Access, log::LevelFilter::Trace));
    registry.register(first.clone()).expect("access 首注");

    let err = registry
        .register(Arc::new(CaptureSink::new(
            Channel::Access,
            log::LevelFilter::Trace,
        )))
        .expect_err("重复注册必须 Err");
    assert!(
        matches!(
            err,
            ChannelError::AlreadyRegistered {
                channel: Channel::Access
            }
        ),
        "错误面: {err}"
    );

    registry.emit(Channel::Access, log::Level::Warn, "app::t", "kept");
    assert_eq!(first.lines(), vec!["WARN app::t kept"], "原 sink 未被覆盖");
}

// ④ 未注册通道默认静默：emit 不 panic、无输出面、不计数。
#[test]
fn unregistered_channel_is_silent_and_uncounted() {
    let registry = ChannelRegistry::new();
    registry.emit(Channel::Dns, log::Level::Error, "app::t", "vanish");
    registry.emit(Channel::General, log::Level::Error, "app::t", "vanish");
    registry.emit(Channel::Access, log::Level::Error, "app::t", "vanish");
    assert_eq!(registry.dropped_total(), 0, "未注册丢弃不计数");
}

// ⑤ 丢弃计数：写失败 sink 自计数 + registry 总和跨通道求和。
#[test]
fn write_failures_are_counted_per_sink_and_summed_by_registry() {
    let registry = ChannelRegistry::new();
    let bad = Arc::new(CaptureSink::failing(Channel::Access));
    let good = Arc::new(CaptureSink::new(Channel::General, log::LevelFilter::Trace));
    registry.register(bad.clone()).expect("access 首注");
    registry.register(good.clone()).expect("general 首注");

    registry.emit(Channel::Access, log::Level::Warn, "app::t", "lost-1");
    registry.emit(Channel::Access, log::Level::Warn, "app::t", "lost-2");
    registry.emit(Channel::General, log::Level::Warn, "app::t", "kept");

    assert_eq!(bad.dropped_lines(), 2);
    assert_eq!(good.dropped_lines(), 0);
    assert_eq!(registry.dropped_total(), 2);
}

// ⑥ 掩码唯一布点：set_secrets 后所有通道写前统一过 mask_secrets；
//    未配置（默认）时原样透传。
#[test]
fn secrets_masked_at_single_choke_point_for_every_channel() {
    let registry = ChannelRegistry::new();
    let general = Arc::new(CaptureSink::new(Channel::General, log::LevelFilter::Trace));
    let dns = Arc::new(CaptureSink::new(Channel::Dns, log::LevelFilter::Trace));
    registry.register(general.clone()).expect("general 首注");
    registry.register(dns.clone()).expect("dns 首注");

    // 未配置：透传
    registry.emit(
        Channel::General,
        log::Level::Info,
        "app::t",
        "token=sk-live-1",
    );
    assert_eq!(general.lines(), vec!["INFO app::t token=sk-live-1"]);

    registry.set_secrets(vec!["sk-live-1".to_string()]);
    registry.emit(
        Channel::General,
        log::Level::Info,
        "app::t",
        "token=sk-live-1",
    );
    registry.emit(
        Channel::Dns,
        log::Level::Info,
        "app::t",
        "token=sk-live-1 and sk-live-1 twice",
    );
    assert_eq!(
        general.lines(),
        vec!["INFO app::t token=sk-live-1", "INFO app::t token=***"]
    );
    assert_eq!(
        dns.lines(),
        vec!["INFO app::t token=*** and *** twice"],
        "掩码跨通道同布点生效"
    );
}

// ⑦ 每通道独立子目录真落盘：三个互不相交的根，文件各归各目录（按日滚动
//    文件名沿用 FileSink），行格式 `[UTC LEVEL target] message`，不串道。
#[test]
fn per_channel_file_sinks_land_in_their_own_subdirectories() {
    let scratch = guard("paths");
    let base_root = scratch.path().join("roots");
    let resolver = PerChannelPathResolver::new(FixedRoot(base_root.clone()));

    // 子目录方案钉死：<base>/<channel.as_str()>
    assert_eq!(
        resolver.channel_root(Channel::General),
        base_root.join("general")
    );
    assert_eq!(
        resolver.channel_root(Channel::Access),
        base_root.join("access")
    );

    let registry = ChannelRegistry::new();
    for channel in [Channel::General, Channel::Access, Channel::Dns] {
        let sink = ChannelFileSink::new(
            channel,
            log::LevelFilter::Trace,
            resolver.channel_root(channel),
        )
        .expect("根目录可创建");
        registry.register(Arc::new(sink)).expect("首注");
    }

    registry.emit(
        Channel::General,
        log::Level::Info,
        "app::worker",
        "hello-general",
    );
    registry.emit(
        Channel::Access,
        log::Level::Info,
        "app::access",
        "hello-access",
    );
    registry.emit(Channel::Dns, log::Level::Warn, "app::dns", "hello-dns");

    for (channel, target, message) in [
        (Channel::General, "app::worker", "hello-general"),
        (Channel::Access, "app::access", "hello-access"),
        (Channel::Dns, "app::dns", "hello-dns"),
    ] {
        let content = read_single_log_file(&resolver.channel_root(channel));
        assert!(
            content.starts_with('['),
            "行以 [ 开头（LineFormat 同形）: {content}"
        );
        let expected_tail = format!(" {target}] {message}\n");
        assert!(
            content.ends_with(&expected_tail),
            "行尾含 ` {target}] {message}`: {content}"
        );
    }

    // 不串道：general 文件只含 general 行。
    let general_content = read_single_log_file(&resolver.channel_root(Channel::General));
    assert!(!general_content.contains("hello-access"));
    assert!(!general_content.contains("hello-dns"));
}

/// 目录内恰一个按日滚动日志文件，读出内容（day_file_name 为私有面，集成侧
/// 以「唯一 log-*.log 文件」锚定）。
fn read_single_log_file(dir: &Path) -> String {
    let entries: Vec<PathBuf> = std::fs::read_dir(dir)
        .expect("channel dir exists")
        .map(|entry| entry.expect("entry readable").path())
        .collect();
    assert_eq!(entries.len(), 1, "每通道目录恰一个日志文件: {entries:?}");
    let name = entries[0]
        .file_name()
        .expect("file name")
        .to_string_lossy()
        .into_owned();
    assert!(
        name.starts_with("log-") && name.ends_with(".log"),
        "按日滚动文件名形状: {name}"
    );
    std::fs::read_to_string(&entries[0]).expect("log file readable")
}
