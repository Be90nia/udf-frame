//! #176 / #164：掩码语义与显式红化入口。
//!
//! ① 公共掩码助手：最长优先、非重叠、标记不被后续规则二次命中、空串项忽略；
//! ② 预编译配置（`ChannelRegistry::set_secrets`）与公共助手逐值同语义；
//! ③ `set_secrets` 是替换语义（非累加），空表=透传；
//! ④ `FileSinkWriter::with_secrets` 对**整段缓冲事件**掩码——被 formatter 片段
//!    拆开的密钥同样命中；`FileSinkWriter::new` 逐字保留原文（raw 明确分档；
//!    `ChannelRegistry::set_secrets` 不保护这条裸路径）；
//! ⑤ `init_with_file_with_secrets` 端到端：文件 sink 落盘即掩码。
//!
//! 每事件分配面/延迟对比（#176 条 5）在 `tests/log_redaction_alloc.rs`
//! （独立二进制，独占计数分配器）。

#![cfg(feature = "log")]

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use skeleton::logpath::LogPathResolver;
use skeleton_infra::logx::{
    Channel, ChannelRegistry, ChannelSink, FileSink, FileSinkWriter, LogInitError,
    init_with_file_with_secrets, mask_secrets,
};
use std_ext::fsx::{TempDir, temp_dir_in};

/// 契约测试替身：任意根目录注入。
struct FixedRoot(PathBuf);

impl LogPathResolver for FixedRoot {
    fn log_root(&self) -> PathBuf {
        self.0.clone()
    }
}

/// 工作区内临时根（禁系统 TEMP 红线；TempDir RAII 自清）。
fn guard(tag: &str) -> TempDir {
    temp_dir_in(env!("CARGO_TARGET_TMPDIR"), &format!("logredact-{tag}-"))
        .expect("temp root under CARGO_TARGET_TMPDIR")
}

/// 根下唯一文件的全部字节。
fn only_file(root: &Path) -> Vec<u8> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(root)
        .expect("log root readable")
        .map(|entry| entry.expect("dir entry").path())
        .filter(|path| path.is_file())
        .collect();
    assert_eq!(files.len(), 1, "当日恰一个日志文件");
    std::fs::read(files.remove(0)).expect("log file readable")
}

/// 捕获 sink：只留最后一条消息（断言 registry 掩码后的形态）。
#[derive(Debug, Default)]
struct CaptureSink(Mutex<String>);

impl ChannelSink for CaptureSink {
    fn channel(&self) -> Channel {
        Channel::General
    }

    fn level(&self) -> log::LevelFilter {
        log::LevelFilter::Trace
    }

    fn write(&self, _level: log::Level, _target: &str, message: &str) {
        *self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = message.to_string();
    }

    fn dropped_lines(&self) -> u64 {
        0
    }
}

impl CaptureSink {
    fn last(&self) -> String {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

// ① 公共掩码助手：最长优先（"sk-long" 吃掉 "sk"）、非重叠、且标记不被再次
// 命中——规则含 "*" 时旧实现（逐条 replace）会把已写入的 "***" 再掩成 9 个星。
#[test]
fn public_mask_helper_keeps_longest_match_and_never_remasks() {
    assert_eq!(
        mask_secrets("sk-long and sk", &["sk", "sk-long"]),
        "*** and ***"
    );
    assert_eq!(
        mask_secrets("sk and *", &["sk", "*"]),
        "*** and ***",
        "替换标记不得被后续规则二次掩码（递归掩码是 #176 条 4 明确禁止的）"
    );
    // 非重叠：命中后从命中之后继续扫描。
    assert_eq!(mask_secrets("aaaa", &["aa"]), "******");
    // 空串项忽略（否则每个字符位都会被注入标记）。
    assert_eq!(mask_secrets("plain", &[""]), "plain");
    assert_eq!(mask_secrets("plain", &[]), "plain");
}

// ② 预编译配置与公共助手逐值同语义（预编译只是把剔空+排序搬到 set 期）。
#[test]
fn precompiled_registry_mask_matches_helper_semantics() {
    let registry = ChannelRegistry::new();
    let sink = Arc::new(CaptureSink::default());
    registry.register(sink.clone()).expect("首注恒 Ok");
    registry.set_secrets(vec![
        "sk-live".to_string(),
        "sk-live-123".to_string(),
        "token".to_string(),
        String::new(), // 空串项必须被预编译剔除
    ]);

    let message = "token=sk-live-123 exit";
    registry.emit(Channel::General, log::Level::Info, "app::t", message);
    assert_eq!(sink.last(), "***=*** exit");
    assert_eq!(
        sink.last(),
        mask_secrets(message, &["sk-live", "sk-live-123", "token"]),
        "预编译路径与公共助手同语义（最长优先/非重叠）"
    );
}

// ③ set_secrets = 替换语义（非累加）；空表回到零分配透传。
#[test]
fn set_secrets_replaces_the_table_instead_of_accumulating() {
    let registry = ChannelRegistry::new();
    let sink = Arc::new(CaptureSink::default());
    registry.register(sink.clone()).expect("首注恒 Ok");

    registry.set_secrets(vec!["alpha".to_string()]);
    registry.emit(Channel::General, log::Level::Info, "app::t", "alpha beta");
    assert_eq!(sink.last(), "*** beta");

    registry.set_secrets(vec!["beta".to_string()]);
    registry.emit(Channel::General, log::Level::Info, "app::t", "alpha beta");
    assert_eq!(sink.last(), "alpha ***", "旧规则必须失效（替换而非累加）");

    registry.set_secrets(Vec::new());
    registry.emit(Channel::General, log::Level::Info, "app::t", "alpha beta");
    assert_eq!(sink.last(), "alpha beta", "空表=透传");
}

// ④ #164 Round 2：红化作用在**整段缓冲事件**上，因此跨 formatter 片段拆开的
// 密钥照样命中；同一输入走 raw writer 则逐字保留（两条路径明确分档）。
#[test]
fn redacted_writer_masks_secret_split_across_fragments() {
    let scratch = guard("split");
    let root = scratch.path().join("logs");
    let sink = Arc::new(FileSink::new(&root).expect("root creatable"));

    let mut writer =
        FileSinkWriter::with_secrets(Arc::clone(&sink), vec!["sk-live-123".to_string()]);
    writer.write_all(b"prefix sk-li").expect("soft path");
    writer.write_all(b"ve-123 suffix\n").expect("soft path");
    drop(writer);

    assert_eq!(
        only_file(&root),
        b"prefix *** suffix\n",
        "密钥被片段拆开也必须整段命中"
    );
    assert_eq!(sink.dropped_lines(), 0);
}

#[test]
fn raw_writer_preserves_secret_bytes_exactly() {
    let scratch = guard("raw");
    let root = scratch.path().join("logs");
    let sink = Arc::new(FileSink::new(&root).expect("root creatable"));

    // 同一 registry 配置了密钥也不影响这条裸 writer（作用域分离）。
    let registry = ChannelRegistry::new();
    registry.set_secrets(vec!["sk-live-123".to_string()]);

    let mut writer = FileSinkWriter::new(Arc::clone(&sink));
    writer.write_all(b"prefix sk-li").expect("soft path");
    writer.write_all(b"ve-123 suffix\n").expect("soft path");
    drop(writer);

    assert_eq!(
        only_file(&root),
        b"prefix sk-live-123 suffix\n",
        "raw writer 逐字保留原文（通道 registry 的密钥不作用于本面）"
    );
}

// ⑤ 端到端：#164 要求的显式红化初始化入口。本文件仅此一处触碰全局 logger。
#[test]
fn init_with_file_with_secrets_masks_the_file_sink() {
    /// env 守卫：留存原值、Drop 恢复（RUST_LOG 显式值会改变过滤结果）。
    struct EnvGuard {
        saved: Option<String>,
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            match self.saved.take() {
                Some(v) => std::env::set_var("RUST_LOG", v),
                None => std::env::remove_var("RUST_LOG"),
            }
        }
    }

    let _env = EnvGuard {
        saved: std::env::var("RUST_LOG").ok(),
    };
    std::env::remove_var("RUST_LOG");

    let scratch = guard("init");
    let root = scratch.path().join("logs");
    init_with_file_with_secrets(
        &FixedRoot(root.clone()),
        log::LevelFilter::Info,
        vec!["sk-live-123".to_string()],
    )
    .expect("首调 Ok（本进程唯一 init）");

    tracing::info!(target: "app::worker", "token=sk-live-123 done");

    let content = String::from_utf8(only_file(&root)).expect("utf8 log");
    assert!(
        content.contains("token=*** done"),
        "文件 sink 必须落盘掩码后的行；实际: {content:?}"
    );
    assert!(
        !content.contains("sk-live-123"),
        "明文密钥不得出现在文件 sink；实际: {content:?}"
    );

    // 重复初始化仍 fail-visible（红化变体不改变单例语义）。
    assert!(matches!(
        init_with_file_with_secrets(
            &FixedRoot(root),
            log::LevelFilter::Info,
            vec!["x".to_string()]
        ),
        Err(LogInitError::AlreadyInit)
    ));
}
