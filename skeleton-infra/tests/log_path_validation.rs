//! #218：注入 `LogPathResolver` 的根必须是绝对路径——相对根在**创建任何文件
//! 系统状态之前**被拒（相对根会静默把日志锚到进程 cwd）。
//!
//! ① 相对 / 父相对 / 空根 ⇒ `Err(LogInitError::RelativeRoot { root })`，原路径
//!    原样回传，且没有任何目录/文件被创建；
//! ② 该拒绝发生在全局订阅者状态之前（即使本进程已装过全局 logger，仍报
//!    `RelativeRoot` 而不是 `AlreadyInit`）；
//! ③ `PerChannelPathResolver` → `ChannelFileSink::new` 同样拒绝（`InvalidInput`）；
//! ④ 绝对根通过校验（走到订阅者安装面，故已装过全局 logger 时报 `AlreadyInit`）。

#![cfg(feature = "log")]

use std::path::{Path, PathBuf};

use skeleton::logpath::LogPathResolver;
use skeleton_infra::logx::{
    Channel, ChannelFileSink, LogInitError, PerChannelPathResolver, init_tracing_default,
    init_with_file,
};
use std_ext::fsx::{TempDir, temp_dir_in};

/// 契约测试替身：任意根目录注入。
struct FixedRoot(PathBuf);

impl LogPathResolver for FixedRoot {
    fn log_root(&self) -> PathBuf {
        self.0.clone()
    }
}

fn guard(tag: &str) -> TempDir {
    temp_dir_in(env!("CARGO_TARGET_TMPDIR"), &format!("logpath-{tag}-"))
        .expect("temp root under CARGO_TARGET_TMPDIR")
}

/// 本进程专属的相对路径名（并行测试/残留目录互不干扰）。
fn rel(tag: &str) -> PathBuf {
    PathBuf::from(format!("log-rel-{tag}-{}", std::process::id()))
}

fn assert_absent(path: &Path) {
    assert!(
        std::fs::symlink_metadata(path).is_err(),
        "非法根不得创建任何文件系统条目: {}",
        path.display()
    );
}

// ① 相对 / 父相对 / 空根一律拒绝，且不产生任何文件系统副作用。
#[test]
fn relative_parent_and_empty_roots_are_rejected_without_side_effects() {
    let relative = rel("probe");
    let parent_relative = Path::new("..").join(rel("parent"));
    assert_absent(&relative);
    assert_absent(&parent_relative);

    for bad in [relative.clone(), parent_relative.clone(), PathBuf::new()] {
        match init_with_file(&FixedRoot(bad.clone()), log::LevelFilter::Info) {
            Err(LogInitError::RelativeRoot { root }) => {
                assert_eq!(root, bad, "错误必须原样回传注入的根（诊断可定位）");
            }
            other => panic!("相对根必须 Err(RelativeRoot)，实际 {other:?}（根={bad:?}）"),
        }
        assert_absent(&relative);
        assert_absent(&parent_relative);
    }

    // 错误文案保留原路径（不是「invalid path」这类丢失上下文的泛化消息）。
    let message = LogInitError::RelativeRoot {
        root: relative.clone(),
    }
    .to_string();
    assert!(
        message.contains(&relative.display().to_string()),
        "诊断必须点名原路径: {message}"
    );
}

// ② 校验早于全局状态：即使本进程已占住全局 logger，相对根仍报 RelativeRoot
// （而不是 AlreadyInit）——证明拒绝发生在 try_init 之前。
#[test]
fn relative_root_is_rejected_before_global_subscriber_state() {
    init_tracing_default(log::LevelFilter::Info).expect("首次 init 占位（本进程唯一）");

    let relative = rel("after-init");
    assert!(matches!(
        init_with_file(&FixedRoot(relative.clone()), log::LevelFilter::Info),
        Err(LogInitError::RelativeRoot { .. })
    ));
    assert_absent(&relative);

    // ④ 绝对根通过校验，走到订阅者安装面 ⇒ AlreadyInit（#130 的归属回收在其内）。
    let scratch = guard("absolute");
    let absolute = scratch.path().join("logs");
    assert!(matches!(
        init_with_file(&FixedRoot(absolute), log::LevelFilter::Info),
        Err(LogInitError::AlreadyInit)
    ));
}

// ③ 多通道路径同样吃 resolver 结果：相对根在建目录之前被拒。
#[test]
fn channel_file_sink_rejects_relative_resolver_root() {
    let relative_base = rel("channels");
    let resolver = PerChannelPathResolver::new(FixedRoot(relative_base.clone()));
    let channel_root = resolver.channel_root(Channel::General);
    assert_absent(&channel_root);

    let err = match ChannelFileSink::new(Channel::General, log::LevelFilter::Info, channel_root) {
        Ok(_) => panic!("相对通道根必须被拒"),
        Err(err) => err,
    };
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    assert!(
        err.to_string()
            .contains(&relative_base.display().to_string()),
        "诊断必须点名原路径: {err}"
    );
    assert_absent(&relative_base);
}
