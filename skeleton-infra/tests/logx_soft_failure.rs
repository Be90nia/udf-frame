//! FileSink 软失败端到端（#112/#143/#184/#198）：经公开面 `with_reporter` 注入
//! 诊断出口，坏 stderr 替身下软失败契约不变——`write` 恒返回整行长度、丢弃计数
//! 权威、告警至多一次；打开失败后落点恢复即自然续写（只重试新行，不追认上一行）。
//!
//! 半写/flush 故障面与落点降级阶梯（主文件 → 恢复文件 → 当日停写）的细粒度断言
//! 在 `src/logx.rs` 单元测试——注入 writer 需要私有状态，公开面不可达。

#![cfg(feature = "log-fs")]

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use skeleton_infra::logx::{FileSink, SoftFailureReporter};
use std_ext::fsx::{TempDir, temp_dir_in};

/// 坏 stderr 替身：只计数，不写任何东西（模拟 stderr 不可用）。
#[derive(Debug, Default)]
struct CountingReporter(AtomicUsize);

impl SoftFailureReporter for CountingReporter {
    fn report(&self, _message: &str) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

impl CountingReporter {
    fn count(&self) -> usize {
        self.0.load(Ordering::Relaxed)
    }
}

/// 工作区内临时根（禁系统 TEMP 红线；TempDir RAII 自清）。
fn guard(tag: &str) -> TempDir {
    temp_dir_in(env!("CARGO_TARGET_TMPDIR"), &format!("logx-soft-{tag}-"))
        .expect("temp root under CARGO_TARGET_TMPDIR")
}

/// 唯一当日文件（首行落盘后枚举根目录取得；文件名规则私有，不在此复制）。
fn only_file(root: &Path) -> PathBuf {
    let mut files: Vec<PathBuf> = std::fs::read_dir(root)
        .expect("log root readable")
        .map(|entry| entry.expect("dir entry").path())
        .collect();
    assert_eq!(files.len(), 1, "按日滚动：单日恰一个文件");
    files.remove(0)
}

#[test]
fn soft_failure_survives_broken_stderr_and_recovers_after_reopen() {
    let scratch = guard("e2e");
    let root = scratch.path().join("logs");
    let reporter = Arc::new(CountingReporter::default());

    // 先用一个探针汇打出一行，取得当日文件名（文件名规则私有，不在此复制）。
    let day_file = {
        let mut probe = FileSink::with_reporter(&root, reporter.clone()).expect("root creatable");
        probe.write_all(b"probe\n").expect("soft path never errs");
        only_file(&root)
    };
    // 落点被目录占住 → 新建汇的首次打开失败（不是写失败）。
    std::fs::remove_file(&day_file).expect("remove probe file");
    std::fs::create_dir(&day_file).expect("block day file with a directory");

    let mut sink = FileSink::with_reporter(&root, reporter.clone()).expect("root creatable");
    assert_eq!(sink.write(b"lost-1\n").expect("soft path never errs"), 7);
    assert_eq!(sink.write(b"lost-2\n").expect("soft path never errs"), 7);
    assert_eq!(sink.dropped_lines(), 2, "两次打开失败逐行计数");
    assert_eq!(
        reporter.count(),
        1,
        "one-warning 策略：告警只报一次（诊断出口坏掉也不变）"
    );

    // 落点恢复 → 只重试新行：失败的前两行不追认，未换日故沿用同一文件名。
    std::fs::remove_dir(&day_file).expect("unblock day file");
    sink.write_all(b"recovered\n")
        .expect("soft path never errs");
    assert_eq!(sink.dropped_lines(), 2, "恢复后计数停涨");
    assert_eq!(
        std::fs::read_to_string(&day_file).expect("day file readable"),
        "recovered\n"
    );
    assert_eq!(only_file(&root), day_file, "恢复落在同一当日文件");
}
