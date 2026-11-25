//! 保留策略端到端验收（#128；`log-fs` 面即可跑，不依赖 `log` 门面）：
//!
//! ① 默认（不装策略）不删任何文件——兼容默认不变；
//! ② age 超限删旧日、当日（含当日恢复文件）恒留、宿主自有文件不动；
//! ③ byte 超限按最旧优先删，删到预算内即停；
//! ④ 预算只剩当日文件可达不成时通告一次、不删当日、不 panic；
//! ⑤ 删除失败（条目被替换成目录）不 panic、reporter 可见，写入照常。
//!
//! 日期构造只用「固定的历史日」与「从落盘文件名反读的当日」——测试不重算 UTC
//! 日历，避免自带第二套日期算法把错答案当成锚。

#![cfg(feature = "log-fs")]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use skeleton_infra::logx::{FileSink, RetentionPolicy, SoftFailureReporter};
use std_ext::fsx::{TempDir, temp_dir_in};

/// 工作区内临时根（禁系统 TEMP 红线；TempDir RAII 自清）。
fn guard(tag: &str) -> TempDir {
    temp_dir_in(env!("CARGO_TARGET_TMPDIR"), &format!("logret-{tag}-"))
        .expect("temp root under CARGO_TARGET_TMPDIR")
}

/// 记录型 reporter：断言清理失败/预算不可达的通告可达（软失败面不依赖 stderr）。
#[derive(Debug, Default)]
struct RecordingReporter(std::sync::Mutex<Vec<String>>);

impl RecordingReporter {
    fn messages(&self) -> Vec<String> {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

impl SoftFailureReporter for RecordingReporter {
    fn report(&self, message: &str) {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(message.to_string());
    }
}

/// 写一行触发当日文件落盘，再从目录反读当日文件名（新根下此时只有它）。
fn seed_today(sink: &mut FileSink, root: &Path) -> String {
    use std::io::Write as _;

    sink.write_all(b"today\n").expect("soft path never errs");
    let mut names: Vec<String> = std::fs::read_dir(root)
        .expect("log root readable")
        .map(|entry| {
            entry
                .expect("dir entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    names.sort();
    assert_eq!(names.len(), 1, "新根下应只有当日文件: {names:?}");
    names.pop().expect("today file name")
}

/// `log-YYYY-MM-DD.log` → `YYYY-MM-DD`（当日文件名由 sink 自己给出，测试不造）。
fn date_of(today_file: &str) -> &str {
    today_file
        .strip_prefix("log-")
        .and_then(|rest| rest.strip_suffix(".log"))
        .expect("today file matches log-YYYY-MM-DD.log")
}

// ① 默认面：不装策略 = 一个文件都不删（兼容默认不变，审计日志不被静默清理）。
#[test]
fn default_policy_never_deletes_files() {
    let scratch = guard("default");
    let root = scratch.path().join("logs");
    let mut sink = FileSink::new(&root).expect("root creatable");
    let today = seed_today(&mut sink, &root);

    let main = root.join("log-2019-01-01.log");
    let recovery = root.join("log-2019-01-02.recovery.log");
    let host_own = root.join("host-own.log");
    for path in [&main, &recovery, &host_own] {
        std::fs::write(path, b"seeded\n").expect("seed old file");
    }

    sink.prune();

    for path in [&main, &recovery, &host_own] {
        assert!(path.exists(), "默认策略不得删文件: {}", path.display());
    }
    assert!(root.join(&today).exists(), "当日文件在");
    assert_eq!(sink.pruned_files(), 0);
}

// ② age 面：窗口外的旧日删除，当日主文件与当日恢复文件都留，宿主自有文件不动。
#[test]
fn age_limit_prunes_old_days_but_never_today() {
    let scratch = guard("age");
    let root = scratch.path().join("logs");
    let mut sink = FileSink::new(&root)
        .expect("root creatable")
        .with_retention(RetentionPolicy::new().max_age(Duration::ZERO));
    let today = seed_today(&mut sink, &root);

    let today_recovery = format!("log-{}.recovery.log", date_of(&today));
    let ancient = root.join("log-2019-01-01.log");
    let host_own = root.join("host-own.log");
    for path in [&root.join(&today_recovery), &ancient, &host_own] {
        std::fs::write(path, b"seeded\n").expect("seed file");
    }

    sink.prune();

    assert!(!ancient.exists(), "早于窗口的历史日删除");
    assert!(root.join(&today).exists(), "当日主文件永不删");
    assert!(root.join(&today_recovery).exists(), "当日恢复文件同属当日");
    assert!(host_own.exists(), "宿主自有文件不在保留策略管辖内");
    assert_eq!(sink.pruned_files(), 1);
}

// ③ byte 面：删到预算内即停，且按最旧优先（较新的旧日文件留着）。
#[test]
fn byte_limit_prunes_oldest_first_and_keeps_today() {
    let scratch = guard("bytes");
    let root = scratch.path().join("logs");
    let reporter = Arc::new(RecordingReporter::default());
    let mut sink = FileSink::with_reporter(&root, reporter.clone()).expect("root creatable");
    let today = seed_today(&mut sink, &root);

    let oldest = root.join("log-2019-01-01.log");
    let newer = root.join("log-2019-01-02.log");
    for path in [&oldest, &newer] {
        std::fs::write(path, [b'x'; 100]).expect("seed 100 byte old file");
    }
    let today_len = std::fs::metadata(root.join(&today))
        .expect("today file")
        .len();
    // 预算 = 当日 + 一个旧文件：只够删掉最旧的一个。
    sink = sink.with_retention(RetentionPolicy::new().max_total_bytes(today_len + 100));

    sink.prune();

    assert!(!oldest.exists(), "最旧的旧日文件先删");
    assert!(newer.exists(), "删到预算内即停（较新旧日文件保留）");
    assert!(root.join(&today).exists(), "当日文件永不删");
    assert_eq!(sink.pruned_files(), 1);
    assert!(reporter.messages().is_empty(), "预算达成时无通告");
}

// ④ 预算连「只剩当日文件」都达不成：不越权删当日，通告一次（软失败面不 panic）。
#[test]
fn unreachable_byte_budget_is_reported_and_keeps_today() {
    let scratch = guard("budget");
    let root = scratch.path().join("logs");
    let reporter = Arc::new(RecordingReporter::default());
    let mut sink = FileSink::with_reporter(&root, reporter.clone()).expect("root creatable");
    let today = seed_today(&mut sink, &root);
    sink = sink.with_retention(RetentionPolicy::new().max_total_bytes(1));

    sink.prune();

    assert!(
        root.join(&today).exists(),
        "当日文件永不删（预算不可达也不越权）"
    );
    assert_eq!(sink.pruned_files(), 0);
    let messages = reporter.messages();
    assert!(
        messages.iter().any(|m| m.contains("cannot be met")),
        "预算不可达必须可见: {messages:?}"
    );
}

// ⑤ 删除失败（只读/被占用/条目被替换成目录）：不 panic、通告可见、写入照常。
#[test]
fn delete_failure_is_reported_and_writes_continue() {
    let scratch = guard("failure");
    let root = scratch.path().join("logs");
    let reporter = Arc::new(RecordingReporter::default());
    let mut sink = FileSink::with_reporter(&root, reporter.clone())
        .expect("root creatable")
        .with_retention(RetentionPolicy::new().max_age(Duration::ZERO));
    let today = seed_today(&mut sink, &root);

    // 跨平台注入删除失败：`remove_file` 对目录恒失败（Unix EISDIR / Windows 拒绝访问）。
    let blocked = root.join("log-2019-01-01.log");
    std::fs::create_dir_all(blocked.join("inner")).expect("dir masquerading as log file");

    sink.prune();

    assert!(blocked.is_dir(), "删除失败时条目原样保留");
    assert_eq!(sink.pruned_files(), 0);
    let messages = reporter.messages();
    assert!(
        messages.iter().any(|m| m.contains("could not remove")),
        "删除失败必须可见: {messages:?}"
    );

    use std::io::Write as _;
    sink.write_all(b"after\n").expect("soft path never errs");
    assert_eq!(sink.dropped_lines(), 0, "清理失败不得拖累写入");
    assert_eq!(
        std::fs::read_to_string(root.join(&today)).expect("today file"),
        "today\nafter\n",
        "当日文件内容完整"
    );
}

// 宿主注入路径：with_reporter + with_retention 可链式并存（装配面回归）。
#[test]
fn reporter_and_retention_compose() {
    let scratch = guard("compose");
    let root: PathBuf = scratch.path().join("logs");
    let reporter = Arc::new(RecordingReporter::default());
    let sink = FileSink::with_reporter(&root, reporter)
        .expect("root creatable")
        .with_retention(RetentionPolicy::new().max_age(Duration::from_secs(86_400)));
    sink.prune();
    assert_eq!(sink.pruned_files(), 0, "空根清理是空操作");
    assert_eq!(sink.dropped_lines(), 0);
}
