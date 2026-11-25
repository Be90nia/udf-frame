//! #164 / #219：tracing 事件才是原子记录。
//!
//! ① `FileSinkWriter` 逐事件本地缓冲——分片 `write()` 在最终化前**不落盘**；
//! ② `flush()` 是显式提前提交，且与 `Drop` 互不双提交（提交后片段被忽略）；
//! ③ 两线程交错写分片：两条记录各自完整，绝不片间混合（确定性互锁构造）；
//! ④ 单事件上限 1 MiB：恰好在限内落盘、超一字节整段丢弃且计数恰好 +1；
//! ⑤ 上限可显式覆盖（`with_max_event_bytes`），writer 跟随 sink 的上限；
//! ⑥ 直接 `FileSink: Write` 是字节流适配：每次调用即一条记录，不按行分帧；
//! ⑦ 真实 formatter（tracing-subscriber 0.3.23）不调 `flush`——`Drop` 必须
//!    是提交点（钉死依赖生命周期；升级该依赖前必须重核）。
//!
//! 细粒度失败面（半写/flush 失败/落点降级）在 `src/logx.rs` 单元测试——注入
//! 失败 writer 需要私有状态。

#![cfg(feature = "log")]

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use skeleton_infra::logx::{
    Channel, ChannelFileSink, ChannelSink, DEFAULT_MAX_LOG_EVENT_BYTES, FileSink, FileSinkWriter,
    SoftFailureReporter,
};
use std_ext::fsx::{TempDir, temp_dir_in};

/// 只计数的 reporter：本文件不断言告警文案，只断言软失败不依赖诊断出口。
#[derive(Debug, Default)]
struct CountingReporter(AtomicUsize);

impl SoftFailureReporter for CountingReporter {
    fn report(&self, _message: &str) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

/// 工作区内临时根（禁系统 TEMP 红线；TempDir RAII 自清）。
fn guard(tag: &str) -> TempDir {
    temp_dir_in(env!("CARGO_TARGET_TMPDIR"), &format!("logframe-{tag}-"))
        .expect("temp root under CARGO_TARGET_TMPDIR")
}

/// 根下的文件（按日滚动文件名规则私有，集成侧只按数量/内容断言）。
fn files(root: &Path) -> Vec<PathBuf> {
    std::fs::read_dir(root)
        .expect("log root readable")
        .map(|entry| entry.expect("dir entry").path())
        .filter(|path| path.is_file())
        .collect()
}

/// 根下唯一文件的全部字节。
fn only_file(root: &Path) -> Vec<u8> {
    let mut files = files(root);
    assert_eq!(files.len(), 1, "当日恰一个日志文件");
    std::fs::read(files.remove(0)).expect("log file readable")
}

// ① #164：分片 write 在最终化之前不产生任何落盘字节——旧实现（每片段即时
// 落盘）会在第一次 write 后就建出文件，本断言直接判红。
#[test]
fn fragments_are_buffered_until_finalization() {
    let scratch = guard("buffer");
    let root = scratch.path().join("logs");
    let sink = Arc::new(FileSink::new(&root).expect("root creatable"));
    let mut writer = FileSinkWriter::new(Arc::clone(&sink));

    writer.write_all(b"[ts INFO app] hel").expect("soft path");
    writer.write_all(b"lo\n").expect("soft path");
    assert!(
        files(&root).is_empty(),
        "片段不得各自落盘：事件未最终化时根下不应有文件"
    );
    assert_eq!(sink.dropped_lines(), 0, "缓冲中的事件不算丢弃");

    drop(writer);
    assert_eq!(
        only_file(&root),
        b"[ts INFO app] hello\n",
        "最终化一次提交整段字节（片段顺序逐字保留）"
    );
    assert_eq!(sink.dropped_lines(), 0);
}

// ② #164：`flush()` 提交**当前事件**并把 writer 复位——同一 writer 可继续累积下
// 一条记录（post-flush 写入绝不静默丢弃）；Drop 只提交尚未提交的剩余字节，故不会
// 把已提交的记录再写一次，也不会吞掉 flush 后新写的内容。
#[test]
fn explicit_flush_commits_each_event_and_writer_stays_usable() {
    let scratch = guard("flush-once");
    let root = scratch.path().join("logs");
    let sink = Arc::new(FileSink::new(&root).expect("root creatable"));
    let mut writer = FileSinkWriter::new(Arc::clone(&sink));

    writer.write_all(b"record-1\n").expect("soft path");
    writer.flush().expect("explicit flush");
    assert_eq!(only_file(&root), b"record-1\n", "flush 立即提交当前事件");

    // flush 之后 writer 复位：新片段属于下一条记录（不被丢弃、也不与上一条合并）。
    writer.write_all(b"record-2\n").expect("soft path");
    writer.flush().expect("second event");
    assert_eq!(
        only_file(&root),
        b"record-1\nrecord-2\n",
        "flush 后写入必须落为第二条记录（post-flush 写入不得静默丢弃）"
    );

    // 再 flush 一次不产生空记录；Drop 只提交残留、不重复已提交字节。
    writer.flush().expect("empty flush is a no-op");
    drop(writer);
    assert_eq!(
        only_file(&root),
        b"record-1\nrecord-2\n",
        "flush + Drop 不重复提交、不写空记录"
    );
    assert_eq!(sink.dropped_lines(), 0);
}

// ③ #164 条 6：分片故意交错的两个并发事件必须各自完整。互锁（Barrier）把
// 两个线程的片段次序钉成 A1,B1,B2,A2,A3,B3——旧实现（每片段即时落盘）下文件
// 必然出现 "A1-B1-" / "B2-A2-" 这类混合行，本断言确定性判红。
#[test]
fn concurrent_events_never_interleave_fragments() {
    let scratch = guard("interleave");
    let root = scratch.path().join("logs");
    let sink = Arc::new(FileSink::new(&root).expect("root creatable"));
    let barrier = Arc::new(std::sync::Barrier::new(2));

    let mut handles = Vec::new();
    for tag in ["A", "B"] {
        let sink = Arc::clone(&sink);
        let barrier = Arc::clone(&barrier);
        handles.push(std::thread::spawn(move || {
            let mut writer = FileSinkWriter::new(sink);
            writer
                .write_all(format!("{tag}1-").as_bytes())
                .expect("soft path");
            barrier.wait();
            writer
                .write_all(format!("{tag}2-").as_bytes())
                .expect("soft path");
            barrier.wait();
            writer
                .write_all(format!("{tag}3\n").as_bytes())
                .expect("soft path");
            // 最终化在 drop：本事件整段一次提交。
        }));
    }
    for handle in handles {
        handle.join().expect("writer thread");
    }

    let content = String::from_utf8(only_file(&root)).expect("utf8 log");
    let mut lines: Vec<&str> = content.lines().collect();
    lines.sort_unstable();
    assert_eq!(
        lines,
        vec!["A1-A2-A3", "B1-B2-B3"],
        "两条记录各自完整、无片间混合；实际:\n{content}"
    );
    assert_eq!(sink.dropped_lines(), 0);
}

// ④ #164：上限判定在缓冲之前——恰好在限内整段落盘，超一字节整段丢弃（不落
// 半条记录），且同一 writer 的后续片段不再增长缓冲、最终化只计一次丢弃。
#[test]
fn oversize_event_is_dropped_once_without_partial_output() {
    let scratch = guard("oversize");
    let root = scratch.path().join("logs");
    let sink = Arc::new(
        FileSink::with_reporter(
            &root,
            Arc::new(CountingReporter::default()) as Arc<dyn SoftFailureReporter>,
        )
        .expect("root creatable"),
    );

    // 恰好等于上限：接受并整段落盘。
    let at_limit = vec![b'x'; DEFAULT_MAX_LOG_EVENT_BYTES];
    let mut writer = FileSinkWriter::new(Arc::clone(&sink));
    writer.write_all(&at_limit).expect("soft path");
    drop(writer);
    assert_eq!(
        only_file(&root).len(),
        DEFAULT_MAX_LOG_EVENT_BYTES,
        "恰好等于上限的事件必须整段落盘"
    );
    assert_eq!(sink.dropped_lines(), 0);

    // 超一字节：整段丢弃；后续片段不再增长缓冲；丢弃计数恰好 +1。
    let mut writer = FileSinkWriter::new(Arc::clone(&sink));
    writer
        .write_all(&vec![b'y'; DEFAULT_MAX_LOG_EVENT_BYTES + 1])
        .expect("soft path");
    writer
        .write_all(b"tail-after-overflow\n")
        .expect("soft path");
    assert_eq!(sink.dropped_lines(), 0, "最终化之前不计数");
    writer.flush().expect("finalize the oversize event");
    assert_eq!(sink.dropped_lines(), 1, "超限事件恰好计一次丢弃");
    assert_eq!(
        only_file(&root).len(),
        DEFAULT_MAX_LOG_EVENT_BYTES,
        "超限事件不得留下任何部分字节"
    );

    // 复位后下一条记录照常接收（超限不是「writer 报废」，也不是静默丢弃）。
    writer.write_all(b"after\n").expect("soft path");
    drop(writer);
    assert_eq!(sink.dropped_lines(), 1, "后续正常记录不增计数");
    let content = only_file(&root);
    assert!(
        content.ends_with(b"after\n"),
        "超限事件之后的新记录必须落盘；实际尾部: {:?}",
        String::from_utf8_lossy(&content[content.len().saturating_sub(16)..])
    );
}

// ⑤ #164：上限可显式覆盖，且 writer 跟随所属 sink 的上限（构造期快照）。
#[test]
fn cap_override_is_honoured_by_the_writer() {
    let scratch = guard("cap");
    let root = scratch.path().join("logs");
    let sink = Arc::new(
        FileSink::with_reporter(
            &root,
            Arc::new(CountingReporter::default()) as Arc<dyn SoftFailureReporter>,
        )
        .expect("root creatable")
        .with_max_event_bytes(4),
    );
    assert_eq!(sink.max_event_bytes(), 4);

    let mut writer = FileSinkWriter::new(Arc::clone(&sink));
    writer.write_all(b"ok!\n").expect("soft path"); // 4 字节，恰在限内
    drop(writer);
    assert_eq!(only_file(&root), b"ok!\n");
    assert_eq!(sink.dropped_lines(), 0);

    let mut writer = FileSinkWriter::new(Arc::clone(&sink));
    writer.write_all(b"too-long\n").expect("soft path");
    drop(writer);
    assert_eq!(sink.dropped_lines(), 1);
    assert_eq!(only_file(&root), b"ok!\n", "超限事件不追加任何字节");
}

// ⑥ #219：直接 `FileSink: Write` 是字节流适配——每次调用即一条记录，不按
// 换行拆分、不跨调用合帧（记录边界 = 调用边界，且不要求以换行结尾）。
#[test]
fn direct_file_sink_writes_are_separate_records() {
    let scratch = guard("direct");
    let root = scratch.path().join("logs");
    let mut sink = FileSink::new(&root).expect("root creatable");

    sink.write_all(b"first-").expect("direct write");
    sink.write_all(b"second-no-newline").expect("direct write");
    sink.write_all(b"\n").expect("direct write");

    assert_eq!(
        only_file(&root),
        b"first-second-no-newline\n",
        "三次调用三条记录逐字追加（无隐藏换行/合并/拆分）"
    );
    assert_eq!(sink.dropped_lines(), 0);
}

// ⑦ #164 Round 2 addendum：同一逻辑记录上限也适用通道路径——超限消息在构造
// 格式化 String 之前就被粗筛掉（整段丢弃、不落盘），后续正常记录照常落盘。
#[test]
fn channel_file_sink_drops_oversize_messages_without_partial_output() {
    let scratch = guard("channel-cap");
    let root = scratch.path().join("general");
    let sink = ChannelFileSink::new(Channel::General, log::LevelFilter::Info, &root)
        .expect("root creatable");

    // 消息本身就填满上限：加上行格式开销后必然超限 ⇒ 整段丢弃。
    let oversize = "x".repeat(DEFAULT_MAX_LOG_EVENT_BYTES);
    sink.write(log::Level::Info, "app::t", &oversize);
    assert_eq!(sink.dropped_lines(), 1, "超限记录恰好计一次丢弃");
    assert!(
        files(&root).is_empty(),
        "超限记录不得落盘（无部分输出、无空文件）"
    );

    sink.write(log::Level::Info, "app::t", "normal");
    assert_eq!(sink.dropped_lines(), 1, "后续正常记录不增计数");
    let content = String::from_utf8(only_file(&root)).expect("utf8 log");
    assert!(
        content.ends_with(" INFO app::t] normal\n"),
        "正常记录照常落盘；实际: {content:?}"
    );
}

// ⑧ #164 Round 2 addendum：tracing-subscriber 0.3.23 的 `fmt::Layer::on_event`
// 只做一次 write_all、**不调 flush**，随后丢弃 writer ⇒ 事件必须在 Drop 提交。
// 本测试不调用任何 flush，直接经真实 formatter + scoped subscriber 断言落盘。
// 若依赖升级改变该生命周期，本测试是最先变红的一处。
#[test]
fn real_formatter_commits_on_drop_without_flush() {
    let scratch = guard("lifecycle");
    let root = scratch.path().join("logs");
    let sink = Arc::new(FileSink::new(&root).expect("root creatable"));

    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_max_level(tracing::Level::INFO)
        .with_writer(FileSinkWriter::new(Arc::clone(&sink)))
        .finish();
    tracing::subscriber::with_default(subscriber, || {
        tracing::info!(target: "app::lifecycle", "drop-commit probe");
    });

    let content = String::from_utf8(only_file(&root)).expect("utf8 log");
    assert!(
        content.contains("app::lifecycle") && content.contains("drop-commit probe"),
        "事件必须在 writer 的 Drop 时提交（formatter 不调 flush）；实际: {content:?}"
    );
    assert_eq!(sink.dropped_lines(), 0);

    // 二次事件同样自成一记录（每事件一个 MakeWriter writer）。
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_max_level(tracing::Level::INFO)
        .with_writer(FileSinkWriter::new(Arc::clone(&sink)))
        .finish();
    tracing::subscriber::with_default(subscriber, || {
        tracing::info!(target: "app::lifecycle", "second event");
    });
    let content = String::from_utf8(only_file(&root)).expect("utf8 log");
    assert_eq!(
        content.lines().count(),
        2,
        "两个事件两条记录（互不拼接）；实际: {content:?}"
    );
}
