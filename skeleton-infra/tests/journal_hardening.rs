//! 账本加固验收（issue #217 / #212 / #181 / #178 / #177 / #106 ②）。
//!
//! 覆盖公共 API 可复现的面：记录上限（写/读/头/快照扫描）、文件权限（Unix）、
//! 记录形状 fail-closed、头行计数与换行框架。需要注入 IO 失败的路径（#212 的
//! 写/flush 失败清理、#106 ③ 的 poison 态）在 `src/journal.rs` 的 lib 单元测试里
//! 做（`cfg(test)` 注入点；集成测试编的是非 `cfg(test)` lib，无注入能力）。

#![cfg(feature = "journal")]

use serde::{Deserialize, Serialize};
use skeleton_infra::journal::{
    DEFAULT_MAX_RECORD_BYTES, Journal, JournalError, JsonlJournal, ReplayError,
};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Act {
    id: u64,
    label: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct St {
    count: u64,
}

/// 用例独享临时目录（crate 构建目录内，禁系统 TEMP）：进程 id + 进程内序号隔离；
/// 清理归 [`CaseDir`] 的 Drop（含 panic 路径）。
fn case_dir(name: &str) -> CaseDir {
    static SEQ: AtomicUsize = AtomicUsize::new(0);
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target-iso/journal-hardening")
        .join(format!("{name}-{}-{seq}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    CaseDir(dir)
}

struct CaseDir(PathBuf);

impl std::ops::Deref for CaseDir {
    type Target = Path;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Drop for CaseDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// 手写账本夹具：Unix 上收紧到 `0600`——`open` 拒 group/other 可读的已有文件（#181），
/// 裸 `fs::write` 在 `umask 022` 下产 0644 会把夹具变成隐私错误。
fn write_private(path: &Path, body: &str) {
    std::fs::write(path, body).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
}

fn act(id: u64, label: impl Into<String>) -> Act {
    Act {
        id,
        label: label.into(),
    }
}

// ── #217：单条记录编码上限（含终止换行） ──────────────────────────────────

/// 默认上限 = 16 MiB（issue #217 R2 拍板值），且是公开可配置面。
#[test]
fn default_record_limit_is_16_mib() {
    assert_eq!(DEFAULT_MAX_RECORD_BYTES, 16 * 1024 * 1024);
}

/// 边界：恰好 `limit` 字节的记录可写可读；多 1 字节在**写前**被拒（file 零变化、
/// writer 仍健康）；读侧多 1 字节报带行号的 `RecordTooLarge`。
///
/// 记录布局（`Act`）：`{"t":"a",` (9B) + payload 去外层括号 + `}` + `\n`
/// ⇒ 行长 = 28 + label 字节数；故 at-limit 的 label 长 = `LIMIT - 28`。
#[test]
fn record_limit_boundary_write_and_read() {
    const LIMIT: usize = 128;
    const AT_LIMIT_LABEL: usize = LIMIT - 28;

    let dir = case_dir("limit-boundary");
    let path = dir.join("j.jsonl");
    {
        let mut j = JsonlJournal::<Act, St>::create_with_limit(&path, LIMIT).unwrap();
        j.append_action(&act(1, "x".repeat(AT_LIMIT_LABEL)))
            .expect("恰好等于上限的记录必须可写");
        j.flush().unwrap();
    }
    let body = std::fs::read_to_string(&path).unwrap();
    let second_len = body.lines().nth(1).expect("第二行=action").len() + 1; // + \n
    assert_eq!(second_len, LIMIT, "记录编码长度须恰为上限（含终止换行）");

    // 读侧：同上限可整读；上限少 1 字节即 TooLarge 且行号=2。
    {
        let j = JsonlJournal::<Act, St>::open_with_limit(&path, 1, LIMIT).unwrap();
        let got: Vec<Act> = j.replay().unwrap().map(Result::unwrap).collect();
        assert_eq!(got, vec![act(1, "x".repeat(AT_LIMIT_LABEL))]);
    }
    {
        let j = JsonlJournal::<Act, St>::open_with_limit(&path, 1, LIMIT - 1).unwrap();
        let mut r = j.replay().unwrap();
        match r.next() {
            Some(Err(ReplayError::RecordTooLarge { line, limit })) => {
                assert_eq!((line, limit), (2, LIMIT - 1));
            }
            other => panic!("expected RecordTooLarge at line 2, got {other:?}"),
        }
        assert_eq!(r.skipped_unknown(), 0, "超限行不得计入未知变体计数");
    }

    // 写侧多 1 字节：拒绝 + 文件零变化 + writer 健康（后续小记录照常）。
    {
        let mut j = JsonlJournal::<Act, St>::open_with_limit(&path, 1, LIMIT).unwrap();
        let before = std::fs::read(&path).unwrap();
        match j.append_action(&act(2, "x".repeat(AT_LIMIT_LABEL + 1))) {
            Err(JournalError::RecordTooLarge { line: None, limit }) => assert_eq!(limit, LIMIT),
            other => panic!("expected RecordTooLarge(line:None), got {other:?}"),
        }
        j.flush().unwrap();
        assert_eq!(
            std::fs::read(&path).unwrap(),
            before,
            "超限拒绝必须零字节落盘（文件不变）"
        );
        j.append_action(&act(3, "ok")).unwrap();
        j.flush().unwrap();
        drop(j);
        let j = JsonlJournal::<Act, St>::open_with_limit(&path, 1, LIMIT).unwrap();
        assert_eq!(j.replay().unwrap().count(), 2, "writer 拒绝后仍健康可写");
    }
}

/// 头行也受同一上限：上限容不下头行 ⇒ `open`/`create` 都在校验阶段 typed 拒绝。
#[test]
fn oversized_header_is_rejected_by_the_same_limit() {
    let dir = case_dir("limit-header");
    let path = dir.join("j.jsonl");
    JsonlJournal::<Act, St>::create(&path).unwrap();
    match JsonlJournal::<Act, St>::open_with_limit(&path, 1, 4) {
        Err(JournalError::RecordTooLarge { line, limit }) => {
            assert_eq!((line, limit), (Some(1), 4));
        }
        other => panic!("expected RecordTooLarge at header line, got {other:?}"),
    }
    // create 不建出自身不可 open 的账本（不留陷阱）。
    let other = dir.join("k.jsonl");
    match JsonlJournal::<Act, St>::create_with_limit(&other, 4) {
        Err(JournalError::RecordTooLarge {
            line: None,
            limit: 4,
        }) => {}
        res => panic!("expected RecordTooLarge(line:None), got {res:?}"),
    }
    assert!(!other.exists(), "被上限拒绝时不得创建目标文件");
}

/// 中途出现过大的 action 行：回放 fail-closed 且行号为物理行号。
#[test]
fn oversized_action_line_on_read_carries_physical_line() {
    const LIMIT: usize = 64;
    let dir = case_dir("limit-read-action");
    let path = dir.join("j.jsonl");
    {
        let mut j = JsonlJournal::<Act, St>::create_with_limit(&path, LIMIT).unwrap();
        j.append_action(&act(1, "a")).unwrap();
        j.flush().unwrap();
    }
    // 手工追加一行远超上限的 action（模拟外部写入/异常载荷）。
    let mut body = std::fs::read_to_string(&path).unwrap();
    body.push_str(&format!(
        "{{\"t\":\"a\",\"id\":9,\"label\":\"{}\"}}\n",
        "x".repeat(200)
    ));
    write_private(&path, &body);

    let j = JsonlJournal::<Act, St>::open_with_limit(&path, 1, LIMIT).unwrap();
    let mut r = j.replay().unwrap();
    assert_eq!(r.next().unwrap().unwrap(), act(1, "a"), "行 2 正常");
    match r.next() {
        Some(Err(ReplayError::RecordTooLarge { line, limit })) => {
            assert_eq!((line, limit), (3, LIMIT), "越限行报物理行号 3");
        }
        other => panic!("expected RecordTooLarge at line 3, got {other:?}"),
    }
}

/// 快照扫描面同一上限：过大快照行不得被当「无快照」静默跳过。
#[test]
fn oversized_snapshot_line_fails_restore_scan() {
    const LIMIT: usize = 64;
    let dir = case_dir("limit-restore");
    let path = dir.join("j.jsonl");
    {
        let mut j = JsonlJournal::<Act, St>::create_with_limit(&path, LIMIT).unwrap();
        j.append_action(&act(1, "a")).unwrap();
        j.flush().unwrap();
    }
    let mut body = std::fs::read_to_string(&path).unwrap();
    body.push_str(&format!("{{\"t\":\"s\",\"count\":{}}}\n", "1".repeat(200)));
    write_private(&path, &body);

    let j = JsonlJournal::<Act, St>::open_with_limit(&path, 1, LIMIT).unwrap();
    match j.restore_stream() {
        Err(JournalError::RecordTooLarge { line, limit }) => {
            assert_eq!((line, limit), (Some(3), LIMIT));
        }
        Err(e) => panic!("expected RecordTooLarge(line:Some(3)), got {e:?}"),
        Ok(_) => panic!("expected RecordTooLarge(line:Some(3)), got Ok（扫描面必须 fail-closed）"),
    }
}

// ── #178：记录形状 fail-closed ────────────────────────────────────────────

/// 非对象 / 缺 `t` / 非字符串 `t` 都是 writer 产不出的形状 ⇒ 带行号 `Corrupt`，
/// 且不得计入 `skipped_unknown`。
#[test]
fn records_without_object_string_tag_fail_closed() {
    for row in [
        "42",              // 标量
        "[1,2]",           // 数组
        "\"text\"",        // 字符串
        "null",            // null
        "{\"id\":1}",      // 缺 tag
        "{\"t\":1}",       // 数字 tag
        "{\"t\":null}",    // null tag
        "{\"t\":[\"a\"]}", // 非字符串 tag
        "{not json",       // 坏 JSON
    ] {
        let dir = case_dir("shape");
        let path = dir.join("j.jsonl");
        write_private(&path, &format!("{{\"v\":1}}\n{row}\n"));
        let j = JsonlJournal::<Act, St>::open(&path, 1).unwrap();
        let mut r = j.replay().unwrap();
        match r.next() {
            Some(Err(ReplayError::Corrupt { line, .. })) => {
                assert_eq!(line, 2, "行号必须是物理行 2（row={row}）");
            }
            other => panic!("row {row} 必须 fail-closed，实际: {other:?}"),
        }
        assert_eq!(r.skipped_unknown(), 0, "row {row} 不得计入 skipped_unknown");
    }
}

/// 合法的跳过面：`"s"`（快照）与未知**字符串** tag 各计 1，action 正常回放。
#[test]
fn snapshot_and_unknown_string_tag_are_skipped_and_counted() {
    let dir = case_dir("shape-skip");
    let path = dir.join("j.jsonl");
    write_private(
        &path,
        "{\"v\":1}\n\
         {\"t\":\"future_tag\",\"x\":1}\n\
         {\"t\":\"s\",\"count\":7}\n\
         {\"t\":\"a\",\"id\":5,\"label\":\"ok\"}\n",
    );
    let j = JsonlJournal::<Act, St>::open(&path, 1).unwrap();
    let mut r = j.replay().unwrap();
    assert_eq!(r.next().unwrap().unwrap(), act(5, "ok"));
    assert!(r.next().is_none());
    assert_eq!(r.skipped_unknown(), 2, "未知 string tag + 快照行各 1");
}

// ── #177：版本头不计入 skipped_unknown，行号仍为物理行号 ────────────────

#[test]
fn replay_consumes_header_without_counting() {
    // header-only：0 条 item、skip=0。
    let dir = case_dir("header-only");
    let path = dir.join("j.jsonl");
    write_private(&path, "{\"v\":1}\n");
    let j = JsonlJournal::<Act, St>::open(&path, 1).unwrap();
    let mut r = j.replay().unwrap();
    assert!(r.next().is_none());
    assert_eq!(r.skipped_unknown(), 0, "header-only 账本 skip 必须为 0");

    // 一条 action：skip=0；首条 action 的物理行号为 2（紧随其后的坏行报 3）。
    let one = dir.join("one.jsonl");
    write_private(
        &one,
        "{\"v\":1}\n{\"t\":\"a\",\"id\":1,\"label\":\"a\"}\n{bad\n",
    );
    let j = JsonlJournal::<Act, St>::open(&one, 1).unwrap();
    let mut r = j.replay().unwrap();
    assert_eq!(r.next().unwrap().unwrap(), act(1, "a"));
    assert_eq!(r.skipped_unknown(), 0, "一条 action 的 skip 必须为 0");
    match r.next() {
        Some(Err(ReplayError::Corrupt { line, .. })) => {
            assert_eq!(line, 3, "行号按物理行计（header=1, action=2）");
        }
        other => panic!("expected Corrupt at line 3, got {other:?}"),
    }

    // 一条快照 / 一条未知 tag：各 1。
    for (name, row) in [
        ("snapshot", "{\"t\":\"s\",\"count\":1}"),
        ("unknown", "{\"t\":\"v2_tag\"}"),
    ] {
        let p = dir.join(format!("{name}.jsonl"));
        write_private(&p, &format!("{{\"v\":1}}\n{row}\n"));
        let j = JsonlJournal::<Act, St>::open(&p, 1).unwrap();
        let mut r = j.replay().unwrap();
        assert!(r.next().is_none());
        assert_eq!(r.skipped_unknown(), 1, "{name} 行应计 1");
    }
}

/// 回放也校验头行：版本不符走 `VersionMismatch`（不是静默按当前版本播）。
#[test]
fn replay_validates_header_version() {
    let dir = case_dir("replay-header-version");
    let path = dir.join("j.jsonl");
    write_private(&path, "{\"v\":1}\n{\"t\":\"a\",\"id\":1,\"label\":\"a\"}\n");
    let j = JsonlJournal::<Act, St>::open(&path, 1).unwrap();
    // 文件被外部改写为 v:2 后，已挂载实例的 replay 必须拒绝而不是按 v:1 播。
    write_private(&path, "{\"v\":2}\n{\"t\":\"a\",\"id\":1,\"label\":\"a\"}\n");
    match j.replay() {
        Err(JournalError::VersionMismatch { found, expected }) => {
            assert_eq!((found, expected), (2, 1));
        }
        Err(e) => panic!("expected VersionMismatch, got {e:?}"),
        Ok(_) => panic!("expected VersionMismatch, got Ok"),
    }
}

// ── #106 ②：头行必须有换行终止 ───────────────────────────────────────────

#[test]
fn open_requires_newline_terminated_header() {
    let dir = case_dir("header-framing");
    let truncated = dir.join("truncated.jsonl");
    write_private(&truncated, "{\"v\":1}"); // 缺终止换行
    match JsonlJournal::<Act, St>::open(&truncated, 1) {
        Err(JournalError::HeaderUnterminated) => {}
        res => panic!("expected HeaderUnterminated, got {res:?}"),
    }
    let empty = dir.join("empty.jsonl");
    write_private(&empty, "");
    match JsonlJournal::<Act, St>::open(&empty, 1) {
        Err(JournalError::HeaderUnterminated) => {}
        res => panic!("expected HeaderUnterminated on empty file, got {res:?}"),
    }
}

// ── #181：Unix 权限（Windows 无 POSIX 权限位，隐私靠父目录 ACL 继承） ────

#[cfg(unix)]
mod unix_umask {
    #[cfg(target_os = "macos")]
    type ModeT = u16;
    #[cfg(not(target_os = "macos"))]
    type ModeT = u32;

    extern "C" {
        fn umask(mask: ModeT) -> ModeT;
    }

    /// 设置进程 umask 并返回旧值（仅测试用；进程级全局）。
    pub fn set(mask: u16) -> u16 {
        unsafe { umask(mask as ModeT) as u16 }
    }
}

/// 最宽松 umask（000）下新建账本仍是 0600——`OpenOptionsExt::mode` 独立于 umask
/// （umask 只能清位，故结果非 0600 即说明 mode 未被设置）。
#[cfg(unix)]
#[test]
fn new_journal_is_private_under_permissive_umask() {
    use std::os::unix::fs::PermissionsExt as _;

    let dir = case_dir("unix-umask");
    let path = dir.join("j.jsonl");
    let previous = unix_umask::set(0o000);
    let created = JsonlJournal::<Act, St>::create(&path);
    unix_umask::set(previous);
    created.unwrap();

    let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
    assert_eq!(
        mode & 0o077,
        0,
        "新建账本不得 group/other 可访问，实际 {mode:o}"
    );
    assert_eq!(mode, 0o600, "Unix 新建账本恒 0600");
}

/// 已有文件带 group/other 可读位 ⇒ typed 拒绝（**不隐式 chmod**，也不改内容）。
#[cfg(unix)]
#[test]
fn open_rejects_group_readable_existing_journal() {
    use std::os::unix::fs::PermissionsExt as _;

    let dir = case_dir("unix-existing");
    let path = dir.join("j.jsonl");
    std::fs::write(&path, "{\"v\":1}\n").unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();

    match JsonlJournal::<Act, St>::open(&path, 1) {
        Err(JournalError::InsecurePermissions { path: p, mode }) => {
            assert_eq!(p, path);
            assert_eq!(mode, 0o644);
        }
        res => panic!("expected InsecurePermissions, got {res:?}"),
    }
    let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o644, "open 不得静默改权限");
}

// ── #212：create-new 语义（失败注入路径见 src/journal.rs 单测） ───────────

/// create 撞已存在文件：AlreadyExists、不删不覆盖、目录里不留临时文件。
#[test]
fn create_preserves_preexisting_file_and_leaves_no_temp() {
    let dir = case_dir("create-existing");
    let path = dir.join("j.jsonl");
    write_private(&path, "caller-owned\n");
    match JsonlJournal::<Act, St>::create(&path) {
        Err(JournalError::Io { source, .. }) => {
            assert_eq!(source.kind(), std::io::ErrorKind::AlreadyExists);
        }
        res => panic!("expected Io(AlreadyExists), got {res:?}"),
    }
    assert_eq!(std::fs::read_to_string(&path).unwrap(), "caller-owned\n");
    assert_eq!(
        std::fs::read_dir(&*dir).unwrap().count(),
        1,
        "不得残留临时文件"
    );
}
