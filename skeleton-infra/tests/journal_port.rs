#![cfg(feature = "journal")]
//! JournalPort 契约端口验收：`JsonlJournal` 兑现 `skeleton::JournalPort`——
//! 后端自换路径成立（宿主面向零依赖端口编程，JSONL 只是参考实现）。

use serde::{Deserialize, Serialize};
use skeleton::{JournalPort, Restore};
use skeleton_infra::journal::JsonlJournal;
use std::path::{Path, PathBuf};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct Act {
    id: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct St {
    n: u64,
}

/// 独立子目录；通过用例收尾 remove_dir_all（同 journal.rs case_dir 惯例）。
fn case_dir(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target-iso/journal-port-tests")
        .join(name);
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// impl 断言（编译期）：JsonlJournal 实现契约端口，且端口无 A/S 侧约束
///（零依赖端口对实现词表零假设）。
#[test]
fn jsonl_journal_impls_journal_port() {
    fn assert_impl<T: JournalPort<Act, St, Err = skeleton_infra::journal::JournalError>>() {}
    assert_impl::<JsonlJournal>();
}

/// 三态恢复语义：Empty（空账本）/ FromInitial（无快照全量流）/
/// FromSnapshot（快照+增量）。读侧可见性=已 flush 前缀（BufWriter 纪律）。
#[test]
fn journal_port_restore_three_shapes() {
    let dir = case_dir("three-shapes");
    let mut j = JsonlJournal::create(dir.join("j.jsonl")).unwrap();

    let empty: Restore<St, Act> = j.restore().unwrap();
    assert_eq!(empty, Restore::Empty, "空账本=Empty");

    JournalPort::<Act, St>::append(&mut j, &Act { id: 1 }).unwrap();
    JournalPort::<Act, St>::append(&mut j, &Act { id: 2 }).unwrap();
    j.flush().unwrap(); // 未 flush 的尾部对读侧不可见
    let full: Restore<St, Act> = j.restore().unwrap();
    assert_eq!(
        full,
        Restore::FromInitial(vec![Act { id: 1 }, Act { id: 2 }]),
        "无快照行=自 initial 全量播"
    );

    JournalPort::<Act, St>::snapshot(&mut j, &St { n: 2 }).unwrap(); // 快照即写即刷
    JournalPort::<Act, St>::append(&mut j, &Act { id: 3 }).unwrap();
    j.flush().unwrap();
    let delta: Restore<St, Act> = j.restore().unwrap();
    assert_eq!(
        delta,
        Restore::FromSnapshot(St { n: 2 }, vec![Act { id: 3 }]),
        "快照后动作=增量"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
