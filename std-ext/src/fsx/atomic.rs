//! 崩溃安全原子写：同目录临时件 → `sync_all` → rename 原子替换。
//!
//! 坑面 armor（docs.rs/Win32 事实逐条对表，2026-10-17 核对，tests 钉死；
//! 对照表见 reports/2026-10-17-config-file.md）：
//!
//! - **rename 覆盖语义**（std 文档原文核对，纠正「Windows 不能覆盖」旧
//!   认知）：全平台「replacing the original file if `to` already exists」
//!   ——文件对文件覆盖**原生原子**（Win10 1607+ 走 `FileRenameInfoEx`
//!   POSIX 语义，旧路径 `MoveFileExW`+`MOVEFILE_REPLACE_EXISTING`）。
//!   **不先删旧**：删旧会引入「目标缺失窗口」，毁掉原子性这一卖点本身。
//! - **`to` 为已存在目录恒败**（两平台同）：fail-visible 显式上浮，不吞
//!   不兜底。
//! - **跨分区 rename 失败**（EXDEV）：同目录临时件从根上排除——临时件与
//!   目标同目录=同卷，rename 恒原子。
//! - **fsync 序**：临时件 `sync_all`（数据+元数据）先于 rename；rename 后
//!   Unix 尽力 sync 父目录（`File::open` 目录 + `sync_all`，Linux 有效，
//!   macOS EINVAL 可能——尽力面不决定返回值：rename 已成功，可见状态=
//!   写入成功，此步只提高掉电后目录项可见性）；Windows 无目录 fd，目录
//!   项 sync 不做（文件 sync_all+NTFS 日志给的持久度如实声明上限）。
//! - **临时件残留**：进程崩溃在 rename 前会留 `atomic-{pid}-{seq}{nanos}`
//!   于同目录（崩溃路径无人清扫是物理事实；命名可识别，可手扫/启动扫）。
//!   本函数**错误路径**（写失败/rename 败）自清临时件后以原错误上浮（best
//!   effort 清理，清理失败不掩盖原错误）。
//! - **权限继承**：新文件=umask 新建语义（Unix `0666 & ~umask` / Windows
//!   继承父目录 ACL），**不继承**被替换件原 mode/显式 ACE——声明上限，
//!   需要保权的采用方写后自理。目标只读位（Windows `READONLY`）：替换被
//!   拒 `PermissionDenied` 显式上浮（清位重试属删除域 rm_all 装甲，写域
//!   如实报错由采用方拍板）。
//! - **并发写同目标**：各写者独立临时件，rename 由 OS 原子线性化=最后赢；
//!   读者恒见旧内容或新内容（无半写中间态）。Windows 目标被第三方占开时
//!   替换成败取决于占用方共享模式（std 自开文件含 DELETE 共享；独占打开
//!   则 `PermissionDenied` 显式上浮）。
//!
//! 常驻零依赖件：纯 std，无 feature 门。分配面（counting allocator 口径，
//! 采集器 examples/config_atomic_multidim.rs）：每调用自有分配=临时件名
//! `String` 1 + 路径拼接 `PathBuf` 2（tmp/结果）；`bytes` 借用零拷贝。

use std::fs;
#[cfg(unix)]
use std::fs::File;
use std::io::{self, Write};
use std::path::Path;

use super::unique_temp_name;

/// 原子写 `bytes` 到 `path`：同目录临时件（write+`sync_all`）→ rename 原子
/// 替换。读者视角要么旧内容要么新内容，恒无半写中间态（语义/armor 见模块
/// 头注）。
///
/// 父目录必须已存在（fail-visible，不隐式 mkdir）；错误路径自清临时件。
///
/// # Examples
///
/// ```no_run
/// use std_ext::fsx::atomic_write;
///
/// atomic_write("app.json", br#"{"port":8080}"#).unwrap();
/// ```
pub fn atomic_write(path: impl AsRef<Path>, bytes: &[u8]) -> io::Result<()> {
    let path = path.as_ref();
    // 父目录解析：无父段（"config.toml"）→ CWD（临时件仍与目标同目录=同卷，
    // rename 恒不跨分区）
    let parent = match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    };
    let tmp = parent.join(unique_temp_name("atomic-"));
    let write = || -> io::Result<()> {
        // create_new 独占：撞名显式败（进程内原子序号根除撞名面，同 temp_dir_in）
        let mut f = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp)?;
        f.write_all(bytes)?;
        // fsync 数据+元数据先于 rename（掉电序：临时件先于目录项持久）
        f.sync_all()
    };
    if let Err(err) = write() {
        let _ = fs::remove_file(&tmp); // 失败自清（best-effort，原错误优先不掩盖）
        return Err(err);
    }
    if let Err(err) = fs::rename(&tmp, path) {
        let _ = fs::remove_file(&tmp); // rename 败同清：错误路径不留临时件
        return Err(err);
    }
    sync_parent_best_effort(parent);
    Ok(())
}

/// rename 落盘后的目录项 sync（Unix 尽力面，语义见模块头注）：失败不改变
/// 返回值——rename 已成功，可见状态=写入成功；此步只提高掉电后目录项可见性。
#[cfg(unix)]
fn sync_parent_best_effort(parent: &Path) {
    if let Ok(dir) = File::open(parent) {
        let _ = dir.sync_all();
    }
}

#[cfg(not(unix))]
fn sync_parent_best_effort(_parent: &Path) {}
