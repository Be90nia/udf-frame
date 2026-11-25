//! 崩溃安全原子写：同目录临时件 → `sync_all` → rename 原子替换。
//!
//! 坑面 armor（docs.rs/Win32 事实逐条对表，2026-10-17 核对，tests 钉死；
//! 对照表见 配置文件面（本地留存，未入库））：
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
//! - **临时件残留与所有权**：进程崩溃在 rename 前会留 `atomic-{pid}-{seq}
//!   {nanos}` 于同目录（崩溃路径无人清扫是物理事实；命名可识别，可手扫/
//!   启动扫）。错误路径**只清本调用创建的那一条**：`create_new` 返回句柄前
//!   不承认所有权（撞上崩溃残留/跨进程同名/共享可写目录里被他人预建的条目
//!   一律原样保留，只上浮 `AlreadyExists`）；句柄到手后才挂所有权守卫，写/
//!   同步/rename 失败由守卫清那一条，清理失败不掩盖原错误（best effort）。
//!   `create_new`（`O_EXCL`）不穿既有符号链接：预置的链接/悬空链接一律显式
//!   败——既不会顺链接写到别处，也不会被误删。
//! - **权限策略（#215：显式选择，不改旧入口默认）**：三策略见
//!   [`AtomicWritePolicy`]——`Inherit`=umask/父目录 ACL 的兼容语义（即
//!   [`atomic_write`]）；`Private`=Unix 建件即 0600、Windows 沿用父目录继承
//!   ACL（父目录非本用户独占时不构成私有，如实声明）；`PreserveExisting`
//!   =Unix 按目标现有 mode 建件（`symlink_metadata` 不穿链接；目标缺失回落
//!   Inherit，目标非普通文件显式失败），Windows 无显式 ACL 通道 ⇒
//!   `Unsupported`。权限在**建临时件时**定死，rename 发布同一 inode ⇒ 无权限
//!   窗口；umask 只能收窄请求位，故事后不 chmod（那会引入写后授权窗口）。
//!   目标只读位（Windows `READONLY`）：替换被
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
use std::path::{Path, PathBuf};

use super::unique_temp_name;

/// 原子写 `bytes` 到 `path`：同目录临时件（write+`sync_all`）→ rename 原子
/// 替换。读者视角要么旧内容要么新内容，恒无半写中间态（语义/armor 见模块
/// 头注）。等价于 [`atomic_write_with`] + [`AtomicWritePolicy::Inherit`]——权限
/// 沿 umask/父目录 ACL 的兼容语义，不静默改默认。
///
/// 父目录必须已存在（fail-visible，不隐式 mkdir）；错误路径只自清**本调用
/// 创建**的临时件（`create_new` 失败时不动任何既有条目）。
///
/// # Examples
///
/// ```no_run
/// use std_ext::fsx::atomic_write;
///
/// atomic_write("app.json", br#"{"port":8080}"#).unwrap();
/// ```
pub fn atomic_write(path: impl AsRef<Path>, bytes: &[u8]) -> io::Result<()> {
    atomic_write_with(path, bytes, AtomicWritePolicy::Inherit)
}
/// 目标权限策略（#215：原子性与隐私策略分离，采用方显式选择）。
///
/// 权限在**临时件创建时**定死（写首字节之前），rename 发布同一 inode ⇒ 无
/// 「先建宽件再补权」的窗口；不 chmod 已发布的既有件。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AtomicWritePolicy {
    /// 兼容语义（[`atomic_write`] 即此）：临时件按进程 umask（Unix
    /// `0666 & ~umask`）/ 父目录继承 ACL（Windows）创建，**不**读取目标既有
    /// 权限——替换既有件会按进程状态改权限（声明上限，需私有选
    /// [`AtomicWritePolicy::Private`]，需保权选
    /// [`AtomicWritePolicy::PreserveExisting`]）。
    Inherit,
    /// 私有语义：Unix 临时件建时即 `0600`（umask 只能清位，故拿到的权限恒
    /// 不多于 0600）；Windows 无 mode 通道，沿用父目录继承 ACL——**父目录 ACL
    /// 非本用户独占时不构成私有**（如实声明，不谎称私有）。
    Private,
    /// 保既有权限（仅 Unix）：以目标**现有** mode 创建临时件（`symlink_metadata`
    /// 读取，不穿链接）。
    ///
    /// 语义边界：目标不存在=无可保，回落 [`AtomicWritePolicy::Inherit`]；目标
    /// 存在但不是普通文件（目录/符号链接）⇒ `InvalidInput` 显式失败（不静默
    /// 把链接换成常规文件）；请求的 mode 仍受进程 umask 收窄（只清位不加位）。
    /// 读 mode 与 rename 之间的并发 chmod 会丢失（TOCTOU 声明项）。Windows：
    /// std 无显式 ACL 创建通道 ⇒ `Unsupported` 显式失败，不声称 ACL 保留。
    PreserveExisting,
}

/// 原子写 + 显式权限策略（[`AtomicWritePolicy`]）；其余语义与 [`atomic_write`]
/// 完全一致。
pub fn atomic_write_with(
    path: impl AsRef<Path>,
    bytes: &[u8],
    policy: AtomicWritePolicy,
) -> io::Result<()> {
    atomic_write_named(path.as_ref(), bytes, policy, || unique_temp_name("atomic-"))
}

/// [`atomic_write_with`] 本体（临时件名注入仅供单测构造 `create_new` 撞名）。
fn atomic_write_named(
    path: &Path,
    bytes: &[u8],
    policy: AtomicWritePolicy,
    temp_name: impl FnOnce() -> String,
) -> io::Result<()> {
    // 父目录解析：无父段（"config.toml"）→ CWD（临时件仍与目标同目录=同卷，
    // rename 恒不跨分区）
    let parent = match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    };
    // 策略前置求值：失败（如保权遇到目录目标）在**建临时件之前**上浮=零残留
    let mode = creation_mode(path, policy)?;
    let tmp = parent.join(temp_name());
    let mut opts = fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    if let Some(mode) = mode {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(mode);
    }
    #[cfg(not(unix))]
    let _ = mode;
    // create_new 独占：撞名（崩溃残留/跨进程同名/共享可写目录里被他人预建）
    // 显式败。此路径本调用从未拥有 tmp ⇒ 直接上浮，不做任何删除。
    let mut file = opts.open(&tmp)?;
    // 至此拥有 tmp：守卫在写/同步/rename 任一步失败时只清这一条
    let mut owned = TempPathGuard::new(tmp);
    file.write_all(bytes)?;
    // fsync 数据+元数据先于 rename（掉电序：临时件先于目录项持久）
    file.sync_all()?;
    drop(file);
    fs::rename(owned.path(), path)?;
    owned.disarm();
    sync_parent_best_effort(parent);
    Ok(())
}

/// 临时件创建模式（Unix 权限位；`None`=按 umask 新建）。权限在创建时定死，
/// rename 发布同一 inode ⇒ 无权限窗口。
#[cfg(unix)]
fn creation_mode(path: &Path, policy: AtomicWritePolicy) -> io::Result<Option<u32>> {
    use std::os::unix::fs::MetadataExt;
    Ok(match policy {
        AtomicWritePolicy::Inherit => None,
        AtomicWritePolicy::Private => Some(0o600),
        AtomicWritePolicy::PreserveExisting => match fs::symlink_metadata(path) {
            Ok(md) if md.file_type().is_file() => Some(md.mode() & 0o7777),
            Ok(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "preserve-existing policy requires a regular-file destination",
                ));
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => None,
            Err(err) => return Err(err),
        },
    })
}

/// 非 Unix：`Inherit`/`Private` 同面（父目录继承 ACL），保权无通道 ⇒ 显式失败。
#[cfg(not(unix))]
fn creation_mode(_path: &Path, policy: AtomicWritePolicy) -> io::Result<Option<u32>> {
    match policy {
        AtomicWritePolicy::Inherit | AtomicWritePolicy::Private => Ok(None),
        AtomicWritePolicy::PreserveExisting => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "preserve-existing policy is not supported on this platform",
        )),
    }
}

/// 临时件所有权守卫（#216）：**只在 `create_new` 成功返回句柄后构造**——此前
/// 本调用从未拥有该路径，删它就是删别人的文件。Drop 时清本调用创建的那一条；
/// 清理失败不掩盖调用方原错误（best-effort，原错误优先）。
struct TempPathGuard {
    path: PathBuf,
    armed: bool,
}

impl TempPathGuard {
    fn new(path: PathBuf) -> Self {
        Self { path, armed: true }
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for TempPathGuard {
    fn drop(&mut self) {
        if self.armed {
            let _ = fs::remove_file(&self.path); // best-effort，原错误优先
        }
    }
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

#[cfg(test)]
mod ownership_tests {
    use super::*;

    /// 单测落点：lib 单测拿不到 `CARGO_TARGET_TMPDIR`（只给集成测试/bench
    /// 目标），仍守产物红线——落在 crate 的 gitignored `target/` 下。
    fn scratch(tag: &str) -> PathBuf {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join(format!("fsx-atomic-unit-{tag}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        root
    }

    // `create_new` 撞上既有同名条目 ⇒ 该条目非本调用创建，必须原样保留，
    // 失败只上浮原错误。
    #[test]
    fn create_new_collision_leaves_preexisting_entry_untouched() {
        let dir = scratch("collision");
        let target = dir.join("app.json");
        let squatter = dir.join("atomic-squatter");
        fs::write(&squatter, b"do-not-touch").unwrap();

        let err = atomic_write_named(&target, b"payload", AtomicWritePolicy::Inherit, || {
            "atomic-squatter".to_owned()
        })
        .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists, "{err}");
        assert_eq!(
            fs::read(&squatter).unwrap(),
            b"do-not-touch",
            "非本调用创建的条目不得被失败路径删除"
        );
        assert!(!target.exists(), "失败路径不得发布目标");
        let _ = fs::remove_dir_all(&dir);
    }

    // 预置临时件名是符号链接：`create_new`（O_EXCL）恒 EEXIST ⇒ 不顺链接写、
    // 也不删/替换该链接（本调用从未拥有它）。
    #[cfg(unix)]
    #[test]
    fn create_new_collision_with_symlink_touches_nothing() {
        let dir = scratch("symlink");
        let target = dir.join("app.json");
        let victim = dir.join("victim.txt");
        fs::write(&victim, b"original").unwrap();
        let squatter = dir.join("atomic-link");
        std::os::unix::fs::symlink(&victim, &squatter).unwrap();

        let err = atomic_write_named(&target, b"payload", AtomicWritePolicy::Inherit, || {
            "atomic-link".to_owned()
        })
        .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists, "{err}");
        assert_eq!(
            fs::read(&victim).unwrap(),
            b"original",
            "不得顺临时件名穿链接写目标"
        );
        assert!(
            fs::symlink_metadata(&squatter)
                .unwrap()
                .file_type()
                .is_symlink(),
            "预置链接不得被删/替换"
        );
        assert!(!target.exists());
        let _ = fs::remove_dir_all(&dir);
    }
}
