//! 文件系统受控子集：惰性目录遍历 / 树复制 / 树删除 / 工作区临时目录。
//!
//! commons-io 对应物（调研档 2026-10-14-commons-domain-survey §三.6+§4.6 拍板，
//! P2 但已拍板全做）：对标 walkdir 684M（惰性遍历形）/fs_extra 313M（树复制）/
//! tempfile 886M（RAII 临时目录）——各取其形，坑面装甲为本域自有。内部需求先例：
//! journal 测试 m16 拍板「不引 tempfile dev-dep，std::fs 手工建删」——手搓临时
//! 目录即本域的第一消费者。
//!
//! 坑面拍板（实现前系统排查，对照表见 fsx 域性能锚（本地留存，未入库））：
//!
//! - **symlink 环**（S1 教训：walk 只认 `is_dir` 被穿透）：条目类型恒取
//!   `read_dir` 的不穿透类型（symlink 即 symlink）；跟随策略显式参数，默认不
//!   跟随（环不可达）；跟随模式全目录身份比对（Unix dev+ino / Windows
//!   canonicalize 解析路径，same-file 语义）建祖先栈，命中即 `WalkError::Loop`
//!   且不下钻，遍历继续。**有意分歧一处**：walkdir 根路径恒穿透（文档特例），本域根路
//!   径穿透性同样遵循 `follow_links`——特例即坑，一致性优先。
//! - **TOCTOU**：lstat 与使用之间条目可被并发改写——不可消除的固有权衡，装甲
//!   =全路径 fail-visible（错误显式上浮禁吞）+零缓存判定（每步现查）。
//! - **部分失败中间态**：`copy_tree` 中途首错即停，已处理量以计数显式申报
//!   （`CopyError::Partial`/`Cycle` 携带 report），禁 fs_extra 式静默半途。
//! - **Windows 260 路径长度**：本域零归一零剥离（归一面在 [`crate::paths`]），
//!   verbatim `\\?\` 形态原样透传（`PathBuf::push` 保留前缀），>260 深树以
//!   verbatim 根遍历可达（集成测试实测钉死）。
//! - **只读属性文件删除失败**：Windows `FILE_ATTRIBUTE_READONLY` 历史上使
//!   `remove_file`/remove_dir_all 的 DELETE 处置 ACCESS_DENIED——近版 std 已改
//!   （RELEASES：「remove_file will now remove read-only files」，实测 1.96 直删
//!   成功），[`rm_all`] 装甲退为旧工具链/其余 PermissionDenied 面的防线：首次
//!   `PermissionDenied` 时清只读属性后重试原语恰一轮，仍败以原错误显式上浮。
//!   Unix 只读目录删除 EACCES 属 OS 语义，不自动 chmod（如实上浮，声明上限）。
//! - **临时目录同 pid 并发竞态**：命名=前缀+pid+进程内原子序号+挂钟纳秒（同
//!   进程不撞/跨进程 pid 隔离/pid 回收由纳秒兜底），创建走 `create_dir` 独占
//!   语义（存在即 AlreadyExists 显式上浮，瞬时竞态由原子序号根除）。
//!
//! log-embed 埋点（级别规范：正常流转=debug / 异常路径=warn；target
//! `skeleton::fsx[::phase]`，字段化/零调用点分配纪律见 lib.rs `embed_event!`
//! 注）：walk 帧=帧推进 debug（depth/排序模式）；copy 树=逐项 debug
//!（kind/items/bytes 累计）+ 部分失败 warn（copied/path/error）；tempdir=
//! Drop 清理失败 warn（path/error）。
//!
//! 常驻零依赖件：纯 std，无 feature 门；错误手写同形 `Display`/`Error`
//! （text/encode 域同门风格）。
//!
//! 性能七维收窄声明（单线程 IO 域）：适用维度=①分配声明（每 entry 恰 1 个
//! 自有分配=yield 的 owned `DirEntry` 路径本体；每目录帧 +1 帧路径；排序模式
//! +1/entry 收集缓冲——std 侧 `file_name` 复制等平台内部件不归本域，锚见报告）
//! ②遍历吞吐（文件数梯度 100/1k/10k）③深度梯度；④并发曲线⑤锁⑥内存水位
//! ⑦伪共享=n/a（无锁无线程面；IO 吞吐不做平台间绝对值对标，只报本机锚）。
//! 采集器 `examples/fsx_multidim.rs`，报告：fsx 域性能锚（本地留存，未入库）。

use std::fs::{self, FileType, Metadata};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

// ── walk：惰性目录遍历 ──

/// 目录遍历失败（IO 与环分立：环带祖先路径便于诊断，禁只给 io::Error）。
#[derive(Debug)]
pub enum WalkError {
    /// 条目元数据/枚举 IO 错误（路径回传定位）。
    Io { path: PathBuf, err: io::Error },
    /// 符号链接环：`path` 与祖先 `ancestor` 文件身份相同（仅跟随模式可达）。
    Loop { path: PathBuf, ancestor: PathBuf },
}

impl std::fmt::Display for WalkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WalkError::Io { path, err } => write!(f, "walk {}: {err}", path.display()),
            WalkError::Loop { path, ancestor } => write!(
                f,
                "symlink loop: {} resolves to ancestor {}",
                path.display(),
                ancestor.display()
            ),
        }
    }
}

impl std::error::Error for WalkError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            WalkError::Io { err, .. } => Some(err),
            WalkError::Loop { .. } => None,
        }
    }
}

/// 遍历条目：路径（借用）/深度（根=0）/不穿透文件类型/按策略解读的元数据。
#[derive(Debug, Clone)]
pub struct DirEntry {
    path: PathBuf,
    depth: usize,
    file_type: FileType,
    follow: bool,
}

impl DirEntry {
    /// 条目路径。跟随模式下符号链接条目的 file_type 为穿透后类型。
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// 相对根的深度：根=0，根的直接子项=1。
    pub fn depth(&self) -> usize {
        self.depth
    }

    /// 条目类型：默认模式不穿透（symlink 即 symlink，S1 装甲）；跟随模式下
    /// 符号链接显示为穿透后类型。
    pub fn file_type(&self) -> FileType {
        self.file_type
    }

    /// 条目元数据：按遍历的跟随策略解读（跟随→`fs::metadata`；不跟随→
    /// `fs::symlink_metadata`），与 `file_type` 同语义无二次口径。
    pub fn metadata(&self) -> io::Result<Metadata> {
        if self.follow {
            fs::metadata(&self.path)
        } else {
            fs::symlink_metadata(&self.path)
        }
    }
}

/// 目录内条目排序策略（显式参数，无默认顺序承诺——OS 顺序即 Darwin 坑）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sort {
    /// 不排序：按 `read_dir` 原生顺序产出（最快；顺序不做任何承诺）。
    None,
    /// 按文件名排序（`OsStr` 字节序，逐目录内排序；逐目录惰性收集）。
    ByName,
}

/// 惰性目录遍历器（walkdir 形）：builder 即迭代器，首次 `next()` 才触 IO。
///
/// 深度/排序/符号链接策略全部显式参数，默认=全深度/不排序/不跟随。
///
/// # Examples
///
/// ```
/// use std_ext::fsx::{walk, Sort};
///
/// // 构造零 IO；根不存在在首次 next() 以显式错误上浮（fail-visible）。
/// let mut it = walk("definitely/missing/root").max_depth(2).sort(Sort::ByName);
/// assert!(it.next().unwrap().is_err());
/// ```
pub fn walk(root: impl AsRef<Path>) -> Walk {
    Walk {
        root: root.as_ref().to_path_buf(),
        min_depth: 0,
        max_depth: usize::MAX,
        follow: false,
        sort: Sort::None,
        state: None,
    }
}

/// 遍历器状态（`Walk` 的 builder+iterator 合一面，见 [`walk`]）。
#[derive(Debug)]
pub struct Walk {
    root: PathBuf,
    min_depth: usize,
    max_depth: usize,
    follow: bool,
    sort: Sort,
    state: Option<WalkState>,
}

impl Walk {
    /// 最大下钻深度（根=0；`max_depth(0)` 只产出根本身）。
    pub fn max_depth(mut self, depth: usize) -> Walk {
        self.max_depth = depth;
        self
    }

    /// 最小产出深度（默认 0 含根本身；`min_depth(1)` 跳过根）。
    pub fn min_depth(mut self, depth: usize) -> Walk {
        self.min_depth = depth;
        self
    }

    /// 符号链接跟随策略（默认 `false` 不穿透——S1 装甲；`true` 时启用身份
    /// 祖先栈环检测）。根路径穿透性同样遵循本策略（有意分歧 walkdir 特例）。
    pub fn follow_links(mut self, follow: bool) -> Walk {
        self.follow = follow;
        self
    }

    /// 目录内条目排序策略（默认 [`Sort::None`]）。
    pub fn sort(mut self, sort: Sort) -> Walk {
        self.sort = sort;
        self
    }

    fn next_entry(&mut self) -> Option<Result<DirEntry, WalkError>> {
        let Walk {
            root,
            min_depth,
            max_depth,
            follow,
            sort,
            state,
        } = self;
        if state.is_none() {
            match WalkState::init(root, *follow, *sort) {
                Ok((st, first)) => {
                    *state = Some(st);
                    if let Some(e) = first {
                        if e.depth >= *min_depth {
                            return Some(Ok(e));
                        }
                    }
                }
                Err(err) => {
                    // 根失败只上浮一次：置空栈状态，后续 next() 恒 None
                    *state = Some(WalkState::exhausted(*follow));
                    return Some(Err(err));
                }
            }
        }
        let st = state.as_mut().expect("state initialized above");
        loop {
            let frame = st.stack.last_mut()?;
            let raw = frame.next();
            let std_entry = match raw {
                None => {
                    st.stack.pop();
                    if st.follow {
                        st.ancestors.pop();
                    }
                    continue;
                }
                Some(Err(err)) => {
                    return Some(Err(WalkError::Io {
                        path: frame.dir_path.clone(),
                        err,
                    }));
                }
                Some(Ok(e)) => e,
            };
            let depth = frame.depth + 1;
            if depth > *max_depth {
                // 深度产出帽：越界条目既不产出也不下钻（max_depth 语义=产出帽，
                // walkdir 同语义——max_depth(0) 只产出根本身）
                continue;
            }
            // 每 entry 恰 1 次分配：clone 目录路径后 push 名字，零中间串
            let mut child = frame.dir_path.clone();
            child.push(std_entry.file_name());
            let etype = match std_entry.file_type() {
                Ok(t) => t,
                Err(err) => return Some(Err(WalkError::Io { path: child, err })),
            };
            // 跟随模式：symlink 条目解析穿透后类型（一次 stat，链接稀少成本可控）
            let resolved_ft = if *follow && etype.is_symlink() {
                match fs::metadata(&child) {
                    Ok(m) => m.file_type(),
                    Err(err) => return Some(Err(WalkError::Io { path: child, err })),
                }
            } else {
                etype
            };
            let descend = resolved_ft.is_dir();
            // 环检测只对将下钻的目录做（跟随模式）：身份与祖先栈比对
            let mut loop_hit: Option<PathBuf> = None;
            let mut identity: Option<FileIdentity> = None;
            if descend && *follow {
                match identity_of(&child) {
                    Ok(id) => {
                        if let Some((_, anc_path)) = st.ancestors.iter().find(|(aid, _)| *aid == id)
                        {
                            loop_hit = Some(anc_path.clone());
                        }
                        identity = Some(id);
                    }
                    Err(err) => return Some(Err(WalkError::Io { path: child, err })),
                }
            }
            if let Some(ancestor) = loop_hit {
                return Some(Err(WalkError::Loop {
                    path: child,
                    ancestor,
                }));
            }
            if descend {
                match DirFrame::open(&child, depth, *sort) {
                    Ok(f) => st.stack.push(f),
                    Err(err) => return Some(Err(WalkError::Io { path: child, err })),
                }
                // 埋点「遍历帧推进」：下钻目录每帧一条（深度/排序模式入字段）。
                embed_event!(debug, target: "skeleton::fsx::walk", depth = depth as u64, sort = ?sort);
                if let Some(id) = identity {
                    st.ancestors.push((id, child.clone()));
                }
            }
            let entry = DirEntry {
                path: child,
                depth,
                file_type: resolved_ft,
                follow: *follow,
            };
            if depth >= *min_depth {
                return Some(Ok(entry));
            }
        }
    }
}

impl Iterator for Walk {
    type Item = Result<DirEntry, WalkError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_entry()
    }
}

/// 文件身份（环检测祖先栈比对，same-file 语义）。Unix=dev+ino（stable
/// `MetadataExt`）；Windows=canonicalize 全链接解析后路径——卷序号/文件索引
/// 面（`windows_by_handle`）nightly-only，stable 以 canonical 路径唯一性
/// 等价替代（同一目录恒同一 canonical 路径，环判定语义不变）。
#[cfg(unix)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileIdentity {
    dev: u64,
    ino: u64,
}

#[cfg(windows)]
#[derive(Debug, Clone, PartialEq, Eq)]
struct FileIdentity(PathBuf);

#[cfg(unix)]
fn identity_of(path: &Path) -> io::Result<FileIdentity> {
    use std::os::unix::fs::MetadataExt;
    let md = fs::metadata(path)?;
    Ok(FileIdentity {
        dev: md.dev(),
        ino: md.ino(),
    })
}

#[cfg(windows)]
fn identity_of(path: &Path) -> io::Result<FileIdentity> {
    fs::canonicalize(path).map(FileIdentity)
}

/// 目录读流形态：原生流（不排序）或逐目录收集后的有序序列（排序模式）。
// `Stream` 变体大（std `ReadDir` 内嵌平台缓冲 ~600B）属 std 本体尺寸；Box 化
// 会引入每目录 1 次堆分配，违模块头注①分配声明——保留栈上形态。
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
enum FrameIter {
    Stream(fs::ReadDir),
    Sorted(std::vec::IntoIter<fs::DirEntry>),
}

/// DFS 栈帧：一个打开目录的读流+其路径（owned，每目录 1 分配）+深度。
#[derive(Debug)]
struct DirFrame {
    iter: FrameIter,
    dir_path: PathBuf,
    depth: usize,
}

impl DirFrame {
    fn open(path: &Path, depth: usize, sort: Sort) -> io::Result<DirFrame> {
        let rd = fs::read_dir(path)?;
        let iter = match sort {
            Sort::None => FrameIter::Stream(rd),
            Sort::ByName => {
                let mut entries: Vec<fs::DirEntry> = rd.collect::<io::Result<Vec<_>>>()?;
                // sort_by_key 提键次数 = O(n log n) 次比较各 1 次（实测 sorted
                // walk ~+2 allocs/entry），仍优于 sort_by 每比较闭包内的
                // file_name 调用面——排序模式分配面更小
                entries.sort_by_key(|e| e.file_name());
                FrameIter::Sorted(entries.into_iter())
            }
        };
        Ok(DirFrame {
            iter,
            dir_path: path.to_path_buf(),
            depth,
        })
    }

    fn next(&mut self) -> Option<io::Result<fs::DirEntry>> {
        match &mut self.iter {
            FrameIter::Stream(rd) => rd.next(),
            FrameIter::Sorted(it) => it.next().map(Ok),
        }
    }
}

#[derive(Debug)]
struct WalkState {
    stack: Vec<DirFrame>,
    /// 跟随模式的身份+路径祖先栈（与 DFS 栈同构；根身份在初始化时压入）。
    ancestors: Vec<(FileIdentity, PathBuf)>,
    follow: bool,
}

impl WalkState {
    /// 初始化：根元数据分类（lstat 一次，跟随模式 symlink 根再 stat 一次），
    /// 目录根开帧并压入身份。根 IO 失败=遍历终止错误。
    fn init(
        root: &Path,
        follow: bool,
        sort: Sort,
    ) -> Result<(WalkState, Option<DirEntry>), WalkError> {
        let io_err = |err: io::Error| WalkError::Io {
            path: root.to_path_buf(),
            err,
        };
        let md = fs::symlink_metadata(root).map_err(io_err)?;
        let etype = md.file_type();
        let resolved_ft = if follow && etype.is_symlink() {
            fs::metadata(root).map_err(io_err)?.file_type()
        } else {
            etype
        };
        let first = DirEntry {
            path: root.to_path_buf(),
            depth: 0,
            file_type: resolved_ft,
            follow,
        };
        if !resolved_ft.is_dir() {
            // 根非目录：只产出根本身（跟随策略一致语义：symlink 根不穿透）
            return Ok((WalkState::exhausted(follow), Some(first)));
        }
        let frame = DirFrame::open(root, 0, sort).map_err(io_err)?;
        let mut st = WalkState {
            stack: vec![frame],
            ancestors: Vec::new(),
            follow,
        };
        if follow {
            let id = identity_of(root).map_err(io_err)?;
            st.ancestors.push((id, root.to_path_buf()));
        }
        Ok((st, Some(first)))
    }

    fn exhausted(follow: bool) -> WalkState {
        WalkState {
            stack: Vec::new(),
            ancestors: Vec::new(),
            follow,
        }
    }
}

// ── copy_tree：树复制（部分失败中间态显式）──

/// 树内符号链接处理策略（显式参数，禁平台隐式择一）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SymlinkPolicy {
    /// 穿透复制：链接目标内容以常规文件/目录落入目标树（遍历走环检测）。
    Follow,
    /// 保链复制：目标树内重建等价链接（悬空链接 Unix 原样保留；Windows 保
    /// 链需可解析出目标类型，悬空显式报错——平台差异如实申报）。
    Preserve,
}

/// 树复制报告：已处理量计数（成功返回值/部分失败错误均携带=中间态显式）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CopyReport {
    /// 已创建目录数（含目标根）。
    pub dirs: usize,
    /// 已复制常规文件数。
    pub files: usize,
    /// 已重建链接数（仅 `Preserve` 策略）。
    pub symlinks: usize,
    /// 已复制文件字节总数（`fs::copy` 返回值累加）。
    pub bytes: u64,
}

/// 树复制失败（首错即停；部分失败携已处理计数=中间态显式，禁静默半途）。
#[derive(Debug)]
pub enum CopyError {
    /// 复制未开始（源检查/目标根创建失败）：零中间态。
    Prepare { path: PathBuf, err: io::Error },
    /// 复制中途条目失败（IO）：`report` 为已处理量快照。
    Partial {
        report: CopyReport,
        path: PathBuf,
        err: io::Error,
    },
    /// 跟随模式命中符号链接环：`report` 为已处理量快照，环祖先回传诊断。
    Cycle {
        report: CopyReport,
        path: PathBuf,
        ancestor: PathBuf,
    },
}

impl std::fmt::Display for CopyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CopyError::Prepare { path, err } => {
                write!(f, "copy prepare {}: {err}", path.display())
            }
            CopyError::Partial { path, err, .. } => {
                write!(
                    f,
                    "copy failed at {} (partial tree left behind): {err}",
                    path.display()
                )
            }
            CopyError::Cycle { path, ancestor, .. } => write!(
                f,
                "copy hit symlink loop: {} resolves to ancestor {} (partial tree left behind)",
                path.display(),
                ancestor.display()
            ),
        }
    }
}

impl std::error::Error for CopyError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            CopyError::Prepare { err, .. } | CopyError::Partial { err, .. } => Some(err),
            CopyError::Cycle { .. } => None,
        }
    }
}

/// 递归复制目录树 `src` → `dst`（首错即停，中间态以计数显式申报）。
///
/// 语义：`dst` 必须不存在（`create_dir` 独占语义，已存在显式报错——调用方
/// 自行选择落点）；空目录照建；文件复制=`std::fs::copy` 本体语义（权限保留，
/// 时间戳不承诺）；元数据面不额外保留（声明上限，需要时调用方后续补）。
/// 遍历按文件名排序（`Sort::ByName`）——部分失败的报错点确定可复现。
///
/// # Examples
///
/// ```no_run
/// use std_ext::fsx::{copy_tree, SymlinkPolicy};
///
/// let report = copy_tree("src/tree", "dst/tree", SymlinkPolicy::Preserve).unwrap();
/// println!("{} files, {} bytes", report.files, report.bytes);
/// ```
pub fn copy_tree(
    src: impl AsRef<Path>,
    dst: impl AsRef<Path>,
    links: SymlinkPolicy,
) -> Result<CopyReport, CopyError> {
    let result = copy_tree_inner(src.as_ref(), dst.as_ref(), links);
    // 埋点「部分失败」：首错即停的中间态一条 warn（已复制计数/定位/错误原文；
    // Cycle 环=调用方确定性错误走返回值面，不埋）。多返回点收口单点。
    #[cfg(feature = "log-embed")]
    if let Err(CopyError::Partial { report, path, err }) = &result {
        embed_event!(
            warn,
            target: "skeleton::fsx::copy",
            copied = (report.dirs + report.files + report.symlinks) as u64,
            path = %path.display(),
            error = %err
        );
    }
    result
}

/// [`copy_tree`] 本体（包装层只挂部分失败埋点；路径零二次分配）。
fn copy_tree_inner(src: &Path, dst: &Path, links: SymlinkPolicy) -> Result<CopyReport, CopyError> {
    let src_md = fs::metadata(src).map_err(|err| CopyError::Prepare {
        path: src.to_path_buf(),
        err,
    })?;
    if !src_md.is_dir() {
        return Err(CopyError::Prepare {
            path: src.to_path_buf(),
            err: io::Error::new(io::ErrorKind::InvalidInput, "source is not a directory"),
        });
    }
    fs::create_dir(dst).map_err(|err| CopyError::Prepare {
        path: dst.to_path_buf(),
        err,
    })?;
    let mut report = CopyReport {
        dirs: 1,
        files: 0,
        symlinks: 0,
        bytes: 0,
    };
    let follow = matches!(links, SymlinkPolicy::Follow);
    let walker = walk(src)
        .sort(Sort::ByName)
        .follow_links(follow)
        .min_depth(1);
    for item in walker {
        let entry = match item {
            Ok(e) => e,
            Err(WalkError::Io { path, err }) => {
                return Err(CopyError::Partial { report, path, err });
            }
            Err(WalkError::Loop { path, ancestor }) => {
                return Err(CopyError::Cycle {
                    report,
                    path,
                    ancestor,
                });
            }
        };
        // walk 产物恒在 src 下；相对片段映射到 dst 侧（拼一次，零中间串）
        let rel = entry
            .path()
            .strip_prefix(src)
            .expect("walk yields only entries under root");
        let target = dst.join(rel);
        let ft = entry.file_type();
        if ft.is_dir() {
            fs::create_dir(&target).map_err(|err| CopyError::Partial {
                report,
                path: target,
                err,
            })?;
            report.dirs += 1;
            // 埋点「树复制逐项」：kind/累计 items/bytes（调试定位复制进度面）。
            embed_event!(debug, target: "skeleton::fsx::copy", kind = "dir", items = (report.dirs + report.files + report.symlinks) as u64, bytes = report.bytes);
        } else if ft.is_symlink() {
            // 仅 Preserve 策略可达（Follow 模式 file_type 为穿透后类型）
            copy_link(entry.path(), &target).map_err(|err| CopyError::Partial {
                report,
                path: target,
                err,
            })?;
            report.symlinks += 1;
            embed_event!(debug, target: "skeleton::fsx::copy", kind = "symlink", items = (report.dirs + report.files + report.symlinks) as u64, bytes = report.bytes);
        } else {
            let n = fs::copy(entry.path(), &target).map_err(|err| CopyError::Partial {
                report,
                path: target,
                err,
            })?;
            report.files += 1;
            report.bytes += n;
            embed_event!(debug, target: "skeleton::fsx::copy", kind = "file", items = (report.dirs + report.files + report.symlinks) as u64, bytes = report.bytes);
        }
    }
    Ok(report)
}

/// 保链复制：Unix `symlink(target)` 原样重建（悬空照留）；Windows 需解析出
/// 目标类型选 `symlink_dir`/`symlink_file`（悬空显式报错——平台差异申报面）。
fn copy_link(link: &Path, target: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        let dest = fs::read_link(link)?;
        std::os::unix::fs::symlink(dest, target)
    }
    #[cfg(windows)]
    {
        let dest = fs::read_link(link)?;
        if fs::metadata(link)?.file_type().is_dir() {
            std::os::windows::fs::symlink_dir(dest, target)
        } else {
            std::os::windows::fs::symlink_file(dest, target)
        }
    }
}

// ── rm_all：树/文件删除（Windows 只读装甲）──

/// 删除失败（路径回传定位；错误本体透传 std，禁吞禁包装）。
#[derive(Debug)]
pub struct RmError {
    pub path: PathBuf,
    pub err: io::Error,
}

impl std::fmt::Display for RmError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "rm {}: {}", self.path.display(), self.err)
    }
}

impl std::error::Error for RmError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.err)
    }
}

/// 删除文件或整棵目录树。
///
/// 引擎=std 硬化原语（`remove_dir_all` 的 CVE-2022-21658 相对父目录打开防
/// 护+Windows DELETE_PENDING 等待语义），本函数只加 Windows 只读属性装甲：
/// 首次 `PermissionDenied` 时清只读属性后重试恰一轮，仍败以原错误上浮。
/// Unix 只读目录 EACCES 属 OS 语义如实上浮（不自动 chmod，声明上限）。
///
/// # Examples
///
/// ```no_run
/// use std_ext::fsx::rm_all;
///
/// rm_all("dst/tree").unwrap();
/// ```
pub fn rm_all(path: impl AsRef<Path>) -> Result<(), RmError> {
    fn once(p: &Path) -> io::Result<()> {
        match fs::remove_file(p) {
            Ok(()) => return Ok(()),
            // 缺失=终态错误显式上浮；目录/其余失败分流到树删除
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Err(e),
            Err(_) => {}
        }
        fs::remove_dir_all(p)
    }
    let path = path.as_ref();
    let first = match once(path) {
        Ok(()) => return Ok(()),
        Err(err) => err,
    };
    #[cfg(windows)]
    {
        // 只读装甲：首次 PermissionDenied 清只读属性后重试恰一轮，仍败以原
        // 错误上浮（装甲本身失败同样回落原错误——best-effort，禁吞）
        if first.kind() == io::ErrorKind::PermissionDenied
            && clear_readonly_tree(path).is_ok()
            && once(path).is_ok()
        {
            return Ok(());
        }
    }
    Err(RmError {
        path: path.to_path_buf(),
        err: first,
    })
}

/// 清除 `path` 自身及树内后代的只读属性（`symlink_metadata` 判类型：不穿
/// symlink，递归环安全——symlink 目录不展开）。
#[cfg(windows)]
fn clear_readonly_tree(path: &Path) -> io::Result<()> {
    let md = fs::symlink_metadata(path)?;
    if md.file_type().is_dir() {
        for entry in fs::read_dir(path)? {
            clear_readonly_tree(&entry?.path())?;
        }
    }
    if md.permissions().readonly() {
        // 只清只读位、其余权限位保持（装甲本意；clippy 建议的整模式重写会
        // 连带改写其他属性位，非本函数语义）
        #[allow(clippy::permissions_set_readonly_false)]
        {
            let mut perm = md.permissions();
            perm.set_readonly(false);
            fs::set_permissions(path, perm)?;
        }
    }
    Ok(())
}

// ── temp_dir_in：工作区临时目录（RAII+并发唯一）──

/// 进程内临时目录命名序号（同 pid 并发竞态的根除面：进程内恒新）。
static TEMP_SEQ: AtomicU64 = AtomicU64::new(0);

/// 生成临时目录候选名：`{prefix}{pid}-{seq:012x}{nanos:x}`（纯函数，可测）。
/// pid 十进制段保进程隔离可观测；进程内原子序号根除同 pid 并发竞态；纳秒
/// 兜底 pid 回收复用。
fn unique_temp_name(prefix: &str) -> String {
    let seq = TEMP_SEQ.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    format!("{prefix}{}-{seq:012x}{nanos:x}", std::process::id())
}

/// RAII 临时目录：Drop 即删树（删除失败 fail-visible 记录到 stderr 而非
/// panic——与 memgov LeakGuard 风格对齐）；需要清理结果用 [`TempDir::close`]。
///
/// 落点由调用方给定（对齐产物红线：禁系统 TEMP，工作区 target/ 下自选根）。
#[derive(Debug)]
pub struct TempDir {
    path: PathBuf,
}

impl TempDir {
    /// 临时目录路径。
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// 显式清理（消费 self，删除结果可见）；Drop 为兜底（错误只记录）。
    pub fn close(self) -> io::Result<()> {
        let path = self.path.clone();
        std::mem::forget(self); // 清理结果由本函数申报，不再走 Drop 兜底
        rm_all(&path).map_err(|e| e.err)
    }
}

/// 在 `root` 下以 `prefix` 命名创建唯一临时目录（调研档 §4.6 拍板 API 形；
/// 命名/竞态语义见模块头注）。
///
/// # Examples
///
/// ```no_run
/// use std_ext::fsx::temp_dir_in;
///
/// let tmp = temp_dir_in("target/tmp", "myapp-").unwrap();
/// std::fs::write(tmp.path().join("scratch.txt"), b"x").unwrap();
/// drop(tmp); // RAII：Drop 即删树
/// ```
///
/// `prefix` 按 `Path::join` 语义拼接：含 `..` 的 prefix 可越出 `root`
/// （与 tempfile `TempDir::new_in` 同行为）——调用方对 prefix 内容自负其责，
/// 本函数不消毒。
pub fn temp_dir_in(root: impl AsRef<Path>, prefix: &str) -> io::Result<TempDir> {
    let path = root.as_ref().join(unique_temp_name(prefix));
    // create_dir 独占语义：存在即 AlreadyExists 显式上浮（同名撞车被进程内
    // 原子序号根除，重试环属死代码；跨进程由 pid 段隔离）
    fs::create_dir(&path)?;
    Ok(TempDir { path })
}

impl Drop for TempDir {
    fn drop(&mut self) {
        if let Err(err) = rm_all(&self.path) {
            // 埋点「清理失败」：RAII 收尾失败留痕（eprintln fail-visible 通道
            // 保持不变，事件是宿主可订阅的增量面）。
            embed_event!(warn, target: "skeleton::fsx::tempdir", path = %self.path.display(), error = %err);
            eprintln!(
                "fsx: TempDir cleanup failed at {}: {err}",
                self.path.display()
            );
        }
    }
}

// ── atomic_write：崩溃安全原子写（子模块自带 armor 头注）──

mod atomic;
pub use atomic::atomic_write;

#[cfg(test)]
mod tests;
