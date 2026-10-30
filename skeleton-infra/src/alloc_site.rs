//! 分配站点目录：内存分配/资源点按站点归集计数的**机制层**——编译期静态
//! 表注册 + 每站点原子计数/开关 + 查表零分配。
//!
//! feature `memgov`（零外部依赖纯 std）；由 lib.rs 以
//! `#[cfg(feature = "memgov")] pub mod alloc_site;` 接线。
//!
//! # 心智模型
//!
//! - **站点**：具名计数点（`JOURNAL_APPEND`、`FX_SPAWN`……），常量初始化随
//!   镜像常驻（无 ctor、无惰性初始化）；
//! - **目录表**：[`alloc_sites!`] 一次调用生成的 `&'static [&'static
//!   AllocSite]`，声明序 = 枚举序；每次宏调用生成**独立**静态表——与
//!   `skeleton::lint` 的进程级全局注册表性质相反，表间互不干扰，测试隔离
//!   天然成立；
//! - **两级开关**：全局闸（[`set_global_enabled`]/[`global_enabled`]）一把
//!   切 + 每站点 [`AllocSite::set_enabled`]；任一关 ⇒ [`AllocSite::bump`]
//!   零写入旁路。
//!
//! # 坑面 armor（逐条对表；tests/alloc_site.rs 与 examples/alloc_site_multidim.rs 钉死）
//!
//! 1. **原子序 = Relaxed**：`count` 是单调计数器，无跨变量因果需求——没人
//!    读 count 去同步别的内存，Acquire/Release 的发布语义（写方之前的写入
//!    对读方可见）在此无对象，Relaxed 最廉且足够；
//! 2. **开关检查序 = 先全局后站点**：全局闸关 ⇒ 一刀切零写入，站点开关读
//!    都省一次；反序会把最常见的「全局关」路径逼成两次读；
//! 3. **禁用路径零写入**：关闸（全局或站点）时 `bump` 不碰计数行——不脏
//!    缓存行。语义是「开关」而非「计数为 0」：关闸期间的历史计数保留，重
//!    开继续累加（清零走 [`AllocSite::reset`]）；
//! 4. **枚举序 = 宏声明序**：目录表是 `const` 数组字面量，编译期定死，跨
//!    平台稳定可断言（tests 对表）；
//! 5. **重名不做编译期校验**：同表重名是调用方错误（声明宏无去重机制），
//!    [`find`] 返回第一个命中——表内名字唯一性由采用方保证；
//! 6. **伪共享不预付**：各站点是独立 static，链接器布局不保证同缓存行隔
//!    离；高热站点若出现伪共享证据（性能矩阵④争用读数异常），升级路径 =
//!    `#[repr(align(64))]`（登记不预付，不为未来形态付今天的钱）；
//! 7. **零分配纪律**：`bump`/`count`/`reset`/`set_enabled`/`find`/表枚举
//!    全路径零堆分配（事件计数锚钉死，见 tests/alloc_site.rs）；
//! 8. **与既有面分工**：[`crate::memgov::CountingAllocator`] 是进程级分配
//!    计数（钩全局分配器，非按站点）；`log-embed` 是 tracing 埋点（默认
//!    关、不计数）；本件是第三种口径——按站点的应用层资源计数，不做日志、
//!    不出指标格式。
//!
//! # 收窄声明（明确不做，均属采用方策略层）
//!
//! 阈值判定 / 上报 / 告警 / 序列化 / 站点归属映射 / 自动发现——站点必须经
//! [`alloc_sites!`]（或 [`AllocSite::new`] 手工组表）显式登记。
//!
//! # Examples
//!
//! ```
//! use skeleton_infra::alloc_site::{find, global_enabled};
//! use skeleton_infra::alloc_sites;
//!
//! alloc_sites! {
//!     pub static DEMO_SITES = {
//!         JOURNAL_APPEND,
//!         FX_SPAWN,
//!     };
//! }
//!
//! assert!(global_enabled(), "两级开关默认全开");
//! JOURNAL_APPEND.bump();
//! JOURNAL_APPEND.bump();
//! assert_eq!(JOURNAL_APPEND.count(), 2);
//!
//! assert!(std::ptr::eq(find(DEMO_SITES, "FX_SPAWN").unwrap(), &FX_SPAWN));
//! let names: Vec<_> = DEMO_SITES.iter().map(|s| s.name).collect();
//! assert_eq!(names, ["JOURNAL_APPEND", "FX_SPAWN"]);
//! ```

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// 单个分配站点：静态名 + 原子计数 + 独立开关。
///
/// 通常由 [`alloc_sites!`] 生成；不走宏的手工登记口 = [`AllocSite::new`]
/// 常量构造后自组 `&[&AllocSite]` 表（`name` 自定义）。
///
/// 计数与开关走原子内部可变性——站点以 `static` 共享，`&self` 即可写；
/// 字段 `name` 公开只读（目录打印、[`find`] 比对直接用）。
///
/// # Examples
///
/// ```
/// use skeleton_infra::alloc_site::AllocSite;
///
/// static POOL_GROW: AllocSite = AllocSite::new("pool_grow");
///
/// POOL_GROW.bump();
/// assert_eq!(POOL_GROW.count(), 1);
///
/// POOL_GROW.set_enabled(false);
/// POOL_GROW.bump();
/// assert_eq!(POOL_GROW.count(), 1, "关 ⇒ 旁路，历史计数保留");
/// ```
#[derive(Debug)]
pub struct AllocSite {
    /// 站点名（宏生成 = `stringify!` 标识符；手工构造时自定义）。
    pub name: &'static str,
    count: AtomicU64,
    enabled: AtomicBool,
}

impl AllocSite {
    /// 常量构造：静态表零惰性初始化（无 ctor、无 LazyLock），站点默认开。
    pub const fn new(name: &'static str) -> Self {
        Self {
            name,
            count: AtomicU64::new(0),
            enabled: AtomicBool::new(true),
        }
    }

    /// 计一次。全局闸开 && 站点开 ⇒ `fetch_add(1)`（Relaxed，armor 1）；
    /// 任一关 ⇒ 零写入（armor 2 检查序先全局后站点，armor 3 旁路语义）。
    pub fn bump(&self) {
        if global_enabled() && self.enabled.load(Ordering::Relaxed) {
            self.count.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// 当前计数（Relaxed 读；关闸不清零，历史保留）。
    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    /// 站点开关（armor 3：关 ⇒ `bump` 零写入旁路，历史计数保留）。
    pub fn set_enabled(&self, on: bool) {
        self.enabled.store(on, Ordering::Relaxed);
    }

    /// 站点开关状态。
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    /// 清零计数——测试/基准复用口（不动站点开关与全局闸）。
    pub fn reset(&self) {
        self.count.store(0, Ordering::Relaxed);
    }
}

/// 全局闸（进程级一把切；`static` 原子量常驻，无惰性初始化）。
static GLOBAL_ENABLED: AtomicBool = AtomicBool::new(true);

/// 设全局闸：关 ⇒ 所有站点 [`AllocSite::bump`] 一刀切零写入（armor 2/3）。
/// 默认开。
pub fn set_global_enabled(on: bool) {
    GLOBAL_ENABLED.store(on, Ordering::Relaxed);
}

/// 全局闸当前状态。
pub fn global_enabled() -> bool {
    GLOBAL_ENABLED.load(Ordering::Relaxed)
}

/// 线性扫描站点目录表，返回名字命中的**第一个**站点（armor 5：重名不校
/// 验，先声明者胜）。零分配（armor 7）；未命中 ⇒ `None`。
///
/// 枚举不设包装口：直接遍历目录表静态即可（`table.iter()`，声明序 = 枚举
/// 序，见模块文档「目录表」）。
///
/// # Examples
///
/// ```
/// use skeleton_infra::alloc_site::{find, AllocSite};
/// use skeleton_infra::alloc_sites;
///
/// alloc_sites! {
///     static SITES = {
///         ALPHA,
///     };
/// }
///
/// assert!(find(SITES, "ALPHA").is_some());
/// assert!(find(SITES, "OMEGA").is_none());
/// assert_eq!(find(SITES, "ALPHA").map(AllocSite::count), Some(0));
/// ```
pub fn find(table: &'static [&'static AllocSite], name: &str) -> Option<&'static AllocSite> {
    table.iter().copied().find(|site| site.name == name)
}

/// 编译期静态站点目录注册——站点登记的唯一宏口。
///
/// 生成物（每次调用**独立**成套，表间互不干扰——与 `skeleton::lint` 的进程
/// 级全局注册表性质相反，测试隔离天然成立）：
///
/// - 每站点一个 `static <IDENT>: AllocSite = AllocSite::new(stringify!(IDENT))`
///   （常量初始化：无 ctor、无惰性）；
/// - 一个目录表 `static <TABLE>: &[&AllocSite]`，数组按声明序排列（armor 4：
///   枚举序 = 声明序，编译期定死，跨平台稳定可断言）。
///
/// 标识符原样透传（static 命名规范推荐大写蛇形；小写标识符触发标准
/// `non_upper_case_globals` 警告，宏不代为改名）。表内重名不做编译期校验
/// （armor 5，[`find`](crate::alloc_site::find) 返回第一个）。
///
/// # Examples
///
/// ```
/// use skeleton_infra::alloc_site::{find, AllocSite};
/// use skeleton_infra::alloc_sites;
///
/// alloc_sites! {
///     pub static PIPELINE_SITES = {
///         STAGE_RUN,
///         STAGE_FLUSH,
///     };
/// }
///
/// STAGE_RUN.bump();
/// assert_eq!(PIPELINE_SITES.len(), 2);
/// assert_eq!(find(PIPELINE_SITES, "STAGE_RUN").map(AllocSite::count), Some(1));
/// ```
#[macro_export]
macro_rules! alloc_sites {
    (
        $(#[$table_meta:meta])*
        $vis:vis static $table:ident = {
            $($site:ident),+ $(,)?
        };
    ) => {
        $(
            #[doc = concat!("分配站点 `", stringify!($site), "`（alloc_sites! 宏生成）。")]
            $vis static $site: $crate::alloc_site::AllocSite =
                $crate::alloc_site::AllocSite::new(stringify!($site));
        )+
        $(#[$table_meta])*
        #[doc = concat!("站点目录表（枚举序 = 声明序）：", stringify!($table))]
        $vis static $table: &[&$crate::alloc_site::AllocSite] = &[$(&$site),+];
    };
}
