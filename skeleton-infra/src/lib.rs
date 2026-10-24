//! # skeleton-infra
//!
//! UDF 骨架基础设施层：契约实现（`skeleton::Store` 的参考实现）+ 生态标准件
//! 绑定 + 防坑门面。契约（trait 与数据词汇）在 [`skeleton`]，本 crate 兑现。
//!
//! 批 5-1/5-2 物理拆分（ADR `docs/adr/0004-std-ext-split.md`）：十三个纯 std
//! 常驻域已迁至子 crate `std-ext`——批 5-1 六叶子域 `text`/`encode`/`hash`/
//! `iter`/`backoff`/`fsx` + 批 5-2 七域 `paths`/`clock`/`retry`/`scheduler`/
//! `ratelimit`/`cache`/`id`。分层 `skeleton` ← `std-ext` ← 本 crate（反向依赖）；
//! 本 crate 留守核（runtime/audit/pool）+ 胶水（shutdown/fx/journal/logx 等）。
//!
//! feature 指南（`default = ["runtime", "pool"]`；`--no-default-features` 得纯生态件面）：
//!
//! | feature | 域 |
//! |---|---|
//! | `runtime`（默认） | `runtime::SyncStore`：契约参考实现（std-only）+ `audit` 审计事件值面 |
//! | `pool`（默认） | `pool::PoolBase`：池化管理组合件（resize 校验/水位线/审计/溢出计数公共逻辑写一次） |
//! | （常驻，零 feature） | `shutdown`：跨组件优雅停机编排器（注册序串行；组件 panic/超时 fail-visible 上报不拖垮整体；超时=放弃等待非中断，watchdog 线程照跑如实声明） |
//! | （常驻，零 feature） | **std-ext 侧十三域**（`text`/`encode`/`hash`/`iter`/`backoff`/`fsx`/`paths`/`clock`/`retry`/`scheduler`/`ratelimit`/`cache`/`id`）：随依赖树常驻可用，逐域职责见 `std-ext` crate 文档 |
//! | `config-json` | `ConfigFile`：分层装配（default→file→层叠合并→load）+ `save` 原子落盘（serde_json::Value 统一值面；serde/serde_json 仅此 feature 可达；配置契约 `ConfigPort`/`EnvSource`/`ConfigError` 已下沉 `skeleton::config`） |
//! | `config-toml` | `load_toml`/`save_toml` 类型化直读直写 + `file_layer_toml`（TOML↔值面翻译；Datetime 层面显式拒；toml 仅此 feature 可达） |
//! | `tokio` | `fx`：异步并发托管 |
//! | `log-fs` | `logx` 文件落盘面（纯 std 零三方）：密钥掩码 mask_secrets + 路径解析 DefaultPathResolver（契约 `LogPathResolver` 住 `skeleton::logpath`）+ FileSink 按日滚动 |
//! | `log` | `logx` 门面绑定层（=`log-fs`+log+tracing+tracing-subscriber+tracing-log）：init 系（init_tracing_from_env/init_with_file 双 sink）+ FileSinkWriter；多通道扇出面（Channel/ChannelSink/ChannelRegistry/ChannelFileSink/PerChannelPathResolver，P1-10-a 前置件） |
//! | `journal` | `journal`：账本（serde/serde_json 仅此 feature 可达） |
//! | `testing` | `testing` + `parse_mock`：测试替身（TestStore/脚本化解释器 + 解析面 mock） |
//! | `memgov` | `memgov`：内存治理 |
//! | `compose` | `compose`：父子分片组合器 |
//! | `tracing` | `tracingx`：骨架审计事件流 → tracing 桥 |
//! | `log-embed` | 行为+数据域 tracing 埋点（本 crate：shutdown 一域 6 调用点 + fx 一域 1 调用点；默认关=宏消除零依赖零开销，target `skeleton::<domain>[::<phase>]`。scheduler/ratelimit/retry/cache 四域 26 调用点随批 5-2、backoff/fsx/iter 三域 9 调用点随批 5-1 拆至 `std-ext`（合计 35），由其自有 `log-embed` feature 承载，宏同款副本） |

// ── log-embed 埋点宏（feature 门控唯一通道；文本作用域，先于全部模块声明）──
//
// **同步责任**：本宏与 `std-ext/src/lib.rs` 的 `embed_event!` 为文本副本
//（逐字镜像，含两侧 cfg 臂）——修改任一副本必须同批镜像另一份，漏同步 =
// 两 crate 埋点行为分叉（ADR 0005 §埋点宏：单源化评估可行但劣化面更大，
// 维持双副本）。
// 开（`log-embed`）：转发 `tracing::event!`；关（默认）：展开为空——字段
// 表达式（含为事件新算的量）随之消失，tracing 依赖不存在（cargo tree 零
// tracing 证明）。埋点纪律：
//
// - **只埋行为决策点**（register/cancel/fire/裁决/拒绝/等待分段），禁埋纯
//   计算路径（backoff_delay 的 debug 入参事件是派单契约显式点）；
// - **字段化禁格式化**：字段只收数值/布尔/静态串，禁 format!/Display（昂贵
//   格式化在订阅方兴趣确认后才求值是 tracing 语义，但调用点不得引入分配面）；
// - **关联序号进字段**：seq（scheduler 堆条目）/attempt（retry 轮次）/
//   round（ratelimit 阻塞重查轮）承载跨事件关联，不起 span；
// - **target 规范**：域=`skeleton::<domain>`，子流程=`skeleton::<domain>::
//   <phase>`——EnvFilter `skeleton::retry=debug` 按流程开，`skeleton=info`
//   按域开；级别规范（正常流转=debug / 异常路径=warn / 终态=info）文档化在
//   各模块头注。
#[cfg(feature = "log-embed")]
macro_rules! embed_event {
    (debug, target: $target:expr, $($rest:tt)*) => {
        ::tracing::event!(target: $target, ::tracing::Level::DEBUG, $($rest)*)
    };
    (info, target: $target:expr, $($rest:tt)*) => {
        ::tracing::event!(target: $target, ::tracing::Level::INFO, $($rest)*)
    };
    (warn, target: $target:expr, $($rest:tt)*) => {
        ::tracing::event!(target: $target, ::tracing::Level::WARN, $($rest)*)
    };
}
#[cfg(not(feature = "log-embed"))]
macro_rules! embed_event {
    ($($tt:tt)*) => {};
}

// 契约真下沉后本 crate 的 config 模块只余行为件（ConfigFile/TOML 面）；
// 契约本体在 skeleton::clock / skeleton::config。clock 行为件（format_duration/
// rfc3339）随批 5-2 迁 std-ext。
pub mod config;
pub mod shutdown;

#[cfg(feature = "runtime")]
pub mod audit;

#[cfg(feature = "runtime")]
pub mod runtime;

#[cfg(feature = "tokio")]
pub mod fx;

#[cfg(feature = "testing")]
pub mod testing;

#[cfg(feature = "testing")]
pub mod parse_mock;

#[cfg(feature = "journal")]
pub mod journal;

#[cfg(feature = "log-fs")]
pub mod logx;

#[cfg(feature = "tracing")]
pub mod tracingx;

#[cfg(feature = "memgov")]
pub mod memgov;

#[cfg(feature = "pool")]
pub mod pool;

#[cfg(feature = "pool")]
pub mod pooled_thread_pool;

#[cfg(feature = "compose")]
pub mod compose;

pub use shutdown::{
    ComponentOutcome, OutcomeKind, RegisterError, ShutdownCoordinator, ShutdownReport,
};

#[cfg(feature = "config-json")]
pub use config::ConfigFile;

#[cfg(feature = "config-toml")]
pub use config::{load_toml, save_toml};

#[cfg(feature = "runtime")]
pub use audit::{AuditEvent, AuditEventKind, AuditTicket};

#[cfg(feature = "runtime")]
pub use runtime::{LocalSubscription, SyncStore};

#[cfg(feature = "tokio")]
pub use fx::{
    cancel_pair, expect_idle, CancelSource, CancelToken, FxScope, ScopeClosed, Spawned, TaskError,
};

#[cfg(feature = "testing")]
pub use testing::{ExpectedEffect, Script, ScriptedRuntime, TestStore};

#[cfg(feature = "testing")]
pub use parse_mock::{MockJson, MockMarkdown, MockParseError, MockSqlDialect};

#[cfg(feature = "journal")]
pub use journal::{Journal, JournalError, JsonlJournal, Replay, ReplayError};

#[cfg(feature = "log-fs")]
pub use logx::{mask_secrets, DefaultPathResolver, FileSink, HomeDirUnresolvable};

#[cfg(feature = "log")]
pub use logx::{
    init_tracing_default, init_tracing_from_env, init_with_file, AlreadyInit, FileSinkWriter,
    LogInitError,
};

#[cfg(feature = "log")]
pub use logx::{
    Channel, ChannelError, ChannelFileSink, ChannelRegistry, ChannelSink, PerChannelPathResolver,
};

#[cfg(feature = "tracing")]
pub use tracingx::bridge_events_to_tracing;

#[cfg(feature = "memgov")]
pub use memgov::{
    expect_empty, BoundError, BoundedRegistry, CountingAllocator, Eviction, LeakGuard, LeakReport,
};

#[cfg(feature = "pool")]
pub use pool::{PoolBase, AUDIT_RING_DEFAULT_CAP};

#[cfg(feature = "pool")]
pub use pooled_thread_pool::PooledThreadPool;

#[cfg(feature = "compose")]
pub use compose::{combine, Combine};
