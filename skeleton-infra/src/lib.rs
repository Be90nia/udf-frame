//! # skeleton-infra
//!
//! UDF 骨架基础设施层：契约实现（`skeleton::Store` 的参考实现）+ 生态标准件
//! 绑定 + 防坑门面。契约（trait 与数据词汇）在 [`skeleton`]，本 crate 兑现。
//!
//! 批 5-1/5-2 物理拆分（ADR 0004）：十四域中的
//! 十三域零门常驻（纯 std）已迁至子 crate `std-ext`（第十四条 `json` 域随迁、
//! 门控 `json` feature，见 ADR 0007）——批 5-1 六叶子域 `text`/`encode`/`hash`/
//! `iter`/`backoff`/`fsx` + 批 5-2 七域 `paths`/`clock`/`retry`/`scheduler`/
//! `ratelimit`/`cache`/`id`。分层 `skeleton` ← `std-ext` ← 本 crate（反向依赖）；
//! 本 crate 留守核（runtime/audit/pool）+ 胶水（shutdown/fx/journal/logx 等）。
//!
//! feature 指南（`default = ["runtime", "pool"]`；`--no-default-features` 得纯生态件面）：
//!
//! | feature | 域 |
//! |---|---|
//! | `runtime`（默认） | `runtime::SyncStore`：契约参考实现（std-only）+ `audit` 审计事件值面 |
//! | `pool`（默认） | `pool::PoolBase`：池化管理组合件（resize 校验/水位线/审计/溢出计数公共逻辑写一次）+ `objpool::ObjectPool`：有状态对象池（reset 钩子/有界降级/分片槽位） |
//! | （常驻，零 feature） | `shutdown`：跨组件优雅停机编排器（注册序串行；组件 panic/超时 fail-visible 上报不拖垮整体；超时=放弃等待非中断，watchdog 线程照跑如实声明） |
//! | （常驻，零 feature） | **std-ext 侧十四域**：十三域零门常驻（`text`/`encode`/`hash`/`iter`/`backoff`/`fsx`/`paths`/`clock`/`retry`/`scheduler`/`ratelimit`/`cache`/`id`）+ `json` 域门控（`json` feature）：随依赖树常驻可用（`json` 域需显式开门），逐域职责见 `std-ext` crate 文档 |
//! | `config-json` | `ConfigFile`：分层装配（default→file→层叠合并→load）+ `save` 原子落盘（serde_json::Value 统一值面；serde/serde_json 仅此 feature 可达；配置契约 `ConfigPort`/`EnvSource`/`ConfigError` 已下沉 `skeleton::config`） |
//! | `config-toml` | `load_toml`/`save_toml` 类型化直读直写 + `file_layer_toml`（TOML↔值面翻译；Datetime 层面显式拒；toml 仅此 feature 可达） |
//! | `tokio` | `fx`：异步并发托管 |
//! | `log-fs` | `logx` 文件落盘面（纯 std 零三方）：密钥掩码 mask_secrets + 路径解析 DefaultPathResolver（契约 `LogPathResolver` 住 `skeleton::logpath`）+ FileSink 按日滚动 |
//! | `log` | `logx` 门面绑定层（=`log-fs`+log+tracing+tracing-subscriber+tracing-log）：init 系（init_tracing_from_env/init_with_file 双 sink）+ FileSinkWriter；多通道扇出面（Channel/ChannelSink/ChannelRegistry/ChannelFileSink/PerChannelPathResolver，P1-10-a 前置件） |
//! | `journal` | `journal`：账本（serde/serde_json 仅此 feature 可达） |
//! | `view` | `view`：语言无关视图桥（ViewMap/IntentMap 机器 + 区域通道 + 读侧参考实现；serde_json/udf-std-ext json 仅此 feature 可达） |
//! | `testing` | `testing` + `parse_mock`：测试替身（TestStore/脚本化解释器 + 解析面 mock） |
//! | `memgov` | `memgov`：内存治理；`alloc_site`：分配站点目录（编译期静态表 + 每站点原子计数/开关 + 查表零分配） |
//! | `compose` | `compose`：父子分片组合器 |
//! | `tracing` | `tracingx`：骨架审计事件流 → tracing 桥 |
//! | `log-embed` | 行为+数据域 tracing 埋点（本 crate：shutdown 一域 6 + fx 一域 1 = 7 调用点；默认关=宏消除零依赖零开销，target `skeleton::<domain>[::<phase>]`。scheduler/ratelimit/retry/cache 四域 41 调用点随批 5-2、backoff/fsx/iter 三域 9 调用点随批 5-1 拆至 `std-ext`（合计 50；口径=`embed_event!` 宏调用点，排除宏定义/文档注释/`#[cfg(test)]` 之后内容），由其自有 `log-embed` feature 承载，宏同款副本） |

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
static EMBEDDED_EVENT_PANIC_COUNT: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Number of embedded tracing events whose host subscriber panicked.
///
/// This counter saturates at `u64::MAX`. It is available only when the
/// `log-embed` feature is enabled. The counter is diagnostic; it never changes
/// the result of the operation that emitted the event.
#[cfg(feature = "log-embed")]
pub fn embedded_event_panic_count() -> u64 {
    EMBEDDED_EVENT_PANIC_COUNT.load(std::sync::atomic::Ordering::Relaxed)
}

/// Run one embedded event behind a panic boundary. This is panic containment only:
/// tracing subscribers still run synchronously on the emitting thread and must be
/// fast/non-blocking. It does not make emission under a library lock safe, and
/// `panic=abort` cannot be contained.
#[cfg(feature = "log-embed")]
#[inline]
fn emit_embedded_event(emit: impl FnOnce()) {
    if std::panic::catch_unwind(std::panic::AssertUnwindSafe(emit)).is_err() {
        let _ = EMBEDDED_EVENT_PANIC_COUNT.fetch_update(
            std::sync::atomic::Ordering::Relaxed,
            std::sync::atomic::Ordering::Relaxed,
            |count| Some(count.saturating_add(1)),
        );
        #[cfg(all(test, feature = "log-embed"))]
        embedded_event_panic_window::note_counted_panic();
    }
}

/// 测试窗口（仅单元测试构建）：**本线程**自武装起被计数的 subscriber panic 次数。
/// 进程级计数 `embedded_event_panic_count` 会被同二进制并行邻居测试的 panic
/// 抬高，钉不出「恰好 +1」；开关与计数同住线程局部（const 初始化、无惰性分配），
/// 邻居线程的 panic 天然不进窗。进程级计数本身不因本窗口改变语义。
#[cfg(all(test, feature = "log-embed"))]
mod embedded_event_panic_window {
    use std::cell::Cell;

    thread_local! {
        static WINDOW: Cell<Option<u64>> = const { Cell::new(None) };
    }

    /// 在调用线程上武装窗口（窗内计数从 0 起）。
    pub(crate) fn arm() {
        WINDOW.with(|window| window.set(Some(0)));
    }

    /// 收窗并取走窗内计数；本线程未武装则为 `None`。
    pub(crate) fn take() -> Option<u64> {
        WINDOW.with(|window| window.replace(None))
    }

    /// 计数钩子：进程级计数 +1 之后调用；仅本线程窗口已武装时累加。
    pub(crate) fn note_counted_panic() {
        // 线程析构期 TLS 已销毁 ⇒ 放弃记账（窗口随之不可读），不得 panic。
        let _ = WINDOW.try_with(|window| {
            if let Some(count) = window.get() {
                window.set(Some(count.saturating_add(1)));
            }
        });
    }
}

#[cfg(feature = "log-embed")]
macro_rules! embed_event {
    (debug, target: $target:expr, $($rest:tt)*) => {
        $crate::emit_embedded_event(|| {
            ::tracing::event!(target: $target, ::tracing::Level::DEBUG, $($rest)*)
        })
    };
    (info, target: $target:expr, $($rest:tt)*) => {
        $crate::emit_embedded_event(|| {
            ::tracing::event!(target: $target, ::tracing::Level::INFO, $($rest)*)
        })
    };
    (warn, target: $target:expr, $($rest:tt)*) => {
        $crate::emit_embedded_event(|| {
            ::tracing::event!(target: $target, ::tracing::Level::WARN, $($rest)*)
        })
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

// 测试面共享件（有界护栏单源）：只在本 crate 的单元测试构建里存在，库内无
// watchdog 线程（issue#94/#102 明令）。集成测试用 `#[path]` 直接引入同名文件。
#[cfg(all(test, feature = "tokio"))]
mod test_support;

#[cfg(feature = "testing")]
pub mod testing;

#[cfg(feature = "testing")]
pub mod parse_mock;

#[cfg(feature = "journal")]
pub mod journal;

#[cfg(feature = "view")]
pub mod view;

#[cfg(feature = "log-fs")]
pub mod logx;

#[cfg(feature = "tracing")]
pub mod tracingx;

#[cfg(feature = "memgov")]
pub mod memgov;

#[cfg(feature = "memgov")]
pub mod alloc_site;

#[cfg(feature = "pool")]
pub mod pool;

#[cfg(feature = "pool")]
pub mod objpool;

#[cfg(feature = "pool")]
pub mod pooled_thread_pool;

#[cfg(feature = "pool")]
pub mod size_class_pool;

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
    AdmissionError, CancelSource, CancelToken, FxCounts, FxScope, ScopeClosed, Spawned, TaskError,
    cancel_pair,
};

#[cfg(feature = "testing")]
pub use testing::{ExpectedEffect, Script, ScriptedRuntime, TestStore};

#[cfg(feature = "testing")]
pub use parse_mock::{MockJson, MockMarkdown, MockParseError, MockSqlDialect};

#[cfg(feature = "journal")]
pub use journal::{Journal, JournalError, JsonlJournal, Replay, ReplayError};

#[cfg(feature = "log-fs")]
pub use logx::{
    DEFAULT_MAX_LOG_EVENT_BYTES, DefaultPathResolver, FileSink, HomeDirUnresolvable, mask_secrets,
};

#[cfg(feature = "log")]
pub use logx::{
    AlreadyInit, FileSinkWriter, LogInitError, init_tracing_default, init_tracing_from_env,
    init_with_file, init_with_file_with_secrets,
};

#[cfg(feature = "log")]
pub use logx::{
    Channel, ChannelError, ChannelFileSink, ChannelRegistry, ChannelSink, PerChannelPathResolver,
};

#[cfg(feature = "tracing")]
pub use tracingx::bridge_events_to_tracing;

#[cfg(feature = "memgov")]
pub use memgov::{
    BoundError, BoundedRegistry, CountingAllocator, Eviction, LeakGuard, LeakReport, expect_empty,
};

#[cfg(feature = "pool")]
pub use pool::{AUDIT_RING_DEFAULT_CAP, PoolBase};

#[cfg(feature = "pool")]
pub use objpool::{
    ByteBudget, ObjectPool, ObjectPoolMetrics, ObjectPoolTuning, Owned, Pooled, ResetPhase,
};

#[cfg(feature = "pool")]
pub use pooled_thread_pool::PooledThreadPool;

#[cfg(feature = "pool")]
pub use size_class_pool::{SizeClassHandle, SizeClassMetrics, SizeClassPool, SizeClassStat};

#[cfg(feature = "compose")]
pub use compose::{Combine, combine};

#[cfg(all(test, feature = "log-embed"))]
mod embedded_event_panic_tests {
    use super::*;
    use tracing::span::{Attributes, Id, Record};
    use tracing::{Event, Metadata};

    struct PanicSubscriber;

    impl tracing::Subscriber for PanicSubscriber {
        fn enabled(&self, _: &Metadata<'_>) -> bool {
            true
        }

        fn new_span(&self, _: &Attributes<'_>) -> Id {
            Id::from_u64(1)
        }

        fn record(&self, _: &Id, _: &Record<'_>) {}

        fn record_follows_from(&self, _: &Id, _: &Id) {}

        fn event(&self, _: &Event<'_>) {
            panic!("injected host tracing subscriber panic");
        }

        fn enter(&self, _: &Id) {}

        fn exit(&self, _: &Id) {}
    }

    #[test]
    fn embedded_event_contains_subscriber_panic_and_counts_it() {
        // 精确「恰好 +1」只对线程局部窗口成立：进程级计数是 crate 级全局，同二进制
        // 并行邻居测试的 panic 也会抬高它，故对进程级只钉「严格增加」这一可判定方向。
        embedded_event_panic_window::arm();
        let before = embedded_event_panic_count();
        tracing::subscriber::with_default(PanicSubscriber, || {
            embed_event!(debug, target: "skeleton::telemetry_test", marker = 1);
        });
        assert_eq!(
            embedded_event_panic_window::take(),
            Some(1),
            "本线程窗口内 subscriber panic 恰计一次"
        );
        assert!(
            embedded_event_panic_count() > before,
            "进程级计数同样记到这次 panic，且 panic 不穿出发射调用"
        );
    }

    /// 邻居隔离（确定性握手，禁 sleep）：窗口武装后，N 个邻居线程各触发一次会抬高
    /// 进程级计数的 subscriber panic，全部 join 落定后本线程再触发恰一次——窗内
    /// 计数必须恰为 1（邻居的 panic 不进本线程窗口），进程级计数至少增加 N。
    #[test]
    fn panics_on_neighbor_threads_stay_out_of_the_armed_window() {
        const NEIGHBORS: u64 = 4;

        embedded_event_panic_window::arm();
        let global_before = embedded_event_panic_count();
        std::thread::scope(|scope| {
            for _ in 0..NEIGHBORS {
                scope.spawn(|| {
                    tracing::subscriber::with_default(PanicSubscriber, || {
                        embed_event!(debug, target: "skeleton::telemetry_test", marker = 1);
                    });
                });
            }
        });
        let after_neighbors = embedded_event_panic_count();

        tracing::subscriber::with_default(PanicSubscriber, || {
            embed_event!(debug, target: "skeleton::telemetry_test", marker = 1);
        });

        assert!(
            after_neighbors - global_before >= NEIGHBORS,
            "邻居的 panic 必须真被引发并抬高进程级计数"
        );
        assert_eq!(
            embedded_event_panic_window::take(),
            Some(1),
            "窗内只计本线程那一次：邻居线程的 panic 不得进窗"
        );
    }
}
