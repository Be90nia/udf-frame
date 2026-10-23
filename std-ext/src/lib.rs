//! # std-ext
//!
//! 常驻生态标准件（批 5-1/5-2 自 skeleton-infra 物理拆出，切面决策见
//! `udf-frame/docs/adr/0004-std-ext-split.md`）：十四域零 feature 门常驻可用
//! （默认树：十三域纯 std；`json` 域引 serde_json 普通依赖——P1-9 拍板，
//! 自主仓 xray-conf 上浮）。
//!
//! 分层方向：`skeleton`（最底层契约）← **`std-ext`（本 crate，散件）** ←
//! `skeleton-infra`（核+胶水，反向依赖本 crate）。本 crate 内部自闭合：
//! retry→`crate::backoff`+`crate::clock`；ratelimit→`crate::retry`+`crate::clock`；
//! scheduler/cache/id 消费 `skeleton::clock` 时间源契约。
//!
//! feature 指南（默认零 feature 全开；`--no-default-features` 同面——常驻件
//! 无门控）：
//!
//! | 域 | 面 |
//! |---|---|
//! | （常驻，零 feature） | `text`：UTF-8 解码与字符边界截断 + case 转换（ASCII 词界）+ 编辑距离（字节面） |
//! | （常驻，零 feature） | `backoff`：确定性退避计算 |
//! | （常驻，零 feature） | `fsx`：文件系统受控子集（walk 惰性遍历/copy_tree/rm_all/temp_dir_in + atomic_write 崩溃安全原子写） |
//! | （常驻，零 feature） | `encode`：hex/base64/BOM/UTF-16 编解码（显式策略，禁静默） |
//! | （常驻，零 feature） | `hash`：确定性哈希（`stable64`=FNV-1a 64（RFC 9923）+ `xxh64`=XXH64 spec v0.2.0；算法集版本化=稳定承诺） |
//! | （常驻，零 feature） | `iter`：迭代器扩展精选（zip_longest 显式三态/interleave/minmax 三态/dedup_by 连续去重/process_results 错误短路；itertools 语义子集，std 缺位面） |
//! | （常驻，零 feature） | `json`：JSON 双模解码（RFC 8259 严格 / 注释容忍对齐 Go json_reader；64MiB 输入上限 fail-visible + 行列位置错误；P1-9 自 xray-conf 上浮） |
//! | （常驻，零 feature） | `paths`：路径归一/相对化/containment（Windows verbatim 兼容）+ 家目录/XDG 用户基目录解析（env 面，零 IO 不创建） |
//! | （常驻，零 feature） | `clock`：时长人读格式化 + RFC 3339 双向（时间源契约 `Clock`/`SystemClock`/`MockClock` 住 `skeleton::clock`，批 1 契约真下沉） |
//! | （常驻，零 feature） | `retry`：同步重试编排（政策显式/确定性退避/预算闸/fail-visible 耗尽回灌；退避计算用 `crate::backoff`） |
//! | （常驻，零 feature） | `scheduler`：同步任务调度器（after/every；MockClock 确定性 tick 内核 + condvar park 驱动线程；锚定周期超载退化固定延迟，无补发） |
//! | （常驻，零 feature） | `ratelimit`：令牌桶限流（突发预算 + 恒速回填；满桶启动；MockClock 确定性令牌时间线 + Sleeper 锁外睡到再查，零自旋） |
//! | （常驻，零 feature） | `cache`：LRU+TTL 缓存（O(1) get/insert 索引 arena 双向链零 unsafe；get=touch/peek 不 touch；TtlCache 逐条 expire-after-write + 惰性过期 fail-visible 计数；单线程 &mut 面如实声明） |
//! | （常驻，零 feature） | `id`：UUIDv7 确定性变体时间有序 id（RFC 9562 §5.7 布局；指纹+挂钟毫秒→同指纹严格递增/跨毫秒时间序/回拨停走；同毫秒 4096 配额耗尽 fail-closed；指纹派生推荐 `crate::hash::stable64`） |
//! | `log-embed` | 行为域 tracing 埋点（七域 35 调用点：backoff 1/fsx 6/iter 2/scheduler 11/ratelimit 4/retry 6/cache 5；默认关=宏消除零依赖零开销；target 沿用 `skeleton::<domain>[::<phase>]` 稳定观测契约） |

// ── log-embed 埋点宏（feature 门控唯一通道；文本作用域，先于全部模块声明）──
//
// **同步责任**：本宏与 `skeleton-infra/src/lib.rs` 的 `embed_event!` 为文本
// 副本（逐字镜像，含两侧 cfg 臂）——修改任一副本必须同批镜像另一份，漏同步
// = 两 crate 埋点行为分叉（ADR 0005 §埋点宏：单源化评估可行但劣化面更大，
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

pub mod backoff;
pub mod cache;
pub mod clock;
pub mod encode;
pub mod fsx;
pub mod hash;
pub mod id;
pub mod iter;
pub mod json;
pub mod paths;
pub mod ratelimit;
pub mod retry;
pub mod scheduler;
pub mod text;
