//! 池化管理基座（契约层）：美团池管理理念契约化——「约定不是锁定」。
//!
//! # 理念→契约映射（调研档 §4.4.2/§4.4.3：docs/research/2026-10-15-dynamic-pool-survey.md）
//!
//! | 美团理念 | 契约面 |
//! |---|---|
//! | 动态化（运行时调参不重启） | [`PoolManaged::resize`]（plain `&self`，事前校验） |
//! | 可观测 | [`PoolMetrics`] 共享核心形（max/idle/borrowed/水位线/溢出丢弃计数） |
//! | 三参封顶（Less is More） | [`PoolManaged::Tuning`] 关联类型——参数形状各池特化 |
//! | 事前校验 | 非法参数入口拒绝：[`PoolResizeError`] fail-visible 显式 Err |
//! | 事中告警 | 水位线读数（借出率 `watermark`）——告警语义归宿主自判 |
//! | 事后审计 | [`PoolManaged::subscribe_audit`] 池域自有事件流（seq 单调+容量环） |
//!
//! # 约定不是锁定
//!
//! 本 trait 只约定**管理面**：调参、读数、审计三个挂点。池的实现细节——槽位
//! 策略、调度循环、对象生命周期——完全归实现方（内存池=槽位借还，线程池=
//! worker 调度，互不牵制）。参考组合件 `skeleton_infra::pool::PoolBase`
//! （`pool` feature）：公共理念逻辑（校验/水位线/审计/计数）写一次，双池只写
//! 特化面。
//!
//! # 性能纪律（契约面，2026-10-16 拍板）
//!
//! 管理面读侧零不必要分配：`resize` 校验路径与 [`PoolMetrics`] 读数均为
//! `Copy` 值语义（错误与指标都不构造 `String`/`Vec`）；[`PoolManaged::metrics`]
//! 预期 O(1)（实现方文档给出实测锚）；审计发射走「gate 快路径 + 容量环 +
//! seq」，热路径禁格式化。校验逻辑与状态变更分离（`validate` 形），为无锁
//! 演进预留结构位。

use std::borrow::Borrow;

/// 池指标共享核心形（可观测契约的统一读数面）：任何 [`PoolManaged`] 实现的
/// 指标类型都经 [`Borrow`] 可达本形——宿主对任何池取同一组核心读数，池特有
/// 读数在实现自己的 Metrics 类型上自由组合（无特有读数的池直接用本形，
/// `impl Borrow<T> for T` 天然满足，零包装）。
///
/// 值语义 `Copy`：读侧零分配（性能纪律，见模块文档）。水位线 `watermark` =
/// `borrowed / max`（借出率）：>1.0 = 缩容后借出数尚高于新池深（合法中间态，
/// 借出句柄不受 resize 影响——归还时裁决），宿主据此自判告警阈值（框架不设
/// 告警基建）。
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PoolMetrics {
    /// 池深：idle + borrowed 总量上限。
    pub max: usize,
    /// 空闲件数。
    pub idle: usize,
    /// 借出件数。
    pub borrowed: usize,
    /// 归还时槽满被丢弃的对象累计（缩容后超限——RAII 归还语义下唯一自洽的
    /// 处置：丢弃即计数，禁静默禁阻塞归还）。
    pub dropped_overflow: u64,
    /// 借出率水位线（`borrowed / max`）。
    pub watermark: f64,
}

/// [`PoolManaged::resize`] 入口拒绝（事前校验，fail-visible）：非法参数在改任何
/// 状态前显式报错。`Copy` 值语义（热路径零分配——性能纪律）；`Display` 自带
/// 现场（拒绝原因+两参原值，上层无需回看调用参数）。
///
/// 构造期等价校验由实现方的 `new` 以 panic 兜底（恒不合法的配置必是 bug，同
/// `BoundedRegistry::new` fail-fast 纪律）；运行时调参走本错误显式 Err，禁静默
/// 钳位。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolResizeError {
    /// `max == 0`：恒不可借出 = 池语义自毁。
    ZeroMax,
    /// 空闲上限为 0：永不留存 = 池语义自毁（归还即丢，池退化成直通管道）。
    IdleCapZero,
    /// 空闲上限超过池深（min>max 形非法参数）。
    IdleCapExceedsMax {
        /// 拒绝的空闲上限。
        idle_cap: usize,
        /// 当时的池深。
        max: usize,
    },
}

impl std::fmt::Display for PoolResizeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroMax => write!(
                f,
                "pool resize rejected: max == 0 (a pool that cannot lend is a bug)"
            ),
            Self::IdleCapZero => write!(
                f,
                "pool resize rejected: idle cap == 0 (a pool that never retains is a bug)"
            ),
            Self::IdleCapExceedsMax { idle_cap, max } => {
                write!(
                    f,
                    "pool resize rejected: idle cap {idle_cap} exceeds max {max}"
                )
            }
        }
    }
}

impl std::error::Error for PoolResizeError {}

/// 池审计事件种类（轻量无载荷枚举——管理面时刻；现场经读数面交叉查证，
/// 「seq + 枚举」即对账键，形态同骨架 audit 纪律）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolAuditKind {
    /// 调参生效（参数已过校验，核心读数已更新）。
    Resized,
    /// 调参被入口拒绝（非法参数，状态未动——事后审计的关键留痕）。
    ResizeRejected,
    /// 一件对象借出。
    Borrowed,
    /// 一件对象归还并留存入闲槽。
    Returned,
    /// 归还时槽满（缩容后超限），对象直接丢弃且 `dropped_overflow` 已 +1。
    OverflowDropped,
}

/// 池审计事件：`seq` 进程内单调（池实例级计数器，跨订阅窗不重复）；订阅窗内
/// 连续，空洞 = 容量环溢出丢最旧的显式证据（禁静默重写历史的对账键）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolAuditEvent {
    /// 单调序号。
    pub seq: u64,
    /// 事件种类。
    pub kind: PoolAuditKind,
}

/// 池审计订阅票据：Drop = 关闭事件流（落闸清槽，零开销恢复），`#[must_use]`
/// 拦截裸调用——与骨架审计票据同一纪律。票据对采用方不透明，经实现方
/// [`PoolManaged::subscribe_audit`] 取得；再次订阅 = 替换现役流，旧票据凭
/// 实现方代次对账 inert。
#[must_use = "审计票据被丢弃即立即关闭事件流——`pool.subscribe_audit(..);` 裸调用 = 静默关闭；绑定持有才持续记录"]
pub struct PoolAuditTicket {
    close: Option<Box<dyn FnOnce() + Send>>,
}

impl PoolAuditTicket {
    /// 实现方构造挂点（契约即可实现）：`close` 在票据 Drop 时恰执行一次。
    pub fn new(close: impl FnOnce() + Send + 'static) -> Self {
        Self {
            close: Some(Box::new(close)),
        }
    }
}

impl Drop for PoolAuditTicket {
    fn drop(&mut self) {
        if let Some(close) = self.close.take() {
            close();
        }
    }
}

/// 池化管理基座 trait：动态内存池/线程池/任何池的**管理面**契约——「约束后面
/// 代码长法」的池化版：后面任何人写的池都不会乱（美团理念全套自动到手）。
///
/// 三方法即全契约（`subscribe_audit` 含 `impl FnMut` 泛型参数 = 非 dyn-safe，
/// 同 [`crate::Store`] 先例：出现采用方前不预付装箱面）。trait 不设构造：装配
/// 归实现方的 `new`（三参封顶的参数面在 `Tuning`，基座不统一参数类型）。
///
/// # 语义契约（实现必须兑现）
///
/// 1. **事前校验**：[`Self::resize`] 非法参数入口拒绝（[`PoolResizeError`]），
///    拒绝不改任何状态；合法调参即刻生效（plain `&self`——配置通道归宿主，
///    文件/信号/Effect 皆可，池不管配置从哪来）。
/// 2. **缩容不夺借出**：resize 只调边界，已借出句柄不受影响（RAII 归还时由
///    实现方裁决：槽满 = 丢弃 + `dropped_overflow` 计数，禁阻塞归还禁静默）。
/// 3. **可观测**：[`Self::metrics`] 返回值语义快照，O(1) 读侧（性能预期，实现
///    方文档给实测锚）；共享核心形经 [`Borrow`] 可达，池特有读数自由扩展。
/// 4. **审计**：[`Self::subscribe_audit`] 池域自有事件流（seq 单调 + 容量环，
///    形态同骨架 audit 纪律；不硬接 Store 审计环）；票据 Drop 即关流。
///
/// # Metrics 形状为何是关联类型 + Borrow 核心形
///
/// 指标形状统一 ≠ 指标类型统一：内存池有闲槽上限读数、线程池有队列深/完成
/// 任务数——特有读数没有公共形状；统一的是**核心形**。任何池的 Metrics 经
/// `Borrow<PoolMetrics>` 必可达同一组读数，宿主代码对池类零特化；无特有读数
/// 的池直接用 `PoolMetrics` 本形（`impl Borrow<T> for T` 天然满足），零包装
/// 零分配。
///
/// # Tuning 为何是关联类型
///
/// 三参封顶的「三参」各池不同：内存池 = max/闲槽上限/获取超时，线程池 =
/// core/max/queue cap。参数类型统一是假统一（两池参数互不可换）；关联类型把
/// 参数形状留给特化，基座只锁「resize 存在且事前校验」这一条约定。
///
/// # Examples
///
/// 契约即可实现——无机器玩具池（真实公共逻辑见 `skeleton_infra::pool::PoolBase`）：
///
/// ```
/// use skeleton::pool::{
///     PoolAuditEvent, PoolAuditTicket, PoolManaged, PoolMetrics, PoolResizeError,
/// };
/// use std::borrow::Borrow;
/// use std::sync::Mutex;
///
/// struct ToyPool(Mutex<usize>); // 锁保护的就是池深
///
/// impl PoolManaged for ToyPool {
///     type Tuning = usize;        // 玩具单参：池深
///     type Metrics = PoolMetrics; // 无特有读数：核心形直接用
///
///     fn resize(&self, max: usize) -> Result<(), PoolResizeError> {
///         if max == 0 {
///             return Err(PoolResizeError::ZeroMax); // 事前校验：入口拒绝
///         }
///         *self.0.lock().unwrap() = max;
///         Ok(())
///     }
///
///     fn metrics(&self) -> PoolMetrics {
///         PoolMetrics {
///             max: *self.0.lock().unwrap(),
///             idle: 0,
///             borrowed: 0,
///             dropped_overflow: 0,
///             watermark: 0.0,
///         }
///     }
///
///     fn subscribe_audit(
///         &self,
///         _f: impl FnMut(&PoolAuditEvent) + Send + 'static,
///     ) -> PoolAuditTicket {
///         PoolAuditTicket::new(|| {})
///     }
/// }
///
/// let toy = ToyPool(Mutex::new(1));
/// assert_eq!(toy.resize(0), Err(PoolResizeError::ZeroMax));
/// toy.resize(8).unwrap();
/// assert_eq!(
///     toy.metrics().borrow(),
///     &PoolMetrics { max: 8, idle: 0, borrowed: 0, dropped_overflow: 0, watermark: 0.0 }
/// );
/// ```
pub trait PoolManaged {
    /// 运行时调参形状（三参封顶，各池特化——见类型文档「Tuning 为何是关联类型」）。
    type Tuning;
    /// 指标读数形状：共享核心形 [`PoolMetrics`] 经 [`Borrow`] 必可达
    /// （见类型文档「Metrics 形状」）。
    type Metrics: Borrow<PoolMetrics>;

    /// 运行时调参（美团「动态化」）：事前校验非法参数入口拒绝（fail-visible
    /// 显式 Err），合法调参即刻生效。
    fn resize(&self, tuning: Self::Tuning) -> Result<(), PoolResizeError>;

    /// 指标快照（美团「可观测」）：值语义，O(1) 读侧零分配。
    fn metrics(&self) -> Self::Metrics;

    /// 审计挂点（美团「事后审计」）：池域自有事件流（seq 单调 + 容量环），
    /// 票据 Drop = 关流。
    fn subscribe_audit(&self, f: impl FnMut(&PoolAuditEvent) + Send + 'static) -> PoolAuditTicket;
}
