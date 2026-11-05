//! 通用对象池：[`ObjectPool`]——**有状态**对象的池化件（reset 钩子、有界降级、
//! 分片槽位），消费方「捞出改参数即复用」的主能力在
//! <code>[`Pooled`]::DerefMut</code>（ADR 0010：加解密上下文 / WebSocket 连接
//! 对象——捞出来改参数直接用，用完丢回）。
//!
//! # 借出路径（顺序固定，永不阻塞、永不失败）
//!
//! [`ObjectPool::acquire`] 无超时参数——结构上不存在等待模式：①本线程起始片
//! 有货 → 移出（`reused` +1）；②本片空 → 轮转扫其余片（提命中率）；③全池无
//! 货且宇宙未满 → 工厂**锁外**新建（`constructed` +1，宇宙 +1；工厂 panic ⇒
//! 宇宙配额回滚后 panic 原样传播，同 `MemoryPool::acquire` 纪律，测试
//! `tests/object_pool.rs::factory_panic_rolls_back_universe_and_pool_stays_usable`
//! 钉死）；④宇宙已满 → **降级直构**（`degraded` +1，`constructed` 同步 +1——
//! 工厂调用口径同源「调用发生即留痕」）：该对象**不入池宇宙**、
//! 归还即释放、永不进 idle（测试
//! `tests/object_pool.rs::degraded_when_universe_full_released_on_return` 钉死）。
//!
//! **`degraded` 与 `constructed` 是两个独立计数，不得混账**：每个降级直构同时
//! 是一次工厂调用（两者各自单调、各自留痕）。**降级对象「不入池宇宙」与正常
//! 对象的宇宙记账是两条路径，不得混账**：宇宙（idle + 正常借出）硬上界
//! [`ObjectPoolTuning::max`]；降级对象在宇宙之外——水位线 `borrowed / max` 可
//! > 1.0（降级如实体现，[`PoolMetrics`] 文档已述该形态合法）。
//!
//! # reset 钩子（可选，单钩子 + 相位枚举）
//!
//! [`ResetPhase::OnReturn`]（默认相，.NET `IResettable` / HikariCP 同位）=
//! 归还时重置；[`ResetPhase::OnAcquire`]（deadpool `recycle` 同位）= 取出时
//! 重置。钩子在**锁外**、**独占借用**下调用。**归还路径的 reset 走
//! `catch_unwind` 兜底**：panic ⇒ 该对象状态不可信 ⇒ 丢弃（不入 idle）+
//! `reset_panicked` +1——**禁 abort**（Drop 期 panic 在 unwind 中会 abort
//! 进程），测试
//! `tests/object_pool.rs::reset_panic_on_return_discards_object_pool_survives`
//! 钉死。**payload 两相位处置不同**：`OnReturn` 相位不传播 payload（Drop 期
//! 无日志设施可用，就地消化、仅 `reset_panicked` 计数留痕）；`OnAcquire` 相位
//! 原样传播（宇宙配额回滚后 resume，不吞），测试
//! `tests/object_pool.rs::reset_on_acquire_panic_propagates_and_pool_stays_consistent`
//! 钉死。**新构造的对象（路径③④）不调 reset**——刚出厂即干净，`OnAcquire`
//! 相位同样跳过（同测试钉死该跳过语义）。注意：**`catch_unwind` 兜底只在非
//! unwind 路径生效**——unwind 路径根本不调 reset（见下节「unwind 感知归还」）。
//!
//! **`dropped_overflow` 与 `reset_panicked` 三因分账**：容量因（idle 满 / 全池
//! 余量零 / resize 裁剪）只进 `dropped_overflow`，钩子因（钩子 panic / 重入检出）
//! 只进 `reset_panicked`——禁用 `dropped_overflow` 反推钩子故障。resize 裁剪同样
//! 走容量因（份额被新 `idle_cap` 挤掉，不是钩子坏）。**钩子因只限 `OnReturn`
//! 相位**（`OnAcquire` 相位钩子 panic 原样 `resume_unwind` 传播给调用方，不计数
//! ——可见性由异常本身提供）。
//!
//! # unwind 感知归还（语义 11，r2d2 #31 防御）
//!
//! [`Pooled`] 的 `Drop` **首查 `std::thread::panicking()`**：panic unwind 路径
//! 中析构的句柄 ⇒ **不调 reset、不回池**，直接销毁对象 + `panic_dropped` +1
//! （正常对象宇宙配额回吐；降级对象仅销毁）+ 不发审计事件（无对应 kind，计数
//! 留痕）——panic 中对象不变量可能已破，回池即污染下一个借出方（r2d2 #31 挂
//! 八年：归还脏连接导致跨请求数据污染/全局锁死，
//! <https://github.com/sfackler/r2d2/issues/31> ；HikariCP 同款防御 = PoolBase
//! 标坏不回池）。测试
//! `tests/object_pool.rs::unwind_return_destroys_object_and_does_not_repool`
//! 钉死。两路径、两计数（`panic_dropped` vs `dropped_overflow`），不得混账
//! （仓库「两个独立计数」口径）。
//!
//! **reset 钩子体内不得 `acquire` 同一池**：归还路径重入 = 无限递归（每层
//! 归还再触发钩子）+ 非重入锁自死锁面——重入被 thread_local 在途标记检出 ⇒
//! **明确 panic**（归还路径 ⇒ 走 catch_unwind 兜底：对象丢弃 +
//! `reset_panicked` 留痕；`OnAcquire` 相位 ⇒ 原样传播），不是静默跳过（测试
//! `tests/object_pool.rs::reset_reentrant_acquire_is_detected_and_discarded`
//! 钉死）。**实现为进程内单标志，无法区分池身份**：钩子体内触发**任何**池的
//! reset 钩子（含跨池嵌套）都会被判为重入 ⇒ 明确 panic。这是**保守语义**
//! （宁可多拦，不放过无限递归面）；若未来需要「跨池合法」，须改为按池身份
//! 判定（thread_local 记在途池地址，仅同地址判重入）——红线只封钩子嵌套，
//! 不是「不许借对象」。
//!
//! # 释放三路径（内存换性能不会变成泄漏）
//!
//! idle_cap 满 ⇒ 归还即丢弃（`dropped_overflow` +1）；[`ObjectPool::prune_idle`]
//! 显式老化口（宿主按自己节奏调——**池不起后台线程**，不引生命周期暗面）；
//! 池 drop ⇒ 全部 idle 析构（自然）。[`Pooled::detach`] 让对象彻底脱离池
//! 生命周期：最后一个持有者放手即析构（Rust 天然，无额外回收器）。
//!
//! # 调用方红线（一手依据）
//!
//! 1. **坏对象禁还池**：判定权在调用方（池不知道 `T` 是什么），处置 =
//!    [`Pooled::detach`] 带走不还池——依据 r2d2 `has_broken`
//!    （<https://docs.rs/r2d2/latest/r2d2/trait.ManageConnection.html> ）与
//!    Commons Pool2 `invalidateObject` 动机
//!    （<https://commons.apache.org/proper/commons-pool/apidocs/org/apache/commons/pool2/ObjectPool.html> ）。
//! 2. **池化对象应「等重」**：reset 钩子负责把对象恢复到固定形态（容量归一），
//!    大小悬殊对象混池 = 池毒化 DoS（Go #23199
//!    <https://github.com/golang/go/issues/23199> 、#27735
//!    <https://github.com/golang/go/issues/27735> ）。
//! 3. **unwind 归还语义**：panic 中句柄析构 = 对象销毁不回池（见上节）。
//! 4. **`prune_idle` 尽力而为**：老化回收不构成释放失败信号（Go sync.Pool 官方口径 <https://raw.githubusercontent.com/golang/go/master/src/sync/pool.go>
//!    ）。
//!
//! # 结构性防住的坑（机制自带）
//!
//! 在借对象被池回收（借出 = 值移出槽，池物理不持引用）；重复归还 / stale
//! handle（消费式句柄——`detach`/`Drop` 吃掉 `self`）；池先于句柄死
//! （`Pooled<'p, T>` 借用池，编译期保证）；resize/prune 碰到在借对象（idle 槽
//! 扫描物理看不见在借件）；跨线程归还跨片堆积（归还回借出时的片，无跨片队列）。
//!
//! # 与 memgov::PoolGuard 的关系 / 跨 'static 边界 / 尺寸分档
//!
//! [`Pooled`] 与 `memgov::PoolGuard` 是**两个独立守卫类型，非模式变体**：
//! `Pooled` = 对象所有权移交式借用（`DerefMut` 改参数即复用、Drop RAII 归还、
//! `detach` 永久移交）——「对象本身交给任务」即 `Pooled` 形状，无需
//! 提交`；`PoolGuard` = 缓冲借用（`acquire` 超时失败语义）。跨
//! `'static` 边界（任务闭包/线程）：`Pooled<'p, T>` 借用池非 `'static`，带不
//! 进 `'static` 闭包——两条出口：**① [`ObjectPool::acquire_owned`]**（池以
//! `Arc` 持有 ⇒ 句柄自带一份强引用，`Drop` 照常归还，代价 = 每轮一次 `Arc`
//! 增删，见 [`Owned`]；`Bytes::from_owner` 的所有者、`tokio::spawn` 闭包这类
//! 「归还点是任意线程引用计数归零」的位置要的就是它）；**②
//! [`Pooled::detach`]**（永久移交：对象脱离池生命周期，任务自持，零额外成本但
//! 不再复用）。「尺寸分档」不在本件：通用池单类型；分档 = 按档多实例或组合层
//! 后续件（红线 2「池化对象等重、reset 负责容量归一」已覆盖分档动机）。
//!
//! # 按值插入（`insert`）
//!
//! [`ObjectPool::insert`] 是「按值归还」的第三个口子：对象**不必由本池工厂
//! 构造**，外部按值塞回 idle 供后续 `acquire` 复用（缓冲池 `put(Vec<u8>)` 这类
//! 形态）。契约与归还路径**逐条同口径**：`OnReturn` 相位 reset 先跑（锁外、
//! 独占借用；钩子 panic ⇒ 丢弃 + `reset_panicked`）——**必须跑**，否则外部异形/
//! 脏对象直接投给下一个借出方（池毒化，红线 2）；入槽同时认领宇宙 `alive`
//! （`max` 上界因此对插入件同样成立），**不动 `borrowed`**（从未借出）；无空位
//! ⇒ 对象交回调用方 + `dropped_overflow` + `audit OverflowDropped`（容量因）；
//! 成功入槽发 `audit Returned`（与归还件同一事件流——`PoolAuditKind` 契约词汇
//! 零改动，插入与归还共用「留存入闲槽」语义）。
//! 与 `Pooled::drop` 的唯一差别 = **容量不满足时把对象交回调用方**（归还路径
//! 只能就地丢弃——对象已在句柄里）。
//!
//! # 分片与亲和
//!
//! 分片数构造期自适应 `clamp(idle_cap, 1, 8)`（不设旋钮，同 `MemoryPool`
//! 「内部自适应不设旋钮」先例；[`ObjectPool::resize`] **不改分片数**——分片数
//! 变更 = 槽位重分布，复杂度不值）。
//!
//! **线程亲和片**：同一 (线程, 池) **稳定领一个片号**（`AFFINITY` 表；首个
//! 领号者拿 0 号 ⇒ 单线程用法与旧游标起点一致、行为确定性不随线程调度漂移），
//! 借出先扫本片、空则轮转扫其余片。**亲和表按池地址分槽**（`AFFINITY_SLOTS`
//! 个）——同一线程同时用多个池（加解密 ctx 池 + 连接池是 ADR 背景里的真实宿主
//! 形态）时各自稳定、互不覆盖；单槽实现会让线程每次换池都重新领号，亲和等于
//! 失效。**归还优先回 home 片**（借出时的片，LIFO 热件常驻热缓存）；**home 满时
//! 迁入有余量的片**（`alive` 随迁、先迁入再退 home ⇒ 账面守恒且永不低估），
//! **迁片只发生在归还且 home 片 idle 满这一种情形**。
//!
//! **游标不做每 op 轮转**：每 op 换片会让每片数据每 op 在不同核间往返（跨核行
//! 迁移），实测把 8 线程扩展比压到 ~1.1x；亲和后各片数据常驻本核
//! （`examples/object_pool_multidim.rs` dim4：0.51x → 4.6x）。
//!
//! **记账下沉进分片**：每片自带 `borrowed` / `alive` / `reused` 普通 `usize`
//! （本片 idle 数即槽位数），且每片独占一条 cache line（`Shard` 的
//! `#[repr(align(64))]`）——借还热路径只在片锁内读写，无跨线程原子 RMW、无相邻
//! 片伪共享。`max` / `idle_cap` **按片均分**（余数补给前几片；各片之和恰等于
//! 配置值 ⇒ 「总和 ≤ 配置值」硬上界不变）；**配额一律在目标片锁内读**——
//! [`ObjectPool::resize`] 先 `store` 新值、再逐片加锁裁剪，于是归还的配额裁决
//! 要么读到新值、要么其 push 被紧随其后的裁剪覆盖（锁外的陈旧快照没有这条收敛
//! 路径，缩容并发归还时能把 idle 永久顶在新上界之外）；`metrics()` 逐片取锁求和
//! （O(shards)，非热路径）。`degraded` / `constructed` / `pruned` /
//! `panic_dropped` / `reset_panicked` / `dropped_overflow` 仍是**全局原子**：全在
//! 低频面（新建 / 降级 / 老化 / 拒绝 / 钩子故障），`constructed` 只在 miss 路径
//! 打一次、不是每借还热路径，
//! 下沉要额外取一次片锁（收益 0、成本为正），故按原状保留。
//!
//! # 为什么放独立文件
//!
//! `pool.rs` 已超单文件 800 行上限，且本件与 `PoolBase` 组合件是两个独立
//! 生命周期：本件自带槽位存储与 reset 钩子，**不借 `PoolBase` 的单锁账本**——
//! 记账走**片内计数**（与槽位同一把片锁内完成，无第三套同步面；全局记账锁会
//! 重新制造热点）；审计环经 `crate::pool::PoolAuditSink` 共用（公共逻辑写一
//! 次，`PoolBase` 公开面与行为零变化）；入口校验复用 `crate::pool::validate`
//! 同源（禁复制第二套规则）。
//!
//! # 字节配额（缺口：件数上界 ≠ 内存上界）
//!
//! `max` / `idle_cap` 纯按件数；件大小可差几个数量级（8KB 缓冲 vs 1MB+ TLS
//! 连接），件数上界不等于内存上界。[`ByteBudget`] 是**进程级共享字节预算
//! 仲裁器**（多池共用一个上限的原语，`Arc<ByteBudget>` 传入多池即共享）。
//! 池侧两条构造路（[`ObjectPool::with_byte_quota`] 池私有 /
//! [`ObjectPool::with_shared_byte_quota`] 多池共享）均要求**成对给出**
//! 字节预算与单件计量钩子 `weight(&T) -> usize`（计量钩子缺位 = 无从记账，
//! 单边给出构造期 panic）。归还/插入路径**在片锁内**向预算认领单件字节
//! （`try_take` 原子 check+act，无竞态窗），超预算即丢弃 + `dropped_overflow`
//! （容量因，与件数溢出同一计数——字节超限也是容量因）；借出/prune/resize
//! 裁剪对称回卷。字节上界**构造期定死**（同 reset 钩子先例，不进
//! [`ObjectPoolTuning`]——resize 调件数面，不动安全字节上界）。**weight 钩子
//! 体内不得 acquire 同一池**（调用点在片锁内，回调池借还 = 锁自死锁面）。
//!
//! # 强制清零（缺口：reset 钩子可选，忘配 = 明文驻留）
//!
//! [`ObjectPool::with_zeroing_reset`] 是「含明文/密钥对象」的**指定构造路**：
//! 等价 `with_reset(.., zero, ResetPhase::OnReturn)` 的命名变体，把「归还必
//! 清零」的意图抬到代码审查点可见——review 看到 `new`/`with_reset` 即知无清零
//! 语义。不把清零默认化（会破坏既有「容量归一」语义 + 多一次 memset）；
//! 泛型 `T` 无统一「清零」概念，清零动作由调用方给（缓冲类只许清已写入区，
//! 禁整块清零——bd hdugl 实测 64KiB 整块清零 190.8µs vs 已写区 300ns）。
//!
//! # 自动老化（缺口：prune_idle 无调用点 = 永不老化）
//!
//! [`ObjectPool::prune_idle_with_clock`] 接 udf [`Clock`](skeleton::clock::Clock)
//! 契约（测试注入 [`MockClock`](skeleton::clock::MockClock) 确定性推进；
//! 生产传 `SystemClock`）；**宿主用既有托管口起低频老化任务**（xray-frame
//! `spawn_idle_pruner`：FxScope 托管 + 周期 prune），**池不起后台线程**
//! 纪律不变（udf 只提供同步老化口，任务生命周期归宿主托管口）。
//!
//! # 测试锚
//!
//! 语义契约逐条钉死于 `tests/object_pool.rs`（管理面基座已在
//! `tests/pool_base.rs` 逐件覆盖，本件只测特化面）；端到端演示见
//! `examples/object_pool_multidim.rs`。
//!
//! feature `pool`（纯 std + 契约词汇，零三方增量）；由 lib.rs 以
//! `#[cfg(feature = "pool")] pub mod objpool;` 接线，crate 根 re-export
//! `ObjectPool` / `Pooled` / `Owned` / `ResetPhase` / `ObjectPoolTuning` /
//! `ObjectPoolMetrics` 六件（照 `PoolBase`/`PooledThreadPool` 的 pool 域根导出
//! 惯例）。

use std::{
    borrow::Borrow,
    cell::Cell,
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    thread_local,
    time::{Duration, Instant},
};

use skeleton::{
    clock::Clock,
    pool::{
        PoolAuditEvent, PoolAuditKind, PoolAuditTicket, PoolManaged, PoolMetrics, PoolResizeError,
    },
};

use crate::pool::{AUDIT_RING_DEFAULT_CAP, PoolAuditSink, validate};

// 术语表：**池宇宙** = idle 槽 + 正常借出数（硬上界 `max`）；**降级对象**在
// 宇宙之外。下文所有「宇宙」均指此口径。

/// reset 钩子相位：归还时重置（默认）/ 取出时重置。
///
/// 外部实践无统一答案（Commons Pool2 = 双端 passivate/activate 分离；
/// .NET/HikariCP = 归还时；deadpool = 取出时）→ 本件以相位枚举同时覆盖两端
/// 形态，调用方按场景选：reset 成本 ≪ 构造成本时才值得配钩子，否则优先不配。
///
/// **钩子体内不得 `acquire` 同一池**（归还路径重入 = 无限递归面）——重入被
/// 检出 ⇒ 明确 panic（归还路径 ⇒ 对象丢弃 + `reset_panicked` 留痕；本相位
/// 取出路径 ⇒ 原样传播），不是静默跳过；**跨池嵌套同样被拦**（单标志无法区分
/// 池身份，见模块文档「重入」节）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResetPhase {
    /// 归还时重置（默认相）：归还路径先在锁外调钩子、再判保留。
    OnReturn,
    /// 取出时重置：仅对**复用件**调用——新构造对象跳过（刚出厂即干净）。
    OnAcquire,
}

/// 运行时调参形状：池深 + 空闲槽上限（与 `memgov::MemoryTuning` 同形但语义
/// 自持——各池特化，同 `pool::ThreadPoolTuning` 先例）。
///
/// 非法组合（`max == 0` / `idle_cap == 0` / `idle_cap > max`）构造期 panic、
/// [`ObjectPool::resize`] 显式 `Err`——与 `PoolBase` 同一套规则（`validate`
/// 同源复用，禁复制第二套）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectPoolTuning {
    /// 池深：池宇宙（idle + 正常借出）总量上限，**按片均分**（余数补给前几片，
    /// 各片之和恰等于本值 ⇒ 总量硬上界不变）。降级直构不受此限——降级对象在
    /// 宇宙之外（ADR 0010：保留量硬顶、分配量由并发决定）。
    pub max: usize,
    /// 空闲槽上限（归还留存判据）：常驻保留量硬顶、**按片均分**（总和 ≤ 本值），
    /// 内存换性能不会变成 OOM。
    pub idle_cap: usize,
}

/// 指标快照：共享核心形 + 本件特有读数。经 [`Borrow`] 可达 [`PoolMetrics`]。
///
/// **`degraded` 与 `constructed` 是两个独立计数，不得混账**（口径见模块文档
/// 「借出路径」节）。
///
/// **`reset_panicked` 与 `core.dropped_overflow` 三因分账**：前者只数 reset 钩子
/// 因（钩子 panic / 重入检出，**仅 `OnReturn` 相位**——`OnAcquire` 相位原样
/// 传播不计数），后者只数容量因（idle 满 / 全池余量零 / resize 裁剪）。禁用
/// `dropped_overflow` 反推钩子故障（口径见 [`Self::reset_panicked`]）。
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ObjectPoolMetrics {
    /// 共享核心读数（max/idle/borrowed/dropped_overflow/watermark）。
    ///
    /// **`borrowed` 含降级借出**（降级直构在宇宙之外，但仍借出给调用方），故
    /// `idle + borrowed <= max` **不成立**——`max` 只约束宇宙部分（idle + 正常
    /// 借出）；要拿「宇宙内借出数」请用 `borrowed - 降级在借数`，本件不单列该
    /// 读数（降级件「归还即释放、永不进 idle」，长期稳态下 ≈ `degraded` 的未归还
    /// 部分）。水位线 `borrowed / max` 同理可 > 1.0（形态合法，读数如实）。
    pub core: PoolMetrics,
    /// 降级直构次数（池宇宙已满时）。
    pub degraded: u64,
    /// 命中复用次数（从 idle 槽移出；分片计数求和）——**= idle 槽移出次数**，
    /// 含随后被 reset panic 销毁者（移出即计入，销毁与否归 `reset_panicked`）。
    pub reused: u64,
    /// 工厂调用总次数（含降级直构——调用发生即留痕，工厂 panic 时本计数已 +1）。
    pub constructed: u64,
    /// prune_idle 释放数。
    pub pruned: u64,
    /// reset 钩子因导致的对象销毁数（`OnReturn` 相位钩子 panic 或重入检出 ⇒ 对象
    /// 状态不可信、丢弃不入 idle）。**与 `dropped_overflow`（容量因）是两个独立
    /// 计数，不得混账**——容量因（idle 满 / 全池余量零 / resize 裁剪）只进
    /// `dropped_overflow`，钩子因只进本计数。审计事件无对应 kind ⇒ 不发事件（契约
    /// 词汇零改动），计数即留痕。
    pub reset_panicked: u64,
    /// unwind 中归还而销毁的对象数（panic 路径归还——对象不变量可能已破，禁
    /// reset 禁回池；与 `dropped_overflow` 是两个独立计数，不得混账）。
    pub panic_dropped: u64,
    /// 常驻 idle 字节合计（字节配额池：预算已认领部分；未启用恒 0）。**共享
    /// 预算池（`with_shared_byte_quota`）的读数 = 共享 [`ByteBudget`] 的全局
    /// held（含其他池的认领）**——字节记账单一账本（共享件一处），池内不双记。
    pub idle_bytes: usize,
    /// 池字节预算（构造期定死；未启用恒 0）。
    pub byte_budget: usize,
}

impl Borrow<PoolMetrics> for ObjectPoolMetrics {
    fn borrow(&self) -> &PoolMetrics {
        &self.core
    }
}

/// reset 钩子形状收口（`type_complexity` 抑制 + 单点定义，同 `pool::AuditCallback`
/// 先例）：`Fn(&mut T)` 在锁外、独占借用下调用。
type ResetHook<T> = Box<dyn Fn(&mut T) + Send + Sync>;

/// 单件字节计量钩子：`Fn(&T) -> usize`——调用点在**片锁内**（归还/借出/裁剪
/// 路径），钩子体内不得 acquire 同一池（锁自死锁面，同 reset 钩子红线）；
/// 返回值按**对象当前真实驻留字节**计（`Vec` 类 = `capacity()`，不是 `size_of`）。
type WeightHook<T> = Box<dyn Fn(&T) -> usize + Send + Sync>;

/// 进程级共享字节预算仲裁器：多池共用一个内存上限的原语（bd jovl4 缺口 1）。
///
/// **共享形态** = 同一份 `Arc<ByteBudget>` 传给多个池——各池的归还/插入在
/// `try_take` 上竞争同一份余额，谁先认领谁占用，超预算方丢弃（`dropped_overflow`
/// 容量因）。**记账 fail-fast**：`give_back` 下溢 = 记账 bug，panic（同片内
/// `take` 纪律——计数错账禁静默钳位）。低频面（借还的字节裁决在归还路径、
/// 相对 acquire 热路径低频），内部单把 `Mutex` 足够；持锁序恒「片锁 → 本锁」
/// 单向（本锁内不回调池），无环无死锁。
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
///
/// use skeleton_infra::objpool::{ByteBudget, ObjectPool};
///
/// let budget = Arc::new(ByteBudget::new(64 * 1024));
/// // 多池共享同一份 64KiB 预算：
/// let a = ObjectPool::with_shared_byte_quota(8, 4, Vec::<u8>::new, Arc::clone(&budget), |v| {
///     v.capacity()
/// });
/// let b = ObjectPool::with_shared_byte_quota(8, 4, Vec::<u8>::new, Arc::clone(&budget), |v| {
///     v.capacity()
/// });
/// assert_eq!(budget.available(), 64 * 1024);
/// ```
pub struct ByteBudget {
    total: usize,
    held: Mutex<usize>,
}

impl ByteBudget {
    /// 构造（fail-fast：`total == 0` panic——「0 预算的池」无从起用，同
    /// `validate` 纪律；不限字节配额的池直接走 `new`/`with_reset`，不设
    /// 「无限预算」形态）。
    ///
    /// # Panics
    ///
    /// `total == 0`。
    pub fn new(total: usize) -> Self {
        assert!(total > 0, "ByteBudget::new: total must be non-zero");
        Self {
            total,
            held: Mutex::new(0),
        }
    }

    /// 预算总额（构造期定死，不可 resize——字节上界是安全面）。
    pub fn total(&self) -> usize {
        self.total
    }

    /// 当前已认领字节（池内常驻对象的真实驻留合计；借出对象不计）。
    pub fn held(&self) -> usize {
        *lock(&self.held)
    }

    /// 剩余可认领字节。
    pub fn available(&self) -> usize {
        self.total - self.held()
    }

    /// 认领 `n` 字节（原子 check+act：余额足 ⇒ 认领并返回 true；不足 ⇒ 返回
    /// false，调用方按容量因丢弃）。`n == 0` 恒 true（未启用计量面的调用点
    /// 短路，零开销）。
    pub fn try_take(&self, n: usize) -> bool {
        if n == 0 {
            return true;
        }
        let mut held = lock(&self.held);
        if *held + n > self.total {
            return false;
        }
        *held += n;
        true
    }

    /// 回卷 `n` 字节（借出 / prune / resize 裁剪的对称面）。下溢 = 记账 bug，
    /// fail-fast panic。`n == 0` 短路。
    pub fn give_back(&self, n: usize) {
        if n == 0 {
            return;
        }
        let mut held = lock(&self.held);
        assert!(
            *held >= n,
            "ByteBudget: give_back would go negative (bookkeeping bug)"
        );
        *held -= n;
    }
}

impl std::fmt::Debug for ByteBudget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ByteBudget")
            .field("total", &self.total)
            .field("held", &self.held())
            .finish()
    }
}

/// 空闲槽：对象值 + 最近使用时间戳（归还时刷新；[`ObjectPool::prune_idle`]
/// 按龄释放的判据）。
struct IdleSlot<T> {
    obj: T,
    last_used: Instant,
}

/// 分片：槽位 + **片内计数**——借还热路径只碰本片锁与片内普通 `usize`，无跨线程
/// 原子 RMW。计数只在本片锁内读写，且每片独占一条 cache line（`align(64)`）：
/// 相邻片挤同一行会让 8 线程各记各的账时互相踢行（伪共享），实测足以把扩展比
/// 压到 1 以下。本片 idle 数即 `idle.len()`，不另设冗余计数。
#[repr(align(64))]
struct Shard<T> {
    /// 本片空闲槽（LIFO——热件常驻热缓存）。
    idle: Vec<IdleSlot<T>>,
    /// 本片借出数（正常件 + 归本片的降级件）。
    borrowed: usize,
    /// 本片宇宙存活数（idle + 正常借出；降级件不入宇宙——两条记账路径）。
    alive: usize,
    /// 本片命中复用数（从本片 idle 槽移出）——与借出记账同一临界区，零额外原子。
    reused: u64,
}

/// 亲和表容量：同一线程可稳定亲和 `AFFINITY_SLOTS` 个池。**单槽会互相覆盖**——
/// 宿主同时用多个池（加解密 ctx 池 + 连接池 + …）时，线程每次切换池都要重新领
/// 号，亲和等于失效（`AFFINITY_SLOTS` 是「常见宿主形态」而非上限：超出只降级为
/// 起点漂移）。
const AFFINITY_SLOTS: usize = 4;

/// 亲和表空槽键（真实池地址不可能等于 `usize::MAX`——空槽哨兵）。
const EMPTY_AFFINITY_KEY: usize = usize::MAX;

thread_local! {
    /// 线程对各池的**亲和片**表：`(池地址, 片号)` × `AFFINITY_SLOTS`，线性查
    /// （≤4 次比较，热路径零分配）。
    ///
    /// 首次用某池时向该池领一个片号（`ObjectPool::next_affinity`，每 (线程, 池)
    /// 一次），之后固定 —— 本线程的借还都优先落在自己那片，片内槽位/计数/锁的
    /// cache line 只被本线程踢，**跨核行迁移消失**（实测：轮转游标每 op 换一片
    /// 时，每片数据每 op 在不同核间往返，8 线程扩展比被压到 ~1.1x；固定亲和后
    /// 接近线性）。池不再被本线程使用后旧记录自然失效：片号只决定「起始片」，
    /// 陈旧值最坏只是起点偏一点（正确性不依赖它）。
    static AFFINITY: Cell<[(usize, usize); AFFINITY_SLOTS]> =
        const { Cell::new([(EMPTY_AFFINITY_KEY, 0); AFFINITY_SLOTS]) };
    /// reset 钩子在途标记：钩子体内再进借还路径的 reset = 契约违规——
    /// 归还路径重入 = 无限递归（每层归还再触发钩子）+ 非重入锁自死锁面，
    /// fail-fast 检出（禁静默跳过）。**单标志无法区分池身份** ⇒ 跨池嵌套同样
    /// 判重入（保守语义；按池身份判定是未来升级路径）。
    static RESET_IN_FLIGHT: Cell<bool> = const { Cell::new(false) };
}

/// std Mutex 取锁：中毒 = 持锁方已 panic——fail-fast 传播（同 `pool::lock`
/// 纪律）。收口一处，调用点只见加锁。
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .expect("objpool lock poisoned (prior holder panicked)")
}

/// 片内计数回卷 n：减到负 = 记账 bug（fail-fast，release 同判——计数错账禁静默
/// 钳位）。
fn take(count: &mut usize, n: usize, what: &str) {
    assert!(
        *count >= n,
        "objpool bookkeeping bug: {what} would go negative"
    );
    *count -= n;
}

/// 按片均分上限：`total` 平分、余数补给前 `total % shards` 片——各片之和恒等于
/// `total`，故「各片硬上界」即「全局硬上界」。`max` / `idle_cap` 都走本函数
/// （总和 ≤ 配置值、分布更均匀）。
fn share(total: usize, shards: usize, idx: usize) -> usize {
    total / shards + usize::from(idx < total % shards)
}

/// reset 钩子调用收口（防呆）：本线程已在某 reset 钩子体内时再次进入钩子 =
/// 契约违规（归还路径重入 = 无限递归：每层归还再触发钩子——fail-fast 检出，
/// 禁静默跳过）。返回 Ok(()) 或钩子 panic payload：
/// - 归还路径（`Pooled::drop`）：payload 由外层 `catch_unwind` 语义消化 （对象丢弃 +
///   `reset_panicked` 留痕，禁 abort）——**`OnReturn` 相位不传播 payload**（Drop
///   期无日志设施可用，就地丢弃；故障可见性走 `reset_panicked` 计数，`PoolAuditKind` 无钩子故障词汇
///   ⇔ 不发审计事件）；
/// - `OnAcquire` 路径：payload 经 `resume_unwind` 原样传播（宇宙配额回滚后）。
fn run_reset_hook<T>(
    reset: &dyn Fn(&mut T),
    obj: &mut T,
) -> Result<(), Box<dyn std::any::Any + Send>> {
    let outer_in_flight = RESET_IN_FLIGHT.with(|c| c.replace(true));
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        if outer_in_flight {
            panic!(
                "ObjectPool: reset 钩子重入同一池借还路径 = 契约违规（归还路径重入 = 无限递归面，禁）"
            );
        }
        reset(obj);
    }));
    RESET_IN_FLIGHT.with(|c| c.set(outer_in_flight));
    outcome
}

/// 通用对象池：池化**有状态**对象——借出期池不持强引用（值移动出槽），捞出来
/// 改参数（[`Pooled`] 的 `DerefMut`）即复用。
///
/// # 三参封顶（Less is More）
///
/// max / idle_cap 在 [`ObjectPoolTuning`]（resize 可调）；reset 钩子构造期
/// 一次性给定（[`ResetPhase`] 定时点）。其余（分片数、扫片顺序、留存裁剪时机）
/// 内部自适应，不设旋钮。
///
/// # Examples
///
/// ```
/// use skeleton_infra::objpool::ObjectPool;
///
/// let pool = ObjectPool::new(4, 2, Vec::<u8>::new);
/// let mut buf = pool.acquire();
/// buf.push(7);
/// assert_eq!(pool.metrics().core.borrowed, 1);
/// drop(buf); // RAII：Drop 即还（禁手动还）
/// assert_eq!(pool.metrics().core.idle, 1);
///
/// let again = pool.acquire(); // 复用同一件（状态跨借还保留）
/// assert_eq!(&*again, &[7]);
/// ```
pub struct ObjectPool<T> {
    /// 分片槽位 + **片内计数**：每片一把 `Mutex<Shard<T>>`（LIFO——热件常驻热
    /// 缓存）。借还热路径的全部记账都在本片锁内完成——零跨线程原子 RMW（8 线程
    /// 借还不产生 cache line 乒乓）。
    // ponytail: 每片仍是一把 Mutex（非无锁 CAS）——分片 + thread-local 轮转扫
    // 已摊薄争用；升级路径 = 无锁 Treiber 栈（`examples/memory_pool_profile.rs`
    // 有原型，ADR 0010 登记「每片一锁」为已知上限）。
    slots: Vec<Mutex<Shard<T>>>,
    /// 边界（配置面、非热路径——只有 `resize` 写）：`max` 池宇宙上限、`idle_cap`
    /// 空闲槽上限，两者**按片均分**（见 [`share`]）：各片之和恰等于配置值 ⇒
    /// 「总和 ≤ 配置值」的硬上界性质不变（改由各片自行保证）。Relaxed 序足够：
    /// 读侧终值可见性由 join 边缘保证（同 `tests/counter_concurrency.rs` 头注
    /// 契约）。
    max: AtomicUsize,
    idle_cap: AtomicUsize,
    /// 亲和片发号器：每 (线程, 池) 领一次（只增、非热路径）——见 [`AFFINITY`]。
    next_affinity: AtomicUsize,
    degraded: AtomicU64,
    constructed: AtomicU64,
    pruned: AtomicU64,
    /// unwind 中归还而销毁数（与 `dropped_overflow` 两个独立计数，不得混账）。
    panic_dropped: AtomicU64,
    /// reset 钩子因（钩子 panic / 重入检出）销毁数——与 `dropped_overflow`（容量
    /// 因）**三因分账**：同一次丢弃只进一个口径（低频面，全局原子足够）。
    reset_panicked: AtomicU64,
    dropped_overflow: AtomicU64,
    /// 审计环（`PoolAuditSink` 共用抽取件——公共逻辑写一次）。
    sink: PoolAuditSink,
    factory: Box<dyn Fn() -> T + Send + Sync>,
    reset: Option<ResetHook<T>>,
    phase: ResetPhase,
    /// 单件字节计量（与 `budget` 成对出现——构造期校验，单边 panic）。
    weight: Option<WeightHook<T>>,
    /// 字节预算（构造期定死；`None` = 不启用字节配额——借还零额外开销）。
    budget: Option<Arc<ByteBudget>>,
}

impl<T> ObjectPool<T> {
    /// 构造（fail-fast：非法参数 panic，同 `PoolBase::new` 纪律）。无 reset
    /// 钩子——状态跨借还保留；需要重置语义用 [`Self::with_reset`]。
    ///
    /// # Panics
    ///
    /// `max == 0` / `idle_cap == 0` / `idle_cap > max`。
    pub fn new(
        max: usize,
        idle_cap: usize,
        factory: impl Fn() -> T + Send + Sync + 'static,
    ) -> Self {
        Self::build(
            max,
            idle_cap,
            Box::new(factory),
            None,
            ResetPhase::OnReturn,
            None,
            None,
        )
    }

    /// 构造 + reset 钩子。`phase` 决定钩子调用时点（见 [`ResetPhase`]）；
    /// 新构造的对象不调 reset（刚出厂即干净）。
    ///
    /// # Panics
    ///
    /// 同 [`Self::new`]。
    pub fn with_reset(
        max: usize,
        idle_cap: usize,
        factory: impl Fn() -> T + Send + Sync + 'static,
        reset: impl Fn(&mut T) + Send + Sync + 'static,
        phase: ResetPhase,
    ) -> Self {
        Self::build(
            max,
            idle_cap,
            Box::new(factory),
            Some(Box::new(reset)),
            phase,
            None,
            None,
        )
    }

    /// 构造 + **强制清零**（归还相位 [`ResetPhase::OnReturn`]）：含明文/密钥
    /// 对象的**指定构造路**——把「归还必清零」的意图抬到代码审查点可见
    /// （`new`/`with_reset` 的池无清零语义，review 一眼可辨）。清零动作由
    /// 调用方给（泛型 `T` 无统一清零概念）；缓冲类只许清已写入区
    /// （`[..len]`），禁整块清零（bd hdugl：64KiB 整块 190.8µs vs 已写区
    /// 300ns——清零成本必须与写入量同阶）。钩子纪律（panic 兜底/重入检出/
    /// 不回池）与 [`Self::with_reset`] 逐条同款。
    ///
    /// # Panics
    ///
    /// 同 [`Self::new`]。
    pub fn with_zeroing_reset(
        max: usize,
        idle_cap: usize,
        factory: impl Fn() -> T + Send + Sync + 'static,
        zero: impl Fn(&mut T) + Send + Sync + 'static,
    ) -> Self {
        Self::build(
            max,
            idle_cap,
            Box::new(factory),
            Some(Box::new(zero)),
            ResetPhase::OnReturn,
            None,
            None,
        )
    }

    /// 构造 + **池私有字节配额**（归还/插入超预算即丢弃，`dropped_overflow`
    /// 容量因）。`byte_budget` 与 `weight` 必须成对（计量缺位 = 无从记账）；
    /// 多池共用一份上限用 [`Self::with_shared_byte_quota`] + 共享
    /// `Arc<ByteBudget>`。
    ///
    /// # Panics
    ///
    /// 同 [`Self::new`]；`byte_budget == 0`（[`ByteBudget::new`] 纪律）。
    pub fn with_byte_quota(
        max: usize,
        idle_cap: usize,
        factory: impl Fn() -> T + Send + Sync + 'static,
        byte_budget: usize,
        weight: impl Fn(&T) -> usize + Send + Sync + 'static,
    ) -> Self {
        Self::build(
            max,
            idle_cap,
            Box::new(factory),
            None,
            ResetPhase::OnReturn,
            Some(Box::new(weight)),
            Some(Arc::new(ByteBudget::new(byte_budget))),
        )
    }

    /// 构造 + **共享字节预算**：`budget` 为多池共用的同一份
    /// `Arc<ByteBudget>`——各池归还/插入竞争同一份余额，超预算方丢弃。
    ///
    /// # Panics
    ///
    /// 同 [`Self::new`]。
    pub fn with_shared_byte_quota(
        max: usize,
        idle_cap: usize,
        factory: impl Fn() -> T + Send + Sync + 'static,
        budget: Arc<ByteBudget>,
        weight: impl Fn(&T) -> usize + Send + Sync + 'static,
    ) -> Self {
        Self::build(
            max,
            idle_cap,
            Box::new(factory),
            None,
            ResetPhase::OnReturn,
            Some(Box::new(weight)),
            Some(budget),
        )
    }

    /// 构造收口（公开构造器共用）：校验 fail-fast + 分片自适应 + 片内计数清零。
    /// `weight` / `budget` 必须成对出现（字节计量与配额缺一不可记账，单边
    /// panic——半配置比不配置更危险：有预算无计量 = 恒拒收）。
    #[allow(
        clippy::too_many_arguments,
        reason = "构造收口聚齐七个构造参数（max/idle_cap/factory/reset/phase/weight/budget），拆 builder 反而引入第二构造面"
    )]
    fn build(
        max: usize,
        idle_cap: usize,
        factory: Box<dyn Fn() -> T + Send + Sync>,
        reset: Option<ResetHook<T>>,
        phase: ResetPhase,
        weight: Option<WeightHook<T>>,
        budget: Option<Arc<ByteBudget>>,
    ) -> Self {
        if let Err(e) = validate(max, idle_cap) {
            panic!("ObjectPool::new: {e}");
        }
        assert!(
            weight.is_some() == budget.is_some(),
            "ObjectPool: byte budget and weight hook must be provided together"
        );
        // 分片自适应（构造期定死，resize 不改——doc 钉死）。
        let shards = idle_cap.clamp(1, 8);
        Self {
            slots: (0..shards)
                .map(|_| {
                    Mutex::new(Shard {
                        idle: Vec::new(),
                        borrowed: 0,
                        alive: 0,
                        reused: 0,
                    })
                })
                .collect(),
            max: AtomicUsize::new(max),
            idle_cap: AtomicUsize::new(idle_cap),
            next_affinity: AtomicUsize::new(0),
            degraded: AtomicU64::new(0),
            constructed: AtomicU64::new(0),
            pruned: AtomicU64::new(0),
            panic_dropped: AtomicU64::new(0),
            reset_panicked: AtomicU64::new(0),
            dropped_overflow: AtomicU64::new(0),
            sink: PoolAuditSink::new(),
            factory,
            reset,
            phase,
            weight,
            budget,
        }
    }

    /// 借出一件：**永不阻塞、永不失败、无超时参数**（结构上不存在等待模式）。
    ///
    /// 路径顺序固定（见模块文档「借出路径」节）。`ResetPhase::OnAcquire` 相位
    /// 的 reset 只对复用件调用，panic 原样传播（宇宙配额回滚后 resume，不吞）。
    /// 三条路径的记账都在**目标片锁内**完成（片内普通 `usize`，无跨线程原子）。
    pub fn acquire(&self) -> Pooled<'_, T> {
        let (home, obj, degraded) = self.pop_or_build();
        Pooled {
            pool: self,
            home,
            slot: Some(obj),
            degraded,
        }
    }

    /// `'static` 借出（同 [`Self::acquire`] 的四路径与记账，只是句柄可跨
    /// `'static` 边界）：**要求池以 `Arc` 持有**——句柄 `Drop` 时要回到池，就得
    /// 自己握一份池的强引用（`'static` 句柄无法借用池；这是唯一不泄漏的形态）。
    ///
    /// 代价 = 每借还一轮一次 `Arc` 增删（两次原子），**不是零成本**：零成本主
    /// 路径仍是借用形态 [`Self::acquire`]，只有 `'static` 位（`tokio::spawn`
    /// 闭包、`Bytes::from_owner` 的所有者——归还点是任意线程的引用计数归零）才
    /// 用它。**被否决的两种形态**：泄漏 `&'static ObjectPool`（零成本，但池永不
    /// 释放）；归还通道（mpsc）（零 `Arc`，但多一次分配 + 可能跨线程唤醒，且
    /// `'static` 通道端仍要跟着对象走）。
    pub fn acquire_owned(self: &Arc<Self>) -> Owned<T> {
        let (home, obj, degraded) = self.pop_or_build();
        Owned {
            pool: Arc::clone(self),
            home,
            slot: Some(obj),
            degraded,
        }
    }

    /// 借出四路径（[`Self::acquire`] / [`Self::acquire_owned`] 共用）：返回
    /// (home 片, 对象, 是否降级)。
    fn pop_or_build(&self) -> (usize, T, bool) {
        let start = self.home_shard();
        // ①② 本线程片优先、空则轮转扫其余片（锁内同时回卷本片借出数）。
        if let Some((mut obj, home)) = self.pop_idle(start) {
            if self.phase == ResetPhase::OnAcquire {
                if let Some(reset) = self.reset.as_ref() {
                    if let Err(payload) = run_reset_hook(reset.as_ref(), &mut obj) {
                        // 宇宙配额回滚后 panic 原样传播（同 MemoryPool 工厂 panic 纪律）。
                        self.release(home, true);
                        resume_unwind(payload);
                    }
                }
            }
            self.sink.emit(PoolAuditKind::Borrowed);
            return (home, obj, false);
        }
        // ③ 确有片仍有宇宙空位：工厂锁外新建（新对象不调 reset）。
        if let Some(home) = self.claim_growth(start) {
            // 计数在工厂调用前落账：工厂 panic 时 constructed 已 +1（调用留痕）。
            self.constructed.fetch_add(1, Ordering::Relaxed);
            let obj = match catch_unwind(AssertUnwindSafe(|| (self.factory)())) {
                Ok(obj) => obj,
                Err(payload) => {
                    self.unclaim_growth(home);
                    resume_unwind(payload);
                }
            };
            self.record_borrow(home);
            self.sink.emit(PoolAuditKind::Borrowed);
            return (home, obj, false);
        }
        // ④ 全片宇宙已满：降级直构——不入池宇宙（alive 不记账）、归还即释放。
        // 计数口径与路径③同源：调用发生即留痕（工厂 panic 时已计数、不产生对象）。
        self.constructed.fetch_add(1, Ordering::Relaxed);
        self.degraded.fetch_add(1, Ordering::Relaxed);
        let obj = match catch_unwind(AssertUnwindSafe(|| (self.factory)())) {
            Ok(obj) => obj,
            // 无宇宙配额可回滚（降级对象从未入宇宙）——panic 原样传播。
            Err(payload) => resume_unwind(payload),
        };
        self.record_borrow(start);
        self.sink.emit(PoolAuditKind::Borrowed);
        (start, obj, true)
    }

    /// 按值插入（「按值归还」口子）：把**已有**对象放进 idle 槽，供后续
    /// [`Self::acquire`] 复用——给「对象不是本池工厂造的」消费方（缓冲池按值
    /// `put(Vec<u8>)`、`Bytes::from_owner` 所有者的引用计数归零归还）开口子。
    ///
    /// 判定顺序与 [`Pooled`] 归还**同序**：`OnReturn` 相位的 reset 钩子先跑
    /// （锁外、独占借用；钩子 panic ⇒ 对象丢弃 + `reset_panicked` +1，与归还路径
    /// 同口径、不发审计事件）。**插入必须跑该钩子**：红线 2「池化对象等重、reset
    /// 负责容量归一」——按值塞进来的外部对象形态不受本池控制，跳过钩子即把脏/
    /// 异形对象投给下一个借出方（池毒化）。`OnAcquire` 相位不在此触发（钩子时点
    /// 在取出时，同归还路径）。随后**一次片锁内**完成「idle 片配额 + 宇宙配额」
    /// 双裁决并入槽（配额在片锁内读，与并发 `resize` 的收敛论证同
    /// [`Self::resize`]）；全片皆无空位 ⇒ 对象交回调用方 + `dropped_overflow` +1
    /// + `audit OverflowDropped`（容量因，与钩子因两账不混）；成功入槽发 `audit
    ///   Returned`（与归还件同一事件流，契约词汇零改动）。
    ///
    /// **宇宙记账**：插入件此前不在宇宙内 ⇒ 入槽同时认领 `alive`（同
    /// `claim_growth` 的上界保证），**不动 `borrowed`**（从未借出，不得
    /// 混账）。故 `max` 是「idle + 正常借出」的硬上界：宇宙已满时即使 idle 有空
    /// 位也拒收（溢出到宇宙外就是击穿上界）。
    ///
    /// 返回值：`None` = 池已接管（已入 idle；或钩子失败被销毁——两种情形调用方都
    /// 不该再持有它）；`Some(obj)` = 池没收下（容量因），对象交回（`OnReturn` 相位
    /// 下已跑过 reset，与归还路径同序）。
    #[must_use = "Some(obj) 表示池未收下，对象所有权回到调用方"]
    pub fn insert(&self, mut obj: T) -> Option<T> {
        if self.phase == ResetPhase::OnReturn {
            if let Some(reset) = self.reset.as_ref() {
                if run_reset_hook(reset.as_ref(), &mut obj).is_err() {
                    self.reset_panicked.fetch_add(1, Ordering::Relaxed);
                    drop(obj);
                    return None;
                }
            }
        }
        match self.push_inserted(obj) {
            Ok(()) => {
                self.sink.emit(PoolAuditKind::Returned);
                None
            }
            Err(obj) => {
                self.dropped_overflow.fetch_add(1, Ordering::Relaxed);
                self.sink.emit(PoolAuditKind::OverflowDropped);
                Some(obj)
            }
        }
    }

    /// 插入收口（片锁内一次完成双裁决 + 入槽 + 宇宙认领）：本线程亲和片优先、空
    /// 则轮转（同 [`Self::pop_idle`] 的扫片序——插入方与借出方同线程时热件落在
    /// 同一片）。全片无空位 ⇒ `Err(obj)` 原样交回。
    fn push_inserted(&self, obj: T) -> Result<(), T> {
        let weight = self.weight_of(&obj);
        let shards = self.slots.len();
        let start = self.home_shard();
        for i in 0..shards {
            let idx = (start + i) % shards;
            let mut shard = lock(&self.slots[idx]);
            // 双配额在片锁内读（同 `return_to_shard` 的 resize 收敛论证）；
            // 字节预算 `try_take` 原子 check+act（失败无副作用，逐片重试无害）。
            let idle_cap = self.idle_cap.load(Ordering::Relaxed);
            let max = self.max.load(Ordering::Relaxed);
            if shard.idle.len() < share(idle_cap, shards, idx)
                && shard.alive < share(max, shards, idx)
                && self.budget_admits(weight)
            {
                shard.idle.push(IdleSlot {
                    obj,
                    last_used: Instant::now(),
                });
                shard.alive += 1;
                return Ok(());
            }
        }
        Err(obj)
    }

    /// 句柄析构收口（[`Pooled`] / [`Owned`] 共用，语义逐条见 [`Pooled`] 文档）：
    /// unwind 路径销毁不回池；降级件归还即释放；正常件 `OnReturn` reset → 回 home
    /// 片 → 迁片 → 全池余量零则丢弃（`dropped_overflow` + `OverflowDropped`）。
    ///
    /// 前提：该对象借出时在 `home` 片记过 `borrowed`（`release` / `return_to_shard`
    /// 各回卷一次，禁混账）。
    fn drop_handle(&self, home: usize, degraded: bool, mut obj: T) {
        if std::thread::panicking() {
            // unwind 路径归还（r2d2 #31 防御）：panic 中对象不变量可能已破——
            // 禁 reset、禁回池，销毁 + panic_dropped 留痕（无对应审计 kind）。
            self.release(home, !degraded);
            self.panic_dropped.fetch_add(1, Ordering::Relaxed);
            drop(obj);
            return;
        }
        if degraded {
            // 降级对象：宇宙从未计它——归还即释放，永不进 idle；释放无对应审计
            // kind（契约词汇零改动，降级面经 metrics.degraded 表达）。
            self.release(home, false);
            drop(obj);
            return;
        }
        if self.phase == ResetPhase::OnReturn {
            if let Some(reset) = self.reset.as_ref() {
                // 锁外 + 独占借用；run_reset_hook 兜底——Drop 期 panic 在 unwind
                // 中会 abort 进程，禁；panic（钩子 bug 或重入检出）⇒ 对象状态
                // 不可信 ⇒ 丢弃不入 idle。payload 就地消化（无日志设施，只留
                // `reset_panicked` 计数）；`PoolAuditKind` 无钩子故障词汇 ⇒ 不发
                // 事件（契约词汇零改动）。
                if run_reset_hook(reset.as_ref(), &mut obj).is_err() {
                    self.release(home, true);
                    self.reset_panicked.fetch_add(1, Ordering::Relaxed);
                    drop(obj);
                    return;
                }
            }
        }
        // 放回借出时的片（LIFO 热件常驻）+ 刷新 last_used；片内记账同一临界区。
        if let Some(overflow) = self.return_to_shard(home, obj) {
            self.dropped_overflow.fetch_add(1, Ordering::Relaxed);
            self.sink.emit(PoolAuditKind::OverflowDropped);
            drop(overflow);
        } else {
            self.sink.emit(PoolAuditKind::Returned);
        }
    }

    /// 指标快照（O(shards) = 逐片取锁求和；分片数 ≤ 8、非热路径、零分配）。计数
    /// 下沉进分片后读侧多付几次片锁，换热路径零跨线程原子。
    pub fn metrics(&self) -> ObjectPoolMetrics {
        let mut idle = 0;
        let mut borrowed = 0;
        let mut reused = 0u64;
        for shard in &self.slots {
            let guard = lock(shard);
            idle += guard.idle.len();
            borrowed += guard.borrowed;
            reused += guard.reused;
        }
        let max = self.max.load(Ordering::Relaxed);
        let (byte_budget, idle_bytes) = self.budget_snapshot();
        ObjectPoolMetrics {
            core: PoolMetrics {
                max,
                idle,
                borrowed,
                dropped_overflow: self.dropped_overflow.load(Ordering::Relaxed),
                watermark: borrowed as f64 / max as f64,
            },
            degraded: self.degraded.load(Ordering::Relaxed),
            reused,
            constructed: self.constructed.load(Ordering::Relaxed),
            pruned: self.pruned.load(Ordering::Relaxed),
            reset_panicked: self.reset_panicked.load(Ordering::Relaxed),
            panic_dropped: self.panic_dropped.load(Ordering::Relaxed),
            idle_bytes,
            byte_budget,
        }
    }

    /// 审计环回调 panic 检出数（fail-visible：回调 panic 后被摘除、环照录、池行为
    /// 不变）。与 [`PoolBase::audit_callback_panics`](crate::pool::PoolBase) 同名
    /// 同义——本件共用 `PoolAuditSink`，公开面同款呈现。
    pub fn audit_callback_panics(&self) -> u64 {
        self.sink.audit_callback_panics()
    }

    /// 取走环内审计事件（随取随清；实时回调是持续观测面，本方法是批量对账面）。
    pub fn take_audit_events(&self) -> Vec<PoolAuditEvent> {
        self.sink.take_events()
    }

    /// 亲和号发放次数（诊断读侧，`#[doc(hidden)]`）：用于证明「每 (线程, 池) 只
    /// 领一次号」。**单槽亲和实现下该值会随借还次数增长**——这是本读侧唯一的
    /// 鉴别力来源（借还计数与构造计数在单槽回退时同样正确，抓不到回归）。
    #[doc(hidden)]
    pub fn affinity_issuances(&self) -> usize {
        self.next_affinity.load(Ordering::Relaxed)
    }

    /// 老化口：释放「距上次使用超过 `max_idle_age`」的 idle 对象，返回释放数。
    /// `now` 由调用方给（宿主自己的时钟；**池不起后台线程**）。`Duration::ZERO`
    /// ⇒ 释放全部 idle 对象（特判：不比时间戳直接释放——规避时钟粒度下
    /// 「同一瞬间归还/调用」的 0 龄漏放）。
    pub fn prune_idle(&self, max_idle_age: Duration, now: Instant) -> usize {
        // ZERO 特判：不比时间戳直接释放全部（契约口径「ZERO ⇒ 释放全部」——
        // 规避时钟粒度下「同一瞬间归还/调用」的 0 龄漏放）。
        let zero_sweep = max_idle_age == Duration::ZERO;
        let mut doomed: Vec<T> = Vec::new();
        let mut doomed_bytes = 0usize;
        for shard_mutex in &self.slots {
            let mut shard = lock(shard_mutex);
            let mut i = 0;
            while i < shard.idle.len() {
                // saturating：时钟单调下 last_used 恒 ≤ now；非单调入参如实按
                // 0 龄处理，不 panic。
                let expired = zero_sweep
                    || now.saturating_duration_since(shard.idle[i].last_used) > max_idle_age;
                if expired {
                    let slot = shard.idle.swap_remove(i);
                    doomed_bytes += self.weight_of(&slot.obj);
                    doomed.push(slot.obj);
                    take(&mut shard.alive, 1, "universe alive");
                } else {
                    i += 1;
                }
            }
        }
        let n = doomed.len();
        self.pruned.fetch_add(n as u64, Ordering::Relaxed);
        // 字节额度随件回卷（锁外——ByteBudget 自持锁，锁序干净）。
        self.budget_release(doomed_bytes);
        drop(doomed); // 锁外析构
        n
    }

    /// 老化口（Clock 注入版）：同 [`Self::prune_idle`]，`now` 取自 udf
    /// [`Clock`](skeleton::clock::Clock) 契约——测试注入
    /// [`MockClock`](skeleton::clock::MockClock) 确定性推进（无需真实等待），
    /// 生产传 `SystemClock`。宿主低频老化任务（xray-frame `spawn_idle_pruner`）
    /// 走本口。
    pub fn prune_idle_with_clock(&self, max_idle_age: Duration, clock: &dyn Clock) -> usize {
        self.prune_idle(max_idle_age, clock.now_instant())
    }

    /// 运行时调参（`PoolManaged::resize` 同面）：非法参数入口拒绝（`Err`，状态
    /// 不变）；缩容裁剪 idle（超限丢弃 + `dropped_overflow` 计数）；**缩容不夺
    /// 借出**——借出句柄不受影响，归还时按新边界裁决（本片 idle 满 = 丢弃 +
    /// 计数）。**不改分片数**（构造期自适应定死）；新旧 `max` / `idle_cap` 都按
    /// 片均分（`share`：各片之和恰等于配置值）。
    ///
    /// 合法中间态（doc 钉死）：裁剪逐片串行进行，并发窗口内
    /// `metrics().core.idle` 可能短暂高于新 `idle_cap`——读数如实呈现，终态收敛
    /// （裁剪完成即到新上界），不钳位（同水位线 >1.0 口径）。**收敛靠 `store`
    /// 先于裁剪 + 归还在片锁内读配额**：归还的留存裁决要么读到新值（按新配额
    /// 拒绝入片）、要么其入片被紧随其后的裁剪覆盖，两者都落到新上界之内；配额
    /// 读在片锁外则存在「旧值入片 + 裁剪已过该片」的永久击穿窗口。
    ///
    /// # Errors
    ///
    /// [`PoolResizeError`]：非法参数（拒绝路径零状态变更）。
    pub fn resize(&self, tuning: ObjectPoolTuning) -> Result<(), PoolResizeError> {
        if let Err(e) = validate(tuning.max, tuning.idle_cap) {
            self.sink.emit(PoolAuditKind::ResizeRejected);
            return Err(e);
        }
        self.max.store(tuning.max, Ordering::Relaxed);
        self.idle_cap.store(tuning.idle_cap, Ordering::Relaxed);
        // 缩容裁剪 idle：每片按「本片配额」从最冷端（front）弃起（LIFO 热件常驻）；
        // 锁外析构 + `dropped_overflow` 计数（与 `pruned` 是两个独立计数，不得
        // 混账——resize 丢弃走前者，`prune_idle` 释放走后者）。
        let shards = self.slots.len();
        let mut doomed: Vec<T> = Vec::new();
        let mut doomed_bytes = 0usize;
        for (idx, shard_mutex) in self.slots.iter().enumerate() {
            let cap = share(tuning.idle_cap, shards, idx);
            let mut shard = lock(shard_mutex);
            if shard.idle.len() > cap {
                let excess = shard.idle.len() - cap;
                for slot in shard.idle.drain(0..excess) {
                    doomed_bytes += self.weight_of(&slot.obj);
                    doomed.push(slot.obj);
                }
                take(&mut shard.alive, excess, "universe alive");
            }
        }
        self.dropped_overflow
            .fetch_add(doomed.len() as u64, Ordering::Relaxed);
        // 字节额度随裁剪回卷（与 prune 同款锁外回卷）。
        self.budget_release(doomed_bytes);
        drop(doomed); // 锁外析构
        self.sink.emit(PoolAuditKind::Resized);
        Ok(())
    }

    /// 本线程的亲和片（起始片）：片号每 (线程, 池) 领一次后固定（首个领号者拿
    /// 0 号——单线程用法与旧游标起点一致，行为确定性不随线程调度漂移）。
    ///
    /// 亲和表有 `AFFINITY_SLOTS` 个槽（键 = 池地址、值 = 片号），同线程用多个池
    /// 互不覆盖；表满时覆盖 0 号槽（见 [`AFFINITY`]）。**池地址复用**（旧池析构后
    /// 新池落在同一地址）会让本线程把新池误认成旧池 ⇒ 首个号位偏移（仅影响起点，
    /// 不影响正确性与上界——片号只决定扫片起点）。
    fn home_shard(&self) -> usize {
        let key = self as *const Self as usize;
        AFFINITY.with(|c| {
            let mut table = c.get();
            for entry in &table {
                if entry.0 == key {
                    return entry.1 % self.slots.len();
                }
            }
            let slot = self.next_affinity.fetch_add(1, Ordering::Relaxed);
            // 未命中：先填空槽，无空槽则覆盖 0 号槽。
            // ponytail: 固定 4 槽线性查（热路径 ≤4 次比较，无分配）；同线程持有
            // >4 个池时它们轮流领号（起点漂移，正确性与上界不受影响）。升级路径 =
            // 按使用序淘汰最旧槽，要多一份年龄记账，先不值。
            let target = table
                .iter()
                .position(|entry| entry.0 == EMPTY_AFFINITY_KEY)
                .unwrap_or(0);
            table[target] = (key, slot);
            c.set(table);
            slot % self.slots.len()
        })
    }

    /// 扫片移出：起始片优先、空则轮转扫其余片；锁内一并回卷本片借出数（命中即
    /// 在同一临界区完成记账）——逐片取锁、锁不嵌套。
    fn pop_idle(&self, start: usize) -> Option<(T, usize)> {
        for i in 0..self.slots.len() {
            let idx = (start + i) % self.slots.len();
            let mut shard = lock(&self.slots[idx]);
            if let Some(slot) = shard.idle.pop() {
                shard.borrowed += 1;
                shard.reused += 1;
                // 借出 = 不再驻留：字节回卷（与归还的 `try_take` 对称配平）。
                self.budget_release(self.weight_of(&slot.obj));
                return Some((slot.obj, idx));
            }
        }
        None
    }

    /// 宇宙配额认领（从 `start` 片起扫、逐片取锁不嵌套）：本片 `alive` 未达**片
    /// 配额**（[`share`]）才 +1 并返回片号——并发借出下的硬上界保证（各片独立、
    /// 总和 ≤ `max`）。全片满 ⇒ `None`（降级直构，对象不入宇宙）。
    ///
    /// `max` **在片锁内读**（配额快照不许跨过临界区）：`resize` 先 `store` 新值、
    /// 再逐片加锁 ⇒ 若本片加锁发生在 `store` 之后，读到的一定是新值；陈旧快照
    /// 窗口被收到临界区内。注意 `alive` **不随缩 `max` 回溯裁剪**（缩容不夺借出，
    /// 见 [`ObjectPool::resize`]）：已认领的存活数可短时高于新 `max`（合法中间态，
    /// 随归还在新配额下收敛）。
    fn claim_growth(&self, start: usize) -> Option<usize> {
        let shards = self.slots.len();
        for i in 0..shards {
            let idx = (start + i) % shards;
            let mut shard = lock(&self.slots[idx]);
            let max = self.max.load(Ordering::Relaxed);
            if shard.alive < share(max, shards, idx) {
                shard.alive += 1;
                return Some(idx);
            }
        }
        None
    }

    /// 配额回滚（工厂 panic，尚未交出对象）：只退宇宙存活数——`borrowed` 从未
    /// 记（`record_borrow` 只在工厂返回后调用），不得混账。
    fn unclaim_growth(&self, home: usize) {
        let mut shard = lock(&self.slots[home]);
        take(&mut shard.alive, 1, "universe alive");
    }

    /// 归还收口（锁 home 片一次完成记账 + 留存裁决）：借出数回卷；home 片 idle 未
    /// 达**片配额**（[`share`]）⇒ 刷新时间戳入片、返回 `None`。
    ///
    /// **迁片只发生在「归还 + home 片 idle 满」这一种情形**：轮转找仍有 idle 余量
    /// 的片迁入留存（`alive` 随件迁移、宇宙账面守恒）。按片均分 + 线程亲和下，
    /// 单线程只回自己那片，若死守本片配额则 `idle_cap / shards` 以外的额度全被
    /// 浪费（实测白丢 3/4 容量）。**迁入先记账再退 home**——账面永不低估实际存活
    /// 数，宇宙硬上界不会被击穿（代价：迁片路径多一次片锁）。全池余量为零 ⇒ 宇宙
    /// 存活数回卷、对象交回调用方锁外析构（`dropped_overflow` 由调用方记账）。
    ///
    /// 配额（`idle_cap` / `max`）**一律在目标片锁内读**：`resize` 先 `store` 新值、
    /// 再逐片加锁裁剪 ⇒ 归还要么读到新值（按新配额裁决），要么它的 push 会被紧随
    /// 其后的那次裁剪覆盖。锁外的陈旧快照没有这个收敛保证——缩容并发归还时能把
    /// idle 永久顶在新上界之外（无任何再裁剪路径）。
    fn return_to_shard(&self, home: usize, obj: T) -> Option<T> {
        let weight = self.weight_of(&obj);
        let shards = self.slots.len();
        let mut shard = lock(&self.slots[home]);
        take(&mut shard.borrowed, 1, "borrowed");
        let idle_cap = self.idle_cap.load(Ordering::Relaxed);
        // 字节裁决与片配额同一临界区：`try_take` 原子 check+act（失败无副作用
        // ——额度从未占住，丢弃路径无需回卷）。
        if shard.idle.len() < share(idle_cap, shards, home) && self.budget_admits(weight) {
            shard.idle.push(IdleSlot {
                obj,
                last_used: Instant::now(),
            });
            return None;
        }
        drop(shard); // 迁片前先放锁：逐片取锁、锁不嵌套
        for i in 1..shards {
            let idx = (home + i) % shards;
            let mut candidate = lock(&self.slots[idx]);
            // 迁入判定同样在候选片锁内读配额（同一收敛论证）。
            let idle_cap = self.idle_cap.load(Ordering::Relaxed);
            let max = self.max.load(Ordering::Relaxed);
            if candidate.idle.len() >= share(idle_cap, shards, idx)
                || candidate.alive >= share(max, shards, idx)
                || !self.budget_admits(weight)
            {
                continue;
            }
            candidate.idle.push(IdleSlot {
                obj,
                last_used: Instant::now(),
            });
            candidate.alive += 1;
            drop(candidate);
            take(&mut lock(&self.slots[home]).alive, 1, "universe alive");
            return None;
        }
        take(&mut lock(&self.slots[home]).alive, 1, "universe alive");
        Some(obj)
    }

    /// 对象离开池（销毁 / `detach` 移交 / 配额回滚）：锁 home 片回卷借出数；宇宙
    /// 内对象连同宇宙存活数一并回卷（降级对象不入宇宙——两条记账路径，不得混账）。
    fn release(&self, home: usize, in_universe: bool) {
        let mut shard = lock(&self.slots[home]);
        take(&mut shard.borrowed, 1, "borrowed");
        if in_universe {
            take(&mut shard.alive, 1, "universe alive");
        }
    }

    /// 借出记账（工厂返回后调用）：`borrowed` 只数**真实交出**的对象——工厂 panic
    /// 的在途配额走 [`Self::release`] 回滚，不虚增借出数。
    fn record_borrow(&self, home: usize) {
        let mut shard = lock(&self.slots[home]);
        shard.borrowed += 1;
    }

    /// 单件字节计量（未启用字节配额的池恒 0——`weight`/`budget` 成对出现，
    /// `budget: None` ⇒ `weight: None`）。调用点在片锁内；钩子体内禁 acquire
    /// 同一池（见 [`WeightHook`]）。
    fn weight_of(&self, obj: &T) -> usize {
        self.weight.as_ref().map_or(0, |w| w(obj))
    }

    /// 向字节预算认领 `n`（未启用恒 true）。原子 check+act——失败即容量因
    /// 丢弃（调用方计 `dropped_overflow`），无「认领一半」形态。
    fn budget_admits(&self, n: usize) -> bool {
        self.budget.as_ref().is_none_or(|b| b.try_take(n))
    }

    /// 字节回卷（借出 / prune / resize 裁剪的对称面；未启用恒 0 字节，短路）。
    fn budget_release(&self, n: usize) {
        if let Some(b) = self.budget.as_ref() {
            b.give_back(n);
        }
    }

    /// 池字节读数（metrics 用）：`(byte_budget, idle_bytes)`；未启用 `(0, 0)`。
    fn budget_snapshot(&self) -> (usize, usize) {
        match self.budget.as_ref() {
            Some(b) => (b.total(), b.held()),
            None => (0, 0),
        }
    }
}

impl<T> PoolManaged for ObjectPool<T> {
    type Metrics = ObjectPoolMetrics;
    type Tuning = ObjectPoolTuning;

    fn resize(&self, tuning: ObjectPoolTuning) -> Result<(), PoolResizeError> {
        ObjectPool::resize(self, tuning)
    }

    fn metrics(&self) -> ObjectPoolMetrics {
        ObjectPool::metrics(self)
    }

    fn subscribe_audit(&self, f: impl FnMut(&PoolAuditEvent) + Send + 'static) -> PoolAuditTicket {
        self.sink.subscribe_audit(AUDIT_RING_DEFAULT_CAP, f)
    }
}

/// 借出句柄（RAII）：[`Deref`](std::ops::Deref)/[`DerefMut`](std::ops::DerefMut)
/// 到 `T`（**改参数即复用**的主能力）；**Drop 即归还**（禁手动还）。
///
/// 归还语义（正常对象）：`OnReturn` 相位先锁外调 reset（panic ⇒ 丢弃不入
/// idle，禁 abort，记 `reset_panicked`——与容量因的 `dropped_overflow` 不混账），
/// 再判保留——**优先回 home 片**（借出时的片，LIFO 热件常驻）；
/// home 片 idle 未达**片配额**（`idle_cap` 按片均分、**在片锁内读**：与并发
/// [`ObjectPool::resize`] 无陈旧快照窗口）即入片 + `audit Returned`；
/// home 片满则**迁入有余量的片**（`alive` 随迁、账面守恒）；全池余量为零才丢弃 +
/// `dropped_overflow` +1 + `audit OverflowDropped`。降级对象归还直接释放（不进
/// idle、不计宇宙、无审计事件——降级面经 `metrics.degraded` 表达）。
///
/// **panic unwind 中析构**（r2d2 #31 防御）：不调 reset、不回池，对象销毁 +
/// `panic_dropped` 留痕（语义 11，见模块文档「unwind 感知归还」节）。跨
/// `'static` 边界（任务闭包/线程）：本句柄借用池、非 `'static`，带不进
/// `'static` 闭包——要归还就换 [`ObjectPool::acquire_owned`]（句柄自带 `Arc`，
/// 见 [`Owned`]），不要复用了就 [`Pooled::detach`] 永久移交。
pub struct Pooled<'p, T> {
    pool: &'p ObjectPool<T>,
    /// 借出时的片（归还亲和：LIFO 热件常驻热缓存）。
    home: usize,
    slot: Option<T>,
    degraded: bool,
}

impl<'p, T> Pooled<'p, T> {
    /// 交出所有权：对象彻底脱离池生命周期，调用方最后一个持有者放手即析构
    /// （池不钉住该对象，宇宙配额随件回吐）。
    pub fn detach(mut self) -> T {
        let obj = self
            .slot
            .take()
            .expect("pooled slot empty before detach (bookkeeping bug)");
        self.pool.release(self.home, !self.degraded);
        // 所有权已移交，归还路径不再运行（禁再回池）。
        std::mem::forget(self);
        obj
    }
}

impl<T> std::ops::Deref for Pooled<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        self.slot
            .as_ref()
            .expect("pooled slot empty before drop (bookkeeping bug)")
    }
}

impl<T> std::ops::DerefMut for Pooled<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        self.slot
            .as_mut()
            .expect("pooled slot empty before drop (bookkeeping bug)")
    }
}

impl<T> Drop for Pooled<'_, T> {
    fn drop(&mut self) {
        let obj = self
            .slot
            .take()
            .expect("pooled slot empty on drop (bookkeeping bug)");
        self.pool.drop_handle(self.home, self.degraded, obj);
    }
}

/// `'static` 借出句柄（[`ObjectPool::acquire_owned`] 产出）：与 [`Pooled`] **同语义、
/// 同记账、同 Drop 路径**（共用 `drop_handle`），差别只在**自持
/// `Arc<ObjectPool>` 而非借用** ⇒ 可进 `tokio::spawn` 闭包 / `Bytes::from_owner`
/// 的所有者等 `'static` 位（这些位置的归还点是任意线程的引用计数归零，不可能借用
/// 池）。代价 = 每轮一次 `Arc` 增删。
///
/// **Drop 语义与 [`Pooled`] 逐条相同**：`OnReturn` 相位先锁外 reset（panic ⇒ 丢弃，
/// `reset_panicked` 留痕，禁 abort）→ 优先回 home 片 → 满则迁片 → 全池余量零才
/// 丢弃（`dropped_overflow`）；panic unwind 中析构不 reset 不回池
/// （`panic_dropped`）；降级件归还即释放。**池不再先于句柄死**——`Arc` 让池活到
/// 最后一个句柄放手，句柄永不悬垂（借用形态里这条性质反过来由编译期保证）。
pub struct Owned<T> {
    pool: Arc<ObjectPool<T>>,
    /// 借出时的片（归还亲和；跨线程 Drop 也回这一片）。
    home: usize,
    slot: Option<T>,
    degraded: bool,
}

impl<T> Owned<T> {
    /// 交出所有权（同 [`Pooled::detach`]）：宇宙配额随件回吐，对象彻底脱离池生命
    /// 周期。`slot` 置空后本句柄的 `Drop` 只释放那份 `Arc`（不重复归还）。
    pub fn detach(mut self) -> T {
        let obj = self
            .slot
            .take()
            .expect("owned slot empty before detach (bookkeeping bug)");
        self.pool.release(self.home, !self.degraded);
        obj
    }
}

impl<T> std::ops::Deref for Owned<T> {
    type Target = T;

    fn deref(&self) -> &T {
        self.slot
            .as_ref()
            .expect("owned slot empty before drop (bookkeeping bug)")
    }
}

impl<T> std::ops::DerefMut for Owned<T> {
    fn deref_mut(&mut self) -> &mut T {
        self.slot
            .as_mut()
            .expect("owned slot empty before drop (bookkeeping bug)")
    }
}

impl<T> Drop for Owned<T> {
    fn drop(&mut self) {
        // detach 后 slot 已空：这里只让那份 `Arc` 正常释放（禁 mem::forget——那会
        // 漏掉一次强引用计数，池永不析构）。
        if let Some(obj) = self.slot.take() {
            self.pool.drop_handle(self.home, self.degraded, obj);
        }
    }
}
