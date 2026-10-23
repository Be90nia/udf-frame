//! UUIDv7 确定性变体：时间有序 id 生成（RFC 9562 §5.7 布局逐位对表）。
//!
//! 常驻零依赖件：纯 std，无 feature 门。时间面复用 [`skeleton::clock`] 挂钟面
//!（`now_system` → 48bit `unix_ts_ms`），机器指纹由采用方注入（推荐
//! [`std_ext::hash::stable64`] 派生，本域不绑定派生方式）。
//!
//! # 布局（RFC 9562 §5.7，128bit 大端）
//!
//! ```text
//! 48bit unix_ts_ms | 4bit ver=0111 | 12bit rand_a | 2bit var=10 | 62bit rand_b
//! ```
//!
//! 确定性变体映射（拍板）：rand_a = 同毫秒单调 counter（基址=指纹固定派
//! 生），rand_b = 机器指纹低 62 位。随机 id 破坏确定性测试与回放——本域
//! 契约：**同指纹 + 同钟时间线 + 同调用序 → 逐位同 id 序列**。
//!
//! # 排序保证（契约声明）
//!
//! - 同指纹（全部句柄）：[`IdGen::gen`] 数值**严格递增**（线性化点=锁内
//!   装配）；同毫秒支出上限 = 4096 − 基址（初值毫秒差 1：GenState 初始
//!   `last_ms=0` 与挂钟 ms=0 重合，首调走同毫秒臂、首枚=基址+1，该毫秒
//!   配额 4095 − 基址；基址=0xFFF 时初值毫秒首调即配额 panic）；
//! - 跨指纹：毫秒级时间序（48bit 时间戳），同毫秒内无全局序；
//! - 时钟回拨：停走不倒退（时间戳复用 `last_ms`，counter 前进保序）。
//!
//! # 姿态（骨架哲学 fail-closed/显式/禁静默；拍板点）
//!
//! - **同毫秒配额耗尽 = fail-closed panic**（拍板点）：12bit counter 上限
//!   4096/ms。生态先例（uuid7 crate V7Generator / ulid monotonic）选择
//!   「等待下一毫秒」——本域拒绝：等待与 MockClock 冻结语义/零自旋纪律冲
//!   突（冻结钟上等待=活锁），且 `gen()` 无错误通道；持续率超 4M id/s 的
//!   采用方应分片指纹；
//! - **counter 基址 = 指纹固定派生**（拍板点）：生态为防 id 预测取随机基
//!   址；确定性变体反其道——`base = 指纹 >> 13 & 0xFFF` 纯函数，回放可复
//!   现优先于不可预测性（id 非机密值面）；
//! - **同指纹单权威注册表**（拍板点）：进程级 `OnceLock<Mutex<HashMap>>`
//!   按指纹共享状态，同指纹 [`IdGen::new`] 复用同一权威——跨句柄严格递增
//!   、零撞 id（生态文档明示多生成器同域可能撞 id，ulid monotonic；本域把
//!   该失败类结构上消灭）。注册表项进程内常驻（指纹数量级≈进程内域数）；
//!   后建句柄的 clock 参数只作本句柄读取面，权威状态由实际 gen 序列喂养，
//!   停走 armor 兜任意钟错乱；
//! - **指纹取低 62 位**（拍板点）：高 2 位被 var 位占用——仅高 2 位互异的
//!   指纹在 rand_b 面同值，自选指纹时避免（stable64 散列输出天然分散）。
//!
//! # 坑面 armor（每坑一测，见 `tests/id.rs`；坑面对表 uuid7 crate
//! V7Generator 与 ulid 3.0 monotonic）
//!
//! | 坑 | 来源 | 堵法 | 测试名 |
//! |---|---|---|---|
//! | 同毫秒 12bit 配额（4096）耗尽：静默借位或等待活锁 | uuid7 V7Generator（等待下一 ms） | fail-closed 带因 panic | `counter_overflow_fails_closed` |
//! | 挂钟回拨（NTP）破序 | clock armor：SystemTime 非单调 | 停走不倒退：now<last 复用 last_ms | `clock_rollback_stalls_without_regression` |
//! | counter 初值随机基址破坏确定性回放 | uuid7/ulid 防预测随机基址 | 基址=指纹派生（固定纯函数） | `deterministic_replay_same_timeline_same_sequence` |
//! | 同指纹多实例独立 counter 同毫秒撞 id | ulid monotonic 文档明示的多生成器风险 | 进程级单权威注册表 | `same_fingerprint_handles_share_authority` |
//! | 版本/变体位写错（自造变体） | RFC 9562 §5.7 ver=0111/var=10 | 位面常量集中装配 + 布局逐位测试 | `layout_matches_rfc9562` |
//! | u128 序列化端序漂移 | RFC 9562 §5.1 大端布局 | `{:032x}` 即大端 hex，标准连字形 | `string_form_is_rfc_hyphenated` |
//! | 48bit 时间戳静默回绕（≈公元 10889 年） | RFC 9562 字段宽即上限 | 越界 fail-closed panic | `ts_overflow_fails_closed` |
//! | epoch 前挂钟 | SystemTime 可表示 1970 前 | `duration_since` 显式 Err → fail-closed | `pre_epoch_clock_fails_closed` |
//!
//! # Examples
//!
//! MockClock 确定性时间线：同毫秒单调、跨毫秒时间序、标准连字形式。
//!
//! ```
//! use std::time::Duration;
//! use std::sync::Arc;
//!
//! use skeleton::clock::MockClock;
//! use std_ext::id::IdGen;
//!
//! let clock = Arc::new(MockClock::new());
//! clock.advance(Duration::from_millis(1));
//! let gen = IdGen::new(0xC0FFEE, clock.clone());
//!
//! let a = gen.gen();
//! let b = gen.gen();
//! assert!(b > a, "同毫秒：counter 前进，严格递增");
//!
//! clock.advance(Duration::from_millis(1));
//! let c = gen.gen();
//! assert!(c > b, "跨毫秒：时间序");
//!
//! assert_eq!((a.as_u128() >> 76) & 0xF, 7, "RFC 9562 version 7");
//! let s = a.to_string();
//! assert_eq!(s.len(), 36, "8-4-4-4-12 标准连字形：{s}");
//! ```
//!
//! # 性能锚（2026-10-17，release/std Instant 粗测，详见
//! reports/2026-10-17-id.md；复跑载体 examples/id_multidim.rs）
//!
//! - `gen()` 恒零分配（[`Id`] 值面 Copy 无堆；counting allocator 净活断言
//!   ==0）；
//! - 非竞争 `gen()`：SystemClock 端到端 ~37ns（now_system 直通 ~16ns，净
//!   算术+权威锁 ~21ns 差值口径）；MockClock 臂（替身钟锁+权威锁双锁）
//!   ~34-44ns（跨轮方差）；
//! - 批量梯度 100/1k/10k（冻结钟 counter 面）~34-58ns/id（跨机粗测噪声带，1k 点可达 ~58），无系统性规模效应；
//! - `to_string()`（[`Display`](std::fmt::Display) 字符串面）~590ns（两次
//!   分配即用即弃净零，展示面非热路径）；
//! - 延迟分布（per-call Instant 计时含钟开销双担，绝对值偏高看形态）：
//!   p50/p99 ~100ns，p99.9 ~200-400ns；
//! - 竞争单点（冻结钟 4000 ids，粗测含线程 spawn 摊销，仅定性）：1 线程
//!   ~86ns/id vs 2 线程 ~90ns/id 无悬崖——单 Mutex 权威竞争面在锁域不在
//!   此域（契约明示）；
//! - ④⑦ 收窄：无运行时调参面 / 单权威 cell（非热点计数阵列）。
//!
//! log-embed：无埋点决策点——`gen` 无拒绝/等待路径（配额耗尽=fail-closed
//! panic），装配是纯计算（埋点纪律：禁埋纯计算路径）。

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, LazyLock, Mutex, PoisonError};
use std::time::UNIX_EPOCH;

use skeleton::clock::Clock;

/// 48bit 时间戳字段上界（RFC 9562 §5.7 字段宽即上限）。
const TS_MAX: u64 = (1 << 48) - 1;
/// 12bit 同毫秒 counter 上限（rand_a 字段宽）。
const COUNTER_MAX: u16 = (1 << 12) - 1;
/// 62bit rand_b 掩码（高 2 位为 var 位）。
const MASK62: u64 = (1 << 62) - 1;

/// 时间有序 id 值类型（UUIDv7 确定性变体）：数值面（`u128`）承载排序与
/// 持久化往返（[`Id::as_u128`]/[`Id::from_u128`]），字符串面 = RFC 9562
/// §5.1 标准连字形（[`std::fmt::Display`] 出 / [`std::str::FromStr`] 入，
/// hex 位大小写不敏感）。值语义（`Copy`），比较/散列按数值面全序。
///
/// # Examples
///
/// ```
/// use std_ext::id::Id;
///
/// // 版本/变体 nibble 落位由布局保证（14 位='7'，19 位∈8..b）。
/// let id = Id::from_u128((7u128 << 76) | (0b10u128 << 62));
/// let s = id.to_string();
/// assert_eq!(&s[14..15], "7");
/// assert!(matches!(s.as_bytes()[19], b'8'..=b'b'));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Id(u128);

impl Id {
    /// 数值面（持久化编码/位面诊断用；位布局见模块文档）。
    #[must_use]
    pub const fn as_u128(self) -> u128 {
        self.0
    }

    /// 自数值面装配（回放/跨进程迁移侧用；形状不校验——数值面是透明
    /// 载体，位语义归消费方）。
    #[must_use]
    pub const fn from_u128(value: u128) -> Self {
        Self(value)
    }
}

impl std::fmt::Display for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let hex = format!("{:032x}", self.0);
        write!(
            f,
            "{}-{}-{}-{}-{}",
            &hex[..8],
            &hex[8..12],
            &hex[12..16],
            &hex[16..20],
            &hex[20..32]
        )
    }
}

/// 连字形解析错误（两态显式，禁静默回退）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdParseError {
    /// 非 8-4-4-4-12 连字形（段数/段长不符）。
    Shape,
    /// hex 位非法。
    Hex,
}

impl std::fmt::Display for IdParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Shape => write!(f, "expected 8-4-4-4-12 hyphenated hex id"),
            Self::Hex => write!(f, "invalid hex digit in id"),
        }
    }
}

impl std::error::Error for IdParseError {}

impl std::str::FromStr for Id {
    type Err = IdParseError;

    /// 解析 RFC 9562 §5.1 标准连字形（hex 位大小写不敏感；段数/段长必须
    /// 逐位对表 8-4-4-4-12，否则 [`IdParseError::Shape`]）。
    ///
    /// # Examples
    ///
    /// ```
    /// use std::str::FromStr;
    ///
    /// use std_ext::id::{Id, IdParseError};
    ///
    /// let id = Id::from_str("00000000-0000-7000-8000-000000c0ffee").unwrap();
    /// assert_eq!(id.as_u128() >> 76, 7);
    /// assert_eq!("abc".parse::<Id>(), Err(IdParseError::Shape));
    /// ```
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let segs = s.split('-').collect::<Vec<_>>();
        let shape_ok = segs.len() == 5
            && segs[0].len() == 8
            && segs[1].len() == 4
            && segs[2].len() == 4
            && segs[3].len() == 4
            && segs[4].len() == 12;
        if !shape_ok {
            return Err(IdParseError::Shape);
        }
        let mut value = 0u128;
        for seg in &segs {
            for c in seg.chars() {
                let digit = c.to_digit(16).ok_or(IdParseError::Hex)?;
                value = (value << 4) | u128::from(digit);
            }
        }
        Ok(Self(value))
    }
}

/// 时间有序 id 生成器（UUIDv7 确定性变体）：同一指纹的全部句柄共享单权威。
///
/// 构造经 [`IdGen::new`]；[`Clone`] 廉价（Arc 句柄），跨线程安全
///（`Send + Sync`）。句柄不可手搓，只能经注册表获得——同指纹撞 id 的失败
/// 类在结构上不存在。
#[derive(Clone)]
pub struct IdGen {
    fingerprint: u64,
    clock: Arc<dyn Clock>,
    state: Arc<Mutex<GenState>>,
}

/// 单权威可变状态：线性化点 = `gen` 锁内装配前一刻。
struct GenState {
    /// 线性化时间戳：停走语义下可滞后于真实钟（回拨 armor）。
    last_ms: u64,
    /// 当前毫秒 counter（rand_a）；新毫秒重置为 `base`。
    counter: u16,
    /// 每毫秒 counter 重置基址 = 指纹固定派生（确定性拍板）。
    base: u16,
}

impl GenState {
    /// 构造态 `last_ms=0/counter=base`：epoch 当毫秒（ms=0，仅 MockClock
    /// 零偏移可达）首个 id 的 counter 为 base+1（无「未用」标记位，语义仍
    /// 单调确定；测试从 ms=1 起步避开）。
    fn new(fingerprint: u64) -> Self {
        let base = base_of(fingerprint);
        Self {
            last_ms: 0,
            counter: base,
            base,
        }
    }
}

/// 进程级指纹注册表：同指纹单权威（撞 id 失败类的结构性消灭）。
static REGISTRY: LazyLock<Mutex<HashMap<u64, Arc<Mutex<GenState>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

impl IdGen {
    /// 新生成器：同指纹复用进程内既有权威（跨句柄严格递增、零撞 id）。
    ///
    /// `clock` 只作本句柄读取面（推荐 [`skeleton::clock::SystemClock`] 生产 /
    /// [`skeleton::clock::MockClock`] 测试）；权威时间线由实际 gen 序列喂养，
    /// 任意钟错乱被停走 armor 兜住。
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    ///
    /// use skeleton::clock::SystemClock;
    /// use std_ext::id::IdGen;
    ///
    /// let gen = IdGen::new(0xBADC0DE, Arc::new(SystemClock));
    /// let id = gen.gen();
    /// assert_eq!((id.as_u128() >> 76) & 0xF, 7);
    /// ```
    #[must_use]
    pub fn new(fingerprint: u64, clock: Arc<dyn Clock>) -> Self {
        let mut reg = REGISTRY.lock().unwrap_or_else(PoisonError::into_inner);
        let state = reg
            .entry(fingerprint)
            .or_insert_with(|| Arc::new(Mutex::new(GenState::new(fingerprint))))
            .clone();
        Self {
            fingerprint,
            clock,
            state,
        }
    }

    /// 生成下一个 id（[`Id`] 值，恒零分配）。
    ///
    /// 排序保证与 fail-closed 语义见模块文档；同毫秒配额（4096−基址）耗尽
    /// 时带因 panic（拒绝静默借位/拒绝等待活锁）。
    #[must_use]
    pub fn gen(&self) -> Id {
        let now_ms = unix_ms(&*self.clock);
        let mut s = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if now_ms > s.last_ms {
            // 新毫秒：时间戳前进，counter 重置基址。
            s.last_ms = now_ms;
            s.counter = s.base;
        } else {
            // 同毫秒或挂钟回拨（now_ms <= last_ms）：停走不倒退——时间戳
            // 复用 last_ms，counter 前进保严格递增（回拨 armor）。
            if s.counter == COUNTER_MAX {
                panic!(
                    "id 域同毫秒序列配额耗尽（12bit counter 上限 4096/ms）：fingerprint={:#018x} \
                     停走毫秒 ms={} 已配满——fail-closed 拒绝借位与等待活锁；持续率超 4M id/s 请分片指纹",
                    self.fingerprint, s.last_ms
                );
            }
            s.counter += 1;
        }
        Id(assemble(s.last_ms, s.counter, self.fingerprint))
    }

    /// 本句柄的机器指纹（诊断/分片对账用）。
    #[must_use]
    pub fn fingerprint(&self) -> u64 {
        self.fingerprint
    }
}

impl fmt::Debug for IdGen {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IdGen")
            .field("fingerprint", &self.fingerprint)
            .finish_non_exhaustive()
    }
}

/// counter 基址 = 指纹固定派生（确定性拍板；移位 13 打散与 rand_b 低 12
/// 位的平凡相关，纯函数无安全语义）。
fn base_of(fingerprint: u64) -> u16 {
    ((fingerprint >> 13) & 0x0FFF) as u16
}

/// 挂钟毫秒（48bit 字段语义，越界 fail-closed）。
fn unix_ms(clock: &dyn Clock) -> u64 {
    let since_epoch = clock.now_system().duration_since(UNIX_EPOCH).expect(
        "id v7 时间戳要求挂钟在 UNIX_EPOCH 之后（epoch 前无法编码 48bit unix_ts_ms，fail-closed）",
    );
    let ms = u64::try_from(since_epoch.as_millis())
        .expect("unix 毫秒超出 u64 表示（平台钟异常，fail-closed）");
    if ms > TS_MAX {
        panic!(
            "id v7 时间戳超出 48bit unix_ts_ms 字段（RFC 9562 布局上限，约公元 10889 年）：\
             ms={ms}——fail-closed 拒绝静默回绕破序"
        );
    }
    ms
}

/// RFC 9562 §5.7 位面装配（大端；ver=0111/var=10 常量集中在此，禁散落）。
fn assemble(ms: u64, counter: u16, fingerprint: u64) -> u128 {
    (u128::from(ms) << 80)
        | (0x7u128 << 76)
        | (u128::from(counter) << 64)
        | (0b10u128 << 62)
        | u128::from(fingerprint & MASK62)
}
