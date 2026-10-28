//! 时间源契约：单调测量面与挂钟展示面的双口分流。
//!
//! 自 `skeleton-infra::clock` 下沉（批 1 契约真下沉，2026-10-18；ADR 0001）：`Clock` 是被 scheduler/ratelimit/retry/
//! cache/id 多域消费的核心契约，住契约层与实现同床的架构债在此偿还。实现件
//! [`SystemClock`]/[`MockClock`] 随迁（纯 std 零 feature 门）；时长人读格式化
//! 与 RFC 3339 是行为件，住在 `std_ext::clock`（批 5-2 起，原 `skeleton_infra::clock`）。
//!
//! 装甲 #19（Instant vs SystemTime 混用）的坑面防线：[`Clock`] 双方法名强制
//! 分流——`now_instant`（单调测量/退避/超时）与 `now_system`（挂钟展示/账本
//! 时间戳），调用点名字即语义，禁隐式择一。
//!
//! 坑面 armor（std 文档逐条对表，tests 钉死）：
//!
//! - Instant 单调**非稳定**（tick 不等长、可前跳/时胀，永不后退）→ 只做相对
//!   测量，禁当挂钟展示面；
//! - 系统挂起是否计时不定（跨平台跨 Rust 版本漂移）→ 同上，测量语义禁跨挂起；
//! - `Instant::duration_since` 逆序饱和化为零，掩盖参数颠倒（std 自认
//!   "obscures programming errors"，未来版本或恢复 panic）→ 差值检测用
//!   `checked_duration_since` 显式化；
//! - `Instant + 巨大 Duration` 在 macOS（CLOCK_UPTIME_RAW）panic（std 文档
//!   官例）→ 实现内部恒 `checked_add`，失败 fail-closed 带因 panic；
//! - SystemTime **非单调**（NTP 回拨）且不计闰秒 → 逆序差值恒显式
//!   `Err(SystemTimeError)`（std 契约），测耗时必须走 `now_instant` 面；
//! - 仿钟零自动前进（tokio auto-advance 直跳超时的教训）→ 只 `MockClock::advance`
//!   手动推进，后退在类型层面不可表达（`Duration` 无符号）。

use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant, SystemTime};

/// 时间源抽象：单调测量面与挂钟展示面的双口分流（装甲 #19）。
///
/// 采用方经依赖注入持有（`Arc<dyn Clock>`）：生产装 [`SystemClock`]，测试注入
/// [`MockClock`] 得到逐纳秒可断言的确定性时间线（tokio pause 同款形状：时间
/// 只在测试显式推进时前进）。
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use skeleton::clock::{Clock, SystemClock};
///
/// /// 测量一段工作的真实耗时（单调面；差值用 checked 显式化，禁饱和化掩盖）。
/// fn measure_elapsed(clock: &dyn Clock, work: impl FnOnce()) -> Duration {
///     let start = clock.now_instant();
///     work();
///     clock
///         .now_instant()
///         .checked_duration_since(start)
///         .expect("单调钟上 later >= earlier 恒成立")
/// }
///
/// let elapsed = measure_elapsed(&SystemClock, || ());
/// assert!(elapsed < Duration::from_secs(1));
/// ```
pub trait Clock: Send + Sync {
    /// 单调测量面： benchmark/退避/超时/耗时断言用。
    ///
    /// 语义边界（std 文档）：单调非递减但不保证均匀（可前跳）；跨挂起是否
    /// 计时依平台而定。返回值只与同源 Instant 做相对比较。
    fn now_instant(&self) -> Instant;

    /// 挂钟展示面：账本时间戳/对外协议时间用。
    ///
    /// 语义边界（std 文档）：非单调（可回拨）、不计闰秒、精度依平台（Windows
    /// 100ns）。逆序差值在 std 层就是显式 `Err`，禁用它测耗时。
    fn now_system(&self) -> SystemTime;
}

/// 真实系统时钟：std 直通（[`Instant::now`] / [`SystemTime::now`]）。
///
/// # Examples
///
/// ```
/// use skeleton::clock::{Clock, SystemClock};
///
/// let clock = SystemClock;
/// let _deadline = clock.now_instant();
/// let _stamp = clock.now_system();
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_instant(&self) -> Instant {
        Instant::now()
    }

    fn now_system(&self) -> SystemTime {
        SystemTime::now()
    }
}

/// 仿钟（测试替身）：虚拟时间只在 `advance` 显式推进，两次读取之间严格冻结。
///
/// 确定性语义：
///
/// - `now_system` = `UNIX_EPOCH + 虚拟偏移`——挂钟面**跨运行逐位确定**（真实
///   `Instant` 锚不可合成，故 `now_instant` 面锚定构造瞬间的真实时刻，只做
///   同实例内相对比较；与真实钟/他实例的 Instant 混比无意义）；
/// - 后退不可表达（`advance` 入参 `Duration` 无符号）；累计推进溢出
///   `Duration` 表示范围 → fail-closed 带因 panic（测试写错了就炸，不静默）；
/// - 读取成本 ~11-13ns/次（非竞争 `Mutex`+`checked_add`，2026-10-16 粗测，
///   详见time 域性能锚（本地留存，未入库）；复跑载体 examples/time_multidim.rs，
///   2026-10-17 复验 11.3ns）——测试替身语义，非热路径件。注意 TtlCache
///   路径净税 ~22ns = 本读 + 过期判定面之和（cache_multidim ②′），勿以
///   本锚核缓存路径成本。
///
/// # Examples
///
/// 确定性时间线：零 sleep，逐毫秒精确断言（真实钟做不到）。
///
/// ```
/// use std::time::Duration;
/// use skeleton::clock::{Clock, MockClock};
///
/// let clock = MockClock::new();
/// let before = clock.now_instant();
/// clock.advance(Duration::from_millis(150)); // 替代 thread::sleep：确定性推进
/// let waited = clock
///     .now_instant()
///     .checked_duration_since(before)
///     .expect("仿钟单调，earlier < later 恒成立");
/// assert_eq!(waited, Duration::from_millis(150)); // 逐毫秒断言
/// assert_eq!(clock.unix_offset(), Duration::from_millis(150)); // 挂钟面同步推进
/// ```
#[derive(Debug)]
pub struct MockClock {
    state: Mutex<MockState>,
}

#[derive(Debug)]
struct MockState {
    /// 构造瞬间的真实单调锚：虚拟时间线在其上平移。
    anchor: Instant,
    /// 虚拟偏移：`now_system = UNIX_EPOCH + offset`，`now_instant = anchor + offset`。
    offset: Duration,
}

impl MockClock {
    /// 新仿钟：起点 = UNIX_EPOCH（`unix_offset() == Duration::ZERO`）。
    pub fn new() -> Self {
        Self {
            state: Mutex::new(MockState {
                anchor: Instant::now(),
                offset: Duration::ZERO,
            }),
        }
    }

    /// 虚拟时间前进 `delta`（只进不退；累计溢出 fail-closed panic）。
    pub fn advance(&self, delta: Duration) {
        let mut s = self.lock();
        s.offset = s
            .offset
            .checked_add(delta)
            .expect("MockClock 虚拟时间溢出：advance 累计超出 Duration 表示范围");
    }

    /// 当前虚拟挂钟相对 `UNIX_EPOCH` 的偏移（断言用读数）。
    pub fn unix_offset(&self) -> Duration {
        self.lock().offset
    }

    /// 毒锁恢复：状态本身无不变量可破坏（advance 溢出在持锁内 panic 前已定值），
    /// into_inner 取回继续（毒锁纪律同排空权先例）。
    fn lock(&self) -> std::sync::MutexGuard<'_, MockState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Default for MockClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for MockClock {
    fn now_instant(&self) -> Instant {
        let s = self.lock();
        s.anchor.checked_add(s.offset).expect(
            "MockClock 虚拟时间超出平台 Instant 表示范围（anchor.checked_add=None，平台差异面见 armor）",
        )
    }

    fn now_system(&self) -> SystemTime {
        SystemTime::UNIX_EPOCH
            .checked_add(self.lock().offset)
            .expect("UNIX_EPOCH + offset 溢出 SystemTime 表示范围")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn assert_send_sync<T: Send + Sync>() {}

    // —— 仿钟确定性（装甲 #19 的测试地基）——

    // ① 不 advance 就冻结：两次读取逐位相等（真实钟做不到；tokio auto-advance
    //    教训的对面——零自动前进）。
    #[test]
    fn mock_freezes_without_advance() {
        let clock = MockClock::new();
        assert_eq!(clock.now_instant(), clock.now_instant());
        assert_eq!(clock.now_system(), clock.now_system());
        assert_eq!(clock.now_system(), SystemTime::UNIX_EPOCH);
        assert_eq!(clock.unix_offset(), Duration::ZERO);
    }

    // ② Instant 面：advance 前后差值精确等于 delta（checked 显式化）。
    #[test]
    fn mock_advance_moves_instant_face_exactly() {
        let clock = MockClock::new();
        let before = clock.now_instant();
        clock.advance(Duration::from_millis(90));
        let after = clock.now_instant();
        assert_eq!(
            after.checked_duration_since(before),
            Some(Duration::from_millis(90))
        );
    }

    // ③ 挂钟面：从 UNIX_EPOCH 起精确平移（跨运行逐位确定）。
    #[test]
    fn mock_advance_moves_system_face_from_epoch() {
        let clock = MockClock::new();
        clock.advance(Duration::from_secs(90));
        assert_eq!(
            clock.now_system(),
            SystemTime::UNIX_EPOCH
                .checked_add(Duration::from_secs(90))
                .unwrap()
        );
        assert_eq!(clock.unix_offset(), Duration::from_secs(90));
    }

    // ④ 累计推进记账：多次 advance 求和一致。
    #[test]
    fn mock_unix_offset_tracks_cumulative_advances() {
        let clock = MockClock::new();
        clock.advance(Duration::from_millis(250));
        clock.advance(Duration::from_secs(2));
        clock.advance(Duration::from_nanos(7));
        assert_eq!(
            clock.unix_offset(),
            Duration::from_millis(250) + Duration::from_secs(2) + Duration::from_nanos(7)
        );
    }

    // ⑤ 累计推进溢出 fail-closed：MAX 之后再进 1ns 必须带因 panic（Duration 层
    //    判定，全平台确定；Instant 层的平台差异面不进测试）。
    #[test]
    #[should_panic(expected = "MockClock 虚拟时间溢出")]
    fn advance_overflow_fails_closed() {
        let clock = MockClock::new();
        clock.advance(Duration::MAX);
        clock.advance(Duration::from_nanos(1));
    }

    // —— 对象安全与线程语义（采用方注入形态）——

    // ⑥ Send+Sync + dyn 可用（Arc<dyn Clock> 生产/测试注入标准形）。
    #[test]
    fn clock_objects_are_send_sync_dyn_safe() {
        assert_send_sync::<SystemClock>();
        assert_send_sync::<MockClock>();
        let real: Arc<dyn Clock> = Arc::new(SystemClock);
        let _ = real.now_instant();
        let _ = real.now_system();
        let mock: Arc<dyn Clock> = Arc::new(MockClock::new());
        let _ = mock.now_instant();
        let _ = mock.now_system();
    }

    // —— SystemClock 直通语义 ——

    // ⑦ 双面各自落在真实钟读数窗口内（直通无缓存无平移）。
    #[test]
    fn system_clock_faces_read_system_sources() {
        let clock = SystemClock;
        let i0 = Instant::now();
        let r0 = SystemTime::now();
        let vi = clock.now_instant();
        let vr = clock.now_system();
        let i1 = Instant::now();
        let r1 = SystemTime::now();
        assert!(i0 <= vi && vi <= i1);
        assert!(r0 <= vr && vr <= r1);
    }

    // —— 坑面钉死（std 文档 armor，盲评攻击面）——

    // ⑧ 坑③：duration_since 逆序饱和化为零（std 自认掩盖编程错误）——
    //    pinned 语义 + checked_duration_since 显式检测对照。
    #[test]
    fn instant_duration_since_saturation_pit_pinned() {
        let clock = MockClock::new();
        let earlier = clock.now_instant();
        clock.advance(Duration::from_secs(1));
        let later = clock.now_instant();
        assert_eq!(earlier.duration_since(later), Duration::ZERO); // 饱和化=坑
        assert!(earlier.checked_duration_since(later).is_none()); // 显式检测
        assert_eq!(
            later.checked_duration_since(earlier),
            Some(Duration::from_secs(1))
        );
    }

    // ⑨ 坑④：Instant + Duration::MAX 超出平台表示——checked_add 全平台可检测
    //    （macOS 上 `+` 运算符即 panic 的官方文档坑，检测面无平台差异）。
    #[test]
    fn instant_checked_add_overflow_detectable() {
        assert!(Instant::now().checked_add(Duration::MAX).is_none());
    }

    // ⑩ 坑：SystemTime 非单调——逆序差值是显式 Err（含差量），与 Instant 饱和
    //    化成对照；用例经仿钟构造确定回拨量。
    #[test]
    fn system_time_reversed_gap_is_explicit_error() {
        let clock = MockClock::new();
        let t1 = clock.now_system();
        clock.advance(Duration::from_secs(5));
        let t2 = clock.now_system();
        let err = t1.duration_since(t2).unwrap_err();
        assert_eq!(err.duration(), Duration::from_secs(5));
    }
}
