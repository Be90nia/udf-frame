//! id 域集成测试：坑面 armor 逐条钉死（坑面表见 `src/id.rs` 头注）。
//!
//! 时间线全部经 [`MockClock`] / 脚本化 [`FixedClock`] 虚拟控制（确定性不等
//! 式是真实 sleep 通路的证伪器）。回拨/epoch 前/2^48 越界在 MockClock 类型
//! 层不可表达（`advance` 入参 Duration 无符号）——回拨 armor 的证伪器用脚
//! 本化挂钟自备。
//!
//! 注册表按进程共享（同指纹单权威）：**每个测试用独立指纹常量隔离**，禁
//! 复用他人指纹。

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use skeleton::clock::{Clock, MockClock};
use std_ext::id::{Id, IdGen};

// ---- 字段抽取（RFC 9562 §5.7 布局位面）----

fn ts(id: Id) -> u64 {
    (id.as_u128() >> 80) as u64
}

fn ver(id: Id) -> u128 {
    (id.as_u128() >> 76) & 0xF
}

fn rand_a(id: Id) -> u128 {
    (id.as_u128() >> 64) & 0xFFF
}

fn var(id: Id) -> u128 {
    (id.as_u128() >> 62) & 0b11
}

fn rand_b(id: Id) -> u64 {
    (id.as_u128() & ((1u128 << 62) - 1)) as u64
}

/// 与实现一致的基址派生公式（配额/首 id 断言用）。
fn base_of(fingerprint: u64) -> u128 {
    ((fingerprint >> 13) & 0x0FFF) as u128
}

/// panic payload 的可读化（格式化 panic = String，字面 panic = &str）。
fn panic_msg(err: Box<dyn std::any::Any + Send>) -> String {
    err.downcast_ref::<String>()
        .cloned()
        .or_else(|| err.downcast_ref::<&str>().map(|s| (*s).to_string()))
        .unwrap_or_default()
}

/// 脚本化挂钟：测试手动设值，可回拨/可设 epoch 前/可设 2^48 越界。
struct FixedClock(Mutex<SystemTime>);

impl FixedClock {
    fn at_unix_ms(ms: u64) -> Self {
        Self(Mutex::new(UNIX_EPOCH + Duration::from_millis(ms)))
    }

    fn set_unix_ms(&self, ms: u64) {
        *self.0.lock().unwrap() = UNIX_EPOCH + Duration::from_millis(ms);
    }

    fn set_before_epoch(&self) {
        *self.0.lock().unwrap() = UNIX_EPOCH - Duration::from_secs(1);
    }
}

impl Clock for FixedClock {
    fn now_instant(&self) -> Instant {
        Instant::now()
    }

    fn now_system(&self) -> SystemTime {
        *self.0.lock().unwrap()
    }
}

// ---- 布局（RFC 9562 §5.7 对表）----

#[test]
fn layout_matches_rfc9562() {
    // base 0 指纹（0x1001 = 4097 < 2^13 → 派生基址 0）：新毫秒首 id 的 rand_a = 0。
    let clock = Arc::new(FixedClock::at_unix_ms(7));
    let gen = IdGen::new(0x1001, clock.clone());
    let id = gen.gen();
    assert_eq!(ver(id), 7, "ver 位 = 0111");
    assert_eq!(var(id), 0b10, "var 位 = 10xx");
    assert_eq!(ts(id), 7, "48bit unix_ts_ms = 挂钟毫秒");
    assert_eq!(rand_a(id), 0, "新毫秒 counter = 基址（base 0 指纹）");
    assert_eq!(
        rand_b(id),
        0x1001 & ((1u64 << 62) - 1),
        "rand_b = 指纹低 62 位"
    );

    // 非零基址指纹：钉死基址派生公式（base = fp >> 13 & 0xFFF = 21）。
    let gen2 = IdGen::new(0x2_AAAA, clock.clone());
    let id2 = gen2.gen();
    assert_eq!(rand_a(id2), base_of(0x2_AAAA), "基址 = 指纹固定派生");
    assert_eq!(rand_b(id2), 0x2_AAAA, "不同指纹 rand_b 互异");
}

// ---- 排序保证 ----

#[test]
fn same_ms_strictly_increasing_with_counter_step() {
    let clock = Arc::new(MockClock::new());
    clock.advance(Duration::from_millis(1)); // ms=1：避开 ms=0 构造态首步偏移
    let fp = 0x10_0002;
    let gen = IdGen::new(fp, clock.clone());
    let mut prev = gen.gen();
    assert_eq!(rand_a(prev), base_of(fp));
    for _ in 0..100 {
        let id = gen.gen();
        assert!(id > prev, "同毫秒 counter 前进 → 数值严格递增");
        assert_eq!(ts(id), 1);
        assert_eq!(rand_a(id), rand_a(prev) + 1, "counter 步进 +1（确定性）");
        prev = id;
    }
}

#[test]
fn cross_ms_orders_by_timestamp() {
    let clock = Arc::new(MockClock::new());
    clock.advance(Duration::from_millis(5));
    let fp = 0x10_0003;
    let gen = IdGen::new(fp, clock.clone());
    let mut prev = gen.gen();
    for ms in 6..=16u64 {
        clock.advance(Duration::from_millis(1));
        let id = gen.gen();
        assert!(id > prev, "跨毫秒时间序");
        assert_eq!(ts(id), ms, "时间戳字段跟随挂钟");
        assert_eq!(rand_a(id), base_of(fp), "新毫秒 counter 重置基址");
        prev = id;
    }
}

#[test]
fn clock_rollback_stalls_without_regression() {
    let clock = Arc::new(FixedClock::at_unix_ms(10));
    let gen = IdGen::new(0x10_0004, clock.clone());
    let first = gen.gen();

    clock.set_unix_ms(5); // NTP 回拨：挂钟倒退
    let second = gen.gen();
    assert!(second > first, "回拨不倒退：停走保序");
    assert_eq!(ts(second), 10, "时间戳复用 last_ms（停走）");
    assert_eq!(
        rand_a(second),
        rand_a(first) + 1,
        "停走毫秒 counter 继续前进"
    );

    clock.set_unix_ms(20); // 钟恢复前进：新毫秒重置基址
    let third = gen.gen();
    assert!(third > second);
    assert_eq!(ts(third), 20);
    assert_eq!(rand_a(third), base_of(0x10_0004));
}

// ---- fail-closed 路径 ----

#[test]
fn counter_overflow_fails_closed() {
    let clock = Arc::new(MockClock::new());
    clock.advance(Duration::from_millis(1)); // ms=1：首 id 即基址（0）
    let gen = IdGen::new(0x10_0005, clock.clone());
    let quota = 4096 - base_of(0x10_0005); // 12bit 配额 = 4096 - 基址
    for _ in 0..quota {
        let _ = gen.gen();
    }
    let err = catch_unwind(AssertUnwindSafe(|| gen.gen()))
        .expect_err("配额耗尽必须 fail-closed panic（拒绝静默借位/拒绝等待活锁）");
    let msg = panic_msg(err);
    assert!(msg.contains("4096"), "带因 panic 应含配额语义：{msg}");
}

#[test]
fn pre_epoch_clock_fails_closed() {
    let clock = Arc::new(FixedClock::at_unix_ms(1));
    let gen = IdGen::new(0x10_000C, clock.clone());
    let _ = gen.gen();
    clock.set_before_epoch(); // 挂钟被拨到 1970 前
    let err = catch_unwind(AssertUnwindSafe(|| gen.gen()))
        .expect_err("epoch 前挂钟无法编码 unix_ts_ms，fail-closed");
    let msg = panic_msg(err);
    assert!(msg.contains("UNIX_EPOCH"), "带因 panic：{msg}");
}

#[test]
fn ts_overflow_fails_closed() {
    let clock = Arc::new(FixedClock::at_unix_ms((1u64 << 48) - 1));
    let gen = IdGen::new(0x10_000D, clock.clone());
    let edge = gen.gen(); // 字段上界仍可生成
    assert_eq!(ts(edge), (1u64 << 48) - 1);
    clock.set_unix_ms(1 << 48); // 越界一毫秒
    let err = catch_unwind(AssertUnwindSafe(|| gen.gen()))
        .expect_err("48bit 时间戳越界必须 fail-closed（拒绝静默回绕破序）");
    let msg = panic_msg(err);
    assert!(msg.contains("48bit"), "带因 panic：{msg}");
}

// ---- 确定性回放 ----

#[test]
fn deterministic_replay_same_timeline_same_sequence() {
    let run = |fp: u64| {
        let clock = Arc::new(MockClock::new());
        clock.advance(Duration::from_millis(1));
        let gen = IdGen::new(fp, clock.clone());
        let mut ids = Vec::new();
        for i in 0..50u64 {
            ids.push(gen.gen());
            if i % 7 == 3 {
                clock.advance(Duration::from_millis(1));
            }
        }
        ids
    };
    // 派生是纯函数：同时间线同调用序下，时间戳面与 counter 面逐位相同，
    // id 仅在 rand_b（指纹面）互异——同指纹跨进程回放由此逐位成立。
    let a = run(0x10_0006);
    let b = run(0x10_0007);
    assert_eq!(a.len(), b.len());
    for (x, y) in a.iter().zip(b.iter()) {
        assert_eq!(ts(*x), ts(*y), "时间戳面确定性");
        assert_eq!(rand_a(*x), rand_a(*y), "counter 面确定性");
    }
    assert_eq!(rand_b(a[0]), 0x10_0006);
    assert_eq!(rand_b(b[0]), 0x10_0007);
}

// ---- 单权威注册表 ----

#[test]
fn same_fingerprint_handles_share_authority() {
    let clock = Arc::new(MockClock::new());
    clock.advance(Duration::from_millis(1));
    let fp = 0x10_0008;
    let a = IdGen::new(fp, clock.clone());
    assert_eq!(a.fingerprint(), fp);
    // 后建句柄自带钟（读取面独立，ms=0 < 权威 last_ms=1 → 走停走分支）。
    let b = IdGen::new(fp, Arc::new(MockClock::new()));
    let mut ids = Vec::new();
    for i in 0..30 {
        ids.push(if i % 2 == 0 { a.gen() } else { b.gen() });
    }
    for w in ids.windows(2) {
        assert!(w[1] > w[0], "同指纹跨句柄严格递增（撞 id = id 域死罪）");
    }
}

#[test]
fn different_fingerprints_are_independent() {
    let clock = Arc::new(MockClock::new());
    clock.advance(Duration::from_millis(1));
    let a = IdGen::new(0x10_0009, clock.clone());
    let b = IdGen::new(0x10_000A, clock.clone());
    let id_a = a.gen();
    let id_b = b.gen();
    assert_eq!(rand_a(id_a), rand_a(id_b), "互不干扰：各自从自身基址起步");
    assert_ne!(rand_b(id_a), rand_b(id_b), "指纹面互异 → id 互异");
}

// ---- 标准连字形式（RFC 9562 §5.1 大端）----

#[test]
fn string_form_is_rfc_hyphenated() {
    let clock = Arc::new(MockClock::new());
    clock.advance(Duration::from_millis(123));
    let gen = IdGen::new(0x10_000B, clock);
    let id = gen.gen();
    let s = id.to_string();
    assert_eq!(s.len(), 36, "8-4-4-4-12 连字形：{s}");
    let chars: Vec<char> = s.chars().collect();
    for &dash in &[8, 13, 18, 23] {
        assert_eq!(chars[dash], '-', "连字位置：{s}");
    }
    for (i, c) in chars.iter().enumerate() {
        if matches!(i, 8 | 13 | 18 | 23) {
            continue;
        }
        assert!(
            c.is_ascii_hexdigit() && !c.is_ascii_uppercase(),
            "全小写 hex：{s}"
        );
    }
    assert_eq!(chars[14], '7', "version nibble 位置：{s}");
    assert!(
        matches!(chars[19], '8' | '9' | 'a' | 'b'),
        "variant nibble 位置：{s}"
    );
    let hex: String = chars.iter().filter(|c| **c != '-').collect();
    assert_eq!(
        u128::from_str_radix(&hex, 16).unwrap(),
        id.as_u128(),
        "大端 roundtrip"
    );
    // FromStr 对偶：标准连字形 parse 回原值（Display/FromStr 双向闭环）
    assert_eq!(s.parse::<Id>().unwrap(), id, "FromStr roundtrip");
}

// ---- 并发唯一性 ----

// ---- 类型化字符串面（Display / FromStr，杠点验收：format! 可用 + parse 可逆）----

#[test]
fn display_and_fromstr_roundtrip() {
    let clock = Arc::new(MockClock::new());
    clock.advance(Duration::from_millis(9));
    let gen = IdGen::new(0x10_000F, clock);
    let id = gen.gen();
    // Display 直呼：format!("{id}") 即 RFC 9562 §5.1 标准连字形
    let s = format!("{id}");
    assert_eq!(s, id.to_string());
    assert_eq!(s.len(), 36);
    // FromStr 直呼：parse 回数值面，roundtrip 逐位
    let back: Id = s.parse().expect("标准连字形必须可解析");
    assert_eq!(back, id);
    assert_eq!(back.as_u128(), id.as_u128());
    // hex 位大小写不敏感（RFC 9562 解析器惯例；Display 恒小写出）
    let upper: Id = s.to_ascii_uppercase().parse().expect("大写 hex 可解析");
    assert_eq!(upper, id);
    // 形状错误显式 Err（禁静默；Err=IdParseError）
    assert!("not-an-id!!".parse::<Id>().is_err(), "段数/段长不符=Shape");
    assert!("0123456789abcdef".parse::<Id>().is_err(), "无连字=非标准形");
}

#[test]
fn burst_uniqueness_under_threads() {
    let clock = Arc::new(MockClock::new());
    clock.advance(Duration::from_millis(1)); // 指纹 0x10_000E 基址 128：单毫秒配额 4096−128 ≥ 2000
    let gen = Arc::new(IdGen::new(0x10_000E, clock));
    let handles: Vec<_> = (0..4)
        .map(|_| {
            let gen = Arc::clone(&gen);
            thread::spawn(move || (0..500).map(|_| gen.gen()).collect::<Vec<_>>())
        })
        .collect();
    let mut all: Vec<Id> = handles
        .into_iter()
        .flat_map(|h| h.join().expect("worker 线程不 panic"))
        .collect();
    assert_eq!(all.len(), 2000);
    all.sort_unstable();
    all.dedup();
    assert_eq!(
        all.len(),
        2000,
        "2000 并发 id 无一重复（4 线程 < 4096 配额）"
    );
}

// ---- RandomId（UUIDv4 凭据级，`random` 门：关=本段编译消失、0 测试 exit 0）----
//
// 坑面 armor 对表 src/id.rs RandomId 段与调研报告域 4。真实 OS 熵（无虚拟
// 钟/无替身）——概率断言余量均 ≥1e-10（碰撞面 RFC 9562 §6.7）。

#[cfg(feature = "random")]
mod v4_random {
    use std::collections::HashSet;

    use std_ext::id::{IdParseError, RandomId, RngError};

    fn ver(id: RandomId) -> u128 {
        (id.as_u128() >> 76) & 0xF
    }

    fn var(id: RandomId) -> u128 {
        (id.as_u128() >> 62) & 0b11
    }

    // ---- 布局（RFC 9562 §4.2 逐位对表）----

    #[test]
    fn layout_matches_rfc9562_v4() {
        for _ in 0..256 {
            let id = RandomId::new().expect("os entropy available");
            assert_eq!(ver(id), 0b0100, "ver=0100");
            assert_eq!(var(id), 0b10, "var=10xx");
            // 数值面往返（同 Id 形）。
            assert_eq!(RandomId::from_u128(id.as_u128()), id);
        }
        // 连字形 ver/var nibble 落位（14 位='4'，19 位∈{8,9,a,b}）。
        let s = RandomId::new().expect("os entropy available").to_string();
        assert_eq!(s.len(), 36, "8-4-4-4-12 连字形：{s}");
        assert_eq!(&s[14..15], "4", "version nibble 位置：{s}");
        assert!(
            matches!(s.chars().nth(19), Some('8' | '9' | 'a' | 'b')),
            "variant nibble 位置：{s}"
        );
    }

    // ---- 失败通道两态显式（fallback 在本域结构上不存在）----

    #[test]
    fn rng_failure_fails_closed_no_fallback() {
        // 编译面钉死：失败通道只能是 Result<_, RngError>——禁 Option 化、
        // 禁 panic 化、禁降级成功；实现仅 getrandom 一处熵源调用，弱熵回
        // 退分支不存在。体量反证（弱熵/PID 级序列必撞）见下方唯一性测试。
        fn two_state<T>(r: Result<T, RngError>) -> Result<T, RngError> {
            r
        }
        let _ = two_state(RandomId::new());
        let mut buf = [0u8; 16];
        let _ = two_state(RandomId::fill(&mut buf));
    }

    // ---- 凭据不可预测面（issue#1-4 验收：连续 1e4 无重复 + 无时间前缀可推）----

    #[test]
    fn consecutive_ids_unique_10k() {
        let ids: HashSet<_> = (0..10_000)
            .map(|_| RandomId::new().expect("os entropy available"))
            .collect();
        assert_eq!(
            ids.len(),
            10_000,
            "连续 1e4 枚无一重复（Debian/PID 级弱熵序列在此体量必撞）"
        );
    }

    #[test]
    fn credential_ids_have_no_guessable_fields() {
        // v7 在前 48bit 面是 unix_ts_ms 时间前缀（单调可推，Netscape 型）；
        // v4 同位置必须纯随机：16 枚连续 id 前缀两两互异且非单调递增。
        let ids: Vec<_> = (0..16)
            .map(|_| RandomId::new().expect("os entropy available"))
            .collect();
        let prefix = |id: &RandomId| (id.as_u128() >> 80) as u64;
        for (i, a) in ids.iter().enumerate() {
            for b in &ids[i + 1..] {
                assert_ne!(prefix(a), prefix(b), "时间面前缀不得重源/复用");
            }
        }
        let mut asc: Vec<_> = ids.iter().map(prefix).collect();
        asc.sort_unstable();
        assert_ne!(
            ids.iter().map(prefix).collect::<Vec<_>>(),
            asc,
            "无单调时间前缀可推（v4=纯随机面，非 v7 时间序）"
        );
    }

    // ---- 批量面（armor：批量走一次 fill 大 buffer）----

    #[test]
    fn batch_fill_single_syscall_path() {
        // 批量合规路径：1e4 枚 id 的字节一次 fill 取齐（syscall 计数无法进
        // 程内插桩，单调用结构由 API 形状与测试构造钉死）。
        const BLOCKS: usize = 10_000;
        let mut buf = vec![0xAAu8; BLOCKS * 16];
        RandomId::fill(&mut buf).expect("os entropy available");
        assert!(buf.iter().any(|&b| b != 0xAA), "buffer 必须被整段覆写");
        let blocks: HashSet<_> = buf
            .chunks_exact(16)
            .map(|c| u128::from_be_bytes(c.try_into().expect("16B 块")))
            .collect();
        assert_eq!(
            blocks.len(),
            BLOCKS,
            "1e4 个 16B 块无一重复（弱熵批量必撞）"
        );
        // 空 buffer 平凡成功（getrandom 契约边界）。
        let mut empty: [u8; 0] = [];
        RandomId::fill(&mut empty).expect("空 buffer 平凡 Ok");
    }

    // ---- 类型化字符串面（Display / FromStr 与 Id 同款）----

    #[test]
    fn display_and_fromstr_roundtrip_v4() {
        for _ in 0..8 {
            let id = RandomId::new().expect("os entropy available");
            let s = id.to_string();
            assert_eq!(s.len(), 36);
            for (i, c) in s.chars().enumerate() {
                if matches!(i, 8 | 13 | 18 | 23) {
                    assert_eq!(c, '-', "连字位置：{s}");
                } else {
                    assert!(
                        c.is_ascii_hexdigit() && !c.is_ascii_uppercase(),
                        "全小写 hex：{s}"
                    );
                }
            }
            let back: RandomId = s.parse().expect("标准连字形必须可解析");
            assert_eq!(back, id, "Display/FromStr 双向闭环");
            let upper: RandomId = s.to_ascii_uppercase().parse().expect("大写 hex 可解析");
            assert_eq!(upper, id, "hex 位大小写不敏感");
        }
        // 形状错误显式 Err（禁静默；Err=IdParseError，与 Id 同一套）。
        assert_eq!(
            "not-an-id".parse::<RandomId>(),
            Err(IdParseError::Shape),
            "段数/段长不符=Shape"
        );
    }
}
