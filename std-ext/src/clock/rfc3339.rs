//! RFC 3339 时间戳双向解析/格式化（ISO 8601 基础面，恒 UTC，不涉时区历法库）。
//!
//! 契约锚：RFC 3339 §5.6 ABNF 全集 + §5.7 限制（hour 00-23 禁 24、闰秒规则、
//! 按月天数）+ §4.2 NOTE（小写 t/z 合法）。对标 humantime 2.x 的差异面在
//! 各处 doc 注明（负时间/任意偏移/闰秒语义）。
//!
//! 测试先行（TDD）：本模块 tests 先于实现落地，红绿留痕。

use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// 表示域下界：0000-01-01T00:00:00Z 的 Unix 秒（civil 算法自洽 + 1900 锚推验证）。
const ERA_MIN_SECS: i64 = -62_167_219_200;
/// 表示域上界秒：9999-12-31T23:59:59Z（humantime `max::SECONDS` = 本值 + 1，
/// 源码交叉验证）。
const ERA_MAX_SECS: i64 = 253_402_300_799;

/// RFC 3339 时间戳解析失败（错误优先序：结构 → 数字 → 值域 → 闰秒；
/// `at` = 出错字节偏移，字段类错误指向字段起始位）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimeParseError {
    /// 数字位出现非 ASCII 数字。
    InvalidDigit {
        /// 非法字符的字节偏移。
        at: usize,
        /// 非法字符字节值。
        byte: u8,
    },
    /// 结构错误：长度不足 / 分隔符错 / 偏移形态错 / `Z` 后带尾巴。
    InvalidFormat {
        /// 出错字节偏移。
        at: usize,
    },
    /// 字段值超界（月/日/时/分/偏移字段，或偏移换算越出 current era
    /// 0000-9999，RFC 3339 §1/§5.7）。
    OutOfRange {
        /// 出错字段（或偏移符号位）的字节偏移。
        at: usize,
    },
    /// 闰秒 `:60`：RFC 3339 §5.7 合法但 std [`SystemTime`] 不计闰秒，无法
    /// 表示——fail-closed 拒绝（humantime 同输入静默截到 `:59`，两时刻塌缩，
    /// 本域不做；采用方需要闰秒语义时必须显式预处理）。
    LeapSecond {
        /// 秒字段的字节偏移。
        at: usize,
    },
}

impl fmt::Display for TimeParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidDigit { at, byte } => {
                write!(f, "invalid RFC 3339 digit 0x{byte:02X} at offset {at}")
            }
            Self::InvalidFormat { at } => write!(f, "invalid RFC 3339 format at offset {at}"),
            Self::OutOfRange { at } => write!(f, "RFC 3339 field out of range at offset {at}"),
            Self::LeapSecond { at } => write!(
                f,
                "leap second :60 at offset {at} not representable in std SystemTime (no leap seconds)"
            ),
        }
    }
}

impl std::error::Error for TimeParseError {}

/// 格式化为 RFC 3339 UTC 规范形（恒 `Z` 后缀，`T` 大写，秒偏移恒零）。
///
/// 小数秒**如实最小位**：纳秒为零输出整秒；否则 9 位数字去尾零
///（`.52` / `.123456789`）——RFC 3339 `time-secfrac = "." 1*DIGIT` 合法面，
/// roundtrip 无损。表示域 = RFC 3339 current era
/// `[0000-01-01T00:00:00Z, 9999-12-31T23:59:59.999999999Z]`（§1），越界
/// fail-closed panic（clock 域 armor 纪律同 [`super::MockClock::advance`]
/// 溢出先例）。
///
/// 排序限定（审计 F8）：「字符串序==时间序」仅在**同 frac 位数**下成立
/// （R20 性质前提）；最小尾零形不保证同秒字典序——`.52Z` < `.5Z`（'2' <
/// 'Z'）而时间序相反。需字符串序比较的采用方自保证等长 frac（或先解析），
/// 或改用整秒定宽形 [`format_rfc3339_seconds`]（无 frac ⇒ 全 era 字典序
/// ==时间序）。
///
/// # Examples
///
/// ```
/// use std::time::{Duration, SystemTime};
/// use std_ext::clock::rfc3339::format_rfc3339;
///
/// let epoch = SystemTime::UNIX_EPOCH;
/// assert_eq!(format_rfc3339(epoch), "1970-01-01T00:00:00Z");
/// let frac = epoch + Duration::new(482_196_050, 520_000_000);
/// assert_eq!(format_rfc3339(frac), "1985-04-12T23:20:50.52Z");
/// ```
///
/// # Panics
///
/// 时间越出 current era（年 < 0000 或 > 9999）。
#[must_use]
pub fn format_rfc3339(time: SystemTime) -> String {
    let (secs, nanos) = unix_parts(time);
    let mut out = String::with_capacity(30); // 整秒 20 / 满 frac 30 字节
    push_datetime_secs(&mut out, secs);
    if nanos != 0 {
        out.push('.');
        // 9 位逐位写，尾零裁剪（nanos != 0 恒有非零位，last 有界）。
        let mut digits = [0u8; 9];
        let mut n = nanos;
        for slot in digits.iter_mut().rev() {
            *slot = b'0' + (n % 10) as u8;
            n /= 10;
        }
        let last = digits
            .iter()
            .rposition(|&c| c != b'0')
            .expect("nanos != 0 恒有非零位");
        for &c in &digits[..=last] {
            out.push(c as char); // digits 恒 ASCII 数字
        }
    }
    out.push('Z');
    out
}

/// 格式化为 RFC 3339 UTC **整秒定宽形**：恒 `YYYY-MM-DDTHH:MM:SSZ`，20 字节
/// 定宽、`T` 大写、恒 `Z`。亚秒截断——向过去取整（同秒内任意亚秒同落本秒；
/// epoch 前 `-0.5s` → `1969-12-31T23:59:59Z` 而非跨入 1970，与
/// [`parse_rfc3339`] 对 >9 位小数的截断同向）。表示域/越界 fail-closed
/// panic 与 [`format_rfc3339`] 同。
///
/// 字节稳定契约（GitHub issue#3，追加型日志/账本）：同一逻辑时刻（同秒内
/// 任意亚秒）恒产出同一字节序列，不随 [`format_rfc3339`] 最小尾零形的
/// frac 位演化漂移——消费方可按 20 字节定宽自持二进制布局/换芯。
///
/// 排序性质（审计 F8 升级）：定宽 20 字节 ⇒ **全 era 字典序==时间序**，无
/// [`format_rfc3339`] 最小尾零形的「同 frac 位数」前提。
///
/// # Examples
///
/// ```
/// use std::time::{Duration, SystemTime};
/// use std_ext::clock::rfc3339::format_rfc3339_seconds;
///
/// let epoch = SystemTime::UNIX_EPOCH;
/// assert_eq!(format_rfc3339_seconds(epoch), "1970-01-01T00:00:00Z");
/// // 同一秒的两种亚秒输入 → 同一输出（字节稳定实证）。
/// let frac = epoch + Duration::new(482_196_050, 520_000_000);
/// let max = epoch + Duration::new(482_196_050, 999_999_999);
/// assert_eq!(format_rfc3339_seconds(frac), "1985-04-12T23:20:50Z");
/// assert_eq!(format_rfc3339_seconds(frac), format_rfc3339_seconds(max));
/// ```
///
/// # Panics
///
/// 时间越出 current era（年 < 0000 或 > 9999）。
#[must_use]
pub fn format_rfc3339_seconds(time: SystemTime) -> String {
    let (secs, _) = unix_parts(time);
    let mut out = String::with_capacity(20);
    push_datetime_secs(&mut out, secs);
    out.push('Z');
    out
}

/// 严格档解析：RFC 3339 §5.6 ABNF 全集——`T`/`t`、`Z`/`z`、`±HH:MM`（强制
/// 冒号）。小写 `t`/`z` 合法（§4.2 NOTE：ABNF 字面量大小写不敏感）。
///
/// 闰秒 `:60` 拒（见 [`TimeParseError::LeapSecond`]）；>9 位小数秒截断
///（向过去取整，纳秒精度）；偏移换算越出 current era 报
/// [`TimeParseError::OutOfRange`]。
///
/// # Examples
///
/// ```
/// use std::time::{Duration, SystemTime};
/// use std_ext::clock::rfc3339::parse_rfc3339;
///
/// let t = parse_rfc3339("1996-12-19T16:39:57-08:00")?;
/// assert_eq!(t, SystemTime::UNIX_EPOCH + Duration::new(851_042_397, 0));
/// # Ok::<(), std_ext::clock::rfc3339::TimeParseError>(())
/// ```
pub fn parse_rfc3339(s: &str) -> Result<SystemTime, TimeParseError> {
    parse_inner(s.as_bytes(), false)
}

/// 宽容档解析（人输面）：strict 全集之外再收——空格代 `T`（§5.6 NOTE 可读性
/// 用法）、无冒号偏移 `±HHMM`（ISO basic 形）、缺偏移 = UTC（humantime weak
/// 同语义）。
///
/// # Examples
///
/// ```
/// use std::time::{Duration, SystemTime};
/// use std_ext::clock::rfc3339::parse_rfc3339_weak;
///
/// let t = parse_rfc3339_weak("2018-02-14 00:28:07+0800")?;
/// assert_eq!(t, SystemTime::UNIX_EPOCH + Duration::new(1_518_539_287, 0));
/// # Ok::<(), std_ext::clock::rfc3339::TimeParseError>(())
/// ```
pub fn parse_rfc3339_weak(s: &str) -> Result<SystemTime, TimeParseError> {
    parse_inner(s.as_bytes(), true)
}

/// 双档共享解析核。`weak` 只放宽 `T`/偏移形态三处，值域语义恒同 strict。
fn parse_inner(b: &[u8], weak: bool) -> Result<SystemTime, TimeParseError> {
    // 最短合法形："0000-01-01T00:00:00Z" = 20 字节（weak 缺偏移 = 19）。
    if b.len() < 19 {
        return Err(TimeParseError::InvalidFormat { at: b.len() });
    }
    let year = two_digits(b, 0)? * 100 + two_digits(b, 2)?;
    if b[4] != b'-' {
        return Err(TimeParseError::InvalidFormat { at: 4 });
    }
    let month = two_digits(b, 5)?;
    if b[7] != b'-' {
        return Err(TimeParseError::InvalidFormat { at: 7 });
    }
    let day = two_digits(b, 8)?;
    if !(b[10] == b'T' || b[10] == b't' || (weak && b[10] == b' ')) {
        return Err(TimeParseError::InvalidFormat { at: 10 });
    }
    let hour = two_digits(b, 11)?;
    if b[13] != b':' {
        return Err(TimeParseError::InvalidFormat { at: 13 });
    }
    let minute = two_digits(b, 14)?;
    if b[16] != b':' {
        return Err(TimeParseError::InvalidFormat { at: 16 });
    }
    let second = two_digits(b, 17)?;

    // 值域（§5.7）。second 60 单列：RFC 合法面但表示域不可达，fail-closed。
    if hour > 23 {
        return Err(TimeParseError::OutOfRange { at: 11 });
    }
    if minute > 59 {
        return Err(TimeParseError::OutOfRange { at: 14 });
    }
    if second == 60 {
        return Err(TimeParseError::LeapSecond { at: 17 });
    }
    if second > 60 {
        return Err(TimeParseError::OutOfRange { at: 17 });
    }
    if !(1..=12).contains(&month) {
        return Err(TimeParseError::OutOfRange { at: 5 });
    }
    if !(1..=mdays(year, month)).contains(&day) {
        return Err(TimeParseError::OutOfRange { at: 8 });
    }

    // 小数秒：前 9 位进纳秒，之后位仍须是数字但值截断（>9 位向过去取整）。
    let mut nanos = 0u32;
    let mut i = 19;
    if b.get(19) == Some(&b'.') {
        i += 1;
        let mut mult = 100_000_000u32;
        let mut seen = 0;
        while let Some(&c) = b.get(i) {
            if !c.is_ascii_digit() {
                break;
            }
            if seen < 9 {
                nanos += mult * u32::from(c - b'0');
                mult /= 10;
            }
            seen += 1;
            i += 1;
        }
        if seen == 0 {
            return match b.get(i) {
                Some(&c) => Err(TimeParseError::InvalidDigit { at: i, byte: c }),
                None => Err(TimeParseError::InvalidFormat { at: i }),
            };
        }
    }

    // 偏移段。UTC = local − offset（§4.2）；-00:00 值面同零（§4.3 的语义差
    // 在 std 表示域不可承载，doc 面注明不区分）。
    let offset_start = i; // 偏移段起始（sign 位；无 frac 时=19，带 frac 随后移）
    let offset_secs: i64 = match b.get(i) {
        None => {
            if weak {
                0
            } else {
                return Err(TimeParseError::InvalidFormat { at: i });
            }
        }
        Some(&c) if c == b'Z' || c == b'z' => {
            i += 1;
            if i != b.len() {
                return Err(TimeParseError::InvalidFormat { at: i });
            }
            0
        }
        Some(&sign @ (b'+' | b'-')) => {
            let oh = two_digits(b, i + 1)?;
            let (om, next) = if b.get(i + 3) == Some(&b':') {
                (two_digits(b, i + 4)?, i + 6)
            } else if weak {
                (two_digits(b, i + 3)?, i + 5)
            } else {
                return Err(TimeParseError::InvalidFormat { at: i + 3 });
            };
            if next != b.len() {
                return Err(TimeParseError::InvalidFormat { at: next });
            }
            if oh > 23 {
                return Err(TimeParseError::OutOfRange { at: i + 1 });
            }
            if om > 59 {
                return Err(TimeParseError::OutOfRange { at: i + 4 });
            }
            let mag = i64::from(oh) * 3_600 + i64::from(om) * 60;
            if sign == b'+' {
                mag
            } else {
                -mag
            }
        }
        Some(&c) => return Err(TimeParseError::InvalidDigit { at: i, byte: c }),
    };

    let days = days_from_civil(year, month, day);
    let secs_local =
        days * 86_400 + i64::from(hour) * 3_600 + i64::from(minute) * 60 + i64::from(second);
    let secs_utc = secs_local - offset_secs;
    if !(ERA_MIN_SECS..=ERA_MAX_SECS).contains(&secs_utc) {
        // local 面恒在 era 内（year 已钳 0000-9999），越界只源于偏移面。
        return Err(TimeParseError::OutOfRange { at: offset_start });
    }

    let built = if secs_utc >= 0 {
        UNIX_EPOCH.checked_add(Duration::new(secs_utc as u64, nanos))
    } else if nanos == 0 {
        UNIX_EPOCH.checked_sub(Duration::new(secs_utc.unsigned_abs(), 0))
    } else {
        // -( |s| 秒 - nanos ) = -( (|s|-1) 秒 + (1e9-nanos) 纳秒 )：向秒借位。
        UNIX_EPOCH.checked_sub(Duration::new(
            secs_utc.unsigned_abs() - 1,
            1_000_000_000 - nanos,
        ))
    };
    // era 已钳后 checked None 理论不可达；fail-closed 保底。
    built.ok_or(TimeParseError::OutOfRange { at: offset_start })
}

/// (Unix 秒 i64, 纳秒 u32)：epoch 前负秒支持（humantime 此面直接 panic 的
/// 缺失面）。亚秒借位取规范形（nanos ∈ [0, 1e9)）。
fn unix_parts(time: SystemTime) -> (i64, u32) {
    match time.duration_since(UNIX_EPOCH) {
        Ok(d) => (d.as_secs() as i64, d.subsec_nanos()),
        Err(e) => {
            let d = e.duration();
            let sub = d.subsec_nanos();
            if sub == 0 {
                (-(d.as_secs() as i64), 0)
            } else {
                (-(d.as_secs() as i64) - 1, 1_000_000_000 - sub)
            }
        }
    }
}

/// 共享日期时间段写入（双 format 单源，禁拷贝链——审计 F6.1 教训）：
/// `YYYY-MM-DDTHH:MM:SS` 恒 20 字节定宽；era 越界 fail-closed panic
///（文案是 r17/S5 的 `should_panic` 断言锚，不可改）。
fn push_datetime_secs(out: &mut String, secs: i64) {
    let days = secs.div_euclid(86_400);
    let secs_of_day = u32::try_from(secs.rem_euclid(86_400)).expect("rem_euclid 恒 [0, 86399]");
    let (year, month, day) = civil_from_days(days);
    if !(0..=9_999).contains(&year) {
        panic!("RFC 3339 表示域（0000-9999）外的时间无法格式化：year {year}");
    }
    push_padded(out, year as u64, 4);
    out.push('-');
    push_padded(out, u64::from(month), 2);
    out.push('-');
    push_padded(out, u64::from(day), 2);
    out.push('T');
    push_padded(out, u64::from(secs_of_day / 3_600), 2);
    out.push(':');
    push_padded(out, u64::from(secs_of_day % 3_600 / 60), 2);
    out.push(':');
    push_padded(out, u64::from(secs_of_day % 60), 2);
}

/// 两字符十进制（`at`/`at+1` 必 ASCII 数字，否则带位报错）。
fn two_digits(b: &[u8], at: usize) -> Result<u32, TimeParseError> {
    Ok(digit_at(b, at)? * 10 + digit_at(b, at + 1)?)
}

fn digit_at(b: &[u8], at: usize) -> Result<u32, TimeParseError> {
    match b.get(at) {
        Some(&c) if c.is_ascii_digit() => Ok(u32::from(c - b'0')),
        Some(&c) => Err(TimeParseError::InvalidDigit { at, byte: c }),
        None => Err(TimeParseError::InvalidFormat { at }),
    }
}

/// 闰年（§5.7 / Appendix C：4 整除且非 100 整除，或 400 整除）。
fn is_leap(y: u32) -> bool {
    y.is_multiple_of(4) && (!y.is_multiple_of(100) || y.is_multiple_of(400))
}

/// 当月天数（month 值域已钳 [1,12]；防御分支返回 0 使一切 day 拒绝 =
/// fail-closed）。
fn mdays(y: u32, m: u32) -> u32 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap(y) => 29,
        2 => 28,
        _ => 0,
    }
}

/// 天序 → (年, 月, 日)（Howard Hinnant `civil_from_days`，std 纯整数，
/// 负天序即 epoch 前）。
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// (年, 月, 日) → 天序（`days_from_civil`，与上互逆；1970-01-01 → 0 已验证）。
fn days_from_civil(y: u32, m: u32, d: u32) -> i64 {
    let y = i64::from(y) - i64::from(m <= 2);
    let era = y.div_euclid(400);
    let yoe = y - era * 400; // [0, 399]
    let mp = (i64::from(m) + 9) % 12; // [0, 11]
    let doy = (153 * mp + 2) / 5 + i64::from(d) - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * 146_097 + doe - 719_468
}

/// 定宽零填充 ASCII 写入（width ≤ 4；值域已由调用方钳定）。
fn push_padded(out: &mut String, v: u64, width: usize) {
    let mut buf = [0u8; 4];
    let mut v = v;
    for slot in buf[..width].iter_mut().rev() {
        *slot = b'0' + (v % 10) as u8;
        v /= 10;
    }
    for &c in &buf[..width] {
        out.push(c as char);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// (unix 秒, 纳秒) → SystemTime（负秒支持，测试面专用 helper）。
    fn t(secs: i64, nanos: u32) -> SystemTime {
        if secs >= 0 {
            SystemTime::UNIX_EPOCH + Duration::new(secs as u64, nanos)
        } else {
            // 负秒 + 独立纳秒：-( |s| 秒 - nanos ) = -( (|s|-1) 秒 + (1e9-nanos) 纳秒 )
            SystemTime::UNIX_EPOCH - Duration::new((-secs) as u64 - 1, 1_000_000_000 - nanos)
        }
    }

    fn err(s: &str) -> TimeParseError {
        parse_rfc3339(s).unwrap_err()
    }

    // —— RFC 3339 §5.8 原文示例向量 ——

    // R1 epoch 双向。
    #[test]
    fn r1_epoch_roundtrip() {
        assert_eq!(
            parse_rfc3339("1970-01-01T00:00:00Z"),
            Ok(SystemTime::UNIX_EPOCH)
        );
        assert_eq!(
            format_rfc3339(SystemTime::UNIX_EPOCH),
            "1970-01-01T00:00:00Z"
        );
    }

    // R2 原文例 1：frac 2 位（.52）。
    #[test]
    fn r2_rfc_example_frac_two_digits() {
        let s = "1985-04-12T23:20:50.52Z";
        assert_eq!(parse_rfc3339(s), Ok(t(482_196_050, 520_000_000)));
        assert_eq!(format_rfc3339(t(482_196_050, 520_000_000)), s); // 去尾零规范形恒等
    }

    // R3 原文例 2：负偏移换算（-08:00 → 跨日 UTC）。
    #[test]
    fn r3_rfc_example_negative_offset() {
        assert_eq!(
            parse_rfc3339("1996-12-19T16:39:57-08:00"),
            Ok(t(851_042_397, 0))
        );
        assert_eq!(parse_rfc3339("1996-12-20T00:39:57Z"), Ok(t(851_042_397, 0)));
        assert_eq!(format_rfc3339(t(851_042_397, 0)), "1996-12-20T00:39:57Z");
    }

    // R4 原文例 3：+00:20 分钟级偏移 + .87 + epoch 前负秒。
    #[test]
    fn r4_rfc_example_minute_offset_pre_epoch() {
        assert_eq!(
            parse_rfc3339("1937-01-01T12:00:27.87+00:20"),
            Ok(t(-1_041_337_173, 870_000_000))
        );
        assert_eq!(
            format_rfc3339(t(-1_041_337_173, 870_000_000)),
            "1937-01-01T11:40:27.87Z" // 规范形恒 Z；UTC = local − offset = 正午 − 20 分
        );
    }

    // —— 表示域两端（负 Unix 时间是 humantime 缺失面，本域必钉）——

    // R5 epoch 前 1 秒 format。
    #[test]
    fn r5_format_one_second_before_epoch() {
        assert_eq!(format_rfc3339(t(-1, 0)), "1969-12-31T23:59:59Z");
        assert_eq!(parse_rfc3339("1969-12-31T23:59:59Z"), Ok(t(-1, 0)));
    }

    // R6 下界：0000-01-01T00:00:00Z（-62_167_219_200s）。
    // Windows armor：std SystemTime 内部 FILETIME（100ns tick，无符号，
    // 1601-01-01 起）——1601 前的时刻 std 表示层即不可达（checked_sub None，
    // 构造期 panic），fail-closed 拒绝；year 0 完整 roundtrip 仅非 Windows。
    #[test]
    fn r6_lower_bound_year_zero() {
        let parsed = parse_rfc3339("0000-01-01T00:00:00Z");
        #[cfg(target_os = "windows")]
        assert_eq!(parsed, Err(TimeParseError::OutOfRange { at: 19 }));
        #[cfg(not(target_os = "windows"))]
        {
            assert_eq!(parsed, Ok(t(-62_167_219_200, 0)));
            assert_eq!(
                format_rfc3339(t(-62_167_219_200, 0)),
                "0000-01-01T00:00:00Z"
            );
        }
    }

    // R7 上界：9999-12-31T23:59:59.999999999Z（9 位 frac 满员）。
    // Windows armor：100ns 粒度下 std 表示值为 999999900ns（低两位丢失），
    // roundtrip 值语义恒无损，字符串断言按平台如实值。
    #[test]
    fn r7_upper_bound_year_9999_full_nanos() {
        let s = "9999-12-31T23:59:59.999999999Z";
        let parsed = parse_rfc3339(s).expect("上界语法合法");
        assert_eq!(parse_rfc3339(&format_rfc3339(parsed)), Ok(parsed));
        #[cfg(target_os = "windows")]
        assert_eq!(format_rfc3339(parsed), "9999-12-31T23:59:59.9999999Z");
        #[cfg(not(target_os = "windows"))]
        assert_eq!(format_rfc3339(parsed), s);
    }

    // R8 闰年规则钉死（§5.7 + Appendix C：4/100/400 三规则）。
    #[test]
    fn r8_leap_year_rules() {
        for ok in ["2024-02-29T00:00:00Z", "2000-02-29T00:00:00Z"] {
            assert!(parse_rfc3339(ok).is_ok(), "{ok} 应闰");
        }
        for bad in ["1900-02-29T00:00:00Z", "2100-02-29T00:00:00Z"] {
            assert_eq!(
                parse_rfc3339(bad),
                Err(TimeParseError::OutOfRange { at: 8 }),
                "{bad} 非闰"
            );
        }
    }

    // R9 按月天数与字段值域（§5.7 表）。
    #[test]
    fn r9_month_day_field_ranges() {
        for ok in [
            "2024-01-31T00:00:00Z",
            "2024-04-30T00:00:00Z",
            "2023-02-28T00:00:00Z",
        ] {
            assert!(parse_rfc3339(ok).is_ok(), "{ok} 应接受");
        }
        for bad in [
            "2024-04-31T00:00:00Z", // 4 月只有 30 天
            "2024-02-30T00:00:00Z", // 闰年 2 月也只到 29
            "2024-01-00T00:00:00Z", // 日从 01 起
            "2024-13-01T00:00:00Z", // 月 01-12
            "2024-00-01T00:00:00Z",
            "2024-01-01T24:00:00Z", // hour 00-23（ISO 的 24:00 在本 profile 禁，§5.7）
            "2024-01-01T23:60:00Z", // minute 00-59
        ] {
            assert!(parse_rfc3339(bad).is_err(), "{bad} 应拒绝");
        }
    }

    // —— 闰秒（坑面对照相：humantime 截断到 :59，本域 fail-closed 拒）——

    // R10 闰秒 :60 双档同拒（std SystemTime 不计闰秒，:59/:60 塌缩同刻 =
    //     静默语义发明；RFC §5.7 允许但表示域无法保真）。
    #[test]
    fn r10_leap_second_rejected_both_tiers() {
        for s in ["1990-12-31T23:59:60Z", "1990-12-31T15:59:60-08:00"] {
            assert!(matches!(err(s), TimeParseError::LeapSecond { .. }), "{s}");
            assert!(parse_rfc3339_weak(s).is_err(), "{s} weak 同拒");
        }
    }

    // —— 偏移面（RFC §5.6 numoffset = ±HH:MM，humantime 只认 Z/+00:00 的
    //     缺失面；hour 00-23 minute 00-59）——

    // R11 合法偏移全域。
    #[test]
    fn r11_valid_offsets() {
        // +14:00（现实最大时区）与 -12:45。
        assert_eq!(
            parse_rfc3339("2024-01-01T00:00:00+14:00"),
            Ok(t(1_704_067_200 - 50_400, 0))
        );
        assert!(parse_rfc3339("2024-01-01T00:00:00-12:45").is_ok());
        // -00:00（§4.3 unknown-offset 惯例）：值面同零偏移（语义差 doc 注明）。
        assert_eq!(
            parse_rfc3339("1970-01-01T00:00:00-00:00"),
            Ok(SystemTime::UNIX_EPOCH)
        );
        // 小写 z（§4.2 NOTE：ABNF 字面量大小写不敏感，humantime 拒的本域收）。
        assert_eq!(
            parse_rfc3339("1970-01-01T00:00:00z"),
            Ok(SystemTime::UNIX_EPOCH)
        );
    }

    // R12 strict/weak 差异矩阵：weak = strict 超集。
    #[test]
    fn r12_strict_weak_tier_matrix() {
        // 空格代 T：weak（§5.6 NOTE 可读性用法）收，strict 拒。
        assert_eq!(
            parse_rfc3339_weak("2018-02-14 00:28:07Z"),
            parse_rfc3339("2018-02-14T00:28:07Z")
        );
        assert!(parse_rfc3339("2018-02-14 00:28:07Z").is_err());
        // ±HHMM 无冒号（ISO basic 形）：weak 收，strict 拒（RFC 强制冒号）。
        assert_eq!(
            parse_rfc3339_weak("1996-12-20T00:39:57+0000"),
            parse_rfc3339("1996-12-20T00:39:57Z")
        );
        assert!(parse_rfc3339("1996-12-20T00:39:57+0000").is_err());
        // 缺 offset = UTC：weak 收（humantime weak 同语义），strict 拒。
        assert_eq!(
            parse_rfc3339_weak("2018-02-14T00:28:07"),
            parse_rfc3339("2018-02-14T00:28:07Z")
        );
        assert!(parse_rfc3339("2018-02-14T00:28:07").is_err());
        // 小写 t：两档都收（RFC 合法面）。
        assert_eq!(
            parse_rfc3339("2018-02-14t00:28:07Z"),
            parse_rfc3339("2018-02-14T00:28:07Z")
        );
        // weak 是 strict 超集：strict 接受的 weak 恒同值（抽查 3 形态）。
        for s in [
            "1970-01-01T00:00:00Z",
            "1985-04-12T23:20:50.52Z",
            "1996-12-19T16:39:57-08:00",
        ] {
            assert_eq!(parse_rfc3339(s), parse_rfc3339_weak(s), "{s}");
        }
    }

    // R13 偏移换算越出 current era（§5.7）双端拒。
    #[test]
    fn r13_offset_pushes_out_of_era() {
        // 9999-12-31T23:59:59 负偏移推过 9999 上界。
        assert_eq!(
            parse_rfc3339("9999-12-31T23:59:59-14:00"),
            Err(TimeParseError::OutOfRange { at: 19 })
        );
        // 0000-01-01T00:00:00 正偏移推到公元前。
        assert_eq!(
            parse_rfc3339("0000-01-01T00:00:00+01:00"),
            Err(TimeParseError::OutOfRange { at: 19 })
        );
        // 非法偏移字段：hour 24 / minute 60。
        assert!(parse_rfc3339("2024-01-01T00:00:00+24:00").is_err());
        assert!(parse_rfc3339("2024-01-01T00:00:00+00:60").is_err());
    }

    // —— 小数秒（secfrac = "." 1*DIGIT，任意位，>9 位截断向过去取整）——

    // R14 frac 宽度面。
    #[test]
    fn r14_fraction_digits() {
        // 1 位 / 9 位 / 10 位截断。
        assert_eq!(
            parse_rfc3339("1970-01-01T00:00:00.5Z"),
            Ok(t(0, 500_000_000))
        );
        assert_eq!(
            parse_rfc3339("1970-01-01T00:00:00.123456789Z"),
            Ok(t(0, 123_456_789))
        );
        assert_eq!(
            parse_rfc3339("1970-01-01T00:00:00.1234567891Z"), // 第 10 位起截断
            Ok(t(0, 123_456_789))
        );
        // 全零 frac：值面为零但语法面合法；format 侧省略（ns==0 整秒规范形）。
        assert_eq!(
            parse_rfc3339("1970-01-01T00:00:00.0Z"),
            Ok(SystemTime::UNIX_EPOCH)
        );
        assert_eq!(format_rfc3339(t(0, 0)), "1970-01-01T00:00:00Z");
        // format 去尾零：123450000ns → ".12345"（9 位写满再裁尾零）。
        assert_eq!(
            format_rfc3339(t(0, 123_450_000)),
            "1970-01-01T00:00:00.12345Z"
        );
        // 1ns 如实 9 位（Windows 100ns 粒度下 std 表示层即归零，断言平台化）。
        #[cfg(target_os = "windows")]
        assert_eq!(format_rfc3339(t(0, 1)), "1970-01-01T00:00:00Z");
        #[cfg(not(target_os = "windows"))]
        assert_eq!(format_rfc3339(t(0, 1)), "1970-01-01T00:00:00.000000001Z");
        // "." 后无数字：非法（secfrac 至少 1 DIGIT）。
        assert!(parse_rfc3339("1970-01-01T00:00:00.Z").is_err());
    }

    // —— 语法错误面（错误优先序：结构 → 数字 → 值域 → 闰秒）——

    // R15 结构与数字错误。
    #[test]
    fn r15_syntax_errors() {
        // 短串/空串。
        assert!(parse_rfc3339("").is_err());
        assert!(parse_rfc3339("1970-01-01T00:00:0").is_err());
        // 缺 T（strict 下空格不合法，R12 已钉；这里钉完全缺失）。
        assert!(parse_rfc3339("1970-01-0100:00:00Z").is_err());
        // 字母在数字位。
        assert!(matches!(
            err("197A-01-01T00:00:00Z"),
            TimeParseError::InvalidDigit { at: 3, byte: b'A' }
        ));
        // 分隔符错。
        assert!(matches!(
            err("1970:01-01T00:00:00Z"),
            TimeParseError::InvalidFormat { .. }
        ));
        // 偏移段形态错：Z 后带尾巴 / 偏移缺冒号（strict）已在 R12；
        // 这里钉 Z 在中间。
        assert!(parse_rfc3339("1970-01-01T00:00:00Zx").is_err());
        // 非法偏移符号后非数字。
        assert!(parse_rfc3339("2024-01-01T00:00:00-:000").is_err());
    }

    // R16 Display：带位置与原因（域先例同形：Base64Error 文案风格）。
    #[test]
    fn r16_error_display_carries_position_and_reason() {
        let e = err("197A-01-01T00:00:00Z");
        let msg = e.to_string();
        assert!(msg.contains("offset 3"), "{msg}");
        assert!(msg.contains("digit"), "{msg}");
    }

    // R17 表示域越界 format fail-closed panic（clock 域 armor 先例同款纪律）。
    #[test]
    #[should_panic(expected = "RFC 3339 表示域")]
    fn r17_format_out_of_era_panics() {
        let _ = format_rfc3339(t(253_402_300_800, 0)); // 9999 年末秒 + 1
    }

    // R18 humantime 对照（dev-dep 基线）：共同接受域 parse 值相等 +
    //     跨实现 roundtrip（我们的 format 输出喂 humantime 解析回同一时刻）。
    #[test]
    fn r18_humantime_cross_check() {
        for s in [
            "1970-01-01T00:00:00Z",
            "1985-04-12T23:20:50Z",
            "1996-12-20T00:39:57Z",
            "2018-02-14T00:28:07Z",
            "2018-02-14T00:28:07.133Z",
            "2012-01-01T00:00:00Z",
            "2038-01-19T03:14:07Z",
            "9999-12-31T23:59:59Z",
        ] {
            let ours = parse_rfc3339(s).unwrap_or_else(|e| panic!("{s}: {e}"));
            let theirs =
                humantime::parse_rfc3339(s).unwrap_or_else(|e| panic!("{s}: humantime {e}"));
            assert_eq!(ours, theirs, "{s} 双实现应同值");
            // 跨实现 roundtrip：our format → humantime parse 恒等。
            assert_eq!(
                humantime::parse_rfc3339(&format_rfc3339(ours)),
                Ok(ours),
                "{s}"
            );
        }
    }

    // R19 确定性 fuzz：xorshift64* 固定种子，表示域全域 roundtrip 无损。
    #[test]
    fn r19_deterministic_roundtrip_fuzz() {
        // xorshift64*（seed 来自 dt：解析域判据的可复现形态）。
        let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut next = move || {
            state ^= state >> 12;
            state ^= state << 25;
            state ^= state >> 27;
            state.wrapping_mul(0x2545_F491_4F6C_DD1D)
        };
        // Windows armor：std SystemTime 域下界 1601（FILETIME 无符号 tick，
        // epoch − 11_644_473_600s），取 1601-01-02 垫；粒度 100ns——nanos 按
        // quantum 对齐，保证 fuzz 值落在 std 可表示集内。
        #[cfg(target_os = "windows")]
        let (era_min, quantum) = (ERA_MIN_SECS.max(-11_644_473_600 + 86_400), 100u64);
        #[cfg(not(target_os = "windows"))]
        let (era_min, quantum) = (ERA_MIN_SECS, 1u64);
        for i in 0..5000u32 {
            // 表示域 [era_min, ERA_MAX_SECS]，均匀取自余空间。
            let span = (ERA_MAX_SECS - era_min + 1) as u128;
            let secs = era_min + (next() as u128 % span) as i64;
            let nanos = (next() % (1_000_000_000 / quantum)) * quantum;
            let time = t(secs, nanos as u32); // quantum 面恒 < 1e9，安全收窄
            let s = format_rfc3339(time);
            let back = parse_rfc3339(&s).unwrap_or_else(|e| panic!("iter {i}: {s}: {e}"));
            assert_eq!(back, time, "iter {i}: {s}");
        }
    }

    // R20 时间排序性质（§5.1）：同规范形（恒 Z、同 frac 位数）下字符串序 == 时间序。
    #[test]
    fn r20_lexicographic_ordering_property() {
        let stamps: Vec<String> = (0..64u64)
            .map(|i| {
                format_rfc3339(t(
                    1_700_000_000 + (i * 7919) as i64,
                    (i * 33_583_793 % 999_999_999) as u32,
                ))
            })
            .collect();
        // 字符串排序后解析出的时间序列 == 直接按时间排序的序列。
        let mut by_string = stamps.clone();
        by_string.sort();
        let by_string: Vec<SystemTime> = by_string
            .iter()
            .map(|s| parse_rfc3339(s).unwrap())
            .collect();
        let mut by_time: Vec<SystemTime> =
            stamps.iter().map(|s| parse_rfc3339(s).unwrap()).collect();
        by_time.sort();
        assert_eq!(by_string, by_time);
    }

    // F8（审计 2026-10-17）：同秒+frac 前缀对抗样本——".52Z" < ".5Z"
    //（'2'(0x32) < 'Z'(0x5A)）字符串序与时间序相反，钉死 R20 性质的前提域
    // =「同 frac 位数」（format 最小尾零形不保证同秒字典序，format doc 已声明）。
    #[test]
    fn r20_same_second_frac_prefix_ordering_exception_is_documented() {
        let a = parse_rfc3339("2023-11-14T22:13:20.5Z").unwrap();
        let b = parse_rfc3339("2023-11-14T22:13:20.52Z").unwrap();
        assert!(a < b, "时间序：.5s < .52s");
        let (sa, sb) = (format_rfc3339(a), format_rfc3339(b));
        assert_eq!(
            (sa.as_str(), sb.as_str()),
            ("2023-11-14T22:13:20.5Z", "2023-11-14T22:13:20.52Z")
        );
        assert!(
            sb < sa,
            "同秒 frac 前缀：字符串序与时间序相反（'2' < 'Z'）——R20 前提域钉死"
        );
    }

    // —— 整秒定宽形 format_rfc3339_seconds（GitHub issue#3 / bd udf-frame-wg4：
    // 追加型日志/账本需要字节稳定的时间戳）——

    // S1 字节稳定：同一秒的三种亚秒输入（0 / .52s / .999999999s）输出逐字节
    // 相等，恒 20 字节定宽恒 `Z`。
    #[test]
    fn s1_byte_stable_same_second() {
        let whole = format_rfc3339_seconds(t(1_000_000_000, 0));
        let mid = format_rfc3339_seconds(t(1_000_000_000, 520_000_000));
        let max = format_rfc3339_seconds(t(1_000_000_000, 999_999_999));
        assert_eq!(whole, "2001-09-09T01:46:40Z");
        assert_eq!(mid, whole);
        assert_eq!(max, whole);
        assert_eq!(whole.len(), 20);
    }

    // S2 roundtrip：parse(整秒形) == 截断后时刻（含 epoch 前负秒/亚秒样本；
    // 亚秒统一 100ns 整倍数——Windows std 表示层粒度，见 r7 armor）。
    #[test]
    fn s2_roundtrip_truncated_instant() {
        // 截断方向 = 向过去取整：epoch 前 -0.5s 落 1969-12-31T23:59:59Z，
        // 不跨入 1970-01-01T00:00:00Z。
        assert_eq!(
            format_rfc3339_seconds(t(-1, 500_000_000)),
            "1969-12-31T23:59:59Z"
        );
        for (secs, nanos) in [
            (0, 0),
            (0, 999_999_900),
            (-1, 0),
            (-1, 999_999_900),
            (-2, 500_000_000),
            (482_196_050, 520_000_000),
            (1_000_000_000, 999_999_900),
            (ERA_MAX_SECS, 999_999_900),
        ] {
            let s = format_rfc3339_seconds(t(secs, nanos));
            assert_eq!(parse_rfc3339(&s), Ok(t(secs, 0)), "{s}");
        }
    }

    // S3 排序性质：定宽 20 字节 ⇒ 输出字符串字典序升序（升序时刻采样，含
    // 负秒跨秒样本；ERA_MIN 端见 S4/r6——Windows 构造期不可达）。
    #[test]
    fn s3_lexicographic_ordering_full_era() {
        let secs = [
            -86_400,
            -1,
            0,
            1,
            59,
            60,
            86_399,
            86_400,
            482_196_050,
            1_000_000_000,
            1_700_000_000,
            2_500_000_000,
            ERA_MAX_SECS,
        ];
        let stamps: Vec<String> = secs
            .iter()
            .map(|&s| format_rfc3339_seconds(t(s, 0)))
            .collect();
        for s in &stamps {
            assert_eq!(s.len(), 20, "{s}");
        }
        assert!(stamps.windows(2).all(|w| w[0] < w[1]), "{stamps:?}");
    }

    // S4 era 两端不 panic 且形态正确（截断后整秒仍在表示域内）。
    // Windows armor 同 r6：FILETIME 1601 起算，ERA_MIN 端构造期不可达。
    #[test]
    fn s4_era_boundary_shapes() {
        assert_eq!(
            format_rfc3339_seconds(t(ERA_MAX_SECS, 0)),
            "9999-12-31T23:59:59Z"
        );
        assert_eq!(
            format_rfc3339_seconds(t(ERA_MAX_SECS, 999_999_900)),
            "9999-12-31T23:59:59Z"
        );
        #[cfg(not(target_os = "windows"))]
        {
            assert_eq!(
                format_rfc3339_seconds(t(ERA_MIN_SECS, 0)),
                "0000-01-01T00:00:00Z"
            );
            assert_eq!(
                format_rfc3339_seconds(t(ERA_MIN_SECS, 999_999_900)),
                "0000-01-01T00:00:00Z"
            );
        }
    }

    // S5 表示域越界 fail-closed panic 与 format_rfc3339 同款（r17 先例）。
    #[test]
    #[should_panic(expected = "RFC 3339 表示域")]
    fn s5_out_of_era_panics_like_full_format() {
        let _ = format_rfc3339_seconds(t(ERA_MAX_SECS + 1, 0)); // 10000-01-01T00:00:00Z
    }
}
