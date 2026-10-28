//! 时长人读格式化与 RFC 3339 双向（行为件）。
//!
//! 常驻零依赖件：纯 std，无 feature 门。时间源契约（`Clock`/`SystemClock`/
//! `MockClock`）已下沉 `skeleton::clock`（批 1 契约真下沉，2026-10-18；ADR 0001）——本域保留两个行为面：
//!
//! - [`format_duration`]：humantime 语义去 calendar 单位（day 及以下）；
//! - `rfc3339` 子模块：RFC 3339 时间戳双向解析/格式化（恒 UTC）。
//!
//! 时长格式化对标 humantime（omit-zero/截断不四舍五入/全零 "0s"/ASCII 单位），
//! **去 calendar 单位**：humantime 的 year=365.25d / month=30.44d 是虚构历法
//!（本仓 calendar 判否哲学），只保留 day（=86400s 固定）及以下。

use std::fmt::Write as _;
use std::time::Duration;

/// 时长人读格式化：humantime 语义去 calendar 单位（day 及以下）。
///
/// 规则（humantime `format_duration` 对标，源码逐条对表）：
///
/// - 单位序 `day / h / m / s / ms / us / ns`，零分量全部省略，全零输出 `"0s"`；
/// - **截断不四舍五入**（Display 语义，不发明精度）；纯整数除法分解，无浮点
///   参与（`as_secs_f64` 类路径的大数精度坑不存在）；
/// - day 计数 >1 加复数 `s`（humantime year/month/day 复数惯例，仅 day 存活）；
/// - `us` 用 ASCII（Windows 控制台 GBK 安全），不输出 `µs`。
///
/// 性能锚（2026-10-16 批 1-b 定性，release/std Instant 粗测，详见
/// time 域性能锚（本地留存，未入库））：~87-88ns/次与输入量级无关（整数除法
/// O(1)，无浮点参与）；`Duration::MAX`（44 字符 8 分量）327ns；分配恒
/// 1 次/调用（返回 String 本体，存活承接口径实测）。
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use std_ext::clock::format_duration;
///
/// assert_eq!(format_duration(Duration::new(9420, 0)), "2h 37m");
/// assert_eq!(format_duration(Duration::from_millis(32)), "32ms");
/// assert_eq!(format_duration(Duration::ZERO), "0s");
/// ```
pub fn format_duration(d: Duration) -> String {
    let secs = d.as_secs();
    let nanos = d.subsec_nanos();
    if secs == 0 && nanos == 0 {
        return "0s".to_owned();
    }

    let days = secs / 86_400;
    let day_secs = secs % 86_400;
    let hours = day_secs / 3_600;
    let minutes = day_secs % 3_600 / 60;
    let seconds = day_secs % 60;
    let millis = nanos / 1_000_000;
    let micros = nanos / 1_000 % 1_000;
    let nanos_rem = nanos % 1_000;

    let mut out = String::new();
    let mut started = false;
    // 复数 s 是 humantime 的 year/month/day 惯例，本域仅 day 存活。
    let day_name = if days > 1 { "days" } else { "day" };
    push_unit(&mut out, &mut started, day_name, days);
    push_unit(&mut out, &mut started, "h", hours);
    push_unit(&mut out, &mut started, "m", minutes);
    push_unit(&mut out, &mut started, "s", seconds);
    push_unit(&mut out, &mut started, "ms", u64::from(millis));
    push_unit(&mut out, &mut started, "us", u64::from(micros));
    push_unit(&mut out, &mut started, "ns", u64::from(nanos_rem));
    out
}

/// 追加一个非零分量（零分量静默省略；复数由调用方并入 `name`）。
fn push_unit(out: &mut String, started: &mut bool, name: &str, value: u64) {
    if value == 0 {
        return;
    }
    if *started {
        out.push(' ');
    }
    // String 的 fmt::Write 恒成功（std 契约），Result 恒 Ok 故忽略。
    let _ = write!(out, "{value}{name}");
    *started = true;
}

/// RFC 3339 时间戳双向解析/格式化（ISO 8601 基础面，恒 UTC）。
pub mod rfc3339;

#[cfg(test)]
mod tests {
    use super::*;

    // —— format_duration（humantime 语义去 calendar）——

    // ⑫ 全零 → "0s"（humantime 语义：空串不可接受）。
    #[test]
    fn format_zero_is_zero_s() {
        assert_eq!(format_duration(Duration::ZERO), "0s");
    }

    // ⑬ 亚秒单位链：ms/us/ns 零分量省略、ASCII us。
    #[test]
    fn format_subsecond_units() {
        assert_eq!(format_duration(Duration::from_millis(32)), "32ms");
        assert_eq!(format_duration(Duration::from_micros(1)), "1us");
        assert_eq!(format_duration(Duration::from_nanos(1)), "1ns");
        assert_eq!(format_duration(Duration::from_nanos(1500)), "1us 500ns");
        assert_eq!(format_duration(Duration::from_millis(1500)), "1s 500ms");
    }

    // ⑭ 复合与零分量省略：humantime 官例 "2h 37m"；day=86400s 固定。
    #[test]
    fn format_compound_omits_zero_components() {
        assert_eq!(format_duration(Duration::new(9420, 0)), "2h 37m");
        assert_eq!(format_duration(Duration::from_secs(3600)), "1h");
        let one_day = Duration::from_secs(86_400);
        assert_eq!(
            format_duration(one_day + Duration::from_secs(2 * 3600 + 3 * 60 + 4)),
            "1day 2h 3m 4s"
        );
        assert_eq!(format_duration(2 * one_day), "2days");
    }

    // ⑮ 截断不四舍五入（Display 语义不发明精度）。
    #[test]
    fn format_truncates_not_rounds() {
        assert_eq!(
            format_duration(Duration::from_nanos(1_500_500)),
            "1ms 500us 500ns"
        );
        assert_eq!(
            format_duration(Duration::from_millis(61_999)),
            "1m 1s 999ms"
        );
    }

    // ⑯ Duration::MAX 无溢出：纯整数除法分解（大数 f64 精度坑不存在）。
    #[test]
    fn format_duration_max_no_overflow() {
        let s = format_duration(Duration::MAX);
        assert!(s.ends_with("999ms 999us 999ns"));
        assert!(s.contains("days "));
    }
}
