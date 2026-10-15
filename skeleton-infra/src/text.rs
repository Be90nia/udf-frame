//! 字节→文本解码与字符边界截断。
//!
//! 常驻零依赖件：纯 std，无 feature 门。零依赖纪律（`cargo tree` 单行证明）
//! 要求手写同形 `Display`/`Error` impl（错误文案与 thiserror 派生形态逐字
//! 一致），引入 thiserror 依赖时可换回派生。

/// 解码策略：显式选择，禁静默 lossy 默认（信任边界默认 Strict）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodeStrategy {
    /// 严格 UTF-8：非法字节=显式 Err。
    Strict,
    /// 损失化：非法字节替换 U+FFFD（日志展示等展示型场景显式选择）。
    Lossy,
}

/// 解码失败：首个非法序列的字节偏移区间（禁静默乱码）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodeError {
    /// 失败时所选策略（回传便于上层无需回看调用参数）。
    pub strategy: DecodeStrategy,
    /// 非法序列起始偏移（= std Utf8Error::valid_up_to）。
    pub bad_start: usize,
    /// 非法序列结束偏移（error_len 为 None=截断尾部时取 bytes.len()）。
    pub bad_end: usize,
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "invalid utf-8 at bytes {}..{} (strategy={:?})",
            self.bad_start, self.bad_end, self.strategy
        )
    }
}

impl std::error::Error for DecodeError {}

/// 解码字节为 UTF-8 文本。
///
/// Strict：`std::str::from_utf8` 语义；Lossy：`String::from_utf8_lossy` 语义
///（Lossy 无错误路径恒 Ok）。
///
/// # Examples
///
/// ```
/// use skeleton_infra::text::{decode_bytes, DecodeStrategy};
///
/// assert_eq!(decode_bytes("中文".as_bytes(), DecodeStrategy::Strict), Ok("中文".to_owned()));
/// assert_eq!(decode_bytes(b"\xff", DecodeStrategy::Lossy).unwrap(), "\u{FFFD}");
/// ```
pub fn decode_bytes(bytes: &[u8], strategy: DecodeStrategy) -> Result<String, DecodeError> {
    match strategy {
        DecodeStrategy::Strict => std::str::from_utf8(bytes)
            .map(str::to_owned)
            .map_err(|err| {
                let bad_start = err.valid_up_to();
                let bad_end = match err.error_len() {
                    Some(len) => bad_start + len,
                    None => bytes.len(),
                };
                DecodeError { strategy, bad_start, bad_end }
            }),
        DecodeStrategy::Lossy => Ok(String::from_utf8_lossy(bytes).into_owned()),
    }
}

/// 字符边界安全截断：最多 `max_chars` 个字符；空串/短串原样返回；永不 panic
///（切片前边界由 `char_indices` 偏移结构性保证）。
///
/// # Examples
///
/// ```
/// use skeleton_infra::text::truncate_chars;
///
/// assert_eq!(truncate_chars("中文", 1), "中");
/// assert_eq!(truncate_chars("a\u{1F600}b", 2), "a\u{1F600}");
/// assert_eq!(truncate_chars("", 5), "");
/// ```
pub fn truncate_chars(s: &str, max_chars: usize) -> &str {
    match s.char_indices().nth(max_chars) {
        Some((byte_idx, _)) => &s[..byte_idx],
        None => s,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ① Strict 合法输入 Ok 恒等。
    #[test]
    fn strict_valid_input_is_identity() {
        for case in ["", "hello", "中文", "a\u{1F600}b"] {
            assert_eq!(
                decode_bytes(case.as_bytes(), DecodeStrategy::Strict),
                Ok(case.to_owned())
            );
        }
    }

    // ② Strict 非法输入 Err 且偏移正确（含截断尾部 error_len=None 用例）。
    #[test]
    fn strict_invalid_input_reports_exact_offsets() {
        // 单字节非法：valid_up_to=1，error_len=Some(1)。
        let err = decode_bytes(b"a\xffb", DecodeStrategy::Strict).unwrap_err();
        assert_eq!(err.strategy, DecodeStrategy::Strict);
        assert_eq!(err.bad_start, 1);
        assert_eq!(err.bad_end, 2);

        // 截断尾部：三字节字符「中」只留前两字节，error_len=None → bad_end=输入长度。
        let truncated = &"ab中".as_bytes()[..4];
        let err = decode_bytes(truncated, DecodeStrategy::Strict).unwrap_err();
        assert_eq!(err.bad_start, 2);
        assert_eq!(err.bad_end, truncated.len());

        // 错误文案契约（与 thiserror 派生形态格式串逐字对齐）。
        assert_eq!(
            err.to_string(),
            "invalid utf-8 at bytes 2..4 (strategy=Strict)"
        );
    }

    // ③ Lossy 非法输入替换 U+FFFD。
    #[test]
    fn lossy_replaces_invalid_bytes_with_replacement_char() {
        assert_eq!(
            decode_bytes(b"a\xffb", DecodeStrategy::Lossy).unwrap(),
            "a\u{FFFD}b"
        );
    }

    // ④ truncate 边界：中文字符 / emoji 多字节单元不劈开 / 空串 / max=0 / 超长。
    #[test]
    fn truncate_respects_char_boundaries() {
        assert_eq!(truncate_chars("中文", 1), "中");
        assert_eq!(truncate_chars("中文", 10), "中文"); // 超长 → 原样
        assert_eq!(truncate_chars("a\u{1F600}b", 2), "a\u{1F600}"); // 4 字节单元整体保留
        assert_eq!(truncate_chars("", 5), ""); // 空串
        assert_eq!(truncate_chars("中文", 0), ""); // max=0 → 空串
        assert_eq!(truncate_chars("中文", 2), "中文"); // 恰好等长
    }

    // ⑤ decode 空输入。
    #[test]
    fn decode_empty_input_is_ok_empty() {
        assert_eq!(decode_bytes(b"", DecodeStrategy::Strict), Ok(String::new()));
        assert_eq!(decode_bytes(b"", DecodeStrategy::Lossy), Ok(String::new()));
    }
}
