//! 字节→文本解码、字符边界截断、case 转换（ASCII 词界）与编辑距离（字节面）。
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
/// 性能锚（2026-10-16 批 D 定性，release/std Instant 粗测，详见
/// reports/2026-10-16-perf-batchd.md）：有效路径恒分配（String 拷贝主导，
/// 语义必然），~48-71ns @30-130B；Strict 非法路径零分配即返，~7ns
///（约有效路径 1/6），Display 文案格式化另计 ~155ns。
///
/// # Examples
///
/// ```
/// use std_ext::text::{decode_bytes, DecodeStrategy};
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
                DecodeError {
                    strategy,
                    bad_start,
                    bad_end,
                }
            }),
        DecodeStrategy::Lossy => Ok(String::from_utf8_lossy(bytes).into_owned()),
    }
}

/// 字符边界安全截断：最多 `max_chars` 个字符；空串/短串原样返回；永不 panic
///（切片前边界由 `char_indices` 偏移结构性保证）。
///
/// 性能锚（2026-10-16 批 D 定性，release/std Instant 粗测，详见
/// reports/2026-10-16-perf-batchd.md；复跑载体 examples/text_multidim.rs，
/// 2026-10-17 复验）：零分配字符前缀扫描，~10-12ns @30 字符截 16，线性于
/// 扫描字符数（130 字符截 64 ≈ 34-38ns；越界全扫 64B ≈ 33ns——约 1ns/
/// 扫描字符）。
///
/// # Examples
///
/// ```
/// use std_ext::text::truncate_chars;
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

// ── case 转换（ASCII 词界，heck ASCII 子集语义）──

/// ASCII 词界规则（对齐 heck 0.5 `transform` 的 ASCII 子集，差异见下）：
///
/// 1. **分隔符**：任何 ASCII 非字母数字字符（空格/`_`/`-`/标点等）；相邻分隔符
///    折叠为一，首尾分隔符丢弃。
/// 2. **小写→大写边界**：当前词内最后一个已判小写的 ASCII 字母之后紧跟 ASCII
///    大写字母 → 在该大写字母前切（`fooBar` → `foo|Bar`）。
/// 3. **连续大写（缩略词）**：连续 ASCII 大写视为一个词，但若该段最后一个大写
///    后跟 ASCII 小写 → 在其前切（`XMLHttpRequest` → `XML|Http|Request`）。
/// 4. **数字邻接**：数字延续当前词（`foo2bar` 不切）；数字后跟 ASCII 大写 →
///    在大写前切（`foo2Bar` → `foo2|Bar`）；大写/数字继承前字符模式
///    （`HTTP2Server` → `HTTP2|Server`）。
/// 5. **非 ASCII 字符**：一律按无大小写的词内字符（与数字同档），原样保留、
///    不参与边界判定、不做任何大小写折叠（`caféLatte` → `café_latte`）。
///    与 heck 的差异：heck 按 unicode 分类求值（`É` 触发边界并折叠、emoji 作
///    分隔符）；本件只在 ASCII 字母上判定，语义自洽且零数据表。
#[derive(Clone, Copy, PartialEq, Eq)]
enum WordCase {
    /// 全小写（仅 ASCII 折叠，非 ASCII 原样）。
    Lower,
    /// 全大写（仅 ASCII 折叠，非 ASCII 原样）。
    Upper,
    /// 首字母大写 + 其余小写。
    Capitalized,
}

/// 词内字符判定：ASCII 字母数字，或任何非 ASCII 字符（按无大小写词内字符）。
fn is_word_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || !c.is_ascii()
}

/// 词内已扫描部分的模式（最后一个已判 ASCII 字母的大小写），对齐 heck
/// `WordMode`：无大小写字符（数字/非 ASCII）继承当前模式。
#[derive(Clone, Copy, PartialEq, Eq)]
enum WordMode {
    Boundary,
    Lowercase,
    Uppercase,
}

/// 自 `from` 起找下一个词的字节区间 `[start, end)`（char 边界安全：所有端点
/// 来自 `char_indices`，多字节输入不劈开不 panic）；无词返回 `None`。
///
/// 状态机对齐 heck `transform` 内层扫描：规则 2（小写后大写，在下词前切）先于
/// 规则 3（大写段遇小写，在当前前切）求值；词边界触发后模式归 `Boundary`
///（与 heck 跨词重置一致）。分隔符两点位处理：入口跳过、词内遇之收词。
fn next_word(s: &str, from: usize) -> Option<std::ops::Range<usize>> {
    let mut it = s[from..].char_indices().peekable();
    let start = loop {
        match it.peek() {
            None => return None,
            Some(&(_, c)) if !is_word_char(c) => {
                it.next();
            }
            Some(&(i, _)) => break from + i,
        }
    };
    let mut mode = WordMode::Boundary;
    let mut end = start;
    while let Some((i, c)) = it.next() {
        // 词内遇分隔符：当前词在此收尾（heck 由外层 split 承担，本扫描器单遍自含）。
        if !is_word_char(c) {
            end = from + i;
            break;
        }
        match it.peek() {
            // 末字符无下文：恒随当前词收尾（heck 尾字分支）。
            None => {
                end = from + i + c.len_utf8();
                break;
            }
            Some(&(next_i, next)) => {
                let next_mode = if c.is_ascii_lowercase() {
                    WordMode::Lowercase
                } else if c.is_ascii_uppercase() {
                    WordMode::Uppercase
                } else {
                    mode
                };
                if next_mode == WordMode::Lowercase && next.is_ascii_uppercase() {
                    // 规则 2：foo|Bar / foo2|Bar —— 在下字符前切。
                    end = from + next_i;
                    break;
                } else if mode == WordMode::Uppercase
                    && c.is_ascii_uppercase()
                    && next.is_ascii_lowercase()
                {
                    // 规则 3：XML|Http —— 在当前字符前切。
                    end = from + i;
                    break;
                }
                mode = next_mode;
                end = from + next_i;
            }
        }
    }
    Some(start..end)
}

fn push_word(out: &mut String, word: &str, shape: WordCase) {
    let mut chars = word.chars();
    if let Some(first) = chars.next() {
        match shape {
            WordCase::Lower => out.push(first.to_ascii_lowercase()),
            WordCase::Upper | WordCase::Capitalized => out.push(first.to_ascii_uppercase()),
        }
        for c in chars {
            match shape {
                WordCase::Upper => out.push(c.to_ascii_uppercase()),
                WordCase::Lower | WordCase::Capitalized => out.push(c.to_ascii_lowercase()),
            }
        }
    }
}

/// 五向转换的公共内核：惰性两趟扫描（词数+字节数 → 精确容量一次分配），
/// 无中间 Vec。
///
/// 精确容量成立依据：`push_word` 的 ASCII 折叠 1↔1 字节、非 ASCII 原样拷贝，
/// 输出总长 = 词字节和 + 分隔符数，预估即精确值 → `with_capacity` 后零扩容。
fn convert_case(s: &str, sep: Option<char>, first: WordCase, rest: WordCase) -> String {
    let (mut words, mut bytes) = (0usize, 0usize);
    let mut p = 0;
    while let Some(w) = next_word(s, p) {
        words += 1;
        bytes += w.len();
        p = w.end;
    }
    if words == 0 {
        return String::new();
    }
    let cap = match sep {
        Some(_) => bytes + words - 1,
        None => bytes,
    };
    let mut out = String::with_capacity(cap);
    let mut p = 0;
    let mut is_first = true;
    while let Some(w) = next_word(s, p) {
        if is_first {
            push_word(&mut out, &s[w.start..w.end], first);
            is_first = false;
        } else {
            if let Some(c) = sep {
                out.push(c);
            }
            push_word(&mut out, &s[w.start..w.end], rest);
        }
        p = w.end;
    }
    out
}

/// 转为 `snake_case`。
///
/// 其余四向（camel/pascal/kebab/screaming）同内核同复杂度，锚见本条。
///
/// 性能锚（2026-10-16 批 b4 定性，release/std Instant 粗测，详见
/// reports/2026-10-16-b4-text.md；复跑载体 examples/text_multidim.rs，
/// 2026-10-17 复验）：五向 @XMLHttpRequest(14B) ≈165-178ns；snake/pascal
/// @64B 混合形态 ≈600-700ns（随词数线性，锚绑采集器输入）——两趟词扫描
/// 线性主导 + 输出一次精确容量分配（`with_capacity` 零扩容）；空输出走
/// `String::new()` 零分配。
///
/// # Examples
///
/// ```
/// use std_ext::text::to_snake_case;
///
/// assert_eq!(to_snake_case("XMLHttpRequest"), "xml_http_request");
/// assert_eq!(to_snake_case("foo2Bar"), "foo2_bar");
/// ```
pub fn to_snake_case(s: &str) -> String {
    convert_case(s, Some('_'), WordCase::Lower, WordCase::Lower)
}

/// 转为 `camelCase`（首词全小写，后续词首字母大写）。
///
/// # Examples
///
/// ```
/// use std_ext::text::to_camel_case;
///
/// assert_eq!(to_camel_case("hello_world"), "helloWorld");
/// assert_eq!(to_camel_case("XMLHttpRequest"), "xmlHttpRequest");
/// ```
pub fn to_camel_case(s: &str) -> String {
    convert_case(s, None, WordCase::Lower, WordCase::Capitalized)
}

/// 转为 `PascalCase`。
///
/// # Examples
///
/// ```
/// use std_ext::text::to_pascal_case;
///
/// assert_eq!(to_pascal_case("hello_world"), "HelloWorld");
/// assert_eq!(to_pascal_case("XMLHttpRequest"), "XmlHttpRequest");
/// ```
pub fn to_pascal_case(s: &str) -> String {
    convert_case(s, None, WordCase::Capitalized, WordCase::Capitalized)
}

/// 转为 `kebab-case`。
///
/// # Examples
///
/// ```
/// use std_ext::text::to_kebab_case;
///
/// assert_eq!(to_kebab_case("helloWorld"), "hello-world");
/// ```
pub fn to_kebab_case(s: &str) -> String {
    convert_case(s, Some('-'), WordCase::Lower, WordCase::Lower)
}

/// 转为 `SCREAMING_SNAKE_CASE`。
///
/// # Examples
///
/// ```
/// use std_ext::text::to_screaming_snake_case;
///
/// assert_eq!(to_screaming_snake_case("helloWorld"), "HELLO_WORLD");
/// ```
pub fn to_screaming_snake_case(s: &str) -> String {
    convert_case(s, Some('_'), WordCase::Upper, WordCase::Upper)
}

// ── 编辑距离（字节面）──

/// Levenshtein 编辑距离（**字节面**）。
///
/// 语义拍板：在 `str` 的 UTF-8 字节序列上计算——ASCII 输入与字符级语义完全
/// 一致；非 ASCII 输入按字节计（`"中文"` vs `"中闲"` = 3，非 1）。字节面为
/// 本域既定契约（零拷贝、`O(min(m,n))` 滚动数组仅一次分配）；字符级距离
/// 需求出现时另加显式 API，不在本件内静默换语义。
///
/// 内存：`O(min(m,n))`（短串决定行宽，单行滚动数组恰一次分配），时间
/// `O(m·n)`（格数驱动，与长短侧摆放无关）。
///
/// 性能锚（2026-10-16 批 b4 定性，release/std Instant 粗测，详见
/// reports/2026-10-16-b4-text.md；复跑载体 examples/text_multidim.rs，
/// 2026-10-17 复验）：≈1.1ns/格——100B 对 ≈11.4µs、1KiB 对 ≈1.1ms、
/// 10KiB 对 ≈113ms（长度梯度=格数线性）；差异内容下 ns/格跨形状持平
///（10k×100 / 5k×200 / 1k×1k 全带内，行宽走短侧的结构证据）。观察项：
/// 同串纯重复内容在极端长宽比（10k×100）下分支预测退化可 ~10x——判级用
/// 差异内容对。归一化同距 + 一次除法。
///
/// # Examples
///
/// ```
/// use std_ext::text::edit_distance;
///
/// assert_eq!(edit_distance("kitten", "sitting"), 3);
/// assert_eq!(edit_distance("", "abc"), 3);
/// ```
pub fn edit_distance(a: &str, b: &str) -> usize {
    // 行宽取短侧（O(min)），长侧作外层。
    let (s, t) = if a.len() >= b.len() {
        (a.as_bytes(), b.as_bytes())
    } else {
        (b.as_bytes(), a.as_bytes())
    };
    if t.is_empty() {
        return s.len();
    }
    let mut row: Vec<usize> = (0..=t.len()).collect();
    for (i, &sc) in s.iter().enumerate() {
        let mut diag = row[0]; // D[i-1][0]
        row[0] = i + 1;
        for (j, &tc) in t.iter().enumerate() {
            let above = row[j + 1]; // D[i-1][j+1]
            row[j + 1] = (above + 1)
                .min(row[j] + 1)
                .min(diag + usize::from(sc != tc));
            diag = above;
        }
    }
    row[t.len()]
}

/// 归一化编辑相似度：`1 - edit_distance(a, b) / max(a, b)`（字节长）。
///
/// 两者皆空 = 1.0（恒等）；一空一非空 = 0.0。
///
/// # Examples
///
/// ```
/// use std_ext::text::edit_distance_normalized;
///
/// assert_eq!(edit_distance_normalized("", ""), 1.0);
/// assert_eq!(edit_distance_normalized("kitten", "kitten"), 1.0);
/// assert_eq!(edit_distance_normalized("kitten", "sitting"), 1.0 - 3.0 / 7.0);
/// ```
pub fn edit_distance_normalized(a: &str, b: &str) -> f64 {
    let max_len = a.len().max(b.len());
    if max_len == 0 {
        return 1.0;
    }
    1.0 - edit_distance(a, b) as f64 / max_len as f64
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

    // ⑥ 五向同词收敛：各种书写形态都解出同一组词（含混合分隔符/首尾分隔符）。
    #[test]
    fn five_forms_converge_on_canonical_words() {
        for src in [
            "hello_world",
            "helloWorld",
            "HelloWorld",
            "hello-world",
            "HELLO_WORLD",
            "  hello  world  ",
            "__hello__world__",
        ] {
            assert_eq!(to_snake_case(src), "hello_world", "snake {src:?}");
            assert_eq!(to_camel_case(src), "helloWorld", "camel {src:?}");
            assert_eq!(to_pascal_case(src), "HelloWorld", "pascal {src:?}");
            assert_eq!(to_kebab_case(src), "hello-world", "kebab {src:?}");
            assert_eq!(
                to_screaming_snake_case(src),
                "HELLO_WORLD",
                "scream {src:?}"
            );
        }
        // 四词混合分隔符：与 heck 一致逐词收敛。
        let src = "foo_bar baz-qux";
        assert_eq!(to_snake_case(src), "foo_bar_baz_qux");
        assert_eq!(to_camel_case(src), "fooBarBazQux");
        assert_eq!(to_pascal_case(src), "FooBarBazQux");
        assert_eq!(to_kebab_case(src), "foo-bar-baz-qux");
        assert_eq!(to_screaming_snake_case(src), "FOO_BAR_BAZ_QUX");
    }

    // ⑦ 缩略词边界：XMLHttpRequest / HTTPServer / ABc / AB / A（heck 规则 2/3）。
    #[test]
    fn acronym_segmentation_matches_heck() {
        assert_eq!(to_snake_case("XMLHttpRequest"), "xml_http_request");
        assert_eq!(to_camel_case("XMLHttpRequest"), "xmlHttpRequest");
        assert_eq!(to_pascal_case("XMLHttpRequest"), "XmlHttpRequest");
        assert_eq!(to_kebab_case("XMLHttpRequest"), "xml-http-request");
        assert_eq!(
            to_screaming_snake_case("XMLHttpRequest"),
            "XML_HTTP_REQUEST"
        );

        assert_eq!(to_snake_case("HTTPServer"), "http_server");
        // 连续大写段末位后跟小写 → 末位划入下词；段中位不动。
        assert_eq!(to_snake_case("ABc"), "a_bc");
        // 无下文小写 → 连续大写整段一词。
        assert_eq!(to_snake_case("AB"), "ab");
        // 单字符。
        assert_eq!(to_snake_case("A"), "a");
        assert_eq!(to_camel_case("A"), "a");
        assert_eq!(to_pascal_case("A"), "A");
        assert_eq!(to_screaming_snake_case("A"), "A");
        assert_eq!(to_kebab_case("A"), "a");
    }

    // ⑧ 数字邻接（heck 规则 4：数字继承前字符模式）。
    #[test]
    fn digit_adjacency_follows_inherited_mode() {
        // 数字延续当前词：不切。
        assert_eq!(to_snake_case("foo2bar"), "foo2bar");
        // 数字（继承小写模式）后跟大写 → 大写前切。
        assert_eq!(to_snake_case("foo2Bar"), "foo2_bar");
        // 大写词内数字继承大写模式：HTTP2 一词。
        assert_eq!(to_snake_case("HTTP2Server"), "http2_server");
        // 词首数字无前模式：与后续小写一词。
        assert_eq!(to_snake_case("2foo"), "2foo");
    }

    // ⑨ 空串与纯分隔符串恒空输出（零分配路径 String::new()）。
    #[test]
    fn empty_and_separator_only_yield_empty() {
        for src in ["", "_", "-_- ", "   ", "__--__"] {
            assert_eq!(to_snake_case(src), "");
            assert_eq!(to_camel_case(src), "");
            assert_eq!(to_pascal_case(src), "");
            assert_eq!(to_kebab_case(src), "");
            assert_eq!(to_screaming_snake_case(src), "");
        }
    }

    // ⑩ 非 ASCII：词内原样保留、不做折叠、emoji/中文不劈词不 panic（规则 5）。
    #[test]
    fn non_ascii_is_uncased_word_char() {
        // 纯非 ASCII 一词原样。
        assert_eq!(to_snake_case("中文"), "中文");
        // 中文夹字母数字仍一词（非 ASCII = 无大小写词内字符）。
        assert_eq!(to_snake_case("a中文b"), "a中文b");
        // 非 ASCII 不参与大小写判定：É 介于 a/b 之间无边界。
        assert_eq!(to_snake_case("aÉb"), "aÉb");
        // 边界判定只在 ASCII 字母上：中(Boundary) → Ab 无前置边界 → b|C 切。
        assert_eq!(to_snake_case("中文AbC"), "中文ab_c");
        // é 词内原样，大小写边界照常在 ASCII 上生效。
        assert_eq!(to_snake_case("caféLatte"), "café_latte");
        // emoji = 无大小写词内字符（与 heck 差异点：heck 作分隔符）。
        assert_eq!(to_snake_case("a😀b"), "a😀b");
    }

    // ⑪ Pascal→snake→Pascal 往返恒等（坑面：往返不幂等面）。
    #[test]
    fn pascal_snake_pascal_roundtrip_identity() {
        // 往返恒等只对 Pascal 形态输入成立（缩略词大小写信息经 snake 不可逆）。
        for src in [
            "FooBar",
            "XmlHttpRequest",
            "HttpServer",
            "Foo2bar",
            "Hello",
            "A",
        ] {
            let snake = to_snake_case(src);
            assert_eq!(to_pascal_case(&snake), src, "roundtrip {src:?}");
        }
    }

    // ⑫ 相邻分隔符折叠为一（heck：连续边界折叠）。
    #[test]
    fn adjacent_separators_collapse_to_one() {
        assert_eq!(to_snake_case("hello__world"), "hello_world");
        assert_eq!(to_kebab_case("hello---world"), "hello-world");
        assert_eq!(to_snake_case("foo_-_bar"), "foo_bar");
    }

    // ⑬ 编辑距离经典向量（含空串/单字符）。
    #[test]
    fn edit_distance_classic_vectors() {
        assert_eq!(edit_distance("kitten", "sitting"), 3);
        assert_eq!(edit_distance("flaw", "lawn"), 2);
        assert_eq!(edit_distance("", ""), 0);
        assert_eq!(edit_distance("", "abc"), 3);
        assert_eq!(edit_distance("abc", ""), 3);
        assert_eq!(edit_distance("same", "same"), 0);
        assert_eq!(edit_distance("a", "b"), 1);
    }

    // ⑭ 字节面语义如实：非 ASCII 按字节计（文档契约钉死）。
    #[test]
    fn edit_distance_is_byte_level() {
        // 「文」E6 96 87 vs 「闲」E9 97 B2：三字节全异 → 3（非字符级 1）。
        assert_eq!(edit_distance("中文", "中闲"), 3);
        // é 双字节 vs e 单字节：字节面距离 2。
        assert_eq!(edit_distance("aé", "ae"), 2);
    }

    // ⑮ 对称性 + 三角不等界：|la-lb| ≤ d ≤ max(la, lb)（字节长）。
    #[test]
    fn edit_distance_symmetry_and_bounds() {
        for (a, b) in [
            ("kitten".to_owned(), "sitting".to_owned()),
            (String::new(), "abc".to_owned()),
            ("中文".to_owned(), "中闲".to_owned()),
            ("aé".to_owned(), "ae".to_owned()),
            ("x".repeat(300), "x".repeat(297) + "y"),
        ] {
            let d = edit_distance(&a, &b);
            assert_eq!(d, edit_distance(&b, &a), "symmetry {a:?} {b:?}");
            assert!(d >= a.len().abs_diff(b.len()), "lower bound {a:?} {b:?}");
            assert!(d <= a.len().max(b.len()), "upper bound {a:?} {b:?}");
        }
    }

    // ⑯ O(min) 行宽：长短悬殊输入正确且走短侧行（功能面）。
    #[test]
    fn edit_distance_asymmetric_lengths() {
        let long = format!("{}y", "x".repeat(2000));
        let short = format!("{}y", "x".repeat(500));
        assert_eq!(edit_distance(&long, &short), 1500);
    }

    // ⑰ 归一化相似度边界：空/恒等/经典值。
    #[test]
    fn edit_distance_normalized_edges() {
        assert_eq!(edit_distance_normalized("", ""), 1.0);
        assert_eq!(edit_distance_normalized("kitten", "kitten"), 1.0);
        assert_eq!(edit_distance_normalized("", "abc"), 0.0);
        assert_eq!(edit_distance_normalized("abc", ""), 0.0);
        let got = edit_distance_normalized("kitten", "sitting");
        assert!((got - (1.0 - 3.0 / 7.0)).abs() < 1e-12, "{got}");
    }
}
