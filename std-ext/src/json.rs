// udf-promotion-candidate: 已上浮（P1-9，2026-10-23），来源主仓 crates/xray-conf/src/json.rs。
// 提炼判据=零 xray 类型 ✓（全泛型 reader + serde_json::Value）。注释剥离对齐 Go
// `infra/conf/json_reader.go` 属通用 JSONC 语义（字符串字面量内注释标记保留），非 Xray 特有。
//! JSON 解码标准件：RFC 8259 严格 / 注释容忍双模 + 输入上限 + 行列位置错误。
//!
//! - [`JsonMode::Strict`]：serde_json 原生严格解析（对应 Go `platform.UseStrictJSON` 面）；
//! - [`JsonMode::Tolerant`]：先剥离 `//` 行注释与 `/* */` 块注释再解析（字符串字面量内不动；
//!   注释按词法空白处理——块注释内换行保留、其余内容换成一个空白，未闭合块注释
//!   报错并带开注行列）；
//! - 输入上限 [`MAX_INPUT_BYTES`]（64 MiB）：读满即停、超限报错（fail-visible，绝不无限读、
//!   绝不截断成功）；
//! - 解析错误带 1 起行列位置（serde_json 错误原样提取；未闭合块注释的位置由本域按
//!   原始输入自算。行号与原始输入一致，列号按变换后文本计——见 `strip_json_comments`）。

use std::io::Read;

/// 解码模式。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonMode {
    /// 严格 RFC 8259。
    Strict,
    /// 容忍 `//` 行注释与 `/* */` 块注释（字符串字面量内不动）。
    Tolerant,
}

/// 解析错误（带行列位置）。
///
/// `line`/`column` 为 1 起；无位置语义的失败（读取失败/输入超限/非 UTF-8）为 0。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsonParseError {
    /// 行号（1 起；无位置为 0）。
    pub line: u32,
    /// 列号（1 起；无位置为 0）。
    pub column: u32,
    /// 失败原因文案。
    pub message: String,
}

impl JsonParseError {
    fn no_position(message: impl Into<String>) -> Self {
        Self {
            line: 0,
            column: 0,
            message: message.into(),
        }
    }

    fn from_serde(err: serde_json::Error) -> Self {
        Self {
            line: u32::try_from(err.line()).unwrap_or(u32::MAX),
            column: u32::try_from(err.column()).unwrap_or(u32::MAX),
            message: err.to_string(),
        }
    }

    fn over_limit(len: usize) -> Self {
        Self::no_position(format!(
            "input exceeds {MAX_INPUT_BYTES}-byte limit (read {len} bytes)"
        ))
    }
}

/// 输入上限（64 MiB，与提炼来源 xray-conf 的历史值一致）。
pub const MAX_INPUT_BYTES: usize = 64 * 1024 * 1024;

/// 从 reader 解码（带上限：最多读 `MAX_INPUT_BYTES + 1` 字节即停，绝不无限读；
/// 超限报错而非截断成功）。
pub fn decode<R: Read>(reader: R, mode: JsonMode) -> Result<serde_json::Value, JsonParseError> {
    let mut buf = String::new();
    reader
        .take(MAX_INPUT_BYTES as u64 + 1)
        .read_to_string(&mut buf)
        .map_err(|e| JsonParseError::no_position(format!("read: {e}")))?;
    if buf.len() > MAX_INPUT_BYTES {
        return Err(JsonParseError::over_limit(buf.len()));
    }
    decode_str(&buf, mode)
}

/// 从字符串解码（超 [`MAX_INPUT_BYTES`] 报错）。
pub fn decode_str(s: &str, mode: JsonMode) -> Result<serde_json::Value, JsonParseError> {
    if s.len() > MAX_INPUT_BYTES {
        return Err(JsonParseError::over_limit(s.len()));
    }
    match mode {
        JsonMode::Strict => serde_json::from_str(s).map_err(JsonParseError::from_serde),
        JsonMode::Tolerant => {
            let stripped = strip_json_comments(s)?;
            serde_json::from_str(&stripped).map_err(JsonParseError::from_serde)
        }
    }
}

/// 从字节切片解码（要求 UTF-8）。
pub fn decode_slice(s: &[u8], mode: JsonMode) -> Result<serde_json::Value, JsonParseError> {
    let s =
        std::str::from_utf8(s).map_err(|e| JsonParseError::no_position(format!("UTF-8: {e}")))?;
    decode_str(s, mode)
}

/// 剥离 `//` 行注释和 `/* */` 块注释（注释=词法空白，不是删除）。
///
/// 字符串字面量内的注释标记与转义逐字节原样保留（对应 Go
/// `infra/conf/json_reader.go`）。块注释内的换行（`\n`/`\r`）原样保留、其余
/// 内容移除；无换行的注释替换为一个空格——两侧 token 绝不相接（`1/*c*/2` 是
/// 两个 token，不会变成 `12`）。未闭合块注释报错并带开注位置（原始输入 1 起
/// 行列）。
///
/// 位置约定：换行全保留 ⇒ 行号与原始输入一致；列号按变换后文本计（同一行内
/// 移除注释后列会左移）。唯一按原始输入计位置的是未闭合注释错误。
///
/// 输出容量取输入长度上界：每段注释最多补一个空白字节（`/*` + `*/` 至少 4
/// 字节），注释只减不增，故不会产生超出输入上限的第二块大缓冲。
fn strip_json_comments(input: &str) -> Result<String, JsonParseError> {
    let src = input.as_bytes();
    let mut out = String::with_capacity(input.len());
    // 尚未拷贝的原文起点：只在剥离注释处切段，字符串与普通字节随整段带出
    let mut copied_from = 0;
    let mut i = 0;
    while i < src.len() {
        match src[i] {
            // 字符串字面量：整段原文保留（注释标记不动，转义按两字节跳过）
            b'"' => {
                i += 1;
                while i < src.len() {
                    match src[i] {
                        b'\\' => i += 2,
                        b'"' => {
                            i += 1;
                            break;
                        }
                        _ => i += 1,
                    }
                }
                i = i.min(src.len());
            }
            b'/' if src.get(i + 1) == Some(&b'/') => {
                out.push_str(&input[copied_from..i]);
                i += 2;
                while i < src.len() && src[i] != b'\n' {
                    i += 1;
                }
                // 行尾 `\n`（若有）留作分隔符，由后续整段拷贝带出
                copied_from = i;
            }
            b'/' if src.get(i + 1) == Some(&b'*') => {
                let body_start = i + 2;
                let mut close = body_start;
                while close < src.len() {
                    if src[close] == b'*' && src.get(close + 1) == Some(&b'/') {
                        break;
                    }
                    close += 1;
                }
                if close >= src.len() {
                    let (line, column) = position_at(src, i);
                    return Err(JsonParseError {
                        line,
                        column,
                        message: format!(
                            "unterminated block comment starting at line {line}, column {column}"
                        ),
                    });
                }
                out.push_str(&input[copied_from..i]);
                let mut kept_newline = false;
                for &b in &src[body_start..close] {
                    if b == b'\n' || b == b'\r' {
                        out.push(b as char);
                        kept_newline = true;
                    }
                }
                if !kept_newline {
                    out.push(' ');
                }
                i = close + 2;
                copied_from = i;
            }
            // 普通字节（含多字节 UTF-8 的全部字节）随整段拷贝带出
            _ => i += 1,
        }
    }
    out.push_str(&input[copied_from..]);
    Ok(out)
}

/// 原始输入中 `offset` 处的 1 起行列（列按字符计、续字节不占列；`\n` 换行）。
fn position_at(src: &[u8], offset: usize) -> (u32, u32) {
    let mut line = 1u32;
    let mut column = 1u32;
    for &b in &src[..offset] {
        if b == b'\n' {
            line += 1;
            column = 1;
        } else if b & 0xC0 != 0x80 {
            column += 1;
        }
    }
    (line, column)
}
