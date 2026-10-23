// udf-promotion-candidate: 已上浮（P1-9，2026-10-23），来源主仓 crates/xray-conf/src/json.rs。
// 提炼判据=零 xray 类型 ✓（全泛型 reader + serde_json::Value）。注释剥离对齐 Go
// `infra/conf/json_reader.go` 属通用 JSONC 语义（字符串字面量内注释标记保留），非 Xray 特有。
//! JSON 解码标准件：RFC 8259 严格 / 注释容忍双模 + 输入上限 + 行列位置错误。
//!
//! - [`JsonMode::Strict`]：serde_json 原生严格解析（对应 Go `platform.UseStrictJSON` 面）；
//! - [`JsonMode::Tolerant`]：先剥离 `//` 行注释与 `/* */` 块注释再解析（字符串字面量内不动）；
//! - 输入上限 [`MAX_INPUT_BYTES`]（64 MiB）：读满即停、超限报错（fail-visible，绝不无限读、
//!   绝不截断成功）；
//! - 解析错误带 1 起行列位置（提取自 serde_json，无需手写 offset 换算）。

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
            serde_json::from_str(&strip_json_comments(s)).map_err(JsonParseError::from_serde)
        }
    }
}

/// 从字节切片解码（要求 UTF-8）。
pub fn decode_slice(s: &[u8], mode: JsonMode) -> Result<serde_json::Value, JsonParseError> {
    let s =
        std::str::from_utf8(s).map_err(|e| JsonParseError::no_position(format!("UTF-8: {e}")))?;
    decode_str(s, mode)
}

/// 剥离 `//` 行注释和 `/* */` 块注释。
///
/// 字符串字面量内的注释标记被保留（对应 Go `infra/conf/json_reader.go`）。
fn strip_json_comments(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;
    let mut in_string = false;
    while i < chars.len() {
        let c = chars[i];
        if in_string {
            out.push(c);
            if c == '\\' && i + 1 < chars.len() {
                // 转义字符：保留下一字符
                out.push(chars[i + 1]);
                i += 2;
                continue;
            }
            if c == '"' {
                in_string = false;
            }
            i += 1;
            continue;
        }
        // 不在字符串中
        if c == '"' {
            in_string = true;
            out.push(c);
            i += 1;
            continue;
        }
        // 行注释 //
        if c == '/' && i + 1 < chars.len() && chars[i + 1] == '/' {
            // 跳到行尾
            i += 2;
            while i < chars.len() && chars[i] != '\n' {
                i += 1;
            }
            continue;
        }
        // 块注释 /* */
        if c == '/' && i + 1 < chars.len() && chars[i + 1] == '*' {
            i += 2;
            while i + 1 < chars.len() && !(chars[i] == '*' && chars[i + 1] == '/') {
                i += 1;
            }
            i += 2; // skip */
            if i > chars.len() {
                i = chars.len();
            }
            continue;
        }
        out.push(c);
        i += 1;
    }
    out
}
