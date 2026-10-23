//! 字节↔文本编码转换：hex（Base16）/base64/BOM 剥离/UTF-16。text 域姊妹模块
//! （同一信任边界哲学：显式策略、非法输入显式 Err 禁静默）。
//!
//! 坑面拍板（实现前系统排查，对照表见 reports/2026-10-16-b1-encode.md）：
//!
//! - 非字母表字符（含空白/NUL）恒 Err：RFC 4648 §3.3「Implementations MUST
//!   reject the encoded data if it contains characters outside the base
//!   alphabet」；宽松忽略即 covert channel（§12 Security Considerations 明文）。
//! - 解码默认拒绝非规范尾位（non-canonical trailing bits）：RFC §3.5「decoders
//!   MAY chose to reject an encoding if the pad bits have not been set to
//!   zero」拍板为拒绝——宽容接受 = 静默丢失信息位；base64 crate 0.23 默认同向
//!   （`GeneralPurposeConfig::new()` 即 `decode_allow_trailing_bits = false`）。
//! - pad 形态显式验证：`=` 只许尾部 0/1/2 个且与尾组形态匹配（RFC §4 形态
//!   (1)(2)(3)）；`len % 4 == 1` 恒非法（6 bits 信息不足一字节）。no-pad 输入
//!   按 §3.2 合法变体接受（pad 有无不产生歧义：形状输入自描述）。
//! - hex 大小写：编码两态显式（[`HexCase`]）；解码大小写不敏感（RFC §8「the
//!   standard case-insensitive hex encoding」明文），非 hex 字符/奇数长度显式
//!   Err。
//! - BOM：[`strip_bom`] 只剥一层 UTF-8 BOM 前缀（循环剥会把正文中的 U+FEFF
//!   一起毁掉）；UTF-16 BOM 属端序声明语义，归 [`decode_utf16_bytes`] 处理
//!   （与端序参数一致则剥除，矛盾则 Err）。
//! - UTF-16 端序显式参数（Le/Be），无默认端序——无 BOM 的 UTF-16 字节流端序
//!   不可判定，静默择一即静默出错。unpaired surrogate 显式 Err（text 域
//!   Strict 同哲学，禁 U+FFFD 静默替换）。
//!
//! 常驻零依赖件：纯 std，无 feature 门。零依赖纪律要求手写同形 `Display`/
//! `Error` impl（与 text 域同门风格）。

// ── hex（Base16，RFC 4648 §8）──

/// hex 编码字母大小写：显式选择，无默认。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HexCase {
    /// 小写：`0-9a-f`。
    Lower,
    /// 大写：`0-9A-F`（RFC 4648 §10 BASE16 向量形态）。
    Upper,
}

/// hex 解码失败：奇数长度或非法字符（禁静默截断/跳过，F23）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HexError {
    /// 输入长度为奇数：尾 nibble 无法成字节。`len` = 输入字节长度。
    OddLength {
        /// 输入字节长度。
        len: usize,
    },
    /// 非十六进制字符（空白/NUL 同样非法，RFC §3.3）。`at` = 字符偏移。
    InvalidDigit {
        /// 非法字符的字节偏移。
        at: usize,
    },
}

impl std::fmt::Display for HexError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HexError::OddLength { len } => write!(f, "odd hex length {len}"),
            HexError::InvalidDigit { at } => write!(f, "invalid hex digit at offset {at}"),
        }
    }
}

impl std::error::Error for HexError {}

/// 十六进制 nibble 字符→值的反查表：`0xFF` 哨兵=非法字符。
const HEX_INVALID: u8 = 0xFF;

const fn hex_decode_table() -> [u8; 256] {
    let mut t = [HEX_INVALID; 256];
    let digits = b"0123456789abcdefABCDEF";
    let mut i = 0;
    while i < digits.len() {
        let c = digits[i];
        t[c as usize] = match c {
            b'0'..=b'9' => c - b'0',
            b'a'..=b'f' => c - b'a' + 10,
            _ => c - b'A' + 10,
        };
        i += 1;
    }
    t
}

/// 数值转换查表无分支；合法性判定一次哨兵比较（显式 Err，RFC §3.3）。
static HEX_DECODE: [u8; 256] = hex_decode_table();

/// 编码字节为 hex 文本（Base16）。输出长度恒为输入两倍（RFC §8 无 pad）。
///
/// 性能锚（2026-10-16 批 encode，release/std Instant 粗测，详见
/// reports/2026-10-16-b1-encode.md）：有效路径恒分配（输出 String，语义必
/// 然），~61ns @16B（分配固定开销主导）/ ~1.37µs @1KiB / ~610-720 MiB/s
/// @≥64KiB；查表主路径无分支。
///
/// # Examples
///
/// ```
/// use std_ext::encode::{hex_encode, HexCase};
///
/// assert_eq!(hex_encode(b"foobar", HexCase::Upper), "666F6F626172");
/// assert_eq!(hex_encode(b"\xCA\xFE", HexCase::Lower), "cafe");
/// ```
pub fn hex_encode(bytes: &[u8], case: HexCase) -> String {
    let table: &[u8; 16] = match case {
        HexCase::Lower => b"0123456789abcdef",
        HexCase::Upper => b"0123456789ABCDEF",
    };
    let mut out = String::with_capacity(bytes.len() * 2);
    // SAFETY: 只写入 ASCII nibble 字符（0-9A-Fa-f 恒合法 UTF-8），String
    // 不变量保持。
    let buf = unsafe { out.as_mut_vec() };
    for &b in bytes {
        buf.push(table[(b >> 4) as usize]);
        buf.push(table[(b & 0x0F) as usize]);
    }
    out
}

/// 解码 hex 文本为字节。大小写不敏感（RFC §8 明文）；奇数长度/非法字符显式
/// Err（错误含偏移，定位到首个坏位）。
///
/// 性能锚（同上报告）：恒分配（输出 Vec），~1.09µs @1KiB；错误路径零成本即
/// 返（~40ns @4B 首坏位）。
///
/// # Examples
///
/// ```
/// use std_ext::encode::hex_decode;
///
/// assert_eq!(hex_decode("666F6f").unwrap(), b"foo");
/// assert_eq!(hex_decode("6g"), Err(std_ext::encode::HexError::InvalidDigit { at: 1 }));
/// ```
pub fn hex_decode(s: &str) -> Result<Vec<u8>, HexError> {
    let bytes = s.as_bytes();
    if !bytes.len().is_multiple_of(2) {
        return Err(HexError::OddLength { len: bytes.len() });
    }
    let mut out = Vec::with_capacity(bytes.len() / 2);
    let mut i = 0;
    while i < bytes.len() {
        let hi = HEX_DECODE[bytes[i] as usize];
        if hi == HEX_INVALID {
            return Err(HexError::InvalidDigit { at: i });
        }
        let lo = HEX_DECODE[bytes[i + 1] as usize];
        if lo == HEX_INVALID {
            return Err(HexError::InvalidDigit { at: i + 1 });
        }
        out.push(hi << 4 | lo);
        i += 2;
    }
    Ok(out)
}

// ── base64（RFC 4648 §4/§5）──

/// base64 字母表：标准（RFC §4 Table 1）或 URL/filename 安全（RFC §5 Table 2，
/// 62/63 位换 `-`/`_`）。两表不混用：解码时非本表字符显式 Err（禁静默跨表）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Base64Alphabet {
    /// 标准（`+`/`/`）。
    Std,
    /// URL 与文件名安全（`-`/`_`）。
    UrlSafe,
}

/// base64 解码失败（错误优先序：长度 → pad 形态 → 非法字符 → 非规范尾位）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Base64Error {
    /// 非本字母表字符（含空白/NUL/中段 `=`，RFC §3.3）。`at` = 字节偏移。
    InvalidByte {
        /// 非法字符的字节偏移。
        at: usize,
        /// 非法字符字节值。
        byte: u8,
    },
    /// 尾组 1 字符（`len % 4 == 1`）：6 bits 不足一字节，RFC §4 无此形态。
    /// `len` = 输入字节长度。
    InvalidLength {
        /// 输入字节长度。
        len: usize,
    },
    /// pad 数量与尾组形态不匹配或位置非法（RFC §4 形态 (1)(2)(3) 之外）。
    /// `at` = 首个 pad 字符偏移。
    InvalidPadding {
        /// 首个 pad 字符的字节偏移。
        at: usize,
    },
    /// 非规范尾位：尾字符的未用位非零（RFC §3.5「decoders MAY reject」，拍板
    /// 拒绝；base64 crate 0.23 默认同语义）。无载荷字段：位置恒在尾部。
    NonCanonicalTail,
}

impl std::fmt::Display for Base64Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Base64Error::InvalidByte { at, byte } => {
                write!(f, "invalid base64 byte 0x{byte:02X} at offset {at}")
            }
            Base64Error::InvalidLength { len } => write!(f, "invalid base64 length {len}"),
            Base64Error::InvalidPadding { at } => {
                write!(f, "invalid base64 padding at offset {at}")
            }
            Base64Error::NonCanonicalTail => write!(f, "non-canonical base64 tail bits"),
        }
    }
}

impl std::error::Error for Base64Error {}

/// base64 标准字母表（RFC §4 Table 1）。
const B64_STD: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
/// URL/filename 安全字母表（RFC §5 Table 2）：62/63 位换 `-`/`_`。
const B64_URL_SAFE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

const fn b64_decode_table(alphabet: &[u8; 64]) -> [u8; 256] {
    let mut t = [HEX_INVALID; 256];
    let mut i = 0;
    while i < 64 {
        t[alphabet[i] as usize] = i as u8;
        i += 1;
    }
    t
}

/// 字符→6-bit 值反查表，每字母表一张——**刻意不合一**：URL-safe 与 Std 字符
/// 集混用必须显式 Err（共享表会把 `-`/`_` 静默当 `+`/`/` 收下 = 字母表混用
/// 坑）。`0xFF` 哨兵=非法。
static B64_DECODE_STD: [u8; 256] = b64_decode_table(B64_STD);
static B64_DECODE_URL_SAFE: [u8; 256] = b64_decode_table(B64_URL_SAFE);

/// 编码字节为 base64 文本。pad 与否显式选择（RFC §3.2：默认应带 pad，no-pad
/// 是显式变体——如 URL 场景免百分号编码）；尾组未用位恒零填充（RFC §3.5
/// canonical 承诺）。
///
/// 性能锚（2026-10-16 批 encode，release/std Instant 粗测，详见
/// reports/2026-10-16-b1-encode.md）：有效路径恒分配（输出 String，语义必
/// 然），~1.23µs @1KiB（pad/no-pad 无差）/ ~710-800 MiB/s @≥64KiB。
///
/// # Examples
///
/// ```
/// use std_ext::encode::{base64_encode, Base64Alphabet};
///
/// assert_eq!(base64_encode(b"foobar", Base64Alphabet::Std, true), "Zm9vYmFy");
/// assert_eq!(base64_encode(b"foob", Base64Alphabet::Std, false), "Zm9vYg");
/// assert_eq!(base64_encode(&[0xFF, 0xFF], Base64Alphabet::UrlSafe, true), "__8=");
/// ```
pub fn base64_encode(bytes: &[u8], alphabet: Base64Alphabet, padding: bool) -> String {
    let table = match alphabet {
        Base64Alphabet::Std => B64_STD,
        Base64Alphabet::UrlSafe => B64_URL_SAFE,
    };
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    // SAFETY: 只写入 base64 字母表 ASCII 字符与 `=`，String 不变量保持。
    let buf = unsafe { out.as_mut_vec() };
    let full = bytes.len() - bytes.len() % 3;
    let mut i = 0;
    while i < full {
        let n =
            (u32::from(bytes[i]) << 16) | (u32::from(bytes[i + 1]) << 8) | u32::from(bytes[i + 2]);
        buf.push(table[(n >> 18) as usize]);
        buf.push(table[((n >> 12) & 0x3F) as usize]);
        buf.push(table[((n >> 6) & 0x3F) as usize]);
        buf.push(table[(n & 0x3F) as usize]);
        i += 3;
    }
    // 尾组：未用低位零填充（编码端 canonical 承诺，RFC §3.5）。
    match bytes.len() - full {
        0 => {}
        1 => {
            let n = u32::from(bytes[full]) << 16;
            buf.push(table[(n >> 18) as usize]);
            buf.push(table[((n >> 12) & 0x3F) as usize]);
            if padding {
                buf.extend_from_slice(b"==");
            }
        }
        _ => {
            let n = (u32::from(bytes[full]) << 16) | (u32::from(bytes[full + 1]) << 8);
            buf.push(table[(n >> 18) as usize]);
            buf.push(table[((n >> 12) & 0x3F) as usize]);
            buf.push(table[((n >> 6) & 0x3F) as usize]);
            if padding {
                buf.push(b'=');
            }
        }
    }
    out
}

/// 解码 base64 文本为字节。pad 形态按输入验证：带 pad（数量/位置须与尾组匹
/// 配）与 no-pad 双形态皆收；非规范尾位拒绝（见 [`Base64Error`]）。错误优先
/// 序：InvalidLength → InvalidPadding → InvalidByte → NonCanonicalTail。
///
/// 性能锚（同上报告）：恒分配（输出 Vec），~1.22µs @1KiB / ~740-800 MiB/s
/// @≥64KiB。
///
/// # Examples
///
/// ```
/// use std_ext::encode::{base64_decode, Base64Alphabet};
///
/// assert_eq!(base64_decode("Zm9vYg==", Base64Alphabet::Std).unwrap(), b"foob");
/// assert_eq!(base64_decode("Zm9vYg", Base64Alphabet::Std).unwrap(), b"foob");
/// ```
pub fn base64_decode(s: &str, alphabet: Base64Alphabet) -> Result<Vec<u8>, Base64Error> {
    let table = match alphabet {
        Base64Alphabet::Std => &B64_DECODE_STD,
        Base64Alphabet::UrlSafe => &B64_DECODE_URL_SAFE,
    };
    let bytes = s.as_bytes();
    // '=' 只在尾部合法；中段 '=' 不属字母表，主扫描时按 InvalidByte 报。
    let pad = bytes.iter().rev().take_while(|&&c| c == b'=').count();
    let core_len = bytes.len() - pad;
    // 尾组形态 × pad 数量联合验证（RFC §4 形态 (1)(2)(3)；%4==1 恒非法——
    // 6 bits 不足一字节）。
    match (core_len % 4, pad) {
        (0, 0) | (2, 0) | (3, 0) | (2, 2) | (3, 1) => {}
        (1, _) => return Err(Base64Error::InvalidLength { len: bytes.len() }),
        _ => return Err(Base64Error::InvalidPadding { at: core_len }),
    }
    let tail_out = match core_len % 4 {
        2 => 1usize,
        3 => 2,
        _ => 0,
    };
    let mut out = Vec::with_capacity(core_len / 4 * 3 + tail_out);
    let mut i = 0;
    while i + 4 <= core_len {
        let c0 = table[bytes[i] as usize];
        if c0 == HEX_INVALID {
            return Err(Base64Error::InvalidByte {
                at: i,
                byte: bytes[i],
            });
        }
        let c1 = table[bytes[i + 1] as usize];
        if c1 == HEX_INVALID {
            return Err(Base64Error::InvalidByte {
                at: i + 1,
                byte: bytes[i + 1],
            });
        }
        let c2 = table[bytes[i + 2] as usize];
        if c2 == HEX_INVALID {
            return Err(Base64Error::InvalidByte {
                at: i + 2,
                byte: bytes[i + 2],
            });
        }
        let c3 = table[bytes[i + 3] as usize];
        if c3 == HEX_INVALID {
            return Err(Base64Error::InvalidByte {
                at: i + 3,
                byte: bytes[i + 3],
            });
        }
        let n =
            (u32::from(c0) << 18) | (u32::from(c1) << 12) | (u32::from(c2) << 6) | u32::from(c3);
        out.push((n >> 16) as u8);
        out.push((n >> 8) as u8);
        out.push(n as u8);
        i += 4;
    }
    // 尾组：值转换同查表；未用位非零 = 非规范输入，拒绝（RFC §3.5 拍板）。
    match core_len - i {
        0 => {}
        2 => {
            let c0 = table[bytes[i] as usize];
            if c0 == HEX_INVALID {
                return Err(Base64Error::InvalidByte {
                    at: i,
                    byte: bytes[i],
                });
            }
            let c1 = table[bytes[i + 1] as usize];
            if c1 == HEX_INVALID {
                return Err(Base64Error::InvalidByte {
                    at: i + 1,
                    byte: bytes[i + 1],
                });
            }
            if c1 & 0x0F != 0 {
                return Err(Base64Error::NonCanonicalTail);
            }
            out.push(c0 << 2 | c1 >> 4);
        }
        _ => {
            let c0 = table[bytes[i] as usize];
            if c0 == HEX_INVALID {
                return Err(Base64Error::InvalidByte {
                    at: i,
                    byte: bytes[i],
                });
            }
            let c1 = table[bytes[i + 1] as usize];
            if c1 == HEX_INVALID {
                return Err(Base64Error::InvalidByte {
                    at: i + 1,
                    byte: bytes[i + 1],
                });
            }
            let c2 = table[bytes[i + 2] as usize];
            if c2 == HEX_INVALID {
                return Err(Base64Error::InvalidByte {
                    at: i + 2,
                    byte: bytes[i + 2],
                });
            }
            if c2 & 0x03 != 0 {
                return Err(Base64Error::NonCanonicalTail);
            }
            out.push(c0 << 2 | c1 >> 4);
            out.push((c1 & 0x0F) << 4 | c2 >> 2);
        }
    }
    Ok(out)
}

// ── BOM ──

/// 剥离 UTF-8 BOM 前缀（`EF BB BF`），至多一层：双 BOM 输入只剥第一个，第二
/// 个属正文（循环剥会把正文中的 U+FEFF 一起毁掉）。UTF-16 BOM（`FF FE`/`FE
/// FF`）不属本函数——那是端序声明，语义归 [`decode_utf16_bytes`]。零分配
/// （切片操作）。
///
/// 性能锚（2026-10-16 批 encode，release/std Instant 粗测，详见
/// reports/2026-10-16-b1-encode.md）：零分配，~1.5-1.7ns（输入规模钝感）。
///
/// # Examples
///
/// ```
/// use std_ext::encode::strip_bom;
///
/// assert_eq!(strip_bom(b"\xEF\xBB\xBFhi"), b"hi");
/// assert_eq!(strip_bom(b"hi"), b"hi");
/// assert_eq!(strip_bom(b"\xEF\xBB\xBF\xEF\xBB\xBFhi"), b"\xEF\xBB\xBFhi");
/// ```
pub fn strip_bom(bytes: &[u8]) -> &[u8] {
    bytes.strip_prefix(b"\xEF\xBB\xBF").unwrap_or(bytes)
}

/// 字节前缀嗅探出的 Unicode 编码方案（仅看前 2-3 字节，不做内容推断）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnicodeScheme {
    /// UTF-8 BOM（`EF BB BF`）。
    Utf8,
    /// UTF-16LE BOM（`FF FE`）。
    Utf16Le,
    /// UTF-16BE BOM（`FE FF`）。
    Utf16Be,
    /// 无 BOM 前缀。注意：`FF FE 00 00` 是 UTF-32LE BOM 前缀，本域不做
    /// UTF-32，仍归 [`UnicodeScheme::Utf16Le`]。
    None,
}

/// 按字节前缀嗅探 Unicode 编码方案（仅看前 2-3 字节；`FF FE 00 00` 这类
/// UTF-32LE BOM 前缀不识别，仍报 [`UnicodeScheme::Utf16Le`]——本域不做
/// UTF-32）。
///
/// 性能锚（2026-10-16 批 encode，release/std Instant 粗测，详见
/// reports/2026-10-16-b1-encode.md）：零分配，~1.1ns（输入规模钝感）。
///
/// # Examples
///
/// ```
/// use std_ext::encode::{detect_unicode_scheme, UnicodeScheme};
///
/// assert_eq!(detect_unicode_scheme(b"\xEF\xBB\xBFx"), UnicodeScheme::Utf8);
/// assert_eq!(detect_unicode_scheme(b"\xFF\xFE\x2D\x4E"), UnicodeScheme::Utf16Le);
/// assert_eq!(detect_unicode_scheme(b"plain"), UnicodeScheme::None);
/// ```
pub fn detect_unicode_scheme(bytes: &[u8]) -> UnicodeScheme {
    match bytes {
        [0xEF, 0xBB, 0xBF, ..] => UnicodeScheme::Utf8,
        [0xFF, 0xFE, ..] => UnicodeScheme::Utf16Le,
        [0xFE, 0xFF, ..] => UnicodeScheme::Utf16Be,
        _ => UnicodeScheme::None,
    }
}

// ── UTF-16 ──

/// UTF-16 端序：显式选择，无默认（无 BOM 的 UTF-16 字节流端序不可判定）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Utf16Endianness {
    /// 小端（x86/Windows 惯例）。
    Le,
    /// 大端（网络/部分协议惯例）。
    Be,
}

/// UTF-16 解码失败。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Utf16Error {
    /// 字节长度为奇数：不足完整码元。`len` = 输入字节长度。
    OddLength {
        /// 输入字节长度。
        len: usize,
    },
    /// 字节流带 BOM 且与显式端序参数矛盾（fail-visible：不静默按 BOM 翻转）。
    /// `found` = BOM 声明的端序。
    BomMismatch {
        /// BOM 声明的端序。
        found: Utf16Endianness,
    },
    /// 孤立代理项（unpaired surrogate）：high 后非 low、孤立 low、或结尾悬置
    /// high。Strict 哲学：显式 Err，禁 U+FFFD 静默替换。`at` = 码元起始字节
    /// 偏移。
    UnpairedSurrogate {
        /// 孤立代理项码元的字节偏移。
        at: usize,
    },
}

impl std::fmt::Display for Utf16Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Utf16Error::OddLength { len } => write!(f, "odd utf-16 byte length {len}"),
            Utf16Error::BomMismatch { found } => write!(
                f,
                "utf-16 BOM declares {found:?} but endianness parameter differs"
            ),
            Utf16Error::UnpairedSurrogate { at } => {
                write!(f, "unpaired utf-16 surrogate at byte offset {at}")
            }
        }
    }
}

impl std::error::Error for Utf16Error {}

/// 编码文本为 UTF-16 字节（显式端序）。不写 BOM——是否带 BOM 是采用方协议的
/// 决策，需要时由调用方前置 `FF FE`/`FE FF`（端序已显式，BOM 常量不再经过
/// 本函数二选一）。
///
/// 性能锚（2026-10-16 批 encode，release/std Instant 粗测，详见
/// reports/2026-10-16-b1-encode.md）：恒分配（输出 Vec），~76ns @24 字符。
///
/// # Examples
///
/// ```
/// use std_ext::encode::{encode_utf16_bytes, Utf16Endianness};
///
/// assert_eq!(encode_utf16_bytes("a中", Utf16Endianness::Le), vec![0x61, 0x00, 0x2D, 0x4E]);
/// assert_eq!(encode_utf16_bytes("a中", Utf16Endianness::Be), vec![0x00, 0x61, 0x4E, 0x2D]);
/// ```
pub fn encode_utf16_bytes(s: &str, endianness: Utf16Endianness) -> Vec<u8> {
    // UTF-16 码元 2 字节 ≥ 任何 UTF-8 序列字节长度，容量一次到位。
    let mut out = Vec::with_capacity(s.len() * 2);
    for unit in s.encode_utf16() {
        match endianness {
            Utf16Endianness::Le => out.extend_from_slice(&unit.to_le_bytes()),
            Utf16Endianness::Be => out.extend_from_slice(&unit.to_be_bytes()),
        }
    }
    out
}

/// 解码 UTF-16 字节为文本（显式端序）。BOM 感知：开头 `FF FE`/`FE FF` 与参数
/// 端序一致则剥除后解码，矛盾则 Err（BOM 是端序声明，静默按 BOM 翻转等于让
/// 数据覆盖调用方的显式决策）；无 BOM 按参数端序全量解码。unpaired surrogate
/// 显式 Err。
///
/// 性能锚（同上报告）：恒分配（输出 String），~169ns @76B（含代理项对合成
/// 分支）；BOM 剥除/矛盾判定零额外成本。
///
/// # Examples
///
/// ```
/// use std_ext::encode::{decode_utf16_bytes, Utf16Endianness};
///
/// assert_eq!(decode_utf16_bytes(&[0x61, 0x00, 0x2D, 0x4E], Utf16Endianness::Le).unwrap(), "a中");
/// // BOM 一致：剥除。
/// assert_eq!(
///     decode_utf16_bytes(&[0xFF, 0xFE, 0x61, 0x00], Utf16Endianness::Le).unwrap(),
///     "a"
/// );
/// // BOM 矛盾：Err。
/// assert!(decode_utf16_bytes(&[0xFF, 0xFE, 0x61, 0x00], Utf16Endianness::Be).is_err());
/// ```
pub fn decode_utf16_bytes(bytes: &[u8], endianness: Utf16Endianness) -> Result<String, Utf16Error> {
    // BOM 是端序声明：与显式参数一致则剥除，矛盾则 Err——数据不覆盖调用方
    // 的显式决策（fail-visible，禁静默按 BOM 翻转）。只剥一层：双 BOM 的
    // 第二个属正文。
    let body = match bytes {
        [0xFF, 0xFE, rest @ ..] => {
            if endianness != Utf16Endianness::Le {
                return Err(Utf16Error::BomMismatch {
                    found: Utf16Endianness::Le,
                });
            }
            rest
        }
        [0xFE, 0xFF, rest @ ..] => {
            if endianness != Utf16Endianness::Be {
                return Err(Utf16Error::BomMismatch {
                    found: Utf16Endianness::Be,
                });
            }
            rest
        }
        _ => bytes,
    };
    if !body.len().is_multiple_of(2) {
        return Err(Utf16Error::OddLength { len: bytes.len() });
    }
    // 手动代理项状态机（免中间 u16 Vec 分配）：high 后须接 low，孤立即 Err
    // （Strict 哲学，禁 U+FFFD 静默替换）。
    let mut out = String::with_capacity(body.len() / 2);
    let mut pending_high: Option<u16> = None;
    let mut at = 0;
    for chunk in body.chunks_exact(2) {
        let unit = match endianness {
            Utf16Endianness::Le => u16::from_le_bytes([chunk[0], chunk[1]]),
            Utf16Endianness::Be => u16::from_be_bytes([chunk[0], chunk[1]]),
        };
        if let Some(high) = pending_high {
            if (0xDC00..=0xDFFF).contains(&unit) {
                // 代理项对合成：两段均已落在各自区间，合成值恒为合法标量。
                let c = char::from_u32(
                    0x1_0000 + ((u32::from(high) - 0xD800) << 10) + (u32::from(unit) - 0xDC00),
                )
                .expect("surrogate pair synthesis stays in the scalar range");
                out.push(c);
                pending_high = None;
            } else {
                return Err(Utf16Error::UnpairedSurrogate { at: at - 2 });
            }
        } else if (0xD800..=0xDBFF).contains(&unit) {
            pending_high = Some(unit);
        } else if (0xDC00..=0xDFFF).contains(&unit) {
            return Err(Utf16Error::UnpairedSurrogate { at });
        } else {
            // BMP 非 surrogate：恒为合法标量。
            out.push(char::from_u32(u32::from(unit)).expect("non-surrogate BMP is a scalar"));
        }
        at += 2;
    }
    if pending_high.is_some() {
        return Err(Utf16Error::UnpairedSurrogate { at: at - 2 });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── hex ──

    // ① RFC 4648 §10 BASE16 官方向量（Upper 全 7 条）+ Lower 对照。
    #[test]
    fn hex_encode_rfc4648_base16_vectors() {
        assert_eq!(hex_encode(b"", HexCase::Upper), "");
        assert_eq!(hex_encode(b"f", HexCase::Upper), "66");
        assert_eq!(hex_encode(b"fo", HexCase::Upper), "666F");
        assert_eq!(hex_encode(b"foo", HexCase::Upper), "666F6F");
        assert_eq!(hex_encode(b"foob", HexCase::Upper), "666F6F62");
        assert_eq!(hex_encode(b"fooba", HexCase::Upper), "666F6F6261");
        assert_eq!(hex_encode(b"foobar", HexCase::Upper), "666F6F626172");
        assert_eq!(hex_encode(b"foobar", HexCase::Lower), "666f6f626172");
    }

    // ② 解码大小写不敏感（RFC §8「standard case-insensitive」明文）：逐位混
    //    合大小写同解。
    #[test]
    fn hex_decode_is_case_insensitive() {
        assert_eq!(hex_decode("666F6F62").unwrap(), b"foob");
        assert_eq!(hex_decode("666f6f62").unwrap(), b"foob");
        assert_eq!(hex_decode("666F6f62").unwrap(), b"foob");
        assert_eq!(hex_decode("0A").unwrap(), vec![0x0A]);
    }

    // ③ 奇数长度显式 Err（禁静默截断尾 nibble）。
    #[test]
    fn hex_decode_rejects_odd_length() {
        assert_eq!(hex_decode("666"), Err(HexError::OddLength { len: 3 }));
        assert_eq!(hex_decode("6"), Err(HexError::OddLength { len: 1 }));
    }

    // ④ 非法字符显式 Err 含偏移；空白与嵌入式 NUL 不吞（RFC §3.3 MUST
    //    reject；§12 嵌入 NUL 不得 panic）。
    #[test]
    fn hex_decode_rejects_non_digit_including_whitespace_and_nul() {
        assert_eq!(hex_decode("6g"), Err(HexError::InvalidDigit { at: 1 }));
        assert_eq!(hex_decode("z6"), Err(HexError::InvalidDigit { at: 0 }));
        assert_eq!(hex_decode("66 6FF"), Err(HexError::InvalidDigit { at: 2 }));
        assert_eq!(hex_decode("6\u{0}"), Err(HexError::InvalidDigit { at: 1 }));
    }

    // ⑤ 往返：空/边界值字节。
    #[test]
    fn hex_roundtrip() {
        for bytes in [
            &b""[..],
            &[0x00][..],
            &[0xFF][..],
            b"\x00\x7f\x80\xff abc \xe4\xb8\xad",
        ] {
            assert_eq!(
                hex_decode(&hex_encode(bytes, HexCase::Lower)).unwrap(),
                bytes
            );
            assert_eq!(
                hex_decode(&hex_encode(bytes, HexCase::Upper)).unwrap(),
                bytes
            );
        }
    }

    // ── base64 ──

    // ⑥ RFC 4648 §10 BASE64 官方向量（Std+pad 全 7 条）。
    #[test]
    fn base64_encode_rfc4648_vectors() {
        assert_eq!(base64_encode(b"", Base64Alphabet::Std, true), "");
        assert_eq!(base64_encode(b"f", Base64Alphabet::Std, true), "Zg==");
        assert_eq!(base64_encode(b"fo", Base64Alphabet::Std, true), "Zm8=");
        assert_eq!(base64_encode(b"foo", Base64Alphabet::Std, true), "Zm9v");
        assert_eq!(
            base64_encode(b"foob", Base64Alphabet::Std, true),
            "Zm9vYg=="
        );
        assert_eq!(
            base64_encode(b"fooba", Base64Alphabet::Std, true),
            "Zm9vYmE="
        );
        assert_eq!(
            base64_encode(b"foobar", Base64Alphabet::Std, true),
            "Zm9vYmFy"
        );
    }

    // ⑦ no-pad 变体（RFC §3.2：no-pad 需显式选择）；尾组零填充恒 canonical。
    #[test]
    fn base64_encode_no_pad_variant() {
        assert_eq!(
            base64_encode(b"foobar", Base64Alphabet::Std, false),
            "Zm9vYmFy"
        );
        assert_eq!(base64_encode(b"foob", Base64Alphabet::Std, false), "Zm9vYg");
        assert_eq!(base64_encode(b"fo", Base64Alphabet::Std, false), "Zm8");
        assert_eq!(base64_encode(b"f", Base64Alphabet::Std, false), "Zg");
    }

    // ⑧ URL-safe 字母表：62/63 位换 `-`/`_`（RFC Table 2），其余同 Std。
    #[test]
    fn base64_url_safe_alphabet() {
        assert_eq!(
            base64_encode(&[0xFF, 0xFF], Base64Alphabet::Std, true),
            "//8="
        );
        assert_eq!(
            base64_encode(&[0xFF, 0xFF], Base64Alphabet::UrlSafe, true),
            "__8="
        );
        assert_eq!(
            base64_encode(&[0xFF, 0xFF, 0xFE], Base64Alphabet::Std, false),
            "///+"
        );
        assert_eq!(
            base64_encode(&[0xFF, 0xFF, 0xFE], Base64Alphabet::UrlSafe, false),
            "___-"
        );
    }

    // ⑨ 解码带 pad 与 no-pad 双形态皆收（pad 有无输入自描述，无歧义）。
    #[test]
    fn base64_decode_accepts_padded_and_unpadded() {
        assert_eq!(
            base64_decode("Zm9vYg==", Base64Alphabet::Std).unwrap(),
            b"foob"
        );
        assert_eq!(
            base64_decode("Zm9vYg", Base64Alphabet::Std).unwrap(),
            b"foob"
        );
        assert_eq!(
            base64_decode("Zm9vYmFy", Base64Alphabet::Std).unwrap(),
            b"foobar"
        );
    }

    // ⑩ len%4==1 尾组恒非法（6 bits 不足一字节，RFC §4 无此形态）。
    #[test]
    fn base64_decode_rejects_length_mod_4_eq_1() {
        assert_eq!(
            base64_decode("Zm9vY", Base64Alphabet::Std),
            Err(Base64Error::InvalidLength { len: 5 })
        );
        assert_eq!(
            base64_decode("Z", Base64Alphabet::Std),
            Err(Base64Error::InvalidLength { len: 1 })
        );
    }

    // ⑪ pad 形态错显式 Err：数量/位置不匹配（禁宽松吞 pad）。
    #[test]
    fn base64_decode_rejects_malformed_padding() {
        // 3 字符尾组配 2 pad（应 1 或 0）。
        assert_eq!(
            base64_decode("Zm8==", Base64Alphabet::Std),
            Err(Base64Error::InvalidPadding { at: 3 })
        );
        // 2 字符尾组配 1 pad（应 2 或 0）。
        assert_eq!(
            base64_decode("Zg=", Base64Alphabet::Std),
            Err(Base64Error::InvalidPadding { at: 2 })
        );
        // pad 出现在中段：'=' 不属字母表 → InvalidByte。
        assert_eq!(
            base64_decode("Z=g=", Base64Alphabet::Std),
            Err(Base64Error::InvalidByte { at: 1, byte: b'=' })
        );
        // 过量 pad（RFC §3.3「==="」形态：不忽略，显式 Err）。
        assert_eq!(
            base64_decode("Zg===", Base64Alphabet::Std),
            Err(Base64Error::InvalidPadding { at: 2 })
        );
        // 全 pad 空 core。
        assert_eq!(
            base64_decode("====", Base64Alphabet::Std),
            Err(Base64Error::InvalidPadding { at: 0 })
        );
    }

    // ⑫ 非规范尾位拒绝（RFC §3.5 拍板；base64 crate 0.23 默认同语义）：2 字
    //    符尾组低 4 位须零、3 字符尾组低 2 位须零；规范输入正常解码。
    #[test]
    fn base64_decode_rejects_non_canonical_tail() {
        // "A" 的规范编码："QQ=="（Q=16，低 4 位零）；"QR=="（R=17）尾位非零。
        assert_eq!(base64_decode("QQ==", Base64Alphabet::Std).unwrap(), b"A");
        assert_eq!(
            base64_decode("QR==", Base64Alphabet::Std),
            Err(Base64Error::NonCanonicalTail)
        );
        // "fo" 的 no-pad 编码 "Zm8"（8=60，低 2 位零）；"Zm9"（9=61）尾位 01。
        assert_eq!(base64_decode("Zm8", Base64Alphabet::Std).unwrap(), b"fo");
        assert_eq!(
            base64_decode("Zm9", Base64Alphabet::Std),
            Err(Base64Error::NonCanonicalTail)
        );
    }

    // ⑬ 字母表混用显式 Err（URL-safe 与 Std 字符集不相容，禁静默跨表解码）。
    #[test]
    fn base64_decode_rejects_alphabet_mismatch() {
        assert_eq!(
            base64_decode("__8=", Base64Alphabet::Std),
            Err(Base64Error::InvalidByte { at: 0, byte: b'_' })
        );
        assert_eq!(
            base64_decode("///+", Base64Alphabet::UrlSafe),
            Err(Base64Error::InvalidByte { at: 0, byte: b'/' })
        );
        assert_eq!(
            base64_decode("___-", Base64Alphabet::Std),
            Err(Base64Error::InvalidByte { at: 0, byte: b'_' })
        );
    }

    // ⑭ 空白与嵌入式 NUL 不吞（RFC §3.3；base64 crate FAQ 同向：调用方先清
    //    洗，换行分隔输入=清洗职责不在解码器）。形态 (3,1) 带 pad 串，空白在
    //    core 内先撞 InvalidByte。
    #[test]
    fn base64_decode_rejects_whitespace_and_nul() {
        assert_eq!(
            base64_decode("Zm9v Yg=", Base64Alphabet::Std),
            Err(Base64Error::InvalidByte { at: 4, byte: b' ' })
        );
        assert_eq!(
            base64_decode("Zm9v\nYg=", Base64Alphabet::Std),
            Err(Base64Error::InvalidByte { at: 4, byte: b'\n' })
        );
        assert_eq!(
            base64_decode("Zm9v\0Yg=", Base64Alphabet::Std),
            Err(Base64Error::InvalidByte { at: 4, byte: 0 })
        );
    }

    // ⑮ 往返（空/边界值/多字节）× 字母表 × pad 全组合；空输入恒 Ok 空。
    #[test]
    fn base64_roundtrip_and_empty() {
        for bytes in [
            &b""[..],
            &[0x00][..],
            &[0xFF, 0xFF, 0xFE][..],
            b"\x00\x7f\x80\xff abc \xe4\xb8\xad",
        ] {
            for alphabet in [Base64Alphabet::Std, Base64Alphabet::UrlSafe] {
                for padding in [true, false] {
                    let enc = base64_encode(bytes, alphabet, padding);
                    assert_eq!(base64_decode(&enc, alphabet).unwrap(), bytes, "enc={enc:?}");
                }
            }
        }
    }

    // ── BOM ──

    // ⑯ 只剥一层 UTF-8 BOM（双 BOM 第二个属正文——循环剥毁正文 U+FEFF）；残
    //    缺 BOM/UTF-16 BOM/空输入原样。
    #[test]
    fn strip_bom_removes_single_utf8_bom_only() {
        assert_eq!(strip_bom(b"\xEF\xBB\xBFabc"), b"abc");
        assert_eq!(
            strip_bom(b"\xEF\xBB\xBF\xEF\xBB\xBFabc"),
            b"\xEF\xBB\xBFabc"
        );
        assert_eq!(strip_bom(b"abc"), b"abc");
        assert_eq!(strip_bom(b"\xEF\xBB"), b"\xEF\xBB");
        assert_eq!(strip_bom(b""), b"");
        assert_eq!(strip_bom(b"\xFF\xFE"), b"\xFF\xFE");
    }

    // ⑰ 嗅探仅前 2-3 字节；FF FE 00 00（UTF-32LE BOM 前缀）归 Utf16Le。
    #[test]
    fn detect_unicode_scheme_sniffs_prefix_only() {
        assert_eq!(detect_unicode_scheme(b"\xEF\xBB\xBFx"), UnicodeScheme::Utf8);
        assert_eq!(detect_unicode_scheme(b"\xFF\xFE"), UnicodeScheme::Utf16Le);
        assert_eq!(
            detect_unicode_scheme(b"\xFF\xFE\x00\x00"),
            UnicodeScheme::Utf16Le
        );
        assert_eq!(detect_unicode_scheme(b"\xFE\xFF"), UnicodeScheme::Utf16Be);
        assert_eq!(detect_unicode_scheme(b""), UnicodeScheme::None);
        assert_eq!(detect_unicode_scheme(b"a"), UnicodeScheme::None);
        assert_eq!(detect_unicode_scheme(b"\xEF\xBB"), UnicodeScheme::None);
    }

    // ── UTF-16 ──

    // ⑱ 端序显式（无默认）；不写 BOM。
    #[test]
    fn encode_utf16_bytes_is_explicitly_endianned() {
        assert_eq!(
            encode_utf16_bytes("a中", Utf16Endianness::Le),
            vec![0x61, 0x00, 0x2D, 0x4E]
        );
        assert_eq!(
            encode_utf16_bytes("a中", Utf16Endianness::Be),
            vec![0x00, 0x61, 0x4E, 0x2D]
        );
        assert_eq!(
            encode_utf16_bytes("", Utf16Endianness::Le),
            Vec::<u8>::new()
        );
    }

    // ⑲ 往返：BMP + astral 代理项对 + 空 × 双端序。
    #[test]
    fn utf16_roundtrip_bmp_and_astral() {
        for s in ["", "a", "中文", "a\u{1F600}b"] {
            for e in [Utf16Endianness::Le, Utf16Endianness::Be] {
                assert_eq!(decode_utf16_bytes(&encode_utf16_bytes(s, e), e).unwrap(), s);
            }
        }
    }

    // ⑳ BOM 一致剥除 / 矛盾 Err（BOM=端序声明；矛盾 fail-visible，禁静默按
    //    BOM 翻转）。
    #[test]
    fn decode_utf16_bom_consistency() {
        let le = encode_utf16_bytes("中", Utf16Endianness::Le);
        let be = encode_utf16_bytes("中", Utf16Endianness::Be);
        let mut le_bombed = vec![0xFF, 0xFE];
        le_bombed.extend_from_slice(&le);
        let mut be_bombed = vec![0xFE, 0xFF];
        be_bombed.extend_from_slice(&be);
        assert_eq!(
            decode_utf16_bytes(&le_bombed, Utf16Endianness::Le).unwrap(),
            "中"
        );
        assert_eq!(
            decode_utf16_bytes(&be_bombed, Utf16Endianness::Be).unwrap(),
            "中"
        );
        assert_eq!(
            decode_utf16_bytes(&le_bombed, Utf16Endianness::Be),
            Err(Utf16Error::BomMismatch {
                found: Utf16Endianness::Le
            })
        );
        assert_eq!(
            decode_utf16_bytes(&be_bombed, Utf16Endianness::Le),
            Err(Utf16Error::BomMismatch {
                found: Utf16Endianness::Be
            })
        );
    }

    // ㉑ 奇数字节长度 / 三类孤立代理项显式 Err（Strict 哲学，禁 U+FFFD 静默
    //    替换）。
    #[test]
    fn decode_utf16_rejects_odd_length_and_unpaired_surrogates() {
        assert_eq!(
            decode_utf16_bytes(&[0x61], Utf16Endianness::Le),
            Err(Utf16Error::OddLength { len: 1 })
        );
        // 孤立 high（U+D800 LE = 00 D8）后跟 'a'。
        assert_eq!(
            decode_utf16_bytes(&[0x00, 0xD8, 0x61, 0x00], Utf16Endianness::Le),
            Err(Utf16Error::UnpairedSurrogate { at: 0 })
        );
        // 孤立 low（U+DC00 LE = 00 DC）。
        assert_eq!(
            decode_utf16_bytes(&[0x61, 0x00, 0x00, 0xDC], Utf16Endianness::Le),
            Err(Utf16Error::UnpairedSurrogate { at: 2 })
        );
        // 结尾悬置 high。
        assert_eq!(
            decode_utf16_bytes(&[0x61, 0x00, 0x3D, 0xD8], Utf16Endianness::Le),
            Err(Utf16Error::UnpairedSurrogate { at: 2 })
        );
    }

    // ㉒ Display 文案契约（text 域先例：错误文案逐字对齐）。
    #[test]
    fn error_display_contracts() {
        assert_eq!(
            HexError::OddLength { len: 3 }.to_string(),
            "odd hex length 3"
        );
        assert_eq!(
            HexError::InvalidDigit { at: 7 }.to_string(),
            "invalid hex digit at offset 7"
        );
        assert_eq!(
            Base64Error::InvalidByte { at: 4, byte: b'\n' }.to_string(),
            "invalid base64 byte 0x0A at offset 4"
        );
        assert_eq!(
            Base64Error::InvalidLength { len: 5 }.to_string(),
            "invalid base64 length 5"
        );
        assert_eq!(
            Base64Error::InvalidPadding { at: 3 }.to_string(),
            "invalid base64 padding at offset 3"
        );
        assert_eq!(
            Base64Error::NonCanonicalTail.to_string(),
            "non-canonical base64 tail bits"
        );
        assert_eq!(
            Utf16Error::OddLength { len: 1 }.to_string(),
            "odd utf-16 byte length 1"
        );
        assert_eq!(
            Utf16Error::BomMismatch {
                found: Utf16Endianness::Le
            }
            .to_string(),
            "utf-16 BOM declares Le but endianness parameter differs"
        );
        assert_eq!(
            Utf16Error::UnpairedSurrogate { at: 2 }.to_string(),
            "unpaired utf-16 surrogate at byte offset 2"
        );
    }
}
