//! 读侧参考实现（规格 §3.2-v6 (d)）：把「未知标签跳过 + 计数 + resync 提示」的
//! 语义**实现一次**，TS/Python/C 镜像读器按同一语义复刻（规范原文即契约，
//! `docs/view-region-spec.md` v1）。读侧契约：遇未知 tag/kind ⇒ 跳过该项 +
//! 计数 + 置 `needs_resync`（宿主据此全量重取）；**禁 panic、禁整区/整批 Err**。
//!
//! 「未知标签的判定权在读侧」（谁更懂自己的 schema 谁判）：区域段的值类型 tag 是
//! 协议自描述层（本读器按 tag 空间判已知）；载荷形状 kind 是 schema 层，判定集
//! 由调用方传入（[`read::decode_batch`] 的 `decls` 声明词表）。
use super::{BindingId, Patch, RegionError, ENTRY_LEN, HEADER_LEN};
use serde_json::Value;
use skeleton::view::BindingDecl;

/// 一区解析结果（缺失者缺席；跳过经计数与提示显形，不静默）。
#[derive(Debug)]
pub struct RegionRead {
    /// 已解出的绑定值（未知 tag/kind 的项缺席）。
    pub values: Vec<(BindingId, Value)>,
    /// 跳过计数（累计只增，降级显形）。
    pub unknown_tags_skipped: u64,
    /// 任一未知标签 ⇒ 宿主应做全量重取。
    pub needs_resync: bool,
}

/// 区域读侧参考读器：按规范 v1 解一段共享区字节（整读拷贝形态；SAB 直读的
/// gen/version 复读重试协议属宿主读侧，见规范「读侧协议」节）。
pub struct RegionReader;

impl RegionReader {
    /// 读一区；遇未知 tag ⇒ 按段长跳过（TLV 长度前缀使跳过恒可能）+ 计数 +
    /// 置 needs_resync；**禁整区失败**。结构错误（magic/截断）走
    /// `Err(RegionError)`——那是协议外字节，不是兼容期未知标签。
    pub fn read(bytes: &[u8]) -> Result<RegionRead, RegionError> {
        if bytes.len() < HEADER_LEN {
            return Err(RegionError::Truncated {
                need: HEADER_LEN,
                got: bytes.len(),
            });
        }
        if &bytes[0..4] != b"UVBR" {
            return Err(RegionError::BadMagic);
        }
        let binding_count =
            u32::from_le_bytes(bytes[18..22].try_into().expect("slice len")) as usize;
        let index_offset =
            u64::from_le_bytes(bytes[22..30].try_into().expect("slice len")) as usize;
        let index_end = index_offset.saturating_add(ENTRY_LEN.saturating_mul(binding_count));
        if bytes.len() < index_end {
            return Err(RegionError::Truncated {
                need: index_end,
                got: bytes.len(),
            });
        }
        let mut out = RegionRead {
            values: Vec::new(),
            unknown_tags_skipped: 0,
            needs_resync: false,
        };
        for i in 0..binding_count {
            let at = index_offset + ENTRY_LEN * i;
            let e = &bytes[at..at + ENTRY_LEN];
            let binding_id = u32::from_le_bytes(e[0..4].try_into().expect("slice len"));
            let off = u64::from_le_bytes(e[8..16].try_into().expect("slice len")) as usize;
            let len = u32::from_le_bytes(e[16..20].try_into().expect("slice len")) as usize;
            if len == 0 {
                continue; // 未编码过（gen 0）：缺席而非错误
            }
            if bytes.len() < off + len {
                return Err(RegionError::Truncated {
                    need: off + len,
                    got: bytes.len(),
                });
            }
            match decode_segment(&bytes[off..off + len]) {
                SegOut::Value(v) => out.values.push((BindingId(binding_id), v)),
                SegOut::Unknown => {
                    out.unknown_tags_skipped += 1;
                    out.needs_resync = true;
                }
            }
        }
        Ok(out)
    }
}

enum SegOut {
    Value(Value),
    Unknown,
}

/// 解一段 TLV（段内可多条：单条 ⇒ 标量，多条 ⇒ 数组；未知 tag 按长度前缀跳过）。
fn decode_segment(seg: &[u8]) -> SegOut {
    let mut values: Vec<Value> = Vec::new();
    let mut unknown = false;
    let mut cur = 0;
    while cur + 5 <= seg.len() {
        let tag = seg[cur];
        let len = u32::from_le_bytes(seg[cur + 1..cur + 5].try_into().expect("slice len")) as usize;
        let payload_end = cur + 5 + len;
        if payload_end > seg.len() {
            return SegOut::Unknown; // 段内截断：按未知处理（计数 + 提示，不崩）
        }
        let payload = &seg[cur + 5..payload_end];
        match tag {
            0 if len == 8 => values.push(Value::from(u64::from_le_bytes(
                payload.try_into().expect("slice len"),
            ))),
            1 if len == 8 => values.push(Value::from(i64::from_le_bytes(
                payload.try_into().expect("slice len"),
            ))),
            2 if len == 8 => values.push(Value::from(f64::from_le_bytes(
                payload.try_into().expect("slice len"),
            ))),
            3 => match std::str::from_utf8(payload) {
                Ok(s) => values.push(Value::from(s)),
                Err(_) => return SegOut::Unknown,
            },
            4 => values.push(Value::Array(
                payload.iter().map(|b| Value::from(*b)).collect(),
            )),
            5 => match serde_json::from_slice::<Value>(payload) {
                Ok(v) => values.push(v),
                Err(_) => return SegOut::Unknown,
            },
            6 => values.push(Value::Null),
            _ => unknown = true, // 0x07-0xFF：保留段/采用方段——本读器不识别 ⇒ 跳过
        }
        cur = payload_end;
    }
    if unknown || values.is_empty() {
        return SegOut::Unknown;
    }
    if values.len() == 1 {
        SegOut::Value(values.pop().expect("len checked"))
    } else {
        SegOut::Value(Value::Array(values))
    }
}

/// JSON 通道解码结果（语义与区域读侧同款）。
#[derive(Debug)]
pub struct JsonBatchRead {
    /// 已解出的补丁（未知 kind 项缺席）。
    pub patches: Vec<Patch>,
    pub unknown_tags_skipped: u64,
    pub needs_resync: bool,
}

/// JSON 通道解码助手：遇未知 `kind` ⇒ 跳过该项 + 计数 + resync 提示（其余项
/// 照常）；未知 `protocol_version` ⇒ 逐项按 `kind` 判定（禁整批失败）。
/// `decls` = 采用方声明词表（[`BindingDecl`]）：kind 判定集 + 路径 `'static`
/// 回挂的唯一合法来源——未知路径 = 未知标签同款降级（计数 + 提示，不崩）。
pub fn decode_batch(batch: &Value, decls: &[BindingDecl]) -> JsonBatchRead {
    let mut out = JsonBatchRead {
        patches: Vec::new(),
        unknown_tags_skipped: 0,
        needs_resync: false,
    };
    let Some(items) = batch.get("patches").and_then(Value::as_array) else {
        out.needs_resync = true;
        return out;
    };
    // 未知信封版本不整批失败：逐项按 kind 判定（未知 kind 走跳过路径，见下）。
    for item in items {
        let kind = item.get("kind").and_then(Value::as_u64).unwrap_or(0) as u16;
        let known = kind == 0 || decls.iter().any(|d| d.kind.0 == kind);
        if !known {
            out.unknown_tags_skipped += 1;
            out.needs_resync = true;
            continue;
        }
        let (Some(path), Some(value)) =
            (item.get("path").and_then(Value::as_str), item.get("value"))
        else {
            out.needs_resync = true;
            continue;
        };
        let Some(decl) = decls.iter().find(|d| d.path == path) else {
            out.unknown_tags_skipped += 1;
            out.needs_resync = true;
            continue;
        };
        out.patches.push(Patch {
            path: decl.path,
            kind: decl.kind,
            value: value.clone(),
        });
    }
    out
}
