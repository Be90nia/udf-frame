//! 读侧参考实现（规格 §5.3 + `docs/view-region-spec.md` v1）：把「未知标签跳过 +
//! 计数 + resync 提示」的语义**实现一次**，TS/Python/C 镜像读器按同一语义复刻。
//!
//! 两个**独立入口**（#171 addendum 第 4 条）：
//!
//! - [`RegionReader::read_snapshot`]：不可变字节快照（整读拷贝形态）。普通 load，
//!   单次扫描，序列必须为 even；结构损坏走 typed `Err(RegionError)`。
//! - [`RegionReader::read_lock_free`]：SAB 直读形态。对 backing 的 8 字节对齐
//!   序列做 **acquire load → 扫描 → acquire load**，前后必须相等且为 even；有界
//!   重试，耗尽 ⇒ 可见的 `RetryExhausted`（写者忙不能把读者拖成死循环）。
//!
//! 版本闸（#187）：`protocol_version` 是 **bootstrap 布局 major**。不支持即返回
//! `UnsupportedVersion`，**不解释任何 v1 偏移/计数/条目**；条目级未知 `kind`/未知
//! TLV `tag` 的跳过只在 major 已支持后适用。
//!
//! 「未知标签的判定权在读侧」（谁更懂自己的 schema 谁判）：区域段的值类型 tag 是
//! 协议自描述层（本读器按 tag 空间判已知）；载荷形状 kind 是 schema 层，判定集
//! 由调用方传入（[`decode_batch`] / [`RegionReader`] 的 `decls` 声明词表）。
use std::panic::{AssertUnwindSafe, catch_unwind};

use serde_json::Value;
use skeleton::view::region_layout::{
    ENTRY_LEN, ENTRY_OFFSET_BINDING_ID, ENTRY_OFFSET_KIND, ENTRY_OFFSET_LEN, ENTRY_OFFSET_OFFSET,
    HEADER_LEN, INDEX_OFFSET_V1, MAGIC, OFFSET_BINDING_COUNT, OFFSET_FLAGS, OFFSET_INDEX_OFFSET,
    OFFSET_MAGIC, OFFSET_PROTOCOL, OFFSET_PUBLISH_SEQ, OFFSET_REGION_ID, SEG_ALIGN, TAG_BYTES,
    TAG_F64, TAG_I64, TAG_JSON, TAG_NULL, TAG_U64, TAG_UTF8, TLV_HEADER_LEN,
};
use skeleton::view::{BindingDecl, PROTOCOL_VERSION};

use super::{BindingId, Patch, RegionBacking, RegionError, SeqError};

/// 一区解析结果（缺失者缺席；跳过经计数与提示显形，不静默）。
#[derive(Debug)]
pub struct RegionRead {
    /// 已解出的绑定值（未知 tag/kind/畸形段整条缺席）。
    pub values: Vec<(BindingId, Value)>,
    /// 跳过计数（累计只增，降级显形）。
    pub unknown_tags_skipped: u64,
    /// 任一未知标签/畸形段 ⇒ 宿主应做全量重取。
    pub needs_resync: bool,
    /// 区域代次（变化即 UI 重映射握手）。
    pub region_id: u32,
    /// 本次读取观察到的**稳定 even** 发布序列：与 `PatchBatch::to_version` /
    /// `Snapshot::version` 同源同值（桥唯一 watermark）。
    pub sequence: u64,
}

/// 逐条目解码故障：**整段失效**（多 TLV 值里坏一条不得把半解数组当完整值，
/// §C），计数一次 + 置 resync；其余独立条目照常保留。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SegFault {
    Truncated,
    BadTagLength,
    BadPayload,
    UnknownTag,
}

/// 区域读侧参考实现。
pub struct RegionReader;

impl RegionReader {
    /// `read_lock_free` 的缺省重试次数（足够跨过任何一次写事务的长度）。
    pub const DEFAULT_RETRIES: u32 = 8;

    /// 不可变字节快照入口：普通 load，单次扫描，序列须为 even（odd = 写事务
    /// 进行中 ⇒ `WriterInProgress`，宿主重取快照或改用 lock-free 入口）。
    ///
    /// 未知 tag/`.kind` ⇒ 跳过该项 + 计数 + `needs_resync`；**禁整区失败**。
    /// 结构损坏（magic/版本/偏移/几何/截断）= 索引不可信 ⇒ `Err(RegionError)`。
    pub fn read_snapshot(bytes: &[u8], decls: &[BindingDecl]) -> Result<RegionRead, RegionError> {
        scan(bytes, decls)
    }

    /// SAB 直读入口：`acquire load → 扫描 → acquire load`，两次相等且为 even 才
    /// 接受（写者 odd 期间或前后变化一律重试）；重试耗尽 ⇒ `RetryExhausted`。
    ///
    /// 与 [`Self::read_snapshot`] 是**两个独立入口**：前者允许普通 load 并对不可变
    /// 快照免重试，后者要求 backing 提供对齐原子序列能力。
    pub fn read_lock_free(
        backing: &dyn RegionBacking,
        decls: &[BindingDecl],
        retries: u32,
    ) -> Result<RegionRead, RegionError> {
        let attempts = retries.max(1);
        for _ in 0..attempts {
            let before = load_seq(backing)?;
            if before % 2 != 0 {
                continue; // odd = 写事务进行中：本次不得接受
            }
            let mut slot: Option<Result<RegionRead, RegionError>> = None;
            let scanned = catch_unwind(AssertUnwindSafe(|| {
                backing.with_bytes(&mut |bytes| {
                    slot = Some(scan(bytes, decls));
                })
            }));
            if scanned.is_err() {
                return Err(RegionError::BackingPanicked { op: "with_bytes" });
            }
            let Some(scanned) = slot else {
                return Err(RegionError::BackingPanicked { op: "with_bytes" });
            };
            let after = load_seq(backing)?;
            if after == before {
                // 前后一致 ⇒ 本次读的是一份完整提交态；结构错误才是真错误。
                let mut read = scanned?;
                read.sequence = after;
                return Ok(read);
            }
        }
        Err(RegionError::RetryExhausted { attempts })
    }
}

fn load_seq(backing: &dyn RegionBacking) -> Result<u64, RegionError> {
    match catch_unwind(AssertUnwindSafe(|| backing.load_publish_seq())) {
        Ok(Ok(seq)) => Ok(seq),
        Ok(Err(SeqError::Unsupported)) => Err(RegionError::SequenceUnavailable),
        Ok(Err(SeqError::Failed)) => Err(RegionError::SequenceUnavailable),
        Err(_) => Err(RegionError::BackingPanicked {
            op: "load_publish_seq",
        }),
    }
}

fn truncated(need: usize, got: usize) -> RegionError {
    RegionError::Truncated { need, got }
}

/// 读一次区域字节（`read_snapshot` / `read_lock_free` 共用的纯扫描）。
fn scan(bytes: &[u8], decls: &[BindingDecl]) -> Result<RegionRead, RegionError> {
    if bytes.len() < HEADER_LEN {
        return Err(truncated(HEADER_LEN, bytes.len()));
    }
    // 布局闸：magic / major / flags 先判——任何 v1 偏移解释都在其后（#187）。
    if bytes[OFFSET_MAGIC..OFFSET_PROTOCOL] != MAGIC {
        return Err(RegionError::BadMagic);
    }
    let major = u16::from_le_bytes(
        bytes[OFFSET_PROTOCOL..OFFSET_FLAGS]
            .try_into()
            .expect("2 bytes"),
    );
    if major != PROTOCOL_VERSION {
        return Err(RegionError::UnsupportedVersion { major });
    }
    let flags = u16::from_le_bytes(
        bytes[OFFSET_FLAGS..OFFSET_PUBLISH_SEQ]
            .try_into()
            .expect("2 bytes"),
    );
    if flags != 0 {
        // v1 冻结：非 0 位意味着布局不再是 v1（不猜、不跳，显形）。
        return Err(RegionError::Malformed {
            detail: "header flags must be zero in v1",
        });
    }
    let sequence = u64::from_le_bytes(
        bytes[OFFSET_PUBLISH_SEQ..OFFSET_REGION_ID]
            .try_into()
            .expect("8 bytes"),
    );
    if sequence % 2 != 0 {
        return Err(RegionError::WriterInProgress { sequence });
    }
    let region_id = u32::from_le_bytes(
        bytes[OFFSET_REGION_ID..OFFSET_BINDING_COUNT]
            .try_into()
            .expect("4 bytes"),
    );
    let count = u32::from_le_bytes(
        bytes[OFFSET_BINDING_COUNT..OFFSET_INDEX_OFFSET]
            .try_into()
            .expect("4 bytes"),
    );
    let index_offset = usize::try_from(u64::from_le_bytes(
        bytes[OFFSET_INDEX_OFFSET..HEADER_LEN]
            .try_into()
            .expect("8 bytes"),
    ))
    .map_err(|_| RegionError::Malformed {
        detail: "index_offset is not representable on this target",
    })?;
    if index_offset != INDEX_OFFSET_V1 as usize {
        return Err(RegionError::Malformed {
            detail: "v1 index_offset must be 32",
        });
    }
    let count = usize::try_from(count).map_err(|_| RegionError::Malformed {
        detail: "binding_count is not representable on this target",
    })?;
    let index_end = count
        .checked_mul(ENTRY_LEN)
        .and_then(|n| n.checked_add(index_offset))
        .ok_or(RegionError::Malformed {
            detail: "index geometry overflows",
        })?;
    if bytes.len() < index_end {
        return Err(truncated(index_end, bytes.len()));
    }

    let mut out = RegionRead {
        values: Vec::new(),
        unknown_tags_skipped: 0,
        needs_resync: false,
        region_id,
        sequence,
    };
    let mut seen: Vec<u32> = Vec::with_capacity(count);
    for i in 0..count {
        let at = index_offset + ENTRY_LEN * i;
        let e = &bytes[at..at + ENTRY_LEN];
        let binding_id = u32::from_le_bytes(
            e[ENTRY_OFFSET_BINDING_ID..ENTRY_OFFSET_KIND]
                .try_into()
                .expect("4 bytes"),
        );
        if seen.contains(&binding_id) {
            return Err(RegionError::Malformed {
                detail: "duplicate BindingId in index",
            });
        }
        seen.push(binding_id);
        let kind = u16::from_le_bytes(
            e[ENTRY_OFFSET_KIND..ENTRY_OFFSET_KIND + 2]
                .try_into()
                .expect("2 bytes"),
        );
        let off = usize::try_from(u64::from_le_bytes(
            e[ENTRY_OFFSET_OFFSET..ENTRY_OFFSET_LEN]
                .try_into()
                .expect("8 bytes"),
        ))
        .map_err(|_| RegionError::Malformed {
            detail: "segment offset is not representable on this target",
        })?;
        let len = u32::from_le_bytes(
            e[ENTRY_OFFSET_LEN..ENTRY_OFFSET_LEN + 4]
                .try_into()
                .expect("4 bytes"),
        ) as usize;
        if len == 0 {
            continue; // 未发布过：缺席而非错误（空输出**不是**删除标记）
        }
        // kind 校验（#154/#186）：kind 判定集 = 采用方声明词表；`0` = 未声明
        // （JSON 自描述语义，与 `decode_batch` 同规则）。
        if kind != 0 && !decls.iter().any(|d| d.kind.0 == kind) {
            out.unknown_tags_skipped += 1;
            out.needs_resync = true;
            continue;
        }
        // 几何校验（#165）：段不得落进 Header/Index，必须 8 字节对齐且不越界。
        if off % SEG_ALIGN != 0 {
            return Err(RegionError::Malformed {
                detail: "segment offset is not 8-byte aligned",
            });
        }
        if off < index_end {
            return Err(RegionError::Malformed {
                detail: "segment overlaps header/index",
            });
        }
        let end = off.checked_add(len).ok_or(RegionError::Malformed {
            detail: "segment end overflows",
        })?;
        let seg = bytes
            .get(off..end)
            .ok_or_else(|| truncated(end, bytes.len()))?;
        match decode_segment(seg) {
            Ok(v) => out.values.push((BindingId(binding_id), v)),
            // 整段失效（§C）：不把半解值当完整值，也不让一条坏段毁掉整区。
            Err(_) => {
                out.unknown_tags_skipped += 1;
                out.needs_resync = true;
            }
        }
    }
    Ok(out)
}

/// 解一段 TLV：必须**恰好消费整段**；任一无损性/合法性违规 ⇒ 整段失效。
fn decode_segment(seg: &[u8]) -> Result<Value, SegFault> {
    let mut values: Vec<Value> = Vec::new();
    let mut cur = 0usize;
    while cur < seg.len() {
        let head = seg
            .get(cur..cur + TLV_HEADER_LEN)
            .ok_or(SegFault::Truncated)?;
        let tag = head[0];
        let len = u32::from_le_bytes(head[1..TLV_HEADER_LEN].try_into().expect("4 bytes")) as usize;
        let start = cur + TLV_HEADER_LEN;
        let end = start.checked_add(len).ok_or(SegFault::Truncated)?;
        let payload = seg.get(start..end).ok_or(SegFault::Truncated)?;
        let v = match tag {
            TAG_U64 => Value::from(u64::from_le_bytes(fixed8(payload, len)?)),
            TAG_I64 => Value::from(i64::from_le_bytes(fixed8(payload, len)?)),
            TAG_F64 => {
                let f = f64::from_le_bytes(fixed8(payload, len)?);
                // #153：非有限值在 JSON 值模型里无法表示，禁静默映射成 null。
                if !f.is_finite() {
                    return Err(SegFault::BadPayload);
                }
                Value::from(f)
            }
            TAG_UTF8 => {
                Value::from(std::str::from_utf8(payload).map_err(|_| SegFault::BadPayload)?)
            }
            TAG_BYTES => Value::Array(payload.iter().map(|b| Value::from(*b)).collect()),
            TAG_JSON => {
                serde_json::from_slice::<Value>(payload).map_err(|_| SegFault::BadPayload)?
            }
            TAG_NULL => {
                if len != 0 {
                    return Err(SegFault::BadTagLength);
                }
                Value::Null
            }
            _ => return Err(SegFault::UnknownTag), // 整段失效（§C）
        };
        values.push(v);
        cur = end;
    }
    match values.len() {
        0 => Err(SegFault::Truncated),
        1 => Ok(values.pop().expect("len checked")),
        _ => Ok(Value::Array(values)),
    }
}

fn fixed8(payload: &[u8], len: usize) -> Result<[u8; 8], SegFault> {
    if len != 8 {
        return Err(SegFault::BadTagLength);
    }
    payload.try_into().map_err(|_| SegFault::BadTagLength)
}

/// JSON 通道解码结果（语义与区域读侧同款）。
#[derive(Debug)]
pub struct JsonBatchRead {
    /// 已解出的补丁（未知 kind / kind 与声明不符的项缺席）。
    pub patches: Vec<Patch>,
    pub unknown_tags_skipped: u64,
    pub needs_resync: bool,
    /// 信封 `protocol_version` 不受支持：**未解释任何条目**（#187）。
    pub unsupported_version: bool,
}

/// JSON 通道解码助手：未知/畸形 `kind` ⇒ 跳过该项 + 计数 + resync 提示（其余项
/// 照常）；不支持的 `protocol_version` ⇒ 协商失败（不解释任何条目）。
/// `decls` = 采用方声明词表（[`BindingDecl`]）：kind 判定集 + 路径 `'static`
/// 回挂的唯一合法来源——未知路径 = 未知标签同款降级（计数 + 提示，不崩）。
pub fn decode_batch(batch: &Value, decls: &[BindingDecl]) -> JsonBatchRead {
    let mut out = JsonBatchRead {
        patches: Vec::new(),
        unknown_tags_skipped: 0,
        needs_resync: false,
        unsupported_version: false,
    };
    // 布局闸：版本缺失/不等值 = 信封不可解释（禁按 v1 条目语义硬解）。
    let version = batch.get("protocol_version").and_then(Value::as_u64);
    if version.and_then(|v| u16::try_from(v).ok()) != Some(PROTOCOL_VERSION) {
        out.unsupported_version = true;
        out.needs_resync = true;
        return out;
    }
    let Some(items) = batch.get("patches").and_then(Value::as_array) else {
        out.needs_resync = true;
        return out;
    };
    for item in items {
        // #135/#166：缺失/非整数/越界的 kind 不再 `unwrap_or(0)` 静默当「未声明」。
        let kind = item
            .get("kind")
            .and_then(Value::as_u64)
            .and_then(|k| u16::try_from(k).ok());
        let Some(kind) = kind else {
            out.unknown_tags_skipped += 1;
            out.needs_resync = true;
            continue;
        };
        let (Some(path), Some(value)) =
            (item.get("path").and_then(Value::as_str), item.get("value"))
        else {
            // 字段缺失 = 项畸形：与区域读侧同口径（计数 + 提示，禁整批失败）。
            out.unknown_tags_skipped += 1;
            out.needs_resync = true;
            continue;
        };
        let Some(decl) = decls.iter().find(|d| d.path == path) else {
            out.unknown_tags_skipped += 1;
            out.needs_resync = true;
            continue;
        };
        // kind 与声明不符（形状身份漂移）⇒ 跳过该项：形状不确定的值不得交付。
        if kind != 0 && kind != decl.kind.0 {
            out.unknown_tags_skipped += 1;
            out.needs_resync = true;
            continue;
        }
        out.patches.push(Patch {
            path: decl.path,
            kind: decl.kind,
            value: value.clone(),
        });
    }
    out
}
