// ViewBridge 区域线格式 v1 —— TS 镜像读取器（`docs/view-region-spec.md` 是唯一权威，
// 本文件是 Rust `skeleton_infra::view::read::RegionReader` 的同一语义复刻）。
//
// 两个**独立入口**（规格 §4）：
// - `readRegionSnapshot`：不可变字节快照（宿主已持有稳定副本）。普通 load、单次扫描，
//   序列必须为 even；odd ⇒ `writerInProgress`。
// - `readRegionLockFree`：SAB 直读。对 8 字节对齐序列做 `Atomics.load` → 扫描 →
//   `Atomics.load`，前后相等且为 even 才接受；odd/变化 ⇒ 有界重试，耗尽按最后一次
//   原子观测分两种可见结局：仍 odd ⇒ `writerInProgress`，even 间持续变化 ⇒
//   `retryExhausted`——写者忙不得把读者拖成死循环。
//
// 判定权分工：序列（even = 已提交水位）是唯一一致性标记；条目 `gen` 只是逻辑修订号，
// **不参与**一致性判定（规格 §1/§4）。lock-free 形态仍对每个条目做 gen 前后复核，但那
// 只当**撕裂探测器**（gen 变化必落在 odd 窗口内，本就被序列拒收），不是第二套协议。
// 值类型 tag 是协议自描述层（本读器按 tag 空间判已知）；载荷形状 `kind` 是 schema 层，
// 判定集由调用方 [declaredKinds] 传入。
//
// 常量一律引生成物 `view_bridge_constants.ts`（manifest 是唯一真源），禁手写偏移字面量。
//
// 本文件与同名 `.test.ts` 必须保持「可擦除语法」（禁 enum / namespace / 参数属性）且相对
// 导入带 `.ts` 后缀：Node 直接剥离类型运行 `node --test docs/view-region-reader.test.ts`。

import {
  ENTRY_LEN,
  ENTRY_OFFSET_BINDING_ID,
  ENTRY_OFFSET_GEN,
  ENTRY_OFFSET_KIND,
  ENTRY_OFFSET_LEN,
  ENTRY_OFFSET_OFFSET,
  HEADER_LEN,
  INDEX_OFFSET_V1,
  OFFSET_BINDING_COUNT,
  OFFSET_FLAGS,
  OFFSET_INDEX_OFFSET,
  OFFSET_MAGIC,
  OFFSET_PROTOCOL,
  OFFSET_PUBLISH_SEQ,
  OFFSET_REGION_ID,
  PROTOCOL_VERSION,
  SEG_ALIGN,
  TLV_HEADER_LEN,
} from "./view-bridge-constants/view_bridge_constants.ts";

/** 值类型 tag 空间（规格 §1；只增不改、退役号不复用）。 */
const TAG_U64 = 0;
const TAG_I64 = 1;
const TAG_F64 = 2;
const TAG_UTF8 = 3;
const TAG_BYTES = 4;
const TAG_JSON = 5;
const TAG_NULL = 6;

const MAGIC = [0x55, 0x56, 0x42, 0x52]; // ASCII "UVBR"

/** `readRegionLockFree` 的缺省重试次数（与 Rust `RegionReader::DEFAULT_RETRIES` 同值）。 */
export const DEFAULT_RETRIES = 8;

export type RegionErrorKind =
  | "badMagic"
  | "unsupportedVersion"
  | "truncated"
  | "malformed"
  | "writerInProgress"
  | "retryExhausted";

/**
 * 协议外/结构损坏（索引不可信 ⇒ 整区不采信）。逐条目降级**不走**此处：未知
 * `kind`/不可解 TLV 只跳过该项 + 计数 + `needsResync`。
 */
export class RegionError extends Error {
  readonly kind: RegionErrorKind;
  /** `unsupportedVersion`：协商失败的 major。 */
  readonly major: number | undefined;
  /** `writerInProgress`：观察到的 odd 序列。 */
  readonly sequence: bigint | undefined;
  /** `retryExhausted`：已用尽的尝试次数。 */
  readonly attempts: number | undefined;

  constructor(
    kind: RegionErrorKind,
    message: string,
    extra: { major?: number; sequence?: bigint; attempts?: number } = {},
  ) {
    super(message);
    this.name = "RegionError";
    this.kind = kind;
    this.major = extra.major;
    this.sequence = extra.sequence;
    this.attempts = extra.attempts;
  }
}

/** 一区解析结果（缺失者缺席；跳过经计数与提示显形，不静默）。 */
export interface RegionRead {
  /** 已解出的绑定值（未知 kind/不可解段整条缺席）。 */
  values: Map<number, unknown>;
  /** 跳过计数（累计只增）。 */
  unknownTagsSkipped: number;
  /** 任一未知标签/畸形段 ⇒ 宿主应做全量重取。 */
  needsResync: boolean;
  /** 区域代次（变化即 UI 重映射握手）。 */
  regionId: number;
  /** 本次读取观察到的稳定 even 发布序列（桥唯一 watermark）。 */
  sequence: bigint;
  /** bootstrap 布局 major（恒为 `PROTOCOL_VERSION`；不等值时本读器不解释任何 v1 偏移）。 */
  protocolVersion: number;
}

/**
 * lock-free 入口的最小背衬契约（规格 §8）：`loadSequence` 必须是与 Header 字节
 * `8..16` **同一共享内存位置**的 8 字节对齐原子读（`Atomics.load`）；字节拷贝
 * 不等价于原子读，不得用作降级实现。
 */
export interface RegionBacking {
  withBytes<T>(read: (bytes: Uint8Array) => T): T;
  loadSequence(): bigint;
}

/** 标准 SAB 形背衬：整区视图 + 同一位置的 `BigUint64Array` 序列视图。 */
export function sabBacking(sab: SharedArrayBuffer): RegionBacking {
  if (sab.byteLength < OFFSET_PUBLISH_SEQ + 8) {
    throw truncated(HEADER_LEN, sab.byteLength);
  }
  const bytes = new Uint8Array(sab);
  const sequence = new BigUint64Array(sab, OFFSET_PUBLISH_SEQ, 1);
  return {
    withBytes: (read) => read(bytes),
    loadSequence: () => Atomics.load(sequence, 0),
  };
}

/**
 * 不可变字节快照入口：普通 load、单次扫描。序列 odd ⇒ `writerInProgress`（宿主重取
 * 快照或改用 lock-free 入口）。结构损坏 ⇒ typed `RegionError`。
 */
export function readRegionSnapshot(
  bytes: Uint8Array,
  declaredKinds: readonly number[],
): RegionRead {
  return scan(bytes, declaredKinds);
}

/**
 * SAB 直读入口：`load` → 扫描 → `load`，两次相等且为 even 才接受。前后不等或条目
 * `gen` 在扫描期间变化 ⇒ 本次字节跨在提交窗口里，重试；耗尽按最后一次原子观测给出
 * `writerInProgress`（仍 odd）或 `retryExhausted`（even 间持续变化）。
 */
export function readRegionLockFree(
  backing: RegionBacking,
  declaredKinds: readonly number[],
  retries: number = DEFAULT_RETRIES,
): RegionRead {
  const attempts = Math.max(1, retries);
  let lastSeen: bigint | undefined;
  for (let i = 0; i < attempts; i++) {
    const before = backing.loadSequence();
    lastSeen = before;
    if (before % 2n !== 0n) continue; // odd = 写事务进行中：本次不得接受
    let failure: RegionError | null = null;
    let read: RegionRead | undefined;
    try {
      // 序列字不进扫描：SAB 形下字节副本可能被撕裂，判定权只给原子序列。
      read = backing.withBytes((bytes) => scan(bytes, declaredKinds, before));
    } catch (error) {
      // 条目 gen 在扫描期间变化 = 这份字节不再是同一提交态 ⇒ 本次作废（重试）。
      if (error instanceof TornSnapshot) continue;
      if (!(error instanceof RegionError)) throw error; // 宿主缺陷，不是数据问题
      failure = error;
    }
    if (backing.loadSequence() !== before) continue;
    if (failure !== null) throw failure; // 稳定窗口下的结构错误才是真错误
    if (read === undefined) {
      throw new RegionError("malformed", "region backing produced no bytes");
    }
    return read;
  }
  // 耗尽按最后一次原子观测分类（规格 §4⑥）：仍是 odd ⇒ 写者在途；一直在 even 之间
  // 变化 ⇒ 持续提交。两种结局都可见，写者忙不得把读者拖成死循环。
  if (lastSeen !== undefined && lastSeen % 2n !== 0n) {
    throw new RegionError(
      "writerInProgress",
      `region publication in progress (odd sequence ${lastSeen})`,
      { sequence: lastSeen },
    );
  }
  throw new RegionError(
    "retryExhausted",
    `region writer busy: ${attempts} retries exhausted`,
    { attempts },
  );
}

/** 头几何（§6 布局闸）：magic / major / flags 先判——任何 v1 偏移解释都在其后。 */
function readHeader(
  bytes: Uint8Array,
  stableSequence?: bigint,
): { protocolVersion: number; sequence: bigint; regionId: number; count: number; indexEnd: number } {
  if (bytes.length < HEADER_LEN) throw truncated(HEADER_LEN, bytes.length);
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  for (let i = 0; i < MAGIC.length; i++) {
    if (view.getUint8(OFFSET_MAGIC + i) !== MAGIC[i]) {
      throw new RegionError("badMagic", 'region magic mismatch (expect "UVBR")');
    }
  }
  const major = view.getUint16(OFFSET_PROTOCOL, true);
  if (major !== PROTOCOL_VERSION) {
    throw new RegionError(
      "unsupportedVersion",
      `unsupported protocol major ${major} (this reader supports ${PROTOCOL_VERSION})`,
      { major },
    );
  }
  const flags = view.getUint16(OFFSET_FLAGS, true);
  if (flags !== 0) {
    // v1 冻结：非 0 位意味着布局不再是 v1（不猜、不跳，显形）。
    throw new RegionError("malformed", "header flags must be zero in v1");
  }
  const sequence = stableSequence ?? view.getBigUint64(OFFSET_PUBLISH_SEQ, true);
  if (sequence % 2n !== 0n) {
    throw new RegionError(
      "writerInProgress",
      `region publication in progress (odd sequence ${sequence})`,
      { sequence },
    );
  }
  const regionId = view.getUint32(OFFSET_REGION_ID, true);
  const count = view.getUint32(OFFSET_BINDING_COUNT, true);
  const indexOffset = view.getBigUint64(OFFSET_INDEX_OFFSET, true);
  if (indexOffset !== BigInt(INDEX_OFFSET_V1)) {
    throw new RegionError("malformed", "v1 index_offset must be 32");
  }
  // count 是 u32：count × 24 ≤ 2^32 × 24 < 2^53，无精度损失。
  const indexEnd = INDEX_OFFSET_V1 + count * ENTRY_LEN;
  if (bytes.length < indexEnd) throw truncated(indexEnd, bytes.length);
  return { protocolVersion: major, sequence, regionId, count, indexEnd };
}

/**
 * 单次扫描（两个入口共用）。`stableSequence` = lock-free 入口原子读到的 even 水印；
 * 缺省时从字节读（快照入口）。
 */
function scan(
  bytes: Uint8Array,
  declaredKinds: readonly number[],
  stableSequence?: bigint,
): RegionRead {
  const { protocolVersion, sequence, regionId, count, indexEnd } = readHeader(bytes, stableSequence);
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const out: RegionRead = {
    values: new Map(),
    unknownTagsSkipped: 0,
    needsResync: false,
    regionId,
    sequence,
    protocolVersion,
  };
  // 条目 gen 复核只在 lock-free 形态做（有并发写者）；快照形态按规格 §4 单遍读，
  // 不可变字节即真相。
  const recheckGen = stableSequence !== undefined;
  const seen = new Set<number>();
  for (let i = 0; i < count; i++) {
    const at = INDEX_OFFSET_V1 + ENTRY_LEN * i;
    // gen 前读：与段解码之后的复核配成撕裂探测，窗口覆盖本条目全部字段与载荷字节。
    const genBefore = recheckGen ? view.getUint32(at + ENTRY_OFFSET_GEN, true) : 0;
    const bindingId = view.getUint32(at + ENTRY_OFFSET_BINDING_ID, true);
    if (seen.has(bindingId)) throw new RegionError("malformed", "duplicate BindingId in index");
    seen.add(bindingId);
    const kind = view.getUint16(at + ENTRY_OFFSET_KIND, true);
    const offset = view.getBigUint64(at + ENTRY_OFFSET_OFFSET, true);
    const len = view.getUint32(at + ENTRY_OFFSET_LEN, true);
    if (len === 0) continue; // 未发布过：缺席而非错误（空输出**不是**删除标记）
    // kind 校验：kind 判定集 = 采用方声明词表；`0` = 未声明（自描述语义）。
    if (kind !== 0 && !declaredKinds.includes(kind)) {
      out.unknownTagsSkipped += 1;
      out.needsResync = true;
      continue;
    }
    // 几何校验：段不得落进 Header/Index，必须 8 字节对齐且不越界。
    if (offset % BigInt(SEG_ALIGN) !== 0n) {
      throw new RegionError("malformed", "segment offset is not 8-byte aligned");
    }
    if (offset < BigInt(indexEnd)) {
      throw new RegionError("malformed", "segment overlaps header/index");
    }
    if (!Number.isSafeInteger(Number(offset))) {
      throw new RegionError("malformed", "segment offset is not representable");
    }
    const start = Number(offset);
    const end = start + len;
    // checked 算术：safe-int 偏移 + u32 长度仍可能越出安全整数域（禁先截断再比较）。
    if (!Number.isSafeInteger(end)) {
      throw new RegionError("malformed", "segment end is not representable");
    }
    if (end > bytes.length) throw truncated(end, bytes.length);
    try {
      out.values.set(bindingId, decodeSegment(bytes.subarray(start, end)));
    } catch (error) {
      // 整段失效：不把半解值当完整值，也不让一条坏段毁掉整区。
      if (!(error instanceof SegFault)) throw error;
      out.unknownTagsSkipped += 1;
      out.needsResync = true;
    }
    // gen 后读：与前读不等 ⇒ 本快照在扫描期间被改写，整次作废（上层重试）。这只是
    // 撕裂探测器，不是第二套一致性协议——判定权仍在原子序列（规格 §4）。
    if (recheckGen && view.getUint32(at + ENTRY_OFFSET_GEN, true) !== genBefore) {
      throw new TornSnapshot();
    }
  }
  return out;
}

/** 条目 `gen` 在扫描期间变化（内部信号；lock-free 入口据此重试整次读）。 */
class TornSnapshot extends Error {}

/** 段内解码故障（内部信号；对外一律折成「整段失效 + 计数 + resync」）。 */
class SegFault extends Error {}

const utf8 = new TextDecoder("utf-8", { fatal: true }); // 非法 UTF-8 必须被拒，禁替换字符

/** 解一段 TLV：必须**恰好消费整段**；任一不可解 ⇒ 整段失效（§7.2）。 */
function decodeSegment(seg: Uint8Array): unknown {
  const view = new DataView(seg.buffer, seg.byteOffset, seg.byteLength);
  const values: unknown[] = [];
  let cur = 0;
  while (cur < seg.length) {
    if (cur + TLV_HEADER_LEN > seg.length) throw new SegFault("truncated TLV header");
    const tag = view.getUint8(cur);
    // TLV = tag:u8 | len:u32(LE)——长度按无符号读，禁 bitwise 拼装（高位会被当符号位）。
    const len = view.getUint32(cur + 1, true);
    const payloadAt = cur + TLV_HEADER_LEN;
    const end = payloadAt + len;
    if (end > seg.length) throw new SegFault("truncated TLV payload");
    values.push(decodePayload(tag, seg, payloadAt, len));
    cur = end;
  }
  if (values.length === 0) throw new SegFault("empty segment");
  return values.length === 1 ? values[0] : values;
}

function decodePayload(tag: number, seg: Uint8Array, at: number, len: number): unknown {
  // DataView 必须带 byteOffset：`subarray` 保留原背衬，缺省偏移会读整区开头。
  const view = new DataView(seg.buffer, seg.byteOffset + at, len);
  switch (tag) {
    case TAG_U64:
      requireLen(len, 8);
      return view.getBigUint64(0, true);
    case TAG_I64:
      requireLen(len, 8);
      return view.getBigInt64(0, true);
    case TAG_F64: {
      requireLen(len, 8);
      const value = view.getFloat64(0, true);
      // #153：非有限值在 JSON 值模型里无法表示，禁静默映射成 null。
      if (!Number.isFinite(value)) throw new SegFault("non-finite f64");
      return value;
    }
    case TAG_UTF8:
      return decodeUtf8(seg.subarray(at, at + len));
    case TAG_BYTES:
      // 拷贝：禁止把共享背衬的可变视图交给调用方（无锁读的稳定快照必须独立）。
      return seg.subarray(at, at + len).slice();
    case TAG_JSON:
      return parseJson(seg.subarray(at, at + len));
    case TAG_NULL:
      if (len !== 0) throw new SegFault("null payload must be empty");
      return null;
    default:
      throw new SegFault(`unknown tag ${tag}`); // 0x07–0xFF：整段失效
  }
}

function requireLen(len: number, expected: number): void {
  if (len !== expected) throw new SegFault(`tag payload must be ${expected} bytes`);
}

function decodeUtf8(bytes: Uint8Array): string {
  try {
    return utf8.decode(bytes);
  } catch {
    throw new SegFault("invalid UTF-8 payload");
  }
}

function parseJson(bytes: Uint8Array): unknown {
  const text = decodeUtf8(bytes);
  try {
    // reviver 与 serde_json 同口径：非有限数在线格式里不可表示（#153）。
    return JSON.parse(text, (_key, value: unknown) => {
      if (typeof value === "number" && !Number.isFinite(value)) {
        throw new Error("non-finite number in json payload");
      }
      return value;
    });
  } catch {
    throw new SegFault("invalid JSON payload");
  }
}

function truncated(need: number, got: number): RegionError {
  return new RegionError("truncated", `region truncated: need ${need}, got ${got}`);
}
