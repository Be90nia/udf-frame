// TS 镜像读器的跨语言一致性夹具（规格 §9.6）。字节构造与
// `skeleton-infra/tests/view_region_wire.rs` 的 `entry` / `region_raw` / `region_auto` /
// `tlv` 逐行同构；断言口径（哪些算结构损坏、哪些只跳过该条目）与 Rust 读侧一致。
//
// 跑法：node --test docs/view-region-reader.test.ts

import { test } from "node:test";
import assert from "node:assert/strict";

import {
  DEFAULT_RETRIES,
  RegionError,
  readRegionLockFree,
  readRegionSnapshot,
  sabBacking,
  type RegionBacking,
  type RegionRead,
} from "./view-region-reader.ts";
import {
  ENTRY_LEN,
  ENTRY_OFFSET_BINDING_ID,
  ENTRY_OFFSET_KIND,
  ENTRY_OFFSET_LEN,
  ENTRY_OFFSET_OFFSET,
  HEADER_LEN,
  INDEX_OFFSET_V1,
  INITIAL_SEQ,
  KIND_COUNT,
  KIND_DERIVED_REMAINING,
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

/** 夹具用的单绑定（`/n`，kind 1 = `KIND_COUNT`）。 */
const KIND = KIND_COUNT;
/** 采用方声明词表：kind 判定集。 */
const DECLS = [KIND_COUNT, KIND_DERIVED_REMAINING];

const TAG_U64 = 0;
const TAG_I64 = 1;
const TAG_F64 = 2;
const TAG_UTF8 = 3;
const TAG_BYTES = 4;
const TAG_JSON = 5;
const TAG_NULL = 6;

const MAGIC = [0x55, 0x56, 0x42, 0x52]; // "UVBR"

// ── 夹具构造（镜像 Rust 测试助手） ──

function concat(parts: Uint8Array[]): Uint8Array {
  const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
  let at = 0;
  for (const part of parts) {
    out.set(part, at);
    at += part.length;
  }
  return out;
}

function tlv(tag: number, payload: Uint8Array): Uint8Array {
  const out = new Uint8Array(TLV_HEADER_LEN + payload.length);
  const view = new DataView(out.buffer);
  view.setUint8(0, tag);
  view.setUint32(1, payload.length, true);
  out.set(payload, TLV_HEADER_LEN);
  return out;
}

function u64(value: bigint): Uint8Array {
  const bytes = new Uint8Array(8);
  new DataView(bytes.buffer).setBigUint64(0, value, true);
  return bytes;
}

function i64(value: bigint): Uint8Array {
  const bytes = new Uint8Array(8);
  new DataView(bytes.buffer).setBigInt64(0, value, true);
  return bytes;
}

function f64(value: number): Uint8Array {
  const bytes = new Uint8Array(8);
  new DataView(bytes.buffer).setFloat64(0, value, true);
  return bytes;
}

function utf8(text: string): Uint8Array {
  return new TextEncoder().encode(text);
}

function entry(id: number, kind: number, off: number | bigint, len: number): Uint8Array {
  const bytes = new Uint8Array(ENTRY_LEN);
  const view = new DataView(bytes.buffer);
  view.setUint32(ENTRY_OFFSET_BINDING_ID, id, true);
  view.setUint16(ENTRY_OFFSET_KIND, kind, true);
  view.setBigUint64(ENTRY_OFFSET_OFFSET, BigInt(off), true);
  view.setUint32(ENTRY_OFFSET_LEN, len, true);
  return bytes;
}

/** 原始布局（offset/len 由调用方给，含敌意值）。 */
function regionRaw(
  sequence: number,
  regionId: number,
  meta: [number, number, number | bigint, number][],
  arena: Uint8Array,
): Uint8Array {
  const bytes = new Uint8Array(HEADER_LEN + ENTRY_LEN * meta.length);
  const view = new DataView(bytes.buffer);
  bytes.set(MAGIC, OFFSET_MAGIC);
  view.setUint16(OFFSET_PROTOCOL, PROTOCOL_VERSION, true);
  view.setBigUint64(OFFSET_PUBLISH_SEQ, BigInt(sequence), true);
  view.setUint32(OFFSET_REGION_ID, regionId, true);
  view.setUint32(OFFSET_BINDING_COUNT, meta.length, true);
  view.setBigUint64(OFFSET_INDEX_OFFSET, BigInt(INDEX_OFFSET_V1), true);
  meta.forEach(([id, kind, off, len], i) => {
    bytes.set(entry(id, kind, off, len), HEADER_LEN + ENTRY_LEN * i);
  });
  return concat([bytes, arena]);
}

/** 自动布局（段 8 字节对齐、紧随索引区）。 */
function regionAuto(sequence: number, segs: [number, number, Uint8Array][]): Uint8Array {
  let cursor = HEADER_LEN + ENTRY_LEN * segs.length;
  const meta: [number, number, number, number][] = [];
  const chunks: Uint8Array[] = [];
  for (const [id, kind, seg] of segs) {
    const pad = (SEG_ALIGN - (cursor % SEG_ALIGN)) % SEG_ALIGN;
    if (pad > 0) chunks.push(new Uint8Array(pad));
    cursor += pad;
    meta.push([id, kind, cursor, seg.length]);
    chunks.push(seg);
    cursor += seg.length;
  }
  return regionRaw(sequence, 1, meta, concat(chunks));
}

function hex(bytes: Uint8Array): string {
  return [...bytes].map((b) => b.toString(16).padStart(2, "0")).join("");
}

// ── 真 SAB 写者（供 §③ 的读窗口改写用例） ──

/**
 * 写者视角的字节搬运（规格 §3 第 2/3 步）：段字节先写，再写索引与 Header。序列字节
 * `8..16` **不搬**——它只经 `Atomics.store` 写，禁字节拷贝覆盖。
 */
function writeRegionBytes(sab: SharedArrayBuffer, next: Uint8Array): void {
  const bytes = new Uint8Array(sab);
  const view = new DataView(next.buffer, next.byteOffset, next.byteLength);
  const indexEnd = INDEX_OFFSET_V1 + ENTRY_LEN * view.getUint32(OFFSET_BINDING_COUNT, true);
  bytes.set(next.subarray(indexEnd), indexEnd); // 段字节
  bytes.set(next.subarray(0, OFFSET_PUBLISH_SEQ), 0); // Header 0..8
  bytes.set(next.subarray(OFFSET_REGION_ID, indexEnd), OFFSET_REGION_ID); // Header 16..32 + 索引
}

/** 抓 typed 错误（禁 assert.throws 的宽松匹配：kind 必须精确）。 */
function errorOf(run: () => unknown): RegionError {
  try {
    run();
  } catch (error) {
    if (error instanceof RegionError) return error;
    throw error;
  }
  throw new Error("expected RegionError, got none");
}

function bytesOf(value: unknown): Uint8Array {
  if (!(value instanceof Uint8Array)) throw new Error("expected a byte payload");
  return value;
}

function readWith(backing: RegionBacking, retries?: number): RegionRead {
  return readRegionLockFree(backing, DECLS, retries);
}

// ── ① 布局与版本闸 ──

test("生成的常量与 v1 冻结布局一致（序列 8 字节对齐、索引紧随头）", () => {
  assert.equal(HEADER_LEN, 32);
  assert.equal(ENTRY_LEN, 24);
  assert.equal(INDEX_OFFSET_V1, HEADER_LEN);
  assert.equal(OFFSET_PUBLISH_SEQ % 8, 0, "SAB 原子视图必须 8 字节对齐");
  assert.equal(INITIAL_SEQ % 2, 0, "已提交水位恒 even");
  assert.equal(DEFAULT_RETRIES, 8, "与 Rust RegionReader::DEFAULT_RETRIES 同值");
});

test("黄金样本：region_auto 字节（Rust 夹具同构）解出同一结果", () => {
  const bytes = regionAuto(INITIAL_SEQ, [
    [0, KIND, tlv(TAG_U64, u64(42n))],
    [7, 0, tlv(TAG_U64, u64(9n))],
  ]);
  assert.equal(hex(bytes), GOLDEN_REGION_HEX, "夹具字节与冻结样本一致");

  const read = readRegionSnapshot(bytes, DECLS);
  assert.deepEqual([...read.values], [[0, 42n], [7, 9n]]);
  assert.equal(read.unknownTagsSkipped, 0);
  assert.equal(read.needsResync, false);
  assert.equal(read.regionId, 1);
  assert.equal(read.sequence, BigInt(INITIAL_SEQ));
  assert.equal(read.protocolVersion, PROTOCOL_VERSION, "header 元数据随结果返回");
});

test("major 不支持时任何 v1 偏移都不解释", () => {
  // v1 偏移处全是垃圾：若被当作 v1 硬解，binding_count 会读出 0xFFFF_FFFF。
  const bytes = new Uint8Array(64).fill(0xff);
  bytes.set(MAGIC, OFFSET_MAGIC);
  new DataView(bytes.buffer).setUint16(OFFSET_PROTOCOL, 2, true);

  const error = errorOf(() => readRegionSnapshot(bytes, DECLS));
  assert.equal(error.kind, "unsupportedVersion");
  assert.equal(error.major, 2);
});

test("快照入口拒收 odd 序列与非 0 flags", () => {
  const odd = errorOf(() => readRegionSnapshot(regionAuto(3, []), DECLS));
  assert.equal(odd.kind, "writerInProgress");
  assert.equal(odd.sequence, 3n);

  const flagged = regionAuto(INITIAL_SEQ, []);
  new DataView(flagged.buffer).setUint16(OFFSET_FLAGS, 1, true);
  assert.equal(errorOf(() => readRegionSnapshot(flagged, DECLS)).kind, "malformed");
});

// ── ② 结构损坏 ⇒ typed；逐条目降级 ⇒ 跳过 + 计数 + resync ──

test("结构损坏一律 typed 错误（不 panic、不静默）", () => {
  const seg = tlv(TAG_U64, u64(7n));
  const cases: [string, Uint8Array, string][] = [
    ["头不足", new Uint8Array(8), "truncated"],
    [
      "magic 不符",
      mutate(regionAuto(INITIAL_SEQ, []), (b) => {
        b[0] = 0x58;
      }),
      "badMagic",
    ],
    [
      "index_offset ≠ 32",
      mutate(regionAuto(INITIAL_SEQ, []), (b) =>
        new DataView(b.buffer).setBigUint64(OFFSET_INDEX_OFFSET, 40n, true),
      ),
      "malformed",
    ],
    [
      "索引进界（binding_count 巨大）",
      mutate(regionAuto(INITIAL_SEQ, []), (b) =>
        new DataView(b.buffer).setUint32(OFFSET_BINDING_COUNT, 0xffff, true),
      ),
      "truncated",
    ],
    [
      "重复 BindingId",
      regionAuto(INITIAL_SEQ, [
        [0, KIND, seg],
        [0, KIND, seg],
      ]),
      "malformed",
    ],
    [
      "段偏移未对齐",
      regionRaw(INITIAL_SEQ, 1, [[0, KIND, HEADER_LEN + ENTRY_LEN + 1, seg.length]], new Uint8Array(32)),
      "malformed",
    ],
    [
      "段落进 Header/Index",
      regionRaw(INITIAL_SEQ, 1, [[0, KIND, 0, seg.length]], concat([seg, new Uint8Array(16)])),
      "malformed",
    ],
    [
      "段偏移超出安全整数域（2^53）",
      regionRaw(INITIAL_SEQ, 1, [[0, KIND, 2 ** 53, seg.length]], new Uint8Array(0)),
      "malformed",
    ],
    [
      "段偏移高位 u64（2^63）",
      regionRaw(INITIAL_SEQ, 1, [[0, KIND, 2n ** 63n, seg.length]], new Uint8Array(0)),
      "malformed",
    ],
    [
      // offset 本身安全（2^53−8），但 + u32 长度越出安全整数域：禁先算后丢精度。
      "段 end 超出安全整数域",
      regionRaw(INITIAL_SEQ, 1, [[0, KIND, 2 ** 53 - 8, 0xffffffff]], new Uint8Array(0)),
      "malformed",
    ],
    [
      "段越界（尾部截断）",
      mutate(regionAuto(INITIAL_SEQ, [[0, KIND, seg]]), (b) => b.subarray(0, b.length - 1)),
      "truncated",
    ],
  ];

  for (const [name, bytes, kind] of cases) {
    assert.equal(errorOf(() => readRegionSnapshot(bytes, DECLS)).kind, kind, name);
  }
});

test("未知 kind / 不可解 TLV 只跳过该条目：计数 + resync，独立有效条目保留", () => {
  const bytes = regionAuto(INITIAL_SEQ, [
    [0, KIND, tlv(TAG_U64, u64(42n))],
    [1, 0x7777, tlv(TAG_U64, u64(1n))], // 未知 kind
    [2, KIND, tlv(0x7f, utf8("junk"))], // 未知 tag（0x07–0xFF）
    [3, KIND, concat([tlv(TAG_U64, u64(1n)), new Uint8Array(2)])], // 段内残留 1–4 字节
    [4, KIND, tlv(TAG_F64, f64(Number.NaN))], // 非有限 f64
    [5, KIND, tlv(TAG_NULL, new Uint8Array([1, 2, 3]))], // null 载荷非空
    [6, KIND, new Uint8Array([TAG_U64, 8, 0, 0])], // TLV 头截断
    [7, KIND, tlv(TAG_UTF8, new Uint8Array([0xff, 0xfe]))], // 非法 UTF-8
    [8, KIND, tlv(TAG_JSON, utf8("{not json"))], // 非法 JSON
    [9, 0, tlv(TAG_U64, u64(9n))], // kind 0 = 未声明（自描述）
  ]);

  const read = readRegionSnapshot(bytes, DECLS);
  assert.deepEqual([...read.values], [[0, 42n], [9, 9n]], "独立有效条目必须保留");
  assert.equal(read.unknownTagsSkipped, 8, "未知 kind + 7 条不可解段（各整段失效）");
  assert.equal(read.needsResync, true, "降级必须显形");
  assert.equal(read.sequence, BigInt(INITIAL_SEQ));
  assert.equal(read.regionId, 1);
});

test("TLV 长度按无符号 u32 读：高位为 1 须判载荷截断，禁当负数放行", () => {
  const highBitLen = new Uint8Array(TLV_HEADER_LEN); // tag + len 头，无载荷
  highBitLen[0] = TAG_U64;
  new DataView(highBitLen.buffer).setUint32(1, 0x80000000, true); // 2^31
  const bytes = regionAuto(INITIAL_SEQ, [
    [0, KIND, highBitLen],
    [1, KIND, tlv(TAG_U64, u64(5n))],
  ]);

  const read = readRegionSnapshot(bytes, DECLS);
  assert.equal(read.values.has(0), false, "长度高位为 1 的段必须整段失效（不是负数放行）");
  assert.equal(read.values.get(1), 5n, "同区其他条目照常");
  assert.equal(read.unknownTagsSkipped, 1);
  assert.equal(read.needsResync, true);
});

test("数值/文本/字节/JSON 载荷解对（含非 0 byteOffset 视图）", () => {
  const region = regionAuto(INITIAL_SEQ, [
    [0, KIND, tlv(TAG_U64, u64(2n ** 63n))],
    [1, KIND, tlv(TAG_I64, i64(-5n))],
    [2, KIND, tlv(TAG_F64, f64(-0.5))],
    [3, KIND, tlv(TAG_UTF8, utf8("héllo"))],
    [4, KIND, tlv(TAG_BYTES, new Uint8Array([1, 2, 3]))],
    [5, KIND, tlv(TAG_JSON, utf8('{"a":1,"b":[true,null]}'))],
    [6, KIND, tlv(TAG_NULL, new Uint8Array(0))],
  ]);
  // 整区放进更大的背衬并做非 0 偏移视图：DataView 漏传 byteOffset 会读错值。
  const host = new Uint8Array(region.length + 16);
  host.set(region, 8);
  const view = host.subarray(8, 8 + region.length);

  const read = readRegionSnapshot(view, DECLS);
  assert.equal(read.values.get(0), 2n ** 63n, "u64 走 BigInt，不丢精度");
  assert.equal(read.values.get(1), -5n);
  assert.equal(read.values.get(2), -0.5);
  assert.equal(read.values.get(3), "héllo");
  assert.deepEqual([...bytesOf(read.values.get(4))], [1, 2, 3]);
  assert.deepEqual(read.values.get(5), { a: 1, b: [true, null] });
  assert.equal(read.values.get(6), null);
  assert.equal(read.unknownTagsSkipped, 0);

  // 字节载荷必须是拷贝：共享背衬后续被写者改写不得影响已读值。
  host.fill(0);
  assert.deepEqual([...bytesOf(read.values.get(4))], [1, 2, 3]);
});

// ── ③ SAB 直读：load → 扫描 → load ──

test("sabBacking 的序列视图与 Header 8..16 是同一共享位置", () => {
  const region = regionAuto(INITIAL_SEQ, [[0, KIND, tlv(TAG_U64, u64(1n))]]);
  const sab = new SharedArrayBuffer(region.length);
  new Uint8Array(sab).set(region);
  const backing = sabBacking(sab);

  assert.equal(readWith(backing).sequence, BigInt(INITIAL_SEQ));

  // 原子写序列字 ⇒ 读者看到新 watermark（同一次稳定读里 region_id 一起被观察到）。
  const seq = new BigUint64Array(sab, OFFSET_PUBLISH_SEQ, 1);
  Atomics.store(seq, 0, BigInt(INITIAL_SEQ + 2));
  const read = readWith(backing);
  assert.equal(read.sequence, BigInt(INITIAL_SEQ + 2));
  assert.equal(read.regionId, 1);
});

test("写者在两次 load 之间提交 ⇒ 重试，且只返回完整快照", () => {
  const older = regionAuto(INITIAL_SEQ, [[0, KIND, tlv(TAG_U64, u64(1n))]]);
  const newer = regionAuto(INITIAL_SEQ + 2, [[0, KIND, tlv(TAG_U64, u64(2n))]]);
  // load 调用序列：attempt1 before=2 → 扫描期间写者提交 → after=4（不等 ⇒ 重试）；
  // attempt2 before=4 → 扫描 → after=4（接受）。
  const loads = [2n, 4n, 4n, 4n];
  let loadCalls = 0;
  let scans = 0;
  const backing: RegionBacking = {
    withBytes<T>(read: (bytes: Uint8Array) => T): T {
      scans += 1;
      return read(scans === 1 ? older : newer);
    },
    loadSequence: () => loads[loadCalls++] ?? 4n,
  };

  const read = readWith(backing);
  assert.equal(loadCalls, 4, "撕裂窗口必须重试一次");
  assert.deepEqual([...read.values], [[0, 2n]], "只接受完整新快照，绝不混合");
  assert.equal(read.sequence, 4n);
});

test("真 SAB：写者在 before/after 两次原子 load 之间提交整笔事务 ⇒ 重试且只接受同版快照", () => {
  const older = regionAuto(INITIAL_SEQ, [[0, KIND, tlv(TAG_U64, u64(1n))]]);
  const newer = regionAuto(INITIAL_SEQ + 2, [[0, KIND, tlv(TAG_U64, u64(2n))]]);
  const sab = new SharedArrayBuffer(older.length);
  const seq = new BigUint64Array(sab, OFFSET_PUBLISH_SEQ, 1);
  new Uint8Array(sab).set(older);

  let loads = 0;
  let scans = 0;
  const backing: RegionBacking = {
    withBytes<T>(read: (bytes: Uint8Array) => T): T {
      scans += 1;
      if (scans === 1) {
        // 读器已取到 before 水位，此刻写者完成整笔事务：odd → 字节 → even（规格 §3）。
        Atomics.store(seq, 0, BigInt(INITIAL_SEQ + 1));
        writeRegionBytes(sab, newer);
        Atomics.store(seq, 0, BigInt(INITIAL_SEQ + 2));
      }
      return read(new Uint8Array(sab));
    },
    loadSequence: () => {
      loads += 1;
      return Atomics.load(seq, 0);
    },
  };

  const read = readWith(backing);
  assert.equal(loads, 4, "旧水位 + 新字节的窗口必须整次重试");
  assert.equal(scans, 2, "第二次扫描才被采信");
  assert.equal(read.sequence, BigInt(INITIAL_SEQ + 2));
  assert.equal(read.values.get(0), 2n, "(sequence, value) 必须同版配对，永不混合");
});

test("真 SAB：写者停在 odd ⇒ writerInProgress{sequence}；提交后读到新快照", () => {
  const older = regionAuto(INITIAL_SEQ, [[0, KIND, tlv(TAG_U64, u64(1n))]]);
  const newer = regionAuto(INITIAL_SEQ + 2, [[0, KIND, tlv(TAG_U64, u64(2n))]]);
  const sab = new SharedArrayBuffer(older.length);
  const seq = new BigUint64Array(sab, OFFSET_PUBLISH_SEQ, 1);
  new Uint8Array(sab).set(older);

  let scans = 0;
  const backing: RegionBacking = {
    withBytes<T>(read: (bytes: Uint8Array) => T): T {
      scans += 1;
      return read(new Uint8Array(sab));
    },
    loadSequence: () => Atomics.load(seq, 0),
  };

  // 写者发布 odd 后停住（事务在途、尚未提交）：真实 Atomics.store，无定时器、无线程。
  Atomics.store(seq, 0, BigInt(INITIAL_SEQ + 1));
  const busy = errorOf(() => readWith(backing, 3));
  assert.equal(busy.kind, "writerInProgress");
  assert.equal(busy.sequence, BigInt(INITIAL_SEQ + 1));
  assert.equal(scans, 0, "odd 窗口内不得扫描字节");

  // 写者完成提交：字节 + even（序列字节只走原子写，禁字节拷贝覆盖）。
  writeRegionBytes(sab, newer);
  Atomics.store(seq, 0, BigInt(INITIAL_SEQ + 2));
  const read = readWith(backing);
  assert.equal(read.sequence, BigInt(INITIAL_SEQ + 2));
  assert.equal(read.values.get(0), 2n, "提交后必须读到整版新快照");
  assert.equal(scans, 1);
});

test("写者持续占用 ⇒ 有界重试 + 可见 writerInProgress（不转成死循环）", () => {
  const bytes = regionAuto(INITIAL_SEQ, [[0, KIND, tlv(TAG_U64, u64(1n))]]);
  let scans = 0;
  const odd: RegionBacking = {
    withBytes<T>(read: (bytes: Uint8Array) => T): T {
      scans += 1;
      return read(bytes);
    },
    loadSequence: () => 3n, // 事务永不提交
  };
  const busy = errorOf(() => readWith(odd, 4));
  assert.equal(busy.kind, "writerInProgress", "最后一次原子观测仍是 odd ⇒ 写者在途");
  assert.equal(busy.sequence, 3n);
  assert.equal(scans, 0, "odd 窗口内不得扫描字节");

  // 每轮前后都不同（写者一直在推进）：同样耗尽，且不会返回撕裂内容。
  let tick = 0;
  const churning: RegionBacking = {
    withBytes: (read) => read(bytes),
    loadSequence: () => BigInt(INITIAL_SEQ + 2 * tick++),
  };
  assert.equal(errorOf(() => readWith(churning, 3)).kind, "retryExhausted");
});

test("结构错误只在稳定窗口里才算真错误（撕裂窗口重试）", () => {
  // bad magic
  const corrupted = mutate(regionAuto(INITIAL_SEQ, []), (b) => {
    b[0] = 0x58;
  });
  const stable: RegionBacking = {
    withBytes: (read) => read(corrupted),
    loadSequence: () => 2n,
  };
  assert.equal(errorOf(() => readWith(stable)).kind, "badMagic");

  // 同一个坏字节，但序列在扫描期间前进 ⇒ 本次字节不可信，重试耗尽而非报 badMagic。
  let unstableLoads = 0;
  const unstable: RegionBacking = {
    withBytes: (read) => read(corrupted),
    loadSequence: () => BigInt(2 + 2 * unstableLoads++),
  };
  assert.equal(errorOf(() => readWith(unstable, 3)).kind, "retryExhausted");
});

/** 拷贝一份再改（夹具构造用，禁就地改共享数组）。 */
function mutate(bytes: Uint8Array, edit: (bytes: Uint8Array) => void | Uint8Array): Uint8Array {
  const copy = bytes.slice();
  const replaced = edit(copy);
  return replaced instanceof Uint8Array ? replaced : copy;
}

// 与 Rust 夹具同构的黄金字节（region_auto(INITIAL_SEQ, [(0, /n, tlv(u64, 42)), (7, kind 0, tlv(u64, 9))])）。
const GOLDEN_REGION_HEX =
  "555642520100000002000000000000000100000002000000200000000000000000000000" +
  "0100000050000000000000000d0000000000000007000000000000006000000000000000" +
  "0d0000000000000000080000002a00000000000000000000000800000009000000000000" +
  "00";
