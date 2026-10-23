//! 非加密稳定摘要：FNV-1a 64（RFC 9923）与 XXH64（xxhash spec v0.2.0）。
//! 存在理由：std `DefaultHasher` 的算法不承诺跨版本稳定——持久化键/校验和/
//! 跨进程比对用它等于每次工具链升级等爆（坑面 #1 原文）。本域交付**显式冻结**
//! 的两个摘要函数，核心卖点不是算法强度而是稳定承诺（下）。
//!
//! # 稳定承诺（本域核心契约）
//!
//! [`HASH_VERSION`] = 1 冻结：`stable64` = FNV-1a 64（basis `0xcbf29ce484222325`、
//! prime `0x100000001b3`）；`xxh64` = XXH64 spec v0.2.0、seed 恒 0。两者的输出
//! 值承诺跨本 crate 版本、跨平台、跨 Rust 工具链**永不变**（RFC 9923 按 octet
//! 定义、与端序无关；xxhash spec 明文「a given variant shall produce exactly the
//! same output, irrespective of the cpu / os used」）。升级=新函数名
//! （`stable64_v2` 类）+ [`HASH_VERSION`] 递增，**禁原地换算法**；journal 类
//! 采用方以 [`HASH_VERSION`] 作升级判断锚。
//!
//! # 红线（错误词汇显式拒绝）
//!
//! 非加密摘要，不承诺抗碰撞：RFC 9923 §1.2「FNV is NOT RECOMMENDED for any
//! application that requires that it be computationally infeasible for one of
//! the above types of attacks to succeed」；xxhash spec 同明文「not meant to
//! avoid intentional collisions」。对抗性场景（不可信输入的完整性校验）用
//! 加密摘要（sha2 等按 ADR 引依赖），本域类型不与加密摘要混用。
//!
//! # 坑面拍板（实现前系统排查，对照表见 reports/2026-10-16-b2-hash.md）
//!
//! 1. **std DefaultHasher 跨版本不稳**（std 文档原文：「The internal algorithm
//!    is not specified, and so it and its hashes should not be relied upon over
//!    releases」）——本域全部函数不经过 `Hash`/`DefaultHasher`，直接吃字节；
//!    仓内先例 scheduler.rs `jitter_offset` 同判（FNV-1a 自写、注明禁
//!    DefaultHasher 持久语义）。
//! 2. **FNV-1a vs FNV-1 变体混淆**：FNV-1a = 先 XOR 后乘（RFC 9923 §2 唯一
//!    聚焦变体）；FNV-1 = 先乘后 XOR，同输入输出全不同。本域只实现 1a，官
//!    方向量钉死数值（含对 FNV-1 已发布值的反向断言，变体漂移必炸测试）。
//! 3. **FNV 分布偏置**（RFC 9923 §1.4 Diffusion 原文）：输出最低位=全输入字
//!    节最低位的 XOR，「second through seventh least significant bits」同族
//!    弱；§1.4 Sticky State：哈希变量为 0 时零字节串不改变状态。堵法=文档
//!    钉死已知弱点 + **不提供任何截断/归约 helper**（禁 `hash % 2^k` 误用
//!    面；桶归约采用方自做且应全宽或 XOR folding，RFC §3）+ 代数性质测试
//!    钉死 bit0 行为（静默「修正」= 变体漂移必炸）。
//! 4. **xxh64 版权面**：算法自由实现——spec 文档（doc/xxhash_spec.md v0.2.0）
//!    明文许可任意目的复制分发；参考实现 xxHash 仓库 BSD-2-Clause（宽松，无
//!    copyleft）；本实现纯按 spec 伪代码重写、零代码复制，5 个 PRIME 常量是
//!    算法定义的一部分。官方向量测试即「实现的是 spec 定义的 XXH64 而非自造
//!    变体」的证明。
//! 5. **空输入**：FNV-1a("")=offset basis（零字节循环不执行）；XXH64("")=
//!    `0xEF46DB3751D8E999`——都有专门向量，禁「空输入当特例返回 0」之类的
//!    自作主张。
//! 6. **长度溢出**：流式字节计数 u64 累加用 checked（[io::ErrorKind::InvalidInput]
//!    显式 Err，fail-visible）；`xxh64` 一次性吃 `&[u8]`，长度 ≤ usize::MAX ≤
//!    u64::MAX，结构上无溢出面。`wrapping_mul` 是算法定义（模 2^64），非遗漏。
//! 7. **xxh64 端序陷阱**：lane 一律小端读（spec「reads it using little-endian
//!    convention」），全部走 `u64::from_le_bytes`/`u32::from_le_bytes`，禁
//!    `from_ne_bytes`（大端平台即错；向量在本机 LE 上必炸 LE 写错，BE 平台
//!    由构造保证）。
//!
//! 常驻零依赖件：纯 std，无 feature 门。零依赖纪律要求不引 xxhash/FNV 第三方
//! crate（两个算法合计 ~80 行，依赖重量不成比例）。
//!
//! 性能锚（2026-10-16 批 hash，release/std Instant 粗测，详见
//! reports/2026-10-16-b2-hash.md）：见各函数 doc「性能锚」段。

use std::io::{self, Read};

/// 稳定承诺版本号（见模块 doc「稳定承诺」）：1 = {`stable64` = FNV-1a 64
/// （RFC 9923），`xxh64` = XXH64（spec v0.2.0、seed 0）}。算法集变化时递增并
/// 以新函数名交付，历史函数输出永不变。
pub const HASH_VERSION: u32 = 1;

/// FNV-1a 64 offset basis（RFC 9923 §2.2，即空输入的摘要值）。
const FNV1A64_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
/// FNV-1a 64 prime（RFC 9923 §2.1：2⁴⁰ + 2⁸ + 0xb3）。
const FNV1A64_PRIME: u64 = 0x0000_0100_0000_01b3;

/// FNV-1a 逐 octet 折叠：`h = (h XOR octet) * prime`（先 XOR 后乘，§2）。
/// wrapping 是算法定义（模 2⁶⁴）。
fn fnv1a_fold(mut h: u64, bytes: &[u8]) -> u64 {
    for &b in bytes {
        h = (h ^ u64::from(b)).wrapping_mul(FNV1A64_PRIME);
    }
    h
}

// ── FNV-1a 64（RFC 9923）──

/// 计算数据的 FNV-1a 64 摘要（稳定承诺见模块 doc；升级走新函数禁原地改）。
///
/// FNV-1a 公式（RFC 9923 §2）：`hash = (hash XOR octet) * prime`——**先 XOR
/// 后乘**；空输入返回 offset basis。wrapping 乘法是算法定义（模 2⁶⁴）。
/// 零分配。
///
/// 性能锚（2026-10-16 批 hash，release/std Instant 粗测，详见
/// reports/2026-10-16-b2-hash.md）：零分配（计数分配器 1000 次调用=0 次）；
/// ~1160-1190 MiB/s @≥1KiB（逐字节乘链，规模钝感）；16B 热缓存 ~5.5ns。
///
/// # Examples
///
/// ```
/// use std_ext::hash::stable64;
///
/// // RFC 9923 §8.3 官方测试向量
/// assert_eq!(stable64(b"foobar"), 0x8594_4171_f739_67e8);
/// assert_eq!(stable64(b""), 0xcbf2_9ce4_8422_2325);
/// ```
pub fn stable64(data: &[u8]) -> u64 {
    fnv1a_fold(FNV1A64_BASIS, data)
}

/// 流式计算 FNV-1a 64 摘要：从 `read` 读到 EOF，返回 `(摘要, 字节数)`。
///
/// 与 [`stable64`] 同一稳定承诺（逐 octet 同序折叠，分块不改变结果）。读循环
/// 按 std 约定重试 `Interrupted`、其余错误原样传播（fail-visible，禁静默吞）；
/// 字节计数 checked 累加，超 u64::MAX 显式 [`io::ErrorKind::InvalidInput`]。
/// 零堆分配（固定栈缓冲）。
///
/// 性能锚（同上报告）：零堆分配（16 KiB 栈缓冲，Cursor 100 次=0 次）；
/// 1 MiB 内存 Cursor ~1109-1141 MiB/s（reader 主导口径）。
///
/// # Examples
///
/// ```
/// use std_ext::hash::stable64_io;
/// use std::io::Cursor;
///
/// let (hash, len) = stable64_io(Cursor::new(b"foobar")).unwrap();
/// assert_eq!(hash, 0x8594_4171_f739_67e8);
/// assert_eq!(len, 6);
/// ```
pub fn stable64_io(mut read: impl Read) -> io::Result<(u64, u64)> {
    /// 栈缓冲：16 KiB——syscall 摊销在 ≥4 KiB 已饱和，取栈安全友好档。
    const IO_BUF_BYTES: usize = 16 * 1024;

    let mut hash = FNV1A64_BASIS;
    let mut total: u64 = 0;
    let mut buf = [0u8; IO_BUF_BYTES];
    loop {
        match read.read(&mut buf) {
            Ok(0) => return Ok((hash, total)),
            Ok(n) => {
                hash = fnv1a_fold(hash, &buf[..n]);
                total = bump_len(total, n)?;
            }
            // std 约定：伪信号重试，不外泄；其余错误原样传播。
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
}

/// 流式字节计数 checked 累加：超 u64::MAX 显式 Err（坑面 #6 装甲）。
fn bump_len(total: u64, n: usize) -> io::Result<u64> {
    total
        .checked_add(n as u64)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "input length exceeds u64::MAX"))
}

// ── XXH64（xxhash spec v0.2.0，seed 恒 0）──

/// XXH64 的 5 个素数常量（xxhash spec「XXH64 Algorithm Description」；
/// 算法定义的一部分）。
const P1: u64 = 0x9E37_79B1_85EB_CA87;
const P2: u64 = 0xC2B2_AE3D_27D4_EB4F;
const P3: u64 = 0x1656_67B1_9E37_79F9;
const P4: u64 = 0x85EB_CA77_C2B2_AE63;
const P5: u64 = 0x27D4_EB2F_1656_67C5;

/// spec round：`rotl(acc + lane * PRIME64_2, 31) * PRIME64_1`。
fn xxh64_round(acc: u64, lane: u64) -> u64 {
    acc.wrapping_add(lane.wrapping_mul(P2))
        .rotate_left(31)
        .wrapping_mul(P1)
}

/// spec mergeAccumulator：`((acc ^ round(0, accN)) * PRIME64_1) + PRIME64_4`。
fn xxh64_merge(acc: u64, val: u64) -> u64 {
    (acc ^ xxh64_round(0, val))
        .wrapping_mul(P1)
        .wrapping_add(P4)
}

/// 小端读 8 字节 lane。`b` 长度由调用方切片构造保证为 8（`chunks_exact(8)`）。
fn read_u64_le(b: &[u8]) -> u64 {
    let mut a = [0u8; 8];
    a.copy_from_slice(b);
    u64::from_le_bytes(a)
}

/// 小端读 4 字节 lane。`b` 长度由调用方保证为 4。
fn read_u32_le(b: &[u8]) -> u32 {
    let mut a = [0u8; 4];
    a.copy_from_slice(b);
    u32::from_le_bytes(a)
}

/// 计算数据的 XXH64 摘要（seed 恒 0；稳定承诺见模块 doc）。高吞吐场景优先
/// 本函数（4-lane 累加器并行友好）；`stable64` 优先用于已冻结 FNV 语义的
/// 历史数据。零分配。
///
/// 性能锚（2026-10-16 批 hash，release/std Instant 粗测，详见
/// reports/2026-10-16-b2-hash.md）：零分配（计数分配器 1000 次调用=0 次）；
/// ~17.5 GiB/s @64KiB（**大块 ≥64MiB 受 DRAM 带宽界回落至 ~11-12 GiB/s**，
/// 非实现缺陷；四通道累加器 ILP；参考实现自报 19.4 GiB/s 同量级）；16B 热缓存 ~4.1ns。
///
/// # Examples
///
/// ```
/// use std_ext::hash::xxh64;
///
/// // python-xxhash（参考实现绑定）README 发布值
/// assert_eq!(xxh64(b""), 0xEF46_DB37_51D8_E999);
/// assert_eq!(xxh64(b"xxhash"), 0x32DD_3895_2C4B_C720);
/// ```
pub fn xxh64(data: &[u8]) -> u64 {
    let len = data.len();
    let mut acc;
    // 短路径尾段：长路径时为去 stripe 的剩余（spec Step 5）。
    let mut rest;
    if len >= 32 {
        // seed 恒 0：v1 = P1+P2、v2 = P2、v3 = 0、v4 = -P1（spec Step 1）。
        let mut v = [P1.wrapping_add(P2), P2, 0, P1.wrapping_neg()];
        let mut stripes = data.chunks_exact(32);
        for stripe in stripes.by_ref() {
            for (i, lane) in stripe.chunks_exact(8).enumerate() {
                v[i] = xxh64_round(v[i], read_u64_le(lane));
            }
        }
        // spec Step 3：收敛 + 依序 merge 四累加器。
        acc = v[0]
            .rotate_left(1)
            .wrapping_add(v[1].rotate_left(7))
            .wrapping_add(v[2].rotate_left(12))
            .wrapping_add(v[3].rotate_left(18));
        for &vi in &v {
            acc = xxh64_merge(acc, vi);
        }
        rest = stripes.remainder();
    } else {
        // spec：不足 32 字节单累加器起步。
        acc = P5;
        rest = data;
    }
    // spec Step 4：长度参战。
    acc = acc.wrapping_add(len as u64);
    while rest.len() >= 8 {
        acc ^= xxh64_round(0, read_u64_le(&rest[..8]));
        acc = acc.rotate_left(27).wrapping_mul(P1).wrapping_add(P4);
        rest = &rest[8..];
    }
    if rest.len() >= 4 {
        acc ^= u64::from(read_u32_le(&rest[..4])).wrapping_mul(P1);
        acc = acc.rotate_left(23).wrapping_mul(P2).wrapping_add(P3);
        rest = &rest[4..];
    }
    for &b in rest {
        acc ^= u64::from(b).wrapping_mul(P5);
        acc = acc.rotate_left(11).wrapping_mul(P1);
    }
    // spec Step 6：final avalanche。
    acc ^= acc >> 33;
    acc = acc.wrapping_mul(P2);
    acc ^= acc >> 29;
    acc = acc.wrapping_mul(P3);
    acc ^= acc >> 32;
    acc
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── 稳定承诺 ──

    #[test]
    fn hash_version_pins_algorithm_set() {
        assert_eq!(HASH_VERSION, 1);
    }

    // ── 坑面 #1/#2：DefaultHasher 替代位 + FNV-1a 变体钉死 ──

    /// RFC 9923 §8.3 测试代码官方向量（teststring × FNV64svalues）+ Go 标准库
    /// golden64a 交叉锚（ab/abc，双独立实现同值）。
    #[test]
    fn stable64_rfc9923_vectors() {
        assert_eq!(stable64(b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(stable64(b"a"), 0xaf63_dc4c_8601_ec8c);
        assert_eq!(stable64(b"ab"), 0x089c_4407_b545_986a);
        assert_eq!(stable64(b"abc"), 0xe71f_a219_0541_574b);
        assert_eq!(stable64(b"foobar"), 0x8594_4171_f739_67e8);
        assert_eq!(stable64(b"Hello!\x01\xFF\xED"), 0xbd51_ea70_94ee_6fa1);
    }

    /// 变体混淆反证：同输入的 FNV-1（先乘后 XOR）值全不同（Go 标准库
    /// golden64）。空输入两变体同为 basis，故负向锚只用非空输入。
    #[test]
    fn stable64_is_fnv1a_not_fnv1_variant() {
        assert_ne!(stable64(b"a"), 0xaf63_bd4c_8601_b7be);
        assert_ne!(stable64(b"abc"), 0xd8dc_ca18_6baf_adcb);
    }

    // ── 坑面 #3：分布偏置（RFC 9923 §1.4）──

    /// 钉死文档化弱点而非掩盖：输出 bit0 = basis bit0（=1）XOR 全输入字节
    /// bit0（RFC 9923 §1.4 Diffusion：最低位只依赖输入字节最低位）。任何
    /// 静默「修正」都属变体漂移（违反稳定承诺）。
    #[test]
    fn stable64_low_bit_is_xor_of_input_low_bits_as_documented() {
        let inputs: [&[u8]; 3] = [b"foobar", b"Hello!\x01\xFF\xED", &[0x55u8; 33]];
        for data in inputs {
            let parity = data.iter().fold(0u8, |acc, &b| acc ^ b) & 1;
            assert_eq!(stable64(data) & 1, u64::from(1 ^ parity));
        }
    }

    // ── stable64_io：流式等价 + 字节数 + 错误面（silent-failure 装甲）──

    /// 分块流式必须与一次性逐 octet 同值（稳定承诺的流式面），覆盖缓冲边界
    /// （IO 缓冲 16KiB 两侧）与短读。
    #[test]
    fn stable64_streaming_matches_one_shot_across_buffer_boundaries() {
        let data: Vec<u8> = (0..=255u8).cycle().take(64 * 1024 + 3).collect();
        for &n in &[
            0usize,
            1,
            15,
            4096,
            16 * 1024 - 1,
            16 * 1024,
            16 * 1024 + 1,
            data.len(),
        ] {
            let (h, len) = stable64_io(io::Cursor::new(&data[..n])).unwrap();
            assert_eq!(h, stable64(&data[..n]), "n={n}");
            assert_eq!(len, n as u64);
        }
    }

    #[test]
    fn stable64_io_reports_byte_count() {
        assert_eq!(stable64_io(io::empty()).unwrap(), (FNV1A64_BASIS, 0));
        assert_eq!(
            stable64_io(io::Cursor::new(b"foobar")).unwrap(),
            (0x8594_4171_f739_67e8, 6)
        );
    }

    /// 读错误原样传播（kind 保持），禁静默当 EOF。
    #[test]
    fn stable64_io_propagates_read_errors() {
        struct Failing;
        impl Read for Failing {
            fn read(&mut self, _buf: &mut [u8]) -> io::Result<usize> {
                Err(io::Error::new(io::ErrorKind::InvalidData, "boom"))
            }
        }
        let err = stable64_io(Failing).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    /// 按 std 约定重试 Interrupted（伪信号不外泄），数据照常收全。
    #[test]
    fn stable64_io_retries_interrupted_reads() {
        struct InterruptedOnce(Vec<u8>, bool);
        impl Read for InterruptedOnce {
            fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
                if !self.1 {
                    self.1 = true;
                    return Err(io::Error::new(io::ErrorKind::Interrupted, "retry me"));
                }
                let n = self.0.len().min(buf.len());
                buf[..n].copy_from_slice(&self.0[..n]);
                self.0.drain(..n);
                Ok(n)
            }
        }
        let (h, len) = stable64_io(InterruptedOnce(b"foobar".to_vec(), false)).unwrap();
        assert_eq!(h, stable64(b"foobar"));
        assert_eq!(len, 6);
    }

    /// 每次只吐 1 字节的慢 reader：短读路径正确收全。
    #[test]
    fn stable64_io_handles_short_reads() {
        struct OneByteAtATime(Vec<u8>);
        impl Read for OneByteAtATime {
            fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
                if self.0.is_empty() || buf.is_empty() {
                    return Ok(0);
                }
                buf[0] = self.0.remove(0);
                Ok(1)
            }
        }
        let (h, len) = stable64_io(OneByteAtATime(b"foobar".to_vec())).unwrap();
        assert_eq!(h, stable64(b"foobar"));
        assert_eq!(len, 6);
    }

    // ── 坑面 #6：长度溢出 fail-visible ──

    #[test]
    fn stream_byte_count_overflow_is_explicit_error() {
        assert!(bump_len(u64::MAX, 1).is_err());
        assert_eq!(bump_len(u64::MAX - 1, 1).unwrap(), u64::MAX);
    }

    // ── 坑面 #4/#5/#7：XXH64 官方锚 + 差分阶梯 ──

    /// 发布锚（跨独立实现）：""/"xxhash" = python-xxhash README（参考实现
    /// 绑定）；"a"/"abc" = Linux 内核 test_xxhash 同值；"Nobody inspects…"
    /// （39B，四通道 stripe + rem7 路径）= python-xxhash 生态文档值。
    #[test]
    fn xxh64_published_anchors() {
        assert_eq!(xxh64(b""), 0xEF46_DB37_51D8_E999);
        assert_eq!(xxh64(b"a"), 0xD24E_C4F1_A98C_6E5B);
        assert_eq!(xxh64(b"abc"), 0x44BC_2CF5_AD77_0999);
        assert_eq!(xxh64(b"xxhash"), 0x32DD_3895_2C4B_C720);
        assert_eq!(
            xxh64(b"Nobody inspects the spammish repetition"),
            0xFBCE_A83C_8A37_8BF1
        );
    }

    /// 差分阶梯：16 档覆盖 <32 单累加器、≥32 四通道、rem 8/4/1 全部尾路径。
    /// 值由按 spec 独立编写的第二实现（Python）差分生成，该实现双锚定
    /// python-xxhash 发布值通过后才采信（见报告 §外搜）。
    #[test]
    fn xxh64_differential_ladder_covers_all_branch_paths() {
        let data: Vec<u8> = (0..1000u32).map(|i| (i * 31 % 251) as u8).collect();
        let expected: [(usize, u64); 16] = [
            (0, 0xEF46_DB37_51D8_E999),
            (1, 0xE934_A84A_DB05_2768),
            (3, 0xE5D2_BE4A_E4B3_469A),
            (4, 0x3B4D_7F7C_6BD1_AE90),
            (8, 0x5068_3412_2CB7_B4D0),
            (9, 0xBA2F_457C_2914_D838),
            (31, 0x5836_F086_07DB_DA19),
            (32, 0xB3F0_EC9D_8D21_6DEA),
            (33, 0xC480_DB42_8C35_AB3E),
            (39, 0xA80B_7639_D6A6_AD77),
            (40, 0x07C8_F9E2_4DB5_791B),
            (41, 0x38FF_52F0_BEF6_7BC8),
            (64, 0x28BF_42BB_281A_F2EE),
            (100, 0xCD8C_2446_7B83_82AD),
            (255, 0x60D8_97F9_B519_8039),
            (1000, 0xD1BE_E8E4_F060_3BBF),
        ];
        for (n, want) in expected {
            assert_eq!(xxh64(&data[..n]), want, "len={n}");
        }
    }
}
