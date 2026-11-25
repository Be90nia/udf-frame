//! 语言无关视图数据面契约词汇（ADR 0006）：`ViewMap`/`IntentMap` 与
//! [`crate::Reducer`] 同地位——契约承载「约束后面代码长法的形状」，机器可换、
//! 契约不换（参考机器 `skeleton_infra::view::ViewBridge`）。领域 `S`/`A` 类型
//! 与字段形状不出 Rust：UI 任意语言经绑定路径取数。
//!
//! # 公开词表演进纪律（M6 冻结条款）
//!
//! - **[`ViewMap`] 方法集冻结**：之后不得新增必实现方法；新能力走（i）带默认
//!   实现的新方法（如 [`ViewMap::input_changed`] 缺省 `true` 形）或（ii）独立
//!   可选 trait。理由：采用方升级成本 = 绑定点数 × 每绑定侵入深度。
//! - **bound 永不落在采用方领域 `S` 上**：框架 bound 只落框架词表
//!   （[`ViewMap::View`]、[`PayloadTag`]）。`Reducer::State: Clone` 是既有契约，
//!   新增能力不得再往 `S` 叠 bound。
//! - 只冻结 [`ViewMap`]：[`IntentMap`] 尚无第二实现，冻结为时过早。

/// 载荷 schema 标签：**绑定级**载荷形状身份号（JSON 批每项一枚、区域每条目
/// 一枚）。编号空间分段保留（protobuf reserved 纪律）：`0` = 未声明（读侧按
/// JSON 自描述处理）；`0x0001-0x7FFF` = 采用方扩展；`0x8000-0xFFFF` = 框架
/// 保留。**只增不改、退役号不复用**。区域段的**值类型** tag 是另一张 `u8`
/// 空间（`docs/view-region-spec.md`）——前者判「形状变没变」，后者判「字节
/// 怎么读」，不混用。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PayloadTag(pub u16);

/// 出站协议版本：信封级，两通道共用（区域头 / JSON 批各一份）。读者据此判定
/// **bootstrap 布局代次**：`protocol_version` 不是「可跳过的未知标签」而是布局
/// 闸——本机只支持等值 major；不等值 = 协商/重同步边界，读者**不得**解释任何
/// v1 偏移/计数/条目（#187）。条目级未知 `kind`/`tag` 跳过只在 major 已支持后
/// 适用。
pub const PROTOCOL_VERSION: u16 = 1;

/// 区域线格式 v1 布局常量（**跨语言唯一真源**）。规范原文 =
/// `docs/view-region-spec.md`；TS/Python/C 侧经
/// `docs/view-bridge-constants/manifest.toml` + `view_constants_gen` 生成同一组
/// 数字（`--check` 只比对不写，漂移即红）。Rust 写/读端一律引用本模块，禁在
/// 别处手写偏移字面量。
///
/// Header 32B 冻结布局（#222 §B）：
///
/// | 字节 | 字段 | 说明 |
/// |---|---|---|
/// | 0..4 | magic | ASCII `UVBR` |
/// | 4..6 | protocol major | LE `u16` |
/// | 6..8 | flags | v1 恒 0（非 0 = 布局不确定，读侧拒收） |
/// | 8..16 | publication sequence | LE `u64`，**8 字节对齐**，SAB 原子访问 |
/// | 16..20 | region_id | LE `u32` |
/// | 20..24 | binding_count | LE `u32` |
/// | 24..32 | index_offset | LE `u64`，v1 恒 32 |
///
/// 序列语义：**只有 even 值是被提交的水位**，与 JSON `PatchBatch.from/to_version`
/// 及 `Snapshot.version` 严格同源同值；odd 只表示「写事务进行中」，永不作已提交
/// 版本对外。初始封板水位 = [`region_layout::INITIAL_SEQ`]，每条通知
/// +[`region_layout::SEQ_STEP`]。
pub mod region_layout {
    /// magic 四字节。
    pub const MAGIC: [u8; 4] = *b"UVBR";
    /// Header 固定长度。
    pub const HEADER_LEN: usize = 32;
    /// Index 条目固定长度。
    pub const ENTRY_LEN: usize = 24;
    /// 段起始 8 字节对齐（填充 0，读侧免对齐陷阱）。
    pub const SEG_ALIGN: usize = 8;
    /// TLV 头长度：`tag:u8 | len:u32(LE)`。
    pub const TLV_HEADER_LEN: usize = 5;
    /// 初始封板水位（首个 committed even 序列）。
    pub const INITIAL_SEQ: u64 = 2;
    /// 每次提交的序列推进量（奇数值被 in-progress 占用）。
    pub const SEQ_STEP: u64 = 2;

    // Header 字段偏移。
    pub const OFFSET_MAGIC: usize = 0;
    pub const OFFSET_PROTOCOL: usize = 4;
    pub const OFFSET_FLAGS: usize = 6;
    /// 发布序列偏移——**8 字节对齐是硬前提**（JS `BigUint64Array` 原子视图）。
    pub const OFFSET_PUBLISH_SEQ: usize = 8;
    pub const OFFSET_REGION_ID: usize = 16;
    pub const OFFSET_BINDING_COUNT: usize = 20;
    pub const OFFSET_INDEX_OFFSET: usize = 24;
    /// v1 `index_offset` 恒值（索引区紧随头）。
    pub const INDEX_OFFSET_V1: u64 = 32;

    // Index 条目字段偏移（24B 固定）。
    pub const ENTRY_OFFSET_BINDING_ID: usize = 0;
    pub const ENTRY_OFFSET_KIND: usize = 4;
    pub const ENTRY_OFFSET_OFFSET: usize = 8;
    pub const ENTRY_OFFSET_LEN: usize = 16;
    pub const ENTRY_OFFSET_GEN: usize = 20;

    // TLV 值类型 tag 空间（只增不改、退役号不复用；与 `PayloadTag` 是两张
    // 不同的空间——本表判「字节怎么读」，`PayloadTag` 判「形状变没变」）。
    pub const TAG_U64: u8 = 0;
    pub const TAG_I64: u8 = 1;
    pub const TAG_F64: u8 = 2;
    pub const TAG_UTF8: u8 = 3;
    pub const TAG_BYTES: u8 = 4;
    pub const TAG_JSON: u8 = 5;
    pub const TAG_NULL: u8 = 6;
}

/// 绑定声明：纯数据。路径是**生产方发布的协议常量**，UI 只读消费。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BindingDecl {
    /// RFC 6901 JSON Pointer 形（如 "/todos"）；机器装配期校验并编译为内部
    /// 索引，运行期不回字符串。契约：声明后只加不改。
    pub path: &'static str,
    /// 载荷 schema 身份。非 `0` = 形状版本号——**形状变更必须换新标签**；
    /// 读侧见未知标签 ⇒ 跳过该项 + 计数 + 置降级提示（**禁整条失败**）。
    pub kind: PayloadTag,
}

/// 视图映射（纯函数契约）：State → 类型化视图值集合（按项）。纯度纪律同
/// reducer：确定性、禁 IO/时钟/随机/写共享态（实现纪律，非编译期保证，issue#136）；
/// panic 不跨边界裸传（机器内 catch + 计数 + 毒化闩收口）。
///
/// # 视图词表纪律：互斥状态用 enum，禁布尔旗子
///
/// [`Self::View`] 是采用方的**状态词表**（不是数据传输结构）：互斥状态必须落在
/// 不同变体上，禁「同一绑定 + 载荷里若干 `bool` 旗子」。
///
/// - 正例：`enum Status { Loading, Ready(Data), Failed(Why) }`——三态互斥，
///   非法组合（`loading && failed`）**不可构造**。
/// - 反例：`struct V { loading: bool, failed: bool }`——4 种组合里 3 种非法，
///   采用方 UI 每个组合都得兜，漏一个就是静默错渲染（React「impossible
///   states」/ Kotlin sealed 同向纪律；Bloc 官方对本形态的判词是
///   「Not Type Safe: possible to emit a malformed state」）。
///
/// 该纪律是**契约层**的（零宏下机器咬不住采用方的字段形状），复核靠**穷尽
/// match**：Rust 编码器按变体 `match`（漏臂 = E0004，加变体即爆）；跨语言 UI
/// 侧靠判别式联合 + `never` 兜底臂（缺臂 = 类型检查期错）。旗子形态两头都没
/// 这道闸。
pub trait ViewMap<S> {
    /// 类型化视图词表：**一绑定一变体的采用方 enum**。bound 全部 std 内建
    /// derive 可满足（零 proc-macro 零三方）；**bound 只落本关联类型，领域
    /// State 零新增**。变体尺寸悬殊时大变体 Box 由采用方自定。
    type View: Clone + PartialEq + Send + 'static;
    /// 绑定词表：装配期定死；返回序即绑定序（机器装配期对账，错位 =
    /// debug_assert + 计数留痕，fail-visible）。
    ///
    /// **语义**：本方法声明的每一项 = [`Self::View`] 词表的一个**消费点**
    /// （一项绑定投影出一个变体实例，见 trait 级「视图词表纪律」）。增删项 =
    /// 词表变体增删 ⇒ `kind` 换新标签号（只加不改、退役号不复用）。
    ///
    /// **反例（词表躲进载荷）**：一条 `/state` 绑定 + 载荷
    /// `{"loading": bool, "error": bool}`——互斥状态进了 JSON，机器与编码器都
    /// 咬不住非法组合，UI 侧也无词表可对账。正例：互斥状态拆成
    /// `/detail/loading`、`/detail/failed`（或一个 `Status` 变体各占一绑定），
    /// 每项一枚 `BindingDecl`。
    fn bindings(&self) -> &[BindingDecl];
    /// 绑定级输入哨兵（L1 记忆化的绑定级应用）：index 输入切片是否变更。
    /// **false ⇒ 机器跳过该绑定的投影与比较**。承诺：`project(index)` 输出与
    /// 上拍恒等——「input 覆盖实读」红线，误报仅性能损失，漏报 = 静默陈旧
    /// 违约。缺省 `true` = 无哨兵（升级可选、语义不翻面）。
    fn input_changed(&self, index: usize, state: &S) -> bool {
        let _ = (index, state);
        true
    }
    /// 单点投影：只投影 index 一项，产类型化视图值（内部无 wire 树）。
    fn project(&self, index: usize, state: &S) -> Self::View;
}

/// 意图映射（纯函数契约）：语言边界词 → 领域 action。
pub trait IntentMap<A> {
    /// 入站载体（参考机器以 `serde_json::Value` 钉死）。
    type Wire;
    /// 翻译失败词表（未知意图/载荷畸形——fail-visible，禁 panic 禁吞）。
    type Err;
    fn intent(&self, wire: Self::Wire) -> Result<A, Self::Err>;
}
