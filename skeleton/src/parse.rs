//! 解析面端口化（契约层）：json / sql / md 三解析面——「框架定义形状，生态提供实现」。
//!
//! 拍板锚：通用域调研（本地留存，未入库）§〇（2026-10-16）——三面
//! 只做接口约束，零实现；实现 = 宿主 optional dep（serde_json / pulldown-cmark 等事实
//! 标准，feature 门控隔离）或采用方自供。journal 先例推广：契约零依赖，解析词表
//! 隔离在实现侧（参考实现面另批）。
//!
//! # 三面一览（只看名字即可复述）
//!
//! | 问 | 依据名 |
//! |---|---|
//! | JSON 文本 ↔ 值 | [`JsonPort`]（编解码双成员；词汇 [`Json`] 自持） |
//! | SQL 方言文本原语 | [`SqlDialectPort`]（标识符引用 + 占位符；值绑定归执行层） |
//! | Markdown 结构 | [`MarkdownPort`]（Enter/Exit 配对事件流；词汇 [`MdTag`]/[`MdEvent`]） |
//!
//! # 形状纪律（三面共守）
//!
//! | 纪律 | 依据 |
//! |---|---|
//! | sync 起步 | 解析是 CPU 面；异步签名是无采用方的预付费形状税（拍板已裁） |
//! | 错误面 = 关联类型 `Err` | 解析错误词表是实现的（serde_json::Error / mock 注入错误各成一家），契约只钉 Ok 形——与 [`JournalPort`](crate::JournalPort) 同款 |
//! | 值词汇自持，零依赖 | 泛型输出 `decode<T>()` 无 serde 级 bound 构造不出 T（结构上不可实现）；绑 serde 即破零依赖。故词汇 enum 由本 crate 自持，实现做「生态类型 ↔ 契约词汇」映射 |
//! | 词汇零逻辑 | enum + trait 签名即全部；无 helper 蔓延（先例 `Restore`） |
//! | `#[non_exhaustive]` 演进 | 词表按真实采用面渐进扩；加变体=非破坏演进（采用方 match 须带通配） |
//!
//! 测试替身（三 trait 的脚本化 mock）在 `skeleton_infra::parse_mock`
//! （`testing` feature），供实现方/采用方测试注入。

/// JSON 值词汇（契约自持，零依赖）：实现方把生态类型映射进/出本形
/// （serde_json::Value ↔ [`Json`] 等）。
///
/// # 形状决策
///
/// - **Object = 保序 `Vec<(String, Json)>`**：键序与重复键原样保留（相等比较对序
///   敏感）；合并/去重策略归实现侧映射决策并披露。查找 O(n)——配置规模足够，
///   索引化需求由采用方自行转换，本词汇不背哈希表。
/// - **Int = i64**：覆盖主流整数面；`u64 > i64::MAX` 的映射（降级 Float 或 Err）
///   归实现披露（拍板点，parse 端口性能锚（本地留存，未入库）§五.1）。
/// - **PartialEq 非 Eq**（Float 含 NaN，不等自身）。
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Json {
    /// 空值。
    Null,
    /// 布尔。
    Bool(bool),
    /// 整数（i64 全域；超界映射归实现披露，见类型文档）。
    Int(i64),
    /// 浮点（NaN 不等自身）。
    Float(f64),
    /// 字符串。
    Str(String),
    /// 数组（保序）。
    Array(Vec<Json>),
    /// 对象（保序 + 重复键原样保留，见类型文档）。
    Object(Vec<(String, Json)>),
}

/// JSON 编解码端口：文本 ↔ [`Json`] 的最小双成员面。
///
/// # 语义契约（实现必须兑现）
///
/// 1. **对偶面**：`decode` 的合法输入域与 `encode` 的输出域由实现定义并披露
///    （strict / pretty / 容错模式是实现的可见行为）；契约不钉往返律
///    （`decode(encode(v)) == v` 是否恒成立由实现声明）。
/// 2. **fail-visible**：非法输入如实 `Err(Self::Err)`，禁静默截断/跳过。
/// 3. **零依赖**：实现不得把生态类型泄进签名——进出只用 [`Json`] 与 `String`。
///
/// 不拆 encode/decode 两 trait：两向共享同一错误词表与实现配置，拆开是零信息
/// 分箱（拍板点见报告 §一）。
///
/// # Examples
///
/// 契约即可实现——十几行的玩具编解码器即可演示形状（真实实现走 optional dep，
/// 如 serde_json 门控适配）：
///
/// ```
/// use skeleton::parse::{Json, JsonPort};
///
/// /// 固定值玩具：decode 恒回罐装值，encode 恒出罐装串。
/// struct Fixed(Json);
///
/// impl JsonPort for Fixed {
///     type Err = std::convert::Infallible;
///     fn encode(&self, _value: &Json) -> Result<String, Self::Err> {
///         Ok("null".to_string())
///     }
///     fn decode(&self, _text: &str) -> Result<Json, Self::Err> {
///         Ok(self.0.clone())
///     }
/// }
///
/// let p = Fixed(Json::Int(7));
/// assert_eq!(p.decode("anything").unwrap(), Json::Int(7));
/// assert_eq!(p.encode(&Json::Null).unwrap(), "null");
/// ```
pub trait JsonPort {
    /// 解析/序列化错误（词表归实现，契约只钉 Ok 形）。
    type Err;
    /// 值 → JSON 文本。
    fn encode(&self, value: &Json) -> Result<String, Self::Err>;
    /// JSON 文本 → 值；非法输入如实 `Err`（禁静默截断/跳过）。
    fn decode(&self, text: &str) -> Result<Json, Self::Err>;
}

/// SQL 方言文本原语端口：方言间不可约的**文本生成差异面**——标识符引用与
/// 占位符风格。不涉连接与执行（pool 域已有），不涉语句语法（主流方言
/// SELECT/INSERT 同构）。
///
/// # 语义契约（实现必须兑现）
///
/// 1. **引用转义**：[`Self::quote_ident`] 按方言规则转义标识符内的引用字符
///    （主流=双写：`a"b` → `"a""b"`）；转义规则是实现可见契约面，须披露。
/// 2. **占位符从 1 起**：[`Self::placeholder`] 的 `nth` 从 1 计（`$1` / `?` 风格）；
///    `nth == 0` 如实 `Err`——off-by-one 显式化，禁静默产出。
/// 3. **fail-visible**：空标识符无合法用途，[`Self::quote_ident`] 如实 `Err`。
///
/// # 注入面红线（契约 doc 级禁令）
///
/// 本端口**不提供**「参数值 → 文本拼接」的绑定原语——文本拼接是注入面；
/// 占位符经本面产出后，**值的绑定归执行层**（pool/采用方的参数化执行）。
/// 需要字面量渲染或迁移语句抽象的采用方：那不是本端口的事（拍板点，
/// parse 端口性能锚（本地留存，未入库）§五.2）。
///
/// # Examples
///
/// 玩具方言（SQLite 风格：双写转义 + `?` 占位符）：
///
/// ```
/// use skeleton::parse::SqlDialectPort;
///
/// struct SqliteStyle;
///
/// impl SqlDialectPort for SqliteStyle {
///     type Err = String;
///     fn quote_ident(&self, name: &str) -> Result<String, Self::Err> {
///         if name.is_empty() {
///             return Err("空标识符".to_string());
///         }
///         Ok(format!("\"{}\"", name.replace('"', "\"\"")))
///     }
///     fn placeholder(&self, nth: usize) -> Result<String, Self::Err> {
///         if nth == 0 {
///             return Err("占位符序号从 1 起".to_string());
///         }
///         Ok("?".to_string())
///     }
/// }
///
/// let d = SqliteStyle;
/// assert_eq!(d.quote_ident("order").unwrap(), "\"order\"");
/// assert_eq!(d.quote_ident("a\"b").unwrap(), "\"a\"\"b\"");
/// assert_eq!(d.placeholder(3).unwrap(), "?");
/// assert!(d.placeholder(0).is_err()); // off-by-one 显式 Err
/// assert!(d.quote_ident("").is_err()); // 空标识符 fail-visible
/// ```
pub trait SqlDialectPort {
    /// 方言错误（词表归实现）。
    type Err;
    /// 标识符 → 方言引用形（含转义）；空名如实 `Err`。
    fn quote_ident(&self, name: &str) -> Result<String, Self::Err>;
    /// 第 `nth` 个占位符（从 1 起）→ 方言形；`0` 如实 `Err`。
    fn placeholder(&self, nth: usize) -> Result<String, Self::Err>;
}

/// Markdown 块/行内结构标签（Enter/Exit 共用一形——Exit 携带同值标签，消费方
/// 可断言配对，实现少维护一套镜像枚举）。
///
/// 词表=通用结构面（标题/段落/代码块/列表/引用/强调）；Link/Image/Table 等扩展
/// 构造的降级映射政策见 [`MdEvent`] 类型文档。`level` 取值 1..=6（实现义务，
/// 文档钉死；未做受限新类型=已知上限，滥用出现再升级）。
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum MdTag {
    /// 标题（`level` 1..=6）。
    Heading {
        /// 标题级别（1..=6，实现义务）。
        level: u8,
    },
    /// 段落。
    Paragraph,
    /// 围栏/缩进代码块（`lang` = 信息串，无则 `None`）。
    CodeBlock {
        /// 语言标签（fence info string）。
        lang: Option<String>,
    },
    /// 列表容器（有序/无序）。
    List {
        /// 有序列表为 `true`。
        ordered: bool,
    },
    /// 列表项（[`MdTag::List`] 的直接子级）。
    Item,
    /// 引用块。
    BlockQuote,
    /// 强调（*斜体*级）。
    Emphasis,
    /// 强强调（**粗体**级）。
    Strong,
}

/// Markdown 解析事件（Enter/Exit 配对事件流）：[`MarkdownPort::parse`] 的产出
/// 词汇。
///
/// # 配对纪律（实现义务 + 消费方重建树的钥匙）
///
/// - `Enter(t)` 与 `Exit(t)` 严格 LIFO 配对，`Exit` 的标签与栈顶未闭合 `Enter`
///   同值——消费方可断言配对（失配=实现 bug，禁静默吞）。
/// - 叶子事件（[`MdEvent::Text`]/[`MdEvent::Code`]/[`MdEvent::Rule`]）无配对。
///
/// # 降级政策（词表外构造）
///
/// 源文档中的词表外构造（Link/Image/Table/脚注/删除线等扩展集）由实现自选
/// 降级映射（典型：Link → 其文本 [`MdEvent::Text`]；Table → 连续
/// [`MdTag::Paragraph`]），**降级行为是实现可见契约面，须在实现文档披露**
/// （禁静默）。词表按真实采用面渐进扩（`#[non_exhaustive]`，加变体非破坏）。
///
/// # 切分自由
///
/// [`MdEvent::Text`] 是连续文本**片段**：实现可在行内结构边界切分
/// （`a *b* c` → `Text("a ")` `Enter(Strong)` `Text("b")` `Exit(Strong)`
/// `Text(" c")`）。消费方禁假定单段恰一个 Text。
///
/// # 形状决策（拍板点，parse 端口性能锚（本地留存，未入库）§三/§五.3）
///
/// - **事件流非 AST**：AST 的优势面（随机访问/树变换）在渲染器/转换器类采用方
///   出现前无消费者，而契约自持完整 AST=冻结一套迷你 CommonMark 规范；事件流
///   之上可由消费方自持栈构建 AST，反向不成立。
/// - **配对事件流非扁平 depth 标记**：深度标记线性化丢容器分组（引用块含两段
///   落不可表达），配对流任意嵌套可表达。
/// - **`Vec<MdEvent>` 非流式迭代器**：trait 方法返回 `impl Iterator` 每实现各异
///   类型（dyn 不友好）；解析是 CPU 面、文档有界，eager Vec 是诚实简单形。
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum MdEvent {
    /// 结构开启（入栈）。
    Enter(MdTag),
    /// 与栈顶未闭合 [`MdEvent::Enter`] 同值配对（出栈）。
    Exit(MdTag),
    /// 文本片段（可在行内结构边界切分，见类型文档）。
    Text(String),
    /// 行内代码。
    Code(String),
    /// 主题分隔线（无内容叶子，无配对）。
    Rule,
}

/// Markdown 结构解析端口：源文本 → [`MdEvent`] 序列的最小单成员面（渲染面
/// 不在本端口——解析与渲染是两个采用面，无采用方不预付）。
///
/// # 语义契约（实现必须兑现）
///
/// 1. **配对完备**：`Enter`/`Exit` 严格 LIFO 同值配对（见 [`MdEvent`] 文档）；
///    `parse` 返回时序列自洽（无未闭合 Enter）。
/// 2. **fail-visible**：实现认定不可接受的输入如实 `Err`；可降级构造按降级
///    政策映射并披露（禁静默）。
/// 3. **零依赖**：进出只用 `&str` / `Vec<MdEvent>` / `Self::Err`，不泄生态类型。
///
/// # Examples
///
/// 玩具解析器（脚本化返回——真实实现走 optional dep，如 pulldown-cmark 事件流
/// 原生映射）：
///
/// ```
/// use skeleton::parse::{MdEvent, MdTag, MarkdownPort};
///
/// struct Fixed(Vec<MdEvent>);
///
/// impl MarkdownPort for Fixed {
///     type Err = std::convert::Infallible;
///     fn parse(&self, _src: &str) -> Result<Vec<MdEvent>, Self::Err> {
///         Ok(self.0.clone())
///     }
/// }
///
/// let p = Fixed(vec![
///     MdEvent::Enter(MdTag::Heading { level: 2 }),
///     MdEvent::Text("标题".to_string()),
///     MdEvent::Exit(MdTag::Heading { level: 2 }),
/// ]);
/// assert_eq!(p.parse("# any").unwrap().len(), 3);
/// ```
pub trait MarkdownPort {
    /// 解析错误（词表归实现）。
    type Err;
    /// 源文本 → 事件序列（Enter/Exit LIFO 自洽，见 [`MdEvent`] 文档）。
    fn parse(&self, src: &str) -> Result<Vec<MdEvent>, Self::Err>;
}

#[cfg(test)]
mod tests {
    use super::*;

    // 词汇语义钉：Object 保序 + 重复键原样保留（类型文档承诺，防误改
    // HashMap/去重实现破坏相等语义）。
    #[test]
    fn json_object_preserves_order_and_duplicates() {
        let a = Json::Object(vec![
            ("k".to_string(), Json::Int(1)),
            ("k".to_string(), Json::Int(2)),
        ]);
        let b = a.clone();
        assert_eq!(a, b, "同序同值=相等");
        let c = Json::Object(vec![
            ("k".to_string(), Json::Int(2)),
            ("k".to_string(), Json::Int(1)),
        ]);
        assert_ne!(a, c, "Object 相等对键序敏感（保序承诺）");
        let dup = Json::Object(vec![("k".to_string(), Json::Int(1))]);
        assert_ne!(a, dup, "重复键数量参与相等（原样保留承诺）");
    }

    // PartialEq 非 Eq 的文档承诺：NaN 不等自身。
    #[test]
    fn json_float_nan_not_eq_self() {
        assert_ne!(Json::Float(f64::NAN), Json::Float(f64::NAN));
    }

    // 三 trait 均 dyn-safe（`Box<dyn _Port<Err = _>>` 可换实现）——契约形状
    // 承诺：关联类型不阻对象安全（无泛型方法），采用方按需装箱。
    #[test]
    fn ports_are_dyn_safe() {
        fn assert_dyn<T: ?Sized>() {}
        assert_dyn::<dyn JsonPort<Err = std::convert::Infallible>>();
        assert_dyn::<dyn SqlDialectPort<Err = std::convert::Infallible>>();
        assert_dyn::<dyn MarkdownPort<Err = std::convert::Infallible>>();
    }
}
