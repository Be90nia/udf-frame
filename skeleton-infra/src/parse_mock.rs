//! 解析面脚本化测试替身：`MockJson` / `MockSqlDialect` / `MockMarkdown`——
//! skeleton 三解析端口（`JsonPort`/`SqlDialectPort`/`MarkdownPort`）的实现方
//! 与采用方的测试注入面。
//!
//! feature 门 `testing`（测试替身域 charter 原地扩容，脚本化模式沿
//! `testing::ScriptedRuntime`/`Script` 先例；`testing` 连带 `runtime`/`tokio`
//! 重量=已知取舍，拆轻量 feature 待真实采用方出现——拍板点
//! reports/2026-10-16-parse-port.md §五.4）。
//!
//! # 共享形状
//!
//! - **脚本化 FIFO**：行为完全由脚本队列驱动（`push_*` 入队，调用按序出队），
//!   与到达的输入无关——行为可预测、断言可编排。
//! - **调用记录**：收到的输入原样入记录面（先记录后出队，耗尽 panic 前输入
//!   已留痕），消费方断言「被怎么调用」。端口方法全是 `&self`，替身用
//!   `Mutex` 内部可变（沿 CollectRuntime 先例；取锁走 `runtime::lock` 毒锁
//!   fail-fast 纪律）。
//! - **队列耗尽 panic**：脚本耗尽=测试替身误用=测试 bug，fail-fast 即最清晰
//!   的失败信号（`testing.rs` 既有判例），禁静默默认值。
//! - **`MockParseError`**：三替身共享的罐装错误（零依赖纪律：手写
//!   Display/Error，同 journal 先例）。
//! - **记录面返回拥有 `Vec`**（锁内克隆）：测试断言面不做借出优化。
//!
//! 本件零真实解析：替身只回罐装值（零实现纪律同样约束测试替身——真实行为
//! 测试是实现侧的事）。
//!
//! # Examples
//!
//! 采用方测试注入：被测代码只依赖契约 trait，测试用 mock 钉行为——
//!
//! ```
//! use skeleton::parse::{Json, JsonPort};
//! use skeleton_infra::parse_mock::{MockJson, MockParseError};
//!
//! /// 被测采用方代码：从端口取配置值（与具体实现解耦）。契约不承诺
//! /// `Err: Display`——需要错误文本的消费方自己加 bound（本例示范）。
//! fn read_port<J: JsonPort>(j: &J) -> Result<i64, String>
//! where
//!     J::Err: std::fmt::Display,
//! {
//!     match j.decode("{}") {
//!         Ok(Json::Int(n)) => Ok(n),
//!         Ok(_) => Err("非整数".to_string()),
//!         Err(e) => Err(e.to_string()),
//!     }
//! }
//!
//! # fn main() {
//! let mut mock = MockJson::new();
//! mock.push_decode(Ok(Json::Int(42)));
//! assert_eq!(read_port(&mock).unwrap(), 42);
//! assert_eq!(mock.decoded_inputs(), ["{}"], "收到的输入可断言");
//!
//! // 错误注入：解析失败路径同样可测
//! let mut boom = MockJson::new();
//! boom.push_decode(Err(MockParseError::new("bad json")));
//! assert_eq!(read_port(&boom).unwrap_err(), "mock parse error: bad json");
//! # }
//! ```

use std::collections::VecDeque;
use std::fmt;
use std::sync::Mutex;

use skeleton::parse::{Json, JsonPort, MarkdownPort, MdEvent, SqlDialectPort};

use crate::runtime::lock;

/// 三替身共享的罐装错误：脚本注入失败路径用（Display 含消息，`source` 无）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MockParseError {
    msg: String,
}

impl MockParseError {
    /// 构造罐装错误。
    pub fn new(msg: impl Into<String>) -> Self {
        Self { msg: msg.into() }
    }
}

impl fmt::Display for MockParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "mock parse error: {}", self.msg)
    }
}

impl std::error::Error for MockParseError {}

/// 脚本队列出队：耗尽 panic（fail-fast，见模块文档）——返回值不可达。
fn script_pop<T>(queue: &mut VecDeque<T>, what: &str) -> T {
    queue.pop_front().unwrap_or_else(|| {
        panic!("parse_mock: {what} 脚本队列已耗尽——替身误用（先 push_* 再调用）")
    })
}

/// [`JsonPort`] 脚本化替身：decode/encode 双脚本队列 + 输入记录。
///
/// ```
/// use skeleton::parse::{Json, JsonPort};
/// use skeleton_infra::parse_mock::MockJson;
///
/// let mut mock = MockJson::new();
/// mock.push_encode(Ok("{\"a\":1}".to_string()));
/// mock.push_decode(Ok(Json::Object(vec![("a".to_string(), Json::Int(1))])));
///
/// let text = mock.encode(&Json::Int(9)).unwrap();
/// assert_eq!(text, "{\"a\":1}");
/// let value = mock.decode(&text).unwrap();
/// assert_eq!(value, Json::Object(vec![("a".to_string(), Json::Int(1))]));
/// assert_eq!(mock.encoded_values(), [Json::Int(9)]);
/// assert_eq!(mock.decoded_inputs(), [text.as_str()]);
/// ```
#[derive(Default)]
pub struct MockJson {
    st: Mutex<MockJsonState>,
}

#[derive(Default)]
struct MockJsonState {
    decode_q: VecDeque<Result<Json, MockParseError>>,
    encode_q: VecDeque<Result<String, MockParseError>>,
    decoded_inputs: Vec<String>,
    encoded_values: Vec<Json>,
}

impl MockJson {
    /// 空脚本替身（等同 [`Default::default`]）。
    pub fn new() -> Self {
        Self::default()
    }

    /// 入队一次 `decode` 脚本步（按调用序出队）。
    pub fn push_decode(&mut self, step: Result<Json, MockParseError>) {
        lock(&self.st).decode_q.push_back(step);
    }

    /// 入队一次 `encode` 脚本步。
    pub fn push_encode(&mut self, step: Result<String, MockParseError>) {
        lock(&self.st).encode_q.push_back(step);
    }

    /// `decode` 收到的全部输入（调用序）。
    pub fn decoded_inputs(&self) -> Vec<String> {
        lock(&self.st).decoded_inputs.clone()
    }

    /// `encode` 收到的全部值（调用序）。
    pub fn encoded_values(&self) -> Vec<Json> {
        lock(&self.st).encoded_values.clone()
    }
}

impl JsonPort for MockJson {
    type Err = MockParseError;

    fn encode(&self, value: &Json) -> Result<String, Self::Err> {
        let mut st = lock(&self.st);
        st.encoded_values.push(value.clone());
        script_pop(&mut st.encode_q, "encode")
    }

    fn decode(&self, text: &str) -> Result<Json, Self::Err> {
        let mut st = lock(&self.st);
        st.decoded_inputs.push(text.to_string());
        script_pop(&mut st.decode_q, "decode")
    }
}

/// [`SqlDialectPort`] 脚本化替身：标识符/占位符双脚本队列 + 调用记录。
///
/// ```
/// use skeleton::parse::SqlDialectPort;
/// use skeleton_infra::parse_mock::{MockParseError, MockSqlDialect};
///
/// let mut mock = MockSqlDialect::new();
/// mock.push_quote_ident(Ok("\"name\"".to_string()));
/// mock.push_placeholder(Err(MockParseError::new("zero nth")));
///
/// assert_eq!(mock.quote_ident("name").unwrap(), "\"name\"");
/// assert_eq!(mock.quoted_idents(), ["name"]);
/// assert!(mock.placeholder(0).is_err());
/// assert_eq!(mock.placeholder_nths(), [0]);
/// ```
#[derive(Default)]
pub struct MockSqlDialect {
    st: Mutex<MockSqlState>,
}

#[derive(Default)]
struct MockSqlState {
    ident_q: VecDeque<Result<String, MockParseError>>,
    ph_q: VecDeque<Result<String, MockParseError>>,
    idents: Vec<String>,
    nths: Vec<usize>,
}

impl MockSqlDialect {
    /// 空脚本替身。
    pub fn new() -> Self {
        Self::default()
    }

    /// 入队一次 `quote_ident` 脚本步。
    pub fn push_quote_ident(&mut self, step: Result<String, MockParseError>) {
        lock(&self.st).ident_q.push_back(step);
    }

    /// 入队一次 `placeholder` 脚本步。
    pub fn push_placeholder(&mut self, step: Result<String, MockParseError>) {
        lock(&self.st).ph_q.push_back(step);
    }

    /// `quote_ident` 收到的全部标识符（调用序）。
    pub fn quoted_idents(&self) -> Vec<String> {
        lock(&self.st).idents.clone()
    }

    /// `placeholder` 收到的全部序号（调用序）。
    pub fn placeholder_nths(&self) -> Vec<usize> {
        lock(&self.st).nths.clone()
    }
}

impl SqlDialectPort for MockSqlDialect {
    type Err = MockParseError;

    fn quote_ident(&self, name: &str) -> Result<String, Self::Err> {
        let mut st = lock(&self.st);
        st.idents.push(name.to_string());
        script_pop(&mut st.ident_q, "quote_ident")
    }

    fn placeholder(&self, nth: usize) -> Result<String, Self::Err> {
        let mut st = lock(&self.st);
        st.nths.push(nth);
        script_pop(&mut st.ph_q, "placeholder")
    }
}

/// [`MarkdownPort`] 脚本化替身：parse 脚本队列（罐装事件序列）+ 源文本记录。
///
/// ```
/// use skeleton::parse::{MdEvent, MdTag, MarkdownPort};
/// use skeleton_infra::parse_mock::MockMarkdown;
///
/// let mut mock = MockMarkdown::new();
/// mock.push_parse(Ok(vec![
///     MdEvent::Enter(MdTag::Heading { level: 1 }),
///     MdEvent::Text("t".to_string()),
///     MdEvent::Exit(MdTag::Heading { level: 1 }),
/// ]));
///
/// let events = mock.parse("# t").unwrap();
/// assert_eq!(events.len(), 3);
/// assert_eq!(mock.sources(), ["# t"]);
/// ```
#[derive(Default)]
pub struct MockMarkdown {
    st: Mutex<MockMdState>,
}

#[derive(Default)]
struct MockMdState {
    parse_q: VecDeque<Result<Vec<MdEvent>, MockParseError>>,
    sources: Vec<String>,
}

impl MockMarkdown {
    /// 空脚本替身。
    pub fn new() -> Self {
        Self::default()
    }

    /// 入队一次 `parse` 脚本步（罐装事件序列或错误）。
    pub fn push_parse(&mut self, step: Result<Vec<MdEvent>, MockParseError>) {
        lock(&self.st).parse_q.push_back(step);
    }

    /// `parse` 收到的全部源文本（调用序）。
    pub fn sources(&self) -> Vec<String> {
        lock(&self.st).sources.clone()
    }
}

impl MarkdownPort for MockMarkdown {
    type Err = MockParseError;

    fn parse(&self, src: &str) -> Result<Vec<MdEvent>, Self::Err> {
        let mut st = lock(&self.st);
        st.sources.push(src.to_string());
        script_pop(&mut st.parse_q, "parse")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use skeleton::parse::{JsonPort, MarkdownPort, MdTag, SqlDialectPort};

    // MockJson：decode 按脚本 FIFO 出队，输入进记录面（错误步同样留痕）。
    #[test]
    fn mock_json_decode_scripted_fifo() {
        let mut mock = MockJson::new();
        mock.push_decode(Ok(Json::Int(1)));
        mock.push_decode(Ok(Json::Int(2)));
        mock.push_decode(Err(MockParseError::new("boom")));

        assert_eq!(mock.decode("first").unwrap(), Json::Int(1));
        assert_eq!(mock.decode("second").unwrap(), Json::Int(2));
        assert_eq!(
            mock.decode("third").unwrap_err(),
            MockParseError::new("boom")
        );
        assert_eq!(mock.decoded_inputs(), ["first", "second", "third"]);
    }

    // MockJson：encode 脚本 + 值记录。
    #[test]
    fn mock_json_encode_scripted() {
        let mut mock = MockJson::new();
        mock.push_encode(Ok("a".to_string()));
        mock.push_encode(Err(MockParseError::new("nope")));

        assert_eq!(mock.encode(&Json::Null).unwrap(), "a");
        assert!(mock.encode(&Json::Null).is_err());
        assert_eq!(mock.encoded_values(), [Json::Null, Json::Null]);
    }

    // 脚本耗尽 = panic（fail-fast 判例），禁静默默认值。
    #[test]
    #[should_panic(expected = "脚本队列已耗尽")]
    fn mock_json_exhausted_script_panics() {
        let mock = MockJson::new();
        let _ = mock.decode("x");
    }

    // MockSqlDialect：双队列脚本 + 调用记录；错误注入出 Err（空名输入也留痕）。
    #[test]
    fn mock_sql_dialect_scripted() {
        let mut mock = MockSqlDialect::new();
        mock.push_quote_ident(Ok("\"t\"".to_string()));
        mock.push_quote_ident(Err(MockParseError::new("empty")));
        mock.push_placeholder(Ok("$1".to_string()));

        assert_eq!(mock.quote_ident("t").unwrap(), "\"t\"");
        assert!(mock.quote_ident("").is_err());
        assert_eq!(mock.placeholder(1).unwrap(), "$1");
        assert_eq!(mock.quoted_idents(), ["t", ""]);
        assert_eq!(mock.placeholder_nths(), [1]);
    }

    // MockMarkdown：罐装事件序列按脚本回放 + 源记录。
    #[test]
    fn mock_markdown_scripted() {
        let mut mock = MockMarkdown::new();
        mock.push_parse(Ok(vec![
            MdEvent::Enter(MdTag::Paragraph),
            MdEvent::Text("x".to_string()),
            MdEvent::Exit(MdTag::Paragraph),
        ]));
        mock.push_parse(Err(MockParseError::new("bad src")));

        assert_eq!(mock.parse("a").unwrap().len(), 3);
        assert!(mock.parse("b").is_err());
        assert_eq!(mock.sources(), ["a", "b"]);
    }

    // 罐装错误可作 std 错误传播（Display/Error 面）。
    #[test]
    fn mock_parse_error_is_std_error() {
        let e = MockParseError::new("m");
        assert_eq!(e.to_string(), "mock parse error: m");
        let _: &dyn std::error::Error = &e;
    }
}
