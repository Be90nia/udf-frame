//! 追加式真相源端口 + JSONL 默认实现（先落盘后 apply；schema 只加字段+default）。
//!
//! feature 门 `journal`（绑 serde+serde_json）。契约要点：
//! - **先落盘后 apply**：`append` 契约 Err ⇒ 调用方禁 apply 该 Action；
//! - **单写者**：本件无跨进程锁，双进程挂载同一账本=交错损坏——多进程场景上层编排；
//! - **flush 点=持久点**：BufWriter 缓冲非逐行 flush；读侧可见性=已 flush 前缀；
//! - 行格式：版本头 `{"v":1}`（`deny_unknown_fields`——未知字段=协议不兼容 fail-closed
//!   拒挂载；`"seed"` 落点须同步扩 Header 字段集）；action 行/snapshot 行
//!   均为 `{"t":"a"|"s", ...payload}` 平铺——**`"t"` 为顶层保留键**（payload 携带顶层
//!   `"t"`=append 显式拒绝 Err(Serialize)；值语义仍不校验）。
//!
//! 零依赖纪律（同 text.rs 先例）：thiserror 不入 manifest，手写同形
//! `Display`/`Error` impl（错误文案与 thiserror 派生形态逐字一致），引入
//! thiserror 依赖时可换回派生。
//!
//! 自动快照注记：「append_count 达 n → flush+snapshot 行」中 snapshot 需 `&S`，
//! 而 `append(&A)` 无状态可见——本件实现**自动 flush**（重开后可见），
//! 快照节奏归调用方 `snapshot()`。

use std::fmt;
use std::io::{BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;
use serde::Serialize;

/// 写缓冲容量（64KiB，禁逐行 flush）。
const WRITE_BUF: usize = 64 * 1024;

#[derive(Debug)]
pub enum JournalError {
    /// 底层 IO 失败（含 create 撞已存在文件：kind=AlreadyExists）。携带目标文件
    /// 路径（多账本进程里「哪个文件撞了」不靠猜）。
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    /// 序列化失败（先序列化后写，失败不产生残行）。
    Serialize(serde_json::Error),
    /// 文件中部畸形行（行号从 1 起；尾部残行由 open 截断，不经此路径）。
    Corrupt {
        line: u64,
        source: serde_json::Error,
    },
    /// 版本头与期望不符。
    VersionMismatch { found: u32, expected: u32 },
}

impl fmt::Display for JournalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io { path, source } => write!(f, "io at {}: {source}", path.display()),
            Self::Serialize(e) => write!(f, "serialize action/state: {e}"),
            Self::Corrupt { line, .. } => write!(f, "corrupt json at line {line}"),
            Self::VersionMismatch { found, expected } => {
                write!(f, "version mismatch: file={found}, expected={expected}")
            }
        }
    }
}

impl std::error::Error for JournalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::Serialize(e) => Some(e),
            Self::Corrupt { source, .. } => Some(source),
            Self::VersionMismatch { .. } => None,
        }
    }
}

/// Io 错误构造（统一携带文件路径；`From<std::io::Error>` 无法承载 path，
/// 故收口于此、各 io 调用点显式 `map_err`）。
fn io_err(path: &Path, source: std::io::Error) -> JournalError {
    JournalError::Io { path: path.to_path_buf(), source }
}

impl From<serde_json::Error> for JournalError {
    fn from(e: serde_json::Error) -> Self {
        Self::Serialize(e)
    }
}

#[derive(Debug)]
pub enum ReplayError {
    /// 底层 IO 失败。
    Io(std::io::Error),
    /// 畸形 JSON 行（行号从 1 起，fail-closed 禁静默跳过）。
    Corrupt {
        line: u64,
        source: serde_json::Error,
    },
}

impl fmt::Display for ReplayError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "io: {e}"),
            Self::Corrupt { line, .. } => write!(f, "corrupt json at line {line}"),
        }
    }
}

impl std::error::Error for ReplayError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            Self::Corrupt { source, .. } => Some(source),
        }
    }
}

impl From<std::io::Error> for ReplayError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

/// 追加式真相源端口（dyn 只在组合根一次——冷边界，铁律二）。
pub trait Journal<A, S>
where
    A: Serialize + DeserializeOwned,
    S: Serialize + DeserializeOwned,
{
    /// 追加一行 action。**契约：Err ⇒ 调用方禁 apply 该 Action**（先落盘后 apply 的另一半）。
    ///
    /// 挂点矩阵：外部动作=组合根包装层 append→dispatch；回灌动作=解释器标准形
    /// 模板强制 append-before-send（成功才 back.send）。核心不设钩子=有意（零依赖）。
    ///
    /// BufWriter 缓冲，非逐行 flush；`snapshot_every` 命中时自动 flush（见模块注）。
    fn append(&mut self, action: &A) -> Result<(), JournalError>;

    /// 快照读侧：自最新 "s" 行起播——`Some((快照态, 后续 action 回放))`；
    /// 无快照行=`None`（调用方回退 initial 全量回放）。快照截断/压实留升级位。
    /// 可见性=已 flush 前缀（BufWriter 未 flush 尾部对读侧不可见）；实时面用 Store 快照。
    fn restore(&self) -> Result<Option<(S, Replay<'_, A>)>, JournalError>;

    /// flush + 写快照行（周期位；语义可选——真相源恒为全量 action 流，快照只为加速恢复）。
    fn snapshot(&mut self, state: &S) -> Result<(), JournalError>;

    /// 只读回放迭代器（逐行懒解析）。未知变体行：跳过+计数（schema 演化）；
    /// 畸形 JSON 行：Err fail-closed（禁静默跳过）。
    /// 可见性=已 flush 前缀；实时面用 Store 快照。
    fn replay(&self) -> Result<Replay<'_, A>, JournalError>;
}

/// JSONL 追加式账本（`Journal` 默认实现；单写者）。
///
/// # Examples
///
/// ```no_run
/// use skeleton_infra::journal::{JsonlJournal, Journal};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct Action { id: u64 }
/// #[derive(Serialize, Deserialize)]
/// struct State { count: u64 }
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let mut j = JsonlJournal::create("journal.jsonl")?;
/// Journal::<Action, State>::append(&mut j, &Action { id: 1 })?; // 先落盘：Err ⇒ 禁 apply
/// let _restored = Journal::<Action, State>::restore(&j)?; // Some((快照态, 增量回放)) 或 None
/// Journal::<Action, State>::snapshot(&mut j, &State { count: 1 })?; // 周期位：flush+写快照行
/// j.flush()?; // 停机三段式③：shutdown 前 flush journal
/// # Ok(())
/// # }
/// ```
pub struct JsonlJournal {
    writer: BufWriter<std::fs::File>,
    path: PathBuf,
    version: u32,
    snapshot_every: Option<u64>,
    append_count: u64,
    /// open 时截断的崩溃残行字节数（Debug 可见留痕——残行必未越过 flush 点=非持久）。
    truncated_tail: u64,
}

impl fmt::Debug for JsonlJournal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JsonlJournal")
            .field("path", &self.path)
            .field("version", &self.version)
            .field("snapshot_every", &self.snapshot_every)
            .field("append_count", &self.append_count)
            .field("truncated_tail", &self.truncated_tail)
            .finish_non_exhaustive()
    }
}

/// 版本头（`deny_unknown_fields`：未知字段=协议不兼容，fail-closed 拒挂载禁静默吞；
/// 协议演化走 `v` 版本号显式升级）。
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Header {
    v: u32,
}

impl JsonlJournal {
    /// 新建文件+写版本头 `{"v":1}`；文件已存在=Err(Io kind AlreadyExists，Display 含路径)。
    pub fn create(path: impl AsRef<Path>) -> Result<Self, JournalError> {
        let path = path.as_ref();
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|e| io_err(path, e))?;
        let mut writer = BufWriter::with_capacity(WRITE_BUF, file);
        // 版本头写后即刷：open 往返与挂载校验都依赖头行对读侧可见（flush 点=持久点）。
        writer.write_all(b"{\"v\":1}\n").map_err(|e| io_err(path, e))?;
        writer.flush().map_err(|e| io_err(path, e))?;
        Ok(Self {
            writer,
            path: path.to_path_buf(),
            version: 1,
            snapshot_every: None,
            append_count: 0,
            truncated_tail: 0,
        })
    }

    /// 追加挂载：读版本头校验 → 尾部完整性校验 → append 从截断点续写。
    ///
    /// 末行无 `\n` 结尾=崩溃残行 → 截断到上一完整行边界并计数留痕（`Debug` 可见）。
    /// 残行截断安全：残行必未完整越过 flush 点=非持久，截断不丢任何已持久 action；
    /// 拒绝挂载会令账本报废不可修，故取截断+fail-visible。
    pub fn open(path: impl AsRef<Path>, expected_version: u32) -> Result<Self, JournalError> {
        let path = path.as_ref();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|e| io_err(path, e))?;
        let mut reader = BufReader::new(file);

        // ① 版本头校验。
        let mut header_line = String::new();
        let header_len = reader
            .read_line(&mut header_line)
            .map_err(|e| io_err(path, e))?;
        let header: Header = serde_json::from_str(header_line.trim_end())
            .map_err(|e| JournalError::Corrupt { line: 1, source: e })?;
        if header.v != expected_version {
            return Err(JournalError::VersionMismatch {
                found: header.v,
                expected: expected_version,
            });
        }

        // ② 尾部完整性：末行无 \n=崩溃残行 → 截断到上一完整行边界。
        let mut truncated_tail = 0u64;
        let size = reader.seek(SeekFrom::End(0)).map_err(|e| io_err(path, e))?;
        if size > header_len as u64 {
            let last = find_last_newline(reader.get_mut(), size).map_err(|e| io_err(path, e))?;
            if let Some(pos) = last {
                let complete_end = pos + 1;
                if complete_end < size {
                    truncated_tail = size - complete_end;
                    reader
                        .get_mut()
                        .set_len(complete_end)
                        .map_err(|e| io_err(path, e))?;
                }
            }
        }

        let mut file = reader.into_inner();
        file.seek(SeekFrom::End(0)).map_err(|e| io_err(path, e))?;
        Ok(Self {
            writer: BufWriter::with_capacity(WRITE_BUF, file),
            path: path.to_path_buf(),
            version: header.v,
            snapshot_every: None,
            append_count: 0,
            truncated_tail,
        })
    }

    /// 显式冲刷（停机三段式③：shutdown 前 flush journal）。
    pub fn flush(&mut self) -> Result<(), JournalError> {
        self.writer.flush().map_err(|e| io_err(&self.path, e))?;
        Ok(())
    }

    /// 每 n 次 append 自动 flush 一次（0/None=关闭自动快照；见模块注）。
    pub fn with_snapshot_every(mut self, n: u64) -> Self {
        self.snapshot_every = if n == 0 { None } else { Some(n) };
        self
    }
}

/// 自 EOF 反向分块找最后一个 `\n` 的绝对字节位（残行长度有界：一条未完成行）。
fn find_last_newline(file: &mut std::fs::File, size: u64) -> std::io::Result<Option<u64>> {
    let mut chunk = [0u8; 8192];
    let mut end = size;
    while end > 0 {
        let start = end.saturating_sub(chunk.len() as u64);
        let len = (end - start) as usize;
        file.seek(SeekFrom::Start(start))?;
        file.read_exact(&mut chunk[..len])?;
        if let Some(pos) = chunk[..len].iter().rposition(|&b| b == b'\n') {
            return Ok(Some(start + pos as u64));
        }
        end = start;
    }
    Ok(None)
}

/// 行格式打标：`{"t":"<tag>", ...payload}` 平铺同层。
///
/// 先序列化后拼接：serde 失败发生在任何字节落盘前，不产生残行。
/// tag 置首、载荷保序（确定性）；payload 非对象=显式拒绝（Err(Serialize)）；
/// payload 顶层 `"t"` 键=显式拒绝（Err(Serialize)）——平铺成重复键后 serde_json
/// 后键覆盖前键，replay 整行被当未知 tag 静默跳过=真相源丢史。
fn tagged_line<T: Serialize>(tag: &str, value: &T) -> Result<String, JournalError> {
    let body = serde_json::to_string(value)?;
    if !(body.starts_with('{') && body.ends_with('}')) {
        return Err(JournalError::Serialize(serde_json::Error::io(
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "action/state must serialize to a JSON object for tag flattening",
            ),
        )));
    }
    // 保留键探测（append 冷路径 IO 主导，成本可忽略；失败发生在写字节之前）。
    let probe: serde_json::Value = serde_json::from_str(&body)?;
    if probe.get("t").is_some() {
        return Err(JournalError::Serialize(serde_json::Error::io(
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "payload must not contain a top-level \"t\" key (reserved for line tag)",
            ),
        )));
    }
    let inner = &body[1..body.len() - 1];
    Ok(if inner.is_empty() {
        format!("{{\"t\":\"{tag}\"}}")
    } else {
        format!("{{\"t\":\"{tag}\",{inner}}}")
    })
}

impl<A, S> Journal<A, S> for JsonlJournal
where
    A: Serialize + DeserializeOwned,
    S: Serialize + DeserializeOwned,
{
    fn append(&mut self, action: &A) -> Result<(), JournalError> {
        let line = tagged_line("a", action)?;
        self.writer
            .write_all(line.as_bytes())
            .map_err(|e| io_err(&self.path, e))?;
        self.writer
            .write_all(b"\n")
            .map_err(|e| io_err(&self.path, e))?;
        self.append_count += 1;
        // 自动 flush（with_snapshot_every 命中；快照节奏归调用方 snapshot()，见模块注）。
        if let Some(n) = self.snapshot_every {
            if self.append_count.is_multiple_of(n) {
                self.flush()?;
            }
        }
        Ok(())
    }

    fn restore(&self) -> Result<Option<(S, Replay<'_, A>)>, JournalError> {
        // 单遍扫描找最后一条 "s" 行；返回的 Replay 从该行下一字节起（快照行之前的行
        // 一律跳过且不解析——含更早快照行与畸形行；行后畸形由增量回放 fail-closed）。
        let file =
            std::fs::File::open(&self.path).map_err(|e| io_err(&self.path, e))?;
        let mut reader = BufReader::new(file);
        let mut line = String::new();
        let mut offset = 0u64;
        let mut line_no = 0u64;
        let mut latest: Option<(S, u64, u64)> = None; // (快照态, 行后字节偏移, 行号)
        loop {
            line.clear();
            let n = reader.read_line(&mut line).map_err(|e| io_err(&self.path, e))?;
            if n == 0 {
                break;
            }
            line_no += 1;
            // 扫描只认 "s" 行：畸形行/其他 tag 行不在此处裁决（留给回放面）。
            let is_snapshot = serde_json::from_str::<serde_json::Value>(line.trim_end())
                .ok()
                .and_then(|v| {
                    (v.get("t").and_then(|t| t.as_str()) == Some("s")).then_some(v)
                });
            if let Some(mut v) = is_snapshot {
                if let Some(m) = v.as_object_mut() {
                    m.remove("t");
                }
                match serde_json::from_value::<S>(v) {
                    Ok(state) => latest = Some((state, offset + n as u64, line_no)),
                    // 账本声称的快照读不出来=fail-closed，禁静默回退旧快照。
                    Err(e) => {
                        return Err(JournalError::Corrupt {
                            line: line_no,
                            source: e,
                        })
                    }
                }
            }
            offset += n as u64;
        }
        match latest {
            None => Ok(None),
            Some((state, start, snap_line)) => {
                let mut file = reader.into_inner();
                file.seek(SeekFrom::Start(start))
                    .map_err(|e| io_err(&self.path, e))?;
                Ok(Some((state, Replay {
                    reader: BufReader::new(file),
                    line_no: snap_line,
                    skipped: 0,
                    buf: String::new(),
                    _marker: PhantomData,
                    _anchor: PhantomData,
                })))
            }
        }
    }

    fn snapshot(&mut self, state: &S) -> Result<(), JournalError> {
        // flush 先行：快照行必须落在全部已 append action 之后（先落盘后 apply 的持久点纪律）。
        self.flush()?;
        let line = tagged_line("s", state)?;
        self.writer
            .write_all(line.as_bytes())
            .map_err(|e| io_err(&self.path, e))?;
        self.writer
            .write_all(b"\n")
            .map_err(|e| io_err(&self.path, e))?;
        // 快照行即写即刷：读侧可见性=已 flush 前缀，不刷的快照对恢复无用。
        self.flush()?;
        Ok(())
    }

    fn replay(&self) -> Result<Replay<'_, A>, JournalError> {
        let file = std::fs::File::open(&self.path).map_err(|e| io_err(&self.path, e))?;
        Ok(Replay {
            reader: BufReader::new(file),
            line_no: 0,
            skipped: 0,
            buf: String::new(),
            _marker: PhantomData,
            _anchor: PhantomData,
        })
    }
}

/// 回放迭代器：Item=`Result<A, ReplayError>`；跳过面=未知 "t" tag 行与快照行
///（skipped_unknown 计数）；畸形行 fail-closed。
pub struct Replay<'j, A> {
    reader: BufReader<std::fs::File>,
    /// 当前行号（从 1 起；跨过跳过面后保持绝对行号语义）。
    line_no: u64,
    skipped: u64,
    buf: String,
    _marker: PhantomData<fn() -> A>,
    _anchor: PhantomData<&'j ()>,
}

impl<'j, A: DeserializeOwned> Iterator for Replay<'j, A> {
    type Item = Result<A, ReplayError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            self.buf.clear();
            let n = match self.reader.read_line(&mut self.buf) {
                Ok(n) => n,
                Err(e) => return Some(Err(ReplayError::Io(e))),
            };
            if n == 0 {
                return None;
            }
            self.line_no += 1;
            let value: serde_json::Value =
                match serde_json::from_str(self.buf.trim_end()) {
                    Ok(v) => v,
                    Err(e) => {
                        return Some(Err(ReplayError::Corrupt {
                            line: self.line_no,
                            source: e,
                        }))
                    }
                };
            if value.get("t").and_then(|t| t.as_str()) == Some("a") {
                // 摘掉 tag 后整行交给 A：payload 缺字段走 schema default（add-only 演化），
                // 真畸形=Err fail-closed（禁静默跳过）。
                let mut payload = value;
                if let Some(m) = payload.as_object_mut() {
                    m.remove("t");
                }
                return Some(
                    A::deserialize(&payload).map_err(|e| ReplayError::Corrupt {
                        line: self.line_no,
                        source: e,
                    }),
                );
            }
            // "s" 行与未知 tag 一律跳过+计数（schema 演化）。
            self.skipped += 1;
        }
    }
}

impl<'j, A> Replay<'j, A> {
    /// 未知变体/快照行跳过计数（fail-visible：>0 必显式报告，禁静默）。
    pub fn skipped_unknown(&self) -> u64 {
        self.skipped
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Act {
        id: u64,
        label: String,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct St {
        count: u64,
    }

    /// 临时目录收在 crate 构建目录内（禁系统 TEMP/用户目录），
    /// 每用例独立子目录；通过用例收尾 remove_dir_all，失败用例残留留调试（gitignore 覆盖）。
    fn case_dir(name: &str) -> PathBuf {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target-iso/journal-tests")
            .join(name);
        let _ = std::fs::remove_dir_all(&dir); // 重跑残留清理
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// `Journal<A, S>` 双参在非泛型 `JsonlJournal` 上需显式消歧——测试侧收敛为四个薄封装。
    fn app(j: &mut JsonlJournal, a: &Act) {
        Journal::<Act, St>::append(j, a).unwrap();
    }
    fn snap(j: &mut JsonlJournal, s: &St) {
        Journal::<Act, St>::snapshot(j, s).unwrap();
    }
    fn rep(j: &JsonlJournal) -> Replay<'_, Act> {
        Journal::<Act, St>::replay(j).unwrap()
    }
    fn rest(j: &JsonlJournal) -> Option<(St, Replay<'_, Act>)> {
        Journal::<Act, St>::restore(j).unwrap()
    }

    /// 恒等序列：中文/引号/反斜杠/换行转义/空串。
    fn sample_acts() -> Vec<Act> {
        vec![
            Act {
                id: 1,
                label: "中文\"引号\"与\\反斜杠".into(),
            },
            Act {
                id: 2,
                label: "line\nbreak\ttab".into(),
            },
            Act {
                id: 3,
                label: String::new(),
            },
        ]
    }

    #[test]
    fn append_replay_identity() {
        let dir = case_dir("append-replay");
        let path = dir.join("j.jsonl");
        let acts = sample_acts();
        {
            let mut j = JsonlJournal::create(&path).unwrap();
            for a in &acts {
                app(&mut j, a);
            }
            j.flush().unwrap();
            let mut r = rep(&j);
            let got: Vec<Act> = r.by_ref().map(Result::unwrap).collect();
            assert_eq!(got, acts);
            assert_eq!(r.skipped_unknown(), 1); // 仅版本头
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn version_header_and_mismatch() {
        let dir = case_dir("version-header");
        let path = dir.join("j.jsonl");
        {
            let mut j = JsonlJournal::create(&path).unwrap();
            app(&mut j, &Act { id: 1, label: "a".into() });
        }
        // create→open 往返；VersionMismatch。
        JsonlJournal::open(&path, 1).unwrap();
        match JsonlJournal::open(&path, 2) {
            Err(JournalError::VersionMismatch { found, expected }) => {
                assert_eq!((found, expected), (1, 2));
            }
            other => panic!("expected VersionMismatch, got {other:?}"),
        }
        // 版本头未知字段：deny_unknown_fields → fail-closed。
        let seeded = dir.join("seeded.jsonl");
        std::fs::write(&seeded, "{\"v\":1,\"seed\":42}\n").unwrap();
        match JsonlJournal::open(&seeded, 1) {
            Err(JournalError::Corrupt { line: 1, .. }) => {}
            other => panic!("expected Corrupt at line 1, got {other:?}"),
        }
        // create 撞已存在=Err(Io kind AlreadyExists，Display 含路径)。
        match JsonlJournal::create(&path) {
            Err(JournalError::Io { source: e, .. }) => {
                assert_eq!(e.kind(), std::io::ErrorKind::AlreadyExists);
            }
            other => panic!("expected Io(AlreadyExists), got {other:?}"),
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// 版本头静默吞未知字段
    /// （serde 默认忽略）=语义漂移入口——`deny_unknown_fields` 与畸形行 fail-closed
    /// 语义对齐；头演化走 `v` 版本号（VersionMismatch 显式拒绝），不走静默加字段。
    #[test]
    fn header_unknown_field_rejected_fail_closed() {
        let dir = case_dir("header-unknown-field");
        let path = dir.join("j.jsonl");
        std::fs::write(&path, "{\"v\":1,\"extra\":true}\n").unwrap();
        match JsonlJournal::open(&path, 1) {
            Err(JournalError::Corrupt { line, .. }) => assert_eq!(line, 1),
            other => panic!("expected Corrupt at line 1, got {other:?}"),
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// Io 错误 Display 必须携带文件路径——多账本进程里「哪个文件撞了」不再靠猜
    /// （os error 80 的本地化文案本身无定位价值）。kind 分流可编程性保持。
    #[test]
    fn io_error_display_carries_path() {
        let dir = case_dir("io-error-carries-path");
        let path = dir.join("ledger.jsonl");
        JsonlJournal::create(&path).unwrap();
        let second = JsonlJournal::create(&path); // 撞已存在
        std::fs::remove_dir_all(&dir).unwrap();
        match second {
            Ok(_) => panic!("create 撞已存在文件必须 Err"),
            Err(e) => {
                let msg = e.to_string();
                assert!(
                    msg.contains("ledger.jsonl"),
                    "Io 错误 Display 必须携带文件路径，实际: {msg}"
                );
                match &e {
                    JournalError::Io { source, .. } => {
                        assert_eq!(source.kind(), std::io::ErrorKind::AlreadyExists);
                    }
                    other => panic!("expected Io, got {other:?}"),
                }
            }
        }
    }

    #[test]
    fn unknown_tag_lines_are_skipped_and_counted() {
        let dir = case_dir("unknown-tag");
        let path = dir.join("j.jsonl");
        let a1 = Act { id: 1, label: "a".into() };
        let a2 = Act { id: 2, label: "b".into() };
        {
            let mut j = JsonlJournal::create(&path).unwrap();
            app(&mut j, &a1);
            j.flush().unwrap();
            // 手工注入未知 tag 行与无 tag 行（schema 演化）。
            let mut f = std::fs::OpenOptions::new().append(true).open(&path).unwrap();
            writeln!(f, "{{\"t\":\"future_tag\",\"x\":1}}").unwrap();
            writeln!(f, "{{\"note\":\"no tag at all\"}}").unwrap();
        }
        {
            let mut j = JsonlJournal::open(&path, 1).unwrap();
            app(&mut j, &a2);
            j.flush().unwrap();
            let mut r = rep(&j);
            let got: Vec<Act> = r.by_ref().map(Result::unwrap).collect();
            assert_eq!(got, vec![a1, a2]);
            assert_eq!(r.skipped_unknown(), 3); // 版本头+future_tag+无 tag
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn corrupt_json_line_fails_closed_with_line_number() {
        let dir = case_dir("corrupt-line");
        let path = dir.join("j.jsonl");
        {
            let mut j = JsonlJournal::create(&path).unwrap();
            app(&mut j, &Act { id: 1, label: "a".into() });
            j.flush().unwrap();
            // 注入畸形行（行 3）：header=1, a1=2。
            let mut f = std::fs::OpenOptions::new().append(true).open(&path).unwrap();
            writeln!(f, "这行不是 JSON").unwrap();
        }
        let j = JsonlJournal::open(&path, 1).unwrap();
        let mut r = rep(&j);
        assert!(r.next().unwrap().is_ok()); // 行 2 完整
        match r.next() {
            Some(Err(ReplayError::Corrupt { line, .. })) => assert_eq!(line, 3),
            other => panic!("expected Corrupt at line 3, got {other:?}"),
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn snapshot_lines_stay_out_of_action_stream() {
        let dir = case_dir("snapshot-stream");
        let path = dir.join("j.jsonl");
        let a1 = Act { id: 1, label: "a".into() };
        let a2 = Act { id: 2, label: "b".into() };
        {
            let mut j = JsonlJournal::create(&path).unwrap();
            app(&mut j, &a1);
            snap(&mut j, &St { count: 1 });
            app(&mut j, &a2);
            j.flush().unwrap();
            let mut r = rep(&j);
            let got: Vec<Act> = r.by_ref().map(Result::unwrap).collect();
            assert_eq!(got, vec![a1, a2]); // "s" 行不混入
            assert_eq!(r.skipped_unknown(), 2); // 版本头+"s" 行
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn with_snapshot_every_auto_flushes() {
        let dir = case_dir("auto-flush");
        let path = dir.join("j.jsonl");
        let a1 = Act { id: 1, label: "a".into() };
        let a2 = Act { id: 2, label: "b".into() };
        let mut j = JsonlJournal::create(&path).unwrap().with_snapshot_every(2);
        app(&mut j, &a1);
        app(&mut j, &a2); // 达 n=2 → 自动 flush，无手动 flush
        // j 保持存活（BufWriter drop 兜底 flush 会污染断言），用第二句柄验证重开可见。
        {
            let j2 = JsonlJournal::open(&path, 1).unwrap();
            let mut r = rep(&j2);
            let got: Vec<Act> = r.by_ref().map(Result::unwrap).collect();
            assert_eq!(got, vec![a1, a2]);
        }
        drop(j);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// 恒 Err 的序列化替身（保证 Err(Serialize) 路径确定性）。
    #[derive(Debug, Deserialize)]
    struct BoomAct;
    impl Serialize for BoomAct {
        fn serialize<S: serde::Serializer>(&self, _s: S) -> Result<S::Ok, S::Error> {
            Err(serde::ser::Error::custom("boom always"))
        }
    }

    #[test]
    fn serialize_error_leaves_no_partial_line() {
        let dir = case_dir("serialize-err");
        let path = dir.join("j.jsonl");
        {
            let mut j = JsonlJournal::create(&path).unwrap();
            match Journal::<BoomAct, St>::append(&mut j, &BoomAct) {
                Err(JournalError::Serialize(_)) => {}
                other => panic!("expected Serialize error, got {other:?}"),
            }
        }
        // 先序列化后写：Err 时文件只有版本头，无残行。
        let content = std::fs::read_to_string(&path).unwrap();
        assert_eq!(content, "{\"v\":1}\n");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// 顶层 `"t"` 为行 tag
    /// 保留键——payload 携带同层 `"t"` 时平铺成重复键，serde_json 后键覆盖前键，
    /// replay 整行被当未知 tag 静默跳过（先落盘后 apply 契约下=真相源丢史）。
    /// fail-closed：append 即 Err(Serialize)，字节零落盘。触发形态常见：
    /// `#[serde(tag = "t")]` internally-tagged enum 或任意含顶层 t 字段的类型。
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TaggedAct {
        t: String,
        v: u64,
    }

    #[test]
    fn reserved_top_level_t_key_rejected_fail_closed() {
        let dir = case_dir("reserved-t-key");
        let path = dir.join("j.jsonl");
        {
            let mut j = JsonlJournal::create(&path).unwrap();
            let a = TaggedAct { t: "a".into(), v: 1 };
            match Journal::<TaggedAct, St>::append(&mut j, &a) {
                Err(JournalError::Serialize(_)) => {}
                other => panic!("expected Serialize error, got {other:?}"),
            }
        }
        // 先序列化后写：Err 时文件只有版本头，无残行。
        let content = std::fs::read_to_string(&path).unwrap();
        assert_eq!(content, "{\"v\":1}\n");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn large_action_crosses_write_buffer() {
        let dir = case_dir("large-action");
        let path = dir.join("j.jsonl");
        let big = "字".repeat(70_000); // 210KiB UTF-8，跨 64KiB 写缓冲
        let a_big = Act { id: 1, label: big.clone() };
        let a_small = Act { id: 2, label: "s".into() };
        {
            let mut j = JsonlJournal::create(&path).unwrap();
            app(&mut j, &a_big);
            app(&mut j, &a_small);
            j.flush().unwrap();
            let mut r = rep(&j);
            let got: Vec<Act> = r.by_ref().map(Result::unwrap).collect();
            assert_eq!(got.len(), 2);
            assert_eq!(got[0].label.len(), big.len());
            assert_eq!(got[1], a_small);
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn crash_residual_line_truncated_on_open() {
        let dir = case_dir("crash-residual");
        let path = dir.join("j.jsonl");
        let a1 = Act { id: 1, label: "a".into() };
        let a2 = Act { id: 2, label: "after-crash".into() };
        {
            let mut j = JsonlJournal::create(&path).unwrap();
            app(&mut j, &a1);
            j.flush().unwrap();
        }
        {
            // 模拟崩溃残行：无 \n 的半行。
            let mut f = std::fs::OpenOptions::new().append(true).open(&path).unwrap();
            write!(f, "{{\"t\":\"a\",\"id\":99,\"label\":\"torn").unwrap();
        }
        {
            let mut j = JsonlJournal::open(&path, 1).unwrap(); // 截断残行
            app(&mut j, &a2); // 从完整行边界续写
            j.flush().unwrap();
            let mut r = rep(&j);
            let got: Vec<Act> = r.by_ref().map(Result::unwrap).collect();
            assert_eq!(got, vec![a1, a2]); // 恰见完整序列，无 Corrupt
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn restore_identity_snapshot_plus_delta_equals_full_replay() {
        let dir = case_dir("restore");
        let path = dir.join("j.jsonl");
        let a1 = Act { id: 1, label: "a".into() };
        let a2 = Act { id: 2, label: "b".into() };
        let a3 = Act { id: 3, label: "c".into() };
        {
            let mut j = JsonlJournal::create(&path).unwrap();
            app(&mut j, &a1);
            app(&mut j, &a2);
            snap(&mut j, &St { count: 3 }); // fold(a1,a2)
            app(&mut j, &a3);
            j.flush().unwrap(); // 可见性=已 flush 前缀：先 flush 再 restore
            // 有快照行：起点=快照态，续回放=快照行之后的增量。
            let (s0, mut r) = rest(&j).unwrap();
            assert_eq!(s0, St { count: 3 });
            let inc: Vec<Act> = r.by_ref().map(Result::unwrap).collect();
            assert_eq!(inc, vec![a3]);
            // 终态恒等：快照+增量 ≡ initial+全量回放（Q3 扩展断言）。
            let mut by_snapshot = s0;
            for a in &inc {
                by_snapshot.count += a.id;
            }
            let mut by_full = St { count: 0 };
            for a in rep(&j).by_ref() {
                by_full.count += a.unwrap().id;
            }
            assert_eq!(by_snapshot.count, by_full.count);
        }
        // 无快照行=None。
        let empty = dir.join("empty.jsonl");
        {
            let j0 = JsonlJournal::create(&empty).unwrap();
            assert!(rest(&j0).is_none());
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// 恒 Err 的状态序列化替身（零覆盖 #11：snapshot() 序列化失败面）。
    #[derive(Debug, serde::Deserialize)]
    struct BoomState;

    impl serde::Serialize for BoomState {
        fn serialize<S: serde::Serializer>(&self, _s: S) -> Result<S::Ok, S::Error> {
            Err(serde::ser::Error::custom("boom state"))
        }
    }

    /// 零覆盖 #11：snapshot() 序列化失败 → Err(Serialize) 且零字节落盘。
    /// 契约：flush 先行已把存量 action 落盘（先落盘后 apply 的持久点纪律），
    /// 失败只发生在快照行写出前——账本保持一致前缀（版本头 + action 流），
    /// 无半行残迹。变异检出：先写后序列化的形态在此留残行，立即红。
    #[test]
    fn snapshot_serialize_failure_returns_err_without_partial_line() {
        let dir = case_dir("snapshot-serialize-err");
        let path = dir.join("j.jsonl");
        {
            let mut j = JsonlJournal::create(&path).unwrap();
            app(&mut j, &Act { id: 1, label: "a".into() });
            match Journal::<Act, BoomState>::snapshot(&mut j, &BoomState) {
                Err(JournalError::Serialize(_)) => {}
                other => panic!("expected Serialize error, got {other:?}"),
            }
        }
        let content = std::fs::read_to_string(&path).unwrap();
        assert_eq!(content.lines().count(), 2, "版本头 + 已 append 的 action，无快照残行");
        assert!(!content.contains("\"t\":\"s\""), "失败快照不得落盘");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// 零覆盖 #10：restore 扫描面遇「账本声称的快照读不出」fail-closed——
    /// JSON 合法（t:"s" 可识别）但 S 反序列化失败 = Err(Corrupt{line})，
    /// 禁静默回退更早的好快照（静默回退=恢复出陈旧态还自认成功=数据丢失面）。
    #[test]
    fn restore_fails_closed_on_undeserializable_snapshot_row() {
        let dir = case_dir("restore-bad-snapshot");
        let path = dir.join("j.jsonl");
        {
            let mut j = JsonlJournal::create(&path).unwrap();
            app(&mut j, &Act { id: 1, label: "a".into() });  // 行 2
            snap(&mut j, &St { count: 1 });                  // 行 3：好快照
            app(&mut j, &Act { id: 2, label: "b".into() });  // 行 4
            j.flush().unwrap();
            // 行 5：JSON 合法、t:"s" 可识别，但 count 类型不符——S 反序列化必败
            use std::io::Write as _;
            let mut f = std::fs::OpenOptions::new().append(true).open(&path).unwrap();
            writeln!(f, "{{\"t\":\"s\",\"count\":\"not-a-number\"}}").unwrap();
        }
        let j = JsonlJournal::open(&path, 1).unwrap();
        match Journal::<Act, St>::restore(&j) {
            Ok(_) => panic!("坏快照必须 Err：静默回退旧快照=恢复陈旧态还自认成功"),
            Err(JournalError::Corrupt { line, .. }) => {
                assert_eq!(line, 5, "坏快照行号如实上报");
            }
            Err(e) => panic!("expected Corrupt at line 5, got {e:?}"),
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

}
