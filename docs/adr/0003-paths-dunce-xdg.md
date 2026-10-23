# ADR-0003：paths 域拍板——verbatim 剥离对齐 dunce（自研超集）+ XDG 用户基目录语义

- 状态：已采纳（实施 2026-10-16 批 D / 批 b1-paths；补档 2026-10-18 批 1）
- 决策人：用户拍板（域添加二分法：池化/time/paths 等「生态没有成熟库完全覆盖」的域，框架自己实现把坑踩完）
- 性质：历史欠档补录（拍板依据在 `skeleton-infra/src/paths.rs` 模块头注与域 doc；杠精报告维 1 扣分项「拍板未落 ADR」）

## 背景

- 事故锚 **F19**：std `canonicalize` 在 Windows 返回 `\\?\` verbatim 前缀路径，跨程序边界不兼容（多数 Win32 API 与第三方程序不认 verbatim 形态）。
- 事故锚 **F21**：`..` 词法归一不当时越根解析，可绕过白名单 containment 判定（路径花招逃逸）。
- 生态现状：dunce 1.0.5 是「剥 verbatim 的规范化」事实标准库，但只做 canonicalize 后剥离；containment 判定/相对化错误面/纯词法归一 dunce 不提供。路径面是「自研有安全风险就判否，但有成熟部分生态」与「框架必须把坑踩完」的混合域——按域添加二分法判：接口面自研（有 armor），算法面**对齐**成熟库而非自造。

## 决策

1. **verbatim 剥离算法对齐 dunce 1.0.5，不引入 dunce crate**（零依赖纪律）：四道闸逐条同构——VerbatimDisk-only + 文件名合法性 + DOS 保留名 + 260 UTF-16 上限。
2. **保留名覆盖为 dunce 的有据超集**：MSDN《Naming Files, Paths, and Namespaces》明示 `COM¹` 等上标 ¹²³ 变体同属设备保留名，本域纳入判定（dunce 未覆盖）。
3. **命名纪律（API 层防坑）**：无 IO 归一名带 `lexical`（`normalize_lexical`/`simplify`）、带 IO 归一名带 `canonicalize`（`canonicalize_compat`），禁隐式择一——F19 的坑面由命名强制分流。
4. **containment/相对化错误全显式**：containment 判定必须过 `simplify`（verbatim 形式直比较=复现 F19）；相对化错误枚举 `RelError`（MismatchedKind/PrefixKindMismatch/NotPrefix），收编 pathdiff 的 `Option` 静默坑。
5. **`home_dir` = `std::env::home_dir` 直包**（1.85 恢复非废弃）；None 处置指引文档化：**禁静默回退 cwd**——log 域 `DefaultPathResolver` 构造期显式 `Err` 即先例。
6. **XDG 用户基目录（spec v0.8 原文锚定）**：
   - home 缺失 ⇒ `None`（链断传播——比 spec 更保守的 fail-closed：家锚缺失不出路径）；
   - env 值**空 = 未设**（"either not set or empty"）；
   - **相对值 = invalid，忽略**回退默认（"must be absolute ... invalid and ignore it"）；
   - 结果过 `normalize_lexical`（与 OS 解析一致的词法归一，F21 同语义；零 IO，符号链接不解析）；
   - **Windows 面恒 `None`**：XDG 是 Unix 约定，Windows 原生 Known Folders 需 FFI + ADR 另立项——调用方按 None 分支处置，勿静默回退；
   - **macOS 走本 XDG 面**（std-only 约束不引 CoreFoundation FFI；XDG 变量在 macOS 亦被 git/cargo 等广泛尊重）；
   - 纯解析面零 IO 不创建目录——spec 把创建责任放在写入时（0700 由写入方尝试）。

## 后果

- 正向：零三方依赖下 Windows verbatim 坑（F19）与 `..` 逃逸坑（F21）结构性封堵；判定算法有 dunce/MSDN 权威锚，非自造密码学级风险面；分配面逐路径钉死（`tests/paths_alloc.rs` 事件口径）。
- 代价（如实声明）：dunce 上游修 bug 需手工对表同步（算法面锁版本 1.0.5 语义）；Windows Known Folders 面缺位（XDG 恒 None），需要时的采用方自持 `SHGetKnownFolderPath` FFI。
- 测试面：`tests/paths_alloc.rs`（分配锚逐字）/ `tests/paths_io.rs` / `tests/paths_xdg.rs`（全矩阵零 env 竞争——`resolve_xdg_dir` 纯函数化）三档钉死；性能七维报告 reports/2026-10-16-b1-paths.md（CPU 纯函数域收窄声明）。
