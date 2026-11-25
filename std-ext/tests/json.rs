#![cfg(feature = "json")]
//! `json` 域行为断言（P1-9 上浮验收 + #196/#125 注释=空白面）：
//! ① 严格模式拒注释；② 容忍模式收注释；③ 超 64MiB 输入报错而非截断成功；④ 错误行列正确；
//! ⑤ 注释不能拼接相邻 token（`1/*c*/2` 等 4 例）；⑥ 未闭合块注释报错并带原始开注行列；
//! ⑦ 多行块注释内部换行保留（后续错误行号与原始输入一致）；⑧ 字符串内标记/转义/多字节逐字节不变。
//!
//! 测试收拢在 `mod json` 下——`cargo test -p udf-std-ext --all-features json`
//! 按函数名路径过滤，恰好全量命中本文件。

mod json {
    use std::io::Cursor;

    use std_ext::json::{JsonMode, MAX_INPUT_BYTES, decode, decode_slice, decode_str};

    #[test]
    fn strict_mode_rejects_comments() {
        let err = decode_str("// hello\n{ \"a\": 1 }", JsonMode::Strict).unwrap_err();
        // 严格模式下注释是语法错误，位置在第 1 行
        assert_eq!(err.line, 1);
        assert!(err.column >= 1);
    }

    #[test]
    fn tolerant_mode_accepts_line_and_block_comments() {
        let v = decode_str("// hello\n/* config */ { \"a\": 1 }", JsonMode::Tolerant).unwrap();
        assert_eq!(v["a"], 1);
    }

    #[test]
    fn string_literals_keep_comment_markers() {
        let v = decode_str(
            r#"{"url": "https://example.com/x /* not a comment */"}"#,
            JsonMode::Tolerant,
        )
        .unwrap();
        assert_eq!(v["url"], "https://example.com/x /* not a comment */");
    }

    #[test]
    fn escaped_quote_inside_string_keeps_markers() {
        let v = decode_str(r#"{"s": "a \" // b"}"#, JsonMode::Tolerant).unwrap();
        assert_eq!(v["s"], "a \" // b");
    }

    #[test]
    fn unterminated_block_comment_is_error_with_opening_position() {
        // #125：未闭合块注释必须报错（旧口径「剥离到 EOF」会让畸形配置被接受）
        let err = decode_str("{ \"a\": 1 } /* trailing", JsonMode::Tolerant).unwrap_err();
        assert!(
            err.message.contains("unterminated block comment"),
            "got: {}",
            err.message
        );
        assert_eq!(
            (err.line, err.column),
            (1, 12),
            "位置=原始输入里开注的 `/`（1 起）"
        );

        // 多行输入：行列按原始输入的换行换算
        let err = decode_str(
            "{\n  \"a\": 1,\n  /* open\n  \"b\": 2\n}",
            JsonMode::Tolerant,
        )
        .unwrap_err();
        assert_eq!((err.line, err.column), (3, 3), "got: {err:?}");
    }

    #[test]
    fn block_comment_cannot_merge_adjacent_tokens() {
        // #196/#125：注释是词法空白——「删除式」剥离会把两侧 token 拼成另一个合法值
        for input in [
            r#"{"x": 1/*c*/.5}"#, // 旧：1.5（合法数字）
            r#"{"x": 1/*c*/e3}"#, // 旧：1e3 = 1000
            r#"1/*c*/2"#,         // 旧：12
            r#"tru/*c*/e"#,       // 旧：true
        ] {
            let err = decode_str(input, JsonMode::Tolerant).unwrap_err();
            assert!(
                (err.line, err.column) != (0, 0),
                "{input} 必须是带位置的解析错误"
            );
        }

        // 另一侧：注释出现在 token 之间本就合法的位置，剥离后照常解析
        let v = decode_str("[1/*c*/, 2]", JsonMode::Tolerant).unwrap();
        assert_eq!(v[1], 2);
        let v = decode_str(r#"{"a"/*c*/: 1}"#, JsonMode::Tolerant).unwrap();
        assert_eq!(v["a"], 1);
    }

    #[test]
    fn block_comment_newlines_are_preserved_for_positions() {
        // 多行块注释保留内部的换行 ⇒ 后续错误的行号仍与原始输入一致
        let input = "/* line1\nline2 */\n{\n  \"a\" 1\n}";
        let err = decode_str(input, JsonMode::Tolerant).unwrap_err();
        assert_eq!(err.line, 4, "原始输入第 4 行缺冒号（got: {err:?}）");
    }

    #[test]
    fn string_contents_and_multibyte_bytes_survive_verbatim() {
        // 字符串内的注释标记/转义/多字节原样保留；紧随其后的块注释按空白处理
        let v = decode_str(
            "{\"s\": \"日本 /*x*/ \\\" //y\"/*c*/, \"t\": 2}",
            JsonMode::Tolerant,
        )
        .unwrap();
        assert_eq!(v["s"], "日本 /*x*/ \" //y");
        assert_eq!(v["t"], 2);
    }

    #[test]
    fn input_over_limit_is_error_not_truncation() {
        // 前 64MiB 截断后仍是合法 JSON（`{ "a": 1 }` + 空格）——锁定 fail-visible 而非截断成功
        let mut big = "{ \"a\": 1 }".to_string();
        big.push_str(&" ".repeat(MAX_INPUT_BYTES));

        let err = decode_str(&big, JsonMode::Tolerant).unwrap_err();
        assert!(err.message.contains("exceeds"), "got: {}", err.message);
        assert_eq!((err.line, err.column), (0, 0));

        let err = decode(Cursor::new(big.into_bytes()), JsonMode::Tolerant).unwrap_err();
        assert!(err.message.contains("exceeds"), "got: {}", err.message);
    }

    #[test]
    fn error_carries_line_and_column() {
        let input = "{\n  \"a\": 1,\n}\n";
        let err = decode_str(input, JsonMode::Strict).unwrap_err();
        // 行列与 serde_json 自身报告一致（1 起）
        let raw = serde_json::from_str::<serde_json::Value>(input).unwrap_err();
        assert_eq!(
            (err.line as usize, err.column as usize),
            (raw.line(), raw.column())
        );
        assert_eq!(
            (err.line, err.column),
            (3, 1),
            "trailing comma: line 3 column 1"
        );
    }

    #[test]
    fn slice_and_reader_paths_agree() {
        let v = decode_slice(b"// c\n{ \"a\": 2 }", JsonMode::Tolerant).unwrap();
        assert_eq!(v["a"], 2);

        let v = decode(Cursor::new(br#"{ "a": 3 }"#.to_vec()), JsonMode::Strict).unwrap();
        assert_eq!(v["a"], 3);
    }

    #[test]
    fn invalid_utf8_slice_reports_positionless_error() {
        let err = decode_slice(b"{\"a\": \"\xff\"}", JsonMode::Tolerant).unwrap_err();
        assert!(err.message.contains("UTF-8"), "got: {}", err.message);
        assert_eq!((err.line, err.column), (0, 0));
    }
}
