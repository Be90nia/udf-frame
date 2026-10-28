#![cfg(feature = "json")]
//! `json` 域行为断言（P1-9 上浮验收）：
//! ① 严格模式拒注释；② 容忍模式收注释；③ 超 64MiB 输入报错而非截断成功；④ 错误行列正确。
//!
//! 测试收拢在 `mod json` 下——`cargo test -p udf-std-ext --all-features json`
//! 按函数名路径过滤，恰好全量命中本文件。

mod json {
    use std::io::Cursor;

    use std_ext::json::{decode, decode_slice, decode_str, JsonMode, MAX_INPUT_BYTES};

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
    fn unterminated_block_comment_is_tolerated_strip() {
        // 未闭合块注释：剥离到输入末尾（与提炼来源行为一致）
        let v = decode_str("{ \"a\": 1 } /* trailing", JsonMode::Tolerant).unwrap();
        assert_eq!(v["a"], 1);
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
