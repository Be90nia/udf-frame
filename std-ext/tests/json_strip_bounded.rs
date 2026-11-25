#![cfg(feature = "json")]
//! #195 大输入面：64 MiB ASCII 与多字节 UTF-8 的语义等价（无注释输入 Strict 与
//! Tolerant 必须同值）+ tolerant 剥离的峰值分配上界（淘汰 `Vec<char>` 的 4x 放大）。
//!
//! 分配口径同 `tests/paths_alloc.rs`：本文件自带字节计数分配器，只计**窗内本线程**
//! 分配（const TLS 门，libtest 并发线程不进窗）。`realloc` 按**新尺寸**计，故读数是
//! 上界而非净额——上界已足够判别「4x + 几何增长」与「单个输出缓冲」。

mod json_bounded {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;
    use std::hint::black_box;

    use std_ext::json::{JsonMode, MAX_INPUT_BYTES, decode_str};

    struct ByteCounting;

    // 计数窗门：开关与计数同住 TLS（const 初始化无惰性分配、无析构注册，分配器内
    // 读取安全；进程级原子会被同二进制其他线程的窗口污染）。
    thread_local! {
        static WINDOW: Cell<(bool, usize)> = const { Cell::new((false, 0)) };
    }

    fn note(bytes: usize) {
        let _ = WINDOW.try_with(|c| {
            let (open, n) = c.get();
            if open {
                c.set((true, n + bytes));
            }
        });
    }

    // SAFETY: 纯转发 System，仅在窗内转发前累加字节数；null 原样透传（alloc 契约）。
    unsafe impl GlobalAlloc for ByteCounting {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            note(layout.size());
            unsafe { System.alloc(layout) }
        }
        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            note(layout.size());
            unsafe { System.alloc_zeroed(layout) }
        }
        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            unsafe { System.dealloc(ptr, layout) }
        }
        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            note(new_size);
            unsafe { System.realloc(ptr, layout, new_size) }
        }
    }

    #[global_allocator]
    static COUNTING: ByteCounting = ByteCounting;

    /// op 执行期窗内本线程申请的总字节数（瞬时生灭计入；结果活到函数尾）。
    fn alloc_bytes<T>(op: impl FnOnce() -> T) -> usize {
        WINDOW.with(|c| c.set((true, 0)));
        let out = op();
        black_box(&out);
        let bytes = WINDOW.with(|c| c.get().1);
        WINDOW.with(|c| c.set((false, 0)));
        bytes
    }

    /// 仪器自检：窗内分配必须可见（防分配器没挂上导致假绿）。
    #[test]
    fn alloc_counter_sees_window_allocations() {
        let bytes = alloc_bytes(|| Vec::<u8>::with_capacity(32));
        assert_eq!(bytes, 32, "窗内 32 字节分配必须计 32（仪器在位）");
    }

    fn doc_with_payload(payload: &str) -> String {
        format!("{{\"s\":\"{payload}\"}}")
    }

    /// 无注释的最大尺寸输入在两种模式下同值——ASCII 与多字节 UTF-8 各一（多字节
    /// 字节序被字节扫描器原样拷贝，不改变语义）。
    #[test]
    fn max_size_input_is_semantically_identical_in_both_modes() {
        let ascii_len = MAX_INPUT_BYTES - 8; // {"s":" … "} 外壳 8 字节
        let ascii = doc_with_payload(&"a".repeat(ascii_len));
        assert_eq!(ascii.len(), MAX_INPUT_BYTES);
        let tolerant = decode_str(&ascii, JsonMode::Tolerant).unwrap();
        let strict = decode_str(&ascii, JsonMode::Strict).unwrap();
        assert_eq!(tolerant, strict, "ASCII：无注释输入两模式必须同值");
        assert_eq!(tolerant["s"].as_str().unwrap().len(), ascii_len);

        // 多字节：日（3 字节）铺满上限以内，余量留给外壳
        let multibyte_len = (MAX_INPUT_BYTES - 8) / 3;
        let multibyte = doc_with_payload(&"日".repeat(multibyte_len));
        assert!(multibyte.len() <= MAX_INPUT_BYTES);
        let tolerant = decode_str(&multibyte, JsonMode::Tolerant).unwrap();
        let strict = decode_str(&multibyte, JsonMode::Strict).unwrap();
        assert_eq!(tolerant, strict, "多字节：无注释输入两模式必须同值");
        assert_eq!(
            tolerant["s"].as_str().unwrap(),
            "日".repeat(multibyte_len),
            "多字节序列逐字节不变"
        );
    }

    /// 剥离路径的分配上界：注释占满输入时，窗内申请应 ≈ 单个输出缓冲（= 输入长度
    /// 上界）。旧 `Vec<char>` 路径仅字符向量就要 4x 输入 + 几何增长（合计 ≥ 5x），
    /// 2x 的阈值对两条路径有判别力。
    #[test]
    fn tolerant_strip_allocation_stays_within_input_bound() {
        // ASCII：一条吃掉余下全部输入的 `//` 行注释
        let mut ascii = String::from("{\"a\":1} // ");
        ascii.push_str(&"x".repeat(MAX_INPUT_BYTES - ascii.len()));
        assert_eq!(ascii.len(), MAX_INPUT_BYTES);
        let bytes = alloc_bytes(|| decode_str(&ascii, JsonMode::Tolerant).unwrap());
        assert!(
            bytes < 2 * MAX_INPUT_BYTES,
            "ASCII 行注释：窗内申请 {bytes} 字节，超 2x 输入上限（旧 Vec<char> 路径 ≥ 5x）"
        );

        // 多字节：一条吃掉余下全部输入的 `/* */` 块注释（换行保留逻辑同路径）
        let mut multibyte = String::from("{\"a\":1} /* ");
        // 每次加「日」(3) 后仍要容得下收尾 ` */`(3)
        while multibyte.len() + 6 <= MAX_INPUT_BYTES {
            multibyte.push('日');
        }
        multibyte.push_str(" */");
        assert!(multibyte.len() <= MAX_INPUT_BYTES);
        let v = decode_str(&multibyte, JsonMode::Tolerant).unwrap();
        assert_eq!(v["a"], 1, "多字节块注释剥离后语义不变");
        let bytes = alloc_bytes(|| decode_str(&multibyte, JsonMode::Tolerant).unwrap());
        assert!(
            bytes < 2 * MAX_INPUT_BYTES,
            "多字节块注释：窗内申请 {bytes} 字节，超 2x 输入上限（旧 Vec<char> 路径 ≥ 5x）"
        );
    }
}
