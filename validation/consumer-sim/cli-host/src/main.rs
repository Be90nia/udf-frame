//! 任务调度 CLI 宿主入口：一进程一命令（submit / cancel / shutdown），
//! `--features tokio` 追加 `cancel-async` 取消探针。

use std::process::ExitCode;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();

    #[cfg(feature = "tokio")]
    if matches!(args.first().map(String::as_str), Some("cancel-async")) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("tokio current-thread runtime");
        return match runtime.block_on(cli_host::async_host::run_cancellation_probe()) {
            Ok(line) => {
                println!("{line}");
                ExitCode::SUCCESS
            }
            Err(err) => {
                eprintln!("error: {err}");
                ExitCode::FAILURE
            }
        };
    }

    match cli_host::run_cli(&args) {
        Ok(line) => {
            println!("{line}");
            ExitCode::SUCCESS
        }
        Err(err) => {
            eprintln!("error: {err}");
            ExitCode::FAILURE
        }
    }
}
