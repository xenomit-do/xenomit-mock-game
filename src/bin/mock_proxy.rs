//! `mock-proxy` — standalone CONNECT forward proxy for proxy-aware
//! client tests. A trimmed-down `xenomit-mock-game` without the
//! Backpage/game parts: the proxy needs only the shared fault/event
//! state, which this binary serves on a lightweight control API.

use std::process::ExitCode;
use xenomit_mock_game::{Config, Shared};

const USAGE: &str = "\
mock-proxy — standalone CONNECT forward proxy.

USAGE:
  mock-proxy [options]

OPTIONS:
  --listen <host:port>    Proxy listen address (default 127.0.0.1:0)
  --control <host:port>   Control-API address for /proxy/log + /faults
                        (default 127.0.0.1:0; disabled by --no-control)
  --no-control            Disable the control API
  --allow <host[:port]>   Restrict CONNECT targets (repeatable)
  --deny                  Reject every CONNECT with 403
  --latency <ms>          Fixed delay before honoring CONNECT
  -h, --help              Show this help
";

fn main() -> ExitCode {
    let mut cfg = Config::default();
    cfg.proxy.enabled = true;
    cfg.control.enabled = true;
    let mut deny = false;
    let mut latency = 0u64;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--listen" => match args.next() {
                Some(v) => cfg.proxy.listen = v,
                None => return usage_err("--listen needs host:port"),
            },
            "--control" => match args.next() {
                Some(v) => {
                    cfg.control.enabled = true;
                    cfg.control.listen = v;
                }
                None => return usage_err("--control needs host:port"),
            },
            "--no-control" => cfg.control.enabled = false,
            "--allow" => match args.next() {
                Some(v) => cfg.proxy.allow.push(v),
                None => return usage_err("--allow needs host[:port]"),
            },
            "--deny" => deny = true,
            "--latency" => match args.next().and_then(|v| v.parse().ok()) {
                Some(v) => latency = v,
                None => return usage_err("--latency needs ms"),
            },
            "-h" | "--help" => {
                print!("{USAGE}");
                return ExitCode::SUCCESS;
            }
            other => {
                eprintln!("error: unknown flag {other}\n\n{USAGE}");
                return ExitCode::FAILURE;
            }
        }
    }
    cfg.faults.proxy.deny = deny;
    cfg.faults.proxy.latency_ms = latency;

    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("error: tokio runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    rt.block_on(async move {
        let shared = std::sync::Arc::new(Shared::new(cfg));
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let listener = match tokio::net::TcpListener::bind(&shared.cfg.proxy.listen).await {
            Ok(l) => l,
            Err(e) => {
                eprintln!("error: cannot bind {}: {e}", shared.cfg.proxy.listen);
                return ExitCode::FAILURE;
            }
        };
        let proxy_addr = listener.local_addr().unwrap();
        let mut tasks = vec![tokio::spawn(xenomit_mock_game::proxy::serve(
            listener,
            shared.clone(),
            shutdown_rx.clone(),
        ))];
        let mut control_addr = None;
        if shared.cfg.control.enabled {
            match tokio::net::TcpListener::bind(&shared.cfg.control.listen).await {
                Ok(l) => {
                    control_addr = l.local_addr().ok();
                    tasks.push(tokio::spawn(xenomit_mock_game::control::serve(
                        l,
                        shared.clone(),
                        shutdown_tx.clone(),
                        shutdown_rx.clone(),
                    )));
                }
                Err(e) => eprintln!("warning: control api bind failed: {e}"),
            }
        }
        println!("mock-proxy ready");
        println!("  proxy:   {proxy_addr}");
        if let Some(a) = control_addr {
            println!("  control: http://{a}");
        }
        tokio::signal::ctrl_c().await.ok();
        let _ = shutdown_tx.send(true);
        for t in tasks {
            let _ = t.await;
        }
        ExitCode::SUCCESS
    })
}

fn usage_err(msg: &str) -> ExitCode {
    eprintln!("error: {msg}\n\n{USAGE}");
    ExitCode::FAILURE
}
