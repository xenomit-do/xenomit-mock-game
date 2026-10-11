//! `xenomit-mock-game` CLI — start the mock Backpage + game server +
//! proxy + control API with JSON/flag configuration.

use std::process::ExitCode;
use xenomit_mock_game::{Config, MockServer};

const USAGE: &str = "\
xenomit-mock-game — deterministic mock of the DarkOrbit Backpage and
game server for bot tests and benchmarks.

USAGE:
  xenomit-mock-game [options]

OPTIONS:
  --config <file.json>    Load configuration from a JSON file
                          (flags below override file values)
  --seed <n>              World-simulation seed (default: fixed)
  --backpage <host:port>  Backpage listen address (default 127.0.0.1:0)
  --game <host:port>      Game server listen address (default 127.0.0.1:0)
  --proxy <host:port>     Forward-proxy listen address
  --control <host:port>   Control-API listen address
  --no-proxy              Disable the forward proxy
  --no-control            Disable the control API
  --session-ttl <secs>    dosid lifetime (default 1800; 0 = never)
  --map <id>              Starting map id (default 16)
  --npcs <n>              NPC ships per map (default 6)
  --account <u:p[:id]>    Add an account (repeatable; default bot:bot-pass:1001)
  --print-config          Print the effective JSON config and exit
  -h, --help              Show this help

Once running, the control API (default enabled) exposes:
  GET  /status | /sessions | /accounts | /events | /faults | /proxy/log
  POST /faults  /faults/reset  /sessions/{uid}/{kick|drop|kill}
  POST /sids    /shutdown
";

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let mut cfg = Config::default();
    let mut file_loaded = false;
    let mut accounts: Vec<xenomit_mock_game::config::Account> = Vec::new();
    let mut print_config = false;

    // --config must resolve before other flags so they can override it.
    let argv: Vec<String> = std::env::args().skip(1).collect();
    for (i, a) in argv.iter().enumerate() {
        if a == "--config"
            && let Some(path) = argv.get(i + 1)
        {
            match Config::from_file(std::path::Path::new(path)) {
                Ok(c) => {
                    cfg = c;
                    file_loaded = true;
                }
                Err(e) => {
                    eprintln!("error: cannot load {path}: {e}");
                    return ExitCode::FAILURE;
                }
            }
        }
    }
    // Flags applied after the file override it.
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                args.next();
            }
            "--seed" => match args.next().and_then(|v| v.parse().ok()) {
                Some(v) => cfg.seed = v,
                None => return usage_err("--seed needs a number"),
            },
            "--backpage" => match args.next() {
                Some(v) => cfg.backpage.listen = v,
                None => return usage_err("--backpage needs host:port"),
            },
            "--game" => match args.next() {
                Some(v) => cfg.game.listen = v,
                None => return usage_err("--game needs host:port"),
            },
            "--proxy" => match args.next() {
                Some(v) => {
                    cfg.proxy.enabled = true;
                    cfg.proxy.listen = v;
                }
                None => return usage_err("--proxy needs host:port"),
            },
            "--control" => match args.next() {
                Some(v) => {
                    cfg.control.enabled = true;
                    cfg.control.listen = v;
                }
                None => return usage_err("--control needs host:port"),
            },
            "--no-proxy" => cfg.proxy.enabled = false,
            "--no-control" => cfg.control.enabled = false,
            "--session-ttl" => match args.next().and_then(|v| v.parse().ok()) {
                Some(v) => cfg.backpage.session_ttl_secs = v,
                None => return usage_err("--session-ttl needs seconds"),
            },
            "--map" => match args.next().and_then(|v| v.parse().ok()) {
                Some(v) => cfg.game.map_id = v,
                None => return usage_err("--map needs an id"),
            },
            "--npcs" => match args.next().and_then(|v| v.parse().ok()) {
                Some(v) => cfg.game.npc_count = v,
                None => return usage_err("--npcs needs a count"),
            },
            "--account" => match args.next() {
                Some(v) => match parse_account(&v) {
                    Some(a) => accounts.push(a),
                    None => return usage_err("--account needs user:pass[:id]"),
                },
                None => return usage_err("--account needs user:pass[:id]"),
            },
            "--print-config" => print_config = true,
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
    if !accounts.is_empty() {
        cfg.backpage.accounts = accounts;
    } else if !file_loaded && cfg.backpage.accounts.is_empty() {
        cfg.backpage.accounts.push(Default::default());
    }
    if print_config {
        println!("{}", serde_json::to_string_pretty(&cfg).unwrap());
        return ExitCode::SUCCESS;
    }

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
        let server = match MockServer::start(cfg).await {
            Ok(s) => s,
            Err(e) => {
                eprintln!("error: cannot start mock: {e}");
                return ExitCode::FAILURE;
            }
        };
        let addrs = server.shared.addrs.lock().unwrap().clone();
        println!("xenomit-mock-game ready");
        println!("  backpage: http://{}", addrs.backpage.unwrap());
        println!("  game:     {}", addrs.game.unwrap());
        if let Some(a) = addrs.proxy {
            println!("  proxy:    {a}");
        }
        if let Some(a) = addrs.control {
            println!("  control:  http://{a}");
        }
        // Run until Ctrl-C or POST /shutdown.
        tokio::signal::ctrl_c().await.ok();
        println!("shutting down");
        server.shutdown().await;
        ExitCode::SUCCESS
    })
}

fn parse_account(spec: &str) -> Option<xenomit_mock_game::config::Account> {
    let mut parts = spec.splitn(3, ':');
    let username = parts.next()?.to_string();
    let password = parts.next()?.to_string();
    let user_id = parts.next().and_then(|v| v.parse().ok()).unwrap_or(0);
    let user_id = if user_id == 0 {
        // Stable id derived from the name when not given.
        (username
            .bytes()
            .fold(0u32, |a, b| a.wrapping_mul(31) + b as u32)
            % 900_000
            + 1000) as i32
    } else {
        user_id
    };
    Some(xenomit_mock_game::config::Account {
        username,
        password,
        user_id,
        ..Default::default()
    })
}

fn usage_err(msg: &str) -> ExitCode {
    eprintln!("error: {msg}\n\n{USAGE}");
    ExitCode::FAILURE
}
