//! Localhost control API — inspect live sessions and inject faults at
//! runtime. Plain JSON over the same tiny HTTP stack as the backpage.

use crate::http::{Request, Response};
use crate::state::{SessionCmd, Shared, now_secs};
use serde_json::{Value, json};
use std::sync::Arc;
use tokio::net::TcpListener;

/// Serve the control API.
pub async fn serve(
    listener: TcpListener,
    shared: Arc<Shared>,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> std::io::Result<()> {
    crate::http::serve(listener, "control", shutdown, move |req| {
        let shared = shared.clone();
        let shutdown_tx = shutdown_tx.clone();
        async move { handle(req, &shared, &shutdown_tx).await }
    })
    .await
}

async fn handle(
    req: Request,
    shared: &Arc<Shared>,
    shutdown_tx: &tokio::sync::watch::Sender<bool>,
) -> Response {
    let segments: Vec<&str> = req.path.trim_matches('/').split('/').collect();
    match (req.method.as_str(), segments.as_slice()) {
        ("GET", ["status"]) => Response::json(&status(shared)),
        ("GET", ["sessions"]) => Response::json(&json!({
            "sessions": sessions_view(shared),
        })),
        ("GET", ["accounts"]) => Response::json(&json!({
            "accounts": shared
                .cfg
                .backpage
                .accounts
                .iter()
                .map(crate::backpage::account_view)
                .collect::<Vec<_>>(),
        })),
        ("GET", ["events"]) => {
            let events: Vec<_> = shared.events.lock().unwrap().iter().cloned().collect();
            Response::json(&json!({"events": events}))
        }
        ("GET", ["proxy", "log"]) => {
            let events: Vec<_> = shared
                .proxy_events
                .lock()
                .unwrap()
                .iter()
                .cloned()
                .collect();
            Response::json(&json!({"connections": events}))
        }
        ("GET", ["faults"]) => Response::json(&json!({"faults": shared.faults()})),
        ("POST", ["faults"]) => match serde_json::from_slice::<Value>(&req.body) {
            Ok(patch) => match shared.patch_faults(&patch) {
                Ok(()) => {
                    shared.event("faults", format!("patched: {}", truncate(&req.body)));
                    Response::json(&json!({"ok": true, "faults": shared.faults()}))
                }
                Err(e) => Response::new(
                    400,
                    "application/json",
                    json!({"ok": false, "error": e.to_string()})
                        .to_string()
                        .into_bytes(),
                ),
            },
            Err(e) => Response::new(
                400,
                "application/json",
                json!({"ok": false, "error": e.to_string()})
                    .to_string()
                    .into_bytes(),
            ),
        },
        ("POST", ["faults", "reset"]) => {
            *shared.faults.write().unwrap() = shared.cfg.faults.clone();
            shared.event("faults", "reset to config defaults");
            Response::json(&json!({"ok": true, "faults": shared.faults()}))
        }
        // /sessions/{uid}/kick|drop|kill
        ("POST", ["sessions", uid, action]) => {
            let Ok(uid) = uid.parse::<i32>() else {
                return Response::new(400, "text/plain", b"bad user id".to_vec());
            };
            let cmd = match *action {
                "kick" | "drop" => SessionCmd::Drop {
                    reason: format!("control api {action}"),
                },
                "kill" => SessionCmd::KillHero,
                _ => return Response::not_found(),
            };
            // Clone the sender — the session task owns registry cleanup.
            let cmd_tx = shared
                .sessions
                .lock()
                .unwrap()
                .get(&uid)
                .map(|e| e.cmd.clone());
            match cmd_tx {
                Some(tx) => {
                    let _ = tx.send(cmd).await;
                    shared.event("session-cmd", format!("user={uid} action={action}"));
                    Response::json(&json!({"ok": true, "user_id": uid, "action": action}))
                }
                None => Response::new(404, "text/plain", b"no such session".to_vec()),
            }
        }
        // Mint a fresh sid for a configured account — handy when a test
        // wants the `dosid` path without a full password login.
        ("POST", ["sids"]) => {
            let body: Value = serde_json::from_slice(&req.body).unwrap_or(json!({}));
            let user_id = body["user_id"].as_i64().unwrap_or(0) as i32;
            match shared
                .cfg
                .backpage
                .accounts
                .iter()
                .find(|a| a.user_id == user_id)
            {
                Some(acc) => {
                    let sid = shared.issue_sid(acc);
                    Response::json(&json!({"ok": true, "sid": sid, "user_id": user_id}))
                }
                None => Response::new(404, "text/plain", b"no such account".to_vec()),
            }
        }
        ("POST", ["shutdown"]) => {
            let _ = shutdown_tx.send(true);
            Response::json(&json!({"ok": true}))
        }
        ("GET", []) | ("GET", [""]) => Response::json(&json!({
            "service": "xenomit-mock-game control",
            "endpoints": [
                "GET  /status",
                "GET  /sessions",
                "POST /sessions/{user_id}/kick|drop|kill",
                "GET  /accounts",
                "GET  /events",
                "GET  /faults",
                "POST /faults        (JSON merge patch)",
                "POST /faults/reset",
                "GET  /proxy/log",
                "POST /sids          (mint dosid for a user_id)",
                "POST /shutdown",
            ],
        })),
        _ => Response::not_found(),
    }
}

fn status(shared: &Arc<Shared>) -> Value {
    let addrs = shared.addrs.lock().unwrap().clone();
    json!({
        "uptime_s": shared.started.elapsed().as_secs(),
        "now_unix": now_secs(),
        "seed": shared.cfg.seed,
        "addrs": {
            "backpage": addrs.backpage.map(|a| a.to_string()),
            "game": addrs.game.map(|a| a.to_string()),
            "proxy": addrs.proxy.map(|a| a.to_string()),
            "control": addrs.control.map(|a| a.to_string()),
        },
        "http_requests": shared.http_requests.load(std::sync::atomic::Ordering::Relaxed),
        "game_logins": shared.game_logins.load(std::sync::atomic::Ordering::Relaxed),
        "game_logins_rejected": shared.game_logins_rejected.load(std::sync::atomic::Ordering::Relaxed),
        "proxy_connects": shared.proxy_connects.load(std::sync::atomic::Ordering::Relaxed),
        "live_sessions": shared.sessions.lock().unwrap().len(),
        "issued_sids": shared.sids.lock().unwrap().len(),
    })
}

fn sessions_view(shared: &Arc<Shared>) -> Vec<Value> {
    shared
        .sessions
        .lock()
        .unwrap()
        .values()
        .map(|e| {
            json!({
                "user_id": e.user_id,
                "peer": e.peer.to_string(),
                "connected_for_s": e.connected_at.elapsed().as_secs(),
                "rx_packets": e.stats.rx_packets.load(std::sync::atomic::Ordering::Relaxed),
                "tx_packets": e.stats.tx_packets.load(std::sync::atomic::Ordering::Relaxed),
            })
        })
        .collect()
}

fn truncate(bytes: &[u8]) -> String {
    let s = String::from_utf8_lossy(bytes);
    if s.len() > 300 {
        s[..300].to_string()
    } else {
        s.into_owned()
    }
}
