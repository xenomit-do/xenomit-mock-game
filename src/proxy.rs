//! Minimal forward proxy: HTTP `CONNECT` tunnels plus absolute-form
//! `GET`/`POST` forwarding. Every CONNECT is logged (peer, target,
//! byte counts); game-protocol `LoginRequest`s passing through the
//! tunnel are sniffed so the log shows *which* bot connected.

use crate::http::{Request, Response, read_request, write_response};
use crate::state::{ProxyEvent, Shared, now_secs};
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use xenomit_packet_client::frame::FrameDecoder;
use xenomit_packet_client::modules::id;
use xenomit_packet_client::packet::decode_body;

const MAX_RELAY: usize = 64 * 1024;

/// Serve the proxy until shutdown fires.
pub async fn serve(
    listener: TcpListener,
    shared: Arc<Shared>,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> io::Result<()> {
    let mut shutdown = shutdown;
    loop {
        let (stream, peer) = tokio::select! {
            r = listener.accept() => r?,
            _ = shutdown.changed() => return Ok(()),
        };
        stream.set_nodelay(true).ok();
        let shared = shared.clone();
        let mut shutdown = shutdown.clone();
        tokio::spawn(async move {
            let _ = tokio::select! {
                r = handle_conn(stream, peer, shared) => r,
                _ = shutdown.changed() => Ok(()),
            };
        });
    }
}

async fn handle_conn(stream: TcpStream, peer: SocketAddr, shared: Arc<Shared>) -> io::Result<()> {
    let faults = shared.faults().proxy;
    let (mut rd, mut wr) = stream.into_split();
    let Some(req) = read_request(&mut rd, peer).await? else {
        return Ok(());
    };
    shared.proxy_connects.fetch_add(1, Ordering::Relaxed);
    if faults.latency_ms > 0 {
        tokio::time::sleep(Duration::from_millis(faults.latency_ms)).await;
    }
    if faults.deny {
        shared.proxy_event(ProxyEvent {
            at_unix: now_secs(),
            peer: peer.to_string(),
            target: req.target.clone(),
            outcome: "denied".into(),
            bytes_up: 0,
            bytes_down: 0,
            detail: Some("proxy deny fault".into()),
        });
        return write_response(
            &mut wr,
            Response::new(403, "text/plain", b"denied".to_vec()),
        )
        .await;
    }
    if req.method.eq_ignore_ascii_case("connect") {
        return handle_connect(req, rd, wr, peer, shared).await;
    }
    handle_forward(req, &mut wr, peer, shared).await
}

/// Split `host[:port]` — CONNECT targets usually carry a port; default
/// 443 per RFC 7231, and 80 for absolute-form forward requests.
fn parse_target(target: &str, default_port: u16) -> Option<(String, u16)> {
    let target = target.trim();
    if let Some((host, port)) = target.rsplit_once(':')
        && let Ok(port) = port.parse::<u16>()
    {
        return Some((host.to_string(), port));
    }
    Some((target.to_string(), default_port))
}

fn allowed(shared: &Shared, host: &str, port: u16) -> bool {
    let allow = &shared.cfg.proxy.allow;
    if allow.is_empty() {
        return true;
    }
    let hostport = format!("{host}:{port}");
    allow.iter().any(|a| a == host || *a == hostport)
}

async fn handle_connect(
    req: Request,
    mut client_rd: tokio::net::tcp::OwnedReadHalf,
    mut client_wr: tokio::net::tcp::OwnedWriteHalf,
    peer: SocketAddr,
    shared: Arc<Shared>,
) -> io::Result<()> {
    let Some((host, port)) = parse_target(&req.target, 443) else {
        return write_response(
            &mut client_wr,
            Response::new(400, "text/plain", b"bad target".to_vec()),
        )
        .await;
    };
    if !allowed(&shared, &host, port) {
        shared.proxy_event(ProxyEvent {
            at_unix: now_secs(),
            peer: peer.to_string(),
            target: req.target.clone(),
            outcome: "not-allowed".into(),
            bytes_up: 0,
            bytes_down: 0,
            detail: None,
        });
        return write_response(
            &mut client_wr,
            Response::new(403, "text/plain", b"not allowed".to_vec()),
        )
        .await;
    }
    let upstream = match TcpStream::connect((host.as_str(), port)).await {
        Ok(s) => s,
        Err(e) => {
            shared.proxy_event(ProxyEvent {
                at_unix: now_secs(),
                peer: peer.to_string(),
                target: req.target.clone(),
                outcome: format!("dial-failed: {e}"),
                bytes_up: 0,
                bytes_down: 0,
                detail: None,
            });
            return write_response(
                &mut client_wr,
                Response::new(502, "text/plain", b"upstream unreachable".to_vec()),
            )
            .await;
        }
    };
    upstream.set_nodelay(true).ok();
    // Bare status line — no `Connection: close`, the tunnel stays open.
    client_wr
        .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
        .await?;
    let (mut up_rd, mut up_wr) = upstream.into_split();
    let started = Instant::now();

    // Up direction: sniff game frames so the log can show the bot's
    // login identity; all bytes pass through unchanged.
    let shared_up = shared.clone();
    let target = req.target.clone();
    let up = tokio::spawn(async move {
        let mut dec = FrameDecoder::new();
        let mut seen_greeting = false;
        let mut sniffed_login: Option<String> = None;
        let mut bytes = 0u64;
        let mut buf = [0u8; MAX_RELAY];
        loop {
            let n = match client_rd.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => n,
            };
            let chunk = &buf[..n];
            bytes += chunk.len() as u64;
            // The game protocol opens with one 0x00 byte then framed
            // modules. Sniffed parsing is best-effort only.
            let mut offset = 0;
            if !seen_greeting {
                seen_greeting = chunk.first() == Some(&0);
                offset = 1;
            }
            if seen_greeting && offset < chunk.len() {
                dec.push(&chunk[offset..]);
                while let Some(body) = dec.next_body() {
                    if sniffed_login.is_none()
                        && let Ok(p) = decode_body(&body, false)
                        && p.id == id::LOGIN_REQUEST
                    {
                        let user = p.int("userID").unwrap_or(0);
                        let sid = p.string("sessionID").unwrap_or_default();
                        sniffed_login = Some(format!(
                            "game-login user={user} sid={}…",
                            &sid[..sid.len().min(8)]
                        ));
                        shared_up.event("proxy-login", sniffed_login.clone().unwrap());
                    }
                }
            }
            if up_wr.write_all(chunk).await.is_err() {
                break;
            }
        }
        let _ = up_wr.shutdown().await;
        (bytes, sniffed_login)
    });
    // Down direction: byte counting only.
    let dn = tokio::spawn(async move {
        let mut bytes = 0u64;
        let mut buf = [0u8; MAX_RELAY];
        loop {
            let n = match up_rd.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => n,
            };
            bytes += n as u64;
            if client_wr.write_all(&buf[..n]).await.is_err() {
                break;
            }
        }
        let _ = client_wr.shutdown().await;
        bytes
    });

    let drop_after = shared
        .faults()
        .proxy
        .drop_after_ms
        .map(Duration::from_millis);
    let (outcome, bytes_up, bytes_down, detail) = match drop_after {
        Some(d) => match tokio::time::timeout(d, async { tokio::join!(up, dn) }).await {
            Ok((u, d)) => {
                let (up_bytes, login) = u.unwrap_or((0, None));
                ("closed", up_bytes, d.unwrap_or(0), login)
            }
            Err(_) => ("dropped", 0, 0, Some("drop_after_ms fault".to_string())),
        },
        None => {
            let (u, d) = tokio::join!(up, dn);
            let (up_bytes, login) = u.unwrap_or((0, None));
            ("closed", up_bytes, d.unwrap_or(0), login)
        }
    };
    let _ = started;
    shared.proxy_event(ProxyEvent {
        at_unix: now_secs(),
        peer: peer.to_string(),
        target,
        outcome: outcome.into(),
        bytes_up,
        bytes_down,
        detail,
    });
    Ok(())
}

/// Absolute-form (`GET http://host/path`) plain forwarding — enough for
/// backpage-over-proxy tests, which is all the mock needs it for.
async fn handle_forward(
    req: Request,
    wr: &mut tokio::net::tcp::OwnedWriteHalf,
    peer: SocketAddr,
    shared: Arc<Shared>,
) -> io::Result<()> {
    // Minimal absolute-URI parse: `http://host[:port]/path?query`.
    let Some(rest) = req.target.strip_prefix("http://") else {
        return write_response(
            wr,
            Response::new(400, "text/plain", b"absolute http URI required".to_vec()),
        )
        .await;
    };
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    };
    let Some((host, port)) = parse_target(authority, 80) else {
        return write_response(
            wr,
            Response::new(400, "text/plain", b"bad authority".to_vec()),
        )
        .await;
    };
    if !allowed(&shared, &host, port) {
        return write_response(
            wr,
            Response::new(403, "text/plain", b"not allowed".to_vec()),
        )
        .await;
    }
    let mut upstream = match TcpStream::connect((host.as_str(), port)).await {
        Ok(s) => s,
        Err(e) => {
            return write_response(
                wr,
                Response::new(502, "text/plain", format!("dial failed: {e}").into_bytes()),
            )
            .await;
        }
    };
    upstream.set_nodelay(true).ok();
    let mut out = Vec::with_capacity(512 + req.body.len());
    out.extend_from_slice(format!("{} {} HTTP/1.1\r\n", req.method, path).as_bytes());
    let mut has_host = false;
    let mut has_len = false;
    for (name, value) in &req.headers {
        if name == "proxy-connection" || name == "connection" {
            continue;
        }
        if name == "host" {
            has_host = true;
        }
        if name == "content-length" {
            has_len = true;
        }
        out.extend_from_slice(format!("{name}: {value}\r\n").as_bytes());
    }
    if !has_host {
        out.extend_from_slice(format!("Host: {host}\r\n").as_bytes());
    }
    if !has_len && !req.body.is_empty() {
        out.extend_from_slice(format!("Content-Length: {}\r\n", req.body.len()).as_bytes());
    }
    out.extend_from_slice(b"Connection: close\r\n\r\n");
    out.extend_from_slice(&req.body);
    upstream.write_all(&out).await?;
    upstream.flush().await?;
    shared.event(
        "proxy-forward",
        format!("peer={peer} {} {}", req.method, req.target),
    );
    // Relay the upstream response verbatim until EOF.
    let mut buf = [0u8; MAX_RELAY];
    loop {
        let n = upstream.read(&mut buf).await?;
        if n == 0 {
            return Ok(());
        }
        wr.write_all(&buf[..n]).await?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn target_parsing() {
        assert_eq!(
            parse_target("example.com:8080", 443),
            Some(("example.com".into(), 8080))
        );
        assert_eq!(
            parse_target("example.com", 443),
            Some(("example.com".into(), 443))
        );
    }
}
