//! Phase 5b — proxy-aware clients.
//!
//! Exercises `xenomit_backpage`'s `*_via`/`with_proxy` path and
//! `xenomit-packet-client`'s `Session::connect_via*`/`proxy::dial`
//! against the bundled `mock-proxy` plus three test-local helpers: an
//! authenticating CONNECT proxy, a minimal SOCKS5 proxy and an
//! exit-IP echo server. Assertions lean on the control API: the mock
//! records proxy CONNECTs (with sniffed game logins) and live game
//! sessions, so "tunneled" vs "direct" is observable.

use serde_json::Value;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use xenomit_mock_game::{Config, MockServer};
use xenomit_packet_client::session::{LoginInfo, Session};
use xenomit_packet_client::{ClientError, ProxyConfig as TcpProxy, ProxyError};

type HttpProxy = xenomit_backpage::ProxyConfig;

fn test_config() -> Config {
    let mut cfg = Config::default();
    cfg.game.tick_ms = 50;
    cfg.backpage.session_ttl_secs = 60;
    cfg
}

/// flashvars → `LoginInfo`, mirroring `bot-runtime`'s mapping.
fn login_info(session: &xenomit_backpage::Session) -> LoginInfo {
    let vars = &session.flashvars;
    let mut login = LoginInfo::new(
        vars["userID"].parse::<i32>().unwrap(),
        vars["sessionID"].clone(),
    );
    login.faction_id = vars
        .get("factionID")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    login.instance_id = vars["pid"].parse().unwrap();
    login.is_mini_client = vars.get("platform").is_some_and(|v| v == "PC_CLIENT");
    login
}

async fn control_get(server: &MockServer, path: &str) -> Value {
    reqwest::Client::new()
        .get(format!("{}{path}", server.control_url().unwrap()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

/// Run a blocking client call off the async executor.
async fn blocking<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
    tokio::task::spawn_blocking(f).await.unwrap()
}

/// Poll `/status` until `pred` holds (bounded). The mock registers a
/// game login only after the whole init burst is sent, so assertions on
/// `game_logins`/`live_sessions` must wait for it rather than racing a
/// just-attached session — closing the socket mid-burst cancels the
/// registration entirely.
async fn wait_status(server: &MockServer, pred: impl Fn(&Value) -> bool) -> Value {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let status = control_get(server, "/status").await;
        if pred(&status) {
            return status;
        }
        assert!(
            Instant::now() < deadline,
            "status predicate never met: {status}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Read one HTTP request head (`\r\n\r\n`-terminated, bounded).
fn read_head(stream: &mut TcpStream, cap: usize) -> std::io::Result<Vec<u8>> {
    let mut head = Vec::with_capacity(512);
    let mut byte = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        stream.read_exact(&mut byte)?;
        head.push(byte[0]);
        if head.len() > cap {
            return Err(std::io::Error::other("head too large"));
        }
    }
    Ok(head)
}

fn header<'a>(head: &'a str, name: &str) -> Option<&'a str> {
    head.lines()
        .skip(1)
        .find_map(|l| {
            l.split_once(':')
                .filter(|(n, _)| n.eq_ignore_ascii_case(name))
        })
        .map(|(_, v)| v.trim())
}

/// Splice a client socket with an upstream socket in both directions.
fn splice(client: TcpStream, upstream: TcpStream) {
    let mut c2 = match client.try_clone() {
        Ok(s) => s,
        Err(_) => return,
    };
    let mut u2 = match upstream.try_clone() {
        Ok(s) => s,
        Err(_) => return,
    };
    let mut client = client;
    let mut upstream = upstream;
    let t = std::thread::spawn(move || {
        let _ = std::io::copy(&mut c2, &mut u2);
        let _ = u2.shutdown(std::net::Shutdown::Both);
    });
    let _ = std::io::copy(&mut upstream, &mut client);
    let _ = client.shutdown(std::net::Shutdown::Both);
    let _ = t.join();
}

// ---------------------------------------------------------------------------
// Authenticating CONNECT proxy (mock-proxy has no auth support).
// ---------------------------------------------------------------------------

struct AuthProxy {
    addr: String,
    auth_hits: Arc<AtomicUsize>,
    rejects: Arc<AtomicUsize>,
}

/// CONNECT-only proxy: demands `Proxy-Authorization: Basic <expected>`;
/// replies 407 otherwise. On success dials the target and splices.
fn start_auth_proxy(user: &str, pass: &str) -> AuthProxy {
    use base64::Engine;
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let expected = format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode(format!("{user}:{pass}"))
    );
    let auth_hits = Arc::new(AtomicUsize::new(0));
    let rejects = Arc::new(AtomicUsize::new(0));
    let (h, r) = (auth_hits.clone(), rejects.clone());
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut s) = stream else { break };
            let (expected, h, r) = (expected.clone(), h.clone(), r.clone());
            std::thread::spawn(move || {
                let Ok(head) = read_head(&mut s, 8 * 1024) else {
                    return;
                };
                let head = String::from_utf8_lossy(&head);
                let mut parts = head.lines().next().unwrap_or_default().split(' ');
                let (method, target) = (parts.next().unwrap_or(""), parts.next().unwrap_or(""));
                let authorized =
                    header(&head, "proxy-authorization").is_some_and(|v| v == expected);
                if !authorized {
                    r.fetch_add(1, Ordering::Relaxed);
                    let _ = s.write_all(
                        b"HTTP/1.1 407 Proxy Authentication Required\r\n\
                          Proxy-Authenticate: Basic realm=\"mock\"\r\n\
                          Content-Length: 0\r\n\r\n",
                    );
                    return;
                }
                if method != "CONNECT" {
                    let _ = s.write_all(b"HTTP/1.1 405 Method Not Allowed\r\n\r\n");
                    return;
                }
                h.fetch_add(1, Ordering::Relaxed);
                let Ok(upstream) = TcpStream::connect(target) else {
                    let _ = s.write_all(b"HTTP/1.1 502 Bad Gateway\r\n\r\n");
                    return;
                };
                let _ = s.write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n");
                splice(s, upstream);
            });
        }
    });
    AuthProxy {
        addr,
        auth_hits,
        rejects,
    }
}

// ---------------------------------------------------------------------------
// Minimal SOCKS5 proxy: offers no-auth + user/pass ("su"/"sp"), CONNECT
// with IPv4/domain ATYP, then splices.
// ---------------------------------------------------------------------------

fn start_socks5_proxy() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut s) = stream else { break };
            std::thread::spawn(move || {
                let mut head = [0u8; 2];
                if s.read_exact(&mut head).is_err() || head[0] != 0x05 {
                    return;
                }
                let mut methods = vec![0u8; head[1] as usize];
                if s.read_exact(&mut methods).is_err() {
                    return;
                }
                if methods.contains(&0x02) {
                    if s.write_all(&[0x05, 0x02]).is_err() {
                        return;
                    }
                    let mut uhead = [0u8; 2];
                    if s.read_exact(&mut uhead).is_err() || uhead[0] != 0x01 {
                        return;
                    }
                    let mut ubuf = vec![0u8; uhead[1] as usize + 1];
                    if s.read_exact(&mut ubuf).is_err() {
                        return;
                    }
                    let (u, plen) = (&ubuf[..ubuf.len() - 1], ubuf[ubuf.len() - 1] as usize);
                    let mut pbuf = vec![0u8; plen];
                    if s.read_exact(&mut pbuf).is_err() {
                        return;
                    }
                    let ok = u == b"su" && pbuf == b"sp";
                    if s.write_all(&[0x01, u8::from(!ok)]).is_err() || !ok {
                        return;
                    }
                } else if methods.contains(&0x00) {
                    if s.write_all(&[0x05, 0x00]).is_err() {
                        return;
                    }
                } else {
                    let _ = s.write_all(&[0x05, 0xFF]);
                    return;
                }
                let mut req = [0u8; 4];
                if s.read_exact(&mut req).is_err() || req[0] != 0x05 || req[1] != 0x01 {
                    return;
                }
                let host = match req[3] {
                    0x01 => {
                        let mut a = [0u8; 4];
                        s.read_exact(&mut a)
                            .map(|_| std::net::Ipv4Addr::from(a).to_string())
                    }
                    0x03 => {
                        let mut l = [0u8; 1];
                        s.read_exact(&mut l).and_then(|_| {
                            let mut d = vec![0u8; l[0] as usize];
                            s.read_exact(&mut d)
                                .map(|_| String::from_utf8_lossy(&d).into_owned())
                        })
                    }
                    _ => return,
                };
                let Ok(host) = host else { return };
                let mut port = [0u8; 2];
                if s.read_exact(&mut port).is_err() {
                    return;
                }
                let Ok(upstream) = TcpStream::connect((host.as_str(), u16::from_be_bytes(port)))
                else {
                    return;
                };
                // Success reply with an IPv4 bound addr.
                if s.write_all(&[0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0])
                    .is_err()
                {
                    return;
                }
                splice(s, upstream);
            });
        }
    });
    addr
}

// ---------------------------------------------------------------------------
// Exit-IP echo server: one request per connection, body = peer IP.
// ---------------------------------------------------------------------------

struct EchoServer {
    addr: String,
    hits: Arc<AtomicUsize>,
}

fn start_echo_server() -> EchoServer {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let hits = Arc::new(AtomicUsize::new(0));
    let h = hits.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut s) = stream else { break };
            let h = h.clone();
            std::thread::spawn(move || {
                let peer = s
                    .peer_addr()
                    .map(|a| a.ip().to_string())
                    .unwrap_or_default();
                if read_head(&mut s, 8 * 1024).is_err() {
                    return;
                }
                h.fetch_add(1, Ordering::Relaxed);
                let body = peer.as_bytes();
                let _ = s.write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n\
                         Content-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    )
                    .as_bytes(),
                );
                let _ = s.write_all(body);
            });
        }
    });
    EchoServer { addr, hits }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn login_and_attach_tunneled_through_connect_proxy() {
    let server = MockServer::start(test_config()).await.unwrap();
    let proxy_addr = server.proxy_addr().unwrap();
    let base = server.backpage_url();

    // Backpage login + launch through the proxy (http:// target → the
    // mock's absolute-form forward path).
    let http_proxy = HttpProxy::new(format!("http://{proxy_addr}"));
    let session = xenomit_backpage::login::login_with_credentials_at_via(
        &base,
        "bot",
        "bot-pass",
        &http_proxy,
    )
    .await
    .expect("proxied backpage login");
    let mut client = xenomit_backpage::BackpageClient::with_proxy(session, &http_proxy).unwrap();
    xenomit_backpage::launch::load(&mut client)
        .await
        .expect("proxied launch");
    let login = login_info(client.session());

    // Game TCP through the proxy (CONNECT tunnel).
    let tcp_proxy = TcpProxy::new(format!("http://{proxy_addr}"));
    let game = server.game_addr();
    let game_for_log = game.clone();
    let session = blocking(move || {
        let mut s =
            Session::connect_via_and_login(&tcp_proxy, &game, &login).expect("tunneled login");
        assert!(s.wait_hero_init(2048).unwrap(), "hero init over tunnel");
        s.complete_ready("2D").unwrap();
        s
    })
    .await;

    // The mock game must have seen exactly one login, arriving through
    // the tunnel: the proxy sniffs the LoginRequest and records a
    // "proxy-login" event — that's the proxy-origin proof (a direct
    // connection would produce no such entry).
    let status = wait_status(&server, |s| s["game_logins"].as_u64() == Some(1)).await;
    assert!(status["proxy_connects"].as_u64().unwrap() >= 1);
    assert!(status["http_requests"].as_u64().unwrap() >= 2);

    let events = control_get(&server, "/events").await;
    let ev = events["events"].as_array().unwrap();
    assert!(
        ev.iter().any(|e| e["kind"].as_str() == Some("proxy-login")
            && e["detail"]
                .as_str()
                .unwrap_or_default()
                .contains("user=1001")),
        "game login was not observed inside the tunnel: {ev:?}"
    );
    // Backpage requests transited the absolute-form forward path.
    let forward_hits = ev
        .iter()
        .filter(|e| e["kind"].as_str() == Some("proxy-forward"))
        .count();
    assert!(forward_hits >= 2, "expected proxied backpage calls: {ev:?}");

    let sessions = control_get(&server, "/sessions").await;
    let arr = sessions["sessions"].as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["user_id"].as_i64(), Some(1001));

    // Close the tunnel — the CONNECT record (target + sniffed login +
    // byte counts) lands in /proxy/log at teardown.
    session.stream.shutdown(std::net::Shutdown::Both).ok();
    drop(session);
    let deadline = Instant::now() + Duration::from_secs(5);
    let tunnel = loop {
        let log = control_get(&server, "/proxy/log").await;
        let conns = log["connections"].as_array().unwrap();
        if let Some(c) = conns
            .iter()
            .find(|c| c["target"].as_str() == Some(&game_for_log))
        {
            break c.clone();
        }
        assert!(
            Instant::now() < deadline,
            "no CONNECT for {game_for_log}: {conns:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let detail = tunnel["detail"].as_str().unwrap_or_default();
    assert!(
        detail.contains("game-login user=1001"),
        "login not sniffed inside tunnel: {tunnel:?}"
    );
    assert!(tunnel["bytes_up"].as_u64().unwrap() > 0);
    assert!(tunnel["bytes_down"].as_u64().unwrap() > 0);
    server.shutdown().await;
}

#[tokio::test]
async fn proxy_auth_failure_is_typed() {
    let server = MockServer::start(test_config()).await.unwrap();
    let proxy = start_auth_proxy("bot-user", "s3cret");
    let game = server.game_addr();

    // No credentials → HTTP 407 → typed Auth error.
    let no_auth = TcpProxy::new(format!("http://{}", proxy.addr));
    let g = game.clone();
    let err = blocking(move || Session::connect_via(&no_auth, &g).map(|_| ()))
        .await
        .unwrap_err();
    assert!(
        matches!(err, ClientError::Proxy(ProxyError::Auth(_))),
        "expected Auth, got {err:?}"
    );
    assert_eq!(proxy.rejects.load(Ordering::Relaxed), 1);

    // Wrong credentials → also Auth.
    let wrong = TcpProxy::with_auth(format!("http://{}", proxy.addr), "bot-user", "bad");
    let g = game.clone();
    let err = blocking(move || Session::connect_via(&wrong, &g).map(|_| ()))
        .await
        .unwrap_err();
    assert!(
        matches!(err, ClientError::Proxy(ProxyError::Auth(_))),
        "expected Auth, got {err:?}"
    );

    // Backpage through the same proxy with no credentials → the proxy
    // answers the forward-mode request 407, which reqwest returns as a
    // normal response; the client promotes it to the proxy class.
    let http_proxy = HttpProxy::new(format!("http://{}", proxy.addr));
    let err = xenomit_backpage::login::login_with_credentials_at_via(
        &server.backpage_url(),
        "bot",
        "bot-pass",
        &http_proxy,
    )
    .await
    .unwrap_err();
    assert!(
        err.is_proxy_failure(),
        "expected proxy-class error for HTTP 407, got {err:?}"
    );

    // Right credentials → tunnel established; the Proxy-Authorization
    // header the client sent was accepted (login against real game).
    let session = backpage_session(&server).await;
    let login = login_info(&session);
    let good = TcpProxy::with_auth(format!("http://{}", proxy.addr), "bot-user", "s3cret");
    blocking(move || {
        let mut s = Session::connect_via_and_login(&good, &game, &login)
            .expect("tunneled login with proxy auth");
        assert!(s.wait_hero_init(2048).unwrap());
    })
    .await;
    assert_eq!(proxy.auth_hits.load(Ordering::Relaxed), 1);
    server.shutdown().await;
}

/// Plain (unauthenticated) backpage session for game login info.
async fn backpage_session(server: &MockServer) -> xenomit_backpage::Session {
    let base = server.backpage_url();
    let session = xenomit_backpage::login::login_with_credentials_at(&base, "bot", "bot-pass")
        .await
        .expect("backpage login");
    let mut client = xenomit_backpage::BackpageClient::new(session).unwrap();
    xenomit_backpage::launch::load(&mut client).await.unwrap();
    client.session().clone()
}

#[tokio::test]
async fn proxy_down_fails_with_typed_error() {
    // A bound-then-dropped listener gives us a port that refuses.
    let dead = {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().to_string()
    };
    let server = MockServer::start(test_config()).await.unwrap();

    // Packet client → ClientError::Proxy(Connect), exit-code-69 class.
    let dead_proxy = TcpProxy::new(format!("http://{dead}"));
    let err = blocking(move || Session::connect_via(&dead_proxy, "127.0.0.1:5001").map(|_| ()))
        .await
        .unwrap_err();
    match err {
        ClientError::Proxy(e @ ProxyError::Connect { .. }) => {
            assert_eq!(e.exit_code(), 69);
        }
        other => panic!("expected Proxy(Connect), got {other:?}"),
    }

    // Backpage → Error::Proxy (is_proxy_failure).
    let http_proxy = HttpProxy::new(format!("http://{dead}"));
    let err = xenomit_backpage::login::login_with_credentials_at_via(
        &server.backpage_url(),
        "bot",
        "bot-pass",
        &http_proxy,
    )
    .await
    .unwrap_err();
    assert!(
        err.is_proxy_failure(),
        "expected proxy-class error, got {err:?}"
    );
    server.shutdown().await;
}

#[tokio::test]
async fn denied_proxy_never_falls_back_to_direct() {
    let mut cfg = test_config();
    cfg.faults.proxy.deny = true; // CONNECT → 403, forward refused
    let server = MockServer::start(cfg).await.unwrap();
    let proxy_addr = server.proxy_addr().unwrap();

    // Packet attach: tunnel refused → typed Refused, fail closed.
    let tcp_proxy = TcpProxy::new(format!("http://{proxy_addr}"));
    let game = server.game_addr();
    let err = blocking(move || Session::connect_via(&tcp_proxy, &game).map(|_| ()))
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            ClientError::Proxy(ProxyError::Refused { .. } | ProxyError::Auth(_))
        ),
        "expected Refused/Auth, got {err:?}"
    );

    // Backpage login via the denied proxy fails too (the mock answers
    // forwards with 403) — the exact variant doesn't matter; what
    // matters is below: nothing reached the mock directly.
    let http_proxy = HttpProxy::new(format!("http://{proxy_addr}"));
    let res = xenomit_backpage::login::login_with_credentials_at_via(
        &server.backpage_url(),
        "bot",
        "bot-pass",
        &http_proxy,
    )
    .await;
    assert!(res.is_err(), "login must fail through a denied proxy");

    // Fail-closed proof: the mock game saw zero connections/logins and
    // the backpage saw zero requests — there is no direct fallback.
    let status = control_get(&server, "/status").await;
    assert_eq!(status["game_logins"].as_u64(), Some(0));
    assert_eq!(status["http_requests"].as_u64(), Some(0));
    let sessions = control_get(&server, "/sessions").await;
    assert_eq!(sessions["sessions"].as_array().unwrap().len(), 0);
    server.shutdown().await;
}

#[tokio::test]
async fn socks5_and_socks5h_tunnel_game_login() {
    let server = MockServer::start(test_config()).await.unwrap();
    let socks_addr = start_socks5_proxy();
    let game = server.game_addr();

    for (i, scheme) in ["socks5", "socks5h"].into_iter().enumerate() {
        let session = backpage_session(&server).await;
        let login = login_info(&session);
        let proxy = TcpProxy::with_auth(format!("{scheme}://{socks_addr}"), "su", "sp");
        let g = game.clone();
        let s = blocking(move || {
            let mut s = Session::connect_via_and_login(&proxy, &g, &login)
                .unwrap_or_else(|e| panic!("{scheme} attach: {e:?}"));
            assert!(s.wait_hero_init(2048).unwrap());
            s
        })
        .await;
        // Let the server finish the init burst and register the login
        // before the socket goes away — an early close cancels it.
        let want = i as u64 + 1;
        wait_status(&server, move |s| s["game_logins"].as_u64() == Some(want)).await;
        drop(s);
    }
    let status = control_get(&server, "/status").await;
    assert_eq!(status["game_logins"].as_u64(), Some(2));
    server.shutdown().await;
}

#[tokio::test]
async fn exit_ip_discovered_through_proxy() {
    let server = MockServer::start(test_config()).await.unwrap();
    let echo = start_echo_server();
    let proxy_addr = server.proxy_addr().unwrap();
    let http_proxy = HttpProxy::new(format!("http://{proxy_addr}"));

    // Standalone helper with a pluggable echo URL.
    let echo_url = format!("http://{}/", echo.addr);
    let ip = xenomit_backpage::proxy::fetch_exit_ip(&http_proxy, Some(&echo_url))
        .await
        .expect("exit ip");
    assert_eq!(ip, "127.0.0.1");
    assert_eq!(echo.hits.load(Ordering::Relaxed), 1);

    // Same through an already-proxied session client.
    let session = xenomit_backpage::login::login_with_credentials_at_via(
        &server.backpage_url(),
        "bot",
        "bot-pass",
        &http_proxy,
    )
    .await
    .unwrap();
    let client = xenomit_backpage::BackpageClient::with_proxy(session, &http_proxy).unwrap();
    assert_eq!(client.exit_ip(Some(&echo_url)).await.unwrap(), "127.0.0.1");

    // Without a proxy the helper refuses — a direct client's "exit IP"
    // would be the host's own.
    let direct = xenomit_backpage::BackpageClient::new(client.session().clone()).unwrap();
    assert!(matches!(
        direct.exit_ip(Some(&echo_url)).await,
        Err(xenomit_backpage::Error::InvalidProxy(_))
    ));

    // And the forwards actually transited the mock proxy.
    let events = control_get(&server, "/events").await;
    assert!(
        events["events"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["kind"].as_str() == Some("proxy-forward")
                && e["detail"]
                    .as_str()
                    .unwrap_or_default()
                    .contains(&echo.addr)),
        "echo request never went through the proxy: {events}"
    );
    server.shutdown().await;
}
