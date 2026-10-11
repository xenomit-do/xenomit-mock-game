//! End-to-end tests: real `xenomit_backpage` login + real
//! `xenomit_packet_client` handshake/world round trip against the mock.

use std::time::Duration;
use xenomit_mock_game::{Config, MockServer};
use xenomit_packet_client::modules::VERSION_HASH;
use xenomit_packet_client::session::{Event, LoginInfo, Session};

fn test_config() -> Config {
    let mut cfg = Config::default();
    cfg.game.tick_ms = 50;
    cfg.game.npc_count = 4;
    cfg.game.player_count = 1;
    cfg.backpage.session_ttl_secs = 60;
    cfg
}

/// Real backpage login → session, then `launch::load` for flashvars.
async fn backpage_login(base: &str) -> xenomit_backpage::Session {
    let session = xenomit_backpage::login::login_with_credentials_at(base, "bot", "bot-pass")
        .await
        .expect("password login");
    assert!(!session.sid.is_empty());
    let mut client = xenomit_backpage::BackpageClient::new(session).expect("client");
    xenomit_backpage::launch::load(&mut client)
        .await
        .expect("launch load");
    client.session().clone()
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

/// Blocking packet attach in a worker — returns once hero init arrived.
async fn attach(
    game_addr: String,
    login: LoginInfo,
) -> std::io::Result<Session<std::net::TcpStream>> {
    tokio::task::spawn_blocking(move || {
        let mut s = Session::connect_and_login(&game_addr, &login)
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        let ok = s
            .wait_hero_init(2048)
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        assert!(ok, "hero never initialized");
        s.complete_ready("2D")
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        Ok(s)
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn backpage_login_and_packet_attach_round_trip() {
    let server = MockServer::start(test_config()).await.unwrap();
    let base = server.backpage_url();
    let session = backpage_login(&base).await;

    // The seam: the session must be pinned at the mock origin.
    assert_eq!(session.instance(), format!("{base}/"));
    assert_eq!(session.user_id, 1001);
    let login = login_info(&session);

    // maps.php resolution — what `connect_with(None)` would do.
    let http = reqwest::Client::new();
    let maps = http
        .get(format!("{base}/spacemap/xml/maps.php"))
        .header("Cookie", format!("dosid={}", session.sid))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(maps.contains(&server.game_addr()), "maps.php: {maps}");

    let mut s = attach(server.game_addr(), login).await.unwrap();
    tokio::task::spawn_blocking(move || {
        // Pump ~1.5s of world traffic.
        let mut saw_move = false;
        for _ in 0..30 {
            for ev in s.pump().unwrap() {
                if let Event::Packet(p) = &ev
                    && p.id == xenomit_packet_client::modules::id::MOVE_COMMAND
                {
                    saw_move = true;
                }
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        let w = &s.world;
        assert!(w.logged_in);
        let hero = w.hero.as_ref().expect("hero state");
        assert_eq!(hero.user_id, 1001);
        assert_eq!(hero.map_id, 16);
        assert!(!w.entities.is_empty(), "expected NPC/player entities");
        assert!(!w.collectables.is_empty(), "expected collectables");
        assert!(!w.portals.is_empty(), "expected portals");
        assert!(saw_move, "expected NPC MoveCommand traffic");
        assert_eq!(w.configuration, Some(1));
        assert!(w.pet.observed && w.pet.owned);
        assert!(!w.ores.is_empty());
        assert!(w.gate_builder.is_some());
        assert!(w.trade.is_some());
        // The menu item needed for UseItem must be observed.
        assert!(w.menu_items.contains_key("ammunition_laser_ucb-100"));
    })
    .await
    .unwrap();
    server.shutdown().await;
}

#[tokio::test]
async fn move_and_attack_actions_are_acknowledged() {
    let server = MockServer::start(test_config()).await.unwrap();
    let session = backpage_login(&server.backpage_url()).await;
    let mut s = attach(server.game_addr(), login_info(&session))
        .await
        .unwrap();
    tokio::task::spawn_blocking(move || {
        // move_to: send a MoveRequest, expect HeroMove traffic.
        let (hx, hy) = s.world.hero_pos().unwrap();
        s.send(&xenomit_packet_client::modules::move_request(
            (hx, hy),
            (hx + 2000, hy + 1000),
        ))
        .unwrap();
        let mut saw_hero_move = false;
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        while std::time::Instant::now() < deadline && !saw_hero_move {
            if let Some(Event::Packet(p)) = s.poll(Duration::from_millis(200)).unwrap()
                && p.id == xenomit_packet_client::modules::id::HERO_MOVE
            {
                saw_hero_move = true;
            }
        }
        assert!(saw_hero_move);
        // attack: pick an NPC entity, expect laser-run + hit traffic.
        let npc = s
            .world
            .entities
            .values()
            .find(|e| e.npc)
            .map(|e| (e.id, e.x, e.y))
            .expect("an npc");
        s.send(&xenomit_packet_client::modules::attack_laser_request(
            npc.0,
            (npc.1, npc.2),
        ))
        .unwrap();
        let mut saw_run = false;
        let mut saw_hit = false;
        let deadline = std::time::Instant::now() + Duration::from_secs(4);
        while std::time::Instant::now() < deadline && !(saw_run && saw_hit) {
            if let Some(Event::Packet(p)) = s.poll(Duration::from_millis(200)).unwrap() {
                match p.id {
                    xenomit_packet_client::modules::id::ATTACK_LASER_RUN => saw_run = true,
                    xenomit_packet_client::modules::id::ATTACK_HIT => saw_hit = true,
                    _ => {}
                }
            }
        }
        assert!(saw_run && saw_hit, "laser run={saw_run} hit={saw_hit}");
        // stop_attack → AttackAbortLaserCommand
        s.send(&xenomit_packet_client::modules::attack_abort_laser_request())
            .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        let mut saw_abort = false;
        while std::time::Instant::now() < deadline && !saw_abort {
            if let Some(Event::Packet(p)) = s.poll(Duration::from_millis(200)).unwrap()
                && p.id == xenomit_packet_client::modules::id::ATTACK_ABORT_LASER
            {
                saw_abort = true;
            }
        }
        assert!(saw_abort);
    })
    .await
    .unwrap();
    server.shutdown().await;
}

#[tokio::test]
async fn collect_box_grants_credits() {
    let server = MockServer::start(test_config()).await.unwrap();
    let session = backpage_login(&server.backpage_url()).await;
    let mut s = attach(server.game_addr(), login_info(&session))
        .await
        .unwrap();
    tokio::task::spawn_blocking(move || {
        s.pump().unwrap();
        let hash = s
            .world
            .collectables
            .values()
            .find(|c| c.type_str == "BONUS_BOX")
            .map(|c| c.hash.clone())
            .expect("a bonus box");
        let credits_before = s.world.hero.as_ref().unwrap().credits;
        let pos = s.world.hero_pos().unwrap();
        let cpos = {
            let c = &s.world.collectables[&hash];
            (c.x, c.y)
        };
        s.send(&xenomit_packet_client::modules::collect_box_request(
            &hash, cpos, pos,
        ))
        .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while std::time::Instant::now() < deadline {
            let _ = s.poll(Duration::from_millis(200)).unwrap();
            if s.world.hero.as_ref().unwrap().credits > credits_before {
                break;
            }
        }
        assert!(s.world.hero.as_ref().unwrap().credits > credits_before);
        assert!(!s.world.collectables.contains_key(&hash));
    })
    .await
    .unwrap();
    server.shutdown().await;
}

#[tokio::test]
async fn second_login_kicks_first() {
    let server = MockServer::start(test_config()).await.unwrap();
    let base = server.backpage_url();
    let session_a = backpage_login(&base).await;
    let session_b = backpage_login(&base).await; // same account → second sid
    assert_ne!(session_a.sid, session_b.sid);
    let game = server.game_addr();
    let mut a = attach(game.clone(), login_info(&session_a)).await.unwrap();
    let _b = attach(game, login_info(&session_b)).await.unwrap();
    // A's socket must be closed by the server (one session per account).
    tokio::task::spawn_blocking(move || {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            match a.poll(Duration::from_millis(500)) {
                Err(_) => break, // connection closed — kicked
                Ok(_) => {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "first session was never kicked"
                    );
                }
            }
        }
    })
    .await
    .unwrap();
    server.shutdown().await;
}

#[tokio::test]
async fn bad_credentials_and_faults() {
    let server = MockServer::start(test_config()).await.unwrap();
    let base = server.backpage_url();
    // Wrong password → no dosid → WrongCredentials.
    let err = xenomit_backpage::login::login_with_credentials_at(&base, "bot", "nope").await;
    assert!(matches!(
        err,
        Err(xenomit_backpage::Error::WrongCredentials)
    ));

    // CAPTCHA fault via control API.
    let http = reqwest::Client::new();
    http.post(format!("{}/faults", server.control_url().unwrap()))
        .json(&serde_json::json!({"backpage": {"captcha": true}}))
        .send()
        .await
        .unwrap();
    let err = xenomit_backpage::login::login_with_credentials_at(&base, "bot", "bot-pass").await;
    assert!(matches!(err, Err(xenomit_backpage::Error::CaptchaRequired)));
    http.post(format!("{}/faults", server.control_url().unwrap()))
        .json(&serde_json::json!({"backpage": {"captcha": false, "reject_credentials": true}}))
        .send()
        .await
        .unwrap();
    let err = xenomit_backpage::login::login_with_credentials_at(&base, "bot", "bot-pass").await;
    assert!(matches!(
        err,
        Err(xenomit_backpage::Error::WrongCredentials)
    ));
    server.shutdown().await;
}

#[tokio::test]
async fn version_mismatch_and_drop_faults() {
    // Version mismatch → handshake fails at VersionCommand.
    let mut cfg = test_config();
    cfg.faults.game.version_mismatch = true;
    let server = MockServer::start(cfg).await.unwrap();
    let session = backpage_login(&server.backpage_url()).await;
    let game = server.game_addr();
    let login = login_info(&session);
    let err =
        tokio::task::spawn_blocking(move || Session::connect_and_login(&game, &login).map(|_| ()))
            .await
            .unwrap();
    assert!(err.is_err());
    server.shutdown().await;

    // drop_after_ms → socket dies shortly after login.
    let mut cfg = test_config();
    cfg.faults.game.drop_after_ms = Some(300);
    let server = MockServer::start(cfg).await.unwrap();
    let session = backpage_login(&server.backpage_url()).await;
    let mut s = attach(server.game_addr(), login_info(&session))
        .await
        .unwrap();
    tokio::task::spawn_blocking(move || {
        let deadline = std::time::Instant::now() + Duration::from_secs(4);
        loop {
            match s.poll(Duration::from_millis(300)) {
                Err(_) => break,
                Ok(_) => assert!(
                    std::time::Instant::now() < deadline,
                    "drop_after_ms never fired"
                ),
            }
        }
    })
    .await
    .unwrap();
    server.shutdown().await;
}

#[tokio::test]
async fn login_reject_status_fault() {
    let mut cfg = test_config();
    cfg.faults.game.login_reject_status = Some(9);
    let server = MockServer::start(cfg).await.unwrap();
    let session = backpage_login(&server.backpage_url()).await;
    let game = server.game_addr();
    let login = login_info(&session);
    let err =
        tokio::task::spawn_blocking(move || match Session::connect_and_login(&game, &login) {
            Err(xenomit_packet_client::ClientError::LoginRejected(code)) => Some(code),
            Err(e) => panic!("expected LoginRejected, got {e:?}"),
            Ok(_) => panic!("expected LoginRejected, got a live session"),
        })
        .await
        .unwrap();
    assert_eq!(err, Some(9));
    server.shutdown().await;
}

#[tokio::test]
async fn session_ttl_expires() {
    let mut cfg = test_config();
    // Launch needs ~2s for rate-limited calls; 4s outlives it but
    // still expires inside the test window.
    cfg.backpage.session_ttl_secs = 4;
    let server = MockServer::start(cfg).await.unwrap();
    let session = backpage_login(&server.backpage_url()).await;
    // Fresh sid validates.
    let client = xenomit_backpage::BackpageClient::new(session.clone()).unwrap();
    assert_eq!(client.check_sid().await, xenomit_backpage::SidStatus::Valid);
    drop(client);
    tokio::time::sleep(Duration::from_millis(4600)).await;
    let client = xenomit_backpage::BackpageClient::new(session).unwrap();
    assert_eq!(
        client.check_sid().await,
        xenomit_backpage::SidStatus::Invalid
    );
    server.shutdown().await;
}

#[tokio::test]
async fn direct_sid_login_via_control_api() {
    let server = MockServer::start(test_config()).await.unwrap();
    let base = server.backpage_url();
    let http = reqwest::Client::new();
    // Mint a sid without the password flow.
    let resp: serde_json::Value = http
        .post(format!("{}/sids", server.control_url().unwrap()))
        .json(&serde_json::json!({"user_id": 1001}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let sid = resp["sid"].as_str().unwrap().to_string();
    let session = xenomit_backpage::login::login_with_sid_at(&base, &sid)
        .await
        .expect("sid login");
    assert_eq!(session.user_id, 1001);
    let s = attach(server.game_addr(), login_info(&session))
        .await
        .unwrap();
    tokio::task::spawn_blocking(move || {
        assert!(s.world.hero.is_some());
    })
    .await
    .unwrap();
    server.shutdown().await;
}

#[tokio::test]
async fn game_login_requires_known_sid() {
    let server = MockServer::start(test_config()).await.unwrap();
    // strict_auth is on by default: an invented sid is rejected.
    let mut login = LoginInfo::new(1001, "forged-sid");
    login.version = VERSION_HASH.into();
    let game = server.game_addr();
    let err =
        tokio::task::spawn_blocking(move || match Session::connect_and_login(&game, &login) {
            Err(xenomit_packet_client::ClientError::LoginRejected(code)) => Some(code),
            Err(e) => panic!("expected LoginRejected, got {e:?}"),
            Ok(_) => panic!("expected LoginRejected, got a live session"),
        })
        .await
        .unwrap();
    assert_eq!(
        err,
        Some(xenomit_mock_game::game::STATUS_BAD_SESSION as i16)
    );
    server.shutdown().await;
}

#[tokio::test]
async fn proxy_connect_tunnels_game_traffic() {
    let server = MockServer::start(test_config()).await.unwrap();
    let proxy = server.proxy_addr().unwrap();
    let session = backpage_login(&server.backpage_url()).await;
    let login = login_info(&session);
    let game = server.game_addr();
    let tunnel = game.clone();
    tokio::task::spawn_blocking(move || {
        use std::io::{Read, Write};
        let mut stream = std::net::TcpStream::connect(&proxy).unwrap();
        stream
            .write_all(format!("CONNECT {tunnel} HTTP/1.1\r\nHost: {tunnel}\r\n\r\n").as_bytes())
            .unwrap();
        // Read the proxy's 200 line.
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut head = Vec::new();
        let mut byte = [0u8; 1];
        while !head.ends_with(b"\r\n\r\n") {
            stream.read_exact(&mut byte).unwrap();
            head.push(byte[0]);
        }
        assert!(String::from_utf8_lossy(&head).starts_with("HTTP/1.1 200"));
        // Drive the real handshake through the tunnel.
        let mut s = Session::new(stream);
        s.handshake(&login).unwrap();
        assert!(s.wait_hero_init(2048).unwrap());
    })
    .await
    .unwrap();
    // The proxy log must show the tunnel and the sniffed login.
    let http = reqwest::Client::new();
    let log: serde_json::Value = http
        .get(format!("{}/proxy/log", server.control_url().unwrap()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let conns = log["connections"].as_array().unwrap();
    assert!(conns.iter().any(|c| c["target"].as_str() == Some(&game)));
    server.shutdown().await;
}

#[tokio::test]
async fn proxy_forward_get() {
    // Plain absolute-form GET forwarding → the backpage front page.
    let server = MockServer::start(test_config()).await.unwrap();
    let proxy = server.proxy_addr().unwrap();
    let base = server.backpage_url();
    tokio::task::spawn_blocking(move || {
        use std::io::{Read, Write};
        let mut stream = std::net::TcpStream::connect(&proxy).unwrap();
        stream
            .write_all(format!("GET {base}/ HTTP/1.1\r\n\r\n").as_bytes())
            .unwrap();
        let mut body = String::new();
        stream.read_to_string(&mut body).unwrap();
        assert!(
            body.contains("bgcdw_login_form"),
            "frontpage via proxy: {body}"
        );
    })
    .await
    .unwrap();
    server.shutdown().await;
}

#[tokio::test]
async fn hangar_and_user_ops() {
    let server = MockServer::start(test_config()).await.unwrap();
    let session = backpage_login(&server.backpage_url()).await;
    let client = xenomit_backpage::BackpageClient::new(session).unwrap();
    let hangars = client.hangar_list().await.expect("hangar list");
    let ret = hangars
        .data
        .as_ref()
        .and_then(|d| d.ret.as_ref())
        .expect("ret");
    assert!(ret.hangars.iter().any(|h| h.active));
    let drones = xenomit_backpage::ops::hangar::drones_from_hangar(&hangars);
    assert_eq!(drones.len(), 2);
    // base62 user resolution both ways.
    let user = client.user_by_id(1001).await.unwrap();
    assert_eq!(user.username.as_deref(), Some("bot"));
    let back = client.user_by_name("bot").await.unwrap();
    assert_eq!(back.user_id, 1001);
    server.shutdown().await;
}

#[tokio::test]
async fn kill_hero_opens_kill_screen_and_repair_respawns() {
    let server = MockServer::start(test_config()).await.unwrap();
    let session = backpage_login(&server.backpage_url()).await;
    let login = login_info(&session);
    let mut s = attach(server.game_addr(), login.clone()).await.unwrap();
    let http = reqwest::Client::new();
    http.post(format!(
        "{}/sessions/1001/kill",
        server.control_url().unwrap()
    ))
    .send()
    .await
    .unwrap();
    tokio::task::spawn_blocking(move || {
        // Expect ShipDestroyed + KillScreenPost with a free repair option.
        // `kill_screen` flips on SHIP_DESTROYED; keep polling until the
        // post command has populated the options too.
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        while std::time::Instant::now() < deadline
            && s.world
                .kill_screen
                .as_ref()
                .is_none_or(|k| k.options.is_empty())
        {
            let _ = s.poll(Duration::from_millis(200)).unwrap();
        }
        let ks = s.world.kill_screen.as_ref().expect("kill screen");
        assert!(ks.options.iter().any(|o| o.repair_type == 1 && o.cost == 0));
        // Revive through the repair request.
        s.send(&xenomit_packet_client::modules::repair_request(&login, 1))
            .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        loop {
            let _ = s.poll(Duration::from_millis(200)).unwrap();
            if s.world.kill_screen.is_none() {
                break; // MAP_CHANGED clears it — hero is back
            }
            assert!(std::time::Instant::now() < deadline, "never respawned");
        }
    })
    .await
    .unwrap();
    server.shutdown().await;
}

#[tokio::test]
async fn control_api_reports_sessions() {
    let server = MockServer::start(test_config()).await.unwrap();
    let session = backpage_login(&server.backpage_url()).await;
    let _s = attach(server.game_addr(), login_info(&session))
        .await
        .unwrap();
    let http = reqwest::Client::new();
    let control = server.control_url().unwrap();
    let status: serde_json::Value = http
        .get(format!("{control}/status"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status["game_logins"].as_u64(), Some(1));
    assert_eq!(status["live_sessions"].as_u64(), Some(1));
    let sessions: serde_json::Value = http
        .get(format!("{control}/sessions"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let arr = sessions["sessions"].as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["user_id"].as_i64(), Some(1001));
    // Kick it through the API; the session row must disappear.
    http.post(format!("{control}/sessions/1001/kick"))
        .send()
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let status: serde_json::Value = http
        .get(format!("{control}/status"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status["live_sessions"].as_u64(), Some(0));
    server.shutdown().await;
}
