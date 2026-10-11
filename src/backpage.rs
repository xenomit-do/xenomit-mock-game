//! Mock Backpage: the HTTP endpoints `xenomit-backpage` exercises.
//!
//! Covered flows:
//! * `login_with_credentials[_at]` — frontpage scrape (`bgcdw_login_form`
//!   action URL), `username`/`password` POST, `dosid` cookie + redirect
//!   chain.
//! * `login_with_sid[_at]` / `launch::load` — `internalMapRevolution` map
//!   page with a `flashembed` contract, then `flashAPI/loadingScreen.php`
//!   returning the authenticated flashvars.
//! * `check_sid` — `indexInternal.es` probe actions + `ajax/shop.php`.
//! * `hangar_list` etc. — `flashAPI/inventory.php` base64 envelope.
//! * `maps.php` — points at the mock game server's bound address.
//! * `ajax/user.php`/`pilotprofil.php` — user resolution.

use crate::config::Account;
use crate::http::{Request, Response, percent_decode, percent_encode};
use crate::state::{Shared, SidInfo};
use base64::prelude::*;
use serde_json::{Value, json};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;

const ALPHABET: &[u8; 62] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

fn base62_encode(mut i: i64) -> String {
    if i == 0 {
        return "a".into();
    }
    let mut out = Vec::new();
    while i > 0 {
        out.push(ALPHABET[(i % 62) as usize]);
        i /= 62;
    }
    out.reverse();
    String::from_utf8(out).unwrap()
}

fn base62_decode(s: &str) -> i64 {
    let mut n = 0i64;
    for c in s.chars() {
        let d = ALPHABET.iter().position(|&a| a == c as u8).unwrap_or(0) as i64;
        n = n * 62 + d;
    }
    n
}

/// Start the mock Backpage on an already-bound listener.
pub async fn serve(
    listener: TcpListener,
    shared: Arc<Shared>,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> std::io::Result<()> {
    crate::http::serve(listener, "backpage", shutdown, move |req| {
        let shared = shared.clone();
        async move { handle(req, &shared).await }
    })
    .await
}

async fn handle(req: Request, shared: &Arc<Shared>) -> Response {
    shared
        .http_requests
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let faults = shared.faults().backpage;
    if faults.latency_ms > 0 {
        tokio::time::sleep(Duration::from_millis(faults.latency_ms)).await;
    }
    if faults.offline {
        return Response::new(503, "text/plain", b"mock backpage offline".to_vec());
    }
    let origin = req.origin();
    match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/") => front_page(&origin, faults.captcha),
        ("POST", "/bgcdw_login") => login_post(&req, shared, &origin, &faults),
        ("GET", "/indexInternal.es") => internal_page(&req, shared, &origin),
        ("POST", "/flashAPI/loadingScreen.php") => loading_screen(&req, shared, &origin),
        ("GET", "/flashAPI/loadingScreen.php") => loading_screen(&req, shared, &origin),
        (_, "/flashAPI/inventory.php") => inventory(&req, shared),
        ("GET", "/spacemap/xml/maps.php") => maps_php(shared),
        ("GET", "/spacemap/preloader.swf") => fake_swf(),
        ("POST", "/ajax/user.php") => ajax_user(&req, shared),
        ("POST", "/ajax/pilotprofil.php") => ajax_pilotprofil(&req, shared),
        (_, path) if path.starts_with("ajax/") => ajax_stub(&req, shared),
        _ => Response::not_found(),
    }
}

// ------------------------------------------------------------------
// login flow

fn front_page(origin: &str, captcha: bool) -> Response {
    let captcha_block = if captcha {
        r#"<div class="bgcdw_captcha">captcha required</div>"#
    } else {
        ""
    };
    // `login_form_re` is `"bgcdw_login_form" action="(.*)"` — greedy and
    // line-bound, so `action=` must directly follow the id and be the
    // last quoted attribute on its line.
    Response::html(format!(
        "<html><head><title>DarkOrbit</title></head><body>\n\
         {captcha_block}\n\
         <form id=\"bgcdw_login_form\" action=\"{origin}/bgcdw_login\"\n\
         method=\"post\">\n\
         <input type=\"text\" name=\"username\"/><input type=\"password\" name=\"password\"/>\n\
         <input type=\"submit\" value=\"Login\"/>\n\
         </form></body></html>"
    ))
}

fn login_post(
    req: &Request,
    shared: &Arc<Shared>,
    origin: &str,
    faults: &crate::state::BackpageFaults,
) -> Response {
    if faults.rate_limit {
        return Response::new(429, "text/plain", b"rate limited".to_vec());
    }
    let form = req.form();
    let username = form.get("username").map(String::as_str).unwrap_or("");
    let password = form.get("password").map(String::as_str).unwrap_or("");
    let account = shared
        .cfg
        .backpage
        .accounts
        .iter()
        .find(|a| a.username == username);
    let ok = !faults.reject_credentials && account.is_some_and(|a| a.password == password);
    shared.event(
        "login",
        format!("user={username} ok={ok} peer={}", req.peer),
    );
    if !ok {
        // Real backpage bounces to the front page without a dosid.
        return Response::redirect(&format!("{origin}/"));
    }
    let account = account.unwrap();
    let sid = shared.issue_sid(account);
    Response::redirect(&format!("{origin}/indexInternal.es?action=internalStart"))
        .header("Set-Cookie", &format!("dosid={sid}; Path=/"))
}

/// `indexInternal.es?action=…` — internal pages all require a live sid;
/// the map page additionally carries the `flashembed` launch contract.
fn internal_page(req: &Request, shared: &Arc<Shared>, origin: &str) -> Response {
    let Some(sid_info) = authenticated(req, shared) else {
        return sid_invalid(origin);
    };
    let action = req.query.get("action").map(String::as_str).unwrap_or("");
    match action {
        "internalMapRevolution" => map_page(shared, origin, &sid_info),
        _ => Response::html(format!(
            "<html><body>mock {action} window reloadToken=mock-token-1</body></html>"
        )),
    }
}

fn map_page(shared: &Arc<Shared>, origin: &str, sid: &SidInfo) -> Response {
    let cfg = &shared.cfg.game;
    let account = shared
        .cfg
        .backpage
        .accounts
        .iter()
        .find(|a| a.user_id == sid.user_id);
    let (faction, instance) = account
        .map(|a| (a.faction_id, a.instance_id))
        .unwrap_or((1, 9));
    // The real page embeds the authenticated vars too; loadingScreen.php
    // repeats them — both merge in the client's flashvar table.
    let flashvars = json!({
        "cdn": format!("{origin}/"),
        "mapID": cfg.map_id.to_string(),
        "userID": sid.user_id.to_string(),
        "factionID": faction.to_string(),
        "pid": instance.to_string(),
        "platform": "bigpoint",
        "display2d": "2",
        "lang": "en",
        "gameclientHash": cfg.version_hash,
        "dynamicHost": origin,
        "basePath": "spacemap/",
    });
    // Mirrors the map page's inline script: flashembed(container,
    // {src,width,height,...}, {flashvars}). Authenticated vars arrive
    // separately from flashAPI/loadingScreen.php, like the real page.
    Response::html(format!(
        "<html><body><div id=\"gameClient\"></div>\n\
         <script>\n\
         flashembed(\"gameClient\", \
         {{\"src\":\"{origin}/spacemap/preloader.swf\",\"width\":\"100%\",\"height\":\"100%\",\"version\":[11,0],\"onFail\":onFailFlashembed}}, \
         {flashvars});\n\
         jQuery.post(\"flashAPI/loadingScreen.php\", {{action: \"setClientBrowserConfig\"}});\n\
         </script></body></html>"
    ))
}

/// `flashAPI/loadingScreen.php` — returns the authenticated flashvar
/// contract as a bare JSON object (`sessionID` key required).
fn loading_screen(req: &Request, shared: &Arc<Shared>, origin: &str) -> Response {
    let Some(sid_info) = authenticated(req, shared) else {
        return sid_invalid(origin);
    };
    let cfg = &shared.cfg.game;
    let account = shared
        .cfg
        .backpage
        .accounts
        .iter()
        .find(|a| a.user_id == sid_info.user_id);
    let (faction, instance) = account
        .map(|a| (a.faction_id, a.instance_id))
        .unwrap_or((1, 9));
    Response::json(&json!({
        "userID": sid_info.user_id.to_string(),
        "sessionID": req.cookie("dosid").unwrap_or_default(),
        "factionID": faction.to_string(),
        "pid": instance.to_string(),
        "mapID": cfg.map_id.to_string(),
        "platform": "bigpoint",
        "lang": "en",
        "cdn": format!("{origin}/"),
        "dynamicHost": origin,
        "host": origin,
        "basePath": "spacemap/",
        "gameclientHash": cfg.version_hash,
        "display2d": "2",
        "aid": "1",
        "eventContext": "",
    }))
}

/// `spacemap/xml/maps.php` — `<map id><gameserverIP>host:port` table.
/// Every map points at the mock game server so `map_endpoint()` works
/// whichever map the session lands on.
fn maps_php(shared: &Arc<Shared>) -> Response {
    let game = shared
        .addrs
        .lock()
        .unwrap()
        .game
        .map(|a| a.to_string())
        .unwrap_or_else(|| "127.0.0.1:5001".into());
    let mut maps = String::from("<maps>");
    // Cover the configured map plus neighbours the jump sim can move to.
    let base = shared.cfg.game.map_id;
    for id in [1, base - 1, base, base + 1, base + 2] {
        if id > 0 {
            maps.push_str(&format!(
                "<map id=\"{id}\" name=\"mock-{id}\"><gameserverIP>{game}</gameserverIP></map>"
            ));
        }
    }
    maps.push_str("</maps>");
    Response::new(200, "text/xml", maps.into_bytes())
}

/// A minimal SWF header — enough for `fetch_movie` to succeed.
fn fake_swf() -> Response {
    let mut body = b"FWS".to_vec();
    body.push(6); // version
    body.extend_from_slice(&16u32.to_le_bytes()); // declared length
    body.extend_from_slice(&[0u8; 8]);
    Response::new(200, "application/x-shockwave-flash", body)
}

// ------------------------------------------------------------------
// session gating

fn authenticated(req: &Request, shared: &Arc<Shared>) -> Option<SidInfo> {
    let sid = req.cookie("dosid")?;
    shared.sid_account(&sid)
}

/// Invalid/expired sid → redirect to front page; `launch::load` maps it
/// to `Error::SessionExpired`, `check_sid` to `SidStatus::Invalid`.
fn sid_invalid(origin: &str) -> Response {
    Response::redirect(&format!("{origin}/"))
}

// ------------------------------------------------------------------
// ajax stubs

/// `ajax/shop.php` — the live `check_sid` probe POSTs a purchase; a 200
/// body containing `redirectToLogin` means the sid is dead.
fn ajax_stub(req: &Request, shared: &Arc<Shared>) -> Response {
    if authenticated(req, shared).is_none() {
        return Response::new(
            200,
            "text/html",
            b"<html><body>redirectToLogin</body></html>".to_vec(),
        );
    }
    Response::json(&json!({"result": "OK"}))
}

/// `ajax/user.php` `loadUserInfo` — base62 `userId` → username.
fn ajax_user(req: &Request, shared: &Arc<Shared>) -> Response {
    if authenticated(req, shared).is_none() {
        return ajax_stub(req, shared);
    }
    let form = req.form();
    if form.get("command").map(String::as_str) != Some("loadUserInfo") {
        return Response::json(&json!({"result": "KO"}));
    }
    let user_id = base62_decode(form.get("userId").map(String::as_str).unwrap_or(""));
    match shared
        .cfg
        .backpage
        .accounts
        .iter()
        .find(|a| a.user_id as i64 == user_id)
    {
        Some(a) => Response::json(&json!({
            "result": "OK",
            "userName": percent_encode(&a.username),
            "url": format!("/p/{}-{}", base62_encode(user_id), a.username),
        })),
        None => Response::json(&json!({"result": "KO"})),
    }
}

/// `ajax/pilotprofil.php` `searchProfileFromExternalPPP` — username → id.
fn ajax_pilotprofil(req: &Request, shared: &Arc<Shared>) -> Response {
    if authenticated(req, shared).is_none() {
        return ajax_stub(req, shared);
    }
    let form = req.form();
    let wanted = percent_decode(
        form.get("profileUsername")
            .map(String::as_str)
            .unwrap_or(""),
    );
    match shared
        .cfg
        .backpage
        .accounts
        .iter()
        .find(|a| a.username == wanted)
    {
        Some(a) => Response::json(&json!({
            "result": "OK",
            "userName": percent_encode(&a.username),
            "url": format!("/p/{}-{}", base62_encode(a.user_id as i64), a.username),
        })),
        None => Response::json(&json!({"result": "KO"})),
    }
}

// ------------------------------------------------------------------
// hangar / inventory

/// `flashAPI/inventory.php` — request has `action` + base64 `params`;
/// the response body is base64 JSON `{isError, data:{...}}`.
fn inventory(req: &Request, shared: &Arc<Shared>) -> Response {
    if authenticated(req, shared).is_none() {
        return sid_invalid(&req.origin());
    }
    let params_src = if req.method == "GET" {
        req.query.clone()
    } else {
        req.form()
    };
    let action = params_src
        .get("action")
        .map(String::as_str)
        .unwrap_or("")
        .to_string();
    let sid = req.cookie("dosid").unwrap_or_default();
    let credits = 1_500_000u64 + (sid.len() as u64 % 997);
    let hangar = json!({
        "hangarID": "1",
        "hangar_is_active": true,
        "general": {
            "drones": [
                {"I": "d1", "L": "drone_iris", "LV": 6, "HP": "100%", "repair": 0, "currency": "credits"},
                {"I": "d2", "L": "drone_iris", "LV": 6, "HP": "45%", "repair": 500, "currency": "credits"},
            ],
            "hangarShips": [{"L": "ship_goliath"}],
        }
    });
    let data = match action.as_str() {
        "getHangarList" => json!({"hangars": [hangar]}),
        "getHangar" => json!({"hangars": [hangar]}),
        _ => json!({}),
    };
    let payload = json!({
        "isError": 0,
        "data": {
            "ret": {
                "hangars": data["hangars"].clone(),
                "items": [],
                "itemInfo": [],
                "shipInfo": [{"L": "ship_goliath", "C": 1}],
            },
            "money": {"credits": credits.to_string(), "uridium": "25000"},
            "map": {"lootIds": ["", "drone_flax", "drone_iris", "drone_apis", "drone_zeus"]},
        }
    });
    Response::new(
        200,
        "text/plain",
        BASE64_STANDARD.encode(payload.to_string()).into_bytes(),
    )
}

/// Account record for control/debug output.
pub fn account_view(a: &Account) -> Value {
    json!({
        "username": a.username,
        "user_id": a.user_id,
        "faction_id": a.faction_id,
        "has_preissued_sid": a.sid.is_some(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base62_roundtrip() {
        for i in [1, 61, 62, 12345, 987654321] {
            assert_eq!(base62_decode(&base62_encode(i)), i);
        }
        assert_eq!(base62_encode(0), "a");
    }
}
