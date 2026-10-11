# xenomit-mock-game

Deterministic local mock of the DarkOrbit Backpage web service and game
server for `xenomit-*` bot integration tests and benchmarks (e.g. "50
bots against a mock game server").

Four listeners in one process:

| Listener | Default        | Purpose                                        |
|----------|----------------|------------------------------------------------|
| backpage | `127.0.0.1:0`  | HTTP Backpage (login, launch, ajax, maps.php)  |
| game     | `127.0.0.1:0`  | packet-client TCP game server                  |
| proxy    | `127.0.0.1:0`  | CONNECT/forward HTTP proxy                     |
| control  | `127.0.0.1:0`  | localhost control API (JSON)                   |

Port `0` picks a free port; the bound addresses print on startup and are
also exposed via `GET /status`.

## Running

```
cargo run -p xenomit-mock-game -- \
    --backpage 127.0.0.1:8080 --game 127.0.0.1:8200 \
    --proxy 127.0.0.1:8888 --control 127.0.0.1:9000 \
    --seed 42 --map 16 --npcs 8 \
    --account bot:bot-pass:1001 --account alt:alt-pass:1002
```

Or embed it in tests:

```rust
let server = MockServer::start(Config::default()).await?;
let session = xenomit_backpage::login::login_with_credentials_at(
    &server.backpage_url(), "bot", "bot-pass").await?;
// session.instance() is pinned to the mock; maps.php resolves the
// game endpoint, or pass `server.game_addr()` to connect directly.
```

`--print-config` dumps the effective JSON config (file + flags merged).

## Configuration

JSON file via `--config`, merged with flag overrides. All fields optional:

```json
{
  "seed": 42,
  "backpage": {
    "listen": "127.0.0.1:8080",
    "session_ttl_secs": 1800,
    "accounts": [
      {"username": "bot", "password": "bot-pass", "user_id": 1001,
       "sid": null, "faction_id": 1, "instance_id": 9}
    ]
  },
  "game": {
    "listen": "127.0.0.1:8200", "map_id": 16,
    "map_width": 21000, "map_height": 13100, "tick_ms": 100,
    "npc_count": 6, "player_count": 2, "box_count": 8, "ore_count": 4,
    "version_hash": "43e9184871f4b91cc4158e5c5bef3d3f",
    "strict_auth": true, "has_pet": true
  },
  "proxy": {"enabled": true, "listen": "127.0.0.1:8888", "allow": []},
  "control": {"enabled": true, "listen": "127.0.0.1:9000"},
  "faults": {
    "backpage": {"latency_ms": 0, "captcha": false,
                 "rate_limit": false, "reject_credentials": false},
    "game": {"latency_ms": 0, "version_mismatch": false,
             "drop_after_ms": null, "login_reject_status": null,
             "login_error_code": null},
    "proxy": {"latency_ms": 0, "deny": false}
  }
}
```

- `strict_auth` (default true) rejects game `LoginRequest`s whose
  `sessionID` was never issued by the mock Backpage.
- `allow` restricts CONNECT targets by `host` or `host:port`; empty
  allowlist means allow everything.

## Backpage surface

Serves what `xenomit-backpage` and `xenomit-bot-runtime` consume:

- `GET /` — front page with `bgcdw_login_form`; captcha markup when the
  fault is on.
- `POST /bgcdw_login` — credential check → `Set-Cookie: dosid=…` +
  redirect.
- `indexInternal.es?action=internalMapRevolution` — flashembed launch
  page with authenticated flashvars (`userID`, `sessionID`, `pid`,
  `mapID`, `dynamicHost`, …).
- `POST flashAPI/loadingScreen.php` — JSON echo including `sessionID`.
- `indexInternal.es?action=internalDock|internalSkylab` —
  sid-validation targets (302 → expired sid).
- `flashAPI/inventory.php` (`getHangarList`), `ajax/user.php`
  (`loadUserInfo`), `ajax/pilotprofil.php` (profile search),
  `ajax/shop.php`, `internalSkylab`, `spacemap/xml/maps.php`
  (`<gameserverIP>` resolution used by bot-runtime).

A `dosid` lives `session_ttl_secs`; expiry answers internal pages with a
login redirect (`SidStatus::Invalid` / `SessionExpired`).

## Game server

Speaks the `xenomit-packet-client` wire protocol end to end:

`0x00` greeting → `VersionRequest`/`VersionCommand` →
`LoginRequest`/`LoginResponse` → world-init burst (ship init, map
change, portals + POI, NPC/player `ShipCreate` + `MoveCommand`, boxes,
ores, credits/uridium/ores/HP/shield/speed attributes, config count,
configuration echo, trade info, galaxy-gate builder, menu init, pet
init) → periodic ticks.

Handled client packets: move, laser/rocket attack + abort, collect
box/ore, harvest, jump (portal range → `JumpInitiated` + `MapChanged` +
new map content, else `JumpDenied`), P.E.T. activate/deactivate, menu
item use, kill-screen repair (respawn), trade/sell ore, refine, gate
spins (`GALAXY_GATE_MULTI_ENERGY_REQUEST`), `S|CFG` config switch, ping.
Unknown packets are accepted silently.

**One session per account**: a second game login for a `user_id` kicks
the earlier connection, matching the orchestrator's handoff model.

The world is deterministic: seed + user_id + map_id fully determine NPC
positions, collectables and portals, so repeated runs are identical.

## Control API

```
GET  /status                    uptime, bound addrs, counters, session count
GET  /sessions                  live game sessions (+rx/tx packet counts)
POST /sessions/{uid}/kick|drop  close the session socket
POST /sessions/{uid}/kill       kill the hero (kill-screen flow)
POST /sids   {"user_id":N}      mint a fresh dosid (sid-login flows)
GET  /accounts                  configured accounts
GET  /events                    event ring buffer (logins, kicks, faults)
GET  /faults                    effective fault state
POST /faults   <json>           merge-patch fault state at runtime
POST /faults/reset              restore config-time faults
GET  /proxy/log                 proxy connections + sniffed game logins
POST /shutdown                  graceful shutdown
```

`POST /faults` merges, e.g. `{"game": {"drop_after_ms": 500}}` only
touches that field.

## Fault injection

- **backpage.latency_ms** — delay every response.
- **backpage.captcha** — front page serves `bgcdw_captcha` markup →
  `Error::CaptchaRequired`.
- **backpage.rate_limit** — login POST answers `429`.
- **backpage.reject_credentials** — login always redirects →
  `Error::WrongCredentials`.
- **game.latency_ms** — delay every outbound packet.
- **game.version_mismatch** — `VersionCommand` answers `equal=false`.
- **game.login_reject_status** — `LoginResponse.status = N`.
- **game.login_error_code** — `LoginErrorCommand` instead of a response.
- **game.drop_after_ms** — socket dies N ms after login.
- **proxy.latency_ms / proxy.deny** — delayed or `403` CONNECTs.

## Proxy

`mock-proxy` is a standalone CONNECT forward proxy (same code path as
the bundled one):

```
cargo run --bin mock-proxy -- --listen 127.0.0.1:8888 \
    --control 127.0.0.1:9000 --latency 25
```

CONNECT answers `200 Connection Established` and then splices bytes;
game traffic (detected by the `0x00` greeting) is sniffed for the
`LoginRequest` (`userID`/`sessionID`) and recorded in `/proxy/log`.
Absolute-form `GET`/`POST` forwarding works too.

## Tests

```
cargo test
```

Unit tests cover config parsing, the seeded RNG, HTTP helpers, fault
merging and the pure world-sim (`handle`/`tick_packets`). Integration
tests run the real round trip: `login_with_credentials_at` →
`launch::load` → `maps.php` → packet handshake → world snapshot, plus
move/attack/collect acknowledgements, kill-screen repair, second-login
kick, all fault modes, sid minting, TTL expiry and proxy CONNECT/forward.
