//! Shared mutable state between the backpage, game server, proxy and
//! control API: issued `dosid`s, live game sessions, fault switches and
//! a bounded event log.

use crate::config::Config;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;

pub fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Commands pushed into a live game session task.
#[derive(Debug)]
pub enum SessionCmd {
    /// Second login on the same account — drop this connection.
    Kick { reason: String },
    /// Fault-injection disconnect.
    Drop { reason: String },
    /// Control API: destroy the hero (ShipDestroyed + KillScreenPost).
    KillHero,
}

/// Live game session record for the control API.
pub struct GameSessionEntry {
    pub user_id: i32,
    pub session_id: String,
    pub peer: SocketAddr,
    pub connected_at: Instant,
    pub cmd: mpsc::Sender<SessionCmd>,
    pub stats: std::sync::Arc<SessionStats>,
}

#[derive(Default)]
pub struct SessionStats {
    pub rx_packets: AtomicU64,
    pub tx_packets: AtomicU64,
    pub checksum_violations: AtomicU64,
}

/// An issued `dosid`.
#[derive(Clone, Debug, Serialize)]
pub struct SidInfo {
    pub user_id: i32,
    pub username: String,
    pub issued_at: u64,
    /// `None` = never expires.
    pub expires_at: Option<u64>,
}

impl SidInfo {
    pub fn is_expired(&self) -> bool {
        self.expires_at.is_some_and(|end| now_secs() >= end)
    }
}

/// One event row shown by the control API (bounded buffer).
#[derive(Clone, Debug, Serialize)]
pub struct EventEntry {
    pub at_unix: u64,
    pub kind: String,
    pub detail: String,
}

/// One proxy observation (CONNECT attempt or sniffed game login).
#[derive(Clone, Debug, Serialize)]
pub struct ProxyEvent {
    pub at_unix: u64,
    pub peer: String,
    pub target: String,
    pub outcome: String,
    pub bytes_up: u64,
    pub bytes_down: u64,
    pub detail: Option<String>,
}

/// Runtime fault switches — mutated live through `POST /faults`.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Faults {
    #[serde(default)]
    pub backpage: BackpageFaults,
    #[serde(default)]
    pub game: GameFaults,
    #[serde(default)]
    pub proxy: ProxyFaults,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct BackpageFaults {
    /// Extra latency added to every backpage response.
    #[serde(default)]
    pub latency_ms: u64,
    /// Front page demands a CAPTCHA → `Error::CaptchaRequired` client-side.
    #[serde(default)]
    pub captcha: bool,
    /// All password logins fail (no dosid issued).
    #[serde(default)]
    pub reject_credentials: bool,
    /// HTTP 429 on the login POST.
    #[serde(default)]
    pub rate_limit: bool,
    /// Whole backpage answers 503.
    #[serde(default)]
    pub offline: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct GameFaults {
    /// Per-packet outbound latency applied to every session.
    #[serde(default)]
    pub latency_ms: u64,
    /// `VersionCommand.equal = false` regardless of the client hash.
    #[serde(default)]
    pub version_mismatch: bool,
    /// Force `LoginResponse` status to this value (e.g. 6 = bad sid).
    #[serde(default)]
    pub login_reject_status: Option<i16>,
    /// Send `LoginErrorCommand(errorCode)` instead of LoginResponse.
    #[serde(default)]
    pub login_error_code: Option<i16>,
    /// Close the connection this many ms after login succeeds.
    #[serde(default)]
    pub drop_after_ms: Option<u64>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ProxyFaults {
    /// Delay before honoring CONNECT / forwarding.
    #[serde(default)]
    pub latency_ms: u64,
    /// Deny every CONNECT with 403 and refuse forwarding.
    #[serde(default)]
    pub deny: bool,
    /// Tear down proxied connections after this many ms.
    #[serde(default)]
    pub drop_after_ms: Option<u64>,
}

/// Everything every component shares.
pub struct Shared {
    pub cfg: Config,
    pub faults: std::sync::RwLock<Faults>,
    /// sid → session info.
    pub sids: Mutex<HashMap<String, SidInfo>>,
    /// user_id → live game session (one per account; a second login
    /// kicks the first — see orchestrator README "Game session").
    pub sessions: Mutex<HashMap<i32, GameSessionEntry>>,
    pub events: Mutex<VecDeque<EventEntry>>,
    pub proxy_events: Mutex<VecDeque<ProxyEvent>>,
    pub started: Instant,
    /// Actual bound addresses, filled in during `MockServer::start`.
    pub addrs: Mutex<BoundAddrs>,
    pub sid_counter: AtomicU64,
    pub http_requests: AtomicU64,
    pub game_logins: AtomicU64,
    pub game_logins_rejected: AtomicU64,
    pub proxy_connects: AtomicU64,
}

/// Bound addresses once listeners are up.
#[derive(Clone, Debug, Default)]
pub struct BoundAddrs {
    pub backpage: Option<SocketAddr>,
    pub game: Option<SocketAddr>,
    pub proxy: Option<SocketAddr>,
    pub control: Option<SocketAddr>,
}

const MAX_EVENTS: usize = 512;
const MAX_PROXY_EVENTS: usize = 512;

impl Shared {
    pub fn new(cfg: Config) -> Self {
        let shared = Self {
            faults: std::sync::RwLock::new(cfg.faults.clone()),
            cfg,
            sids: Mutex::new(HashMap::new()),
            sessions: Mutex::new(HashMap::new()),
            events: Mutex::new(VecDeque::new()),
            proxy_events: Mutex::new(VecDeque::new()),
            started: Instant::now(),
            addrs: Mutex::new(BoundAddrs::default()),
            sid_counter: AtomicU64::new(0),
            http_requests: AtomicU64::new(0),
            game_logins: AtomicU64::new(0),
            game_logins_rejected: AtomicU64::new(0),
            proxy_connects: AtomicU64::new(0),
        };
        // Pre-issue configured sids.
        let mut sids = shared.sids.lock().unwrap();
        for account in &shared.cfg.backpage.accounts {
            if let Some(sid) = &account.sid {
                sids.insert(
                    sid.clone(),
                    SidInfo {
                        user_id: account.user_id,
                        username: account.username.clone(),
                        issued_at: now_secs(),
                        expires_at: None,
                    },
                );
            }
        }
        drop(sids);
        shared
    }

    pub fn event(&self, kind: &str, detail: impl Into<String>) {
        let mut events = self.events.lock().unwrap();
        if events.len() >= MAX_EVENTS {
            events.pop_front();
        }
        events.push_back(EventEntry {
            at_unix: now_secs(),
            kind: kind.to_string(),
            detail: detail.into(),
        });
    }

    pub fn proxy_event(&self, ev: ProxyEvent) {
        let mut log = self.proxy_events.lock().unwrap();
        if log.len() >= MAX_PROXY_EVENTS {
            log.pop_front();
        }
        log.push_back(ev);
    }

    pub fn faults(&self) -> Faults {
        self.faults.read().unwrap().clone()
    }

    /// Deep-merge a JSON patch into the current faults.
    pub fn patch_faults(&self, patch: &serde_json::Value) -> Result<(), serde_json::Error> {
        let mut current = serde_json::to_value(&*self.faults.read().unwrap())?;
        merge(&mut current, patch);
        let faults: Faults = serde_json::from_value(current)?;
        *self.faults.write().unwrap() = faults;
        Ok(())
    }

    /// Look up a sid and check expiry. Returns the account row.
    pub fn sid_account(&self, sid: &str) -> Option<SidInfo> {
        let sids = self.sids.lock().unwrap();
        sids.get(sid).filter(|s| !s.is_expired()).cloned()
    }

    /// Mint a new sid for an account.
    pub fn issue_sid(&self, account: &crate::config::Account) -> String {
        let n = self.sid_counter.fetch_add(1, Ordering::Relaxed);
        let ttl = self.cfg.backpage.session_ttl_secs;
        let sid = format!(
            "{:016x}{:016x}",
            self.cfg.seed ^ (account.user_id as u64).wrapping_mul(0x1000_0000_01B3),
            (n + 1).wrapping_mul(0x2545_F491_4F6C_DD1D)
        );
        self.sids.lock().unwrap().insert(
            sid.clone(),
            SidInfo {
                user_id: account.user_id,
                username: account.username.clone(),
                issued_at: now_secs(),
                expires_at: (ttl > 0).then(|| now_secs() + ttl),
            },
        );
        sid
    }
}

fn merge(base: &mut serde_json::Value, patch: &serde_json::Value) {
    match (base, patch) {
        (serde_json::Value::Object(b), serde_json::Value::Object(p)) => {
            for (k, v) in p {
                merge(b.entry(k.clone()).or_insert(serde_json::Value::Null), v);
            }
        }
        (base, patch) => *base = patch.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn faults_merge_patch() {
        let shared = Shared::new(Config::from_json_str("{}").unwrap());
        shared
            .patch_faults(&serde_json::json!({"game": {"version_mismatch": true}}))
            .unwrap();
        assert!(shared.faults().game.version_mismatch);
        assert!(!shared.faults().backpage.captcha);
        shared
            .patch_faults(&serde_json::json!({"game": {"latency_ms": 50}}))
            .unwrap();
        assert_eq!(shared.faults().game.latency_ms, 50);
        // merge didn't clear the earlier flag
        assert!(shared.faults().game.version_mismatch);
    }

    #[test]
    fn sid_issue_and_lookup() {
        let shared = Shared::new(Config::from_json_str("{}").unwrap());
        let acc = crate::config::Account::default();
        let sid = shared.issue_sid(&acc);
        assert_eq!(shared.sid_account(&sid).unwrap().user_id, 1001);
        assert!(shared.sid_account("nope").is_none());
    }
}
