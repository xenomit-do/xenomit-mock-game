//! Declarative configuration for the mock — JSON file plus CLI overrides.

use serde::{Deserialize, Serialize};

/// Top-level mock configuration. Every listener accepts `"host:port"`;
/// port `0` picks a free port (reported on the `ready` line and through
/// the control API).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Config {
    /// Deterministic seed for the world simulation.
    #[serde(default = "default_seed")]
    pub seed: u64,
    #[serde(default)]
    pub backpage: BackpageConfig,
    #[serde(default)]
    pub game: GameConfig,
    /// Forward proxy; `None`/`enabled = false` disables it.
    #[serde(default)]
    pub proxy: ProxyConfig,
    /// Localhost control API.
    #[serde(default)]
    pub control: ControlConfig,
    /// Runtime fault injection, all unset/disabled by default.
    #[serde(default)]
    pub faults: crate::state::Faults,
}

fn default_seed() -> u64 {
    0x5EED_5EED_5EED_5EED
}

impl Default for Config {
    fn default() -> Self {
        Self {
            seed: default_seed(),
            backpage: BackpageConfig::default(),
            game: GameConfig::default(),
            proxy: ProxyConfig::default(),
            control: ControlConfig::default(),
            faults: crate::state::Faults::default(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BackpageConfig {
    #[serde(default = "default_backpage_listen")]
    pub listen: String,
    /// `dosid` lifetime in seconds; `0` = never expires.
    #[serde(default = "default_session_ttl")]
    pub session_ttl_secs: u64,
    #[serde(default)]
    pub accounts: Vec<Account>,
}

impl Default for BackpageConfig {
    fn default() -> Self {
        Self {
            listen: default_backpage_listen(),
            session_ttl_secs: default_session_ttl(),
            accounts: vec![Account::default()],
        }
    }
}

fn default_backpage_listen() -> String {
    "127.0.0.1:0".into()
}
fn default_session_ttl() -> u64 {
    30 * 60
}

/// A mock account. `sid` optionally pre-issues a session id so tests can
/// do `dosid` logins without the password flow.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Account {
    #[serde(default = "default_username")]
    pub username: String,
    #[serde(default = "default_password")]
    pub password: String,
    #[serde(default = "default_user_id")]
    pub user_id: i32,
    /// Pre-issued dosid for `login_with_sid`-style flows.
    #[serde(default)]
    pub sid: Option<String>,
    #[serde(default)]
    pub faction_id: i16,
    #[serde(default = "default_instance_id")]
    pub instance_id: i32,
}

impl Default for Account {
    fn default() -> Self {
        Self {
            username: default_username(),
            password: default_password(),
            user_id: default_user_id(),
            sid: None,
            faction_id: 1,
            instance_id: default_instance_id(),
        }
    }
}

fn default_username() -> String {
    "bot".into()
}
fn default_password() -> String {
    "bot-pass".into()
}
fn default_user_id() -> i32 {
    1001
}
fn default_instance_id() -> i32 {
    9
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GameConfig {
    #[serde(default = "default_game_listen")]
    pub listen: String,
    /// Map id the hero spawns on; `maps.php` serves this map plus the
    /// jump targets around it.
    #[serde(default = "default_map_id")]
    pub map_id: i32,
    #[serde(default = "default_map_width")]
    pub map_width: i32,
    #[serde(default = "default_map_height")]
    pub map_height: i32,
    /// Simulation tick period in milliseconds.
    #[serde(default = "default_tick_ms")]
    pub tick_ms: u64,
    #[serde(default = "default_npc_count")]
    pub npc_count: usize,
    #[serde(default = "default_player_count")]
    pub player_count: usize,
    #[serde(default = "default_box_count")]
    pub box_count: usize,
    #[serde(default = "default_ore_count")]
    pub ore_count: usize,
    /// Expected client version hash; mismatches answer `equal=false`.
    #[serde(default = "default_version_hash")]
    pub version_hash: String,
    /// Reject `LoginRequest`s whose `sessionID` was never issued by the
    /// mock Backpage. Turn off for protocol-level fuzzing.
    #[serde(default = "default_true")]
    pub strict_auth: bool,
    /// Issue a pet on this account (PetInitialization hasPet/hasFuel).
    #[serde(default = "default_true")]
    pub has_pet: bool,
}

impl Default for GameConfig {
    fn default() -> Self {
        Self {
            listen: default_game_listen(),
            map_id: default_map_id(),
            map_width: default_map_width(),
            map_height: default_map_height(),
            tick_ms: default_tick_ms(),
            npc_count: default_npc_count(),
            player_count: default_player_count(),
            box_count: default_box_count(),
            ore_count: default_ore_count(),
            version_hash: default_version_hash(),
            strict_auth: default_true(),
            has_pet: default_true(),
        }
    }
}

fn default_game_listen() -> String {
    "127.0.0.1:0".into()
}
fn default_map_id() -> i32 {
    16
}
fn default_map_width() -> i32 {
    21_000
}
fn default_map_height() -> i32 {
    13_100
}
fn default_tick_ms() -> u64 {
    100
}
fn default_npc_count() -> usize {
    6
}
fn default_player_count() -> usize {
    2
}
fn default_box_count() -> usize {
    8
}
fn default_ore_count() -> usize {
    4
}
fn default_version_hash() -> String {
    xenomit_packet_client::modules::VERSION_HASH.into()
}
fn default_true() -> bool {
    true
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProxyConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_proxy_listen")]
    pub listen: String,
    /// Optional target allowlist (`host` or `host:port` entries); empty =
    /// allow everything.
    #[serde(default)]
    pub allow: Vec<String>,
}

impl Default for ProxyConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            listen: default_proxy_listen(),
            allow: Vec::new(),
        }
    }
}

fn default_proxy_listen() -> String {
    "127.0.0.1:0".into()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ControlConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_control_listen")]
    pub listen: String,
}

impl Default for ControlConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            listen: default_control_listen(),
        }
    }
}

fn default_control_listen() -> String {
    "127.0.0.1:0".into()
}

impl Config {
    pub fn from_json_str(text: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(text)
    }

    pub fn from_file(path: &std::path::Path) -> std::io::Result<Self> {
        let text = std::fs::read_to_string(path)?;
        serde_json::from_str(&text)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_complete() {
        let cfg: Config = serde_json::from_str("{}").unwrap();
        assert_eq!(cfg.seed, default_seed());
        assert_eq!(cfg.backpage.accounts.len(), 1);
        assert!(cfg.proxy.enabled);
        assert!(cfg.control.enabled);
        assert_eq!(cfg.game.map_id, 16);
    }

    #[test]
    fn partial_config_fills_defaults() {
        let cfg: Config =
            serde_json::from_str(r#"{"seed":7,"game":{"map_id":1,"npc_count":3}}"#).unwrap();
        assert_eq!(cfg.seed, 7);
        assert_eq!(cfg.game.map_id, 1);
        assert_eq!(cfg.game.npc_count, 3);
        assert_eq!(cfg.game.player_count, 2);
    }
}
