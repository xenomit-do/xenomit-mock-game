//! Mock game TCP server — wire-compatible with `xenomit-packet-client`.
//!
//! Per connection: one `0x00` greeting byte, then framed
//! `[u24 len][i16 id][fields]` modules. Handshake is
//! `VersionRequest → VersionCommand → LoginRequest → LoginResponse`,
//! then a world-init burst and a deterministic tick-driven sim.
//! Client requests (move/attack/collect/jump/pet/menu/…) are answered
//! with the same packets the real server would push.

use crate::rng::Rng;
use crate::state::{GameSessionEntry, SessionCmd, SessionStats, Shared};
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use xenomit_packet_client::frame::{FrameDecoder, frame_body};
use xenomit_packet_client::modules::id;
use xenomit_packet_client::packet::{Packet, Value, decode_body, encode_body};

/// Server-side login rejection codes (any nonzero → client
/// `ClientError::LoginRejected`).
pub const STATUS_BAD_SESSION: i64 = 6;

// ------------------------------------------------------------------
// packet helpers

fn mk(module_id: i16, fields: Vec<(&'static str, Value)>) -> Packet {
    Packet {
        id: module_id,
        name: xenomit_packet_client::registry::module_name(module_id),
        fields: fields.into_iter().map(|(n, v)| (Some(n), v)).collect(),
    }
}

fn i(name: &'static str, v: i64) -> (&'static str, Value) {
    (name, Value::Int(v))
}
fn s(name: &'static str, v: impl Into<String>) -> (&'static str, Value) {
    (name, Value::Str(v.into()))
}
fn b(name: &'static str, v: bool) -> (&'static str, Value) {
    (name, Value::Bool(v))
}
fn f(name: &'static str, v: f64) -> (&'static str, Value) {
    (name, Value::F64(v))
}
fn null(name: &'static str) -> (&'static str, Value) {
    (name, Value::Null)
}
fn list(name: &'static str, v: Vec<Value>) -> (&'static str, Value) {
    (name, Value::List(v))
}

fn ore_count_module(ore_type: i64, count: f64) -> Value {
    Value::Module(Box::new(mk(
        id::ORE_COUNT_MODULE,
        vec![
            (
                "oreType",
                Value::Module(Box::new(mk(
                    id::ORE_TYPE_MODULE,
                    vec![i("typeValue", ore_type)],
                ))),
            ),
            ("count", Value::F64(count)),
        ],
    )))
}

// ------------------------------------------------------------------
// deterministic world sim

struct Npc {
    id: i32,
    name: String,
    type_id: String,
    x: i32,
    y: i32,
    tx: i32,
    ty: i32,
    /// px per 100ms tick.
    speed: i32,
    hp: f64,
    hp_max: f64,
    npc: bool,
    faction: i32,
    /// ticks since its MoveCommand was last broadcast.
    since_move_emit: u32,
}

struct Collectable {
    hash: String,
    kind: CollectKind,
    x: i32,
    y: i32,
    /// boxType string or ore typeValue.
    aux: i32,
}

#[derive(Clone, Copy, PartialEq)]
enum CollectKind {
    Box,
    Ore,
}

#[derive(Clone, Copy)]
struct Portal {
    id: i32,
    design_id: i32,
    x: i32,
    y: i32,
    target_map: i32,
}

const NPC_TYPES: &[&str] = &[
    "ship_streuner",
    "ship_devolarium",
    "ship_mordon",
    "ship_saimon",
    "ship_lordakia",
];
const PLAYER_TYPES: &[&str] = &["ship_goliath", "ship_vengeance", "ship_bigboy"];

/// Deterministic per-session world. Everything derives from
/// `(seed, user_id, map_id)` so two runs produce identical traffic.
struct World {
    rng: Rng,
    cfg: crate::config::GameConfig,
    /// Stable per-session value used to re-seed map generation.
    seed_base: u64,
    map_id: i32,
    width: i32,
    height: i32,
    tick: u64,
    hero_x: i32,
    hero_y: i32,
    hero_tx: i32,
    hero_ty: i32,
    hero_moving: bool,
    hero_speed: i32,
    hero_hp: f64,
    hero_hp_max: f64,
    hero_shield: i32,
    hero_dead: bool,
    attack_target: Option<i32>,
    pet_id: Option<i32>,
    credits: i64,
    uridium: i64,
    /// ore typeValue → count.
    ores: Vec<(i64, f64)>,
    npcs: Vec<Npc>,
    collectables: Vec<Collectable>,
    portals: Vec<Portal>,
    configuration: i32,
    /// gate id → parts collected.
    gates: Vec<(i32, i32)>,
    collect_counter: u32,
}

impl World {
    fn new(seed: u64, user_id: i32, cfg: &crate::config::GameConfig) -> Self {
        let mut w = World {
            rng: Rng::for_session(seed, user_id),
            cfg: cfg.clone(),
            seed_base: seed ^ (user_id as u64).wrapping_mul(0x2545_F491_4F6C_DD1D),
            map_id: cfg.map_id,
            width: cfg.map_width,
            height: cfg.map_height,
            tick: 0,
            hero_x: cfg.map_width / 2,
            hero_y: cfg.map_height / 2,
            hero_tx: cfg.map_width / 2,
            hero_ty: cfg.map_height / 2,
            hero_moving: false,
            hero_speed: 360,
            hero_hp: 256_000.0,
            hero_hp_max: 256_000.0,
            hero_shield: 100_000,
            hero_dead: false,
            attack_target: None,
            pet_id: None,
            credits: 1_500_000,
            uridium: 25_000,
            ores: vec![(0, 500.0), (1, 400.0), (2, 300.0), (3, 20.0)],
            npcs: Vec::new(),
            collectables: Vec::new(),
            portals: Vec::new(),
            configuration: 1,
            gates: vec![(1, 12), (2, 5)],
            collect_counter: 0,
        };
        w.populate_map();
        w
    }

    /// (Re)build map-local content — deterministic per `map_id`.
    fn populate_map(&mut self) {
        let cfg = self.cfg.clone();
        // NPC positions depend on the map too: re-seed a scratch rng so
        // map 16 always looks like map 16 regardless of jump order.
        let mut rng =
            Rng::new(self.seed_base ^ (self.map_id as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15));
        self.npcs.clear();
        self.collectables.clear();
        self.portals.clear();
        for i in 0..cfg.npc_count + cfg.player_count {
            let npc = i < cfg.npc_count;
            let type_id = if npc {
                NPC_TYPES[i % NPC_TYPES.len()]
            } else {
                PLAYER_TYPES[i % PLAYER_TYPES.len()]
            };
            let (x, y) = (
                rng.range(500, self.width - 500),
                rng.range(500, self.height - 500),
            );
            let (tx, ty) = (
                rng.range(500, self.width - 500),
                rng.range(500, self.height - 500),
            );
            self.npcs.push(Npc {
                id: 2_000_001 + i as i32,
                name: if npc {
                    type_id.trim_start_matches("ship_").to_string()
                } else {
                    format!("pilot-{i}")
                },
                type_id: type_id.into(),
                x,
                y,
                tx,
                ty,
                speed: rng.range(20, 60),
                hp: 20_000.0 + (i as f64) * 5_000.0,
                hp_max: 20_000.0 + (i as f64) * 5_000.0,
                npc,
                faction: if npc { 0 } else { 1 + (i as i32 % 3) },
                since_move_emit: 99,
            });
        }
        for i in 0..cfg.box_count {
            self.collectables.push(Collectable {
                hash: format!("box_{}_{:04x}", self.map_id, i),
                kind: CollectKind::Box,
                x: rng.range(300, self.width - 300),
                y: rng.range(300, self.height - 300),
                aux: 0,
            });
        }
        for i in 0..cfg.ore_count {
            self.collectables.push(Collectable {
                hash: format!("ore_{}_{:04x}", self.map_id, i),
                kind: CollectKind::Ore,
                x: rng.range(300, self.width - 300),
                y: rng.range(300, self.height - 300),
                aux: (i % 3) as i32, // prometium/endurium/terbium
            });
        }
        // One portal at the hero spawn (always in jump range) plus one
        // across the map; they chain map ± 1.
        self.portals.push(Portal {
            id: 10,
            design_id: 1,
            x: self.hero_x + 100,
            y: self.hero_y,
            target_map: self.map_id + 1,
        });
        self.portals.push(Portal {
            id: 11,
            design_id: 2,
            x: self.width - 800,
            y: self.height - 800,
            target_map: (self.map_id - 1).max(1),
        });
    }

    // --------------------------------------------------------------
    // world-init burst — order loosely mirrors the real server

    fn init_burst(
        &self,
        user_id: i32,
        session_id: &str,
        faction: i32,
        has_pet: bool,
    ) -> Vec<Packet> {
        let mut out = Vec::new();
        // Hero init — everything the model's HeroState reads.
        out.push(mk(
            id::SHIP_INITIALIZATION,
            vec![
                i("userId", user_id as i64),
                s("userName", format!("bot-{user_id}")),
                s("typeId", "ship_goliath"),
                i("speed", self.hero_speed as i64),
                i("shield", self.hero_shield as i64),
                i("shieldMax", self.hero_shield as i64),
                f("hitPoints", self.hero_hp),
                f("hitMax", self.hero_hp_max),
                i("cargoSpace", 0),
                i("cargoSpaceMax", 4000),
                i("nanoHull", 256_000),
                i("maxNanoHull", 256_000),
                i("x", self.hero_x as i64),
                i("y", self.hero_y as i64),
                i("mapId", self.map_id as i64),
                i("factionId", faction as i64),
                i("clanId", 0),
                i("expansionStage", 0),
                b("premium", true),
                f("ep", 1_234_567.0),
                f("honourPoints", 10_000.0),
                i("level", 12),
                f("credits", self.credits as f64),
                f("uridium", self.uridium as f64),
                ("jackpot", Value::F32(0.0)),
                i("dailyRank", 42),
                s("clanTag", ""),
                i("galaxyGatesDone", 0),
                b("useSystemFont", false),
                b("cloaked", false),
                b("refreshMap", true),
                i("captainEnergy", 50),
                i("maxCaptainEnergy", 50),
                list("modifier", vec![]),
            ],
        ));
        out.push(mk(id::MAP_CHANGED, vec![i("newMapId", self.map_id as i64)]));
        self.push_map_content(&mut out);
        // Attribute updates: credits/uri/jackpot, speed, cargo space,
        // configuration count, hitpoint info.
        out.push(self.credits_packet());
        out.push(mk(
            id::ATTRIBUTE_SHIP_SPEED_UPDATE,
            vec![
                i("newSpeed", self.hero_speed as i64),
                i("displaySpeed", self.hero_speed as i64),
            ],
        ));
        out.push(mk(
            id::ATTRIBUTE_SPACE_UPDATE,
            vec![i("spaceType", 0), i("spaceLeft", 4000)],
        ));
        out.push(mk(id::ATTRIBUTE_CONFIG_COUNT, vec![i("configCount", 2)]));
        out.push(mk(
            id::HITPOINT_INFO,
            vec![
                f("hitpoints", self.hero_hp),
                f("hitpointsMax", self.hero_hp_max),
                i("nanoHull", 256_000),
                i("nanoHullMax", 256_000),
            ],
        ));
        out.push(mk(
            id::ATTRIBUTE_SHIELD_UPDATE,
            vec![
                i("shieldNow", self.hero_shield as i64),
                i("shieldMax", self.hero_shield as i64),
            ],
        ));
        out.push(self.ore_packet());
        // Gate builder info (two gates with partial parts).
        out.push(self.gate_info_packet());
        // Trade window available (space-station type).
        out.push(mk(
            id::ORE_TRADE_INFO,
            vec![
                i("sepromPrice", 1500),
                i("terbiumPrice", 15),
                i("duraniumPrice", 45),
                i("palladiumExchangeRate", 15),
                i("enduriumPrice", 10),
                i("osmiumPrice", 60),
                i("promeriumPrice", 75),
                i("type", 1),
                i("prometiumPrice", 5),
                i("remainingCharges", -1),
                i("prometidPrice", 30),
            ],
        ));
        // Selected configuration echo: `0|S|CFG|1`.
        out.push(mk(
            id::LEGACY,
            vec![s("message", format!("0|S|CFG|{}", self.configuration))],
        ));
        // Session menu: one usable ammunition item so `UseItem` works.
        out.push(self.menu_init_packet());
        if has_pet {
            out.push(mk(
                id::PET_INITIALIZATION,
                vec![b("hasPet", true), b("hasFuel", true), b("petIsAlive", true)],
            ));
        } else {
            out.push(mk(
                id::PET_INITIALIZATION,
                vec![
                    b("hasPet", false),
                    b("hasFuel", false),
                    b("petIsAlive", false),
                ],
            ));
        }
        // A friendly console line in the session log.
        out.push(log_message(&format!(
            "welcome to the mock, session {session_id}"
        )));
        out
    }

    /// Portals, POI, NPCs and collectables for the current map —
    /// re-sent after every map change.
    fn push_map_content(&self, out: &mut Vec<Packet>) {
        for p in &self.portals {
            out.push(mk(
                id::JUMPGATE_CREATE,
                vec![
                    i("gateId", p.id as i64),
                    i("factionId", 0),
                    i("designId", p.design_id as i64),
                    i("x", p.x as i64),
                    i("y", p.y as i64),
                    b("isVisibleOnMinimap", true),
                    b("showBubble", false),
                    list("decoratorAssetTypes", vec![]),
                ],
            ));
        }
        out.push(mk(
            id::MAP_ADD_POI,
            vec![
                b("inverted", false),
                s("poiTypeSpecification", "base"),
                null("poiType"),
                i("shape", 0),
                b("active", true),
                null("design"),
                s("poiId", format!("base_{}", self.map_id)),
                list("shapeCoordinates", vec![Value::Int(500), Value::Int(500)]),
            ],
        ));
        for n in &self.npcs {
            out.push(mk(
                id::SHIP_CREATE,
                vec![
                    i("userId", n.id as i64),
                    s("typeId", n.type_id.clone()),
                    i("expansionStage", 0),
                    s("clanTag", ""),
                    s("userName", n.name.clone()),
                    i("x", n.x as i64),
                    i("y", n.y as i64),
                    i("factionId", n.faction as i64),
                    i("clanId", 0),
                    i("dailyRank", 0),
                    b("warnBox", false),
                    null("clanDiplomacy"),
                    i("galaxyGatesDone", 0),
                    b("useSystemFont", false),
                    b("npc", n.npc),
                    b("cloaked", false),
                    i("motherShipId", 0),
                    i("positionIndex", 0),
                    s("designSet", ""),
                    list("modifier", vec![]),
                    null("minimapStrategyModule"),
                    s("specialNpcType", ""),
                ],
            ));
            out.push(move_command(n));
        }
        for c in &self.collectables {
            out.push(match c.kind {
                CollectKind::Box => mk(
                    id::ADD_BOX,
                    vec![
                        i("y", c.y as i64),
                        s("hash", c.hash.clone()),
                        i("x", c.x as i64),
                        s("boxType", "BONUS_BOX"),
                    ],
                ),
                CollectKind::Ore => mk(
                    id::ADD_ORE,
                    vec![
                        i("y", c.y as i64),
                        s("hash", c.hash.clone()),
                        i("x", c.x as i64),
                        (
                            "oreType",
                            Value::Module(Box::new(mk(
                                id::ORE_TYPE_MODULE,
                                vec![i("typeValue", c.aux as i64)],
                            ))),
                        ),
                    ],
                ),
            });
        }
    }

    fn credits_packet(&self) -> Packet {
        mk(
            id::ATTRIBUTE_CREDITS_UPDATE,
            vec![
                i("credits", self.credits),
                i("uridium", self.uridium),
                ("jackpot", Value::F32(0.0)),
            ],
        )
    }

    fn ore_packet(&self) -> Packet {
        mk(
            id::ATTRIBUTE_ORE_COUNT_UPDATE,
            vec![list(
                "oreCountList",
                self.ores
                    .iter()
                    .map(|(t, c)| ore_count_module(*t, *c))
                    .collect(),
            )],
        )
    }

    fn gate_info_packet(&self) -> Packet {
        mk(
            id::GALAXY_GATE_INFO,
            vec![
                b("bonusRewardsDay", false),
                i("spinamount_selected", 1),
                list(
                    "spinAmounts",
                    vec![
                        Value::Int(1),
                        Value::Int(5),
                        Value::Int(10),
                        Value::Int(100),
                    ],
                ),
                f("samples", 3.0),
                i("energy_cost", 100),
                b("spinOnSale", false),
                f("money", 25_000.0),
                list(
                    "gates",
                    self.gates
                        .iter()
                        .map(|(gate_id, parts)| {
                            Value::Module(Box::new(mk(
                                id::GALAXY_GATE_GATE_MODULE,
                                vec![
                                    list("bonusGGRewardModules", vec![]),
                                    i("currWave", 0),
                                    s("mode", "alpha"),
                                    i("currPart", *parts as i64),
                                    list("boosts", vec![]),
                                    i("state", 0),
                                    list("probabilityModules", vec![]),
                                    i("lifePrice", -1),
                                    i("id", *gate_id as i64),
                                    i("multiplier", 2),
                                    list("multiGateProgressModules", vec![]),
                                    i("livesLeft", -1),
                                ],
                            )))
                        })
                        .collect(),
                ),
                b("galaxyGateDay", false),
            ],
        )
    }

    fn menu_init_packet(&self) -> Packet {
        let status = mk(
            id::MENU_ITEM_STATUS,
            vec![
                b("visible", true),
                b("selected", false),
                null("toolTipSlotBar"),
                i("counterStyle", 0),
                b("blocked", false),
                f("counterValue", 500.0),
                b("activatable", true),
                s("menuItemId", "ammunition_laser_ucb-100"),
                b("available", true),
                null("toolTipItemBar"),
                b("buyable", true),
                f("maxCounterValue", 10_000.0),
                s("iconLootId", "ammunition_laser_ucb-100"),
            ],
        );
        let item = mk(
            id::MENU_ITEM_INIT,
            vec![
                i("counterType", 0),
                b("showTooltipCooldownTimer", false),
                null("timer"),
                i("actionStyle", 0),
                null("cooldownGroup"),
                ("status", Value::Module(Box::new(status))),
                i("defaultIndex", 0),
            ],
        );
        let category = mk(
            id::MENU_CATEGORY_INIT,
            vec![
                list("menuItems", vec![Value::Module(Box::new(item))]),
                s("categoryId", "ammunition_laser"),
            ],
        );
        mk(
            id::MENU_INITIALIZATION,
            vec![
                list("slotBars", vec![]),
                s("itemBarPosition", "0"),
                list("categories", vec![Value::Module(Box::new(category))]),
            ],
        )
    }

    // --------------------------------------------------------------
    // tick: NPC drift, hero motion, combat ticks

    fn tick_packets(&mut self, user_id: i32) -> Vec<Packet> {
        self.tick += 1;
        let mut out = Vec::new();
        // `rng` is checked out for the whole tick so `self` fields stay
        // borrowable while it is used.
        let mut rng = std::mem::replace(&mut self.rng, Rng::new(1));
        // NPCs drift toward their waypoint; on arrival pick a new one
        // and broadcast a MoveCommand. A low-rate heartbeat re-broadcasts
        // stale destinations so late consumers still observe motion.
        for n in &mut self.npcs {
            if n.id < 0 {
                continue; // destroyed, handled by the respawn pass below
            }
            let (dx, dy) = (n.tx - n.x, n.ty - n.y);
            let dist = ((dx as f64).hypot(dy as f64)).max(1.0);
            let step = (n.speed as f64).min(dist);
            n.x += (dx as f64 / dist * step) as i32;
            n.y += (dy as f64 / dist * step) as i32;
            n.since_move_emit += 1;
            if dist <= step + 1.0 || n.since_move_emit >= 50 {
                if dist <= step + 1.0 {
                    n.tx = rng.range(300, self.width - 300);
                    n.ty = rng.range(300, self.height - 300);
                }
                n.since_move_emit = 0;
                out.push(move_command(n));
            }
        }
        // Hero movement toward its target.
        if self.hero_moving && !self.hero_dead {
            let (dx, dy) = (self.hero_tx - self.hero_x, self.hero_ty - self.hero_y);
            let dist = (dx as f64).hypot(dy as f64);
            // hero_speed is px/sec; one tick ≈ 100ms.
            let step = self.hero_speed as f64 / 10.0;
            if dist <= step {
                self.hero_x = self.hero_tx;
                self.hero_y = self.hero_ty;
                self.hero_moving = false;
            } else {
                self.hero_x += (dx as f64 / dist * step) as i32;
                self.hero_y += (dy as f64 / dist * step) as i32;
            }
            if self.hero_moving || self.tick % 5 == 0 {
                out.push(mk(
                    id::HERO_MOVE,
                    vec![i("x", self.hero_x as i64), i("y", self.hero_y as i64)],
                ));
            }
            if !self.hero_moving {
                out.push(mk(
                    id::HERO_MOVE,
                    vec![i("x", self.hero_x as i64), i("y", self.hero_y as i64)],
                ));
            }
        }
        // Combat tick: hero's laser chews on the locked target.
        if let Some(tid) = self.attack_target
            && !self.hero_dead
        {
            if let Some(n) = self.npcs.iter_mut().find(|n| n.id == tid) {
                let dmg = 3_500.0;
                n.hp -= dmg;
                out.push(mk(
                    id::ATTACK_HIT,
                    vec![
                        (
                            "attackType",
                            Value::Module(Box::new(mk(
                                id::ATTACK_TYPE_MODULE,
                                vec![i("attackTypeValue", 0)],
                            ))),
                        ),
                        i("attackerId", user_id as i64),
                        i("victimId", tid as i64),
                        f("victimHitpoints", n.hp.max(0.0)),
                        i("victimShield", 0),
                        i("victimNanoHull", 0),
                        i("damage", dmg as i64),
                        b("skilled", false),
                        b("strike", false),
                        i("cannonType", 0),
                    ],
                ));
                if n.hp <= 0.0 {
                    out.push(mk(
                        id::SHIP_DESTROYED,
                        vec![i("destroyedUserId", tid as i64), i("explosionTypeId", 0)],
                    ));
                    out.push(mk(id::SHIP_REMOVE, vec![i("userId", tid as i64)]));
                    let (x, y) = (n.x, n.y);
                    n.id = -n.id; // mark for respawn pass below
                    self.attack_target = None;
                    // Leave a loot box where the NPC died, then respawn
                    // the NPC elsewhere — keeps the world alive.
                    self.collect_counter += 1;
                    let drop_hash = format!("drop_{}_{}", tid, self.collect_counter);
                    self.collectables.push(Collectable {
                        hash: drop_hash.clone(),
                        kind: CollectKind::Box,
                        x,
                        y,
                        aux: 0,
                    });
                    out.push(mk(
                        id::ADD_BOX,
                        vec![
                            i("y", y as i64),
                            s("hash", drop_hash),
                            i("x", x as i64),
                            s("boxType", "FROM_SHIP"),
                        ],
                    ));
                    out.push(mk(
                        id::LEGACY,
                        vec![s("message", "0|y|EP|6000|6000".to_string())],
                    ));
                    out.push(mk(
                        id::LEGACY,
                        vec![s("message", "0|y|HON|500|500".to_string())],
                    ));
                }
            } else {
                self.attack_target = None;
            }
        }
        // Respawn destroyed NPCs at a fresh edge position.
        for idx in 0..self.npcs.len() {
            if self.npcs[idx].id < 0 {
                self.npcs[idx].id = -self.npcs[idx].id;
                self.npcs[idx].hp = self.npcs[idx].hp_max;
                self.npcs[idx].x = rng.range(300, self.width - 300);
                self.npcs[idx].y = rng.range(300, self.height - 300);
                self.npcs[idx].tx = rng.range(300, self.width - 300);
                self.npcs[idx].ty = rng.range(300, self.height - 300);
                let n = &self.npcs[idx];
                out.push(mk(
                    id::SHIP_CREATE,
                    vec![
                        i("userId", n.id as i64),
                        s("typeId", n.type_id.clone()),
                        i("expansionStage", 0),
                        s("clanTag", ""),
                        s("userName", n.name.clone()),
                        i("x", n.x as i64),
                        i("y", n.y as i64),
                        i("factionId", n.faction as i64),
                        i("clanId", 0),
                        i("dailyRank", 0),
                        b("warnBox", false),
                        null("clanDiplomacy"),
                        i("galaxyGatesDone", 0),
                        b("useSystemFont", false),
                        b("npc", n.npc),
                        b("cloaked", false),
                        i("motherShipId", 0),
                        i("positionIndex", 0),
                        s("designSet", ""),
                        list("modifier", vec![]),
                        null("minimapStrategyModule"),
                        s("specialNpcType", ""),
                    ],
                ));
            }
        }
        self.rng = rng;
        let _ = user_id;
        out
    }

    // --------------------------------------------------------------
    // client request handling — returns outbound packets

    fn handle(&mut self, p: &Packet, user_id: i32, session_id: &str) -> Vec<Packet> {
        let mut out = Vec::new();
        match p.id {
            id::MOVE_REQUEST => {
                let tx = p.int("targetX").unwrap_or(0) as i32;
                let ty = p.int("targetY").unwrap_or(0) as i32;
                self.hero_tx = tx;
                self.hero_ty = ty;
                self.hero_moving = true;
                out.push(mk(
                    id::HERO_MOVE,
                    vec![i("x", self.hero_x as i64), i("y", self.hero_y as i64)],
                ));
            }
            id::ATTACK_LASER_REQUEST => {
                let tid = p.int("targetId").unwrap_or(0) as i32;
                if self.npcs.iter().any(|n| n.id == tid) && !self.hero_dead {
                    self.attack_target = Some(tid);
                    out.push(mk(
                        id::ATTACK_LASER_RUN,
                        vec![
                            i("attackerId", user_id as i64),
                            i("targetId", tid as i64),
                            i("laserColor", 0),
                            b("isDiminishedBySkillShield", false),
                            b("skilledLaser", false),
                            i("visualType", 0),
                        ],
                    ));
                } else {
                    out.push(log_message("no such target"));
                }
            }
            id::ATTACK_ROCKET_REQUEST => {
                let tid = p.int("targetId").unwrap_or(0) as i32;
                if let Some(n) = self.npcs.iter_mut().find(|n| n.id == tid) {
                    n.hp -= 8_000.0;
                    out.push(mk(
                        id::HELLSTORM_ATTACK,
                        vec![
                            i("attackerId", user_id as i64),
                            i("targetId", tid as i64),
                            b("hit", true),
                            i("currentLoad", 1),
                            null("rocketType"),
                        ],
                    ));
                }
            }
            id::ATTACK_ABORT_LASER_REQUEST => {
                self.attack_target = None;
                out.push(mk(id::ATTACK_ABORT_LASER, vec![i("uid", user_id as i64)]));
            }
            id::COLLECT_BOX_REQUEST | id::HARVEST_REQUEST => {
                let hash = p.string("itemHash").unwrap_or_default().to_owned();
                if let Some(pos) = self.collectables.iter().position(|c| c.hash == hash) {
                    let c = self.collectables.remove(pos);
                    out.push(mk(
                        -6137, // RemoveCollectableCommand
                        vec![b("collected", true), s("hash", c.hash.clone())],
                    ));
                    match c.kind {
                        CollectKind::Box => {
                            self.credits += 1_000;
                            out.push(self.credits_packet());
                            out.push(mk(
                                id::LEGACY,
                                vec![s("message", format!("0|y|CRE|1000|{}", self.credits))],
                            ));
                        }
                        CollectKind::Ore => {
                            if let Some(slot) =
                                self.ores.iter_mut().find(|(t, _)| *t == c.aux as i64)
                            {
                                slot.1 += 10.0;
                            } else {
                                self.ores.push((c.aux as i64, 10.0));
                            }
                            out.push(self.ore_packet());
                        }
                    }
                } else {
                    out.push(log_message("nothing to collect"));
                }
            }
            id::JUMP_REQUEST => {
                // Jump if a portal is in range; otherwise deny.
                let near = self
                    .portals
                    .iter()
                    .find(|p| {
                        ((p.x - self.hero_x) as f64).hypot((p.y - self.hero_y) as f64) <= 1_000.0
                    })
                    .copied();
                match near {
                    Some(portal) => {
                        out.push(mk(
                            id::JUMP_INITIATED,
                            vec![
                                i("mapId", portal.target_map as i64),
                                i("gateId", portal.id as i64),
                            ],
                        ));
                        self.map_id = portal.target_map;
                        self.hero_x = self.width / 2;
                        self.hero_y = self.height / 2;
                        self.hero_moving = false;
                        self.attack_target = None;
                        self.populate_map();
                        out.push(mk(id::MAP_CHANGED, vec![i("newMapId", self.map_id as i64)]));
                        out.push(mk(
                            id::HERO_MOVE,
                            vec![i("x", self.hero_x as i64), i("y", self.hero_y as i64)],
                        ));
                        self.push_map_content(&mut out);
                    }
                    None => {
                        out.push(mk(id::JUMP_DENIED, vec![i("mandatoryLevel", 1)]));
                    }
                }
            }
            id::PET_REQUEST => {
                let on = p.int("petRequestType").unwrap_or(1) == 0;
                if on && self.pet_id.is_none() {
                    let pet_id = user_id.wrapping_add(9_000_000);
                    self.pet_id = Some(pet_id);
                    out.push(mk(
                        id::PET_HERO_ACTIVATION,
                        vec![
                            i("ownerId", user_id as i64),
                            i("petId", pet_id as i64),
                            i("shipType", 72),
                            i("expansionStage", 0),
                            s("petName", "PET-mock"),
                            i("factionId", 1),
                            i("clanId", 0),
                            i("petLevel", 5),
                            s("clanTag", ""),
                            i("x", (self.hero_x + 60) as i64),
                            i("y", (self.hero_y + 60) as i64),
                            i("petSpeed", 420),
                            null("minimapStrategyModule"),
                        ],
                    ));
                    out.push(mk(
                        id::PET_STATUS,
                        vec![
                            i("petId", pet_id as i64),
                            i("petLevel", 5),
                            f("petExperiencePoints", 1_000.0),
                            f("petExperiencePointsUntilNextLevel", 50_000.0),
                            i("petHitPoints", 50_000),
                            i("petHitPointsMax", 50_000),
                            i("petShieldEnergyNow", 5_000),
                            i("petShieldEnergyMax", 10_000),
                            i("petCurrentFuel", 90_000),
                            i("petMaxFuel", 100_000),
                            i("petSpeed", 420),
                            s("petName", "PET-mock"),
                            s("petLootId", "pet_pet10-5"),
                        ],
                    ));
                } else if !on && let Some(pet_id) = self.pet_id.take() {
                    out.push(mk(id::PET_DEACTIVATION, vec![i("petId", pet_id as i64)]));
                }
            }
            id::MENU_ACTION_REQUEST => {
                let item = p.string("menuItemId").unwrap_or_default().to_owned();
                let activate = p.int("actionType").unwrap_or(0) == 1;
                out.push(mk(
                    id::MENU_ITEM_STATUS,
                    vec![
                        b("visible", true),
                        b("selected", activate),
                        null("toolTipSlotBar"),
                        i("counterStyle", 0),
                        b("blocked", false),
                        f("counterValue", 500.0),
                        b("activatable", true),
                        s("menuItemId", item),
                        b("available", true),
                        null("toolTipItemBar"),
                        b("buyable", true),
                        f("maxCounterValue", 10_000.0),
                        s("iconLootId", "ammunition_laser_ucb-100"),
                    ],
                ));
            }
            id::KILL_SCREEN_REPAIR_REQUEST => {
                if self.hero_dead {
                    self.hero_dead = false;
                    self.hero_hp = self.hero_hp_max;
                    out.push(mk(id::MAP_CHANGED, vec![i("newMapId", self.map_id as i64)]));
                    self.push_map_content(&mut out);
                    out.push(self.credits_packet());
                    out.push(mk(
                        id::HERO_MOVE,
                        vec![i("x", self.hero_x as i64), i("y", self.hero_y as i64)],
                    ));
                    out.push(mk(
                        id::HITPOINT_INFO,
                        vec![
                            f("hitpoints", self.hero_hp),
                            f("hitpointsMax", self.hero_hp_max),
                            i("nanoHull", 256_000),
                            i("nanoHullMax", 256_000),
                        ],
                    ));
                }
            }
            id::TRADE_REQUEST => {
                out.push(mk(id::TRADE_REPLY, vec![list("priceInfos", vec![])]));
                out.push(mk(
                    id::ORE_TRADE_INFO,
                    vec![
                        i("sepromPrice", 1500),
                        i("terbiumPrice", 15),
                        i("duraniumPrice", 45),
                        i("palladiumExchangeRate", 15),
                        i("enduriumPrice", 10),
                        i("osmiumPrice", 60),
                        i("promeriumPrice", 75),
                        i("type", 1),
                        i("prometiumPrice", 5),
                        i("remainingCharges", -1),
                        i("prometidPrice", 30),
                    ],
                ));
            }
            id::TRADE_SELL_ORE_REQUEST => {
                if let Ok(m) = p.module("toSell")
                    && let Ok(t) = m.module("oreType").and_then(|o| o.int("typeValue"))
                    && let Ok(amount) = m.f64("count")
                {
                    if let Some(slot) = self.ores.iter_mut().find(|(ot, _)| *ot == t) {
                        slot.1 = (slot.1 - amount).max(0.0);
                        self.credits += (amount * 10.0) as i64;
                        out.push(self.ore_packet());
                        out.push(self.credits_packet());
                    }
                }
            }
            id::LAB_REFINEMENT_REQUEST => {
                if let Ok(m) = p.module("toProduce")
                    && let Ok(t) = m.module("oreType").and_then(|o| o.int("typeValue"))
                    && let Ok(amount) = m.f64("count")
                {
                    if let Some(slot) = self.ores.iter_mut().find(|(ot, _)| *ot == t) {
                        slot.1 += amount;
                    }
                    out.push(self.ore_packet());
                }
            }
            id::GALAXY_GATE_MULTI_ENERGY_REQUEST => {
                let gate = p.int("gateId").unwrap_or(1) as i32;
                let spins = p.int("spinAmount").unwrap_or(1);
                if let Some(g) = self.gates.iter_mut().find(|(id, _)| *id == gate) {
                    g.1 += spins as i32;
                } else {
                    self.gates.push((gate, spins as i32));
                }
                out.push(self.gate_info_packet());
            }
            id::LEGACY => {
                // `S|CFG|n|uid|sid` configuration switch — echo `0|S|CFG|n`.
                let msg = p.string("message").unwrap_or_default().to_owned();
                let fields: Vec<&str> = msg.split('|').collect();
                if fields.first() == Some(&"S")
                    && fields.get(1) == Some(&"CFG")
                    && let Some(Ok(n)) = fields.get(2).map(|v| v.parse::<i32>())
                {
                    self.configuration = n;
                    out.push(mk(id::LEGACY, vec![s("message", format!("0|S|CFG|{n}"))]));
                }
            }
            id::PING_POSITION_REQUEST => {
                let (x, y) = (p.int("x").unwrap_or(0), p.int("y").unwrap_or(0));
                out.push(mk(id::PING, vec![i("x", x), i("y", y)]));
            }
            // KeepAlive, ReadyRequest, ShipSelectRequest, ChannelClose,
            // StayinAlive and anything unknown: accepted silently.
            _ => {}
        }
        let _ = session_id;
        out
    }

    /// Forced kill via the control API.
    fn destroy_hero(&mut self, user_id: i32) -> Vec<Packet> {
        self.hero_dead = true;
        self.hero_hp = 0.0;
        let option = mk(
            id::KILL_SCREEN_OPTION_MODULE,
            vec![
                (
                    "repairType",
                    Value::Module(Box::new(mk(
                        id::KILL_SCREEN_OPTION_TYPE_MODULE,
                        vec![i("repairTypeValue", 1)],
                    ))),
                ),
                (
                    "price",
                    Value::Module(Box::new(mk(
                        13131, // PriceModule
                        vec![i("type", 0), i("amount", 0)],
                    ))),
                ),
                b("affordableForPlayer", true),
                i("cooldownTime", 0),
                null("descriptionKey"),
                null("descriptionKeyAddon"),
                null("toolTipKey"),
                null("buttonKey"),
            ],
        );
        vec![
            mk(
                id::SHIP_DESTROYED,
                vec![
                    i("destroyedUserId", user_id as i64),
                    i("explosionTypeId", 0),
                ],
            ),
            mk(
                id::KILL_SCREEN_POST,
                vec![
                    s("killerName", "mock-patrol"),
                    s("killerEpppLink", ""),
                    s("playerShipAlias", "ship_goliath"),
                    null("cause"),
                    list("options", vec![Value::Module(Box::new(option))]),
                ],
            ),
        ]
    }
}

fn move_command(n: &Npc) -> Packet {
    let dist = ((n.tx - n.x) as f64).hypot((n.ty - n.y) as f64);
    let ttt = if n.speed > 0 {
        (dist / n.speed as f64 * 100.0) as i64
    } else {
        0
    };
    mk(
        id::MOVE_COMMAND,
        vec![
            i("userId", n.id as i64),
            i("x", n.tx as i64),
            i("y", n.ty as i64),
            i("timeToTarget", ttt),
        ],
    )
}

/// `LogMessageCommand`: a localized-message module with a baseKey.
fn log_message(text: &str) -> Packet {
    mk(
        id::LOG_MESSAGE,
        vec![
            i("visibility", 0),
            i("style", 0),
            (
                "message",
                Value::Module(Box::new(mk(
                    -3325, // MessageKeyListWildcardModule
                    vec![
                        list("wildCardReplacements", vec![]),
                        s("baseKey", text),
                        null("baseKeyType"),
                        i("textStyle", 0),
                    ],
                ))),
            ),
        ],
    )
}

// ------------------------------------------------------------------
// connection task

/// Serve accepted connections until shutdown.
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
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            if let Err(e) = run_session(stream, peer, shared, shutdown).await {
                // io errors on a dead client are normal during tests.
                if e.kind() != io::ErrorKind::UnexpectedEof {
                    eprintln!("[game] {peer}: {e}");
                }
            }
        });
    }
}

/// Copy of [`crate::config::GameConfig`] the sim is built from — the
/// session reads it once so live `faults` edits don't re-shape a world.
fn game_cfg(shared: &Shared) -> crate::config::GameConfig {
    shared.cfg.game.clone()
}

/// One client connection: greeting → version → login → world loop.
async fn run_session(
    stream: TcpStream,
    peer: SocketAddr,
    shared: Arc<Shared>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> io::Result<()> {
    let (mut rd, mut wr) = stream.into_split();
    let mut decoder = FrameDecoder::new();
    let stats = Arc::new(SessionStats::default());
    let (cmd_tx, mut cmd_rx) = mpsc::channel::<SessionCmd>(8);
    let mut user_id: Option<i32> = None;
    let mut session_id = String::new();
    let mut world: Option<World> = None;
    let mut registered = false;
    let tick = Duration::from_millis(game_cfg(&shared).tick_ms.max(20));
    let mut interval = tokio::time::interval(tick);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Fault: drop the connection N ms after a successful login.
    let mut drop_at: Option<Instant> = None;
    let mut buf = [0u8; 8192];

    // First byte on the wire is the single 0x00 greeting — not a frame.
    let n = tokio::select! {
        r = rd.read(&mut buf[..1]) => r?,
        _ = shutdown.changed() => return Ok(()),
    };
    if n == 0 {
        return Ok(());
    }
    shared.event("connect", format!("peer={peer}"));

    let result = loop {
        let cmd_or_tick = tokio::select! {
            r = rd.read(&mut buf) => ReadOrCmd::Read(r),
            _ = interval.tick(), if world.is_some() => ReadOrCmd::Tick,
            c = cmd_rx.recv() => ReadOrCmd::Cmd(c),
            _ = async { if let Some(t) = drop_at { tokio::time::sleep_until(t.into()).await } else { std::future::pending::<()>().await } } => ReadOrCmd::DropFault,
            _ = shutdown.changed() => return Ok(()),
        };
        match cmd_or_tick {
            ReadOrCmd::Read(r) => {
                let n = r?;
                if n == 0 {
                    break Ok(());
                }
                decoder.push(&buf[..n]);
                while let Some(body) = decoder.next_body() {
                    stats.rx_packets.fetch_add(1, Ordering::Relaxed);
                    let packet = match decode_body(&body, false) {
                        Ok(p) => p,
                        Err(e) => {
                            stats.checksum_violations.fetch_add(1, Ordering::Relaxed);
                            shared.event("bad-packet", format!("peer={peer} err={e}"));
                            continue;
                        }
                    };
                    match packet.id {
                        id::VERSION_REQUEST => {
                            let client_v = packet.string("version").unwrap_or_default().to_owned();
                            let cfg = game_cfg(&shared);
                            let equal = !shared.faults().game.version_mismatch
                                && client_v == cfg.version_hash;
                            send(
                                &mut wr,
                                &shared,
                                &stats,
                                mk(
                                    id::VERSION_COMMAND,
                                    vec![s("version", cfg.version_hash.clone()), b("equal", equal)],
                                ),
                            )
                            .await?;
                            if !equal {
                                shared.event(
                                    "version-mismatch",
                                    format!("peer={peer} client={client_v}"),
                                );
                            }
                        }
                        id::LOGIN_REQUEST => {
                            let faults = shared.faults().game;
                            let uid = packet.int("userID").unwrap_or(0) as i32;
                            let sid = packet.string("sessionID").unwrap_or_default().to_owned();
                            // Fault: reject with a status code or a
                            // LoginErrorCommand outright.
                            if let Some(code) = faults.login_error_code {
                                send(
                                    &mut wr,
                                    &shared,
                                    &stats,
                                    mk(
                                        id::LOGIN_ERROR,
                                        vec![
                                            i("errorCode", code as i64),
                                            i("mapId", game_cfg(&shared).map_id as i64),
                                        ],
                                    ),
                                )
                                .await?;
                                continue;
                            }
                            let known = !game_cfg(&shared).strict_auth
                                || shared.sid_account(&sid).is_some_and(|s| s.user_id == uid);
                            let status = if !known {
                                STATUS_BAD_SESSION
                            } else {
                                faults.login_reject_status.unwrap_or(0) as i64
                            };
                            if status != 0 {
                                shared.game_logins_rejected.fetch_add(1, Ordering::Relaxed);
                                send(
                                    &mut wr,
                                    &shared,
                                    &stats,
                                    mk(id::LOGIN_RESPONSE, vec![i("status", status)]),
                                )
                                .await?;
                                continue;
                            }
                            // One session per account: kick the previous
                            // connection holding this user_id.
                            if let Some(old) = shared.sessions.lock().unwrap().remove(&uid) {
                                let _ = old.cmd.try_send(SessionCmd::Kick {
                                    reason: "second login on this account".into(),
                                });
                                shared.event("kick", format!("user={uid} replaced by {peer}"));
                            }
                            user_id = Some(uid);
                            session_id = sid;
                            let w = World::new(shared.cfg.seed, uid, &game_cfg(&shared));
                            send(
                                &mut wr,
                                &shared,
                                &stats,
                                mk(id::LOGIN_RESPONSE, vec![i("status", 0)]),
                            )
                            .await?;
                            let account = shared
                                .cfg
                                .backpage
                                .accounts
                                .iter()
                                .find(|a| a.user_id == uid);
                            let faction = account.map(|a| a.faction_id as i32).unwrap_or(1);
                            for p in
                                w.init_burst(uid, &session_id, faction, game_cfg(&shared).has_pet)
                            {
                                send(&mut wr, &shared, &stats, p).await?;
                            }
                            shared.sessions.lock().unwrap().insert(
                                uid,
                                GameSessionEntry {
                                    user_id: uid,
                                    session_id: session_id.clone(),
                                    peer,
                                    connected_at: Instant::now(),
                                    cmd: cmd_tx.clone(),
                                    stats: stats.clone(),
                                },
                            );
                            registered = true;
                            shared.game_logins.fetch_add(1, Ordering::Relaxed);
                            shared.event("game-login", format!("user={uid} peer={peer}"));
                            if let Some(ms) = shared.faults().game.drop_after_ms {
                                drop_at = Some(Instant::now() + Duration::from_millis(ms));
                            }
                            world = Some(w);
                        }
                        _ => {
                            if let (Some(w), Some(uid)) = (world.as_mut(), user_id) {
                                for p in w.handle(&packet, uid, &session_id) {
                                    send(&mut wr, &shared, &stats, p).await?;
                                }
                            }
                            // Packets before login are ignored — the real
                            // server does the same until authenticated.
                        }
                    }
                }
            }
            ReadOrCmd::Tick => {
                if let (Some(w), Some(uid)) = (world.as_mut(), user_id) {
                    for p in w.tick_packets(uid) {
                        send(&mut wr, &shared, &stats, p).await?;
                    }
                }
            }
            ReadOrCmd::Cmd(c) => match c {
                Some(SessionCmd::Kick { reason }) | Some(SessionCmd::Drop { reason }) => {
                    shared.event("session-drop", format!("peer={peer} reason={reason}"));
                    break Ok(());
                }
                Some(SessionCmd::KillHero) => {
                    if let (Some(w), Some(uid)) = (world.as_mut(), user_id) {
                        for p in w.destroy_hero(uid) {
                            send(&mut wr, &shared, &stats, p).await?;
                        }
                    }
                }
                None => {}
            },
            ReadOrCmd::DropFault => {
                shared.event("fault-drop", format!("peer={peer}"));
                break Ok(());
            }
        }
    };
    if registered && let Some(uid) = user_id {
        // Only remove if it's still our registration (a newer login may
        // have already replaced it while we drained).
        let mut sessions = shared.sessions.lock().unwrap();
        if sessions.get(&uid).is_some_and(|e| e.peer == peer) {
            sessions.remove(&uid);
        }
    }
    result
}

enum ReadOrCmd {
    Read(io::Result<usize>),
    Tick,
    Cmd(Option<SessionCmd>),
    DropFault,
}

/// Encode + write one framed packet, applying the latency fault.
async fn send(
    wr: &mut tokio::net::tcp::OwnedWriteHalf,
    shared: &Shared,
    stats: &SessionStats,
    packet: Packet,
) -> io::Result<()> {
    let body = encode_body(&packet)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
    let frame =
        frame_body(&body).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
    let latency = shared.faults().game.latency_ms;
    if latency > 0 {
        tokio::time::sleep(Duration::from_millis(latency)).await;
    }
    wr.write_all(&frame).await?;
    stats.tx_packets.fetch_add(1, Ordering::Relaxed);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use xenomit_packet_client::modules;

    fn world() -> World {
        let cfg = crate::config::GameConfig {
            listen: "127.0.0.1:0".into(),
            map_id: 16,
            map_width: 21_000,
            map_height: 13_100,
            tick_ms: 50,
            npc_count: 3,
            player_count: 0,
            box_count: 6,
            ore_count: 4,
            version_hash: "v".into(),
            strict_auth: true,
            has_pet: true,
        };
        World::new(42, 1001, &cfg)
    }

    #[test]
    fn world_is_deterministic() {
        let a = world();
        let b = world();
        let dump = |w: &World| {
            format!(
                "{:?}/{}/{:?}/{:?}/{:?}",
                (w.hero_x, w.hero_y),
                w.map_id,
                w.npcs.iter().map(|n| (n.id, n.x, n.y)).collect::<Vec<_>>(),
                w.collectables
                    .iter()
                    .map(|c| (c.hash.clone(), c.x, c.y))
                    .collect::<Vec<_>>(),
                w.portals.iter().map(|p| p.id).collect::<Vec<_>>(),
            )
        };
        assert_eq!(dump(&a), dump(&b));
    }

    #[test]
    fn init_burst_has_hero_and_world() {
        let w = world();
        let out = w.init_burst(1001, "sid", 1, true);
        let ids: Vec<i16> = out.iter().map(|p| p.id).collect();
        assert!(ids.contains(&id::SHIP_INITIALIZATION));
        assert!(ids.contains(&id::MAP_CHANGED));
        assert!(ids.contains(&id::MENU_INITIALIZATION));
        assert!(ids.contains(&id::GALAXY_GATE_INFO));
        assert!(ids.contains(&id::ORE_TRADE_INFO));
        assert!(ids.contains(&id::PET_INITIALIZATION));
        assert_eq!(ids.iter().filter(|i| **i == id::ADD_BOX).count(), 6);
        assert_eq!(ids.iter().filter(|i| **i == id::ADD_ORE).count(), 4);
        assert_eq!(ids.iter().filter(|i| **i == id::SHIP_CREATE).count(), 3);
    }

    #[test]
    fn move_request_positions_hero() {
        let mut w = world();
        let out = w.handle(
            &modules::move_request((w.hero_x, w.hero_y), (15_000, 8_000)),
            1001,
            "sid",
        );
        assert_eq!((w.hero_tx, w.hero_ty), (15_000, 8_000));
        assert!(w.hero_moving);
        assert!(out.iter().any(|p| p.id == id::HERO_MOVE));
        let mut arrived = false;
        for _ in 0..2000 {
            w.tick_packets(1001);
            if !w.hero_moving {
                arrived = true;
                break;
            }
        }
        assert!(arrived);
        assert_eq!((w.hero_x, w.hero_y), (15_000, 8_000));
    }

    #[test]
    fn laser_attack_damages_and_kills_npc() {
        let mut w = world();
        let (npc, pos) = (w.npcs[0].id, (w.npcs[0].x, w.npcs[0].y));
        let out = w.handle(&modules::attack_laser_request(npc, pos), 1001, "sid");
        assert!(out.iter().any(|p| p.id == id::ATTACK_LASER_RUN));
        let mut killed = false;
        for _ in 0..400 {
            for p in w.tick_packets(1001) {
                if p.id == id::SHIP_DESTROYED {
                    killed = true;
                }
            }
            if killed {
                break;
            }
        }
        assert!(killed);
        // The NPC respawns in place — the negative id marker is cleared.
        assert!(w.npcs.iter().all(|n| n.id > 0));
    }

    #[test]
    fn collect_box_grants_credits_and_updates() {
        let mut w = world();
        let before = w.credits;
        let (hash, x, y) = w
            .collectables
            .iter()
            .find(|c| matches!(c.kind, CollectKind::Box))
            .map(|c| (c.hash.clone(), c.x, c.y))
            .unwrap();
        let out = w.handle(
            &modules::collect_box_request(&hash, (x, y), (w.hero_x, w.hero_y)),
            1001,
            "sid",
        );
        assert!(w.credits > before);
        assert!(!w.collectables.iter().any(|c| c.hash == hash));
        let ids: Vec<i16> = out.iter().map(|p| p.id).collect();
        assert!(ids.contains(&-6137)); // RemoveCollectableCommand
        assert!(ids.contains(&id::ATTRIBUTE_CREDITS_UPDATE));
        assert!(ids.contains(&id::LEGACY)); // credits log line
    }
}
