//! `xenomit-mock-game` — a deterministic local stand-in for the real
//! DarkOrbit service: mock Backpage (HTTP login/session), mock game
//! server (packet protocol over TCP), CONNECT forward proxy, and a
//! localhost control API for runtime fault injection.
//!
//! ```no_run
//! # async fn demo() {
//! let server = xenomit_mock_game::MockServer::start(
//!     xenomit_mock_game::Config::from_json_str("{}").unwrap(),
//! ).await.unwrap();
//! println!("backpage: {}", server.backpage_url());
//! println!("game:     {}", server.game_addr());
//! # }
//! ```

pub mod backpage;
pub mod config;
pub mod control;
pub mod game;
pub mod http;
pub mod proxy;
pub mod rng;
pub mod state;

pub use config::Config;
pub use state::Shared;

use std::io;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

/// A running mock: all listeners bound, serve loops running. Drop /
/// [`MockServer::shutdown`] to stop.
pub struct MockServer {
    pub shared: Arc<Shared>,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    tasks: Vec<JoinHandle<io::Result<()>>>,
}

impl MockServer {
    /// Bind every enabled listener and spawn its serve loop. Bound
    /// addresses are readable immediately via `shared.addrs` or the
    /// accessors below.
    pub async fn start(cfg: Config) -> io::Result<Self> {
        let shared = Arc::new(Shared::new(cfg));
        Self::start_with_shared(shared).await
    }

    /// Same as [`Self::start`] but over a caller-built [`Shared`] — lets
    /// tests pre-seed sids or faults before listeners open.
    pub async fn start_with_shared(shared: Arc<Shared>) -> io::Result<Self> {
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let mut tasks = Vec::new();

        // Bind everything first so partial startup fails cleanly.
        let backpage_l = TcpListener::bind(&shared.cfg.backpage.listen).await?;
        let game_l = TcpListener::bind(&shared.cfg.game.listen).await?;
        let proxy_l = if shared.cfg.proxy.enabled {
            Some(TcpListener::bind(&shared.cfg.proxy.listen).await?)
        } else {
            None
        };
        let control_l = if shared.cfg.control.enabled {
            Some(TcpListener::bind(&shared.cfg.control.listen).await?)
        } else {
            None
        };
        {
            let mut addrs = shared.addrs.lock().unwrap();
            addrs.backpage = backpage_l.local_addr().ok();
            addrs.game = game_l.local_addr().ok();
            addrs.proxy = proxy_l.as_ref().and_then(|l| l.local_addr().ok());
            addrs.control = control_l.as_ref().and_then(|l| l.local_addr().ok());
        }

        tasks.push(tokio::spawn(backpage::serve(
            backpage_l,
            shared.clone(),
            shutdown_rx.clone(),
        )));
        tasks.push(tokio::spawn(game::serve(
            game_l,
            shared.clone(),
            shutdown_rx.clone(),
        )));
        if let Some(l) = proxy_l {
            tasks.push(tokio::spawn(proxy::serve(
                l,
                shared.clone(),
                shutdown_rx.clone(),
            )));
        }
        if let Some(l) = control_l {
            tasks.push(tokio::spawn(control::serve(
                l,
                shared.clone(),
                shutdown_tx.clone(),
                shutdown_rx.clone(),
            )));
        }
        Ok(Self {
            shared,
            shutdown_tx,
            tasks,
        })
    }

    /// `http://{addr}` of the mock Backpage — pass to
    /// `xenomit_backpage::login::login_with_credentials_at`.
    pub fn backpage_url(&self) -> String {
        let a = self.shared.addrs.lock().unwrap().backpage;
        format!("http://{}", a.expect("backpage listener bound"))
    }

    /// `host:port` of the mock game server — pass to
    /// `Session::connect`/`connect_and_login` or the runtime's
    /// `connect_with` `game_endpoint`.
    pub fn game_addr(&self) -> String {
        let a = self.shared.addrs.lock().unwrap().game;
        a.expect("game listener bound").to_string()
    }

    /// `host:port` of the forward proxy, if enabled.
    pub fn proxy_addr(&self) -> Option<String> {
        self.shared
            .addrs
            .lock()
            .unwrap()
            .proxy
            .map(|a| a.to_string())
    }

    /// `http://{addr}` of the control API, if enabled.
    pub fn control_url(&self) -> Option<String> {
        self.shared
            .addrs
            .lock()
            .unwrap()
            .control
            .map(|a| format!("http://{a}"))
    }

    /// Stop all listeners and connection tasks.
    pub async fn shutdown(mut self) {
        let _ = self.shutdown_tx.send(true);
        for t in std::mem::take(&mut self.tasks) {
            let _ = t.await;
        }
    }
}

impl Drop for MockServer {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(true);
    }
}
