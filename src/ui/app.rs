//! UI state, key handling and the (blocking) calls to the control API.

use super::api::{Client, Event, Service, Status};
use super::http::ApiError;
use ratatui::crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use std::collections::VecDeque;
use std::path::PathBuf;

const MAX_EVENTS: usize = 1000;

#[derive(Debug, Clone, PartialEq)]
pub enum Mode {
    Normal,
    /// Waiting for `y` to confirm a destructive action.
    Confirm(Action),
    /// No live server in the data dir; offering to start one.
    NoServer,
}

/// Work that talks to the server; run after a frame showing "busy" was drawn.
#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    Start(String),
    Stop(String),
    Reset(String),
    Shutdown,
    Launch,
}

impl Action {
    pub fn busy_text(&self) -> String {
        match self {
            Action::Start(n) => format!("starting {n}..."),
            Action::Stop(n) => format!("stopping {n}..."),
            Action::Reset(n) => format!("resetting {n}..."),
            Action::Shutdown => "shutting down the server...".into(),
            Action::Launch => "starting noida-db...".into(),
        }
    }
}

pub struct App {
    pub data_dir: PathBuf,
    pub client: Option<Client>,
    pub status: Option<Status>,
    pub events: VecDeque<Event>,
    pub since: u64,
    pub selected: usize,
    pub mode: Mode,
    /// Activity pane filter: only this service's events.
    pub filter: Option<String>,
    /// Last result line; true = error.
    pub message: Option<(String, bool)>,
    pub busy: Option<String>,
    pub connected: bool,
    pub quit: bool,
    /// Text the event loop should copy to the clipboard through OSC 52.
    pub clipboard: Option<String>,
}

impl App {
    pub fn new(data_dir: PathBuf) -> App {
        App {
            data_dir,
            client: None,
            status: None,
            events: VecDeque::new(),
            since: 0,
            selected: 0,
            mode: Mode::NoServer,
            filter: None,
            message: None,
            busy: None,
            connected: false,
            quit: false,
            clipboard: None,
        }
    }

    pub fn services(&self) -> &[Service] {
        self.status.as_ref().map(|s| s.services.as_slice()).unwrap_or(&[])
    }

    pub fn selected_service(&self) -> Option<&Service> {
        self.services().get(self.selected)
    }

    /// Look for a live server at startup; fall back to the "start it?" offer.
    pub fn connect_initial(&mut self) {
        self.mode = Mode::NoServer;
        if self.try_attach() {
            self.mode = Mode::Normal;
            self.tick();
        }
    }

    fn try_attach(&mut self) -> bool {
        let Some(c) = Client::from_data_dir(&self.data_dir) else { return false };
        match c.status() {
            Ok(st) => {
                self.apply_status(st);
                self.client = Some(c);
                self.connected = true;
                true
            }
            Err(_) => false,
        }
    }

    fn apply_status(&mut self, st: Status) {
        self.status = Some(st);
        let n = self.services().len();
        if self.selected >= n {
            self.selected = n.saturating_sub(1);
        }
    }

    /// Poll status and new activity; on failure mark disconnected and re-discover.
    pub fn tick(&mut self) {
        if self.mode == Mode::NoServer {
            return;
        }
        let Some(c) = self.client.clone() else {
            self.connected = false;
            self.try_attach();
            return;
        };
        let res = c.status().and_then(|st| {
            let act = c.activity(self.since)?;
            Ok((st, act))
        });
        match res {
            Ok((st, act)) => {
                self.connected = true;
                self.apply_status(st);
                if act.next < self.since {
                    // The server restarted and its sequence started over.
                    self.events.clear();
                }
                self.since = act.next;
                for e in act.events {
                    self.events.push_back(e);
                }
                while self.events.len() > MAX_EVENTS {
                    self.events.pop_front();
                }
            }
            Err(_) => {
                self.connected = false;
                // The server may have come back on a new port/token.
                self.client = None;
                if self.try_attach() {
                    self.since = 0;
                }
            }
        }
    }

    pub fn handle_key(&mut self, key: KeyEvent) -> Option<Action> {
        if key.kind == KeyEventKind::Release {
            return None;
        }
        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
            self.quit = true;
            return None;
        }
        match self.mode.clone() {
            Mode::NoServer => match key.code {
                KeyCode::Char('y') | KeyCode::Char('Y') => Some(Action::Launch),
                KeyCode::Char('n') | KeyCode::Char('N') | KeyCode::Char('q') | KeyCode::Esc => {
                    self.quit = true;
                    None
                }
                _ => None,
            },
            Mode::Confirm(action) => {
                self.mode = Mode::Normal;
                match key.code {
                    KeyCode::Char('y') | KeyCode::Char('Y') => Some(action),
                    _ => {
                        self.message = Some(("cancelled".into(), false));
                        None
                    }
                }
            }
            Mode::Normal => self.normal_key(key.code),
        }
    }

    fn normal_key(&mut self, code: KeyCode) -> Option<Action> {
        let n = self.services().len();
        match code {
            KeyCode::Char('q') | KeyCode::Esc => self.quit = true,
            KeyCode::Up | KeyCode::Char('k') => self.selected = self.selected.saturating_sub(1),
            KeyCode::Down | KeyCode::Char('j') => {
                if self.selected + 1 < n {
                    self.selected += 1;
                }
            }
            KeyCode::Char(' ') | KeyCode::Enter => {
                let svc = self.selected_service()?;
                let name = svc.name.clone();
                match svc.state.as_str() {
                    "running" => return Some(Action::Stop(name)),
                    "stopped" => return Some(Action::Start(name)),
                    _ => {
                        self.message =
                            Some((format!("{name} is not available in this build"), true))
                    }
                }
            }
            KeyCode::Char('c') => {
                if let Some(s) = self.selected_service().filter(|s| !s.connect.is_empty()) {
                    let text = s.connect.clone();
                    self.message = Some((format!("copied {text}"), false));
                    self.clipboard = Some(text);
                }
            }
            KeyCode::Char('r') => {
                if let Some(s) = self.selected_service().filter(|s| s.state != "unavailable") {
                    self.mode = Mode::Confirm(Action::Reset(s.name.clone()));
                }
            }
            KeyCode::Char('s') if self.connected => self.mode = Mode::Confirm(Action::Shutdown),
            KeyCode::Char('f') => {
                self.filter = match (&self.filter, self.selected_service()) {
                    (None, Some(s)) => Some(s.name.clone()),
                    _ => None,
                };
            }
            _ => {}
        }
        None
    }

    pub fn perform(&mut self, action: Action) {
        if action == Action::Launch {
            return self.launch();
        }
        let Some(c) = self.client.clone() else {
            self.message = Some(("disconnected: server not reachable".into(), true));
            return;
        };
        let (res, done) = match &action {
            Action::Start(n) => (c.service_action(n, "start").map(Some), format!("{n} started")),
            Action::Stop(n) => (c.service_action(n, "stop").map(Some), format!("{n} stopped")),
            Action::Reset(n) => (c.service_action(n, "reset").map(Some), format!("{n} data reset")),
            _ => (c.shutdown().map(|_| None), "server stopped".into()),
        };
        match res {
            Ok(svc) => {
                if let (Some(svc), Some(st)) = (svc, self.status.as_mut())
                    && let Some(slot) = st.services.iter_mut().find(|s| s.name == svc.name)
                {
                    let err = svc.error.clone();
                    *slot = svc;
                    if let Some(e) = err {
                        self.message = Some((format!("{done}, but: {e}"), true));
                        return;
                    }
                }
                self.message = Some((done, false));
                if action == Action::Shutdown {
                    self.connected = false;
                }
            }
            Err(ApiError::Down(m)) => {
                self.connected = false;
                self.message = Some((format!("disconnected: {m}"), true));
            }
            Err(e) => self.message = Some((e.to_string(), true)),
        }
    }

    /// Spawn `noida-db up --data-dir <dir>` as a child and wait for it to publish its control file.
    fn launch(&mut self) {
        let spawned = std::env::current_exe().and_then(|exe| {
            let mut cmd = std::process::Command::new(exe);
            cmd.arg("up")
                .arg("--data-dir")
                .arg(&self.data_dir)
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null());
            #[cfg(unix)]
            {
                use std::os::unix::process::CommandExt;
                cmd.process_group(0);
            }
            cmd.spawn()
        });
        let mut child = match spawned {
            Ok(c) => c,
            Err(e) => {
                self.message = Some((format!("cannot start noida-db: {e}"), true));
                return;
            }
        };
        for _ in 0..50 {
            if self.try_attach() {
                self.mode = Mode::Normal;
                self.since = 0;
                self.message = Some(("noida-db started".into(), false));
                self.tick();
                return;
            }
            if let Ok(Some(st)) = child.try_wait() {
                self.message = Some((format!("noida-db exited at startup ({st})"), true));
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        self.message = Some(("noida-db did not come up in 5s".into(), true));
    }
}
