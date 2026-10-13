//! The registry of services compiled into this binary, and the running
//! server's control over them: each one can be started, stopped,
//! restarted and reset while the process keeps running (see
//! `crate::lifecycle` and the control API in `crate::control`).

use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use serde_json::{Value, json};

use crate::config::{Config, DEFAULT_SERVICES};
use crate::lifecycle::Lifecycle;

/// Starts `name` on `addr` with on-disk persistence rooted at `data_dir`:
/// loads its snapshot from there if one exists, and registers its saver
/// with `crate::persistence` (autosave, and a save on a clean exit).
/// Services without persistence (ClickHouse, MongoDB) start empty. `None`
/// if the service isn't built into this binary.
pub fn start_persistent(name: &str, addr: &str, data_dir: &Path) -> Option<io::Result<SocketAddr>> {
    start_until_stopped(name, addr, data_dir, &Lifecycle::default())
}

/// Like `start_persistent`, until `life` is stopped.
pub fn start_until_stopped(
    name: &str,
    addr: &str,
    data_dir: &Path,
    life: &Lifecycle,
) -> Option<io::Result<SocketAddr>> {
    match name {
        #[cfg(feature = "mysql")]
        "mysql" => Some(crate::mysql::server::spawn_persistent(addr, data_dir, life)),
        #[cfg(feature = "postgres")]
        "postgres" => Some(crate::postgres::server::spawn_persistent(addr, data_dir, life)),
        #[cfg(feature = "elasticsearch")]
        "elasticsearch" => Some(crate::elasticsearch::spawn_persistent(addr, data_dir, life)),
        #[cfg(feature = "redis")]
        "redis" => Some(crate::redis::server::spawn_persistent(addr, data_dir, life)),
        #[cfg(feature = "kafka")]
        "kafka" => Some(crate::kafka::spawn_persistent(addr, data_dir, life)),
        #[cfg(feature = "mongodb")]
        "mongodb" => Some(crate::mongodb::spawn_with(addr, life)),
        #[cfg(feature = "clickhouse")]
        "clickhouse" => Some(crate::clickhouse::spawn_with(addr, life)),
        _ => {
            let _ = (addr, data_dir, life);
            None
        }
    }
}

/// Starts `name` on `addr`. `None` if the service isn't built into this
/// binary (its Cargo feature is off, or it isn't implemented yet).
pub fn start(name: &str, addr: &str) -> Option<io::Result<SocketAddr>> {
    match name {
        #[cfg(feature = "mysql")]
        "mysql" => Some(crate::mysql::server::spawn(addr)),
        #[cfg(feature = "redis")]
        "redis" => Some(crate::redis::server::spawn(addr)),
        #[cfg(feature = "postgres")]
        "postgres" => Some(crate::postgres::spawn(addr)),
        #[cfg(feature = "kafka")]
        "kafka" => Some(crate::kafka::spawn(addr)),
        #[cfg(feature = "mongodb")]
        "mongodb" => Some(crate::mongodb::spawn(addr)),
        #[cfg(feature = "elasticsearch")]
        "elasticsearch" => Some(crate::elasticsearch::spawn(addr)),
        #[cfg(feature = "clickhouse")]
        "clickhouse" => Some(crate::clickhouse::spawn(addr)),
        _ => {
            let _ = addr;
            None
        }
    }
}

/// Whether `name` is compiled into this binary.
pub fn available(name: &str) -> bool {
    (name == "postgres" && cfg!(feature = "postgres"))
        || (name == "mysql" && cfg!(feature = "mysql"))
        || (name == "redis" && cfg!(feature = "redis"))
        || (name == "kafka" && cfg!(feature = "kafka"))
        || (name == "elasticsearch" && cfg!(feature = "elasticsearch"))
        || (name == "clickhouse" && cfg!(feature = "clickhouse"))
        || (name == "mongodb" && cfg!(feature = "mongodb"))
}

/// The files under the data dir that hold `name`'s saved data.
pub fn snapshot_files(name: &str) -> &'static [&'static str] {
    match name {
        "postgres" => &["postgres.json"],
        "mysql" => &["mysql.json"],
        "redis" => &["redis.json", "redis-functions.json"],
        "kafka" => &["kafka.json"],
        "elasticsearch" => &["elasticsearch.json"],
        _ => &[],
    }
}

/// Deletes `name`'s saved data. Missing files are fine.
pub fn delete_snapshots(name: &str, data_dir: &Path) -> io::Result<()> {
    for file in snapshot_files(name) {
        match std::fs::remove_file(data_dir.join(file)) {
            Err(e) if e.kind() != io::ErrorKind::NotFound => return Err(e),
            _ => {}
        }
    }
    Ok(())
}

/// The connection string a client pastes for `name` at `host:port`.
pub fn connect_string(name: &str, host: &str, port: u16) -> String {
    let host = if host.contains(':') { format!("[{host}]") } else { host.to_string() };
    match name {
        "postgres" => format!("postgres://postgres@{host}:{port}/postgres"),
        "mysql" => format!("mysql://root@{host}:{port}"),
        "redis" => format!("redis://{host}:{port}"),
        "elasticsearch" | "clickhouse" => format!("http://{host}:{port}"),
        "mongodb" => format!("mongodb://{host}:{port}"),
        _ => format!("{host}:{port}"),
    }
}

/// One service as the control API reports it.
#[derive(Debug, Clone, PartialEq)]
pub struct ServiceStatus {
    pub name: &'static str,
    /// "running", "stopped" or "unavailable" (not in this build).
    pub state: &'static str,
    pub host: String,
    pub port: u16,
    pub data_bytes: Option<u64>,
    pub error: Option<String>,
}

impl ServiceStatus {
    pub fn to_json(&self) -> Value {
        json!({
            "name": self.name,
            "state": self.state,
            "host": self.host,
            "port": self.port,
            "connect": connect_string(self.name, &self.host, self.port),
            "data_bytes": self.data_bytes,
            "error": self.error,
        })
    }
}

struct Slot {
    name: &'static str,
    /// The port to listen on: the configured one, then the one actually
    /// bound (so a `--<service>-port 0` keeps its port across restarts).
    port: u16,
    running: Option<Lifecycle>,
    error: Option<String>,
}

struct Manager {
    host: String,
    data_dir: PathBuf,
    slots: Vec<Slot>,
}

static MANAGER: Mutex<Option<Manager>> = Mutex::new(None);

/// Sets up the running server's services from `cfg` (all of them, so one
/// left out by `--only` can still be started later); starts none.
pub fn configure(cfg: &Config) {
    let slots = DEFAULT_SERVICES
        .iter()
        .map(|d| Slot {
            name: d.name,
            port: cfg.services.iter().find(|s| s.name == d.name).map_or(d.port, |s| s.port),
            running: None,
            error: None,
        })
        .collect();
    *lock() = Some(Manager { host: cfg.host.clone(), data_dir: cfg.data_dir.clone(), slots });
}

fn lock() -> std::sync::MutexGuard<'static, Option<Manager>> {
    MANAGER.lock().unwrap_or_else(|e| e.into_inner())
}

/// Every service's status, in the contract's fixed order.
pub fn status() -> Vec<ServiceStatus> {
    let guard = lock();
    let Some(m) = guard.as_ref() else { return Vec::new() };
    m.slots.iter().map(|s| m.status(s)).collect()
}

/// What the control API answers: the service as it is now, and whether the
/// action worked (or why not).
pub type Outcome = (ServiceStatus, Result<(), String>);

/// Starts `name` (a no-op if it's running).
pub fn start_service(name: &str) -> Outcome {
    with_slot(name, |m, i| m.start(i))
}

/// Stops `name`: closes its port and connections, saves its data and
/// drops it from memory (a no-op if it's stopped).
pub fn stop_service(name: &str) -> Outcome {
    with_slot(name, |m, i| {
        m.stop(i, true);
        Ok(())
    })
}

pub fn restart_service(name: &str) -> Outcome {
    with_slot(name, |m, i| {
        m.stop(i, true);
        m.start(i)
    })
}

/// Wipes `name`'s saved and in-memory data; a running service is started
/// again, empty.
pub fn reset_service(name: &str) -> Outcome {
    with_slot(name, |m, i| {
        let was_running = m.slots[i].running.is_some();
        m.stop(i, false);
        delete_snapshots(m.slots[i].name, &m.data_dir).map_err(|e| e.to_string())?;
        if was_running { m.start(i) } else { Ok(()) }
    })
}

fn with_slot(
    name: &str,
    action: impl FnOnce(&mut Manager, usize) -> Result<(), String>,
) -> Outcome {
    let mut guard = lock();
    let Some(m) = guard.as_mut() else {
        return (unavailable(name), Err("no server is running in this process".into()));
    };
    let Some(i) = m.slots.iter().position(|s| s.name == name) else {
        return (unavailable(name), Err(format!("unknown service '{name}'")));
    };
    if !available(name) {
        let msg = format!("{name} is not in this build of noida-db");
        return (m.status(&m.slots[i]), Err(msg));
    }
    let result = action(m, i);
    (m.status(&m.slots[i]), result)
}

fn unavailable(name: &str) -> ServiceStatus {
    ServiceStatus {
        name: DEFAULT_SERVICES.iter().find(|s| s.name == name).map_or("unknown", |s| s.name),
        state: "unavailable",
        host: "127.0.0.1".into(),
        port: 0,
        data_bytes: None,
        error: None,
    }
}

impl Manager {
    fn start(&mut self, i: usize) -> Result<(), String> {
        if self.slots[i].running.is_some() {
            return Ok(());
        }
        let life = Lifecycle::default();
        let slot = &self.slots[i];
        let addr = format!("{}:{}", self.host, slot.port);
        let result = start_until_stopped(slot.name, &addr, &self.data_dir, &life);
        let slot = &mut self.slots[i];
        match result {
            Some(Ok(bound)) => {
                slot.port = bound.port();
                slot.running = Some(life);
                slot.error = None;
                Ok(())
            }
            Some(Err(e)) => {
                let msg = listen_error(&e, &addr);
                slot.error = Some(msg.clone());
                Err(msg)
            }
            None => Err(format!("{} is not in this build of noida-db", slot.name)),
        }
    }

    /// Stops slot `i`, saving its data first when `save` (a reset doesn't).
    fn stop(&mut self, i: usize, save: bool) {
        let slot = &mut self.slots[i];
        if let Some(life) = slot.running.take() {
            life.stop();
            // No client is left, so this is the final state; after this
            // no autosave or shutdown save touches its files again.
            crate::persistence::forget(slot.name, save);
        }
    }

    fn status(&self, slot: &Slot) -> ServiceStatus {
        let state = if !available(slot.name) {
            "unavailable"
        } else if slot.running.is_some() {
            "running"
        } else {
            "stopped"
        };
        let mut data_bytes = None;
        for file in snapshot_files(slot.name) {
            if let Ok(meta) = std::fs::metadata(self.data_dir.join(file)) {
                *data_bytes.get_or_insert(0) += meta.len();
            }
        }
        ServiceStatus {
            name: slot.name,
            state,
            host: client_host(&self.host),
            port: slot.port,
            data_bytes,
            error: slot.error.clone(),
        }
    }
}

/// The address a client on this machine connects to for a server bound to
/// `host` (a wildcard bind is reached through loopback).
pub fn client_host(host: &str) -> String {
    match host {
        "0.0.0.0" | "" => "127.0.0.1".into(),
        "::" | "[::]" => "::1".into(),
        h => h.trim_start_matches('[').trim_end_matches(']').to_string(),
    }
}

/// A start failure in words, e.g. "port 6379 is already in use".
pub fn listen_error(e: &io::Error, addr: &str) -> String {
    if e.kind() == io::ErrorKind::AddrInUse {
        let port = addr.rsplit(':').next().unwrap_or(addr);
        format!("port {port} is already in use")
    } else {
        format!("cannot listen on {addr}: {e}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connect_strings_follow_the_contract() {
        assert_eq!(
            connect_string("postgres", "127.0.0.1", 5432),
            "postgres://postgres@127.0.0.1:5432/postgres"
        );
        assert_eq!(connect_string("mysql", "127.0.0.1", 3306), "mysql://root@127.0.0.1:3306");
        assert_eq!(connect_string("redis", "127.0.0.1", 6379), "redis://127.0.0.1:6379");
        assert_eq!(connect_string("kafka", "127.0.0.1", 9092), "127.0.0.1:9092");
        assert_eq!(connect_string("elasticsearch", "127.0.0.1", 9200), "http://127.0.0.1:9200");
        assert_eq!(connect_string("mongodb", "::1", 27017), "mongodb://[::1]:27017");
        assert_eq!(client_host("0.0.0.0"), "127.0.0.1");
    }

    /// One RESP command and its reply, over a fresh connection.
    #[cfg(feature = "redis")]
    fn redis(port: u16, words: &[&str]) -> io::Result<String> {
        use std::io::{Read, Write};
        let mut s = std::net::TcpStream::connect(("127.0.0.1", port))?;
        let mut req = format!("*{}\r\n", words.len());
        for w in words {
            req.push_str(&format!("${}\r\n{w}\r\n", w.len()));
        }
        s.write_all(req.as_bytes())?;
        let mut buf = [0u8; 256];
        let n = s.read(&mut buf)?;
        Ok(String::from_utf8_lossy(&buf[..n]).into_owned())
    }

    #[cfg(feature = "redis")]
    fn ok((status, result): Outcome) -> ServiceStatus {
        result.unwrap();
        status
    }

    #[cfg(feature = "redis")]
    #[test]
    fn stop_start_and_reset_keep_or_wipe_data() {
        let dir = std::env::temp_dir().join(format!("noida-services-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let cfg = Config {
            data_dir: dir.clone(),
            services: vec![crate::config::Service { name: "redis", port: 0 }],
            ..Config::default()
        };
        configure(&cfg);
        let st = |name: &str| status().into_iter().find(|s| s.name == name).unwrap();
        assert_eq!(st("redis").state, "stopped");

        let port = ok(start_service("redis")).port;
        assert_eq!(st("redis").state, "running");
        assert_eq!(redis(port, &["SET", "k", "v"]).unwrap(), "+OK\r\n");
        // A client still connected when the service stops is disconnected.
        let mut idle = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();

        let stopped = ok(stop_service("redis"));
        assert_eq!(stopped.state, "stopped");
        assert!(stopped.data_bytes.unwrap() > 0);
        assert!(redis(port, &["PING"]).is_err());
        let mut buf = [0u8; 8];
        assert_eq!(std::io::Read::read(&mut idle, &mut buf).unwrap_or(0), 0);

        assert_eq!(ok(start_service("redis")).port, port);
        assert_eq!(redis(port, &["GET", "k"]).unwrap(), "$1\r\nv\r\n");
        assert_eq!(ok(restart_service("redis")).state, "running");
        assert_eq!(redis(port, &["GET", "k"]).unwrap(), "$1\r\nv\r\n");

        let reset = ok(reset_service("redis"));
        assert_eq!((reset.state, reset.data_bytes), ("running", None));
        assert_eq!(redis(port, &["GET", "k"]).unwrap(), "$-1\r\n");

        // Reset leaves a stopped service stopped.
        redis(port, &["SET", "k", "v2"]).unwrap();
        ok(stop_service("redis"));
        assert_eq!(ok(reset_service("redis")).state, "stopped");
        assert!(!dir.join("redis.json").exists());

        assert!(start_service("oracle").1.is_err());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn in_use_ports_are_reported_plainly() {
        let e = io::Error::from(io::ErrorKind::AddrInUse);
        assert_eq!(listen_error(&e, "127.0.0.1:6379"), "port 6379 is already in use");
    }
}
