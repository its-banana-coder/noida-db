//! The registry of services compiled into this binary.

use std::io;
use std::net::SocketAddr;
use std::path::Path;

/// Starts `name` on `addr` with on-disk persistence rooted at `data_dir`:
/// loads a snapshot from there on startup if one exists, and registers a
/// shutdown hook (via `crate::persistence::on_shutdown`) to save one back
/// on a clean exit (SIGINT/SIGTERM — see `crate::persistence`). A service
/// that hasn't wired persistence up yet falls back to `start`'s plain
/// ephemeral behavior, so this is always safe to call from `main`
/// regardless of how many services have been migrated.
// Each service's own persistence PR adds a real arm here (`"name" =>
// crate::<service>::spawn_persistent(addr, data_dir)`) as it lands.
pub fn start_persistent(name: &str, addr: &str, data_dir: &Path) -> Option<io::Result<SocketAddr>> {
    match name {
        #[cfg(feature = "mysql")]
        "mysql" => Some(crate::mysql::server::spawn_persistent(addr, data_dir)),
        #[cfg(feature = "postgres")]
        "postgres" => Some(crate::postgres::server::spawn_persistent(addr, data_dir)),
        #[cfg(feature = "elasticsearch")]
        "elasticsearch" => Some(crate::elasticsearch::spawn_persistent(addr, data_dir)),
        #[cfg(feature = "redis")]
        "redis" => Some(crate::redis::server::spawn_persistent(addr, data_dir)),
        #[cfg(feature = "kafka")]
        "kafka" => Some(crate::kafka::spawn_persistent(addr, data_dir)),
        _ => {
            let _ = data_dir;
            start(name, addr)
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
