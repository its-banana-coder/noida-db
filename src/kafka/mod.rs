//! Kafka compatibility: native Kafka binary protocol on port 9092.

pub mod codec;
pub mod connection;
pub mod engine;
pub mod log;

#[cfg(test)]
mod tests;

use engine::{Engine, Snapshot};
use std::io;
use std::net::{SocketAddr, TcpListener};
use std::path::Path;
use std::thread;

/// Binds `addr` and serves Kafka on background threads.
pub fn spawn(addr: &str) -> io::Result<SocketAddr> {
    let listener = TcpListener::bind(addr)?;
    let local_addr = listener.local_addr()?;

    let engine = Engine::new(local_addr.ip().to_string(), local_addr.port() as i32);
    engine.spawn_log_cleaner();

    thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let engine = engine.clone();
            thread::spawn(move || {
                connection::handle_connection(stream, engine);
            });
        }
    });

    Ok(local_addr)
}

/// Binds `addr`, loads a snapshot from `data_dir/kafka.json` if one exists,
/// and registers a shutdown hook to save on a clean exit.
pub fn spawn_persistent(addr: &str, data_dir: &Path) -> io::Result<SocketAddr> {
    let (addr, save) = spawn_persistent_for_test(addr, data_dir)?;
    crate::persistence::on_save("kafka", save);
    Ok(addr)
}

/// Like `spawn_persistent`, but also returns the save closure so tests can
/// trigger a save directly without going through the process-wide shutdown hook.
pub fn spawn_persistent_for_test(
    addr: &str,
    data_dir: &Path,
) -> io::Result<(SocketAddr, impl Fn() + Send + Sync + 'static)> {
    let listener = TcpListener::bind(addr)?;
    let local_addr = listener.local_addr()?;

    let path = data_dir.join("kafka.json");
    let engine = if path.exists() {
        let bytes = std::fs::read(&path)?;
        let snapshot: Snapshot = serde_json::from_slice(&bytes).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("failed to parse kafka snapshot: {e}"),
            )
        })?;
        Engine::new_persistent(snapshot, local_addr.ip().to_string(), local_addr.port() as i32)
    } else {
        Engine::new(local_addr.ip().to_string(), local_addr.port() as i32)
    };

    engine.spawn_log_cleaner();
    let save_engine = engine.clone();
    let save = move || {
        let snapshot = save_engine.snapshot();
        let bytes = serde_json::to_vec(&snapshot).expect("kafka snapshot serialization failed");
        let _ = crate::persistence::write_snapshot_atomically(&path, &bytes);
    };

    thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let engine = engine.clone();
            thread::spawn(move || {
                connection::handle_connection(stream, engine);
            });
        }
    });

    Ok((local_addr, save))
}
