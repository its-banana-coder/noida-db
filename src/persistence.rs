//! Shared on-disk-persistence infrastructure: a process-wide shutdown-hook
//! registry, and the SIGINT/SIGTERM handler that runs it.
//!
//! Each persistent service (see `services::start_persistent`) registers a
//! hook via `on_shutdown` when it starts, which saves its own snapshot to
//! its own file under the process's data dir. `install_shutdown_handler`
//! (called once from `main`) is what actually catches SIGINT/SIGTERM and
//! runs every registered hook, in registration order, before exiting.
//!
//! Saves also happen while running: a service calls `mark` when it
//! changes data, and an autosave thread saves each marked service about a
//! second later (an idle server saves nothing). So a hard kill (`kill -9`,
//! a crash, closing WSL) loses at most the last second, and never corrupts
//! a snapshot, since `write_snapshot_atomically` only ever replaces a
//! snapshot file after the new one is fully written.

use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use std::sync::atomic::AtomicU64;

type Hook = Box<dyn Fn() + Send + Sync>;

/// The services that persist, each with a change counter.
const SERVICES: [&str; 5] = ["postgres", "mysql", "redis", "kafka", "elasticsearch"];
static CHANGES: [AtomicU64; 5] =
    [AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)];

/// Notes that `service`'s data changed, so the autosave writes it soon.
/// Cheap (one atomic add); calling it for a change that didn't happen only
/// costs a redundant save.
pub fn mark(service: &str) {
    if let Some(i) = SERVICES.iter().position(|s| *s == service) {
        CHANGES[i].fetch_add(1, Ordering::Relaxed);
    }
}

type Saver = (usize, Arc<dyn Fn() + Send + Sync>);

fn savers() -> &'static Mutex<Vec<Saver>> {
    static SAVERS: OnceLock<Mutex<Vec<Saver>>> = OnceLock::new();
    SAVERS.get_or_init(|| Mutex::new(Vec::new()))
}

/// Serializes saves: the autosave and the shutdown save write the same
/// files (through the same temp file).
fn save_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

/// Registers `save` for `service`: run by the autosave after `mark`, and
/// once more at shutdown.
pub fn on_save(service: &'static str, save: impl Fn() + Send + Sync + 'static) {
    let save: Arc<dyn Fn() + Send + Sync> = Arc::new(save);
    let shutdown = save.clone();
    on_shutdown(move || shutdown());
    if let Some(i) = SERVICES.iter().position(|s| *s == service) {
        savers().lock().unwrap().push((i, save));
    }
}

/// Saves each marked service once a second (started by
/// `install_shutdown_handler`).
fn start_autosave() {
    std::thread::Builder::new()
        .name("autosave".into())
        .spawn(|| {
            let mut saved = [0u64; 5];
            loop {
                std::thread::sleep(std::time::Duration::from_secs(1));
                let list: Vec<Saver> = savers().lock().unwrap().clone();
                for (i, save) in list {
                    let now = CHANGES[i].load(Ordering::Relaxed);
                    if now != saved[i] {
                        // Read before saving: a change during the save
                        // marks again and is saved next round.
                        saved[i] = now;
                        let _g = save_lock().lock().unwrap();
                        save();
                    }
                }
            }
        })
        .expect("spawn autosave thread");
}

fn hooks() -> &'static Mutex<Vec<Hook>> {
    static HOOKS: OnceLock<Mutex<Vec<Hook>>> = OnceLock::new();
    HOOKS.get_or_init(|| Mutex::new(Vec::new()))
}

/// Registers `hook` to run once, in registration order, when the process
/// catches SIGINT/SIGTERM via `install_shutdown_handler`. Each persistent
/// service's `spawn_persistent` calls this once at startup to save its
/// snapshot before the process actually exits.
pub fn on_shutdown(hook: impl Fn() + Send + Sync + 'static) {
    hooks().lock().unwrap().push(Box::new(hook));
}

/// Installs a SIGINT/SIGTERM handler that runs every hook registered via
/// `on_shutdown` (in registration order) and then exits the process with
/// status 0. Call once from `main`, before starting any service.
///
/// A `kill -9`/SIGKILL can't be caught by any process, by design -- that's
/// the "hard kill loses the since-last-save window" half of this
/// project's persistence model, not a gap in this handler.
pub fn install_shutdown_handler() {
    let term = Arc::new(AtomicBool::new(false));
    // SIGINT (Ctrl-C) and SIGTERM (`kill`, `docker stop`, ...) both just
    // flip a flag here -- a signal handler has to stay async-signal-safe
    // (no locks, no allocation), so the actual save work happens on the
    // watcher thread below, never in the handler itself.
    for sig in [signal_hook::consts::SIGINT, signal_hook::consts::SIGTERM] {
        let _ = signal_hook::flag::register(sig, term.clone());
    }
    start_autosave();
    std::thread::spawn(move || {
        loop {
            if term.load(Ordering::Relaxed) {
                let _g = save_lock().lock().unwrap();
                for hook in hooks().lock().unwrap().iter() {
                    hook();
                }
                std::process::exit(0);
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
    });
}

/// Writes `bytes` to `path` by writing to a sibling temp file first and
/// renaming it into place, so a save interrupted mid-write (a hard kill
/// during the save itself) never leaves a half-written, corrupt snapshot
/// behind -- the rename is atomic, so a reader only ever sees the old file
/// or the fully-written new one, never a partial one. Every service's own
/// snapshot-save function should go through this rather than writing
/// `path` directly.
pub fn write_snapshot_atomically(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, bytes)?;
    std::fs::rename(&tmp, path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn atomic_write_creates_file_with_contents() {
        let dir = std::env::temp_dir().join(format!("noida-persist-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("snapshot.json");
        write_snapshot_atomically(&path, b"{\"a\":1}").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"{\"a\":1}");
        // No leftover temp file.
        assert!(!path.with_extension("tmp").exists());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn atomic_write_replaces_existing_file() {
        let dir = std::env::temp_dir().join(format!("noida-persist-test2-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("snapshot.json");
        write_snapshot_atomically(&path, b"old").unwrap();
        write_snapshot_atomically(&path, b"new").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"new");
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
