//! Shared on-disk-persistence infrastructure: a process-wide shutdown-hook
//! registry, and the SIGINT/SIGTERM handler that runs it.
//!
//! Each persistent service (see `services::start_persistent`) registers a
//! hook via `on_shutdown` when it starts, which saves its own snapshot to
//! its own file under the process's data dir. `install_shutdown_handler`
//! (called once from `main`) is what actually catches SIGINT/SIGTERM and
//! runs every registered hook, in registration order, before exiting.
//!
//! Deliberately simple over robust: no periodic autosave, only a
//! save-on-clean-shutdown -- a hard kill (`kill -9`, a crash) loses
//! whatever changed since the last clean shutdown, but never corrupts the
//! snapshot, since `write_snapshot_atomically` only ever replaces a
//! snapshot file after the new one is fully written.

use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

type Hook = Box<dyn Fn() + Send + Sync>;

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

/// Runs all registered hooks immediately. For testing only.
pub fn run_hooks_for_test() {
    for hook in hooks().lock().unwrap().iter() {
        hook();
    }
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
    std::thread::spawn(move || {
        loop {
            if term.load(Ordering::Relaxed) {
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
