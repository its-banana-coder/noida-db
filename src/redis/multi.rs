//! Transactions: MULTI, EXEC, DISCARD, WATCH and UNWATCH, ported from
//! Redis's multi.c.
//!
//! WATCH follows `touchWatchedKey`: every successful write command touches
//! the keys it names (from Redis's own key specs), which makes the
//! watching clients' EXEC fail. A watched key that expires counts as
//! changed too.

use super::command_meta;
use super::engine::{Command, Ctx, Engine, NUM_DBS, Reply, cmd, resolve};
use super::resp::Value;

pub static COMMANDS: &[Command] = &[
    cmd("multi", multi),
    cmd("exec", exec),
    cmd("discard", discard),
    cmd("watch", watch),
    cmd("unwatch", unwatch),
];

/// A key a client WATCHes.
pub struct Watched {
    db: usize,
    key: Vec<u8>,
    /// The key's expiry when WATCH ran.
    expires_at: Option<u64>,
    /// It had already expired then (Redis's `wk->expired`).
    expired: bool,
}

/// Commands that run right away inside MULTI instead of being queued.
pub fn runs_in_multi(name: &str) -> bool {
    matches!(name, "exec" | "discard" | "multi" | "watch" | "quit" | "reset")
}

const EXECABORT: &str = "EXECABORT Transaction discarded because of previous errors.";

impl Engine {
    /// A command that can't even be queued (unknown, wrong arity) flags the
    /// transaction; EXEC itself failing that way aborts it at once.
    pub(crate) fn multi_reject(&mut self, id: u64, name: &str, err: Value) -> Value {
        let Some(client) = self.clients.get_mut(&id) else { return err };
        if name == "exec" {
            client.multi = None;
            client.multi_error = false;
            client.dirty_cas = false;
            self.unwatch_all(id);
            let Value::Error(msg) = err else { return err };
            let msg = msg.strip_prefix("ERR ").unwrap_or(&msg).to_string();
            return Value::err(format!("EXECABORT Transaction discarded because of: {msg}"));
        }
        client.multi_error = true;
        err
    }

    /// Queues a command inside MULTI (`queueMultiCommand`).
    pub(crate) fn queue(&mut self, id: u64, name: &str, args: &[Vec<u8>]) -> Value {
        let client = self.clients.get_mut(&id).expect("registered");
        if command_meta::lookup(name).is_some_and(|m| m.has_flag("no_multi")) {
            client.multi_error = true;
            return Value::err("ERR Command not allowed inside a transaction");
        }
        if !client.multi_error && !client.dirty_cas {
            client.multi.get_or_insert_with(Vec::new).push(args.to_vec());
        }
        Value::Simple("QUEUED".into())
    }

    pub(crate) fn unwatch_all(&mut self, id: u64) {
        let Some(client) = self.clients.get_mut(&id) else { return };
        for w in std::mem::take(&mut client.watched) {
            let k = (w.db, w.key);
            if let Some(ids) = self.watchers.get_mut(&k) {
                ids.retain(|c| *c != id);
                if ids.is_empty() {
                    self.watchers.remove(&k);
                }
            }
        }
    }

    /// Marks the clients watching `key` in `db` as dirty.
    pub(crate) fn touch(&mut self, db: usize, key: &[u8]) {
        if self.watchers.is_empty() {
            return;
        }
        let Some(ids) = self.watchers.get(&(db, key.to_vec())).cloned() else { return };
        let now = self.now();
        for id in ids {
            let exists = self.dbs[db].contains(key, now);
            let Some(c) = self.clients.get_mut(&id) else { continue };
            // A key that was already expired when watched and is now gone
            // hasn't logically changed.
            let already_expired = c.watched.iter().any(|w| w.db == db && w.key == key && w.expired);
            if already_expired && !exists {
                continue;
            }
            c.dirty_cas = true;
        }
    }

    /// Before a database-wide write (FLUSHDB, FLUSHALL, SWAPDB): the watched
    /// keys it could affect, and whether each exists now.
    pub(crate) fn watch_snapshot(
        &mut self,
        args: &[Vec<u8>],
        db: usize,
    ) -> Vec<(usize, Vec<u8>, bool)> {
        if self.watchers.is_empty() {
            return vec![];
        }
        let name = String::from_utf8_lossy(&args[0]).to_ascii_lowercase();
        let dbs: Vec<usize> = match name.as_str() {
            "flushdb" => vec![db],
            "flushall" => (0..NUM_DBS).collect(),
            "swapdb" => args[1..]
                .iter()
                .filter_map(|a| std::str::from_utf8(a).ok()?.parse::<usize>().ok())
                .filter(|d| *d < NUM_DBS)
                .collect(),
            _ => return vec![],
        };
        let now = self.now();
        let keys: Vec<(usize, Vec<u8>)> =
            self.watchers.keys().filter(|(d, _)| dbs.contains(d)).cloned().collect();
        keys.into_iter()
            .map(|(d, k)| {
                let exists = self.dbs[d].contains(&k, now);
                (d, k, exists)
            })
            .collect()
    }

    /// After a successful command: touches what it wrote.
    pub(crate) fn touch_written(
        &mut self,
        args: &[Vec<u8>],
        db: usize,
        before: Vec<(usize, Vec<u8>, bool)>,
    ) {
        if self.watchers.is_empty() {
            return;
        }
        let now = self.now();
        for (d, k, existed) in before {
            if existed || self.dbs[d].contains(&k, now) {
                self.touch(d, &k);
            }
        }
        let name = String::from_utf8_lossy(&args[0]).to_ascii_lowercase();
        let Some(meta) = command_meta::lookup(&name) else { return };
        if !meta.has_flag("write") {
            return;
        }
        for (pos, _) in meta.keys(args) {
            self.touch(db, &args[pos]);
        }
        // Keys written in another database.
        let other_db = match name.as_str() {
            "move" => args.get(2),
            "copy" => args
                .iter()
                .position(|a| a.eq_ignore_ascii_case(b"db"))
                .and_then(|i| args.get(i + 1)),
            _ => None,
        };
        let key = if name == "move" { args.get(1) } else { args.get(2) };
        if let (Some(d), Some(key)) = (other_db, key)
            && let Some(d) = std::str::from_utf8(d).ok().and_then(|s| s.parse::<usize>().ok())
            && d < NUM_DBS
        {
            let key = key.clone();
            self.touch(d, &key);
        }
    }

    /// Whether a watched key expired since WATCH (`isWatchedKeyExpired`).
    fn watched_key_expired(&self, id: u64) -> bool {
        let now = self.now();
        self.clients[&id]
            .watched
            .iter()
            .any(|w| !w.expired && w.expires_at.is_some_and(|t| now > t))
    }
}

fn multi(ctx: &mut Ctx, _: &[Vec<u8>]) -> Reply {
    let c = ctx.client();
    if c.multi.is_some() {
        return Err(Value::err("ERR MULTI calls can not be nested"));
    }
    c.multi = Some(Vec::new());
    Ok(Value::ok())
}

fn discard(ctx: &mut Ctx, _: &[Vec<u8>]) -> Reply {
    let id = ctx.session.id;
    let c = ctx.client();
    if c.multi.take().is_none() {
        return Err(Value::err("ERR DISCARD without MULTI"));
    }
    c.multi_error = false;
    c.dirty_cas = false;
    ctx.engine.unwatch_all(id);
    Ok(Value::ok())
}

fn exec(ctx: &mut Ctx, _: &[Vec<u8>]) -> Reply {
    let id = ctx.session.id;
    let expired = ctx.engine.watched_key_expired(id);
    let c = ctx.client();
    let Some(queue) = c.multi.take() else {
        return Err(Value::err("ERR EXEC without MULTI"));
    };
    let failed = std::mem::take(&mut c.multi_error);
    let dirty = std::mem::take(&mut c.dirty_cas) || expired;
    ctx.engine.unwatch_all(id);
    if failed {
        return Err(Value::err(EXECABORT));
    }
    if dirty {
        return Ok(Value::NullArray);
    }
    let mut replies = Vec::with_capacity(queue.len());
    for args in queue {
        let reply = match resolve(&args) {
            Ok((handler, _)) => ctx.engine.call(ctx.session, handler, &args, true, None),
            Err(e) => e,
        };
        replies.push(reply);
    }
    Ok(Value::Array(replies))
}

fn watch(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let id = ctx.session.id;
    let db = ctx.db_index();
    let now = ctx.now;
    let c = ctx.client();
    if c.multi.is_some() {
        return Err(Value::err("ERR WATCH inside MULTI is not allowed"));
    }
    if c.dirty_cas {
        // No point watching more: EXEC will fail anyway.
        return Ok(Value::ok());
    }
    for key in &a[1..] {
        if ctx.client().watched.iter().any(|w| w.db == db && w.key == *key) {
            continue;
        }
        let expires_at = ctx.engine.dbs[db].raw_expiry(key);
        let expired = expires_at.is_some_and(|t| now > t);
        ctx.client().watched.push(Watched { db, key: key.clone(), expires_at, expired });
        ctx.engine.watchers.entry((db, key.clone())).or_default().push(id);
    }
    Ok(Value::ok())
}

fn unwatch(ctx: &mut Ctx, _: &[Vec<u8>]) -> Reply {
    let id = ctx.session.id;
    ctx.engine.unwatch_all(id);
    ctx.client().dirty_cas = false;
    Ok(Value::ok())
}
