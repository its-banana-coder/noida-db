//! Clients blocked on keys (BLPOP and friends), ported from Redis's
//! blocked.c.
//!
//! A blocking command that can't be served registers the client on its
//! keys. After every command the engine looks at the keys that were added
//! and, for each one with waiters, re-runs the waiters' commands in the order
//! they blocked (FIFO), exactly as Redis's `handleClientsBlockedOnKeys`
//! does. Their replies are stored until the connection thread picks them up.

use std::collections::hash_map::Entry as MapEntry;

use super::engine::{Ctx, Data, Engine, NUM_DBS, Reply, Session, resolve};
use super::longdouble;
use super::resp::Value;

/// What a client waits for: a key of this type to exist.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum BlockKind {
    List,
    Zset,
    Stream,
}

impl BlockKind {
    fn satisfied_by(self, data: &Data) -> bool {
        matches!((self, data), (BlockKind::List, Data::List(_)) | (BlockKind::Zset, Data::Zset(_)))
    }
}

/// A handler's request to block (see `Ctx::block`).
pub struct BlockRequest {
    kind: BlockKind,
    keys: Vec<Vec<u8>>,
    deadline: u64,
}

/// A blocked client's state.
pub struct BlockState {
    pub kind: BlockKind,
    pub keys: Vec<Vec<u8>>,
    pub db: usize,
    /// Unix ms after which the client times out; 0 waits forever.
    pub deadline: u64,
    /// The command, re-run when one of the keys is ready.
    pub args: Vec<Vec<u8>>,
}

impl Ctx<'_> {
    /// Blocks the client until one of `keys` holds a `kind` value or
    /// `deadline` passes (unix ms, 0 = never). A re-run command keeps the
    /// deadline it first blocked with.
    pub fn block(&mut self, kind: BlockKind, keys: &[Vec<u8>], deadline: u64) -> Reply {
        let deadline = self.reprocess_deadline.unwrap_or(deadline);
        self.block = Some(BlockRequest { kind, keys: keys.to_vec(), deadline });
        Ok(Value::NoReply)
    }
}

/// A timeout in seconds, possibly fractional, as an absolute unix ms time
/// (0 = none): `getTimeoutFromObjectOrReply(.., UNIT_SECONDS)`.
pub fn timeout_secs_arg(b: &[u8], now: u64) -> Result<u64, Value> {
    let not_float = || Value::err("ERR timeout is not a float or out of range");
    longdouble::parse(b).map_err(|_| not_float())?;
    let secs: f64 =
        std::str::from_utf8(b).ok().and_then(|s| s.parse().ok()).ok_or_else(not_float)?;
    let ms = (secs * 1000.0).ceil();
    if ms > i64::MAX as f64 {
        return Err(Value::err("ERR timeout is out of range"));
    }
    if ms < 0.0 {
        return Err(Value::err("ERR timeout is negative"));
    }
    let ms = ms as i64;
    if ms == 0 {
        return Ok(0);
    }
    ms.checked_add(now as i64)
        .map(|t| t as u64)
        .ok_or_else(|| Value::err("ERR timeout is out of range"))
}

impl Engine {
    pub(crate) fn block_client(
        &mut self,
        session: &mut Session,
        req: BlockRequest,
        args: &[Vec<u8>],
    ) {
        let id = session.id;
        let Some(client) = self.clients.get_mut(&id) else { return };
        let db = client.db;
        let mut keys: Vec<Vec<u8>> = Vec::new();
        for k in req.keys {
            if !keys.contains(&k) {
                keys.push(k);
            }
        }
        for k in &keys {
            self.waiting[db].entry(k.clone()).or_default().push_back(id);
        }
        client.blocked = Some(BlockState {
            kind: req.kind,
            keys,
            db,
            deadline: req.deadline,
            args: args.to_vec(),
        });
        session.blocked = true;
    }

    /// Stops a client waiting, forgetting it on every key.
    pub fn unblock(&mut self, id: u64) -> Option<BlockState> {
        let state = self.clients.get_mut(&id)?.blocked.take()?;
        for k in &state.keys {
            if let MapEntry::Occupied(mut e) = self.waiting[state.db].entry(k.clone()) {
                e.get_mut().retain(|c| *c != id);
                if e.get().is_empty() {
                    e.remove();
                }
            }
        }
        Some(state)
    }

    /// Unblocks a client with `reply`, as a timeout or CLIENT UNBLOCK does.
    pub fn unblock_with(&mut self, id: u64, reply: Value) -> bool {
        if self.unblock(id).is_none() {
            return false;
        }
        self.replies.insert(id, reply);
        true
    }

    /// The reply for a blocked session, once it has one.
    pub fn take_reply(&mut self, session: &mut Session) -> Option<Value> {
        let reply = self.replies.remove(&session.id)?;
        session.blocked = false;
        Some(reply)
    }

    /// Whether any unblocked client has a reply waiting to be sent.
    pub fn has_replies(&self) -> bool {
        !self.replies.is_empty()
    }

    pub fn has_reply_for(&self, id: u64) -> bool {
        self.replies.contains_key(&id)
    }

    /// When the client's wait ends by itself, if it is blocked.
    pub fn block_deadline(&self, id: u64) -> Option<u64> {
        self.clients.get(&id)?.blocked.as_ref().map(|b| b.deadline)
    }

    /// Times out blocked clients whose deadline has passed. Returns whether
    /// any did.
    pub fn expire_blocked(&mut self) -> bool {
        let now = self.now();
        let expired: Vec<u64> = self
            .clients
            .values()
            .filter(|c| c.blocked.as_ref().is_some_and(|b| b.deadline != 0 && b.deadline < now))
            .map(|c| c.id)
            .collect();
        for &id in &expired {
            self.unblock_with(id, Value::NullArray);
        }
        !expired.is_empty()
    }

    pub fn blocked_count(&self) -> usize {
        self.clients.values().filter(|c| c.blocked.is_some()).count()
    }

    /// Blocked clients with a timeout (INFO's `clients_in_timeout_table`).
    pub fn timeout_count(&self) -> usize {
        self.clients
            .values()
            .filter(|c| c.blocked.as_ref().is_some_and(|b| b.deadline != 0))
            .count()
    }

    /// Serves clients blocked on keys that were just added, FIFO per key,
    /// until no more keys become ready (a BLMOVE can feed another waiter).
    pub(crate) fn serve_blocked(&mut self) {
        loop {
            let mut ready: Vec<(usize, Vec<u8>)> = Vec::new();
            for db in 0..NUM_DBS {
                let added = std::mem::take(&mut self.dbs[db].added);
                if self.waiting[db].is_empty() {
                    continue;
                }
                for key in added {
                    if self.waiting[db].contains_key(&key)
                        && !ready.iter().any(|(d, k)| *d == db && *k == key)
                    {
                        ready.push((db, key));
                    }
                }
            }
            if ready.is_empty() {
                return;
            }
            for (db, key) in ready {
                self.serve_key(db, &key);
            }
        }
    }

    /// Marks keys as ready without adding them (SWAPDB, stream appends).
    pub(crate) fn signal_ready(&mut self, db: usize, key: &[u8]) {
        self.dbs[db].added.push(key.to_vec());
    }

    fn serve_key(&mut self, db: usize, key: &[u8]) {
        let waiters = self.waiting[db].get(key).map_or(0, |q| q.len());
        for _ in 0..waiters {
            let Some(queue) = self.waiting[db].get_mut(key) else { break };
            // Rotate first: if the client blocks again it keeps its turn
            // behind the others, as in Redis.
            let Some(id) = queue.pop_front() else { break };
            queue.push_back(id);
            let Some(kind) = self.clients.get(&id).and_then(|c| c.blocked.as_ref()).map(|b| b.kind)
            else {
                continue;
            };
            let now = self.now();
            if !self.dbs[db].get(key, now).is_some_and(|e| kind.satisfied_by(&e.data)) {
                continue;
            }
            let Some(state) = self.unblock(id) else { continue };
            let resp = self.clients[&id].resp;
            let mut session = Session { id, resp, closing: false, blocked: false };
            let Ok((handler, _)) = resolve(&state.args) else { continue };
            let reply = self.call(&mut session, handler, &state.args, false, Some(state.deadline));
            if !session.blocked {
                self.replies.insert(id, reply);
            }
        }
    }
}
