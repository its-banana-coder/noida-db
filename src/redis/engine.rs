//! The Redis keyspace and command dispatch.

use std::collections::HashMap;
use std::sync::Arc;

use super::resp::Value;
use super::{connection, keys, strings};

/// Milliseconds since the Unix epoch. Injected so tests control time.
pub type Clock = Arc<dyn Fn() -> u64 + Send + Sync>;

pub const NUM_DBS: usize = 16;

#[derive(Clone, Debug)]
pub enum Data {
    Str(Vec<u8>),
}

impl Data {
    pub fn type_name(&self) -> &'static str {
        match self {
            Data::Str(_) => "string",
        }
    }
}

#[derive(Clone, Debug)]
pub struct Entry {
    pub data: Data,
    /// Absolute expiry in unix milliseconds.
    pub expires_at: Option<u64>,
}

impl Entry {
    pub fn new(data: Data) -> Entry {
        Entry { data, expires_at: None }
    }

    fn is_expired(&self, now: u64) -> bool {
        // Redis keeps a key alive until the clock passes its expiry.
        self.expires_at.is_some_and(|at| now > at)
    }
}

/// One of the 16 logical databases. Expired keys are removed lazily on
/// access and periodically by the server.
#[derive(Default)]
pub struct Db {
    map: HashMap<Vec<u8>, Entry>,
}

impl Db {
    pub fn get(&mut self, key: &[u8], now: u64) -> Option<&mut Entry> {
        if self.map.get(key).is_some_and(|e| e.is_expired(now)) {
            self.map.remove(key);
        }
        self.map.get_mut(key)
    }

    pub fn contains(&mut self, key: &[u8], now: u64) -> bool {
        self.get(key, now).is_some()
    }

    pub fn remove(&mut self, key: &[u8], now: u64) -> Option<Entry> {
        self.map.remove(key).filter(|e| !e.is_expired(now))
    }

    pub fn insert(&mut self, key: Vec<u8>, entry: Entry) {
        self.map.insert(key, entry);
    }

    pub fn purge_expired(&mut self, now: u64) {
        self.map.retain(|_, e| !e.is_expired(now));
    }

    pub fn len(&mut self, now: u64) -> usize {
        self.purge_expired(now);
        self.map.len()
    }

    /// Live keys in a stable (sorted) order, which SCAN cursors rely on.
    pub fn keys(&mut self, now: u64) -> Vec<Vec<u8>> {
        self.purge_expired(now);
        let mut keys: Vec<_> = self.map.keys().cloned().collect();
        keys.sort();
        keys
    }

    pub fn clear(&mut self) {
        self.map.clear();
    }
}

pub struct Session {
    pub id: u64,
    pub db: usize,
    pub name: Option<Vec<u8>>,
    /// Set by QUIT: the server closes the connection after replying.
    pub closing: bool,
}

impl Session {
    pub fn new(id: u64) -> Session {
        Session { id, db: 0, name: None, closing: false }
    }
}

pub struct Engine {
    pub dbs: Vec<Db>,
    clock: Clock,
    rng: u64,
}

pub type Reply = Result<Value, Value>;
pub type Handler = fn(&mut Ctx, &[Vec<u8>]) -> Reply;

pub struct Command {
    pub name: &'static str,
    /// Redis arity: positive means exact, negative means at least |n|.
    /// Includes the command name itself.
    pub arity: i32,
    pub handler: Handler,
}

fn command_table() -> impl Iterator<Item = &'static Command> {
    connection::COMMANDS.iter().chain(keys::COMMANDS).chain(strings::COMMANDS)
}

/// Names of every command noida implements.
pub fn command_names() -> impl Iterator<Item = &'static str> {
    command_table().map(|c| c.name)
}

/// Whether noida implements a (lowercase) top-level command.
pub fn is_implemented(name: &str) -> bool {
    command_table().any(|c| c.name == name)
}

/// Everything a command handler can touch.
pub struct Ctx<'a> {
    pub engine: &'a mut Engine,
    pub session: &'a mut Session,
    pub now: u64,
}

impl Ctx<'_> {
    pub fn db(&mut self) -> &mut Db {
        &mut self.engine.dbs[self.session.db]
    }

    pub fn lookup(&mut self, key: &[u8]) -> Option<&mut Entry> {
        let now = self.now;
        self.db().get(key, now)
    }

    /// The string at `key`, `None` if missing, WRONGTYPE for other types.
    pub fn get_str(&mut self, key: &[u8]) -> Result<Option<&mut Vec<u8>>, Value> {
        match self.lookup(key) {
            None => Ok(None),
            Some(Entry { data: Data::Str(s), .. }) => Ok(Some(s)),
            #[allow(unreachable_patterns)]
            Some(_) => Err(wrong_type()),
        }
    }

    pub fn random(&mut self) -> u64 {
        // xorshift64: plenty for RANDOMKEY and friends.
        let mut x = self.engine.rng;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.engine.rng = x;
        x
    }
}

impl Default for Engine {
    fn default() -> Self {
        Self::new()
    }
}

impl Engine {
    pub fn new() -> Engine {
        Engine::with_clock(Arc::new(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0)
        }))
    }

    pub fn with_clock(clock: Clock) -> Engine {
        let seed = clock() | 1;
        Engine { dbs: (0..NUM_DBS).map(|_| Db::default()).collect(), clock, rng: seed }
    }

    pub fn now(&self) -> u64 {
        (self.clock)()
    }

    pub fn purge_expired(&mut self) {
        let now = self.now();
        for db in &mut self.dbs {
            db.purge_expired(now);
        }
    }

    pub fn execute(&mut self, session: &mut Session, args: &[Vec<u8>]) -> Value {
        let name = String::from_utf8_lossy(&args[0]).to_ascii_lowercase();
        let Some(cmd) = command_table().find(|c| c.name == name) else {
            return unknown_command(args);
        };
        let n = args.len() as i32;
        if (cmd.arity > 0 && n != cmd.arity) || n < -cmd.arity {
            return arity_error(cmd.name);
        }
        let now = self.now();
        let mut ctx = Ctx { engine: self, session, now };
        match (cmd.handler)(&mut ctx, args) {
            Ok(v) | Err(v) => v,
        }
    }
}

fn unknown_command(args: &[Vec<u8>]) -> Value {
    let mut shown = String::new();
    for arg in &args[1..] {
        if shown.len() >= 128 {
            break;
        }
        let room = 128 - shown.len();
        let arg = String::from_utf8_lossy(arg);
        let arg: String = arg.chars().take(room).collect();
        shown.push_str(&format!("'{arg}' "));
    }
    let name: String = String::from_utf8_lossy(&args[0]).chars().take(128).collect();
    let msg = format!("ERR unknown command '{name}', with args beginning with: {shown}");
    // Redis never lets newlines into an error line.
    Value::Error(msg.replace(['\r', '\n'], " "))
}

// ---- shared error replies ----

pub fn arity_error(name: &str) -> Value {
    Value::err(format!("ERR wrong number of arguments for '{name}' command"))
}

pub fn wrong_type() -> Value {
    Value::err("WRONGTYPE Operation against a key holding the wrong kind of value")
}

pub fn not_int() -> Value {
    Value::err("ERR value is not an integer or out of range")
}

pub fn syntax() -> Value {
    Value::err("ERR syntax error")
}

pub fn invalid_expire(cmd: &str) -> Value {
    Value::err(format!("ERR invalid expire time in '{cmd}' command"))
}

pub fn db_out_of_range() -> Value {
    Value::err("ERR DB index is out of range")
}

pub fn same_object() -> Value {
    Value::err("ERR source and destination objects are the same")
}

/// Parses an integer argument or fails with Redis's standard error.
pub fn int_arg(b: &[u8]) -> Result<i64, Value> {
    super::num::parse_int(b).ok_or_else(not_int)
}

/// Parses a DB index the way `getIntFromObjectOrReply` + `selectDb` do.
pub fn db_arg(b: &[u8]) -> Result<usize, Value> {
    let n = int_arg(b)?;
    if n < i32::MIN as i64 || n > i32::MAX as i64 {
        return Err(not_int());
    }
    if n < 0 || n >= NUM_DBS as i64 {
        return Err(db_out_of_range());
    }
    Ok(n as usize)
}

pub fn eq_ic(a: &[u8], b: &str) -> bool {
    a.eq_ignore_ascii_case(b.as_bytes())
}
