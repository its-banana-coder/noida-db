//! The Redis keyspace, connected clients, and command dispatch.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;

use super::blocking::{BlockRequest, BlockState};
use super::command_meta::{self, CommandMeta};
use super::ordered::OrderedMap;
use super::resp::Value;
use super::{
    admin, bitops, config, connection, geo, hashes, keys, lists, multi, pubsub, sets, strings,
    zsets,
};

/// Milliseconds since the Unix epoch. Injected so tests control time.
pub type Clock = Arc<dyn Fn() -> u64 + Send + Sync>;

pub const NUM_DBS: usize = 16;

#[derive(Clone, Debug)]
pub enum Data {
    Str(Vec<u8>),
    Hash(Hash),
    List(VecDeque<Vec<u8>>),
    Set(super::sets::Set),
    Zset(super::zsets::Zset),
}

impl Data {
    pub fn type_name(&self) -> &'static str {
        match self {
            Data::Str(_) => "string",
            Data::Hash(_) => "hash",
            Data::List(_) => "list",
            Data::Set(_) => "set",
            Data::Zset(_) => "zset",
        }
    }
}

/// What the compact encodings hold before Redis converts them, from the
/// matching `*-max-listpack-*` / `set-max-intset-entries` parameters.
#[derive(Clone, Copy)]
pub struct Limits {
    pub entries: usize,
    pub value: usize,
    pub intset: usize,
}

#[derive(Clone, Debug, Default)]
pub struct Hash {
    pub map: OrderedMap,
    /// Converted to Redis's hashtable encoding; like Redis, never goes back.
    pub big: bool,
}

impl Hash {
    pub fn insert(&mut self, k: &[u8], v: &[u8], lim: Limits) -> bool {
        if k.len() > lim.value || v.len() > lim.value {
            self.big = true;
        }
        let new = self.map.insert(k.to_vec(), v.to_vec());
        if self.map.len() > lim.entries {
            self.big = true;
        }
        new
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
    /// Keys added since the engine last looked, in order: they may wake
    /// blocked clients (Redis's `signalKeyAsReady` from `dbAdd`).
    pub(crate) added: Vec<Vec<u8>>,
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
        self.added.push(key.clone());
        self.map.insert(key, entry);
    }

    /// The key's expiry even if it has passed (the key isn't removed).
    pub fn raw_expiry(&self, key: &[u8]) -> Option<u64> {
        self.map.get(key).and_then(|e| e.expires_at)
    }

    pub fn purge_expired(&mut self, now: u64) {
        self.map.retain(|_, e| !e.is_expired(now));
    }

    pub fn len(&mut self, now: u64) -> usize {
        self.purge_expired(now);
        self.map.len()
    }

    /// (keys, keys with an expiry), for INFO keyspace.
    pub fn counts(&mut self, now: u64) -> (usize, usize) {
        self.purge_expired(now);
        (self.map.len(), self.map.values().filter(|e| e.expires_at.is_some()).count())
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

/// How a server connection presents itself to the engine.
pub struct ClientConn {
    pub addr: String,
    pub laddr: String,
    pub fd: i64,
    /// Closes the connection from another thread (CLIENT KILL).
    pub kill: Option<Box<dyn Fn() + Send>>,
    /// Queues bytes on the connection's output, in order with its replies
    /// (pub/sub messages).
    pub push: Option<Box<dyn Fn(Vec<u8>) + Send>>,
}

/// Per-connection state that other clients can see (CLIENT LIST).
pub struct Client {
    pub id: u64,
    pub conn: ClientConn,
    pub name: Option<Vec<u8>>,
    pub lib_name: Option<Vec<u8>>,
    pub lib_ver: Option<Vec<u8>>,
    pub db: usize,
    pub resp: u8,
    pub created: u64,
    pub last_interaction: u64,
    pub last_cmd: Option<String>,
    pub no_evict: bool,
    pub no_touch: bool,
    pub reply_off: bool,
    reply_skip: bool,
    pub reply_skip_next: bool,
    /// Waiting in a blocking command (BLPOP and friends).
    pub blocked: Option<BlockState>,
    /// Commands queued since MULTI.
    pub multi: Option<Vec<Vec<Vec<u8>>>>,
    /// A command failed to queue: EXEC will abort (CLIENT_DIRTY_EXEC).
    pub multi_error: bool,
    /// A watched key changed: EXEC will fail (CLIENT_DIRTY_CAS).
    pub dirty_cas: bool,
    pub watched: Vec<super::multi::Watched>,
    pub subs: super::pubsub::Subscriptions,
    /// Pushes for a connection without a socket (tests).
    pub pushed: Vec<Value>,
}

/// The connection's handle, owned by its thread.
pub struct Session {
    pub id: u64,
    /// RESP version for encoding replies.
    pub resp: u8,
    /// The server closes the connection after sending the current reply.
    pub closing: bool,
    /// The last command blocked: the reply comes later, from `take_reply`.
    pub blocked: bool,
}

pub struct Pause {
    /// Unix ms at which the pause ends.
    pub until: u64,
    /// Pause everything, not just writes.
    pub all: bool,
}

pub struct Engine {
    pub dbs: Vec<Db>,
    pub clients: BTreeMap<u64, Client>,
    pub pause: Option<Pause>,
    pub started: u64,
    /// Unix seconds of the last SAVE/BGSAVE (LASTSAVE).
    pub last_save: u64,
    /// Per database: clients blocked on each key, in the order they blocked.
    pub waiting: Vec<HashMap<Vec<u8>, VecDeque<u64>>>,
    /// Replies for clients that were unblocked by other clients' commands.
    pub(crate) replies: HashMap<u64, Value>,
    /// Clients WATCHing each (db, key).
    pub(crate) watchers: HashMap<(usize, Vec<u8>), Vec<u64>>,
    pub(crate) pubsub: super::pubsub::PubSub,
    pub(crate) config: super::config::Config,
    next_client_id: u64,
    clock: Clock,
    rng: u64,
}

pub type Reply = Result<Value, Value>;
pub type Handler = fn(&mut Ctx, &[Vec<u8>]) -> Reply;

/// A command noida implements. Arity and every other property come from
/// Redis's own command table (`command_meta`).
pub struct Command {
    pub name: &'static str,
    pub handler: Handler,
    pub subs: &'static [Command],
}

pub const fn cmd(name: &'static str, handler: Handler) -> Command {
    Command { name, handler, subs: &[] }
}

/// A command with subcommands, like CLIENT. `handler` runs when it is
/// called without one (only COMMAND allows that).
pub const fn container(name: &'static str, handler: Handler, subs: &'static [Command]) -> Command {
    Command { name, handler, subs }
}

fn command_table() -> impl Iterator<Item = &'static Command> {
    connection::COMMANDS
        .iter()
        .chain(admin::COMMANDS)
        .chain(keys::COMMANDS)
        .chain(strings::COMMANDS)
        .chain(hashes::COMMANDS)
        .chain(lists::COMMANDS)
        .chain(sets::COMMANDS)
        .chain(zsets::COMMANDS)
        .chain(multi::COMMANDS)
        .chain(pubsub::COMMANDS)
        .chain(config::COMMANDS)
        .chain(bitops::COMMANDS)
        .chain(geo::COMMANDS)
}

fn find(name: &str) -> Option<&'static Command> {
    static INDEX: std::sync::OnceLock<HashMap<&'static str, &'static Command>> =
        std::sync::OnceLock::new();
    INDEX.get_or_init(|| command_table().map(|c| (c.name, c)).collect()).get(name).copied()
}

/// Names of every top-level command noida implements.
pub fn command_names() -> impl Iterator<Item = &'static str> {
    command_table().map(|c| c.name)
}

/// Whether noida implements a command, by full name ("get",
/// "client|setname").
pub fn is_implemented(fullname: &str) -> bool {
    match fullname.split_once('|') {
        None => find(fullname).is_some(),
        Some((parent, sub)) => find(parent).is_some_and(|c| c.subs.iter().any(|s| s.name == sub)),
    }
}

/// Everything a command handler can touch.
pub struct Ctx<'a> {
    pub engine: &'a mut Engine,
    pub session: &'a mut Session,
    pub now: u64,
    /// Blocking commands must answer right away (inside MULTI, scripts).
    pub deny_blocking: bool,
    /// Running inside EXEC.
    pub in_exec: bool,
    /// Set by `Ctx::block`: the command waits for keys.
    pub(crate) block: Option<BlockRequest>,
    /// When re-running a blocked command, its original deadline.
    pub(crate) reprocess_deadline: Option<u64>,
}

impl Ctx<'_> {
    /// The encoding limits configured for `kind` ("hash", "set", "zset").
    pub fn limits(&self, kind: &str) -> Limits {
        let num = |name: String| self.engine.config_num(&name).max(0) as usize;
        Limits {
            entries: num(format!("{kind}-max-listpack-entries")),
            value: num(format!("{kind}-max-listpack-value")),
            intset: num("set-max-intset-entries".into()),
        }
    }

    pub fn client(&mut self) -> &mut Client {
        self.engine.clients.get_mut(&self.session.id).expect("client is registered")
    }

    pub fn db_index(&self) -> usize {
        self.engine.clients[&self.session.id].db
    }

    pub fn db(&mut self) -> &mut Db {
        let i = self.db_index();
        &mut self.engine.dbs[i]
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
            Some(_) => Err(wrong_type()),
        }
    }

    /// The hash at `key`, `None` if missing, WRONGTYPE for other types.
    pub fn get_hash(&mut self, key: &[u8]) -> Result<Option<&mut Hash>, Value> {
        match self.lookup(key) {
            None => Ok(None),
            Some(Entry { data: Data::Hash(h), .. }) => Ok(Some(h)),
            Some(_) => Err(wrong_type()),
        }
    }

    /// The hash at `key`, created empty if missing.
    pub fn hash_or_create(&mut self, key: &[u8]) -> Result<&mut Hash, Value> {
        if self.get_hash(key)?.is_none() {
            self.db().insert(key.to_vec(), Entry::new(Data::Hash(Hash::default())));
        }
        Ok(self.get_hash(key)?.expect("just created"))
    }

    /// The list at `key`, `None` if missing, WRONGTYPE for other types.
    pub fn get_list(&mut self, key: &[u8]) -> Result<Option<&mut VecDeque<Vec<u8>>>, Value> {
        match self.lookup(key) {
            None => Ok(None),
            Some(Entry { data: Data::List(l), .. }) => Ok(Some(l)),
            Some(_) => Err(wrong_type()),
        }
    }

    /// The list at `key`, created empty if missing.
    pub fn list_or_create(&mut self, key: &[u8]) -> Result<&mut VecDeque<Vec<u8>>, Value> {
        if self.get_list(key)?.is_none() {
            self.db().insert(key.to_vec(), Entry::new(Data::List(VecDeque::new())));
        }
        Ok(self.get_list(key)?.expect("just created"))
    }

    /// Deletes `key` if its collection became empty, as Redis does.
    pub fn drop_if_empty(&mut self, key: &[u8]) {
        let empty = match self.lookup(key) {
            Some(Entry { data: Data::Hash(h), .. }) => h.map.is_empty(),
            Some(Entry { data: Data::List(l), .. }) => l.is_empty(),
            Some(Entry { data: Data::Set(s), .. }) => s.is_empty(),
            Some(Entry { data: Data::Zset(z), .. }) => z.is_empty(),
            _ => false,
        };
        if empty {
            let now = self.now;
            self.db().remove(key, now);
        }
    }

    /// The client's RESP version (some replies differ between 2 and 3).
    pub fn resp(&self) -> u8 {
        self.engine.clients[&self.session.id].resp
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
        let now = clock();
        Engine {
            dbs: (0..NUM_DBS).map(|_| Db::default()).collect(),
            clients: BTreeMap::new(),
            pause: None,
            started: now,
            last_save: now / 1000,
            waiting: (0..NUM_DBS).map(|_| HashMap::new()).collect(),
            replies: HashMap::new(),
            watchers: HashMap::new(),
            pubsub: Default::default(),
            config: Default::default(),
            next_client_id: 1,
            clock,
            rng: now | 1,
        }
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

    pub fn connect(&mut self, conn: ClientConn) -> Session {
        let id = self.next_client_id;
        self.next_client_id += 1;
        let now = self.now();
        self.clients.insert(
            id,
            Client {
                id,
                conn,
                name: None,
                lib_name: None,
                lib_ver: None,
                db: 0,
                resp: 2,
                created: now,
                last_interaction: now,
                last_cmd: None,
                no_evict: false,
                no_touch: false,
                reply_off: false,
                reply_skip: false,
                reply_skip_next: false,
                blocked: None,
                multi: None,
                multi_error: false,
                dirty_cas: false,
                watched: Vec::new(),
                subs: Default::default(),
                pushed: Vec::new(),
            },
        );
        Session { id, resp: 2, closing: false, blocked: false }
    }

    pub fn disconnect(&mut self, session: &Session) {
        self.remove_client(session.id);
    }

    /// Forgets a client and everything it was waiting for.
    pub fn remove_client(&mut self, id: u64) -> Option<Client> {
        self.unblock(id);
        self.unwatch_all(id);
        self.unsubscribe_everything(id);
        self.replies.remove(&id);
        self.clients.remove(&id)
    }

    pub fn client_count(&self) -> usize {
        self.clients.len()
    }

    /// Whether CLIENT PAUSE holds back this command right now. The server
    /// waits and retries until it doesn't.
    pub fn is_paused_for(&mut self, args: &[Vec<u8>]) -> bool {
        let Some(pause) = &self.pause else { return false };
        if self.now() >= pause.until {
            self.pause = None;
            return false;
        }
        if pause.all {
            return true;
        }
        let name = String::from_utf8_lossy(&args[0]).to_ascii_lowercase();
        command_meta::lookup(&name)
            .is_some_and(|m| m.has_flag("write") || m.has_flag("may_replicate"))
    }

    pub fn execute(&mut self, session: &mut Session, args: &[Vec<u8>]) -> Value {
        if !self.clients.contains_key(&session.id) {
            // Killed by another client: the connection is going away.
            session.closing = true;
            return Value::NoReply;
        }
        let name = String::from_utf8_lossy(&args[0]).to_ascii_lowercase();
        let in_multi = self.clients[&session.id].multi.is_some();
        let (handler, fullname) = match resolve(args) {
            Ok(found) => found,
            Err(e) if in_multi => return self.multi_reject(session.id, &name, e),
            Err(e) => return e,
        };
        let now = self.now();
        let client = self.clients.get_mut(&session.id).unwrap();
        client.last_interaction = now;
        client.last_cmd = Some(fullname);
        let c = &self.clients[&session.id];
        if c.subs.active() && c.resp == 2 && !super::pubsub::allowed_while_subscribed(&name) {
            let shown = self.clients[&session.id].last_cmd.clone().unwrap_or_default();
            let e = Value::err(format!(
                "ERR Can't execute '{shown}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT \
                 / RESET are allowed in this context"
            ));
            return if in_multi { self.multi_reject(session.id, &name, e) } else { e };
        }
        if in_multi && !super::multi::runs_in_multi(&name) {
            return self.queue(session.id, &name, args);
        }

        let reply = self.call(session, handler, args, false, None);
        self.serve_blocked();

        let Some(client) = self.clients.get_mut(&session.id) else {
            session.closing = true;
            return reply;
        };
        let suppressed = client.reply_off || client.reply_skip;
        client.reply_skip = std::mem::take(&mut client.reply_skip_next);
        session.resp = client.resp;
        if suppressed { Value::NoReply } else { reply }
    }

    /// Runs one command handler. If it blocks, the client is registered as
    /// waiting and `NoReply` comes back.
    pub(crate) fn call(
        &mut self,
        session: &mut Session,
        handler: Handler,
        args: &[Vec<u8>],
        deny_blocking: bool,
        reprocess_deadline: Option<u64>,
    ) -> Value {
        let now = self.now();
        let db = self.clients.get(&session.id).map_or(0, |c| c.db);
        let before = self.watch_snapshot(args, db);
        let mut ctx = Ctx {
            engine: self,
            session,
            now,
            deny_blocking,
            in_exec: deny_blocking,
            block: None,
            reprocess_deadline,
        };
        let reply = match handler(&mut ctx, args) {
            Ok(v) | Err(v) => v,
        };
        if let Some(req) = ctx.block.take() {
            self.block_client(session, req, args);
            return Value::NoReply;
        }
        if !matches!(reply, Value::Error(_)) {
            self.touch_written(args, db, before);
        }
        reply
    }
}

/// Finds the handler and full name for `args`, or the error Redis gives.
pub(crate) fn resolve(args: &[Vec<u8>]) -> Result<(Handler, String), Value> {
    let name = String::from_utf8_lossy(&args[0]).to_ascii_lowercase();
    let (Some(cmd), Some(meta)) = (find(&name), command_meta::lookup(&name)) else {
        return Err(unknown_command(args));
    };
    if cmd.subs.is_empty() || args.len() == 1 {
        check_arity(meta, args.len())?;
        return Ok((cmd.handler, name));
    }
    let sub_name = String::from_utf8_lossy(&args[1]).to_ascii_lowercase();
    let fullname = format!("{name}|{sub_name}");
    let (Some(sub), Some(sub_meta)) =
        (cmd.subs.iter().find(|s| s.name == sub_name), command_meta::lookup(&fullname))
    else {
        let shown: String = String::from_utf8_lossy(&args[1]).chars().take(128).collect();
        let msg = format!("ERR unknown subcommand '{shown}'. Try {} HELP.", name.to_uppercase());
        return Err(Value::Error(msg.replace(['\r', '\n'], " ")));
    };
    check_arity(sub_meta, args.len())?;
    Ok((sub.handler, fullname))
}

fn check_arity(meta: &CommandMeta, argc: usize) -> Result<(), Value> {
    let n = argc as i64;
    if (meta.arity > 0 && n != meta.arity) || n < -meta.arity {
        return Err(arity_error(meta.name));
    }
    Ok(())
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

/// The reply of a `HELP` subcommand, framed the way Redis's `addReplyHelp`
/// does.
pub fn help_reply(command: &str, lines: &[&str]) -> Value {
    let mut out = vec![Value::Simple(format!(
        "{} <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
        command.to_uppercase()
    ))];
    out.extend(lines.iter().map(|l| Value::Simple((*l).into())));
    out.push(Value::Simple("HELP".into()));
    out.push(Value::Simple("    Print this help.".into()));
    Value::Array(out)
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

/// `getLongFromObjectOrReply` (a long is 64 bits here).
pub fn long_arg(b: &[u8]) -> Result<i64, Value> {
    int_arg(b)
}

/// `getRangeLongFromObjectOrReply`: an integer in `min..=max`, failing with
/// `msg` (or Redis's default messages).
pub fn range_long(b: &[u8], min: i64, max: i64, msg: Option<&str>) -> Result<i64, Value> {
    let custom = |m: &str| Value::err(format!("ERR {m}"));
    let n = match super::num::parse_int(b) {
        Some(n) => n,
        None => return Err(msg.map_or_else(not_int, custom)),
    };
    if n < min || n > max {
        return Err(msg.map_or_else(
            || custom(&format!("value is out of range, value must between {min} and {max}")),
            custom,
        ));
    }
    Ok(n)
}

/// `getPositiveLongFromObjectOrReply`.
pub fn positive_long(b: &[u8], msg: Option<&str>) -> Result<i64, Value> {
    range_long(b, 0, i64::MAX, Some(msg.unwrap_or("value is out of range, must be positive")))
}

/// Parses an integer argument, failing with a custom message.
pub fn int_arg_msg(b: &[u8], msg: &str) -> Result<i64, Value> {
    super::num::parse_int(b).ok_or_else(|| Value::err(format!("ERR {msg}")))
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

/// A millisecond timeout as an absolute time (0 = none), like
/// `getTimeoutFromObjectOrReply(.., UNIT_MILLISECONDS)`.
pub fn timeout_ms_arg(b: &[u8], now: u64) -> Result<u64, Value> {
    let t = int_arg_msg(b, "timeout is not an integer or out of range")?;
    if t < 0 {
        return Err(Value::err("ERR timeout is negative"));
    }
    if t == 0 {
        return Ok(0);
    }
    t.checked_add(now as i64)
        .map(|v| v as u64)
        .ok_or_else(|| Value::err("ERR timeout is out of range"))
}

pub fn eq_ic(a: &[u8], b: &str) -> bool {
    a.eq_ignore_ascii_case(b.as_bytes())
}
