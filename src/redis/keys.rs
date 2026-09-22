//! Generic keyspace commands: DEL, EXISTS, EXPIRE and TTL, KEYS, SCAN,
//! RENAME, COPY, MOVE, and database-level commands.

use super::engine::{
    Command, Ctx, Data, NUM_DBS, Reply, cmd, container, db_arg, eq_ic, help_reply, int_arg,
    invalid_expire, same_object, syntax,
};
use super::glob;
use super::resp::Value;

pub static COMMANDS: &[Command] = &[
    cmd("del", del),
    cmd("unlink", del),
    cmd("exists", exists),
    cmd("touch", exists),
    cmd("type", type_cmd),
    cmd("expire", expire),
    cmd("pexpire", pexpire),
    cmd("expireat", expireat),
    cmd("pexpireat", pexpireat),
    cmd("ttl", ttl),
    cmd("pttl", pttl),
    cmd("expiretime", expiretime),
    cmd("pexpiretime", pexpiretime),
    cmd("persist", persist),
    cmd("keys", keys),
    cmd("scan", scan),
    cmd("randomkey", randomkey),
    cmd("rename", rename),
    cmd("renamenx", renamenx),
    cmd("copy", copy),
    cmd("move", move_cmd),
    cmd("dbsize", dbsize),
    cmd("flushdb", flushdb),
    cmd("flushall", flushall),
    cmd("swapdb", swapdb),
    container("object", object_help, OBJECT),
];

static OBJECT: &[Command] = &[
    cmd("help", object_help),
    cmd("encoding", object_encoding),
    cmd("refcount", object_refcount),
    cmd("idletime", object_idletime),
    cmd("freq", object_freq),
];

fn del(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let now = ctx.now;
    let n = a[1..].iter().filter(|k| ctx.db().remove(k, now).is_some()).count();
    Ok(Value::Integer(n as i64))
}

/// EXISTS and TOUCH: counts keys, repeats included.
fn exists(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let n = a[1..].iter().filter(|k| ctx.lookup(k).is_some()).count();
    Ok(Value::Integer(n as i64))
}

fn type_cmd(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let name = ctx.lookup(&a[1]).map_or("none", |e| e.data.type_name());
    Ok(Value::Simple(name.into()))
}

#[derive(Clone, Copy, PartialEq)]
enum Unit {
    Seconds,
    Millis,
}

fn expire(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let now = ctx.now as i64;
    expire_generic(ctx, a, now, Unit::Seconds, "expire")
}

fn pexpire(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let now = ctx.now as i64;
    expire_generic(ctx, a, now, Unit::Millis, "pexpire")
}

fn expireat(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    expire_generic(ctx, a, 0, Unit::Seconds, "expireat")
}

fn pexpireat(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    expire_generic(ctx, a, 0, Unit::Millis, "pexpireat")
}

/// A port of Redis's `expireGenericCommand`, including NX/XX/GT/LT.
fn expire_generic(ctx: &mut Ctx, a: &[Vec<u8>], base: i64, unit: Unit, cmd: &str) -> Reply {
    let (mut nx, mut xx, mut gt, mut lt) = (false, false, false, false);
    for opt in &a[3..] {
        match opt.to_ascii_lowercase().as_slice() {
            b"nx" => nx = true,
            b"xx" => xx = true,
            b"gt" => gt = true,
            b"lt" => lt = true,
            _ => {
                return Err(Value::err(format!(
                    "ERR Unsupported option {}",
                    String::from_utf8_lossy(opt)
                )));
            }
        }
    }
    if nx && (xx || gt || lt) {
        return Err(Value::err(
            "ERR NX and XX, GT or LT options at the same time are not compatible",
        ));
    }
    if gt && lt {
        return Err(Value::err("ERR GT and LT options at the same time are not compatible"));
    }

    let mut when = int_arg(&a[2])?;
    if unit == Unit::Seconds {
        when = when.checked_mul(1000).ok_or_else(|| invalid_expire(cmd))?;
    }
    let when = when.checked_add(base).ok_or_else(|| invalid_expire(cmd))?;

    let now = ctx.now as i64;
    let Some(entry) = ctx.lookup(&a[1]) else {
        return Ok(Value::Integer(0));
    };
    let current = entry.expires_at.map(|t| t as i64);
    let refused = (nx && current.is_some())
        || (xx && current.is_none())
        || (gt && current.is_none_or(|c| when <= c))
        || (lt && current.is_some_and(|c| when >= c));
    if refused {
        return Ok(Value::Integer(0));
    }
    if when <= now {
        ctx.db().remove(&a[1], now as u64);
    } else if let Some(e) = ctx.lookup(&a[1]) {
        e.expires_at = Some(when as u64);
    }
    Ok(Value::Integer(1))
}

/// A port of `ttlGenericCommand`.
fn ttl_generic(ctx: &mut Ctx, key: &[u8], millis: bool, absolute: bool) -> Reply {
    let now = ctx.now;
    let Some(entry) = ctx.lookup(key) else {
        return Ok(Value::Integer(-2));
    };
    let Some(at) = entry.expires_at else {
        return Ok(Value::Integer(-1));
    };
    let t = if absolute { at as i64 } else { (at as i64 - now as i64).max(0) };
    Ok(Value::Integer(if millis { t } else { (t + 500) / 1000 }))
}

fn ttl(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    ttl_generic(ctx, &a[1], false, false)
}

fn pttl(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    ttl_generic(ctx, &a[1], true, false)
}

fn expiretime(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    ttl_generic(ctx, &a[1], false, true)
}

fn pexpiretime(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    ttl_generic(ctx, &a[1], true, true)
}

fn persist(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let had = ctx.lookup(&a[1]).and_then(|e| e.expires_at.take()).is_some();
    Ok(Value::Integer(had as i64))
}

fn keys(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let now = ctx.now;
    let pattern = &a[1];
    let found = ctx
        .db()
        .keys(now)
        .into_iter()
        .filter(|k| glob::matches_key(pattern, k))
        .map(Value::Bulk)
        .collect();
    Ok(Value::Array(found))
}

/// Options shared by SCAN, HSCAN, SSCAN and ZSCAN.
pub struct ScanArgs<'a> {
    pub cursor: usize,
    pub count: usize,
    pub pattern: Option<&'a [u8]>,
    pub type_filter: Option<Vec<u8>>,
}

impl ScanArgs<'_> {
    pub fn matches(&self, item: &[u8]) -> bool {
        self.pattern.is_none_or(|p| glob::matches_key(p, item))
    }
}

/// Parses `cursor [MATCH p] [COUNT n] [TYPE t]` starting at `a[i]`. TYPE is
/// only valid for SCAN itself, as in Redis's `scanGenericCommand`.
pub fn parse_scan(a: &[Vec<u8>], i: usize, allow_type: bool) -> Result<ScanArgs<'_>, Value> {
    let cursor: usize = std::str::from_utf8(&a[i])
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| Value::err("ERR invalid cursor"))?;
    let mut args = ScanArgs { cursor, count: 10, pattern: None, type_filter: None };
    let mut j = i + 1;
    while j < a.len() {
        let has_value = j + 1 < a.len();
        if eq_ic(&a[j], "count") && has_value {
            let n = int_arg(&a[j + 1])?;
            if n < 1 {
                return Err(syntax());
            }
            args.count = n as usize;
        } else if eq_ic(&a[j], "match") && has_value {
            args.pattern = Some(&a[j + 1]);
        } else if eq_ic(&a[j], "type") && has_value && allow_type {
            args.type_filter = Some(a[j + 1].to_ascii_lowercase());
        } else {
            return Err(syntax());
        }
        j += 2;
    }
    Ok(args)
}

/// SCAN over the sorted keyspace; the cursor is an index into it. Keys that
/// exist for the whole iteration are always returned, as Redis guarantees.
fn scan(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let args = parse_scan(a, 1, true)?;
    let now = ctx.now;
    let all = ctx.db().keys(now);
    let end = (args.cursor + args.count).min(all.len());
    let start = args.cursor.min(end);
    let mut out = Vec::new();
    for key in &all[start..end] {
        if !args.matches(key) {
            continue;
        }
        if let Some(t) = &args.type_filter {
            let ty = ctx.lookup(key).map(|e| e.data.type_name());
            if ty.map(str::as_bytes) != Some(t.as_slice()) {
                continue;
            }
        }
        out.push(Value::Bulk(key.clone()));
    }
    let next = if end >= all.len() { 0 } else { end };
    Ok(Value::Array(vec![Value::bulk(next.to_string()), Value::Array(out)]))
}

fn randomkey(ctx: &mut Ctx, _: &[Vec<u8>]) -> Reply {
    let now = ctx.now;
    let keys = ctx.db().keys(now);
    if keys.is_empty() {
        return Ok(Value::Null);
    }
    let i = ctx.random() as usize % keys.len();
    Ok(Value::Bulk(keys[i].clone()))
}

fn rename_generic(ctx: &mut Ctx, a: &[Vec<u8>], nx: bool) -> Reply {
    let (src, dst) = (&a[1], &a[2]);
    if ctx.lookup(src).is_none() {
        return Err(Value::err("ERR no such key"));
    }
    if src == dst {
        return Ok(if nx { Value::Integer(0) } else { Value::ok() });
    }
    let now = ctx.now;
    if ctx.lookup(dst).is_some() {
        if nx {
            return Ok(Value::Integer(0));
        }
        ctx.db().remove(dst, now);
    }
    let entry = ctx.db().remove(src, now).expect("checked above");
    ctx.db().insert(dst.clone(), entry);
    Ok(if nx { Value::Integer(1) } else { Value::ok() })
}

fn rename(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    rename_generic(ctx, a, false)
}

fn renamenx(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    rename_generic(ctx, a, true)
}

fn copy(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let (src, dst) = (&a[1], &a[2]);
    let mut replace = false;
    let mut dst_db = ctx.db_index();
    let mut i = 3;
    while i < a.len() {
        if eq_ic(&a[i], "replace") {
            replace = true;
        } else if eq_ic(&a[i], "db") && i + 1 < a.len() {
            dst_db = db_arg(&a[i + 1])?;
            i += 1;
        } else {
            return Err(syntax());
        }
        i += 1;
    }
    if dst_db == ctx.db_index() && src == dst {
        return Err(same_object());
    }
    let Some(entry) = ctx.lookup(src).cloned() else {
        return Ok(Value::Integer(0));
    };
    let now = ctx.now;
    let target = &mut ctx.engine.dbs[dst_db];
    if target.contains(dst, now) {
        if !replace {
            return Ok(Value::Integer(0));
        }
        target.remove(dst, now);
    }
    target.insert(dst.clone(), entry);
    Ok(Value::Integer(1))
}

fn move_cmd(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let dst_db = db_arg(&a[2])?;
    if dst_db == ctx.db_index() {
        return Err(same_object());
    }
    let now = ctx.now;
    if ctx.lookup(&a[1]).is_none() || ctx.engine.dbs[dst_db].contains(&a[1], now) {
        return Ok(Value::Integer(0));
    }
    let entry = ctx.db().remove(&a[1], now).expect("checked above");
    ctx.engine.dbs[dst_db].insert(a[1].clone(), entry);
    Ok(Value::Integer(1))
}

fn dbsize(ctx: &mut Ctx, _: &[Vec<u8>]) -> Reply {
    let now = ctx.now;
    Ok(Value::Integer(ctx.db().len(now) as i64))
}

/// FLUSHDB / FLUSHALL accept one optional ASYNC or SYNC.
fn flush_mode_ok(a: &[Vec<u8>]) -> bool {
    match a.len() {
        1 => true,
        2 => eq_ic(&a[1], "async") || eq_ic(&a[1], "sync"),
        _ => false,
    }
}

fn flushdb(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    if !flush_mode_ok(a) {
        return Err(syntax());
    }
    ctx.db().clear();
    Ok(Value::ok())
}

fn flushall(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    if !flush_mode_ok(a) {
        return Err(syntax());
    }
    ctx.engine.dbs.iter_mut().for_each(|db| db.clear());
    Ok(Value::ok())
}

fn swapdb(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let index = |b: &[u8], which: &str| -> Result<usize, Value> {
        let n = int_arg(b).map_err(|_| Value::err(format!("ERR invalid {which} DB index")))?;
        if n < 0 || n >= NUM_DBS as i64 {
            return Err(Value::err("ERR DB index is out of range"));
        }
        Ok(n as usize)
    };
    let x = index(&a[1], "first")?;
    let y = index(&a[2], "second")?;
    ctx.engine.dbs.swap(x, y);
    // Clients stay blocked on their database index; the keys they wait for
    // may exist now (Redis's `scanDatabaseForReadyKeys`).
    for db in [x, y] {
        let keys: Vec<Vec<u8>> = ctx.engine.waiting[db].keys().cloned().collect();
        for k in keys {
            ctx.engine.signal_ready(db, &k);
        }
    }
    Ok(Value::ok())
}

// ---- OBJECT ----

fn object_help(_: &mut Ctx, _: &[Vec<u8>]) -> Reply {
    Ok(help_reply(
        "object",
        &[
            "ENCODING <key>",
            "    Return the kind of internal representation used in order to store the value",
            "    associated with a <key>.",
            "FREQ <key>",
            "    Return the access frequency index of the <key>. The returned integer is",
            "    proportional to the logarithm of the recent access frequency of the key.",
            "IDLETIME <key>",
            "    Return the idle time of the <key>, that is the approximated number of",
            "    seconds elapsed since the last access to the key.",
            "REFCOUNT <key>",
            "    Return the number of references of the value associated with the specified",
            "    <key>.",
        ],
    ))
}

/// Bytes a value takes in a listpack, to tell when Redis would switch a
/// list to a quicklist (`list-max-listpack-size -2`: 8KB).
fn listpack_entry_size(v: &[u8]) -> usize {
    let body = match super::num::parse_int(v) {
        Some(n) if (0..=127).contains(&n) => 1,
        Some(n) if (-4096..4096).contains(&n) => 2,
        Some(n) if i16::try_from(n).is_ok() => 3,
        Some(n) if (-(1 << 23)..(1 << 23)).contains(&n) => 4,
        Some(n) if i32::try_from(n).is_ok() => 5,
        Some(_) => 9,
        None if v.len() < 64 => 1 + v.len(),
        None if v.len() < 4096 => 2 + v.len(),
        None => 5 + v.len(),
    };
    body + if body < 128 {
        1
    } else if body < 16384 {
        2
    } else {
        3
    }
}

/// The name Redis's OBJECT ENCODING gives a value's representation.
pub fn encoding(data: &Data) -> &'static str {
    match data {
        Data::Str(s) if s.len() <= 20 && super::num::parse_int(s).is_some() => "int",
        Data::Str(s) if s.len() <= 44 => "embstr",
        Data::Str(_) => "raw",
        Data::Hash(h) if h.big => "hashtable",
        Data::Hash(_) => "listpack",
        Data::List(l) => {
            let bytes: usize = 7 + l.iter().map(|v| listpack_entry_size(v)).sum::<usize>();
            if bytes <= 8192 { "listpack" } else { "quicklist" }
        }
        Data::Set(s) => s.encoding(),
    }
}

fn object_encoding(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    Ok(ctx.lookup(&a[2]).map_or(Value::Null, |e| Value::bulk(encoding(&e.data))))
}

fn object_refcount(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    Ok(ctx.lookup(&a[2]).map_or(Value::Null, |_| Value::Integer(1)))
}

fn object_idletime(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    Ok(ctx.lookup(&a[2]).map_or(Value::Null, |_| Value::Integer(0)))
}

const NO_LFU: &str = "ERR An LFU maxmemory policy is not selected, access frequency not tracked. \
Please note that when switching between policies at runtime LRU and LFU data will take some time \
to adjust.";

fn object_freq(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    if ctx.lookup(&a[2]).is_none() {
        return Ok(Value::Null);
    }
    Err(Value::err(NO_LFU))
}
