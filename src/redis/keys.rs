//! Generic keyspace commands: DEL, EXISTS, EXPIRE and TTL, KEYS, SCAN,
//! RENAME, COPY, MOVE, and database-level commands.

use super::engine::{
    Command, Ctx, NUM_DBS, Reply, db_arg, eq_ic, int_arg, invalid_expire, same_object, syntax,
};
use super::glob;
use super::resp::Value;

pub static COMMANDS: &[Command] = &[
    Command { name: "del", arity: -2, handler: del },
    Command { name: "unlink", arity: -2, handler: del },
    Command { name: "exists", arity: -2, handler: exists },
    Command { name: "touch", arity: -2, handler: exists },
    Command { name: "type", arity: 2, handler: type_cmd },
    Command { name: "expire", arity: -3, handler: expire },
    Command { name: "pexpire", arity: -3, handler: pexpire },
    Command { name: "expireat", arity: -3, handler: expireat },
    Command { name: "pexpireat", arity: -3, handler: pexpireat },
    Command { name: "ttl", arity: 2, handler: ttl },
    Command { name: "pttl", arity: 2, handler: pttl },
    Command { name: "expiretime", arity: 2, handler: expiretime },
    Command { name: "pexpiretime", arity: 2, handler: pexpiretime },
    Command { name: "persist", arity: 2, handler: persist },
    Command { name: "keys", arity: 2, handler: keys },
    Command { name: "scan", arity: -2, handler: scan },
    Command { name: "randomkey", arity: 1, handler: randomkey },
    Command { name: "rename", arity: 3, handler: rename },
    Command { name: "renamenx", arity: 3, handler: renamenx },
    Command { name: "copy", arity: -3, handler: copy },
    Command { name: "move", arity: 3, handler: move_cmd },
    Command { name: "dbsize", arity: 1, handler: dbsize },
    Command { name: "flushdb", arity: -1, handler: flushdb },
    Command { name: "flushall", arity: -1, handler: flushall },
    Command { name: "swapdb", arity: 3, handler: swapdb },
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

/// SCAN over the sorted keyspace; the cursor is an index into it. Keys that
/// exist for the whole iteration are always returned, as Redis guarantees.
fn scan(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let cursor: usize = std::str::from_utf8(&a[1])
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| Value::err("ERR invalid cursor"))?;
    let mut count = 10usize;
    let mut pattern: Option<&[u8]> = None;
    let mut type_filter: Option<Vec<u8>> = None;
    let mut i = 2;
    while i < a.len() {
        let has_value = i + 1 < a.len();
        if eq_ic(&a[i], "count") && has_value {
            let n = int_arg(&a[i + 1])?;
            if n < 1 {
                return Err(syntax());
            }
            count = n as usize;
        } else if eq_ic(&a[i], "match") && has_value {
            pattern = Some(&a[i + 1]);
        } else if eq_ic(&a[i], "type") && has_value {
            type_filter = Some(a[i + 1].to_ascii_lowercase());
        } else {
            return Err(syntax());
        }
        i += 2;
    }

    let now = ctx.now;
    let all = ctx.db().keys(now);
    let end = (cursor + count).min(all.len());
    let start = cursor.min(end);
    let mut out = Vec::new();
    for key in &all[start..end] {
        if pattern.is_some_and(|p| !glob::matches_key(p, key)) {
            continue;
        }
        if let Some(t) = &type_filter {
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
    let mut dst_db = ctx.session.db;
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
    if dst_db == ctx.session.db && src == dst {
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
    if dst_db == ctx.session.db {
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
    Ok(Value::ok())
}
