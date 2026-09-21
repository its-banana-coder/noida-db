//! Hash commands, ported from Redis's t_hash.c.

use super::engine::{Command, Ctx, Reply, arity_error, cmd, eq_ic, int_arg, syntax};
use super::keys::parse_scan;
use super::num;
use super::resp::Value;

pub static COMMANDS: &[Command] = &[
    cmd("hset", hset),
    cmd("hmset", hmset),
    cmd("hsetnx", hsetnx),
    cmd("hget", hget),
    cmd("hmget", hmget),
    cmd("hdel", hdel),
    cmd("hlen", hlen),
    cmd("hstrlen", hstrlen),
    cmd("hexists", hexists),
    cmd("hgetall", hgetall),
    cmd("hkeys", hkeys),
    cmd("hvals", hvals),
    cmd("hincrby", hincrby),
    cmd("hincrbyfloat", hincrbyfloat),
    cmd("hscan", hscan),
    cmd("hrandfield", hrandfield),
];

fn set_fields(ctx: &mut Ctx, a: &[Vec<u8>], name: &str) -> Result<i64, Value> {
    if a.len() % 2 == 1 {
        return Err(arity_error(name));
    }
    let h = ctx.hash_or_create(&a[1])?;
    Ok(a[2..].chunks(2).filter(|p| h.insert(&p[0], &p[1])).count() as i64)
}

fn hset(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    set_fields(ctx, a, "hset").map(Value::Integer)
}

fn hmset(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    set_fields(ctx, a, "hmset").map(|_| Value::ok())
}

fn hsetnx(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let h = ctx.hash_or_create(&a[1])?;
    if h.map.contains(&a[2]) {
        return Ok(Value::Integer(0));
    }
    h.insert(&a[2], &a[3]);
    Ok(Value::Integer(1))
}

fn hget(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let v = ctx.get_hash(&a[1])?.and_then(|h| h.map.get(&a[2]).cloned());
    Ok(v.map_or(Value::Null, Value::Bulk))
}

fn hmget(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let h = ctx.get_hash(&a[1])?;
    let values = a[2..]
        .iter()
        .map(|f| h.as_ref().and_then(|h| h.map.get(f)).map_or(Value::Null, Value::bulk))
        .collect();
    Ok(Value::Array(values))
}

fn hdel(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let Some(h) = ctx.get_hash(&a[1])? else { return Ok(Value::Integer(0)) };
    let n = a[2..].iter().filter(|f| h.map.remove(f)).count();
    ctx.drop_if_empty(&a[1]);
    Ok(Value::Integer(n as i64))
}

fn hlen(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    Ok(Value::Integer(ctx.get_hash(&a[1])?.map_or(0, |h| h.map.len()) as i64))
}

fn hstrlen(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let len = ctx.get_hash(&a[1])?.and_then(|h| h.map.get(&a[2])).map_or(0, |v| v.len());
    Ok(Value::Integer(len as i64))
}

fn hexists(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let found = ctx.get_hash(&a[1])?.is_some_and(|h| h.map.contains(&a[2]));
    Ok(Value::Integer(found as i64))
}

fn hgetall(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let pairs = match ctx.get_hash(&a[1])? {
        Some(h) => h.map.iter().map(|(k, v)| (Value::bulk(k), Value::bulk(v))).collect(),
        None => vec![],
    };
    Ok(Value::Map(pairs))
}

fn hkeys(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let keys = ctx
        .get_hash(&a[1])?
        .map_or(vec![], |h| h.map.iter().map(|(k, _)| Value::bulk(k)).collect());
    Ok(Value::Array(keys))
}

fn hvals(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let vals = ctx
        .get_hash(&a[1])?
        .map_or(vec![], |h| h.map.iter().map(|(_, v)| Value::bulk(v)).collect());
    Ok(Value::Array(vals))
}

fn hincrby(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let by = int_arg(&a[3])?;
    let h = ctx.hash_or_create(&a[1])?;
    let current = match h.map.get(&a[2]) {
        Some(v) => {
            num::parse_int(v).ok_or_else(|| Value::err("ERR hash value is not an integer"))?
        }
        None => 0,
    };
    let next = current
        .checked_add(by)
        .ok_or_else(|| Value::err("ERR increment or decrement would overflow"))?;
    h.insert(&a[2], next.to_string().as_bytes());
    Ok(Value::Integer(next))
}

fn hincrbyfloat(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let by = num::parse_float(&a[3]).ok_or_else(|| Value::err("ERR value is not a valid float"))?;
    if !by.is_finite() {
        return Err(Value::err("ERR value is NaN or Infinity"));
    }
    let h = ctx.hash_or_create(&a[1])?;
    let current = h.map.get(&a[2]).cloned().unwrap_or_else(|| b"0".to_vec());
    if num::parse_float(&current).is_none() {
        return Err(Value::err("ERR hash value is not a float"));
    }
    let next = num::add_human(&current, &a[3]).map_err(Value::err)?;
    h.insert(&a[2], next.as_bytes());
    Ok(Value::bulk(next))
}

/// HSCAN: a small hash comes back whole in one call (Redis ignores COUNT
/// for listpacks); a big one is walked with the cursor.
fn hscan(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let args = parse_scan(a, 2, false)?;
    let Some(h) = ctx.get_hash(&a[1])? else {
        return Ok(Value::Array(vec![Value::bulk("0"), Value::Array(vec![])]));
    };
    let (start, end) = if h.big {
        let end = (args.cursor + args.count).min(h.map.len());
        (args.cursor.min(end), end)
    } else {
        (0, h.map.len())
    };
    let mut out = Vec::new();
    for i in start..end {
        let (k, v) = h.map.entry_at(i);
        if args.matches(k) {
            out.push(Value::bulk(k));
            out.push(Value::bulk(v));
        }
    }
    let next = if end >= h.map.len() { 0 } else { end };
    Ok(Value::Array(vec![Value::bulk(next.to_string()), Value::Array(out)]))
}

/// A port of `hrandfieldCommand` / `hrandfieldWithCountCommand`.
fn hrandfield(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    if a.len() == 2 {
        let Some(len) = ctx.get_hash(&a[1])?.map(|h| h.map.len()) else {
            return Ok(Value::Null);
        };
        let i = ctx.random() as usize % len;
        let h = ctx.get_hash(&a[1])?.unwrap();
        return Ok(Value::bulk(&h.map.entry_at(i).0));
    }
    let l = int_arg(&a[2])?;
    if l == i64::MIN {
        return Err(Value::err(format!(
            "ERR value is out of range, value must between {} and {}",
            -i64::MAX,
            i64::MAX
        )));
    }
    if a.len() > 4 || (a.len() == 4 && !eq_ic(&a[3], "withvalues")) {
        return Err(syntax());
    }
    let with_values = a.len() == 4;
    if with_values && !(-i64::MAX / 2..=i64::MAX / 2).contains(&l) {
        return Err(Value::err("ERR value is out of range"));
    }
    let resp3 = ctx.resp() >= 3;
    let Some(size) = ctx.get_hash(&a[1])?.map(|h| h.map.len()) else {
        return Ok(Value::Array(vec![]));
    };
    let (count, unique) =
        if l >= 0 { (l as usize, true) } else { (l.unsigned_abs() as usize, false) };
    if count == 0 {
        return Ok(Value::Array(vec![]));
    }

    // Pick entry indexes: with repeats for a negative count, otherwise
    // distinct ones kept in stored order (as Redis does for small hashes).
    let picks: Vec<usize> = if !unique || count == 1 {
        (0..count).map(|_| ctx.random() as usize % size).collect()
    } else if count >= size {
        (0..size).collect()
    } else {
        let mut chosen = vec![false; size];
        let mut n = 0;
        while n < count {
            let i = ctx.random() as usize % size;
            if !chosen[i] {
                chosen[i] = true;
                n += 1;
            }
        }
        (0..size).filter(|i| chosen[*i]).collect()
    };

    let h = ctx.get_hash(&a[1])?.unwrap();
    let mut out = Vec::new();
    for i in picks {
        let (k, v) = h.map.entry_at(i);
        match (with_values, resp3) {
            (false, _) => out.push(Value::bulk(k)),
            (true, true) => out.push(Value::Array(vec![Value::bulk(k), Value::bulk(v)])),
            (true, false) => {
                out.push(Value::bulk(k));
                out.push(Value::bulk(v));
            }
        }
    }
    Ok(Value::Array(out))
}
