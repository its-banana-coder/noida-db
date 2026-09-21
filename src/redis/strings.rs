//! String commands.

use super::engine::{
    Command, Ctx, Data, Entry, Reply, arity_error, cmd, eq_ic, int_arg, invalid_expire, not_int,
    syntax,
};
use super::num;
use super::resp::Value;

/// Redis's `proto-max-bulk-len` default.
const MAX_STRING: usize = 512 * 1024 * 1024;

pub static COMMANDS: &[Command] = &[
    cmd("get", get),
    cmd("set", set),
    cmd("setnx", setnx),
    cmd("setex", setex),
    cmd("psetex", psetex),
    cmd("getset", getset),
    cmd("getdel", getdel),
    cmd("getex", getex),
    cmd("mget", mget),
    cmd("mset", mset),
    cmd("msetnx", msetnx),
    cmd("append", append),
    cmd("strlen", strlen),
    cmd("incr", incr),
    cmd("decr", decr),
    cmd("incrby", incrby),
    cmd("decrby", decrby),
    cmd("incrbyfloat", incrbyfloat),
    cmd("getrange", getrange),
    cmd("substr", getrange),
    cmd("setrange", setrange),
    cmd("lcs", lcs),
];

fn opt_bulk(v: Option<&mut Vec<u8>>) -> Value {
    v.map_or(Value::Null, Value::bulk)
}

fn get(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    Ok(opt_bulk(ctx.get_str(&a[1])?))
}

/// Parsed options shared by SET and GETEX (Redis's
/// `parseExtendedStringArgumentsOrReply`).
#[derive(Default)]
struct ExtArgs {
    nx: bool,
    xx: bool,
    get: bool,
    keepttl: bool,
    persist: bool,
    /// Absolute expiry in unix ms.
    expire_at: Option<i64>,
}

fn parse_ext(ctx: &Ctx, args: &[Vec<u8>], is_set: bool, cmd: &str) -> Result<ExtArgs, Value> {
    #[derive(PartialEq, Clone, Copy)]
    enum Exp {
        Ex,
        Px,
        ExAt,
        PxAt,
    }
    let mut o = ExtArgs::default();
    let mut exp: Option<(Exp, &[u8])> = None;
    let mut j = 0;
    while j < args.len() {
        let arg = args[j].to_ascii_lowercase();
        let next = args.get(j + 1);
        let exp_kind = exp.map(|(k, _)| k);
        let exp_allowed = |k: Exp| !o.keepttl && !o.persist && exp_kind.is_none_or(|e| e == k);
        match arg.as_slice() {
            b"nx" if is_set && !o.xx => o.nx = true,
            b"xx" if is_set && !o.nx => o.xx = true,
            b"get" if is_set => o.get = true,
            b"keepttl" if is_set && exp.is_none() => o.keepttl = true,
            b"persist" if !is_set && exp.is_none() => o.persist = true,
            b"ex" | b"px" | b"exat" | b"pxat" => {
                let kind = match arg.as_slice() {
                    b"ex" => Exp::Ex,
                    b"px" => Exp::Px,
                    b"exat" => Exp::ExAt,
                    _ => Exp::PxAt,
                };
                match next {
                    Some(v) if exp_allowed(kind) => {
                        exp = Some((kind, v));
                        j += 1;
                    }
                    _ => return Err(syntax()),
                }
            }
            _ => return Err(syntax()),
        }
        j += 1;
    }
    if let Some((kind, raw)) = exp {
        let n = int_arg(raw)?;
        let seconds = matches!(kind, Exp::Ex | Exp::ExAt);
        if n <= 0 || (seconds && n > i64::MAX / 1000) {
            return Err(invalid_expire(cmd));
        }
        let mut ms = if seconds { n * 1000 } else { n };
        if matches!(kind, Exp::Ex | Exp::Px) {
            ms = ms.checked_add(ctx.now as i64).ok_or_else(|| invalid_expire(cmd))?;
        }
        o.expire_at = Some(ms);
    }
    Ok(o)
}

fn set(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let o = parse_ext(ctx, &a[3..], true, "set")?;
    let old = if o.get { Some(ctx.get_str(&a[1])?.map(|s| s.clone())) } else { None };
    let existing = ctx.lookup(&a[1]).map(|e| e.expires_at);
    if (o.nx && existing.is_some()) || (o.xx && existing.is_none()) {
        return Ok(old.map_or(Value::Null, |o| o.map_or(Value::Null, Value::Bulk)));
    }
    let expires_at = match o.expire_at {
        Some(at) => Some(at as u64),
        None if o.keepttl => existing.flatten(),
        None => None,
    };
    ctx.db().insert(a[1].clone(), Entry { data: Data::Str(a[2].clone()), expires_at });
    Ok(old.map_or(Value::ok(), |o| o.map_or(Value::Null, Value::Bulk)))
}

fn set_str(ctx: &mut Ctx, key: &[u8], value: Vec<u8>, expires_at: Option<u64>) {
    ctx.db().insert(key.to_vec(), Entry { data: Data::Str(value), expires_at });
}

fn setnx(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    if ctx.lookup(&a[1]).is_some() {
        return Ok(Value::Integer(0));
    }
    set_str(ctx, &a[1], a[2].clone(), None);
    Ok(Value::Integer(1))
}

fn setex_generic(ctx: &mut Ctx, a: &[Vec<u8>], millis: bool, cmd: &str) -> Reply {
    let n = int_arg(&a[2])?;
    if n <= 0 || (!millis && n > i64::MAX / 1000) {
        return Err(invalid_expire(cmd));
    }
    let ms = if millis { n } else { n * 1000 };
    let at = ms.checked_add(ctx.now as i64).ok_or_else(|| invalid_expire(cmd))?;
    set_str(ctx, &a[1], a[3].clone(), Some(at as u64));
    Ok(Value::ok())
}

fn setex(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    setex_generic(ctx, a, false, "setex")
}

fn psetex(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    setex_generic(ctx, a, true, "psetex")
}

fn getset(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let old = opt_bulk(ctx.get_str(&a[1])?);
    set_str(ctx, &a[1], a[2].clone(), None);
    Ok(old)
}

fn getdel(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let old = opt_bulk(ctx.get_str(&a[1])?);
    if old != Value::Null {
        let now = ctx.now;
        ctx.db().remove(&a[1], now);
    }
    Ok(old)
}

fn getex(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let o = parse_ext(ctx, &a[2..], false, "getex")?;
    let now = ctx.now as i64;
    let value = opt_bulk(ctx.get_str(&a[1])?);
    if value == Value::Null {
        return Ok(value);
    }
    match o.expire_at {
        Some(at) if at <= now => {
            ctx.db().remove(&a[1], now as u64);
        }
        Some(at) => ctx.lookup(&a[1]).unwrap().expires_at = Some(at as u64),
        None if o.persist => ctx.lookup(&a[1]).unwrap().expires_at = None,
        None => {}
    }
    Ok(value)
}

fn mget(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let values = a[1..]
        .iter()
        .map(|k| match ctx.lookup(k) {
            Some(Entry { data: Data::Str(s), .. }) => Value::bulk(s),
            // MGET never errors: other types read as nil.
            #[allow(unreachable_patterns)]
            _ => Value::Null,
        })
        .collect();
    Ok(Value::Array(values))
}

fn mset(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    if a.len().is_multiple_of(2) {
        return Err(arity_error("mset"));
    }
    for pair in a[1..].chunks(2) {
        set_str(ctx, &pair[0], pair[1].clone(), None);
    }
    Ok(Value::ok())
}

fn msetnx(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    if a.len().is_multiple_of(2) {
        return Err(arity_error("msetnx"));
    }
    if a[1..].chunks(2).any(|pair| ctx.lookup(&pair[0]).is_some()) {
        return Ok(Value::Integer(0));
    }
    for pair in a[1..].chunks(2) {
        set_str(ctx, &pair[0], pair[1].clone(), None);
    }
    Ok(Value::Integer(1))
}

fn append(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    match ctx.get_str(&a[1])? {
        Some(s) => {
            if s.len() + a[2].len() > MAX_STRING {
                return Err(too_big());
            }
            s.extend_from_slice(&a[2]);
            Ok(Value::Integer(s.len() as i64))
        }
        None => {
            set_str(ctx, &a[1], a[2].clone(), None);
            Ok(Value::Integer(a[2].len() as i64))
        }
    }
}

fn too_big() -> Value {
    Value::err("ERR string exceeds maximum allowed size (proto-max-bulk-len)")
}

fn strlen(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    Ok(Value::Integer(ctx.get_str(&a[1])?.map_or(0, |s| s.len()) as i64))
}

/// INCR and friends: the TTL survives, as in Redis.
fn incr_by(ctx: &mut Ctx, key: &[u8], by: i64) -> Reply {
    let current = match ctx.get_str(key)? {
        Some(s) => num::parse_int(s).ok_or_else(not_int)?,
        None => 0,
    };
    let next = current
        .checked_add(by)
        .ok_or_else(|| Value::err("ERR increment or decrement would overflow"))?;
    let text = next.to_string().into_bytes();
    match ctx.get_str(key)? {
        Some(s) => *s = text,
        None => set_str(ctx, key, text, None),
    }
    Ok(Value::Integer(next))
}

fn incr(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    incr_by(ctx, &a[1], 1)
}

fn decr(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    incr_by(ctx, &a[1], -1)
}

fn incrby(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let by = int_arg(&a[2])?;
    incr_by(ctx, &a[1], by)
}

fn decrby(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let by = int_arg(&a[2])?;
    let by = by.checked_neg().ok_or_else(|| Value::err("ERR decrement would overflow"))?;
    incr_by(ctx, &a[1], by)
}

fn incrbyfloat(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let current = ctx.get_str(&a[1])?.map_or(b"0".to_vec(), |s| s.clone());
    let next = num::add_human(&current, &a[2]).map_err(Value::err)?;
    match ctx.get_str(&a[1])? {
        Some(s) => *s = next.clone().into_bytes(),
        None => set_str(ctx, &a[1], next.clone().into_bytes(), None),
    }
    Ok(Value::bulk(next))
}

fn getrange(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let (mut start, mut end) = (int_arg(&a[2])?, int_arg(&a[3])?);
    let Some(s) = ctx.get_str(&a[1])? else {
        return Ok(Value::bulk(""));
    };
    let len = s.len() as i64;
    if start < 0 && end < 0 && start > end {
        return Ok(Value::bulk(""));
    }
    if start < 0 {
        start += len;
    }
    if end < 0 {
        end += len;
    }
    start = start.max(0);
    end = end.max(0).min(len - 1);
    if start > end || len == 0 {
        return Ok(Value::bulk(""));
    }
    Ok(Value::bulk(&s[start as usize..=end as usize]))
}

fn setrange(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let offset = int_arg(&a[2])?;
    if offset < 0 {
        return Err(Value::err("ERR offset is out of range"));
    }
    let offset = offset as usize;
    let value = &a[3];
    if !value.is_empty() && offset + value.len() > MAX_STRING {
        return Err(too_big());
    }
    let s = match ctx.get_str(&a[1])? {
        Some(s) => s,
        None if value.is_empty() => return Ok(Value::Integer(0)),
        None => {
            set_str(ctx, &a[1], Vec::new(), None);
            ctx.get_str(&a[1])?.unwrap()
        }
    };
    if !value.is_empty() {
        if s.len() < offset + value.len() {
            s.resize(offset + value.len(), 0);
        }
        s[offset..offset + value.len()].copy_from_slice(value);
    }
    Ok(Value::Integer(s.len() as i64))
}

/// A port of Redis's `lcsCommand`, including its match-range reporting.
fn lcs(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    // Redis checks the key types before parsing options.
    let mut fetch = |k: &[u8]| -> Result<Vec<u8>, Value> {
        match ctx.get_str(k) {
            Ok(v) => Ok(v.map(|s| s.clone()).unwrap_or_default()),
            Err(_) => Err(Value::err("ERR The specified keys must contain string values")),
        }
    };
    let x = fetch(&a[1])?;
    let y = fetch(&a[2])?;
    let (mut get_len, mut get_idx, mut with_match_len) = (false, false, false);
    let mut min_match_len = 0i64;
    let mut j = 3;
    while j < a.len() {
        let more = j + 1 < a.len();
        if eq_ic(&a[j], "idx") {
            get_idx = true;
        } else if eq_ic(&a[j], "len") {
            get_len = true;
        } else if eq_ic(&a[j], "withmatchlen") {
            with_match_len = true;
        } else if eq_ic(&a[j], "minmatchlen") && more {
            min_match_len = int_arg(&a[j + 1])?.max(0);
            j += 1;
        } else {
            return Err(syntax());
        }
        j += 1;
    }
    if get_idx && get_len {
        return Err(Value::err(
            "ERR If you want both the length and indexes, please just use IDX.",
        ));
    }

    let (alen, blen) = (x.len(), y.len());
    let w = blen + 1;
    let mut t = vec![0u32; (alen + 1) * w];
    for i in 1..=alen {
        for k in 1..=blen {
            t[i * w + k] = if x[i - 1] == y[k - 1] {
                t[(i - 1) * w + k - 1] + 1
            } else {
                t[(i - 1) * w + k].max(t[i * w + k - 1])
            };
        }
    }
    let total = t[alen * w + blen] as usize;
    let mut result = vec![0u8; total];
    let mut idx = total;
    let mut matches = Vec::new();
    let (mut i, mut k) = (alen, blen);
    let (mut a_start, mut a_end, mut b_start, mut b_end) = (alen, 0, 0, 0);
    while i > 0 && k > 0 {
        let mut emit = false;
        if x[i - 1] == y[k - 1] {
            result[idx - 1] = x[i - 1];
            if a_start == alen {
                (a_start, a_end, b_start, b_end) = (i - 1, i - 1, k - 1, k - 1);
            } else if a_start == i && b_start == k {
                a_start -= 1;
                b_start -= 1;
            } else {
                emit = true;
            }
            if a_start == 0 || b_start == 0 {
                emit = true;
            }
            idx -= 1;
            i -= 1;
            k -= 1;
        } else {
            if t[(i - 1) * w + k] > t[i * w + k - 1] {
                i -= 1;
            } else {
                k -= 1;
            }
            if a_start != alen {
                emit = true;
            }
        }
        let match_len = a_end as i64 - a_start as i64 + 1;
        if emit {
            if min_match_len == 0 || match_len >= min_match_len {
                let mut m = vec![
                    Value::Array(vec![
                        Value::Integer(a_start as i64),
                        Value::Integer(a_end as i64),
                    ]),
                    Value::Array(vec![
                        Value::Integer(b_start as i64),
                        Value::Integer(b_end as i64),
                    ]),
                ];
                if with_match_len {
                    m.push(Value::Integer(match_len));
                }
                matches.push(Value::Array(m));
            }
            a_start = alen;
        }
    }

    if get_idx {
        Ok(Value::Array(vec![
            Value::bulk("matches"),
            Value::Array(matches),
            Value::bulk("len"),
            Value::Integer(total as i64),
        ]))
    } else if get_len {
        Ok(Value::Integer(total as i64))
    } else {
        Ok(Value::Bulk(result))
    }
}
