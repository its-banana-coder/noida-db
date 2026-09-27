//! List commands, ported from Redis's t_list.c.

use std::collections::VecDeque;

use super::blocking::{BlockKind, timeout_secs_arg};
use super::engine::{
    Command, Ctx, Reply, arity_error, cmd, eq_ic, long_arg, positive_long, range_long, syntax,
};
use super::resp::Value;

pub static COMMANDS: &[Command] = &[
    cmd("lpush", lpush),
    cmd("rpush", rpush),
    cmd("lpushx", lpushx),
    cmd("rpushx", rpushx),
    cmd("linsert", linsert),
    cmd("llen", llen),
    cmd("lindex", lindex),
    cmd("lset", lset),
    cmd("lpop", lpop),
    cmd("rpop", rpop),
    cmd("lmpop", lmpop),
    cmd("lrange", lrange),
    cmd("ltrim", ltrim),
    cmd("lpos", lpos),
    cmd("lrem", lrem),
    cmd("lmove", lmove),
    cmd("rpoplpush", rpoplpush),
    cmd("blpop", blpop),
    cmd("brpop", brpop),
    cmd("blmove", blmove),
    cmd("brpoplpush", brpoplpush),
    cmd("blmpop", blmpop),
];

#[derive(Clone, Copy, PartialEq)]
enum End {
    Head,
    Tail,
}

type List = VecDeque<Vec<u8>>;

fn push(l: &mut List, v: Vec<u8>, end: End) {
    match end {
        End::Head => l.push_front(v),
        End::Tail => l.push_back(v),
    }
}

fn pop(l: &mut List, end: End) -> Option<Vec<u8>> {
    match end {
        End::Head => l.pop_front(),
        End::Tail => l.pop_back(),
    }
}

fn push_generic(ctx: &mut Ctx, a: &[Vec<u8>], end: End, only_existing: bool) -> Reply {
    if only_existing && ctx.get_list(&a[1])?.is_none() {
        return Ok(Value::Integer(0));
    }
    let l = ctx.list_or_create(&a[1])?;
    for v in &a[2..] {
        push(l, v.clone(), end);
    }
    Ok(Value::Integer(l.len() as i64))
}

fn lpush(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    push_generic(ctx, a, End::Head, false)
}

fn rpush(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    push_generic(ctx, a, End::Tail, false)
}

fn lpushx(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    push_generic(ctx, a, End::Head, true)
}

fn rpushx(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    push_generic(ctx, a, End::Tail, true)
}

fn linsert(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let after = if eq_ic(&a[2], "after") {
        true
    } else if eq_ic(&a[2], "before") {
        false
    } else {
        return Err(syntax());
    };
    let Some(l) = ctx.get_list(&a[1])? else { return Ok(Value::Integer(0)) };
    let Some(i) = l.iter().position(|v| *v == a[3]) else { return Ok(Value::Integer(-1)) };
    l.insert(if after { i + 1 } else { i }, a[4].clone());
    Ok(Value::Integer(l.len() as i64))
}

fn llen(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    Ok(Value::Integer(ctx.get_list(&a[1])?.map_or(0, |l| l.len()) as i64))
}

/// A list index (negative from the end) as a position, if in range.
fn resolve_index(len: usize, index: i64) -> Option<usize> {
    let i = if index < 0 { len as i64 + index } else { index };
    (0..len as i64).contains(&i).then_some(i as usize)
}

fn lindex(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let Some(l) = ctx.get_list(&a[1])? else { return Ok(Value::Null) };
    let index = long_arg(&a[2])?;
    Ok(resolve_index(l.len(), index).map_or(Value::Null, |i| Value::bulk(&l[i])))
}

fn lset(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let Some(l) = ctx.get_list(&a[1])? else { return Err(Value::err("ERR no such key")) };
    let index = long_arg(&a[2])?;
    let i = resolve_index(l.len(), index).ok_or_else(|| Value::err("ERR index out of range"))?;
    l[i] = a[3].clone();
    Ok(Value::ok())
}

/// Removes and returns up to `count` elements from one end, in pop order,
/// deleting the key if the list empties.
fn pop_many(ctx: &mut Ctx, key: &[u8], end: End, count: usize) -> Result<Vec<Value>, Value> {
    let l = ctx.get_list(key)?.expect("caller checked the key exists");
    let n = count.min(l.len());
    let out = (0..n).filter_map(|_| pop(l, end)).map(Value::Bulk).collect();
    ctx.drop_if_empty(key);
    Ok(out)
}

fn pop_generic(ctx: &mut Ctx, a: &[Vec<u8>], end: End, name: &str) -> Reply {
    if a.len() > 3 {
        return Err(arity_error(name));
    }
    let count = if a.len() == 3 { Some(positive_long(&a[2], None)?) } else { None };
    if ctx.get_list(&a[1])?.is_none() {
        return Ok(if count.is_some() { Value::NullArray } else { Value::Null });
    }
    match count {
        None => Ok(pop_many(ctx, &a[1], end, 1)?.pop().unwrap_or(Value::Null)),
        Some(0) => Ok(Value::Array(vec![])),
        Some(n) => Ok(Value::Array(pop_many(ctx, &a[1], end, n as usize)?)),
    }
}

fn lpop(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    pop_generic(ctx, a, End::Head, "lpop")
}

fn rpop(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    pop_generic(ctx, a, End::Tail, "rpop")
}

/// Normalizes an inclusive `start..=end` range with negative indexes, as
/// `addListRangeReply` and LTRIM do. `None` if it is empty.
fn range(len: usize, start: i64, end: i64) -> Option<(usize, usize)> {
    let len = len as i64;
    let mut start = if start < 0 { len + start } else { start };
    let mut end = if end < 0 { len + end } else { end };
    if start < 0 {
        start = 0;
    }
    if start > end || start >= len {
        return None;
    }
    if end >= len {
        end = len - 1;
    }
    Some((start as usize, end as usize))
}

fn lrange(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let start = long_arg(&a[2])?;
    let end = long_arg(&a[3])?;
    let Some(l) = ctx.get_list(&a[1])? else { return Ok(Value::Array(vec![])) };
    let items = match range(l.len(), start, end) {
        Some((s, e)) => l.range(s..=e).map(Value::bulk).collect(),
        None => vec![],
    };
    Ok(Value::Array(items))
}

fn ltrim(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let start = long_arg(&a[2])?;
    let end = long_arg(&a[3])?;
    let Some(l) = ctx.get_list(&a[1])? else { return Ok(Value::ok()) };
    match range(l.len(), start, end) {
        Some((s, e)) => {
            l.truncate(e + 1);
            l.drain(..s);
        }
        None => l.clear(),
    }
    ctx.drop_if_empty(&a[1]);
    Ok(Value::ok())
}

fn lpos(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let (mut rank, mut count, mut maxlen) = (1i64, None, 0i64);
    let mut j = 3;
    while j < a.len() {
        let more = j + 1 < a.len();
        if eq_ic(&a[j], "rank") && more {
            j += 1;
            rank = range_long(&a[j], -i64::MAX, i64::MAX, None)?;
            if rank == 0 {
                return Err(Value::err(
                    "ERR RANK can't be zero: use 1 to start from the first match, 2 from the \
                     second ... or use negative to start from the end of the list",
                ));
            }
        } else if eq_ic(&a[j], "count") && more {
            j += 1;
            count = Some(positive_long(&a[j], Some("COUNT can't be negative"))?);
        } else if eq_ic(&a[j], "maxlen") && more {
            j += 1;
            maxlen = positive_long(&a[j], Some("MAXLEN can't be negative"))?;
        } else {
            return Err(syntax());
        }
        j += 1;
    }
    let Some(l) = ctx.get_list(&a[1])? else {
        return Ok(if count.is_some() { Value::Array(vec![]) } else { Value::Null });
    };
    let from_tail = rank < 0;
    let rank = rank.unsigned_abs();
    let len = l.len();
    let mut matches = 0u64;
    let mut found = Vec::new();
    for index in 0..len {
        if maxlen != 0 && index as i64 >= maxlen {
            break;
        }
        let pos = if from_tail { len - index - 1 } else { index };
        if l[pos] != a[2] {
            continue;
        }
        matches += 1;
        if matches >= rank {
            found.push(Value::Integer(pos as i64));
            match count {
                None => break,
                Some(c) if c != 0 && found.len() as i64 >= c => break,
                _ => {}
            }
        }
    }
    Ok(match count {
        Some(_) => Value::Array(found),
        None => found.pop().unwrap_or(Value::Null),
    })
}

fn lrem(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let n = long_arg(&a[2])?;
    let Some(l) = ctx.get_list(&a[1])? else { return Ok(Value::Integer(0)) };
    let limit = n.unsigned_abs() as usize;
    let mut removed = 0;
    if n < 0 {
        let mut i = l.len();
        while i > 0 && (limit == 0 || removed < limit) {
            i -= 1;
            if l[i] == a[3] {
                l.remove(i);
                removed += 1;
            }
        }
    } else {
        let mut i = 0;
        while i < l.len() && (limit == 0 || removed < limit) {
            if l[i] == a[3] {
                l.remove(i);
                removed += 1;
            } else {
                i += 1;
            }
        }
    }
    ctx.drop_if_empty(&a[1]);
    Ok(Value::Integer(removed as i64))
}

fn end_arg(b: &[u8]) -> Result<End, Value> {
    if eq_ic(b, "right") {
        Ok(End::Tail)
    } else if eq_ic(b, "left") {
        Ok(End::Head)
    } else {
        Err(syntax())
    }
}

/// `lmoveGenericCommand`: pops from `src` and pushes onto `dst`.
fn lmove_generic(ctx: &mut Ctx, src: &[u8], dst: &[u8], from: End, to: End) -> Reply {
    if ctx.get_list(src)?.is_none() {
        return Ok(Value::Null);
    }
    ctx.get_list(dst)?;
    let l = ctx.get_list(src)?.expect("checked above");
    let v = pop(l, from).expect("lists are never empty");
    ctx.drop_if_empty(src);
    push(ctx.list_or_create(dst)?, v.clone(), to);
    Ok(Value::Bulk(v))
}

fn lmove(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let from = end_arg(&a[3])?;
    let to = end_arg(&a[4])?;
    lmove_generic(ctx, &a[1], &a[2], from, to)
}

fn rpoplpush(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    lmove_generic(ctx, &a[1], &a[2], End::Tail, End::Head)
}

/// `mpopGenericCommand` / `blockingPopGenericCommand`: pops from the first
/// non-empty list, or blocks. `count` is `None` for BLPOP's single-element
/// reply shape.
fn mpop(
    ctx: &mut Ctx,
    keys: &[Vec<u8>],
    end: End,
    count: Option<usize>,
    block: Option<u64>,
) -> Reply {
    for key in keys {
        if ctx.get_list(key)?.is_none() {
            continue;
        }
        let popped = pop_many(ctx, key, end, count.unwrap_or(1))?;
        return Ok(match count {
            Some(_) => Value::Array(vec![Value::bulk(key), Value::Array(popped)]),
            None => {
                let mut pair = vec![Value::bulk(key)];
                pair.extend(popped);
                Value::Array(pair)
            }
        });
    }
    match block {
        Some(deadline) if !ctx.deny_blocking => ctx.block(BlockKind::List, keys, deadline),
        _ => Ok(Value::NullArray),
    }
}

/// Parses `numkeys key... LEFT|RIGHT [COUNT n]` from `a[i]`.
fn mpop_args(a: &[Vec<u8>], i: usize) -> Result<(&[Vec<u8>], End, usize), Value> {
    let numkeys = range_long(&a[i], 1, i64::MAX, Some("numkeys should be greater than 0"))?;
    let where_idx = i as i64 + numkeys + 1;
    if where_idx >= a.len() as i64 {
        return Err(syntax());
    }
    let where_idx = where_idx as usize;
    let end = end_arg(&a[where_idx])?;
    let mut count = None;
    let mut j = where_idx + 1;
    while j < a.len() {
        if count.is_none() && eq_ic(&a[j], "count") && j + 1 < a.len() {
            j += 1;
            count = Some(range_long(&a[j], 1, i64::MAX, Some("count should be greater than 0"))?);
        } else {
            return Err(syntax());
        }
        j += 1;
    }
    Ok((&a[i + 1..where_idx], end, count.unwrap_or(1) as usize))
}

fn lmpop(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let (keys, end, count) = mpop_args(a, 1)?;
    mpop(ctx, keys, end, Some(count), None)
}

fn blmpop(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let (keys, end, count) = mpop_args(a, 2)?;
    let deadline = timeout_secs_arg(&a[1], ctx.now)?;
    mpop(ctx, keys, end, Some(count), Some(deadline))
}

fn blpop(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let deadline = timeout_secs_arg(&a[a.len() - 1], ctx.now)?;
    mpop(ctx, &a[1..a.len() - 1], End::Head, None, Some(deadline))
}

fn brpop(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let deadline = timeout_secs_arg(&a[a.len() - 1], ctx.now)?;
    mpop(ctx, &a[1..a.len() - 1], End::Tail, None, Some(deadline))
}

fn blmove_generic(ctx: &mut Ctx, a: &[Vec<u8>], from: End, to: End, deadline: u64) -> Reply {
    if ctx.get_list(&a[1])?.is_some() {
        return lmove_generic(ctx, &a[1], &a[2], from, to);
    }
    if ctx.deny_blocking {
        return Ok(Value::Null);
    }
    ctx.block(BlockKind::List, &a[1..2], deadline)
}

fn blmove(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let from = end_arg(&a[3])?;
    let to = end_arg(&a[4])?;
    let deadline = timeout_secs_arg(&a[5], ctx.now)?;
    blmove_generic(ctx, a, from, to, deadline)
}

fn brpoplpush(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let deadline = timeout_secs_arg(&a[3], ctx.now)?;
    blmove_generic(ctx, a, End::Tail, End::Head, deadline)
}
