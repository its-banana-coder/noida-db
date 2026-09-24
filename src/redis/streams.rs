//! Streams, ported from Redis's t_stream.c.
//!
//! Entries live in an ordered map keyed by (ms, seq). Trimming is always
//! exact: Redis's `~` only drops whole listpack nodes, which noida doesn't
//! model, so `~` trims like `=`.

use std::collections::BTreeMap;

use super::blocking::{BlockKind, timeout_ms_arg};
use super::engine::{
    Command, Ctx, Data, Entry, Reply, arity_error, cmd, eq_ic, int_arg, syntax, wrong_type,
};
use super::resp::Value;

pub static COMMANDS: &[Command] = &[
    cmd("xadd", xadd),
    cmd("xlen", xlen),
    cmd("xrange", xrange),
    cmd("xrevrange", xrevrange),
    cmd("xdel", xdel),
    cmd("xtrim", xtrim),
    cmd("xsetid", xsetid),
    cmd("xread", xread),
];

/// A stream entry ID.
pub type Id = (u64, u64);

#[derive(Clone, Debug, Default)]
pub struct Stream {
    pub entries: BTreeMap<Id, Vec<Vec<u8>>>,
    pub last_id: Id,
    pub max_deleted_id: Id,
    pub entries_added: u64,
}

impl Stream {
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The largest ID in the stream, or 0-0 (`streamLastValidID`).
    pub fn last_valid_id(&self) -> Id {
        self.entries.keys().next_back().copied().unwrap_or((0, 0))
    }
}

fn fmt_id(id: Id) -> String {
    format!("{}-{}", id.0, id.1)
}

fn invalid_id() -> Value {
    Value::err("ERR Invalid stream ID specified as stream command argument")
}

/// `streamGenericParseID`. `strict` refuses `-` and `+`; `missing_seq` is
/// what an ID without a sequence means.
fn parse_id(raw: &[u8], missing_seq: u64, strict: bool) -> Result<Id, Value> {
    if raw.is_empty() {
        return Err(invalid_id());
    }
    if !strict && raw.len() == 1 {
        match raw[0] {
            b'-' => return Ok((0, 0)),
            b'+' => return Ok((u64::MAX, u64::MAX)),
            _ => {}
        }
    }
    let s = std::str::from_utf8(raw).map_err(|_| invalid_id())?;
    let (ms, seq) = match s.split_once('-') {
        Some((ms, seq)) => {
            let seq = if seq == "*" {
                return Err(invalid_id());
            } else {
                seq.parse::<u64>().map_err(|_| invalid_id())?
            };
            (ms, seq)
        }
        None => (s, missing_seq),
    };
    Ok((ms.parse::<u64>().map_err(|_| invalid_id())?, seq))
}

/// A range endpoint, which may be exclusive (`(`).
fn parse_range_id(raw: &[u8], missing_seq: u64) -> Result<(Id, bool), Value> {
    if raw.len() > 1 && raw[0] == b'(' {
        return Ok((parse_id(&raw[1..], missing_seq, true)?, true));
    }
    Ok((parse_id(raw, missing_seq, false)?, false))
}

fn next_id(id: Id) -> Option<Id> {
    match id {
        (u64::MAX, u64::MAX) => None,
        (ms, u64::MAX) => Some((ms + 1, 0)),
        (ms, seq) => Some((ms, seq + 1)),
    }
}

fn prev_id(id: Id) -> Option<Id> {
    match id {
        (0, 0) => None,
        (ms, 0) => Some((ms - 1, u64::MAX)),
        (ms, seq) => Some((ms, seq - 1)),
    }
}

impl Ctx<'_> {
    /// The stream at `key`, `None` if missing, WRONGTYPE otherwise.
    pub fn get_stream(&mut self, key: &[u8]) -> Result<Option<&mut Stream>, Value> {
        match self.lookup(key) {
            None => Ok(None),
            Some(Entry { data: Data::Stream(s), .. }) => Ok(Some(s)),
            Some(_) => Err(wrong_type()),
        }
    }
}

/// How XADD and XTRIM were told to trim.
#[derive(Clone, Copy, PartialEq)]
enum Trim {
    None,
    MaxLen(i64),
    MinId(Id),
}

struct AddTrimArgs {
    trim: Trim,
    limit_given: bool,
    approx: bool,
    no_mkstream: bool,
    /// The explicit ID, and whether its sequence was given.
    id: Option<(Id, bool)>,
    /// The index just past the parsed options.
    next: usize,
}

/// `streamParseAddOrTrimArgsOrReply`.
fn parse_add_trim(a: &[Vec<u8>], xadd: bool) -> Result<AddTrimArgs, Value> {
    let mut args = AddTrimArgs {
        trim: Trim::None,
        limit_given: false,
        approx: false,
        no_mkstream: false,
        id: None,
        next: a.len(),
    };
    let incompatible = || {
        Value::err("ERR syntax error, MAXLEN and MINID options at the same time are not compatible")
    };
    let mut i = 2;
    while i < a.len() {
        let more = a.len() - 1 - i;
        if xadd && a[i] == b"*" {
            break;
        } else if eq_ic(&a[i], "maxlen") && more > 0 {
            if args.trim != Trim::None {
                return Err(incompatible());
            }
            args.approx = false;
            if more >= 2 && (a[i + 1] == b"~" || a[i + 1] == b"=") {
                args.approx = a[i + 1] == b"~";
                i += 1;
            }
            let n = int_arg(&a[i + 1])?;
            if n < 0 {
                return Err(Value::err("ERR The MAXLEN argument must be >= 0."));
            }
            args.trim = Trim::MaxLen(n);
            i += 1;
        } else if eq_ic(&a[i], "minid") && more > 0 {
            if args.trim != Trim::None {
                return Err(incompatible());
            }
            args.approx = false;
            if more >= 2 && (a[i + 1] == b"~" || a[i + 1] == b"=") {
                args.approx = a[i + 1] == b"~";
                i += 1;
            }
            args.trim = Trim::MinId(parse_id(&a[i + 1], 0, true)?);
            i += 1;
        } else if eq_ic(&a[i], "limit") && more > 0 {
            if int_arg(&a[i + 1])? < 0 {
                return Err(Value::err("ERR The LIMIT argument must be >= 0."));
            }
            args.limit_given = true;
            i += 1;
        } else if xadd && eq_ic(&a[i], "nomkstream") {
            args.no_mkstream = true;
        } else if xadd {
            let seq_given = !a[i].ends_with(b"-*");
            let id = if seq_given {
                parse_id(&a[i], 0, true)?
            } else {
                let ms = std::str::from_utf8(&a[i][..a[i].len() - 2])
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
                    .ok_or_else(invalid_id)?;
                (ms, 0)
            };
            args.id = Some((id, seq_given));
            break;
        } else {
            return Err(syntax());
        }
        i += 1;
    }
    args.next = i;
    if args.limit_given && args.trim == Trim::None {
        return Err(Value::err(
            "ERR syntax error, LIMIT cannot be used without specifying a trimming strategy",
        ));
    }
    if !xadd && args.trim == Trim::None {
        return Err(Value::err("ERR syntax error, XTRIM must be called with a trimming strategy"));
    }
    if args.limit_given && !args.approx {
        return Err(Value::err(
            "ERR syntax error, LIMIT cannot be used without the special ~ option",
        ));
    }
    Ok(args)
}

/// `streamTrim`, always exact.
fn trim(s: &mut Stream, how: Trim) -> usize {
    let victims: Vec<Id> = match how {
        Trim::None => vec![],
        Trim::MaxLen(n) => {
            let extra = s.entries.len().saturating_sub(n.max(0) as usize);
            s.entries.keys().take(extra).copied().collect()
        }
        Trim::MinId(min) => s.entries.range(..min).map(|(id, _)| *id).collect(),
    };
    for id in &victims {
        s.entries.remove(id);
        if *id > s.max_deleted_id {
            s.max_deleted_id = *id;
        }
    }
    victims.len()
}

fn xadd(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let args = parse_add_trim(a, true)?;
    let field_pos = args.next + 1;
    if a.len() < field_pos + 2 || !(a.len() - field_pos).is_multiple_of(2) {
        return Err(arity_error("xadd"));
    }
    if let Some(((0, 0), true)) = args.id {
        return Err(Value::err("ERR The ID specified in XADD must be greater than 0-0"));
    }
    let now = ctx.now;
    if ctx.get_stream(&a[1])?.is_none() {
        if args.no_mkstream {
            return Ok(Value::Null);
        }
        ctx.db().insert(a[1].clone(), Entry::new(Data::Stream(Stream::default())));
    }
    let s = ctx.get_stream(&a[1])?.expect("created");
    if s.last_id == (u64::MAX, u64::MAX) {
        return Err(Value::err(
            "ERR The stream has exhausted the last possible ID, unable to add more items",
        ));
    }
    // `streamAppendItem`: pick the ID, then check it grows.
    let id = match args.id {
        Some((id, true)) => id,
        Some(((ms, _), false)) => {
            if ms < s.last_id.0 {
                return Err(too_small());
            }
            if ms == s.last_id.0 {
                let Some(next) = next_id(s.last_id) else { return Err(too_small()) };
                next
            } else {
                (ms, 0)
            }
        }
        None => {
            if now > s.last_id.0 {
                (now, 0)
            } else {
                let Some(next) = next_id(s.last_id) else { return Err(too_small()) };
                next
            }
        }
    };
    if id <= s.last_id && !(s.last_id == (0, 0) && s.entries_added == 0 && id > (0, 0)) {
        return Err(too_small());
    }
    s.entries.insert(id, a[field_pos..].to_vec());
    s.last_id = id;
    s.entries_added += 1;
    trim(s, args.trim);
    let db = ctx.db_index();
    ctx.engine.signal_ready(db, &a[1]);
    Ok(Value::bulk(fmt_id(id)))
}

fn too_small() -> Value {
    Value::err("ERR The ID specified in XADD is equal or smaller than the target stream top item")
}

fn xlen(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    Ok(Value::Integer(ctx.get_stream(&a[1])?.map_or(0, |s| s.len()) as i64))
}

/// One entry as `[id, [field, value, ...]]`.
fn entry_reply(id: Id, fields: &[Vec<u8>]) -> Value {
    Value::Array(vec![
        Value::bulk(fmt_id(id)),
        Value::Array(fields.iter().map(Value::bulk).collect()),
    ])
}

fn range_generic(ctx: &mut Ctx, a: &[Vec<u8>], rev: bool) -> Reply {
    let (start_raw, end_raw) = if rev { (&a[3], &a[2]) } else { (&a[2], &a[3]) };
    let (mut start, start_ex) = parse_range_id(start_raw, 0)?;
    let (mut end, end_ex) = parse_range_id(end_raw, u64::MAX)?;
    let mut count = usize::MAX;
    if a.len() > 4 {
        if a.len() != 6 || !eq_ic(&a[4], "count") {
            return Err(syntax());
        }
        count = int_arg(&a[5])?.max(0) as usize;
        if count == 0 {
            return Ok(Value::NullArray);
        }
    }
    if start_ex {
        let Some(next) = next_id(start) else {
            return Err(Value::err("ERR invalid start ID for the interval"));
        };
        start = next;
    }
    if end_ex {
        let Some(prev) = prev_id(end) else {
            return Err(Value::err("ERR invalid end ID for the interval"));
        };
        end = prev;
    }
    let Some(s) = ctx.get_stream(&a[1])? else { return Ok(Value::Array(vec![])) };
    if start > end {
        return Ok(Value::Array(vec![]));
    }
    let picked: Vec<Value> = if rev {
        s.entries.range(start..=end).rev().take(count).map(|(id, f)| entry_reply(*id, f)).collect()
    } else {
        s.entries.range(start..=end).take(count).map(|(id, f)| entry_reply(*id, f)).collect()
    };
    Ok(Value::Array(picked))
}

fn xrange(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    range_generic(ctx, a, false)
}

fn xrevrange(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    range_generic(ctx, a, true)
}

fn xdel(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let ids: Vec<Id> = a[2..].iter().map(|raw| parse_id(raw, 0, true)).collect::<Result<_, _>>()?;
    let Some(s) = ctx.get_stream(&a[1])? else { return Ok(Value::Integer(0)) };
    let mut n = 0;
    for id in ids {
        if s.entries.remove(&id).is_some() {
            n += 1;
            if id > s.max_deleted_id {
                s.max_deleted_id = id;
            }
        }
    }
    Ok(Value::Integer(n))
}

fn xtrim(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let args = parse_add_trim(a, false)?;
    if args.next != a.len() {
        return Err(syntax());
    }
    let Some(s) = ctx.get_stream(&a[1])? else { return Ok(Value::Integer(0)) };
    Ok(Value::Integer(trim(s, args.trim) as i64))
}

fn xsetid(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let id = parse_id(&a[2], 0, true)?;
    let mut entries_added: Option<i64> = None;
    let mut max_deleted: Option<Id> = None;
    let mut i = 3;
    while i < a.len() {
        if eq_ic(&a[i], "entriesadded") && i + 1 < a.len() {
            let n = int_arg(&a[i + 1])?;
            if n < 0 {
                return Err(Value::err("ERR entries_added must be positive"));
            }
            entries_added = Some(n);
            i += 2;
        } else if eq_ic(&a[i], "maxdeletedid") && i + 1 < a.len() {
            max_deleted = Some(parse_id(&a[i + 1], 0, true)?);
            i += 2;
        } else {
            return Err(syntax());
        }
    }
    if let Some(max) = max_deleted
        && id < max
    {
        return Err(Value::err(
            "ERR The ID specified in XSETID is smaller than the provided max_deleted_entry_id",
        ));
    }
    let Some(s) = ctx.get_stream(&a[1])? else {
        return Err(Value::err("ERR no such key"));
    };
    if let Some(&last) = s.entries.keys().next_back() {
        if id < last {
            return Err(Value::err(
                "ERR The ID specified in XSETID is smaller than the target stream top item",
            ));
        }
        if entries_added.is_some_and(|n| (n as usize) < s.len()) {
            return Err(Value::err(
                "ERR The entries_added specified in XSETID is smaller than the target stream length",
            ));
        }
    }
    if let Some(max) = max_deleted {
        s.max_deleted_id = max;
    } else if id < s.max_deleted_id {
        return Err(Value::err(
            "ERR The ID specified in XSETID is smaller than current max_deleted_entry_id",
        ));
    }
    s.last_id = id;
    if let Some(n) = entries_added {
        s.entries_added = n as u64;
    }
    Ok(Value::ok())
}

/// XREAD, and the shared part of XREADGROUP.
fn xread(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let mut count = usize::MAX;
    let mut block: Option<i64> = None;
    let mut streams_arg = 0;
    let mut i = 1;
    while i < a.len() {
        let more = a.len() - i - 1;
        if eq_ic(&a[i], "block") && more > 0 {
            block = Some(
                int_arg(&a[i + 1])
                    .map_err(|_| Value::err("ERR timeout is not an integer or out of range"))?,
            );
            i += 2;
        } else if eq_ic(&a[i], "count") && more > 0 {
            count = int_arg(&a[i + 1])?.max(0) as usize;
            i += 2;
        } else if eq_ic(&a[i], "streams") && more > 0 {
            streams_arg = i + 1;
            if !(a.len() - streams_arg).is_multiple_of(2) {
                return Err(Value::err(
                    "ERR Unbalanced 'xread' list of streams: for each stream key an ID or '$' must be specified.",
                ));
            }
            break;
        } else if eq_ic(&a[i], "group") && more >= 2 {
            return Err(Value::err(
                "ERR The GROUP option is only supported by XREADGROUP. You called XREAD instead.",
            ));
        } else if eq_ic(&a[i], "noack") {
            return Err(Value::err(
                "ERR The NOACK option is only supported by XREADGROUP. You called XREAD instead.",
            ));
        } else {
            return Err(syntax());
        }
    }
    if streams_arg == 0 {
        return Err(syntax());
    }
    let n = (a.len() - streams_arg) / 2;
    let keys = &a[streams_arg..streams_arg + n];
    let mut ids = Vec::with_capacity(n);
    for (k, raw) in keys.iter().zip(&a[streams_arg + n..]) {
        if raw.as_slice() == b">" {
            return Err(Value::err(
                "ERR The > ID can be specified only when calling XREADGROUP using the GROUP <group> <consumer> option.",
            ));
        }
        ids.push(if raw.as_slice() == b"$" {
            ctx.get_stream(k)?.map_or((0, 0), |s| s.last_id)
        } else {
            parse_id(raw, 0, true)?
        });
    }

    let resp3 = ctx.resp() >= 3;
    let mut out: Vec<(Value, Value)> = Vec::new();
    for (key, after) in keys.iter().zip(&ids) {
        let Some(s) = ctx.get_stream(key)? else { continue };
        let Some(from) = next_id(*after) else { continue };
        let entries: Vec<Value> =
            s.entries.range(from..).take(count).map(|(id, f)| entry_reply(*id, f)).collect();
        if !entries.is_empty() {
            out.push((Value::bulk(key), Value::Array(entries)));
        }
    }
    if !out.is_empty() {
        return Ok(if resp3 {
            Value::Map(out)
        } else {
            Value::Array(out.into_iter().map(|(k, v)| Value::Array(vec![k, v])).collect())
        });
    }
    let Some(ms) = block else { return Ok(Value::NullArray) };
    if ctx.deny_blocking {
        return Ok(Value::NullArray);
    }
    // Block on the keys, remembering the IDs already seen: a waiter is
    // served by entries added after them.
    let deadline = timeout_ms_arg(ms, ctx.now)?;
    let rewritten = rewrite_ids(a, streams_arg, n, &ids);
    ctx.block_with_args(BlockKind::Stream, keys, deadline, rewritten)
}

/// Replaces `$` with the ID it resolved to, so a blocked XREAD rerun sees
/// only newer entries (Redis rewrites the command the same way).
fn rewrite_ids(a: &[Vec<u8>], streams_arg: usize, n: usize, ids: &[Id]) -> Vec<Vec<u8>> {
    let mut out = a.to_vec();
    for (j, id) in ids.iter().enumerate() {
        out[streams_arg + n + j] = fmt_id(*id).into_bytes();
    }
    out
}
