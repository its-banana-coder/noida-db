//! Streams, ported from Redis's t_stream.c.
//!
//! Entries live in an ordered map keyed by (ms, seq). Trimming is always
//! exact: Redis's `~` only drops whole listpack nodes, which noida doesn't
//! model, so `~` trims like `=`.

use std::collections::{BTreeMap, BTreeSet};

use super::blocking::{BlockKind, timeout_ms_arg};
use super::engine::{
    Command, Ctx, Data, Entry, Reply, arity_error, cmd, container, eq_ic, help_reply, int_arg,
    syntax, wrong_type,
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
    cmd("xreadgroup", xreadgroup),
    cmd("xack", xack),
    cmd("xpending", xpending),
    cmd("xclaim", xclaim),
    cmd("xautoclaim", xautoclaim),
    container("xgroup", xgroup, XGROUP),
    container("xinfo", xinfo, XINFO),
];

static XGROUP: &[Command] = &[
    cmd("create", xgroup),
    cmd("createconsumer", xgroup),
    cmd("delconsumer", xgroup),
    cmd("destroy", xgroup),
    cmd("setid", xgroup),
    cmd("help", xgroup),
];

static XINFO: &[Command] =
    &[cmd("consumers", xinfo), cmd("groups", xinfo), cmd("stream", xinfo), cmd("help", xinfo)];

/// A stream entry ID.
pub type Id = (u64, u64);

#[derive(Clone, Debug, Default)]
pub struct Stream {
    pub entries: BTreeMap<Id, Vec<Vec<u8>>>,
    pub last_id: Id,
    pub max_deleted_id: Id,
    pub entries_added: u64,
    /// Consumer groups by name; Redis's radix tree reports them sorted.
    pub groups: BTreeMap<Vec<u8>, Group>,
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

    /// Redis's `first_id`: the smallest ID still stored, or 0-0.
    pub fn first_id(&self) -> Id {
        self.entries.keys().next().copied().unwrap_or((0, 0))
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

fn xread(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    read_generic(ctx, a, false)
}

fn xreadgroup(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    read_generic(ctx, a, true)
}

/// The ID a client asks for with `>`: everything the group hasn't handed
/// out yet.
const NEW_MESSAGES: Id = (u64::MAX, u64::MAX);

/// XREAD and XREADGROUP (`xreadCommand`).
fn read_generic(ctx: &mut Ctx, a: &[Vec<u8>], is_group: bool) -> Reply {
    let name = if is_group { "xreadgroup" } else { "xread" };
    let mut count = usize::MAX;
    let mut block: Option<i64> = None;
    let mut streams_arg = 0;
    let mut group: Option<(Vec<u8>, Vec<u8>)> = None;
    let mut noack = false;
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
                let symbol = if is_group { '>' } else { '$' };
                return Err(Value::err(format!(
                    "ERR Unbalanced '{name}' list of streams: for each stream key an ID or \
                     '{symbol}' must be specified."
                )));
            }
            break;
        } else if eq_ic(&a[i], "group") && more >= 2 {
            if !is_group {
                return Err(Value::err(
                    "ERR The GROUP option is only supported by XREADGROUP. You called XREAD instead.",
                ));
            }
            group = Some((a[i + 1].clone(), a[i + 2].clone()));
            i += 3;
        } else if eq_ic(&a[i], "noack") {
            if !is_group {
                return Err(Value::err(
                    "ERR The NOACK option is only supported by XREADGROUP. You called XREAD instead.",
                ));
            }
            noack = true;
            i += 1;
        } else {
            return Err(syntax());
        }
    }
    if streams_arg == 0 {
        return Err(syntax());
    }
    if is_group && group.is_none() {
        return Err(Value::err("ERR Missing GROUP option for XREADGROUP"));
    }
    let n = (a.len() - streams_arg) / 2;
    let keys = a[streams_arg..streams_arg + n].to_vec();
    let mut ids = Vec::with_capacity(n);
    for (k, raw) in keys.iter().zip(&a[streams_arg + n..]) {
        if let Some((gname, _)) = &group {
            let known = ctx.get_stream(k)?.is_some_and(|s| s.groups.contains_key(gname.as_slice()));
            if !known {
                return Err(Value::err(format!(
                    "NOGROUP No such key '{}' or consumer group '{}' in XREADGROUP with GROUP \
                     option",
                    String::from_utf8_lossy(k),
                    String::from_utf8_lossy(gname)
                )));
            }
        }
        ids.push(match raw.as_slice() {
            b"$" if is_group => {
                return Err(Value::err(
                    "ERR The $ ID is meaningless in the context of XREADGROUP: you want to read \
                     the history of this consumer by specifying a proper ID, or use the > ID to \
                     get new messages. The $ ID would just return an empty result set.",
                ));
            }
            b"$" => ctx.get_stream(k)?.map_or((0, 0), |s| s.last_id),
            b">" if !is_group => {
                return Err(Value::err(
                    "ERR The > ID can be specified only when calling XREADGROUP using the GROUP \
                     <group> <consumer> option.",
                ));
            }
            b">" => NEW_MESSAGES,
            raw => parse_id(raw, 0, true)?,
        });
    }

    let resp3 = ctx.resp() >= 3;
    let now = ctx.now;
    let mut out: Vec<(Value, Value)> = Vec::new();
    for (key, after) in keys.iter().zip(&ids) {
        if ctx.get_stream(key)?.is_none() {
            continue;
        }
        let Some((gname, consumer)) = group.clone() else {
            let s = ctx.get_stream(key)?.expect("exists");
            let Some(from) = next_id(*after) else { continue };
            let entries: Vec<Value> =
                s.entries.range(from..).take(count).map(|(id, f)| entry_reply(*id, f)).collect();
            if !entries.is_empty() {
                out.push((Value::bulk(key), Value::Array(entries)));
            }
            continue;
        };
        let s = ctx.get_stream(key)?.expect("exists");
        let history = *after != NEW_MESSAGES;
        let last_valid = s.last_valid_id();
        let len = s.len();
        let g = s.groups.get_mut(&gname).expect("checked above");
        g.consumer(&consumer, now).seen_time = now;
        let group_last = g.last_id;
        if history {
            let from = next_id(*after).unwrap_or(NEW_MESSAGES);
            let entries = read_history(s, &gname, &consumer, from, count, now);
            out.push((Value::bulk(key), Value::Array(entries)));
        } else if len > 0 && last_valid > group_last {
            let from = next_id(group_last).expect("not the maximum id");
            let entries = deliver_new(s, &gname, &consumer, from, count, noack, now);
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
    ctx.block_with_args(BlockKind::Stream, &keys, deadline, rewritten)
}

/// `streamReplyWithRangeFromConsumerPEL`: entries the consumer already
/// holds, with deleted ones reported as a null field list.
fn read_history(
    s: &mut Stream,
    gname: &[u8],
    consumer: &[u8],
    from: Id,
    count: usize,
    now: u64,
) -> Vec<Value> {
    let entries: BTreeMap<Id, Vec<Vec<u8>>> = s.entries.clone();
    let g = s.groups.get_mut(gname).expect("checked");
    let ids: Vec<Id> = match g.consumers.get(consumer) {
        Some(c) => c.pending.range(from..).take(count).copied().collect(),
        None => vec![],
    };
    let mut out = Vec::new();
    for id in ids {
        match entries.get(&id) {
            Some(fields) => {
                if let Some(nack) = g.pending.get_mut(&id) {
                    nack.delivery_time = now;
                    nack.delivery_count += 1;
                }
                out.push(entry_reply(id, fields));
            }
            None => out.push(Value::Array(vec![Value::bulk(fmt_id(id)), Value::NullArray])),
        }
    }
    out
}

/// `streamReplyWithRange` with a group: hands new entries to the consumer
/// and records them in the group's pending list.
fn deliver_new(
    s: &mut Stream,
    gname: &[u8],
    consumer: &[u8],
    from: Id,
    count: usize,
    noack: bool,
    now: u64,
) -> Vec<Value> {
    let picked: Vec<(Id, Vec<Vec<u8>>)> =
        s.entries.range(from..).take(count).map(|(id, f)| (*id, f.clone())).collect();
    let first_id = s.first_id();
    let entries_added = s.entries_added;
    let max_deleted = s.max_deleted_id;
    let tombstones = |id: Id| max_deleted != (0, 0) && id <= max_deleted;
    let estimate: Vec<Option<i64>> =
        picked.iter().map(|(id, _)| estimate_entries_read(s, *id)).collect();
    let g = s.groups.get_mut(gname).expect("checked");
    let mut out = Vec::new();
    for (i, (id, fields)) in picked.into_iter().enumerate() {
        if id > g.last_id {
            if g.entries_read.is_some() && g.last_id >= first_id && !tombstones(g.last_id) {
                g.entries_read = g.entries_read.map(|n| n + 1);
            } else if entries_added > 0 {
                g.entries_read = estimate[i];
            }
            g.last_id = id;
        }
        if !noack {
            g.deliver(id, consumer, now);
            g.consumer(consumer, now).active_time = now;
        }
        out.push(entry_reply(id, &fields));
    }
    out
}

/// Replaces `$` with the ID it resolved to, so a blocked XREAD rerun sees
/// only newer entries (Redis rewrites the command the same way). `>` stays
/// as it is: it always means whatever the group hasn't delivered yet.
fn rewrite_ids(a: &[Vec<u8>], streams_arg: usize, n: usize, ids: &[Id]) -> Vec<Vec<u8>> {
    let mut out = a.to_vec();
    for (j, id) in ids.iter().enumerate() {
        let pos = streams_arg + n + j;
        if out[pos] == b"$" {
            out[pos] = fmt_id(*id).into_bytes();
        }
    }
    out
}

// ---- consumer groups ----

/// A pending entry: which consumer holds it, when it was delivered and how
/// often (`streamNACK`).
#[derive(Clone, Debug)]
pub struct Nack {
    pub consumer: Vec<u8>,
    pub delivery_time: u64,
    pub delivery_count: u64,
}

#[derive(Clone, Debug, Default)]
pub struct Consumer {
    pub seen_time: u64,
    pub active_time: u64,
    pub pending: BTreeSet<Id>,
}

/// Consumers, like groups, are kept sorted by name: the order Redis's
/// radix tree reports them in.
#[derive(Clone, Debug, Default)]
pub struct Group {
    pub last_id: Id,
    /// `entries_read`, or `None` for Redis's "invalid" counter.
    pub entries_read: Option<i64>,
    pub pending: BTreeMap<Id, Nack>,
    pub consumers: BTreeMap<Vec<u8>, Consumer>,
}

impl Group {
    fn consumer(&mut self, name: &[u8], now: u64) -> &mut Consumer {
        self.consumers.entry(name.to_vec()).or_insert_with(|| Consumer {
            seen_time: now,
            active_time: now,
            ..Consumer::default()
        })
    }

    /// Hands `id` to `consumer`, taking it from whoever held it before.
    fn deliver(&mut self, id: Id, consumer: &[u8], now: u64) {
        if let Some(nack) = self.pending.get(&id) {
            let previous = nack.consumer.clone();
            if previous != consumer
                && let Some(c) = self.consumers.get_mut(&previous)
            {
                c.pending.remove(&id);
            }
        }
        self.pending.insert(
            id,
            Nack { consumer: consumer.to_vec(), delivery_time: now, delivery_count: 1 },
        );
        self.consumer(consumer, now).pending.insert(id);
    }

    /// Drops `id` from the group and its consumer's pending lists.
    fn forget(&mut self, id: Id) {
        if let Some(nack) = self.pending.remove(&id)
            && let Some(c) = self.consumers.get_mut(&nack.consumer)
        {
            c.pending.remove(&id);
        }
    }

    /// Moves `id` to `consumer`, keeping the NACK (XCLAIM/XAUTOCLAIM).
    fn reassign(&mut self, id: Id, consumer: &[u8], now: u64) {
        let previous = self.pending[&id].consumer.clone();
        if previous != consumer
            && let Some(c) = self.consumers.get_mut(&previous)
        {
            c.pending.remove(&id);
        }
        self.pending.get_mut(&id).expect("present").consumer = consumer.to_vec();
        let c = self.consumer(consumer, now);
        c.pending.insert(id);
        c.active_time = now;
    }
}

fn nogroup(key: &[u8], group: &[u8]) -> Value {
    Value::err(format!(
        "NOGROUP No such key '{}' or consumer group '{}'",
        String::from_utf8_lossy(key),
        String::from_utf8_lossy(group)
    ))
}

/// The group at `key`, or a NOGROUP error naming both.
fn group_of<'a>(ctx: &'a mut Ctx, key: &[u8], name: &[u8]) -> Result<&'a mut Group, Value> {
    let missing = nogroup(key, name);
    match ctx.get_stream(key)? {
        Some(s) => s.groups.get_mut(name).ok_or(missing),
        None => Err(missing),
    }
}

/// `streamRangeHasTombstones` for the range starting at `start`.
fn has_tombstones(s: &Stream, start: Id) -> bool {
    if s.is_empty() || s.max_deleted_id == (0, 0) {
        return false;
    }
    start <= s.max_deleted_id
}

/// `streamEstimateDistanceFromFirstEverEntry`.
fn estimate_entries_read(s: &Stream, id: Id) -> Option<i64> {
    if s.entries_added == 0 {
        return Some(0);
    }
    if s.is_empty() && id <= s.last_id {
        return Some(s.entries_added as i64);
    }
    if id != (0, 0) && id < s.max_deleted_id {
        return None;
    }
    match id.cmp(&s.last_id) {
        std::cmp::Ordering::Equal => return Some(s.entries_added as i64),
        std::cmp::Ordering::Greater => return None,
        std::cmp::Ordering::Less => {}
    }
    let first = s.first_id();
    if s.max_deleted_id == (0, 0) || s.max_deleted_id < first {
        let added = s.entries_added as i64;
        let len = s.len() as i64;
        if id < first {
            return Some(added - len);
        }
        if id == first {
            return Some(added - len + 1);
        }
    }
    None
}

/// `streamReplyWithCGLag`: how far the group is behind, if it is knowable.
fn lag_reply(s: &Stream, g: &Group) -> Value {
    let first = s.first_id();
    let lag = if s.entries_added == 0 || s.is_empty() {
        Some(0)
    } else if g.last_id < first && s.max_deleted_id < first {
        Some(s.len() as i64)
    } else if let Some(read) = g.entries_read.filter(|_| !has_tombstones(s, g.last_id)) {
        Some(s.entries_added as i64 - read)
    } else {
        estimate_entries_read(s, g.last_id).map(|read| s.entries_added as i64 - read)
    };
    lag.map_or(Value::Null, Value::Integer)
}

const XGROUP_HELP: &[&str] = &[
    "CREATE <key> <groupname> <id|$> [option]",
    "    Create a new consumer group.",
    "    Options are:",
    "    * MKSTREAM",
    "      Create the empty stream if it does not exist.",
    "    * ENTRIESREAD entries-read",
    "      Set the group's entries-read counter (internal use).",
    "CREATECONSUMER <key> <groupname> <consumer>",
    "    Create a new consumer in the specified group.",
    "DELCONSUMER <key> <groupname> <consumer>",
    "    Remove the specified consumer.",
    "DESTROY <key> <groupname>",
    "    Remove the specified group.",
    "SETID <key> <groupname> <id|$> [ENTRIESREAD entries-read]",
    "    Set the current group ID and entries-read counter.",
];

fn xgroup(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let sub = a[1].to_ascii_lowercase();
    if sub == b"help" {
        return Ok(help_reply("xgroup", XGROUP_HELP));
    }
    let (key, name) = (a[2].clone(), a[3].clone());
    // Options CREATE and SETID share.
    let mut mkstream = false;
    let mut entries_read: Option<i64> = None;
    let mut i = 5;
    while i < a.len() {
        if sub == b"create" && eq_ic(&a[i], "mkstream") {
            mkstream = true;
            i += 1;
        } else if (sub == b"create" || sub == b"setid")
            && eq_ic(&a[i], "entriesread")
            && i + 1 < a.len()
        {
            let n = int_arg(&a[i + 1])?;
            if n < 0 && n != -1 {
                return Err(Value::err("ERR value for ENTRIESREAD must be positive or -1"));
            }
            entries_read = Some(n);
            i += 2;
        } else {
            return Err(syntax());
        }
    }
    let exists = ctx.get_stream(&key)?.is_some();
    if !exists && !mkstream {
        return Err(Value::err(
            "ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may \
             want to use the MKSTREAM option to create an empty stream automatically.",
        ));
    }
    if exists
        && matches!(sub.as_slice(), b"setid" | b"createconsumer" | b"delconsumer")
        && !ctx.get_stream(&key)?.expect("exists").groups.contains_key(&name)
    {
        return Err(Value::err(format!(
            "NOGROUP No such consumer group '{}' for key name '{}'",
            String::from_utf8_lossy(&name),
            String::from_utf8_lossy(&key)
        )));
    }
    let now = ctx.now;
    match sub.as_slice() {
        b"create" => {
            let id = if a[4] == b"$" {
                ctx.get_stream(&key)?.map_or((0, 0), |s| s.last_id)
            } else {
                parse_id(&a[4], 0, true)?
            };
            if ctx.get_stream(&key)?.is_none() {
                ctx.db().insert(key.clone(), Entry::new(Data::Stream(Stream::default())));
            }
            let s = ctx.get_stream(&key)?.expect("created");
            if s.groups.contains_key(&name) {
                return Err(Value::err("BUSYGROUP Consumer Group name already exists"));
            }
            s.groups.insert(
                name,
                Group {
                    last_id: id,
                    entries_read: entries_read.filter(|n| *n != -1),
                    ..Group::default()
                },
            );
            Ok(Value::ok())
        }
        b"setid" => {
            let id = if a[4] == b"$" {
                ctx.get_stream(&key)?.map_or((0, 0), |s| s.last_id)
            } else {
                parse_id(&a[4], 0, true)?
            };
            let g = group_of(ctx, &key, &name)?;
            g.last_id = id;
            g.entries_read = entries_read.filter(|n| *n != -1);
            Ok(Value::ok())
        }
        b"destroy" => {
            let s = ctx.get_stream(&key)?.expect("checked");
            Ok(Value::Integer(s.groups.remove(&name).is_some() as i64))
        }
        b"createconsumer" => {
            let consumer = a[4].clone();
            let g = group_of(ctx, &key, &name)?;
            let created = !g.consumers.contains_key(&consumer);
            g.consumer(&consumer, now);
            Ok(Value::Integer(created as i64))
        }
        b"delconsumer" => {
            let g = group_of(ctx, &key, &name)?;
            let Some(consumer) = g.consumers.remove(&a[4]) else { return Ok(Value::Integer(0)) };
            for id in &consumer.pending {
                g.pending.remove(id);
            }
            Ok(Value::Integer(consumer.pending.len() as i64))
        }
        _ => Err(syntax()),
    }
}

fn xack(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let ids: Vec<Id> = a[3..].iter().map(|raw| parse_id(raw, 0, true)).collect::<Result<_, _>>()?;
    let Some(s) = ctx.get_stream(&a[1])? else { return Ok(Value::Integer(0)) };
    let Some(g) = s.groups.get_mut(&a[2]) else { return Ok(Value::Integer(0)) };
    let mut n = 0;
    for id in ids {
        if g.pending.contains_key(&id) {
            g.forget(id);
            n += 1;
        }
    }
    Ok(Value::Integer(n))
}

fn xpending(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    if a.len() != 3 && !(6..=9).contains(&a.len()) {
        return Err(syntax());
    }
    let now = ctx.now;
    let mut min_idle = 0;
    let mut start = (0, 0);
    let mut end = (u64::MAX, u64::MAX);
    let mut count = usize::MAX;
    let mut consumer: Option<Vec<u8>> = None;
    if a.len() >= 6 {
        let mut idx = 3;
        if eq_ic(&a[3], "idle") {
            min_idle = int_arg(&a[4])?.max(0) as u64;
            if a.len() < 8 {
                return Err(syntax());
            }
            idx += 2;
        }
        count = int_arg(&a[idx + 2])?.max(0) as usize;
        let (s_id, s_ex) = parse_range_id(&a[idx], 0)?;
        start = if s_ex {
            next_id(s_id).ok_or_else(|| Value::err("ERR invalid start ID for the interval"))?
        } else {
            s_id
        };
        let (e_id, e_ex) = parse_range_id(&a[idx + 1], u64::MAX)?;
        end = if e_ex {
            prev_id(e_id).ok_or_else(|| Value::err("ERR invalid end ID for the interval"))?
        } else {
            e_id
        };
        if idx + 3 < a.len() {
            consumer = Some(a[idx + 3].clone());
        }
    }
    let g = group_of(ctx, &a[1], &a[2])?;
    if a.len() == 3 {
        // The summary form.
        if g.pending.is_empty() {
            return Ok(Value::Array(vec![
                Value::Integer(0),
                Value::Null,
                Value::Null,
                Value::NullArray,
            ]));
        }
        let first = *g.pending.keys().next().expect("non-empty");
        let last = *g.pending.keys().next_back().expect("non-empty");
        let per_consumer: Vec<Value> = g
            .consumers
            .iter()
            .filter(|(_, c)| !c.pending.is_empty())
            .map(|(name, c)| {
                Value::Array(vec![Value::bulk(name), Value::bulk(c.pending.len().to_string())])
            })
            .collect();
        return Ok(Value::Array(vec![
            Value::Integer(g.pending.len() as i64),
            Value::bulk(fmt_id(first)),
            Value::bulk(fmt_id(last)),
            Value::Array(per_consumer),
        ]));
    }
    let ids: Vec<Id> = match &consumer {
        Some(name) => match g.consumers.get(name) {
            Some(c) => c.pending.range(start..=end).copied().collect(),
            None => return Ok(Value::Array(vec![])),
        },
        None => g.pending.range(start..=end).map(|(id, _)| *id).collect(),
    };
    let mut out = Vec::new();
    for id in ids {
        if out.len() >= count {
            break;
        }
        let nack = &g.pending[&id];
        let idle = now.saturating_sub(nack.delivery_time);
        if min_idle > 0 && idle < min_idle {
            continue;
        }
        out.push(Value::Array(vec![
            Value::bulk(fmt_id(id)),
            Value::bulk(&nack.consumer),
            Value::Integer(idle as i64),
            Value::Integer(nack.delivery_count as i64),
        ]));
    }
    Ok(Value::Array(out))
}

fn xclaim(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let now = ctx.now;
    let min_idle = int_arg(&a[4])
        .map_err(|_| Value::err("ERR Invalid min-idle-time argument for XCLAIM"))?
        .max(0) as u64;
    // The IDs run until the first argument that isn't one.
    let mut ids = Vec::new();
    let mut j = 5;
    while j < a.len() {
        match parse_id(&a[j], 0, true) {
            Ok(id) => ids.push(id),
            // An argument that isn't an ID ends the list; if it isn't a
            // valid option either, the option parser reports it.
            Err(_) => break,
        }
        j += 1;
    }
    let (mut force, mut justid) = (false, false);
    let mut retrycount: Option<u64> = None;
    let mut delivery_time: Option<u64> = None;
    let mut last_id: Option<Id> = None;
    while j < a.len() {
        let more = a.len() - 1 - j;
        if eq_ic(&a[j], "force") {
            force = true;
        } else if eq_ic(&a[j], "justid") {
            justid = true;
        } else if eq_ic(&a[j], "idle") && more > 0 {
            let idle = int_arg(&a[j + 1])
                .map_err(|_| Value::err("ERR Invalid IDLE option argument for XCLAIM"))?;
            delivery_time = Some(now.saturating_sub(idle.max(0) as u64));
            j += 1;
        } else if eq_ic(&a[j], "time") && more > 0 {
            let t = int_arg(&a[j + 1])
                .map_err(|_| Value::err("ERR Invalid TIME option argument for XCLAIM"))?;
            delivery_time = Some(t.max(0) as u64);
            j += 1;
        } else if eq_ic(&a[j], "retrycount") && more > 0 {
            retrycount = Some(
                int_arg(&a[j + 1])
                    .map_err(|_| Value::err("ERR Invalid RETRYCOUNT option argument for XCLAIM"))?
                    .max(0) as u64,
            );
            j += 1;
        } else if eq_ic(&a[j], "lastid") && more > 0 {
            last_id = Some(parse_id(&a[j + 1], 0, true)?);
            j += 1;
        } else {
            return Err(Value::err(format!(
                "ERR Unrecognized XCLAIM option '{}'",
                String::from_utf8_lossy(&a[j])
            )));
        }
        j += 1;
    }
    if ctx.get_stream(&a[1])?.is_none()
        || !ctx.get_stream(&a[1])?.expect("exists").groups.contains_key(&a[2])
    {
        return Err(nogroup(&a[1], &a[2]));
    }
    let consumer = a[3].clone();
    let s = ctx.get_stream(&a[1])?.expect("exists");
    let entries: BTreeMap<Id, Vec<Vec<u8>>> = s
        .entries
        .iter()
        .filter(|(id, _)| ids.contains(id))
        .map(|(id, f)| (*id, f.clone()))
        .collect();
    let g = s.groups.get_mut(&a[2]).expect("checked");
    if let Some(id) = last_id
        && id > g.last_id
    {
        g.last_id = id;
    }
    g.consumer(&consumer, now).seen_time = now;
    let mut out = Vec::new();
    for id in ids {
        if !entries.contains_key(&id) {
            // The entry is gone: drop any pending record of it.
            g.forget(id);
            continue;
        }
        if force && !g.pending.contains_key(&id) {
            g.pending.insert(
                id,
                Nack { consumer: consumer.clone(), delivery_time: now, delivery_count: 0 },
            );
            g.consumer(&consumer, now).pending.insert(id);
        }
        let Some(nack) = g.pending.get(&id) else { continue };
        if min_idle > 0 && now.saturating_sub(nack.delivery_time) < min_idle {
            continue;
        }
        g.reassign(id, &consumer, now);
        let nack = g.pending.get_mut(&id).expect("present");
        nack.delivery_time = delivery_time.unwrap_or(now).min(now);
        match retrycount {
            Some(n) => nack.delivery_count = n,
            None if !justid => nack.delivery_count += 1,
            None => {}
        }
        out.push(if justid { Value::bulk(fmt_id(id)) } else { entry_reply(id, &entries[&id]) });
    }
    Ok(Value::Array(out))
}

fn xautoclaim(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let now = ctx.now;
    let min_idle = int_arg(&a[4])
        .map_err(|_| Value::err("ERR Invalid min-idle-time argument for XAUTOCLAIM"))?
        .max(0) as u64;
    let (start_id, start_ex) = parse_range_id(&a[5], 0)?;
    let start = if start_ex {
        next_id(start_id).ok_or_else(|| Value::err("ERR invalid start ID for the interval"))?
    } else {
        start_id
    };
    let mut count = 100usize;
    let mut justid = false;
    let mut j = 6;
    while j < a.len() {
        let more = a.len() - 1 - j;
        if eq_ic(&a[j], "count") && more > 0 {
            let n = int_arg(&a[j + 1])?;
            if n < 1 {
                return Err(Value::err("ERR COUNT must be > 0"));
            }
            count = n as usize;
            j += 2;
        } else if eq_ic(&a[j], "justid") {
            justid = true;
            j += 1;
        } else {
            return Err(syntax());
        }
    }
    if ctx.get_stream(&a[1])?.is_none()
        || !ctx.get_stream(&a[1])?.expect("exists").groups.contains_key(&a[2])
    {
        return Err(nogroup(&a[1], &a[2]));
    }
    let consumer = a[3].clone();
    let s = ctx.get_stream(&a[1])?.expect("exists");
    let entries: BTreeMap<Id, Vec<Vec<u8>>> = s.entries.clone();
    let g = s.groups.get_mut(&a[2]).expect("checked");
    g.consumer(&consumer, now).seen_time = now;
    let candidates: Vec<Id> = g.pending.range(start..).map(|(id, _)| *id).collect();
    let mut claimed = Vec::new();
    let mut deleted = Vec::new();
    let mut cursor = (0, 0);
    let mut left = count;
    for id in candidates {
        if left == 0 {
            cursor = id;
            break;
        }
        if !entries.contains_key(&id) {
            g.forget(id);
            deleted.push(Value::bulk(fmt_id(id)));
            left -= 1;
            continue;
        }
        let nack = &g.pending[&id];
        if min_idle > 0 && now.saturating_sub(nack.delivery_time) < min_idle {
            continue;
        }
        g.reassign(id, &consumer, now);
        let nack = g.pending.get_mut(&id).expect("present");
        nack.delivery_time = now;
        if !justid {
            nack.delivery_count += 1;
        }
        claimed.push(if justid { Value::bulk(fmt_id(id)) } else { entry_reply(id, &entries[&id]) });
        left -= 1;
    }
    Ok(Value::Array(vec![
        Value::bulk(fmt_id(cursor)),
        Value::Array(claimed),
        Value::Array(deleted),
    ]))
}

const XINFO_HELP: &[&str] = &[
    "CONSUMERS <key> <groupname>",
    "    Show consumers of <groupname>.",
    "GROUPS <key>",
    "    Show the stream consumer groups.",
    "STREAM <key> [FULL [COUNT <count>]",
    "    Show information about the stream.",
];

fn xinfo(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    if eq_ic(&a[1], "help") {
        return Ok(help_reply("xinfo", XINFO_HELP));
    }
    let now = ctx.now;
    let Some(s) = ctx.get_stream(&a[2])?.cloned() else {
        return Err(Value::err("ERR no such key"));
    };
    let field = |k: &str, v: Value| (Value::bulk(k), v);
    if eq_ic(&a[1], "groups") {
        let groups: Vec<Value> = s
            .groups
            .iter()
            .map(|(name, g)| {
                Value::Map(vec![
                    field("name", Value::bulk(name)),
                    field("consumers", Value::Integer(g.consumers.len() as i64)),
                    field("pending", Value::Integer(g.pending.len() as i64)),
                    field("last-delivered-id", Value::bulk(fmt_id(g.last_id))),
                    field("entries-read", g.entries_read.map_or(Value::Null, Value::Integer)),
                    field("lag", lag_reply(&s, g)),
                ])
            })
            .collect();
        return Ok(Value::Array(groups));
    }
    if eq_ic(&a[1], "consumers") {
        let Some(g) = s.groups.get(&a[3]) else {
            return Err(Value::err(format!(
                "NOGROUP No such consumer group '{}' for key name '{}'",
                String::from_utf8_lossy(&a[3]),
                String::from_utf8_lossy(&a[2])
            )));
        };
        let consumers: Vec<Value> = g
            .consumers
            .iter()
            .map(|(name, c)| {
                Value::Map(vec![
                    field("name", Value::bulk(name)),
                    field("pending", Value::Integer(c.pending.len() as i64)),
                    field("idle", Value::Integer(now.saturating_sub(c.seen_time) as i64)),
                    field("inactive", Value::Integer(now.saturating_sub(c.active_time) as i64)),
                ])
            })
            .collect();
        return Ok(Value::Array(consumers));
    }
    if !eq_ic(&a[1], "stream") {
        return Err(syntax());
    }
    // Fields every XINFO STREAM form starts with.
    let mut out = vec![
        field("length", Value::Integer(s.len() as i64)),
        // noida has no radix tree; it reports what a small stream looks
        // like in Redis.
        field("radix-tree-keys", Value::Integer(s.len().min(1) as i64)),
        field("radix-tree-nodes", Value::Integer(s.len().min(1) as i64 + 1)),
        field("last-generated-id", Value::bulk(fmt_id(s.last_id))),
        field("max-deleted-entry-id", Value::bulk(fmt_id(s.max_deleted_id))),
        field("entries-added", Value::Integer(s.entries_added as i64)),
        field("recorded-first-entry-id", Value::bulk(fmt_id(s.first_id()))),
    ];
    if a.len() == 3 {
        let entry_or_null = |e: Option<(&Id, &Vec<Vec<u8>>)>| match e {
            Some((id, f)) => entry_reply(*id, f),
            None => Value::Null,
        };
        out.push(field("groups", Value::Integer(s.groups.len() as i64)));
        out.push(field("first-entry", entry_or_null(s.entries.iter().next())));
        out.push(field("last-entry", entry_or_null(s.entries.iter().next_back())));
        return Ok(Value::Map(out));
    }
    // XINFO STREAM <key> FULL [COUNT <count>]
    if !eq_ic(&a[3], "full") || (a.len() != 4 && a.len() != 6) {
        return Err(syntax());
    }
    let mut count = 10usize;
    if a.len() == 6 {
        if !eq_ic(&a[4], "count") {
            return Err(syntax());
        }
        let n = int_arg(&a[5])?;
        count = if n < 0 { 10 } else { n as usize };
    }
    // COUNT 0 means "everything", as it does elsewhere in streams.
    let limit = if count == 0 { usize::MAX } else { count };
    let entries: Vec<Value> =
        s.entries.iter().take(limit).map(|(id, f)| entry_reply(*id, f)).collect();
    out.push(field("entries", Value::Array(entries)));
    let groups: Vec<Value> = s
        .groups
        .iter()
        .map(|(name, g)| {
            let pending: Vec<Value> = g
                .pending
                .iter()
                .take(limit)
                .map(|(id, nack)| {
                    Value::Array(vec![
                        Value::bulk(fmt_id(*id)),
                        Value::bulk(&nack.consumer),
                        Value::Integer(nack.delivery_time as i64),
                        Value::Integer(nack.delivery_count as i64),
                    ])
                })
                .collect();
            let consumers: Vec<Value> = g
                .consumers
                .iter()
                .map(|(cname, c)| {
                    let cpending: Vec<Value> = c
                        .pending
                        .iter()
                        .take(limit)
                        .map(|id| {
                            let nack = &g.pending[id];
                            Value::Array(vec![
                                Value::bulk(fmt_id(*id)),
                                Value::Integer(nack.delivery_time as i64),
                                Value::Integer(nack.delivery_count as i64),
                            ])
                        })
                        .collect();
                    Value::Map(vec![
                        field("name", Value::bulk(cname)),
                        field("seen-time", Value::Integer(c.seen_time as i64)),
                        field("active-time", Value::Integer(c.active_time as i64)),
                        field("pel-count", Value::Integer(c.pending.len() as i64)),
                        field("pending", Value::Array(cpending)),
                    ])
                })
                .collect();
            Value::Map(vec![
                field("name", Value::bulk(name)),
                field("last-delivered-id", Value::bulk(fmt_id(g.last_id))),
                field("entries-read", g.entries_read.map_or(Value::Null, Value::Integer)),
                field("lag", lag_reply(&s, g)),
                field("pel-count", Value::Integer(g.pending.len() as i64)),
                field("pending", Value::Array(pending)),
                field("consumers", Value::Array(consumers)),
            ])
        })
        .collect();
    out.push(field("groups", Value::Array(groups)));
    Ok(Value::Map(out))
}
