//! Sorted set commands, ported from Redis's t_zset.c.
//!
//! Members are ordered by (score, member bytes) whatever the encoding, so
//! a sorted Vec plus a score index is enough. The listpack/skiplist
//! distinction is tracked only for OBJECT ENCODING.

use std::cmp::Ordering;
use std::collections::HashMap;

use super::blocking::{BlockKind, timeout_secs_arg};
use super::double::parse_double;
use super::engine::{
    Command, Ctx, Data, Entry, Reply, arity_error, cmd, eq_ic, long_arg, positive_long, range_long,
    syntax, wrong_type,
};
use super::keys::parse_scan;
use super::resp::Value;

pub static COMMANDS: &[Command] = &[
    cmd("zadd", zadd),
    cmd("zincrby", zincrby),
    cmd("zrem", zrem),
    cmd("zcard", zcard),
    cmd("zscore", zscore),
    cmd("zmscore", zmscore),
    cmd("zrank", zrank),
    cmd("zrevrank", zrevrank),
    cmd("zcount", zcount),
    cmd("zlexcount", zlexcount),
    cmd("zrange", zrange),
    cmd("zrangestore", zrangestore),
    cmd("zrevrange", zrevrange),
    cmd("zrangebyscore", zrangebyscore),
    cmd("zrevrangebyscore", zrevrangebyscore),
    cmd("zrangebylex", zrangebylex),
    cmd("zrevrangebylex", zrevrangebylex),
    cmd("zremrangebyrank", zremrangebyrank),
    cmd("zremrangebyscore", zremrangebyscore),
    cmd("zremrangebylex", zremrangebylex),
    cmd("zpopmin", zpopmin),
    cmd("zpopmax", zpopmax),
    cmd("bzpopmin", bzpopmin),
    cmd("bzpopmax", bzpopmax),
    cmd("zmpop", zmpop),
    cmd("bzmpop", bzmpop),
    cmd("zrandmember", zrandmember),
    cmd("zscan", zscan),
    cmd("zunion", zunion),
    cmd("zinter", zinter),
    cmd("zdiff", zdiff),
    cmd("zunionstore", zunionstore),
    cmd("zinterstore", zinterstore),
    cmd("zdiffstore", zdiffstore),
    cmd("zintercard", zintercard),
];

/// `zset-max-listpack-entries` / `zset-max-listpack-value` defaults.
const MAX_LISTPACK: usize = 128;
const MAX_LISTPACK_VALUE: usize = 64;

#[derive(Clone, Debug, Default)]
pub struct Zset {
    /// Sorted by (score, member).
    items: Vec<(f64, Vec<u8>)>,
    scores: HashMap<Vec<u8>, f64>,
    /// Converted to a skiplist; like Redis, never converts back by itself.
    big: bool,
}

fn cmp(a: (f64, &[u8]), b: (f64, &[u8])) -> Ordering {
    a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal).then_with(|| a.1.cmp(b.1))
}

impl Zset {
    /// `zsetTypeCreate`.
    fn create(size_hint: usize, value_len: usize) -> Zset {
        Zset { big: size_hint > MAX_LISTPACK || value_len > MAX_LISTPACK_VALUE, ..Zset::default() }
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn encoding(&self) -> &'static str {
        if self.big { "skiplist" } else { "listpack" }
    }

    pub fn score(&self, m: &[u8]) -> Option<f64> {
        self.scores.get(m).copied()
    }

    fn pos(&self, score: f64, m: &[u8]) -> Result<usize, usize> {
        self.items.binary_search_by(|(s, x)| cmp((*s, x), (score, m)))
    }

    /// Inserts or moves `m` to `score`. Returns true if `m` was new.
    pub fn insert(&mut self, m: &[u8], score: f64) -> bool {
        let new = match self.scores.get(m) {
            Some(&old) => {
                if old == score {
                    return false;
                }
                let i = self.pos(old, m).expect("indexed");
                self.items.remove(i);
                false
            }
            None => true,
        };
        if new && (self.items.len() + 1 > MAX_LISTPACK || m.len() > MAX_LISTPACK_VALUE) {
            self.big = true;
        }
        // A listpack stores the score as text, and "-0" comes back as the
        // integer 0.
        let score = if !self.big && score == 0.0 { 0.0 } else { score };
        let i = self.pos(score, m).unwrap_or_else(|i| i);
        self.items.insert(i, (score, m.to_vec()));
        self.scores.insert(m.to_vec(), score);
        new
    }

    pub fn remove(&mut self, m: &[u8]) -> bool {
        let Some(score) = self.scores.remove(m) else { return false };
        let i = self.pos(score, m).expect("indexed");
        self.items.remove(i);
        true
    }

    fn rank(&self, m: &[u8]) -> Option<usize> {
        let score = self.score(m)?;
        self.pos(score, m).ok()
    }

    /// `zsetConvertToListpackIfNeeded` for stored results.
    fn shrink_if_small(&mut self) {
        let max_len = self.items.iter().map(|(_, m)| m.len()).max().unwrap_or(0);
        if self.big && self.len() <= MAX_LISTPACK && max_len <= MAX_LISTPACK_VALUE {
            self.big = false;
        }
    }
}

impl Ctx<'_> {
    /// The sorted set at `key`, `None` if missing, WRONGTYPE otherwise.
    pub fn get_zset(&mut self, key: &[u8]) -> Result<Option<&mut Zset>, Value> {
        match self.lookup(key) {
            None => Ok(None),
            Some(Entry { data: Data::Zset(z), .. }) => Ok(Some(z)),
            Some(_) => Err(wrong_type()),
        }
    }

    fn store_zset(&mut self, key: &[u8], z: Zset) -> Reply {
        let now = self.now;
        let len = z.len();
        self.db().remove(key, now);
        if len > 0 {
            self.db().insert(key.to_vec(), Entry::new(Data::Zset(z)));
        }
        Ok(Value::Integer(len as i64))
    }
}

fn not_float() -> Value {
    Value::err("ERR value is not a valid float")
}

/// Members with scores, the way Redis emits them: a flat list on RESP2,
/// [member, score] pairs on RESP3.
fn with_scores(items: &[(f64, Vec<u8>)], scores: bool, resp3: bool) -> Value {
    let mut out = Vec::new();
    for (s, m) in items {
        match (scores, resp3) {
            (false, _) => out.push(Value::bulk(m)),
            (true, true) => out.push(Value::Array(vec![Value::bulk(m), Value::Double(*s)])),
            (true, false) => {
                out.push(Value::bulk(m));
                out.push(Value::Double(*s));
            }
        }
    }
    Value::Array(out)
}

// ---- ZADD / ZINCRBY ----

#[derive(Default)]
struct AddFlags {
    nx: bool,
    xx: bool,
    gt: bool,
    lt: bool,
    incr: bool,
}

fn zadd_generic(ctx: &mut Ctx, a: &[Vec<u8>], mut f: AddFlags) -> Reply {
    let mut ch = false;
    let mut i = 2;
    while i < a.len() {
        match a[i].to_ascii_lowercase().as_slice() {
            b"nx" => f.nx = true,
            b"xx" => f.xx = true,
            b"ch" => ch = true,
            b"incr" => f.incr = true,
            b"gt" => f.gt = true,
            b"lt" => f.lt = true,
            _ => break,
        }
        i += 1;
    }
    let rest = &a[i..];
    if rest.is_empty() || rest.len() % 2 == 1 {
        return Err(syntax());
    }
    if f.nx && f.xx {
        return Err(Value::err("ERR XX and NX options at the same time are not compatible"));
    }
    if (f.gt && f.nx) || (f.lt && f.nx) || (f.gt && f.lt) {
        return Err(Value::err(
            "ERR GT, LT, and/or NX options at the same time are not compatible",
        ));
    }
    if f.incr && rest.len() > 2 {
        return Err(Value::err("ERR INCR option supports a single increment-element pair"));
    }
    let pairs: Vec<(f64, &Vec<u8>)> = rest
        .chunks(2)
        .map(|p| parse_double(&p[0]).map(|s| (s, &p[1])).ok_or_else(not_float))
        .collect::<Result<_, _>>()?;

    let (mut added, mut updated, mut processed) = (0, 0, 0);
    let mut last = 0.0;
    match ctx.get_zset(&a[1])? {
        Some(z) => {
            if pairs.len() > MAX_LISTPACK {
                z.big = true;
            }
        }
        None if f.xx => {}
        None => {
            let z = Zset::create(pairs.len(), pairs[0].1.len());
            ctx.db().insert(a[1].clone(), Entry::new(Data::Zset(z)));
        }
    }
    if let Some(z) = ctx.get_zset(&a[1])? {
        for (score, m) in pairs {
            let mut score = score;
            match z.score(m) {
                Some(cur) => {
                    if f.nx {
                        continue;
                    }
                    if f.incr {
                        score += cur;
                        if score.is_nan() {
                            return Err(Value::err("ERR resulting score is not a number (NaN)"));
                        }
                    }
                    if (f.lt && score >= cur) || (f.gt && score <= cur) {
                        continue;
                    }
                    if score != cur {
                        z.insert(m, score);
                        updated += 1;
                    }
                }
                None if f.xx => continue,
                None => {
                    z.insert(m, score);
                    added += 1;
                }
            }
            processed += 1;
            last = score;
        }
    }
    if f.incr {
        return Ok(if processed > 0 { Value::Double(last) } else { Value::Null });
    }
    Ok(Value::Integer(if ch { added + updated } else { added }))
}

fn zadd(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    zadd_generic(ctx, a, AddFlags::default())
}

fn zincrby(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    zadd_generic(ctx, a, AddFlags { incr: true, ..AddFlags::default() })
}

fn zrem(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let Some(z) = ctx.get_zset(&a[1])? else { return Ok(Value::Integer(0)) };
    let mut n = 0;
    for m in &a[2..] {
        if z.remove(m) {
            n += 1;
        }
        if z.is_empty() {
            break;
        }
    }
    ctx.drop_if_empty(&a[1]);
    Ok(Value::Integer(n))
}

fn zcard(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    Ok(Value::Integer(ctx.get_zset(&a[1])?.map_or(0, |z| z.len()) as i64))
}

fn zscore(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    Ok(ctx.get_zset(&a[1])?.and_then(|z| z.score(&a[2])).map_or(Value::Null, Value::Double))
}

fn zmscore(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let z = ctx.get_zset(&a[1])?;
    Ok(Value::Array(
        a[2..]
            .iter()
            .map(|m| z.as_ref().and_then(|z| z.score(m)).map_or(Value::Null, Value::Double))
            .collect(),
    ))
}

fn zrank_generic(ctx: &mut Ctx, a: &[Vec<u8>], rev: bool, name: &str) -> Reply {
    if a.len() > 4 {
        return Err(arity_error(name));
    }
    let with = a.len() == 4;
    if with && !eq_ic(&a[3], "withscore") {
        return Err(syntax());
    }
    let missing = if with { Value::NullArray } else { Value::Null };
    let Some(z) = ctx.get_zset(&a[1])? else { return Ok(missing) };
    let Some(r) = z.rank(&a[2]) else { return Ok(missing) };
    let r = if rev { z.len() - 1 - r } else { r } as i64;
    Ok(if with {
        Value::Array(vec![Value::Integer(r), Value::Double(z.score(&a[2]).unwrap())])
    } else {
        Value::Integer(r)
    })
}

fn zrank(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    zrank_generic(ctx, a, false, "zrank")
}

fn zrevrank(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    zrank_generic(ctx, a, true, "zrevrank")
}

// ---- ranges ----

/// `zrangespec`: a score interval.
struct ScoreRange {
    min: f64,
    max: f64,
    minex: bool,
    maxex: bool,
}

impl ScoreRange {
    fn parse(min: &[u8], max: &[u8]) -> Result<ScoreRange, Value> {
        let bad = || Value::err("ERR min or max is not a float");
        let one = |b: &[u8]| -> Result<(f64, bool), Value> {
            let (ex, body) = match b.first() {
                Some(b'(') => (true, &b[1..]),
                _ => (false, b),
            };
            // strtod: leading spaces are allowed here, trailing junk isn't.
            let s = std::str::from_utf8(body).map_err(|_| bad())?.trim_start();
            // strtod on an empty string parses nothing and yields 0.
            let v = if s.is_empty() { Some(0.0) } else { parse_double(s.as_bytes()) };
            v.map(|v| (v, ex)).ok_or_else(bad)
        };
        let (min, minex) = one(min)?;
        let (max, maxex) = one(max)?;
        Ok(ScoreRange { min, max, minex, maxex })
    }

    fn gte_min(&self, v: f64) -> bool {
        if self.minex { v > self.min } else { v >= self.min }
    }

    fn lte_max(&self, v: f64) -> bool {
        if self.maxex { v < self.max } else { v <= self.max }
    }

    fn contains(&self, v: f64) -> bool {
        self.gte_min(v) && self.lte_max(v)
    }
}

/// One end of a lex range: `-`, `+`, `[x` or `(x`.
#[derive(Clone)]
enum LexBound {
    Min,
    Max,
    Incl(Vec<u8>),
    Excl(Vec<u8>),
}

struct LexRange {
    min: LexBound,
    max: LexBound,
}

impl LexRange {
    fn parse(min: &[u8], max: &[u8]) -> Result<LexRange, Value> {
        let bad = || Value::err("ERR min or max not valid string range item");
        let one = |b: &[u8]| -> Result<LexBound, Value> {
            match b.first() {
                Some(b'+') if b.len() == 1 => Ok(LexBound::Max),
                Some(b'-') if b.len() == 1 => Ok(LexBound::Min),
                Some(b'(') => Ok(LexBound::Excl(b[1..].to_vec())),
                Some(b'[') => Ok(LexBound::Incl(b[1..].to_vec())),
                _ => Err(bad()),
            }
        };
        Ok(LexRange { min: one(min)?, max: one(max)? })
    }

    fn gte_min(&self, v: &[u8]) -> bool {
        match &self.min {
            LexBound::Min => true,
            LexBound::Max => false,
            LexBound::Incl(x) => v >= x.as_slice(),
            LexBound::Excl(x) => v > x.as_slice(),
        }
    }

    fn lte_max(&self, v: &[u8]) -> bool {
        match &self.max {
            LexBound::Min => false,
            LexBound::Max => true,
            LexBound::Incl(x) => v <= x.as_slice(),
            LexBound::Excl(x) => v < x.as_slice(),
        }
    }

    fn contains(&self, v: &[u8]) -> bool {
        self.gte_min(v) && self.lte_max(v)
    }
}

fn zcount(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let r = ScoreRange::parse(&a[2], &a[3])?;
    let Some(z) = ctx.get_zset(&a[1])? else { return Ok(Value::Integer(0)) };
    Ok(Value::Integer(z.items.iter().filter(|(s, _)| r.contains(*s)).count() as i64))
}

fn zlexcount(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let r = LexRange::parse(&a[2], &a[3])?;
    let Some(z) = ctx.get_zset(&a[1])? else { return Ok(Value::Integer(0)) };
    Ok(Value::Integer(z.items.iter().filter(|(_, m)| r.contains(m)).count() as i64))
}

#[derive(Clone, Copy, PartialEq)]
enum RangeType {
    Auto,
    Rank,
    Score,
    Lex,
}

/// Normalizes a rank range, as `genericZrangebyrankCommand` does.
fn rank_range(len: usize, start: i64, end: i64) -> Option<(usize, usize)> {
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

/// Applies LIMIT offset/count to an ordered selection.
fn limit(items: Vec<(f64, Vec<u8>)>, len: usize, offset: i64, count: i64) -> Vec<(f64, Vec<u8>)> {
    if offset < 0 || (offset > 0 && offset as usize >= len) {
        return vec![];
    }
    let it = items.into_iter().skip(offset as usize);
    if count < 0 { it.collect() } else { it.take(count as usize).collect() }
}

/// `zrangeGenericCommand`: ZRANGE, ZRANGESTORE and the legacy variants.
/// `start` is the index of the source key.
fn zrange_generic(
    ctx: &mut Ctx,
    a: &[Vec<u8>],
    start: usize,
    store: Option<&[u8]>,
    mut kind: RangeType,
    mut rev: Option<bool>,
) -> Reply {
    let mut withscores = false;
    let (mut offset, mut count) = (0i64, -1i64);
    let mut j = start + 3;
    while j < a.len() {
        let left = a.len() - j - 1;
        if store.is_none() && eq_ic(&a[j], "withscores") {
            withscores = true;
        } else if eq_ic(&a[j], "limit") && left >= 2 {
            offset = long_arg(&a[j + 1])?;
            count = long_arg(&a[j + 2])?;
            j += 2;
        } else if rev.is_none() && eq_ic(&a[j], "rev") {
            rev = Some(true);
        } else if kind == RangeType::Auto && eq_ic(&a[j], "bylex") {
            kind = RangeType::Lex;
        } else if kind == RangeType::Auto && eq_ic(&a[j], "byscore") {
            kind = RangeType::Score;
        } else {
            return Err(syntax());
        }
        j += 1;
    }
    let rev = rev.unwrap_or(false);
    if kind == RangeType::Auto {
        kind = RangeType::Rank;
    }
    if count != -1 && kind == RangeType::Rank {
        return Err(Value::err(
            "ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX",
        ));
    }
    if withscores && kind == RangeType::Lex {
        return Err(Value::err(
            "ERR syntax error, WITHSCORES not supported in combination with BYLEX",
        ));
    }
    let (mut lo, mut hi) = (&a[start + 1], &a[start + 2]);
    if rev && kind != RangeType::Rank {
        std::mem::swap(&mut lo, &mut hi);
    }
    enum Spec {
        Rank(i64, i64),
        Score(ScoreRange),
        Lex(LexRange),
    }
    let spec = match kind {
        RangeType::Score => Spec::Score(ScoreRange::parse(lo, hi)?),
        RangeType::Lex => Spec::Lex(LexRange::parse(lo, hi)?),
        _ => Spec::Rank(long_arg(lo)?, long_arg(hi)?),
    };
    let resp3 = ctx.resp() >= 3;
    let selected: Vec<(f64, Vec<u8>)> = match ctx.get_zset(&a[start])? {
        None => vec![],
        Some(z) => {
            let len = z.len();
            let ordered: Box<dyn Iterator<Item = &(f64, Vec<u8>)>> =
                if rev { Box::new(z.items.iter().rev()) } else { Box::new(z.items.iter()) };
            match spec {
                Spec::Rank(s, e) => match rank_range(len, s, e) {
                    Some((s, e)) => ordered.skip(s).take(e - s + 1).cloned().collect(),
                    None => vec![],
                },
                Spec::Score(r) => {
                    let all = ordered.filter(|(s, _)| r.contains(*s)).cloned().collect();
                    limit(all, len, offset, count)
                }
                Spec::Lex(r) => {
                    let all = ordered.filter(|(_, m)| r.contains(m)).cloned().collect();
                    limit(all, len, offset, count)
                }
            }
        }
    };
    match store {
        Some(dst) => {
            let mut z = Zset::create(selected.len(), 0);
            for (s, m) in &selected {
                z.insert(m, *s);
            }
            ctx.store_zset(dst, z)
        }
        None => Ok(with_scores(&selected, withscores, resp3)),
    }
}

fn zrange(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    zrange_generic(ctx, a, 1, None, RangeType::Auto, None)
}

fn zrangestore(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let dst = a[1].clone();
    zrange_generic(ctx, a, 2, Some(&dst), RangeType::Auto, None)
}

fn zrevrange(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    zrange_generic(ctx, a, 1, None, RangeType::Rank, Some(true))
}

fn zrangebyscore(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    zrange_generic(ctx, a, 1, None, RangeType::Score, Some(false))
}

fn zrevrangebyscore(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    zrange_generic(ctx, a, 1, None, RangeType::Score, Some(true))
}

fn zrangebylex(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    zrange_generic(ctx, a, 1, None, RangeType::Lex, Some(false))
}

fn zrevrangebylex(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    zrange_generic(ctx, a, 1, None, RangeType::Lex, Some(true))
}

fn zremrange(ctx: &mut Ctx, a: &[Vec<u8>], kind: RangeType) -> Reply {
    enum Spec {
        Rank(i64, i64),
        Score(ScoreRange),
        Lex(LexRange),
    }
    let spec = match kind {
        RangeType::Score => Spec::Score(ScoreRange::parse(&a[2], &a[3])?),
        RangeType::Lex => Spec::Lex(LexRange::parse(&a[2], &a[3])?),
        _ => Spec::Rank(long_arg(&a[2])?, long_arg(&a[3])?),
    };
    let Some(z) = ctx.get_zset(&a[1])? else { return Ok(Value::Integer(0)) };
    let victims: Vec<Vec<u8>> = match spec {
        Spec::Rank(s, e) => match rank_range(z.len(), s, e) {
            Some((s, e)) => z.items[s..=e].iter().map(|(_, m)| m.clone()).collect(),
            None => return Ok(Value::Integer(0)),
        },
        Spec::Score(r) => {
            z.items.iter().filter(|(s, _)| r.contains(*s)).map(|(_, m)| m.clone()).collect()
        }
        Spec::Lex(r) => {
            z.items.iter().filter(|(_, m)| r.contains(m)).map(|(_, m)| m.clone()).collect()
        }
    };
    for m in &victims {
        z.remove(m);
    }
    ctx.drop_if_empty(&a[1]);
    Ok(Value::Integer(victims.len() as i64))
}

fn zremrangebyrank(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    zremrange(ctx, a, RangeType::Rank)
}

fn zremrangebyscore(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    zremrange(ctx, a, RangeType::Score)
}

fn zremrangebylex(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    zremrange(ctx, a, RangeType::Lex)
}

// ---- popping ----

/// Reply shapes of `genericZpopCommand`.
#[derive(Clone, Copy, PartialEq)]
enum PopShape {
    /// ZPOPMIN/ZPOPMAX: flat, or pairs on RESP3 when a count was given.
    Plain { nested: bool },
    /// BZPOPMIN/BZPOPMAX: [key, member, score].
    Keyed,
    /// ZMPOP/BZMPOP: [key, [[member, score], ...]].
    Multi,
}

/// Pops up to `count` from the first existing key. `None` if every key
/// is missing.
fn pop_from(
    ctx: &mut Ctx,
    keys: &[Vec<u8>],
    max: bool,
    count: usize,
    shape: PopShape,
) -> Result<Option<Value>, Value> {
    for key in keys {
        let Some(z) = ctx.get_zset(key)? else { continue };
        let n = count.min(z.len());
        let mut popped = Vec::with_capacity(n);
        for _ in 0..n {
            let item = if max { z.items.pop() } else { Some(z.items.remove(0)) };
            let item = item.expect("non-empty");
            z.scores.remove(&item.1);
            popped.push(item);
        }
        ctx.drop_if_empty(key);
        let pairs = |items: &[(f64, Vec<u8>)]| {
            Value::Array(
                items
                    .iter()
                    .map(|(s, m)| Value::Array(vec![Value::bulk(m), Value::Double(*s)]))
                    .collect(),
            )
        };
        let flat = |items: &[(f64, Vec<u8>)]| {
            items.iter().flat_map(|(s, m)| [Value::bulk(m), Value::Double(*s)]).collect::<Vec<_>>()
        };
        return Ok(Some(match shape {
            PopShape::Plain { nested: true } => pairs(&popped),
            PopShape::Plain { nested: false } => Value::Array(flat(&popped)),
            PopShape::Keyed => {
                let mut v = vec![Value::bulk(key)];
                v.extend(flat(&popped));
                Value::Array(v)
            }
            PopShape::Multi => Value::Array(vec![Value::bulk(key), pairs(&popped)]),
        }));
    }
    Ok(None)
}

fn zpop(ctx: &mut Ctx, a: &[Vec<u8>], max: bool) -> Reply {
    if a.len() > 3 {
        return Err(syntax());
    }
    let count = if a.len() == 3 { Some(positive_long(&a[2], None)? as usize) } else { None };
    let nested = ctx.resp() >= 3 && count.is_some();
    if count == Some(0) {
        // Still type-checks the key.
        ctx.get_zset(&a[1])?;
        return Ok(Value::Array(vec![]));
    }
    let shape = PopShape::Plain { nested };
    Ok(pop_from(ctx, &a[1..2], max, count.unwrap_or(1), shape)?.unwrap_or(Value::Array(vec![])))
}

fn zpopmin(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    zpop(ctx, a, false)
}

fn zpopmax(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    zpop(ctx, a, true)
}

fn blocking_pop(
    ctx: &mut Ctx,
    keys: &[Vec<u8>],
    max: bool,
    count: usize,
    shape: PopShape,
    deadline: u64,
) -> Reply {
    if let Some(v) = pop_from(ctx, keys, max, count, shape)? {
        return Ok(v);
    }
    if ctx.deny_blocking {
        return Ok(Value::NullArray);
    }
    ctx.block(BlockKind::Zset, keys, deadline)
}

fn bzpopmin(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let deadline = timeout_secs_arg(&a[a.len() - 1], ctx.now)?;
    blocking_pop(ctx, &a[1..a.len() - 1], false, 1, PopShape::Keyed, deadline)
}

fn bzpopmax(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let deadline = timeout_secs_arg(&a[a.len() - 1], ctx.now)?;
    blocking_pop(ctx, &a[1..a.len() - 1], true, 1, PopShape::Keyed, deadline)
}

/// Parses `numkeys key... MIN|MAX [COUNT n]` from `a[i]`.
fn mpop_args(a: &[Vec<u8>], i: usize) -> Result<(&[Vec<u8>], bool, usize), Value> {
    let numkeys = range_long(&a[i], 1, i64::MAX, Some("numkeys should be greater than 0"))?;
    let where_idx = i as i64 + numkeys + 1;
    if where_idx >= a.len() as i64 {
        return Err(syntax());
    }
    let w = where_idx as usize;
    let max = if eq_ic(&a[w], "min") {
        false
    } else if eq_ic(&a[w], "max") {
        true
    } else {
        return Err(syntax());
    };
    let mut count = None;
    let mut j = w + 1;
    while j < a.len() {
        if count.is_none() && eq_ic(&a[j], "count") && j + 1 < a.len() {
            j += 1;
            count = Some(range_long(&a[j], 1, i64::MAX, Some("count should be greater than 0"))?);
        } else {
            return Err(syntax());
        }
        j += 1;
    }
    Ok((&a[i + 1..w], max, count.unwrap_or(1) as usize))
}

fn zmpop(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let (keys, max, count) = mpop_args(a, 1)?;
    Ok(pop_from(ctx, keys, max, count, PopShape::Multi)?.unwrap_or(Value::NullArray))
}

fn bzmpop(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let (keys, max, count) = mpop_args(a, 2)?;
    let deadline = timeout_secs_arg(&a[1], ctx.now)?;
    blocking_pop(ctx, keys, max, count, PopShape::Multi, deadline)
}

// ---- random, scan ----

fn zrandmember(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    if a.len() == 2 {
        let Some(len) = ctx.get_zset(&a[1])?.map(|z| z.len()) else { return Ok(Value::Null) };
        let i = ctx.random() as usize % len;
        return Ok(Value::bulk(&ctx.get_zset(&a[1])?.unwrap().items[i].1));
    }
    let l = range_long(&a[2], -i64::MAX, i64::MAX, None)?;
    if a.len() > 4 || (a.len() == 4 && !eq_ic(&a[3], "withscores")) {
        return Err(syntax());
    }
    let withscores = a.len() == 4;
    if withscores && !(-i64::MAX / 2..=i64::MAX / 2).contains(&l) {
        return Err(Value::err("ERR value is out of range"));
    }
    let resp3 = ctx.resp() >= 3;
    let Some(z) = ctx.get_zset(&a[1])?.cloned() else { return Ok(Value::Array(vec![])) };
    let (count, unique) = (l.unsigned_abs() as usize, l >= 0);
    if count == 0 {
        return Ok(Value::Array(vec![]));
    }
    let size = z.len();
    let picks: Vec<usize> = if !unique || count == 1 {
        (0..count).map(|_| ctx.random() as usize % size).collect()
    } else if count >= size {
        // Redis's zset iterator walks from the highest score down.
        (0..size).rev().collect()
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
    let items: Vec<(f64, Vec<u8>)> = picks.into_iter().map(|i| z.items[i].clone()).collect();
    Ok(with_scores(&items, withscores, resp3))
}

fn zscan(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let args = parse_scan(a, 2, false)?;
    let Some(z) = ctx.get_zset(&a[1])? else {
        return Ok(Value::Array(vec![Value::bulk("0"), Value::Array(vec![])]));
    };
    let len = z.len();
    let (start, end) = if z.big {
        let end = (args.cursor + args.count).min(len);
        (args.cursor.min(end), end)
    } else {
        (0, len)
    };
    let mut out = Vec::new();
    for (s, m) in &z.items[start..end] {
        if args.matches(m) {
            out.push(Value::bulk(m));
            out.push(Value::bulk(super::double::d2string(*s)));
        }
    }
    let next = if end >= len { 0 } else { end };
    Ok(Value::Array(vec![Value::bulk(next.to_string()), Value::Array(out)]))
}

// ---- ZUNION / ZINTER / ZDIFF ----

#[derive(Clone, Copy, PartialEq)]
enum SetOp {
    Union,
    Inter,
    Diff,
}

#[derive(Clone, Copy)]
enum Aggregate {
    Sum,
    Min,
    Max,
}

fn aggregate(target: &mut f64, v: f64, agg: Aggregate) {
    match agg {
        Aggregate::Sum => {
            *target += v;
            if target.is_nan() {
                *target = 0.0;
            }
        }
        Aggregate::Min => *target = target.min(v),
        Aggregate::Max => *target = target.max(v),
    }
}

/// An input to ZUNION and friends: a sorted set, or a plain set whose
/// members all score 1.
type Source = Vec<(f64, Vec<u8>)>;

/// `zunionInterDiffGenericCommand`. `numkeys_idx` points at numkeys.
fn zsetop(
    ctx: &mut Ctx,
    a: &[Vec<u8>],
    numkeys_idx: usize,
    dst: Option<&[u8]>,
    op: SetOp,
    card_only: bool,
    name: &str,
) -> Reply {
    let setnum = long_arg(&a[numkeys_idx])?;
    if setnum < 1 {
        return Err(Value::err(format!("ERR at least 1 input key is needed for '{name}' command")));
    }
    if setnum > (a.len() - numkeys_idx - 1) as i64 {
        return Err(syntax());
    }
    let setnum = setnum as usize;
    let mut srcs: Vec<Option<Source>> = Vec::with_capacity(setnum);
    for key in &a[numkeys_idx + 1..numkeys_idx + 1 + setnum] {
        srcs.push(match ctx.lookup(key) {
            None => None,
            Some(Entry { data: Data::Zset(z), .. }) => Some(z.items.clone()),
            Some(Entry { data: Data::Set(s), .. }) => {
                Some(s.members().into_iter().map(|m| (1.0, m)).collect())
            }
            Some(_) => return Err(wrong_type()),
        });
    }
    let mut weights = vec![1.0; setnum];
    let mut agg = Aggregate::Sum;
    let mut withscores = false;
    let mut lim = 0usize;
    let mut j = numkeys_idx + 1 + setnum;
    while j < a.len() {
        let remaining = a.len() - j;
        if op != SetOp::Diff && !card_only && remaining > setnum && eq_ic(&a[j], "weights") {
            j += 1;
            for w in weights.iter_mut() {
                *w = parse_double(&a[j])
                    .ok_or_else(|| Value::err("ERR weight value is not a float"))?;
                j += 1;
            }
            continue;
        } else if op != SetOp::Diff && !card_only && remaining >= 2 && eq_ic(&a[j], "aggregate") {
            agg = match a[j + 1].to_ascii_lowercase().as_slice() {
                b"sum" => Aggregate::Sum,
                b"min" => Aggregate::Min,
                b"max" => Aggregate::Max,
                _ => return Err(syntax()),
            };
            j += 2;
            continue;
        } else if dst.is_none() && !card_only && eq_ic(&a[j], "withscores") {
            withscores = true;
        } else if card_only && remaining >= 2 && eq_ic(&a[j], "limit") {
            lim = positive_long(&a[j + 1], Some("LIMIT can't be negative"))? as usize;
            j += 1;
        } else {
            return Err(syntax());
        }
        j += 1;
    }

    let mut result = Zset::default();
    let mut order: Vec<usize> = (0..setnum).collect();
    if op != SetOp::Diff {
        // Smallest first, as Redis's qsort by cardinality.
        order.sort_by_key(|&i| srcs[i].as_ref().map_or(0, |s| s.len()));
    }
    let lookup: Vec<Option<HashMap<&[u8], f64>>> = srcs
        .iter()
        .map(|s| s.as_ref().map(|s| s.iter().map(|(sc, m)| (m.as_slice(), *sc)).collect()))
        .collect();
    match op {
        SetOp::Inter => {
            let first = order[0];
            let mut card = 0;
            if let Some(src) = &srcs[first] {
                'outer: for (sc, m) in src {
                    let mut score = weights[first] * sc;
                    if score.is_nan() {
                        score = 0.0;
                    }
                    for &i in &order[1..] {
                        match lookup[i].as_ref().and_then(|l| l.get(m.as_slice())) {
                            Some(v) => aggregate(&mut score, v * weights[i], agg),
                            None => continue 'outer,
                        }
                    }
                    card += 1;
                    if card_only {
                        if lim != 0 && card >= lim {
                            break;
                        }
                        continue;
                    }
                    result.insert(m, score);
                }
            }
            if card_only {
                return Ok(Value::Integer(card as i64));
            }
        }
        SetOp::Union => {
            let mut acc: HashMap<Vec<u8>, f64> = HashMap::new();
            for &i in &order {
                for (sc, m) in srcs[i].iter().flatten() {
                    let mut score = weights[i] * sc;
                    if score.is_nan() {
                        score = 0.0;
                    }
                    match acc.get_mut(m) {
                        Some(t) => aggregate(t, score, agg),
                        None => {
                            acc.insert(m.clone(), score);
                        }
                    }
                }
            }
            for (m, s) in acc {
                result.insert(&m, s);
            }
        }
        SetOp::Diff => {
            if let Some(first) = &srcs[0] {
                for (sc, m) in first {
                    if !lookup[1..].iter().flatten().any(|l| l.contains_key(m.as_slice())) {
                        result.insert(m, *sc);
                    }
                }
            }
        }
    }
    match dst {
        Some(dst) => {
            result.shrink_if_small();
            ctx.store_zset(dst, result)
        }
        None => {
            let resp3 = ctx.resp() >= 3;
            Ok(with_scores(&result.items, withscores, resp3))
        }
    }
}

fn zunion(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    zsetop(ctx, a, 1, None, SetOp::Union, false, "zunion")
}

fn zinter(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    zsetop(ctx, a, 1, None, SetOp::Inter, false, "zinter")
}

fn zdiff(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    zsetop(ctx, a, 1, None, SetOp::Diff, false, "zdiff")
}

fn zunionstore(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let dst = a[1].clone();
    zsetop(ctx, a, 2, Some(&dst), SetOp::Union, false, "zunionstore")
}

fn zinterstore(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let dst = a[1].clone();
    zsetop(ctx, a, 2, Some(&dst), SetOp::Inter, false, "zinterstore")
}

fn zdiffstore(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let dst = a[1].clone();
    zsetop(ctx, a, 2, Some(&dst), SetOp::Diff, false, "zdiffstore")
}

fn zintercard(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    zsetop(ctx, a, 1, None, SetOp::Inter, true, "zintercard")
}
