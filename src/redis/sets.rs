//! Set commands, ported from Redis's t_set.c.
//!
//! Sets mirror Redis's three encodings because clients see their orders:
//! an intset is sorted, a listpack keeps insertion order, and a hashtable
//! has no defined order.

use super::engine::{
    Command, Ctx, Data, Entry, Reply, cmd, eq_ic, positive_long, range_long, syntax, wrong_type,
};
use super::keys::parse_scan;
use super::num::parse_int;
use super::ordered::OrderedSet;
use super::resp::Value;

pub static COMMANDS: &[Command] = &[
    cmd("sadd", sadd),
    cmd("srem", srem),
    cmd("smove", smove),
    cmd("sismember", sismember),
    cmd("smismember", smismember),
    cmd("scard", scard),
    cmd("spop", spop),
    cmd("srandmember", srandmember),
    cmd("sinter", sinter),
    cmd("smembers", sinter),
    cmd("sintercard", sintercard),
    cmd("sinterstore", sinterstore),
    cmd("sunion", sunion),
    cmd("sunionstore", sunionstore),
    cmd("sdiff", sdiff),
    cmd("sdiffstore", sdiffstore),
    cmd("sscan", sscan),
];

/// `set-max-intset-entries`, `set-max-listpack-entries` and
/// `set-max-listpack-value` defaults.
const MAX_INTSET: usize = 512;
const MAX_LISTPACK: usize = 128;
const MAX_LISTPACK_VALUE: usize = 64;

#[derive(Clone, Debug)]
pub enum Set {
    /// Sorted integers.
    Int(Vec<i64>),
    /// Insertion order.
    Pack(OrderedSet),
    /// No defined order (O(1) removals move the last member).
    Hash(OrderedSet),
}

impl Set {
    /// `setTypeCreate`: the encoding for a new set whose first member is
    /// `first`, expecting about `size_hint` members.
    pub fn create(first: &[u8], size_hint: usize) -> Set {
        if parse_int(first).is_some() && size_hint <= MAX_INTSET {
            Set::Int(Vec::new())
        } else if size_hint <= MAX_LISTPACK {
            Set::Pack(OrderedSet::default())
        } else {
            Set::Hash(OrderedSet::default())
        }
    }

    pub fn encoding(&self) -> &'static str {
        match self {
            Set::Int(_) => "intset",
            Set::Pack(_) => "listpack",
            Set::Hash(_) => "hashtable",
        }
    }

    /// `setTypeMaybeConvert`: go straight to a hashtable if `size_hint`
    /// members won't fit the compact encoding.
    fn maybe_convert(&mut self, size_hint: usize) {
        let too_big = match self {
            Set::Int(_) => size_hint > MAX_INTSET,
            Set::Pack(_) => size_hint > MAX_LISTPACK,
            Set::Hash(_) => false,
        };
        if too_big {
            self.convert_to_hash();
        }
    }

    fn convert_to_hash(&mut self) {
        let mut h = OrderedSet::default();
        for m in self.members() {
            h.insert(&m);
        }
        *self = Set::Hash(h);
    }

    pub fn len(&self) -> usize {
        match self {
            Set::Int(v) => v.len(),
            Set::Pack(s) | Set::Hash(s) => s.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Members in the encoding's iteration order.
    pub fn members(&self) -> Vec<Vec<u8>> {
        match self {
            Set::Int(v) => v.iter().map(|n| n.to_string().into_bytes()).collect(),
            Set::Pack(s) | Set::Hash(s) => s.iter().cloned().collect(),
        }
    }

    fn get(&self, i: usize) -> Vec<u8> {
        match self {
            Set::Int(v) => v[i].to_string().into_bytes(),
            Set::Pack(s) | Set::Hash(s) => s.get(i).to_vec(),
        }
    }

    pub fn contains(&self, m: &[u8]) -> bool {
        match self {
            Set::Int(v) => parse_int(m).is_some_and(|n| v.binary_search(&n).is_ok()),
            Set::Pack(s) | Set::Hash(s) => s.contains(m),
        }
    }

    /// `setTypeAdd`. Returns true if `m` was new.
    pub fn add(&mut self, m: &[u8]) -> bool {
        match self {
            Set::Int(v) => {
                if let Some(n) = parse_int(m) {
                    let Err(i) = v.binary_search(&n) else { return false };
                    v.insert(i, n);
                    if v.len() > MAX_INTSET {
                        self.convert_to_hash();
                    }
                    return true;
                }
                let members = self.members();
                let mut s = OrderedSet::default();
                for x in &members {
                    s.insert(x);
                }
                s.insert(m);
                *self = if members.len() < MAX_LISTPACK && m.len() <= MAX_LISTPACK_VALUE {
                    Set::Pack(s)
                } else {
                    Set::Hash(s)
                };
                true
            }
            Set::Pack(s) => {
                if s.contains(m) {
                    return false;
                }
                if s.len() < MAX_LISTPACK && m.len() <= MAX_LISTPACK_VALUE {
                    s.insert(m);
                } else {
                    self.convert_to_hash();
                    self.add(m);
                }
                true
            }
            Set::Hash(s) => s.insert(m),
        }
    }

    /// `setTypeRemove`. Returns true if `m` was there.
    pub fn remove(&mut self, m: &[u8]) -> bool {
        match self {
            Set::Int(v) => match parse_int(m).map(|n| v.binary_search(&n)) {
                Some(Ok(i)) => {
                    v.remove(i);
                    true
                }
                _ => false,
            },
            Set::Pack(s) => s.remove(m),
            Set::Hash(s) => s.swap_remove(m),
        }
    }

    /// `maybeConvertToIntset`, used when a stored result is all integers.
    fn maybe_convert_to_intset(&mut self) {
        if matches!(self, Set::Int(_)) || self.len() > MAX_INTSET {
            return;
        }
        let mut v: Vec<i64> = self.members().iter().filter_map(|m| parse_int(m)).collect();
        v.sort_unstable();
        *self = Set::Int(v);
    }
}

impl Ctx<'_> {
    /// The set at `key`, `None` if missing, WRONGTYPE for other types.
    pub fn get_set(&mut self, key: &[u8]) -> Result<Option<&mut Set>, Value> {
        match self.lookup(key) {
            None => Ok(None),
            Some(Entry { data: Data::Set(s), .. }) => Ok(Some(s)),
            Some(_) => Err(wrong_type()),
        }
    }

    /// A copy of the set at `key` (for multi-key operations).
    fn set_copy(&mut self, key: &[u8]) -> Result<Option<Set>, Value> {
        Ok(self.get_set(key)?.cloned())
    }

    fn store_set(&mut self, key: &[u8], set: Set) {
        let now = self.now;
        self.db().remove(key, now);
        self.db().insert(key.to_vec(), Entry::new(Data::Set(set)));
    }
}

fn sadd(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let hint = a.len() - 2;
    match ctx.get_set(&a[1])? {
        Some(s) => s.maybe_convert(hint),
        None => {
            let s = Set::create(&a[2], hint);
            ctx.db().insert(a[1].clone(), Entry::new(Data::Set(s)));
        }
    }
    let s = ctx.get_set(&a[1])?.expect("exists");
    let added = a[2..].iter().filter(|m| s.add(m)).count();
    Ok(Value::Integer(added as i64))
}

fn srem(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let Some(s) = ctx.get_set(&a[1])? else { return Ok(Value::Integer(0)) };
    let mut removed = 0;
    for m in &a[2..] {
        if s.remove(m) {
            removed += 1;
            if s.is_empty() {
                break;
            }
        }
    }
    ctx.drop_if_empty(&a[1]);
    Ok(Value::Integer(removed))
}

fn smove(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let (src, dst, m) = (&a[1], &a[2], &a[3]);
    if ctx.lookup(src).is_none() {
        return Ok(Value::Integer(0));
    }
    ctx.get_set(src)?;
    ctx.get_set(dst)?;
    if src == dst {
        return Ok(Value::Integer(ctx.get_set(src)?.expect("exists").contains(m) as i64));
    }
    if !ctx.get_set(src)?.expect("exists").remove(m) {
        return Ok(Value::Integer(0));
    }
    ctx.drop_if_empty(src);
    if ctx.get_set(dst)?.is_none() {
        ctx.db().insert(dst.clone(), Entry::new(Data::Set(Set::create(m, 1))));
    }
    ctx.get_set(dst)?.expect("exists").add(m);
    Ok(Value::Integer(1))
}

fn sismember(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    Ok(Value::Integer(ctx.get_set(&a[1])?.is_some_and(|s| s.contains(&a[2])) as i64))
}

fn smismember(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let s = ctx.get_set(&a[1])?;
    let found = a[2..]
        .iter()
        .map(|m| Value::Integer(s.as_ref().is_some_and(|s| s.contains(m)) as i64))
        .collect();
    Ok(Value::Array(found))
}

fn scard(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    Ok(Value::Integer(ctx.get_set(&a[1])?.map_or(0, |s| s.len()) as i64))
}

fn bulk_set(members: Vec<Vec<u8>>) -> Value {
    Value::Set(members.into_iter().map(Value::Bulk).collect())
}

/// Removes and returns a random member (`setTypePopRandom`).
fn pop_random(ctx: &mut Ctx, key: &[u8]) -> Vec<u8> {
    let len = ctx.get_set(key).ok().flatten().map_or(0, |s| s.len());
    let i = ctx.random() as usize % len;
    let s = ctx.get_set(key).ok().flatten().expect("exists");
    let m = s.get(i);
    s.remove(&m);
    m
}

fn spop(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    if a.len() > 3 {
        return Err(syntax());
    }
    if a.len() == 2 {
        if ctx.get_set(&a[1])?.is_none() {
            return Ok(Value::Null);
        }
        let m = pop_random(ctx, &a[1]);
        ctx.drop_if_empty(&a[1]);
        return Ok(Value::Bulk(m));
    }
    let count = positive_long(&a[2], None)? as usize;
    let Some(size) = ctx.get_set(&a[1])?.map(|s| s.len()) else {
        return Ok(bulk_set(vec![]));
    };
    if count == 0 {
        return Ok(bulk_set(vec![]));
    }
    if count >= size {
        // The whole set, in the order SUNION would give it.
        let s = ctx.set_copy(&a[1])?;
        let now = ctx.now;
        ctx.db().remove(&a[1], now);
        return Ok(bulk_set(union_diff(&[s], false, false).members()));
    }
    let remaining = size - count;
    let s = ctx.get_set(&a[1])?.expect("exists").clone();
    let is_pack = matches!(s, Set::Pack(_));
    let popped = if remaining * 5 > count {
        if is_pack {
            // A listpack is walked once: the picks come out in its order.
            let picks: Vec<Vec<u8>> =
                random_subset(ctx, size, count).into_iter().map(|i| s.get(i)).collect();
            let set = ctx.get_set(&a[1])?.expect("exists");
            for m in &picks {
                set.remove(m);
            }
            picks
        } else {
            (0..count).map(|_| pop_random(ctx, &a[1])).collect()
        }
    } else {
        // Keep `remaining` random members in a new set; reply with the rest
        // in the old set's order.
        let keep: Vec<usize> = if is_pack {
            random_subset(ctx, size, remaining)
        } else {
            let mut all: Vec<usize> = (0..size).collect();
            (0..remaining).map(|_| all.swap_remove(ctx.random() as usize % all.len())).collect()
        };
        let mut new = match s {
            Set::Int(_) => Set::Int(vec![]),
            _ => Set::Pack(OrderedSet::default()),
        };
        let mut old = s.clone();
        for &i in &keep {
            let m = s.get(i);
            new.add(&m);
            old.remove(&m);
        }
        *ctx.get_set(&a[1])?.expect("exists") = new;
        old.members()
    };
    Ok(bulk_set(popped))
}

/// `k` distinct random indexes below `n`, in increasing order.
fn random_subset(ctx: &mut Ctx, n: usize, k: usize) -> Vec<usize> {
    let mut chosen = vec![false; n];
    let mut picked = 0;
    while picked < k.min(n) {
        let i = ctx.random() as usize % n;
        if !chosen[i] {
            chosen[i] = true;
            picked += 1;
        }
    }
    (0..n).filter(|i| chosen[*i]).collect()
}

fn srandmember(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    if a.len() > 3 {
        return Err(syntax());
    }
    if a.len() == 2 {
        let Some(len) = ctx.get_set(&a[1])?.map(|s| s.len()) else { return Ok(Value::Null) };
        let i = ctx.random() as usize % len;
        return Ok(Value::Bulk(ctx.get_set(&a[1])?.expect("exists").get(i)));
    }
    let l = range_long(&a[2], -i64::MAX, i64::MAX, None)?;
    let Some(s) = ctx.set_copy(&a[1])? else { return Ok(Value::Array(vec![])) };
    let (count, unique) = (l.unsigned_abs() as usize, l >= 0);
    let size = s.len();
    if count == 0 {
        return Ok(Value::Array(vec![]));
    }
    let picks: Vec<usize> = if !unique || count == 1 {
        (0..count).map(|_| ctx.random() as usize % size).collect()
    } else if count >= size {
        (0..size).collect()
    } else {
        random_subset(ctx, size, count)
    };
    Ok(Value::Array(picks.into_iter().map(|i| Value::Bulk(s.get(i))).collect()))
}

/// Looks up every key as a set (missing = `None`), failing on the first
/// key of another type.
fn load_sets(ctx: &mut Ctx, keys: &[Vec<u8>]) -> Result<Vec<Option<Set>>, Value> {
    keys.iter().map(|k| ctx.set_copy(k)).collect()
}

/// `sinterGenericCommand`: the intersection in the order of the smallest
/// set, and whether all its members are integers.
fn intersect(sets: &[Set], limit: usize) -> Vec<Vec<u8>> {
    let mut order: Vec<&Set> = sets.iter().collect();
    order.sort_by_key(|s| s.len());
    let mut out = Vec::new();
    for m in order[0].members() {
        if order[1..].iter().all(|s| s.contains(&m)) {
            out.push(m);
            if limit != 0 && out.len() >= limit {
                break;
            }
        }
    }
    out
}

fn sinter(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let sets = load_sets(ctx, &a[1..])?;
    let Some(sets) = sets.into_iter().collect::<Option<Vec<Set>>>() else {
        return Ok(bulk_set(vec![]));
    };
    Ok(bulk_set(intersect(&sets, 0)))
}

fn sintercard(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let numkeys = range_long(&a[1], 1, i64::MAX, Some("numkeys should be greater than 0"))?;
    if numkeys > a.len() as i64 - 2 {
        return Err(Value::err("ERR Number of keys can't be greater than number of args"));
    }
    let numkeys = numkeys as usize;
    let mut limit = 0;
    let mut j = 2 + numkeys;
    while j < a.len() {
        if eq_ic(&a[j], "limit") && j + 1 < a.len() {
            j += 1;
            limit = positive_long(&a[j], Some("LIMIT can't be negative"))? as usize;
        } else {
            return Err(syntax());
        }
        j += 1;
    }
    let sets = load_sets(ctx, &a[2..2 + numkeys])?;
    let Some(sets) = sets.into_iter().collect::<Option<Vec<Set>>>() else {
        return Ok(Value::Integer(0));
    };
    Ok(Value::Integer(intersect(&sets, limit).len() as i64))
}

/// Stores `set` at `dst` (or deletes `dst` if empty) and replies its size.
fn store_result(ctx: &mut Ctx, dst: &[u8], set: Set) -> Reply {
    let len = set.len();
    if len == 0 {
        let now = ctx.now;
        ctx.db().remove(dst, now);
    } else {
        ctx.store_set(dst, set);
    }
    Ok(Value::Integer(len as i64))
}

fn sinterstore(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let sets = load_sets(ctx, &a[2..])?;
    let Some(mut sets) = sets.into_iter().collect::<Option<Vec<Set>>>() else {
        return store_result(ctx, &a[1], Set::Int(vec![]));
    };
    sets.sort_by_key(|s| s.len());
    let members = intersect(&sets, 0);
    let mut dst = match sets[0] {
        Set::Int(_) => Set::Int(vec![]),
        _ => Set::Pack(OrderedSet::default()),
    };
    for m in &members {
        dst.add(m);
    }
    if !dst.is_empty() && members.iter().all(|m| parse_int(m).is_some()) {
        dst.maybe_convert_to_intset();
    }
    store_result(ctx, &a[1], dst)
}

/// `sunionDiffGenericCommand`: builds the result the way Redis does,
/// starting from an intset, so its encoding and order match.
fn union_diff(sets: &[Option<Set>], diff: bool, sameset: bool) -> Set {
    let mut dst = Set::Int(vec![]);
    if !diff {
        for s in sets.iter().flatten() {
            for m in s.members() {
                dst.add(&m);
            }
        }
        return dst;
    }
    let Some(first) = &sets[0] else { return dst };
    if sameset {
        return dst;
    }
    let others: Vec<&Set> = sets[1..].iter().flatten().collect();
    let algo_one = first.len() * (others.len() + 1) / 2;
    let algo_two = first.len() + others.iter().map(|s| s.len()).sum::<usize>();
    if algo_one <= algo_two {
        for m in first.members() {
            if !others.iter().any(|s| s.contains(&m)) {
                dst.add(&m);
            }
        }
    } else {
        for m in first.members() {
            dst.add(&m);
        }
        for s in others {
            if dst.is_empty() {
                break;
            }
            for m in s.members() {
                dst.remove(&m);
            }
        }
    }
    dst
}

/// SUNION/SDIFF over `keys`; the first key repeated makes a diff empty.
fn union_diff_keys(ctx: &mut Ctx, keys: &[Vec<u8>], diff: bool) -> Result<Set, Value> {
    let sets = load_sets(ctx, keys)?;
    let sameset = keys[1..].contains(&keys[0]);
    Ok(union_diff(&sets, diff, sameset))
}

fn sunion(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    Ok(bulk_set(union_diff_keys(ctx, &a[1..], false)?.members()))
}

fn sunionstore(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let result = union_diff_keys(ctx, &a[2..], false)?;
    store_result(ctx, &a[1], result)
}

fn sdiff(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    Ok(bulk_set(union_diff_keys(ctx, &a[1..], true)?.members()))
}

fn sdiffstore(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let result = union_diff_keys(ctx, &a[2..], true)?;
    store_result(ctx, &a[1], result)
}

/// SSCAN: compact sets come back whole; a hashtable is walked by cursor.
fn sscan(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let args = parse_scan(a, 2, false)?;
    let Some(s) = ctx.get_set(&a[1])? else {
        return Ok(Value::Array(vec![Value::bulk("0"), Value::Array(vec![])]));
    };
    let members = s.members();
    let (start, end) = if matches!(s, Set::Hash(_)) {
        let end = (args.cursor + args.count).min(members.len());
        (args.cursor.min(end), end)
    } else {
        (0, members.len())
    };
    let out = members[start..end].iter().filter(|m| args.matches(m)).map(Value::bulk).collect();
    let next = if end >= members.len() { 0 } else { end };
    Ok(Value::Array(vec![Value::bulk(next.to_string()), Value::Array(out)]))
}
