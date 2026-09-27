//! SORT and SORT_RO, ported from Redis's sort.c.
//!
//! Elements come from a list, set or sorted set; they are ordered as numbers
//! (or bytes with ALPHA), optionally by the value of another key (`BY`),
//! windowed with LIMIT, and returned (or, with GET, replaced by the values of
//! other keys) or stored as a list.

use std::cmp::Ordering;
use std::collections::VecDeque;

use super::engine::{Command, Ctx, Data, Entry, Reply, cmd, eq_ic, int_arg, syntax, wrong_type};
use super::longdouble::{self, Ld};
use super::resp::Value;

pub static COMMANDS: &[Command] = &[cmd("sort", sort), cmd("sort_ro", sort_ro)];

fn sort(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    generic(ctx, a, false)
}

fn sort_ro(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    generic(ctx, a, true)
}

enum Kind {
    List,
    Set,
    Zset,
}

struct Item {
    obj: Vec<u8>,
    score: f64,
    /// The BY value when sorting ALPHA by a pattern; `None` if the key is missing.
    cmp: Option<Vec<u8>>,
}

fn generic(ctx: &mut Ctx, a: &[Vec<u8>], read_only: bool) -> Reply {
    let (mut desc, mut alpha) = (false, false);
    let (mut limit_start, mut limit_count) = (0i64, -1i64);
    let mut sortby: Option<Vec<u8>> = None;
    let mut store: Option<Vec<u8>> = None;
    let mut gets: Vec<Vec<u8>> = Vec::new();
    let mut dontsort = false;

    let mut j = 2;
    while j < a.len() {
        let left = a.len() - j - 1;
        let opt = &a[j];
        if eq_ic(opt, "asc") {
            desc = false;
        } else if eq_ic(opt, "desc") {
            desc = true;
        } else if eq_ic(opt, "alpha") {
            alpha = true;
        } else if eq_ic(opt, "limit") && left >= 2 {
            limit_start = int_arg(&a[j + 1])?;
            limit_count = int_arg(&a[j + 2])?;
            j += 2;
        } else if !read_only && eq_ic(opt, "store") && left >= 1 {
            store = Some(a[j + 1].clone());
            j += 1;
        } else if eq_ic(opt, "by") && left >= 1 {
            sortby = Some(a[j + 1].clone());
            // A pattern without '*' means "don't sort".
            dontsort = !a[j + 1].contains(&b'*');
            j += 1;
        } else if eq_ic(opt, "get") && left >= 1 {
            gets.push(a[j + 1].clone());
            j += 1;
        } else {
            return Err(syntax());
        }
        j += 1;
    }

    let (kind, mut items): (Kind, Vec<Vec<u8>>) = match ctx.lookup(&a[1]) {
        None => (Kind::List, vec![]),
        Some(Entry { data: Data::List(l), .. }) => (Kind::List, l.iter().cloned().collect()),
        Some(Entry { data: Data::Set(s), .. }) => (Kind::Set, s.members()),
        Some(Entry { data: Data::Zset(z), .. }) => {
            (Kind::Zset, z.iter().map(|(_, m)| m.clone()).collect())
        }
        Some(_) => return Err(wrong_type()),
    };
    // A stored or scripted SORT of a set must be repeatable, so it sorts as text.
    if dontsort && matches!(kind, Kind::Set) && (store.is_some() || ctx.engine.lua_calls > 0) {
        dontsort = false;
        alpha = true;
        sortby = None;
    }

    // The LIMIT window, clamped exactly as Redis does.
    let len = items.len() as i64;
    let mut start = limit_start.max(0).min(len);
    let count = limit_count.max(-1).min(len);
    let mut end = if count < 0 { len - 1 } else { start + count - 1 };
    if start >= len {
        start = len - 1;
        end = len - 2;
    }
    if end >= len {
        end = len - 1;
    }

    let mut sorted: Vec<Item> = Vec::new();
    if dontsort {
        // Natural order; lists and sorted sets can be walked backwards.
        if desc && matches!(kind, Kind::List | Kind::Zset) {
            items.reverse();
        }
        sorted = items.into_iter().map(|obj| Item { obj, score: 0.0, cmp: None }).collect();
    } else {
        let mut conversion_error = false;
        for obj in items {
            let mut item = Item { obj, score: 0.0, cmp: None };
            let byval = match &sortby {
                Some(pattern) => lookup_by_pattern(ctx, pattern, &item.obj),
                None => Some(item.obj.clone()),
            };
            if let Some(byval) = byval {
                if alpha {
                    if sortby.is_some() {
                        item.cmp = Some(byval);
                    }
                } else {
                    match strtod(&byval) {
                        Some(score) => item.score = score,
                        None => conversion_error = true,
                    }
                }
            }
            sorted.push(item);
        }
        let by_pattern = sortby.is_some();
        sorted.sort_by(|x, y| {
            let ord = if !alpha {
                match x.score.partial_cmp(&y.score) {
                    Some(Ordering::Equal) | None => x.obj.cmp(&y.obj),
                    Some(o) => o,
                }
            } else if by_pattern {
                match (&x.cmp, &y.cmp) {
                    (None, None) => Ordering::Equal,
                    (None, _) => Ordering::Less,
                    (_, None) => Ordering::Greater,
                    (Some(p), Some(q)) => p.cmp(q),
                }
            } else {
                x.obj.cmp(&y.obj)
            };
            if desc { ord.reverse() } else { ord }
        });
        if conversion_error {
            return Err(Value::err("ERR One or more scores can't be converted into double"));
        }
    }

    let window: &[Item] = if start <= end { &sorted[start as usize..=end as usize] } else { &[] };
    // One value per GET per element, or the element itself without GETs.
    let mut values: Vec<Option<Vec<u8>>> = Vec::new();
    for item in window {
        if gets.is_empty() {
            values.push(Some(item.obj.clone()));
        } else {
            for pattern in &gets {
                values.push(lookup_by_pattern(ctx, pattern, &item.obj));
            }
        }
    }

    let Some(dest) = store else {
        return Ok(Value::Array(
            values.into_iter().map(|v| v.map_or(Value::Null, Value::Bulk)).collect(),
        ));
    };
    let n = values.len() as i64;
    let list: VecDeque<Vec<u8>> = values.into_iter().map(Option::unwrap_or_default).collect();
    let now = ctx.now;
    if list.is_empty() {
        ctx.db().remove(&dest, now);
    } else {
        ctx.db().insert(dest, Entry::new(Data::List(list)));
    }
    Ok(Value::Integer(n))
}

/// `lookupKeyByPattern`: `#` is the element; otherwise the first `*` of the
/// pattern is replaced by the element and the resulting key is read: a
/// string, or, with `->field`, a hash field.
fn lookup_by_pattern(ctx: &mut Ctx, pattern: &[u8], subst: &[u8]) -> Option<Vec<u8>> {
    if pattern == b"#" {
        return Some(subst.to_vec());
    }
    let star = pattern.iter().position(|&c| c == b'*')?;
    // "->field" after the '*' (only if something follows the arrow).
    let arrow = pattern[star + 1..].windows(2).position(|w| w == b"->").map(|i| star + 1 + i);
    let field = arrow.filter(|&i| i + 2 < pattern.len()).map(|i| (i, &pattern[i + 2..]));
    let postfix_end = field.map_or(pattern.len(), |(i, _)| i);
    let mut key = pattern[..star].to_vec();
    key.extend_from_slice(subst);
    key.extend_from_slice(&pattern[star + 1..postfix_end]);

    match (ctx.lookup(&key)?, field) {
        (Entry { data: Data::Hash(h), .. }, Some((_, name))) => h.map.get(name).cloned(),
        (Entry { data: Data::Str(s), .. }, None) => Some(s.clone()),
        _ => None,
    }
}

/// `strtod` followed by Redis's checks: the whole string must be consumed,
/// the result must not be NaN and must not overflow. An empty string is 0.
fn strtod(b: &[u8]) -> Option<f64> {
    if b.is_empty() {
        return Some(0.0);
    }
    let text = std::str::from_utf8(b).ok()?;
    let text = text.trim_start_matches([' ', '\t', '\n', '\x0b', '\x0c', '\r']);
    let unsigned = text.trim_start_matches(['+', '-']);
    // strtod accepts one sign at most.
    if text.len() - unsigned.len() > 1 {
        return None;
    }
    let value = if unsigned.starts_with("0x") || unsigned.starts_with("0X") {
        match longdouble::parse(text.as_bytes()).ok()? {
            Ld::Zero { .. } => 0.0,
            Ld::Finite { neg, mant, exp } => {
                let v = mant as f64 * 2f64.powi(exp as i32);
                if neg { -v } else { v }
            }
            Ld::Inf { neg } => {
                if neg {
                    f64::NEG_INFINITY
                } else {
                    f64::INFINITY
                }
            }
            Ld::Nan => return None,
        }
    } else {
        text.parse::<f64>().ok()?
    };
    let spelled_infinity =
        unsigned.eq_ignore_ascii_case("inf") || unsigned.eq_ignore_ascii_case("infinity");
    if value.is_nan() || (value.is_infinite() && !spelled_infinity) {
        return None;
    }
    Some(value)
}
