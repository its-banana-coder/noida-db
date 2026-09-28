//! Range types: `int4range`/`int8range`/`numrange`/`daterange`/`tsrange`/
//! `tstzrange`. A range value is its canonical Postgres text form (a plain
//! `Value::Text`, re-parsed here on demand) — same reasoning as `fts.rs`:
//! there's no index to accelerate, so nothing is gained by keeping the
//! parsed form around.
//!
//! Scope: construction, `@>`/`<@`/`&&`, and `lower`/`upper`/`isempty`. Not
//! implemented: `lower_inc`/`upper_inc`, the union/difference/intersection
//! operators, adjacency and positional operators, multiranges, exclusion
//! constraints — see docs/LIMITATIONS.md.

use super::datetime::Ctx;
use super::error::{PgError, PgResult, code};
use super::types::{self, Base, Type, Value};

#[derive(Clone, Debug, PartialEq)]
pub enum Bound {
    Unbounded,
    Inclusive(Value),
    Exclusive(Value),
}

#[derive(Clone, Debug, PartialEq)]
pub struct Range {
    pub empty: bool,
    pub lower: Bound,
    pub upper: Bound,
}

/// The scalar type a range's bounds are values of.
pub fn elem_type(base: Base) -> Type {
    match base {
        Base::Int4Range => Type::INT4,
        Base::Int8Range => Type::INT8,
        Base::NumRange => Type::NUMERIC,
        Base::DateRange => Type::DATE,
        Base::TsRange => Type::TIMESTAMP,
        Base::TstzRange => Type::TIMESTAMPTZ,
        _ => Type::TEXT,
    }
}

/// Discrete ranges (int4/int8/date) always canonicalize to `[lower,upper)`;
/// continuous ones (numeric/timestamp[tz]) keep whatever bounds were given.
fn is_discrete(base: Base) -> bool {
    matches!(base, Base::Int4Range | Base::Int8Range | Base::DateRange)
}

/// The discrete successor of a bound value (`+1`, or `+1 day` for a date).
fn step(v: &Value, base: Base) -> Value {
    match (v, base) {
        (Value::Int(i), Base::Int4Range | Base::Int8Range) => Value::Int(i + 1),
        (Value::Date(d), Base::DateRange) => Value::Date(d + 1),
        _ => v.clone(),
    }
}

fn cmp(a: &Value, b: &Value) -> std::cmp::Ordering {
    types::cmp_values(a, b)
}

/// Parses a range's own text form: `empty`, or `[lower,upper]` with any mix
/// of `[`/`(` and `]`/`)`, either side blank for unbounded.
pub fn parse(s: &str, base: Base, ctx: &Ctx) -> PgResult<Range> {
    let t = s.trim();
    if t.eq_ignore_ascii_case("empty") {
        return Ok(Range { empty: true, lower: Bound::Unbounded, upper: Bound::Unbounded });
    }
    let bad = || malformed(s);
    let lower_inc = match t.chars().next() {
        Some('[') => true,
        Some('(') => false,
        _ => return Err(bad()),
    };
    let upper_inc = match t.chars().next_back() {
        Some(']') => true,
        Some(')') => false,
        _ => return Err(bad()),
    };
    let inner = &t[1..t.len() - 1];
    let (lo_txt, hi_txt) = split_bounds(inner).ok_or_else(bad)?;
    let elem = elem_type(base);
    let parse_bound = |txt: &str| -> PgResult<Option<Value>> {
        let txt = txt.trim();
        if txt.is_empty() {
            return Ok(None);
        }
        let unquoted = if let Some(q) = txt.strip_prefix('"').and_then(|x| x.strip_suffix('"')) {
            q
        } else {
            txt
        };
        Ok(Some(types::from_text(unquoted, elem, ctx)?))
    };
    let lower = match parse_bound(lo_txt)? {
        None => Bound::Unbounded,
        Some(v) if lower_inc => Bound::Inclusive(v),
        Some(v) => Bound::Exclusive(v),
    };
    let upper = match parse_bound(hi_txt)? {
        None => Bound::Unbounded,
        Some(v) if upper_inc => Bound::Inclusive(v),
        Some(v) => Bound::Exclusive(v),
    };
    canonicalize(Range { empty: false, lower, upper }, base)
}

/// Splits `lower,upper` on the one comma that separates them (a quoted
/// bound may itself contain a comma).
fn split_bounds(s: &str) -> Option<(&str, &str)> {
    let mut in_quotes = false;
    let mut chars = s.char_indices();
    for (i, c) in &mut chars {
        match c {
            '"' => in_quotes = !in_quotes,
            ',' if !in_quotes => return Some((&s[..i], &s[i + 1..])),
            _ => {}
        }
    }
    None
}

/// Applies discrete step-adjustment (int4/int8/date only) and then the
/// order check every range type gets: `lower > upper` is an error, `lower
/// == upper` is empty unless both bounds are inclusive (a single point).
fn canonicalize(r: Range, base: Base) -> PgResult<Range> {
    if r.empty {
        return Ok(r);
    }
    let (lower, upper) = if is_discrete(base) {
        (
            match r.lower {
                Bound::Exclusive(v) => Bound::Inclusive(step(&v, base)),
                other => other,
            },
            match r.upper {
                Bound::Inclusive(v) => Bound::Exclusive(step(&v, base)),
                other => other,
            },
        )
    } else {
        (r.lower, r.upper)
    };
    if let (
        Bound::Inclusive(lo) | Bound::Exclusive(lo),
        Bound::Inclusive(hi) | Bound::Exclusive(hi),
    ) = (&lower, &upper)
    {
        match cmp(lo, hi) {
            std::cmp::Ordering::Greater => {
                return Err(PgError::new(
                    code::DATA_EXCEPTION,
                    "range lower bound must be less than or equal to range upper bound",
                ));
            }
            std::cmp::Ordering::Equal
                if !(matches!(lower, Bound::Inclusive(_))
                    && matches!(upper, Bound::Inclusive(_))) =>
            {
                return Ok(Range { empty: true, lower: Bound::Unbounded, upper: Bound::Unbounded });
            }
            _ => {}
        }
    }
    Ok(Range { empty: false, lower, upper })
}

fn malformed(s: &str) -> PgError {
    PgError::new(code::INVALID_TEXT_REPRESENTATION, format!("malformed range literal: \"{s}\""))
}

/// The canonical text a range value prints as.
pub fn format(r: &Range, base: Base, fmt: &types::FmtCtx) -> String {
    if r.empty {
        return "empty".into();
    }
    let elem = elem_type(base);
    let quote = |v: &Value| {
        let s = types::to_text(v, elem, fmt);
        if s.contains([',', '"', '(', ')', '[', ']'].as_slice())
            || s.chars().any(char::is_whitespace)
        {
            format!("\"{}\"", s.replace('"', "\"\""))
        } else {
            s
        }
    };
    // Unbounded always displays as the "open" bracket on that side: there's
    // no value there for inclusive/exclusive to describe.
    let (lo_ch, lo_txt) = match &r.lower {
        Bound::Unbounded => ('(', String::new()),
        Bound::Inclusive(v) => ('[', quote(v)),
        Bound::Exclusive(v) => ('(', quote(v)),
    };
    let (hi_ch, hi_txt) = match &r.upper {
        Bound::Unbounded => (')', String::new()),
        Bound::Inclusive(v) => (']', quote(v)),
        Bound::Exclusive(v) => (')', quote(v)),
    };
    format!("{lo_ch}{lo_txt},{hi_txt}{hi_ch}")
}

fn lower_le(a: &Bound, b: &Bound) -> bool {
    match (a, b) {
        (Bound::Unbounded, _) => true,
        (_, Bound::Unbounded) => false,
        (Bound::Inclusive(x) | Bound::Exclusive(x), Bound::Inclusive(y) | Bound::Exclusive(y)) => {
            match cmp(x, y) {
                std::cmp::Ordering::Equal => {
                    matches!(a, Bound::Inclusive(_)) || matches!(b, Bound::Exclusive(_))
                }
                o => o.is_le(),
            }
        }
    }
}

fn upper_ge(a: &Bound, b: &Bound) -> bool {
    match (a, b) {
        (Bound::Unbounded, _) => true,
        (_, Bound::Unbounded) => false,
        (Bound::Inclusive(x) | Bound::Exclusive(x), Bound::Inclusive(y) | Bound::Exclusive(y)) => {
            match cmp(x, y) {
                std::cmp::Ordering::Equal => {
                    matches!(a, Bound::Inclusive(_)) || matches!(b, Bound::Exclusive(_))
                }
                o => o.is_ge(),
            }
        }
    }
}

/// `@>` (range contains range).
pub fn contains_range(a: &Range, b: &Range) -> bool {
    if b.empty {
        return true;
    }
    if a.empty {
        return false;
    }
    lower_le(&a.lower, &b.lower) && upper_ge(&a.upper, &b.upper)
}

/// `@>` (range contains a single element).
pub fn contains_elem(r: &Range, v: &Value) -> bool {
    if r.empty {
        return false;
    }
    let above_lower = match &r.lower {
        Bound::Unbounded => true,
        Bound::Inclusive(lo) => cmp(v, lo).is_ge(),
        Bound::Exclusive(lo) => cmp(v, lo).is_gt(),
    };
    let below_upper = match &r.upper {
        Bound::Unbounded => true,
        Bound::Inclusive(hi) => cmp(v, hi).is_le(),
        Bound::Exclusive(hi) => cmp(v, hi).is_lt(),
    };
    above_lower && below_upper
}

/// Whether every element of `a` is strictly less than every element of
/// `b` (so the two ranges don't touch at all).
fn entirely_before(a: &Range, b: &Range) -> bool {
    let (Bound::Inclusive(au) | Bound::Exclusive(au)) = &a.upper else { return false };
    let (Bound::Inclusive(bl) | Bound::Exclusive(bl)) = &b.lower else { return false };
    match cmp(au, bl) {
        std::cmp::Ordering::Less => true,
        std::cmp::Ordering::Equal => {
            !(matches!(a.upper, Bound::Inclusive(_)) && matches!(b.lower, Bound::Inclusive(_)))
        }
        std::cmp::Ordering::Greater => false,
    }
}

/// `&&` (overlaps).
pub fn overlaps(a: &Range, b: &Range) -> bool {
    if a.empty || b.empty {
        return false;
    }
    !entirely_before(a, b) && !entirely_before(b, a)
}

impl Range {
    pub fn lower_value(&self) -> Value {
        match &self.lower {
            Bound::Inclusive(v) | Bound::Exclusive(v) => v.clone(),
            Bound::Unbounded => Value::Null,
        }
    }

    pub fn upper_value(&self) -> Value {
        match &self.upper {
            Bound::Inclusive(v) | Bound::Exclusive(v) => v.clone(),
            Bound::Unbounded => Value::Null,
        }
    }
}

/// `int4range(a, b[, bounds])` and friends: builds the canonical text form
/// from two bound values (`Value::Null` for unbounded) and an optional
/// bounds spec (default `[)`).
pub fn construct(lo: Value, hi: Value, bounds: &str, base: Base) -> PgResult<String> {
    if bounds.len() != 2
        || !matches!(bounds.as_bytes()[0], b'[' | b'(')
        || !matches!(bounds.as_bytes()[1], b')' | b']')
    {
        return Err(PgError::new(
            code::INVALID_PARAMETER_VALUE,
            format!("invalid preceding or trailing character in \"{bounds}\""),
        ));
    }
    let lower = if lo.is_null() {
        Bound::Unbounded
    } else if bounds.as_bytes()[0] == b'[' {
        Bound::Inclusive(lo)
    } else {
        Bound::Exclusive(lo)
    };
    let upper = if hi.is_null() {
        Bound::Unbounded
    } else if bounds.as_bytes()[1] == b']' {
        Bound::Inclusive(hi)
    } else {
        Bound::Exclusive(hi)
    };
    let r = canonicalize(Range { empty: false, lower, upper }, base)?;
    let fmt = types::FmtCtx::default();
    Ok(format(&r, base, &fmt))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx() -> Ctx<'static> {
        Ctx { now: 0, zone: Box::leak(Box::new(super::super::tz::Zone::utc())) }
    }

    #[test]
    fn discrete_canonicalizes_to_half_open() {
        let r = parse("[1,10]", Base::Int4Range, &ctx()).unwrap();
        assert_eq!(format(&r, Base::Int4Range, &types::FmtCtx::default()), "[1,11)");
        let r = parse("(,10)", Base::Int4Range, &ctx()).unwrap();
        assert_eq!(format(&r, Base::Int4Range, &types::FmtCtx::default()), "(,10)");
        // An unbounded side always shows as the open bracket, whichever was
        // actually given.
        let r = parse("[1,)", Base::Int4Range, &ctx()).unwrap();
        assert_eq!(format(&r, Base::Int4Range, &types::FmtCtx::default()), "[1,)");
    }

    #[test]
    fn empty_range() {
        let r = parse("empty", Base::Int4Range, &ctx()).unwrap();
        assert!(r.empty);
        assert_eq!(format(&r, Base::Int4Range, &types::FmtCtx::default()), "empty");
        // Canonicalizes to empty when the (adjusted) bounds don't leave room.
        let r = parse("[5,5)", Base::Int4Range, &ctx()).unwrap();
        assert!(r.empty);
        let r = parse("[5,5)", Base::NumRange, &ctx()).unwrap();
        assert!(r.empty);
        // Both-inclusive at the same point is a real single-point range.
        let r = parse("[5,5]", Base::NumRange, &ctx()).unwrap();
        assert!(!r.empty);
        assert_eq!(format(&r, Base::NumRange, &types::FmtCtx::default()), "[5,5]");
    }

    #[test]
    fn backwards_bounds_error() {
        assert!(parse("(10,5]", Base::NumRange, &ctx()).is_err());
        assert!(parse("[10,5)", Base::Int4Range, &ctx()).is_err());
    }

    #[test]
    fn quotes_values_with_internal_whitespace() {
        let r = parse("[2020-01-01 10:00:00,2020-01-01 12:00:00)", Base::TsRange, &ctx()).unwrap();
        assert_eq!(
            format(&r, Base::TsRange, &types::FmtCtx::default()),
            "[\"2020-01-01 10:00:00\",\"2020-01-01 12:00:00\")"
        );
    }

    #[test]
    fn contains_and_overlaps() {
        let a = parse("[1,10)", Base::Int4Range, &ctx()).unwrap();
        let b = parse("[3,8)", Base::Int4Range, &ctx()).unwrap();
        let c = parse("[8,20)", Base::Int4Range, &ctx()).unwrap();
        assert!(contains_range(&a, &b));
        assert!(!contains_range(&b, &a));
        assert!(contains_elem(&a, &Value::Int(5)));
        assert!(!contains_elem(&a, &Value::Int(10)));
        assert!(overlaps(&a, &c));
        assert!(!overlaps(&b, &c));
    }

    #[test]
    fn construct_matches_parse() {
        let s = construct(Value::Int(1), Value::Int(10), "[)", Base::Int4Range).unwrap();
        assert_eq!(s, "[1,10)");
        let s = construct(Value::Int(1), Value::Int(10), "[]", Base::Int4Range).unwrap();
        assert_eq!(s, "[1,11)");
    }

    #[test]
    fn numeric_range_keeps_its_own_bounds() {
        let r = parse("(1.5,10.5]", Base::NumRange, &ctx()).unwrap();
        assert_eq!(format(&r, Base::NumRange, &types::FmtCtx::default()), "(1.5,10.5]");
    }
}
