//! Type conversion rules (pg_cast) and value conversion.

use super::datetime::{self, USECS_PER_DAY, USECS_PER_SEC};
use super::error::{PgError, PgResult, code};
use super::json::{self, Json};
use super::numeric::Numeric;
use super::types::{self, Array, Base, FmtCtx, Type, Value};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum CastCtx {
    Implicit,
    Assignment,
    Explicit,
}

/// The built-in casts between distinct base types, from Postgres 16's pg_cast.
const CASTS: &str = "bit>int4:e bit>int8:e bool>bpchar:a bool>int4:e bool>text:a bool>varchar:a \
bpchar>char:a bpchar>name:i bpchar>text:i bpchar>varchar:i char>bpchar:a char>int4:e char>text:i char>varchar:a \
cidr>inet:i date>timestamp:i date>timestamptz:i float4>float8:i float4>int2:a float4>int4:a float4>int8:a \
float4>numeric:a float8>float4:a float8>int2:a float8>int4:a float8>int8:a float8>numeric:a inet>cidr:a \
int2>float4:i int2>float8:i int2>int4:i int2>int8:i int2>numeric:i int2>oid:i int2>regclass:i int2>regnamespace:i \
int2>regproc:i int2>regrole:i int2>regtype:i int4>bit:e int4>bool:e int4>char:e int4>float4:i int4>float8:i \
int4>int2:a int4>int8:i int4>money:a int4>numeric:i int4>oid:i int4>regclass:i int4>regnamespace:i int4>regproc:i \
int4>regrole:i int4>regtype:i int8>bit:e int8>float4:i int8>float8:i int8>int2:a int8>int4:a int8>money:a \
int8>numeric:i int8>oid:i int8>regclass:i int8>regnamespace:i int8>regproc:i int8>regrole:i int8>regtype:i \
interval>time:a json>jsonb:a jsonb>bool:e jsonb>float4:e jsonb>float8:e jsonb>int2:e jsonb>int4:e jsonb>int8:e \
jsonb>json:a jsonb>numeric:e money>numeric:a name>bpchar:a name>text:i name>varchar:a numeric>float4:i \
numeric>float8:i numeric>int2:a numeric>int4:a numeric>int8:a numeric>money:a oid>int4:a oid>int8:a oid>regclass:i \
oid>regnamespace:i oid>regproc:i oid>regrole:i oid>regtype:i regclass>int4:a regclass>int8:a regclass>oid:i \
regnamespace>int4:a regnamespace>int8:a regnamespace>oid:i regproc>int4:a regproc>int8:a regproc>oid:i \
regrole>int4:a regrole>int8:a regrole>oid:i regtype>int4:a regtype>int8:a regtype>oid:i text>bpchar:i text>char:a \
text>name:i text>regclass:i text>varchar:i time>interval:i time>timetz:i timestamp>date:a timestamp>time:a \
timestamp>timestamptz:i timestamptz>date:a timestamptz>time:a timestamptz>timestamp:a timestamptz>timetz:a \
timetz>time:a varchar>bpchar:i varchar>char:a varchar>name:i varchar>regclass:i varchar>text:i \
regprocedure>oid:i oid>regprocedure:i int4>regprocedure:i int8>regprocedure:i regoper>oid:i oid>regoper:i";

fn builtin_cast(from: Base, to: Base) -> Option<CastCtx> {
    let (f, t) = (from.info()?.name, to.info()?.name);
    for c in CASTS.split_ascii_whitespace() {
        let (pair, ctx) = c.split_once(':')?;
        let (a, b) = pair.split_once('>')?;
        if a == f && b == t {
            return Some(match ctx {
                "i" => CastCtx::Implicit,
                "a" => CastCtx::Assignment,
                _ => CastCtx::Explicit,
            });
        }
    }
    None
}

/// The weakest context in which `from` converts to `to`, or None.
pub fn cast_context(from: Type, to: Type) -> Option<CastCtx> {
    if from == to {
        return Some(CastCtx::Implicit);
    }
    if from.is_unknown() {
        return Some(CastCtx::Implicit);
    }
    if to.base == Base::Any || (to.base == Base::AnyElement && !to.array) {
        return Some(CastCtx::Implicit);
    }
    if to.base == Base::AnyArray && to.array {
        return from.array.then_some(CastCtx::Implicit);
    }
    if from.array && to.array {
        return cast_context(from.elem(), to.elem());
    }
    if from.array != to.array {
        // Arrays convert to strings via I/O; nothing else.
        return if !from.array && false {
            None
        } else if to.is_string() && from.array {
            Some(CastCtx::Assignment)
        } else if from.is_string() && to.array {
            Some(CastCtx::Explicit)
        } else {
            None
        };
    }
    if let Some(c) = builtin_cast(from.base, to.base) {
        return Some(c);
    }
    if matches!(from.base, Base::Enum(_)) && to.is_string() {
        return Some(CastCtx::Assignment);
    }
    // Automatic I/O conversion casts.
    if to.is_string() {
        return Some(CastCtx::Assignment);
    }
    if from.is_string() {
        return Some(CastCtx::Explicit);
    }
    // Record and pseudo types have no casts.
    None
}

fn range_err(ty: Type) -> PgError {
    PgError::new(code::NUMERIC_VALUE_OUT_OF_RANGE, types::int_range_msg(ty))
}

/// float → integer with rint() (round half to even).
fn float_to_int(x: f64, to: Type) -> PgResult<i64> {
    if x.is_nan() || x.is_infinite() {
        return Err(range_err(to));
    }
    let r = x.round_ties_even();
    if r < i64::MIN as f64 || r >= 9.223_372_036_854_776e18 {
        return Err(range_err(to));
    }
    types::check_int_range(r as i64, to)
}

/// Converts `v` of type `from` to `to` (then applies `typmod`).
pub fn cast(
    v: Value,
    from: Type,
    to: Type,
    typmod: i32,
    explicit: bool,
    fmt: &FmtCtx,
    now: i64,
) -> PgResult<Value> {
    if v.is_null() {
        return Ok(Value::Null);
    }
    let out = convert(v, from, to, fmt, now)?;
    types::apply_typmod(out, to, typmod, explicit)
}

fn convert(v: Value, from: Type, to: Type, fmt: &FmtCtx, now: i64) -> PgResult<Value> {
    if from == to
        || to.base == Base::Any
        || to.base == Base::AnyElement
        || to.base == Base::AnyArray
    {
        return Ok(v);
    }
    let dctx = datetime::Ctx { now, zone: &fmt.zone };
    if from.array && to.array {
        return match v {
            Value::Array(a) => {
                let Array { dims, items } = *a;
                let items = items
                    .into_iter()
                    .map(|x| {
                        if x.is_null() {
                            Ok(Value::Null)
                        } else {
                            convert(x, from.elem(), to.elem(), fmt, now)
                        }
                    })
                    .collect::<PgResult<Vec<_>>>()?;
                Ok(Value::Array(Box::new(Array { dims, items })))
            }
            other => Ok(other),
        };
    }
    // From unknown/text-like: parse.
    if from.is_unknown()
        || (from.is_string() && !to.is_string())
        || (from.base == Base::Char && !to.is_string())
    {
        let s = match &v {
            Value::Text(s) => s.clone(),
            other => types::to_text(other, from, fmt),
        };
        let s = if from.base == Base::Bpchar { s.trim_end_matches(' ').to_string() } else { s };
        return types::from_text(&s, to, &dctx);
    }
    // To string types.
    if to.is_string() || to.base == Base::Char {
        let s = match (&v, from.base) {
            (Value::Bool(b), Base::Bool) => if *b { "true" } else { "false" }.to_string(),
            (Value::Text(s), Base::Bpchar) if to.base != Base::Bpchar => {
                s.trim_end_matches(' ').to_string()
            }
            (Value::Text(s), _) => s.clone(),
            _ => types::to_text(&v, from, fmt),
        };
        return Ok(Value::Text(match to.base {
            Base::Name => types::truncate_name(&s),
            Base::Char => s.chars().next().map(String::from).unwrap_or_default(),
            _ => s,
        }));
    }
    let tb = to.base;
    Ok(match (v, from.base) {
        (Value::Int(i), _) if to.is_integer() || matches!(tb, Base::Oid) || to.is_reg() => {
            Value::Int(types::check_int_range(i, to).map_err(|_| range_err(to))?)
        }
        (Value::Int(i), _) if matches!(tb, Base::Float4 | Base::Float8) => {
            Value::Float(if tb == Base::Float4 { i as f32 as f64 } else { i as f64 })
        }
        (Value::Int(i), _) if tb == Base::Numeric => Value::Num(Numeric::from_i64(i)),
        (Value::Int(i), _) if tb == Base::Bool => Value::Bool(i != 0),
        (Value::Int(i), _) if tb == Base::Money => Value::Int(i * 100),
        (Value::Float(x), _) if to.is_integer() => Value::Int(float_to_int(x, to)?),
        (Value::Float(x), _) if tb == Base::Float4 => {
            let f = x as f32;
            if f.is_infinite() && x.is_finite() {
                return Err(PgError::new(
                    code::NUMERIC_VALUE_OUT_OF_RANGE,
                    "value out of range: overflow",
                ));
            }
            if f == 0.0 && x != 0.0 {
                return Err(PgError::new(
                    code::NUMERIC_VALUE_OUT_OF_RANGE,
                    "value out of range: underflow",
                ));
            }
            Value::Float(f as f64)
        }
        (Value::Float(x), _) if tb == Base::Float8 => Value::Float(x),
        (Value::Float(x), b) if tb == Base::Numeric => Value::Num(if b == Base::Float4 {
            Numeric::from_f32(x as f32)
        } else {
            Numeric::from_f64(x)
        }),
        (Value::Num(n), _) if to.is_integer() => {
            if n.is_nan() {
                return Err(PgError::new(
                    code::FEATURE_NOT_SUPPORTED,
                    format!("cannot convert NaN to {}", to.display(-1)),
                ));
            }
            if matches!(n, Numeric::Inf(_)) {
                return Err(PgError::new(
                    code::FEATURE_NOT_SUPPORTED,
                    format!("cannot convert infinity to {}", to.display(-1)),
                ));
            }
            let i = n.to_i64().ok_or_else(|| range_err(to))?;
            Value::Int(types::check_int_range(i, to).map_err(|_| range_err(to))?)
        }
        (Value::Num(n), _) if matches!(tb, Base::Float4 | Base::Float8) => {
            let x = n.to_f64();
            Value::Float(if tb == Base::Float4 { x as f32 as f64 } else { x })
        }
        (Value::Num(n), _) if tb == Base::Numeric => Value::Num(n),
        (Value::Bool(b), _) if tb == Base::Int4 => Value::Int(b as i64),
        (Value::Date(d), _) if tb == Base::Timestamp => Value::Ts(date_to_ts(d)),
        (Value::Date(d), _) if tb == Base::Timestamptz => {
            let local = date_to_ts(d);
            Value::Ts(datetime::local_to_utc(local, &fmt.zone))
        }
        (Value::Ts(t), Base::Timestamp) if tb == Base::Date => Value::Date(ts_to_date(t)),
        (Value::Ts(t), Base::Timestamptz) if tb == Base::Date => {
            Value::Date(ts_to_date(datetime::utc_to_local(t, &fmt.zone)))
        }
        (Value::Ts(t), Base::Timestamp) if tb == Base::Timestamptz => {
            Value::Ts(datetime::local_to_utc(t, &fmt.zone))
        }
        (Value::Ts(t), Base::Timestamptz) if tb == Base::Timestamp => {
            Value::Ts(datetime::utc_to_local(t, &fmt.zone))
        }
        (Value::Ts(t), Base::Timestamp) if tb == Base::Time => {
            Value::Time(t.rem_euclid(USECS_PER_DAY))
        }
        (Value::Ts(t), Base::Timestamptz) if tb == Base::Time => {
            Value::Time(datetime::utc_to_local(t, &fmt.zone).rem_euclid(USECS_PER_DAY))
        }
        (Value::Ts(t), Base::Timestamptz) if tb == Base::Timetz => {
            let off = fmt
                .zone
                .offset_at_utc(t.div_euclid(USECS_PER_SEC) - datetime::PG_EPOCH_DAYS * 86400);
            Value::TimeTz(datetime::utc_to_local(t, &fmt.zone).rem_euclid(USECS_PER_DAY), off)
        }
        (Value::Time(t), _) if tb == Base::Interval => {
            Value::Interval(datetime::Interval { months: 0, days: 0, micros: t })
        }
        (Value::Time(t), _) if tb == Base::Timetz => Value::TimeTz(
            t,
            fmt.zone.offset_at_utc(now / USECS_PER_SEC + datetime::PG_EPOCH_DAYS * 86400),
        ),
        (Value::TimeTz(t, _), _) if tb == Base::Time => Value::Time(t),
        (Value::Interval(iv), _) if tb == Base::Time => {
            Value::Time(iv.micros.rem_euclid(USECS_PER_DAY))
        }
        (Value::Text(s), Base::Json) if tb == Base::Jsonb => Value::Jsonb(Box::new(
            json::parse_jsonb(&s).map_err(|e| types::invalid_input("json", &s).detail(e.0))?,
        )),
        (Value::Jsonb(j), _) if tb == Base::Json => Value::Text(j.to_jsonb_string()),
        (Value::Jsonb(j), _) => jsonb_to_scalar(&j, to)?,
        (v, _) => {
            // Anything else goes through text.
            let s = types::to_text(&v, from, fmt);
            if cast_context(from, to).is_none() {
                return Err(cannot_cast(from, to));
            }
            types::from_text(&s, to, &dctx)?
        }
    })
}

pub fn cannot_cast(from: Type, to: Type) -> PgError {
    PgError::new(
        code::CANNOT_COERCE,
        format!("cannot cast type {} to {}", from.display(-1), to.display(-1)),
    )
}

fn jsonb_to_scalar(j: &Json, to: Type) -> PgResult<Value> {
    let tname = to.display(-1);
    let wrong = |what: &str| {
        PgError::new(
            code::INVALID_PARAMETER_VALUE,
            format!("cannot cast jsonb {what} to type {tname}"),
        )
    };
    match (j, to.base) {
        (Json::Bool(b), Base::Bool) => Ok(Value::Bool(*b)),
        (Json::Num(n), Base::Numeric) => Ok(Value::Num(n.clone())),
        (Json::Num(n), Base::Float8 | Base::Float4) => Ok(Value::Float(n.to_f64())),
        (Json::Num(n), Base::Int2 | Base::Int4 | Base::Int8) => {
            let i = n.to_i64().ok_or_else(|| range_err(to))?;
            Ok(Value::Int(types::check_int_range(i, to).map_err(|_| range_err(to))?))
        }
        (Json::Null, _) => Err(wrong("null")),
        (Json::Str(_), _) => Err(wrong("string")),
        (Json::Num(_), _) => Err(wrong("numeric")),
        (Json::Bool(_), _) => Err(wrong("boolean")),
        (Json::Array(_), _) => Err(wrong("array")),
        (Json::Object(_), _) => Err(wrong("object")),
    }
}

pub fn date_to_ts(d: i32) -> i64 {
    match d {
        datetime::DATE_INF => datetime::TS_INF,
        datetime::DATE_NEG_INF => datetime::TS_NEG_INF,
        _ => d as i64 * USECS_PER_DAY,
    }
}

pub fn ts_to_date(t: i64) -> i32 {
    match t {
        datetime::TS_INF => datetime::DATE_INF,
        datetime::TS_NEG_INF => datetime::DATE_NEG_INF,
        _ => t.div_euclid(USECS_PER_DAY) as i32,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contexts() {
        assert_eq!(cast_context(Type::INT4, Type::INT8), Some(CastCtx::Implicit));
        assert_eq!(cast_context(Type::INT8, Type::INT4), Some(CastCtx::Assignment));
        assert_eq!(cast_context(Type::TEXT, Type::INT4), Some(CastCtx::Explicit));
        assert_eq!(cast_context(Type::INT4, Type::TEXT), Some(CastCtx::Assignment));
        assert_eq!(cast_context(Type::BOOL, Type::INT8), None);
        assert_eq!(
            cast_context(Type::array_of(Base::Int4), Type::array_of(Base::Int8)),
            Some(CastCtx::Implicit)
        );
    }

    #[test]
    fn conversions() {
        let f = FmtCtx::default();
        let c = |v, from, to| cast(v, from, to, -1, true, &f, 0).unwrap();
        assert!(matches!(c(Value::Float(2.5), Type::FLOAT8, Type::INT4), Value::Int(2)));
        assert!(matches!(c(Value::Float(3.5), Type::FLOAT8, Type::INT4), Value::Int(4)));
        assert!(matches!(
            c(Value::Num(Numeric::parse("2.5").unwrap()), Type::NUMERIC, Type::INT4),
            Value::Int(3)
        ));
        assert_eq!(c(Value::Bool(true), Type::BOOL, Type::TEXT).as_str(), Some("true"));
        assert!(cast(Value::Int(70000), Type::INT4, Type::INT2, -1, true, &f, 0).is_err());
    }
}
