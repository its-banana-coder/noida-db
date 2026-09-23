//! Built-in scalar functions and operators that need no catalog access.

use super::casts;
use super::datetime::{self, USECS_PER_DAY, USECS_PER_SEC};
use super::error::{PgError, PgResult, code};
use super::json::{self, Json};
use super::numeric::{NumError, Numeric};
use super::types::{self, Array, Base, FmtCtx, Type, Value};

/// What pure functions may read from the session.
pub struct Env<'a> {
    pub fmt: &'a FmtCtx,
    /// Transaction start (now()), microseconds since 2000-01-01 UTC.
    pub now: i64,
    pub stmt_now: i64,
}

impl Env<'_> {
    pub fn dctx(&self) -> datetime::Ctx<'_> {
        datetime::Ctx { now: self.now, zone: &self.fmt.zone }
    }
}

pub fn err(code: &'static str, msg: impl Into<String>) -> PgError {
    PgError::new(code, msg)
}

fn div_zero() -> PgError {
    err(code::DIVISION_BY_ZERO, "division by zero")
}

fn num_err(e: NumError) -> PgError {
    match e {
        NumError::DivByZero => div_zero(),
        NumError::Overflow => {
            err(code::NUMERIC_VALUE_OUT_OF_RANGE, "value overflows numeric format")
        }
        NumError::Syntax => err(code::INVALID_TEXT_REPRESENTATION, "invalid numeric"),
    }
}

fn dt_range(e: datetime::DtErr, what: &str) -> PgError {
    let _ = e;
    err(code::DATETIME_FIELD_OVERFLOW, format!("{what} out of range"))
}

fn float_check(x: f64, inputs_finite: bool) -> PgResult<Value> {
    if x.is_infinite() && inputs_finite {
        return Err(err(code::NUMERIC_VALUE_OUT_OF_RANGE, "value out of range: overflow"));
    }
    Ok(Value::Float(x))
}

pub fn as_num(v: &Value) -> Numeric {
    match v {
        Value::Num(n) => n.clone(),
        Value::Int(i) => Numeric::from_i64(*i),
        Value::Float(f) => Numeric::from_f64(*f),
        _ => Numeric::NaN,
    }
}

pub fn as_f64(v: &Value) -> f64 {
    match v {
        Value::Float(f) => *f,
        Value::Int(i) => *i as f64,
        Value::Num(n) => n.to_f64(),
        _ => f64::NAN,
    }
}

fn int_result(v: Option<i64>, ty: Type) -> PgResult<Value> {
    let v = v.ok_or_else(|| err(code::NUMERIC_VALUE_OUT_OF_RANGE, types::int_range_msg(ty)))?;
    types::check_int_range(v, ty).map(Value::Int)
}

fn text(v: &Value) -> &str {
    match v {
        Value::Text(s) => s,
        _ => "",
    }
}

fn int(v: &Value) -> i64 {
    match v {
        Value::Int(i) => *i,
        Value::Float(f) => *f as i64,
        Value::Num(n) => n.to_i64().unwrap_or(0),
        _ => 0,
    }
}

/// Arithmetic and other operators, dispatched on the operand values.
pub fn binop(
    op: &str,
    a: &Value,
    b: &Value,
    ret: Type,
    tys: &[Type],
    env: &Env,
) -> PgResult<Value> {
    use Value::*;
    let zone = &env.fmt.zone;
    Ok(match (op, a, b) {
        ("+", Int(x), Int(y)) => return int_result(x.checked_add(*y), ret),
        ("-", Int(x), Int(y)) => return int_result(x.checked_sub(*y), ret),
        ("*", Int(x), Int(y)) => return int_result(x.checked_mul(*y), ret),
        ("/", Int(x), Int(y)) => {
            if *y == 0 {
                return Err(div_zero());
            }
            return int_result(x.checked_div(*y), ret);
        }
        ("%", Int(x), Int(y)) => {
            if *y == 0 {
                return Err(div_zero());
            }
            Int(x.checked_rem(*y).unwrap_or(0))
        }
        ("+" | "-" | "*" | "/" | "%" | "^", _, _) if ret.base == Base::Numeric && !ret.array => {
            let (x, y) = (as_num(a), as_num(b));
            Num(match op {
                "+" => x.add(&y),
                "-" => x.sub(&y),
                "*" => x.mul(&y),
                "/" => x.div(&y).map_err(num_err)?,
                "%" => x.rem(&y).map_err(num_err)?,
                _ => numeric_power(&x, &y)?,
            })
        }
        ("+" | "-" | "*" | "/" | "^", _, _)
            if matches!(ret.base, Base::Float4 | Base::Float8) && !ret.array =>
        {
            let (x, y) = (as_f64(a), as_f64(b));
            let fin = x.is_finite() && y.is_finite();
            let r = match op {
                "+" => x + y,
                "-" => x - y,
                "*" => x * y,
                "/" => {
                    if y == 0.0 {
                        return Err(div_zero());
                    }
                    x / y
                }
                _ => {
                    if x == 0.0 && y < 0.0 {
                        return Err(err(
                            code::INVALID_ARGUMENT_FOR_POWER,
                            "zero raised to a negative power is undefined",
                        ));
                    }
                    if x < 0.0 && y.fract() != 0.0 {
                        return Err(err(
                            code::INVALID_ARGUMENT_FOR_POWER,
                            "a negative number raised to a non-integer power yields a complex result",
                        ));
                    }
                    x.powf(y)
                }
            };
            let r = if ret.base == Base::Float4 { r as f32 as f64 } else { r };
            if r == 0.0 && x != 0.0 && op == "*" && y != 0.0 {
                return Err(err(code::NUMERIC_VALUE_OUT_OF_RANGE, "value out of range: underflow"));
            }
            return float_check(r, fin);
        }
        // Date/time arithmetic.
        ("+", Date(d), Int(n)) | ("+", Int(n), Date(d)) => Date(add_days(*d, *n)?),
        ("-", Date(d), Int(n)) => Date(add_days(*d, -*n)?),
        ("-", Date(x), Date(y)) => {
            if matches!(*x, datetime::DATE_INF | datetime::DATE_NEG_INF)
                || matches!(*y, datetime::DATE_INF | datetime::DATE_NEG_INF)
            {
                return Err(err(code::DATETIME_FIELD_OVERFLOW, "cannot subtract infinite dates"));
            }
            Int(*x as i64 - *y as i64)
        }
        ("+", Date(d), Interval(iv)) | ("+", Interval(iv), Date(d)) => {
            Ts(datetime::timestamp_add(casts::date_to_ts(*d), iv)
                .map_err(|e| dt_range(e, "timestamp"))?)
        }
        ("-", Date(d), Interval(iv)) => Ts(datetime::timestamp_add(
            casts::date_to_ts(*d),
            &iv.neg().map_err(|e| dt_range(e, "interval"))?,
        )
        .map_err(|e| dt_range(e, "timestamp"))?),
        ("+", Date(d), Time(t)) | ("+", Time(t), Date(d)) => Ts(casts::date_to_ts(*d) + t),
        ("+", Ts(t), Interval(iv)) | ("+", Interval(iv), Ts(t)) => {
            if ret.base == Base::Timestamptz {
                Ts(datetime::timestamptz_add(*t, iv, zone).map_err(|e| dt_range(e, "timestamp"))?)
            } else {
                Ts(datetime::timestamp_add(*t, iv).map_err(|e| dt_range(e, "timestamp"))?)
            }
        }
        ("-", Ts(t), Interval(iv)) => {
            let n = iv.neg().map_err(|e| dt_range(e, "interval"))?;
            if ret.base == Base::Timestamptz {
                Ts(datetime::timestamptz_add(*t, &n, zone).map_err(|e| dt_range(e, "timestamp"))?)
            } else {
                Ts(datetime::timestamp_add(*t, &n).map_err(|e| dt_range(e, "timestamp"))?)
            }
        }
        ("-", Ts(x), Ts(y)) => Interval(datetime::timestamp_diff(*x, *y).map_err(|_| {
            err(code::DATETIME_FIELD_OVERFLOW, "cannot subtract infinite timestamps")
        })?),
        ("+", Time(t), Interval(iv)) | ("+", Interval(iv), Time(t)) => {
            Time((t + iv.micros).rem_euclid(USECS_PER_DAY))
        }
        ("-", Time(t), Interval(iv)) => Time((t - iv.micros).rem_euclid(USECS_PER_DAY)),
        ("-", Time(x), Time(y)) => {
            Interval(datetime::Interval { months: 0, days: 0, micros: x - y })
        }
        ("+", Interval(x), Interval(y)) => Interval(x.add(y).map_err(|e| dt_range(e, "interval"))?),
        ("-", Interval(x), Interval(y)) => Interval(x.sub(y).map_err(|e| dt_range(e, "interval"))?),
        ("*", Interval(iv), n) | ("*", n, Interval(iv)) => {
            Interval(iv.mul(as_f64(n)).map_err(|e| dt_range(e, "interval"))?)
        }
        ("/", Interval(iv), n) => {
            let f = as_f64(n);
            if f == 0.0 {
                return Err(div_zero());
            }
            Interval(iv.div(f).map_err(|e| dt_range(e, "interval"))?)
        }
        // Strings.
        ("||", _, _) => {
            if ret.array {
                return array_concat_op(a, b, tys);
            }
            if ret.base == Base::Jsonb {
                return Ok(Jsonb(Box::new(jsonb_concat(jv(a), jv(b)))));
            }
            if ret.base == Base::Bytea {
                let (Bytes(x), Bytes(y)) = (a, b) else { return Ok(Null) };
                let mut v = x.clone();
                v.extend_from_slice(y);
                return Ok(Bytes(v));
            }
            let sa = to_str(a, tys[0], env);
            let sb = to_str(b, tys[1], env);
            Text(sa + &sb)
        }
        // Bitwise.
        ("&", Int(x), Int(y)) => Int(x & y),
        ("|", Int(x), Int(y)) => Int(x | y),
        ("#", Int(x), Int(y)) => Int(x ^ y),
        ("<<", Int(x), Int(y)) => return int_result(Some(shift(*x, *y, true, ret)), ret),
        (">>", Int(x), Int(y)) => return int_result(Some(shift(*x, *y, false, ret)), ret),
        // JSON.
        ("->", _, _) => return json_get(a, b, tys[0].base == Base::Jsonb, false),
        ("->>", _, _) => return json_get(a, b, tys[0].base == Base::Jsonb, true),
        ("#>", _, _) => return json_path_op(a, b, tys[0].base == Base::Jsonb, false),
        ("#>>", _, _) => return json_path_op(a, b, tys[0].base == Base::Jsonb, true),
        ("@>", Jsonb(x), Jsonb(y)) => Bool(x.contains(y)),
        ("<@", Jsonb(x), Jsonb(y)) => Bool(y.contains(x)),
        ("?", Jsonb(x), Text(k)) => Bool(x.has_key(k)),
        ("?|", Jsonb(x), Array(ks)) => {
            Bool(ks.items.iter().any(|k| matches!(k, Text(k) if x.has_key(k))))
        }
        ("?&", Jsonb(x), Array(ks)) => {
            Bool(ks.items.iter().all(|k| matches!(k, Text(k) if x.has_key(k))))
        }
        ("-", Jsonb(x), Text(k)) => Jsonb(Box::new(jsonb_delete_key(x, k)?)),
        ("-", Jsonb(x), Int(i)) => Jsonb(Box::new(jsonb_delete_index(x, *i)?)),
        ("-", Jsonb(x), Array(ks)) => {
            let mut j = (**x).clone();
            for k in &ks.items {
                if let Text(k) = k {
                    j = jsonb_delete_key(&j, k)?;
                }
            }
            Jsonb(Box::new(j))
        }
        ("#-", Jsonb(x), Array(p)) => Jsonb(Box::new(jsonb_delete_path(x, &text_items(p))?)),
        // Arrays.
        ("@>", Array(x), Array(y)) => Bool(
            y.items
                .iter()
                .filter(|v| !v.is_null())
                .all(|v| x.items.iter().any(|w| types::values_equal(v, w)))
                && !y.items.iter().any(Value::is_null),
        ),
        ("<@", Array(x), Array(y)) => Bool(
            x.items
                .iter()
                .filter(|v| !v.is_null())
                .all(|v| y.items.iter().any(|w| types::values_equal(v, w)))
                && !x.items.iter().any(Value::is_null),
        ),
        ("&&", Array(x), Array(y)) => Bool(
            x.items
                .iter()
                .any(|v| !v.is_null() && y.items.iter().any(|w| types::values_equal(v, w))),
        ),
        // Pattern matching.
        ("~~", Text(s), Text(p)) => Bool(like(s, p, false, Some('\\'))?),
        ("!~~", Text(s), Text(p)) => Bool(!like(s, p, false, Some('\\'))?),
        ("~~*", Text(s), Text(p)) => Bool(like(s, p, true, Some('\\'))?),
        ("!~~*", Text(s), Text(p)) => Bool(!like(s, p, true, Some('\\'))?),
        ("~", Text(s), Text(p)) => Bool(regex(p, "")?.is_match(s)),
        ("!~", Text(s), Text(p)) => Bool(!regex(p, "")?.is_match(s)),
        ("~*", Text(s), Text(p)) => Bool(regex(p, "i")?.is_match(s)),
        ("!~*", Text(s), Text(p)) => Bool(!regex(p, "i")?.is_match(s)),
        ("^@", Text(s), Text(p)) => Bool(s.starts_with(p.as_str())),
        _ => {
            return Err(err(
                code::UNDEFINED_FUNCTION,
                format!(
                    "operator does not exist: {} {op} {}",
                    tys[0].display(-1),
                    tys[1].display(-1)
                ),
            ));
        }
    })
}

fn shift(x: i64, y: i64, left: bool, ty: Type) -> i64 {
    let bits = match ty.base {
        Base::Int2 => 16,
        Base::Int4 => 32,
        _ => 64,
    };
    let s = (y.rem_euclid(bits)) as u32;
    let v = if left { x.wrapping_shl(s) } else { x.wrapping_shr(s) };
    match bits {
        16 => v as i16 as i64,
        32 => v as i32 as i64,
        _ => v,
    }
}

fn add_days(d: i32, n: i64) -> PgResult<i32> {
    if matches!(d, datetime::DATE_INF | datetime::DATE_NEG_INF) {
        return Ok(d);
    }
    let r = d as i64 + n;
    if !(-2_451_545..=2_147_483_494).contains(&r) {
        return Err(err(code::DATETIME_FIELD_OVERFLOW, "date out of range"));
    }
    Ok(r as i32)
}

pub fn to_str(v: &Value, ty: Type, env: &Env) -> String {
    match v {
        Value::Text(s) => {
            if ty.base == Base::Bpchar {
                s.trim_end_matches(' ').to_string()
            } else {
                s.clone()
            }
        }
        Value::Null => String::new(),
        Value::Bool(b) if !ty.array => if *b { "true" } else { "false" }.into(),
        other => types::to_text(other, ty, env.fmt),
    }
}

pub fn unop(op: &str, a: &Value, ret: Type) -> PgResult<Value> {
    use Value::*;
    Ok(match (op, a) {
        ("-", Int(x)) => return int_result(x.checked_neg(), ret),
        ("-", Float(x)) => Float(-x),
        ("-", Num(n)) => Num(n.neg()),
        ("-", Interval(iv)) => Interval(iv.neg().map_err(|e| dt_range(e, "interval"))?),
        ("+", v) => v.clone(),
        ("~", Int(x)) => Int(!x),
        ("@", Int(x)) => return int_result(x.checked_abs(), ret),
        ("@", Float(x)) => Float(x.abs()),
        ("@", Num(n)) => Num(n.abs()),
        ("|/", v) => {
            let x = as_f64(v);
            if x < 0.0 {
                return Err(err(
                    code::INVALID_ARGUMENT_FOR_POWER,
                    "cannot take square root of a negative number",
                ));
            }
            Float(x.sqrt())
        }
        ("||/", v) => Float(as_f64(v).cbrt()),
        _ => return Err(err(code::UNDEFINED_FUNCTION, format!("operator does not exist: {op}"))),
    })
}

fn numeric_power(x: &Numeric, y: &Numeric) -> PgResult<Numeric> {
    if x.is_zero() && y.is_negative() {
        return Err(err(
            code::INVALID_ARGUMENT_FOR_POWER,
            "zero raised to a negative power is undefined",
        ));
    }
    let is_int = y.trunc(0).sub(y).is_zero();
    if x.is_negative() && !is_int {
        return Err(err(
            code::INVALID_ARGUMENT_FOR_POWER,
            "a negative number raised to a non-integer power yields a complex result",
        ));
    }
    if is_int && let Some(e) = y.to_i64() {
        let rscale = 16i64.max(x.scale() as i64);
        if e == 0 {
            return Ok(Numeric::from_i64(1).round(rscale));
        }
        let mut result = Numeric::from_i64(1);
        let mut base = x.clone();
        let mut n = e.unsigned_abs();
        // Exact integer power, then scale.
        while n > 0 {
            if n & 1 == 1 {
                result = result.mul(&base);
            }
            base = base.mul(&base);
            n >>= 1;
            if result.scale() > 2000 {
                result = result.round(rscale + 20);
                base = base.round(rscale + 20);
            }
        }
        if e < 0 {
            return Numeric::from_i64(1).div_scale(&result, rscale, false).map_err(num_err);
        }
        return Ok(result.round(rscale));
    }
    let r = x.to_f64().powf(y.to_f64());
    Ok(round_sig(r, x.scale().max(y.scale())))
}

/// Rounds an f64 result to Postgres's 16-significant-digit display scale.
fn round_sig(r: f64, min_scale: u32) -> Numeric {
    if !r.is_finite() {
        return Numeric::from_f64(r);
    }
    let weight = if r == 0.0 { 0 } else { r.abs().log10().floor() as i64 };
    let rscale = (16 - weight).max(min_scale as i64).max(0);
    let n =
        Numeric::parse(&format!("{:.*}", (rscale as usize).min(300), r)).unwrap_or(Numeric::NaN);
    n.round(rscale)
}

pub fn like(s: &str, p: &str, ci: bool, esc: Option<char>) -> PgResult<bool> {
    let (s, p): (Vec<char>, Vec<char>) = if ci {
        (s.to_lowercase().chars().collect(), p.to_lowercase().chars().collect())
    } else {
        (s.chars().collect(), p.chars().collect())
    };
    // Compile into tokens.
    #[derive(Debug)]
    enum T {
        Lit(char),
        One,
        Any,
    }
    let mut toks = vec![];
    let mut i = 0;
    while i < p.len() {
        let c = p[i];
        if Some(c) == esc {
            if i + 1 >= p.len() {
                return Err(err(
                    code::INVALID_ESCAPE_SEQUENCE,
                    "LIKE pattern must not end with escape character",
                ));
            }
            toks.push(T::Lit(p[i + 1]));
            i += 2;
            continue;
        }
        toks.push(match c {
            '%' => T::Any,
            '_' => T::One,
            c => T::Lit(c),
        });
        i += 1;
    }
    // Iterative wildcard matching with backtracking on the last %.
    let (mut si, mut ti) = (0usize, 0usize);
    let mut star: Option<(usize, usize)> = None;
    while si < s.len() {
        if ti < toks.len() {
            match toks[ti] {
                T::Any => {
                    star = Some((ti, si));
                    ti += 1;
                    continue;
                }
                T::One => {
                    si += 1;
                    ti += 1;
                    continue;
                }
                T::Lit(c) if c == s[si] => {
                    si += 1;
                    ti += 1;
                    continue;
                }
                _ => {}
            }
        }
        match star {
            Some((st, ss)) => {
                ti = st + 1;
                si = ss + 1;
                star = Some((st, ss + 1));
            }
            None => return Ok(false),
        }
    }
    while ti < toks.len() && matches!(toks[ti], T::Any) {
        ti += 1;
    }
    Ok(ti == toks.len())
}

/// Translates a Postgres (ARE) regex to regex-lite syntax where they differ.
fn translate_regex(p: &str) -> String {
    let mut out = String::with_capacity(p.len());
    let mut chars = p.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('m') => out.push_str(r"\b"),
                Some('M') => out.push_str(r"\b"),
                Some('y') => out.push_str(r"\b"),
                Some('Y') => out.push_str(r"\B"),
                Some('A') => out.push('^'),
                Some('Z') => out.push('$'),
                Some(o) => {
                    out.push('\\');
                    out.push(o);
                }
                None => out.push_str("\\\\"),
            }
        } else if c == '[' && p.contains("[[:") {
            // POSIX classes like [[:alpha:]] are supported by regex-lite as-is.
            out.push(c);
        } else {
            out.push(c);
        }
    }
    out
}

pub fn regex(p: &str, flags: &str) -> PgResult<regex_lite::Regex> {
    let mut prefix = String::new();
    for f in flags.chars() {
        match f {
            'i' => prefix.push_str("(?i)"),
            'n' | 'm' => prefix.push_str("(?m)"),
            's' => prefix.push_str("(?s)"),
            'x' => prefix.push_str("(?x)"),
            'c' | 'g' => {}
            other => {
                return Err(err(
                    code::INVALID_PARAMETER_VALUE,
                    format!("invalid regular expression option: \"{other}\""),
                ));
            }
        }
    }
    regex_lite::Regex::new(&format!("{prefix}{}", translate_regex(p))).map_err(|e| {
        err(code::INVALID_REGULAR_EXPRESSION, format!("invalid regular expression: {e}"))
    })
}

/// Converts `\1` backreferences to regex-lite's `${1}` and escapes `$`.
fn regex_replacement(r: &str) -> String {
    let mut out = String::new();
    let mut chars = r.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\\' => match chars.next() {
                Some(d) if d.is_ascii_digit() => out.push_str(&format!("${{{d}}}")),
                Some('&') => out.push_str("${0}"),
                Some(o) => out.push(o),
                None => out.push('\\'),
            },
            '$' => out.push_str("$$"),
            c => out.push(c),
        }
    }
    out
}

fn jv(v: &Value) -> &Json {
    static NULL: Json = Json::Null;
    match v {
        Value::Jsonb(j) => j,
        _ => &NULL,
    }
}

fn json_of(v: &Value, jsonb: bool) -> PgResult<Json> {
    match v {
        Value::Jsonb(j) => Ok((**j).clone()),
        Value::Text(s) => {
            let j = json::parse(s).map_err(|e| types::invalid_input("json", s).detail(e.0))?;
            Ok(if jsonb { j.normalize() } else { j })
        }
        _ => Ok(Json::Null),
    }
}

fn json_get(a: &Value, key: &Value, jsonb: bool, as_text: bool) -> PgResult<Value> {
    if !jsonb {
        // json keeps the source text of the member.
        let Value::Text(src) = a else { return Ok(Value::Null) };
        let Some(children) = json::raw_children(src) else { return Ok(Value::Null) };
        let found = match key {
            Value::Text(k) => children
                .iter()
                .rev()
                .find(|(ck, _)| ck.as_deref() == Some(k.as_str()))
                .map(|(_, r)| *r),
            Value::Int(i) => {
                if children.first().is_some_and(|c| c.0.is_some()) {
                    None
                } else {
                    let idx = if *i < 0 { children.len() as i64 + i } else { *i };
                    (idx >= 0).then(|| children.get(idx as usize).map(|c| c.1)).flatten()
                }
            }
            _ => None,
        };
        return Ok(match found {
            None => Value::Null,
            Some(raw) if as_text => match json::parse(raw) {
                Ok(Json::Null) => Value::Null,
                Ok(Json::Str(s)) => Value::Text(s),
                _ => Value::Text(raw.to_string()),
            },
            Some(raw) => Value::Text(raw.to_string()),
        });
    }
    let j = jv(a);
    let sub = match key {
        Value::Text(k) => j.get(k),
        Value::Int(i) => j.index(*i),
        _ => None,
    };
    Ok(match sub {
        None => Value::Null,
        Some(x) if as_text => x.as_text(true).map_or(Value::Null, Value::Text),
        Some(x) => Value::Jsonb(Box::new(x.clone())),
    })
}

fn text_items(a: &Array) -> Vec<Option<String>> {
    a.items.iter().map(|v| v.as_str().map(str::to_string)).collect()
}

fn json_path_op(a: &Value, path: &Value, jsonb: bool, as_text: bool) -> PgResult<Value> {
    let Value::Array(p) = path else { return Ok(Value::Null) };
    let j = json_of(a, jsonb)?;
    let steps = text_items(p);
    Ok(match j.path(&steps) {
        None => Value::Null,
        Some(x) if as_text => x.as_text(jsonb).map_or(Value::Null, Value::Text),
        Some(x) if jsonb => Value::Jsonb(Box::new(x.clone())),
        Some(x) => Value::Text(x.to_compact_string()),
    })
}

pub fn jsonb_concat(a: &Json, b: &Json) -> Json {
    match (a, b) {
        (Json::Object(x), Json::Object(y)) => {
            let mut m = x.clone();
            m.extend(y.iter().cloned());
            Json::Object(m).normalize()
        }
        (Json::Array(x), Json::Array(y)) => Json::Array(x.iter().chain(y).cloned().collect()),
        (Json::Array(x), other) => {
            let mut v = x.clone();
            v.push(other.clone());
            Json::Array(v)
        }
        (other, Json::Array(y)) => {
            let mut v = vec![other.clone()];
            v.extend(y.iter().cloned());
            Json::Array(v)
        }
        (x, y) => Json::Array(vec![x.clone(), y.clone()]),
    }
}

fn jsonb_delete_key(j: &Json, k: &str) -> PgResult<Json> {
    Ok(match j {
        Json::Object(m) => Json::Object(m.iter().filter(|(key, _)| key != k).cloned().collect()),
        Json::Array(a) => Json::Array(
            a.iter().filter(|v| !matches!(v, Json::Str(s) if s == k)).cloned().collect(),
        ),
        _ => return Err(err(code::INVALID_PARAMETER_VALUE, "cannot delete from scalar")),
    })
}

fn jsonb_delete_index(j: &Json, i: i64) -> PgResult<Json> {
    match j {
        Json::Array(a) => {
            let idx = if i < 0 { a.len() as i64 + i } else { i };
            Ok(Json::Array(
                a.iter()
                    .enumerate()
                    .filter(|(n, _)| *n as i64 != idx)
                    .map(|(_, v)| v.clone())
                    .collect(),
            ))
        }
        Json::Object(_) => {
            Err(err(code::INVALID_PARAMETER_VALUE, "cannot delete from object using integer index"))
        }
        _ => Err(err(code::INVALID_PARAMETER_VALUE, "cannot delete from scalar")),
    }
}

fn jsonb_delete_path(j: &Json, path: &[Option<String>]) -> PgResult<Json> {
    if path.is_empty() {
        return Ok(j.clone());
    }
    let step = path[0].clone().unwrap_or_default();
    Ok(match j {
        Json::Object(m) => Json::Object(
            m.iter()
                .filter_map(|(k, v)| {
                    if *k == step {
                        if path.len() == 1 {
                            None
                        } else {
                            Some(jsonb_delete_path(v, &path[1..]).map(|nv| (k.clone(), nv)))
                        }
                    } else {
                        Some(Ok((k.clone(), v.clone())))
                    }
                })
                .collect::<PgResult<Vec<_>>>()?,
        ),
        Json::Array(a) => {
            let Ok(i) = step.parse::<i64>() else {
                return Err(err(
                    code::INVALID_TEXT_REPRESENTATION,
                    format!("path element at position 1 is not an integer: \"{step}\""),
                ));
            };
            let idx = if i < 0 { a.len() as i64 + i } else { i };
            let mut out = vec![];
            for (n, v) in a.iter().enumerate() {
                if n as i64 == idx {
                    if path.len() > 1 {
                        out.push(jsonb_delete_path(v, &path[1..])?);
                    }
                } else {
                    out.push(v.clone());
                }
            }
            Json::Array(out)
        }
        other => other.clone(),
    })
}

fn jsonb_set(
    j: &Json,
    path: &[Option<String>],
    new: &Json,
    create: bool,
    insert_after: Option<bool>,
) -> PgResult<Json> {
    if path.is_empty() {
        return Ok(j.clone());
    }
    let step = path[0]
        .clone()
        .ok_or_else(|| err(code::NULL_VALUE_NOT_ALLOWED, "path element at position 1 is null"))?;
    let last = path.len() == 1;
    Ok(match j {
        Json::Object(m) => {
            let mut m = m.clone();
            match m.iter().position(|(k, _)| *k == step) {
                Some(i) => {
                    if last {
                        if insert_after.is_some() {
                            return Err(err(
                                code::INVALID_PARAMETER_VALUE,
                                "cannot replace existing key",
                            )
                            .hint("Try using the function jsonb_set to replace key value."));
                        }
                        m[i].1 = new.clone();
                    } else {
                        m[i].1 = jsonb_set(&m[i].1, &path[1..], new, create, insert_after)?;
                    }
                }
                None => {
                    if last && (create || insert_after.is_some()) {
                        m.push((step, new.clone()));
                    }
                }
            }
            Json::Object(m).normalize()
        }
        Json::Array(a) => {
            let i: i64 = step.trim().parse().map_err(|_| {
                err(
                    code::INVALID_TEXT_REPRESENTATION,
                    format!("path element at position 1 is not an integer: \"{step}\""),
                )
            })?;
            let mut a = a.clone();
            let len = a.len() as i64;
            let idx = if i < 0 { len + i } else { i };
            if last {
                match insert_after {
                    Some(after) => {
                        let pos = if idx < 0 {
                            0
                        } else if idx >= len {
                            len
                        } else if after {
                            idx + 1
                        } else {
                            idx
                        };
                        a.insert(pos as usize, new.clone());
                    }
                    None => {
                        if idx >= 0 && idx < len {
                            a[idx as usize] = new.clone();
                        } else if create {
                            if idx < 0 {
                                a.insert(0, new.clone());
                            } else {
                                a.push(new.clone());
                            }
                        }
                    }
                }
            } else if idx >= 0 && idx < len {
                a[idx as usize] =
                    jsonb_set(&a[idx as usize], &path[1..], new, create, insert_after)?;
            }
            Json::Array(a)
        }
        _ => return Err(err(code::INVALID_PARAMETER_VALUE, "cannot set path in scalar")),
    })
}

fn strip_nulls(j: &Json) -> Json {
    match j {
        Json::Object(m) => Json::Object(
            m.iter()
                .filter(|(_, v)| !matches!(v, Json::Null))
                .map(|(k, v)| (k.clone(), strip_nulls(v)))
                .collect(),
        ),
        Json::Array(a) => Json::Array(a.iter().map(strip_nulls).collect()),
        o => o.clone(),
    }
}

/// SQL value → JSON value (to_json / to_jsonb semantics).
pub fn to_json_value(v: &Value, ty: Type, env: &Env) -> Json {
    if ty.array
        && let Value::Array(a) = v
    {
        return array_to_json(a, 0, &mut 0, ty.elem(), env);
    }
    match v {
        Value::Null => Json::Null,
        Value::Bool(b) => Json::Bool(*b),
        Value::Int(i) if !ty.is_reg() => Json::Num(Numeric::from_i64(*i)),
        Value::Float(f) if f.is_finite() => Json::Num(
            Numeric::parse(&types::format_float(*f, ty.base == Base::Float4, 1))
                .unwrap_or(Numeric::from_f64(*f)),
        ),
        Value::Float(f) => Json::Str(types::format_float(*f, false, 1)),
        Value::Num(n) if matches!(n, Numeric::Fin(_)) => Json::Num(n.clone()),
        Value::Jsonb(j) => (**j).clone(),
        Value::Text(s) if ty.base == Base::Json => json::parse(s).unwrap_or(Json::Str(s.clone())),
        Value::Ts(t) => Json::Str(json_timestamp(*t, ty.base == Base::Timestamptz, env)),
        Value::Date(d) => Json::Str(datetime::format_date(*d)),
        Value::Time(t) => Json::Str(datetime::format_time(*t)),
        Value::Record(fields) => Json::Array(
            fields.iter().map(|f| to_json_value(f, types::value_type_guess(f), env)).collect(),
        ),
        other => Json::Str(to_str(other, ty, env)),
    }
}

fn array_to_json(a: &Array, dim: usize, idx: &mut usize, elem: Type, env: &Env) -> Json {
    if a.dims.is_empty() {
        return Json::Array(vec![]);
    }
    let mut out = vec![];
    for _ in 0..a.dims[dim].0 {
        if dim + 1 < a.dims.len() {
            out.push(array_to_json(a, dim + 1, idx, elem, env));
        } else {
            out.push(to_json_value(&a.items[*idx], elem, env));
            *idx += 1;
        }
    }
    Json::Array(out)
}

/// ISO 8601 as JSON output uses: `2020-01-01T10:00:00+00:00`.
pub fn json_timestamp(t: i64, tz: bool, env: &Env) -> String {
    if t == datetime::TS_INF {
        return "infinity".into();
    }
    if t == datetime::TS_NEG_INF {
        return "-infinity".into();
    }
    if tz {
        let off = env
            .fmt
            .zone
            .offset_at_utc(t.div_euclid(USECS_PER_SEC) - datetime::PG_EPOCH_DAYS * 86400);
        let local =
            datetime::format_timestamp(t + off as i64 * USECS_PER_SEC).replacen(' ', "T", 1);
        let sign = if off < 0 { '-' } else { '+' };
        let a = off.unsigned_abs();
        let mut s = format!("{local}{sign}{:02}:{:02}", a / 3600, a / 60 % 60);
        if !a.is_multiple_of(60) {
            s.push_str(&format!(":{:02}", a % 60));
        }
        s
    } else {
        datetime::format_timestamp(t).replacen(' ', "T", 1)
    }
}

/// `json` text for a value: nested json stays compact the way Postgres builds it.
pub fn to_json_text(v: &Value, ty: Type, env: &Env) -> String {
    match v {
        Value::Text(s) if ty.base == Base::Json && !ty.array => s.clone(),
        Value::Jsonb(j) => j.to_jsonb_string(),
        Value::Array(a) if ty.array => {
            let mut s = String::from("[");
            json_array_text(a, 0, &mut 0, ty.elem(), env, &mut s);
            s.push(']');
            s
        }
        Value::Null => "null".into(),
        other => to_json_value(other, ty, env).to_compact_string(),
    }
}

fn json_array_text(
    a: &Array,
    dim: usize,
    idx: &mut usize,
    elem: Type,
    env: &Env,
    out: &mut String,
) {
    if a.dims.is_empty() {
        return;
    }
    for i in 0..a.dims[dim].0 {
        if i > 0 {
            out.push(',');
        }
        if dim + 1 < a.dims.len() {
            out.push('[');
            json_array_text(a, dim + 1, idx, elem, env, out);
            out.push(']');
        } else {
            out.push_str(&to_json_text(&a.items[*idx], elem, env));
            *idx += 1;
        }
    }
}

fn array_concat_op(a: &Value, b: &Value, tys: &[Type]) -> PgResult<Value> {
    let elem_arr = |v: &Value| match v {
        Value::Array(x) => (**x).clone(),
        Value::Null => Array::empty(),
        other => Array::new(vec![other.clone()]),
    };
    let (ta, tb) = (tys[0], tys[1]);
    let r = match (ta.array, tb.array) {
        (true, true) => array_cat(&elem_arr(a), &elem_arr(b))?,
        (true, false) => {
            let mut x = elem_arr(a);
            push_elem(&mut x, b.clone(), false)?;
            x
        }
        (false, true) => {
            let mut x = elem_arr(b);
            push_elem(&mut x, a.clone(), true)?;
            x
        }
        _ => Array::new(vec![a.clone(), b.clone()]),
    };
    Ok(Value::Array(Box::new(r)))
}

fn push_elem(a: &mut Array, v: Value, front: bool) -> PgResult<()> {
    if a.dims.len() > 1 {
        return Err(err(
            code::DATATYPE_MISMATCH,
            "argument must be empty or one-dimensional array",
        ));
    }
    if a.dims.is_empty() {
        *a = Array::new(vec![v]);
        return Ok(());
    }
    if front {
        a.items.insert(0, v);
        a.dims[0].1 -= 1;
    } else {
        a.items.push(v);
    }
    a.dims[0].0 += 1;
    Ok(())
}

pub fn array_cat(x: &Array, y: &Array) -> PgResult<Array> {
    if x.dims.is_empty() {
        return Ok(y.clone());
    }
    if y.dims.is_empty() {
        return Ok(x.clone());
    }
    if x.dims.len() == y.dims.len() {
        if x.dims[1..].iter().zip(&y.dims[1..]).any(|(a, b)| a.0 != b.0) {
            return Err(err(
                code::ARRAY_SUBSCRIPT_ERROR,
                "cannot concatenate incompatible arrays",
            )
            .detail("Arrays with differing element dimensions are not compatible for concatenation."));
        }
        let mut dims = x.dims.clone();
        dims[0].0 += y.dims[0].0;
        let mut items = x.items.clone();
        items.extend(y.items.iter().cloned());
        return Ok(Array { dims, items });
    }
    if x.dims.len() + 1 == y.dims.len() {
        let mut dims = y.dims.clone();
        dims[0].0 += 1;
        let mut items = x.items.clone();
        items.extend(y.items.iter().cloned());
        return Ok(Array { dims, items });
    }
    if x.dims.len() == y.dims.len() + 1 {
        let mut dims = x.dims.clone();
        dims[0].0 += 1;
        let mut items = x.items.clone();
        items.extend(y.items.iter().cloned());
        return Ok(Array { dims, items });
    }
    Err(err(code::ARRAY_SUBSCRIPT_ERROR, "cannot concatenate incompatible arrays"))
}

fn trim_chars(s: &str, chars: &str, left: bool, right: bool) -> String {
    let set: Vec<char> = chars.chars().collect();
    let mut r = s;
    if left {
        r = r.trim_start_matches(|c| set.contains(&c));
    }
    if right {
        r = r.trim_end_matches(|c| set.contains(&c));
    }
    r.to_string()
}

fn pad(s: &str, len: i64, fill: &str, left: bool) -> String {
    let len = len.max(0) as usize;
    let chars: Vec<char> = s.chars().collect();
    if chars.len() >= len {
        return chars[..len].iter().collect();
    }
    if fill.is_empty() {
        return s.to_string();
    }
    let need = len - chars.len();
    let padding: String = fill.chars().cycle().take(need).collect();
    if left { padding + s } else { s.to_string() + &padding }
}

fn substr(s: &str, start: i64, len: Option<i64>) -> PgResult<String> {
    if let Some(l) = len
        && l < 0
    {
        return Err(err(code::SUBSTRING_ERROR, "negative substring length not allowed"));
    }
    let chars: Vec<char> = s.chars().collect();
    let begin = start - 1;
    let end = match len {
        Some(l) => begin.saturating_add(l),
        None => i64::MAX,
    };
    let b = begin.max(0) as usize;
    let e = end.min(chars.len() as i64).max(0) as usize;
    if b >= e {
        return Ok(String::new());
    }
    Ok(chars[b..e].iter().collect())
}

fn initcap(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut prev_alnum = false;
    for c in s.chars() {
        if prev_alnum {
            out.extend(c.to_lowercase());
        } else {
            out.extend(c.to_uppercase());
        }
        prev_alnum = c.is_alphanumeric();
    }
    out
}

pub fn quote_ident(s: &str) -> String {
    let safe = !s.is_empty()
        && s.chars().next().is_some_and(|c| c.is_ascii_lowercase() || c == '_')
        && s.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        && !super::keywords::is_reserved(s);
    if safe { s.to_string() } else { format!("\"{}\"", s.replace('"', "\"\"")) }
}

pub fn quote_literal(s: &str) -> String {
    if s.contains('\\') {
        format!("E'{}'", s.replace('\\', "\\\\").replace('\'', "''"))
    } else {
        format!("'{}'", s.replace('\'', "''"))
    }
}

pub fn md5_hex(data: &[u8]) -> String {
    super::auth::md5(data).iter().map(|b| format!("{b:02x}")).collect()
}

fn format_fn(fmt: &str, args: &[Value], tys: &[Type], env: &Env) -> PgResult<String> {
    let mut out = String::new();
    let chars: Vec<char> = fmt.chars().collect();
    let mut i = 0;
    let mut argi = 0usize;
    while i < chars.len() {
        let c = chars[i];
        if c != '%' {
            out.push(c);
            i += 1;
            continue;
        }
        i += 1;
        if i >= chars.len() {
            return Err(err(code::INVALID_PARAMETER_VALUE, "unterminated format() type specifier")
                .hint("For a single \"%\" use \"%%\"."));
        }
        if chars[i] == '%' {
            out.push('%');
            i += 1;
            continue;
        }
        // Optional position n$, flags '-', width.
        let mut pos: Option<usize> = None;
        let start = i;
        while i < chars.len() && chars[i].is_ascii_digit() {
            i += 1;
        }
        if i < chars.len() && chars[i] == '$' && i > start {
            pos = Some(chars[start..i].iter().collect::<String>().parse::<usize>().unwrap_or(1));
            i += 1;
        } else {
            i = start;
        }
        let mut left = false;
        if i < chars.len() && chars[i] == '-' {
            left = true;
            i += 1;
        }
        let ws = i;
        while i < chars.len() && chars[i].is_ascii_digit() {
            i += 1;
        }
        let width: usize = chars[ws..i].iter().collect::<String>().parse().unwrap_or(0);
        if i >= chars.len() {
            return Err(err(code::INVALID_PARAMETER_VALUE, "unterminated format() type specifier"));
        }
        let kind = chars[i];
        i += 1;
        let idx = match pos {
            Some(p) => {
                argi = p;
                p - 1
            }
            None => {
                argi += 1;
                argi - 1
            }
        };
        let Some(v) = args.get(idx) else {
            return Err(err(code::INVALID_PARAMETER_VALUE, "too few arguments for format()"));
        };
        let ty = tys.get(idx).copied().unwrap_or(Type::TEXT);
        let s = match kind {
            's' => {
                if v.is_null() {
                    String::new()
                } else {
                    to_str(v, ty, env)
                }
            }
            'I' => {
                if v.is_null() {
                    return Err(err(
                        code::NULL_VALUE_NOT_ALLOWED,
                        "null values cannot be formatted as an SQL identifier",
                    ));
                }
                quote_ident(&to_str(v, ty, env))
            }
            'L' => {
                if v.is_null() {
                    "NULL".into()
                } else {
                    quote_literal(&to_str(v, ty, env))
                }
            }
            other => {
                return Err(err(
                    code::INVALID_PARAMETER_VALUE,
                    format!("unrecognized format() type specifier \"{other}\""),
                )
                .hint("For a single \"%\" use \"%%\"."));
            }
        };
        let n = s.chars().count();
        if n < width {
            let padding = " ".repeat(width - n);
            if left {
                out.push_str(&s);
                out.push_str(&padding);
            } else {
                out.push_str(&padding);
                out.push_str(&s);
            }
        } else {
            out.push_str(&s);
        }
    }
    Ok(out)
}

fn arr_items(v: &Value) -> &[Value] {
    match v {
        Value::Array(a) => &a.items,
        _ => &[],
    }
}

fn arr(v: &Value) -> Option<&Array> {
    match v {
        Value::Array(a) => Some(a),
        _ => None,
    }
}

fn text_array(items: Vec<Option<String>>) -> Value {
    Value::Array(Box::new(Array::new(
        items.into_iter().map(|s| s.map_or(Value::Null, Value::Text)).collect(),
    )))
}

fn field_name(v: &Value) -> String {
    text(v).to_ascii_lowercase()
}

/// Pure scalar functions. `None` means "not handled here".
pub fn call(
    name: &str,
    a: &[Value],
    tys: &[Type],
    ret: Type,
    env: &Env,
) -> PgResult<Option<Value>> {
    use Value::*;
    let v = match name {
        // --- math
        "abs" => match &a[0] {
            Int(i) => return int_result(i.checked_abs(), ret).map(Some),
            Float(f) => Float(f.abs()),
            Num(n) => Num(n.abs()),
            _ => Null,
        },
        "ceil" | "ceiling" => match &a[0] {
            Num(n) => Num(n.ceil()),
            v => Float(as_f64(v).ceil()),
        },
        "floor" => match &a[0] {
            Num(n) => Num(n.floor()),
            v => Float(as_f64(v).floor()),
        },
        "round" => match (&a[0], a.get(1)) {
            (Num(n), None) => Num(n.round(0)),
            (Num(n), Some(s)) => Num(n.round(int(s))),
            (v, _) => Float(as_f64(v).round_ties_even()),
        },
        "trunc" => match (&a[0], a.get(1)) {
            (Num(n), None) => Num(n.trunc(0)),
            (Num(n), Some(s)) => Num(n.trunc(int(s))),
            (v, _) => Float(as_f64(v).trunc()),
        },
        "sign" => match &a[0] {
            Num(n) => Num(n.sign()),
            v => {
                let x = as_f64(v);
                Float(if x > 0.0 {
                    1.0
                } else if x < 0.0 {
                    -1.0
                } else {
                    0.0
                })
            }
        },
        "sqrt" => match &a[0] {
            Num(n) => Num(n.sqrt().ok_or_else(|| {
                err(
                    code::INVALID_ARGUMENT_FOR_POWER,
                    "cannot take square root of a negative number",
                )
            })?),
            v => {
                let x = as_f64(v);
                if x < 0.0 {
                    return Err(err(
                        code::INVALID_ARGUMENT_FOR_POWER,
                        "cannot take square root of a negative number",
                    ));
                }
                Float(x.sqrt())
            }
        },
        "cbrt" => Float(as_f64(&a[0]).cbrt()),
        "exp" => match &a[0] {
            Num(n) => Num(round_sig(n.to_f64().exp(), n.scale())),
            v => return float_check(as_f64(v).exp(), true).map(Some),
        },
        "ln" | "log" | "log10" => {
            let two = a.len() == 2;
            let x = as_f64(if two { &a[1] } else { &a[0] });
            if x == 0.0 {
                return Err(err(code::INVALID_ARGUMENT_FOR_LOG, "cannot take logarithm of zero"));
            }
            if x < 0.0 {
                return Err(err(
                    code::INVALID_ARGUMENT_FOR_LOG,
                    "cannot take logarithm of a negative number",
                ));
            }
            let r = if two {
                x.ln() / as_f64(&a[0]).ln()
            } else if name == "ln" {
                x.ln()
            } else {
                x.log10()
            };
            if ret.base == Base::Numeric {
                let sc = a.iter().map(|v| as_num(v).scale()).max().unwrap_or(0);
                Num(round_sig(r, sc))
            } else {
                Float(r)
            }
        }
        "power" | "pow" => return binop("^", &a[0], &a[1], ret, tys, env).map(Some),
        "mod" => return binop("%", &a[0], &a[1], ret, tys, env).map(Some),
        "div" => Num(as_num(&a[0]).div_trunc(&as_num(&a[1])).map_err(num_err)?),
        "gcd" | "lcm" => {
            let (mut x, mut y) = (int(&a[0]).unsigned_abs(), int(&a[1]).unsigned_abs());
            let (ox, oy) = (x, y);
            while y != 0 {
                let t = x % y;
                x = y;
                y = t;
            }
            if name == "gcd" {
                Int(x as i64)
            } else if ox == 0 || oy == 0 {
                Int(0)
            } else {
                return int_result(i64::try_from((ox / x) as u128 * oy as u128).ok(), ret)
                    .map(Some);
            }
        }
        "pi" => Float(std::f64::consts::PI),
        "random" => Float(random_f64()),
        "setseed" => Null,
        "degrees" => Float(as_f64(&a[0]).to_degrees()),
        "radians" => Float(as_f64(&a[0]).to_radians()),
        "sin" => Float(as_f64(&a[0]).sin()),
        "cos" => Float(as_f64(&a[0]).cos()),
        "tan" => Float(as_f64(&a[0]).tan()),
        "asin" => Float(as_f64(&a[0]).asin()),
        "acos" => Float(as_f64(&a[0]).acos()),
        "atan" => Float(as_f64(&a[0]).atan()),
        "atan2" => Float(as_f64(&a[0]).atan2(as_f64(&a[1]))),
        "cot" => Float(1.0 / as_f64(&a[0]).tan()),
        "scale" => Int(as_num(&a[0]).scale() as i64),
        "min_scale" => {
            let s = as_num(&a[0]).to_string();
            Int(s.split_once('.').map_or(0, |(_, f)| f.trim_end_matches('0').len()) as i64)
        }
        "trim_scale" => {
            let n = as_num(&a[0]);
            let s = n.to_string();
            let t = if s.contains('.') {
                s.trim_end_matches('0').trim_end_matches('.').to_string()
            } else {
                s
            };
            Num(Numeric::parse(&t).unwrap_or(n))
        }
        "factorial" => {
            let n = int(&a[0]);
            if n < 0 {
                return Err(err(
                    code::INVALID_PARAMETER_VALUE,
                    "factorial of a negative number is undefined",
                ));
            }
            let mut r = Numeric::from_i64(1);
            for i in 2..=n {
                r = r.mul(&Numeric::from_i64(i));
            }
            Num(r)
        }
        "width_bucket" => {
            let (x, lo, hi, n) = (as_f64(&a[0]), as_f64(&a[1]), as_f64(&a[2]), int(&a[3]));
            if n <= 0 {
                return Err(err(
                    code::INVALID_ARGUMENT_FOR_WIDTH_BUCKET,
                    "count must be greater than zero",
                ));
            }
            Int(if lo < hi {
                if x < lo {
                    0
                } else if x >= hi {
                    n + 1
                } else {
                    ((x - lo) / (hi - lo) * n as f64) as i64 + 1
                }
            } else if x > lo {
                0
            } else if x <= hi {
                n + 1
            } else {
                ((lo - x) / (lo - hi) * n as f64) as i64 + 1
            })
        }
        // --- strings
        "lower" => Text(text(&a[0]).to_lowercase()),
        "upper" => Text(text(&a[0]).to_uppercase()),
        "initcap" => Text(initcap(text(&a[0]))),
        "length" | "char_length" | "character_length" => match &a[0] {
            Bytes(b) => Int(b.len() as i64),
            v => {
                let s = to_str(v, tys[0], env);
                Int(s.chars().count() as i64)
            }
        },
        "octet_length" => match &a[0] {
            Bytes(b) => Int(b.len() as i64),
            v => Int(text(v).len() as i64),
        },
        "bit_length" => Int(text(&a[0]).len() as i64 * 8),
        "substr" | "substring" => match (&a[0], tys.get(1)) {
            (Bytes(b), _) => {
                let start = int(&a[1]);
                let len = a.get(2).map(int);
                let begin = (start - 1).max(0) as usize;
                let end = match len {
                    Some(l) => {
                        if l < 0 {
                            return Err(err(
                                code::SUBSTRING_ERROR,
                                "negative substring length not allowed",
                            ));
                        }
                        (start - 1 + l).clamp(0, b.len() as i64) as usize
                    }
                    None => b.len(),
                };
                Bytes(if begin < end { b[begin..end].to_vec() } else { vec![] })
            }
            (Text(s), Some(t)) if t.is_string() || t.is_unknown() => {
                // substring(text from pattern [for escape])
                if a.len() == 3 {
                    similar_substring(s, text(&a[1]), text(&a[2]))?
                } else {
                    let re = regex(text(&a[1]), "")?;
                    match re.captures(s) {
                        None => Null,
                        Some(c) => c
                            .get(1)
                            .or_else(|| c.get(0))
                            .map_or(Null, |m| Text(m.as_str().to_string())),
                    }
                }
            }
            (v, _) => Text(substr(&to_str(v, tys[0], env), int(&a[1]), a.get(2).map(int))?),
        },
        "strpos" | "position" => {
            let (s, sub) = (text(&a[0]), text(&a[1]));
            Int(match s.find(sub) {
                Some(b) => s[..b].chars().count() as i64 + 1,
                None => 0,
            })
        }
        "replace" => {
            let from = text(&a[1]);
            Text(if from.is_empty() {
                text(&a[0]).to_string()
            } else {
                text(&a[0]).replace(from, text(&a[2]))
            })
        }
        "translate" => {
            let from: Vec<char> = text(&a[1]).chars().collect();
            let to: Vec<char> = text(&a[2]).chars().collect();
            Text(
                text(&a[0])
                    .chars()
                    .filter_map(|c| match from.iter().position(|&f| f == c) {
                        Some(i) => to.get(i).copied(),
                        None => Some(c),
                    })
                    .collect(),
            )
        }
        "btrim" | "ltrim" | "rtrim" => {
            let chars = a.get(1).map_or(" ", text);
            let s = to_str(&a[0], tys[0], env);
            Text(trim_chars(&s, chars, name != "rtrim", name != "ltrim"))
        }
        "lpad" | "rpad" => {
            Text(pad(text(&a[0]), int(&a[1]), a.get(2).map_or(" ", text), name == "lpad"))
        }
        "left" | "right" => {
            let chars: Vec<char> = text(&a[0]).chars().collect();
            let n = int(&a[1]);
            let len = chars.len() as i64;
            let k = if n < 0 { (len + n).max(0) } else { n.min(len) } as usize;
            Text(if name == "left" {
                chars[..k].iter().collect()
            } else {
                chars[chars.len() - k..].iter().collect()
            })
        }
        "repeat" => {
            let n = int(&a[1]).max(0) as usize;
            if text(&a[0]).len().saturating_mul(n) > 1 << 30 {
                return Err(err(code::PROGRAM_LIMIT_EXCEEDED, "requested length too large"));
            }
            Text(text(&a[0]).repeat(n))
        }
        "reverse" => Text(text(&a[0]).chars().rev().collect()),
        "concat" => Text(
            a.iter()
                .zip(tys)
                .filter(|(v, _)| !v.is_null())
                .map(|(v, t)| to_str(v, *t, env))
                .collect(),
        ),
        "concat_ws" => {
            if a[0].is_null() {
                Null
            } else {
                let sep = text(&a[0]).to_string();
                let parts: Vec<String> = a[1..]
                    .iter()
                    .zip(&tys[1..])
                    .filter(|(v, _)| !v.is_null())
                    .map(|(v, t)| to_str(v, *t, env))
                    .collect();
                Text(parts.join(&sep))
            }
        }
        "split_part" => {
            let (s, d, n) = (text(&a[0]), text(&a[1]), int(&a[2]));
            if n == 0 {
                return Err(err(code::INVALID_PARAMETER_VALUE, "field position must not be zero"));
            }
            let parts: Vec<&str> = if d.is_empty() { vec![s] } else { s.split(d).collect() };
            let idx = if n > 0 { n - 1 } else { parts.len() as i64 + n };
            Text(if idx >= 0 {
                parts.get(idx as usize).copied().unwrap_or("").to_string()
            } else {
                String::new()
            })
        }
        "md5" => Text(match &a[0] {
            Bytes(b) => md5_hex(b),
            v => md5_hex(text(v).as_bytes()),
        }),
        "sha256" => match &a[0] {
            Bytes(b) => Bytes(super::auth::sha256(b).to_vec()),
            _ => Null,
        },
        "ascii" => Int(text(&a[0]).chars().next().map_or(0, |c| c as i64)),
        "chr" => {
            let n = int(&a[0]);
            if n == 0 {
                return Err(err(code::PROGRAM_LIMIT_EXCEEDED, "null character not permitted"));
            }
            Text(
                char::from_u32(n as u32)
                    .ok_or_else(|| {
                        err(
                            code::PROGRAM_LIMIT_EXCEEDED,
                            "requested character too large for encoding",
                        )
                    })?
                    .to_string(),
            )
        }
        "format" => {
            if a[0].is_null() {
                Null
            } else {
                Text(format_fn(text(&a[0]), &a[1..], &tys[1..], env)?)
            }
        }
        "quote_ident" => Text(quote_ident(text(&a[0]))),
        "quote_literal" => Text(quote_literal(&to_str(&a[0], tys[0], env))),
        "quote_nullable" => {
            if a[0].is_null() {
                Text("NULL".into())
            } else {
                Text(quote_literal(&to_str(&a[0], tys[0], env)))
            }
        }
        "regexp_replace" => {
            let flags = a.get(3).map_or("", text);
            let re = regex(text(&a[1]), flags)?;
            let rep = regex_replacement(text(&a[2]));
            Text(if flags.contains('g') {
                re.replace_all(text(&a[0]), rep.as_str()).into_owned()
            } else {
                re.replace(text(&a[0]), rep.as_str()).into_owned()
            })
        }
        "regexp_match" => {
            let re = regex(text(&a[1]), a.get(2).map_or("", text))?;
            match re.captures(text(&a[0])) {
                None => Null,
                Some(c) => {
                    if c.len() == 1 {
                        text_array(vec![c.get(0).map(|m| m.as_str().to_string())])
                    } else {
                        text_array(
                            (1..c.len())
                                .map(|i| c.get(i).map(|m| m.as_str().to_string()))
                                .collect(),
                        )
                    }
                }
            }
        }
        "regexp_like" => Bool(regex(text(&a[1]), a.get(2).map_or("", text))?.is_match(text(&a[0]))),
        "regexp_count" => Int(regex(text(&a[1]), "")?.find_iter(text(&a[0])).count() as i64),
        "regexp_split_to_array" => {
            let re = regex(text(&a[1]), a.get(2).map_or("", text))?;
            text_array(regex_split(&re, text(&a[0])).into_iter().map(Some).collect())
        }
        "string_to_array" => {
            if a[0].is_null() {
                Null
            } else {
                let s = text(&a[0]);
                let null_str = a.get(2).and_then(|v| v.as_str());
                let parts: Vec<String> = match &a[1] {
                    Null => s.chars().map(|c| c.to_string()).collect(),
                    Text(d) if d.is_empty() => {
                        if s.is_empty() {
                            vec![]
                        } else {
                            vec![s.to_string()]
                        }
                    }
                    Text(d) => {
                        if s.is_empty() {
                            vec![]
                        } else {
                            s.split(d.as_str()).map(str::to_string).collect()
                        }
                    }
                    _ => vec![],
                };
                text_array(
                    parts
                        .into_iter()
                        .map(|p| if Some(p.as_str()) == null_str { None } else { Some(p) })
                        .collect(),
                )
            }
        }
        "array_to_string" => {
            if a[0].is_null() || a[1].is_null() {
                Null
            } else {
                let sep = text(&a[1]);
                let null_rep = a.get(2).and_then(|v| v.as_str());
                let parts: Vec<String> = arr_items(&a[0])
                    .iter()
                    .filter_map(|x| {
                        if x.is_null() {
                            null_rep.map(str::to_string)
                        } else {
                            Some(to_str(x, tys[0].elem(), env))
                        }
                    })
                    .collect();
                Text(parts.join(sep))
            }
        }
        "starts_with" => Bool(text(&a[0]).starts_with(text(&a[1]))),
        "to_hex" => Text(match tys[0].base {
            Base::Int4 => format!("{:x}", int(&a[0]) as i32 as u32),
            _ => format!("{:x}", int(&a[0]) as u64),
        }),
        "encode" => {
            let Bytes(b) = &a[0] else { return Ok(Some(Null)) };
            Text(match text(&a[1]).to_ascii_lowercase().as_str() {
                "hex" => b.iter().map(|x| format!("{x:02x}")).collect(),
                "base64" => base64_encode(b),
                "escape" => types::format_bytea(b, true),
                other => {
                    return Err(err(
                        code::INVALID_PARAMETER_VALUE,
                        format!("unrecognized encoding: \"{other}\""),
                    ));
                }
            })
        }
        "decode" => {
            let s = text(&a[0]);
            Bytes(match text(&a[1]).to_ascii_lowercase().as_str() {
                "hex" => types::parse_bytea(&format!("\\x{s}"))?,
                "base64" => base64_decode(s).ok_or_else(|| {
                    err(
                        code::INVALID_PARAMETER_VALUE,
                        "invalid symbol found while decoding base64 sequence",
                    )
                })?,
                "escape" => types::parse_bytea(s)?,
                other => {
                    return Err(err(
                        code::INVALID_PARAMETER_VALUE,
                        format!("unrecognized encoding: \"{other}\""),
                    ));
                }
            })
        }
        "convert_to" => Bytes(text(&a[0]).as_bytes().to_vec()),
        "convert_from" => match &a[0] {
            Bytes(b) => Text(String::from_utf8(b.clone()).map_err(|_| {
                err(
                    code::CHARACTER_NOT_IN_REPERTOIRE,
                    "invalid byte sequence for encoding \"UTF8\"",
                )
            })?),
            _ => Null,
        },
        "to_ascii" | "unistr" => Text(text(&a[0]).to_string()),
        "gen_random_uuid" | "uuid_generate_v4" => Uuid(random_uuid()),
        // --- date/time
        "now" | "transaction_timestamp" => Ts(env.now),
        "statement_timestamp" => Ts(env.stmt_now),
        "clock_timestamp" => Ts(datetime::now_micros()),
        "timeofday" => Text(datetime::format_timestamptz(datetime::now_micros(), &env.fmt.zone)),
        "date_trunc" => {
            let field = field_name(&a[0]);
            match (&a[1], tys[1].base) {
                (Interval(iv), _) => Interval(
                    datetime::trunc_interval(&field, iv)
                        .ok_or_else(|| unit_err(&field, "interval"))?,
                ),
                (Ts(t), Base::Timestamptz) => {
                    let zone = match a.get(2) {
                        Some(Text(z)) => super::tz::lookup(z).ok_or_else(|| {
                            err(
                                code::INVALID_PARAMETER_VALUE,
                                format!("time zone \"{z}\" not recognized"),
                            )
                        })?,
                        _ => env.fmt.zone.clone(),
                    };
                    if *t == datetime::TS_INF || *t == datetime::TS_NEG_INF {
                        Ts(*t)
                    } else {
                        let local = datetime::utc_to_local(*t, &zone);
                        let tl = datetime::trunc_local(&field, local)
                            .ok_or_else(|| unit_err(&field, "timestamp with time zone"))?;
                        Ts(
                            if matches!(
                                field.as_str(),
                                "microseconds" | "milliseconds" | "second" | "minute" | "hour"
                            ) && field != "hour"
                            {
                                *t - (local - tl)
                            } else {
                                datetime::local_to_utc(tl, &zone)
                            },
                        )
                    }
                }
                (Ts(t), _) => {
                    if *t == datetime::TS_INF || *t == datetime::TS_NEG_INF {
                        Ts(*t)
                    } else {
                        Ts(datetime::trunc_local(&field, *t)
                            .ok_or_else(|| unit_err(&field, "timestamp without time zone"))?)
                    }
                }
                _ => Null,
            }
        }
        "date_part" | "extract" => {
            let field = normalize_field(&field_name(&a[0]));
            let n = extract_value(&field, &a[1], tys[1], env)?;
            if name == "date_part" { Float(n.to_f64()) } else { Num(n) }
        }
        "age" => {
            let (x, y) = if a.len() == 1 {
                // age(ts) = age(current_date midnight, ts)
                let today = datetime::utc_to_local(env.now, &env.fmt.zone)
                    .div_euclid(USECS_PER_DAY)
                    * USECS_PER_DAY;
                let t = match (&a[0], tys[0].base) {
                    (Ts(t), Base::Timestamptz) => datetime::utc_to_local(*t, &env.fmt.zone),
                    (Ts(t), _) => *t,
                    _ => 0,
                };
                (today, t)
            } else {
                let conv = |v: &Value, t: Type| match (v, t.base) {
                    (Ts(x), Base::Timestamptz) => datetime::utc_to_local(*x, &env.fmt.zone),
                    (Ts(x), _) => *x,
                    _ => 0,
                };
                (conv(&a[0], tys[0]), conv(&a[1], tys[1]))
            };
            Interval(datetime::age(x, y))
        }
        "to_char" => to_char(&a[0], tys[0], text(&a[1]), env)?,
        "to_date" => Date(to_date_fmt(text(&a[0]), text(&a[1]))?),
        "to_timestamp" => match &a[0] {
            Float(f) => Ts(datetime::from_unix_seconds(*f)
                .map_err(|_| err(code::DATETIME_FIELD_OVERFLOW, "timestamp out of range"))?),
            Int(i) => Ts(datetime::from_unix_seconds(*i as f64)
                .map_err(|_| err(code::DATETIME_FIELD_OVERFLOW, "timestamp out of range"))?),
            Num(n) => Ts(datetime::from_unix_seconds(n.to_f64())
                .map_err(|_| err(code::DATETIME_FIELD_OVERFLOW, "timestamp out of range"))?),
            Text(s) => {
                let local = to_timestamp_fmt(s, text(&a[1]))?;
                Ts(datetime::local_to_utc(local, &env.fmt.zone))
            }
            _ => Null,
        },
        "to_number" => {
            let s: String = text(&a[0])
                .chars()
                .filter(|c| c.is_ascii_digit() || *c == '.' || *c == '-')
                .collect();
            Num(Numeric::parse(&s).map_err(|_| types::invalid_input("numeric", text(&a[0])))?)
        }
        "make_date" => {
            let (y, m, d) = (int(&a[0]), int(&a[1]), int(&a[2]));
            if !(1..=12).contains(&m)
                || d < 1
                || d > datetime::days_in_month(y, m as u32) as i64
                || y == 0
            {
                return Err(err(
                    code::DATETIME_FIELD_OVERFLOW,
                    format!("date field value out of range: {y}-{m:02}-{d:02}"),
                ));
            }
            let y = if y < 0 { y + 1 } else { y };
            Date(datetime::date_from_ymd(y, m as u32, d as u32))
        }
        "make_time" => {
            let (h, m, s) = (int(&a[0]), int(&a[1]), as_f64(&a[2]));
            if !(0..=24).contains(&h) || !(0..60).contains(&m) || !(0.0..=60.0).contains(&s) {
                return Err(err(
                    code::DATETIME_FIELD_OVERFLOW,
                    format!("time field value out of range: {h}:{m:02}:{s}"),
                ));
            }
            Time((h * 3600 + m * 60) * USECS_PER_SEC + (s * 1e6).round() as i64)
        }
        "make_timestamp" | "make_timestamptz" => {
            let (y, mo, d, h, mi, s) =
                (int(&a[0]), int(&a[1]), int(&a[2]), int(&a[3]), int(&a[4]), as_f64(&a[5]));
            if !(1..=12).contains(&mo) || d < 1 || d > datetime::days_in_month(y, mo as u32) as i64
            {
                return Err(err(
                    code::DATETIME_FIELD_OVERFLOW,
                    format!("date field value out of range: {y}-{mo:02}-{d:02}"),
                ));
            }
            let local = datetime::date_from_ymd(y, mo as u32, d as u32) as i64 * USECS_PER_DAY
                + (h * 3600 + mi * 60) * USECS_PER_SEC
                + (s * 1e6).round() as i64;
            if name == "make_timestamp" {
                Ts(local)
            } else {
                let zone = match a.get(6) {
                    Some(Text(z)) => super::tz::lookup(z).ok_or_else(|| {
                        err(
                            code::INVALID_PARAMETER_VALUE,
                            format!("time zone \"{z}\" not recognized"),
                        )
                    })?,
                    _ => env.fmt.zone.clone(),
                };
                Ts(datetime::local_to_utc(local, &zone))
            }
        }
        "make_interval" => {
            let g = |i: usize| a.get(i).map_or(0, int);
            let secs = a.get(6).map_or(0.0, as_f64);
            Interval(datetime::Interval {
                months: (g(0) * 12 + g(1)) as i32,
                days: (g(2) * 7 + g(3)) as i32,
                micros: (g(4) * 3600 + g(5) * 60) * USECS_PER_SEC + (secs * 1e6).round() as i64,
            })
        }
        "justify_days" | "justify_hours" | "justify_interval" => match &a[0] {
            Interval(iv) => Interval(match name {
                "justify_days" => iv.justify_days(),
                "justify_hours" => iv.justify_hours(),
                _ => iv.justify(),
            }),
            _ => Null,
        },
        "isfinite" => Bool(match &a[0] {
            Date(d) => !matches!(*d, datetime::DATE_INF | datetime::DATE_NEG_INF),
            Ts(t) => !matches!(*t, datetime::TS_INF | datetime::TS_NEG_INF),
            _ => true,
        }),
        "timezone" => {
            let zone = match &a[0] {
                Text(z) => super::tz::lookup(z).ok_or_else(|| {
                    err(code::INVALID_PARAMETER_VALUE, format!("time zone \"{z}\" not recognized"))
                })?,
                Interval(iv) => super::tz::Zone::Fixed((iv.micros / USECS_PER_SEC) as i32),
                _ => return Ok(Some(Null)),
            };
            match (&a[1], tys[1].base) {
                (Ts(t), Base::Timestamptz) => Ts(datetime::utc_to_local(*t, &zone)),
                (Ts(t), _) => Ts(datetime::local_to_utc(*t, &zone)),
                _ => Null,
            }
        }
        "date_bin" => {
            let (Interval(iv), Ts(t), Ts(origin)) = (&a[0], &a[1], &a[2]) else {
                return Ok(Some(Null));
            };
            if iv.months != 0 {
                return Err(err(
                    code::FEATURE_NOT_SUPPORTED,
                    "timestamps cannot be binned into intervals containing months or years",
                ));
            }
            let stride = iv.days as i64 * USECS_PER_DAY + iv.micros;
            if stride <= 0 {
                return Err(err(code::DATETIME_FIELD_OVERFLOW, "stride must be greater than zero"));
            }
            let diff = t - origin;
            Ts(origin + diff.div_euclid(stride) * stride)
        }
        // --- JSON
        "to_json" | "array_to_json" | "row_to_json" => {
            if a[0].is_null() {
                Null
            } else {
                Text(to_json_text(&a[0], tys[0], env))
            }
        }
        "to_jsonb" => {
            if a[0].is_null() {
                Null
            } else {
                Jsonb(Box::new(to_json_value(&a[0], tys[0], env).normalize()))
            }
        }
        "json_build_object" | "jsonb_build_object" => {
            if !a.len().is_multiple_of(2) {
                return Err(err(
                    code::INVALID_PARAMETER_VALUE,
                    "argument list must have even number of elements",
                )
                .hint(format!(
                    "The arguments of {name}() must consist of alternating keys and values."
                )));
            }
            if name == "jsonb_build_object" {
                let mut m = vec![];
                for (i, pair) in a.chunks(2).enumerate() {
                    if pair[0].is_null() {
                        return Err(err(
                            code::NULL_VALUE_NOT_ALLOWED,
                            format!("argument {}: key must not be null", i * 2 + 1),
                        ));
                    }
                    m.push((
                        to_str(&pair[0], tys[i * 2], env),
                        to_json_value(&pair[1], tys[i * 2 + 1], env),
                    ));
                }
                Jsonb(Box::new(Json::Object(m).normalize()))
            } else {
                let mut s = String::from("{");
                for (i, pair) in a.chunks(2).enumerate() {
                    if pair[0].is_null() {
                        return Err(err(
                            code::NULL_VALUE_NOT_ALLOWED,
                            format!("argument {}: key must not be null", i * 2 + 1),
                        ));
                    }
                    if i > 0 {
                        s.push_str(", ");
                    }
                    s.push_str(&json::escape(&to_str(&pair[0], tys[i * 2], env)));
                    s.push_str(" : ");
                    s.push_str(&to_json_text(&pair[1], tys[i * 2 + 1], env));
                }
                s.push('}');
                Text(s)
            }
        }
        "json_build_array" | "jsonb_build_array" => {
            if name == "jsonb_build_array" {
                Jsonb(Box::new(Json::Array(
                    a.iter().zip(tys).map(|(v, t)| to_json_value(v, *t, env).normalize()).collect(),
                )))
            } else {
                let parts: Vec<String> =
                    a.iter().zip(tys).map(|(v, t)| to_json_text(v, *t, env)).collect();
                Text(format!("[{}]", parts.join(", ")))
            }
        }
        "json_object" | "jsonb_object" => {
            let (keys, vals): (Vec<Value>, Vec<Value>) = if a.len() == 2 {
                (arr_items(&a[0]).to_vec(), arr_items(&a[1]).to_vec())
            } else {
                let items = arr_items(&a[0]);
                if !items.len().is_multiple_of(2) {
                    return Err(err(
                        code::ARRAY_SUBSCRIPT_ERROR,
                        "array must have even number of elements",
                    ));
                }
                (
                    items.iter().step_by(2).cloned().collect(),
                    items.iter().skip(1).step_by(2).cloned().collect(),
                )
            };
            let m: Vec<(String, Json)> = keys
                .iter()
                .zip(&vals)
                .map(|(k, v)| {
                    (
                        text(k).to_string(),
                        v.as_str().map_or(Json::Null, |s| Json::Str(s.to_string())),
                    )
                })
                .collect();
            let j = Json::Object(m);
            if name == "jsonb_object" {
                Jsonb(Box::new(j.normalize()))
            } else {
                Text(j.to_jsonb_string())
            }
        }
        "jsonb_typeof" | "json_typeof" => Text(json_of(&a[0], true)?.type_name().to_string()),
        "jsonb_array_length" | "json_array_length" => match json_of(&a[0], true)? {
            Json::Array(x) => Int(x.len() as i64),
            Json::Object(_) => {
                return Err(err(
                    code::INVALID_PARAMETER_VALUE,
                    "cannot get array length of a non-array",
                ));
            }
            _ => {
                return Err(err(
                    code::INVALID_PARAMETER_VALUE,
                    "cannot get array length of a scalar",
                ));
            }
        },
        "jsonb_extract_path"
        | "jsonb_extract_path_text"
        | "json_extract_path"
        | "json_extract_path_text" => {
            let jsonb = name.starts_with("jsonb");
            let path = Value::Array(Box::new(types::Array::new(a[1..].to_vec())));
            return json_path_op(&a[0], &path, jsonb, name.ends_with("_text")).map(Some);
        }
        "jsonb_set" | "jsonb_set_lax" | "jsonb_insert" => {
            let path = text_items(arr(&a[1]).unwrap_or(&types::Array::empty()));
            let new = json_of(&a[2], true)?;
            let flag = a.get(3).and_then(Value::as_bool);
            let r = if name == "jsonb_insert" {
                jsonb_set(jv(&a[0]), &path, &new, false, Some(flag.unwrap_or(false)))?
            } else {
                jsonb_set(jv(&a[0]), &path, &new, flag.unwrap_or(true), None)?
            };
            Jsonb(Box::new(r))
        }
        "jsonb_pretty" => Text(jv(&a[0]).pretty()),
        "jsonb_strip_nulls" => Jsonb(Box::new(strip_nulls(jv(&a[0])))),
        "json_strip_nulls" => Text(strip_nulls(&json_of(&a[0], false)?).to_compact_string()),
        "jsonb_exists" => Bool(jv(&a[0]).has_key(text(&a[1]))),
        "jsonb_concat" => Jsonb(Box::new(jsonb_concat(jv(&a[0]), jv(&a[1])))),
        // --- arrays
        "array_length" | "array_upper" | "array_lower" => {
            let Some(x) = arr(&a[0]) else { return Ok(Some(Null)) };
            let d = int(&a[1]);
            if d < 1 || d as usize > x.dims.len() {
                Null
            } else {
                let (len, lb) = x.dims[d as usize - 1];
                Int(match name {
                    "array_length" => len as i64,
                    "array_lower" => lb as i64,
                    _ => (lb + len - 1) as i64,
                })
            }
        }
        "cardinality" => Int(arr(&a[0]).map_or(0, |x| x.items.len()) as i64),
        "array_ndims" => arr(&a[0])
            .map_or(Null, |x| if x.dims.is_empty() { Null } else { Int(x.dims.len() as i64) }),
        "array_dims" => match arr(&a[0]) {
            Some(x) if !x.dims.is_empty() => {
                Text(x.dims.iter().map(|(l, lb)| format!("[{}:{}]", lb, lb + l - 1)).collect())
            }
            _ => Null,
        },
        "array_append" | "array_prepend" => {
            let (arr_v, elem, front) =
                if name == "array_append" { (&a[0], &a[1], false) } else { (&a[1], &a[0], true) };
            let mut x = arr(arr_v).cloned().unwrap_or_else(types::Array::empty);
            push_elem(&mut x, elem.clone(), front)?;
            Array(Box::new(x))
        }
        "array_cat" => match (&a[0], &a[1]) {
            (Null, Null) => Null,
            (Null, v) | (v, Null) => v.clone(),
            (x, y) => Array(Box::new(array_cat(arr(x).unwrap(), arr(y).unwrap())?)),
        },
        "array_remove" => match arr(&a[0]) {
            None => Null,
            Some(x) => {
                if x.dims.len() > 1 {
                    return Err(err(
                        code::FEATURE_NOT_SUPPORTED,
                        "removing elements from multidimensional arrays is not supported",
                    ));
                }
                let items: Vec<Value> = x
                    .items
                    .iter()
                    .filter(|v| {
                        !(v.is_null() && a[1].is_null()
                            || !a[1].is_null() && types::values_equal(v, &a[1]))
                    })
                    .cloned()
                    .collect();
                Array(Box::new(types::Array::new(items)))
            }
        },
        "array_replace" => match arr(&a[0]) {
            None => Null,
            Some(x) => {
                let mut y = x.clone();
                for v in &mut y.items {
                    if (v.is_null() && a[1].is_null())
                        || (!a[1].is_null() && !v.is_null() && types::values_equal(v, &a[1]))
                    {
                        *v = a[2].clone();
                    }
                }
                Array(Box::new(y))
            }
        },
        "array_position" => match arr(&a[0]) {
            None => Null,
            Some(x) => x
                .items
                .iter()
                .position(|v| {
                    (v.is_null() && a[1].is_null())
                        || (!v.is_null() && !a[1].is_null() && types::values_equal(v, &a[1]))
                })
                .map_or(Null, |p| Int(p as i64 + x.dims.first().map_or(1, |d| d.1) as i64)),
        },
        "array_positions" => match arr(&a[0]) {
            None => Null,
            Some(x) => Array(Box::new(types::Array::new(
                x.items
                    .iter()
                    .enumerate()
                    .filter(|(_, v)| {
                        (v.is_null() && a[1].is_null())
                            || (!v.is_null() && types::values_equal(v, &a[1]))
                    })
                    .map(|(i, _)| Int(i as i64 + 1))
                    .collect(),
            ))),
        },
        "array_fill" => {
            let dims: Vec<i64> = arr_items(&a[1]).iter().map(int).collect();
            let total: i64 = dims.iter().product();
            Array(Box::new(types::Array {
                dims: dims.iter().map(|&d| (d as i32, 1)).collect(),
                items: vec![a[0].clone(); total.max(0) as usize],
            }))
        }
        "trim_array" => match arr(&a[0]) {
            None => Null,
            Some(x) => {
                let n = int(&a[1]);
                if n < 0 || n as usize > x.items.len() {
                    return Err(err(
                        code::ARRAY_SUBSCRIPT_ERROR,
                        "number of elements to trim must be between 0 and ".to_string()
                            + &x.items.len().to_string(),
                    ));
                }
                let keep = x.items.len() - n as usize;
                Array(Box::new(types::Array::new(x.items[..keep].to_vec())))
            }
        },
        "num_nulls" => Int(a.iter().filter(|v| v.is_null()).count() as i64),
        "num_nonnulls" => Int(a.iter().filter(|v| !v.is_null()).count() as i64),
        "pg_size_pretty" => {
            let n = as_f64(&a[0]);
            Text(size_pretty(n))
        }
        _ => return Ok(None),
    };
    Ok(Some(v))
}

fn size_pretty(n: f64) -> String {
    let units = ["bytes", "kB", "MB", "GB", "TB", "PB"];
    let mut v = n;
    let mut i = 0;
    while v.abs() >= 10.0 * 1024.0 - 0.5 && i < units.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    format!("{} {}", v.round() as i64, units[i])
}

fn regex_split(re: &regex_lite::Regex, s: &str) -> Vec<String> {
    let mut out = vec![];
    let mut last = 0;
    for m in re.find_iter(s) {
        if m.start() == m.end() {
            // Zero-width match: split between characters.
            if m.start() == 0 || m.start() >= s.len() {
                continue;
            }
        }
        out.push(s[last..m.start()].to_string());
        last = m.end();
    }
    out.push(s[last..].to_string());
    if re.as_str().is_empty()
        || out.iter().all(|x| x.is_empty())
            && !s.is_empty()
            && re.find(s).is_some_and(|m| m.start() == m.end())
    {
        return s.chars().map(|c| c.to_string()).collect();
    }
    out
}

pub fn regex_split_pub(pattern: &str, flags: &str, s: &str) -> PgResult<Vec<String>> {
    let re = regex(pattern, flags)?;
    Ok(regex_split(&re, s))
}

fn similar_substring(s: &str, pat: &str, esc: &str) -> PgResult<Value> {
    let re_src = similar_to_regex(pat, esc.chars().next())?;
    let re = regex_lite::Regex::new(&re_src)
        .map_err(|e| err(code::INVALID_REGULAR_EXPRESSION, e.to_string()))?;
    Ok(match re.captures(s) {
        None => Value::Null,
        Some(c) => c
            .get(1)
            .or_else(|| c.get(0))
            .map_or(Value::Null, |m| Value::Text(m.as_str().to_string())),
    })
}

/// SIMILAR TO pattern → anchored regex.
pub fn similar_to_regex(p: &str, esc: Option<char>) -> PgResult<String> {
    let mut out = String::from("^(?:");
    let mut chars = p.chars().peekable();
    let mut in_class = false;
    while let Some(c) = chars.next() {
        if Some(c) == esc {
            match chars.next() {
                Some('"') => out.push_str(if out.contains("(?P<x>") { ")" } else { "(" }),
                Some(n) => {
                    out.push_str(&regex_lite::escape(&n.to_string()));
                }
                None => return Err(err(code::INVALID_ESCAPE_SEQUENCE, "invalid escape string")),
            }
            continue;
        }
        if in_class {
            out.push(c);
            if c == ']' {
                in_class = false;
            }
            continue;
        }
        match c {
            '%' => out.push_str(".*"),
            '_' => out.push('.'),
            '[' => {
                in_class = true;
                out.push('[');
            }
            '|' | '*' | '+' | '?' | '(' | ')' | '{' | '}' => out.push(c),
            '.' | '^' | '$' | '\\' => {
                out.push('\\');
                out.push(c);
            }
            c => out.push(c),
        }
    }
    out.push_str(")$");
    Ok(out)
}

fn unit_err(field: &str, ty: &str) -> PgError {
    err(code::FEATURE_NOT_SUPPORTED, format!("unit \"{field}\" not supported for type {ty}"))
}

pub fn normalize_field(f: &str) -> String {
    match f {
        "years" | "y" | "yr" | "yrs" => "year",
        "months" | "mon" | "mons" => "month",
        "days" | "d" => "day",
        "hours" | "h" | "hr" | "hrs" => "hour",
        "minutes" | "m" | "min" | "mins" => "minute",
        "seconds" | "s" | "sec" | "secs" => "second",
        "milliseconds" | "ms" | "msec" | "msecs" | "millisecond" => "milliseconds",
        "microseconds" | "us" | "usec" | "usecs" | "microsecond" => "microseconds",
        "weeks" | "w" => "week",
        "decades" => "decade",
        "centuries" => "century",
        "millenniums" | "millennia" => "millennium",
        "tz" => "timezone",
        other => other,
    }
    .to_string()
}

pub fn extract_value(field: &str, v: &Value, ty: Type, env: &Env) -> PgResult<Numeric> {
    let bad = |tn: &str| {
        err(code::FEATURE_NOT_SUPPORTED, format!("unit \"{field}\" not supported for type {tn}"))
    };
    let unknown = |tn: &str| {
        err(code::INVALID_PARAMETER_VALUE, format!("unit \"{field}\" not recognized for type {tn}"))
    };
    let known = [
        "year",
        "month",
        "day",
        "hour",
        "minute",
        "second",
        "milliseconds",
        "microseconds",
        "quarter",
        "dow",
        "isodow",
        "doy",
        "week",
        "isoyear",
        "decade",
        "century",
        "millennium",
        "julian",
        "epoch",
        "timezone",
        "timezone_hour",
        "timezone_minute",
    ];
    match (v, ty.base) {
        (Value::Interval(iv), _) => datetime::extract_interval(field, iv).ok_or_else(|| {
            if known.contains(&field) { bad("interval") } else { unknown("interval") }
        }),
        (Value::Time(t), _) => {
            let r = match field {
                "hour" => Some(Numeric::from_i64(t / 3_600_000_000)),
                "minute" => Some(Numeric::from_i64(t / 60_000_000 % 60)),
                "second" => Numeric::from_i64(t % 60_000_000)
                    .div_scale(&Numeric::from_i64(1_000_000), 6, false)
                    .ok(),
                "milliseconds" => Numeric::from_i64(t % 60_000_000)
                    .div_scale(&Numeric::from_i64(1000), 3, false)
                    .ok(),
                "microseconds" => Some(Numeric::from_i64(t % 60_000_000)),
                "epoch" => {
                    Numeric::from_i64(*t).div_scale(&Numeric::from_i64(1_000_000), 6, false).ok()
                }
                _ => None,
            };
            r.ok_or_else(|| {
                if known.contains(&field) {
                    bad("time without time zone")
                } else {
                    unknown("time without time zone")
                }
            })
        }
        (Value::Date(d), _) => {
            if matches!(
                field,
                "hour"
                    | "minute"
                    | "second"
                    | "milliseconds"
                    | "microseconds"
                    | "timezone"
                    | "timezone_hour"
                    | "timezone_minute"
            ) {
                return Err(bad("date"));
            }
            if matches!(*d, datetime::DATE_INF | datetime::DATE_NEG_INF) {
                return Ok(
                    if field == "epoch"
                        || field == "year"
                        || field == "decade"
                        || field == "century"
                        || field == "millennium"
                        || field == "julian"
                        || field == "isoyear"
                    {
                        Numeric::Inf(*d == datetime::DATE_NEG_INF)
                    } else {
                        Numeric::NaN
                    },
                );
            }
            let local = casts::date_to_ts(*d);
            datetime::extract(field, local, None, None, true).ok_or_else(|| unknown("date"))
        }
        (Value::Ts(t), b) => {
            let tz = b == Base::Timestamptz;
            let tn = if tz { "timestamp with time zone" } else { "timestamp without time zone" };
            if matches!(*t, datetime::TS_INF | datetime::TS_NEG_INF) {
                return Ok(Numeric::Inf(*t == datetime::TS_NEG_INF));
            }
            let (local, utc, off) = if tz {
                let off = env
                    .fmt
                    .zone
                    .offset_at_utc(t.div_euclid(USECS_PER_SEC) - datetime::PG_EPOCH_DAYS * 86400);
                (t + off as i64 * USECS_PER_SEC, Some(*t), Some(off))
            } else {
                (*t, None, None)
            };
            if !tz && field.starts_with("timezone") {
                return Err(bad(tn));
            }
            datetime::extract(field, local, utc, off, false).ok_or_else(|| unknown(tn))
        }
        _ => Err(unknown(&ty.display(-1))),
    }
}

// ---------------------------------------------------------------------------
// to_char / to_date / to_timestamp

const MONTH_NAMES: [&str; 12] = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
];
const DAY_NAMES: [&str; 7] =
    ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];

fn to_char(v: &Value, ty: Type, fmt: &str, env: &Env) -> PgResult<Value> {
    match v {
        Value::Ts(_) | Value::Date(_) | Value::Interval(_) => {}
        Value::Int(_) | Value::Num(_) | Value::Float(_) => {
            return Ok(Value::Text(to_char_num(v, fmt)));
        }
        _ => return Ok(Value::Null),
    }
    let (local, off, iv) = match (v, ty.base) {
        (Value::Ts(t), Base::Timestamptz) => {
            let off = env
                .fmt
                .zone
                .offset_at_utc(t.div_euclid(USECS_PER_SEC) - datetime::PG_EPOCH_DAYS * 86400);
            (t + off as i64 * USECS_PER_SEC, Some(off), None)
        }
        (Value::Ts(t), _) => (*t, None, None),
        (Value::Date(d), _) => (casts::date_to_ts(*d), None, None),
        (Value::Interval(iv), _) => (iv.micros, None, Some(*iv)),
        _ => return Ok(Value::Null),
    };
    let f = datetime::fields(local);
    let (year, month, day) = match iv {
        Some(i) => ((i.months / 12) as i64, (i.months % 12) as u32, i.days as u32),
        None => (f.year, f.month, f.day),
    };
    let hour = match iv {
        Some(i) => i.micros / 3_600_000_000,
        None => f.hour,
    };
    let sec = f.micros / 1_000_000;
    let us = f.micros % 1_000_000;
    let dow = (f.days + datetime::PG_EPOCH_DAYS + 4).rem_euclid(7) as usize;
    let doy = f.days - (datetime::days_from_civil(f.year, 1, 1) - datetime::PG_EPOCH_DAYS) + 1;
    let mut out = String::new();
    let b = fmt.as_bytes();
    let mut i = 0;
    let mut fm = false;
    let starts =
        |i: usize, p: &str| fmt.len() >= i + p.len() && fmt[i..i + p.len()].eq_ignore_ascii_case(p);
    let num = |out: &mut String, v: i64, w: usize, fm: bool| {
        if fm {
            out.push_str(&v.to_string());
        } else {
            out.push_str(&format!("{v:0w$}"));
        }
    };
    let case_of = |i: usize, s: &str| -> String {
        let p = &fmt[i..];
        if p.starts_with(|c: char| c.is_ascii_uppercase())
            && p.chars().nth(1).is_some_and(|c| c.is_ascii_uppercase())
        {
            s.to_uppercase()
        } else if p.starts_with(|c: char| c.is_ascii_uppercase()) {
            s.to_string()
        } else {
            s.to_lowercase()
        }
    };
    while i < b.len() {
        if starts(i, "FM") {
            fm = true;
            i += 2;
            continue;
        }
        let fm_here = fm;
        fm = false;
        let fm = fm_here;
        let pad_name = |s: String, w: usize, fm: bool| if fm { s } else { format!("{s:<w$}") };
        if b[i] == b'"' {
            i += 1;
            while i < b.len() && b[i] != b'"' {
                if b[i] == b'\\' && i + 1 < b.len() {
                    i += 1;
                }
                out.push(b[i] as char);
                i += 1;
            }
            i += 1;
        } else if starts(i, "HH24") {
            num(&mut out, hour, 2, fm);
            i += 4;
        } else if starts(i, "HH12") || starts(i, "HH") {
            let h = hour % 12;
            num(&mut out, if h == 0 { 12 } else { h }, 2, fm);
            i += if starts(i, "HH12") { 4 } else { 2 };
        } else if starts(i, "MI") {
            num(&mut out, f.minute, 2, fm);
            i += 2;
        } else if starts(i, "SSSS") {
            num(&mut out, hour * 3600 + f.minute * 60 + sec, 1, true);
            i += 4;
        } else if starts(i, "SS") {
            num(&mut out, sec, 2, fm);
            i += 2;
        } else if starts(i, "MS") {
            num(&mut out, us / 1000, 3, fm);
            i += 2;
        } else if starts(i, "US") {
            num(&mut out, us, 6, fm);
            i += 2;
        } else if starts(i, "AM") || starts(i, "PM") {
            out.push_str(&case_of(i, if hour >= 12 { "PM" } else { "AM" }));
            i += 2;
        } else if starts(i, "A.M.") || starts(i, "P.M.") {
            out.push_str(&case_of(i, if hour >= 12 { "P.M." } else { "A.M." }));
            i += 4;
        } else if starts(i, "YYYY") {
            num(&mut out, if year <= 0 { 1 - year } else { year }, 4, fm);
            i += 4;
        } else if starts(i, "Y,YYY") {
            out.push_str(&format!("{},{:03}", year / 1000, year % 1000));
            i += 5;
        } else if starts(i, "YYY") {
            num(&mut out, year % 1000, 3, fm);
            i += 3;
        } else if starts(i, "YY") {
            num(&mut out, year % 100, 2, fm);
            i += 2;
        } else if starts(i, "IYYY") {
            num(&mut out, datetime::iso_week(f.days).1, 4, fm);
            i += 4;
        } else if starts(i, "IW") {
            num(&mut out, datetime::iso_week(f.days).0, 2, fm);
            i += 2;
        } else if starts(i, "MONTH") {
            let n = MONTH_NAMES[month.saturating_sub(1) as usize % 12];
            out.push_str(&pad_name(case_of(i, n), 9, fm));
            i += 5;
        } else if starts(i, "MON") {
            let n = &MONTH_NAMES[month.saturating_sub(1) as usize % 12][..3];
            out.push_str(&case_of(i, n));
            i += 3;
        } else if starts(i, "MM") {
            num(&mut out, month as i64, 2, fm);
            i += 2;
        } else if starts(i, "DAY") {
            out.push_str(&pad_name(case_of(i, DAY_NAMES[dow]), 9, fm));
            i += 3;
        } else if starts(i, "DY") {
            out.push_str(&case_of(i, &DAY_NAMES[dow][..3]));
            i += 2;
        } else if starts(i, "DDD") {
            num(&mut out, doy, 3, fm);
            i += 3;
        } else if starts(i, "DD") {
            num(&mut out, day as i64, 2, fm);
            i += 2;
        } else if starts(i, "D") && !starts(i, "DE") {
            num(&mut out, dow as i64 + 1, 1, fm);
            i += 1;
        } else if starts(i, "Q") {
            num(&mut out, (month as i64 - 1) / 3 + 1, 1, fm);
            i += 1;
        } else if starts(i, "WW") {
            num(&mut out, (doy - 1) / 7 + 1, 2, fm);
            i += 2;
        } else if starts(i, "CC") {
            num(&mut out, (year + 99) / 100, 2, fm);
            i += 2;
        } else if starts(i, "J") {
            num(&mut out, f.days + 2_451_545, 1, true);
            i += 1;
        } else if starts(i, "TZH") {
            let o = off.unwrap_or(0);
            out.push_str(&format!("{}{:02}", if o < 0 { '-' } else { '+' }, o.abs() / 3600));
            i += 3;
        } else if starts(i, "TZM") {
            num(&mut out, (off.unwrap_or(0).abs() / 60 % 60) as i64, 2, fm);
            i += 3;
        } else if starts(i, "TZ") || starts(i, "OF") {
            let o = off.unwrap_or(0);
            let abbr = if starts(i, "OF") {
                super::tz::format_offset(o, false)
            } else {
                let (_, _, ab) = env.fmt.zone.info_at_utc(
                    (local - o as i64 * USECS_PER_SEC).div_euclid(USECS_PER_SEC)
                        - datetime::PG_EPOCH_DAYS * 86400,
                );
                if off.is_some() { ab } else { String::new() }
            };
            out.push_str(&if b[i].is_ascii_lowercase() { abbr.to_lowercase() } else { abbr });
            i += 2;
        } else if starts(i, "BC") || starts(i, "AD") {
            out.push_str(&case_of(i, if year <= 0 { "BC" } else { "AD" }));
            i += 2;
        } else {
            // Copy literal character (UTF-8 safe).
            let ch = fmt[i..].chars().next().unwrap();
            if ch == '\\' && i + 1 < b.len() {
                let n = fmt[i + 1..].chars().next().unwrap();
                out.push(n);
                i += 1 + n.len_utf8();
                continue;
            }
            out.push(ch);
            i += ch.len_utf8();
        }
    }
    Ok(Value::Text(out))
}

fn to_char_num(v: &Value, fmt: &str) -> String {
    // Supports digit patterns 9/0, decimal point, FM, and thousands separator.
    let n = as_num(v);
    let neg = n.is_negative();
    let fm = fmt.to_ascii_uppercase().contains("FM");
    let pat: String =
        fmt.chars().filter(|c| matches!(c, '9' | '0' | '.' | ',' | 'D' | 'G')).collect();
    let (int_pat, frac_pat) = match pat.find(['.', 'D']) {
        Some(p) => (&pat[..p], &pat[p + 1..]),
        None => (pat.as_str(), ""),
    };
    let frac_digits = frac_pat.chars().filter(|c| matches!(c, '9' | '0')).count();
    let r = n.abs().round(frac_digits as i64).to_string();
    let (ip, fp) = r.split_once('.').unwrap_or((&r, ""));
    let int_slots = int_pat.chars().filter(|c| matches!(c, '9' | '0')).count();
    if ip.len() > int_slots {
        let hashes: String =
            pat.chars().map(|c| if matches!(c, '9' | '0') { '#' } else { c }).collect();
        return format!(" {hashes}");
    }
    let mut digits: Vec<char> = ip.chars().collect();
    let mut out = String::new();
    let mut di = digits.len() as i64 - int_slots as i64;
    let mut started = false;
    for c in int_pat.chars() {
        match c {
            '9' | '0' => {
                if di < 0 {
                    if c == '0' || (int_slots == 0) {
                        out.push('0');
                        started = true;
                    } else if !fm {
                        out.push(' ');
                    }
                } else {
                    let d = digits[di as usize];
                    if d == '0' && !started && c == '9' && (di as usize) < digits.len() - 1 {
                        if !fm {
                            out.push(' ');
                        }
                    } else {
                        out.push(d);
                        started = true;
                    }
                }
                di += 1;
            }
            ',' | 'G' => {
                if started {
                    out.push(',');
                } else if !fm {
                    out.push(' ');
                }
            }
            _ => {}
        }
    }
    if ip == "0" && !out.ends_with('0') && frac_digits == 0 {
        out.push('0');
    }
    digits.clear();
    if !frac_pat.is_empty() {
        out.push('.');
        let mut f: String = fp.to_string();
        while f.len() < frac_digits {
            f.push('0');
        }
        if fm {
            out.push_str(f.trim_end_matches('0'));
        } else {
            out.push_str(&f);
        }
    }
    let sign = if neg {
        "-"
    } else if fm {
        ""
    } else {
        " "
    };
    // Place the sign right before the first digit.
    let trimmed = out.trim_start();
    let lead = out.len() - trimmed.len();
    format!("{}{}{}", &out[..lead], sign, trimmed)
}

fn parse_by_format(s: &str, fmt: &str) -> PgResult<(i64, u32, u32, i64, i64, i64, i64)> {
    let (mut y, mut mo, mut d, mut h, mut mi, mut se, mut us) =
        (1i64, 1u32, 1u32, 0i64, 0i64, 0i64, 0i64);
    let mut pm: Option<bool> = None;
    let sb = s.as_bytes();
    let mut si = 0;
    let fb = fmt;
    let mut fi = 0;
    let take_num = |si: &mut usize, max: usize| -> PgResult<i64> {
        while *si < sb.len() && sb[*si] == b' ' {
            *si += 1;
        }
        let start = *si;
        if *si < sb.len() && sb[*si] == b'-' {
            *si += 1;
        }
        while *si < sb.len() && sb[*si].is_ascii_digit() && *si - start < max {
            *si += 1;
        }
        s[start..*si].parse().map_err(|_| {
            err(
                code::INVALID_DATETIME_FORMAT,
                format!("invalid value \"{}\" for format", &s[start.min(s.len())..]),
            )
        })
    };
    let starts =
        |i: usize, p: &str| fb.len() >= i + p.len() && fb[i..i + p.len()].eq_ignore_ascii_case(p);
    while fi < fb.len() {
        if starts(fi, "FM") {
            fi += 2;
        } else if starts(fi, "YYYY") {
            y = take_num(&mut si, 4)?;
            fi += 4;
        } else if starts(fi, "YY") {
            y = 2000 + take_num(&mut si, 2)?;
            fi += 2;
        } else if starts(fi, "MONTH") || starts(fi, "MON") {
            let rest = s[si..].to_ascii_lowercase();
            let idx = MONTH_NAMES
                .iter()
                .position(|m| rest.starts_with(&m[..3].to_ascii_lowercase()))
                .ok_or_else(|| {
                    err(
                        code::INVALID_DATETIME_FORMAT,
                        format!("invalid value \"{}\" for \"MON\"", &s[si..]),
                    )
                })?;
            mo = idx as u32 + 1;
            let full = MONTH_NAMES[idx].to_ascii_lowercase();
            si += if starts(fi, "MONTH") && rest.starts_with(&full) { full.len() } else { 3 };
            fi += if starts(fi, "MONTH") { 5 } else { 3 };
        } else if starts(fi, "MM") {
            mo = take_num(&mut si, 2)? as u32;
            fi += 2;
        } else if starts(fi, "DDD") {
            let doy = take_num(&mut si, 3)?;
            let base = datetime::days_from_civil(y, 1, 1) + doy - 1;
            let (_, m2, d2) = datetime::civil_from_days(base);
            mo = m2;
            d = d2;
            fi += 3;
        } else if starts(fi, "DD") {
            d = take_num(&mut si, 2)? as u32;
            fi += 2;
        } else if starts(fi, "HH24") {
            h = take_num(&mut si, 2)?;
            fi += 4;
        } else if starts(fi, "HH12") || starts(fi, "HH") {
            h = take_num(&mut si, 2)?;
            fi += if starts(fi, "HH12") { 4 } else { 2 };
        } else if starts(fi, "MI") {
            mi = take_num(&mut si, 2)?;
            fi += 2;
        } else if starts(fi, "SS") {
            se = take_num(&mut si, 2)?;
            fi += 2;
        } else if starts(fi, "MS") {
            us = take_num(&mut si, 3)? * 1000;
            fi += 2;
        } else if starts(fi, "US") {
            us = take_num(&mut si, 6)?;
            fi += 2;
        } else if starts(fi, "AM") || starts(fi, "PM") {
            let r = s[si..].to_ascii_uppercase();
            pm = Some(r.starts_with("PM"));
            si += 2;
            fi += 2;
        } else {
            fi += fb[fi..].chars().next().map_or(1, char::len_utf8);
            if si < sb.len() && !sb[si].is_ascii_alphanumeric() {
                si += 1;
            }
        }
    }
    if let Some(p) = pm {
        if p && h < 12 {
            h += 12;
        } else if !p && h == 12 {
            h = 0;
        }
    }
    if !(1..=12).contains(&mo) || d < 1 || d > datetime::days_in_month(y, mo) {
        return Err(err(
            code::DATETIME_FIELD_OVERFLOW,
            format!("date/time field value out of range: \"{s}\""),
        ));
    }
    Ok((y, mo, d, h, mi, se, us))
}

fn to_date_fmt(s: &str, fmt: &str) -> PgResult<i32> {
    let (y, m, d, ..) = parse_by_format(s, fmt)?;
    Ok(datetime::date_from_ymd(y, m, d))
}

fn to_timestamp_fmt(s: &str, fmt: &str) -> PgResult<i64> {
    let (y, m, d, h, mi, se, us) = parse_by_format(s, fmt)?;
    Ok(datetime::date_from_ymd(y, m, d) as i64 * USECS_PER_DAY
        + (h * 3600 + mi * 60 + se) * USECS_PER_SEC
        + us)
}

// ---------------------------------------------------------------------------
// Randomness and encodings

pub fn random_u64() -> u64 {
    use std::cell::Cell;
    use std::hash::{BuildHasher, Hasher};
    thread_local! {
        static STATE: Cell<u64> = Cell::new({
            let mut h = std::collections::hash_map::RandomState::new().build_hasher();
            h.write_u64(datetime::now_micros() as u64);
            h.finish() | 1
        });
    }
    STATE.with(|s| {
        // xorshift64*
        let mut x = s.get();
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        s.set(x);
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    })
}

pub fn random_f64() -> f64 {
    (random_u64() >> 11) as f64 / (1u64 << 53) as f64
}

pub fn random_uuid() -> [u8; 16] {
    let mut u = [0u8; 16];
    u[..8].copy_from_slice(&random_u64().to_be_bytes());
    u[8..].copy_from_slice(&random_u64().to_be_bytes());
    u[6] = (u[6] & 0x0f) | 0x40;
    u[8] = (u[8] & 0x3f) | 0x80;
    u
}

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

pub fn base64_encode(b: &[u8]) -> String {
    let mut out = String::new();
    for (n, chunk) in b.chunks(3).enumerate() {
        if n > 0 && n % 19 == 0 {
            out.push('\n');
        }
        let v = (chunk[0] as u32) << 16
            | (*chunk.get(1).unwrap_or(&0) as u32) << 8
            | *chunk.get(2).unwrap_or(&0) as u32;
        out.push(B64[(v >> 18) as usize & 63] as char);
        out.push(B64[(v >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 { B64[(v >> 6) as usize & 63] as char } else { '=' });
        out.push(if chunk.len() > 2 { B64[v as usize & 63] as char } else { '=' });
    }
    out
}

pub fn base64_decode(s: &str) -> Option<Vec<u8>> {
    let mut out = vec![];
    let mut buf = 0u32;
    let mut bits = 0;
    for c in s.bytes() {
        if c.is_ascii_whitespace() {
            continue;
        }
        if c == b'=' {
            break;
        }
        let v = B64.iter().position(|&x| x == c)? as u32;
        buf = buf << 6 | v;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((buf >> bits) as u8);
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn like_patterns() {
        assert!(like("hello", "h%o", false, Some('\\')).unwrap());
        assert!(like("hello", "h_llo", false, Some('\\')).unwrap());
        assert!(!like("hello", "h_lo", false, Some('\\')).unwrap());
        assert!(like("50%", "50\\%", false, Some('\\')).unwrap());
        assert!(like("HeLLo", "hel%", true, Some('\\')).unwrap());
        assert!(like("", "%", false, None).unwrap());
        assert!(like("abcabc", "%bc", false, None).unwrap());
    }

    #[test]
    fn base64() {
        assert_eq!(base64_encode(b"hello"), "aGVsbG8=");
        assert_eq!(base64_decode("aGVsbG8=").unwrap(), b"hello");
    }

    #[test]
    fn quoting() {
        assert_eq!(quote_ident("abc"), "abc");
        assert_eq!(quote_ident("Abc"), "\"Abc\"");
        assert_eq!(quote_ident("select"), "\"select\"");
        assert_eq!(quote_literal("it's"), "'it''s'");
    }

    #[test]
    fn to_char_formats() {
        let f = FmtCtx::default();
        let env = Env { fmt: &f, now: 0, stmt_now: 0 };
        let t = datetime::parse_timestamp("2020-03-05 14:07:09.123", &env.dctx()).unwrap();
        let s = to_char(&Value::Ts(t), Type::TIMESTAMP, "YYYY-MM-DD HH24:MI:SS.MS Dy Mon", &env)
            .unwrap();
        assert_eq!(s.as_str(), Some("2020-03-05 14:07:09.123 Thu Mar"));
        let s = to_char(&Value::Ts(t), Type::TIMESTAMP, "FMMonth FMDD, HH12 AM", &env).unwrap();
        assert_eq!(s.as_str(), Some("March 5, 02 PM"));
    }
}
