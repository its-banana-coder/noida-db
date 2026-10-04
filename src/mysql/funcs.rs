//! MySQL's scalar function library: the string, math, date and JSON
//! functions application SQL and ORMs actually emit. Session functions
//! (`DATABASE()`, `LAST_INSERT_ID()`, ...) and the original handful
//! (`CONCAT`, `SUBSTRING`, `COALESCE`, `NOW()`, ...) live in
//! `exec::eval_call`, which falls through to `eval` here.
//!
//! NULL in, NULL out unless MySQL says otherwise (`CONCAT_WS`, `IF`,
//! `JSON_ARRAY`, ...). Strings compare the way the default `_ci` collation
//! does (`LOCATE`, `FIELD`, `STRCMP`), via `exec::mysql_cmp`.

use std::cmp::Ordering;

use crate::mysql::error::MySqlError;
use crate::mysql::exec::{
    eval_call, mysql_cmp, parse_mysql_date, parse_mysql_datetime, render_text, value_as_text,
    value_to_f64, value_to_numeric,
};
use crate::mysql::types::Value;
use crate::sql::datetime::{
    USECS_PER_DAY, USECS_PER_SEC, date_from_ymd, days_in_month, ymd_from_date,
};
use crate::sql::json::{self, Json};
use crate::sql::numeric::Numeric;

/// Days from 1970-01-01 to 2000-01-01, the epoch `Value::Date`/`Ts` count from.
const UNIX_EPOCH_SECS: i64 = 10_957 * 86_400;

pub(crate) fn eval(name: &str, a: &[Value]) -> Result<Value, MySqlError> {
    let arg = |i: usize| a.get(i).cloned().unwrap_or(Value::Null);
    let text = |i: usize| a.get(i).and_then(value_as_text);
    let int = |i: usize| a.get(i).filter(|v| !v.is_null()).map(|v| value_to_f64(v) as i64);
    let need = |n: usize| -> Result<(), MySqlError> {
        if a.len() < n {
            Err(MySqlError::new(
                1582,
                "42000",
                format!("Incorrect parameter count in the call to native function '{name}'"),
            ))
        } else {
            Ok(())
        }
    };
    let any_null = |n: usize| a.iter().take(n).any(Value::is_null);

    Ok(match name {
        // ---- control flow ----
        "IF" => {
            need(3)?;
            if truthy(&a[0]) { arg(1) } else { arg(2) }
        }
        "NULLIF" => {
            need(2)?;
            if !a[0].is_null() && mysql_cmp(&a[0], &a[1]) == Some(Ordering::Equal) {
                Value::Null
            } else {
                arg(0)
            }
        }
        "GREATEST" | "LEAST" => {
            need(2)?;
            if a.iter().any(Value::is_null) {
                return Ok(Value::Null);
            }
            let want = if name == "GREATEST" { Ordering::Greater } else { Ordering::Less };
            let mut best = &a[0];
            for v in &a[1..] {
                if mysql_cmp(v, best) == Some(want) {
                    best = v;
                }
            }
            best.clone()
        }

        // ---- math ----
        "ROUND" | "TRUNCATE" => {
            need(if name == "TRUNCATE" { 2 } else { 1 })?;
            if any_null(a.len()) {
                return Ok(Value::Null);
            }
            let d = int(1).unwrap_or(0);
            let trunc = name == "TRUNCATE";
            match &a[0] {
                Value::Int(i) if d >= 0 => Value::Int(*i),
                Value::Float(f) => Value::Float(round_f64(*f, d, trunc)),
                Value::Text(s) => match s.trim().parse::<f64>() {
                    Ok(f) => Value::Float(round_f64(f, d, trunc)),
                    Err(_) => Value::Float(0.0),
                },
                v => {
                    let n = value_to_numeric(v).unwrap_or_else(Numeric::zero);
                    let r = if trunc { n.trunc(d) } else { n.round(d) };
                    match (v, r.to_i64()) {
                        (Value::Int(_), Some(i)) => Value::Int(i),
                        _ => Value::Num(r),
                    }
                }
            }
        }
        "ABS" | "SIGN" | "CEIL" | "CEILING" | "FLOOR" => {
            need(1)?;
            match &a[0] {
                Value::Null => Value::Null,
                Value::Int(i) => Value::Int(match name {
                    "ABS" => i.checked_abs().ok_or_else(|| out_of_range(name))?,
                    "SIGN" => i.signum(),
                    _ => *i,
                }),
                Value::Num(n) => match name {
                    "ABS" => Value::Num(n.abs()),
                    "SIGN" => Value::Int(n.sign().to_i64().unwrap_or(0)),
                    "FLOOR" => int_or_num(n.floor()),
                    _ => int_or_num(n.ceil()),
                },
                v => {
                    let f = value_to_f64(v);
                    match name {
                        "ABS" => Value::Float(f.abs()),
                        "SIGN" => Value::Int(if f > 0.0 {
                            1
                        } else if f < 0.0 {
                            -1
                        } else {
                            0
                        }),
                        "FLOOR" => Value::Float(f.floor()),
                        _ => Value::Float(f.ceil()),
                    }
                }
            }
        }
        "MOD" => {
            need(2)?;
            crate::mysql::exec::eval_arith(crate::mysql::plan::ArithOp::Mod, arg(0), arg(1))?
        }
        "DIV" => {
            need(2)?;
            if any_null(2) {
                return Ok(Value::Null);
            }
            match (&a[0], &a[1]) {
                (_, Value::Int(0)) => Value::Null,
                (Value::Int(x), Value::Int(y)) => {
                    Value::Int(x.checked_div(*y).ok_or_else(|| out_of_range(name))?)
                }
                (x, y) => {
                    let d = value_to_f64(y);
                    if d == 0.0 {
                        Value::Null
                    } else {
                        Value::Int((value_to_f64(x) / d).trunc() as i64)
                    }
                }
            }
        }
        "POW" | "POWER" => {
            need(2)?;
            if any_null(2) {
                return Ok(Value::Null);
            }
            Value::Float(value_to_f64(&a[0]).powf(value_to_f64(&a[1])))
        }
        "SQRT" => {
            need(1)?;
            if any_null(1) {
                return Ok(Value::Null);
            }
            let f = value_to_f64(&a[0]);
            if f < 0.0 { Value::Null } else { Value::Float(f.sqrt()) }
        }

        // ---- strings ----
        "UCASE" => return eval_call("UPPER", a),
        "LCASE" => return eval_call("LOWER", a),
        "MID" => return eval_call("SUBSTRING", a),
        "CHAR_LENGTH" | "CHARACTER_LENGTH" => match text(0) {
            Some(s) => Value::Int(s.chars().count() as i64),
            None => Value::Null,
        },
        "CONCAT_WS" => {
            need(1)?;
            let Some(sep) = text(0) else { return Ok(Value::Null) };
            let parts: Vec<String> = a[1..].iter().filter_map(value_as_text).collect();
            Value::Text(parts.join(&sep))
        }
        "TRIM" | "LTRIM" | "RTRIM" => {
            need(1)?;
            let Some(s) = text(0) else { return Ok(Value::Null) };
            let what = match a.get(1) {
                Some(v) if v.is_null() => return Ok(Value::Null),
                Some(v) => render_text(v),
                None => " ".to_string(),
            };
            let mut out = s.as_str();
            if !what.is_empty() {
                if name != "RTRIM" {
                    while let Some(rest) = out.strip_prefix(what.as_str()) {
                        out = rest;
                    }
                }
                if name != "LTRIM" {
                    while let Some(rest) = out.strip_suffix(what.as_str()) {
                        out = rest;
                    }
                }
            }
            Value::Text(out.to_string())
        }
        "REPLACE" => {
            need(3)?;
            let (Some(s), Some(from), Some(to)) = (text(0), text(1), text(2)) else {
                return Ok(Value::Null);
            };
            Value::Text(if from.is_empty() { s } else { s.replace(&from, &to) })
        }
        "LEFT" | "RIGHT" => {
            need(2)?;
            let (Some(s), Some(n)) = (text(0), int(1)) else { return Ok(Value::Null) };
            let chars: Vec<char> = s.chars().collect();
            let n = (n.max(0) as usize).min(chars.len());
            Value::Text(if name == "LEFT" {
                chars[..n].iter().collect()
            } else {
                chars[chars.len() - n..].iter().collect()
            })
        }
        "LPAD" | "RPAD" => {
            need(3)?;
            let (Some(s), Some(n), Some(pad)) = (text(0), int(1), text(2)) else {
                return Ok(Value::Null);
            };
            if n < 0 {
                return Ok(Value::Null);
            }
            let n = n as usize;
            let chars: Vec<char> = s.chars().collect();
            if chars.len() >= n {
                return Ok(Value::Text(chars[..n].iter().collect()));
            }
            if pad.is_empty() {
                return Ok(Value::Null);
            }
            let fill: String = pad.chars().cycle().take(n - chars.len()).collect();
            Value::Text(if name == "LPAD" { fill + &s } else { s + &fill })
        }
        "REPEAT" => {
            need(2)?;
            let (Some(s), Some(n)) = (text(0), int(1)) else { return Ok(Value::Null) };
            Value::Text(s.repeat(n.clamp(0, 1 << 20) as usize))
        }
        "REVERSE" => match text(0) {
            Some(s) => Value::Text(s.chars().rev().collect()),
            None => Value::Null,
        },
        // Positions are 1-based character offsets, 0 when absent; the
        // search is case-insensitive like the default collation.
        "LOCATE" | "INSTR" => {
            need(2)?;
            let (needle, hay) =
                if name == "LOCATE" { (text(0), text(1)) } else { (text(1), text(0)) };
            let (Some(needle), Some(hay)) = (needle, hay) else { return Ok(Value::Null) };
            let start = if name == "LOCATE" { int(2).unwrap_or(1) } else { 1 };
            Value::Int(locate_ci(&needle, &hay, start))
        }
        "FIELD" => {
            need(2)?;
            if a[0].is_null() {
                return Ok(Value::Int(0));
            }
            let pos = a[1..].iter().position(|v| mysql_cmp(&a[0], v) == Some(Ordering::Equal));
            Value::Int(pos.map(|p| p as i64 + 1).unwrap_or(0))
        }
        "ELT" => {
            need(2)?;
            match int(0) {
                Some(n) if n >= 1 => arg(n as usize),
                _ => Value::Null,
            }
        }
        "STRCMP" => {
            need(2)?;
            if any_null(2) {
                return Ok(Value::Null);
            }
            Value::Int(match mysql_cmp(&a[0], &a[1]) {
                Some(Ordering::Less) => -1,
                Some(Ordering::Greater) => 1,
                _ => 0,
            })
        }
        "HEX" => match &arg(0) {
            Value::Null => Value::Null,
            Value::Int(i) => Value::Text(format!("{:X}", i)),
            Value::Bytes(b) => Value::Text(b.iter().map(|x| format!("{x:02X}")).collect()),
            v => Value::Text(render_text(v).bytes().map(|x| format!("{x:02X}")).collect()),
        },

        // ---- dates ----
        "DATE" => match to_ts(&arg(0)) {
            Some(t) => Value::Date(t.div_euclid(USECS_PER_DAY) as i32),
            None => Value::Null,
        },
        "TIME" => match to_ts(&arg(0)) {
            Some(t) => Value::Time(t.rem_euclid(USECS_PER_DAY)),
            None => Value::Null,
        },
        "YEAR" | "MONTH" | "DAY" | "DAYOFMONTH" | "HOUR" | "MINUTE" | "SECOND" | "DAYOFWEEK"
        | "DAYOFYEAR" | "WEEKDAY" | "QUARTER" | "MICROSECOND" => match to_ts(&arg(0)) {
            Some(t) => Value::Int(field(name, t)),
            None => Value::Null,
        },
        "EXTRACT" => {
            need(2)?;
            let unit = text(0).unwrap_or_default();
            match to_ts(&arg(1)) {
                Some(t) => Value::Int(field(&unit, t)),
                None => Value::Null,
            }
        }
        "LAST_DAY" => match to_ts(&arg(0)) {
            Some(t) => {
                let (y, m, _) = ymd_from_date(t.div_euclid(USECS_PER_DAY) as i32);
                Value::Date(date_from_ymd(y, m, days_in_month(y, m)))
            }
            None => Value::Null,
        },
        "DATE_FORMAT" => {
            need(2)?;
            let (Some(t), Some(fmt)) = (to_ts(&arg(0)), text(1)) else { return Ok(Value::Null) };
            Value::Text(date_format(t, &fmt))
        }
        "DATE_ADD" | "DATE_SUB" => {
            need(3)?;
            let unit = text(2).unwrap_or_default();
            let Some(t) = to_ts(&a[0]) else { return Ok(Value::Null) };
            let n = match &a[1] {
                Value::Null => return Ok(Value::Null),
                Value::Text(s) => s
                    .trim()
                    .parse::<f64>()
                    .map_err(|_| MySqlError::unsupported(&format!("INTERVAL '{s}' {unit}")))?,
                v => value_to_f64(v),
            };
            let n = if name == "DATE_SUB" { -n } else { n };
            let out = add_interval(t, n, &unit)?;
            // A DATE plus whole days/months/years stays a DATE.
            let date_only = is_date_only(&a[0])
                && matches!(unit.as_str(), "DAY" | "WEEK" | "MONTH" | "QUARTER" | "YEAR");
            if date_only {
                Value::Date(out.div_euclid(USECS_PER_DAY) as i32)
            } else {
                Value::Ts(out)
            }
        }
        "TIMESTAMPADD" => {
            need(3)?;
            return eval("DATE_ADD", &[arg(2), arg(1), arg(0)]);
        }
        "TIMESTAMPDIFF" => {
            need(3)?;
            let unit = text(0).unwrap_or_default();
            let (Some(x), Some(y)) = (to_ts(&a[1]), to_ts(&a[2])) else { return Ok(Value::Null) };
            Value::Int(timestamp_diff(&unit, x, y)?)
        }
        "DATEDIFF" => {
            need(2)?;
            let (Some(x), Some(y)) = (to_ts(&a[0]), to_ts(&a[1])) else { return Ok(Value::Null) };
            Value::Int(x.div_euclid(USECS_PER_DAY) - y.div_euclid(USECS_PER_DAY))
        }
        "UNIX_TIMESTAMP" => {
            let t = if a.is_empty() {
                crate::mysql::exec::now_ts()
            } else {
                match to_ts(&a[0]) {
                    Some(t) => t,
                    None => return Ok(Value::Null),
                }
            };
            Value::Int(t.div_euclid(USECS_PER_SEC) + UNIX_EPOCH_SECS)
        }
        "FROM_UNIXTIME" => {
            need(1)?;
            if any_null(1) {
                return Ok(Value::Null);
            }
            let secs = value_to_f64(&a[0]);
            let t = ((secs - UNIX_EPOCH_SECS as f64) * USECS_PER_SEC as f64).round() as i64;
            match a.get(1) {
                Some(f) => match value_as_text(f) {
                    Some(fmt) => Value::Text(date_format(t, &fmt)),
                    None => Value::Null,
                },
                None => Value::Ts(t),
            }
        }
        "UTC_TIMESTAMP" => Value::Ts(crate::mysql::exec::now_ts()),
        "UTC_DATE" => Value::Date(crate::mysql::exec::now_ts().div_euclid(USECS_PER_DAY) as i32),

        // ---- CAST ----
        "CAST" => {
            need(2)?;
            cast(arg(0), &text(1).unwrap_or_default())?
        }

        // ---- JSON ----
        "JSON_EXTRACT" => {
            need(2)?;
            if any_null(a.len()) {
                return Ok(Value::Null);
            }
            let doc = to_json(&a[0], name)?;
            let mut found = Vec::new();
            for p in &a[1..] {
                let path = parse_path(&render_text(p))?;
                if let Some(j) = walk(&doc, &path) {
                    found.push(j.clone());
                }
            }
            match (a.len(), found.len()) {
                (_, 0) => Value::Null,
                (2, _) => Value::Json(Box::new(found.remove(0))),
                _ => Value::Json(Box::new(Json::Array(found))),
            }
        }
        "JSON_UNQUOTE" => match arg(0) {
            Value::Null => Value::Null,
            Value::Json(j) => match *j {
                Json::Str(s) => Value::Text(s),
                other => Value::Text(other.to_jsonb_string()),
            },
            v => {
                let s = render_text(&v);
                match json::parse(&s) {
                    Ok(Json::Str(inner)) if s.starts_with('"') => Value::Text(inner),
                    _ => Value::Text(s),
                }
            }
        },
        "JSON_OBJECT" => {
            if !a.len().is_multiple_of(2) {
                return Err(MySqlError::new(
                    1582,
                    "42000",
                    "Incorrect parameter count in the call to native function 'JSON_OBJECT'",
                ));
            }
            let mut obj = Vec::new();
            for pair in a.chunks(2) {
                let k = value_as_text(&pair[0]).ok_or_else(|| {
                    MySqlError::new(
                        3158,
                        "22032",
                        "JSON documents may not contain NULL member names.",
                    )
                })?;
                obj.push((k, value_to_json(&pair[1])));
            }
            Value::Json(Box::new(Json::Object(obj).normalize()))
        }
        "JSON_ARRAY" => Value::Json(Box::new(Json::Array(a.iter().map(value_to_json).collect()))),
        "JSON_VALID" => match arg(0) {
            Value::Null => Value::Null,
            Value::Json(_) => Value::Int(1),
            v => Value::Int(json::parse(&render_text(&v)).is_ok() as i64),
        },
        "JSON_TYPE" => {
            if any_null(1) {
                return Ok(Value::Null);
            }
            let j = to_json(&arg(0), name)?;
            Value::Text(
                match &j {
                    Json::Null => "NULL",
                    Json::Bool(_) => "BOOLEAN",
                    Json::Num(n) if n.scale() == 0 => "INTEGER",
                    Json::Num(_) => "DOUBLE",
                    Json::Str(_) => "STRING",
                    Json::Array(_) => "ARRAY",
                    Json::Object(_) => "OBJECT",
                }
                .to_string(),
            )
        }
        "JSON_LENGTH" => {
            if any_null(a.len().max(1)) {
                return Ok(Value::Null);
            }
            let doc = to_json(&arg(0), name)?;
            let target = match a.get(1) {
                Some(p) => match walk(&doc, &parse_path(&render_text(p))?) {
                    Some(j) => j.clone(),
                    None => return Ok(Value::Null),
                },
                None => doc,
            };
            Value::Int(match target {
                Json::Array(v) => v.len() as i64,
                Json::Object(v) => v.len() as i64,
                _ => 1,
            })
        }
        // `JSON_CONTAINS(target, candidate[, path])`: MySQL's containment
        // (objects by key, arrays by element), the same rule as jsonb `@>`.
        "JSON_CONTAINS" => {
            need(2)?;
            if any_null(a.len()) {
                return Ok(Value::Null);
            }
            let mut target = to_json(&a[0], name)?;
            if let Some(p) = a.get(2) {
                match walk(&target, &parse_path(&render_text(p))?) {
                    Some(j) => target = j.clone(),
                    None => return Ok(Value::Null),
                }
            }
            let candidate = to_json(&a[1], name)?;
            Value::Int(i64::from(target.contains(&candidate)))
        }
        "JSON_CONTAINS_PATH" => {
            need(3)?;
            if any_null(a.len()) {
                return Ok(Value::Null);
            }
            let doc = to_json(&a[0], name)?;
            let all = render_text(&a[1]).eq_ignore_ascii_case("all");
            let mut found = Vec::new();
            for p in &a[2..] {
                found.push(walk(&doc, &parse_path(&render_text(p))?).is_some());
            }
            Value::Int(i64::from(if all {
                found.iter().all(|f| *f)
            } else {
                found.iter().any(|f| *f)
            }))
        }
        "JSON_KEYS" => {
            need(1)?;
            if any_null(a.len()) {
                return Ok(Value::Null);
            }
            let mut doc = to_json(&a[0], name)?;
            if let Some(p) = a.get(1) {
                match walk(&doc, &parse_path(&render_text(p))?) {
                    Some(j) => doc = j.clone(),
                    None => return Ok(Value::Null),
                }
            }
            match doc {
                Json::Object(m) => Value::Json(Box::new(Json::Array(
                    m.into_iter().map(|(k, _)| Json::Str(k)).collect(),
                ))),
                _ => Value::Null,
            }
        }
        // `CONVERT_TZ(dt, from, to)`, with offsets (`'+05:30'`), `UTC`/
        // `SYSTEM` (this server's time zone is UTC) or IANA names. Django
        // probes it on connect when USE_TZ is on.
        "CONVERT_TZ" => {
            need(3)?;
            let (Some(t), Some(from), Some(to)) = (to_ts(&a[0]), text(1), text(2)) else {
                return Ok(Value::Null);
            };
            let zone = |n: &str| {
                if n.eq_ignore_ascii_case("system") {
                    Some(crate::sql::tz::Zone::utc())
                } else {
                    crate::sql::tz::lookup(n)
                }
            };
            let (Some(zf), Some(zt)) = (zone(&from), zone(&to)) else { return Ok(Value::Null) };
            let local_unix = t.div_euclid(USECS_PER_SEC) + UNIX_EPOCH_SECS;
            let from_off = zf.offset_for_local(local_unix) as i64;
            let utc_unix = local_unix - from_off;
            let shift = (zt.offset_at_utc(utc_unix) as i64 - from_off) * USECS_PER_SEC;
            Value::Ts(t + shift)
        }
        "INTERVAL" => {
            return Err(MySqlError::unsupported("INTERVAL outside DATE_ADD/DATE_SUB or +/-"));
        }
        _ => return Err(MySqlError::unsupported(&format!("function {name}"))),
    })
}

fn out_of_range(name: &str) -> MySqlError {
    MySqlError::new(1690, "22003", format!("BIGINT value is out of range in '{name}'"))
}

/// MySQL truthiness: non-NULL and numerically non-zero (`'abc'` is 0).
pub(crate) fn truthy(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Int(i) => *i != 0,
        Value::Bool(b) => *b,
        Value::Num(n) => n.to_f64() != 0.0,
        Value::Text(s) => leading_f64(s) != 0.0,
        v => value_to_f64(v) != 0.0,
    }
}

/// The longest numeric prefix of a string, as MySQL reads `'12abc'` (12).
pub(crate) fn leading_f64(s: &str) -> f64 {
    let s = s.trim_start();
    let mut end = 0;
    for (i, c) in s.char_indices() {
        let ok = c.is_ascii_digit()
            || (i == 0 && (c == '-' || c == '+'))
            || c == '.'
            || c == 'e'
            || c == 'E';
        if !ok {
            break;
        }
        if s[..i + c.len_utf8()].parse::<f64>().is_ok() {
            end = i + c.len_utf8();
        }
    }
    s[..end].parse().unwrap_or(0.0)
}

fn int_or_num(n: Numeric) -> Value {
    match n.to_i64() {
        Some(i) => Value::Int(i),
        None => Value::Num(n),
    }
}

/// Rounds half away from zero (MySQL's rule for approximate values too).
fn round_f64(f: f64, d: i64, trunc: bool) -> f64 {
    let m = 10f64.powi(d.clamp(-30, 30) as i32);
    let x = f * m;
    (if trunc { x.trunc() } else { x.round() }) / m
}

fn locate_ci(needle: &str, hay: &str, start: i64) -> i64 {
    let n: Vec<char> = needle.to_lowercase().chars().collect();
    let h: Vec<char> = hay.to_lowercase().chars().collect();
    if start < 1 {
        return 0;
    }
    let from = (start - 1) as usize;
    if n.is_empty() {
        return if from <= h.len() { start } else { 0 };
    }
    (from..h.len().saturating_sub(n.len() - 1).max(from))
        .find(|&i| i + n.len() <= h.len() && h[i..i + n.len()] == n[..])
        .map(|i| i as i64 + 1)
        .unwrap_or(0)
}

/// A date/time value as microseconds since 2000-01-01: DATETIME/DATE
/// values, or text in any format MySQL accepts for them.
fn to_ts(v: &Value) -> Option<i64> {
    match v {
        Value::Ts(t) => Some(*t),
        Value::Date(d) => Some(*d as i64 * USECS_PER_DAY),
        Value::Text(s) => parse_mysql_datetime(s)
            .or_else(|| parse_mysql_date(s).map(|d| d as i64 * USECS_PER_DAY)),
        _ => None,
    }
}

fn is_date_only(v: &Value) -> bool {
    match v {
        Value::Date(_) => true,
        Value::Text(s) => s.trim().len() <= 10 && parse_mysql_date(s).is_some(),
        _ => false,
    }
}

fn field(unit: &str, t: i64) -> i64 {
    let days = t.div_euclid(USECS_PER_DAY);
    let us = t.rem_euclid(USECS_PER_DAY);
    let (y, m, d) = ymd_from_date(days as i32);
    // 2000-01-01 was a Saturday: 0 = Sunday.
    let dow = (days + 6).rem_euclid(7);
    match unit {
        "YEAR" => y,
        "MONTH" => m as i64,
        "QUARTER" => (m as i64 - 1) / 3 + 1,
        "DAY" | "DAYOFMONTH" => d as i64,
        "HOUR" => us / (3600 * USECS_PER_SEC),
        "MINUTE" => us / (60 * USECS_PER_SEC) % 60,
        "SECOND" => us / USECS_PER_SEC % 60,
        "MICROSECOND" => us % USECS_PER_SEC,
        "DAYOFWEEK" => dow + 1,
        "WEEKDAY" => (dow + 6) % 7,
        "DAYOFYEAR" => days - date_from_ymd(y, 1, 1) as i64 + 1,
        _ => 0,
    }
}

const MONTHS: [&str; 12] = [
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
const WEEKDAYS: [&str; 7] =
    ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];

/// `DATE_FORMAT`'s specifiers (the ones apps use; an unknown `%x` prints `x`,
/// as MySQL does).
fn date_format(t: i64, fmt: &str) -> String {
    let (y, m, d) = ymd_from_date(t.div_euclid(USECS_PER_DAY) as i32);
    let us = t.rem_euclid(USECS_PER_DAY);
    let (h, mi, s) = (us / 3_600_000_000, us / 60_000_000 % 60, us / 1_000_000 % 60);
    let h12 = if h % 12 == 0 { 12 } else { h % 12 };
    let ampm = if h < 12 { "AM" } else { "PM" };
    let dow = field("DAYOFWEEK", t) as usize - 1;
    let mut out = String::new();
    let mut chars = fmt.chars();
    while let Some(c) = chars.next() {
        if c != '%' {
            out.push(c);
            continue;
        }
        let Some(spec) = chars.next() else { break };
        match spec {
            'Y' => out.push_str(&format!("{y:04}")),
            'y' => out.push_str(&format!("{:02}", y.rem_euclid(100))),
            'm' => out.push_str(&format!("{m:02}")),
            'c' => out.push_str(&m.to_string()),
            'M' => out.push_str(MONTHS[m as usize - 1]),
            'b' => out.push_str(&MONTHS[m as usize - 1][..3]),
            'd' => out.push_str(&format!("{d:02}")),
            'e' => out.push_str(&d.to_string()),
            'D' => {
                let suffix = match (d % 10, d % 100) {
                    (_, 11..=13) => "th",
                    (1, _) => "st",
                    (2, _) => "nd",
                    (3, _) => "rd",
                    _ => "th",
                };
                out.push_str(&format!("{d}{suffix}"));
            }
            'j' => out.push_str(&format!("{:03}", field("DAYOFYEAR", t))),
            'W' => out.push_str(WEEKDAYS[dow]),
            'a' => out.push_str(&WEEKDAYS[dow][..3]),
            'w' => out.push_str(&dow.to_string()),
            'H' => out.push_str(&format!("{h:02}")),
            'k' => out.push_str(&h.to_string()),
            'h' | 'I' => out.push_str(&format!("{h12:02}")),
            'l' => out.push_str(&h12.to_string()),
            'i' => out.push_str(&format!("{mi:02}")),
            's' | 'S' => out.push_str(&format!("{s:02}")),
            'f' => out.push_str(&format!("{:06}", us % 1_000_000)),
            'p' => out.push_str(ampm),
            'T' => out.push_str(&format!("{h:02}:{mi:02}:{s:02}")),
            'r' => out.push_str(&format!("{h12:02}:{mi:02}:{s:02} {ampm}")),
            other => out.push(other),
        }
    }
    out
}

fn unit_micros(unit: &str) -> Option<i64> {
    Some(match unit {
        "MICROSECOND" => 1,
        "SECOND" => USECS_PER_SEC,
        "MINUTE" => 60 * USECS_PER_SEC,
        "HOUR" => 3600 * USECS_PER_SEC,
        "DAY" => USECS_PER_DAY,
        "WEEK" => 7 * USECS_PER_DAY,
        _ => return None,
    })
}

fn add_interval(t: i64, n: f64, unit: &str) -> Result<i64, MySqlError> {
    if let Some(u) = unit_micros(unit) {
        return Ok(t + (n * u as f64).round() as i64);
    }
    let months = match unit {
        "MONTH" => n as i64,
        "QUARTER" => n as i64 * 3,
        "YEAR" => n as i64 * 12,
        _ => return Err(MySqlError::unsupported(&format!("INTERVAL unit {unit}"))),
    };
    let days = t.div_euclid(USECS_PER_DAY);
    let us = t.rem_euclid(USECS_PER_DAY);
    let (y, m, d) = ymd_from_date(days as i32);
    let total = y * 12 + (m as i64 - 1) + months;
    let (ny, nm) = (total.div_euclid(12), (total.rem_euclid(12) + 1) as u32);
    // Jan 31 + 1 MONTH is Feb 28/29, as in MySQL.
    let nd = d.min(days_in_month(ny, nm));
    Ok(date_from_ymd(ny, nm, nd) as i64 * USECS_PER_DAY + us)
}

fn timestamp_diff(unit: &str, a: i64, b: i64) -> Result<i64, MySqlError> {
    if let Some(u) = unit_micros(unit) {
        return Ok((b - a) / u);
    }
    let per = match unit {
        "MONTH" => 1,
        "QUARTER" => 3,
        "YEAR" => 12,
        _ => return Err(MySqlError::unsupported(&format!("TIMESTAMPDIFF unit {unit}"))),
    };
    let parts = |t: i64| {
        let (y, m, d) = ymd_from_date(t.div_euclid(USECS_PER_DAY) as i32);
        (y * 12 + m as i64, (d as i64) * USECS_PER_DAY + t.rem_euclid(USECS_PER_DAY))
    };
    let ((ma, ra), (mb, rb)) = (parts(a), parts(b));
    let mut months = mb - ma;
    // Only whole months count: Jan 31 -> Feb 28 is 0 months.
    if months > 0 && rb < ra {
        months -= 1;
    } else if months < 0 && rb > ra {
        months += 1;
    }
    Ok(months / per)
}

fn cast(v: Value, ty: &str) -> Result<Value, MySqlError> {
    if v.is_null() {
        return Ok(Value::Null);
    }
    let base = ty.split('(').next().unwrap_or("").trim();
    let args: Vec<i64> = ty
        .split_once('(')
        .map(|(_, r)| {
            r.trim_end_matches(')').split(',').filter_map(|x| x.trim().parse().ok()).collect()
        })
        .unwrap_or_default();
    Ok(match base {
        "SIGNED" | "SIGNED INTEGER" | "UNSIGNED" | "UNSIGNED INTEGER" | "INT" | "INTEGER"
        | "BIGINT" => match &v {
            Value::Int(_) => v,
            Value::Text(s) => Value::Int(leading_f64(s).trunc() as i64),
            other => Value::Int(value_to_f64(other).round() as i64),
        },
        "CHAR" | "VARCHAR" | "NCHAR" | "TEXT" | "BINARY" => {
            let s = render_text(&v);
            match args.first() {
                Some(&n) if n >= 0 => Value::Text(s.chars().take(n as usize).collect()),
                _ => Value::Text(s),
            }
        }
        "DECIMAL" | "NUMERIC" | "DEC" => {
            let scale = args.get(1).copied().unwrap_or(0);
            let n = match &v {
                Value::Text(s) => {
                    Numeric::parse(s.trim()).unwrap_or_else(|_| Numeric::from_f64(leading_f64(s)))
                }
                other => value_to_numeric(other).unwrap_or_else(Numeric::zero),
            };
            Value::Num(n.round(scale))
        }
        "DOUBLE" | "FLOAT" | "REAL" => match &v {
            Value::Text(s) => Value::Float(leading_f64(s)),
            other => Value::Float(value_to_f64(other)),
        },
        "DATE" => match to_ts(&v) {
            Some(t) => Value::Date(t.div_euclid(USECS_PER_DAY) as i32),
            None => Value::Null,
        },
        "DATETIME" | "TIMESTAMP" => match to_ts(&v) {
            Some(t) => Value::Ts(t),
            None => Value::Null,
        },
        "TIME" => match &v {
            Value::Text(s) => match crate::sql::datetime::parse_time(s.trim()) {
                Ok(us) => Value::Time(us),
                Err(_) => to_ts(&v)
                    .map(|t| Value::Time(t.rem_euclid(USECS_PER_DAY)))
                    .unwrap_or(Value::Null),
            },
            other => to_ts(other)
                .map(|t| Value::Time(t.rem_euclid(USECS_PER_DAY)))
                .unwrap_or(Value::Null),
        },
        "JSON" => Value::Json(Box::new(match &v {
            Value::Text(s) => json::parse_jsonb(s).map_err(|_| invalid_json("CAST"))?,
            other => value_to_json(other),
        })),
        _ => return Err(MySqlError::unsupported(&format!("CAST AS {ty}"))),
    })
}

fn invalid_json(func: &str) -> MySqlError {
    MySqlError::new(
        3141,
        "22032",
        format!(
            "Invalid JSON text in argument 1 to function {}: \"Invalid value.\" at position 0.",
            func.to_lowercase()
        ),
    )
}

fn to_json(v: &Value, func: &str) -> Result<Json, MySqlError> {
    match v {
        Value::Json(j) => Ok((**j).clone()),
        other => json::parse_jsonb(&render_text(other)).map_err(|_| invalid_json(func)),
    }
}

fn value_to_json(v: &Value) -> Json {
    match v {
        Value::Null => Json::Null,
        Value::Bool(b) => Json::Bool(*b),
        Value::Int(i) => Json::Num(Numeric::from_i64(*i)),
        Value::Num(n) => Json::Num(n.clone()),
        Value::Float(f) => Json::Num(Numeric::from_f64(*f)),
        Value::Json(j) => (**j).clone(),
        other => Json::Str(render_text(other)),
    }
}

enum Step {
    Key(String),
    Index(usize),
}

/// A MySQL JSON path: `$`, `$.a.b`, `$."a key"`, `$[0]`, `$.a[2].b`.
/// Wildcards (`*`, `**`) are rejected loudly rather than matched wrongly.
fn parse_path(p: &str) -> Result<Vec<Step>, MySqlError> {
    let bad = || {
        MySqlError::new(
            3143,
            "42000",
            format!(
                "Invalid JSON path expression. The error is around character position 0 in '{p}'."
            ),
        )
    };
    let s = p.trim();
    let mut rest = s.strip_prefix('$').ok_or_else(bad)?;
    let mut out = Vec::new();
    while !rest.is_empty() {
        if let Some(r) = rest.strip_prefix('.') {
            if let Some(r) = r.strip_prefix('"') {
                let end = r.find('"').ok_or_else(bad)?;
                out.push(Step::Key(r[..end].to_string()));
                rest = &r[end + 1..];
            } else {
                let end = r.find(['.', '[']).unwrap_or(r.len());
                let key = &r[..end];
                if key.is_empty() || key.contains('*') {
                    return Err(bad());
                }
                out.push(Step::Key(key.to_string()));
                rest = &r[end..];
            }
        } else if let Some(r) = rest.strip_prefix('[') {
            let end = r.find(']').ok_or_else(bad)?;
            let i = r[..end].trim().parse::<usize>().map_err(|_| bad())?;
            out.push(Step::Index(i));
            rest = &r[end + 1..];
        } else {
            return Err(bad());
        }
    }
    Ok(out)
}

fn walk<'a>(doc: &'a Json, path: &[Step]) -> Option<&'a Json> {
    let mut cur = doc;
    for step in path {
        cur = match (step, cur) {
            (Step::Key(k), Json::Object(_)) => cur.get(k)?,
            (Step::Index(i), Json::Array(v)) => v.get(*i)?,
            // MySQL: `$[0]` on a non-array is the value itself.
            (Step::Index(0), other) => other,
            _ => return None,
        };
    }
    Some(cur)
}
