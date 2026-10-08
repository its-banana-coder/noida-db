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
        // ---- bit operators (unsigned 64-bit, as in MySQL) ----
        "&" | "|" | "^" | "<<" | ">>" => {
            need(2)?;
            if any_null(2) {
                return Ok(Value::Null);
            }
            let (x, y) = (to_u64(&a[0]), to_u64(&a[1]));
            from_u64(match name {
                "&" => x & y,
                "|" => x | y,
                "^" => x ^ y,
                "<<" => x.checked_shl(y.min(64) as u32).unwrap_or(0),
                _ => x.checked_shr(y.min(64) as u32).unwrap_or(0),
            })
        }
        "~" => {
            need(1)?;
            if any_null(1) {
                return Ok(Value::Null);
            }
            from_u64(!to_u64(&a[0]))
        }
        "XOR" => {
            need(2)?;
            if any_null(2) {
                return Ok(Value::Null);
            }
            Value::Int(i64::from(truthy(&a[0]) != truthy(&a[1])))
        }

        // ---- floating-point math (NULL outside the domain) ----
        "PI" => Value::Float(std::f64::consts::PI),
        "ACOS" | "ASIN" | "ATAN" | "ATAN2" | "COS" | "COT" | "SIN" | "TAN" | "DEGREES"
        | "RADIANS" | "EXP" | "LN" | "LOG" | "LOG2" | "LOG10" => {
            need(1)?;
            let two = a.len() == 2 && matches!(name, "ATAN" | "ATAN2" | "LOG");
            if a.len() > 1 + usize::from(two) || (name == "ATAN2" && !two) {
                return Err(MySqlError::new(
                    1582,
                    "42000",
                    format!("Incorrect parameter count in the call to native function '{name}'"),
                ));
            }
            if any_null(a.len()) {
                return Ok(Value::Null);
            }
            let x = value_to_f64(&a[0]);
            let r = if two {
                let y = value_to_f64(&a[1]);
                match name {
                    // LOG(b, x): log of x in base b.
                    "LOG" => {
                        if x <= 0.0 || x == 1.0 || y <= 0.0 {
                            return Ok(Value::Null);
                        }
                        y.ln() / x.ln()
                    }
                    _ => x.atan2(y),
                }
            } else {
                match name {
                    "ACOS" | "ASIN" if !(-1.0..=1.0).contains(&x) => return Ok(Value::Null),
                    "ACOS" => x.acos(),
                    "ASIN" => x.asin(),
                    "ATAN" => x.atan(),
                    "COS" => x.cos(),
                    "SIN" => x.sin(),
                    "TAN" => x.tan(),
                    "COT" => {
                        let t = x.tan();
                        if t == 0.0 {
                            return Err(MySqlError::new(
                                1690,
                                "22003",
                                format!(
                                    "DOUBLE value is out of range in 'cot({})'",
                                    render_text(&a[0])
                                ),
                            ));
                        }
                        1.0 / t
                    }
                    "DEGREES" => x.to_degrees(),
                    "RADIANS" => x.to_radians(),
                    "EXP" => {
                        let r = x.exp();
                        if r.is_infinite() {
                            return Err(MySqlError::new(
                                1690,
                                "22003",
                                format!(
                                    "DOUBLE value is out of range in 'exp({})'",
                                    render_text(&a[0])
                                ),
                            ));
                        }
                        r
                    }
                    _ if x <= 0.0 => return Ok(Value::Null),
                    "LN" | "LOG" => x.ln(),
                    "LOG2" => x.log2(),
                    _ => x.log10(),
                }
            };
            Value::Float(r)
        }
        // RAND(): uniform in [0, 1). RAND(n) uses MySQL's own generator,
        // so it gives the same first value MySQL does for that seed.
        "RAND" => {
            let seed = match a.first() {
                Some(v) if !v.is_null() => Some(value_to_f64(v) as i64 as u64),
                Some(_) => Some(0),
                None => None,
            };
            Value::Float(match seed {
                Some(s) => {
                    const MAX: u64 = 0x3FFF_FFFF;
                    // MySQL seeds in 32-bit arithmetic.
                    let s = s as u32;
                    let mut s1 = u64::from(s.wrapping_mul(0x10001).wrapping_add(55_555_555)) % MAX;
                    let s2 = u64::from(s.wrapping_mul(0x1000_0001)) % MAX;
                    s1 = (s1 * 3 + s2) % MAX;
                    s1 as f64 / MAX as f64
                }
                None => random_f64(),
            })
        }
        "ORD" => {
            need(1)?;
            match &a[0] {
                Value::Null => Value::Null,
                v => {
                    let bytes: Vec<u8> = match v {
                        Value::Bytes(b) => b.first().map(|b| vec![*b]).unwrap_or_default(),
                        v => render_text(v)
                            .chars()
                            .next()
                            .map(|c| c.to_string().into_bytes())
                            .unwrap_or_default(),
                    };
                    Value::Int(bytes.iter().fold(0i64, |acc, b| acc * 256 + i64::from(*b)))
                }
            }
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

        // ---- IP addresses ----
        "INET_ATON" | "INET_NTOA" | "INET6_ATON" | "INET6_NTOA" | "IS_IPV4" | "IS_IPV6"
        | "IS_IPV4_MAPPED" | "IS_IPV4_COMPAT" => {
            need(1)?;
            if any_null(1) {
                return Ok(Value::Null);
            }
            let txt = || render_text(&a[0]);
            let bytes = || match &a[0] {
                Value::Bytes(b) => b.clone(),
                v => render_text(v).into_bytes(),
            };
            match name {
                "INET_ATON" => inet_aton(&txt()).map_or(Value::Null, |n| Value::Int(n as i64)),
                "INET_NTOA" => {
                    let n = value_to_f64(&a[0]);
                    if !(0.0..=u32::MAX as f64).contains(&n) {
                        Value::Null
                    } else {
                        Value::Text(std::net::Ipv4Addr::from(n as u32).to_string())
                    }
                }
                "INET6_ATON" => {
                    let t = txt();
                    if let Ok(v4) = t.parse::<std::net::Ipv4Addr>() {
                        Value::Bytes(v4.octets().to_vec())
                    } else if let Ok(v6) = t.parse::<std::net::Ipv6Addr>() {
                        Value::Bytes(v6.octets().to_vec())
                    } else {
                        Value::Null
                    }
                }
                "INET6_NTOA" => {
                    let b = bytes();
                    match b.len() {
                        4 => {
                            Value::Text(std::net::Ipv4Addr::new(b[0], b[1], b[2], b[3]).to_string())
                        }
                        16 => {
                            let arr: [u8; 16] = b.as_slice().try_into().unwrap();
                            Value::Text(ipv6_text(&arr))
                        }
                        _ => Value::Null,
                    }
                }
                "IS_IPV4" => Value::Int(txt().parse::<std::net::Ipv4Addr>().is_ok() as i64),
                "IS_IPV6" => Value::Int(txt().parse::<std::net::Ipv6Addr>().is_ok() as i64),
                _ => {
                    let b = bytes();
                    let ok = b.len() == 16
                        && b[..10].iter().all(|x| *x == 0)
                        && if name == "IS_IPV4_MAPPED" {
                            b[10] == 0xff && b[11] == 0xff
                        } else {
                            b[10] == 0 && b[11] == 0
                        };
                    Value::Int(ok as i64)
                }
            }
        }

        // ---- hashing and encoding ----
        "MD5" | "SHA" | "SHA1" | "SHA2" | "CRC32" | "TO_BASE64" | "FROM_BASE64" | "UNHEX" => {
            let want = if name == "SHA2" { 2 } else { 1 };
            if a.len() != want {
                return Err(MySqlError::new(
                    1582,
                    "42000",
                    format!("Incorrect parameter count in the call to native function '{name}'"),
                ));
            }
            if any_null(want) {
                return Ok(Value::Null);
            }
            let data = match &a[0] {
                Value::Bytes(b) => b.clone(),
                v => render_text(v).into_bytes(),
            };
            use crate::sql::hash;
            match name {
                "MD5" => Value::Text(hash::hex(&hash::digest("md5", &data).unwrap())),
                "SHA" | "SHA1" => Value::Text(hash::hex(&hash::digest("sha1", &data).unwrap())),
                "SHA2" => {
                    let alg = match value_to_f64(&a[1]) as i64 {
                        0 | 256 => "sha256",
                        224 => "sha224",
                        384 => "sha384",
                        512 => "sha512",
                        _ => return Ok(Value::Null),
                    };
                    Value::Text(hash::hex(&hash::digest(alg, &data).unwrap()))
                }
                "CRC32" => Value::Int(crc32(&data) as i64),
                "TO_BASE64" => Value::Text(hash::base64_encode(&data)),
                "FROM_BASE64" => {
                    match std::str::from_utf8(&data).ok().and_then(hash::base64_decode) {
                        Some(b) => Value::Bytes(b),
                        None => Value::Null,
                    }
                }
                _ => {
                    // UNHEX: an odd length gets a leading 0; a non-hex digit is NULL.
                    let s = String::from_utf8_lossy(&data);
                    let s = if s.len() % 2 == 1 { format!("0{s}") } else { s.into_owned() };
                    let out: Option<Vec<u8>> = (0..s.len())
                        .step_by(2)
                        .map(|i| s.get(i..i + 2).and_then(|h| u8::from_str_radix(h, 16).ok()))
                        .collect();
                    out.map_or(Value::Null, Value::Bytes)
                }
            }
        }

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
        "EXTRACT" | "NOIDA_EXTRACT" => {
            need(2)?;
            let unit = text(0).unwrap_or_default();
            match to_ts(&arg(1)) {
                Some(t) => Value::Int(field(&unit, t)),
                None => Value::Null,
            }
        }
        // WEEK(d[, mode]) / YEARWEEK(d[, mode]) / WEEKOFYEAR(d): MySQL's
        // own week numbering (`calc_week`).
        "WEEK" | "YEARWEEK" | "WEEKOFYEAR" => {
            need(1)?;
            let Some(t) = to_ts(&arg(0)) else { return Ok(Value::Null) };
            let mode = match name {
                "WEEKOFYEAR" => 3,
                _ => match a.get(1) {
                    Some(Value::Null) => return Ok(Value::Null),
                    Some(v) => value_to_f64(v) as i64,
                    None => 0,
                },
            };
            let mut behaviour = week_mode(mode);
            if name == "YEARWEEK" {
                behaviour |= WEEK_YEAR;
            }
            let (year, week) = calc_week(t.div_euclid(USECS_PER_DAY) as i32, behaviour);
            Value::Int(if name == "YEARWEEK" { year * 100 + week } else { week })
        }
        // MAKEDATE(year, dayofyear).
        "MAKEDATE" => {
            need(2)?;
            let (Some(y), Some(n)) = (int(0), int(1)) else { return Ok(Value::Null) };
            let y = match y {
                0..=69 => y + 2000,
                70..=99 => y + 1900,
                _ => y,
            };
            if n <= 0 || !(0..=9999).contains(&y) {
                return Ok(Value::Null);
            }
            let d = date_from_ymd(y, 1, 1) as i64 + n - 1;
            if ymd_from_date(d as i32).0 > 9999 {
                return Ok(Value::Null);
            }
            Value::Date(d as i32)
        }
        "TIME_TO_SEC" => {
            need(1)?;
            let us = match &a[0] {
                Value::Null => return Ok(Value::Null),
                Value::Time(t) => *t,
                Value::Ts(t) => t.rem_euclid(USECS_PER_DAY),
                v => match crate::mysql::exec::parse_mysql_time(&render_text(v)) {
                    Some(t) => t,
                    None => return Ok(Value::Null),
                },
            };
            // Whole seconds: a fraction is dropped.
            Value::Int(us / USECS_PER_SEC)
        }
        "SEC_TO_TIME" => {
            need(1)?;
            if any_null(1) {
                return Ok(Value::Null);
            }
            let us = (value_to_f64(&a[0]) * 1e6).round() as i64;
            let max = crate::mysql::exec::MAX_MYSQL_TIME;
            Value::Time(us.clamp(-max, max))
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
            let mut wild = false;
            for p in &a[1..] {
                let path = parse_path(&render_text(p))?;
                wild |= has_wildcard(&path);
                walk_all(&doc, &path, &mut found);
            }
            // One plain path returns its value; wildcards or several paths
            // return an array of every match.
            match (a.len() == 2 && !wild, found.len()) {
                (_, 0) => Value::Null,
                (true, _) => Value::Json(Box::new(found.remove(0).clone())),
                (false, _) => {
                    Value::Json(Box::new(Json::Array(found.into_iter().cloned().collect())))
                }
            }
        }
        "JSON_SET" | "JSON_INSERT" | "JSON_REPLACE" | "JSON_ARRAY_APPEND" | "JSON_ARRAY_INSERT" => {
            if a.len() < 3 || a.len().is_multiple_of(2) {
                return Err(MySqlError::new(
                    1582,
                    "42000",
                    format!(
                        "Incorrect parameter count in the call to native function '{}'",
                        name.to_lowercase()
                    ),
                ));
            }
            if a[0].is_null() {
                return Ok(Value::Null);
            }
            let mut doc = to_json(&a[0], name)?;
            for pair in a[1..].chunks(2) {
                if pair[0].is_null() {
                    return Ok(Value::Null);
                }
                let path = parse_path(&render_text(&pair[0]))?;
                if has_wildcard(&path) {
                    return Err(no_wildcards());
                }
                json_modify(name, &mut doc, &path, value_to_json(&pair[1]))?;
            }
            Value::Json(Box::new(doc))
        }
        "JSON_REMOVE" => {
            need(2)?;
            if any_null(a.len()) {
                return Ok(Value::Null);
            }
            let mut doc = to_json(&a[0], name)?;
            for p in &a[1..] {
                let path = parse_path(&render_text(p))?;
                if has_wildcard(&path) {
                    return Err(no_wildcards());
                }
                let Some((last, parent)) = path.split_last() else {
                    return Err(MySqlError::new(
                        3153,
                        "42000",
                        "The path expression '$' is not allowed in this context.",
                    ));
                };
                match (walk_mut(&mut doc, parent), last) {
                    (Some(Json::Object(m)), Step::Key(k)) => m.retain(|(x, _)| x != k),
                    (Some(Json::Array(v)), Step::Index(i)) => {
                        if let Some(i) = i.resolve(v.len()).filter(|&i| i < v.len()) {
                            v.remove(i);
                        }
                    }
                    _ => {}
                }
            }
            Value::Json(Box::new(doc))
        }
        "JSON_MERGE_PATCH" | "JSON_MERGE_PRESERVE" | "JSON_MERGE" => {
            need(2)?;
            let mut out: Option<Json> = None;
            for v in a {
                // MERGE_PATCH: a NULL argument makes the result NULL only
                // until a later object replaces it.
                if v.is_null() {
                    if name != "JSON_MERGE_PATCH" {
                        return Ok(Value::Null);
                    }
                    out = None;
                    continue;
                }
                let j = to_json(v, name)?;
                out = Some(match out {
                    None => j,
                    Some(acc) if name == "JSON_MERGE_PATCH" => merge_patch(acc, j),
                    Some(acc) => merge_preserve(acc, j),
                });
            }
            match out {
                Some(j) => Value::Json(Box::new(j)),
                None => Value::Null,
            }
        }
        "JSON_QUOTE" => match arg(0) {
            Value::Null => Value::Null,
            v => Value::Text(Json::Str(render_text(&v)).to_jsonb_string()),
        },
        "JSON_DEPTH" => match arg(0) {
            Value::Null => Value::Null,
            v => Value::Int(json_depth(&to_json(&v, name)?)),
        },
        "JSON_OVERLAPS" => {
            need(2)?;
            if any_null(2) {
                return Ok(Value::Null);
            }
            let (x, y) = (to_json(&a[0], name)?, to_json(&a[1], name)?);
            let items = |j: &Json| match j {
                Json::Array(v) => v.clone(),
                other => vec![other.clone()],
            };
            let hit = match (&x, &y) {
                (Json::Object(m1), Json::Object(m2)) => m1.iter().any(|kv| m2.contains(kv)),
                _ => items(&x).iter().any(|i| items(&y).contains(i)),
            };
            Value::Int(i64::from(hit))
        }
        "JSON_SEARCH" => {
            // JSON_SEARCH(doc, 'one'|'all', search[, escape[, path...]])
            need(3)?;
            if any_null(3) {
                return Ok(Value::Null);
            }
            let doc = to_json(&a[0], name)?;
            let mode = render_text(&a[1]).to_ascii_lowercase();
            if mode != "one" && mode != "all" {
                return Err(MySqlError::new(
                    3154,
                    "42000",
                    "The oneOrAll argument to json_search may take these values: 'one' or 'all'.",
                ));
            }
            let needle = render_text(&a[2]);
            let mut hits = Vec::new();
            search_strings(&doc, "$".to_string(), &needle, &mut hits);
            match (hits.len(), mode.as_str()) {
                (0, _) => Value::Null,
                (_, "one") | (1, _) => Value::Json(Box::new(Json::Str(hits.remove(0)))),
                _ => Value::Json(Box::new(Json::Array(hits.into_iter().map(Json::Str).collect()))),
            }
        }
        "JSON_PRETTY" => match arg(0) {
            Value::Null => Value::Null,
            v => Value::Text(pretty(&to_json(&v, name)?, 0)),
        },
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
        "WEEK" => calc_week(days as i32, week_mode(0)).1,
        // Compound units: the fields' digits run together.
        "YEAR_MONTH" => y * 100 + m as i64,
        _ => compound(unit, d as i64, us).unwrap_or(0),
    }
}

/// `DAY_SECOND`, `HOUR_MICROSECOND`, ...: DDHHMMSS-style integers.
fn compound(unit: &str, day: i64, us: i64) -> Option<i64> {
    let (from, to) = unit.split_once('_')?;
    let parts = ["DAY", "HOUR", "MINUTE", "SECOND", "MICROSECOND"];
    let lo = parts.iter().position(|p| *p == from)?;
    let hi = parts.iter().position(|p| *p == to)?;
    if lo >= hi {
        return None;
    }
    let vals = [
        (day, 100),
        (us / (3600 * USECS_PER_SEC), 100),
        (us / (60 * USECS_PER_SEC) % 60, 100),
        (us / USECS_PER_SEC % 60, 100),
        (us % USECS_PER_SEC, 1_000_000),
    ];
    Some(vals[lo..=hi].iter().fold(0, |acc, (v, width)| acc * width + v))
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
        // A DOUBLE keeps its decimal point in JSON (`2.0`), as in MySQL.
        Value::Float(f) if f.is_finite() && f.fract() == 0.0 && f.abs() < 1e15 => {
            Json::Num(Numeric::parse(&format!("{f:.1}")).unwrap_or_else(|_| Numeric::from_f64(*f)))
        }
        Value::Float(f) => Json::Num(Numeric::from_f64(*f)),
        Value::Json(j) => (**j).clone(),
        other => Json::Str(render_text(other)),
    }
}

enum Step {
    Key(String),
    Index(Idx),
    /// `.*`
    AnyKey,
    /// `[*]`
    AnyIndex,
    /// `**`: any depth (including none).
    Descend,
}

/// An array index: `[3]`, `[last]`, `[last-1]`.
#[derive(Clone, Copy)]
enum Idx {
    At(usize),
    FromLast(usize),
}

impl Idx {
    fn resolve(self, len: usize) -> Option<usize> {
        match self {
            Idx::At(i) => Some(i),
            Idx::FromLast(k) => len.checked_sub(1 + k),
        }
    }
}

fn has_wildcard(path: &[Step]) -> bool {
    path.iter().any(|s| matches!(s, Step::AnyKey | Step::AnyIndex | Step::Descend))
}

fn no_wildcards() -> MySqlError {
    MySqlError::new(
        3149,
        "42000",
        "In this situation, path expressions may not contain the * and ** tokens.",
    )
}

/// Every value `path` matches in `doc`, in document order.
fn walk_all<'a>(doc: &'a Json, path: &[Step], out: &mut Vec<&'a Json>) {
    let Some((step, rest)) = path.split_first() else {
        out.push(doc);
        return;
    };
    match (step, doc) {
        (Step::Key(k), Json::Object(_)) => {
            if let Some(v) = doc.get(k) {
                walk_all(v, rest, out);
            }
        }
        (Step::Index(i), Json::Array(v)) => {
            if let Some(x) = i.resolve(v.len()).and_then(|i| v.get(i)) {
                walk_all(x, rest, out);
            }
        }
        // MySQL: `$[0]` (and `$[last]`) on a non-array is the value itself.
        (Step::Index(Idx::At(0) | Idx::FromLast(0)), other) => walk_all(other, rest, out),
        (Step::AnyKey, Json::Object(m)) => m.iter().for_each(|(_, v)| walk_all(v, rest, out)),
        (Step::AnyIndex, Json::Array(v)) => v.iter().for_each(|x| walk_all(x, rest, out)),
        (Step::Descend, _) => {
            walk_all(doc, rest, out);
            match doc {
                Json::Object(m) => m.iter().for_each(|(_, v)| walk_all(v, path, out)),
                Json::Array(v) => v.iter().for_each(|x| walk_all(x, path, out)),
                _ => {}
            }
        }
        _ => {}
    }
}

fn walk_mut<'a>(doc: &'a mut Json, path: &[Step]) -> Option<&'a mut Json> {
    let mut cur = doc;
    for step in path {
        cur = match (step, cur) {
            (Step::Key(k), Json::Object(m)) => &mut m.iter_mut().find(|(x, _)| x == k)?.1,
            (Step::Index(i), Json::Array(v)) => {
                let i = i.resolve(v.len())?;
                v.get_mut(i)?
            }
            (Step::Index(Idx::At(0) | Idx::FromLast(0)), other) => other,
            _ => return None,
        };
    }
    Some(cur)
}

fn not_a_cell() -> MySqlError {
    MySqlError::new(3165, "42000", "A path expression is not a path to a cell in an array.")
}

/// One JSON_SET/INSERT/REPLACE/ARRAY_APPEND/ARRAY_INSERT path-value pair.
fn json_modify(func: &str, doc: &mut Json, path: &[Step], val: Json) -> Result<(), MySqlError> {
    if func == "JSON_ARRAY_APPEND" {
        if let Some(target) = walk_mut(doc, path) {
            match target {
                Json::Array(v) => v.push(val),
                other => {
                    let old = std::mem::replace(other, Json::Null);
                    *other = Json::Array(vec![old, val]);
                }
            }
        }
        return Ok(());
    }
    let Some((last, parent)) = path.split_last() else {
        // `$` itself: SET and REPLACE replace the document.
        match func {
            "JSON_SET" | "JSON_REPLACE" => *doc = val,
            "JSON_ARRAY_INSERT" => return Err(not_a_cell()),
            _ => {}
        }
        return Ok(());
    };
    if func == "JSON_ARRAY_INSERT" {
        let Step::Index(i) = last else { return Err(not_a_cell()) };
        if let Some(Json::Array(v)) = walk_mut(doc, parent) {
            let at = i.resolve(v.len()).unwrap_or(0).min(v.len());
            v.insert(at, val);
        }
        return Ok(());
    }
    let (set, insert) = match func {
        "JSON_SET" => (true, true),
        "JSON_INSERT" => (false, true),
        _ => (true, false),
    };
    let Some(target) = walk_mut(doc, parent) else { return Ok(()) };
    match (last, target) {
        (Step::Key(k), Json::Object(m)) => match m.iter_mut().find(|(x, _)| x == k) {
            Some((_, v)) if set => *v = val,
            None if insert => m.push((k.clone(), val)),
            _ => {}
        },
        (Step::Index(i), Json::Array(v)) => match i.resolve(v.len()).filter(|&i| i < v.len()) {
            Some(i) if set => v[i] = val,
            None if insert => v.push(val),
            _ => {}
        },
        // A scalar or object is a one-element array to `$[n]`.
        (Step::Index(i), other) => {
            if matches!(i, Idx::At(0) | Idx::FromLast(0)) {
                if set {
                    *other = val;
                }
            } else if insert {
                let old = std::mem::replace(other, Json::Null);
                *other = Json::Array(vec![old, val]);
            }
        }
        _ => {}
    }
    Ok(())
}

/// RFC 7396 merge patch.
fn merge_patch(target: Json, patch: Json) -> Json {
    match patch {
        Json::Object(pm) => {
            let mut tm = match target {
                Json::Object(m) => m,
                _ => Vec::new(),
            };
            for (k, v) in pm {
                let existing = tm.iter().position(|(x, _)| *x == k);
                if matches!(v, Json::Null) {
                    if let Some(i) = existing {
                        tm.remove(i);
                    }
                } else if let Some(i) = existing {
                    let old = std::mem::replace(&mut tm[i].1, Json::Null);
                    tm[i].1 = merge_patch(old, v);
                } else {
                    tm.push((k, merge_patch(Json::Null, v)));
                }
            }
            Json::Object(tm)
        }
        other => other,
    }
}

/// JSON_MERGE_PRESERVE: arrays concatenate, objects merge (a shared key's
/// values merge too), anything else is wrapped in an array first.
fn merge_preserve(a: Json, b: Json) -> Json {
    match (a, b) {
        (Json::Object(mut am), Json::Object(bm)) => {
            for (k, v) in bm {
                match am.iter().position(|(x, _)| *x == k) {
                    Some(i) => {
                        let old = std::mem::replace(&mut am[i].1, Json::Null);
                        am[i].1 = merge_preserve(old, v);
                    }
                    None => am.push((k, v)),
                }
            }
            Json::Object(am)
        }
        (a, b) => {
            let items = |j: Json| match j {
                Json::Array(v) => v,
                other => vec![other],
            };
            let mut v = items(a);
            v.extend(items(b));
            Json::Array(v)
        }
    }
}

fn json_depth(j: &Json) -> i64 {
    match j {
        Json::Array(v) if !v.is_empty() => 1 + v.iter().map(json_depth).max().unwrap_or(0),
        Json::Object(m) if !m.is_empty() => {
            1 + m.iter().map(|(_, v)| json_depth(v)).max().unwrap_or(0)
        }
        _ => 1,
    }
}

/// Paths of the string values in `j` matching `needle` (JSON_SEARCH; `%`
/// and `_` wildcards as in LIKE).
fn search_strings(j: &Json, at: String, needle: &str, out: &mut Vec<String>) {
    match j {
        Json::Str(s) => {
            if crate::mysql::exec::mysql_like(s, needle, Some('\\')) {
                out.push(at);
            }
        }
        Json::Array(v) => {
            for (i, x) in v.iter().enumerate() {
                search_strings(x, format!("{at}[{i}]"), needle, out);
            }
        }
        Json::Object(m) => {
            for (k, v) in m {
                let key = if k.chars().all(|c| c.is_alphanumeric() || c == '_') {
                    k.clone()
                } else {
                    format!("\"{k}\"")
                };
                search_strings(v, format!("{at}.{key}"), needle, out);
            }
        }
        _ => {}
    }
}

fn pretty(j: &Json, indent: usize) -> String {
    let pad = "  ".repeat(indent + 1);
    let end = "  ".repeat(indent);
    match j {
        Json::Array(v) if !v.is_empty() => format!(
            "[\n{}\n{end}]",
            v.iter()
                .map(|x| format!("{pad}{}", pretty(x, indent + 1)))
                .collect::<Vec<_>>()
                .join(",\n")
        ),
        Json::Object(m) if !m.is_empty() => format!(
            "{{\n{}\n{end}}}",
            m.iter()
                .map(|(k, v)| {
                    format!(
                        "{pad}{}: {}",
                        Json::Str(k.clone()).to_jsonb_string(),
                        pretty(v, indent + 1)
                    )
                })
                .collect::<Vec<_>>()
                .join(",\n")
        ),
        other => other.to_jsonb_string(),
    }
}

/// A MySQL JSON path: `$`, `$.a.b`, `$."a key"`, `$[0]`, `$.a[2].b`,
/// `$[last]`, `$[last-1]`, and the wildcards `.*`, `[*]` and `**`.
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
        if let Some(r) = rest.strip_prefix("**") {
            out.push(Step::Descend);
            rest = r;
            // `**` must be followed by a member or cell.
            if rest.is_empty() {
                return Err(bad());
            }
        } else if let Some(r) = rest.strip_prefix(".*") {
            out.push(Step::AnyKey);
            rest = r;
        } else if let Some(r) = rest.strip_prefix('.') {
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
            let inner = r[..end].trim();
            if inner == "*" {
                out.push(Step::AnyIndex);
            } else if let Some(l) = inner.strip_prefix("last") {
                let l = l.trim();
                let k = match l.strip_prefix('-') {
                    Some(n) => n.trim().parse::<usize>().map_err(|_| bad())?,
                    None if l.is_empty() => 0,
                    None => return Err(bad()),
                };
                out.push(Step::Index(Idx::FromLast(k)));
            } else {
                out.push(Step::Index(Idx::At(inner.parse::<usize>().map_err(|_| bad())?)));
            }
            rest = &r[end + 1..];
        } else {
            return Err(bad());
        }
    }
    Ok(out)
}

fn walk<'a>(doc: &'a Json, path: &[Step]) -> Option<&'a Json> {
    let mut found = Vec::new();
    walk_all(doc, path, &mut found);
    found.into_iter().next()
}

/// CRC-32 (IEEE), as MySQL's `CRC32()`.
fn crc32(data: &[u8]) -> u32 {
    let mut c = !0u32;
    for &b in data {
        c ^= b as u32;
        for _ in 0..8 {
            c = if c & 1 != 0 { (c >> 1) ^ 0xEDB8_8320 } else { c >> 1 };
        }
    }
    !c
}

/// A value as MySQL's bit operators see it: an unsigned 64-bit integer
/// (negative integers wrap, fractions round, strings read their number).
pub(crate) fn to_u64(v: &Value) -> u64 {
    match v {
        Value::Int(i) => *i as u64,
        Value::Bool(b) => u64::from(*b),
        Value::Num(n) => {
            let r = n.round(0);
            match r.to_i64() {
                Some(i) => i as u64,
                None if r.is_negative() => i64::MIN as u64,
                None => r.to_string().parse::<u64>().unwrap_or(u64::MAX),
            }
        }
        other => {
            let f = value_to_f64(other).round();
            if f < 0.0 {
                (f.max(i64::MIN as f64) as i64) as u64
            } else if f >= u64::MAX as f64 {
                u64::MAX
            } else {
                f as u64
            }
        }
    }
}

/// An unsigned 64-bit result: an integer when it fits, else a DECIMAL.
pub(crate) fn from_u64(u: u64) -> Value {
    match i64::try_from(u) {
        Ok(i) => Value::Int(i),
        Err(_) => Value::Num(Numeric::parse(&u.to_string()).unwrap_or_else(|_| Numeric::zero())),
    }
}

/// A pseudo-random double in [0, 1) for `RAND()` (no seed).
fn random_f64() -> f64 {
    use std::cell::Cell;
    thread_local! {
        static STATE: Cell<u64> = Cell::new({
            let t = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0x9E37_79B9_7F4A_7C15);
            t | 1
        });
    }
    STATE.with(|s| {
        let mut x = s.get();
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        s.set(x);
        (x >> 11) as f64 / (1u64 << 53) as f64
    })
}

const WEEK_MONDAY_FIRST: u32 = 1;
const WEEK_YEAR: u32 = 2;
const WEEK_FIRST_WEEKDAY: u32 = 4;

/// MySQL's `week_mode`: WEEK()'s mode argument as calc_week flags.
fn week_mode(mode: i64) -> u32 {
    let mut f = (mode & 7) as u32;
    if f & WEEK_MONDAY_FIRST == 0 {
        f ^= WEEK_FIRST_WEEKDAY;
    }
    f
}

fn days_in_year(y: i64) -> i64 {
    if crate::sql::datetime::is_leap(y) { 366 } else { 365 }
}

/// Day-of-week index of an epoch day: 0 = Monday (or 0 = Sunday when
/// `sunday_first`). Day 0 (2000-01-01) was a Saturday.
fn weekday(day: i64, sunday_first: bool) -> i64 {
    (day + 5 + i64::from(sunday_first)).rem_euclid(7)
}

/// MySQL's `calc_week`: (year, week) of an epoch day.
fn calc_week(day: i32, behaviour: u32) -> (i64, i64) {
    let (y, m, d) = ymd_from_date(day);
    let daynr = day as i64;
    let mut first_daynr = date_from_ymd(y, 1, 1) as i64;
    let monday_first = behaviour & WEEK_MONDAY_FIRST != 0;
    let mut week_year = behaviour & WEEK_YEAR != 0;
    let first_weekday = behaviour & WEEK_FIRST_WEEKDAY != 0;
    let mut wd = weekday(first_daynr, !monday_first);
    let mut year = y;
    if m == 1 && (d as i64) <= 7 - wd {
        if !week_year && ((first_weekday && wd != 0) || (!first_weekday && wd >= 4)) {
            return (year, 0);
        }
        week_year = true;
        year -= 1;
        let days = days_in_year(year);
        first_daynr -= days;
        wd = (wd + 53 * 7 - days) % 7;
    }
    let days = if (first_weekday && wd != 0) || (!first_weekday && wd >= 4) {
        daynr - (first_daynr + (7 - wd))
    } else {
        daynr - (first_daynr - wd)
    };
    if week_year && days >= 52 * 7 {
        let wd2 = (wd + days_in_year(year)) % 7;
        if (!first_weekday && wd2 < 4) || (first_weekday && wd2 == 0) {
            return (year + 1, 1);
        }
    }
    (year, days / 7 + 1)
}

/// MySQL's INET_ATON: dotted quad, also with fewer parts (`127.1` is
/// 127.0.0.1: the last part fills the remaining bytes).
fn inet_aton(s: &str) -> Option<u32> {
    let parts: Vec<&str> = s.split('.').collect();
    if parts.is_empty()
        || parts.len() > 4
        || parts.iter().any(|p| p.is_empty() || !p.bytes().all(|c| c.is_ascii_digit()))
    {
        return None;
    }
    let nums: Vec<u64> = parts.iter().map(|p| p.parse().ok()).collect::<Option<_>>()?;
    let (last, head) = nums.split_last()?;
    if head.iter().any(|n| *n > 255) {
        return None;
    }
    let rest_bits = 8 * (4 - head.len() as u32);
    if rest_bits < 32 && *last >= 1u64 << rest_bits {
        return None;
    }
    let mut v: u64 = 0;
    for n in head {
        v = (v << 8) | n;
    }
    Some(((v << rest_bits) | last) as u32)
}

/// IPv6 text as MySQL's INET6_NTOA writes it: IPv4-mapped and
/// IPv4-compatible addresses end in a dotted quad.
fn ipv6_text(b: &[u8; 16]) -> String {
    let compat =
        b[..12].iter().all(|x| *x == 0) && (b[12] != 0 || b[13] != 0 || b[14] != 0 || b[15] > 1);
    if compat {
        return format!("::{}.{}.{}.{}", b[12], b[13], b[14], b[15]);
    }
    std::net::Ipv6Addr::from(*b).to_string()
}
