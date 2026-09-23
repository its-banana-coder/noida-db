//! CONFIG GET/SET/RESETSTAT/REWRITE, ported from Redis's config.c.
//!
//! The parameter table (src/redis/config_table.rs) is generated from
//! Redis 7.2's own table, so every name, alias, default, range and enum is
//! Redis's. Values are remembered and reported back; the ones noida acts
//! on are read from here by the rest of the server.

use std::collections::HashMap;

use super::config_table::PARAMS;
use super::engine::{Command, Ctx, Engine, Reply, cmd, container, eq_ic, help_reply, syntax};
use super::glob;
use super::resp::Value;

pub static COMMANDS: &[Command] =
    &[container("config", config_help, CONFIG), cmd("lolwut", lolwut)];

static CONFIG: &[Command] = &[
    cmd("help", config_help),
    cmd("get", config_get),
    cmd("set", config_set),
    cmd("resetstat", config_resetstat),
    cmd("rewrite", config_rewrite),
];

/// What a parameter accepts.
pub enum Kind {
    Bool,
    Text,
    Enum(&'static [&'static str]),
    Number {
        lower: i64,
        upper: i64,
        memory: bool,
        percent: bool,
        octal: bool,
    },
    /// Parsed by hand in Redis (save, notify-keyspace-events, bind...).
    Special,
}

pub struct Param {
    pub name: &'static str,
    /// The other name this parameter also answers to.
    pub alias: Option<&'static str>,
    pub kind: Kind,
    pub default: &'static str,
    pub immutable: bool,
    pub multi_arg: bool,
}

/// Every parameter's current value, by canonical (table) name.
#[derive(Default)]
pub struct Config {
    values: HashMap<&'static str, String>,
}

fn lookup(name: &str) -> Option<&'static Param> {
    PARAMS.iter().find(|p| p.name.eq_ignore_ascii_case(name))
}

/// Aliases share one value, kept under the first table entry.
fn canonical(p: &'static Param) -> &'static Param {
    match p.alias.and_then(lookup) {
        Some(other) if other.name < p.name => other,
        _ => p,
    }
}

impl Config {
    pub fn get(&self, name: &str) -> Option<String> {
        let p = lookup(name)?;
        Some(self.values.get(canonical(p).name).cloned().unwrap_or_else(|| p.default.to_string()))
    }

    fn set(&mut self, p: &'static Param, value: String) {
        self.values.insert(canonical(p).name, value);
    }
}

impl Engine {
    /// A parameter's value as a number, for the places noida honours it.
    pub fn config_num(&self, name: &str) -> i64 {
        self.config.get(name).and_then(|v| v.parse().ok()).unwrap_or(0)
    }
}

/// `memtoull`: 1k = 1000, 1kb = 1024, and so on up to gb.
fn memtoull(s: &str) -> Option<i64> {
    let digits: String = s.chars().take_while(|c| c.is_ascii_digit()).collect();
    if digits.is_empty() {
        return None;
    }
    let unit = &s[digits.len()..];
    let mul: i64 = match unit.to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "k" => 1000,
        "kb" => 1024,
        "m" => 1000 * 1000,
        "mb" => 1024 * 1024,
        "g" => 1000 * 1000 * 1000,
        "gb" => 1024 * 1024 * 1024,
        _ => return None,
    };
    digits.parse::<i64>().ok()?.checked_mul(mul)
}

/// `keyspaceEventsStringToFlags` + `keyspaceEventsFlagsToString`: checks the
/// class letters and returns them in Redis's canonical order.
fn keyspace_events(s: &str) -> Option<String> {
    const CLASSES: &str = "g$lshzxetdn";
    let mut all = false;
    let (mut keyspace, mut keyevent, mut miss) = (false, false, false);
    let mut seen = String::new();
    for c in s.chars() {
        match c {
            'A' => all = true,
            'K' => keyspace = true,
            'E' => keyevent = true,
            'm' => miss = true,
            c if CLASSES.contains(c) => {
                if !seen.contains(c) {
                    seen.push(c);
                }
            }
            _ => return None,
        }
    }
    let mut out = String::new();
    if all || CLASSES.chars().all(|c| seen.contains(c)) {
        out.push('A');
    } else {
        out.extend(CLASSES.chars().filter(|c| seen.contains(*c)));
    }
    if keyspace {
        out.push('K');
    }
    if keyevent {
        out.push('E');
    }
    if miss {
        out.push('m');
    }
    Some(out)
}

/// Checks a value for a parameter and returns what Redis would store.
fn validate(p: &Param, value: &str) -> Result<String, String> {
    match &p.kind {
        Kind::Bool => match value.to_ascii_lowercase().as_str() {
            "yes" => Ok("yes".into()),
            "no" => Ok("no".into()),
            _ => Err("argument must be 'yes' or 'no'".into()),
        },
        Kind::Text => Ok(value.to_string()),
        Kind::Enum(values) => {
            let words: Vec<&str> =
                if p.multi_arg { value.split(' ').collect() } else { vec![value] };
            if words.iter().all(|w| values.iter().any(|v| v.eq_ignore_ascii_case(w))) {
                let mut out = Vec::new();
                for w in words {
                    let v = values.iter().find(|v| v.eq_ignore_ascii_case(w)).unwrap();
                    out.push(*v);
                }
                return Ok(out.join(" "));
            }
            Err(format!("argument(s) must be one of the following: {}", values.join(", ")))
        }
        Kind::Number { lower, upper, memory, percent, octal } => {
            let parsed = parse_number(value, *memory, *percent, *octal).ok_or_else(|| {
                match (memory, percent, octal) {
                    (true, true, _) => "argument must be a memory or percent value",
                    (true, false, _) => "argument must be a memory value",
                    (false, _, true) => "argument couldn't be parsed as an octal number",
                    _ => "argument couldn't be parsed into an integer",
                }
                .to_string()
            })?;
            if *percent && parsed < 0 {
                if parsed < *lower {
                    return Err(format!("percentage argument must be less or equal to {}", -lower));
                }
            } else if parsed > *upper || parsed < *lower {
                let (lo, hi) = if *octal {
                    (format!("{lower:o}"), format!("{upper:o}"))
                } else {
                    (lower.to_string(), upper.to_string())
                };
                return Err(format!("argument must be between {lo} and {hi} inclusive"));
            }
            Ok(if *percent && parsed < 0 {
                format!("{}%", -parsed)
            } else if *octal {
                format!("{parsed:o}")
            } else {
                parsed.to_string()
            })
        }
        Kind::Special => validate_special(p, value),
    }
}

fn parse_number(value: &str, memory: bool, percent: bool, octal: bool) -> Option<i64> {
    if memory && let Some(v) = memtoull(value) {
        return Some(v);
    }
    if percent
        && let Some(body) = value.strip_suffix('%')
        && let Ok(v) = body.parse::<i64>()
        && v >= 0
    {
        return Some(-v);
    }
    if octal && let Ok(v) = i64::from_str_radix(value, 8) {
        return Some(v);
    }
    if !memory && !percent && !octal {
        return value.parse().ok();
    }
    None
}

fn validate_special(p: &Param, value: &str) -> Result<String, String> {
    let words: Vec<&str> = value.split_whitespace().collect();
    match p.name {
        "notify-keyspace-events" => keyspace_events(value)
            .ok_or_else(|| "Invalid event class character. Use 'Ag$lshzxeKEtmdn'.".into()),
        "save" => {
            if words.len() % 2 == 1 {
                return Err("Invalid save parameters".into());
            }
            for (i, w) in words.iter().enumerate() {
                match w.parse::<i64>() {
                    Ok(v) if (i % 2 == 0 && v >= 1) || (i % 2 == 1 && v >= 0) => {}
                    _ => return Err("Invalid save parameters".into()),
                }
            }
            Ok(words.join(" "))
        }
        "oom-score-adj-values" => {
            if words.len() != 3 {
                return Err("wrong number of arguments".into());
            }
            for w in &words {
                match w.parse::<i64>() {
                    Ok(v) if (-2000..=2000).contains(&v) => {}
                    _ => {
                        return Err(
                            "Invalid oom-score-adj-values, elements must be between -2000 and 2000."
                                .into(),
                        );
                    }
                }
            }
            Ok(words.join(" "))
        }
        "latency-tracking-info-percentiles" => {
            let mut out = Vec::new();
            for w in &words {
                match w.parse::<f64>() {
                    Ok(v) if (0.0..=100.0).contains(&v) => out.push(super::double::d2string(v)),
                    Ok(_) => {
                        return Err(
                            "latency-tracking-info-percentiles parameters should sit between [0.0,100.0]"
                                .into(),
                        );
                    }
                    Err(_) => {
                        return Err("Invalid latency-tracking-info-percentiles parameters".into());
                    }
                }
            }
            Ok(out.join(" "))
        }
        _ => Ok(value.to_string()),
    }
}

fn config_get(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let mut out: Vec<(String, String)> = Vec::new();
    for raw in &a[2..] {
        let name = String::from_utf8_lossy(raw).to_string();
        if !name.contains(['*', '?', '[']) {
            if out.iter().any(|(n, _)| n.eq_ignore_ascii_case(&name)) {
                continue;
            }
            if let Some(v) = ctx.engine.config.get(&name) {
                out.push((name, v));
            }
            continue;
        }
        for p in PARAMS {
            if out.iter().any(|(n, _)| n == p.name) {
                continue;
            }
            if glob::matches(raw, p.name.as_bytes(), true) {
                let v = ctx.engine.config.get(p.name).unwrap_or_default();
                out.push((p.name.to_string(), v));
            }
        }
    }
    Ok(Value::Map(out.into_iter().map(|(n, v)| (Value::bulk(n), Value::bulk(v))).collect()))
}

fn config_set(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    if a.len() % 2 == 1 || a.len() < 4 {
        return Err(syntax());
    }
    let mut pending: Vec<(&'static Param, String)> = Vec::new();
    for pair in a[2..].chunks(2) {
        let name = String::from_utf8_lossy(&pair[0]).to_string();
        let value = String::from_utf8_lossy(&pair[1]).to_string();
        let Some(p) = lookup(&name) else {
            return Err(Value::err(format!(
                "ERR Unknown option or number of arguments for CONFIG SET - '{name}'"
            )));
        };
        let fail = |msg: &str| {
            Value::err(format!(
                "ERR CONFIG SET failed (possibly related to argument '{name}') - {msg}"
            ))
        };
        if p.immutable {
            return Err(fail("can't set immutable config"));
        }
        if pending.iter().any(|(q, _)| canonical(q).name == canonical(p).name) {
            return Err(fail("duplicate parameter"));
        }
        match validate(p, &value) {
            Ok(v) => pending.push((p, v)),
            Err(msg) => return Err(fail(&msg)),
        }
    }
    for (p, v) in pending {
        ctx.engine.config.set(p, v);
    }
    Ok(Value::ok())
}

fn config_resetstat(_: &mut Ctx, _: &[Vec<u8>]) -> Reply {
    // The INFO counters noida reports are constants, so there is nothing
    // to clear.
    Ok(Value::ok())
}

fn config_rewrite(_: &mut Ctx, _: &[Vec<u8>]) -> Reply {
    // noida is always started without a configuration file.
    Err(Value::err("ERR The server is running without a config file"))
}

fn config_help(_: &mut Ctx, _: &[Vec<u8>]) -> Reply {
    Ok(help_reply(
        "config",
        &[
            "GET <pattern>",
            "    Return parameters matching the glob-like <pattern> and their values.",
            "SET <directive> <value>",
            "    Set the configuration <directive> to <value>.",
            "RESETSTAT",
            "    Reset statistics reported by the INFO command.",
            "REWRITE",
            "    Rewrite the configuration file.",
        ],
    ))
}

/// LOLWUT, which every Redis has answered since 5.0.
fn lolwut(_: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    for pair in a[1..].chunks(2) {
        if !eq_ic(&pair[0], "version") || pair.len() != 2 {
            return Err(syntax());
        }
    }
    let version = super::REDIS_VERSION;
    Ok(Value::bulk(format!(
        "Redis ver. {version}\n\nnoida speaks Redis; the art is left to the original.\n"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_parameter_round_trips_its_default() {
        for p in PARAMS {
            if matches!(p.kind, Kind::Special) {
                continue;
            }
            let stored = validate(p, p.default)
                .unwrap_or_else(|e| panic!("{} default {:?}: {e}", p.name, p.default));
            assert_eq!(stored, p.default, "{}", p.name);
        }
    }

    #[test]
    fn keyspace_event_flags_are_canonical() {
        assert_eq!(keyspace_events("KEA").unwrap(), "AKE");
        assert_eq!(keyspace_events("").unwrap(), "");
        assert_eq!(keyspace_events("lKz").unwrap(), "lzK");
        assert_eq!(keyspace_events("g$lshzxetdn").unwrap(), "A");
        assert_eq!(keyspace_events("Em").unwrap(), "Em");
        assert_eq!(keyspace_events("q"), None);
    }
}
