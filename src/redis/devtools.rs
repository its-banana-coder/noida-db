//! Commands developer GUIs and CLIs probe on connect: SLOWLOG, LATENCY,
//! MEMORY, MODULE LIST and the read-only parts of ACL (Redis 7.2 shapes).
//!
//! noida does no performance analysis, so the slow log and latency history
//! are always empty and MEMORY figures are estimates. There is one user,
//! `default`, so ACL only reports it; user management is a production
//! concern and is left out (see the scope filter in docs/specs/README.md).

use super::command_meta;
use super::engine::{
    Command, Ctx, Data, Entry, NUM_DBS, Reply, arity_error, cmd, container, eq_ic, help_reply,
    int_arg, is_implemented, range_long, syntax,
};
use super::resp::Value;

pub static COMMANDS: &[Command] = &[
    container("slowlog", bare, SLOWLOG),
    container("latency", bare, LATENCY),
    container("memory", bare, MEMORY),
    container("module", bare, MODULE),
    container("acl", bare, ACL),
];

static SLOWLOG: &[Command] =
    &[cmd("get", slowlog_get), cmd("len", zero), cmd("reset", ok), cmd("help", slowlog_help)];

static LATENCY: &[Command] = &[
    cmd("doctor", latency_doctor),
    cmd("graph", latency_graph),
    cmd("histogram", latency_histogram),
    cmd("history", empty_array),
    cmd("latest", empty_array),
    cmd("reset", zero),
    cmd("help", latency_help),
];

static MEMORY: &[Command] = &[
    cmd("doctor", memory_doctor),
    cmd("malloc-stats", memory_malloc_stats),
    cmd("purge", ok),
    cmd("stats", memory_stats),
    cmd("usage", memory_usage),
    cmd("help", memory_help),
];

static MODULE: &[Command] = &[cmd("list", empty_array), cmd("help", module_help)];

static ACL: &[Command] = &[
    cmd("cat", acl_cat),
    cmd("genpass", acl_genpass),
    cmd("getuser", acl_getuser),
    cmd("list", acl_list),
    cmd("log", acl_log),
    cmd("users", acl_users),
    cmd("whoami", acl_whoami),
    cmd("help", acl_help),
];

/// A container called without a subcommand fails its arity check first.
fn bare(_: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    Err(arity_error(&String::from_utf8_lossy(&a[0]).to_ascii_lowercase()))
}

fn ok(_: &mut Ctx, _: &[Vec<u8>]) -> Reply {
    Ok(Value::ok())
}

fn zero(_: &mut Ctx, _: &[Vec<u8>]) -> Reply {
    Ok(Value::Integer(0))
}

fn empty_array(_: &mut Ctx, _: &[Vec<u8>]) -> Reply {
    Ok(Value::Array(vec![]))
}

fn txt(s: &str) -> Value {
    Value::Verbatim("txt", s.as_bytes().to_vec())
}

// ---- SLOWLOG ----

const SLOWLOG_HELP: &[&str] = &[
    "GET [<count>]",
    "    Return top <count> entries from the slowlog (default: 10, -1 mean all).",
    "    Entries are made of:",
    "    id, timestamp, time in microseconds, arguments array, client IP and port,",
    "    client name",
    "LEN",
    "    Return the length of the slowlog.",
    "RESET",
    "    Reset the slowlog.",
];

fn slowlog_help(_: &mut Ctx, _: &[Vec<u8>]) -> Reply {
    Ok(help_reply("slowlog", SLOWLOG_HELP))
}

fn slowlog_get(_: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    if let Some(count) = a.get(2) {
        range_long(count, -1, i64::MAX, Some("count should be greater than or equal to -1"))?;
    }
    Ok(Value::Array(vec![]))
}

// ---- LATENCY ----

const LATENCY_HELP: &[&str] = &[
    "DOCTOR",
    "    Return a human readable latency analysis report.",
    "GRAPH <event>",
    "    Return an ASCII latency graph for the <event> class.",
    "HISTORY <event>",
    "    Return time-latency samples for the <event> class.",
    "LATEST",
    "    Return the latest latency samples for all events.",
    "RESET [<event> ...]",
    "    Reset latency data of one or more <event> classes.",
    "    (default: reset all data for all event classes)",
    "HISTOGRAM [COMMAND ...]",
    "    Return a cumulative distribution of latencies in the format of a histogram for the specified command names.",
    "    If no commands are specified then all histograms are replied.",
];

fn latency_help(_: &mut Ctx, _: &[Vec<u8>]) -> Reply {
    Ok(help_reply("latency", LATENCY_HELP))
}

fn latency_doctor(ctx: &mut Ctx, _: &[Vec<u8>]) -> Reply {
    // Monitoring is off by default; with it on there are still no samples.
    let threshold: i64 = ctx
        .engine
        .config
        .get("latency-monitor-threshold")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    Ok(txt(if threshold == 0 {
        "I'm sorry, Dave, I can't do that. Latency monitoring is disabled in this Redis instance. \
         You may use \"CONFIG SET latency-monitor-threshold <milliseconds>.\" in order to enable \
         it. If we weren't in a deep space mission I'd suggest to take a look at \
         https://redis.io/topics/latency-monitor.\n"
    } else {
        "Dave, no latency spike was observed during the lifetime of this Redis instance, not in \
         the slightest bit. I honestly think you ought to sit down calmly, take a stress pill, \
         and think things over.\n"
    }))
}

fn latency_graph(_: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    Err(Value::err(format!(
        "ERR No samples available for event '{}'",
        String::from_utf8_lossy(&a[2])
    )))
}

fn latency_histogram(_: &mut Ctx, _: &[Vec<u8>]) -> Reply {
    Ok(Value::Map(vec![]))
}

// ---- MEMORY ----

const MEMORY_HELP: &[&str] = &[
    "DOCTOR",
    "    Return memory problems reports.",
    "MALLOC-STATS",
    "    Return internal statistics report from the memory allocator.",
    "PURGE",
    "    Attempt to purge dirty pages for reclamation by the allocator.",
    "STATS",
    "    Return information about the memory usage of the server.",
    "USAGE <key> [SAMPLES <count>]",
    "    Return memory in bytes used by <key> and its value. Nested values are",
    "    sampled up to <count> times (default: 5, 0 means sample all).",
];

fn memory_help(_: &mut Ctx, _: &[Vec<u8>]) -> Reply {
    Ok(help_reply("memory", MEMORY_HELP))
}

fn memory_doctor(_: &mut Ctx, _: &[Vec<u8>]) -> Reply {
    // Under 5MB Redis reports the instance as empty; noida always is.
    Ok(txt("Hi Sam, this instance is empty or is using very little memory, my issues detector \
         can't be used in these conditions. Please, leave for your mission on Earth and fill it \
         with some data. The new Sam and I will be back to our programming as soon as I finished \
         rebooting.\n"))
}

fn memory_malloc_stats(_: &mut Ctx, _: &[Vec<u8>]) -> Reply {
    Ok(Value::bulk("Stats not supported for the current allocator"))
}

/// A rough size in bytes: enough for tools to rank keys, not an exact
/// figure (noida's memory layout is not Redis's).
fn estimate(key: &[u8], entry: &Entry) -> i64 {
    let value = match &entry.data {
        Data::Str(s) => s.len() + 16,
        Data::Hash(h) => 64 + h.map.iter().map(|(k, v)| k.len() + v.len() + 16).sum::<usize>(),
        Data::List(l) => 64 + l.iter().map(|e| e.len() + 11).sum::<usize>(),
        Data::Set(s) => 64 + s.members().iter().map(|m| m.len() + 16).sum::<usize>(),
        Data::Zset(z) => 64 + z.iter().map(|(_, m)| m.len() + 24).sum::<usize>(),
        Data::Stream(s) => 128 + s.len() * 64,
    };
    (value + key.len() + 48) as i64
}

fn memory_usage(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    // Options are validated before the key is looked up, as in Redis.
    let mut j = 3;
    while j < a.len() {
        if eq_ic(&a[j], "samples") && j + 1 < a.len() {
            if int_arg(&a[j + 1])? < 0 {
                return Err(syntax());
            }
            j += 2;
        } else {
            return Err(syntax());
        }
    }
    let size = ctx.lookup(&a[2]).map(|e| estimate(&a[2], e));
    Ok(size.map_or(Value::Null, Value::Integer))
}

fn memory_stats(ctx: &mut Ctx, _: &[Vec<u8>]) -> Reply {
    let now = ctx.now;
    let mut keys = 0usize;
    let mut dataset = 0i64;
    let mut dbs = Vec::new();
    for i in 0..NUM_DBS {
        let (n, expires) = ctx.engine.dbs[i].counts(now);
        if n == 0 {
            continue;
        }
        keys += n;
        for k in ctx.engine.dbs[i].keys(now) {
            if let Some(entry) = ctx.engine.dbs[i].get(&k, now) {
                dataset += estimate(&k, entry);
            }
        }
        let field = |name: &str, v: i64| (Value::bulk(name), Value::Integer(v));
        dbs.push((
            Value::bulk(format!("db.{i}")),
            Value::Map(vec![
                field("overhead.hashtable.main", n as i64 * 32),
                field("overhead.hashtable.expires", expires as i64 * 32),
                field("overhead.hashtable.slot-to-keys", 0),
            ]),
        ));
    }
    let startup = 1_000_000i64;
    let total = startup + dataset;
    let int = |name: &str, v: i64| (Value::bulk(name), Value::Integer(v));
    let dbl = |name: &str, v: f64| (Value::bulk(name), Value::Double(v));
    let mut out = vec![
        int("peak.allocated", total),
        int("total.allocated", total),
        int("startup.allocated", startup),
        int("replication.backlog", 0),
        int("clients.slaves", 0),
        int("clients.normal", 0),
        int("cluster.links", 0),
        int("aof.buffer", 0),
        int("lua.caches", 0),
        int("functions.caches", 0),
    ];
    out.extend(dbs);
    let per_key = if keys > 0 { dataset / keys as i64 } else { 0 };
    out.extend([
        int("overhead.total", startup),
        int("keys.count", keys as i64),
        int("keys.bytes-per-key", per_key),
        int("dataset.bytes", dataset),
        dbl("dataset.percentage", if dataset > 0 { 100.0 } else { 0.0 }),
        dbl("peak.percentage", 100.0),
        int("allocator.allocated", total),
        int("allocator.active", total),
        int("allocator.resident", total),
        dbl("allocator-fragmentation.ratio", 1.0),
        int("allocator-fragmentation.bytes", 0),
        dbl("allocator-rss.ratio", 1.0),
        int("allocator-rss.bytes", 0),
        dbl("rss-overhead.ratio", 1.0),
        int("rss-overhead.bytes", 0),
        dbl("fragmentation", 1.0),
        int("fragmentation.bytes", 0),
    ]);
    Ok(Value::Map(out))
}

// ---- MODULE ----

const MODULE_HELP: &[&str] = &["LIST", "    Return a list of loaded modules."];

fn module_help(_: &mut Ctx, _: &[Vec<u8>]) -> Reply {
    Ok(help_reply("module", MODULE_HELP))
}

// ---- ACL (default user only) ----

const ACL_HELP: &[&str] = &[
    "CAT [<category>]",
    "    List all commands that belong to <category>, or all command categories",
    "    when no category is specified.",
    "GETUSER <username>",
    "    Get the user's details.",
    "GENPASS [<bits>]",
    "    Generate a secure 256-bit user password. The optional `bits` argument can",
    "    be used to specify a different size.",
    "LIST",
    "    Show users details in config file format.",
    "LOG [<count> | RESET]",
    "    Show the ACL log entries.",
    "USERS",
    "    List all the registered usernames.",
    "WHOAMI",
    "    Return the current connection username.",
];

fn acl_help(_: &mut Ctx, _: &[Vec<u8>]) -> Reply {
    Ok(help_reply("acl", ACL_HELP))
}

fn acl_whoami(_: &mut Ctx, _: &[Vec<u8>]) -> Reply {
    Ok(Value::bulk("default"))
}

fn acl_users(_: &mut Ctx, _: &[Vec<u8>]) -> Reply {
    Ok(Value::Array(vec![Value::bulk("default")]))
}

fn acl_list(_: &mut Ctx, _: &[Vec<u8>]) -> Reply {
    Ok(Value::Array(vec![Value::bulk("user default on nopass sanitize-payload ~* &* +@all")]))
}

fn acl_getuser(_: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    if a[2] != b"default" {
        return Ok(Value::Null);
    }
    let b = Value::bulk;
    let flags = ["on", "nopass", "sanitize-payload"];
    Ok(Value::Map(vec![
        (b("flags"), Value::Set(flags.iter().map(|f| Value::bulk(f)).collect())),
        (b("passwords"), Value::Array(vec![])),
        (b("commands"), b("+@all")),
        (b("keys"), b("~*")),
        (b("channels"), b("&*")),
        (b("selectors"), Value::Array(vec![])),
    ]))
}

fn acl_cat(_: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let names = command_meta::acl_category_names();
    let Some(category) = a.get(2) else {
        return Ok(Value::Array(names.iter().map(Value::bulk).collect()));
    };
    let wanted = String::from_utf8_lossy(category).to_ascii_lowercase();
    if !names.contains(&wanted.as_str()) {
        let shown: String = String::from_utf8_lossy(category).chars().take(128).collect();
        return Err(Value::err(format!("ERR Unknown category '{shown}'")));
    }
    let mut found = Vec::new();
    for c in command_meta::all().iter().filter(|c| is_implemented(c.name)) {
        for m in std::iter::once(c).chain(c.subcommands.iter().filter(|s| is_implemented(s.name))) {
            if m.acl_categories().contains(&wanted.as_str()) {
                found.push(Value::bulk(m.name));
            }
        }
    }
    Ok(Value::Array(found))
}

fn acl_genpass(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let bits = match a.get(2) {
        Some(b) => int_arg(b)?,
        None => 256,
    };
    if bits <= 0 || bits > 4096 {
        return Err(Value::err(
            "ERR ACL GENPASS argument must be the number of bits for the output password, a \
             positive number up to 4096",
        ));
    }
    let chars = ((bits + 3) / 4) as usize;
    let mut out = String::new();
    while out.len() < chars {
        out += &format!("{:016x}", ctx.random());
    }
    out.truncate(chars);
    Ok(Value::bulk(out))
}

fn acl_log(_: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    match a.get(2) {
        None => Ok(Value::Array(vec![])),
        Some(arg) if a.len() == 3 && eq_ic(arg, "reset") => Ok(Value::ok()),
        Some(arg) if a.len() == 3 => {
            int_arg(arg)?;
            Ok(Value::Array(vec![]))
        }
        Some(_) => Err(syntax()),
    }
}
