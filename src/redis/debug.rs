//! DEBUG: the subcommands test suites (Redis's own included) use to steer
//! the server, in Redis 7.2's shapes. noida-db answers as a Redis started
//! with `enable-debug-command yes` would.
//!
//! Left out on purpose: the subcommands that crash or restart the server
//! (SEGFAULT, PANIC, OOM, ASSERT, RESTART, CRASH-AND-RECOVER), and the ones
//! that only exist to inspect Redis's C internals or its files (LOADAOF,
//! SDSLEN, HTSTATS, LISTPACK, QUICKLIST, STRUCTSIZE, ...). Those answer
//! with Redis's unknown-subcommand error.

use std::collections::BTreeSet;
use std::time::Duration;

use super::engine::{Command, Ctx, Data, Entry, Reply, cmd, eq_ic, help_reply};
use super::resp::Value;

pub static COMMANDS: &[Command] = &[cmd("debug", debug)];

fn debug(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let sub = String::from_utf8_lossy(&a[1]).to_lowercase();
    let n = a.len();
    match (sub.as_str(), n) {
        ("help", 2) => Ok(help_reply("debug", HELP)),
        ("sleep", 3) => {
            // strtod: anything unparsable is 0.
            let secs: f64 = std::str::from_utf8(&a[2])
                .ok()
                .and_then(|s| s.trim().parse().ok())
                .filter(|s: &f64| s.is_finite() && *s > 0.0)
                .unwrap_or(0.0);
            // The whole server stops, as in Redis: the engine stays locked.
            std::thread::sleep(Duration::from_secs_f64(secs.min(3600.0)));
            Ok(Value::ok())
        }
        ("set-active-expire", 3) => {
            let on = atoi(&a[2]) != 0;
            for db in ctx.engine.dbs.iter_mut() {
                db.lazy_expire_only = !on;
            }
            Ok(Value::ok())
        }
        ("set-skip-checksum-validation", 3) => {
            ctx.engine.skip_checksum = atoi(&a[2]) != 0;
            Ok(Value::ok())
        }
        // Knobs for Redis internals noida-db doesn't have: accepted, no effect.
        ("quicklist-packed-threshold", 3) => {
            super::config::memtoull(&String::from_utf8_lossy(&a[2])).ok_or_else(|| {
                Value::err("ERR argument must be a memory value bigger than 1 and smaller than 4gb")
            })?;
            Ok(Value::ok())
        }
        ("pause-cron", 3)
        | ("set-disable-deny-scripts", 3)
        | ("set-active-expire-effort", 3)
        | ("aof-flush-sleep", 3)
        | ("log", 3)
        | ("leak", 3)
        | ("change-repl-id", 2)
        | ("drop-cluster-packet-filter", 3)
        | ("stringmatch-len", _) => Ok(Value::ok()),
        ("replybuffer", 4) => Ok(Value::ok()),
        ("error", 3) => Err(Value::Error(String::from_utf8_lossy(&a[2]).into_owned())),
        ("protocol", 3) => protocol(ctx, &a[2]),
        ("object", 3) => object(ctx, &a[2]),
        ("digest", 2) => Ok(Value::Simple(hex(&dataset_digest(ctx)))),
        ("digest-value", _) => {
            let mut out = Vec::new();
            for key in &a[2..] {
                let d = match ctx.lookup_notouch(key) {
                    Some(e) => value_digest(&e.data),
                    None => [0; 20],
                };
                out.push(Value::Simple(hex(&d)));
            }
            Ok(Value::Array(out))
        }
        ("populate", 3..=5) => populate(ctx, a),
        ("reload", _) => reload(ctx, &a[2..]),
        _ => Err(Value::err(format!(
            "ERR unknown subcommand or wrong number of arguments for '{}'. Try DEBUG HELP.",
            String::from_utf8_lossy(&a[1])
        ))),
    }
}

fn atoi(b: &[u8]) -> i64 {
    let s = String::from_utf8_lossy(b);
    let s = s.trim_start();
    let (neg, digits) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    let n: i64 = digits
        .chars()
        .take_while(|c| c.is_ascii_digit())
        .fold(0i64, |n, c| n.wrapping_mul(10).wrapping_add(c as i64 - '0' as i64));
    if neg { -n } else { n }
}

fn protocol(ctx: &mut Ctx, name: &[u8]) -> Reply {
    let ints = || (0..3).map(Value::Integer).collect::<Vec<_>>();
    let resp3 = ctx.client().resp >= 3;
    let name = String::from_utf8_lossy(name).to_lowercase();
    Ok(match name.as_str() {
        "string" => Value::bulk("Hello World"),
        "integer" => Value::Integer(12345),
        #[allow(clippy::approx_constant)] // Redis's literal, not pi
        "double" => Value::Double(3.141),
        "bignum" => Value::BigNumber("1234567999999999999999999999999999999".into()),
        "null" => Value::Null,
        "array" => Value::Array(ints()),
        "set" => Value::Set(ints()),
        "map" => Value::Map((0..3).map(|j| (Value::Integer(j), Value::Bool(j == 1))).collect()),
        "attrib" => {
            let reply = Value::bulk("Some real reply following the attribute");
            if resp3 {
                Value::Many(vec![
                    Value::Attribute(vec![(
                        Value::bulk("key-popularity"),
                        Value::Array(vec![Value::bulk("key:123"), Value::Integer(90)]),
                    )]),
                    reply,
                ])
            } else {
                reply
            }
        }
        "push" => {
            if !resp3 {
                return Err(Value::err("ERR RESP2 is not supported by this command"));
            }
            // Redis writes the push after the command's own reply.
            Value::Many(vec![
                Value::bulk("Some real reply following the push reply"),
                Value::Push(vec![Value::bulk("server-cpu-usage"), Value::Integer(42)]),
            ])
        }
        "true" => Value::Bool(true),
        "false" => Value::Bool(false),
        "verbatim" => Value::Verbatim("txt", b"This is a verbatim\nstring".to_vec()),
        _ => {
            return Err(Value::err(
                "ERR Wrong protocol type name. Please use one of the following: \
                 string|integer|double|bignum|null|array|set|map|attrib|push|verbatim|true|false",
            ));
        }
    })
}

/// DEBUG OBJECT's one-line summary. The address is made up but stable for a
/// key (Redis's changes only when the value is reallocated).
fn object(ctx: &mut Ctx, key: &[u8]) -> Reply {
    let now = ctx.now;
    let access = ctx.db().access_of(key);
    let db = ctx.db_index();
    let Some(entry) = ctx.lookup_notouch(key) else {
        return Err(Value::err("ERR no such key"));
    };
    let encoding = super::keys::encoding(&entry.data);
    let serialized =
        // DUMP's payload less its type byte, RDB version and CRC.
        super::rdb::dump(&entry.data).map_or(0, |d| d.len().saturating_sub(11));
    let last = access.map_or(now, |a| a.0);
    let lru = (last / 1000) & ((1 << 24) - 1);
    let idle = now.saturating_sub(last) / 1000;
    let mut addr_src = key.to_vec();
    addr_src.push(db as u8);
    let d = digest_bytes(&addr_src);
    let addr =
        0x7f00_0000_0000u64 | (u64::from_be_bytes(d[..8].try_into().unwrap()) & 0xff_ffff_fff0);
    Ok(Value::Simple(format!(
        "Value at:0x{addr:x} refcount:1 encoding:{encoding} serializedlength:{serialized} \
         lru:{lru} lru_seconds_idle:{idle}"
    )))
}

fn populate(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let positive = |b: &[u8]| -> Result<i64, Value> {
        match super::num::parse_int(b) {
            Some(n) if n >= 0 => Ok(n),
            Some(_) => Err(Value::err("ERR value is out of range, must be positive")),
            None => Err(Value::err("ERR value is out of range, must be positive")),
        }
    };
    let count = positive(&a[2])?;
    let prefix = a.get(3).map_or(&b"key"[..], |p| &p[..]);
    let size = match a.get(4) {
        Some(s) => positive(s)? as usize,
        None => 0,
    };
    // A guard against a typo'd count taking the dev machine's memory.
    if count > 10_000_000 || size > 512 * 1024 * 1024 {
        return Err(Value::err("ERR OOM in dictTryExpand"));
    }
    for j in 0..count {
        let mut key = prefix.to_vec();
        key.extend_from_slice(format!(":{j}").as_bytes());
        if ctx.lookup(&key).is_some() {
            continue;
        }
        let base = format!("value:{j}").into_bytes();
        let value = if size == 0 {
            base
        } else {
            let mut v = vec![0u8; size];
            let n = size.min(base.len());
            v[..n].copy_from_slice(&base[..n]);
            v
        };
        ctx.db().insert(key, Entry::new(Data::Str(value)));
    }
    Ok(Value::ok())
}

/// Save, flush and load: every value goes through its DUMP form and back,
/// so it comes back in the encoding a fresh load gives it, and keys whose
/// time has passed are gone.
fn reload(ctx: &mut Ctx, opts: &[Vec<u8>]) -> Reply {
    for opt in opts {
        if !(eq_ic(opt, "merge") || eq_ic(opt, "noflush") || eq_ic(opt, "nosave")) {
            return Err(Value::err(
                "ERR DEBUG RELOAD only supports the MERGE, NOFLUSH and NOSAVE options.",
            ));
        }
    }
    if !opts.is_empty() {
        // These load whatever RDB file is on disk; noida-db has none.
        return Err(Value::err("ERR noida-db supports DEBUG RELOAD without options only"));
    }
    let limits = (ctx.limits("set"), ctx.limits("zset"), ctx.limits("hash"));
    let now = ctx.now;
    for db in ctx.engine.dbs.iter_mut() {
        let lazy = db.lazy_expire_only;
        db.lazy_expire_only = false;
        db.purge_expired(now);
        db.lazy_expire_only = lazy;
        for (_, entry) in db.entries_mut() {
            if let Some(payload) = super::rdb::dump(&entry.data)
                && let Some(body) = super::rdb::unseal(&payload)
                && let Some(data) = super::rdb::load(body, limits.0, limits.1, limits.2)
            {
                entry.data = data;
            }
        }
    }
    Ok(Value::ok())
}

// ---- digests ----

fn digest_bytes(data: &[u8]) -> [u8; 20] {
    let h = super::sha1::hex(data);
    let mut out = [0u8; 20];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(&h[i * 2..i * 2 + 2], 16).unwrap_or(0);
    }
    out
}

fn hex(d: &[u8; 20]) -> String {
    d.iter().map(|b| format!("{b:02x}")).collect()
}

fn xor(acc: &mut [u8; 20], d: &[u8; 20]) {
    for (a, b) in acc.iter_mut().zip(d) {
        *a ^= b;
    }
}

/// Feeds `chunk` into an ordered digest (Redis's `mixDigest`).
fn mix(acc: &mut [u8; 20], chunk: &[u8]) {
    let mut buf = acc.to_vec();
    buf.extend_from_slice(chunk);
    *acc = digest_bytes(&buf);
}

/// A digest of a value's content: equal values give equal digests whatever
/// their encoding; members of sets and hashes are combined order-free.
fn value_digest(data: &Data) -> [u8; 20] {
    let mut d = [0u8; 20];
    match data {
        Data::Str(s) => mix(&mut d, s),
        Data::List(l) => {
            for item in l {
                mix(&mut d, item);
            }
        }
        Data::Set(s) => {
            for m in s.members() {
                xor(&mut d, &digest_bytes(&m));
            }
        }
        Data::Hash(h) => {
            for (k, v) in h.map.iter() {
                let mut kv = k.clone();
                kv.push(0);
                kv.extend_from_slice(v);
                xor(&mut d, &digest_bytes(&kv));
            }
        }
        Data::Zset(z) => {
            for (score, m) in z.iter() {
                mix(&mut d, m);
                mix(&mut d, &score.to_bits().to_le_bytes());
            }
        }
        Data::Stream(s) => {
            mix(&mut d, &serde_json::to_vec(s).unwrap_or_default());
        }
    }
    d
}

/// DEBUG DIGEST: all zeros for an empty dataset.
fn dataset_digest(ctx: &mut Ctx) -> [u8; 20] {
    let now = ctx.now;
    let mut out = [0u8; 20];
    for (i, db) in ctx.engine.dbs.iter_mut().enumerate() {
        let live: BTreeSet<Vec<u8>> = db.keys(now).into_iter().collect();
        if live.is_empty() {
            continue;
        }
        mix(&mut out, &(i as u32).to_be_bytes());
        for (key, entry) in db.entries() {
            if !live.contains(key) {
                continue;
            }
            let mut d = digest_bytes(key);
            xor(&mut d, &value_digest(&entry.data));
            if let Some(at) = entry.expires_at {
                mix(&mut d, &at.to_le_bytes());
            }
            xor(&mut out, &d);
        }
    }
    out
}

static HELP: &[&str] = &[
    "CHANGE-REPL-ID",
    "    Change the replication IDs of the instance.",
    "    Dangerous: should be used only for testing the replication subsystem.",
    "DIGEST",
    "    Output a hex signature representing the current DB content.",
    "DIGEST-VALUE <key> [<key> ...]",
    "    Output a hex signature of the values of all the specified keys.",
    "ERROR <string>",
    "    Return a Redis protocol error with <string> as message. Useful for clients",
    "    unit tests to simulate Redis errors.",
    "LOG <message>",
    "    Write <message> to the server log.",
    "OBJECT <key>",
    "    Show low level info about `key` and associated value.",
    "POPULATE <count> [<prefix>] [<size>]",
    "    Create <count> string keys named key:<num>. If <prefix> is specified then",
    "    it is used instead of the 'key' prefix.",
    "PROTOCOL <type>",
    "    Reply with a test value of the specified type. <type> can be: string,",
    "    integer, double, bignum, null, array, set, map, attrib, push, verbatim,",
    "    true, false.",
    "RELOAD",
    "    Save the dataset, flush and reload it back to memory.",
    "SET-ACTIVE-EXPIRE <0|1>",
    "    Setting it to 0 disables expiring keys in background when they are not",
    "    accessed (otherwise the Redis behavior). Setting it to 1 reenables back the",
    "    default.",
    "QUICKLIST-PACKED-THRESHOLD <size>",
    "    Accepted for compatibility; noida-db has no quicklist nodes.",
    "SET-SKIP-CHECKSUM-VALIDATION <0|1>",
    "    Enables or disables checksum checks for RESTORE's payload.",
    "SLEEP <seconds>",
    "    Stop the server for <seconds>. Decimals allowed.",
    "PAUSE-CRON <0|1>",
    "    Stop periodic cron job processing.",
];
