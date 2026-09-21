//! Differential tests: every script runs against real Redis and noida, and
//! every reply must be identical.
//!
//! The reference server is `NOIDA_REDIS_REF=host:port` if set (CI points this
//! at Redis 7.2), otherwise a `redis-server` from PATH started on a free port.
//! Scripts newer than the reference server's version are skipped and counted.

mod common;

use std::net::{SocketAddr, TcpListener, ToSocketAddrs};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use common::RawClient;
use noida::redis::resp::Value;

/// (minimum Redis version, script). Line prefixes:
/// - `~` compares the reply as an unordered set (KEYS and similar).
/// - `@X.Y ` compares only if the reference is at least X.Y (error wording
///   that changed since older Redis). The command still runs on both.
const SCRIPTS: &[((u32, u32), &[&str])] = &[
    ((2, 0), &["PING", "PING hi", "ECHO x", "PING a b", "ECHO"]),
    ((2, 0), &["GET k", "SET k v", "GET k", "SET k w", "GET k", "GET", "SET k"]),
    (
        (2, 6),
        &[
            "SET k v XX",
            "SET k v NX",
            "SET k w NX",
            "SET k w XX",
            "GET k",
            "SET k v NX XX",
            "SET k v BOGUS",
        ],
    ),
    (
        (2, 6),
        &[
            "SET k v EX 100",
            "TTL k",
            "SET k v PX 5000",
            "TTL k",
            "SET k v",
            "TTL k",
            "@7.0 SET k v EX 0",
            "SET k v EX abc",
            "SET k v EX 1 PX 1",
            "SET k v EX",
        ],
    ),
    ((6, 0), &["SET k v EX 100", "SET k v2 KEEPTTL", "TTL k", "SET k v3 EX 5 KEEPTTL"]),
    (
        (6, 2),
        &[
            "SET k a",
            "SET k b GET",
            "SET nope b GET",
            "GETDEL k",
            "GETDEL k",
            "SET k v",
            "GETEX k EX 50",
            "TTL k",
            "GETEX k PERSIST",
            "TTL k",
            "GETEX k EX 1 PX 1",
            "GETEX k EX 0",
            "GETEX missing",
        ],
    ),
    (
        (2, 0),
        &[
            "SETNX k a",
            "SETNX k b",
            "GETSET k c",
            "GET k",
            "SETEX k 30 v",
            "TTL k",
            "@7.0 SETEX k 0 v",
            "SETEX k x v",
            "@7.0 PSETEX k 0 v",
        ],
    ),
    (
        (2, 0),
        &[
            "MSET a 1 b 2",
            "MGET a b c",
            "@7.0 MSET a 1 b",
            "MSETNX c 3 a 9",
            "GET c",
            "MSETNX c 3 d 4",
            "MGET c d",
        ],
    ),
    (
        (2, 0),
        &[
            "INCR n",
            "INCRBY n 10",
            "DECR n",
            "DECRBY n 4",
            "GET n",
            "SET n 9223372036854775807",
            "INCR n",
            "SET s abc",
            "INCR s",
            "SET s 007",
            "INCR s",
            "SET s +1",
            "INCR s",
            "INCRBY n x",
            "SET n -9223372036854775808",
            "DECR n",
            "@7.0 DECRBY n -9223372036854775808",
        ],
    ),
    (
        (2, 6),
        &[
            "SET f 10.50",
            "INCRBYFLOAT f 0.1",
            "INCRBYFLOAT f -5",
            "SET f 5.0e3",
            "INCRBYFLOAT f 2.0e2",
            "INCRBYFLOAT new 3",
            "SET g 0.1",
            "INCRBYFLOAT g 0.2",
            "SET s abc",
            "INCRBYFLOAT s 1",
            "INCRBYFLOAT f x",
            "INCRBYFLOAT f inf",
        ],
    ),
    (
        (2, 2),
        &[
            "APPEND k Hello",
            "APPEND k World",
            "STRLEN k",
            "STRLEN missing",
            "SET s Thisisastring",
            "GETRANGE s 0 3",
            "GETRANGE s -3 -1",
            "GETRANGE s 0 -1",
            "GETRANGE s 10 100",
            "GETRANGE s 5 2",
            "GETRANGE s -1 -5",
            "GETRANGE missing 0 -1",
            "SUBSTR s 0 3",
            "SETRANGE s 4 XX",
            "GET s",
            "SETRANGE z 3 ab",
            "GET z",
            "SETRANGE s -1 x",
            "GETRANGE s a 1",
        ],
    ),
    (
        (7, 0),
        &[
            "MSET key1 ohmytext key2 mynewtext",
            "LCS key1 key2",
            "LCS key1 key2 LEN",
            "LCS key1 key2 IDX",
            "LCS key1 key2 IDX MINMATCHLEN 4 WITHMATCHLEN",
            "LCS key1 key2 LEN IDX",
            "LCS key1 key2 BOGUS",
            "LCS missing1 missing2",
        ],
    ),
    (
        (2, 0),
        &[
            "MSET a 1 b 2",
            "EXISTS a b missing a",
            "TYPE a",
            "TYPE missing",
            "DEL a missing",
            "UNLINK b",
            "EXISTS a b",
            "TOUCH a",
        ],
    ),
    (
        (2, 0),
        &[
            "TTL k",
            "EXPIRE k 10",
            "SET k v",
            "TTL k",
            "EXPIRE k 100",
            "TTL k",
            "PEXPIRE k 50000",
            "TTL k",
            "PERSIST k",
            "PERSIST k",
            "TTL k",
            "EXPIRE k abc",
            "EXPIRE k -1",
            "EXISTS k",
            "SET k v",
            "EXPIREAT k 1",
            "EXISTS k",
        ],
    ),
    (
        (7, 0),
        &[
            "SET k v",
            "EXPIRE k 10 XX",
            "EXPIRE k 10 NX",
            "EXPIRE k 20 NX",
            "EXPIRE k 5 GT",
            "EXPIRE k 50 GT",
            "EXPIRE k 60 LT",
            "EXPIRE k 30 LT",
            "TTL k",
            "EXPIRE k 30 NX XX",
            "EXPIRE k 30 GT LT",
            "EXPIRE k 30 FOO",
            "SET p v",
            "EXPIRE p 10 GT",
            "EXPIRE p 10 LT",
            "EXPIRETIME missing",
            "PERSIST p",
            "EXPIRETIME p",
        ],
    ),
    (
        (2, 0),
        &[
            "MSET hello 1 hallo 1 hxllo 1 hllo 1 heeeello 1 h*llo 1",
            "~KEYS h?llo",
            "~KEYS h*llo",
            "~KEYS h[ae]llo",
            "~KEYS h[^e]llo",
            "~KEYS h[a-b]llo",
            "KEYS h\\*llo",
            "~KEYS *",
            "KEYS nomatch*",
        ],
    ),
    (
        (2, 0),
        &[
            "RENAME a b",
            "SET a 1",
            "EXPIRE a 100",
            "SET c 3",
            "RENAME a b",
            "GET b",
            "TTL b",
            "RENAMENX b c",
            "RENAMENX b d",
            "RENAME d d",
            "RENAMENX d d",
            "RENAMENX missing x",
        ],
    ),
    (
        (6, 2),
        &[
            "SET a 1",
            "COPY a b",
            "COPY a b",
            "COPY a b REPLACE",
            "COPY a x DB 3",
            "COPY a a",
            "COPY a b DB 99",
            "COPY a b BOGUS",
            "COPY missing z",
        ],
    ),
    (
        (2, 0),
        &[
            "SET a 1",
            "MOVE a 2",
            "EXISTS a",
            "MOVE a 2",
            "MOVE missing 2",
            "SET b 1",
            "MOVE b 0",
            "@7.0 MOVE b 99",
            "@7.0 MOVE b x",
            "SELECT 2",
            "GET a",
            "SELECT 0",
        ],
    ),
    (
        (2, 0),
        &[
            "SET k v0",
            "SELECT 1",
            "GET k",
            "SET k v1",
            "DBSIZE",
            "SELECT 0",
            "GET k",
            "@7.0 SELECT 16",
            "@7.0 SELECT x",
            "@7.0 SELECT -1",
        ],
    ),
    (
        (4, 0),
        &[
            "SET a 1",
            "SELECT 1",
            "SET b 2",
            "SWAPDB 0 1",
            "GET b",
            "SELECT 0",
            "GET b",
            "SWAPDB 0 99",
            "SWAPDB x 1",
            "SWAPDB 0 y",
        ],
    ),
    (
        (4, 0),
        &[
            "SET a 1",
            "FLUSHDB",
            "DBSIZE",
            "SET a 1",
            "FLUSHALL ASYNC",
            "DBSIZE",
            "FLUSHALL LAZY",
            "FLUSHDB SYNC ASYNC",
        ],
    ),
    (
        (2, 8),
        &[
            "SET a 1",
            "SCAN 0 COUNT 100",
            "SCAN x",
            "SCAN 0 COUNT 0",
            "SCAN 0 COUNT x",
            "SCAN 0 BOGUS 1",
        ],
    ),
    ((7, 0), &["FOO a b", "foo"]),
];

fn parse_version(v: &str) -> (u32, u32) {
    let mut it = v.split('.').map(|p| p.parse().unwrap_or(0));
    (it.next().unwrap_or(0), it.next().unwrap_or(0))
}

struct Reference {
    addr: SocketAddr,
    _child: Option<ChildGuard>,
}

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn reference() -> Option<Reference> {
    if let Ok(addr) = std::env::var("NOIDA_REDIS_REF") {
        let addr = addr.to_socket_addrs().ok()?.next()?;
        return Some(Reference { addr, _child: None });
    }
    let port = TcpListener::bind("127.0.0.1:0").ok()?.local_addr().ok()?.port();
    let child = Command::new("redis-server")
        .args(["--port", &port.to_string(), "--save", "", "--appendonly", "no"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    let guard = ChildGuard(child);
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while std::net::TcpStream::connect(addr).is_err() {
        if Instant::now() > deadline {
            return None;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    Some(Reference { addr, _child: Some(guard) })
}

fn server_version(c: &mut RawClient) -> (u32, u32) {
    let Value::Bulk(info) = c.run("INFO server") else { panic!("INFO failed") };
    let info = String::from_utf8(info).unwrap();
    let line =
        info.lines().find_map(|l| l.strip_prefix("redis_version:")).expect("redis_version in INFO");
    parse_version(line.trim())
}

fn unordered(v: Value) -> Value {
    match v {
        Value::Array(mut items) => {
            items.sort_by_key(|i| format!("{i:?}"));
            Value::Array(items)
        }
        other => other,
    }
}

/// Splits a script line on spaces only, so `h\*llo` keeps its backslash.
fn args(line: &str) -> Vec<Vec<u8>> {
    line.split(' ').filter(|s| !s.is_empty()).map(|s| s.as_bytes().to_vec()).collect()
}

#[test]
fn replies_match_real_redis() {
    let Some(reference) = reference() else {
        eprintln!("SKIPPED: no reference Redis (set NOIDA_REDIS_REF or install redis-server)");
        return;
    };
    let mut real = RawClient::connect(reference.addr);
    let mut ours = RawClient::connect(common::start_noida_redis());
    let version = server_version(&mut real);

    let mut failures = Vec::new();
    let (mut ran, mut skipped) = (0, 0);
    let (mut lines_compared, mut lines_skipped) = (0, 0);
    for (min, script) in SCRIPTS {
        if version < *min {
            skipped += 1;
            continue;
        }
        ran += 1;
        for c in [&mut real, &mut ours] {
            c.run("SELECT 0");
            c.run("FLUSHALL");
        }
        for line in *script {
            let (line_min, line) = match line.strip_prefix('@') {
                Some(rest) => {
                    let (v, rest) = rest.split_once(' ').unwrap();
                    (parse_version(v), rest)
                }
                None => ((0, 0), *line),
            };
            let (is_set, line) = match line.strip_prefix('~') {
                Some(rest) => (true, rest),
                None => (false, line),
            };
            let a = args(line);
            let refs: Vec<&[u8]> = a.iter().map(Vec::as_slice).collect();
            let (mut want, mut got) = (real.cmd(&refs), ours.cmd(&refs));
            if is_set {
                want = unordered(want);
                got = unordered(got);
            }
            if version < line_min {
                lines_skipped += 1;
                continue;
            }
            lines_compared += 1;
            if want != got {
                failures.push(format!("{line}\n    redis: {want:?}\n    noida: {got:?}"));
            }
        }
    }
    eprintln!(
        "reference Redis {}.{}: ran {ran} scripts ({lines_compared} replies compared), \
         skipped {skipped} scripts and {lines_skipped} lines needing a newer version",
        version.0, version.1
    );
    assert!(
        failures.is_empty(),
        "{} mismatches with real Redis:\n  {}",
        failures.len(),
        failures.join("\n  ")
    );
}
