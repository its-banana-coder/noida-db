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
/// - `!` runs the command on both but doesn't compare (ids, versions).
/// - `&N CMD` sends CMD on extra connection N (1-9) without reading the
///   reply (a blocking command), then waits a moment so it is seen first.
/// - `<N` reads the next reply on connection N and compares it.
/// - `=N CMD` runs CMD on connection N and compares the reply.
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
            "SET t 1000",
            "INCRBYFLOAT t 0.1",
            "INCRBYFLOAT t 1e300",
            "SET u 0.1",
            "INCRBYFLOAT u 0.2",
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
    (
        (4, 0),
        &[
            "HSET h a 1 b 2",
            "HSET h a 9 c 3",
            "HGET h a",
            "HGET h nope",
            "HGET nokey a",
            "HMSET h x 1 y 2",
            "HGETALL h",
            "HKEYS h",
            "HVALS h",
            "HDEL h a nope b",
            "HGETALL h",
            "HLEN h",
            "HDEL h c x y",
            "EXISTS h",
            "HDEL h a",
            "HSET h a",
            "@7.0 HMSET h x",
        ],
    ),
    (
        (4, 0),
        &[
            "HSETNX h f v",
            "HSETNX h f w",
            "HEXISTS h f",
            "HEXISTS h g",
            "HEXISTS nokey f",
            "HLEN nokey",
            "HSET h long helloworld",
            "HSTRLEN h long",
            "HSTRLEN h nope",
            "HMGET h f nope long",
            "HMGET nokey a",
            "HGETALL nokey",
            "HKEYS nokey",
            "HVALS nokey",
        ],
    ),
    ((4, 0), &["HSET h z 1 a 2 m 3", "HSET h a 20", "HDEL h z", "HGETALL h", "HKEYS h", "HVALS h"]),
    (
        (4, 0),
        &[
            "HINCRBY h n 5",
            "HINCRBY h n -7",
            "HINCRBY h n x",
            "HSET h s abc",
            "HINCRBY h s 1",
            "HSET h big 9223372036854775807",
            "HINCRBY h big 1",
            "HSET h f 10.50",
            "HINCRBYFLOAT h f 0.1",
            "HINCRBYFLOAT h new 0.25",
            "HINCRBYFLOAT h n 1.5",
            "HINCRBYFLOAT h f x",
            "HINCRBYFLOAT h s 1",
            "HGETALL h",
        ],
    ),
    (
        (7, 0),
        &[
            "HSET h f 1",
            "HINCRBYFLOAT h f inf",
            "HSET h huge 1e308",
            "HINCRBYFLOAT h huge 1e308",
            "HSET h max 1e4932",
            "HINCRBYFLOAT h max 1e4932",
            "SET s 1000",
            "INCRBYFLOAT s 0.1",
        ],
    ),
    (
        (4, 0),
        &[
            "HSET h f v",
            "SET s v",
            "TYPE h",
            "GET h",
            "APPEND h x",
            "INCR h",
            "HGET s f",
            "HSET s f v",
            "HGETALL s",
            "MGET h s",
            "SET h str",
            "TYPE h",
        ],
    ),
    (
        (4, 0),
        &[
            "HSET h a 1 b 2 c 3",
            "HSCAN h 0 COUNT 1",
            "HSCAN h 0 MATCH b*",
            "HSCAN nokey 0",
            "HSCAN h x",
            "HSCAN h 0 COUNT 0",
            "HSCAN h 0 TYPE string",
        ],
    ),
    (
        (6, 2),
        &[
            "HRANDFIELD nokey",
            "HRANDFIELD nokey 3",
            "HSET h a 1 b 2 c 3",
            "HRANDFIELD h 0",
            "HRANDFIELD h 5",
            "HRANDFIELD h 3 WITHVALUES",
            "HRANDFIELD h 1 FOO",
            "HRANDFIELD h x",
            "@7.0 HRANDFIELD h -9223372036854775807 WITHVALUES",
            "!HELLO 3",
            "HRANDFIELD h 3 WITHVALUES",
            "HGETALL h",
        ],
    ),
    ((6, 0), &["!HELLO 3", "SET k v", "GET k", "MGET k nope", "GET nope", "!HELLO 2", "GET nope"]),
    ((7, 0), &["HELLO 4", "HELLO x", "HELLO 3 FOO", "HELLO 3 SETNAME", "HELLO 3 AUTH bob pw"]),
    ((6, 0), &["AUTH secret", "@6.2 AUTH default whatever", "@7.0 AUTH bob pw", "AUTH a b c"]),
    (
        (6, 0),
        &[
            "CLIENT GETNAME",
            "CLIENT SETNAME app",
            "CLIENT GETNAME",
            "@7.0 CLIENT SETNAME",
            "CLIENT",
            "@7.0 CLIENT FOO",
            "@7.2 CLIENT HELP",
        ],
    ),
    (
        (7, 2),
        &[
            "CLIENT SETINFO lib-name mylib",
            "CLIENT SETINFO lib-ver 1.0",
            "CLIENT SETINFO foo x",
            "CLIENT NO-EVICT on",
            "CLIENT NO-EVICT off",
            "CLIENT NO-TOUCH maybe",
            "CLIENT NO-EVICT maybe",
        ],
    ),
    (
        (6, 2),
        &[
            "CLIENT KILL 1.2.3.4:5",
            "CLIENT KILL ID 0",
            "CLIENT KILL ID 99999",
            "CLIENT KILL TYPE foo",
            "CLIENT KILL USER bob",
            "CLIENT KILL SKIPME maybe",
            "CLIENT LIST TYPE foo",
            "CLIENT LIST FOO",
            "CLIENT LIST ID x",
        ],
    ),
    (
        (6, 2),
        &[
            "CLIENT UNBLOCK 99999",
            "CLIENT UNBLOCK 99999 FOO",
            "CLIENT PAUSE 0 FOO",
            "CLIENT PAUSE x",
            "CLIENT PAUSE -1",
            "CLIENT PAUSE 0",
            "CLIENT UNPAUSE",
        ],
    ),
    ((6, 2), &["SELECT 3", "RESET", "GET k"]),
    (
        (7, 0),
        &[
            "COMMAND HELP",
            "COMMAND GETKEYS SET k v",
            "COMMAND GETKEYS MSET a 1 b 2",
            "COMMAND GETKEYS PING",
            "COMMAND GETKEYS NOSUCH x",
            "COMMAND GETKEYS GET",
            "COMMAND GETKEYSANDFLAGS SET k v",
            "COMMAND GETKEYSANDFLAGS SET k v GET",
            "COMMAND GETKEYSANDFLAGS LCS a b",
            "COMMAND INFO nosuch",
            "COMMAND DOCS nosuch",
            "COMMAND LIST FILTERBY FOO x",
            "COMMAND LIST FILTERBY",
        ],
    ),
    // ---- lists ----
    (
        (2, 0),
        &[
            "RPUSH l a b c",
            "LPUSH l z y",
            "LRANGE l 0 -1",
            "LRANGE l 1 2",
            "LRANGE l -2 100",
            "LRANGE l 3 1",
            "LRANGE l 10 20",
            "LRANGE nokey 0 -1",
            "LRANGE l x 1",
            "LLEN l",
            "LLEN nokey",
            "LPOP l",
            "RPOP l",
            "LINDEX l 0",
            "LINDEX l -1",
            "LINDEX l 9",
            "LINDEX l x",
            "LINDEX nokey 0",
            "LSET l 0 A",
            "LSET l 9 x",
            "LSET nokey 0 x",
            "LRANGE l 0 -1",
            "LPUSHX nokey a",
            "RPUSHX l d e",
            "LINSERT l BEFORE b B",
            "LINSERT l AFTER e f",
            "LINSERT l AFTER nope x",
            "LINSERT nokey AFTER a x",
            "LINSERT l MIDDLE a x",
            "LRANGE l 0 -1",
            "SET s v",
            "LPUSH s a",
            "LLEN s",
            "LRANGE s 0 1",
            "TYPE l",
            "LPOP nokey",
        ],
    ),
    (
        (6, 2),
        &[
            "RPUSH l a b c d e",
            "LPOP l 2",
            "RPOP l 2",
            "LPOP l 0",
            "RPOP l 5",
            "EXISTS l",
            "LPOP l 2",
            "@7.0 LPOP l -1",
            "@7.0 LPOP l x",
            "LPOP l 1 2",
        ],
    ),
    (
        (6, 0),
        &[
            "RPUSH l a b a c a d",
            "LPOS l a",
            "LPOS l a RANK 2",
            "LPOS l a RANK -1",
            "LPOS l a COUNT 0",
            "LPOS l a COUNT 2 RANK -1",
            "LPOS l a MAXLEN 1 COUNT 0",
            "LPOS l z",
            "LPOS l z COUNT 1",
            "LPOS nokey a",
            "LPOS nokey a COUNT 1",
            "@7.0 LPOS l a RANK 0",
            "LPOS l a COUNT -1",
            "LPOS l a MAXLEN -1",
            "LPOS l a FOO",
            "LREM l -2 a",
            "LRANGE l 0 -1",
            "LREM l 0 z",
            "LREM nokey 0 z",
            "LREM l x a",
            "LTRIM l 1 -2",
            "LRANGE l 0 -1",
            "LTRIM nokey 0 1",
            "LTRIM l 5 1",
            "EXISTS l",
        ],
    ),
    (
        (6, 2),
        &[
            "RPUSH src a b c",
            "LMOVE src dst LEFT RIGHT",
            "LMOVE src dst RIGHT LEFT",
            "LRANGE dst 0 -1",
            "RPOPLPUSH src dst",
            "EXISTS src",
            "RPOPLPUSH src dst",
            "LMOVE dst dst LEFT RIGHT",
            "LRANGE dst 0 -1",
            "LMOVE dst dst UP RIGHT",
            "SET s v",
            "LMOVE dst s LEFT RIGHT",
            "LMOVE s dst LEFT RIGHT",
            "LRANGE dst 0 -1",
        ],
    ),
    (
        (7, 0),
        &[
            "RPUSH dst c a b",
            "LMPOP 2 nokey dst LEFT",
            "LMPOP 1 dst RIGHT COUNT 10",
            "LMPOP 1 dst RIGHT",
            "LMPOP 0 dst RIGHT",
            "LMPOP 2 dst RIGHT",
            "LMPOP 1 dst UP",
            "LMPOP 1 dst LEFT COUNT 0",
            "LMPOP 1 dst LEFT COUNT 1 COUNT 1",
            "SET s v",
            "LMPOP 1 s LEFT",
            "RPUSH b x y",
            "BLMPOP 0 2 a b RIGHT COUNT 5",
            "BLMPOP 0 0 a LEFT",
            "BLMPOP 0.01 1 a LEFT",
        ],
    ),
    (
        (6, 2),
        &[
            "RPUSH b x y",
            "BLPOP a b 0",
            "BRPOP a b 0",
            "RPUSH b x",
            "BLMOVE b c LEFT LEFT 0",
            "BRPOPLPUSH c d 0",
            "BLPOP a x",
            "BLPOP a -1",
            "BLPOP a 1e300",
            "BLPOP a 0.01",
            "BRPOPLPUSH a b 0.01",
            "SET s v",
            "BLPOP s 0",
            "BLMOVE s x LEFT LEFT 0",
            "BLMOVE b c UP LEFT 0",
        ],
    ),
    // Waiters are served FIFO when data arrives, one element each.
    (
        (2, 0),
        &[
            "&1 BLPOP k 0",
            "&2 BRPOP other k 0",
            "&3 BLPOP k 0",
            "RPUSH k 1 2",
            "<1",
            "<2",
            "EXISTS k",
            "LPUSH k 3",
            "<3",
            "EXISTS k",
        ],
    ),
    (
        (6, 2),
        &[
            "&1 BLMOVE src mid LEFT RIGHT 0",
            "&2 BLPOP mid 0",
            "RPUSH src v",
            "<1",
            "<2",
            "EXISTS src mid",
            "&3 BLPOP k 0",
            "SET k s",
            "DEL k",
            "RPUSH tmp x y",
            "RENAME tmp k",
            "<3",
            "LRANGE k 0 -1",
        ],
    ),
    (
        (7, 0),
        &[
            "&1 BLMPOP 0 2 a b RIGHT COUNT 3",
            "SELECT 1",
            "RPUSH b x y z w",
            "MOVE b 0",
            "<1",
            "SELECT 0",
            "LRANGE b 0 -1",
            "=2 HELLO 3",
            "&2 BLPOP q 0.05",
            "<2",
            "&2 BLPOP q 0",
            "RPUSH q v",
            "<2",
        ],
    ),
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
    let noida = common::start_noida_redis();
    let mut ours = RawClient::connect(noida);
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
            c.run("RESET");
            c.run("FLUSHALL");
        }
        let mut side: std::collections::HashMap<char, (RawClient, RawClient)> = Default::default();
        for line in *script {
            let (line_min, line) = match line.strip_prefix('@') {
                Some(rest) => {
                    let (v, rest) = rest.split_once(' ').unwrap();
                    (parse_version(v), rest)
                }
                None => ((0, 0), *line),
            };
            let (ignore, line) = match line.strip_prefix('!') {
                Some(rest) => (true, rest),
                None => (false, line),
            };
            let (is_set, line) = match line.strip_prefix('~') {
                Some(rest) => (true, rest),
                None => (false, line),
            };
            let conn = match line.as_bytes() {
                [op @ (b'&' | b'<' | b'='), n, ..] => Some((*op, *n as char)),
                _ => None,
            };
            let line = if conn.is_some() { line[2..].trim_start() } else { line };
            let a = args(line);
            let refs: Vec<&[u8]> = a.iter().map(Vec::as_slice).collect();
            let (mut want, mut got) = match conn {
                None => (real.cmd(&refs), ours.cmd(&refs)),
                Some((op, n)) => {
                    let (r, o) = side.entry(n).or_insert_with(|| {
                        (RawClient::connect(reference.addr), RawClient::connect(noida))
                    });
                    match op {
                        b'&' => {
                            r.send(&refs);
                            o.send(&refs);
                            std::thread::sleep(Duration::from_millis(50));
                            continue;
                        }
                        b'<' => (r.read().expect("reply"), o.read().expect("reply")),
                        _ => (r.cmd(&refs), o.cmd(&refs)),
                    }
                }
            };
            if is_set {
                want = unordered(want);
                got = unordered(got);
            }
            if ignore {
                continue;
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

/// COMMAND INFO and COMMAND DOCS for every command and subcommand noida
/// implements must match real Redis exactly. This checks the generated
/// command table and the code that formats it.
#[test]
fn command_introspection_matches_real_redis() {
    let Some(reference) = reference() else {
        eprintln!("SKIPPED: no reference Redis (set NOIDA_REDIS_REF or install redis-server)");
        return;
    };
    let mut real = RawClient::connect(reference.addr);
    let mut ours = RawClient::connect(common::start_noida_redis());
    if server_version(&mut real) < (7, 0) {
        eprintln!("SKIPPED: COMMAND INFO/DOCS changed in Redis 7.0; reference is older");
        return;
    }
    let Value::Array(names) = ours.run("COMMAND LIST") else { panic!("COMMAND LIST") };
    let mut failures = Vec::new();
    let mut compared = 0;
    for name in names {
        let Value::Bulk(name) = name else { panic!() };
        let name = String::from_utf8(name).unwrap();
        let has_subs = !name.contains('|') && {
            let Value::Array(sub) = ours.run(&format!("COMMAND LIST FILTERBY PATTERN {name}|*"))
            else {
                panic!()
            };
            !sub.is_empty()
        };
        for sub in ["INFO", "DOCS"] {
            let line = format!("COMMAND {sub} {name}");
            let (want, got) = (real.run(&line), ours.run(&line));
            // A container's subcommands come in hash order on real Redis;
            // they are compared one by one instead.
            let (want, got) = if has_subs {
                (drop_subcommands(want), drop_subcommands(got))
            } else {
                (want, got)
            };
            compared += 1;
            if want != got {
                failures.push(format!("{line}\n    redis: {want:?}\n    noida: {got:?}"));
            }
        }
    }
    eprintln!("compared {compared} COMMAND INFO/DOCS replies");
    assert!(failures.is_empty(), "{} mismatches:\n  {}", failures.len(), failures.join("\n  "));
}

/// Removes the trailing subcommand list from a COMMAND INFO or DOCS reply.
fn drop_subcommands(v: Value) -> Value {
    match v {
        // INFO: [[name, arity, ..., subcommands]]
        Value::Array(mut outer) if matches!(outer.first(), Some(Value::Array(_))) => {
            if let Some(Value::Array(info)) = outer.first_mut() {
                info.pop();
            }
            Value::Array(outer)
        }
        // DOCS: [name, [k, v, ..., "subcommands", {...}]]
        Value::Array(mut outer) if outer.len() == 2 => {
            if let Some(Value::Array(doc)) = outer.get_mut(1)
                && let Some(i) = doc.iter().position(|x| *x == Value::bulk("subcommands"))
            {
                doc.truncate(i);
            }
            Value::Array(outer)
        }
        other => other,
    }
}
