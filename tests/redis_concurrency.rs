//! Redis concurrency and atomicity tests: real multi-connection races
//! against both a real Redis server and noida-db, checking invariants
//! rather than exact reply sequences (ordering under genuine concurrency
//! isn't deterministic, so these don't diff command-by-command the way
//! `tests/redis_diff.rs` does -- they check that the *outcome* -- "never
//! lost an update", "never oversold", "WATCH aborts on a real race",
//! "only the lock's owner can release it" -- holds on both servers).
//!
//! The reference server is `NOIDA_REDIS_REF=host:port` if set (CI points
//! this at Redis 7.2), otherwise a `redis-server` from PATH started on a
//! free port -- same convention as `tests/redis_diff.rs`.
//!
//! This complements `redis_diff.rs`'s already-extensive command-level
//! coverage (virtually every command family: strings, TTL, hashes,
//! lists, sets, zsets, HyperLogLog, bitmaps, GEO, transactions, WATCH,
//! Lua, streams, pub/sub) with the one thing that suite's sequential,
//! single-connection script model structurally can't exercise: real
//! concurrent clients racing against each other.

mod common;

use std::net::{SocketAddr, TcpListener, ToSocketAddrs};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use common::RawClient;
use noida::redis::resp::Value;

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

/// A reply naming a type, data structure, or status: Redis sends these as
/// Simple Strings (`TYPE`, `SET ... NX`'s "OK") rather than Bulk Strings,
/// so comparisons need to check both.
fn as_text(v: &Value) -> Option<Vec<u8>> {
    match v {
        Value::Bulk(b) => Some(b.clone()),
        Value::Simple(s) => Some(s.clone().into_bytes()),
        _ => None,
    }
}

/// Runs `EVAL script numkeys key... arg...` as a real RESP array, each
/// element its own argument -- not a space-split string, which would
/// break apart a multi-line Lua script into multiple bogus arguments.
fn eval(c: &mut RawClient, script: &str, keys: &[&str], argv: &[&str]) -> Value {
    let mut args: Vec<&[u8]> = vec![b"EVAL", script.as_bytes()];
    let numkeys = keys.len().to_string();
    args.push(numkeys.as_bytes());
    for k in keys {
        args.push(k.as_bytes());
    }
    for a in argv {
        args.push(a.as_bytes());
    }
    c.cmd(&args)
}

// ---------------------------------------------------------------------
// WRONGTYPE matrix: every data structure's own op, tried against every
// OTHER structure's key, must reply WRONGTYPE -- and the key itself must
// come out unchanged (still the right TYPE, still readable).
// ---------------------------------------------------------------------

fn assert_wrongtype_matrix(addr: SocketAddr, label: &str) {
    let mut c = RawClient::connect(addr);

    let setups: &[(&str, &[&str])] = &[
        ("string", &["SET wt:string hello"]),
        ("list", &["RPUSH wt:list a"]),
        ("set", &["SADD wt:set a"]),
        ("hash", &["HSET wt:hash f v"]),
        ("zset", &["ZADD wt:zset 1 a"]),
    ];
    for (_, setup) in setups {
        for cmd in *setup {
            c.run(cmd);
        }
    }

    // (op template with {} standing in for the key, the structure it
    // actually belongs to -- every OTHER structure's key must reject it).
    let ops: &[(&str, &str)] = &[
        ("LRANGE {} 0 -1", "list"),
        ("LPOP {}", "list"),
        ("SMEMBERS {}", "set"),
        ("SADD {} x", "set"),
        ("HGET {} f", "hash"),
        ("HSET {} f v", "hash"),
        ("ZSCORE {} a", "zset"),
        ("ZADD {} 1 a", "zset"),
        ("APPEND {} x", "string"),
        ("GETRANGE {} 0 -1", "string"),
    ];

    let keys: &[(&str, &str)] = &[
        ("wt:string", "string"),
        ("wt:list", "list"),
        ("wt:set", "set"),
        ("wt:hash", "hash"),
        ("wt:zset", "zset"),
    ];

    for (op_template, owner) in ops {
        for (key, key_type) in keys {
            if key_type == owner {
                continue; // the op's own structure -- not a mismatch.
            }
            let cmd = op_template.replace("{}", key);
            let reply = c.run(&cmd);
            match &reply {
                Value::Error(msg) => {
                    assert!(
                        msg.starts_with("WRONGTYPE"),
                        "{label}: `{cmd}` against a {key_type} key replied {msg:?}, expected a WRONGTYPE error"
                    );
                }
                other => panic!(
                    "{label}: `{cmd}` against a {key_type} key replied {other:?}, expected a WRONGTYPE error"
                ),
            }
            // The mismatched op must not have corrupted the key.
            let ty = c.run(&format!("TYPE {key}"));
            assert_eq!(
                as_text(&ty).as_deref(),
                Some(key_type.as_bytes()),
                "{label}: `{cmd}` changed {key}'s TYPE"
            );
        }
    }
}

#[test]
fn wrongtype_matrix_matches_real_redis() {
    let Some(reference) = reference() else {
        println!("SKIPPED: no reference Redis (set NOIDA_REDIS_REF or install redis-server)");
        return;
    };
    let noida_addr = common::start_noida_redis();
    assert_wrongtype_matrix(reference.addr, "redis");
    assert_wrongtype_matrix(noida_addr, "noida-db");
}

// ---------------------------------------------------------------------
// Empty-collection semantics: removing a list/set/hash/zset's last
// element must delete the key entirely, the same way real Redis does.
// ---------------------------------------------------------------------

fn assert_empty_collection_deletes_key(addr: SocketAddr, label: &str) {
    let mut c = RawClient::connect(addr);

    c.run("RPUSH ec:list a");
    c.run("LPOP ec:list");
    assert_eq!(
        c.run("EXISTS ec:list"),
        Value::Integer(0),
        "{label}: emptied list key still exists"
    );

    c.run("SADD ec:set a");
    c.run("SREM ec:set a");
    assert_eq!(c.run("EXISTS ec:set"), Value::Integer(0), "{label}: emptied set key still exists");

    c.run("HSET ec:hash f v");
    c.run("HDEL ec:hash f");
    assert_eq!(
        c.run("EXISTS ec:hash"),
        Value::Integer(0),
        "{label}: emptied hash key still exists"
    );

    c.run("ZADD ec:zset 1 a");
    c.run("ZREM ec:zset a");
    assert_eq!(
        c.run("EXISTS ec:zset"),
        Value::Integer(0),
        "{label}: emptied zset key still exists"
    );
}

#[test]
fn empty_collection_deletes_key_matches_real_redis() {
    let Some(reference) = reference() else {
        println!("SKIPPED: no reference Redis (set NOIDA_REDIS_REF or install redis-server)");
        return;
    };
    let noida_addr = common::start_noida_redis();
    assert_empty_collection_deletes_key(reference.addr, "redis");
    assert_empty_collection_deletes_key(noida_addr, "noida-db");
}

// ---------------------------------------------------------------------
// WATCH must abort a transaction modified by another connection between
// WATCH and EXEC -- a real two-connection race, deterministically
// sequenced (not timing-dependent): connection A's WATCH is guaranteed
// complete before connection B's modification, which is guaranteed
// complete before connection A's EXEC.
// ---------------------------------------------------------------------

fn assert_watch_aborts_on_concurrent_modification(addr: SocketAddr, label: &str) {
    let mut a = RawClient::connect(addr);
    let mut b = RawClient::connect(addr);

    a.run("SET watch:key 10");
    a.run("WATCH watch:key");
    b.run("SET watch:key 5"); // a real modification from a different connection.
    a.run("MULTI");
    a.run("DECR watch:key");
    let exec_reply = a.run("EXEC");
    assert_eq!(
        exec_reply,
        Value::NullArray,
        "{label}: EXEC should abort (nil) after a watched key changed"
    );

    // The unrelated connection's write must still have taken effect.
    assert_eq!(
        b.run("GET watch:key"),
        Value::Bulk(b"5".to_vec()),
        "{label}: the real modification was lost"
    );
}

#[test]
fn watch_aborts_on_concurrent_modification_matches_real_redis() {
    let Some(reference) = reference() else {
        println!("SKIPPED: no reference Redis (set NOIDA_REDIS_REF or install redis-server)");
        return;
    };
    let noida_addr = common::start_noida_redis();
    assert_watch_aborts_on_concurrent_modification(reference.addr, "redis");
    assert_watch_aborts_on_concurrent_modification(noida_addr, "noida-db");
}

// ---------------------------------------------------------------------
// Lock ownership: only the connection holding the right token can
// release a lock, via the standard compare-and-delete Lua pattern.
// ---------------------------------------------------------------------

const UNLOCK_SCRIPT: &str = r#"
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
else
    return 0
end
"#;

fn assert_lock_ownership_enforced(addr: SocketAddr, label: &str) {
    let mut a = RawClient::connect(addr);

    a.run("DEL lock:resource");
    let acquired = a.run("SET lock:resource token-A NX EX 10");
    assert_eq!(
        acquired,
        Value::Simple("OK".to_string()),
        "{label}: first SET NX should acquire the lock"
    );

    let contended = a.run("SET lock:resource token-B NX EX 10");
    assert_eq!(contended, Value::Null, "{label}: a held lock must reject a second NX acquire");

    // The wrong token must not release it.
    let wrong_release = eval(&mut a, UNLOCK_SCRIPT, &["lock:resource"], &["token-B"]);
    assert_eq!(
        wrong_release,
        Value::Integer(0),
        "{label}: the wrong token must not release the lock"
    );
    assert_eq!(
        a.run("GET lock:resource"),
        Value::Bulk(b"token-A".to_vec()),
        "{label}: lock was released by the wrong token"
    );

    // The right token does release it.
    let right_release = eval(&mut a, UNLOCK_SCRIPT, &["lock:resource"], &["token-A"]);
    assert_eq!(
        right_release,
        Value::Integer(1),
        "{label}: the right token should release the lock"
    );
    assert_eq!(
        a.run("EXISTS lock:resource"),
        Value::Integer(0),
        "{label}: lock key should be gone after release"
    );
}

#[test]
fn lock_ownership_enforced_matches_real_redis() {
    let Some(reference) = reference() else {
        println!("SKIPPED: no reference Redis (set NOIDA_REDIS_REF or install redis-server)");
        return;
    };
    let noida_addr = common::start_noida_redis();
    assert_lock_ownership_enforced(reference.addr, "redis");
    assert_lock_ownership_enforced(noida_addr, "noida-db");
}

// ---------------------------------------------------------------------
// Atomic inventory decrement via Lua: check-then-decrement must be one
// atomic step, never oversold.
// ---------------------------------------------------------------------

const DECREMENT_IF_AVAILABLE: &str = r#"
local stock = tonumber(redis.call('GET', KEYS[1]))
if stock == nil then return -1 end
if stock < tonumber(ARGV[1]) then return 0 end
redis.call('DECRBY', KEYS[1], ARGV[1])
return 1
"#;

fn assert_atomic_inventory_decrement(addr: SocketAddr, label: &str) {
    let mut c = RawClient::connect(addr);
    c.run("SET inv:flash 10");

    let ok = eval(&mut c, DECREMENT_IF_AVAILABLE, &["inv:flash"], &["3"]);
    assert_eq!(ok, Value::Integer(1), "{label}: a satisfiable decrement should succeed");
    assert_eq!(
        c.run("GET inv:flash"),
        Value::Bulk(b"7".to_vec()),
        "{label}: stock should now be 7"
    );

    let too_much = eval(&mut c, DECREMENT_IF_AVAILABLE, &["inv:flash"], &["10"]);
    assert_eq!(
        too_much,
        Value::Integer(0),
        "{label}: an unsatisfiable decrement should fail, not partially apply"
    );
    assert_eq!(
        c.run("GET inv:flash"),
        Value::Bulk(b"7".to_vec()),
        "{label}: stock should remain 7 after a failed decrement"
    );
}

#[test]
fn atomic_inventory_decrement_matches_real_redis() {
    let Some(reference) = reference() else {
        println!("SKIPPED: no reference Redis (set NOIDA_REDIS_REF or install redis-server)");
        return;
    };
    let noida_addr = common::start_noida_redis();
    assert_atomic_inventory_decrement(reference.addr, "redis");
    assert_atomic_inventory_decrement(noida_addr, "noida-db");
}

// ---------------------------------------------------------------------
// Real concurrency stress: N threads, each with their own connection,
// racing INCR against a shared counter. The only acceptable final value
// is exactly threads * increments_per_thread -- a single lost update
// from a races condition would under-count.
// ---------------------------------------------------------------------

fn run_concurrent_incr(addr: SocketAddr, threads: usize, increments_per_thread: usize) -> i64 {
    let mut setup = RawClient::connect(addr);
    setup.run("DEL stress:counter");
    drop(setup);

    let barrier = Arc::new(Barrier::new(threads));
    let handles: Vec<_> = (0..threads)
        .map(|_| {
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                let mut c = RawClient::connect(addr);
                barrier.wait();
                for _ in 0..increments_per_thread {
                    c.run("INCR stress:counter");
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }

    let mut c = RawClient::connect(addr);
    match c.run("GET stress:counter") {
        Value::Bulk(b) => String::from_utf8(b).unwrap().parse().unwrap(),
        other => panic!("unexpected reply for GET stress:counter: {other:?}"),
    }
}

#[test]
fn concurrent_incr_never_loses_an_update() {
    let Some(reference) = reference() else {
        println!("SKIPPED: no reference Redis (set NOIDA_REDIS_REF or install redis-server)");
        return;
    };
    let noida_addr = common::start_noida_redis();

    const THREADS: usize = 20;
    const PER_THREAD: usize = 200;
    const EXPECTED: i64 = (THREADS * PER_THREAD) as i64;

    let redis_result = run_concurrent_incr(reference.addr, THREADS, PER_THREAD);
    assert_eq!(
        redis_result, EXPECTED,
        "real Redis itself didn't reach the expected count (test setup issue)"
    );

    let noida_result = run_concurrent_incr(noida_addr, THREADS, PER_THREAD);
    assert_eq!(
        noida_result, EXPECTED,
        "noida-db lost at least one concurrent INCR: {THREADS} threads x {PER_THREAD} each should total {EXPECTED}, got {noida_result}"
    );
}

// ---------------------------------------------------------------------
// Real concurrency stress: the "flash sale" scenario from the user's own
// exercise -- N threads (N > stock) race to atomically decrement a
// shared inventory counter by 1 via Lua. Exactly `stock` of them must
// succeed and exactly `N - stock` must fail; the final stock must be 0.
// "Never 49 or 51."
// ---------------------------------------------------------------------

fn run_flash_sale(addr: SocketAddr, stock: i64, buyers: usize) -> (usize, usize, i64) {
    let mut setup = RawClient::connect(addr);
    setup.run("DEL stress:flash_sale");
    setup.run(&format!("SET stress:flash_sale {stock}"));
    drop(setup);

    let barrier = Arc::new(Barrier::new(buyers));
    let handles: Vec<_> = (0..buyers)
        .map(|_| {
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                let mut c = RawClient::connect(addr);
                barrier.wait();
                eval(&mut c, DECREMENT_IF_AVAILABLE, &["stress:flash_sale"], &["1"])
            })
        })
        .collect();

    let mut succeeded = 0usize;
    let mut failed = 0usize;
    for h in handles {
        match h.join().unwrap() {
            Value::Integer(1) => succeeded += 1,
            Value::Integer(0) => failed += 1,
            other => panic!("unexpected EVAL reply during flash sale: {other:?}"),
        }
    }

    let mut c = RawClient::connect(addr);
    let remaining = match c.run("GET stress:flash_sale") {
        Value::Bulk(b) => String::from_utf8(b).unwrap().parse().unwrap(),
        other => panic!("unexpected reply for GET stress:flash_sale: {other:?}"),
    };
    (succeeded, failed, remaining)
}

#[test]
fn concurrent_flash_sale_never_oversells() {
    let Some(reference) = reference() else {
        println!("SKIPPED: no reference Redis (set NOIDA_REDIS_REF or install redis-server)");
        return;
    };
    let noida_addr = common::start_noida_redis();

    const STOCK: i64 = 50;
    const BUYERS: usize = 120;

    let (r_ok, r_fail, r_remaining) = run_flash_sale(reference.addr, STOCK, BUYERS);
    assert_eq!(
        (r_ok, r_fail, r_remaining),
        (STOCK as usize, BUYERS - STOCK as usize, 0),
        "real Redis itself didn't match the expected outcome (test setup issue)"
    );

    let (n_ok, n_fail, n_remaining) = run_flash_sale(noida_addr, STOCK, BUYERS);
    assert_eq!(
        (n_ok, n_fail, n_remaining),
        (STOCK as usize, BUYERS - STOCK as usize, 0),
        "noida-db oversold or undersold: {BUYERS} buyers racing for {STOCK} stock should give exactly {STOCK} successes, {} failures, 0 remaining -- got {n_ok} successes, {n_fail} failures, {n_remaining} remaining",
        BUYERS - STOCK as usize
    );
}
