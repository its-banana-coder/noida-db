//! Command-level tests against the in-process engine. Expected replies and
//! error strings are Redis 7.2's, byte for byte.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use super::resp::{Value, split_inline};
use super::{Engine, Session};

const START_MS: u64 = 1_700_000_000_000;

struct T {
    engine: Engine,
    session: Session,
    now: Arc<AtomicU64>,
}

impl T {
    fn new() -> T {
        let now = Arc::new(AtomicU64::new(START_MS));
        let clock = now.clone();
        T {
            engine: Engine::with_clock(Arc::new(move || clock.load(Ordering::SeqCst))),
            session: Session::new(1),
            now,
        }
    }

    fn run(&mut self, line: &str) -> Value {
        let args = split_inline(line.as_bytes()).expect("bad test command");
        self.engine.execute(&mut self.session, &args)
    }

    fn advance(&self, ms: u64) {
        self.now.fetch_add(ms, Ordering::SeqCst);
    }
}

fn ok() -> Value {
    Value::ok()
}
fn nil() -> Value {
    Value::Null
}
fn int(n: i64) -> Value {
    Value::Integer(n)
}
fn bulk(s: &str) -> Value {
    Value::bulk(s)
}
fn simple(s: &str) -> Value {
    Value::Simple(s.into())
}
fn err(s: &str) -> Value {
    Value::err(s)
}
fn arr(items: Vec<Value>) -> Value {
    Value::Array(items)
}
fn bulks(items: &[&str]) -> Value {
    arr(items.iter().map(|s| bulk(s)).collect())
}
fn sorted(v: Value) -> Value {
    match v {
        Value::Array(mut items) => {
            items.sort_by_key(|i| format!("{i:?}"));
            Value::Array(items)
        }
        other => other,
    }
}

const NOT_INT: &str = "ERR value is not an integer or out of range";
const SYNTAX: &str = "ERR syntax error";

// ---- connection ----

#[test]
fn ping_and_echo() {
    let mut t = T::new();
    assert_eq!(t.run("PING"), simple("PONG"));
    assert_eq!(t.run("ping hello"), bulk("hello"));
    assert_eq!(t.run("PING a b"), err("ERR wrong number of arguments for 'ping' command"));
    assert_eq!(t.run("ECHO hi"), bulk("hi"));
    assert_eq!(t.run("ECHO"), err("ERR wrong number of arguments for 'echo' command"));
}

#[test]
fn unknown_command_matches_redis_message() {
    let mut t = T::new();
    assert_eq!(
        t.run("FOO a b"),
        err("ERR unknown command 'FOO', with args beginning with: 'a' 'b' ")
    );
    assert_eq!(t.run("foo"), err("ERR unknown command 'foo', with args beginning with: "));
}

#[test]
fn select_isolates_databases() {
    let mut t = T::new();
    assert_eq!(t.run("SET k v0"), ok());
    assert_eq!(t.run("SELECT 1"), ok());
    assert_eq!(t.run("GET k"), nil());
    assert_eq!(t.run("SET k v1"), ok());
    assert_eq!(t.run("DBSIZE"), int(1));
    assert_eq!(t.run("SELECT 0"), ok());
    assert_eq!(t.run("GET k"), bulk("v0"));
    assert_eq!(t.run("SELECT 16"), err("ERR DB index is out of range"));
    assert_eq!(t.run("SELECT x"), err(NOT_INT));
}

#[test]
fn flushdb_and_flushall() {
    let mut t = T::new();
    t.run("SET a 1");
    t.run("SELECT 1");
    t.run("SET b 1");
    assert_eq!(t.run("FLUSHDB"), ok());
    assert_eq!(t.run("DBSIZE"), int(0));
    t.run("SELECT 0");
    assert_eq!(t.run("DBSIZE"), int(1));
    assert_eq!(t.run("FLUSHALL"), ok());
    assert_eq!(t.run("DBSIZE"), int(0));
    assert_eq!(t.run("FLUSHALL ASYNC"), ok());
    assert_eq!(t.run("FLUSHALL LAZY"), err(SYNTAX));
}

// ---- strings ----

#[test]
fn set_and_get() {
    let mut t = T::new();
    assert_eq!(t.run("GET k"), nil());
    assert_eq!(t.run("SET k \"hello world\""), ok());
    assert_eq!(t.run("GET k"), bulk("hello world"));
    assert_eq!(t.run("SET k v2"), ok());
    assert_eq!(t.run("GET k"), bulk("v2"));
}

#[test]
fn set_nx_xx_get() {
    let mut t = T::new();
    assert_eq!(t.run("SET k v XX"), nil());
    assert_eq!(t.run("SET k v NX"), ok());
    assert_eq!(t.run("SET k w NX"), nil());
    assert_eq!(t.run("SET k w XX"), ok());
    assert_eq!(t.run("SET k z GET"), bulk("w"));
    assert_eq!(t.run("SET new z GET"), nil());
    assert_eq!(t.run("SET k v NX XX"), err(SYNTAX));
    assert_eq!(t.run("SET k v BOGUS"), err(SYNTAX));
}

#[test]
fn set_expiry_options() {
    let mut t = T::new();
    assert_eq!(t.run("SET k v EX 10"), ok());
    assert_eq!(t.run("TTL k"), int(10));
    assert_eq!(t.run("SET k v PX 2500"), ok());
    assert_eq!(t.run("PTTL k"), int(2500));
    assert_eq!(t.run("TTL k"), int(3));
    assert_eq!(t.run(&format!("SET k v EXAT {}", START_MS / 1000 + 50)), ok());
    assert_eq!(t.run("TTL k"), int(50));
    assert_eq!(t.run(&format!("SET k v PXAT {}", START_MS + 700)), ok());
    assert_eq!(t.run("PTTL k"), int(700));
    assert_eq!(t.run("SET k v2 KEEPTTL"), ok());
    assert_eq!(t.run("PTTL k"), int(700));
    assert_eq!(t.run("SET k v3"), ok());
    assert_eq!(t.run("TTL k"), int(-1));
    assert_eq!(t.run("SET k v EX 0"), err("ERR invalid expire time in 'set' command"));
    assert_eq!(t.run("SET k v PX -5"), err("ERR invalid expire time in 'set' command"));
    assert_eq!(t.run("SET k v EX abc"), err(NOT_INT));
    assert_eq!(t.run("SET k v EX 10 PX 10"), err(SYNTAX));
    assert_eq!(t.run("SET k v EX 10 KEEPTTL"), err(SYNTAX));
    assert_eq!(t.run("SET k v EX"), err(SYNTAX));
}

#[test]
fn keys_expire_lazily() {
    let mut t = T::new();
    t.run("SET k v PX 100");
    // Redis keeps a key until the clock passes its expiry time.
    t.advance(100);
    assert_eq!(t.run("GET k"), bulk("v"));
    t.advance(1);
    assert_eq!(t.run("GET k"), nil());
    assert_eq!(t.run("EXISTS k"), int(0));
    assert_eq!(t.run("DBSIZE"), int(0));
}

#[test]
fn setnx_setex_psetex_getset_getdel() {
    let mut t = T::new();
    assert_eq!(t.run("SETNX k a"), int(1));
    assert_eq!(t.run("SETNX k b"), int(0));
    assert_eq!(t.run("GETSET k c"), bulk("a"));
    assert_eq!(t.run("GETDEL k"), bulk("c"));
    assert_eq!(t.run("GETDEL k"), nil());
    assert_eq!(t.run("SETEX k 5 v"), ok());
    assert_eq!(t.run("TTL k"), int(5));
    assert_eq!(t.run("SETEX k 0 v"), err("ERR invalid expire time in 'setex' command"));
    assert_eq!(t.run("PSETEX k 1500 v"), ok());
    assert_eq!(t.run("PTTL k"), int(1500));
}

#[test]
fn getex() {
    let mut t = T::new();
    assert_eq!(t.run("GETEX k"), nil());
    t.run("SET k v");
    assert_eq!(t.run("GETEX k EX 20"), bulk("v"));
    assert_eq!(t.run("TTL k"), int(20));
    assert_eq!(t.run("GETEX k PERSIST"), bulk("v"));
    assert_eq!(t.run("TTL k"), int(-1));
    assert_eq!(t.run("GETEX k EX 1 PX 1"), err(SYNTAX));
    assert_eq!(t.run("GETEX k EX 0"), err("ERR invalid expire time in 'getex' command"));
}

#[test]
fn mset_mget_msetnx() {
    let mut t = T::new();
    assert_eq!(t.run("MSET a 1 b 2"), ok());
    assert_eq!(t.run("MGET a b c"), arr(vec![bulk("1"), bulk("2"), nil()]));
    assert_eq!(t.run("MSET a 1 b"), err("ERR wrong number of arguments for 'mset' command"));
    assert_eq!(t.run("MSETNX c 3 a 9"), int(0));
    assert_eq!(t.run("GET c"), nil());
    assert_eq!(t.run("MSETNX c 3 d 4"), int(1));
    assert_eq!(t.run("MGET c d"), bulks(&["3", "4"]));
}

#[test]
fn incr_family() {
    let mut t = T::new();
    assert_eq!(t.run("INCR n"), int(1));
    assert_eq!(t.run("INCRBY n 10"), int(11));
    assert_eq!(t.run("DECR n"), int(10));
    assert_eq!(t.run("DECRBY n 4"), int(6));
    assert_eq!(t.run("GET n"), bulk("6"));
    t.run("SET n 9223372036854775807");
    assert_eq!(t.run("INCR n"), err("ERR increment or decrement would overflow"));
    t.run("SET s abc");
    assert_eq!(t.run("INCR s"), err(NOT_INT));
    // Redis rejects leading zeros, '+' signs and spaces.
    t.run("SET s 007");
    assert_eq!(t.run("INCR s"), err(NOT_INT));
    t.run("SET s +1");
    assert_eq!(t.run("INCR s"), err(NOT_INT));
    assert_eq!(t.run("INCRBY n x"), err(NOT_INT));
}

#[test]
fn incr_keeps_ttl() {
    let mut t = T::new();
    t.run("SET n 1 EX 100");
    t.run("INCR n");
    assert_eq!(t.run("TTL n"), int(100));
}

#[test]
fn incrbyfloat() {
    let mut t = T::new();
    t.run("SET f 10.50");
    assert_eq!(t.run("INCRBYFLOAT f 0.1"), bulk("10.6"));
    assert_eq!(t.run("INCRBYFLOAT f -5"), bulk("5.6"));
    t.run("SET f 5.0e3");
    assert_eq!(t.run("INCRBYFLOAT f 2.0e2"), bulk("5200"));
    assert_eq!(t.run("INCRBYFLOAT new 3"), bulk("3"));
    t.run("SET s abc");
    assert_eq!(t.run("INCRBYFLOAT s 1"), err("ERR value is not a valid float"));
    assert_eq!(t.run("INCRBYFLOAT f x"), err("ERR value is not a valid float"));
    assert_eq!(t.run("INCRBYFLOAT f inf"), err("ERR increment would produce NaN or Infinity"));
}

#[test]
fn append_strlen_ranges() {
    let mut t = T::new();
    assert_eq!(t.run("APPEND k Hello"), int(5));
    assert_eq!(t.run("APPEND k \" World\""), int(11));
    assert_eq!(t.run("STRLEN k"), int(11));
    assert_eq!(t.run("STRLEN missing"), int(0));
    t.run("SET s \"This is a string\"");
    assert_eq!(t.run("GETRANGE s 0 3"), bulk("This"));
    assert_eq!(t.run("GETRANGE s -3 -1"), bulk("ing"));
    assert_eq!(t.run("GETRANGE s 0 -1"), bulk("This is a string"));
    assert_eq!(t.run("GETRANGE s 10 100"), bulk("string"));
    assert_eq!(t.run("GETRANGE s 5 2"), bulk(""));
    assert_eq!(t.run("GETRANGE missing 0 -1"), bulk(""));
    assert_eq!(t.run("SUBSTR s 0 3"), bulk("This"));
    t.run("SET h \"Hello World\"");
    assert_eq!(t.run("SETRANGE h 6 Redis"), int(11));
    assert_eq!(t.run("GET h"), bulk("Hello Redis"));
    assert_eq!(t.run("SETRANGE z 3 ab"), int(5));
    assert_eq!(t.run("GET z"), bulk("\0\0\0ab"));
    assert_eq!(t.run("SETRANGE h -1 x"), err("ERR offset is out of range"));
    assert_eq!(t.run("SETRANGE empty 0 \"\""), int(0));
    assert_eq!(t.run("EXISTS empty"), int(0));
}

#[test]
fn lcs() {
    let mut t = T::new();
    t.run("MSET key1 ohmytext key2 mynewtext");
    assert_eq!(t.run("LCS key1 key2"), bulk("mytext"));
    assert_eq!(t.run("LCS key1 key2 LEN"), int(6));
}

// ---- generic key commands ----

#[test]
fn del_exists_type() {
    let mut t = T::new();
    t.run("MSET a 1 b 2");
    assert_eq!(t.run("EXISTS a b missing a"), int(3));
    assert_eq!(t.run("TYPE a"), simple("string"));
    assert_eq!(t.run("TYPE missing"), simple("none"));
    assert_eq!(t.run("DEL a missing"), int(1));
    assert_eq!(t.run("UNLINK b"), int(1));
    assert_eq!(t.run("EXISTS a b"), int(0));
}

#[test]
fn expire_ttl_persist() {
    let mut t = T::new();
    assert_eq!(t.run("TTL k"), int(-2));
    assert_eq!(t.run("EXPIRE k 10"), int(0));
    t.run("SET k v");
    assert_eq!(t.run("TTL k"), int(-1));
    assert_eq!(t.run("EXPIRE k 10"), int(1));
    assert_eq!(t.run("TTL k"), int(10));
    assert_eq!(t.run("PEXPIRE k 1500"), int(1));
    assert_eq!(t.run("PTTL k"), int(1500));
    // TTL rounds to the nearest second.
    assert_eq!(t.run("TTL k"), int(2));
    assert_eq!(t.run("EXPIRETIME k"), int(((START_MS + 1500 + 500) / 1000) as i64));
    assert_eq!(t.run("PEXPIRETIME k"), int((START_MS + 1500) as i64));
    assert_eq!(t.run("PERSIST k"), int(1));
    assert_eq!(t.run("PERSIST k"), int(0));
    assert_eq!(t.run("EXPIRETIME k"), int(-1));
    assert_eq!(t.run("EXPIRETIME missing"), int(-2));
    assert_eq!(t.run("EXPIRE k abc"), err(NOT_INT));
}

#[test]
fn expire_in_the_past_deletes() {
    let mut t = T::new();
    t.run("SET k v");
    assert_eq!(t.run("EXPIRE k -1"), int(1));
    assert_eq!(t.run("EXISTS k"), int(0));
    t.run("SET k v");
    assert_eq!(t.run("EXPIREAT k 1"), int(1));
    assert_eq!(t.run("EXISTS k"), int(0));
    t.run("SET k v");
    assert_eq!(t.run(&format!("PEXPIREAT k {}", START_MS + 10)), int(1));
    assert_eq!(t.run("PTTL k"), int(10));
}

#[test]
fn expire_nx_xx_gt_lt() {
    let mut t = T::new();
    t.run("SET k v");
    assert_eq!(t.run("EXPIRE k 10 XX"), int(0));
    assert_eq!(t.run("EXPIRE k 10 NX"), int(1));
    assert_eq!(t.run("EXPIRE k 20 NX"), int(0));
    assert_eq!(t.run("EXPIRE k 5 GT"), int(0));
    assert_eq!(t.run("EXPIRE k 50 GT"), int(1));
    assert_eq!(t.run("EXPIRE k 60 LT"), int(0));
    assert_eq!(t.run("EXPIRE k 30 LT"), int(1));
    assert_eq!(t.run("TTL k"), int(30));
    assert_eq!(
        t.run("EXPIRE k 30 NX XX"),
        err("ERR NX and XX, GT or LT options at the same time are not compatible")
    );
    assert_eq!(
        t.run("EXPIRE k 30 GT LT"),
        err("ERR GT and LT options at the same time are not compatible")
    );
    assert_eq!(t.run("EXPIRE k 30 FOO"), err("ERR Unsupported option FOO"));
    // A key without a TTL counts as an infinite TTL for GT/LT.
    t.run("SET p v");
    assert_eq!(t.run("EXPIRE p 10 GT"), int(0));
    assert_eq!(t.run("EXPIRE p 10 LT"), int(1));
}

#[test]
fn keys_glob_patterns() {
    let mut t = T::new();
    t.run("MSET hello 1 hallo 1 hxllo 1 hllo 1 heeeello 1 h*llo 1");
    // '?' matches any byte, including a literal '*'.
    assert_eq!(sorted(t.run("KEYS h?llo")), sorted(bulks(&["hello", "hallo", "hxllo", "h*llo"])));
    assert_eq!(
        sorted(t.run("KEYS h*llo")),
        sorted(bulks(&["hello", "hallo", "hxllo", "hllo", "heeeello", "h*llo"]))
    );
    assert_eq!(sorted(t.run("KEYS h[ae]llo")), sorted(bulks(&["hello", "hallo"])));
    assert_eq!(sorted(t.run("KEYS h[^e]llo")), sorted(bulks(&["hallo", "hxllo", "h*llo"])));
    assert_eq!(sorted(t.run("KEYS h[a-b]llo")), sorted(bulks(&["hallo"])));
    assert_eq!(t.run("KEYS h\\*llo"), bulks(&["h*llo"]));
}

#[test]
fn bare_star_matches_the_empty_key() {
    let mut t = T::new();
    t.run("SET \"\" v");
    assert_eq!(t.run("KEYS *"), bulks(&[""]));
}

#[test]
fn rename_and_renamenx() {
    let mut t = T::new();
    assert_eq!(t.run("RENAME a b"), err("ERR no such key"));
    t.run("SET a 1 EX 100");
    t.run("SET c 3");
    assert_eq!(t.run("RENAME a b"), ok());
    assert_eq!(t.run("GET b"), bulk("1"));
    assert_eq!(t.run("TTL b"), int(100));
    assert_eq!(t.run("RENAMENX b c"), int(0));
    assert_eq!(t.run("RENAMENX b d"), int(1));
    assert_eq!(t.run("RENAME d d"), ok());
    assert_eq!(t.run("RENAMENX missing x"), err("ERR no such key"));
}

#[test]
fn copy_and_move() {
    let mut t = T::new();
    t.run("SET a 1");
    assert_eq!(t.run("COPY a b"), int(1));
    assert_eq!(t.run("COPY a b"), int(0));
    assert_eq!(t.run("COPY a b REPLACE"), int(1));
    assert_eq!(t.run("COPY a x DB 3"), int(1));
    assert_eq!(t.run("MOVE a 2"), int(1));
    assert_eq!(t.run("EXISTS a"), int(0));
    t.run("SELECT 2");
    assert_eq!(t.run("GET a"), bulk("1"));
    t.run("SELECT 3");
    assert_eq!(t.run("GET x"), bulk("1"));
}

#[test]
fn scan_visits_every_key() {
    let mut t = T::new();
    for i in 0..25 {
        t.run(&format!("SET key:{i} v"));
    }
    t.run("SET other v");
    let mut cursor = "0".to_string();
    let mut seen = Vec::new();
    loop {
        let Value::Array(reply) = t.run(&format!("SCAN {cursor} MATCH key:* COUNT 7")) else {
            panic!("expected array")
        };
        let Value::Bulk(next) = &reply[0] else { panic!() };
        let Value::Array(keys) = &reply[1] else { panic!() };
        seen.extend(keys.iter().cloned());
        cursor = String::from_utf8(next.clone()).unwrap();
        if cursor == "0" {
            break;
        }
    }
    seen.sort_by_key(|v| format!("{v:?}"));
    seen.dedup();
    assert_eq!(seen.len(), 25);
    assert_eq!(t.run("SCAN x"), err("ERR invalid cursor"));
}

#[test]
fn randomkey() {
    let mut t = T::new();
    assert_eq!(t.run("RANDOMKEY"), nil());
    t.run("SET only v");
    assert_eq!(t.run("RANDOMKEY"), bulk("only"));
}

#[test]
fn wrong_arity_uses_lowercase_name() {
    let mut t = T::new();
    assert_eq!(t.run("GET"), err("ERR wrong number of arguments for 'get' command"));
    assert_eq!(t.run("SeT k"), err("ERR wrong number of arguments for 'set' command"));
}
