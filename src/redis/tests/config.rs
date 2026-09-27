//! CONFIG GET/SET.

use super::*;

/// The value CONFIG GET reports for one parameter. The reply is a map,
/// which RESP2 clients see as a flat array.
fn one(t: &mut T, name: &str) -> String {
    match t.run(&format!("CONFIG GET {name}")) {
        Value::Map(pairs) if pairs.len() == 1 => match &pairs[0].1 {
            Value::Bulk(v) => String::from_utf8_lossy(v).into_owned(),
            other => panic!("{other:?}"),
        },
        other => panic!("{other:?}"),
    }
}

#[test]
fn get_and_set_parameters() {
    let mut t = T::new();
    assert_eq!(one(&mut t, "maxmemory"), "0");
    assert_eq!(one(&mut t, "databases"), "16");
    assert_eq!(one(&mut t, "appendfsync"), "everysec");
    assert_eq!(t.run("CONFIG GET nosuchparam"), Value::Map(vec![]));
    assert_eq!(t.run("CONFIG SET maxmemory 100mb"), ok());
    assert_eq!(one(&mut t, "maxmemory"), "104857600");
    assert_eq!(t.run("CONFIG SET maxmemory 2g"), ok());
    assert_eq!(one(&mut t, "maxmemory"), "2000000000");
    // Aliases are one value under two names.
    assert_eq!(t.run("CONFIG SET hash-max-ziplist-entries 64"), ok());
    assert_eq!(one(&mut t, "hash-max-listpack-entries"), "64");
    assert_eq!(t.run("CONFIG SET hash-max-listpack-entries 300"), ok());
    assert_eq!(one(&mut t, "hash-max-ziplist-entries"), "300");
    // Several parameters at once, and a percent value.
    assert_eq!(t.run("CONFIG SET timeout 100 maxmemory-clients 50%"), ok());
    assert_eq!(one(&mut t, "timeout"), "100");
    assert_eq!(one(&mut t, "maxmemory-clients"), "50%");
    // Patterns.
    let Value::Map(pairs) = t.run("CONFIG GET maxmemory-*") else { panic!() };
    assert_eq!(pairs.len(), 4);
    assert_eq!(t.run("CONFIG RESETSTAT"), ok());
    assert_eq!(t.run("CONFIG REWRITE"), err("ERR The server is running without a config file"));
}

#[test]
fn set_rejects_what_redis_rejects() {
    let mut t = T::new();
    assert_eq!(
        t.run("CONFIG SET nosuchparam 1"),
        err("ERR Unknown option or number of arguments for CONFIG SET - 'nosuchparam'")
    );
    assert_eq!(
        t.run("CONFIG SET databases 32"),
        err(
            "ERR CONFIG SET failed (possibly related to argument 'databases') - can't set immutable config"
        )
    );
    assert_eq!(
        t.run("CONFIG SET timeout 1 timeout 2"),
        err("ERR CONFIG SET failed (possibly related to argument 'timeout') - duplicate parameter")
    );
    assert_eq!(
        t.run("CONFIG SET timeout notanumber"),
        err(
            "ERR CONFIG SET failed (possibly related to argument 'timeout') - argument couldn't be parsed into an integer"
        )
    );
    assert_eq!(
        t.run("CONFIG SET timeout -1"),
        err(
            "ERR CONFIG SET failed (possibly related to argument 'timeout') - argument must be between 0 and 2147483647 inclusive"
        )
    );
    assert_eq!(
        t.run("CONFIG SET appendonly maybe"),
        err(
            "ERR CONFIG SET failed (possibly related to argument 'appendonly') - argument must be 'yes' or 'no'"
        )
    );
    assert_eq!(
        t.run("CONFIG SET maxmemory-policy nosuchpolicy"),
        err(
            "ERR CONFIG SET failed (possibly related to argument 'maxmemory-policy') - argument(s) must be one of the following: volatile-lru, volatile-lfu, volatile-random, volatile-ttl, allkeys-lru, allkeys-lfu, allkeys-random, noeviction"
        )
    );
    assert_eq!(
        t.run("CONFIG SET timeout"),
        err("ERR wrong number of arguments for 'config|set' command")
    );
    assert_eq!(t.run("CONFIG SET timeout 5 tcp-keepalive"), err(SYNTAX));
    // Nothing is applied when one pair is bad.
    assert_eq!(one(&mut t, "timeout"), "0");
}

#[test]
fn special_parameters() {
    let mut t = T::new();
    assert_eq!(t.run("CONFIG SET notify-keyspace-events KEA"), ok());
    assert_eq!(one(&mut t, "notify-keyspace-events"), "AKE");
    assert_eq!(
        t.run("CONFIG SET notify-keyspace-events Q"),
        err(
            "ERR CONFIG SET failed (possibly related to argument 'notify-keyspace-events') - Invalid event class character. Use 'Ag$lshzxeKEtmdn'."
        )
    );
    assert_eq!(t.run("CONFIG SET save \"900 1\""), ok());
    assert_eq!(one(&mut t, "save"), "900 1");
    assert_eq!(
        t.run("CONFIG SET save \"900 x\""),
        err(
            "ERR CONFIG SET failed (possibly related to argument 'save') - Invalid save parameters"
        )
    );
    // unixsocketperm is one of Redis's immutable parameters.
    assert_eq!(
        t.run("CONFIG SET unixsocketperm 700"),
        err(
            "ERR CONFIG SET failed (possibly related to argument 'unixsocketperm') - can't set immutable config"
        )
    );
    assert_eq!(one(&mut t, "unixsocketperm"), "0");
}

#[test]
fn limits_drive_encodings() {
    let mut t = T::new();
    for i in 0..200 {
        t.run(&format!("HSET h f{i} v"));
    }
    // Redis's built-in hash-max-listpack-entries is 512, not redis.conf's 128.
    assert_eq!(t.run("OBJECT ENCODING h"), bulk("listpack"));
    t.run("CONFIG SET hash-max-listpack-entries 10");
    t.run("HSET h other v");
    assert_eq!(t.run("OBJECT ENCODING h"), bulk("hashtable"));
    t.run("CONFIG SET set-max-intset-entries 2");
    t.run("SADD s 1 2 3");
    assert_eq!(t.run("OBJECT ENCODING s"), bulk("listpack"));
    t.run("CONFIG SET zset-max-listpack-entries 2");
    t.run("ZADD z 1 a 2 b 3 c");
    assert_eq!(t.run("OBJECT ENCODING z"), bulk("skiplist"));
}
