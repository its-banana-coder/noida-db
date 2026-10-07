//! DEBUG's test-support subcommands (Redis 7.2 with enable-debug-command).

use super::*;

const ZEROS: &str = "0000000000000000000000000000000000000000";

#[test]
fn unknown_and_help() {
    let mut t = T::new();
    assert_eq!(
        t.run("DEBUG nope"),
        err("ERR unknown subcommand or wrong number of arguments for 'nope'. Try DEBUG HELP.")
    );
    assert_eq!(t.run("DEBUG"), err("ERR wrong number of arguments for 'debug' command"));
    // Crashing the server is never on offer.
    assert!(matches!(t.run("DEBUG segfault"), Value::Error(_)));
    let Value::Array(lines) = t.run("DEBUG HELP") else { panic!() };
    assert_eq!(lines[0], simple("DEBUG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:"));
    assert_eq!(t.run("DEBUG sleep 0"), ok());
    assert_eq!(t.run("DEBUG quicklist-packed-threshold 1b"), ok());
    assert_eq!(
        t.run("DEBUG quicklist-packed-threshold x"),
        err("ERR argument must be a memory value bigger than 1 and smaller than 4gb")
    );
    assert_eq!(t.run("DEBUG error \"FOO bar\""), err("FOO bar"));
}

#[test]
fn active_expire_off_keeps_expired_keys_until_touched() {
    let mut t = T::new();
    t.run("SET a 1 PX 10");
    t.run("SET b 1");
    assert_eq!(t.run("DEBUG set-active-expire 0"), ok());
    t.advance(100);
    t.engine.purge_expired();
    assert_eq!(t.run("DBSIZE"), int(2));
    assert_eq!(t.run("KEYS *"), bulks(&["b"]));
    assert_eq!(t.run("GET a"), nil());
    assert_eq!(t.run("DBSIZE"), int(1));
    t.run("SET c 1 PX 10");
    assert_eq!(t.run("DEBUG set-active-expire 1"), ok());
    t.advance(100);
    assert_eq!(t.run("DBSIZE"), int(1));
}

#[test]
fn digests() {
    let mut t = T::new();
    assert_eq!(t.run("DEBUG digest"), simple(ZEROS));
    assert_eq!(t.run("DEBUG digest-value nokey"), arr(vec![simple(ZEROS)]));
    t.run("SADD s 1 2 3");
    t.run("SADD s2 3 2 1");
    t.run("HSET h a 1 b 2");
    let d1 = t.run("DEBUG digest");
    assert_ne!(d1, simple(ZEROS));
    // Same content, same digest, whatever the order it was built in.
    let Value::Array(v) = t.run("DEBUG digest-value s s2 nokey") else { panic!() };
    assert_eq!(v[0], v[1]);
    assert_eq!(v[2], simple(ZEROS));
    assert_eq!(t.run("DEBUG reload"), ok());
    assert_eq!(t.run("DEBUG digest"), d1);
    t.run("HSET h a 2");
    assert_ne!(t.run("DEBUG digest"), d1);
}

#[test]
fn reload_reencodes_and_drops_expired() {
    let mut t = T::new();
    t.run("CONFIG SET hash-max-listpack-entries 2");
    t.run("HSET h a 1 b 2 c 3");
    t.run("HDEL h c");
    t.run("CONFIG SET hash-max-listpack-entries 128");
    assert_eq!(t.run("OBJECT ENCODING h"), bulk("hashtable"));
    t.run("SET gone v PX 10");
    t.run("DEBUG set-active-expire 0");
    t.advance(50);
    assert_eq!(t.run("DEBUG reload"), ok());
    assert_eq!(t.run("OBJECT ENCODING h"), bulk("listpack"));
    assert_eq!(t.run("DBSIZE"), int(1));
    assert_eq!(
        t.run("DEBUG reload foo"),
        err("ERR DEBUG RELOAD only supports the MERGE, NOFLUSH and NOSAVE options.")
    );
}

#[test]
fn object_and_populate() {
    let mut t = T::new();
    assert_eq!(t.run("DEBUG object nokey"), err("ERR no such key"));
    t.run("SET foo 12345");
    let Value::Simple(s) = t.run("DEBUG object foo") else { panic!() };
    assert!(s.starts_with("Value at:0x"), "{s}");
    assert!(s.contains(" refcount:1 encoding:int serializedlength:3 lru:"), "{s}");
    assert!(s.contains(" lru_seconds_idle:"), "{s}");
    t.run("INCR foo");
    let Value::Simple(s2) = t.run("DEBUG object foo") else { panic!() };
    assert_eq!(s.split(' ').nth(1), s2.split(' ').nth(1));

    assert_eq!(t.run("DEBUG populate 3"), ok());
    assert_eq!(t.run("MGET key:0 key:2"), bulks(&["value:0", "value:2"]));
    assert_eq!(t.run("DEBUG populate 2 p 3"), ok());
    assert_eq!(t.run("MGET p:0 p:1"), bulks(&["val", "val"]));
    t.run("SET key:1 mine");
    t.run("DEBUG populate 3");
    assert_eq!(t.run("GET key:1"), bulk("mine"));
    assert_eq!(t.run("DEBUG populate -1"), err("ERR value is out of range, must be positive"));
    assert_eq!(t.run("DEBUG populate x"), err("ERR value is out of range, must be positive"));
}

#[test]
fn protocol_replies() {
    let mut t = T::new();
    assert_eq!(t.run("DEBUG protocol string"), bulk("Hello World"));
    assert_eq!(t.run("DEBUG protocol integer"), int(12345));
    #[allow(clippy::approx_constant)]
    let d = Value::Double(3.141);
    assert_eq!(t.run("DEBUG protocol double"), d);
    assert_eq!(t.run("DEBUG protocol attrib"), bulk("Some real reply following the attribute"));
    assert_eq!(t.run("DEBUG protocol push"), err("ERR RESP2 is not supported by this command"));
    t.run("HELLO 3");
    let mut out = Vec::new();
    super::super::resp::encode(&t.run("DEBUG protocol attrib"), 3, &mut out);
    assert_eq!(
        out,
        b"|1\r\n$14\r\nkey-popularity\r\n*2\r\n$7\r\nkey:123\r\n:90\r\n\
          $39\r\nSome real reply following the attribute\r\n"
    );
    assert_eq!(
        t.run("DEBUG protocol nope"),
        err("ERR Wrong protocol type name. Please use one of the following: \
             string|integer|double|bignum|null|array|set|map|attrib|push|verbatim|true|false")
    );
}

#[test]
fn skip_checksum_validation() {
    let mut t = T::new();
    t.run("SET k v");
    let Value::Bulk(mut payload) = t.run("DUMP k") else { panic!() };
    let n = payload.len();
    payload[n - 1] ^= 0xff;
    let args = vec![b"RESTORE".to_vec(), b"k2".to_vec(), b"0".to_vec(), payload];
    let restore = |t: &mut T| t.engine.execute(&mut t.session, &args);
    assert_eq!(restore(&mut t), err("ERR DUMP payload version or checksum are wrong"));
    t.run("DEBUG set-skip-checksum-validation 1");
    assert_eq!(restore(&mut t), ok());
    assert_eq!(t.run("GET k2"), bulk("v"));
}
