//! Set commands. Small sets keep Redis's deterministic orders: integer
//! sets (intsets) are sorted, other small sets (listpacks) keep insertion
//! order.

use super::*;

const WRONGTYPE: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";

fn set(items: &[&str]) -> Value {
    Value::Set(items.iter().map(|s| bulk(s)).collect())
}

#[test]
fn add_remove_members() {
    let mut t = T::new();
    assert_eq!(t.run("SADD s 3 1 2 1"), int(3));
    assert_eq!(t.run("SMEMBERS s"), set(&["1", "2", "3"]));
    assert_eq!(t.run("SADD s -5"), int(1));
    assert_eq!(t.run("SMEMBERS s"), set(&["-5", "1", "2", "3"]));
    // A non-integer turns the intset into a listpack, keeping its order.
    assert_eq!(t.run("SADD s x 0"), int(2));
    assert_eq!(t.run("SMEMBERS s"), set(&["-5", "1", "2", "3", "x", "0"]));
    assert_eq!(t.run("SREM s 2 nope x"), int(2));
    assert_eq!(t.run("SMEMBERS s"), set(&["-5", "1", "3", "0"]));
    assert_eq!(t.run("SCARD s"), int(4));
    assert_eq!(t.run("SCARD nokey"), int(0));
    assert_eq!(t.run("SISMEMBER s 3"), int(1));
    assert_eq!(t.run("SISMEMBER s 2"), int(0));
    assert_eq!(t.run("SISMEMBER nokey 2"), int(0));
    assert_eq!(t.run("SMISMEMBER s 1 2 0"), arr(vec![int(1), int(0), int(1)]));
    assert_eq!(t.run("SMISMEMBER nokey 1"), arr(vec![int(0)]));
    assert_eq!(t.run("SMEMBERS nokey"), set(&[]));
    assert_eq!(t.run("SREM s -5 1 3 0"), int(4));
    assert_eq!(t.run("EXISTS s"), int(0));
    assert_eq!(t.run("SREM s a"), int(0));
    t.run("SET str v");
    assert_eq!(t.run("SADD str a"), err(WRONGTYPE));
    assert_eq!(t.run("SMEMBERS str"), err(WRONGTYPE));
    assert_eq!(t.run("SMISMEMBER str a"), err(WRONGTYPE));
    t.run("SADD s a");
    assert_eq!(t.run("TYPE s"), simple("set"));
}

#[test]
fn big_sets_change_encoding() {
    let mut t = T::new();
    let ints: Vec<String> = (0..600).map(|i| i.to_string()).collect();
    t.run(&format!("SADD big {}", ints.join(" ")));
    assert_eq!(t.run("SCARD big"), int(600));
    assert_eq!(t.run("OBJECT ENCODING big"), bulk("hashtable"));
    let words: Vec<String> = (0..128).map(|i| format!("w{i}")).collect();
    t.run(&format!("SADD words {}", words.join(" ")));
    assert_eq!(t.run("OBJECT ENCODING words"), bulk("listpack"));
    t.run("SADD words one-more");
    assert_eq!(t.run("OBJECT ENCODING words"), bulk("hashtable"));
    t.run("SADD small 1 2");
    assert_eq!(t.run("OBJECT ENCODING small"), bulk("intset"));
    let long = "x".repeat(65);
    t.run(&format!("SADD small {long}"));
    assert_eq!(t.run("OBJECT ENCODING small"), bulk("hashtable"));
}

#[test]
fn smove() {
    let mut t = T::new();
    t.run("SADD a 1 2");
    assert_eq!(t.run("SMOVE a b 1"), int(1));
    assert_eq!(t.run("SMOVE a b 9"), int(0));
    assert_eq!(t.run("SMOVE nokey b 1"), int(0));
    assert_eq!(t.run("SMOVE a a 2"), int(1));
    assert_eq!(t.run("SMOVE a a 9"), int(0));
    assert_eq!(t.run("SMOVE a b 2"), int(1));
    assert_eq!(t.run("EXISTS a"), int(0));
    assert_eq!(t.run("SMEMBERS b"), set(&["1", "2"]));
    t.run("SET str v");
    assert_eq!(t.run("SMOVE b str 1"), err(WRONGTYPE));
    assert_eq!(t.run("SMOVE str b 1"), err(WRONGTYPE));
}

#[test]
fn algebra() {
    let mut t = T::new();
    t.run("SADD a c b a d");
    t.run("SADD b d x c");
    t.run("SADD n 3 1 2");
    assert_eq!(t.run("SINTER a b"), set(&["d", "c"]));
    assert_eq!(t.run("SINTER a nokey"), set(&[]));
    assert_eq!(t.run("SUNION a b"), set(&["c", "b", "a", "d", "x"]));
    assert_eq!(t.run("SUNION n nokey"), set(&["1", "2", "3"]));
    assert_eq!(t.run("SDIFF a b"), set(&["b", "a"]));
    assert_eq!(t.run("SDIFF a a"), set(&[]));
    assert_eq!(t.run("SDIFF nokey a"), set(&[]));
    assert_eq!(t.run("SINTERSTORE dst a b"), int(2));
    assert_eq!(t.run("SMEMBERS dst"), set(&["d", "c"]));
    assert_eq!(t.run("SUNIONSTORE dst n a"), int(7));
    assert_eq!(t.run("SMEMBERS dst"), set(&["1", "2", "3", "c", "b", "a", "d"]));
    assert_eq!(t.run("SDIFFSTORE dst a a"), int(0));
    assert_eq!(t.run("EXISTS dst"), int(0));
    assert_eq!(t.run("SINTERCARD 2 a b"), int(2));
    assert_eq!(t.run("SINTERCARD 2 a b LIMIT 1"), int(1));
    assert_eq!(t.run("SINTERCARD 1 nokey"), int(0));
    assert_eq!(t.run("SINTERCARD 0 a"), err("ERR numkeys should be greater than 0"));
    assert_eq!(
        t.run("SINTERCARD 3 a b"),
        err("ERR Number of keys can't be greater than number of args")
    );
    assert_eq!(t.run("SINTERCARD 1 a LIMIT -1"), err("ERR LIMIT can't be negative"));
    assert_eq!(t.run("SINTERCARD 1 a FOO"), err(SYNTAX));
    t.run("SET str v");
    assert_eq!(t.run("SUNION a str"), err(WRONGTYPE));
    assert_eq!(t.run("SINTER nokey str"), err(WRONGTYPE));
}

#[test]
fn spop_and_srandmember() {
    let mut t = T::new();
    t.run("SADD s a b c d e");
    let Value::Bulk(one) = t.run("SPOP s") else { panic!() };
    assert_eq!(t.run(&format!("SISMEMBER s {}", String::from_utf8(one).unwrap())), int(0));
    assert_eq!(t.run("SCARD s"), int(4));
    let Value::Set(two) = t.run("SPOP s 2") else { panic!() };
    assert_eq!(two.len(), 2);
    assert_eq!(t.run("SCARD s"), int(2));
    assert_eq!(t.run("SPOP s 0"), set(&[]));
    let Value::Array(r) = t.run("SRANDMEMBER s 5") else { panic!() };
    assert_eq!(r.len(), 2);
    let Value::Array(r) = t.run("SRANDMEMBER s -5") else { panic!() };
    assert_eq!(r.len(), 5);
    assert_eq!(t.run("SRANDMEMBER s 0"), arr(vec![]));
    let Value::Set(rest) = t.run("SPOP s 10") else { panic!() };
    assert_eq!(rest.len(), 2);
    assert_eq!(t.run("EXISTS s"), int(0));
    assert_eq!(t.run("SPOP s"), nil());
    assert_eq!(t.run("SPOP s 3"), set(&[]));
    assert_eq!(t.run("SRANDMEMBER s"), nil());
    assert_eq!(t.run("SRANDMEMBER s 3"), arr(vec![]));
    assert_eq!(t.run("SPOP s -1"), err("ERR value is out of range, must be positive"));
    assert_eq!(t.run("SPOP s 1 2"), err(SYNTAX));
    assert_eq!(t.run("SRANDMEMBER s x"), err(NOT_INT));
    assert_eq!(t.run("SRANDMEMBER s 1 2"), err(SYNTAX));
}

#[test]
fn sscan_returns_small_sets_whole() {
    let mut t = T::new();
    t.run("SADD s a b c");
    assert_eq!(t.run("SSCAN s 0 COUNT 1"), arr(vec![bulk("0"), bulks(&["a", "b", "c"])]));
    assert_eq!(t.run("SSCAN s 0 MATCH b"), arr(vec![bulk("0"), bulks(&["b"])]));
    assert_eq!(t.run("SSCAN nokey 0"), arr(vec![bulk("0"), arr(vec![])]));
    let ints: Vec<String> = (0..1000).map(|i| i.to_string()).collect();
    t.run(&format!("SADD big {}", ints.join(" ")));
    let mut cursor = "0".to_string();
    let mut seen = 0;
    loop {
        let Value::Array(r) = t.run(&format!("SSCAN big {cursor}")) else { panic!() };
        let Value::Array(items) = &r[1] else { panic!() };
        seen += items.len();
        cursor = text(&r[0]);
        if cursor == "0" {
            break;
        }
    }
    assert_eq!(seen, 1000);
}
