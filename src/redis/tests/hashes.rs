//! Hash commands.

use super::*;

const WRONGTYPE: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";

#[test]
fn hset_hget_hdel() {
    let mut t = T::new();
    assert_eq!(t.run("HSET h a 1 b 2"), int(2));
    assert_eq!(t.run("HSET h a 9 c 3"), int(1));
    assert_eq!(t.run("HGET h a"), bulk("9"));
    assert_eq!(t.run("HGET h nope"), nil());
    assert_eq!(t.run("HGET nokey a"), nil());
    assert_eq!(t.run("HSET h a"), err("ERR wrong number of arguments for 'hset' command"));
    assert_eq!(t.run("HMSET h x 1 y 2"), ok());
    assert_eq!(t.run("HMSET h x"), err("ERR wrong number of arguments for 'hmset' command"));
    assert_eq!(t.run("HDEL h a nope b"), int(2));
    assert_eq!(t.run("HLEN h"), int(3));
    // Deleting the last field deletes the key.
    assert_eq!(t.run("HDEL h c x y"), int(3));
    assert_eq!(t.run("EXISTS h"), int(0));
    assert_eq!(t.run("HDEL h a"), int(0));
}

#[test]
fn hsetnx_hexists_hlen_hstrlen() {
    let mut t = T::new();
    assert_eq!(t.run("HSETNX h f v"), int(1));
    assert_eq!(t.run("HSETNX h f w"), int(0));
    assert_eq!(t.run("HGET h f"), bulk("v"));
    assert_eq!(t.run("HEXISTS h f"), int(1));
    assert_eq!(t.run("HEXISTS h g"), int(0));
    assert_eq!(t.run("HEXISTS nokey f"), int(0));
    assert_eq!(t.run("HLEN h"), int(1));
    assert_eq!(t.run("HLEN nokey"), int(0));
    t.run("HSET h long \"hello world\"");
    assert_eq!(t.run("HSTRLEN h long"), int(11));
    assert_eq!(t.run("HSTRLEN h nope"), int(0));
}

#[test]
fn getall_keys_vals_keep_insertion_order() {
    let mut t = T::new();
    t.run("HSET h z 1 a 2 m 3");
    assert_eq!(t.run("HGETALL h"), map(vec![("z", bulk("1")), ("a", bulk("2")), ("m", bulk("3"))]));
    assert_eq!(t.run("HKEYS h"), bulks(&["z", "a", "m"]));
    assert_eq!(t.run("HVALS h"), bulks(&["1", "2", "3"]));
    // Overwriting keeps the position; deleting closes the gap.
    t.run("HSET h a 20");
    t.run("HDEL h z");
    assert_eq!(t.run("HKEYS h"), bulks(&["a", "m"]));
    assert_eq!(t.run("HGETALL nokey"), map(vec![]));
    assert_eq!(t.run("HKEYS nokey"), arr(vec![]));
    assert_eq!(t.run("HMGET h a nope m"), arr(vec![bulk("20"), nil(), bulk("3")]));
    assert_eq!(t.run("HMGET nokey a"), arr(vec![nil()]));
}

#[test]
fn hincrby_and_hincrbyfloat() {
    let mut t = T::new();
    assert_eq!(t.run("HINCRBY h n 5"), int(5));
    assert_eq!(t.run("HINCRBY h n -7"), int(-2));
    assert_eq!(t.run("HINCRBY h n x"), err(NOT_INT));
    t.run("HSET h s abc");
    assert_eq!(t.run("HINCRBY h s 1"), err("ERR hash value is not an integer"));
    t.run("HSET h big 9223372036854775807");
    assert_eq!(t.run("HINCRBY h big 1"), err("ERR increment or decrement would overflow"));

    t.run("HSET h f 10.50");
    assert_eq!(t.run("HINCRBYFLOAT h f 0.1"), bulk("10.6"));
    assert_eq!(t.run("HINCRBYFLOAT h new 0.25"), bulk("0.25"));
    assert_eq!(t.run("HINCRBYFLOAT h n 1.5"), bulk("-0.5"));
    assert_eq!(t.run("HINCRBYFLOAT h f x"), err("ERR value is not a valid float"));
    assert_eq!(t.run("HINCRBYFLOAT h f inf"), err("ERR value is NaN or Infinity"));
    assert_eq!(t.run("HINCRBYFLOAT h s 1"), err("ERR hash value is not a float"));
    // long double: 1e308 + 1e308 doesn't overflow, 1e4932 + 1e4932 does.
    t.run("HSET h huge 1e308");
    assert!(text(&t.run("HINCRBYFLOAT h huge 1e308")).starts_with("19999999999999999999"));
    t.run("HSET h max 1e4932");
    assert_eq!(
        t.run("HINCRBYFLOAT h max 1e4932"),
        err("ERR increment would produce NaN or Infinity")
    );
}

#[test]
fn wrong_types_are_rejected_both_ways() {
    let mut t = T::new();
    t.run("HSET h f v");
    t.run("SET s v");
    assert_eq!(t.run("TYPE h"), simple("hash"));
    assert_eq!(t.run("GET h"), err(WRONGTYPE));
    assert_eq!(t.run("APPEND h x"), err(WRONGTYPE));
    assert_eq!(t.run("INCR h"), err(WRONGTYPE));
    assert_eq!(t.run("HGET s f"), err(WRONGTYPE));
    assert_eq!(t.run("HSET s f v"), err(WRONGTYPE));
    assert_eq!(t.run("HGETALL s"), err(WRONGTYPE));
    // MGET never fails: other types read as nil.
    assert_eq!(t.run("MGET h s"), arr(vec![nil(), bulk("v")]));
    // SET replaces any type.
    assert_eq!(t.run("SET h now-a-string"), ok());
    assert_eq!(t.run("TYPE h"), simple("string"));
    // SCAN TYPE filters by type.
    t.run("HSET h2 f v");
    assert_eq!(t.run("SCAN 0 TYPE hash"), arr(vec![bulk("0"), bulks(&["h2"])]));
}

#[test]
fn hash_survives_rename_copy_and_expiry() {
    let mut t = T::new();
    t.run("HSET h f v");
    t.run("EXPIRE h 100");
    t.run("RENAME h h2");
    assert_eq!(t.run("HGET h2 f"), bulk("v"));
    assert_eq!(t.run("TTL h2"), int(100));
    assert_eq!(t.run("COPY h2 h3"), int(1));
    t.run("HSET h3 f changed");
    assert_eq!(t.run("HGET h2 f"), bulk("v"), "COPY must be deep");
}

#[test]
fn hscan_small_hash_returns_everything_at_once() {
    let mut t = T::new();
    t.run("HSET h a 1 b 2 c 3");
    assert_eq!(
        t.run("HSCAN h 0 COUNT 1"),
        arr(vec![bulk("0"), bulks(&["a", "1", "b", "2", "c", "3"])])
    );
    assert_eq!(t.run("HSCAN h 0 MATCH b*"), arr(vec![bulk("0"), bulks(&["b", "2"])]));
    assert_eq!(t.run("HSCAN nokey 0"), arr(vec![bulk("0"), arr(vec![])]));
    assert_eq!(t.run("HSCAN h x"), err("ERR invalid cursor"));
    assert_eq!(t.run("HSCAN h 0 COUNT 0"), err(SYNTAX));
    assert_eq!(t.run("HSCAN h 0 TYPE string"), err(SYNTAX));
}

#[test]
fn hscan_big_hash_iterates_with_cursor() {
    let mut t = T::new();
    for i in 0..300 {
        t.run(&format!("HSET big f{i} v{i}"));
    }
    let mut cursor = "0".to_string();
    let mut fields = 0;
    loop {
        let Value::Array(r) = t.run(&format!("HSCAN big {cursor} COUNT 50")) else { panic!() };
        let Value::Array(items) = &r[1] else { panic!() };
        fields += items.len() / 2;
        cursor = text(&r[0]);
        if cursor == "0" {
            break;
        }
    }
    assert_eq!(fields, 300);
}

#[test]
fn hrandfield() {
    let mut t = T::new();
    assert_eq!(t.run("HRANDFIELD nokey"), nil());
    assert_eq!(t.run("HRANDFIELD nokey 3"), arr(vec![]));
    t.run("HSET h a 1 b 2 c 3");
    let one = t.run("HRANDFIELD h");
    assert!([bulk("a"), bulk("b"), bulk("c")].contains(&one));
    assert_eq!(t.run("HRANDFIELD h 0"), arr(vec![]));
    // A count at least the size returns the whole hash, in order.
    assert_eq!(t.run("HRANDFIELD h 5"), bulks(&["a", "b", "c"]));
    assert_eq!(t.run("HRANDFIELD h 3 WITHVALUES"), bulks(&["a", "1", "b", "2", "c", "3"]));
    // Positive counts give distinct fields.
    let Value::Array(two) = t.run("HRANDFIELD h 2") else { panic!() };
    assert_eq!(two.len(), 2);
    assert_ne!(two[0], two[1]);
    // Negative counts may repeat and return exactly |count| fields.
    let Value::Array(many) = t.run("HRANDFIELD h -10") else { panic!() };
    assert_eq!(many.len(), 10);
    let Value::Array(pairs) = t.run("HRANDFIELD h -4 WITHVALUES") else { panic!() };
    assert_eq!(pairs.len(), 8);
    assert_eq!(t.run("HRANDFIELD h 1 FOO"), err(SYNTAX));
    assert_eq!(t.run("HRANDFIELD h x"), err(NOT_INT));
    assert_eq!(
        t.run("HRANDFIELD h -9223372036854775807 WITHVALUES"),
        err("ERR value is out of range")
    );
}

#[test]
fn hrandfield_withvalues_nests_pairs_on_resp3() {
    let mut t = T::new();
    t.run("HSET h a 1 b 2");
    t.run("HELLO 3");
    assert_eq!(
        t.run("HRANDFIELD h 2 WITHVALUES"),
        arr(vec![bulks(&["a", "1"]), bulks(&["b", "2"])])
    );
}
