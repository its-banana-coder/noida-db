//! Sorted set commands. Scores print exactly as Redis's `d2string`.

use super::*;

fn d(v: f64) -> Value {
    Value::Double(v)
}

#[test]
fn zadd_options_and_scores() {
    let mut t = T::new();
    assert_eq!(t.run("ZADD z 1 a 2 b 3 c"), int(3));
    assert_eq!(t.run("ZADD z 1.5 a 10 d"), int(1));
    assert_eq!(t.run("ZRANGE z 0 -1"), bulks(&["a", "b", "c", "d"]));
    assert_eq!(t.run("ZADD z NX 100 a 5 e"), int(1));
    assert_eq!(t.run("ZADD z XX CH 7 a 9 nope"), int(1));
    assert_eq!(t.run("ZADD z GT CH 1 a 20 b"), int(1));
    assert_eq!(t.run("ZSCORE z a"), d(7.0));
    assert_eq!(t.run("ZSCORE z b"), d(20.0));
    assert_eq!(t.run("ZADD z INCR 2.5 c"), d(5.5));
    assert_eq!(t.run("ZADD z NX INCR 1 c"), nil());
    assert_eq!(t.run("ZINCRBY z 0.1 c"), d(5.6));
    assert_eq!(
        t.run("ZADD z NX XX 1 a"),
        err("ERR XX and NX options at the same time are not compatible")
    );
    assert_eq!(
        t.run("ZADD z GT LT 1 a"),
        err("ERR GT, LT, and/or NX options at the same time are not compatible")
    );
    assert_eq!(
        t.run("ZADD z INCR 1 a 2 b"),
        err("ERR INCR option supports a single increment-element pair")
    );
    assert_eq!(t.run("ZADD z x a"), err("ERR value is not a valid float"));
    assert_eq!(t.run("ZADD z NX 1"), err(SYNTAX));
    t.run("ZADD k inf x");
    assert_eq!(t.run("ZINCRBY k -inf x"), err("ERR resulting score is not a number (NaN)"));
    assert_eq!(t.run("ZCARD z"), int(5));
    assert_eq!(t.run("ZMSCORE z a nope"), arr(vec![d(7.0), nil()]));
    assert_eq!(t.run("ZREM z a nope"), int(1));
    assert_eq!(t.run("TYPE z"), simple("zset"));
}

#[test]
fn ranks_and_ranges() {
    let mut t = T::new();
    t.run("ZADD z 1 a 2 b 3 c 4 d 5 e");
    assert_eq!(t.run("ZRANK z c"), int(2));
    assert_eq!(t.run("ZREVRANK z c"), int(2));
    assert_eq!(t.run("ZRANK z d WITHSCORE"), arr(vec![int(3), d(4.0)]));
    assert_eq!(t.run("ZRANK z nope WITHSCORE"), Value::NullArray);
    assert_eq!(t.run("ZRANGE z -2 -1 WITHSCORES"), arr(vec![bulk("d"), d(4.0), bulk("e"), d(5.0)]));
    assert_eq!(t.run("ZRANGE z (1 4 BYSCORE"), bulks(&["b", "c", "d"]));
    assert_eq!(t.run("ZRANGE z 4 (2 BYSCORE REV"), bulks(&["d", "c"]));
    assert_eq!(t.run("ZRANGE z -inf +inf BYSCORE LIMIT 1 2"), bulks(&["b", "c"]));
    assert_eq!(t.run("ZRANGE z [b (d BYLEX"), bulks(&["b", "c"]));
    assert_eq!(
        t.run("ZRANGE z 0 1 LIMIT 0 1"),
        err(
            "ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX"
        )
    );
    assert_eq!(t.run("ZRANGE z a b BYSCORE"), err("ERR min or max is not a float"));
    assert_eq!(t.run("ZRANGE z a b BYLEX"), err("ERR min or max not valid string range item"));
    assert_eq!(t.run("ZREVRANGEBYSCORE z +inf -inf LIMIT 0 2"), bulks(&["e", "d"]));
    assert_eq!(t.run("ZCOUNT z (2 +inf"), int(3));
    assert_eq!(t.run("ZLEXCOUNT z - +"), int(5));
    assert_eq!(t.run("ZRANGESTORE dst z 1 2"), int(2));
    assert_eq!(t.run("ZRANGE dst 0 -1"), bulks(&["b", "c"]));
    assert_eq!(t.run("ZREMRANGEBYSCORE z (3 4"), int(1));
    assert_eq!(t.run("ZREMRANGEBYRANK z 0 0"), int(1));
    assert_eq!(t.run("ZRANGE z 0 -1"), bulks(&["b", "c", "e"]));
    // RESP3 pairs each member with a double.
    t.run("HELLO 3");
    assert_eq!(t.run("ZRANGE z 0 0 WITHSCORES"), arr(vec![arr(vec![bulk("b"), d(2.0)])]));
}

#[test]
fn pops_and_blocking_pops() {
    let mut t = T::new();
    t.run("ZADD z 1 a 2 b 3 c");
    assert_eq!(t.run("ZPOPMIN z"), arr(vec![bulk("a"), d(1.0)]));
    assert_eq!(t.run("ZPOPMAX z 5"), arr(vec![bulk("c"), d(3.0), bulk("b"), d(2.0)]));
    assert_eq!(t.run("EXISTS z"), int(0));
    assert_eq!(t.run("ZPOPMIN z"), arr(vec![]));
    assert_eq!(t.run("ZMPOP 1 z MIN"), Value::NullArray);
    let mut a = t.connect();
    let mut b = t.connect();
    assert_eq!(t.run_as(&mut a, "BZPOPMIN z 0"), Value::NoReply);
    assert_eq!(t.run_as(&mut b, "BZMPOP 0 1 z MAX COUNT 5"), Value::NoReply);
    t.run("ZADD z 1 x 2 y 3 w");
    assert_eq!(t.engine.take_reply(&mut a), Some(arr(vec![bulk("z"), bulk("x"), d(1.0)])));
    assert_eq!(
        t.engine.take_reply(&mut b),
        Some(arr(vec![
            bulk("z"),
            arr(vec![arr(vec![bulk("w"), d(3.0)]), arr(vec![bulk("y"), d(2.0)])])
        ]))
    );
    // A list at the key doesn't wake a sorted set waiter.
    t.run_as(&mut a, "BZPOPMIN q 0");
    t.run("RPUSH q v");
    assert_eq!(t.engine.take_reply(&mut a), None);
}

#[test]
fn union_inter_diff() {
    let mut t = T::new();
    t.run("ZADD a 1 x 2 y 3 z");
    t.run("ZADD b 10 y 20 z 30 w");
    t.run("SADD s x w");
    assert_eq!(
        t.run("ZUNION 2 a b WITHSCORES"),
        arr(vec![bulk("x"), d(1.0), bulk("y"), d(12.0), bulk("z"), d(23.0), bulk("w"), d(30.0)])
    );
    assert_eq!(t.run("ZINTER 2 a s WITHSCORES"), arr(vec![bulk("x"), d(2.0)]));
    assert_eq!(t.run("ZDIFF 2 a b"), bulks(&["x"]));
    assert_eq!(t.run("ZINTERSTORE dst 2 a b AGGREGATE MAX"), int(2));
    assert_eq!(
        t.run("ZRANGE dst 0 -1 WITHSCORES"),
        arr(vec![bulk("y"), d(10.0), bulk("z"), d(20.0)])
    );
    assert_eq!(t.run("ZINTERCARD 2 a b LIMIT 1"), int(1));
    assert_eq!(t.run("ZUNION 0 a"), err("ERR at least 1 input key is needed for 'zunion' command"));
    assert_eq!(t.run("ZUNION 2 a b WEIGHTS 1 x"), err("ERR weight value is not a float"));
}
