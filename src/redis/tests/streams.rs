//! Streams: XADD and friends, plus blocking XREAD.

use super::*;

/// `[id, [field, value, ...]]`, the shape stream commands reply with.
fn entry(id: &str, fields: &[&str]) -> Value {
    arr(vec![bulk(id), bulks(fields)])
}

#[test]
fn xadd_ids_and_ranges() {
    let mut t = T::new();
    assert_eq!(t.run("XADD s 1-1 a 1"), bulk("1-1"));
    assert_eq!(t.run("XADD s 1-2 b 2"), bulk("1-2"));
    assert_eq!(t.run("XADD s 2-* c 3"), bulk("2-0"));
    assert_eq!(t.run("XADD s 2-* d 4"), bulk("2-1"));
    assert_eq!(t.run("XADD s 3 e 5"), bulk("3-0"));
    // The clock gives auto IDs their millisecond part.
    assert_eq!(t.run("XADD s * f 6"), bulk(&format!("{START_MS}-0")));
    assert_eq!(t.run("XADD s * g 7"), bulk(&format!("{START_MS}-1")));
    assert_eq!(t.run("XLEN s"), int(7));
    assert_eq!(t.run("TYPE s"), simple("stream"));
    assert_eq!(
        t.run("XADD s 1-1 x 1"),
        err("ERR The ID specified in XADD is equal or smaller than the target stream top item")
    );
    assert_eq!(
        t.run("XADD s 0-0 x 1"),
        err("ERR The ID specified in XADD must be greater than 0-0")
    );
    assert_eq!(
        t.run("XADD s bad x 1"),
        err("ERR Invalid stream ID specified as stream command argument")
    );
    assert_eq!(
        t.run("XADD s 9-1 onlyfield"),
        err("ERR wrong number of arguments for 'xadd' command")
    );
    assert_eq!(t.run("XADD nosuch NOMKSTREAM 1-1 a 1"), nil());
    assert_eq!(t.run("EXISTS nosuch"), int(0));
    assert_eq!(
        t.run("XRANGE s 1 2"),
        arr(vec![
            entry("1-1", &["a", "1"]),
            entry("1-2", &["b", "2"]),
            entry("2-0", &["c", "3"]),
            entry("2-1", &["d", "4"])
        ])
    );
    assert_eq!(
        t.run("XRANGE s (1-2 2"),
        arr(vec![entry("2-0", &["c", "3"]), entry("2-1", &["d", "4"])])
    );
    assert_eq!(t.run("XRANGE s - + COUNT 1"), arr(vec![entry("1-1", &["a", "1"])]));
    assert_eq!(t.run("XRANGE s - + COUNT 0"), Value::NullArray);
    assert_eq!(
        t.run("XREVRANGE s + - COUNT 1"),
        arr(vec![entry(&format!("{START_MS}-1"), &["g", "7"])])
    );
    assert_eq!(t.run("XRANGE nokey - +"), arr(vec![]));
    assert_eq!(t.run("XDEL s 1-1 9-9"), int(1));
    assert_eq!(t.run("XLEN s"), int(6));
    t.run("SET str v");
    assert_eq!(t.run("XADD str 1-1 a 1"), err(WRONGTYPE));
}

#[test]
fn trimming_and_setid() {
    let mut t = T::new();
    for i in 1..=4 {
        t.run(&format!("XADD s {i}-1 f {i}"));
    }
    assert_eq!(t.run("XTRIM s MAXLEN 2"), int(2));
    assert_eq!(t.run("XLEN s"), int(2));
    assert_eq!(t.run("XTRIM s MINID 4"), int(1));
    assert_eq!(t.run("XRANGE s - +"), arr(vec![entry("4-1", &["f", "4"])]));
    assert_eq!(t.run("XTRIM s MAXLEN -1"), err("ERR The MAXLEN argument must be >= 0."));
    assert_eq!(
        t.run("XTRIM s MAXLEN 1 MINID 1"),
        err("ERR syntax error, MAXLEN and MINID options at the same time are not compatible")
    );
    assert_eq!(
        t.run("XTRIM s LIMIT 5"),
        err("ERR syntax error, LIMIT cannot be used without specifying a trimming strategy")
    );
    assert_eq!(
        t.run("XTRIM s MAXLEN 1 LIMIT 5"),
        err("ERR syntax error, LIMIT cannot be used without the special ~ option")
    );
    assert_eq!(t.run("XADD s MAXLEN 1 5-1 e 5"), bulk("5-1"));
    assert_eq!(t.run("XLEN s"), int(1));
    assert_eq!(t.run("XSETID s 100-0"), ok());
    assert_eq!(t.run("XADD s * z 1"), bulk(&format!("{START_MS}-0")));
    assert_eq!(
        t.run("XSETID s 1-1"),
        err("ERR The ID specified in XSETID is smaller than the target stream top item")
    );
    assert_eq!(t.run("XSETID nokey 5-5"), err("ERR no such key"));
}

#[test]
fn xread_and_blocking() {
    let mut t = T::new();
    t.run("XADD s 1-1 a 1");
    t.run("XADD s 2-1 b 2");
    assert_eq!(
        t.run("XREAD STREAMS s 0"),
        arr(vec![arr(vec![
            bulk("s"),
            arr(vec![entry("1-1", &["a", "1"]), entry("2-1", &["b", "2"])])
        ])])
    );
    assert_eq!(
        t.run("XREAD COUNT 1 STREAMS s 0"),
        arr(vec![arr(vec![bulk("s"), arr(vec![entry("1-1", &["a", "1"])])])])
    );
    assert_eq!(t.run("XREAD STREAMS s $"), Value::NullArray);
    assert_eq!(t.run("XREAD STREAMS nokey 0"), Value::NullArray);
    assert_eq!(
        t.run("XREAD STREAMS s t 0"),
        err(
            "ERR Unbalanced 'xread' list of streams: for each stream key an ID or '$' must be specified."
        )
    );
    assert_eq!(
        t.run("XREAD STREAMS s >"),
        err(
            "ERR The > ID can be specified only when calling XREADGROUP using the GROUP <group> <consumer> option."
        )
    );
    // RESP3 keys the streams by name.
    t.run("HELLO 3");
    assert_eq!(
        t.run("XREAD STREAMS s 1-1"),
        Value::Map(vec![(bulk("s"), arr(vec![entry("2-1", &["b", "2"])]))])
    );
    t.run("HELLO 2");

    // Blocked readers are served in order by the next XADD.
    let mut a = t.connect();
    let mut b = t.connect();
    assert_eq!(t.run_as(&mut a, "XREAD BLOCK 0 STREAMS s $"), Value::NoReply);
    assert_eq!(t.run_as(&mut b, "XREAD BLOCK 0 STREAMS s $"), Value::NoReply);
    t.run("XADD s 3-1 c 3");
    let served = arr(vec![arr(vec![bulk("s"), arr(vec![entry("3-1", &["c", "3"])])])]);
    assert_eq!(t.engine.take_reply(&mut a), Some(served.clone()));
    assert_eq!(t.engine.take_reply(&mut b), Some(served));
    // A brand new stream wakes its waiter too.
    assert_eq!(t.run_as(&mut a, "XREAD BLOCK 0 STREAMS fresh $"), Value::NoReply);
    t.run("XADD fresh 1-1 x 1");
    assert_eq!(
        t.engine.take_reply(&mut a),
        Some(arr(vec![arr(vec![bulk("fresh"), arr(vec![entry("1-1", &["x", "1"])])])]))
    );
    // Inside MULTI it can't block.
    t.run("MULTI");
    t.run("XREAD BLOCK 0 STREAMS s $");
    assert_eq!(t.run("EXEC"), arr(vec![Value::NullArray]));
}
