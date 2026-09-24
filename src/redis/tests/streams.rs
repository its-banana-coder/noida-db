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

#[test]
fn xgroup_create_and_destroy() {
    let mut t = T::new();
    assert_eq!(
        t.run("XGROUP CREATE s g 0"),
        err("ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may \
             want to use the MKSTREAM option to create an empty stream automatically.")
    );
    assert_eq!(t.run("XGROUP CREATE s g $ MKSTREAM"), ok());
    assert_eq!(t.run("TYPE s"), simple("stream"));
    assert_eq!(t.run("XGROUP CREATE s g 0"), err("BUSYGROUP Consumer Group name already exists"));
    assert_eq!(t.run("XGROUP CREATE s other 0"), ok());
    assert_eq!(t.run("XGROUP CREATECONSUMER s g alice"), int(1));
    assert_eq!(t.run("XGROUP CREATECONSUMER s g alice"), int(0));
    assert_eq!(t.run("XGROUP DELCONSUMER s g alice"), int(0));
    assert_eq!(t.run("XGROUP DELCONSUMER s g nobody"), int(0));
    assert_eq!(
        t.run("XGROUP CREATECONSUMER s nogroup alice"),
        err("NOGROUP No such consumer group 'nogroup' for key name 's'")
    );
    assert_eq!(t.run("XGROUP SETID s g 5-5"), ok());
    assert_eq!(t.run("XGROUP SETID s g 5-5 ENTRIESREAD 3"), ok());
    assert_eq!(
        t.run("XGROUP SETID s g 5-5 ENTRIESREAD -3"),
        err("ERR value for ENTRIESREAD must be positive or -1")
    );
    assert_eq!(t.run("XGROUP DESTROY s other"), int(1));
    assert_eq!(t.run("XGROUP DESTROY s other"), int(0));
    assert_eq!(t.run("XGROUP FOO s g"), err("ERR unknown subcommand 'FOO'. Try XGROUP HELP."));
}

#[test]
fn xreadgroup_delivers_and_tracks_pending() {
    let mut t = T::new();
    t.run("XADD s 1-1 a 1");
    t.run("XADD s 2-1 b 2");
    t.run("XGROUP CREATE s g 0");
    assert_eq!(
        t.run("XREADGROUP GROUP g alice COUNT 1 STREAMS s >"),
        arr(vec![arr(vec![bulk("s"), arr(vec![entry("1-1", &["a", "1"])])])])
    );
    assert_eq!(
        t.run("XREADGROUP GROUP g bob STREAMS s >"),
        arr(vec![arr(vec![bulk("s"), arr(vec![entry("2-1", &["b", "2"])])])])
    );
    // Nothing new is left for a third reader.
    assert_eq!(t.run("XREADGROUP GROUP g carol STREAMS s >"), Value::NullArray);
    // History: a consumer's own pending entries.
    assert_eq!(
        t.run("XREADGROUP GROUP g alice STREAMS s 0"),
        arr(vec![arr(vec![bulk("s"), arr(vec![entry("1-1", &["a", "1"])])])])
    );
    assert_eq!(
        t.run("XREADGROUP GROUP g carol STREAMS s 0"),
        arr(vec![arr(vec![bulk("s"), arr(vec![])])])
    );
    assert_eq!(
        t.run("XPENDING s g"),
        arr(vec![
            int(2),
            bulk("1-1"),
            bulk("2-1"),
            arr(vec![arr(vec![bulk("alice"), bulk("1")]), arr(vec![bulk("bob"), bulk("1")])])
        ])
    );
    assert_eq!(t.run("XACK s g 1-1 1-1 9-9"), int(1));
    assert_eq!(t.run("XACK nosuch g 1-1"), int(0));
    assert_eq!(
        t.run("XPENDING s g"),
        arr(vec![int(1), bulk("2-1"), bulk("2-1"), arr(vec![arr(vec![bulk("bob"), bulk("1")])])])
    );
    // A deleted entry still shows up in history, with no fields.
    t.run("XDEL s 2-1");
    assert_eq!(
        t.run("XREADGROUP GROUP g bob STREAMS s 0"),
        arr(vec![arr(vec![bulk("s"), arr(vec![arr(vec![bulk("2-1"), Value::NullArray])])])])
    );
    // NOACK skips the pending list.
    t.run("XADD s 3-1 c 3");
    assert_eq!(
        t.run("XREADGROUP GROUP g alice NOACK STREAMS s >"),
        arr(vec![arr(vec![bulk("s"), arr(vec![entry("3-1", &["c", "3"])])])])
    );
    assert_eq!(
        t.run("XPENDING s g"),
        arr(vec![int(1), bulk("2-1"), bulk("2-1"), arr(vec![arr(vec![bulk("bob"), bulk("1")])])])
    );
    assert_eq!(
        t.run("XREADGROUP GROUP nope alice STREAMS s >"),
        err("NOGROUP No such key 's' or consumer group 'nope' in XREADGROUP with GROUP option")
    );
    assert_eq!(
        t.run("XREADGROUP COUNT 1 BLOCK 0 NOACK STREAMS s >"),
        err("ERR Missing GROUP option for XREADGROUP")
    );
    assert_eq!(
        t.run("XREADGROUP GROUP g alice STREAMS s $"),
        err("ERR The $ ID is meaningless in the context of XREADGROUP: you want to read the \
             history of this consumer by specifying a proper ID, or use the > ID to get new \
             messages. The $ ID would just return an empty result set.")
    );
    assert_eq!(
        t.run("XREAD GROUP g alice STREAMS s >"),
        err("ERR The GROUP option is only supported by XREADGROUP. You called XREAD instead.")
    );
}

#[test]
fn xclaim_and_xautoclaim_move_pending_entries() {
    let mut t = T::new();
    t.run("XADD s 1-1 a 1");
    t.run("XADD s 2-1 b 2");
    t.run("XGROUP CREATE s g 0");
    t.run("XREADGROUP GROUP g alice STREAMS s >");
    t.advance(5000);
    // Too young to claim.
    assert_eq!(t.run("XCLAIM s g bob 10000 1-1"), arr(vec![]));
    assert_eq!(t.run("XCLAIM s g bob 1000 1-1"), arr(vec![entry("1-1", &["a", "1"])]));
    assert_eq!(t.run("XCLAIM s g bob 0 9-9"), arr(vec![]));
    assert_eq!(t.run("XCLAIM s g bob 0 2-1 JUSTID"), bulks(&["2-1"]));
    assert_eq!(
        t.run("XPENDING s g - + 10"),
        arr(vec![
            arr(vec![bulk("1-1"), bulk("bob"), int(0), int(2)]),
            arr(vec![bulk("2-1"), bulk("bob"), int(0), int(1)]),
        ])
    );
    assert_eq!(t.run("XPENDING s g - + 10 alice"), arr(vec![]));
    assert_eq!(t.run("XPENDING s g IDLE 100 - + 10"), arr(vec![]));
    // FORCE creates the pending entry for a message nobody holds.
    t.run("XADD s 3-1 c 3");
    assert_eq!(t.run("XCLAIM s g carol 0 3-1 FORCE JUSTID"), bulks(&["3-1"]));
    assert_eq!(
        t.run("XPENDING s g - + 10 carol"),
        arr(vec![arr(vec![bulk("3-1"), bulk("carol"), int(0), int(0)])])
    );
    assert_eq!(
        t.run("XCLAIM s nogroup bob 0 1-1"),
        err("NOGROUP No such key 's' or consumer group 'nogroup'")
    );
    // XAUTOCLAIM sweeps the whole pending list.
    t.advance(1000);
    assert_eq!(
        t.run("XAUTOCLAIM s g dave 0 0 COUNT 2"),
        arr(vec![
            bulk("3-1"),
            arr(vec![entry("1-1", &["a", "1"]), entry("2-1", &["b", "2"])]),
            arr(vec![])
        ])
    );
    assert_eq!(
        t.run("XAUTOCLAIM s g dave 0 3-1 JUSTID"),
        arr(vec![bulk("0-0"), bulks(&["3-1"]), arr(vec![])])
    );
    // Deleted entries leave the pending list and are reported.
    t.run("XDEL s 1-1");
    assert_eq!(
        t.run("XAUTOCLAIM s g erin 0 0 JUSTID"),
        arr(vec![bulk("0-0"), bulks(&["2-1", "3-1"]), bulks(&["1-1"])])
    );
    assert_eq!(t.run("XAUTOCLAIM s g dave 0 0 COUNT 0"), err("ERR COUNT must be > 0"));
}

#[test]
fn xinfo_reports_groups_and_consumers() {
    let mut t = T::new();
    t.run("XADD s 1-1 a 1");
    t.run("XADD s 2-1 b 2");
    t.run("XGROUP CREATE s g 0");
    t.run("XREADGROUP GROUP g alice COUNT 1 STREAMS s >");
    let Value::Array(groups) = t.run("XINFO GROUPS s") else { panic!() };
    assert_eq!(
        groups[0],
        Value::Map(vec![
            (bulk("name"), bulk("g")),
            (bulk("consumers"), int(1)),
            (bulk("pending"), int(1)),
            (bulk("last-delivered-id"), bulk("1-1")),
            (bulk("entries-read"), int(1)),
            (bulk("lag"), int(1)),
        ])
    );
    let Value::Array(consumers) = t.run("XINFO CONSUMERS s g") else { panic!() };
    assert_eq!(
        consumers[0],
        Value::Map(vec![
            (bulk("name"), bulk("alice")),
            (bulk("pending"), int(1)),
            (bulk("idle"), int(0)),
            (bulk("inactive"), int(0)),
        ])
    );
    assert_eq!(
        t.run("XINFO CONSUMERS s nope"),
        err("NOGROUP No such consumer group 'nope' for key name 's'")
    );
    assert_eq!(t.run("XINFO STREAM nokey"), err("ERR no such key"));
    let Value::Map(info) = t.run("XINFO STREAM s") else { panic!() };
    assert_eq!(info[0], (bulk("length"), int(2)));
    assert_eq!(info[7], (bulk("groups"), int(1)));
    let Value::Map(full) = t.run("XINFO STREAM s FULL") else { panic!() };
    assert_eq!(full[7].0, bulk("entries"));
    assert_eq!(full[8].0, bulk("groups"));
    let Value::Array(groups) = &full[8].1 else { panic!() };
    let Value::Map(g) = &groups[0] else { panic!() };
    assert_eq!(g[0], (bulk("name"), bulk("g")));
    assert_eq!(g[4], (bulk("pel-count"), int(1)));
}

#[test]
fn blocked_xreadgroup_gets_new_entries() {
    let mut t = T::new();
    t.run("XADD s 1-1 a 1");
    t.run("XGROUP CREATE s g $");
    let mut a = t.connect();
    assert_eq!(t.run_as(&mut a, "XREADGROUP GROUP g alice BLOCK 0 STREAMS s >"), Value::NoReply);
    t.run("XADD s 2-1 b 2");
    assert_eq!(
        t.engine.take_reply(&mut a),
        Some(arr(vec![arr(vec![bulk("s"), arr(vec![entry("2-1", &["b", "2"])])])]))
    );
    assert_eq!(
        t.run("XPENDING s g - + 10 alice"),
        arr(vec![arr(vec![bulk("2-1"), bulk("alice"), int(0), int(1)])])
    );
}
