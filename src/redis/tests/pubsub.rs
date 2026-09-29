//! Pub/Sub.

use super::*;

fn push(items: Vec<Value>) -> Value {
    Value::Push(items)
}

#[test]
fn subscribe_publish_unsubscribe() {
    let mut t = T::new();
    let mut sub = t.connect();
    assert_eq!(
        t.run_as(&mut sub, "SUBSCRIBE a b"),
        Value::Many(vec![
            push(vec![bulk("subscribe"), bulk("a"), int(1)]),
            push(vec![bulk("subscribe"), bulk("b"), int(2)]),
        ])
    );
    assert_eq!(
        t.run_as(&mut sub, "PSUBSCRIBE a*"),
        push(vec![bulk("psubscribe"), bulk("a*"), int(3)])
    );
    assert_eq!(t.run("PUBLISH a hi"), int(2));
    assert_eq!(t.run("PUBLISH zzz hi"), int(0));
    assert_eq!(
        t.engine.take_pushes(sub.id),
        vec![
            push(vec![bulk("message"), bulk("a"), bulk("hi")]),
            push(vec![bulk("pmessage"), bulk("a*"), bulk("a"), bulk("hi")]),
        ]
    );
    // RESP2 subscribers may only run a few commands.
    assert_eq!(
        t.run_as(&mut sub, "GET k"),
        err(
            "ERR Can't execute 'get': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context"
        )
    );
    assert_eq!(t.run_as(&mut sub, "PING"), arr(vec![bulk("pong"), bulk("")]));
    assert_eq!(t.run_as(&mut sub, "PING x"), arr(vec![bulk("pong"), bulk("x")]));
    assert_eq!(t.run("PUBSUB CHANNELS"), bulks(&["a", "b"]));
    assert_eq!(t.run("PUBSUB CHANNELS b*"), bulks(&["b"]));
    assert_eq!(t.run("PUBSUB NUMSUB a nope"), arr(vec![bulk("a"), int(1), bulk("nope"), int(0)]));
    assert_eq!(t.run("PUBSUB NUMPAT"), int(1));
    let list = text(&t.run("CLIENT LIST TYPE pubsub"));
    assert!(list.contains(" flags=P ") && list.contains(" sub=2 psub=1 ssub=0 "), "{list}");
    assert_eq!(
        t.run_as(&mut sub, "UNSUBSCRIBE a"),
        push(vec![bulk("unsubscribe"), bulk("a"), int(2)])
    );
    assert_eq!(
        t.run_as(&mut sub, "PUNSUBSCRIBE"),
        push(vec![bulk("punsubscribe"), bulk("a*"), int(1)])
    );
    assert_eq!(t.run_as(&mut sub, "PUNSUBSCRIBE"), push(vec![bulk("punsubscribe"), nil(), int(1)]));
    assert_eq!(
        t.run_as(&mut sub, "UNSUBSCRIBE"),
        push(vec![bulk("unsubscribe"), bulk("b"), int(0)])
    );
    assert_eq!(t.run_as(&mut sub, "GET k"), nil());
}

#[test]
fn shard_channels_and_resp3() {
    let mut t = T::new();
    let mut sub = t.connect();
    t.run_as(&mut sub, "HELLO 3");
    assert_eq!(
        t.run_as(&mut sub, "SSUBSCRIBE s"),
        push(vec![bulk("ssubscribe"), bulk("s"), int(1)])
    );
    assert_eq!(t.run_as(&mut sub, "SUBSCRIBE c"), push(vec![bulk("subscribe"), bulk("c"), int(1)]));
    // RESP3 subscribers can run anything.
    assert_eq!(t.run_as(&mut sub, "SET k v"), ok());
    assert_eq!(t.run("SPUBLISH s m"), int(1));
    assert_eq!(t.run("PUBSUB SHARDCHANNELS"), bulks(&["s"]));
    assert_eq!(t.run("PUBSUB SHARDNUMSUB s"), arr(vec![bulk("s"), int(1)]));
    assert_eq!(
        t.engine.take_pushes(sub.id),
        vec![push(vec![bulk("smessage"), bulk("s"), bulk("m")])]
    );
    assert_eq!(t.run_as(&mut sub, "RESET"), simple("RESET"));
    assert_eq!(t.run("PUBLISH c x"), int(0));
    t.run("MULTI");
    t.run("SSUBSCRIBE s");
    t.run("SUBSCRIBE c");
    assert_eq!(
        t.run("EXEC"),
        arr(vec![
            err("ERR SSUBSCRIBE isn't allowed for a DENY BLOCKING client"),
            push(vec![bulk("subscribe"), bulk("c"), int(1)])
        ])
    );
}
