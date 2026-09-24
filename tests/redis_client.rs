//! A real Redis client library (redis-rs) against noida over TCP.

mod common;

use redis::{Commands, RedisResult};

fn connect() -> redis::Connection {
    let addr = common::start_noida_redis();
    redis::Client::open(format!("redis://{addr}/"))
        .unwrap()
        .get_connection()
        .expect("client connects")
}

#[test]
fn set_get_del() -> RedisResult<()> {
    let mut con = connect();
    let _: () = con.set("greeting", "hello")?;
    let v: String = con.get("greeting")?;
    assert_eq!(v, "hello");
    let missing: Option<String> = con.get("missing")?;
    assert_eq!(missing, None);
    let n: i64 = con.del("greeting")?;
    assert_eq!(n, 1);
    Ok(())
}

#[test]
fn counters_and_expiry() -> RedisResult<()> {
    let mut con = connect();
    let n: i64 = con.incr("hits", 5)?;
    assert_eq!(n, 5);
    let _: () = con.set_ex("session", "abc", 60)?;
    let ttl: i64 = con.ttl("session")?;
    assert_eq!(ttl, 60);
    Ok(())
}

#[test]
fn pipelining() -> RedisResult<()> {
    let mut con = connect();
    let (a, b, c): (String, i64, Vec<Option<String>>) = redis::pipe()
        .set("a", "1")
        .ignore()
        .get("a")
        .incr("b", 2)
        .mget(&["a", "nope"])
        .query(&mut con)?;
    assert_eq!(a, "1");
    assert_eq!(b, 2);
    assert_eq!(c, vec![Some("1".to_string()), None]);
    Ok(())
}

#[test]
fn database_selection_in_url() -> RedisResult<()> {
    let addr = common::start_noida_redis();
    let mut db0 = redis::Client::open(format!("redis://{addr}/0"))?.get_connection()?;
    let mut db3 = redis::Client::open(format!("redis://{addr}/3"))?.get_connection()?;
    let _: () = db3.set("k", "in-3")?;
    let v: Option<String> = db0.get("k")?;
    assert_eq!(v, None);
    let v: String = db3.get("k")?;
    assert_eq!(v, "in-3");
    Ok(())
}

#[test]
fn server_errors_reach_the_client() {
    let mut con = connect();
    let _: () = con.set("s", "text").unwrap();
    let r: RedisResult<i64> = con.incr("s", 1);
    let e = r.unwrap_err();
    assert_eq!(e.code(), Some("ERR"));
    assert!(e.to_string().contains("value is not an integer or out of range"));
}

#[test]
fn many_concurrent_clients() {
    let addr = common::start_noida_redis();
    let handles: Vec<_> = (0..16)
        .map(|i| {
            std::thread::spawn(move || {
                let mut con = redis::Client::open(format!("redis://{addr}/"))
                    .unwrap()
                    .get_connection()
                    .unwrap();
                for _ in 0..100 {
                    let _: i64 = con.incr("shared", 1).unwrap();
                }
                let _: () = con.set(format!("own:{i}"), i).unwrap();
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    let mut con =
        redis::Client::open(format!("redis://{addr}/")).unwrap().get_connection().unwrap();
    let total: i64 = con.get("shared").unwrap();
    assert_eq!(total, 1600);
}

#[test]
fn raw_protocol_edge_cases() {
    use noida::redis::resp::Value;
    let addr = common::start_noida_redis();
    let mut c = common::RawClient::connect(addr);
    // Inline commands work, as with `telnet` or `nc`.
    c.send_raw(b"PING\r\n");
    assert_eq!(c.read(), Some(Value::Simple("PONG".into())));
    // Pipelined requests in one write get replies in order.
    c.send_raw(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n*2\r\n$3\r\nGET\r\n$1\r\nk\r\n");
    assert_eq!(c.read(), Some(Value::ok()));
    assert_eq!(c.read(), Some(Value::bulk("v")));
    // QUIT replies OK and closes.
    c.send_raw(b"QUIT\r\n");
    assert_eq!(c.read(), Some(Value::ok()));
    assert_eq!(c.read(), None);
}

#[test]
fn protocol_error_closes_the_connection() {
    use noida::redis::resp::Value;
    let addr = common::start_noida_redis();
    let mut c = common::RawClient::connect(addr);
    c.send_raw(b"*1\r\n$abc\r\n");
    assert_eq!(c.read(), Some(Value::err("ERR Protocol error: invalid bulk length")));
    assert_eq!(c.read(), None);
}

#[test]
fn resp3_clients_work() -> RedisResult<()> {
    let addr = common::start_noida_redis();
    let mut con =
        redis::Client::open(format!("redis://{addr}/?protocol=resp3"))?.get_connection()?;
    let _: () = con.set("k", "v")?;
    let v: String = con.get("k")?;
    assert_eq!(v, "v");
    let missing: Option<String> = con.get("nope")?;
    assert_eq!(missing, None);
    let info: String = redis::cmd("CLIENT").arg("INFO").query(&mut con)?;
    assert!(info.contains(" resp=3 "), "{info}");
    Ok(())
}

#[test]
fn client_kill_closes_the_other_connection() {
    use noida::redis::resp::Value;
    let addr = common::start_noida_redis();
    let mut victim = common::RawClient::connect(addr);
    let Value::Integer(victim_id) = victim.run("CLIENT ID") else { panic!() };
    let mut killer = common::RawClient::connect(addr);
    assert_eq!(killer.run(&format!("CLIENT KILL ID {victim_id}")), Value::Integer(1));
    // The victim's socket is closed: its next read hits EOF.
    victim.send_raw(b"PING\r\n");
    assert_eq!(victim.read(), None);
}

#[test]
fn client_pause_holds_writes_from_other_clients() {
    use noida::redis::resp::Value;
    use std::time::{Duration, Instant};
    let addr = common::start_noida_redis();
    let mut admin = common::RawClient::connect(addr);
    let mut writer = common::RawClient::connect(addr);
    assert_eq!(admin.run("CLIENT PAUSE 300 WRITE"), Value::ok());
    // Reads go straight through...
    let start = Instant::now();
    assert_eq!(writer.run("GET k"), Value::Null);
    assert!(start.elapsed() < Duration::from_millis(200));
    // ...writes wait for the pause to end.
    assert_eq!(writer.run("SET k v"), Value::ok());
    assert!(start.elapsed() >= Duration::from_millis(250), "{:?}", start.elapsed());
}

#[test]
fn hashes_through_a_real_client() -> RedisResult<()> {
    use std::collections::HashMap;
    let addr = common::start_noida_redis();
    for url in [format!("redis://{addr}/"), format!("redis://{addr}/?protocol=resp3")] {
        let mut con = redis::Client::open(url)?.get_connection()?;
        let _: () = con.del("user:1")?;
        let _: () = con.hset_multiple("user:1", &[("name", "Ada"), ("lang", "rust")])?;
        let n: i64 = con.hincr("user:1", "visits", 3)?;
        assert_eq!(n, 3);
        let all: HashMap<String, String> = con.hgetall("user:1")?;
        assert_eq!(all.len(), 3);
        assert_eq!(all["name"], "Ada");
        let name: Option<String> = con.hget("user:1", "name")?;
        assert_eq!(name.as_deref(), Some("Ada"));
    }
    Ok(())
}

#[test]
fn lists_through_a_real_client() -> RedisResult<()> {
    let addr = common::start_noida_redis();
    for url in [format!("redis://{addr}/"), format!("redis://{addr}/?protocol=resp3")] {
        let mut con = redis::Client::open(url)?.get_connection()?;
        let _: () = con.del("jobs")?;
        let n: i64 = con.rpush("jobs", &["a", "b", "c"])?;
        assert_eq!(n, 3);
        let _: i64 = con.lpush("jobs", "z")?;
        let all: Vec<String> = con.lrange("jobs", 0, -1)?;
        assert_eq!(all, ["z", "a", "b", "c"]);
        let first: Option<String> = con.lpop("jobs", None)?;
        assert_eq!(first.as_deref(), Some("z"));
        let two: Vec<String> = con.rpop("jobs", std::num::NonZeroUsize::new(2))?;
        assert_eq!(two, ["c", "b"]);
        let popped: Option<(String, String)> = con.blpop("jobs", 0.0)?;
        assert_eq!(popped, Some(("jobs".into(), "a".into())));
        // Times out with a nil reply.
        let none: Option<(String, String)> = con.brpop("jobs", 0.1)?;
        assert_eq!(none, None);
    }
    Ok(())
}

/// Clients blocked on a key are served in the order they blocked, when the
/// data arrives.
#[test]
fn blocked_clients_are_served_in_order() {
    use noida::redis::resp::Value;
    let addr = common::start_noida_redis();
    let mut admin = common::RawClient::connect(addr);
    let mut waiters = Vec::new();
    for i in 1..=3 {
        let mut c = common::RawClient::connect(addr);
        c.send(&[b"BLPOP", b"queue", b"0"]);
        // Let the server see each BLPOP before the next one.
        wait_for_blocked(&mut admin, i);
        waiters.push(c);
    }
    assert_eq!(admin.run("RPUSH queue 1 2 3"), Value::Integer(3));
    for (i, c) in waiters.iter_mut().enumerate() {
        let want = Value::Array(vec![Value::bulk("queue"), Value::bulk((i + 1).to_string())]);
        assert_eq!(c.read(), Some(want));
    }
}

#[test]
fn blocking_timeouts_unblock_and_disconnects() {
    use noida::redis::resp::Value;
    use std::time::{Duration, Instant};
    let addr = common::start_noida_redis();
    let mut c = common::RawClient::connect(addr);
    let start = Instant::now();
    assert_eq!(c.run("BRPOP nothing 0.2"), Value::NullArray);
    let took = start.elapsed();
    assert!(took >= Duration::from_millis(200) && took < Duration::from_secs(1), "{took:?}");

    // A client that disconnects while blocked must not take the data.
    let mut gone = common::RawClient::connect(addr);
    gone.send(&[b"BLPOP", b"k", b"0"]);
    wait_for_blocked(&mut c, 1);
    drop(gone);
    wait_for_blocked(&mut c, 0);
    assert_eq!(c.run("RPUSH k v"), Value::Integer(1));
    assert_eq!(c.run("LLEN k"), Value::Integer(1));

    // CLIENT UNBLOCK wakes a waiter with a timeout reply.
    let mut waiter = common::RawClient::connect(addr);
    let Value::Integer(id) = waiter.run("CLIENT ID") else { panic!() };
    waiter.send(&[b"BLPOP", b"empty", b"0"]);
    wait_for_blocked(&mut c, 1);
    assert_eq!(c.run(&format!("CLIENT UNBLOCK {id}")), Value::Integer(1));
    assert_eq!(waiter.read(), Some(Value::NullArray));
    // The connection keeps working afterwards.
    assert_eq!(waiter.run("PING"), Value::Simple("PONG".into()));
}

/// Polls INFO until exactly `n` clients are blocked.
fn wait_for_blocked(c: &mut common::RawClient, n: usize) {
    use noida::redis::resp::Value;
    let want = format!("blocked_clients:{n}\r\n");
    for _ in 0..500 {
        let Value::Bulk(info) = c.run("INFO clients") else { panic!() };
        if String::from_utf8(info).unwrap().contains(&want) {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    panic!("never saw {want:?}");
}

#[test]
fn sets_through_a_real_client() -> RedisResult<()> {
    use std::collections::HashSet;
    let addr = common::start_noida_redis();
    for url in [format!("redis://{addr}/"), format!("redis://{addr}/?protocol=resp3")] {
        let mut con = redis::Client::open(url)?.get_connection()?;
        let _: () = con.del(&["tags", "other"])?;
        let n: i64 = con.sadd("tags", &["rust", "redis", "db"])?;
        assert_eq!(n, 3);
        let _: i64 = con.sadd("other", &["db", "sql"])?;
        let all: HashSet<String> = con.smembers("tags")?;
        assert_eq!(all.len(), 3);
        let yes: bool = con.sismember("tags", "rust")?;
        assert!(yes);
        let inter: Vec<String> = con.sinter(&["tags", "other"])?;
        assert_eq!(inter, ["db"]);
        let ordered: Vec<i64> = {
            let _: i64 = con.sadd("nums", &[3, 1, 2])?;
            con.smembers("nums")?
        };
        assert_eq!(ordered, [1, 2, 3]);
        let _: () = con.del("nums")?;
    }
    Ok(())
}

#[test]
fn sorted_sets_through_a_real_client() -> RedisResult<()> {
    let addr = common::start_noida_redis();
    for url in [format!("redis://{addr}/"), format!("redis://{addr}/?protocol=resp3")] {
        let mut con = redis::Client::open(url)?.get_connection()?;
        let _: () = con.del("board")?;
        let n: i64 = con.zadd_multiple("board", &[(10.5, "ada"), (3.0, "bob"), (7.0, "cy")])?;
        assert_eq!(n, 3);
        let top: Vec<(String, f64)> = con.zrevrange_withscores("board", 0, 1)?;
        assert_eq!(top, [("ada".to_string(), 10.5), ("cy".to_string(), 7.0)]);
        let score: Option<f64> = con.zscore("board", "bob")?;
        assert_eq!(score, Some(3.0));
        let s: f64 = con.zincr("board", "bob", 0.25)?;
        assert_eq!(s, 3.25);
        let rank: Option<i64> = con.zrank("board", "cy")?;
        assert_eq!(rank, Some(1));
        let low: Vec<String> = con.zrangebyscore("board", "-inf", 8)?;
        assert_eq!(low, ["bob", "cy"]);
    }
    Ok(())
}

#[test]
fn transactions_through_a_real_client() -> RedisResult<()> {
    let addr = common::start_noida_redis();
    for url in [format!("redis://{addr}/"), format!("redis://{addr}/?protocol=resp3")] {
        let client = redis::Client::open(url)?;
        let mut con = client.get_connection()?;
        let mut other = client.get_connection()?;
        let _: () = con.set("balance", 10)?;
        // MULTI/EXEC through an atomic pipeline.
        let (a, b): (i64, i64) =
            redis::pipe().atomic().incr("balance", 5).decr("balance", 3).query(&mut con)?;
        assert_eq!((a, b), (15, 12));
        // WATCH: a concurrent write makes EXEC return nil.
        let _: () = redis::cmd("WATCH").arg("balance").query(&mut con)?;
        let _: () = other.set("balance", 100)?;
        let r: Option<(i64,)> = redis::pipe().atomic().incr("balance", 1).query(&mut con)?;
        assert_eq!(r, None);
        // The optimistic-locking helper retries and succeeds.
        let (n,): (i64,) = redis::transaction(&mut con, &["balance"], |con, pipe| {
            let v: i64 = con.get("balance")?;
            pipe.set("balance", v * 2).ignore().get("balance").query(con)
        })?;
        assert_eq!(n, 200);
    }
    Ok(())
}

#[test]
fn pubsub_through_a_real_client() -> RedisResult<()> {
    let addr = common::start_noida_redis();
    let publisher_client = redis::Client::open(format!("redis://{addr}/"))?;
    let mut publisher = publisher_client.get_connection()?;
    // RESP2: a dedicated subscriber connection.
    let client = redis::Client::open(format!("redis://{addr}/"))?;
    let mut con = client.get_connection()?;
    let mut pubsub = con.as_pubsub();
    pubsub.subscribe("news")?;
    pubsub.psubscribe("sport.*")?;
    let n: i64 = publisher.publish("news", "hello")?;
    assert_eq!(n, 1);
    let _: i64 = publisher.publish("sport.f1", "fast")?;
    let msg = pubsub.get_message()?;
    assert_eq!(msg.get_channel_name(), "news");
    assert_eq!(msg.get_payload::<String>()?, "hello");
    let msg = pubsub.get_message()?;
    assert_eq!(msg.get_channel_name(), "sport.f1");
    assert_eq!(msg.get_pattern::<String>()?, "sport.*");
    assert_eq!(msg.get_payload::<String>()?, "fast");
    pubsub.unsubscribe("news")?;
    pubsub.punsubscribe("sport.*")?;
    drop(pubsub);
    // Back to normal commands once unsubscribed.
    let _: () = con.set("k", "v")?;

    // RESP3: pushes arrive on a connection that keeps running commands.
    let (tx, rx) = std::sync::mpsc::channel();
    let client3 = redis::Client::open(format!("redis://{addr}/?protocol=resp3"))?;
    let mut con3 = client3.get_connection()?;
    con3.set_push_sender(tx);
    // SUBSCRIBE over RESP3 doesn't wait for a reply, and this client only
    // dispatches pushes while it reads one, so run a command to make sure
    // the subscription is in place before publishing, and another to pick
    // the message up.
    con3.subscribe_resp3("alerts")?;
    let v: String = con3.set("x", "1").and_then(|()| con3.get("x"))?;
    assert_eq!(v, "1");
    let _: i64 = publisher.publish("alerts", "fire")?;
    let _: String = con3.get("x")?;
    let mut seen = false;
    while let Ok(push) = rx.recv_timeout(std::time::Duration::from_secs(10)) {
        if push.kind == redis::PushKind::Message {
            assert_eq!(push.data[1], redis::Value::BulkString(b"fire".to_vec()));
            seen = true;
            break;
        }
    }
    assert!(seen, "no RESP3 push message");
    Ok(())
}

/// A worker loop the way a job queue uses streams: read new entries for a
/// consumer, ack them, and take over a dead worker's pending entry.
#[test]
fn stream_consumer_groups_through_a_real_client() -> RedisResult<()> {
    use redis::streams::{StreamAutoClaimOptions, StreamAutoClaimReply, StreamReadOptions};
    let addr = common::start_noida_redis();
    let mut con = redis::Client::open(format!("redis://{addr}/"))?.get_connection()?;
    let _: String = con.xadd("jobs", "1-1", &[("task", "a")])?;
    let _: String = con.xadd("jobs", "2-1", &[("task", "b")])?;
    let _: () = con.xgroup_create("jobs", "workers", "0")?;

    let opts = StreamReadOptions::default().group("workers", "worker-1").count(1);
    let reply: redis::streams::StreamReadReply = con.xread_options(&["jobs"], &[">"], &opts)?;
    assert_eq!(reply.keys.len(), 1);
    assert_eq!(reply.keys[0].ids.len(), 1);
    assert_eq!(reply.keys[0].ids[0].id, "1-1");
    let acked: i64 = con.xack("jobs", "workers", &["1-1"])?;
    assert_eq!(acked, 1);

    // worker-2 takes the entry worker-1 left pending.
    let opts = StreamReadOptions::default().group("workers", "worker-2").count(1);
    let _: redis::streams::StreamReadReply = con.xread_options(&["jobs"], &[">"], &opts)?;
    let pending: redis::streams::StreamPendingReply = con.xpending("jobs", "workers")?;
    assert_eq!(pending.count(), 1);
    let claimed: StreamAutoClaimReply = con.xautoclaim_options(
        "jobs",
        "workers",
        "worker-3",
        0,
        "0",
        StreamAutoClaimOptions::default(),
    )?;
    assert_eq!(claimed.claimed.len(), 1);
    assert_eq!(claimed.claimed[0].id, "2-1");
    let info: redis::streams::StreamInfoConsumersReply = con.xinfo_consumers("jobs", "workers")?;
    let names: Vec<&str> = info.consumers.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, ["worker-1", "worker-2", "worker-3"]);
    Ok(())
}
