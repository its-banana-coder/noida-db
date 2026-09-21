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
