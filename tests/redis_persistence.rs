//! Real on-disk persistence for Redis: save on a clean "shutdown"
//! (simulated directly here via the save closure `spawn_persistent_for_test`
//! returns, not a real SIGTERM -- unreliable to send/observe in a test),
//! load back into a fresh server, and confirm every data type round-trips
//! correctly, not just plain strings.

use noida::redis::server::spawn_persistent_for_test;
use redis::Commands;
use std::net::SocketAddr;

fn connect(addr: SocketAddr) -> redis::Connection {
    redis::Client::open(format!("redis://{addr}/"))
        .unwrap()
        .get_connection()
        .expect("client connects")
}

#[test]
fn persists_every_data_type_across_a_restart() {
    let dir = std::env::temp_dir().join(format!("noida-redis-test-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let (addr1, save1) = spawn_persistent_for_test("127.0.0.1:0", &dir).unwrap();
    {
        let mut con = connect(addr1);
        let _: () = con.set("greeting", "hello").unwrap();
        let _: () = con.set_ex("with_ttl", "soon", 3600).unwrap();
        let _: () = con.hset("a_hash", "field1", "value1").unwrap();
        let _: () = con.hset("a_hash", "field2", "value2").unwrap();
        let _: () = con.sadd("a_set", "member1").unwrap();
        let _: () = con.sadd("a_set", "member2").unwrap();
        let _: () = con.zadd("a_zset", "one", 1.0).unwrap();
        let _: () = con.zadd("a_zset", "two", 2.0).unwrap();
        let _: () = con.rpush("a_list", "x").unwrap();
        let _: () = con.rpush("a_list", "y").unwrap();
        // A stream with a consumer group -- exercises the Id-tuple-keyed
        // and Vec<u8>-keyed maps inside Stream/Group specifically.
        let _: String = redis::cmd("XADD")
            .arg("a_stream")
            .arg("*")
            .arg("field")
            .arg("value")
            .query(&mut con)
            .unwrap();
        let _: () = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg("a_stream")
            .arg("a_group")
            .arg("0")
            .query(&mut con)
            .unwrap();

        // Select db 1 too, to confirm more than just db 0 survives.
        let _: () = redis::cmd("SELECT").arg(1).query(&mut con).unwrap();
        let _: () = con.set("in_db_one", "yes").unwrap();
    }

    // Trigger save directly (see module doc for why not a real signal).
    save1();
    assert!(dir.join("redis.json").exists());

    // Start a second, completely fresh server against the same dir.
    let (addr2, _save2) = spawn_persistent_for_test("127.0.0.1:0", &dir).unwrap();
    let mut con = connect(addr2);

    let v: String = con.get("greeting").unwrap();
    assert_eq!(v, "hello");
    let ttl: i64 = con.ttl("with_ttl").unwrap();
    assert!(ttl > 0 && ttl <= 3600);

    let hash: std::collections::HashMap<String, String> = con.hgetall("a_hash").unwrap();
    assert_eq!(hash.get("field1"), Some(&"value1".to_string()));
    assert_eq!(hash.get("field2"), Some(&"value2".to_string()));

    let mut members: Vec<String> = con.smembers("a_set").unwrap();
    members.sort();
    assert_eq!(members, vec!["member1".to_string(), "member2".to_string()]);

    let zrange: Vec<String> = con.zrange("a_zset", 0, -1).unwrap();
    assert_eq!(zrange, vec!["one".to_string(), "two".to_string()]);
    let score: f64 = con.zscore("a_zset", "two").unwrap();
    assert_eq!(score, 2.0);

    let list: Vec<String> = con.lrange("a_list", 0, -1).unwrap();
    assert_eq!(list, vec!["x".to_string(), "y".to_string()]);

    let stream_len: i64 = redis::cmd("XLEN").arg("a_stream").query(&mut con).unwrap();
    assert_eq!(stream_len, 1);
    let groups: redis::Value =
        redis::cmd("XINFO").arg("GROUPS").arg("a_stream").query(&mut con).unwrap();
    // Just confirm the group survived at all (a real reply, not empty/nil).
    match groups {
        redis::Value::Array(v) => assert_eq!(v.len(), 1),
        other => panic!("unexpected XINFO GROUPS reply: {other:?}"),
    }

    let _: () = redis::cmd("SELECT").arg(1).query(&mut con).unwrap();
    let v: String = con.get("in_db_one").unwrap();
    assert_eq!(v, "yes");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn persistent_server_starts_empty_when_no_data_dir() {
    let dir = std::env::temp_dir().join(format!("noida-redis-test-empty-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let (addr, _save) = spawn_persistent_for_test("127.0.0.1:0", &dir).unwrap();
    let mut con = connect(addr);
    let v: Option<String> = con.get("anything").unwrap();
    assert_eq!(v, None);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn expired_key_is_not_resurrected_after_reload() {
    let dir = std::env::temp_dir().join(format!("noida-redis-test-expiry-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let (addr1, save1) = spawn_persistent_for_test("127.0.0.1:0", &dir).unwrap();
    {
        let mut con = connect(addr1);
        // A very short TTL, then sleep past it -- the key should still be
        // in the snapshot (lazy expiry doesn't proactively delete it) but
        // must not be served as live data after reload.
        let _: () = con.set_ex("soon_gone", "x", 1).unwrap();
    }
    std::thread::sleep(std::time::Duration::from_millis(1100));
    save1();

    let (addr2, _save2) = spawn_persistent_for_test("127.0.0.1:0", &dir).unwrap();
    let mut con = connect(addr2);
    let v: Option<String> = con.get("soon_gone").unwrap();
    assert_eq!(v, None);

    let _ = std::fs::remove_dir_all(&dir);
}
