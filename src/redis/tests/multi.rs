//! MULTI/EXEC/DISCARD/WATCH.

use super::*;

const EXECABORT: &str = "EXECABORT Transaction discarded because of previous errors.";

#[test]
fn queue_and_exec() {
    let mut t = T::new();
    assert_eq!(t.run("MULTI"), ok());
    assert_eq!(t.run("SET k 1"), simple("QUEUED"));
    assert_eq!(t.run("INCR k"), simple("QUEUED"));
    assert_eq!(t.run("LPUSH k x"), simple("QUEUED"));
    assert_eq!(t.run("GET k"), simple("QUEUED"));
    let mut other = t.connect();
    let list = text(&t.run_as(&mut other, "CLIENT LIST"));
    assert!(list.contains(" flags=x ") && list.contains(" multi=4 "), "{list}");
    assert_eq!(
        t.run("EXEC"),
        arr(vec![
            ok(),
            int(2),
            err("WRONGTYPE Operation against a key holding the wrong kind of value"),
            bulk("2")
        ])
    );
    assert_eq!(t.run("EXEC"), err("ERR EXEC without MULTI"));
    assert_eq!(t.run("DISCARD"), err("ERR DISCARD without MULTI"));
    t.run("MULTI");
    assert_eq!(t.run("MULTI"), err("ERR MULTI calls can not be nested"));
    assert_eq!(t.run("WATCH k"), err("ERR WATCH inside MULTI is not allowed"));
    t.run("SET k 5");
    assert_eq!(t.run("DISCARD"), ok());
    assert_eq!(t.run("GET k"), bulk("2"));
    t.run("MULTI");
    assert_eq!(t.run("EXEC"), arr(vec![]));
}

#[test]
fn queueing_errors_abort_exec() {
    let mut t = T::new();
    t.run("MULTI");
    t.run("SET k 1");
    assert_eq!(t.run("GET"), err("ERR wrong number of arguments for 'get' command"));
    assert_eq!(t.run("NOPE"), err("ERR unknown command 'NOPE', with args beginning with: "));
    assert_eq!(t.run("EXEC"), err(EXECABORT));
    assert_eq!(t.run("GET k"), nil());
    t.run("MULTI");
    assert_eq!(
        t.run("EXEC x"),
        err(
            "EXECABORT Transaction discarded because of: wrong number of arguments for 'exec' command"
        )
    );
    assert_eq!(t.run("EXEC"), err("ERR EXEC without MULTI"));
}

#[test]
fn watch_detects_writes_from_other_clients() {
    let mut t = T::new();
    let mut other = t.connect();
    t.run("SET k 1");
    assert_eq!(t.run("WATCH k"), ok());
    t.run_as(&mut other, "SET k 2");
    t.run("MULTI");
    t.run("SET k 3");
    assert_eq!(t.run("EXEC"), Value::NullArray);
    assert_eq!(t.run("GET k"), bulk("2"));
    // Unwatched after EXEC: the next transaction runs.
    t.run("MULTI");
    t.run("SET k 3");
    assert_eq!(t.run("EXEC"), arr(vec![ok()]));
    // Writes to other keys, reads and failed writes don't count.
    t.run("WATCH k");
    t.run_as(&mut other, "SET j 1");
    t.run_as(&mut other, "GET k");
    t.run_as(&mut other, "LPUSH k x");
    t.run("MULTI");
    t.run("GET k");
    assert_eq!(t.run("EXEC"), arr(vec![bulk("3")]));
    // UNWATCH and DISCARD forget watched keys.
    t.run("WATCH k");
    t.run("UNWATCH");
    t.run_as(&mut other, "SET k 9");
    t.run("MULTI");
    assert_eq!(t.run("EXEC"), arr(vec![]));
    // FLUSHALL touches keys that existed; a watched key expiring counts.
    t.run("WATCH k");
    t.run_as(&mut other, "FLUSHALL");
    t.run("MULTI");
    assert_eq!(t.run("EXEC"), Value::NullArray);
    t.run("SET k v PX 100");
    t.run("WATCH k");
    t.advance(200);
    t.run("MULTI");
    assert_eq!(t.run("EXEC"), Value::NullArray);
}

#[test]
fn blocking_commands_dont_block_in_exec() {
    let mut t = T::new();
    t.run("MULTI");
    t.run("BLPOP q 0");
    t.run("BLMOVE q r LEFT LEFT 0");
    t.run("BZPOPMIN z 0");
    assert_eq!(t.run("EXEC"), arr(vec![Value::NullArray, nil(), Value::NullArray]));
    assert!(!t.session.blocked);
    // A push inside a transaction serves waiters after EXEC.
    let mut w = t.connect();
    t.run_as(&mut w, "BLPOP q 0");
    t.run("MULTI");
    t.run("RPUSH q a");
    t.run("RPUSH q b");
    assert_eq!(t.run("EXEC"), arr(vec![int(1), int(2)]));
    assert_eq!(t.engine.take_reply(&mut w), Some(bulks(&["q", "a"])));
}

#[test]
fn persistence_commands_reply_like_redis() {
    let mut t = T::new();
    assert_eq!(t.run("LASTSAVE"), int((START_MS / 1000) as i64));
    t.advance(5000);
    assert_eq!(t.run("SAVE"), ok());
    assert_eq!(t.run("LASTSAVE"), int((START_MS / 1000 + 5) as i64));
    assert_eq!(t.run("BGSAVE"), simple("Background saving started"));
    assert_eq!(t.run("BGSAVE SCHEDULE"), simple("Background saving started"));
    assert_eq!(t.run("BGSAVE foo"), err(SYNTAX));
    assert_eq!(t.run("BGREWRITEAOF"), simple("Background append only file rewriting started"));
    let Value::Array(time) = t.run("TIME") else { panic!() };
    assert_eq!(time.len(), 2);
    t.run("MULTI");
    assert_eq!(t.run("SAVE"), err("ERR Command not allowed inside a transaction"));
    assert_eq!(t.run("EXEC"), err("EXECABORT Transaction discarded because of previous errors."));
}
