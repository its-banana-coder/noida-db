//! List commands, including the blocking ones.

use super::*;

const WRONGTYPE: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";

#[test]
fn push_pop_and_range() {
    let mut t = T::new();
    assert_eq!(t.run("RPUSH l a b c"), int(3));
    assert_eq!(t.run("LPUSH l z y"), int(5));
    assert_eq!(t.run("LRANGE l 0 -1"), bulks(&["y", "z", "a", "b", "c"]));
    assert_eq!(t.run("LRANGE l 1 2"), bulks(&["z", "a"]));
    assert_eq!(t.run("LRANGE l -2 100"), bulks(&["b", "c"]));
    assert_eq!(t.run("LRANGE l 3 1"), arr(vec![]));
    assert_eq!(t.run("LRANGE l 10 20"), arr(vec![]));
    assert_eq!(t.run("LRANGE nokey 0 -1"), arr(vec![]));
    assert_eq!(t.run("LRANGE l x 1"), err(NOT_INT));
    assert_eq!(t.run("LLEN l"), int(5));
    assert_eq!(t.run("LLEN nokey"), int(0));
    assert_eq!(t.run("LPOP l"), bulk("y"));
    assert_eq!(t.run("RPOP l"), bulk("c"));
    assert_eq!(t.run("LPOP l 2"), bulks(&["z", "a"]));
    assert_eq!(t.run("LPOP l 0"), arr(vec![]));
    assert_eq!(t.run("RPOP l 5"), bulks(&["b"]));
    // Popping the last element deletes the key.
    assert_eq!(t.run("EXISTS l"), int(0));
    assert_eq!(t.run("LPOP l"), nil());
    assert_eq!(t.run("LPOP l 2"), Value::NullArray);
    assert_eq!(t.run("LPOP l -1"), err("ERR value is out of range, must be positive"));
    assert_eq!(t.run("LPOP l x"), err("ERR value is out of range, must be positive"));
    assert_eq!(t.run("LPOP l 1 2"), err("ERR wrong number of arguments for 'lpop' command"));
    t.run("SET s v");
    assert_eq!(t.run("LPUSH s a"), err(WRONGTYPE));
    assert_eq!(t.run("LRANGE s 0 1"), err(WRONGTYPE));
    assert_eq!(t.run("TYPE s"), simple("string"));
    t.run("RPUSH l a");
    assert_eq!(t.run("TYPE l"), simple("list"));
}

#[test]
fn pushx_insert_index_set() {
    let mut t = T::new();
    assert_eq!(t.run("LPUSHX l a"), int(0));
    assert_eq!(t.run("RPUSHX l a"), int(0));
    assert_eq!(t.run("EXISTS l"), int(0));
    t.run("RPUSH l a c");
    assert_eq!(t.run("RPUSHX l d e"), int(4));
    assert_eq!(t.run("LPUSHX l 0"), int(5));
    assert_eq!(t.run("LINSERT l BEFORE c b"), int(6));
    assert_eq!(t.run("LINSERT l after e f"), int(7));
    assert_eq!(t.run("LINSERT l AFTER nope x"), int(-1));
    assert_eq!(t.run("LINSERT nokey AFTER a x"), int(0));
    assert_eq!(t.run("LINSERT l MIDDLE a x"), err(SYNTAX));
    assert_eq!(t.run("LRANGE l 0 -1"), bulks(&["0", "a", "b", "c", "d", "e", "f"]));
    assert_eq!(t.run("LINDEX l 0"), bulk("0"));
    assert_eq!(t.run("LINDEX l -1"), bulk("f"));
    assert_eq!(t.run("LINDEX l 7"), nil());
    assert_eq!(t.run("LINDEX l x"), err(NOT_INT));
    assert_eq!(t.run("LINDEX nokey 0"), nil());
    assert_eq!(t.run("LSET l 1 A"), ok());
    assert_eq!(t.run("LSET l -1 F"), ok());
    assert_eq!(t.run("LSET l 7 x"), err("ERR index out of range"));
    assert_eq!(t.run("LSET nokey 0 x"), err("ERR no such key"));
    assert_eq!(t.run("LRANGE l 0 -1"), bulks(&["0", "A", "b", "c", "d", "e", "F"]));
}

#[test]
fn trim_rem_pos() {
    let mut t = T::new();
    t.run("RPUSH l a b a c a d");
    assert_eq!(t.run("LPOS l a"), int(0));
    assert_eq!(t.run("LPOS l a RANK 2"), int(2));
    assert_eq!(t.run("LPOS l a RANK -1"), int(4));
    assert_eq!(t.run("LPOS l a COUNT 0"), arr(vec![int(0), int(2), int(4)]));
    assert_eq!(t.run("LPOS l a COUNT 2 RANK -1"), arr(vec![int(4), int(2)]));
    assert_eq!(t.run("LPOS l a MAXLEN 1 COUNT 0"), arr(vec![int(0)]));
    assert_eq!(t.run("LPOS l z"), nil());
    assert_eq!(t.run("LPOS l z COUNT 1"), arr(vec![]));
    assert_eq!(t.run("LPOS nokey a"), nil());
    assert_eq!(t.run("LPOS nokey a COUNT 1"), arr(vec![]));
    assert_eq!(
        t.run("LPOS l a RANK 0"),
        err("ERR RANK can't be zero: use 1 to start from the first match, 2 from the second ... \
             or use negative to start from the end of the list")
    );
    assert_eq!(t.run("LPOS l a COUNT -1"), err("ERR COUNT can't be negative"));
    assert_eq!(t.run("LPOS l a MAXLEN -1"), err("ERR MAXLEN can't be negative"));
    assert_eq!(t.run("LPOS l a FOO"), err(SYNTAX));
    assert_eq!(t.run("LREM l -2 a"), int(2));
    assert_eq!(t.run("LRANGE l 0 -1"), bulks(&["a", "b", "c", "d"]));
    assert_eq!(t.run("LREM l 0 z"), int(0));
    assert_eq!(t.run("LREM nokey 0 z"), int(0));
    assert_eq!(t.run("LTRIM l 1 -2"), ok());
    assert_eq!(t.run("LRANGE l 0 -1"), bulks(&["b", "c"]));
    assert_eq!(t.run("LTRIM nokey 0 1"), ok());
    assert_eq!(t.run("LTRIM l 5 1"), ok());
    assert_eq!(t.run("EXISTS l"), int(0));
    t.run("RPUSH l x x");
    assert_eq!(t.run("LREM l 0 x"), int(2));
    assert_eq!(t.run("EXISTS l"), int(0));
}

#[test]
fn move_and_mpop() {
    let mut t = T::new();
    t.run("RPUSH src a b c");
    assert_eq!(t.run("LMOVE src dst LEFT RIGHT"), bulk("a"));
    assert_eq!(t.run("LMOVE src dst RIGHT LEFT"), bulk("c"));
    assert_eq!(t.run("LRANGE dst 0 -1"), bulks(&["c", "a"]));
    assert_eq!(t.run("RPOPLPUSH src dst"), bulk("b"));
    assert_eq!(t.run("EXISTS src"), int(0));
    assert_eq!(t.run("RPOPLPUSH src dst"), nil());
    assert_eq!(t.run("LMOVE dst dst LEFT RIGHT"), bulk("b"));
    assert_eq!(t.run("LRANGE dst 0 -1"), bulks(&["c", "a", "b"]));
    assert_eq!(t.run("LMOVE dst dst UP RIGHT"), err(SYNTAX));
    t.run("SET s v");
    assert_eq!(t.run("LMOVE dst s LEFT RIGHT"), err(WRONGTYPE));
    assert_eq!(t.run("LRANGE dst 0 -1"), bulks(&["c", "a", "b"]));

    assert_eq!(t.run("LMPOP 2 nokey dst LEFT"), arr(vec![bulk("dst"), bulks(&["c"])]));
    assert_eq!(t.run("LMPOP 1 dst RIGHT COUNT 10"), arr(vec![bulk("dst"), bulks(&["b", "a"])]));
    assert_eq!(t.run("LMPOP 1 dst RIGHT"), Value::NullArray);
    assert_eq!(t.run("LMPOP 0 dst RIGHT"), err("ERR numkeys should be greater than 0"));
    assert_eq!(t.run("LMPOP 2 dst RIGHT"), err(SYNTAX));
    assert_eq!(t.run("LMPOP 1 dst UP"), err(SYNTAX));
    assert_eq!(t.run("LMPOP 1 dst LEFT COUNT 0"), err("ERR count should be greater than 0"));
    assert_eq!(t.run("LMPOP 1 dst LEFT COUNT 1 COUNT 1"), err(SYNTAX));
    assert_eq!(t.run("LMPOP 1 s LEFT"), err(WRONGTYPE));
}

#[test]
fn blocking_pop_serves_immediately_when_data_exists() {
    let mut t = T::new();
    t.run("RPUSH b x y");
    assert_eq!(t.run("BLPOP a b 0"), bulks(&["b", "x"]));
    assert_eq!(t.run("BRPOP a b 0"), bulks(&["b", "y"]));
    t.run("RPUSH b x y");
    assert_eq!(t.run("BLMPOP 0 2 a b RIGHT COUNT 5"), arr(vec![bulk("b"), bulks(&["y", "x"])]));
    t.run("RPUSH b x");
    assert_eq!(t.run("BLMOVE b c LEFT LEFT 0"), bulk("x"));
    assert_eq!(t.run("BRPOPLPUSH c d 0"), bulk("x"));
    assert_eq!(t.run("BLPOP a x"), err("ERR timeout is not a float or out of range"));
    assert_eq!(t.run("BLPOP a -1"), err("ERR timeout is negative"));
    assert_eq!(t.run("BLPOP a 1e300"), err("ERR timeout is out of range"));
    assert_eq!(t.run("BLMPOP 0 0 a LEFT"), err("ERR numkeys should be greater than 0"));
    t.run("SET s v");
    assert_eq!(t.run("BLPOP s 0"), err(WRONGTYPE));
}

#[test]
fn blocked_clients_are_served_fifo_when_data_arrives() {
    let mut t = T::new();
    let mut a = t.connect();
    let mut b = t.connect();
    let mut c = t.connect();
    assert_eq!(t.run_as(&mut a, "BLPOP k 0"), Value::NoReply);
    assert!(a.blocked);
    assert_eq!(t.run_as(&mut b, "BRPOP other k 0"), Value::NoReply);
    assert_eq!(t.run_as(&mut c, "BLPOP k 0"), Value::NoReply);
    assert_eq!(t.engine.take_reply(&mut a), None);
    // One push of two elements serves the first two waiters, in order.
    assert_eq!(t.run("RPUSH k 1 2"), int(2));
    assert_eq!(t.engine.take_reply(&mut a), Some(bulks(&["k", "1"])));
    assert!(!a.blocked);
    assert_eq!(t.engine.take_reply(&mut b), Some(bulks(&["k", "2"])));
    assert_eq!(t.engine.take_reply(&mut c), None);
    assert_eq!(t.run("EXISTS k"), int(0));
    assert!(text(&t.run("CLIENT LIST")).contains(" flags=b "));
    assert!(text(&t.run("INFO clients")).contains("blocked_clients:1\r\n"));
    t.run("LPUSH k 3");
    assert_eq!(t.engine.take_reply(&mut c), Some(bulks(&["k", "3"])));
    assert!(text(&t.run("INFO clients")).contains("blocked_clients:0\r\n"));
}

#[test]
fn blocked_clients_time_out_with_a_null_array() {
    let mut t = T::new();
    let mut a = t.connect();
    assert_eq!(t.run_as(&mut a, "BLPOP k 0.5"), Value::NoReply);
    t.advance(400);
    t.engine.expire_blocked();
    assert_eq!(t.engine.take_reply(&mut a), None);
    t.advance(200);
    t.engine.expire_blocked();
    assert_eq!(t.engine.take_reply(&mut a), Some(Value::NullArray));
    // Nothing is left waiting: a later push stays in the list.
    t.run("RPUSH k v");
    assert_eq!(t.run("LLEN k"), int(1));
}

#[test]
fn blmove_chains_into_other_waiters() {
    let mut t = T::new();
    let mut a = t.connect();
    let mut b = t.connect();
    assert_eq!(t.run_as(&mut a, "BLMOVE src mid LEFT RIGHT 0"), Value::NoReply);
    assert_eq!(t.run_as(&mut b, "BLPOP mid 0"), Value::NoReply);
    t.run("RPUSH src v");
    assert_eq!(t.engine.take_reply(&mut a), Some(bulk("v")));
    assert_eq!(t.engine.take_reply(&mut b), Some(bulks(&["mid", "v"])));
    assert_eq!(t.run("EXISTS src mid"), int(0));
}

#[test]
fn keys_created_by_other_commands_wake_waiters() {
    let mut t = T::new();
    let mut a = t.connect();
    let mut b = t.connect();
    t.run_as(&mut a, "BLPOP k 0");
    t.run_as(&mut b, "BLMPOP 0 1 k2 RIGHT COUNT 3");
    // A string key doesn't satisfy a list waiter.
    t.run("SET k s");
    assert_eq!(t.engine.take_reply(&mut a), None);
    t.run("DEL k");
    t.run("RPUSH tmp x y");
    t.run("RENAME tmp k");
    assert_eq!(t.engine.take_reply(&mut a), Some(bulks(&["k", "x"])));
    t.run("SELECT 1");
    t.run("RPUSH k2 a b");
    t.run("MOVE k2 0");
    assert_eq!(t.engine.take_reply(&mut b), Some(arr(vec![bulk("k2"), bulks(&["b", "a"])])));
}

#[test]
fn client_unblock() {
    let mut t = T::new();
    let mut a = t.connect();
    let mut b = t.connect();
    t.run_as(&mut a, "BLPOP k 0");
    t.run_as(&mut b, "BLPOP k 0");
    let (ida, idb) = (a.id, b.id);
    assert_eq!(t.run(&format!("CLIENT UNBLOCK {ida}")), int(1));
    assert_eq!(t.engine.take_reply(&mut a), Some(Value::NullArray));
    assert_eq!(t.run(&format!("CLIENT UNBLOCK {ida}")), int(0));
    assert_eq!(t.run(&format!("CLIENT UNBLOCK {idb} ERROR")), int(1));
    assert_eq!(
        t.engine.take_reply(&mut b),
        Some(err("UNBLOCKED client unblocked via CLIENT UNBLOCK"))
    );
}

#[test]
fn disconnecting_a_blocked_client_forgets_it() {
    let mut t = T::new();
    let mut a = t.connect();
    t.run_as(&mut a, "BLPOP k 0");
    t.engine.disconnect(&a);
    t.run("RPUSH k v");
    assert_eq!(t.run("LLEN k"), int(1));
}
