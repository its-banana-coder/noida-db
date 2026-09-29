//! MONITOR: every command other clients run is streamed to monitor clients
//! (Redis 7.2 feeds after the command ran, except EVAL/EVALSHA/FCALL, which
//! feed first so a script's own commands follow it).

use super::*;

/// The monitor lines the connection has been sent so far.
fn lines(t: &mut T, mon: &Session) -> Vec<String> {
    t.engine
        .take_pushes(mon.id)
        .into_iter()
        .map(|v| match v {
            Value::Simple(s) => s,
            other => panic!("monitor lines are simple strings, got {other:?}"),
        })
        .collect()
}

fn monitor(t: &mut T) -> Session {
    let mut mon = t.connect();
    assert_eq!(t.run_as(&mut mon, "MONITOR"), ok());
    mon
}

/// Redis shows the command exactly as the client typed it, so these tests send
/// upper case and expect upper case back.
const TS: &str = "1700000000.000000";

#[test]
fn other_clients_commands_are_streamed() {
    let mut t = T::new();
    let mon = monitor(&mut t);
    t.run("SET k v");
    t.run("GET k");
    assert_eq!(
        lines(&mut t, &mon),
        [
            format!(r#"{TS} [0 127.0.0.1:50001] "SET" "k" "v""#),
            format!(r#"{TS} [0 127.0.0.1:50001] "GET" "k""#),
        ]
    );
}

#[test]
fn timestamps_follow_the_clock() {
    let mut t = T::new();
    let mon = monitor(&mut t);
    t.advance(1_234);
    t.run("PING");
    assert_eq!(lines(&mut t, &mon), [r#"1700000001.234000 [0 127.0.0.1:50001] "PING""#]);
}

#[test]
fn arguments_are_quoted_like_sdscatrepr() {
    let mut t = T::new();
    let mon = monitor(&mut t);
    // a"b\c, a newline, a tab, a bell, a backspace, a NUL, é (two bytes)
    let args: Vec<Vec<u8>> = vec![b"ECHO".to_vec(), b"a\"b\\c\n\t\x07\x08\0\xc3\xa9 ~".to_vec()];
    t.engine.execute(&mut t.session, &args);
    assert_eq!(
        lines(&mut t, &mon),
        [format!(r#"{TS} [0 127.0.0.1:50001] "ECHO" "a\"b\\c\n\t\a\b\x00\xc3\xa9 ~""#)]
    );
}

#[test]
fn the_database_shown_is_the_one_after_the_command() {
    let mut t = T::new();
    let mon = monitor(&mut t);
    t.run("SELECT 3");
    t.run("GET a");
    assert_eq!(
        lines(&mut t, &mon),
        [
            format!(r#"{TS} [3 127.0.0.1:50001] "SELECT" "3""#),
            format!(r#"{TS} [3 127.0.0.1:50001] "GET" "a""#),
        ]
    );
}

#[test]
fn admin_commands_are_not_shown() {
    let mut t = T::new();
    let mon = monitor(&mut t);
    t.run("CONFIG SET maxmemory 0");
    t.run("CONFIG GET maxmemory");
    t.run("SAVE");
    t.run("GET k");
    assert_eq!(lines(&mut t, &mon), [format!(r#"{TS} [0 127.0.0.1:50001] "GET" "k""#)]);
}

#[test]
fn failed_commands_are_shown_but_unknown_ones_are_not() {
    let mut t = T::new();
    let mon = monitor(&mut t);
    t.run("SET s v");
    t.run("INCR s"); // fails, but it ran
    t.run("NOSUCH x"); // never reached a handler
    assert_eq!(
        lines(&mut t, &mon),
        [
            format!(r#"{TS} [0 127.0.0.1:50001] "SET" "s" "v""#),
            format!(r#"{TS} [0 127.0.0.1:50001] "INCR" "s""#),
        ]
    );
}

#[test]
fn a_script_shows_up_before_the_commands_it_runs() {
    let mut t = T::new();
    let mon = monitor(&mut t);
    t.run("EVAL \"return redis.call('set',KEYS[1],'v')\" 1 k");
    assert_eq!(
        lines(&mut t, &mon),
        [
            format!(
                r#"{TS} [0 127.0.0.1:50001] "EVAL" "return redis.call('set',KEYS[1],'v')" "1" "k""#
            ),
            format!(r#"{TS} [0 lua] "set" "k" "v""#),
        ]
    );
}

#[test]
fn a_transaction_shows_multi_then_its_commands_then_exec() {
    let mut t = T::new();
    let mon = monitor(&mut t);
    t.run("MULTI");
    t.run("SET a 1");
    t.run("INCR a");
    assert_eq!(lines(&mut t, &mon).len(), 1, "only MULTI so far: queued commands haven't run");
    t.run("EXEC");
    assert_eq!(
        lines(&mut t, &mon),
        [
            format!(r#"{TS} [0 127.0.0.1:50001] "SET" "a" "1""#),
            format!(r#"{TS} [0 127.0.0.1:50001] "INCR" "a""#),
            format!(r#"{TS} [0 127.0.0.1:50001] "EXEC""#),
        ]
    );
}

#[test]
fn every_monitor_gets_every_line() {
    let mut t = T::new();
    let (a, b) = (monitor(&mut t), monitor(&mut t));
    t.run("PING");
    assert_eq!(lines(&mut t, &a).len(), 1);
    assert_eq!(lines(&mut t, &b).len(), 1);
}

#[test]
fn a_monitor_client_is_flagged_and_can_leave() {
    let mut t = T::new();
    let mut mon = monitor(&mut t);
    let info = text(&t.run_as(&mut mon, "CLIENT INFO"));
    assert!(info.contains(" flags=O "), "{info}");
    // MONITOR again is ignored without a reply, as in Redis.
    assert_eq!(t.run_as(&mut mon, "MONITOR"), Value::NoReply);
    t.engine.disconnect(&mon);
    t.run("PING"); // nobody left to tell
    assert!(t.engine.take_pushes(mon.id).is_empty());
}

#[test]
fn monitor_is_refused_where_blocking_is_denied() {
    let mut t = T::new();
    t.run("MULTI");
    t.run("MONITOR");
    assert_eq!(t.run("EXEC"), arr(vec![err("ERR MONITOR isn't allowed for DENY BLOCKING client")]));
}
