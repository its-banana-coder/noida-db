//! Connection commands: PING, ECHO, HELLO, AUTH, CLIENT, RESET, QUIT.

use super::*;
use crate::redis::REDIS_VERSION;

#[test]
fn ping_and_echo() {
    let mut t = T::new();
    assert_eq!(t.run("PING"), simple("PONG"));
    assert_eq!(t.run("ping hello"), bulk("hello"));
    assert_eq!(t.run("PING a b"), err("ERR wrong number of arguments for 'ping' command"));
    assert_eq!(t.run("ECHO hi"), bulk("hi"));
    assert_eq!(t.run("ECHO"), err("ERR wrong number of arguments for 'echo' command"));
}

#[test]
fn unknown_command_matches_redis_message() {
    let mut t = T::new();
    assert_eq!(
        t.run("FOO a b"),
        err("ERR unknown command 'FOO', with args beginning with: 'a' 'b' ")
    );
    assert_eq!(t.run("foo"), err("ERR unknown command 'foo', with args beginning with: "));
}

fn hello_reply(proto: i64, id: i64) -> Value {
    map(vec![
        ("server", bulk("redis")),
        ("version", bulk(REDIS_VERSION)),
        ("proto", int(proto)),
        ("id", int(id)),
        ("mode", bulk("standalone")),
        ("role", bulk("master")),
        ("modules", arr(vec![])),
    ])
}

#[test]
fn hello_reports_the_server_and_switches_protocol() {
    let mut t = T::new();
    assert_eq!(t.run("HELLO"), hello_reply(2, 1));
    assert_eq!(t.run("HELLO 3"), hello_reply(3, 1));
    assert_eq!(t.session.resp, 3);
    assert_eq!(t.run("HELLO"), hello_reply(3, 1));
    assert_eq!(t.run("HELLO 2"), hello_reply(2, 1));
    assert_eq!(t.session.resp, 2);
}

#[test]
fn hello_options_and_errors() {
    let mut t = T::new();
    assert_eq!(t.run("HELLO 4"), err("NOPROTO unsupported protocol version"));
    assert_eq!(t.run("HELLO 1"), err("NOPROTO unsupported protocol version"));
    assert_eq!(t.run("HELLO x"), err("ERR Protocol version is not an integer or out of range"));
    assert_eq!(t.run("HELLO 3 FOO"), err("ERR Syntax error in HELLO option 'FOO'"));
    assert_eq!(t.run("HELLO 3 SETNAME"), err("ERR Syntax error in HELLO option 'SETNAME'"));
    assert_eq!(
        t.run("HELLO 3 SETNAME \"a b\""),
        err("ERR Client names cannot contain spaces, newlines or special characters.")
    );
    // A failed HELLO changes nothing.
    assert_eq!(t.session.resp, 2);
    assert_eq!(t.run("HELLO 3 SETNAME app AUTH default secret"), hello_reply(3, 1));
    assert_eq!(t.run("CLIENT GETNAME"), bulk("app"));
    assert_eq!(
        t.run("HELLO 3 AUTH bob pw"),
        err("WRONGPASS invalid username-password pair or user is disabled.")
    );
}

#[test]
fn auth_without_configured_password() {
    let mut t = T::new();
    assert_eq!(
        t.run("AUTH secret"),
        err(
            "ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?"
        )
    );
    // The default user has no password, so it accepts anything.
    assert_eq!(t.run("AUTH default whatever"), ok());
    assert_eq!(
        t.run("AUTH bob pw"),
        err("WRONGPASS invalid username-password pair or user is disabled.")
    );
    assert_eq!(t.run("AUTH a b c"), err(SYNTAX));
}

#[test]
fn client_id_name_and_info() {
    let mut t = T::new();
    assert_eq!(t.run("CLIENT ID"), int(1));
    assert_eq!(t.run("CLIENT GETNAME"), nil());
    assert_eq!(t.run("CLIENT SETNAME worker-1"), ok());
    assert_eq!(t.run("CLIENT GETNAME"), bulk("worker-1"));
    assert_eq!(
        t.run("CLIENT SETNAME \"a b\""),
        err("ERR Client names cannot contain spaces, newlines or special characters.")
    );
    // An empty name clears it.
    assert_eq!(t.run("CLIENT SETNAME \"\""), ok());
    assert_eq!(t.run("CLIENT GETNAME"), nil());
    assert_eq!(t.run("CLIENT SETINFO LIB-NAME redis-py"), ok());
    assert_eq!(t.run("CLIENT SETINFO lib-ver 5.0.1"), ok());
    assert_eq!(t.run("CLIENT SETINFO lib-foo x"), err("ERR Unrecognized option 'lib-foo'"));
    assert_eq!(
        t.run("CLIENT SETINFO lib-name \"a b\""),
        err("ERR lib-name cannot contain spaces, newlines or special characters.")
    );

    t.run("SELECT 4");
    t.run("CLIENT SETNAME app");
    let info = text(&t.run("CLIENT INFO"));
    assert!(info.ends_with('\n'), "{info:?}");
    let fields: Vec<&str> = info.trim_end().split(' ').collect();
    for expected in [
        "id=1",
        "addr=127.0.0.1:50001",
        "laddr=127.0.0.1:6379",
        "fd=8",
        "name=app",
        "age=0",
        "idle=0",
        "flags=N",
        "db=4",
        "sub=0",
        "psub=0",
        "ssub=0",
        "multi=-1",
        "cmd=client|info",
        "user=default",
        "redir=-1",
        "resp=2",
        "lib-name=redis-py",
        "lib-ver=5.0.1",
    ] {
        assert!(fields.contains(&expected), "missing {expected} in {info}");
    }
    // Field order is fixed; tools split on spaces.
    let names: Vec<&str> = fields.iter().map(|f| f.split('=').next().unwrap()).collect();
    assert_eq!(
        names,
        [
            "id",
            "addr",
            "laddr",
            "fd",
            "name",
            "age",
            "idle",
            "flags",
            "db",
            "sub",
            "psub",
            "ssub",
            "multi",
            "qbuf",
            "qbuf-free",
            "argv-mem",
            "multi-mem",
            "rbs",
            "rbp",
            "obl",
            "oll",
            "omem",
            "tot-mem",
            "events",
            "cmd",
            "user",
            "redir",
            "resp",
            "lib-name",
            "lib-ver"
        ]
    );
}

#[test]
fn client_list_shows_every_connection() {
    let mut t = T::new();
    let mut other = t.connect();
    t.run_as(&mut other, "CLIENT SETNAME other");
    let list = text(&t.run("CLIENT LIST"));
    let lines: Vec<&str> = list.lines().collect();
    assert_eq!(lines.len(), 2);
    assert!(lines[0].starts_with("id=1 "));
    assert!(lines[1].starts_with("id=2 ") && lines[1].contains(" name=other "));
    assert_eq!(text(&t.run("CLIENT LIST ID 2")).lines().count(), 1);
    assert_eq!(text(&t.run("CLIENT LIST ID 2 99")).lines().count(), 1);
    assert_eq!(text(&t.run("CLIENT LIST TYPE normal")).lines().count(), 2);
    assert_eq!(text(&t.run("CLIENT LIST TYPE pubsub")), "");
    assert_eq!(t.run("CLIENT LIST TYPE foo"), err("ERR Unknown client type 'foo'"));
    assert_eq!(t.run("CLIENT LIST ID x"), err("ERR Invalid client ID"));
    assert_eq!(t.run("CLIENT LIST FOO"), err(SYNTAX));
}

#[test]
fn client_kill() {
    let mut t = T::new();
    let mut other = t.connect();
    assert_eq!(t.run("CLIENT KILL 1.2.3.4:5"), err("ERR No such client"));
    assert_eq!(t.run("CLIENT KILL ID 0"), err("ERR client-id should be greater than 0"));
    assert_eq!(t.run("CLIENT KILL ID 99"), int(0));
    assert_eq!(t.run("CLIENT KILL TYPE foo"), err("ERR Unknown client type 'foo'"));
    assert_eq!(t.run("CLIENT KILL USER bob"), err("ERR No such user 'bob'"));
    assert_eq!(t.run("CLIENT KILL SKIPME maybe"), err(SYNTAX));
    // New syntax skips the caller by default.
    assert_eq!(t.run("CLIENT KILL TYPE normal"), int(1));
    assert!(t.engine.client_count() == 1);
    assert!(!t.session.closing);
    let _ = t.run_as(&mut other, "PING");
    // Old syntax can kill yourself; the reply goes out first.
    assert_eq!(t.run("CLIENT KILL 127.0.0.1:50001"), ok());
    assert!(t.session.closing);
}

#[test]
fn client_reply_modes() {
    let mut t = T::new();
    assert_eq!(t.run("CLIENT REPLY OFF"), Value::NoReply);
    assert_eq!(t.run("SET k v"), Value::NoReply);
    assert_eq!(t.run("CLIENT REPLY ON"), ok());
    assert_eq!(t.run("GET k"), bulk("v"));
    assert_eq!(t.run("CLIENT REPLY SKIP"), Value::NoReply);
    assert_eq!(t.run("GET k"), Value::NoReply);
    assert_eq!(t.run("GET k"), bulk("v"));
    assert_eq!(t.run("CLIENT REPLY MAYBE"), err(SYNTAX));
}

#[test]
fn client_flags_and_pause() {
    let mut t = T::new();
    assert_eq!(t.run("CLIENT NO-EVICT on"), ok());
    assert_eq!(t.run("CLIENT NO-TOUCH ON"), ok());
    assert!(text(&t.run("CLIENT INFO")).contains(" flags=eT "));
    assert_eq!(t.run("CLIENT NO-EVICT off"), ok());
    assert_eq!(t.run("CLIENT NO-TOUCH off"), ok());
    assert_eq!(t.run("CLIENT NO-EVICT maybe"), err(SYNTAX));
    assert_eq!(t.run("CLIENT PAUSE 100"), ok());
    assert_eq!(t.run("CLIENT PAUSE 100 WRITE"), ok());
    assert_eq!(t.run("CLIENT PAUSE 100 FOO"), err("ERR CLIENT PAUSE mode must be WRITE or ALL"));
    assert_eq!(t.run("CLIENT PAUSE x"), err("ERR timeout is not an integer or out of range"));
    assert_eq!(t.run("CLIENT PAUSE -1"), err("ERR timeout is negative"));
    assert_eq!(t.run("CLIENT UNPAUSE"), ok());
    assert_eq!(t.run("CLIENT UNBLOCK 2"), int(0));
    assert_eq!(
        t.run("CLIENT UNBLOCK 2 FOO"),
        err("ERR CLIENT UNBLOCK reason should be TIMEOUT or ERROR")
    );
}

#[test]
fn pause_blocks_other_clients_until_it_ends() {
    let mut t = T::new();
    t.run("CLIENT PAUSE 100 WRITE");
    let set = crate::redis::resp::split_inline(b"SET k v").unwrap();
    let get = crate::redis::resp::split_inline(b"GET k").unwrap();
    assert!(t.engine.is_paused_for(&set));
    assert!(!t.engine.is_paused_for(&get));
    t.run("CLIENT PAUSE 100 ALL");
    assert!(t.engine.is_paused_for(&get));
    t.advance(101);
    assert!(!t.engine.is_paused_for(&set));
    t.run("CLIENT PAUSE 100");
    t.run("CLIENT UNPAUSE");
    assert!(!t.engine.is_paused_for(&set));
}

#[test]
fn client_subcommand_errors() {
    let mut t = T::new();
    assert_eq!(t.run("CLIENT"), err("ERR wrong number of arguments for 'client' command"));
    assert_eq!(t.run("CLIENT FOO"), err("ERR unknown subcommand 'FOO'. Try CLIENT HELP."));
    assert_eq!(
        t.run("client setname"),
        err("ERR wrong number of arguments for 'client|setname' command")
    );
    let Value::Array(help) = t.run("CLIENT HELP") else { panic!() };
    assert_eq!(help[0], simple("CLIENT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:"));
    assert_eq!(help[help.len() - 2], simple("HELP"));
    assert_eq!(help[help.len() - 1], simple("    Print this help."));
}

#[test]
fn reset_restores_a_fresh_connection() {
    let mut t = T::new();
    t.run("SELECT 3");
    t.run("CLIENT SETNAME app");
    t.run("HELLO 3");
    t.run("CLIENT NO-EVICT on");
    assert_eq!(t.run("RESET"), simple("RESET"));
    assert_eq!(t.session.resp, 2);
    assert_eq!(t.run("CLIENT GETNAME"), nil());
    let info = text(&t.run("CLIENT INFO"));
    assert!(info.contains(" db=0 ") && info.contains(" flags=N "), "{info}");
}

#[test]
fn quit_replies_then_closes() {
    let mut t = T::new();
    assert_eq!(t.run("QUIT"), ok());
    assert!(t.session.closing);
}

#[test]
fn disconnect_removes_the_client() {
    let mut t = T::new();
    let other = t.connect();
    assert_eq!(t.engine.client_count(), 2);
    t.engine.disconnect(&other);
    assert_eq!(t.engine.client_count(), 1);
}
