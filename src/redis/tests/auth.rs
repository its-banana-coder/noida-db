//! `requirepass`: with a password set, new connections must AUTH first
//! (Redis 7.2 semantics from server.c and networking.c).

use super::*;

const NOAUTH: &str = "NOAUTH Authentication required.";
const WRONGPASS: &str = "WRONGPASS invalid username-password pair or user is disabled.";
const HELLO_NOAUTH: &str = "NOAUTH HELLO must be called with the client already authenticated, \
                            otherwise the HELLO <proto> AUTH <user> <pass> option can be used to \
                            authenticate the client and select the RESP protocol version at the \
                            same time";

/// A server with a password and a second, unauthenticated connection.
fn locked() -> (T, Session) {
    let mut t = T::new();
    assert_eq!(t.run("CONFIG SET requirepass secret"), ok());
    let c = t.connect();
    (t, c)
}

#[test]
fn connections_made_before_the_password_stay_authenticated() {
    let (mut t, _) = locked();
    assert_eq!(t.run("PING"), simple("PONG"));
    assert_eq!(t.run("SET k v"), ok());
}

#[test]
fn a_new_connection_must_authenticate_first() {
    let (mut t, mut c) = locked();
    assert_eq!(t.run_as(&mut c, "PING"), err(NOAUTH));
    assert_eq!(t.run_as(&mut c, "GET k"), err(NOAUTH));
    assert_eq!(t.run_as(&mut c, "CLIENT ID"), err(NOAUTH));
    assert_eq!(t.run_as(&mut c, "AUTH wrong"), err(WRONGPASS));
    assert_eq!(t.run_as(&mut c, "PING"), err(NOAUTH), "a failed AUTH doesn't authenticate");
    assert_eq!(t.run_as(&mut c, "AUTH secret"), ok());
    assert_eq!(t.run_as(&mut c, "PING"), simple("PONG"));
}

#[test]
fn auth_with_a_username() {
    let (mut t, mut c) = locked();
    assert_eq!(t.run_as(&mut c, "AUTH default wrong"), err(WRONGPASS));
    assert_eq!(t.run_as(&mut c, "AUTH bob secret"), err(WRONGPASS), "there is no such user");
    assert_eq!(t.run_as(&mut c, "AUTH default secret"), ok());
    assert_eq!(t.run_as(&mut c, "GET k"), nil());
}

#[test]
fn unknown_commands_and_bad_arity_are_reported_before_the_auth_check() {
    let (mut t, mut c) = locked();
    assert_eq!(
        t.run_as(&mut c, "NOSUCH a"),
        err("ERR unknown command 'NOSUCH', with args beginning with: 'a' ")
    );
    assert_eq!(t.run_as(&mut c, "GET"), err("ERR wrong number of arguments for 'get' command"));
}

#[test]
fn commands_flagged_no_auth_work_without_it() {
    let (mut t, mut c) = locked();
    assert_eq!(t.run_as(&mut c, "RESET"), simple("RESET"));
    assert_eq!(t.run_as(&mut c, "QUIT"), ok());
}

#[test]
fn hello_needs_credentials_when_a_password_is_set() {
    let (mut t, mut c) = locked();
    assert_eq!(t.run_as(&mut c, "HELLO 3"), err(HELLO_NOAUTH));
    assert_eq!(c.resp, 2, "a refused HELLO doesn't switch the protocol");
    assert_eq!(t.run_as(&mut c, "HELLO 3 AUTH default wrong"), err(WRONGPASS));
    assert_eq!(t.run_as(&mut c, "PING"), err(NOAUTH));
    let Value::Map(reply) = t.run_as(&mut c, "HELLO 3 AUTH default secret") else {
        panic!("HELLO replies with a map")
    };
    assert!(reply.contains(&(bulk("proto"), int(3))));
    assert_eq!(t.run_as(&mut c, "PING"), simple("PONG"), "HELLO AUTH authenticated the client");
}

#[test]
fn reset_forgets_the_authentication() {
    let (mut t, mut c) = locked();
    assert_eq!(t.run_as(&mut c, "AUTH secret"), ok());
    assert_eq!(t.run_as(&mut c, "PING"), simple("PONG"));
    assert_eq!(t.run_as(&mut c, "RESET"), simple("RESET"));
    assert_eq!(t.run_as(&mut c, "PING"), err(NOAUTH));
}

#[test]
fn clearing_the_password_opens_the_server_again() {
    let (mut t, mut c) = locked();
    assert_eq!(t.run("CONFIG SET requirepass \"\""), ok());
    // The connection that was already there is still fine, and so is a new one.
    let mut fresh = t.connect();
    assert_eq!(t.run_as(&mut fresh, "PING"), simple("PONG"));
    // A connection made while locked stays unauthenticated until it says so.
    assert_eq!(t.run_as(&mut c, "PING"), simple("PONG"), "no password is required any more");
}

#[test]
fn the_password_can_be_read_back() {
    let (mut t, _) = locked();
    assert_eq!(t.run("CONFIG GET requirepass"), map(vec![("requirepass", bulk("secret"))]));
}
