//! COMMAND and INFO.

use super::*;

fn set(items: Vec<Value>) -> Value {
    Value::Set(items)
}

fn get_info() -> Value {
    arr(vec![
        bulk("get"),
        int(2),
        simples(&["readonly", "fast"]),
        int(1),
        int(1),
        int(1),
        simples(&["@read", "@string", "@fast"]),
        set(vec![]),
        set(vec![map(vec![
            ("flags", simples(&["RO", "access"])),
            (
                "begin_search",
                map(vec![("type", bulk("index")), ("spec", map(vec![("index", int(1))]))]),
            ),
            (
                "find_keys",
                map(vec![
                    ("type", bulk("range")),
                    (
                        "spec",
                        map(vec![("lastkey", int(0)), ("keystep", int(1)), ("limit", int(0))]),
                    ),
                ]),
            ),
        ])]),
        set(vec![]),
    ])
}

#[test]
fn command_info_for_get() {
    let mut t = T::new();
    assert_eq!(t.run("COMMAND INFO get"), arr(vec![get_info()]));
    assert_eq!(t.run("COMMAND INFO GET nosuch"), arr(vec![get_info(), nil()]));
}

#[test]
fn command_info_flags_movablekeys_and_legacy_range() {
    let mut t = T::new();
    let Value::Array(info) = t.run("COMMAND INFO mset") else { panic!() };
    let Value::Array(mset) = &info[0] else { panic!() };
    assert_eq!(mset[1], int(-3));
    assert_eq!(mset[2], simples(&["write", "denyoom"]));
    // first key 1, last key -1 (to the end), step 2
    assert_eq!(&mset[3..6], &[int(1), int(-1), int(2)]);
    assert_eq!(mset[6], simples(&["@write", "@string", "@slow"]));
}

#[test]
fn command_info_for_a_container_lists_implemented_subcommands() {
    let mut t = T::new();
    let Value::Array(info) = t.run("COMMAND INFO client") else { panic!() };
    let Value::Array(client) = &info[0] else { panic!() };
    assert_eq!(client[0], bulk("client"));
    assert_eq!(client[1], int(-2));
    let Value::Array(subs) = &client[9] else { panic!("{:?}", client[9]) };
    let names: Vec<Value> = subs
        .iter()
        .map(|s| match s {
            Value::Array(v) => v[0].clone(),
            _ => panic!(),
        })
        .collect();
    assert!(names.contains(&bulk("client|setname")));
    assert!(names.contains(&bulk("client|list")));
}

#[test]
fn command_docs_for_get() {
    let mut t = T::new();
    assert_eq!(
        t.run("COMMAND DOCS get"),
        map(vec![(
            "get",
            map(vec![
                ("summary", bulk("Returns the string value of a key.")),
                ("since", bulk("1.0.0")),
                ("group", bulk("string")),
                ("complexity", bulk("O(1)")),
                (
                    "arguments",
                    arr(vec![map(vec![
                        ("name", bulk("key")),
                        ("type", bulk("key")),
                        ("display_text", bulk("key")),
                        ("key_spec_index", int(0)),
                    ])]),
                ),
            ]),
        )])
    );
    // Unknown names are skipped in DOCS.
    assert_eq!(t.run("COMMAND DOCS nosuch"), map(vec![]));
}

#[test]
fn command_docs_include_history() {
    let mut t = T::new();
    let Value::Map(docs) = t.run("COMMAND DOCS set") else { panic!() };
    let Value::Map(set_docs) = &docs[0].1 else { panic!() };
    let history = set_docs.iter().find(|(k, _)| *k == bulk("history")).map(|(_, v)| v.clone());
    let Some(Value::Set(entries)) = history else { panic!("{set_docs:?}") };
    assert_eq!(entries[0], bulks(&["2.6.12", "Added the `EX`, `PX`, `NX` and `XX` options."]));
}

#[test]
fn command_getkeys() {
    let mut t = T::new();
    assert_eq!(t.run("COMMAND GETKEYS SET k v"), bulks(&["k"]));
    assert_eq!(t.run("COMMAND GETKEYS MSET a 1 b 2"), bulks(&["a", "b"]));
    assert_eq!(t.run("COMMAND GETKEYS LCS x y"), bulks(&["x", "y"]));
    assert_eq!(t.run("COMMAND GETKEYS PING"), err("ERR The command has no key arguments"));
    assert_eq!(t.run("COMMAND GETKEYS NOSUCH x"), err("ERR Invalid command specified"));
    assert_eq!(
        t.run("COMMAND GETKEYS GET"),
        err("ERR Invalid number of arguments specified for command")
    );
    // SET's flags depend on GET, so Redis uses its own key function.
    assert_eq!(
        t.run("COMMAND GETKEYSANDFLAGS SET k v"),
        arr(vec![arr(vec![bulk("k"), simples(&["OW", "update"])])])
    );
    assert_eq!(
        t.run("COMMAND GETKEYSANDFLAGS SET k v GET"),
        arr(vec![arr(vec![bulk("k"), simples(&["RW", "access", "update"])])])
    );
}

#[test]
fn command_count_and_list() {
    let mut t = T::new();
    let implemented = crate::redis::command_names().count() as i64;
    assert_eq!(t.run("COMMAND COUNT"), int(implemented));
    let Value::Array(all) = t.run("COMMAND") else { panic!() };
    assert_eq!(all.len() as i64, implemented);

    let Value::Array(list) = t.run("COMMAND LIST") else { panic!() };
    assert!(list.contains(&bulk("get")));
    assert!(list.contains(&bulk("client|setname")));
    let Value::Array(clients) = t.run("COMMAND LIST FILTERBY PATTERN client|*") else { panic!() };
    assert!(!clients.is_empty() && clients.iter().all(|c| text(c).starts_with("client|")));
    let Value::Array(strings) = t.run("COMMAND LIST FILTERBY ACLCAT string") else { panic!() };
    assert!(strings.contains(&bulk("append")) && !strings.contains(&bulk("del")));
    assert_eq!(t.run("COMMAND LIST FILTERBY ACLCAT nosuchcat"), arr(vec![]));
    assert_eq!(t.run("COMMAND LIST FILTERBY FOO x"), err(SYNTAX));
    assert_eq!(t.run("COMMAND LIST FILTERBY"), err(SYNTAX));
}

#[test]
fn command_help() {
    let mut t = T::new();
    let Value::Array(help) = t.run("COMMAND HELP") else { panic!() };
    assert_eq!(help[0], simple("COMMAND <subcommand> [<arg> [value] [opt] ...]. Subcommands are:"));
}

#[test]
fn info_sections() {
    let mut t = T::new();
    t.run("SET a 1");
    t.run("SET b 2 EX 100");
    let all = text(&t.run("INFO"));
    assert!(all.starts_with("# Server\r\nredis_version:"), "{all}");
    for section in [
        "# Clients",
        "# Memory",
        "# Persistence",
        "# Stats",
        "# Replication",
        "# CPU",
        "# Modules",
        "# Errorstats",
        "# Cluster",
        "# Keyspace",
    ] {
        assert!(all.contains(section), "missing {section}");
    }
    assert!(all.contains("\r\nrole:master\r\n"));
    assert!(all.contains("\r\nconnected_clients:1\r\n"));
    assert!(all.contains("\r\ndb0:keys=2,expires=1,avg_ttl=0\r\n"));

    let server = text(&t.run("INFO server"));
    assert!(server.contains("redis_mode:standalone") && !server.contains("# Clients"));
    let two = text(&t.run("INFO server keyspace"));
    assert!(two.contains("# Server") && two.contains("# Keyspace") && !two.contains("# Memory"));
    assert_eq!(text(&t.run("INFO nosuchsection")), "");
}
