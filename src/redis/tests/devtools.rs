//! Commands developer GUIs and CLIs probe: SLOWLOG, LATENCY, MEMORY,
//! MODULE LIST and the read-only parts of ACL. noida does no performance
//! analysis, so the logs are always empty; the shapes match Redis 7.2.

use super::*;

fn help_first_line(t: &mut T, line: &str) -> Value {
    let Value::Array(help) = t.run(line) else { panic!("help is an array") };
    help[0].clone()
}

#[test]
fn slowlog_is_always_empty() {
    let mut t = T::new();
    assert_eq!(t.run("SLOWLOG GET"), arr(vec![]));
    assert_eq!(t.run("SLOWLOG GET 5"), arr(vec![]));
    assert_eq!(t.run("SLOWLOG GET -1"), arr(vec![]));
    assert_eq!(t.run("SLOWLOG LEN"), int(0));
    assert_eq!(t.run("SLOWLOG RESET"), ok());
    let msg = "ERR count should be greater than or equal to -1";
    assert_eq!(t.run("SLOWLOG GET -2"), err(msg));
    assert_eq!(t.run("SLOWLOG GET x"), err(msg));
    assert_eq!(
        help_first_line(&mut t, "SLOWLOG HELP"),
        simple("SLOWLOG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:")
    );
    assert_eq!(t.run("SLOWLOG FOO"), err("ERR unknown subcommand 'FOO'. Try SLOWLOG HELP."));
}

#[test]
fn latency_has_no_samples() {
    let mut t = T::new();
    assert_eq!(t.run("LATENCY LATEST"), arr(vec![]));
    assert_eq!(t.run("LATENCY HISTORY command"), arr(vec![]));
    assert_eq!(t.run("LATENCY GRAPH command"), err("ERR No samples available for event 'command'"));
    assert_eq!(t.run("LATENCY RESET"), int(0));
    assert_eq!(t.run("LATENCY RESET command fast-command"), int(0));
    assert_eq!(t.run("LATENCY HISTOGRAM"), map(vec![]));
    let Value::Verbatim("txt", report) = t.run("LATENCY DOCTOR") else { panic!("doctor is text") };
    assert!(String::from_utf8(report).unwrap().starts_with("I'm sorry, Dave"), "off by default");
    t.run("CONFIG SET latency-monitor-threshold 100");
    let Value::Verbatim("txt", report) = t.run("LATENCY DOCTOR") else { panic!("doctor is text") };
    assert!(String::from_utf8(report).unwrap().starts_with("Dave, no latency spike was observed"));
    assert_eq!(
        help_first_line(&mut t, "LATENCY HELP"),
        simple("LATENCY <subcommand> [<arg> [value] [opt] ...]. Subcommands are:")
    );
}

#[test]
fn memory_usage_reports_a_size_for_every_type() {
    let mut t = T::new();
    assert_eq!(t.run("MEMORY USAGE nokey"), nil());
    t.run("SET s value");
    t.run("HSET h a 1");
    t.run("RPUSH l a b c");
    t.run("SADD st a b");
    t.run("ZADD z 1 a");
    t.run("XADD x 1-1 f v");
    for key in ["s", "h", "l", "st", "z", "x"] {
        let Value::Integer(n) = t.run(&format!("MEMORY USAGE {key}")) else { panic!("{key}") };
        assert!(n > 0, "{key}: {n}");
    }
    // A bigger value takes more memory.
    let Value::Integer(small) = t.run("MEMORY USAGE s") else { panic!() };
    t.run(&format!("SET big {}", "x".repeat(1000)));
    let Value::Integer(big) = t.run("MEMORY USAGE big") else { panic!() };
    assert!(big > small + 900, "{big} vs {small}");
    assert!(matches!(t.run("MEMORY USAGE s SAMPLES 0"), Value::Integer(_)));
    assert!(matches!(t.run("MEMORY USAGE s SAMPLES 3"), Value::Integer(_)));
}

#[test]
fn memory_usage_checks_its_options_before_the_key() {
    let mut t = T::new();
    assert_eq!(t.run("MEMORY USAGE nokey SAMPLES -1"), err(SYNTAX));
    assert_eq!(t.run("MEMORY USAGE nokey SAMPLES"), err(SYNTAX));
    assert_eq!(t.run("MEMORY USAGE nokey FOO 1"), err(SYNTAX));
    assert_eq!(t.run("MEMORY USAGE nokey SAMPLES x"), err(NOT_INT));
}

#[test]
fn memory_doctor_stats_and_purge() {
    let mut t = T::new();
    let Value::Verbatim("txt", report) = t.run("MEMORY DOCTOR") else { panic!("doctor is text") };
    assert!(String::from_utf8(report).unwrap().starts_with("Hi Sam, this instance is empty"));
    assert_eq!(t.run("MEMORY PURGE"), ok());
    assert_eq!(t.run("MEMORY MALLOC-STATS"), bulk("Stats not supported for the current allocator"));
    t.run("SET a 1");
    let Value::Map(stats) = t.run("MEMORY STATS") else { panic!("stats is a map") };
    let keys: Vec<String> = stats.iter().map(|(k, _)| text(k)).collect();
    for expected in [
        "peak.allocated",
        "total.allocated",
        "startup.allocated",
        "clients.normal",
        "db.0",
        "overhead.total",
        "keys.count",
        "keys.bytes-per-key",
        "dataset.bytes",
        "dataset.percentage",
        "fragmentation",
    ] {
        assert!(keys.contains(&expected.to_string()), "missing {expected} in {keys:?}");
    }
    let count = stats.iter().find(|(k, _)| text(k) == "keys.count").unwrap();
    assert_eq!(count.1, int(1));
    assert_eq!(
        help_first_line(&mut t, "MEMORY HELP"),
        simple("MEMORY <subcommand> [<arg> [value] [opt] ...]. Subcommands are:")
    );
}

#[test]
fn module_list_is_empty() {
    let mut t = T::new();
    assert_eq!(t.run("MODULE LIST"), arr(vec![]));
    assert_eq!(
        help_first_line(&mut t, "MODULE HELP"),
        simple("MODULE <subcommand> [<arg> [value] [opt] ...]. Subcommands are:")
    );
}

#[test]
fn acl_knows_only_the_default_user() {
    let mut t = T::new();
    assert_eq!(t.run("ACL WHOAMI"), bulk("default"));
    assert_eq!(t.run("ACL USERS"), bulks(&["default"]));
    assert_eq!(t.run("ACL LIST"), bulks(&["user default on nopass sanitize-payload ~* &* +@all"]));
    assert_eq!(
        t.run("ACL GETUSER default"),
        map(vec![
            ("flags", simples(&["on", "nopass", "sanitize-payload"])),
            ("passwords", arr(vec![])),
            ("commands", bulk("+@all")),
            ("keys", bulk("~*")),
            ("channels", bulk("&*")),
            ("selectors", arr(vec![])),
        ])
    );
    assert_eq!(t.run("ACL GETUSER nobody"), nil());
    assert_eq!(t.run("ACL LOG"), arr(vec![]));
    assert_eq!(t.run("ACL LOG 3"), arr(vec![]));
    assert_eq!(t.run("ACL LOG RESET"), ok());
    assert_eq!(
        help_first_line(&mut t, "ACL HELP"),
        simple("ACL <subcommand> [<arg> [value] [opt] ...]. Subcommands are:")
    );
}

#[test]
fn acl_categories() {
    let mut t = T::new();
    let expected = [
        "keyspace",
        "read",
        "write",
        "set",
        "sortedset",
        "list",
        "hash",
        "string",
        "bitmap",
        "hyperloglog",
        "geo",
        "stream",
        "pubsub",
        "admin",
        "fast",
        "slow",
        "blocking",
        "dangerous",
        "connection",
        "transaction",
        "scripting",
    ];
    assert_eq!(t.run("ACL CAT"), bulks(&expected));
    let Value::Array(strings) = t.run("ACL CAT string") else { panic!() };
    assert!(strings.contains(&bulk("get")) && strings.contains(&bulk("append")));
    assert!(!strings.contains(&bulk("lpush")));
    assert_eq!(t.run("ACL CAT nosuch"), err("ERR Unknown category 'nosuch'"));
}

#[test]
fn acl_genpass() {
    let mut t = T::new();
    let hex = |v: Value| text(&v);
    let a = hex(t.run("ACL GENPASS"));
    assert_eq!(a.len(), 64);
    assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
    assert_ne!(a, hex(t.run("ACL GENPASS")), "passwords are random");
    assert_eq!(hex(t.run("ACL GENPASS 5")).len(), 2);
    assert_eq!(hex(t.run("ACL GENPASS 4096")).len(), 1024);
    let msg = "ERR ACL GENPASS argument must be the number of bits for the output password, \
               a positive number up to 4096";
    assert_eq!(t.run("ACL GENPASS 0"), err(msg));
    assert_eq!(t.run("ACL GENPASS 4097"), err(msg));
    assert_eq!(t.run("ACL GENPASS x"), err(NOT_INT));
}

#[test]
fn acl_management_is_not_offered() {
    // Multi-user management is a production concern (see the scope filter).
    let mut t = T::new();
    for sub in ["SETUSER bob on", "DELUSER bob", "LOAD", "SAVE", "DRYRUN default get k"] {
        assert_eq!(
            t.run(&format!("ACL {sub}")),
            err(&format!(
                "ERR unknown subcommand '{}'. Try ACL HELP.",
                sub.split(' ').next().unwrap()
            )),
            "{sub}"
        );
    }
}
