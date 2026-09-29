//! Tracks how many of Redis 7.2's 242 commands noida-db implements.
//!
//! `IMPLEMENTED_FLOOR` is a ratchet: raise it whenever commands land, and the
//! test fails if coverage ever drops. `all_commands_implemented` is the
//! finish line; run it with `cargo test -- --ignored`.

const COMMANDS: &str = include_str!("data/redis-7.2-commands.txt");

/// Raise this as commands are implemented. Never lower it.
const IMPLEMENTED_FLOOR: usize = 217;

fn commands() -> Vec<(&'static str, &'static str)> {
    COMMANDS
        .lines()
        .filter(|l| !l.starts_with('#') && !l.trim().is_empty())
        .map(|l| {
            let mut parts = l.split_whitespace();
            (parts.next().unwrap(), parts.next().unwrap())
        })
        .collect()
}

/// Commands noida-db deliberately does not have: production-only operations
/// (see the scope filter in docs/specs/README.md). They answer as unknown.
/// The same list, with the reasons, is in docs/LIMITATIONS.md.
const OUT_OF_SCOPE: &[&str] = &[
    // RDB payloads and key migration
    "dump",
    "restore",
    "restore-asking",
    "migrate",
    // replication, failover, sentinel, cluster
    "psync",
    "sync",
    "replconf",
    "replicaof",
    "slaveof",
    "role",
    "wait",
    "waitaof",
    "failover",
    "sentinel",
    "cluster",
    "asking",
    "readonly",
    "readwrite",
    // server internals
    "debug",
    "shutdown",
    "pfdebug",
    "pfselftest",
];

fn missing() -> Vec<(&'static str, &'static str)> {
    commands()
        .into_iter()
        .filter(|(name, _)| !noida::redis::is_implemented(name) && !OUT_OF_SCOPE.contains(name))
        .collect()
}

#[test]
fn coverage_never_drops() {
    let total = commands().len();
    let left = missing().len();
    let done = total - left - OUT_OF_SCOPE.len();
    eprintln!(
        "redis: {done} implemented, {} out of scope, {left} still to do (of {total})",
        OUT_OF_SCOPE.len()
    );
    assert!(done >= IMPLEMENTED_FLOOR, "coverage dropped to {done} (floor {IMPLEMENTED_FLOOR})");
}

#[test]
fn every_implemented_command_is_a_real_redis_command() {
    // Catches typos in the command table: noida-db must not invent commands.
    let real: Vec<_> = commands().into_iter().map(|(n, _)| n).collect();
    let invented: Vec<_> = noida::redis::command_names().filter(|n| !real.contains(n)).collect();
    assert!(invented.is_empty(), "not Redis 7.2 commands: {invented:?}");
}

#[test]
fn out_of_scope_commands_stay_unknown() {
    for name in OUT_OF_SCOPE {
        assert!(!noida::redis::is_implemented(name), "{name} is out of scope but implemented");
    }
    let known: Vec<_> = commands().into_iter().map(|(n, _)| n).collect();
    for name in OUT_OF_SCOPE {
        assert!(known.contains(name), "{name} is not a Redis 7.2 command");
    }
}

#[test]
#[ignore = "finish line: passes once every in-scope command is implemented"]
fn all_commands_implemented() {
    let missing = missing();
    let mut by_group: std::collections::BTreeMap<&str, Vec<&str>> = Default::default();
    for (name, group) in &missing {
        by_group.entry(group).or_default().push(name);
    }
    let report: Vec<String> = by_group
        .iter()
        .map(|(g, names)| format!("{g} ({}): {}", names.len(), names.join(" ")))
        .collect();
    assert!(missing.is_empty(), "{} missing:\n{}", missing.len(), report.join("\n"));
}
