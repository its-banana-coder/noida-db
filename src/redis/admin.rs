//! Server introspection: COMMAND and INFO.

use super::REDIS_VERSION;
use super::command_meta::{self, CommandMeta};
use super::engine::{
    Command, Ctx, NUM_DBS, Reply, cmd, container, eq_ic, help_reply, is_implemented, syntax,
};
use super::resp::Value;

pub static COMMANDS: &[Command] = &[container("command", command_all, COMMAND), cmd("info", info)];

static COMMAND: &[Command] = &[
    cmd("count", command_count),
    cmd("docs", command_docs),
    cmd("getkeys", command_getkeys),
    cmd("getkeysandflags", command_getkeysandflags),
    cmd("help", command_help),
    cmd("info", command_info),
    cmd("list", command_list),
];

/// COMMAND reports only what noida implements, so clients never plan
/// around a command that would fail.
fn implemented() -> impl Iterator<Item = &'static CommandMeta> {
    command_meta::all().iter().filter(|c| is_implemented(c.name))
}

fn lookup_implemented(name: &[u8]) -> Option<&'static CommandMeta> {
    let name = String::from_utf8_lossy(name).to_ascii_lowercase();
    command_meta::lookup(&name).filter(|m| is_implemented(m.name))
}

fn command_all(_: &mut Ctx, _: &[Vec<u8>]) -> Reply {
    Ok(Value::Array(implemented().map(|c| c.info(&is_implemented)).collect()))
}

fn command_count(_: &mut Ctx, _: &[Vec<u8>]) -> Reply {
    Ok(Value::Integer(implemented().count() as i64))
}

fn command_info(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    if a.len() == 2 {
        return command_all(ctx, a);
    }
    Ok(Value::Array(
        a[2..]
            .iter()
            .map(|n| lookup_implemented(n).map_or(Value::Null, |c| c.info(&is_implemented)))
            .collect(),
    ))
}

fn command_docs(_: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let docs = |c: &CommandMeta| (Value::bulk(c.name), c.docs(&is_implemented));
    if a.len() == 2 {
        return Ok(Value::Map(implemented().map(docs).collect()));
    }
    Ok(Value::Map(a[2..].iter().filter_map(|n| lookup_implemented(n)).map(docs).collect()))
}

fn command_list(_: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let mut filter: Option<(String, &[u8])> = None;
    let mut i = 2;
    while i < a.len() {
        if eq_ic(&a[i], "filterby") && a.len() - 1 - i == 2 {
            let kind = String::from_utf8_lossy(&a[i + 1]).to_ascii_lowercase();
            if !["module", "aclcat", "pattern"].contains(&kind.as_str()) {
                return Err(syntax());
            }
            filter = Some((kind, &a[i + 2]));
            i += 3;
        } else {
            return Err(syntax());
        }
    }
    let keep = |c: &CommandMeta| {
        filter.as_ref().is_none_or(|(k, arg)| command_meta::list_filter(c, k, arg))
    };
    let mut names = Vec::new();
    for c in implemented() {
        if keep(c) {
            names.push(Value::bulk(c.name));
        }
        for s in c.subcommands.iter().filter(|s| is_implemented(s.name)) {
            if keep(s) {
                names.push(Value::bulk(s.name));
            }
        }
    }
    Ok(Value::Array(names))
}

/// A port of `getKeysSubcommandImpl`.
fn getkeys(a: &[Vec<u8>], with_flags: bool) -> Reply {
    let argv = &a[2..];
    let name = String::from_utf8_lossy(&argv[0]).to_ascii_lowercase();
    let full = match command_meta::lookup(&name) {
        Some(m) if !m.subcommands.is_empty() && argv.len() >= 2 => {
            format!("{name}|{}", String::from_utf8_lossy(&argv[1]).to_ascii_lowercase())
        }
        _ => name,
    };
    let Some(meta) = command_meta::lookup(&full) else {
        return Err(Value::err("ERR Invalid command specified"));
    };
    if !meta.has_keys() {
        return Err(Value::err("ERR The command has no key arguments"));
    }
    let n = argv.len() as i64;
    if (meta.arity > 0 && meta.arity != n) || n < -meta.arity {
        return Err(Value::err("ERR Invalid number of arguments specified for command"));
    }
    let keys = meta.keys(argv);
    if keys.is_empty() {
        if meta.has_flag("no_mandatory_keys") {
            return Ok(Value::Array(vec![]));
        }
        return Err(Value::err("ERR Invalid arguments specified for command"));
    }
    Ok(Value::Array(
        keys.into_iter()
            .map(|(pos, flags)| {
                let key = Value::bulk(&argv[pos]);
                if with_flags {
                    Value::Array(vec![key, command_meta::key_flags(flags)])
                } else {
                    key
                }
            })
            .collect(),
    ))
}

fn command_getkeys(_: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    getkeys(a, false)
}

fn command_getkeysandflags(_: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    getkeys(a, true)
}

const COMMAND_HELP: &[&str] = &[
    "(no subcommand)",
    "    Return details about all Redis commands.",
    "COUNT",
    "    Return the total number of commands in this Redis server.",
    "LIST",
    "    Return a list of all commands in this Redis server.",
    "INFO [<command-name> ...]",
    "    Return details about multiple Redis commands.",
    "    If no command names are given, documentation details for all",
    "    commands are returned.",
    "DOCS [<command-name> ...]",
    "    Return documentation details about multiple Redis commands.",
    "    If no command names are given, documentation details for all",
    "    commands are returned.",
    "GETKEYS <full-command>",
    "    Return the keys from a full Redis command.",
    "GETKEYSANDFLAGS <full-command>",
    "    Return the keys and the access flags from a full Redis command.",
];

fn command_help(_: &mut Ctx, _: &[Vec<u8>]) -> Reply {
    Ok(help_reply("command", COMMAND_HELP))
}

// ---- INFO ----

const DEFAULT_SECTIONS: &[&str] = &[
    "server",
    "clients",
    "memory",
    "persistence",
    "stats",
    "replication",
    "cpu",
    "module_list",
    "errorstats",
    "cluster",
    "keyspace",
];

/// INFO with Redis's sections and field names. Counters that only matter
/// for performance analysis are reported as 0.
fn info(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let mut wanted: Vec<String> = Vec::new();
    let mut all = false;
    if a.len() == 1 {
        wanted.extend(DEFAULT_SECTIONS.iter().map(|s| s.to_string()));
    }
    for arg in &a[1..] {
        let s = String::from_utf8_lossy(arg).to_ascii_lowercase();
        match s.as_str() {
            "default" => wanted.extend(DEFAULT_SECTIONS.iter().map(|s| s.to_string())),
            "all" | "everything" => all = true,
            _ => wanted.push(s),
        }
    }
    let want =
        |s: &str| all || wanted.iter().any(|w| w == s || (s == "module_list" && w == "modules"));

    let now = ctx.now;
    let uptime = now.saturating_sub(ctx.engine.started) / 1000;
    let mut sections: Vec<String> = Vec::new();
    let mut add = |title: &str, fields: Vec<(String, String)>| {
        let mut s = format!("# {title}\r\n");
        for (k, v) in fields {
            s += &format!("{k}:{v}\r\n");
        }
        sections.push(s);
    };
    let f = |k: &str, v: String| (k.to_string(), v);

    if want("server") {
        add(
            "Server",
            vec![
                f("redis_version", REDIS_VERSION.into()),
                f("redis_git_sha1", "00000000".into()),
                f("redis_git_dirty", "0".into()),
                f("redis_build_id", "0".into()),
                f("redis_mode", "standalone".into()),
                f("os", format!("{} {}", std::env::consts::OS, std::env::consts::ARCH)),
                f("arch_bits", (usize::BITS).to_string()),
                f("monotonic_clock", "POSIX clock_gettime".into()),
                f("multiplexing_api", "noida".into()),
                f("atomicvar_api", "c11-builtin".into()),
                f("gcc_version", "0.0.0".into()),
                f("process_id", std::process::id().to_string()),
                f("process_supervised", "no".into()),
                f("run_id", format!("{:040x}", ctx.engine.started)),
                f("tcp_port", "6379".into()),
                f("server_time_usec", (now * 1000).to_string()),
                f("uptime_in_seconds", uptime.to_string()),
                f("uptime_in_days", (uptime / 86400).to_string()),
                f("hz", "10".into()),
                f("configured_hz", "10".into()),
                f("lru_clock", ((now / 1000) & 0xFFFFFF).to_string()),
                f(
                    "executable",
                    std::env::current_exe().map(|p| p.display().to_string()).unwrap_or_default(),
                ),
                f("config_file", "".into()),
                f("io_threads_active", "0".into()),
            ],
        );
    }
    if want("clients") {
        add(
            "Clients",
            vec![
                f("connected_clients", ctx.engine.clients.len().to_string()),
                f("cluster_connections", "0".into()),
                f("maxclients", "10000".into()),
                f("client_recent_max_input_buffer", "0".into()),
                f("client_recent_max_output_buffer", "0".into()),
                f("blocked_clients", "0".into()),
                f("tracking_clients", "0".into()),
                f("clients_in_timeout_table", "0".into()),
                f("total_blocking_keys", "0".into()),
                f("total_blocking_keys_on_nokey", "0".into()),
            ],
        );
    }
    if want("memory") {
        add(
            "Memory",
            vec![
                f("used_memory", "0".into()),
                f("used_memory_human", "0B".into()),
                f("maxmemory", "0".into()),
                f("maxmemory_human", "0B".into()),
                f("maxmemory_policy", "noeviction".into()),
                f("mem_allocator", "libc".into()),
            ],
        );
    }
    if want("persistence") {
        add(
            "Persistence",
            vec![
                f("loading", "0".into()),
                f("async_loading", "0".into()),
                f("rdb_changes_since_last_save", "0".into()),
                f("rdb_bgsave_in_progress", "0".into()),
                f("rdb_last_save_time", (ctx.engine.started / 1000).to_string()),
                f("rdb_last_bgsave_status", "ok".into()),
                f("aof_enabled", "0".into()),
                f("aof_rewrite_in_progress", "0".into()),
                f("aof_last_bgrewrite_status", "ok".into()),
                f("aof_last_write_status", "ok".into()),
            ],
        );
    }
    if want("stats") {
        add(
            "Stats",
            vec![
                f("total_connections_received", "0".into()),
                f("total_commands_processed", "0".into()),
                f("instantaneous_ops_per_sec", "0".into()),
                f("rejected_connections", "0".into()),
                f("expired_keys", "0".into()),
                f("evicted_keys", "0".into()),
                f("keyspace_hits", "0".into()),
                f("keyspace_misses", "0".into()),
                f("pubsub_channels", "0".into()),
                f("pubsub_patterns", "0".into()),
                f("pubsubshard_channels", "0".into()),
                f("total_error_replies", "0".into()),
            ],
        );
    }
    if want("replication") {
        add(
            "Replication",
            vec![
                f("role", "master".into()),
                f("connected_slaves", "0".into()),
                f("master_failover_state", "no-failover".into()),
                f("master_replid", format!("{:040x}", ctx.engine.started)),
                f("master_replid2", "0".repeat(40)),
                f("master_repl_offset", "0".into()),
                f("second_repl_offset", "-1".into()),
                f("repl_backlog_active", "0".into()),
                f("repl_backlog_size", "1048576".into()),
                f("repl_backlog_first_byte_offset", "0".into()),
                f("repl_backlog_histlen", "0".into()),
            ],
        );
    }
    if want("cpu") {
        add(
            "CPU",
            vec![
                f("used_cpu_sys", "0.000000".into()),
                f("used_cpu_user", "0.000000".into()),
                f("used_cpu_sys_children", "0.000000".into()),
                f("used_cpu_user_children", "0.000000".into()),
            ],
        );
    }
    if want("module_list") {
        add("Modules", vec![]);
    }
    if want("errorstats") {
        add("Errorstats", vec![]);
    }
    if want("cluster") {
        add("Cluster", vec![f("cluster_enabled", "0".into())]);
    }
    if want("keyspace") {
        let mut fields = Vec::new();
        for i in 0..NUM_DBS {
            let (keys, expires) = ctx.engine.dbs[i].counts(now);
            if keys > 0 {
                fields
                    .push(f(&format!("db{i}"), format!("keys={keys},expires={expires},avg_ttl=0")));
            }
        }
        add("Keyspace", fields);
    }
    Ok(Value::Verbatim("txt", sections.join("\r\n").into_bytes()))
}
