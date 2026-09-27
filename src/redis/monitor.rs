//! MONITOR: stream every command other clients run, as Redis 7.2 does
//! (`replicationFeedMonitors`).
//!
//! A command is shown after it ran, with the database it left the client in,
//! except EVAL/EVALSHA/FCALL (and their `_RO` forms), which are shown first so
//! the commands a script runs come after it. Admin commands are not shown.

use super::command_meta;
use super::engine::{Command, Ctx, Engine, Reply, cmd};
use super::resp::Value;

pub static COMMANDS: &[Command] = &[cmd("monitor", monitor)];

fn monitor(ctx: &mut Ctx, _: &[Vec<u8>]) -> Reply {
    if ctx.deny_blocking {
        return Err(Value::err("ERR MONITOR isn't allowed for DENY BLOCKING client"));
    }
    let id = ctx.session.id;
    if ctx.engine.monitors.contains(&id) {
        // Already monitoring: Redis ignores it without replying.
        return Ok(Value::NoReply);
    }
    ctx.engine.monitors.push(id);
    ctx.client().monitor = true;
    Ok(Value::ok())
}

/// When (if ever) a command is shown to monitors.
#[derive(PartialEq)]
pub(crate) enum Feed {
    Never,
    Before,
    After,
}

/// Looks the command up the way dispatch does, subcommands included.
pub(crate) fn feed_mode(args: &[Vec<u8>]) -> Feed {
    let name = String::from_utf8_lossy(&args[0]).to_ascii_lowercase();
    let Some(mut meta) = command_meta::lookup(&name) else { return Feed::Never };
    if !meta.subcommands.is_empty() && args.len() > 1 {
        let sub = String::from_utf8_lossy(&args[1]).to_ascii_lowercase();
        match command_meta::lookup(&format!("{name}|{sub}")) {
            Some(m) => meta = m,
            None => return Feed::Never,
        }
    }
    if meta.has_flag("admin") {
        Feed::Never
    } else if meta.has_flag("skip_monitor") {
        // Only the script-call commands carry this flag; they feed themselves
        // ahead of the script.
        Feed::Before
    } else {
        Feed::After
    }
}

/// `sdscatrepr`: a quoted, escaped rendering of arbitrary bytes.
pub(crate) fn repr(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() + 2);
    out.push('"');
    for &b in bytes {
        match b {
            b'\\' | b'"' => {
                out.push('\\');
                out.push(b as char);
            }
            b'\n' => out.push_str("\\n"),
            b'\r' => out.push_str("\\r"),
            b'\t' => out.push_str("\\t"),
            7 => out.push_str("\\a"),
            8 => out.push_str("\\b"),
            0x20..=0x7e => out.push(b as char),
            _ => out.push_str(&format!("\\x{b:02x}")),
        }
    }
    out.push('"');
    out
}

impl Engine {
    /// Sends `args`, run by client `id`, to every monitor.
    pub(crate) fn feed_monitors(&mut self, id: u64, args: &[Vec<u8>]) {
        let Some(c) = self.clients.get(&id) else { return };
        let now = self.now();
        let origin = if self.lua_calls > 0 {
            format!("{} lua", c.db)
        } else {
            format!("{} {}", c.db, c.conn.addr)
        };
        let mut line = format!("{}.{:06} [{origin}]", now / 1000, (now % 1000) * 1000);
        for a in args {
            line.push(' ');
            line.push_str(&repr(a));
        }
        for m in self.monitors.clone() {
            self.deliver(m, Value::Simple(line.clone()));
        }
    }
}
