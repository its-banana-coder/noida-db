//! Connection commands: PING, ECHO, SELECT, QUIT, HELLO, AUTH, RESET and
//! CLIENT. Ported from Redis's networking.c and server.c.

use super::REDIS_VERSION;
use super::engine::{
    Client, Command, Ctx, Pause, Reply, arity_error, cmd, container, db_arg, eq_ic, help_reply,
    int_arg, int_arg_msg, syntax, timeout_ms_arg,
};
use super::resp::Value;

pub static COMMANDS: &[Command] = &[
    cmd("ping", ping),
    cmd("echo", echo),
    cmd("select", select),
    cmd("quit", quit),
    cmd("hello", hello),
    cmd("auth", auth),
    cmd("reset", reset),
    container("client", no_subcommand, CLIENT),
];

static CLIENT: &[Command] = &[
    cmd("help", client_help),
    cmd("id", client_id),
    cmd("info", client_info),
    cmd("list", client_list),
    cmd("kill", client_kill),
    cmd("getname", client_getname),
    cmd("setname", client_setname),
    cmd("setinfo", client_setinfo),
    cmd("reply", client_reply),
    cmd("no-evict", client_no_evict),
    cmd("no-touch", client_no_touch),
    cmd("pause", client_pause),
    cmd("unpause", client_unpause),
    cmd("unblock", client_unblock),
];

/// CLIENT alone fails its arity check before reaching this.
fn no_subcommand(_: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    Err(arity_error(&String::from_utf8_lossy(&a[0]).to_ascii_lowercase()))
}

fn ping(_: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    match a.len() {
        1 => Ok(Value::Simple("PONG".into())),
        2 => Ok(Value::bulk(&a[1])),
        _ => Err(arity_error("ping")),
    }
}

fn echo(_: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    Ok(Value::bulk(&a[1]))
}

fn select(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    ctx.client().db = db_arg(&a[1])?;
    Ok(Value::ok())
}

fn quit(ctx: &mut Ctx, _: &[Vec<u8>]) -> Reply {
    ctx.session.closing = true;
    Ok(Value::ok())
}

const WRONGPASS: &str = "WRONGPASS invalid username-password pair or user is disabled.";

/// noida has one user, "default", with no password (Redis's out-of-the-box
/// setup), so any password works for it.
fn authenticate(user: &[u8], _password: &[u8]) -> Result<(), Value> {
    if user == b"default" { Ok(()) } else { Err(Value::err(WRONGPASS)) }
}

fn auth(_: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    match a.len() {
        2 => Err(Value::err(
            "ERR AUTH <password> called without any password configured for the default user. \
             Are you sure your configuration is correct?",
        )),
        3 => authenticate(&a[1], &a[2]).map(|_| Value::ok()),
        _ => Err(syntax()),
    }
}

/// Client names and lib info must be printable ASCII without spaces, so
/// CLIENT LIST stays splittable on spaces.
fn valid_attr(v: &[u8]) -> bool {
    v.iter().all(|c| (b'!'..=b'~').contains(c))
}

const BAD_NAME: &str = "ERR Client names cannot contain spaces, newlines or special characters.";

fn set_name(client: &mut Client, name: &[u8]) -> Result<(), Value> {
    if !valid_attr(name) {
        return Err(Value::err(BAD_NAME));
    }
    client.name = (!name.is_empty()).then(|| name.to_vec());
    Ok(())
}

fn hello(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let mut version = None;
    let mut j = 1;
    if a.len() >= 2 {
        let v = int_arg_msg(&a[1], "Protocol version is not an integer or out of range")?;
        if !(2..=3).contains(&v) {
            return Err(Value::err("NOPROTO unsupported protocol version"));
        }
        version = Some(v as u8);
        j = 2;
    }
    let mut credentials = None;
    let mut name = None;
    while j < a.len() {
        let more = a.len() - 1 - j;
        if eq_ic(&a[j], "auth") && more >= 2 {
            credentials = Some((&a[j + 1], &a[j + 2]));
            j += 2;
        } else if eq_ic(&a[j], "setname") && more >= 1 {
            if !valid_attr(&a[j + 1]) {
                return Err(Value::err(BAD_NAME));
            }
            name = Some(&a[j + 1]);
            j += 1;
        } else {
            return Err(Value::err(format!(
                "ERR Syntax error in HELLO option '{}'",
                String::from_utf8_lossy(&a[j])
            )));
        }
        j += 1;
    }
    if let Some((user, pass)) = credentials {
        authenticate(user, pass)?;
    }
    let id = ctx.session.id as i64;
    let client = ctx.client();
    if let Some(name) = name {
        set_name(client, name)?;
    }
    if let Some(v) = version {
        client.resp = v;
    }
    let b = Value::bulk;
    Ok(Value::Map(vec![
        (b("server"), b("redis")),
        (b("version"), b(REDIS_VERSION)),
        (b("proto"), Value::Integer(client.resp as i64)),
        (b("id"), Value::Integer(id)),
        (b("mode"), b("standalone")),
        (b("role"), b("master")),
        (b("modules"), Value::Array(vec![])),
    ]))
}

/// RESET: back to the state of a fresh connection.
fn reset(ctx: &mut Ctx, _: &[Vec<u8>]) -> Reply {
    let c = ctx.client();
    c.db = 0;
    c.resp = 2;
    c.name = None;
    c.no_evict = false;
    c.no_touch = false;
    c.reply_off = false;
    c.reply_skip_next = false;
    Ok(Value::Simple("RESET".into()))
}

// ---- CLIENT ----

const CLIENT_HELP: &[&str] = &[
    "CACHING (YES|NO)",
    "    Enable/disable tracking of the keys for next command in OPTIN/OPTOUT modes.",
    "GETREDIR",
    "    Return the client ID we are redirecting to when tracking is enabled.",
    "GETNAME",
    "    Return the name of the current connection.",
    "ID",
    "    Return the ID of the current connection.",
    "INFO",
    "    Return information about the current client connection.",
    "KILL <ip:port>",
    "    Kill connection made from <ip:port>.",
    "KILL <option> <value> [<option> <value> [...]]",
    "    Kill connections. Options are:",
    "    * ADDR (<ip:port>|<unixsocket>:0)",
    "      Kill connections made from the specified address",
    "    * LADDR (<ip:port>|<unixsocket>:0)",
    "      Kill connections made to specified local address",
    "    * TYPE (NORMAL|MASTER|REPLICA|PUBSUB)",
    "      Kill connections by type.",
    "    * USER <username>",
    "      Kill connections authenticated by <username>.",
    "    * SKIPME (YES|NO)",
    "      Skip killing current connection (default: yes).",
    "LIST [options ...]",
    "    Return information about client connections. Options:",
    "    * TYPE (NORMAL|MASTER|REPLICA|PUBSUB)",
    "      Return clients of specified type.",
    "UNPAUSE",
    "    Stop the current client pause, resuming traffic.",
    "PAUSE <timeout> [WRITE|ALL]",
    "    Suspend all, or just write, clients for <timeout> milliseconds.",
    "REPLY (ON|OFF|SKIP)",
    "    Control the replies sent to the current connection.",
    "SETNAME <name>",
    "    Assign the name <name> to the current connection.",
    "SETINFO <option> <value>",
    "    Set client meta attr. Options are:",
    "    * LIB-NAME: the client lib name.",
    "    * LIB-VER: the client lib version.",
    "UNBLOCK <clientid> [TIMEOUT|ERROR]",
    "    Unblock the specified blocked client.",
    "TRACKING (ON|OFF) [REDIRECT <id>] [BCAST] [PREFIX <prefix> [...]]",
    "         [OPTIN] [OPTOUT] [NOLOOP]",
    "    Control server assisted client side caching.",
    "TRACKINGINFO",
    "    Report tracking status for the current connection.",
    "NO-EVICT (ON|OFF)",
    "    Protect current client connection from eviction.",
    "NO-TOUCH (ON|OFF)",
    "    Will not touch LRU/LFU stats when this mode is on.",
];

fn client_help(_: &mut Ctx, _: &[Vec<u8>]) -> Reply {
    Ok(help_reply("client", CLIENT_HELP))
}

fn client_id(ctx: &mut Ctx, _: &[Vec<u8>]) -> Reply {
    Ok(Value::Integer(ctx.session.id as i64))
}

/// One CLIENT LIST line, field for field as Redis's `catClientInfoString`.
/// Buffer and memory counters are reported as 0: noida doesn't do
/// performance analysis.
fn info_line(c: &Client, now: u64) -> String {
    let mut flags = String::new();
    if c.blocked.is_some() {
        flags.push('b');
    }
    if c.no_evict {
        flags.push('e');
    }
    if c.no_touch {
        flags.push('T');
    }
    if flags.is_empty() {
        flags.push('N');
    }
    let s = |v: &Option<Vec<u8>>| {
        v.as_deref().map(|b| String::from_utf8_lossy(b).into_owned()).unwrap_or_default()
    };
    format!(
        "id={} addr={} laddr={} fd={} name={} age={} idle={} flags={} db={} sub=0 psub=0 ssub=0 \
         multi=-1 qbuf=0 qbuf-free=0 argv-mem=0 multi-mem=0 rbs=0 rbp=0 obl=0 oll=0 omem=0 \
         tot-mem=0 events=r cmd={} user=default redir=-1 resp={} lib-name={} lib-ver={}",
        c.id,
        c.conn.addr,
        c.conn.laddr,
        c.conn.fd,
        s(&c.name),
        now.saturating_sub(c.created) / 1000,
        now.saturating_sub(c.last_interaction) / 1000,
        flags,
        c.db,
        c.last_cmd.as_deref().unwrap_or("NULL"),
        c.resp,
        s(&c.lib_name),
        s(&c.lib_ver),
    )
}

fn txt(s: String) -> Value {
    Value::Verbatim("txt", s.into_bytes())
}

fn client_info(ctx: &mut Ctx, _: &[Vec<u8>]) -> Reply {
    let now = ctx.now;
    Ok(txt(info_line(ctx.client(), now) + "\n"))
}

/// Client types for LIST/KILL TYPE. noida's clients are all "normal"
/// (pub/sub subscribers will report "pubsub").
fn client_type(name: &[u8]) -> Result<&'static str, Value> {
    match name.to_ascii_lowercase().as_slice() {
        b"normal" => Ok("normal"),
        b"slave" | b"replica" => Ok("replica"),
        b"pubsub" => Ok("pubsub"),
        b"master" => Ok("master"),
        _ => {
            Err(Value::err(format!("ERR Unknown client type '{}'", String::from_utf8_lossy(name))))
        }
    }
}

fn client_list(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let now = ctx.now;
    let mut out = String::new();
    if a.len() == 4 && eq_ic(&a[2], "type") {
        if client_type(&a[3])? == "normal" {
            for c in ctx.engine.clients.values() {
                out += &(info_line(c, now) + "\n");
            }
        }
    } else if a.len() > 3 && eq_ic(&a[2], "id") {
        for raw in &a[3..] {
            let id = int_arg_msg(raw, "Invalid client ID")?;
            if let Some(c) = ctx.engine.clients.get(&(id as u64)) {
                out += &(info_line(c, now) + "\n");
            }
        }
    } else if a.len() != 2 {
        return Err(syntax());
    } else {
        for c in ctx.engine.clients.values() {
            out += &(info_line(c, now) + "\n");
        }
    }
    Ok(txt(out))
}

fn client_kill(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let old_style = a.len() == 3;
    let (mut addr, mut laddr, mut ty, mut id) = (None, None, None, None);
    let mut skipme = !old_style;
    if old_style {
        addr = Some(a[2].clone());
    } else {
        let mut i = 2;
        while i < a.len() {
            let Some(v) = a.get(i + 1) else { return Err(syntax()) };
            match a[i].to_ascii_lowercase().as_slice() {
                b"id" => {
                    let n = int_arg_msg(v, "client-id should be greater than 0")?;
                    if n < 1 {
                        return Err(Value::err("ERR client-id should be greater than 0"));
                    }
                    id = Some(n as u64);
                }
                b"type" => ty = Some(client_type(v)?),
                b"addr" => addr = Some(v.clone()),
                b"laddr" => laddr = Some(v.clone()),
                b"user" => {
                    if v.as_slice() != b"default" {
                        return Err(Value::err(format!(
                            "ERR No such user '{}'",
                            String::from_utf8_lossy(v)
                        )));
                    }
                }
                b"skipme" => {
                    skipme = match v.to_ascii_lowercase().as_slice() {
                        b"yes" => true,
                        b"no" => false,
                        _ => return Err(syntax()),
                    }
                }
                _ => return Err(syntax()),
            }
            i += 2;
        }
    }

    let me = ctx.session.id;
    let victims: Vec<u64> = ctx
        .engine
        .clients
        .values()
        .filter(|c| addr.as_ref().is_none_or(|x| c.conn.addr.as_bytes() == x.as_slice()))
        .filter(|c| laddr.as_ref().is_none_or(|x| c.conn.laddr.as_bytes() == x.as_slice()))
        .filter(|_| ty.is_none_or(|t| t == "normal"))
        .filter(|c| id.is_none_or(|i| c.id == i))
        .filter(|c| !(skipme && c.id == me))
        .map(|c| c.id)
        .collect();
    for v in &victims {
        if *v == me {
            // Close after the reply goes out.
            ctx.session.closing = true;
        } else if let Some(c) = ctx.engine.remove_client(*v)
            && let Some(kill) = &c.conn.kill
        {
            kill();
        }
    }
    if old_style {
        if victims.is_empty() { Err(Value::err("ERR No such client")) } else { Ok(Value::ok()) }
    } else {
        Ok(Value::Integer(victims.len() as i64))
    }
}

fn client_getname(ctx: &mut Ctx, _: &[Vec<u8>]) -> Reply {
    Ok(ctx.client().name.as_ref().map_or(Value::Null, Value::bulk))
}

fn client_setname(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    set_name(ctx.client(), &a[2]).map(|_| Value::ok())
}

fn client_setinfo(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let attr = String::from_utf8_lossy(&a[2]).into_owned();
    let lib_name = match attr.to_ascii_lowercase().as_str() {
        "lib-name" => true,
        "lib-ver" => false,
        _ => return Err(Value::err(format!("ERR Unrecognized option '{attr}'"))),
    };
    if !valid_attr(&a[3]) {
        return Err(Value::err(format!(
            "ERR {attr} cannot contain spaces, newlines or special characters."
        )));
    }
    let value = (!a[3].is_empty()).then(|| a[3].clone());
    let c = ctx.client();
    if lib_name {
        c.lib_name = value;
    } else {
        c.lib_ver = value;
    }
    Ok(Value::ok())
}

fn client_reply(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let c = ctx.client();
    match a[2].to_ascii_lowercase().as_slice() {
        b"on" => {
            c.reply_off = false;
            c.reply_skip_next = false;
            Ok(Value::ok())
        }
        b"off" => {
            c.reply_off = true;
            Ok(Value::NoReply)
        }
        b"skip" => {
            if !c.reply_off {
                c.reply_skip_next = true;
            }
            Ok(Value::NoReply)
        }
        _ => Err(syntax()),
    }
}

fn on_off(v: &[u8]) -> Result<bool, Value> {
    match v.to_ascii_lowercase().as_slice() {
        b"on" => Ok(true),
        b"off" => Ok(false),
        _ => Err(syntax()),
    }
}

fn client_no_evict(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    ctx.client().no_evict = on_off(&a[2])?;
    Ok(Value::ok())
}

fn client_no_touch(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    ctx.client().no_touch = on_off(&a[2])?;
    Ok(Value::ok())
}

fn client_pause(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let mut all = true;
    if a.len() == 4 {
        if eq_ic(&a[3], "write") {
            all = false;
        } else if !eq_ic(&a[3], "all") {
            return Err(Value::err("ERR CLIENT PAUSE mode must be WRITE or ALL"));
        }
    }
    let until = timeout_ms_arg(&a[2], ctx.now)?;
    // Like Redis, a new pause never shortens or weakens an active one.
    let pause = match ctx.engine.pause.take() {
        Some(p) => Pause { until: p.until.max(until), all: p.all || all },
        None => Pause { until, all },
    };
    ctx.engine.pause = Some(pause);
    Ok(Value::ok())
}

fn client_unpause(ctx: &mut Ctx, _: &[Vec<u8>]) -> Reply {
    ctx.engine.pause = None;
    Ok(Value::ok())
}

fn client_unblock(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let error = a.len() == 4 && eq_ic(&a[3], "error");
    if a.len() == 4 && !error && !eq_ic(&a[3], "timeout") {
        return Err(Value::err("ERR CLIENT UNBLOCK reason should be TIMEOUT or ERROR"));
    }
    let id = int_arg(&a[2])?;
    let reply = if error {
        Value::err("UNBLOCKED client unblocked via CLIENT UNBLOCK")
    } else {
        Value::NullArray
    };
    Ok(Value::Integer(ctx.engine.unblock_with(id as u64, reply) as i64))
}
