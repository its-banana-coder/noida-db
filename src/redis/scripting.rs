//! Lua scripting: EVAL, EVALSHA, their read-only variants and SCRIPT,
//! ported from Redis's eval.c and script_lua.c.
//!
//! Redis embeds Lua 5.1, so noida-db embeds the same interpreter (mlua with a
//! vendored Lua 5.1). Each script runs in a fresh interpreter with Redis's
//! sandbox: no globals may be created or read unless they exist, `redis.*`
//! runs commands through the engine, and the Lua/RESP conversions and error
//! strings follow script_lua.c.

use std::cell::RefCell;

use mlua::{Lua, LuaOptions, LuaString, MultiValue, StdLib, Table, Value as Lv, Variadic};

use super::REDIS_VERSION;
use super::engine::{Command, Ctx, Engine, Reply, cmd, container, eq_ic, help_reply, resolve};
use super::resp::Value;
use super::{command_meta, sha1};

pub static COMMANDS: &[Command] = &[
    cmd("eval", eval),
    cmd("eval_ro", eval_ro),
    cmd("evalsha", evalsha),
    cmd("evalsha_ro", evalsha_ro),
    container("script", script_help, SCRIPT),
];

static SCRIPT: &[Command] = &[
    cmd("load", script_load),
    cmd("exists", script_exists),
    cmd("flush", script_flush),
    cmd("kill", script_kill),
    cmd("help", script_help),
];

/// Redis's error handler for scripts: it turns a Lua error into a table
/// carrying the source and line the error came from (eval.c).
const ERR_HANDLER: &str = r#"
local dbg = debug
return function (err)
  -- Report the script's own frame: skip C functions (like `error`) and our
  -- Lua nonexistent-global guard (@protect), which in Redis is a C function.
  local level = 2
  local i = dbg.getinfo(level,'nSl')
  while i and (i.what == 'C' or i.source == '@protect' or i.source == '@wrap_lib') do
    level = level + 1
    i = dbg.getinfo(level,'nSl')
  end
  if type(err) ~= 'table' then
    err = {err='ERR ' .. tostring(err)}
  end
  if i then
    if err['source'] == nil then err['source'] = i.source end
    if err['line'] == nil then err['line'] = i.currentline end
  end
  return err
end
"#;

/// C library functions in Redis fail with `luaL_error`, whose message starts
/// with the position of the calling Lua function ("user_script:1: ...") and is
/// a plain string. Errors from our Rust functions arrive as opaque values, so
/// wrap each function to re-raise them the same way: one level up, which
/// gives no position when the caller is `pcall`, as in Redis.
const WRAP_LIB_ERRORS: &str = r#"
local pcall, tostring, error, type, select, unpack = pcall, tostring, error, type, select, unpack
local function pack(...) return {n = select('#', ...), ...} end
return function (lib)
  for name, f in pairs(lib) do
    if type(f) == 'function' then
      lib[name] = function (...)
        local r = pack(pcall(f, ...))
        if r[1] then return unpack(r, 2, r.n) end
        local e = tostring(r[2])
        e = e:match('^runtime error: (.-)\nstack traceback:') or e:match('^runtime error: (.*)$') or e
        -- Not a tail call, so level 2 is the script (or pcall) that called us.
        error(e, 2)
      end
    end
  end
  return lib
end
"#;

/// The Lua side of `redis.call` / `redis.pcall`: it runs the command
/// through Rust and, for `call`, raises the error table with the caller's
/// position, exactly as Redis's C implementation does.
const REDIS_LIB: &str = r#"
local rawcall, dbg = ...
-- Lua 5.1 drops the caller's frame on `return redis.call(...)` (a tail
-- call into this Lua wrapper), so the stack no longer says where the script
-- was. Redis's C implementation keeps the frame, so track the running line
-- of the script with a line hook and use it for that case.
local script_line = -1
dbg.sethook(function (_, line)
  local i = dbg.getinfo(2, 'S')
  if i and i.source == '@user_script' then script_line = line end
end, 'l')
local function locate(r)
  local i = dbg.getinfo(3, 'nSl')
  if i and i.what == 'tail' then
    r.source = '@user_script'; r.line = script_line
  elseif i then
    r.source = i.source; r.line = i.currentline
  end
end
local function call(...)
  local r = rawcall(...)
  if type(r) == 'table' and r.err ~= nil then
    locate(r)
    error(r, 0)
  end
  return r
end
-- redis.pcall returns the error table instead of raising it.
return call, rawcall
"#;

/// The sandbox Redis puts around the globals: reading or creating an
/// unknown global is an error (script_lua.c's readonly global table).
const PROTECT_GLOBALS: &str = r#"
setmetatable(_G, {
  __index = function (t, n)
    error("Script attempted to access nonexistent global variable '"..tostring(n).."'", 2)
  end,
  __newindex = function (t, n, v)
    error("Attempt to modify a readonly table", 2)
  end,
})
"#;

/// Scripts noida-db has seen, by SHA-1 digest (Redis's script cache).
#[derive(Default)]
pub struct Scripts {
    pub cache: std::collections::HashMap<String, Vec<u8>>,
}

impl Engine {
    fn script_body(&self, sha: &str) -> Option<Vec<u8>> {
        self.scripts.cache.get(&sha.to_ascii_lowercase()).cloned()
    }
}

// ---- EVAL ----

fn eval(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    eval_generic(ctx, a, false, false)
}

fn eval_ro(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    eval_generic(ctx, a, false, true)
}

fn evalsha(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    eval_generic(ctx, a, true, false)
}

fn evalsha_ro(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    eval_generic(ctx, a, true, true)
}

fn eval_generic(ctx: &mut Ctx, a: &[Vec<u8>], by_sha: bool, read_only: bool) -> Reply {
    let numkeys = super::engine::int_arg(&a[2])?;
    if numkeys > a.len() as i64 - 3 {
        return Err(Value::err("ERR Number of keys can't be greater than number of args"));
    }
    if numkeys < 0 {
        return Err(Value::err("ERR Number of keys can't be negative"));
    }
    let numkeys = numkeys as usize;
    let (body, sha) = if by_sha {
        let sha = String::from_utf8_lossy(&a[1]).to_ascii_lowercase();
        let Some(body) = ctx.engine.script_body(&sha) else {
            return Err(Value::err("NOSCRIPT No matching script. Please use EVAL."));
        };
        (body, sha)
    } else {
        let sha = sha1::hex(&a[1]);
        (a[1].clone(), sha)
    };
    let keys = &a[3..3 + numkeys];
    let args = &a[3 + numkeys..];
    let reply = run_script(ctx, &body, &sha, keys, args, read_only)?;
    if !by_sha {
        ctx.engine.scripts.cache.insert(sha, body);
    }
    Ok(reply)
}

// ---- SCRIPT ----

fn script_load(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    compile_check(&a[2])?;
    let sha = sha1::hex(&a[2]);
    ctx.engine.scripts.cache.insert(sha.clone(), a[2].clone());
    Ok(Value::bulk(sha))
}

fn script_exists(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let found = a[2..]
        .iter()
        .map(|s| {
            let sha = String::from_utf8_lossy(s).to_ascii_lowercase();
            Value::Integer(ctx.engine.scripts.cache.contains_key(&sha) as i64)
        })
        .collect();
    Ok(Value::Array(found))
}

fn script_flush(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    if a.len() > 3 || (a.len() == 3 && !eq_ic(&a[2], "sync") && !eq_ic(&a[2], "async")) {
        return Err(Value::err("ERR SCRIPT FLUSH only support SYNC|ASYNC option"));
    }
    ctx.engine.scripts.cache.clear();
    Ok(Value::ok())
}

/// No script ever runs long enough to be killed: noida-db runs them to
/// completion while holding the engine lock.
fn script_kill(_: &mut Ctx, _: &[Vec<u8>]) -> Reply {
    Err(Value::err("NOTBUSY No scripts in execution right now."))
}

fn script_help(_: &mut Ctx, _: &[Vec<u8>]) -> Reply {
    Ok(help_reply(
        "script",
        &[
            "EXISTS <sha1> [<sha1> ...]",
            "    Return information about the existence of the scripts in the script cache.",
            "FLUSH [ASYNC|SYNC]",
            "    Flush the Lua scripts cache. Very dangerous on replicas.",
            "    When called without the optional mode argument, the behavior is determined by the",
            "    lazyfree-lazy-user-flush configuration directive. Valid modes are:",
            "    * ASYNC: Asynchronously flush the scripts cache.",
            "    * SYNC: Synchronously flush the scripts cache.",
            "KILL",
            "    Kill the currently executing Lua script.",
            "LOAD <script>",
            "    Load a script into the scripts cache without executing it.",
        ],
    ))
}

// ---- running a script ----

/// A fresh interpreter with the libraries Redis gives scripts. `debug` is
/// only there to build the error handler and is removed afterwards, as
/// Redis does.
fn new_lua() -> Lua {
    // SAFETY: the debug library is loaded, then taken away from scripts
    // before any of them runs (see `build_env`).
    unsafe { Lua::unsafe_new_with(StdLib::ALL, LuaOptions::default()) }
}

/// Compiles `body` to check it, as SCRIPT LOAD does.
fn compile_check(body: &[u8]) -> Result<(), Value> {
    let lua = new_lua();
    load_script(&lua, body).map(|_| ())
}

fn load_script(lua: &Lua, body: &[u8]) -> Result<mlua::Function, Value> {
    lua.load(body).set_name("@user_script").into_function().map_err(|e| {
        let msg = lua_error_message(&e);
        // Lua reports "[string "user_script"]:1: ..."; Redis's chunk name
        // makes it "user_script:1: ...".
        let msg =
            msg.rsplit_once("]:").map_or(msg.clone(), |(_, rest)| format!("user_script:{rest}"));
        Value::err(format!("ERR Error compiling script (new function): {}", first_line(&msg)))
    })
}

fn first_line(s: &str) -> String {
    s.lines().next().unwrap_or("").to_string()
}

fn lua_error_message(e: &mlua::Error) -> String {
    match e {
        mlua::Error::SyntaxError { message, .. } => message.clone(),
        mlua::Error::RuntimeError(m) => m.clone(),
        other => other.to_string(),
    }
}

/// Runs `body` with Redis's Lua environment and converts what it returns.
fn run_script(
    ctx: &mut Ctx,
    body: &[u8],
    sha: &str,
    keys: &[Vec<u8>],
    args: &[Vec<u8>],
    read_only: bool,
) -> Result<Value, Value> {
    let lua = new_lua();
    let resp = RefCell::new(2u8);
    let out: Result<Value, Value> = {
        let ctx_cell = RefCell::new(ctx);
        lua.scope(|scope| {
            let rawcall = scope.create_function(|lua, args: Variadic<Lv>| -> mlua::Result<Lv> {
                let mut c = ctx_cell.borrow_mut();
                let resp = *resp.borrow();
                Ok(do_call(lua, &mut c, args, read_only, resp))
            })?;
            let setresp = scope.create_function(|_, v: f64| -> mlua::Result<()> {
                if v != 2.0 && v != 3.0 {
                    return Err(script_error("RESP version must be 2 or 3."));
                }
                *resp.borrow_mut() = v as u8;
                Ok(())
            })?;
            build_env(&lua, rawcall, setresp, keys, args)?;

            let f = match load_script(&lua, body) {
                Ok(f) => f,
                Err(e) => return Ok(Err(e)),
            };
            let handler: mlua::Function = lua.load(ERR_HANDLER).set_name("@err_handler").call(())?;
            // Redis drops `debug` once the error handler holds a reference.
            lua.globals().raw_set("debug", Lv::Nil)?;
            lua.load(PROTECT_GLOBALS).set_name("@protect").exec()?;

            let xpcall: mlua::Function = lua.globals().raw_get("xpcall")?;
            let res: MultiValue = xpcall.call((f, handler))?;
            let mut it = res.into_iter();
            let ok = matches!(it.next(), Some(Lv::Boolean(true)));
            let value = it.next().unwrap_or(Lv::Nil);
            if ok {
                Ok(Ok(lua_to_reply(&value, *resp.borrow())))
            } else {
                Ok(Err(script_failure(&value, sha)))
            }
        })
        .map_err(|e| Value::err(format!("ERR {}", first_line(&lua_error_message(&e)))))?
    };
    out
}

/// An error raised from a Rust callback, carrying a Redis error message.
fn script_error(msg: &str) -> mlua::Error {
    mlua::Error::RuntimeError(msg.to_string())
}

/// Formats the error a failed script leaves behind (`luaCallFunction`).
fn script_failure(value: &Lv, sha: &str) -> Value {
    let Lv::Table(t) = value else {
        let msg = match value {
            Lv::String(s) => s.to_string_lossy().to_string(),
            other => format!("{other:?}"),
        };
        return Value::err(format!("ERR {msg}"));
    };
    let msg: Option<String> = t.get("err").ok();
    let source: Option<String> = t.get("source").ok();
    let line: Option<i64> = t.get("line").ok();
    let mut out = msg.unwrap_or_else(|| "ERR execution failure".into());
    if let (Some(source), Some(line)) = (source, line) {
        out += &format!(" script: {sha}, on {source}:{line}.");
    }
    Value::Error(out.replace(['\r', '\n'], " "))
}

/// Builds the sandbox: KEYS, ARGV, the `redis` table, cjson and the
/// globals Redis hides.
fn build_env(
    lua: &Lua,
    rawcall: mlua::Function,
    setresp: mlua::Function,
    keys: &[Vec<u8>],
    args: &[Vec<u8>],
) -> mlua::Result<()> {
    let globals = lua.globals();
    let dbg: Table = globals.raw_get("debug")?;
    let (call, pcall_): (mlua::Function, mlua::Function) =
        lua.load(REDIS_LIB).set_name("@redis_lib").call((rawcall, dbg))?;

    let redis = lua.create_table()?;
    redis.raw_set("call", call)?;
    redis.raw_set("pcall", pcall_)?;
    redis.raw_set("setresp", setresp)?;
    redis.raw_set(
        "error_reply",
        lua.create_function(|lua, msg: LuaString| {
            let t = lua.create_table()?;
            // luaRedisErrorReplyCommand adds the '-' so a leading word becomes the code.
            let mut dashed = msg.as_bytes().to_vec();
            if dashed.first() != Some(&b'-') {
                dashed.insert(0, b'-');
            }
            t.raw_set("err", error_message(&dashed))?;
            Ok(t)
        })?,
    )?;
    redis.raw_set(
        "status_reply",
        lua.create_function(|lua, msg: LuaString| {
            let t = lua.create_table()?;
            t.raw_set("ok", msg)?;
            Ok(t)
        })?,
    )?;
    redis.raw_set(
        "sha1hex",
        lua.create_function(|lua, v: Lv| {
            let s = lua.coerce_string(v)?;
            let bytes = s.as_ref().map(|s| s.as_bytes().to_vec()).unwrap_or_default();
            Ok(sha1::hex(&bytes))
        })?,
    )?;
    // noida-db keeps no log file; the levels exist so scripts can call it.
    redis.raw_set("log", lua.create_function(|_, _: Variadic<Lv>| Ok(()))?)?;
    for (name, level) in
        [("LOG_DEBUG", 0), ("LOG_VERBOSE", 1), ("LOG_NOTICE", 2), ("LOG_WARNING", 3)]
    {
        redis.raw_set(name, level)?;
    }
    for (name, flag) in
        [("REPL_NONE", 0), ("REPL_AOF", 1), ("REPL_SLAVE", 2), ("REPL_REPLICA", 2), ("REPL_ALL", 3)]
    {
        redis.raw_set(name, flag)?;
    }
    // Scripts are always "effect replicated" in Redis 7; these are no-ops.
    redis.raw_set("replicate_commands", lua.create_function(|_, ()| Ok(true))?)?;
    redis.raw_set("set_repl", lua.create_function(|_, _: Variadic<Lv>| Ok(()))?)?;
    redis.raw_set("breakpoint", lua.create_function(|_, ()| Ok(false))?)?;
    redis.raw_set("debug", lua.create_function(|_, _: Variadic<Lv>| Ok(()))?)?;
    redis.raw_set("REDIS_VERSION", REDIS_VERSION)?;
    let v: Vec<u32> = REDIS_VERSION.split('.').filter_map(|p| p.parse().ok()).collect();
    redis.raw_set("REDIS_VERSION_NUM", ((v[0] << 16) | (v[1] << 8) | v[2]) as i64)?;
    globals.raw_set("redis", &redis)?;
    globals.raw_set("server", &redis)?;

    globals.raw_set("KEYS", string_array(lua, keys)?)?;
    globals.raw_set("ARGV", string_array(lua, args)?)?;
    let wrap: mlua::Function = lua.load(WRAP_LIB_ERRORS).set_name("@wrap_lib").call(())?;
    globals.raw_set("cjson", wrap.call::<Table>(super::cjson::table(lua)?)?)?;
    globals.raw_set("cmsgpack", wrap.call::<Table>(super::cmsgpack::table(lua)?)?)?;

    // Globals Redis doesn't expose to scripts.
    for name in ["print", "dofile", "loadfile", "os", "io", "package", "require", "module"] {
        globals.raw_set(name, Lv::Nil)?;
    }
    Ok(())
}

/// `luaPushErrorBuff`: every error reply carries an error code.
fn error_message(msg: &[u8]) -> String {
    let msg = String::from_utf8_lossy(msg).to_string();
    let body = msg.strip_prefix('-').unwrap_or(&msg);
    let with_code = match body.split_once(' ') {
        Some(_) if msg.starts_with('-') => body.to_string(),
        _ => format!("ERR {body}"),
    };
    with_code.trim_end_matches(['\r', '\n']).to_string()
}

fn string_array(lua: &Lua, items: &[Vec<u8>]) -> mlua::Result<Table> {
    let t = lua.create_table()?;
    for (i, v) in items.iter().enumerate() {
        t.raw_set(i + 1, lua.create_string(v)?)?;
    }
    Ok(t)
}

/// `luaRedisGenericCommand`: runs one command for `redis.call`/`pcall`.
fn do_call(lua: &Lua, ctx: &mut Ctx, args: Variadic<Lv>, read_only: bool, resp: u8) -> Lv {
    // Errors come back as an error table; `redis.call` raises it, and
    // `redis.pcall` returns it (luaRedisGenericCommand with raise_error=0).
    let fail = |lua: &Lua, msg: &str| -> Lv {
        let t = lua.create_table().expect("table");
        let _ = t.raw_set("err", error_message(msg.as_bytes()));
        Lv::Table(t)
    };
    if args.is_empty() {
        return fail(lua, "Please specify at least one argument for this redis lib call");
    }
    let mut argv: Vec<Vec<u8>> = Vec::with_capacity(args.len());
    for v in args.iter() {
        match v {
            Lv::String(s) => argv.push(s.as_bytes().to_vec()),
            Lv::Integer(_) | Lv::Number(_) => {
                let n = match v {
                    Lv::Integer(n) => *n as f64,
                    Lv::Number(n) => *n,
                    _ => unreachable!(),
                };
                argv.push(number_arg(n).into_bytes());
            }
            _ => {
                return fail(lua, "Lua redis lib command arguments must be strings or integers");
            }
        }
    }
    let name = String::from_utf8_lossy(&argv[0]).to_ascii_lowercase();
    let Some(meta) = command_meta::lookup(&name) else {
        return fail(lua, "Unknown Redis command called from script");
    };
    if meta.has_flag("noscript") {
        return fail(lua, "This Redis command is not allowed from script");
    }
    let handler = match resolve(&argv) {
        Ok((handler, _)) => handler,
        Err(Value::Error(e)) if e.starts_with("ERR wrong number of arguments") => {
            return fail(lua, "Wrong number of args calling Redis command from script");
        }
        Err(_) => return fail(lua, "Unknown Redis command called from script"),
    };
    if read_only && meta.has_flag("write") {
        return fail(lua, "Write commands are not allowed from read-only scripts.");
    }
    let Ctx { engine, session, .. } = ctx;
    engine.lua_calls += 1;
    // A script talks RESP2 unless it called redis.setresp(3), whatever the
    // calling client speaks; command handlers shape replies by the client's.
    let saved = engine.clients.get_mut(&session.id).map(|c| std::mem::replace(&mut c.resp, resp));
    let reply = engine.call(session, handler, &argv, true, None);
    if let (Some(saved), Some(c)) = (saved, engine.clients.get_mut(&session.id)) {
        c.resp = saved;
    }
    engine.lua_calls -= 1;
    reply_to_lua(lua, &reply, resp)
}

/// RESP → Lua (`redisProtocolToLuaType`).
fn reply_to_lua(lua: &Lua, v: &Value, resp: u8) -> Lv {
    let table = |pairs: Vec<(&str, Lv)>| -> Lv {
        let t = lua.create_table().expect("table");
        for (k, v) in pairs {
            let _ = t.raw_set(k, v);
        }
        Lv::Table(t)
    };
    match v {
        Value::Integer(n) => Lv::Integer(*n),
        Value::Simple(s) => table(vec![("ok", Lv::String(lua.create_string(s).expect("string")))]),
        Value::Error(e) => table(vec![("err", Lv::String(lua.create_string(e).expect("string")))]),
        Value::Bulk(b) | Value::Verbatim(_, b) => Lv::String(lua.create_string(b).expect("string")),
        Value::Null | Value::NullArray | Value::NoReply => Lv::Boolean(false),
        Value::Bool(b) if resp >= 3 => Lv::Boolean(*b),
        Value::Bool(b) => {
            if *b {
                Lv::Integer(1)
            } else {
                Lv::Boolean(false)
            }
        }
        Value::Double(d) if resp >= 3 => table(vec![("double", Lv::Number(*d))]),
        Value::Double(d) => Lv::String(lua.create_string(super::double::d2string(*d)).expect("s")),
        Value::BigNumber(n) if resp >= 3 => {
            table(vec![("big_number", Lv::String(lua.create_string(n).expect("s")))])
        }
        Value::BigNumber(n) => Lv::String(lua.create_string(n).expect("string")),
        Value::Array(items) | Value::Push(items) | Value::Many(items) => {
            let t = lua.create_table().expect("table");
            for (i, item) in items.iter().enumerate() {
                let _ = t.raw_set(i + 1, reply_to_lua(lua, item, resp));
            }
            Lv::Table(t)
        }
        Value::Set(items) if resp >= 3 => {
            let inner = lua.create_table().expect("table");
            for item in items {
                let _ = inner.raw_set(reply_to_lua(lua, item, resp), true);
            }
            table(vec![("set", Lv::Table(inner))])
        }
        Value::Set(items) => {
            let t = lua.create_table().expect("table");
            for (i, item) in items.iter().enumerate() {
                let _ = t.raw_set(i + 1, reply_to_lua(lua, item, resp));
            }
            Lv::Table(t)
        }
        Value::Map(pairs) if resp >= 3 => {
            let inner = lua.create_table().expect("table");
            for (k, v) in pairs {
                let _ = inner.raw_set(reply_to_lua(lua, k, resp), reply_to_lua(lua, v, resp));
            }
            table(vec![("map", Lv::Table(inner))])
        }
        Value::Map(pairs) => {
            let t = lua.create_table().expect("table");
            let mut i = 1;
            for (k, v) in pairs {
                let _ = t.raw_set(i, reply_to_lua(lua, k, resp));
                let _ = t.raw_set(i + 1, reply_to_lua(lua, v, resp));
                i += 2;
            }
            Lv::Table(t)
        }
    }
}

/// Lua → RESP (`luaReplyToRedisReply`).
fn lua_to_reply(v: &Lv, resp: u8) -> Value {
    match v {
        Lv::Nil => Value::Null,
        Lv::Boolean(true) if resp >= 3 => Value::Bool(true),
        Lv::Boolean(false) if resp >= 3 => Value::Bool(false),
        Lv::Boolean(b) => {
            if *b {
                Value::Integer(1)
            } else {
                Value::Null
            }
        }
        Lv::Integer(n) => Value::Integer(*n),
        Lv::Number(n) => Value::Integer(*n as i64),
        Lv::String(s) => Value::Bulk(s.as_bytes().to_vec()),
        Lv::Table(t) => table_to_reply(t, resp),
        _ => Value::Null,
    }
}

fn table_to_reply(t: &Table, resp: u8) -> Value {
    if let Ok(Lv::String(err)) = t.raw_get::<Lv>("err") {
        let msg = String::from_utf8_lossy(&err.as_bytes()).replace(['\r', '\n'], " ");
        return Value::Error(msg);
    }
    if let Ok(Lv::String(ok)) = t.raw_get::<Lv>("ok") {
        let msg = String::from_utf8_lossy(&ok.as_bytes()).replace(['\r', '\n'], " ");
        return Value::Simple(msg);
    }
    if let Ok(Lv::Number(d)) = t.raw_get::<Lv>("double") {
        return Value::Double(d);
    }
    if let Ok(Lv::Integer(d)) = t.raw_get::<Lv>("double") {
        return Value::Double(d as f64);
    }
    if let Ok(Lv::String(n)) = t.raw_get::<Lv>("big_number") {
        return Value::BigNumber(n.to_string_lossy().to_string());
    }
    if let Ok(Lv::Table(inner)) = t.raw_get::<Lv>("map") {
        let mut pairs = Vec::new();
        for entry in inner.pairs::<Lv, Lv>().flatten() {
            pairs.push((lua_to_reply(&entry.0, resp), lua_to_reply(&entry.1, resp)));
        }
        return Value::Map(pairs);
    }
    if let Ok(Lv::Table(inner)) = t.raw_get::<Lv>("set") {
        let mut items = Vec::new();
        for entry in inner.pairs::<Lv, Lv>().flatten() {
            items.push(lua_to_reply(&entry.0, resp));
        }
        return Value::Set(items);
    }
    let mut items = Vec::new();
    for i in 1.. {
        match t.raw_get::<Lv>(i) {
            Ok(Lv::Nil) | Err(_) => break,
            Ok(v) => items.push(lua_to_reply(&v, resp)),
        }
    }
    Value::Array(items)
}

/// A Lua number as a command argument (`luaArgsToRedisArgv`): integers that
/// fit an int64 print as integers, the rest as the shortest string that reads
/// back exactly. Lua's own conversion (`%.14g`) would round large values such
/// as millisecond timestamps.
fn number_arg(n: f64) -> String {
    if let Some(i) = super::cmsgpack::double_to_i64(n) {
        return i.to_string();
    }
    super::double::d2string(n)
}

#[cfg(test)]
mod number_format_tests {
    use super::*;

    #[test]
    fn matches_redis_7_2() {
        // Expected strings are what a real Redis 7.2.5 stores.
        for (n, want) in [
            (1790482289286.0, "1790482289286"),
            (3.7, "3.7"),
            (0.1, "0.1"),
            (1e19, "1e+19"),
            (1e17, "100000000000000000"),
            (1.5e300, "1.5e+300"),
            (2.5e-7, "2.5e-7"),
            (1e-5, "0.00001"),
            (-0.0, "0"),
            (0.30000000000000004, "0.30000000000000004"),
            (12345678901234567890.0, "12345678901234567000"),
            (5e-324, "5e-324"),
        ] {
            assert_eq!(number_arg(n), want, "{n:e}");
        }
    }
}
