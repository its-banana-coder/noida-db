//! Redis Functions: FUNCTION LOAD/LIST/DELETE/FLUSH/DUMP/RESTORE/STATS/
//! KILL/HELP, FCALL and FCALL_RO (Redis 7's function libraries).
//!
//! A library is Lua code whose first line is `#!lua name=<lib>` and which
//! registers functions with `redis.register_function`. Each FCALL runs the
//! library code again in a fresh interpreter to get the function (noida-db
//! keeps no interpreter alive between commands) and then calls it with the
//! keys and arguments, in the same sandbox EVAL uses.

use super::engine::{Command, Ctx, Reply, cmd, container, eq_ic, help_reply};
use super::resp::Value;
use super::{glob, rdb};

pub static COMMANDS: &[Command] = &[
    cmd("fcall", fcall),
    cmd("fcall_ro", fcall_ro),
    container("function", function_help, FUNCTION),
];

static FUNCTION: &[Command] = &[
    cmd("load", function_load),
    cmd("delete", function_delete),
    cmd("flush", function_flush),
    cmd("list", function_list),
    cmd("stats", function_stats),
    cmd("dump", function_dump),
    cmd("restore", function_restore),
    cmd("kill", function_kill),
    cmd("help", function_help),
];

/// One loaded library.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Library {
    pub name: String,
    pub code: Vec<u8>,
    pub functions: Vec<FunctionMeta>,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct FunctionMeta {
    pub name: String,
    pub description: Option<String>,
    pub flags: Vec<String>,
}

/// Flags `redis.register_function` accepts.
pub const FLAGS: &[&str] =
    &["no-writes", "allow-oom", "allow-stale", "no-cluster", "allow-cross-slot-keys"];

fn valid_name(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

/// Parses `#!<engine> name=<lib> ...` and checks the engine.
fn metadata(code: &[u8]) -> Result<String, Value> {
    let text = String::from_utf8_lossy(code);
    let first = text.lines().next().unwrap_or("");
    let Some(rest) = first.strip_prefix("#!") else {
        return Err(Value::err("ERR Missing library metadata"));
    };
    let mut parts = rest.split_whitespace();
    let engine = parts.next().unwrap_or("");
    if !engine.eq_ignore_ascii_case("lua") {
        return Err(Value::err(format!("ERR Engine '{engine}' not found")));
    }
    let mut name = None;
    for p in parts {
        match p.split_once('=') {
            Some(("name", v)) => name = Some(v.to_string()),
            _ => return Err(Value::err(format!("ERR Invalid metadata value given: {p}"))),
        }
    }
    let name = name.ok_or_else(|| Value::err("ERR Library name was not given"))?;
    if !valid_name(&name) {
        return Err(Value::err(
            "ERR Library names can only contain letters, numbers, or underscores(_) and must be at least one character long",
        ));
    }
    Ok(name)
}

/// Compiles and runs a library's code to learn what it registers.
fn register(code: &[u8], name: &str) -> Result<Library, Value> {
    let functions = super::scripting::library_functions(code)?;
    if functions.is_empty() {
        return Err(Value::err("ERR No functions registered"));
    }
    Ok(Library { name: name.to_string(), code: code.to_vec(), functions })
}

/// Adds `lib` to `libs`: with `replace`, over a library of the same name.
fn install(libs: &mut Vec<Library>, lib: Library, replace: bool) -> Result<(), Value> {
    if libs.iter().any(|l| l.name == lib.name) && !replace {
        return Err(Value::err(format!("ERR Library '{}' already exists", lib.name)));
    }
    for f in &lib.functions {
        if libs
            .iter()
            .filter(|l| l.name != lib.name)
            .any(|l| l.functions.iter().any(|g| g.name == f.name))
        {
            return Err(Value::err(format!("ERR Function {} already exists", f.name)));
        }
    }
    match libs.iter().position(|l| l.name == lib.name) {
        Some(i) => libs[i] = lib,
        None => libs.push(lib),
    }
    Ok(())
}

fn function_load(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    // FUNCTION LOAD [REPLACE] code: every argument before the code must be
    // an option.
    let code = &a[a.len() - 1];
    let mut replace = false;
    for opt in &a[2..a.len() - 1] {
        if eq_ic(opt, "replace") {
            replace = true;
        } else {
            return Err(Value::err(format!(
                "ERR Unknown option given: {}",
                String::from_utf8_lossy(opt)
            )));
        }
    }
    let name = metadata(code)?;
    let lib = register(code, &name)?;
    install(&mut ctx.engine.scripts.libraries, lib, replace)?;
    Ok(Value::bulk(name))
}

fn function_delete(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    if a.len() != 3 {
        return Err(super::engine::syntax());
    }
    let name = String::from_utf8_lossy(&a[2]);
    let libs = &mut ctx.engine.scripts.libraries;
    match libs.iter().position(|l| l.name == name) {
        Some(i) => {
            libs.remove(i);
            Ok(Value::ok())
        }
        None => Err(Value::err("ERR Library not found")),
    }
}

fn function_flush(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    if a.len() > 3 || (a.len() == 3 && !eq_ic(&a[2], "sync") && !eq_ic(&a[2], "async")) {
        return Err(Value::err("ERR FUNCTION FLUSH only supports SYNC|ASYNC option"));
    }
    ctx.engine.scripts.libraries.clear();
    Ok(Value::ok())
}

fn function_list(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let mut pattern: Option<&[u8]> = None;
    let mut with_code = false;
    let mut i = 2;
    while i < a.len() {
        if eq_ic(&a[i], "withcode") && !with_code {
            with_code = true;
        } else if eq_ic(&a[i], "libraryname") && pattern.is_none() {
            let Some(p) = a.get(i + 1) else {
                return Err(Value::err("ERR library name argument was not given"));
            };
            pattern = Some(p);
            i += 1;
        } else {
            return Err(Value::err(format!(
                "ERR Unknown argument {}",
                String::from_utf8_lossy(&a[i])
            )));
        }
        i += 1;
    }
    let s = |t: &str| Value::bulk(t);
    let out = ctx
        .engine
        .scripts
        .libraries
        .iter()
        .filter(|l| pattern.is_none_or(|p| glob::matches(p, l.name.as_bytes(), false)))
        .map(|l| {
            let funcs = l
                .functions
                .iter()
                .map(|f| {
                    Value::Map(vec![
                        (s("name"), s(&f.name)),
                        (s("description"), f.description.as_deref().map_or(Value::Null, s)),
                        (
                            s("flags"),
                            Value::Set(f.flags.iter().map(|x| Value::Simple(x.clone())).collect()),
                        ),
                    ])
                })
                .collect();
            let mut m = vec![
                (s("library_name"), s(&l.name)),
                (s("engine"), s("LUA")),
                (s("functions"), Value::Array(funcs)),
            ];
            if with_code {
                m.push((s("library_code"), Value::bulk(&l.code)));
            }
            Value::Map(m)
        })
        .collect();
    Ok(Value::Array(out))
}

fn function_stats(ctx: &mut Ctx, _: &[Vec<u8>]) -> Reply {
    let libs = &ctx.engine.scripts.libraries;
    let s = |t: &str| Value::bulk(t);
    Ok(Value::Map(vec![
        (s("running_script"), Value::Null),
        (
            s("engines"),
            Value::Map(vec![(
                s("LUA"),
                Value::Map(vec![
                    (s("libraries_count"), Value::Integer(libs.len() as i64)),
                    (
                        s("functions_count"),
                        Value::Integer(libs.iter().map(|l| l.functions.len() as i64).sum()),
                    ),
                ]),
            )]),
        ),
    ]))
}

/// Libraries in the RDB format Redis's FUNCTION DUMP uses.
fn function_dump(ctx: &mut Ctx, _: &[Vec<u8>]) -> Reply {
    let mut out = vec![];
    for l in &ctx.engine.scripts.libraries {
        out.push(rdb::OPCODE_FUNCTION2);
        rdb::write_string(&mut out, &l.code);
    }
    Ok(Value::Bulk(rdb::seal(out)))
}

fn function_restore(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let policy = match a.get(3) {
        None => "append",
        Some(p) if eq_ic(p, "append") => "append",
        Some(p) if eq_ic(p, "replace") => "replace",
        Some(p) if eq_ic(p, "flush") => "flush",
        Some(_) => {
            return Err(Value::err(
                "ERR Wrong restore policy given, value should be either FLUSH, APPEND or REPLACE.",
            ));
        }
    };
    if a.len() > 4 {
        return Err(super::engine::syntax());
    }
    let Some(body) = rdb::unseal(&a[2]) else {
        return Err(Value::err("ERR DUMP payload version or checksum are wrong"));
    };
    let mut r = rdb::Reader::new(body);
    let mut loaded = vec![];
    while !r.at_end() {
        if r.byte() != Some(rdb::OPCODE_FUNCTION2) {
            return Err(Value::err("ERR given type is not a function"));
        }
        let code = r.string().ok_or_else(|| Value::err("ERR Failed loading library"))?;
        let name = metadata(&code)?;
        loaded.push(register(&code, &name)?);
    }
    let mut libs = if policy == "flush" { vec![] } else { ctx.engine.scripts.libraries.clone() };
    for lib in loaded {
        if policy == "append" && libs.iter().any(|l| l.name == lib.name) {
            return Err(Value::err(format!("ERR Library {} already exists", lib.name)));
        }
        install(&mut libs, lib, true)?;
    }
    ctx.engine.scripts.libraries = libs;
    Ok(Value::ok())
}

fn function_kill(_: &mut Ctx, _: &[Vec<u8>]) -> Reply {
    Err(Value::err("NOTBUSY No scripts in execution right now."))
}

fn function_help(_: &mut Ctx, _: &[Vec<u8>]) -> Reply {
    Ok(help_reply(
        "function",
        &[
            "LOAD [REPLACE] <FUNCTION CODE>",
            "    Create a new library with the given library name and code.",
            "DELETE <LIBRARY NAME>",
            "    Delete the given library.",
            "LIST [LIBRARYNAME PATTERN] [WITHCODE]",
            "    Return general information on all the libraries:",
            "    * Library name",
            "    * The engine used to run the Library",
            "    * Library description",
            "    * Functions list",
            "    * Library code (if WITHCODE is given)",
            "    It also possible to get only function that matches a pattern using LIBRARYNAME argument.",
            "STATS",
            "    Return information about the current function running:",
            "    * Function name",
            "    * Command used to run the function",
            "    * Duration in MS that the function is running",
            "    If no function is running, return nil",
            "    In addition, returns a list of available engines.",
            "KILL",
            "    Kill the current running function.",
            "FLUSH [ASYNC|SYNC]",
            "    Delete all the libraries.",
            "    When called without the optional mode argument, the behavior is determined by the",
            "    lazyfree-lazy-user-flush configuration directive. Valid modes are:",
            "    * ASYNC: Asynchronously flush the libraries.",
            "    * SYNC: Synchronously flush the libraries.",
            "DUMP",
            "    Return a serialized payload representing the current libraries, can be restored using FUNCTION RESTORE command",
            "RESTORE <PAYLOAD> [FLUSH|APPEND|REPLACE]",
            "    Restore the libraries represented by the given payload, it is possible to give a restore policy to",
            "    control how to handle existing libraries (default APPEND):",
            "    * FLUSH: delete all existing libraries.",
            "    * APPEND: appends the restored libraries to the existing libraries. On collision, abort.",
            "    * REPLACE: appends the restored libraries to the existing libraries, On collision, replace the old",
            "      libraries with the new libraries (notice that even on this option there is a chance of failure",
            "      in case of functions name collision with another library).",
        ],
    ))
}

fn fcall(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    fcall_generic(ctx, a, false)
}

fn fcall_ro(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    fcall_generic(ctx, a, true)
}

fn fcall_generic(ctx: &mut Ctx, a: &[Vec<u8>], ro: bool) -> Reply {
    let numkeys = super::engine::int_arg(&a[2])?;
    if numkeys > a.len() as i64 - 3 {
        return Err(Value::err("ERR Number of keys can't be greater than number of args"));
    }
    if numkeys < 0 {
        return Err(Value::err("ERR Number of keys can't be negative"));
    }
    let name = String::from_utf8_lossy(&a[1]).to_string();
    let found = ctx.engine.scripts.libraries.iter().find_map(|l| {
        l.functions.iter().find(|f| f.name == name).map(|f| (l.code.clone(), f.flags.clone()))
    });
    let Some((code, flags)) = found else {
        return Err(Value::err("ERR Function not found"));
    };
    let no_writes = flags.iter().any(|f| f == "no-writes");
    if ro && !no_writes {
        return Err(Value::err("ERR Can not execute a script with write flag using *_ro command."));
    }
    let numkeys = numkeys as usize;
    let keys = &a[3..3 + numkeys];
    let args = &a[3 + numkeys..];
    super::scripting::run_function(ctx, &code, &name, keys, args, ro || no_writes)
}
