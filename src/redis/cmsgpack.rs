//! The `cmsgpack` library Redis exposes to scripts (`cmsgpack.pack`,
//! `unpack`, `unpack_one`, `unpack_limit`). BullMQ and other job queues use it
//! to store job options.
//!
//! Ported from lua-cmsgpack 0.4.0 (`deps/lua/src/lua_cmsgpack.c` in Redis 7.2,
//! BSD-2-Clause, Copyright (C) 2012 Salvatore Sanfilippo); see
//! `THIRD_PARTY.md`. The encoding choices (float vs double, array vs map,
//! nesting limit, error texts) follow the C code.

use mlua::{Lua, MultiValue, Table, Value as Lv, Variadic};

/// `LUACMSGPACK_MAX_NESTING`: tables nested deeper than this encode as nil.
const MAX_NESTING: usize = 16;

pub fn table(lua: &Lua) -> mlua::Result<Table> {
    let t = lua.create_table()?;
    t.raw_set("pack", lua.create_function(|lua, args: Variadic<Lv>| pack(lua, &args))?)?;
    t.raw_set(
        "unpack",
        lua.create_function(|lua, args: Variadic<Lv>| {
            let s = string_arg(lua, &args, 0, "unpack")?;
            unpack_full(lua, &s, 0, 0)
        })?,
    )?;
    t.raw_set(
        "unpack_one",
        lua.create_function(|lua, args: Variadic<Lv>| {
            let s = string_arg(lua, &args, 0, "unpack_one")?;
            let offset = opt_int(lua, &args, 1, "unpack_one", 0)?;
            unpack_full(lua, &s, 1, offset)
        })?,
    )?;
    t.raw_set(
        "unpack_limit",
        lua.create_function(|lua, args: Variadic<Lv>| {
            let s = string_arg(lua, &args, 0, "unpack_limit")?;
            let limit = int_arg(lua, &args, 1, "unpack_limit")?;
            let offset = opt_int(lua, &args, 2, "unpack_limit", 0)?;
            unpack_full(lua, &s, limit, offset)
        })?,
    )?;
    t.raw_set("_NAME", "cmsgpack")?;
    t.raw_set("_VERSION", "lua-cmsgpack 0.4.0")?;
    t.raw_set("_COPYRIGHT", "Copyright (C) 2012, Salvatore Sanfilippo")?;
    t.raw_set("_DESCRIPTION", "MessagePack C implementation for Lua")?;
    Ok(t)
}

fn arg_error(n: usize, func: &str, msg: &str) -> mlua::Error {
    mlua::Error::RuntimeError(format!("bad argument #{n} to '{func}' ({msg})"))
}

/// `luaL_checklstring`: a string, or a number converted to one.
fn string_arg(lua: &Lua, args: &[Lv], i: usize, func: &str) -> mlua::Result<Vec<u8>> {
    let v = args.get(i).cloned().unwrap_or(Lv::Nil);
    match &v {
        Lv::String(s) => Ok(s.as_bytes().to_vec()),
        Lv::Integer(_) | Lv::Number(_) => {
            Ok(lua.coerce_string(v.clone())?.map(|s| s.as_bytes().to_vec()).unwrap_or_default())
        }
        other => Err(arg_error(
            i + 1,
            func,
            &format!(
                "string expected, got {}",
                if args.len() <= i { "no value" } else { other.type_name() }
            ),
        )),
    }
}

/// `luaL_checkinteger`.
fn int_arg(lua: &Lua, args: &[Lv], i: usize, func: &str) -> mlua::Result<i64> {
    match args.get(i) {
        Some(Lv::Integer(n)) => Ok(*n),
        Some(Lv::Number(n)) => Ok(*n as i64),
        Some(Lv::String(s)) => s
            .to_str()
            .ok()
            .and_then(|s| s.trim().parse::<f64>().ok())
            .map(|n| n as i64)
            .ok_or_else(|| arg_error(i + 1, func, "number expected, got string")),
        Some(other) => {
            Err(arg_error(i + 1, func, &format!("number expected, got {}", other.type_name())))
        }
        None => {
            let _ = lua;
            Err(arg_error(i + 1, func, "number expected, got no value"))
        }
    }
}

/// `luaL_optinteger`.
fn opt_int(lua: &Lua, args: &[Lv], i: usize, func: &str, default: i64) -> mlua::Result<i64> {
    match args.get(i) {
        None | Some(Lv::Nil) => Ok(default),
        _ => int_arg(lua, args, i, func),
    }
}

// ---- encoding ----

fn pack(lua: &Lua, args: &[Lv]) -> mlua::Result<Lv> {
    if args.is_empty() {
        return Err(arg_error(0, "pack", "MessagePack pack needs input."));
    }
    let mut buf = Vec::new();
    for v in args {
        encode(&mut buf, v, 0)?;
    }
    Ok(Lv::String(lua.create_string(&buf)?))
}

fn encode(buf: &mut Vec<u8>, v: &Lv, level: usize) -> mlua::Result<()> {
    match v {
        Lv::String(s) => encode_bytes(buf, &s.as_bytes()),
        Lv::Boolean(b) => buf.push(if *b { 0xc3 } else { 0xc2 }),
        Lv::Integer(n) => encode_number(buf, *n as f64),
        Lv::Number(n) => encode_number(buf, *n),
        Lv::Table(t) if level < MAX_NESTING => encode_table(buf, t, level)?,
        // Anything else, and tables nested too deep, become nil.
        _ => buf.push(0xc0),
    }
    Ok(())
}

fn encode_bytes(buf: &mut Vec<u8>, s: &[u8]) {
    let len = s.len();
    if len < 32 {
        buf.push(0xa0 | len as u8);
    } else if len <= 0xff {
        buf.extend([0xd9, len as u8]);
    } else if len <= 0xffff {
        buf.push(0xda);
        buf.extend((len as u16).to_be_bytes());
    } else {
        buf.push(0xdb);
        buf.extend((len as u32).to_be_bytes());
    }
    buf.extend(s);
}

/// Lua 5.1 numbers are doubles: integers that fit an int64 are written as
/// integers, everything else as a float when that is exact, else a double.
fn encode_number(buf: &mut Vec<u8>, n: f64) {
    if let Some(i) = double_to_i64(n) {
        encode_int(buf, i);
    } else {
        let f = n as f32;
        if n == f as f64 {
            buf.push(0xca);
            buf.extend(f.to_be_bytes());
        } else {
            buf.push(0xcb);
            buf.extend(n.to_be_bytes());
        }
    }
}

/// `double2ll`-style check (`(int64_t)n == n` in C): the integer a double
/// stands for, if it has an exact int64 value.
pub fn double_to_i64(n: f64) -> Option<i64> {
    // The range test keeps 2^63 out, which saturates in Rust and overflows in C.
    ((-9223372036854775808.0..9223372036854775808.0).contains(&n) && (n as i64) as f64 == n)
        .then_some(n as i64)
}

fn encode_int(buf: &mut Vec<u8>, n: i64) {
    if n >= 0 {
        if n <= 127 {
            buf.push(n as u8);
        } else if n <= 0xff {
            buf.extend([0xcc, n as u8]);
        } else if n <= 0xffff {
            buf.push(0xcd);
            buf.extend((n as u16).to_be_bytes());
        } else if n <= 0xffff_ffff {
            buf.push(0xce);
            buf.extend((n as u32).to_be_bytes());
        } else {
            buf.push(0xcf);
            buf.extend((n as u64).to_be_bytes());
        }
    } else if n >= -32 {
        buf.push(n as i8 as u8);
    } else if n >= -128 {
        buf.extend([0xd0, n as i8 as u8]);
    } else if n >= -32768 {
        buf.push(0xd1);
        buf.extend((n as i16).to_be_bytes());
    } else if n >= -2147483648 {
        buf.push(0xd2);
        buf.extend((n as i32).to_be_bytes());
    } else {
        buf.push(0xd3);
        buf.extend(n.to_be_bytes());
    }
}

fn encode_len(buf: &mut Vec<u8>, n: usize, fix: u8, short: u8, long: u8) {
    if n <= 15 {
        buf.push(fix | n as u8);
    } else if n <= 65535 {
        buf.push(short);
        buf.extend((n as u16).to_be_bytes());
    } else {
        buf.push(long);
        buf.extend((n as u32).to_be_bytes());
    }
}

fn encode_table(buf: &mut Vec<u8>, t: &Table, level: usize) -> mlua::Result<()> {
    if is_array(t)? {
        let len = t.raw_len();
        encode_len(buf, len, 0x90, 0xdc, 0xdd);
        for i in 1..=len {
            let v: Lv = t.raw_get(i)?;
            encode(buf, &v, level + 1)?;
        }
    } else {
        let mut pairs = Vec::new();
        for entry in t.clone().pairs::<Lv, Lv>() {
            pairs.push(entry?);
        }
        encode_len(buf, pairs.len(), 0x80, 0xde, 0xdf);
        for (k, v) in pairs {
            encode(buf, &k, level + 1)?;
            encode(buf, &v, level + 1)?;
        }
    }
    Ok(())
}

/// `table_is_an_array`: only positive integer keys, with no holes.
fn is_array(t: &Table) -> mlua::Result<bool> {
    let (mut count, mut max) = (0i64, 0i64);
    for entry in t.clone().pairs::<Lv, Lv>() {
        let (k, _) = entry?;
        let n = match k {
            Lv::Integer(n) => n as f64,
            Lv::Number(n) => n,
            _ => return Ok(false),
        };
        // Same as the C `<= 0` and int-equivalence checks.
        if n <= 0.0 || n > i32::MAX as f64 || (n as i32) as f64 != n {
            return Ok(false);
        }
        max = max.max(n as i64);
        count += 1;
    }
    Ok(max == count)
}

// ---- decoding ----

enum Fail {
    Eof,
    BadFormat,
    Lua(mlua::Error),
}

impl From<mlua::Error> for Fail {
    fn from(e: mlua::Error) -> Self {
        Fail::Lua(e)
    }
}

struct Cursor<'a> {
    s: &'a [u8],
    at: usize,
}

impl Cursor<'_> {
    fn left(&self) -> usize {
        self.s.len() - self.at
    }
    fn need(&self, n: usize) -> Result<(), Fail> {
        if self.left() < n { Err(Fail::Eof) } else { Ok(()) }
    }
    fn take(&mut self, n: usize) -> Result<&[u8], Fail> {
        self.need(n)?;
        let out = &self.s[self.at..self.at + n];
        self.at += n;
        Ok(out)
    }
}

fn be(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0, |a, b| (a << 8) | *b as u64)
}

fn decode(lua: &Lua, c: &mut Cursor) -> Result<Lv, Fail> {
    c.need(1)?;
    let tag = c.s[c.at];
    c.at += 1;
    let num = |n: f64| Lv::Number(n);
    Ok(match tag {
        0xcc => num(be(c.take(1)?) as f64),
        0xd0 => num(c.take(1)?[0] as i8 as f64),
        0xcd => num(be(c.take(2)?) as f64),
        0xd1 => num(be(c.take(2)?) as u16 as i16 as f64),
        0xce => num(be(c.take(4)?) as f64),
        0xd2 => num(be(c.take(4)?) as u32 as i32 as f64),
        0xcf => num(be(c.take(8)?) as f64),
        0xd3 => num(be(c.take(8)?) as i64 as f64),
        0xc0 => Lv::Nil,
        0xc3 => Lv::Boolean(true),
        0xc2 => Lv::Boolean(false),
        0xca => num(f32::from_be_bytes(c.take(4)?.try_into().unwrap()) as f64),
        0xcb => num(f64::from_be_bytes(c.take(8)?.try_into().unwrap())),
        0xd9..=0xdb => {
            let width = match tag {
                0xd9 => 1,
                0xda => 2,
                _ => 4,
            };
            let len = be(c.take(width)?) as usize;
            Lv::String(lua.create_string(c.take(len)?)?)
        }
        0xdc | 0xdd => {
            let len = be(c.take(if tag == 0xdc { 2 } else { 4 })?) as usize;
            decode_array(lua, c, len)?
        }
        0xde | 0xdf => {
            let len = be(c.take(if tag == 0xde { 2 } else { 4 })?) as usize;
            decode_map(lua, c, len)?
        }
        t if t & 0x80 == 0 => num(t as f64),
        t if t & 0xe0 == 0xe0 => num(t as i8 as f64),
        t if t & 0xe0 == 0xa0 => Lv::String(lua.create_string(c.take((t & 0x1f) as usize)?)?),
        t if t & 0xf0 == 0x90 => decode_array(lua, c, (t & 0xf) as usize)?,
        t if t & 0xf0 == 0x80 => decode_map(lua, c, (t & 0xf) as usize)?,
        _ => return Err(Fail::BadFormat),
    })
}

fn decode_array(lua: &Lua, c: &mut Cursor, len: usize) -> Result<Lv, Fail> {
    let t = lua.create_table()?;
    for i in 1..=len {
        t.raw_set(i, decode(lua, c)?)?;
    }
    Ok(Lv::Table(t))
}

fn decode_map(lua: &Lua, c: &mut Cursor, len: usize) -> Result<Lv, Fail> {
    let t = lua.create_table()?;
    for _ in 0..len {
        let k = decode(lua, c)?;
        let v = decode(lua, c)?;
        t.raw_set(k, v)?;
    }
    Ok(Lv::Table(t))
}

/// `mp_unpack_full`. With no limit and no offset every value is returned;
/// otherwise the next offset (-1 at the end) comes first.
fn unpack_full(lua: &Lua, s: &[u8], limit: i64, offset: i64) -> mlua::Result<MultiValue> {
    let decode_all = limit == 0 && offset == 0;
    let err = |m: String| mlua::Error::RuntimeError(m);
    if offset < 0 || limit < 0 {
        // Redis reports the input length where the limit belongs; so do we.
        return Err(err(format!(
            "Invalid request to unpack with offset of {} and limit of {}.",
            offset as i32,
            s.len() as i32
        )));
    } else if offset as usize > s.len() {
        return Err(err(format!(
            "Start offset {} greater than input length {}.",
            offset as i32,
            s.len() as i32
        )));
    }
    let limit = if decode_all { i32::MAX as i64 } else { limit };
    let mut c = Cursor { s, at: offset as usize };
    let mut values = Vec::new();
    let mut count = 0;
    while c.left() > 0 && count < limit {
        match decode(lua, &mut c) {
            Ok(v) => values.push(v),
            Err(Fail::Eof) => return Err(err("Missing bytes in input.".into())),
            Err(Fail::BadFormat) => return Err(err("Bad data format in input.".into())),
            Err(Fail::Lua(e)) => return Err(e),
        }
        count += 1;
    }
    if !decode_all {
        let next = if c.left() == 0 { -1 } else { c.at as i64 };
        values.insert(0, Lv::Number(next as f64));
    }
    Ok(MultiValue::from_iter(values))
}
