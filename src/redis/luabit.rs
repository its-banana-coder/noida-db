//! The `bit` (LuaBitOp) and `struct` libraries Redis bundles for scripts.
//!
//! `bit` follows LuaBitOp 1.0.2: every operation works on 32-bit integers
//! (a Lua number is first reduced modulo 2^32, rounding to nearest) and
//! returns a signed 32-bit result. `struct` follows Roberto Ierusalimschy's
//! struct.c 0.2 as Redis 7.2 ships it: `>`/`<`/`=` endianness, `!n`
//! alignment, `b B h H l L T i I[n] c[n] s f d x`. Both re-implemented from
//! their documented behaviour.

use mlua::{Lua, MultiValue, Table, Value as Lv, Variadic};

/// LuaBitOp's number -> 32-bit conversion: round to nearest, wrap mod 2^32.
fn tobit(n: f64) -> i32 {
    let r = n.round_ties_even();
    if !r.is_finite() {
        return 0;
    }
    (r.rem_euclid(4294967296.0) as u64 as u32) as i32
}

fn num_arg(args: &[Lv], i: usize, fname: &str) -> mlua::Result<f64> {
    match args.get(i) {
        Some(Lv::Integer(n)) => Ok(*n as f64),
        Some(Lv::Number(n)) => Ok(*n),
        Some(Lv::String(s)) => s
            .to_str()
            .ok()
            .and_then(|s| s.trim().parse::<f64>().ok())
            .ok_or_else(|| bad_arg(i, fname, "number expected, got string")),
        Some(other) => {
            Err(bad_arg(i, fname, &format!("number expected, got {}", other.type_name())))
        }
        None => Err(bad_arg(i, fname, "number expected, got no value")),
    }
}

fn bad_arg(i: usize, fname: &str, msg: &str) -> mlua::Error {
    mlua::Error::RuntimeError(format!("bad argument #{} to '{fname}' ({msg})", i + 1))
}

fn ret(v: i32) -> Lv {
    Lv::Number(v as f64)
}

pub fn bit_table(lua: &Lua) -> mlua::Result<Table> {
    let t = lua.create_table()?;
    t.raw_set(
        "tobit",
        lua.create_function(|_, a: Variadic<Lv>| Ok(ret(tobit(num_arg(&a, 0, "tobit")?))))?,
    )?;
    t.raw_set(
        "bnot",
        lua.create_function(|_, a: Variadic<Lv>| Ok(ret(!tobit(num_arg(&a, 0, "bnot")?))))?,
    )?;
    t.raw_set(
        "bswap",
        lua.create_function(|_, a: Variadic<Lv>| {
            Ok(ret(tobit(num_arg(&a, 0, "bswap")?).swap_bytes()))
        })?,
    )?;
    for (name, op) in [
        ("band", (|x: i32, y: i32| x & y) as fn(i32, i32) -> i32),
        ("bor", |x, y| x | y),
        ("bxor", |x, y| x ^ y),
    ] {
        t.raw_set(
            name,
            lua.create_function(move |_, a: Variadic<Lv>| {
                let mut acc = tobit(num_arg(&a, 0, name)?);
                for i in 1..a.len() {
                    acc = op(acc, tobit(num_arg(&a, i, name)?));
                }
                Ok(ret(acc))
            })?,
        )?;
    }
    for (name, op) in [
        ("lshift", (|x: i32, n: u32| ((x as u32) << n) as i32) as fn(i32, u32) -> i32),
        ("rshift", |x, n| ((x as u32) >> n) as i32),
        ("arshift", |x, n| x >> n),
        ("rol", |x, n| (x as u32).rotate_left(n) as i32),
        ("ror", |x, n| (x as u32).rotate_right(n) as i32),
    ] {
        t.raw_set(
            name,
            lua.create_function(move |_, a: Variadic<Lv>| {
                let x = tobit(num_arg(&a, 0, name)?);
                let n = (tobit(num_arg(&a, 1, name)?) as u32) & 31;
                Ok(ret(op(x, n)))
            })?,
        )?;
    }
    t.raw_set(
        "tohex",
        lua.create_function(|lua, a: Variadic<Lv>| {
            let x = tobit(num_arg(&a, 0, "tohex")?) as u32;
            let mut n = if a.len() > 1 { tobit(num_arg(&a, 1, "tohex")?) } else { 8 };
            let upper = n < 0;
            if upper {
                n = -n;
            }
            let n = n.clamp(1, 8) as usize;
            let digits = if upper { format!("{x:08X}") } else { format!("{x:08x}") };
            lua.create_string(&digits[8 - n..])
        })?,
    )?;
    Ok(t)
}

// ---------------------------------------------------------------------------
// struct

#[derive(Clone, Copy)]
struct Header {
    little: bool,
    align: usize,
}

const MAXINTSIZE: usize = 32;
const MAXALIGN: usize = 8;

fn struct_err(msg: impl Into<String>) -> mlua::Error {
    mlua::Error::RuntimeError(msg.into())
}

/// Reads an optional size after a format char (`i4`, `c10`).
fn read_num(fmt: &[u8], i: &mut usize, default: usize) -> usize {
    if *i >= fmt.len() || !fmt[*i].is_ascii_digit() {
        return default;
    }
    let mut n = 0usize;
    while *i < fmt.len() && fmt[*i].is_ascii_digit() {
        n = n.saturating_mul(10).saturating_add((fmt[*i] - b'0') as usize);
        *i += 1;
    }
    n
}

/// The size of option `opt` (with its size suffix consumed from `fmt`).
fn opt_size(opt: u8, fmt: &[u8], i: &mut usize) -> mlua::Result<usize> {
    Ok(match opt {
        b'B' | b'b' | b'x' => 1,
        b'H' | b'h' => 2,
        b'L' | b'l' | b'T' => 8,
        b'f' => 4,
        b'd' => 8,
        b'c' => read_num(fmt, i, 1),
        b'i' | b'I' => {
            let sz = read_num(fmt, i, 4);
            if sz > MAXINTSIZE {
                return Err(struct_err(format!(
                    "integral size {sz} is larger than limit of {MAXINTSIZE}"
                )));
            }
            sz
        }
        _ => 0,
    })
}

/// Padding needed before an item of `size` at offset `len`.
fn gettoalign(len: usize, h: Header, opt: u8, size: usize) -> usize {
    if size == 0 || opt == b'c' {
        return 0;
    }
    let size = size.min(h.align);
    if size <= 1 {
        return 0;
    }
    (size - (len & (size - 1))) & (size - 1)
}

/// Handles the non-item options; returns true if `opt` was one.
fn control(opt: u8, fmt: &[u8], i: &mut usize, h: &mut Header) -> mlua::Result<bool> {
    match opt {
        b' ' => {}
        b'>' => h.little = false,
        b'<' | b'=' => h.little = true,
        b'!' => {
            let a = read_num(fmt, i, MAXALIGN);
            if !a.is_power_of_two() {
                return Err(struct_err(format!("alignment {a} is not a power of 2")));
            }
            h.align = a;
        }
        _ => return Ok(false),
    }
    Ok(true)
}

fn put_int(out: &mut Vec<u8>, n: f64, little: bool, size: usize) {
    let v: u64 = if n < 0.0 { (n as i64) as u64 } else { n as u64 };
    let mut bytes = vec![0u8; size];
    for (k, b) in bytes.iter_mut().enumerate() {
        *b = if k < 8 {
            (v >> (8 * k)) as u8
        } else if (v as i64) < 0 {
            0xff
        } else {
            0
        };
    }
    if !little {
        bytes.reverse();
    }
    out.extend(bytes);
}

fn get_int(data: &[u8], little: bool, signed: bool, size: usize) -> f64 {
    let mut v: u64 = 0;
    for k in 0..size.min(8) {
        let b = if little { data[k] } else { data[size - 1 - k] };
        v |= (b as u64) << (8 * k);
    }
    if signed && size < 8 {
        let shift = 64 - 8 * size;
        return ((v << shift) as i64 >> shift) as f64;
    }
    if signed { v as i64 as f64 } else { v as f64 }
}

fn bytes_of(v: &Lv) -> Option<Vec<u8>> {
    match v {
        Lv::String(s) => Some(s.as_bytes().to_vec()),
        Lv::Integer(n) => Some(n.to_string().into_bytes()),
        // Lua's own number-to-string conversion (`%.14g`), for the common
        // cases: integral values print without a fraction.
        Lv::Number(n) if n.fract() == 0.0 && n.abs() < 1e15 => {
            Some(format!("{}", *n as i64).into_bytes())
        }
        Lv::Number(n) => Some(format!("{n}").into_bytes()),
        _ => None,
    }
}

fn struct_pack(lua: &Lua, a: &[Lv]) -> mlua::Result<mlua::LuaString> {
    let fmt = match a.first().and_then(bytes_of) {
        Some(f) => f,
        None => return Err(bad_arg(0, "pack", "string expected, got no value")),
    };
    let mut h = Header { little: true, align: 1 };
    let mut out: Vec<u8> = vec![];
    let mut arg = 1;
    let mut i = 0;
    while i < fmt.len() {
        let opt = fmt[i];
        i += 1;
        if control(opt, &fmt, &mut i, &mut h)? {
            continue;
        }
        let size = opt_size(opt, &fmt, &mut i)?;
        let pad = gettoalign(out.len(), h, opt, size);
        out.extend(std::iter::repeat_n(0u8, pad));
        match opt {
            b'b' | b'B' | b'h' | b'H' | b'l' | b'L' | b'T' | b'i' | b'I' => {
                let n = num_arg(a, arg, "pack")?;
                arg += 1;
                put_int(&mut out, n, h.little, size);
            }
            b'x' => out.push(0),
            b'f' => {
                let n = num_arg(a, arg, "pack")? as f32;
                arg += 1;
                let b = if h.little { n.to_le_bytes() } else { n.to_be_bytes() };
                out.extend(b);
            }
            b'd' => {
                let n = num_arg(a, arg, "pack")?;
                arg += 1;
                let b = if h.little { n.to_le_bytes() } else { n.to_be_bytes() };
                out.extend(b);
            }
            b'c' | b's' => {
                let s = a
                    .get(arg)
                    .and_then(bytes_of)
                    .ok_or_else(|| bad_arg(arg, "pack", "string expected, got no value"))?;
                arg += 1;
                let mut l = s.len();
                if opt == b'c' {
                    // `c0` packs the whole string; `cN` needs at least N bytes.
                    let n = if fmt[i - 1].is_ascii_digit() { size } else { 1 };
                    if n != 0 {
                        if l < n {
                            return Err(bad_arg(arg - 1, "pack", "string too short"));
                        }
                        l = n;
                    }
                }
                out.extend_from_slice(&s[..l]);
                if opt == b's' {
                    out.push(0);
                }
            }
            other => {
                return Err(bad_arg(
                    0,
                    "pack",
                    &format!("invalid format option '{}'", other as char),
                ));
            }
        }
    }
    lua.create_string(&out)
}

fn struct_unpack(lua: &Lua, a: &[Lv]) -> mlua::Result<MultiValue> {
    let fmt = a
        .first()
        .and_then(bytes_of)
        .ok_or_else(|| bad_arg(0, "unpack", "string expected, got no value"))?;
    let data = a
        .get(1)
        .and_then(bytes_of)
        .ok_or_else(|| bad_arg(1, "unpack", "string expected, got no value"))?;
    let mut pos = match a.get(2) {
        Some(Lv::Nil) | None => 0,
        Some(_) => {
            let p = num_arg(a, 2, "unpack")? as i64;
            if p < 1 {
                return Err(bad_arg(2, "unpack", "offset must be 1 or greater"));
            }
            (p - 1) as usize
        }
    };
    let mut h = Header { little: true, align: 1 };
    let mut out: Vec<Lv> = vec![];
    let mut i = 0;
    let too_short = || bad_arg(1, "unpack", "data string too short");
    while i < fmt.len() {
        let opt = fmt[i];
        i += 1;
        if control(opt, &fmt, &mut i, &mut h)? {
            continue;
        }
        let size = opt_size(opt, &fmt, &mut i)?;
        pos += gettoalign(pos, h, opt, size);
        if opt != b's' && opt != b'c' && pos + size > data.len() {
            return Err(too_short());
        }
        match opt {
            b'b' | b'h' | b'l' | b'i' | b'B' | b'H' | b'L' | b'T' | b'I' => {
                let signed = opt.is_ascii_lowercase();
                out.push(Lv::Number(get_int(&data[pos..], h.little, signed, size)));
                pos += size;
            }
            b'x' => pos += 1,
            b'f' => {
                let b: [u8; 4] = data[pos..pos + 4].try_into().unwrap();
                let f = if h.little { f32::from_le_bytes(b) } else { f32::from_be_bytes(b) };
                out.push(Lv::Number(f as f64));
                pos += 4;
            }
            b'd' => {
                let b: [u8; 8] = data[pos..pos + 8].try_into().unwrap();
                let f = if h.little { f64::from_le_bytes(b) } else { f64::from_be_bytes(b) };
                out.push(Lv::Number(f));
                pos += 8;
            }
            b'c' => {
                // `c0` takes its length from the previous value.
                let mut n = if fmt[i - 1].is_ascii_digit() { size } else { 1 };
                if n == 0 {
                    let prev = match out.pop() {
                        Some(Lv::Number(x)) => x as usize,
                        _ => return Err(struct_err("format 'c0' needs a previous size")),
                    };
                    n = prev;
                }
                if pos + n > data.len() {
                    return Err(too_short());
                }
                out.push(Lv::String(lua.create_string(&data[pos..pos + n])?));
                pos += n;
            }
            b's' => {
                let end = data[pos..]
                    .iter()
                    .position(|&b| b == 0)
                    .ok_or_else(|| struct_err("unfinished string in data"))?;
                out.push(Lv::String(lua.create_string(&data[pos..pos + end])?));
                pos += end + 1;
            }
            other => {
                return Err(bad_arg(
                    0,
                    "unpack",
                    &format!("invalid format option '{}'", other as char),
                ));
            }
        }
    }
    out.push(Lv::Number((pos + 1) as f64));
    Ok(MultiValue::from_iter(out))
}

fn struct_size(a: &[Lv]) -> mlua::Result<Lv> {
    let fmt = a
        .first()
        .and_then(bytes_of)
        .ok_or_else(|| bad_arg(0, "size", "string expected, got no value"))?;
    let mut h = Header { little: true, align: 1 };
    let mut pos = 0usize;
    let mut i = 0;
    while i < fmt.len() {
        let opt = fmt[i];
        i += 1;
        if control(opt, &fmt, &mut i, &mut h)? {
            continue;
        }
        let size = opt_size(opt, &fmt, &mut i)?;
        pos += gettoalign(pos, h, opt, size);
        if opt == b's' {
            return Err(bad_arg(0, "size", "options 'c0' - 's' have undefined sizes"));
        }
        if opt == b'c' && size == 0 && fmt[i - 1].is_ascii_digit() {
            return Err(bad_arg(0, "size", "options 'c0' - 's' have undefined sizes"));
        }
        pos += size;
    }
    Ok(Lv::Number(pos as f64))
}

pub fn struct_table(lua: &Lua) -> mlua::Result<Table> {
    let t = lua.create_table()?;
    t.raw_set("pack", lua.create_function(|lua, a: Variadic<Lv>| struct_pack(lua, &a))?)?;
    t.raw_set("unpack", lua.create_function(|lua, a: Variadic<Lv>| struct_unpack(lua, &a))?)?;
    t.raw_set("size", lua.create_function(|_, a: Variadic<Lv>| struct_size(&a))?)?;
    Ok(t)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tobit_wraps_like_luabitop() {
        assert_eq!(tobit(4294967295.0), -1);
        assert_eq!(tobit(2147483648.0), -2147483648);
        assert_eq!(tobit(-1.0), -1);
        assert_eq!(tobit(1.5), 2);
        assert_eq!(tobit(2.5), 2);
    }
}
