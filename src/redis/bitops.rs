//! Bitmap commands, ported from Redis's bitops.c.

use super::engine::{Command, Ctx, Data, Entry, Reply, cmd, eq_ic, int_arg, syntax};
use super::resp::Value;

pub static COMMANDS: &[Command] = &[
    cmd("setbit", setbit),
    cmd("getbit", getbit),
    cmd("bitcount", bitcount),
    cmd("bitpos", bitpos),
    cmd("bitop", bitop),
    cmd("bitfield", bitfield),
    cmd("bitfield_ro", bitfield_ro),
];

fn bad_offset() -> Value {
    Value::err("ERR bit offset is not an integer or out of range")
}

/// `getBitOffsetFromArgument`. `hash` allows BITFIELD's `#<offset>` form.
fn bit_offset(ctx: &Ctx, raw: &[u8], hash: bool, bits: i64) -> Result<u64, Value> {
    let (body, mul) = match raw.first() {
        Some(b'#') if hash && bits > 0 => (&raw[1..], bits),
        _ => (raw, 1),
    };
    let n: i64 =
        std::str::from_utf8(body).ok().and_then(|s| s.parse().ok()).ok_or_else(bad_offset)?;
    let offset = n.checked_mul(mul).ok_or_else(bad_offset)?;
    let max = ctx.engine.config_num("proto-max-bulk-len");
    if offset < 0 || (offset >> 3) >= max {
        return Err(bad_offset());
    }
    Ok(offset as u64)
}

/// `lookupStringForBitCommand`: the string, grown with zero bytes so that
/// `maxbit` can be addressed. Returns whether the key grew.
fn string_for_write<'a>(
    ctx: &'a mut Ctx,
    key: &[u8],
    maxbit: u64,
) -> Result<(&'a mut Vec<u8>, bool), Value> {
    let needed = (maxbit >> 3) as usize + 1;
    if ctx.get_str(key)?.is_none() {
        ctx.db().insert(key.to_vec(), Entry::new(Data::Str(Vec::new())));
    }
    let s = ctx.get_str(key)?.expect("created");
    let grew = s.len() < needed;
    if grew {
        s.resize(needed, 0);
    }
    Ok((s, grew))
}

fn setbit(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let offset = bit_offset(ctx, &a[2], false, 0)?;
    let bad = || Value::err("ERR bit is not an integer or out of range");
    let on = std::str::from_utf8(&a[3]).ok().and_then(|s| s.parse::<i64>().ok()).ok_or_else(bad)?;
    if on & !1 != 0 {
        return Err(bad());
    }
    // The type check happens before the key is created.
    ctx.get_str(&a[1])?;
    let (s, _) = string_for_write(ctx, &a[1], offset)?;
    let byte = (offset >> 3) as usize;
    let bit = 7 - (offset & 7);
    let old = (s[byte] >> bit) & 1;
    s[byte] &= !(1 << bit);
    s[byte] |= (on as u8 & 1) << bit;
    Ok(Value::Integer(old as i64))
}

fn getbit(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let offset = bit_offset(ctx, &a[2], false, 0)?;
    let Some(s) = ctx.get_str(&a[1])? else { return Ok(Value::Integer(0)) };
    let byte = (offset >> 3) as usize;
    let bit = 7 - (offset & 7);
    let v = s.get(byte).map_or(0, |b| (b >> bit) & 1);
    Ok(Value::Integer(v as i64))
}

/// The shared start/end handling of BITCOUNT and BITPOS: returns the byte
/// range plus the masks of the bits to ignore in the first and last byte.
struct Range {
    start: i64,
    end: i64,
    first_mask: u8,
    last_mask: u8,
}

fn bit_range(len: usize, mut start: i64, mut end: i64, isbit: bool) -> Range {
    let mut totlen = len as i64;
    if isbit {
        totlen <<= 3;
    }
    if start < 0 {
        start += totlen;
    }
    if end < 0 {
        end += totlen;
    }
    start = start.max(0);
    end = end.max(0);
    if end >= totlen {
        end = totlen - 1;
    }
    let (mut first_mask, mut last_mask) = (0u8, 0u8);
    if isbit && start <= end {
        first_mask = !(((1u16 << (8 - (start & 7))) - 1) as u8);
        last_mask = ((1u16 << (7 - (end & 7))) - 1) as u8;
        start >>= 3;
        end >>= 3;
    }
    Range { start, end, first_mask, last_mask }
}

/// Parses a trailing BYTE|BIT argument.
fn isbit_arg(raw: &[u8]) -> Result<bool, Value> {
    if eq_ic(raw, "bit") {
        Ok(true)
    } else if eq_ic(raw, "byte") {
        Ok(false)
    } else {
        Err(syntax())
    }
}

fn bitcount(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let s = ctx.get_str(&a[1])?.cloned().unwrap_or_default();
    let range = match a.len() {
        2 => Range { start: 0, end: s.len() as i64 - 1, first_mask: 0, last_mask: 0 },
        4 | 5 => {
            let start = int_arg(&a[2])?;
            let end = int_arg(&a[3])?;
            if start < 0 && end < 0 && start > end {
                return Ok(Value::Integer(0));
            }
            let isbit = if a.len() == 5 { isbit_arg(&a[4])? } else { false };
            bit_range(s.len(), start, end, isbit)
        }
        _ => return Err(syntax()),
    };
    if range.start > range.end || s.is_empty() {
        return Ok(Value::Integer(0));
    }
    let (lo, hi) = (range.start as usize, range.end as usize);
    let mut count: i64 = s[lo..=hi].iter().map(|b| b.count_ones() as i64).sum();
    if range.first_mask != 0 {
        count -= (s[lo] & range.first_mask).count_ones() as i64;
    }
    if range.last_mask != 0 {
        count -= (s[hi] & range.last_mask).count_ones() as i64;
    }
    Ok(Value::Integer(count))
}

/// The first bit equal to `bit` in `bytes`, or -1. Like `redisBitpos`, a
/// search for 0 that finds only ones returns the bit just past the end.
fn first_bit(bytes: &[u8], bit: bool) -> i64 {
    for (i, b) in bytes.iter().enumerate() {
        let found = if bit { *b != 0 } else { *b != 0xff };
        if found {
            let n = if bit { b.leading_zeros() } else { (!b).leading_zeros() };
            return (i as i64) * 8 + n as i64;
        }
    }
    if bit { -1 } else { (bytes.len() as i64) << 3 }
}

fn bitpos(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let bit = int_arg(&a[2])?;
    if bit != 0 && bit != 1 {
        return Err(Value::err("ERR The bit argument must be 1 or 0."));
    }
    let bit = bit == 1;
    let Some(s) = ctx.get_str(&a[1])?.cloned() else {
        return Ok(Value::Integer(if bit { -1 } else { 0 }));
    };
    let mut end_given = false;
    let range = match a.len() {
        3 => Range { start: 0, end: s.len() as i64 - 1, first_mask: 0, last_mask: 0 },
        4..=6 => {
            let start = int_arg(&a[3])?;
            let isbit = if a.len() == 6 { isbit_arg(&a[5])? } else { false };
            let end = if a.len() >= 5 {
                end_given = true;
                int_arg(&a[4])?
            } else if isbit {
                ((s.len() as i64) << 3) + 7
            } else {
                s.len() as i64 - 1
            };
            bit_range(s.len(), start, end, isbit)
        }
        _ => return Err(syntax()),
    };
    if range.start > range.end || s.is_empty() {
        return Ok(Value::Integer(-1));
    }
    // Mask the bits outside the range off (searching for 1) or on
    // (searching for 0), then search the range as a whole.
    let (lo, hi) = (range.start as usize, range.end as usize);
    let mut window: Vec<u8> = s[lo..=hi].to_vec();
    let last = window.len() - 1;
    if range.first_mask != 0 {
        if bit {
            window[0] &= !range.first_mask;
        } else {
            window[0] |= range.first_mask;
        }
    }
    if range.last_mask != 0 {
        if bit {
            window[last] &= !range.last_mask;
        } else {
            window[last] |= range.last_mask;
        }
    }
    let bytes = window.len() as i64;
    let mut pos = first_bit(&window, bit);
    if end_given && !bit && pos == bytes << 3 {
        return Ok(Value::Integer(-1));
    }
    if pos != -1 {
        pos += (range.start) << 3;
    }
    Ok(Value::Integer(pos))
}

fn bitop(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let op = a[1].to_ascii_lowercase();
    if !matches!(op.as_slice(), b"and" | b"or" | b"xor" | b"not") {
        return Err(syntax());
    }
    let srcs = &a[3..];
    if op == b"not" && srcs.len() != 1 {
        return Err(Value::err("ERR BITOP NOT must be called with a single source key."));
    }
    let mut values = Vec::with_capacity(srcs.len());
    for key in srcs {
        values.push(ctx.get_str(key)?.cloned().unwrap_or_default());
    }
    let maxlen = values.iter().map(|v| v.len()).max().unwrap_or(0);
    let mut out = vec![0u8; maxlen];
    for (i, slot) in out.iter_mut().enumerate() {
        let byte = |v: &Vec<u8>| v.get(i).copied().unwrap_or(0);
        let mut acc = byte(&values[0]);
        if op == b"not" {
            acc = !acc;
        }
        for v in &values[1..] {
            acc = match op.as_slice() {
                b"and" => acc & byte(v),
                b"or" => acc | byte(v),
                _ => acc ^ byte(v),
            };
        }
        *slot = acc;
    }
    let dst = a[2].clone();
    let now = ctx.now;
    ctx.db().remove(&dst, now);
    let len = out.len();
    if len > 0 {
        ctx.db().insert(dst, Entry::new(Data::Str(out)));
    }
    Ok(Value::Integer(len as i64))
}

// ---- BITFIELD ----

#[derive(Clone, Copy, PartialEq)]
enum Op {
    Get,
    Set,
    Incrby,
}

#[derive(Clone, Copy, PartialEq)]
enum Overflow {
    Wrap,
    Sat,
    Fail,
}

struct FieldOp {
    op: Op,
    offset: u64,
    value: i64,
    overflow: Overflow,
    bits: u32,
    signed: bool,
}

/// `getBitfieldTypeFromArgument`.
fn field_type(raw: &[u8]) -> Result<(bool, u32), Value> {
    let bad = || {
        Value::err(
            "ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not \
             supported but i64 is.",
        )
    };
    let signed = match raw.first() {
        Some(b'i') => true,
        Some(b'u') => false,
        _ => return Err(bad()),
    };
    let bits: i64 =
        std::str::from_utf8(&raw[1..]).ok().and_then(|s| s.parse().ok()).ok_or_else(bad)?;
    if bits < 1 || (signed && bits > 64) || (!signed && bits > 63) {
        return Err(bad());
    }
    Ok((signed, bits as u32))
}

fn get_unsigned(s: &[u8], offset: u64, bits: u32) -> u64 {
    let mut v: u64 = 0;
    for i in 0..bits as u64 {
        let byte = ((offset + i) >> 3) as usize;
        let bit = 7 - ((offset + i) & 7);
        let b = s.get(byte).map_or(0, |b| (b >> bit) & 1);
        v = (v << 1) | b as u64;
    }
    v
}

fn get_signed(s: &[u8], offset: u64, bits: u32) -> i64 {
    let v = get_unsigned(s, offset, bits);
    if bits < 64 && v & (1 << (bits - 1)) != 0 { (v | (!0u64 << bits)) as i64 } else { v as i64 }
}

fn set_bits(s: &mut [u8], offset: u64, bits: u32, value: u64) {
    for i in 0..bits as u64 {
        let bitval = (value >> (bits as u64 - 1 - i)) & 1;
        let byte = ((offset + i) >> 3) as usize;
        let bit = 7 - ((offset + i) & 7);
        s[byte] &= !(1 << bit);
        s[byte] |= (bitval as u8) << bit;
    }
}

/// `checkUnsignedBitfieldOverflow`: the wrapped or saturated result when
/// `value + incr` doesn't fit, `None` when it does.
fn unsigned_overflow(value: u64, incr: i64, bits: u32, ow: Overflow) -> Option<u64> {
    let max = if bits == 64 { u64::MAX } else { (1u64 << bits) - 1 };
    let over = (incr > 0 && (incr as u64) > max - value) || value > max;
    let under = incr < 0 && (incr.unsigned_abs()) > value;
    if !over && !under {
        return None;
    }
    Some(match ow {
        Overflow::Wrap => value.wrapping_add(incr as u64) & !(!0u64 << bits.min(63)),
        Overflow::Sat => {
            if over {
                max
            } else {
                0
            }
        }
        Overflow::Fail => 0,
    })
}

/// `checkSignedBitfieldOverflow`.
fn signed_overflow(value: i64, incr: i64, bits: u32, ow: Overflow) -> Option<i64> {
    let max = if bits == 64 { i64::MAX } else { (1i64 << (bits - 1)) - 1 };
    let min = -max - 1;
    let sum = (value as i128) + (incr as i128);
    let over = sum > max as i128;
    let under = sum < min as i128;
    if !over && !under {
        return None;
    }
    Some(match ow {
        Overflow::Wrap => {
            let c = (value as u64).wrapping_add(incr as u64);
            if bits < 64 {
                let msb = 1u64 << (bits - 1);
                let mask = !0u64 << bits;
                if c & msb != 0 { (c | mask) as i64 } else { (c & !mask) as i64 }
            } else {
                c as i64
            }
        }
        Overflow::Sat => {
            if over {
                max
            } else {
                min
            }
        }
        Overflow::Fail => 0,
    })
}

fn bitfield_generic(ctx: &mut Ctx, a: &[Vec<u8>], readonly_cmd: bool) -> Reply {
    let mut ops: Vec<FieldOp> = Vec::new();
    let mut overflow = Overflow::Wrap;
    let mut writes = false;
    let mut highest = 0u64;
    let mut j = 2;
    while j < a.len() {
        let left = a.len() - j - 1;
        let op = if eq_ic(&a[j], "get") && left >= 2 {
            Op::Get
        } else if eq_ic(&a[j], "set") && left >= 3 {
            Op::Set
        } else if eq_ic(&a[j], "incrby") && left >= 3 {
            Op::Incrby
        } else if eq_ic(&a[j], "overflow") && left >= 1 {
            overflow = if eq_ic(&a[j + 1], "wrap") {
                Overflow::Wrap
            } else if eq_ic(&a[j + 1], "sat") {
                Overflow::Sat
            } else if eq_ic(&a[j + 1], "fail") {
                Overflow::Fail
            } else {
                return Err(Value::err("ERR Invalid OVERFLOW type specified"));
            };
            j += 2;
            continue;
        } else {
            return Err(syntax());
        };
        let (signed, bits) = field_type(&a[j + 1])?;
        let offset = bit_offset(ctx, &a[j + 2], true, bits as i64)?;
        let mut value = 0;
        if op != Op::Get {
            writes = true;
            highest = highest.max(offset + bits as u64 - 1);
            value = int_arg(&a[j + 3])?;
        }
        ops.push(FieldOp { op, offset, value, overflow, bits, signed });
        j += if op == Op::Get { 3 } else { 4 };
    }
    if writes && readonly_cmd {
        return Err(Value::err("ERR BITFIELD_RO only supports the GET subcommand"));
    }
    let s = if writes {
        ctx.get_str(&a[1])?;
        string_for_write(ctx, &a[1], highest)?.0.clone()
    } else {
        ctx.get_str(&a[1])?.cloned().unwrap_or_default()
    };
    let mut buf = s;
    let mut out = Vec::with_capacity(ops.len());
    for o in &ops {
        match o.op {
            Op::Get => {
                out.push(Value::Integer(if o.signed {
                    get_signed(&buf, o.offset, o.bits)
                } else {
                    get_unsigned(&buf, o.offset, o.bits) as i64
                }));
            }
            _ => {
                let (reply, new) = apply(&buf, o);
                match reply {
                    Some(v) => {
                        out.push(Value::Integer(v));
                        set_bits(&mut buf, o.offset, o.bits, new);
                    }
                    None => out.push(Value::Null),
                }
            }
        }
    }
    if writes {
        *ctx.get_str(&a[1])?.expect("created") = buf;
    }
    Ok(Value::Array(out))
}

/// One SET or INCRBY: the value to reply (None when OVERFLOW FAIL bites)
/// and the bits to store.
fn apply(buf: &[u8], o: &FieldOp) -> (Option<i64>, u64) {
    if o.signed {
        let old = get_signed(buf, o.offset, o.bits);
        let (incr, base) = if o.op == Op::Incrby { (o.value, old) } else { (0, o.value) };
        let limited = signed_overflow(base, incr, o.bits, o.overflow);
        if limited.is_some() && o.overflow == Overflow::Fail {
            return (None, 0);
        }
        let new = limited.unwrap_or(base.wrapping_add(incr));
        let reply = if o.op == Op::Incrby { new } else { old };
        (Some(reply), new as u64)
    } else {
        let old = get_unsigned(buf, o.offset, o.bits);
        let (incr, base) = if o.op == Op::Incrby { (o.value, old) } else { (0, o.value as u64) };
        let limited = unsigned_overflow(base, incr, o.bits, o.overflow);
        if limited.is_some() && o.overflow == Overflow::Fail {
            return (None, 0);
        }
        let new = limited.unwrap_or(base.wrapping_add(incr as u64));
        let reply = if o.op == Op::Incrby { new } else { old };
        (Some(reply as i64), new)
    }
}

fn bitfield(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    bitfield_generic(ctx, a, false)
}

fn bitfield_ro(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    bitfield_generic(ctx, a, true)
}
