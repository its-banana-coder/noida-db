//! PFADD, PFCOUNT and PFMERGE: HyperLogLog, ported from Redis 7.2's
//! src/hyperloglog.c (BSD-3-Clause, see THIRD_PARTY.md) so counts and the
//! stored bytes match a real Redis.
//!
//! A HyperLogLog is an ordinary string: a 16 byte header (`HYLL`, encoding,
//! 3 unused bytes, an 8 byte cached cardinality) followed by 16384 registers.
//! New ones are *sparse* (run-length opcodes) and become *dense* (6 bits per
//! register, 12304 bytes in all) once they outgrow `hll-sparse-max-bytes`.

use super::engine::{Command, Ctx, Data, Entry, Reply, cmd, wrong_type};
use super::resp::Value;

pub static COMMANDS: &[Command] =
    &[cmd("pfadd", pfadd), cmd("pfcount", pfcount), cmd("pfmerge", pfmerge)];

const P: usize = 14;
const Q: usize = 64 - P;
const REGISTERS: usize = 1 << P;
const P_MASK: u64 = (REGISTERS - 1) as u64;
const BITS: usize = 6;
const REGISTER_MAX: u8 = 63;
const HDR: usize = 16;
const DENSE_SIZE: usize = HDR + (REGISTERS * BITS).div_ceil(8);
const DENSE: u8 = 0;
const SPARSE: u8 = 1;
const ALPHA_INF: f64 = 0.721_347_520_444_481_7;

const VAL_MAX_VALUE: u8 = 32;
const VAL_MAX_LEN: usize = 4;
const ZERO_MAX_LEN: usize = 64;
const XZERO_MAX_LEN: usize = 16384;
const DEFAULT_SPARSE_MAX_BYTES: usize = 3000;

const INVALID_HLL: &str = "WRONGTYPE Key is not a valid HyperLogLog string value.";
const CORRUPT_HLL: &str = "INVALIDOBJ Corrupted HLL object detected";

// ---- sparse opcodes ----

fn is_zero(b: u8) -> bool {
    b & 0xc0 == 0
}
fn is_xzero(b: u8) -> bool {
    b & 0xc0 == 0x40
}
fn is_val(b: u8) -> bool {
    b & 0x80 != 0
}
fn zero_len(b: u8) -> usize {
    (b & 0x3f) as usize + 1
}
fn xzero_len(b: u8, next: u8) -> usize {
    (((b & 0x3f) as usize) << 8 | next as usize) + 1
}
fn val_value(b: u8) -> u8 {
    ((b >> 2) & 0x1f) + 1
}
fn val_len(b: u8) -> usize {
    (b & 0x3) as usize + 1
}
fn val_op(val: u8, len: usize) -> u8 {
    ((val - 1) << 2 | (len as u8 - 1)) | 0x80
}
fn zero_op(len: usize) -> u8 {
    len as u8 - 1
}
fn xzero_op(len: usize) -> [u8; 2] {
    let l = len - 1;
    [(l >> 8) as u8 | 0x40, (l & 0xff) as u8]
}

// ---- hashing ----

fn murmur64a(key: &[u8], seed: u64) -> u64 {
    const M: u64 = 0xc6a4a7935bd1e995;
    const R: u32 = 47;
    let mut h = seed ^ (key.len() as u64).wrapping_mul(M);
    let (chunks, tail) = key.as_chunks::<8>();
    for chunk in chunks {
        let mut k = u64::from_le_bytes(*chunk);
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h ^= k;
        h = h.wrapping_mul(M);
    }
    if !tail.is_empty() {
        for (i, &b) in tail.iter().enumerate().rev() {
            h ^= (b as u64) << (8 * i);
        }
        h = h.wrapping_mul(M);
    }
    h ^= h >> R;
    h = h.wrapping_mul(M);
    h ^= h >> R;
    h
}

/// The register an element maps to and the value it proposes for it.
fn pat_len(ele: &[u8]) -> (usize, u8) {
    let mut hash = murmur64a(ele, 0xadc83b19);
    let index = (hash & P_MASK) as usize;
    hash >>= P;
    hash |= 1 << Q;
    let mut bit = 1u64;
    let mut count = 1u8;
    while hash & bit == 0 {
        count += 1;
        bit <<= 1;
    }
    (index, count)
}

// ---- dense registers ----

fn dense_get(regs: &[u8], i: usize) -> u8 {
    let byte = i * BITS / 8;
    let fb = (i * BITS) & 7;
    let b0 = regs[byte] as u32;
    let b1 = regs.get(byte + 1).copied().unwrap_or(0) as u32;
    (((b0 >> fb) | (b1 << (8 - fb))) & REGISTER_MAX as u32) as u8
}

fn dense_put(regs: &mut [u8], i: usize, val: u8) {
    let byte = i * BITS / 8;
    let fb = (i * BITS) & 7;
    let v = val as u32;
    regs[byte] &= !((REGISTER_MAX as u32) << fb) as u8;
    regs[byte] |= (v << fb) as u8;
    if let Some(next) = regs.get_mut(byte + 1) {
        *next &= !((REGISTER_MAX as u32) >> (8 - fb)) as u8;
        *next |= (v >> (8 - fb)) as u8;
    }
}

fn dense_set(regs: &mut [u8], i: usize, count: u8) -> bool {
    if count > dense_get(regs, i) {
        dense_put(regs, i, count);
        true
    } else {
        false
    }
}

// ---- construction and conversion ----

/// A new, empty HLL in the sparse encoding.
fn create() -> Vec<u8> {
    let mut s = vec![0u8; HDR];
    s[..4].copy_from_slice(b"HYLL");
    s[4] = SPARSE;
    let mut left = REGISTERS;
    while left > 0 {
        let run = XZERO_MAX_LEN.min(left);
        s.extend_from_slice(&xzero_op(run));
        left -= run;
    }
    s
}

/// A well-formed HLL string (`isHLLObjectOrReply`).
fn is_hll(s: &[u8]) -> bool {
    s.len() >= HDR
        && &s[..4] == b"HYLL"
        && s[4] <= SPARSE
        && (s[4] != DENSE || s.len() == DENSE_SIZE)
}

/// Walks a sparse body, calling `f(first_register, run_length, value)` for
/// each run. `Err` if the runs don't add up to exactly 16384 registers.
fn walk_sparse(body: &[u8], mut f: impl FnMut(usize, usize, u8)) -> Result<(), ()> {
    let (mut p, mut idx) = (0, 0);
    while p < body.len() {
        let b = body[p];
        let (run, val, width) = if is_zero(b) {
            (zero_len(b), 0, 1)
        } else if is_xzero(b) {
            (xzero_len(b, body.get(p + 1).copied().unwrap_or(0)), 0, 2)
        } else {
            (val_len(b), val_value(b), 1)
        };
        if idx + run > REGISTERS {
            return Err(());
        }
        f(idx, run, val);
        idx += run;
        p += width;
    }
    if idx == REGISTERS { Ok(()) } else { Err(()) }
}

/// `hllSparseToDense`: rebuilds the string in the dense encoding.
fn sparse_to_dense(o: &mut Vec<u8>) -> Result<(), ()> {
    if o[4] == DENSE {
        return Ok(());
    }
    let mut dense = vec![0u8; DENSE_SIZE];
    dense[..HDR].copy_from_slice(&o[..HDR]);
    dense[4] = DENSE;
    let regs = &mut dense[HDR..];
    walk_sparse(&o[HDR..], |first, run, val| {
        if val > 0 {
            for i in first..first + run {
                dense_put(regs, i, val);
            }
        }
    })?;
    *o = dense;
    Ok(())
}

fn invalidate_cache(o: &mut [u8]) {
    o[15] |= 1 << 7;
}

fn valid_cache(o: &[u8]) -> bool {
    o[15] & (1 << 7) == 0
}

/// `hllSparseSet`: raises register `index` to `count` in a sparse HLL.
/// 1 = changed, 0 = unchanged, -1 = corrupt. Promotes to dense when the
/// value is too large for a sparse opcode or the string outgrows `max_bytes`.
fn sparse_set(o: &mut Vec<u8>, index: usize, count: u8, max_bytes: usize) -> i32 {
    let promote = |o: &mut Vec<u8>| -> i32 {
        if sparse_to_dense(o).is_err() {
            return -1;
        }
        dense_set(&mut o[HDR..], index, count);
        1
    };
    if count > VAL_MAX_VALUE {
        return promote(o);
    }

    // Find the opcode covering `index`.
    let mut end = o.len();
    let (mut p, mut first, mut span) = (HDR, 0usize, 0usize);
    let mut prev: Option<usize> = None;
    while p < end {
        let b = o[p];
        let mut oplen = 1;
        if is_zero(b) {
            span = zero_len(b);
        } else if is_val(b) {
            span = val_len(b);
        } else {
            span = xzero_len(b, o.get(p + 1).copied().unwrap_or(0));
            oplen = 2;
        }
        if index < first + span {
            break;
        }
        prev = Some(p);
        p += oplen;
        first += span;
    }
    if span == 0 || p >= end {
        return -1;
    }

    let b = o[p];
    let (is_z, is_xz, is_v) = (is_zero(b), is_xzero(b), !is_zero(b) && !is_xzero(b));
    let runlen = if is_z {
        zero_len(b)
    } else if is_xz {
        xzero_len(b, o[p + 1])
    } else {
        val_len(b)
    };

    let mut resolved = false;
    if is_v {
        if val_value(b) >= count {
            return 0;
        }
        if runlen == 1 {
            o[p] = val_op(count, 1);
            resolved = true;
        }
    }
    if !resolved && is_z && runlen == 1 {
        o[p] = val_op(count, 1);
        resolved = true;
    }
    if !resolved {
        // Split the run around `index`.
        let last = first + span - 1;
        let mut seq: Vec<u8> = Vec::with_capacity(5);
        let push_zeros = |seq: &mut Vec<u8>, len: usize| {
            if len > ZERO_MAX_LEN {
                seq.extend_from_slice(&xzero_op(len));
            } else {
                seq.push(zero_op(len));
            }
        };
        if is_z || is_xz {
            if index != first {
                push_zeros(&mut seq, index - first);
            }
            seq.push(val_op(count, 1));
            if index != last {
                push_zeros(&mut seq, last - index);
            }
        } else {
            let cur = val_value(b);
            if index != first {
                seq.push(val_op(cur, index - first));
            }
            seq.push(val_op(count, 1));
            if index != last {
                seq.push(val_op(cur, last - index));
            }
        }
        let oldlen = if is_xz { 2 } else { 1 };
        let delta = seq.len() as isize - oldlen as isize;
        if delta > 0 && o.len() + delta as usize > max_bytes {
            return promote(o);
        }
        o.splice(p..p + oldlen, seq);
        end = (end as isize + delta) as usize;
    }

    // Merge neighbouring VAL opcodes that now hold the same value.
    let mut p = prev.unwrap_or(HDR);
    let mut scan = 5;
    while p < end && scan > 0 {
        scan -= 1;
        if is_xzero(o[p]) {
            p += 2;
            continue;
        } else if is_zero(o[p]) {
            p += 1;
            continue;
        }
        if p + 1 < end && is_val(o[p + 1]) {
            let (v1, v2) = (val_value(o[p]), val_value(o[p + 1]));
            if v1 == v2 {
                let len = val_len(o[p]) + val_len(o[p + 1]);
                if len <= VAL_MAX_LEN {
                    o[p + 1] = val_op(v1, len);
                    o.remove(p);
                    end -= 1;
                    continue;
                }
            }
        }
        p += 1;
    }
    invalidate_cache(o);
    1
}

/// `hllAdd`: 1 = a register changed, 0 = not, -1 = corrupt.
fn add(o: &mut Vec<u8>, ele: &[u8], max_bytes: usize) -> i32 {
    let (index, count) = pat_len(ele);
    match o[4] {
        DENSE => dense_set(&mut o[HDR..], index, count) as i32,
        SPARSE => sparse_set(o, index, count, max_bytes),
        _ => -1,
    }
}

// ---- estimation ----

fn sigma(mut x: f64) -> f64 {
    if x == 1.0 {
        return f64::INFINITY;
    }
    let (mut y, mut z) = (1.0, x);
    loop {
        x *= x;
        let z_prime = z;
        z += x * y;
        y += y;
        if z_prime == z {
            return z;
        }
    }
}

fn tau(mut x: f64) -> f64 {
    if x == 0.0 || x == 1.0 {
        return 0.0;
    }
    let (mut y, mut z) = (1.0, 1.0 - x);
    loop {
        x = x.sqrt();
        let z_prime = z;
        y *= 0.5;
        let d = 1.0 - x;
        z -= d * d * y;
        if z_prime == z {
            return z / 3.0;
        }
    }
}

/// The estimated cardinality from a histogram of register values.
fn estimate(histo: &[u32; 64]) -> u64 {
    let m = REGISTERS as f64;
    let mut z = m * tau((m - histo[Q + 1] as f64) / m);
    for j in (1..=Q).rev() {
        z += histo[j] as f64;
        z *= 0.5;
    }
    z += m * sigma(histo[0] as f64 / m);
    (ALPHA_INF * m * m / z).round() as u64
}

/// The register histogram of a valid HLL string, `Err` if a sparse body is corrupt.
fn histogram(o: &[u8]) -> Result<[u32; 64], ()> {
    let mut histo = [0u32; 64];
    if o[4] == DENSE {
        for i in 0..REGISTERS {
            histo[dense_get(&o[HDR..], i) as usize] += 1;
        }
    } else {
        walk_sparse(&o[HDR..], |_, run, val| histo[val as usize] += run as u32)?;
    }
    Ok(histo)
}

/// `hllMerge`: raises `max` to the registers of `o`.
fn merge_into(max: &mut [u8; REGISTERS], o: &[u8]) -> Result<(), ()> {
    if o[4] == DENSE {
        for (i, m) in max.iter_mut().enumerate() {
            *m = (*m).max(dense_get(&o[HDR..], i));
        }
        Ok(())
    } else {
        walk_sparse(&o[HDR..], |first, run, val| {
            for m in &mut max[first..first + run] {
                *m = (*m).max(val);
            }
        })
    }
}

// ---- commands ----

fn max_bytes(ctx: &Ctx) -> usize {
    ctx.engine
        .config
        .get("hll-sparse-max-bytes")
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_SPARSE_MAX_BYTES)
}

/// The HLL at `key`: `None` if missing; an error for other types or a
/// string that isn't an HLL.
fn read_hll(ctx: &mut Ctx, key: &[u8]) -> Result<Option<Vec<u8>>, Value> {
    match ctx.lookup(key) {
        None => Ok(None),
        Some(Entry { data: Data::Str(s), .. }) if is_hll(s) => Ok(Some(s.clone())),
        Some(Entry { data: Data::Str(_), .. }) => Err(Value::err(INVALID_HLL)),
        Some(_) => Err(wrong_type()),
    }
}

fn pfadd(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let limit = max_bytes(ctx);
    let mut updated = 0;
    if read_hll(ctx, &a[1])?.is_none() {
        ctx.db().insert(a[1].clone(), Entry::new(Data::Str(create())));
        updated += 1;
    }
    let Some(Entry { data: Data::Str(s), .. }) = ctx.lookup(&a[1]) else { unreachable!() };
    for ele in &a[2..] {
        match add(s, ele, limit) {
            1 => updated += 1,
            -1 => return Err(Value::err(CORRUPT_HLL)),
            _ => {}
        }
    }
    if updated > 0 {
        invalidate_cache(s);
        ctx.notify_keyspace_event('$', "pfadd", &a[1]);
    }
    Ok(Value::Integer((updated > 0) as i64))
}

fn pfcount(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let corrupt = || Value::err(CORRUPT_HLL);
    if a.len() > 2 {
        // The union of several HLLs.
        let mut max = [0u8; REGISTERS];
        for key in &a[1..] {
            let Some(hll) = read_hll(ctx, key)? else { continue };
            merge_into(&mut max, &hll).map_err(|_| corrupt())?;
        }
        let mut histo = [0u32; 64];
        for &m in &max {
            histo[m as usize] += 1;
        }
        return Ok(Value::Integer(estimate(&histo) as i64));
    }
    let Some(hll) = read_hll(ctx, &a[1])? else { return Ok(Value::Integer(0)) };
    if valid_cache(&hll) {
        let card = u64::from_le_bytes(hll[8..16].try_into().unwrap());
        return Ok(Value::Integer(card as i64));
    }
    let card = estimate(&histogram(&hll).map_err(|_| corrupt())?);
    // Cache the answer in the header, as Redis does.
    if let Some(Entry { data: Data::Str(s), .. }) = ctx.lookup(&a[1]) {
        s[8..16].copy_from_slice(&card.to_le_bytes());
    }
    Ok(Value::Integer(card as i64))
}

fn pfmerge(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let limit = max_bytes(ctx);
    let mut max = [0u8; REGISTERS];
    let mut use_dense = false;
    for key in &a[1..] {
        let Some(hll) = read_hll(ctx, key)? else { continue };
        use_dense |= hll[4] == DENSE;
        merge_into(&mut max, &hll).map_err(|_| Value::err(CORRUPT_HLL))?;
    }
    if ctx.lookup(&a[1]).is_none() {
        ctx.db().insert(a[1].clone(), Entry::new(Data::Str(create())));
    }
    let Some(Entry { data: Data::Str(s), .. }) = ctx.lookup(&a[1]) else { unreachable!() };
    if use_dense && sparse_to_dense(s).is_err() {
        return Err(Value::err(CORRUPT_HLL));
    }
    for (i, &m) in max.iter().enumerate() {
        if m == 0 {
            continue;
        }
        match s[4] {
            DENSE => {
                dense_set(&mut s[HDR..], i, m);
            }
            SPARSE => {
                sparse_set(s, i, m, limit);
            }
            _ => {}
        }
    }
    invalidate_cache(s);
    ctx.notify_keyspace_event('$', "pfmerge", &a[1]);
    Ok(Value::ok())
}
