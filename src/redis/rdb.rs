//! The RDB serialization DUMP/RESTORE and FUNCTION DUMP/RESTORE use: a type
//! byte, the value, the RDB version (2 bytes) and a CRC-64 of everything
//! before it (8 bytes), all as in Redis 7.2's rdb.c.
//!
//! Writing uses the plain encodings every Redis loads (a string, then
//! length-prefixed elements). Reading also takes the compact encodings
//! Redis 7.2 itself writes — listpack, ziplist, intset, quicklist and
//! LZF-compressed strings — so a payload from a real Redis restores here,
//! and one from here restores into a real Redis. Streams aren't supported.
//! Re-implemented from the RDB format's documented layout.

use std::collections::VecDeque;

use super::engine::{Data, Hash, Limits};

/// RDB_VERSION of Redis 7.2.
pub const RDB_VERSION: u16 = 11;

const TYPE_STRING: u8 = 0;
const TYPE_LIST: u8 = 1;
const TYPE_SET: u8 = 2;
const TYPE_ZSET: u8 = 3;
const TYPE_HASH: u8 = 4;
const TYPE_ZSET_2: u8 = 5;
const TYPE_HASH_ZIPMAP: u8 = 9;
const TYPE_LIST_ZIPLIST: u8 = 10;
const TYPE_SET_INTSET: u8 = 11;
const TYPE_ZSET_ZIPLIST: u8 = 12;
const TYPE_HASH_ZIPLIST: u8 = 13;
const TYPE_LIST_QUICKLIST: u8 = 14;
const TYPE_HASH_LISTPACK: u8 = 16;
const TYPE_ZSET_LISTPACK: u8 = 17;
const TYPE_LIST_QUICKLIST_2: u8 = 18;
const TYPE_SET_LISTPACK: u8 = 20;
/// FUNCTION DUMP's per-library opcode.
pub const OPCODE_FUNCTION2: u8 = 245;

/// Redis's CRC-64 (Jones polynomial, reflected, no final xor).
pub fn crc64(data: &[u8]) -> u64 {
    const POLY: u64 = 0x95ac_9329_ac4b_c9b5;
    let mut crc: u64 = 0;
    for &b in data {
        crc ^= b as u64;
        for _ in 0..8 {
            crc = if crc & 1 == 1 { (crc >> 1) ^ POLY } else { crc >> 1 };
        }
    }
    crc
}

/// Appends the version and checksum footer.
pub fn seal(mut payload: Vec<u8>) -> Vec<u8> {
    payload.extend_from_slice(&RDB_VERSION.to_le_bytes());
    let crc = crc64(&payload);
    payload.extend_from_slice(&crc.to_le_bytes());
    payload
}

/// The body of a sealed payload, if its version is one Redis 7.2 loads and
/// its checksum is right (a zero checksum means "not computed").
pub fn unseal(payload: &[u8]) -> Option<&[u8]> {
    unseal_with(payload, true)
}

/// `unseal`, optionally without the checksum test (DEBUG
/// SET-SKIP-CHECKSUM-VALIDATION 1).
pub fn unseal_with(payload: &[u8], check_crc: bool) -> Option<&[u8]> {
    if payload.len() < 10 {
        return None;
    }
    let n = payload.len();
    let version = u16::from_le_bytes([payload[n - 10], payload[n - 9]]);
    if version > RDB_VERSION {
        return None;
    }
    let crc = u64::from_le_bytes(payload[n - 8..].try_into().ok()?);
    if check_crc && crc != 0 && crc != crc64(&payload[..n - 8]) {
        return None;
    }
    Some(&payload[..n - 10])
}

// ---- writing ----

pub fn write_len(out: &mut Vec<u8>, len: u64) {
    if len < 64 {
        out.push(len as u8);
    } else if len < 16384 {
        out.push(0x40 | (len >> 8) as u8);
        out.push(len as u8);
    } else if len <= u32::MAX as u64 {
        out.push(0x80);
        out.extend_from_slice(&(len as u32).to_be_bytes());
    } else {
        out.push(0x81);
        out.extend_from_slice(&len.to_be_bytes());
    }
}

/// A string, integer-encoded when it is a small canonical integer.
pub fn write_string(out: &mut Vec<u8>, s: &[u8]) {
    if s.len() <= 11
        && let Ok(text) = std::str::from_utf8(s)
        && let Ok(v) = text.parse::<i64>()
        && v.to_string() == text
    {
        if let Ok(b) = i8::try_from(v) {
            out.push(0xC0);
            out.push(b as u8);
            return;
        } else if let Ok(b) = i16::try_from(v) {
            out.push(0xC1);
            out.extend_from_slice(&b.to_le_bytes());
            return;
        } else if let Ok(b) = i32::try_from(v) {
            out.push(0xC2);
            out.extend_from_slice(&b.to_le_bytes());
            return;
        }
    }
    write_len(out, s.len() as u64);
    out.extend_from_slice(s);
}

/// DUMP's payload for a value.
pub fn dump(data: &Data) -> Option<Vec<u8>> {
    let mut out = vec![];
    match data {
        Data::Str(s) => {
            out.push(TYPE_STRING);
            write_string(&mut out, s);
        }
        Data::List(l) => {
            out.push(TYPE_LIST);
            write_len(&mut out, l.len() as u64);
            for item in l {
                write_string(&mut out, item);
            }
        }
        Data::Set(s) => {
            let members = s.members();
            out.push(TYPE_SET);
            write_len(&mut out, members.len() as u64);
            for m in &members {
                write_string(&mut out, m);
            }
        }
        Data::Zset(z) => {
            out.push(TYPE_ZSET_2);
            write_len(&mut out, z.len() as u64);
            // Redis writes the highest score first; either order loads.
            for (score, m) in z.iter().collect::<Vec<_>>().into_iter().rev() {
                write_string(&mut out, m);
                out.extend_from_slice(&score.to_le_bytes());
            }
        }
        Data::Hash(h) => {
            out.push(TYPE_HASH);
            write_len(&mut out, h.map.len() as u64);
            for (k, v) in h.map.iter() {
                write_string(&mut out, k);
                write_string(&mut out, v);
            }
        }
        Data::Stream(_) => return None,
    }
    Some(seal(out))
}

// ---- reading ----

pub struct Reader<'a> {
    pub buf: &'a [u8],
    pub pos: usize,
}

impl<'a> Reader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Reader { buf, pos: 0 }
    }

    pub fn at_end(&self) -> bool {
        self.pos >= self.buf.len()
    }

    pub fn byte(&mut self) -> Option<u8> {
        let b = *self.buf.get(self.pos)?;
        self.pos += 1;
        Some(b)
    }

    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let s = self.buf.get(self.pos..self.pos.checked_add(n)?)?;
        self.pos += n;
        Some(s)
    }

    /// A length, or `Err(enc)` for a special (11xxxxxx) string encoding.
    fn len_or_enc(&mut self) -> Option<Result<u64, u8>> {
        let b = self.byte()?;
        Some(match b >> 6 {
            0 => Ok((b & 0x3f) as u64),
            1 => Ok((((b & 0x3f) as u64) << 8) | self.byte()? as u64),
            2 => match b {
                0x80 => Ok(u32::from_be_bytes(self.take(4)?.try_into().ok()?) as u64),
                0x81 => Ok(u64::from_be_bytes(self.take(8)?.try_into().ok()?)),
                _ => return None,
            },
            _ => Err(b & 0x3f),
        })
    }

    pub fn len(&mut self) -> Option<u64> {
        self.len_or_enc()?.ok()
    }

    pub fn string(&mut self) -> Option<Vec<u8>> {
        match self.len_or_enc()? {
            Ok(n) => Some(self.take(n as usize)?.to_vec()),
            Err(0) => Some((self.byte()? as i8).to_string().into_bytes()),
            Err(1) => {
                Some(i16::from_le_bytes(self.take(2)?.try_into().ok()?).to_string().into_bytes())
            }
            Err(2) => {
                Some(i32::from_le_bytes(self.take(4)?.try_into().ok()?).to_string().into_bytes())
            }
            Err(3) => {
                let clen = self.len()? as usize;
                let len = self.len()? as usize;
                lzf_decompress(self.take(clen)?, len)
            }
            Err(_) => None,
        }
    }

    fn double_text(&mut self) -> Option<f64> {
        // RDB_TYPE_ZSET: a length byte then the score as text (253 = nan,
        // 254 = inf, 255 = -inf).
        let n = self.byte()?;
        match n {
            253 => Some(f64::NAN),
            254 => Some(f64::INFINITY),
            255 => Some(f64::NEG_INFINITY),
            n => std::str::from_utf8(self.take(n as usize)?).ok()?.parse().ok(),
        }
    }
}

/// LZF decompression (liblzf's format).
fn lzf_decompress(input: &[u8], out_len: usize) -> Option<Vec<u8>> {
    let mut out: Vec<u8> = Vec::with_capacity(out_len);
    let mut i = 0;
    while i < input.len() {
        let ctrl = input[i] as usize;
        i += 1;
        if ctrl < 32 {
            let n = ctrl + 1;
            out.extend_from_slice(input.get(i..i + n)?);
            i += n;
        } else {
            let mut len = ctrl >> 5;
            if len == 7 {
                len += *input.get(i)? as usize;
                i += 1;
            }
            let back = ((ctrl & 0x1f) << 8) + *input.get(i)? as usize + 1;
            i += 1;
            let start = out.len().checked_sub(back)?;
            for k in 0..len + 2 {
                let b = out[start + k];
                out.push(b);
            }
        }
    }
    (out.len() == out_len).then_some(out)
}

/// The entries of a listpack blob.
fn listpack(blob: &[u8]) -> Option<Vec<Vec<u8>>> {
    let mut out = vec![];
    let mut p = 6usize;
    while p < blob.len() && blob[p] != 0xFF {
        let b = blob[p];
        let (val, enc_len): (Vec<u8>, usize) = if b & 0x80 == 0 {
            ((b & 0x7f).to_string().into_bytes(), 1)
        } else if b & 0xC0 == 0x80 {
            let n = (b & 0x3f) as usize;
            (blob.get(p + 1..p + 1 + n)?.to_vec(), 1 + n)
        } else if b & 0xE0 == 0xC0 {
            let raw = (((b & 0x1f) as u16) << 8) | *blob.get(p + 1)? as u16;
            let v = ((raw << 3) as i16) >> 3;
            (v.to_string().into_bytes(), 2)
        } else if b & 0xF0 == 0xE0 {
            let n = (((b & 0x0f) as usize) << 8) | *blob.get(p + 1)? as usize;
            (blob.get(p + 2..p + 2 + n)?.to_vec(), 2 + n)
        } else {
            match b {
                0xF0 => {
                    let n = u32::from_le_bytes(blob.get(p + 1..p + 5)?.try_into().ok()?) as usize;
                    (blob.get(p + 5..p + 5 + n)?.to_vec(), 5 + n)
                }
                0xF1 => (
                    i16::from_le_bytes(blob.get(p + 1..p + 3)?.try_into().ok()?)
                        .to_string()
                        .into_bytes(),
                    3,
                ),
                0xF2 => {
                    let raw = blob.get(p + 1..p + 4)?;
                    let v = ((raw[0] as i32) | ((raw[1] as i32) << 8) | ((raw[2] as i32) << 16))
                        << 8
                        >> 8;
                    (v.to_string().into_bytes(), 4)
                }
                0xF3 => (
                    i32::from_le_bytes(blob.get(p + 1..p + 5)?.try_into().ok()?)
                        .to_string()
                        .into_bytes(),
                    5,
                ),
                0xF4 => (
                    i64::from_le_bytes(blob.get(p + 1..p + 9)?.try_into().ok()?)
                        .to_string()
                        .into_bytes(),
                    9,
                ),
                _ => return None,
            }
        };
        // The backlen after each entry: 1 to 5 bytes, by the entry's size.
        let back = match enc_len {
            0..=127 => 1,
            128..=16382 => 2,
            16383..=2097150 => 3,
            2097151..=268435454 => 4,
            _ => 5,
        };
        out.push(val);
        p += enc_len + back;
    }
    Some(out)
}

/// The entries of a ziplist blob.
fn ziplist(blob: &[u8]) -> Option<Vec<Vec<u8>>> {
    let mut out = vec![];
    let mut p = 10usize;
    while p < blob.len() && blob[p] != 0xFF {
        // prevlen
        p += if blob[p] == 0xFE { 5 } else { 1 };
        let b = *blob.get(p)?;
        let (val, len): (Vec<u8>, usize) = match b >> 6 {
            0 => {
                let n = (b & 0x3f) as usize;
                (blob.get(p + 1..p + 1 + n)?.to_vec(), 1 + n)
            }
            1 => {
                let n = (((b & 0x3f) as usize) << 8) | *blob.get(p + 1)? as usize;
                (blob.get(p + 2..p + 2 + n)?.to_vec(), 2 + n)
            }
            2 => {
                let n = u32::from_be_bytes(blob.get(p + 1..p + 5)?.try_into().ok()?) as usize;
                (blob.get(p + 5..p + 5 + n)?.to_vec(), 5 + n)
            }
            _ => match b {
                0xC0 => (
                    i16::from_le_bytes(blob.get(p + 1..p + 3)?.try_into().ok()?)
                        .to_string()
                        .into_bytes(),
                    3,
                ),
                0xD0 => (
                    i32::from_le_bytes(blob.get(p + 1..p + 5)?.try_into().ok()?)
                        .to_string()
                        .into_bytes(),
                    5,
                ),
                0xE0 => (
                    i64::from_le_bytes(blob.get(p + 1..p + 9)?.try_into().ok()?)
                        .to_string()
                        .into_bytes(),
                    9,
                ),
                0xF0 => {
                    let raw = blob.get(p + 1..p + 4)?;
                    let v = ((raw[0] as i32) | ((raw[1] as i32) << 8) | ((raw[2] as i32) << 16))
                        << 8
                        >> 8;
                    (v.to_string().into_bytes(), 4)
                }
                0xFE => ((*blob.get(p + 1)? as i8).to_string().into_bytes(), 2),
                0xF1..=0xFD => (((b & 0x0f) - 1).to_string().into_bytes(), 1),
                _ => return None,
            },
        };
        out.push(val);
        p += len;
    }
    Some(out)
}

fn intset(blob: &[u8]) -> Option<Vec<Vec<u8>>> {
    let enc = u32::from_le_bytes(blob.get(0..4)?.try_into().ok()?) as usize;
    let n = u32::from_le_bytes(blob.get(4..8)?.try_into().ok()?) as usize;
    let mut out = vec![];
    for k in 0..n {
        let at = 8 + k * enc;
        let raw = blob.get(at..at + enc)?;
        let v = match enc {
            2 => i16::from_le_bytes(raw.try_into().ok()?) as i64,
            4 => i32::from_le_bytes(raw.try_into().ok()?) as i64,
            8 => i64::from_le_bytes(raw.try_into().ok()?),
            _ => return None,
        };
        out.push(v.to_string().into_bytes());
    }
    Some(out)
}

fn pairs(items: Vec<Vec<u8>>) -> Option<Vec<(Vec<u8>, Vec<u8>)>> {
    if !items.len().is_multiple_of(2) {
        return None;
    }
    let mut it = items.into_iter();
    let mut out = vec![];
    while let (Some(a), Some(b)) = (it.next(), it.next()) {
        out.push((a, b));
    }
    Some(out)
}

fn parse_score(s: &[u8]) -> Option<f64> {
    std::str::from_utf8(s).ok()?.parse().ok()
}

/// A value from a RESTORE payload body (after `unseal`).
pub fn load(body: &[u8], set_lim: Limits, zset_lim: Limits, hash_lim: Limits) -> Option<Data> {
    let mut r = Reader::new(body);
    let ty = r.byte()?;
    let list = |items: Vec<Vec<u8>>| Data::List(items.into_iter().collect::<VecDeque<_>>());
    let set = |items: Vec<Vec<u8>>| {
        let first = items.first().cloned().unwrap_or_default();
        let mut s = super::sets::Set::create(&first, items.len(), set_lim);
        for m in &items {
            s.add(m, set_lim);
        }
        Data::Set(s)
    };
    let zset = |items: Vec<(Vec<u8>, f64)>| {
        let mut z = super::zsets::Zset::default();
        for (m, score) in items {
            z.insert(&m, score, zset_lim);
        }
        Data::Zset(z)
    };
    let hash = |items: Vec<(Vec<u8>, Vec<u8>)>| {
        let mut h = Hash::default();
        for (k, v) in items {
            h.insert(&k, &v, hash_lim);
        }
        Data::Hash(h)
    };
    let data = match ty {
        TYPE_STRING => Data::Str(r.string()?),
        TYPE_LIST => {
            let n = r.len()?;
            let mut items = vec![];
            for _ in 0..n {
                items.push(r.string()?);
            }
            list(items)
        }
        TYPE_SET => {
            let n = r.len()?;
            let mut items = vec![];
            for _ in 0..n {
                items.push(r.string()?);
            }
            set(items)
        }
        TYPE_ZSET | TYPE_ZSET_2 => {
            let n = r.len()?;
            let mut items = vec![];
            for _ in 0..n {
                let m = r.string()?;
                let score = if ty == TYPE_ZSET_2 {
                    f64::from_le_bytes(r.take(8)?.try_into().ok()?)
                } else {
                    r.double_text()?
                };
                items.push((m, score));
            }
            zset(items)
        }
        TYPE_HASH => {
            let n = r.len()?;
            let mut items = vec![];
            for _ in 0..n {
                let k = r.string()?;
                let v = r.string()?;
                items.push((k, v));
            }
            hash(items)
        }
        TYPE_LIST_ZIPLIST => list(ziplist(&r.string()?)?),
        TYPE_SET_INTSET => set(intset(&r.string()?)?),
        TYPE_SET_LISTPACK => set(listpack(&r.string()?)?),
        TYPE_ZSET_ZIPLIST | TYPE_ZSET_LISTPACK => {
            let blob = r.string()?;
            let items = if ty == TYPE_ZSET_ZIPLIST { ziplist(&blob)? } else { listpack(&blob)? };
            let mut out = vec![];
            for (m, s) in pairs(items)? {
                out.push((m, parse_score(&s)?));
            }
            zset(out)
        }
        TYPE_HASH_ZIPLIST | TYPE_HASH_LISTPACK => {
            let blob = r.string()?;
            let items = if ty == TYPE_HASH_ZIPLIST { ziplist(&blob)? } else { listpack(&blob)? };
            hash(pairs(items)?)
        }
        TYPE_LIST_QUICKLIST => {
            let n = r.len()?;
            let mut items = vec![];
            for _ in 0..n {
                items.extend(ziplist(&r.string()?)?);
            }
            list(items)
        }
        TYPE_LIST_QUICKLIST_2 => {
            let n = r.len()?;
            let mut items = vec![];
            for _ in 0..n {
                let container = r.len()?;
                let blob = r.string()?;
                if container == 1 {
                    items.push(blob);
                } else {
                    items.extend(listpack(&blob)?);
                }
            }
            list(items)
        }
        TYPE_HASH_ZIPMAP => return None,
        _ => return None,
    };
    r.at_end().then_some(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc64_matches_redis() {
        // crc64.c's self-test value.
        assert_eq!(crc64(b"123456789"), 0xe9c6_d914_c4b8_d9ca);
    }

    #[test]
    fn string_dump_matches_redis() {
        // `SET k v` / `DUMP k` on Redis 7.2.
        let d = dump(&Data::Str(b"v".to_vec())).unwrap();
        assert_eq!(&d[..5], &[0x00, 0x01, b'v', 0x0b, 0x00]);
        assert_eq!(unseal(&d), Some(&d[..3]));
    }

    #[test]
    fn lzf_roundtrip_of_a_known_block() {
        // "aaaaaaaaaa" compressed by liblzf: literal 'a', back-reference.
        let c = [0x00, b'a', 0xE0, 0x01, 0x00];
        assert_eq!(lzf_decompress(&c, 11).as_deref(), Some(&b"aaaaaaaaaaa"[..]));
    }
}
