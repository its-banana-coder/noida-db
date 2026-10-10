//! Time-series indices (`index.mode: time_series`, TSDB): the settings and
//! mappings such an index requires, the `_tsid` (time series id) and `_id`
//! a document gets from its dimensions and `@timestamp`, and the
//! operations a time-series index refuses (routing, updates, ...).
//!
//! The ids are Elasticsearch 8.15's byte for byte, so clients that compute
//! or remember them (and tests that pin them) see the same values: the
//! `_tsid` is `TimeSeriesIdFieldMapper`'s hashed form, the `_id` is
//! `TsidExtractingIdFieldMapper`'s routing hash + tsid hash + timestamp.

use serde_json::{Map, Value, json};

use super::dates;
use super::search::raw_values;

type Fail = (u16, Value);

const ROUTING_PATH_MSG: &str = "All fields that match routing_path must be configured with \
     [time_series_dimension: true] or flattened fields with a list of dimensions in \
     [time_series_dimensions] and without the [script] parameter.";

// --- errors -------------------------------------------------------------

fn fail(kind: &str, reason: &str, status: u16) -> Fail {
    (status, super::engine::error(kind, reason, status))
}

fn bad(reason: &str) -> Fail {
    fail("illegal_argument_exception", reason, 400)
}

/// An error whose root cause is `kind` but which carries a `caused_by`.
fn fail_caused(kind: &str, reason: &str, cause: (&str, &str), status: u16) -> Fail {
    let mut e = super::engine::error(kind, reason, status);
    e["error"]["caused_by"] = json!({"type": cause.0, "reason": cause.1});
    (status, e)
}

/// `Failed to parse mapping: <reason>`, as a mapping Elasticsearch
/// rejects while parsing it.
fn mapping_failure(reason: &str) -> Fail {
    fail_caused(
        "mapper_parsing_exception",
        &format!("Failed to parse mapping: {reason}"),
        ("illegal_argument_exception", reason),
        400,
    )
}

/// The parser position Elasticsearch reports for a document-level
/// failure: the end of the document (`[line:column]`).
fn doc_position(body: &[u8]) -> String {
    let text = String::from_utf8_lossy(body);
    let text = text.trim_end();
    let line = text.matches('\n').count() + 1;
    let col = text.rsplit('\n').next().map_or(0, |l| l.chars().count());
    format!("[{line}:{col}]")
}

/// A `document_parsing_exception` for `reason` (an
/// `illegal_argument_exception` underneath).
fn parse_failure(body: &[u8], reason: &str) -> Fail {
    let outer = format!("{} failed to parse: {reason}", doc_position(body));
    fail_caused("document_parsing_exception", &outer, ("illegal_argument_exception", reason), 400)
}

// --- hashing and encoding ---------------------------------------------

/// Lucene's `StringHelper.murmurhash3_x86_32`.
pub(super) fn murmur3_32(data: &[u8], seed: u32) -> i32 {
    const C1: u32 = 0xcc9e_2d51;
    const C2: u32 = 0x1b87_3593;
    let mut h = seed;
    let blocks = data.len() / 4;
    for i in 0..blocks {
        let mut k = u32::from_le_bytes(data[i * 4..i * 4 + 4].try_into().unwrap());
        k = k.wrapping_mul(C1).rotate_left(15).wrapping_mul(C2);
        h ^= k;
        h = h.rotate_left(13).wrapping_mul(5).wrapping_add(0xe654_6b64);
    }
    let tail = &data[blocks * 4..];
    let mut k = 0u32;
    if tail.len() >= 3 {
        k ^= u32::from(tail[2]) << 16;
    }
    if tail.len() >= 2 {
        k ^= u32::from(tail[1]) << 8;
    }
    if !tail.is_empty() {
        k ^= u32::from(tail[0]);
        h ^= k.wrapping_mul(C1).rotate_left(15).wrapping_mul(C2);
    }
    h ^= data.len() as u32;
    h ^= h >> 16;
    h = h.wrapping_mul(0x85eb_ca6b);
    h ^= h >> 13;
    h = h.wrapping_mul(0xc2b2_ae35);
    h ^= h >> 16;
    h as i32
}

fn fmix64(mut k: u64) -> u64 {
    k ^= k >> 33;
    k = k.wrapping_mul(0xff51_afd7_ed55_8ccd);
    k ^= k >> 33;
    k = k.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    k ^= k >> 33;
    k
}

/// Elasticsearch's `MurmurHash3.hash128` (x64, 128-bit): `(h1, h2)`.
pub(super) fn murmur3_128(data: &[u8], seed: u64) -> (u64, u64) {
    const C1: u64 = 0x87c3_7b91_1142_53d5;
    const C2: u64 = 0x4cf5_ad43_2745_937f;
    let (mut h1, mut h2) = (seed, seed);
    let blocks = data.len() / 16;
    for i in 0..blocks {
        let k1 = u64::from_le_bytes(data[i * 16..i * 16 + 8].try_into().unwrap());
        let k2 = u64::from_le_bytes(data[i * 16 + 8..i * 16 + 16].try_into().unwrap());
        h1 ^= k1.wrapping_mul(C1).rotate_left(31).wrapping_mul(C2);
        h1 = h1.rotate_left(27).wrapping_add(h2).wrapping_mul(5).wrapping_add(0x52dc_e729);
        h2 ^= k2.wrapping_mul(C2).rotate_left(33).wrapping_mul(C1);
        h2 = h2.rotate_left(31).wrapping_add(h1).wrapping_mul(5).wrapping_add(0x3849_5ab5);
    }
    let tail = &data[blocks * 16..];
    let (mut k1, mut k2) = (0u64, 0u64);
    for (i, b) in tail.iter().enumerate() {
        if i < 8 {
            k1 ^= u64::from(*b) << (i * 8);
        } else {
            k2 ^= u64::from(*b) << ((i - 8) * 8);
        }
    }
    if tail.len() > 8 {
        h2 ^= k2.wrapping_mul(C2).rotate_left(33).wrapping_mul(C1);
    }
    if !tail.is_empty() {
        h1 ^= k1.wrapping_mul(C1).rotate_left(31).wrapping_mul(C2);
    }
    h1 ^= data.len() as u64;
    h2 ^= data.len() as u64;
    h1 = h1.wrapping_add(h2);
    h2 = h2.wrapping_add(h1);
    h1 = fmix64(h1);
    h2 = fmix64(h2);
    h1 = h1.wrapping_add(h2);
    h2 = h2.wrapping_add(h1);
    (h1, h2)
}

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

/// URL-safe base64 without padding (`Base64.getUrlEncoder().withoutPadding()`).
pub(super) fn b64(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let n = (u32::from(chunk[0]) << 16)
            | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
            | u32::from(*chunk.get(2).unwrap_or(&0));
        for i in 0..=chunk.len() {
            out.push(B64[(n >> (18 - 6 * i) & 63) as usize] as char);
        }
    }
    out
}

/// Java's URL-safe base64 decoder (padding optional).
pub(super) fn unb64(s: &str) -> Option<Vec<u8>> {
    let s = s.trim_end_matches('=');
    if s.len() % 4 == 1 {
        return None;
    }
    let mut out = Vec::with_capacity(s.len() * 3 / 4);
    let (mut acc, mut bits) = (0u32, 0u32);
    for c in s.bytes() {
        let v = B64.iter().position(|&b| b == c)? as u32;
        acc = (acc << 6) | v;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
            acc &= (1 << bits) - 1;
        }
    }
    Some(out)
}

fn put_vint(out: &mut Vec<u8>, mut n: u32) {
    while n >= 0x80 {
        out.push((n as u8 & 0x7f) | 0x80);
        n >>= 7;
    }
    out.push(n as u8);
}

/// The size of the array behind a `BytesStreamOutput` holding `len`
/// bytes: it grows by Lucene's `ArrayUtil.oversize` steps. Elasticsearch
/// hashes the whole array (zero padding included) into the `_tsid`, so
/// the padding is part of the id.
fn stream_capacity(writes: &[usize]) -> usize {
    let mut cap = 0usize;
    let mut len = 0usize;
    for w in writes {
        len += w;
        if len > cap {
            let extra = (len >> 3).max(3);
            cap = if len < 16384 { ((len + extra + 7) & !7).min(16384) } else { len };
        }
    }
    cap
}

/// One serialized dimension value: the bytes and the zero-padded array
/// they live in.
struct DimValue {
    bytes: Vec<u8>,
    padded: Vec<u8>,
}

fn string_dim(s: &str) -> DimValue {
    let utf8 = s.as_bytes();
    let mut bytes = vec![b's'];
    let before = bytes.len();
    put_vint(&mut bytes, utf8.len() as u32);
    let vint_len = bytes.len() - before;
    bytes.extend_from_slice(utf8);
    // `writeVInt` writes a one-byte vint as a byte, a longer one as a block.
    let mut writes = vec![1, vint_len];
    if !utf8.is_empty() {
        writes.push(utf8.len());
    }
    padded(bytes, &writes)
}

fn long_dim(tag: u8, v: i64) -> DimValue {
    let mut bytes = vec![tag];
    bytes.extend_from_slice(&v.to_be_bytes());
    padded(bytes, &[1, 8])
}

fn padded(bytes: Vec<u8>, writes: &[usize]) -> DimValue {
    let mut p = bytes.clone();
    p.resize(stream_capacity(writes).max(bytes.len()), 0);
    DimValue { bytes, padded: p }
}

/// The `_tsid` of a set of dimensions (name, value), Elasticsearch's
/// `TimeSeriesIdBuilder.buildTsidHash`: a vint length, then
/// hash128(names) + hash32 per value + hash128(values).
fn tsid_bytes(mut dims: Vec<(String, DimValue)>) -> Vec<u8> {
    dims.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    let n = dims.len().min(512);
    let len = 16 + 16 + 4 * n;
    let mut out = Vec::with_capacity(len + 2);
    put_vint(&mut out, len as u32);
    // A field name's `BytesRef` array is sized for the worst-case UTF-8
    // (3 bytes per UTF-16 unit), and the whole array is hashed.
    let mut names = Vec::new();
    for (name, _) in &dims {
        let mut b = name.as_bytes().to_vec();
        b.resize((name.encode_utf16().count() * 3).max(b.len()), 0);
        names.extend(b);
    }
    let (h1, h2) = murmur3_128(&names, 0);
    out.extend(h1.to_le_bytes());
    out.extend(h2.to_le_bytes());
    for (_, v) in dims.iter().take(n) {
        out.extend(murmur3_32(&v.bytes, 0).to_le_bytes());
    }
    let values: Vec<u8> = dims.iter().flat_map(|(_, v)| v.padded.iter().copied()).collect();
    let (h1, h2) = murmur3_128(&values, 0);
    out.extend(h1.to_le_bytes());
    out.extend(h2.to_le_bytes());
    out
}

/// The routing hash of (path, value) pairs taken from `_source`
/// (`IndexRouting.ExtractFromSource.Builder.buildHash`).
fn routing_hash(mut pairs: Vec<(String, String)>) -> Result<i32, String> {
    pairs.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    let mut hash: Option<i32> = None;
    for (i, (name, value)) in pairs.iter().enumerate() {
        if i > 0 && pairs[i - 1].0 == *name {
            return Err(format!("Duplicate routing dimension for [{name}]"));
        }
        let this = murmur3_32(name.as_bytes(), 0) ^ murmur3_32(value.as_bytes(), 0);
        hash = Some(match hash {
            None => this,
            Some(h) => h.wrapping_mul(31).wrapping_add(this),
        });
    }
    hash.ok_or_else(|| "source didn't contain any routing fields".to_string())
}

/// `TsidExtractingIdFieldMapper.createId`.
fn make_id(routing: i32, tsid: &[u8], timestamp: i64) -> String {
    let (h1, _) = murmur3_128(tsid, 0);
    let mut bytes = Vec::with_capacity(20);
    bytes.extend(routing.to_le_bytes());
    bytes.extend(h1.to_le_bytes());
    bytes.extend(timestamp.to_be_bytes());
    b64(&bytes)
}

/// `_ts_routing_hash` of a document: the first four bytes of its `_id`.
pub(super) fn routing_hash_of_id(id: &str) -> Option<String> {
    let bytes = unb64(id)?;
    (bytes.len() >= 4).then(|| b64(&bytes[..4]))
}

// --- instants -----------------------------------------------------------

const NANOS: i128 = 1_000_000_000;

/// An ISO-8601 date-time (`2021-04-28T00:00:00Z`, `-9999-01-01T00:00:00Z`,
/// `2021-09-26T03:09:42.123456789Z`, `2021-04-28`) or epoch milliseconds,
/// as nanoseconds since the epoch.
fn parse_instant(s: &str) -> Option<i128> {
    let s = s.trim();
    if !s.is_empty() && s.trim_start_matches('-').bytes().all(|c| c.is_ascii_digit()) {
        return s.parse::<i128>().ok().map(|ms| ms * 1_000_000);
    }
    let b = s.as_bytes();
    let mut i = 0;
    let neg = match b.first() {
        Some(b'-') => {
            i = 1;
            true
        }
        Some(b'+') => {
            i = 1;
            false
        }
        _ => false,
    };
    let start = i;
    while b.get(i).is_some_and(u8::is_ascii_digit) {
        i += 1;
    }
    if i - start < 4 {
        return None;
    }
    let year: i64 = s[start..i].parse().ok()?;
    let year = if neg { -year } else { year };
    let num = |at: usize| -> Option<i64> {
        let part = s.get(at..at + 2)?;
        part.bytes().all(|c| c.is_ascii_digit()).then(|| part.parse().ok())?
    };
    let (mut mo, mut d, mut h, mut mi, mut sec, mut frac) = (1, 1, 0, 0, 0, 0i128);
    if b.get(i) == Some(&b'-') {
        mo = num(i + 1)?;
        i += 3;
        if b.get(i) == Some(&b'-') {
            d = num(i + 1)?;
            i += 3;
            if matches!(b.get(i), Some(b'T' | b't')) {
                h = num(i + 1)?;
                i += 3;
                if b.get(i) == Some(&b':') {
                    mi = num(i + 1)?;
                    i += 3;
                    if b.get(i) == Some(&b':') {
                        sec = num(i + 1)?;
                        i += 3;
                        if matches!(b.get(i), Some(b'.' | b',')) {
                            let from = i + 1;
                            i = from;
                            while b.get(i).is_some_and(u8::is_ascii_digit) {
                                i += 1;
                            }
                            if i == from || i - from > 9 {
                                return None;
                            }
                            frac = format!("{:0<9}", &s[from..i]).parse().ok()?;
                        }
                    }
                }
            }
        }
    }
    if !(1..=12).contains(&mo) || !(1..=31).contains(&d) || h > 23 || mi > 59 || sec > 59 {
        return None;
    }
    let offset_ms = if i == s.len() { 0 } else { dates::parse_offset(&s[i..])? };
    let days = dates::days_from_civil(year, mo, d);
    let secs = i128::from(days) * 86_400 + i128::from(h * 3600 + mi * 60 + sec);
    Some(secs * NANOS + frac - i128::from(offset_ms) * 1_000_000)
}

/// Java's `Instant.toString()`: seconds always, the fraction in groups of
/// three digits, `+` before a five-digit year.
fn instant_string(nanos: i128) -> String {
    let secs = nanos.div_euclid(NANOS);
    let frac = nanos.rem_euclid(NANOS);
    let days = secs.div_euclid(86_400) as i64;
    let rem = secs.rem_euclid(86_400) as i64;
    let (y, mo, d) = dates::civil_from_days(days);
    let year = if y > 9999 {
        format!("+{y}")
    } else if y < 0 {
        format!("-{:04}", -y)
    } else {
        format!("{y:04}")
    };
    let fraction = if frac == 0 {
        String::new()
    } else if frac % 1_000_000 == 0 {
        format!(".{:03}", frac / 1_000_000)
    } else if frac % 1000 == 0 {
        format!(".{:06}", frac / 1000)
    } else {
        format!(".{frac:09}")
    };
    format!(
        "{year}-{mo:02}-{d:02}T{:02}:{:02}:{:02}{fraction}Z",
        rem / 3600,
        rem / 60 % 60,
        rem % 60
    )
}

fn setting_text(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

// --- settings -----------------------------------------------------------

/// An `index.*` setting as stored (nested objects, `index.` prefix).
fn index_setting<'a>(settings: &'a Value, path: &str) -> Option<&'a Value> {
    let mut node = settings.get("index")?;
    for seg in path.split('.') {
        node = node.get(seg)?;
    }
    Some(node).filter(|v| !v.is_null())
}

/// Whether the index is a time-series index.
pub(super) fn is_time_series(settings: &Value) -> bool {
    index_setting(settings, "mode")
        .and_then(Value::as_str)
        .is_some_and(|m| m.eq_ignore_ascii_case("time_series"))
}

/// `index.routing_path`: a list, or a comma-separated string.
fn routing_path(settings: &Value) -> Vec<String> {
    match index_setting(settings, "routing_path") {
        Some(Value::Array(a)) => a.iter().filter_map(setting_text).collect(),
        Some(Value::String(s)) => {
            s.split(',').map(str::trim).filter(|p| !p.is_empty()).map(String::from).collect()
        }
        _ => Vec::new(),
    }
}

/// `index.time_series.start_time` / `end_time` in nanoseconds (the
/// defaults span every representable date).
fn time_bound(settings: &Value, which: &str) -> Result<i128, Fail> {
    match index_setting(settings, &format!("time_series.{which}")).and_then(setting_text) {
        None => Ok(if which == "start_time" {
            parse_instant("-9999-01-01T00:00:00Z").unwrap_or(i128::MIN)
        } else {
            parse_instant("9999-12-31T23:59:59.999Z").unwrap_or(i128::MAX)
        }),
        Some(s) if s.trim().is_empty() => Err(bad("cannot parse empty datetime")),
        Some(s) => parse_instant(&s).ok_or_else(|| {
            bad(&format!(
                "failed to parse date field [{s}] with format [strict_date_optional_time]"
            ))
        }),
    }
}

/// The checks Elasticsearch makes on a new index's settings: what
/// `index.mode` allows and requires.
pub(super) fn validate_new_settings(settings: &Value) -> Result<(), Fail> {
    if let Some(mode) = index_setting(settings, "mode").and_then(setting_text)
        && !matches!(mode.to_ascii_lowercase().as_str(), "standard" | "time_series" | "logsdb")
    {
        return Err(bad(&format!(
            "No enum constant org.elasticsearch.index.IndexMode.{}",
            mode.to_ascii_uppercase()
        )));
    }
    let tsdb = is_time_series(settings);
    for which in ["start_time", "end_time"] {
        if index_setting(settings, &format!("time_series.{which}")).is_some() {
            time_bound(settings, which)?;
            if !tsdb {
                return Err(bad(&format!(
                    "[index.time_series.{which}] requires [index.mode=time_series]"
                )));
            }
        }
    }
    if !tsdb {
        if index_setting(settings, "routing_path").is_some() {
            return Err(bad("[index.routing_path] requires [index.mode=time_series]"));
        }
        return Ok(());
    }
    for s in ["sort.field", "sort.order", "sort.mode", "sort.missing", "routing_partition_size"] {
        if index_setting(settings, s).is_some() {
            return Err(bad(&format!("[index.mode=time_series] is incompatible with [index.{s}]")));
        }
    }
    if routing_path(settings).is_empty() {
        return Err(bad("[index.mode=time_series] requires a non-empty [index.routing_path]"));
    }
    Ok(())
}

/// The checks on `PUT <index>/_settings` for the time-series settings:
/// `end_time` may only grow, `start_time` and `routing_path` are fixed.
pub(super) fn validate_settings_update(
    index: &str,
    current: &Value,
    open: bool,
    flat: &[(String, Value)],
) -> Result<(), Fail> {
    let tsdb = is_time_series(current);
    for (key, v) in flat {
        let key = if key.starts_with("index.") { key.clone() } else { format!("index.{key}") };
        match key.as_str() {
            "index.time_series.end_time" => {
                if !tsdb {
                    return Err(bad(
                        "[index.time_series.end_time] requires [index.mode=time_series]",
                    ));
                }
                let Some(text) = setting_text(v) else { continue };
                let new = parse_instant(&text)
                    .ok_or_else(|| bad(&format!("failed to parse date field [{text}]")))?;
                if let Some(cur) =
                    index_setting(current, "time_series.end_time").and_then(setting_text)
                    && let Some(old) = parse_instant(&cur)
                    && new <= old
                {
                    return Err(bad(&format!(
                        "index.time_series.end_time must be larger than current value [{cur}] but was [{}]",
                        instant_string(new)
                    )));
                }
            }
            "index.time_series.start_time" | "index.routing_path" if !open => {
                return Err(bad(&format!("final {index} setting [{key}], not updateable")));
            }
            _ => {}
        }
    }
    Ok(())
}

// --- mappings -----------------------------------------------------------

/// A mapped field: its full dotted name, definition, and whether it sits
/// inside a `nested` object.
struct Mapped<'a> {
    name: String,
    def: &'a Value,
    in_nested: bool,
}

fn type_of(def: &Value) -> &str {
    def.get("type").and_then(Value::as_str).unwrap_or("object")
}

fn walk<'a>(props: Option<&'a Value>, prefix: &str, in_nested: bool, out: &mut Vec<Mapped<'a>>) {
    let Some(Value::Object(m)) = props else { return };
    for (k, def) in m {
        let name = if prefix.is_empty() { k.clone() } else { format!("{prefix}.{k}") };
        out.push(Mapped { name: name.clone(), def, in_nested });
        let nested = in_nested || type_of(def) == "nested";
        walk(def.get("properties"), &name, nested, out);
    }
}

fn all_fields(mappings: &Value) -> Vec<Mapped<'_>> {
    let mut out = Vec::new();
    walk(mappings.get("properties"), "", false, &mut out);
    out
}

fn is_dimension(def: &Value) -> bool {
    def.get("time_series_dimension").is_some_and(|v| v == &json!(true) || v == "true")
}

/// `Regex.simpleMatch`: `*` matches any run of characters (dots too).
fn simple_match(pattern: &str, s: &str) -> bool {
    let parts: Vec<&str> = pattern.split('*').collect();
    if parts.len() == 1 {
        return pattern == s;
    }
    let mut rest = s;
    for (i, p) in parts.iter().enumerate() {
        if i == 0 {
            let Some(r) = rest.strip_prefix(p) else { return false };
            rest = r;
        } else if i == parts.len() - 1 {
            return rest.ends_with(p);
        } else if let Some(at) = rest.find(p) {
            rest = &rest[at + p.len()..];
        } else {
            return false;
        }
    }
    true
}

fn matches_routing_path(patterns: &[String], name: &str) -> bool {
    patterns.iter().any(|p| simple_match(p, name))
}

/// A runtime field may not shadow a dimension or metric.
fn shadowing(mappings: &Value, runtime: &Value) -> Option<String> {
    let m = runtime.as_object()?;
    for f in all_fields(mappings) {
        if !m.contains_key(&f.name) {
            continue;
        }
        if is_dimension(f.def) {
            return Some(format!("Field [{}] attempted to shadow a time_series_dimension", f.name));
        }
        if f.def.get("time_series_metric").is_some() {
            return Some(format!("Field [{}] attempted to shadow a time_series_metric", f.name));
        }
    }
    None
}

/// Mapping checks for every index: dimensions can't live in `nested`
/// objects, runtime fields take no time-series parameters and don't
/// shadow dimensions or metrics.
pub(super) fn validate_mapping(mappings: &Value) -> Result<(), Fail> {
    for f in all_fields(mappings) {
        if f.in_nested && is_dimension(f.def) {
            return Err(bad(&format!(
                "time_series_dimension can't be configured in nested field [{}]",
                f.name
            )));
        }
    }
    if let Some(Value::Object(rt)) = mappings.get("runtime") {
        for (name, def) in rt {
            for param in ["time_series_dimension", "time_series_metric"] {
                if def.get(param).is_some() {
                    let reason = format!(
                        "unknown parameter [{param}] on runtime field [{name}] of type [{}]",
                        def.get("type").and_then(Value::as_str).unwrap_or("keyword")
                    );
                    let (status, mut e) = fail_caused(
                        "mapper_parsing_exception",
                        &format!("Failed to parse mapping: {reason}"),
                        ("mapper_parsing_exception", &reason),
                        400,
                    );
                    e["error"]["root_cause"][0]["reason"] = json!(reason);
                    return Err((status, e));
                }
            }
        }
    }
    if let Some(reason) = mappings.get("runtime").and_then(|rt| shadowing(mappings, rt)) {
        return Err(fail("mapper_parsing_exception", &reason, 400));
    }
    Ok(())
}

/// A mapping update may not change a field's `time_series_dimension` or
/// `time_series_metric`.
pub(super) fn check_param_updates(current: &Value, incoming: &Value) -> Result<(), Fail> {
    fn walk(cur: Option<&Value>, inc: &Value, prefix: &str) -> Result<(), Fail> {
        let Some(props) = inc.get("properties").and_then(Value::as_object) else { return Ok(()) };
        for (k, def) in props {
            let name = if prefix.is_empty() { k.clone() } else { format!("{prefix}.{k}") };
            let old = cur.and_then(|c| c.get("properties")).and_then(|p| p.get(k));
            if let Some(old) = old
                && def.get("type").is_some()
                && old.get("type").is_some()
            {
                let conflict = |param: &str, from: String, to: String| {
                    bad(&format!(
                        "Mapper for [{name}] conflicts with existing mapper:\n\tCannot update parameter [{param}] from [{from}] to [{to}]"
                    ))
                };
                let (was, is) = (is_dimension(old), is_dimension(def));
                if was != is {
                    return Err(conflict("time_series_dimension", was.to_string(), is.to_string()));
                }
                let metric = |d: &Value| {
                    d.get("time_series_metric")
                        .and_then(Value::as_str)
                        .unwrap_or("null")
                        .to_string()
                };
                if metric(old) != metric(def) {
                    return Err(conflict("time_series_metric", metric(old), metric(def)));
                }
            }
            walk(old, def, &name)?;
        }
        Ok(())
    }
    walk(Some(current), incoming, "")
}

/// A new time-series index's mapping: checked the way Elasticsearch
/// checks it, then given what the index mode adds (`@timestamp` as a
/// `date`, the `_data_stream_timestamp` meta field, synthetic `_source`).
pub(super) fn prepare_new_mapping(settings: &Value, mappings: &mut Value) -> Result<(), Fail> {
    if !is_time_series(settings) {
        return Ok(());
    }
    if mappings.get("_routing").and_then(|r| r.get("required")).and_then(Value::as_bool)
        == Some(true)
    {
        return Err(bad(
            "routing is forbidden on CRUD operations that target indices in [index.mode=time_series]",
        ));
    }
    match mappings.get("_data_stream_timestamp") {
        None => {}
        Some(Value::Object(m)) => {
            if m.get("enabled").is_some_and(|v| v == &json!(false) || v == "false") {
                return Err(fail(
                    "illegal_state_exception",
                    "[_data_stream_timestamp] meta field has been disabled",
                    500,
                ));
            }
        }
        Some(_) => {
            return Err(mapping_failure("[_data_stream_timestamp] config must be an object"));
        }
    }
    if let Some(src) = mappings.get("_source").and_then(Value::as_object) {
        if src.get("enabled").is_some_and(|v| v == &json!(false) || v == "false") {
            let reason = "Indices with with index mode [time_series] only support synthetic source";
            let (status, mut e) = fail_caused(
                "mapper_parsing_exception",
                &format!("Failed to parse mapping: {reason}"),
                ("mapper_parsing_exception", reason),
                400,
            );
            e["error"]["root_cause"][0]["reason"] = json!(reason);
            return Err((status, e));
        }
        match src.get("mode").and_then(Value::as_str) {
            Some("stored" | "disabled") => {
                return Err(mapping_failure("time series indices only support synthetic source"));
            }
            _ => {}
        }
        if src.contains_key("includes") || src.contains_key("excludes") {
            return Err(mapping_failure(
                "filtering the stored _source is incompatible with synthetic source",
            ));
        }
    }
    if mappings.get("runtime").and_then(|r| r.get("@timestamp")).is_some() {
        return Err(fail_caused(
            "illegal_argument_exception",
            "docvalues not found for index sort field:[@timestamp]",
            ("unsupported_operation_exception", "Runtime fields not supported for [index sort]"),
            400,
        ));
    }
    if !mappings.get("properties").is_some_and(Value::is_object) {
        mappings["properties"] = json!({});
    }
    match mappings["properties"].get("@timestamp") {
        None => mappings["properties"]["@timestamp"] = json!({"type": "date"}),
        Some(def) => {
            let ty = type_of(def);
            if !matches!(ty, "date" | "date_nanos") {
                return Err(bad(&format!(
                    "data stream timestamp field [@timestamp] is of type [{ty}], but [date,date_nanos] is expected"
                )));
            }
        }
    }
    validate_tsdb_mapping(settings, mappings)?;
    mappings["_data_stream_timestamp"] = json!({"enabled": true});
    let mut source = json!({"mode": "synthetic"});
    if let Some(Value::Object(old)) = mappings.get("_source") {
        for (k, v) in old {
            source[k] = v.clone();
        }
    }
    mappings["_source"] = source;
    Ok(())
}

/// The checks a time-series index's mapping must pass on every change:
/// no `nested` fields, and every field `routing_path` matches is a
/// dimension.
pub(super) fn validate_tsdb_mapping(settings: &Value, mappings: &Value) -> Result<(), Fail> {
    if !is_time_series(settings) {
        return Ok(());
    }
    let fields = all_fields(mappings);
    if fields.iter().any(|f| type_of(f.def) == "nested") {
        return Err(bad("cannot have nested fields when index is in [index.mode=time_series]"));
    }
    let patterns = routing_path(settings);
    for f in &fields {
        let ty = type_of(f.def);
        if ty == "object" {
            if patterns.iter().any(|p| p == &f.name) {
                return Err(bad(&format!("{ROUTING_PATH_MSG} [{}] was [object].", f.name)));
            }
            continue;
        }
        if ty == "flattened" {
            continue;
        }
        if matches_routing_path(&patterns, &f.name) && !is_dimension(f.def) {
            return Err(bad(&format!("{ROUTING_PATH_MSG} [{}] was not a dimension.", f.name)));
        }
    }
    if let Some(Value::Object(rt)) = mappings.get("runtime") {
        for (name, def) in rt {
            if matches_routing_path(&patterns, name) {
                let ty = def.get("type").and_then(Value::as_str).unwrap_or("keyword");
                return Err(bad(&format!("{ROUTING_PATH_MSG} [{name}] was a runtime [{ty}].")));
            }
        }
    }
    Ok(())
}

/// The queries inside `filter` / `filters` aggregations can't use `_tsid`
/// either.
fn check_agg_queries(aggs: &Value) -> Result<(), super::search::EsError> {
    let not_searchable = || {
        super::search::EsError::shard_failure(
            "illegal_argument_exception",
            "[_tsid] is not searchable",
        )
    };
    for agg in aggs.as_object().into_iter().flat_map(|m| m.values()) {
        for (k, body) in agg.as_object().into_iter().flatten() {
            let queries: Vec<&Value> = match k.as_str() {
                "aggs" | "aggregations" => {
                    check_agg_queries(body)?;
                    continue;
                }
                "filter" => vec![body],
                "filters" => match body.get("filters") {
                    Some(Value::Object(m)) => m.values().collect(),
                    Some(Value::Array(a)) => a.iter().collect(),
                    _ => Vec::new(),
                },
                _ => continue,
            };
            for q in queries {
                if let Some(m) = q.as_object()
                    && check_query(m).is_err()
                {
                    return Err(not_searchable());
                }
            }
        }
    }
    Ok(())
}

/// A numeric aggregation (`avg`, `sum`, ...) over a `position` metric:
/// (field, aggregation type).
fn position_agg(mappings: &Value, aggs: &Value) -> Option<(String, String)> {
    const NUMERIC: &[&str] = &[
        "avg",
        "sum",
        "min",
        "max",
        "stats",
        "extended_stats",
        "percentiles",
        "percentile_ranks",
        "median_absolute_deviation",
        "histogram",
    ];
    for agg in aggs.as_object()?.values() {
        for (k, body) in agg.as_object().into_iter().flatten() {
            if matches!(k.as_str(), "aggs" | "aggregations") {
                if let Some(found) = position_agg(mappings, body) {
                    return Some(found);
                }
                continue;
            }
            let Some(field) = body.get("field").and_then(Value::as_str) else { continue };
            if !NUMERIC.contains(&k.as_str()) {
                continue;
            }
            let position = all_fields(mappings).iter().any(|f| {
                f.name == field
                    && type_of(f.def) == "geo_point"
                    && f.def.get("time_series_metric").and_then(Value::as_str) == Some("position")
            });
            if position {
                return Some((field.to_string(), k.clone()));
            }
        }
    }
    None
}

/// Whether a search request sorts or aggregates on `_id`.
fn uses_id_doc_values(req: &Value) -> bool {
    fn in_aggs(aggs: &Value) -> bool {
        aggs.as_object().is_some_and(|m| {
            m.values().any(|agg| {
                agg.as_object().is_some_and(|a| {
                    a.iter().any(|(k, body)| match k.as_str() {
                        "aggs" | "aggregations" => in_aggs(body),
                        _ => body.get("field").and_then(Value::as_str) == Some("_id"),
                    })
                })
            })
        })
    }
    let sorts: Vec<&Value> = match req.get("sort") {
        Some(Value::Array(a)) => a.iter().collect(),
        Some(v) => vec![v],
        None => Vec::new(),
    };
    let sorts_id = sorts.iter().any(|s| match s {
        Value::String(f) => f == "_id",
        Value::Object(m) => m.contains_key("_id"),
        _ => false,
    });
    sorts_id || req.get("aggs").or_else(|| req.get("aggregations")).is_some_and(in_aggs)
}

/// Request-level checks for a search over `targets` (name, settings,
/// mappings): no `routing` and no `_id` doc values on a time-series
/// index, and no runtime field shadowing a dimension, a metric or the
/// routing path.
pub(super) fn check_search(
    targets: &[(&str, &Value, &Value)],
    routing: bool,
    req: &Value,
) -> Result<(), Fail> {
    let shard = |kind: &str, reason: &str| {
        let e = super::search::EsError::shard_failure(kind, reason);
        (e.status, e.to_json())
    };
    for (name, settings, mappings) in targets {
        let ts = is_time_series(settings);
        if ts && routing {
            return Err(search_routing_error(name));
        }
        if let Some(rt) = req.get("runtime_mappings") {
            if let Some(reason) = shadowing(mappings, rt) {
                return Err(shard("mapper_parsing_exception", &reason));
            }
            if ts && let Some(m) = rt.as_object() {
                let patterns = routing_path(settings);
                if let Some(name) = m.keys().find(|n| matches_routing_path(&patterns, n)) {
                    return Err(shard(
                        "illegal_argument_exception",
                        &format!(
                            "runtime fields may not match [routing_path] but [{name}] matched"
                        ),
                    ));
                }
            }
        }
        if ts && let Some(aggs) = req.get("aggs").or_else(|| req.get("aggregations")) {
            check_agg_queries(aggs).map_err(|e| (e.status, e.to_json()))?;
        }
        if ts
            && let Some(aggs) = req.get("aggs").or_else(|| req.get("aggregations"))
            && let Some((field, agg)) = position_agg(mappings, aggs)
        {
            return Err(shard(
                "illegal_argument_exception",
                &format!(
                    "Field [{field}] of type [geo_point][position] is not supported for aggregation [{agg}]"
                ),
            ));
        }
        if ts && uses_id_doc_values(req) {
            return Err(shard(
                "illegal_argument_exception",
                "Fielddata is not supported on [_id] field in [time_series] indices",
            ));
        }
    }
    Ok(())
}

// --- documents ----------------------------------------------------------

/// What a time-series document is indexed under.
pub(super) struct TsDoc {
    pub id: String,
    pub tsid: String,
    /// `@timestamp` in epoch milliseconds.
    pub millis: i64,
}

impl TsDoc {
    /// How Elasticsearch names the document in a version conflict:
    /// `[<_id>][<_tsid>@<@timestamp>]`.
    pub fn description(&self) -> String {
        let tsid = if self.tsid.len() > 1000 {
            format!("{}...", &self.tsid[..1000])
        } else {
            self.tsid.clone()
        };
        format!("[{}][{tsid}@{}]", self.id, dates::format(self.millis, None, 0))
    }
}

/// Every leaf of `source` as (dotted path, value); arrays are leaves.
fn source_leaves<'a>(v: &'a Value, prefix: &str, out: &mut Vec<(String, &'a Value)>) {
    if let Value::Object(m) = v {
        for (k, x) in m {
            let name = if prefix.is_empty() { k.clone() } else { format!("{prefix}.{k}") };
            if x.is_object() {
                source_leaves(x, &name, out);
            } else {
                out.push((name, x));
            }
        }
    }
}

/// The routing values of a document: every `_source` leaf a
/// `routing_path` entry matches (an object matched as a whole contributes
/// all its leaves).
fn routing_values(patterns: &[String], source: &Value) -> Result<Vec<(String, String)>, Fail> {
    let mut leaves = Vec::new();
    source_leaves(source, "", &mut leaves);
    let mut out = Vec::new();
    for (path, v) in leaves {
        let segs: Vec<&str> = path.split('.').collect();
        let matched =
            (1..=segs.len()).any(|n| matches_routing_path(patterns, &segs[..n].join(".")));
        if !matched {
            continue;
        }
        let token = match v {
            Value::String(s) => s.clone(),
            Value::Number(n) => n.to_string(),
            Value::Null => continue,
            Value::Bool(_) => "VALUE_BOOLEAN".to_string(),
            _ => "START_ARRAY".to_string(),
        };
        if !matches!(v, Value::String(_) | Value::Number(_)) {
            let inner = format!("Routing values must be strings but found [{token}]");
            return Err(fail_caused(
                "illegal_argument_exception",
                &format!("Error extracting routing: {inner}"),
                ("parsing_exception", &inner),
                400,
            ));
        }
        out.push((path, token));
    }
    Ok(out)
}

/// The type a `dynamic: runtime` object gives a new field.
fn runtime_type(v: &Value) -> &'static str {
    match v {
        Value::Bool(_) => "boolean",
        Value::Number(n) if n.is_i64() || n.is_u64() => "long",
        Value::Number(_) => "double",
        Value::String(s) if parse_instant(s).is_some() && s.contains('-') => "date",
        _ => "keyword",
    }
}

/// The nearest mapped object above `path` and its `dynamic` setting, for
/// an unmapped routing field.
fn unmapped_dynamic(mappings: &Value, path: &str) -> Option<(String, String)> {
    let segs: Vec<&str> = path.split('.').collect();
    let mut node = mappings;
    let mut dynamic = mappings.get("dynamic").and_then(setting_text);
    for (i, seg) in segs.iter().enumerate() {
        let Some(next) = node.get("properties").and_then(|p| p.get(*seg)) else {
            return dynamic.map(|d| (segs[..=i].join("."), d));
        };
        if let Some(d) = next
            .get("dynamic")
            .and_then(|d| setting_text(d).or_else(|| d.as_bool().map(|b| b.to_string())))
        {
            dynamic = Some(d);
        }
        node = next;
    }
    None
}

/// The dimension values of a document, serialized for the `_tsid`.
fn dimensions(
    mappings: &Value,
    source: &Value,
    body: &[u8],
    millis: i64,
) -> Result<Vec<(String, DimValue)>, Fail> {
    let mut out = Vec::new();
    let field_failure = |name: &str, ty: &str, preview: &str, cause: &str| {
        let reason = format!(
            "{} failed to parse field [{name}] of type [{ty}] in a time series document at [{}]. Preview of field's value: '{preview}'",
            doc_position(body),
            dates::format(millis, None, 0)
        );
        fail_caused(
            "document_parsing_exception",
            &reason,
            ("illegal_argument_exception", cause),
            400,
        )
    };
    for f in all_fields(mappings) {
        let ty = type_of(f.def);
        if ty == "geo_point"
            && f.def.get("time_series_metric").and_then(Value::as_str) == Some("position")
        {
            if points_at(source, &f.name) > 1 {
                let reason =
                    format!("field type for [{}] does not accept more than single value", f.name);
                return Err(fail_caused(
                    "document_parsing_exception",
                    &format!("{} failed to parse: {reason}", doc_position(body)),
                    ("parse_exception", &reason),
                    400,
                ));
            }
            continue;
        }
        if ty == "flattened" {
            let keys: Vec<String> = match f.def.get("time_series_dimensions") {
                Some(Value::Array(a)) => a.iter().filter_map(setting_text).collect(),
                _ => continue,
            };
            for key in keys {
                let name = format!("{}.{key}", f.name);
                let vals: Vec<&Value> =
                    raw_values(source, &name).into_iter().filter(|v| !v.is_null()).collect();
                if vals.len() > 1 {
                    let preview = setting_text(vals[1]).unwrap_or_default();
                    return Err(field_failure(
                        &f.name,
                        ty,
                        &preview,
                        &format!("Dimension field [{name}] cannot be a multi-valued field."),
                    ));
                }
                if let Some(v) = vals.first()
                    && let Some(text) = match v {
                        Value::String(s) => Some(s.clone()),
                        Value::Number(n) => Some(n.to_string()),
                        Value::Bool(b) => Some(b.to_string()),
                        _ => None,
                    }
                {
                    out.push((name, string_dim(&text)));
                }
            }
            continue;
        }
        if !is_dimension(f.def) {
            continue;
        }
        let vals: Vec<&Value> =
            raw_values(source, &f.name).into_iter().filter(|v| !v.is_null()).collect();
        if vals.len() > 1 {
            let preview = match vals[1] {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            return Err(field_failure(
                &f.name,
                ty,
                &preview,
                &format!("Dimension field [{}] cannot be a multi-valued field.", f.name),
            ));
        }
        let Some(v) = vals.first() else { continue };
        let value = match ty {
            "keyword" => match v {
                Value::String(s) => string_dim(s),
                Value::Number(n) => string_dim(&n.to_string()),
                Value::Bool(b) => string_dim(&b.to_string()),
                other => {
                    return Err(field_failure(
                        &f.name,
                        ty,
                        &other.to_string(),
                        "Expected text but found START_OBJECT",
                    ));
                }
            },
            "ip" => {
                let text = setting_text(v).unwrap_or_default();
                let Some(ip) = format_ip(&text) else {
                    return Err(field_failure(
                        &f.name,
                        ty,
                        &text,
                        &format!("'{text}' is not an IP string literal."),
                    ));
                };
                string_dim(&ip)
            }
            "long" | "integer" | "short" | "byte" => {
                let n = match v {
                    Value::Number(n) => n.as_i64().or_else(|| n.as_f64().map(|f| f.trunc() as i64)),
                    Value::String(s) => s.trim().parse::<i64>().ok().or_else(|| {
                        s.trim()
                            .parse::<f64>()
                            .ok()
                            .filter(|f| f.is_finite())
                            .map(|f| f.trunc() as i64)
                    }),
                    _ => None,
                };
                let Some(n) = n else {
                    let text = setting_text(v).unwrap_or_else(|| v.to_string());
                    return Err(field_failure(
                        &f.name,
                        ty,
                        &text,
                        &format!("For input string: \"{text}\""),
                    ));
                };
                long_dim(b'l', n)
            }
            "unsigned_long" => {
                let n = match v {
                    Value::Number(n) => n.as_u64(),
                    Value::String(s) => s.trim().parse::<u64>().ok(),
                    _ => None,
                };
                let Some(n) = n else {
                    let text = setting_text(v).unwrap_or_else(|| v.to_string());
                    return Err(field_failure(
                        &f.name,
                        ty,
                        &text,
                        &format!("For input string: \"{text}\""),
                    ));
                };
                if n <= i64::MAX as u64 {
                    long_dim(b'l', n as i64)
                } else {
                    long_dim(b'u', (n ^ (1 << 63)) as i64)
                }
            }
            _ => continue,
        };
        out.push((f.name.clone(), value));
    }
    Ok(out)
}

/// How many geo points a document holds at `path` (`[lon, lat]` is one).
fn points_at(source: &Value, path: &str) -> usize {
    let mut node = Some(source);
    for seg in path.split('.') {
        node = node.and_then(|n| n.get(seg));
    }
    match node {
        Some(Value::Array(a)) if a.iter().all(Value::is_number) => 1,
        Some(Value::Array(a)) => a.iter().filter(|v| !v.is_null()).count(),
        Some(Value::Null) | None => 0,
        Some(_) => 1,
    }
}

/// `NetworkAddress.format`: IPv4 dotted, IPv6 compressed (RFC 5952), an
/// IPv4-mapped IPv6 address as IPv4.
pub(super) fn format_ip(s: &str) -> Option<String> {
    let s = s.trim();
    if let Ok(v4) = s.parse::<std::net::Ipv4Addr>() {
        return Some(v4.to_string());
    }
    let v6: std::net::Ipv6Addr = s.parse().ok()?;
    if let Some(v4) = v6.to_ipv4_mapped() {
        return Some(v4.to_string());
    }
    let g = v6.segments();
    // The longest run (two or more) of zero groups is compressed.
    let (mut best, mut best_len) = (usize::MAX, 1);
    let mut i = 0;
    while i < 8 {
        if g[i] == 0 {
            let start = i;
            while i < 8 && g[i] == 0 {
                i += 1;
            }
            if i - start > best_len {
                best = start;
                best_len = i - start;
            }
        } else {
            i += 1;
        }
    }
    let hex = |r: &[u16]| r.iter().map(|x| format!("{x:x}")).collect::<Vec<_>>().join(":");
    Some(if best == usize::MAX {
        hex(&g)
    } else {
        format!("{}::{}", hex(&g[..best]), hex(&g[best + best_len..]))
    })
}

/// A time-series document as Elasticsearch indexes it: routing extracted
/// from `_source`, `@timestamp` checked against the index's time range,
/// and the `_tsid` and `_id` computed. `requested_id` (empty when the
/// client gave none) must be that `_id`.
pub(super) fn prepare_doc(
    index: &str,
    settings: &Value,
    mappings: &Value,
    source: &Value,
    body: &[u8],
    requested_id: &str,
) -> Result<TsDoc, Fail> {
    let patterns = routing_path(settings);
    let pairs = routing_values(&patterns, source)?;
    let routing =
        routing_hash(pairs).map_err(|e| bad(&format!("Error extracting routing: {e}")))?;
    let ts_def = mappings.get("properties").and_then(|p| p.get("@timestamp"));
    let nanos_type = ts_def.map(type_of) == Some("date_nanos");
    let format = ts_def.and_then(|d| d.get("format")).and_then(Value::as_str);
    let Some(raw) = source.get("@timestamp").filter(|v| !v.is_null()) else {
        return Err(parse_failure(body, "data stream timestamp field [@timestamp] is missing"));
    };
    let nanos = match (raw, format) {
        (Value::String(s), None) => parse_instant(s),
        (Value::Number(n), None) => n
            .as_i64()
            .map(|ms| i128::from(ms) * 1_000_000)
            .or_else(|| n.as_f64().map(|f| (f * 1e6) as i128)),
        (v, f) => dates::value_millis(v, f).map(|ms| i128::from(ms) * 1_000_000),
    };
    let Some(mut nanos) = nanos else {
        let shown = setting_text(raw).unwrap_or_default();
        return Err(parse_failure(
            body,
            &format!("failed to parse field [@timestamp] of type [date]: {shown}"),
        ));
    };
    if !nanos_type {
        nanos = nanos.div_euclid(1_000_000) * 1_000_000;
    }
    let start = time_bound(settings, "start_time")?;
    let end = time_bound(settings, "end_time")?;
    if nanos < start {
        return Err(parse_failure(
            body,
            &format!(
                "time series index @timestamp value [{}] must be larger than {}",
                instant_string(nanos),
                instant_string(start)
            ),
        ));
    }
    if nanos >= end {
        return Err(parse_failure(
            body,
            &format!(
                "time series index @timestamp value [{}] must be smaller than {}",
                instant_string(nanos),
                instant_string(end)
            ),
        ));
    }
    let millis = nanos.div_euclid(1_000_000) as i64;
    let dims = dimensions(mappings, source, body, millis)?;
    let tsid = tsid_bytes(dims);
    let stamp = if nanos_type { nanos as i64 } else { millis };
    let id = make_id(routing, &tsid, stamp);
    if !requested_id.is_empty() && requested_id != id {
        return Err(parse_failure(
            body,
            &format!(
                "_id must be unset or set to [{id}] but was [{requested_id}] because [{index}] is in time_series mode"
            ),
        ));
    }
    Ok(TsDoc { id, tsid: b64(&tsid), millis })
}

/// Before a time-series document's dynamic mapping: routing fields under
/// a `dynamic: false` object would never become dimensions.
pub(super) fn check_unmapped_routing(
    settings: &Value,
    mappings: &Value,
    source: &Value,
    body: &[u8],
) -> Result<(), Fail> {
    let patterns = routing_path(settings);
    let mut leaves = Vec::new();
    source_leaves(source, "", &mut leaves);
    for (path, _) in &leaves {
        let segs: Vec<&str> = path.split('.').collect();
        if !(1..=segs.len()).any(|n| matches_routing_path(&patterns, &segs[..n].join("."))) {
            continue;
        }
        if let Some((field, dynamic)) = unmapped_dynamic(mappings, path)
            && dynamic == "false"
        {
            return Err(fail(
                "document_parsing_exception",
                &format!(
                    "{} All fields matching [routing_path] must be mapped but [{field}] was declared as [dynamic: false]",
                    doc_position(body)
                ),
                400,
            ));
        }
    }
    Ok(())
}

/// After a time-series document's dynamic mapping: a new field under a
/// `dynamic: runtime` object that the routing path matches.
pub(super) fn check_runtime_routing(
    settings: &Value,
    mappings: &Value,
    source: &Value,
) -> Result<(), Fail> {
    let patterns = routing_path(settings);
    let mut leaves = Vec::new();
    source_leaves(source, "", &mut leaves);
    for (path, v) in leaves {
        if !matches_routing_path(&patterns, &path) {
            continue;
        }
        if let Some((_, dynamic)) = unmapped_dynamic(mappings, &path)
            && dynamic == "runtime"
        {
            return Err(bad(&format!(
                "{ROUTING_PATH_MSG} [{path}] was a runtime [{}].",
                runtime_type(v)
            )));
        }
    }
    Ok(())
}

/// `routing` on a CRUD request against a time-series index.
pub(super) fn routing_error(index: &str) -> Fail {
    bad(&format!(
        "specifying routing is not supported because the destination index [{index}] is in time series mode"
    ))
}

/// `routing` on a search of a time-series index.
pub(super) fn search_routing_error(index: &str) -> Fail {
    bad(&format!(
        "searching with a specified routing is not supported because the destination index [{index}] is in time series mode"
    ))
}

pub(super) fn update_error(index: &str) -> Fail {
    bad(&format!(
        "update is not supported because the destination index [{index}] is in time series mode"
    ))
}

pub(super) fn alias_routing_error() -> Fail {
    bad("routing is forbidden on CRUD operations that target indices in [index.mode=time_series]")
}

/// A GET or DELETE of `id` in a time-series index: no routing, and the
/// id must be one the index could have generated.
pub(super) fn check_doc_access(index: &str, id: &str, routing: Option<&str>) -> Result<(), Fail> {
    if routing.is_some() {
        return Err(routing_error(index));
    }
    if unb64(id).is_none_or(|b| b.len() < 4) {
        return Err(fail(
            "resource_not_found_exception",
            &format!("invalid id [{id}] for index [{index}] in time series mode"),
            404,
        ));
    }
    Ok(())
}

/// The version conflict of a `create` over an existing time-series
/// document.
pub(super) fn create_conflict(index: &str, doc: &TsDoc, version: i64) -> Fail {
    let reason = format!(
        "{}: version conflict, document already exists (current version [{version}])",
        doc.description()
    );
    let mut e = super::engine::error("version_conflict_engine_exception", &reason, 409);
    let extra = json!({"index_uuid": "noida", "shard": "0", "index": index});
    for (k, v) in extra.as_object().into_iter().flatten() {
        e["error"]["root_cause"][0][k] = v.clone();
        e["error"][k] = v.clone();
    }
    (409, e)
}

// --- date_nanos ---------------------------------------------------------

/// A `date_nanos` value (an ISO string, or epoch milliseconds) in
/// nanoseconds since the epoch, unbounded.
fn nanos_of(v: &Value) -> Option<i128> {
    match v {
        Value::String(s) => parse_instant(s)
            .or_else(|| dates::value_millis(v, None).map(|ms| i128::from(ms) * 1_000_000)),
        Value::Number(n) => n
            .as_i64()
            .map(|ms| i128::from(ms) * 1_000_000)
            .or_else(|| n.as_f64().map(|f| (f * 1e6) as i128)),
        _ => None,
    }
}

/// A `date_nanos` doc value: nanoseconds since the epoch.
pub(super) fn date_nanos(v: &Value) -> Option<i64> {
    nanos_of(v).and_then(|n| i64::try_from(n).ok())
}

/// `date_nanos` fields only hold dates from 1970 to 2262 (a long of
/// nanoseconds): a document with one outside fails to index.
pub(super) fn check_date_nanos(mappings: &Value, source: &Value, id: &str) -> Result<(), Fail> {
    for f in all_fields(mappings) {
        if type_of(f.def) != "date_nanos" || f.def.get("format").is_some() {
            continue;
        }
        for v in raw_values(source, &f.name) {
            let Some(n) = nanos_of(v) else { continue };
            let shown = match v {
                Value::String(s) => s.clone(),
                _ => instant_string(n),
            };
            let cause = if n < 0 {
                format!(
                    "date[{shown}] is before the epoch in 1970 and cannot be stored in nanosecond resolution"
                )
            } else if n > i128::from(i64::MAX) {
                format!(
                    "date[{shown}] is after 2262-04-11T23:47:16.854775807 and cannot be stored in nanosecond resolution"
                )
            } else {
                continue;
            };
            let preview = match v {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            let reason = format!(
                "[1:1] failed to parse field [{}] of type [date_nanos] in document with id '{id}'. Preview of field's value: '{preview}'",
                f.name
            );
            return Err(fail_caused(
                "document_parsing_exception",
                &reason,
                ("illegal_argument_exception", &cause),
                400,
            ));
        }
    }
    Ok(())
}

// --- search -------------------------------------------------------------

/// The `_tsid` doc value of a hit, decoded for byte-order comparisons.
pub(super) fn tsid_sort_key(tsid: &str) -> Vec<u8> {
    unb64(tsid).unwrap_or_default()
}

/// Queries on `_tsid` aren't possible (it has doc values only).
pub(super) fn check_query(query: &Map<String, Value>) -> Result<(), super::search::EsError> {
    const KINDS: &[&str] = &[
        "term",
        "terms",
        "match",
        "match_phrase",
        "prefix",
        "wildcard",
        "regexp",
        "range",
        "fuzzy",
    ];
    let on_tsid = KINDS
        .iter()
        .any(|k| query.get(*k).and_then(Value::as_object).is_some_and(|m| m.contains_key("_tsid")));
    if on_tsid {
        return Err(super::search::EsError::shard_failure(
            "query_shard_exception",
            "failed to create query: [_tsid] is not searchable",
        )
        .caused_by("illegal_argument_exception", "[_tsid] is not searchable"));
    }
    Ok(())
}

/// The `time_series_dimension` / `time_series_metric` entries
/// `_field_caps` reports for a mapped field.
pub(super) fn field_caps_extras(def: &Value, time_series: bool) -> Map<String, Value> {
    let mut out = Map::new();
    if !time_series {
        return out;
    }
    if is_dimension(def) {
        out.insert("time_series_dimension".into(), json!(true));
    }
    if let Some(m) = def.get("time_series_metric").and_then(Value::as_str) {
        out.insert("time_series_metric".into(), json!(m));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &str) -> DimValue {
        string_dim(v)
    }

    #[test]
    fn tsid_matches_elasticsearch() {
        let tsid = tsid_bytes(vec![
            ("metricset".into(), s("pod")),
            ("k8s.pod.uid".into(), s("947e4ced-1786-4e53-9e0c-5c447e959507")),
        ]);
        assert_eq!(b64(&tsid), "KCjEJ9R_BgO8TRX2QOd6dpR12oDh--qoyNZRQPy43y34Qdy2dpsyG0o");
        let tsid = tsid_bytes(vec![("metricset".into(), s("cat"))]);
        assert_eq!(b64(&tsid), "JNu4XCk2JFwjn2IrkVkU1soGlT_5e6_NYGOZWULpmMG9IAlZlA");
        let tsid =
            tsid_bytes(vec![("metricset".into(), s("aa")), ("id".into(), long_dim(b'l', 2))]);
        assert_eq!(b64(&tsid), "KMaueSdBhc_WIhY4xoPE2EdDgKYd73outpXn7LJV-gQfvlrec7NyMho");
    }

    #[test]
    fn id_matches_elasticsearch() {
        let uid = "947e4ced-1786-4e53-9e0c-5c447e959507";
        let tsid = tsid_bytes(vec![("metricset".into(), s("pod")), ("k8s.pod.uid".into(), s(uid))]);
        let routing = routing_hash(vec![
            ("metricset".into(), "pod".into()),
            ("k8s.pod.uid".into(), uid.into()),
        ])
        .unwrap();
        let ts = parse_instant("2021-04-28T18:52:04.467Z").unwrap() / 1_000_000;
        assert_eq!(make_id(routing, &tsid, ts as i64), "cZZNs7B9sSWsyrL5AAABeRnS7fM");
        assert_eq!(routing_hash_of_id("cn4excfoxSs_KdA5AAABeRnRFAY").unwrap(), "cn4exQ");
    }

    #[test]
    fn instants_print_like_java() {
        let n = parse_instant("2021-09-26T03:09:41Z").unwrap();
        assert_eq!(instant_string(n), "2021-09-26T03:09:41Z");
        let n = parse_instant("2010-09-26T03:09:52.123456789Z").unwrap();
        assert_eq!(instant_string(n), "2010-09-26T03:09:52.123456789Z");
        assert_eq!(instant_string(parse_instant("1632625782000").unwrap()), "2021-09-26T03:09:42Z");
        assert_eq!(
            instant_string(parse_instant("-9999-01-01T00:00:00Z").unwrap()),
            "-9999-01-01T00:00:00Z"
        );
    }

    #[test]
    fn ipv6_is_compressed() {
        assert_eq!(
            format_ip("2001:0db8:85a3:0000:0000:8a2e:0370:7334").unwrap(),
            "2001:db8:85a3::8a2e:370:7334"
        );
        assert_eq!(format_ip("::ffff:10.10.1.1").unwrap(), "10.10.1.1");
    }

    #[test]
    fn base64_round_trips() {
        for n in 0..10 {
            let bytes: Vec<u8> = (0..n).map(|i| (i * 37 + 11) as u8).collect();
            assert_eq!(unb64(&b64(&bytes)).unwrap(), bytes);
        }
    }
}
