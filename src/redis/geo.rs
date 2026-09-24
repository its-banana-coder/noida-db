//! Geo commands, ported from Redis's geo.c, geohash.c and geohash_helper.c.
//!
//! A geo set is a sorted set whose scores are 52-bit geohashes, so GEOADD
//! is ZADD with computed scores and the rest decode those scores back.

use super::double::parse_double;
use super::engine::{Command, Ctx, Data, Entry, Reply, cmd, eq_ic, int_arg, syntax};
use super::resp::Value;
use super::zsets::Zset;

pub static COMMANDS: &[Command] = &[
    cmd("geoadd", geoadd),
    cmd("geopos", geopos),
    cmd("geodist", geodist),
    cmd("geohash", geohash),
    cmd("geosearch", geosearch),
    cmd("geosearchstore", geosearchstore),
    cmd("georadius", georadius),
    cmd("georadius_ro", georadius_ro),
    cmd("georadiusbymember", georadiusbymember),
    cmd("georadiusbymember_ro", georadiusbymember_ro),
];

const LAT_MIN: f64 = -85.05112878;
const LAT_MAX: f64 = 85.05112878;
const LONG_MIN: f64 = -180.0;
const LONG_MAX: f64 = 180.0;
const STEP: u32 = 26;
const EARTH_RADIUS_IN_METERS: f64 = 6372797.560856;

fn deg_rad(a: f64) -> f64 {
    a * (std::f64::consts::PI / 180.0)
}

/// `interleave64`: spreads the low 32 bits of each value into even and odd
/// bit positions.
fn interleave64(xlo: u32, ylo: u32) -> u64 {
    fn spread(v: u32) -> u64 {
        let mut x = v as u64;
        x = (x | (x << 16)) & 0x0000_FFFF_0000_FFFF;
        x = (x | (x << 8)) & 0x00FF_00FF_00FF_00FF;
        x = (x | (x << 4)) & 0x0F0F_0F0F_0F0F_0F0F;
        x = (x | (x << 2)) & 0x3333_3333_3333_3333;
        x = (x | (x << 1)) & 0x5555_5555_5555_5555;
        x
    }
    spread(xlo) | (spread(ylo) << 1)
}

/// `deinterleave64`: the reverse, returning (even bits, odd bits).
fn deinterleave64(interleaved: u64) -> (u32, u32) {
    fn squash(mut x: u64) -> u32 {
        x &= 0x5555_5555_5555_5555;
        x = (x | (x >> 1)) & 0x3333_3333_3333_3333;
        x = (x | (x >> 2)) & 0x0F0F_0F0F_0F0F_0F0F;
        x = (x | (x >> 4)) & 0x00FF_00FF_00FF_00FF;
        x = (x | (x >> 8)) & 0x0000_FFFF_0000_FFFF;
        x = (x | (x >> 16)) & 0x0000_0000_FFFF_FFFF;
        x as u32
    }
    (squash(interleaved), squash(interleaved >> 1))
}

/// `geohashEncode` over an arbitrary range, at `step` bits per coordinate.
fn encode(lon: f64, lat: f64, step: u32, ranges: ((f64, f64), (f64, f64))) -> u64 {
    let ((lon_min, lon_max), (lat_min, lat_max)) = ranges;
    let lat_offset = (lat - lat_min) / (lat_max - lat_min) * (1u64 << step) as f64;
    let long_offset = (lon - lon_min) / (lon_max - lon_min) * (1u64 << step) as f64;
    interleave64(lat_offset as u32, long_offset as u32)
}

/// The 52-bit score GEOADD stores (`geohashEncodeWGS84` + `geohashAlign52Bits`).
fn encode_wgs84(lon: f64, lat: f64) -> u64 {
    encode(lon, lat, STEP, ((LONG_MIN, LONG_MAX), (LAT_MIN, LAT_MAX)))
}

/// `decodeGeohash`: the centre of the cell a score names.
fn decode(bits: u64) -> (f64, f64) {
    let (ilato, ilono) = deinterleave64(bits);
    let scale = |min: f64, max: f64, i: u32| {
        let lo = min + (i as f64 / (1u64 << STEP) as f64) * (max - min);
        let hi = min + ((i + 1) as f64 / (1u64 << STEP) as f64) * (max - min);
        (lo + hi) / 2.0
    };
    let lon = scale(LONG_MIN, LONG_MAX, ilono).clamp(LONG_MIN, LONG_MAX);
    let lat = scale(LAT_MIN, LAT_MAX, ilato).clamp(LAT_MIN, LAT_MAX);
    (lon, lat)
}

/// `geohashGetLatDistance`.
fn lat_distance(lat1: f64, lat2: f64) -> f64 {
    EARTH_RADIUS_IN_METERS * (deg_rad(lat2) - deg_rad(lat1)).abs()
}

/// `geohashGetDistance`: haversine, in metres.
fn distance(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let v = ((deg_rad(lon2) - deg_rad(lon1)) / 2.0).sin();
    if v == 0.0 {
        return lat_distance(lat1, lat2);
    }
    let (lat1r, lat2r) = (deg_rad(lat1), deg_rad(lat2));
    let u = ((lat2r - lat1r) / 2.0).sin();
    let a = u * u + lat1r.cos() * lat2r.cos() * v * v;
    2.0 * EARTH_RADIUS_IN_METERS * a.sqrt().asin()
}

/// `fixedpoint_d2string(.., 4)`, used for distances.
fn distance_string(d: f64) -> String {
    let scaled = (d * 10000.0).round_ties_even() as i64;
    let (sign, v) = if scaled < 0 { ("-", scaled.unsigned_abs()) } else { ("", scaled as u64) };
    format!("{sign}{}.{:04}", v / 10000, v % 10000)
}

/// `addReplyHumanLongDouble`, used for coordinates.
fn human(v: f64) -> Value {
    let s = format!("{v:.17}");
    let s = s.trim_end_matches('0').trim_end_matches('.');
    Value::bulk(if s == "-0" { "0" } else { s })
}

fn unit_to_meters(raw: &[u8]) -> Result<f64, Value> {
    match raw.to_ascii_lowercase().as_slice() {
        b"m" => Ok(1.0),
        b"km" => Ok(1000.0),
        b"ft" => Ok(0.3048),
        b"mi" => Ok(1609.34),
        _ => Err(Value::err("ERR unsupported unit provided. please use M, KM, FT, MI")),
    }
}

/// `extractLongLatOrReply`.
fn lon_lat(lon: &[u8], lat: &[u8]) -> Result<(f64, f64), Value> {
    let bad = || Value::err("ERR value is not a valid float");
    let x = parse_double(lon).ok_or_else(bad)?;
    let y = parse_double(lat).ok_or_else(bad)?;
    if !(LONG_MIN..=LONG_MAX).contains(&x) || !(LAT_MIN..=LAT_MAX).contains(&y) {
        return Err(Value::err(format!("ERR invalid longitude,latitude pair {x:.6},{y:.6}")));
    }
    Ok((x, y))
}

fn geoadd(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let mut longidx = 2;
    let (mut nx, mut xx) = (false, false);
    while longidx < a.len() {
        if eq_ic(&a[longidx], "nx") {
            nx = true;
        } else if eq_ic(&a[longidx], "xx") {
            xx = true;
        } else if !eq_ic(&a[longidx], "ch") {
            break;
        }
        longidx += 1;
    }
    let rest = &a[longidx..];
    if !rest.len().is_multiple_of(3) || rest.is_empty() || (nx && xx) {
        return Err(syntax());
    }
    // Rewritten as ZADD, exactly as Redis does it.
    let mut zargs: Vec<Vec<u8>> = vec![b"zadd".to_vec()];
    zargs.extend_from_slice(&a[1..longidx]);
    for triple in rest.chunks(3) {
        let (lon, lat) = lon_lat(&triple[0], &triple[1])?;
        zargs.push(encode_wgs84(lon, lat).to_string().into_bytes());
        zargs.push(triple[2].clone());
    }
    super::zsets::zadd_command(ctx, &zargs)
}

/// The score of `member`, or None.
fn member_score(ctx: &mut Ctx, key: &[u8], member: &[u8]) -> Result<Option<f64>, Value> {
    Ok(ctx.get_zset(key)?.and_then(|z| z.score(member)))
}

fn geopos(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    let mut out = Vec::with_capacity(a.len() - 2);
    for m in &a[2..] {
        out.push(match member_score(ctx, &a[1], m)? {
            Some(score) => {
                let (lon, lat) = decode(score as u64);
                Value::Array(vec![human(lon), human(lat)])
            }
            None => Value::NullArray,
        });
    }
    Ok(Value::Array(out))
}

fn geodist(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    if a.len() > 5 {
        return Err(syntax());
    }
    let to_meters = if a.len() == 5 { unit_to_meters(&a[4])? } else { 1.0 };
    let (Some(s1), Some(s2)) = (member_score(ctx, &a[1], &a[2])?, member_score(ctx, &a[1], &a[3])?)
    else {
        return Ok(Value::Null);
    };
    let (lon1, lat1) = decode(s1 as u64);
    let (lon2, lat2) = decode(s2 as u64);
    Ok(Value::bulk(distance_string(distance(lon1, lat1, lon2, lat2) / to_meters)))
}

fn geohash(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    const ALPHABET: &[u8] = b"0123456789bcdefghjkmnpqrstuvwxyz";
    let mut out = Vec::with_capacity(a.len() - 2);
    for m in &a[2..] {
        let Some(score) = member_score(ctx, &a[1], m)? else {
            out.push(Value::Null);
            continue;
        };
        // Standard geohashes use the full latitude range, not Redis's.
        let (lon, lat) = decode(score as u64);
        let bits = encode(lon, lat, STEP, ((-180.0, 180.0), (-90.0, 90.0)));
        let mut s = Vec::with_capacity(11);
        for i in 0..11u32 {
            // Only 52 bits are meaningful, so the last characters pad with 0.
            let idx = if i * 5 + 5 > 52 { 0 } else { ((bits << (i * 5)) >> 47) & 0x1f };
            s.push(ALPHABET[idx as usize]);
        }
        out.push(Value::Bulk(s));
    }
    Ok(Value::Array(out))
}

// ---- searching ----

#[derive(Clone, Copy, PartialEq)]
enum Shape {
    Radius(f64),
    Box(f64, f64),
}

#[derive(Clone, Copy, PartialEq)]
enum Sort {
    None,
    Asc,
    Desc,
}

/// Which command is running, which decides the argument shape.
#[derive(Clone, Copy, PartialEq)]
struct Variant {
    /// GEOSEARCH / GEOSEARCHSTORE take FROMMEMBER/FROMLONLAT/BYRADIUS/BYBOX.
    search: bool,
    store_variant: bool,
    /// The centre is a member (GEORADIUSBYMEMBER).
    by_member: bool,
    /// The read-only variants refuse STORE.
    no_store: bool,
}

struct Found {
    member: Vec<u8>,
    score: u64,
    dist: f64,
    lon: f64,
    lat: f64,
}

#[allow(clippy::too_many_lines)]
fn geo_search(ctx: &mut Ctx, a: &[Vec<u8>], v: Variant) -> Reply {
    let name = String::from_utf8_lossy(&a[0]).to_lowercase();
    let src = if v.store_variant { 2 } else { 1 };
    // The source key is type-checked first, like Redis.
    let exists = ctx.get_zset(&a[src])?.is_some();
    let mut storekey: Option<Vec<u8>> = None;
    let mut storedist = false;
    let mut centre: Option<(f64, f64)> = None;
    let mut shape: Option<Shape> = None;
    let mut to_meters = 1.0;

    let base = if v.search {
        if v.store_variant {
            storekey = Some(a[1].clone());
            3
        } else {
            2
        }
    } else if v.by_member {
        if exists {
            let Some(score) = member_score(ctx, &a[src], &a[2])? else {
                return Err(Value::err("ERR could not decode requested zset member"));
            };
            centre = Some(decode(score as u64));
        }
        let radius = parse_radius(&a[3])?;
        to_meters = unit_to_meters(&a[4])?;
        shape = Some(Shape::Radius(radius * to_meters));
        5
    } else {
        centre = Some(lon_lat(&a[2], &a[3])?);
        let radius = parse_radius(&a[4])?;
        to_meters = unit_to_meters(&a[5])?;
        shape = Some(Shape::Radius(radius * to_meters));
        6
    };

    let (mut withdist, mut withhash, mut withcoord) = (false, false, false);
    let (mut frommember, mut fromloc, mut byradius, mut bybox) = (false, false, false, false);
    let mut sort = Sort::None;
    let mut any = false;
    let mut count: i64 = 0;
    let mut i = base;
    while i < a.len() {
        let left = a.len() - i - 1;
        if eq_ic(&a[i], "withdist") {
            withdist = true;
        } else if eq_ic(&a[i], "withhash") {
            withhash = true;
        } else if eq_ic(&a[i], "withcoord") {
            withcoord = true;
        } else if eq_ic(&a[i], "any") {
            any = true;
        } else if eq_ic(&a[i], "asc") {
            sort = Sort::Asc;
        } else if eq_ic(&a[i], "desc") {
            sort = Sort::Desc;
        } else if eq_ic(&a[i], "count") && left >= 1 {
            count = int_arg(&a[i + 1])?;
            if count <= 0 {
                return Err(Value::err("ERR COUNT must be > 0"));
            }
            i += 1;
        } else if (eq_ic(&a[i], "store") || eq_ic(&a[i], "storedist"))
            && left >= 1
            && !v.no_store
            && !v.search
        {
            storedist = eq_ic(&a[i], "storedist");
            storekey = Some(a[i + 1].clone());
            i += 1;
        } else if eq_ic(&a[i], "storedist") && v.search && v.store_variant {
            storedist = true;
        } else if eq_ic(&a[i], "frommember") && left >= 1 && v.search && !fromloc {
            if exists {
                let Some(score) = member_score(ctx, &a[src], &a[i + 1])? else {
                    return Err(Value::err("ERR could not decode requested zset member"));
                };
                centre = Some(decode(score as u64));
            }
            frommember = true;
            i += 1;
        } else if eq_ic(&a[i], "fromlonlat") && left >= 2 && v.search && !frommember {
            centre = Some(lon_lat(&a[i + 1], &a[i + 2])?);
            fromloc = true;
            i += 2;
        } else if eq_ic(&a[i], "byradius") && left >= 2 && v.search && !bybox {
            let radius = parse_radius(&a[i + 1])?;
            to_meters = unit_to_meters(&a[i + 2])?;
            shape = Some(Shape::Radius(radius * to_meters));
            byradius = true;
            i += 2;
        } else if eq_ic(&a[i], "bybox") && left >= 3 && v.search && !byradius {
            let w = parse_box(&a[i + 1], "need numeric width")?;
            let h = parse_box(&a[i + 2], "need numeric height")?;
            to_meters = unit_to_meters(&a[i + 3])?;
            shape = Some(Shape::Box(w * to_meters, h * to_meters));
            bybox = true;
            i += 3;
        } else {
            return Err(syntax());
        }
        i += 1;
    }

    if storekey.is_some() && (withdist || withhash || withcoord) {
        let what = if v.store_variant { "GEOSEARCHSTORE" } else { "STORE option in GEORADIUS" };
        return Err(Value::err(format!(
            "ERR {what} is not compatible with WITHDIST, WITHHASH and WITHCOORD options"
        )));
    }
    if v.search && !(frommember || fromloc) {
        return Err(Value::err(format!(
            "ERR exactly one of FROMMEMBER or FROMLONLAT can be specified for {name}"
        )));
    }
    if v.search && !(byradius || bybox) {
        return Err(Value::err(format!(
            "ERR exactly one of BYRADIUS and BYBOX can be specified for {name}"
        )));
    }
    if any && count == 0 {
        return Err(Value::err("ERR the ANY argument requires COUNT argument"));
    }

    if !exists {
        return Ok(match storekey {
            Some(key) => {
                let now = ctx.now;
                ctx.db().remove(&key, now);
                Value::Integer(0)
            }
            None => Value::Array(vec![]),
        });
    }
    let (cx, cy) = centre.expect("a centre for an existing key");
    let shape = shape.expect("a shape");

    let mut found: Vec<Found> = Vec::new();
    let members: Vec<(f64, Vec<u8>)> =
        ctx.get_zset(&a[src])?.expect("exists").iter().map(|(s, m)| (*s, m.clone())).collect();
    for (score, member) in members {
        let (lon, lat) = decode(score as u64);
        let Some(dist) = in_shape(shape, cx, cy, lon, lat) else { continue };
        found.push(Found { member, score: score as u64, dist, lon, lat });
        if any && count > 0 && found.len() as i64 >= count {
            break;
        }
    }

    match sort {
        Sort::Asc => found.sort_by(|x, y| x.dist.total_cmp(&y.dist)),
        Sort::Desc => found.sort_by(|x, y| y.dist.total_cmp(&x.dist)),
        Sort::None => {}
    }
    let returned = if count == 0 { found.len() } else { found.len().min(count as usize) };
    found.truncate(returned);

    let Some(key) = storekey else {
        let mut out = Vec::with_capacity(found.len());
        for f in &found {
            let dist = f.dist / to_meters;
            let mut item = vec![Value::Bulk(f.member.clone())];
            if withdist {
                item.push(Value::bulk(distance_string(dist)));
            }
            if withhash {
                item.push(Value::Integer(f.score as i64));
            }
            if withcoord {
                item.push(Value::Array(vec![human(f.lon), human(f.lat)]));
            }
            out.push(if item.len() == 1 {
                item.pop().expect("member")
            } else {
                Value::Array(item)
            });
        }
        return Ok(Value::Array(out));
    };
    let lim = ctx.limits("zset");
    let mut z = Zset::default();
    for f in &found {
        let score = if storedist { f.dist / to_meters } else { f.score as f64 };
        z.insert(&f.member, score, lim);
    }
    let now = ctx.now;
    ctx.db().remove(&key, now);
    let len = z.len();
    if len > 0 {
        ctx.db().insert(key, Entry::new(Data::Zset(z)));
    }
    Ok(Value::Integer(len as i64))
}

fn parse_radius(raw: &[u8]) -> Result<f64, Value> {
    let d = parse_double(raw).ok_or_else(|| Value::err("ERR need numeric radius"))?;
    if d < 0.0 {
        return Err(Value::err("ERR radius cannot be negative"));
    }
    Ok(d)
}

fn parse_box(raw: &[u8], msg: &str) -> Result<f64, Value> {
    let d = parse_double(raw).ok_or_else(|| Value::err(format!("ERR {msg}")))?;
    if d < 0.0 {
        return Err(Value::err("ERR height or width cannot be negative"));
    }
    Ok(d)
}

/// `geohashGetDistanceIfInRadius` / `geohashGetDistanceIfInRectangle`.
fn in_shape(shape: Shape, cx: f64, cy: f64, lon: f64, lat: f64) -> Option<f64> {
    match shape {
        Shape::Radius(r) => {
            let d = distance(cx, cy, lon, lat);
            (d <= r).then_some(d)
        }
        Shape::Box(w, h) => {
            if lat_distance(lat, cy) > h / 2.0 {
                return None;
            }
            if distance(lon, lat, cx, lat) > w / 2.0 {
                return None;
            }
            Some(distance(cx, cy, lon, lat))
        }
    }
}

fn geosearch(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    geo_search(
        ctx,
        a,
        Variant { search: true, store_variant: false, by_member: false, no_store: true },
    )
}

fn geosearchstore(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    geo_search(
        ctx,
        a,
        Variant { search: true, store_variant: true, by_member: false, no_store: false },
    )
}

fn georadius(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    geo_search(
        ctx,
        a,
        Variant { search: false, store_variant: false, by_member: false, no_store: false },
    )
}

fn georadius_ro(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    geo_search(
        ctx,
        a,
        Variant { search: false, store_variant: false, by_member: false, no_store: true },
    )
}

fn georadiusbymember(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    geo_search(
        ctx,
        a,
        Variant { search: false, store_variant: false, by_member: true, no_store: false },
    )
}

fn georadiusbymember_ro(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    geo_search(
        ctx,
        a,
        Variant { search: false, store_variant: false, by_member: true, no_store: true },
    )
}
