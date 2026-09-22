//! Time zones: fixed offsets, plus named zones read from the system tz
//! database (`/usr/share/zoneinfo`, TZif files) so no tzdata is bundled.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

/// A resolved time zone. Offsets are seconds east of UTC.
#[derive(Clone, Debug)]
pub enum Zone {
    Fixed(i32),
    Tzif(Arc<TzInfo>),
}

#[derive(Debug)]
pub struct TzInfo {
    /// Transition instants (unix seconds), each with the offset/abbrev in force after it.
    transitions: Vec<(i64, usize)>,
    types: Vec<(i32, bool, String)>,
    /// POSIX rule for instants after the last transition.
    footer: Option<PosixTz>,
}

impl Zone {
    pub fn utc() -> Zone {
        Zone::Fixed(0)
    }

    /// Offset in force at a UTC instant (unix seconds).
    pub fn offset_at_utc(&self, t: i64) -> i32 {
        self.info_at_utc(t).0
    }

    pub fn info_at_utc(&self, t: i64) -> (i32, bool, String) {
        match self {
            Zone::Fixed(o) => (*o, false, fixed_abbrev(*o)),
            Zone::Tzif(info) => info.at_utc(t),
        }
    }

    /// Offset for a local wall-clock time (unix seconds as if UTC). Like
    /// Postgres, an ambiguous or skipped time resolves using the offset in
    /// force just before the transition.
    pub fn offset_for_local(&self, local: i64) -> i32 {
        match self {
            Zone::Fixed(o) => *o,
            Zone::Tzif(_) => {
                // Try the offset in force at (local - off) for candidate offsets.
                let before = self.offset_at_utc(local - 86400) as i64;
                let o1 = self.offset_at_utc(local - before);
                let o2 = self.offset_at_utc(local - o1 as i64);
                if o1 == o2 {
                    return o1;
                }
                // Gap or overlap: prefer the pre-transition offset.
                if self.offset_at_utc(local - before) as i64 == before { before as i32 } else { o2 }
            }
        }
    }
}

fn fixed_abbrev(o: i32) -> String {
    if o == 0 {
        return "UTC".into();
    }
    format_offset(o, true)
}

/// `+05:30`, `-08`, `+05:21:10` as Postgres prints offsets.
pub fn format_offset(o: i32, _always_minutes: bool) -> String {
    let sign = if o < 0 { '-' } else { '+' };
    let a = o.unsigned_abs();
    let (h, m, s) = (a / 3600, a / 60 % 60, a % 60);
    if s != 0 {
        format!("{sign}{h:02}:{m:02}:{s:02}")
    } else if m != 0 {
        format!("{sign}{h:02}:{m:02}")
    } else {
        format!("{sign}{h:02}")
    }
}

impl TzInfo {
    fn at_utc(&self, t: i64) -> (i32, bool, String) {
        let idx = self.transitions.partition_point(|&(at, _)| at <= t);
        if idx == self.transitions.len()
            && let Some(f) = &self.footer
        {
            return f.at_utc(t);
        }
        let ty = if idx == 0 {
            // Before the first transition: the first non-DST type.
            self.types.iter().position(|t| !t.1).unwrap_or(0)
        } else {
            self.transitions[idx - 1].1
        };
        self.types.get(ty).cloned().unwrap_or((0, false, "UTC".into()))
    }
}

static CACHE: OnceLock<Mutex<HashMap<String, Option<Zone>>>> = OnceLock::new();

/// Resolves a zone name the way `SET TIME ZONE` / `AT TIME ZONE` accept it.
pub fn lookup(name: &str) -> Option<Zone> {
    let trimmed = name.trim();
    let lower = trimmed.to_ascii_lowercase();
    if matches!(
        lower.as_str(),
        "utc" | "gmt" | "z" | "zulu" | "etc/utc" | "etc/gmt" | "universal" | "uct"
    ) {
        return Some(Zone::Fixed(0));
    }
    if let Some(o) = parse_iso_offset(trimmed) {
        return Some(Zone::Fixed(o));
    }
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    if let Some(z) = cache.lock().unwrap().get(&lower) {
        return z.clone();
    }
    let zone = load_tzif(trimmed).map(|i| Zone::Tzif(Arc::new(i)));
    let zone = zone.or_else(|| abbrev_offset(&lower).map(Zone::Fixed));
    cache.lock().unwrap().insert(lower, zone.clone());
    zone
}

/// Common abbreviations Postgres knows by default.
pub fn abbrev_offset(a: &str) -> Option<i32> {
    Some(match a {
        "utc" | "gmt" | "z" | "ut" => 0,
        "est" => -5 * 3600,
        "edt" => -4 * 3600,
        "cst" => -6 * 3600,
        "cdt" => -5 * 3600,
        "mst" => -7 * 3600,
        "mdt" => -6 * 3600,
        "pst" => -8 * 3600,
        "pdt" => -7 * 3600,
        "cet" => 3600,
        "cest" => 7200,
        "eet" => 7200,
        "eest" => 3 * 3600,
        "bst" => 3600,
        "ist" => 7200,
        "jst" => 9 * 3600,
        "msk" => 3 * 3600,
        _ => return None,
    })
}

/// `+05`, `-0530`, `+05:30`, `+05:30:15`: seconds east of UTC.
pub fn parse_iso_offset(s: &str) -> Option<i32> {
    let b = s.as_bytes();
    if b.len() < 2 || (b[0] != b'+' && b[0] != b'-') {
        return None;
    }
    let neg = b[0] == b'-';
    let rest = &s[1..];
    let parts: Vec<&str> = if rest.contains(':') {
        rest.split(':').collect()
    } else {
        match rest.len() {
            1 | 2 => vec![rest],
            4 => vec![&rest[..2], &rest[2..]],
            6 => vec![&rest[..2], &rest[2..4], &rest[4..]],
            _ => return None,
        }
    };
    if parts.is_empty()
        || parts.len() > 3
        || parts.iter().any(|p| p.is_empty() || !p.bytes().all(|c| c.is_ascii_digit()))
    {
        return None;
    }
    let h: i32 = parts[0].parse().ok()?;
    let m: i32 = parts.get(1).map_or(Some(0), |p| p.parse().ok())?;
    let sec: i32 = parts.get(2).map_or(Some(0), |p| p.parse().ok())?;
    if h > 15 || m > 59 || sec > 59 {
        return None;
    }
    let v = h * 3600 + m * 60 + sec;
    Some(if neg { -v } else { v })
}

fn load_tzif(name: &str) -> Option<TzInfo> {
    if name.is_empty() || name.contains("..") || name.starts_with('/') {
        return None;
    }
    let dirs = ["/usr/share/zoneinfo", "/usr/lib/zoneinfo", "/usr/share/lib/zoneinfo"];
    for dir in dirs {
        let path = format!("{dir}/{name}");
        if let Ok(bytes) = std::fs::read(&path) {
            return parse_tzif(&bytes);
        }
        // Case-insensitive fallback, as Postgres matches zone names that way.
        if let Some(found) = find_case_insensitive(dir, name)
            && let Ok(bytes) = std::fs::read(found)
        {
            return parse_tzif(&bytes);
        }
    }
    None
}

fn find_case_insensitive(dir: &str, name: &str) -> Option<String> {
    let mut cur = std::path::PathBuf::from(dir);
    for part in name.split('/') {
        let entry = std::fs::read_dir(&cur)
            .ok()?
            .flatten()
            .find(|e| e.file_name().to_string_lossy().eq_ignore_ascii_case(part))?;
        cur = entry.path();
    }
    cur.is_file().then(|| cur.to_string_lossy().into_owned())
}

/// Canonical spelling of a zone name, e.g. `america/new_york` → `America/New_York`.
pub fn canonical_name(name: &str) -> String {
    for dir in ["/usr/share/zoneinfo", "/usr/lib/zoneinfo"] {
        if std::path::Path::new(&format!("{dir}/{name}")).is_file() {
            return name.to_string();
        }
        if let Some(found) = find_case_insensitive(dir, name) {
            return found[dir.len() + 1..].to_string();
        }
    }
    name.to_string()
}

fn be32(b: &[u8], at: usize) -> Option<i64> {
    Some(i32::from_be_bytes(b.get(at..at + 4)?.try_into().ok()?) as i64)
}

fn be64(b: &[u8], at: usize) -> Option<i64> {
    Some(i64::from_be_bytes(b.get(at..at + 8)?.try_into().ok()?))
}

fn parse_tzif(b: &[u8]) -> Option<TzInfo> {
    if b.get(0..4)? != b"TZif" {
        return None;
    }
    let version = *b.get(4)?;
    let counts = |at: usize| -> Option<[usize; 6]> {
        let mut c = [0usize; 6];
        for (i, v) in c.iter_mut().enumerate() {
            *v = be32(b, at + 20 + i * 4)? as usize;
        }
        Some(c)
    };
    // counts: isutcnt, isstdcnt, leapcnt, timecnt, typecnt, charcnt
    let c1 = counts(0)?;
    let v1_len = c1[3] * 5 + c1[4] * 6 + c1[5] + c1[2] * 8 + c1[1] + c1[0];
    let (start, c, time_size) = if version >= b'2' {
        let h2 = 44 + v1_len;
        (h2 + 44, counts(h2)?, 8)
    } else {
        (44, c1, 4)
    };
    let [isut, isstd, leap, timecnt, typecnt, charcnt] = c;
    let mut p = start;
    let mut times = Vec::with_capacity(timecnt);
    for _ in 0..timecnt {
        times.push(if time_size == 8 { be64(b, p)? } else { be32(b, p)? });
        p += time_size;
    }
    let idxs = b.get(p..p + timecnt)?.to_vec();
    p += timecnt;
    let mut raw_types = Vec::with_capacity(typecnt);
    for _ in 0..typecnt {
        let off = be32(b, p)? as i32;
        let dst = *b.get(p + 4)? != 0;
        let ai = *b.get(p + 5)? as usize;
        raw_types.push((off, dst, ai));
        p += 6;
    }
    let chars = b.get(p..p + charcnt)?;
    p += charcnt + leap * (time_size + 4) + isstd + isut;
    let types = raw_types
        .into_iter()
        .map(|(o, d, ai)| {
            let end = chars[ai.min(chars.len())..]
                .iter()
                .position(|&c| c == 0)
                .map_or(chars.len(), |e| ai + e);
            (o, d, String::from_utf8_lossy(&chars[ai.min(end)..end]).into_owned())
        })
        .collect();
    let transitions = times.into_iter().zip(idxs.into_iter().map(|i| i as usize)).collect();
    let footer = if version >= b'2' {
        let rest = b.get(p..)?;
        let s = String::from_utf8_lossy(rest);
        s.trim_matches('\n').lines().next().and_then(PosixTz::parse)
    } else {
        None
    };
    Some(TzInfo { transitions, types, footer })
}

/// A POSIX TZ rule such as `EST5EDT,M3.2.0,M11.1.0`.
#[derive(Debug)]
struct PosixTz {
    std_name: String,
    std_off: i32,
    dst: Option<(String, i32, Rule, i32, Rule, i32)>,
}

#[derive(Debug, Clone, Copy)]
enum Rule {
    /// Month, week (1-5), weekday (0 = Sunday).
    M(u32, u32, u32),
    /// Julian day 1..365, ignoring Feb 29.
    J(u32),
    /// Zero-based day of year.
    D(u32),
}

impl PosixTz {
    fn parse(s: &str) -> Option<PosixTz> {
        let mut p = Cursor { s: s.as_bytes(), i: 0 };
        let std_name = p.name()?;
        let std_off = -p.offset()?;
        if p.done() {
            return Some(PosixTz { std_name, std_off, dst: None });
        }
        let dst_name = p.name()?;
        let dst_off = if p.peek() != Some(b',') { -p.offset()? } else { std_off + 3600 };
        p.expect(b',')?;
        let r1 = p.rule()?;
        let t1 = if p.peek() == Some(b'/') {
            p.i += 1;
            p.offset()?
        } else {
            7200
        };
        p.expect(b',')?;
        let r2 = p.rule()?;
        let t2 = if p.peek() == Some(b'/') {
            p.i += 1;
            p.offset()?
        } else {
            7200
        };
        Some(PosixTz { std_name, std_off, dst: Some((dst_name, dst_off, r1, t1, r2, t2)) })
    }

    fn at_utc(&self, t: i64) -> (i32, bool, String) {
        let Some((dst_name, dst_off, r1, t1, r2, t2)) = &self.dst else {
            return (self.std_off, false, self.std_name.clone());
        };
        let year =
            crate::postgres::datetime::civil_from_days((t + self.std_off as i64).div_euclid(86400))
                .0;
        // DST starts at local standard time t1, ends at local DST time t2.
        let start = rule_day(*r1, year) * 86400 + *t1 as i64 - self.std_off as i64;
        let end = rule_day(*r2, year) * 86400 + *t2 as i64 - *dst_off as i64;
        let in_dst = if start < end { t >= start && t < end } else { t >= start || t < end };
        if in_dst {
            (*dst_off, true, dst_name.clone())
        } else {
            (self.std_off, false, self.std_name.clone())
        }
    }
}

/// Unix day of a rule's date in `year`.
fn rule_day(r: Rule, year: i64) -> i64 {
    use crate::postgres::datetime::{days_from_civil, is_leap};
    match r {
        Rule::J(n) => {
            let mut d = n as i64 - 1;
            if is_leap(year) && n >= 60 {
                d += 1;
            }
            days_from_civil(year, 1, 1) + d
        }
        Rule::D(n) => days_from_civil(year, 1, 1) + n as i64,
        Rule::M(m, w, wd) => {
            let first = days_from_civil(year, m, 1);
            // 1970-01-01 was a Thursday (4).
            let first_wd = (first + 4).rem_euclid(7);
            let mut d = first + (wd as i64 - first_wd).rem_euclid(7) + (w as i64 - 1) * 7;
            let next_month = if m == 12 {
                days_from_civil(year + 1, 1, 1)
            } else {
                days_from_civil(year, m + 1, 1)
            };
            while d >= next_month {
                d -= 7;
            }
            d
        }
    }
}

struct Cursor<'a> {
    s: &'a [u8],
    i: usize,
}

impl Cursor<'_> {
    fn peek(&self) -> Option<u8> {
        self.s.get(self.i).copied()
    }
    fn done(&self) -> bool {
        self.i >= self.s.len()
    }
    fn expect(&mut self, c: u8) -> Option<()> {
        (self.peek()? == c).then(|| self.i += 1)
    }
    fn name(&mut self) -> Option<String> {
        let start = self.i;
        if self.peek() == Some(b'<') {
            self.i += 1;
            while self.peek()? != b'>' {
                self.i += 1;
            }
            self.i += 1;
            return Some(String::from_utf8_lossy(&self.s[start + 1..self.i - 1]).into_owned());
        }
        while self.peek().is_some_and(|c| c.is_ascii_alphabetic()) {
            self.i += 1;
        }
        (self.i > start).then(|| String::from_utf8_lossy(&self.s[start..self.i]).into_owned())
    }
    fn num(&mut self) -> Option<i64> {
        let start = self.i;
        while self.peek().is_some_and(|c| c.is_ascii_digit()) {
            self.i += 1;
        }
        std::str::from_utf8(&self.s[start..self.i]).ok()?.parse().ok()
    }
    /// `[+-]hh[:mm[:ss]]` in seconds.
    fn offset(&mut self) -> Option<i32> {
        let mut sign = 1;
        match self.peek() {
            Some(b'-') => {
                sign = -1;
                self.i += 1;
            }
            Some(b'+') => self.i += 1,
            _ => {}
        }
        let mut v = self.num()? * 3600;
        if self.peek() == Some(b':') {
            self.i += 1;
            v += self.num()? * 60;
            if self.peek() == Some(b':') {
                self.i += 1;
                v += self.num()?;
            }
        }
        Some(sign * v as i32)
    }
    fn rule(&mut self) -> Option<Rule> {
        match self.peek()? {
            b'M' => {
                self.i += 1;
                let m = self.num()? as u32;
                self.expect(b'.')?;
                let w = self.num()? as u32;
                self.expect(b'.')?;
                let d = self.num()? as u32;
                Some(Rule::M(m, w, d))
            }
            b'J' => {
                self.i += 1;
                Some(Rule::J(self.num()? as u32))
            }
            _ => Some(Rule::D(self.num()? as u32)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offsets() {
        assert_eq!(parse_iso_offset("+05:30"), Some(19800));
        assert_eq!(parse_iso_offset("-08"), Some(-28800));
        assert_eq!(parse_iso_offset("+0530"), Some(19800));
        assert_eq!(parse_iso_offset("x"), None);
        assert_eq!(format_offset(19800, false), "+05:30");
        assert_eq!(format_offset(-28800, false), "-08");
    }

    #[test]
    fn posix_rules() {
        let tz = PosixTz::parse("EST5EDT,M3.2.0,M11.1.0").unwrap();
        // 2030-07-01 12:00 UTC is in DST.
        let t = crate::postgres::datetime::days_from_civil(2030, 7, 1) * 86400 + 43200;
        assert_eq!(tz.at_utc(t).0, -4 * 3600);
        let t = crate::postgres::datetime::days_from_civil(2030, 1, 1) * 86400;
        assert_eq!(tz.at_utc(t).0, -5 * 3600);
    }

    #[test]
    fn system_zone_if_present() {
        if let Some(z) = lookup("America/New_York") {
            let t = crate::postgres::datetime::days_from_civil(2020, 6, 1) * 86400;
            assert_eq!(z.offset_at_utc(t), -4 * 3600);
            let t = crate::postgres::datetime::days_from_civil(2020, 1, 1) * 86400;
            assert_eq!(z.offset_at_utc(t), -5 * 3600);
        }
    }
}
