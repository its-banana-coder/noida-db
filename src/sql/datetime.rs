//! Dates, times, timestamps and intervals with Postgres's representation:
//! dates are days and timestamps microseconds since 2000-01-01.

use super::numeric::Numeric;
use super::tz::{self, Zone};

pub const USECS_PER_SEC: i64 = 1_000_000;
pub const USECS_PER_DAY: i64 = 86_400 * USECS_PER_SEC;
/// Days from 1970-01-01 to 2000-01-01.
pub const PG_EPOCH_DAYS: i64 = 10_957;
pub const DATE_INF: i32 = i32::MAX;
pub const DATE_NEG_INF: i32 = i32::MIN;
pub const TS_INF: i64 = i64::MAX;
pub const TS_NEG_INF: i64 = i64::MIN;

/// Earliest and latest timestamps Postgres accepts (4714-11-24 BC, 294277 AD).
const MIN_TS: i64 = -211_813_488_000_000_000;
const MAX_TS: i64 = 9_223_371_331_200_000_000;

#[derive(Debug, PartialEq)]
pub enum DtErr {
    Syntax,
    Range,
    /// Unknown time zone name.
    Zone(String),
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct Interval {
    pub months: i32,
    pub days: i32,
    pub micros: i64,
}

impl Interval {
    /// Postgres's comparison key: months as 30 days, days as 24 hours.
    pub fn span(&self) -> i128 {
        (self.months as i128 * 30 + self.days as i128) * USECS_PER_DAY as i128 + self.micros as i128
    }

    pub fn neg(&self) -> Result<Interval, DtErr> {
        Ok(Interval {
            months: self.months.checked_neg().ok_or(DtErr::Range)?,
            days: self.days.checked_neg().ok_or(DtErr::Range)?,
            micros: self.micros.checked_neg().ok_or(DtErr::Range)?,
        })
    }

    pub fn add(&self, o: &Interval) -> Result<Interval, DtErr> {
        Ok(Interval {
            months: self.months.checked_add(o.months).ok_or(DtErr::Range)?,
            days: self.days.checked_add(o.days).ok_or(DtErr::Range)?,
            micros: self.micros.checked_add(o.micros).ok_or(DtErr::Range)?,
        })
    }

    pub fn sub(&self, o: &Interval) -> Result<Interval, DtErr> {
        self.add(&o.neg()?)
    }

    /// `interval * float8`, spilling fractions down like Postgres.
    pub fn mul(&self, f: f64) -> Result<Interval, DtErr> {
        let months_f = self.months as f64 * f;
        let days_f = self.days as f64 * f;
        if !months_f.is_finite()
            || months_f.abs() > i32::MAX as f64
            || days_f.abs() > i32::MAX as f64
        {
            return Err(DtErr::Range);
        }
        let months = months_f.trunc();
        let mut days = days_f.trunc();
        // Fractional months become days (30 per month).
        let month_rem_days = (months_f - months) * 30.0;
        let whole_rem_days = month_rem_days.trunc();
        days += whole_rem_days;
        let mut sec_rem =
            (month_rem_days - whole_rem_days) * 86400.0 + (days_f - days_f.trunc()) * 86400.0;
        // Round to microseconds to avoid float noise.
        sec_rem = (sec_rem * 1e6).round() / 1e6;
        if sec_rem.abs() >= 86400.0 {
            let d = (sec_rem / 86400.0).trunc();
            days += d;
            sec_rem -= d * 86400.0;
        }
        let micros_f = self.micros as f64 * f + sec_rem * 1e6;
        if !micros_f.is_finite() || micros_f.abs() > i64::MAX as f64 {
            return Err(DtErr::Range);
        }
        Ok(Interval { months: months as i32, days: days as i32, micros: micros_f.round() as i64 })
    }

    pub fn div(&self, f: f64) -> Result<Interval, DtErr> {
        self.mul(1.0 / f)
    }

    pub fn justify_hours(&self) -> Interval {
        let mut r = *self;
        let extra = r.micros / USECS_PER_DAY;
        r.days += extra as i32;
        r.micros -= extra * USECS_PER_DAY;
        if r.days > 0 && r.micros < 0 {
            r.micros += USECS_PER_DAY;
            r.days -= 1;
        } else if r.days < 0 && r.micros > 0 {
            r.micros -= USECS_PER_DAY;
            r.days += 1;
        }
        r
    }

    pub fn justify_days(&self) -> Interval {
        let mut r = *self;
        let extra = r.days / 30;
        r.months += extra;
        r.days -= extra * 30;
        if r.months > 0 && r.days < 0 {
            r.days += 30;
            r.months -= 1;
        } else if r.months < 0 && r.days > 0 {
            r.days -= 30;
            r.months += 1;
        }
        r
    }

    pub fn justify(&self) -> Interval {
        let mut r = *self;
        let extra_days = r.micros / USECS_PER_DAY;
        r.days += extra_days as i32;
        r.micros -= extra_days * USECS_PER_DAY;
        let extra_months = r.days / 30;
        r.months += extra_months;
        r.days -= extra_months * 30;
        if r.months > 0 && (r.days < 0 || (r.days == 0 && r.micros < 0)) {
            r.days += 30;
            r.months -= 1;
        } else if r.months < 0 && (r.days > 0 || (r.days == 0 && r.micros > 0)) {
            r.days -= 30;
            r.months += 1;
        }
        if r.days > 0 && r.micros < 0 {
            r.micros += USECS_PER_DAY;
            r.days -= 1;
        } else if r.days < 0 && r.micros > 0 {
            r.micros -= USECS_PER_DAY;
            r.days += 1;
        }
        r
    }
}

pub fn is_leap(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

pub fn days_in_month(y: i64, m: u32) -> u32 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => {
            if is_leap(y) {
                29
            } else {
                28
            }
        }
    }
}

/// Unix day number of a proleptic Gregorian date (year 0 = 1 BC).
pub fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m as i64 + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

pub fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Postgres date (days since 2000-01-01) from y/m/d.
pub fn date_from_ymd(y: i64, m: u32, d: u32) -> i32 {
    (days_from_civil(y, m, d) - PG_EPOCH_DAYS) as i32
}

pub fn ymd_from_date(d: i32) -> (i64, u32, u32) {
    civil_from_days(d as i64 + PG_EPOCH_DAYS)
}

// ---------------------------------------------------------------------------
// Output

fn year_str(y: i64) -> (String, bool) {
    if y <= 0 { (format!("{:04}", 1 - y), true) } else { (format!("{y:04}"), false) }
}

pub fn format_date(d: i32) -> String {
    match d {
        DATE_INF => return "infinity".into(),
        DATE_NEG_INF => return "-infinity".into(),
        _ => {}
    }
    let (y, m, day) = ymd_from_date(d);
    let (ys, bc) = year_str(y);
    format!("{ys}-{m:02}-{day:02}{}", if bc { " BC" } else { "" })
}

/// `HH:MM:SS[.ffffff]` with trailing fractional zeros trimmed.
pub fn format_time(us: i64) -> String {
    let secs = us / USECS_PER_SEC;
    let frac = us % USECS_PER_SEC;
    let mut s = format!("{:02}:{:02}:{:02}", secs / 3600, secs / 60 % 60, secs % 60);
    push_frac(&mut s, frac);
    s
}

fn push_frac(s: &mut String, frac: i64) {
    if frac != 0 {
        let f = format!("{:06}", frac.abs());
        s.push('.');
        s.push_str(f.trim_end_matches('0'));
    }
}

pub fn format_timestamp(ts: i64) -> String {
    match ts {
        TS_INF => return "infinity".into(),
        TS_NEG_INF => return "-infinity".into(),
        _ => {}
    }
    let (s, bc) = format_ts_parts(ts);
    if bc { format!("{s} BC") } else { s }
}

fn format_ts_parts(ts: i64) -> (String, bool) {
    let days = ts.div_euclid(USECS_PER_DAY);
    let tod = ts.rem_euclid(USECS_PER_DAY);
    let (y, m, d) = civil_from_days(days + PG_EPOCH_DAYS);
    let (ys, bc) = year_str(y);
    (format!("{ys}-{m:02}-{d:02} {}", format_time(tod)), bc)
}

pub fn format_timestamptz(ts: i64, zone: &Zone) -> String {
    match ts {
        TS_INF => return "infinity".into(),
        TS_NEG_INF => return "-infinity".into(),
        _ => {}
    }
    let off = zone.offset_at_utc(ts.div_euclid(USECS_PER_SEC) - PG_EPOCH_DAYS * 86400);
    let (s, bc) = format_ts_parts(ts + off as i64 * USECS_PER_SEC);
    format!("{s}{}{}", tz::format_offset(off, false), if bc { " BC" } else { "" })
}

pub fn format_timetz(us: i64, off: i32) -> String {
    format!("{}{}", format_time(us), tz::format_offset(off, false))
}

/// Postgres-style interval text (IntervalStyle = postgres).
pub fn format_interval(iv: &Interval) -> String {
    let mut out = String::new();
    let mut is_zero = true;
    let mut is_before = false;
    let year = iv.months / 12;
    let mon = iv.months % 12;
    let mut part = |out: &mut String, v: i64, unit: &str| {
        if v == 0 {
            return;
        }
        let sep = if is_zero { "" } else { " " };
        let plus = if is_before && v > 0 { "+" } else { "" };
        let s = if v != 1 { "s" } else { "" };
        out.push_str(&format!("{sep}{plus}{v} {unit}{s}"));
        is_before = v < 0;
        is_zero = false;
    };
    part(&mut out, year as i64, "year");
    part(&mut out, mon as i64, "mon");
    part(&mut out, iv.days as i64, "day");
    let t = iv.micros;
    if is_zero || t != 0 {
        let minus = t < 0;
        let a = t.unsigned_abs() as i64;
        let secs = a / USECS_PER_SEC;
        let sign = if minus {
            "-"
        } else if is_before {
            "+"
        } else {
            ""
        };
        out.push_str(&format!(
            "{}{sign}{:02}:{:02}:{:02}",
            if is_zero { "" } else { " " },
            secs / 3600,
            secs / 60 % 60,
            secs % 60
        ));
        push_frac(&mut out, a % USECS_PER_SEC);
    }
    out
}

/// ISO 8601 interval output (IntervalStyle = iso_8601).
pub fn format_interval_iso(iv: &Interval) -> String {
    if iv.months == 0 && iv.days == 0 && iv.micros == 0 {
        return "PT0S".into();
    }
    let mut s = String::from("P");
    let (y, m) = (iv.months / 12, iv.months % 12);
    if y != 0 {
        s.push_str(&format!("{y}Y"));
    }
    if m != 0 {
        s.push_str(&format!("{m}M"));
    }
    if iv.days != 0 {
        s.push_str(&format!("{}D", iv.days));
    }
    if iv.micros != 0 {
        s.push('T');
        let neg = iv.micros < 0;
        let a = iv.micros.unsigned_abs() as i64;
        let secs = a / USECS_PER_SEC;
        let (h, mi, se) = (secs / 3600, secs / 60 % 60, secs % 60);
        let sg = if neg { "-" } else { "" };
        if h != 0 {
            s.push_str(&format!("{sg}{h}H"));
        }
        if mi != 0 {
            s.push_str(&format!("{sg}{mi}M"));
        }
        let frac = a % USECS_PER_SEC;
        if se != 0 || frac != 0 {
            s.push_str(&format!("{sg}{se}"));
            push_frac(&mut s, frac);
            s.push('S');
        }
    }
    s
}

// ---------------------------------------------------------------------------
// Input

/// What a date/time string denotes before it is resolved to a type.
#[derive(Debug, Default)]
pub struct Parsed {
    pub ymd: Option<(i64, u32, u32)>,
    pub time: Option<i64>,
    pub tz: Option<TzSpec>,
    pub special: Option<Special>,
}

#[derive(Debug, Clone)]
pub enum TzSpec {
    Offset(i32),
    Named(Zone),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Special {
    Infinity,
    NegInfinity,
    Epoch,
    Now,
    Today,
    Tomorrow,
    Yesterday,
    Allballs,
}

const MONTHS: [&str; 12] =
    ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"];

fn month_from_name(s: &str) -> Option<u32> {
    let l = s.to_ascii_lowercase();
    let l = l.trim_end_matches('.');
    if l.len() < 3 {
        return None;
    }
    const FULL: [&str; 12] = [
        "january",
        "february",
        "march",
        "april",
        "may",
        "june",
        "july",
        "august",
        "september",
        "october",
        "november",
        "december",
    ];
    for (i, m) in MONTHS.iter().enumerate() {
        if l == *m || l == FULL[i] || (l == "sept" && i == 8) {
            return Some(i as u32 + 1);
        }
    }
    None
}

fn is_weekday_name(s: &str) -> bool {
    let l = s.to_ascii_lowercase();
    let l = l.trim_end_matches('.').trim_end_matches(',');
    const DAYS: [&str; 7] =
        ["sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday"];
    DAYS.iter().any(|d| l == *d || (l.len() >= 3 && d.starts_with(l)))
}

/// Parses `HH:MM[:SS[.frac]]` (hours may be 24 only as exactly 24:00:00).
fn parse_clock(s: &str) -> Result<i64, DtErr> {
    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() < 2 || parts.len() > 3 {
        return Err(DtErr::Syntax);
    }
    let num = |p: &str| -> Result<i64, DtErr> {
        if p.is_empty() || p.len() > 9 || !p.bytes().all(|c| c.is_ascii_digit()) {
            return Err(DtErr::Syntax);
        }
        p.parse().map_err(|_| DtErr::Syntax)
    };
    let h = num(parts[0])?;
    let m = num(parts[1])?;
    let (sec, frac) = if let Some(sp) = parts.get(2) {
        let (whole, f) = sp.split_once('.').unwrap_or((sp, ""));
        let sec = num(whole)?;
        if !f.bytes().all(|c| c.is_ascii_digit()) {
            return Err(DtErr::Syntax);
        }
        (sec, parse_frac_micros(f))
    } else {
        (0, 0)
    };
    if m > 59 || sec > 60 || h > 24 {
        return Err(DtErr::Range);
    }
    let total = ((h * 60 + m) * 60 + sec) * USECS_PER_SEC + frac;
    if total > USECS_PER_DAY {
        return Err(DtErr::Range);
    }
    Ok(total)
}

/// Fractional-second digits to microseconds, rounding half up.
fn parse_frac_micros(f: &str) -> i64 {
    if f.is_empty() {
        return 0;
    }
    let mut digits: Vec<u8> = f.bytes().map(|c| c - b'0').collect();
    digits.resize(7, 0);
    let mut v = 0i64;
    for d in &digits[..6] {
        v = v * 10 + *d as i64;
    }
    if digits[6] >= 5 {
        v += 1;
    }
    v
}

/// Splits a string into date/time tokens, separating a trailing zone
/// offset glued to a time (`10:00+05:30`, `10:00Z`) and ISO `T`.
fn tokenize(s: &str) -> Vec<String> {
    let mut toks = vec![];
    for raw in s.split(|c: char| c.is_whitespace() || c == ',') {
        if raw.is_empty() {
            continue;
        }
        // ISO 8601: 2020-01-01T10:00:00
        let raw = raw.to_string();
        let mut pieces = vec![];
        if let Some(pos) = raw.find(['T', 't'])
            && pos > 0
            && raw[..pos].bytes().all(|c| c.is_ascii_digit() || c == b'-')
            && raw[pos + 1..].starts_with(|c: char| c.is_ascii_digit())
        {
            pieces.push(raw[..pos].to_string());
            pieces.push(raw[pos + 1..].to_string());
        } else {
            pieces.push(raw);
        }
        for p in pieces {
            // Time with zone suffix.
            if p.contains(':') && p.as_bytes()[0].is_ascii_digit() {
                if let Some(pos) = p[1..].find(['+', '-']).map(|i| i + 1) {
                    toks.push(p[..pos].to_string());
                    toks.push(p[pos..].to_string());
                    continue;
                }
                if p.ends_with(['Z', 'z']) {
                    toks.push(p[..p.len() - 1].to_string());
                    toks.push("Z".into());
                    continue;
                }
            }
            toks.push(p);
        }
    }
    toks
}

pub fn parse_datetime(s: &str) -> Result<Parsed, DtErr> {
    let mut out = Parsed::default();
    let lower = s.trim().to_ascii_lowercase();
    let special = match lower.as_str() {
        "infinity" | "+infinity" => Some(Special::Infinity),
        "-infinity" => Some(Special::NegInfinity),
        "epoch" => Some(Special::Epoch),
        "now" => Some(Special::Now),
        "today" => Some(Special::Today),
        "tomorrow" => Some(Special::Tomorrow),
        "yesterday" => Some(Special::Yesterday),
        "allballs" => Some(Special::Allballs),
        _ => None,
    };
    if special.is_some() {
        out.special = special;
        return Ok(out);
    }
    let toks = tokenize(s);
    if toks.is_empty() {
        return Err(DtErr::Syntax);
    }
    let mut bc = false;
    let mut pm: Option<bool> = None;
    let mut month_name: Option<u32> = None;
    let mut loose_nums: Vec<String> = vec![];
    for tok in &toks {
        let t = tok.as_str();
        let l = t.to_ascii_lowercase();
        if l == "bc" {
            bc = true;
        } else if l == "ad" {
        } else if l == "am" || l == "a.m." {
            pm = Some(false);
        } else if l == "pm" || l == "p.m." {
            pm = Some(true);
        } else if matches!(l.as_str(), "today" | "now" | "tomorrow" | "yesterday" | "allballs") {
            out.special = Some(match l.as_str() {
                "today" => Special::Today,
                "now" => Special::Now,
                "tomorrow" => Special::Tomorrow,
                "yesterday" => Special::Yesterday,
                _ => Special::Allballs,
            });
        } else if t.contains(':') && t.as_bytes()[0].is_ascii_digit() {
            if out.time.is_some() {
                return Err(DtErr::Syntax);
            }
            out.time = Some(parse_clock(t)?);
        } else if (t.starts_with('+') || t.starts_with('-')) && out.time.is_some()
            || t.starts_with('+')
        {
            let o = tz::parse_iso_offset(t).ok_or(DtErr::Syntax)?;
            out.tz = Some(TzSpec::Offset(o));
        } else if l == "z" {
            out.tz = Some(TzSpec::Offset(0));
        } else if let Some(ymd) = parse_date_token(t)? {
            if out.ymd.is_some() {
                return Err(DtErr::Syntax);
            }
            out.ymd = Some(ymd);
        } else if let Some(m) = month_from_name(t) {
            month_name = Some(m);
        } else if is_weekday_name(t) {
        } else if t.bytes().all(|c| c.is_ascii_digit()) {
            loose_nums.push(t.to_string());
        } else if t.starts_with('-') && tz::parse_iso_offset(t).is_some() {
            out.tz = Some(TzSpec::Offset(tz::parse_iso_offset(t).unwrap()));
        } else if let Some(z) = tz::lookup(t) {
            out.tz = Some(TzSpec::Named(z));
        } else if t.contains('/') && t.bytes().any(|c| c.is_ascii_alphabetic()) {
            return Err(DtErr::Zone(t.to_string()));
        } else {
            return Err(DtErr::Syntax);
        }
    }
    if let Some(m) = month_name {
        // "Jan 5 2020", "5 Jan 2020", "January 5, 2020"
        if out.ymd.is_some() || loose_nums.len() != 2 {
            return Err(DtErr::Syntax);
        }
        let (a, b) = (&loose_nums[0], &loose_nums[1]);
        let (day, year) = if a.len() > 2 { (b, a) } else { (a, b) };
        let y: i64 = year.parse().map_err(|_| DtErr::Syntax)?;
        let d: u32 = day.parse().map_err(|_| DtErr::Syntax)?;
        out.ymd = Some((y, m, d));
        loose_nums.clear();
    }
    if !loose_nums.is_empty() {
        // A bare number after a date can be a compact time (e.g. 101500) — rare; reject.
        return Err(DtErr::Syntax);
    }
    if let Some((y, m, d)) = out.ymd.as_mut() {
        if bc {
            *y = 1 - *y;
        }
        if *m < 1 || *m > 12 || *d < 1 || *d > days_in_month(*y, *m) {
            return Err(DtErr::Range);
        }
    }
    if let Some(p) = pm {
        let t = out.time.ok_or(DtErr::Syntax)?;
        let h = t / (3600 * USECS_PER_SEC);
        if h > 12 || h == 0 && p {
            return Err(DtErr::Range);
        }
        if p && h < 12 {
            out.time = Some(t + 12 * 3600 * USECS_PER_SEC);
        } else if !p && h == 12 {
            out.time = Some(t - 12 * 3600 * USECS_PER_SEC);
        }
    }
    Ok(out)
}

/// `YYYY-MM-DD`, `YYYY/MM/DD`, `MM/DD/YYYY`, `YYYYMMDD`, `YYYY-Mon-DD`.
fn parse_date_token(t: &str) -> Result<Option<(i64, u32, u32)>, DtErr> {
    let sep = if t.contains('-') && !t.starts_with('-') {
        '-'
    } else if t.contains('/') {
        '/'
    } else if t.contains('.') {
        '.'
    } else {
        if t.len() == 8 && t.bytes().all(|c| c.is_ascii_digit()) {
            let y = t[..4].parse().unwrap();
            let m = t[4..6].parse().unwrap();
            let d = t[6..].parse().unwrap();
            return Ok(Some((y, m, d)));
        }
        return Ok(None);
    };
    let parts: Vec<&str> = t.split(sep).collect();
    if parts.len() != 3 {
        return if parts.iter().all(|p| p.bytes().all(|c| c.is_ascii_digit())) {
            Err(DtErr::Syntax)
        } else {
            Ok(None)
        };
    }
    let num = |p: &str| -> Result<i64, DtErr> {
        if p.is_empty() || !p.bytes().all(|c| c.is_ascii_digit()) {
            return Err(DtErr::Syntax);
        }
        p.parse().map_err(|_| DtErr::Range)
    };
    let mid_month = month_from_name(parts[1]);
    if parts[0].len() >= 3 {
        let y = num(parts[0])?;
        let m = match mid_month {
            Some(m) => m as i64,
            None => num(parts[1])?,
        };
        let d = num(parts[2])?;
        if m > 12 || d > 31 {
            return Err(DtErr::Range);
        }
        return Ok(Some((y, m as u32, d as u32)));
    }
    if let Some(m) = mid_month {
        // DD-Mon-YYYY
        let d = num(parts[0])?;
        let y = num(parts[2])?;
        return Ok(Some((y, m, d as u32)));
    }
    // DateStyle MDY: MM/DD/YYYY
    let m = num(parts[0])?;
    let d = num(parts[1])?;
    let mut y = num(parts[2])?;
    if parts[2].len() <= 2 {
        y += if y < 70 { 2000 } else { 1900 };
    }
    if m > 12 || d > 31 {
        return Err(DtErr::Range);
    }
    Ok(Some((y, m as u32, d as u32)))
}

/// The clock and zone a conversion needs.
pub struct Ctx<'a> {
    /// Transaction start time, microseconds since 2000-01-01 UTC.
    pub now: i64,
    pub zone: &'a Zone,
}

impl Ctx<'_> {
    fn today_local(&self) -> i64 {
        let local = utc_to_local(self.now, self.zone);
        local.div_euclid(USECS_PER_DAY)
    }
}

pub fn utc_to_local(ts: i64, zone: &Zone) -> i64 {
    if ts == TS_INF || ts == TS_NEG_INF {
        return ts;
    }
    ts + zone.offset_at_utc(ts.div_euclid(USECS_PER_SEC) - PG_EPOCH_DAYS * 86400) as i64
        * USECS_PER_SEC
}

pub fn local_to_utc(local: i64, zone: &Zone) -> i64 {
    if local == TS_INF || local == TS_NEG_INF {
        return local;
    }
    local
        - zone.offset_for_local(local.div_euclid(USECS_PER_SEC) - PG_EPOCH_DAYS * 86400) as i64
            * USECS_PER_SEC
}

pub fn parse_date(s: &str, ctx: &Ctx) -> Result<i32, DtErr> {
    let p = parse_datetime(s)?;
    match p.special {
        Some(Special::Infinity) => return Ok(DATE_INF),
        Some(Special::NegInfinity) => return Ok(DATE_NEG_INF),
        Some(Special::Epoch) => return Ok(date_from_ymd(1970, 1, 1)),
        Some(Special::Today) | Some(Special::Now) => return Ok(ctx.today_local() as i32),
        Some(Special::Tomorrow) => return Ok(ctx.today_local() as i32 + 1),
        Some(Special::Yesterday) => return Ok(ctx.today_local() as i32 - 1),
        _ => {}
    }
    let (y, m, d) = p.ymd.ok_or(DtErr::Syntax)?;
    check_year(y)?;
    Ok(date_from_ymd(y, m, d))
}

fn check_year(y: i64) -> Result<(), DtErr> {
    if !(-4713..=5_874_897).contains(&y) {
        return Err(DtErr::Range);
    }
    Ok(())
}

fn ts_from_parts(p: &Parsed, ctx: &Ctx, with_tz: bool) -> Result<i64, DtErr> {
    match p.special {
        Some(Special::Infinity) => return Ok(TS_INF),
        Some(Special::NegInfinity) => return Ok(TS_NEG_INF),
        Some(Special::Epoch) => return Ok(-PG_EPOCH_DAYS * USECS_PER_DAY),
        Some(Special::Now) => {
            return Ok(if with_tz { ctx.now } else { utc_to_local(ctx.now, ctx.zone) });
        }
        _ => {}
    }
    let day = match p.special {
        Some(Special::Today) => ctx.today_local(),
        Some(Special::Tomorrow) => ctx.today_local() + 1,
        Some(Special::Yesterday) => ctx.today_local() - 1,
        _ => {
            let (y, m, d) = p.ymd.ok_or(DtErr::Syntax)?;
            check_year(y)?;
            days_from_civil(y, m, d) - PG_EPOCH_DAYS
        }
    };
    let local = day
        .checked_mul(USECS_PER_DAY)
        .and_then(|v| v.checked_add(p.time.unwrap_or(0)))
        .ok_or(DtErr::Range)?;
    let ts = if with_tz {
        match &p.tz {
            Some(TzSpec::Offset(o)) => local - *o as i64 * USECS_PER_SEC,
            Some(TzSpec::Named(z)) => local_to_utc(local, z),
            None => local_to_utc(local, ctx.zone),
        }
    } else {
        local
    };
    if !(MIN_TS..MAX_TS).contains(&ts) {
        return Err(DtErr::Range);
    }
    Ok(ts)
}

pub fn parse_timestamp(s: &str, ctx: &Ctx) -> Result<i64, DtErr> {
    ts_from_parts(&parse_datetime(s)?, ctx, false)
}

pub fn parse_timestamptz(s: &str, ctx: &Ctx) -> Result<i64, DtErr> {
    ts_from_parts(&parse_datetime(s)?, ctx, true)
}

pub fn parse_time(s: &str) -> Result<i64, DtErr> {
    let p = parse_datetime(s)?;
    if p.special == Some(Special::Allballs) {
        return Ok(0);
    }
    if p.special.is_some() && p.special != Some(Special::Now) {
        return Err(DtErr::Syntax);
    }
    p.time.ok_or(DtErr::Syntax)
}

/// `timetz`: time plus the zone offset given (or the session's).
pub fn parse_timetz(s: &str, ctx: &Ctx) -> Result<(i64, i32), DtErr> {
    let p = parse_datetime(s)?;
    let t = p.time.ok_or(DtErr::Syntax)?;
    let off = match &p.tz {
        Some(TzSpec::Offset(o)) => *o,
        Some(TzSpec::Named(z)) => z.offset_at_utc(ctx.now / USECS_PER_SEC),
        None => ctx.zone.offset_at_utc(ctx.now / USECS_PER_SEC + PG_EPOCH_DAYS * 86400),
    };
    Ok((t, off))
}

// ---------------------------------------------------------------------------
// Intervals

fn unit_of(word: &str) -> Option<&'static str> {
    Some(match word {
        "microsecond" | "microseconds" | "us" | "usec" | "usecs" | "useconds" | "usecond" => "us",
        "millisecond" | "milliseconds" | "ms" | "msec" | "msecs" | "mseconds" | "msecond" => "ms",
        "second" | "seconds" | "s" | "sec" | "secs" => "s",
        "minute" | "minutes" | "m" | "min" | "mins" => "min",
        "hour" | "hours" | "h" | "hr" | "hrs" => "h",
        "day" | "days" | "d" => "d",
        "week" | "weeks" | "w" => "w",
        "month" | "months" | "mon" | "mons" => "mon",
        "year" | "years" | "y" | "yr" | "yrs" => "y",
        "decade" | "decades" | "dec" | "decs" => "dec",
        "century" | "centuries" | "c" | "cent" => "cent",
        "millennium" | "millennia" | "mil" | "mils" => "mil",
        _ => return None,
    })
}

#[derive(Default)]
struct IvAcc {
    months: f64,
    days: f64,
    micros: f64,
}

impl IvAcc {
    fn add(&mut self, v: f64, unit: &str) {
        match unit {
            "us" => self.micros += v,
            "ms" => self.micros += v * 1000.0,
            "s" => self.micros += v * 1e6,
            "min" => self.micros += v * 60e6,
            "h" => self.micros += v * 3600e6,
            "d" => self.days += v,
            "w" => self.days += v * 7.0,
            "mon" => self.months += v,
            "y" => self.months += v * 12.0,
            "dec" => self.months += v * 120.0,
            "cent" => self.months += v * 1200.0,
            "mil" => self.months += v * 12000.0,
            _ => {}
        }
    }

    fn finish(self) -> Result<Interval, DtErr> {
        // Cascade fractions down: months → days (30), days → micros.
        let months = self.months.trunc();
        let mdays = (self.months - months) * 30.0;
        let days_total = self.days + mdays;
        let days = days_total.trunc();
        let micros = self.micros + (days_total - days) * USECS_PER_DAY as f64;
        if months.abs() > i32::MAX as f64
            || days.abs() > i32::MAX as f64
            || micros.abs() > i64::MAX as f64
        {
            return Err(DtErr::Range);
        }
        Ok(Interval { months: months as i32, days: days as i32, micros: micros.round() as i64 })
    }
}

pub fn parse_interval(s: &str) -> Result<Interval, DtErr> {
    let t = s.trim();
    if t.starts_with(['P', 'p']) {
        return parse_iso_interval(&t[1..]);
    }
    let lower = t.to_ascii_lowercase();
    let lower = lower.strip_prefix('@').unwrap_or(&lower).trim();
    let mut toks: Vec<&str> = lower.split_whitespace().collect();
    let mut ago = false;
    if toks.last() == Some(&"ago") {
        ago = true;
        toks.pop();
    }
    if toks.is_empty() {
        return Err(DtErr::Syntax);
    }
    let mut acc = IvAcc::default();
    let mut i = 0;
    let mut any = false;
    while i < toks.len() {
        let tok = toks[i];
        if tok.contains(':') {
            // [-]HH:MM[:SS[.f]]
            let (neg, body) = match tok.strip_prefix('-') {
                Some(b) => (true, b),
                None => (false, tok.strip_prefix('+').unwrap_or(tok)),
            };
            let parts: Vec<&str> = body.split(':').collect();
            if parts.len() > 3 || parts.iter().any(|p| p.is_empty()) {
                return Err(DtErr::Syntax);
            }
            let h: f64 = parts[0].parse().map_err(|_| DtErr::Syntax)?;
            let m: f64 = parts[1].parse().map_err(|_| DtErr::Syntax)?;
            let sec: f64 =
                parts.get(2).map_or(Ok(0.0), |p| p.parse().map_err(|_| DtErr::Syntax))?;
            if m >= 60.0 || sec >= 60.0 {
                return Err(DtErr::Range);
            }
            let total = (h * 3600.0 + m * 60.0 + sec) * 1e6;
            acc.micros += if neg { -total } else { total };
            any = true;
            i += 1;
            continue;
        }
        // Number optionally glued to a unit: "1day", "5min".
        let split = tok.find(|c: char| c.is_ascii_alphabetic()).unwrap_or(tok.len());
        let (num_s, unit_s) = tok.split_at(split);
        if num_s.is_empty() {
            return Err(DtErr::Syntax);
        }
        let v: f64 = num_s.parse().map_err(|_| DtErr::Syntax)?;
        let unit = if !unit_s.is_empty() {
            unit_of(unit_s).ok_or(DtErr::Syntax)?
        } else if i + 1 < toks.len() && unit_of(toks[i + 1]).is_some() {
            i += 1;
            unit_of(toks[i]).unwrap()
        } else {
            "s"
        };
        acc.add(v, unit);
        any = true;
        i += 1;
    }
    if !any {
        return Err(DtErr::Syntax);
    }
    let iv = acc.finish()?;
    if ago { iv.neg() } else { Ok(iv) }
}

fn parse_iso_interval(s: &str) -> Result<Interval, DtErr> {
    let mut acc = IvAcc::default();
    let mut in_time = false;
    let mut num = String::new();
    for c in s.chars() {
        match c {
            'T' | 't' => in_time = true,
            '0'..='9' | '.' | '-' | '+' => num.push(c),
            _ => {
                let v: f64 = num.parse().map_err(|_| DtErr::Syntax)?;
                num.clear();
                let unit = match (c.to_ascii_uppercase(), in_time) {
                    ('Y', false) => "y",
                    ('M', false) => "mon",
                    ('W', false) => "w",
                    ('D', false) => "d",
                    ('H', true) => "h",
                    ('M', true) => "min",
                    ('S', true) => "s",
                    _ => return Err(DtErr::Syntax),
                };
                acc.add(v, unit);
            }
        }
    }
    if !num.is_empty() {
        return Err(DtErr::Syntax);
    }
    acc.finish()
}

// ---------------------------------------------------------------------------
// Arithmetic

/// Adds months to a local timestamp, clamping the day to the month's end.
fn add_months_local(local: i64, months: i32) -> Result<i64, DtErr> {
    if months == 0 {
        return Ok(local);
    }
    let days = local.div_euclid(USECS_PER_DAY);
    let tod = local.rem_euclid(USECS_PER_DAY);
    let (y, m, d) = civil_from_days(days + PG_EPOCH_DAYS);
    let total = y * 12 + (m as i64 - 1) + months as i64;
    let (ny, nm) = (total.div_euclid(12), total.rem_euclid(12) as u32 + 1);
    let nd = d.min(days_in_month(ny, nm));
    check_year(ny)?;
    Ok((days_from_civil(ny, nm, nd) - PG_EPOCH_DAYS) * USECS_PER_DAY + tod)
}

pub fn timestamp_add(ts: i64, iv: &Interval) -> Result<i64, DtErr> {
    if ts == TS_INF || ts == TS_NEG_INF {
        return Ok(ts);
    }
    let t = add_months_local(ts, iv.months)?;
    let t = t.checked_add(iv.days as i64 * USECS_PER_DAY).ok_or(DtErr::Range)?;
    let t = t.checked_add(iv.micros).ok_or(DtErr::Range)?;
    if !(MIN_TS..MAX_TS).contains(&t) {
        return Err(DtErr::Range);
    }
    Ok(t)
}

pub fn timestamptz_add(ts: i64, iv: &Interval, zone: &Zone) -> Result<i64, DtErr> {
    if ts == TS_INF || ts == TS_NEG_INF {
        return Ok(ts);
    }
    let mut t = ts;
    if iv.months != 0 || iv.days != 0 {
        let local = utc_to_local(ts, zone);
        let l = add_months_local(local, iv.months)?;
        let l = l.checked_add(iv.days as i64 * USECS_PER_DAY).ok_or(DtErr::Range)?;
        t = local_to_utc(l, zone);
    }
    let t = t.checked_add(iv.micros).ok_or(DtErr::Range)?;
    if !(MIN_TS..MAX_TS).contains(&t) {
        return Err(DtErr::Range);
    }
    Ok(t)
}

/// `timestamp - timestamp`: justified to days + time.
pub fn timestamp_diff(a: i64, b: i64) -> Result<Interval, DtErr> {
    if a == TS_INF || a == TS_NEG_INF || b == TS_INF || b == TS_NEG_INF {
        return Err(DtErr::Range);
    }
    let us = a.checked_sub(b).ok_or(DtErr::Range)?;
    Ok(Interval { months: 0, days: 0, micros: us }.justify_hours())
}

/// `age(a, b)`: symbolic years/months/days difference.
pub fn age(a: i64, b: i64) -> Interval {
    let (ad, at) = (a.div_euclid(USECS_PER_DAY), a.rem_euclid(USECS_PER_DAY));
    let (bd, bt) = (b.div_euclid(USECS_PER_DAY), b.rem_euclid(USECS_PER_DAY));
    let (y1, m1, d1) = civil_from_days(ad + PG_EPOCH_DAYS);
    let (y2, m2, d2) = civil_from_days(bd + PG_EPOCH_DAYS);
    let mut fsec = at - bt;
    let mut day = d1 as i64 - d2 as i64;
    let mut mon = m1 as i64 - m2 as i64;
    let mut year = y1 - y2;
    let neg = a < b;
    if neg {
        fsec = -fsec;
        day = -day;
        mon = -mon;
        year = -year;
    }
    while fsec < 0 {
        fsec += USECS_PER_DAY;
        day -= 1;
    }
    while day < 0 {
        // Borrow from the month before the later date.
        let (ry, rm) = if neg { (y1, m1) } else { (y2, m2) };
        day += days_in_month(ry, rm) as i64;
        mon -= 1;
    }
    while mon < 0 {
        mon += 12;
        year -= 1;
    }
    let mut iv = Interval { months: (year * 12 + mon) as i32, days: day as i32, micros: fsec };
    if neg {
        iv = iv.neg().unwrap_or(iv);
    }
    iv
}

/// Local timestamp fields used by EXTRACT, date_trunc, to_char.
#[derive(Debug, Clone, Copy)]
pub struct Fields {
    pub year: i64,
    pub month: u32,
    pub day: u32,
    pub hour: i64,
    pub minute: i64,
    pub micros: i64, // microseconds within the minute
    pub days: i64,   // days since 2000-01-01
}

pub fn fields(local: i64) -> Fields {
    let days = local.div_euclid(USECS_PER_DAY);
    let tod = local.rem_euclid(USECS_PER_DAY);
    let (year, month, day) = civil_from_days(days + PG_EPOCH_DAYS);
    Fields {
        year,
        month,
        day,
        hour: tod / (3600 * USECS_PER_SEC),
        minute: tod / (60 * USECS_PER_SEC) % 60,
        micros: tod % (60 * USECS_PER_SEC),
        days,
    }
}

/// ISO week number and ISO year.
pub fn iso_week(days: i64) -> (i64, i64) {
    let (y, _, _) = civil_from_days(days + PG_EPOCH_DAYS);
    let dow = |d: i64| (d + PG_EPOCH_DAYS + 3).rem_euclid(7); // 0 = Monday
    let week1_start = |yr: i64| {
        let jan4 = days_from_civil(yr, 1, 4) - PG_EPOCH_DAYS;
        jan4 - dow(jan4)
    };
    let mut iso_year = y;
    if days < week1_start(y) {
        iso_year = y - 1;
    } else if days >= week1_start(y + 1) {
        iso_year = y + 1;
    }
    ((days - week1_start(iso_year)) / 7 + 1, iso_year)
}

/// EXTRACT on a local timestamp. `None` for fields that don't apply.
/// Returns numeric with Postgres's scale (PG14+).
pub fn extract(
    field: &str,
    local: i64,
    utc: Option<i64>,
    tz_off: Option<i32>,
    is_date: bool,
) -> Option<Numeric> {
    let f = fields(local);
    let n = Numeric::from_i64;
    let sec_scaled = |micros: i64, scale: u32| {
        let v = Numeric::from_i64(micros);
        let d = Numeric::from_i64(10i64.pow(6 - scale));
        v.div_scale(&d, scale as i64, false).unwrap()
    };
    Some(match field {
        "year" => n(if f.year <= 0 { f.year - 1 } else { f.year }),
        "month" => n(f.month as i64),
        "day" => n(f.day as i64),
        "hour" if !is_date => n(f.hour),
        "minute" if !is_date => n(f.minute),
        "second" if !is_date => sec_scaled(f.micros, 6),
        "milliseconds" if !is_date => sec_scaled(f.micros * 1000, 6).round(3),
        "microseconds" if !is_date => n(f.micros),
        "quarter" => n((f.month as i64 - 1) / 3 + 1),
        "dow" => n((f.days + PG_EPOCH_DAYS + 4).rem_euclid(7)),
        "isodow" => n((f.days + PG_EPOCH_DAYS + 3).rem_euclid(7) + 1),
        "doy" => n(f.days - (days_from_civil(f.year, 1, 1) - PG_EPOCH_DAYS) + 1),
        "week" => n(iso_week(f.days).0),
        "isoyear" => {
            let y = iso_week(f.days).1;
            n(if y <= 0 { y - 1 } else { y })
        }
        "decade" => n(if f.year >= 0 { f.year / 10 } else { -((8 - (f.year - 1)) / 10) }),
        "century" => n(if f.year > 0 { (f.year + 99) / 100 } else { -((99 - (f.year - 1)) / 100) }),
        "millennium" => {
            n(if f.year > 0 { (f.year + 999) / 1000 } else { -((999 - (f.year - 1)) / 1000) })
        }
        "julian" => {
            let jd = f.days + 2_451_545;
            if is_date {
                n(jd)
            } else {
                let tod = local.rem_euclid(USECS_PER_DAY);
                Numeric::from_i64(jd).add(
                    &Numeric::from_i64(tod)
                        .div_scale(&Numeric::from_i64(USECS_PER_DAY), 18, false)
                        .ok()?,
                )
            }
        }
        "epoch" => {
            let base = utc.unwrap_or(local);
            let us = base as i128 + PG_EPOCH_DAYS as i128 * USECS_PER_DAY as i128;
            if is_date {
                Numeric::from_i128(us / USECS_PER_SEC as i128)
            } else {
                Numeric::from_i128(us)
                    .div_scale(&Numeric::from_i64(USECS_PER_SEC), 6, false)
                    .ok()?
            }
        }
        "timezone" => n(tz_off? as i64),
        "timezone_hour" => n(tz_off? as i64 / 3600),
        "timezone_minute" => n(tz_off? as i64 / 60 % 60),
        _ => return None,
    })
}

/// EXTRACT from an interval.
pub fn extract_interval(field: &str, iv: &Interval) -> Option<Numeric> {
    let n = Numeric::from_i64;
    let us = iv.micros;
    Some(match field {
        "microseconds" => n(us % (60 * USECS_PER_SEC)),
        "milliseconds" => {
            Numeric::from_i64(us % (60 * USECS_PER_SEC)).div_scale(&n(1000), 3, false).ok()?
        }
        "second" => Numeric::from_i64(us % (60 * USECS_PER_SEC))
            .div_scale(&n(USECS_PER_SEC), 6, false)
            .ok()?,
        "minute" => n(us / (60 * USECS_PER_SEC) % 60),
        "hour" => n(us / (3600 * USECS_PER_SEC)),
        "day" => n(iv.days as i64),
        "month" => n((iv.months % 12) as i64),
        "quarter" => n((iv.months % 12 / 3 + 1) as i64),
        "year" => n((iv.months / 12) as i64),
        "decade" => n((iv.months / 120) as i64),
        "century" => n((iv.months / 1200) as i64),
        "millennium" => n((iv.months / 12000) as i64),
        "epoch" => {
            let secs_months =
                iv.months as i128 / 12 * 31_557_600 + (iv.months as i128 % 12) * 2_592_000;
            let total_us =
                (secs_months + iv.days as i128 * 86400) * USECS_PER_SEC as i128 + us as i128;
            Numeric::from_i128(total_us).div_scale(&n(USECS_PER_SEC), 6, false).ok()?
        }
        _ => return None,
    })
}

/// `date_trunc` on a local timestamp.
pub fn trunc_local(field: &str, local: i64) -> Option<i64> {
    let f = fields(local);
    let day_start = f.days * USECS_PER_DAY;
    let ymd = |y: i64, m: u32, d: u32| (days_from_civil(y, m, d) - PG_EPOCH_DAYS) * USECS_PER_DAY;
    Some(match field {
        "microseconds" => local,
        "milliseconds" => local - local.rem_euclid(1000),
        "second" => local - local.rem_euclid(USECS_PER_SEC),
        "minute" => local - local.rem_euclid(60 * USECS_PER_SEC),
        "hour" => local - local.rem_euclid(3600 * USECS_PER_SEC),
        "day" => day_start,
        "week" => {
            let dow = (f.days + PG_EPOCH_DAYS + 3).rem_euclid(7);
            (f.days - dow) * USECS_PER_DAY
        }
        "month" => ymd(f.year, f.month, 1),
        "quarter" => ymd(f.year, (f.month - 1) / 3 * 3 + 1, 1),
        "year" => ymd(f.year, 1, 1),
        "decade" => ymd(f.year.div_euclid(10) * 10, 1, 1),
        "century" => ymd(
            if f.year > 0 {
                (f.year - 1) / 100 * 100 + 1
            } else {
                -((99 - f.year) / 100 * 100) + 1
            },
            1,
            1,
        ),
        "millennium" => ymd(
            if f.year > 0 {
                (f.year - 1) / 1000 * 1000 + 1
            } else {
                -((999 - f.year) / 1000 * 1000) + 1
            },
            1,
            1,
        ),
        _ => return None,
    })
}

pub fn trunc_interval(field: &str, iv: &Interval) -> Option<Interval> {
    let mut r = *iv;
    let us = iv.micros;
    match field {
        "microseconds" => {}
        "milliseconds" => r.micros = us - us % 1000,
        "second" => r.micros = us - us % USECS_PER_SEC,
        "minute" => r.micros = us - us % (60 * USECS_PER_SEC),
        "hour" => r.micros = us - us % (3600 * USECS_PER_SEC),
        "day" => r.micros = 0,
        "month" => {
            r.micros = 0;
            r.days = 0;
        }
        "quarter" => {
            r = Interval { months: iv.months - iv.months % 3, days: 0, micros: 0 };
        }
        "year" => r = Interval { months: iv.months - iv.months % 12, days: 0, micros: 0 },
        "decade" => r = Interval { months: iv.months - iv.months % 120, days: 0, micros: 0 },
        "century" => r = Interval { months: iv.months - iv.months % 1200, days: 0, micros: 0 },
        "millennium" => r = Interval { months: iv.months - iv.months % 12000, days: 0, micros: 0 },
        _ => return None,
    }
    Some(r)
}

/// Unix seconds (with fraction) to a Postgres timestamp.
pub fn from_unix_seconds(secs: f64) -> Result<i64, DtErr> {
    if secs.is_infinite() {
        return Ok(if secs > 0.0 { TS_INF } else { TS_NEG_INF });
    }
    if secs.is_nan() {
        return Err(DtErr::Range);
    }
    let us = (secs * 1e6).round() - (PG_EPOCH_DAYS * USECS_PER_DAY) as f64;
    if !(MIN_TS as f64..MAX_TS as f64).contains(&us) {
        return Err(DtErr::Range);
    }
    Ok(us as i64)
}

/// Rounds a timestamp/time to `precision` fractional digits (typmod).
pub fn round_micros(v: i64, precision: i32) -> i64 {
    if !(0..6).contains(&precision) || v == TS_INF || v == TS_NEG_INF {
        return v;
    }
    let unit = 10i64.pow(6 - precision as u32);
    let r = v.rem_euclid(unit);
    let base = v - r;
    if r * 2 >= unit { base + unit } else { base }
}

pub fn now_micros() -> i64 {
    let d = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
    d.as_micros() as i64 - PG_EPOCH_DAYS * USECS_PER_DAY
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx_utc() -> (i64, Zone) {
        (0, Zone::utc())
    }

    #[test]
    fn civil_roundtrip() {
        for d in [-800_000i64, -1, 0, 1, 10957, 18262, 2_000_000] {
            let (y, m, dd) = civil_from_days(d);
            assert_eq!(days_from_civil(y, m, dd), d);
        }
        assert_eq!(days_from_civil(2000, 1, 1), PG_EPOCH_DAYS);
    }

    #[test]
    fn date_io() {
        let (now, z) = ctx_utc();
        let ctx = Ctx { now, zone: &z };
        assert_eq!(parse_date("2020-02-29", &ctx).unwrap(), date_from_ymd(2020, 2, 29));
        assert_eq!(format_date(parse_date("2020-02-29", &ctx).unwrap()), "2020-02-29");
        assert_eq!(parse_date("2021-02-29", &ctx), Err(DtErr::Range));
        assert_eq!(parse_date("garbage", &ctx), Err(DtErr::Syntax));
        assert_eq!(format_date(date_from_ymd(1, 1, 1) - 1), "0001-12-31 BC");
        assert_eq!(parse_date("Jan 5, 2020", &ctx).unwrap(), date_from_ymd(2020, 1, 5));
        assert_eq!(parse_date("01/05/2020", &ctx).unwrap(), date_from_ymd(2020, 1, 5));
    }

    #[test]
    fn timestamp_io() {
        let (now, z) = ctx_utc();
        let ctx = Ctx { now, zone: &z };
        let t = parse_timestamptz("2020-01-01 10:00:00.5+05:30", &ctx).unwrap();
        assert_eq!(format_timestamptz(t, &z), "2020-01-01 04:30:00.5+00");
        let t = parse_timestamp("2020-01-01T10:00:00Z", &ctx).unwrap();
        assert_eq!(format_timestamp(t), "2020-01-01 10:00:00");
        let t = parse_timestamp("2020-01-01 11:30 pm", &ctx).unwrap();
        assert_eq!(format_timestamp(t), "2020-01-01 23:30:00");
        assert_eq!(parse_time("24:00").unwrap(), USECS_PER_DAY);
        assert_eq!(parse_time("25:00"), Err(DtErr::Range));
    }

    #[test]
    fn interval_io() {
        let f = |s: &str| format_interval(&parse_interval(s).unwrap());
        assert_eq!(f("-1 days 2 hours"), "-1 days +02:00:00");
        assert_eq!(f("1 year -2 mons"), "10 mons");
        assert_eq!(f("1.5 days"), "1 day 12:00:00");
        assert_eq!(f("36 hours"), "36:00:00");
        assert_eq!(f("0"), "00:00:00");
        assert_eq!(f("-00:00:01.5"), "-00:00:01.5");
        assert_eq!(f("P1Y2M3DT4H5M6S"), "1 year 2 mons 3 days 04:05:06");
        assert_eq!(f("1 week ago"), "-7 days");
        assert_eq!(f("2 mons 1 day -3 min"), "2 mons 1 day -00:03:00");
    }

    #[test]
    fn arithmetic() {
        let (now, z) = ctx_utc();
        let ctx = Ctx { now, zone: &z };
        let t = parse_timestamp("2020-02-29", &ctx).unwrap();
        let r = timestamp_add(t, &parse_interval("1 year").unwrap()).unwrap();
        assert_eq!(format_timestamp(r), "2021-02-28 00:00:00");
        let a = parse_timestamp("2020-01-02 02:00", &ctx).unwrap();
        let b = parse_timestamp("2020-01-01", &ctx).unwrap();
        assert_eq!(format_interval(&timestamp_diff(a, b).unwrap()), "1 day 02:00:00");
        let a = parse_timestamp("2021-03-15", &ctx).unwrap();
        let b = parse_timestamp("2020-01-20", &ctx).unwrap();
        assert_eq!(format_interval(&age(a, b)), "1 year 1 mon 26 days");
    }
}
