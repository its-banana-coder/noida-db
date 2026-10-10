//! Elasticsearch dates: parsing (`strict_date_optional_time||epoch_millis`
//! and Java-style `format` patterns), date math (`now-1d/d`,
//! `2024-01-15||+1M/M`), fixed-offset time zones, and the calendar
//! arithmetic `date_histogram` needs. Everything is epoch milliseconds,
//! UTC, the way Elasticsearch stores a `date` field.

const MS_DAY: i64 = 86_400_000;

/// Days since 1970-01-01 for a proleptic Gregorian date (Howard Hinnant's
/// `days_from_civil`).
pub fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// The (year, month, day) of a day number from `days_from_civil`.
pub fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

fn days_in_month(y: i64, m: i64) -> i64 {
    days_from_civil(if m == 12 { y + 1 } else { y }, if m == 12 { 1 } else { m + 1 }, 1)
        - days_from_civil(y, m, 1)
}

/// Broken-down UTC fields of `ms`: (y, mo, d, h, mi, s, millis).
pub fn fields(ms: i64) -> (i64, i64, i64, i64, i64, i64, i64) {
    let days = ms.div_euclid(MS_DAY);
    let rem = ms.rem_euclid(MS_DAY);
    let (y, mo, d) = civil_from_days(days);
    (y, mo, d, rem / 3_600_000, rem / 60_000 % 60, rem / 1000 % 60, rem % 1000)
}

fn from_fields(y: i64, mo: i64, d: i64, h: i64, mi: i64, s: i64, ms: i64) -> i64 {
    days_from_civil(y, mo, d) * MS_DAY + h * 3_600_000 + mi * 60_000 + s * 1000 + ms
}

/// A fixed UTC offset in milliseconds: `Z`, `UTC`, `+05:00`, `-0800`,
/// `+5`. Region ids (`Europe/Paris`) aren't supported.
pub fn parse_offset(s: &str) -> Option<i64> {
    let s = s.trim();
    if s.eq_ignore_ascii_case("z") || s.eq_ignore_ascii_case("utc") || s == "GMT" {
        return Some(0);
    }
    let s = s.strip_prefix("UTC").or_else(|| s.strip_prefix("GMT")).unwrap_or(s);
    let (sign, rest) = match s.as_bytes().first()? {
        b'+' => (1, &s[1..]),
        b'-' => (-1, &s[1..]),
        _ => return None,
    };
    let digits: String = rest.chars().filter(|c| *c != ':').collect();
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let (h, m) = match digits.len() {
        1 | 2 => (digits.parse::<i64>().ok()?, 0),
        3 => (digits[..1].parse().ok()?, digits[1..].parse().ok()?),
        4 => (digits[..2].parse().ok()?, digits[2..].parse().ok()?),
        _ => return None,
    };
    if h > 18 || m > 59 {
        return None;
    }
    Some(sign * (h * 3_600_000 + m * 60_000))
}

/// `strict_date_optional_time`: `yyyy[-MM[-dd['T'HH[:mm[:ss[.S+]]]]]]` with
/// an optional `Z`/offset, read in `tz` when it carries none.
fn parse_iso(s: &str, tz: i64) -> Option<i64> {
    let b = s.as_bytes();
    let num = |from: usize, len: usize| -> Option<i64> {
        let part = s.get(from..from + len)?;
        part.bytes().all(|c| c.is_ascii_digit()).then(|| part.parse().ok())?
    };
    let y = num(0, 4)?;
    let (mut mo, mut d, mut h, mut mi, mut sec, mut ms) = (1, 1, 0, 0, 0, 0);
    let mut i = 4;
    if b.get(i) == Some(&b'-') {
        mo = num(i + 1, 2)?;
        i += 3;
        if b.get(i) == Some(&b'-') {
            d = num(i + 1, 2)?;
            i += 3;
            if matches!(b.get(i), Some(b'T') | Some(b't')) {
                h = num(i + 1, 2)?;
                i += 3;
                if b.get(i) == Some(&b':') {
                    mi = num(i + 1, 2)?;
                    i += 3;
                    if b.get(i) == Some(&b':') {
                        sec = num(i + 1, 2)?;
                        i += 3;
                        if matches!(b.get(i), Some(b'.') | Some(b',')) {
                            let start = i + 1;
                            let mut end = start;
                            while b.get(end).is_some_and(u8::is_ascii_digit) {
                                end += 1;
                            }
                            if end == start || end - start > 9 {
                                return None;
                            }
                            let frac = &s[start..end];
                            let padded = format!("{frac:0<3}");
                            ms = padded[..3].parse().ok()?;
                            i = end;
                        }
                    }
                }
            }
        }
    }
    if !(1..=12).contains(&mo) || d < 1 || d > days_in_month(y, mo) || h > 23 || mi > 59 || sec > 59
    {
        return None;
    }
    let offset = if i == s.len() { tz } else { parse_offset(&s[i..])? };
    Some(from_fields(y, mo, d, h, mi, sec, ms) - offset)
}

/// A Java `DateTimeFormatter`-style pattern (`dd/MM/yyyy HH:mm`): the
/// letters y M d H m s S, quoted literals, and anything else literal.
fn parse_pattern(s: &str, pattern: &str, tz: i64) -> Option<i64> {
    let (mut y, mut mo, mut d, mut h, mut mi, mut sec, mut ms) = (1970, 1, 1, 0, 0, 0, 0);
    let mut offset = None;
    let pat: Vec<char> = pattern.chars().collect();
    let text: Vec<char> = s.chars().collect();
    let (mut pi, mut ti) = (0, 0);
    while pi < pat.len() {
        let c = pat[pi];
        if c == '\'' {
            pi += 1;
            while pi < pat.len() && pat[pi] != '\'' {
                if text.get(ti) != Some(&pat[pi]) {
                    return None;
                }
                pi += 1;
                ti += 1;
            }
            pi += 1;
            continue;
        }
        let mut run = 1;
        while pi + run < pat.len() && pat[pi + run] == c {
            run += 1;
        }
        if c.is_ascii_alphabetic() {
            // Day and month names (`E`, `MMM`): English or French words,
            // abbreviated with a trailing `.` or not.
            if c == 'E' || (c == 'M' && run >= 3) {
                let start = ti;
                while ti < text.len() && (text[ti].is_alphabetic() || text[ti] == '.') {
                    ti += 1;
                }
                let word: String = text[start..ti].iter().collect::<String>().to_lowercase();
                if word.trim_end_matches('.').is_empty() {
                    return None;
                }
                if c == 'M' {
                    mo = month_number(word.trim_end_matches('.'))?;
                }
                pi += run;
                continue;
            }
            if c == 'X' || c == 'Z' {
                let start = ti;
                while ti < text.len() && (text[ti].is_ascii_digit() || "+-:Z".contains(text[ti])) {
                    ti += 1;
                }
                offset = Some(parse_offset(&text[start..ti].iter().collect::<String>())?);
                pi += run;
                continue;
            }
            // Variable width for a single letter, fixed width otherwise.
            let max =
                if run == 1 { 9 } else { run.max(if c == 'y' || c == 'u' { 4 } else { run }) };
            let start = ti;
            while ti < text.len() && ti - start < max && text[ti].is_ascii_digit() {
                ti += 1;
            }
            if ti == start {
                return None;
            }
            let v: i64 = text[start..ti].iter().collect::<String>().parse().ok()?;
            match c {
                'y' | 'u' => y = v,
                'M' => mo = v,
                'd' => d = v,
                'H' => h = v,
                'm' => mi = v,
                's' => sec = v,
                'S' => ms = v,
                _ => return None,
            }
            pi += run;
        } else {
            for _ in 0..run {
                if text.get(ti) != Some(&c) {
                    return None;
                }
                ti += 1;
            }
            pi += run;
        }
    }
    if ti != text.len()
        || !(1..=12).contains(&mo)
        || d < 1
        || d > days_in_month(y, mo)
        || h > 23
        || mi > 59
        || sec > 59
    {
        return None;
    }
    Some(from_fields(y, mo, d, h, mi, sec, ms) - offset.unwrap_or(tz))
}

/// A month's number from its English or French name or abbreviation.
fn month_number(word: &str) -> Option<i64> {
    const NAMES: [[&str; 12]; 2] = [
        [
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
        ],
        [
            "janvier",
            "février",
            "mars",
            "avril",
            "mai",
            "juin",
            "juillet",
            "août",
            "septembre",
            "octobre",
            "novembre",
            "décembre",
        ],
    ];
    const FRENCH_SHORT: [&str; 12] =
        ["janv", "févr", "mars", "avr", "mai", "juin", "juil", "août", "sept", "oct", "nov", "déc"];
    if let Some(i) = FRENCH_SHORT.iter().position(|m| *m == word) {
        return Some(i as i64 + 1);
    }
    if word.chars().count() < 3 {
        return None;
    }
    NAMES
        .iter()
        .find_map(|names| names.iter().position(|m| m.starts_with(word)))
        .map(|i| i as i64 + 1)
}

/// Parses one date the way a `date` field with `format` does (default
/// `strict_date_optional_time||epoch_millis`); `tz` applies to values
/// without an offset.
pub fn parse(s: &str, format: Option<&str>, tz: i64) -> Option<i64> {
    let format = format.unwrap_or("strict_date_optional_time||epoch_millis");
    for f in format.split("||") {
        let got = match f.trim() {
            "strict_date_optional_time"
            | "date_optional_time"
            | "strict_date_time"
            | "date_time"
            | "strict_date"
            | "date"
            | "iso8601" => parse_iso(s, tz),
            "epoch_millis" => {
                s.parse::<i64>().ok().or_else(|| s.parse::<f64>().ok().map(|f| f as i64))
            }
            "epoch_second" => s.parse::<f64>().ok().map(|f| (f * 1000.0) as i64),
            "basic_date" => parse_pattern(s, "yyyyMMdd", tz),
            "year_month_day" => parse_pattern(s, "yyyy-MM-dd", tz),
            "year_month" => parse_pattern(s, "yyyy-MM", tz),
            "year" => parse_pattern(s, "yyyy", tz),
            p => parse_pattern(s, p, tz),
        };
        if got.is_some() {
            return got;
        }
    }
    None
}

/// A JSON date value (string or epoch-millis number) as epoch millis.
pub fn value_millis(v: &serde_json::Value, format: Option<&str>) -> Option<i64> {
    match v {
        serde_json::Value::Number(n) => n.as_i64().or_else(|| n.as_f64().map(|f| f as i64)),
        serde_json::Value::String(s) => parse(s, format, 0),
        _ => None,
    }
}

/// Rounds `ms` (seen in `tz`) down to the start of `unit`.
pub fn round_down(ms: i64, unit: char, tz: i64) -> i64 {
    let local = ms + tz;
    let (y, mo, d, h, mi, s, _) = fields(local);
    let r = match unit {
        'y' => from_fields(y, 1, 1, 0, 0, 0, 0),
        'q' => from_fields(y, (mo - 1) / 3 * 3 + 1, 1, 0, 0, 0, 0),
        'M' => from_fields(y, mo, 1, 0, 0, 0, 0),
        'w' => {
            // ISO weeks start on Monday; 1970-01-01 was a Thursday.
            let days = local.div_euclid(MS_DAY);
            let monday = days - (days + 3).rem_euclid(7);
            monday * MS_DAY
        }
        'd' => from_fields(y, mo, d, 0, 0, 0, 0),
        'h' | 'H' => from_fields(y, mo, d, h, 0, 0, 0),
        'm' => from_fields(y, mo, d, h, mi, 0, 0),
        's' => from_fields(y, mo, d, h, mi, s, 0),
        _ => local,
    };
    r - tz
}

/// Adds `n` of `unit` to `ms` in `tz` (calendar units clamp the day of
/// month, as Java's `plusMonths` does).
pub fn add(ms: i64, n: i64, unit: char, tz: i64) -> i64 {
    match unit {
        'y' | 'M' | 'q' => {
            let months = match unit {
                'y' => n * 12,
                'q' => n * 3,
                _ => n,
            };
            let local = ms + tz;
            let (y, mo, d, ..) = fields(local);
            let time = local.rem_euclid(MS_DAY);
            let total = y * 12 + (mo - 1) + months;
            let (ny, nmo) = (total.div_euclid(12), total.rem_euclid(12) + 1);
            let nd = d.min(days_in_month(ny, nmo));
            days_from_civil(ny, nmo, nd) * MS_DAY + time - tz
        }
        'w' => ms + n * 7 * MS_DAY,
        'd' => ms + n * MS_DAY,
        'h' | 'H' => ms + n * 3_600_000,
        'm' => ms + n * 60_000,
        's' => ms + n * 1000,
        _ => ms,
    }
}

/// Evaluates date math: `now`, or a date followed by `||`, then any number
/// of `+N<unit>`, `-N<unit>` and `/<unit>`. `round_up` makes `/unit` go
/// to the last millisecond of the unit (Elasticsearch does that for
/// `gt` and `lte` bounds).
pub fn parse_math(s: &str, now: i64, round_up: bool, format: Option<&str>, tz: i64) -> Option<i64> {
    let (mut t, math) = if let Some(rest) = s.strip_prefix("now") {
        (now, rest)
    } else if let Some(pos) = s.find("||") {
        (parse(&s[..pos], format, tz)?, &s[pos + 2..])
    } else {
        return parse(s, format, tz);
    };
    let b = math.as_bytes();
    let mut i = 0;
    while i < b.len() {
        let op = b[i];
        i += 1;
        if op == b'/' {
            let unit = *b.get(i)? as char;
            i += 1;
            if !"yMwdhHmsq".contains(unit) {
                return None;
            }
            t = round_down(t, unit, tz);
            if round_up {
                t = add(t, 1, unit, tz) - 1;
            }
            continue;
        }
        if op != b'+' && op != b'-' {
            return None;
        }
        let start = i;
        while b.get(i).is_some_and(u8::is_ascii_digit) {
            i += 1;
        }
        let n: i64 = if i == start { 1 } else { math[start..i].parse().ok()? };
        let unit = *b.get(i)? as char;
        i += 1;
        if !"yMwdhHms".contains(unit) {
            return None;
        }
        t = add(t, if op == b'-' { -n } else { n }, unit, tz);
    }
    Some(t)
}

/// Formats `ms` with a Java-style pattern, or Elasticsearch's default
/// `strict_date_optional_time` (`2024-01-15T10:00:00.000Z`), in `tz`.
pub fn format(ms: i64, pattern: Option<&str>, tz: i64) -> String {
    let local = ms + tz;
    let (y, mo, d, h, mi, s, milli) = fields(local);
    let zone = if tz == 0 {
        "Z".to_string()
    } else {
        let a = tz.abs() / 60_000;
        format!("{}{:02}:{:02}", if tz < 0 { '-' } else { '+' }, a / 60, a % 60)
    };
    let pattern = match pattern.map(|p| p.split("||").next().unwrap_or(p).trim()) {
        None | Some("strict_date_optional_time") | Some("date_optional_time") => {
            return format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}.{milli:03}{zone}");
        }
        Some("epoch_millis") => return ms.to_string(),
        Some("epoch_second") => return (ms.div_euclid(1000)).to_string(),
        Some("strict_date") | Some("date") | Some("year_month_day") => "yyyy-MM-dd",
        Some(p) => p,
    };
    let pat: Vec<char> = pattern.chars().collect();
    let mut out = String::new();
    let mut i = 0;
    while i < pat.len() {
        let c = pat[i];
        if c == '\'' {
            i += 1;
            while i < pat.len() && pat[i] != '\'' {
                out.push(pat[i]);
                i += 1;
            }
            i += 1;
            continue;
        }
        let mut run = 1;
        while i + run < pat.len() && pat[i + run] == c {
            run += 1;
        }
        let num = |v: i64, w: usize| format!("{v:0w$}");
        match c {
            'y' | 'u' => out.push_str(&if run == 2 { num(y % 100, 2) } else { num(y, run) }),
            'M' => out.push_str(&num(mo, run)),
            'd' => out.push_str(&num(d, run)),
            'H' => out.push_str(&num(h, run)),
            'm' => out.push_str(&num(mi, run)),
            's' => out.push_str(&num(s, run)),
            'S' => out.push_str(&num(milli, 3)[..run.min(3)]),
            'X' | 'Z' => out.push_str(&zone),
            _ => {
                for _ in 0..run {
                    out.push(c);
                }
            }
        }
        i += run;
    }
    out
}

pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn civil_round_trip() {
        for days in [-800_000, -1, 0, 1, 19_737, 60_000] {
            let (y, m, d) = civil_from_days(days);
            assert_eq!(days_from_civil(y, m, d), days);
        }
        assert_eq!(days_from_civil(2024, 1, 15), 19_737);
    }

    #[test]
    fn parses_iso_and_patterns() {
        assert_eq!(parse("2024-01-15T10:00:00Z", None, 0), Some(1_705_312_800_000));
        assert_eq!(parse("2024-01-15", None, 0), Some(1_705_276_800_000));
        assert_eq!(parse("2024-01-15T15:00:00+05:00", None, 0), Some(1_705_312_800_000));
        assert_eq!(parse("1705312800000", None, 0), Some(1_705_312_800_000));
        assert_eq!(parse("15/01/2024", Some("dd/MM/yyyy"), 0), Some(1_705_276_800_000));
        assert_eq!(parse("2024-02-30", None, 0), None);
        assert_eq!(parse("not a date", None, 0), None);
    }

    #[test]
    fn date_math() {
        let base = parse("2024-01-15T10:00:00Z", None, 0).unwrap();
        let feb = parse("2024-02-01", None, 0).unwrap();
        assert_eq!(parse_math("2024-01-15T10:00:00Z||+1M/M", 0, false, None, 0), Some(feb));
        assert_eq!(
            parse_math("2024-01-15||/d", 0, true, None, 0),
            Some(parse("2024-01-16", None, 0).unwrap() - 1)
        );
        assert_eq!(parse_math("now-1d", base, false, None, 0), Some(base - MS_DAY));
        // Jan 31 + 1 month clamps to Feb 29 (2024 is a leap year).
        assert_eq!(parse_math("2024-01-31||+1M", 0, false, None, 0), parse("2024-02-29", None, 0));
        // 2024-01-15 is a Monday.
        assert_eq!(round_down(base, 'w', 0), parse("2024-01-15", None, 0).unwrap());
    }

    #[test]
    fn formats() {
        let t = parse("2024-01-15T10:00:00Z", None, 0).unwrap();
        assert_eq!(format(t, None, 0), "2024-01-15T10:00:00.000Z");
        assert_eq!(format(t, Some("yyyy-MM"), 0), "2024-01");
        assert_eq!(format(t, None, 5 * 3_600_000), "2024-01-15T15:00:00.000+05:00");
    }
}
