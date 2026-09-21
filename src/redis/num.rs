//! Number parsing and formatting with Redis's exact rules.

/// Parses an integer the way Redis's `string2ll` does: no `+`, no spaces, no
/// leading zeros, and no overflow.
pub fn parse_int(b: &[u8]) -> Option<i64> {
    if b.is_empty() || b.len() > 20 {
        return None;
    }
    if b == b"0" {
        return Some(0);
    }
    let (neg, digits) = match b[0] {
        b'-' => (true, &b[1..]),
        _ => (false, b),
    };
    if !matches!(digits.first(), Some(b'1'..=b'9')) || !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    let mut n: i64 = 0;
    for &d in digits {
        let d = (d - b'0') as i64;
        n = n.checked_mul(10)?;
        n = if neg { n.checked_sub(d)? } else { n.checked_add(d)? };
    }
    Some(n)
}

/// Parses a float the way Redis's `string2ld` does: `strtold` syntax,
/// no surrounding spaces, NaN rejected, infinities allowed.
pub fn parse_float(b: &[u8]) -> Option<f64> {
    let s = std::str::from_utf8(b).ok()?;
    if s.is_empty() || s.starts_with(char::is_whitespace) || s.ends_with(char::is_whitespace) {
        return None;
    }
    let v: f64 = s.parse().ok()?;
    (!v.is_nan()).then_some(v)
}

const SCALE_DIGITS: u32 = 17;
const SCALE: i128 = 10i128.pow(SCALE_DIGITS);

/// Parses a finite decimal into an integer scaled by 10^17, when that is
/// exact. `None` means the value needs more precision or range than that.
fn parse_scaled(b: &[u8]) -> Option<i128> {
    let s = std::str::from_utf8(b).ok()?;
    let (mantissa, exp) = match s.find(['e', 'E']) {
        Some(i) => (&s[..i], s[i + 1..].parse::<i32>().ok()?),
        None => (s, 0),
    };
    let (neg, mantissa) = match mantissa.as_bytes().first() {
        Some(b'-') => (true, &mantissa[1..]),
        Some(b'+') => (false, &mantissa[1..]),
        _ => (false, mantissa),
    };
    let (int_part, frac_part) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if int_part.is_empty() && frac_part.is_empty() {
        return None;
    }
    let digits = format!("{int_part}{frac_part}");
    if !digits.bytes().all(|c| c.is_ascii_digit()) || digits.len() > 36 {
        return None;
    }
    let mut value: i128 = if digits.is_empty() { 0 } else { digits.parse().ok()? };
    let shift = SCALE_DIGITS as i32 + exp - frac_part.len() as i32;
    if shift >= 0 {
        value = value.checked_mul(10i128.checked_pow(shift as u32)?)?;
    } else {
        let div = 10i128.checked_pow((-shift) as u32)?;
        if value % div != 0 {
            return None;
        }
        value /= div;
    }
    Some(if neg { -value } else { value })
}

fn format_scaled(v: i128) -> String {
    let sign = if v < 0 { "-" } else { "" };
    let abs = v.unsigned_abs();
    let int = abs / SCALE as u128;
    let frac = abs % SCALE as u128;
    if frac == 0 {
        return format!("{sign}{int}");
    }
    let frac = format!("{frac:017}");
    format!("{sign}{int}.{}", frac.trim_end_matches('0'))
}

/// Formats like Redis's `ld2string(.., LD_STR_HUMAN)`: 17 decimals with
/// trailing zeros (and a trailing dot) removed.
pub fn format_human(v: f64) -> String {
    let s = format!("{v:.17}");
    let s = s.trim_end_matches('0').trim_end_matches('.');
    if s == "-0" { "0".into() } else { s.into() }
}

/// Adds two decimals for INCRBYFLOAT / HINCRBYFLOAT. Redis does this in
/// `long double`; exact decimal arithmetic reproduces its output for the
/// values people actually use (e.g. 0.1 + 0.2 = "0.3").
pub fn add_human(a: &[u8], b: &[u8]) -> Result<String, &'static str> {
    let (x, y) = match (parse_float(a), parse_float(b)) {
        (Some(x), Some(y)) => (x, y),
        _ => return Err("ERR value is not a valid float"),
    };
    let sum = x + y;
    if !sum.is_finite() {
        return Err("ERR increment would produce NaN or Infinity");
    }
    if let (Some(sa), Some(sb)) = (parse_scaled(a), parse_scaled(b)) {
        if let Some(total) = sa.checked_add(sb) {
            return Ok(format_scaled(total));
        }
    }
    Ok(format_human(sum))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strict_integers() {
        assert_eq!(parse_int(b"0"), Some(0));
        assert_eq!(parse_int(b"-12"), Some(-12));
        assert_eq!(parse_int(b"9223372036854775807"), Some(i64::MAX));
        assert_eq!(parse_int(b"-9223372036854775808"), Some(i64::MIN));
        for bad in ["", "+1", " 1", "1 ", "01", "-0", "9223372036854775808", "1.0", "-"] {
            assert_eq!(parse_int(bad.as_bytes()), None, "{bad:?}");
        }
    }

    #[test]
    fn floats() {
        assert_eq!(parse_float(b"1.5"), Some(1.5));
        assert_eq!(parse_float(b"5.0e3"), Some(5000.0));
        assert_eq!(parse_float(b"inf"), Some(f64::INFINITY));
        assert_eq!(parse_float(b"nan"), None);
        assert_eq!(parse_float(b" 1"), None);
        assert_eq!(parse_float(b"abc"), None);
    }

    #[test]
    fn human_addition() {
        let add = |a: &str, b: &str| add_human(a.as_bytes(), b.as_bytes()).unwrap();
        assert_eq!(add("10.50", "0.1"), "10.6");
        assert_eq!(add("0.1", "0.2"), "0.3");
        assert_eq!(add("5.0e3", "2.0e2"), "5200");
        assert_eq!(add("1", "-1"), "0");
        assert_eq!(add("-1.5", "0.25"), "-1.25");
        assert_eq!(add(".5", "1"), "1.5");
    }
}
