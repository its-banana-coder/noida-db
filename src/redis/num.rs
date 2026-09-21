//! Number parsing and formatting with Redis's exact rules.

use super::longdouble;

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

/// INCRBYFLOAT / HINCRBYFLOAT arithmetic. Redis does it in x87 `long
/// double`, so it's emulated exactly (see `longdouble`).
pub fn add_human(a: &[u8], b: &[u8]) -> Result<String, &'static str> {
    let (Ok(x), Ok(y)) = (longdouble::parse(a), longdouble::parse(b)) else {
        return Err("ERR value is not a valid float");
    };
    let sum = x.plus(y);
    if !sum.is_finite() {
        return Err("ERR increment would produce NaN or Infinity");
    }
    Ok(sum.to_human())
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
    fn human_addition() {
        let add = |a: &str, b: &str| add_human(a.as_bytes(), b.as_bytes()).unwrap();
        assert_eq!(add("10.50", "0.1"), "10.6");
        assert_eq!(add("0.1", "0.2"), "0.3");
        assert_eq!(add("5.0e3", "2.0e2"), "5200");
        assert_eq!(add("1", "-1"), "0");
        assert_eq!(add("-1.5", "0.25"), "-1.25");
        assert_eq!(add(".5", "1"), "1.5");
        // x87 long double: exactly what Redis prints on x86-64.
        assert_eq!(add("1000", "0.1"), "1000.09999999999999998");
        assert!(add_human(b"1e308", b"1e308").is_ok());
        assert_eq!(add_human(b"x", b"1"), Err("ERR value is not a valid float"));
        assert_eq!(add_human(b"1", b"inf"), Err("ERR increment would produce NaN or Infinity"));
    }
}
