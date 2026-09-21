//! Software x87 80-bit `long double`, just enough for INCRBYFLOAT and
//! HINCRBYFLOAT: parsing (`strtold`), addition, and `%.17Lf` formatting,
//! all correctly rounded (round half to even) like the hardware and glibc.
//!
//! Redis does these commands in `long double`, so its output depends on
//! 64-bit-mantissa arithmetic: `INCRBYFLOAT k 0.1` on 1000 gives
//! "1000.09999999999999998", and 1e308 + 1e308 does not overflow. We match
//! Redis on x86-64, the common server platform.

use std::cmp::Ordering;

/// Mantissa bits of the x87 extended format.
const MANT_BITS: i64 = 64;
/// Smallest exponent for `mant * 2^exp` (subnormals bottom out here).
const MIN_EXP: i64 = -16445;
/// Largest unbiased exponent of the leading bit.
const MAX_LEAD_EXP: i64 = 16383;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Ld {
    Zero {
        neg: bool,
    },
    Inf {
        neg: bool,
    },
    Nan,
    /// `(-1)^neg * mant * 2^exp`, with `mant` normalized (top bit set)
    /// unless subnormal.
    Finite {
        neg: bool,
        mant: u64,
        exp: i64,
    },
}

// ---- a minimal unsigned big integer (little-endian u64 limbs) ----

#[derive(Clone, Debug, PartialEq, Eq)]
struct Big(Vec<u64>);

impl Big {
    fn from_u128(v: u128) -> Big {
        let mut b = Big(vec![v as u64, (v >> 64) as u64]);
        b.trim();
        b
    }

    fn trim(&mut self) {
        while self.0.last() == Some(&0) {
            self.0.pop();
        }
    }

    fn is_zero(&self) -> bool {
        self.0.is_empty()
    }

    fn bits(&self) -> i64 {
        match self.0.last() {
            None => 0,
            Some(top) => (self.0.len() as i64 - 1) * 64 + (64 - top.leading_zeros() as i64),
        }
    }

    fn bit(&self, i: i64) -> bool {
        if i < 0 {
            return false;
        }
        let (w, b) = ((i / 64) as usize, i % 64);
        self.0.get(w).is_some_and(|x| x >> b & 1 == 1)
    }

    /// Whether any bit below position `i` is set.
    fn any_below(&self, i: i64) -> bool {
        if i <= 0 {
            return false;
        }
        let (w, b) = ((i / 64) as usize, i % 64);
        self.0.iter().take(w.min(self.0.len())).any(|&x| x != 0)
            || (b > 0 && self.0.get(w).is_some_and(|x| x & ((1u64 << b) - 1) != 0))
    }

    fn shl(&self, n: i64) -> Big {
        if self.is_zero() || n == 0 {
            return self.clone();
        }
        let (w, b) = ((n / 64) as usize, (n % 64) as u32);
        let mut out = vec![0u64; w];
        let mut carry = 0u64;
        for &x in &self.0 {
            out.push(if b == 0 { x } else { (x << b) | carry });
            carry = if b == 0 { 0 } else { x >> (64 - b) };
        }
        out.push(carry);
        let mut r = Big(out);
        r.trim();
        r
    }

    fn shr(&self, n: i64) -> Big {
        let (w, b) = ((n / 64) as usize, (n % 64) as u32);
        if w >= self.0.len() {
            return Big(vec![]);
        }
        let src = &self.0[w..];
        let mut out = Vec::with_capacity(src.len());
        for i in 0..src.len() {
            let lo = src[i] >> b;
            let hi = if b == 0 { 0 } else { src.get(i + 1).map_or(0, |x| x << (64 - b)) };
            out.push(lo | hi);
        }
        let mut r = Big(out);
        r.trim();
        r
    }

    fn cmp(&self, o: &Big) -> Ordering {
        self.0.len().cmp(&o.0.len()).then_with(|| self.0.iter().rev().cmp(o.0.iter().rev()))
    }

    fn add(&self, o: &Big) -> Big {
        let mut out = Vec::with_capacity(self.0.len().max(o.0.len()) + 1);
        let mut carry = 0u128;
        for i in 0..self.0.len().max(o.0.len()) {
            let s =
                *self.0.get(i).unwrap_or(&0) as u128 + *o.0.get(i).unwrap_or(&0) as u128 + carry;
            out.push(s as u64);
            carry = s >> 64;
        }
        out.push(carry as u64);
        let mut r = Big(out);
        r.trim();
        r
    }

    /// `self - o`, requires `self >= o`.
    fn sub(&self, o: &Big) -> Big {
        let mut out = Vec::with_capacity(self.0.len());
        let mut borrow = 0i128;
        for i in 0..self.0.len() {
            let mut d = self.0[i] as i128 - *o.0.get(i).unwrap_or(&0) as i128 - borrow;
            borrow = if d < 0 {
                d += 1 << 64;
                1
            } else {
                0
            };
            out.push(d as u64);
        }
        let mut r = Big(out);
        r.trim();
        r
    }

    fn mul_small(&self, m: u64) -> Big {
        let mut out = Vec::with_capacity(self.0.len() + 1);
        let mut carry = 0u128;
        for &x in &self.0 {
            let p = x as u128 * m as u128 + carry;
            out.push(p as u64);
            carry = p >> 64;
        }
        out.push(carry as u64);
        let mut r = Big(out);
        r.trim();
        r
    }

    /// (self / d, self % d) for a small divisor.
    fn divmod_small(&self, d: u64) -> (Big, u64) {
        let mut out = vec![0u64; self.0.len()];
        let mut rem = 0u128;
        for i in (0..self.0.len()).rev() {
            let cur = (rem << 64) | self.0[i] as u128;
            out[i] = (cur / d as u128) as u64;
            rem = cur % d as u128;
        }
        let mut r = Big(out);
        r.trim();
        (r, rem as u64)
    }

    /// Floor division by a big divisor (bitwise long division); returns
    /// (quotient, remainder is nonzero).
    fn div(&self, d: &Big) -> (Big, bool) {
        let mut q = Big(vec![0; self.0.len()]);
        let mut r = Big(vec![]);
        for i in (0..self.bits()).rev() {
            r = r.shl(1);
            if self.bit(i) {
                r = r.add(&Big::from_u128(1));
            }
            if r.cmp(d) != Ordering::Less {
                r = r.sub(d);
                q.0[(i / 64) as usize] |= 1 << (i % 64);
            }
        }
        q.trim();
        (q, !r.is_zero())
    }

    fn to_decimal(&self) -> String {
        if self.is_zero() {
            return "0".into();
        }
        let mut chunks = Vec::new();
        let mut n = self.clone();
        while !n.is_zero() {
            let (q, r) = n.divmod_small(10u64.pow(19));
            chunks.push(r);
            n = q;
        }
        let mut s = chunks.pop().unwrap().to_string();
        for c in chunks.iter().rev() {
            s += &format!("{c:019}");
        }
        s
    }
}

/// Rounds the exact value `x * 2^s` (plus a tiny positive amount if
/// `sticky`) to extended precision, half to even.
fn round(neg: bool, x: &Big, s: i64, sticky: bool) -> Ld {
    if x.is_zero() {
        return Ld::Zero { neg };
    }
    let n = x.bits();
    let mut exp = s + n - MANT_BITS;
    let mut shift = n - MANT_BITS;
    if exp < MIN_EXP {
        shift += MIN_EXP - exp;
        exp = MIN_EXP;
    }
    let mut mant: u128 = if shift <= 0 {
        let v = x.shl(-shift);
        v.0.first().copied().unwrap_or(0) as u128 | (v.0.get(1).copied().unwrap_or(0) as u128) << 64
    } else {
        let v = x.shr(shift);
        let mut m = v.0.first().copied().unwrap_or(0) as u128
            | (v.0.get(1).copied().unwrap_or(0) as u128) << 64;
        let half = x.bit(shift - 1);
        let rest = x.any_below(shift - 1) || sticky;
        if half && (rest || m & 1 == 1) {
            m += 1;
        }
        m
    };
    if mant >> 64 != 0 {
        mant >>= 1;
        exp += 1;
    }
    if mant == 0 {
        return Ld::Zero { neg };
    }
    if exp + MANT_BITS - 1 > MAX_LEAD_EXP {
        return Ld::Inf { neg };
    }
    Ld::Finite { neg, mant: mant as u64, exp }
}

/// Why a string isn't a valid `long double` for Redis's `string2ld`.
#[derive(Debug, PartialEq)]
pub enum ParseError {
    Invalid,
    /// Overflowed to infinity or underflowed to zero (strtold's ERANGE).
    Range,
}

/// `strtold` on the whole string, with Redis's extra rules: no leading
/// space, no trailing junk, NaN rejected, ERANGE overflow/underflow
/// rejected.
pub fn parse(s: &[u8]) -> Result<Ld, ParseError> {
    let s = std::str::from_utf8(s).map_err(|_| ParseError::Invalid)?;
    if s.is_empty() || s.starts_with(|c: char| c.is_ascii_whitespace()) || s.len() >= 5 * 1024 {
        return Err(ParseError::Invalid);
    }
    let (neg, body) = match s.as_bytes()[0] {
        b'-' => (true, &s[1..]),
        b'+' => (false, &s[1..]),
        _ => (false, s),
    };
    let lower = body.to_ascii_lowercase();
    if lower == "inf" || lower == "infinity" {
        return Ok(Ld::Inf { neg });
    }
    if lower.starts_with("nan") {
        return Err(ParseError::Invalid);
    }
    if let Some(hex) = lower.strip_prefix("0x") {
        return parse_hex(neg, hex);
    }
    let (mantissa, exp10) = match lower.find('e') {
        Some(i) => {
            let e = &lower[i + 1..];
            let digits = e.trim_start_matches(['+', '-']);
            if digits.is_empty() || !digits.bytes().all(|c| c.is_ascii_digit()) {
                return Err(ParseError::Invalid);
            }
            // Huge exponents saturate; the value is out of range anyway.
            let v: i64 = digits.parse().unwrap_or(i64::MAX / 4).min(1_000_000);
            (&lower[..i], if e.starts_with('-') { -v } else { v })
        }
        None => (&lower[..], 0),
    };
    let (int_part, frac_part) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if int_part.is_empty() && frac_part.is_empty() {
        return Err(ParseError::Invalid);
    }
    if !int_part.bytes().chain(frac_part.bytes()).all(|c| c.is_ascii_digit()) {
        return Err(ParseError::Invalid);
    }
    let digits = format!("{int_part}{frac_part}");
    let digits = digits.trim_start_matches('0');
    if digits.is_empty() {
        return Ok(Ld::Zero { neg });
    }
    let mut n = Big(vec![]);
    for chunk in digits.as_bytes().chunks(19) {
        let v: u64 = std::str::from_utf8(chunk).unwrap().parse().unwrap();
        n = n.mul_small(10u64.pow(chunk.len() as u32)).add(&Big::from_u128(v as u128));
    }
    let e = exp10 - frac_part.len() as i64;
    // Far outside the range: over- or underflow without the bignum work.
    let magnitude = digits.len() as i64 + e;
    if magnitude > 4933 {
        return Err(ParseError::Range);
    }
    if magnitude < -4952 {
        return Err(ParseError::Range);
    }
    let v = if e >= 0 {
        round(neg, &n.mul_pow10(e as u32), 0, false)
    } else {
        // n / 10^-e: scale up so the quotient has plenty of bits.
        let den = Big::from_u128(1).mul_pow10((-e) as u32);
        let k = (den.bits() - n.bits() + MANT_BITS + 2).max(0);
        let (q, inexact) = n.shl(k).div(&den);
        round(neg, &q, -k, inexact)
    };
    match v {
        Ld::Inf { .. } | Ld::Zero { .. } => Err(ParseError::Range),
        v => Ok(v),
    }
}

impl Big {
    fn mul_pow10(&self, e: u32) -> Big {
        let mut r = self.clone();
        let mut left = e;
        while left >= 19 {
            r = r.mul_small(10u64.pow(19));
            left -= 19;
        }
        r.mul_small(10u64.pow(left))
    }
}

fn parse_hex(neg: bool, s: &str) -> Result<Ld, ParseError> {
    let (mantissa, exp2) = match s.find('p') {
        Some(i) => (&s[..i], s[i + 1..].parse::<i64>().map_err(|_| ParseError::Invalid)?),
        None => (s, 0),
    };
    let (int_part, frac_part) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if int_part.is_empty() && frac_part.is_empty() {
        return Err(ParseError::Invalid);
    }
    let mut n = Big(vec![]);
    for c in int_part.chars().chain(frac_part.chars()) {
        let d = c.to_digit(16).ok_or(ParseError::Invalid)?;
        n = n.shl(4).add(&Big::from_u128(d as u128));
    }
    match round(neg, &n, exp2 - 4 * frac_part.len() as i64, false) {
        Ld::Zero { .. } if !n.is_zero() => Err(ParseError::Range),
        Ld::Inf { .. } => Err(ParseError::Range),
        v => Ok(v),
    }
}

impl Ld {
    pub fn is_finite(&self) -> bool {
        matches!(self, Ld::Zero { .. } | Ld::Finite { .. })
    }

    fn neg(&self) -> bool {
        match *self {
            Ld::Zero { neg } | Ld::Inf { neg } | Ld::Finite { neg, .. } => neg,
            Ld::Nan => false,
        }
    }

    /// Correctly rounded addition (round to nearest, ties to even).
    pub fn plus(self, o: Ld) -> Ld {
        match (self, o) {
            (Ld::Nan, _) | (_, Ld::Nan) => Ld::Nan,
            (Ld::Inf { neg: a }, Ld::Inf { neg: b }) => {
                if a == b {
                    self
                } else {
                    Ld::Nan
                }
            }
            (Ld::Inf { .. }, _) => self,
            (_, Ld::Inf { .. }) => o,
            (Ld::Zero { neg: a }, Ld::Zero { neg: b }) => Ld::Zero { neg: a && b },
            (Ld::Zero { .. }, _) => o,
            (_, Ld::Zero { .. }) => self,
            (
                Ld::Finite { neg: na, mant: ma, exp: ea },
                Ld::Finite { neg: nb, mant: mb, exp: eb },
            ) => {
                // Far apart: the smaller can't change the rounded result.
                if (ea - eb).abs() > 2 * MANT_BITS + 4 {
                    return if ea > eb { self } else { o };
                }
                let e = ea.min(eb);
                let a = Big::from_u128(ma as u128).shl(ea - e);
                let b = Big::from_u128(mb as u128).shl(eb - e);
                if na == nb {
                    return round(na, &a.add(&b), e, false);
                }
                match a.cmp(&b) {
                    Ordering::Equal => Ld::Zero { neg: false },
                    Ordering::Greater => round(na, &a.sub(&b), e, false),
                    Ordering::Less => round(nb, &b.sub(&a), e, false),
                }
            }
        }
    }

    /// `printf("%.17Lf")`: exact decimal expansion rounded to 17 places.
    fn format_17f(&self) -> String {
        let sign = if self.neg() { "-" } else { "" };
        let (mant, exp) = match *self {
            Ld::Finite { mant, exp, .. } => (mant, exp),
            Ld::Zero { .. } => return format!("{sign}0.00000000000000000"),
            Ld::Inf { .. } => return format!("{sign}inf"),
            Ld::Nan => return "nan".into(),
        };
        let scaled = Big::from_u128(mant as u128).mul_pow10(17);
        let q = if exp >= 0 {
            scaled.shl(exp)
        } else {
            let sh = -exp;
            let q = scaled.shr(sh);
            let half = scaled.bit(sh - 1);
            let rest = scaled.any_below(sh - 1);
            let odd = q.bit(0);
            if half && (rest || odd) { q.add(&Big::from_u128(1)) } else { q }
        };
        let (int_part, frac) = q.divmod_small(10u64.pow(17));
        format!("{sign}{}.{frac:017}", int_part.to_decimal())
    }

    /// Redis's `ld2string(.., LD_STR_HUMAN)`: `%.17Lf` with trailing zeros
    /// (and a bare trailing dot) removed, "-0" shown as "0". Output that
    /// wouldn't fit Redis's 5KiB buffer comes back empty, as in Redis.
    pub fn to_human(&self) -> String {
        let s = self.format_17f();
        if s.len() + 1 > 5 * 1024 {
            return String::new();
        }
        let s = if s.contains('.') { s.trim_end_matches('0').trim_end_matches('.') } else { &s };
        if s == "-0" { "0".into() } else { s.into() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn add(a: &str, b: &str) -> String {
        parse(a.as_bytes()).unwrap().plus(parse(b.as_bytes()).unwrap()).to_human()
    }

    /// Expected values come from gcc on x86-64: strtold, +, %.17Lf.
    #[test]
    fn matches_x87_results() {
        assert_eq!(add("1000", "0.1"), "1000.09999999999999998");
        assert_eq!(add("0.1", "0.2"), "0.3");
        assert_eq!(add("10.50", "0.1"), "10.6");
        assert_eq!(add("5.0e3", "2.0e2"), "5200");
        assert_eq!(add("1", "-1"), "0");
        assert_eq!(add("-1.5", "0.25"), "-1.25");
        assert_eq!(
            add("1e308", "1e308"),
            "199999999999999999993371759311691291321120199694831134415594095989843469737676123744200253\
             843777078640893494450108026446304269499187921167194841628860392837535918200039206381557326\
             219209014213335878306791577877829121087126122536729803237260434173178506889763247582601711\
             514636284849020905456510092687857156096"
        );
    }

    #[test]
    fn parsing_rules() {
        assert!(parse(b"inf").unwrap() == Ld::Inf { neg: false });
        assert!(parse(b"-Infinity").unwrap() == Ld::Inf { neg: true });
        assert_eq!(parse(b"nan"), Err(ParseError::Invalid));
        assert_eq!(parse(b" 1"), Err(ParseError::Invalid));
        assert_eq!(parse(b"1 "), Err(ParseError::Invalid));
        assert_eq!(parse(b"abc"), Err(ParseError::Invalid));
        assert_eq!(parse(b""), Err(ParseError::Invalid));
        assert_eq!(parse(b"1e5000"), Err(ParseError::Range));
        assert_eq!(parse(b"1e-5000"), Err(ParseError::Range));
        assert_eq!(parse(b"0x1.8p1").unwrap().to_human(), "3");
        assert_eq!(parse(b"+.5").unwrap().to_human(), "0.5");
        assert_eq!(parse(b"0").unwrap(), Ld::Zero { neg: false });
    }

    #[test]
    fn overflow_to_infinity() {
        let max = parse(b"1e4932").unwrap();
        assert!(!max.plus(max).is_finite());
    }
}
