//! Arbitrary-precision `numeric`, following Postgres's scale rules.
//!
//! Stored as a decimal digit string (one digit per byte) plus a scale.
//! Slow but simple: performance is not a goal.

use std::cmp::Ordering;

/// Postgres's NUMERIC_MIN_SIG_DIGITS and display-scale bounds.
const MIN_SIG_DIGITS: i64 = 16;
const MAX_DISPLAY_SCALE: i64 = 1000;

#[derive(Clone, Debug)]
pub enum Numeric {
    NaN,
    /// `true` for -Infinity.
    Inf(bool),
    Fin(Dec),
}

/// A finite decimal: `(-1)^neg * digits * 10^-scale`.
#[derive(Clone, Debug, Default)]
pub struct Dec {
    pub neg: bool,
    /// Most significant first, no leading zeros; empty means zero.
    pub digits: Vec<u8>,
    pub scale: u32,
}

impl Dec {
    fn zero(scale: u32) -> Dec {
        Dec { neg: false, digits: vec![], scale }
    }

    fn normalize(mut self) -> Dec {
        let lead = self.digits.iter().take_while(|&&d| d == 0).count();
        self.digits.drain(..lead);
        if self.digits.is_empty() {
            self.neg = false;
        }
        self
    }

    pub fn is_zero(&self) -> bool {
        self.digits.is_empty()
    }

    /// Rescales up (appending zeros); never loses digits.
    fn with_scale(&self, scale: u32) -> Vec<u8> {
        let mut d = self.digits.clone();
        if !d.is_empty() {
            d.extend(std::iter::repeat_n(0, (scale - self.scale) as usize));
        }
        d
    }

    /// Rounds half away from zero (or truncates) to `scale` digits after the
    /// point. A negative scale rounds to the left of the point.
    pub fn round_to(&self, scale: i64, truncate: bool) -> Dec {
        let cur = self.scale as i64;
        if scale >= cur {
            let s = scale.min(MAX_DISPLAY_SCALE) as u32;
            return Dec { neg: self.neg, digits: self.with_scale(s), scale: s };
        }
        let drop = (cur - scale) as usize;
        let keep_scale = scale.max(0) as u32;
        let mut digits = self.digits.clone();
        let (mut kept, removed) = if drop >= digits.len() {
            (vec![], std::mem::take(&mut digits))
        } else {
            let removed = digits.split_off(digits.len() - drop);
            (digits, removed)
        };
        let round_up =
            !truncate && removed.len() == drop && removed.first().is_some_and(|&d| d >= 5);
        if round_up {
            kept = add_mag(&kept, &[1]);
        }
        // A negative target scale: pad back zeros for the dropped integer places.
        if scale < 0 {
            let pad = (-scale) as usize;
            if !kept.is_empty() {
                kept.extend(std::iter::repeat_n(0, pad));
            }
        }
        Dec { neg: self.neg, digits: kept, scale: keep_scale }.normalize()
    }

    /// Base-10000 weight and first digit, as Postgres's `NumericVar` has them.
    fn nbase_weight(&self) -> (i64, u32) {
        if self.is_zero() {
            return (0, 0);
        }
        let n = self.digits.len() as i64;
        let int_digits = n - self.scale as i64;
        if int_digits > 0 {
            let weight = (int_digits - 1) / 4;
            let lead = ((int_digits - 1) % 4 + 1) as usize;
            let first = self.digits[..lead].iter().fold(0u32, |a, &d| a * 10 + d as u32);
            (weight, first)
        } else {
            // All digits are fractional; `zeros` leading zeros after the point.
            let zeros = (-int_digits) as usize;
            let group = zeros / 4;
            let weight = -(group as i64) - 1;
            // Digits of the fraction from position group*4 for 4 places.
            let frac: Vec<u8> =
                std::iter::repeat_n(0u8, zeros).chain(self.digits.iter().copied()).collect();
            let mut v = 0u32;
            for i in 0..4 {
                v = v * 10 + *frac.get(group * 4 + i).unwrap_or(&0) as u32;
            }
            (weight, v)
        }
    }
}

fn cmp_mag(a: &[u8], b: &[u8]) -> Ordering {
    a.len().cmp(&b.len()).then_with(|| a.cmp(b))
}

fn add_mag(a: &[u8], b: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(a.len().max(b.len()) + 1);
    let (mut i, mut j, mut carry) = (a.len(), b.len(), 0u8);
    while i > 0 || j > 0 || carry > 0 {
        let mut s = carry;
        if i > 0 {
            i -= 1;
            s += a[i];
        }
        if j > 0 {
            j -= 1;
            s += b[j];
        }
        out.push(s % 10);
        carry = s / 10;
    }
    out.reverse();
    strip(out)
}

/// `a - b` where `a >= b`.
fn sub_mag(a: &[u8], b: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(a.len());
    let (mut i, mut j, mut borrow) = (a.len(), b.len(), 0i8);
    while i > 0 {
        i -= 1;
        let mut s = a[i] as i8 - borrow;
        if j > 0 {
            j -= 1;
            s -= b[j] as i8;
        }
        if s < 0 {
            s += 10;
            borrow = 1;
        } else {
            borrow = 0;
        }
        out.push(s as u8);
    }
    out.reverse();
    strip(out)
}

fn mul_mag(a: &[u8], b: &[u8]) -> Vec<u8> {
    if a.is_empty() || b.is_empty() {
        return vec![];
    }
    let mut acc = vec![0u32; a.len() + b.len()];
    for (i, &x) in a.iter().enumerate().rev() {
        for (j, &y) in b.iter().enumerate().rev() {
            acc[i + j + 1] += x as u32 * y as u32;
        }
    }
    for k in (1..acc.len()).rev() {
        let carry = acc[k] / 10;
        acc[k] %= 10;
        acc[k - 1] += carry;
    }
    strip(acc.into_iter().map(|d| d as u8).collect())
}

/// Long division of magnitudes: (quotient, remainder).
fn divmod_mag(a: &[u8], b: &[u8]) -> (Vec<u8>, Vec<u8>) {
    let mut q = Vec::with_capacity(a.len());
    let mut r: Vec<u8> = vec![];
    for &d in a {
        r.push(d);
        r = strip(r);
        let mut n = 0;
        while cmp_mag(&r, b) != Ordering::Less {
            r = sub_mag(&r, b);
            n += 1;
        }
        q.push(n);
    }
    (strip(q), r)
}

fn strip(mut v: Vec<u8>) -> Vec<u8> {
    let lead = v.iter().take_while(|&&d| d == 0).count();
    v.drain(..lead);
    v
}

#[derive(Debug, PartialEq)]
pub enum NumError {
    Syntax,
    DivByZero,
    Overflow,
}

impl Numeric {
    pub fn zero() -> Numeric {
        Numeric::Fin(Dec::zero(0))
    }

    pub fn from_i64(v: i64) -> Numeric {
        let neg = v < 0;
        let digits = strip(v.unsigned_abs().to_string().bytes().map(|b| b - b'0').collect());
        Numeric::Fin(Dec { neg, digits, scale: 0 }.normalize())
    }

    pub fn from_i128(v: i128) -> Numeric {
        let neg = v < 0;
        let digits = strip(v.unsigned_abs().to_string().bytes().map(|b| b - b'0').collect());
        Numeric::Fin(Dec { neg, digits, scale: 0 }.normalize())
    }

    /// Postgres converts float8 to numeric through `%.15g`.
    pub fn from_f64(v: f64) -> Numeric {
        if v.is_nan() {
            return Numeric::NaN;
        }
        if v.is_infinite() {
            return Numeric::Inf(v < 0.0);
        }
        Numeric::parse(&format_g(v, 15)).unwrap_or(Numeric::NaN)
    }

    /// float4 goes through `%.6g`.
    pub fn from_f32(v: f32) -> Numeric {
        if v.is_nan() {
            return Numeric::NaN;
        }
        if v.is_infinite() {
            return Numeric::Inf(v < 0.0);
        }
        Numeric::parse(&format_g(v as f64, 6)).unwrap_or(Numeric::NaN)
    }

    pub fn parse(s: &str) -> Result<Numeric, NumError> {
        let t = s.trim();
        let lower = t.to_ascii_lowercase();
        match lower.as_str() {
            "nan" => return Ok(Numeric::NaN),
            "infinity" | "+infinity" | "inf" | "+inf" => return Ok(Numeric::Inf(false)),
            "-infinity" | "-inf" => return Ok(Numeric::Inf(true)),
            _ => {}
        }
        let b = t.as_bytes();
        let mut i = 0;
        let mut neg = false;
        if i < b.len() && (b[i] == b'+' || b[i] == b'-') {
            neg = b[i] == b'-';
            i += 1;
        }
        let mut digits = vec![];
        let mut scale: i64 = 0;
        let mut seen_digit = false;
        let mut seen_point = false;
        while i < b.len() {
            let c = b[i];
            if c.is_ascii_digit() {
                digits.push(c - b'0');
                seen_digit = true;
                if seen_point {
                    scale += 1;
                }
            } else if c == b'.' && !seen_point {
                seen_point = true;
            } else if c == b'_' && seen_digit && i + 1 < b.len() && b[i + 1].is_ascii_digit() {
                // PG16 allows underscores between digits; harmless to accept.
            } else {
                break;
            }
            i += 1;
        }
        if !seen_digit {
            return Err(NumError::Syntax);
        }
        if i < b.len() && (b[i] == b'e' || b[i] == b'E') {
            i += 1;
            let start = i;
            if i < b.len() && (b[i] == b'+' || b[i] == b'-') {
                i += 1;
            }
            let ds = i;
            while i < b.len() && b[i].is_ascii_digit() {
                i += 1;
            }
            if ds == i {
                return Err(NumError::Syntax);
            }
            let exp: i64 = t[start..i].parse().map_err(|_| NumError::Overflow)?;
            if exp.abs() > 100_000 {
                return Err(NumError::Overflow);
            }
            scale -= exp;
        }
        if i != b.len() {
            return Err(NumError::Syntax);
        }
        if scale < 0 {
            digits.extend(std::iter::repeat_n(0, (-scale) as usize));
            scale = 0;
        }
        Ok(Numeric::Fin(Dec { neg, digits: strip(digits), scale: scale as u32 }.normalize()))
    }

    pub fn is_nan(&self) -> bool {
        matches!(self, Numeric::NaN)
    }

    pub fn scale(&self) -> u32 {
        match self {
            Numeric::Fin(d) => d.scale,
            _ => 0,
        }
    }

    pub fn is_zero(&self) -> bool {
        matches!(self, Numeric::Fin(d) if d.is_zero())
    }

    pub fn is_negative(&self) -> bool {
        match self {
            Numeric::Fin(d) => d.neg,
            Numeric::Inf(n) => *n,
            Numeric::NaN => false,
        }
    }

    pub fn neg(&self) -> Numeric {
        match self {
            Numeric::NaN => Numeric::NaN,
            Numeric::Inf(n) => Numeric::Inf(!n),
            Numeric::Fin(d) => Numeric::Fin(Dec { neg: !d.neg, ..d.clone() }.normalize()),
        }
    }

    pub fn abs(&self) -> Numeric {
        match self {
            Numeric::NaN => Numeric::NaN,
            Numeric::Inf(_) => Numeric::Inf(false),
            Numeric::Fin(d) => Numeric::Fin(Dec { neg: false, ..d.clone() }),
        }
    }

    pub fn add(&self, o: &Numeric) -> Numeric {
        match (self, o) {
            (Numeric::NaN, _) | (_, Numeric::NaN) => Numeric::NaN,
            (Numeric::Inf(a), Numeric::Inf(b)) => {
                if a == b {
                    Numeric::Inf(*a)
                } else {
                    Numeric::NaN
                }
            }
            (Numeric::Inf(a), _) | (_, Numeric::Inf(a)) => Numeric::Inf(*a),
            (Numeric::Fin(a), Numeric::Fin(b)) => Numeric::Fin(add_dec(a, b)),
        }
    }

    pub fn sub(&self, o: &Numeric) -> Numeric {
        self.add(&o.neg())
    }

    pub fn mul(&self, o: &Numeric) -> Numeric {
        match (self, o) {
            (Numeric::NaN, _) | (_, Numeric::NaN) => Numeric::NaN,
            (Numeric::Inf(_), x) | (x, Numeric::Inf(_)) if x.is_zero() => Numeric::NaN,
            (Numeric::Inf(_), _) | (_, Numeric::Inf(_)) => {
                Numeric::Inf(self.is_negative() != o.is_negative())
            }
            (Numeric::Fin(a), Numeric::Fin(b)) => {
                let d = Dec {
                    neg: a.neg != b.neg,
                    digits: mul_mag(&a.digits, &b.digits),
                    scale: a.scale + b.scale,
                }
                .normalize();
                let d = if d.scale as i64 > MAX_DISPLAY_SCALE {
                    d.round_to(MAX_DISPLAY_SCALE, false)
                } else {
                    d
                };
                Numeric::Fin(d)
            }
        }
    }

    /// Division with Postgres's result-scale selection.
    pub fn div(&self, o: &Numeric) -> Result<Numeric, NumError> {
        match (self, o) {
            (Numeric::NaN, _) | (_, Numeric::NaN) => Ok(Numeric::NaN),
            (_, x) if x.is_zero() => Err(NumError::DivByZero),
            (Numeric::Inf(_), Numeric::Inf(_)) => Ok(Numeric::NaN),
            (Numeric::Inf(a), _) => Ok(Numeric::Inf(*a != o.is_negative())),
            (_, Numeric::Inf(_)) => Ok(Numeric::zero()),
            (Numeric::Fin(a), Numeric::Fin(b)) => {
                let rscale = select_div_scale(a, b);
                Ok(Numeric::Fin(div_dec(a, b, rscale, false)))
            }
        }
    }

    /// Division to an explicit scale.
    pub fn div_scale(&self, o: &Numeric, rscale: i64, truncate: bool) -> Result<Numeric, NumError> {
        match (self, o) {
            (Numeric::Fin(a), Numeric::Fin(b)) => {
                if b.is_zero() {
                    return Err(NumError::DivByZero);
                }
                Ok(Numeric::Fin(div_dec(a, b, rscale, truncate)))
            }
            _ => self.div(o),
        }
    }

    /// `div(a, b)`: truncated integer quotient.
    pub fn div_trunc(&self, o: &Numeric) -> Result<Numeric, NumError> {
        self.div_scale(o, 0, true)
    }

    /// `a % b`: remainder with the sign of `a`.
    pub fn rem(&self, o: &Numeric) -> Result<Numeric, NumError> {
        match (self, o) {
            (Numeric::Fin(a), Numeric::Fin(b)) => {
                if b.is_zero() {
                    return Err(NumError::DivByZero);
                }
                let q = div_dec(a, b, 0, true);
                let prod = Numeric::Fin(q).mul(o);
                Ok(self.sub(&prod))
            }
            (Numeric::NaN, _) | (_, Numeric::NaN) | (Numeric::Inf(_), _) => Ok(Numeric::NaN),
            (_, _) => Ok(self.clone()),
        }
    }

    pub fn round(&self, scale: i64) -> Numeric {
        match self {
            Numeric::Fin(d) => Numeric::Fin(d.round_to(scale, false)),
            x => x.clone(),
        }
    }

    pub fn trunc(&self, scale: i64) -> Numeric {
        match self {
            Numeric::Fin(d) => Numeric::Fin(d.round_to(scale, true)),
            x => x.clone(),
        }
    }

    pub fn ceil(&self) -> Numeric {
        match self {
            Numeric::Fin(d) => {
                let t = d.round_to(0, true);
                let frac = cmp_num(&Numeric::Fin(t.clone()), self) != Ordering::Equal;
                let t = Numeric::Fin(t);
                if frac && !d.neg { t.add(&Numeric::from_i64(1)) } else { t }
            }
            x => x.clone(),
        }
    }

    pub fn floor(&self) -> Numeric {
        match self {
            Numeric::Fin(d) => {
                let t = d.round_to(0, true);
                let frac = cmp_num(&Numeric::Fin(t.clone()), self) != Ordering::Equal;
                let t = Numeric::Fin(t);
                if frac && d.neg { t.sub(&Numeric::from_i64(1)) } else { t }
            }
            x => x.clone(),
        }
    }

    pub fn sign(&self) -> Numeric {
        match self {
            Numeric::NaN => Numeric::NaN,
            x if x.is_zero() => Numeric::zero(),
            x if x.is_negative() => Numeric::from_i64(-1),
            _ => Numeric::from_i64(1),
        }
    }

    /// Rounds half away from zero to an integer.
    pub fn to_i128(&self) -> Option<i128> {
        match self {
            Numeric::Fin(d) => {
                let r = d.round_to(0, false);
                if r.digits.len() > 38 {
                    return None;
                }
                let mut v: i128 = 0;
                for &x in &r.digits {
                    v = v * 10 + x as i128;
                }
                Some(if r.neg { -v } else { v })
            }
            _ => None,
        }
    }

    pub fn to_i64(&self) -> Option<i64> {
        self.to_i128().and_then(|v| i64::try_from(v).ok())
    }

    pub fn to_f64(&self) -> f64 {
        match self {
            Numeric::NaN => f64::NAN,
            Numeric::Inf(n) => {
                if *n {
                    f64::NEG_INFINITY
                } else {
                    f64::INFINITY
                }
            }
            Numeric::Fin(_) => self.to_string().parse().unwrap_or(f64::NAN),
        }
    }

    /// Applies a `numeric(precision, scale)` typmod.
    pub fn apply_typmod(&self, precision: i64, scale: i64) -> Result<Numeric, NumError> {
        match self {
            Numeric::Fin(d) => {
                let r = d.round_to(scale, false);
                let int_digits = r.digits.len() as i64 - r.scale as i64;
                if !r.is_zero() && int_digits > precision - scale {
                    return Err(NumError::Overflow);
                }
                Ok(Numeric::Fin(r))
            }
            // PG14 accepts NaN; infinity doesn't fit a constrained numeric.
            Numeric::NaN => Ok(Numeric::NaN),
            Numeric::Inf(_) => Err(NumError::Overflow),
        }
    }

    /// Square root with Postgres's result-scale rule.
    pub fn sqrt(&self) -> Option<Numeric> {
        match self {
            Numeric::NaN => Some(Numeric::NaN),
            Numeric::Inf(false) => Some(Numeric::Inf(false)),
            Numeric::Inf(true) => None,
            Numeric::Fin(d) => {
                if d.neg {
                    return None;
                }
                let (weight, _) = d.nbase_weight();
                // sweight = (arg.weight + 1) * DEC_DIGITS / 2 - 1
                let sweight = (weight + 1) * 4 / 2 - 1;
                let rscale =
                    (MIN_SIG_DIGITS - sweight).max(d.scale as i64).clamp(0, MAX_DISPLAY_SCALE);
                // Integer square root of digits * 10^(2*rscale + 2 - scale), then round.
                let work = rscale + 1;
                let shift = 2 * work - d.scale as i64;
                let mut n = d.digits.clone();
                if shift >= 0 {
                    n.extend(std::iter::repeat_n(0, shift as usize));
                } else {
                    let keep = n.len().saturating_sub((-shift) as usize);
                    n.truncate(keep);
                }
                let root = isqrt(&strip(n));
                let r = Dec { neg: false, digits: root, scale: work as u32 }.normalize();
                Some(Numeric::Fin(r.round_to(rscale, false)))
            }
        }
    }
}

fn isqrt(n: &[u8]) -> Vec<u8> {
    if n.is_empty() {
        return vec![];
    }
    // Digit-by-digit square root.
    let mut digits = n.to_vec();
    if digits.len() % 2 == 1 {
        digits.insert(0, 0);
    }
    let mut rem: Vec<u8> = vec![];
    let mut root: Vec<u8> = vec![];
    for pair in digits.chunks(2) {
        rem.extend_from_slice(pair);
        rem = strip(rem);
        // Find largest x with (20*root + x) * x <= rem.
        let base = mul_mag(&root, &[2, 0]);
        let mut x = 0u8;
        for cand in (0..=9u8).rev() {
            let t = mul_mag(&add_mag(&base, &[cand]), &[cand]);
            if cmp_mag(&t, &rem) != Ordering::Greater {
                x = cand;
                rem = sub_mag(&rem, &t);
                break;
            }
        }
        root.push(x);
        root = strip(root);
    }
    root
}

fn add_dec(a: &Dec, b: &Dec) -> Dec {
    let scale = a.scale.max(b.scale);
    let da = a.with_scale(scale);
    let db = b.with_scale(scale);
    if a.neg == b.neg {
        return Dec { neg: a.neg, digits: add_mag(&da, &db), scale }.normalize();
    }
    match cmp_mag(&da, &db) {
        Ordering::Equal => Dec::zero(scale),
        Ordering::Greater => Dec { neg: a.neg, digits: sub_mag(&da, &db), scale }.normalize(),
        Ordering::Less => Dec { neg: b.neg, digits: sub_mag(&db, &da), scale }.normalize(),
    }
}

fn select_div_scale(a: &Dec, b: &Dec) -> i64 {
    let (w1, f1) = a.nbase_weight();
    let (w2, f2) = b.nbase_weight();
    let mut qweight = w1 - w2;
    if f1 <= f2 {
        qweight -= 1;
    }
    let rscale = MIN_SIG_DIGITS - qweight * 4;
    rscale.max(a.scale as i64).max(b.scale as i64).clamp(0, MAX_DISPLAY_SCALE)
}

/// `a / b` to `rscale` digits, rounded half away from zero or truncated.
fn div_dec(a: &Dec, b: &Dec, rscale: i64, truncate: bool) -> Dec {
    // quotient = a.digits * 10^(rscale + 1 + b.scale - a.scale) / b.digits
    let work = rscale + 1;
    let shift = work + b.scale as i64 - a.scale as i64;
    let mut num = a.digits.clone();
    let mut den = b.digits.clone();
    if shift >= 0 {
        if !num.is_empty() {
            num.extend(std::iter::repeat_n(0, shift as usize));
        }
    } else {
        den.extend(std::iter::repeat_n(0, (-shift) as usize));
    }
    let (q, _) = divmod_mag(&num, &den);
    let d = Dec { neg: a.neg != b.neg, digits: q, scale: work.max(0) as u32 }.normalize();
    // `work` digits then round (or truncate) to rscale.
    d.round_to(rscale, truncate)
}

pub fn cmp_num(a: &Numeric, b: &Numeric) -> Ordering {
    // Postgres orders NaN above everything, and treats NaN = NaN.
    match (a, b) {
        (Numeric::NaN, Numeric::NaN) => Ordering::Equal,
        (Numeric::NaN, _) => Ordering::Greater,
        (_, Numeric::NaN) => Ordering::Less,
        (Numeric::Inf(x), Numeric::Inf(y)) => y.cmp(x),
        (Numeric::Inf(x), _) => {
            if *x {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (_, Numeric::Inf(y)) => {
            if *y {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        (Numeric::Fin(x), Numeric::Fin(y)) => {
            if x.neg != y.neg {
                return if x.neg { Ordering::Less } else { Ordering::Greater };
            }
            let s = x.scale.max(y.scale);
            let m = cmp_mag(&x.with_scale(s), &y.with_scale(s));
            if x.neg { m.reverse() } else { m }
        }
    }
}

impl std::fmt::Display for Numeric {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Numeric::NaN => f.write_str("NaN"),
            Numeric::Inf(false) => f.write_str("Infinity"),
            Numeric::Inf(true) => f.write_str("-Infinity"),
            Numeric::Fin(d) => {
                let scale = d.scale as usize;
                let mut s = String::with_capacity(d.digits.len() + 3);
                if d.neg {
                    s.push('-');
                }
                let digits: Vec<u8> = if d.digits.len() <= scale {
                    let mut v = vec![0u8; scale + 1 - d.digits.len()];
                    v.extend_from_slice(&d.digits);
                    v
                } else {
                    d.digits.clone()
                };
                let int_len = digits.len() - scale;
                for &x in &digits[..int_len] {
                    s.push((b'0' + x) as char);
                }
                if scale > 0 {
                    s.push('.');
                    for &x in &digits[int_len..] {
                        s.push((b'0' + x) as char);
                    }
                }
                f.write_str(&s)
            }
        }
    }
}

/// C's `%.{prec}g`.
pub fn format_g(v: f64, prec: usize) -> String {
    if v == 0.0 {
        return if v.is_sign_negative() { "-0".into() } else { "0".into() };
    }
    let e = format!("{:.*e}", prec - 1, v);
    let (mant, exp) = e.split_once('e').unwrap();
    let exp: i32 = exp.parse().unwrap();
    if exp < -4 || exp >= prec as i32 {
        let mant = trim_frac(mant);
        let sign = if exp < 0 { '-' } else { '+' };
        format!("{mant}e{sign}{:02}", exp.abs())
    } else {
        let decimals = (prec as i32 - 1 - exp).max(0) as usize;
        trim_frac(&format!("{:.*}", decimals, v)).to_string()
    }
}

fn trim_frac(s: &str) -> &str {
    if s.contains('.') { s.trim_end_matches('0').trim_end_matches('.') } else { s }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn n(s: &str) -> Numeric {
        Numeric::parse(s).unwrap()
    }

    #[test]
    fn parse_and_print() {
        assert_eq!(n("1.50").to_string(), "1.50");
        assert_eq!(n("-0.001").to_string(), "-0.001");
        assert_eq!(n("1e3").to_string(), "1000");
        assert_eq!(n("1.5e-3").to_string(), "0.0015");
        assert_eq!(n("-0").to_string(), "0");
        assert_eq!(n("  007 ").to_string(), "7");
        assert_eq!(n(".5").to_string(), "0.5");
        assert!(Numeric::parse("abc").is_err());
        assert!(Numeric::parse("1.2.3").is_err());
    }

    #[test]
    fn arithmetic_scales() {
        assert_eq!(n("1.5").add(&n("2.25")).to_string(), "3.75");
        assert_eq!(n("1.5").sub(&n("2.25")).to_string(), "-0.75");
        assert_eq!(n("1.50").mul(&n("2.0")).to_string(), "3.000");
        assert_eq!(n("1").div(&n("3.0")).unwrap().to_string(), "0.33333333333333333333");
        assert_eq!(n("10").div(&n("4.0")).unwrap().to_string(), "2.5000000000000000");
        assert_eq!(n("1.0").div(&n("7")).unwrap().to_string(), "0.14285714285714285714");
        assert_eq!(n("123456.789").div(&n("0.001")).unwrap().to_string(), "123456789.00000000");
        assert_eq!(n("2").div(&n("3")).unwrap().to_string(), "0.66666666666666666667");
        assert_eq!(n("7").rem(&n("-3")).unwrap().to_string(), "1");
        assert_eq!(n("-7.5").rem(&n("2")).unwrap().to_string(), "-1.5");
        assert_eq!(n("1").div(&n("0")).unwrap_err(), NumError::DivByZero);
    }

    #[test]
    fn rounding() {
        assert_eq!(n("2.5").round(0).to_string(), "3");
        assert_eq!(n("-2.5").round(0).to_string(), "-3");
        assert_eq!(n("1234.5678").round(-2).to_string(), "1200");
        assert_eq!(n("1234.5678").round(2).to_string(), "1234.57");
        assert_eq!(n("1234.5678").trunc(2).to_string(), "1234.56");
        assert_eq!(n("1.2").round(3).to_string(), "1.200");
        assert_eq!(n("-1.2").ceil().to_string(), "-1");
        assert_eq!(n("-1.2").floor().to_string(), "-2");
        assert_eq!(n("0.4").round(0).to_string(), "0");
        assert_eq!(n("9.99").round(1).to_string(), "10.0");
    }

    #[test]
    fn typmod_and_conversions() {
        assert_eq!(n("123.456").apply_typmod(5, 2).unwrap().to_string(), "123.46");
        assert_eq!(n("1234.5").apply_typmod(5, 2).unwrap_err(), NumError::Overflow);
        assert_eq!(Numeric::from_f64(0.1).to_string(), "0.1");
        assert_eq!(Numeric::from_f64(1e20).to_string(), "100000000000000000000");
        assert_eq!(n("2.5").to_i64(), Some(3));
        assert_eq!(n("2").sqrt().unwrap().to_string(), "1.414213562373095");
        assert_eq!(format_g(1e-5, 15), "1e-05");
        assert_eq!(format_g(123.25, 15), "123.25");
    }
}
