//! Redis's double formatting (`d2string`), used for sorted set scores and
//! RESP3 doubles: integers print as integers, everything else goes through
//! fpconv's grisu2 (`fpconv_dtoa`), ported here bit for bit so the digits
//! match Redis even where grisu2 isn't the shortest representation.

/// `d2string` from Redis's util.c.
pub fn d2string(v: f64) -> String {
    if v.is_nan() {
        return "nan".into();
    }
    if v.is_infinite() {
        return if v < 0.0 { "-inf" } else { "inf" }.into();
    }
    if v == 0.0 {
        return if v.is_sign_negative() { "-0" } else { "0" }.into();
    }
    // double2ll: exact integers in the safe range print as integers.
    let half = (i64::MAX / 2) as f64;
    if v >= -half && v <= half {
        let ll = v as i64;
        if ll as f64 == v {
            return ll.to_string();
        }
    }
    fpconv_dtoa(v)
}

const FRACMASK: u64 = 0x000F_FFFF_FFFF_FFFF;
const EXPMASK: u64 = 0x7FF0_0000_0000_0000;
const HIDDENBIT: u64 = 0x0010_0000_0000_0000;
const SIGNMASK: u64 = 0x8000_0000_0000_0000;
const EXPBIAS: i32 = 1023 + 52;

const TENS: [u64; 20] = [
    10000000000000000000,
    1000000000000000000,
    100000000000000000,
    10000000000000000,
    1000000000000000,
    100000000000000,
    10000000000000,
    1000000000000,
    100000000000,
    10000000000,
    1000000000,
    100000000,
    10000000,
    1000000,
    100000,
    10000,
    1000,
    100,
    10,
    1,
];

#[derive(Clone, Copy)]
struct Fp {
    frac: u64,
    exp: i32,
}

#[rustfmt::skip]
const POWERS_TEN: [(u64, i32); 87] = [
    (18054884314459144840, -1220), (13451937075301367670, -1193),
    (10022474136428063862, -1166), (14934650266808366570, -1140),
    (11127181549972568877, -1113), (16580792590934885855, -1087),
    (12353653155963782858, -1060), (18408377700990114895, -1034),
    (13715310171984221708, -1007), (10218702384817765436, -980), (15227053142812498563, -954),
    (11345038669416679861, -927), (16905424996341287883, -901), (12595523146049147757, -874),
    (9384396036005875287, -847), (13983839803942852151, -821), (10418772551374772303, -794),
    (15525180923007089351, -768), (11567161174868858868, -741), (17236413322193710309, -715),
    (12842128665889583758, -688), (9568131466127621947, -661), (14257626930069360058, -635),
    (10622759856335341974, -608), (15829145694278690180, -582), (11793632577567316726, -555),
    (17573882009934360870, -529), (13093562431584567480, -502), (9755464219737475723, -475),
    (14536774485912137811, -449), (10830740992659433045, -422), (16139061738043178685, -396),
    (12024538023802026127, -369), (17917957937422433684, -343), (13349918974505688015, -316),
    (9946464728195732843, -289), (14821387422376473014, -263), (11042794154864902060, -236),
    (16455045573212060422, -210), (12259964326927110867, -183), (18268770466636286478, -157),
    (13611294676837538539, -130), (10141204801825835212, -103), (15111572745182864684, -77),
    (11258999068426240000, -50), (16777216000000000000, -24), (12500000000000000000, 3),
    (9313225746154785156, 30), (13877787807814456755, 56), (10339757656912845936, 83),
    (15407439555097886824, 109), (11479437019748901445, 136), (17105694144590052135, 162),
    (12744735289059618216, 189), (9495567745759798747, 216), (14149498560666738074, 242),
    (10542197943230523224, 269), (15709099088952724970, 295), (11704190886730495818, 322),
    (17440603504673385349, 348), (12994262207056124023, 375), (9681479787123295682, 402),
    (14426529090290212157, 428), (10748601772107342003, 455), (16016664761464807395, 481),
    (11933345169920330789, 508), (17782069995880619868, 534), (13248674568444952270, 561),
    (9871031767461413346, 588), (14708983551653345445, 614), (10959046745042015199, 641),
    (16330252207878254650, 667), (12166986024289022870, 694), (18130221999122236476, 720),
    (13508068024458167312, 747), (10064294952495520794, 774), (14996968138956309548, 800),
    (11173611982879273257, 827), (16649979327439178909, 853), (12405201291620119593, 880),
    (9242595204427927429, 907), (13772540099066387757, 933), (10261342003245940623, 960),
    (15290591125556738113, 986), (11392378155556871081, 1013), (16975966327722178521, 1039),
    (12648080533535911531, 1066),
];

fn find_cachedpow10(exp: i32) -> (Fp, i32) {
    const ONE_LOG_TEN: f64 = 0.30102999566398114;
    let approx = (-(exp + 87) as f64 * ONE_LOG_TEN) as i32;
    let mut idx = (approx - (-348)) / 8;
    loop {
        let current = exp + POWERS_TEN[idx as usize].1 + 64;
        if current < -60 {
            idx += 1;
            continue;
        }
        if current > -32 {
            idx -= 1;
            continue;
        }
        let (frac, e) = POWERS_TEN[idx as usize];
        return (Fp { frac, exp: e }, -348 + idx * 8);
    }
}

fn build_fp(d: f64) -> Fp {
    let bits = d.to_bits();
    let mut fp = Fp { frac: bits & FRACMASK, exp: ((bits & EXPMASK) >> 52) as i32 };
    if fp.exp != 0 {
        fp.frac += HIDDENBIT;
        fp.exp -= EXPBIAS;
    } else {
        fp.exp = -EXPBIAS + 1;
    }
    fp
}

fn normalize(fp: &mut Fp) {
    while fp.frac & HIDDENBIT == 0 {
        fp.frac <<= 1;
        fp.exp -= 1;
    }
    let shift = 64 - 52 - 1;
    fp.frac <<= shift;
    fp.exp -= shift;
}

fn get_normalized_boundaries(fp: &Fp) -> (Fp, Fp) {
    let mut upper = Fp { frac: (fp.frac << 1) + 1, exp: fp.exp - 1 };
    while upper.frac & (HIDDENBIT << 1) == 0 {
        upper.frac <<= 1;
        upper.exp -= 1;
    }
    let u_shift = 64 - 52 - 2;
    upper.frac <<= u_shift;
    upper.exp -= u_shift;
    let l_shift = if fp.frac == HIDDENBIT { 2 } else { 1 };
    let mut lower = Fp { frac: (fp.frac << l_shift) - 1, exp: fp.exp - l_shift };
    lower.frac <<= lower.exp - upper.exp;
    lower.exp = upper.exp;
    (lower, upper)
}

fn multiply(a: &Fp, b: &Fp) -> Fp {
    const LOMASK: u64 = 0x0000_0000_FFFF_FFFF;
    let ah_bl = (a.frac >> 32) * (b.frac & LOMASK);
    let al_bh = (a.frac & LOMASK) * (b.frac >> 32);
    let al_bl = (a.frac & LOMASK) * (b.frac & LOMASK);
    let ah_bh = (a.frac >> 32) * (b.frac >> 32);
    let mut tmp = (ah_bl & LOMASK) + (al_bh & LOMASK) + (al_bl >> 32);
    tmp += 1u64 << 31;
    Fp { frac: ah_bh + (ah_bl >> 32) + (al_bh >> 32) + (tmp >> 32), exp: a.exp + b.exp + 64 }
}

fn round_digit(digits: &mut [u8], ndigits: usize, delta: u64, mut rem: u64, kappa: u64, frac: u64) {
    while rem < frac
        && delta - rem >= kappa
        && (rem.wrapping_add(kappa) < frac
            || frac - rem > rem.wrapping_add(kappa).wrapping_sub(frac))
    {
        digits[ndigits - 1] -= 1;
        rem += kappa;
    }
}

fn generate_digits(fp: &Fp, upper: &Fp, lower: &Fp, digits: &mut [u8; 18], k: &mut i32) -> usize {
    let wfrac = upper.frac - fp.frac;
    let mut delta = upper.frac - lower.frac;
    let one = Fp { frac: 1u64 << -upper.exp, exp: upper.exp };
    let mut part1 = upper.frac >> -one.exp;
    let mut part2 = upper.frac & (one.frac - 1);

    let mut idx = 0usize;
    let mut kappa = 10;
    let mut divp = 10;
    while kappa > 0 {
        let div = TENS[divp];
        let digit = part1 / div;
        if digit != 0 || idx != 0 {
            digits[idx] = digit as u8 + b'0';
            idx += 1;
        }
        part1 -= digit * div;
        kappa -= 1;
        let tmp = (part1 << -one.exp) + part2;
        if tmp <= delta {
            *k += kappa;
            round_digit(digits, idx, delta, tmp, div << -one.exp, wfrac);
            return idx;
        }
        divp += 1;
    }

    let mut unit = 18usize;
    loop {
        part2 = part2.wrapping_mul(10);
        delta = delta.wrapping_mul(10);
        kappa -= 1;
        let digit = part2 >> -one.exp;
        if digit != 0 || idx != 0 {
            digits[idx] = digit as u8 + b'0';
            idx += 1;
        }
        part2 &= one.frac - 1;
        if part2 < delta {
            *k += kappa;
            round_digit(digits, idx, delta, part2, one.frac, wfrac.wrapping_mul(TENS[unit]));
            return idx;
        }
        unit -= 1;
    }
}

fn grisu2(d: f64, digits: &mut [u8; 18], k: &mut i32) -> usize {
    let mut w = build_fp(d);
    let (mut lower, mut upper) = get_normalized_boundaries(&w);
    normalize(&mut w);
    let (cp, kk) = find_cachedpow10(upper.exp);
    w = multiply(&w, &cp);
    upper = multiply(&upper, &cp);
    lower = multiply(&lower, &cp);
    lower.frac += 1;
    upper.frac -= 1;
    *k = -kk;
    generate_digits(&w, &upper, &lower, digits, k)
}

fn emit_digits(digits: &[u8], mut ndigits: usize, k: i32, neg: bool) -> String {
    let nd = ndigits as i32;
    let mut exp = (k + nd - 1).abs();
    let mut out = String::new();
    let ds = |a: usize, b: usize| std::str::from_utf8(&digits[a..b]).unwrap().to_string();

    // Plain integer.
    if k >= 0 && exp < nd + 7 {
        out += &ds(0, ndigits);
        out += &"0".repeat(k as usize);
        return out;
    }
    // Decimal without scientific notation.
    if k < 0 && (k > -7 || exp < 4) {
        let offset = nd - k.abs();
        if offset <= 0 {
            out += "0.";
            out += &"0".repeat((-offset) as usize);
            out += &ds(0, ndigits);
        } else {
            out += &ds(0, offset as usize);
            out.push('.');
            out += &ds(offset as usize, ndigits);
        }
        return out;
    }
    // Scientific notation.
    ndigits = ndigits.min(18 - neg as usize);
    out.push(digits[0] as char);
    if ndigits > 1 {
        out.push('.');
        out += &ds(1, ndigits);
    }
    out.push('e');
    out.push(if k + ndigits as i32 - 1 < 0 { '-' } else { '+' });
    let mut cent = 0;
    if exp > 99 {
        cent = exp / 100;
        out.push((b'0' + cent as u8) as char);
        exp -= cent * 100;
    }
    if exp > 9 {
        let dec = exp / 10;
        out.push((b'0' + dec as u8) as char);
        exp -= dec * 10;
    } else if cent != 0 {
        out.push('0');
    }
    out.push((b'0' + (exp % 10) as u8) as char);
    out
}

/// `fpconv_dtoa` for finite, non-zero values.
fn fpconv_dtoa(d: f64) -> String {
    let neg = d.to_bits() & SIGNMASK != 0;
    let mut digits = [0u8; 18];
    let mut k = 0;
    let n = grisu2(d, &mut digits, &mut k);
    let body = emit_digits(&digits, n, k, neg);
    if neg { format!("-{body}") } else { body }
}

/// Parses a score the way `getDoubleFromObject` (`string2d`) does: strtod
/// on the whole string, no leading space, NaN rejected.
pub fn parse_double(b: &[u8]) -> Option<f64> {
    let s = std::str::from_utf8(b).ok()?;
    if s.is_empty() || s.starts_with(|c: char| c.is_ascii_whitespace()) {
        return None;
    }
    let lower = s.to_ascii_lowercase();
    let body = lower.trim_start_matches(['+', '-']);
    if body.starts_with("nan") || body.starts_with("0x") {
        return None;
    }
    let v: f64 = s.parse().ok()?;
    (!v.is_nan()).then_some(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_redis_formatting() {
        // Every expectation is Redis 7.2's ZSCORE output.
        let cases = [
            (1.0, "1"),
            (-3.0, "-3"),
            (0.1, "0.1"),
            (0.1 + 0.2, "0.30000000000000004"),
            (1e21, "1e+21"),
            (1e22, "1e+22"),
            (1e23, "99999999999999990000000"),
            (123456789012345680000.0, "123456789012345680000"),
            (1e-7, "1e-7"),
            (1.5e-7, "1.5e-7"),
            (123e-20, "1.23e-18"),
            (0.000001, "0.000001"),
            (1e-5, "0.00001"),
            (0.001, "0.001"),
            (1e15, "1000000000000000"),
            (1e17, "100000000000000000"),
            (1e300, "1e+300"),
            (-1.25e-300, "-1.25e-300"),
            (4611686018427387904.0, "4611686018427387904"),
            (9223372036854775807.0, "9223372036854776000"),
            (f64::INFINITY, "inf"),
            (f64::NEG_INFINITY, "-inf"),
            (-0.0, "-0"),
            (5e-324, "5e-324"),
            (f64::MAX, "1.7976931348623157e+308"),
        ];
        for (v, want) in cases {
            assert_eq!(d2string(v), want, "{v:e}");
        }
    }

    #[test]
    fn parses_like_strtod() {
        assert_eq!(parse_double(b"1.5"), Some(1.5));
        assert_eq!(parse_double(b"+inf"), Some(f64::INFINITY));
        assert_eq!(parse_double(b"-inf"), Some(f64::NEG_INFINITY));
        assert_eq!(parse_double(b"1e3"), Some(1000.0));
        for bad in ["", " 1", "1 ", "nan", "abc", "1.5x"] {
            assert_eq!(parse_double(bad.as_bytes()), None, "{bad:?}");
        }
    }
}
