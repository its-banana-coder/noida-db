//! Character classes the analysis components need, approximating Java's
//! `Character` predicates and the Unicode properties Lucene's tokenizers
//! use (scripts, Word_Break-ish classes, emoji).

fn in_ranges(c: char, ranges: &[(u32, u32)]) -> bool {
    let u = c as u32;
    ranges.iter().any(|&(a, b)| u >= a && u <= b)
}

/// Han ideographs (Lucene's `<IDEOGRAPHIC>`).
pub fn is_ideographic(c: char) -> bool {
    in_ranges(
        c,
        &[
            (0x3005, 0x3007),
            (0x3021, 0x3029),
            (0x3038, 0x303B),
            (0x3400, 0x4DBF),
            (0x4E00, 0x9FFF),
            (0xF900, 0xFAFF),
            (0x20000, 0x2FFFF),
            (0x30000, 0x3134F),
        ],
    )
}

pub fn is_hiragana(c: char) -> bool {
    in_ranges(c, &[(0x3041, 0x309F), (0x1B001, 0x1B11F)])
}

pub fn is_katakana(c: char) -> bool {
    in_ranges(
        c,
        &[(0x30A0, 0x30FF), (0x31F0, 0x31FF), (0x32D0, 0x32FE), (0x3300, 0x3357), (0xFF66, 0xFF9F)],
    )
}

pub fn is_hangul(c: char) -> bool {
    in_ranges(
        c,
        &[
            (0x1100, 0x11FF),
            (0x3130, 0x318F),
            (0xA960, 0xA97F),
            (0xAC00, 0xD7A3),
            (0xD7B0, 0xD7FF),
            (0xFFA0, 0xFFDC),
        ],
    )
}

/// Line_Break=Complex_Context scripts (Thai, Lao, Myanmar, Khmer, ...):
/// Lucene's `<SOUTHEAST_ASIAN>` runs.
pub fn is_southeast_asian(c: char) -> bool {
    in_ranges(
        c,
        &[
            (0x0E01, 0x0E3A),
            (0x0E40, 0x0E4F),
            (0x0E81, 0x0EDF),
            (0x1000, 0x103F),
            (0x1050, 0x108F),
            (0x109A, 0x109F),
            (0x1780, 0x17D3),
            (0x17D7, 0x17D7),
            (0x17DC, 0x17DD),
            (0x1950, 0x19DF),
            (0x1A20, 0x1AAD),
            (0xA9E0, 0xA9EF),
            (0xA9FA, 0xA9FE),
            (0xAA60, 0xAADF),
        ],
    )
}

/// Extended_Pictographic (and regional indicators): Lucene's `<EMOJI>`.
pub fn is_emoji(c: char) -> bool {
    in_ranges(
        c,
        &[
            (0xA9, 0xA9),
            (0xAE, 0xAE),
            (0x203C, 0x203C),
            (0x2049, 0x2049),
            (0x2122, 0x2122),
            (0x2139, 0x2139),
            (0x2194, 0x2199),
            (0x21A9, 0x21AA),
            (0x231A, 0x231B),
            (0x2328, 0x2328),
            (0x2388, 0x2388),
            (0x23CF, 0x23CF),
            (0x23E9, 0x23F3),
            (0x23F8, 0x23FA),
            (0x24C2, 0x24C2),
            (0x25AA, 0x25AB),
            (0x25B6, 0x25B6),
            (0x25C0, 0x25C0),
            (0x25FB, 0x25FE),
            (0x2600, 0x27BF),
            (0x2934, 0x2935),
            (0x2B05, 0x2B07),
            (0x2B1B, 0x2B1C),
            (0x2B50, 0x2B50),
            (0x2B55, 0x2B55),
            (0x3030, 0x3030),
            (0x303D, 0x303D),
            (0x3297, 0x3297),
            (0x3299, 0x3299),
            (0x1F000, 0x1FAFF),
            (0x1FC00, 0x1FFFD),
        ],
    )
}

/// Characters that only extend the previous one (combining marks,
/// variation selectors, ZWJ, skin tones).
pub fn is_extend(c: char) -> bool {
    in_ranges(
        c,
        &[
            (0x0300, 0x036F),
            (0x200C, 0x200D),
            (0x20D0, 0x20FF),
            (0xFE00, 0xFE0F),
            (0xFE20, 0xFE2F),
            (0x1F3FB, 0x1F3FF),
            (0xE0020, 0xE007F),
            (0xE0100, 0xE01EF),
        ],
    )
}

/// A decimal digit (general category Nd), not other numerics like `½`.
pub fn is_digit(c: char) -> bool {
    if c.is_ascii_digit() {
        return true;
    }
    if c.is_ascii() || !c.is_numeric() {
        return false;
    }
    !in_ranges(
        c,
        &[
            (0xB2, 0xB3),
            (0xB9, 0xB9),
            (0xBC, 0xBE),
            (0x09F4, 0x09F9),
            (0x0BF0, 0x0BF2),
            (0x0F2A, 0x0F33),
            (0x1369, 0x137C),
            (0x16EE, 0x16F0),
            (0x17F0, 0x17F9),
            (0x2070, 0x209F),
            (0x2150, 0x218F),
            (0x2460, 0x24FF),
            (0x2776, 0x2793),
            (0x2CFD, 0x2CFD),
            (0x3007, 0x3007),
            (0x3021, 0x3029),
            (0x3038, 0x303A),
            (0x3192, 0x3195),
            (0x3220, 0x3229),
            (0x3248, 0x324F),
            (0x3251, 0x325F),
            (0x3280, 0x3289),
            (0x32B1, 0x32BF),
            (0x1F100, 0x1F10C),
        ],
    )
}

/// Java's `Character.isLetter`.
pub fn is_letter(c: char) -> bool {
    c.is_alphabetic() && !is_extend(c) && !c.is_numeric()
}

/// Java's `Character.isWhitespace`: space separators except the
/// non-breaking ones, plus the ASCII controls Java counts.
pub fn is_java_whitespace(c: char) -> bool {
    match c {
        '\t' | '\n' | '\u{0B}' | '\u{0C}' | '\r' | '\u{1C}'..='\u{1F}' => true,
        '\u{A0}' | '\u{2007}' | '\u{202F}' => false,
        _ => {
            matches!(c, ' ' | '\u{1680}' | '\u{2000}'..='\u{2006}' | '\u{2008}'..='\u{200A}')
                || matches!(c, '\u{2028}' | '\u{2029}' | '\u{205F}' | '\u{3000}')
        }
    }
}

/// Java's symbol categories (Sm, Sc, Sk, So).
pub fn is_symbol(c: char) -> bool {
    if c.is_ascii() {
        return matches!(c, '$' | '+' | '<' | '=' | '>' | '^' | '`' | '|' | '~');
    }
    in_ranges(
        c,
        &[
            (0xA2, 0xA9),
            (0xAC, 0xAC),
            (0xAE, 0xB1),
            (0xB4, 0xB4),
            (0xB8, 0xB8),
            (0xD7, 0xD7),
            (0xF7, 0xF7),
            (0x02C2, 0x02C5),
            (0x02D2, 0x02DF),
            (0x2044, 0x2044),
            (0x2052, 0x2052),
            (0x20A0, 0x20CF),
            (0x2100, 0x214F),
            (0x2190, 0x2BFF),
            (0x3004, 0x3004),
            (0x3012, 0x3013),
            (0x3020, 0x3020),
            (0x1F000, 0x1FAFF),
        ],
    ) && !(is_letter(c) || c.is_numeric())
}

/// Java's punctuation categories (Pc, Pd, Ps, Pe, Pi, Pf, Po).
pub fn is_punctuation(c: char) -> bool {
    if c.is_ascii() {
        return c.is_ascii_punctuation() && !is_symbol(c);
    }
    !c.is_alphanumeric() && !c.is_whitespace() && !is_symbol(c) && !c.is_control() && !is_extend(c)
}

/// Java's `toLowerCase` for one code point (single-char mappings only,
/// as `Character.toLowerCase(int)` does).
pub fn lower(c: char) -> char {
    let mut it = c.to_lowercase();
    match (it.next(), it.next()) {
        (Some(l), None) => l,
        _ => c,
    }
}

pub fn upper(c: char) -> char {
    let mut it = c.to_uppercase();
    match (it.next(), it.next()) {
        (Some(u), None) => u,
        _ => c,
    }
}

/// Lowercases a term the way Lucene's `LowerCaseFilter` does (code point
/// by code point, no special casing).
pub fn lowercase(s: &str) -> String {
    s.chars().map(lower).collect()
}
