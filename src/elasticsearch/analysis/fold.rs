//! `asciifolding`: Lucene's `ASCIIFoldingFilter` for the Latin letters,
//! ligatures, full-width forms and typographic punctuation that come up
//! in practice (a subset of Lucene's table).

/// Folds of U+00C0..=U+017F, in order ("" = unchanged).
const LATIN: [&str; 192] = [
    "A", "A", "A", "A", "A", "A", "AE", "C", "E", "E", "E", "E", "I", "I", "I", "I", // C0
    "D", "N", "O", "O", "O", "O", "O", "", "O", "U", "U", "U", "U", "Y", "TH", "ss", // D0
    "a", "a", "a", "a", "a", "a", "ae", "c", "e", "e", "e", "e", "i", "i", "i", "i", // E0
    "d", "n", "o", "o", "o", "o", "o", "", "o", "u", "u", "u", "u", "y", "th", "y", // F0
    "A", "a", "A", "a", "A", "a", "C", "c", "C", "c", "C", "c", "C", "c", "D", "d", // 100
    "D", "d", "E", "e", "E", "e", "E", "e", "E", "e", "E", "e", "G", "g", "G", "g", // 110
    "G", "g", "G", "g", "H", "h", "H", "h", "I", "i", "I", "i", "I", "i", "I", "i", // 120
    "I", "i", "IJ", "ij", "J", "j", "K", "k", "q", "L", "l", "L", "l", "L", "l", "L", // 130
    "l", "L", "l", "N", "n", "N", "n", "N", "n", "'n", "N", "n", "O", "o", "O", "o", // 140
    "O", "o", "OE", "oe", "R", "r", "R", "r", "R", "r", "S", "s", "S", "s", "S", "s", // 150
    "S", "s", "T", "t", "T", "t", "T", "t", "U", "u", "U", "u", "U", "u", "U", "u", // 160
    "U", "u", "U", "u", "W", "w", "Y", "y", "Y", "Z", "z", "Z", "z", "Z", "z", "s", // 170
];

fn fold_char(c: char, out: &mut String) -> bool {
    let u = c as u32;
    if (0xC0..=0x17F).contains(&u) {
        let f = LATIN[(u - 0xC0) as usize];
        if !f.is_empty() {
            out.push_str(f);
            return true;
        }
        return false;
    }
    if (0xFF01..=0xFF5E).contains(&u) {
        if let Some(a) = char::from_u32(u - 0xFEE0) {
            out.push(a);
            return true;
        }
    }
    let s = match c {
        'ƀ' | 'ƃ' | 'ɓ' => "b",
        'Ɓ' | 'Ƃ' => "B",
        'ƈ' | 'ȼ' => "c",
        'ƒ' => "f",
        'ǎ' | 'ǟ' | 'ǡ' | 'ǻ' | 'ȁ' | 'ȃ' | 'ȧ' | 'ạ' | 'ả' | 'ấ' | 'ầ' | 'ẩ' | 'ẫ' | 'ậ' | 'ắ'
        | 'ằ' | 'ẳ' | 'ẵ' | 'ặ' => "a",
        'Ǎ' | 'Ǟ' | 'Ǡ' | 'Ǻ' | 'Ȁ' | 'Ȃ' | 'Ȧ' | 'Ạ' | 'Ả' | 'Ấ' | 'Ầ' | 'Ẩ' | 'Ẫ' | 'Ậ' | 'Ắ'
        | 'Ằ' | 'Ẳ' | 'Ẵ' | 'Ặ' => "A",
        'ȅ' | 'ȇ' | 'ȩ' | 'ẹ' | 'ẻ' | 'ẽ' | 'ế' | 'ề' | 'ể' | 'ễ' | 'ệ' => "e",
        'Ȅ' | 'Ȇ' | 'Ȩ' | 'Ẹ' | 'Ẻ' | 'Ẽ' | 'Ế' | 'Ề' | 'Ể' | 'Ễ' | 'Ệ' => "E",
        'ǐ' | 'ȉ' | 'ȋ' | 'ỉ' | 'ị' => "i",
        'Ǐ' | 'Ȉ' | 'Ȋ' | 'Ỉ' | 'Ị' => "I",
        'ǒ' | 'ǫ' | 'ǭ' | 'ǿ' | 'ȍ' | 'ȏ' | 'ȫ' | 'ȭ' | 'ȯ' | 'ȱ' | 'ơ' | 'ọ' | 'ỏ' | 'ố' | 'ồ'
        | 'ổ' | 'ỗ' | 'ộ' | 'ớ' | 'ờ' | 'ở' | 'ỡ' | 'ợ' => "o",
        'Ǒ' | 'Ǫ' | 'Ǭ' | 'Ǿ' | 'Ȍ' | 'Ȏ' | 'Ȫ' | 'Ȭ' | 'Ȯ' | 'Ȱ' | 'Ơ' | 'Ọ' | 'Ỏ' | 'Ố' | 'Ồ'
        | 'Ổ' | 'Ỗ' | 'Ộ' | 'Ớ' | 'Ờ' | 'Ở' | 'Ỡ' | 'Ợ' => "O",
        'ǔ' | 'ǖ' | 'ǘ' | 'ǚ' | 'ǜ' | 'ȕ' | 'ȗ' | 'ư' | 'ụ' | 'ủ' | 'ứ' | 'ừ' | 'ử' | 'ữ' | 'ự' => {
            "u"
        }
        'Ǔ' | 'Ǖ' | 'Ǘ' | 'Ǚ' | 'Ǜ' | 'Ȕ' | 'Ȗ' | 'Ư' | 'Ụ' | 'Ủ' | 'Ứ' | 'Ừ' | 'Ử' | 'Ữ' | 'Ự' => {
            "U"
        }
        'ỳ' | 'ỵ' | 'ỷ' | 'ỹ' | 'ȳ' => "y",
        'Ỳ' | 'Ỵ' | 'Ỷ' | 'Ỹ' | 'Ȳ' => "Y",
        'ș' => "s",
        'Ș' => "S",
        'ț' => "t",
        'Ț' => "T",
        'ẞ' => "SS",
        'ǆ' => "dz",
        'ǅ' | 'Ǆ' => "DZ",
        'ǉ' => "lj",
        'ǌ' => "nj",
        'ﬀ' => "ff",
        'ﬁ' => "fi",
        'ﬂ' => "fl",
        'ﬃ' => "ffi",
        'ﬄ' => "ffl",
        'ﬅ' => "ft",
        'ﬆ' => "st",
        '‘' | '’' | '‚' | '‛' | '′' | '‹' | '›' | '❛' | '❜' => "'",
        '“' | '”' | '„' | '‟' | '″' | '«' | '»' | '❝' | '❞' => "\"",
        '‐' | '‑' | '‒' | '–' | '—' | '⁻' | '₋' => "-",
        '…' => "...",
        '¹' | '₁' | '①' => "1",
        '²' | '₂' | '②' => "2",
        '³' | '₃' | '③' => "3",
        '⁰' | '₀' => "0",
        '\u{2002}' | '\u{2003}' | '\u{2009}' | '\u{200A}' | '\u{202F}' | '\u{205F}'
        | '\u{3000}' => " ",
        _ => return false,
    };
    out.push_str(s);
    true
}

pub fn fold(s: &str) -> String {
    if s.is_ascii() {
        return s.to_string();
    }
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if c.is_ascii() || !fold_char(c, &mut out) {
            out.push(c);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    #[test]
    fn folds_latin() {
        assert_eq!(super::fold("Ünïcödé CAFÉ Straße ﬁne Łódź"), "Unicode CAFE Strasse fine Lodz");
    }
}
