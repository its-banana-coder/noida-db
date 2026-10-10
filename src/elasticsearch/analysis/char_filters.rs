//! Char filters: rewrite the text before tokenization, recording how
//! output offsets map back to input offsets (Lucene's `BaseCharFilter`
//! offset corrections) so tokens still point into the original text.

use std::collections::HashSet;

use super::jregex::JPattern;

/// Output-offset to input-offset corrections: the cumulative difference
/// in effect from each recorded output offset onwards.
#[derive(Debug, Clone, Default)]
pub struct Corrections {
    points: Vec<(usize, isize)>,
}

impl Corrections {
    fn cumulative(&self) -> isize {
        self.points.last().map_or(0, |p| p.1)
    }

    /// Records that input `[.., .. + matched)` became output
    /// `[out_start, out_start + replaced)`.
    fn record(&mut self, out_start: usize, matched: usize, replaced: usize) {
        let cum = self.cumulative();
        if replaced < matched {
            self.points.push((out_start + replaced, cum + (matched - replaced) as isize));
        } else {
            for k in 0..replaced - matched {
                self.points.push((out_start + matched + k, cum - k as isize - 1));
            }
        }
    }

    pub fn correct(&self, off: usize) -> usize {
        let idx = self.points.partition_point(|p| p.0 <= off);
        let diff = if idx == 0 { 0 } else { self.points[idx - 1].1 };
        (off as isize + diff).max(0) as usize
    }
}

#[derive(Debug, Clone)]
pub enum CharFilter {
    HtmlStrip {
        escaped: HashSet<String>,
    },
    /// Longest-match replacements, sorted longest key first.
    Mapping {
        rules: Vec<(Vec<char>, Vec<char>)>,
    },
    PatternReplace {
        re: Regex,
        replacement: String,
    },
}

impl CharFilter {
    pub fn apply(&self, text: &[char]) -> (Vec<char>, Corrections) {
        match self {
            CharFilter::HtmlStrip { escaped } => html_strip(text, escaped),
            CharFilter::Mapping { rules } => mapping(text, rules),
            CharFilter::PatternReplace { re, replacement } => {
                pattern_replace(text, re, replacement)
            }
        }
    }
}

fn mapping(text: &[char], rules: &[(Vec<char>, Vec<char>)]) -> (Vec<char>, Corrections) {
    let mut out = Vec::with_capacity(text.len());
    let mut corr = Corrections::default();
    let mut i = 0;
    while i < text.len() {
        let hit = rules.iter().find(|(from, _)| text[i..].starts_with(from));
        match hit {
            Some((from, to)) => {
                let out_start = out.len();
                out.extend_from_slice(to);
                if from.len() != to.len() {
                    corr.record(out_start, from.len(), to.len());
                }
                i += from.len();
            }
            None => {
                out.push(text[i]);
                i += 1;
            }
        }
    }
    (out, corr)
}

/// Parses `mapping` char filter rules (`"a => b"`, with Lucene's
/// escapes), longest source first.
pub fn parse_mapping_rules(rules: &[String]) -> Result<Vec<(Vec<char>, Vec<char>)>, String> {
    fn unescape(s: &str) -> Vec<char> {
        let chars: Vec<char> = s.chars().collect();
        let mut out = Vec::new();
        let mut i = 0;
        while i < chars.len() {
            if chars[i] == '\\' && i + 1 < chars.len() {
                let n = chars[i + 1];
                i += 2;
                match n {
                    't' => out.push('\t'),
                    'n' => out.push('\n'),
                    'r' => out.push('\r'),
                    'b' => out.push('\u{8}'),
                    'f' => out.push('\u{C}'),
                    'u' if i + 4 <= chars.len() => {
                        let hex: String = chars[i..i + 4].iter().collect();
                        if let Some(c) = u32::from_str_radix(&hex, 16).ok().and_then(char::from_u32)
                        {
                            out.push(c);
                        }
                        i += 4;
                    }
                    other => out.push(other),
                }
            } else {
                out.push(chars[i]);
                i += 1;
            }
        }
        out
    }
    let mut out: Vec<(Vec<char>, Vec<char>)> = Vec::new();
    for rule in rules {
        let Some((from, to)) = rule.split_once("=>") else {
            return Err(format!("Invalid Mapping Rule : [{rule}]"));
        };
        let from = unescape(from.trim());
        if from.is_empty() {
            return Err(format!("Invalid Mapping Rule : [{rule}]"));
        }
        let to = unescape(to.trim());
        // A later rule for the same source replaces an earlier one.
        out.retain(|(f, _)| *f != from);
        out.push((from, to));
    }
    out.sort_by(|a, b| b.0.len().cmp(&a.0.len()));
    Ok(out)
}

fn pattern_replace(text: &[char], re: &JPattern, replacement: &str) -> (Vec<char>, Corrections) {
    let mut out: Vec<char> = Vec::with_capacity(text.len());
    let mut corr = Corrections::default();
    let mut last = 0;
    for g in re.captures_all(text) {
        let Some((a, b)) = g[0] else { continue };
        out.extend_from_slice(&text[last..a]);
        let rep: Vec<char> = re.expand(replacement, &g, text).chars().collect();
        if b - a != rep.len() {
            corr.record(out.len(), b - a, rep.len());
        }
        out.extend_from_slice(&rep);
        last = b;
    }
    out.extend_from_slice(&text[last..]);
    (out, corr)
}

/// Inline elements Lucene's `HTMLStripCharFilter` removes without a
/// trace; every other tag becomes a line break.
const INLINE: &[&str] = &[
    "a", "abbr", "acronym", "b", "basefont", "bdo", "big", "cite", "code", "dfn", "em", "font",
    "i", "img", "input", "kbd", "label", "q", "s", "samp", "select", "small", "span", "strike",
    "strong", "sub", "sup", "textarea", "tt", "u", "var",
];

/// HTML 4 character entity names for code points 160..=255, in order.
const LATIN1: &[&str] = &[
    "nbsp", "iexcl", "cent", "pound", "curren", "yen", "brvbar", "sect", "uml", "copy", "ordf",
    "laquo", "not", "shy", "reg", "macr", "deg", "plusmn", "sup2", "sup3", "acute", "micro",
    "para", "middot", "cedil", "sup1", "ordm", "raquo", "frac14", "frac12", "frac34", "iquest",
    "Agrave", "Aacute", "Acirc", "Atilde", "Auml", "Aring", "AElig", "Ccedil", "Egrave", "Eacute",
    "Ecirc", "Euml", "Igrave", "Iacute", "Icirc", "Iuml", "ETH", "Ntilde", "Ograve", "Oacute",
    "Ocirc", "Otilde", "Ouml", "times", "Oslash", "Ugrave", "Uacute", "Ucirc", "Uuml", "Yacute",
    "THORN", "szlig", "agrave", "aacute", "acirc", "atilde", "auml", "aring", "aelig", "ccedil",
    "egrave", "eacute", "ecirc", "euml", "igrave", "iacute", "icirc", "iuml", "eth", "ntilde",
    "ograve", "oacute", "ocirc", "otilde", "ouml", "divide", "oslash", "ugrave", "uacute", "ucirc",
    "uuml", "yacute", "thorn", "yuml",
];

const OTHER_ENTITIES: &[(&str, u32)] = &[
    ("quot", 34),
    ("QUOT", 34),
    ("amp", 38),
    ("AMP", 38),
    ("apos", 39),
    ("lt", 60),
    ("LT", 60),
    ("gt", 62),
    ("GT", 62),
    ("COPY", 169),
    ("REG", 174),
    ("OElig", 338),
    ("oelig", 339),
    ("Scaron", 352),
    ("scaron", 353),
    ("Yuml", 376),
    ("fnof", 402),
    ("circ", 710),
    ("tilde", 732),
    ("Alpha", 913),
    ("Beta", 914),
    ("Gamma", 915),
    ("Delta", 916),
    ("Omega", 937),
    ("alpha", 945),
    ("beta", 946),
    ("gamma", 947),
    ("delta", 948),
    ("epsilon", 949),
    ("lambda", 955),
    ("mu", 956),
    ("pi", 960),
    ("sigma", 963),
    ("omega", 969),
    ("ensp", 8194),
    ("emsp", 8195),
    ("thinsp", 8201),
    ("zwnj", 8204),
    ("zwj", 8205),
    ("lrm", 8206),
    ("rlm", 8207),
    ("ndash", 8211),
    ("mdash", 8212),
    ("lsquo", 8216),
    ("rsquo", 8217),
    ("sbquo", 8218),
    ("ldquo", 8220),
    ("rdquo", 8221),
    ("bdquo", 8222),
    ("dagger", 8224),
    ("Dagger", 8225),
    ("bull", 8226),
    ("hellip", 8230),
    ("permil", 8240),
    ("prime", 8242),
    ("Prime", 8243),
    ("lsaquo", 8249),
    ("rsaquo", 8250),
    ("euro", 8364),
    ("trade", 8482),
    ("TRADE", 8482),
    ("larr", 8592),
    ("uarr", 8593),
    ("rarr", 8594),
    ("darr", 8595),
    ("harr", 8596),
    ("minus", 8722),
    ("infin", 8734),
    ("ne", 8800),
    ("le", 8804),
    ("ge", 8805),
    ("spades", 9824),
    ("clubs", 9827),
    ("hearts", 9829),
    ("diams", 9830),
];

fn entity(name: &str) -> Option<char> {
    if let Some(num) = name.strip_prefix('#') {
        let v = match num.strip_prefix(['x', 'X']) {
            Some(hex) => u32::from_str_radix(hex, 16).ok()?,
            None => num.parse::<u32>().ok()?,
        };
        return char::from_u32(v);
    }
    if let Some(i) = LATIN1.iter().position(|n| *n == name) {
        return char::from_u32(160 + i as u32);
    }
    OTHER_ENTITIES.iter().find(|(n, _)| *n == name).and_then(|(_, v)| char::from_u32(*v))
}

fn find(text: &[char], from: usize, pat: &str) -> Option<usize> {
    let p: Vec<char> = pat.chars().collect();
    (from..text.len().saturating_sub(p.len() - 1))
        .find(|&i| text[i..i + p.len()].iter().zip(&p).all(|(a, b)| a.eq_ignore_ascii_case(b)))
}

/// A tag at `i` (`<name ...>`, `</name>`, `<name/>`): (name, closing,
/// end index past `>`).
fn tag_at(text: &[char], i: usize) -> Option<(String, bool, usize)> {
    let mut j = i + 1;
    let closing = text.get(j) == Some(&'/');
    if closing {
        j += 1;
    }
    let ns = j;
    if !text.get(j).is_some_and(|c| c.is_ascii_alphabetic()) {
        return None;
    }
    while j < text.len() && (text[j].is_ascii_alphanumeric() || ":_-".contains(text[j])) {
        j += 1;
    }
    let name: String = text[ns..j].iter().collect::<String>().to_ascii_lowercase();
    // Attributes, quoted values may hold `>`.
    let mut quote: Option<char> = None;
    while j < text.len() {
        let c = text[j];
        match quote {
            Some(q) if c == q => quote = None,
            Some(_) => {}
            None if c == '"' || c == '\'' => quote = Some(c),
            None if c == '>' => return Some((name, closing, j + 1)),
            None if c == '<' => return None,
            None => {}
        }
        j += 1;
    }
    None
}

fn html_strip(text: &[char], escaped: &HashSet<String>) -> (Vec<char>, Corrections) {
    let mut out: Vec<char> = Vec::with_capacity(text.len());
    let mut corr = Corrections::default();
    let mut i = 0;
    let mut replace = |out: &mut Vec<char>, matched: usize, rep: &[char]| {
        let start = out.len();
        out.extend_from_slice(rep);
        if matched != rep.len() {
            corr.record(start, matched, rep.len());
        }
    };
    while i < text.len() {
        let c = text[i];
        if c == '<' {
            let rest_starts = |p: &str| {
                let p: Vec<char> = p.chars().collect();
                text.len() >= i + p.len()
                    && text[i..i + p.len()].iter().zip(&p).all(|(a, b)| a.eq_ignore_ascii_case(b))
            };
            if rest_starts("<!--") {
                if let Some(e) = find(text, i + 4, "-->") {
                    replace(&mut out, e + 3 - i, &[]);
                    i = e + 3;
                    continue;
                }
            } else if rest_starts("<![CDATA[") {
                if let Some(e) = find(text, i + 9, "]]>") {
                    replace(&mut out, 9, &[]);
                    out.extend_from_slice(&text[i + 9..e]);
                    replace(&mut out, 3, &[]);
                    i = e + 3;
                    continue;
                }
            } else if rest_starts("<!") || rest_starts("<?") {
                if let Some(e) = find(text, i + 2, ">") {
                    replace(&mut out, e + 1 - i, &[]);
                    i = e + 1;
                    continue;
                }
            } else if let Some((name, closing, end)) = tag_at(text, i) {
                if escaped.contains(&name) {
                    out.extend_from_slice(&text[i..end]);
                    i = end;
                    continue;
                }
                if !closing && (name == "script" || name == "style") {
                    let close = format!("</{name}");
                    let stop = find(text, end, &close)
                        .and_then(|p| find(text, p, ">").map(|g| g + 1))
                        .unwrap_or(text.len());
                    replace(&mut out, stop - i, &['\n']);
                    i = stop;
                    continue;
                }
                let rep: &[char] = if INLINE.contains(&name.as_str()) { &[] } else { &['\n'] };
                replace(&mut out, end - i, rep);
                i = end;
                continue;
            }
        } else if c == '&'
            && let Some(semi) = text[i + 1..].iter().take(12).position(|&c| c == ';')
        {
            let name: String = text[i + 1..i + 1 + semi].iter().collect();
            if let Some(ch) = entity(&name) {
                replace(&mut out, semi + 2, &[ch]);
                i += semi + 2;
                continue;
            }
        }
        out.push(c);
        i += 1;
    }
    (out, corr)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(f: &CharFilter, s: &str) -> (String, Corrections) {
        let chars: Vec<char> = s.chars().collect();
        let (o, c) = f.apply(&chars);
        (o.into_iter().collect(), c)
    }

    #[test]
    fn html_strip_tags_entities_and_offsets() {
        let f = CharFilter::HtmlStrip { escaped: HashSet::new() };
        let (s, c) = run(&f, "<p>Hello <b>World</b></p>");
        assert_eq!(s, "\nHello World\n");
        assert_eq!((c.correct(1), c.correct(7), c.correct(12)), (3, 12, 21));
        let (s, _) = run(&f, "a<!-- x -->b<script>x</script>c &amp; &bogus; &#65;");
        assert_eq!(s, "ab\nc & &bogus; A");
    }

    #[test]
    fn mapping_is_longest_match_with_offsets() {
        let rules =
            parse_mapping_rules(&[":) => _happy_".into(), "ab => X".into(), "abc => Y".into()])
                .unwrap();
        let f = CharFilter::Mapping { rules };
        let (s, c) = run(&f, ":) ab abc");
        assert_eq!(s, "_happy_ X Y");
        assert_eq!((c.correct(0), c.correct(7)), (0, 2));
        assert_eq!(c.correct(11), 9);
    }
}
