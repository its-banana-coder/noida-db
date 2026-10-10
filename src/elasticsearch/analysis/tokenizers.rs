//! Tokenizers: text (after char filters) into tokens. Offsets here are
//! character indices into the filtered text; the analyzer corrects them
//! back to the original afterwards.

use regex_lite::Regex;
use unicode_segmentation::UnicodeSegmentation;

use super::jregex::JPattern;

use super::chars::*;
use super::token::{ALPHANUM, NUM, Token, WORD};

/// Which characters an (edge) n-gram or char-group tokenizer keeps.
#[derive(Debug, Clone, Default)]
pub struct TokenChars {
    pub letter: bool,
    pub digit: bool,
    pub whitespace: bool,
    pub punctuation: bool,
    pub symbol: bool,
    pub custom: Vec<char>,
}

impl TokenChars {
    pub fn is_empty(&self) -> bool {
        !(self.letter || self.digit || self.whitespace || self.punctuation || self.symbol)
            && self.custom.is_empty()
    }

    pub fn matches(&self, c: char) -> bool {
        (self.letter && is_letter(c))
            || (self.digit && c.is_numeric())
            || (self.whitespace && is_java_whitespace(c))
            || (self.punctuation && is_punctuation(c))
            || (self.symbol && is_symbol(c))
            || self.custom.contains(&c)
    }
}

#[derive(Debug, Clone)]
pub enum Tokenizer {
    Standard { max_len: usize },
    Classic { max_len: usize },
    UaxUrlEmail { max_len: usize },
    Whitespace { max_len: usize },
    Letter { max_len: usize, lowercase: bool },
    Keyword,
    NGram { min: usize, max: usize, chars: TokenChars, edge: bool },
    Pattern { re: JPattern, group: i64 },
    SimplePattern { re: JPattern },
    SimplePatternSplit { re: JPattern },
    CharGroup { split_on: TokenChars, max_len: usize },
    PathHierarchy { delimiter: char, replacement: char, skip: usize, reverse: bool },
    Thai,
}

impl Tokenizer {
    pub fn tokenize(&self, text: &[char]) -> Vec<Token> {
        match self {
            Tokenizer::Standard { max_len } => standard(text, *max_len),
            Tokenizer::Classic { max_len } => classic(text, *max_len),
            Tokenizer::UaxUrlEmail { max_len } => uax_url_email(text, *max_len),
            Tokenizer::Whitespace { max_len } => {
                char_tokenizer(text, *max_len, |c| !is_java_whitespace(c), false)
            }
            Tokenizer::Letter { max_len, lowercase } => {
                char_tokenizer(text, *max_len, is_letter, *lowercase)
            }
            Tokenizer::Keyword => {
                vec![Token::new(text.iter().collect::<String>(), 0, text.len(), WORD)]
            }
            Tokenizer::NGram { min, max, chars, edge } => ngrams(text, *min, *max, chars, *edge),
            Tokenizer::Pattern { re, group } => pattern(text, re, *group),
            Tokenizer::SimplePattern { re } => simple_pattern(text, re),
            Tokenizer::SimplePatternSplit { re } => simple_pattern_split(text, re),
            Tokenizer::CharGroup { split_on, max_len } => {
                char_tokenizer(text, *max_len, |c| !split_on.matches(c), false)
            }
            Tokenizer::PathHierarchy { delimiter, replacement, skip, reverse } => {
                path_hierarchy(text, *delimiter, *replacement, *skip, *reverse)
            }
            Tokenizer::Thai => thai(text),
        }
    }
}

/// Lucene's `CharTokenizer`: runs of accepted characters, cut every
/// `max_len` characters.
fn char_tokenizer(
    text: &[char],
    max_len: usize,
    keep: impl Fn(char) -> bool,
    lowercase: bool,
) -> Vec<Token> {
    let mut out = Vec::new();
    let mut start = None;
    let mut term = String::new();
    let mut len = 0;
    for (i, &c) in text.iter().enumerate() {
        if keep(c) {
            if start.is_none() {
                start = Some(i);
            }
            term.push(if lowercase { lower(c) } else { c });
            len += 1;
            if len >= max_len.max(1) {
                out.push(Token::new(std::mem::take(&mut term), start.unwrap_or(i), i + 1, WORD));
                start = None;
                len = 0;
            }
        } else if let Some(s) = start.take() {
            out.push(Token::new(std::mem::take(&mut term), s, i, WORD));
            len = 0;
        }
    }
    if let Some(s) = start {
        out.push(Token::new(term, s, text.len(), WORD));
    }
    out
}

/// The kind of word a UAX#29 segment is, as Lucene's `StandardTokenizer`
/// types it (`None`: not a token at all, e.g. punctuation).
fn segment_type(seg: &[char]) -> Option<&'static str> {
    let (mut letters, mut digits, mut hangul, mut kata, mut hira, mut ideo, mut sea, mut emoji) =
        (0, 0, 0, 0, 0, 0, 0, 0);
    for &c in seg {
        if is_ideographic(c) {
            ideo += 1;
        } else if is_hiragana(c) {
            hira += 1;
        } else if is_katakana(c) {
            kata += 1;
        } else if is_hangul(c) {
            hangul += 1;
        } else if is_southeast_asian(c) {
            sea += 1;
        } else if is_digit(c) {
            digits += 1;
        } else if is_letter(c) {
            letters += 1;
        } else if is_emoji(c) {
            emoji += 1;
        }
    }
    Some(if ideo > 0 {
        "<IDEOGRAPHIC>"
    } else if hira > 0 {
        "<HIRAGANA>"
    } else if sea > 0 && letters == 0 && digits == 0 {
        "<SOUTHEAST_ASIAN>"
    } else if kata > 0 && letters == 0 && digits == 0 {
        "<KATAKANA>"
    } else if hangul > 0 && letters == 0 && digits == 0 {
        "<HANGUL>"
    } else if letters > 0 || hangul > 0 || kata > 0 {
        ALPHANUM
    } else if digits > 0 {
        NUM
    } else if emoji > 0 {
        "<EMOJI>"
    } else {
        return None;
    })
}

/// UAX#29 word segments of `text` with their char ranges.
fn word_segments(text: &[char]) -> Vec<(usize, usize)> {
    let s: String = text.iter().collect();
    let mut out = Vec::new();
    let mut ci = 0;
    let mut last_byte = 0;
    for (b, seg) in s.split_word_bound_indices() {
        ci += s[last_byte..b].chars().count();
        last_byte = b;
        let n = seg.chars().count();
        out.push((ci, ci + n));
    }
    out
}

/// Lucene's `StandardTokenizer`: UAX#29 word boundaries, keeping the
/// segments that hold letters, digits, ideographs, kana, Hangul,
/// Southeast Asian runs or emoji.
pub fn standard(text: &[char], max_len: usize) -> Vec<Token> {
    let mut words: Vec<(usize, usize, &'static str)> = Vec::new();
    for (s, e) in word_segments(text) {
        let Some(ty) = segment_type(&text[s..e]) else { continue };
        // Southeast Asian scripts have no word breaks of their own here:
        // a run of them is one token.
        if ty == "<SOUTHEAST_ASIAN>"
            && let Some(last) = words.last_mut()
            && last.2 == ty
            && last.1 == s
        {
            last.1 = e;
            continue;
        }
        words.push((s, e, ty));
    }
    let max_len = max_len.max(1);
    let mut out = Vec::new();
    for (s, e, ty) in words {
        let mut a = s;
        while a < e {
            let b = (a + max_len).min(e);
            out.push(Token::new(text[a..b].iter().collect::<String>(), a, b, ty));
            a = b;
        }
    }
    out
}

/// Common top-level domains for URL / e-mail recognition.
const TLDS: &[&str] = &[
    "com",
    "org",
    "net",
    "edu",
    "gov",
    "mil",
    "int",
    "info",
    "biz",
    "name",
    "pro",
    "aero",
    "coop",
    "museum",
    "mobi",
    "asia",
    "tel",
    "travel",
    "jobs",
    "cat",
    "io",
    "app",
    "dev",
    "ai",
    "xyz",
    "online",
    "site",
    "tech",
    "store",
    "blog",
    "cloud",
    "shop",
    "page",
    "email",
    "news",
    "media",
    "global",
    "group",
    "live",
    "today",
    "world",
    "space",
    "website",
    "agency",
    "digital",
    "network",
    "company",
    "systems",
    "solutions",
    "services",
    "academy",
    "local",
    "localhost",
];

fn is_tld(s: &str) -> bool {
    let l = s.to_ascii_lowercase();
    (l.len() == 2 && l.chars().all(|c| c.is_ascii_alphabetic())) || TLDS.contains(&l.as_str())
}

fn is_host_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '-' || (c as u32 > 0x7F && c.is_alphanumeric())
}

/// A `label(.label)*.tld` host starting at `i`: its end.
fn host_at(text: &[char], i: usize) -> Option<usize> {
    let mut j = i;
    let mut labels = Vec::new();
    loop {
        let s = j;
        while j < text.len() && is_host_char(text[j]) {
            j += 1;
        }
        if j == s {
            break;
        }
        labels.push((s, j));
        if j + 1 < text.len() && text[j] == '.' && is_host_char(text[j + 1]) {
            j += 1;
        } else {
            break;
        }
    }
    // Back off to the longest prefix ending in a known TLD.
    while labels.len() >= 2 {
        let (s, e) = *labels.last()?;
        let tld: String = text[s..e].iter().collect();
        if is_tld(&tld) {
            return Some(e);
        }
        labels.pop();
    }
    None
}

/// An e-mail address or URL starting at `i`: (end, type).
fn url_or_email_at(text: &[char], i: usize) -> Option<(usize, &'static str)> {
    let is_local = |c: char| c.is_ascii_alphanumeric() || "!#$%&'*+/=?^_`{|}~-.".contains(c);
    // E-mail: local@host.
    let mut j = i;
    while j < text.len() && is_local(text[j]) {
        j += 1;
    }
    if j > i
        && j < text.len()
        && text[j] == '@'
        && text[i] != '.'
        && text[j - 1] != '.'
        && let Some(end) = host_at(text, j + 1)
    {
        return Some((end, "<EMAIL>"));
    }
    // URL: scheme://host..., or a bare host with a known TLD.
    let mut k = i;
    while k < text.len() && (text[k].is_ascii_alphanumeric() || "+-.".contains(text[k])) {
        k += 1;
    }
    let after_scheme = if k > i
        && text[i].is_ascii_alphabetic()
        && text.get(k) == Some(&':')
        && text.get(k + 1) == Some(&'/')
        && text.get(k + 2) == Some(&'/')
    {
        Some(k + 3)
    } else {
        None
    };
    let host_start = after_scheme.unwrap_or(i);
    let mut end = host_at(text, host_start)?;
    if after_scheme.is_none() && (i > 0 && (text[i - 1].is_alphanumeric() || text[i - 1] == '@')) {
        return None;
    }
    // Port.
    if text.get(end) == Some(&':') && text.get(end + 1).is_some_and(char::is_ascii_digit) {
        end += 1;
        while end < text.len() && text[end].is_ascii_digit() {
            end += 1;
        }
    }
    // Path, query and fragment, without trailing punctuation.
    if matches!(text.get(end), Some('/') | Some('?') | Some('#')) {
        let path_char = |c: char| c.is_alphanumeric() || "-._~:/?#[]@!$&'()*+,;=%".contains(c);
        while end < text.len() && path_char(text[end]) {
            end += 1;
        }
        while end > host_start && ".,;:!?)'".contains(text[end - 1]) {
            end -= 1;
        }
    }
    Some((end, "<URL>"))
}

/// Lucene's `UAX29URLEmailTokenizer`: the standard tokenizer, with URLs
/// and e-mail addresses kept whole.
fn uax_url_email(text: &[char], max_len: usize) -> Vec<Token> {
    let mut out = Vec::new();
    let mut seg_start = 0;
    let mut i = 0;
    while i < text.len() {
        let boundary = i == 0 || !(text[i - 1].is_alphanumeric() || "._-+%".contains(text[i - 1]));
        if boundary && text[i].is_alphanumeric() {
            if let Some((end, ty)) = url_or_email_at(text, i) {
                for mut t in standard(&text[seg_start..i], max_len) {
                    t.start += seg_start;
                    t.end += seg_start;
                    out.push(t);
                }
                out.push(Token::new(text[i..end].iter().collect::<String>(), i, end, ty));
                i = end;
                seg_start = end;
                continue;
            }
        }
        i += 1;
    }
    for mut t in standard(&text[seg_start..], max_len) {
        t.start += seg_start;
        t.end += seg_start;
        out.push(t);
    }
    out
}

/// One character class of the classic grammar, as an ASCII "shape".
fn classic_shape(c: char) -> char {
    let u = c as u32;
    let cj = matches!(u, 0x3040..=0x30FF | 0x3100..=0x312F | 0x31F0..=0x31FF | 0x3300..=0x337F | 0x3400..=0x4DBF | 0x4E00..=0x9FFF | 0xF900..=0xFAFF | 0xFF65..=0xFF9F);
    if cj {
        'c'
    } else if c.is_ascii_digit() || (!c.is_ascii() && is_digit(c)) {
        '0'
    } else if is_letter(c) || (0x0E00..=0x0E59).contains(&u) {
        'a'
    } else if c.is_ascii() {
        if c.is_ascii_whitespace() { ' ' } else { c }
    } else {
        ' '
    }
}

/// Lucene's `ClassicTokenizer` grammar, matched on character shapes:
/// the longest match of any rule at each position (earlier rules win
/// ties).
fn classic(text: &[char], max_len: usize) -> Vec<Token> {
    use std::sync::OnceLock;
    static RULES: OnceLock<Vec<(Regex, &'static str)>> = OnceLock::new();
    let rules = RULES.get_or_init(|| {
        let alnum = "[a0]+";
        let alpha = "a+";
        let has_digit = "[a0]*0[a0]*";
        let p = "[_\\-/.,]";
        let num_alts = [
            format!("{alnum}{p}{has_digit}"),
            format!("{has_digit}{p}{alnum}"),
            format!("{alnum}(?:{p}{has_digit}{p}{alnum})+"),
            format!("{has_digit}(?:{p}{alnum}{p}{has_digit})+"),
            format!("{alnum}{p}{has_digit}(?:{p}{alnum}{p}{has_digit})+"),
            format!("{has_digit}{p}{alnum}(?:{p}{has_digit}{p}{alnum})+"),
        ];
        let mut v: Vec<(String, &'static str)> = vec![
            (alnum.to_string(), "<ALPHANUM>"),
            (format!("{alpha}(?:'{alpha})+"), "<APOSTROPHE>"),
            ("a\\.(?:a\\.)+".to_string(), "<ACRONYM>"),
            (format!("{alpha}[&@]{alpha}"), "<COMPANY>"),
            (format!("{alnum}(?:[._\\-]{alnum})*@{alnum}(?:[.\\-]{alnum})+"), "<EMAIL>"),
            (format!("{alnum}(?:\\.{alnum})+"), "<HOST>"),
        ];
        v.extend(num_alts.into_iter().map(|r| (r, "<NUM>")));
        v.push(("c".to_string(), "<CJ>"));
        v.push((format!("{alnum}\\.(?:{alnum}\\.)+"), "<ACRONYM_DEP>"));
        v.into_iter()
            .filter_map(|(r, t)| Regex::new(&format!("^(?:{r})")).ok().map(|re| (re, t)))
            .collect()
    });
    let shape: String = text.iter().map(|&c| classic_shape(c)).collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < text.len() {
        let rest = &shape[i..];
        let mut best: Option<(usize, &str)> = None;
        for (re, ty) in rules {
            if let Some(m) = re.find(rest)
                && m.end() > 0
                && best.is_none_or(|b| m.end() > b.0)
            {
                best = Some((m.end(), ty));
            }
        }
        match best {
            Some((len, ty)) => {
                let (mut e, mut ty) = (i + len, ty);
                if ty == "<ACRONYM_DEP>" {
                    // A dotted host with a trailing dot: typed as a host,
                    // the dot dropped.
                    e -= 1;
                    ty = "<HOST>";
                }
                let e2 = e.min(i + max_len.max(1));
                out.push(Token::new(text[i..e2].iter().collect::<String>(), i, e2, ty));
                i += len;
            }
            None => i += 1,
        }
    }
    out
}

fn ngrams(text: &[char], min: usize, max: usize, keep: &TokenChars, edge: bool) -> Vec<Token> {
    let mut out = Vec::new();
    let runs: Vec<(usize, usize)> = if keep.is_empty() {
        if text.is_empty() { vec![] } else { vec![(0, text.len())] }
    } else {
        let mut runs = Vec::new();
        let mut s = None;
        for (i, &c) in text.iter().enumerate() {
            if keep.matches(c) {
                s.get_or_insert(i);
            } else if let Some(st) = s.take() {
                runs.push((st, i));
            }
        }
        if let Some(st) = s {
            runs.push((st, text.len()));
        }
        runs
    };
    let min = min.max(1);
    for (s, e) in runs {
        let starts: Vec<usize> = if edge { vec![s] } else { (s..e).collect() };
        for a in starts {
            for n in min..=max {
                if a + n > e {
                    break;
                }
                out.push(Token::new(text[a..a + n].iter().collect::<String>(), a, a + n, WORD));
            }
        }
    }
    out
}

fn piece(text: &[char], a: usize, b: usize) -> Token {
    Token::new(text[a..b].iter().collect::<String>(), a, b, WORD)
}

fn pattern(text: &[char], re: &JPattern, group: i64) -> Vec<Token> {
    let mut out = Vec::new();
    if group < 0 {
        let mut last = 0;
        for g in re.captures_all(text) {
            let Some((a, b)) = g[0] else { continue };
            if a > last {
                out.push(piece(text, last, a));
            }
            last = b;
        }
        if last < text.len() {
            out.push(piece(text, last, text.len()));
        }
    } else {
        for g in re.captures_all(text) {
            if let Some(Some((a, b))) = g.get(group as usize)
                && b > *a
            {
                out.push(piece(text, *a, *b));
            }
        }
    }
    out
}

fn simple_pattern(text: &[char], re: &JPattern) -> Vec<Token> {
    re.captures_all(text)
        .into_iter()
        .filter_map(|g| g[0])
        .filter(|(a, b)| b > a)
        .map(|(a, b)| piece(text, a, b))
        .collect()
}

fn simple_pattern_split(text: &[char], re: &JPattern) -> Vec<Token> {
    let mut out = Vec::new();
    let mut last = 0;
    for (a, b) in re.captures_all(text).into_iter().filter_map(|g| g[0]).filter(|(a, b)| b > a) {
        if a > last {
            out.push(piece(text, last, a));
        }
        last = b;
    }
    if last < text.len() {
        out.push(piece(text, last, text.len()));
    }
    out
}

fn path_hierarchy(
    text: &[char],
    delimiter: char,
    replacement: char,
    skip: usize,
    reverse: bool,
) -> Vec<Token> {
    if text.is_empty() {
        return Vec::new();
    }
    let render = |a: usize, b: usize| -> String {
        text[a..b].iter().map(|&c| if c == delimiter { replacement } else { c }).collect()
    };
    let delims: Vec<usize> =
        text.iter().enumerate().filter(|(_, c)| **c == delimiter).map(|(i, _)| i).collect();
    let mut out: Vec<Token> = Vec::new();
    if reverse {
        // Suffixes: the whole path, then what follows each delimiter;
        // `skip` drops that many trailing elements.
        let mut end = text.len();
        for _ in 0..skip {
            match delims.iter().rev().find(|&&d| d < end) {
                Some(&d) => end = d,
                None => return Vec::new(),
            }
        }
        let mut starts = vec![0];
        starts.extend(delims.iter().map(|d| d + 1).filter(|&s| s < end));
        for s in starts {
            if s < end {
                out.push(Token::new(render(s, end), s, end, WORD));
            }
        }
    } else {
        // Element starts: a leading delimiter belongs to the first one.
        let mut elem_starts = Vec::new();
        if text[0] != delimiter {
            elem_starts.push(0);
        }
        elem_starts.extend(delims.iter().copied());
        let Some(&start) = elem_starts.get(skip) else { return Vec::new() };
        let mut ends: Vec<usize> = delims.iter().copied().filter(|&d| d > start).collect();
        ends.push(text.len());
        for e in ends {
            out.push(Token::new(render(start, e), start, e, WORD));
        }
    }
    for t in out.iter_mut().skip(1) {
        t.pos_inc = 0;
    }
    out
}

/// The `thai` tokenizer, for non-Thai text: words split on whitespace
/// and surrounding punctuation.
fn thai(text: &[char]) -> Vec<Token> {
    let mut out = Vec::new();
    for t in char_tokenizer(text, usize::MAX, |c| !is_java_whitespace(c), false) {
        let chars: Vec<char> = t.term.chars().collect();
        let mut a = 0;
        let mut b = chars.len();
        while a < b && !chars[a].is_alphanumeric() {
            a += 1;
        }
        while b > a && !chars[b - 1].is_alphanumeric() {
            b -= 1;
        }
        if a < b {
            out.push(Token::new(
                chars[a..b].iter().collect::<String>(),
                t.start + a,
                t.start + b,
                WORD,
            ));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn terms(t: &Tokenizer, s: &str) -> Vec<(String, String)> {
        let chars: Vec<char> = s.chars().collect();
        t.tokenize(&chars).into_iter().map(|t| (t.term, t.ty)).collect()
    }

    #[test]
    fn standard_follows_uax29() {
        let t = Tokenizer::Standard { max_len: 255 };
        let got: Vec<String> = terms(&t, "foo_bar U.S.A. 3.14 dog's e-mail@x.com 12.ab α-βeta ½")
            .into_iter()
            .map(|x| x.0)
            .collect();
        assert_eq!(
            got,
            ["foo_bar", "U.S.A", "3.14", "dog's", "e", "mail", "x.com", "12", "ab", "α", "βeta"]
        );
        let types = terms(&t, "2 x 東京 ひら タワー 한국 🙂");
        let tys: Vec<&str> = types.iter().map(|x| x.1.as_str()).collect();
        assert_eq!(
            tys,
            [
                "<NUM>",
                "<ALPHANUM>",
                "<IDEOGRAPHIC>",
                "<IDEOGRAPHIC>",
                "<HIRAGANA>",
                "<HIRAGANA>",
                "<KATAKANA>",
                "<HANGUL>",
                "<EMOJI>"
            ]
        );
    }

    #[test]
    fn standard_splits_long_tokens() {
        let t = Tokenizer::Standard { max_len: 3 };
        let got: Vec<String> = terms(&t, "quick").into_iter().map(|x| x.0).collect();
        assert_eq!(got, ["qui", "ck"]);
    }

    #[test]
    fn ngram_order_is_by_start_then_length() {
        let t = Tokenizer::NGram { min: 1, max: 2, chars: TokenChars::default(), edge: false };
        let got: Vec<String> = terms(&t, "abc").into_iter().map(|x| x.0).collect();
        assert_eq!(got, ["a", "ab", "b", "bc", "c"]);
    }

    #[test]
    fn path_hierarchy_prefixes_and_suffixes() {
        let t =
            Tokenizer::PathHierarchy { delimiter: '/', replacement: '/', skip: 0, reverse: false };
        let got: Vec<String> = terms(&t, "/one/two/three").into_iter().map(|x| x.0).collect();
        assert_eq!(got, ["/one", "/one/two", "/one/two/three"]);
        let t =
            Tokenizer::PathHierarchy { delimiter: '-', replacement: '/', skip: 2, reverse: false };
        let got: Vec<String> = terms(&t, "one-two-three-four").into_iter().map(|x| x.0).collect();
        assert_eq!(got, ["/three", "/three/four"]);
        let t =
            Tokenizer::PathHierarchy { delimiter: '.', replacement: '.', skip: 0, reverse: true };
        let got: Vec<String> = terms(&t, "www.elastic.co").into_iter().map(|x| x.0).collect();
        assert_eq!(got, ["www.elastic.co", "elastic.co", "co"]);
    }

    #[test]
    fn uax_url_email_keeps_urls_and_emails() {
        let t = Tokenizer::UaxUrlEmail { max_len: 255 };
        let got = terms(&t, "mail e-mail@x.com or http://www.example.com/a?b=1 now");
        assert_eq!(got[1], ("e-mail@x.com".to_string(), "<EMAIL>".to_string()));
        assert_eq!(got[3], ("http://www.example.com/a?b=1".to_string(), "<URL>".to_string()));
    }
}
