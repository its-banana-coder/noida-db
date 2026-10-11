//! Token filters: transformations of a token stream.

use std::collections::HashSet;
use std::rc::Rc;

use super::chars::{is_digit, lower, lowercase, upper};
use super::fold;
use super::jregex::JPattern;
use super::stemmers;
use super::synonyms::{self, SynonymMap};
use super::token::{SHINGLE, Token, WORD};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LowerLang {
    Default,
    Greek,
    Irish,
    Turkish,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Stem {
    Porter,
    MinimalEnglish,
    Possessive,
    KStem,
    Snowball(rust_stemmers::Algorithm),
    Light(stemmers::Light),
}

#[derive(Debug, Clone)]
pub struct WordDelimiter {
    pub graph: bool,
    pub generate_word_parts: bool,
    pub generate_number_parts: bool,
    pub catenate_words: bool,
    pub catenate_numbers: bool,
    pub catenate_all: bool,
    pub split_on_case_change: bool,
    pub split_on_numerics: bool,
    pub preserve_original: bool,
    pub stem_english_possessive: bool,
    pub protected: HashSet<String>,
}

#[derive(Debug, Clone)]
pub struct Shingle {
    pub min: usize,
    pub max: usize,
    pub output_unigrams: bool,
    pub output_unigrams_if_no_shingles: bool,
    pub separator: String,
    pub filler: String,
}

#[derive(Debug, Clone)]
pub enum TokenFilter {
    Lowercase(LowerLang),
    Uppercase,
    AsciiFolding {
        preserve: bool,
    },
    Stop {
        words: HashSet<String>,
        ignore_case: bool,
        remove_trailing: bool,
    },
    Stem(Stem),
    StemmerOverride(Vec<(String, String)>),
    Shingle(Shingle),
    NGram {
        min: usize,
        max: usize,
        preserve: bool,
        edge: bool,
    },
    WordDelimiter(Box<WordDelimiter>),
    Length {
        min: usize,
        max: usize,
    },
    Truncate(usize),
    Unique {
        same_position: bool,
    },
    RemoveDuplicates,
    Trim,
    Reverse,
    Elision {
        articles: HashSet<String>,
        ignore_case: bool,
    },
    Apostrophe,
    Synonym {
        map: Rc<SynonymMap>,
        graph: bool,
    },
    KeywordMarker {
        words: HashSet<String>,
        pattern: Option<JPattern>,
        ignore_case: bool,
    },
    KeywordRepeat,
    PatternReplace {
        re: JPattern,
        replacement: String,
        all: bool,
    },
    Limit {
        max: usize,
    },
    DecimalDigit,
    CjkWidth,
    CjkBigram {
        output_unigrams: bool,
    },
    Fingerprint {
        separator: String,
        max_output: usize,
    },
    Classic,
    GermanNormalization,
    DelimitedPayload {
        delimiter: char,
    },
    Keep {
        words: HashSet<String>,
        case_sensitive: bool,
    },
    KeepTypes {
        types: HashSet<String>,
        include: bool,
    },
    PatternCapture {
        patterns: Vec<JPattern>,
        preserve: bool,
    },
    CommonGrams {
        words: HashSet<String>,
        ignore_case: bool,
        query_mode: bool,
    },
    Multiplexer {
        chains: Vec<Vec<TokenFilter>>,
        preserve: bool,
    },
    Decompounder {
        words: HashSet<String>,
        min_word: usize,
        min_sub: usize,
        max_sub: usize,
        longest_only: bool,
    },
    ScandinavianFolding,
    ScandinavianNormalization,
    /// `flatten_graph` and other filters that leave the terms alone.
    Identity,
}

impl TokenFilter {
    /// Whether the filter adds Lucene's `KeywordAttribute` (shown as
    /// `keyword` in `_analyze` explain output).
    pub fn sets_keyword_attr(&self) -> bool {
        matches!(
            self,
            TokenFilter::Stem(
                Stem::Porter
                    | Stem::MinimalEnglish
                    | Stem::KStem
                    | Stem::Snowball(_)
                    | Stem::Light(_)
            ) | TokenFilter::StemmerOverride(_)
                | TokenFilter::KeywordMarker { .. }
                | TokenFilter::KeywordRepeat
        )
    }

    pub fn apply(&self, tokens: Vec<Token>) -> Vec<Token> {
        match self {
            TokenFilter::Lowercase(lang) => map_terms(tokens, |t| lower_lang(t, *lang)),
            TokenFilter::Uppercase => map_terms(tokens, |t| t.chars().map(upper).collect()),
            TokenFilter::AsciiFolding { preserve } => {
                let mut out = Vec::with_capacity(tokens.len());
                for t in tokens {
                    let folded = fold::fold(&t.term);
                    if *preserve && folded != t.term {
                        let mut f = t.with_term(folded);
                        out.push(f.clone());
                        f.term = t.term.clone();
                        f.pos_inc = 0;
                        out.push(f);
                    } else {
                        out.push(t.with_term(folded));
                    }
                }
                out
            }
            TokenFilter::Stop { words, ignore_case, remove_trailing } => {
                let n = tokens.len();
                let is_stop = |t: &str| {
                    if *ignore_case { words.contains(&lowercase(t)) } else { words.contains(t) }
                };
                filter_tokens(tokens, |i, t| !is_stop(&t.term) || (!*remove_trailing && i + 1 == n))
            }
            TokenFilter::Stem(stem) => tokens
                .into_iter()
                .map(|t| {
                    if t.keyword {
                        return t;
                    }
                    let s = match stem {
                        Stem::Porter => stemmers::porter(&t.term),
                        Stem::MinimalEnglish => stemmers::minimal_english(&t.term),
                        Stem::Possessive => stemmers::possessive(&t.term),
                        Stem::KStem => stemmers::kstem(&t.term),
                        Stem::Snowball(alg) => stemmers::snowball(*alg, &t.term),
                        Stem::Light(lang) => stemmers::light(*lang, &t.term),
                    };
                    t.with_term(s)
                })
                .collect(),
            TokenFilter::StemmerOverride(rules) => tokens
                .into_iter()
                .map(|mut t| {
                    if !t.keyword
                        && let Some((_, to)) = rules.iter().find(|(from, _)| *from == t.term)
                    {
                        t.term = to.clone();
                        t.keyword = true;
                    }
                    t
                })
                .collect(),
            TokenFilter::Shingle(s) => shingles(tokens, s),
            TokenFilter::NGram { min, max, preserve, edge } => {
                let mut out = Vec::new();
                for t in tokens {
                    let chars: Vec<char> = t.term.chars().collect();
                    let mut first = true;
                    let mut push = |term: String, out: &mut Vec<Token>| {
                        let mut g = t.with_term(term);
                        g.pos_inc = if first { t.pos_inc } else { 0 };
                        first = false;
                        out.push(g);
                    };
                    let starts: Vec<usize> =
                        if *edge { vec![0] } else { (0..chars.len()).collect() };
                    let mut any = false;
                    for a in starts {
                        for n in *min..=*max {
                            if a + n > chars.len() || n == 0 {
                                break;
                            }
                            push(chars[a..a + n].iter().collect(), &mut out);
                            any = true;
                        }
                    }
                    let fits = chars.len() >= *min && chars.len() <= *max;
                    if *preserve && !(any && fits) {
                        push(t.term.clone(), &mut out);
                    }
                }
                out
            }
            TokenFilter::WordDelimiter(wd) => word_delimiter(tokens, wd),
            TokenFilter::Length { min, max } => filter_tokens(tokens, |_, t| {
                let n = t.term.chars().count();
                n >= *min && n <= *max
            }),
            TokenFilter::Truncate(n) => map_terms(tokens, |t| t.chars().take(*n).collect()),
            TokenFilter::Unique { same_position } => {
                let mut seen: HashSet<String> = HashSet::new();
                let mut out = Vec::new();
                let mut carry = 0;
                for mut t in tokens {
                    if *same_position && t.pos_inc > 0 {
                        seen.clear();
                    }
                    if seen.insert(t.term.clone()) {
                        t.pos_inc += carry;
                        carry = 0;
                        out.push(t);
                    } else {
                        carry += t.pos_inc;
                    }
                }
                out
            }
            TokenFilter::RemoveDuplicates => {
                let mut out: Vec<Token> = Vec::new();
                let mut at_pos: Vec<String> = Vec::new();
                for t in tokens {
                    if t.pos_inc > 0 {
                        at_pos.clear();
                    }
                    if at_pos.contains(&t.term) {
                        continue;
                    }
                    at_pos.push(t.term.clone());
                    out.push(t);
                }
                out
            }
            TokenFilter::Trim => map_terms(tokens, |t| t.trim().to_string()),
            TokenFilter::Reverse => map_terms(tokens, |t| t.chars().rev().collect()),
            TokenFilter::Elision { articles, ignore_case } => {
                map_terms(tokens, |t| match t.find(['\'', '\u{2019}']) {
                    Some(p) => {
                        let prefix = &t[..p];
                        let hit = if *ignore_case {
                            articles.contains(&lowercase(prefix))
                        } else {
                            articles.contains(prefix)
                        };
                        if hit {
                            t[p + t[p..].chars().next().map_or(1, char::len_utf8)..].to_string()
                        } else {
                            t.to_string()
                        }
                    }
                    None => t.to_string(),
                })
            }
            TokenFilter::Apostrophe => map_terms(tokens, |t| match t.find(['\'', '\u{2019}']) {
                Some(p) => t[..p].to_string(),
                None => t.to_string(),
            }),
            TokenFilter::Synonym { map, graph } => {
                if *graph {
                    synonyms::apply_graph(map, tokens)
                } else {
                    synonyms::apply_flat(map, tokens)
                }
            }
            TokenFilter::KeywordMarker { words, pattern, ignore_case } => tokens
                .into_iter()
                .map(|mut t| {
                    let hit = match pattern {
                        Some(re) => re.full_match(&t.term),
                        None if *ignore_case => words.contains(&lowercase(&t.term)),
                        None => words.contains(&t.term),
                    };
                    if hit {
                        t.keyword = true;
                    }
                    t
                })
                .collect(),
            TokenFilter::KeywordRepeat => {
                let mut out = Vec::with_capacity(tokens.len() * 2);
                for t in tokens {
                    let mut k = t.clone();
                    k.keyword = true;
                    out.push(k);
                    let mut s = t;
                    s.pos_inc = 0;
                    s.keyword = false;
                    out.push(s);
                }
                out
            }
            TokenFilter::PatternReplace { re, replacement, all } => map_terms(tokens, |t| {
                let chars: Vec<char> = t.chars().collect();
                let mut out = String::new();
                let mut last = 0;
                for g in re.captures_all(&chars) {
                    let Some((a, b)) = g[0] else { continue };
                    out.extend(&chars[last..a]);
                    out.push_str(&re.expand(replacement, &g, &chars));
                    last = b;
                    if !*all {
                        break;
                    }
                }
                out.extend(&chars[last..]);
                out
            }),
            TokenFilter::Limit { max } => tokens.into_iter().take(*max).collect(),
            TokenFilter::DecimalDigit => {
                map_terms(tokens, |t| t.chars().map(decimal_digit).collect())
            }
            TokenFilter::CjkWidth => map_terms(tokens, cjk_width),
            TokenFilter::CjkBigram { output_unigrams } => cjk_bigram(tokens, *output_unigrams),
            TokenFilter::Fingerprint { separator, max_output } => {
                if tokens.is_empty() {
                    return tokens;
                }
                let start = tokens.iter().map(|t| t.start).min().unwrap_or(0);
                let end = tokens.iter().map(|t| t.end).max().unwrap_or(0);
                let mut terms: Vec<String> = tokens.into_iter().map(|t| t.term).collect();
                terms.sort();
                terms.dedup();
                let joined = terms.join(separator);
                if joined.chars().count() > *max_output {
                    return Vec::new();
                }
                vec![Token::new(joined, start, end, "fingerprint")]
            }
            TokenFilter::Classic => map_terms_typed(tokens, |t, ty| match ty {
                "<APOSTROPHE>" => {
                    if t.ends_with("'s") || t.ends_with("'S") {
                        t[..t.len() - 2].to_string()
                    } else {
                        t.to_string()
                    }
                }
                "<ACRONYM>" => t.replace('.', ""),
                _ => t.to_string(),
            }),
            TokenFilter::GermanNormalization => map_terms(tokens, german_normalize),
            TokenFilter::DelimitedPayload { delimiter } => {
                map_terms(tokens, |t| match t.find(*delimiter) {
                    Some(p) => t[..p].to_string(),
                    None => t.to_string(),
                })
            }
            TokenFilter::Keep { words, case_sensitive } => filter_tokens(tokens, |_, t| {
                if *case_sensitive {
                    words.contains(&t.term)
                } else {
                    words.contains(&lowercase(&t.term))
                }
            }),
            TokenFilter::KeepTypes { types, include } => {
                filter_tokens(tokens, |_, t| types.contains(&t.ty) == *include)
            }
            TokenFilter::PatternCapture { patterns, preserve } => {
                pattern_capture(tokens, patterns, *preserve)
            }
            TokenFilter::CommonGrams { words, ignore_case, query_mode } => {
                let grams = common_grams(tokens, words, *ignore_case);
                if *query_mode { common_grams_query(grams) } else { grams }
            }
            TokenFilter::Multiplexer { chains, preserve } => {
                let mut out = Vec::new();
                for t in tokens {
                    let mut here: Vec<Token> = Vec::new();
                    if *preserve {
                        here.push(t.clone());
                    }
                    for chain in chains {
                        let mut ts = vec![t.clone()];
                        for f in chain {
                            ts = f.apply(ts);
                        }
                        here.extend(ts);
                    }
                    let mut first = true;
                    let mut seen: Vec<String> = Vec::new();
                    for mut h in here {
                        if seen.contains(&h.term) {
                            continue;
                        }
                        seen.push(h.term.clone());
                        h.pos_inc = if first { t.pos_inc } else { 0 };
                        first = false;
                        out.push(h);
                    }
                }
                out
            }
            TokenFilter::Decompounder { words, min_word, min_sub, max_sub, longest_only } => {
                let mut out = Vec::new();
                for t in tokens {
                    let chars: Vec<char> = t.term.chars().collect();
                    let n = chars.len();
                    out.push(t.clone());
                    if n < *min_word || t.keyword {
                        continue;
                    }
                    for i in 0..=n.saturating_sub(*min_sub) {
                        let mut longest: Option<String> = None;
                        for j in *min_sub..=*max_sub {
                            if i + j > n {
                                break;
                            }
                            let sub: String = chars[i..i + j].iter().collect();
                            if words.contains(&sub) {
                                if *longest_only {
                                    longest = Some(sub);
                                } else {
                                    let mut p = t.with_term(sub);
                                    p.pos_inc = 0;
                                    out.push(p);
                                }
                            }
                        }
                        if let Some(sub) = longest {
                            let mut p = t.with_term(sub);
                            p.pos_inc = 0;
                            out.push(p);
                        }
                    }
                }
                out
            }
            TokenFilter::ScandinavianFolding => map_terms(tokens, scandinavian_folding),
            TokenFilter::ScandinavianNormalization => map_terms(tokens, scandinavian_normalization),
            TokenFilter::Identity => tokens,
        }
    }
}

/// Lucene's `PatternCaptureGroupTokenFilter`: every capture group of
/// every pattern's matches, in text order, stacked on the token.
fn pattern_capture(tokens: Vec<Token>, patterns: &[JPattern], preserve: bool) -> Vec<Token> {
    let mut out = Vec::new();
    for t in tokens {
        let chars: Vec<char> = t.term.chars().collect();
        let mut caps: Vec<(usize, usize, usize)> = Vec::new();
        for (pi, p) in patterns.iter().enumerate() {
            for g in p.captures_all(&chars) {
                for span in g.iter().skip(1).flatten() {
                    if span.1 > span.0 && !(preserve && span.0 == 0 && span.1 == chars.len()) {
                        caps.push((span.0, pi, span.1));
                    }
                }
            }
        }
        caps.sort();
        caps.dedup_by(|a, b| a.0 == b.0 && a.2 == b.2);
        if caps.is_empty() {
            out.push(t);
            continue;
        }
        let mut first = true;
        if preserve {
            out.push(t.clone());
            first = false;
        }
        for (a, _, b) in caps {
            let mut c = t.with_term(chars[a..b].iter().collect::<String>());
            c.pos_inc = if first { t.pos_inc } else { 0 };
            first = false;
            out.push(c);
        }
    }
    out
}

/// Lucene's `CommonGramsFilter`: a `word_word` gram after every token
/// that, or whose successor, is a common word.
fn common_grams(tokens: Vec<Token>, words: &HashSet<String>, ignore_case: bool) -> Vec<Token> {
    let common = |t: &Token| {
        if ignore_case { words.contains(&lowercase(&t.term)) } else { words.contains(&t.term) }
    };
    let mut out = Vec::new();
    for i in 0..tokens.len() {
        out.push(tokens[i].clone());
        if let Some(next) = tokens.get(i + 1)
            && (common(&tokens[i]) || common(next))
        {
            let mut g = Token::new(
                format!("{}_{}", tokens[i].term, next.term),
                tokens[i].start,
                next.end,
                "gram",
            );
            g.pos_inc = 0;
            g.pos_len = 2;
            out.push(g);
        }
    }
    out
}

/// Lucene's `CommonGramsQueryFilter` over a common-grams stream: a word
/// is dropped when a gram starts at it, and the last word after a gram.
fn common_grams_query(stream: Vec<Token>) -> Vec<Token> {
    // Matches Lucene's CommonGramsQueryFilter: a word followed by a gram is
    // replaced by it, and a trailing word that ends a gram is dropped.
    let mut out: Vec<Token> = Vec::new();
    for t in stream {
        if t.ty == "gram" {
            if out.last().is_some_and(|l| l.ty != "gram") {
                out.pop();
            }
            let mut g = t;
            g.pos_inc = 1;
            g.pos_len = 1;
            out.push(g);
        } else {
            out.push(t);
        }
    }
    if out.len() >= 2 && out[out.len() - 1].ty != "gram" && out[out.len() - 2].ty == "gram" {
        out.pop();
    }
    out
}

fn scandinavian_folding(t: &str) -> String {
    let mut out: Vec<char> = Vec::new();
    let chars: Vec<char> = t.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        let mapped = match c {
            'å' | 'ä' | 'á' | 'à' | 'â' | 'æ' => 'a',
            'Å' | 'Ä' | 'Á' | 'À' | 'Â' | 'Æ' => 'A',
            'ö' | 'ø' | 'ó' | 'ò' | 'ô' => 'o',
            'Ö' | 'Ø' | 'Ó' | 'Ò' | 'Ô' => 'O',
            c => c,
        };
        out.push(mapped);
        if let Some(&n) = chars.get(i + 1) {
            let skip = (matches!(c, 'a' | 'A') && matches!(n, 'a' | 'A' | 'e' | 'E' | 'o' | 'O'))
                || (matches!(c, 'o' | 'O') && matches!(n, 'e' | 'E' | 'o' | 'O'));
            if skip {
                i += 1;
            }
        }
        i += 1;
    }
    out.into_iter().collect()
}

fn scandinavian_normalization(t: &str) -> String {
    let mut out: Vec<char> = Vec::new();
    let chars: Vec<char> = t.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        let n = chars.get(i + 1).copied();
        let pair = match (c, n) {
            ('a', Some('a')) | ('a', Some('o')) => Some('å'),
            ('A', Some('a' | 'A' | 'o' | 'O')) => Some('Å'),
            ('a', Some('e')) => Some('æ'),
            ('A', Some('e' | 'E')) => Some('Æ'),
            ('o', Some('e' | 'o')) => Some('ø'),
            ('O', Some('e' | 'E' | 'o' | 'O')) => Some('Ø'),
            _ => None,
        };
        if let Some(p) = pair {
            out.push(p);
            i += 2;
            continue;
        }
        out.push(match c {
            'ä' => 'æ',
            'Ä' => 'Æ',
            'ö' => 'ø',
            'Ö' => 'Ø',
            c => c,
        });
        i += 1;
    }
    out.into_iter().collect()
}

fn map_terms(tokens: Vec<Token>, f: impl Fn(&str) -> String) -> Vec<Token> {
    tokens
        .into_iter()
        .map(|mut t| {
            t.term = f(&t.term);
            t
        })
        .collect()
}

fn map_terms_typed(tokens: Vec<Token>, f: impl Fn(&str, &str) -> String) -> Vec<Token> {
    tokens
        .into_iter()
        .map(|mut t| {
            t.term = f(&t.term, &t.ty);
            t
        })
        .collect()
}

/// Drops tokens (Lucene's `FilteringTokenFilter`): a dropped token's
/// position increment carries over to the next kept one.
fn filter_tokens(tokens: Vec<Token>, keep: impl Fn(usize, &Token) -> bool) -> Vec<Token> {
    let mut out = Vec::with_capacity(tokens.len());
    let mut carry = 0;
    for (i, mut t) in tokens.into_iter().enumerate() {
        if keep(i, &t) {
            t.pos_inc += carry;
            carry = 0;
            out.push(t);
        } else {
            carry += t.pos_inc;
        }
    }
    out
}

fn lower_lang(t: &str, lang: LowerLang) -> String {
    match lang {
        LowerLang::Default => lowercase(t),
        LowerLang::Turkish => t
            .chars()
            .map(|c| match c {
                'I' => 'ı',
                'İ' => 'i',
                c => lower(c),
            })
            .collect(),
        LowerLang::Greek => t
            .chars()
            .map(|c| match lower(c) {
                'ς' => 'σ',
                'ά' => 'α',
                'έ' => 'ε',
                'ή' => 'η',
                'ί' | 'ϊ' | 'ΐ' => 'ι',
                'ό' => 'ο',
                'ύ' | 'ϋ' | 'ΰ' => 'υ',
                'ώ' => 'ω',
                l => l,
            })
            .collect(),
        LowerLang::Irish => {
            let chars: Vec<char> = t.chars().collect();
            let vowel_upper = |c: char| "AEIOUÁÉÍÓÚ".contains(c);
            if chars.len() > 1 && matches!(chars[0], 'n' | 't') && vowel_upper(chars[1]) {
                format!("{}-{}", chars[0], lowercase(&chars[1..].iter().collect::<String>()))
            } else {
                lowercase(t)
            }
        }
    }
}

fn decimal_digit(c: char) -> char {
    if c.is_ascii_digit() || !is_digit(c) {
        return c;
    }
    // Decimal digits come in runs of ten starting at zero.
    let mut zero = c as u32;
    while zero > 0 && char::from_u32(zero - 1).is_some_and(is_digit) && (c as u32 - (zero - 1)) < 10
    {
        zero -= 1;
    }
    let v = (c as u32 - zero) % 10;
    char::from_digit(v, 10).unwrap_or(c)
}

/// Halfwidth katakana `0xFF65..=0xFF9F` as fullwidth.
const KANA_NORM: [u32; 59] = [
    0x30fb, 0x30f2, 0x30a1, 0x30a3, 0x30a5, 0x30a7, 0x30a9, 0x30e3, 0x30e5, 0x30e7, 0x30c3, 0x30fc,
    0x30a2, 0x30a4, 0x30a6, 0x30a8, 0x30aa, 0x30ab, 0x30ad, 0x30af, 0x30b1, 0x30b3, 0x30b5, 0x30b7,
    0x30b9, 0x30bb, 0x30bd, 0x30bf, 0x30c1, 0x30c4, 0x30c6, 0x30c8, 0x30ca, 0x30cb, 0x30cc, 0x30cd,
    0x30ce, 0x30cf, 0x30d2, 0x30d5, 0x30d8, 0x30db, 0x30de, 0x30df, 0x30e0, 0x30e1, 0x30e2, 0x30e4,
    0x30e6, 0x30e8, 0x30e9, 0x30ea, 0x30eb, 0x30ec, 0x30ed, 0x30ef, 0x30f3, 0x3099, 0x309A,
];

fn cjk_width(t: &str) -> String {
    let mut out: Vec<char> = Vec::new();
    for c in t.chars() {
        let u = c as u32;
        if (0xFF01..=0xFF5E).contains(&u) {
            out.push(char::from_u32(u - 0xFEE0).unwrap_or(c));
        } else if (0xFF65..=0xFF9F).contains(&u) {
            let n = KANA_NORM[(u - 0xFF65) as usize];
            // Voiced / semi-voiced marks combine with the previous kana.
            if (u == 0xFF9E || u == 0xFF9F)
                && let Some(prev) = out.last_mut()
            {
                let p = *prev as u32;
                let voiced = matches!(p, 0x30AB..=0x30C2 | 0x30C4..=0x30C9 | 0x30CF..=0x30DD);
                if u == 0xFF9E && p == 0x30A6 {
                    *prev = 'ヴ';
                    continue;
                }
                if voiced {
                    let is_ha_row =
                        (0x30CF..=0x30DD).contains(&p) && (p - 0x30CF).is_multiple_of(3);
                    let is_kt_row = matches!(p, 0x30AB..=0x30C2 | 0x30C4..=0x30C9) && {
                        let base = if p >= 0x30C4 { p - 0x30C4 } else { p - 0x30AB };
                        base % 2 == 0
                    };
                    if u == 0xFF9E && (is_ha_row || is_kt_row) {
                        *prev = char::from_u32(p + 1).unwrap_or(*prev);
                        continue;
                    }
                    if u == 0xFF9F && is_ha_row {
                        *prev = char::from_u32(p + 2).unwrap_or(*prev);
                        continue;
                    }
                }
            }
            out.push(char::from_u32(n).unwrap_or(c));
        } else {
            out.push(c);
        }
    }
    out.into_iter().collect()
}

fn is_cjk_type(ty: &str) -> bool {
    matches!(ty, "<IDEOGRAPHIC>" | "<HIRAGANA>" | "<KATAKANA>" | "<HANGUL>")
}

fn cjk_bigram(tokens: Vec<Token>, output_unigrams: bool) -> Vec<Token> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < tokens.len() {
        if !is_cjk_type(&tokens[i].ty) {
            out.push(tokens[i].clone());
            i += 1;
            continue;
        }
        // A run of adjacent CJK tokens.
        let mut j = i + 1;
        while j < tokens.len() && is_cjk_type(&tokens[j].ty) && tokens[j].start == tokens[j - 1].end
        {
            j += 1;
        }
        let run = &tokens[i..j];
        if run.len() == 1 {
            let mut t = run[0].clone();
            t.ty = "<SINGLE>".into();
            out.push(t);
        } else {
            for k in 0..run.len() {
                if output_unigrams {
                    let mut u = run[k].clone();
                    u.ty = "<SINGLE>".into();
                    out.push(u);
                }
                if k + 1 < run.len() {
                    let mut b = Token::new(
                        format!("{}{}", run[k].term, run[k + 1].term),
                        run[k].start,
                        run[k + 1].end,
                        "<DOUBLE>",
                    );
                    b.pos_inc = if output_unigrams { 0 } else { 1 };
                    if output_unigrams {
                        b.pos_len = 2;
                    }
                    out.push(b);
                }
            }
        }
        i = j;
    }
    out
}

/// Lucene's `GermanNormalizationFilter`: umlauts and `ß` folded, and
/// `ae`/`oe`/`ue` reduced to the vowel where they stand for an umlaut.
fn german_normalize(t: &str) -> String {
    #[derive(PartialEq)]
    enum St {
        N,
        V,
        U,
    }
    let mut out: Vec<char> = Vec::with_capacity(t.len());
    let mut state = St::N;
    for c in t.chars() {
        match c {
            'a' | 'o' => {
                out.push(c);
                state = St::U;
            }
            'u' => {
                out.push(c);
                state = if state == St::N { St::U } else { St::V };
            }
            'e' => {
                if state != St::U {
                    out.push(c);
                }
                state = St::V;
            }
            'i' | 'q' | 'y' => {
                out.push(c);
                state = St::V;
            }
            'ä' => {
                out.push('a');
                state = St::V;
            }
            'ö' => {
                out.push('o');
                state = St::V;
            }
            'ü' => {
                out.push('u');
                state = St::V;
            }
            'ß' => {
                out.push('s');
                out.push('s');
                state = St::N;
            }
            c => {
                out.push(c);
                state = St::N;
            }
        }
    }
    out.into_iter().collect()
}

fn shingles(tokens: Vec<Token>, s: &Shingle) -> Vec<Token> {
    // Expand position gaps into filler tokens first.
    let mut stream: Vec<Token> = Vec::new();
    for (i, t) in tokens.iter().enumerate() {
        if i > 0 && t.pos_inc > 1 {
            for _ in 1..t.pos_inc {
                let mut f = Token::new(s.filler.clone(), t.start, t.start, WORD);
                f.term = s.filler.clone();
                stream.push(f);
            }
        }
        stream.push(t.clone());
    }
    let n = stream.len();
    let mut out = Vec::new();
    let mut any_shingle = false;
    let unigram_pos_len = 1;
    let _ = unigram_pos_len;
    for i in 0..n {
        let is_filler = |t: &Token| t.term == s.filler && t.start == t.end && !s.filler.is_empty();
        let mut first_here = true;
        let base_inc = if i == 0 { tokens.first().map_or(1, |t| t.pos_inc) } else { 1 };
        if s.output_unigrams && !is_filler(&stream[i]) {
            let mut u = stream[i].clone();
            u.pos_inc = base_inc;
            first_here = false;
            out.push(u);
        }
        for size in s.min.max(2)..=s.max {
            if i + size > n {
                break;
            }
            let parts = &stream[i..i + size];
            if is_filler(&parts[size - 1]) || is_filler(&parts[0]) {
                continue;
            }
            let term = parts.iter().map(|t| t.term.as_str()).collect::<Vec<_>>().join(&s.separator);
            let mut t = Token::new(term, parts[0].start, parts[size - 1].end, SHINGLE);
            t.pos_inc = if first_here { base_inc } else { 0 };
            first_here = false;
            t.pos_len =
                if s.output_unigrams { size as u32 } else { (size + 1 - s.min.max(2)) as u32 };
            out.push(t);
            any_shingle = true;
        }
    }
    if !any_shingle && !s.output_unigrams && s.output_unigrams_if_no_shingles {
        return tokens;
    }
    out
}

/// Character classes of the word delimiter: lower, upper, digit, other.
#[derive(Clone, Copy, PartialEq)]
enum WdType {
    Lower,
    Upper,
    Digit,
    Delim,
}

fn wd_type(c: char) -> WdType {
    if c.is_lowercase() {
        WdType::Lower
    } else if c.is_uppercase() {
        WdType::Upper
    } else if c.is_alphabetic() {
        WdType::Lower
    } else if c.is_numeric() {
        WdType::Digit
    } else {
        WdType::Delim
    }
}

/// The subword ranges (and whether each is numeric) of `chars`.
fn subwords(chars: &[char], wd: &WordDelimiter) -> Vec<(usize, usize, bool)> {
    let ty: Vec<WdType> = chars.iter().map(|&c| wd_type(c)).collect();
    let is_alpha = |t: WdType| matches!(t, WdType::Lower | WdType::Upper);
    let possessive_at = |end: usize| -> bool {
        wd.stem_english_possessive
            && end >= 3
            && end <= chars.len()
            && chars[end - 2] == '\''
            && matches!(chars[end - 1], 's' | 'S')
            && is_alpha(ty[end - 3])
            && (end == chars.len() || ty[end] == WdType::Delim)
    };
    let mut start = 0;
    let mut end = chars.len();
    while start < end && ty[start] == WdType::Delim {
        start += 1;
    }
    while end > start && ty[end - 1] == WdType::Delim {
        end -= 1;
    }
    if possessive_at(end) {
        end -= 2;
    }
    let is_break = |last: WdType, cur: WdType| -> bool {
        if last == cur {
            return false;
        }
        if !wd.split_on_case_change && is_alpha(last) && is_alpha(cur) {
            return false;
        }
        if last == WdType::Upper && is_alpha(cur) {
            return false;
        }
        if !wd.split_on_numerics
            && ((is_alpha(last) && cur == WdType::Digit)
                || (last == WdType::Digit && is_alpha(cur)))
        {
            return false;
        }
        true
    };
    let mut out = Vec::new();
    let mut i = start;
    while i < end {
        while i < end && ty[i] == WdType::Delim {
            i += 1;
        }
        if i >= end {
            break;
        }
        let s = i;
        let mut last = ty[i];
        i += 1;
        while i < end && ty[i] != WdType::Delim && !is_break(last, ty[i]) {
            // An uppercase run followed by lowercase: the last capital
            // starts the next word ("SDBook" stays, "PowerShot" splits).
            last = ty[i];
            i += 1;
        }
        let numeric = ty[s] == WdType::Digit;
        out.push((s, i, numeric));
        if possessive_at(i + 2) && i + 2 <= end {
            i += 2;
        }
    }
    out
}

fn word_delimiter(tokens: Vec<Token>, wd: &WordDelimiter) -> Vec<Token> {
    let mut out = Vec::new();
    let mut carry = 0u32;
    for t in tokens {
        let chars: Vec<char> = t.term.chars().collect();
        let parts = subwords(&chars, wd);
        let whole = parts.len() == 1 && parts[0].0 == 0 && parts[0].1 == chars.len();
        if wd.protected.contains(&t.term) || whole || t.keyword {
            let mut t = t;
            t.pos_inc += carry;
            carry = 0;
            out.push(t);
            continue;
        }
        if parts.is_empty() {
            if wd.preserve_original {
                let mut t = t;
                t.pos_inc += carry;
                carry = 0;
                out.push(t);
            } else {
                carry += t.pos_inc;
            }
            continue;
        }
        // Offsets: a part maps into the token's own span when the term
        // still has the original length.
        let same_len = t.end.saturating_sub(t.start) == chars.len();
        let span = |a: usize, b: usize| {
            if same_len { (t.start + a, t.start + b) } else { (t.start, t.end) }
        };
        let text = |a: usize, b: usize| chars[a..b].iter().collect::<String>();
        // (term, start, end, part index, parts covered, is original)
        let mut items: Vec<(String, usize, usize, usize, usize, u8)> = Vec::new();
        let want_part =
            |numeric: bool| if numeric { wd.generate_number_parts } else { wd.generate_word_parts };
        for (k, &(a, b, numeric)) in parts.iter().enumerate() {
            if want_part(numeric) {
                let (s, e) = span(a, b);
                items.push((text(a, b), s, e, k, 1, 1));
            }
        }
        // Concatenations of runs of same-kind parts.
        let mut concat = |pred: &dyn Fn(bool) -> bool| {
            let mut k = 0;
            while k < parts.len() {
                if !pred(parts[k].2) {
                    k += 1;
                    continue;
                }
                let s = k;
                while k < parts.len() && pred(parts[k].2) {
                    k += 1;
                }
                if k - s > 1 {
                    let term: String = parts[s..k].iter().map(|p| text(p.0, p.1)).collect();
                    let (a, _) = span(parts[s].0, parts[s].1);
                    let (_, e) = span(parts[k - 1].0, parts[k - 1].1);
                    items.push((term, a, e, s, k - s, 2));
                }
            }
        };
        if wd.catenate_words {
            concat(&|numeric| !numeric);
        }
        if wd.catenate_numbers {
            concat(&|numeric| numeric);
        }
        if wd.catenate_all && parts.len() > 1 {
            let term: String = parts.iter().map(|p| text(p.0, p.1)).collect();
            let (a, _) = span(parts[0].0, parts[0].1);
            let (_, e) = span(parts[parts.len() - 1].0, parts[parts.len() - 1].1);
            if !items.iter().any(|it| it.5 == 2 && it.0 == term) {
                items.push((term, a, e, 0, parts.len(), 2));
            }
        }
        if wd.preserve_original {
            items.push((t.term.clone(), t.start, t.end, 0, parts.len(), 0));
        }
        if items.is_empty() {
            carry += t.pos_inc;
            continue;
        }
        if wd.graph {
            // By position, longer spans first, the original before all.
            items.sort_by(|x, y| x.3.cmp(&y.3).then(y.4.cmp(&x.4)).then(x.5.cmp(&y.5)));
        } else {
            // The original first, then by start offset (parts before the
            // concatenations starting with them).
            items.sort_by(|x, y| {
                (x.5 != 0).cmp(&(y.5 != 0)).then(x.1.cmp(&y.1)).then((x.5 == 2).cmp(&(y.5 == 2)))
            });
        }
        let mut last_part: Option<usize> = None;
        for (term, s, e, k, len, _) in items {
            let mut nt = t.with_term(term);
            nt.start = s;
            nt.end = e;
            nt.pos_inc = match last_part {
                None => t.pos_inc + carry,
                Some(p) if k > p => (k - p) as u32,
                Some(_) => 0,
            };
            if last_part.is_none_or(|p| k > p) {
                last_part = Some(k);
            }
            nt.pos_len = if wd.graph { len as u32 } else { 1 };
            out.push(nt);
        }
        carry = 0;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn toks(s: &str) -> Vec<Token> {
        let mut out = Vec::new();
        let mut off = 0;
        for w in s.split(' ') {
            let n = w.chars().count();
            out.push(Token::new(w, off, off + n, WORD));
            off += n + 1;
        }
        out
    }

    fn terms(ts: &[Token]) -> Vec<(String, i64)> {
        let pos = super::super::token::positions(ts);
        ts.iter().zip(pos).map(|(t, p)| (t.term.clone(), p)).collect()
    }

    #[test]
    fn stop_leaves_position_gaps() {
        let f = TokenFilter::Stop {
            words: ["the", "and"].iter().map(|s| s.to_string()).collect(),
            ignore_case: false,
            remove_trailing: true,
        };
        assert_eq!(
            terms(&f.apply(toks("the quick and dead"))),
            [("quick".into(), 1), ("dead".into(), 3)]
        );
    }

    #[test]
    fn shingles_default() {
        let f = TokenFilter::Shingle(Shingle {
            min: 2,
            max: 2,
            output_unigrams: true,
            output_unigrams_if_no_shingles: false,
            separator: " ".into(),
            filler: "_".into(),
        });
        let got: Vec<String> = f.apply(toks("a b c")).into_iter().map(|t| t.term).collect();
        assert_eq!(got, ["a", "a b", "b", "b c", "c"]);
    }

    #[test]
    fn word_delimiter_parts() {
        let wd = WordDelimiter {
            graph: true,
            generate_word_parts: true,
            generate_number_parts: true,
            catenate_words: true,
            catenate_numbers: false,
            catenate_all: false,
            split_on_case_change: true,
            split_on_numerics: true,
            preserve_original: true,
            stem_english_possessive: true,
            protected: HashSet::new(),
        };
        let f = TokenFilter::WordDelimiter(Box::new(wd));
        let got = terms(&f.apply(toks("Wi-Fi PowerShot500")));
        let want: Vec<(String, i64)> = [
            ("Wi-Fi", 0),
            ("WiFi", 0),
            ("Wi", 0),
            ("Fi", 1),
            ("PowerShot500", 2),
            ("PowerShot", 2),
            ("Power", 2),
            ("Shot", 3),
            ("500", 4),
        ]
        .iter()
        .map(|(a, b)| (a.to_string(), *b))
        .collect();
        assert_eq!(got, want);
    }

    #[test]
    fn digits_and_widths() {
        assert_eq!(decimal_digit('٣'), '3');
        assert_eq!(cjk_width("ＡＢＣ１"), "ABC1");
        assert_eq!(cjk_width("ﾄﾞﾗ"), "ドラ");
    }
}
