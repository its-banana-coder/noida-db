//! Full-text search: `to_tsvector`/`to_tsquery`/`plainto_tsquery`, the `@@`
//! match operator, and `ts_rank`.
//!
//! A `tsvector`/`tsquery` value is represented as its canonical Postgres
//! text form (a plain `Value::Text`, re-parsed here on demand) rather than
//! as its own `Value` variant: there's no index to accelerate, so nothing
//! is gained by keeping the parsed form around, and every other type stays
//! untouched.
//!
//! Only the `'english'` and `'simple'` text search configurations are
//! implemented; any other config name runs as `'simple'` (no stopwords, no
//! stemming) rather than erroring — see docs/LIMITATIONS.md.

use rust_stemmers::{Algorithm, Stemmer};

use super::error::{PgError, PgResult, code};

/// PostgreSQL's own `'english'` stopword list
/// (`src/backend/snowball/stopwords/english.stop`, PostgreSQL licence).
const STOPWORDS_EN: &[&str] = &[
    "i",
    "me",
    "my",
    "myself",
    "we",
    "our",
    "ours",
    "ourselves",
    "you",
    "your",
    "yours",
    "yourself",
    "yourselves",
    "he",
    "him",
    "his",
    "himself",
    "she",
    "her",
    "hers",
    "herself",
    "it",
    "its",
    "itself",
    "they",
    "them",
    "their",
    "theirs",
    "themselves",
    "what",
    "which",
    "who",
    "whom",
    "this",
    "that",
    "these",
    "those",
    "am",
    "is",
    "are",
    "was",
    "were",
    "be",
    "been",
    "being",
    "have",
    "has",
    "had",
    "having",
    "do",
    "does",
    "did",
    "doing",
    "would",
    "should",
    "could",
    "ought",
    "i'm",
    "you're",
    "he's",
    "she's",
    "it's",
    "we're",
    "they're",
    "i've",
    "you've",
    "we've",
    "they've",
    "i'd",
    "you'd",
    "he'd",
    "she'd",
    "we'd",
    "they'd",
    "i'll",
    "you'll",
    "he'll",
    "she'll",
    "we'll",
    "they'll",
    "isn't",
    "aren't",
    "wasn't",
    "weren't",
    "hasn't",
    "haven't",
    "hadn't",
    "doesn't",
    "don't",
    "didn't",
    "won't",
    "wouldn't",
    "shan't",
    "shouldn't",
    "can't",
    "cannot",
    "couldn't",
    "mustn't",
    "let's",
    "that's",
    "who's",
    "what's",
    "here's",
    "there's",
    "when's",
    "where's",
    "why's",
    "how's",
    "a",
    "an",
    "the",
    "and",
    "but",
    "if",
    "or",
    "because",
    "as",
    "until",
    "while",
    "of",
    "at",
    "by",
    "for",
    "with",
    "about",
    "against",
    "between",
    "into",
    "through",
    "during",
    "before",
    "after",
    "above",
    "below",
    "to",
    "from",
    "up",
    "down",
    "in",
    "out",
    "on",
    "off",
    "over",
    "under",
    "again",
    "further",
    "then",
    "once",
    "here",
    "there",
    "when",
    "where",
    "why",
    "how",
    "all",
    "any",
    "both",
    "each",
    "few",
    "more",
    "most",
    "other",
    "some",
    "such",
    "no",
    "nor",
    "not",
    "only",
    "own",
    "same",
    "so",
    "than",
    "too",
    "very",
];

fn is_english(config: &str) -> bool {
    config.eq_ignore_ascii_case("english")
}

/// Splits `text` into lowercased word tokens the way Postgres's parser
/// does for plain prose: runs of letters/digits/apostrophes-inside-words,
/// everything else is a separator.
fn words(text: &str) -> Vec<String> {
    let mut out = vec![];
    let mut cur = String::new();
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        if c.is_alphanumeric() {
            cur.push(c.to_lowercase().next().unwrap_or(c));
        } else if c == '\'' && !cur.is_empty() && chars.peek().is_some_and(|n| n.is_alphanumeric())
        {
            cur.push('\'');
        } else if !cur.is_empty() {
            out.push(std::mem::take(&mut cur));
        }
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

/// One word reduced to its lexeme (stopwords dropped as `None`).
fn lexeme(word: &str, config: &str) -> Option<String> {
    if is_english(config) {
        if STOPWORDS_EN.contains(&word) {
            return None;
        }
        let stemmer = Stemmer::create(Algorithm::English);
        Some(stemmer.stem(word).into_owned())
    } else {
        Some(word.to_string())
    }
}

/// `(lexeme, position)` pairs, 1-based, in reading order — what
/// `to_tsvector` and `to_tsquery`/`plainto_tsquery` both tokenize with.
fn tokenize(text: &str, config: &str) -> Vec<(String, u16)> {
    words(text)
        .into_iter()
        .enumerate()
        .filter_map(|(i, w)| lexeme(&w, config).map(|l| (l, (i + 1).min(u16::MAX as usize) as u16)))
        .collect()
}

/// A tsvector's lexemes, each with the (sorted, deduplicated) positions it
/// appears at.
pub type Vector = Vec<(String, Vec<u16>)>;

fn merge_positions(entries: &mut Vector, lexeme: String, pos: u16) {
    match entries.iter_mut().find(|(l, _)| *l == lexeme) {
        Some((_, ps)) if !ps.contains(&pos) => {
            ps.push(pos);
            ps.sort_unstable();
        }
        Some(_) => {}
        None => entries.push((lexeme, vec![pos])),
    }
}

/// `to_tsvector(config, text)`.
pub fn to_tsvector(text: &str, config: &str) -> Vector {
    let mut entries: Vector = vec![];
    for (l, p) in tokenize(text, config) {
        merge_positions(&mut entries, l, p);
    }
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    entries
}

fn quote_lexeme(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// The canonical `tsvector` text a value prints as.
pub fn format_vector(v: &Vector) -> String {
    v.iter()
        .map(|(l, ps)| {
            if ps.is_empty() {
                quote_lexeme(l)
            } else {
                let list: Vec<String> = ps.iter().map(|p| p.to_string()).collect();
                format!("{}:{}", quote_lexeme(l), list.join(","))
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// `setweight(vector, weight)`: labels every position in `vector`'s own
/// text form with `weight` (`A`/`B`/`C`/`D`), e.g. `'cat':1` becomes
/// `'cat':1A`. The label round-trips through the vector's text form but
/// isn't otherwise used here — `rank`'s label-weighting is a documented
/// approximation (see its own doc comment) — so this works directly on
/// text (reusing `parse_vector` for the position numbers only) rather
/// than threading a weight field through `Vector` itself.
pub fn set_weight(s: &str, weight: char) -> PgResult<String> {
    if !matches!(weight, 'A' | 'B' | 'C' | 'D') {
        return Err(PgError::new(
            code::INVALID_PARAMETER_VALUE,
            format!("unrecognized weight: \"{weight}\""),
        ));
    }
    let v = parse_vector(s)?;
    Ok(v.iter()
        .map(|(l, ps)| {
            if ps.is_empty() {
                quote_lexeme(l)
            } else {
                let list: Vec<String> = ps.iter().map(|p| format!("{p}{weight}")).collect();
                format!("{}:{}", quote_lexeme(l), list.join(","))
            }
        })
        .collect::<Vec<_>>()
        .join(" "))
}

/// Parses a `tsvector`'s own text form (`'cat':2 'fox':4,9`), the input
/// syntax for `'...'::tsvector` and what `format_vector` round-trips.
pub fn parse_vector(s: &str) -> PgResult<Vector> {
    let mut out: Vector = vec![];
    let mut chars = s.chars().peekable();
    loop {
        while chars.peek().is_some_and(|c| c.is_whitespace()) {
            chars.next();
        }
        let Some(&c) = chars.peek() else { break };
        let lexeme = if c == '\'' {
            chars.next();
            let mut l = String::new();
            loop {
                match chars.next() {
                    Some('\'') if chars.peek() == Some(&'\'') => {
                        chars.next();
                        l.push('\'');
                    }
                    Some('\'') | None => break,
                    Some(c) => l.push(c),
                }
            }
            l
        } else {
            let mut l = String::new();
            while chars.peek().is_some_and(|c| !c.is_whitespace()) {
                l.push(chars.next().unwrap());
            }
            l
        };
        let mut positions = vec![];
        if chars.peek() == Some(&':') {
            chars.next();
            loop {
                let mut n = String::new();
                while chars.peek().is_some_and(|c| c.is_ascii_digit()) {
                    n.push(chars.next().unwrap());
                }
                // A weight letter (A/B/C/D) may follow a position; ignored.
                while chars.peek().is_some_and(|c| c.is_ascii_alphabetic()) {
                    chars.next();
                }
                if let Ok(p) = n.parse() {
                    positions.push(p);
                }
                if chars.peek() == Some(&',') {
                    chars.next();
                    continue;
                }
                break;
            }
            positions.sort_unstable();
        }
        merge_or_push(&mut out, lexeme, positions);
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(out)
}

fn merge_or_push(out: &mut Vector, lexeme: String, positions: Vec<u16>) {
    match out.iter_mut().find(|(l, _)| *l == lexeme) {
        Some((_, ps)) => {
            for p in positions {
                if !ps.contains(&p) {
                    ps.push(p);
                }
            }
            ps.sort_unstable();
        }
        None => out.push((lexeme, positions)),
    }
}

/// A lexeme's positions with an optional weight label (`'\0'` = none), used
/// only by `concat_vectors`/`parse_weighted`/`format_weighted`.
type WeightedVector = Vec<(String, Vec<(u16, char)>)>;

/// `vector1 || vector2`: unlike matching/ranking, concatenation must keep
/// each side's weight labels (Postgres's own docs: "the positional
/// information... is preserved"), which `Vector`/`parse_vector` don't
/// track at all (see `rank`'s doc comment) — so this parses each side
/// itself, capturing a `'\0'`-for-none weight char per position instead
/// of discarding it. The right side's positions are shifted past the
/// left side's highest position, matching Postgres's own behavior for
/// combining e.g. a title vector and a body vector into one document.
pub fn concat_vectors(a: &str, b: &str) -> PgResult<String> {
    let va = parse_weighted(a)?;
    let vb = parse_weighted(b)?;
    let offset = va.iter().flat_map(|(_, ps)| ps.iter().map(|(p, _)| *p)).max().unwrap_or(0);
    let mut out = va;
    for (lexeme, positions) in vb {
        let shifted: Vec<(u16, char)> =
            positions.into_iter().map(|(p, w)| (p.saturating_add(offset), w)).collect();
        match out.iter_mut().find(|(l, _)| *l == lexeme) {
            Some((_, ps)) => {
                for pw in shifted {
                    if !ps.contains(&pw) {
                        ps.push(pw);
                    }
                }
                ps.sort_unstable();
            }
            None => out.push((lexeme, shifted)),
        }
    }
    out.sort_by(|x, y| x.0.cmp(&y.0));
    Ok(format_weighted(&out))
}

fn parse_weighted(s: &str) -> PgResult<WeightedVector> {
    let mut out: WeightedVector = vec![];
    let mut chars = s.chars().peekable();
    loop {
        while chars.peek().is_some_and(|c| c.is_whitespace()) {
            chars.next();
        }
        let Some(&c) = chars.peek() else { break };
        let lexeme = if c == '\'' {
            chars.next();
            let mut l = String::new();
            loop {
                match chars.next() {
                    Some('\'') if chars.peek() == Some(&'\'') => {
                        chars.next();
                        l.push('\'');
                    }
                    Some('\'') | None => break,
                    Some(c) => l.push(c),
                }
            }
            l
        } else {
            let mut l = String::new();
            while chars.peek().is_some_and(|c| !c.is_whitespace()) {
                l.push(chars.next().unwrap());
            }
            l
        };
        let mut positions = vec![];
        if chars.peek() == Some(&':') {
            chars.next();
            loop {
                let mut n = String::new();
                while chars.peek().is_some_and(|c| c.is_ascii_digit()) {
                    n.push(chars.next().unwrap());
                }
                let weight = if chars.peek().is_some_and(|c| c.is_ascii_alphabetic()) {
                    chars.next().unwrap()
                } else {
                    '\0'
                };
                if let Ok(p) = n.parse() {
                    positions.push((p, weight));
                }
                if chars.peek() == Some(&',') {
                    chars.next();
                    continue;
                }
                break;
            }
            positions.sort_unstable();
        }
        match out.iter_mut().find(|(l, _)| *l == lexeme) {
            Some((_, ps)) => {
                for pw in positions {
                    if !ps.contains(&pw) {
                        ps.push(pw);
                    }
                }
                ps.sort_unstable();
            }
            None => out.push((lexeme, positions)),
        }
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(out)
}

fn format_weighted(v: &WeightedVector) -> String {
    v.iter()
        .map(|(l, ps)| {
            if ps.is_empty() {
                quote_lexeme(l)
            } else {
                let list: Vec<String> = ps
                    .iter()
                    .map(|(p, w)| if *w == '\0' { p.to_string() } else { format!("{p}{w}") })
                    .collect();
                format!("{}:{}", quote_lexeme(l), list.join(","))
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// A parsed `tsquery`.
#[derive(Debug, Clone, PartialEq)]
pub enum Query {
    Lexeme(String, bool),
    Not(Box<Query>),
    And(Box<Query>, Box<Query>),
    Or(Box<Query>, Box<Query>),
    /// `<->` (distance 1) / `<N>`.
    Phrase(Box<Query>, Box<Query>, u16),
}

/// `plainto_tsquery`: every word ANDed together, ignoring any operators in
/// the input (it's plain text, not query syntax).
pub fn plainto_tsquery(text: &str, config: &str) -> Option<Query> {
    and_all(tokenize(text, config).into_iter().map(|(l, _)| Query::Lexeme(l, false)))
}

/// `phraseto_tsquery`: like `plainto_tsquery`, but adjacent words must be
/// adjacent in the match too.
pub fn phraseto_tsquery(text: &str, config: &str) -> Option<Query> {
    let mut it = tokenize(text, config).into_iter();
    let mut acc = Query::Lexeme(it.next()?.0, false);
    for (l, _) in it {
        acc = Query::Phrase(Box::new(acc), Box::new(Query::Lexeme(l, false)), 1);
    }
    Some(acc)
}

/// `websearch_to_tsquery`: web-search-engine-like syntax. Unquoted words
/// are ANDed (each stemmed like `plainto_tsquery`), `"quoted phrases"`
/// become phrase searches (like `phraseto_tsquery`), a leading `-` on a
/// word or phrase excludes it, and the literal word `OR` between two
/// terms makes that one connector an OR instead of an AND. Unlike
/// `to_tsquery`, malformed input (an unmatched quote, a bare `-` or `OR`)
/// never errors — it degrades gracefully, the same way real Postgres's
/// own parser does.
pub fn websearch_to_tsquery(text: &str, config: &str) -> Option<Query> {
    let mut terms: Vec<(Query, bool)> = vec![];
    let mut want_or = false;
    let mut chars = text.chars().peekable();
    loop {
        while chars.peek().is_some_and(|c| c.is_whitespace()) {
            chars.next();
        }
        let Some(&c) = chars.peek() else { break };
        let negate = c == '-';
        if negate {
            chars.next();
        }
        let term = if chars.peek() == Some(&'"') {
            chars.next();
            let mut phrase = String::new();
            loop {
                match chars.next() {
                    Some('"') | None => break,
                    Some(c) => phrase.push(c),
                }
            }
            phraseto_tsquery(&phrase, config)
        } else {
            let mut word = String::new();
            while chars.peek().is_some_and(|c| !c.is_whitespace()) {
                word.push(chars.next().unwrap());
            }
            if word.is_empty() {
                continue;
            }
            if !negate && word.eq_ignore_ascii_case("or") {
                want_or = true;
                continue;
            }
            plainto_tsquery(&word, config)
        };
        if let Some(q) = term {
            let q = if negate { Query::Not(Box::new(q)) } else { q };
            terms.push((q, want_or));
        }
        want_or = false;
    }
    let mut it = terms.into_iter();
    let (acc0, _) = it.next()?;
    let mut acc = acc0;
    for (q, is_or) in it {
        acc = if is_or {
            Query::Or(Box::new(acc), Box::new(q))
        } else {
            Query::And(Box::new(acc), Box::new(q))
        };
    }
    Some(acc)
}

fn and_all(mut it: impl Iterator<Item = Query>) -> Option<Query> {
    let mut acc = it.next()?;
    for q in it {
        acc = Query::And(Box::new(acc), Box::new(q));
    }
    Some(acc)
}

/// `to_tsquery`: the boolean-operator syntax (`a & b`, `a | !b`, `a <-> b`,
/// `a:*` for a prefix match, parentheses), stemming each lexeme it finds.
pub fn to_tsquery(text: &str, config: &str) -> PgResult<Option<Query>> {
    QueryParser { s: text.as_bytes(), i: 0, config, stem: true }.parse_top()
}

/// A `tsquery`'s own text form (what `'...'::tsquery` accepts and what
/// `format_query` produces) — same grammar, but a quoted lexeme is taken
/// literally (it has already been stemmed).
pub fn parse_query_text(text: &str) -> PgResult<Option<Query>> {
    QueryParser { s: text.as_bytes(), i: 0, config: "simple", stem: false }.parse_top()
}

struct QueryParser<'a> {
    s: &'a [u8],
    i: usize,
    config: &'a str,
    /// Stem each lexeme (`to_tsquery`'s own input) vs. take it literally
    /// (parsing an already-canonical tsquery's text form).
    stem: bool,
}

impl QueryParser<'_> {
    fn parse_top(&mut self) -> PgResult<Option<Query>> {
        self.skip_ws();
        if self.i >= self.s.len() {
            return Ok(None);
        }
        let q = self.or_expr()?;
        self.skip_ws();
        if self.i < self.s.len() {
            return Err(syntax_error());
        }
        Ok(Some(q))
    }

    fn skip_ws(&mut self) {
        while self.i < self.s.len() && self.s[self.i].is_ascii_whitespace() {
            self.i += 1;
        }
    }

    fn peek(&self) -> Option<u8> {
        self.s.get(self.i).copied()
    }

    fn or_expr(&mut self) -> PgResult<Query> {
        let mut left = self.and_expr()?;
        loop {
            self.skip_ws();
            if self.peek() == Some(b'|') {
                self.i += 1;
                let right = self.and_expr()?;
                left = Query::Or(Box::new(left), Box::new(right));
            } else {
                return Ok(left);
            }
        }
    }

    fn and_expr(&mut self) -> PgResult<Query> {
        let mut left = self.phrase_expr()?;
        loop {
            self.skip_ws();
            match self.peek() {
                Some(b'&') => {
                    self.i += 1;
                    let right = self.phrase_expr()?;
                    left = Query::And(Box::new(left), Box::new(right));
                }
                _ => return Ok(left),
            }
        }
    }

    fn phrase_expr(&mut self) -> PgResult<Query> {
        let mut left = self.not_expr()?;
        loop {
            self.skip_ws();
            if self.peek() == Some(b'<') {
                let start = self.i;
                self.i += 1;
                let mut n = String::new();
                while self.peek().is_some_and(|c| c.is_ascii_digit()) {
                    n.push(self.s[self.i] as char);
                    self.i += 1;
                }
                // Either `<->` (distance 1, no digits) or exactly `<N>`.
                let dist: u16 = if self.s.get(start + 1) == Some(&b'-')
                    && self.s.get(start + 2) == Some(&b'>')
                {
                    self.i = start + 3;
                    1
                } else if self.peek() == Some(b'>') {
                    self.i += 1;
                    n.parse().map_err(|_| syntax_error())?
                } else {
                    return Err(syntax_error());
                };
                let right = self.not_expr()?;
                left = Query::Phrase(Box::new(left), Box::new(right), dist);
            } else {
                return Ok(left);
            }
        }
    }

    fn not_expr(&mut self) -> PgResult<Query> {
        self.skip_ws();
        if self.peek() == Some(b'!') {
            self.i += 1;
            return Ok(Query::Not(Box::new(self.not_expr()?)));
        }
        self.primary()
    }

    fn primary(&mut self) -> PgResult<Query> {
        self.skip_ws();
        match self.peek() {
            Some(b'(') => {
                self.i += 1;
                let q = self.or_expr()?;
                self.skip_ws();
                if self.peek() != Some(b')') {
                    return Err(syntax_error());
                }
                self.i += 1;
                Ok(q)
            }
            Some(b'\'') => {
                self.i += 1;
                let mut l = String::new();
                loop {
                    match self.s.get(self.i) {
                        Some(b'\'') if self.s.get(self.i + 1) == Some(&b'\'') => {
                            l.push('\'');
                            self.i += 2;
                        }
                        Some(b'\'') => {
                            self.i += 1;
                            break;
                        }
                        Some(&c) => {
                            l.push(c as char);
                            self.i += 1;
                        }
                        None => return Err(syntax_error()),
                    }
                }
                self.lexeme_node(l)
            }
            Some(c) if c.is_ascii_alphanumeric() => {
                let start = self.i;
                while self
                    .peek()
                    .is_some_and(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'\'')
                {
                    self.i += 1;
                }
                let word = String::from_utf8_lossy(&self.s[start..self.i]).to_string();
                self.lexeme_node(word)
            }
            _ => Err(syntax_error()),
        }
    }

    fn lexeme_node(&mut self, raw: String) -> PgResult<Query> {
        let prefix = self.peek() == Some(b':') && self.s.get(self.i + 1) == Some(&b'*');
        if prefix {
            self.i += 2;
        } else if self.peek() == Some(b':') {
            // A weight-restriction list (`:A`, `:AB*`...): skip it, unused.
            self.i += 1;
            while self.peek().is_some_and(|c| c.is_ascii_alphabetic() || c == b'*') {
                self.i += 1;
            }
        }
        let lexeme = if self.stem {
            match lexeme(&raw.to_lowercase(), self.config) {
                Some(l) => l,
                // A stopword alone parses as an always-true placeholder in
                // real Postgres; approximated the same way here.
                None => return Ok(Query::Lexeme(String::new(), false)),
            }
        } else {
            raw
        };
        Ok(Query::Lexeme(lexeme, prefix))
    }
}

fn syntax_error() -> PgError {
    PgError::new(code::SYNTAX_ERROR, "syntax error in tsquery")
}

/// The canonical `tsquery` text a value prints as: fully parenthesized,
/// tightest-binding operator first (`!`, then `<->`, `&`, `|`).
pub fn format_query(q: &Query) -> String {
    fn go(q: &Query, parent_prec: u8) -> String {
        let (s, prec) = match q {
            Query::Lexeme(l, prefix) => {
                (format!("'{}'{}", l.replace('\'', "''"), if *prefix { ":*" } else { "" }), 4)
            }
            Query::Not(x) => (format!("!{}", go(x, 3)), 3),
            Query::Phrase(a, b, 1) => (format!("{} <-> {}", go(a, 2), go(b, 2)), 2),
            Query::Phrase(a, b, n) => (format!("{} <{}> {}", go(a, 2), n, go(b, 2)), 2),
            Query::And(a, b) => (format!("{} & {}", go(a, 1), go(b, 1)), 1),
            Query::Or(a, b) => (format!("{} | {}", go(a, 0), go(b, 0)), 0),
        };
        if prec < parent_prec { format!("( {s} )") } else { s }
    }
    go(q, 0)
}

/// `@@`: does `v` satisfy `q`?
pub fn matches(v: &Vector, q: &Query) -> bool {
    positions_matching(v, q).is_some()
}

/// The set of positions in `v` where `q` matches ending, or `None` if it
/// doesn't match at all — phrase (`<->`) needs to know exactly where a
/// sub-match landed to check adjacency for the next word.
fn positions_matching(v: &Vector, q: &Query) -> Option<Vec<u16>> {
    match q {
        Query::Lexeme(l, false) => v.iter().find(|(x, _)| x == l).map(|(_, p)| p.clone()),
        Query::Lexeme(l, true) => {
            let ps: Vec<u16> = v
                .iter()
                .filter(|(x, _)| x.starts_with(l.as_str()))
                .flat_map(|(_, p)| p.clone())
                .collect();
            (!ps.is_empty()).then_some(ps)
        }
        Query::Not(x) => (!matches(v, x)).then(Vec::new),
        Query::And(a, b) => {
            let (pa, pb) = (matches(v, a), matches(v, b));
            (pa && pb).then(Vec::new)
        }
        Query::Or(a, b) => {
            if matches(v, a) || matches(v, b) {
                Some(vec![])
            } else {
                None
            }
        }
        Query::Phrase(a, b, dist) => {
            let pa = positions_matching(v, a)?;
            let pb = positions_matching(v, b)?;
            let hits: Vec<u16> = pb
                .iter()
                .copied()
                .filter(|p| pa.iter().any(|&x| p.checked_sub(x) == Some(*dist)))
                .collect();
            (!hits.is_empty()).then_some(hits)
        }
    }
}

/// `ts_rank`: how well `v` matches `q`, roughly. This is an approximation
/// of Postgres's own ranking, which weights lexeme "importance" labels and
/// document length in ways nothing here tracks; it orders matches
/// sensibly (more distinct matched terms, closer together, ranks higher)
/// but does not reproduce Postgres's exact numbers.
pub fn rank(v: &Vector, q: &Query) -> f32 {
    fn score(v: &Vector, q: &Query) -> f32 {
        match q {
            Query::Lexeme(l, false) => v.iter().find(|(x, _)| x == l).map_or(0.0, |(_, p)| {
                1.0 / (1.0
                    + p.iter().map(|&x| x as f32).sum::<f32>() / p.len().max(1) as f32 * 0.001)
            }),
            Query::Lexeme(l, true) => {
                let n = v.iter().filter(|(x, _)| x.starts_with(l.as_str())).count();
                if n == 0 { 0.0 } else { 1.0 }
            }
            Query::Not(_) => 0.0,
            Query::And(a, b) | Query::Phrase(a, b, _) => {
                if matches(v, q) {
                    score(v, a) + score(v, b)
                } else {
                    0.0
                }
            }
            Query::Or(a, b) => score(v, a).max(score(v, b)),
        }
    }
    if matches(v, q) { score(v, q) } else { 0.0 }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenizes_and_stems_english() {
        let v = to_tsvector("The quick Foxes are jumping", "english");
        let words: Vec<&str> = v.iter().map(|(l, _)| l.as_str()).collect();
        // "the", "are" are stopwords; the rest are stemmed.
        assert_eq!(words, vec!["fox", "jump", "quick"]);
    }

    #[test]
    fn positions_and_canonical_format() {
        let v = to_tsvector("a fox a fox", "simple");
        assert_eq!(format_vector(&v), "'a':1,3 'fox':2,4");
    }

    #[test]
    fn vector_text_roundtrips() {
        let v = to_tsvector("the quick brown fox", "simple");
        let text = format_vector(&v);
        let back = parse_vector(&text).unwrap();
        assert_eq!(v, back);
    }

    #[test]
    fn query_operators_and_precedence() {
        let q = to_tsquery("fox & quick | !brown", "simple").unwrap().unwrap();
        assert_eq!(format_query(&q), "'fox' & 'quick' | !'brown'");
    }

    #[test]
    fn matching_and_or_not() {
        let v = to_tsvector("the quick brown fox", "english");
        let q = |s: &str| to_tsquery(s, "english").unwrap().unwrap();
        assert!(matches(&v, &q("fox & quick")));
        assert!(!matches(&v, &q("fox & slow")));
        assert!(matches(&v, &q("fox | slow")));
        assert!(matches(&v, &q("!slow")));
        assert!(!matches(&v, &q("!fox")));
    }

    #[test]
    fn phrase_distance() {
        let v = to_tsvector("a quick brown fox jumps", "simple");
        assert!(matches(&v, &to_tsquery("quick <-> brown", "simple").unwrap().unwrap()));
        assert!(!matches(&v, &to_tsquery("brown <-> quick", "simple").unwrap().unwrap()));
        assert!(matches(&v, &to_tsquery("quick <2> fox", "simple").unwrap().unwrap()));
    }

    #[test]
    fn prefix_match() {
        let v = to_tsvector("jumping jumper", "simple");
        assert!(matches(&v, &to_tsquery("jump:*", "simple").unwrap().unwrap()));
    }

    #[test]
    fn plainto_and_phraseto() {
        let v = to_tsvector("a quick brown fox", "english");
        assert!(matches(&v, &plainto_tsquery("quick fox", "english").unwrap()));
        assert!(matches(&v, &phraseto_tsquery("quick brown", "english").unwrap()));
        assert!(!matches(&v, &phraseto_tsquery("brown quick", "english").unwrap()));
    }

    #[test]
    fn rank_orders_more_matches_higher() {
        let v = to_tsvector("the quick brown fox jumps over the lazy dog", "english");
        let one = to_tsquery("fox", "english").unwrap().unwrap();
        let two = to_tsquery("fox & dog", "english").unwrap().unwrap();
        assert!(rank(&v, &two) > rank(&v, &one));
    }
}
