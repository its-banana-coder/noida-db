//! `highlight`: the unified (default), plain and fvh highlighters' output
//! for the common cases — the query's terms (per field unless
//! `require_field_match: false`, plus `matched_fields`) wrapped in
//! `pre_tags`/`post_tags` (default `<em>`, or the `styled` schema), HTML
//! encoding, a matched phrase as one span (the unified highlighter's
//! weighted matches) or term by term, keyword fields highlighted whole,
//! and the text cut into fragments around the matches: the unified
//! highlighter's sentence (or word) passages, never spanning two values
//! of a multi-valued field, and the plain highlighter's Lucene
//! `Highlighter` fragments (`span` / `simple` fragmenters). The fast
//! vector highlighter is validated like Elasticsearch's and otherwise
//! produces the unified highlighter's fragments.

use std::collections::HashSet;

use serde_json::{Map, Value, json};

use super::analysis;
use super::query_string;
use super::search::{EsError, raw_values, resolve_field};

#[derive(Debug, Clone)]
enum Hl {
    Term(String),
    Phrase(Vec<String>),
    Prefix(String),
    Wildcard(String),
    Fuzzy(String, usize),
}

fn analyze(mappings: &Value, field: &str, text: &str) -> Vec<String> {
    super::search::analyze_for(mappings, field, text)
}

fn text_of(v: Option<&Value>) -> String {
    match v {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Number(n)) => n.to_string(),
        Some(Value::Bool(b)) => b.to_string(),
        _ => String::new(),
    }
}

fn field_spec(v: &Value) -> Option<(&String, &Value)> {
    v.as_object()?.iter().find(|(k, _)| k.as_str() != "boost" && k.as_str() != "_name")
}

/// `AUTO` fuzziness: no edits up to 2 characters, one up to 5, then two.
fn auto_edits(term: &str, fuzziness: Option<&Value>) -> usize {
    let n = term.chars().count();
    match fuzziness {
        Some(Value::Number(x)) => x.as_u64().unwrap_or(0).min(2) as usize,
        Some(Value::String(s)) if s.parse::<usize>().is_ok() => {
            s.parse::<usize>().unwrap_or(0).min(2)
        }
        _ => {
            if n < 3 {
                0
            } else if n < 6 {
                1
            } else {
                2
            }
        }
    }
}

/// The fields a query's field list names: `title^2` is `title`, and a
/// pattern (`title*`) the mapped text-like fields it matches.
fn query_fields(mappings: &Value, list: Option<&Value>) -> Vec<String> {
    let mut out = Vec::new();
    for f in list.and_then(Value::as_array).cloned().unwrap_or_default() {
        let Some(f) = f.as_str() else { continue };
        let f = f.split('^').next().unwrap_or(f);
        out.extend(expand_field_pattern(mappings, f));
    }
    out
}

/// A field name, or for a pattern every mapped field (multi-fields
/// included) of a text or keyword type it matches.
pub(crate) fn expand_field_pattern(mappings: &Value, pattern: &str) -> Vec<String> {
    if !pattern.contains('*') {
        return vec![pattern.to_string()];
    }
    let mut all = Vec::new();
    fn walk(props: Option<&Value>, prefix: &str, out: &mut Vec<(String, String)>) {
        let Some(obj) = props.and_then(Value::as_object) else { return };
        for (k, v) in obj {
            let full = if prefix.is_empty() { k.clone() } else { format!("{prefix}.{k}") };
            match v.get("properties") {
                Some(p) => walk(Some(p), &full, out),
                None => {
                    let ty = v.get("type").and_then(Value::as_str).unwrap_or("object");
                    out.push((full.clone(), ty.to_string()));
                    if let Some(subs) = v.get("fields").and_then(Value::as_object) {
                        for (s, sd) in subs {
                            let ty = sd.get("type").and_then(Value::as_str).unwrap_or("keyword");
                            out.push((format!("{full}.{s}"), ty.to_string()));
                        }
                    }
                }
            }
        }
    }
    walk(mappings.get("properties"), "", &mut all);
    all.into_iter()
        .filter(|(f, ty)| {
            matches!(ty.as_str(), "text" | "match_only_text" | "keyword" | "constant_keyword")
                && glob(pattern, f)
        })
        .map(|(f, _)| f)
        .collect()
}

/// The highlightable terms in `query`, tagged with the field they target.
fn collect(query: &Value, mappings: &Value, out: &mut Vec<(String, Hl)>) {
    let Some(obj) = query.as_object() else { return };
    for (kind, body) in obj {
        match kind.as_str() {
            "match" | "match_phrase" | "match_phrase_prefix" | "match_bool_prefix" => {
                let Some((field, spec)) = field_spec(body) else { continue };
                let (text, fuzziness) = match spec {
                    Value::Object(o) => (text_of(o.get("query")), o.get("fuzziness")),
                    other => (text_of(Some(other)), None),
                };
                let terms = analyze(mappings, field, &text);
                if kind == "match_phrase" && terms.len() > 1 {
                    out.push((field.clone(), Hl::Phrase(terms)));
                } else {
                    let last = terms.len().saturating_sub(1);
                    for (i, t) in terms.into_iter().enumerate() {
                        let prefix = kind.ends_with("prefix") && i == last;
                        let h = if prefix {
                            Hl::Prefix(t)
                        } else if fuzziness.is_some() {
                            let n = auto_edits(&t, fuzziness);
                            Hl::Fuzzy(t, n)
                        } else {
                            Hl::Term(t)
                        };
                        out.push((field.clone(), h));
                    }
                }
            }
            "multi_match" | "combined_fields" => {
                let text = text_of(body.get("query"));
                let phrase = body.get("type").and_then(Value::as_str) == Some("phrase");
                for f in query_fields(mappings, body.get("fields")) {
                    let terms = analyze(mappings, &f, &text);
                    if phrase && terms.len() > 1 {
                        out.push((f, Hl::Phrase(terms)));
                    } else {
                        out.extend(terms.into_iter().map(|t| (f.clone(), Hl::Term(t))));
                    }
                }
            }
            "term" => {
                if let Some((field, spec)) = field_spec(body) {
                    let v = match spec {
                        Value::Object(o) => text_of(o.get("value")),
                        other => text_of(Some(other)),
                    };
                    out.push((field.clone(), Hl::Term(v)));
                }
            }
            "terms" => {
                if let Some((field, Value::Array(vals))) = field_spec(body) {
                    out.extend(vals.iter().map(|v| (field.clone(), Hl::Term(text_of(Some(v))))));
                }
            }
            "prefix" | "wildcard" | "fuzzy" | "regexp" => {
                let Some((field, spec)) = field_spec(body) else { continue };
                let (v, fuzziness) = match spec {
                    Value::Object(o) => (text_of(o.get("value")), o.get("fuzziness")),
                    other => (text_of(Some(other)), None),
                };
                let h = match kind.as_str() {
                    "prefix" => Hl::Prefix(v),
                    "fuzzy" => {
                        let n = auto_edits(&v, fuzziness);
                        Hl::Fuzzy(v, n)
                    }
                    "regexp" => continue,
                    _ => Hl::Wildcard(v),
                };
                out.push((field.clone(), h));
            }
            "bool" => {
                for key in ["must", "should", "filter"] {
                    match body.get(key) {
                        Some(Value::Array(a)) => a.iter().for_each(|q| collect(q, mappings, out)),
                        Some(q) => collect(q, mappings, out),
                        None => {}
                    }
                }
            }
            "dis_max" => {
                for q in body.get("queries").and_then(Value::as_array).cloned().unwrap_or_default()
                {
                    collect(&q, mappings, out);
                }
            }
            "constant_score" => {
                if let Some(f) = body.get("filter") {
                    collect(f, mappings, out);
                }
            }
            "nested" => {
                if let Some(q) = body.get("query") {
                    collect(q, mappings, out);
                }
            }
            "query_string" => {
                if let Ok(q) = query_string::query_string(body, mappings) {
                    collect(&q, mappings, out);
                }
            }
            "simple_query_string" => {
                if let Ok(q) = query_string::simple_query_string(body, mappings) {
                    collect(&q, mappings, out);
                }
            }
            _ => {}
        }
    }
}

/// Whether `query` holds a `nested` clause (which turns the unified
/// highlighter's weighted matches off).
pub(crate) fn has_nested(query: &Value) -> bool {
    match query {
        Value::Object(o) => o.iter().any(|(k, v)| k == "nested" || has_nested(v)),
        Value::Array(a) => a.iter().any(has_nested),
        _ => false,
    }
}

fn glob(pattern: &str, text: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();
    fn rec(p: &[char], t: &[char]) -> bool {
        match (p.first(), t.first()) {
            (None, None) => true,
            (Some('*'), _) => rec(&p[1..], t) || (!t.is_empty() && rec(p, &t[1..])),
            (Some('?'), Some(_)) => rec(&p[1..], &t[1..]),
            (Some(a), Some(b)) if a == b => rec(&p[1..], &t[1..]),
            _ => false,
        }
    }
    rec(&p, &t)
}

/// Levenshtein distance with adjacent transpositions (fuzzy queries'
/// default `transpositions: true`).
fn edit_distance(a: &str, b: &str) -> usize {
    let (a, b): (Vec<char>, Vec<char>) = (a.chars().collect(), b.chars().collect());
    let mut d = vec![vec![0usize; b.len() + 1]; a.len() + 1];
    for (i, row) in d.iter_mut().enumerate() {
        row[0] = i;
    }
    for (j, v) in d[0].iter_mut().enumerate() {
        *v = j;
    }
    for i in 1..=a.len() {
        for j in 1..=b.len() {
            let cost = usize::from(a[i - 1] != b[j - 1]);
            let mut v = (d[i - 1][j] + 1).min(d[i][j - 1] + 1).min(d[i - 1][j - 1] + cost);
            if i > 1 && j > 1 && a[i - 1] == b[j - 2] && a[i - 2] == b[j - 1] {
                v = v.min(d[i - 2][j - 2] + 1);
            }
            d[i][j] = v;
        }
    }
    d[a.len()][b.len()]
}

/// A token: its term and byte range in the text.
type Token = (String, usize, usize);

/// The tokens of `field`'s value `text` (its index analyzer), with byte
/// offsets.
fn tokenize(mappings: &Value, field: &str, text: &str, keyword: bool) -> Vec<Token> {
    if keyword {
        if text.is_empty() { vec![] } else { vec![(text.to_string(), 0, text.len())] }
    } else {
        let toks = analysis::field_tokens(mappings, field, text, analysis::Mode::Index);
        analysis::with_byte_offsets(text, toks)
    }
}

/// Spans of `text` to tag for `field`: its own matches, plus (with
/// `matched_fields`) each other field's matches in its own analysis of
/// the same text.
#[allow(clippy::too_many_arguments)]
fn field_spans(
    mappings: &Value,
    field: &str,
    text: &str,
    keyword: bool,
    terms: &[(String, Hl)],
    matched_fields: &[String],
    hls: &[&Hl],
    weighted: bool,
    limited: &dyn Fn(&str, Vec<Token>) -> Vec<Token>,
) -> Vec<(usize, usize)> {
    if matched_fields.is_empty() {
        return spans(&limited(text, tokenize(mappings, field, text, keyword)), hls, weighted);
    }
    let mut all = Vec::new();
    let mut sources: Vec<&str> = vec![field];
    sources.extend(matched_fields.iter().map(String::as_str).filter(|f| *f != field));
    for f in sources {
        let these: Vec<&Hl> = terms.iter().filter(|(tf, _)| tf == f).map(|(_, h)| h).collect();
        if these.is_empty() {
            continue;
        }
        all.extend(spans(&limited(text, tokenize(mappings, f, text, keyword)), &these, weighted));
    }
    all.sort();
    let mut merged: Vec<(usize, usize)> = Vec::new();
    for (s, e) in all {
        match merged.last_mut() {
            Some(last) if s < last.1 => last.1 = last.1.max(e),
            _ => merged.push((s, e)),
        }
    }
    merged
}

/// Which tokens a query term matches, and where its phrases occur (as
/// inclusive token ranges).
fn token_matches(tokens: &[Token], hls: &[&Hl]) -> (Vec<bool>, Vec<(usize, usize)>) {
    let mut hit = vec![false; tokens.len()];
    let mut phrases = Vec::new();
    for (i, (tok, _, _)) in tokens.iter().enumerate() {
        for h in hls {
            let m = match h {
                Hl::Term(t) => tok == t,
                Hl::Prefix(p) => tok.starts_with(p.as_str()),
                Hl::Wildcard(w) => glob(w, tok),
                Hl::Fuzzy(t, n) => edit_distance(tok, t) <= *n,
                Hl::Phrase(terms) => {
                    if tokens.len() >= i + terms.len()
                        && terms.iter().enumerate().all(|(k, t)| &tokens[i + k].0 == t)
                    {
                        phrases.push((i, i + terms.len() - 1));
                    }
                    false
                }
            };
            hit[i] |= m;
        }
    }
    (hit, phrases)
}

/// Byte spans of `tokens` to tag: matched terms, and each phrase either
/// as one span (`weighted`) or term by term.
fn spans(tokens: &[Token], hls: &[&Hl], weighted: bool) -> Vec<(usize, usize)> {
    let (hit, phrases) = token_matches(tokens, hls);
    let mut out: Vec<(usize, usize)> =
        tokens.iter().zip(&hit).filter(|(_, h)| **h).map(|(t, _)| (t.1, t.2)).collect();
    for (a, b) in phrases {
        if weighted {
            out.push((tokens[a].1, tokens[b].2));
        } else {
            out.extend(tokens[a..=b].iter().map(|t| (t.1, t.2)));
        }
    }
    out.sort();
    // Merge overlapping spans (a phrase and its own terms).
    let mut merged: Vec<(usize, usize)> = Vec::new();
    for (s, e) in out {
        match merged.last_mut() {
            Some(last) if s < last.1 => last.1 = last.1.max(e),
            _ => merged.push((s, e)),
        }
    }
    merged
}

/// `encoder: html`: the characters Elasticsearch escapes.
fn push_encoded(out: &mut String, s: &str, html: bool) {
    if !html {
        out.push_str(s);
        return;
    }
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#x27;"),
            '/' => out.push_str("&#x2F;"),
            c => out.push(c),
        }
    }
}

fn push_chars(out: &mut String, c: &[char], html: bool) {
    push_encoded(out, &c.iter().collect::<String>(), html);
}

/// Java's sentence `BreakIterator` boundaries (approximately): after `!`
/// or `?`, after `.` unless a lowercase letter follows, and after line or
/// paragraph separators; the trailing whitespace stays with the sentence.
fn sentence_breaks(c: &[char]) -> Vec<usize> {
    let mut out = vec![0];
    let mut i = 0;
    while i < c.len() {
        let ch = c[i];
        if ch == '\n' || ch == '\u{2029}' {
            out.push(i + 1);
        } else if matches!(ch, '.' | '!' | '?') {
            let mut j = i + 1;
            while j < c.len() && matches!(c[j], '"' | '\'' | ')' | ']') {
                j += 1;
            }
            let mut k = j;
            while k < c.len() && c[k].is_whitespace() && c[k] != '\u{2029}' {
                k += 1;
            }
            if k < c.len() && k > j && (ch != '.' || !c[k].is_lowercase()) {
                out.push(k);
                i = k;
                continue;
            }
        }
        i += 1;
    }
    out.push(c.len());
    out.dedup();
    out
}

/// Java's word `BreakIterator` boundaries: between runs of word
/// characters, runs of spaces, and around each punctuation character.
fn word_breaks(c: &[char]) -> Vec<usize> {
    let class = |ch: char| {
        if ch.is_alphanumeric() || ch == '_' {
            1
        } else if ch.is_whitespace() && ch != '\u{2029}' {
            0
        } else {
            2
        }
    };
    let mut out = vec![0];
    for i in 1..c.len() {
        let (a, b) = (class(c[i - 1]), class(c[i]));
        if a != b || a == 2 {
            out.push(i);
        }
    }
    out.push(c.len());
    out.dedup();
    out
}

fn preceding(b: &[usize], off: usize) -> usize {
    b.iter().rev().find(|&&x| x < off).copied().unwrap_or(0)
}

fn following(b: &[usize], off: usize, len: usize) -> usize {
    b.iter().find(|&&x| x > off).copied().unwrap_or(len)
}

/// Elasticsearch's `BoundedBreakIteratorScanner`: a passage is the
/// sentences around a match, grown while it stays within `max_len`; a
/// sentence longer than that is cut at word boundaries around the match.
struct Bounded<'a> {
    sentences: &'a [usize],
    words: &'a [usize],
    len: usize,
    max_len: usize,
    window_start: usize,
    window_end: usize,
    inner_start: usize,
    inner_end: usize,
}

impl Bounded<'_> {
    /// The passage containing the match starting at `start`.
    fn passage(&mut self, start: usize) -> (usize, usize) {
        let offset = start + 1;
        if offset > self.window_start && offset < self.window_end {
            self.inner_start = self.inner_end;
            self.inner_end = self.window_end;
        } else {
            self.window_start = preceding(self.sentences, offset);
            self.inner_start = self.window_start;
            self.window_end = following(self.sentences, offset - 1, self.len);
            self.inner_end = self.window_end;
            while self.inner_end - self.inner_start < self.max_len {
                let next = following(self.sentences, self.inner_end, self.len);
                if next <= self.inner_end || next - self.inner_start > self.max_len {
                    break;
                }
                self.inner_end = next;
                self.window_end = next;
            }
        }
        if self.inner_end - self.inner_start > self.max_len {
            // Measured against Elasticsearch 8.15: the word-boundary cut
            // around a match in an over-long sentence is taken from one
            // character past the highlighter's offset.
            let offset = offset + 1;
            if offset > self.max_len && offset - self.max_len > self.inner_start {
                self.inner_start =
                    self.inner_start.max(preceding(self.words, offset - self.max_len));
            }
            let remaining = self.max_len.saturating_sub(offset - self.inner_start);
            if offset + remaining < self.window_end {
                self.inner_end =
                    self.window_end.min(following(self.words, offset + remaining, self.len));
            }
        }
        (self.inner_start, self.inner_end.min(self.len).max(start.min(self.len)))
    }
}

/// A `[start, end)` range of characters.
type Span = (usize, usize);

/// Lucene's `PassageScorer` (BM25 over passages, pivot 87).
fn passage_score(
    matches: &[(usize, usize)],
    p: (usize, usize),
    content: &[char],
    all: &[(usize, usize)],
) -> f32 {
    let (k1, b, pivot) = (1.2f32, 0.75f32, 87f32);
    let key = |s: usize, e: usize| content[s..e].iter().collect::<String>().to_lowercase();
    let content_len = content.len() as f32;
    let mut freq: std::collections::BTreeMap<String, u32> = Default::default();
    for &(s, e) in matches {
        *freq.entry(key(s, e)).or_insert(0) += 1;
    }
    let plen = (p.1 - p.0) as f32;
    let mut score = 0.0;
    for (term, f) in freq {
        let total = all.iter().filter(|&&(s, e)| key(s, e) == term).count() as f32;
        let num_docs = 1.0 + content_len / pivot;
        let doc_freq = 1.0 + total;
        let weight = (k1 + 1.0) * ((1.0 + (num_docs + 0.5) / (doc_freq + 0.5)) as f64).ln() as f32;
        let tf = f as f32 / (f as f32 + k1 * (1.0 - b + b * plen / pivot));
        score += weight * tf;
    }
    score * (1.0 + 1.0 / ((pivot + p.0 as f32) as f64).ln() as f32)
}

/// How the unified highlighter cuts passages.
#[derive(Clone, Copy, PartialEq)]
enum Scanner {
    Sentence,
    Word,
}

/// The tag pair and encoding a field's fragments are written with.
struct Format<'a> {
    pre: &'a str,
    post: &'a str,
    html: bool,
}

/// The fragments for one field's (joined) content, Lucene's way: walk the
/// matches in order, open a passage at each match outside the current
/// one, keep the best `count` by score, then order them. Each value of a
/// multi-valued field (joined by U+2029) is scanned on its own, as
/// Elasticsearch's splitting break iterator does.
fn fragment_text(
    content: &str,
    byte_spans: &[(usize, usize)],
    max_len: usize,
    count: usize,
    by_score: bool,
    scanner: Scanner,
    fmt: &Format,
) -> Vec<String> {
    let chars: Vec<char> = content.chars().collect();
    let mut char_of = vec![0usize; content.len() + 1];
    for (ci, (bi, _)) in content.char_indices().enumerate() {
        char_of[bi] = ci;
    }
    char_of[content.len()] = chars.len();
    let spans: Vec<(usize, usize)> =
        byte_spans.iter().map(|&(s, e)| (char_of[s], char_of[e])).collect();
    // Each value's [start, end) in `chars`.
    let mut segments = Vec::new();
    let mut seg_start = 0;
    for (i, &c) in chars.iter().enumerate() {
        if c == '\u{2029}' {
            segments.push((seg_start, i));
            seg_start = i + 1;
        }
    }
    segments.push((seg_start, chars.len()));
    let mut current: Option<(usize, Vec<usize>, Vec<usize>)> = None;
    let mut state = (0usize, 0usize, 0usize, 0usize);
    let mut passages: Vec<(Span, Vec<Span>)> = Vec::new();
    for &(s, e) in &spans {
        if let Some((p, m)) = passages.last_mut()
            && s < p.1
        {
            m.push((s, e));
            continue;
        }
        let si = segments.iter().position(|&(a, b)| s >= a && s <= b).unwrap_or(0);
        let (a, b) = segments[si];
        if current.as_ref().is_none_or(|c| c.0 != si) {
            let seg = &chars[a..b];
            current = Some((si, sentence_breaks(seg), word_breaks(seg)));
            state = (0, 0, 0, 0);
        }
        let (_, sentences, words) = current.as_ref().unwrap();
        let p = match scanner {
            Scanner::Word => {
                let off = s - a;
                (preceding(words, off + 1), following(words, off, b - a).max(off))
            }
            Scanner::Sentence => {
                let mut bounded = Bounded {
                    sentences,
                    words,
                    len: b - a,
                    max_len,
                    window_start: state.0,
                    window_end: state.1,
                    inner_start: state.2,
                    inner_end: state.3,
                };
                let p = bounded.passage(s - a);
                state = (
                    bounded.window_start,
                    bounded.window_end,
                    bounded.inner_start,
                    bounded.inner_end,
                );
                p
            }
        };
        passages.push(((p.0 + a, p.1 + a), vec![(s, e)]));
    }
    let mut scored: Vec<(f32, Span, Vec<Span>)> =
        passages.into_iter().map(|(p, m)| (passage_score(&m, p, &chars, &spans), p, m)).collect();
    // Best first; among equal scores Lucene's queue keeps the later one.
    scored.sort_by(|a, b| {
        b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal).then(b.1.0.cmp(&a.1.0))
    });
    scored.truncate(count);
    if !by_score {
        scored.sort_by_key(|x| x.1.0);
    }
    scored
        .into_iter()
        .map(|(_, (ps, pe), m)| {
            let mut out = String::new();
            let mut pos = ps;
            let mut i = 0;
            while i < m.len() {
                let (s, mut e) = m[i];
                while i + 1 < m.len() && m[i + 1].0 < e {
                    i += 1;
                    e = e.max(m[i].1);
                }
                i += 1;
                if s < pos {
                    continue;
                }
                let e = e.min(pe).max(s);
                push_chars(&mut out, &chars[pos..s], fmt.html);
                out.push_str(fmt.pre);
                push_chars(&mut out, &chars[s..e], fmt.html);
                out.push_str(fmt.post);
                pos = e;
            }
            if pos < pe {
                push_chars(&mut out, &chars[pos..pe], fmt.html);
            }
            out.trim_matches(|c: char| c.is_whitespace() || c == '\u{2029}').to_string()
        })
        .collect()
}

fn tag(text: &str, spans: &[(usize, usize)], fmt: &Format) -> String {
    let mut out = String::new();
    let mut pos = 0;
    for &(s, e) in spans {
        push_encoded(&mut out, &text[pos..s], fmt.html);
        out.push_str(fmt.pre);
        push_encoded(&mut out, &text[s..e], fmt.html);
        out.push_str(fmt.post);
        pos = e;
    }
    push_encoded(&mut out, &text[pos..], fmt.html);
    out
}

/// The plain highlighter's fragmenters.
#[derive(Clone, Copy, PartialEq)]
enum Fragmenter {
    /// `span` (the default): new fragments every `fragment_size`
    /// characters, never inside a matched phrase.
    Span,
    Simple,
    /// `number_of_fragments: 0`: the whole value.
    Null,
}

/// Lucene's `Highlighter.getBestTextFragments` for one value: (score,
/// fragment number, text) of each fragment.
fn plain_fragments(
    text: &str,
    tokens: &[Token],
    hls: &[&Hl],
    size: usize,
    fragmenter: Fragmenter,
    fmt: &Format,
) -> Vec<(f32, usize, String)> {
    let (term_hit, phrases) = token_matches(tokens, hls);
    let mut hit = term_hit.clone();
    for &(a, b) in &phrases {
        hit[a..=b].iter_mut().for_each(|h| *h = true);
    }
    // Every term of a matched phrase, with the phrase spans it starts.
    let phrase_term = |t: &str| {
        phrases
            .iter()
            .filter(|&&(a, b)| tokens[a..=b].iter().any(|x| x.0 == t))
            .copied()
            .collect::<Vec<_>>()
    };
    let utf16 = |b: usize| text[..b].encode_utf16().count();
    let text_size = utf16(text.len());
    let mut new_text = String::new();
    // (start, end, score) of each fragment in `new_text`.
    let mut frags: Vec<(usize, usize, f32)> = vec![(0, 0, 0.0)];
    let mut found: HashSet<&str> = HashSet::new();
    let mut total = 0f32;
    let mut last_end = 0usize;
    let mut pending: Option<usize> = None;
    let (mut position, mut wait, mut num_frags) = (-1i64, -1i64, 1usize);
    let flush = |g: usize, new_text: &mut String, last_end: &mut usize| {
        let (_, s, e) = &tokens[g];
        if *s > *last_end {
            push_encoded(new_text, &text[*last_end..*s], fmt.html);
        }
        if hit[g] {
            new_text.push_str(fmt.pre);
            push_encoded(new_text, &text[*s..*e], fmt.html);
            new_text.push_str(fmt.post);
        } else {
            push_encoded(new_text, &text[*s..*e], fmt.html);
        }
        *last_end = (*last_end).max(*e);
    };
    for (i, tok) in tokens.iter().enumerate() {
        if let Some(g) = pending.take() {
            flush(g, &mut new_text, &mut last_end);
            let end = utf16(tok.2);
            let new = match fragmenter {
                Fragmenter::Null => false,
                Fragmenter::Simple => {
                    let n = end >= size * num_frags;
                    if n {
                        num_frags += 1;
                    }
                    n
                }
                Fragmenter::Span => {
                    position += 1;
                    let mut blocked = false;
                    if wait <= position {
                        wait = -1;
                    } else if wait != -1 {
                        blocked = true;
                    }
                    if blocked {
                        false
                    } else {
                        if hit[i]
                            && let Some(&(_, b)) =
                                phrase_term(&tok.0).iter().find(|&&(a, _)| a as i64 == position)
                        {
                            wait = b as i64 + 1;
                        }
                        let n =
                            end >= size * num_frags && text_size.saturating_sub(end) >= size / 2;
                        if n {
                            num_frags += 1;
                        }
                        n
                    }
                }
            };
            if new {
                let last = frags.last_mut().unwrap();
                last.1 = new_text.len();
                last.2 = total;
                frags.push((new_text.len(), 0, 0.0));
                found.clear();
                total = 0.0;
            }
        }
        if hit[i] && found.insert(tok.0.as_str()) {
            total += 1.0;
        }
        pending = Some(i);
    }
    if let Some(g) = pending {
        flush(g, &mut new_text, &mut last_end);
    }
    if last_end < text.len() {
        push_encoded(&mut new_text, &text[last_end..], fmt.html);
    }
    let last = frags.last_mut().unwrap();
    last.1 = new_text.len();
    last.2 = total;
    frags
        .into_iter()
        .enumerate()
        .map(|(n, (s, e, sc))| (sc, n, new_text[s..e].to_string()))
        .collect()
}

/// The plain highlighter's `no_match_size` excerpt: up to the end of the
/// last token ending within the size.
fn plain_no_match(text: &str, tokens: &[Token], size: usize) -> Option<String> {
    let utf16 = |b: usize| text[..b].encode_utf16().count();
    let mut end: Option<usize> = None;
    for (_, _, e) in tokens {
        let u = utf16(*e);
        if u >= size {
            if u == size {
                end = Some(*e);
            }
            break;
        }
        end = Some(*e);
    }
    end.filter(|&e| e > 0).map(|e| text[..e].to_string())
}

/// The definition of `field` in `mappings` (multi-fields included).
fn field_def<'a>(mappings: &'a Value, field: &str) -> Option<&'a Value> {
    let segs: Vec<&str> = field.split('.').collect();
    let mut props = mappings.get("properties");
    for (i, seg) in segs.iter().enumerate() {
        let node = props?.get(*seg)?;
        if i + 1 == segs.len() {
            return Some(node);
        }
        if i + 2 == segs.len()
            && let Some(sub) = node.get("fields").and_then(|f| f.get(segs[i + 1]))
        {
            return Some(sub);
        }
        props = node.get("properties");
    }
    None
}

/// Where a hit is being highlighted: what the index settings and the
/// search request decide beyond the `highlight` section itself.
pub struct Context<'a> {
    pub index: &'a str,
    /// The document's number in its index (error messages name it).
    pub doc: usize,
    /// The index's settings (`index.highlight.*`).
    pub settings: &'a Value,
    /// A `nested` or `knn` part of the search turns weighted matches off.
    pub weighted: bool,
}

impl Context<'_> {
    fn setting(&self, key: &str) -> Option<&Value> {
        let h = self.settings.get("index")?.get("highlight")?;
        let mut node = h;
        for k in key.split('.') {
            node = node.get(k)?;
        }
        Some(node)
    }
}

fn num(v: Option<&Value>) -> Option<i64> {
    match v? {
        Value::Number(n) => n.as_i64(),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

fn tags_of(v: Option<&Value>) -> Option<Vec<String>> {
    v.and_then(Value::as_array)
        .map(|a| a.iter().filter_map(Value::as_str).map(str::to_string).collect())
}

/// Request-level checks Elasticsearch makes while parsing `highlight`.
pub fn validate(spec: &Value) -> Result<(), EsError> {
    let parse = |field: &str, cause: &str| {
        EsError::new(
            400,
            "x_content_parse_exception",
            &format!("[highlight] failed to parse field [{field}]"),
        )
        .caused_by("illegal_argument_exception", cause)
    };
    let mut levels = vec![spec];
    match spec.get("fields") {
        Some(Value::Object(o)) => levels.extend(o.values()),
        Some(Value::Array(a)) => {
            levels.extend(a.iter().filter_map(Value::as_object).flat_map(|o| o.values()))
        }
        _ => {}
    }
    for l in levels {
        let (pre, post) = (tags_of(l.get("pre_tags")), tags_of(l.get("post_tags")));
        if pre.as_ref().is_some_and(Vec::is_empty) || post.as_ref().is_some_and(Vec::is_empty) {
            return Err(EsError::parsing("pre_tags or post_tags must not be empty"));
        }
        if pre.is_some() && post.is_none() && spec.get("post_tags").is_none() {
            return Err(EsError::parsing("pre_tags are set but post_tags are not set"));
        }
        if let Some(m) = l.get("max_analyzed_offset")
            && num(Some(m)).is_none_or(|n| n < 1)
        {
            return Err(parse(
                "max_analyzed_offset",
                "[max_analyzed_offset] must be a positive integer",
            ));
        }
        if let Some(b) = l.get("boundary_scanner")
            && !matches!(b.as_str(), Some("chars" | "word" | "sentence"))
        {
            return Err(parse(
                "boundary_scanner",
                &format!(
                    "No enum constant org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder.BoundaryScannerType.{}",
                    b.as_str().unwrap_or("").to_uppercase()
                ),
            ));
        }
    }
    if let Some(t) = spec.get("tags_schema")
        && !matches!(t.as_str(), Some("styled" | "default"))
    {
        return Err(parse(
            "tags_schema",
            &format!("Unknown tag schema [{}]", t.as_str().unwrap_or("")),
        ));
    }
    Ok(())
}

const STYLED: &[&str] = &[
    "<em class=\"hlt1\">",
    "<em class=\"hlt2\">",
    "<em class=\"hlt3\">",
    "<em class=\"hlt4\">",
    "<em class=\"hlt5\">",
    "<em class=\"hlt6\">",
    "<em class=\"hlt7\">",
    "<em class=\"hlt8\">",
    "<em class=\"hlt9\">",
    "<em class=\"hlt10\">",
];

fn shard_err(reason: &str) -> EsError {
    EsError::shard_failure("illegal_argument_exception", reason)
}

/// A hit's `highlight` object, or `None` when nothing in it matched.
pub fn highlight(
    spec: &Value,
    query: &Value,
    mappings: &Value,
    source: &Value,
    ctx: &Context,
) -> Result<Option<Value>, EsError> {
    let mut out = Map::new();
    let fields: Vec<(String, Value)> = match spec.get("fields") {
        Some(Value::Object(o)) => o.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
        Some(Value::Array(a)) => a
            .iter()
            .filter_map(Value::as_object)
            .flat_map(|o| o.iter().map(|(k, v)| (k.clone(), v.clone())))
            .collect(),
        _ => Vec::new(),
    };
    let index_limit = num(ctx.setting("max_analyzed_offset")).unwrap_or(1_000_000).max(0) as usize;
    let weight_setting = ctx
        .setting("weight_matches_mode.enabled")
        .or_else(|| ctx.setting("weight_matches_mode").and_then(|w| w.get("enabled")))
        .is_none_or(|v| v != "false" && v != &json!(false));
    for (pattern, opts) in fields {
        let get = |k: &str| opts.get(k).or_else(|| spec.get(k));
        let ty = get("type").and_then(Value::as_str).unwrap_or("unified").to_string();
        let require = get("require_field_match").and_then(Value::as_bool).unwrap_or(true);
        let size = get("fragment_size").and_then(Value::as_u64).unwrap_or(100) as usize;
        let count = get("number_of_fragments").and_then(Value::as_u64).unwrap_or(5) as usize;
        let no_match = get("no_match_size").and_then(Value::as_u64).unwrap_or(0) as usize;
        let by_score = get("order").and_then(Value::as_str) == Some("score");
        let html = get("encoder").and_then(Value::as_str) == Some("html");
        let request_limit = num(get("max_analyzed_offset")).map(|n| n.max(0) as usize);
        let styled = spec.get("tags_schema").and_then(Value::as_str) == Some("styled");
        let pre = tags_of(get("pre_tags"))
            .and_then(|t| t.into_iter().next())
            .unwrap_or_else(|| if styled { STYLED[0].to_string() } else { "<em>".to_string() });
        let post = tags_of(get("post_tags"))
            .and_then(|t| t.into_iter().next())
            .unwrap_or_else(|| "</em>".to_string());
        let fmt = Format { pre: &pre, post: &post, html };
        let matched_fields: Vec<String> = opts
            .get("matched_fields")
            .and_then(Value::as_array)
            .map(|a| a.iter().filter_map(Value::as_str).map(str::to_string).collect())
            .unwrap_or_default();
        let hl_query = opts.get("highlight_query").or_else(|| spec.get("highlight_query"));
        let mut terms = Vec::new();
        collect(hl_query.unwrap_or(query), mappings, &mut terms);
        for field in expand_field_pattern(mappings, &pattern) {
            if !matches!(ty.as_str(), "unified" | "plain" | "fvh") {
                return Err(shard_err(&format!(
                    "unknown highlighter type [{ty}] for the field [{field}]"
                )));
            }
            if !matched_fields.is_empty() && !require {
                return Err(shard_err(
                    "Matched fields are not supported when [require_field_match] is set to [false]",
                ));
            }
            let (path, fty) = resolve_field(mappings, &field);
            let keyword = matches!(fty.as_deref(), Some("keyword") | Some("constant_keyword"));
            if !matches!(
                fty.as_deref(),
                None | Some("text")
                    | Some("match_only_text")
                    | Some("keyword")
                    | Some("constant_keyword")
            ) {
                continue;
            }
            let def = field_def(mappings, &field);
            let def_str = |k: &str| def.and_then(|d| d.get(k)).and_then(Value::as_str);
            if ty == "fvh" && def_str("term_vector") != Some("with_positions_offsets") {
                return Err(shard_err(&format!(
                    "the field [{field}] should be indexed with term vector with position offsets to be used with fast vector highlighter"
                )));
            }
            let scanner = match get("boundary_scanner").and_then(Value::as_str) {
                Some("word") => Scanner::Word,
                Some("chars") if ty == "unified" => {
                    return Err(shard_err("Invalid boundary scanner type: chars"));
                }
                _ => Scanner::Sentence,
            };
            let fragmenter = match (count, get("fragmenter").and_then(Value::as_str)) {
                (0, _) => Fragmenter::Null,
                (_, None | Some("span")) => Fragmenter::Span,
                (_, Some("simple")) => Fragmenter::Simple,
                (_, Some(other)) if ty == "plain" => {
                    return Err(shard_err(&format!(
                        "unknown fragmenter option [{other}] for the field [{field}]"
                    )));
                }
                _ => Fragmenter::Span,
            };
            let hls: Vec<&Hl> = terms
                .iter()
                .filter(|(f, _)| !require || f == &field || matched_fields.contains(f))
                .map(|(_, h)| h)
                .collect();
            let ignore_above = def.and_then(|d| d.get("ignore_above")).and_then(Value::as_u64);
            let values: Vec<String> = raw_values(source, &path)
                .into_iter()
                .filter_map(|v| match v {
                    Value::String(s) => Some(s.clone()),
                    Value::Number(n) => Some(n.to_string()),
                    _ => None,
                })
                .filter(|s| !keyword || ignore_above.is_none_or(|n| s.chars().count() as u64 <= n))
                .collect();
            if values.is_empty() {
                continue;
            }
            let synthetic =
                mappings.get("_source").and_then(|s| s.get("mode")).and_then(Value::as_str)
                    == Some("synthetic");
            if ty == "fvh" && synthetic && values.len() > 1 {
                return Err(shard_err(&format!(
                    "The fast vector highlighter doesn't support loading multi-valued fields from _source in index [{}] because _source can reorder field values",
                    ctx.index
                )));
            }
            // Analysis stops at `max_analyzed_offset`: past the index
            // limit it's an error (unless the unified highlighter can read
            // stored offsets), below it a request limit truncates.
            let has_offsets = def_str("index_options") == Some("offsets")
                || def_str("term_vector").is_some_and(|t| t.contains("offsets"));
            let limit = match request_limit {
                Some(r) if r < index_limit => Some(r),
                _ => {
                    let len = if ty == "plain" {
                        values.iter().map(|v| v.encode_utf16().count()).max().unwrap_or(0)
                    } else {
                        values.iter().map(|v| v.encode_utf16().count() + 1).sum::<usize>() - 1
                    };
                    if len > index_limit && !(ty != "plain" && has_offsets) {
                        return Err(shard_err(&format!(
                            "The length [{len}] of field [{field}] in doc[{}]/index[{}] exceeds the [index.highlight.max_analyzed_offset] limit [{index_limit}]. To avoid this error, set the query parameter [max_analyzed_offset] to a value less than index setting [{index_limit}] and this will tolerate long field values by truncating them.",
                            ctx.doc, ctx.index
                        )));
                    }
                    request_limit
                }
            };
            let limited = |text: &str, toks: Vec<Token>| -> Vec<Token> {
                match limit {
                    Some(l) => toks
                        .into_iter()
                        .filter(|t| text[..t.1].encode_utf16().count() <= l)
                        .collect(),
                    None => toks,
                }
            };
            let mut frags: Vec<String> = Vec::new();
            if ty == "plain" {
                let mut all: Vec<(f32, usize, String)> = Vec::new();
                for text in &values {
                    let toks = limited(text, tokenize(mappings, &field, text, keyword));
                    let mut fs = plain_fragments(text, &toks, &hls, size, fragmenter, &fmt);
                    fs.sort_by(|a, b| {
                        b.0.partial_cmp(&a.0)
                            .unwrap_or(std::cmp::Ordering::Equal)
                            .then(a.1.cmp(&b.1))
                    });
                    fs.truncate(count.max(1));
                    // The best fragments, back in text order.
                    fs.sort_by_key(|f| f.1);
                    all.extend(fs.into_iter().filter(|f| f.0 > 0.0));
                }
                if by_score {
                    all.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
                }
                if !(count == 0 && values.len() > 1) {
                    all.truncate(count.max(1));
                }
                frags = all.into_iter().map(|f| f.2).collect();
                if frags.is_empty()
                    && no_match > 0
                    && let Some(e) = plain_no_match(
                        &values[0],
                        &tokenize(mappings, &field, &values[0], keyword),
                        no_match,
                    )
                {
                    let mut s = String::new();
                    push_encoded(&mut s, &e, html);
                    frags.push(s);
                }
            } else {
                // The fast vector highlighter always tags a phrase whole.
                let weighted = ty == "fvh" || (ctx.weighted && weight_setting);
                if count == 0 || keyword {
                    for text in &values {
                        let sp = field_spans(
                            mappings,
                            &field,
                            text,
                            keyword,
                            &terms,
                            &matched_fields,
                            &hls,
                            weighted,
                            &limited,
                        );
                        if !sp.is_empty() {
                            frags.push(tag(text, &sp, &fmt));
                        }
                    }
                } else {
                    // Values are highlighted as one text, separated so no
                    // passage spans two of them.
                    let content = values.join("\u{2029}");
                    let sp = field_spans(
                        mappings,
                        &field,
                        &content,
                        false,
                        &terms,
                        &matched_fields,
                        &hls,
                        weighted,
                        &limited,
                    );
                    if !sp.is_empty() {
                        frags = fragment_text(&content, &sp, size, count, by_score, scanner, &fmt);
                    }
                }
                if frags.is_empty() && no_match > 0 {
                    // `no_match_size` characters, extended to the end of a word.
                    let chars: Vec<char> = values[0].chars().collect();
                    let end = if no_match >= chars.len() {
                        chars.len()
                    } else {
                        following(&word_breaks(&chars), no_match, chars.len())
                    };
                    let mut s = String::new();
                    push_chars(&mut s, &chars[..end], html);
                    frags.push(s.trim().to_string());
                }
            }
            if !frags.is_empty() {
                out.insert(field, json!(frags));
            }
        }
    }
    Ok((!out.is_empty()).then_some(Value::Object(out)))
}
