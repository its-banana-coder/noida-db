//! `highlight`: the unified highlighter's output for the common cases —
//! the query's terms (per field unless `require_field_match: false`)
//! wrapped in `pre_tags`/`post_tags` (default `<em>`), a matched phrase as
//! one span, keyword fields highlighted whole, and the text cut into
//! sentence fragments around the matches (`fragment_size`,
//! `number_of_fragments`, 0 for the whole value).

use serde_json::{Map, Value, json};

use super::analysis;
use super::query_string;
use super::search::{raw_values, resolve_field};

#[derive(Debug, Clone)]
enum Hl {
    Term(String),
    Phrase(Vec<String>),
    Prefix(String),
    Wildcard(String),
    Fuzzy(String, usize),
}

fn analyze(mappings: &Value, field: &str, text: &str) -> Vec<String> {
    match resolve_field(mappings, field).1.as_deref() {
        None | Some("text") | Some("match_only_text") => analysis::standard(text),
        Some(_) => vec![text.to_string()],
    }
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

/// The highlightable terms in `query`, tagged with the field they target.
fn collect(query: &Value, mappings: &Value, out: &mut Vec<(String, Hl)>) {
    let Some(obj) = query.as_object() else { return };
    for (kind, body) in obj {
        match kind.as_str() {
            "match" | "match_phrase" | "match_phrase_prefix" | "match_bool_prefix" => {
                let Some((field, spec)) = field_spec(body) else { continue };
                let text = match spec {
                    Value::Object(o) => text_of(o.get("query")),
                    other => text_of(Some(other)),
                };
                let terms = analyze(mappings, field, &text);
                if kind == "match_phrase" && terms.len() > 1 {
                    out.push((field.clone(), Hl::Phrase(terms)));
                } else {
                    let last = terms.len().saturating_sub(1);
                    for (i, t) in terms.into_iter().enumerate() {
                        let prefix = kind.ends_with("prefix") && i == last;
                        out.push((field.clone(), if prefix { Hl::Prefix(t) } else { Hl::Term(t) }));
                    }
                }
            }
            "multi_match" => {
                let text = text_of(body.get("query"));
                let phrase = body.get("type").and_then(Value::as_str) == Some("phrase");
                for f in body.get("fields").and_then(Value::as_array).cloned().unwrap_or_default() {
                    let Some(f) = f.as_str() else { continue };
                    let f = f.split('^').next().unwrap_or(f).to_string();
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
                let v = match spec {
                    Value::Object(o) => text_of(o.get("value")),
                    other => text_of(Some(other)),
                };
                let h = match kind.as_str() {
                    "prefix" => Hl::Prefix(v),
                    "fuzzy" => {
                        let n = v.chars().count();
                        Hl::Fuzzy(
                            v,
                            if n < 3 {
                                0
                            } else if n < 6 {
                                1
                            } else {
                                2
                            },
                        )
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

fn edit_distance(a: &str, b: &str) -> usize {
    let (a, b): (Vec<char>, Vec<char>) = (a.chars().collect(), b.chars().collect());
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    for i in 1..=a.len() {
        let mut cur = vec![i; b.len() + 1];
        for j in 1..=b.len() {
            let cost = usize::from(a[i - 1] != b[j - 1]);
            cur[j] = (prev[j] + 1).min(cur[j - 1] + 1).min(prev[j - 1] + cost);
        }
        prev = cur;
    }
    prev[b.len()]
}

/// Byte spans of `text` to tag, each with the matched text (lowercased,
/// the passage scorer's notion of "the same term").
fn spans(text: &str, keyword: bool, hls: &[&Hl]) -> Vec<(usize, usize)> {
    let tokens: Vec<(String, usize, usize)> = if keyword {
        vec![(text.to_string(), 0, text.len())]
    } else {
        analysis::standard_with_offsets(text)
    };
    let mut out: Vec<(usize, usize)> = Vec::new();
    for (i, (tok, s, e)) in tokens.iter().enumerate() {
        for h in hls {
            let hit = match h {
                Hl::Term(t) => tok == t,
                Hl::Prefix(p) => tok.starts_with(p.as_str()),
                Hl::Wildcard(w) => glob(w, tok),
                Hl::Fuzzy(t, n) => edit_distance(tok, t) <= *n,
                Hl::Phrase(terms) => {
                    if tokens.len() >= i + terms.len()
                        && terms.iter().enumerate().all(|(k, t)| &tokens[i + k].0 == t)
                    {
                        out.push((*s, tokens[i + terms.len() - 1].2));
                    }
                    false
                }
            };
            if hit {
                out.push((*s, *e));
            }
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

/// The fragments for one field's (joined) content, Lucene's way: walk the
/// matches in order, open a passage at each match outside the current
/// one, keep the best `count` by score, then order them.
fn fragment_text(
    content: &str,
    byte_spans: &[(usize, usize)],
    max_len: usize,
    count: usize,
    by_score: bool,
    pre: &str,
    post: &str,
) -> Vec<String> {
    let chars: Vec<char> = content.chars().collect();
    let mut char_of = vec![0usize; content.len() + 1];
    for (ci, (bi, _)) in content.char_indices().enumerate() {
        char_of[bi] = ci;
    }
    char_of[content.len()] = chars.len();
    let spans: Vec<(usize, usize)> =
        byte_spans.iter().map(|&(s, e)| (char_of[s], char_of[e])).collect();
    let sentences = sentence_breaks(&chars);
    let words = word_breaks(&chars);
    let mut bounded = Bounded {
        sentences: &sentences,
        words: &words,
        len: chars.len(),
        max_len,
        window_start: 0,
        window_end: 0,
        inner_start: 0,
        inner_end: 0,
    };
    let mut passages: Vec<(Span, Vec<Span>)> = Vec::new();
    for &(s, e) in &spans {
        match passages.last_mut() {
            Some((p, m)) if s < p.1 => m.push((s, e)),
            _ => {
                let p = bounded.passage(s);
                passages.push((p, vec![(s, e)]));
            }
        }
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
            for (s, e) in m {
                let e = e.min(pe.max(e));
                if s < pos {
                    continue;
                }
                out.extend(&chars[pos..s]);
                out.push_str(pre);
                out.extend(&chars[s..e]);
                out.push_str(post);
                pos = e;
            }
            if pos < pe {
                out.extend(&chars[pos..pe]);
            }
            out.trim_matches(|c: char| c.is_whitespace() || c == '\u{2029}').to_string()
        })
        .collect()
}

fn tag(text: &str, spans: &[(usize, usize)], pre: &str, post: &str) -> String {
    let mut out = String::new();
    let mut pos = 0;
    for &(s, e) in spans {
        out.push_str(&text[pos..s]);
        out.push_str(pre);
        out.push_str(&text[s..e]);
        out.push_str(post);
        pos = e;
    }
    out.push_str(&text[pos..]);
    out
}

fn field_names(pattern: &str, mappings: &Value) -> Vec<String> {
    if !pattern.contains('*') {
        return vec![pattern.to_string()];
    }
    let mut all = Vec::new();
    fn walk(props: Option<&Value>, prefix: &str, out: &mut Vec<String>) {
        let Some(obj) = props.and_then(Value::as_object) else { return };
        for (k, v) in obj {
            let full = if prefix.is_empty() { k.clone() } else { format!("{prefix}.{k}") };
            match v.get("properties") {
                Some(p) => walk(Some(p), &full, out),
                None => out.push(full),
            }
        }
    }
    walk(mappings.get("properties"), "", &mut all);
    all.into_iter().filter(|f| glob(pattern, f)).collect()
}

/// A hit's `highlight` object, or `None` when nothing in it matched.
pub fn highlight(spec: &Value, query: &Value, mappings: &Value, source: &Value) -> Option<Value> {
    let q = spec.get("highlight_query").unwrap_or(query);
    let mut terms = Vec::new();
    collect(q, mappings, &mut terms);
    let tags = |key: &str, d: &str| -> String {
        spec.get(key)
            .and_then(Value::as_array)
            .and_then(|a| a.first())
            .and_then(Value::as_str)
            .unwrap_or(d)
            .to_string()
    };
    let (pre, post) = (tags("pre_tags", "<em>"), tags("post_tags", "</em>"));
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
    for (pattern, opts) in fields {
        let get = |k: &str| opts.get(k).or_else(|| spec.get(k));
        let require = get("require_field_match").and_then(Value::as_bool).unwrap_or(true);
        let size = get("fragment_size").and_then(Value::as_u64).unwrap_or(100) as usize;
        let count = get("number_of_fragments").and_then(Value::as_u64).unwrap_or(5) as usize;
        let no_match = get("no_match_size").and_then(Value::as_u64).unwrap_or(0) as usize;
        let (fpre, fpost) = (
            opts.get("pre_tags")
                .and_then(|a| a.get(0))
                .and_then(Value::as_str)
                .unwrap_or(&pre)
                .to_string(),
            opts.get("post_tags")
                .and_then(|a| a.get(0))
                .and_then(Value::as_str)
                .unwrap_or(&post)
                .to_string(),
        );
        for field in field_names(&pattern, mappings) {
            let (path, ty) = resolve_field(mappings, &field);
            let keyword = matches!(ty.as_deref(), Some("keyword") | Some("constant_keyword"));
            if !matches!(
                ty.as_deref(),
                None | Some("text")
                    | Some("match_only_text")
                    | Some("keyword")
                    | Some("constant_keyword")
            ) {
                continue;
            }
            let hls: Vec<&Hl> = terms
                .iter()
                .filter(|(f, _)| !require || f == &field || (field.contains('*')))
                .map(|(_, h)| h)
                .collect();
            let values: Vec<String> = raw_values(source, &path)
                .into_iter()
                .filter_map(|v| match v {
                    Value::String(s) => Some(s.clone()),
                    Value::Number(n) => Some(n.to_string()),
                    _ => None,
                })
                .collect();
            let by_score = get("order").and_then(Value::as_str) == Some("score");
            let mut frags: Vec<String> = Vec::new();
            if count == 0 || keyword {
                for text in &values {
                    let sp = spans(text, keyword, &hls);
                    if !sp.is_empty() {
                        frags.push(tag(text, &sp, &fpre, &fpost));
                    }
                }
            } else {
                // Values are highlighted as one text, separated so no
                // passage spans two of them.
                let content = values.join("\u{2029}");
                let sp = spans(&content, false, &hls);
                if !sp.is_empty() {
                    frags = fragment_text(&content, &sp, size, count, by_score, &fpre, &fpost);
                }
            }
            if frags.is_empty()
                && no_match > 0
                && let Some(text) = values.first()
            {
                // `no_match_size` characters, extended to the end of a word.
                let chars: Vec<char> = text.chars().collect();
                let end = if no_match >= chars.len() {
                    chars.len()
                } else {
                    following(&word_breaks(&chars), no_match - 1, chars.len())
                };
                frags.push(chars[..end].iter().collect::<String>().trim().to_string());
            }
            if !frags.is_empty() {
                out.insert(field, json!(frags));
            }
        }
    }
    (!out.is_empty()).then_some(Value::Object(out))
}
