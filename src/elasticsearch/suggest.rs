//! `suggest`: the term, phrase and completion suggesters, after Lucene's
//! `DirectSpellChecker` (term), Elasticsearch's noisy-channel phrase
//! suggester (phrase, unigram language model since noida has no shingle
//! fields), and the `completion` field's prefix/fuzzy/regex lookups with
//! category and geo contexts. Indexes here are tiny, so every lookup is a
//! scan over the refreshed documents rather than an FST.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Map, Value, json};

use super::analysis;
use super::search::{CommittedDoc, EsError, eval, raw_values, resolve_field, tokens_for};

fn bad(reason: &str) -> EsError {
    EsError::shard_failure("illegal_argument_exception", reason)
}

fn parse_err(reason: &str) -> EsError {
    EsError::new(400, "parse_exception", reason)
}

fn num(v: Option<&Value>) -> Option<f64> {
    match v? {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

fn int(o: &Map<String, Value>, key: &str, default: usize) -> usize {
    num(o.get(key)).map_or(default, |n| n.max(0.0) as usize)
}

fn float(o: &Map<String, Value>, key: &str, default: f64) -> f64 {
    num(o.get(key)).unwrap_or(default)
}

fn text_of(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

fn utf16_len(s: &str) -> usize {
    s.encode_utf16().count()
}

/// Tokens of `text` as `analyzer` produces them, each with its UTF-16
/// offset and length in `text` (what an entry's `offset`/`length` report).
fn analyze_offsets(analyzer: &str, text: &str) -> Vec<(String, usize, usize)> {
    let spans: Vec<(String, usize, usize)> = match analyzer {
        "keyword" => {
            if text.is_empty() {
                vec![]
            } else {
                vec![(text.to_string(), 0, text.len())]
            }
        }
        name => analysis::with_byte_offsets(text, analysis::analyzer(name).tokens(text)),
    };
    spans.into_iter().map(|(t, s, e)| (t, utf16_len(&text[..s]), utf16_len(&text[s..e]))).collect()
}

fn check_analyzer(name: &str) -> Result<(), EsError> {
    if analysis::analyzer_exists(name) {
        Ok(())
    } else {
        Err(bad(&format!("analyzer [{name}] doesn't exist")))
    }
}

/// The analyzer a field's query text goes through: the field's search
/// analyzer for text fields, the whole text for everything else.
fn field_analyzer(mappings: &Value, field: &str) -> String {
    match resolve_field(mappings, field).1.as_deref() {
        None | Some("text") | Some("match_only_text") => {
            analysis::field_analyzer_name(mappings, field, analysis::Mode::Search)
        }
        Some(_) => "keyword".to_string(),
    }
}

// ---------------------------------------------------------------------------
// Term statistics and Lucene's DirectSpellChecker.

/// A field's terms with (doc freq, total term freq), as one segment.
struct Terms {
    stats: BTreeMap<String, (u64, u64)>,
    max_doc: u64,
    sum_ttf: u64,
}

impl Terms {
    fn of(mappings: &Value, docs: &[CommittedDoc], field: &str) -> Terms {
        let mut stats: BTreeMap<String, (u64, u64)> = BTreeMap::new();
        for d in docs {
            let toks = tokens_for(mappings, d.full(), field);
            let mut seen = BTreeSet::new();
            for t in toks {
                let e = stats.entry(t.clone()).or_insert((0, 0));
                e.1 += 1;
                if seen.insert(t) {
                    e.0 += 1;
                }
            }
        }
        let sum_ttf = stats.values().map(|s| s.1).sum();
        Terms { stats, max_doc: docs.len() as u64, sum_ttf }
    }

    fn df(&self, t: &str) -> u64 {
        self.stats.get(t).map_or(0, |s| s.0)
    }

    fn ttf(&self, t: &str) -> u64 {
        self.stats.get(t).map_or(0, |s| s.1)
    }
}

/// Optimal-string-alignment distance (Levenshtein with adjacent
/// transpositions), what Lucene's Levenshtein automata accept.
fn osa(a: &[char], b: &[char]) -> usize {
    let (n, m) = (a.len(), b.len());
    let mut d = vec![vec![0usize; m + 1]; n + 1];
    for (i, row) in d.iter_mut().enumerate() {
        row[0] = i;
    }
    for (j, v) in d[0].iter_mut().enumerate() {
        *v = j;
    }
    for i in 1..=n {
        for j in 1..=m {
            let cost = usize::from(a[i - 1] != b[j - 1]);
            let mut v = (d[i - 1][j] + 1).min(d[i][j - 1] + 1).min(d[i - 1][j - 1] + cost);
            if i > 1 && j > 1 && a[i - 1] == b[j - 2] && a[i - 2] == b[j - 1] {
                v = v.min(d[i - 2][j - 2] + 1);
            }
            d[i][j] = v;
        }
    }
    d[n][m]
}

fn levenshtein(a: &[char], b: &[char]) -> usize {
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

/// Lucene's `JaroWinklerDistance` (threshold 0.7).
fn jaro_winkler(s1: &[char], s2: &[char]) -> f32 {
    let (max, min) = if s1.len() > s2.len() { (s1, s2) } else { (s2, s1) };
    let range = (max.len() / 2).saturating_sub(1);
    let mut idx = vec![None; min.len()];
    let mut flags = vec![false; max.len()];
    let mut matches = 0;
    for (mi, &c) in min.iter().enumerate() {
        let lo = mi.saturating_sub(range);
        let hi = (mi + range + 1).min(max.len());
        for xi in lo..hi {
            if !flags[xi] && c == max[xi] {
                idx[mi] = Some(xi);
                flags[xi] = true;
                matches += 1;
                break;
            }
        }
    }
    let ms1: Vec<char> =
        min.iter().zip(&idx).filter(|(_, i)| i.is_some()).map(|(c, _)| *c).collect();
    let ms2: Vec<char> = max.iter().zip(&flags).filter(|(_, f)| **f).map(|(c, _)| *c).collect();
    let transpositions = ms1.iter().zip(&ms2).filter(|(a, b)| a != b).count() / 2;
    let prefix = s1.iter().zip(s2).take(min.len()).take_while(|(a, b)| a == b).count();
    let m = matches as f32;
    if m == 0.0 {
        return 0.0;
    }
    let j = (m / s1.len() as f32 + m / s2.len() as f32 + (m - transpositions as f32) / m) / 3.0;
    if j < 0.7 { j } else { j + (0.1f32).min(1.0 / max.len() as f32) * prefix as f32 * (1.0 - j) }
}

/// Lucene's `NGramDistance` with n = 2.
fn ngram_distance(source: &[char], target: &[char]) -> f32 {
    let n = 2;
    let (sl, tl) = (source.len(), target.len());
    if sl == 0 || tl == 0 {
        return if sl == tl { 1.0 } else { 0.0 };
    }
    if sl < n || tl < n {
        let cost = source.iter().zip(target).filter(|(a, b)| a == b).count();
        return cost as f32 / sl.max(tl) as f32;
    }
    let mut sa = vec!['\0'; sl + n - 1];
    for (i, c) in sa.iter_mut().enumerate().skip(n - 1) {
        *c = source[i - n + 1];
    }
    let mut p: Vec<f32> = (0..=sl).map(|i| i as f32).collect();
    let mut d = vec![0f32; sl + 1];
    for j in 1..=tl {
        let t_j: Vec<char> = if j < n {
            let mut v = vec!['\0'; n - j];
            v.extend(&target[..j]);
            v
        } else {
            target[j - n..j].to_vec()
        };
        d[0] = j as f32;
        for i in 1..=sl {
            let mut cost = 0;
            let mut tn = n;
            for ni in 0..n {
                if sa[i - 1 + ni] != t_j[ni] {
                    cost += 1;
                } else if sa[i - 1 + ni] == '\0' {
                    tn -= 1;
                }
            }
            let ec = cost as f32 / tn as f32;
            d[i] = (d[i - 1] + 1.0).min(p[i] + 1.0).min(p[i - 1] + ec);
        }
        std::mem::swap(&mut p, &mut d);
    }
    1.0 - p[sl] / tl.max(sl) as f32
}

#[derive(Clone, Copy, PartialEq)]
enum Mode {
    Missing,
    Popular,
    Always,
}

#[derive(Clone, Copy, PartialEq)]
enum Distance {
    Internal,
    Damerau,
    Levenshtein,
    JaroWinkler,
    NGram,
}

fn distance(kind: Distance, a: &[char], b: &[char]) -> f32 {
    match kind {
        Distance::Internal | Distance::Damerau => {
            let min = a.len().min(b.len());
            if min == 0 { 0.0 } else { 1.0 - osa(a, b) as f32 / min as f32 }
        }
        Distance::Levenshtein => {
            let max = a.len().max(b.len());
            if max == 0 { 1.0 } else { 1.0 - levenshtein(a, b) as f32 / max as f32 }
        }
        Distance::JaroWinkler => jaro_winkler(a, b),
        Distance::NGram => ngram_distance(a, b),
    }
}

/// The direct spellchecker's options (term suggester / direct generator).
#[derive(Clone)]
struct Spell {
    field: String,
    size: usize,
    max_edits: usize,
    prefix_length: usize,
    min_word_length: usize,
    max_inspections: usize,
    accuracy: f32,
    max_term_freq: f32,
    min_doc_freq: f32,
    mode: Mode,
    by_freq: bool,
    distance: Distance,
}

impl Spell {
    fn parse(o: &Map<String, Value>, default_size: usize) -> Result<Spell, EsError> {
        let field = o
            .get("field")
            .and_then(Value::as_str)
            .ok_or_else(|| parse_err("the required field option [field] is missing"))?
            .to_string();
        let mode = match o.get("suggest_mode").and_then(Value::as_str).unwrap_or("missing") {
            "missing" => Mode::Missing,
            "popular" => Mode::Popular,
            "always" => Mode::Always,
            other => {
                return Err(EsError::new(
                    400,
                    "illegal_argument_exception",
                    &format!("Illegal suggest mode {}", other.to_uppercase()),
                ));
            }
        };
        let by_freq = match o.get("sort").and_then(Value::as_str).unwrap_or("score") {
            "score" => false,
            "frequency" => true,
            other => {
                return Err(EsError::new(
                    400,
                    "illegal_argument_exception",
                    &format!("Illegal suggest sort {other}"),
                ));
            }
        };
        let distance = match o.get("string_distance").and_then(Value::as_str).unwrap_or("internal")
        {
            "internal" => Distance::Internal,
            "damerau_levenshtein" => Distance::Damerau,
            "levenshtein" => Distance::Levenshtein,
            "jaro_winkler" => Distance::JaroWinkler,
            "ngram" => Distance::NGram,
            other => {
                return Err(EsError::new(
                    400,
                    "illegal_argument_exception",
                    &format!("Illegal distance option {other}"),
                ));
            }
        };
        let max_edits = int(o, "max_edits", 2);
        if !(1..=2).contains(&max_edits) {
            return Err(EsError::new(
                400,
                "illegal_argument_exception",
                &format!("Illegal max_edits value {max_edits}"),
            ));
        }
        Ok(Spell {
            field,
            size: int(o, "size", default_size),
            max_edits,
            prefix_length: int(o, "prefix_length", int(o, "prefix_len", 1)),
            min_word_length: int(o, "min_word_length", int(o, "min_word_len", 4)),
            max_inspections: int(o, "max_inspections", 5),
            accuracy: float(o, "accuracy", 0.5) as f32,
            max_term_freq: float(o, "max_term_freq", 0.01) as f32,
            min_doc_freq: float(o, "min_doc_freq", 0.0) as f32,
            mode,
            by_freq,
            distance,
        })
    }

    /// `DirectSpellChecker.suggestSimilar`: (term, score, doc freq), best
    /// first, at most `num`.
    fn similar(
        &self,
        term: &str,
        num: usize,
        terms: &Terms,
        threshold: Option<f32>,
    ) -> Vec<(String, f32, u64)> {
        let q: Vec<char> = term.chars().collect();
        if q.len() < self.min_word_length {
            return vec![];
        }
        let df = terms.df(term);
        if self.mode == Mode::Missing && df > 0 {
            return vec![];
        }
        let max_doc = terms.max_doc as f32;
        if self.max_term_freq >= 1.0 {
            if df as f32 > self.max_term_freq {
                return vec![];
            }
        } else if df as f32 > (self.max_term_freq * max_doc).ceil() {
            return vec![];
        }
        let mut floor: i64 = if self.mode == Mode::Popular { df as i64 } else { 0 };
        let threshold = threshold.unwrap_or(self.min_doc_freq);
        if threshold >= 1.0 {
            floor = floor.max(threshold as i64);
        } else if threshold > 0.0 {
            floor = floor.max((threshold * max_doc) as i64 - 1);
        }
        let inspections = num * self.max_inspections;
        // Lucene tries one edit first, then the full distance.
        let mut found = self.pass(&q, 1, self.prefix_length, inspections, floor, terms);
        if self.max_edits > 1 && found.len() < inspections {
            let more = self.pass(
                &q,
                self.max_edits,
                self.prefix_length.max(self.max_edits - 1),
                inspections,
                floor,
                terms,
            );
            for m in more {
                if !found.iter().any(|f| f.0 == m.0) {
                    found.push(m);
                }
            }
        }
        let mut out: Vec<(String, f32, u64)> =
            found.into_iter().map(|(t, _, s, f)| (t, s, f)).collect();
        out.sort_by(|a, b| self.compare(a, b));
        out.truncate(num);
        out
    }

    fn compare(&self, a: &(String, f32, u64), b: &(String, f32, u64)) -> std::cmp::Ordering {
        let score = b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal);
        let freq = b.2.cmp(&a.2);
        let first = if self.by_freq { freq.then(score) } else { score.then(freq) };
        first.then_with(|| a.0.cmp(&b.0))
    }

    /// One `FuzzyTermsEnum` pass: (term, boost, score, doc freq).
    fn pass(
        &self,
        q: &[char],
        edits: usize,
        prefix: usize,
        inspections: usize,
        floor: i64,
        terms: &Terms,
    ) -> Vec<(String, f32, f32, u64)> {
        let prefix = prefix.min(q.len());
        let mut queue: Vec<(String, f32, f32, u64)> = Vec::new();
        for (cand, &(cdf, _)) in &terms.stats {
            let c: Vec<char> = cand.chars().collect();
            if c.len() < prefix || c[..prefix] != q[..prefix] {
                continue;
            }
            let ed = osa(q, &c);
            if ed > edits || ed == 0 {
                continue;
            }
            let min = q.len().min(c.len());
            let boost = if min == 0 { 0.0 } else { 1.0 - ed as f32 / min as f32 };
            let weakest = queue.iter().map(|e| e.1).fold(f32::INFINITY, f32::min);
            if queue.len() >= inspections && boost <= weakest {
                continue;
            }
            if cdf as i64 <= floor {
                continue;
            }
            let score = if self.distance == Distance::Internal {
                boost
            } else {
                distance(self.distance, q, &c)
            };
            if score < self.accuracy {
                continue;
            }
            queue.push((cand.clone(), boost, score, cdf));
            if queue.len() > inspections {
                // Drop the weakest: lowest boost, then the greatest term.
                let (i, _) = queue
                    .iter()
                    .enumerate()
                    .min_by(|(_, a), (_, b)| {
                        a.1.partial_cmp(&b.1)
                            .unwrap_or(std::cmp::Ordering::Equal)
                            .then_with(|| b.0.cmp(&a.0))
                    })
                    .unwrap();
                queue.remove(i);
            }
        }
        queue
    }
}

fn require_mapped(mappings: &Value, field: &str) -> Result<(), EsError> {
    if resolve_field(mappings, field).1.is_none() {
        return Err(bad(&format!("no mapping found for field [{field}]")));
    }
    Ok(())
}

fn query_analyzer(
    o: &Map<String, Value>,
    mappings: &Value,
    field: &str,
) -> Result<String, EsError> {
    match o.get("analyzer").and_then(Value::as_str) {
        Some(a) => {
            check_analyzer(a)?;
            Ok(a.to_string())
        }
        None => Ok(field_analyzer(mappings, field)),
    }
}

fn term_suggester(
    o: &Map<String, Value>,
    text: &str,
    mappings: &Value,
    docs: &[CommittedDoc],
) -> Result<Vec<Value>, EsError> {
    let spell = Spell::parse(o, 5)?;
    require_mapped(mappings, &spell.field)?;
    let shard_size = int(o, "shard_size", spell.size).max(spell.size);
    let terms = Terms::of(mappings, docs, &spell.field);
    let analyzer = query_analyzer(o, mappings, &spell.field)?;
    let mut entries = Vec::new();
    for (tok, offset, length) in analyze_offsets(&analyzer, text) {
        let mut words = spell.similar(&tok, shard_size, &terms, None);
        words.sort_by(|a, b| spell.compare(a, b));
        words.truncate(spell.size);
        let options: Vec<Value> = words
            .into_iter()
            .map(|(t, score, freq)| json!({"text": t, "score": score, "freq": freq}))
            .collect();
        entries.push(json!({"text": tok, "offset": offset, "length": length, "options": options}));
    }
    Ok(entries)
}

// ---------------------------------------------------------------------------
// Phrase suggester.

#[derive(Clone)]
struct Candidate {
    term: String,
    freq: u64,
    distance: f64,
    score: f64,
    user_input: bool,
}

enum Smoothing {
    StupidBackoff(f64),
    Laplace(f64),
    /// Only the unigram weight matters without n-gram counts.
    Linear(f64),
}

struct Scorer {
    vocab: f64,
    num_terms: f64,
    real_word: f64,
    smoothing: Smoothing,
    gram: usize,
}

impl Scorer {
    fn channel(&self, c: &Candidate) -> f64 {
        if c.distance == 1.0 { self.real_word } else { c.distance }
    }

    fn unigram(&self, w: &Candidate) -> f64 {
        match self.smoothing {
            Smoothing::Laplace(alpha) => {
                (w.freq as f64 + alpha) / (self.vocab + alpha * self.num_terms)
            }
            _ => (1.0 + w.freq as f64) / (self.vocab + self.num_terms),
        }
    }

    /// n-gram counts come from shingle fields; noida has none, so every
    /// bigram and trigram count is zero and the models back off.
    fn bigram(&self, w: &Candidate, w1: &Candidate) -> f64 {
        match self.smoothing {
            Smoothing::StupidBackoff(discount) => discount * self.unigram(w),
            Smoothing::Laplace(alpha) => alpha / (w1.freq as f64 + alpha * self.num_terms),
            Smoothing::Linear(unigram) => unigram * self.unigram(w),
        }
    }

    fn trigram(&self, w: &Candidate, w1: &Candidate) -> f64 {
        match self.smoothing {
            Smoothing::StupidBackoff(discount) => discount * self.bigram(w, w1),
            Smoothing::Laplace(alpha) => alpha / (alpha * self.num_terms),
            Smoothing::Linear(..) => self.bigram(w, w1),
        }
    }

    fn score(&self, path: &[Candidate], at: usize) -> f64 {
        let c = self.channel(&path[at]);
        let p = if at == 0 || self.gram == 1 {
            self.unigram(&path[at])
        } else if at == 1 || self.gram == 2 {
            self.bigram(&path[at], &path[at - 1])
        } else {
            self.trigram(&path[at], &path[at - 1])
        };
        (c * p).log10()
    }
}

struct CandidateSet {
    original: Candidate,
    candidates: Vec<Candidate>,
}

/// A correction: its score and the chosen candidate at each position.
type Correction = (f64, Vec<Candidate>);

/// `Correction.compareTo`: by score, then earlier terms win ties.
fn correction_cmp(a: &Correction, b: &Correction) -> std::cmp::Ordering {
    a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal).then_with(|| {
        for (x, y) in a.1.iter().zip(&b.1) {
            let c = x.term.cmp(&y.term);
            if c != std::cmp::Ordering::Equal {
                return c.reverse();
            }
        }
        a.1.len().cmp(&b.1.len())
    })
}

struct Search<'a> {
    scorer: &'a Scorer,
    sets: &'a [CandidateSet],
    cutoff: f64,
    max: usize,
    out: Vec<Correction>,
}

impl Search<'_> {
    fn update(&mut self, path: &[Candidate], log_score: f64) {
        let score = log_score.exp();
        if score <= self.cutoff {
            return;
        }
        let c: Correction = (score, path.to_vec());
        if self.out.len() < self.max {
            self.out.push(c);
        } else if let Some((i, weakest)) =
            self.out.iter().enumerate().min_by(|a, b| correction_cmp(a.1, b.1))
            && correction_cmp(weakest, &c) == std::cmp::Ordering::Less
        {
            self.out[i] = c;
        }
    }

    fn find(&mut self, path: &mut Vec<Candidate>, ord: usize, left: usize, score: f64) {
        let set = &self.sets[ord];
        let last = ord == self.sets.len() - 1;
        let mut options = vec![(set.original.clone(), left)];
        if left > 0 {
            options.extend(set.candidates.iter().map(|c| (c.clone(), left - 1)));
        }
        for (c, rest) in options {
            path.truncate(ord);
            path.push(c);
            let s = score + self.scorer.score(path, ord);
            if last {
                self.update(path, s);
            } else {
                self.find(path, ord + 1, rest, s);
            }
        }
        path.truncate(ord);
    }
}

fn smoothing(o: &Map<String, Value>) -> Result<Smoothing, EsError> {
    let Some(s) = o.get("smoothing").and_then(Value::as_object) else {
        return Ok(Smoothing::StupidBackoff(0.4));
    };
    let Some((kind, body)) = s.iter().next() else {
        return Err(EsError::parsing("smoothing model is missing"));
    };
    let b = body.as_object().cloned().unwrap_or_default();
    Ok(match kind.as_str() {
        "stupid_backoff" => Smoothing::StupidBackoff(float(&b, "discount", 0.4)),
        "laplace" => Smoothing::Laplace(float(&b, "alpha", 0.5)),
        "linear" => Smoothing::Linear(float(&b, "unigram_lambda", 0.0)),
        other => {
            return Err(EsError::new(
                400,
                "illegal_argument_exception",
                &format!("suggester[phrase] doesn't support [{other}]"),
            ));
        }
    })
}

fn phrase_suggester(
    o: &Map<String, Value>,
    text: &str,
    mappings: &Value,
    docs: &[CommittedDoc],
) -> Result<Vec<Value>, EsError> {
    let field = o
        .get("field")
        .and_then(Value::as_str)
        .ok_or_else(|| parse_err("the required field option [field] is missing"))?
        .to_string();
    require_mapped(mappings, &field)?;
    let size = int(o, "size", 5);
    let shard_size = int(o, "shard_size", 5).max(size);
    let gram = int(o, "gram_size", 1).max(1);
    let real_word = float(o, "real_word_error_likelihood", 0.95);
    let confidence = float(o, "confidence", 1.0);
    let max_errors = float(o, "max_errors", 1.0);
    let separator = o.get("separator").and_then(Value::as_str).unwrap_or(" ").to_string();
    let token_limit = int(o, "token_limit", 10);
    let smoothing = smoothing(o)?;
    let highlight = o.get("highlight").map(|h| {
        (
            h.get("pre_tag").and_then(Value::as_str).unwrap_or("").to_string(),
            h.get("post_tag").and_then(Value::as_str).unwrap_or("").to_string(),
        )
    });
    let mut generators = Vec::new();
    match o.get("direct_generator") {
        Some(Value::Array(gs)) => {
            for g in gs {
                let g = g.as_object().cloned().unwrap_or_default();
                if g.contains_key("pre_filter") || g.contains_key("post_filter") {
                    return Err(bad("noida-db does not support direct_generator pre/post filters"));
                }
                let spell = Spell::parse(&g, 5)?;
                require_mapped(mappings, &spell.field)?;
                generators.push(spell);
            }
        }
        Some(_) => return Err(EsError::parsing("[direct_generator] must be an array")),
        None => {
            let mut g = Map::new();
            g.insert("field".into(), json!(field));
            generators.push(Spell::parse(&g, 5)?);
        }
    }
    let terms = Terms::of(mappings, docs, &field);
    let analyzer = query_analyzer(o, mappings, &field)?;
    let tokens = analyze_offsets(&analyzer, text);
    let entry = |options: Vec<Value>| json!({"text": text, "offset": 0, "length": utf16_len(text), "options": options});
    if tokens.is_empty() || tokens.len() >= token_limit {
        return Ok(vec![entry(vec![])]);
    }
    let gen_terms: Vec<Terms> =
        generators.iter().map(|g| Terms::of(mappings, docs, &g.field)).collect();
    let dict = terms.max_doc as f64;
    let candidate = |term: &str, distance: f64, df: u64, user: bool| Candidate {
        term: term.to_string(),
        freq: terms.ttf(term),
        distance,
        score: distance * ((df as f64 + 1.0) / (dict + 1.0)),
        user_input: user,
    };
    let mut sets = Vec::new();
    for (tok, _, _) in &tokens {
        let original = candidate(tok, 1.0, terms.df(tok), true);
        let mut cands: Vec<Candidate> = Vec::new();
        for (g, gt) in generators.iter().zip(&gen_terms) {
            let tf = original.freq;
            let threshold = if g.mode == Mode::Always || tf == 0 {
                0.0
            } else {
                let t = tf as f64;
                ((t * ((t - 1e-6).log10() / 5f64.log10())).round().max(0.0) + 1.0) as f32
            };
            for (t, score, df) in g.similar(tok, g.size, gt, Some(threshold)) {
                if !cands.iter().any(|c| c.term == t) {
                    cands.push(candidate(&t, score as f64, df, false));
                }
            }
        }
        cands.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.term.cmp(&b.term))
        });
        cands.truncate(shard_size);
        sets.push(CandidateSet { original, candidates: cands });
    }
    let scorer = Scorer {
        vocab: terms.sum_ttf as f64,
        num_terms: terms.stats.len() as f64,
        real_word,
        smoothing,
        gram,
    };
    let cutoff = if confidence > 0.0 {
        let path: Vec<Candidate> = sets.iter().map(|s| s.original.clone()).collect();
        let total: f64 = (0..path.len()).map(|i| scorer.score(&path, i)).sum();
        total.exp() * confidence
    } else {
        f64::MIN_POSITIVE
    };
    let misspellings = if max_errors >= 1.0 {
        max_errors as usize
    } else {
        (max_errors * sets.len() as f64 + 0.5).floor() as usize
    }
    .max(1);
    let mut search = Search { scorer: &scorer, sets: &sets, cutoff, max: shard_size, out: vec![] };
    search.find(&mut Vec::new(), 0, misspellings, 0.0);
    let mut corrections = search.out;
    corrections.sort_by(|a, b| correction_cmp(b, a));
    let collate = o.get("collate");
    let mut options = Vec::new();
    for (score, path) in corrections {
        let plain: Vec<&str> = path.iter().map(|c| c.term.as_str()).collect();
        let phrase = plain.join(&separator);
        let mut opt = json!({"text": phrase, "score": score as f32});
        if let Some((pre, post)) = &highlight {
            let marked: Vec<String> =
                path.iter()
                    .map(|c| {
                        if c.user_input { c.term.clone() } else { format!("{pre}{}{post}", c.term) }
                    })
                    .collect();
            opt["highlighted"] = json!(marked.join(&separator));
        }
        if let Some(c) = collate {
            let matched = collate_matches(c, &phrase, mappings, docs)?;
            if c.get("prune").and_then(Value::as_bool).unwrap_or(false) {
                opt["collate_match"] = json!(matched);
            } else if !matched {
                continue;
            }
        }
        options.push(opt);
        if options.len() == size {
            break;
        }
    }
    Ok(vec![entry(options)])
}

/// `collate`: whether the query template, with `{{suggestion}}` (and any
/// `params`) filled in, matches a document.
fn collate_matches(
    c: &Value,
    suggestion: &str,
    mappings: &Value,
    docs: &[CommittedDoc],
) -> Result<bool, EsError> {
    let source = c
        .get("query")
        .and_then(|q| q.get("source").or_else(|| q.get("inline")))
        .ok_or_else(|| EsError::parsing("[collate] requires a query source"))?;
    let mut tpl = match source {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    };
    let quote = |s: &str| {
        let q = serde_json::to_string(s).unwrap_or_default();
        q[1..q.len() - 1].to_string()
    };
    tpl = tpl.replace("{{suggestion}}", &quote(suggestion));
    if let Some(params) = c.get("params").and_then(Value::as_object) {
        for (k, v) in params {
            let s = text_of(v).unwrap_or_else(|| v.to_string());
            tpl = tpl.replace(&format!("{{{{{k}}}}}"), &quote(&s));
        }
    }
    let query: Value = serde_json::from_str(&tpl)
        .map_err(|_| EsError::parsing("[collate] query template is not valid JSON"))?;
    Ok(!eval(&query, mappings, docs)?.is_empty())
}

// ---------------------------------------------------------------------------
// Completion suggester.

/// The `contexts` of a completion field's mapping.
#[derive(Clone)]
struct ContextMapping {
    name: String,
    geo: bool,
    path: Option<String>,
    precision: usize,
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

/// The geohash level Elasticsearch picks for a distance precision
/// (measured against 8.15: `ceil((bits - 3) * 2 / 5)` where `bits` is the
/// binary log of the cells of that size across the equator).
fn geohash_levels(meters: f64) -> usize {
    if meters <= 0.0 {
        return 12;
    }
    let equator = 40_075_016.69;
    let polar = std::f64::consts::PI * 6_356_752.314_245;
    let ratio = 1.0 + polar / equator;
    let bits = (equator * ratio / meters).log2().ceil();
    (((bits - 3.0) * 2.0 / 5.0).ceil() as i64).clamp(1, 12) as usize
}

fn precision_of(v: Option<&Value>) -> Option<usize> {
    match v? {
        Value::Number(n) => n.as_u64().map(|n| n as usize),
        Value::String(s) => {
            if let Ok(n) = s.parse::<usize>() {
                return Some(n);
            }
            let s = s.trim();
            let idx = s.find(|c: char| c.is_alphabetic())?;
            let (n, unit) = s.split_at(idx);
            let n: f64 = n.trim().parse().ok()?;
            let m = match unit {
                "km" | "kilometers" => 1000.0,
                "m" | "meters" => 1.0,
                "cm" => 0.01,
                "mm" => 0.001,
                "mi" | "miles" => 1609.344,
                "yd" | "yards" => 0.9144,
                "ft" | "feet" => 0.3048,
                "in" | "inch" => 0.0254,
                "nmi" | "NM" => 1852.0,
                _ => return None,
            };
            Some(geohash_levels(n * m))
        }
        _ => None,
    }
}

fn context_mappings(def: &Value) -> Vec<ContextMapping> {
    def.get("contexts")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .map(|c| ContextMapping {
                    name: c.get("name").and_then(Value::as_str).unwrap_or("").to_string(),
                    geo: c
                        .get("type")
                        .and_then(Value::as_str)
                        .is_some_and(|t| t.eq_ignore_ascii_case("geo")),
                    path: c.get("path").and_then(Value::as_str).map(str::to_string),
                    precision: precision_of(c.get("precision")).unwrap_or(6),
                })
                .collect()
        })
        .unwrap_or_default()
}

const BASE32: &[u8] = b"0123456789bcdefghjkmnpqrstuvwxyz";

/// The geohash cell (row, column) of a point at `level`.
fn cell(lat: f64, lon: f64, level: usize) -> (i64, i64) {
    let bits = 5 * level;
    let lon_bits = bits.div_ceil(2);
    let lat_bits = bits / 2;
    let scale = |v: f64, lo: f64, span: f64, n: usize| {
        let cells = (1u64 << n) as f64;
        (((v - lo) / span * cells).floor() as i64).clamp(0, cells as i64 - 1)
    };
    (scale(lat, -90.0, 180.0, lat_bits), scale(lon, -180.0, 360.0, lon_bits))
}

fn cell_hash(row: i64, col: i64, level: usize) -> String {
    let bits = 5 * level;
    let lon_bits = bits.div_ceil(2);
    let lat_bits = bits / 2;
    let mut out = String::new();
    let (mut li, mut ai) = (lon_bits, lat_bits);
    let mut acc = 0usize;
    for b in 0..bits {
        let bit = if b % 2 == 0 {
            li -= 1;
            (col >> li) & 1
        } else {
            ai -= 1;
            (row >> ai) & 1
        };
        acc = (acc << 1) | bit as usize;
        if b % 5 == 4 {
            out.push(BASE32[acc] as char);
            acc = 0;
        }
    }
    out
}

fn decode_geohash(h: &str) -> Option<(f64, f64)> {
    let (mut lat, mut lon) = ((-90.0f64, 90.0f64), (-180.0f64, 180.0f64));
    let mut even = true;
    for c in h.chars() {
        let v = BASE32.iter().position(|&b| b as char == c.to_ascii_lowercase())?;
        for i in (0..5).rev() {
            let bit = (v >> i) & 1 == 1;
            let r = if even { &mut lon } else { &mut lat };
            let mid = (r.0 + r.1) / 2.0;
            if bit {
                r.0 = mid;
            } else {
                r.1 = mid;
            }
            even = !even;
        }
    }
    if h.is_empty() {
        return None;
    }
    Some(((lat.0 + lat.1) / 2.0, (lon.0 + lon.1) / 2.0))
}

fn geo_point(v: &Value) -> Option<(f64, f64)> {
    match v {
        Value::Object(o) => Some((num(o.get("lat"))?, num(o.get("lon"))?)),
        Value::Array(a) if a.len() == 2 && a.iter().all(Value::is_number) => {
            Some((a[1].as_f64()?, a[0].as_f64()?))
        }
        Value::String(s) => match s.split_once(',') {
            Some((la, lo)) => Some((la.trim().parse().ok()?, lo.trim().parse().ok()?)),
            None => decode_geohash(s.trim()),
        },
        _ => None,
    }
}

fn geo_points(v: &Value) -> Vec<(f64, f64)> {
    match v {
        Value::Array(a) if !(a.len() == 2 && a.iter().all(Value::is_number)) => {
            a.iter().filter_map(geo_point).collect()
        }
        other => geo_point(other).into_iter().collect(),
    }
}

fn category_values(v: &Value) -> Vec<String> {
    match v {
        Value::Array(a) => a.iter().filter_map(text_of).collect(),
        other => text_of(other).into_iter().collect(),
    }
}

/// Context values by context name.
type Contexts = BTreeMap<String, BTreeSet<String>>;

/// One completion input: its surface form, weight and contexts.
struct Entry {
    input: String,
    weight: u64,
    contexts: BTreeMap<String, BTreeSet<String>>,
}

fn doc_parse_err(reason: &str) -> EsError {
    EsError::new(400, "document_parsing_exception", &format!("failed to parse: {reason}"))
}

/// The entries of one completion field value in `source`.
fn entries_of(
    field: &str,
    v: &Value,
    cms: &[ContextMapping],
    source: &Value,
    max_len: usize,
) -> Result<Vec<Entry>, EsError> {
    let (inputs, weight, explicit) = match v {
        Value::String(s) => (vec![s.clone()], 1u64, None),
        Value::Object(o) => {
            if let Some(k) =
                o.keys().find(|k| !["input", "weight", "contexts"].contains(&k.as_str()))
            {
                return Err(doc_parse_err(&format!(
                    "unknown field name [{k}], must be one of [input, weight, contexts]"
                )));
            }
            let inputs = match o.get("input") {
                Some(Value::Array(a)) => {
                    a.iter().filter_map(Value::as_str).map(str::to_string).collect()
                }
                Some(Value::String(s)) => vec![s.clone()],
                _ => vec![],
            };
            let weight = match o.get("weight") {
                None => 1,
                Some(w) => {
                    let n = match w {
                        Value::Number(n) => n.as_f64(),
                        Value::String(s) => s.parse::<f64>().ok(),
                        _ => None,
                    };
                    let Some(n) = n.filter(|n| n.fract() == 0.0) else {
                        let shown = text_of(w).unwrap_or_else(|| w.to_string());
                        return Err(doc_parse_err(&format!(
                            "weight must be an integer, but was [{shown}]"
                        )));
                    };
                    if !(0.0..=2147483647.0).contains(&n) {
                        return Err(doc_parse_err(&format!(
                            "weight must be in the interval [0..2147483647], but was [{n}]"
                        )));
                    }
                    n as u64
                }
            };
            (inputs, weight, o.get("contexts"))
        }
        Value::Null => return Ok(vec![]),
        other => {
            let kind = match other {
                Value::Number(_) => "VALUE_NUMBER",
                Value::Bool(_) => "VALUE_BOOLEAN",
                _ => "START_ARRAY",
            };
            return Err(EsError::new(
                400,
                "document_parsing_exception",
                &format!(
                    "failed to parse: failed to parse [{field}]: expected text or object, but got {kind}"
                ),
            ));
        }
    };
    let mut contexts: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for cm in cms {
        let given = explicit.and_then(|e| e.get(&cm.name));
        let from_path: Vec<&Value> = match (&cm.path, given) {
            (Some(p), None) => raw_values(source, p),
            _ => vec![],
        };
        let mut vals = BTreeSet::new();
        for v in given.into_iter().chain(from_path) {
            if cm.geo {
                for (lat, lon) in geo_points(v) {
                    let (r, c) = cell(lat, lon, cm.precision);
                    vals.insert(cell_hash(r, c, cm.precision));
                }
            } else {
                vals.extend(category_values(v));
            }
        }
        if !vals.is_empty() {
            contexts.insert(cm.name.clone(), vals);
        }
    }
    if !cms.is_empty() && contexts.is_empty() && !inputs.is_empty() {
        return Err(EsError::new(
            400,
            "illegal_argument_exception",
            &format!("Contexts are mandatory in context enabled completion field [{field}]"),
        ));
    }
    Ok(inputs
        .into_iter()
        .map(|i| {
            let input =
                if i.chars().count() > max_len { i.chars().take(max_len).collect() } else { i };
            Entry { input, weight, contexts: contexts.clone() }
        })
        .collect())
}

/// Every completion field in `mappings` as (full name, value path, def).
fn completion_fields(mappings: &Value) -> Vec<(String, String, Value)> {
    fn walk(props: Option<&Value>, prefix: &str, out: &mut Vec<(String, String, Value)>) {
        let Some(obj) = props.and_then(Value::as_object) else { return };
        for (k, def) in obj {
            let full = if prefix.is_empty() { k.clone() } else { format!("{prefix}.{k}") };
            if def.get("type").and_then(Value::as_str) == Some("completion") {
                out.push((full.clone(), full.clone(), def.clone()));
            }
            if let Some(subs) = def.get("fields").and_then(Value::as_object) {
                for (s, sd) in subs {
                    if sd.get("type").and_then(Value::as_str) == Some("completion") {
                        out.push((format!("{full}.{s}"), full.clone(), sd.clone()));
                    }
                }
            }
            walk(def.get("properties"), &full, out);
        }
    }
    let mut out = Vec::new();
    walk(mappings.get("properties"), "", &mut out);
    out
}

/// Index-time checks on a document's completion fields: Elasticsearch
/// rejects bad weights, unknown keys and missing contexts.
pub fn validate_doc(mappings: &Value, source: &Value) -> Result<(), EsError> {
    for (name, path, def) in completion_fields(mappings) {
        let cms = context_mappings(&def);
        for v in raw_values(source, &path) {
            entries_of(&name, v, &cms, source, usize::MAX)?;
        }
    }
    Ok(())
}

/// The analyzed form a completion input or prefix is matched on: the
/// analyzer's tokens, joined by a separator when separators are kept.
fn completion_form(def: &Value, text: &str, search: bool) -> String {
    let analyzer = (if search { def.get("search_analyzer") } else { None })
        .or_else(|| def.get("analyzer"))
        .and_then(Value::as_str)
        .unwrap_or("simple");
    let tokens: Vec<String> = analyze_offsets(analyzer, text).into_iter().map(|t| t.0).collect();
    let sep = def.get("preserve_separators").and_then(Value::as_bool).unwrap_or(true);
    tokens.join(if sep { "\u{1f}" } else { "" })
}

/// A query context: category value or geohash, boost, and whether it
/// matches as a prefix.
struct QueryContext {
    name: String,
    value: String,
    boost: f64,
    prefix: bool,
}

fn query_contexts(
    spec: Option<&Value>,
    cms: &[ContextMapping],
) -> Result<Vec<QueryContext>, EsError> {
    let mut out = Vec::new();
    let Some(spec) = spec.and_then(Value::as_object) else { return Ok(out) };
    for (name, v) in spec {
        let Some(cm) = cms.iter().find(|c| &c.name == name) else {
            let names: Vec<&str> = cms.iter().map(|c| c.name.as_str()).collect();
            return Err(bad(&format!(
                "Unknown context name [{name}], must be one of [{}]",
                names.join(", ")
            )));
        };
        let items: Vec<&Value> = match v {
            Value::Array(a) if !(cm.geo && a.len() == 2 && a.iter().all(Value::is_number)) => {
                a.iter().collect()
            }
            other => vec![other],
        };
        for item in items {
            let (ctx, boost, prefix) = match item {
                Value::Object(o) if o.contains_key("context") => (
                    o.get("context").cloned().unwrap_or(Value::Null),
                    num(o.get("boost")).unwrap_or(1.0),
                    o,
                ),
                other => (other.clone(), 1.0, &Map::new()),
            };
            if cm.geo {
                let Some((lat, lon)) = geo_point(&ctx) else {
                    return Err(EsError::parsing("contexts field must be a geo point"));
                };
                let precision =
                    precision_of(prefix.get("precision")).unwrap_or(12).min(cm.precision);
                let neighbours: Vec<usize> = match prefix.get("neighbours") {
                    Some(Value::Array(a)) => {
                        a.iter().filter_map(|v| precision_of(Some(v))).collect()
                    }
                    Some(v) => precision_of(Some(v)).into_iter().collect(),
                    None => vec![],
                };
                let mut hashes = BTreeSet::new();
                let (r, c) = cell(lat, lon, precision);
                hashes.insert(cell_hash(r, c, precision));
                let around = |level: usize, set: &mut BTreeSet<String>| {
                    let (r, c) = cell(lat, lon, level);
                    let rows = 1i64 << ((5 * level) / 2);
                    let cols = 1i64 << (5 * level).div_ceil(2);
                    for dr in -1..=1 {
                        for dc in -1..=1 {
                            let nr = r + dr;
                            if nr < 0 || nr >= rows {
                                continue;
                            }
                            set.insert(cell_hash(nr, (c + dc).rem_euclid(cols), level));
                        }
                    }
                };
                if neighbours.is_empty() && precision == cm.precision {
                    around(precision, &mut hashes);
                } else {
                    for n in neighbours.into_iter().filter(|&n| n < precision) {
                        around(n, &mut hashes);
                    }
                }
                for h in hashes {
                    let p = h.len() < cm.precision;
                    out.push(QueryContext { name: name.clone(), value: h, boost, prefix: p });
                }
            } else {
                let Some(value) = text_of(&ctx) else {
                    return Err(EsError::parsing(
                        "category context must be a string, number or boolean",
                    ));
                };
                let p = prefix.get("prefix").and_then(Value::as_bool).unwrap_or(false);
                out.push(QueryContext { name: name.clone(), value, boost, prefix: p });
            }
        }
    }
    Ok(out)
}

/// A completion fuzzy query's edit distance: `AUTO` (the default) resolves
/// without the text, to one edit, as Elasticsearch 8.15 does.
fn fuzziness(v: Option<&Value>) -> usize {
    match v {
        Some(Value::Number(n)) => n.as_u64().unwrap_or(1).min(2) as usize,
        Some(Value::String(s)) => s.parse::<usize>().map_or(1, |n| n.min(2)),
        _ => 1,
    }
}

enum Matcher {
    Prefix(String),
    Fuzzy { q: Vec<char>, edits: usize, prefix: usize },
    Regex(regex_lite::Regex),
}

impl Matcher {
    /// `None` when `form` doesn't match, else the match's boost.
    fn boost(&self, form: &str) -> Option<f64> {
        match self {
            Matcher::Prefix(p) => form.starts_with(p.as_str()).then_some(0.0),
            Matcher::Regex(re) => re.is_match(form).then_some(0.0),
            Matcher::Fuzzy { q, edits, prefix } => {
                let f: Vec<char> = form.chars().collect();
                let pl = (*prefix).min(q.len());
                if f.len() < pl || f[..pl] != q[..pl] {
                    return None;
                }
                // Lucene stops at the shortest accepted prefix and boosts
                // by the length it shares with the query.
                (0..=f.len())
                    .find(|&n| osa(&f[..n], q) <= *edits)
                    .map(|n| f[..n].iter().zip(q).take_while(|(a, b)| a == b).count() as f64)
            }
        }
    }
}

fn completion_suggester(
    o: &Map<String, Value>,
    text: &str,
    regex: Option<&str>,
    mappings: &Value,
    docs: &[CommittedDoc],
) -> Result<Vec<Value>, EsError> {
    let field = o
        .get("field")
        .and_then(Value::as_str)
        .ok_or_else(|| parse_err("the required field option [field] is missing"))?
        .to_string();
    let def = field_def(mappings, &field)
        .ok_or_else(|| bad(&format!("no mapping found for field [{field}]")))?;
    if def.get("type").and_then(Value::as_str) != Some("completion") {
        return Err(bad(&format!("Field [{field}] is not a completion suggest field")));
    }
    let size = int(o, "size", 5);
    let skip_duplicates = o.get("skip_duplicates").and_then(Value::as_bool).unwrap_or(false);
    let cms = context_mappings(def);
    let qcs = query_contexts(o.get("contexts"), &cms)?;
    if !cms.is_empty() && qcs.is_empty() {
        return Err(bad("Missing mandatory contexts in context query"));
    }
    let max_len = def.get("max_input_length").and_then(Value::as_u64).unwrap_or(50) as usize;
    let matcher = if let Some(r) = regex {
        let re = regex_lite::Regex::new(&format!("^(?:{r})"))
            .map_err(|e| bad(&format!("invalid regex [{r}]: {e}")))?;
        Matcher::Regex(re)
    } else {
        let q = completion_form(def, text, true);
        if q.is_empty() {
            return Ok(vec![
                json!({"text": text, "offset": 0, "length": utf16_len(text), "options": []}),
            ]);
        }
        match o.get("fuzzy") {
            None | Some(Value::Bool(false)) => Matcher::Prefix(q),
            Some(f) => {
                let qc: Vec<char> = q.chars().collect();
                let min_length = f.get("min_length").and_then(Value::as_u64).unwrap_or(3) as usize;
                let edits = if qc.len() < min_length { 0 } else { fuzziness(f.get("fuzziness")) };
                let prefix = f.get("prefix_length").and_then(Value::as_u64).unwrap_or(1) as usize;
                Matcher::Fuzzy { q: qc, edits, prefix }
            }
        }
    };
    let (path, _) = resolve_field(mappings, &field);
    // Best match per document: (score, text, doc, matched contexts).
    let mut hits: Vec<(f64, String, usize, Contexts)> = Vec::new();
    for (di, d) in docs.iter().enumerate() {
        let source = d.full();
        let mut best: Option<(f64, String, String)> = None;
        let mut matched_ctx: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        // The strongest single context match (what `skip_duplicates`
        // reports, as it stops at the first path per document).
        let mut first_ctx: Option<(f64, String, String)> = None;
        for v in raw_values(source, &path) {
            for e in entries_of(&field, v, &cms, source, max_len)? {
                let form = completion_form(def, &e.input, false);
                let Some(fuzzy_boost) = matcher.boost(&form) else { continue };
                let ctx_boost = if cms.is_empty() {
                    0.0
                } else {
                    let mut b: Option<f64> = None;
                    for qc in &qcs {
                        let Some(vals) = e.contexts.get(&qc.name) else { continue };
                        for v in vals {
                            let hit =
                                if qc.prefix { v.starts_with(&qc.value) } else { *v == qc.value };
                            if hit {
                                b = Some(b.map_or(qc.boost, |x| x.max(qc.boost)));
                                matched_ctx.entry(qc.name.clone()).or_default().insert(v.clone());
                                let cand = (qc.boost, qc.name.clone(), v.clone());
                                let stronger = first_ctx.as_ref().is_none_or(|f| {
                                    cand.0 > f.0
                                        || (cand.0 == f.0 && (&cand.1, &cand.2) < (&f.1, &f.2))
                                });
                                if stronger {
                                    first_ctx = Some(cand);
                                }
                            }
                        }
                    }
                    match b {
                        Some(b) => b,
                        None => continue,
                    }
                };
                let boost = ctx_boost + fuzzy_boost;
                let w = e.weight as f64;
                let score = if boost == 0.0 {
                    w
                } else if w == 0.0 {
                    boost
                } else {
                    w * boost
                };
                // Ties go to the smaller analyzed form, then surface form.
                let better = match &best {
                    None => true,
                    Some((s, f, t)) => score > *s || (score == *s && (&form, &e.input) < (f, t)),
                };
                if better {
                    best = Some((score, form, e.input.clone()));
                }
            }
        }
        if let Some((s, _, t)) = best {
            if skip_duplicates && let Some((_, name, value)) = first_ctx {
                matched_ctx = BTreeMap::from([(name, BTreeSet::from([value]))]);
            }
            hits.push((s, t, di, matched_ctx));
        }
    }
    hits.sort_by(|a, b| {
        b.0.partial_cmp(&a.0)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.1.cmp(&b.1))
            .then(a.2.cmp(&b.2))
    });
    let mut seen = BTreeSet::new();
    let mut options = Vec::new();
    for (score, t, di, ctx) in hits {
        if skip_duplicates && !seen.insert(t.clone()) {
            continue;
        }
        if options.len() == size {
            break;
        }
        let d = &docs[di];
        let mut opt = json!({
            "text": t,
            "_index": d.index,
            "_id": d.id,
            "_score": score as f32,
            "_source": d.full(),
        });
        if !ctx.is_empty() {
            opt["contexts"] = json!(ctx);
        }
        options.push(opt);
    }
    let shown = regex.unwrap_or(text);
    Ok(vec![json!({"text": shown, "offset": 0, "length": utf16_len(shown), "options": options})])
}

// ---------------------------------------------------------------------------

/// The `suggest` section of a search response.
pub fn suggest(spec: &Value, mappings: &Value, docs: &[CommittedDoc]) -> Result<Value, EsError> {
    let Some(obj) = spec.as_object() else {
        return Err(EsError::parsing("suggest must be an object"));
    };
    let global = obj.get("text").and_then(text_of);
    let mut out = Map::new();
    for (name, s) in obj {
        if name == "text" {
            continue;
        }
        let Some(so) = s.as_object() else {
            return Err(EsError::parsing(&format!("suggestion [{name}] must be an object")));
        };
        let mut kind = None;
        for (k, v) in so {
            match k.as_str() {
                "text" | "prefix" | "regex" => {}
                "term" | "phrase" | "completion" => kind = Some((k.as_str(), v)),
                other => {
                    return Err(EsError::new(
                        400,
                        "named_object_not_found_exception",
                        &format!("[1:1] unknown field [{other}]"),
                    ));
                }
            }
        }
        let Some((kind, body)) = kind else {
            return Err(EsError::new(
                400,
                "illegal_argument_exception",
                &format!("suggestion [{name}] does not have a suggester type"),
            ));
        };
        let body = body.as_object().cloned().unwrap_or_default();
        let text = so
            .get("text")
            .or_else(|| so.get("prefix"))
            .and_then(text_of)
            .or_else(|| global.clone());
        let regex = so.get("regex").and_then(Value::as_str);
        let entries = match (kind, text, regex) {
            ("completion", t, Some(r)) => {
                completion_suggester(&body, &t.unwrap_or_default(), Some(r), mappings, docs)?
            }
            (_, None, _) => {
                if !body.contains_key("field") {
                    return Err(parse_err("the required field option [field] is missing"));
                }
                return Err(bad("The required text option is missing"));
            }
            ("term", Some(t), _) => term_suggester(&body, &t, mappings, docs)?,
            ("phrase", Some(t), _) => phrase_suggester(&body, &t, mappings, docs)?,
            (_, Some(t), _) => completion_suggester(&body, &t, None, mappings, docs)?,
        };
        out.insert(name.clone(), Value::Array(entries));
    }
    Ok(Value::Object(out))
}

/// `typed_keys`: each suggestion named `<type>#<name>`.
pub fn type_keys(req: &Value, resp: &mut Value) {
    let Some(spec) = req.get("suggest").and_then(Value::as_object) else { return };
    let Some(s) = resp.get_mut("suggest").and_then(Value::as_object_mut) else { return };
    let names: Vec<String> = s.keys().cloned().collect();
    for name in names {
        let kind = spec
            .get(&name)
            .and_then(Value::as_object)
            .and_then(|o| o.keys().find(|k| ["term", "phrase", "completion"].contains(&k.as_str())))
            .cloned();
        if let (Some(kind), Some(v)) = (kind, s.remove(&name)) {
            s.insert(format!("{kind}#{name}"), v);
        }
    }
}

/// A completion field's mapping as Elasticsearch shows it: analyzer and
/// limits filled in, context types upper-cased, geo precision as a level.
pub fn normalize_mappings(m: &mut Value) {
    fn walk(props: Option<&mut Value>) {
        let Some(obj) = props.and_then(Value::as_object_mut) else { return };
        for (_, def) in obj.iter_mut() {
            normalize(def);
            if let Some(subs) = def.get_mut("fields").and_then(Value::as_object_mut) {
                for (_, sd) in subs.iter_mut() {
                    normalize(sd);
                }
            }
            walk(def.get_mut("properties"));
        }
    }
    fn normalize(def: &mut Value) {
        if def.get("type").and_then(Value::as_str) != Some("completion") {
            return;
        }
        let Some(o) = def.as_object_mut() else { return };
        let mut out = Map::new();
        out.insert("type".into(), json!("completion"));
        let analyzer = o.get("analyzer").cloned().unwrap_or(json!("simple"));
        out.insert("analyzer".into(), analyzer.clone());
        if let Some(sa) = o.get("search_analyzer").filter(|sa| **sa != analyzer) {
            out.insert("search_analyzer".into(), sa.clone());
        }
        for (k, d) in [
            ("preserve_separators", json!(true)),
            ("preserve_position_increments", json!(true)),
            ("max_input_length", json!(50)),
        ] {
            out.insert(k.into(), o.get(k).cloned().unwrap_or(d));
        }
        if let Some(cs) = o.get("contexts").and_then(Value::as_array) {
            let cs: Vec<Value> = cs
                .iter()
                .map(|c| {
                    let mut n = Map::new();
                    n.insert("name".into(), c.get("name").cloned().unwrap_or(Value::Null));
                    let ty = c.get("type").and_then(Value::as_str).unwrap_or("category");
                    n.insert("type".into(), json!(ty.to_uppercase()));
                    if let Some(p) = c.get("path") {
                        n.insert("path".into(), p.clone());
                    }
                    if ty.eq_ignore_ascii_case("geo") {
                        n.insert(
                            "precision".into(),
                            json!(precision_of(c.get("precision")).unwrap_or(6)),
                        );
                    }
                    Value::Object(n)
                })
                .collect();
            out.insert("contexts".into(), json!(cs));
        }
        for (k, v) in o.iter() {
            if !out.contains_key(k) && k != "search_analyzer" {
                out.insert(k.clone(), v.clone());
            }
        }
        *o = out;
    }
    walk(m.get_mut("properties"));
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chars(s: &str) -> Vec<char> {
        s.chars().collect()
    }

    #[test]
    fn osa_counts_a_transposition_as_one_edit() {
        assert_eq!(osa(&chars("amsterdma"), &chars("amsterdam")), 1);
        assert_eq!(levenshtein(&chars("amsterdma"), &chars("amsterdam")), 2);
    }

    #[test]
    fn geohash_round_trips() {
        let (r, c) = cell(57.64911, 10.40744, 11);
        assert_eq!(cell_hash(r, c, 11), "u4pruydqqvj");
        assert_eq!(geohash_levels(5000.0), 5);
        assert_eq!(geohash_levels(10000.0), 4);
        assert_eq!(geohash_levels(1.0), 10);
    }
}
