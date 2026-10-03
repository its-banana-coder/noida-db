//! `_search` / `_count`: query DSL evaluation, BM25 relevance ranking and
//! sorting/pagination over an index's refreshed (near-real-time) snapshot.
//!
//! Deliberately not a real inverted index: for the small document counts a
//! local dev database holds, a linear scan per query re-tokenizing on the
//! fly is simpler to keep correct and is fast enough (see
//! `docs/SERVICE_GUIDE.md`: "small and simple, performance is not a goal").

use serde_json::{Map, Value, json};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use super::analysis;
use super::scoring;

/// A refreshed, searchable copy of one document. Docs written since the
/// last refresh are not included here, matching Elasticsearch's
/// near-real-time semantics: they exist for real-time GET but not search.
#[derive(Clone)]
pub struct CommittedDoc {
    pub index: String,
    pub id: String,
    pub source: Value,
    /// Not read yet: reserved for `version`/`if_seq_no` query support on
    /// `_search`.
    #[allow(dead_code)]
    pub version: i64,
}

/// Where a (possibly dotted) field name's values come from in `_source`,
/// and its mapped type: a plain field (`brand`), an object path
/// (`meta.source`), or a multi-field (`name.raw` / `name.keyword`: the
/// values of `name`, indexed with the sub-field's type). Found via testing
/// before a public release: multi-fields weren't resolved at all, so
/// `term: {"name.raw": ...}` and sorting/aggregating on a `.raw` sub-field
/// silently matched nothing.
fn resolve_field(mappings: &Value, field: &str) -> (String, Option<String>) {
    let segs: Vec<&str> = field.split('.').collect();
    let mut props = mappings.get("properties");
    for (i, seg) in segs.iter().enumerate() {
        let Some(node) = props.and_then(|p| p.get(*seg)) else { break };
        if i + 1 == segs.len() {
            let ty = node.get("type").and_then(Value::as_str).unwrap_or("object");
            return (field.to_string(), Some(ty.to_string()));
        }
        if i + 2 == segs.len()
            && let Some(sub) = node.get("fields").and_then(|f| f.get(segs[i + 1]))
        {
            let ty = sub.get("type").and_then(Value::as_str).unwrap_or("keyword");
            return (segs[..=i].join("."), Some(ty.to_string()));
        }
        props = node.get("properties");
    }
    // Unmapped (e.g. a search across indices): `x.keyword` is the dynamic
    // keyword sub-field of `x`.
    if let Some(base) = field.strip_suffix(".keyword") {
        return (base.to_string(), Some("keyword".to_string()));
    }
    (field.to_string(), None)
}

/// Walks a dotted field path through nested objects, flattening arrays
/// along the way, the way Elasticsearch resolves e.g. `"meta.source"`.
fn navigate<'a>(v: &'a Value, path: &[&str]) -> Vec<&'a Value> {
    if path.is_empty() {
        // A leaf array is that many values (`"tags": ["a", "b"]`). Found
        // via testing before a public release: it used to be one opaque
        // value, so term/terms queries and aggregations never matched any
        // array field.
        return match v {
            Value::Array(arr) => arr.iter().flat_map(|e| navigate(e, path)).collect(),
            _ => vec![v],
        };
    }
    match v {
        Value::Object(m) => m.get(path[0]).map(|nv| navigate(nv, &path[1..])).unwrap_or_default(),
        Value::Array(arr) => arr.iter().flat_map(|e| navigate(e, path)).collect(),
        _ => Vec::new(),
    }
}

pub fn raw_values<'a>(source: &'a Value, field: &str) -> Vec<&'a Value> {
    let segs: Vec<&str> = field.split('.').collect();
    navigate(source, &segs)
}

fn value_to_term(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        _ => String::new(),
    }
}

/// The index terms for `field` in `source`: exact for `keyword` fields and
/// the implicit `<field>.keyword` sub-field, `standard`-analyzed otherwise
/// (Elasticsearch's dynamic-mapping default for strings).
pub fn tokens_for(mappings: &Value, source: &Value, field: &str) -> Vec<String> {
    let (path, ty) = resolve_field(mappings, field);
    let ty = ty.as_deref();
    if ty == Some("keyword") && path != field && field.ends_with(".keyword") {
        return raw_values(source, &path)
            .into_iter()
            .filter_map(Value::as_str)
            .filter(|s| s.chars().count() <= 256)
            .map(str::to_string)
            .collect();
    }
    let mut out = Vec::new();
    for v in raw_values(source, &path) {
        match v {
            Value::String(s) => {
                if ty == Some("keyword") {
                    out.push(s.clone());
                } else {
                    out.extend(analysis::standard(s));
                }
            }
            Value::Number(n) => out.push(n.to_string()),
            Value::Bool(b) => out.push(b.to_string()),
            _ => {}
        }
    }
    out
}

fn doc_tokens(mappings: &Value, docs: &[CommittedDoc], field: &str) -> Vec<Vec<String>> {
    docs.iter().map(|d| tokens_for(mappings, &d.source, field)).collect()
}

/// BM25 (Lucene/Elasticsearch defaults k1=1.2, b=0.75) over the given field
/// for a set of already-analyzed query terms.
fn bm25_scores(
    mappings: &Value,
    docs: &[CommittedDoc],
    field: &str,
    query_terms: &[String],
    require_all: bool,
) -> HashMap<usize, f32> {
    let per_doc = doc_tokens(mappings, docs, field);
    let doc_count = per_doc.iter().filter(|t| !t.is_empty()).count() as u64;
    let total_len: u64 = per_doc.iter().map(|t| t.len() as u64).sum();
    let avg_len = if doc_count > 0 { total_len as f32 / doc_count as f32 } else { 1.0 };

    let mut scores: HashMap<usize, f32> = HashMap::new();
    let mut matched_terms: HashMap<usize, usize> = HashMap::new();
    for term in query_terms {
        let doc_freq = per_doc.iter().filter(|t| t.contains(term)).count() as u64;
        if doc_freq == 0 {
            continue;
        }
        for (idx, toks) in per_doc.iter().enumerate() {
            let tf = toks.iter().filter(|t| *t == term).count() as u32;
            if tf == 0 {
                continue;
            }
            let doc_len = scoring::norm_doc_len(toks.len() as u32).max(1);
            let s = scoring::score(tf, doc_len, avg_len, doc_freq, doc_count.max(1));
            *scores.entry(idx).or_insert(0.0) += s;
            *matched_terms.entry(idx).or_insert(0) += 1;
        }
    }
    if require_all {
        let n = query_terms.len();
        scores.retain(|idx, _| matched_terms.get(idx).copied().unwrap_or(0) == n);
    }
    scores
}

fn field_and_spec(v: &Value) -> Option<(&str, &Value)> {
    v.as_object().and_then(|o| o.iter().next()).map(|(k, v)| (k.as_str(), v))
}

fn value_and_boost(spec: &Value) -> (Value, f32) {
    if let Some(o) = spec.as_object()
        && o.contains_key("value")
    {
        return (
            o.get("value").cloned().unwrap_or(Value::Null),
            o.get("boost").and_then(Value::as_f64).unwrap_or(1.0) as f32,
        );
    }
    (spec.clone(), 1.0)
}

fn eval_term(v: &Value, mappings: &Value, docs: &[CommittedDoc]) -> HashMap<usize, f32> {
    let Some((field, spec)) = field_and_spec(v) else { return HashMap::new() };
    let (value, boost) = value_and_boost(spec);
    let target = value_to_term(&value);
    let mut out = HashMap::new();
    for (idx, d) in docs.iter().enumerate() {
        if tokens_for(mappings, &d.source, field).contains(&target) {
            out.insert(idx, boost);
        }
    }
    out
}

fn eval_terms(v: &Value, mappings: &Value, docs: &[CommittedDoc]) -> HashMap<usize, f32> {
    let Some((field, arr)) =
        v.as_object().and_then(|o| o.iter().find(|(k, _)| k.as_str() != "boost"))
    else {
        return HashMap::new();
    };
    let targets: Vec<String> =
        arr.as_array().map(|a| a.iter().map(value_to_term).collect()).unwrap_or_default();
    let mut out = HashMap::new();
    for (idx, d) in docs.iter().enumerate() {
        let toks = tokens_for(mappings, &d.source, field);
        if targets.iter().any(|t| toks.contains(t)) {
            out.insert(idx, 1.0);
        }
    }
    out
}

fn eval_match(v: &Value, mappings: &Value, docs: &[CommittedDoc]) -> HashMap<usize, f32> {
    let Some((field, spec)) = field_and_spec(v) else { return HashMap::new() };
    let (text, op) = if let Some(o) = spec.as_object() {
        (
            o.get("query").and_then(Value::as_str).unwrap_or("").to_string(),
            o.get("operator").and_then(Value::as_str).unwrap_or("or").to_string(),
        )
    } else {
        (spec.as_str().unwrap_or("").to_string(), "or".to_string())
    };
    let query_terms = analysis::standard(&text);
    if query_terms.is_empty() {
        return HashMap::new();
    }
    bm25_scores(mappings, docs, field, &query_terms, op.eq_ignore_ascii_case("and"))
}

/// Whether `query_terms` occurs in `doc_tokens` as a contiguous run at
/// consecutive positions — Lucene's default `slop=0` phrase match. Document
/// term vectors here are already position-ordered with no gaps (nothing
/// filters tokens out of `tokens_for`), so the vector index *is* the term
/// position, exactly like a real positional inverted index at slop 0.
fn phrase_matches(doc_tokens: &[String], query_terms: &[String]) -> bool {
    let n = query_terms.len();
    if n == 0 || doc_tokens.len() < n {
        return false;
    }
    (0..=doc_tokens.len() - n).any(|start| doc_tokens[start..start + n] == query_terms[..])
}

/// `match_phrase`: like `match`, but the query's analyzed terms must appear
/// in the document at consecutive positions, in order (slop 0 — the only
/// slop value implemented; a non-zero `slop` option is accepted but
/// currently treated as 0).
fn eval_match_phrase(v: &Value, mappings: &Value, docs: &[CommittedDoc]) -> HashMap<usize, f32> {
    let Some((field, spec)) = field_and_spec(v) else { return HashMap::new() };
    let text = if let Some(o) = spec.as_object() {
        o.get("query").and_then(Value::as_str).unwrap_or("").to_string()
    } else {
        spec.as_str().unwrap_or("").to_string()
    };
    let query_terms = analysis::standard(&text);
    if query_terms.is_empty() {
        return HashMap::new();
    }
    let per_doc = doc_tokens(mappings, docs, field);
    let matched: HashSet<usize> = per_doc
        .iter()
        .enumerate()
        .filter(|(_, toks)| phrase_matches(toks, &query_terms))
        .map(|(idx, _)| idx)
        .collect();
    // Score the same as an AND `match` (every term must be present, which a
    // phrase match already implies) restricted to documents where the
    // phrase actually occurs at consecutive positions.
    let mut scores = bm25_scores(mappings, docs, field, &query_terms, true);
    scores.retain(|idx, _| matched.contains(idx));
    scores
}

/// `field` or `field^boost` (multi_match's field-boost syntax).
fn parse_field_boost(spec: &str) -> (&str, f32) {
    match spec.rsplit_once('^') {
        Some((field, boost)) => match boost.parse::<f32>() {
            Ok(b) => (field, b),
            Err(_) => (spec, 1.0),
        },
        None => (spec, 1.0),
    }
}

/// `multi_match` (`best_fields` type, Elasticsearch's default): analyzes the
/// query once and runs it as a `match` against every listed field, keeping
/// each matched document's *highest*-scoring field as its overall score —
/// matching `DisjunctionMaxQuery` with `tie_breaker=0.0` (also ES's
/// default), i.e. take the max, ignore the rest.
fn eval_multi_match(v: &Value, mappings: &Value, docs: &[CommittedDoc]) -> HashMap<usize, f32> {
    let Some(obj) = v.as_object() else { return HashMap::new() };
    let text = obj.get("query").and_then(Value::as_str).unwrap_or("");
    let query_terms = analysis::standard(text);
    if query_terms.is_empty() {
        return HashMap::new();
    }
    let fields: Vec<(String, f32)> = obj
        .get("fields")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .map(|s| {
                    let (f, b) = parse_field_boost(s);
                    (f.to_string(), b)
                })
                .collect()
        })
        .unwrap_or_default();
    let op = obj.get("operator").and_then(Value::as_str).unwrap_or("or");
    let require_all = op.eq_ignore_ascii_case("and");

    let mut best: HashMap<usize, f32> = HashMap::new();
    for (field, boost) in &fields {
        for (idx, score) in bm25_scores(mappings, docs, field, &query_terms, require_all) {
            let scaled = score * boost;
            let entry = best.entry(idx).or_insert(scaled);
            if scaled > *entry {
                *entry = scaled;
            }
        }
    }
    best
}

/// Glob match (`*` = any run of characters, `?` = exactly one character),
/// case-sensitive, the way Elasticsearch's `wildcard` query matches against
/// index terms.
fn glob_match(pattern: &[char], text: &[char]) -> bool {
    // Standard two-pointer glob matcher with backtracking on `*`: `star`
    // remembers the last `*` position in the pattern and how far into the
    // text we'd consumed when we saw it, so a failed match past that point
    // can retry by having `*` eat one more character.
    let (mut pi, mut ti) = (0usize, 0usize);
    let (mut star_pi, mut star_ti): (Option<usize>, usize) = (None, 0);
    while ti < text.len() {
        if pi < pattern.len() && (pattern[pi] == '?' || pattern[pi] == text[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < pattern.len() && pattern[pi] == '*' {
            star_pi = Some(pi);
            star_ti = ti;
            pi += 1;
        } else if let Some(sp) = star_pi {
            pi = sp + 1;
            star_ti += 1;
            ti = star_ti;
        } else {
            return false;
        }
    }
    while pi < pattern.len() && pattern[pi] == '*' {
        pi += 1;
    }
    pi == pattern.len()
}

/// `wildcard`: constant-score, non-analyzed glob match against a field's
/// index terms (the raw value for `keyword` fields, analyzed tokens for
/// `text` fields — same as `prefix`/`term`).
fn eval_wildcard(v: &Value, mappings: &Value, docs: &[CommittedDoc]) -> HashMap<usize, f32> {
    let Some((field, spec)) = field_and_spec(v) else { return HashMap::new() };
    let (value, boost) = value_and_boost(spec);
    let pattern = value_to_term(&value);
    let pattern_chars: Vec<char> = pattern.chars().collect();
    let mut out = HashMap::new();
    for (idx, d) in docs.iter().enumerate() {
        let matched = tokens_for(mappings, &d.source, field).iter().any(|t| {
            let t_chars: Vec<char> = t.chars().collect();
            glob_match(&pattern_chars, &t_chars)
        });
        if matched {
            out.insert(idx, boost);
        }
    }
    out
}

/// `regexp`: constant-score regular-expression match against a field's
/// index terms. Elasticsearch anchors the whole term against the pattern
/// (it must match start to end), which `regex_lite::Regex` doesn't do by
/// default, so the pattern is wrapped in `^(?:...)$`.
fn eval_regexp(v: &Value, mappings: &Value, docs: &[CommittedDoc]) -> HashMap<usize, f32> {
    let Some((field, spec)) = field_and_spec(v) else { return HashMap::new() };
    let (value, boost) = value_and_boost(spec);
    let pattern = value_to_term(&value);
    let Ok(re) = regex_lite::Regex::new(&format!("^(?:{pattern})$")) else {
        return HashMap::new();
    };
    let mut out = HashMap::new();
    for (idx, d) in docs.iter().enumerate() {
        if tokens_for(mappings, &d.source, field).iter().any(|t| re.is_match(t)) {
            out.insert(idx, boost);
        }
    }
    out
}

fn number_cmp(a: &Value, b: &Value) -> Option<Ordering> {
    a.as_f64()?.partial_cmp(&b.as_f64()?)
}

fn in_range(v: &Value, cond: &Value) -> bool {
    for op in ["gte", "gt", "lte", "lt"] {
        let Some(bound) = cond.get(op) else { continue };
        let ord = match (v, bound) {
            (Value::Number(_), Value::Number(_)) => number_cmp(v, bound),
            (Value::String(a), Value::String(b)) => Some(a.as_str().cmp(b.as_str())),
            _ => None,
        };
        let Some(ord) = ord else { return false };
        let ok = match op {
            "gte" => ord != Ordering::Less,
            "gt" => ord == Ordering::Greater,
            "lte" => ord != Ordering::Greater,
            "lt" => ord == Ordering::Less,
            _ => false,
        };
        if !ok {
            return false;
        }
    }
    true
}

fn eval_range(v: &Value, _mappings: &Value, docs: &[CommittedDoc]) -> HashMap<usize, f32> {
    let Some((field, cond)) = field_and_spec(v) else { return HashMap::new() };
    let mut out = HashMap::new();
    for (idx, d) in docs.iter().enumerate() {
        if raw_values(&d.source, field).into_iter().any(|val| in_range(val, cond)) {
            out.insert(idx, 1.0);
        }
    }
    out
}

fn eval_exists(v: &Value, docs: &[CommittedDoc]) -> HashMap<usize, f32> {
    let Some(field) = v.get("field").and_then(Value::as_str) else { return HashMap::new() };
    let mut out = HashMap::new();
    for (idx, d) in docs.iter().enumerate() {
        if raw_values(&d.source, field).into_iter().any(|v| !v.is_null()) {
            out.insert(idx, 1.0);
        }
    }
    out
}

fn eval_prefix(v: &Value, mappings: &Value, docs: &[CommittedDoc]) -> HashMap<usize, f32> {
    let Some((field, spec)) = field_and_spec(v) else { return HashMap::new() };
    let (value, boost) = value_and_boost(spec);
    let prefix = value_to_term(&value);
    let mut out = HashMap::new();
    for (idx, d) in docs.iter().enumerate() {
        if tokens_for(mappings, &d.source, field).iter().any(|t| t.starts_with(&prefix)) {
            out.insert(idx, boost);
        }
    }
    out
}

fn eval_ids(v: &Value, docs: &[CommittedDoc]) -> HashMap<usize, f32> {
    let ids: Vec<&str> = v
        .get("values")
        .and_then(Value::as_array)
        .map(|a| a.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    docs.iter()
        .enumerate()
        .filter(|(_, d)| ids.contains(&d.id.as_str()))
        .map(|(i, _)| (i, 1.0))
        .collect()
}

fn clauses(v: &Value, key: &str) -> Vec<Value> {
    match v.get(key) {
        Some(Value::Array(a)) => a.clone(),
        Some(other) => vec![other.clone()],
        None => vec![],
    }
}

fn eval_bool(
    v: &Value,
    mappings: &Value,
    docs: &[CommittedDoc],
) -> Result<HashMap<usize, f32>, String> {
    let must = clauses(v, "must");
    let should = clauses(v, "should");
    let filter = clauses(v, "filter");
    let must_not = clauses(v, "must_not");

    let mut candidates: HashSet<usize> = (0..docs.len()).collect();
    let mut scores: HashMap<usize, f32> = HashMap::new();

    for q in &must {
        let m = eval(q, mappings, docs)?;
        candidates.retain(|i| m.contains_key(i));
        for (i, s) in &m {
            *scores.entry(*i).or_insert(0.0) += s;
        }
    }
    for q in &filter {
        let m = eval(q, mappings, docs)?;
        candidates.retain(|i| m.contains_key(i));
    }
    for q in &must_not {
        let m = eval(q, mappings, docs)?;
        candidates.retain(|i| !m.contains_key(i));
    }
    if !should.is_empty() {
        let default_msm = if must.is_empty() && filter.is_empty() { 1 } else { 0 };
        let msm = v.get("minimum_should_match").and_then(Value::as_i64).unwrap_or(default_msm);
        let mut should_count: HashMap<usize, i64> = HashMap::new();
        for q in &should {
            for (i, s) in eval(q, mappings, docs)? {
                *scores.entry(i).or_insert(0.0) += s;
                *should_count.entry(i).or_insert(0) += 1;
            }
        }
        if msm > 0 {
            candidates.retain(|i| should_count.get(i).copied().unwrap_or(0) >= msm);
        }
    }
    scores.retain(|i, _| candidates.contains(i));
    // Filter and must_not clauses don't score: a bool with nothing else
    // scores 0, as in Elasticsearch.
    for i in &candidates {
        scores.entry(*i).or_insert(0.0);
    }
    Ok(scores)
}

/// Evaluates a Query DSL clause, returning matched document indices (into
/// `docs`) mapped to their score contribution. Structured queries (`term`,
/// `range`, `exists`, `prefix`, `ids`, `wildcard`, `regexp`) score as a
/// constant (their boost, 1.0 by default) the way Elasticsearch's
/// `ConstantScoreQuery` does; `match_all`, `match`, `match_phrase` and
/// `multi_match` produce a graded, BM25-backed score.
pub fn eval(
    query: &Value,
    mappings: &Value,
    docs: &[CommittedDoc],
) -> Result<HashMap<usize, f32>, String> {
    let Some(obj) = query.as_object() else {
        return Err("query must be an object".to_string());
    };
    if obj.contains_key("match_all") {
        return Ok((0..docs.len()).map(|i| (i, 1.0)).collect());
    }
    if obj.contains_key("match_none") {
        return Ok(HashMap::new());
    }
    if let Some(v) = obj.get("term") {
        return Ok(eval_term(v, mappings, docs));
    }
    if let Some(v) = obj.get("terms") {
        return Ok(eval_terms(v, mappings, docs));
    }
    if let Some(v) = obj.get("match") {
        return Ok(eval_match(v, mappings, docs));
    }
    if let Some(v) = obj.get("match_phrase") {
        return Ok(eval_match_phrase(v, mappings, docs));
    }
    if let Some(v) = obj.get("multi_match") {
        return Ok(eval_multi_match(v, mappings, docs));
    }
    if let Some(v) = obj.get("wildcard") {
        return Ok(eval_wildcard(v, mappings, docs));
    }
    if let Some(v) = obj.get("regexp") {
        return Ok(eval_regexp(v, mappings, docs));
    }
    if let Some(v) = obj.get("range") {
        return Ok(eval_range(v, mappings, docs));
    }
    if let Some(v) = obj.get("exists") {
        return Ok(eval_exists(v, docs));
    }
    if let Some(v) = obj.get("prefix") {
        return Ok(eval_prefix(v, mappings, docs));
    }
    if let Some(v) = obj.get("ids") {
        return Ok(eval_ids(v, docs));
    }
    if let Some(v) = obj.get("bool") {
        return eval_bool(v, mappings, docs);
    }
    if let Some(v) = obj.get("constant_score") {
        let inner = v.get("filter").cloned().unwrap_or_else(|| json!({"match_all":{}}));
        let boost = v.get("boost").and_then(Value::as_f64).unwrap_or(1.0) as f32;
        return Ok(eval(&inner, mappings, docs)?.into_keys().map(|k| (k, boost)).collect());
    }
    // Found via testing before a public release: this used to fall
    // through to `HashMap::new()` -- a real client sending a query type
    // this engine doesn't understand (`query_string`, `nested`, `fuzzy`,
    // `function_score`, ...) got a perfectly well-formed "0 hits"
    // response instead of an error, silently wrong rather than loudly
    // unsupported. See `docs/specs/README.md`'s own stated principle:
    // "never silently wrong."
    let clause = obj.keys().next().map(String::as_str).unwrap_or("<empty>");
    Err(format!("no [{clause}] query registered"))
}

fn parse_sort(s: &Value) -> (String, String) {
    match s {
        Value::String(f) => (f.clone(), "asc".to_string()),
        Value::Object(o) => {
            let Some((k, v)) = o.iter().next() else {
                return ("_score".to_string(), "desc".to_string());
            };
            let order = if let Some(order) = v.as_str() {
                order.to_string()
            } else {
                v.get("order").and_then(Value::as_str).unwrap_or("asc").to_string()
            };
            (k.clone(), order)
        }
        _ => ("_score".to_string(), "desc".to_string()),
    }
}

fn compare_field(a: Option<&Value>, b: Option<&Value>) -> Ordering {
    match (a, b) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Greater,
        (Some(_), None) => Ordering::Less,
        (Some(Value::Number(_)), Some(Value::Number(_))) => {
            number_cmp(a.unwrap(), b.unwrap()).unwrap_or(Ordering::Equal)
        }
        (Some(Value::String(x)), Some(Value::String(y))) => x.cmp(y),
        (Some(Value::Bool(x)), Some(Value::Bool(y))) => x.cmp(y),
        _ => Ordering::Equal,
    }
}

fn sort_ranked(ranked: &mut [(usize, f32)], spec: &Value, mappings: &Value, docs: &[CommittedDoc]) {
    let sorts: Vec<Value> = spec.as_array().cloned().unwrap_or_else(|| vec![spec.clone()]);
    ranked.sort_by(|a, b| {
        for s in &sorts {
            let (field, order) = parse_sort(s);
            let field = resolve_field(mappings, &field).0;
            let ord = if field == "_score" {
                b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal)
            } else if field == "_doc" {
                a.0.cmp(&b.0)
            } else {
                let av = raw_values(&docs[a.0].source, &field).into_iter().next();
                let bv = raw_values(&docs[b.0].source, &field).into_iter().next();
                compare_field(av, bv)
            };
            let ord = if order == "desc" { ord.reverse() } else { ord };
            if ord != Ordering::Equal {
                return ord;
            }
        }
        a.0.cmp(&b.0)
    });
}

fn pick_fields(source: &Value, fields: &[String]) -> Value {
    let mut m = Map::new();
    if let Value::Object(src) = source {
        for (k, v) in src {
            if fields.iter().any(|f| field_pattern_matches(f, k)) {
                m.insert(k.clone(), v.clone());
            }
        }
    }
    Value::Object(m)
}

/// A `_source` include/exclude pattern: an exact field, a `prefix*`
/// wildcard, or a dotted path naming a field inside an object (matched at
/// its top-level key).
fn field_pattern_matches(pattern: &str, key: &str) -> bool {
    match pattern.strip_suffix('*') {
        Some(prefix) => key.starts_with(prefix),
        None => pattern == key || pattern.split('.').next() == Some(key),
    }
}

pub fn filter_source(source: &Value, filter: Option<&Value>) -> Value {
    apply_source_filter(source, filter)
}

fn apply_source_filter(source: &Value, filter: Option<&Value>) -> Value {
    match filter {
        None => source.clone(),
        Some(Value::Bool(false)) => Value::Null,
        Some(Value::Bool(true)) => source.clone(),
        Some(Value::String(s)) => pick_fields(source, std::slice::from_ref(s)),
        Some(Value::Array(a)) => {
            let fields: Vec<String> =
                a.iter().filter_map(|v| v.as_str().map(str::to_string)).collect();
            pick_fields(source, &fields)
        }
        Some(Value::Object(o)) => {
            let includes: Vec<String> = o
                .get("includes")
                .and_then(Value::as_array)
                .map(|a| a.iter().filter_map(|v| v.as_str().map(str::to_string)).collect())
                .unwrap_or_default();
            let excludes: Vec<String> = o
                .get("excludes")
                .and_then(Value::as_array)
                .map(|a| a.iter().filter_map(|v| v.as_str().map(str::to_string)).collect())
                .unwrap_or_default();
            let mut result =
                if includes.is_empty() { source.clone() } else { pick_fields(source, &includes) };
            if let Value::Object(m) = &mut result {
                m.retain(|k, _| {
                    !excludes.iter().any(|e| field_pattern_matches(e, k) && !e.contains('.'))
                });
            }
            result
        }
        _ => source.clone(),
    }
}

// --- Aggregations -----------------------------------------------------
//
// Metric aggregations reduce a bucket (a set of document indices) to a
// number; bucket aggregations partition a bucket into named sub-buckets,
// each of which can itself carry sub-aggregations, recursively.

/// The values a field contributes to an aggregation: raw (typed) scalars
/// for `keyword`/numeric/boolean fields and the implicit `.keyword`
/// sub-field, analyzed terms (as strings) for `text` fields — so a `terms`
/// aggregation bucket key comes back as the right JSON type.
fn agg_values(mappings: &Value, source: &Value, field: &str) -> Vec<Value> {
    let (path, ty) = resolve_field(mappings, field);
    let ty = ty.as_deref();
    let mut out = Vec::new();
    for v in raw_values(source, &path) {
        match v {
            Value::String(s) => {
                if ty == Some("keyword") {
                    out.push(Value::String(s.clone()));
                } else {
                    out.extend(analysis::standard(s).into_iter().map(Value::String));
                }
            }
            Value::Number(_) | Value::Bool(_) => out.push(v.clone()),
            _ => {}
        }
    }
    out
}

fn numeric_values(
    mappings: &Value,
    docs: &[CommittedDoc],
    bucket: &[usize],
    field: &str,
) -> Vec<f64> {
    bucket
        .iter()
        .flat_map(|&i| agg_values(mappings, &docs[i].source, field))
        .filter_map(|v| v.as_f64())
        .collect()
}

fn agg_field(spec: &Value) -> &str {
    spec.get("field").and_then(Value::as_str).unwrap_or("")
}

fn sub_aggs_of(spec: &Value) -> Option<&Value> {
    spec.get("aggs").or_else(|| spec.get("aggregations"))
}

fn with_sub_aggs(
    mut bucket_obj: Map<String, Value>,
    spec: &Value,
    mappings: &Value,
    docs: &[CommittedDoc],
    idxs: &[usize],
) -> Value {
    if let Some(sa) = sub_aggs_of(spec)
        && let Value::Object(computed) = eval_aggs(sa, mappings, docs, idxs)
    {
        for (k, v) in computed {
            bucket_obj.insert(k, v);
        }
    }
    Value::Object(bucket_obj)
}

fn terms_agg(spec: &Value, mappings: &Value, docs: &[CommittedDoc], bucket: &[usize]) -> Value {
    let inner = spec.get("terms").cloned().unwrap_or_default();
    let field = agg_field(&inner);
    let size = inner.get("size").and_then(Value::as_u64).unwrap_or(10) as usize;
    let mut buckets: HashMap<String, (Value, Vec<usize>)> = HashMap::new();
    for &idx in bucket {
        let mut seen = HashSet::new();
        for v in agg_values(mappings, &docs[idx].source, field) {
            let key = value_to_term(&v);
            if seen.insert(key.clone()) {
                buckets.entry(key).or_insert_with(|| (v, Vec::new())).1.push(idx);
            }
        }
    }
    let mut entries: Vec<(Value, Vec<usize>)> = buckets.into_values().collect();
    entries.sort_by(|a, b| {
        b.1.len().cmp(&a.1.len()).then_with(|| value_to_term(&a.0).cmp(&value_to_term(&b.0)))
    });
    let sum_other: usize = entries.iter().skip(size).map(|(_, idxs)| idxs.len()).sum();
    let out_buckets: Vec<Value> = entries
        .into_iter()
        .take(size)
        .map(|(key, idxs)| {
            let mut b = Map::new();
            b.insert("key".to_string(), key);
            b.insert("doc_count".to_string(), json!(idxs.len()));
            with_sub_aggs(b, spec, mappings, docs, &idxs)
        })
        .collect();
    json!({"doc_count_error_upper_bound": 0, "sum_other_doc_count": sum_other, "buckets": out_buckets})
}

fn range_agg(spec: &Value, mappings: &Value, docs: &[CommittedDoc], bucket: &[usize]) -> Value {
    let inner = spec.get("range").cloned().unwrap_or_default();
    let field = agg_field(&inner);
    let ranges = inner.get("ranges").and_then(Value::as_array).cloned().unwrap_or_default();
    let keyed = inner.get("keyed").and_then(Value::as_bool).unwrap_or(false);
    let mut named = Vec::new();
    for r in &ranges {
        let from = r.get("from").and_then(Value::as_f64);
        let to = r.get("to").and_then(Value::as_f64);
        let idxs: Vec<usize> = bucket
            .iter()
            .copied()
            .filter(|&idx| {
                agg_values(mappings, &docs[idx].source, field).iter().any(|v| {
                    let Some(n) = v.as_f64() else { return false };
                    from.is_none_or(|f| n >= f) && to.is_none_or(|t| n < t)
                })
            })
            .collect();
        let mut b = Map::new();
        // Elasticsearch always keys a range bucket: its own `key`, else
        // `from-to` with `*` for an open end (`*-100.0`, `100.0-200.0`).
        let fmt = |x: Option<f64>| x.map_or("*".to_string(), |v| format!("{v:?}"));
        let key = Some(
            r.get("key")
                .and_then(Value::as_str)
                .map(str::to_string)
                .unwrap_or_else(|| format!("{}-{}", fmt(from), fmt(to))),
        );
        if let Some(k) = &key {
            b.insert("key".to_string(), json!(k));
        }
        if let Some(f) = from {
            b.insert("from".to_string(), json!(f));
        }
        if let Some(t) = to {
            b.insert("to".to_string(), json!(t));
        }
        b.insert("doc_count".to_string(), json!(idxs.len()));
        named.push((key, with_sub_aggs(b, spec, mappings, docs, &idxs)));
    }
    if keyed {
        let map: Map<String, Value> = named
            .into_iter()
            .enumerate()
            .map(|(i, (k, v))| (k.unwrap_or_else(|| i.to_string()), v))
            .collect();
        json!({"buckets": map})
    } else {
        json!({"buckets": named.into_iter().map(|(_, v)| v).collect::<Vec<_>>()})
    }
}

fn histogram_agg(spec: &Value, mappings: &Value, docs: &[CommittedDoc], bucket: &[usize]) -> Value {
    let inner = spec.get("histogram").cloned().unwrap_or_default();
    let field = agg_field(&inner);
    let interval =
        inner.get("interval").and_then(Value::as_f64).unwrap_or(1.0).max(f64::MIN_POSITIVE);
    let min_doc_count = inner.get("min_doc_count").and_then(Value::as_u64).unwrap_or(1);
    let mut buckets: HashMap<i64, Vec<usize>> = HashMap::new();
    for &idx in bucket {
        for v in agg_values(mappings, &docs[idx].source, field) {
            if let Some(n) = v.as_f64() {
                buckets.entry((n / interval).floor() as i64).or_default().push(idx);
            }
        }
    }
    let mut keys: Vec<i64> = buckets.keys().copied().collect();
    keys.sort_unstable();
    let out: Vec<Value> = keys
        .into_iter()
        .filter_map(|k| {
            let idxs = buckets.remove(&k).unwrap_or_default();
            if (idxs.len() as u64) < min_doc_count {
                return None;
            }
            let mut b = Map::new();
            b.insert("key".to_string(), json!(k as f64 * interval));
            b.insert("doc_count".to_string(), json!(idxs.len()));
            Some(with_sub_aggs(b, spec, mappings, docs, &idxs))
        })
        .collect();
    json!({"buckets": out})
}

fn filter_agg(spec: &Value, mappings: &Value, docs: &[CommittedDoc], bucket: &[usize]) -> Value {
    let filter_query = spec.get("filter").cloned().unwrap_or_else(|| json!({"match_all":{}}));
    // The aggregation pipeline doesn't thread a `Result` the way the main
    // `_search` query does (see `eval`'s own doc comment) -- an
    // unsupported query type inside a `filter` aggregation's own filter
    // falls back to "matches nothing" here rather than a real error. A
    // real gap, smaller in practice than the main-query case this was
    // found and fixed for.
    let matched = eval(&filter_query, mappings, docs).unwrap_or_default();
    let idxs: Vec<usize> = bucket.iter().copied().filter(|i| matched.contains_key(i)).collect();
    let mut b = Map::new();
    b.insert("doc_count".to_string(), json!(idxs.len()));
    with_sub_aggs(b, spec, mappings, docs, &idxs)
}

fn named_filter_bucket(
    filter_query: &Value,
    spec: &Value,
    mappings: &Value,
    docs: &[CommittedDoc],
    bucket: &[usize],
) -> Value {
    let matched = eval(filter_query, mappings, docs).unwrap_or_default();
    let idxs: Vec<usize> = bucket.iter().copied().filter(|i| matched.contains_key(i)).collect();
    let mut b = Map::new();
    b.insert("doc_count".to_string(), json!(idxs.len()));
    with_sub_aggs(b, spec, mappings, docs, &idxs)
}

fn filters_agg(spec: &Value, mappings: &Value, docs: &[CommittedDoc], bucket: &[usize]) -> Value {
    let filters =
        spec.get("filters").and_then(|v| v.get("filters")).cloned().unwrap_or_else(|| json!({}));
    match &filters {
        Value::Object(map) => {
            let out: Map<String, Value> = map
                .iter()
                .map(|(name, q)| {
                    (name.clone(), named_filter_bucket(q, spec, mappings, docs, bucket))
                })
                .collect();
            json!({"buckets": out})
        }
        Value::Array(arr) => {
            let out: Vec<Value> =
                arr.iter().map(|q| named_filter_bucket(q, spec, mappings, docs, bucket)).collect();
            json!({"buckets": out})
        }
        _ => json!({"buckets": {}}),
    }
}

fn missing_agg(spec: &Value, mappings: &Value, docs: &[CommittedDoc], bucket: &[usize]) -> Value {
    let inner = spec.get("missing").cloned().unwrap_or_default();
    let field = agg_field(&inner);
    let idxs: Vec<usize> = bucket
        .iter()
        .copied()
        .filter(|&i| raw_values(&docs[i].source, field).into_iter().all(|v| v.is_null()))
        .collect();
    let mut b = Map::new();
    b.insert("doc_count".to_string(), json!(idxs.len()));
    with_sub_aggs(b, spec, mappings, docs, &idxs)
}

fn metric_agg(
    spec: &Value,
    mappings: &Value,
    docs: &[CommittedDoc],
    bucket: &[usize],
    f: impl Fn(&[f64]) -> Option<f64>,
) -> Value {
    let vals = numeric_values(mappings, docs, bucket, agg_field(spec));
    json!({"value": f(&vals)})
}

fn stats_agg(spec: &Value, mappings: &Value, docs: &[CommittedDoc], bucket: &[usize]) -> Value {
    let vals = numeric_values(mappings, docs, bucket, agg_field(spec));
    let count = vals.len();
    let sum: f64 = vals.iter().sum();
    if count == 0 {
        return json!({"count": 0, "min": null, "max": null, "avg": null, "sum": 0.0});
    }
    let min = vals.iter().cloned().fold(f64::INFINITY, f64::min);
    let max = vals.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    json!({"count": count, "min": min, "max": max, "avg": sum / count as f64, "sum": sum})
}

fn value_count_agg(
    spec: &Value,
    mappings: &Value,
    docs: &[CommittedDoc],
    bucket: &[usize],
) -> Value {
    let field = agg_field(spec);
    let count: usize =
        bucket.iter().map(|&i| agg_values(mappings, &docs[i].source, field).len()).sum();
    json!({"value": count})
}

fn cardinality_agg(
    spec: &Value,
    mappings: &Value,
    docs: &[CommittedDoc],
    bucket: &[usize],
) -> Value {
    let field = agg_field(spec);
    let mut seen = HashSet::new();
    for &i in bucket {
        for v in agg_values(mappings, &docs[i].source, field) {
            seen.insert(value_to_term(&v));
        }
    }
    json!({"value": seen.len()})
}

fn top_hits_agg(spec: &Value, mappings: &Value, docs: &[CommittedDoc], bucket: &[usize]) -> Value {
    let size = spec.get("size").and_then(Value::as_u64).unwrap_or(3) as usize;
    let mut ranked: Vec<(usize, f32)> = bucket.iter().map(|&i| (i, 0.0)).collect();
    if let Some(sort) = spec.get("sort") {
        sort_ranked(&mut ranked, sort, mappings, docs);
    }
    let hits: Vec<Value> = ranked
        .iter()
        .take(size)
        .map(|&(i, _)| {
            let d = &docs[i];
            let src = apply_source_filter(&d.source, spec.get("_source"));
            json!({"_index": d.index, "_id": d.id, "_score": Value::Null, "_source": src})
        })
        .collect();
    json!({"hits": {"total": {"value": bucket.len(), "relation": "eq"}, "max_score": Value::Null, "hits": hits}})
}

fn eval_agg(spec: &Value, mappings: &Value, docs: &[CommittedDoc], bucket: &[usize]) -> Value {
    // These take the full outer `spec` (not just the type-specific inner
    // object) because a sibling `aggs`/`aggregations` key holding nested
    // sub-aggregations lives on the outer object, not inside e.g. `terms`.
    if spec.get("terms").is_some() {
        return terms_agg(spec, mappings, docs, bucket);
    }
    if spec.get("range").is_some() {
        return range_agg(spec, mappings, docs, bucket);
    }
    if spec.get("histogram").is_some() {
        return histogram_agg(spec, mappings, docs, bucket);
    }
    if spec.get("filter").is_some() {
        return filter_agg(spec, mappings, docs, bucket);
    }
    if spec.get("filters").is_some() {
        return filters_agg(spec, mappings, docs, bucket);
    }
    if spec.get("missing").is_some() {
        return missing_agg(spec, mappings, docs, bucket);
    }
    if let Some(v) = spec.get("avg") {
        return metric_agg(v, mappings, docs, bucket, |vals| {
            (!vals.is_empty()).then(|| vals.iter().sum::<f64>() / vals.len() as f64)
        });
    }
    if let Some(v) = spec.get("sum") {
        return metric_agg(v, mappings, docs, bucket, |vals| Some(vals.iter().sum()));
    }
    if let Some(v) = spec.get("min") {
        return metric_agg(v, mappings, docs, bucket, |vals| {
            vals.iter().copied().fold(None, |acc, x| Some(acc.map_or(x, |a: f64| a.min(x))))
        });
    }
    if let Some(v) = spec.get("max") {
        return metric_agg(v, mappings, docs, bucket, |vals| {
            vals.iter().copied().fold(None, |acc, x| Some(acc.map_or(x, |a: f64| a.max(x))))
        });
    }
    if let Some(v) = spec.get("stats") {
        return stats_agg(v, mappings, docs, bucket);
    }
    if let Some(v) = spec.get("value_count") {
        return value_count_agg(v, mappings, docs, bucket);
    }
    if let Some(v) = spec.get("cardinality") {
        return cardinality_agg(v, mappings, docs, bucket);
    }
    if let Some(v) = spec.get("top_hits") {
        return top_hits_agg(v, mappings, docs, bucket);
    }
    json!({})
}

/// Evaluates every named aggregation in an `aggs`/`aggregations` block over
/// `bucket` (a set of document indices — the whole matched set for a
/// top-level `_search`, or a sub-bucket's members for a nested `aggs`).
pub fn eval_aggs(aggs: &Value, mappings: &Value, docs: &[CommittedDoc], bucket: &[usize]) -> Value {
    let Some(obj) = aggs.as_object() else { return json!({}) };
    let out: Map<String, Value> = obj
        .iter()
        .map(|(name, spec)| (name.clone(), eval_agg(spec, mappings, docs, bucket)))
        .collect();
    Value::Object(out)
}

/// `POST/GET _search`: runs the query, ranks and paginates the results,
/// and computes any `aggs`/`aggregations` over the full matched set.
pub fn search(mappings: &Value, docs: &[CommittedDoc], body: &Value) -> Result<Value, String> {
    let query = body.get("query").cloned().unwrap_or_else(|| json!({"match_all":{}}));
    let mut scores = eval(&query, mappings, docs)?;
    if let Some(min_score) = body.get("min_score").and_then(Value::as_f64) {
        let min_score = min_score as f32;
        scores.retain(|_, s| *s >= min_score);
    }
    let matched: Vec<usize> = scores.keys().copied().collect();
    let total = scores.len();
    let from = body.get("from").and_then(Value::as_u64).unwrap_or(0) as usize;
    let size = body.get("size").and_then(Value::as_u64).unwrap_or(10) as usize;

    let mut ranked: Vec<(usize, f32)> = scores.into_iter().collect();
    match body.get("sort") {
        Some(spec) => sort_ranked(&mut ranked, spec, mappings, docs),
        None => ranked
            .sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal).then(a.0.cmp(&b.0))),
    }

    let max_score = ranked.first().map(|(_, s)| *s);
    let source_filter = body.get("_source");
    let hits: Vec<Value> = ranked
        .iter()
        .skip(from)
        .take(size)
        .map(|(idx, score)| {
            let d = &docs[*idx];
            let mut hit = json!({"_index": d.index, "_id": d.id, "_score": score});
            // `"_source": false` omits the key, as Elasticsearch does.
            if !matches!(source_filter, Some(Value::Bool(false))) {
                hit["_source"] = apply_source_filter(&d.source, source_filter);
            }
            hit
        })
        .collect();

    let mut resp = json!({
        "took": 0,
        "timed_out": false,
        "_shards": {"total": 1, "successful": 1, "skipped": 0, "failed": 0},
        "hits": {
            "total": {"value": total, "relation": "eq"},
            "max_score": max_score,
            "hits": hits,
        }
    });
    if let Some(agg_spec) = body.get("aggs").or_else(|| body.get("aggregations")) {
        resp["aggregations"] = eval_aggs(agg_spec, mappings, docs, &matched);
    }
    Ok(resp)
}

/// `POST/GET _count`: the number of matching documents.
pub fn count(mappings: &Value, docs: &[CommittedDoc], body: &Value) -> Result<u64, String> {
    let query = body.get("query").cloned().unwrap_or_else(|| json!({"match_all":{}}));
    Ok(eval(&query, mappings, docs)?.len() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doc(index: &str, id: &str, source: Value) -> CommittedDoc {
        CommittedDoc { index: index.to_string(), id: id.to_string(), source, version: 1 }
    }

    fn mappings_with_text(field: &str) -> Value {
        json!({"properties": {field: {"type": "text"}}})
    }

    #[test]
    fn match_all_scores_every_doc_one() {
        let docs = vec![doc("i", "1", json!({"a":1})), doc("i", "2", json!({"a":2}))];
        let m = eval(&json!({"match_all": {}}), &json!({}), &docs).unwrap();
        assert_eq!(m.len(), 2);
        assert!(m.values().all(|&s| s == 1.0));
    }

    #[test]
    fn term_query_is_exact_and_case_sensitive_on_keyword() {
        let mappings = json!({"properties": {"status": {"type": "keyword"}}});
        let docs = vec![
            doc("i", "1", json!({"status":"Active"})),
            doc("i", "2", json!({"status":"active"})),
        ];
        let m = eval(&json!({"term": {"status": "Active"}}), &mappings, &docs).unwrap();
        assert_eq!(m.len(), 1);
        assert!(m.contains_key(&0));
    }

    #[test]
    fn match_query_ranks_more_relevant_doc_first() {
        let mappings = mappings_with_text("body");
        let docs = vec![
            doc("i", "1", json!({"body":"the quick fox"})),
            doc("i", "2", json!({"body":"quick quick quick fox fox"})),
        ];
        let resp = search(&mappings, &docs, &json!({"query":{"match":{"body":"quick"}}})).unwrap();
        let hits = resp["hits"]["hits"].as_array().unwrap();
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0]["_id"], "2");
        assert!(hits[0]["_score"].as_f64().unwrap() > hits[1]["_score"].as_f64().unwrap());
    }

    #[test]
    fn bool_must_and_must_not_and_filter() {
        let mappings = json!({"properties": {"tag": {"type": "keyword"}}});
        let docs = vec![
            doc("i", "1", json!({"tag":"a","n":1})),
            doc("i", "2", json!({"tag":"a","n":2})),
            doc("i", "3", json!({"tag":"b","n":1})),
        ];
        let q = json!({"bool": {
            "filter": [{"term": {"tag": "a"}}],
            "must_not": [{"range": {"n": {"gt": 1}}}]
        }});
        let m = eval(&q, &mappings, &docs).unwrap();
        assert_eq!(m.len(), 1);
        assert!(m.contains_key(&0));
    }

    #[test]
    fn range_query_matches_numeric_bounds() {
        let docs = vec![doc("i", "1", json!({"n":5})), doc("i", "2", json!({"n":15}))];
        let m = eval(&json!({"range": {"n": {"gte": 10}}}), &json!({}), &docs).unwrap();
        assert_eq!(m.len(), 1);
        assert!(m.contains_key(&1));
    }

    #[test]
    fn sort_by_field_overrides_score_order() {
        let docs = vec![doc("i", "1", json!({"n":5})), doc("i", "2", json!({"n":1}))];
        let resp = search(&json!({}), &docs, &json!({"sort": [{"n": {"order": "asc"}}]})).unwrap();
        let hits = resp["hits"]["hits"].as_array().unwrap();
        assert_eq!(hits[0]["_id"], "2");
        assert_eq!(hits[1]["_id"], "1");
    }

    #[test]
    fn from_and_size_paginate() {
        let docs: Vec<_> = (0..5).map(|i| doc("i", &i.to_string(), json!({"n": i}))).collect();
        let resp =
            search(&json!({}), &docs, &json!({"from": 2, "size": 2, "sort": ["n"]})).unwrap();
        let hits = resp["hits"]["hits"].as_array().unwrap();
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0]["_id"], "2");
        assert_eq!(hits[1]["_id"], "3");
        assert_eq!(resp["hits"]["total"]["value"], 5);
    }

    #[test]
    fn source_filtering_includes_only_requested_fields() {
        let docs = vec![doc("i", "1", json!({"a":1,"b":2}))];
        let resp = search(&json!({}), &docs, &json!({"_source": ["a"]})).unwrap();
        assert_eq!(resp["hits"]["hits"][0]["_source"], json!({"a":1}));
    }

    fn agg_docs() -> Vec<CommittedDoc> {
        vec![
            doc("i", "1", json!({"tag":"a","price":10})),
            doc("i", "2", json!({"tag":"a","price":20})),
            doc("i", "3", json!({"tag":"b","price":30})),
            doc("i", "4", json!({"price":40})),
        ]
    }

    #[test]
    fn terms_aggregation_buckets_by_keyword_field_with_sub_metric() {
        let mappings = json!({"properties": {"tag": {"type": "keyword"}}});
        let docs = agg_docs();
        let resp = search(
            &mappings,
            &docs,
            &json!({"size":0,"aggs":{"by_tag":{"terms":{"field":"tag"},"aggs":{"avg_price":{"avg":{"field":"price"}}}}}}),
        ).unwrap();
        assert_eq!(resp["hits"]["hits"].as_array().unwrap().len(), 0);
        let buckets = resp["aggregations"]["by_tag"]["buckets"].as_array().unwrap();
        assert_eq!(buckets[0]["key"], "a");
        assert_eq!(buckets[0]["doc_count"], 2);
        assert_eq!(buckets[0]["avg_price"]["value"], 15.0);
        assert_eq!(buckets[1]["key"], "b");
        assert_eq!(buckets[1]["doc_count"], 1);
    }

    #[test]
    fn stats_and_cardinality_aggregations() {
        let mappings = json!({"properties": {"tag": {"type": "keyword"}}});
        let docs = agg_docs();
        let resp = search(
            &mappings,
            &docs,
            &json!({"aggs":{"price_stats":{"stats":{"field":"price"}},"distinct_tags":{"cardinality":{"field":"tag"}}}}),
        ).unwrap();
        assert_eq!(resp["aggregations"]["price_stats"]["count"], 4);
        assert_eq!(resp["aggregations"]["price_stats"]["min"], 10.0);
        assert_eq!(resp["aggregations"]["price_stats"]["max"], 40.0);
        assert_eq!(resp["aggregations"]["price_stats"]["sum"], 100.0);
        assert_eq!(resp["aggregations"]["distinct_tags"]["value"], 2);
    }

    #[test]
    fn range_and_missing_aggregations() {
        let docs = agg_docs();
        let resp = search(
            &json!({}),
            &docs,
            &json!({"aggs":{
                "by_price":{"range":{"field":"price","ranges":[{"to":25},{"from":25}]}},
                "no_tag":{"missing":{"field":"tag"}}
            }}),
        )
        .unwrap();
        let buckets = resp["aggregations"]["by_price"]["buckets"].as_array().unwrap();
        assert_eq!(buckets[0]["doc_count"], 2);
        assert_eq!(buckets[1]["doc_count"], 2);
        assert_eq!(resp["aggregations"]["no_tag"]["doc_count"], 1);
    }

    #[test]
    fn range_and_missing_aggregations_carry_sub_aggregations() {
        // `aggs` is a sibling of `range`/`missing` in the request body, not
        // nested inside them — regression test for a bug where sub-aggs
        // were looked up on the wrong (inner) object and silently dropped.
        let docs = agg_docs();
        let resp = search(
            &json!({}),
            &docs,
            &json!({"aggs":{
                "by_price":{
                    "range":{"field":"price","ranges":[{"to":25},{"from":25}]},
                    "aggs":{"avg_price":{"avg":{"field":"price"}}}
                },
                "no_tag":{
                    "missing":{"field":"tag"},
                    "aggs":{"avg_price":{"avg":{"field":"price"}}}
                }
            }}),
        )
        .unwrap();
        let buckets = resp["aggregations"]["by_price"]["buckets"].as_array().unwrap();
        assert_eq!(buckets[0]["avg_price"]["value"], 15.0);
        assert_eq!(buckets[1]["avg_price"]["value"], 35.0);
        assert_eq!(resp["aggregations"]["no_tag"]["avg_price"]["value"], 40.0);
    }

    #[test]
    fn filter_and_filters_aggregations_restrict_the_bucket() {
        let mappings = json!({"properties": {"tag": {"type": "keyword"}}});
        let docs = agg_docs();
        let resp = search(
            &mappings,
            &docs,
            &json!({"aggs":{
                "tag_a":{"filter":{"term":{"tag":"a"}}},
                "by_tag":{"filters":{"filters":{"a":{"term":{"tag":"a"}},"b":{"term":{"tag":"b"}}}}}
            }}),
        )
        .unwrap();
        assert_eq!(resp["aggregations"]["tag_a"]["doc_count"], 2);
        assert_eq!(resp["aggregations"]["by_tag"]["buckets"]["a"]["doc_count"], 2);
        assert_eq!(resp["aggregations"]["by_tag"]["buckets"]["b"]["doc_count"], 1);
    }
}
