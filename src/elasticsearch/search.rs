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

fn mapped_type<'a>(mappings: &'a Value, field: &str) -> Option<&'a str> {
    mappings.get("properties")?.get(field)?.get("type")?.as_str()
}

/// Walks a dotted field path through nested objects, flattening arrays
/// along the way, the way Elasticsearch resolves e.g. `"meta.source"`.
fn navigate<'a>(v: &'a Value, path: &[&str]) -> Vec<&'a Value> {
    if path.is_empty() {
        return vec![v];
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
    if let Some(base) = field.strip_suffix(".keyword") {
        return raw_values(source, base)
            .into_iter()
            .filter_map(Value::as_str)
            .filter(|s| s.chars().count() <= 256)
            .map(str::to_string)
            .collect();
    }
    let ty = mapped_type(mappings, field);
    let mut out = Vec::new();
    for v in raw_values(source, field) {
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

fn eval_bool(v: &Value, mappings: &Value, docs: &[CommittedDoc]) -> HashMap<usize, f32> {
    let must = clauses(v, "must");
    let should = clauses(v, "should");
    let filter = clauses(v, "filter");
    let must_not = clauses(v, "must_not");

    let mut candidates: HashSet<usize> = (0..docs.len()).collect();
    let mut scores: HashMap<usize, f32> = HashMap::new();

    for q in &must {
        let m = eval(q, mappings, docs);
        candidates.retain(|i| m.contains_key(i));
        for (i, s) in &m {
            *scores.entry(*i).or_insert(0.0) += s;
        }
    }
    for q in &filter {
        let m = eval(q, mappings, docs);
        candidates.retain(|i| m.contains_key(i));
    }
    for q in &must_not {
        let m = eval(q, mappings, docs);
        candidates.retain(|i| !m.contains_key(i));
    }
    if !should.is_empty() {
        let default_msm = if must.is_empty() && filter.is_empty() { 1 } else { 0 };
        let msm = v.get("minimum_should_match").and_then(Value::as_i64).unwrap_or(default_msm);
        let mut should_count: HashMap<usize, i64> = HashMap::new();
        for q in &should {
            for (i, s) in eval(q, mappings, docs) {
                *scores.entry(i).or_insert(0.0) += s;
                *should_count.entry(i).or_insert(0) += 1;
            }
        }
        if msm > 0 {
            candidates.retain(|i| should_count.get(i).copied().unwrap_or(0) >= msm);
        }
    }
    scores.retain(|i, _| candidates.contains(i));
    for i in &candidates {
        scores.entry(*i).or_insert(1.0);
    }
    scores
}

/// Evaluates a Query DSL clause, returning matched document indices (into
/// `docs`) mapped to their score contribution. Structured queries (`term`,
/// `range`, `exists`, `prefix`, `ids`) score as a constant (their boost, 1.0
/// by default) the way Elasticsearch's `ConstantScoreQuery` does; only
/// `match_all` and `match` produce a graded, BM25-backed score.
pub fn eval(query: &Value, mappings: &Value, docs: &[CommittedDoc]) -> HashMap<usize, f32> {
    let Some(obj) = query.as_object() else { return HashMap::new() };
    if obj.contains_key("match_all") {
        return (0..docs.len()).map(|i| (i, 1.0)).collect();
    }
    if obj.contains_key("match_none") {
        return HashMap::new();
    }
    if let Some(v) = obj.get("term") {
        return eval_term(v, mappings, docs);
    }
    if let Some(v) = obj.get("terms") {
        return eval_terms(v, mappings, docs);
    }
    if let Some(v) = obj.get("match") {
        return eval_match(v, mappings, docs);
    }
    if let Some(v) = obj.get("range") {
        return eval_range(v, mappings, docs);
    }
    if let Some(v) = obj.get("exists") {
        return eval_exists(v, docs);
    }
    if let Some(v) = obj.get("prefix") {
        return eval_prefix(v, mappings, docs);
    }
    if let Some(v) = obj.get("ids") {
        return eval_ids(v, docs);
    }
    if let Some(v) = obj.get("bool") {
        return eval_bool(v, mappings, docs);
    }
    if let Some(v) = obj.get("constant_score") {
        let inner = v.get("filter").cloned().unwrap_or_else(|| json!({"match_all":{}}));
        let boost = v.get("boost").and_then(Value::as_f64).unwrap_or(1.0) as f32;
        return eval(&inner, mappings, docs).into_keys().map(|k| (k, boost)).collect();
    }
    HashMap::new()
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

fn sort_ranked(ranked: &mut [(usize, f32)], spec: &Value, docs: &[CommittedDoc]) {
    let sorts: Vec<Value> = spec.as_array().cloned().unwrap_or_else(|| vec![spec.clone()]);
    ranked.sort_by(|a, b| {
        for s in &sorts {
            let (field, order) = parse_sort(s);
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
        for f in fields {
            if let Some(v) = src.get(f) {
                m.insert(f.clone(), v.clone());
            }
        }
    }
    Value::Object(m)
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
                for e in &excludes {
                    m.remove(e);
                }
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
    if let Some(base) = field.strip_suffix(".keyword") {
        return raw_values(source, base).into_iter().filter(|v| v.is_string()).cloned().collect();
    }
    let ty = mapped_type(mappings, field);
    let mut out = Vec::new();
    for v in raw_values(source, field) {
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
        let key = r.get("key").and_then(Value::as_str).map(str::to_string);
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
    let matched = eval(&filter_query, mappings, docs);
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
    let matched = eval(filter_query, mappings, docs);
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

fn top_hits_agg(spec: &Value, docs: &[CommittedDoc], bucket: &[usize]) -> Value {
    let size = spec.get("size").and_then(Value::as_u64).unwrap_or(3) as usize;
    let hits: Vec<Value> = bucket
        .iter()
        .take(size)
        .map(|&i| {
            let d = &docs[i];
            json!({"_index": d.index, "_id": d.id, "_score": Value::Null, "_source": d.source})
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
        return top_hits_agg(v, docs, bucket);
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
pub fn search(mappings: &Value, docs: &[CommittedDoc], body: &Value) -> Value {
    let query = body.get("query").cloned().unwrap_or_else(|| json!({"match_all":{}}));
    let mut scores = eval(&query, mappings, docs);
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
        Some(spec) => sort_ranked(&mut ranked, spec, docs),
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
            json!({
                "_index": d.index,
                "_id": d.id,
                "_score": score,
                "_source": apply_source_filter(&d.source, source_filter),
            })
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
    resp
}

/// `POST/GET _count`: the number of matching documents.
pub fn count(mappings: &Value, docs: &[CommittedDoc], body: &Value) -> u64 {
    let query = body.get("query").cloned().unwrap_or_else(|| json!({"match_all":{}}));
    eval(&query, mappings, docs).len() as u64
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
        let m = eval(&json!({"match_all": {}}), &json!({}), &docs);
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
        let m = eval(&json!({"term": {"status": "Active"}}), &mappings, &docs);
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
        let resp = search(&mappings, &docs, &json!({"query":{"match":{"body":"quick"}}}));
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
        let m = eval(&q, &mappings, &docs);
        assert_eq!(m.len(), 1);
        assert!(m.contains_key(&0));
    }

    #[test]
    fn range_query_matches_numeric_bounds() {
        let docs = vec![doc("i", "1", json!({"n":5})), doc("i", "2", json!({"n":15}))];
        let m = eval(&json!({"range": {"n": {"gte": 10}}}), &json!({}), &docs);
        assert_eq!(m.len(), 1);
        assert!(m.contains_key(&1));
    }

    #[test]
    fn sort_by_field_overrides_score_order() {
        let docs = vec![doc("i", "1", json!({"n":5})), doc("i", "2", json!({"n":1}))];
        let resp = search(&json!({}), &docs, &json!({"sort": [{"n": {"order": "asc"}}]}));
        let hits = resp["hits"]["hits"].as_array().unwrap();
        assert_eq!(hits[0]["_id"], "2");
        assert_eq!(hits[1]["_id"], "1");
    }

    #[test]
    fn from_and_size_paginate() {
        let docs: Vec<_> = (0..5).map(|i| doc("i", &i.to_string(), json!({"n": i}))).collect();
        let resp = search(&json!({}), &docs, &json!({"from": 2, "size": 2, "sort": ["n"]}));
        let hits = resp["hits"]["hits"].as_array().unwrap();
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0]["_id"], "2");
        assert_eq!(hits[1]["_id"], "3");
        assert_eq!(resp["hits"]["total"]["value"], 5);
    }

    #[test]
    fn source_filtering_includes_only_requested_fields() {
        let docs = vec![doc("i", "1", json!({"a":1,"b":2}))];
        let resp = search(&json!({}), &docs, &json!({"_source": ["a"]}));
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
        );
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
        );
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
        );
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
        );
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
        );
        assert_eq!(resp["aggregations"]["tag_a"]["doc_count"], 2);
        assert_eq!(resp["aggregations"]["by_tag"]["buckets"]["a"]["doc_count"], 2);
        assert_eq!(resp["aggregations"]["by_tag"]["buckets"]["b"]["doc_count"], 1);
    }
}
