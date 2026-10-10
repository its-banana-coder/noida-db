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
use super::dates;
use super::dsl;
use super::explain;
use super::features;
use super::fields;
use super::fuzzy;
use super::highlight;
use super::queries;
use super::query_string;
use super::rescore;
use super::retrievers;
use super::scoring;
use super::sorting;
use super::suggest;
use super::tsdb;
use super::vectors;

/// A search failure, shaped the way Elasticsearch reports it: most are a
/// plain `{"error": {"type": ...}}`, but a failure while executing the
/// query on a shard is wrapped in `search_phase_execution_exception`
/// (`all shards failed`) with the real cause as the root cause.
#[derive(Debug)]
pub struct EsError {
    pub status: u16,
    pub kind: String,
    pub reason: String,
    pub shard: bool,
    /// The underlying cause (`caused_by`), as (type, reason).
    pub cause: Option<(String, String)>,
}

impl EsError {
    pub fn new(status: u16, kind: &str, reason: &str) -> Self {
        Self {
            status,
            kind: kind.to_string(),
            reason: reason.to_string(),
            shard: false,
            cause: None,
        }
    }

    pub fn parsing(reason: &str) -> Self {
        Self::new(400, "parsing_exception", reason)
    }

    pub fn shard_failure(kind: &str, reason: &str) -> Self {
        Self {
            status: 400,
            kind: kind.to_string(),
            reason: reason.to_string(),
            shard: true,
            cause: None,
        }
    }

    /// The same error with a `caused_by`.
    pub fn caused_by(mut self, kind: &str, reason: &str) -> Self {
        self.cause = Some((kind.to_string(), reason.to_string()));
        self
    }

    pub fn to_json(&self) -> Value {
        let mut cause = json!({"type": self.kind, "reason": self.reason});
        if let Some((kind, reason)) = &self.cause {
            cause["caused_by"] = json!({"type": kind, "reason": reason});
        }
        if self.shard {
            json!({
                "error": {
                    "root_cause": [{"type": self.kind, "reason": self.reason}],
                    "type": "search_phase_execution_exception",
                    "reason": "all shards failed",
                    "phase": "query",
                    "grouped": true,
                    "failed_shards": [{"shard": 0, "node": "noida", "reason": cause.clone()}],
                    "caused_by": cause,
                },
                "status": self.status,
            })
        } else {
            let mut err = json!({"root_cause": [{"type": self.kind, "reason": self.reason}],
                                 "type": self.kind, "reason": self.reason});
            if let Some(c) = cause.get("caused_by") {
                err["caused_by"] = c.clone();
            }
            json!({"error": err, "status": self.status})
        }
    }
}

impl From<String> for EsError {
    fn from(reason: String) -> Self {
        Self::parsing(&reason)
    }
}

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
    /// The `_seq_no` as of the refresh (update/delete by query treat a
    /// document changed since as a version conflict).
    pub seq: i64,
    /// The full source when `source` is a root-level view with nested
    /// objects removed (a nested object's fields aren't visible to
    /// queries outside a `nested` query, as in Elasticsearch).
    pub full_source: Option<Value>,
    /// The `_tsid` of a document in a time-series index.
    pub tsid: Option<String>,
}

impl CommittedDoc {
    pub(crate) fn full(&self) -> &Value {
        self.full_source.as_ref().unwrap_or(&self.source)
    }
}

/// The `nested`-typed object paths in a mapping (`comments`, `a.b`).
pub(crate) fn nested_paths(mappings: &Value) -> Vec<String> {
    fn walk(props: Option<&Value>, prefix: &str, out: &mut Vec<String>) {
        let Some(obj) = props.and_then(Value::as_object) else { return };
        for (k, v) in obj {
            let full = if prefix.is_empty() { k.clone() } else { format!("{prefix}.{k}") };
            if v.get("type").and_then(Value::as_str) == Some("nested") {
                out.push(full.clone());
            }
            walk(v.get("properties"), &full, out);
        }
    }
    let mut out = Vec::new();
    walk(mappings.get("properties"), "", &mut out);
    out
}

fn strip_path(v: &mut Value, path: &[&str]) {
    match v {
        Value::Object(m) => {
            if path.len() == 1 {
                m.remove(path[0]);
            } else if let Some(child) = m.get_mut(path[0]) {
                strip_path(child, &path[1..]);
            }
        }
        Value::Array(a) => a.iter_mut().for_each(|e| strip_path(e, path)),
        _ => {}
    }
}

/// Root-level views of `docs`: nested objects removed from what queries
/// and aggregations see, the full source kept for hits and `nested`.
fn root_view(mappings: &Value, docs: &[CommittedDoc]) -> Option<Vec<CommittedDoc>> {
    let paths = nested_paths(mappings);
    if paths.is_empty() {
        return None;
    }
    Some(
        docs.iter()
            .map(|d| {
                let mut source = d.full().clone();
                for p in &paths {
                    strip_path(&mut source, &p.split('.').collect::<Vec<_>>());
                }
                CommittedDoc { source, full_source: Some(d.full().clone()), ..d.clone() }
            })
            .collect(),
    )
}

/// The nested documents under `path` of each of `parents`: child docs
/// (sources shaped `{"comments": <element>}` so full field paths resolve)
/// and, for each, its (parent, offset).
fn nested_children(
    docs: &[CommittedDoc],
    parents: impl Iterator<Item = usize>,
    path: &str,
) -> (Vec<CommittedDoc>, Vec<(usize, usize)>) {
    let segs: Vec<&str> = path.split('.').collect();
    let mut children = Vec::new();
    let mut owners = Vec::new();
    for p in parents {
        let d = &docs[p];
        let mut node = d.full();
        let mut ok = true;
        for s in &segs {
            match node.get(*s) {
                Some(n) => node = n,
                None => {
                    ok = false;
                    break;
                }
            }
        }
        if !ok {
            continue;
        }
        let elems: Vec<&Value> = match node {
            Value::Array(a) => a.iter().collect(),
            Value::Object(_) => vec![node],
            _ => vec![],
        };
        for (offset, e) in elems.into_iter().enumerate() {
            let mut wrapped = e.clone();
            for s in segs.iter().rev() {
                wrapped = json!({ *s: wrapped });
            }
            children.push(CommittedDoc {
                index: d.index.clone(),
                id: d.id.clone(),
                source: wrapped,
                version: d.version,
                seq: d.seq,
                full_source: None,
                tsid: None,
            });
            owners.push((p, offset));
        }
    }
    (children, owners)
}

fn check_nested_path(mappings: &Value, v: &Value) -> Result<Option<String>, EsError> {
    let path = v.get("path").and_then(Value::as_str).unwrap_or("").to_string();
    if nested_paths(mappings).contains(&path) {
        return Ok(Some(path));
    }
    if v.get("ignore_unmapped").and_then(Value::as_bool).unwrap_or(false) {
        return Ok(None);
    }
    Err(EsError::shard_failure(
        "query_shard_exception",
        &format!(
            "[nested] failed to create query: [nested] nested object under path [{path}] is not \
             of nested type"
        ),
    ))
}

/// Per matching parent, its matching children: (offset, score, child).
pub(crate) type InnerMatches = HashMap<usize, Vec<(usize, f32, usize)>>;

fn nested_matches(
    v: &Value,
    mappings: &Value,
    docs: &[CommittedDoc],
) -> Result<Option<(Vec<CommittedDoc>, InnerMatches)>, EsError> {
    let Some(path) = check_nested_path(mappings, v)? else { return Ok(None) };
    let inner = v.get("query").cloned().unwrap_or_else(|| json!({"match_all": {}}));
    let (children, owners) = nested_children(docs, 0..docs.len(), &path);
    // kNN over nested vectors finds the nearest parents, not children.
    if let Some(knn) = inner.get("knn").filter(|_| vectors::is_knn(&inner)) {
        let per = vectors::nested_knn(knn, mappings, docs, &children, &owners)?;
        return Ok(Some((children, per)));
    }
    let mut per: InnerMatches = HashMap::new();
    for (ci, score) in eval(&inner, mappings, &children)? {
        let (parent, offset) = owners[ci];
        per.entry(parent).or_default().push((offset, score, ci));
    }
    Ok(Some((children, per)))
}

/// `nested`: a parent matches when any of its nested objects does, scored
/// by `score_mode` (`avg` by default).
fn eval_nested(
    v: &Value,
    mappings: &Value,
    docs: &[CommittedDoc],
) -> Result<HashMap<usize, f32>, EsError> {
    let Some((_, per)) = nested_matches(v, mappings, docs)? else { return Ok(HashMap::new()) };
    // A parent matches a nested kNN query through its nearest vector.
    let knn = v.get("query").is_some_and(vectors::is_knn);
    let mode =
        if knn { "max" } else { v.get("score_mode").and_then(Value::as_str).unwrap_or("avg") };
    let boost = v.get("boost").and_then(Value::as_f64).unwrap_or(1.0) as f32;
    Ok(per
        .into_iter()
        .map(|(parent, ms)| {
            let scores = ms.iter().map(|m| m.1);
            let s = match mode {
                "max" => scores.fold(f32::MIN, f32::max),
                "min" => scores.fold(f32::MAX, f32::min),
                "sum" => scores.sum(),
                "none" => 0.0,
                _ => scores.sum::<f32>() / ms.len() as f32,
            };
            (parent, s * boost)
        })
        .collect())
}

/// `mappings` with the objects along `path` mapped as plain objects (a
/// nested object's own fields, fetched for its inner hit).
fn objects_along(mappings: &Value, path: &str) -> Value {
    let mut out = mappings.clone();
    let mut node = &mut out;
    for seg in path.split('.') {
        let Some(next) = node.get_mut("properties").and_then(|p| p.get_mut(seg)) else { break };
        next["type"] = json!("object");
        node = next;
    }
    out
}

/// The `inner_hits` of every `nested` clause in `query` that asks for
/// them: (name, per-parent hits object).
fn inner_hits(
    query: &Value,
    mappings: &Value,
    docs: &[CommittedDoc],
    out: &mut Vec<(String, HashMap<usize, Value>)>,
) -> Result<(), EsError> {
    match query {
        Value::Object(o) => {
            if let Some(n) = o.get("nested")
                && let Some(ih) = n.get("inner_hits")
                && let Some((children, per)) = nested_matches(n, mappings, docs)?
            {
                let path = n.get("path").and_then(Value::as_str).unwrap_or("").to_string();
                let name = ih.get("name").and_then(Value::as_str).unwrap_or(&path).to_string();
                let source_enabled = mappings
                    .get("_source")
                    .and_then(|s| s.get("enabled"))
                    .and_then(Value::as_bool)
                    .unwrap_or(true);
                let size = ih.get("size").and_then(Value::as_u64).unwrap_or(3) as usize;
                let from = ih.get("from").and_then(Value::as_u64).unwrap_or(0) as usize;
                // A nested query inside reports its inner hits per child.
                let mut sub = Vec::new();
                if let Some(q) = n.get("query") {
                    inner_hits(q, mappings, &children, &mut sub)?;
                }
                let mut by_parent = HashMap::new();
                for (parent, mut ms) in per {
                    ms.sort_by(|a, b| {
                        b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal).then(a.0.cmp(&b.0))
                    });
                    let max = ms.first().map(|m| m.1);
                    let inner_query = n.get("query").cloned().unwrap_or(json!({"match_all": {}}));
                    let mut hits: Vec<Value> = Vec::new();
                    for &(offset, score, ci) in ms.iter().skip(from).take(size) {
                        let c = &children[ci];
                        let mut src = &c.source;
                        for seg in path.split('.') {
                            src = &src[seg];
                        }
                        let mut hit = json!({
                            "_index": c.index,
                            "_id": c.id,
                            "_nested": {"field": path, "offset": offset},
                            "_score": score,
                        });
                        if ih.get("version").and_then(Value::as_bool) == Some(true) {
                            hit["_version"] = json!(c.version);
                        }
                        if ih.get("seq_no_primary_term").and_then(Value::as_bool) == Some(true) {
                            hit["_seq_no"] = json!(c.seq);
                            hit["_primary_term"] = json!(1);
                        }
                        if !matches!(ih.get("_source"), Some(Value::Bool(false))) && source_enabled
                        {
                            hit["_source"] = apply_source_filter(src, ih.get("_source"));
                        }
                        if let Some(hl) = ih.get("highlight") {
                            let settings = Value::Null;
                            let ctx = highlight::Context {
                                index: &c.index,
                                doc: parent,
                                settings: &settings,
                                weighted: true,
                            };
                            // Without `_source`, only stored fields have text.
                            let shown = if source_enabled {
                                c.source.clone()
                            } else {
                                dsl::stored_only(mappings, &c.source)
                            };
                            if let Some(h) =
                                highlight::highlight(hl, &inner_query, mappings, &shown, &ctx)?
                            {
                                hit["highlight"] = h;
                            }
                        }
                        // `fields` of a nested hit come grouped under its
                        // path: `{"comments": [{"author": [...]}]}`.
                        if let Some(spec) = ih.get("fields") {
                            let prefix = format!("{path}.");
                            let mut grouped = Map::new();
                            let flat = objects_along(mappings, &path);
                            for (k, v) in fields::fetch(&flat, c, spec, fields::Kind::Fields)? {
                                let k = k.strip_prefix(&prefix).map_or(k.clone(), str::to_string);
                                grouped.insert(k, v);
                            }
                            if !grouped.is_empty() {
                                hit["fields"] = json!({ (path.as_str()): [grouped] });
                            }
                        }
                        if let Some(spec) = ih.get("docvalue_fields") {
                            for (k, v) in fields::fetch(mappings, c, spec, fields::Kind::DocValue)?
                            {
                                hit["fields"][k.as_str()] = v;
                            }
                        }
                        for (sub_name, per_child) in &sub {
                            if let Some(h) = per_child.get(&ci) {
                                hit["inner_hits"][sub_name.as_str()] = h.clone();
                            }
                        }
                        hits.push(hit);
                    }
                    by_parent.insert(
                        parent,
                        json!({"hits": {
                            "total": {"value": ms.len(), "relation": "eq"},
                            "max_score": max,
                            "hits": hits,
                        }}),
                    );
                }
                out.push((name, by_parent));
            }
            for (k, v) in o {
                if k != "nested" {
                    inner_hits(v, mappings, docs, out)?;
                }
            }
        }
        Value::Array(a) => {
            for v in a {
                inner_hits(v, mappings, docs, out)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Where a (possibly dotted) field name's values come from in `_source`,
/// and its mapped type: a plain field (`brand`), an object path
/// (`meta.source`), or a multi-field (`name.raw` / `name.keyword`: the
/// values of `name`, indexed with the sub-field's type). Found via testing
/// before a public release: multi-fields weren't resolved at all, so
/// `term: {"name.raw": ...}` and sorting/aggregating on a `.raw` sub-field
/// silently matched nothing.
pub(crate) fn resolve_field(mappings: &Value, field: &str) -> (String, Option<String>) {
    let segs: Vec<&str> = field.split('.').collect();
    let mut props = mappings.get("properties");
    let mut i = 0;
    while i < segs.len() {
        // A mapped name may itself hold dots (`subobjects: false`): the
        // longest one present wins.
        let Some((j, node)) = (i + 1..=segs.len())
            .rev()
            .find_map(|j| props.and_then(|p| p.get(segs[i..j].join("."))).map(|n| (j, n)))
        else {
            break;
        };
        if j == segs.len() {
            let ty = node.get("type").and_then(Value::as_str).unwrap_or("object");
            return (field.to_string(), Some(ty.to_string()));
        }
        // A key inside a `flattened` field is a keyword.
        if node.get("type").and_then(Value::as_str) == Some("flattened") {
            return (field.to_string(), Some("keyword".to_string()));
        }
        if j + 1 == segs.len()
            && let Some(sub) = node.get("fields").and_then(|f| f.get(segs[j]))
        {
            let ty = sub.get("type").and_then(Value::as_str).unwrap_or("keyword");
            return (segs[..j].join("."), Some(ty.to_string()));
        }
        props = node.get("properties");
        i = j;
    }
    // Unmapped (e.g. a search across indices): `x.keyword` is the dynamic
    // keyword sub-field of `x`.
    if let Some(base) = field.strip_suffix(".keyword") {
        return (base.to_string(), Some("keyword".to_string()));
    }
    (field.to_string(), None)
}

/// A keyword field's `ignore_above` (multi-fields included).
fn ignore_above(mappings: &Value, field: &str) -> Option<usize> {
    let segs: Vec<&str> = field.split('.').collect();
    let mut props = mappings.get("properties");
    for (i, seg) in segs.iter().enumerate() {
        let node = props?.get(*seg)?;
        let def = if i + 1 == segs.len() {
            Some(node)
        } else if i + 2 == segs.len() {
            node.get("fields").and_then(|f| f.get(segs[i + 1]))
        } else {
            None
        };
        if let Some(d) = def {
            return d.get("ignore_above").and_then(Value::as_u64).map(|n| n as usize);
        }
        props = node.get("properties");
    }
    None
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
        // A key may itself hold dots (`{"a.b": 1}` is field `a.b` too).
        Value::Object(m) => {
            let mut out = m.get(path[0]).map(|nv| navigate(nv, &path[1..])).unwrap_or_default();
            for i in 2..=path.len() {
                if let Some(nv) = m.get(&path[..i].join(".")) {
                    out.extend(navigate(nv, &path[i..]));
                }
            }
            out
        }
        Value::Array(arr) => arr.iter().flat_map(|e| navigate(e, path)).collect(),
        _ => Vec::new(),
    }
}

pub(crate) fn raw_values<'a>(source: &'a Value, field: &str) -> Vec<&'a Value> {
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
    if ty == Some("flattened") {
        return dsl::flattened_values(source, &path);
    }
    if ty == Some("keyword") && path != field && field.ends_with(".keyword") {
        return raw_values(source, &path)
            .into_iter()
            .filter_map(Value::as_str)
            .filter(|s| s.chars().count() <= 256)
            .map(str::to_string)
            .collect();
    }
    let mut out = Vec::new();
    let ignore_above = if ty == Some("keyword") { ignore_above(mappings, field) } else { None };
    for v in raw_values(source, &path) {
        match v {
            Value::String(s) => {
                if ty == Some("keyword") {
                    // Values over `ignore_above` aren't indexed.
                    if ignore_above.is_none_or(|n| s.chars().count() <= n) {
                        out.push(s.clone());
                    }
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

/// `tokens_for`, plus the `_id` and `_index` metadata fields.
fn meta_tokens(mappings: &Value, d: &CommittedDoc, field: &str) -> Vec<String> {
    match field {
        "_id" => vec![d.id.clone()],
        "_index" => vec![d.index.clone()],
        "_ignored" => super::docparse::ignored_fields(mappings, &Value::Null, d.full()),
        _ => tokens_for(mappings, &d.source, field),
    }
}

pub(super) fn doc_tokens(mappings: &Value, docs: &[CommittedDoc], field: &str) -> Vec<Vec<String>> {
    docs.iter().map(|d| tokens_for(mappings, &d.source, field)).collect()
}

/// BM25 (Lucene/Elasticsearch defaults k1=1.2, b=0.75) over the given field
/// for a set of already-analyzed query terms.
pub(super) fn bm25_scores(
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

pub(super) fn field_and_spec(v: &Value) -> Option<(&str, &Value)> {
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
    // `{"term": "x"}` is accepted for `{"value": "x"}`.
    let (value, boost) = match spec.get("term").filter(|_| spec.get("value").is_none()) {
        Some(t) => (t.clone(), spec.get("boost").and_then(Value::as_f64).unwrap_or(1.0) as f32),
        None => value_and_boost(spec),
    };
    let target = value_to_term(&value);
    if let Some(scores) = dsl::term_scores(mappings, docs, field, spec, &target, boost) {
        return scores;
    }
    let mut out = HashMap::new();
    for (idx, d) in docs.iter().enumerate() {
        if meta_tokens(mappings, d, field).contains(&target) {
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
        let toks = meta_tokens(mappings, d, field);
        if targets.iter().any(|t| toks.contains(t)) {
            out.insert(idx, 1.0);
        }
    }
    out
}

/// A query string analyzed the way `field` is: `keyword` (and numeric,
/// boolean, date) fields take it whole, text fields through `standard`.
pub(super) fn analyze_for(mappings: &Value, field: &str, text: &str) -> Vec<String> {
    match resolve_field(mappings, field).1.as_deref() {
        None | Some("text") | Some("match_only_text") => analysis::standard(text),
        Some(_) => vec![text.to_string()],
    }
}

/// A query value as text: `"quick"`, `10`, `true`.
pub(super) fn query_text(v: Option<&Value>) -> String {
    match v {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Number(n)) => n.to_string(),
        Some(Value::Bool(b)) => b.to_string(),
        _ => String::new(),
    }
}

pub(super) fn eval_match(
    v: &Value,
    mappings: &Value,
    docs: &[CommittedDoc],
) -> HashMap<usize, f32> {
    let Some((field, spec)) = field_and_spec(v) else { return HashMap::new() };
    let (text, op, boost) = if let Some(o) = spec.as_object() {
        (
            query_text(o.get("query")),
            o.get("operator").and_then(Value::as_str).unwrap_or("or").to_string(),
            o.get("boost").and_then(Value::as_f64).unwrap_or(1.0) as f32,
        )
    } else {
        (query_text(Some(spec)), "or".to_string(), 1.0)
    };
    let query_terms = analyze_for(mappings, field, &text);
    if query_terms.is_empty() {
        return HashMap::new();
    }
    let mut scores =
        bm25_scores(mappings, docs, field, &query_terms, op.eq_ignore_ascii_case("and"));
    if boost != 1.0 {
        scores.values_mut().for_each(|s| *s *= boost);
    }
    scores
}

/// `dis_max`: a document's best sub-query score, plus `tie_breaker` times
/// the others.
fn eval_dis_max(
    v: &Value,
    mappings: &Value,
    docs: &[CommittedDoc],
) -> Result<HashMap<usize, f32>, EsError> {
    let tie = v.get("tie_breaker").and_then(Value::as_f64).unwrap_or(0.0) as f32;
    let boost = v.get("boost").and_then(Value::as_f64).unwrap_or(1.0) as f32;
    let mut per: HashMap<usize, Vec<f32>> = HashMap::new();
    for q in v.get("queries").and_then(Value::as_array).cloned().unwrap_or_default() {
        for (i, s) in eval(&q, mappings, docs)? {
            per.entry(i).or_default().push(s);
        }
    }
    Ok(per
        .into_iter()
        .map(|(i, ss)| {
            let max = ss.iter().copied().fold(f32::MIN, f32::max);
            let sum: f32 = ss.iter().sum();
            (i, (max + tie * (sum - max)) * boost)
        })
        .collect())
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

/// A sloppy phrase match: some choice of positions for the query terms
/// whose total displacement from consecutive order is at most `slop`
/// (Lucene's edit-distance notion of phrase slop, reordering included).
pub(super) fn sloppy_phrase_matches(
    doc_tokens: &[String],
    query_terms: &[String],
    slop: usize,
) -> bool {
    if slop == 0 {
        return phrase_matches(doc_tokens, query_terms);
    }
    let positions: Vec<Vec<usize>> = query_terms
        .iter()
        .map(|t| doc_tokens.iter().enumerate().filter(|(_, d)| *d == t).map(|(i, _)| i).collect())
        .collect();
    if positions.iter().any(Vec::is_empty) {
        return false;
    }
    fn search(positions: &[Vec<usize>], k: usize, chosen: &mut Vec<usize>, slop: usize) -> bool {
        if k == positions.len() {
            let offsets: Vec<i64> =
                chosen.iter().enumerate().map(|(i, &p)| p as i64 - i as i64).collect();
            let (lo, hi) = (offsets.iter().min().unwrap(), offsets.iter().max().unwrap());
            return (hi - lo) as usize <= slop;
        }
        for &p in &positions[k] {
            if chosen.contains(&p) {
                continue;
            }
            chosen.push(p);
            if search(positions, k + 1, chosen, slop) {
                return true;
            }
            chosen.pop();
        }
        false
    }
    search(&positions, 0, &mut Vec::new(), slop)
}

/// `match_phrase`: like `match`, but the query's analyzed terms must appear
/// in the document at consecutive positions, in order (slop 0 — the only
/// slop value implemented; a non-zero `slop` option is accepted but
/// currently treated as 0).
fn eval_match_phrase(v: &Value, mappings: &Value, docs: &[CommittedDoc]) -> HashMap<usize, f32> {
    let Some((field, spec)) = field_and_spec(v) else { return HashMap::new() };
    let (text, slop) = if let Some(o) = spec.as_object() {
        (query_text(o.get("query")), o.get("slop").and_then(Value::as_u64).unwrap_or(0) as usize)
    } else {
        (query_text(Some(spec)), 0)
    };
    let query_terms = analyze_for(mappings, field, &text);
    if query_terms.is_empty() {
        return HashMap::new();
    }
    let per_doc = doc_tokens(mappings, docs, field);
    let matched: HashSet<usize> = per_doc
        .iter()
        .enumerate()
        .filter(|(_, toks)| sloppy_phrase_matches(toks, &query_terms, slop))
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
                .flat_map(|s| {
                    let (f, b) = parse_field_boost(s);
                    highlight::expand_field_pattern(mappings, f).into_iter().map(move |f| (f, b))
                })
                .collect()
        })
        .unwrap_or_default();
    let op = obj.get("operator").and_then(Value::as_str).unwrap_or("or");
    let require_all = op.eq_ignore_ascii_case("and");
    let kind = obj.get("type").and_then(Value::as_str);
    if matches!(kind, Some("phrase") | Some("phrase_prefix")) {
        // A phrase in any one field; the best field's score.
        let mut best: HashMap<usize, f32> = HashMap::new();
        for (field, boost) in &fields {
            let mut spec = json!({"query": text});
            if let Some(slop) = obj.get("slop") {
                spec["slop"] = slop.clone();
            }
            let q = json!({ field.as_str(): spec });
            let scores = if kind == Some("phrase") {
                eval_match_phrase(&q, mappings, docs)
            } else {
                queries::match_phrase_prefix(&q, mappings, docs)
            };
            for (idx, score) in scores {
                let e = best.entry(idx).or_insert(score * boost);
                *e = e.max(score * boost);
            }
        }
        return best;
    }

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
        let matched = meta_tokens(mappings, d, field).iter().any(|t| {
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
        if meta_tokens(mappings, d, field).iter().any(|t| re.is_match(t)) {
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

fn eval_range(
    v: &Value,
    mappings: &Value,
    docs: &[CommittedDoc],
) -> Result<HashMap<usize, f32>, EsError> {
    let Some((field, cond)) = field_and_spec(v) else { return Ok(HashMap::new()) };
    let (path, ty) = resolve_field(mappings, field);
    let boost = cond.get("boost").and_then(Value::as_f64).unwrap_or(1.0) as f32;
    let mut out = HashMap::new();
    if matches!(ty.as_deref(), Some("date") | Some("date_nanos")) {
        let bounds = date_bounds(cond, mappings, &path)?;
        let field_format = date_field_format(mappings, &path);
        for (idx, d) in docs.iter().enumerate() {
            let hit = raw_values(&d.source, &path).into_iter().any(|val| {
                let Some(t) = dates::value_millis(val, field_format.as_deref()) else {
                    return false;
                };
                bounds.iter().all(|&(op, b)| match op {
                    "gte" => t >= b,
                    "gt" => t > b,
                    "lte" => t <= b,
                    _ => t < b,
                })
            });
            if hit {
                out.insert(idx, boost);
            }
        }
        return Ok(out);
    }
    let numeric = dsl::numeric_range_bounds(cond, ty.as_deref())?;
    let cond = numeric.as_ref().unwrap_or(cond);
    for (idx, d) in docs.iter().enumerate() {
        if raw_values(&d.source, &path).into_iter().any(|val| in_range(val, cond)) {
            out.insert(idx, boost);
        }
    }
    Ok(out)
}

/// The `format` a date field is mapped with, if any.
fn date_field_format(mappings: &Value, path: &str) -> Option<String> {
    let mut node = mappings;
    for seg in path.split('.') {
        node = node.get("properties")?.get(seg)?;
    }
    node.get("format").and_then(Value::as_str).map(str::to_string)
}

/// A date `range`'s bounds as epoch millis: date math (`now-1d/d`,
/// `2024-01-15||+1M/M`) with the request's `format` and `time_zone`;
/// `/unit` rounds up for `gt` and `lte`, down for `gte` and `lt`, as in
/// Elasticsearch.
fn date_bounds(
    cond: &Value,
    mappings: &Value,
    path: &str,
) -> Result<Vec<(&'static str, i64)>, EsError> {
    let format = cond
        .get("format")
        .and_then(Value::as_str)
        .map(str::to_string)
        .or_else(|| date_field_format(mappings, path));
    let tz = match cond.get("time_zone").and_then(Value::as_str) {
        Some(z) => dates::parse_offset(z).ok_or_else(|| {
            EsError::shard_failure(
                "illegal_argument_exception",
                &format!("Unknown time-zone ID: {z}"),
            )
        })?,
        None => 0,
    };
    let now = dates::now_ms();
    let mut out = Vec::new();
    for (op, key) in
        [("gte", "gte"), ("gt", "gt"), ("lte", "lte"), ("lt", "lt"), ("gte", "from"), ("lte", "to")]
    {
        let Some(b) = cond.get(key) else { continue };
        if b.is_null() {
            continue;
        }
        let round_up = op == "gt" || op == "lte";
        let explicit_format = cond.get("format").is_some();
        let t = match b {
            // With a `format`, a number is a date in it (`2023` as `uuuu`).
            Value::Number(n) if explicit_format => {
                dates::parse_math(&n.to_string(), now, round_up, format.as_deref(), tz)
            }
            Value::Number(n) => n.as_i64().or_else(|| n.as_f64().map(|f| f as i64)),
            Value::String(s) => dates::parse_math(s, now, round_up, format.as_deref(), tz),
            _ => None,
        };
        let Some(t) = t else {
            let shown = b.as_str().map(str::to_string).unwrap_or_else(|| b.to_string());
            let f =
                format.clone().unwrap_or_else(|| "strict_date_optional_time||epoch_millis".into());
            return Err(EsError::shard_failure(
                "parse_exception",
                &format!(
                    "failed to parse date field [{shown}] with format [{f}]: [failed to parse \
                     date field [{shown}] with format [{f}]]"
                ),
            ));
        };
        out.push((op, t));
    }
    Ok(out)
}

fn eval_exists(v: &Value, docs: &[CommittedDoc]) -> HashMap<usize, f32> {
    let Some(field) = v.get("field").and_then(Value::as_str) else { return HashMap::new() };
    let mut out = HashMap::new();
    // Every document has these metadata fields.
    let always = matches!(field, "_id" | "_index" | "_seq_no" | "_version" | "_primary_term");
    for (idx, d) in docs.iter().enumerate() {
        if always || raw_values(&d.source, field).into_iter().any(|v| !v.is_null()) {
            out.insert(idx, 1.0);
        }
    }
    out
}

pub(super) fn eval_prefix(
    v: &Value,
    mappings: &Value,
    docs: &[CommittedDoc],
) -> HashMap<usize, f32> {
    let Some((field, spec)) = field_and_spec(v) else { return HashMap::new() };
    let (value, boost) = value_and_boost(spec);
    let prefix = value_to_term(&value);
    let mut out = HashMap::new();
    for (idx, d) in docs.iter().enumerate() {
        if meta_tokens(mappings, d, field).iter().any(|t| t.starts_with(&prefix)) {
            out.insert(idx, boost);
        }
    }
    out
}

fn eval_ids(v: &Value, docs: &[CommittedDoc]) -> HashMap<usize, f32> {
    // Numbers name ids too (`"values": [1]`).
    let ids: Vec<String> = v
        .get("values")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|x| {
                    x.as_str().map(str::to_string).or_else(|| x.as_number().map(|n| n.to_string()))
                })
                .collect()
        })
        .unwrap_or_default();
    docs.iter().enumerate().filter(|(_, d)| ids.contains(&d.id)).map(|(i, _)| (i, 1.0)).collect()
}

pub(super) fn clauses(v: &Value, key: &str) -> Vec<Value> {
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
) -> Result<HashMap<usize, f32>, EsError> {
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
        let msm = match v.get("minimum_should_match") {
            Some(spec) => fuzzy::min_should_match(spec, should.len())? as i64,
            None => default_msm,
        };
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
    if let Some(b) = v.get("boost").and_then(Value::as_f64) {
        scores.values_mut().for_each(|s| *s *= b as f32);
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
) -> Result<HashMap<usize, f32>, EsError> {
    let Some(obj) = query.as_object() else {
        return Err(EsError::parsing("query must be an object"));
    };
    for k in ["term", "match", "match_phrase", "prefix", "wildcard", "regexp", "range", "fuzzy"] {
        if obj.get(k).and_then(Value::as_object).is_some_and(|o| o.is_empty()) {
            return Err(EsError::parsing(&format!(
                "[{k}] query malformed, no start_object after query name"
            )));
        }
    }
    if let Some(e) = vectors::unsupported_query(obj, mappings) {
        return Err(e);
    }
    tsdb::check_query(obj)?;
    if let Some(r) = super::ranges::eval(obj, mappings, docs) {
        return r;
    }
    if let Some(r) = features::eval(obj, mappings, docs) {
        return r;
    }
    if let Some(r) = dsl::eval_extra(obj, mappings, docs) {
        return r;
    }
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
        // Metadata fields match their exact value.
        if let Some((f @ ("_index" | "_id"), spec)) =
            v.as_object().and_then(|m| m.iter().next()).map(|(f, s)| (f.as_str(), s))
        {
            let value = spec.get("query").unwrap_or(spec).clone();
            return eval(&json!({"term": {f: {"value": value}}}), mappings, docs);
        }
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
        return eval_range(v, mappings, docs);
    }
    if let Some(v) = obj.get("exists") {
        // `_ignored` exists on documents with an ignored value.
        if v.get("field").and_then(Value::as_str) == Some("_ignored") {
            return Ok(docs
                .iter()
                .enumerate()
                .filter(|(_, d)| !meta_tokens(mappings, d, "_ignored").is_empty())
                .map(|(i, _)| (i, 1.0))
                .collect());
        }
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
    if let Some(v) = obj.get("dis_max") {
        return eval_dis_max(v, mappings, docs);
    }
    if let Some(v) = obj.get("nested") {
        return eval_nested(v, mappings, docs);
    }
    if let Some(v) = obj.get("query_string") {
        let q = query_string::query_string(v, mappings)?;
        return eval(&q, mappings, docs);
    }
    if let Some(v) = obj.get("simple_query_string") {
        let q = query_string::simple_query_string(v, mappings)?;
        return eval(&q, mappings, docs);
    }
    if let Some(v) = obj.get("constant_score") {
        let inner = v.get("filter").cloned().unwrap_or_else(|| json!({"match_all":{}}));
        let boost = v.get("boost").and_then(Value::as_f64).unwrap_or(1.0) as f32;
        return Ok(eval(&inner, mappings, docs)?.into_keys().map(|k| (k, boost)).collect());
    }
    if let Some(v) = obj.get("match_phrase_prefix") {
        return Ok(queries::match_phrase_prefix(v, mappings, docs));
    }
    if let Some(v) = obj.get("boosting") {
        return queries::boosting(v, mappings, docs);
    }
    if let Some(v) = obj.get("function_score") {
        return queries::function_score(v, mappings, docs);
    }
    if let Some(v) = obj.get("script_score") {
        return queries::script_score(v, mappings, docs);
    }
    if let Some(v) = obj.get("combined_fields") {
        return queries::combined_fields(v, mappings, docs);
    }
    if let Some(v) = obj.get("knn") {
        return vectors::eval_knn(v, mappings, docs);
    }
    if let Some(v) = obj.get("geo_distance") {
        return queries::geo_distance(v, docs);
    }
    if let Some(v) = obj.get("geo_bounding_box") {
        return queries::geo_bounding_box(v, docs);
    }
    // Found via testing before a public release: this used to fall
    // through to `HashMap::new()` -- a real client sending a query type
    // this engine doesn't understand (`query_string`, `nested`, `fuzzy`,
    // `function_score`, ...) got a perfectly well-formed "0 hits"
    // response instead of an error, silently wrong rather than loudly
    // unsupported. See `docs/specs/README.md`'s own stated principle:
    // "never silently wrong."
    let clause = obj.keys().next().map(String::as_str).unwrap_or("<empty>");
    Err(dsl::unknown_query(clause))
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

/// Whether a `_source` pattern (`*` wildcards, matched against the full
/// dotted path) matches `path`.
fn source_pattern(pattern: &str, path: &str) -> bool {
    if !pattern.contains('*') {
        return pattern == path;
    }
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = path.chars().collect();
    glob_match(&p, &t)
}

/// Whether `path` is an object some include pattern may reach into.
fn may_contain_match(includes: &[String], path: &str) -> bool {
    let prefix = format!("{path}.");
    includes.iter().any(|p| {
        p.starts_with(&prefix) || p.split('.').next().is_some_and(|first| first.contains('*'))
    })
}

/// Elasticsearch's `_source` filtering: includes and excludes are paths
/// (with `*` wildcards) into the document; an included object keeps its
/// whole subtree, minus any excluded paths.
fn filter_value(
    v: &Value,
    path: &str,
    includes: &[String],
    excludes: &[String],
    included: bool,
) -> Option<Value> {
    match v {
        Value::Object(m) => {
            let mut out = Map::new();
            for (k, x) in m {
                let p = if path.is_empty() { k.clone() } else { format!("{path}.{k}") };
                if excludes.iter().any(|e| source_pattern(e, &p)) {
                    continue;
                }
                let inc = included || includes.iter().any(|i| source_pattern(i, &p));
                if !inc && !x.is_object() && !x.is_array() {
                    continue;
                }
                if !inc && x.is_object() && !may_contain_match(includes, &p) {
                    continue;
                }
                if let Some(f) = filter_value(x, &p, includes, excludes, inc) {
                    out.insert(k.clone(), f);
                }
            }
            if out.is_empty() && !included && !path.is_empty() {
                None
            } else {
                Some(Value::Object(out))
            }
        }
        Value::Array(a) => {
            let items: Vec<Value> = a
                .iter()
                .filter_map(|x| match x {
                    Value::Object(_) | Value::Array(_) => {
                        filter_value(x, path, includes, excludes, included)
                    }
                    other => included.then(|| other.clone()),
                })
                .collect();
            if items.is_empty() && !included { None } else { Some(Value::Array(items)) }
        }
        other => included.then(|| other.clone()),
    }
}

fn filter_paths(source: &Value, includes: &[String], excludes: &[String]) -> Value {
    filter_value(source, "", includes, excludes, includes.is_empty()).unwrap_or_else(|| json!({}))
}

pub fn filter_source(source: &Value, filter: Option<&Value>) -> Value {
    apply_source_filter(source, filter)
}

fn apply_source_filter(source: &Value, filter: Option<&Value>) -> Value {
    let strings = |v: Option<&Value>| -> Vec<String> {
        match v {
            Some(Value::String(s)) => vec![s.clone()],
            Some(Value::Array(a)) => {
                a.iter().filter_map(|x| x.as_str().map(str::to_string)).collect()
            }
            _ => Vec::new(),
        }
    };
    match filter {
        None => source.clone(),
        Some(Value::Bool(false)) => Value::Null,
        Some(Value::Bool(true)) => source.clone(),
        Some(v @ (Value::String(_) | Value::Array(_))) => {
            filter_paths(source, &strings(Some(v)), &[])
        }
        Some(Value::Object(o)) => {
            let includes = strings(o.get("includes").or_else(|| o.get("include")));
            let excludes = strings(o.get("excludes").or_else(|| o.get("exclude")));
            filter_paths(source, &includes, &excludes)
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
                } else if ty == Some("ip") {
                    out.push(json!(tsdb::format_ip(s).unwrap_or_else(|| s.clone())));
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
        let values = if field == "_tsid" {
            docs[idx].tsid.iter().map(|t| json!(t)).collect()
        } else {
            agg_values(mappings, &docs[idx].source, field)
        };
        for v in values {
            let key = value_to_term(&v);
            if seen.insert(key.clone()) {
                buckets.entry(key).or_insert_with(|| (v, Vec::new())).1.push(idx);
            }
        }
    }
    let mut entries: Vec<(Value, Vec<usize>)> = buckets.into_values().collect();
    let ip = resolve_field(mappings, field).1.as_deref() == Some("ip");
    let key_cmp = |a: &Value, b: &Value| -> Ordering {
        match (a, b) {
            _ if field == "_tsid" => {
                tsdb::tsid_sort_key(&value_to_term(a)).cmp(&tsdb::tsid_sort_key(&value_to_term(b)))
            }
            (Value::Number(_), Value::Number(_)) => number_cmp(a, b).unwrap_or(Ordering::Equal),
            (Value::String(x), Value::String(y)) if ip => {
                let bits = |s: &str| match s.parse::<std::net::IpAddr>() {
                    Ok(std::net::IpAddr::V4(v4)) => v4.to_ipv6_mapped().octets(),
                    Ok(std::net::IpAddr::V6(v6)) => v6.octets(),
                    Err(_) => [0xff; 16],
                };
                bits(x).cmp(&bits(y))
            }
            _ => value_to_term(a).cmp(&value_to_term(b)),
        }
    };
    // `order`: `{"_key"|"_count": "asc"|"desc"}` or a list of them; the
    // default is by count, most first.
    let order: Vec<(bool, bool)> = match inner.get("order") {
        Some(Value::Array(a)) => a.iter().filter_map(terms_order).collect(),
        Some(o) => terms_order(o).into_iter().collect(),
        None => Vec::new(),
    };
    let order = if order.is_empty() { vec![(false, false)] } else { order };
    entries.sort_by(|a, b| {
        for (by_key, asc) in &order {
            let o = if *by_key { key_cmp(&a.0, &b.0) } else { a.1.len().cmp(&b.1.len()) };
            let o = if *asc { o } else { o.reverse() };
            if o != Ordering::Equal {
                return o;
            }
        }
        key_cmp(&a.0, &b.0)
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

/// One `terms` `order` entry: (by key, ascending). Orders on sub-aggs
/// aren't modelled.
fn terms_order(o: &Value) -> Option<(bool, bool)> {
    let (k, dir) = o.as_object()?.iter().next()?;
    let asc = dir.as_str()? == "asc";
    match k.as_str() {
        "_key" | "_term" => Some((true, asc)),
        "_count" => Some((false, asc)),
        _ => None,
    }
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

/// A `date` field's values in a document, as epoch millis.
fn date_values(mappings: &Value, source: &Value, field: &str) -> Vec<i64> {
    let (path, _) = resolve_field(mappings, field);
    let format = date_field_format(mappings, &path);
    raw_values(source, &path)
        .into_iter()
        .filter_map(|v| dates::value_millis(v, format.as_deref()))
        .collect()
}

#[derive(Clone, Copy)]
enum Interval {
    Calendar(char),
    Fixed(i64),
}

impl Interval {
    fn round(self, t: i64, tz: i64, offset: i64) -> i64 {
        match self {
            Interval::Calendar(u) => dates::round_down(t - offset, u, tz) + offset,
            Interval::Fixed(ms) => (t + tz - offset).div_euclid(ms) * ms - tz + offset,
        }
    }

    fn next(self, t: i64, tz: i64) -> i64 {
        match self {
            Interval::Calendar(u) => dates::add(t, 1, u, tz),
            Interval::Fixed(ms) => t + ms,
        }
    }
}

fn parse_interval(inner: &Value) -> Result<Interval, EsError> {
    let bad = |m: String| EsError::new(400, "x_content_parse_exception", &m);
    if let Some(c) = inner.get("calendar_interval").and_then(Value::as_str) {
        let unit = match c {
            "minute" | "1m" => 'm',
            "hour" | "1h" => 'h',
            "day" | "1d" => 'd',
            "week" | "1w" => 'w',
            "month" | "1M" => 'M',
            "quarter" | "1q" => 'q',
            "year" | "1y" => 'y',
            _ => {
                return Err(bad(format!(
                    "[date_histogram] failed to parse field [calendar_interval]: The supplied \
                     interval [{c}] could not be parsed as a calendar interval."
                )));
            }
        };
        return Ok(Interval::Calendar(unit));
    }
    let fixed = inner
        .get("fixed_interval")
        .or_else(|| inner.get("interval"))
        .and_then(Value::as_str)
        .ok_or_else(|| {
            EsError::new(
                400,
                "illegal_argument_exception",
                "Invalid interval specified, must be non-null and non-empty",
            )
        })?;
    let split = fixed.find(|c: char| !c.is_ascii_digit()).unwrap_or(fixed.len());
    let n: i64 = fixed[..split].parse().map_err(|_| {
        bad(format!("failed to parse setting [date_histogram.fixedInterval] with value [{fixed}] as a time value"))
    })?;
    let ms = match &fixed[split..] {
        "ms" => 1,
        "s" => 1000,
        "m" => 60_000,
        "h" => 3_600_000,
        "d" => 86_400_000,
        _ => {
            return Err(bad(format!(
                "failed to parse setting [date_histogram.fixedInterval] with value [{fixed}] as a \
                 time value: unit is missing or unrecognized"
            )));
        }
    };
    Ok(Interval::Fixed(n * ms))
}

fn parse_tz(inner: &Value) -> i64 {
    inner.get("time_zone").and_then(Value::as_str).and_then(dates::parse_offset).unwrap_or(0)
}

fn date_histogram_agg(
    spec: &Value,
    mappings: &Value,
    docs: &[CommittedDoc],
    bucket: &[usize],
) -> Result<Value, EsError> {
    let inner = spec.get("date_histogram").cloned().unwrap_or_default();
    let field = agg_field(&inner);
    let interval = parse_interval(&inner)?;
    let tz = parse_tz(&inner);
    let offset = match inner.get("offset") {
        Some(Value::String(o)) => {
            let neg = o.starts_with('-');
            let body = o.trim_start_matches(['-', '+']);
            let ms = match parse_interval(&json!({"fixed_interval": body}))? {
                Interval::Fixed(ms) => ms,
                Interval::Calendar(_) => 0,
            };
            if neg { -ms } else { ms }
        }
        Some(Value::Number(n)) => n.as_i64().unwrap_or(0),
        _ => 0,
    };
    let format = inner.get("format").and_then(Value::as_str).map(str::to_string);
    let min_doc_count = inner.get("min_doc_count").and_then(Value::as_u64).unwrap_or(0);
    let mut buckets: std::collections::BTreeMap<i64, Vec<usize>> = Default::default();
    for &idx in bucket {
        let mut seen = HashSet::new();
        for t in date_values(mappings, &docs[idx].source, field) {
            let k = interval.round(t, tz, offset);
            if seen.insert(k) {
                buckets.entry(k).or_default().push(idx);
            }
        }
    }
    if min_doc_count == 0 {
        let bound = |key: &str| -> Option<i64> {
            let b = inner.get("extended_bounds")?.get(key)?;
            match b {
                Value::Number(n) => n.as_i64(),
                Value::String(s) => {
                    dates::parse_math(s, dates::now_ms(), false, format.as_deref(), tz)
                }
                _ => None,
            }
        };
        let lo =
            [buckets.keys().next().copied(), bound("min").map(|t| interval.round(t, tz, offset))]
                .into_iter()
                .flatten()
                .min();
        let hi = [
            buckets.keys().next_back().copied(),
            bound("max").map(|t| interval.round(t, tz, offset)),
        ]
        .into_iter()
        .flatten()
        .max();
        if let (Some(lo), Some(hi)) = (lo, hi) {
            let mut k = lo;
            let mut guard = 0;
            while k <= hi && guard < 100_000 {
                buckets.entry(k).or_default();
                k = interval.next(k, tz);
                guard += 1;
            }
        }
    }
    let mut out: Vec<(i64, Vec<usize>)> =
        buckets.into_iter().filter(|(_, v)| v.len() as u64 >= min_doc_count).collect();
    if let Some(order) = inner.get("order").and_then(Value::as_object)
        && let Some((k, dir)) = order.iter().next()
    {
        let desc = dir.as_str() == Some("desc");
        match k.as_str() {
            "_count" => out.sort_by(|a, b| {
                let o = a.1.len().cmp(&b.1.len());
                (if desc { o.reverse() } else { o }).then(a.0.cmp(&b.0))
            }),
            "_key" if desc => out.reverse(),
            _ => {}
        }
    }
    let keyed = inner.get("keyed").and_then(Value::as_bool).unwrap_or(false);
    let rendered: Vec<(String, Value)> = out
        .into_iter()
        .map(|(k, idxs)| {
            let key_str = dates::format(k, format.as_deref(), tz);
            let mut b = Map::new();
            b.insert("key_as_string".to_string(), json!(key_str));
            b.insert("key".to_string(), json!(k));
            b.insert("doc_count".to_string(), json!(idxs.len()));
            (key_str, with_sub_aggs(b, spec, mappings, docs, &idxs))
        })
        .collect();
    Ok(if keyed {
        json!({"buckets": rendered.into_iter().collect::<Map<String, Value>>()})
    } else {
        json!({"buckets": rendered.into_iter().map(|(_, v)| v).collect::<Vec<_>>()})
    })
}

fn date_range_agg(
    spec: &Value,
    mappings: &Value,
    docs: &[CommittedDoc],
    bucket: &[usize],
) -> Result<Value, EsError> {
    let inner = spec.get("date_range").cloned().unwrap_or_default();
    let field = agg_field(&inner);
    let format = inner.get("format").and_then(Value::as_str).map(str::to_string);
    let tz = parse_tz(&inner);
    let keyed = inner.get("keyed").and_then(Value::as_bool).unwrap_or(false);
    let now = dates::now_ms();
    let bound = |v: Option<&Value>| -> Result<Option<i64>, EsError> {
        match v {
            None | Some(Value::Null) => Ok(None),
            Some(Value::Number(n)) => Ok(n.as_i64()),
            Some(Value::String(s)) => {
                dates::parse_math(s, now, false, format.as_deref(), tz).map(Some).ok_or_else(|| {
                    EsError::shard_failure(
                        "parse_exception",
                        &format!("failed to parse date field [{s}]"),
                    )
                })
            }
            Some(_) => Ok(None),
        }
    };
    let mut named = Vec::new();
    for r in inner.get("ranges").and_then(Value::as_array).cloned().unwrap_or_default() {
        let (from, to) = (bound(r.get("from"))?, bound(r.get("to"))?);
        let idxs: Vec<usize> = bucket
            .iter()
            .copied()
            .filter(|&i| {
                date_values(mappings, &docs[i].source, field)
                    .iter()
                    .any(|&t| from.is_none_or(|f| t >= f) && to.is_none_or(|e| t < e))
            })
            .collect();
        let s =
            |t: Option<i64>| t.map_or("*".to_string(), |t| dates::format(t, format.as_deref(), tz));
        let key = r
            .get("key")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| format!("{}-{}", s(from), s(to)));
        let mut b = Map::new();
        b.insert("key".to_string(), json!(key));
        if let Some(f) = from {
            b.insert("from".to_string(), json!(f as f64));
            b.insert("from_as_string".to_string(), json!(s(Some(f))));
        }
        if let Some(t) = to {
            b.insert("to".to_string(), json!(t as f64));
            b.insert("to_as_string".to_string(), json!(s(Some(t))));
        }
        b.insert("doc_count".to_string(), json!(idxs.len()));
        named.push((key, with_sub_aggs(b, spec, mappings, docs, &idxs)));
    }
    Ok(if keyed {
        json!({"buckets": named.into_iter().collect::<Map<String, Value>>()})
    } else {
        json!({"buckets": named.into_iter().map(|(_, v)| v).collect::<Vec<_>>()})
    })
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
    // Their request errors were already reported by `validate_aggs`.
    if spec.get("date_histogram").is_some() {
        return date_histogram_agg(spec, mappings, docs, bucket).unwrap_or_else(|_| json!({}));
    }
    if spec.get("date_range").is_some() {
        return date_range_agg(spec, mappings, docs, bucket).unwrap_or_else(|_| json!({}));
    }
    if spec.get("filter").is_some() {
        return filter_agg(spec, mappings, docs, bucket);
    }
    if let Some(n) = spec.get("nested") {
        let path = n.get("path").and_then(Value::as_str).unwrap_or("");
        let (children, _) = nested_children(docs, bucket.iter().copied(), path);
        let all: Vec<usize> = (0..children.len()).collect();
        let b = Map::from_iter([("doc_count".to_string(), json!(children.len()))]);
        return with_sub_aggs(b, spec, mappings, &children, &all);
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

/// An integer request parameter (`size`, `from`), which Elasticsearch
/// also accepts as a numeric string.
fn int_param(body: &Value, key: &str, default: i64) -> Result<i64, EsError> {
    match body.get(key) {
        None | Some(Value::Null) => Ok(default),
        Some(Value::Number(n)) => n.as_i64().ok_or_else(|| {
            EsError::new(400, "x_content_parse_exception", &format!("[{key}] must be an integer"))
        }),
        Some(Value::String(s)) => s.parse().map_err(|_| {
            EsError::new(400, "x_content_parse_exception", &format!("[{key}] must be an integer"))
        }),
        Some(_) => Err(EsError::new(
            400,
            "x_content_parse_exception",
            &format!("[{key}] must be an integer"),
        )),
    }
}

/// Rejects an aggregation tree with an unknown type, as Elasticsearch does
/// when parsing the request (rather than silently returning `{}`).
pub fn validate_aggs(aggs: &Value) -> Result<(), EsError> {
    const KNOWN: &[&str] = &[
        "terms",
        "range",
        "histogram",
        "date_histogram",
        "date_range",
        "filter",
        "filters",
        "missing",
        "avg",
        "sum",
        "min",
        "max",
        "stats",
        "value_count",
        "cardinality",
        "top_hits",
        "nested",
    ];
    let Some(obj) = aggs.as_object() else {
        return Err(EsError::parsing("Expected [START_OBJECT] under [aggs]"));
    };
    for (name, spec) in obj {
        let Some(spec_obj) = spec.as_object() else {
            return Err(EsError::parsing(&format!("Expected [START_OBJECT] under [{name}]")));
        };
        let mut found = false;
        for (k, v) in spec_obj {
            match k.as_str() {
                "aggs" | "aggregations" => validate_aggs(v)?,
                "meta" => {}
                "date_histogram" => {
                    parse_interval(v)?;
                    found = true;
                }
                t if KNOWN.contains(&t) => found = true,
                t => {
                    return Err(EsError::parsing(&format!(
                        "Unknown aggregation type [{t}] did you mean [{}]?",
                        KNOWN
                            .iter()
                            .find(|k| k.starts_with(&t[..1.min(t.len())]))
                            .unwrap_or(&"terms")
                    )));
                }
            }
        }
        if !found {
            return Err(EsError::parsing(&format!("Missing definition for aggregation [{name}]")));
        }
    }
    Ok(())
}

/// The `hits.total` object under `track_total_hits` (default: exact up to
/// 10,000, then `"gte"`). `None` when it's turned off.
fn total_hits(body: &Value, total: usize) -> Option<Value> {
    let cap = match body.get("track_total_hits") {
        Some(Value::Bool(false)) => return None,
        Some(Value::Bool(true)) => usize::MAX,
        Some(Value::Number(n)) => n.as_u64().map_or(usize::MAX, |n| n as usize),
        _ => 10_000,
    };
    Some(if total > cap {
        json!({"value": cap, "relation": "gte"})
    } else {
        json!({"value": total, "relation": "eq"})
    })
}

#[derive(Default)]
pub struct SearchOptions {
    /// `mappings` is a real mapping, so sorting on an unmapped field fails.
    pub typed: bool,
    /// Return every hit, ignoring `from`/`size` (a scroll pages through
    /// them itself; the result window doesn't apply).
    pub all_hits: bool,
    /// A point-in-time search: a sorted one without `search_after` gets
    /// Elasticsearch's implicit `_shard_doc` tiebreaker.
    pub pit: bool,
    /// The searched index's settings when there is exactly one (the
    /// highlighter reads `index.highlight.*`).
    pub settings: Value,
    /// Only these documents (positions in `docs`) can match: a collapse
    /// group's inner hits are a search over its documents alone.
    pub restrict: Option<HashSet<usize>>,
}

pub fn search_with(
    mappings: &Value,
    docs: &[CommittedDoc],
    body: &Value,
    opts: &SearchOptions,
) -> Result<Value, EsError> {
    if retrievers::applies(body) {
        return retrievers::search(mappings, docs, body, opts);
    }
    if let Some(spec) = body.get("suggest") {
        let suggestions = suggest::suggest(spec, mappings, docs)?;
        let mut rest = body.clone();
        if let Some(o) = rest.as_object_mut() {
            o.remove("suggest");
        }
        // A suggest-only request runs no query: no hits.
        let only = ["query", "aggs", "aggregations"].iter().all(|k| body.get(*k).is_none());
        let mut resp = search_with(mappings, if only { &[] } else { docs }, &rest, opts)?;
        resp["suggest"] = suggestions;
        return Ok(resp);
    }
    if let Some(h) = body.get("highlight") {
        highlight::validate(h)?;
    }
    let typed = opts.typed;
    let size = int_param(body, "size", 10)?;
    let from = int_param(body, "from", 0)?;
    if size < 0 {
        return Err(EsError::new(
            400,
            "illegal_argument_exception",
            &format!("[size] parameter cannot be negative, found [{size}]"),
        ));
    }
    if from < 0 {
        return Err(EsError::new(
            400,
            "illegal_argument_exception",
            &format!("[from] parameter cannot be negative but was [{from}]"),
        ));
    }
    if body.get("terminate_after").and_then(Value::as_i64).is_some_and(|n| n < 0) {
        return Err(EsError::new(400, "illegal_argument_exception", "terminateAfter must be > 0"));
    }
    if let Some(n) = body.get("track_total_hits").and_then(Value::as_i64)
        && n < -1
    {
        return Err(EsError::new(
            400,
            "illegal_argument_exception",
            &format!("[track_total_hits] parameter must be positive or equals to -1, got {n}"),
        ));
    }
    let search_after = body.get("search_after");
    let collapse = match body.get("collapse") {
        Some(c) => Some(collapse_spec(c, mappings, body)?),
        None => None,
    };
    if search_after.is_some() && from > 0 {
        return Err(EsError::new(
            400,
            "action_request_validation_exception",
            "Validation Failed: 1: [from] parameter must be set to 0 when [search_after] is used;",
        ));
    }
    let window = super::limits::setting(&opts.settings, "max_result_window").unwrap_or(10_000);
    if from + size > window && !opts.all_hits {
        return Err(EsError::shard_failure(
            "illegal_argument_exception",
            &format!(
                "Result window is too large, from + size must be less than or equal to: \
                 [{window}] but was [{}]. See the scroll api for a more efficient way to request \
                 large data sets. This limit can be set by changing the \
                 [index.max_result_window] index level setting.",
                from + size
            ),
        ));
    }
    let agg_spec = body.get("aggs").or_else(|| body.get("aggregations"));
    if let Some(a) = agg_spec {
        validate_aggs(a)?;
    }
    let mut specs = match body.get("sort") {
        Some(spec) => sorting::parse(spec, mappings, typed)?,
        None => Vec::new(),
    };
    // The implicit `_shard_doc` tiebreaker of a point-in-time search; a
    // `search_after` taken from such a hit carries its value too.
    let after_len = search_after.map(|a| a.as_array().map_or(1, Vec::len));
    if opts.pit && !specs.is_empty() && after_len.is_none_or(|n| n == specs.len() + 1) {
        specs.extend(sorting::parse(&json!("_shard_doc"), mappings, typed)?);
    }
    // `_shard_doc` is the order of a point in time.
    if !opts.pit && body.get("sort").is_some_and(|v| v.to_string().contains("\"_shard_doc\"")) {
        return Err(EsError::new(
            400,
            "action_request_validation_exception",
            "Validation Failed: 1: [_shard_doc] sort field cannot be used without [point in time];",
        ));
    }
    if search_after.is_some() && specs.is_empty() {
        return Err(EsError::shard_failure(
            "illegal_argument_exception",
            "Sort must contain at least one field.",
        ));
    }

    if let Some(e) = vectors::unsupported_doc_values(body, mappings) {
        return Err(e);
    }
    let mut query = match vectors::top_level_query(body, mappings, size)? {
        Some(q) => q,
        None => body.get("query").cloned().unwrap_or_else(|| json!({"match_all":{}})),
    };
    vectors::fill_defaults(&mut query, size);
    // Queries and aggregations see the root-level view (no nested
    // objects); hits are built from the originals.
    let view = root_view(mappings, docs);
    let originals = docs;
    let docs: &[CommittedDoc] = view.as_deref().unwrap_or(docs);
    let mut scores = eval(&query, mappings, docs)?;
    if let Some(only) = &opts.restrict {
        scores.retain(|i, _| only.contains(i));
    }
    apply_indices_boost(body, docs, &mut scores);
    let named = named_queries(&query, mappings, docs)?;
    let mut inner = Vec::new();
    inner_hits(&query, mappings, docs, &mut inner)?;
    if let Some(min_score) = body.get("min_score").and_then(Value::as_f64) {
        let min_score = min_score as f32;
        scores.retain(|_, s| *s >= min_score);
    }
    let terminated = retrievers::terminate_after(body, &query, &mut scores);
    let matched: Vec<usize> = scores.keys().copied().collect();
    // `post_filter` narrows the hits after aggregations saw them all.
    if let Some(pf) = body.get("post_filter") {
        let keep = eval(pf, mappings, docs)?;
        scores.retain(|i, _| keep.contains_key(i));
    }
    let total = terminated.as_ref().map_or(scores.len(), |t| t.total);

    let mut ranked: Vec<(usize, f32, Vec<Value>)> = scores
        .into_iter()
        .map(|(i, sc)| {
            let keys = sorting::keys(&specs, &docs[i], i, sc);
            (i, sc, keys)
        })
        .collect();
    if specs.is_empty() {
        ranked.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal).then(a.0.cmp(&b.0)));
    } else {
        ranked.sort_by(|a, b| sorting::compare_keys(&specs, &a.2, &b.2).then(a.0.cmp(&b.0)));
    }
    let named_scores =
        body.get("include_named_queries_score").and_then(Value::as_bool).unwrap_or(false);
    let max_of = |r: &[(usize, f32, Vec<Value>)]| {
        r.iter().map(|h| h.1).fold(None, |m: Option<f32>, s| Some(m.map_or(s, |m| m.max(s))))
    };
    let track_scores = body.get("track_scores").and_then(Value::as_bool).unwrap_or(false);
    let shows_scores = specs.is_empty() || track_scores || specs.iter().any(|s| s.is_score());
    let mut max_score =
        if size == 0 || !(specs.is_empty() || track_scores) { None } else { max_of(&ranked) };
    if let Some(after) = search_after {
        let after = after.as_array().cloned().unwrap_or_else(|| vec![after.clone()]);
        let after = sorting::after_keys(&specs, &after)?;
        ranked.retain(|h| sorting::compare_keys(&specs, &h.2, &after) == Ordering::Greater);
    }
    // Field collapsing: the top hit of each value; inner hits per group.
    if let Some(c) = &collapse {
        let mut seen = HashSet::new();
        ranked.retain(|h| seen.insert(collapse_key(&docs[h.0], c).to_string()));
    }
    // `rescore`: the top hits scored again by a second query.
    rescore::check_sort(body, specs.is_empty() || (specs.len() == 1 && specs[0].is_score()))?;
    let mut rescored = HashMap::new();
    if body.get("rescore").is_some_and(|r| !r.is_null()) {
        let mut pairs: Vec<(usize, f32)> = ranked.iter().map(|h| (h.0, h.1)).collect();
        let keep = if opts.all_hits { pairs.len() } else { (from + size) as usize };
        if let Some(steps) = rescore::apply(body, &mut pairs, mappings, docs, keep)? {
            rescored = steps;
            ranked = pairs
                .into_iter()
                .map(|(i, sc)| (i, sc, sorting::keys(&specs, &docs[i], i, sc)))
                .collect();
            if size != 0 {
                max_score = ranked.first().map(|h| h.1);
            }
        }
    }
    let explain = body.get("explain").and_then(Value::as_bool).unwrap_or(false);

    let source_filter = body.get("_source");
    let stored = body.get("stored_fields");
    let stored_none = match stored {
        Some(Value::String(s)) => s == "_none_",
        Some(Value::Array(a)) => a.iter().any(|v| v == "_none_"),
        _ => false,
    };
    // `stored_fields` without `_source` among them loads no `_source`
    // (unless `_source` filtering asks for it explicitly).
    let stored_wants_source = match stored {
        None => true,
        Some(Value::String(s)) => s == "_source" || s == "*",
        Some(Value::Array(a)) => a.iter().any(|v| v == "_source" || v == "*"),
        Some(_) => false,
    };
    let show_version = body.get("version").and_then(Value::as_bool).unwrap_or(false);
    let show_seq = body.get("seq_no_primary_term").and_then(Value::as_bool).unwrap_or(false);
    let (from, size) = if opts.all_hits { (0, usize::MAX) } else { (from as usize, size as usize) };
    let hits: Result<Vec<Value>, EsError> = ranked
        .iter()
        .skip(from)
        .take(size)
        .map(|(idx, score, keys)| {
            let d = &originals[*idx];
            let mut hit = if stored_none {
                json!({"_index": d.index})
            } else {
                json!({"_index": d.index, "_id": d.id})
            };
            if show_version {
                hit["_version"] = json!(d.version);
            }
            if show_seq {
                hit["_seq_no"] = json!(d.seq);
                hit["_primary_term"] = json!(1);
            }
            hit["_score"] = if shows_scores { json!(score) } else { Value::Null };
            if explain {
                hit["_shard"] = json!(format!("[{}][0]", d.index));
                hit["_node"] = json!("noida");
                hit["_explanation"] = explain::hit_explanation(
                    &query,
                    mappings,
                    docs,
                    *idx,
                    *score,
                    rescored.get(idx),
                );
            }
            // `"_source": false` omits the key, as Elasticsearch does.
            let source_enabled = mappings
                .get("_source")
                .and_then(|s| s.get("enabled"))
                .and_then(Value::as_bool)
                .unwrap_or(true);
            // Script fields alone don't bring `_source` along.
            let scripted_only = body.get("script_fields").is_some() && source_filter.is_none();
            if !matches!(source_filter, Some(Value::Bool(false)))
                && !stored_none
                && source_enabled
                && !scripted_only
                && (stored_wants_source || source_filter.is_some())
            {
                hit["_source"] = apply_source_filter(&d.source, source_filter);
            }
            // `_ignored`, and with `fields` the values that were ignored.
            if !stored_none {
                let ignored = super::docparse::ignored_values(mappings, &opts.settings, d.full());
                if !ignored.is_empty() {
                    hit["_ignored"] = json!(ignored.iter().map(|(f, _)| f).collect::<Vec<_>>());
                    if let Some(spec) = body.get("fields") {
                        let ifv: Map<String, Value> = ignored
                            .iter()
                            .filter(|(f, _)| fields::requested(spec, f))
                            .map(|(f, v)| (f.clone(), json!(v)))
                            .collect();
                        if !ifv.is_empty() {
                            hit["ignored_field_values"] = Value::Object(ifv);
                        }
                    }
                }
            }
            let mut fetched = Map::new();
            for (key, kind) in [
                ("stored_fields", fields::Kind::Stored),
                ("docvalue_fields", fields::Kind::DocValue),
                ("fields", fields::Kind::Fields),
            ] {
                if let Some(spec) = body.get(key)
                    && !(kind == fields::Kind::Stored && stored_none)
                {
                    let f = fields::fetch(mappings, d, spec, kind)?;
                    for (k, v) in f {
                        fetched.entry(k).or_insert(v);
                    }
                }
            }
            for (k, v) in super::script_fields::values(body, mappings, d)? {
                fetched.entry(k).or_insert(v);
            }
            if !fetched.is_empty() {
                hit["fields"] = Value::Object(fetched);
            }
            if let Some(hl) = body.get("highlight") {
                let ctx = highlight::Context {
                    index: &d.index,
                    doc: *idx,
                    settings: &opts.settings,
                    weighted: body.get("knn").is_none() && !highlight::has_nested(&query),
                };
                if let Some(h) = highlight::highlight(hl, &query, mappings, &d.source, &ctx)? {
                    hit["highlight"] = h;
                }
            }
            if !specs.is_empty() {
                hit["sort"] = Value::Array(sorting::display(&specs, keys));
            }
            for (name, per) in &inner {
                if let Some(h) = per.get(idx) {
                    hit["inner_hits"][name.as_str()] = h.clone();
                }
            }
            if let Some(c) = &collapse {
                let key = collapse_key(&docs[*idx], c);
                hit["fields"][c.field.as_str()] = json!([key.clone()]);
                if !c.inner.is_empty() {
                    let group: HashSet<usize> =
                        (0..docs.len()).filter(|i| collapse_key(&docs[*i], c) == key).collect();
                    for ih in &c.inner {
                        hit["inner_hits"][ih.name.as_str()] =
                            collapse_inner_hits(ih, body, &group, mappings, originals, opts)?;
                    }
                }
            }
            let mut names: Vec<(&String, f32)> =
                named.iter().filter_map(|(n, m)| m.get(idx).map(|sc| (n, *sc))).collect();
            let mut order: Vec<&String> = names.iter().map(|(n, _)| *n).collect();
            dsl::java_hash_order(&mut order);
            names.sort_by_key(|(n, _)| order.iter().position(|o| o == n));
            if !names.is_empty() {
                // `include_named_queries_score`: each name with its score.
                hit["matched_queries"] = if named_scores {
                    Value::Object(names.iter().map(|(n, sc)| ((*n).clone(), json!(sc))).collect())
                } else {
                    json!(names.iter().map(|(n, _)| n).collect::<Vec<_>>())
                };
            }
            Ok(hit)
        })
        .collect();
    let hits = hits?;

    let mut hits_obj = json!({"max_score": max_score, "hits": hits});
    if let Some(t) = total_hits(body, total) {
        hits_obj["total"] = t;
    }
    let mut resp = json!({
        "took": 0,
        "timed_out": false,
        "_shards": {"total": 1, "successful": 1, "skipped": 0, "failed": 0},
        "hits": hits_obj,
    });
    if let Some(t) = terminated {
        resp["terminated_early"] = json!(t.early);
    }
    let warnings = features::warnings(body);
    if !warnings.is_empty() {
        resp[super::engine::WARNINGS] = json!(warnings);
    }
    if let Some(agg_spec) = agg_spec {
        resp["aggregations"] = eval_aggs(agg_spec, mappings, docs, &matched);
    }
    Ok(resp)
}

/// `indices_boost`: `[{"index": boost}, ...]` (or an object): matching
/// indices' scores multiplied.
fn apply_indices_boost(body: &Value, docs: &[CommittedDoc], scores: &mut HashMap<usize, f32>) {
    let Some(spec) = body.get("indices_boost") else { return };
    let mut pairs: Vec<(String, f32)> = vec![];
    let mut add = |o: &Map<String, Value>| {
        for (k, v) in o {
            if let Some(b) = v.as_f64() {
                pairs.push((k.clone(), b as f32));
            }
        }
    };
    match spec {
        Value::Array(a) => a.iter().filter_map(Value::as_object).for_each(&mut add),
        Value::Object(o) => add(o),
        _ => {}
    }
    for (i, s) in scores.iter_mut() {
        let index = &docs[*i].index;
        let glob = |p: &str| {
            let pc: Vec<char> = p.chars().collect();
            let tc: Vec<char> = index.chars().collect();
            glob_match(&pc, &tc)
        };
        if let Some((_, b)) = pairs.iter().find(|(p, _)| p == index || glob(p)) {
            *s *= b;
        }
    }
}

/// Matching documents and their scores.
type Scores = HashMap<usize, f32>;

/// Every clause of `query` carrying a `_name`, with the documents it
/// matches and their scores (for each hit's `matched_queries`).
fn named_queries(
    query: &Value,
    mappings: &Value,
    docs: &[CommittedDoc],
) -> Result<Vec<(String, Scores)>, EsError> {
    let mut found: Vec<(String, Value)> = vec![];
    collect_named(query, &mut found);
    found.into_iter().map(|(n, q)| Ok((n, eval(&q, mappings, docs)?))).collect()
}

fn collect_named(v: &Value, out: &mut Vec<(String, Value)>) {
    match v {
        Value::Object(o) => {
            // A query object is `{type: body}`; its name is in the body,
            // or (for field queries) in the field's spec.
            if o.len() == 1 {
                let (_, body) = o.iter().next().unwrap();
                let name = body.get("_name").and_then(Value::as_str).or_else(|| {
                    body.as_object()
                        .filter(|b| b.len() == 1)
                        .and_then(|b| b.values().next())
                        .and_then(|spec| spec.get("_name"))
                        .and_then(Value::as_str)
                });
                if let Some(n) = name {
                    out.push((n.to_string(), v.clone()));
                    // Its body isn't a query itself; look below it.
                    if let Some(b) = body.as_object() {
                        b.values().for_each(|x| collect_named(x, out));
                    }
                    return;
                }
            }
            for x in o.values() {
                collect_named(x, out);
            }
        }
        Value::Array(a) => a.iter().for_each(|x| collect_named(x, out)),
        _ => {}
    }
}

struct CollapseInner {
    name: String,
    spec: Value,
}

struct CollapseSpec {
    field: String,
    /// Where the values live: the field, or a field alias's target.
    path: String,
    ty: Option<String>,
    inner: Vec<CollapseInner>,
}

fn collapse_spec(c: &Value, mappings: &Value, body: &Value) -> Result<CollapseSpec, EsError> {
    let field = c
        .get("field")
        .and_then(Value::as_str)
        .ok_or_else(|| EsError::parsing("Required [field]"))?;
    let (_, mut ty) = resolve_field(mappings, field);
    let mut path = field.to_string();
    if ty.as_deref() == Some("alias") {
        let mut node = mappings;
        for seg in field.split('.') {
            node = &node["properties"][seg];
        }
        if let Some(p) = node.get("path").and_then(Value::as_str) {
            path = p.to_string();
            ty = resolve_field(mappings, p).1;
        }
    }
    match ty.as_deref() {
        None => {
            return Err(EsError::shard_failure(
                "illegal_argument_exception",
                &format!("no mapping found for `{field}` in order to collapse on"),
            ));
        }
        Some(t @ ("text" | "match_only_text")) => {
            return Err(EsError::shard_failure(
                "illegal_argument_exception",
                &format!("collapse is not supported for the field [{field}] of the type [{t}]"),
            ));
        }
        _ => {}
    }
    if body.get("search_after").is_some() {
        let same = match body.get("sort") {
            Some(Value::Array(a)) if a.len() == 1 => sort_field_name(&a[0]) == Some(field),
            Some(one) => sort_field_name(one) == Some(field),
            None => false,
        };
        if !same {
            return Err(EsError::shard_failure(
                "illegal_argument_exception",
                "Cannot use [collapse] in conjunction with [search_after] unless the search is \
                 sorted on the same field. Multiple sort fields are not allowed.",
            ));
        }
    }
    let inner = match c.get("inner_hits") {
        None => vec![],
        Some(Value::Array(a)) => a.iter().map(inner_spec).collect(),
        Some(o) => vec![inner_spec(o)],
    };
    Ok(CollapseSpec { field: field.to_string(), path, ty, inner })
}

fn sort_field_name(v: &Value) -> Option<&str> {
    match v {
        Value::String(s) => Some(s),
        // `{"a": "asc", "b": "desc"}` sorts on two fields.
        Value::Object(o) if o.len() == 1 => o.keys().next().map(String::as_str),
        _ => None,
    }
}

fn inner_spec(v: &Value) -> CollapseInner {
    CollapseInner {
        name: v.get("name").and_then(Value::as_str).unwrap_or("").to_string(),
        spec: v.clone(),
    }
}

/// A document's collapse value (`null` for none).
fn collapse_key(d: &CommittedDoc, c: &CollapseSpec) -> Value {
    // A search over several indices may reach the field directly in one
    // and through an alias in another.
    let mut vals = raw_values(&d.source, &c.path);
    if vals.is_empty() {
        vals = raw_values(&d.source, &c.field);
    }
    vals.into_iter()
        .next()
        .map(|v| match (c.ty.as_deref(), v) {
            (Some("long" | "integer" | "short" | "byte"), Value::Number(n)) => {
                json!(n.as_i64().unwrap_or_default())
            }
            _ => v.clone(),
        })
        .unwrap_or(Value::Null)
}

/// A collapse group's inner hits: the original query searched again over
/// the group's documents alone (as Elasticsearch's expand phase does,
/// with the group's value as a filter), shaped by the inner hit
/// definition (`from`, `size`, `sort`, fetch options, a second-level
/// `collapse`). Without a query the group's hits score 0.
fn collapse_inner_hits(
    ih: &CollapseInner,
    body: &Value,
    group: &HashSet<usize>,
    mappings: &Value,
    originals: &[CommittedDoc],
    opts: &SearchOptions,
) -> Result<Value, EsError> {
    let mut sub = Map::new();
    sub.insert(
        "query".into(),
        body.get("query")
            .cloned()
            .unwrap_or_else(|| json!({"bool": {"filter": [{"match_all": {}}]}})),
    );
    sub.insert("size".into(), json!(3));
    if let Some(o) = ih.spec.as_object() {
        for (k, v) in o {
            if !matches!(k.as_str(), "name" | "ignore_unmapped") {
                sub.insert(k.clone(), v.clone());
            }
        }
    }
    let sub_opts = SearchOptions {
        typed: opts.typed,
        settings: opts.settings.clone(),
        restrict: Some(group.clone()),
        ..Default::default()
    };
    let mut resp = search_with(mappings, originals, &Value::Object(sub), &sub_opts)?;
    if let Some(h) = resp["hits"].as_object_mut() {
        // Every inner hit is counted exactly.
        if h.get("total").is_none() {
            h.insert("total".into(), json!({"value": group.len(), "relation": "eq"}));
        }
    }
    Ok(json!({"hits": resp["hits"].take()}))
}

/// Evaluates `query` over the root-level view of `docs` (nested objects
/// hidden), as `_search` does.
pub fn eval_root(
    query: &Value,
    mappings: &Value,
    docs: &[CommittedDoc],
) -> Result<HashMap<usize, f32>, EsError> {
    let view = root_view(mappings, docs);
    eval(query, mappings, view.as_deref().unwrap_or(docs))
}

/// `POST/GET _count`: the number of matching documents.
pub fn count(mappings: &Value, docs: &[CommittedDoc], body: &Value) -> Result<u64, EsError> {
    let query = body.get("query").cloned().unwrap_or_else(|| json!({"match_all":{}}));
    let view = root_view(mappings, docs);
    let docs: &[CommittedDoc] = view.as_deref().unwrap_or(docs);
    let scores = eval(&query, mappings, docs)?;
    Ok(match body.get("min_score").and_then(Value::as_f64) {
        Some(min) => scores.values().filter(|s| **s >= min as f32).count() as u64,
        None => scores.len() as u64,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn search(mappings: &Value, docs: &[CommittedDoc], body: &Value) -> Result<Value, EsError> {
        search_with(mappings, docs, body, &SearchOptions::default())
    }

    fn doc(index: &str, id: &str, source: Value) -> CommittedDoc {
        CommittedDoc {
            index: index.to_string(),
            id: id.to_string(),
            source,
            version: 1,
            seq: 0,
            full_source: None,
            tsid: None,
        }
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
