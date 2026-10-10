//! Query DSL plumbing around `search::eval`:
//!
//! - `prepare`: request checks and rewrites done once per search before
//!   any shard work, as Elasticsearch's coordinator does -- unknown body
//!   keys, malformed query objects, unknown queries ("did you mean"),
//!   `terms` lookups and `more_like_this` documents fetched from their
//!   index, and `index.max_terms_count`;
//! - `eval_extra`: the query types and options implemented outside
//!   `search.rs` (intervals, spans, `more_like_this`, `distance_feature`,
//!   fuzziness and `minimum_should_match` in the `match` family);
//! - `search.allow_expensive_queries`, set per request through `enter`.

use std::cell::Cell;
use std::collections::HashMap;

use serde_json::{Map, Value, json};

use super::search::{CommittedDoc, EsError, raw_values, resolve_field};
use super::{dates, fuzzy, intervals, mlt, queries};

type Scores = HashMap<usize, f32>;

/// An error response as the engine returns it.
pub type Fail = (u16, Value);

fn fail(e: EsError) -> Fail {
    (e.status, e.to_json())
}

thread_local! {
    static ALLOW_EXPENSIVE: Cell<bool> = const { Cell::new(true) };
}

/// Restores the previous `search.allow_expensive_queries` when dropped.
pub struct Guard(bool);

impl Drop for Guard {
    fn drop(&mut self) {
        ALLOW_EXPENSIVE.with(|c| c.set(self.0));
    }
}

/// Runs the rest of the request with `search.allow_expensive_queries`.
pub fn enter(allow_expensive: bool) -> Guard {
    Guard(ALLOW_EXPENSIVE.with(|c| c.replace(allow_expensive)))
}

/// Every query name Elasticsearch 8.15 registers (for "did you mean").
const QUERY_NAMES: &[&str] = &[
    "bool",
    "boosting",
    "combined_fields",
    "constant_score",
    "dis_max",
    "distance_feature",
    "exists",
    "field_masking_span",
    "function_score",
    "fuzzy",
    "geo_bounding_box",
    "geo_distance",
    "geo_shape",
    "has_child",
    "has_parent",
    "ids",
    "intervals",
    "knn",
    "match",
    "match_all",
    "match_bool_prefix",
    "match_none",
    "match_phrase",
    "match_phrase_prefix",
    "more_like_this",
    "multi_match",
    "nested",
    "parent_id",
    "percolate",
    "pinned",
    "prefix",
    "query_string",
    "range",
    "rank_feature",
    "regexp",
    "rule",
    "script",
    "script_score",
    "semantic",
    "shape",
    "simple_query_string",
    "span_containing",
    "span_field_masking",
    "span_first",
    "span_gap",
    "span_multi",
    "span_near",
    "span_not",
    "span_or",
    "span_term",
    "span_within",
    "sparse_vector",
    "term",
    "terms",
    "terms_set",
    "text_expansion",
    "weighted_tokens",
    "wildcard",
    "wrapper",
];

/// Lucene's `LevenshteinDistance`: `1 - edits / max(len)`.
fn similarity(a: &str, b: &str) -> f32 {
    let (a, b): (Vec<char>, Vec<char>) = (a.chars().collect(), b.chars().collect());
    let n = a.len().max(b.len());
    if n == 0 {
        return 1.0;
    }
    1.0 - fuzzy::edit_distance(&a, &b, false) as f32 / n as f32
}

/// ` did you mean [x]?` / ` did you mean any of [x, y]?` (empty when
/// nothing is close), Elasticsearch's `SuggestingErrorOnUnknown`.
pub fn did_you_mean(name: &str, candidates: &[&str]) -> String {
    let mut scored: Vec<(f32, &str)> =
        candidates.iter().map(|c| (similarity(name, c), *c)).filter(|(d, _)| *d > 0.5).collect();
    scored.sort_by(|a, b| b.0.total_cmp(&a.0).then_with(|| a.1.cmp(b.1)));
    match scored.as_slice() {
        [] => String::new(),
        [(_, one)] => format!(" did you mean [{one}]?"),
        many => format!(
            " did you mean any of [{}]?",
            many.iter().map(|(_, c)| *c).collect::<Vec<_>>().join(", ")
        ),
    }
}

/// The parse error for a query name `eval` doesn't know (one
/// Elasticsearch has but noida doesn't run is reported as unknown, without
/// suggestions).
pub fn unknown_query(name: &str) -> EsError {
    let hint =
        if QUERY_NAMES.contains(&name) { String::new() } else { did_you_mean(name, QUERY_NAMES) };
    EsError::parsing(&format!("unknown query [{name}]{hint}"))
}

/// The `line:col` Elasticsearch reports for `"name"` in the request body:
/// the name itself, or (`value`) the value after it.
fn locate(raw: &str, name: &str, value: bool) -> Option<(usize, usize)> {
    let quoted = format!("\"{name}\"");
    let mut from = 0;
    while let Some(i) = raw[from..].find(&quoted).map(|i| i + from) {
        let rest = &raw[i + quoted.len()..];
        let ws = rest.len() - rest.trim_start().len();
        if rest[ws..].starts_with(':') {
            let at = if value {
                let after = &rest[ws + 1..];
                i + quoted.len() + ws + 1 + (after.len() - after.trim_start().len())
            } else {
                i
            };
            let line = raw[..at].matches('\n').count() + 1;
            let col = at - raw[..at].rfind('\n').map_or(0, |n| n + 1) + 1;
            return Some((line, col));
        }
        from = i + quoted.len();
    }
    None
}

/// A `parsing_exception` response, positioned in the body when known.
fn parse_failure(reason: &str, at: Option<(usize, usize)>, cause: Option<Value>) -> Fail {
    let mut e = json!({"type": "parsing_exception", "reason": reason});
    if let Some((line, col)) = at {
        e["line"] = json!(line);
        e["col"] = json!(col);
    }
    let root = e.clone();
    if let Some(c) = cause {
        e["caused_by"] = c;
    }
    e["root_cause"] = json!([root]);
    (400, json!({"error": e, "status": 400}))
}

/// A parse failure inside a `bool` clause, wrapped the way
/// Elasticsearch's object parser reports it.
fn wrap_bool_clause(f: Fail, clause: &str) -> Fail {
    let (status, mut v) = f;
    let kind = v["error"]["type"].as_str().unwrap_or_default().to_string();
    if !matches!(kind.as_str(), "parsing_exception" | "x_content_parse_exception") {
        return (status, v);
    }
    let mut inner = v["error"].take();
    let root = inner.as_object_mut().and_then(|o| o.remove("root_cause"));
    let pos = match (inner.get("line"), inner.get("col")) {
        (Some(l), Some(c)) => format!("[{l}:{c}] "),
        _ => String::new(),
    };
    (
        status,
        json!({"error": {"root_cause": root, "type": "x_content_parse_exception",
                         "reason": format!("{pos}[bool] failed to parse field [{clause}]"),
                         "caused_by": inner}, "status": status}),
    )
}

/// The keys a search request body may have.
const BODY_KEYS: &[&str] = &[
    "_source",
    "aggregations",
    "aggs",
    "collapse",
    "docvalue_fields",
    "explain",
    "ext",
    "fields",
    "from",
    "highlight",
    // The URL parameter, carried in the body.
    "include_named_queries_score",
    "indices_boost",
    "knn",
    "min_score",
    "pit",
    "post_filter",
    "profile",
    "query",
    "rank",
    "rescore",
    "retriever",
    "runtime_mappings",
    "script_fields",
    "search_after",
    "seq_no_primary_term",
    "size",
    "slice",
    "sort",
    "stats",
    "stored_fields",
    "sub_searches",
    "suggest",
    "terminate_after",
    "timeout",
    "track_scores",
    "track_total_hits",
    "version",
];

fn token_kind(v: &Value) -> &'static str {
    match v {
        Value::Object(_) => "START_OBJECT",
        Value::Array(_) => "START_ARRAY",
        Value::String(_) => "VALUE_STRING",
        Value::Number(_) => "VALUE_NUMBER",
        Value::Bool(_) => "VALUE_BOOLEAN",
        Value::Null => "VALUE_NULL",
    }
}

/// What `prepare` needs from the engine.
pub struct Env<'a> {
    /// A document's current source (real-time, like a GET), `None` when
    /// it doesn't exist; an error when the index doesn't.
    pub fetch: &'a dyn Fn(&str, &str, Option<&str>) -> Result<Option<Value>, Fail>,
    /// The index searched (where an item without `_index` is looked up).
    pub default_index: Option<String>,
    /// The lowest `index.max_terms_count` of the searched indices.
    pub max_terms_count: usize,
    /// A `_search` body (a `_count` body takes only `query`).
    pub search: bool,
    /// The request body as sent (for error positions).
    pub raw: &'a str,
}

/// Checks and rewrites a search request before it runs.
pub fn prepare(req: &mut Value, env: &Env) -> Result<(), Fail> {
    if env.search
        && let Some(o) = req.as_object()
        && let Some((k, v)) = o.iter().find(|(k, _)| !BODY_KEYS.contains(&k.as_str()))
    {
        return Err(fail(EsError::parsing(&format!(
            "Unknown key for a {} in [{k}].",
            token_kind(v)
        ))));
    }
    if let Some(q) = req.get_mut("query") {
        walk(q, env)?;
    }
    if let Some(q) = req.get_mut("post_filter") {
        walk(q, env)?;
    }
    match req.get_mut("rescore") {
        Some(Value::Array(a)) => {
            for r in a {
                if let Some(q) = r.pointer_mut("/query/rescore_query") {
                    walk(q, env)?;
                }
            }
        }
        Some(r) => {
            if let Some(q) = r.pointer_mut("/query/rescore_query") {
                walk(q, env)?;
            }
        }
        None => {}
    }
    Ok(())
}

/// The sub-queries of a compound query body, by key.
const CHILD_QUERIES: &[(&str, &[&str])] = &[
    ("bool", &["must", "should", "filter", "must_not"]),
    ("dis_max", &["queries"]),
    ("constant_score", &["filter"]),
    ("boosting", &["positive", "negative"]),
    ("nested", &["query"]),
    ("has_child", &["query"]),
    ("has_parent", &["query"]),
    ("function_score", &["query"]),
    ("script_score", &["query"]),
    ("pinned", &["organic"]),
];

fn walk(q: &mut Value, env: &Env) -> Result<(), Fail> {
    let Some(o) = q.as_object_mut() else { return Ok(()) };
    if o.len() > 1 {
        let name = o
            .keys()
            .find(|k| QUERY_NAMES.contains(&k.as_str()))
            .unwrap_or_else(|| o.keys().next().expect("non-empty"));
        let extra = o.keys().find(|k| *k != name).cloned().unwrap_or_default();
        return Err(parse_failure(
            &format!("[{name}] malformed query, expected [END_OBJECT] but found [FIELD_NAME]"),
            locate(env.raw, &extra, false),
            None,
        ));
    }
    let Some((name, body)) = o.iter_mut().next() else { return Ok(()) };
    if !QUERY_NAMES.contains(&name.as_str()) {
        let at = locate(env.raw, name, true);
        let pos = at.map_or(String::new(), |(l, c)| format!("[{l}:{c}] "));
        return Err(parse_failure(
            &format!("unknown query [{name}]{}", did_you_mean(name, QUERY_NAMES)),
            at,
            Some(json!({"type": "named_object_not_found_exception",
                        "reason": format!("{pos}unknown field [{name}]")})),
        ));
    }
    if let Some((_, keys)) = CHILD_QUERIES.iter().find(|(n, _)| *n == name) {
        let in_bool = name == "bool";
        for k in *keys {
            let wrap = |f: Fail| if in_bool { wrap_bool_clause(f, k) } else { f };
            match body.get_mut(*k) {
                Some(Value::Array(a)) => {
                    for c in a {
                        walk(c, env).map_err(wrap)?;
                    }
                }
                Some(c @ Value::Object(_)) => walk(c, env).map_err(wrap)?,
                _ => {}
            }
        }
        if name == "function_score"
            && let Some(Value::Array(fs)) = body.get_mut("functions")
        {
            for f in fs {
                if let Some(c) = f.get_mut("filter") {
                    walk(c, env)?;
                }
            }
        }
    }
    match name.as_str() {
        "terms" => terms_lookup(body, env)?,
        "more_like_this" => {
            for key in ["like", "unlike"] {
                match body.get_mut(key) {
                    Some(Value::Array(a)) => {
                        for item in a {
                            fetch_item(item, env)?;
                        }
                    }
                    Some(item @ Value::Object(_)) => fetch_item(item, env)?,
                    _ => {}
                }
            }
        }
        _ => {}
    }
    Ok(())
}

/// `{"terms": {"field": {"index", "id", "path"}}}`: the values at `path`
/// of that document; and the `index.max_terms_count` limit.
fn terms_lookup(body: &mut Value, env: &Env) -> Result<(), Fail> {
    let Some(o) = body.as_object_mut() else { return Ok(()) };
    for (field, spec) in o.iter_mut() {
        if field == "boost" || field == "_name" {
            continue;
        }
        if let Value::Object(l) = spec {
            let Some(id) = l.get("id").and_then(Value::as_str) else {
                return Err(fail(EsError::new(400, "illegal_argument_exception", "Required [id]")));
            };
            let index = l
                .get("index")
                .and_then(Value::as_str)
                .map(str::to_string)
                .or_else(|| env.default_index.clone())
                .unwrap_or_default();
            let path = l.get("path").and_then(Value::as_str).unwrap_or("").to_string();
            let routing = l.get("routing").and_then(Value::as_str);
            let values: Vec<Value> = match (env.fetch)(&index, id, routing)? {
                Some(src) => raw_values(&src, &path).into_iter().cloned().collect(),
                None => vec![],
            };
            *spec = Value::Array(values);
        }
        if let Value::Array(a) = spec
            && a.len() > env.max_terms_count
        {
            let reason = format!(
                "The number of terms [{}] used in the Terms Query request has exceeded the \
                 allowed maximum of [{}]. This maximum can be set by changing the \
                 [index.max_terms_count] index level setting.",
                a.len(),
                env.max_terms_count
            );
            return Err(fail(
                EsError::shard_failure(
                    "query_shard_exception",
                    &format!("failed to create query: {reason}"),
                )
                .caused_by("illegal_argument_exception", &reason),
            ));
        }
    }
    Ok(())
}

/// A `more_like_this` item naming a document: its source fetched into
/// `doc` (it stays `_id`-identified so it's excluded from the hits).
fn fetch_item(item: &mut Value, env: &Env) -> Result<(), Fail> {
    let Some(o) = item.as_object_mut() else { return Ok(()) };
    if o.contains_key("doc") {
        return Ok(());
    }
    let Some(id) = o.get("_id").and_then(Value::as_str).map(str::to_string) else {
        return Ok(());
    };
    let index = match o.get("_index").and_then(Value::as_str) {
        Some(i) => i.to_string(),
        None => match &env.default_index {
            Some(i) => i.clone(),
            None => return Ok(()),
        },
    };
    let routing = o.get("routing").and_then(Value::as_str).map(str::to_string);
    if let Some(src) = (env.fetch)(&index, &id, routing.as_deref())? {
        o.insert("doc".into(), src);
    }
    o.insert("_index".into(), json!(index));
    Ok(())
}

fn expensive(reason: &str) -> EsError {
    EsError::shard_failure("query_shard_exception", &format!("failed to create query: {reason}"))
        .caused_by("exception", reason)
}

/// `search.allow_expensive_queries: false` refuses the queries that may
/// be slow on large indices.
fn check_allowed(obj: &Map<String, Value>, mappings: &Value) -> Result<(), EsError> {
    if ALLOW_EXPENSIVE.with(Cell::get) {
        return Ok(());
    }
    let tail = "cannot be executed when 'search.allow_expensive_queries' is set to false.";
    let Some((name, body)) = obj.iter().next() else { return Ok(()) };
    let field = body.as_object().and_then(|b| {
        b.iter().find(|(k, _)| !matches!(k.as_str(), "boost" | "_name")).map(|(k, _)| k.as_str())
    });
    let ty = field.and_then(|f| resolve_field(mappings, f).1);
    match name.as_str() {
        "prefix" => {
            let len = field
                .and_then(|f| body.get(f))
                .map(|s| match s {
                    Value::Object(o) => o.get("value").and_then(Value::as_str).unwrap_or(""),
                    other => other.as_str().unwrap_or(""),
                })
                .map_or(0, |p| p.chars().count());
            let optimised = field.and_then(|f| field_def(mappings, f)).and_then(|d| {
                let ip = d.get("index_prefixes")?;
                let min = ip.get("min_chars").and_then(Value::as_u64).unwrap_or(2) as usize;
                let max = ip.get("max_chars").and_then(Value::as_u64).unwrap_or(5) as usize;
                Some(len >= min && len <= max)
            });
            if optimised != Some(true) {
                return Err(expensive(&format!(
                    "[prefix] queries {tail} For optimised prefix queries on text fields please \
                     enable [index_prefixes]."
                )));
            }
        }
        "fuzzy" | "regexp" | "wildcard" => {
            return Err(expensive(&format!("[{name}] queries {tail}")));
        }
        "match" | "multi_match" | "match_bool_prefix" => {
            let has_fuzz = body.get("fuzziness").is_some()
                || field.and_then(|f| body.get(f)).and_then(|s| s.get("fuzziness")).is_some();
            if has_fuzz {
                return Err(expensive(&format!("[fuzzy] queries {tail}")));
            }
        }
        "range" if matches!(ty.as_deref(), Some("text" | "keyword")) => {
            return Err(expensive(&format!(
                "[range] queries on [text] or [keyword] fields {tail}"
            )));
        }
        "nested" | "has_child" | "has_parent" => {
            return Err(expensive(&format!("[joining] queries {tail}")));
        }
        "script" => return Err(expensive(&format!("[script] queries {tail}"))),
        "script_score" => return Err(expensive(&format!("[script score] queries {tail}"))),
        _ => {}
    }
    Ok(())
}

/// A field's mapping definition (multi-fields included).
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

/// The query types and options handled outside `search.rs`; `None` for
/// the rest.
pub fn eval_extra(
    obj: &Map<String, Value>,
    mappings: &Value,
    docs: &[CommittedDoc],
) -> Option<Result<Scores, EsError>> {
    if let Err(e) = check_allowed(obj, mappings) {
        return Some(Err(e));
    }
    let (name, v) = obj.iter().next()?;
    Some(match name.as_str() {
        "intervals" => intervals::eval_intervals(v, mappings, docs),
        n if intervals::SPAN_QUERIES.contains(&n) => intervals::eval_span(n, v, mappings, docs),
        "more_like_this" => mlt::eval(v, mappings, docs),
        "distance_feature" => distance_feature(v, mappings, docs),
        "fuzzy" => fuzzy::eval_fuzzy(v, mappings, docs),
        "match_bool_prefix" => fuzzy::eval_match_bool_prefix(v, mappings, docs),
        "multi_match" if fuzzy::handles_multi_match(v) => {
            fuzzy::eval_multi_match(v, mappings, docs)
        }
        "match" => {
            let (field, spec) = v.as_object().and_then(|o| o.iter().next())?;
            if let Some(ty) =
                resolve_field(mappings, field).1.filter(|t| NUMERIC.contains(&t.as_str()))
            {
                return Some(numeric_match(field, &ty, spec, docs));
            }
            if matches!(resolve_field(mappings, field).1.as_deref(), Some("date" | "date_nanos")) {
                let text = match spec {
                    Value::Object(o) => o.get("query").cloned().unwrap_or(Value::Null),
                    other => other.clone(),
                };
                return Some(date_term(field, &text, spec, mappings, docs));
            }
            let extra = ["fuzziness", "minimum_should_match", "zero_terms_query"]
                .iter()
                .any(|k| spec.get(*k).is_some());
            if !extra {
                return None;
            }
            fuzzy::eval_match(v, mappings, docs)
        }
        "term" | "prefix" | "wildcard" | "regexp" if case_insensitive(v) => {
            Ok(case_insensitive_query(name, v, mappings, docs))
        }
        "term" => {
            let (field, spec) = v.as_object().and_then(|o| o.iter().next())?;
            if !matches!(resolve_field(mappings, field).1.as_deref(), Some("date" | "date_nanos")) {
                return None;
            }
            let value = match spec {
                Value::Object(o) => o.get("value").cloned().unwrap_or(Value::Null),
                other => other.clone(),
            };
            date_term(field, &value, spec, mappings, docs)
        }
        "exists" if v.get("field").and_then(Value::as_str) == Some("_source") => Err(
            EsError::shard_failure("query_shard_exception", "The _source field is not searchable"),
        ),
        _ => return None,
    })
}

const NUMERIC: &[&str] = &[
    "long",
    "integer",
    "short",
    "byte",
    "double",
    "float",
    "half_float",
    "scaled_float",
    "unsigned_long",
];

/// `match` on a numeric field: a term query on the number, scoring a
/// constant (its boost).
fn numeric_match(
    field: &str,
    ty: &str,
    spec: &Value,
    docs: &[CommittedDoc],
) -> Result<Scores, EsError> {
    let (text, boost, lenient) = match spec {
        Value::Object(o) => (
            o.get("query").cloned().unwrap_or(Value::Null),
            o.get("boost").and_then(Value::as_f64).unwrap_or(1.0) as f32,
            o.get("lenient").and_then(Value::as_bool).unwrap_or(false),
        ),
        other => (other.clone(), 1.0, false),
    };
    if spec.get("fuzziness").is_some() {
        fuzzy::check_fuzzy_field(ty, field)?;
    }
    let as_num = |v: &Value| v.as_f64().or_else(|| v.as_str().and_then(|s| s.trim().parse().ok()));
    let Some(target) = as_num(&text) else {
        if lenient {
            return Ok(Scores::new());
        }
        let shown = text.as_str().map(str::to_string).unwrap_or_else(|| text.to_string());
        let reason = format!("For input string: \"{shown}\"");
        return Err(EsError::shard_failure(
            "query_shard_exception",
            &format!("failed to create query: {reason}"),
        )
        .caused_by("number_format_exception", &reason));
    };
    // Integer fields hold whole numbers: a fractional query matches none.
    let integral = matches!(ty, "long" | "integer" | "short" | "byte" | "unsigned_long");
    if integral && target.fract() != 0.0 {
        return Ok(Scores::new());
    }
    Ok(docs
        .iter()
        .enumerate()
        .filter(|(_, d)| {
            raw_values(&d.source, field).into_iter().any(|x| as_num(x) == Some(target))
        })
        .map(|(i, _)| (i, boost))
        .collect())
}

/// A numeric field's `range` bounds as numbers (YAML and clients may send
/// `"1e+300"`), `from`/`to` as `gte`/`lte` (`gt`/`lt` when not
/// `include_lower`/`include_upper`); `None` for other fields.
pub fn numeric_range_bounds(cond: &Value, ty: Option<&str>) -> Result<Option<Value>, EsError> {
    if !ty.is_some_and(|t| NUMERIC.contains(&t)) {
        return Ok(None);
    }
    let Some(o) = cond.as_object() else { return Ok(None) };
    let mut out = Map::new();
    let inclusive = |k: &str| o.get(k).and_then(Value::as_bool).unwrap_or(true);
    for (key, target) in [
        ("gte", "gte"),
        ("gt", "gt"),
        ("lte", "lte"),
        ("lt", "lt"),
        ("from", if inclusive("include_lower") { "gte" } else { "gt" }),
        ("to", if inclusive("include_upper") { "lte" } else { "lt" }),
    ] {
        let n = match o.get(key) {
            None | Some(Value::Null) => continue,
            Some(Value::String(s)) => s.trim().parse::<f64>().map_err(|_| {
                let reason = format!("For input string: \"{s}\"");
                EsError::shard_failure(
                    "query_shard_exception",
                    &format!("failed to create query: {reason}"),
                )
                .caused_by("number_format_exception", &reason)
            })?,
            Some(v) => match v.as_f64() {
                Some(n) => n,
                None => continue,
            },
        };
        out.insert(target.into(), json!(n));
    }
    Ok(Some(Value::Object(out)))
}

/// The leaf values of a `flattened` field, as its keyword terms.
pub fn flattened_values(source: &Value, path: &str) -> Vec<String> {
    fn walk(v: &Value, out: &mut Vec<String>) {
        match v {
            Value::Object(m) => m.values().for_each(|x| walk(x, out)),
            Value::Array(a) => a.iter().for_each(|x| walk(x, out)),
            Value::String(s) => out.push(s.clone()),
            Value::Number(n) => out.push(n.to_string()),
            Value::Bool(b) => out.push(b.to_string()),
            Value::Null => {}
        }
    }
    let mut out = Vec::new();
    for v in raw_values(source, path) {
        walk(v, &mut out);
    }
    out
}

/// `source` with only the fields mapped `store: true` (what a highlighter
/// can read when `_source` is disabled).
pub fn stored_only(mappings: &Value, source: &Value) -> Value {
    fn walk(props: &Value, src: &Value) -> Option<Value> {
        let (Some(props), Some(src)) = (props.as_object(), src.as_object()) else { return None };
        let mut out = Map::new();
        for (k, v) in src {
            let Some(def) = props.get(k) else { continue };
            if let Some(sub) = def.get("properties") {
                let kept = match v {
                    Value::Array(a) => {
                        let items: Vec<Value> = a.iter().filter_map(|x| walk(sub, x)).collect();
                        (!items.is_empty()).then_some(Value::Array(items))
                    }
                    other => walk(sub, other),
                };
                if let Some(x) = kept {
                    out.insert(k.clone(), x);
                }
            } else if def.get("store").and_then(Value::as_bool) == Some(true) {
                out.insert(k.clone(), v.clone());
            }
        }
        (!out.is_empty()).then_some(Value::Object(out))
    }
    mappings.get("properties").and_then(|p| walk(p, source)).unwrap_or_else(|| json!({}))
}

/// A `term` query's BM25 scores on a text, keyword or boolean field (as
/// Elasticsearch scores a `TermQuery`); `None` where it scores a constant
/// (numbers, dates, `_id`, `case_insensitive`).
pub fn term_scores(
    mappings: &Value,
    docs: &[CommittedDoc],
    field: &str,
    spec: &Value,
    term: &str,
    boost: f32,
) -> Option<Scores> {
    if spec.get("case_insensitive").and_then(Value::as_bool) == Some(true) {
        return None;
    }
    let ty = resolve_field(mappings, field).1?;
    let ft = fuzzy::FieldTerms::new(mappings, docs, field);
    match ty.as_str() {
        "text" => Some(ft.term_scores(term, ft.doc_freq(term), boost)),
        "keyword" | "boolean" | "flattened" => Some(ft.term_scores_no_norms(term, boost)),
        _ => None,
    }
}

/// Names in the order a Java `HashMap` iterates them (how Elasticsearch
/// lists a hit's `matched_queries`).
pub fn java_hash_order(names: &mut [&String]) {
    let hash = |s: &str| {
        let h = s.encode_utf16().fold(0u32, |h, c| h.wrapping_mul(31).wrapping_add(c as u32));
        h ^ (h >> 16)
    };
    let mut cap = 16usize;
    while names.len() * 4 > cap * 3 {
        cap *= 2;
    }
    names.sort_by_key(|n| hash(n) as usize & (cap - 1));
}

fn case_insensitive(v: &Value) -> bool {
    v.as_object()
        .and_then(|o| o.values().next())
        .and_then(|s| s.get("case_insensitive"))
        .and_then(Value::as_bool)
        == Some(true)
}

/// `term`/`prefix`/`wildcard`/`regexp` with `case_insensitive: true`: the
/// pattern matched against lowercased index terms, constant score.
fn case_insensitive_query(
    kind: &str,
    v: &Value,
    mappings: &Value,
    docs: &[CommittedDoc],
) -> Scores {
    let Some((field, spec)) = v.as_object().and_then(|o| o.iter().next()) else {
        return Scores::new();
    };
    let value = spec.get("value").or_else(|| spec.get(kind)).cloned().unwrap_or(Value::Null);
    let text = match &value {
        Value::String(s) => s.to_lowercase(),
        other => other.to_string(),
    };
    let boost = spec.get("boost").and_then(Value::as_f64).unwrap_or(1.0) as f32;
    let pattern = match kind {
        "term" => format!("^{}$", regex_lite::escape(&text)),
        "prefix" => format!("^{}", regex_lite::escape(&text)),
        "wildcard" => {
            let mut re = String::from("^");
            for c in text.chars() {
                match c {
                    '*' => re.push_str(".*"),
                    '?' => re.push('.'),
                    c => re.push_str(&regex_lite::escape(&c.to_string())),
                }
            }
            re.push('$');
            re
        }
        _ => format!("^(?:{text})$"),
    };
    let Ok(re) = regex_lite::Regex::new(&pattern) else { return Scores::new() };
    docs.iter()
        .enumerate()
        .filter(|(_, d)| {
            super::search::tokens_for(mappings, &d.source, field)
                .iter()
                .any(|t| re.is_match(&t.to_lowercase()))
        })
        .map(|(i, _)| (i, boost))
        .collect()
}

/// `term`/`match` on a date field: documents whose value is that instant
/// (parsed with the field's format), constant score.
fn date_term(
    field: &str,
    value: &Value,
    spec: &Value,
    mappings: &Value,
    docs: &[CommittedDoc],
) -> Result<Scores, EsError> {
    let format = field_def(mappings, field)
        .and_then(|d| d.get("format"))
        .and_then(Value::as_str)
        .map(str::to_string);
    let boost = spec.get("boost").and_then(Value::as_f64).unwrap_or(1.0) as f32;
    let target = match value {
        Value::String(s) => dates::parse_math(s, dates::now_ms(), false, format.as_deref(), 0),
        other => dates::value_millis(other, format.as_deref()),
    };
    let Some(target) = target else {
        let shown = value.as_str().map(str::to_string).unwrap_or_else(|| value.to_string());
        let f = format.unwrap_or_else(|| "strict_date_optional_time||epoch_millis".into());
        return Err(EsError::shard_failure(
            "parse_exception",
            &format!(
                "failed to parse date field [{shown}] with format [{f}]: [failed to parse date \
                 field [{shown}] with format [{f}]]"
            ),
        ));
    };
    Ok(docs
        .iter()
        .enumerate()
        .filter(|(_, d)| {
            raw_values(&d.source, field)
                .into_iter()
                .any(|x| dates::value_millis(x, format.as_deref()) == Some(target))
        })
        .map(|(i, _)| (i, boost))
        .collect())
}

/// A duration with its unit (`1h`, `100000000nanos`) in nanoseconds.
fn duration_nanos(s: &str) -> Option<f64> {
    let s = s.trim();
    let split = s.find(|c: char| !c.is_ascii_digit() && c != '.')?;
    let (n, unit) = s.split_at(split);
    let n: f64 = n.parse().ok()?;
    let ns = match unit {
        "nanos" => 1.0,
        "micros" => 1e3,
        "ms" => 1e6,
        "s" => 1e9,
        "m" => 60e9,
        "h" => 3600e9,
        "d" => 86400e9,
        _ => return None,
    };
    Some(n * ns)
}

/// Epoch nanoseconds of a date (math), keeping a `date_nanos` string's
/// sub-millisecond digits.
fn date_nanos(v: &Value, format: Option<&str>, round_up: bool) -> Option<f64> {
    let ms = match v {
        Value::String(s) => {
            dates::parse_math(s, dates::now_ms(), round_up, format, 0)?
                + if round_up { round_up_fill(s) } else { 0 }
        }
        other => dates::value_millis(other, format)?,
    };
    let mut nanos = ms as f64 * 1e6;
    if let Some(s) = v.as_str()
        && let Some(frac) = s.split('.').nth(1)
    {
        let digits: String = frac.chars().take_while(char::is_ascii_digit).collect();
        if digits.len() > 3 {
            let sub: String = digits.chars().skip(3).take(6).collect();
            let scale = 10f64.powi(6 - sub.len() as i32);
            nanos += sub.parse::<f64>().unwrap_or(0.0) * scale;
        }
    }
    Some(nanos)
}

/// The milliseconds a rounded-up ISO date adds for the fields it leaves
/// out (`2018-02-01` is that day's last millisecond).
fn round_up_fill(s: &str) -> i64 {
    if s.contains("now") || s.contains("||") {
        return 0;
    }
    match s.split_once('T') {
        None if s.len() == 10 => 86_399_999,
        None => 0,
        Some((_, time)) => {
            let time = time.trim_end_matches('Z');
            let time = time.split(['+', '-']).next().unwrap_or(time);
            if time.contains('.') {
                0
            } else {
                match time.matches(':').count() {
                    0 => 3_599_999,
                    1 => 59_999,
                    _ => 999,
                }
            }
        }
    }
}

/// `distance_feature`: `boost * pivot / (pivot + distance)` from `origin`
/// to the document's closest value of a date or geo_point field.
fn distance_feature(v: &Value, mappings: &Value, docs: &[CommittedDoc]) -> Result<Scores, EsError> {
    let field = v
        .get("field")
        .and_then(Value::as_str)
        .ok_or_else(|| EsError::parsing("[distance_feature] query requires [field] to be set"))?;
    let origin = v
        .get("origin")
        .ok_or_else(|| EsError::parsing("[distance_feature] query requires [origin] to be set"))?;
    let pivot = v
        .get("pivot")
        .ok_or_else(|| EsError::parsing("[distance_feature] query requires [pivot] to be set"))?;
    let boost = v.get("boost").and_then(Value::as_f64).unwrap_or(1.0);
    let ty = resolve_field(mappings, field).1;
    let mut out = Scores::new();
    let bad_pivot = || {
        EsError::shard_failure(
            "illegal_argument_exception",
            &format!("failed to parse [pivot] value [{}]", pivot.as_str().unwrap_or_default()),
        )
    };
    match ty.as_deref() {
        None => {}
        Some("date" | "date_nanos") => {
            let format = field_def(mappings, field)
                .and_then(|d| d.get("format"))
                .and_then(Value::as_str)
                .map(str::to_string);
            // The origin rounds up (`2018-02-01` is the end of that day).
            let o = date_nanos(origin, None, true).ok_or_else(|| {
                EsError::shard_failure(
                    "parse_exception",
                    &format!(
                        "failed to parse date field [{}]",
                        origin.as_str().unwrap_or_default()
                    ),
                )
            })?;
            let p = pivot.as_str().and_then(duration_nanos).ok_or_else(bad_pivot)?;
            for (i, d) in docs.iter().enumerate() {
                let best = raw_values(&d.source, field)
                    .into_iter()
                    .filter_map(|x| date_nanos(x, format.as_deref(), false))
                    .map(|t| (t - o).abs())
                    .fold(None, |m: Option<f64>, x| Some(m.map_or(x, |m| m.min(x))));
                if let Some(dist) = best {
                    out.insert(i, (boost * p / (p + dist)) as f32);
                }
            }
        }
        Some("geo_point") => {
            let o = queries::parse_point(origin).ok_or_else(|| {
                EsError::parsing("[distance_feature] query requires a valid geo_point [origin]")
            })?;
            let p = queries::parse_distance(pivot).ok_or_else(bad_pivot)?;
            for (i, d) in docs.iter().enumerate() {
                let best = queries::points(d, field)
                    .into_iter()
                    .map(|pt| queries::distance_m(o, pt))
                    .fold(None, |m: Option<f64>, x| Some(m.map_or(x, |m| m.min(x))));
                if let Some(dist) = best {
                    out.insert(i, (boost * p / (p + dist)) as f32);
                }
            }
        }
        Some(t) => {
            return Err(EsError::shard_failure(
                "illegal_argument_exception",
                &format!(
                    "Asked for a [distance_feature] query on field [{field}] of type [{t}], but \
                     this field type is not supported: only [date, date_nanos, geo_point] are"
                ),
            ));
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn did_you_mean_suggests_close_names() {
        assert_eq!(did_you_mean("boool", QUERY_NAMES), " did you mean [bool]?");
        assert_eq!(
            did_you_mean("matchall", QUERY_NAMES),
            " did you mean any of [match_all, match]?"
        );
        assert_eq!(did_you_mean("xyzzy", QUERY_NAMES), " did you mean [fuzzy]?");
        assert_eq!(did_you_mean("zzzzzzzzzz", QUERY_NAMES), "");
    }

    #[test]
    fn durations_in_nanos() {
        assert_eq!(duration_nanos("1h"), Some(3600e9));
        assert_eq!(duration_nanos("100000000nanos"), Some(1e8));
    }

    #[test]
    fn expensive_queries_refused_only_when_disallowed() {
        let m = json!({"properties": {"t": {"type": "text"}}});
        let q = json!({"wildcard": {"t": "out?ide"}});
        assert!(check_allowed(q.as_object().unwrap(), &m).is_ok());
        let _g = enter(false);
        assert!(check_allowed(q.as_object().unwrap(), &m).is_err());
    }
}
