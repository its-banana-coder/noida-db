//! The search API's `retriever` option (Elasticsearch 8.14+) and the
//! older spelling of rank fusion it replaces (`sub_searches` plus
//! `rank: {"rrf": {...}}`).
//!
//! A `standard` or `knn` retriever is another way to write a plain search,
//! and is rewritten into one (`query` + `filter` + `sort` + ..., or a
//! top-level `knn` search). The compound `rrf` retriever runs each child
//! as its own ranked result set and fuses them by reciprocal rank fusion:
//! a document scores the sum, over the result sets it is in, of
//! `1 / (rank_constant + rank)`, and the hits are the fused ranking
//! (`_rank` numbered), while `hits.total` and aggregations cover every
//! document any child matched. `text_similarity_reranker` needs an
//! inference endpoint, which noida never has.
//!
//! On a real cluster RRF and the reranker need a paid license (a basic
//! one refuses them with a 403); noida behaves as a licensed cluster.

use serde_json::{Map, Value, json};
use std::cmp::Ordering;
use std::collections::HashMap;

use super::search::{CommittedDoc, EsError, SearchOptions, search_with};
use super::vectors::token_name;

const DEFAULT_WINDOW: i64 = 100;
const DEFAULT_CONSTANT: i64 = 60;
const NAMES: &[&str] = &["knn", "rrf", "standard", "text_similarity_reranker"];

/// Whether a search body uses an option this module handles.
pub fn applies(body: &Value) -> bool {
    ["retriever", "sub_searches", "rank"].iter().any(|k| body.get(*k).is_some())
}

/// A parsed retriever.
enum Retriever {
    Standard { spec: Map<String, Value>, filters: Vec<Value> },
    Knn { spec: Map<String, Value>, filters: Vec<Value> },
    Rrf { children: Vec<Retriever>, window: i64, constant: i64, filters: Vec<Value> },
    Rerank { child: Box<Retriever>, inference_id: String, filters: Vec<Value> },
}

/// Lucene's `LevenshteinDistance`: 1 minus the edit distance over the
/// longer length (how Elasticsearch picks "did you mean" suggestions).
fn similarity(a: &str, b: &str) -> f32 {
    let (a, b): (Vec<char>, Vec<char>) = (a.chars().collect(), b.chars().collect());
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    for (i, ca) in a.iter().enumerate() {
        let mut cur = vec![i + 1; b.len() + 1];
        for (j, cb) in b.iter().enumerate() {
            cur[j + 1] = (prev[j + 1] + 1).min(cur[j] + 1).min(prev[j] + usize::from(ca != cb));
        }
        prev = cur;
    }
    let longest = a.len().max(b.len()).max(1);
    1.0 - prev[b.len()] as f32 / longest as f32
}

fn unknown_retriever(name: &str) -> EsError {
    let mut close: Vec<(f32, &str)> =
        NAMES.iter().map(|c| (similarity(name, c), *c)).filter(|(d, _)| *d > 0.5).collect();
    close.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(Ordering::Equal).then(a.1.cmp(b.1)));
    let hint = match close.as_slice() {
        [] => String::new(),
        [(_, one)] => format!(" did you mean [{one}]?"),
        many => format!(
            " did you mean any of [{}]?",
            many.iter().map(|(_, c)| *c).collect::<Vec<_>>().join(", ")
        ),
    };
    EsError::parsing(&format!("unknown retriever [{name}]{hint}"))
        .caused_by("named_object_not_found_exception", &format!("[1:1] unknown field [{name}]"))
}

fn unknown_field(retriever: &str, key: &str) -> EsError {
    EsError::new(400, "x_content_parse_exception", &format!("[{retriever}] unknown field [{key}]"))
}

/// `filter`: one query or an array of them, each checked like a query.
fn filters_of(retriever: &str, v: Option<&Value>, mappings: &Value) -> Result<Vec<Value>, EsError> {
    let list = match v {
        None | Some(Value::Null) => Vec::new(),
        Some(Value::Array(a)) => a.clone(),
        Some(q) => vec![q.clone()],
    };
    for q in &list {
        check_query(retriever, "filter", q, mappings)?;
    }
    Ok(list)
}

/// A query that fails to parse fails the retriever's parsing, as in
/// Elasticsearch (`[standard] failed to parse field [query]`).
fn check_query(retriever: &str, field: &str, q: &Value, mappings: &Value) -> Result<(), EsError> {
    match super::search::eval(q, mappings, &[]) {
        Err(e) if !e.shard && e.kind == "parsing_exception" => Err(EsError::new(
            400,
            "x_content_parse_exception",
            &format!("[{retriever}] failed to parse field [{field}]"),
        )
        .caused_by(&e.kind, &e.reason)),
        _ => Ok(()),
    }
}

fn int_field(retriever: &str, o: &Map<String, Value>, key: &str) -> Result<Option<i64>, EsError> {
    match o.get(key) {
        None => Ok(None),
        Some(v) => {
            v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse().ok())).map(Some).ok_or_else(
                || {
                    EsError::new(
                        400,
                        "x_content_parse_exception",
                        &format!("[{retriever}] failed to parse field [{key}]"),
                    )
                },
            )
        }
    }
}

/// Parses `{"<name>": {...}}`.
fn parse(v: &Value, mappings: &Value) -> Result<Retriever, EsError> {
    let o = match v {
        Value::Object(o) => o,
        other => {
            return Err(EsError::parsing(&format!(
                "Unknown key for a {} in [retriever].",
                token_name(other)
            )));
        }
    };
    let Some((name, spec)) = o.iter().next() else {
        return Err(EsError::parsing("retriever malformed, empty clause found"));
    };
    let Some(spec) = spec.as_object() else {
        return Err(EsError::parsing(&format!(
            "[{name}] retriever malformed, no [START_OBJECT] after retriever name"
        )));
    };
    let parsed = match name.as_str() {
        "standard" => parse_standard(spec, mappings)?,
        "knn" => parse_knn(spec, mappings)?,
        "rrf" => parse_rrf(spec, mappings)?,
        "text_similarity_reranker" => parse_rerank(spec, mappings)?,
        _ => return Err(unknown_retriever(name)),
    };
    if o.len() > 1 {
        return Err(EsError::parsing(&format!(
            "[{name}] malformed retriever, expected [END_OBJECT] but found [FIELD_NAME]"
        )));
    }
    Ok(parsed)
}

const STANDARD_KEYS: &[&str] = &[
    "query",
    "filter",
    "search_after",
    "terminate_after",
    "sort",
    "min_score",
    "collapse",
    "_name",
];

fn parse_standard(o: &Map<String, Value>, mappings: &Value) -> Result<Retriever, EsError> {
    if let Some(k) = o.keys().find(|k| !STANDARD_KEYS.contains(&k.as_str())) {
        return Err(unknown_field("standard", k));
    }
    if let Some(q) = o.get("query") {
        check_query("standard", "query", q, mappings)?;
    }
    if let Some(s) = o.get("sort")
        && !(s.is_array() || s.is_object())
    {
        return Err(EsError::new(
            400,
            "x_content_parse_exception",
            &format!("[standard] sort doesn't support values of type: {}", token_name(s)),
        ));
    }
    let filters = filters_of("standard", o.get("filter"), mappings)?;
    let mut spec = o.clone();
    spec.remove("filter");
    spec.remove("_name");
    Ok(Retriever::Standard { spec, filters })
}

const KNN_KEYS: &[&str] = &[
    "field",
    "query_vector",
    "query_vector_builder",
    "k",
    "num_candidates",
    "filter",
    "similarity",
    "_name",
];

fn parse_knn(o: &Map<String, Value>, mappings: &Value) -> Result<Retriever, EsError> {
    if let Some(k) = o.keys().find(|k| !KNN_KEYS.contains(&k.as_str())) {
        return Err(unknown_field("knn", k));
    }
    let missing: Vec<&str> =
        ["field", "k", "num_candidates"].into_iter().filter(|k| !o.contains_key(*k)).collect();
    if !missing.is_empty() {
        return Err(EsError::new(
            400,
            "illegal_argument_exception",
            &format!("Required [{}]", missing.join(", ")),
        ));
    }
    let filters = filters_of("knn", o.get("filter"), mappings)?;
    let mut spec = o.clone();
    spec.remove("filter");
    spec.remove("_name");
    Ok(Retriever::Knn { spec, filters })
}

fn parse_rrf(o: &Map<String, Value>, mappings: &Value) -> Result<Retriever, EsError> {
    const KEYS: &[&str] = &["retrievers", "rank_window_size", "rank_constant", "filter", "_name"];
    if let Some(k) = o.keys().find(|k| !KEYS.contains(&k.as_str())) {
        return Err(unknown_field("rrf", k));
    }
    let children = match o.get("retrievers") {
        None => Vec::new(),
        Some(Value::Array(a)) => {
            a.iter().map(|c| parse(c, mappings)).collect::<Result<Vec<_>, _>>()?
        }
        Some(other) => {
            return Err(EsError::new(
                400,
                "x_content_parse_exception",
                &format!("[rrf] retrievers doesn't support values of type: {}", token_name(other)),
            ));
        }
    };
    Ok(Retriever::Rrf {
        children,
        window: int_field("rrf", o, "rank_window_size")?.unwrap_or(DEFAULT_WINDOW),
        constant: int_field("rrf", o, "rank_constant")?.unwrap_or(DEFAULT_CONSTANT),
        filters: filters_of("rrf", o.get("filter"), mappings)?,
    })
}

fn parse_rerank(o: &Map<String, Value>, mappings: &Value) -> Result<Retriever, EsError> {
    const KEYS: &[&str] = &[
        "retriever",
        "inference_id",
        "inference_text",
        "field",
        "rank_window_size",
        "min_score",
        "filter",
        "_name",
    ];
    if let Some(k) = o.keys().find(|k| !KEYS.contains(&k.as_str())) {
        return Err(unknown_field("text_similarity_reranker", k));
    }
    let child = match o.get("retriever") {
        Some(c) => Some(parse(c, mappings)?),
        None => None,
    };
    let missing: Vec<&str> = ["retriever", "inference_id", "inference_text", "field"]
        .into_iter()
        .filter(|k| !o.contains_key(*k))
        .collect();
    let Some(child) = child.filter(|_| missing.is_empty()) else {
        return Err(EsError::new(
            400,
            "illegal_argument_exception",
            &format!("Required [{}]", missing.join(", ")),
        ));
    };
    int_field("text_similarity_reranker", o, "rank_window_size")?;
    Ok(Retriever::Rerank {
        child: Box::new(child),
        inference_id: o.get("inference_id").and_then(Value::as_str).unwrap_or("").to_string(),
        filters: filters_of("text_similarity_reranker", o.get("filter"), mappings)?,
    })
}

/// What a retriever contributes to the plain search it is rewritten into.
#[derive(Default)]
struct Extracted {
    /// The search body's own keys (`sort`, `collapse`, ...).
    keys: Map<String, Value>,
    /// Ranked result sets: queries (`sub_searches`) and kNN searches.
    queries: Vec<Value>,
    knn: Vec<Value>,
    rank: Option<Value>,
    rerank: Option<String>,
}

fn illegal(reason: &str) -> EsError {
    EsError::new(400, "illegal_argument_exception", reason)
}

/// Elasticsearch's `extractToSearchSourceBuilder`: a retriever (with the
/// pre-filters of the compound retrievers above it) into a search;
/// `compound` when it is a child of a compound retriever.
fn extract(
    r: Retriever,
    pre: &[Value],
    compound: bool,
    out: &mut Extracted,
) -> Result<(), EsError> {
    match r {
        Retriever::Standard { mut spec, filters } => {
            let filters: Vec<Value> = pre.iter().cloned().chain(filters).collect();
            let query = spec.remove("query");
            if !filters.is_empty() {
                let mut b = json!({"filter": filters});
                if let Some(q) = query {
                    b["must"] = q;
                }
                out.queries.push(json!({"bool": b}));
            } else if let Some(q) = query {
                out.queries.push(q);
            }
            for key in ["search_after", "terminate_after", "sort", "min_score", "collapse"] {
                if let Some(v) = spec.remove(key) {
                    if compound {
                        return Err(illegal(&format!(
                            "[{key}] cannot be used in children of compound retrievers"
                        )));
                    }
                    out.keys.insert(key.to_string(), v);
                }
            }
        }
        Retriever::Knn { mut spec, filters } => {
            let filters: Vec<Value> = pre.iter().cloned().chain(filters).collect();
            if !filters.is_empty() {
                spec.insert("filter".into(), Value::Array(filters));
            }
            out.knn.push(Value::Object(spec));
        }
        Retriever::Rrf { children, window, constant, filters } => {
            if compound {
                return Err(illegal("[rank] cannot be used in children of compound retrievers"));
            }
            let pre: Vec<Value> = pre.iter().cloned().chain(filters).collect();
            for c in children {
                extract(c, &pre, true, out)?;
            }
            if constant < 1 {
                return Err(illegal(
                    "[rank_constant] must be greater than or equal to [1] for [rrf]",
                ));
            }
            out.rank =
                Some(json!({"rrf": {"rank_window_size": window, "rank_constant": constant}}));
        }
        Retriever::Rerank { child, inference_id, filters } => {
            if compound {
                return Err(illegal(
                    "[text_similarity_reranker] cannot be used in children of compound retrievers",
                ));
            }
            let pre: Vec<Value> = pre.iter().cloned().chain(filters).collect();
            extract(*child, &pre, compound, out)?;
            if out.rank.is_some() {
                return Err(illegal(
                    "text similarity rank builder cannot be combined with other rank builders",
                ));
            }
            out.rerank = Some(inference_id);
        }
    }
    Ok(())
}

/// The keys a request may not combine with `retriever`, in the order
/// Elasticsearch lists them.
fn conflicts(body: &Value) -> Vec<&'static str> {
    let mut out = Vec::new();
    if body.get("query").is_some() || body.get("sub_searches").is_some() {
        out.push("query");
    }
    if body.get("knn").is_some() {
        out.push("knn");
    }
    if body.get("search_after").is_some() {
        out.push("search_after");
    }
    if body.get("terminate_after").and_then(Value::as_i64).is_some_and(|n| n != 0) {
        out.push("terminate_after");
    }
    for key in ["sort", "rescore", "min_score", "rank"] {
        if body.get(key).is_some() {
            out.push(key);
        }
    }
    out
}

/// `rank: {"rrf": {...}}`: (rank_window_size, rank_constant).
fn parse_rank(v: &Value) -> Result<(i64, i64), EsError> {
    let o = match v {
        Value::Object(o) => o,
        other => {
            return Err(EsError::parsing(&format!(
                "Unknown key for a {} in [rank].",
                token_name(other)
            )));
        }
    };
    let Some((name, spec)) = o.iter().next() else {
        return Err(EsError::parsing(
            "expected a rank name, but found token [START_OBJECT] for [rank]",
        ));
    };
    if name != "rrf" {
        return Err(EsError::new(
            400,
            "named_object_not_found_exception",
            &format!("[1:1] unknown field [{name}]"),
        ));
    }
    let Some(spec) = spec.as_object() else {
        return Err(EsError::new(
            400,
            "x_content_parse_exception",
            "[rrf] Expected START_OBJECT but was: VALUE_NUMBER",
        ));
    };
    if let Some(k) = spec
        .keys()
        .find(|k| !matches!(k.as_str(), "rank_window_size" | "window_size" | "rank_constant"))
    {
        return Err(unknown_field("rrf", k));
    }
    let window = match int_field("rrf", spec, "rank_window_size")? {
        Some(w) => w,
        None => int_field("rrf", spec, "window_size")?.unwrap_or(DEFAULT_WINDOW),
    };
    let constant = int_field("rrf", spec, "rank_constant")?.unwrap_or(DEFAULT_CONSTANT);
    if constant < 1 {
        return Err(EsError::new(400, "x_content_parse_exception", "[rrf] failed to build")
            .caused_by(
                "illegal_argument_exception",
                "[rank_constant] must be greater than or equal to [1] for [rrf]",
            ));
    }
    Ok((window, constant))
}

/// `sub_searches`: `[{"query": {...}}, ...]`, as queries.
fn parse_sub_searches(v: &Value, mappings: &Value) -> Result<Vec<Value>, EsError> {
    let Value::Array(a) = v else {
        return Err(EsError::parsing(&format!(
            "Unknown key for a {} in [sub_searches].",
            token_name(v)
        )));
    };
    let mut out = Vec::new();
    for s in a {
        let Some(o) = s.as_object() else {
            return Err(EsError::parsing(&format!(
                "Unknown key for a {} in [sub_searches].",
                token_name(s)
            )));
        };
        if let Some(k) = o.keys().find(|k| *k != "query") {
            return Err(unknown_field("sub_search_source_builder", k));
        }
        let Some(q) = o.get("query") else {
            return Err(EsError::new(400, "illegal_argument_exception", "Required [query]"));
        };
        check_query("sub_search_source_builder", "query", q, mappings)?;
        out.push(q.clone());
    }
    Ok(out)
}

/// A search with a `retriever`, `sub_searches` or `rank`.
pub fn search(
    mappings: &Value,
    docs: &[CommittedDoc],
    body: &Value,
    opts: &SearchOptions,
) -> Result<Value, EsError> {
    let mut plain = body.clone();
    let Some(obj) = plain.as_object_mut() else {
        return search_with(mappings, docs, body, opts);
    };
    let mut ex = Extracted::default();
    if let Some(r) = obj.remove("retriever") {
        let parsed = parse(&r, mappings)?;
        let conflicting = conflicts(body);
        if !conflicting.is_empty() {
            return Err(illegal(&format!(
                "cannot specify [retriever] and [{}]",
                conflicting.join(", ")
            )));
        }
        extract(parsed, &[], false, &mut ex)?;
        for (k, v) in std::mem::take(&mut ex.keys) {
            obj.insert(k, v);
        }
    }
    if let Some(s) = obj.remove("sub_searches") {
        let queries = parse_sub_searches(&s, mappings)?;
        if obj.contains_key("query") {
            return Err(illegal("cannot specify field [query] and field [sub_searches]"));
        }
        ex.queries.extend(queries);
    } else if let Some(q) = obj.remove("query") {
        ex.queries.push(q);
    }
    match obj.remove("knn") {
        Some(Value::Array(a)) => ex.knn.extend(a),
        Some(k) => ex.knn.push(k),
        None => {}
    }
    if let Some(r) = obj.remove("rank") {
        parse_rank(&r)?;
        ex.rank = Some(r);
    }
    if let Some(id) = ex.rerank.take() {
        return rerank(mappings, docs, plain, ex, &id, opts);
    }
    let Some(rank) = ex.rank.take() else {
        if ex.queries.len() > 1 {
            return Err(EsError::new(
                400,
                "action_request_validation_exception",
                "Validation Failed: 1: [sub_searches] requires [rank];",
            ));
        }
        set_sources(&mut plain, ex.queries, ex.knn);
        return search_with(mappings, docs, &plain, opts);
    };
    let (window, constant) = parse_rank(&rank)?;
    validate_rank(&plain, &ex, window, opts)?;
    fuse(mappings, docs, plain, ex, window, constant, opts)
}

/// Puts the result sets back as the plain search's `query` (several
/// combined in a `bool` `should`, as Elasticsearch's combined query for
/// the total hits and aggregations) and `knn`.
fn set_sources(body: &mut Value, mut queries: Vec<Value>, knn: Vec<Value>) {
    match queries.len() {
        0 => {}
        1 => body["query"] = queries.pop().unwrap_or_default(),
        _ => body["query"] = json!({"bool": {"should": queries}}),
    }
    if !knn.is_empty() {
        body["knn"] = Value::Array(knn);
    }
}

/// `SearchRequest.validate`'s checks for a request with `rank`.
fn validate_rank(
    body: &Value,
    ex: &Extracted,
    window: i64,
    opts: &SearchOptions,
) -> Result<(), EsError> {
    let size = body.get("size").and_then(Value::as_i64).unwrap_or(10);
    let mut problems: Vec<String> = Vec::new();
    if size == 0 {
        problems.push("[rank] requires [size] greater than [0]".into());
    }
    if size > window {
        problems.push(format!(
            "[rank] requires [rank_window_size: {window}] be greater than or equal to [size: {size}]"
        ));
    }
    if ex.queries.len() + ex.knn.len() < 2 {
        problems.push(
            "[rank] requires a minimum of [2] result sets using a combination of sub searches \
             and/or knn searches"
                .into(),
        );
    }
    if opts.all_hits {
        problems.push("[rank] cannot be used in a scroll context".into());
    }
    for (key, what) in [
        ("rescore", "rescore"),
        ("sort", "sort"),
        ("collapse", "collapse"),
        ("suggest", "suggest"),
        ("highlight", "highlighter"),
        ("pit", "point in time"),
    ] {
        if body.get(key).is_some() {
            problems.push(format!("[rank] cannot be used with [{what}]"));
        }
    }
    if problems.is_empty() {
        return Ok(());
    }
    let listed: String =
        problems.iter().enumerate().map(|(i, p)| format!("{}: {p};", i + 1)).collect();
    Err(EsError::new(
        400,
        "action_request_validation_exception",
        &format!("Validation Failed: {listed}"),
    ))
}

/// A hit's identity: (index, id).
fn key_of(hit: &Value) -> (String, String) {
    let s = |k: &str| hit.get(k).and_then(Value::as_str).unwrap_or("").to_string();
    (s("_index"), s("_id"))
}

/// The `_name` a result set's query carries (on the clause, or on a leaf
/// query's single field), for explanations.
fn query_name(q: &Value) -> Option<String> {
    let (_, spec) = q.as_object()?.iter().next()?;
    let o = spec.as_object()?;
    if let Some(n) = o.get("_name").and_then(Value::as_str) {
        return Some(n.to_string());
    }
    if o.len() == 1 {
        let (_, inner) = o.iter().next()?;
        return inner.get("_name").and_then(Value::as_str).map(str::to_string);
    }
    None
}

/// One fused document: its RRF score, and per result set its 0-based
/// position and score there.
struct Fused {
    key: (String, String),
    score: f32,
    positions: Vec<Option<usize>>,
    scores: Vec<f32>,
    explanations: Vec<Option<Value>>,
}

#[allow(clippy::too_many_arguments)]
fn fuse(
    mappings: &Value,
    docs: &[CommittedDoc],
    mut body: Value,
    ex: Extracted,
    window: i64,
    constant: i64,
    opts: &SearchOptions,
) -> Result<Value, EsError> {
    let size = body.get("size").and_then(Value::as_i64).unwrap_or(10).max(0) as usize;
    let from = body.get("from").and_then(Value::as_i64).unwrap_or(0).max(0) as usize;
    if from + size > 10_000 {
        return Err(EsError::shard_failure(
            "illegal_argument_exception",
            &format!(
                "Result window is too large, from + size must be less than or equal to: [10000] \
                 but was [{}]. See the scroll api for a more efficient way to request large data \
                 sets. This limit can be set by changing the [index.max_result_window] index \
                 level setting.",
                from + size
            ),
        ));
    }
    let explain = body.get("explain").and_then(Value::as_bool).unwrap_or(false);
    // A kNN search without `k` takes the request's `size`.
    let knn: Vec<Value> = ex
        .knn
        .into_iter()
        .map(|mut k| {
            if k.get("k").is_none() && k.is_object() {
                k["k"] = json!(size.max(1));
            }
            k
        })
        .collect();
    // Each result set, ranked on its own: queries first, then kNN searches.
    let mut sets: Vec<(Value, Option<String>)> =
        ex.queries.iter().map(|q| (json!({"query": q}), query_name(q))).collect();
    for k in &knn {
        let name = k.get("_name").and_then(Value::as_str).map(str::to_string);
        sets.push((json!({"knn": k}), name));
    }
    let list_opts =
        SearchOptions { typed: opts.typed, settings: opts.settings.clone(), ..Default::default() };
    let mut fused: Vec<Fused> = Vec::new();
    let mut at: HashMap<(String, String), usize> = HashMap::new();
    let n = sets.len();
    for (i, (set, _)) in sets.iter().enumerate() {
        let mut b = set.clone();
        b["size"] = json!(window.max(0));
        b["_source"] = json!(false);
        b["track_total_hits"] = json!(false);
        if explain {
            b["explain"] = json!(true);
        }
        if let Some(ib) = body.get("indices_boost") {
            b["indices_boost"] = ib.clone();
        }
        let resp = search_with(mappings, docs, &b, &list_opts)?;
        let hits = resp["hits"]["hits"].as_array().cloned().unwrap_or_default();
        for (pos, h) in hits.iter().enumerate() {
            let key = key_of(h);
            let idx = *at.entry(key.clone()).or_insert_with(|| {
                fused.push(Fused {
                    key,
                    score: 0.0,
                    positions: vec![None; n],
                    scores: vec![0.0; n],
                    explanations: vec![None; n],
                });
                fused.len() - 1
            });
            let f = &mut fused[idx];
            f.score += 1.0 / (constant as f32 + (pos + 1) as f32);
            f.positions[i] = Some(pos);
            f.scores[i] = h.get("_score").and_then(Value::as_f64).unwrap_or(0.0) as f32;
            f.explanations[i] = h.get("_explanation").cloned();
        }
    }
    let order: HashMap<(String, String), usize> =
        docs.iter().enumerate().map(|(i, d)| ((d.index.clone(), d.id.clone()), i)).collect();
    fused.sort_by(|a, b| {
        b.score.partial_cmp(&a.score).unwrap_or(Ordering::Equal).then_with(|| {
            for qi in 0..n {
                match (a.positions[qi], b.positions[qi]) {
                    (Some(_), Some(_)) if a.scores[qi] != b.scores[qi] => {
                        return b.scores[qi].partial_cmp(&a.scores[qi]).unwrap_or(Ordering::Equal);
                    }
                    (Some(_), None) => return Ordering::Less,
                    (None, Some(_)) => return Ordering::Greater,
                    _ => {}
                }
            }
            order.get(&a.key).cmp(&order.get(&b.key))
        })
    });
    // The combined search: every document any result set matched counts
    // toward the total and the aggregations.
    set_sources(&mut body, ex.queries, knn);
    if let Some(o) = body.as_object_mut() {
        o.remove("explain");
    }
    let all = SearchOptions { all_hits: true, ..list_opts };
    let mut resp = search_with(mappings, docs, &body, &all)?;
    let by_key: HashMap<(String, String), Value> = resp["hits"]["hits"]
        .as_array()
        .map(|a| a.iter().map(|h| (key_of(h), h.clone())).collect())
        .unwrap_or_default();
    let names: Vec<Option<String>> = sets.into_iter().map(|(_, n)| n).collect();
    let page: Vec<Value> = fused
        .iter()
        .enumerate()
        .skip(from)
        .take(size)
        .filter_map(|(rank, f)| {
            let mut hit = by_key.get(&f.key)?.clone();
            hit["_score"] = json!(f.score);
            hit["_rank"] = json!(rank + 1);
            if explain {
                hit["_explanation"] = explanation(f, &names, constant);
            }
            Some(hit)
        })
        .collect();
    resp["hits"]["hits"] = Value::Array(page);
    resp["hits"]["max_score"] = Value::Null;
    Ok(resp)
}

/// `explain: true` for a fused hit, as Elasticsearch's RRF explains one.
fn explanation(f: &Fused, names: &[Option<String>], constant: i64) -> Value {
    let details: Vec<Value> = (0..f.positions.len())
        .map(|i| {
            let which = match &names[i] {
                Some(n) => format!("[{n}]"),
                None => format!("at index [{i}]"),
            };
            match f.positions[i] {
                None => json!({
                    "value": 0.0,
                    "description": format!("rrf score: [0], result not found in query {which}"),
                    "details": [],
                }),
                Some(p) => {
                    let rank = p + 1;
                    let inner = f.explanations[i].clone().unwrap_or_else(
                        || json!({"value": f.scores[i], "description": "score", "details": []}),
                    );
                    json!({
                        "value": rank,
                        "description": format!(
                            "rrf score: [{}], for rank [{rank}] in query {which} computed as \
                             [1 / ({rank} + {constant})], for matching query with score: ",
                            1.0f32 / (rank as f32 + constant as f32)
                        ),
                        "details": [inner],
                    })
                }
            }
        })
        .collect();
    let ranks: Vec<String> =
        f.positions.iter().map(|p| p.map_or(0, |p| p + 1).to_string()).collect();
    json!({
        "value": f.score,
        "description": format!(
            "rrf score: [{}] computed for initial ranks [{}] with rankConstant: [{constant}] as \
             sum of [1 / (rank + rankConstant)] for each query",
            f.score,
            ranks.join(", ")
        ),
        "details": details,
    })
}

/// `text_similarity_reranker`: the child's results are re-scored by an
/// inference endpoint, which noida never has, so (as on a cluster without
/// that endpoint) any results fail the rank phase.
fn rerank(
    mappings: &Value,
    docs: &[CommittedDoc],
    mut body: Value,
    ex: Extracted,
    inference_id: &str,
    opts: &SearchOptions,
) -> Result<Value, EsError> {
    if ex.queries.len() > 1 {
        return Err(EsError::new(
            400,
            "action_request_validation_exception",
            "Validation Failed: 1: [sub_searches] requires [rank];",
        ));
    }
    set_sources(&mut body, ex.queries, ex.knn);
    let resp = search_with(mappings, docs, &body, opts)?;
    if resp["hits"]["hits"].as_array().is_none_or(Vec::is_empty) {
        return Ok(resp);
    }
    Err(EsError::new(
        404,
        "search_phase_execution_exception",
        "Computing updated ranks for results failed",
    )
    .caused_by(
        "resource_not_found_exception",
        &format!("Inference endpoint not found [{inference_id}]"),
    ))
}

/// What `terminate_after` did to a search.
pub struct Terminated {
    /// More documents matched than were collected.
    pub early: bool,
    /// `hits.total`.
    pub total: usize,
}

/// Whether Lucene counts a query's matches without collecting them
/// (`Weight#count`): its total survives `terminate_after`.
fn countable(q: &Value) -> bool {
    let Some((kind, spec)) = q.as_object().and_then(|o| o.iter().next()) else { return true };
    match kind.as_str() {
        "match_all" | "term" | "exists" => true,
        "match" => spec.as_object().and_then(|o| o.values().next()).is_some_and(|v| {
            let text = v.get("query").unwrap_or(v);
            text.as_str().is_some_and(|s| s.split_whitespace().count() == 1)
        }),
        "constant_score" => spec.get("filter").is_some_and(countable),
        "bool" => {
            let clauses = |k: &str| match spec.get(k) {
                Some(Value::Array(a)) => a.clone(),
                Some(v) => vec![v.clone()],
                None => Vec::new(),
            };
            let required: Vec<Value> =
                clauses("must").into_iter().chain(clauses("filter")).collect();
            required.len() == 1
                && clauses("should").is_empty()
                && clauses("must_not").is_empty()
                && countable(&required[0])
        }
        _ => false,
    }
}

/// `terminate_after`: a shard stops collecting after that many matches
/// (in index order), and the hits and aggregations see only those; the
/// total stays exact when Lucene can count the query's matches directly.
/// (Used by the `standard` retriever's `terminate_after` too.)
pub fn terminate_after(
    body: &Value,
    query: &Value,
    scores: &mut HashMap<usize, f32>,
) -> Option<Terminated> {
    let n = body.get("terminate_after").and_then(Value::as_u64).filter(|n| *n > 0)? as usize;
    let full = scores.len();
    let early = full > n;
    if early {
        let mut ids: Vec<usize> = scores.keys().copied().collect();
        ids.sort_unstable();
        for i in &ids[n..] {
            scores.remove(i);
        }
    }
    let total = if countable(query) { full } else { scores.len() };
    Some(Terminated { early, total })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doc(id: &str, source: Value) -> CommittedDoc {
        CommittedDoc {
            index: "i".into(),
            id: id.into(),
            source,
            version: 1,
            seq: 0,
            full_source: None,
        }
    }

    fn mappings() -> Value {
        json!({"properties": {
            "t": {"type": "text"},
            "k": {"type": "keyword"},
            "n": {"type": "integer"},
            "v": {"type": "dense_vector", "dims": 2, "index": true, "similarity": "l2_norm"},
        }})
    }

    fn docs() -> Vec<CommittedDoc> {
        vec![
            doc("1", json!({"t": "a b", "k": "x", "n": 1, "v": [1.0, 1.0]})),
            doc("2", json!({"t": "a", "k": "y", "n": 2, "v": [2.0, 2.0]})),
            doc("3", json!({"t": "b", "k": "x", "n": 3, "v": [3.0, 3.0]})),
        ]
    }

    fn run(body: Value) -> Result<Value, EsError> {
        search(&mappings(), &docs(), &body, &SearchOptions { typed: true, ..Default::default() })
    }

    fn ids(r: &Value) -> Vec<String> {
        r["hits"]["hits"]
            .as_array()
            .unwrap()
            .iter()
            .map(|h| h["_id"].as_str().unwrap().to_string())
            .collect()
    }

    #[test]
    fn standard_retriever_is_a_plain_search() {
        let r = run(json!({"retriever": {"standard": {
            "query": {"match_all": {}}, "filter": {"term": {"k": "x"}}, "sort": [{"n": "desc"}]}}}))
        .unwrap();
        assert_eq!(ids(&r), ["3", "1"]);
        assert_eq!(r["hits"]["total"]["value"], 2);
    }

    #[test]
    fn knn_retriever_is_a_knn_search() {
        let r = run(json!({"retriever": {"knn": {
            "field": "v", "query_vector": [3.0, 3.0], "k": 2, "num_candidates": 3}}}))
        .unwrap();
        assert_eq!(ids(&r), ["3", "2"]);
        let e =
            run(json!({"retriever": {"knn": {"field": "v", "query_vector": [1, 1]}}})).unwrap_err();
        assert_eq!(e.reason, "Required [k, num_candidates]");
    }

    #[test]
    fn retriever_conflicts_and_parse_errors() {
        let e =
            run(json!({"retriever": {"standard": {}}, "query": {"match_all": {}}, "sort": ["n"]}))
                .unwrap_err();
        assert_eq!(e.reason, "cannot specify [retriever] and [query, sort]");
        assert_eq!(
            run(json!({"retriever": {}})).unwrap_err().reason,
            "retriever malformed, empty clause found"
        );
        assert_eq!(
            run(json!({"retriever": {"standar": {}}})).unwrap_err().reason,
            "unknown retriever [standar] did you mean [standard]?"
        );
        assert_eq!(
            run(json!({"retriever": {"standard": {"x": 1}}})).unwrap_err().kind,
            "x_content_parse_exception"
        );
    }

    #[test]
    fn rrf_fuses_ranks() {
        let r = run(json!({"retriever": {"rrf": {"retrievers": [
            {"standard": {"query": {"match": {"t": "a"}}}},
            {"knn": {"field": "v", "query_vector": [3.0, 3.0], "k": 3, "num_candidates": 3}},
        ], "rank_constant": 1}}}))
        .unwrap();
        // match "a": 2 (shorter field) then 1; knn: 3, 2, 1.
        // 2: 1/2 + 1/3; 1: 1/3 + 1/4; 3: 1/2.
        assert_eq!(ids(&r), ["2", "1", "3"]);
        assert_eq!(r["hits"]["hits"][0]["_rank"], 1);
        assert!((r["hits"]["hits"][0]["_score"].as_f64().unwrap() - 0.833_333).abs() < 1e-5);
        assert_eq!(r["hits"]["max_score"], Value::Null);
        assert_eq!(r["hits"]["total"]["value"], 3);
    }

    #[test]
    fn rrf_validation() {
        let e = run(json!({"retriever": {"rrf": {"retrievers": [
            {"standard": {"query": {"match_all": {}}}}]}}}))
        .unwrap_err();
        assert!(e.reason.contains("requires a minimum of [2] result sets"));
        let e = run(json!({"retriever": {"rrf": {"retrievers": [
            {"standard": {"query": {"match_all": {}}, "sort": ["n"]}},
            {"standard": {"query": {"match_all": {}}}}]}}}))
        .unwrap_err();
        assert_eq!(e.reason, "[sort] cannot be used in children of compound retrievers");
        let e = run(
            json!({"sub_searches": [{"query": {"match_all": {}}}, {"query": {"match_all": {}}}]}),
        )
        .unwrap_err();
        assert_eq!(e.reason, "Validation Failed: 1: [sub_searches] requires [rank];");
    }

    #[test]
    fn terminate_after_collects_in_index_order() {
        let r = run(json!({"retriever": {"standard": {
            "filter": {"bool": {"must_not": {"term": {"k": "y"}}}},
            "sort": [{"n": "desc"}], "terminate_after": 1}}}))
        .unwrap();
        assert_eq!(ids(&r), ["1"]);
        assert_eq!(r["hits"]["total"]["value"], 1);
        assert_eq!(r["terminated_early"], true);
        // A query Lucene counts directly keeps its full total.
        let r = run(json!({"terminate_after": 1, "query": {"term": {"k": "x"}}})).unwrap();
        assert_eq!(r["hits"]["total"]["value"], 2);
    }

    #[test]
    fn reranker_needs_an_inference_endpoint() {
        let e = run(json!({"retriever": {"text_similarity_reranker": {
            "retriever": {"standard": {"query": {"match_all": {}}}},
            "field": "t", "inference_id": "x", "inference_text": "a"}}}))
        .unwrap_err();
        assert_eq!(e.status, 404);
        // Nothing to rerank: the child's (empty) results.
        let r = run(json!({"retriever": {"text_similarity_reranker": {
            "retriever": {"standard": {"query": {"term": {"k": "none"}}}},
            "field": "t", "inference_id": "x", "inference_text": "a"}}}))
        .unwrap();
        assert_eq!(r["hits"]["total"]["value"], 0);
    }

    #[test]
    fn rrf_window_pagination_and_aggregations() {
        // Each result set keeps its top `rank_window_size`; the total and
        // the aggregations cover every match.
        let r = run(json!({
            "sub_searches": [{"query": {"match": {"t": "a"}}}, {"query": {"term": {"k": "x"}}}],
            "rank": {"rrf": {"rank_window_size": 1, "rank_constant": 10}},
            "size": 1,
            "aggs": {"k": {"terms": {"field": "k"}}},
        }))
        .unwrap();
        assert_eq!(r["hits"]["total"]["value"], 3);
        assert_eq!(r["aggregations"]["k"]["buckets"][0]["doc_count"], 2);
        assert_eq!(ids(&r), ["2"]);
        let r = run(json!({
            "query": {"match": {"t": "a"}},
            "knn": {"field": "v", "query_vector": [3.0, 3.0], "k": 3, "num_candidates": 3},
            "rank": {"rrf": {}},
            "from": 1,
            "size": 2,
            "explain": true,
        }))
        .unwrap();
        assert_eq!(ids(&r), ["1", "3"]);
        assert_eq!(r["hits"]["hits"][0]["_rank"], 2);
        let e = &r["hits"]["hits"][1]["_explanation"];
        assert!(e["description"].as_str().unwrap().contains("initial ranks [0, 1]"));
        assert_eq!(e["details"][0]["value"], 0.0);
    }

    #[test]
    fn rrf_errors() {
        let two = json!([{"standard": {"query": {"match_all": {}}}}, {"standard": {"query": {"match_all": {}}}}]);
        let e = run(
            json!({"retriever": {"rrf": {"retrievers": two, "rank_window_size": 5}}, "size": 6}),
        )
        .unwrap_err();
        assert!(e.reason.contains(
            "[rank] requires [rank_window_size: 5] be greater than or equal to [size: 6]"
        ));
        let e = run(json!({"retriever": {"rrf": {"retrievers": [
            {"rrf": {"retrievers": two}}, {"standard": {}}]}}}))
        .unwrap_err();
        assert_eq!(e.reason, "[rank] cannot be used in children of compound retrievers");
        let e = run(json!({"retriever": {"rrf": {"retrievers": two, "rank_constant": 0}}}))
            .unwrap_err();
        assert_eq!(e.reason, "[rank_constant] must be greater than or equal to [1] for [rrf]");
        let e = run(
            json!({"sub_searches": [{"query": {"match_all": {}}}, {"query": {"match_all": {}}}],
                           "rank": {"rrf": {}}, "sort": ["n"], "collapse": {"field": "k"}}),
        )
        .unwrap_err();
        assert_eq!(
            e.reason,
            "Validation Failed: 1: [rank] cannot be used with [sort];2: [rank] cannot be used with [collapse];"
        );
    }
}
