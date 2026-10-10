//! `"profile": true`: the per-shard timing breakdown Elasticsearch adds
//! to a search response. Nothing here is a real Lucene execution, so the
//! shape is what matters: each searched shard with its query tree (the
//! Lucene query each DSL clause becomes, described the way Lucene prints
//! it), collectors, aggregations, the `dfs` phase when one runs, and the
//! `fetch` phase with its sub-phases on the shards that returned hits.
//! Timings are the search's own elapsed time spread over the parts.

use serde_json::{Map, Value, json};

use super::search::analyze_for;

/// One searched shard: its index, shard number, and how many of the
/// returned hits it holds (a shard holding none runs no fetch phase).
pub struct Shard {
    pub index: String,
    pub shard: u64,
    pub fetched: usize,
    /// Documents with a vector in the kNN field (one comparison each).
    pub vectors: usize,
}

/// The node id every profile entry names (22 characters, like a real
/// node id).
const NODE: &str = "noidaNodeId0000000000A";

const QUERY_KEYS: &[&str] = &[
    "set_min_competitive_score_count",
    "match_count",
    "shallow_advance_count",
    "set_min_competitive_score",
    "next_doc",
    "match",
    "next_doc_count",
    "score_count",
    "compute_max_score_count",
    "compute_max_score",
    "advance",
    "advance_count",
    "score",
    "count_weight_count",
    "build_scorer_count",
    "create_weight",
    "shallow_advance",
    "count_weight",
    "create_weight_count",
    "build_scorer",
];

/// A query breakdown: the steps a scoring search takes get time and a
/// count, the others zero.
fn query_breakdown(t: u64, hits: usize) -> Value {
    let mut m = Map::new();
    for k in QUERY_KEYS {
        let v = match *k {
            "create_weight" | "build_scorer" => t / 3,
            "next_doc" | "score" => (t / 20).max(1),
            "create_weight_count" => 1,
            "build_scorer_count" | "next_doc_count" => 2,
            "score_count" => hits as u64,
            _ => 0,
        };
        m.insert((*k).into(), json!(v));
    }
    Value::Object(m)
}

fn node(ty: &str, desc: String, t: u64, hits: usize, children: Vec<Value>) -> Value {
    let mut v = json!({"type": ty, "description": desc, "time_in_nanos": t,
        "breakdown": query_breakdown(t, hits)});
    if !children.is_empty() {
        v["children"] = Value::Array(children);
    }
    v
}

fn scalar(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => "null".into(),
        other => other.to_string(),
    }
}

/// The single `{field: spec}` of a field query, and its value.
fn field_spec<'a>(body: &'a Value, value_key: &str) -> Option<(&'a str, &'a Value)> {
    let (f, spec) =
        body.as_object()?.iter().find(|(k, _)| !matches!(k.as_str(), "boost" | "_name"))?;
    Some((f.as_str(), spec.get(value_key).unwrap_or(spec)))
}

fn mapped_type(mappings: &Value, field: &str) -> Option<String> {
    super::search::resolve_field(mappings, field).1
}

fn is_numeric(ty: Option<&str>) -> bool {
    matches!(
        ty,
        Some(
            "long"
                | "integer"
                | "short"
                | "byte"
                | "double"
                | "float"
                | "half_float"
                | "scaled_float"
                | "unsigned_long"
                | "date"
                | "date_nanos"
        )
    )
}

/// The Lucene query a DSL query becomes, as a profile tree node.
fn describe(q: &Value, mappings: &Value, t: u64, hits: usize) -> Value {
    let Some((kind, body)) = q.as_object().and_then(|o| o.iter().next()) else {
        return node("MatchAllDocsQuery", "*:*".into(), t, hits, vec![]);
    };
    let child_t = (t / 2).max(1);
    match kind.as_str() {
        "match_all" => node("MatchAllDocsQuery", "*:*".into(), t, hits, vec![]),
        "match_none" => node("MatchNoDocsQuery", "MatchNoDocsQuery(\"\")".into(), t, 0, vec![]),
        "term" => {
            let (f, v) = field_spec(body, "value").unwrap_or(("", &Value::Null));
            if is_numeric(mapped_type(mappings, f).as_deref()) {
                let v = scalar(v);
                node("PointRangeQuery", format!("{f}:[{v} TO {v}]"), t, hits, vec![])
            } else {
                node("TermQuery", format!("{f}:{}", scalar(v)), t, hits, vec![])
            }
        }
        "terms" => {
            let (f, v) = field_spec(body, "").unwrap_or(("", &Value::Null));
            let vals: Vec<String> = v.as_array().into_iter().flatten().map(scalar).collect();
            node(
                "MultiTermQueryConstantScoreBlendedWrapper",
                format!("{f}:({})", vals.join(" ")),
                t,
                hits,
                vec![],
            )
        }
        "match" | "match_phrase" => {
            let (f, v) = field_spec(body, "query").unwrap_or(("", &Value::Null));
            let tokens = analyze_for(mappings, f, &scalar(v));
            if kind == "match_phrase" && tokens.len() > 1 {
                return node(
                    "PhraseQuery",
                    format!("{f}:\"{}\"", tokens.join(" ")),
                    t,
                    hits,
                    vec![],
                );
            }
            if tokens.len() == 1 {
                return node("TermQuery", format!("{f}:{}", tokens[0]), t, hits, vec![]);
            }
            let children = tokens
                .iter()
                .map(|tok| node("TermQuery", format!("{f}:{tok}"), child_t, hits, vec![]))
                .collect();
            let desc = tokens.iter().map(|tok| format!("{f}:{tok}")).collect::<Vec<_>>().join(" ");
            node("BooleanQuery", desc, t, hits, children)
        }
        "range" => {
            let (f, spec) = field_spec(body, "").unwrap_or(("", &Value::Null));
            let lo = spec.get("gte").or_else(|| spec.get("gt")).map_or("*".into(), scalar);
            let hi = spec.get("lte").or_else(|| spec.get("lt")).map_or("*".into(), scalar);
            let r = format!("{f}:[{lo} TO {hi}]");
            node(
                "IndexOrDocValuesQuery",
                format!("IndexOrDocValuesQuery(indexQuery={r}, dvQuery={r})"),
                t,
                hits,
                vec![],
            )
        }
        "exists" => {
            let f = body.get("field").map(scalar).unwrap_or_default();
            node("FieldExistsQuery", format!("FieldExistsQuery [field={f}]"), t, hits, vec![])
        }
        "prefix" | "wildcard" | "regexp" | "fuzzy" => {
            let (f, v) = field_spec(body, "value").unwrap_or(("", &Value::Null));
            let v = scalar(v);
            let desc = match kind.as_str() {
                "prefix" => format!("{f}:{v}*"),
                "regexp" => format!("{f}:/{v}/"),
                "fuzzy" => format!("{f}:{v}~2"),
                _ => format!("{f}:{v}"),
            };
            let ty = if kind == "fuzzy" {
                "FuzzyQuery"
            } else {
                "MultiTermQueryConstantScoreBlendedWrapper"
            };
            node(ty, desc, t, hits, vec![])
        }
        "ids" => {
            let ids: Vec<String> = body
                .get("values")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .map(scalar)
                .collect();
            node("TermInSetQuery", format!("_id:({})", ids.join(" ")), t, hits, vec![])
        }
        "bool" => {
            let mut children = vec![];
            let mut parts = vec![];
            for (key, prefix) in [("must", "+"), ("must_not", "-"), ("should", ""), ("filter", "#")]
            {
                let clauses: Vec<Value> = match body.get(key) {
                    Some(Value::Array(a)) => a.clone(),
                    Some(o @ Value::Object(_)) => vec![o.clone()],
                    _ => vec![],
                };
                for c in clauses {
                    let n = describe(&c, mappings, child_t, hits);
                    let d = n["description"].as_str().unwrap_or("").to_string();
                    let d = if n["type"] == "BooleanQuery" { format!("({d})") } else { d };
                    parts.push(format!("{prefix}{d}"));
                    children.push(n);
                }
            }
            if children.is_empty() {
                return node("MatchAllDocsQuery", "*:*".into(), t, hits, vec![]);
            }
            node("BooleanQuery", parts.join(" "), t, hits, children)
        }
        "constant_score" => {
            let inner = body.get("filter").map(|f| describe(f, mappings, child_t, hits));
            let d = inner.as_ref().and_then(|n| n["description"].as_str()).unwrap_or("*:*");
            node(
                "ConstantScoreQuery",
                format!("ConstantScore({d})"),
                t,
                hits,
                inner.into_iter().collect(),
            )
        }
        "nested" => {
            let inner = body.get("query").map(|f| describe(f, mappings, child_t, hits));
            let d = inner.as_ref().and_then(|n| n["description"].as_str()).unwrap_or("*:*");
            node(
                "ESToParentBlockJoinQuery",
                format!("ToParentBlockJoinQuery ({d})"),
                t,
                hits,
                inner.into_iter().collect(),
            )
        }
        other => {
            // Anything else: the query's DSL name in Lucene's style.
            let mut ty: String = other
                .split('_')
                .map(|w| {
                    let mut c = w.chars();
                    c.next()
                        .map(|f| f.to_uppercase().chain(c).collect::<String>())
                        .unwrap_or_default()
                })
                .collect();
            ty.push_str("Query");
            node(&ty, body.to_string(), t, hits, vec![])
        }
    }
}

/// Fetch phase: the sub-phases this request runs, alphabetically by
/// their class names, as Elasticsearch lists them.
fn fetch(req: &Value, hits: usize, t: u64) -> Value {
    let stored_none = match req.get("stored_fields") {
        Some(Value::String(s)) => s == "_none_",
        Some(Value::Array(a)) => a.iter().any(|v| v == "_none_"),
        _ => false,
    };
    let source_off = matches!(req.get("_source"), Some(Value::Bool(false)))
        || (req.get("script_fields").is_some() && req.get("_source").is_none())
        || (req.get("stored_fields").is_some() && req.get("_source").is_none());
    let loads_source = !stored_none;
    let per = (t / 8).max(1);
    let mut f = json!({
        "type": "fetch", "description": "", "time_in_nanos": t,
        "breakdown": {
            "load_stored_fields": per, "load_source": if loads_source { per } else { 0 },
            "load_stored_fields_count": hits, "next_reader_count": 1,
            "load_source_count": if loads_source { hits } else { 0 }, "next_reader": per,
        },
        "debug": {"stored_fields": if stored_none { json!([]) } else { json!(["_id", "_routing", "_source"]) }},
    });
    if stored_none {
        return f;
    }
    let phase = |name: &str| {
        json!({"type": name, "description": "", "time_in_nanos": per,
            "breakdown": {"process_count": hits, "process": per / 2 + 1, "next_reader": per / 2 + 1, "next_reader_count": 1}})
    };
    let has = |k: &str| req.get(k).is_some_and(|v| !v.is_null() && v != &json!(false));
    let sorted = req.get("sort").is_some();
    let mut children = vec![];
    if has("explain") {
        children.push(phase("ExplainPhase"));
    }
    if has("docvalue_fields") {
        children.push(phase("FetchDocValuesPhase"));
    }
    children.push(phase("FetchFieldsPhase"));
    if sorted && has("track_scores") {
        children.push(phase("FetchScorePhase"));
    }
    if !source_off {
        let mut p = phase("FetchSourcePhase");
        p["debug"] = json!({"fast_path": hits});
        children.push(p);
    }
    if has("version") {
        children.push(phase("FetchVersionPhase"));
    }
    if has("highlight") {
        children.push(phase("HighlightPhase"));
    }
    let text = req.get("query").map(Value::to_string).unwrap_or_default();
    if text.contains("\"inner_hits\"") {
        children.push(phase("InnerHitsPhase"));
    }
    if text.contains("\"_name\"") {
        children.push(phase("MatchedQueriesPhase"));
    }
    if has("script_fields") {
        children.push(phase("ScriptFieldsPhase"));
    }
    if has("seq_no_primary_term") {
        children.push(phase("SeqNoPrimaryTermPhase"));
    }
    children.push(phase("StoredFieldsPhase"));
    f["children"] = Value::Array(children);
    f
}

fn aggregations(req: &Value, t: u64) -> Vec<Value> {
    let Some(aggs) = req.get("aggs").or_else(|| req.get("aggregations")).and_then(Value::as_object)
    else {
        return vec![];
    };
    aggs.iter()
        .map(|(name, spec)| {
            let kind = spec
                .as_object()
                .and_then(|o| {
                    o.keys().find(|k| !matches!(k.as_str(), "aggs" | "aggregations" | "meta"))
                })
                .cloned()
                .unwrap_or_default();
            let ty = match kind.as_str() {
                "terms" => "GlobalOrdinalsStringTermsAggregator".to_string(),
                other => {
                    let mut s: String = other
                        .split('_')
                        .map(|w| {
                            let mut c = w.chars();
                            c.next()
                                .map(|f| f.to_uppercase().chain(c).collect::<String>())
                                .unwrap_or_default()
                        })
                        .collect();
                    s.push_str("Aggregator");
                    s
                }
            };
            json!({"type": ty, "description": name, "time_in_nanos": t,
                "breakdown": {"reduce": 0, "build_aggregation_count": 1, "post_collection": 1,
                    "initialize_count": 1, "reduce_count": 0, "collect_count": 1,
                    "post_collection_count": 1, "build_leaf_collector": t / 4,
                    "build_aggregation": t / 4, "build_leaf_collector_count": 1,
                    "initialize": 1, "collect": t / 4}})
        })
        .collect()
}

/// A query the way Elasticsearch prints one back (alias filters in
/// `_search_shards`): a field query's shorthand `{"f": "v"}` expanded to
/// `{"f": {"value": "v"}}` (`query` for the match family); compound
/// queries normalized clause by clause.
pub fn canonical_query(q: &Value) -> Value {
    let Some((kind, body)) = q.as_object().filter(|o| o.len() == 1).and_then(|o| o.iter().next())
    else {
        return q.clone();
    };
    let key = match kind.as_str() {
        "term" | "prefix" | "wildcard" | "regexp" | "fuzzy" => Some("value"),
        "match" | "match_phrase" | "match_phrase_prefix" | "match_bool_prefix" => Some("query"),
        _ => None,
    };
    if let Some(key) = key
        && let Some(m) = body.as_object()
    {
        let fixed: Map<String, Value> = m
            .iter()
            .map(|(f, v)| {
                let v = if v.is_object() { v.clone() } else { json!({ key: v }) };
                (f.clone(), v)
            })
            .collect();
        return json!({ kind.as_str(): fixed });
    }
    if kind == "bool"
        && let Some(m) = body.as_object()
    {
        let mut out = Map::new();
        for (k, v) in m {
            let v = match v {
                Value::Array(a) => Value::Array(a.iter().map(canonical_query).collect()),
                Value::Object(_) if k != "minimum_should_match" => json!([canonical_query(v)]),
                other => other.clone(),
            };
            out.insert(k.clone(), v);
        }
        out.entry("boost").or_insert(json!(1.0));
        return json!({"bool": out});
    }
    q.clone()
}

/// The response's `profile` section for a search `req` over `shards`.
/// `dfs` is a `dfs_query_then_fetch` search; `elapsed` the search's time.
pub fn build(req: &Value, mappings: &Value, shards: &[Shard], dfs: bool, elapsed: u64) -> Value {
    let t = elapsed.max(1000);
    let total_shards = shards.len();
    let knn: Vec<Value> = match req.get("knn") {
        Some(Value::Array(a)) => a.clone(),
        Some(k @ Value::Object(_)) => vec![k.clone()],
        _ => vec![],
    };
    let out: Vec<Value> = shards
        .iter()
        .map(|sh| {
            let mut entry = json!({
                "id": format!("[{NODE}][{}][{}]", sh.index, sh.shard),
                "node_id": NODE,
                "shard_id": sh.shard,
                "index": sh.index,
                "cluster": "(local)",
            });
            // A single-shard dfs search skips the dfs round; kNN always
            // runs one.
            if (dfs && total_shards > 1) || !knn.is_empty() {
                let mut d = json!({"statistics": {"type": "statistics",
                    "description": "collect term statistics", "time_in_nanos": t / 10,
                    "breakdown": {"term_statistics": 0, "create_weight": t / 10,
                        "collection_statistics": 0, "collection_statistics_count": 0,
                        "term_statistics_count": 0, "rewrite_count": 0,
                        "create_weight_count": 1, "rewrite": 0}}});
                if !knn.is_empty() {
                    let entries: Vec<Value> = knn
                        .iter()
                        .map(|k| {
                            let cands = k
                                .get("num_candidates")
                                .and_then(Value::as_u64)
                                .or_else(|| k.get("k").and_then(Value::as_u64).map(|n| (n * 3 / 2).clamp(100, 10_000)))
                                .unwrap_or(100);
                            json!({
                                "vector_operations_count": sh.vectors,
                                "query": [node("DocAndScoreQuery", format!("DocAndScore[{cands}]"), t / 4, sh.vectors.max(1), vec![])],
                                "rewrite_time": t / 4,
                                "collector": [{"name": "SimpleTopScoreDocCollector",
                                    "reason": "search_top_hits", "time_in_nanos": t / 10}],
                            })
                        })
                        .collect();
                    d["knn"] = Value::Array(entries);
                }
                entry["dfs"] = d;
            }
            let query = if knn.is_empty() {
                req.get("query").cloned().unwrap_or_else(|| json!({"match_all": {}}))
            } else {
                json!({"knn_score_doc": {}})
            };
            let size = req.get("size").and_then(Value::as_i64).unwrap_or(10);
            let has_aggs = req.get("aggs").or_else(|| req.get("aggregations")).is_some();
            let mut qnode = if knn.is_empty() {
                describe(&query, mappings, t / 2, sh.fetched)
            } else {
                node("KnnScoreDocQuery", "ScoreAndDocQuery".into(), t / 2, sh.fetched, vec![])
            };
            if size == 0 && has_aggs && qnode["type"] == "MatchAllDocsQuery" {
                let inner = qnode.clone();
                qnode = node("ConstantScoreQuery", "ConstantScore(*:*)".into(), t / 2, 0, vec![inner]);
            }
            let mut children = vec![];
            if size > 0 {
                children.push(json!({"name": "SimpleTopScoreDocCollector", "reason": "search_top_hits", "time_in_nanos": t / 10}));
            } else {
                children.push(json!({"name": "PartialHitCountCollector", "reason": "search_count", "time_in_nanos": t / 10}));
            }
            if let Some(aggs) = req.get("aggs").or_else(|| req.get("aggregations")).and_then(Value::as_object) {
                let names: Vec<&str> = aggs.keys().map(String::as_str).collect();
                children.push(json!({"name": format!("AggregatorCollector: [{}]", names.join(", ")),
                    "reason": "aggregation", "time_in_nanos": t / 10}));
            }
            entry["searches"] = json!([{
                "query": [qnode],
                "rewrite_time": t / 20,
                "collector": [{"name": "QueryPhaseCollector", "reason": "search_query_phase",
                    "time_in_nanos": t / 5, "children": children}],
            }]);
            entry["aggregations"] = Value::Array(aggregations(req, t / 4));
            if sh.fetched > 0 {
                entry["fetch"] = fetch(req, sh.fetched, t / 3);
            }
            entry
        })
        .collect();
    json!({"shards": out})
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fetch_phases_follow_the_request() {
        let shards = [Shard { index: "test".into(), shard: 0, fetched: 1, vectors: 0 }];
        let p =
            build(&json!({"_source": false, "fields": ["k"]}), &json!({}), &shards, false, 5000);
        let f = &p["shards"][0]["fetch"];
        assert_eq!(f["children"][0]["type"], "FetchFieldsPhase");
        assert_eq!(f["children"][1]["type"], "StoredFieldsPhase");
        assert_eq!(p["shards"][0]["node_id"].as_str().unwrap().len(), 22);
        let p = build(&json!({"stored_fields": "_none_"}), &json!({}), &shards, false, 5000);
        assert_eq!(p["shards"][0]["fetch"]["debug"]["stored_fields"], json!([]));
        assert!(p["shards"][0]["fetch"].get("children").is_none());
        let q = json!({"bool": {"must": [{"match": {"t": "brown fox"}}], "filter": [{"term": {"k": "x"}}]}});
        let d = describe(&q, &json!({}), 100, 1);
        assert_eq!(d["description"], "+(t:brown t:fox) #k:x");
    }
}
