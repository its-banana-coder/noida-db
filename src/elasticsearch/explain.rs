//! Score explanations and query descriptions: a hit's `_explanation`
//! (`explain: true`), the explain API (`/<index>/_explain/<id>`) and the
//! validate API (`/<index>/_validate/query`), whose `explanation` is the
//! query in Lucene's `toString` form (`text:big text:wolf`, `*:*`).

use std::collections::HashMap;

use serde_json::{Map, Value, json};

use super::engine::{Engine, error, parse_json};
use super::fuzzy::FieldTerms;
use super::query_string;
use super::rescore::Step;
use super::scoring;
use super::search::{CommittedDoc, analyze_for, eval, field_and_spec, query_text};

fn num_text(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => "*".into(),
        other => other.to_string(),
    }
}

fn spec_value<'a>(spec: &'a Value, key: &str) -> &'a Value {
    match spec {
        Value::Object(o) => o.get(key).unwrap_or(&Value::Null),
        other => other,
    }
}

fn boosted(s: String, spec: &Value) -> String {
    match spec.get("boost").and_then(Value::as_f64) {
        Some(b) if b != 1.0 => format!("({s})^{b:?}"),
        _ => s,
    }
}

/// A query as Lucene prints it.
pub fn lucene_string(q: &Value, mappings: &Value) -> String {
    let Some((kind, body)) = q.as_object().and_then(|o| o.iter().next()) else {
        return String::new();
    };
    let clause = |v: &Value| lucene_string(v, mappings);
    match kind.as_str() {
        "match_all" => boosted("*:*".into(), body),
        "match_none" => "MatchNoDocsQuery(\"User requested \"match_none\" query.\")".into(),
        "term" => match field_and_spec(body) {
            Some((f, spec)) => {
                let v = match spec {
                    Value::Object(o) => {
                        o.get("value").or_else(|| o.get("term")).unwrap_or(&Value::Null)
                    }
                    other => other,
                };
                boosted(format!("{f}:{}", num_text(v)), spec)
            }
            None => String::new(),
        },
        "terms" => match body.as_object().and_then(|o| o.iter().find(|(k, _)| *k != "boost")) {
            Some((f, Value::Array(vals))) => {
                format!("{f}:({})", vals.iter().map(num_text).collect::<Vec<_>>().join(" "))
            }
            _ => String::new(),
        },
        "match" | "match_phrase" | "match_bool_prefix" | "match_phrase_prefix" => {
            let Some((f, spec)) = field_and_spec(body) else { return String::new() };
            let text = query_text(Some(spec_value(spec, "query")));
            let terms = analyze_for(mappings, f, &text);
            let and = spec
                .get("operator")
                .and_then(Value::as_str)
                .is_some_and(|o| o.eq_ignore_ascii_case("and"));
            let s = match (kind.as_str(), terms.len()) {
                (_, 0) => {
                    "MatchNoDocsQuery(\"Matching no documents because no terms present\")".into()
                }
                ("match", 1) => format!("{f}:{}", terms[0]),
                ("match", _) => terms
                    .iter()
                    .map(|t| if and { format!("+{f}:{t}") } else { format!("{f}:{t}") })
                    .collect::<Vec<_>>()
                    .join(" "),
                ("match_bool_prefix", _) => {
                    let (last, head) = terms.split_last().unwrap();
                    let mut parts: Vec<String> = head.iter().map(|t| format!("{f}:{t}")).collect();
                    parts.push(format!("{f}:{last}*"));
                    parts.join(" ")
                }
                ("match_phrase_prefix", _) => format!("{f}:\"{}*\"", terms.join(" ")),
                _ => format!("{f}:\"{}\"", terms.join(" ")),
            };
            boosted(s, spec)
        }
        "multi_match" => {
            let text = query_text(body.get("query"));
            let fields: Vec<String> = body
                .get("fields")
                .and_then(Value::as_array)
                .map(|a| a.iter().filter_map(Value::as_str).map(str::to_string).collect())
                .unwrap_or_default();
            let parts: Vec<String> = fields
                .iter()
                .map(|f| clause(&json!({"match": {f.as_str(): {"query": text}}})))
                .collect();
            format!("({})", parts.join(" | "))
        }
        "prefix" => match field_and_spec(body) {
            Some((f, spec)) => {
                boosted(format!("{f}:{}*", num_text(spec_value(spec, "value"))), spec)
            }
            None => String::new(),
        },
        "wildcard" => match field_and_spec(body) {
            Some((f, spec)) => {
                let v = match spec {
                    Value::Object(o) => {
                        o.get("value").or_else(|| o.get("wildcard")).unwrap_or(&Value::Null)
                    }
                    other => other,
                };
                boosted(format!("{f}:{}", num_text(v)), spec)
            }
            None => String::new(),
        },
        "regexp" => match field_and_spec(body) {
            Some((f, spec)) => {
                boosted(format!("{f}:/{}/", num_text(spec_value(spec, "value"))), spec)
            }
            None => String::new(),
        },
        "fuzzy" => match field_and_spec(body) {
            Some((f, spec)) => {
                let edits = match spec.get("fuzziness") {
                    Some(fz) => super::fuzzy::parse_fuzziness(fz).ok(),
                    None => None,
                }
                .unwrap_or(super::fuzzy::Fuzziness::Auto(3, 6));
                let v = num_text(spec_value(spec, "value"));
                format!("{f}:{v}~{}", edits.edits(&v))
            }
            None => String::new(),
        },
        "range" => match field_and_spec(body) {
            Some((f, spec)) => {
                let lo = spec.get("gte").or_else(|| spec.get("gt")).or_else(|| spec.get("from"));
                let hi = spec.get("lte").or_else(|| spec.get("lt")).or_else(|| spec.get("to"));
                let open = if spec.get("gt").is_some() { '{' } else { '[' };
                let close = if spec.get("lt").is_some() { '}' } else { ']' };
                format!(
                    "{f}:{open}{} TO {}{close}",
                    lo.map_or("*".into(), num_text),
                    hi.map_or("*".into(), num_text)
                )
            }
            None => String::new(),
        },
        "exists" => format!(
            "FieldExistsQuery [field={}]",
            body.get("field").and_then(Value::as_str).unwrap_or_default()
        ),
        "ids" => format!(
            "_id:({})",
            body.get("values")
                .and_then(Value::as_array)
                .map(|a| a.iter().map(num_text).collect::<Vec<_>>().join(" "))
                .unwrap_or_default()
        ),
        "constant_score" => {
            let inner = body.get("filter").map(clause).unwrap_or_default();
            boosted(format!("ConstantScore({inner})"), body)
        }
        "nested" => format!(
            "ToParentBlockJoinQuery ({})",
            body.get("query").map(clause).unwrap_or_default()
        ),
        "dis_max" => {
            let qs: Vec<String> = body
                .get("queries")
                .and_then(Value::as_array)
                .map(|a| a.iter().map(clause).collect())
                .unwrap_or_default();
            format!("({})", qs.join(" | "))
        }
        "query_string" => match query_string::query_string(body, mappings) {
            Ok(q) => clause(&q),
            Err(_) => String::new(),
        },
        "simple_query_string" => match query_string::simple_query_string(body, mappings) {
            Ok(q) => clause(&q),
            Err(_) => String::new(),
        },
        "bool" => {
            let mut parts = Vec::new();
            let list = |k: &str| -> Vec<Value> {
                match body.get(k) {
                    Some(Value::Array(a)) => a.clone(),
                    Some(o) => vec![o.clone()],
                    None => vec![],
                }
            };
            let wrap = |v: &Value| {
                let s = clause(v);
                if v.get("bool").is_some() { format!("({s})") } else { s }
            };
            for (k, prefix) in [("must", "+"), ("filter", "#"), ("must_not", "-"), ("should", "")] {
                for c in list(k) {
                    parts.push(format!("{prefix}{}", wrap(&c)));
                }
            }
            let mut s = parts.join(" ");
            if let Some(m) = body.get("minimum_should_match") {
                s = format!("({s})~{}", num_text(m));
            }
            boosted(s, body)
        }
        other => format!("{other}({body})"),
    }
}

fn node(value: f32, description: &str, details: Vec<Value>) -> Value {
    json!({"value": value, "description": description, "details": details})
}

/// BM25's explanation of one term in one document, as Lucene words it.
fn bm25(field: &str, term: &str, ft: &FieldTerms, idx: usize, boost: f32) -> Option<Value> {
    let toks = ft.toks.get(idx)?;
    let freq = toks.iter().filter(|t| *t == term).count() as u32;
    if freq == 0 {
        return None;
    }
    let n = ft.doc_freq(term);
    let big_n = ft.toks.iter().filter(|t| !t.is_empty()).count() as u64;
    let total: u64 = ft.toks.iter().map(|t| t.len() as u64).sum();
    let avg = if big_n > 0 { total as f32 / big_n as f32 } else { 1.0 };
    let dl = scoring::norm_doc_len(toks.len() as u32).max(1) as f32;
    let idf = scoring::idf(n, big_n.max(1));
    let f = freq as f32;
    let tf = f / (f + scoring::K1 * (1.0 - scoring::B + scoring::B * dl / avg));
    let b = boost * (scoring::K1 + 1.0);
    let score = b * idf * tf;
    Some(node(
        score,
        &format!("weight({field}:{term} in {idx}) [PerFieldSimilarity], result of:"),
        vec![node(
            score,
            &format!("score(freq={f:?}), computed as boost * idf * tf from:"),
            vec![
                node(b, "boost", vec![]),
                json!({"value": idf, "description": "idf, computed as log(1 + (N - n + 0.5) / (n + 0.5)) from:",
                       "details": [
                           {"value": n, "description": "n, number of documents containing term", "details": []},
                           {"value": big_n, "description": "N, total number of documents with field", "details": []}]}),
                node(
                    tf,
                    "tf, computed as freq / (freq + k1 * (1 - b + b * dl / avgdl)) from:",
                    vec![
                        node(f, "freq, occurrences of term within document", vec![]),
                        node(scoring::K1, "k1, term saturation parameter", vec![]),
                        node(scoring::B, "b, length normalization parameter", vec![]),
                        node(dl, "dl, length of field", vec![]),
                        node(avg, "avgdl, average length of field", vec![]),
                    ],
                ),
            ],
        )],
    ))
}

/// Why `q` scored `score` for document `idx`.
fn explain_query(
    q: &Value,
    mappings: &Value,
    docs: &[CommittedDoc],
    idx: usize,
    score: f32,
) -> Value {
    let Some((kind, body)) = q.as_object().and_then(|o| o.iter().next()) else {
        return node(score, "", vec![]);
    };
    match kind.as_str() {
        "bool" => {
            let mut details = Vec::new();
            for k in ["must", "should"] {
                let list = match body.get(k) {
                    Some(Value::Array(a)) => a.clone(),
                    Some(o) => vec![o.clone()],
                    None => vec![],
                };
                for c in list {
                    if let Ok(m) = eval(&c, mappings, docs)
                        && let Some(s) = m.get(&idx)
                    {
                        details.push(explain_query(&c, mappings, docs, idx, *s));
                    }
                }
            }
            if details.is_empty() {
                return node(
                    score,
                    &format!("ConstantScore({})", lucene_string(q, mappings)),
                    vec![],
                );
            }
            node(score, "sum of:", details)
        }
        "match" => {
            let Some((f, spec)) = field_and_spec(body) else { return node(score, "", vec![]) };
            let options = spec.as_object().is_some_and(|o| {
                ["fuzziness", "minimum_should_match", "analyzer", "zero_terms_query"]
                    .iter()
                    .any(|k| o.contains_key(*k))
            });
            let text = query_text(Some(spec_value(spec, "query")));
            let terms = analysis_terms(mappings, f, &text);
            let boost = spec.get("boost").and_then(Value::as_f64).unwrap_or(1.0) as f32;
            if options || terms.is_empty() {
                return node(score, &lucene_string(q, mappings), vec![]);
            }
            let ft = FieldTerms::new(mappings, docs, f);
            let parts: Vec<Value> =
                terms.iter().filter_map(|t| bm25(f, t, &ft, idx, boost)).collect();
            match parts.len() {
                0 => node(score, &lucene_string(q, mappings), vec![]),
                1 => parts.into_iter().next().unwrap(),
                _ => node(score, "sum of:", parts),
            }
        }
        "match_all" => node(score, "*:*", vec![]),
        _ => node(score, &lucene_string(q, mappings), vec![]),
    }
}

fn analysis_terms(mappings: &Value, field: &str, text: &str) -> Vec<String> {
    match super::search::resolve_field(mappings, field).1.as_deref() {
        None | Some("text" | "match_only_text") => analyze_for(mappings, field, text),
        _ => vec![],
    }
}

/// A hit's `_explanation`: the query's, then each rescorer's.
pub fn hit_explanation(
    query: &Value,
    mappings: &Value,
    docs: &[CommittedDoc],
    idx: usize,
    score: f32,
    steps: Option<&Vec<Step>>,
) -> Value {
    let Some(steps) = steps.filter(|s| !s.is_empty()) else {
        let mut e = explain_query(query, mappings, docs, idx, score);
        e["value"] = json!(score);
        return e;
    };
    let mut e = explain_query(query, mappings, docs, idx, steps[0].primary);
    for st in steps {
        let primary = node(
            st.primary * st.query_weight,
            "product of:",
            vec![e, node(st.query_weight, "primaryWeight", vec![])],
        );
        e = match st.secondary {
            Some(s) => {
                let secondary = node(
                    s * st.rescore_weight,
                    "product of:",
                    vec![
                        node(s, "rescore score", vec![]),
                        node(st.rescore_weight, "secondaryWeight", vec![]),
                    ],
                );
                let (a, b) = (st.primary * st.query_weight, s * st.rescore_weight);
                let (desc, value) = match st.mode.as_str() {
                    "multiply" => ("product of:", a * b),
                    "avg" => ("avg of:", (a + b) / 2.0),
                    "max" => ("max of:", a.max(b)),
                    "min" => ("min of:", a.min(b)),
                    _ => ("sum of:", a + b),
                };
                node(value, desc, vec![primary, secondary])
            }
            None => primary,
        };
    }
    e["value"] = json!(score);
    e
}

/// The Java class Elasticsearch names an error type with in a validate
/// API `error`.
fn java_class(kind: &str) -> String {
    match kind {
        "parsing_exception" => "org.elasticsearch.common.ParsingException".into(),
        "query_shard_exception" => "org.elasticsearch.index.query.QueryShardException".into(),
        "illegal_argument_exception" => "java.lang.IllegalArgumentException".into(),
        "named_object_not_found_exception" => {
            "org.elasticsearch.xcontent.NamedObjectNotFoundException".into()
        }
        "number_format_exception" => "java.lang.NumberFormatException".into(),
        other => format!("org.elasticsearch.ElasticsearchException[{other}]"),
    }
}

/// An error response's root cause (and its cause) as `Class: reason`.
fn error_text(resp: &Value) -> String {
    let err = &resp["error"];
    // A failure inside a compound query's clause.
    if err["type"] == "x_content_parse_exception" {
        return format!(
            "org.elasticsearch.common.ParsingException: Failed to parse; \
             org.elasticsearch.xcontent.XContentParseException: {}",
            err["reason"].as_str().unwrap_or_default()
        );
    }
    let root = err.get("root_cause").and_then(|r| r.get(0)).unwrap_or(err);
    let mut out = format!(
        "{}: {}",
        java_class(root["type"].as_str().unwrap_or("exception")),
        root["reason"].as_str().unwrap_or_default()
    );
    let cause =
        err.get("caused_by").filter(|c| c["type"] != root["type"] || c["reason"] != root["reason"]);
    if let Some(c) = cause {
        out.push_str(&format!(
            "; {}: {}",
            java_class(c["type"].as_str().unwrap_or("exception")),
            c["reason"].as_str().unwrap_or_default()
        ));
    }
    out
}

/// The `query_string` a `?q=` search runs.
fn uri_query(q: &HashMap<String, String>) -> Option<Value> {
    let text = q.get("q")?;
    let mut qs = json!({"query": text});
    for (param, key) in [
        ("df", "default_field"),
        ("default_operator", "default_operator"),
        ("analyze_wildcard", "analyze_wildcard"),
        ("lenient", "lenient"),
        ("analyzer", "analyzer"),
    ] {
        if let Some(v) = q.get(param) {
            qs[key] = match v.as_str() {
                "true" => json!(true),
                "false" => json!(false),
                other => json!(other),
            };
        }
    }
    Some(json!({"query_string": qs}))
}

impl Engine {
    /// `GET|POST /<index>/_explain/<id>`: whether the document matches
    /// the query, and how it scores.
    pub(super) fn explain_api(
        &self,
        method: &str,
        target: &str,
        index: &str,
        id: &str,
        q: &HashMap<String, String>,
        body: &[u8],
    ) -> (u16, Value) {
        if method != "GET" && method != "POST" {
            return (405, error("method_not_allowed_exception", "Incorrect HTTP method", 405));
        }
        let Some(req) = parse_json(body) else {
            return (400, super::engine::malformed_body());
        };
        if let Some(bad) = req.as_object().and_then(|o| o.keys().find(|k| *k != "query")) {
            return (
                400,
                error("parsing_exception", &format!("request does not support [{bad}]"), 400),
            );
        }
        let query = match (req.get("query"), q.contains_key("q")) {
            (Some(qv), false) => qv.clone(),
            (_, true) => json!({"match_all": {}}),
            (None, false) => {
                return (
                    400,
                    error(
                        "action_request_validation_exception",
                        "Validation Failed: 1: query is missing;",
                        400,
                    ),
                );
            }
        };
        let filter = json!({"ids": {"values": [id]}});
        let mut search = json!({"query": query, "post_filter": filter, "explain": true,
                                "seq_no_primary_term": true, "size": 1});
        let wants_source = ["_source", "_source_includes", "_source_excludes", "stored_fields"]
            .iter()
            .any(|k| q.contains_key(*k));
        let mut params: HashMap<String, String> = q
            .iter()
            .filter(|(k, _)| {
                matches!(
                    k.as_str(),
                    "q" | "df"
                        | "default_operator"
                        | "analyze_wildcard"
                        | "lenient"
                        | "analyzer"
                        | "_source"
                        | "_source_includes"
                        | "_source_excludes"
                        | "stored_fields"
                )
            })
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        if !wants_source {
            search["_source"] = json!(false);
            params.remove("_source");
        }
        let bytes = serde_json::to_vec(&search).unwrap_or_default();
        let (status, resp) = self.search_or_count("POST", "_search", target, &params, &bytes);
        if status >= 400 {
            return (status, resp);
        }
        let Some(hit) = resp["hits"]["hits"].get(0) else {
            let probe = json!({"query": {"ids": {"values": [id]}}, "size": 0});
            let bytes = serde_json::to_vec(&probe).unwrap_or_default();
            let (_, r) = self.search_or_count("POST", "_search", index, &HashMap::new(), &bytes);
            let exists = r["hits"]["total"]["value"].as_u64().unwrap_or(0) > 0;
            let mut out = json!({"_index": index, "_id": id, "matched": false});
            if !exists {
                return (404, out);
            }
            out["explanation"] =
                json!({"value": 0.0, "description": "no matching term", "details": []});
            return (200, out);
        };
        let mut out = json!({
            "_index": hit["_index"],
            "_id": hit["_id"],
            "matched": true,
            "explanation": hit["_explanation"],
        });
        if wants_source && q.get("_source").map(String::as_str) != Some("false") {
            let mut get = json!({"_seq_no": hit["_seq_no"], "_primary_term": hit["_primary_term"], "found": true});
            if let Some(src) = hit.get("_source") {
                get["_source"] = src.clone();
            }
            if let Some(f) = hit.get("fields") {
                get["fields"] = f.clone();
            }
            out["get"] = get;
        }
        (200, out)
    }

    /// `GET|POST [/<index>]/_validate/query`: whether the query parses
    /// and can run, optionally with its Lucene form per index.
    pub(super) fn validate_api(
        &self,
        method: &str,
        target: &str,
        q: &HashMap<String, String>,
        body: &[u8],
    ) -> (u16, Value) {
        if method != "GET" && method != "POST" {
            return (405, error("method_not_allowed_exception", "Incorrect HTTP method", 405));
        }
        let explain = q.get("explain").is_some_and(|v| v.is_empty() || v == "true");
        let invalid = |reason: Option<String>| {
            let mut v = json!({"valid": false});
            if explain && let Some(r) = reason {
                v["error"] = json!(r);
            }
            (200, v)
        };
        let Some(req) = parse_json(body) else {
            return (400, super::engine::malformed_body());
        };
        if let Some(bad) = req.as_object().and_then(|o| o.keys().find(|k| *k != "query")) {
            return invalid(Some(format!(
                "org.elasticsearch.common.ParsingException: request does not support [{bad}]"
            )));
        }
        let mut params: HashMap<String, String> = q
            .iter()
            .filter(|(k, _)| {
                matches!(
                    k.as_str(),
                    "q" | "df"
                        | "default_operator"
                        | "analyze_wildcard"
                        | "lenient"
                        | "analyzer"
                        | "ignore_unavailable"
                        | "allow_no_indices"
                        | "expand_wildcards"
                )
            })
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        params.insert("size".into(), "0".into());
        // The body as sent, so errors point at its positions.
        let (status, resp) = self.search_or_count("POST", "_search", target, &params, body);
        if status == 404 {
            return (status, resp);
        }
        if status >= 400 {
            return invalid(Some(error_text(&resp)));
        }
        let query = uri_query(q)
            .or_else(|| req.get("query").cloned())
            .unwrap_or_else(|| json!({"match_all": {}}));
        let indices = self.index_mappings(target);
        let total = indices.len().max(1);
        let mut out = json!({
            "_shards": {"total": total, "successful": total, "failed": 0},
            "valid": true,
        });
        if explain || q.get("rewrite").is_some_and(|v| v == "true") {
            let all_shards = q.get("all_shards").is_some_and(|v| v == "true");
            let explanations: Vec<Value> = indices
                .iter()
                .map(|(name, mappings)| {
                    let mut e = Map::new();
                    e.insert("index".into(), json!(name));
                    if all_shards {
                        e.insert("shard".into(), json!(0));
                    }
                    e.insert("valid".into(), json!(true));
                    e.insert("explanation".into(), json!(lucene_string(&query, mappings)));
                    Value::Object(e)
                })
                .collect();
            out["explanations"] = json!(explanations);
        }
        (200, out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lucene_strings_of_common_queries() {
        let m = json!({"properties": {"text": {"type": "text"}}});
        assert_eq!(lucene_string(&json!({"match_all": {}}), &m), "*:*");
        assert_eq!(
            lucene_string(&json!({"match": {"text": "Big wolf"}}), &m),
            "text:big text:wolf"
        );
        assert_eq!(lucene_string(&json!({"term": {"text": "x"}}), &m), "text:x");
        assert_eq!(lucene_string(&json!({"prefix": {"text": "ba"}}), &m), "text:ba*");
        assert_eq!(
            lucene_string(
                &json!({"bool": {"must": [{"term": {"text": "a"}}], "filter": {"term": {"text": "b"}}}}),
                &m
            ),
            "+text:a #text:b"
        );
    }
}
