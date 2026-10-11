//! `_search` request checks Elasticsearch makes before running a search:
//! URL parameter ranges, unknown body keys, and the per-index limits
//! (`index.max_*` settings) a request can exceed -- each with the error
//! Elasticsearch reports for it.

use serde_json::{Value, json};
use std::collections::HashMap;

use super::jsonpos;

/// Top-level keys of a search body (`SearchSourceBuilder`).
const BODY_KEYS: &[&str] = &[
    "from",
    "size",
    "timeout",
    "terminate_after",
    "query",
    "sub_searches",
    "post_filter",
    "knn",
    "rank",
    "retriever",
    "min_score",
    "version",
    "seq_no_primary_term",
    "explain",
    "_source",
    "stored_fields",
    "docvalue_fields",
    "fields",
    "script_fields",
    "sort",
    "track_scores",
    "track_total_hits",
    "indices_boost",
    "aggregations",
    "aggs",
    "highlight",
    "suggest",
    "rescore",
    "stats",
    "ext",
    "profile",
    "search_after",
    "collapse",
    "slice",
    "pit",
    "runtime_mappings",
];

fn bad(kind: &str, reason: &str) -> (u16, Value) {
    (
        400,
        json!({"error": {"root_cause": [{"type": kind, "reason": reason}], "type": kind, "reason": reason}, "status": 400}),
    )
}

fn param_i64(q: &HashMap<String, String>, key: &str) -> Option<i64> {
    q.get(key).and_then(|v| v.parse().ok())
}

/// URL parameters (and body options) checked before any index is
/// resolved; `scroll` is whether the search opens a scroll.
pub fn check_params(
    q: &HashMap<String, String>,
    req: &Value,
    scroll: bool,
) -> Result<(), (u16, Value)> {
    if req.get("terminate_after").and_then(Value::as_i64).is_some_and(|n| n < 0) {
        return Err(bad("illegal_argument_exception", "terminateAfter must be > 0"));
    }
    if scroll && req.get("collapse").is_some() {
        return Err(bad(
            "action_request_validation_exception",
            "Validation Failed: 1: cannot use `collapse` in a scroll context;",
        ));
    }
    if param_i64(q, "batched_reduce_size").is_some_and(|n| n < 2) {
        return Err(bad("illegal_argument_exception", "batchedReduceSize must be >= 2"));
    }
    if param_i64(q, "pre_filter_shard_size").is_some_and(|n| n < 1) {
        return Err(bad("illegal_argument_exception", "preFilterShardSize must be >= 1"));
    }
    if let Some(n) = req.get("track_total_hits").and_then(Value::as_i64)
        && n < -1
    {
        return Err(bad(
            "illegal_argument_exception",
            &format!("[track_total_hits] parameter must be positive or equals to -1, got {n}"),
        ));
    }
    if let Some(n) = req.get("from").and_then(Value::as_i64)
        && n < 0
    {
        return Err(bad(
            "illegal_argument_exception",
            &format!("[from] parameter cannot be negative but was [{n}]"),
        ));
    }
    Ok(())
}

/// Body keys a search doesn't take, reported (in document order) as the
/// parser does: `Unknown key for a START_OBJECT in [match].` with the
/// value's line and column. A top-level `query` holding more than one
/// query is malformed the same way.
pub fn check_body(raw: &[u8]) -> Result<(), (u16, Value)> {
    let text = String::from_utf8_lossy(raw);
    if text.trim().is_empty() {
        return Ok(());
    }
    let all = jsonpos::scan(&text);
    for p in &all {
        // `indices_boost` is a list of single-entry objects since 8.0.
        let old_boost = p.path == "indices_boost" && p.kind == jsonpos::Kind::Object && !p.in_array;
        if !old_boost
            && (p.path.is_empty() || p.path.contains('.') || BODY_KEYS.contains(&p.path.as_str()))
        {
            continue;
        }
        // Array elements share the path: only the first value counts.
        let token = p.token(jsonpos::first_char(&text, p));
        let reason = format!("Unknown key for a {token} in [{}].", p.path);
        return Err((
            400,
            json!({"error": {"root_cause": [{"type": "parsing_exception", "reason": reason, "line": p.start.0, "col": p.start.1}],
                "type": "parsing_exception", "reason": reason, "line": p.start.0, "col": p.start.1}, "status": 400}),
        ));
    }
    check_collapse(&text, &all)?;
    // `{"query": {"term": {...}, "preference": ...}}`: the second key
    // after a complete query.
    let mut in_query: Vec<&jsonpos::ValuePos> = vec![];
    for p in &all {
        if p.path.starts_with("query.")
            && p.path[6..].split('.').count() == 1
            && in_query.iter().all(|q| q.path != p.path)
        {
            in_query.push(p);
        }
    }
    if in_query.len() > 1 {
        let first = in_query[0].path[6..].to_string();
        let second = in_query[1];
        let key_col = key_col(&text, second, &second.path[6..]);
        let reason =
            format!("[{first}] malformed query, expected [END_OBJECT] but found [FIELD_NAME]");
        return Err((
            400,
            json!({"error": {"root_cause": [{"type": "parsing_exception", "reason": reason, "line": second.start.0, "col": key_col}],
                "type": "parsing_exception", "reason": reason, "line": second.start.0, "col": key_col}, "status": 400}),
        ));
    }
    Ok(())
}

/// The column of the `"key"` whose value starts at `p` (the parser
/// stands on a field name when it rejects it).
pub(super) fn key_col(text: &str, p: &jsonpos::ValuePos, key: &str) -> usize {
    let line = text.lines().nth(p.start.0 - 1).unwrap_or("");
    let before: String = line.chars().take(p.start.1 - 1).collect();
    before.rfind(&format!("\"{key}\"")).map_or(p.start.1, |b| before[..b].chars().count() + 1)
}

/// `collapse.inner_hits` checks made while parsing: every inner hit
/// needs a `name`, and a second-level collapse can't nest further.
fn check_collapse(text: &str, all: &[jsonpos::ValuePos]) -> Result<(), (u16, Value)> {
    let Some(ih) = all.iter().find(|p| p.path == "collapse.inner_hits") else { return Ok(()) };
    let at = |p: &jsonpos::ValuePos| format!("[{}:{}]", p.end.0, p.end.1);
    // The definitions: the value itself, or each object of an array.
    let defs: Vec<&jsonpos::ValuePos> = if ih.kind == jsonpos::Kind::Array {
        all.iter()
            .filter(|p| p.path == "collapse.inner_hits" && p.kind == jsonpos::Kind::Object)
            .collect()
    } else {
        vec![ih]
    };
    let within = |p: &jsonpos::ValuePos, d: &jsonpos::ValuePos| p.start > d.start && p.end <= d.end;
    for d in &defs {
        for inner in all.iter().filter(|p| {
            p.path.starts_with("collapse.inner_hits.collapse.")
                && p.path.matches('.').count() == 3
                && within(p, d)
        }) {
            let key = inner.path.rsplit('.').next().unwrap_or("");
            if matches!(key, "inner_hits" | "collapse") {
                let col = key_col(text, inner, key);
                let pos = format!("[{}:{col}]", inner.start.0);
                let root = json!({"type": "parsing_exception", "reason": "Invalid token in the inner collapse", "line": inner.start.0, "col": col});
                return Err((
                    400,
                    json!({"error": {"root_cause": [root.clone()], "type": "x_content_parse_exception",
                        "reason": format!("{pos} [collapse] failed to parse field [inner_hits]"),
                        "caused_by": {"type": "x_content_parse_exception",
                            "reason": format!("{pos} [inner_hits] failed to parse field [collapse]"),
                            "caused_by": root}}, "status": 400}),
                ));
            }
        }
        let named = all.iter().any(|p| p.path == "collapse.inner_hits.name" && within(p, d));
        if !named {
            let reason = format!("{} [collapse] failed to parse field [inner_hits]", at(ih));
            return Err((
                400,
                json!({"error": {"root_cause": [{"type": "x_content_parse_exception", "reason": reason}],
                    "type": "x_content_parse_exception", "reason": reason,
                    "caused_by": {"type": "illegal_argument_exception",
                        "reason": "inner_hits must have a [name]; set the [name] field in the inner_hits definition"}},
                    "status": 400}),
            ));
        }
    }
    Ok(())
}

/// An `index.*` setting as a number (settings are stored as strings).
pub fn setting(settings: &Value, key: &str) -> Option<i64> {
    let v = settings.get("index").and_then(|i| i.get(key))?;
    v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse().ok()))
}

/// A failure while building the query on a shard (`query_shard_exception`
/// wrapping the real cause).
pub(super) fn query_shard_failure(index: &str, settings: &Value, reason: &str) -> (u16, Value) {
    let uuid = settings["index"]["uuid"].as_str().unwrap_or("_na_");
    let full = format!("failed to create query: {reason}");
    let root = json!({"type": "query_shard_exception", "reason": full, "index_uuid": uuid, "index": index});
    let mut cause = root.clone();
    cause["caused_by"] = json!({"type": "illegal_argument_exception", "reason": reason});
    (
        400,
        json!({"error": {"root_cause": [root], "type": "search_phase_execution_exception",
            "reason": "all shards failed", "phase": "query", "grouped": true,
            "failed_shards": [{"shard": 0, "index": index, "node": "noida", "reason": cause}]},
            "status": 400}),
    )
}

/// An `illegal_argument_exception` failing every shard of `index`.
pub(super) fn shard_iae(index: &str, reason: &str) -> (u16, Value) {
    let iae = json!({"type": "illegal_argument_exception", "reason": reason});
    let mut caused = iae.clone();
    caused["caused_by"] = iae.clone();
    (
        400,
        json!({"error": {"root_cause": [iae.clone()], "type": "search_phase_execution_exception",
            "reason": "all shards failed", "phase": "query", "grouped": true,
            "failed_shards": [{"shard": 0, "index": index, "node": "noida", "reason": iae}],
            "caused_by": caused}, "status": 400}),
    )
}

/// The value of a single-field query (`{"f": "v"}` or `{"f": {"value": "v"}}`).
fn field_value(spec: &Value) -> Option<&str> {
    let (_, v) =
        spec.as_object()?.iter().find(|(k, _)| !matches!(k.as_str(), "boost" | "_name"))?;
    v.as_str().or_else(|| v.get("value").and_then(Value::as_str))
}

/// Regex, prefix and terms-count limits of every query in `q`.
fn query_limits(q: &Value, index: &str, settings: &Value) -> Result<(), (u16, Value)> {
    let max_regex = setting(settings, "max_regex_length").unwrap_or(1000);
    let max_terms = setting(settings, "max_terms_count").unwrap_or(65536);
    match q {
        Value::Object(m) => {
            for (k, v) in m {
                match k.as_str() {
                    "regexp" if v.is_object() => {
                        if let Some(r) = field_value(v)
                            && r.chars().count() as i64 > max_regex
                        {
                            return Err(query_shard_failure(
                                index,
                                settings,
                                &format!(
                                    "The length of regex [{}] used in the Regexp Query request has exceeded the allowed maximum of [{max_regex}]. This maximum can be set by changing the [index.max_regex_length] index level setting.",
                                    r.chars().count()
                                ),
                            ));
                        }
                    }
                    "prefix" if v.is_object() => {
                        if let Some(p) = field_value(v)
                            && p.chars().count() as i64 > max_regex
                        {
                            return Err(query_shard_failure(
                                index,
                                settings,
                                &format!(
                                    "The length of prefix [{}] used in the Prefix Query request has exceeded the allowed maximum of [{max_regex}]. This maximum can be set by changing the [index.max_regex_length] index level setting.",
                                    p.chars().count()
                                ),
                            ));
                        }
                    }
                    "terms" if v.is_object() => {
                        let n = v
                            .as_object()
                            .into_iter()
                            .flatten()
                            .filter_map(|(_, x)| x.as_array())
                            .map(Vec::len)
                            .max()
                            .unwrap_or(0) as i64;
                        if n > max_terms {
                            return Err(query_shard_failure(
                                index,
                                settings,
                                &format!(
                                    "The number of terms [{n}] used in the Terms Query request has exceeded the allowed maximum of [{max_terms}]. This maximum can be set by changing the [index.max_terms_count] index level setting."
                                ),
                            ));
                        }
                    }
                    "query_string" if v.is_object() => {
                        let text = v.get("query").and_then(Value::as_str).unwrap_or("");
                        for r in slash_regexes(text) {
                            if r.chars().count() as i64 > max_regex {
                                return Err(query_shard_failure(
                                    index,
                                    settings,
                                    &format!(
                                        "The length of regex [{}] used in the [query_string] has exceeded the allowed maximum of [{max_regex}]. This maximum can be set by changing the [index.max_regex_length] index level setting.",
                                        r.chars().count()
                                    ),
                                ));
                            }
                        }
                    }
                    _ => {}
                }
                query_limits(v, index, settings)?;
            }
        }
        Value::Array(a) => {
            for v in a {
                query_limits(v, index, settings)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// The `/regex/` parts of a query_string query.
fn slash_regexes(text: &str) -> Vec<String> {
    let mut out = vec![];
    let mut cur: Option<String> = None;
    let mut esc = false;
    for c in text.chars() {
        if esc {
            if let Some(r) = cur.as_mut() {
                r.push('\\');
                r.push(c);
            }
            esc = false;
            continue;
        }
        match (c, cur.as_mut()) {
            ('\\', _) => esc = true,
            ('/', None) => cur = Some(String::new()),
            ('/', Some(_)) => out.push(cur.take().unwrap_or_default()),
            (c, Some(r)) => r.push(c),
            _ => {}
        }
    }
    // An unterminated regex still counts (the parser reads to the end).
    if let Some(r) = cur {
        out.push(r);
    }
    out
}

/// The limits of index `name` (with `settings`) a search `req` can
/// exceed; `scroll` searches are held to the result window per batch.
pub fn check_index(
    name: &str,
    settings: &Value,
    req: &Value,
    scroll: bool,
) -> Result<(), (u16, Value)> {
    for key in ["query", "post_filter"] {
        if let Some(q) = req.get(key) {
            query_limits(q, name, settings)?;
        }
    }
    let window = setting(settings, "max_result_window").unwrap_or(10_000);
    if scroll {
        let size = req.get("size").and_then(Value::as_i64).unwrap_or(10);
        if size > window {
            return Err(shard_iae(
                name,
                &format!(
                    "Batch size is too large, size must be less than or equal to: [{window}] but was [{size}]. Scroll batch sizes cost as much memory as result windows so they are controlled by the [index.max_result_window] index level setting."
                ),
            ));
        }
    }
    let max_rescore = setting(settings, "max_rescore_window").unwrap_or(window);
    let rescores = match req.get("rescore") {
        Some(Value::Array(a)) => a.clone(),
        Some(o @ Value::Object(_)) => vec![o.clone()],
        _ => vec![],
    };
    for r in &rescores {
        let w = r.get("window_size").and_then(Value::as_i64).unwrap_or(10);
        if w > max_rescore {
            return Err(shard_iae(
                name,
                &format!(
                    "Rescore window [{w}] is too large. It must be less than [{max_rescore}]. This prevents allocating massive heaps for storing the results to be rescored. This limit can be set by changing the [index.max_rescore_window] index level setting."
                ),
            ));
        }
        if let Some(q) = r.get("query").and_then(|q| q.get("rescore_query")) {
            query_limits(q, name, settings)?;
        }
    }
    let max_dv = setting(settings, "max_docvalue_fields_search").unwrap_or(100);
    let dv = req.get("docvalue_fields").and_then(Value::as_array).map_or(0, Vec::len) as i64;
    if dv > max_dv {
        return Err(shard_iae(
            name,
            &format!(
                "Trying to retrieve too many docvalue_fields. Must be less than or equal to: [{max_dv}] but was [{dv}]. This limit can be set by changing the [index.max_docvalue_fields_search] index level setting."
            ),
        ));
    }
    let max_sf = setting(settings, "max_script_fields").unwrap_or(32);
    let sf = req.get("script_fields").and_then(Value::as_object).map_or(0, |m| m.len()) as i64;
    if sf > max_sf {
        return Err(shard_iae(
            name,
            &format!(
                "Trying to retrieve too many script_fields. Must be less than or equal to: [{max_sf}] but was [{sf}]. This limit can be set by changing the [index.max_script_fields] index level setting."
            ),
        ));
    }
    Ok(())
}

/// `num_reduce_phases` of a search over `shards` shards whose results
/// are merged `batched_reduce_size` at a time: each full batch is reduced
/// into one partial result that joins the next batch, and the last
/// result arriving triggers the final reduce.
pub fn reduce_phases(shards: u64, batched_reduce_size: u64) -> u64 {
    let (mut buffered, mut partial) = (0, 0);
    for i in 1..=shards {
        buffered += 1;
        if i < shards && buffered >= batched_reduce_size {
            partial += 1;
            buffered = 1;
        }
    }
    partial + 1
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reduce_phases_by_batch_size() {
        assert_eq!(reduce_phases(5, 2), 4);
        assert_eq!(reduce_phases(5, 3), 2);
        assert_eq!(reduce_phases(5, 4), 2);
        assert_eq!(reduce_phases(5, 5), 1);
        assert_eq!(reduce_phases(1, 512), 1);
    }

    #[test]
    fn unknown_body_keys_cite_their_position() {
        let (status, e) = check_body(br#"{"match":{"foo":"bar"}}"#).unwrap_err();
        assert_eq!(status, 400);
        assert_eq!(e["error"]["reason"], "Unknown key for a START_OBJECT in [match].");
        assert_eq!(e["error"]["col"], 10);
        assert!(check_body(br#"{"query":{"match_all":{}},"size":1}"#).is_ok());
        let (_, e) =
            check_body(br#"{"query":{"term":{"data":"some"},"preference":"_local"}}"#).unwrap_err();
        assert_eq!(
            e["error"]["reason"],
            "[term] malformed query, expected [END_OBJECT] but found [FIELD_NAME]"
        );
        assert_eq!(e["error"]["col"], 34);
    }

    #[test]
    fn regex_and_terms_limits() {
        let settings = json!({"index": {"max_terms_count": "2"}});
        let req = json!({"query": {"terms": {"user": ["a", "b", "c"]}}});
        assert!(check_index("i", &settings, &req, false).is_err());
        let req = json!({"query": {"regexp": {"f": "a".repeat(1001)}}});
        assert!(check_index("i", &json!({}), &req, false).is_err());
        assert_eq!(slash_regexes("a /b.c/ d"), vec!["b.c"]);
    }
}
