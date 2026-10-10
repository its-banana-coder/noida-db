//! `POST _reindex`: copies the documents a query matches in one or more
//! source indices into a destination index (through its ingest pipelines),
//! optionally transforming each with a Painless script.

use serde_json::{Map, Value, json};
use std::collections::HashMap;

use super::lifecycle::validation_failed;
use super::{Engine, error, painless, parse_json, search};

static TASKS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

/// A document read from a source index.
struct SourceDoc {
    index: String,
    id: String,
    source: Value,
    version: i64,
    routing: Option<String>,
}

/// `_source` filtering of the copied documents (`"_source": ["a", "b.*"]`).
fn filter_source(src: &Value, spec: &Value) -> Value {
    let includes: Vec<String> = match spec {
        Value::Bool(false) => return json!({}),
        Value::String(s) => vec![s.clone()],
        Value::Array(a) => a.iter().filter_map(Value::as_str).map(String::from).collect(),
        Value::Object(o) => o
            .get("includes")
            .and_then(Value::as_array)
            .map(|a| a.iter().filter_map(Value::as_str).map(String::from).collect())
            .unwrap_or_default(),
        _ => return src.clone(),
    };
    if includes.is_empty() {
        return src.clone();
    }
    fn walk(v: &Value, prefix: &str, includes: &[String]) -> Option<Value> {
        let Value::Object(m) = v else { return None };
        let mut out = Map::new();
        for (k, x) in m {
            let path = if prefix.is_empty() { k.clone() } else { format!("{prefix}.{k}") };
            if includes.iter().any(|p| super::glob_match(p, &path)) {
                out.insert(k.clone(), x.clone());
            } else if x.is_object()
                && includes.iter().any(|p| p.starts_with(&format!("{path}.")))
                && let Some(inner) = walk(x, &path, includes)
            {
                out.insert(k.clone(), inner);
            }
        }
        Some(Value::Object(out))
    }
    walk(src, "", &includes).unwrap_or_else(|| json!({}))
}

impl Engine {
    /// `POST /_reindex`.
    pub(super) fn reindex(
        &self,
        method: &str,
        q: &HashMap<String, String>,
        body: &[u8],
    ) -> (u16, Value) {
        if method != "POST" {
            return (405, error("method_not_allowed_exception", "Incorrect HTTP method", 405));
        }
        let started = std::time::Instant::now();
        let Some(req) = parse_json(body).filter(Value::is_object) else {
            return (400, super::malformed_body());
        };
        const KNOWN: &[&str] = &["conflicts", "max_docs", "source", "dest", "script", "size"];
        if let Some(k) =
            req.as_object().and_then(|m| m.keys().find(|k| !KNOWN.contains(&k.as_str())))
        {
            return (
                400,
                error(
                    "x_content_parse_exception",
                    &format!("[1:2] [reindex] unknown field [{k}]"),
                    400,
                ),
            );
        }
        let source = req.get("source").cloned().unwrap_or_else(|| json!({}));
        let dest = req.get("dest").cloned().unwrap_or_else(|| json!({}));
        if source.get("remote").is_some() {
            return (
                400,
                error(
                    "illegal_argument_exception",
                    "reindexing from a remote cluster is not supported by noida",
                    400,
                ),
            );
        }
        let Some(dest_index) = dest.get("index").and_then(Value::as_str).map(String::from) else {
            return validation_failed("index must be specified");
        };
        let src_expr = match source.get("index") {
            Some(Value::String(s)) => s.clone(),
            Some(Value::Array(a)) => {
                a.iter().filter_map(Value::as_str).collect::<Vec<_>>().join(",")
            }
            _ => {
                return validation_failed(
                    "use _all if you really want to copy from all existing indexes",
                );
            }
        };
        let proceed = req.get("conflicts").and_then(Value::as_str) == Some("proceed")
            || q.get("conflicts").is_some_and(|c| c == "proceed");
        let max_docs = req
            .get("max_docs")
            .or_else(|| req.get("size"))
            .and_then(Value::as_u64)
            .or_else(|| q.get("max_docs").and_then(|m| m.parse().ok()))
            .unwrap_or(u64::MAX) as usize;
        let batch = source.get("size").and_then(Value::as_u64).unwrap_or(1000).max(1) as usize;
        let query = source.get("query").cloned().unwrap_or_else(|| json!({"match_all": {}}));
        let script = match req.get("script") {
            Some(sc) => match painless::script_parts(sc)
                .and_then(|(src, params)| painless::compile(&src).map(|c| (c, src, params)))
            {
                Ok(x) => Some(x),
                Err(e) => return (400, painless::script_error(&sc.to_string(), &e, true)),
            },
            None => None,
        };
        // Read the matching documents from the refreshed view.
        let docs = {
            let mut s = self.0.lock().unwrap();
            let names = match Self::resolve_targets(&s, &src_expr, &HashMap::new()) {
                Ok(n) => n,
                Err(e) => return e,
            };
            if let Some(n) = names.iter().find(|n| **n == dest_index) {
                return validation_failed(&format!(
                    "reindex cannot write into an index its reading from [{n}]"
                ));
            }
            let mut docs = Vec::new();
            for n in &names {
                let Some(i) = s.indices.get_mut(n) else { continue };
                i.auto_refresh(n);
                let matched = match search::eval_root(&query, &i.mappings, &i.committed) {
                    Ok(m) => m,
                    Err(e) => return (e.status, e.to_json()),
                };
                let mut hits: Vec<usize> = matched.keys().copied().collect();
                hits.sort_unstable();
                for k in hits {
                    let c = &i.committed[k];
                    let src = match source.get("_source") {
                        Some(spec) => filter_source(&c.source, spec),
                        None => c.source.clone(),
                    };
                    docs.push(SourceDoc {
                        index: n.clone(),
                        id: c.id.clone(),
                        source: src,
                        version: c.version,
                        routing: i.docs.get(&c.id).and_then(|d| d.routing.clone()),
                    });
                }
            }
            docs.truncate(max_docs);
            docs
        };
        let op_create = dest.get("op_type").and_then(Value::as_str) == Some("create");
        let version_type = dest.get("version_type").and_then(Value::as_str).unwrap_or("internal");
        let routing_mode = dest.get("routing").and_then(Value::as_str).unwrap_or("keep");
        let pipeline = dest.get("pipeline").and_then(Value::as_str);
        let (mut created, mut updated, mut deleted, mut noops, mut conflicts) = (0, 0, 0, 0, 0);
        let mut failures: Vec<Value> = Vec::new();
        let mut touched: Vec<String> = Vec::new();
        let total = docs.len();
        let mut batches = 0;
        for chunk in docs.chunks(batch) {
            batches += 1;
            for d in chunk {
                let (mut index, mut id, mut routing) = (
                    dest_index.clone(),
                    d.id.clone(),
                    match routing_mode {
                        "keep" => d.routing.clone(),
                        "discard" => None,
                        r => r.strip_prefix('=').map(String::from),
                    },
                );
                let mut src = d.source.clone();
                if let Some((sc, text, params)) = &script {
                    let ctx = json!({"_index": d.index, "_id": d.id, "_version": d.version,
                                     "_routing": routing, "_source": src, "op": "index",
                                     "_now": super::super::dates::now_ms()});
                    let ctx = match sc.run(ctx, params.clone()) {
                        Ok(c) => c,
                        Err(e) => return (400, painless::script_error(text, &e, false)),
                    };
                    match ctx["op"].as_str().unwrap_or("index") {
                        "noop" => {
                            noops += 1;
                            continue;
                        }
                        "delete" => {
                            let (st, _) = self.document_api(
                                "DELETE",
                                &index,
                                &id,
                                "_doc",
                                &HashMap::new(),
                                b"",
                            );
                            if st < 300 {
                                deleted += 1;
                            }
                            continue;
                        }
                        _ => {}
                    }
                    if ctx["_index"].as_str().is_some_and(|x| x != d.index) {
                        index = ctx["_index"].as_str().unwrap_or(&index).to_string();
                    }
                    if let Some(x) = ctx["_id"].as_str() {
                        id = x.to_string();
                    }
                    routing = ctx["_routing"].as_str().map(String::from);
                    src = ctx["_source"].clone();
                }
                let mut wq: HashMap<String, String> = HashMap::new();
                if let Some(r) = routing {
                    wq.insert("routing".into(), r);
                }
                if let Some(p) = pipeline {
                    wq.insert("pipeline".into(), p.to_string());
                }
                if matches!(version_type, "external" | "external_gte") {
                    wq.insert("version".into(), d.version.to_string());
                    wq.insert("version_type".into(), version_type.to_string());
                }
                let kind = if op_create { "_create" } else { "_doc" };
                let target = match self.write_target(&index) {
                    Ok(t) => t,
                    Err((st, e)) => {
                        failures.push(failure(&index, &id, st, &e));
                        continue;
                    }
                };
                let (st, resp) = self.index_with_pipelines(
                    "PUT",
                    &target,
                    &id,
                    kind,
                    &wq,
                    src.to_string().as_bytes(),
                );
                touched.push(target.clone());
                match st {
                    201 => created += 1,
                    200 if resp["result"] == json!("noop") => noops += 1,
                    200 => updated += 1,
                    409 => {
                        conflicts += 1;
                        if !proceed {
                            failures.push(failure(&target, &id, st, &resp));
                        }
                    }
                    _ => failures.push(failure(&target, &id, st, &resp)),
                }
            }
            if !failures.is_empty() {
                break;
            }
        }
        if q.get("refresh").is_some_and(|v| v.is_empty() || v == "true") {
            let mut s = self.0.lock().unwrap();
            touched.sort();
            touched.dedup();
            for n in touched {
                if let Some(i) = s.indices.get_mut(&n) {
                    i.refresh(&n);
                }
            }
        }
        if q.get("wait_for_completion").is_some_and(|v| v == "false") {
            let n = TASKS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return (200, json!({"task": format!("noida:{n}")}));
        }
        let status =
            failures.iter().filter_map(|f| f["status"].as_u64()).max().unwrap_or(200) as u16;
        (
            status,
            json!({"took": started.elapsed().as_millis() as u64, "timed_out": false, "total": total,
                   "updated": updated, "created": created, "deleted": deleted, "batches": batches,
                   "version_conflicts": conflicts, "noops": noops,
                   "retries": {"bulk": 0, "search": 0}, "throttled_millis": 0,
                   "requests_per_second": -1.0, "throttled_until_millis": 0, "failures": failures}),
        )
    }
}

/// One failed write, as `_reindex` lists it.
fn failure(index: &str, id: &str, status: u16, resp: &Value) -> Value {
    let mut cause = resp.get("error").cloned().unwrap_or_else(|| json!({}));
    if let Some(m) = cause.as_object_mut() {
        m.remove("root_cause");
    }
    json!({"index": index, "id": id, "cause": cause, "status": status})
}

#[cfg(test)]
mod tests {
    use super::super::Engine;
    use serde_json::{Value, json};

    fn call(e: &Engine, method: &str, path: &str, body: Value) -> (u16, Value) {
        let (p, q) = path.split_once('?').unwrap_or((path, ""));
        let b = if body.is_null() { vec![] } else { body.to_string().into_bytes() };
        e.dispatch(method, p, q, &b)
    }

    #[test]
    fn reindex_copies_and_transforms() {
        let e = Engine::default();
        call(&e, "PUT", "/src/_doc/1", json!({"a": 1}));
        call(&e, "PUT", "/src/_doc/2?refresh=true", json!({"a": 2}));
        let (st, r) = call(
            &e,
            "POST",
            "/_reindex?refresh=true",
            json!({"source": {"index": "src"}, "dest": {"index": "dst"}}),
        );
        assert_eq!(st, 200);
        assert_eq!((r["total"].clone(), r["created"].clone()), (json!(2), json!(2)));
        let (_, c) = call(&e, "GET", "/dst/_count", Value::Null);
        assert_eq!(c["count"], json!(2));
        let (st, r) = call(
            &e,
            "POST",
            "/_reindex",
            json!({"source": {"index": "src"}, "dest": {"index": "dst", "op_type": "create"}}),
        );
        assert_eq!((st, r["version_conflicts"].clone()), (409, json!(2)));
        let (_, r) = call(
            &e,
            "POST",
            "/_reindex?refresh=true",
            json!({"source": {"index": "src", "query": {"term": {"a": 1}}}, "dest": {"index": "d2"},
                   "script": {"source": "ctx._source.b = ctx._source.a + 10"}}),
        );
        assert_eq!(r["created"], json!(1));
        assert_eq!(
            call(&e, "GET", "/d2/_doc/1", Value::Null).1["_source"],
            json!({"a": 1, "b": 11})
        );
        let (st, _) = call(
            &e,
            "POST",
            "/_reindex",
            json!({"source": {"index": "src"}, "dest": {"index": "src"}}),
        );
        assert_eq!(st, 400);
    }
}
