use serde_json::{Map, Value, json};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use super::analysis;
use super::search::{self, CommittedDoc};

#[derive(Clone)]
pub struct Engine(Arc<Mutex<State>>);

use serde::{Deserialize, Serialize};

#[derive(Default, Serialize, Deserialize)]
struct State {
    indices: HashMap<String, Index>,
    templates: HashMap<String, Value>,
    /// Open scrolls and points in time (in memory only, like a node's).
    #[serde(skip)]
    contexts: HashMap<String, SearchContext>,
}

/// A frozen view of the searched indices: a scroll pages through hits
/// computed when it opened; a point in time is searched again and again.
struct SearchContext {
    expires: std::time::Instant,
    kind: ContextKind,
}

enum ContextKind {
    Scroll { hits: Vec<Value>, total: Value, max_score: Value, pos: usize, size: usize },
    Pit { mappings: Value, docs: Vec<CommittedDoc> },
}

/// A `keep_alive`/`scroll` duration (`1m`, `30s`, `500ms`, `2h`, `1d`).
fn parse_keep_alive(s: &str) -> Option<std::time::Duration> {
    let split = s.find(|c: char| !c.is_ascii_digit())?;
    let n: u64 = s[..split].parse().ok()?;
    let ms = match &s[split..] {
        "ms" => n,
        "s" => n * 1000,
        "m" => n * 60_000,
        "h" => n * 3_600_000,
        "d" => n * 86_400_000,
        _ => return None,
    };
    Some(std::time::Duration::from_millis(ms))
}

fn context_missing(id: &str) -> (u16, Value) {
    let mut e = search::EsError::shard_failure(
        "search_context_missing_exception",
        &format!("No search context found for id [{id}]"),
    );
    e.status = 404;
    (404, e.to_json())
}

#[derive(Default, Serialize, Deserialize)]
struct Index {
    settings: Value,
    mappings: Value,
    aliases: HashMap<String, Value>,
    docs: HashMap<String, Document>,
    order: Vec<String>,
    seq: i64,
    opened: bool,
    /// The last-refreshed, searchable snapshot (near-real-time semantics:
    /// `_search` sees this, real-time GET reads `docs` directly).
    #[serde(skip)]
    committed: Vec<CommittedDoc>,
}

impl Index {
    fn refresh(&mut self, name: &str) {
        self.committed = self
            .order
            .iter()
            .filter_map(|id| {
                self.docs.get(id).map(|d| CommittedDoc {
                    index: name.to_string(),
                    id: id.clone(),
                    source: d.source.clone(),
                    version: d.version,
                    full_source: None,
                })
            })
            .collect();
    }
}

#[derive(Serialize, Deserialize)]
struct Document {
    source: Value,
    version: i64,
    seq: i64,
}

static IDS: AtomicU64 = AtomicU64::new(1);

impl Default for Engine {
    fn default() -> Self {
        Self(Arc::new(Mutex::new(State::default())))
    }
}

impl Engine {
    pub(crate) fn load(bytes: &[u8]) -> Self {
        let mut state: State = serde_json::from_slice(bytes).unwrap_or_default();
        let names: Vec<String> = state.indices.keys().cloned().collect();
        for name in names {
            if let Some(index) = state.indices.get_mut(&name) {
                index.refresh(&name);
            }
        }
        Self(Arc::new(Mutex::new(state)))
    }

    pub(crate) fn snapshot(&self) -> Vec<u8> {
        let state = self.0.lock().unwrap();
        serde_json::to_vec(&*state).unwrap()
    }
}

impl Engine {
    pub fn dispatch(&self, method: &str, path: &str, query: &str, body: &[u8]) -> (u16, Value) {
        let segments: Vec<&str> =
            path.trim_matches('/').split('/').filter(|s| !s.is_empty()).collect();
        let q = query_params(query);
        if path == "/" || path.is_empty() {
            return (
                200,
                json!({"name":"noida-db","cluster_name":"docker-cluster","cluster_uuid":"noida-local","version":{"number":"8.15.3","build_flavor":"default","build_type":"docker","build_hash":"noida","build_date":"2024-09-05T00:00:00.000Z","build_snapshot":false,"lucene_version":"9.11.1","minimum_wire_compatibility_version":"7.17.0","minimum_index_compatibility_version":"7.0.0"},"tagline":"You Know, for Search"}),
            );
        }
        if segments.first() == Some(&"_index_template") {
            return self.template(method, &segments, body);
        }
        if segments.first() == Some(&"_component_template") {
            return self.template(method, &segments, body);
        }
        if segments.first() == Some(&"_aliases") {
            return self.aliases(method, body);
        }
        if segments.first() == Some(&"_alias") {
            return self.get_alias(&segments);
        }
        if segments.first() == Some(&"_search") && segments.get(1) == Some(&"scroll") {
            return self.scroll_api(method, segments.get(2).copied(), &q, body);
        }
        if segments.first() == Some(&"_pit") {
            return self.close_pit(method, body);
        }
        if segments.first() == Some(&"_search") || segments.first() == Some(&"_count") {
            return self.search_or_count(method, segments[0], "*", &q, body);
        }
        if segments.first() == Some(&"_analyze") {
            return self.analyze(body);
        }
        if segments.first() == Some(&"_bulk") {
            // The global bulk endpoint -- no index in the URL, each
            // action line names its own `_index` instead. Found missing
            // via testing before a public release: only the per-index
            // `/<index>/_bulk` form (below) was routed at all, so this
            // -- the form most real bulk-ingestion tooling actually
            // uses -- 404'd outright rather than running the bulk
            // request. `bulk()`'s own `index` fallback parameter is
            // irrelevant here since every real caller of this form sets
            // `_index` on every line; pass an empty string rather than a
            // real index name.
            return self.bulk(method, "", &q, body);
        }
        if segments.len() == 1 && segments[0].starts_with('_') {
            return (404, error("not_found", "no handler found for uri", 404));
        }
        if segments.is_empty() {
            return (200, json!({}));
        }
        // Resolves an alias to its real backing index for every
        // document-level and admin operation below (GET/PUT a document,
        // _bulk, _update, _mget, _mapping, _refresh, ...), the same way
        // `resolve_indices` already does for `_search`/`_count` -- an
        // alias pointing at more than one index picks the first
        // (arbitrary but deterministic via BTree-like iteration order
        // isn't guaranteed here; real Elasticsearch itself requires a
        // single-document op's alias to resolve to exactly one index and
        // errors otherwise, which this simplified version doesn't
        // enforce, but every real caller of a write-alias only ever
        // points it at one index at a time anyway).
        let index_name = {
            let s = self.0.lock().unwrap();
            if s.indices.contains_key(segments[0]) {
                segments[0].to_string()
            } else {
                s.indices
                    .iter()
                    .find(|(_, i)| i.aliases.contains_key(segments[0]))
                    .map(|(n, _)| n.clone())
                    .unwrap_or_else(|| segments[0].to_string())
            }
        };
        let index_name = index_name.as_str();
        if segments.len() == 1 {
            return self.index_api(method, index_name, body);
        }
        match segments[1] {
            "_mapping" => self.mapping_api(method, index_name, body),
            "_settings" => self.settings_api(method, index_name, body),
            "_refresh" | "_flush" | "_open" | "_close" => {
                self.index_action(method, index_name, segments[1])
            }
            "_search" | "_count" => self.search_or_count(method, segments[1], index_name, &q, body),
            "_pit" if method == "POST" => self.open_pit(index_name, &q),
            "_delete_by_query" | "_update_by_query" => {
                self.by_query(method, segments[1], index_name, &q, body)
            }
            "_alias" | "_aliases" => self.index_alias(method, index_name, segments.get(2).copied()),
            "_analyze" => self.analyze(body),
            "_doc" | "_create" | "_source" if segments.len() == 3 => {
                self.document_api(method, index_name, segments[2], segments[1], &q, body)
            }
            "_doc" if method == "POST" && segments.len() == 2 => {
                self.document_api(method, index_name, "", "_doc", &q, body)
            }
            "_bulk" => self.bulk(method, index_name, &q, body),
            "_update" if segments.len() == 3 => self.update(method, index_name, segments[2], body),
            "_mget" => self.mget(method, index_name, &q, body),
            _ => no_handler(method, path),
        }
    }

    /// Index names/patterns matching Elasticsearch's rules for `_search`
    /// targets: an exact name, a comma-separated list, a `name*` prefix
    /// wildcard, or `_all`/`*` for every index.
    /// Resolves an index name, wildcard pattern, OR alias to the real
    /// index name(s) backing it. An alias pointing at more than one index
    /// (every index it was `add`ed to) resolves to all of them, matching
    /// real Elasticsearch's own "search across every index behind this
    /// alias" behavior -- this is what makes a zero-downtime alias
    /// switch (point `products-current` at `products-v2` instead of
    /// `products-v1`, keep searching `products-current`) actually work,
    /// rather than only letting `_aliases`/`_alias` manage the alias
    /// metadata without `_search` ever being able to use it.
    fn resolve_indices(s: &State, pattern: &str) -> Vec<String> {
        if pattern == "_all" || pattern == "*" {
            let mut names: Vec<String> = s.indices.keys().cloned().collect();
            names.sort();
            return names;
        }
        let mut names = Vec::new();
        for part in pattern.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            if let Some(prefix) = part.strip_suffix('*') {
                names.extend(s.indices.keys().filter(|k| k.starts_with(prefix)).cloned());
            } else if s.indices.contains_key(part) {
                names.push(part.to_string());
            } else {
                names.extend(
                    s.indices
                        .iter()
                        .filter(|(_, i)| i.aliases.contains_key(part))
                        .map(|(name, _)| name.clone()),
                );
            }
        }
        names.sort();
        names.dedup();
        names
    }

    fn search_or_count(
        &self,
        method: &str,
        action: &str,
        index_pattern: &str,
        q: &HashMap<String, String>,
        body: &[u8],
    ) -> (u16, Value) {
        if method != "GET" && method != "POST" {
            return (405, error("method_not_allowed_exception", "Incorrect HTTP method", 405));
        }
        let Some(mut req) = parse_json(body) else {
            return (400, malformed_body());
        };
        // URL parameters the clients send instead of body fields. Found via
        // testing before a public release: `_source_includes` etc. were
        // ignored, returning whole documents.
        if let Some(f) = source_filter_from_params(q) {
            req["_source"] = f;
        }
        for (param, key) in [("size", "size"), ("from", "from")] {
            if let Some(n) = q.get(param).and_then(|v| v.parse::<i64>().ok()) {
                req[key] = json!(n);
            }
        }
        if let Some(sort) = q.get("sort") {
            // `?sort=price:desc,name`
            req["sort"] = Value::Array(
                sort.split(',')
                    .map(|s| match s.split_once(':') {
                        Some((f, o)) => json!({f: o}),
                        None => json!(s),
                    })
                    .collect(),
            );
        }
        let mut s = self.0.lock().unwrap();
        let now = std::time::Instant::now();
        s.contexts.retain(|_, c| c.expires > now);
        if let Some(pit) = req.get("pit") {
            if index_pattern != "*" {
                return (
                    400,
                    error(
                        "action_request_validation_exception",
                        "Validation Failed: 1: [indices] cannot be used with point in time. Do \
                         not specify any index with point in time.;",
                        400,
                    ),
                );
            }
            let id = pit.get("id").and_then(Value::as_str).unwrap_or("").to_string();
            let keep = pit.get("keep_alive").and_then(Value::as_str).and_then(parse_keep_alive);
            let Some(ctx) = s.contexts.get_mut(&id) else { return context_missing(&id) };
            if let Some(k) = keep {
                ctx.expires = now + k;
            }
            let ContextKind::Pit { mappings, docs } = &ctx.kind else {
                return context_missing(&id);
            };
            let opts = search::SearchOptions { typed: true, pit: true, ..Default::default() };
            return match search::search_with(mappings, docs, &req, &opts) {
                Ok(mut resp) => {
                    resp["pit_id"] = json!(id);
                    (200, resp)
                }
                Err(e) => (e.status, e.to_json()),
            };
        }
        let is_wildcard = index_pattern == "_all" || index_pattern == "*";
        let names = Self::resolve_indices(&s, index_pattern);
        let ignore_unavailable = q.get("ignore_unavailable").is_some_and(|v| v == "true");
        if names.is_empty() && !is_wildcard {
            if ignore_unavailable && action == "_search" {
                return (
                    200,
                    json!({"took": 0, "timed_out": false,
                           "_shards": {"total": 0, "successful": 0, "skipped": 0, "failed": 0},
                           "hits": {"total": {"value": 0, "relation": "eq"}, "max_score": 0.0, "hits": []}}),
                );
            }
            return missing_index(index_pattern);
        }
        if action == "_count" {
            let counts: Result<Vec<u64>, search::EsError> = names
                .iter()
                .filter_map(|n| s.indices.get(n))
                .map(|i| search::count(&i.mappings, &i.committed, &req))
                .collect();
            let total: u64 = match counts {
                Ok(cs) => cs.into_iter().sum(),
                Err(e) => return (e.status, e.to_json()),
            };
            let shards = names.len().max(1);
            return (
                200,
                json!({"count": total, "_shards": {"total": shards, "successful": shards, "skipped": 0, "failed": 0}}),
            );
        }
        // A query spanning more than one index may mix mappings, so fields
        // fall back to runtime type inference rather than any one index's
        // explicit mapping (see `search::tokens_for`).
        let (mappings, docs, typed): (Value, Vec<CommittedDoc>, bool) = if names.len() == 1 {
            let i = &s.indices[&names[0]];
            (i.mappings.clone(), i.committed.clone(), true)
        } else {
            // Several indices: their mappings merged (the first index to
            // map a field wins), so typed fields still sort and range as
            // their type.
            let mut docs = Vec::new();
            let mut props = Map::new();
            for n in &names {
                docs.extend(s.indices[n].committed.iter().cloned());
                if let Some(p) = s.indices[n].mappings.get("properties").and_then(Value::as_object)
                {
                    for (k, v) in p {
                        props.entry(k.clone()).or_insert_with(|| v.clone());
                    }
                }
            }
            (json!({"properties": props}), docs, !names.is_empty())
        };
        let scroll = q
            .get("scroll")
            .cloned()
            .or_else(|| req.get("scroll").and_then(Value::as_str).map(str::to_string));
        if let Some(scroll) = scroll {
            let keep = parse_keep_alive(&scroll).unwrap_or(std::time::Duration::from_secs(60));
            let size = req.get("size").and_then(Value::as_u64).unwrap_or(10) as usize;
            let opts = search::SearchOptions { typed, all_hits: true, ..Default::default() };
            let mut resp = match search::search_with(&mappings, &docs, &req, &opts) {
                Ok(r) => r,
                Err(e) => return (e.status, e.to_json()),
            };
            let hits = resp["hits"]["hits"].as_array().cloned().unwrap_or_default();
            resp["hits"]["hits"] = Value::Array(hits.iter().take(size).cloned().collect());
            let id = scroll_id();
            resp["_scroll_id"] = json!(id);
            s.contexts.insert(
                id,
                SearchContext {
                    expires: now + keep,
                    kind: ContextKind::Scroll {
                        total: resp["hits"]["total"].clone(),
                        max_score: resp["hits"]["max_score"].clone(),
                        hits,
                        pos: size,
                        size,
                    },
                },
            );
            return (200, resp);
        }
        match search::search_typed(&mappings, &docs, &req, typed) {
            Ok(resp) => (200, resp),
            Err(e) => (e.status, e.to_json()),
        }
    }

    /// `POST/GET /_search/scroll` (next page) and `DELETE /_search/scroll`
    /// (clear one, several, or `_all`).
    fn scroll_api(
        &self,
        method: &str,
        path_id: Option<&str>,
        q: &HashMap<String, String>,
        body: &[u8],
    ) -> (u16, Value) {
        let Some(req) = parse_json(body) else { return (400, malformed_body()) };
        let mut s = self.0.lock().unwrap();
        let now = std::time::Instant::now();
        s.contexts.retain(|_, c| c.expires > now);
        let ids: Vec<String> = match (path_id, req.get("scroll_id"), q.get("scroll_id")) {
            (Some(p), _, _) => p.split(',').map(str::to_string).collect(),
            (_, Some(Value::String(i)), _) => vec![i.clone()],
            (_, Some(Value::Array(a)), _) => {
                a.iter().filter_map(Value::as_str).map(str::to_string).collect()
            }
            (_, _, Some(i)) => vec![i.clone()],
            _ => Vec::new(),
        };
        if method == "DELETE" {
            let freed = if ids.iter().any(|i| i == "_all") {
                let before = s.contexts.len();
                s.contexts.retain(|_, c| !matches!(c.kind, ContextKind::Scroll { .. }));
                before - s.contexts.len()
            } else {
                ids.iter().filter(|i| s.contexts.remove(i.as_str()).is_some()).count()
            };
            let all = ids.iter().any(|i| i == "_all");
            let status = if freed == 0 && !all { 404 } else { 200 };
            return (status, json!({"succeeded": true, "num_freed": freed}));
        }
        let Some(id) = ids.first().cloned() else {
            return (
                400,
                error(
                    "action_request_validation_exception",
                    "Validation Failed: 1: scrollId is missing;",
                    400,
                ),
            );
        };
        let keep = req
            .get("scroll")
            .and_then(Value::as_str)
            .or(q.get("scroll").map(String::as_str))
            .and_then(parse_keep_alive);
        let Some(ctx) = s.contexts.get_mut(&id) else { return context_missing(&id) };
        if let Some(k) = keep {
            ctx.expires = now + k;
        }
        let ContextKind::Scroll { hits, total, max_score, pos, size } = &mut ctx.kind else {
            return context_missing(&id);
        };
        let page: Vec<Value> = hits.iter().skip(*pos).take(*size).cloned().collect();
        *pos += *size;
        (
            200,
            json!({
                "_scroll_id": id,
                "took": 0,
                "timed_out": false,
                "_shards": {"total": 1, "successful": 1, "skipped": 0, "failed": 0},
                "hits": {"total": total, "max_score": max_score, "hits": page},
            }),
        )
    }

    /// `POST /<index>/_pit?keep_alive=1m`: freezes the index's searchable
    /// documents for later `pit` searches.
    fn open_pit(&self, index_pattern: &str, q: &HashMap<String, String>) -> (u16, Value) {
        let Some(keep) = q.get("keep_alive").and_then(|k| parse_keep_alive(k)) else {
            return (
                400,
                error(
                    "action_request_validation_exception",
                    "Validation Failed: 1: [keep_alive] is not specified;",
                    400,
                ),
            );
        };
        let mut s = self.0.lock().unwrap();
        let names = Self::resolve_indices(&s, index_pattern);
        if names.is_empty() {
            return missing_index(index_pattern);
        }
        let mut docs = Vec::new();
        let mut props = Map::new();
        for n in &names {
            docs.extend(s.indices[n].committed.iter().cloned());
            if let Some(p) = s.indices[n].mappings.get("properties").and_then(Value::as_object) {
                for (k, v) in p {
                    props.entry(k.clone()).or_insert_with(|| v.clone());
                }
            }
        }
        let id = scroll_id();
        s.contexts.insert(
            id.clone(),
            SearchContext {
                expires: std::time::Instant::now() + keep,
                kind: ContextKind::Pit { mappings: json!({"properties": props}), docs },
            },
        );
        (200, json!({"id": id}))
    }

    /// `DELETE /_pit` with `{"id": ...}`.
    fn close_pit(&self, method: &str, body: &[u8]) -> (u16, Value) {
        if method != "DELETE" {
            return no_handler(method, "/_pit");
        }
        let Some(req) = parse_json(body) else { return (400, malformed_body()) };
        let id = req.get("id").and_then(Value::as_str).unwrap_or("");
        let freed = usize::from(self.0.lock().unwrap().contexts.remove(id).is_some());
        (if freed == 0 { 404 } else { 200 }, json!({"succeeded": true, "num_freed": freed}))
    }

    fn analyze(&self, body: &[u8]) -> (u16, Value) {
        let req = parse_json(body).unwrap_or_else(|| json!({}));
        let Some(text) = req.get("text").and_then(|t| {
            t.as_str().map(str::to_string).or_else(|| {
                t.as_array()
                    .map(|a| a.iter().filter_map(Value::as_str).collect::<Vec<_>>().join(" "))
            })
        }) else {
            return (400, error("x_content_parse_exception", "text is required", 400));
        };
        let analyzer = req.get("analyzer").and_then(Value::as_str).unwrap_or("standard");
        let tokens = analysis::analyze(analyzer, &text);
        let mut position = 0i64;
        let mut offset = 0usize;
        let out: Vec<Value> = tokens
            .into_iter()
            .map(|t| {
                let start = offset;
                offset += t.chars().count();
                let v = json!({"token": t, "start_offset": start, "end_offset": offset, "type": "<ALPHANUM>", "position": position});
                position += 1;
                v
            })
            .collect();
        (200, json!({"tokens": out}))
    }

    fn index_api(&self, method: &str, name: &str, body: &[u8]) -> (u16, Value) {
        let mut s = self.0.lock().unwrap();
        match method {
            "PUT" => {
                if !valid_index(name) {
                    return (
                        400,
                        error(
                            "invalid_index_name_exception",
                            &format!(
                                "Invalid index name [{name}], must not contain the following characters [\\\\/*?\"<>| ,#:]"
                            ),
                            400,
                        ),
                    );
                }
                if s.indices.contains_key(name) {
                    return (
                        400,
                        error(
                            "resource_already_exists_exception",
                            &format!("index [{name}/noida] already exists"),
                            400,
                        ),
                    );
                }
                let req: Value = parse_json(body).unwrap_or_else(|| json!({}));
                let mut index = new_index_from_templates(&s.templates, name);
                if let Some(m) = req.get("mappings") {
                    merge(&mut index.mappings, m.clone());
                }
                if let Some(st) = req.get("settings") {
                    merge(&mut index.settings, st.clone());
                }
                if let Some(a) = req.get("aliases").and_then(Value::as_object) {
                    index.aliases.extend(a.clone());
                }
                s.indices.insert(name.to_string(), index);
                (200, json!({"acknowledged":true,"shards_acknowledged":true,"index":name}))
            }
            "GET" => {
                let Some(i) = s.indices.get(name) else {
                    return missing_index(name);
                };
                (
                    200,
                    json!({(name):{"aliases":i.aliases,"mappings":i.mappings,"settings":i.settings}}),
                )
            }
            "DELETE" => {
                if s.indices.remove(name).is_some() {
                    (200, json!({"acknowledged":true}))
                } else {
                    missing_index(name)
                }
            }
            "HEAD" => {
                if s.indices.contains_key(name) {
                    (200, json!({}))
                } else {
                    (404, json!({}))
                }
            }
            _ => (405, error("method_not_allowed_exception", "Incorrect HTTP method", 405)),
        }
    }

    fn mapping_api(&self, method: &str, name: &str, body: &[u8]) -> (u16, Value) {
        let mut s = self.0.lock().unwrap();
        let Some(i) = s.indices.get_mut(name) else {
            return missing_index(name);
        };
        match method {
            // `{"<index>": {"mappings": {...}}}`, as Elasticsearch shapes it.
            "GET" => (200, json!({(name): {"mappings": i.mappings}})),
            "PUT" | "POST" => {
                let next = parse_json(body).unwrap_or_else(|| json!({}));
                merge(&mut i.mappings, next);
                (200, json!({"acknowledged":true}))
            }
            _ => (405, error("method_not_allowed_exception", "Incorrect HTTP method", 405)),
        }
    }

    fn settings_api(&self, method: &str, name: &str, body: &[u8]) -> (u16, Value) {
        let mut s = self.0.lock().unwrap();
        let Some(i) = s.indices.get_mut(name) else {
            return missing_index(name);
        };
        match method {
            "GET" => (200, json!({(name):{"settings":i.settings}})),
            "PUT" => {
                merge(&mut i.settings, parse_json(body).unwrap_or_else(|| json!({})));
                (200, json!({"acknowledged":true}))
            }
            _ => (405, error("method_not_allowed_exception", "Incorrect HTTP method", 405)),
        }
    }

    fn index_action(&self, _method: &str, name: &str, action: &str) -> (u16, Value) {
        let mut s = self.0.lock().unwrap();
        let Some(i) = s.indices.get_mut(name) else {
            return missing_index(name);
        };
        match action {
            "_refresh" => {
                i.refresh(name);
                (200, json!({"_shards":{"total":1,"successful":1,"failed":0}}))
            }
            "_flush" => (200, json!({"_shards":{"total":1,"successful":1,"failed":0}})),
            "_open" => {
                i.opened = true;
                (200, json!({"acknowledged":true,"shards_acknowledged":true}))
            }
            "_close" => {
                i.opened = false;
                (200, json!({"acknowledged":true,"shards_acknowledged":true}))
            }
            _ => (200, json!({})),
        }
    }

    fn document_api(
        &self,
        method: &str,
        index: &str,
        id: &str,
        kind: &str,
        q: &HashMap<String, String>,
        body: &[u8],
    ) -> (u16, Value) {
        let mut s = self.0.lock().unwrap();
        // Real Elasticsearch auto-creates an index on its first write
        // (`action.auto_create_index`, on by default) -- found via
        // testing before a public release: this engine required the
        // index to already exist even for PUT/POST, so the ordinary
        // "just start writing" pattern every real client relies on 404'd
        // instead. GET/HEAD/DELETE on a genuinely missing index still
        // correctly 404 below -- only the write path auto-creates.
        if !s.indices.contains_key(index) && matches!(method, "PUT" | "POST") {
            let created = new_index_from_templates(&s.templates, index);
            s.indices.insert(index.to_string(), created);
        }
        let Some(i) = s.indices.get_mut(index) else {
            return missing_index(index);
        };
        if kind == "_source" {
            return i
                .docs
                .get(id)
                .map(|d| (200, d.source.clone()))
                .unwrap_or_else(|| missing_doc(index, id));
        }
        if id.is_empty() && method != "POST" {
            return (405, error("method_not_allowed_exception", "Incorrect HTTP method", 405));
        }
        let id = if id.is_empty() { auto_id() } else { id.to_string() };
        match method {
            "GET" | "HEAD" => match i.docs.get(&id) {
                Some(d) => {
                    let mut r = doc_response(index, &id, d, "");
                    if let Some(f) = source_filter_from_params(q) {
                        r["_source"] = search::filter_source(&d.source, Some(&f));
                    }
                    (200, r)
                }
                None => {
                    if method == "HEAD" {
                        (404, json!({}))
                    } else {
                        missing_doc(index, &id)
                    }
                }
            },
            "PUT" | "POST" => {
                if kind == "_create" && i.docs.contains_key(&id) {
                    return (409, version_conflict(index, &id));
                }
                // Optimistic concurrency: `?if_seq_no=...&if_primary_term=...`
                // (the standard compare-and-swap pattern -- GET a document,
                // write it back only if nothing else has touched it since)
                // must reject a write made against a stale seq_no/
                // primary_term with a real version conflict, not silently
                // overwrite. `_primary_term` is always 1 in this engine (no
                // real shard/replica model), matching what `doc_response`
                // itself always reports.
                if let (Some(want_seq), Some(want_term)) =
                    (q.get("if_seq_no"), q.get("if_primary_term"))
                {
                    let want = want_seq.parse::<i64>().ok().zip(want_term.parse::<i64>().ok());
                    let current = i.docs.get(&id).map(|d| (d.seq, 1i64));
                    if current != want {
                        return (409, version_conflict(index, &id));
                    }
                }
                let Some(src) = parse_json(body) else {
                    return (
                        400,
                        error("x_content_parse_exception", "Failed to parse content to map", 400),
                    );
                };
                dynamic_mapping(&mut i.mappings, &src);
                let exists = i.docs.contains_key(&id);
                i.seq += 1;
                let seq = i.seq;
                let d = i.docs.entry(id.clone()).or_insert_with(|| {
                    i.order.push(id.clone());
                    Document { source: json!({}), version: 0, seq }
                });
                d.source = src;
                d.version += 1;
                d.seq = seq;
                let mut result = (
                    if exists { 200 } else { 201 },
                    doc_response(index, &id, d, if exists { "updated" } else { "created" }),
                );
                maybe_refresh(i, index, q);
                mark_forced_refresh(&mut result.1, q);
                result
            }
            "DELETE" => {
                if let Some(d) = i.docs.remove(&id) {
                    i.order.retain(|x| x != &id);
                    i.seq += 1;
                    let mut result = (
                        200,
                        json!({"_index":index,"_id":id,"_version":d.version+1,"result":"deleted","_shards":{"total":2,"successful":1,"failed":0},"_seq_no":i.seq,"_primary_term":1}),
                    );
                    maybe_refresh(i, index, q);
                    mark_forced_refresh(&mut result.1, q);
                    result
                } else {
                    missing_doc(index, &id)
                }
            }
            _ => (405, error("method_not_allowed_exception", "Incorrect HTTP method", 405)),
        }
    }

    fn update(&self, method: &str, index: &str, id: &str, body: &[u8]) -> (u16, Value) {
        if method != "POST" {
            return (405, error("method_not_allowed_exception", "Incorrect HTTP method", 405));
        }
        let req = parse_json(body).unwrap_or_else(|| json!({}));
        let patch = req.get("doc").cloned().unwrap_or_else(|| json!({}));
        {
            let mut s = self.0.lock().unwrap();
            let Some(i) = s.indices.get_mut(index) else { return missing_index(index) };
            if let Some(d) = i.docs.get_mut(id) {
                merge(&mut d.source, patch);
                i.seq += 1;
                d.seq = i.seq;
                d.version += 1;
                return (200, doc_response(index, id, d, "updated"));
            }
            if let Some(src) = req.get("upsert").or_else(|| {
                if req.get("doc_as_upsert").and_then(Value::as_bool) == Some(true) {
                    Some(&patch)
                } else {
                    None
                }
            }) {
                dynamic_mapping(&mut i.mappings, src);
                i.seq += 1;
                let seq = i.seq;
                i.order.push(id.to_string());
                i.docs.insert(id.to_string(), Document { source: src.clone(), version: 1, seq });
                return (201, doc_response(index, id, i.docs.get(id).unwrap(), "created"));
            }
        }
        (404, error("document_missing_exception", &format!("[{}]: document missing", id), 404))
    }

    fn bulk(
        &self,
        method: &str,
        index: &str,
        q: &HashMap<String, String>,
        body: &[u8],
    ) -> (u16, Value) {
        if method != "POST" && method != "PUT" {
            return (405, error("method_not_allowed_exception", "Incorrect HTTP method", 405));
        }
        let text = String::from_utf8_lossy(body);
        let mut lines = text.lines().filter(|l| !l.trim().is_empty());
        let mut items = Vec::new();
        let mut errors = false;
        let no_refresh = HashMap::new();
        let mut touched: Vec<String> = Vec::new();
        while let Some(meta) = lines.next() {
            let Ok(m) = serde_json::from_str::<Value>(meta) else {
                errors = true;
                items.push(json!({"index":{"status":400,"error":{"type":"parse_exception","reason":"malformed action/metadata line"}}}));
                break;
            };
            let Some((action, opts)) = m.as_object().and_then(|o| o.iter().next()) else {
                continue;
            };
            let ix = opts.get("_index").and_then(Value::as_str).unwrap_or(index);
            let id =
                opts.get("_id").and_then(Value::as_str).map(str::to_string).unwrap_or_else(auto_id);
            touched.push(ix.to_string());
            if action == "update" {
                // A partial update (`{"doc": ...}` / upsert), not a
                // replace. Found via testing before a public release: bulk
                // `update` used to store the `{"doc": ...}` wrapper itself as
                // the new document.
                let data = lines.next().unwrap_or("").as_bytes();
                let (status, mut res) = self.update("POST", ix, &id, data);
                errors |= status >= 300;
                res["status"] = json!(status);
                let mut item = Map::new();
                item.insert(action.to_string(), res);
                items.push(Value::Object(item));
            } else if action == "delete" {
                let (status, mut res) =
                    self.document_api("DELETE", ix, &id, "_doc", &no_refresh, b"");
                errors |= status >= 300;
                res["status"] = json!(status);
                let mut item = Map::new();
                item.insert(action.to_string(), res);
                items.push(Value::Object(item));
            } else {
                let data = lines.next().unwrap_or("").as_bytes();
                let verb = if action == "create" { "POST" } else { "PUT" };
                let kind = if action == "create" { "_create" } else { "_doc" };
                let (status, mut res) = self.document_api(verb, ix, &id, kind, &no_refresh, data);
                errors |= status >= 300;
                res["status"] = json!(status);
                let mut item = Map::new();
                item.insert(action.to_string(), res);
                items.push(Value::Object(item));
            }
        }
        if wants_refresh(q) {
            let mut s = self.0.lock().unwrap();
            touched.sort();
            touched.dedup();
            for ix in touched {
                if let Some(i) = s.indices.get_mut(&ix) {
                    i.refresh(&ix);
                }
            }
        }
        (200, json!({"took":0,"errors":errors,"items":items}))
    }

    fn mget(
        &self,
        method: &str,
        index: &str,
        q: &HashMap<String, String>,
        body: &[u8],
    ) -> (u16, Value) {
        if method != "POST" && method != "GET" {
            return (405, error("method_not_allowed_exception", "Incorrect HTTP method", 405));
        }
        let req = parse_json(body).unwrap_or_else(|| json!({}));
        // `{"docs": [{"_id": ...}]}` or the `{"ids": [...]}` shorthand.
        let mut docs = req.get("docs").and_then(Value::as_array).cloned().unwrap_or_default();
        if let Some(ids) = req.get("ids").and_then(Value::as_array) {
            docs.extend(ids.iter().map(|id| json!({"_id": id})));
        }
        let filter = source_filter_from_params(q);
        let items=docs.iter().map(|d|{let ix=d.get("_index").and_then(Value::as_str).unwrap_or(index); let id=d.get("_id").and_then(Value::as_str).unwrap_or("");let s=self.0.lock().unwrap(); let doc=s.indices.get(ix).and_then(|i|i.docs.get(id)); match doc {Some(doc)=>json!({"_index":ix,"_id":id,"_version":doc.version,"_seq_no":doc.seq,"_primary_term":1,"found":true,"_source":search::filter_source(&doc.source, filter.as_ref())}),None=>json!({"_index":ix,"_id":id,"found":false})}}).collect::<Vec<_>>();
        (200, json!({"docs":items}))
    }

    /// `_delete_by_query` / `_update_by_query` (no script: a reindex in
    /// place, which bumps each matching document's version). Like
    /// Elasticsearch, they act on the refreshed (searchable) view.
    fn by_query(
        &self,
        method: &str,
        action: &str,
        index: &str,
        q: &HashMap<String, String>,
        body: &[u8],
    ) -> (u16, Value) {
        if method != "POST" {
            return (405, error("method_not_allowed_exception", "Incorrect HTTP method", 405));
        }
        let req = parse_json(body).unwrap_or_else(|| json!({}));
        if action == "_update_by_query" && req.get("script").is_some() {
            return (
                400,
                error("illegal_argument_exception", "scripts are not supported by noida-db", 400),
            );
        }
        let query = req.get("query").cloned().unwrap_or_else(|| json!({"match_all": {}}));
        let mut s = self.0.lock().unwrap();
        let names = Self::resolve_indices(&s, index);
        if names.is_empty() {
            return missing_index(index);
        }
        let mut total = 0;
        for name in &names {
            let i = s.indices.get_mut(name).unwrap();
            let matched = match search::eval(&query, &i.mappings, &i.committed) {
                Ok(m) => m,
                Err(e) => return (e.status, e.to_json()),
            };
            let ids: Vec<String> = matched.keys().map(|&k| i.committed[k].id.clone()).collect();
            for id in ids {
                if action == "_delete_by_query" {
                    if i.docs.remove(&id).is_some() {
                        i.order.retain(|x| x != &id);
                        i.seq += 1;
                        total += 1;
                    }
                } else if let Some(d) = i.docs.get_mut(&id) {
                    i.seq += 1;
                    d.seq = i.seq;
                    d.version += 1;
                    total += 1;
                }
            }
            maybe_refresh(i, name, q);
        }
        let mut out = json!({"took": 0, "timed_out": false, "total": total, "batches": 1,
            "version_conflicts": 0, "noops": 0, "failures": [],
            "retries": {"bulk": 0, "search": 0}, "throttled_millis": 0,
            "requests_per_second": -1.0, "throttled_until_millis": 0});
        out[if action == "_delete_by_query" { "deleted" } else { "updated" }] = json!(total);
        (200, out)
    }

    /// `PUT|DELETE /<index>/_alias/<name>` and `GET /<index>/_alias`.
    fn index_alias(&self, method: &str, index: &str, alias: Option<&str>) -> (u16, Value) {
        let mut s = self.0.lock().unwrap();
        let Some(i) = s.indices.get_mut(index) else { return missing_index(index) };
        match (method, alias) {
            ("PUT" | "POST", Some(a)) => {
                i.aliases.insert(a.to_string(), json!({}));
                (200, json!({"acknowledged": true}))
            }
            ("DELETE", Some(a)) => {
                if i.aliases.remove(a).is_some() {
                    (200, json!({"acknowledged": true}))
                } else {
                    (
                        404,
                        error(
                            "aliases_not_found_exception",
                            &format!("aliases [{a}] missing"),
                            404,
                        ),
                    )
                }
            }
            ("GET", _) => {
                let a: Map<String, Value> = i
                    .aliases
                    .iter()
                    .filter(|(k, _)| alias.is_none_or(|x| x == k.as_str()))
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                (200, json!({(index): {"aliases": a}}))
            }
            _ => (405, error("method_not_allowed_exception", "Incorrect HTTP method", 405)),
        }
    }

    fn aliases(&self, method: &str, body: &[u8]) -> (u16, Value) {
        if method != "POST" {
            return (405, error("method_not_allowed_exception", "Incorrect HTTP method", 405));
        }
        let req = parse_json(body).unwrap_or_else(|| json!({}));
        let Some(actions) = req.get("actions").and_then(Value::as_array) else {
            return (
                400,
                error("x_content_parse_exception", "Required [actions] field missing", 400),
            );
        };
        let mut s = self.0.lock().unwrap();
        for action in actions {
            let Some((op, v)) = action.as_object().and_then(|o| o.iter().next()) else { continue };
            let ix = v.get("index").and_then(Value::as_str).unwrap_or("");
            let alias = v.get("alias").and_then(Value::as_str).unwrap_or("");
            if let Some(i) = s.indices.get_mut(ix) {
                if op == "remove" {
                    i.aliases.remove(alias);
                } else if op == "add" {
                    i.aliases.insert(alias.to_string(), v.clone());
                }
            }
        }
        (200, json!({"acknowledged":true}))
    }
    fn get_alias(&self, segments: &[&str]) -> (u16, Value) {
        let s = self.0.lock().unwrap();
        let alias = segments.get(1).copied();
        let mut out = Map::new();
        for (name, i) in &s.indices {
            let a: Map<String, Value> = i
                .aliases
                .iter()
                .filter(|(k, _)| alias.is_none() || Some(k.as_str()) == alias)
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            if !a.is_empty() {
                out.insert(name.clone(), json!({"aliases":a}));
            }
        }
        if out.is_empty() {
            (404, error("alias_missing_exception", "alias does not exist", 404))
        } else {
            (200, Value::Object(out))
        }
    }

    fn template(&self, method: &str, segments: &[&str], body: &[u8]) -> (u16, Value) {
        let name = segments.get(1).copied().unwrap_or("");
        let mut s = self.0.lock().unwrap();
        match method {
            "PUT" => {
                s.templates.insert(name.to_string(), parse_json(body).unwrap_or_else(|| json!({})));
                (200, json!({"acknowledged":true}))
            }
            "GET" => s.templates.get(name).map(|v| (200, json!({(name):v}))).unwrap_or_else(|| {
                (404, error("resource_not_found_exception", "index template missing", 404))
            }),
            "DELETE" => {
                if s.templates.remove(name).is_some() {
                    (200, json!({"acknowledged":true}))
                } else {
                    (404, error("resource_not_found_exception", "index template missing", 404))
                }
            }
            _ => (405, error("method_not_allowed_exception", "Incorrect HTTP method", 405)),
        }
    }
}

pub fn parse_json(bytes: &[u8]) -> Option<Value> {
    if bytes.is_empty() { Some(json!({})) } else { serde_json::from_slice(bytes).ok() }
}
pub fn error(kind: &str, reason: &str, status: u16) -> Value {
    json!({"error":{"root_cause":[{"type":kind,"reason":reason}],"type":kind,"reason":reason},"status":status})
}
fn missing_index(name: &str) -> (u16, Value) {
    (
        404,
        json!({"error":{"root_cause":[{"type":"index_not_found_exception","reason":format!("no such index [{name}]"),"index_uuid":"_na_","resource.type":"index_or_alias","resource.id":name,"index":name}],"type":"index_not_found_exception","reason":format!("no such index [{name}]"),"index_uuid":"_na_","resource.type":"index_or_alias","resource.id":name,"index":name},"status":404}),
    )
}
/// An opaque id for a scroll or point in time.
fn scroll_id() -> String {
    use std::sync::atomic::AtomicU64;
    static NEXT: AtomicU64 = AtomicU64::new(1);
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("FGx1ZXJ5QW5kRmV0Y2gB{nanos:x}{n:x}")
}

fn missing_doc(index: &str, id: &str) -> (u16, Value) {
    (404, json!({"_index":index,"_id":id,"found":false}))
}
/// A request body that isn't JSON.
pub fn malformed_body() -> Value {
    error("x_content_parse_exception", "Failed to parse request body: not valid JSON", 400)
}
/// Elasticsearch's answer for a path/method no REST handler takes.
pub fn no_handler(method: &str, path: &str) -> (u16, Value) {
    (400, json!({"error": format!("no handler found for uri [{path}] and method [{method}]")}))
}
fn version_conflict(_index: &str, id: &str) -> Value {
    error(
        "version_conflict_engine_exception",
        &format!("[{}]: version conflict, document already exists", id),
        409,
    )
}
fn doc_response(index: &str, id: &str, d: &Document, result: &str) -> Value {
    let mut v =
        json!({"_index":index,"_id":id,"_version":d.version,"_seq_no":d.seq,"_primary_term":1});
    if !result.is_empty() {
        v["result"] = json!(result);
        v["_shards"] = json!({"total":2,"successful":1,"failed":0});
    } else {
        v["found"] = json!(true);
        v["_source"] = d.source.clone();
    }
    v
}
fn valid_index(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 255
        && s == s.to_lowercase()
        && !s.starts_with(['_', '-', '+'])
        && !s.chars().any(|c| {
            matches!(c, '\\' | '/' | '*' | '?' | '"' | '<' | '>' | '|' | ' ' | ',' | '#' | ':')
        })
}
fn auto_id() -> String {
    const A: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let n = IDS.fetch_add(1, Ordering::Relaxed)
        ^ ((std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()) as u64);
    (0..20)
        .map(|i| {
            A[((n
                .rotate_left((i % 63) as u32)
                .wrapping_add((i as u64).wrapping_mul(0x9e3779b97f4a7c15))
                >> (i % 8 * 8)) as usize)
                % A.len()] as char
        })
        .collect()
}
fn wants_refresh(q: &HashMap<String, String>) -> bool {
    matches!(q.get("refresh").map(String::as_str), Some("true") | Some("wait_for") | Some(""))
}

fn maybe_refresh(i: &mut Index, name: &str, q: &HashMap<String, String>) {
    if wants_refresh(q) {
        i.refresh(name);
    }
}

/// `refresh=true` (not `wait_for`) adds `"forced_refresh": true` to a
/// write's response.
fn mark_forced_refresh(resp: &mut Value, q: &HashMap<String, String>) {
    if q.get("refresh").is_some_and(|v| v.is_empty() || v == "true") {
        resp["forced_refresh"] = json!(true);
    }
}

fn query_params(q: &str) -> HashMap<String, String> {
    q.split('&')
        .filter_map(|p| p.split_once('='))
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}
fn merge(a: &mut Value, b: Value) {
    if let (Some(x), Some(y)) = (a.as_object_mut(), b.as_object()) {
        for (k, v) in y {
            if let Some(existing) = x.get_mut(k) {
                merge(existing, v.clone());
            } else {
                x.insert(k.clone(), v.clone());
            }
        }
    } else {
        *a = b;
    }
}
fn dynamic_mapping(m: &mut Value, src: &Value) {
    if m.get("properties").is_none() {
        m["properties"] = json!({});
    }
    let props = m["properties"].as_object_mut().unwrap();
    if let Some(fields) = src.as_object() {
        for (k, v) in fields {
            if props.contains_key(k) {
                continue;
            }
            let ty = match v {
                // `date_detection` (on by default): an ISO date string maps
                // as a date, not text.
                Value::String(s) if looks_like_date(s) => "date",
                Value::String(_) => "text",
                Value::Bool(_) => "boolean",
                Value::Number(n) => {
                    if n.is_i64() {
                        "long"
                    } else {
                        "float"
                    }
                }
                Value::Array(a) => a
                    .first()
                    .map(|v| match v {
                        Value::String(_) => "text",
                        Value::Bool(_) => "boolean",
                        Value::Number(n) => {
                            if n.is_i64() {
                                "long"
                            } else {
                                "float"
                            }
                        }
                        Value::Object(_) => "object",
                        _ => "object",
                    })
                    .unwrap_or("object"),
                _ => "object",
            };
            props.insert(k.clone(),if ty=="text"{json!({"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}})}else{json!({"type":ty})});
        }
    }
}

/// A new index's starting state: every composable index template whose
/// `index_patterns` match `name`, lowest `priority` first so the highest
/// wins. Applies to explicit creation and to auto-creation on first write.
/// Found via testing before a public release: templates were stored but
/// never applied.
fn new_index_from_templates(templates: &HashMap<String, Value>, name: &str) -> Index {
    let mut index = Index {
        mappings: json!({"properties": {}}),
        settings: json!({"index": {"number_of_shards": "1", "number_of_replicas": "1"}}),
        opened: true,
        // Writes bump this before using it: the first gets seq_no 0.
        seq: -1,
        ..Index::default()
    };
    let mut matching: Vec<&Value> = templates
        .values()
        .filter(|t| {
            let pats: Vec<&str> = match t.get("index_patterns") {
                Some(Value::String(p)) => vec![p.as_str()],
                Some(Value::Array(a)) => a.iter().filter_map(Value::as_str).collect(),
                _ => vec![],
            };
            pats.iter().any(|p| match p.strip_suffix('*') {
                Some(prefix) => name.starts_with(prefix),
                None => *p == name,
            })
        })
        .collect();
    matching.sort_by_key(|t| t.get("priority").and_then(Value::as_i64).unwrap_or(0));
    for t in matching {
        let Some(tpl) = t.get("template") else { continue };
        if let Some(m) = tpl.get("mappings") {
            merge(&mut index.mappings, m.clone());
        }
        if let Some(st) = tpl.get("settings") {
            merge(&mut index.settings, st.clone());
        }
        if let Some(a) = tpl.get("aliases").and_then(Value::as_object) {
            index.aliases.extend(a.clone());
        }
    }
    index
}

/// `_source`, `_source_includes` and `_source_excludes` URL parameters as
/// a body-style `_source` filter.
fn source_filter_from_params(q: &HashMap<String, String>) -> Option<Value> {
    let list =
        |k: &str| q.get(k).map(|v| v.split(',').map(|s| json!(s.trim())).collect::<Vec<_>>());
    let includes = list("_source_includes");
    let excludes = list("_source_excludes");
    if includes.is_some() || excludes.is_some() {
        return Some(
            json!({"includes": includes.unwrap_or_default(), "excludes": excludes.unwrap_or_default()}),
        );
    }
    match q.get("_source").map(String::as_str) {
        Some("false") => Some(json!(false)),
        Some("true") | None => None,
        Some(fields) => Some(json!({"includes": fields.split(',').collect::<Vec<_>>()})),
    }
}

/// `strict_date_optional_time`: `yyyy-MM-dd` optionally followed by
/// `THH:mm[:ss[.fff]]` and a zone.
fn looks_like_date(s: &str) -> bool {
    let b = s.as_bytes();
    let digits =
        |r: std::ops::Range<usize>| r.clone().all(|i| b.get(i).is_some_and(u8::is_ascii_digit));
    if b.len() < 10
        || !digits(0..4)
        || b[4] != b'-'
        || !digits(5..7)
        || b[7] != b'-'
        || !digits(8..10)
    {
        return false;
    }
    b.len() == 10
        || (b[10] == b'T' && b.len() >= 16 && digits(11..13) && b[13] == b':' && digits(14..16))
}
