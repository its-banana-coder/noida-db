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
        if segments.first() == Some(&"_search") || segments.first() == Some(&"_count") {
            return self.search_or_count(method, segments[0], "*", body);
        }
        if segments.first() == Some(&"_analyze") {
            return self.analyze(body);
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
            "_search" | "_count" => self.search_or_count(method, segments[1], index_name, body),
            "_analyze" => self.analyze(body),
            "_doc" | "_create" | "_source" if segments.len() >= 3 => {
                self.document_api(method, index_name, segments[2], segments[1], &q, body)
            }
            "_doc" if method == "POST" => {
                self.document_api(method, index_name, "", "_doc", &q, body)
            }
            "_bulk" => self.bulk(method, index_name, &q, body),
            "_update" if segments.len() >= 3 => self.update(method, index_name, segments[2], body),
            "_mget" => self.mget(method, index_name, body),
            _ => (404, error("not_found", "no handler found for uri", 404)),
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
        body: &[u8],
    ) -> (u16, Value) {
        if method != "GET" && method != "POST" {
            return (405, error("method_not_allowed_exception", "Incorrect HTTP method", 405));
        }
        let req = parse_json(body).unwrap_or_else(|| json!({}));
        let s = self.0.lock().unwrap();
        let is_wildcard = index_pattern == "_all" || index_pattern == "*";
        let names = Self::resolve_indices(&s, index_pattern);
        if names.is_empty() && !is_wildcard {
            return missing_index(index_pattern);
        }
        if action == "_count" {
            let total: u64 = names
                .iter()
                .filter_map(|n| s.indices.get(n))
                .map(|i| search::count(&i.mappings, &i.committed, &req))
                .sum();
            let shards = names.len().max(1);
            return (
                200,
                json!({"count": total, "_shards": {"total": shards, "successful": shards, "skipped": 0, "failed": 0}}),
            );
        }
        // A query spanning more than one index may mix mappings, so fields
        // fall back to runtime type inference rather than any one index's
        // explicit mapping (see `search::tokens_for`).
        let (mappings, docs): (Value, Vec<CommittedDoc>) = if names.len() == 1 {
            let i = &s.indices[&names[0]];
            (i.mappings.clone(), i.committed.clone())
        } else {
            let mut docs = Vec::new();
            for n in &names {
                docs.extend(s.indices[n].committed.iter().cloned());
            }
            (json!({"properties": {}}), docs)
        };
        (200, search::search(&mappings, &docs, &req))
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
                let mappings = req.get("mappings").cloned().unwrap_or(json!({"properties":{}}));
                let settings = req
                    .get("settings")
                    .cloned()
                    .unwrap_or(json!({"index":{"number_of_shards":"1","number_of_replicas":"1"}}));
                let aliases = req
                    .get("aliases")
                    .and_then(Value::as_object)
                    .cloned()
                    .unwrap_or_default()
                    .into_iter()
                    .collect();
                s.indices.insert(
                    name.to_string(),
                    Index { settings, mappings, aliases, opened: true, ..Index::default() },
                );
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
            "GET" => (200, json!({(name):i.mappings})),
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
                Some(d) => (200, doc_response(index, &id, d, "")),
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
                let result = (
                    if exists { 200 } else { 201 },
                    doc_response(index, &id, d, if exists { "updated" } else { "created" }),
                );
                maybe_refresh(i, index, q);
                result
            }
            "DELETE" => {
                if let Some(d) = i.docs.remove(&id) {
                    i.order.retain(|x| x != &id);
                    i.seq += 1;
                    let result = (
                        200,
                        json!({"_index":index,"_id":id,"_version":d.version+1,"result":"deleted","_shards":{"total":2,"successful":1,"failed":0},"_seq_no":i.seq,"_primary_term":1}),
                    );
                    maybe_refresh(i, index, q);
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
            if action == "delete" {
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

    fn mget(&self, method: &str, index: &str, body: &[u8]) -> (u16, Value) {
        if method != "POST" {
            return (405, error("method_not_allowed_exception", "Incorrect HTTP method", 405));
        }
        let req = parse_json(body).unwrap_or_else(|| json!({}));
        let docs = req.get("docs").and_then(Value::as_array).cloned().unwrap_or_default();
        let items=docs.iter().map(|d|{let ix=d.get("_index").and_then(Value::as_str).unwrap_or(index); let id=d.get("_id").and_then(Value::as_str).unwrap_or("");let s=self.0.lock().unwrap(); let doc=s.indices.get(ix).and_then(|i|i.docs.get(id)); match doc {Some(doc)=>json!({"_index":ix,"_id":id,"_version":doc.version,"found":true,"_source":doc.source}),None=>json!({"_index":ix,"_id":id,"found":false,"_source":null})}}).collect::<Vec<_>>();
        (200, json!({"docs":items}))
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
fn missing_doc(index: &str, id: &str) -> (u16, Value) {
    (404, json!({"_index":index,"_id":id,"found":false,"_source":null}))
}
fn version_conflict(_index: &str, id: &str) -> Value {
    error(
        "version_conflict_engine_exception",
        &format!("[{}]: version conflict, document already exists", id),
        409,
    )
}
fn doc_response(index: &str, id: &str, d: &Document, result: &str) -> Value {
    let mut v = json!({"_index":index,"_id":id,"_version":d.version,"_seq_no":d.seq,"_primary_term":1,"_shards":{"total":2,"successful":1,"failed":0}});
    if !result.is_empty() {
        v["result"] = json!(result);
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
            A[((n.rotate_left((i % 63) as u32).wrapping_add(i as u64 * 0x9e3779b97f4a7c15)
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
