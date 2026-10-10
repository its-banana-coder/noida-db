use serde_json::{Map, Value, json};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use super::analysis;
use super::cat;
use super::dates;
use super::painless;
use super::search::{self, CommittedDoc};
use super::suggest;

#[derive(Clone)]
pub struct Engine(Arc<Mutex<State>>);

use serde::{Deserialize, Serialize};

use super::templates::Templates;
use super::vectors;

mod cat_tables;
mod cluster;
mod monitor;
pub(super) mod node;
mod nodes;
mod stats;

#[derive(Default, Serialize, Deserialize)]
struct State {
    indices: HashMap<String, Index>,
    #[serde(flatten)]
    templates: Templates,
    /// `PUT _cluster/settings` values (persistent, transient).
    #[serde(default)]
    cluster_settings: HashMap<String, Value>,
    /// Voting exclusions and desired nodes.
    #[serde(default)]
    cluster_meta: cluster::ClusterMeta,
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
    /// Per-shard `_seq_no` counters when the index has several shards.
    #[serde(default)]
    shard_seq: HashMap<i64, i64>,
    opened: bool,
    /// The last-refreshed, searchable snapshot (near-real-time semantics:
    /// `_search` sees this, real-time GET reads `docs` directly).
    #[serde(skip)]
    committed: Vec<CommittedDoc>,
    /// When the searchable snapshot was last rebuilt, and when the index
    /// was last searched (for `refresh_interval` and search-idle shards).
    #[serde(skip)]
    last_refresh: Option<std::time::Instant>,
    #[serde(skip)]
    last_search: Option<std::time::Instant>,
    /// Operation counters for `_stats` (a node's, so not persisted).
    #[serde(skip)]
    counters: stats::Counters,
}

/// `index.refresh_interval` (default 1s; `-1` turns periodic refresh off).
fn refresh_interval(settings: &Value) -> Option<std::time::Duration> {
    let v = settings
        .get("index")
        .and_then(|i| i.get("refresh_interval"))
        .or_else(|| settings.get("refresh_interval"))
        .or_else(|| settings.get("index.refresh_interval"));
    match v
        .and_then(|v| v.as_str().map(str::to_string).or_else(|| v.as_i64().map(|n| n.to_string())))
    {
        None => Some(std::time::Duration::from_secs(1)),
        Some(s) if s == "-1" => None,
        Some(s) => parse_keep_alive(&s).or(Some(std::time::Duration::from_secs(1))),
    }
}

impl Index {
    /// Elasticsearch's periodic refresh, applied lazily when the index is
    /// searched: a refresh is due once `refresh_interval` has passed since
    /// the last one, and a search after 30s without searches (a
    /// "search-idle" shard) refreshes first.
    fn auto_refresh(&mut self, name: &str) {
        let now = std::time::Instant::now();
        let last_search = *self.last_search.get_or_insert(now);
        let last_refresh = *self.last_refresh.get_or_insert(now);
        if let Some(every) = refresh_interval(&self.settings)
            && (now.duration_since(last_refresh) >= every
                || now.duration_since(last_search) >= std::time::Duration::from_secs(30))
        {
            self.refresh(name);
        }
        self.last_search = Some(now);
    }

    fn refresh(&mut self, name: &str) {
        self.last_refresh = Some(std::time::Instant::now());
        self.committed = self
            .order
            .iter()
            .filter_map(|id| {
                self.docs.get(id).map(|d| CommittedDoc {
                    index: name.to_string(),
                    id: id.clone(),
                    source: d.source.clone(),
                    version: d.version,
                    seq: d.seq,
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
    /// The `routing` value it was written with, returned as `_routing`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    routing: Option<String>,
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
                index.counters.existing_store = true;
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
        let (status, mut resp) = self.route(method, path, query, body);
        self.observe(method, path, query, body, status, &resp);
        // `?local` on the alias reads is deprecated (it has no effect).
        if query.split('&').any(|p| p == "local" || p.starts_with("local="))
            && method == "GET"
            && let Some(m) = resp.as_object_mut()
        {
            let what = if path.starts_with("/_cat/aliases") {
                Some("cat-aliases")
            } else if path.contains("/_alias") {
                Some("get-aliases")
            } else {
                None
            };
            if let Some(what) = what {
                m.insert(
                    WARNINGS.into(),
                    json!([format!(
                        "the [?local=true] query parameter to {what} requests has no effect and will be removed in a future version"
                    )]),
                );
            }
        }
        // `rest_total_hits_as_int=true`: `hits.total` as the bare number
        // (pre-7.0 shape), in a search, scroll or each msearch response.
        if query.split('&').any(|p| p == "rest_total_hits_as_int=true") {
            fn flatten_total(v: &mut Value) {
                if let Some(h) = v.get_mut("hits").and_then(Value::as_object_mut) {
                    // `track_total_hits: false` reads as -1 in this shape.
                    let n = h.get("total").map_or(json!(-1), |t| t["value"].clone());
                    h.insert("total".into(), n);
                }
                if let Some(Value::Array(rs)) = v.get_mut("responses") {
                    rs.iter_mut().for_each(flatten_total);
                }
            }
            flatten_total(&mut resp);
        }
        // `flat_settings=true`: settings objects as dotted keys.
        if query.split('&').any(|p| p == "flat_settings=true") {
            fn flat(prefix: &str, v: &Value, out: &mut serde_json::Map<String, Value>) {
                match v {
                    Value::Object(m) if !m.is_empty() => {
                        for (k, x) in m {
                            let key =
                                if prefix.is_empty() { k.clone() } else { format!("{prefix}.{k}") };
                            flat(&key, x, out);
                        }
                    }
                    other => {
                        out.insert(prefix.to_string(), other.clone());
                    }
                }
            }
            fn walk(v: &mut Value) {
                if let Value::Object(m) = v {
                    for (k, x) in m.iter_mut() {
                        if matches!(
                            k.as_str(),
                            "settings" | "persistent" | "transient" | "defaults"
                        ) && x.is_object()
                        {
                            let mut out = serde_json::Map::new();
                            if let Value::Object(inner) = &*x {
                                for (ik, iv) in inner {
                                    flat(ik, iv, &mut out);
                                }
                            }
                            *x = Value::Object(out);
                        } else {
                            walk(x);
                        }
                    }
                } else if let Value::Array(a) = v {
                    a.iter_mut().for_each(walk);
                }
            }
            walk(&mut resp);
        }
        // `filter_path`: keep (or with `-`, drop) the named parts.
        if status < 400
            && let Some(fp) = query.split('&').find_map(|p| p.strip_prefix("filter_path="))
        {
            let fp = percent_decode_segment(&fp.replace('+', " "));
            let (mut inc, mut exc) = (vec![], vec![]);
            for p in fp.split(',').map(str::trim).filter(|p| !p.is_empty()) {
                match p.strip_prefix('-') {
                    Some(e) => exc.push(e.split('.').map(String::from).collect::<Vec<_>>()),
                    None => inc.push(p.split('.').map(String::from).collect::<Vec<_>>()),
                }
            }
            if !inc.is_empty() {
                resp = filter_path_include(&resp, &inc).unwrap_or_else(|| json!({}));
            }
            if !exc.is_empty() {
                resp = filter_path_exclude(&resp, &exc).unwrap_or_else(|| json!({}));
            }
        }
        (status, resp)
    }

    fn route(&self, method: &str, path: &str, query: &str, body: &[u8]) -> (u16, Value) {
        let read = matches!(method, "GET" | "HEAD")
            || [
                "_search",
                "_count",
                "_msearch",
                "_mget",
                "_explain",
                "_validate",
                "_field_caps",
                "_analyze",
            ]
            .iter()
            .any(|p| path.contains(p));
        if !read {
            crate::persistence::mark("elasticsearch");
        }
        // Each path segment is percent-decoded on its own (an encoded `/`
        // inside a document id stays inside that id), as Elasticsearch
        // does: `PUT /test-%E4%B8%AD` creates the index `test-中`.
        let decoded: Vec<String> = path
            .trim_matches('/')
            .split('/')
            .filter(|s| !s.is_empty())
            .map(percent_decode_segment)
            .collect();
        let segments: Vec<&str> = decoded.iter().map(String::as_str).collect();
        let q = query_params(query);
        if let Some(r) = self.monitoring_route(method, &segments, &q, body, path) {
            return r;
        }
        if path == "/" || path.is_empty() {
            return (
                200,
                json!({"name":"noida-db","cluster_name":"docker-cluster","cluster_uuid":"noida-local","version":{"number":"8.15.3","build_flavor":"default","build_type":"docker","build_hash":"noida","build_date":"2024-09-05T00:00:00.000Z","build_snapshot":false,"lucene_version":"9.11.1","minimum_wire_compatibility_version":"7.17.0","minimum_index_compatibility_version":"7.0.0"},"tagline":"You Know, for Search"}),
            );
        }
        match segments.first() {
            Some(&"_index_template") => {
                return self.0.lock().unwrap().templates.index_api(method, &segments, &q, body);
            }
            Some(&"_component_template") => {
                return self.0.lock().unwrap().templates.component_api(method, &segments, &q, body);
            }
            Some(&"_template") => {
                return self.0.lock().unwrap().templates.legacy_api(method, &segments, &q, body);
            }
            _ => {}
        }
        if segments.first() == Some(&"_aliases") && method == "POST" {
            return self.aliases(method, body);
        }
        if matches!(segments.first(), Some(&"_alias") | Some(&"_aliases")) {
            return self.alias_api(method, "_all", segments.get(1).copied(), &q, body, true);
        }
        if segments.first() == Some(&"_search") && segments.get(1) == Some(&"scroll") {
            return self.scroll_api(method, segments.get(2).copied(), &q, body);
        }
        if segments.first() == Some(&"_pit") {
            return self.close_pit(method, body);
        }
        match segments.first() {
            Some(&"_cluster") => return self.cluster_api(method, &segments, &q, body),
            Some(&"_cat") => return self.cat_api(&segments, &q),
            _ => {}
        }
        if segments.first() == Some(&"_search") || segments.first() == Some(&"_count") {
            return self.search_or_count(method, segments[0], "*", &q, body);
        }
        if let Some(action @ ("_refresh" | "_flush" | "_forcemerge")) = segments.first().copied()
            && segments.len() == 1
            && matches!(method, "POST" | "GET")
        {
            return self.index_action("_all", action, &q);
        }
        if segments.first() == Some(&"_cache") && segments.get(1) == Some(&"clear") {
            return self.index_action("_all", "_cache", &q);
        }
        if segments.len() == 1 && segments[0] == "_field_caps" {
            return self.field_caps_api(method, "_all", &q, body);
        }
        if segments.len() == 1 && segments[0] == "_msearch" {
            return self.msearch(method, "", &q, body);
        }
        if segments.first() == Some(&"_mget") && segments.len() == 1 {
            return self.mget(method, "", &q, body);
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
        // Index-less mapping/settings APIs: every index.
        match segments.as_slice() {
            ["_mapping"] => return self.mapping_api(method, "_all", &q, body),
            ["_mapping", "field", f] => return self.field_mapping_api(method, "_all", f, &q),
            ["_settings"] => return self.settings_api(method, "_all", None, &q, body),
            ["_settings", name] => return self.settings_api(method, "_all", Some(name), &q, body),
            _ => {}
        }
        if segments.len() == 1 && segments[0].starts_with('_') {
            // `/{index}` with a name Elasticsearch can't have.
            if matches!(method, "GET" | "HEAD" | "PUT" | "DELETE") && !segments[0].contains('*') {
                let n = segments[0];
                return (
                    400,
                    json!({"error": {"root_cause": [{"type": "invalid_index_name_exception",
                        "reason": format!("Invalid index name [{n}], must not start with '_'."),
                        "index_uuid": "_na_", "index": n}],
                        "type": "invalid_index_name_exception",
                        "reason": format!("Invalid index name [{n}], must not start with '_'."),
                        "index_uuid": "_na_", "index": n}, "status": 400}),
                );
            }
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
        // Single-document ops through an alias need one target index.
        let single_doc = match segments.get(1).copied() {
            Some("_doc" | "_create" | "_update") if segments.len() == 3 => true,
            Some("_doc") => method == "POST" && segments.len() == 2,
            Some("_source" | "_explain" | "_termvectors") => segments.len() == 3,
            _ => false,
        };
        let index_name = if single_doc {
            let write = matches!(method, "PUT" | "POST" | "DELETE")
                && matches!(segments[1], "_doc" | "_create" | "_update");
            let s = self.0.lock().unwrap();
            match Self::single_target(&s, segments[0], write) {
                Ok(t) => t,
                Err(e) => return e,
            }
        } else {
            index_name
        };
        let index_name = index_name.as_str();
        if q.get("require_alias").is_some_and(|v| v.is_empty() || v == "true")
            && matches!(segments.get(1).copied(), Some("_doc" | "_create" | "_update"))
            && matches!(method, "PUT" | "POST")
            && !self.is_alias(segments[0])
        {
            return require_alias_error(segments[0]);
        }
        // Reads and writes against a closed index fail; a wildcard just
        // skips it.
        if matches!(
            segments.get(1).copied(),
            Some(
                "_search"
                    | "_count"
                    | "_doc"
                    | "_create"
                    | "_source"
                    | "_update"
                    | "_bulk"
                    | "_mget"
                    | "_msearch"
                    | "_delete_by_query"
                    | "_update_by_query"
                    | "_pit"
            )
        ) {
            let s = self.0.lock().unwrap();
            let ignore_unavailable = q.get("ignore_unavailable").is_some_and(|v| v == "true");
            let ew = q.get("expand_wildcards").map_or("open", String::as_str);
            let reach_closed = ew.split(',').any(|w| matches!(w.trim(), "closed" | "all"));
            for part in segments[0].split(',').map(str::trim) {
                if part.contains('*') && reach_closed {
                    if let Some((n, _)) =
                        s.indices.iter().find(|(n, i)| !i.opened && glob_match(part, n))
                    {
                        return index_closed(n);
                    }
                    continue;
                }
                if part.contains('*') || ignore_unavailable {
                    continue;
                }
                let target = if s.indices.contains_key(part) {
                    Some(part.to_string())
                } else {
                    s.indices
                        .iter()
                        .find(|(_, i)| i.aliases.contains_key(part))
                        .map(|(n, _)| n.clone())
                };
                if let Some(t) = target
                    && s.indices.get(&t).is_some_and(|i| !i.opened)
                {
                    return index_closed(&t);
                }
            }
        }
        if segments.len() == 1 {
            // GET / DELETE take an index expression; PUT / HEAD one name.
            if matches!(method, "GET" | "DELETE") {
                return self.index_expr_api(method, segments[0], &q);
            }
            return self.index_api(method, index_name, body);
        }
        match segments[1] {
            "_mapping" if segments.get(2) == Some(&"field") && segments.len() == 4 => {
                self.field_mapping_api(method, segments[0], segments[3], &q)
            }
            "_mapping" => self.mapping_api(method, segments[0], &q, body),
            "_settings" => {
                self.settings_api(method, segments[0], segments.get(2).copied(), &q, body)
            }
            "_refresh" | "_flush" | "_open" | "_close" | "_forcemerge"
                if method == "POST"
                    || (method == "GET" && matches!(segments[1], "_refresh" | "_flush")) =>
            {
                self.index_action(segments[0], segments[1], &q)
            }
            "_cache" if segments.get(2) == Some(&"clear") && method == "POST" => {
                self.index_action(segments[0], "_cache", &q)
            }
            "_search" | "_count" => {
                self.search_or_count(method, segments[1], segments[0], &q, body)
            }
            "_knn_search" if segments.len() == 2 => self.knn_search(method, segments[0], &q, body),
            "_pit" if method == "POST" => self.open_pit(index_name, &q),
            "_delete_by_query" | "_update_by_query" => {
                self.by_query(method, segments[1], index_name, &q, body)
            }
            "_alias" | "_aliases" => {
                self.alias_api(method, segments[0], segments.get(2).copied(), &q, body, false)
            }
            "_analyze" => self.analyze(body),
            "_doc" | "_create" | "_source" if segments.len() == 3 => {
                self.document_api(method, index_name, segments[2], segments[1], &q, body)
            }
            "_doc" if method == "POST" && segments.len() == 2 => {
                self.document_api(method, index_name, "", "_doc", &q, body)
            }
            "_bulk" => self.bulk(method, index_name, &q, body),
            "_update" if segments.len() == 3 => {
                self.update(method, index_name, segments[2], &q, body)
            }
            "_mget" => self.mget(method, index_name, &q, body),
            "_field_caps" if segments.len() == 2 => {
                self.field_caps_api(method, segments[0], &q, body)
            }
            "_msearch" if segments.len() == 2 => self.msearch(method, segments[0], &q, body),
            _ => no_handler(method, path),
        }
    }

    /// The index a single-document write to `name` goes to: the index
    /// itself, or an alias's write index (or only index).
    fn write_target(&self, name: &str) -> Result<String, (u16, Value)> {
        let s = self.0.lock().unwrap();
        Self::single_target(&s, name, true)
    }

    /// The one index a single-document op on `name` reaches: the index
    /// itself, or for an alias its write index (writes) or its only index
    /// (reads), with Elasticsearch's errors otherwise.
    fn single_target(s: &State, name: &str, write: bool) -> Result<String, (u16, Value)> {
        if s.indices.contains_key(name) {
            return Ok(name.to_string());
        }
        let mut with: Vec<(&String, &Index)> =
            s.indices.iter().filter(|(_, i)| i.aliases.contains_key(name)).collect();
        if with.is_empty() {
            return Ok(name.to_string());
        }
        with.sort_by(|a, b| a.0.cmp(b.0));
        let flag = |i: &Index| i.aliases[name].get("is_write_index").and_then(Value::as_bool);
        if write {
            if let Some((n, _)) = with.iter().find(|(_, i)| flag(i) == Some(true)) {
                return Ok(n.to_string());
            }
            if with.len() == 1 && flag(with[0].1) != Some(false) {
                return Ok(with[0].0.to_string());
            }
            return Err((
                400,
                error(
                    "illegal_argument_exception",
                    &format!(
                        "no write index is defined for alias [{name}]. The write index may be explicitly disabled using is_write_index=false or the alias points to multiple indices without one being designated as a write index"
                    ),
                    400,
                ),
            ));
        }
        if with.len() == 1 {
            return Ok(with[0].0.to_string());
        }
        let list = with.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>().join(", ");
        Err((
            400,
            error(
                "illegal_argument_exception",
                &format!(
                    "alias [{name}] has more than one index associated with it [{list}], can't execute a single index op"
                ),
                400,
            ),
        ))
    }

    /// `GET|POST [/<index>]/_field_caps?fields=...`.
    fn field_caps_api(
        &self,
        method: &str,
        expr: &str,
        q: &HashMap<String, String>,
        body: &[u8],
    ) -> (u16, Value) {
        if !matches!(method, "GET" | "POST") {
            return (405, error("method_not_allowed_exception", "Incorrect HTTP method", 405));
        }
        let Some(req) = parse_json(body) else { return (400, malformed_body()) };
        if q.get("fields").is_none_or(|f| f.is_empty()) && req.get("fields").is_none() {
            return (
                400,
                error(
                    "action_request_validation_exception",
                    "Validation Failed: 1: no fields specified;",
                    400,
                ),
            );
        }
        if let Err(e) = super::field_caps::check_filters(q) {
            return (400, error("illegal_argument_exception", &e, 400));
        }
        let s = self.0.lock().unwrap();
        let names = match Self::resolve_targets(&s, expr, q) {
            Ok(n) => n,
            Err(e) => return e,
        };
        let mut indices: Vec<(String, Value, Vec<Value>)> = Vec::new();
        for n in &names {
            let i = &s.indices[n];
            // `index_filter`: indices where the query can't match any
            // document are left out.
            if let Some(f) = req.get("index_filter")
                && !index_can_match(i, f)
            {
                continue;
            }
            let sources = i.docs.values().map(|d| d.source.clone()).collect();
            indices.push((n.clone(), i.mappings.clone(), sources));
        }
        (200, super::field_caps::field_caps(&indices, q, &req))
    }

    /// `POST [/<index>]/_msearch`: header/body line pairs, each run as a
    /// search; a failing search is an error entry, not a failed request.
    fn msearch(
        &self,
        method: &str,
        index: &str,
        q: &HashMap<String, String>,
        body: &[u8],
    ) -> (u16, Value) {
        if !matches!(method, "GET" | "POST") {
            return (405, error("method_not_allowed_exception", "Incorrect HTTP method", 405));
        }
        let text = String::from_utf8_lossy(body);
        let lines: Vec<&str> = text.lines().filter(|l| !l.trim().is_empty()).collect();
        if lines.is_empty() {
            return (
                400,
                error("parse_exception", "request body or source parameter is required", 400),
            );
        }
        if q.get("rest_total_hits_as_int").is_some_and(|v| v == "true") {
            for pair in lines.chunks(2) {
                if let Some(Ok(b)) = pair.get(1).map(|l| serde_json::from_str::<Value>(l))
                    && let Some(err) = total_hits_as_int_error(&b)
                {
                    return err;
                }
            }
        }
        let mut responses = Vec::new();
        for pair in lines.chunks(2) {
            let Ok(header) = serde_json::from_str::<Value>(pair[0]) else {
                return (400, malformed_body());
            };
            let target = match header.get("index") {
                Some(Value::String(s)) => s.clone(),
                Some(Value::Array(a)) => {
                    a.iter().filter_map(Value::as_str).collect::<Vec<_>>().join(",")
                }
                _ if !index.is_empty() => index.to_string(),
                _ => "_all".to_string(),
            };
            let mut item_q = q.clone();
            if let Some(h) = header.as_object() {
                for (k, v) in h {
                    if k != "index" {
                        item_q.insert(
                            k.clone(),
                            v.as_str().map(str::to_string).unwrap_or_else(|| v.to_string()),
                        );
                    }
                }
            }
            let data = pair.get(1).copied().unwrap_or("{}");
            let (status, mut resp) =
                self.search_or_count("POST", "_search", &target, &item_q, data.as_bytes());
            if status >= 400 {
                let mut e = json!({"error": resp["error"].clone(), "status": status});
                if e["error"].is_null() {
                    e["error"] = resp;
                }
                responses.push(e);
            } else {
                resp["status"] = json!(status);
                responses.push(resp);
            }
        }
        (200, json!({"took": 0, "responses": responses}))
    }

    /// For each index an expression reaches: `None` when reached
    /// directly (or through an alias without a filter), else the filters
    /// of the filtered aliases it was reached through.
    fn alias_filters(s: &State, expr: &str) -> HashMap<String, Option<Vec<Value>>> {
        let mut out: HashMap<String, Option<Vec<Value>>> = HashMap::new();
        for part in expr.split(',').map(str::trim).filter(|p| !p.is_empty() && !p.starts_with('-'))
        {
            for (n, i) in &s.indices {
                if part == "_all" || part == "*" || glob_match(part, n) {
                    out.insert(n.clone(), None);
                    continue;
                }
                for (a, spec) in &i.aliases {
                    if !glob_match(part, a) {
                        continue;
                    }
                    match spec.get("filter") {
                        Some(f) => {
                            if let Some(list) =
                                out.entry(n.clone()).or_insert_with(|| Some(Vec::new()))
                            {
                                list.push(f.clone());
                            }
                        }
                        None => {
                            out.insert(n.clone(), None);
                        }
                    }
                }
            }
        }
        out
    }

    fn is_alias(&self, name: &str) -> bool {
        self.0.lock().unwrap().indices.values().any(|i| i.aliases.contains_key(name))
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
        let mut names: Vec<String> = Vec::new();
        for (n, part) in pattern.split(',').map(str::trim).enumerate() {
            if part.is_empty() {
                continue;
            }
            // `-name` / `-pat*` after an earlier expression excludes.
            if n > 0
                && let Some(ex) = part.strip_prefix('-')
            {
                names.retain(|k| !glob_match(ex, k));
                continue;
            }
            if part == "_all" || part == "*" {
                names.extend(s.indices.keys().cloned());
            } else if part.contains('*') {
                names.extend(s.indices.keys().filter(|k| glob_match(part, k)).cloned());
                names.extend(
                    s.indices
                        .iter()
                        .filter(|(_, i)| i.aliases.keys().any(|a| glob_match(part, a)))
                        .map(|(name, _)| name.clone()),
                );
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

    /// An index expression for an index-level admin action (`_refresh`,
    /// `_flush`, `_forcemerge`, `_cache/clear`, `_open`, `_close`): like
    /// `resolve_indices`, but a concrete name that matches nothing is a
    /// 404 `index_not_found_exception` unless `ignore_unavailable=true`,
    /// and `allow_no_indices=false` refuses an expression matching none.
    fn resolve_targets(
        s: &State,
        expr: &str,
        q: &HashMap<String, String>,
    ) -> Result<Vec<String>, (u16, Value)> {
        let ignore_unavailable = q.get("ignore_unavailable").is_some_and(|v| v == "true");
        // `expand_wildcards=none`: a wildcard is a literal (missing) name.
        let no_expand = q.get("expand_wildcards").is_some_and(|v| v == "none");
        if !ignore_unavailable {
            for (n, part) in expr.split(',').map(str::trim).enumerate() {
                let excluded = n > 0 && part.starts_with('-');
                if part.is_empty() || excluded || part == "_all" {
                    continue;
                }
                if part.contains('*') {
                    if no_expand {
                        return Err(missing_index(part));
                    }
                    continue;
                }
                if Self::resolve_indices(s, part).is_empty() {
                    return Err(missing_index(part));
                }
            }
        }
        let names = if no_expand {
            let literal: Vec<&str> = expr
                .split(',')
                .map(str::trim)
                .filter(|p| !p.contains('*') && *p != "_all")
                .collect();
            if literal.is_empty() { vec![] } else { Self::resolve_indices(s, &literal.join(",")) }
        } else {
            Self::resolve_indices(s, expr)
        };
        // `expand_wildcards` (default `open`) picks which indices a
        // wildcard reaches; named indices are always kept.
        let ew = q.get("expand_wildcards").map_or("open", String::as_str);
        let want_open = ew.split(',').any(|w| matches!(w.trim(), "open" | "all"));
        let want_closed = ew.split(',').any(|w| matches!(w.trim(), "closed" | "all"));
        let literal: Vec<&str> = expr
            .split(',')
            .map(str::trim)
            .filter(|p| !p.contains('*') && *p != "_all" && !p.starts_with('-'))
            .collect();
        let named =
            if literal.is_empty() { vec![] } else { Self::resolve_indices(s, &literal.join(",")) };
        let names: Vec<String> = names
            .into_iter()
            .filter(|n| {
                named.contains(n)
                    || s.indices
                        .get(n)
                        .is_none_or(|i| if i.opened { want_open } else { want_closed })
            })
            .collect();
        if names.is_empty() && q.get("allow_no_indices").is_some_and(|v| v == "false") {
            return Err(missing_index(expr));
        }
        Ok(names)
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
        // Other body options a client may pass as URL parameters instead.
        for key in [
            "track_total_hits",
            "version",
            "seq_no_primary_term",
            "explain",
            "terminate_after",
            "track_scores",
            "timeout",
            "min_score",
        ] {
            if let Some(v) = q.get(key)
                && req.get(key).is_none()
            {
                req[key] = match v.as_str() {
                    "true" => json!(true),
                    "false" => json!(false),
                    other => other
                        .parse::<i64>()
                        .map(|n| json!(n))
                        .or_else(|_| other.parse::<f64>().map(|f| json!(f)))
                        .unwrap_or_else(|_| json!(other)),
                };
            }
        }
        for key in ["stored_fields", "docvalue_fields"] {
            if let Some(v) = q.get(key)
                && req.get(key).is_none()
            {
                req[key] = json!(v.split(',').collect::<Vec<_>>());
            }
        }
        if q.get("rest_total_hits_as_int").is_some_and(|v| v == "true")
            && let Some(err) = total_hits_as_int_error(&req)
        {
            return err;
        }
        if q.get("rest_total_hits_as_int").is_some_and(|v| v == "true")
            && req.get("track_total_hits").is_none()
        {
            req["track_total_hits"] = json!(true);
        }
        // URI search: `?q=title:quick`, a query_string query.
        if let Some(text) = q.get("q") {
            let mut qs = json!({"query": text});
            for (param, key) in [
                ("df", "default_field"),
                ("default_operator", "default_operator"),
                ("analyze_wildcard", "analyze_wildcard"),
                ("lenient", "lenient"),
            ] {
                if let Some(v) = q.get(param) {
                    qs[key] = match v.as_str() {
                        "true" => json!(true),
                        "false" => json!(false),
                        other => json!(other),
                    };
                }
            }
            req["query"] = json!({"query_string": qs});
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
        let scroll_param = q
            .get("scroll")
            .cloned()
            .or_else(|| req.get("scroll").and_then(Value::as_str).map(str::to_string));
        if q.contains_key("search_type") && req.get("knn").is_some() {
            return (
                400,
                error(
                    "illegal_argument_exception",
                    "cannot set [search_type] when using [knn] search, since the search type is \
                     determined automatically",
                    400,
                ),
            );
        }
        let mut s = self.0.lock().unwrap();
        if let Err(e) =
            validate_scroll_and_slice(&s, index_pattern, &req, q, scroll_param.as_deref())
        {
            return e;
        }
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
            let (mappings, docs) = (mappings.clone(), docs.clone());
            let opts = search::SearchOptions { typed: true, pit: true, ..Default::default() };
            let docs = sliced(&s, docs, &req);
            return match search::search_with(&mappings, &docs, &req, &opts) {
                Ok(mut resp) => {
                    resp["pit_id"] = json!(id);
                    (200, resp)
                }
                Err(e) => (e.status, e.to_json()),
            };
        }
        let is_wildcard = index_pattern == "_all" || index_pattern == "*";
        let names = Self::resolve_indices(&s, index_pattern);
        for n in &names {
            if let Some(i) = s.indices.get_mut(n) {
                i.auto_refresh(n);
            }
        }
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
            // A count body takes only `query`.
            if let Some(bad) = parse_json(body)
                .and_then(|b| b.as_object().and_then(|m| m.keys().find(|k| *k != "query").cloned()))
            {
                return (
                    400,
                    error("parsing_exception", &format!("request does not support [{bad}]"), 400),
                );
            }
            if let Some(m) = q.get("min_score").and_then(|v| v.parse::<f64>().ok()) {
                req["min_score"] = json!(m);
            }
            let filters = Self::alias_filters(&s, index_pattern);
            let counts: Result<Vec<u64>, search::EsError> = names
                .iter()
                .filter_map(|n| s.indices.get(n).map(|i| (n, i)))
                .map(|(n, i)| {
                    let docs = filtered_docs(i, filters.get(n))?;
                    search::count(&i.mappings, &docs, &req)
                })
                .collect();
            let total: u64 = match counts {
                Ok(cs) => cs.into_iter().sum(),
                Err(e) => return (e.status, e.to_json()),
            };
            let shards: u64 = names
                .iter()
                .filter_map(|n| s.indices.get(n))
                .map(|i| shard_counts(i).0.max(1))
                .sum::<u64>()
                .max(1);
            return (
                200,
                json!({"count": total, "_shards": {"total": shards, "successful": shards, "skipped": 0, "failed": 0}}),
            );
        }
        // A query spanning more than one index may mix mappings, so fields
        // fall back to runtime type inference rather than any one index's
        // explicit mapping (see `search::tokens_for`).
        let filters = Self::alias_filters(&s, index_pattern);
        let (mappings, docs, typed): (Value, Vec<CommittedDoc>, bool) = if names.len() == 1 {
            let i = &s.indices[&names[0]];
            let docs = match filtered_docs(i, filters.get(&names[0])) {
                Ok(d) => d,
                Err(e) => return (e.status, e.to_json()),
            };
            (i.mappings.clone(), docs, true)
        } else {
            // Several indices: their mappings merged (the first index to
            // map a field wins), so typed fields still sort and range as
            // their type.
            let mut docs = Vec::new();
            let mut props = Map::new();
            for n in &names {
                match filtered_docs(&s.indices[n], filters.get(n)) {
                    Ok(d) => docs.extend(d),
                    Err(e) => return (e.status, e.to_json()),
                }
                if let Some(p) = s.indices[n].mappings.get("properties").and_then(Value::as_object)
                {
                    for (k, v) in p {
                        props.entry(k.clone()).or_insert_with(|| v.clone());
                    }
                }
            }
            (json!({"properties": props}), docs, !names.is_empty())
        };
        // Every primary shard of the targets is searched.
        let shard_total: u64 =
            names.iter().filter_map(|n| s.indices.get(n)).map(|i| shard_counts(i).0.max(1)).sum();
        let scroll = scroll_param;
        if let Some(scroll) = scroll {
            let docs = sliced(&s, docs, &req);
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
            set_search_shards(&mut resp, shard_total);
            return (200, resp);
        }
        let settings =
            if names.len() == 1 { s.indices[&names[0]].settings.clone() } else { Value::Null };
        let opts = search::SearchOptions { typed, settings, ..Default::default() };
        match search::search_with(&mappings, &docs, &req, &opts) {
            Ok(mut resp) => {
                if q.get("typed_keys").is_some_and(|v| v == "true") {
                    suggest::type_keys(&req, &mut resp);
                }
                set_search_shards(&mut resp, shard_total);
                (200, resp)
            }
            Err(e) => (e.status, e.to_json()),
        }
    }

    /// `/<index>/_knn_search` (deprecated since 8.4 for the search API's
    /// `knn` option): a kNN search with its own top-level `filter`,
    /// returning `k` hits.
    fn knn_search(
        &self,
        method: &str,
        index: &str,
        q: &HashMap<String, String>,
        body: &[u8],
    ) -> (u16, Value) {
        let Some(mut req) = parse_json(body) else {
            return (400, malformed_body());
        };
        if let Some(f) = req.as_object_mut().and_then(|o| o.remove("filter"))
            && req.get("knn").is_some_and(Value::is_object)
        {
            req["knn"]["filter"] = f;
        }
        let k = req.get("knn").and_then(|k| k.get("k")).and_then(Value::as_i64).unwrap_or(10);
        req["size"] = json!(k);
        let body = serde_json::to_vec(&req).unwrap_or_default();
        self.search_or_count(method, "_search", index, q, &body)
    }

    /// Cluster health numbers over `names` (see `cluster::health_body`).
    fn health_of(s: &State, names: &[String]) -> Value {
        cluster::health_body(s, names)
    }

    /// `_cluster/settings` (the other `_cluster` APIs are in
    /// `cluster.rs`).
    fn cluster_api(
        &self,
        method: &str,
        segments: &[&str],
        _q: &HashMap<String, String>,
        body: &[u8],
    ) -> (u16, Value) {
        let mut s = self.0.lock().unwrap();
        match segments.get(1).copied() {
            Some("settings") => {
                if method == "PUT" {
                    let Some(req) = parse_json(body) else { return (400, malformed_body()) };
                    for key in ["persistent", "transient"] {
                        if let Some(Value::Object(m)) = req.get(key) {
                            let slot = s
                                .cluster_settings
                                .entry(key.to_string())
                                .or_insert_with(|| json!({}));
                            // Stored flat (`a.b.c`), values as strings.
                            let mut flat = Vec::new();
                            flatten_keys("", &Value::Object(m.clone()), &mut flat);
                            for (k, v) in &flat {
                                let v = match v {
                                    Value::Null | Value::String(_) | Value::Array(_) => v.clone(),
                                    other => json!(other.to_string()),
                                };
                                if v.is_null() {
                                    slot.as_object_mut().unwrap().remove(k);
                                } else {
                                    slot[k.as_str()] = v;
                                }
                            }
                        }
                    }
                    let mut out = json!({"acknowledged": true, "persistent": {}, "transient": {}});
                    for key in ["persistent", "transient"] {
                        if let Some(v) = req.get(key) {
                            let mut flat = Vec::new();
                            flatten_keys("", v, &mut flat);
                            let kept: Map<String, Value> = flat
                                .into_iter()
                                .filter(|(_, x)| !x.is_null())
                                .map(|(k, x)| {
                                    let x = if x.is_string() || x.is_array() {
                                        x
                                    } else {
                                        json!(x.to_string())
                                    };
                                    (k, x)
                                })
                                .collect();
                            out[key] = nest_keys(&kept);
                        }
                    }
                    return (200, out);
                }
                (
                    200,
                    json!({
                        "persistent": s.cluster_settings.get("persistent").and_then(Value::as_object).map(nest_keys).unwrap_or_else(|| json!({})),
                        "transient": s.cluster_settings.get("transient").and_then(Value::as_object).map(nest_keys).unwrap_or_else(|| json!({})),
                    }),
                )
            }
            _ => no_handler(method, &format!("/{}", segments.join("/"))),
        }
    }

    fn cat_api(&self, segments: &[&str], q: &HashMap<String, String>) -> (u16, Value) {
        let mut s = self.0.lock().unwrap();
        let pattern = segments.get(2).copied().unwrap_or("*");
        let names = {
            let mut n = Self::resolve_indices(&s, pattern);
            n.sort();
            n
        };
        if segments.len() > 2
            && names.is_empty()
            && !pattern.contains('*')
            && !matches!(segments[1], "templates" | "aliases")
        {
            return missing_index(pattern);
        }
        for n in &names {
            if let Some(i) = s.indices.get_mut(n) {
                i.auto_refresh(n);
            }
        }
        match segments.get(1).copied() {
            Some("aliases") => {
                let mut rows = Vec::new();
                let mut idx: Vec<&String> = s.indices.keys().collect();
                idx.sort();
                let patterns: Vec<&str> = segments
                    .get(2)
                    .map(|n| n.split(',').map(str::trim).collect())
                    .unwrap_or_default();
                for n in idx {
                    let mut aliases: Vec<(&String, &Value)> = s.indices[n].aliases.iter().collect();
                    aliases.sort_by_key(|a| a.0);
                    for (a, spec) in aliases {
                        if !alias_selected(&patterns, a) {
                            continue;
                        }
                        let w = spec
                            .get("is_write_index")
                            .and_then(Value::as_bool)
                            .map_or("-".to_string(), |b| b.to_string());
                        let f = if spec.get("filter").is_some() { "*" } else { "-" };
                        let r = |k: &str| {
                            spec.get(k).and_then(Value::as_str).unwrap_or("-").to_string()
                        };
                        rows.push(vec![
                            a.clone(),
                            n.clone(),
                            f.into(),
                            r("index_routing"),
                            r("search_routing"),
                            w,
                        ]);
                    }
                }
                (
                    200,
                    cat::render(
                        &[
                            "alias",
                            "index",
                            "filter",
                            "routing.index",
                            "routing.search",
                            "is_write_index",
                        ],
                        &[],
                        rows,
                        q,
                    ),
                )
            }
            Some("templates") => {
                let rows = match s.templates.cat_rows(segments.get(2).copied()) {
                    Ok(r) => r,
                    Err(e) => return e,
                };
                (
                    200,
                    cat::render(
                        &["name", "index_patterns", "order", "version", "composed_of"],
                        &["order", "version"],
                        rows,
                        q,
                    ),
                )
            }
            _ => no_handler("GET", &format!("/{}", segments.join("/"))),
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
        // A body `scroll_id` wins over the URL's.
        let ids: Vec<String> = match (req.get("scroll_id"), path_id, q.get("scroll_id")) {
            (Some(Value::String(i)), _, _) => i.split(',').map(str::to_string).collect(),
            (Some(Value::Array(a)), _, _) => {
                a.iter().filter_map(Value::as_str).map(str::to_string).collect()
            }
            (_, Some(p), _) => p.split(',').map(str::to_string).collect(),
            (_, _, Some(i)) => i.split(',').map(str::to_string).collect(),
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
        if let Some(k) = keep {
            let max_keep = ["transient", "persistent"]
                .iter()
                .find_map(|c| {
                    s.cluster_settings
                        .get(*c)
                        .and_then(|m| m.get("search.max_keep_alive"))
                        .and_then(Value::as_str)
                        .and_then(parse_keep_alive)
                })
                .unwrap_or(std::time::Duration::from_secs(86_400));
            if k > max_keep {
                let e = search::EsError::shard_failure(
                    "illegal_argument_exception",
                    &format!(
                        "Keep alive for request ({}) is too large. It must be less than ({}). This limit can be set by changing the [search.max_keep_alive] cluster level setting.",
                        time_value(k),
                        time_value(max_keep)
                    ),
                );
                return (400, e.to_json());
            }
        }
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
        for n in &names {
            if let Some(i) = s.indices.get_mut(n) {
                i.auto_refresh(n);
            }
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
                    let mut m = expand_dotted(m);
                    if let Err(e) = validate_mapping(&json!({}), &m) {
                        return e;
                    }
                    if let Err(e) = vectors::prepare_mapping(&index.mappings, &mut m) {
                        return e;
                    }
                    merge(&mut index.mappings, m);
                    suggest::normalize_mappings(&mut index.mappings);
                }
                if let Some(st) = req.get("settings") {
                    if let Err(e) = translog_retention_check(st) {
                        return e;
                    }
                    apply_settings(&mut index.settings, st);
                }
                if let Some(a) = req.get("aliases").and_then(Value::as_object) {
                    index.aliases.extend(a.iter().map(|(k, v)| (k.clone(), normalize_alias(v))));
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

    /// `GET|PUT [/{index-expr}]/_mapping`.
    fn mapping_api(
        &self,
        method: &str,
        expr: &str,
        q: &HashMap<String, String>,
        body: &[u8],
    ) -> (u16, Value) {
        let mut s = self.0.lock().unwrap();
        let names = match Self::resolve_targets(&s, expr, q) {
            Ok(n) => n,
            Err(e) => return e,
        };
        match method {
            // `{"<index>": {"mappings": {...}}}`, as Elasticsearch shapes it.
            "GET" => {
                let mut out = Map::new();
                for n in &names {
                    if let Some(i) = s.indices.get(n) {
                        out.insert(n.clone(), json!({"mappings": shown_mappings(&i.mappings)}));
                    }
                }
                (200, Value::Object(out))
            }
            "PUT" | "POST" => {
                let next = parse_json(body).unwrap_or_else(|| json!({}));
                if next.get("_doc").is_some() {
                    return (
                        400,
                        error(
                            "illegal_argument_exception",
                            "Types cannot be provided in put mapping requests",
                            400,
                        ),
                    );
                }
                let mut next = expand_dotted(&next);
                for n in &names {
                    if let Some(i) = s.indices.get(n) {
                        if let Err(e) = validate_mapping(&i.mappings, &next) {
                            return e;
                        }
                        if let Err(e) = vectors::prepare_mapping(&i.mappings, &mut next.clone()) {
                            return e;
                        }
                    }
                }
                if let Err(e) = vectors::prepare_mapping(&json!({}), &mut next) {
                    return e;
                }
                for n in &names {
                    if let Some(i) = s.indices.get_mut(n) {
                        merge(&mut i.mappings, next.clone());
                        suggest::normalize_mappings(&mut i.mappings);
                    }
                }
                (200, json!({"acknowledged":true}))
            }
            _ => (405, error("method_not_allowed_exception", "Incorrect HTTP method", 405)),
        }
    }

    /// `GET [/{index-expr}]/_mapping/field/{fields}`: each field's
    /// definition by full name (wildcards allowed).
    fn field_mapping_api(
        &self,
        method: &str,
        expr: &str,
        fields: &str,
        q: &HashMap<String, String>,
    ) -> (u16, Value) {
        if method != "GET" {
            return (405, error("method_not_allowed_exception", "Incorrect HTTP method", 405));
        }
        // `local` was removed from this API in 8.0.
        if q.contains_key("local") {
            return (
                400,
                error(
                    "illegal_argument_exception",
                    "request [/_mapping/field] contains unrecognized parameter: [local]",
                    400,
                ),
            );
        }
        let s = self.0.lock().unwrap();
        let names = match Self::resolve_targets(&s, expr, q) {
            Ok(n) => n,
            Err(e) => return e,
        };
        let patterns: Vec<&str> =
            fields.split(',').map(str::trim).filter(|p| !p.is_empty()).collect();
        let mut out = Map::new();
        for n in &names {
            let Some(i) = s.indices.get(n) else { continue };
            let mut leaves = vec![];
            mapped_leaves(&i.mappings, "", &mut leaves);
            let mut m = Map::new();
            for (full, def) in leaves {
                if patterns.iter().any(|p| glob_match(p, &full)) {
                    let leaf = full.rsplit('.').next().unwrap_or(&full).to_string();
                    m.insert(full.clone(), json!({"full_name": full, "mapping": {(leaf): def}}));
                }
            }
            for meta in META_FIELDS {
                if patterns.iter().any(|p| p.contains('*') && glob_match(p, meta)) {
                    m.insert(meta.to_string(), json!({"full_name": meta, "mapping": {}}));
                }
            }
            out.insert(n.clone(), json!({"mappings": m}));
        }
        (200, Value::Object(out))
    }

    /// `GET|PUT [/{index-expr}]/_settings[/{names}]`.
    fn settings_api(
        &self,
        method: &str,
        expr: &str,
        filter: Option<&str>,
        q: &HashMap<String, String>,
        body: &[u8],
    ) -> (u16, Value) {
        let mut s = self.0.lock().unwrap();
        let q = &with_default_expand(q, "open");
        let names = match Self::resolve_targets(&s, expr, q) {
            Ok(n) => n,
            Err(e) => return e,
        };
        match method {
            "GET" => {
                let patterns: Option<Vec<String>> = filter.filter(|f| *f != "_all").map(|f| {
                    f.split(',').map(|p| p.trim().to_string()).filter(|p| !p.is_empty()).collect()
                });
                let defaults =
                    q.get("include_defaults").is_some_and(|v| v.is_empty() || v == "true");
                let mut out = Map::new();
                for n in &names {
                    let Some(i) = s.indices.get(n) else { continue };
                    let flat = flat_settings(&i.settings);
                    let keep = |k: &str| {
                        patterns.as_ref().is_none_or(|ps| ps.iter().any(|p| glob_match(p, k)))
                    };
                    let shown: Vec<(String, Value)> =
                        flat.iter().filter(|(k, _)| keep(k)).cloned().collect();
                    let mut entry = Map::new();
                    if !shown.is_empty() {
                        entry.insert("settings".into(), nest_settings(&shown));
                    }
                    if defaults {
                        let rest: Vec<(String, Value)> = DEFAULT_SETTINGS
                            .iter()
                            .filter(|(k, _)| keep(k) && !flat.iter().any(|(f, _)| f == k))
                            .map(|(k, v)| (k.to_string(), json!(v)))
                            .collect();
                        entry.insert("defaults".into(), nest_settings(&rest));
                        entry.entry("settings").or_insert_with(|| json!({}));
                    }
                    if !entry.is_empty() {
                        out.insert(n.clone(), Value::Object(entry));
                    }
                }
                (200, Value::Object(out))
            }
            "PUT" => {
                let Some(req) = parse_json(body) else { return (400, malformed_body()) };
                let req = match req.get("settings") {
                    Some(inner) if req.as_object().is_some_and(|m| m.len() == 1) => inner.clone(),
                    _ => req,
                };
                if let Err(e) = validate_settings(&req, true) {
                    return e;
                }
                let preserve = q.get("preserve_existing").is_some_and(|v| v == "true");
                for n in &names {
                    if let Some(i) = s.indices.get_mut(n) {
                        let req = if preserve {
                            let have = flat_settings(&i.settings);
                            let fresh: Vec<(String, Value)> = flat_settings(&nested_incoming(&req))
                                .into_iter()
                                .filter(|(k, _)| !have.iter().any(|(h, _)| h == k))
                                .collect();
                            nest_settings(&fresh)
                        } else {
                            req.clone()
                        };
                        apply_settings(&mut i.settings, &req);
                    }
                }
                (200, json!({"acknowledged":true}))
            }
            _ => (405, error("method_not_allowed_exception", "Incorrect HTTP method", 405)),
        }
    }

    /// `GET /{index-expr}` and `DELETE /{index-expr}`.
    fn index_expr_api(
        &self,
        method: &str,
        expr: &str,
        q: &HashMap<String, String>,
    ) -> (u16, Value) {
        let mut s = self.0.lock().unwrap();
        if method == "DELETE" {
            if let Err(e) = destructive_check(&s, expr) {
                return e;
            }
            // Deleting through an alias is refused, wildcards excepted
            // (with `ignore_unavailable` the alias is just skipped).
            let ignore_unavailable = q.get("ignore_unavailable").is_some_and(|v| v == "true");
            for part in expr.split(',').map(str::trim) {
                if !ignore_unavailable
                    && !part.contains('*')
                    && !s.indices.contains_key(part)
                    && s.indices.values().any(|i| i.aliases.contains_key(part))
                {
                    return (
                        400,
                        error(
                            "illegal_argument_exception",
                            &format!(
                                "The provided expression [{part}] matches an alias, specify the corresponding concrete indices instead."
                            ),
                            400,
                        ),
                    );
                }
            }
        }
        let q = &with_default_expand(q, if method == "DELETE" { "open,closed" } else { "open" });
        let names = match Self::resolve_targets(&s, expr, q) {
            Ok(n) => n,
            Err(e) => return e,
        };
        // A wildcard delete only removes indices the pattern names, never
        // those reached through an alias.
        let names: Vec<String> = if method == "DELETE" {
            names
                .into_iter()
                .filter(|n| {
                    expr.split(',')
                        .map(str::trim)
                        .any(|p| p == n || p == "_all" || glob_match(p, n))
                })
                .collect()
        } else {
            names
        };
        if method == "DELETE" {
            // With `allow_no_indices=false` every wildcard must match an
            // index by name.
            if q.get("allow_no_indices").is_some_and(|v| v == "false") {
                for part in expr.split(',').map(str::trim).filter(|p| p.contains('*')) {
                    if !s.indices.keys().any(|n| glob_match(part, n)) {
                        return missing_index(part);
                    }
                }
                if names.is_empty() {
                    return missing_index(expr);
                }
            }
            for n in &names {
                s.indices.remove(n);
            }
            return (200, json!({"acknowledged":true}));
        }
        // `features` picks which sections are filled in.
        let features = q.get("features").map_or("aliases,mappings,settings", String::as_str);
        let has = |f: &str| features.split(',').any(|x| x.trim() == f);
        let human = q.get("human").is_some_and(|v| v.is_empty() || v == "true");
        let mut out = Map::new();
        for n in &names {
            if let Some(i) = s.indices.get(n) {
                let mut settings = i.settings.clone();
                if human {
                    add_human_settings(&mut settings);
                }
                out.insert(
                    n.clone(),
                    json!({
                        "aliases": if has("aliases") { json!(i.aliases) } else { json!({}) },
                        "mappings": if has("mappings") { shown_mappings(&i.mappings) } else { json!({}) },
                        "settings": if has("settings") { settings } else { json!({}) },
                    }),
                );
            }
        }
        (200, Value::Object(out))
    }

    /// Index-level admin actions over an index expression (one name, a
    /// list, wildcards, `_all`, or none for every index).
    fn index_action(&self, expr: &str, action: &str, q: &HashMap<String, String>) -> (u16, Value) {
        let mut s = self.0.lock().unwrap();
        if matches!(action, "_open" | "_close")
            && let Err(e) = destructive_check(&s, expr)
        {
            return e;
        }
        // Opening reaches closed indices by default; the rest open ones.
        let q = &with_default_expand(q, if action == "_open" { "closed" } else { "open" });
        let names = match Self::resolve_targets(&s, expr, q) {
            Ok(n) => n,
            Err(e) => return e,
        };
        let (mut total, mut ok) = (0, 0);
        for n in &names {
            if let Some(i) = s.indices.get(n) {
                let (p, r) = shard_counts(i);
                total += p * (1 + r);
                ok += p;
            }
        }
        let shards = json!({"_shards":{"total":total,"successful":ok,"failed":0}});
        match action {
            "_refresh" => {
                for n in &names {
                    if let Some(i) = s.indices.get_mut(n) {
                        i.refresh(n);
                    }
                }
                (200, shards)
            }
            "_open" | "_close" => {
                let open = action == "_open";
                let mut out = serde_json::Map::new();
                for n in &names {
                    if let Some(i) = s.indices.get_mut(n) {
                        i.opened = open;
                        out.insert(n.clone(), json!({"closed": true}));
                    }
                }
                if open {
                    (200, json!({"acknowledged":true,"shards_acknowledged":true}))
                } else {
                    (200, json!({"acknowledged":true,"shards_acknowledged":true,"indices":out}))
                }
            }
            // A force merge leaves the merged segments searchable.
            "_forcemerge" => {
                for n in &names {
                    if let Some(i) = s.indices.get_mut(n) {
                        i.refresh(n);
                    }
                }
                (200, shards)
            }
            // `_flush`, `_cache/clear`: nothing to do in memory beyond
            // acknowledging every shard.
            _ => (200, shards),
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
        if matches!(method, "PUT" | "POST")
            && let Err(e) = validate_write(id, kind, q)
        {
            return e;
        }
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
        if kind == "_source" && matches!(method, "GET" | "HEAD") {
            let (status, doc) = get_doc(i, index, id, &GetOpts::from_params(q));
            if status != 200 {
                return (status, if status == 404 { missing_doc(index, id).1 } else { doc });
            }
            return match doc.get("_source") {
                Some(src) => (200, src.clone()),
                None => (
                    404,
                    error(
                        "resource_not_found_exception",
                        &format!("Source not found [{index}]/[{id}]"),
                        404,
                    ),
                ),
            };
        }
        if id.is_empty() && method != "POST" {
            return (405, error("method_not_allowed_exception", "Incorrect HTTP method", 405));
        }
        let id = if id.is_empty() { auto_id() } else { id.to_string() };
        let shards = shards_header(i);
        if matches!(method, "GET" | "HEAD")
            && q.get("refresh").is_some_and(|v| v.is_empty() || v == "true")
        {
            i.refresh(index);
        }
        match method {
            "GET" | "HEAD" => {
                let (status, doc) = get_doc(i, index, &id, &GetOpts::from_params(q));
                if method == "HEAD" && status == 404 { (404, json!({})) } else { (status, doc) }
            }
            "PUT" | "POST" => {
                if q.get("routing").is_none() && routing_required(i) {
                    return routing_missing(index, &id);
                }
                let create = kind == "_create" || q.get("op_type").is_some_and(|o| o == "create");
                if create && i.docs.contains_key(&id) {
                    return (409, version_conflict(index, &id, i.docs[&id].version));
                }
                if let Err(e) = check_seq_no(i.docs.get(&id), &id, q) {
                    return e;
                }
                let external = external_version(q);
                if let (Some((v, gte)), Some(d)) = (external, i.docs.get(&id))
                    && (v < d.version || (!gte && v == d.version))
                {
                    return (409, external_conflict(&id, d.version, v));
                }
                let Some(src) = parse_json(body) else {
                    return (
                        400,
                        error("x_content_parse_exception", "Failed to parse content to map", 400),
                    );
                };
                if let Err(e) = suggest::validate_doc(&i.mappings, &src) {
                    // Missing contexts fail while indexing, after the
                    // operation took a sequence number; parse errors don't.
                    if e.kind == "illegal_argument_exception" {
                        i.next_seq(q.get("routing").map_or(id.as_str(), String::as_str));
                    }
                    return (e.status, e.to_json());
                }
                if let Err(e) = index_mapping(&mut i.mappings, &src, &id) {
                    return e;
                }
                let exists = i.docs.contains_key(&id);
                let seq = i.next_seq(q.get("routing").map_or(id.as_str(), String::as_str));
                // A new version goes to the end of the index order, as
                // Lucene appends it (ties in search order follow this).
                if exists {
                    i.order.retain(|x| x != &id);
                }
                i.order.push(id.clone());
                let d = i.docs.entry(id.clone()).or_insert_with(|| Document {
                    source: json!({}),
                    version: 0,
                    seq,
                    routing: None,
                });
                d.source = src;
                d.version = match external {
                    Some((v, _)) => v,
                    None => d.version + 1,
                };
                d.seq = seq;
                d.routing = q.get("routing").cloned();
                let mut result = (
                    if exists { 200 } else { 201 },
                    doc_response(index, &id, d, if exists { "updated" } else { "created" }),
                );
                result.1["_shards"] = shards;
                maybe_refresh(i, index, q);
                mark_forced_refresh(&mut result.1, q);
                result
            }
            "DELETE" => {
                if q.get("routing").is_none() && routing_required(i) {
                    return routing_missing(index, &id);
                }
                let visible = routed_visible(i, &id, q.get("routing").map(String::as_str));
                if !visible {
                    let seq = i.next_seq(q.get("routing").map_or(id.as_str(), String::as_str));
                    return (
                        404,
                        json!({"_index": index, "_id": id, "_version": 1, "result": "not_found",
                               "_shards": shards, "_seq_no": seq, "_primary_term": 1}),
                    );
                }
                if let Err(e) = check_seq_no(i.docs.get(&id), &id, q) {
                    return e;
                }
                let external = external_version(q);
                if let (Some((v, gte)), Some(d)) = (external, i.docs.get(&id))
                    && (v < d.version || (!gte && v == d.version))
                {
                    return (409, external_conflict(&id, d.version, v));
                }
                let seq = i.next_seq(q.get("routing").map_or(id.as_str(), String::as_str));
                let (status, version, result) = match i.docs.remove(&id) {
                    Some(d) => {
                        i.order.retain(|x| x != &id);
                        (200, external.map_or(d.version + 1, |(v, _)| v), "deleted")
                    }
                    None => (404, external.map_or(1, |(v, _)| v), "not_found"),
                };
                let mut out = (
                    status,
                    json!({"_index": index, "_id": id, "_version": version, "result": result,
                           "_shards": shards, "_seq_no": seq, "_primary_term": 1}),
                );
                maybe_refresh(i, index, q);
                mark_forced_refresh(&mut out.1, q);
                out
            }
            _ => (405, error("method_not_allowed_exception", "Incorrect HTTP method", 405)),
        }
    }

    /// `POST /<index>/_update/<id>`: a partial `doc` (a no-op when it
    /// changes nothing), or a Painless `script` that may set `ctx.op` to
    /// `noop` or `delete`; `upsert` / `doc_as_upsert` / `scripted_upsert`
    /// for a missing document.
    fn update(
        &self,
        method: &str,
        index: &str,
        id: &str,
        q: &HashMap<String, String>,
        body: &[u8],
    ) -> (u16, Value) {
        if method != "POST" {
            return (405, error("method_not_allowed_exception", "Incorrect HTTP method", 405));
        }
        let Some(req) = parse_json(body) else { return (400, malformed_body()) };
        const KNOWN: &[&str] = &[
            "doc",
            "upsert",
            "doc_as_upsert",
            "script",
            "scripted_upsert",
            "detect_noop",
            "_source",
            "lang",
        ];
        if let Some(bad) =
            req.as_object().and_then(|m| m.keys().find(|k| !KNOWN.contains(&k.as_str())))
        {
            return (
                400,
                error(
                    "x_content_parse_exception",
                    &format!(
                        "[1:2] [UpdateRequest] unknown field [{bad}]{}",
                        did_you_mean(bad, KNOWN)
                    ),
                    400,
                ),
            );
        }
        let script = match req.get("script").map(|sc| {
            painless::script_parts(sc).and_then(|(src, params)| {
                painless::compile(&src).map(|c| (c, src, params)).map_err(|e| e.to_string())
            })
        }) {
            None => None,
            Some(Ok(x)) => Some(x),
            Some(Err(e)) => {
                return (400, painless::script_error(&req["script"].to_string(), &e, true));
            }
        };
        let patch = req.get("doc").cloned();
        if script.is_none() && patch.is_none() {
            return (
                400,
                error(
                    "action_request_validation_exception",
                    "Validation Failed: 1: script or doc is missing;",
                    400,
                ),
            );
        }
        let mut s = self.0.lock().unwrap();
        if !s.indices.contains_key(index) {
            if req.get("upsert").is_none() && req.get("doc_as_upsert").is_none() {
                return missing_index(index);
            }
            let created = new_index_from_templates(&s.templates, index);
            s.indices.insert(index.to_string(), created);
        }
        let i = s.indices.get_mut(index).unwrap();
        if let (Some(want_seq), Some(want_term)) = (q.get("if_seq_no"), q.get("if_primary_term")) {
            let current = i.docs.get(id).map(|d| d.seq);
            if want_term != "1"
                || current.map(|c| c.to_string()).as_deref() != Some(want_seq.as_str())
            {
                return (
                    409,
                    error(
                        "version_conflict_engine_exception",
                        &format!(
                            "[{id}]: version conflict, required seqNo [{want_seq}], primary term \
                             [{want_term}]. current document has seqNo [{}] and primary term [1]",
                            current.unwrap_or(-2)
                        ),
                        409,
                    ),
                );
            }
        }
        let run_script =
            |sc: &(painless::Script, String, Value), source: Value, op: &str, version: i64| {
                let ctx = json!({"_source": source, "op": op, "_id": id, "_index": index,
                             "_version": version, "_routing": null, "_now": dates::now_ms()});
                sc.0.run(ctx, sc.2.clone())
                    .map_err(|e| (400, painless::script_error(&sc.1, &e, false)))
            };
        if q.get("routing").is_none() && routing_required(i) {
            return routing_missing(index, id);
        }
        let visible = routed_visible(i, id, q.get("routing").map(String::as_str));
        let mut result = if let Some(d) = i.docs.get(id).filter(|_| visible) {
            let (new_source, op) = match &script {
                Some(sc) => match run_script(sc, d.source.clone(), "index", d.version) {
                    Ok(ctx) => (
                        ctx.get("_source").cloned().unwrap_or_default(),
                        ctx.get("op").and_then(Value::as_str).unwrap_or("index").to_string(),
                    ),
                    Err(e) => return e,
                },
                None => {
                    let mut src = d.source.clone();
                    merge(&mut src, patch.clone().unwrap_or_default());
                    let detect_noop =
                        req.get("detect_noop").and_then(Value::as_bool).unwrap_or(true);
                    let op = if detect_noop && src == d.source { "noop" } else { "index" };
                    (src, op.to_string())
                }
            };
            match op.as_str() {
                "delete" => {
                    let version = d.version + 1;
                    let key = d.routing.clone().unwrap_or_else(|| id.to_string());
                    i.docs.remove(id);
                    i.order.retain(|x| x != id);
                    let seq = i.next_seq(&key);
                    (
                        200,
                        json!({"_index": index, "_id": id, "_version": version, "result": "deleted",
                               "_shards": {"total": 2, "successful": 1, "failed": 0},
                               "_seq_no": seq, "_primary_term": 1}),
                    )
                }
                "index" | "create" => {
                    if let Err(e) = index_mapping(&mut i.mappings, &new_source, id) {
                        return e;
                    }
                    let key = d.routing.clone().unwrap_or_else(|| id.to_string());
                    let seq = i.next_seq(&key);
                    i.order.retain(|x| x != id);
                    i.order.push(id.to_string());
                    let d = i.docs.get_mut(id).unwrap();
                    d.source = new_source;
                    d.seq = seq;
                    d.version += 1;
                    (200, doc_response(index, id, d, "updated"))
                }
                // Elasticsearch 8 treats any other op as a noop (with a
                // deprecation warning).
                _ => (200, doc_response(index, id, d, "noop")),
            }
        } else {
            let upsert = req.get("upsert").cloned().or_else(|| {
                (req.get("doc_as_upsert").and_then(Value::as_bool) == Some(true))
                    .then(|| patch.clone())
                    .flatten()
            });
            let Some(mut src) = upsert else {
                return (
                    404,
                    json!({"error": {
                        "root_cause": [{"type": "document_missing_exception", "reason": format!("[{id}]: document missing"), "index_uuid": "noida", "shard": "0", "index": index}],
                        "type": "document_missing_exception", "reason": format!("[{id}]: document missing"),
                        "index_uuid": "noida", "shard": "0", "index": index}, "status": 404}),
                );
            };
            if req.get("scripted_upsert").and_then(Value::as_bool) == Some(true)
                && let Some(sc) = &script
            {
                match run_script(sc, src, "create", 0) {
                    Ok(ctx) => {
                        if matches!(
                            ctx.get("op").and_then(Value::as_str),
                            Some("noop") | Some("none")
                        ) {
                            return (
                                200,
                                json!({"_index": index, "_id": id, "_version": 0, "result": "noop",
                                       "_shards": {"total": 0, "successful": 0, "failed": 0},
                                       "_seq_no": -2, "_primary_term": 0}),
                            );
                        }
                        src = ctx.get("_source").cloned().unwrap_or_default();
                    }
                    Err(e) => return e,
                }
            }
            if let Err(e) = index_mapping(&mut i.mappings, &src, id) {
                return e;
            }
            let seq = i.next_seq(q.get("routing").map_or(id, String::as_str));
            i.order.push(id.to_string());
            i.docs.insert(
                id.to_string(),
                Document { source: src, version: 1, seq, routing: q.get("routing").cloned() },
            );
            (201, doc_response(index, id, i.docs.get(id).unwrap(), "created"))
        };
        if result.1["result"] != "noop" && result.1["_shards"]["total"] != 0 {
            result.1["_shards"] = shards_header(i);
        }
        if let Some(rt) = q.get("routing")
            && let Some(d) = i.docs.get_mut(id)
        {
            d.routing = Some(rt.clone());
        }
        // `_source` (in the body or as a parameter): the updated document
        // comes back under `get`.
        let source_param = source_filter_from_params(q);
        let want_get = match req.get("_source") {
            Some(Value::Bool(false)) => None,
            Some(Value::Bool(true)) => Some(None),
            Some(other) => Some(Some(other.clone())),
            None if q.contains_key("_source")
                || q.contains_key("_source_includes")
                || q.contains_key("_source_excludes") =>
            {
                match &source_param {
                    Some(Value::Bool(false)) => None,
                    other => Some(other.clone()),
                }
            }
            None => None,
        };
        if let Some(filter) = want_get
            && let Some(d) = i.docs.get(id)
        {
            result.1["get"] = json!({"_seq_no": d.seq, "_primary_term": 1, "found": true,
                                     "_source": search::filter_source(&d.source, filter.as_ref())});
        }
        maybe_refresh(i, index, q);
        if result.1["result"] != "noop" {
            mark_forced_refresh(&mut result.1, q);
        }
        result
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
        // Every action line is checked before anything runs: a bad one
        // fails the whole request.
        {
            let mut numbered = text.lines().enumerate().filter(|(_, l)| !l.trim().is_empty());
            while let Some((n, line)) = numbered.next() {
                let Ok(Value::Object(m)) = serde_json::from_str::<Value>(line) else { break };
                let malformed = |what: String| {
                    (
                        400,
                        error(
                            "illegal_argument_exception",
                            &format!("Malformed action/metadata line [{}], {what}", n + 1),
                            400,
                        ),
                    )
                };
                let Some(action) = m.keys().next() else {
                    return malformed("expected FIELD_NAME but found [END_OBJECT]".into());
                };
                if !matches!(action.as_str(), "index" | "create" | "update" | "delete") {
                    return malformed(format!(
                        "expected field [create], [delete], [index] or [update] but found [{action}]"
                    ));
                }
                if action != "delete" {
                    numbered.next();
                }
            }
        }
        let mut lines = text.lines().filter(|l| !l.trim().is_empty());
        let mut items = Vec::new();
        let mut errors = false;
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
            // `index` with `op_type: create` is a create.
            let action = if action == "index"
                && opts.get("op_type").and_then(Value::as_str) == Some("create")
            {
                "create"
            } else {
                action.as_str()
            };
            let ix = opts.get("_index").and_then(Value::as_str).unwrap_or(index);
            // A numeric `_id` is taken as its string form.
            let given_id_owned = opts.get("_id").and_then(|v| match v {
                Value::String(s) => Some(s.clone()),
                Value::Number(n) => Some(n.to_string()),
                _ => None,
            });
            let given_id = given_id_owned.as_deref();
            let id = given_id.map(str::to_string).unwrap_or_else(auto_id);
            // Per-item options, as the single-document APIs take them.
            let mut item_q: HashMap<String, String> = HashMap::new();
            for k in ["routing", "version", "version_type", "if_seq_no", "if_primary_term"] {
                if let Some(v) = opts.get(k) {
                    item_q.insert(
                        k.to_string(),
                        v.as_str().map(str::to_string).unwrap_or_else(|| v.to_string()),
                    );
                }
            }
            if let Some(r) = q.get("routing") {
                item_q.entry("routing".into()).or_insert_with(|| r.clone());
            }
            for k in ["_source", "_source_includes", "_source_excludes"] {
                if let Some(v) = q.get(k) {
                    item_q.insert(k.into(), v.clone());
                }
            }
            let require_alias =
                opts.get("require_alias").and_then(Value::as_bool).unwrap_or_else(|| {
                    q.get("require_alias").is_some_and(|v| v.is_empty() || v == "true")
                });
            let item_error = |status: u16, e: Value| {
                let mut item = Map::new();
                let mut body =
                    json!({"_index": ix, "_id": given_id.unwrap_or(""), "status": status});
                body["error"] = e["error"].clone();
                item.insert(action.to_string(), body);
                Value::Object(item)
            };
            if given_id == Some("") {
                if action != "delete" {
                    lines.next();
                }
                errors = true;
                items.push(item_error(
                    400,
                    error(
                        "illegal_argument_exception",
                        "if _id is specified it must not be empty",
                        400,
                    ),
                ));
                continue;
            }
            if require_alias && action != "delete" && !self.is_alias(ix) {
                lines.next();
                errors = true;
                items.push(item_error(404, require_alias_error(ix).1));
                continue;
            }
            let ix = &match self.write_target(ix) {
                Ok(t) => t,
                Err(e) => {
                    if action != "delete" {
                        lines.next();
                    }
                    errors = true;
                    items.push(item_error(e.0, e.1));
                    continue;
                }
            };
            touched.push(ix.to_string());
            if action == "update" {
                // A partial update (`{"doc": ...}` / upsert), not a
                // replace. Found via testing before a public release: bulk
                // `update` used to store the `{"doc": ...}` wrapper itself as
                // the new document.
                let data = lines.next().unwrap_or("").as_bytes();
                let mut uq = item_q.clone();
                if let Some(src) = opts.get("_source") {
                    uq.insert(
                        "_source".into(),
                        src.as_str().map(str::to_string).unwrap_or_else(|| src.to_string()),
                    );
                }
                let (status, res) = self.update("POST", ix, &id, &uq, data);
                errors |= status >= 300;
                let res = bulk_item(ix, &id, status, res);
                let mut item = Map::new();
                item.insert(action.to_string(), res);
                items.push(Value::Object(item));
            } else if action == "delete" {
                let (status, res) = self.document_api("DELETE", ix, &id, "_doc", &item_q, b"");
                errors |= status >= 300;
                let res = bulk_item(ix, &id, status, res);
                let mut item = Map::new();
                item.insert(action.to_string(), res);
                items.push(Value::Object(item));
            } else {
                let data = lines.next().unwrap_or("").as_bytes();
                let verb = if action == "create" { "POST" } else { "PUT" };
                let kind = if action == "create" { "_create" } else { "_doc" };
                let (status, res) = self.document_api(verb, ix, &id, kind, &item_q, data);
                errors |= status >= 300;
                let res = bulk_item(ix, &id, status, res);
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
        let Some(req) = parse_json(body) else { return (400, malformed_body()) };
        // `{"docs": [{"_id": ...}]}` or the `{"ids": [...]}` shorthand.
        let mut docs = req.get("docs").and_then(Value::as_array).cloned().unwrap_or_default();
        if let Some(ids) = req.get("ids").and_then(Value::as_array) {
            docs.extend(ids.iter().map(|id| json!({"_id": id})));
        }
        let id_of = |d: &Value| match d.get("_id") {
            Some(Value::String(s)) => Some(s.clone()),
            Some(Value::Number(n)) => Some(n.to_string()),
            _ => None,
        };
        let mut problems = Vec::new();
        if docs.is_empty() {
            problems.push("no documents to get".to_string());
        }
        for (n, d) in docs.iter().enumerate() {
            if d.get("_index").and_then(Value::as_str).is_none() && index.is_empty() {
                problems.push(format!("index is missing for doc {n}"));
            }
            if id_of(d).is_none() {
                problems.push(format!("id is missing for doc {n}"));
            }
        }
        if !problems.is_empty() {
            let reason: String = problems
                .iter()
                .enumerate()
                .map(|(k, p)| format!("{}: {p};", k + 1))
                .collect::<Vec<_>>()
                .join(" ");
            return (
                400,
                error(
                    "action_request_validation_exception",
                    &format!("Validation Failed: {reason}"),
                    400,
                ),
            );
        }
        let mut s = self.0.lock().unwrap();
        if q.get("refresh").is_some_and(|v| v.is_empty() || v == "true") {
            for (n, i) in s.indices.iter_mut() {
                i.refresh(n);
            }
        }
        let items: Vec<Value> = docs
            .iter()
            .map(|d| {
                let target = d.get("_index").and_then(Value::as_str).unwrap_or(index);
                let id = id_of(d).unwrap_or_default();
                let names = Self::resolve_indices(&s, target);
                let Some(ix) = names.first() else {
                    let e = missing_index(target).1["error"].clone();
                    return json!({"_index": target, "_id": id, "error": e});
                };
                if names.len() > 1 {
                    let reason = format!(
                        "alias [{target}] has more than one index associated with it [{}], can't execute a single index op",
                        names.join(", ")
                    );
                    return json!({"_index": target, "_id": id, "error": {
                        "root_cause": [{"type": "illegal_argument_exception", "reason": reason}],
                        "type": "illegal_argument_exception", "reason": reason}});
                }
                let mut opts = GetOpts::from_params(q);
                if let Some(src) = d.get("_source") {
                    opts.source = match src {
                        Value::Bool(true) => None,
                        other => Some(other.clone()),
                    };
                    opts.source_explicit = true;
                }
                if let Some(sf) = d.get("stored_fields") {
                    opts.stored_fields = Some(sf.clone());
                }
                if let Some(r) = d.get("routing").or_else(|| d.get("_routing")) {
                    opts.routing = Some(r.as_str().map(str::to_string).unwrap_or_else(|| r.to_string()));
                }
                let i = &s.indices[ix];
                let (status, doc) = get_doc(i, ix, &id, &opts);
                if status >= 400 && doc.get("error").is_some() {
                    return json!({"_index": ix, "_id": id, "error": doc["error"]});
                }
                doc
            })
            .collect();
        (200, json!({"docs": items}))
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
        let Some(req) = parse_json(body) else { return (400, malformed_body()) };
        let script = match (action, req.get("script")) {
            ("_update_by_query", Some(sc)) => match painless::script_parts(sc)
                .and_then(|(src, params)| painless::compile(&src).map(|c| (c, src, params)))
            {
                Ok(x) => Some(x),
                Err(e) => return (400, painless::script_error(&sc.to_string(), &e, true)),
            },
            _ => None,
        };
        let proceed = q.get("conflicts").map(String::as_str) == Some("proceed")
            || req.get("conflicts").and_then(Value::as_str) == Some("proceed");
        let max_docs = req
            .get("max_docs")
            .and_then(Value::as_u64)
            .or_else(|| q.get("max_docs").and_then(|m| m.parse().ok()))
            .unwrap_or(u64::MAX) as usize;
        let query = req.get("query").cloned().unwrap_or_else(|| json!({"match_all": {}}));
        let mut s = self.0.lock().unwrap();
        let names = Self::resolve_indices(&s, index);
        if names.is_empty() {
            return missing_index(index);
        }
        for n in &names {
            if let Some(i) = s.indices.get_mut(n) {
                i.auto_refresh(n);
            }
        }
        let (mut total, mut done, mut conflicts, mut noops) = (0, 0, 0, 0);
        let mut failures = Vec::new();
        for name in &names {
            let i = s.indices.get_mut(name).unwrap();
            let matched = match search::eval_root(&query, &i.mappings, &i.committed) {
                Ok(m) => m,
                Err(e) => return (e.status, e.to_json()),
            };
            let mut hits: Vec<usize> = matched.keys().copied().collect();
            hits.sort_unstable();
            hits.truncate(max_docs.saturating_sub(total));
            let mut wrote = false;
            for k in hits {
                let snap = i.committed[k].clone();
                total += 1;
                // Like Elasticsearch, these work from the last refresh: the
                // script sees the refreshed source, and a write to a
                // document changed (or deleted) since is a version conflict.
                let (src, op) = match &script {
                    Some(sc) => {
                        let ctx = json!({"_source": snap.source, "op": "index", "_id": snap.id,
                                         "_index": name, "_version": snap.version, "_now": dates::now_ms()});
                        match sc.0.run(ctx, sc.2.clone()) {
                            Ok(ctx) => (
                                ctx.get("_source").cloned().unwrap_or_default(),
                                ctx.get("op")
                                    .and_then(Value::as_str)
                                    .unwrap_or("index")
                                    .to_string(),
                            ),
                            Err(e) => return (400, painless::script_error(&sc.1, &e, false)),
                        }
                    }
                    None if action == "_delete_by_query" => (Value::Null, "delete".to_string()),
                    None => (snap.source.clone(), "index".to_string()),
                };
                if !matches!(op.as_str(), "index" | "delete") {
                    noops += 1;
                    continue;
                }
                let current = i.docs.get(&snap.id).map(|d| d.seq);
                if current != Some(snap.seq) {
                    conflicts += 1;
                    if !proceed {
                        let reason = match current {
                            Some(c) => format!(
                                "[{}]: version conflict, required seqNo [{}], primary term [1]. \
                                 current document has seqNo [{c}] and primary term [1]",
                                snap.id, snap.seq
                            ),
                            None => {
                                format!("[{}]: version conflict, document already deleted", snap.id)
                            }
                        };
                        failures.push(json!({
                            "index": name, "id": snap.id,
                            "cause": {"type": "version_conflict_engine_exception", "reason": reason,
                                      "index_uuid": "noida", "shard": "0", "index": name},
                            "status": 409}));
                    }
                    continue;
                }
                if op != "delete"
                    && let Err((status, e)) = index_mapping(&mut i.mappings, &src, &snap.id)
                {
                    failures.push(json!({"index": name, "id": snap.id, "cause": e["error"],
                                         "status": status}));
                    continue;
                }
                wrote = true;
                done += 1;
                let key = i.docs.get(&snap.id).and_then(|d| d.routing.clone());
                let seq = i.next_seq(key.as_deref().unwrap_or(&snap.id));
                if op == "delete" {
                    i.docs.remove(&snap.id);
                    i.order.retain(|x| x != &snap.id);
                } else {
                    let d = i.docs.get_mut(&snap.id).unwrap();
                    d.source = src;
                    d.seq = seq;
                    d.version += 1;
                    // A rewritten document moves to the end of the index
                    // order (Lucene appends the new version).
                    i.order.retain(|x| x != &snap.id);
                    i.order.push(snap.id.clone());
                }
            }
            // Elasticsearch only refreshes an index the request wrote to.
            if wrote {
                maybe_refresh(i, name, q);
            }
        }
        let mut out = json!({"took": 0, "timed_out": false, "total": total, "batches": 1,
            "version_conflicts": conflicts, "noops": noops, "failures": failures,
            "retries": {"bulk": 0, "search": 0}, "throttled_millis": 0,
            "requests_per_second": -1.0, "throttled_until_millis": 0});
        out["deleted"] = json!(if action == "_delete_by_query" { done } else { 0 });
        if action == "_update_by_query" {
            out["updated"] = json!(done);
        }
        let status = if conflicts > 0 && !proceed { 409 } else { 200 };
        (status, out)
    }

    /// `PUT|DELETE /<index>/_alias/<name>` and `GET /<index>/_alias`.
    /// Concrete indices for an alias API's index expression (`_all`, `*`,
    /// globs, lists); a concrete name that doesn't exist is a 404.
    /// `expand_wildcards=open` leaves closed indices out of wildcards.
    fn alias_indices(
        s: &State,
        expr: &str,
        q: &HashMap<String, String>,
    ) -> Result<Vec<String>, (u16, Value)> {
        let open_only = q.get("expand_wildcards").is_some_and(|w| w == "open");
        let mut out = Vec::new();
        for part in expr.split(',').map(str::trim).filter(|p| !p.is_empty()) {
            if part == "_all" || part.contains('*') {
                out.extend(
                    s.indices
                        .iter()
                        .filter(|(n, i)| {
                            (part == "_all" || glob_match(part, n)) && (!open_only || i.opened)
                        })
                        .map(|(n, _)| n.clone()),
                );
            } else if s.indices.contains_key(part) {
                out.push(part.to_string());
            } else {
                return Err(missing_index(part));
            }
        }
        out.sort();
        out.dedup();
        Ok(out)
    }

    /// `/{index}/_alias[/{name}]`, `/_alias[/{name}]`: GET, HEAD, PUT,
    /// POST and DELETE.
    fn alias_api(
        &self,
        method: &str,
        index_expr: &str,
        name: Option<&str>,
        q: &HashMap<String, String>,
        body: &[u8],
        no_index: bool,
    ) -> (u16, Value) {
        let mut s = self.0.lock().unwrap();
        match method {
            "GET" | "HEAD" => {
                let indices = match Self::alias_indices(&s, index_expr, q) {
                    Ok(i) => i,
                    Err(e) => return e,
                };
                let Some(name) = name.filter(|n| !n.is_empty()) else {
                    // No alias names: every index, with whatever aliases.
                    let mut out = Map::new();
                    for n in &indices {
                        out.insert(
                            n.clone(),
                            json!({"aliases": aliases_out(&s.indices[n].aliases)}),
                        );
                    }
                    return (200, Value::Object(out));
                };
                let patterns: Vec<&str> = name.split(',').map(str::trim).collect();
                let mut out = Map::new();
                let mut returned: Vec<String> = Vec::new();
                for n in &indices {
                    let i = &s.indices[n];
                    let picked: Map<String, Value> = i
                        .aliases
                        .iter()
                        .filter(|(a, _)| alias_selected(&patterns, a))
                        .map(|(a, v)| (a.clone(), v.clone()))
                        .collect();
                    if !picked.is_empty() {
                        returned.extend(picked.keys().cloned());
                        out.insert(n.clone(), json!({"aliases": picked}));
                    }
                }
                let missing = missing_aliases(&patterns, &returned);
                if missing.is_empty() {
                    return (200, Value::Object(out));
                }
                out.insert(
                    "error".into(),
                    json!(if missing.len() == 1 {
                        format!("alias [{}] missing", missing[0])
                    } else {
                        format!("aliases [{}] missing", missing.join(","))
                    }),
                );
                out.insert("status".into(), json!(404));
                (404, Value::Object(out))
            }
            "PUT" | "POST" => {
                if no_index {
                    return (
                        400,
                        error(
                            "action_request_validation_exception",
                            "Validation Failed: 1: [index] is missing;",
                            400,
                        ),
                    );
                }
                let Some(name) = name else {
                    return (
                        400,
                        error(
                            "action_request_validation_exception",
                            "Validation Failed: 1: [alias] is missing;",
                            400,
                        ),
                    );
                };
                let Some(req) = parse_json(body) else { return (400, malformed_body()) };
                let indices = match Self::alias_indices(&s, index_expr, q) {
                    Ok(i) => i,
                    Err(e) => return e,
                };
                if let Err(e) = validate_alias_name(name, |n| s.indices.contains_key(n)) {
                    return e;
                }
                let spec = alias_spec(&req);
                for n in indices {
                    if let Some(i) = s.indices.get_mut(&n) {
                        i.aliases.insert(name.to_string(), spec.clone());
                    }
                }
                (200, json!({"acknowledged": true, "errors": false}))
            }
            "DELETE" => {
                let (Some(name), false) = (name, no_index) else {
                    return (
                        400,
                        error(
                            "action_request_validation_exception",
                            "Validation Failed: 1: [index] and [name] are required;",
                            400,
                        ),
                    );
                };
                let indices = match Self::alias_indices(&s, index_expr, q) {
                    Ok(i) => i,
                    Err(e) => return e,
                };
                let patterns: Vec<&str> = name.split(',').map(str::trim).collect();
                let mut removed = false;
                for n in indices {
                    if let Some(i) = s.indices.get_mut(&n) {
                        let before = i.aliases.len();
                        i.aliases.retain(|a, _| !alias_selected(&patterns, a));
                        removed |= i.aliases.len() != before;
                    }
                }
                if removed {
                    (200, json!({"acknowledged": true, "errors": false}))
                } else {
                    (
                        404,
                        error(
                            "aliases_not_found_exception",
                            &format!("aliases [{name}] missing"),
                            404,
                        ),
                    )
                }
            }
            _ => (405, error("method_not_allowed_exception", "Incorrect HTTP method", 405)),
        }
    }

    /// `POST /_aliases`: add / remove / remove_index actions, applied
    /// atomically; a `remove` that finds nothing is reported per action
    /// (or fails the request with `must_exist: true`).
    fn aliases(&self, method: &str, body: &[u8]) -> (u16, Value) {
        if method != "POST" {
            return (405, error("method_not_allowed_exception", "Incorrect HTTP method", 405));
        }
        let Some(req) = parse_json(body) else { return (400, malformed_body()) };
        let Some(actions) = req.get("actions").and_then(Value::as_array) else {
            return (
                400,
                error("x_content_parse_exception", "Required [actions] field missing", 400),
            );
        };
        let list = |v: &Value, one: &str, many: &str| -> Vec<String> {
            let mut out = Vec::new();
            for k in [one, many] {
                match v.get(k) {
                    Some(Value::String(x)) => out.push(x.clone()),
                    Some(Value::Array(a)) => {
                        out.extend(a.iter().filter_map(Value::as_str).map(str::to_string))
                    }
                    _ => {}
                }
            }
            out
        };
        let mut s = self.0.lock().unwrap();
        // Work on a copy; nothing changes unless every action can apply.
        let mut next: HashMap<String, HashMap<String, Value>> =
            s.indices.iter().map(|(n, i)| (n.clone(), i.aliases.clone())).collect();
        let mut drop_indices: Vec<String> = Vec::new();
        let mut results = Vec::new();
        let mut errors = false;
        let no_q = HashMap::new();
        for action in actions {
            let Some((op, v)) = action.as_object().and_then(|o| o.iter().next()) else {
                continue;
            };
            let index_exprs = list(v, "index", "indices");
            let alias_names = list(v, "alias", "aliases");
            if index_exprs.is_empty() {
                return (
                    400,
                    error(
                        "action_request_validation_exception",
                        "Validation Failed: 1: One of [index/indices] is required;",
                        400,
                    ),
                );
            }
            if v.get("must_exist").is_some() && op != "remove" {
                return (
                    400,
                    error(
                        "x_content_parse_exception",
                        &format!("[must_exist] is unsupported for [{op}]"),
                        400,
                    ),
                );
            }
            let mut indices = Vec::new();
            for e in &index_exprs {
                match Self::alias_indices(&s, e, &no_q) {
                    Ok(i) => indices.extend(i),
                    Err(err) => return err,
                }
            }
            let mut concrete = indices.clone();
            concrete.sort();
            concrete.dedup();
            let summary = json!({"type": op, "indices": concrete, "aliases": alias_names});
            match op.as_str() {
                "add" => {
                    if alias_names.is_empty() {
                        return (
                            400,
                            error(
                                "action_request_validation_exception",
                                "Validation Failed: 1: One of [alias/aliases] is required;",
                                400,
                            ),
                        );
                    }
                    let spec = alias_spec(v);
                    for a in &alias_names {
                        if let Err(e) = validate_alias_name(a, |n| {
                            s.indices.contains_key(n) && !drop_indices.iter().any(|d| d == n)
                        }) {
                            return e;
                        }
                        for n in &indices {
                            if let Some(m) = next.get_mut(n) {
                                m.insert(a.clone(), spec.clone());
                            }
                        }
                    }
                    results.push(json!({"action": summary, "status": 200}));
                }
                "remove" => {
                    let patterns: Vec<&str> = alias_names.iter().map(String::as_str).collect();
                    let mut removed = false;
                    for n in &indices {
                        if let Some(m) = next.get_mut(n) {
                            let before = m.len();
                            m.retain(|a, _| !alias_selected(&patterns, a));
                            removed |= m.len() != before;
                        }
                    }
                    if removed {
                        results.push(json!({"action": summary, "status": 200}));
                    } else {
                        let reason = format!("aliases [{}] missing", alias_names.join(","));
                        if v.get("must_exist").and_then(Value::as_bool) == Some(true) {
                            return (404, error("aliases_not_found_exception", &reason, 404));
                        }
                        errors = true;
                        let id = alias_names.join(",");
                        results.push(json!({"action": summary, "status": 404,
                            "error": {"type": "aliases_not_found_exception", "reason": reason,
                                      "resource.type": "aliases", "resource.id": id}}));
                    }
                }
                "remove_index" => {
                    drop_indices.extend(indices);
                    results.push(json!({"action": summary, "status": 200}));
                }
                other => {
                    return (
                        400,
                        error(
                            "x_content_parse_exception",
                            &format!("[1:1] [alias_action] unknown field [{other}]"),
                            400,
                        ),
                    );
                }
            }
        }
        for (n, m) in next {
            if let Some(i) = s.indices.get_mut(&n) {
                i.aliases = m;
            }
        }
        for n in drop_indices {
            s.indices.remove(&n);
        }
        if errors {
            (200, json!({"acknowledged": true, "errors": true, "action_results": results}))
        } else {
            (200, json!({"acknowledged": true, "errors": false}))
        }
    }
}

/// An index's searchable documents, narrowed to those matching any of
/// the alias filters it was reached through.
fn filtered_docs(
    i: &Index,
    filters: Option<&Option<Vec<Value>>>,
) -> Result<Vec<CommittedDoc>, search::EsError> {
    let Some(Some(filters)) = filters else { return Ok(i.committed.clone()) };
    let mut keep = std::collections::BTreeSet::new();
    for f in filters {
        keep.extend(search::eval_root(f, &i.mappings, &i.committed)?.into_keys());
    }
    Ok(keep.into_iter().map(|k| i.committed[k].clone()).collect())
}

/// Alias definitions as GET returns them.
fn aliases_out(m: &HashMap<String, Value>) -> Map<String, Value> {
    m.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
}

/// An alias definition from a request body (`filter`, routing,
/// `is_write_index`, `is_hidden`), normalized as stored.
fn alias_spec(v: &Value) -> Value {
    let mut out = Map::new();
    for k in ["filter", "routing", "index_routing", "search_routing", "is_write_index", "is_hidden"]
    {
        if let Some(x) = v.get(k).filter(|x| !x.is_null()) {
            out.insert(k.to_string(), x.clone());
        }
    }
    normalize_alias(&Value::Object(out))
}

fn validate_alias_name(
    name: &str,
    index_exists: impl Fn(&str) -> bool,
) -> Result<(), (u16, Value)> {
    let bad = |reason: String| Err((400, error("invalid_alias_name_exception", &reason, 400)));
    if name.chars().any(|c| "\\/*?\"<>| ,#".contains(c)) {
        return bad(format!(
            "Invalid alias name [{name}]: must not contain the following characters [ , \", *, \\, <, |, ,, >, /, ?]"
        ));
    }
    if name.starts_with(['_', '-', '+']) {
        return bad(format!("Invalid alias name [{name}]: must not start with '_', '-', or '+'"));
    }
    if index_exists(name) {
        return bad(format!(
            "Invalid alias name [{name}]: an index or data stream exists with the same name as the alias"
        ));
    }
    Ok(())
}

/// Elasticsearch's alias name expressions: names, `*` globs and `_all`;
/// a `-pattern` at or after the first wildcard excludes. The last
/// pattern that matches decides.
fn alias_selected(patterns: &[&str], alias: &str) -> bool {
    if patterns.is_empty() {
        return true;
    }
    let first_wild =
        patterns.iter().position(|p| *p == "_all" || p.contains('*')).unwrap_or(patterns.len());
    let mut selected = false;
    for (i, p) in patterns.iter().enumerate() {
        let (include, pat) = match p.strip_prefix('-') {
            Some(rest) if i >= first_wild => (false, rest),
            _ => (true, *p),
        };
        if pat == "_all" || glob_match(pat, alias) {
            selected = include;
        }
    }
    selected
}

/// Explicitly named aliases (not globs, not exclusions, not excluded
/// later) that matched nothing: those make a GET a 404.
fn missing_aliases(patterns: &[&str], returned: &[String]) -> Vec<String> {
    let first_wild =
        patterns.iter().position(|p| *p == "_all" || p.contains('*')).unwrap_or(patterns.len());
    let mut missing = Vec::new();
    for (i, p) in patterns.iter().enumerate() {
        if *p == "_all" || p.contains('*') || (i >= first_wild && p.starts_with('-')) {
            continue;
        }
        let excluded = patterns
            .iter()
            .enumerate()
            .skip((i + 1).max(first_wild))
            .any(|(_, q)| q.strip_prefix('-').is_some_and(|x| x == "_all" || glob_match(x, p)));
        if !excluded && !returned.iter().any(|r| r == p) {
            missing.push(p.to_string());
        }
    }
    missing.sort();
    missing.dedup();
    missing
}

/// Elasticsearch's "can match" pre-filter for `index_filter`: an index
/// is skipped only when the query provably matches nothing there -- a
/// query on a field the index doesn't map, or a range on a date field
/// outside every value the index holds. Anything else may match.
fn index_can_match(i: &Index, q: &Value) -> bool {
    let Some((kind, body)) = q.as_object().and_then(|m| m.iter().next()) else { return true };
    match kind.as_str() {
        "bool" => {
            let must: Vec<&Value> = ["must", "filter"]
                .iter()
                .filter_map(|k| body.get(*k))
                .flat_map(|v| match v {
                    Value::Array(a) => a.iter().collect::<Vec<_>>(),
                    other => vec![other],
                })
                .collect();
            must.iter().all(|c| index_can_match(i, c))
        }
        "match_none" => false,
        "range" | "term" | "terms" | "match" | "prefix" | "wildcard" | "exists" => {
            let field = if kind == "exists" {
                body.get("field").and_then(Value::as_str).map(str::to_string)
            } else {
                body.as_object().and_then(|m| m.keys().find(|k| *k != "boost").cloned())
            };
            let Some(field) = field else { return true };
            let (_, ty) = search::resolve_field(&i.mappings, &field);
            let Some(ty) = ty else { return false };
            if kind == "range" && matches!(ty.as_str(), "date" | "date_nanos") {
                let cond = &body[field.as_str()];
                let format = cond.get("format").and_then(Value::as_str);
                let bound = |k: &str| {
                    cond.get(k).and_then(|v| match v {
                        Value::String(s) => dates::parse_math(
                            s,
                            dates::now_ms(),
                            k == "gt" || k == "lte",
                            format,
                            0,
                        ),
                        Value::Number(n) => n.as_i64(),
                        _ => None,
                    })
                };
                let (gte, gt, lte, lt) = (bound("gte"), bound("gt"), bound("lte"), bound("lt"));
                return i.committed.iter().any(|d| {
                    search::raw_values(&d.source, &field).into_iter().any(|v| {
                        let Some(ms) = dates::value_millis(v, None) else { return false };
                        gte.is_none_or(|b| ms >= b)
                            && gt.is_none_or(|b| ms > b)
                            && lte.is_none_or(|b| ms <= b)
                            && lt.is_none_or(|b| ms < b)
                    })
                });
            }
            true
        }
        _ => true,
    }
}

/// `rest_total_hits_as_int` needs exact totals: a numeric
/// `track_total_hits` is refused.
fn total_hits_as_int_error(req: &Value) -> Option<(u16, Value)> {
    let n = req.get("track_total_hits")?.as_i64()?;
    Some((
        400,
        error(
            "illegal_argument_exception",
            &format!(
                "[rest_total_hits_as_int] cannot be used if the tracking of total hits is not accurate, got {n}"
            ),
            400,
        ),
    ))
}

/// `{"a": {"b": 1}}` as `[("a.b", 1)]`.
fn flatten_keys(prefix: &str, v: &Value, out: &mut Vec<(String, Value)>) {
    match v {
        Value::Object(m) => {
            for (k, x) in m {
                let key = if prefix.is_empty() { k.clone() } else { format!("{prefix}.{k}") };
                flatten_keys(&key, x, out);
            }
        }
        other => out.push((prefix.to_string(), other.clone())),
    }
}

/// Dotted keys as nested objects: `{"a.b": 1}` as `{"a": {"b": 1}}`.
fn nest_keys(m: &Map<String, Value>) -> Value {
    let mut out = json!({});
    for (k, v) in m {
        let parts: Vec<&str> = k.split('.').collect();
        let mut node = &mut out;
        for p in &parts[..parts.len() - 1] {
            if !node.get(*p).is_some_and(Value::is_object) {
                node[*p] = json!({});
            }
            node = &mut node[*p];
        }
        node[parts[parts.len() - 1]] = v.clone();
    }
    out
}

/// The payload key carrying deprecation warnings for the `Warning`
/// response header.
pub const WARNINGS: &str = "\u{0}noida_warnings";

pub fn parse_json(bytes: &[u8]) -> Option<Value> {
    if bytes.is_empty() { Some(json!({})) } else { serde_json::from_slice(bytes).ok() }
}
pub fn error(kind: &str, reason: &str, status: u16) -> Value {
    json!({"error":{"root_cause":[{"type":kind,"reason":reason}],"type":kind,"reason":reason},"status":status})
}
/// Index settings the way Elasticsearch stores them: `index.`-prefixed,
/// nested, every value a string (`{"number_of_replicas": 0}` and
/// `{"index.number_of_replicas": "0"}` are the same setting). A `null`
/// value resets the setting to its default (drops it).
pub(super) fn apply_settings(target: &mut Value, incoming: &Value) {
    fn flatten(prefix: &str, v: &Value, out: &mut Vec<(String, Value)>) {
        match v {
            Value::Object(m) => {
                for (k, x) in m {
                    let key = if prefix.is_empty() { k.clone() } else { format!("{prefix}.{k}") };
                    flatten(&key, x, out);
                }
            }
            Value::Null => out.push((prefix.to_string(), Value::Null)),
            Value::String(_) => out.push((prefix.to_string(), v.clone())),
            Value::Array(a) => out.push((
                prefix.to_string(),
                Value::Array(
                    a.iter()
                        .map(|x| match x {
                            Value::String(_) => x.clone(),
                            other => json!(other.to_string()),
                        })
                        .collect(),
                ),
            )),
            other => out.push((prefix.to_string(), json!(other.to_string()))),
        }
    }
    let mut flat = Vec::new();
    flatten("", incoming, &mut flat);
    if !target.is_object() {
        *target = json!({});
    }
    for (key, val) in flat {
        let key = if key.starts_with("index.") { key } else { format!("index.{key}") };
        let parts: Vec<&str> = key.split('.').collect();
        let mut node = &mut *target;
        for p in &parts[..parts.len() - 1] {
            if !node.get(*p).is_some_and(Value::is_object) {
                node[*p] = json!({});
            }
            node = &mut node[*p];
        }
        let last = parts[parts.len() - 1];
        if val.is_null() {
            if let Some(m) = node.as_object_mut() {
                m.remove(last);
            }
        } else {
            node[last] = val;
        }
    }
}

/// (primaries, replicas per primary) from an index's settings; replicas
/// are never assigned on a single node, so they count as unassigned.
fn shard_counts(i: &Index) -> (u64, u64) {
    let num = |key: &str, default: u64| {
        let v = &i.settings["index"][key];
        v.as_u64().or_else(|| v.as_str().and_then(|x| x.parse().ok())).unwrap_or(default)
    };
    (num("number_of_shards", 1), num("number_of_replicas", 1))
}

/// A search response's `_shards` header for `total` primary shards.
fn set_search_shards(resp: &mut Value, total: u64) {
    if total > 1 && resp.get("_shards").is_some() {
        resp["_shards"]["total"] = json!(total);
        resp["_shards"]["successful"] = json!(total);
    }
}

/// `?human`: readable forms of the creation date and version.
fn add_human_settings(settings: &mut Value) {
    let Some(idx) = settings.get_mut("index").and_then(Value::as_object_mut) else { return };
    if let Some(ms) =
        idx.get("creation_date").and_then(Value::as_str).and_then(|v| v.parse::<i64>().ok())
    {
        idx.insert("creation_date_string".into(), json!(super::dates::format(ms, None, 0)));
    }
    if let Some(v) = idx.get_mut("version").and_then(Value::as_object_mut)
        && v.contains_key("created")
    {
        v.insert("created_string".into(), json!("8.15.0-8.15.3"));
    }
}

/// `action.destructive_requires_name` (default true): deleting, opening
/// or closing by wildcard or `_all` is refused.
fn destructive_check(s: &State, expr: &str) -> Result<(), (u16, Value)> {
    let setting = ["transient", "persistent"].iter().find_map(|k| {
        s.cluster_settings.get(*k).and_then(|m| m.get("action.destructive_requires_name")).cloned()
    });
    let requires_name = setting.as_ref().and_then(Value::as_str).is_none_or(|v| v != "false");
    if requires_name && expr.split(',').any(|p| p.trim() == "_all" || p.contains('*')) {
        return Err((
            400,
            error(
                "illegal_argument_exception",
                "Wildcard expressions or all indices are not allowed",
                400,
            ),
        ));
    }
    Ok(())
}

/// Scroll and slice request checks, with Elasticsearch's messages.
fn validate_scroll_and_slice(
    s: &State,
    index_pattern: &str,
    req: &Value,
    q: &HashMap<String, String>,
    scroll: Option<&str>,
) -> Result<(), (u16, Value)> {
    let cluster_setting = |key: &str| {
        ["transient", "persistent"].iter().find_map(|k| {
            s.cluster_settings
                .get(*k)
                .and_then(|m| m.get(key))
                .and_then(Value::as_str)
                .map(str::to_string)
        })
    };
    let max_keep = cluster_setting("search.max_keep_alive")
        .and_then(|v| parse_keep_alive(&v))
        .unwrap_or(std::time::Duration::from_secs(86_400));
    let invalid = |msg: &str| {
        Err((
            400,
            error(
                "action_request_validation_exception",
                &format!("Validation Failed: 1: {msg};"),
                400,
            ),
        ))
    };
    if let Some(sc) = scroll {
        let size = q
            .get("size")
            .and_then(|v| v.parse::<i64>().ok())
            .or_else(|| req.get("size").and_then(Value::as_i64));
        if size == Some(0) {
            return invalid("[size] cannot be [0] in a scroll context");
        }
        if q.get("request_cache").is_some_and(|v| v == "true") {
            return invalid("[request_cache] cannot be used in a scroll context");
        }
        if let Some(keep) = parse_keep_alive(sc)
            && keep > max_keep
        {
            let e = search::EsError::shard_failure(
                "illegal_argument_exception",
                &format!(
                    "Keep alive for request ({}) is too large. It must be less than ({}). This limit can be set by changing the [search.max_keep_alive] cluster level setting.",
                    time_value(keep),
                    time_value(max_keep)
                ),
            );
            return Err((400, e.to_json()));
        }
    }
    let Some(slice) = req.get("slice") else { return Ok(()) };
    let id = slice_num(slice, "id").unwrap_or(-1);
    let max = slice_num(slice, "max").unwrap_or(-1);
    let parse_err = |field: &str, reason: &str| {
        Err((
            400,
            json!({"error": {"root_cause": [{"type": "x_content_parse_exception", "reason": format!("[slice] failed to parse field [{field}]")}],
                "type": "x_content_parse_exception", "reason": format!("[slice] failed to parse field [{field}]"),
                "caused_by": {"type": "illegal_argument_exception", "reason": reason}}, "status": 400}),
        ))
    };
    if id < 0 {
        return parse_err("id", "id must be greater than or equal to 0");
    }
    if max <= 1 {
        return parse_err("max", "max must be greater than 1");
    }
    if id >= max {
        return parse_err("max", "max must be greater than id");
    }
    if scroll.is_none() && req.get("pit").is_none() {
        return invalid("[slice] can only be used with [scroll] or [point-in-time] requests");
    }
    let limit = Engine::resolve_indices(s, index_pattern)
        .iter()
        .filter_map(|n| s.indices.get(n))
        .map(|i| {
            let v = &i.settings["index"]["max_slices_per_scroll"];
            v.as_i64().or_else(|| v.as_str().and_then(|x| x.parse().ok())).unwrap_or(1024)
        })
        .min()
        .unwrap_or(1024);
    if max > limit {
        let e = search::EsError::shard_failure(
            "illegal_argument_exception",
            &format!(
                "The number of slices [{max}] is too large. It must be less than [{limit}]. This limit can be set by changing the [index.max_slices_per_scroll] index level setting."
            ),
        );
        return Err((400, e.to_json()));
    }
    Ok(())
}

/// A `slice` number, given as a number or a numeric string.
fn slice_num(slice: &Value, key: &str) -> Option<i64> {
    let v = slice.get(key)?;
    v.as_i64().or_else(|| v.as_str().and_then(|s| s.trim().parse().ok()))
}

/// The documents in a request's `slice` (all of them without one): each
/// document belongs to one slice by a hash of its `_id` (or `field`).
fn sliced(s: &State, docs: Vec<CommittedDoc>, req: &Value) -> Vec<CommittedDoc> {
    let Some(slice) = req.get("slice") else { return docs };
    let (Some(id), Some(max)) = (slice_num(slice, "id"), slice_num(slice, "max")) else {
        return docs;
    };
    let field = slice.get("field").and_then(Value::as_str).unwrap_or("_id");
    let hash_of = |d: &CommittedDoc| {
        let key = if field == "_id" {
            d.id.clone()
        } else {
            match search::raw_values(&d.source, field).first() {
                Some(Value::String(s)) => s.clone(),
                Some(v) => v.to_string(),
                None => String::new(),
            }
        };
        i64::from(murmur3_routing(&key))
    };
    docs.into_iter()
        .filter(|d| {
            let Some(i) = s.indices.get(&d.index) else {
                return hash_of(d).rem_euclid(max) == id;
            };
            let shards = shard_counts(i).0.max(1) as i64;
            let routing = i.docs.get(&d.id).and_then(|x| x.routing.clone());
            let shard = shard_of(i, routing.as_deref().unwrap_or(&d.id));
            if max <= shards {
                // Whole shards go to slices.
                return shard % max == id;
            }
            // More slices than shards: each shard is split further.
            if shard != id % shards {
                return false;
            }
            let per_shard = max / shards + i64::from(max % shards > shard);
            per_shard <= 1 || hash_of(d).rem_euclid(per_shard) == id / shards
        })
        .collect()
}

/// A duration as Elasticsearch's `TimeValue` prints it: `2m`, `41.6d`.
fn time_value(d: std::time::Duration) -> String {
    let ms = d.as_millis() as f64;
    for (unit, size) in [("d", 86_400_000.0), ("h", 3_600_000.0), ("m", 60_000.0), ("s", 1000.0)] {
        if ms >= size {
            let v = ms / size;
            return if v.fract() == 0.0 { format!("{v}{unit}") } else { format!("{v:.1}{unit}") };
        }
    }
    format!("{ms}ms")
}

/// A bulk item's response: a failure keeps only the target and the
/// error's cause, as Elasticsearch reports it per item.
fn bulk_item(index: &str, id: &str, status: u16, mut res: Value) -> Value {
    if status >= 300
        && let Some(err) = res.get("error").cloned()
        && err.is_object()
    {
        let mut cause = err.clone();
        if let Some(o) = cause.as_object_mut() {
            o.remove("root_cause");
        }
        return json!({"_index": index, "_id": id, "status": status, "error": cause});
    }
    res["status"] = json!(status);
    res
}

/// The query with an API's own `expand_wildcards` default filled in.
fn with_default_expand(q: &HashMap<String, String>, default: &str) -> HashMap<String, String> {
    let mut q = q.clone();
    q.entry("expand_wildcards".into()).or_insert_with(|| default.into());
    q
}

/// `index_closed_exception` for a read or write against a closed index.
fn index_closed(name: &str) -> (u16, Value) {
    let e = json!({"type": "index_closed_exception", "reason": "closed", "index_uuid": "noida", "index": name});
    (
        400,
        json!({"error": {"root_cause": [e.clone()], "type": "index_closed_exception", "reason": "closed",
                           "index_uuid": "noida", "index": name}, "status": 400}),
    )
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
/// Read options of GET / mget / get_source / exists.
struct GetOpts {
    source: Option<Value>,
    /// `_source` was given explicitly (keeps it alongside stored_fields).
    source_explicit: bool,
    stored_fields: Option<Value>,
    realtime: bool,
    version: Option<i64>,
    routing: Option<String>,
}

impl GetOpts {
    fn from_params(q: &HashMap<String, String>) -> Self {
        GetOpts {
            source: source_filter_from_params(q),
            source_explicit: q.contains_key("_source")
                || q.contains_key("_source_includes")
                || q.contains_key("_source_excludes"),
            stored_fields: q
                .get("stored_fields")
                .map(|f| json!(f.split(',').map(str::trim).collect::<Vec<_>>())),
            realtime: q.get("realtime").is_none_or(|v| v != "false"),
            version: q.get("version").and_then(|v| v.parse().ok()),
            routing: q.get("routing").cloned(),
        }
    }
}

/// One document as GET returns it: real-time from the live documents, or
/// (`realtime=false`) from the last refresh.
fn get_doc(i: &Index, index: &str, id: &str, o: &GetOpts) -> (u16, Value) {
    let found = if o.realtime {
        i.docs.get(id).map(|d| (d.source.clone(), d.version, d.seq))
    } else {
        i.committed.iter().find(|c| c.id == id).map(|c| (c.full().clone(), c.version, c.seq))
    };
    if o.routing.is_none() && routing_required(i) {
        return routing_missing(index, id);
    }
    let Some((source, version, seq)) = found else {
        return missing_doc(index, id);
    };
    if !routed_visible(i, id, o.routing.as_deref()) {
        return missing_doc(index, id);
    }
    if let Some(want) = o.version
        && want != version
    {
        return (
            409,
            error(
                "version_conflict_engine_exception",
                &format!(
                    "[{id}]: version conflict, current version [{version}] is different than the one provided [{want}]"
                ),
                409,
            ),
        );
    }
    let mut r = json!({"_index": index, "_id": id, "_version": version, "_seq_no": seq,
                       "_primary_term": 1});
    if let Some(rt) = i.docs.get(id).and_then(|d| d.routing.clone()) {
        r["_routing"] = json!(rt);
    }
    r["found"] = json!(true);
    let source_enabled =
        i.mappings.get("_source").and_then(|s| s.get("enabled")).and_then(Value::as_bool)
            != Some(false);
    let lists_source = o.stored_fields.as_ref().is_some_and(|sf| match sf {
        Value::Array(a) => a.iter().any(|f| f == "_source"),
        Value::String(f) => f.split(',').any(|f| f.trim() == "_source"),
        _ => false,
    });
    let want_source = o.stored_fields.is_none() || o.source_explicit || lists_source;
    if source_enabled && want_source && !matches!(o.source, Some(Value::Bool(false))) {
        r["_source"] = search::filter_source(&source, o.source.as_ref());
    }
    if let Some(sf) = &o.stored_fields {
        let doc = search::CommittedDoc {
            index: index.to_string(),
            id: id.to_string(),
            source,
            version,
            seq,
            full_source: None,
        };
        if let Ok(f) = super::fields::fetch(&i.mappings, &doc, sf, super::fields::Kind::Stored)
            && !f.is_empty()
        {
            r["fields"] = Value::Object(f);
        }
    }
    (200, r)
}

/// Elasticsearch's `Murmur3HashFunction.hash(String)`: murmur3 x86 32-bit
/// (seed 0) over the string's UTF-16 code units, little-endian.
fn murmur3_routing(s: &str) -> i32 {
    let bytes: Vec<u8> = s.encode_utf16().flat_map(|c| c.to_le_bytes()).collect();
    let (c1, c2) = (0xcc9e2d51u32, 0x1b873593u32);
    let mut h: u32 = 0;
    let (chunks, tail) = bytes.as_chunks::<4>();
    for ch in chunks {
        let mut k = u32::from_le_bytes(*ch);
        k = k.wrapping_mul(c1).rotate_left(15).wrapping_mul(c2);
        h ^= k;
        h = h.rotate_left(13).wrapping_mul(5).wrapping_add(0xe6546b64);
    }
    let mut k: u32 = 0;
    for (n, b) in tail.iter().enumerate() {
        k ^= (*b as u32) << (8 * n);
    }
    if !tail.is_empty() {
        k = k.wrapping_mul(c1).rotate_left(15).wrapping_mul(c2);
        h ^= k;
    }
    h ^= bytes.len() as u32;
    h ^= h >> 16;
    h = h.wrapping_mul(0x85ebca6b);
    h ^= h >> 13;
    h = h.wrapping_mul(0xc2b2ae35);
    h ^= h >> 16;
    h as i32
}

/// The shard a routing value (or, without one, the id) lands on, as
/// Elasticsearch's `IndexRouting` computes it.
fn shard_of(i: &Index, key: &str) -> i64 {
    let (shards, _) = shard_counts(i);
    let shards = shards.max(1) as i64;
    let v = &i.settings["index"]["number_of_routing_shards"];
    let routing_shards =
        v.as_i64().or_else(|| v.as_str().and_then(|x| x.parse().ok())).unwrap_or_else(|| {
            let log2 = 64 - ((shards - 1) as u64).leading_zeros() as i64;
            shards << (10 - log2).max(1)
        });
    let factor = (routing_shards / shards).max(1);
    (murmur3_routing(key) as i64).rem_euclid(routing_shards) / factor
}

impl Index {
    /// The next `_seq_no` for a write routed by `key` (routing or id):
    /// Elasticsearch numbers each shard's operations separately.
    fn next_seq(&mut self, key: &str) -> i64 {
        if shard_counts(self).0 <= 1 {
            self.seq += 1;
            return self.seq;
        }
        let shard = shard_of(self, key);
        let n = self.shard_seq.entry(shard).or_insert(-1);
        *n += 1;
        *n
    }
}

/// Whether a read or write with `routing` reaches the shard holding `id`.
fn routed_visible(i: &Index, id: &str, routing: Option<&str>) -> bool {
    let Some(d) = i.docs.get(id) else { return true };
    if shard_counts(i).0 <= 1 {
        return true;
    }
    shard_of(i, d.routing.as_deref().unwrap_or(id)) == shard_of(i, routing.unwrap_or(id))
}

fn routing_required(i: &Index) -> bool {
    i.mappings.get("_routing").and_then(|r| r.get("required")).and_then(Value::as_bool)
        == Some(true)
}

fn routing_missing(index: &str, id: &str) -> (u16, Value) {
    let reason = format!("routing is required for [{index}]/[{id}]");
    (
        400,
        json!({"error": {"root_cause": [{"type": "routing_missing_exception", "reason": reason, "index_uuid": "_na_", "index": index}],
                         "type": "routing_missing_exception", "reason": reason, "index_uuid": "_na_", "index": index}, "status": 400}),
    )
}

/// The `_shards` header of a write: one primary plus its replicas, of
/// which only the primary is ever assigned on a single node.
fn shards_header(i: &Index) -> Value {
    let (_, r) = shard_counts(i);
    json!({"total": 1 + r, "successful": 1, "failed": 0})
}

/// `version` + `version_type=external|external_gte`: (version, gte).
fn external_version(q: &HashMap<String, String>) -> Option<(i64, bool)> {
    let gte = match q.get("version_type").map(String::as_str) {
        Some("external") => false,
        Some("external_gte") => true,
        _ => return None,
    };
    q.get("version").and_then(|v| v.parse().ok()).map(|v| (v, gte))
}

fn external_conflict(id: &str, current: i64, given: i64) -> Value {
    error(
        "version_conflict_engine_exception",
        &format!(
            "[{id}]: version conflict, current version [{current}] is higher or equal to the one provided [{given}]"
        ),
        409,
    )
}

/// `if_seq_no` / `if_primary_term` compare-and-swap.
fn check_seq_no(
    current: Option<&Document>,
    id: &str,
    q: &HashMap<String, String>,
) -> Result<(), (u16, Value)> {
    let (Some(want_seq), Some(want_term)) = (q.get("if_seq_no"), q.get("if_primary_term")) else {
        return Ok(());
    };
    let cur = current.map(|d| d.seq);
    if want_term != "1" || cur.map(|c| c.to_string()).as_deref() != Some(want_seq.as_str()) {
        let reason = match cur {
            Some(c) => format!(
                "[{id}]: version conflict, required seqNo [{want_seq}], primary term [{want_term}]. current document has seqNo [{c}] and primary term [1]"
            ),
            None => format!(
                "[{id}]: version conflict, required seqNo [{want_seq}], primary term [{want_term}] but no document was found"
            ),
        };
        return Err((409, error("version_conflict_engine_exception", &reason, 409)));
    }
    Ok(())
}

/// Request validation for an index/create write.
fn validate_write(id: &str, kind: &str, q: &HashMap<String, String>) -> Result<(), (u16, Value)> {
    let fail = |m: String| {
        Err((
            400,
            error(
                "action_request_validation_exception",
                &format!("Validation Failed: 1: {m};"),
                400,
            ),
        ))
    };
    if id.len() > 512 {
        return fail(format!(
            "id [{id}] is too long, must be no longer than 512 bytes but was: {}",
            id.len()
        ));
    }
    let create = kind == "_create" || q.get("op_type").is_some_and(|o| o == "create");
    let vt = q.get("version_type").map(String::as_str);
    if create && matches!(vt, Some("external" | "external_gte")) {
        return fail(
            "create operations only support internal versioning. use index instead".into(),
        );
    }
    if q.contains_key("version") && !matches!(vt, Some("external" | "external_gte")) {
        return fail(
            "internal versioning can not be used for optimistic concurrency control. Please use `if_seq_no` and `if_primary_term` instead"
                .into(),
        );
    }
    Ok(())
}

fn require_alias_error(name: &str) -> (u16, Value) {
    let reason = format!(
        "no such index [{name}] and [require_alias] request flag is [true] and [{name}] is not an alias"
    );
    (
        404,
        json!({"error": {"root_cause": [{"type": "index_not_found_exception", "reason": reason,
                                         "index_uuid": "_na_", "index": name}],
                         "type": "index_not_found_exception", "reason": reason,
                         "index_uuid": "_na_", "index": name}, "status": 404}),
    )
}

/// A did-you-mean suggestion for a misspelled field name.
fn did_you_mean(field: &str, known: &[&str]) -> String {
    fn dist(a: &str, b: &str) -> usize {
        let (a, b): (Vec<char>, Vec<char>) = (a.chars().collect(), b.chars().collect());
        let mut prev: Vec<usize> = (0..=b.len()).collect();
        for i in 1..=a.len() {
            let mut cur = vec![i; b.len() + 1];
            for j in 1..=b.len() {
                let c = if a[i - 1] == b[j - 1] { 0 } else { 1 };
                cur[j] = (prev[j] + 1).min(cur[j - 1] + 1).min(prev[j - 1] + c);
            }
            prev = cur;
        }
        prev[b.len()]
    }
    match known.iter().filter(|k| dist(field, k) <= 2).min_by_key(|k| dist(field, k)) {
        Some(k) => format!(" did you mean [{k}]?"),
        None => String::new(),
    }
}

fn version_conflict(index: &str, id: &str, current: i64) -> Value {
    let cause = json!({"type": "version_conflict_engine_exception",
        "reason": format!("[{id}]: version conflict, document already exists (current version [{current}])"),
        "index_uuid": "noida", "shard": "0", "index": index});
    let mut top = cause.clone();
    top["root_cause"] = json!([cause]);
    json!({"error": top, "status": 409})
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

/// `*` wildcard matching for index and alias names.
fn glob_match(pat: &str, name: &str) -> bool {
    let parts: Vec<&str> = pat.split('*').collect();
    if parts.len() == 1 {
        return pat == name;
    }
    let mut rest = name;
    for (i, p) in parts.iter().enumerate() {
        if i == 0 {
            match rest.strip_prefix(p) {
                Some(r) => rest = r,
                None => return false,
            }
        } else if i == parts.len() - 1 {
            return rest.len() >= p.len() && rest.ends_with(p);
        } else if let Some(at) = rest.find(p) {
            rest = &rest[at + p.len()..];
        } else {
            return false;
        }
    }
    true
}

/// The paths left to match below key `k`, `**` matching any depth.
fn filter_path_step(paths: &[Vec<String>], k: &str) -> Vec<Vec<String>> {
    let mut next = vec![];
    for p in paths {
        let Some(head) = p.first() else { continue };
        if head == "**" {
            next.push(p.clone());
            if let Some(after) = p.get(1)
                && glob_match(after, k)
            {
                next.push(p[2..].to_vec());
            }
        } else if glob_match(head, k) {
            next.push(p[1..].to_vec());
        }
    }
    next
}

/// `filter_path` includes: only what some path reaches (arrays don't use
/// a path segment); `None` when nothing is left.
fn filter_path_include(v: &Value, paths: &[Vec<String>]) -> Option<Value> {
    if paths.iter().any(|p| p.is_empty() || (p.len() == 1 && p[0] == "**")) {
        return Some(v.clone());
    }
    match v {
        Value::Object(m) => {
            let out: Map<String, Value> = m
                .iter()
                .filter_map(|(k, x)| {
                    let next = filter_path_step(paths, k);
                    if next.is_empty() {
                        None
                    } else {
                        filter_path_include(x, &next).map(|r| (k.clone(), r))
                    }
                })
                .collect();
            (!out.is_empty()).then_some(Value::Object(out))
        }
        Value::Array(a) => {
            let out: Vec<Value> = a.iter().filter_map(|x| filter_path_include(x, paths)).collect();
            (!out.is_empty()).then_some(Value::Array(out))
        }
        _ => None,
    }
}

/// `filter_path` excludes (`-path`): everything but what a path reaches.
fn filter_path_exclude(v: &Value, paths: &[Vec<String>]) -> Option<Value> {
    if paths.iter().any(|p| p.is_empty()) {
        return None;
    }
    match v {
        Value::Object(m) => Some(Value::Object(
            m.iter()
                .filter_map(|(k, x)| {
                    let next = filter_path_step(paths, k);
                    if next.is_empty() {
                        Some((k.clone(), x.clone()))
                    } else {
                        filter_path_exclude(x, &next).map(|r| (k.clone(), r))
                    }
                })
                .collect(),
        )),
        Value::Array(a) => {
            Some(Value::Array(a.iter().filter_map(|x| filter_path_exclude(x, paths)).collect()))
        }
        other => Some(other.clone()),
    }
}

/// Percent-decodes one URL path segment (`+` stays a plus sign there).
fn percent_decode_segment(seg: &str) -> String {
    if !seg.contains('%') {
        return seg.to_string();
    }
    let b = seg.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%'
            && i + 2 < b.len()
            && let Some(v) = seg.get(i + 1..i + 3).and_then(|h| u8::from_str_radix(h, 16).ok())
        {
            out.push(v);
            i += 3;
            continue;
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// URL query parameters, percent-decoded (`+` is a space); a bare flag
/// (`?v`, `?pretty`, `?refresh`) has an empty value.
fn query_params(q: &str) -> HashMap<String, String> {
    fn decode(s: &str) -> String {
        let b = s.as_bytes();
        let mut out = Vec::with_capacity(b.len());
        let mut i = 0;
        while i < b.len() {
            match b[i] {
                b'+' => out.push(b' '),
                b'%' if i + 2 < b.len() => match u8::from_str_radix(&s[i + 1..i + 3], 16) {
                    Ok(v) => {
                        out.push(v);
                        i += 2;
                    }
                    Err(_) => out.push(b'%'),
                },
                c => out.push(c),
            }
            i += 1;
        }
        String::from_utf8_lossy(&out).into_owned()
    }
    q.split('&')
        .filter(|p| !p.is_empty())
        .map(|p| match p.split_once('=') {
            Some((k, v)) => (decode(k), decode(v)),
            None => (decode(p), String::new()),
        })
        .collect()
}
/// An alias definition as stored: `routing` expands to `index_routing`
/// and `search_routing`; routing values are strings.
pub(super) fn normalize_alias(v: &Value) -> Value {
    let mut v = v.clone();
    if let Some(m) = v.as_object_mut() {
        if let Some(r) = m.remove("routing") {
            m.entry("index_routing").or_insert_with(|| r.clone());
            m.entry("search_routing").or_insert(r);
        }
        for k in ["index_routing", "search_routing"] {
            if let Some(x) = m.get_mut(k)
                && !x.is_string()
            {
                *x = json!(x.to_string());
            }
        }
    }
    v
}

/// A mapping with dotted field names (`"object1.red": {...}`) expanded
/// into object fields, as Elasticsearch stores it -- except under
/// `subobjects: false`, where dotted names are leaf names.
pub(super) fn expand_dotted(m: &Value) -> Value {
    let mut out = m.clone();
    let subobjects = m.get("subobjects").and_then(Value::as_bool).unwrap_or(true);
    if let Some(Value::Object(props)) = m.get("properties") {
        let mut np = json!({});
        for (k, v) in props {
            let v = expand_dotted(v);
            let parts: Vec<&str> =
                if subobjects { k.split('.').collect() } else { vec![k.as_str()] };
            let mut node = json!({});
            // Build {"a": {"properties": {"b": v}}} from the inside out.
            let mut cur = v;
            for p in parts[1..].iter().rev() {
                cur = json!({"properties": {(*p): cur}});
            }
            node[parts[0]] = cur;
            merge(&mut np, node);
        }
        out["properties"] = np;
    }
    out
}

pub(super) fn merge(a: &mut Value, b: Value) {
    // A `dense_vector` definition comes complete (defaults filled in, see
    // `vectors::prepare_mapping`) and replaces the old one.
    if b.get("type").and_then(Value::as_str) == Some("dense_vector") {
        *a = b;
        return;
    }
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
/// The mapping a document is indexed with: dynamic mapping applied, and
/// its vectors checked (only a document Elasticsearch accepts changes it).
fn index_mapping(m: &mut Value, src: &Value, id: &str) -> Result<(), (u16, Value)> {
    let mut next = m.clone();
    dynamic_mapping(&mut next, src);
    vectors::check_source(&mut next, src, id)?;
    *m = next;
    Ok(())
}

fn dynamic_mapping(m: &mut Value, src: &Value) {
    if m.get("properties").is_none() {
        m["properties"] = json!({});
    }
    let props = m["properties"].as_object_mut().unwrap();
    if let Some(fields) = src.as_object() {
        for (k, v) in fields {
            // An object (or an array of them) maps its own fields, as
            // Elasticsearch maps sub-objects: `{"properties": {...}}`.
            let objects: Vec<&Value> = match v {
                Value::Object(_) => vec![v],
                Value::Array(a) => a.iter().filter(|e| e.is_object()).collect(),
                _ => Vec::new(),
            };
            if let Some(existing) = props.get_mut(k) {
                let ty = existing.get("type").and_then(Value::as_str);
                if existing.get("properties").is_some() || matches!(ty, Some("object" | "nested")) {
                    for o in objects {
                        dynamic_mapping(existing, o);
                    }
                }
                continue;
            }
            // No value (null, `[]`) maps nothing yet.
            if v.is_null() || v.as_array().is_some_and(|a| a.iter().all(Value::is_null)) {
                continue;
            }
            if !objects.is_empty() && v.as_array().is_none_or(|a| a[0].is_object()) {
                let mut def = json!({});
                for o in objects {
                    dynamic_mapping(&mut def, o);
                }
                if def["properties"].as_object().is_some_and(Map::is_empty) {
                    def = json!({"type": "object"});
                }
                props.insert(k.clone(), def);
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
            if let Value::Array(a) = v
                && let Some(def) = vectors::dynamic_def(a)
            {
                props.insert(k.clone(), def);
                continue;
            }
            props.insert(k.clone(),if ty=="text"{json!({"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}})}else{json!({"type":ty})});
        }
    }
}

/// A new index's starting state: the defaults, then whatever the
/// matching templates give it (see `templates::Templates::resolve`).
/// Applies to explicit creation and to auto-creation on first write.
fn new_index_from_templates(templates: &Templates, name: &str) -> Index {
    let mut index = Index {
        mappings: json!({"properties": {}}),
        settings: json!({"index": {"number_of_shards": "1", "number_of_replicas": "1"}}),
        opened: true,
        // Writes bump this before using it: the first gets seq_no 0.
        seq: -1,
        ..Index::default()
    };
    if let Some(r) = templates.resolve(name) {
        let mut m = r.mappings;
        // Template mappings were checked when the template was stored;
        // this fills in the vector defaults.
        let _ = vectors::prepare_mapping(&json!({}), &mut m);
        merge(&mut index.mappings, m);
        apply_settings(&mut index.settings, &r.settings);
        index.aliases.extend(r.aliases);
    }
    // The settings Elasticsearch fills in for every new index.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let generated = json!({"index": {
        "routing": {"allocation": {"include": {"_tier_preference": "data_content"}}},
        "provided_name": name,
        "creation_date": now.to_string(),
        "uuid": index_uuid(),
        "version": {"created": "8512000"},
    }});
    let mut settings = generated;
    merge(&mut settings, index.settings.clone());
    index.settings = settings;
    index
}

/// A random-looking 22-character index uuid, as Elasticsearch prints one.
fn index_uuid() -> String {
    use std::hash::{BuildHasher, Hasher};
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::with_capacity(22);
    while out.len() < 22 {
        let mut h = std::collections::hash_map::RandomState::new().build_hasher();
        h.write_usize(out.len());
        let mut v = h.finish();
        for _ in 0..10 {
            if out.len() == 22 {
                break;
            }
            out.push(ALPHABET[(v & 63) as usize] as char);
            v >>= 6;
        }
    }
    out
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

/// Metadata fields `_mapping/field/*` lists.
const META_FIELDS: &[&str] = &[
    "_data_stream_timestamp",
    "_doc_count",
    "_feature",
    "_field_names",
    "_id",
    "_ignored",
    "_ignored_source",
    "_index",
    "_nested_path",
    "_routing",
    "_seq_no",
    "_source",
    "_tier",
    "_version",
];

/// Field types Elasticsearch 8.15 knows.
const FIELD_TYPES: &[&str] = &[
    "text",
    "keyword",
    "long",
    "integer",
    "short",
    "byte",
    "double",
    "float",
    "half_float",
    "scaled_float",
    "unsigned_long",
    "date",
    "date_nanos",
    "boolean",
    "binary",
    "object",
    "nested",
    "flattened",
    "geo_point",
    "geo_shape",
    "point",
    "shape",
    "ip",
    "completion",
    "search_as_you_type",
    "token_count",
    "dense_vector",
    "sparse_vector",
    "rank_feature",
    "rank_features",
    "alias",
    "join",
    "percolator",
    "integer_range",
    "float_range",
    "long_range",
    "double_range",
    "date_range",
    "ip_range",
    "match_only_text",
    "wildcard",
    "constant_keyword",
    "version",
    "histogram",
    "aggregate_metric_double",
    "semantic_text",
    "counted_keyword",
    "passthrough",
];

/// `_cluster/stats` `indices.mappings.field_types`: per mapped field type,
/// how many fields and indices use it (and, for `dense_vector`, the
/// indexed vectors' dimensions).
fn field_type_stats(s: &State) -> Value {
    fn walk(props: &Value, out: &mut Vec<Value>) {
        let Some(p) = props.as_object() else { return };
        for def in p.values() {
            out.push(def.clone());
            if let Some(Value::Object(subs)) = def.get("fields") {
                out.extend(subs.values().cloned());
            }
            if let Some(inner) = def.get("properties") {
                walk(inner, out);
            }
        }
    }
    // type -> (fields, indices, indexed vectors, min dims, max dims)
    let mut stats: std::collections::BTreeMap<String, (u64, u64, u64, i64, i64)> =
        std::collections::BTreeMap::new();
    for i in s.indices.values() {
        let mut defs = Vec::new();
        walk(i.mappings.get("properties").unwrap_or(&Value::Null), &mut defs);
        let mut seen = std::collections::HashSet::new();
        for def in defs {
            let ty = def.get("type").and_then(Value::as_str).unwrap_or("object").to_string();
            let e = stats.entry(ty.clone()).or_insert((0, 0, 0, i64::MAX, i64::MIN));
            e.0 += 1;
            if seen.insert(ty.clone()) {
                e.1 += 1;
            }
            if ty == "dense_vector" && def.get("index") != Some(&json!(false)) {
                e.2 += 1;
                if let Some(d) = def.get("dims").and_then(Value::as_i64) {
                    e.3 = e.3.min(d);
                    e.4 = e.4.max(d);
                }
            }
        }
    }
    Value::Array(
        stats
            .into_iter()
            .map(|(name, (count, indices, vectors, min, max))| {
                if name == "dense_vector" {
                    let (min, max) = if min > max { (-1, -1) } else { (min, max) };
                    json!({"name": name, "count": count, "index_count": indices,
                           "indexed_vector_count": vectors, "indexed_vector_dim_min": min,
                           "indexed_vector_dim_max": max})
                } else {
                    json!({"name": name, "count": count, "index_count": indices, "script_count": 0})
                }
            })
            .collect(),
    )
}

/// The mapping as GET shows it: no `properties` key when there are none.
fn shown_mappings(m: &Value) -> Value {
    match m.as_object() {
        Some(o)
            if o.len() == 1
                && o.get("properties")
                    .is_some_and(|p| p.as_object().is_some_and(Map::is_empty)) =>
        {
            json!({})
        }
        _ => m.clone(),
    }
}

/// Every mapped leaf field as (dotted full name, definition).
fn mapped_leaves(m: &Value, prefix: &str, out: &mut Vec<(String, Value)>) {
    let Some(props) = m.get("properties").and_then(Value::as_object) else { return };
    for (k, def) in props {
        let full = if prefix.is_empty() { k.clone() } else { format!("{prefix}.{k}") };
        if def.get("properties").is_some()
            && def.get("type").is_none_or(|t| t == "object" || t == "nested")
        {
            mapped_leaves(def, &full, out);
        } else {
            let mut d = def.clone();
            if let Some(o) = d.as_object_mut() {
                o.remove("fields");
            }
            out.push((full.clone(), def.clone()));
            if let Some(multi) = def.get("fields").and_then(Value::as_object) {
                for (sub, sd) in multi {
                    out.push((format!("{full}.{sub}"), sd.clone()));
                }
            }
        }
    }
}

/// An incoming mapping checked against the index's current one: known
/// field types, and no type change for an existing field.
fn validate_mapping(current: &Value, incoming: &Value) -> Result<(), (u16, Value)> {
    fn walk(cur: Option<&Value>, inc: &Value, prefix: &str) -> Result<(), (u16, Value)> {
        let Some(props) = inc.get("properties").and_then(Value::as_object) else { return Ok(()) };
        for (k, def) in props {
            let full = if prefix.is_empty() { k.clone() } else { format!("{prefix}.{k}") };
            let existing = cur.and_then(|c| c.get("properties")).and_then(|p| p.get(k));
            if !def.is_object() {
                let class = match def {
                    Value::String(_) => "java.lang.String",
                    Value::Bool(_) => "java.lang.Boolean",
                    Value::Number(n) if n.is_i64() => "java.lang.Integer",
                    Value::Number(_) => "java.lang.Double",
                    Value::Array(_) => "java.util.ArrayList",
                    _ => "null",
                };
                let reason = format!(
                    "Expected map for property [fields] on field [{k}] but got a class {class}"
                );
                return Err((
                    400,
                    error(
                        "mapper_parsing_exception",
                        &format!("Failed to parse mapping: {reason}"),
                        400,
                    ),
                ));
            }
            if let Some(t) = def.get("type").and_then(Value::as_str) {
                if !FIELD_TYPES.contains(&t) {
                    return Err((
                        400,
                        error(
                            "mapper_parsing_exception",
                            &format!(
                                "Failed to parse mapping: No handler for type [{t}] declared on field [{k}]"
                            ),
                            400,
                        ),
                    ));
                }
                let old =
                    existing.and_then(|e| e.get("type")).and_then(Value::as_str).or_else(|| {
                        existing.filter(|e| e.get("properties").is_some()).map(|_| "object")
                    });
                if let Some(o) = old
                    && o != t
                {
                    return Err((
                        400,
                        error(
                            "illegal_argument_exception",
                            &format!("mapper [{full}] cannot be changed from type [{o}] to [{t}]"),
                            400,
                        ),
                    ));
                }
            }
            walk(existing, def, &full)?;
        }
        Ok(())
    }
    walk(Some(current), incoming, "")
}

/// Settings as dotted `index.*` keys with string values.
fn flat_settings(v: &Value) -> Vec<(String, Value)> {
    fn go(prefix: &str, v: &Value, out: &mut Vec<(String, Value)>) {
        match v {
            Value::Object(m) => {
                for (k, x) in m {
                    let key = if prefix.is_empty() { k.clone() } else { format!("{prefix}.{k}") };
                    go(&key, x, out);
                }
            }
            other => out.push((prefix.to_string(), other.clone())),
        }
    }
    let mut out = vec![];
    go("", v, &mut out);
    out.into_iter()
        .map(|(k, v)| (if k.starts_with("index.") { k } else { format!("index.{k}") }, v))
        .collect()
}

/// Dotted settings back into the nested form Elasticsearch returns.
fn nest_settings(flat: &[(String, Value)]) -> Value {
    let mut root = json!({});
    for (k, v) in flat {
        let parts: Vec<&str> = k.split('.').collect();
        let mut node = &mut root;
        for p in &parts[..parts.len() - 1] {
            if !node.get(*p).is_some_and(Value::is_object) {
                node[*p] = json!({});
            }
            node = &mut node[*p];
        }
        node[parts[parts.len() - 1]] = v.clone();
    }
    root
}

/// A settings request with every key under `index`.
fn nested_incoming(req: &Value) -> Value {
    nest_settings(&flat_settings(req))
}

/// Index settings shown under `defaults` with `include_defaults=true`.
const DEFAULT_SETTINGS: &[(&str, &str)] = &[
    ("index.auto_expand_replicas", "false"),
    ("index.blocks.read_only", "false"),
    ("index.blocks.read_only_allow_delete", "false"),
    ("index.blocks.write", "false"),
    ("index.codec", "default"),
    ("index.hidden", "false"),
    ("index.mapping.depth.limit", "20"),
    ("index.mapping.nested_fields.limit", "50"),
    ("index.mapping.nested_objects.limit", "10000"),
    ("index.mapping.total_fields.limit", "1000"),
    ("index.max_docvalue_fields_search", "100"),
    ("index.max_inner_result_window", "100"),
    ("index.max_ngram_diff", "1"),
    ("index.max_regex_length", "1000"),
    ("index.max_rescore_window", "10000"),
    ("index.max_result_window", "10000"),
    ("index.max_script_fields", "32"),
    ("index.max_shingle_diff", "3"),
    ("index.max_terms_count", "65536"),
    ("index.number_of_replicas", "1"),
    ("index.number_of_shards", "1"),
    ("index.refresh_interval", "1s"),
];

/// Top-level `index.*` setting groups Elasticsearch accepts.
const KNOWN_SETTINGS: &[&str] = &[
    "number_of_shards",
    "number_of_replicas",
    "number_of_routing_shards",
    "refresh_interval",
    "max_result_window",
    "max_inner_result_window",
    "max_rescore_window",
    "max_docvalue_fields_search",
    "max_script_fields",
    "max_ngram_diff",
    "max_shingle_diff",
    "max_terms_count",
    "max_regex_length",
    "max_slices_per_scroll",
    "max_refresh_listeners",
    "max_adjacency_matrix_filters",
    "blocks",
    "routing",
    "mapping",
    "analysis",
    "lifecycle",
    "sort",
    "codec",
    "similarity",
    "search",
    "indexing",
    "translog",
    "merge",
    "query",
    "highlight",
    "default_pipeline",
    "final_pipeline",
    "hidden",
    "auto_expand_replicas",
    "unassigned",
    "priority",
    "store",
    "shard",
    "write",
    "gc_deletes",
    "requests",
    "soft_deletes",
    "load_fixed_bitset_filters_eagerly",
    "version",
    "creation_date",
    "uuid",
    "provided_name",
    "routing_partition_size",
    "format",
    "frozen",
    "fast_refresh",
    "mode",
    "time_series",
    "look_ahead_time",
    "look_back_time",
    "dense_vector",
    "default_allocation",
    "verified_before_close",
    "resize",
    "plugin",
    "queries",
    "fielddata",
    "warmer",
    "ccr",
    "xpack",
    "data_path",
    "check_on_startup",
    "compound_format",
    "allocation",
];

/// Settings that can't change on an open index.
const STATIC_SETTINGS: &[&str] = &[
    "index.number_of_shards",
    "index.number_of_routing_shards",
    "index.routing_partition_size",
    "index.codec",
    "index.soft_deletes.enabled",
    "index.store.type",
    "index.mode",
];

/// A settings update: unknown settings and (on an open index) static
/// ones are refused, as Elasticsearch does.
/// Soft deletes replaced translog retention in 8.0: its settings are
/// refused.
fn translog_retention_check(settings: &Value) -> Result<(), (u16, Value)> {
    if flat_settings(settings).iter().any(|(k, v)| {
        let k = k.trim_start_matches("index.");
        (k == "translog.retention.size" || k == "translog.retention.age") && !v.is_null()
    }) {
        return Err((
            400,
            error(
                "illegal_argument_exception",
                "Translog retention settings [index.translog.retention.age] and [index.translog.retention.size] are no longer supported. Please do not specify values for these settings",
                400,
            ),
        ));
    }
    Ok(())
}

fn validate_settings(req: &Value, update: bool) -> Result<(), (u16, Value)> {
    let flat = flat_settings(req);
    for (k, _) in &flat {
        let group = k.trim_start_matches("index.").split('.').next().unwrap_or("");
        if !KNOWN_SETTINGS.contains(&group) {
            return Err((
                400,
                error(
                    "illegal_argument_exception",
                    &format!(
                        "unknown setting [{k}] please check that any required plugins are installed, or check the breaking changes documentation for removed settings"
                    ),
                    400,
                ),
            ));
        }
    }
    translog_retention_check(req)?;
    if update {
        let statics: Vec<&String> = flat
            .iter()
            .map(|(k, _)| k)
            .filter(|k| STATIC_SETTINGS.contains(&k.as_str()) || k.starts_with("index.analysis."))
            .collect();
        if !statics.is_empty() {
            let list = statics.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(", ");
            return Err((
                400,
                error(
                    "illegal_argument_exception",
                    &format!(
                        "Can't update non dynamic settings [[{list}]] for open indices unless the `reopen` query parameter is set to true. Alternatively, close the indices, apply the settings changes, and reopen the indices"
                    ),
                    400,
                ),
            ));
        }
    }
    Ok(())
}
