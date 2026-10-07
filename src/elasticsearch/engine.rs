use serde_json::{Map, Value, json};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use super::analysis;
use super::cat;
use super::dates;
use super::painless;
use super::search::{self, CommittedDoc};

#[derive(Clone)]
pub struct Engine(Arc<Mutex<State>>);

use serde::{Deserialize, Serialize};

use super::templates::Templates;

#[derive(Default, Serialize, Deserialize)]
struct State {
    indices: HashMap<String, Index>,
    #[serde(flatten)]
    templates: Templates,
    /// `PUT _cluster/settings` values (persistent, transient).
    #[serde(default)]
    cluster_settings: HashMap<String, Value>,
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
    /// When the searchable snapshot was last rebuilt, and when the index
    /// was last searched (for `refresh_interval` and search-idle shards).
    #[serde(skip)]
    last_refresh: Option<std::time::Instant>,
    #[serde(skip)]
    last_search: Option<std::time::Instant>,
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
        let (status, mut resp) = self.route(method, path, query, body);
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
        match segments.first() {
            Some(&"_cluster") => return self.cluster_api(method, &segments, &q, body),
            Some(&"_cat") => return self.cat_api(&segments, &q),
            Some(&"_nodes") => return self.nodes_api(),
            Some(&"_stats") => return self.stats_api("*"),
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
            "_refresh" | "_flush" | "_open" | "_close" | "_forcemerge"
                if method == "POST"
                    || (method == "GET" && matches!(segments[1], "_refresh" | "_flush")) =>
            {
                self.index_action(segments[0], segments[1], &q)
            }
            "_cache" if segments.get(2) == Some(&"clear") && method == "POST" => {
                self.index_action(segments[0], "_cache", &q)
            }
            "_search" | "_count" => self.search_or_count(method, segments[1], index_name, &q, body),
            "_pit" if method == "POST" => self.open_pit(index_name, &q),
            "_stats" if method == "GET" => self.stats_api(index_name),
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
            "_update" if segments.len() == 3 => {
                self.update(method, index_name, segments[2], &q, body)
            }
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
        if !ignore_unavailable {
            for (n, part) in expr.split(',').map(str::trim).enumerate() {
                let excluded = n > 0 && part.starts_with('-');
                if part.is_empty() || excluded || part == "_all" || part.contains('*') {
                    continue;
                }
                if Self::resolve_indices(s, part).is_empty() {
                    return Err(missing_index(part));
                }
            }
        }
        let names = Self::resolve_indices(s, expr);
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

    /// Health of `indices` (all when `None`): green when no index wants
    /// replicas, yellow otherwise (a single node can't place them).
    fn health_of(s: &State, names: &[String]) -> Value {
        let pri: usize = names.len();
        let replicas: usize = names
            .iter()
            .filter_map(|n| s.indices.get(n))
            .map(|i| {
                i.settings["index"]["number_of_replicas"]
                    .as_str()
                    .and_then(|r| r.parse().ok())
                    .or_else(|| {
                        i.settings["index"]["number_of_replicas"].as_u64().map(|r| r as usize)
                    })
                    .unwrap_or(1)
            })
            .sum();
        let status = if replicas > 0 { "yellow" } else { "green" };
        let pct =
            if pri + replicas == 0 { 100.0 } else { pri as f64 * 100.0 / (pri + replicas) as f64 };
        json!({
            "cluster_name": "docker-cluster", "status": status, "timed_out": false,
            "number_of_nodes": 1, "number_of_data_nodes": 1,
            "active_primary_shards": pri, "active_shards": pri, "relocating_shards": 0,
            "initializing_shards": 0, "unassigned_shards": replicas,
            "delayed_unassigned_shards": 0, "number_of_pending_tasks": 0,
            "number_of_in_flight_fetch": 0, "task_max_waiting_in_queue_millis": 0,
            "active_shards_percent_as_number": pct,
        })
    }

    /// `_cluster/health[/<indices>]`, `_cluster/settings`, `_cluster/state`
    /// (minimal), `_cluster/stats` (minimal).
    fn cluster_api(
        &self,
        method: &str,
        segments: &[&str],
        q: &HashMap<String, String>,
        body: &[u8],
    ) -> (u16, Value) {
        let mut s = self.0.lock().unwrap();
        match segments.get(1).copied() {
            Some("health") => {
                let names: Vec<String> = match segments.get(2) {
                    Some(p) => {
                        let names = Self::resolve_indices(&s, p);
                        if names.is_empty() {
                            // Elasticsearch waits for the index to appear,
                            // then gives up red.
                            let mut h = Self::health_of(&s, &[]);
                            h["status"] = json!("red");
                            h["timed_out"] = json!(true);
                            return (408, h);
                        }
                        names
                    }
                    None => s.indices.keys().cloned().collect(),
                };
                let mut h = Self::health_of(&s, &names);
                if let Some(want) = q.get("wait_for_status") {
                    let rank = |st: &str| match st {
                        "green" => 0,
                        "yellow" => 1,
                        _ => 2,
                    };
                    if rank(h["status"].as_str().unwrap_or("red")) > rank(want) {
                        h["timed_out"] = json!(true);
                        return (408, h);
                    }
                }
                (200, h)
            }
            Some("settings") => {
                if method == "PUT" {
                    let Some(req) = parse_json(body) else { return (400, malformed_body()) };
                    for key in ["persistent", "transient"] {
                        if let Some(Value::Object(m)) = req.get(key) {
                            let slot = s
                                .cluster_settings
                                .entry(key.to_string())
                                .or_insert_with(|| json!({}));
                            for (k, v) in m {
                                if v.is_null() {
                                    slot.as_object_mut().unwrap().remove(k);
                                } else {
                                    slot[k] = v.clone();
                                }
                            }
                        }
                    }
                    let mut out = json!({"acknowledged": true, "persistent": {}, "transient": {}});
                    for key in ["persistent", "transient"] {
                        if let Some(v) = req.get(key) {
                            out[key] = v.clone();
                        }
                    }
                    return (200, out);
                }
                (
                    200,
                    json!({
                        "persistent": s.cluster_settings.get("persistent").cloned().unwrap_or_else(|| json!({})),
                        "transient": s.cluster_settings.get("transient").cloned().unwrap_or_else(|| json!({})),
                    }),
                )
            }
            Some("state") => (
                200,
                json!({"cluster_name": "docker-cluster", "cluster_uuid": "noida-local",
                       "master_node": "noida", "metadata": {"indices": s.indices.keys().map(|k| (k.clone(), json!({"state": "open"}))).collect::<Map<String, Value>>()}}),
            ),
            Some("stats") => {
                let docs: usize = s.indices.values().map(|i| i.committed.len()).sum();
                (
                    200,
                    json!({"cluster_name": "docker-cluster", "cluster_uuid": "noida-local",
                           "status": Self::health_of(&s, &s.indices.keys().cloned().collect::<Vec<_>>())["status"],
                           "indices": {"count": s.indices.len(), "docs": {"count": docs, "deleted": 0}},
                           "nodes": {"count": {"total": 1, "data": 1, "master": 1}}}),
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
        let size_of = |i: &Index| -> u64 {
            i.docs.values().map(|d| d.source.to_string().len() as u64 + 120).sum()
        };
        match segments.get(1).copied() {
            Some("indices") => {
                let rows = names
                    .iter()
                    .map(|n| {
                        let i = &s.indices[n];
                        let health = Self::health_of(&s, std::slice::from_ref(n))["status"]
                            .as_str()
                            .unwrap_or("green")
                            .to_string();
                        let rep = if health == "yellow" { "1" } else { "0" };
                        let size = cat::human_bytes(size_of(i));
                        vec![
                            health,
                            if i.opened { "open" } else { "close" }.to_string(),
                            n.clone(),
                            format!("noida-{n}"),
                            "1".into(),
                            rep.into(),
                            i.committed.len().to_string(),
                            "0".into(),
                            size.clone(),
                            size.clone(),
                            size,
                        ]
                    })
                    .collect();
                (
                    200,
                    cat::render(
                        &[
                            "health",
                            "status",
                            "index",
                            "uuid",
                            "pri",
                            "rep",
                            "docs.count",
                            "docs.deleted",
                            "store.size",
                            "pri.store.size",
                            "dataset.size",
                        ],
                        &[
                            "pri",
                            "rep",
                            "docs.count",
                            "docs.deleted",
                            "store.size",
                            "pri.store.size",
                            "dataset.size",
                        ],
                        rows,
                        q,
                    ),
                )
            }
            Some("count") => {
                let (epoch, ts) = cat::now_columns();
                let count: usize = names.iter().map(|n| s.indices[n].committed.len()).sum();
                (
                    200,
                    cat::render(
                        &["epoch", "timestamp", "count"],
                        &["epoch", "count"],
                        vec![vec![epoch, ts, count.to_string()]],
                        q,
                    ),
                )
            }
            Some("health") => {
                let (epoch, ts) = cat::now_columns();
                let all: Vec<String> = s.indices.keys().cloned().collect();
                let h = Self::health_of(&s, &all);
                let row = vec![
                    epoch,
                    ts,
                    "docker-cluster".into(),
                    h["status"].as_str().unwrap_or("green").to_string(),
                    "1".into(),
                    "1".into(),
                    h["active_shards"].to_string(),
                    h["active_primary_shards"].to_string(),
                    "0".into(),
                    "0".into(),
                    h["unassigned_shards"].to_string(),
                    "0".into(),
                    "-".into(),
                    format!(
                        "{:.1}%",
                        h["active_shards_percent_as_number"].as_f64().unwrap_or(100.0)
                    ),
                ];
                (
                    200,
                    cat::render(
                        &[
                            "epoch",
                            "timestamp",
                            "cluster",
                            "status",
                            "node.total",
                            "node.data",
                            "shards",
                            "pri",
                            "relo",
                            "init",
                            "unassign",
                            "pending_tasks",
                            "max_task_wait_time",
                            "active_shards_percent",
                        ],
                        &[
                            "node.total",
                            "node.data",
                            "shards",
                            "pri",
                            "relo",
                            "init",
                            "unassign",
                            "pending_tasks",
                            "max_task_wait_time",
                            "active_shards_percent",
                        ],
                        vec![row],
                        q,
                    ),
                )
            }
            Some("aliases") => {
                let mut rows = Vec::new();
                let mut idx: Vec<&String> = s.indices.keys().collect();
                idx.sort();
                for n in idx {
                    let mut aliases: Vec<(&String, &Value)> = s.indices[n].aliases.iter().collect();
                    aliases.sort_by_key(|a| a.0);
                    for (a, spec) in aliases {
                        let w = spec
                            .get("is_write_index")
                            .and_then(Value::as_bool)
                            .map_or("-".to_string(), |b| b.to_string());
                        let f = if spec.get("filter").is_some() { "*" } else { "-" };
                        rows.push(vec![a.clone(), n.clone(), f.into(), "-".into(), "-".into(), w]);
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
            Some("nodes") => (
                200,
                cat::render(
                    &[
                        "ip",
                        "heap.percent",
                        "ram.percent",
                        "cpu",
                        "load_1m",
                        "load_5m",
                        "load_15m",
                        "node.role",
                        "master",
                        "name",
                    ],
                    &["heap.percent", "ram.percent", "cpu", "load_1m", "load_5m", "load_15m"],
                    vec![vec![
                        "127.0.0.1".into(),
                        "10".into(),
                        "50".into(),
                        "1".into(),
                        "0.00".into(),
                        "0.00".into(),
                        "0.00".into(),
                        "cdfhilmrstw".into(),
                        "*".into(),
                        "noida".into(),
                    ]],
                    q,
                ),
            ),
            _ => no_handler("GET", &format!("/{}", segments.join("/"))),
        }
    }

    fn nodes_api(&self) -> (u16, Value) {
        (
            200,
            json!({
                "_nodes": {"total": 1, "successful": 1, "failed": 0},
                "cluster_name": "docker-cluster",
                "nodes": {"noida": {
                    "name": "noida", "transport_address": "127.0.0.1:9300", "host": "127.0.0.1",
                    "ip": "127.0.0.1", "version": "8.15.3", "build_flavor": "default",
                    "build_type": "docker", "roles": ["data", "ingest", "master"],
                    "http": {"publish_address": "127.0.0.1:9200", "bound_address": ["127.0.0.1:9200"]},
                }},
            }),
        )
    }

    /// `/_stats`, `/<index>/_stats`: document counts (of the refreshed view,
    /// as Elasticsearch reports them) and an approximate store size.
    fn stats_api(&self, pattern: &str) -> (u16, Value) {
        let s = self.0.lock().unwrap();
        let names = Self::resolve_indices(&s, pattern);
        if names.is_empty() && pattern != "*" && pattern != "_all" {
            return missing_index(pattern);
        }
        let section = |count: usize, size: u64| {
            json!({"docs": {"count": count, "deleted": 0, "total_size_in_bytes": size},
                   "store": {"size_in_bytes": size, "total_data_set_size_in_bytes": size, "reserved_in_bytes": 0},
                   "indexing": {"index_total": count, "index_current": 0, "delete_total": 0},
                   "search": {"query_total": 0, "query_current": 0}})
        };
        let mut indices = Map::new();
        let (mut all_count, mut all_size) = (0, 0);
        for n in &names {
            let i = &s.indices[n];
            let size: u64 = i.docs.values().map(|d| d.source.to_string().len() as u64 + 120).sum();
            let count = i.committed.len();
            all_count += count;
            all_size += size;
            indices.insert(
                n.clone(),
                json!({"uuid": format!("noida-{n}"), "health": Self::health_of(&s, std::slice::from_ref(n))["status"],
                       "status": "open", "primaries": section(count, size), "total": section(count, size)}),
            );
        }
        let shards = names.len();
        (
            200,
            json!({
                "_shards": {"total": shards * 2, "successful": shards, "failed": 0},
                "_all": {"primaries": section(all_count, all_size), "total": section(all_count, all_size)},
                "indices": indices,
            }),
        )
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
                    merge(&mut index.mappings, expand_dotted(m));
                }
                if let Some(st) = req.get("settings") {
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
                merge(&mut i.mappings, expand_dotted(&next));
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
                let Some(req) = parse_json(body) else { return (400, malformed_body()) };
                let req = match req.get("settings") {
                    Some(inner) if req.as_object().is_some_and(|m| m.len() == 1) => inner.clone(),
                    _ => req,
                };
                apply_settings(&mut i.settings, &req);
                (200, json!({"acknowledged":true}))
            }
            _ => (405, error("method_not_allowed_exception", "Incorrect HTTP method", 405)),
        }
    }

    /// Index-level admin actions over an index expression (one name, a
    /// list, wildcards, `_all`, or none for every index).
    fn index_action(&self, expr: &str, action: &str, q: &HashMap<String, String>) -> (u16, Value) {
        let mut s = self.0.lock().unwrap();
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
            // `_flush`, `_forcemerge`, `_cache/clear`: nothing to do in
            // memory beyond acknowledging every shard.
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
        let mut result = if let Some(d) = i.docs.get(id) {
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
                    i.docs.remove(id);
                    i.order.retain(|x| x != id);
                    i.seq += 1;
                    (
                        200,
                        json!({"_index": index, "_id": id, "_version": version, "result": "deleted",
                               "_shards": {"total": 2, "successful": 1, "failed": 0},
                               "_seq_no": i.seq, "_primary_term": 1}),
                    )
                }
                "index" | "create" => {
                    dynamic_mapping(&mut i.mappings, &new_source);
                    i.seq += 1;
                    let seq = i.seq;
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
            dynamic_mapping(&mut i.mappings, &src);
            i.seq += 1;
            let seq = i.seq;
            i.order.push(id.to_string());
            i.docs.insert(id.to_string(), Document { source: src, version: 1, seq });
            (201, doc_response(index, id, i.docs.get(id).unwrap(), "created"))
        };
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
                let (status, mut res) = self.update("POST", ix, &id, &no_refresh, data);
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
                wrote = true;
                done += 1;
                i.seq += 1;
                if op == "delete" {
                    i.docs.remove(&snap.id);
                    i.order.retain(|x| x != &snap.id);
                } else {
                    dynamic_mapping(&mut i.mappings, &src);
                    let seq = i.seq;
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
}

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
        merge(&mut index.mappings, r.mappings);
        apply_settings(&mut index.settings, &r.settings);
        index.aliases.extend(r.aliases);
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
