//! Snapshot repositories and snapshots: `PUT/GET/DELETE _snapshot/{repo}`
//! (`fs`, `url` and `source` repositories, `_verify`, `_cleanup`), and
//! snapshot create / get / status / delete / clone / restore, with
//! `_cat/snapshots` and `_cat/repositories`.
//!
//! A snapshot is a full copy of each index (settings, mappings, aliases,
//! documents) kept in the engine state, so it persists with the data dir
//! like everything else. Snapshots belong to a repository *location*: a
//! repository removed and registered again over the same location (or
//! two repositories sharing one) sees the same snapshots and uuid.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, HashMap};

use super::lifecycle::validation_failed;
use super::{Engine, Index, State, cat, error, flat_settings, glob_match, index_uuid, parse_json};

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct Repository {
    #[serde(rename = "type")]
    kind: String,
    settings: Value,
}

#[derive(Default, Serialize, Deserialize)]
pub(super) struct RepoData {
    uuid: String,
    snapshots: Vec<Snap>,
}

#[derive(Clone, Serialize, Deserialize)]
struct Snap {
    name: String,
    uuid: String,
    indices: Vec<String>,
    include_global_state: bool,
    #[serde(default)]
    metadata: Option<Value>,
    start: i64,
    end: i64,
    /// Primary shards per index.
    shards: BTreeMap<String, u64>,
    /// Each index as stored (`Index` serialized).
    data: BTreeMap<String, Value>,
    /// Index and component templates, for `include_global_state`.
    #[serde(default)]
    templates: Option<Value>,
}

impl Snap {
    fn total_shards(&self) -> u64 {
        self.shards.values().sum()
    }

    /// Bytes and files of one index's copy (Lucene would write a few
    /// files per shard plus each document).
    fn index_size(&self, index: &str) -> (u64, u64) {
        let shards = self.shards.get(index).copied().unwrap_or(1);
        let docs = self.data.get(index).and_then(|d| d.get("docs")).and_then(Value::as_object);
        let bytes: u64 = docs
            .map(|m| m.values().map(|d| super::lifecycle::doc_size(&d["source"])).sum())
            .unwrap_or(0);
        let files = if docs.is_some_and(|m| !m.is_empty()) { 4 * shards } else { shards };
        (bytes + 225 * shards, files)
    }
}

static SNAP_IDS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// A distinct 22-character uuid (`index_uuid` mixed with a counter, so
/// snapshots taken in the same instant still differ).
fn snapshot_uuid() -> String {
    let n = SNAP_IDS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let mut u = index_uuid();
    u.replace_range(0..4, &format!("{:04x}", n % 0x10000));
    u
}

fn truthy(q: &HashMap<String, String>, k: &str) -> bool {
    q.get(k).is_some_and(|v| v.is_empty() || v == "true")
}

fn repo_missing(name: &str) -> (u16, Value) {
    (404, error("repository_missing_exception", &format!("[{name}] missing"), 404))
}

fn snap_missing(repo: &str, name: &str) -> (u16, Value) {
    (404, error("snapshot_missing_exception", &format!("[{repo}:{name}] is missing"), 404))
}

fn repo_error(status: u16, msg: &str) -> (u16, Value) {
    (status, error("repository_exception", msg, status))
}

/// `200ms`, `1.5s`, `2m`, ... as Elasticsearch prints a `TimeValue`.
fn time_value(ms: i64) -> String {
    let units = [(86_400_000, "d"), (3_600_000, "h"), (60_000, "m"), (1000, "s")];
    for (n, u) in units {
        if ms >= n {
            let v = ms as f64 / n as f64;
            let s = format!("{v:.1}");
            return format!("{}{u}", s.trim_end_matches(".0"));
        }
    }
    if ms == 0 { "0s".into() } else { format!("{ms}ms") }
}

/// Settings as Elasticsearch keeps them: nested, every leaf a string.
fn string_settings(v: &Value) -> Value {
    fn go(prefix: &str, v: &Value, out: &mut Vec<(String, Value)>) {
        match v {
            Value::Object(m) => {
                for (k, x) in m {
                    let key = if prefix.is_empty() { k.clone() } else { format!("{prefix}.{k}") };
                    go(&key, x, out);
                }
            }
            Value::String(_) | Value::Array(_) => out.push((prefix.to_string(), v.clone())),
            other => out.push((prefix.to_string(), json!(other.to_string()))),
        }
    }
    let mut flat = Vec::new();
    go("", v, &mut flat);
    super::nest_settings(&flat)
}

impl Repository {
    /// Where its snapshots live.
    fn location(&self, name: &str) -> String {
        let s = |k: &str| self.settings.get(k).and_then(Value::as_str).unwrap_or("").to_string();
        match self.kind.as_str() {
            "fs" => format!("fs:{}", s("location")),
            "url" => format!("url:{}", s("url")),
            _ => format!("{}:{name}", self.kind),
        }
    }

    fn readonly(&self) -> bool {
        self.kind == "url"
            || ["readonly", "read_only"]
                .iter()
                .any(|k| self.settings.get(*k).and_then(Value::as_str) == Some("true"))
    }
}

/// Snapshot names Elasticsearch accepts.
fn invalid_snapshot_name(repo: &str, name: &str) -> Option<(u16, Value)> {
    let why = if name.is_empty() {
        "cannot be empty"
    } else if name.contains('#') {
        "must not contain '#'"
    } else if name.starts_with('_') {
        "must not start with '_'"
    } else if name != name.to_lowercase() {
        "must be lowercase"
    } else if name
        .chars()
        .any(|c| matches!(c, '\\' | '/' | '*' | '?' | '"' | '<' | '>' | '|' | ' ' | ','))
    {
        "must not contain the following characters [\\, /, *, ?, \", <, >, |,  , ,]"
    } else {
        return None;
    };
    Some((
        400,
        error(
            "invalid_snapshot_name_exception",
            &format!("[{repo}:{name}] Invalid snapshot name [{name}], {why}"),
            400,
        ),
    ))
}

/// Comma-separated names or an array, as `indices` is given.
fn name_list(v: Option<&Value>) -> Vec<String> {
    match v {
        Some(Value::String(s)) => {
            s.split(',').map(str::trim).filter(|p| !p.is_empty()).map(String::from).collect()
        }
        Some(Value::Array(a)) => a.iter().filter_map(Value::as_str).map(String::from).collect(),
        _ => vec![],
    }
}

/// Names matching `patterns` (`*`, `_all`, globs, `-exclusions`).
fn select<'a>(patterns: &[String], names: impl Iterator<Item = &'a String> + Clone) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for (n, p) in patterns.iter().enumerate() {
        if n > 0
            && let Some(ex) = p.strip_prefix('-')
        {
            out.retain(|x| !glob_match(ex, x));
            continue;
        }
        if p == "_all" {
            out.extend(names.clone().cloned());
        } else {
            out.extend(names.clone().filter(|x| glob_match(p, x)).cloned());
        }
    }
    let mut seen = std::collections::HashSet::new();
    out.retain(|x| seen.insert(x.clone()));
    out
}

impl Engine {
    /// Everything under `/_snapshot`.
    pub(super) fn snapshot_api(
        &self,
        method: &str,
        seg: &[&str],
        q: &HashMap<String, String>,
        body: &[u8],
    ) -> (u16, Value) {
        match (method, &seg[1..]) {
            ("GET", []) => self.get_repositories("_all"),
            ("GET", ["_status"]) => (200, json!({"snapshots": []})),
            ("GET", [repo]) => self.get_repositories(repo),
            ("PUT" | "POST", [repo]) => self.put_repository(repo, q, body),
            ("DELETE", [repo]) => self.delete_repository(repo),
            ("POST", [repo, "_verify"]) => self.verify_repository(repo),
            ("POST", [repo, "_cleanup"]) => {
                let s = self.0.lock().unwrap();
                if !s.admin.repositories.contains_key(*repo) {
                    return repo_missing(repo);
                }
                (200, json!({"results": {"deleted_bytes": 0, "deleted_blobs": 0}}))
            }
            ("GET", [repo, "_status"]) => {
                let s = self.0.lock().unwrap();
                if !s.admin.repositories.contains_key(*repo) {
                    return repo_missing(repo);
                }
                (200, json!({"snapshots": []}))
            }
            ("GET", [repo, snap, "_status"]) => self.snapshot_status(repo, snap, q),
            ("GET", [repo, snap]) => self.get_snapshots(repo, snap, q),
            ("PUT" | "POST", [repo, snap]) => self.create_snapshot(repo, snap, q, body),
            ("DELETE", [repo, snap]) => self.delete_snapshots(repo, snap),
            ("PUT", [repo, source, "_clone", target]) => {
                self.clone_snapshot(repo, source, target, body)
            }
            ("POST", [repo, snap, "_restore"]) => self.restore_snapshot(repo, snap, q, body),
            ("POST" | "PUT", [_, _, "_mount"]) => (
                400,
                error(
                    "illegal_argument_exception",
                    "searchable snapshots are not supported by noida",
                    400,
                ),
            ),
            _ => super::no_handler(method, &format!("/{}", seg.join("/"))),
        }
    }

    fn get_repositories(&self, expr: &str) -> (u16, Value) {
        let s = self.0.lock().unwrap();
        let patterns: Vec<String> = expr.split(',').map(|p| p.trim().to_string()).collect();
        let wild = patterns.iter().any(|p| p == "_all" || p.contains('*'));
        let mut out = Map::new();
        for p in &patterns {
            if p != "_all"
                && !p.contains('*')
                && !p.starts_with('-')
                && !s.admin.repositories.contains_key(p)
            {
                return repo_missing(p);
            }
        }
        let names = select(&patterns, s.admin.repositories.keys());
        for n in names {
            let r = &s.admin.repositories[&n];
            let mut o = Map::new();
            o.insert("type".into(), json!(r.kind));
            if let Some(d) = s.admin.repo_data.get(&r.location(&n)) {
                o.insert("uuid".into(), json!(d.uuid));
            }
            o.insert("settings".into(), r.settings.clone());
            out.insert(n, Value::Object(o));
        }
        let _ = wild;
        (200, Value::Object(out))
    }

    fn put_repository(&self, name: &str, q: &HashMap<String, String>, body: &[u8]) -> (u16, Value) {
        let Some(req) = parse_json(body) else { return (400, super::malformed_body()) };
        let Some(kind) = req.get("type").and_then(Value::as_str) else {
            return validation_failed("type is missing");
        };
        let settings = string_settings(req.get("settings").unwrap_or(&json!({})));
        let failed = |inner: &str| {
            (
                500,
                json!({"error": {"root_cause": [{"type": "repository_exception", "reason": format!("[{name}] {inner}")}],
                                 "type": "repository_exception", "reason": format!("[{name}] failed to create repository"),
                                 "caused_by": {"type": "repository_exception", "reason": format!("[{name}] {inner}")}},
                       "status": 500}),
            )
        };
        match kind {
            "fs" => {
                if settings.get("location").and_then(Value::as_str).is_none_or(str::is_empty) {
                    return failed("missing location");
                }
            }
            "url" => {
                if settings.get("url").and_then(Value::as_str).is_none_or(str::is_empty) {
                    return failed("missing url");
                }
            }
            "source" => {}
            "s3" | "gcs" | "azure" | "hdfs" => {
                return (
                    500,
                    error(
                        "repository_verification_exception",
                        &format!("[{name}] path  is not accessible on master node"),
                        500,
                    ),
                );
            }
            other => {
                return repo_error(
                    500,
                    &format!("[{name}] repository type [{other}] does not exist"),
                );
            }
        }
        let _ = q;
        let mut s = self.0.lock().unwrap();
        s.admin
            .repositories
            .insert(name.to_string(), Repository { kind: kind.to_string(), settings });
        (200, json!({"acknowledged": true}))
    }

    fn delete_repository(&self, expr: &str) -> (u16, Value) {
        let mut s = self.0.lock().unwrap();
        let patterns: Vec<String> = expr.split(',').map(|p| p.trim().to_string()).collect();
        let names = select(&patterns, s.admin.repositories.keys());
        if names.is_empty() {
            return repo_missing(expr);
        }
        for n in names {
            s.admin.repositories.remove(&n);
        }
        (200, json!({"acknowledged": true}))
    }

    fn verify_repository(&self, name: &str) -> (u16, Value) {
        let s = self.0.lock().unwrap();
        if !s.admin.repositories.contains_key(name) {
            return repo_missing(name);
        }
        (200, json!({"nodes": {"noida": {"name": "noida"}}}))
    }

    /// `PUT /_snapshot/{repo}/{snapshot}`.
    fn create_snapshot(
        &self,
        repo: &str,
        name: &str,
        q: &HashMap<String, String>,
        body: &[u8],
    ) -> (u16, Value) {
        let req = if body.iter().all(u8::is_ascii_whitespace) {
            json!({})
        } else {
            match parse_json(body) {
                Some(v) => v,
                None => return (400, super::malformed_body()),
            }
        };
        let mut s = self.0.lock().unwrap();
        let Some(r) = s.admin.repositories.get(repo).cloned() else { return repo_missing(repo) };
        if let Some(e) = invalid_snapshot_name(repo, name) {
            return e;
        }
        if r.readonly() {
            return repo_error(
                500,
                &format!("[{repo}] cannot create snapshot in a readonly repository"),
            );
        }
        let loc = r.location(repo);
        if s.admin.repo_data.get(&loc).is_some_and(|d| d.snapshots.iter().any(|x| x.name == name)) {
            return (
                400,
                error(
                    "snapshot_name_already_in_use_exception",
                    &format!(
                        "[{repo}:{name}] Invalid snapshot name [{name}], snapshot with the same name already exists"
                    ),
                    400,
                ),
            );
        }
        let ignore_unavailable =
            req.get("ignore_unavailable").and_then(Value::as_bool) == Some(true);
        let mut patterns = name_list(req.get("indices"));
        if patterns.is_empty() {
            patterns.push("*".into());
        }
        let all: Vec<String> = s.indices.keys().cloned().collect();
        let mut indices = Vec::new();
        for (n, p) in patterns.iter().enumerate() {
            if n > 0
                && let Some(ex) = p.strip_prefix('-')
            {
                indices.retain(|x: &String| !glob_match(ex, x));
                continue;
            }
            if p == "_all" || p.contains('*') {
                indices.extend(all.iter().filter(|x| p == "_all" || glob_match(p, x)).cloned());
            } else if s.indices.contains_key(p) {
                indices.push(p.clone());
            } else {
                let via_alias: Vec<String> = s
                    .indices
                    .iter()
                    .filter(|(_, i)| i.aliases.contains_key(p))
                    .map(|(k, _)| k.clone())
                    .collect();
                if via_alias.is_empty() && !ignore_unavailable {
                    return super::missing_index(p);
                }
                indices.extend(via_alias);
            }
        }
        indices.sort();
        indices.dedup();
        let start = super::super::dates::now_ms();
        let mut snap = Snap {
            name: name.to_string(),
            uuid: snapshot_uuid(),
            indices: indices.clone(),
            include_global_state: req.get("include_global_state").and_then(Value::as_bool)
                != Some(false),
            metadata: req.get("metadata").filter(|m| !m.is_null()).cloned(),
            start,
            end: start,
            shards: BTreeMap::new(),
            data: BTreeMap::new(),
            templates: None,
        };
        for n in &indices {
            let i = &s.indices[n];
            snap.shards.insert(n.clone(), super::shard_counts(i).0);
            if let Ok(v) = serde_json::to_value(i) {
                snap.data.insert(n.clone(), v);
            }
        }
        if snap.include_global_state {
            snap.templates = serde_json::to_value(&s.templates).ok();
        }
        snap.end = super::super::dates::now_ms();
        let data = s
            .admin
            .repo_data
            .entry(loc)
            .or_insert_with(|| RepoData { uuid: index_uuid(), snapshots: vec![] });
        data.snapshots.push(snap.clone());
        if truthy(q, "wait_for_completion") {
            let info = snapshot_info(&snap, repo, &InfoOpts::full());
            (200, json!({"snapshot": info}))
        } else {
            (200, json!({"accepted": true}))
        }
    }

    /// The snapshots `names` selects in `repo`: (repo, snapshot) pairs, or
    /// the error for a missing repository or (unless ignored) snapshot.
    fn find_snapshots(
        s: &State,
        repos: &str,
        names: &str,
        ignore_unavailable: bool,
    ) -> Result<Vec<(String, Snap)>, (u16, Value)> {
        let repo_patterns: Vec<String> = repos.split(',').map(|p| p.trim().to_string()).collect();
        for p in &repo_patterns {
            if p != "_all"
                && !p.contains('*')
                && !p.starts_with('-')
                && !s.admin.repositories.contains_key(p)
            {
                return Err(repo_missing(p));
            }
        }
        let snap_patterns: Vec<String> = names.split(',').map(|p| p.trim().to_string()).collect();
        let mut out = Vec::new();
        for repo in select(&repo_patterns, s.admin.repositories.keys()) {
            let r = &s.admin.repositories[&repo];
            let snaps: &[Snap] =
                s.admin.repo_data.get(&r.location(&repo)).map_or(&[], |d| &d.snapshots[..]);
            let names: Vec<String> = snaps.iter().map(|x| x.name.clone()).collect();
            if !ignore_unavailable {
                for p in &snap_patterns {
                    let literal =
                        p != "_all" && p != "_current" && !p.contains('*') && !p.starts_with('-');
                    if literal && !names.contains(p) {
                        return Err(snap_missing(&repo, p));
                    }
                }
            }
            let patterns: Vec<String> =
                snap_patterns.iter().filter(|p| *p != "_current").cloned().collect();
            for n in select(&patterns, names.iter()) {
                if let Some(x) = snaps.iter().find(|x| x.name == n) {
                    out.push((repo.clone(), x.clone()));
                }
            }
        }
        Ok(out)
    }

    /// `GET /_snapshot/{repo}/{snapshot}`.
    fn get_snapshots(&self, repos: &str, names: &str, q: &HashMap<String, String>) -> (u16, Value) {
        let s = self.0.lock().unwrap();
        let mut found =
            match Self::find_snapshots(&s, repos, names, truthy(q, "ignore_unavailable")) {
                Ok(f) => f,
                Err(e) => return e,
            };
        drop(s);
        let sort = q.get("sort").map_or("start_time", String::as_str);
        let key = |repo: &str, x: &Snap| -> (Value, String) {
            let v = match sort {
                "start_time" => json!(x.start),
                "name" => json!(x.name),
                "duration" => json!(x.end - x.start),
                "index_count" => json!(x.indices.len()),
                "shard_count" => json!(x.total_shards()),
                "failed_shard_count" => json!(0),
                "repository" => json!(repo),
                _ => Value::Null,
            };
            let text = match &v {
                Value::String(t) => t.clone(),
                other => other.to_string(),
            };
            (v, text)
        };
        if !matches!(
            sort,
            "start_time"
                | "name"
                | "duration"
                | "index_count"
                | "shard_count"
                | "failed_shard_count"
                | "repository"
        ) {
            return (
                400,
                error("illegal_argument_exception", &format!("unknown sort key [{sort}]"), 400),
            );
        }
        let cmp = |a: &(String, Snap), b: &(String, Snap)| {
            let (ka, kb) = (key(&a.0, &a.1).0, key(&b.0, &b.1).0);
            let ord = match (ka.as_i64(), kb.as_i64()) {
                (Some(x), Some(y)) => x.cmp(&y),
                _ => ka.as_str().unwrap_or("").cmp(kb.as_str().unwrap_or("")),
            };
            ord.then_with(|| a.1.name.cmp(&b.1.name)).then_with(|| a.0.cmp(&b.0))
        };
        found.sort_by(cmp);
        if q.get("order").is_some_and(|o| o == "desc") {
            found.reverse();
        }
        // `after` (a previous page's `next`) or `offset`, then `size`.
        let total = found.len();
        if let Some(after) = q.get("after") {
            let decoded = base64_decode(after).unwrap_or_default();
            let mut parts = decoded.rsplitn(3, ',');
            let (n, r) = (parts.next().unwrap_or(""), parts.next().unwrap_or(""));
            if let Some(pos) = found.iter().position(|(repo, x)| x.name == n && repo == r) {
                found.drain(..=pos);
            }
        }
        if let Some(off) = q.get("offset").and_then(|o| o.parse::<usize>().ok()) {
            found.drain(..off.min(found.len()));
        }
        let available = found.len();
        let mut next = None;
        if let Some(size) = q.get("size").and_then(|v| v.parse::<i64>().ok()).filter(|n| *n >= 0) {
            let size = size as usize;
            if found.len() > size {
                found.truncate(size);
                if let Some((repo, last)) = found.last() {
                    let k = key(repo, last).1;
                    next = Some(base64_encode(&format!("{k},{repo},{}", last.name)));
                }
            }
        }
        let remaining = available - found.len();
        let opts = InfoOpts {
            verbose: q.get("verbose").is_none_or(|v| v != "false"),
            index_details: truthy(q, "index_details"),
            index_names: q.get("index_names").is_none_or(|v| v != "false"),
            include_repository: q.get("include_repository").is_none_or(|v| v != "false"),
            human: truthy(q, "human"),
        };
        let snaps: Vec<Value> =
            found.iter().map(|(repo, x)| snapshot_info(x, repo, &opts)).collect();
        let mut out = json!({"snapshots": snaps});
        if let Some(n) = next {
            out["next"] = json!(n);
        }
        out["total"] = json!(total);
        out["remaining"] = json!(remaining);
        (200, out)
    }

    /// `GET /_snapshot/{repo}/{snapshot}/_status`.
    fn snapshot_status(
        &self,
        repo: &str,
        names: &str,
        q: &HashMap<String, String>,
    ) -> (u16, Value) {
        let s = self.0.lock().unwrap();
        let found = match Self::find_snapshots(&s, repo, names, truthy(q, "ignore_unavailable")) {
            Ok(f) => f,
            Err(e) => return e,
        };
        let shard_stats = |n: u64| json!({"initializing": 0, "started": 0, "finalizing": 0, "done": n, "failed": 0, "total": n});
        let stats = |files: u64, bytes: u64, start: i64, took: i64| {
            json!({"incremental": {"file_count": files, "size_in_bytes": bytes},
                   "total": {"file_count": files, "size_in_bytes": bytes},
                   "start_time_in_millis": start, "time_in_millis": took})
        };
        let mut out = Vec::new();
        for (repo, x) in &found {
            let mut indices = Map::new();
            let (mut all_files, mut all_bytes) = (0, 0);
            for n in &x.indices {
                let (bytes, files) = x.index_size(n);
                all_files += files;
                all_bytes += bytes;
                let shards = x.shards.get(n).copied().unwrap_or(1);
                let per = |k: u64| -> Value {
                    let (f, b) = if k == 0 {
                        (files - (shards - 1), bytes - 225 * (shards - 1))
                    } else {
                        (1, 225)
                    };
                    json!({"stage": "DONE", "stats": stats(f, b, x.start, 0)})
                };
                let shard_map: Map<String, Value> =
                    (0..shards).map(|k| (k.to_string(), per(k))).collect();
                indices.insert(
                    n.clone(),
                    json!({"shards_stats": shard_stats(shards), "stats": stats(files, bytes, x.start, 0),
                           "shards": shard_map}),
                );
            }
            out.push(json!({
                "snapshot": x.name, "repository": repo, "uuid": x.uuid, "state": "SUCCESS",
                "include_global_state": x.include_global_state,
                "shards_stats": shard_stats(x.total_shards()),
                "stats": stats(all_files, all_bytes, x.start, x.end - x.start),
                "indices": indices,
            }));
        }
        (200, json!({"snapshots": out}))
    }

    /// `DELETE /_snapshot/{repo}/{snapshot[,...]}`.
    fn delete_snapshots(&self, repo: &str, names: &str) -> (u16, Value) {
        let mut s = self.0.lock().unwrap();
        let Some(r) = s.admin.repositories.get(repo).cloned() else { return repo_missing(repo) };
        let found = match Self::find_snapshots(&s, repo, names, false) {
            Ok(f) => f,
            Err(e) => return e,
        };
        if r.readonly() {
            return repo_error(500, &format!("[{repo}] repository is readonly"));
        }
        if let Some(d) = s.admin.repo_data.get_mut(&r.location(repo)) {
            d.snapshots.retain(|x| !found.iter().any(|(_, f)| f.name == x.name));
        }
        (200, json!({"acknowledged": true}))
    }

    /// `PUT /_snapshot/{repo}/{source}/_clone/{target}`.
    fn clone_snapshot(&self, repo: &str, source: &str, target: &str, body: &[u8]) -> (u16, Value) {
        let req = parse_json(body).unwrap_or_else(|| json!({}));
        let patterns = name_list(req.get("indices"));
        if patterns.is_empty() {
            return validation_failed("indices patterns are empty");
        }
        let mut s = self.0.lock().unwrap();
        let Some(r) = s.admin.repositories.get(repo).cloned() else { return repo_missing(repo) };
        if let Some(e) = invalid_snapshot_name(repo, target) {
            return e;
        }
        let loc = r.location(repo);
        let snaps: &[Snap] = s.admin.repo_data.get(&loc).map_or(&[], |d| &d.snapshots[..]);
        let Some(src) = snaps.iter().find(|x| x.name == source).cloned() else {
            return snap_missing(repo, source);
        };
        if snaps.iter().any(|x| x.name == target) {
            return (
                400,
                error(
                    "snapshot_name_already_in_use_exception",
                    &format!(
                        "[{repo}:{target}] Invalid snapshot name [{target}], snapshot with the same name already exists"
                    ),
                    400,
                ),
            );
        }
        let chosen = select(&patterns, src.indices.iter());
        if chosen.is_empty() {
            let p = patterns.join(",");
            return super::missing_index(&p);
        }
        let start = super::super::dates::now_ms();
        let copy = Snap {
            name: target.to_string(),
            uuid: snapshot_uuid(),
            indices: chosen.clone(),
            include_global_state: true,
            metadata: Some(json!({})),
            start,
            end: start,
            shards: src
                .shards
                .iter()
                .filter(|(k, _)| chosen.contains(k))
                .map(|(k, v)| (k.clone(), *v))
                .collect(),
            data: src
                .data
                .iter()
                .filter(|(k, _)| chosen.contains(k))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            templates: None,
        };
        if let Some(d) = s.admin.repo_data.get_mut(&loc) {
            d.snapshots.push(copy);
        }
        (200, json!({"acknowledged": true}))
    }

    /// `POST /_snapshot/{repo}/{snapshot}/_restore`.
    fn restore_snapshot(
        &self,
        repo: &str,
        name: &str,
        q: &HashMap<String, String>,
        body: &[u8],
    ) -> (u16, Value) {
        let req = if body.iter().all(u8::is_ascii_whitespace) {
            json!({})
        } else {
            match parse_json(body) {
                Some(v) => v,
                None => return (400, super::malformed_body()),
            }
        };
        let mut s = self.0.lock().unwrap();
        let Some(r) = s.admin.repositories.get(repo).cloned() else { return repo_missing(repo) };
        let loc = r.location(repo);
        let Some(snap) = s
            .admin
            .repo_data
            .get(&loc)
            .and_then(|d| d.snapshots.iter().find(|x| x.name == name))
            .cloned()
        else {
            return (
                500,
                error(
                    "snapshot_restore_exception",
                    &format!("[{repo}:{name}] snapshot does not exist"),
                    500,
                ),
            );
        };
        let ignore_unavailable =
            req.get("ignore_unavailable").and_then(Value::as_bool) == Some(true);
        let mut patterns = name_list(req.get("indices"));
        if patterns.is_empty() {
            patterns.push("*".into());
        }
        for p in &patterns {
            if !p.contains('*')
                && p != "_all"
                && !p.starts_with('-')
                && !snap.indices.contains(p)
                && !ignore_unavailable
            {
                return (
                    500,
                    error(
                        "snapshot_restore_exception",
                        &format!(
                            "[{repo}:{name}/{}] cannot restore index [{p}] because it cannot be resolved",
                            snap.uuid
                        ),
                        500,
                    ),
                );
            }
        }
        let chosen = select(&patterns, snap.indices.iter());
        let rename = match (
            req.get("rename_pattern").and_then(Value::as_str),
            req.get("rename_replacement").and_then(Value::as_str),
        ) {
            (Some(p), Some(r)) => match regex_lite::Regex::new(p) {
                Ok(re) => Some((re, r.to_string())),
                Err(e) => return (400, error("illegal_argument_exception", &e.to_string(), 400)),
            },
            _ => None,
        };
        let target_of = |n: &str| match &rename {
            Some((re, rep)) => re.replace_all(n, rep.as_str()).into_owned(),
            None => n.to_string(),
        };
        // Every target must be free (or a closed index, which is replaced).
        for n in &chosen {
            let t = target_of(n);
            if let Some(e) = super::lifecycle::invalid_index_name(&t) {
                return e;
            }
            if s.indices.get(&t).is_some_and(|i| i.opened) {
                return (
                    500,
                    error(
                        "snapshot_restore_exception",
                        &format!(
                            "[{repo}:{name}/{}] cannot restore index [{t}] because an open index with same name already exists in the cluster. Either close or delete the existing index or restore the index under a different name by providing a rename pattern and replacement name",
                            snap.uuid
                        ),
                        500,
                    ),
                );
            }
        }
        let include_aliases = req.get("include_aliases").and_then(Value::as_bool) != Some(false);
        let ignore: Vec<String> = name_list(req.get("ignore_index_settings"));
        let mut restored = Vec::new();
        let mut shards = 0;
        for n in &chosen {
            let Some(mut index) =
                snap.data.get(n).cloned().and_then(|v| serde_json::from_value::<Index>(v).ok())
            else {
                continue;
            };
            let t = target_of(n);
            if !ignore.is_empty() {
                let kept: Vec<(String, Value)> = flat_settings(&index.settings)
                    .into_iter()
                    .filter(|(k, _)| {
                        !ignore.iter().any(|p| {
                            let p = if p.starts_with("index.") {
                                p.clone()
                            } else {
                                format!("index.{p}")
                            };
                            glob_match(&p, k)
                        })
                    })
                    .collect();
                index.settings = super::nest_settings(&kept);
            }
            if let Some(extra) = req.get("index_settings") {
                super::apply_settings(&mut index.settings, extra);
            }
            super::apply_settings(
                &mut index.settings,
                &json!({"index": {"uuid": index_uuid(), "provided_name": t}}),
            );
            if !include_aliases {
                index.aliases.clear();
            }
            index.opened = true;
            index.refresh(&t);
            shards += super::shard_counts(&index).0;
            s.indices.insert(t.clone(), index);
            restored.push(t);
        }
        if req.get("include_global_state").and_then(Value::as_bool) == Some(true)
            && let Some(t) =
                snap.templates.as_ref().and_then(|t| serde_json::from_value(t.clone()).ok())
        {
            s.templates = t;
        }
        if truthy(q, "wait_for_completion") {
            (
                200,
                json!({"snapshot": {"snapshot": name, "indices": restored,
                        "shards": {"total": shards, "failed": 0, "successful": shards}}}),
            )
        } else {
            (200, json!({"accepted": true}))
        }
    }

    /// `GET /_cat/snapshots[/{repo}]`.
    pub(super) fn cat_snapshots(
        &self,
        repo: Option<&str>,
        q: &HashMap<String, String>,
    ) -> (u16, Value) {
        let all = [
            "id",
            "repository",
            "status",
            "start_epoch",
            "start_time",
            "end_epoch",
            "end_time",
            "duration",
            "indices",
            "successful_shards",
            "failed_shards",
            "total_shards",
            "reason",
        ];
        let numeric = [
            "start_epoch",
            "end_epoch",
            "indices",
            "successful_shards",
            "failed_shards",
            "total_shards",
        ];
        let s = self.0.lock().unwrap();
        let found = match Self::find_snapshots(&s, repo.unwrap_or("_all"), "_all", false) {
            Ok(f) => f,
            Err(e) => return e,
        };
        let hms = |ms: i64| {
            let t = (ms / 1000).rem_euclid(86_400);
            format!("{:02}:{:02}:{:02}", t / 3600, t / 60 % 60, t % 60)
        };
        let rows: Vec<Vec<String>> = found
            .iter()
            .map(|(repo, x)| {
                let n = x.total_shards().to_string();
                vec![
                    x.name.clone(),
                    repo.clone(),
                    "SUCCESS".into(),
                    (x.start / 1000).to_string(),
                    hms(x.start),
                    (x.end / 1000).to_string(),
                    hms(x.end),
                    time_value(x.end - x.start),
                    x.indices.len().to_string(),
                    n.clone(),
                    "0".into(),
                    n,
                    String::new(),
                ]
            })
            .collect();
        // `reason` shows only when asked for.
        let wants_reason = q.contains_key("help")
            || q.get("h").is_some_and(|h| h.split(',').any(|c| c.trim() == "reason"));
        let columns: Vec<&str> =
            all.iter().copied().filter(|c| wants_reason || *c != "reason").collect();
        let rows = rows.into_iter().map(|mut r| {
            r.truncate(columns.len());
            r
        });
        (200, cat::render(&columns, &numeric, rows.collect(), q))
    }

    /// `GET /_cat/repositories`.
    pub(super) fn cat_repositories(&self, q: &HashMap<String, String>) -> (u16, Value) {
        let s = self.0.lock().unwrap();
        let rows: Vec<Vec<String>> =
            s.admin.repositories.iter().map(|(n, r)| vec![n.clone(), r.kind.clone()]).collect();
        (200, cat::render(&["id", "type"], &[], rows, q))
    }
}

struct InfoOpts {
    verbose: bool,
    index_details: bool,
    index_names: bool,
    include_repository: bool,
    human: bool,
}

impl InfoOpts {
    fn full() -> Self {
        Self {
            verbose: true,
            index_details: false,
            index_names: true,
            include_repository: true,
            human: false,
        }
    }
}

/// One snapshot as `GET _snapshot` (and a completed create) shows it.
fn snapshot_info(x: &Snap, repo: &str, o: &InfoOpts) -> Value {
    let mut m = Map::new();
    m.insert("snapshot".into(), json!(x.name));
    m.insert("uuid".into(), json!(x.uuid));
    if o.include_repository {
        m.insert("repository".into(), json!(repo));
    }
    if o.verbose {
        m.insert("version_id".into(), json!(8512000));
        m.insert("version".into(), json!("8.15.0-8.15.3"));
    }
    if o.index_names {
        m.insert("indices".into(), json!(x.indices));
    }
    if o.verbose && o.index_details {
        let details: Map<String, Value> = x
            .indices
            .iter()
            .map(|n| {
                let (bytes, _) = x.index_size(n);
                let mut d = json!({"shard_count": x.shards.get(n).copied().unwrap_or(1)});
                if o.human {
                    d["size"] = json!(cat::human_bytes(bytes));
                }
                d["size_in_bytes"] = json!(bytes);
                d["max_segments_per_shard"] = json!(1);
                (n.clone(), d)
            })
            .collect();
        m.insert("index_details".into(), Value::Object(details));
    }
    m.insert("data_streams".into(), json!([]));
    if !o.verbose {
        m.insert("state".into(), json!("SUCCESS"));
        return Value::Object(m);
    }
    m.insert("include_global_state".into(), json!(x.include_global_state));
    if let Some(md) = &x.metadata {
        m.insert("metadata".into(), md.clone());
    }
    m.insert("state".into(), json!("SUCCESS"));
    let date = |ms: i64| super::super::dates::format(ms, None, 0);
    m.insert("start_time".into(), json!(date(x.start)));
    m.insert("start_time_in_millis".into(), json!(x.start));
    m.insert("end_time".into(), json!(date(x.end)));
    m.insert("end_time_in_millis".into(), json!(x.end));
    if o.human {
        m.insert("duration".into(), json!(time_value(x.end - x.start)));
    }
    m.insert("duration_in_millis".into(), json!(x.end - x.start));
    m.insert("failures".into(), json!([]));
    let n = x.total_shards();
    m.insert("shards".into(), json!({"total": n, "failed": 0, "successful": n}));
    m.insert("feature_states".into(), json!([]));
    Value::Object(m)
}

const B64: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn base64_encode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = String::new();
    for chunk in b.chunks(3) {
        let n = (chunk[0] as u32) << 16
            | (chunk.get(1).copied().unwrap_or(0) as u32) << 8
            | chunk.get(2).copied().unwrap_or(0) as u32;
        for k in 0..4 {
            if k <= chunk.len() {
                out.push(B64[(n >> (18 - 6 * k) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

fn base64_decode(s: &str) -> Option<String> {
    let mut bits = 0u32;
    let mut n = 0;
    let mut out = Vec::new();
    for c in s.bytes().filter(|c| *c != b'=') {
        let v = B64.iter().position(|x| *x == c)? as u32;
        bits = bits << 6 | v;
        n += 6;
        if n >= 8 {
            n -= 8;
            out.push((bits >> n & 0xff) as u8);
        }
    }
    String::from_utf8(out).ok()
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
    fn snapshot_create_get_restore_delete() {
        let e = Engine::default();
        let repo = json!({"type": "fs", "settings": {"location": "loc"}});
        assert_eq!(call(&e, "PUT", "/_snapshot/r", repo).0, 200);
        let (_, r) = call(&e, "GET", "/_snapshot/r", Value::Null);
        assert_eq!(r, json!({"r": {"type": "fs", "settings": {"location": "loc"}}}));
        call(&e, "PUT", "/idx/_doc/1?refresh=true", json!({"a": 1}));
        let (st, r) = call(&e, "PUT", "/_snapshot/r/s1?wait_for_completion=true", Value::Null);
        assert_eq!(st, 200);
        assert_eq!(r["snapshot"]["state"], json!("SUCCESS"));
        assert_eq!(r["snapshot"]["shards"]["successful"], json!(1));
        let (st, r) = call(&e, "PUT", "/_snapshot/r/s1", Value::Null);
        assert_eq!(
            (st, r["error"]["type"].clone()),
            (400, json!("snapshot_name_already_in_use_exception"))
        );
        let (_, r) = call(&e, "GET", "/_snapshot/r/s1?verbose=false", Value::Null);
        assert_eq!(r["snapshots"][0]["indices"], json!(["idx"]));
        assert!(r["snapshots"][0].get("version").is_none());
        let (st, _) = call(&e, "POST", "/_snapshot/r/s1/_restore", Value::Null);
        assert_eq!(st, 500);
        call(&e, "DELETE", "/idx", Value::Null);
        let (st, r) =
            call(&e, "POST", "/_snapshot/r/s1/_restore?wait_for_completion=true", Value::Null);
        assert_eq!(st, 200);
        assert_eq!(r["snapshot"]["indices"], json!(["idx"]));
        let (_, d) = call(&e, "GET", "/idx/_doc/1", Value::Null);
        assert_eq!(d["_source"], json!({"a": 1}));
        assert_eq!(call(&e, "DELETE", "/_snapshot/r/s1", Value::Null).0, 200);
        assert_eq!(call(&e, "GET", "/_snapshot/r/s1", Value::Null).0, 404);
        // The repository's uuid stays with its location.
        let (_, r) = call(&e, "GET", "/_snapshot", Value::Null);
        assert!(r["r"]["uuid"].is_string());
    }

    #[test]
    fn base64_round_trips() {
        let s = "1791611052855,lc-repo1,lc-snap1";
        assert_eq!(super::base64_encode(s), "MTc5MTYxMTA1Mjg1NSxsYy1yZXBvMSxsYy1zbmFwMQ==");
        assert_eq!(super::base64_decode(&super::base64_encode(s)).unwrap(), s);
        assert_eq!(super::time_value(200), "200ms");
        assert_eq!(super::time_value(1500), "1.5s");
    }
}
