//! Index lifecycle and admin APIs: rollover, resize (`_split`, `_shrink`,
//! `_clone`), index blocks (`_block` and `index.blocks.*` enforcement),
//! `_resolve/index` and `_resolve/cluster`, plus small admin endpoints
//! (script contexts and languages, `_migration/system_features`, flush
//! and force-merge parameter checks). Snapshots, ingest pipelines and
//! stored scripts live in their own modules; this one routes to them.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, HashMap};

use super::snapshots::{RepoData, Repository};
use super::{
    Engine, Index, State, error, flat_settings, glob_match, index_uuid, missing_index, parse_json,
    shard_of,
};

/// Cluster-level state outside the indices: snapshot repositories (and
/// the snapshots stored at each repository location), ingest pipelines
/// and stored scripts.
#[derive(Default, Serialize, Deserialize)]
pub(super) struct Admin {
    #[serde(default)]
    pub(super) repositories: BTreeMap<String, Repository>,
    /// Snapshots by repository location: two repositories registered on
    /// the same location see the same snapshots, as on a shared disk.
    #[serde(default)]
    pub(super) repo_data: BTreeMap<String, RepoData>,
    #[serde(default)]
    pub(super) pipelines: BTreeMap<String, Value>,
    #[serde(default)]
    pub(super) scripts: BTreeMap<String, Value>,
}

static TASK_IDS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

impl Engine {
    /// The APIs this module (and its siblings) answer, or `None` to let
    /// the main router carry on. Index-block checks for reads and
    /// metadata changes happen here too.
    pub(super) fn lifecycle_route(
        &self,
        method: &str,
        seg: &[&str],
        q: &HashMap<String, String>,
        body: &[u8],
    ) -> Option<(u16, Value)> {
        let r = match seg {
            ["_snapshot", ..] => self.snapshot_api(method, seg, q, body),
            ["_cat", "snapshots"] => self.cat_snapshots(None, q),
            ["_cat", "snapshots", repo] => self.cat_snapshots(Some(repo), q),
            ["_cat", "repositories"] => self.cat_repositories(q),
            ["_ingest", ..] => self.ingest_api(method, seg, q, body),
            ["_scripts", ..] => self.scripts_api(method, seg, body),
            ["_script_context"] if method == "GET" => {
                (200, serde_json::from_str(include_str!("script_context.json")).unwrap())
            }
            ["_script_language"] if method == "GET" => {
                (200, serde_json::from_str(include_str!("script_language.json")).unwrap())
            }
            ["_migration", "system_features"] => match method {
                "GET" => {
                    (200, serde_json::from_str(include_str!("migration_features.json")).unwrap())
                }
                "POST" => (
                    200,
                    json!({"accepted": false, "reason": "No system indices require migration"}),
                ),
                _ => return None,
            },
            ["_resolve", "index", name] if method == "GET" => self.resolve_index(name, q),
            ["_resolve", "cluster", name] if method == "GET" => self.resolve_cluster(name, q),
            ["_reindex"] => self.reindex(method, q, body),
            [alias, "_rollover"] => self.rollover(method, alias, None, q, body),
            [alias, "_rollover", new] => self.rollover(method, alias, Some(new), q, body),
            [src, op @ ("_split" | "_shrink" | "_clone"), target] => {
                self.resize(method, op, src, target, body)
            }
            [expr, "_block", block] => self.add_block(method, expr, block, q),
            _ => return self.admin_checks(method, seg, q, body),
        };
        Some(r)
    }

    /// Parameter checks for `_forcemerge`/`_flush`, an asynchronous force
    /// merge, and index blocks on reads and metadata operations.
    fn admin_checks(
        &self,
        method: &str,
        seg: &[&str],
        q: &HashMap<String, String>,
        body: &[u8],
    ) -> Option<(u16, Value)> {
        let (expr, action) = match seg {
            [a @ ("_forcemerge" | "_flush")] => ("_all", Some(*a)),
            [e, a @ ("_forcemerge" | "_flush")] => (*e, Some(*a)),
            _ => ("", None),
        };
        let truthy = |k: &str| q.get(k).is_some_and(|v| v.is_empty() || v == "true");
        match action {
            Some("_forcemerge") if matches!(method, "POST" | "GET") => {
                if truthy("only_expunge_deletes") && q.contains_key("max_num_segments") {
                    return Some(validation_failed(
                        "cannot set only_expunge_deletes and max_num_segments at the same time, those two parameters are mutually exclusive",
                    ));
                }
                if q.get("wait_for_completion").is_some_and(|v| v == "false") {
                    let (status, resp) = self.index_action(expr, "_forcemerge", q);
                    if status >= 400 {
                        return Some((status, resp));
                    }
                    let n = TASK_IDS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    return Some((200, json!({"task": format!("noida:{n}")})));
                }
            }
            Some("_flush") => {
                if truthy("force") && q.get("wait_if_ongoing").is_some_and(|v| v == "false") {
                    return Some(validation_failed(
                        "wait_if_ongoing must be true for a force flush",
                    ));
                }
            }
            _ => {}
        }
        self.block_check(method, seg, body)
    }
}

/// An `action_request_validation_exception` with one message.
pub(super) fn validation_failed(msg: &str) -> (u16, Value) {
    (
        400,
        error("action_request_validation_exception", &format!("Validation Failed: 1: {msg};"), 400),
    )
}

/// An `x_content_parse_exception` for an unknown request body field.
fn unknown_field(parser: &str, field: &str) -> (u16, Value) {
    (
        400,
        error(
            "x_content_parse_exception",
            &format!("[1:2] [{parser}] unknown field [{field}]"),
            400,
        ),
    )
}

/// Elasticsearch's index name checks, with its messages.
pub(super) fn invalid_index_name(name: &str) -> Option<(u16, Value)> {
    let why = if name != name.to_lowercase() {
        "must be lowercase".to_string()
    } else if name.starts_with(['_', '-', '+']) {
        "must not start with '_', '-', or '+'".to_string()
    } else if name.contains('#') {
        "must not contain '#'".to_string()
    } else if name.contains(':') {
        "must not contain ':'".to_string()
    } else if name
        .chars()
        .any(|c| matches!(c, '\\' | '/' | '*' | '?' | '"' | '<' | '>' | '|' | ' ' | ','))
    {
        "must not contain the following characters ['\\',' ','\"','<','*','?','>','|',',','/']"
            .to_string()
    } else if name.is_empty() || name == "." || name == ".." {
        "must not be '.' or '..'".to_string()
    } else if name.len() > 255 {
        "index name is too long, (> 255)".to_string()
    } else {
        return None;
    };
    let reason = format!("Invalid index name [{name}], {why}");
    Some((
        400,
        json!({"error": {"root_cause": [{"type": "invalid_index_name_exception", "reason": reason, "index_uuid": "_na_", "index": name}],
                         "type": "invalid_index_name_exception", "reason": reason, "index_uuid": "_na_", "index": name},
               "status": 400}),
    ))
}

/// `resource_already_exists_exception` for an existing index.
pub(super) fn index_exists(s: &State, name: &str) -> (u16, Value) {
    let uuid = s
        .indices
        .get(name)
        .and_then(|i| i.settings["index"]["uuid"].as_str().map(str::to_string))
        .unwrap_or_else(|| "_na_".into());
    let reason = format!("index [{name}/{uuid}] already exists");
    (
        400,
        json!({"error": {"root_cause": [{"type": "resource_already_exists_exception", "reason": reason, "index_uuid": uuid, "index": name}],
                         "type": "resource_already_exists_exception", "reason": reason, "index_uuid": uuid, "index": name},
               "status": 400}),
    )
}

/// An index setting read as a boolean (`"true"` or `true`).
fn setting_true(i: &Index, path: &[&str]) -> bool {
    let mut v = &i.settings["index"];
    for p in path {
        v = &v[*p];
    }
    v.as_bool() == Some(true) || v.as_str() == Some("true")
}

/// The number-valued index setting `key` (stored as a string).
fn setting_num(i: &Index, key: &str) -> Option<i64> {
    let v = &i.settings["index"][key];
    v.as_i64().or_else(|| v.as_str().and_then(|x| x.parse().ok()))
}

// ---------------------------------------------------------------------
// Index blocks
// ---------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq)]
enum Level {
    Read,
    Write,
    MetadataRead,
    MetadataWrite,
}

/// Each `index.blocks.*` setting: (name, block text, retryable (429),
/// levels it blocks). Listed in the order Elasticsearch names them.
const BLOCKS: &[(&str, &str, bool, &[Level])] = &[
    ("write", "FORBIDDEN/8/index write (api)", false, &[Level::Write]),
    (
        "read_only",
        "FORBIDDEN/5/index read-only (api)",
        false,
        &[Level::Write, Level::MetadataWrite],
    ),
    ("read", "FORBIDDEN/7/index read (api)", false, &[Level::Read]),
    (
        "metadata",
        "FORBIDDEN/9/index metadata (api)",
        false,
        &[Level::MetadataRead, Level::MetadataWrite],
    ),
    (
        "read_only_allow_delete",
        "TOO_MANY_REQUESTS/12/disk usage exceeded flood-stage watermark, index has read-only-allow-delete block",
        true,
        &[Level::Write],
    ),
];

/// The `cluster_block_exception` for `names` at `level`, if any is
/// blocked.
fn blocked(s: &State, names: &[String], level: Level) -> Option<(u16, Value)> {
    let mut reason = String::new();
    let mut retryable = true;
    for n in names {
        let Some(i) = s.indices.get(n) else { continue };
        let hit: Vec<&(&str, &str, bool, &[Level])> = BLOCKS
            .iter()
            .filter(|(name, _, _, levels)| {
                levels.contains(&level) && setting_true(i, &["blocks", name])
            })
            .collect();
        if hit.is_empty() {
            continue;
        }
        retryable &= hit.iter().all(|b| b.2);
        let list = hit.iter().map(|b| b.1).collect::<Vec<_>>().join(", ");
        reason.push_str(&format!("index [{n}] blocked by: [{list}];"));
    }
    if reason.is_empty() {
        return None;
    }
    let status = if retryable { 429 } else { 403 };
    Some((status, error("cluster_block_exception", &reason, status)))
}

/// A document write (index, create, update, delete) on a blocked index.
pub(super) fn write_blocked(s: &State, index: &str) -> Option<(u16, Value)> {
    blocked(s, &[index.to_string()], Level::Write)
}

impl Engine {
    /// Reads, metadata reads and metadata changes on indices whose
    /// `index.blocks.*` settings forbid them.
    fn block_check(&self, method: &str, seg: &[&str], body: &[u8]) -> Option<(u16, Value)> {
        let read = matches!(method, "GET" | "HEAD");
        let (expr, level) = match seg {
            [] => return None,
            ["_search" | "_count" | "_msearch" | "_field_caps", ..] => ("*", Level::Read),
            [first, ..] if first.starts_with('_') => return None,
            [e] => (*e, if read { Level::MetadataRead } else { Level::MetadataWrite }),
            [e, "_mapping" | "_alias" | "_aliases", ..] => {
                (*e, if read { Level::MetadataRead } else { Level::MetadataWrite })
            }
            [e, "_settings", ..] => {
                if read {
                    (*e, Level::MetadataRead)
                } else {
                    // Changing only `index.blocks.*` is always allowed (it
                    // is how a block is lifted).
                    let only_blocks = parse_json(body).is_some_and(|req| {
                        let req = req.get("settings").cloned().unwrap_or(req);
                        let flat = flat_settings(&req);
                        !flat.is_empty()
                            && flat
                                .iter()
                                .all(|(k, _)| k.trim_start_matches("index.").starts_with("blocks."))
                    });
                    if only_blocks {
                        return None;
                    }
                    (*e, Level::MetadataWrite)
                }
            }
            [e, "_close" | "_open"] => (*e, Level::MetadataWrite),
            [
                e,
                "_search" | "_count" | "_msearch" | "_mget" | "_explain" | "_termvectors"
                | "_field_caps" | "_delete_by_query" | "_update_by_query" | "_knn_search"
                | "_validate" | "_mtermvectors",
                ..,
            ] => (*e, Level::Read),
            [e, "_doc" | "_source", _] if read => (*e, Level::Read),
            _ => return None,
        };
        let s = self.0.lock().unwrap();
        let names = Self::resolve_indices(&s, expr);
        blocked(&s, &names, level)
    }

    /// `PUT /{index}/_block/{block}`: sets `index.blocks.{block}`.
    fn add_block(
        &self,
        method: &str,
        expr: &str,
        block: &str,
        q: &HashMap<String, String>,
    ) -> (u16, Value) {
        if method != "PUT" {
            return (405, error("method_not_allowed_exception", "Incorrect HTTP method", 405));
        }
        if !matches!(block, "metadata" | "read" | "read_only" | "write") {
            return (
                400,
                error(
                    "illegal_argument_exception",
                    &format!("No block found with name {block}"),
                    400,
                ),
            );
        }
        let mut s = self.0.lock().unwrap();
        if let Err(e) = super::destructive_check(&s, expr) {
            return e;
        }
        let q = super::with_default_expand(q, "open");
        let names = match Self::resolve_targets(&s, expr, &q) {
            Ok(n) => n,
            Err(e) => return e,
        };
        if names.is_empty() {
            return missing_index(expr);
        }
        let mut out = Vec::new();
        for n in &names {
            if let Some(i) = s.indices.get_mut(n) {
                super::apply_settings(
                    &mut i.settings,
                    &json!({"index": {"blocks": {(block): true}}}),
                );
                out.push(json!({"name": n, "blocked": true}));
            }
        }
        (200, json!({"acknowledged": true, "shards_acknowledged": true, "indices": out}))
    }
}

// ---------------------------------------------------------------------
// Rollover
// ---------------------------------------------------------------------

const MAX_CONDITIONS: &[&str] =
    &["max_age", "max_docs", "max_size", "max_primary_shard_size", "max_primary_shard_docs"];
const MIN_CONDITIONS: &[&str] =
    &["min_age", "min_docs", "min_size", "min_primary_shard_size", "min_primary_shard_docs"];

/// `1b`, `10kb`, `1.5gb`, ... as bytes.
pub(super) fn parse_bytes(s: &str) -> Option<u64> {
    let t = s.trim().to_ascii_lowercase();
    let split = t.find(|c: char| !(c.is_ascii_digit() || c == '.'))?;
    let (num, unit) = t.split_at(split);
    let n: f64 = num.parse().ok()?;
    let mult: f64 = match unit.trim() {
        "b" => 1.0,
        "k" | "kb" => 1024.0,
        "m" | "mb" => 1024.0 * 1024.0,
        "g" | "gb" => 1024.0 * 1024.0 * 1024.0,
        "t" | "tb" => 1024f64.powi(4),
        "p" | "pb" => 1024f64.powi(5),
        _ => return None,
    };
    Some((n * mult) as u64)
}

/// `0s`, `7d`, `500ms`, `1h`, ... as milliseconds.
pub(super) fn parse_time(s: &str) -> Option<i64> {
    let t = s.trim();
    let split = t.find(|c: char| !(c.is_ascii_digit() || c == '.'))?;
    let (num, unit) = t.split_at(split);
    let n: f64 = num.parse().ok()?;
    let ms = match unit {
        "nanos" => n / 1_000_000.0,
        "micros" => n / 1000.0,
        "ms" => n,
        "s" => n * 1000.0,
        "m" => n * 60_000.0,
        "h" => n * 3_600_000.0,
        "d" => n * 86_400_000.0,
        _ => return None,
    };
    Some(ms as i64)
}

/// Document count and source size of an index's refreshed documents,
/// in total and for its largest primary shard.
struct DocStats {
    docs: u64,
    size: u64,
    shard_docs: u64,
    shard_size: u64,
}

pub(super) fn doc_size(source: &Value) -> u64 {
    source.to_string().len() as u64 + 120
}

fn doc_stats(i: &Index) -> DocStats {
    let mut per_shard: HashMap<i64, (u64, u64)> = HashMap::new();
    let (mut docs, mut size) = (0, 0);
    for d in &i.committed {
        let routing = i.docs.get(&d.id).and_then(|x| x.routing.clone());
        let shard = shard_of(i, routing.as_deref().unwrap_or(&d.id));
        let sz = doc_size(&d.source);
        let e = per_shard.entry(shard).or_default();
        e.0 += 1;
        e.1 += sz;
        docs += 1;
        size += sz;
    }
    DocStats {
        docs,
        size,
        shard_docs: per_shard.values().map(|v| v.0).max().unwrap_or(0),
        shard_size: per_shard.values().map(|v| v.1).max().unwrap_or(0),
    }
}

/// A rollover condition, parsed: its name, its text (`[max_docs: 1]`
/// uses it as given) and its threshold.
struct Condition {
    name: String,
    text: String,
    value: i64,
}

fn parse_conditions(c: &Value) -> Result<Vec<Condition>, (u16, Value)> {
    let Some(m) = c.as_object() else {
        return Err((
            400,
            error(
                "x_content_parse_exception",
                "[rollover] conditions doesn't support values of type: VALUE_STRING",
                400,
            ),
        ));
    };
    let mut out = Vec::new();
    for (k, v) in m {
        if !MAX_CONDITIONS.contains(&k.as_str()) && !MIN_CONDITIONS.contains(&k.as_str()) {
            let inner = format!("[1:2] [rollover_conditions] unknown field [{k}]");
            return Err((
                400,
                json!({"error": {"root_cause": [{"type": "x_content_parse_exception", "reason": inner}],
                                 "type": "x_content_parse_exception",
                                 "reason": "[1:2] [rollover] failed to parse field [conditions]",
                                 "caused_by": {"type": "x_content_parse_exception", "reason": inner}},
                       "status": 400}),
            ));
        }
        let text = match v {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        let value = if k.ends_with("_docs") {
            v.as_i64().or_else(|| v.as_str().and_then(|s| s.trim().parse().ok()))
        } else if k.ends_with("_age") {
            v.as_str().and_then(parse_time)
        } else {
            v.as_str().and_then(parse_bytes).map(|b| b as i64)
        };
        let Some(value) = value else {
            let inner = format!("[1:2] [rollover_conditions] failed to parse field [{k}]");
            return Err((
                400,
                json!({"error": {"root_cause": [{"type": "x_content_parse_exception", "reason": inner}],
                                 "type": "x_content_parse_exception",
                                 "reason": "[1:2] [rollover] failed to parse field [conditions]",
                                 "caused_by": {"type": "x_content_parse_exception", "reason": inner,
                                               "caused_by": {"type": "illegal_argument_exception",
                                                             "reason": format!("failed to parse [{text}]")}}},
                       "status": 400}),
            ));
        };
        out.push(Condition { name: k.clone(), text, value });
    }
    Ok(out)
}

/// The next name in a rollover series: `logs-1` -> `logs-000002`.
fn next_rollover_name(old: &str) -> Result<String, (u16, Value)> {
    let bad = || {
        (
            400,
            error(
                "illegal_argument_exception",
                &format!("index name [{old}] does not match pattern '^.*-\\d+$'"),
                400,
            ),
        )
    };
    let (prefix, num) = old.rsplit_once('-').ok_or_else(bad)?;
    if num.is_empty() || !num.chars().all(|c| c.is_ascii_digit()) {
        return Err(bad());
    }
    let n: u64 = num.parse().map_err(|_| bad())?;
    Ok(format!("{prefix}-{:06}", n + 1))
}

impl Engine {
    /// `POST /{alias}/_rollover[/{new_index}]`: a new write index for an
    /// alias once its conditions are met.
    fn rollover(
        &self,
        method: &str,
        alias: &str,
        new_name: Option<&str>,
        q: &HashMap<String, String>,
        body: &[u8],
    ) -> (u16, Value) {
        if method != "POST" {
            return (405, error("method_not_allowed_exception", "Incorrect HTTP method", 405));
        }
        let req = if body.iter().all(u8::is_ascii_whitespace) {
            json!({})
        } else {
            match parse_json(body) {
                Some(v) if v.is_object() => v,
                _ => return (400, super::malformed_body()),
            }
        };
        if let Some(k) = req.as_object().and_then(|m| {
            m.keys()
                .find(|k| !matches!(k.as_str(), "conditions" | "settings" | "mappings" | "aliases"))
        }) {
            return unknown_field("rollover", k);
        }
        let conditions = match req.get("conditions") {
            Some(c) => match parse_conditions(c) {
                Ok(c) => c,
                Err(e) => return e,
            },
            None => vec![],
        };
        let has_max = conditions.iter().any(|c| c.name.starts_with("max_"));
        if !has_max && conditions.iter().any(|c| c.name.starts_with("min_")) {
            return validation_failed(
                "at least one max_* rollover condition must be set when using min_* conditions",
            );
        }
        let dry_run = q.get("dry_run").is_some_and(|v| v.is_empty() || v == "true");
        if q.get("lazy").is_some_and(|v| v.is_empty() || v == "true") {
            return (
                400,
                error(
                    "illegal_argument_exception",
                    "Lazy rollover can be applied only on a data stream. Please remove the query parameter 'lazy'.",
                    400,
                ),
            );
        }
        let (old, new, results, explicit, hidden) = {
            let mut s = self.0.lock().unwrap();
            if s.indices.contains_key(alias) {
                return (
                    400,
                    error(
                        "illegal_argument_exception",
                        "rollover target is a [concrete index] but one of [alias,data_stream] was expected",
                        400,
                    ),
                );
            }
            let mut with: Vec<&String> = s
                .indices
                .iter()
                .filter(|(_, i)| i.aliases.contains_key(alias))
                .map(|(n, _)| n)
                .collect();
            if with.is_empty() {
                return (
                    400,
                    error(
                        "illegal_argument_exception",
                        &format!("rollover target [{alias}] does not exist"),
                        400,
                    ),
                );
            }
            with.sort();
            let flag = |n: &str| {
                s.indices[n].aliases[alias].get("is_write_index").and_then(Value::as_bool)
            };
            let old = match with.iter().find(|n| flag(n) == Some(true)) {
                Some(n) => (*n).clone(),
                None if with.len() == 1 && flag(with[0]) != Some(false) => with[0].clone(),
                None => {
                    return (
                        400,
                        error(
                            "illegal_argument_exception",
                            &format!("rollover target [{alias}] does not point to a write index"),
                            400,
                        ),
                    );
                }
            };
            let spec = s.indices[&old].aliases[alias].clone();
            let explicit = spec.get("is_write_index").and_then(Value::as_bool) == Some(true);
            let hidden = spec.get("is_hidden").and_then(Value::as_bool);
            let new = match new_name {
                Some(n) => n.to_string(),
                None => match next_rollover_name(&old) {
                    Ok(n) => n,
                    Err(e) => return e,
                },
            };
            if let Some(e) = invalid_index_name(&new) {
                return e;
            }
            if s.indices.contains_key(&new) {
                return index_exists(&s, &new);
            }
            let i = s.indices.get_mut(&old).unwrap();
            i.auto_refresh(&old);
            let stats = doc_stats(i);
            let age = super::super::dates::now_ms()
                - i.settings["index"]["creation_date"]
                    .as_str()
                    .and_then(|c| c.parse::<i64>().ok())
                    .unwrap_or(0);
            let results: Vec<(String, bool, bool)> = conditions
                .iter()
                .map(|c| {
                    let have = match c.name.split_once('_').map_or("", |x| x.1) {
                        "age" => age,
                        "docs" => stats.docs as i64,
                        "size" => stats.size as i64,
                        "primary_shard_size" => stats.shard_size as i64,
                        "primary_shard_docs" => stats.shard_docs as i64,
                        _ => 0,
                    };
                    (
                        format!("[{}: {}]", c.name, c.text),
                        have >= c.value,
                        c.name.starts_with("max_"),
                    )
                })
                .collect();
            (old, new, results, explicit, hidden)
        };
        let met = results.is_empty()
            || (results.iter().filter(|r| !r.2).all(|r| r.1) && results.iter().any(|r| r.2 && r.1));
        let conds: Map<String, Value> = results.iter().map(|r| (r.0.clone(), json!(r.1))).collect();
        let response = |rolled: bool| {
            json!({"acknowledged": rolled, "shards_acknowledged": rolled, "old_index": old,
                   "new_index": new, "rolled_over": rolled, "dry_run": dry_run, "lazy": false,
                   "conditions": conds})
        };
        if dry_run || !met {
            return (200, response(false));
        }
        let mut create = Map::new();
        for k in ["settings", "mappings", "aliases"] {
            if let Some(v) = req.get(k) {
                create.insert(k.into(), v.clone());
            }
        }
        let (status, resp) =
            self.index_api("PUT", &new, Value::Object(create).to_string().as_bytes());
        if status >= 300 {
            return (status, resp);
        }
        let mut s = self.0.lock().unwrap();
        let mut spec = json!({});
        if let Some(h) = hidden {
            spec["is_hidden"] = json!(h);
        }
        if explicit {
            let mut old_spec = spec.clone();
            old_spec["is_write_index"] = json!(false);
            spec["is_write_index"] = json!(true);
            if let Some(i) = s.indices.get_mut(&old) {
                i.aliases.insert(alias.to_string(), old_spec);
            }
        } else if let Some(i) = s.indices.get_mut(&old) {
            i.aliases.remove(alias);
        }
        if let Some(i) = s.indices.get_mut(&new) {
            i.aliases.insert(alias.to_string(), spec);
        }
        (200, response(true))
    }
}

// ---------------------------------------------------------------------
// Resize: split, shrink, clone
// ---------------------------------------------------------------------

/// The routing shards an index hashes into: `index.number_of_routing_shards`,
/// or Elasticsearch's default for its shard count.
fn routing_shards(shards: i64, explicit: Option<i64>) -> i64 {
    explicit.unwrap_or_else(|| {
        let log2 = 64 - ((shards.max(1) - 1) as u64).leading_zeros() as i64;
        shards << (10 - log2).max(1)
    })
}

/// Settings a resize never copies from its source.
const NOT_COPIED: &[&str] = &[
    "index.number_of_shards",
    "index.number_of_replicas",
    "index.uuid",
    "index.creation_date",
    "index.provided_name",
    "index.hidden",
];

impl Engine {
    /// `PUT|POST /{source}/_split|_shrink|_clone/{target}`.
    fn resize(&self, method: &str, op: &str, src: &str, target: &str, body: &[u8]) -> (u16, Value) {
        if !matches!(method, "PUT" | "POST") {
            return (405, error("method_not_allowed_exception", "Incorrect HTTP method", 405));
        }
        let req = if body.iter().all(u8::is_ascii_whitespace) {
            json!({})
        } else {
            match parse_json(body) {
                Some(v) if v.is_object() => v,
                _ => return (400, super::malformed_body()),
            }
        };
        let allowed: &[&str] = if op == "_shrink" {
            &["settings", "aliases", "max_primary_shard_size"]
        } else {
            &["settings", "aliases"]
        };
        if let Some(k) =
            req.as_object().and_then(|m| m.keys().find(|k| !allowed.contains(&k.as_str())))
        {
            return unknown_field("resize_request", k);
        }
        let req_settings = req.get("settings").cloned().unwrap_or_else(|| json!({}));
        let flat: Vec<(String, Value)> = flat_settings(&req_settings)
            .into_iter()
            .map(|(k, v)| (if k.starts_with("index.") { k } else { format!("index.{k}") }, v))
            .collect();
        let num = |key: &str| {
            flat.iter()
                .find(|(k, _)| k == key)
                .and_then(|(_, v)| v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
        };
        let wanted = num("index.number_of_shards");
        if op == "_split" && wanted.is_none() {
            return validation_failed("index.number_of_shards is required for split operations");
        }
        let mut s = self.0.lock().unwrap();
        let Some(source) = s.indices.get(src) else { return missing_index(src) };
        if let Some(e) = invalid_index_name(target) {
            return e;
        }
        if s.indices.contains_key(target) {
            return index_exists(&s, target);
        }
        if flat.iter().any(|(k, _)| k == "index.number_of_routing_shards") {
            return (
                400,
                error(
                    "illegal_argument_exception",
                    "cannot provide index.number_of_routing_shards on resize",
                    400,
                ),
            );
        }
        if let Err(e) = super::validate_settings(&req_settings, false) {
            return e;
        }
        let have = setting_num(source, "number_of_shards").unwrap_or(1);
        let explicit_routing = setting_num(source, "number_of_routing_shards");
        let iae = |m: String| (400, error("illegal_argument_exception", &m, 400));
        let shards = match op {
            "_split" => {
                let t = wanted.unwrap_or(have);
                if have > t {
                    return iae(format!(
                        "the number of source shards [{have}] must be less that the number of target shards [{t}]"
                    ));
                }
                if t % have != 0 {
                    return iae(format!(
                        "the number of source shards [{have}] must be a factor of [{t}]"
                    ));
                }
                // A one-shard source can split into any count; otherwise
                // the target must divide the source's routing shards.
                if have > 1 {
                    let r = routing_shards(have, explicit_routing);
                    if r % t != 0 {
                        return (
                            500,
                            error(
                                "illegal_state_exception",
                                &format!(
                                    "the number of routing shards [{r}] must be a multiple of the target shards [{t}]"
                                ),
                                500,
                            ),
                        );
                    }
                }
                t
            }
            "_shrink" => {
                let t = match (wanted, req.get("max_primary_shard_size").and_then(Value::as_str)) {
                    (Some(_), Some(_)) => {
                        return iae(
                            "Cannot set both index.number_of_shards and max_primary_shard_size for the target index"
                                .into(),
                        );
                    }
                    (Some(t), None) => t,
                    (None, Some(max)) => {
                        let max = parse_bytes(max).unwrap_or(u64::MAX).max(1);
                        let size = doc_stats(source).size;
                        (1..=have)
                            .find(|t| have % t == 0 && size.div_ceil(*t as u64) <= max)
                            .unwrap_or(have)
                    }
                    (None, None) => 1,
                };
                if t > have {
                    return iae(format!(
                        "the number of target shards [{t}] must be less that the number of source shards [{have}]"
                    ));
                }
                if t < 1 || have % t != 0 {
                    return iae(format!(
                        "the number of source shards [{have}] must be a must be a multiple of [{t}]"
                    ));
                }
                t
            }
            _ => {
                let t = wanted.unwrap_or(have);
                if t != have {
                    return iae(format!(
                        "the number of target shards ({t}) must be the same as the number of  source shards ( {have})"
                    ));
                }
                t
            }
        };
        let write_blocked = ["write", "read_only", "read_only_allow_delete"]
            .iter()
            .any(|b| setting_true(source, &["blocks", b]));
        if !write_blocked {
            return (
                500,
                error(
                    "illegal_state_exception",
                    &format!(
                        "index {src} must be read-only to resize index. use \"index.blocks.write=true\""
                    ),
                    500,
                ),
            );
        }
        let Ok(copy) = serde_json::to_value(source).and_then(serde_json::from_value::<Index>)
        else {
            return (500, error("exception", "failed to copy the source index", 500));
        };
        let mut index = copy;
        // Settings: the source's (less a few), then the request's.
        let kept: Vec<(String, Value)> = flat_settings(&index.settings)
            .into_iter()
            .filter(|(k, _)| !NOT_COPIED.contains(&k.as_str()))
            .collect();
        let mut settings = super::nest_settings(&kept);
        let now = super::super::dates::now_ms();
        super::apply_settings(
            &mut settings,
            &json!({"index": {"number_of_shards": shards, "number_of_replicas": 1,
                              "uuid": index_uuid(), "creation_date": now, "provided_name": target}}),
        );
        if settings["index"].get("routing_partition_size").is_none() {
            super::apply_settings(&mut settings, &json!({"index": {"routing_partition_size": 1}}));
        }
        super::apply_settings(&mut settings, &req_settings);
        index.settings = settings;
        index.aliases = req
            .get("aliases")
            .and_then(Value::as_object)
            .map(|a| a.iter().map(|(k, v)| (k.clone(), super::normalize_alias(v))).collect())
            .unwrap_or_default();
        index.opened = true;
        // Each shard numbers its operations from where its documents left off.
        index.shard_seq.clear();
        if shards > 1 {
            let mut per: HashMap<i64, i64> = HashMap::new();
            for (id, d) in &index.docs {
                let shard = shard_of(&index, d.routing.as_deref().unwrap_or(id));
                let e = per.entry(shard).or_insert(-1);
                *e = (*e).max(d.seq);
            }
            index.shard_seq = per;
        }
        index.refresh(target);
        s.indices.insert(target.to_string(), index);
        (200, json!({"acknowledged": true, "shards_acknowledged": true, "index": target}))
    }
}

// ---------------------------------------------------------------------
// _resolve/index and _resolve/cluster
// ---------------------------------------------------------------------

/// System indices (hidden, and flagged `system` by `_resolve/index`).
fn is_system(name: &str) -> bool {
    matches!(name, ".tasks" | ".security" | ".security-7" | ".kibana" | ".async-search")
        || name.starts_with(".kibana_")
        || name.starts_with(".security-")
}

fn is_hidden(name: &str, i: &Index) -> bool {
    setting_true(i, &["hidden"]) || is_system(name)
}

/// What an index expression resolves to: (indices, aliases).
type Resolved = (Vec<String>, Vec<String>);

impl Engine {
    fn resolve_expression(
        s: &State,
        expr: &str,
        q: &HashMap<String, String>,
    ) -> Result<Resolved, (u16, Value)> {
        let ew = q.get("expand_wildcards").map_or("open", String::as_str);
        let has = |w: &str| ew.split(',').any(|x| x.trim() == w || x.trim() == "all");
        let (open, closed, hidden) = (has("open"), has("closed"), has("hidden"));
        let ignore_unavailable = q.get("ignore_unavailable").is_some_and(|v| v == "true");
        let mut aliases_all: Vec<String> =
            s.indices.values().flat_map(|i| i.aliases.keys().cloned()).collect();
        aliases_all.sort();
        aliases_all.dedup();
        let (mut indices, mut aliases): (Vec<String>, Vec<String>) = (vec![], vec![]);
        for (n, part) in expr.split(',').map(str::trim).enumerate() {
            if part.is_empty() {
                continue;
            }
            if n > 0
                && let Some(ex) = part.strip_prefix('-')
            {
                indices.retain(|k| !glob_match(ex, k));
                aliases.retain(|k| !glob_match(ex, k));
                continue;
            }
            let part = if part == "_all" { "*" } else { part };
            if part.contains('*') {
                for (name, i) in &s.indices {
                    if !glob_match(part, name) {
                        continue;
                    }
                    // A hidden index needs `hidden`, or a pattern that
                    // starts with a dot for a dot-prefixed one.
                    if is_hidden(name, i)
                        && !hidden
                        && !(part.starts_with('.') && name.starts_with('.'))
                    {
                        continue;
                    }
                    if name.starts_with('.') && !part.starts_with('.') && !hidden {
                        continue;
                    }
                    if (i.opened && open) || (!i.opened && closed) {
                        indices.push(name.clone());
                    }
                }
                aliases.extend(aliases_all.iter().filter(|a| glob_match(part, a)).cloned());
            } else if s.indices.contains_key(part) {
                indices.push(part.to_string());
            } else if aliases_all.iter().any(|a| a == part) {
                aliases.push(part.to_string());
            } else if !ignore_unavailable {
                return Err(missing_index(part));
            }
        }
        indices.sort();
        indices.dedup();
        aliases.sort();
        aliases.dedup();
        Ok((indices, aliases))
    }

    /// `GET /_resolve/index/{name}`.
    fn resolve_index(&self, expr: &str, q: &HashMap<String, String>) -> (u16, Value) {
        let s = self.0.lock().unwrap();
        let (indices, aliases) = match Self::resolve_expression(&s, expr, q) {
            Ok(r) => r,
            Err(e) => return e,
        };
        let indices: Vec<Value> = indices
            .iter()
            .map(|n| {
                let i = &s.indices[n];
                let mut names: Vec<&String> = i.aliases.keys().collect();
                names.sort();
                let mut attrs = vec![];
                if is_hidden(n, i) {
                    attrs.push("hidden");
                }
                attrs.push(if i.opened { "open" } else { "closed" });
                if is_system(n) {
                    attrs.push("system");
                }
                let mut o = json!({"name": n});
                if !names.is_empty() {
                    o["aliases"] = json!(names);
                }
                o["attributes"] = json!(attrs);
                o
            })
            .collect();
        let aliases: Vec<Value> = aliases
            .iter()
            .map(|a| {
                let mut on: Vec<&String> = s
                    .indices
                    .iter()
                    .filter(|(_, i)| i.aliases.contains_key(a))
                    .map(|(n, _)| n)
                    .collect();
                on.sort();
                json!({"name": a, "indices": on})
            })
            .collect();
        (200, json!({"indices": indices, "aliases": aliases, "data_streams": []}))
    }

    /// `GET /_resolve/cluster/{name}`: only the local cluster exists.
    fn resolve_cluster(&self, expr: &str, q: &HashMap<String, String>) -> (u16, Value) {
        if let Some(remote) = expr.split(',').find_map(|p| p.split_once(':').map(|x| x.0)) {
            return (
                400,
                error(
                    "no_such_remote_cluster_exception",
                    &format!("no such remote cluster: [{remote}]"),
                    400,
                ),
            );
        }
        let s = self.0.lock().unwrap();
        let mut local = json!({"connected": true, "skip_unavailable": false});
        match Self::resolve_expression(&s, expr, q) {
            Ok((indices, aliases)) => {
                let any_open = indices.iter().any(|n| s.indices.get(n).is_some_and(|i| i.opened));
                local["matching_indices"] = json!(any_open || !aliases.is_empty());
                local["version"] = json!({"number": "8.15.3", "build_flavor": "default",
                    "minimum_wire_compatibility_version": "7.17.0",
                    "minimum_index_compatibility_version": "7.0.0"});
            }
            Err((_, e)) => {
                local["error"] = e["error"]["reason"].clone();
            }
        }
        (200, json!({"(local)": local}))
    }
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
    fn rollover_moves_the_alias_once_conditions_are_met() {
        let e = Engine::default();
        call(&e, "PUT", "/logs-1", json!({"aliases": {"logs": {}}}));
        let (st, r) = call(&e, "POST", "/logs/_rollover", json!({"conditions": {"max_docs": 1}}));
        assert_eq!(st, 200);
        assert_eq!(r["rolled_over"], json!(false));
        assert_eq!(r["new_index"], json!("logs-000002"));
        assert_eq!(r["conditions"]["[max_docs: 1]"], json!(false));
        call(&e, "PUT", "/logs-1/_doc/1?refresh=true", json!({"a": 1}));
        let (_, r) = call(&e, "POST", "/logs/_rollover", json!({"conditions": {"max_docs": 1}}));
        assert_eq!(r["rolled_over"], json!(true));
        let (_, a) = call(&e, "GET", "/_alias/logs", Value::Null);
        assert_eq!(a, json!({"logs-000002": {"aliases": {"logs": {}}}}));
        let (st, _) = call(&e, "POST", "/logs/_rollover", json!({"conditions": {"min_docs": 1}}));
        assert_eq!(st, 400);
    }

    #[test]
    fn rollover_with_a_write_index_keeps_the_old_index_readable() {
        let e = Engine::default();
        call(&e, "PUT", "/w-000001", json!({"aliases": {"w": {"is_write_index": true}}}));
        let (_, r) = call(&e, "POST", "/w/_rollover", Value::Null);
        assert_eq!(r["rolled_over"], json!(true));
        let (_, a) = call(&e, "GET", "/_alias/w", Value::Null);
        assert_eq!(a["w-000001"]["aliases"]["w"]["is_write_index"], json!(false));
        assert_eq!(a["w-000002"]["aliases"]["w"]["is_write_index"], json!(true));
    }

    #[test]
    fn split_requires_a_write_block_and_copies_documents() {
        let e = Engine::default();
        call(
            &e,
            "PUT",
            "/src",
            json!({"settings": {"number_of_shards": 2, "number_of_routing_shards": 4}}),
        );
        call(&e, "PUT", "/src/_doc/1", json!({"foo": "x"}));
        let body = json!({"settings": {"index.number_of_shards": 4}});
        let (st, r) = call(&e, "PUT", "/src/_split/dst", body.clone());
        assert_eq!((st, r["error"]["type"].clone()), (500, json!("illegal_state_exception")));
        call(&e, "PUT", "/src/_settings", json!({"index.blocks.write": true}));
        let (st, _) = call(&e, "PUT", "/src/_split/dst", body);
        assert_eq!(st, 200);
        let (_, d) = call(&e, "GET", "/dst/_doc/1", Value::Null);
        assert_eq!(d["_source"], json!({"foo": "x"}));
        let (_, st) = call(&e, "GET", "/dst/_settings", Value::Null);
        assert_eq!(st["dst"]["settings"]["index"]["number_of_shards"], json!("4"));
        assert_eq!(st["dst"]["settings"]["index"]["blocks"]["write"], json!("true"));
        let (st, r) =
            call(&e, "PUT", "/src/_clone/c", json!({"settings": {"index.number_of_shards": 3}}));
        assert_eq!((st, r["error"]["type"].clone()), (400, json!("illegal_argument_exception")));
    }

    #[test]
    fn blocks_are_enforced() {
        let e = Engine::default();
        call(&e, "PUT", "/b", Value::Null);
        let (st, r) = call(&e, "PUT", "/b/_block/write", Value::Null);
        assert_eq!(st, 200);
        assert_eq!(r["indices"], json!([{"name": "b", "blocked": true}]));
        let (st, r) = call(&e, "PUT", "/b/_doc/1", json!({"a": 1}));
        assert_eq!((st, r["error"]["type"].clone()), (403, json!("cluster_block_exception")));
        assert_eq!(call(&e, "POST", "/b/_search", Value::Null).0, 200);
        call(
            &e,
            "PUT",
            "/b/_settings",
            json!({"index.blocks.write": false, "index.blocks.read": true}),
        );
        assert_eq!(call(&e, "PUT", "/b/_doc/1", json!({"a": 1})).0, 201);
        assert_eq!(call(&e, "POST", "/b/_search", Value::Null).0, 403);
    }

    #[test]
    fn resolve_index_lists_indices_and_aliases() {
        let e = Engine::default();
        call(&e, "PUT", "/i1", json!({"aliases": {"a": {}, "b": {}}}));
        call(&e, "PUT", "/i2", json!({"aliases": {"a": {}}}));
        call(&e, "POST", "/i2/_close", Value::Null);
        let (_, r) = call(&e, "GET", "/_resolve/index/*", Value::Null);
        assert_eq!(
            r["indices"],
            json!([{"name": "i1", "aliases": ["a", "b"], "attributes": ["open"]}])
        );
        assert_eq!(r["aliases"][0], json!({"name": "a", "indices": ["i1", "i2"]}));
        let (_, r) = call(&e, "GET", "/_resolve/cluster/i2", Value::Null);
        assert_eq!(r["(local)"]["matching_indices"], json!(false));
        let (_, r) = call(&e, "GET", "/_resolve/cluster/nope", Value::Null);
        assert_eq!(r["(local)"]["error"], json!("no such index [nope]"));
    }

    #[test]
    fn units_parse() {
        assert_eq!(super::parse_bytes("10b"), Some(10));
        assert_eq!(super::parse_bytes("1kb"), Some(1024));
        assert_eq!(super::parse_time("7d"), Some(7 * 86_400_000));
        assert_eq!(super::parse_time("0s"), Some(0));
        assert_eq!(super::next_rollover_name("logs-1").unwrap(), "logs-000002");
        assert!(super::next_rollover_name("logs").is_err());
    }
}
