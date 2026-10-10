//! The engine side of text analysis (a child module of `engine`, so it
//! can reach the cluster state): the synonyms API and its store, the
//! `_analyze` endpoint, `_reload_search_analyzers`, the per-request
//! analysis scope, and analysis checks on index creation and mapping
//! updates.

use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::analysis;
use super::{Engine, State, error, missing_index, parse_json, shard_counts};

/// Synonym sets: name -> rule id -> rule text (ids in order, as GET
/// lists them).
#[derive(Default, Serialize, Deserialize, Clone)]
pub(super) struct SynonymStore {
    sets: BTreeMap<String, BTreeMap<String, String>>,
}

impl SynonymStore {
    /// The rule texts of a set, in id order.
    pub(super) fn rules(&self, set: &str) -> Option<Vec<String>> {
        self.sets.get(set).map(|r| r.values().cloned().collect())
    }
}

/// Elasticsearch's request validation error.
fn validation(problems: &[String]) -> (u16, Value) {
    let list: String =
        problems.iter().enumerate().map(|(i, p)| format!("{}: {p};", i + 1)).collect();
    (400, error("action_request_validation_exception", &format!("Validation Failed: {list}"), 400))
}

fn not_found(reason: &str) -> (u16, Value) {
    (404, error("resource_not_found_exception", reason, 404))
}

fn parse_failure(body: &[u8], cause: Value) -> (u16, Value) {
    let raw = String::from_utf8_lossy(body).to_string();
    let reason = format!("Failed to parse: {raw}");
    (
        400,
        json!({"error": {"root_cause": [{"type": "illegal_argument_exception", "reason": reason}],
            "type": "illegal_argument_exception", "reason": reason, "caused_by": cause}, "status": 400}),
    )
}

/// A random rule id, like Elasticsearch's generated document ids.
fn new_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(1);
    let n = N.fetch_add(1, Ordering::Relaxed);
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    const ALPHA: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut x = t ^ n.rotate_left(32) ^ 0x9E37_79B9_7F4A_7C15;
    (0..20)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            ALPHA[(x % 64) as usize] as char
        })
        .collect()
}

/// `from` / `size` paging of the synonyms APIs.
fn paging(q: &HashMap<String, String>) -> Result<(usize, usize), (u16, Value)> {
    let num = |k: &str, d: i64| q.get(k).map_or(Ok(d), |v| v.parse::<i64>().map_err(|_| v.clone()));
    let from = num("from", 0).map_err(|v| {
        (
            400,
            error(
                "illegal_argument_exception",
                &format!("Failed to parse int parameter [from] with value [{v}]"),
                400,
            ),
        )
    })?;
    let size = num("size", 10).map_err(|v| {
        (
            400,
            error(
                "illegal_argument_exception",
                &format!("Failed to parse int parameter [size] with value [{v}]"),
                400,
            ),
        )
    })?;
    let mut problems = Vec::new();
    if from < 0 {
        problems.push("[from] must be a positive integer".to_string());
    }
    if from > 10_000 {
        problems.push("[from] must be less than or equal to 10000".to_string());
    }
    if size < 0 {
        problems.push("[size] must be a positive integer".to_string());
    }
    if size > 10_000 {
        problems.push("[size] must be less than or equal to 10000".to_string());
    }
    if from + size > 10_000 {
        problems.push(
            "Too many results to retrieve. [from] + [size] must be less than or equal to 10000"
                .to_string(),
        );
    }
    if !problems.is_empty() {
        return Err(validation(&problems));
    }
    Ok((from as usize, size as usize))
}

/// One `{"synonyms": ..., "id"?: ...}` rule of a request body.
fn parse_rule(
    v: &Value,
    allow_id: bool,
    body: &[u8],
) -> Result<(Option<String>, String), (u16, Value)> {
    let Some(o) = v.as_object() else {
        return Err(parse_failure(
            body,
            json!({"type": "x_content_parse_exception", "reason": "[synonym_rule] failed to parse"}),
        ));
    };
    for k in o.keys() {
        if k != "synonyms" && !(allow_id && k == "id") {
            let what = if allow_id { "synonym_rule" } else { "synonyms" };
            let inner = json!({"type": "x_content_parse_exception", "reason": format!("[{what}] unknown field [{k}]")});
            let cause = if allow_id {
                json!({"type": "x_content_parse_exception", "reason": "[synonyms_set] failed to parse field [synonyms_set]", "caused_by": inner})
            } else {
                inner
            };
            return Err(parse_failure(body, cause));
        }
    }
    let Some(syn) = o.get("synonyms").and_then(Value::as_str) else {
        let inner = json!({"type": "illegal_argument_exception", "reason": "Required [synonyms]"});
        let cause = if allow_id {
            json!({"type": "x_content_parse_exception", "reason": "[synonyms_set] failed to parse field [synonyms_set]", "caused_by": inner})
        } else {
            inner
        };
        return Err(parse_failure(body, cause));
    };
    let id =
        o.get("id").and_then(|v| v.as_str().map(str::to_string).or_else(|| Some(v.to_string())));
    Ok((id, syn.to_string()))
}

impl Engine {
    /// The analysis scope for a request: the analysis settings of the
    /// indices its path targets, and the synonym sets they use.
    pub(super) fn analysis_scope(&self, segments: &[&str]) -> analysis::ScopeGuard {
        let s = self.0.lock().unwrap();
        let target = match segments.first() {
            Some(
                &("_search" | "_count" | "_msearch" | "_validate" | "_field_caps" | "_knn_search"
                | "_mget" | "_explain"),
            ) => Some("*"),
            Some(t) if !t.starts_with('_') => Some(*t),
            _ => None,
        };
        let names: Vec<String> = match target {
            None => Vec::new(),
            Some(t) => {
                let mut n = Self::resolve_indices(&s, t);
                if n.is_empty() && s.indices.contains_key(t) {
                    n.push(t.to_string());
                }
                n
            }
        };
        let mut scope = analysis::Scope::default();
        let mut merged = serde_json::Map::new();
        for n in &names {
            let Some(i) = s.indices.get(n) else { continue };
            if scope.index_settings.is_null() {
                scope.index_settings = i.settings.get("index").cloned().unwrap_or(Value::Null);
            }
            let Some(a) =
                i.settings.get("index").and_then(|x| x.get("analysis")).and_then(Value::as_object)
            else {
                continue;
            };
            for (section, defs) in a {
                let entry = merged.entry(section.clone()).or_insert_with(|| json!({}));
                if let (Some(dst), Some(src)) = (entry.as_object_mut(), defs.as_object()) {
                    for (k, v) in src {
                        dst.entry(k.clone()).or_insert_with(|| v.clone());
                    }
                }
            }
        }
        if !merged.is_empty() {
            scope.analysis = Value::Object(merged);
            let wrapper = json!({"index": {"analysis": scope.analysis.clone()}});
            for set in analysis::synonym_sets_used(&wrapper) {
                if let Some(rules) = s.synonyms.rules(&set) {
                    scope.synonym_sets.insert(set, rules);
                }
            }
        }
        drop(s);
        analysis::enter(scope)
    }

    /// `[/{index}]/_analyze`.
    pub(super) fn analyze_request(
        &self,
        index: Option<&str>,
        q: &HashMap<String, String>,
        body: &[u8],
    ) -> (u16, Value) {
        let req = if body.is_empty() {
            match q.get("source").map(|s| serde_json::from_str::<Value>(s)) {
                Some(Ok(v)) => v,
                _ => {
                    return (
                        400,
                        error(
                            "parse_exception",
                            "request body or source parameter is required",
                            400,
                        ),
                    );
                }
            }
        } else {
            match parse_json(body) {
                Some(v) => v,
                None => return (400, super::malformed_body()),
            }
        };
        let s = self.0.lock().unwrap();
        let info = match index {
            None => None,
            Some(name) => {
                let target = if s.indices.contains_key(name) {
                    Some(name.to_string())
                } else {
                    s.indices
                        .iter()
                        .find(|(_, i)| i.aliases.contains_key(name))
                        .map(|(n, _)| n.clone())
                };
                let Some(target) = target else { return missing_index(name) };
                let i = &s.indices[&target];
                let max = i.settings["index"]["analyze"]["max_token_count"]
                    .as_str()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(10_000);
                Some((i.mappings.clone(), max))
            }
        };
        drop(s);
        let info_ref = info
            .as_ref()
            .map(|(m, max)| analysis::IndexInfo { mappings: m, max_token_count: *max });
        match analysis::analyze_api(&req, info_ref) {
            Ok(v) => (200, v),
            Err(e) => (e.status, e.to_json()),
        }
    }

    /// `_synonyms[/{set}[/{rule}]]`.
    pub(super) fn synonyms_api(
        &self,
        method: &str,
        segments: &[&str],
        q: &HashMap<String, String>,
        body: &[u8],
    ) -> (u16, Value) {
        let mut s = self.0.lock().unwrap();
        match (method, segments.len()) {
            ("GET", 1) => {
                let (from, size) = match paging(q) {
                    Ok(p) => p,
                    Err(e) => return e,
                };
                let results: Vec<Value> = s
                    .synonyms
                    .sets
                    .iter()
                    .skip(from)
                    .take(size)
                    .map(|(n, r)| json!({"synonyms_set": n, "count": r.len()}))
                    .collect();
                (200, json!({"count": s.synonyms.sets.len(), "results": results}))
            }
            ("GET", 2) => {
                let set = segments[1];
                let (from, size) = match paging(q) {
                    Ok(p) => p,
                    Err(e) => return e,
                };
                let Some(rules) = s.synonyms.sets.get(set) else {
                    return not_found(&format!("synonyms set [{set}] not found"));
                };
                let list: Vec<Value> = rules
                    .iter()
                    .skip(from)
                    .take(size)
                    .map(|(id, r)| json!({"id": id, "synonyms": r}))
                    .collect();
                (200, json!({"count": rules.len(), "synonyms_set": list}))
            }
            ("PUT", 2) => {
                let set = segments[1];
                let Some(req) = parse_json(body) else { return (400, super::malformed_body()) };
                let Some(list) = req.get("synonyms_set") else {
                    return parse_failure(
                        body,
                        json!({"type": "illegal_argument_exception", "reason": "Required [synonyms_set]"}),
                    );
                };
                let Some(list) = list.as_array() else {
                    return parse_failure(
                        body,
                        json!({"type": "x_content_parse_exception", "reason": "[synonyms_set] failed to parse field [synonyms_set]"}),
                    );
                };
                if list.len() > 100_000 {
                    return validation(&[
                        "The number of synonyms rules in a synonym set cannot exceed 100000"
                            .to_string(),
                    ]);
                }
                let mut rules = BTreeMap::new();
                let mut problems = Vec::new();
                for r in list {
                    let (id, text) = match parse_rule(r, true, body) {
                        Ok(x) => x,
                        Err(e) => return e,
                    };
                    if let Err(p) = analysis::validate_api_rule(&text) {
                        problems.push(p);
                    }
                    rules.insert(id.unwrap_or_else(new_id), text);
                }
                if !problems.is_empty() {
                    return validation(&problems);
                }
                let created = s.synonyms.sets.insert(set.to_string(), rules).is_none();
                let details = reload_details(&s, set);
                (
                    if created { 201 } else { 200 },
                    json!({"result": if created { "created" } else { "updated" }, "reload_analyzers_details": details}),
                )
            }
            ("DELETE", 2) => {
                let set = segments[1];
                if !s.synonyms.sets.contains_key(set) {
                    return not_found(&format!("synonyms set [{set}] not found"));
                }
                let users = indices_using(&s, set);
                if !users.is_empty() {
                    return (
                        400,
                        error(
                            "illegal_argument_exception",
                            &format!(
                                "synonyms set [{set}] cannot be deleted as it is used in the following indices: {}",
                                users.join(", ")
                            ),
                            400,
                        ),
                    );
                }
                s.synonyms.sets.remove(set);
                (200, json!({"acknowledged": true}))
            }
            ("GET", 3) => {
                let (set, rule) = (segments[1], segments[2]);
                let Some(rules) = s.synonyms.sets.get(set) else {
                    return not_found(&format!("synonyms set [{set}] not found"));
                };
                match rules.get(rule) {
                    Some(r) => (200, json!({"id": rule, "synonyms": r})),
                    None => not_found(&format!("synonym rule [{rule}] not found")),
                }
            }
            ("PUT", 3) => {
                let (set, rule) = (segments[1], segments[2]);
                let Some(req) = parse_json(body) else { return (400, super::malformed_body()) };
                let (_, text) = match parse_rule(&req, false, body) {
                    Ok(x) => x,
                    Err(e) => return e,
                };
                if let Err(p) = analysis::validate_api_rule(&text) {
                    return validation(&[p]);
                }
                let Some(rules) = s.synonyms.sets.get_mut(set) else {
                    return not_found(&format!("synonyms set [{set}] not found"));
                };
                let created = rules.insert(rule.to_string(), text).is_none();
                let details = reload_details(&s, set);
                (
                    if created { 201 } else { 200 },
                    json!({"result": if created { "created" } else { "updated" }, "reload_analyzers_details": details}),
                )
            }
            ("DELETE", 3) => {
                let (set, rule) = (segments[1], segments[2]);
                let Some(rules) = s.synonyms.sets.get_mut(set) else {
                    return not_found(&format!("synonyms set [{set}] not found"));
                };
                if rules.remove(rule).is_none() {
                    return not_found(&format!(
                        "synonym rule [{rule}] not found on synonyms set [{set}]"
                    ));
                }
                let details = reload_details(&s, set);
                (200, json!({"result": "deleted", "reload_analyzers_details": details}))
            }
            _ => super::no_handler(method, &format!("/{}", segments.join("/"))),
        }
    }

    /// `POST /{index}/_reload_search_analyzers`.
    pub(super) fn reload_search_analyzers(&self, expr: &str) -> (u16, Value) {
        let s = self.0.lock().unwrap();
        let names = Self::resolve_indices(&s, expr);
        if names.is_empty() && !expr.contains('*') {
            return missing_index(expr);
        }
        let (mut total, mut ok) = (0, 0);
        let mut details = Vec::new();
        for n in &names {
            let i = &s.indices[n];
            let (p, r) = shard_counts(i);
            total += p * (1 + r);
            ok += p;
            let mut reloaded: Vec<String> =
                analysis::synonym_set_users(&i.settings).into_values().flatten().collect();
            reloaded.sort();
            reloaded.dedup();
            if !reloaded.is_empty() {
                details.push(json!({"index": n, "reloaded_analyzers": reloaded, "reloaded_node_ids": ["noida"]}));
            }
        }
        (
            200,
            json!({"_shards": {"total": total, "successful": ok, "failed": 0}, "reload_details": details}),
        )
    }
}

/// The indices whose analysis settings use synonym set `set`.
fn indices_using(s: &State, set: &str) -> Vec<String> {
    let mut out: Vec<String> = s
        .indices
        .iter()
        .filter(|(_, i)| analysis::synonym_sets_used(&i.settings).iter().any(|x| x == set))
        .map(|(n, _)| n.clone())
        .collect();
    out.sort();
    out
}

/// `reload_analyzers_details` after synonym set `set` changed: every
/// index's shards are asked, those using the set report their reloaded
/// analyzers.
fn reload_details(s: &State, set: &str) -> Value {
    let (mut total, mut ok) = (0, 0);
    let mut names: Vec<&String> = s.indices.keys().collect();
    names.sort();
    let mut details = Vec::new();
    for n in names {
        let i = &s.indices[n];
        if n.starts_with('.') {
            continue;
        }
        let (p, r) = shard_counts(i);
        total += p * (1 + r);
        ok += p;
        if let Some(users) = analysis::synonym_set_users(&i.settings).get(set) {
            details.push(
                json!({"index": n, "reloaded_analyzers": users, "reloaded_node_ids": ["noida"]}),
            );
        }
    }
    json!({"_shards": {"total": total, "successful": ok, "failed": 0}, "reload_details": details})
}

/// Analysis checks for an index being created or changing its mappings
/// or analysis settings.
pub(super) fn check_index(
    s: &State,
    settings: &Value,
    mappings: &Value,
) -> Result<(), (u16, Value)> {
    let sets = |name: &str| s.synonyms.rules(name);
    analysis::validate_index(settings, mappings, &sets).map_err(|e| (e.status, e.to_json()))
}

/// Whether every synonym set the settings use exists (an index using a
/// missing one is created, but its shards don't start).
pub(super) fn synonym_sets_present(s: &State, settings: &Value) -> bool {
    analysis::synonym_sets_used(settings).iter().all(|set| s.synonyms.sets.contains_key(set))
}
