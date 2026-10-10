//! Cluster-level APIs of the one-node cluster: health, state, stats,
//! `_info`, the health report, allocation explain, reroute, pending
//! tasks, remote info, voting exclusions, desired nodes and balance,
//! node-removal prevalidation, tasks, features and capabilities.

use super::node;
use super::stats::{self, Resolve, StatsRequest};
use super::*;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::atomic::AtomicU64;

/// Cluster metadata beyond indices and templates.
#[derive(Default, Clone, Serialize, Deserialize)]
pub(super) struct ClusterMeta {
    #[serde(default)]
    pub voting_exclusions: Vec<Value>,
    /// `{history_id, version, nodes}` as last accepted.
    #[serde(default)]
    pub desired_nodes: Option<Value>,
}

/// The cluster state's version: one more for every metadata change.
static STATE_VERSION: AtomicU64 = AtomicU64::new(1);

pub(super) fn bump_state_version() {
    STATE_VERSION.fetch_add(1, Ordering::Relaxed);
}

/// `index.routing.allocation.enable: none`: the index's primaries stay
/// unassigned.
pub(super) fn allocation_disabled(i: &Index) -> bool {
    let idx = &i.settings["index"];
    let v = idx["routing"]["allocation"]["enable"]
        .as_str()
        .or_else(|| idx["routing.allocation.enable"].as_str());
    v == Some("none")
}

/// Shard counts of one index: (primaries, replicas, active primaries,
/// unassigned copies).
pub(super) fn index_shards(i: &Index) -> (u64, u64, u64, u64) {
    let (p, r) = shard_counts(i);
    if allocation_disabled(i) { (p, r, 0, p * (1 + r)) } else { (p, r, p, p * r) }
}

fn status_of(active_pri: u64, pri: u64, unassigned: u64) -> &'static str {
    if active_pri < pri {
        "red"
    } else if unassigned > 0 {
        "yellow"
    } else {
        "green"
    }
}

/// `_cluster/health` numbers over `names`.
pub(super) fn health_body(s: &State, names: &[String]) -> Value {
    let (mut pri, mut active_pri, mut unassigned) = (0u64, 0u64, 0u64);
    for i in names.iter().filter_map(|n| s.indices.get(n)) {
        let (p, _, a, u) = index_shards(i);
        pri += p;
        active_pri += a;
        unassigned += u;
    }
    let total = active_pri + unassigned;
    let pct = if total == 0 { 100.0 } else { active_pri as f64 * 100.0 / total as f64 };
    json!({
        "cluster_name": node::CLUSTER_NAME, "status": status_of(active_pri, pri, unassigned),
        "timed_out": false, "number_of_nodes": 1, "number_of_data_nodes": 1,
        "active_primary_shards": active_pri, "active_shards": active_pri, "relocating_shards": 0,
        "initializing_shards": 0, "unassigned_shards": unassigned,
        "delayed_unassigned_shards": 0, "number_of_pending_tasks": 0,
        "number_of_in_flight_fetch": 0, "task_max_waiting_in_queue_millis": 0,
        "active_shards_percent_as_number": pct,
    })
}

/// The tasks running right now: this request's own (a list-tasks action
/// and its node-level child).
pub(super) fn current_tasks(detailed: bool) -> Vec<Value> {
    let parent = node::next_task();
    let child = node::next_task();
    let now = node::now_millis();
    let headers = match node::opaque_id() {
        Some(o) => json!({"X-Opaque-Id": o}),
        None => json!({}),
    };
    let mut a = json!({"node": node::NODE_ID, "id": parent, "type": "transport",
        "action": "cluster:monitor/tasks/lists", "start_time_in_millis": now,
        "running_time_in_nanos": 250_000, "cancellable": false, "headers": headers});
    let mut b = json!({"node": node::NODE_ID, "id": child, "type": "transport",
        "action": "cluster:monitor/tasks/lists[n]", "start_time_in_millis": now,
        "running_time_in_nanos": 100_000, "cancellable": false,
        "parent_task_id": format!("{}:{parent}", node::NODE_ID), "headers": headers});
    if detailed {
        a["description"] = json!("");
        b["description"] = json!("");
    }
    vec![a, b]
}

fn bad_request(kind: &str, reason: &str) -> (u16, Value) {
    (400, error(kind, reason, 400))
}

fn validation(reason: &str) -> (u16, Value) {
    bad_request("action_request_validation_exception", &format!("Validation Failed: 1: {reason};"))
}

fn not_found(reason: &str) -> (u16, Value) {
    (404, error("resource_not_found_exception", reason, 404))
}

/// A node as `toString` prints it in messages.
fn node_string() -> String {
    format!(
        "{{{}}}{{{}}}{{{}}}{{{}}}{{{}}}{{{}}}{{{}}}{{{}}}{{7000099-8512000}}{{xpack.installed=true, transform.config_version=10.0.0, ml.config_version=12.0.0}}",
        node::NODE_NAME,
        node::NODE_ID,
        node::EPHEMERAL_ID,
        node::NODE_NAME,
        node::HOST,
        node::TRANSPORT_ADDRESS,
        node::ROLE_CHARS,
        node::VERSION
    )
}

const STATE_METRICS: &[&str] =
    &["version", "master_node", "blocks", "nodes", "metadata", "routing_table", "routing_nodes"];

impl Engine {
    /// `/_cluster/...` APIs handled here; `None` for the rest
    /// (`_cluster/settings`).
    pub(super) fn cluster_route(
        &self,
        method: &str,
        segments: &[&str],
        q: &HashMap<String, String>,
        body: &[u8],
        path: &str,
    ) -> Option<(u16, Value)> {
        let r = match (method, segments.get(1).copied()) {
            ("GET", Some("health")) => self.cluster_health(segments.get(2).copied(), q),
            ("GET", Some("state")) => {
                self.cluster_state(segments.get(2).copied(), segments.get(3).copied(), q, path)
            }
            ("GET", Some("stats")) => self.cluster_stats(q),
            ("GET", Some("pending_tasks")) => (200, json!({"tasks": []})),
            ("GET" | "POST", Some("allocation")) if segments.get(2) == Some(&"explain") => {
                self.allocation_explain(q, body)
            }
            ("POST", Some("reroute")) => self.reroute(q, body),
            (_, Some("voting_config_exclusions")) => self.voting_exclusions(method, q),
            _ => return None,
        };
        Some(r)
    }

    fn cluster_health(&self, index: Option<&str>, q: &HashMap<String, String>) -> (u16, Value) {
        let level = q.get("level").map_or("cluster", String::as_str);
        if !matches!(level, "cluster" | "indices" | "shards") {
            let msg = format!(
                "level parameter must be one of [cluster] or [indices] or [shards] but was [{level}]"
            );
            return bad_request("illegal_argument_exception", &msg);
        }
        let s = self.0.lock().unwrap();
        let names: Vec<String> = match index {
            Some(expr) => {
                let opts = Resolve { expand: "all", lenient: true, forbid_closed: false };
                let names = stats::resolve(&s, expr, q, &opts).unwrap_or_default();
                let named_missing = expr.split(',').map(str::trim).any(|p| {
                    !p.contains('*')
                        && p != "_all"
                        && !p.starts_with('-')
                        && !s.indices.contains_key(p)
                        && !s.indices.values().any(|i| i.aliases.contains_key(p))
                });
                if named_missing {
                    // Elasticsearch waits for the index to appear, then
                    // gives up red.
                    let mut h = health_body(&s, &[]);
                    h["status"] = json!("red");
                    h["timed_out"] = json!(true);
                    return (408, h);
                }
                names
            }
            None => s.indices.keys().cloned().collect(),
        };
        let mut h = health_body(&s, &names);
        let mut timed_out = false;
        if let Some(want) = q.get("wait_for_status") {
            let rank = |st: &str| match st {
                "green" => 0,
                "yellow" => 1,
                _ => 2,
            };
            timed_out |= rank(h["status"].as_str().unwrap_or("red")) > rank(want);
        }
        if let Some(want) = q.get("wait_for_nodes") {
            timed_out |= !nodes_condition(want, 1);
        }
        if let Some(want) = q.get("wait_for_active_shards") {
            let active = h["active_shards"].as_u64().unwrap_or(0);
            let total = active + h["unassigned_shards"].as_u64().unwrap_or(0);
            let needed = match want.as_str() {
                "all" => total,
                n => n.parse::<u64>().unwrap_or(0),
            };
            timed_out |= active < needed;
        }
        if level != "cluster" {
            let mut indices = Map::new();
            let mut sorted = names.clone();
            sorted.sort();
            for n in &sorted {
                let i = &s.indices[n];
                let (p, r, a, u) = index_shards(i);
                let mut e = json!({"status": status_of(a, p, u), "number_of_shards": p,
                    "number_of_replicas": r, "active_primary_shards": a, "active_shards": a,
                    "relocating_shards": 0, "initializing_shards": 0, "unassigned_shards": u});
                if level == "shards" {
                    let per = u.checked_div(p).unwrap_or(0);
                    let shards: Map<String, Value> = (0..p)
                        .map(|sh| {
                            let active = u64::from(a > 0);
                            (
                                sh.to_string(),
                                json!({"status": status_of(active, 1, per),
                                "primary_active": a > 0, "active_shards": active,
                                "relocating_shards": 0, "initializing_shards": 0,
                                "unassigned_shards": per}),
                            )
                        })
                        .collect();
                    e["shards"] = Value::Object(shards);
                }
                indices.insert(n.clone(), e);
            }
            h["indices"] = Value::Object(indices);
        }
        if timed_out {
            h["timed_out"] = json!(true);
            return (408, h);
        }
        (200, h)
    }

    fn cluster_state(
        &self,
        metric: Option<&str>,
        index: Option<&str>,
        q: &HashMap<String, String>,
        path: &str,
    ) -> (u16, Value) {
        // Unknown metrics are ignored, as Elasticsearch does.
        let _ = path;
        let mut wanted: Vec<&str> = Vec::new();
        match metric {
            None | Some("_all") => wanted.extend(STATE_METRICS),
            Some(list) => {
                for m in list.split(',').map(str::trim) {
                    match m {
                        "_all" => wanted.extend(STATE_METRICS),
                        m if STATE_METRICS.contains(&m) => wanted.push(m),
                        _ => {}
                    }
                }
            }
        }
        let s = self.0.lock().unwrap();
        let names: Vec<String> = match index {
            None | Some("_all") | Some("*") if q.get("expand_wildcards").is_none() => {
                let mut v: Vec<String> = s.indices.keys().cloned().collect();
                v.sort();
                v
            }
            expr => match stats::resolve(&s, expr.unwrap_or("_all"), q, &Resolve::LENIENT_OPEN) {
                Ok(n) => n,
                Err(e) => return e,
            },
        };
        let version = STATE_VERSION.load(Ordering::Relaxed) + s.indices.len() as u64;
        let mut out =
            json!({"cluster_name": node::CLUSTER_NAME, "cluster_uuid": node::CLUSTER_UUID});
        if wanted.contains(&"version") {
            out["version"] = json!(version);
            out["state_uuid"] = json!(format!("noida_state_{version:010}"));
        }
        if wanted.contains(&"master_node") {
            out["master_node"] = json!(node::NODE_ID);
        }
        if wanted.contains(&"blocks") {
            out["blocks"] = blocks(&s);
        }
        if wanted.contains(&"nodes") {
            let mut nodes = Map::new();
            nodes.insert(node::NODE_ID.into(), node::discovery_node());
            out["nodes"] = Value::Object(nodes);
            out["nodes_versions"] = json!([{"node_id": node::NODE_ID, "transport_version": "8702003",
                                            "mappings_versions": {}}]);
            out["nodes_features"] = json!([{"node_id": node::NODE_ID, "features": NODE_FEATURES}]);
        }
        if wanted.contains(&"metadata") {
            let mut indices = Map::new();
            for n in &names {
                indices.insert(n.clone(), index_metadata(&s.indices[n]));
            }
            let templates: Map<String, Value> = Map::new();
            out["metadata"] = json!({
                "cluster_uuid": node::CLUSTER_UUID, "cluster_uuid_committed": true,
                "cluster_coordination": {"term": 1, "last_committed_config": [node::NODE_ID],
                                         "last_accepted_config": [node::NODE_ID],
                                         "voting_config_exclusions": s.cluster_meta.voting_exclusions},
                "templates": templates, "indices": indices,
                "index-graveyard": {"tombstones": []}, "reserved_state": {},
            });
        }
        if wanted.contains(&"routing_table") {
            let mut indices = Map::new();
            for n in &names {
                indices.insert(n.clone(), json!({"shards": routing_shards(n, &s.indices[n])}));
            }
            out["routing_table"] = json!({"indices": indices});
        }
        if metric.is_none_or(|m| m == "_all") {
            out["repository_cleanup"] = json!({"repository_cleanup": []});
            out["snapshots"] = json!({"snapshots": [], "node_ids_for_removal": []});
            out["restore"] = json!({"snapshots": []});
            out["snapshot_deletions"] = json!({"snapshot_deletions": []});
            out["health"] = json!({"disk": {"high_watermark": "90%", "high_max_headroom": "150gb",
                "flood_stage_watermark": "95%", "flood_stage_max_headroom": "100gb",
                "frozen_flood_stage_watermark": "95%", "frozen_flood_stage_max_headroom": "20gb"},
                "shard_limits": {"max_shards_per_node": 1000, "max_shards_per_node_frozen": 3000}});
        }
        if wanted.contains(&"routing_nodes") {
            let (mut assigned, mut unassigned) = (vec![], vec![]);
            for n in &names {
                for copies in routing_shards(n, &s.indices[n])
                    .as_object()
                    .into_iter()
                    .flat_map(|m| m.values())
                {
                    for c in copies.as_array().into_iter().flatten() {
                        if c["state"] == json!("STARTED") {
                            assigned.push(c.clone());
                        } else {
                            unassigned.push(c.clone());
                        }
                    }
                }
            }
            let mut nodes = Map::new();
            nodes.insert(node::NODE_ID.into(), Value::Array(assigned));
            out["routing_nodes"] = json!({"unassigned": unassigned, "nodes": nodes});
        }
        (200, out)
    }

    fn cluster_stats(&self, q: &HashMap<String, String>) -> (u16, Value) {
        let s = self.0.lock().unwrap();
        let mut names: Vec<String> = s.indices.keys().cloned().collect();
        names.sort();
        let h = health_body(&s, &names);
        let req = StatsRequest::all();
        let mut sum = json!({});
        let (mut shards, mut pri) = (0u64, 0u64);
        let mut per_index: Vec<u64> = vec![];
        for n in &names {
            let i = &s.indices[n];
            if i.opened {
                stats::add_stats(&mut sum, &stats::index_sections(i, &req));
            }
            let (_, _, a, _) = index_shards(i);
            shards += a;
            pri += a;
            per_index.push(a);
        }
        let get = |k: &str| sum.get(k).cloned().unwrap_or_else(|| json!({}));
        let stat = |v: &[u64]| {
            if v.is_empty() {
                json!({})
            } else {
                let min = *v.iter().min().unwrap();
                let max = *v.iter().max().unwrap();
                let avg = v.iter().sum::<u64>() as f64 / v.len() as f64;
                json!({"min": min, "max": max, "avg": avg})
            }
        };
        let index_shards_stats = if per_index.is_empty() {
            json!({})
        } else {
            json!({"shards": stat(&per_index), "primaries": stat(&per_index),
                   "replication": {"min": 0.0, "max": 0.0, "avg": 0.0}})
        };
        let docs_bytes = sum["store"]["size_in_bytes"].as_u64().unwrap_or(0);
        let mut docs = get("docs");
        if docs.as_object().is_none_or(Map::is_empty) {
            docs = json!({"count": 0, "deleted": 0, "total_size_in_bytes": 0});
        }
        docs["total_size_in_bytes"] = json!(docs_bytes);
        let (mem_total, mem_free) = super::nodes::memory();
        let (disk_total, disk_avail) = super::nodes::disk();
        let (total_fields, dedup_fields, dedup_bytes) = mapping_sizes(&s);
        let mut out = json!({
            "_nodes": node::nodes_header(1), "cluster_name": node::CLUSTER_NAME,
            "cluster_uuid": node::CLUSTER_UUID, "timestamp": node::now_millis(),
            "status": h["status"],
            "indices": {
                "count": names.len(),
                "shards": {"total": shards, "primaries": pri, "replication": 0.0, "index": index_shards_stats},
                "docs": docs,
                "store": or_zero(get("store"), json!({"size_in_bytes": 0, "total_data_set_size_in_bytes": 0, "reserved_in_bytes": 0})),
                "fielddata": or_zero(get("fielddata"), json!({"memory_size_in_bytes": 0, "evictions": 0, "global_ordinals": {"build_time_in_millis": 0}})),
                "query_cache": or_zero(get("query_cache"), json!({"memory_size_in_bytes": 0, "total_count": 0, "hit_count": 0, "miss_count": 0, "cache_size": 0, "cache_count": 0, "evictions": 0})),
                "completion": or_zero(get("completion"), json!({"size_in_bytes": 0})),
                "segments": or_zero(get("segments"), json!({"count": 0, "memory_in_bytes": 0, "terms_memory_in_bytes": 0,
                    "stored_fields_memory_in_bytes": 0, "term_vectors_memory_in_bytes": 0, "norms_memory_in_bytes": 0,
                    "points_memory_in_bytes": 0, "doc_values_memory_in_bytes": 0, "index_writer_memory_in_bytes": 0,
                    "version_map_memory_in_bytes": 0, "fixed_bit_set_memory_in_bytes": 0,
                    "max_unsafe_auto_id_timestamp": -1, "file_sizes": {}})),
                "mappings": {"total_field_count": total_fields, "total_deduplicated_field_count": dedup_fields,
                             "total_deduplicated_mapping_size_in_bytes": dedup_bytes,
                             "field_types": field_type_stats(&s), "runtime_field_types": runtime_field_stats(&s)},
                "analysis": {"char_filter_types": [], "tokenizer_types": [], "filter_types": [],
                             "analyzer_types": [], "built_in_char_filters": [], "built_in_tokenizers": [],
                             "built_in_filters": [], "built_in_analyzers": [], "synonyms": {}},
                "versions": if names.is_empty() { json!([]) } else {
                    json!([{"version": "8.15.0-8.15.3", "index_count": names.len(), "primary_shard_count": pri,
                            "total_primary_bytes": docs_bytes}]) },
                "search": {"total": 0, "queries": {}, "rescorers": {}, "sections": {}},
                "dense_vector": or_zero(get("dense_vector"), json!({"value_count": 0})),
                "sparse_vector": or_zero(get("sparse_vector"), json!({"value_count": 0})),
            },
            "nodes": {
                "count": {"total": 1, "coordinating_only": 0, "data": 1, "data_cold": 1, "data_content": 1,
                          "data_frozen": 1, "data_hot": 1, "data_warm": 1, "index": 0, "ingest": 1, "master": 1,
                          "ml": 1, "remote_cluster_client": 1, "search": 0, "transform": 1, "voting_only": 0},
                "versions": [node::VERSION],
                "os": {"available_processors": 4, "allocated_processors": 4,
                       "names": [{"name": "Linux", "count": 1}], "pretty_names": [{"pretty_name": "Linux", "count": 1}],
                       "architectures": [{"arch": "amd64", "count": 1}],
                       "mem": {"total_in_bytes": mem_total, "adjusted_total_in_bytes": mem_total,
                               "free_in_bytes": mem_free, "used_in_bytes": mem_total - mem_free,
                               "free_percent": mem_free * 100 / mem_total.max(1),
                               "used_percent": (mem_total - mem_free) * 100 / mem_total.max(1)}},
                "process": {"cpu": {"percent": 0}, "open_file_descriptors": {"min": 64, "max": 64, "avg": 64}},
                "jvm": {"max_uptime_in_millis": node::uptime_millis(),
                        "versions": [{"version": "22.0.1", "vm_name": "OpenJDK 64-Bit Server VM",
                                      "vm_version": "22.0.1+8-16", "vm_vendor": "Oracle Corporation",
                                      "bundled_jdk": true, "using_bundled_jdk": true, "count": 1}],
                        "mem": {"heap_used_in_bytes": 134_217_728u64, "heap_max_in_bytes": 536_870_912u64},
                        "threads": 64},
                "fs": {"total_in_bytes": disk_total, "free_in_bytes": disk_avail, "available_in_bytes": disk_avail},
                "plugins": [],
                "network_types": {"transport_types": {"netty4": 1}, "http_types": {"netty4": 1}},
                "discovery_types": {"single-node": 1},
                "packaging_types": [{"flavor": "default", "type": "docker", "count": 1}],
                "ingest": {"number_of_pipelines": 0, "processor_stats": {}},
                "indexing_pressure": {"memory": {
                    "current": {"combined_coordinating_and_primary_in_bytes": 0, "coordinating_in_bytes": 0,
                                "primary_in_bytes": 0, "replica_in_bytes": 0, "all_in_bytes": 0},
                    "total": {"combined_coordinating_and_primary_in_bytes": 0, "coordinating_in_bytes": 0,
                              "primary_in_bytes": 0, "replica_in_bytes": 0, "all_in_bytes": 0,
                              "coordinating_rejections": 0, "primary_rejections": 0, "replica_rejections": 0,
                              "primary_document_rejections": 0},
                    "limit_in_bytes": 0}},
            },
            "snapshots": {"current_counts": {"snapshots": 0, "shard_snapshots": 0, "snapshot_deletions": 0,
                                             "concurrent_operations": 0, "cleanups": 0},
                          "repositories": {}},
        });
        if stats::human(q) {
            node::add_human(&mut out);
            if let Some(v) = out["indices"]["versions"].get_mut(0) {
                v["total_primary_size"] = json!(node::human_size(docs_bytes));
            }
        }
        (200, out)
    }

    fn allocation_explain(&self, q: &HashMap<String, String>, body: &[u8]) -> (u16, Value) {
        let req = if body.iter().all(u8::is_ascii_whitespace) {
            json!({})
        } else {
            match parse_json(body) {
                Some(v) => v,
                None => return (400, malformed_body()),
            }
        };
        let s = self.0.lock().unwrap();
        let mut names: Vec<&String> = s.indices.keys().collect();
        names.sort();
        // (index, shard, primary)
        let target = if req.get("index").is_none() && req.get("shard").is_none() {
            let unassigned = names.iter().find_map(|n| {
                let i = &s.indices[*n];
                let (_, r, a, u) = index_shards(i);
                if u == 0 { None } else { Some(((*n).clone(), 0u64, a == 0 || r == 0)) }
            });
            match unassigned {
                Some(t) => t,
                None => {
                    return bad_request(
                        "illegal_argument_exception",
                        "No shard was specified in the request which means the response should explain a randomly-chosen unassigned shard, but there are no unassigned shards in this cluster. To explain the allocation of an assigned shard you must specify the target shard in the request.",
                    );
                }
            }
        } else {
            let (Some(idx), Some(shard), Some(primary)) = (
                req.get("index").and_then(Value::as_str),
                req.get("shard").and_then(Value::as_u64),
                req.get("primary").and_then(Value::as_bool),
            ) else {
                return bad_request(
                    "action_request_validation_exception",
                    "Validation Failed: 1: index must be specified;2: shard must be specified;3: primary must be specified;",
                );
            };
            (idx.to_string(), shard, primary)
        };
        let (idx, shard, primary) = target;
        let Some(i) = s.indices.get(&idx) else { return missing_index(&idx) };
        let (p, _, a, _) = index_shards(i);
        if shard >= p {
            let msg = format!("No shard was found for [{idx}][{shard}]");
            return bad_request("illegal_argument_exception", &msg);
        }
        let current_node = json!({"id": node::NODE_ID, "name": node::NODE_NAME,
            "transport_address": node::TRANSPORT_ADDRESS, "attributes": node::attributes(),
            "roles": node::ROLES, "weight_ranking": 1});
        let created = i.settings["index"]["creation_date"]
            .as_str()
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(0);
        let mut out = json!({"index": idx, "shard": shard, "primary": primary});
        if req.get("index").is_none() {
            out["note"] = json!(
                "No shard was specified in the explain API request, so this response explains a randomly chosen unassigned shard. There may be other unassigned shards in this cluster which cannot be assigned for different reasons. It may not be possible to assign this shard until one of the other shards is assigned correctly. To explain the allocation of other shards (whether assigned or unassigned) you must specify the target shard in the request to this API."
            );
        }
        if primary && a > 0 {
            out["current_state"] = json!("started");
            out["current_node"] = current_node;
            out["can_remain_on_current_node"] = json!("yes");
            out["can_rebalance_cluster"] = json!("no");
            out["can_rebalance_cluster_decisions"] = json!([{"decider": "rebalance_only_when_active",
                "decision": "NO", "explanation": "rebalancing is not allowed until all copies of this shard are active"}]);
            out["can_rebalance_to_other_node"] = json!("no");
            out["rebalance_explanation"] = json!(
                "Elasticsearch is not allowed to allocate or rebalance this shard to another node. If you expect this shard to be rebalanced to another node, find this node in the node-by-node explanation and address the reasons which prevent Elasticsearch from rebalancing this shard there."
            );
        } else {
            out["current_state"] = json!("unassigned");
            out["unassigned_info"] = json!({"reason": "INDEX_CREATED",
                "at": dates::format(created, None, 0), "last_allocation_status": "no_attempt"});
            if q.get("include_disk_info").is_some_and(|v| v.is_empty() || v == "true") {
                let (total, avail) = super::nodes::disk();
                let disk = json!({"path": "/usr/share/elasticsearch/data", "total_bytes": total,
                    "used_bytes": total - avail, "free_bytes": avail,
                    "free_disk_percent": (avail as f64 * 1000.0 / total as f64).round() / 10.0,
                    "used_disk_percent": ((total - avail) as f64 * 1000.0 / total as f64).round() / 10.0});
                let mut nodes = Map::new();
                nodes.insert(
                    node::NODE_ID.into(),
                    json!({"node_name": node::NODE_NAME,
                    "least_available": disk, "most_available": disk}),
                );
                out["cluster_info"] = json!({"nodes": nodes, "shard_sizes": {}, "shard_data_set_sizes": {},
                    "shard_paths": {}, "reserved_sizes": []});
            }
            out["can_allocate"] = json!("no");
            out["allocate_explanation"] = json!(
                "Elasticsearch isn't allowed to allocate this shard to any of the nodes in the cluster. Choose a node to which you expect this shard to be allocated, find this node in the node-by-node explanation, and address the reasons which prevent Elasticsearch from allocating this shard there."
            );
            let decider = if primary {
                json!({"decider": "enable", "decision": "NO",
                       "explanation": "no allocations are allowed due to index setting [index.routing.allocation.enable=none]"})
            } else {
                json!({"decider": "same_shard", "decision": "NO",
                       "explanation": format!("a copy of this shard is already allocated to this node [[{idx}][{shard}], node[{}], [P], s[STARTED], a[id=noida]]", node::NODE_ID)})
            };
            out["node_allocation_decisions"] = json!([{"node_id": node::NODE_ID, "node_name": node::NODE_NAME,
                "transport_address": node::TRANSPORT_ADDRESS, "node_attributes": node::attributes(),
                "roles": node::ROLES, "node_decision": "no", "weight_ranking": 1, "deciders": [decider]}]);
        }
        (200, out)
    }

    fn reroute(&self, q: &HashMap<String, String>, body: &[u8]) -> (u16, Value) {
        let req = if body.iter().all(u8::is_ascii_whitespace) {
            json!({})
        } else {
            match parse_json(body) {
                Some(v) => v,
                None => return (400, malformed_body()),
            }
        };
        let explain = q.get("explain").is_some_and(|v| v.is_empty() || v == "true");
        let s = self.0.lock().unwrap();
        let mut explanations = Vec::new();
        for cmd in req["commands"].as_array().into_iter().flatten() {
            let Some((name, params)) = cmd.as_object().and_then(|o| o.iter().next()) else {
                continue;
            };
            let idx = params["index"].as_str().unwrap_or("");
            let shard = params["shard"].as_u64().unwrap_or(0);
            let node_name = params["node"].as_str().unwrap_or("");
            let mut parameters = params.clone();
            if name == "cancel" && parameters.get("allow_primary").is_none() {
                parameters["allow_primary"] = json!(false);
            }
            let reason = match s.indices.get(idx) {
                None => format!("[{name}_allocation_command] failed to find index [{idx}]"),
                Some(i) if shard >= shard_counts(i).0 => format!(
                    "can't cancel [{idx}][{shard}], failed to find it on node {}",
                    node_string()
                ),
                Some(_) if !node::selected(node_name) => {
                    format!("failed to resolve [{node_name}], no matching nodes")
                }
                Some(_) => format!(
                    "can't cancel [{idx}][{shard}] on node {}, shard is primary and initializing its state",
                    node_string()
                ),
            };
            let decider = format!("{name}_allocation_command");
            if !explain {
                return bad_request("illegal_argument_exception", &format!("[{decider}] {reason}"));
            }
            explanations.push(json!({"command": name, "parameters": parameters,
                "decisions": [{"decider": decider, "decision": "NO", "explanation": reason}]}));
        }
        let mut out = json!({"acknowledged": true});
        let metric = q.get("metric").map(String::as_str);
        let mut warn = false;
        if metric != Some("none") {
            warn = true;
            let m = match metric {
                None => "version,master_node,blocks,nodes,routing_table",
                Some(m) => m,
            };
            drop(s);
            let (_, state) =
                self.cluster_state(Some(m), None, &HashMap::new(), "/_cluster/reroute");
            out["state"] = state;
        }
        if explain {
            out["explanations"] = Value::Array(explanations);
        }
        if warn {
            out[WARNINGS] = json!([
                "The [state] field in the response to the reroute API is deprecated and will be removed in a future version. Specify ?metric=none to adopt the future behaviour."
            ]);
        }
        (200, out)
    }

    fn voting_exclusions(&self, method: &str, q: &HashMap<String, String>) -> (u16, Value) {
        let mut s = self.0.lock().unwrap();
        match method {
            "POST" => {
                let ids = q.get("node_ids").filter(|v| !v.is_empty());
                let names = q.get("node_names").filter(|v| !v.is_empty());
                let list: Vec<Value> = match (ids, names) {
                    (Some(ids), None) => ids
                        .split(',')
                        .map(|id| {
                            let name =
                                if id == node::NODE_ID { node::NODE_NAME } else { "_absent_" };
                            json!({"node_id": id, "node_name": name})
                        })
                        .collect(),
                    (None, Some(names)) => names
                        .split(',')
                        .map(|n| {
                            let id = if n == node::NODE_NAME { node::NODE_ID } else { "_absent_" };
                            json!({"node_id": id, "node_name": n})
                        })
                        .collect(),
                    _ => {
                        return bad_request(
                            "illegal_argument_exception",
                            "Please set node identifiers correctly. One and only one of [node_name], [node_names] and [node_ids] has to be set",
                        );
                    }
                };
                for e in list {
                    if !s.cluster_meta.voting_exclusions.contains(&e) {
                        s.cluster_meta.voting_exclusions.push(e);
                    }
                }
                bump_state_version();
                (200, json!({}))
            }
            "DELETE" => {
                s.cluster_meta.voting_exclusions.clear();
                bump_state_version();
                (200, json!({}))
            }
            _ => no_handler(method, "/_cluster/voting_config_exclusions"),
        }
    }

    /// `/_info/<targets>`.
    pub(super) fn cluster_info(&self, target: Option<&str>, path: &str) -> (u16, Value) {
        let known = ["_all", "http", "ingest", "thread_pool", "script"];
        let targets: Vec<&str> =
            target.unwrap_or("").split(',').map(str::trim).filter(|t| !t.is_empty()).collect();
        if targets.contains(&"_all") && targets.len() > 1 {
            let msg = format!(
                "request [{path}] contains _all and individual target [{}]",
                targets.join(",")
            );
            return bad_request("illegal_argument_exception", &msg);
        }
        let bad: Vec<String> =
            targets.iter().filter(|t| !known.contains(t)).map(|t| t.to_string()).collect();
        if !bad.is_empty() {
            let msg = node::unrecognized(path, &bad, &known, "target");
            return bad_request("illegal_argument_exception", &msg);
        }
        let all = targets.contains(&"_all");
        let want = |t: &str| all || targets.contains(&t);
        let mut out = json!({"cluster_name": node::CLUSTER_NAME});
        if want("thread_pool") {
            let pools: Map<String, Value> = node::thread_pools()
                .into_iter()
                .map(|(n, info)| (n, node::thread_pool_stats(&info)))
                .collect();
            out["thread_pool"] = Value::Object(pools);
        }
        if want("http") {
            out["http"] = node::http_stats();
        }
        if want("script") {
            out["script"] = json!({"compilations": 0, "cache_evictions": 0, "compilation_limit_triggered": 0,
                                   "compilations_history": {}, "contexts": []});
        }
        if want("ingest") {
            out["ingest"] = json!({"total": {"count": 0, "time_in_millis": 0, "current": 0, "failed": 0},
                                   "pipelines": {}});
        }
        (200, out)
    }

    /// `GET /_health_report[/<indicator>]`.
    pub(super) fn health_report(
        &self,
        feature: Option<&str>,
        q: &HashMap<String, String>,
    ) -> (u16, Value) {
        let verbose = q.get("verbose").is_none_or(|v| v != "false");
        let s = self.0.lock().unwrap();
        let mut names: Vec<&String> = s.indices.keys().collect();
        names.sort();
        let (mut started_pri, mut unassigned_pri, mut unassigned_rep) = (0u64, 0u64, 0u64);
        let (mut disabled, mut replicated) = (vec![], vec![]);
        for n in &names {
            let i = &s.indices[*n];
            let (p, r, a, _) = index_shards(i);
            started_pri += a;
            unassigned_pri += p - a;
            unassigned_rep += p * r;
            if a < p {
                disabled.push((*n).clone());
            }
            if r > 0 {
                replicated.push((*n).clone());
            }
        }
        let plural = |n: u64, what: &str| {
            format!("{n} unavailable {what} shard{}", if n == 1 { "" } else { "s" })
        };
        let (status, symptom) = match (unassigned_pri, unassigned_rep) {
            (0, 0) => ("green", "This cluster has all shards available.".to_string()),
            (0, r) => ("yellow", format!("This cluster has {}.", plural(r, "replica"))),
            (p, 0) => ("red", format!("This cluster has {}.", plural(p, "primary"))),
            (p, r) => (
                "red",
                format!("This cluster has {}, {}.", plural(p, "primary"), plural(r, "replica")),
            ),
        };
        let list = |v: &[String]| {
            let shown: Vec<&str> = v.iter().take(10).map(String::as_str).collect();
            format!("{}{}", shown.join(", "), if v.len() > 10 { ", ..." } else { "" })
        };
        let mut availability = json!({"status": status, "symptom": symptom, "details": {
            "restarting_primaries": 0, "started_primaries": started_pri, "unassigned_replicas": unassigned_rep,
            "initializing_replicas": 0, "creating_primaries": 0, "restarting_replicas": 0,
            "unassigned_primaries": unassigned_pri, "started_replicas": 0, "creating_replicas": 0,
            "initializing_primaries": 0}});
        let mut impacts = vec![];
        let mut diagnosis = vec![];
        if !disabled.is_empty() {
            let n = disabled.len();
            impacts.push(json!({"id": "elasticsearch:health:shards_availability:impact:primary_unassigned",
                "severity": 1, "description": format!("Cannot add data to {n} ind{} [{}]. Searches might return incomplete results.",
                    if n == 1 { "ex" } else { "ices" }, list(&disabled)),
                "impact_areas": ["ingest", "search"]}));
        }
        if unassigned_rep > 0 {
            let n = replicated.len();
            impacts.push(json!({"id": "elasticsearch:health:shards_availability:impact:replica_unassigned",
                "severity": 2, "description": format!("Searches might be slower than usual. Fewer redundant copies of the data exist on {n} ind{} [{}].",
                    if n == 1 { "ex" } else { "ices" }, list(&replicated)),
                "impact_areas": ["search"]}));
            diagnosis.push(json!({"id": "elasticsearch:health:shards_availability:diagnosis:increase_tier_capacity_for_allocations:tier:data_content",
                "cause": "Elasticsearch isn't allowed to allocate some shards from these indices to any of the nodes in the desired data tier because there are not enough nodes in the [data_content] tier to allocate each shard copy on a different node.",
                "action": "Increase the number of nodes in this tier or decrease the number of replica shards in the affected indices.",
                "help_url": "https://ela.st/tier-capacity", "affected_resources": {"indices": replicated}}));
        }
        if !disabled.is_empty() {
            diagnosis.push(json!({"id": "elasticsearch:health:shards_availability:diagnosis:enable_index_allocations",
                "cause": "Elasticsearch isn't allowed to allocate some shards from these indices because allocation for those shards has been disabled at the index level.",
                "action": "Check that the [index.routing.allocation.enable] index settings are set to [all].",
                "help_url": "https://ela.st/fix-index-allocation", "affected_resources": {"indices": disabled}}));
        }
        if !impacts.is_empty() {
            availability["impacts"] = Value::Array(impacts);
        }
        if !diagnosis.is_empty() {
            availability["diagnosis"] = Value::Array(diagnosis);
        }
        let master = json!({"node_id": node::NODE_ID, "name": node::NODE_NAME});
        let indicators: Vec<(&str, Value)> = vec![
            (
                "master_is_stable",
                json!({"status": "green", "symptom": "The cluster has a stable master node",
                "details": {"current_master": master, "recent_masters": [master]}}),
            ),
            (
                "repository_integrity",
                json!({"status": "green", "symptom": "No snapshot repositories configured."}),
            ),
            (
                "disk",
                json!({"status": "green", "symptom": "The cluster has enough available disk space.",
                "details": {"indices_with_readonly_block": 0, "nodes_with_enough_disk_space": 1,
                            "nodes_with_unknown_disk_status": 0, "nodes_over_high_watermark": 0,
                            "nodes_over_flood_stage_watermark": 0}}),
            ),
            (
                "shards_capacity",
                json!({"status": "green", "symptom": "The cluster has enough room to add new shards.",
                "details": {"data": {"max_shards_in_cluster": 1000}, "frozen": {"max_shards_in_cluster": 3000}}}),
            ),
            ("shards_availability", availability),
            (
                "data_stream_lifecycle",
                json!({"status": "green", "symptom": "Data streams are executing their lifecycles without issues",
                "details": {"stagnating_backing_indices_count": 0, "total_backing_indices_in_error": 0}}),
            ),
            (
                "slm",
                json!({"status": "green", "symptom": "No Snapshot Lifecycle Management policies configured",
                "details": {"slm_status": "RUNNING", "policies": 0}}),
            ),
            (
                "ilm",
                json!({"status": "green", "symptom": "No Index Lifecycle Management policies configured",
                "details": {"policies": 0, "stagnating_indices": 0, "ilm_status": "RUNNING"}}),
            ),
        ];
        let rank = |s: &str| match s {
            "green" => 0,
            "unknown" => 1,
            "yellow" => 2,
            _ => 3,
        };
        let mut chosen = Map::new();
        let mut worst = "green";
        for (name, mut v) in indicators {
            if feature.is_some_and(|f| f != name) {
                continue;
            }
            if !verbose && let Some(o) = v.as_object_mut() {
                o.remove("details");
                o.remove("diagnosis");
            }
            let st = v["status"].as_str().unwrap_or("green");
            if rank(st) > rank(worst) {
                worst = if st == "red" {
                    "red"
                } else if st == "yellow" {
                    "yellow"
                } else {
                    worst
                };
            }
            chosen.insert(name.into(), v);
        }
        if let Some(f) = feature
            && chosen.is_empty()
        {
            return not_found(&format!("Did not find indicator {f}"));
        }
        let mut out = json!({});
        if feature.is_none() {
            out["status"] = json!(worst);
        }
        out["cluster_name"] = json!(node::CLUSTER_NAME);
        out["indicators"] = Value::Object(chosen);
        (200, out)
    }

    /// `/_tasks...`.
    pub(super) fn tasks_api(
        &self,
        method: &str,
        segments: &[&str],
        q: &HashMap<String, String>,
    ) -> (u16, Value) {
        match (method, segments) {
            ("GET", ["_tasks"]) => {
                let detailed = q.get("detailed").is_some_and(|v| v.is_empty() || v == "true");
                let actions: Option<Vec<&str>> =
                    q.get("actions").map(|a| a.split(',').map(str::trim).collect());
                let picked_node = q.get("nodes").is_none_or(|n| node::selected(n));
                let tasks: Vec<Value> = current_tasks(detailed)
                    .into_iter()
                    .filter(|t| {
                        actions.as_ref().is_none_or(|a| {
                            a.iter().any(|p| glob_match(p, t["action"].as_str().unwrap_or("")))
                        })
                    })
                    .filter(|t| {
                        q.get("parent_task_id")
                            .is_none_or(|p| t["parent_task_id"].as_str() == Some(p.as_str()))
                    })
                    .filter(|_| picked_node)
                    .collect();
                let key = |t: &Value| format!("{}:{}", node::NODE_ID, t["id"]);
                match q.get("group_by").map_or("nodes", String::as_str) {
                    "none" => (200, json!({"tasks": tasks})),
                    "parents" => {
                        let mut top = Map::new();
                        for t in &tasks {
                            let parent = t["parent_task_id"].as_str();
                            if parent.is_some_and(|p| tasks.iter().any(|x| key(x) == p)) {
                                continue;
                            }
                            let mut e = t.clone();
                            let children: Vec<Value> = tasks
                                .iter()
                                .filter(|c| c["parent_task_id"].as_str() == Some(key(t).as_str()))
                                .cloned()
                                .collect();
                            if !children.is_empty() {
                                e["children"] = Value::Array(children);
                            }
                            top.insert(key(t), e);
                        }
                        (200, json!({"tasks": top}))
                    }
                    _ => {
                        let mut nodes = Map::new();
                        if !tasks.is_empty() {
                            let by_id: Map<String, Value> =
                                tasks.iter().map(|t| (key(t), t.clone())).collect();
                            nodes.insert(
                                node::NODE_ID.into(),
                                json!({"name": node::NODE_NAME,
                                "transport_address": node::TRANSPORT_ADDRESS, "host": node::HOST,
                                "ip": node::TRANSPORT_ADDRESS, "roles": node::ROLES,
                                "attributes": node::attributes(), "tasks": by_id}),
                            );
                        }
                        (200, json!({"nodes": nodes}))
                    }
                }
            }
            ("POST", ["_tasks", "_cancel"]) => (200, json!({"nodes": {}})),
            ("POST", ["_tasks", id, "_cancel"]) | ("GET", ["_tasks", id]) => {
                let Some((n, num)) = id.split_once(':').filter(|(_, x)| x.parse::<u64>().is_ok())
                else {
                    return bad_request(
                        "illegal_argument_exception",
                        &format!("malformed task id {id}"),
                    );
                };
                let _ = num;
                if n != node::NODE_ID {
                    return not_found(&format!(
                        "task [{id}] belongs to the node [{n}] which isn't part of the cluster and there is no record of the task"
                    ));
                }
                if method == "POST" {
                    return not_found(&format!("task [{id}] is not found"));
                }
                not_found(&format!("task [{id}] isn't running and hasn't stored its results"))
            }
            _ => no_handler(method, &format!("/{}", segments.join("/"))),
        }
    }

    /// `/_internal/...`: desired nodes, desired balance, node-removal
    /// prevalidation.
    pub(super) fn internal_api(
        &self,
        method: &str,
        segments: &[&str],
        q: &HashMap<String, String>,
        body: &[u8],
    ) -> Option<(u16, Value)> {
        let r = match (method, segments) {
            ("PUT", ["_internal", "desired_nodes", history, version]) => {
                self.update_desired_nodes(history, version, q, body)
            }
            ("GET", ["_internal", "desired_nodes", "_latest"]) => {
                match &self.0.lock().unwrap().cluster_meta.desired_nodes {
                    Some(d) => (200, d.clone()),
                    None => not_found("Desired nodes not found"),
                }
            }
            ("DELETE", ["_internal", "desired_nodes"]) => {
                self.0.lock().unwrap().cluster_meta.desired_nodes = None;
                bump_state_version();
                (200, json!({"acknowledged": true}))
            }
            ("GET", ["_internal", "desired_balance"]) => self.desired_balance(),
            ("DELETE", ["_internal", "desired_balance"]) => (200, json!({})),
            ("POST", ["_internal", "prevalidate_node_removal"]) => self.prevalidate_node_removal(q),
            _ => return None,
        };
        Some(r)
    }

    fn update_desired_nodes(
        &self,
        history: &str,
        version: &str,
        q: &HashMap<String, String>,
        body: &[u8],
    ) -> (u16, Value) {
        let Ok(version) = version.parse::<i64>() else {
            let mut e = error(
                "illegal_argument_exception",
                &format!("Failed to parse long parameter [version] with value [{version}]"),
                400,
            );
            e["error"]["caused_by"] = json!({"type": "number_format_exception",
                "reason": format!("For input string: \"{version}\"")});
            return (400, e);
        };
        let Some(req) = parse_json(body) else { return (400, malformed_body()) };
        let mut nodes = Vec::new();
        let mut warn = false;
        for n in req["nodes"].as_array().into_iter().flatten() {
            match parse_desired_node(n) {
                Ok((node, had_version)) => {
                    warn |= had_version;
                    nodes.push(node);
                }
                Err(e) => return e,
            }
        }
        let mut problems = Vec::new();
        if history.trim().is_empty() {
            problems.push("historyID should not be empty");
        }
        if version < 0 {
            problems.push("version must be positive");
        }
        let has_master = nodes.iter().any(|n| {
            n["settings"]["node"]["roles"]
                .as_array()
                .is_none_or(|r| r.iter().any(|x| x == "master"))
        });
        if !has_master {
            problems.push("nodes must contain at least one master node");
        }
        if !problems.is_empty() {
            let reason: String =
                problems.iter().enumerate().map(|(i, p)| format!("{}: {p};", i + 1)).collect();
            return bad_request(
                "action_request_validation_exception",
                &format!("Validation Failed: {reason}"),
            );
        }
        for key in ["external_id", "name"] {
            let mut seen = std::collections::HashSet::new();
            for n in &nodes {
                if let Some(v) = n["settings"]["node"][key].as_str()
                    && !seen.insert(v.to_string())
                {
                    let msg =
                        format!("Some nodes contain the same setting value [{v}] for [node.{key}]");
                    return bad_request("illegal_argument_exception", &msg);
                }
            }
        }
        let dry_run = q.get("dry_run").is_some_and(|v| v.is_empty() || v == "true");
        let mut s = self.0.lock().unwrap();
        let mut replaced = false;
        if let Some(cur) = &s.cluster_meta.desired_nodes {
            if cur["history_id"] == json!(history) {
                let cur_version = cur["version"].as_i64().unwrap_or(0);
                if version < cur_version {
                    let msg = format!(
                        "version [{version}] has been superseded by version [{cur_version}] for history [{history}]"
                    );
                    return (409, error("version_conflict_exception", &msg, 409));
                }
                if version == cur_version {
                    let key = |v: &Value| {
                        let mut items: Vec<String> =
                            v.as_array().into_iter().flatten().map(Value::to_string).collect();
                        items.sort();
                        items
                    };
                    if key(&cur["nodes"]) != key(&Value::Array(nodes.clone())) {
                        let msg = format!(
                            "Desired nodes with history [{history}] and version [{version}] already exists with a different definition"
                        );
                        return bad_request("illegal_argument_exception", &msg);
                    }
                }
            } else {
                replaced = true;
            }
        }
        if !dry_run {
            s.cluster_meta.desired_nodes =
                Some(json!({"history_id": history, "version": version, "nodes": nodes}));
            bump_state_version();
        }
        let mut out = json!({"replaced_existing_history_id": replaced, "dry_run": dry_run});
        if warn {
            out[WARNINGS] = json!([
                "[version removal] Specifying node_version in desired nodes requests is deprecated."
            ]);
        }
        (200, out)
    }

    fn desired_balance(&self) -> (u16, Value) {
        let s = self.0.lock().unwrap();
        let mut names: Vec<&String> = s.indices.keys().collect();
        names.sort();
        let mut routing = Map::new();
        let mut sizes = Map::new();
        let (mut shard_count, mut bytes) = (0u64, 0u64);
        for n in &names {
            let i = &s.indices[*n];
            let (_, r, a, _) = index_shards(i);
            let tier = i.settings["index"]["routing"]["allocation"]["include"]["_tier_preference"]
                .as_str()
                .unwrap_or("data_content")
                .to_string();
            let mut shards = Map::new();
            for sh in stats::primaries(i) {
                let started = a > 0;
                if started {
                    shard_count += 1;
                    let b = stats::shard_store(i, sh);
                    bytes += b;
                    sizes.insert(format!("[{n}][{sh}][p]_bytes"), json!(b));
                }
                let copy = |primary: bool, started: bool| {
                    json!({"index": n, "shard_id": sh, "state": if started { "STARTED" } else { "UNASSIGNED" },
                           "primary": primary, "node": if started { json!(node::NODE_ID) } else { Value::Null },
                           "node_is_desired": started, "relocating_node": null,
                           "relocating_node_is_desired": null, "forecast_write_load": null,
                           "forecast_shard_size_in_bytes": null, "tier_preference": [tier]})
                };
                let mut current = vec![copy(true, started)];
                current.extend((0..r).map(|_| copy(false, false)));
                let unassigned = r + u64::from(!started);
                shards.insert(sh.to_string(), json!({"current": current,
                    "desired": {"node_ids": if started { json!([node::NODE_ID]) } else { json!([]) },
                                "total": 1 + r, "unassigned": unassigned, "ignored": unassigned}}));
            }
            routing.insert((*n).clone(), Value::Object(shards));
        }
        let five = |v: f64| json!({"total": v, "min": v, "max": v, "average": v, "std_dev": 0.0});
        let tier = json!({"shard_count": five(shard_count as f64), "undesired_shard_allocation_count": five(0.0),
            "forecast_write_load": five(0.0), "forecast_disk_usage": five(bytes as f64),
            "actual_disk_usage": five(bytes as f64)});
        let mut tiers = Map::new();
        for t in ["data", "data_cold", "data_content", "data_frozen", "data_hot", "data_warm"] {
            tiers.insert(t.into(), tier.clone());
        }
        let mut nodes = Map::new();
        nodes.insert(node::NODE_NAME.into(), json!({"node_id": node::NODE_ID, "roles": node::ROLES,
            "shard_count": shard_count, "undesired_shard_allocation_count": 0, "forecast_write_load": 0.0,
            "forecast_disk_usage_bytes": bytes, "actual_disk_usage_bytes": bytes}));
        let (total, avail) = super::nodes::disk();
        let disk = json!({"path": "/usr/share/elasticsearch/data", "total_bytes": total,
            "used_bytes": total - avail, "free_bytes": avail});
        let mut info_nodes = Map::new();
        info_nodes.insert(
            node::NODE_ID.into(),
            json!({"node_name": node::NODE_NAME,
            "least_available": disk, "most_available": disk}),
        );
        let n = STATE_VERSION.load(Ordering::Relaxed);
        (
            200,
            json!({
                "stats": {"computation_converged_index": n, "computation_active": false,
                          "computation_submitted": n, "computation_executed": n, "computation_converged": n,
                          "computation_iterations": n, "computed_shard_movements": 0,
                          "computation_time_in_millis": 0, "reconciliation_time_in_millis": 0,
                          "unassigned_shards": 0, "total_allocations": shard_count,
                          "undesired_allocations": 0, "undesired_allocations_ratio": 0.0},
                "cluster_balance_stats": {"shard_count": shard_count, "undesired_shard_allocation_count": 0,
                                          "tiers": tiers, "nodes": nodes},
                "routing_table": routing,
                "cluster_info": {"nodes": info_nodes, "shard_sizes": sizes, "shard_data_set_sizes": {},
                                 "shard_paths": {}, "reserved_sizes": []},
            }),
        )
    }

    fn prevalidate_node_removal(&self, q: &HashMap<String, String>) -> (u16, Value) {
        let given: Vec<(&str, &String)> = ["names", "ids", "external_ids"]
            .into_iter()
            .filter_map(|k| q.get(k).filter(|v| !v.is_empty()).map(|v| (k, v)))
            .collect();
        let (kind, list) = match given.as_slice() {
            [] => {
                return validation(
                    "request must contain one of the parameters 'names', 'ids', or 'external_ids'",
                );
            }
            [one] => *one,
            _ => {
                return validation(
                    "request must contain only one of the parameters 'names', 'ids', or 'external_ids'",
                );
            }
        };
        let wanted: Vec<&str> = list.split(',').map(str::trim).collect();
        let ours = match kind {
            "ids" => node::NODE_ID,
            _ => node::NODE_NAME,
        };
        let missing: Vec<&str> = wanted.iter().copied().filter(|w| *w != ours).collect();
        if !missing.is_empty() {
            let what = match kind {
                "names" => "names",
                "ids" => "IDs",
                _ => "external IDs",
            };
            return not_found(&format!("could not resolve node {what} [{}]", missing.join(", ")));
        }
        let s = self.0.lock().unwrap();
        let red = s.indices.values().any(|i| {
            let (p, _, a, _) = index_shards(i);
            a < p
        });
        let result = if red {
            json!({"is_safe": false, "reason": "red_shards_on_node", "message": "node contains copies of the following red shards"})
        } else {
            json!({"is_safe": true, "reason": "no_problems", "message": ""})
        };
        (
            200,
            json!({"is_safe": !red, "message": "", "nodes": [{"id": node::NODE_ID, "name": node::NODE_NAME,
            "external_id": node::NODE_NAME, "result": result}]}),
        )
    }
}

/// Parses one desired node, Elasticsearch's way: `settings` required and
/// naming the node, `processors` or `processors_range` (positive,
/// finite), `memory` and `storage` required. Returns the stored form and
/// whether it carried the deprecated `node_version`.
fn parse_desired_node(n: &Value) -> Result<(Value, bool), (u16, Value)> {
    let parse_err = |inner: Value| {
        let mut e = error(
            "x_content_parse_exception",
            "[1:1] [update_desired_nodes_request] failed to parse field [nodes]",
            400,
        );
        e["error"]["caused_by"] = inner;
        e["error"]["root_cause"] = json!([{"type": "x_content_parse_exception",
            "reason": "[1:1] [update_desired_nodes_request] failed to parse field [nodes]"}]);
        Err((400, e))
    };
    let build_err = |reason: String| {
        parse_err(json!({"type": "x_content_parse_exception",
            "reason": "Failed to build [desired_node] after last required field arrived",
            "caused_by": {"type": "illegal_argument_exception", "reason": reason}}))
    };
    let field_err = |field: &str, reason: String| {
        parse_err(json!({"type": "x_content_parse_exception",
            "reason": format!("[1:1] [desired_node] failed to parse field [{field}]"),
            "caused_by": {"type": "illegal_argument_exception", "reason": reason}}))
    };
    let Some(o) = n.as_object() else {
        return parse_err(
            json!({"type": "illegal_argument_exception", "reason": "Required [settings]"}),
        );
    };
    for k in ["memory", "storage", "settings"] {
        if o.get(k) == Some(&Value::Null) {
            return parse_err(json!({"type": "x_content_parse_exception",
                "reason": format!("[1:1] [desired_node] {k} doesn't support values of type: VALUE_NULL")}));
        }
    }
    let Some(settings) = o.get("settings").and_then(Value::as_object) else {
        return parse_err(
            json!({"type": "illegal_argument_exception", "reason": "Required [settings]"}),
        );
    };
    for k in ["memory", "storage"] {
        if !o.contains_key(k) {
            return parse_err(
                json!({"type": "illegal_argument_exception", "reason": format!("Required [{k}]")}),
            );
        }
    }
    let number = |v: &Value| -> Option<f64> {
        match v {
            Value::Number(n) => n.as_f64(),
            Value::String(s) => s.parse::<f64>().ok(),
            _ => None,
        }
    };
    let positive = |field: &str, v: &Value| -> Result<f64, String> {
        match number(v) {
            Some(f) if f.is_finite() && f > 0.0 => Ok(f),
            _ => {
                let shown = match v {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                };
                Err(format!(
                    "Only a positive number of [{field}] are allowed and [{shown}] was provided"
                ))
            }
        }
    };
    let mut out = Map::new();
    let mut nested = json!({});
    let mut flat: Vec<(String, Value)> = Vec::new();
    flatten_keys("", &Value::Object(settings.clone()), &mut flat);
    for (k, v) in &flat {
        let v = match v {
            Value::String(_) | Value::Array(_) => v.clone(),
            other => json!(other.to_string()),
        };
        flat_insert(&mut nested, k, v);
    }
    let name = |k: &str| nested["node"][k].as_str().map(str::trim).is_some_and(|v| !v.is_empty());
    let named = name("name") || name("external_id");
    if let Some(roles) = nested["node"]["roles"].as_str().map(str::to_string) {
        let known = node::ROLES
            .iter()
            .chain(["voting_only", "index", "search"].iter())
            .copied()
            .collect::<Vec<_>>();
        let list: Vec<&str> = roles.split(',').map(str::trim).filter(|r| !r.is_empty()).collect();
        if let Some(bad) = list.iter().find(|r| !known.contains(r)) {
            return build_err(format!("unknown role [{bad}]"));
        }
        nested["node"]["roles"] = json!(list);
    }
    let processors = o.get("processors");
    let range = o.get("processors_range");
    let processors = match processors {
        Some(p) => match positive("processors", p) {
            Ok(f) => Some(f),
            Err(e) => return field_err("processors", e),
        },
        None => None,
    };
    let range = match range {
        Some(r) => {
            let range_err = |reason: String| {
                parse_err(json!({"type": "x_content_parse_exception",
                    "reason": "[1:1] [desired_node] failed to parse field [processors_range]",
                    "caused_by": {"type": "x_content_parse_exception",
                                  "reason": "Failed to build [processors_range] after last required field arrived",
                                  "caused_by": {"type": "illegal_argument_exception", "reason": reason}}}))
            };
            let Some(min) = r.get("min") else { return range_err("Required [min]".into()) };
            let min = match positive("min", min) {
                Ok(f) => f,
                Err(e) => return range_err(e),
            };
            let max = match r.get("max") {
                None | Some(Value::Null) => None,
                Some(m) => match positive("max", m) {
                    Ok(f) => Some(f),
                    Err(e) => return range_err(e),
                },
            };
            if let Some(max) = max
                && min > max
            {
                return range_err(format!(
                    "min processors must be less than or equal to max processors and it was: min: {min:?} max: {max:?}"
                ));
            }
            let mut m = json!({"min": min});
            if let Some(max) = max {
                m["max"] = json!(max);
            }
            Some(m)
        }
        None => None,
    };
    match (&processors, &range) {
        (Some(_), Some(_)) => {
            return build_err(
                "processors and processors_range were specified, but only one should be specified"
                    .into(),
            );
        }
        (None, None) => {
            return build_err("Either processors or processors_range must be specified".into());
        }
        _ => {}
    }
    if !named {
        return build_err("[node.name] or [node.external_id] is missing or empty".into());
    }
    out.insert("settings".into(), nested);
    if let Some(p) = processors {
        out.insert("processors".into(), json!(p));
    }
    if let Some(r) = range {
        out.insert("processors_range".into(), r);
    }
    out.insert("memory".into(), o["memory"].clone());
    out.insert("storage".into(), o["storage"].clone());
    let had_version = o.get("node_version").is_some();
    if let Some(v) = o.get("node_version") {
        out.insert("node_version".into(), v.clone());
    }
    Ok((Value::Object(out), had_version))
}

/// Inserts `a.b.c = v` as nested objects.
fn flat_insert(root: &mut Value, key: &str, v: Value) {
    let mut cur = root;
    let parts: Vec<&str> = key.split('.').collect();
    for (i, p) in parts.iter().enumerate() {
        if i + 1 == parts.len() {
            cur[*p] = v;
            return;
        }
        if !cur[*p].is_object() {
            cur[*p] = json!({});
        }
        cur = &mut cur[*p];
    }
}

fn or_zero(v: Value, zero: Value) -> Value {
    if v.as_object().is_none_or(Map::is_empty) { zero } else { v }
}

/// Index blocks (`index.blocks.*` settings, closed indices) as the
/// cluster state lists them.
fn blocks(s: &State) -> Value {
    let mut indices = Map::new();
    let mut names: Vec<&String> = s.indices.keys().collect();
    names.sort();
    for n in names {
        let i = &s.indices[n];
        let b = &i.settings["index"]["blocks"];
        let on = |k: &str| {
            let v = b.get(k).or_else(|| i.settings["index"].get(format!("blocks.{k}")));
            v.and_then(|x| x.as_bool().or_else(|| x.as_str().map(|s| s == "true"))) == Some(true)
        };
        let mut m = Map::new();
        if !i.opened {
            m.insert("4".into(), json!({"description": "index closed", "retryable": false, "levels": ["read", "write"]}));
        }
        if on("read_only") {
            m.insert("5".into(), json!({"description": "index read-only (api)", "retryable": false, "levels": ["write", "metadata_write"]}));
        }
        if on("read") {
            m.insert(
                "7".into(),
                json!({"description": "index read (api)", "retryable": false, "levels": ["read"]}),
            );
        }
        if on("write") {
            m.insert("8".into(), json!({"description": "index write (api)", "retryable": false, "levels": ["write"]}));
        }
        if on("metadata") {
            m.insert("9".into(), json!({"description": "index metadata (api)", "retryable": false, "levels": ["metadata_read", "metadata_write"]}));
        }
        if on("read_only_allow_delete") {
            m.insert("12".into(), json!({"description": "disk usage exceeded flood-stage watermark, index has read-only-allow-delete block",
                "retryable": true, "levels": ["write", "metadata_write"]}));
        }
        if !m.is_empty() {
            indices.insert(n.clone(), Value::Object(m));
        }
    }
    if indices.is_empty() { json!({}) } else { json!({"indices": indices}) }
}

/// One index in the cluster state's `metadata.indices`.
fn index_metadata(i: &Index) -> Value {
    let (p, _) = shard_counts(i);
    let uuid = i.settings["index"]["uuid"].as_str().unwrap_or("_na_");
    let terms: Map<String, Value> = (0..p).map(|s| (s.to_string(), json!(1))).collect();
    let in_sync: Map<String, Value> =
        (0..p).map(|s| (s.to_string(), json!([format!("{uuid}{s}")]))).collect();
    let mut aliases: Vec<&String> = i.aliases.keys().collect();
    aliases.sort();
    let mappings = shown_mappings(&i.mappings);
    let mappings = if mappings.as_object().is_some_and(Map::is_empty) {
        json!({})
    } else {
        json!({"_doc": mappings})
    };
    json!({
        "version": 1, "mapping_version": 1, "settings_version": 1, "aliases_version": 1,
        "routing_num_shards": p.max(1) * 1024 / p.max(1).next_power_of_two().max(1),
        "state": if i.opened { "open" } else { "close" },
        "settings": i.settings, "mappings": mappings, "aliases": aliases,
        "primary_terms": terms, "in_sync_allocations": in_sync, "rollover_info": {},
        "mappings_updated_version": 8_512_000, "system": false,
        "timestamp_range": {"unknown": true}, "event_ingested_range": {"unknown": true},
    })
}

/// The routing table entries of one index: shard -> copies.
fn routing_shards(name: &str, i: &Index) -> Value {
    let (p, r, a, _) = index_shards(i);
    let uuid = i.settings["index"]["uuid"].as_str().unwrap_or("_na_");
    let created = i.settings["index"]["creation_date"]
        .as_str()
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(0);
    let unassigned = |primary: bool, shard: u64| {
        json!({"state": "UNASSIGNED", "primary": primary, "node": null, "relocating_node": null,
               "shard": shard, "index": name,
               "recovery_source": {"type": if primary { "EMPTY_STORE" } else { "PEER" }},
               "unassigned_info": {"reason": "INDEX_CREATED", "at": dates::format(created, None, 0),
                                   "delayed": false, "allocation_status": "no_attempt"},
               "relocation_failure_info": {"failed_attempts": 0}})
    };
    let shards: Map<String, Value> = (0..p)
        .map(|s| {
            let mut copies = vec![if a > 0 {
                json!({"state": "STARTED", "primary": true, "node": node::NODE_ID, "relocating_node": null,
                       "shard": s, "index": name, "allocation_id": {"id": format!("{uuid}{s}")},
                       "relocation_failure_info": {"failed_attempts": 0}})
            } else {
                unassigned(true, s)
            }];
            copies.extend((0..r).map(|_| unassigned(false, s)));
            (s.to_string(), Value::Array(copies))
        })
        .collect();
    Value::Object(shards)
}

/// `indices.mappings` sizes of cluster stats: fields over all indices,
/// fields of distinct mappings, and the distinct mappings' bytes.
fn mapping_sizes(s: &State) -> (u64, u64, u64) {
    let mut total = 0;
    let mut distinct: BTreeMap<String, u64> = BTreeMap::new();
    for i in s.indices.values() {
        let mut leaves = Vec::new();
        mapped_leaves(&i.mappings, "", &mut leaves);
        let n = leaves.len() as u64;
        total += n;
        distinct.insert(i.mappings.to_string(), n);
    }
    let fields = distinct.values().sum();
    let bytes =
        distinct.keys().filter(|k| *k != "{\"properties\":{}}").map(|k| k.len() as u64).sum();
    (total, fields, bytes)
}

/// `indices.mappings.runtime_field_types` of cluster stats.
fn runtime_field_stats(s: &State) -> Value {
    // type -> (count, indices, scriptless, shadowed)
    let mut stats: BTreeMap<String, (u64, u64, u64, u64)> = BTreeMap::new();
    for i in s.indices.values() {
        let Some(rt) = i.mappings.get("runtime").and_then(Value::as_object) else { continue };
        let mut leaves = Vec::new();
        mapped_leaves(&i.mappings, "", &mut leaves);
        let mut seen = std::collections::HashSet::new();
        for (name, def) in rt {
            let ty = def.get("type").and_then(Value::as_str).unwrap_or("keyword").to_string();
            let e = stats.entry(ty.clone()).or_default();
            e.0 += 1;
            if seen.insert(ty) {
                e.1 += 1;
            }
            if def.get("script").is_none() {
                e.2 += 1;
            }
            if leaves.iter().any(|(l, _)| l == name) {
                e.3 += 1;
            }
        }
    }
    Value::Array(
        stats
            .into_iter()
            .map(|(name, (count, idx, scriptless, shadowed))| {
                json!({"name": name, "count": count, "index_count": idx, "scriptless_count": scriptless,
                       "shadowed_count": shadowed, "lang": [], "lines_max": 0, "lines_total": 0,
                       "chars_max": 0, "chars_total": 0, "source_max": 0, "source_total": 0,
                       "doc_max": 0, "doc_total": 0})
            })
            .collect(),
    )
}

/// Whether `want` (`5`, `>=2`, `<3`, `le(4)`, ...) holds for `n` nodes.
fn nodes_condition(want: &str, n: i64) -> bool {
    let w = want.trim();
    let (op, num) = if let Some(x) = w.strip_prefix(">=") {
        (">=", x)
    } else if let Some(x) = w.strip_prefix("<=") {
        ("<=", x)
    } else if let Some(x) = w.strip_prefix('>') {
        (">", x)
    } else if let Some(x) = w.strip_prefix('<') {
        ("<", x)
    } else if let Some(x) = w.strip_prefix("ge(").and_then(|x| x.strip_suffix(')')) {
        (">=", x)
    } else if let Some(x) = w.strip_prefix("le(").and_then(|x| x.strip_suffix(')')) {
        ("<=", x)
    } else if let Some(x) = w.strip_prefix("gt(").and_then(|x| x.strip_suffix(')')) {
        (">", x)
    } else if let Some(x) = w.strip_prefix("lt(").and_then(|x| x.strip_suffix(')')) {
        ("<", x)
    } else {
        ("=", w)
    };
    let Ok(m) = num.trim().parse::<i64>() else { return true };
    match op {
        ">=" => n >= m,
        "<=" => n <= m,
        ">" => n > m,
        "<" => n < m,
        _ => n == m,
    }
}

/// The node's features, as `_cluster/state` lists them (and the YAML
/// suite's cluster features are checked against).
const NODE_FEATURES: &[&str] = &[
    "data_stream.auto_sharding",
    "data_stream.lifecycle.global_retention",
    "data_stream.rollover.lazy",
    "desired_node.version_deprecated",
    "features_supported",
    "file_settings",
    "health.dsl.info",
    "health.extended_repository_indicator",
    "knn_retriever_supported",
    "mapper.index_sorting_on_nested",
    "mapper.keyword_dimension_ignore_above",
    "mapper.pass_through_priority",
    "mapper.range.null_values_off_by_one_fix",
    "mapper.source.synthetic_source_fallback",
    "mapper.source.synthetic_source_stored_fields_advance_fix",
    "mapper.track_ignored_source",
    "mapper.vectors.bit_vectors",
    "mapper.vectors.int4_quantization",
    "rest.capabilities_action",
    "retrievers_supported",
    "rrf_retriever_supported",
    "script.hamming",
    "search.vectors.k_param_supported",
    "standard_retriever_supported",
    "stats.include_disk_thresholds",
    "text_similarity_reranker_retriever_supported",
    "unified_highlighter_matched_fields",
    "usage.data_tiers.precalculate_stats",
];

/// `GET /_features`: the system features and what they manage.
pub(super) fn features() -> Value {
    let list = [
        ("logstash_management", "Enables Logstash Central Management pipeline storage"),
        ("searchable_snapshots", "Manages caches and configuration for searchable snapshots"),
        ("security", "Manages configuration for Security features, such as users and roles"),
        ("tasks", "Manages task results"),
        ("inference_plugin", "Inference plugin for managing inference services and inference"),
        ("enrich", "Manages data related to Enrich policies"),
        ("fleet", "Manages configuration for Fleet"),
        ("watcher", "Manages Watch definitions and state"),
        ("geoip", "Manages data related to GeoIP database downloader"),
        ("machine_learning", "Provides anomaly detection and forecasting functionality"),
        ("ent_search", "Manages configuration for Enterprise Search features"),
        ("async_search", "Manages results of async searches"),
        ("synonyms", "Manages synonyms"),
        ("kibana", "Manages Kibana configuration and reports"),
        ("transform", "Manages configuration and state for transforms"),
    ];
    json!({"features": list.iter().map(|(n, d)| json!({"name": n, "description": d})).collect::<Vec<_>>()})
}

/// `POST /_features/_reset`: there is no system state to reset.
pub(super) fn reset_features() -> Value {
    let list = features();
    json!({"features": list["features"].as_array().into_iter().flatten()
        .map(|f| json!({"feature_name": f["name"], "status": "SUCCESS"})).collect::<Vec<_>>()})
}

/// `GET /_capabilities`: whether a REST endpoint supports a method,
/// parameters and named capabilities, as an 8.15.3 node answers. Only
/// handlers that declare their parameters or capabilities can say no to
/// them.
pub(super) fn capabilities(q: &HashMap<String, String>) -> (u16, Value) {
    let method = q.get("method").map_or("GET", String::as_str);
    let Some(path) = q.get("path") else {
        return (
            500,
            error(
                "null_pointer_exception",
                "Cannot invoke \"String.length()\" because \"s\" is null",
                500,
            ),
        );
    };
    let list = |k: &str| -> Vec<&str> {
        q.get(k)
            .map(|v| v.split(',').map(str::trim).filter(|x| !x.is_empty()).collect())
            .unwrap_or_default()
    };
    let params = list("parameters");
    let caps = list("capabilities");
    // (method, path, declared parameters, declared capabilities)
    #[allow(clippy::type_complexity)]
    let declared: &[(&str, &str, Option<&[&str]>, &[&str])] = &[
        ("GET", "/_capabilities", Some(&["method", "path", "parameters", "capabilities"]), &[]),
        (
            "DELETE",
            "/_snapshot/{repository}/{snapshot}",
            Some(&["master_timeout", "wait_for_completion"]),
            &[],
        ),
        ("PUT", "/{index}", None, &["logsdb_index_mode"]),
        ("GET", "/_cluster/stats", None, &["human-readable-total-docs-size"]),
    ];
    let common = ["pretty", "human", "error_trace", "filter_path", "format"];
    let supported = if !matches!(method, "GET" | "HEAD" | "POST" | "PUT" | "DELETE") {
        false
    } else if let Some((_, _, p, c)) =
        declared.iter().find(|(m, d, _, _)| *d == path && *m == method)
    {
        let params_ok =
            p.is_none_or(|p| params.iter().all(|x| p.contains(x) || common.contains(x)));
        params_ok && caps.iter().all(|x| c.contains(x))
    } else if declared.iter().any(|(_, d, _, _)| *d == path) {
        false
    } else {
        caps.is_empty()
    };
    (
        200,
        json!({"_nodes": node::nodes_header(1), "cluster_name": node::CLUSTER_NAME, "supported": supported}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wait_for_nodes_conditions() {
        assert!(nodes_condition(">=1", 1));
        assert!(!nodes_condition("10", 1));
        assert!(nodes_condition("le(2)", 1));
        assert!(!nodes_condition(">1", 1));
    }

    #[test]
    fn desired_node_validation() {
        let ok = json!({"settings": {"node.name": "a"}, "processors": 8, "memory": "1gb", "storage": "1gb"});
        let (n, v) = parse_desired_node(&ok).unwrap();
        assert!(!v);
        assert_eq!(n["settings"]["node"]["name"], json!("a"));
        assert_eq!(n["processors"], json!(8.0));
        let nan = json!({"settings": {"node.name": "a"}, "processors": "NaN", "memory": "1gb", "storage": "1gb"});
        assert_eq!(
            parse_desired_node(&nan).err().unwrap().1["error"]["type"],
            json!("x_content_parse_exception")
        );
        let nameless = json!({"settings": {}, "processors": 8, "memory": "1gb", "storage": "1gb"});
        let e = parse_desired_node(&nameless).err().unwrap().1;
        assert_eq!(
            e["error"]["caused_by"]["caused_by"]["reason"],
            json!("[node.name] or [node.external_id] is missing or empty")
        );
    }

    #[test]
    fn capabilities_answers() {
        let q =
            |p: &[(&str, &str)]| p.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        let ask = |p: &[(&str, &str)]| capabilities(&q(p)).1["supported"].clone();
        assert_eq!(
            ask(&[("method", "GET"), ("path", "/_capabilities"), ("parameters", "method,path")]),
            json!(true)
        );
        assert_eq!(
            ask(&[("method", "GET"), ("path", "/_capabilities"), ("parameters", "unknown")]),
            json!(false)
        );
        assert_eq!(
            ask(&[("method", "PUT"), ("path", "/{index}"), ("capabilities", "logsdb_index_mode")]),
            json!(true)
        );
        assert_eq!(
            ask(&[("method", "GET"), ("path", "/_search"), ("capabilities", "xyz")]),
            json!(false)
        );
    }
}
