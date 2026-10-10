//! `_nodes` APIs about the single node: info, stats, usage, hot threads
//! and reloading secure settings.

use super::node;
use super::stats::{self, StatsRequest};
use super::*;

/// (total, available) bytes of the data path's disk. The simulation has
/// no data path of its own to measure, so this is a fixed, roomy disk.
pub(super) fn disk() -> (u64, u64) {
    (107_374_182_400, 85_899_345_920)
}

/// (total, free) bytes of memory, from `/proc/meminfo` where there is one.
pub(super) fn memory() -> (u64, u64) {
    let read = || -> Option<(u64, u64)> {
        let text = std::fs::read_to_string("/proc/meminfo").ok()?;
        let field = |name: &str| {
            text.lines()
                .find(|l| l.starts_with(name))
                .and_then(|l| l.split_whitespace().nth(1))
                .and_then(|v| v.parse::<u64>().ok())
                .map(|kb| kb * 1024)
        };
        Some((field("MemTotal:")?, field("MemAvailable:").or_else(|| field("MemFree:"))?))
    };
    read().unwrap_or((8_589_934_592, 4_294_967_296))
}

/// Mapped fields of an index as node stats count them: the 14 metadata
/// fields, every field and multi-field, object mappers and runtime
/// fields.
pub(super) fn mapping_field_count(i: &Index) -> u64 {
    fn walk(props: &Value) -> u64 {
        let Some(p) = props.as_object() else { return 0 };
        p.values()
            .map(|def| {
                let object = def.get("properties").is_some()
                    && def.get("type").is_none_or(|t| t == "object" || t == "nested");
                if object {
                    1 + walk(&def["properties"])
                } else {
                    1 + def.get("fields").and_then(Value::as_object).map_or(0, |f| f.len() as u64)
                }
            })
            .sum()
    }
    14 + walk(&i.mappings["properties"])
        + i.mappings.get("runtime").and_then(Value::as_object).map_or(0, |r| r.len() as u64)
}

/// Node-info metrics.
const INFO_METRICS: &[&str] = &[
    "settings",
    "os",
    "process",
    "jvm",
    "thread_pool",
    "transport",
    "http",
    "remote_cluster_server",
    "plugins",
    "ingest",
    "aggregations",
    "indices",
];

/// Node-stats metrics (`indices` aside).
const STATS_METRICS: &[&str] = &[
    "os",
    "process",
    "jvm",
    "thread_pool",
    "fs",
    "transport",
    "http",
    "breaker",
    "script",
    "discovery",
    "ingest",
    "adaptive_selection",
    "script_cache",
    "indexing_pressure",
    "repositories",
    "allocations",
];

fn node_settings() -> Value {
    let http = node::http_address();
    let port = http.rsplit(':').next().unwrap_or("9200").to_string();
    json!({
        "cluster": {"name": node::CLUSTER_NAME},
        "node": {"name": node::NODE_NAME,
                 "attr": {"xpack": {"installed": "true"}, "transform": {"config_version": "10.0.0"},
                          "ml": {"config_version": "12.0.0"}}},
        "path": {"home": "/usr/share/elasticsearch", "data": ["/usr/share/elasticsearch/data"],
                 "logs": "/usr/share/elasticsearch/logs"},
        "discovery": {"type": "single-node"},
        "http": {"type": {"default": "netty4"}, "port": port},
        "transport": {"type": {"default": "netty4"}, "port": "9300"},
        "xpack": {"security": {"enabled": "false"}},
    })
}

impl Engine {
    /// Everything under `/_nodes`.
    pub(super) fn nodes_route(
        &self,
        method: &str,
        segments: &[&str],
        q: &HashMap<String, String>,
        body: &[u8],
        path: &str,
    ) -> (u16, Value) {
        // `/_nodes/<node_id>/<action>...` or `/_nodes/<action>...`.
        let actions = ["stats", "usage", "hot_threads", "hotthreads", "reload_secure_settings"];
        let (nodes, rest): (&str, &[&str]) = match segments.get(1) {
            Some(a) if actions.contains(a) => ("_all", &segments[1..]),
            Some(n) if segments.len() > 2 && actions.contains(&segments[2]) => (n, &segments[2..]),
            _ => ("_all", &segments[1..]),
        };
        match rest.first().copied() {
            Some("stats") if method == "GET" => {
                self.nodes_stats(nodes, rest.get(1).copied(), rest.get(2).copied(), q, path)
            }
            Some("usage") if method == "GET" => nodes_usage(nodes),
            Some("hot_threads" | "hotthreads") if method == "GET" => hot_threads(nodes, q),
            Some("reload_secure_settings") if method == "POST" => reload_secure_settings(nodes, body),
            _ if method == "GET" && rest.len() <= 2 => {
                // `/_nodes`, `/_nodes/<ids or metrics>`, `/_nodes/<ids>/<metrics>`.
                let (ids, metrics) = match rest {
                    [] => ("_all", "_all"),
                    [one] => {
                        let all_metrics = one.split(',').all(|m| {
                            INFO_METRICS.contains(&m.trim()) || m == "_all" || m == "_none"
                        });
                        if all_metrics { ("_all", *one) } else { (*one, "_all") }
                    }
                    [ids, metrics, ..] => (*ids, *metrics),
                };
                self.nodes_info(ids, metrics, q)
            }
            _ => no_handler(method, path),
        }
    }

    fn nodes_info(&self, ids: &str, metrics: &str, q: &HashMap<String, String>) -> (u16, Value) {
        let mut wanted: Vec<&str> = Vec::new();
        for m in metrics.split(',').map(str::trim) {
            match m {
                "_all" => wanted.extend(INFO_METRICS),
                "_none" => {}
                m => wanted.push(m),
            }
        }
        let mut nodes = Map::new();
        if node::selected(ids) {
            let data = node::info_data();
            let mut n = json!({
                "name": node::NODE_NAME, "transport_address": node::TRANSPORT_ADDRESS,
                "host": node::HOST, "ip": node::HOST, "version": node::VERSION,
                "transport_version": 8_702_003, "index_version": 8_512_000,
                "component_versions": {"transform_config_version": 10_000_099, "ml_config_version": 12_000_099},
                "build_flavor": "default", "build_type": "docker", "build_hash": "noida",
            });
            if wanted.contains(&"indices") {
                n["total_indexing_buffer"] = json!(53_687_091);
            }
            n["roles"] = json!(node::ROLES);
            n["attributes"] = node::attributes();
            let http = node::http_address();
            for m in INFO_METRICS {
                if !wanted.contains(m) {
                    continue;
                }
                match *m {
                    "settings" => n["settings"] = node_settings(),
                    "os" | "process" | "thread_pool" | "ingest" | "aggregations" => {
                        n[*m] = data[*m].clone();
                    }
                    "jvm" => {
                        let mut j = data["jvm"].clone();
                        j["pid"] = json!(std::process::id());
                        j["start_time_in_millis"] = json!(node::start_millis());
                        n["jvm"] = j;
                    }
                    "transport" => {
                        n["transport"] = json!({"bound_address": [node::TRANSPORT_ADDRESS],
                            "publish_address": node::TRANSPORT_ADDRESS, "profiles": {}});
                    }
                    "http" => {
                        n["http"] = json!({"bound_address": [http], "publish_address": http,
                            "max_content_length_in_bytes": 104_857_600});
                    }
                    "plugins" => {
                        n["plugins"] = json!([]);
                        n["modules"] = data["modules"].clone();
                    }
                    _ => {}
                }
            }
            if let Some(p) = n.get_mut("process") {
                p["id"] = json!(std::process::id());
            }
            nodes.insert(node::NODE_ID.into(), n);
        }
        let mut out = json!({"_nodes": node::nodes_header(nodes.len()),
                             "cluster_name": node::CLUSTER_NAME, "nodes": nodes});
        if stats::human(q) {
            node::add_human(&mut out);
        }
        (200, out)
    }

    fn nodes_stats(
        &self,
        ids: &str,
        metric: Option<&str>,
        index_metric: Option<&str>,
        q: &HashMap<String, String>,
        path: &str,
    ) -> (u16, Value) {
        let mut wanted: Vec<&str> = Vec::new();
        let mut bad = Vec::new();
        match metric {
            None | Some("_all") => {
                wanted.extend(STATS_METRICS);
                wanted.push("indices");
            }
            Some(list) => {
                for m in list.split(',').map(str::trim).filter(|m| !m.is_empty()) {
                    if m == "_all" {
                        wanted.extend(STATS_METRICS);
                        wanted.push("indices");
                    } else if m == "indices" || STATS_METRICS.contains(&m) {
                        wanted.push(m);
                    } else {
                        bad.push(m.to_string());
                    }
                }
            }
        }
        if !bad.is_empty() {
            let mut known: Vec<&str> = STATS_METRICS.to_vec();
            known.extend(["indices", "_all"]);
            let msg = node::unrecognized(path, &bad, &known, "metric");
            return (400, error("illegal_argument_exception", &msg, 400));
        }
        if let Some(im) = index_metric
            && !wanted.contains(&"indices")
        {
            let msg = format!(
                "request [{path}] contains index metrics [{im}] but indices stats not requested"
            );
            return (400, error("illegal_argument_exception", &msg, 400));
        }
        let req = match StatsRequest::parse(index_metric, q, path, "index metric") {
            Ok(r) => r,
            Err(e) => return e,
        };
        let level = q.get("level").map_or("node", String::as_str);
        if !matches!(level, "node" | "indices" | "shards") {
            let msg = format!(
                "level parameter must be one of [node] or [indices] or [shards] but was [{level}]"
            );
            return (400, error("illegal_argument_exception", &msg, 400));
        }
        let mut nodes = Map::new();
        if node::selected(ids) {
            let s = self.0.lock().unwrap();
            let now = node::now_millis();
            let mut n = json!({
                "timestamp": now, "name": node::NODE_NAME,
                "transport_address": node::TRANSPORT_ADDRESS, "host": node::HOST,
                "ip": node::TRANSPORT_ADDRESS, "roles": node::ROLES, "attributes": node::attributes(),
            });
            let open: Vec<&String> = {
                let mut v: Vec<&String> = s.indices.iter().filter(|(_, i)| i.opened).map(|(k, _)| k).collect();
                v.sort();
                v
            };
            if wanted.contains(&"indices") {
                n["indices"] = indices_section(&s, &open, &req, level);
            }
            let (mem_total, mem_free) = memory();
            let (disk_total, disk_avail) = disk();
            let uptime = node::uptime_millis();
            for m in STATS_METRICS {
                if !wanted.contains(m) {
                    continue;
                }
                let (key, v) = match *m {
                    "os" => ("os", json!({"timestamp": now,
                        "cpu": {"percent": 1, "load_average": {"1m": 0.0, "5m": 0.0, "15m": 0.0}},
                        "mem": {"total_in_bytes": mem_total, "adjusted_total_in_bytes": mem_total,
                                "free_in_bytes": mem_free, "used_in_bytes": mem_total - mem_free,
                                "free_percent": mem_free * 100 / mem_total.max(1),
                                "used_percent": (mem_total - mem_free) * 100 / mem_total.max(1)},
                        "swap": {"total_in_bytes": 0, "free_in_bytes": 0, "used_in_bytes": 0}})),
                    "process" => ("process", json!({"timestamp": now, "open_file_descriptors": 64,
                        "max_file_descriptors": 65535, "cpu": {"percent": 0, "total_in_millis": 0},
                        "mem": {"total_virtual_in_bytes": 0}})),
                    "jvm" => ("jvm", json!({"timestamp": now, "uptime_in_millis": uptime,
                        "mem": {"heap_used_in_bytes": 134_217_728u64, "heap_used_percent": 25,
                                "heap_committed_in_bytes": 536_870_912u64,
                                "heap_max_in_bytes": 536_870_912u64,
                                "non_heap_used_in_bytes": 0, "non_heap_committed_in_bytes": 0,
                                "pools": {"young": {"used_in_bytes": 0, "max_in_bytes": 0, "peak_used_in_bytes": 0, "peak_max_in_bytes": 0},
                                          "old": {"used_in_bytes": 0, "max_in_bytes": 536_870_912u64, "peak_used_in_bytes": 0, "peak_max_in_bytes": 536_870_912u64},
                                          "survivor": {"used_in_bytes": 0, "max_in_bytes": 0, "peak_used_in_bytes": 0, "peak_max_in_bytes": 0}}},
                        "threads": {"count": 64, "peak_count": 64},
                        "gc": {"collectors": {"young": {"collection_count": 0, "collection_time_in_millis": 0},
                                              "old": {"collection_count": 0, "collection_time_in_millis": 0}}},
                        "buffer_pools": {"mapped": {"count": 0, "used_in_bytes": 0, "total_capacity_in_bytes": 0},
                                         "direct": {"count": 0, "used_in_bytes": 0, "total_capacity_in_bytes": 0}},
                        "classes": {"current_loaded_count": 0, "total_loaded_count": 0, "total_unloaded_count": 0}})),
                    "thread_pool" => {
                        let pools: Map<String, Value> = node::thread_pools()
                            .into_iter()
                            .map(|(name, info)| (name, node::thread_pool_stats(&info)))
                            .collect();
                        ("thread_pool", Value::Object(pools))
                    }
                    "fs" => {
                        let path = "/usr/share/elasticsearch/data";
                        ("fs", json!({"timestamp": now,
                            "total": {"total_in_bytes": disk_total, "free_in_bytes": disk_avail, "available_in_bytes": disk_avail},
                            "data": [{"path": path, "mount": "/ (overlay)", "type": "overlay",
                                      "total_in_bytes": disk_total, "free_in_bytes": disk_avail,
                                      "available_in_bytes": disk_avail,
                                      "low_watermark_free_space_in_bytes": disk_total / 100 * 15,
                                      "high_watermark_free_space_in_bytes": disk_total / 10,
                                      "flood_stage_free_space_in_bytes": disk_total / 20}]}))
                    }
                    "transport" => ("transport", json!({"server_open": 0, "total_outbound_connections": 0,
                        "rx_count": 0, "rx_size_in_bytes": 0, "tx_count": 0, "tx_size_in_bytes": 0,
                        "inbound_handling_time_histogram": [], "outbound_handling_time_histogram": []})),
                    "http" => ("http", node::http_stats()),
                    "breaker" => {
                        let b = |limit: u64, overhead: f64| json!({"limit_size_in_bytes": limit,
                            "limit_size": node::human_size(limit), "estimated_size_in_bytes": 0,
                            "estimated_size": "0b", "overhead": overhead, "tripped": 0});
                        ("breakers", json!({"fielddata": b(214_748_364, 1.03), "request": b(322_122_547, 1.0),
                            "inflight_requests": b(536_870_912, 2.0), "parent": b(510_027_366, 1.0)}))
                    }
                    "script" => ("script", json!({"compilations": 0, "cache_evictions": 0,
                        "compilation_limit_triggered": 0})),
                    "discovery" => {
                        let t = |extra: &[&str]| {
                            let mut m = Map::new();
                            m.insert("count".into(), json!(0));
                            for k in extra {
                                m.insert(format!("{k}_time_millis"), json!(0));
                            }
                            Value::Object(m)
                        };
                        let phases = ["computation", "publication", "context_construction", "commit",
                                      "completion", "master_apply", "notification"];
                        ("discovery", json!({
                            "cluster_state_queue": {"total": 0, "pending": 0, "committed": 0},
                            "serialized_cluster_states": {
                                "full_states": {"count": 0, "uncompressed_size_in_bytes": 0, "compressed_size_in_bytes": 0},
                                "diffs": {"count": 0, "uncompressed_size_in_bytes": 0, "compressed_size_in_bytes": 0}},
                            "published_cluster_states": {"full_states": 0, "incompatible_diffs": 0, "compatible_diffs": 0},
                            "cluster_state_update": {"unchanged": t(&["computation", "notification"]),
                                                     "success": t(&phases), "failure": t(&phases)},
                            "cluster_applier_stats": {"recordings": [
                                {"name": "IndicesClusterStateService#applyClusterState",
                                 "cumulative_execution_count": 1, "cumulative_execution_time_millis": 1}]},
                        }))
                    }
                    "ingest" => ("ingest", json!({"total": {"count": 0, "time_in_millis": 0, "current": 0, "failed": 0},
                        "pipelines": {}})),
                    "adaptive_selection" => ("adaptive_selection", json!({})),
                    "script_cache" => ("script_cache", json!({"sum": {"compilations": 0, "cache_evictions": 0,
                        "compilation_limit_triggered": 0}})),
                    "indexing_pressure" => {
                        let cur = json!({"combined_coordinating_and_primary_in_bytes": 0, "coordinating_in_bytes": 0,
                            "primary_in_bytes": 0, "replica_in_bytes": 0, "all_in_bytes": 0});
                        let mut total = cur.clone();
                        for k in ["coordinating_rejections", "primary_rejections", "replica_rejections",
                                  "primary_document_rejections"] {
                            total[k] = json!(0);
                        }
                        ("indexing_pressure", json!({"memory": {"current": cur, "total": total,
                            "limit_in_bytes": 53_687_091}}))
                    }
                    "repositories" => ("repositories", json!({})),
                    "allocations" => {
                        let (mut shards, mut bytes) = (0u64, 0u64);
                        for name in &open {
                            let i = &s.indices[*name];
                            if super::cluster::allocation_disabled(i) {
                                continue;
                            }
                            shards += shard_counts(i).0;
                            bytes += stats::primaries(i).iter().map(|sh| stats::shard_store(i, *sh)).sum::<u64>();
                        }
                        ("allocations", json!({"shards": shards, "undesired_shards": 0,
                            "forecasted_ingest_load": 0.0, "forecasted_disk_usage_in_bytes": bytes,
                            "current_disk_usage_in_bytes": bytes}))
                    }
                    _ => continue,
                };
                n[key] = v;
            }
            nodes.insert(node::NODE_ID.into(), n);
        }
        let mut out = json!({"_nodes": node::nodes_header(nodes.len()),
                             "cluster_name": node::CLUSTER_NAME, "nodes": nodes});
        if stats::human(q) {
            node::add_human(&mut out);
        }
        (200, out)
    }
}

/// The `indices` section of node stats: every shard on the node summed
/// (all sections present even without shards), plus `mappings`; per
/// index or per shard with `level`.
fn indices_section(s: &State, names: &[&String], r: &StatsRequest, level: &str) -> Value {
    let mut sum = zero_sections(r);
    let mut per_index = Map::new();
    let mut per_shard = Map::new();
    let mut fields = 0u64;
    for n in names {
        let i = &s.indices[*n];
        let sec = stats::index_sections(i, r);
        stats::add_stats(&mut sum, &sec);
        let count = mapping_field_count(i);
        fields += count;
        if level == "indices" {
            let mut e = sec.clone();
            if r.metrics.contains("mappings") {
                e["mappings"] = json!({"total_count": count, "total_estimated_overhead_in_bytes": count * 1024});
            }
            per_index.insert((*n).clone(), e);
        }
        if level == "shards" {
            let shards: Vec<Value> = stats::primaries(i)
                .into_iter()
                .map(|sh| {
                    let mut m = Map::new();
                    m.insert(sh.to_string(), Value::Object(stats::shard_sections(i, sh, r)));
                    Value::Object(m)
                })
                .collect();
            per_shard.insert((*n).clone(), Value::Array(shards));
        }
    }
    if r.metrics.contains("mappings") {
        // Sections stay in Elasticsearch's order: `mappings` after `bulk`.
        let mut ordered = Map::new();
        let old = sum.as_object().cloned().unwrap_or_default();
        for (k, v) in old {
            let after = k == "bulk";
            ordered.insert(k, v);
            if after {
                ordered.insert(
                    "mappings".into(),
                    json!({"total_count": fields, "total_estimated_overhead_in_bytes": fields * 1024}),
                );
            }
        }
        if !ordered.contains_key("mappings") {
            ordered.insert(
                "mappings".into(),
                json!({"total_count": fields, "total_estimated_overhead_in_bytes": fields * 1024}),
            );
        }
        sum = Value::Object(ordered);
    }
    if level == "indices" {
        sum["indices"] = Value::Object(per_index);
    }
    if level == "shards" {
        sum["shards"] = Value::Object(per_shard);
    }
    sum
}

/// All sections with every number zeroed (a node without shards).
fn zero_sections(r: &StatsRequest) -> Value {
    fn zero(v: &mut Value) {
        match v {
            Value::Number(_) => *v = json!(0),
            Value::Object(m) => {
                for (k, x) in m.iter_mut() {
                    if k == "max_unsafe_auto_id_timestamp" {
                        *x = json!(-1);
                    } else if k == "write_load" {
                        *x = json!(0.0);
                    } else {
                        zero(x);
                    }
                }
            }
            _ => {}
        }
    }
    let mut v = Value::Object(stats::shard_sections(&Index::default(), 0, r));
    zero(&mut v);
    v
}

fn nodes_usage(ids: &str) -> (u16, Value) {
    let mut nodes = Map::new();
    if node::selected(ids) {
        nodes.insert(
            node::NODE_ID.into(),
            json!({"timestamp": node::now_millis(), "since": node::start_millis(),
                   "rest_actions": {}, "aggregations": {}}),
        );
    }
    (200, json!({"_nodes": node::nodes_header(nodes.len()), "cluster_name": node::CLUSTER_NAME, "nodes": nodes}))
}

/// `GET /_nodes/hot_threads`: the node's header and an empty sample (no
/// thread is busy in the simulation).
fn hot_threads(ids: &str, q: &HashMap<String, String>) -> (u16, Value) {
    let ty = q.get("type").map_or("cpu", String::as_str);
    if !matches!(ty, "cpu" | "wait" | "block" | "mem") {
        let msg = format!("type not supported [{ty}]");
        return (400, error("illegal_argument_exception", &msg, 400));
    }
    if let Some(sort) = q.get("sort")
        && !matches!(sort.as_str(), "cpu" | "total")
    {
        let msg = format!("sort order not supported [{sort}]");
        return (400, error("illegal_argument_exception", &msg, 400));
    }
    if !node::selected(ids) {
        return (200, json!({ cat::RAW_TEXT: "" }));
    }
    let interval = q.get("interval").map_or("500ms", String::as_str);
    let threads = q.get("threads").map_or("3", String::as_str);
    let ignore_idle = q.get("ignore_idle_threads").map_or("true", String::as_str);
    let attrs = node::attributes()
        .as_object()
        .map(|m| {
            m.iter().map(|(k, v)| format!("{k}={}", v.as_str().unwrap_or(""))).collect::<Vec<_>>().join(", ")
        })
        .unwrap_or_default();
    let now = dates::format(node::now_millis(), None, 0);
    let text = format!(
        "::: {{{}}}{{{}}}{{{}}}{{{}}}{{{}}}{{{}}}{{{}}}{{{}}}{{7000099-8512000}}{{{attrs}}}\n   Hot threads at {now}, interval={interval}, busiestThreads={threads}, ignoreIdleThreads={ignore_idle}:\n\n",
        node::NODE_NAME,
        node::NODE_ID,
        node::EPHEMERAL_ID,
        node::NODE_NAME,
        node::HOST,
        node::TRANSPORT_ADDRESS,
        node::ROLE_CHARS,
        node::VERSION,
    );
    (200, json!({ cat::RAW_TEXT: text }))
}

/// `POST /_nodes/reload_secure_settings`: the keystore has no password,
/// so any password given is wrong.
fn reload_secure_settings(ids: &str, body: &[u8]) -> (u16, Value) {
    let req = parse_json(body).unwrap_or_else(|| json!({}));
    let password = req.get("secure_settings_password").and_then(Value::as_str).unwrap_or("");
    let mut nodes = Map::new();
    if node::selected(ids) {
        let mut n = json!({"name": node::NODE_NAME});
        if !password.is_empty() {
            n["reload_exception"] = json!({"type": "security_exception",
                "reason": "Provided keystore password was incorrect"});
        }
        nodes.insert(node::NODE_ID.into(), n);
    }
    (200, json!({"_nodes": node::nodes_header(nodes.len()), "cluster_name": node::CLUSTER_NAME, "nodes": nodes}))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mapping_count_includes_metadata_objects_and_runtime() {
        let i = Index {
            mappings: json!({"runtime": {"r": {"type": "keyword"}}, "properties": {
                "a": {"type": "text", "fields": {"k": {"type": "keyword"}}},
                "o": {"properties": {"b": {"type": "long"}}}}}),
            ..Index::default()
        };
        // 14 metadata + a + a.k + o + o.b + r
        assert_eq!(mapping_field_count(&i), 19);
    }
}
