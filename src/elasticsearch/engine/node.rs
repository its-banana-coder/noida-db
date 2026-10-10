//! The one simulated node: its identity (id, name, roles, attributes),
//! the facts `_nodes`, `_cluster` and `_cat` report about it, its live
//! HTTP-client statistics and its task ids.

use serde_json::{Map, Value, json};
use std::cell::RefCell;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};

/// A 22-character id, as Elasticsearch generates node ids.
pub(crate) const NODE_ID: &str = "noida_local_node_00001";
pub(crate) const NODE_NAME: &str = "noida-db";
pub(crate) const EPHEMERAL_ID: &str = "noida_ephemeral_000001";
pub(crate) const CLUSTER_NAME: &str = "docker-cluster";
pub(crate) const CLUSTER_UUID: &str = "noida-local";
pub(crate) const VERSION: &str = "8.15.3";
pub(crate) const TRANSPORT_ADDRESS: &str = "127.0.0.1:9300";
pub(crate) const HOST: &str = "127.0.0.1";
/// Every role a default node has, sorted as Elasticsearch prints them.
pub(crate) const ROLES: &[&str] = &[
    "data",
    "data_cold",
    "data_content",
    "data_frozen",
    "data_hot",
    "data_warm",
    "ingest",
    "master",
    "ml",
    "remote_cluster_client",
    "transform",
];
/// The roles as `_cat` abbreviates them.
pub(crate) const ROLE_CHARS: &str = "cdfhilmrstw";

/// The node's attributes (what a default distribution sets).
pub(crate) fn attributes() -> Value {
    json!({"xpack.installed": "true", "transform.config_version": "10.0.0", "ml.config_version": "12.0.0"})
}

/// The static parts of node info (thread pools, modules, ingest
/// processors, aggregation types) as an 8.15.3 node reports them.
pub(crate) fn info_data() -> &'static Value {
    static DATA: OnceLock<Value> = OnceLock::new();
    DATA.get_or_init(|| serde_json::from_str(include_str!("node_info.json")).unwrap_or_default())
}

/// When this node started (epoch millis).
pub(crate) fn start_millis() -> i64 {
    static START: OnceLock<i64> = OnceLock::new();
    *START.get_or_init(now_millis)
}

pub(crate) fn now_millis() -> i64 {
    super::dates::now_ms()
}

pub(crate) fn uptime_millis() -> i64 {
    (now_millis() - start_millis()).max(0)
}

static HTTP_ADDRESS: Mutex<Option<String>> = Mutex::new(None);

/// Records the address the HTTP listener is bound to.
pub(crate) fn set_http_address(addr: std::net::SocketAddr) {
    start_millis();
    *HTTP_ADDRESS.lock().unwrap() = Some(addr.to_string());
}

pub(crate) fn http_address() -> String {
    HTTP_ADDRESS.lock().unwrap().clone().unwrap_or_else(|| "127.0.0.1:9200".to_string())
}

/// The node's identity fields shared by node info, cluster state and
/// allocation output (`roles` sorted, `attributes`).
pub(crate) fn discovery_node() -> Value {
    json!({
        "name": NODE_NAME, "ephemeral_id": EPHEMERAL_ID, "transport_address": TRANSPORT_ADDRESS,
        "external_id": NODE_NAME, "attributes": attributes(), "roles": ROLES,
        "version": VERSION, "min_index_version": 7000099, "max_index_version": 8512000,
    })
}

/// Whether a node selector (`_all`, `_local`, `_master`, an id or name,
/// a wildcard, `role:true|false`, an address or `attr:value`; several
/// comma-separated) picks this node.
pub(crate) fn selected(spec: &str) -> bool {
    let mut picked = false;
    for part in spec.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        let hit = match part {
            "_all" | "*" | "_local" | "_master" => true,
            p if p == NODE_ID || p == NODE_NAME || p == HOST || p == TRANSPORT_ADDRESS => true,
            p if p.contains('*') => glob(p, NODE_ID) || glob(p, NODE_NAME),
            p => match p.split_once(':') {
                Some((role, flag)) if role == "master" || role == "data" || role == "ingest" => {
                    flag == "true"
                }
                Some(("coordinating_only", flag)) => flag == "false",
                Some((k, v)) if ROLES.contains(&k) => v == "true",
                Some((k, v)) => {
                    attributes().get(k).and_then(Value::as_str).is_some_and(|a| glob(v, a))
                }
                None => false,
            },
        };
        picked |= hit;
    }
    picked
}

fn glob(pat: &str, s: &str) -> bool {
    let parts: Vec<&str> = pat.split('*').collect();
    if parts.len() == 1 {
        return pat == s;
    }
    let mut rest = s;
    for (i, p) in parts.iter().enumerate() {
        if i == 0 {
            match rest.strip_prefix(p) {
                Some(r) => rest = r,
                None => return false,
            }
        } else if i == parts.len() - 1 {
            return rest.ends_with(p);
        } else if let Some(at) = rest.find(p) {
            rest = &rest[at + p.len()..];
        } else {
            return false;
        }
    }
    true
}

/// The `_nodes` header of a node-level response.
pub(crate) fn nodes_header(n: usize) -> Value {
    json!({"total": n, "successful": n, "failed": 0})
}

// --- HTTP client statistics ----------------------------------------------

#[derive(Clone)]
struct HttpClient {
    id: u64,
    agent: Option<String>,
    local: String,
    remote: String,
    last_uri: Option<String>,
    opened: i64,
    closed: Option<i64>,
    last_request: i64,
    requests: u64,
    bytes: u64,
    opaque_id: Option<String>,
    forwarded_for: Option<String>,
}

#[derive(Default)]
struct HttpStats {
    total_opened: u64,
    clients: Vec<HttpClient>,
}

static HTTP: Mutex<HttpStats> = Mutex::new(HttpStats { total_opened: 0, clients: Vec::new() });
static NEXT_CLIENT: AtomicU64 = AtomicU64::new(1);

/// Closed connections kept in the client list (Elasticsearch keeps the
/// recently closed ones too).
const KEEP_CLOSED: usize = 100;

/// Registers a newly accepted HTTP connection; returns its client id.
pub(crate) fn http_opened(remote: String, local: String) -> u64 {
    let id = NEXT_CLIENT.fetch_add(1, Ordering::Relaxed);
    let now = now_millis();
    let mut h = HTTP.lock().unwrap();
    h.total_opened += 1;
    h.clients.push(HttpClient {
        id,
        agent: None,
        local,
        remote,
        last_uri: None,
        opened: now,
        closed: None,
        last_request: now,
        requests: 0,
        bytes: 0,
        opaque_id: None,
        forwarded_for: None,
    });
    id
}

thread_local! {
    /// The `X-Opaque-Id` of the request this thread is serving.
    static OPAQUE_ID: RefCell<Option<String>> = const { RefCell::new(None) };
}

/// Notes one request on a connection (and remembers its `X-Opaque-Id`
/// for the task list while it runs).
pub(crate) fn http_request(
    client: u64,
    uri: &str,
    size: usize,
    headers: &std::collections::HashMap<String, String>,
) {
    let opaque = headers.get("x-opaque-id").cloned();
    OPAQUE_ID.with(|o| *o.borrow_mut() = opaque.clone());
    let mut h = HTTP.lock().unwrap();
    if let Some(c) = h.clients.iter_mut().find(|c| c.id == client) {
        c.agent = headers.get("user-agent").cloned().or(c.agent.take());
        c.last_uri = Some(uri.to_string());
        c.last_request = now_millis();
        c.requests += 1;
        c.bytes += size as u64;
        if opaque.is_some() {
            c.opaque_id = opaque;
        }
        if let Some(f) = headers.get("x-forwarded-for") {
            c.forwarded_for = Some(f.clone());
        }
    }
}

/// Marks a connection closed, dropping the oldest closed ones.
pub(crate) fn http_closed(client: u64) {
    let mut h = HTTP.lock().unwrap();
    if let Some(c) = h.clients.iter_mut().find(|c| c.id == client) {
        c.closed = Some(now_millis());
    }
    let closed = h.clients.iter().filter(|c| c.closed.is_some()).count();
    if closed > KEEP_CLOSED {
        let mut drop = closed - KEEP_CLOSED;
        h.clients.retain(|c| {
            if drop > 0 && c.closed.is_some() {
                drop -= 1;
                false
            } else {
                true
            }
        });
    }
}

/// The `X-Opaque-Id` of the current request.
pub(crate) fn opaque_id() -> Option<String> {
    OPAQUE_ID.with(|o| o.borrow().clone())
}

/// `http` node stats: open connections and per-client detail.
pub(crate) fn http_stats() -> Value {
    let h = HTTP.lock().unwrap();
    let open = h.clients.iter().filter(|c| c.closed.is_none()).count();
    let clients: Vec<Value> = h
        .clients
        .iter()
        .map(|c| {
            let mut m = Map::new();
            m.insert("id".into(), json!(c.id));
            if let Some(a) = &c.agent {
                m.insert("agent".into(), json!(a));
            }
            m.insert("local_address".into(), json!(c.local));
            m.insert("remote_address".into(), json!(c.remote));
            if let Some(u) = &c.last_uri {
                m.insert("last_uri".into(), json!(u));
            }
            if let Some(f) = &c.forwarded_for {
                m.insert("x_forwarded_for".into(), json!(f));
            }
            if let Some(o) = &c.opaque_id {
                m.insert("x_opaque_id".into(), json!(o));
            }
            m.insert("opened_time_millis".into(), json!(c.opened));
            if let Some(t) = c.closed {
                m.insert("closed_time_millis".into(), json!(t));
            }
            m.insert("last_request_time_millis".into(), json!(c.last_request));
            m.insert("request_count".into(), json!(c.requests));
            m.insert("request_size_bytes".into(), json!(c.bytes));
            Value::Object(m)
        })
        .collect();
    json!({"current_open": open, "total_opened": h.total_opened.max(open as u64), "clients": clients})
}

// --- tasks ---------------------------------------------------------------

static NEXT_TASK: AtomicU64 = AtomicU64::new(1000);

/// A fresh task number on this node.
pub(crate) fn next_task() -> u64 {
    NEXT_TASK.fetch_add(1, Ordering::Relaxed)
}

/// Thread pools: (name, info section) in name order.
pub(crate) fn thread_pools() -> Vec<(String, Value)> {
    let mut v: Vec<(String, Value)> = info_data()["thread_pool"]
        .as_object()
        .map(|m| m.iter().map(|(k, x)| (k.clone(), x.clone())).collect())
        .unwrap_or_default();
    v.sort_by(|a, b| a.0.cmp(&b.0));
    v
}

/// A thread pool's live counters (`threads`, `queue`, `active`, ...):
/// idle pools, their core threads started.
pub(crate) fn thread_pool_stats(info: &Value) -> Value {
    let threads = info.get("size").or_else(|| info.get("core")).and_then(Value::as_u64).unwrap_or(0);
    json!({"threads": threads, "queue": 0, "active": 0, "rejected": 0, "largest": threads, "completed": 0})
}

/// Bytes the way `?human` prints them (`1.5kb`, `512b`, `7.6gb`).
pub(crate) fn human_size(n: u64) -> String {
    let units = ["b", "kb", "mb", "gb", "tb", "pb"];
    let mut v = n as f64;
    let mut u = 0;
    while v >= 1024.0 && u + 1 < units.len() {
        v /= 1024.0;
        u += 1;
    }
    if u == 0 {
        format!("{n}b")
    } else {
        let s = format!("{v:.1}");
        format!("{}{}", s.trim_end_matches(".0"), units[u])
    }
}

/// A duration the way `?human` prints it (`0s`, `250ms`, `1.2m`).
pub(crate) fn human_time(ms: i64) -> String {
    if ms <= 0 {
        return "0s".to_string();
    }
    let ms = ms as f64;
    let (v, unit) = if ms < 1000.0 {
        return format!("{}ms", ms as i64);
    } else if ms < 60_000.0 {
        (ms / 1000.0, "s")
    } else if ms < 3_600_000.0 {
        (ms / 60_000.0, "m")
    } else if ms < 86_400_000.0 {
        (ms / 3_600_000.0, "h")
    } else {
        (ms / 86_400_000.0, "d")
    };
    let s = format!("{v:.1}");
    format!("{}{unit}", s.trim_end_matches(".0"))
}

/// Adds `?human` companions to a stats object, recursively: `x_in_bytes`
/// gains `x` (`1.2kb`), `x_in_millis` and `x_time_millis` gain `x` /
/// `x_time` (`1.2s`), unless a key of that name is already there.
/// Epoch timestamps (`*_time_in_millis` of a start, `timestamp`) are left
/// alone.
pub(crate) fn add_human(v: &mut Value) {
    fn companion(k: &str, x: &Value) -> Option<(String, Value)> {
        let n = x.as_i64()?;
        if let Some(base) = k.strip_suffix("_in_bytes") {
            return Some((base.to_string(), json!(human_size(n.max(0) as u64))));
        }
        let epoch = ["start_time", "stop_time", "opened_time", "closed_time", "last_request_time"];
        if epoch.iter().any(|e| k.starts_with(e)) {
            return None;
        }
        let base = k.strip_suffix("_in_millis").or_else(|| k.strip_suffix("_time_millis").map(|_| &k[..k.len() - 7]))?;
        Some((base.to_string(), json!(human_time(n))))
    }
    match v {
        Value::Object(m) => {
            for x in m.values_mut() {
                add_human(x);
            }
            let old = std::mem::take(m);
            let keys: Vec<String> = old.keys().cloned().collect();
            for (k, x) in old {
                if let Some((name, h)) = companion(&k, &x)
                    && !keys.contains(&name)
                    && !m.contains_key(&name)
                {
                    m.insert(name, h);
                }
                m.insert(k, x);
            }
        }
        Value::Array(a) => a.iter_mut().for_each(add_human),
        _ => {}
    }
}

/// Lucene's normalized Levenshtein similarity (1 - distance / longer).
fn similarity(a: &str, b: &str) -> f32 {
    let (a, b): (Vec<char>, Vec<char>) = (a.chars().collect(), b.chars().collect());
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    for i in 1..=a.len() {
        let mut cur = vec![i; b.len() + 1];
        for j in 1..=b.len() {
            let c = usize::from(a[i - 1] != b[j - 1]);
            cur[j] = (prev[j] + 1).min(cur[j - 1] + 1).min(prev[j - 1] + c);
        }
        prev = cur;
    }
    let longest = a.len().max(b.len()).max(1);
    1.0 - prev[b.len()] as f32 / longest as f32
}

/// Elasticsearch's "request [path] contains unrecognized metric(s): [x]
/// -> did you mean [y]?" message.
pub(crate) fn unrecognized(path: &str, invalid: &[String], known: &[&str], what: &str) -> String {
    let mut invalid: Vec<&String> = invalid.iter().collect();
    invalid.sort();
    invalid.dedup();
    let mut msg = format!(
        "request [{path}] contains unrecognized {what}{}: ",
        if invalid.len() > 1 { "s" } else { "" }
    );
    for (n, bad) in invalid.iter().enumerate() {
        if n > 0 {
            msg.push_str(", ");
        }
        let mut scored: Vec<(f32, &str)> = known
            .iter()
            .map(|k| (similarity(bad, k), *k))
            .filter(|(s, _)| *s > 0.5)
            .collect();
        scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal).then(a.1.cmp(b.1)));
        msg.push_str(&format!("[{bad}]"));
        match scored.len() {
            0 => {}
            1 => msg.push_str(&format!(" -> did you mean [{}]?", scored[0].1)),
            _ => {
                let keys: Vec<&str> = scored.iter().map(|s| s.1).collect();
                msg.push_str(&format!(" -> did you mean any of [{}]?", keys.join(", ")));
            }
        }
    }
    msg
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_selectors() {
        assert!(selected("_all"));
        assert!(selected("_master"));
        assert!(selected("data:true"));
        assert!(!selected("data:false"));
        assert!(selected(NODE_ID));
        assert!(selected("noi*"));
        assert!(!selected("non_existent"));
        assert!(selected("xpack.installed:true"));
    }

    #[test]
    fn unrecognized_metric_message() {
        assert_eq!(
            unrecognized("/_stats/fieldata", &["fieldata".into()], &["fielddata", "docs"], "metric"),
            "request [/_stats/fieldata] contains unrecognized metric: [fieldata] -> did you mean [fielddata]?"
        );
        assert_eq!(
            unrecognized(
                "/_nodes/stats/transprot,foo",
                &["transprot".into(), "foo".into()],
                &["transport", "os"],
                "metric"
            ),
            "request [/_nodes/stats/transprot,foo] contains unrecognized metrics: [foo], [transprot] -> did you mean [transport]?"
        );
    }

    #[test]
    fn human_values() {
        assert_eq!(human_size(512), "512b");
        assert_eq!(human_size(1536), "1.5kb");
        assert_eq!(human_time(250), "250ms");
        assert_eq!(human_time(1500), "1.5s");
        let mut v = json!({"size_in_bytes": 2048, "total_time_in_millis": 0});
        add_human(&mut v);
        assert_eq!(v["size"], json!("2kb"));
        assert_eq!(v["total_time"], json!("0s"));
        let mut d = json!({"computation_time_millis": 1500, "opened_time_millis": 5});
        add_human(&mut d);
        assert_eq!(d["computation_time"], json!("1.5s"));
        assert!(d.get("opened_time").is_none());
    }
}
