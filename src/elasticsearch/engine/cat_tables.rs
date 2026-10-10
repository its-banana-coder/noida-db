//! The `_cat` tables about the cluster, its node, indices and shards:
//! `indices`, `count`, `health`, `nodes`, `allocation`, `shards`,
//! `segments`, `recovery`, `fielddata`, `master`, `nodeattrs`, `plugins`,
//! `tasks`, `thread_pool` and `pending_tasks`.
//!
//! Columns (names, aliases, descriptions, which are shown by default and
//! which are right-aligned) are Elasticsearch's own, from
//! `cat_columns.txt`. `?h` picks columns by name, alias or wildcard,
//! `?s` sorts, `?v` adds the header, `?help` lists the columns, `?bytes`
//! and `?time` fix the units, `?format=json` returns objects.

use super::node;
use super::stats::{self, Resolve};
use super::*;
use std::collections::BTreeMap;
use std::sync::OnceLock;

pub(super) struct Col {
    name: String,
    aliases: Vec<String>,
    desc: String,
    default: bool,
    right: bool,
}

/// The column definitions of each table.
fn columns(table: &str) -> &'static [Col] {
    static ALL: OnceLock<HashMap<String, Vec<Col>>> = OnceLock::new();
    let all = ALL.get_or_init(|| {
        let mut m: HashMap<String, Vec<Col>> = HashMap::new();
        for line in include_str!("cat_columns.txt").lines() {
            let p: Vec<&str> = line.split('|').collect();
            if p.len() < 6 {
                continue;
            }
            m.entry(p[0].to_string()).or_default().push(Col {
                name: p[1].to_string(),
                aliases: p[2].split(',').filter(|a| !a.is_empty()).map(str::to_string).collect(),
                desc: p[3].to_string(),
                default: p[4] == "1",
                right: p[5] == "r",
            });
        }
        m
    });
    all.get(table).map_or(&[], Vec::as_slice)
}

/// One cell: text, a byte size, a duration, or nothing.
#[derive(Clone, Debug)]
pub(super) enum Cell {
    Text(String),
    Bytes(u64),
    Millis(i64),
    Null,
}

fn t(s: impl ToString) -> Cell {
    Cell::Text(s.to_string())
}

type Row = BTreeMap<&'static str, Cell>;

/// Renders a cell per `?bytes` / `?time`.
fn cell_text(c: &Cell, q: &HashMap<String, String>) -> Option<String> {
    match c {
        Cell::Null => None,
        Cell::Text(s) => Some(s.clone()),
        Cell::Bytes(n) => Some(match q.get("bytes").map(String::as_str) {
            Some("b") => n.to_string(),
            Some("k" | "kb") => (n >> 10).to_string(),
            Some("m" | "mb") => (n >> 20).to_string(),
            Some("g" | "gb") => (n >> 30).to_string(),
            Some("t" | "tb") => (n >> 40).to_string(),
            Some("p" | "pb") => (n >> 50).to_string(),
            _ => node::human_size(*n),
        }),
        Cell::Millis(ms) => Some(match q.get("time").map(String::as_str) {
            Some("d") => (ms / 86_400_000).to_string(),
            Some("h") => (ms / 3_600_000).to_string(),
            Some("m") => (ms / 60_000).to_string(),
            Some("s") => (ms / 1000).to_string(),
            Some("ms") => ms.to_string(),
            Some("micros") => (ms * 1000).to_string(),
            Some("nanos") => (ms * 1_000_000).to_string(),
            _ => node::human_time(*ms),
        }),
    }
}

/// A sort key: numbers (bytes, durations, numeric text) compare as
/// numbers, everything else as text; missing values last.
fn sort_key(c: &Cell) -> (u8, f64, String) {
    match c {
        Cell::Null => (2, 0.0, String::new()),
        Cell::Bytes(n) => (0, *n as f64, String::new()),
        Cell::Millis(n) => (0, *n as f64, String::new()),
        Cell::Text(s) => match s.trim_end_matches('%').parse::<f64>() {
            Ok(f) => (0, f, String::new()),
            Err(_) => (1, 0.0, s.clone()),
        },
    }
}

/// Which columns `?h` names (in its order), or the default ones, each
/// with the name it is shown under: the name or alias `?h` used.
fn wanted<'a>(cols: &'a [Col], q: &HashMap<String, String>) -> Vec<(String, &'a Col)> {
    let Some(h) = q.get("h") else {
        return cols.iter().filter(|c| c.default).map(|c| (c.name.clone(), c)).collect();
    };
    let mut out: Vec<(String, &Col)> = Vec::new();
    for item in h.split(',').map(str::trim).filter(|x| !x.is_empty()) {
        if item.contains('*') {
            for c in cols {
                if glob_match(item, &c.name) && !out.iter().any(|o| o.1.name == c.name) {
                    out.push((c.name.clone(), c));
                }
            }
        } else if let Some(c) =
            cols.iter().find(|c| c.name == item || c.aliases.iter().any(|a| a == item))
        {
            out.push((item.to_string(), c));
        }
    }
    out
}

/// Renders a table as `_cat` does.
pub(super) fn render(table: &str, rows: Vec<Row>, q: &HashMap<String, String>) -> (u16, Value) {
    let cols = columns(table);
    if q.contains_key("help") {
        let w1 = cols.iter().map(|c| c.name.len()).max().unwrap_or(0);
        let w2 = cols.iter().map(|c| c.aliases.join(",").len()).max().unwrap_or(0);
        let w3 = cols.iter().map(|c| c.desc.len()).max().unwrap_or(0);
        let text: String = cols
            .iter()
            .map(|c| format!("{:<w1$} | {:<w2$} | {:<w3$}\n", c.name, c.aliases.join(","), c.desc))
            .collect();
        return (200, json!({ cat::RAW_TEXT: text }));
    }
    let mut rows = rows;
    if let Some(spec) = q.get("s") {
        let keys: Vec<(&'static str, bool)> = spec
            .split(',')
            .filter_map(|k| {
                let (name, dir) = k.trim().split_once(':').unwrap_or((k.trim(), "asc"));
                cols.iter()
                    .find(|c| c.name == name || c.aliases.iter().any(|a| a == name))
                    .map(|c| (c.name.as_str(), dir == "desc"))
            })
            .collect();
        rows.sort_by(|x, y| {
            for (k, desc) in &keys {
                let a = sort_key(x.get(k).unwrap_or(&Cell::Null));
                let b = sort_key(y.get(k).unwrap_or(&Cell::Null));
                let ord = a.0.cmp(&b.0).then(
                    a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal).then(a.2.cmp(&b.2)),
                );
                let ord = if *desc && a.0 == b.0 { ord.reverse() } else { ord };
                if ord != std::cmp::Ordering::Equal {
                    return ord;
                }
            }
            std::cmp::Ordering::Equal
        });
    }
    let shown = wanted(cols, q);
    let cells: Vec<Vec<Option<String>>> = rows
        .iter()
        .map(|r| shown.iter().map(|(_, c)| r.get(c.name.as_str()).and_then(|x| cell_text(x, q))).collect())
        .collect();
    if q.get("format").map(String::as_str) == Some("json") {
        return (
            200,
            Value::Array(
                cells
                    .iter()
                    .map(|r| {
                        Value::Object(
                            shown
                                .iter()
                                .zip(r)
                                .map(|((name, _), v)| (name.clone(), v.clone().map_or(Value::Null, Value::String)))
                                .collect(),
                        )
                    })
                    .collect(),
            ),
        );
    }
    let header = q.get("v").is_some_and(|v| v.is_empty() || v == "true");
    let mut widths: Vec<usize> =
        shown.iter().map(|(name, _)| if header { name.chars().count() } else { 0 }).collect();
    for r in &cells {
        for (k, v) in r.iter().enumerate() {
            widths[k] = widths[k].max(v.as_deref().unwrap_or("").chars().count());
        }
    }
    let line = |vals: Vec<&str>| -> String {
        let n = vals.len();
        let mut out = String::new();
        for (k, v) in vals.into_iter().enumerate() {
            let pad = widths[k].saturating_sub(v.chars().count());
            if shown[k].1.right {
                out.push_str(&" ".repeat(pad));
                out.push_str(v);
            } else {
                out.push_str(v);
                if k + 1 < n {
                    out.push_str(&" ".repeat(pad));
                }
            }
            if k + 1 < n {
                out.push(' ');
            }
        }
        out + "\n"
    };
    let mut text = String::new();
    if header {
        text.push_str(&line(shown.iter().map(|(name, _)| name.as_str()).collect()));
    }
    for r in &cells {
        text.push_str(&line(r.iter().map(|v| v.as_deref().unwrap_or("")).collect()));
    }
    (200, json!({ cat::RAW_TEXT: text }))
}

/// Column names live as long as the table definitions; this gives the
/// `&'static str` a row is keyed by.
fn leak(s: &str) -> &'static str {
    for table in [
        "allocation", "fielddata", "health", "indices", "master", "nodeattrs", "nodes", "plugins",
        "recovery", "segments", "shards", "tasks", "thread_pool", "count", "pending_tasks",
    ] {
        if let Some(c) = columns(table).iter().find(|c| c.name == s) {
            return c.name.as_str();
        }
    }
    ""
}

/// `epoch` and `timestamp` cells for "now".
fn now_cells(r: &mut Row) {
    let (epoch, ts) = cat::now_columns();
    r.insert("epoch", t(epoch));
    r.insert("timestamp", t(ts));
}

/// A node's identity cells (`id`, `host`, `ip`, `port`, `node`, ...).
fn node_cells(r: &mut Row, full_id: bool) {
    let id = if full_id { node::NODE_ID.to_string() } else { node::NODE_ID[..4].to_string() };
    r.insert("id", t(id.clone()));
    r.insert("node_id", t(id));
    r.insert("host", t(node::HOST));
    r.insert("ip", t(node::HOST));
    r.insert("port", t("9300"));
    r.insert("pid", t(std::process::id()));
    r.insert("node", t(node::NODE_NAME));
    r.insert("name", t(node::NODE_NAME));
    r.insert("node_name", t(node::NODE_NAME));
    r.insert("ephemeral_node_id", t(&node::EPHEMERAL_ID[..4]));
}

fn flag(q: &HashMap<String, String>, k: &str) -> bool {
    q.get(k).is_some_and(|v| v.is_empty() || v == "true")
}

/// The stats cells a table shows for some shards' (or an index's)
/// sections, under `prefix` (`""` or `"pri."`).
fn stats_cells(r: &mut Row, prefix: &str, s: &Value) {
    let n = |p: &[&str]| p.iter().fold(s, |v, k| &v[*k]).as_u64().unwrap_or(0);
    let mut put = |k: &str, c: Cell| {
        r.insert(leak(&format!("{prefix}{k}")), c);
    };
    put("completion.size", Cell::Bytes(n(&["completion", "size_in_bytes"])));
    put("fielddata.memory_size", Cell::Bytes(n(&["fielddata", "memory_size_in_bytes"])));
    put("fielddata.evictions", t(n(&["fielddata", "evictions"])));
    put("query_cache.memory_size", Cell::Bytes(0));
    put("query_cache.evictions", t(0));
    put("request_cache.memory_size", Cell::Bytes(0));
    for k in ["request_cache.evictions", "request_cache.hit_count", "request_cache.miss_count"] {
        put(k, t(0));
    }
    put("flush.total", t(n(&["flush", "total"])));
    put("flush.total_time", Cell::Millis(0));
    put("get.current", t(0));
    put("get.time", Cell::Millis(0));
    put("get.total", t(n(&["get", "total"])));
    put("get.exists_time", Cell::Millis(0));
    put("get.exists_total", t(n(&["get", "exists_total"])));
    put("get.missing_time", Cell::Millis(0));
    put("get.missing_total", t(n(&["get", "missing_total"])));
    put("indexing.delete_current", t(0));
    put("indexing.delete_time", Cell::Millis(0));
    put("indexing.delete_total", t(n(&["indexing", "delete_total"])));
    put("indexing.index_current", t(0));
    put("indexing.index_time", Cell::Millis(0));
    put("indexing.index_total", t(n(&["indexing", "index_total"])));
    put("indexing.index_failed", t(n(&["indexing", "index_failed"])));
    put("merges.current", t(0));
    put("merges.current_docs", t(0));
    put("merges.current_size", Cell::Bytes(0));
    put("merges.total", t(n(&["merges", "total"])));
    put("merges.total_docs", t(0));
    put("merges.total_size", Cell::Bytes(0));
    put("merges.total_time", Cell::Millis(0));
    put("refresh.total", t(n(&["refresh", "total"])));
    put("refresh.time", Cell::Millis(0));
    put("refresh.external_total", t(n(&["refresh", "external_total"])));
    put("refresh.external_time", Cell::Millis(0));
    put("refresh.listeners", t(0));
    put("search.fetch_current", t(0));
    put("search.fetch_time", Cell::Millis(0));
    put("search.fetch_total", t(n(&["search", "fetch_total"])));
    put("search.open_contexts", t(0));
    put("search.query_current", t(0));
    put("search.query_time", Cell::Millis(0));
    put("search.query_total", t(n(&["search", "query_total"])));
    put("search.scroll_current", t(0));
    put("search.scroll_time", Cell::Millis(0));
    put("search.scroll_total", t(n(&["search", "scroll_total"])));
    put("segments.count", t(n(&["segments", "count"])));
    put("segments.memory", Cell::Bytes(0));
    put("segments.index_writer_memory", Cell::Bytes(0));
    put("segments.version_map_memory", Cell::Bytes(0));
    put("segments.fixed_bitset_memory", Cell::Bytes(0));
    put("warmer.current", t(0));
    put("warmer.total", t(n(&["warmer", "total"])));
    put("warmer.total_time", Cell::Millis(0));
    put("suggest.current", t(0));
    put("suggest.time", Cell::Millis(0));
    put("suggest.total", t(n(&["search", "suggest_total"])));
    put("memory.total", Cell::Bytes(0));
    put("bulk.total_operations", t(n(&["bulk", "total_operations"])));
    put("bulk.total_time", Cell::Millis(0));
    put("bulk.total_size_in_bytes", t(n(&["bulk", "total_size_in_bytes"])));
    put("bulk.avg_time", Cell::Millis(0));
    put("bulk.avg_size_in_bytes", t(n(&["bulk", "avg_size_in_bytes"])));
    put("dense_vector.value_count", t(n(&["dense_vector", "value_count"])));
    put("sparse_vector.value_count", t(n(&["sparse_vector", "value_count"])));
}

impl Engine {
    /// `GET /_cat/<table>...` for the tables this module renders; `None`
    /// for the rest (`aliases`, `templates`, ...).
    pub(super) fn cat_tables(
        &self,
        segments: &[&str],
        q: &HashMap<String, String>,
    ) -> Option<(u16, Value)> {
        let arg = segments.get(2).copied();
        // `?help` takes no path parameter.
        if q.contains_key("help") && arg.is_some() {
            let param = match segments.get(1).copied() {
                Some("allocation") => "node_id",
                Some("thread_pool") => "thread_pool_patterns",
                Some("fielddata") => "fields",
                Some("indices" | "count" | "shards" | "segments" | "recovery") => "index",
                _ => return None,
            };
            let msg = format!(
                "request [/{}] contains unrecognized parameter: [{param}]",
                segments.join("/")
            );
            return Some((400, error("illegal_argument_exception", &msg, 400)));
        }
        let r = match segments.get(1).copied() {
            None => {
                let text = "=^.^=\n/_cat/allocation\n/_cat/shards\n/_cat/shards/{index}\n/_cat/master\n/_cat/nodes\n/_cat/tasks\n/_cat/indices\n/_cat/indices/{index}\n/_cat/segments\n/_cat/segments/{index}\n/_cat/count\n/_cat/count/{index}\n/_cat/recovery\n/_cat/recovery/{index}\n/_cat/health\n/_cat/pending_tasks\n/_cat/aliases\n/_cat/aliases/{alias}\n/_cat/thread_pool\n/_cat/thread_pool/{thread_pools}\n/_cat/plugins\n/_cat/fielddata\n/_cat/fielddata/{fields}\n/_cat/nodeattrs\n/_cat/repositories\n/_cat/snapshots/{repository}\n/_cat/templates\n/_cat/component_templates/_cat/transforms\n/_cat/transforms/{transform_id}\n";
                (200, json!({ cat::RAW_TEXT: text }))
            }
            Some("indices") => self.cat_indices(arg, q),
            Some("count") => self.cat_count(arg, q),
            Some("health") => self.cat_health(q),
            Some("nodes") => self.cat_nodes(q),
            Some("allocation") => self.cat_allocation(arg, q),
            Some("shards") => self.cat_shards(arg, q),
            Some("segments") => self.cat_segments(arg, q),
            Some("recovery") => self.cat_recovery(arg, q),
            Some("fielddata") => self.cat_fielddata(arg, q),
            Some("master") => {
                let mut r = Row::new();
                node_cells(&mut r, true);
                render("master", vec![r], q)
            }
            Some("nodeattrs") => {
                let rows = node::attributes()
                    .as_object()
                    .map(|m| {
                        m.iter()
                            .map(|(k, v)| {
                                let mut r = Row::new();
                                node_cells(&mut r, false);
                                r.insert("attr", t(k));
                                r.insert("value", t(v.as_str().unwrap_or("")));
                                r
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                render("nodeattrs", rows, q)
            }
            Some("plugins") => render("plugins", vec![], q),
            Some("pending_tasks") => render("pending_tasks", vec![], q),
            Some("thread_pool") => {
                let patterns: Vec<&str> = arg
                    .or(q.get("thread_pool_patterns").map(String::as_str))
                    .map(|p| p.split(',').map(str::trim).collect())
                    .unwrap_or_default();
                let rows = node::thread_pools()
                    .into_iter()
                    .filter(|(n, _)| patterns.is_empty() || patterns.iter().any(|p| glob_match(p, n)))
                    .map(|(name, info)| {
                        let mut r = Row::new();
                        node_cells(&mut r, false);
                        let st = node::thread_pool_stats(&info);
                        let num = |v: &Value| v.as_i64().map(|n| t(n)).unwrap_or(Cell::Null);
                        r.insert("name", t(&name));
                        r.insert("type", t(info["type"].as_str().unwrap_or("fixed")));
                        r.insert("active", t(0));
                        r.insert("pool_size", num(&st["threads"]));
                        r.insert("queue", t(0));
                        r.insert("queue_size", num(&info["queue_size"]));
                        r.insert("rejected", t(0));
                        r.insert("largest", num(&st["largest"]));
                        r.insert("completed", t(0));
                        r.insert("core", num(&info["core"]));
                        r.insert("max", num(&info["max"]));
                        r.insert("size", num(&info["size"]));
                        r.insert(
                            "keep_alive",
                            info["keep_alive"].as_str().map_or(Cell::Null, t),
                        );
                        r
                    })
                    .collect();
                render("thread_pool", rows, q)
            }
            Some("tasks") => {
                let rows = super::cluster::current_tasks(flag(q, "detailed"))
                    .into_iter()
                    .map(|task| {
                        let mut r = Row::new();
                        node_cells(&mut r, false);
                        let start = task["start_time_in_millis"].as_i64().unwrap_or(0);
                        let secs = start / 1000;
                        let tod = secs.rem_euclid(86_400);
                        r.insert("id", t(task["id"].as_u64().unwrap_or(0)));
                        r.insert("action", t(task["action"].as_str().unwrap_or("")));
                        r.insert("task_id", t(format!("{}:{}", node::NODE_ID, task["id"])));
                        r.insert(
                            "parent_task_id",
                            t(task["parent_task_id"].as_str().unwrap_or("-")),
                        );
                        r.insert("type", t("transport"));
                        r.insert("start_time", t(start));
                        r.insert(
                            "timestamp",
                            t(format!("{:02}:{:02}:{:02}", tod / 3600, tod / 60 % 60, tod % 60)),
                        );
                        let nanos = task["running_time_in_nanos"].as_i64().unwrap_or(0);
                        r.insert("running_time_ns", t(nanos));
                        r.insert("running_time", t(format!("{:.1}micros", nanos as f64 / 1000.0)));
                        r.insert("node_id", t(node::NODE_ID));
                        r.insert("version", t(node::VERSION));
                        r.insert(
                            "x_opaque_id",
                            task["headers"]["X-Opaque-Id"].as_str().map_or(Cell::Null, t),
                        );
                        if let Some(d) = task["description"].as_str() {
                            r.insert("description", t(d));
                        }
                        r
                    })
                    .collect();
                let mut q = q.clone();
                if flag(&q, "detailed") && !q.contains_key("h") {
                    let cols: Vec<&str> = columns("tasks")
                        .iter()
                        .filter(|c| c.default)
                        .map(|c| c.name.as_str())
                        .chain(["description"])
                        .collect();
                    q.insert("h".into(), cols.join(","));
                }
                render("tasks", rows, &q)
            }
            _ => return None,
        };
        Some(r)
    }

    fn cat_indices(&self, arg: Option<&str>, q: &HashMap<String, String>) -> (u16, Value) {
        let health = q.get("health").map(String::as_str);
        if let Some(h) = health
            && !matches!(h, "green" | "yellow" | "red")
        {
            let msg = format!("unknown cluster health status [{h}]");
            return (400, error("illegal_argument_exception", &msg, 400));
        }
        let pri_only = flag(q, "pri");
        let mut s = self.0.lock().unwrap();
        let opts = Resolve { expand: "all", lenient: false, forbid_closed: false };
        let mut q2 = q.clone();
        if !q.contains_key("expand_wildcards") {
            q2.insert("expand_wildcards".into(), "open,closed".into());
        }
        let names = match stats::resolve(&s, arg.unwrap_or("_all"), &q2, &opts) {
            Ok(n) => n,
            Err(e) => return e,
        };
        for n in &names {
            if let Some(i) = s.indices.get_mut(n) {
                i.auto_refresh(n);
            }
        }
        let r = stats::StatsRequest::all();
        let mut rows = Vec::new();
        for n in &names {
            let i = &s.indices[n];
            let h = Self::health_of(&s, std::slice::from_ref(n));
            let status = h["status"].as_str().unwrap_or("green").to_string();
            if health.is_some_and(|w| w != status) {
                continue;
            }
            let (p, rep) = shard_counts(i);
            let mut row = Row::new();
            row.insert("health", t(&status));
            row.insert("status", t(if i.opened { "open" } else { "close" }));
            row.insert("index", t(n));
            row.insert("uuid", t(i.settings["index"]["uuid"].as_str().unwrap_or("_na_")));
            row.insert("pri", t(p));
            row.insert("rep", t(rep));
            let created = i.settings["index"]["creation_date"]
                .as_str()
                .and_then(|v| v.parse::<i64>().ok())
                .unwrap_or(0);
            row.insert("creation.date", t(created));
            row.insert("creation.date.string", t(dates::format(created, None, 0)));
            if i.opened {
                let sec = stats::index_sections(i, &r);
                let store = sec["store"]["size_in_bytes"].as_u64().unwrap_or(0);
                row.insert("docs.count", t(sec["docs"]["count"].as_u64().unwrap_or(0)));
                row.insert("docs.deleted", t(0));
                row.insert("store.size", Cell::Bytes(store));
                row.insert("pri.store.size", Cell::Bytes(store));
                row.insert("dataset.size", Cell::Bytes(store));
                stats_cells(&mut row, "", &sec);
                stats_cells(&mut row, "pri.", &sec);
            }
            let _ = pri_only;
            rows.push(row);
        }
        render("indices", rows, q)
    }

    fn cat_count(&self, arg: Option<&str>, q: &HashMap<String, String>) -> (u16, Value) {
        let mut s = self.0.lock().unwrap();
        let names = match stats::resolve(&s, arg.unwrap_or("_all"), q, &Resolve::STRICT_OPEN) {
            Ok(n) => n,
            Err(e) => return e,
        };
        let mut count = 0;
        for n in &names {
            if let Some(i) = s.indices.get_mut(n) {
                i.auto_refresh(n);
                count += i.committed.len();
            }
        }
        let mut r = Row::new();
        now_cells(&mut r);
        r.insert("count", t(count));
        render("count", vec![r], q)
    }

    fn cat_health(&self, q: &HashMap<String, String>) -> (u16, Value) {
        let s = self.0.lock().unwrap();
        let all: Vec<String> = s.indices.keys().cloned().collect();
        let h = Self::health_of(&s, &all);
        let mut r = Row::new();
        now_cells(&mut r);
        r.insert("cluster", t(node::CLUSTER_NAME));
        r.insert("status", t(h["status"].as_str().unwrap_or("green")));
        r.insert("node.total", t(1));
        r.insert("node.data", t(1));
        r.insert("shards", t(&h["active_shards"]));
        r.insert("pri", t(&h["active_primary_shards"]));
        r.insert("relo", t(0));
        r.insert("init", t(0));
        r.insert("unassign", t(&h["unassigned_shards"]));
        r.insert("pending_tasks", t(0));
        r.insert("max_task_wait_time", t("-"));
        r.insert(
            "active_shards_percent",
            t(format!("{:.1}%", h["active_shards_percent_as_number"].as_f64().unwrap_or(100.0))),
        );
        let mut q = q.clone();
        if q.get("ts").is_some_and(|v| v == "false") && !q.contains_key("h") {
            let cols: Vec<&str> = columns("health")
                .iter()
                .filter(|c| c.default && c.name != "epoch" && c.name != "timestamp")
                .map(|c| c.name.as_str())
                .collect();
            q.insert("h".into(), cols.join(","));
        }
        render("health", vec![r], &q)
    }

    fn cat_nodes(&self, q: &HashMap<String, String>) -> (u16, Value) {
        let s = self.0.lock().unwrap();
        let mut r = Row::new();
        node_cells(&mut r, flag(q, "full_id"));
        let names: Vec<String> = s.indices.iter().filter(|(_, i)| i.opened).map(|(n, _)| n.clone()).collect();
        let req = stats::StatsRequest::all();
        let mut sum = json!({});
        for n in &names {
            stats::add_stats(&mut sum, &stats::index_sections(&s.indices[n], &req));
        }
        let (heap_max, heap_used) = (536_870_912u64, 134_217_728u64);
        let (disk_total, disk_avail) = super::nodes::disk();
        let (ram_total, ram_free) = super::nodes::memory();
        r.insert("heap.current", Cell::Bytes(heap_used));
        r.insert("heap.percent", t(heap_used * 100 / heap_max));
        r.insert("heap.max", Cell::Bytes(heap_max));
        r.insert("ram.current", Cell::Bytes(ram_total - ram_free));
        r.insert("ram.percent", t((ram_total - ram_free) * 100 / ram_total.max(1)));
        r.insert("ram.max", Cell::Bytes(ram_total));
        r.insert("file_desc.current", t(64));
        r.insert("file_desc.percent", t(0));
        r.insert("file_desc.max", t(65535));
        r.insert("cpu", t(1));
        r.insert("load_1m", t("0.00"));
        r.insert("load_5m", t("0.00"));
        r.insert("load_15m", t("0.00"));
        r.insert("uptime", Cell::Millis(node::uptime_millis()));
        r.insert("node.role", t(node::ROLE_CHARS));
        r.insert("master", t("*"));
        r.insert("version", t(node::VERSION));
        r.insert("type", t("docker"));
        r.insert("build", t("noida"));
        r.insert("jdk", t("22.0.1"));
        r.insert("disk.total", Cell::Bytes(disk_total));
        r.insert("disk.used", Cell::Bytes(disk_total - disk_avail));
        r.insert("disk.avail", Cell::Bytes(disk_avail));
        r.insert(
            "disk.used_percent",
            t(format!("{:.2}", (disk_total - disk_avail) as f64 * 100.0 / disk_total.max(1) as f64)),
        );
        r.insert("http_address", t(node::http_address()));
        r.insert("http", t(node::http_address()));
        r.insert("shard_stats.total_count", t(sum["shard_stats"]["total_count"].as_u64().unwrap_or(0)));
        let fields: u64 = names.iter().map(|n| super::nodes::mapping_field_count(&s.indices[n])).sum();
        r.insert("mappings.total_count", t(fields));
        r.insert("mappings.total_estimated_overhead_in_bytes", t(fields * 1024));
        r.insert("script.compilations", t(0));
        r.insert("script.cache_evictions", t(0));
        r.insert("script.compilation_limit_triggered", t(0));
        r.insert("query_cache.hit_count", t(0));
        r.insert("query_cache.miss_count", t(0));
        stats_cells(&mut r, "", &sum);
        render("nodes", vec![r], q)
    }

    fn cat_allocation(&self, arg: Option<&str>, q: &HashMap<String, String>) -> (u16, Value) {
        let s = self.0.lock().unwrap();
        let mut rows = Vec::new();
        let (mut shards, mut unassigned, mut bytes) = (0u64, 0u64, 0u64);
        for i in s.indices.values() {
            let (p, rep) = shard_counts(i);
            if super::cluster::allocation_disabled(i) {
                unassigned += p * (1 + rep);
                continue;
            }
            shards += p;
            unassigned += p * rep;
            bytes += stats::primaries(i).iter().map(|sh| stats::shard_store(i, *sh)).sum::<u64>();
        }
        if arg.is_none_or(node::selected) {
            let (total, avail) = super::nodes::disk();
            let mut r = Row::new();
            node_cells(&mut r, false);
            r.insert("shards", t(shards));
            r.insert("shards.undesired", t(0));
            r.insert("write_load.forecast", t("0.0"));
            r.insert("disk.indices.forecast", Cell::Bytes(bytes));
            r.insert("disk.indices", Cell::Bytes(bytes));
            r.insert("disk.used", Cell::Bytes(total - avail));
            r.insert("disk.avail", Cell::Bytes(avail));
            r.insert("disk.total", Cell::Bytes(total));
            r.insert("disk.percent", t((total - avail) * 100 / total.max(1)));
            r.insert("node.role", t(node::ROLE_CHARS));
            rows.push(r);
        }
        if unassigned > 0 && arg.is_none_or(|a| a == "*" || a == "_all") {
            let mut r = Row::new();
            r.insert("shards", t(unassigned));
            r.insert("node", t("UNASSIGNED"));
            rows.push(r);
        }
        render("allocation", rows, q)
    }

    fn cat_shards(&self, arg: Option<&str>, q: &HashMap<String, String>) -> (u16, Value) {
        let s = self.0.lock().unwrap();
        let mut q2 = q.clone();
        if !q.contains_key("expand_wildcards") {
            q2.insert("expand_wildcards".into(), "all".into());
        }
        let names = match stats::resolve(&s, arg.unwrap_or("_all"), &q2, &Resolve::STRICT_OPEN) {
            Ok(n) => n,
            Err(e) => return e,
        };
        let req = stats::StatsRequest::all();
        let mut rows = Vec::new();
        for n in &names {
            let i = &s.indices[n];
            let (_, rep) = shard_counts(i);
            let disabled = super::cluster::allocation_disabled(i);
            let created = i.settings["index"]["creation_date"].as_str().and_then(|v| v.parse::<i64>().ok()).unwrap_or(0);
            for shard in stats::primaries(i) {
                let mut base = Row::new();
                base.insert("index", t(n));
                base.insert("shard", t(shard));
                let mut p = base.clone();
                p.insert("prirep", t("p"));
                if disabled {
                    unassigned_cells(&mut p, created, "INDEX_CREATED", "empty_store");
                } else {
                    let sec = Value::Object(stats::shard_sections(i, shard, &req));
                    let store = sec["store"]["size_in_bytes"].as_u64().unwrap_or(0);
                    p.insert("state", t("STARTED"));
                    p.insert("docs", t(sec["docs"]["count"].as_u64().unwrap_or(0)));
                    p.insert("store", Cell::Bytes(store));
                    p.insert("dataset", Cell::Bytes(store));
                    node_cells(&mut p, false);
                    p.insert("index", t(n));
                    p.insert("recoverysource.type", Cell::Null);
                    let seq = stats::shard_max_seq(i, shard);
                    p.insert("seq_no.max", t(seq));
                    p.insert("seq_no.local_checkpoint", t(seq));
                    p.insert("seq_no.global_checkpoint", t(seq));
                    p.insert("sync_id", Cell::Null);
                    stats_cells(&mut p, "", &sec);
                }
                rows.push(p);
                for _ in 0..rep {
                    let mut r = base.clone();
                    r.insert("prirep", t("r"));
                    unassigned_cells(&mut r, created, "INDEX_CREATED", "peer");
                    rows.push(r);
                }
            }
        }
        render("shards", rows, q)
    }

    fn cat_segments(&self, arg: Option<&str>, q: &HashMap<String, String>) -> (u16, Value) {
        let s = self.0.lock().unwrap();
        let opts = Resolve { expand: "open", lenient: false, forbid_closed: true };
        let names = match stats::resolve(&s, arg.unwrap_or("_all"), q, &opts) {
            Ok(n) => n,
            Err(e) => return e,
        };
        let mut rows = Vec::new();
        for n in &names {
            let i = &s.indices[n];
            for shard in stats::primaries(i) {
                let docs = stats::segment_docs(i, shard);
                if docs == 0 {
                    continue;
                }
                let mut r = Row::new();
                node_cells(&mut r, false);
                r.insert("index", t(n));
                r.insert("shard", t(shard));
                r.insert("prirep", t("p"));
                r.insert("segment", t("_0"));
                r.insert("generation", t(0));
                r.insert("docs.count", t(docs));
                r.insert("docs.deleted", t(0));
                r.insert("size", Cell::Bytes(stats::shard_store(i, shard)));
                r.insert("size.memory", t(0));
                r.insert("committed", t(i.counters.shards.get(&shard).is_some_and(|c| c.flush_total > 0)));
                r.insert("searchable", t(true));
                r.insert("version", t("9.11.1"));
                r.insert("compound", t(true));
                rows.push(r);
            }
        }
        render("segments", rows, q)
    }

    fn cat_recovery(&self, arg: Option<&str>, q: &HashMap<String, String>) -> (u16, Value) {
        let s = self.0.lock().unwrap();
        let mut q2 = q.clone();
        if !q.contains_key("expand_wildcards") {
            q2.insert("expand_wildcards".into(), "open,closed".into());
        }
        let names = match stats::resolve(&s, arg.unwrap_or("_all"), &q2, &Resolve::STRICT_OPEN) {
            Ok(n) => n,
            Err(e) => return e,
        };
        let mut rows = Vec::new();
        for n in &names {
            let i = &s.indices[n];
            for shard in stats::primaries(i) {
                let mut r = Row::new();
                r.insert("index", t(n));
                r.insert("shard", t(shard));
                r.insert("start_time", t(i.counters.created_ms));
                r.insert("start_time_millis", t(i.counters.created_ms));
                r.insert("stop_time", t(i.counters.created_ms + 20));
                r.insert("stop_time_millis", t(i.counters.created_ms + 20));
                r.insert("time", Cell::Millis(20));
                let ty = if i.counters.existing_store { "existing_store" } else { "empty_store" };
                r.insert("type", t(ty));
                r.insert("stage", t("done"));
                r.insert("source_host", t("n/a"));
                r.insert("source_node", t("n/a"));
                r.insert("target_host", t(node::HOST));
                r.insert("target_node", t(node::NODE_NAME));
                r.insert("repository", t("n/a"));
                r.insert("snapshot", t("n/a"));
                r.insert("files", t(0));
                r.insert("files_recovered", t(0));
                r.insert("files_percent", t("0.0%"));
                r.insert("files_total", t(0));
                r.insert("bytes", Cell::Bytes(0));
                r.insert("bytes_recovered", Cell::Bytes(0));
                r.insert("bytes_percent", t("0.0%"));
                r.insert("bytes_total", Cell::Bytes(0));
                r.insert("translog_ops", t(0));
                r.insert("translog_ops_recovered", t(0));
                r.insert("translog_ops_percent", t("100.0%"));
                rows.push(r);
            }
        }
        render("recovery", rows, q)
    }

    fn cat_fielddata(&self, arg: Option<&str>, q: &HashMap<String, String>) -> (u16, Value) {
        let s = self.0.lock().unwrap();
        let wanted: Option<Vec<String>> = arg
            .map(str::to_string)
            .or_else(|| q.get("fields").cloned())
            .map(|f| f.split(',').map(|x| x.trim().to_string()).collect());
        let fields = wanted.clone().unwrap_or_else(|| vec!["*".into()]);
        let req = stats::StatsRequest {
            metrics: ["fielddata"].into_iter().collect(),
            fielddata_fields: Some(fields),
            completion_fields: None,
            groups: None,
            file_sizes: false,
            unloaded_segments: false,
        };
        let mut sum = json!({});
        for i in s.indices.values().filter(|i| i.opened) {
            stats::add_stats(&mut sum, &stats::index_sections(i, &req));
        }
        let rows = sum["fielddata"]["fields"]
            .as_object()
            .map(|m| {
                m.iter()
                    .map(|(f, v)| {
                        let mut r = Row::new();
                        node_cells(&mut r, true);
                        r.insert("field", t(f));
                        r.insert("size", Cell::Bytes(v["memory_size_in_bytes"].as_u64().unwrap_or(0)));
                        r
                    })
                    .collect()
            })
            .unwrap_or_default();
        render("fielddata", rows, q)
    }
}

fn unassigned_cells(r: &mut Row, created: i64, reason: &str, source: &str) {
    r.insert("state", t("UNASSIGNED"));
    r.insert("unassigned.reason", t(reason));
    r.insert("unassigned.at", t(dates::format(created, None, 0)));
    r.insert("unassigned.for", Cell::Millis((node::now_millis() - created).max(0)));
    r.insert("unassigned.details", Cell::Null);
    r.insert("recoverysource.type", t(source));
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(v: (u16, Value)) -> String {
        v.1[cat::RAW_TEXT].as_str().unwrap().to_string()
    }

    #[test]
    fn columns_by_alias_wildcard_and_bytes() {
        let mut r = Row::new();
        r.insert("index", t("foo"));
        r.insert("docs.count", t(2));
        r.insert("store.size", Cell::Bytes(2048));
        let q: HashMap<String, String> =
            [("h".to_string(), "i,dc,store.*".to_string())].into_iter().collect();
        assert_eq!(text(render("indices", vec![r.clone()], &q)), "foo 2 2kb\n");
        let q: HashMap<String, String> = [("h".to_string(), "store.size".to_string()), ("bytes".to_string(), "b".to_string())]
            .into_iter()
            .collect();
        assert_eq!(text(render("indices", vec![r], &q)), "2048\n");
    }

    #[test]
    fn help_lists_every_column() {
        let q: HashMap<String, String> = [("help".to_string(), String::new())].into_iter().collect();
        let h = text(render("count", vec![], &q));
        assert!(h.starts_with("epoch "));
        assert_eq!(h.lines().count(), 3);
    }
}
