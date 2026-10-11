//! Index statistics: per-shard counters kept as requests go by (index,
//! get, search, refresh, flush, ...), and the APIs that report them and
//! the shard layout behind them: `_stats`, `_segments`, `_recovery`,
//! `_shard_stores`, `_disk_usage` and `_field_usage_stats`.
//!
//! The counters are fed by `Engine::observe`, which looks at each
//! request and its response after the fact, so the request handlers stay
//! unaware of them. Sizes are estimates from the stored JSON; the shard
//! layout (which shard holds which document) is the real routing.

use super::node;
use super::*;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

/// One shard's operation counters.
#[derive(Clone, Debug, Default)]
pub(super) struct ShardCounters {
    pub index_total: u64,
    pub index_failed: u64,
    pub delete_total: u64,
    pub noop_update_total: u64,
    pub get_total: u64,
    pub get_exists: u64,
    pub get_missing: u64,
    pub query_total: u64,
    pub fetch_total: u64,
    pub scroll_total: u64,
    pub suggest_total: u64,
    /// `stats` groups a search named: group -> (query, fetch) totals.
    pub groups: BTreeMap<String, (u64, u64)>,
    pub refresh_total: u64,
    pub external_refresh_total: u64,
    pub flush_total: u64,
    pub merge_total: u64,
    pub bulk_ops: u64,
    pub bulk_bytes: u64,
    /// Operations (and their bytes) written since the last flush.
    pub translog_ops: u64,
    pub translog_bytes: u64,
    pub warmer_total: u64,
    /// A flush wrote the shard's documents to a segment.
    pub flushed: bool,
}

/// An index's counters: per shard, plus what applies to the whole index.
#[derive(Clone, Debug)]
pub(super) struct Counters {
    pub shards: BTreeMap<u64, ShardCounters>,
    /// Fields whose field data was loaded: name -> built by an
    /// aggregation (global ordinals) rather than a sort.
    pub fielddata: BTreeMap<String, bool>,
    /// Per-field usage (`_field_usage_stats`): field -> kind -> count.
    pub field_usage: BTreeMap<String, BTreeMap<&'static str, u64>>,
    pub created: Instant,
    pub created_ms: i64,
    pub last_search: Option<Instant>,
    pub last_write: Option<Instant>,
    /// Recovered from an existing store (reopened, or loaded from disk)
    /// rather than created empty.
    pub existing_store: bool,
}

impl Default for Counters {
    fn default() -> Self {
        Counters {
            shards: BTreeMap::new(),
            fielddata: BTreeMap::new(),
            field_usage: BTreeMap::new(),
            created: Instant::now(),
            created_ms: node::now_millis(),
            last_search: None,
            last_write: None,
            existing_store: false,
        }
    }
}

impl Counters {
    fn shard(&mut self, n: u64) -> &mut ShardCounters {
        self.shards.entry(n).or_default()
    }
}

/// Approximate stored bytes of one document.
fn doc_bytes(d: &Document) -> u64 {
    d.source.to_string().len() as u64 + 120
}

/// What an empty shard occupies on disk.
const EMPTY_SHARD_BYTES: u64 = 227;
/// A translog's header (an empty one is this big).
const EMPTY_TRANSLOG_BYTES: u64 = 55;

pub(super) fn is_hidden(i: &Index) -> bool {
    let v = &i.settings["index"]["hidden"];
    v.as_bool() == Some(true) || v.as_str() == Some("true")
}

fn alias_hidden(spec: &Value) -> bool {
    spec.get("is_hidden").and_then(Value::as_bool) == Some(true)
}

/// How a request's index expression resolves.
pub(super) struct Resolve {
    /// `expand_wildcards` when the request doesn't say.
    pub expand: &'static str,
    /// Missing names are skipped unless `ignore_unavailable=false`.
    pub lenient: bool,
    /// A named closed index is an error (`forbid_closed_indices`).
    pub forbid_closed: bool,
}

impl Resolve {
    pub const STRICT_OPEN: Resolve =
        Resolve { expand: "open", lenient: false, forbid_closed: false };
    pub const LENIENT_OPEN: Resolve =
        Resolve { expand: "open", lenient: true, forbid_closed: false };
}

/// Resolves an index expression the way Elasticsearch's resolver does:
/// names, aliases, `*` wildcards (open/closed/hidden per
/// `expand_wildcards`; a dot-prefixed pattern reaches dot-prefixed hidden
/// indices), `-` exclusions, `_all`, `ignore_unavailable` and
/// `allow_no_indices`. Sorted, without duplicates.
pub(super) fn resolve(
    s: &State,
    expr: &str,
    q: &HashMap<String, String>,
    opts: &Resolve,
) -> Result<Vec<String>, (u16, Value)> {
    let ew = q.get("expand_wildcards").map_or(opts.expand, String::as_str);
    let ews: Vec<&str> = ew.split(',').map(str::trim).collect();
    let all = ews.contains(&"all");
    let (want_open, want_closed, want_hidden) = (
        all || ews.contains(&"open"),
        all || ews.contains(&"closed"),
        all || ews.contains(&"hidden"),
    );
    let flag = |k: &str, default: bool| match q.get(k).map(String::as_str) {
        Some("true") | Some("") => true,
        Some("false") => false,
        _ => default,
    };
    let ignore_unavailable = flag("ignore_unavailable", opts.lenient);
    let allow_no_indices = flag("allow_no_indices", true);
    let forbid_closed = flag("forbid_closed_indices", opts.forbid_closed);
    let state_ok = |i: &Index| if i.opened { want_open } else { want_closed };
    let expr = if expr.trim().is_empty() { "_all" } else { expr };
    let mut names: BTreeSet<String> = BTreeSet::new();
    for (n, part) in expr.split(',').map(str::trim).enumerate() {
        if part.is_empty() {
            continue;
        }
        if n > 0
            && let Some(ex) = part.strip_prefix('-')
        {
            names.retain(|k| !glob_match(ex, k));
            continue;
        }
        if part == "_all" || part.contains('*') {
            let pat = if part == "_all" { "*" } else { part };
            let dot = pat.starts_with('.');
            for (name, i) in &s.indices {
                if glob_match(pat, name)
                    && state_ok(i)
                    && (!is_hidden(i) || want_hidden || (dot && name.starts_with('.')))
                {
                    names.insert(name.clone());
                }
            }
            for (name, i) in &s.indices {
                if i.aliases.iter().any(|(a, spec)| {
                    glob_match(pat, a)
                        && (!alias_hidden(spec) || want_hidden || (dot && a.starts_with('.')))
                }) && state_ok(i)
                {
                    names.insert(name.clone());
                }
            }
            continue;
        }
        if let Some(i) = s.indices.get(part) {
            if !i.opened && forbid_closed {
                if ignore_unavailable {
                    continue;
                }
                return Err(index_closed(part));
            }
            names.insert(part.to_string());
            continue;
        }
        let via_alias: Vec<String> = s
            .indices
            .iter()
            .filter(|(_, i)| i.aliases.contains_key(part) && (i.opened || !forbid_closed))
            .map(|(n, _)| n.clone())
            .collect();
        if via_alias.is_empty()
            && !s.indices.values().any(|i| i.aliases.contains_key(part))
            && !ignore_unavailable
        {
            return Err(missing_index(part));
        }
        names.extend(via_alias);
    }
    if names.is_empty() && !allow_no_indices {
        if expr == "_all" && s.indices.is_empty() {
            let (status, mut e) = missing_index("_all");
            let reason = json!("no such index [_all] and no indices exist");
            e["error"]["reason"] = reason.clone();
            e["error"]["root_cause"][0]["reason"] = reason;
            return Err((status, e));
        }
        return Err(missing_index(expr));
    }
    Ok(names.into_iter().collect())
}

/// The shard a stored document lives on.
fn doc_shard(i: &Index, id: &str) -> u64 {
    if shard_counts(i).0 <= 1 {
        return 0;
    }
    let key = i.docs.get(id).and_then(|d| d.routing.clone()).unwrap_or_else(|| id.to_string());
    shard_of(i, &key).max(0) as u64
}

/// The searchable (refreshed) documents of each shard.
pub(super) fn shard_docs(i: &Index) -> BTreeMap<u64, Vec<&CommittedDoc>> {
    let mut out: BTreeMap<u64, Vec<&CommittedDoc>> = BTreeMap::new();
    for d in &i.committed {
        out.entry(doc_shard(i, &d.id)).or_default().push(d);
    }
    out
}

/// Bytes a shard's store holds: an empty shard's files plus its
/// documents.
pub(super) fn shard_store(i: &Index, shard: u64) -> u64 {
    EMPTY_SHARD_BYTES
        + i.docs
            .iter()
            .filter(|(id, _)| doc_shard(i, id) == shard)
            .map(|(_, d)| doc_bytes(d))
            .sum::<u64>()
}

/// Every primary shard number of an index.
pub(super) fn primaries(i: &Index) -> Vec<u64> {
    (0..shard_counts(i).0.max(1)).collect()
}

/// The highest `_seq_no` written to a shard (-1 when none).
pub(super) fn shard_max_seq(i: &Index, shard: u64) -> i64 {
    if shard_counts(i).0 <= 1 {
        return i.seq;
    }
    i.shard_seq.get(&(shard as i64)).copied().unwrap_or(-1)
}

/// Documents in a shard's segment: the refreshed ones, or after a flush
/// every one written.
pub(super) fn segment_docs(i: &Index, shard: u64) -> usize {
    let refreshed = i.committed.iter().filter(|d| doc_shard(i, &d.id) == shard).count();
    if refreshed > 0 || !i.counters.shards.get(&shard).is_some_and(|c| c.flushed) {
        return refreshed;
    }
    i.docs.keys().filter(|id| doc_shard(i, id) == shard).count()
}

/// Segments a shard has: its refreshed (or flushed) documents sit in one.
pub(super) fn shard_segments(i: &Index, shard: u64) -> u64 {
    let flushed = i.counters.shards.get(&shard).is_some_and(|c| c.flushed);
    u64::from(flushed || i.committed.iter().any(|d| doc_shard(i, &d.id) == shard))
}

/// Fields of a mapping with a given type, as dotted names (multi-fields
/// included).
fn fields_of_type(m: &Value, ty: &str) -> Vec<String> {
    let mut leaves = Vec::new();
    mapped_leaves(m, "", &mut leaves);
    leaves
        .into_iter()
        .filter(|(_, d)| d.get("type").and_then(Value::as_str) == Some(ty))
        .map(|(n, _)| n)
        .collect()
}

/// A field's values in a source document; a multi-field (`a.b` where `a`
/// is a leaf) reads its parent's values.
fn field_values<'a>(m: &Value, source: &'a Value, field: &str) -> Vec<&'a Value> {
    let direct = search::raw_values(source, field);
    if !direct.is_empty() {
        return direct;
    }
    match field.rsplit_once('.') {
        Some((parent, _)) if search::resolve_field(m, parent).1.is_some() => {
            search::raw_values(source, parent)
        }
        _ => vec![],
    }
}

/// Bytes the completion suggester's FST holds for a field in `docs`.
fn completion_bytes(m: &Value, docs: &[&CommittedDoc], field: &str) -> u64 {
    let mut n = 0;
    for d in docs {
        for v in field_values(m, &d.source, field) {
            let inputs: Vec<&Value> = match v {
                Value::Object(o) => match o.get("input") {
                    Some(Value::Array(a)) => a.iter().collect(),
                    Some(x) => vec![x],
                    None => vec![],
                },
                Value::Array(a) => a.iter().collect(),
                x => vec![x],
            };
            for i in inputs {
                if let Some(s) = i.as_str() {
                    n += s.len() as u64 + 24;
                }
            }
        }
    }
    n
}

/// Bytes the field data (or global ordinals) of a field takes in `docs`:
/// its distinct terms.
fn fielddata_bytes(m: &Value, docs: &[&CommittedDoc], field: &str) -> u64 {
    let (_, ty) = search::resolve_field(m, field);
    let mut terms: BTreeSet<String> = BTreeSet::new();
    for d in docs {
        if ty.as_deref() == Some("text") {
            for v in field_values(m, &d.source, field) {
                if let Some(s) = v.as_str() {
                    terms.extend(search::analyze_for(m, field, s));
                }
            }
        } else {
            for v in field_values(m, &d.source, field) {
                terms.insert(match v {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                });
            }
        }
    }
    terms.iter().map(|t| t.len() as u64 + 16).sum()
}

/// The vectors stored for fields of a type (`dense_vector`,
/// `sparse_vector`) in `docs`.
fn vector_count(m: &Value, docs: &[&CommittedDoc], ty: &str) -> u64 {
    let fields = fields_of_type(m, ty);
    docs.iter()
        .map(|d| {
            fields.iter().filter(|f| !search::raw_values(&d.source, f).is_empty()).count() as u64
        })
        .sum()
}

/// Which stats a request asked for and how (`fields`, `groups`, ...).
pub(super) struct StatsRequest {
    pub metrics: BTreeSet<&'static str>,
    pub fielddata_fields: Option<Vec<String>>,
    pub completion_fields: Option<Vec<String>>,
    pub groups: Option<Vec<String>>,
    pub file_sizes: bool,
    pub unloaded_segments: bool,
}

/// Index-level metric names, in the order Elasticsearch prints sections.
pub(super) const INDEX_METRICS: &[&str] = &[
    "docs",
    "shard_stats",
    "store",
    "indexing",
    "get",
    "search",
    "merge",
    "refresh",
    "flush",
    "warmer",
    "query_cache",
    "fielddata",
    "completion",
    "segments",
    "translog",
    "request_cache",
    "recovery",
    "bulk",
    "mappings",
    "dense_vector",
    "sparse_vector",
];

impl StatsRequest {
    /// Parses the metric list (`_all` or empty: all) and the field
    /// parameters; an unknown metric is a 400 naming it.
    pub(super) fn parse(
        metrics: Option<&str>,
        q: &HashMap<String, String>,
        path: &str,
        what: &str,
    ) -> Result<Self, (u16, Value)> {
        let mut set = BTreeSet::new();
        let mut bad = Vec::new();
        match metrics {
            None | Some("_all") | Some("") => set.extend(INDEX_METRICS.iter().copied()),
            Some(list) => {
                for m in list.split(',').map(str::trim).filter(|m| !m.is_empty()) {
                    match INDEX_METRICS.iter().find(|k| **k == m) {
                        Some(k) => {
                            set.insert(*k);
                        }
                        None if m == "_all" => set.extend(INDEX_METRICS.iter().copied()),
                        None => bad.push(m.to_string()),
                    }
                }
            }
        }
        if !bad.is_empty() {
            let mut known: Vec<&str> = INDEX_METRICS.to_vec();
            known.push("_all");
            let msg = node::unrecognized(path, &bad, &known, what);
            return Err((400, error("illegal_argument_exception", &msg, 400)));
        }
        let list = |k: &str| {
            q.get(k).map(|v| {
                v.split(',')
                    .map(|x| x.trim().to_string())
                    .filter(|x| !x.is_empty())
                    .collect::<Vec<_>>()
            })
        };
        let fields = list("fields");
        let truthy = |k: &str| q.get(k).is_some_and(|v| v.is_empty() || v == "true");
        Ok(StatsRequest {
            metrics: set,
            fielddata_fields: list("fielddata_fields").or_else(|| fields.clone()),
            completion_fields: list("completion_fields").or(fields),
            groups: list("groups"),
            file_sizes: truthy("include_segment_file_sizes"),
            unloaded_segments: truthy("include_unloaded_segments"),
        })
    }

    pub(super) fn all() -> Self {
        StatsRequest {
            metrics: INDEX_METRICS.iter().copied().collect(),
            fielddata_fields: None,
            completion_fields: None,
            groups: None,
            file_sizes: false,
            unloaded_segments: false,
        }
    }

    fn wants(&self, m: &str) -> bool {
        self.metrics.contains(m)
    }
}

fn any_glob(patterns: &[String], name: &str) -> bool {
    patterns.iter().any(|p| glob_match(p, name))
}

/// One shard's stats sections, as `_stats` reports a shard copy.
pub(super) fn shard_sections(i: &Index, shard: u64, r: &StatsRequest) -> Map<String, Value> {
    let c = i.counters.shards.get(&shard).cloned().unwrap_or_default();
    let by_shard = shard_docs(i);
    let docs: Vec<&CommittedDoc> = by_shard.get(&shard).cloned().unwrap_or_default();
    let store = shard_store(i, shard);
    let m = &i.mappings;
    let mut out = Map::new();
    for metric in INDEX_METRICS {
        if !r.wants(metric) {
            continue;
        }
        let (key, v) = match *metric {
            "docs" => {
                ("docs", json!({"count": docs.len(), "deleted": 0, "total_size_in_bytes": store}))
            }
            "shard_stats" => ("shard_stats", json!({"total_count": 1})),
            "store" => (
                "store",
                json!({"size_in_bytes": store, "total_data_set_size_in_bytes": store, "reserved_in_bytes": 0}),
            ),
            "indexing" => (
                "indexing",
                json!({"index_total": c.index_total, "index_time_in_millis": 0, "index_current": 0,
                       "index_failed": c.index_failed, "delete_total": c.delete_total,
                       "delete_time_in_millis": 0, "delete_current": 0,
                       "noop_update_total": c.noop_update_total, "is_throttled": false,
                       "throttle_time_in_millis": 0, "write_load": 0.0}),
            ),
            "get" => (
                "get",
                json!({"total": c.get_total, "time_in_millis": 0, "exists_total": c.get_exists,
                       "exists_time_in_millis": 0, "missing_total": c.get_missing,
                       "missing_time_in_millis": 0, "current": 0}),
            ),
            "search" => {
                let mut v = json!({"open_contexts": 0, "query_total": c.query_total,
                    "query_time_in_millis": 0, "query_current": 0, "fetch_total": c.fetch_total,
                    "fetch_time_in_millis": 0, "fetch_current": 0, "scroll_total": c.scroll_total,
                    "scroll_time_in_millis": 0, "scroll_current": 0,
                    "suggest_total": c.suggest_total, "suggest_time_in_millis": 0,
                    "suggest_current": 0});
                if let Some(wanted) = &r.groups {
                    let groups: Map<String, Value> = c
                        .groups
                        .iter()
                        .filter(|(g, _)| any_glob(wanted, g))
                        .map(|(g, (qt, ft))| {
                            (
                                g.clone(),
                                json!({"query_total": qt, "query_time_in_millis": 0,
                                "query_current": 0, "fetch_total": ft, "fetch_time_in_millis": 0,
                                "fetch_current": 0, "scroll_total": 0, "scroll_time_in_millis": 0,
                                "scroll_current": 0, "suggest_total": 0,
                                "suggest_time_in_millis": 0, "suggest_current": 0}),
                            )
                        })
                        .collect();
                    if !groups.is_empty() {
                        v["groups"] = Value::Object(groups);
                    }
                }
                ("search", v)
            }
            "merge" => (
                "merges",
                json!({"current": 0, "current_docs": 0, "current_size_in_bytes": 0,
                       "total": c.merge_total, "total_time_in_millis": 0, "total_docs": 0,
                       "total_size_in_bytes": 0, "total_stopped_time_in_millis": 0,
                       "total_throttled_time_in_millis": 0,
                       "total_auto_throttle_in_bytes": 20_971_520}),
            ),
            "refresh" => (
                "refresh",
                json!({"total": c.refresh_total, "total_time_in_millis": 0,
                       "external_total": c.external_refresh_total,
                       "external_total_time_in_millis": 0, "listeners": 0}),
            ),
            "flush" => (
                "flush",
                json!({"total": c.flush_total, "periodic": 0, "total_time_in_millis": 0,
                       "total_time_excluding_waiting_on_lock_in_millis": 0}),
            ),
            "warmer" => (
                "warmer",
                json!({"current": 0, "total": c.warmer_total, "total_time_in_millis": 0}),
            ),
            "query_cache" => (
                "query_cache",
                json!({"memory_size_in_bytes": 0, "total_count": 0, "hit_count": 0, "miss_count": 0,
                       "cache_size": 0, "cache_count": 0, "evictions": 0}),
            ),
            "fielddata" => {
                let loaded: Vec<(&String, &bool)> = i.counters.fielddata.iter().collect();
                let sizes: Vec<(&String, bool, u64)> = loaded
                    .iter()
                    .map(|(f, ords)| (*f, **ords, fielddata_bytes(m, &docs, f)))
                    .filter(|(_, _, n)| *n > 0)
                    .collect();
                let total: u64 = sizes.iter().map(|(_, _, n)| n).sum();
                let mut v = json!({"memory_size_in_bytes": total, "evictions": 0,
                                   "global_ordinals": {"build_time_in_millis": 0}});
                if let Some(wanted) = &r.fielddata_fields {
                    let fields: Map<String, Value> = sizes
                        .iter()
                        .filter(|(f, _, _)| any_glob(wanted, f))
                        .map(|(f, _, n)| ((*f).clone(), json!({"memory_size_in_bytes": n})))
                        .collect();
                    if !fields.is_empty() {
                        v["fields"] = Value::Object(fields);
                    }
                    let ords: Map<String, Value> = sizes
                        .iter()
                        .filter(|(f, o, _)| *o && any_glob(wanted, f))
                        .map(|(f, _, _)| {
                            let count = docs
                                .iter()
                                .map(|d| field_values(m, &d.source, f).len())
                                .sum::<usize>();
                            (
                                (*f).clone(),
                                json!({"build_time_in_millis": 0, "shard_max_value_count": count}),
                            )
                        })
                        .collect();
                    if !ords.is_empty() {
                        v["global_ordinals"]["fields"] = Value::Object(ords);
                    }
                }
                ("fielddata", v)
            }
            "completion" => {
                let fields: Vec<(String, u64)> = fields_of_type(m, "completion")
                    .into_iter()
                    .map(|f| {
                        let n = completion_bytes(m, &docs, &f);
                        (f, n)
                    })
                    .collect();
                let total: u64 = fields.iter().map(|(_, n)| n).sum();
                let mut v = json!({"size_in_bytes": total});
                if let Some(wanted) = &r.completion_fields {
                    let picked: Map<String, Value> = fields
                        .iter()
                        .filter(|(f, n)| *n > 0 && any_glob(wanted, f))
                        .map(|(f, n)| (f.clone(), json!({"size_in_bytes": n})))
                        .collect();
                    if !picked.is_empty() {
                        v["fields"] = Value::Object(picked);
                    }
                }
                ("completion", v)
            }
            "segments" => {
                let count =
                    if i.opened || r.unloaded_segments { shard_segments(i, shard) } else { 0 };
                let mut v = json!({"count": count, "memory_in_bytes": 0,
                    "terms_memory_in_bytes": 0, "stored_fields_memory_in_bytes": 0,
                    "term_vectors_memory_in_bytes": 0, "norms_memory_in_bytes": 0,
                    "points_memory_in_bytes": 0, "doc_values_memory_in_bytes": 0,
                    "index_writer_memory_in_bytes": 0, "version_map_memory_in_bytes": 0,
                    "fixed_bit_set_memory_in_bytes": 0, "max_unsafe_auto_id_timestamp": -1,
                    "file_sizes": {}});
                if r.file_sizes && count > 0 {
                    let mut files = Map::new();
                    for (ext, desc, size) in [
                        ("si", "Segment Info", 360u64),
                        ("cfe", "Compound Files Entries", 479),
                        ("cfs", "Compound Files", store.saturating_sub(EMPTY_SHARD_BYTES).max(1)),
                    ] {
                        files.insert(
                            ext.into(),
                            json!({"size_in_bytes": size * count,
                            "min_size_in_bytes": size, "max_size_in_bytes": size,
                            "average_size_in_bytes": size, "count": count, "description": desc}),
                        );
                    }
                    v["file_sizes"] = Value::Object(files);
                }
                ("segments", v)
            }
            "translog" => {
                let ops = if i.opened { c.translog_ops } else { 0 };
                let bytes = EMPTY_TRANSLOG_BYTES + if i.opened { c.translog_bytes } else { 0 };
                let age = i.counters.last_write.unwrap_or(i.counters.created).elapsed().as_millis()
                    as u64;
                (
                    "translog",
                    json!({"operations": ops, "size_in_bytes": bytes, "uncommitted_operations": ops,
                           "uncommitted_size_in_bytes": bytes, "earliest_last_modified_age": age}),
                )
            }
            "request_cache" => (
                "request_cache",
                json!({"memory_size_in_bytes": 0, "evictions": 0, "hit_count": 0, "miss_count": 0}),
            ),
            "recovery" => (
                "recovery",
                json!({"current_as_source": 0, "current_as_target": 0, "throttle_time_in_millis": 0}),
            ),
            "bulk" => {
                let avg = |x: u64| x.checked_div(c.bulk_ops).unwrap_or(0);
                (
                    "bulk",
                    json!({"total_operations": c.bulk_ops, "total_time_in_millis": 0,
                           "total_size_in_bytes": c.bulk_bytes, "avg_time_in_millis": 0,
                           "avg_size_in_bytes": avg(c.bulk_bytes)}),
                )
            }
            "dense_vector" => {
                ("dense_vector", json!({"value_count": vector_count(m, &docs, "dense_vector")}))
            }
            "sparse_vector" => {
                ("sparse_vector", json!({"value_count": vector_count(m, &docs, "sparse_vector")}))
            }
            _ => continue,
        };
        out.insert(key.into(), v);
    }
    out
}

/// Adds stats `b` into `a`: numbers summed (`max_unsafe_auto_id_timestamp`
/// by max, `earliest_last_modified_age` by min), maps merged.
pub(super) fn add_stats(a: &mut Value, b: &Value) {
    match (a, b) {
        (Value::Object(x), Value::Object(y)) => {
            for (k, v) in y {
                match x.get_mut(k) {
                    None => {
                        x.insert(k.clone(), v.clone());
                    }
                    Some(cur) => match (k.as_str(), &*cur, v) {
                        ("max_unsafe_auto_id_timestamp", Value::Number(p), Value::Number(q)) => {
                            *cur = json!(p.as_i64().unwrap_or(-1).max(q.as_i64().unwrap_or(-1)));
                        }
                        ("earliest_last_modified_age", Value::Number(p), Value::Number(q)) => {
                            *cur = json!(p.as_u64().unwrap_or(0).min(q.as_u64().unwrap_or(0)));
                        }
                        ("min_size_in_bytes", Value::Number(p), Value::Number(q)) => {
                            *cur = json!(p.as_u64().unwrap_or(0).min(q.as_u64().unwrap_or(0)));
                        }
                        ("max_size_in_bytes", Value::Number(p), Value::Number(q)) => {
                            *cur = json!(p.as_u64().unwrap_or(0).max(q.as_u64().unwrap_or(0)));
                        }
                        _ => add_stats(cur, v),
                    },
                }
            }
        }
        (Value::Number(x), Value::Number(y)) => {
            let sum = match (x.as_u64(), y.as_u64(), x.as_i64(), y.as_i64()) {
                (Some(p), Some(q), _, _) => json!(p + q),
                (_, _, Some(p), Some(q)) => json!(p + q),
                _ => json!(x.as_f64().unwrap_or(0.0) + y.as_f64().unwrap_or(0.0)),
            };
            *x = match sum {
                Value::Number(n) => n,
                _ => x.clone(),
            };
        }
        _ => {}
    }
}

/// The shard-level extras of `level=shards` (routing, commit, seq_no,
/// retention leases, path, search idleness).
fn shard_extras(name: &str, i: &Index, shard: u64) -> Map<String, Value> {
    let max_seq = shard_max_seq(i, shard);
    let idle_ms = i.counters.last_search.unwrap_or(i.counters.created).elapsed().as_millis() as u64;
    let uuid = i.settings["index"]["uuid"].as_str().unwrap_or("_na_").to_string();
    let docs = shard_docs(i).get(&shard).map_or(0, Vec::len);
    let mut m = Map::new();
    m.insert(
        "routing".into(),
        json!({"state": "STARTED", "primary": true, "node": node::NODE_ID, "relocating_node": null}),
    );
    m.insert(
        "commit".into(),
        json!({"id": format!("{}{shard:02}AAAAAAAAAAAAA==", &uuid[..uuid.len().min(9)]),
               "generation": 2 + i.counters.shards.get(&shard).map_or(0, |c| c.flush_total),
               "user_data": {"local_checkpoint": max_seq.to_string(), "max_seq_no": max_seq.to_string(),
                             "max_unsafe_auto_id_timestamp": "-1", "es_version": "8512000",
                             "history_uuid": format!("{uuid}-h"), "translog_uuid": format!("{uuid}-t")},
               "num_docs": docs}),
    );
    m.insert(
        "seq_no".into(),
        json!({"max_seq_no": max_seq, "local_checkpoint": max_seq, "global_checkpoint": max_seq}),
    );
    m.insert(
        "retention_leases".into(),
        json!({"primary_term": 1, "version": 1, "leases": [{
            "id": format!("peer_recovery/{}", node::NODE_ID), "retaining_seq_no": max_seq + 1,
            "timestamp": i.counters.created_ms, "source": "peer recovery"}]}),
    );
    let path = format!("/usr/share/elasticsearch/data/indices/{uuid}/{shard}");
    m.insert(
        "shard_path".into(),
        json!({"state_path": path, "data_path": path, "is_custom_data_path": false}),
    );
    let _ = name;
    m.insert("search_idle".into(), json!(false));
    m.insert("search_idle_time".into(), json!(idle_ms));
    m
}

/// An index's stats sections (primaries; replicas never start on one
/// node, so `total` is the same).
pub(super) fn index_sections(i: &Index, r: &StatsRequest) -> Value {
    let mut sum = json!({});
    for shard in primaries(i) {
        add_stats(&mut sum, &Value::Object(shard_sections(i, shard, r)));
    }
    sum
}

impl Engine {
    /// `GET [/<index>]/_stats[/<metric>]`.
    pub(super) fn indices_stats(
        &self,
        expr: &str,
        metric: Option<&str>,
        q: &HashMap<String, String>,
        path: &str,
    ) -> (u16, Value) {
        let r = match StatsRequest::parse(metric, q, path, "metric") {
            Ok(r) => r,
            Err(e) => return e,
        };
        let level = q.get("level").map_or("indices", String::as_str);
        if !matches!(level, "cluster" | "indices" | "shards") {
            let msg = format!(
                "level parameter must be one of [cluster] or [indices] or [shards] but was [{level}]"
            );
            return (400, error("illegal_argument_exception", &msg, 400));
        }
        let s = self.0.lock().unwrap();
        let opts = Resolve { expand: "open", lenient: false, forbid_closed: true };
        let names = match resolve(&s, expr, q, &opts) {
            Ok(n) => n,
            Err(e) => return e,
        };
        let mut all = json!({});
        let mut indices = Map::new();
        let (mut total_shards, mut ok_shards) = (0u64, 0u64);
        for n in &names {
            let i = &s.indices[n];
            let (p, rep) = shard_counts(i);
            total_shards += p * (1 + rep);
            ok_shards += p;
            let sections = index_sections(i, &r);
            add_stats(&mut all, &sections);
            if level == "cluster" {
                continue;
            }
            let health = Self::health_of(&s, std::slice::from_ref(n))["status"].clone();
            let mut entry = json!({
                "uuid": i.settings["index"]["uuid"], "health": health,
                "status": if i.opened { "open" } else { "close" },
                "primaries": sections, "total": sections,
            });
            if level == "shards" {
                let mut shards = Map::new();
                for shard in primaries(i) {
                    let mut copy = Map::new();
                    let extras = shard_extras(n, i, shard);
                    copy.insert("routing".into(), extras["routing"].clone());
                    copy.extend(shard_sections(i, shard, &r));
                    for (k, v) in extras {
                        if k != "routing" {
                            copy.insert(k, v);
                        }
                    }
                    shards.insert(shard.to_string(), json!([Value::Object(copy)]));
                }
                entry["shards"] = Value::Object(shards);
            }
            indices.insert(n.clone(), entry);
        }
        let mut out = json!({
            "_shards": {"total": total_shards, "successful": ok_shards, "failed": 0},
            "_all": {"primaries": all, "total": all},
        });
        if level != "cluster" {
            out["indices"] = Value::Object(indices);
        }
        if human(q) {
            node::add_human(&mut out);
        }
        (200, out)
    }

    /// `GET [/<index>]/_segments`.
    pub(super) fn segments_api(&self, expr: &str, q: &HashMap<String, String>) -> (u16, Value) {
        let s = self.0.lock().unwrap();
        let opts = Resolve { expand: "open", lenient: false, forbid_closed: true };
        let names = match resolve(&s, expr, q, &opts) {
            Ok(n) => n,
            Err(e) => return e,
        };
        let mut indices = Map::new();
        let (mut total, mut ok) = (0u64, 0u64);
        for n in &names {
            let i = &s.indices[n];
            let (p, rep) = shard_counts(i);
            total += p * (1 + rep);
            ok += p;
            let mut shards = Map::new();
            for shard in primaries(i) {
                let docs = segment_docs(i, shard);
                let mut segments = Map::new();
                if docs > 0 {
                    segments.insert(
                        "_0".into(),
                        json!({"generation": 0, "num_docs": docs, "deleted_docs": 0,
                               "size_in_bytes": shard_store(i, shard), "committed": true,
                               "search": true, "version": "9.11.1", "compound": true,
                               "attributes": {"Lucene90StoredFieldsFormat.mode": "BEST_SPEED"}}),
                    );
                }
                let n = segments.len();
                shards.insert(
                    shard.to_string(),
                    json!([{"routing": {"state": "STARTED", "primary": true, "node": node::NODE_ID},
                            "num_committed_segments": n, "num_search_segments": n,
                            "segments": segments}]),
                );
            }
            indices.insert(n.clone(), json!({"shards": shards}));
        }
        (
            200,
            json!({"_shards": {"total": total, "successful": ok, "failed": 0}, "indices": indices}),
        )
    }

    /// `GET [/<index>]/_recovery`: each primary was recovered once, from
    /// an empty store when the index was created, from its existing store
    /// when it was reopened (or loaded from disk).
    pub(super) fn recovery_api(&self, expr: &str, q: &HashMap<String, String>) -> (u16, Value) {
        let s = self.0.lock().unwrap();
        let opts = Resolve { expand: "open,closed", lenient: false, forbid_closed: false };
        let names = match resolve(&s, expr, q, &opts) {
            Ok(n) => n,
            Err(e) => return e,
        };
        let detailed = q.get("detailed").is_some_and(|v| v.is_empty() || v == "true");
        let mut out = Map::new();
        for n in &names {
            let i = &s.indices[n];
            let existing = i.counters.existing_store;
            let start = i.counters.created_ms;
            let shards: Vec<Value> = primaries(i)
                .into_iter()
                .map(|shard| {
                    let mut files = json!({"total": 0, "reused": 0, "recovered": 0, "percent": "0.0%"});
                    if detailed {
                        files["details"] = json!([]);
                    }
                    json!({
                        "id": shard, "type": if existing { "EXISTING_STORE" } else { "EMPTY_STORE" },
                        "stage": "DONE", "primary": true, "start_time_in_millis": start,
                        "stop_time_in_millis": start + 20, "total_time_in_millis": 20,
                        "source": if existing { json!({"bootstrap_new_history_uuid": false}) } else { json!({}) },
                        "target": {"id": node::NODE_ID, "host": node::HOST,
                                   "transport_address": node::TRANSPORT_ADDRESS, "ip": node::HOST,
                                   "name": node::NODE_NAME},
                        "index": {"size": {"total_in_bytes": 0, "reused_in_bytes": 0,
                                           "recovered_in_bytes": 0,
                                           "recovered_from_snapshot_in_bytes": 0,
                                           "percent": "0.0%"},
                                  "files": files, "total_time_in_millis": 0,
                                  "source_throttle_time_in_millis": 0,
                                  "target_throttle_time_in_millis": 0},
                        "translog": {"recovered": 0, "total": 0, "percent": "100.0%",
                                     "total_on_start": 0, "total_time_in_millis": 0},
                        "verify_index": {"check_index_time_in_millis": 0, "total_time_in_millis": 0},
                    })
                })
                .collect();
            out.insert(n.clone(), json!({"shards": shards}));
        }
        let mut out = Value::Object(out);
        if human(q) {
            add_recovery_human(&mut out);
        }
        (200, out)
    }

    /// `GET [/<index>]/_shard_stores`: the store of each primary whose
    /// shard health matches `status` (default `yellow,red`).
    pub(super) fn shard_stores_api(&self, expr: &str, q: &HashMap<String, String>) -> (u16, Value) {
        let s = self.0.lock().unwrap();
        let names = match resolve(&s, expr, q, &Resolve::STRICT_OPEN) {
            Ok(n) => n,
            Err(e) => return e,
        };
        let wanted: Vec<&str> = q
            .get("status")
            .map_or(vec!["yellow", "red"], |v| v.split(',').map(str::trim).collect());
        let mut indices = Map::new();
        for n in &names {
            let i = &s.indices[n];
            let (_, rep) = shard_counts(i);
            let health = if rep > 0 { "yellow" } else { "green" };
            if !wanted.contains(&"all") && !wanted.contains(&health) {
                continue;
            }
            let uuid = i.settings["index"]["uuid"].as_str().unwrap_or("_na_");
            let mut shards = Map::new();
            for shard in primaries(i) {
                let mut store = Map::new();
                store.insert(node::NODE_ID.into(), node::discovery_node());
                store.insert("allocation_id".into(), json!(format!("{uuid}{shard}")));
                store.insert("allocation".into(), json!("primary"));
                shards.insert(shard.to_string(), json!({"stores": [Value::Object(store)]}));
            }
            indices.insert(n.clone(), json!({"shards": shards}));
        }
        (200, json!({"indices": indices}))
    }

    /// `POST /<index>/_disk_usage?run_expensive_tasks=true`: the bytes
    /// each field's structures (inverted index, stored fields, doc values,
    /// points, norms, vectors) take, estimated from the documents.
    pub(super) fn disk_usage_api(&self, expr: &str, q: &HashMap<String, String>) -> (u16, Value) {
        if !q.get("run_expensive_tasks").is_some_and(|v| v.is_empty() || v == "true") {
            return (
                400,
                error(
                    "illegal_argument_exception",
                    "analyzing the disk usage of an index is expensive and resource-intensive, the parameter [run_expensive_tasks] must be set to [true] in order for the task to be performed.",
                    400,
                ),
            );
        }
        let s = self.0.lock().unwrap();
        let names = match resolve(&s, expr, q, &Resolve::STRICT_OPEN) {
            Ok(n) => n,
            Err(e) => return e,
        };
        let mut out = Map::new();
        let mut shards = 0;
        for n in &names {
            let i = &s.indices[n];
            shards += primaries(i).len();
            out.insert(n.clone(), disk_usage(i));
        }
        let mut body = json!({"_shards": {"total": shards, "successful": shards, "failed": 0}});
        for (k, v) in out {
            body[k] = v;
        }
        (200, body)
    }

    /// `GET /<index>/_field_usage_stats`: per shard, how often each field's
    /// structures were used by searches since the index was created.
    pub(super) fn field_usage_api(&self, expr: &str, q: &HashMap<String, String>) -> (u16, Value) {
        let s = self.0.lock().unwrap();
        let names = match resolve(&s, expr, q, &Resolve::STRICT_OPEN) {
            Ok(n) => n,
            Err(e) => return e,
        };
        let mut out = Map::new();
        let mut total = 0;
        for n in &names {
            let i = &s.indices[n];
            let mut fields = Map::new();
            let mut all = usage_entry(&BTreeMap::new());
            for (f, kinds) in &i.counters.field_usage {
                let e = usage_entry(kinds);
                add_stats(&mut all, &e);
                fields.insert(f.clone(), e);
            }
            let shard_entries: Vec<Value> = primaries(i)
                .into_iter()
                .map(|shard| {
                    // Usage is tracked per index; it is reported on the
                    // first shard.
                    let (f, a) = if shard == 0 {
                        (Value::Object(fields.clone()), all.clone())
                    } else {
                        (json!({}), usage_entry(&BTreeMap::new()))
                    };
                    json!({"tracking_id": format!("{}-{shard}", i.settings["index"]["uuid"].as_str().unwrap_or("_na_")),
                           "tracking_started_at_millis": i.counters.created_ms,
                           "routing": {"state": "STARTED", "primary": true, "node": node::NODE_ID, "relocating_node": null},
                           "stats": {"all_fields": a, "fields": f}})
                })
                .collect();
            total += shard_entries.len();
            out.insert(n.clone(), json!({"shards": shard_entries}));
        }
        let mut body = json!({"_shards": {"total": total, "successful": total, "failed": 0}});
        for (k, v) in out {
            body[k] = v;
        }
        (200, body)
    }
}

fn usage_entry(kinds: &BTreeMap<&'static str, u64>) -> Value {
    let g = |k: &str| kinds.get(k).copied().unwrap_or(0);
    let inverted =
        ["terms", "postings", "proximity", "term_frequencies", "positions", "offsets", "payloads"];
    let any = kinds.values().copied().max().unwrap_or(0);
    let mut inv = Map::new();
    for k in inverted {
        inv.insert(k.into(), json!(g(k)));
    }
    json!({"any": any, "inverted_index": inv, "stored_fields": g("stored_fields"),
           "doc_values": g("doc_values"), "points": g("points"), "norms": g("norms"),
           "term_vectors": g("term_vectors"), "knn_vectors": g("knn_vectors")})
}

/// `?human` on a recovery response: readable times and sizes.
fn add_recovery_human(v: &mut Value) {
    let Some(m) = v.as_object_mut() else { return };
    for idx in m.values_mut() {
        let Some(shards) = idx.get_mut("shards").and_then(Value::as_array_mut) else { continue };
        for sh in shards {
            for k in ["start_time", "stop_time"] {
                if let Some(ms) = sh.get(format!("{k}_in_millis")).and_then(Value::as_i64) {
                    sh[k] = json!(dates::format(ms, None, 0));
                }
            }
            node::add_human(sh);
            // `add_human` skips epoch keys; it gave sizes and durations.
        }
    }
}

pub(super) fn human(q: &HashMap<String, String>) -> bool {
    q.get("human").is_some_and(|v| v.is_empty() || v == "true")
}

/// The disk usage breakdown of one index.
fn disk_usage(i: &Index) -> Value {
    let m = &i.mappings;
    let mut leaves = Vec::new();
    mapped_leaves(m, "", &mut leaves);
    let docs: Vec<&Document> = i.docs.values().collect();
    let mut fields = Map::new();
    let zero = |inv: u64, stored: u64, dv: u64, points: u64, norms: u64, knn: u64| {
        let total = inv + stored + dv + points + norms + knn;
        json!({"total": node::human_size(total), "total_in_bytes": total,
               "inverted_index": {"total": node::human_size(inv), "total_in_bytes": inv},
               "stored_fields": node::human_size(stored), "stored_fields_in_bytes": stored,
               "doc_values": node::human_size(dv), "doc_values_in_bytes": dv,
               "points": node::human_size(points), "points_in_bytes": points,
               "norms": node::human_size(norms), "norms_in_bytes": norms,
               "term_vectors": "0b", "term_vectors_in_bytes": 0,
               "knn_vectors": node::human_size(knn), "knn_vectors_in_bytes": knn})
    };
    let n = docs.len() as u64;
    if n > 0 {
        let source: u64 = docs.iter().map(|d| d.source.to_string().len() as u64).sum();
        fields.insert("_field_names".into(), zero(n * 4, 0, 0, 0, 0, 0));
        fields.insert("_id".into(), zero(n * 10, n * 10, 0, 0, 0, 0));
        fields.insert("_primary_term".into(), zero(0, 0, n, 0, 0, 0));
        fields.insert("_seq_no".into(), zero(0, 0, n * 2, n * 2, 0, 0));
        fields.insert("_version".into(), zero(0, 0, n, 0, 0, 0));
        fields.insert("_source".into(), zero(0, source, 0, 0, 0, 0));
    }
    for (name, def) in &leaves {
        let ty = def.get("type").and_then(Value::as_str).unwrap_or("object");
        let bytes: u64 = docs
            .iter()
            .flat_map(|d| field_values(m, &d.source, name))
            .map(|v| match v {
                Value::String(s) => s.len() as u64,
                Value::Array(a) => a.len() as u64 * 4,
                _ => 8,
            })
            .sum();
        if bytes == 0 {
            continue;
        }
        let dv_on = def.get("doc_values").and_then(Value::as_bool) != Some(false);
        let idx_on = def.get("index").and_then(Value::as_bool) != Some(false);
        let (inv, dv, points, norms, knn) = match ty {
            "text" | "match_only_text" => (bytes + 8, 0, 0, if idx_on { n } else { 0 }, 0),
            "keyword" | "constant_keyword" | "wildcard" => {
                (if idx_on { bytes + 8 } else { 0 }, if dv_on { bytes } else { 0 }, 0, 0, 0)
            }
            "dense_vector" => (0, 0, 0, 0, bytes),
            "long" | "integer" | "short" | "byte" | "double" | "float" | "half_float"
            | "scaled_float" | "unsigned_long" | "date" | "date_nanos" | "ip" | "boolean" => {
                (0, if dv_on { bytes } else { 0 }, if idx_on { bytes } else { 0 }, 0, 0)
            }
            _ => (bytes, 0, 0, 0, 0),
        };
        fields.insert(name.clone(), zero(inv, 0, dv, points, norms, knn));
    }
    let mut all = json!({});
    for v in fields.values() {
        add_stats(&mut all, v);
    }
    if fields.is_empty() {
        all = zero(0, 0, 0, 0, 0, 0);
    }
    // Recompute the readable totals the sum added up as strings.
    let get = |p: &[&str]| p.iter().fold(&all, |v, k| &v[*k]).as_u64().unwrap_or(0);
    let all = zero(
        get(&["inverted_index", "total_in_bytes"]),
        get(&["stored_fields_in_bytes"]),
        get(&["doc_values_in_bytes"]),
        get(&["points_in_bytes"]),
        get(&["norms_in_bytes"]),
        get(&["knn_vectors_in_bytes"]),
    );
    let store: u64 = primaries(i).iter().map(|s| shard_store(i, *s)).sum();
    json!({"store_size": node::human_size(store), "store_size_in_bytes": store,
           "all_fields": all, "fields": fields})
}

// --- the observer --------------------------------------------------------

/// The fields a search body sorts or aggregates on: (field, by an
/// aggregation).
fn loaded_fields(body: &Value) -> Vec<(String, bool)> {
    let mut out = Vec::new();
    let sorts: Vec<Value> = match body.get("sort") {
        Some(Value::Array(a)) => a.clone(),
        Some(v) => vec![v.clone()],
        None => vec![],
    };
    for s in sorts {
        match s {
            Value::String(f) => out.push((f, false)),
            Value::Object(o) => out.extend(o.keys().map(|k| (k.clone(), false))),
            _ => {}
        }
    }
    fn aggs(v: &Value, out: &mut Vec<(String, bool)>) {
        let Some(m) = v.as_object() else { return };
        for def in m.values() {
            let Some(d) = def.as_object() else { continue };
            for (kind, body) in d {
                if matches!(
                    kind.as_str(),
                    "terms"
                        | "significant_terms"
                        | "rare_terms"
                        | "cardinality"
                        | "diversified_sampler"
                ) && let Some(f) = body.get("field").and_then(Value::as_str)
                {
                    out.push((f.to_string(), true));
                }
                if kind == "aggs" || kind == "aggregations" {
                    aggs(body, out);
                }
            }
        }
    }
    for k in ["aggs", "aggregations"] {
        if let Some(a) = body.get(k) {
            aggs(a, &mut out);
        }
    }
    out
}

/// Fields a query touches, with the structures it reads.
fn query_usage(q: &Value, scored: bool, out: &mut Vec<(String, &'static str)>) {
    let Some(m) = q.as_object() else { return };
    for (kind, body) in m {
        match kind.as_str() {
            "bool" => {
                for (clause, sc) in
                    [("must", scored), ("should", scored), ("filter", false), ("must_not", false)]
                {
                    match body.get(clause) {
                        Some(Value::Array(a)) => a.iter().for_each(|x| query_usage(x, sc, out)),
                        Some(x) => query_usage(x, sc, out),
                        None => {}
                    }
                }
            }
            "constant_score" => {
                if let Some(f) = body.get("filter") {
                    query_usage(f, false, out);
                }
            }
            "match_phrase" | "match_phrase_prefix" => {
                if let Some((f, _)) = body.as_object().and_then(|o| o.iter().next()) {
                    for k in ["terms", "postings", "proximity", "term_frequencies", "positions"] {
                        out.push((f.clone(), k));
                    }
                    if scored {
                        out.push((f.clone(), "norms"));
                    }
                }
            }
            "match" | "term" | "terms" | "prefix" | "wildcard" | "fuzzy" | "regexp" => {
                if let Some((f, _)) = body.as_object().and_then(|o| o.iter().next()) {
                    out.push((f.clone(), "terms"));
                    out.push((f.clone(), "postings"));
                    if scored {
                        out.push((f.clone(), "term_frequencies"));
                        out.push((f.clone(), "norms"));
                    }
                }
            }
            "range" => {
                if let Some((f, _)) = body.as_object().and_then(|o| o.iter().next()) {
                    out.push((f.clone(), "points"));
                    out.push((f.clone(), "doc_values"));
                }
            }
            "exists" => {
                if let Some(f) = body.get("field").and_then(Value::as_str) {
                    out.push((f.to_string(), "doc_values"));
                }
            }
            _ => {}
        }
    }
}

impl Engine {
    /// Updates the counters from a request and the response it got.
    pub(super) fn observe(
        &self,
        method: &str,
        path: &str,
        query: &str,
        body: &[u8],
        status: u16,
        resp: &Value,
    ) {
        let segs: Vec<String> = path
            .trim_matches('/')
            .split('/')
            .filter(|s| !s.is_empty())
            .map(percent_decode_segment)
            .collect();
        let segs: Vec<&str> = segs.iter().map(String::as_str).collect();
        let q = query_params(query);
        let refresh =
            q.get("refresh").is_some_and(|v| v.is_empty() || v == "true" || v == "wait_for");
        let write = matches!(method, "PUT" | "POST");
        let doc_op = segs.iter().any(|x| {
            matches!(
                *x,
                "_doc"
                    | "_create"
                    | "_update"
                    | "_bulk"
                    | "_search"
                    | "_count"
                    | "_msearch"
                    | "_mget"
                    | "_refresh"
                    | "_flush"
                    | "_forcemerge"
                    | "_termvectors"
                    | "_explain"
                    | "_validate"
                    | "_field_caps"
                    | "_analyze"
                    | "_pit"
                    | "_cat"
                    | "_nodes"
                    | "_tasks"
                    | "_capabilities"
                    | "_health_report"
            )
        });
        if status < 300 && !doc_op && matches!(method, "PUT" | "POST" | "DELETE") {
            super::cluster::bump_state_version();
        }
        let mut s = self.0.lock().unwrap();
        let s = &mut *s;
        match segs.as_slice() {
            [_, "_doc" | "_create", _] | [_, "_doc"] if write => {
                note_write(s, resp, status, segs[0], body.len() as u64);
                if refresh {
                    note_refresh(s, resp["_index"].as_str().unwrap_or(""), true);
                }
            }
            [_, "_doc", _] if method == "DELETE" => {
                if let Some(idx) = resp["_index"].as_str().map(str::to_string)
                    && let Some(i) = s.indices.get_mut(&idx)
                {
                    let shard = doc_shard(i, segs[2]);
                    let c = i.counters.shard(shard);
                    c.delete_total += 1;
                    c.translog_ops += 1;
                    c.translog_bytes += 60;
                    i.counters.last_write = Some(Instant::now());
                    if refresh {
                        note_refresh(s, &idx, true);
                    }
                }
            }
            [idx, "_doc" | "_source", id] if matches!(method, "GET" | "HEAD") => {
                let target = resp["_index"].as_str().map(str::to_string).or_else(|| {
                    resolve(s, idx, &q, &Resolve::LENIENT_OPEN)
                        .ok()
                        .and_then(|v| v.into_iter().next())
                });
                if let Some(t) = target {
                    note_get(s, &t, id, status == 200);
                }
            }
            [idx, "_update", id] if method == "POST" => {
                let target = resp["_index"].as_str().map(str::to_string).or_else(|| {
                    resolve(s, idx, &q, &Resolve::LENIENT_OPEN)
                        .ok()
                        .and_then(|v| v.into_iter().next())
                });
                if let Some(t) = target {
                    note_get(s, &t, id, resp["result"] != json!("created") && status < 300);
                    if resp["result"] == json!("noop") {
                        if let Some(i) = s.indices.get_mut(&t) {
                            let shard = doc_shard(i, id);
                            i.counters.shard(shard).noop_update_total += 1;
                        }
                    } else {
                        note_write(s, resp, status, &t, body.len() as u64);
                    }
                    if refresh {
                        note_refresh(s, &t, true);
                    }
                }
            }
            ["_bulk"] | [_, "_bulk"] => {
                let mut touched: BTreeSet<(String, u64)> = BTreeSet::new();
                for item in resp["items"].as_array().into_iter().flatten() {
                    let Some((op, r)) = item.as_object().and_then(|o| o.iter().next()) else {
                        continue;
                    };
                    let Some(idx) = r["_index"].as_str() else { continue };
                    let id = r["_id"].as_str().unwrap_or("");
                    let Some(i) = s.indices.get_mut(idx) else { continue };
                    let shard = doc_shard(i, id);
                    touched.insert((idx.to_string(), shard));
                    let c = i.counters.shard(shard);
                    let ok = r["status"].as_u64().unwrap_or(500) < 300;
                    match (op.as_str(), r["result"].as_str()) {
                        (_, Some("noop")) => c.noop_update_total += 1,
                        ("delete", _) if ok => c.delete_total += 1,
                        (_, Some("created" | "updated")) => c.index_total += 1,
                        _ if !ok && r["status"].as_u64() == Some(400) => c.index_failed += 1,
                        _ => {}
                    }
                    if ok {
                        c.translog_ops += 1;
                        c.translog_bytes += 80;
                        i.counters.last_write = Some(Instant::now());
                    }
                }
                let size = body.len() as u64 / touched.len().max(1) as u64;
                for (idx, shard) in &touched {
                    if let Some(i) = s.indices.get_mut(idx) {
                        let c = i.counters.shard(*shard);
                        c.bulk_ops += 1;
                        c.bulk_bytes += size;
                    }
                }
                if refresh {
                    let idx: BTreeSet<String> = touched.into_iter().map(|t| t.0).collect();
                    for n in idx {
                        note_refresh(s, &n, true);
                    }
                }
            }
            ["_mget"] | [_, "_mget"] => {
                for d in resp["docs"].as_array().into_iter().flatten() {
                    if let (Some(idx), Some(id)) = (d["_index"].as_str(), d["_id"].as_str())
                        && d.get("error").is_none()
                    {
                        let found = d["found"].as_bool() == Some(true);
                        note_get(s, &idx.to_string(), id, found);
                    }
                }
            }
            ["_search" | "_count"] | [_, "_search" | "_count"] if status < 300 => {
                let pattern = if segs.len() == 2 { segs[0] } else { "_all" };
                let body = parse_json(body).unwrap_or_else(|| json!({}));
                note_search(s, pattern, &q, &body, resp, segs.last() == Some(&"_search"));
            }
            ["_msearch"] | [_, "_msearch"] if status < 300 => {
                let default = if segs.len() == 2 { segs[0] } else { "_all" };
                let text = String::from_utf8_lossy(body);
                let lines: Vec<&str> = text.lines().filter(|l| !l.trim().is_empty()).collect();
                let responses = resp["responses"].as_array().cloned().unwrap_or_default();
                for (n, pair) in lines.chunks(2).enumerate() {
                    let header = parse_json(pair[0].as_bytes()).unwrap_or_else(|| json!({}));
                    let body = pair
                        .get(1)
                        .and_then(|b| parse_json(b.as_bytes()))
                        .unwrap_or_else(|| json!({}));
                    let pattern = match &header["index"] {
                        Value::String(x) => x.clone(),
                        Value::Array(a) => {
                            a.iter().filter_map(Value::as_str).collect::<Vec<_>>().join(",")
                        }
                        _ => default.to_string(),
                    };
                    let r = responses.get(n).cloned().unwrap_or_default();
                    if r.get("error").is_none() {
                        note_search(s, &pattern, &q, &body, &r, true);
                    }
                }
            }
            ["_refresh"] | [_, "_refresh"] if status < 300 => {
                let expr = if segs.len() == 2 { segs[0] } else { "_all" };
                for n in resolve(s, expr, &q, &Resolve::LENIENT_OPEN).unwrap_or_default() {
                    note_refresh(s, &n, true);
                }
            }
            ["_flush"] | [_, "_flush"] if status < 300 => {
                let expr = if segs.len() == 2 { segs[0] } else { "_all" };
                for n in resolve(s, expr, &q, &Resolve::LENIENT_OPEN).unwrap_or_default() {
                    if let Some(i) = s.indices.get_mut(&n) {
                        for shard in primaries(i) {
                            let has_docs = i.docs.keys().any(|id| doc_shard(i, id) == shard);
                            let c = i.counters.shard(shard);
                            c.flushed |= has_docs;
                            c.flush_total += 1;
                            c.translog_ops = 0;
                            c.translog_bytes = 0;
                        }
                    }
                }
            }
            ["_forcemerge"] | [_, "_forcemerge"] if status < 300 => {
                let expr = if segs.len() == 2 { segs[0] } else { "_all" };
                for n in resolve(s, expr, &q, &Resolve::LENIENT_OPEN).unwrap_or_default() {
                    if let Some(i) = s.indices.get_mut(&n) {
                        for shard in primaries(i) {
                            i.counters.shard(shard).merge_total += 1;
                        }
                    }
                }
            }
            [expr, "_close" | "_open"] if status < 300 => {
                let opts = Resolve { expand: "all", lenient: true, forbid_closed: false };
                for n in resolve(s, expr, &q, &opts).unwrap_or_default() {
                    if let Some(i) = s.indices.get_mut(&n) {
                        i.counters.existing_store = true;
                        for c in i.counters.shards.values_mut() {
                            c.translog_ops = 0;
                            c.translog_bytes = 0;
                        }
                    }
                }
            }
            [idx, "_termvectors", ..] if status < 300 => {
                let fields: Vec<String> = q
                    .get("fields")
                    .map(|f| f.split(',').map(str::to_string).collect())
                    .unwrap_or_default();
                let target = resp["_index"].as_str().unwrap_or(idx).to_string();
                if let Some(i) = s.indices.get_mut(&target) {
                    for f in fields {
                        *i.counters
                            .field_usage
                            .entry(f)
                            .or_default()
                            .entry("term_vectors")
                            .or_default() += 1;
                    }
                }
            }
            _ => {}
        }
    }
}

/// An index or update result: one more indexed document (or a failure).
fn note_write(s: &mut State, resp: &Value, status: u16, path_index: &str, bytes: u64) {
    let idx = resp["_index"].as_str().unwrap_or(path_index).to_string();
    let Some(i) = s.indices.get_mut(&idx) else { return };
    let id = resp["_id"].as_str().unwrap_or("");
    let shard = doc_shard(i, id);
    let c = i.counters.shard(shard);
    match resp["result"].as_str() {
        Some("created" | "updated") => {
            c.index_total += 1;
            c.translog_ops += 1;
            c.translog_bytes += bytes + 60;
            c.bulk_ops += 1;
            c.bulk_bytes += bytes;
            i.counters.last_write = Some(Instant::now());
        }
        _ if status == 400 => c.index_failed += 1,
        _ => {}
    }
}

fn note_get(s: &mut State, idx: &String, id: &str, found: bool) {
    let Some(i) = s.indices.get_mut(idx) else { return };
    let shard = doc_shard(i, id);
    let c = i.counters.shard(shard);
    c.get_total += 1;
    if found {
        c.get_exists += 1;
    } else {
        c.get_missing += 1;
    }
}

fn note_refresh(s: &mut State, idx: &str, external: bool) {
    let Some(i) = s.indices.get_mut(idx) else { return };
    for shard in primaries(i) {
        let c = i.counters.shard(shard);
        c.refresh_total += 1;
        if external {
            c.external_refresh_total += 1;
        }
    }
}

/// One search over `pattern`: a query (and, with hits, a fetch) per
/// shard, the `stats` groups it named, suggesters, and the field data its
/// sorts and aggregations load.
fn note_search(
    s: &mut State,
    pattern: &str,
    q: &HashMap<String, String>,
    body: &Value,
    resp: &Value,
    is_search: bool,
) {
    let names = resolve(s, pattern, q, &Resolve::LENIENT_OPEN).unwrap_or_default();
    let groups: Vec<String> = match body.get("stats") {
        Some(Value::Array(a)) => a.iter().filter_map(Value::as_str).map(str::to_string).collect(),
        Some(Value::String(g)) => vec![g.clone()],
        _ => vec![],
    };
    let suggest = body.get("suggest").is_some();
    // A search that only suggests counts as a suggest, not a query.
    let suggest_only = suggest
        && ["query", "aggs", "aggregations", "knn", "post_filter", "sort"]
            .iter()
            .all(|k| body.get(k).is_none());
    let loaded = loaded_fields(body);
    let scored = body.get("sort").is_none();
    let mut usage = Vec::new();
    if let Some(query) = body.get("query") {
        query_usage(query, scored, &mut usage);
    }
    for f in &loaded {
        usage.push((f.0.clone(), "doc_values"));
    }
    if body.get("highlight").is_some()
        && let Some(query) = body.get("query")
    {
        let mut hl = Vec::new();
        query_usage(query, false, &mut hl);
        usage.extend(hl.into_iter().map(|(f, _)| (f, "offsets")));
    }
    let hits: Vec<(String, String)> = resp["hits"]["hits"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|h| Some((h["_index"].as_str()?.to_string(), h["_id"].as_str()?.to_string())))
        .collect();
    for n in names {
        let Some(i) = s.indices.get_mut(&n) else { continue };
        if !i.opened {
            continue;
        }
        let hit_shards: BTreeSet<u64> =
            hits.iter().filter(|(x, _)| *x == n).map(|(_, id)| doc_shard(i, id)).collect();
        for shard in primaries(i) {
            let fetched = u64::from(is_search && hit_shards.contains(&shard));
            let c = i.counters.shard(shard);
            if suggest_only {
                c.suggest_total += 1;
                continue;
            }
            c.query_total += 1;
            c.fetch_total += fetched;
            if suggest {
                c.suggest_total += 1;
            }
            for g in &groups {
                let e = c.groups.entry(g.clone()).or_default();
                e.0 += 1;
                e.1 += fetched;
            }
        }
        i.counters.last_search = Some(Instant::now());
        for (f, by_agg) in &loaded {
            let (path, ty) = search::resolve_field(&i.mappings, f);
            let fielddata = match ty.as_deref() {
                Some("text") => {
                    let mut leaves = Vec::new();
                    mapped_leaves(&i.mappings, "", &mut leaves);
                    leaves
                        .iter()
                        .any(|(name, d)| name == &path && d.get("fielddata") == Some(&json!(true)))
                }
                Some("keyword") => *by_agg,
                _ => false,
            };
            if fielddata {
                let e = i.counters.fielddata.entry(path).or_insert(false);
                *e |= *by_agg;
            }
        }
        if !hits.is_empty() || !usage.is_empty() {
            let mut used: Vec<(String, &'static str)> = usage.clone();
            if hit_shards.is_empty() {
                // Nothing fetched from this index.
            } else {
                used.push(("_id".into(), "stored_fields"));
                used.push(("_source".into(), "stored_fields"));
            }
            for (f, kind) in used {
                if f.contains('*') {
                    continue;
                }
                *i.counters.field_usage.entry(f).or_default().entry(kind).or_default() += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stats_are_added() {
        let mut a = json!({"docs": {"count": 1}, "segments": {"max_unsafe_auto_id_timestamp": -1}});
        add_stats(
            &mut a,
            &json!({"docs": {"count": 2}, "segments": {"max_unsafe_auto_id_timestamp": 5}}),
        );
        assert_eq!(a["docs"]["count"], json!(3));
        assert_eq!(a["segments"]["max_unsafe_auto_id_timestamp"], json!(5));
    }

    #[test]
    fn unknown_metric_is_rejected() {
        let e =
            StatsRequest::parse(Some("fieldata"), &HashMap::new(), "/_stats/fieldata", "metric")
                .err()
                .unwrap();
        assert_eq!(e.0, 400);
        assert_eq!(
            e.1["error"]["reason"],
            json!(
                "request [/_stats/fieldata] contains unrecognized metric: [fieldata] -> did you mean [fielddata]?"
            )
        );
    }

    #[test]
    fn sorted_and_aggregated_fields() {
        let body = json!({"sort": ["a", {"b": "desc"}], "aggs": {"x": {"terms": {"field": "c"}}}});
        assert_eq!(
            loaded_fields(&body),
            vec![("a".to_string(), false), ("b".to_string(), false), ("c".to_string(), true)]
        );
    }
}
