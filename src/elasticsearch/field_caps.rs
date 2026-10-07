//! `_field_caps`: for each requested field, its type(s) across the
//! searched indices and whether it is searchable and aggregatable, merged
//! the way Elasticsearch merges them (`indices` only when a field has more
//! than one type, `non_searchable_indices` / `non_aggregatable_indices`
//! only when the indices disagree).

use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, HashMap};

use super::search::raw_values;
use super::templates::glob;

/// One field as one index maps it.
struct Cap {
    ty: String,
    searchable: bool,
    aggregatable: bool,
    metadata: bool,
    meta: Map<String, Value>,
    /// A multi-field (`text.keyword`), or a field inside a nested object.
    multifield: bool,
    in_nested: bool,
}

impl Cap {
    fn new(ty: &str, searchable: bool, aggregatable: bool, metadata: bool) -> Self {
        Cap {
            ty: ty.into(),
            searchable,
            aggregatable,
            metadata,
            meta: Map::new(),
            multifield: false,
            in_nested: false,
        }
    }
}

/// The metadata fields every index has: (name, type, searchable,
/// aggregatable).
const METADATA: &[(&str, &str, bool, bool)] = &[
    ("_id", "_id", true, false),
    ("_index", "_index", true, true),
    ("_routing", "_routing", true, false),
    ("_source", "_source", false, false),
    ("_seq_no", "_seq_no", true, true),
    ("_version", "_version", false, true),
    ("_ignored", "_ignored", true, true),
    ("_ignored_source", "_ignored_source", false, false),
    ("_field_names", "_field_names", true, false),
    ("_feature", "_feature", false, false),
    ("_doc_count", "integer", false, false),
    ("_tier", "keyword", true, true),
    ("_data_stream_timestamp", "_data_stream_timestamp", false, false),
];

struct Walk {
    multifield: bool,
    in_nested: bool,
}

fn leaf_caps(
    props: &Value,
    prefix: &str,
    w: &Walk,
    out: &mut Vec<(String, Cap)>,
    has_nested: &mut bool,
) {
    let Some(m) = props.as_object() else { return };
    for (k, node) in m {
        let name = if prefix.is_empty() { k.clone() } else { format!("{prefix}.{k}") };
        let ty = node.get("type").and_then(Value::as_str).unwrap_or("object");
        let flag = |key: &str| node.get(key).and_then(Value::as_bool);
        let meta = node.get("meta").and_then(Value::as_object).cloned().unwrap_or_default();
        match ty {
            "object" | "nested" => {
                *has_nested |= ty == "nested";
                let mut cap = Cap::new(ty, false, false, false);
                cap.meta = meta;
                cap.in_nested = w.in_nested;
                out.push((name.clone(), cap));
                if let Some(p) = node.get("properties") {
                    let inner =
                        Walk { multifield: false, in_nested: w.in_nested || ty == "nested" };
                    leaf_caps(p, &name, &inner, out, has_nested);
                }
            }
            _ => {
                let indexed = flag("index").unwrap_or(true);
                let doc_values = flag("doc_values").unwrap_or(true);
                let (searchable, aggregatable) = match ty {
                    "text" | "match_only_text" | "search_as_you_type" | "annotated_text" => {
                        (indexed, flag("fielddata").unwrap_or(false))
                    }
                    "binary" => (false, doc_values && flag("doc_values").unwrap_or(false)),
                    "completion" => (indexed, false),
                    "dense_vector" | "sparse_vector" | "rank_feature" | "rank_features" => {
                        (indexed && ty == "dense_vector", false)
                    }
                    "flattened" | "keyword" | "constant_keyword" | "wildcard" | "ip"
                    | "boolean" | "date" | "date_nanos" | "version" | "geo_point" | "long"
                    | "integer" | "short" | "byte" | "double" | "float" | "half_float"
                    | "scaled_float" | "unsigned_long" => {
                        // Doc values keep a non-indexed field searchable.
                        (indexed || doc_values, doc_values)
                    }
                    _ => (indexed, doc_values),
                };
                let mut cap = Cap::new(ty, searchable, aggregatable, false);
                cap.meta = meta;
                cap.multifield = w.multifield;
                cap.in_nested = w.in_nested;
                out.push((name.clone(), cap));
                if let Some(subs @ Value::Object(_)) = node.get("fields") {
                    // Multi-fields: `name.sub`.
                    let inner = Walk { multifield: true, in_nested: w.in_nested };
                    leaf_caps(subs, &name, &inner, out, has_nested);
                }
            }
        }
    }
}

fn all_caps(mappings: &Value) -> Vec<(String, Cap)> {
    let mut out = Vec::new();
    let mut has_nested = false;
    if let Some(p) = mappings.get("properties") {
        leaf_caps(p, "", &Walk { multifield: false, in_nested: false }, &mut out, &mut has_nested);
    }
    for (name, ty, s, a) in METADATA {
        out.push((name.to_string(), Cap::new(ty, *s, *a, true)));
    }
    if has_nested {
        out.push(("_nested_path".into(), Cap::new("_nested_path", true, false, true)));
    }
    out
}

/// The `_field_caps` response for `indices` (name, mappings).
/// The `filters` values Elasticsearch accepts.
pub fn check_filters(q: &HashMap<String, String>) -> Result<(), String> {
    for f in q.get("filters").map(|f| f.split(',').collect::<Vec<_>>()).unwrap_or_default() {
        let f = f.trim();
        if !matches!(f, "+metadata" | "-metadata" | "-nested" | "-multifield" | "-parent" | "") {
            return Err(format!("Unknown field caps filter [{f}]"));
        }
    }
    Ok(())
}

pub fn field_caps(
    indices: &[(String, Value, Vec<Value>)],
    q: &HashMap<String, String>,
    body: &Value,
) -> Value {
    let list = |v: Option<&Value>| -> Vec<String> {
        match v {
            Some(Value::String(s)) => s.split(',').map(|x| x.trim().to_string()).collect(),
            Some(Value::Array(a)) => {
                a.iter().filter_map(Value::as_str).map(str::to_string).collect()
            }
            _ => Vec::new(),
        }
    };
    let qv = |k: &str| q.get(k).map(|v| json!(v));
    let mut patterns = list(qv("fields").as_ref());
    patterns.extend(list(body.get("fields")));
    let filters = list(qv("filters").as_ref());
    let types = list(qv("types").as_ref());
    let include_unmapped = q.get("include_unmapped").is_some_and(|v| v == "true");
    let wanted = |name: &str| {
        let mut hit = false;
        for p in &patterns {
            if let Some(ex) = p.strip_prefix('-') {
                if glob(ex, name) {
                    hit = false;
                }
            } else if glob(p, name) {
                hit = true;
            }
        }
        hit
    };
    // field -> type -> [(index, cap)]
    let mut fields: BTreeMap<String, BTreeMap<String, Vec<(String, Cap)>>> = BTreeMap::new();
    let include_empty = q.get("include_empty_fields").is_none_or(|v| v != "false");
    for (index, mappings, sources) in indices {
        let caps = all_caps(mappings);
        // `include_empty_fields=false`: only fields some document has.
        let has_value = |name: &str, cap: &Cap| {
            if include_empty || cap.metadata {
                return true;
            }
            let path =
                if cap.multifield { name.rsplit_once('.').map_or(name, |(p, _)| p) } else { name };
            sources.iter().any(|src| !raw_values(src, path).into_iter().all(Value::is_null))
        };
        let names: Vec<&String> = caps.iter().map(|(n, _)| n).collect();
        let mut picked: Vec<String> = Vec::new();
        // Parent objects of a field that passes the `types` filter come
        // along whatever their own type.
        let mut parents: Vec<String> = Vec::new();
        for (name, cap) in &caps {
            if wanted(name) {
                picked.push(name.clone());
                if !types.is_empty() && !types.contains(&cap.ty) && !parents.contains(name) {
                    continue;
                }
                // A matched sub-field brings its parent objects along.
                if !filters.iter().any(|f| f == "-parent") {
                    let mut p = name.as_str();
                    while let Some((parent, _)) = p.rsplit_once('.') {
                        if names.iter().any(|n| n.as_str() == parent)
                            && caps.iter().any(|(n, c)| {
                                n == parent && matches!(c.ty.as_str(), "object" | "nested")
                            })
                        {
                            picked.push(parent.to_string());
                            parents.push(parent.to_string());
                        }
                        p = parent;
                    }
                }
            }
        }
        for (name, cap) in caps {
            if !picked.contains(&name) || !has_value(&name, &cap) {
                continue;
            }
            if filters.iter().any(|f| f == "-metadata") && cap.metadata {
                continue;
            }
            if filters.iter().any(|f| f == "+metadata") && !cap.metadata {
                continue;
            }
            if filters.iter().any(|f| f == "-parent")
                && matches!(cap.ty.as_str(), "object" | "nested")
            {
                continue;
            }
            if filters.iter().any(|f| f == "-nested") && (cap.in_nested || cap.ty == "nested") {
                continue;
            }
            if filters.iter().any(|f| f == "-multifield") && cap.multifield {
                continue;
            }
            if !types.is_empty() && !types.contains(&cap.ty) {
                continue;
            }
            fields
                .entry(name)
                .or_default()
                .entry(cap.ty.clone())
                .or_default()
                .push((index.clone(), cap));
        }
    }
    if include_unmapped {
        for by_type in fields.values_mut() {
            let mapped: Vec<String> =
                by_type.values().flat_map(|v| v.iter().map(|(i, _)| i.clone())).collect();
            for (index, _, _) in indices {
                if !mapped.contains(index) {
                    by_type
                        .entry("unmapped".into())
                        .or_default()
                        .push((index.clone(), Cap::new("unmapped", false, false, false)));
                }
            }
        }
    }
    let mut out = Map::new();
    for (name, by_type) in fields {
        let multi = by_type.len() > 1;
        let mut types_out = Map::new();
        for (ty, entries) in by_type {
            let searchable = entries.iter().all(|(_, c)| c.searchable);
            let aggregatable = entries.iter().all(|(_, c)| c.aggregatable);
            let mut e = json!({
                "type": ty,
                "metadata_field": entries.iter().any(|(_, c)| c.metadata),
                "searchable": searchable,
                "aggregatable": aggregatable,
            });
            let mut idx: Vec<String> = entries.iter().map(|(i, _)| i.clone()).collect();
            idx.sort();
            if multi {
                e["indices"] = json!(idx);
            }
            if !searchable && entries.iter().any(|(_, c)| c.searchable) {
                let mut l: Vec<&String> =
                    entries.iter().filter(|(_, c)| !c.searchable).map(|(i, _)| i).collect();
                l.sort();
                e["non_searchable_indices"] = json!(l);
            }
            if !aggregatable && entries.iter().any(|(_, c)| c.aggregatable) {
                let mut l: Vec<&String> =
                    entries.iter().filter(|(_, c)| !c.aggregatable).map(|(i, _)| i).collect();
                l.sort();
                e["non_aggregatable_indices"] = json!(l);
            }
            let mut meta: BTreeMap<String, Vec<Value>> = BTreeMap::new();
            for (_, c) in &entries {
                for (k, v) in &c.meta {
                    let slot = meta.entry(k.clone()).or_default();
                    if !slot.contains(v) {
                        slot.push(v.clone());
                    }
                }
            }
            if !meta.is_empty() {
                for vals in meta.values_mut() {
                    vals.sort_by_key(|v| v.to_string());
                }
                e["meta"] = json!(meta);
            }
            types_out.insert(ty, e);
        }
        out.insert(name, Value::Object(types_out));
    }
    let mut names: Vec<&String> = indices.iter().map(|(n, _, _)| n).collect();
    names.sort();
    json!({"indices": names, "fields": out})
}
