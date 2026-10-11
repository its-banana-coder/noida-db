//! Lookup runtime fields (`runtime_mappings: {"f": {"type": "lookup",
//! "target_index", "input_field", "target_field", "fetch_fields"}}`):
//! fetched per hit from the documents of another index whose
//! `target_field` equals the hit's `input_field`. They can only be
//! fetched -- not queried or aggregated.

use serde_json::{Value, json};

use super::limits::{query_shard_failure, shard_iae};

/// The request's lookup fields: (name, definition).
pub fn fields(req: &Value) -> Vec<(String, Value)> {
    req.get("runtime_mappings")
        .and_then(Value::as_object)
        .into_iter()
        .flatten()
        .filter(|(_, d)| d.get("type").and_then(Value::as_str) == Some("lookup"))
        .map(|(n, d)| (n.clone(), d.clone()))
        .collect()
}

/// The error for a query or aggregation on a lookup field of `index`.
pub fn check(req: &Value, index: &str, settings: &Value) -> Result<(), (u16, Value)> {
    let names: Vec<String> = fields(req).into_iter().map(|(n, _)| n).collect();
    if names.is_empty() {
        return Ok(());
    }
    if let Some(q) = req.get("query")
        && let Some((name, kind)) = queried(q, &names)
    {
        let reason = if kind.starts_with("match") {
            format!("Field [{name}] of type [lookup] does not support {kind} queries")
        } else {
            format!("Cannot search on field [{name}] since it is a lookup field.")
        };
        return Err(query_shard_failure(index, settings, &reason));
    }
    if let Some(a) = req.get("aggs").or_else(|| req.get("aggregations"))
        && let Some(name) = aggregated(a, &names)
    {
        return Err(shard_iae(
            index,
            &format!("Fielddata is not supported on field [{name}] of type [lookup]"),
        ));
    }
    Ok(())
}

/// A (lookup field, query kind) that `q` searches on.
fn queried(q: &Value, names: &[String]) -> Option<(String, String)> {
    match q {
        Value::Object(m) => {
            for (kind, body) in m {
                if kind != "exists"
                    && let Some(n) =
                        body.as_object().and_then(|b| b.keys().find(|k| names.contains(k)))
                {
                    return Some((n.clone(), kind.clone()));
                }
                if let Some(found) = queried(body, names) {
                    return Some(found);
                }
            }
            None
        }
        Value::Array(a) => a.iter().find_map(|x| queried(x, names)),
        _ => None,
    }
}

fn aggregated(a: &Value, names: &[String]) -> Option<String> {
    match a {
        Value::Object(m) => m.iter().find_map(|(k, v)| match (k.as_str(), v) {
            ("field", Value::String(f)) if names.contains(f) => Some(f.clone()),
            _ => aggregated(v, names),
        }),
        Value::Array(x) => x.iter().find_map(|v| aggregated(v, names)),
        _ => None,
    }
}

/// The value(s) a lookup reads from the hit (`input_field`).
pub fn inputs(def: &Value, source: &Value) -> Vec<String> {
    let field = def.get("input_field").and_then(Value::as_str).unwrap_or("");
    super::search::raw_values(source, field)
        .into_iter()
        .map(|v| v.as_str().map_or(v.to_string(), str::to_string))
        .collect()
}

/// Whether a target document (`id`, `source`) is the one input `v`
/// names through `target_field`.
pub fn is_target(def: &Value, id: &str, source: &Value, v: &str) -> bool {
    match def.get("target_field").and_then(Value::as_str).unwrap_or("_id") {
        "_id" => id == v,
        f => super::search::raw_values(source, f)
            .into_iter()
            .any(|x| x.as_str().map_or(x.to_string(), str::to_string) == v),
    }
}

/// The `fetch_fields` request of a lookup.
pub fn fetch_spec(def: &Value) -> Value {
    def.get("fetch_fields").cloned().unwrap_or_else(|| json!([]))
}
