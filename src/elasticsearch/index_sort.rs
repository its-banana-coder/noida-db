//! Index sorting (`index.sort.field`, `.order`, `.missing`, `.mode`): an
//! index whose segments keep documents in that order. Searches sorted by
//! `_doc` (and ties in any sort) see documents in index order, which this
//! engine applies to the whole searchable snapshot at each refresh, as
//! after a force merge.

use std::cmp::Ordering;

use serde_json::{Value, json};

use super::dates;
use super::search::{CommittedDoc, raw_values};

/// An `index.sort.*` setting as a list (a single value or an array).
fn list(settings: &Value, key: &str) -> Option<Vec<String>> {
    let v = settings.get("index")?.get("sort")?.get(key)?;
    Some(match v {
        Value::Array(a) => {
            a.iter().map(|x| x.as_str().map_or_else(|| x.to_string(), str::to_string)).collect()
        }
        Value::String(s) => s.split(',').map(|p| p.trim().to_string()).collect(),
        other => vec![other.to_string()],
    })
}

fn illegal(reason: &str) -> (u16, Value) {
    (
        400,
        json!({"error": {"root_cause": [{"type": "illegal_argument_exception", "reason": reason}],
                         "type": "illegal_argument_exception", "reason": reason}, "status": 400}),
    )
}

/// The mapping of the field at `path`, and the nested object it is under.
fn lookup<'a>(mappings: &'a Value, path: &str) -> (Option<&'a Value>, Option<String>) {
    let segs: Vec<&str> = path.split('.').collect();
    let mut props = mappings.get("properties");
    let mut nested = None;
    for (i, seg) in segs.iter().enumerate() {
        let Some(node) = props.and_then(|p| p.get(*seg)) else { return (None, nested) };
        if i + 1 == segs.len() {
            return (Some(node), nested);
        }
        if node.get("type").and_then(Value::as_str) == Some("nested") && nested.is_none() {
            nested = Some(segs[..=i].join("."));
        }
        if i + 2 == segs.len()
            && let Some(sub) = node.get("fields").and_then(|f| f.get(segs[i + 1]))
        {
            return (Some(sub), nested);
        }
        props = node.get("properties");
    }
    (None, nested)
}

/// The index sort settings Elasticsearch refuses when creating the index.
pub(crate) fn validate(settings: &Value, mappings: &Value) -> Result<(), (u16, Value)> {
    let Some(fields) = list(settings, "field") else { return Ok(()) };
    for key in ["order", "missing", "mode"] {
        if let Some(other) = list(settings, key)
            && other.len() != fields.len()
        {
            return Err(illegal(&format!(
                "index.sort.field:[{}] index.sort.{key}:[{}], size mismatch",
                fields.join(", "),
                other.join(", ")
            )));
        }
    }
    for f in &fields {
        let (def, nested) = lookup(mappings, f);
        if let Some(n) = nested {
            return Err(illegal(&format!(
                "cannot apply index sort to field [{f}] under nested object [{n}]"
            )));
        }
        let Some(def) = def else {
            return Err(illegal(&format!("unknown index sort field:[{f}]")));
        };
        let ty = def.get("type").and_then(Value::as_str).unwrap_or("object");
        if ty == "text" && def.get("fielddata").and_then(Value::as_bool) != Some(true) {
            let reason = format!("docvalues not found for index sort field:[{f}]");
            let cause = format!(
                "Fielddata is disabled on [{f}] in []. Text fields are not optimised for \
                 operations that require per-document field data like aggregations and \
                 sorting, so these operations are disabled by default. Please use a keyword \
                 field instead. Alternatively, set fielddata=true on [{f}] in order to load \
                 field data by uninverting the inverted index. Note that this can use \
                 significant memory."
            );
            let mut e = illegal(&reason);
            e.1["error"]["caused_by"] =
                json!({"type": "illegal_argument_exception", "reason": cause});
            return Err(e);
        }
    }
    Ok(())
}

/// One document's sort key for one field: its min (or max) value.
fn key(doc: &CommittedDoc, field: &str, ty: &str, max: bool) -> Option<Value> {
    let vals = raw_values(doc.full(), field);
    let typed: Vec<Value> = vals
        .into_iter()
        .filter_map(|v| match ty {
            "date" | "date_nanos" => dates::value_millis(v, None).map(|ms| json!(ms)),
            "boolean" => v.as_bool().map(|b| json!(b as i64)),
            _ => (!v.is_null()).then(|| v.clone()),
        })
        .collect();
    let pick = |a: &Value, b: &Value| compare(a, b);
    if max { typed.into_iter().max_by(pick) } else { typed.into_iter().min_by(pick) }
}

fn compare(a: &Value, b: &Value) -> Ordering {
    match (a.as_f64(), b.as_f64()) {
        (Some(x), Some(y)) => x.partial_cmp(&y).unwrap_or(Ordering::Equal),
        _ => match (a.as_str(), b.as_str()) {
            (Some(x), Some(y)) => x.cmp(y),
            _ => a.to_string().cmp(&b.to_string()),
        },
    }
}

/// Puts `docs` in the index's sort order (a stable sort: ties keep their
/// write order).
pub(crate) fn apply(settings: &Value, mappings: &Value, docs: &mut [CommittedDoc]) {
    let Some(fields) = list(settings, "field") else { return };
    let orders = list(settings, "order").unwrap_or_default();
    let missing = list(settings, "missing").unwrap_or_default();
    let modes = list(settings, "mode").unwrap_or_default();
    // (field, type, descending, missing first); a field's value is its
    // min, or (`mode: max`, the default when descending) its max.
    let specs: Vec<(String, String, bool, bool)> = fields
        .iter()
        .enumerate()
        .map(|(n, f)| {
            let desc = orders.get(n).is_some_and(|o| o == "desc");
            let first = missing.get(n).is_some_and(|m| m == "_first");
            let ty = lookup(mappings, f)
                .0
                .and_then(|d| d.get("type"))
                .and_then(Value::as_str)
                .unwrap_or("keyword")
                .to_string();
            (f.clone(), ty, desc, first)
        })
        .collect();
    let keyed: Vec<Vec<Option<Value>>> = docs
        .iter()
        .map(|d| {
            specs
                .iter()
                .enumerate()
                .map(|(n, (f, ty, desc, _))| {
                    key(d, f, ty, modes.get(n).map_or(*desc, |m| m == "max"))
                })
                .collect()
        })
        .collect();
    let mut idx: Vec<usize> = (0..docs.len()).collect();
    idx.sort_by(|&a, &b| {
        for (n, (_, _, desc, first)) in specs.iter().enumerate() {
            let o = match (&keyed[a][n], &keyed[b][n]) {
                (Some(x), Some(y)) => {
                    let o = compare(x, y);
                    if *desc { o.reverse() } else { o }
                }
                (None, None) => Ordering::Equal,
                (None, Some(_)) => {
                    if *first {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    }
                }
                (Some(_), None) => {
                    if *first {
                        Ordering::Greater
                    } else {
                        Ordering::Less
                    }
                }
            };
            if o != Ordering::Equal {
                return o;
            }
        }
        Ordering::Equal
    });
    let sorted: Vec<CommittedDoc> = idx.iter().map(|&i| docs[i].clone()).collect();
    docs.clone_from_slice(&sorted);
}
