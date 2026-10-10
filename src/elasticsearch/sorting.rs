//! `sort`: per-hit sort values the way Elasticsearch reports them (`"sort":
//! [40]`, dates as epoch millis, keywords as strings), missing values last,
//! multi-valued fields by their min (asc) or max (desc), and `search_after`
//! on top of the same keys.

use serde_json::{Value, json};
use std::cmp::Ordering;

use super::dates;
use super::search::{CommittedDoc, EsError, raw_values, resolve_field};

#[derive(Clone, Debug)]
enum Key {
    Score,
    Doc,
    Field {
        path: String,
        ty: Option<String>,
    },
    /// A time-series document's `_tsid` (ordered by its bytes).
    Tsid,
    /// `_geo_distance`: from `origin` (lat, lon), in meters per `unit`.
    Geo {
        path: String,
        origin: (f64, f64),
        unit_m: f64,
    },
}

#[derive(Clone, Debug)]
pub struct SortSpec {
    key: Key,
    desc: bool,
    /// `min`/`max`/`avg`/`sum`/`median` for multi-valued fields.
    mode: Option<String>,
    /// `_first` / `_last` / a literal value.
    missing: Option<Value>,
}

impl SortSpec {
    pub fn is_score(&self) -> bool {
        matches!(self.key, Key::Score)
    }
}

fn is_long(ty: &str) -> bool {
    matches!(ty, "long" | "integer" | "short" | "byte" | "unsigned_long" | "date" | "date_nanos")
}

fn is_float(ty: &str) -> bool {
    matches!(ty, "double" | "float" | "half_float" | "scaled_float")
}

/// Parses a request's `sort` (a field name, `{field: order}`,
/// `{field: {order, mode, missing, unmapped_type}}`, or an array of them).
/// `typed` says whether `mappings` is a real index mapping (a search across
/// several indices merges theirs) — only then is an unmapped field an
/// error.
pub fn parse(spec: &Value, mappings: &Value, typed: bool) -> Result<Vec<SortSpec>, EsError> {
    let items: Vec<Value> = match spec {
        Value::Array(a) => a.clone(),
        other => vec![other.clone()],
    };
    let mut out = Vec::new();
    for item in items {
        let (field, opts) = match &item {
            Value::String(f) => (f.clone(), Value::Null),
            Value::Object(o) if o.len() == 1 => {
                let (k, v) = o.iter().next().unwrap();
                (k.clone(), v.clone())
            }
            _ => return Err(EsError::parsing("[sort] must be a field name or an object")),
        };
        let order = match &opts {
            Value::String(s) => Some(s.clone()),
            Value::Object(o) => o.get("order").and_then(Value::as_str).map(str::to_string),
            _ => None,
        };
        if let Some(o) = &order
            && o != "asc"
            && o != "desc"
        {
            return Err(EsError::new(
                400,
                "x_content_parse_exception",
                &format!("[field_sort] failed to parse field [order]: unknown order [{o}]"),
            ));
        }
        let key = match field.as_str() {
            "_geo_distance" => {
                let o = opts.as_object().cloned().unwrap_or_default();
                let skip = ["order", "unit", "mode", "distance_type", "ignore_unmapped", "nested"];
                let (path, point) = o
                    .iter()
                    .find(|(k, _)| !skip.contains(&k.as_str()))
                    .ok_or_else(|| EsError::parsing("[_geo_distance] requires a field"))?;
                // One point, or the first of several origins.
                let point = match point {
                    Value::Array(a) if !a.iter().all(Value::is_number) => {
                        a.first().cloned().unwrap_or_default()
                    }
                    p => p.clone(),
                };
                let origin = super::queries::parse_point(&point)
                    .ok_or_else(|| EsError::parsing("[_geo_distance] failed to parse point"))?;
                let unit = o.get("unit").and_then(Value::as_str).unwrap_or("m");
                let unit_m = super::queries::unit_meters(unit)
                    .ok_or_else(|| EsError::parsing(&format!("No distance unit match [{unit}]")))?;
                Key::Geo { path: path.clone(), origin, unit_m }
            }
            "_score" => Key::Score,
            "_doc" | "_shard_doc" => Key::Doc,
            "_tsid" => Key::Tsid,
            f => {
                let (path, ty) = resolve_field(mappings, f);
                let unmapped = opts.get("unmapped_type").and_then(Value::as_str);
                let ty = ty.or_else(|| unmapped.map(str::to_string));
                if ty.is_none() && typed && unmapped.is_none() {
                    return Err(EsError::shard_failure(
                        "query_shard_exception",
                        &format!("No mapping found for [{f}] in order to sort on"),
                    ));
                }
                if ty.as_deref() == Some("dense_vector") {
                    return Err(EsError::shard_failure(
                        "illegal_argument_exception",
                        &format!("Field [{f}] of type [dense_vector] doesn't support sort"),
                    ));
                }
                if ty.as_deref() == Some("text") {
                    return Err(EsError::shard_failure(
                        "illegal_argument_exception",
                        &format!(
                            "Text fields are not optimised for operations that require \
                             per-document field data like aggregations and sorting, so these \
                             operations are disabled by default. Please use a keyword field \
                             instead. Alternatively, set fielddata=true on [{f}] in order to load \
                             field data by uninverting the inverted index. Note that this can use \
                             significant memory."
                        ),
                    ));
                }
                // An unmapped field sorted with `unmapped_type` has no values.
                let path = if unmapped.is_some() && resolve_field(mappings, f).1.is_none() {
                    String::from("\u{0}unmapped")
                } else {
                    path
                };
                Key::Field { path, ty }
            }
        };
        let desc = match order.as_deref() {
            Some(o) => o == "desc",
            None => matches!(key, Key::Score),
        };
        out.push(SortSpec {
            key,
            desc,
            mode: opts.get("mode").and_then(Value::as_str).map(str::to_string),
            missing: opts.get("missing").cloned(),
        });
    }
    Ok(out)
}

/// One field value as a sort key of the field's type.
fn typed_value(v: &Value, ty: Option<&str>) -> Option<Value> {
    match ty {
        // `date_nanos` sorts (and reports) nanoseconds.
        Some("date_nanos") => super::tsdb::date_nanos(v).map(|n| json!(n)),
        Some(t) if t == "date" => dates::value_millis(v, None).map(|m| json!(m)),
        Some(t) if is_long(t) => match v {
            Value::Number(n) => {
                n.as_i64().or_else(|| n.as_f64().map(|f| f as i64)).map(|n| json!(n))
            }
            Value::String(s) => s.parse::<f64>().ok().map(|f| json!(f as i64)),
            _ => None,
        },
        Some(t) if is_float(t) => match v {
            Value::Number(n) => n.as_f64().map(|f| json!(f)),
            Value::String(s) => s.parse::<f64>().ok().map(|f| json!(f)),
            _ => None,
        },
        Some("boolean") => match v {
            Value::Bool(b) => Some(json!(if *b { 1 } else { 0 })),
            Value::String(s) if s == "true" => Some(json!(1)),
            Value::String(s) if s == "false" => Some(json!(0)),
            _ => None,
        },
        _ => match v {
            Value::String(_) | Value::Number(_) => Some(v.clone()),
            Value::Bool(b) => Some(json!(b.to_string())),
            _ => None,
        },
    }
}

pub fn compare_values(a: &Value, b: &Value) -> Ordering {
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Greater,
        (_, Value::Null) => Ordering::Less,
        (Value::Number(x), Value::Number(y)) => match (x.as_i64(), y.as_i64()) {
            (Some(i), Some(j)) => i.cmp(&j),
            _ => x.as_f64().partial_cmp(&y.as_f64()).unwrap_or(Ordering::Equal),
        },
        (Value::String(x), Value::String(y)) => x.as_bytes().cmp(y.as_bytes()),
        (Value::Number(_), Value::String(_)) => Ordering::Less,
        (Value::String(_), Value::Number(_)) => Ordering::Greater,
        _ => Ordering::Equal,
    }
}

/// The value a missing field sorts with (and reports), Elasticsearch's
/// way: the type's max for `_last` ascending, min for descending; `null`
/// for keywords.
fn missing_value(spec: &SortSpec, ty: Option<&str>) -> Value {
    let last = match &spec.missing {
        None => true,
        Some(Value::String(s)) if s == "_last" => true,
        Some(Value::String(s)) if s == "_first" => false,
        Some(v) => return typed_value(v, ty).unwrap_or(Value::Null),
    };
    let high = last != spec.desc;
    match ty {
        Some(t) if is_long(t) || t == "boolean" => json!(if high { i64::MAX } else { i64::MIN }),
        Some(t) if is_float(t) => json!(if high { f64::MAX } else { -f64::MAX }),
        _ => Value::Null,
    }
}

/// The sort values of one hit.
pub fn keys(specs: &[SortSpec], doc: &CommittedDoc, doc_idx: usize, score: f32) -> Vec<Value> {
    specs
        .iter()
        .map(|s| match &s.key {
            Key::Score => json!(score),
            Key::Doc => json!(doc_idx),
            Key::Tsid => doc.tsid.as_ref().map_or(Value::Null, |t| json!(t)),
            Key::Geo { path, origin, unit_m } => {
                let mode_max = s.mode.as_deref() == Some("max") || (s.mode.is_none() && s.desc);
                match super::queries::sort_distance(doc, path, *origin, *unit_m, mode_max) {
                    Some(d) => json!(d),
                    None => Value::Null,
                }
            }
            Key::Field { path, ty } => {
                let ty = ty.as_deref();
                let mut vals: Vec<Value> = raw_values(&doc.source, path)
                    .into_iter()
                    .filter_map(|v| typed_value(v, ty))
                    .collect();
                if vals.is_empty() {
                    return missing_value(s, ty);
                }
                vals.sort_by(compare_values);
                let numeric = || vals.iter().filter_map(Value::as_f64).collect::<Vec<_>>();
                match s.mode.as_deref() {
                    Some("max") => vals.pop().unwrap(),
                    Some("min") => vals.swap_remove(0),
                    Some("sum") => json!(numeric().iter().sum::<f64>()),
                    Some("avg") => {
                        let n = numeric();
                        json!(n.iter().sum::<f64>() / n.len().max(1) as f64)
                    }
                    Some("median") => {
                        let n = numeric();
                        json!(n[n.len() / 2])
                    }
                    _ if s.desc => vals.pop().unwrap(),
                    _ => vals.swap_remove(0),
                }
            }
        })
        .collect()
}

/// Orders two hits' sort keys under `specs`. Missing keyword values
/// (`null`) sort last in either direction.
pub fn compare_keys(specs: &[SortSpec], a: &[Value], b: &[Value]) -> Ordering {
    for (i, s) in specs.iter().enumerate() {
        let (x, y) = (&a[i], &b[i]);
        let ord = match (x.is_null(), y.is_null()) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Greater,
            (false, true) => Ordering::Less,
            _ => {
                let o = match (&s.key, x, y) {
                    (Key::Tsid, Value::String(a), Value::String(b)) => {
                        super::tsdb::tsid_sort_key(a).cmp(&super::tsdb::tsid_sort_key(b))
                    }
                    _ => compare_values(x, y),
                };
                if s.desc { o.reverse() } else { o }
            }
        };
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

/// Converts a request's `search_after` values to comparable keys (dates
/// may be given as strings).
pub fn after_keys(specs: &[SortSpec], after: &[Value]) -> Result<Vec<Value>, EsError> {
    if after.len() != specs.len() {
        return Err(EsError::shard_failure(
            "illegal_argument_exception",
            &format!("search_after has {} value(s) but sort has {}.", after.len(), specs.len()),
        ));
    }
    Ok(specs
        .iter()
        .zip(after)
        .map(|(s, v)| match &s.key {
            Key::Field { ty, .. } if ty.as_deref() == Some("date_nanos") && v.is_number() => {
                v.clone()
            }
            Key::Field { ty, .. } => typed_value(v, ty.as_deref()).unwrap_or_else(|| v.clone()),
            _ => v.clone(),
        })
        .collect())
}
