//! The `fields`, `docvalue_fields` and `stored_fields` search options: a
//! hit's field values read from `_source` and formatted by the mapping
//! (dates in the mapping's or the requested format, numbers as the mapped
//! type, keywords as strings), the way Elasticsearch returns them.

use serde_json::{Map, Value, json};

use super::dates;
use super::search::{CommittedDoc, EsError, raw_values};

#[derive(Clone, Copy, PartialEq)]
pub enum Kind {
    /// `fields`: any mapped field.
    Fields,
    /// `docvalue_fields`: fields with doc values (not `text`).
    DocValue,
    /// `stored_fields`: fields mapped with `store: true`.
    Stored,
}

/// A mapped leaf field: its full name, where its values live in
/// `_source` (a multi-field reads its parent's), its type and mapping.
struct Leaf {
    name: String,
    path: String,
    ty: String,
    node: Value,
}

fn leaves(props: &Value, prefix: &str, out: &mut Vec<Leaf>) {
    let Some(m) = props.as_object() else { return };
    for (k, node) in m {
        let name = if prefix.is_empty() { k.clone() } else { format!("{prefix}.{k}") };
        let ty = node.get("type").and_then(Value::as_str).unwrap_or("object");
        match ty {
            "object" => {
                if let Some(p) = node.get("properties") {
                    leaves(p, &name, out);
                }
            }
            // Nested objects come back grouped per object, which isn't
            // modelled here: they are left out rather than flattened.
            "nested" => {}
            _ => {
                if let Some(Value::Object(subs)) = node.get("fields") {
                    for (sk, sn) in subs {
                        out.push(Leaf {
                            name: format!("{name}.{sk}"),
                            path: name.clone(),
                            ty: sn.get("type").and_then(Value::as_str).unwrap_or("keyword").into(),
                            node: sn.clone(),
                        });
                    }
                }
                out.push(Leaf {
                    name: name.clone(),
                    path: name,
                    ty: ty.into(),
                    node: node.clone(),
                });
            }
        }
    }
}

fn glob(pat: &str, name: &str) -> bool {
    let parts: Vec<&str> = pat.split('*').collect();
    if parts.len() == 1 {
        return pat == name;
    }
    let mut rest = name;
    for (i, p) in parts.iter().enumerate() {
        if i == 0 {
            match rest.strip_prefix(p) {
                Some(r) => rest = r,
                None => return false,
            }
        } else if i == parts.len() - 1 {
            return rest.len() >= p.len() && rest.ends_with(p);
        } else if let Some(at) = rest.find(p) {
            rest = &rest[at + p.len()..];
        } else {
            return false;
        }
    }
    true
}

fn is_numeric(ty: &str) -> bool {
    matches!(
        ty,
        "long"
            | "integer"
            | "short"
            | "byte"
            | "unsigned_long"
            | "double"
            | "float"
            | "half_float"
            | "scaled_float"
    )
}

fn as_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.trim().parse().ok(),
        _ => None,
    }
}

/// One source value as the mapped type returns it; `None` for a value the
/// field would have rejected or ignored (`ignore_malformed`, `ignore_above`).
fn format_value(leaf: &Leaf, v: &Value, format: Option<&str>) -> Option<Value> {
    match leaf.ty.as_str() {
        "keyword" | "constant_keyword" | "wildcard" | "text" | "match_only_text" | "version"
        | "ip" | "binary" | "search_as_you_type" => {
            let s = match v {
                Value::String(s) => s.clone(),
                Value::Number(n) => n.to_string(),
                Value::Bool(b) => b.to_string(),
                _ => return None,
            };
            if let Some(limit) = leaf.node.get("ignore_above").and_then(Value::as_u64)
                && s.chars().count() as u64 > limit
            {
                return None;
            }
            Some(json!(s))
        }
        "long" | "integer" | "short" | "byte" | "unsigned_long" => {
            let f = as_f64(v)?;
            Some(json!(f.trunc() as i64))
        }
        "double" | "float" | "half_float" | "scaled_float" => {
            let f = as_f64(v)?;
            Some(json!(f))
        }
        "boolean" => match v {
            Value::Bool(b) => Some(json!(b)),
            Value::String(s) if s == "true" => Some(json!(true)),
            Value::String(s) if s == "false" || s.is_empty() => Some(json!(false)),
            _ => None,
        },
        "date" | "date_nanos" => {
            let mapped = leaf.node.get("format").and_then(Value::as_str);
            if leaf.ty == "date_nanos"
                && format.is_none()
                && mapped.is_none()
                && let Value::String(s) = v
                && s.ends_with('Z')
                && s.contains('T')
            {
                // Sub-millisecond digits survive as indexed.
                return Some(json!(s));
            }
            let ms = dates::value_millis(v, mapped)?;
            Some(json!(dates::format(ms, format.or(mapped), 0)))
        }
        _ => Some(v.clone()),
    }
}

/// `"lat,lon"`, `{"lat":..,"lon":..}` or `[lon, lat]` as GeoJSON.
fn geo_point(v: &Value) -> Option<Value> {
    let (lat, lon) = match v {
        Value::Object(m) => (as_f64(m.get("lat")?)?, as_f64(m.get("lon")?)?),
        Value::Array(a) if a.len() == 2 && a.iter().all(Value::is_number) => {
            (a[1].as_f64()?, a[0].as_f64()?)
        }
        Value::String(s) => {
            let (a, b) = s.split_once(',')?;
            (a.trim().parse().ok()?, b.trim().parse().ok()?)
        }
        _ => return None,
    };
    Some(json!({"type": "Point", "coordinates": [lon, lat]}))
}

/// The values of `leaf` in `source`.
fn values_of(leaf: &Leaf, source: &Value, format: Option<&str>) -> Vec<Value> {
    if leaf.ty == "geo_point" {
        let mut node = Some(source);
        for seg in leaf.path.split('.') {
            node = node.and_then(|n| n.get(seg));
        }
        return match node {
            Some(Value::Array(a)) if !a.iter().all(Value::is_number) => {
                a.iter().filter_map(geo_point).collect()
            }
            Some(v) => geo_point(v).into_iter().collect(),
            None => Vec::new(),
        };
    }
    raw_values(source, &leaf.path)
        .into_iter()
        .filter_map(|v| format_value(leaf, v, format))
        .collect()
}

/// Every leaf value of an unmapped part of `source`, keyed by dotted path.
fn source_leaves(v: &Value, prefix: &str, out: &mut Vec<(String, Value)>) {
    match v {
        Value::Object(m) => {
            for (k, x) in m {
                let name = if prefix.is_empty() { k.clone() } else { format!("{prefix}.{k}") };
                source_leaves(x, &name, out);
            }
        }
        Value::Array(a) => a.iter().for_each(|x| source_leaves(x, prefix, out)),
        Value::Null => {}
        other => out.push((prefix.to_string(), other.clone())),
    }
}

/// The `fields` object of one hit for the requested `specs` (an array of
/// patterns or `{"field", "format", "include_unmapped"}` objects).
pub fn fetch(
    mappings: &Value,
    doc: &CommittedDoc,
    specs: &Value,
    kind: Kind,
) -> Result<Map<String, Value>, EsError> {
    let mut all = Vec::new();
    if let Some(p) = mappings.get("properties") {
        leaves(p, "", &mut all);
    }
    let source_enabled = mappings
        .get("_source")
        .and_then(|s| s.get("enabled"))
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let specs: Vec<Value> = match specs {
        Value::Array(a) => a.clone(),
        Value::Null => Vec::new(),
        other => vec![other.clone()],
    };
    let mut out = Map::new();
    for spec in &specs {
        let (pat, format, unmapped) = match spec {
            Value::String(s) => (s.as_str(), None, false),
            Value::Object(m) => (
                m.get("field").and_then(Value::as_str).unwrap_or(""),
                m.get("format").and_then(Value::as_str),
                m.get("include_unmapped").and_then(Value::as_bool).unwrap_or(false),
            ),
            _ => continue,
        };
        match pat {
            "_id" => {
                out.insert("_id".into(), json!([doc.id]));
                continue;
            }
            "_index" => {
                out.insert("_index".into(), json!([doc.index]));
                continue;
            }
            "_seq_no" if kind != Kind::Stored => {
                out.insert("_seq_no".into(), json!([doc.seq]));
                continue;
            }
            "_none_" | "_source" | "_routing" | "_ignored" => continue,
            _ => {}
        }
        let source = if source_enabled { &doc.source } else { &Value::Null };
        for leaf in all.iter().filter(|l| glob(pat, &l.name)) {
            if format.is_some() && !(leaf.ty.starts_with("date") || is_numeric(&leaf.ty)) {
                return Err(EsError::shard_failure(
                    "illegal_argument_exception",
                    &format!(
                        "Field [{}] of type [{}] doesn't support formats.",
                        leaf.name, leaf.ty
                    ),
                ));
            }
            match kind {
                Kind::Stored
                    if !leaf.node.get("store").and_then(Value::as_bool).unwrap_or(false) =>
                {
                    continue;
                }
                Kind::DocValue if matches!(leaf.ty.as_str(), "text" | "match_only_text") => {
                    if !pat.contains('*') {
                        return Err(EsError::shard_failure(
                            "illegal_argument_exception",
                            &format!(
                                "Text fields are not optimised for operations that require \
                                 per-document field data like aggregations and sorting, so these \
                                 operations are disabled by default. Please use a keyword field \
                                 instead. Alternatively, set fielddata=true on [{}] in order to \
                                 load field data by uninverting the inverted index. Note that this \
                                 can use significant memory.",
                                leaf.name
                            ),
                        ));
                    }
                    continue;
                }
                _ => {}
            }
            // A numeric `format` is a Java DecimalFormat; only the plain
            // `#.0`-style forms used for doc values are understood.
            let vals: Vec<Value> = if is_numeric(&leaf.ty) && format.is_some() {
                raw_values(source, &leaf.path)
                    .into_iter()
                    .filter_map(as_f64)
                    .map(|f| json!(decimal_format(f, format.unwrap_or(""))))
                    .collect()
            } else {
                values_of(leaf, source, format)
            };
            let vals =
                if kind == Kind::DocValue { sorted_doc_values(&leaf.ty, vals) } else { vals };
            if !vals.is_empty() {
                out.insert(leaf.name.clone(), Value::Array(vals));
            }
        }
        if unmapped && kind == Kind::Fields {
            let mut src = Vec::new();
            source_leaves(source, "", &mut src);
            for (name, v) in src {
                if glob(pat, &name)
                    && !all.iter().any(|l| l.name == name)
                    && let Some(a) = out.entry(name).or_insert_with(|| json!([])).as_array_mut()
                {
                    a.push(v);
                }
            }
        }
    }
    Ok(out)
}

/// Doc values come back sorted (and keyword doc values deduplicated).
fn sorted_doc_values(ty: &str, mut vals: Vec<Value>) -> Vec<Value> {
    if is_numeric(ty) {
        vals.sort_by(|a, b| {
            a.as_f64().partial_cmp(&b.as_f64()).unwrap_or(std::cmp::Ordering::Equal)
        });
    } else if matches!(ty, "keyword" | "constant_keyword" | "wildcard" | "ip" | "version") {
        vals.sort_by(|a, b| a.as_str().cmp(&b.as_str()));
        vals.dedup();
    }
    vals
}

/// The subset of Java's DecimalFormat used for numeric doc values:
/// `#`/`0` integer digits, an optional `.` and fraction digits.
fn decimal_format(f: f64, pattern: &str) -> String {
    let frac = pattern.split_once('.').map(|(_, fr)| fr);
    match frac {
        Some(fr) => {
            let min = fr.chars().filter(|c| *c == '0').count();
            let max = fr.chars().filter(|c| *c == '0' || *c == '#').count();
            let mut s = format!("{f:.max$}");
            if s.contains('.') {
                while s.ends_with('0') && s.split('.').nth(1).is_some_and(|d| d.len() > min) {
                    s.pop();
                }
                if s.ends_with('.') {
                    s.pop();
                }
            }
            s
        }
        None => format!("{}", f.round() as i64),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doc(source: Value) -> CommittedDoc {
        CommittedDoc {
            index: "i".into(),
            id: "1".into(),
            source,
            version: 1,
            seq: 0,
            full_source: None,
        }
    }

    #[test]
    fn fields_format_by_mapping() {
        let m = json!({"properties": {
            "k": {"type": "keyword"}, "n": {"type": "integer"}, "d": {"type": "date"},
            "t": {"type": "text", "fields": {"raw": {"type": "keyword"}}}
        }});
        let d = doc(json!({"k": ["a", "b"], "n": "42", "d": "1990-12-29T22:30:00Z", "t": "x y"}));
        let f = fetch(&m, &d, &json!(["*"]), Kind::Fields).unwrap();
        assert_eq!(f["k"], json!(["a", "b"]));
        assert_eq!(f["n"], json!([42]));
        assert_eq!(f["d"], json!(["1990-12-29T22:30:00.000Z"]));
        assert_eq!(f["t.raw"], json!(["x y"]));
        let f =
            fetch(&m, &d, &json!([{"field": "d", "format": "yyyy/MM/dd"}]), Kind::Fields).unwrap();
        assert_eq!(f["d"], json!(["1990/12/29"]));
        assert!(fetch(&m, &d, &json!([{"field": "k", "format": "yyyy"}]), Kind::Fields).is_err());
        assert!(fetch(&m, &d, &json!(["t"]), Kind::DocValue).is_err());
        assert_eq!(decimal_format(1.0, "#.0"), "1.0");
    }
}
