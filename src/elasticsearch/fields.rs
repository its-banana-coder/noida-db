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
/// `_source` (relative to the nested object it's in; a multi-field reads
/// its parent's), its type and mapping.
struct Leaf {
    name: String,
    path: String,
    ty: String,
    node: Value,
}

/// A `nested` field: its objects' fields come back grouped per object.
struct Nested {
    name: String,
    path: String,
    node: Value,
}

fn join(a: &str, b: &str) -> String {
    if a.is_empty() { b.to_string() } else { format!("{a}.{b}") }
}

fn leaves(props: &Value, prefix: &str, path: &str, out: &mut Vec<Leaf>, nested: &mut Vec<Nested>) {
    let Some(m) = props.as_object() else { return };
    for (k, node) in m {
        let name = join(prefix, k);
        let at = join(path, k);
        let ty = node.get("type").and_then(Value::as_str).unwrap_or("object");
        match ty {
            "object" => {
                if let Some(p) = node.get("properties") {
                    leaves(p, &name, &at, out, nested);
                }
            }
            "nested" => nested.push(Nested { name, path: at, node: node.clone() }),
            _ => {
                if let Some(Value::Object(subs)) = node.get("fields") {
                    for (sk, sn) in subs {
                        out.push(Leaf {
                            name: format!("{name}.{sk}"),
                            path: at.clone(),
                            ty: sn.get("type").and_then(Value::as_str).unwrap_or("keyword").into(),
                            node: sn.clone(),
                        });
                    }
                }
                out.push(Leaf { name, path: at, ty: ty.into(), node: node.clone() });
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
        "token_count" => match v {
            Value::String(s) => Some(json!(super::analysis::standard(s).len())),
            _ => None,
        },
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

/// A GeoJSON point as WKT.
fn wkt(p: &Value) -> Value {
    let c = &p["coordinates"];
    let num = |v: &Value| v.as_f64().map_or(String::new(), |f| f.to_string());
    json!(format!("POINT ({} {})", num(&c[0]), num(&c[1])))
}

/// The values of `leaf` in `source`.
fn values_of(leaf: &Leaf, source: &Value, format: Option<&str>) -> Vec<Value> {
    if leaf.ty == "geo_point" && format == Some("wkt") {
        return values_of(leaf, source, None).iter().map(wkt).collect();
    }
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

/// One requested field pattern.
struct Spec<'a> {
    pat: &'a str,
    format: Option<&'a str>,
    unmapped: bool,
}

/// Formats each field type supports.
fn supports_format(ty: &str, format: &str) -> bool {
    ty.starts_with("date")
        || is_numeric(ty)
        || (matches!(ty, "geo_point" | "geo_shape" | "point" | "shape")
            && matches!(format, "geojson" | "wkt"))
}

fn format_error(leaf_name: &str, ty: &str, pat: &str) -> EsError {
    let matched = if pat.contains('*') { format!(" which matched [{pat}]") } else { String::new() };
    EsError::shard_failure(
        "illegal_argument_exception",
        &format!(
            "error fetching [{leaf_name}]{matched}: Field [{leaf_name}] of type [{ty}] doesn't support formats."
        ),
    )
}

/// The fields of `source`, an object at nested scope `scope` (`""` at
/// the document root) mapped by `props`, keyed relative to the scope.
fn fetch_scope(
    props: Option<&Value>,
    source: &Value,
    scope: &str,
    specs: &[Spec],
    kind: Kind,
    out: &mut Map<String, Value>,
) -> Result<(), EsError> {
    let mut all = Vec::new();
    let mut nested = Vec::new();
    if let Some(p) = props {
        leaves(p, scope, "", &mut all, &mut nested);
    }
    // A field alias reads its target's values, as its target's type.
    let targets: Vec<(String, String, Value)> =
        all.iter().map(|l| (l.name.clone(), l.path.clone(), l.node.clone())).collect();
    for leaf in all.iter_mut().filter(|l| l.ty == "alias") {
        let target = leaf.node.get("path").and_then(Value::as_str).unwrap_or("").to_string();
        if let Some((_, path, node)) = targets.iter().find(|(n, _, _)| *n == target) {
            leaf.path = path.clone();
            leaf.ty = node.get("type").and_then(Value::as_str).unwrap_or("keyword").into();
            leaf.node = node.clone();
        } else {
            leaf.ty = format!("alias:{target}");
        }
    }
    let rel = |name: &str| -> String {
        if scope.is_empty() { name.to_string() } else { name[scope.len() + 1..].to_string() }
    };
    for spec in specs {
        let (pat, format) = (spec.pat, spec.format);
        for leaf in all.iter().filter(|l| glob(pat, &l.name)) {
            if let Some(f) = format
                && !supports_format(&leaf.ty, f)
            {
                return Err(format_error(&leaf.name, &leaf.ty, pat));
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
                out.insert(rel(&leaf.name), Value::Array(vals));
            }
        }
        // A flattened field's keys by exact name: `flattened.some_field`.
        if kind == Kind::Fields && !pat.contains('*') {
            for leaf in all.iter().filter(|l| l.ty == "flattened") {
                if let Some(key) = pat.strip_prefix(&format!("{}.", leaf.name)) {
                    if format.is_some() {
                        return Err(format_error(pat, "flattened", pat));
                    }
                    let mut vals = vec![];
                    for obj in raw_values(source, &leaf.path) {
                        for v in raw_values(obj, key) {
                            if !v.is_object() && !v.is_null() {
                                vals.push(json!(match v {
                                    Value::String(s) => s.clone(),
                                    other => other.to_string(),
                                }));
                            }
                        }
                    }
                    if !vals.is_empty() {
                        out.insert(rel(pat), Value::Array(vals));
                    }
                }
            }
        }
        if spec.unmapped && kind == Kind::Fields {
            let mut src = Vec::new();
            source_leaves(source, "", &mut src);
            for (path, v) in src {
                let name = join(scope, &path);
                // Mapped fields (and whatever is inside one, like a
                // flattened object's keys) and nested objects' fields
                // aren't unmapped.
                let inside = |n: &str| name == n || name.starts_with(&format!("{n}."));
                if glob(pat, &name)
                    && !all.iter().any(|l| inside(&l.name))
                    && !nested.iter().any(|n| inside(&n.name))
                    && let Some(a) = out.entry(path).or_insert_with(|| json!([])).as_array_mut()
                {
                    a.push(v);
                }
            }
        }
    }
    // Nested objects: one entry per object, with its fields.
    if kind == Kind::Fields {
        for n in &nested {
            let mut group = vec![];
            for obj in raw_values(source, &n.path) {
                let mut m = Map::new();
                fetch_scope(n.node.get("properties"), obj, &n.name, specs, kind, &mut m)?;
                if !m.is_empty() {
                    group.push(Value::Object(m));
                }
            }
            if !group.is_empty() {
                out.insert(rel(&n.name), Value::Array(group));
            }
        }
    }
    Ok(())
}

/// The `fields` object of one hit for the requested `specs` (an array of
/// patterns or `{"field", "format", "include_unmapped"}` objects).
pub fn fetch(
    mappings: &Value,
    doc: &CommittedDoc,
    specs: &Value,
    kind: Kind,
) -> Result<Map<String, Value>, EsError> {
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
    let mut wanted = vec![];
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
        // Metadata fields are fetched by exact name only.
        match pat {
            "_id" => {
                out.insert("_id".into(), json!([doc.id]));
                continue;
            }
            "_index" => {
                out.insert("_index".into(), json!([doc.index]));
                continue;
            }
            "_version" if kind == Kind::Fields => {
                out.insert("_version".into(), json!([doc.version]));
                continue;
            }
            "_ignored" if kind == Kind::Fields => {
                let ignored = super::docparse::ignored_fields(mappings, &Value::Null, doc.full());
                if !ignored.is_empty() {
                    out.insert("_ignored".into(), json!(ignored));
                }
                continue;
            }
            "_seq_no" | "_source" | "_primary_term" | "_field_names" if kind == Kind::Fields => {
                let mut e = EsError::shard_failure(
                    "unsupported_operation_exception",
                    &format!("Cannot fetch values for internal field [{pat}]."),
                );
                e.status = 500;
                return Err(e);
            }
            "_none_" | "_source" | "_routing" | "_ignored" | "_seq_no" | "_version" => continue,
            _ => {}
        }
        wanted.push(Spec { pat, format, unmapped });
    }
    let source = if source_enabled { &doc.source } else { &Value::Null };
    let mut fetched = Map::new();
    fetch_scope(mappings.get("properties"), source, "", &wanted, kind, &mut fetched)?;
    // An alias of `_id` reads the document's id.
    let mut all = vec![];
    let mut nested = vec![];
    if let Some(p) = mappings.get("properties") {
        leaves(p, "", "", &mut all, &mut nested);
    }
    for l in all.iter().filter(|l| l.ty == "alias") {
        if l.node.get("path").and_then(Value::as_str) == Some("_id")
            && kind == Kind::Fields
            && wanted.iter().any(|w| glob(w.pat, &l.name))
        {
            fetched.insert(l.name.clone(), json!([doc.id]));
        }
    }
    for (k, v) in fetched {
        out.insert(k, v);
    }
    Ok(out)
}

/// Whether a `fields` request (`specs`) asks for field `name`.
pub fn requested(specs: &Value, name: &str) -> bool {
    let one = |s: &Value| match s {
        Value::String(p) => glob(p, name),
        Value::Object(m) => m.get("field").and_then(Value::as_str).is_some_and(|p| glob(p, name)),
        _ => false,
    };
    match specs {
        Value::Array(a) => a.iter().any(one),
        other => one(other),
    }
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
