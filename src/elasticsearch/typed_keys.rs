//! `typed_keys=true`: each aggregation in the response named
//! `<type>#<name>` (sub-aggregations too), the type being the result
//! class Elasticsearch writes (`sterms`, `lterms`, `tdigest_percentiles`,
//! `simple_value`, ...), so clients can parse it back.

use serde_json::{Map, Value};

use super::search::resolve_field;

fn kind_of(def: &Value) -> Option<&str> {
    def.as_object()?
        .keys()
        .find(|k| !matches!(k.as_str(), "aggs" | "aggregations" | "meta"))
        .map(String::as_str)
}

fn field_type(mappings: &Value, body: &Value) -> Option<String> {
    let f = body.get("field").and_then(Value::as_str)?;
    resolve_field(mappings, f).1
}

fn is_long(t: &str) -> bool {
    matches!(
        t,
        "long" | "integer" | "short" | "byte" | "unsigned_long" | "date" | "date_nanos" | "boolean"
    )
}

fn is_double(t: &str) -> bool {
    matches!(t, "double" | "float" | "half_float" | "scaled_float")
}

/// The type name `typed_keys` gives an aggregation of `kind`.
fn type_name(kind: &str, body: &Value, mappings: &Value) -> String {
    let ty = field_type(mappings, body).unwrap_or_default();
    let numeric = |l: &str, d: &str, s: &str| {
        if is_long(&ty) {
            l.to_string()
        } else if is_double(&ty) {
            d.to_string()
        } else {
            s.to_string()
        }
    };
    match kind {
        "terms" => numeric("lterms", "dterms", "sterms"),
        "rare_terms" => numeric("lrareterms", "lrareterms", "srareterms"),
        "significant_terms" => numeric("siglterms", "siglterms", "sigsterms"),
        "percentiles" if body.get("hdr").is_some() => "hdr_percentiles".into(),
        "percentiles" => "tdigest_percentiles".into(),
        "percentile_ranks" if body.get("hdr").is_some() => "hdr_percentile_ranks".into(),
        "percentile_ranks" => "tdigest_percentile_ranks".into(),
        "avg_bucket" | "sum_bucket" | "cumulative_sum" | "bucket_script" | "moving_fn"
        | "serial_diff" => "simple_value".into(),
        "max_bucket" | "min_bucket" => "bucket_metric_value".into(),
        other => other.into(),
    }
}

/// Renames the aggregations `spec` asks for inside `obj` (a response's
/// `aggregations`, a bucket, or a single-bucket aggregation).
fn walk(spec: &Map<String, Value>, obj: &mut Map<String, Value>, mappings: &Value) {
    for (name, def) in spec {
        let Some(kind) = kind_of(def) else { continue };
        let Some(mut v) = obj.remove(name) else { continue };
        if let Some(sub) =
            def.get("aggs").or_else(|| def.get("aggregations")).and_then(Value::as_object)
        {
            if let Some(m) = v.as_object_mut() {
                walk(sub, m, mappings);
            }
            match v.get_mut("buckets") {
                Some(Value::Array(bs)) => {
                    for b in bs.iter_mut().filter_map(Value::as_object_mut) {
                        walk(sub, b, mappings);
                    }
                }
                Some(Value::Object(bs)) => {
                    for b in bs.values_mut().filter_map(Value::as_object_mut) {
                        walk(sub, b, mappings);
                    }
                }
                _ => {}
            }
        }
        let body = &def[kind];
        obj.insert(format!("{}#{name}", type_name(kind, body, mappings)), v);
    }
}

/// Applies `typed_keys` to a search response's aggregations.
pub fn aggs(req: &Value, resp: &mut Value, mappings: &Value) {
    let Some(spec) = req.get("aggs").or_else(|| req.get("aggregations")).and_then(Value::as_object)
    else {
        return;
    };
    if let Some(a) = resp.get_mut("aggregations").and_then(Value::as_object_mut) {
        walk(spec, a, mappings);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn names_by_result_type() {
        let m = json!({"properties": {"kw": {"type": "keyword"}, "n": {"type": "long"}}});
        let req = json!({"aggs": {
            "t": {"terms": {"field": "kw"}, "aggs": {"x": {"max": {"field": "n"}}}},
            "p": {"percentiles": {"field": "n"}}}});
        let mut resp = json!({"aggregations": {
            "t": {"buckets": [{"key": "a", "doc_count": 1, "x": {"value": 1.0}}]},
            "p": {"values": {}}}});
        aggs(&req, &mut resp, &m);
        let a = &resp["aggregations"];
        assert!(a["sterms#t"]["buckets"][0]["max#x"].is_object());
        assert!(a["tdigest_percentiles#p"].is_object());
    }
}
