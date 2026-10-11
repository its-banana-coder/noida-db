//! Range field types (`integer_range`, `long_range`, `float_range`,
//! `double_range`, `date_range`, `ip_range`): a document holds an interval
//! (`{"gte": 1, "lte": 5}`, or a CIDR for `ip_range`), and `range` /
//! `term` / `terms` queries match by how the query relates to it
//! (`intersects`, the default, `contains` or `within`).

use serde_json::{Map, Value};
use std::cmp::Ordering;
use std::collections::HashMap;

use super::dates;
use super::search::{CommittedDoc, EsError, field_and_spec, raw_values, resolve_field};

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Integer,
    Long,
    Float,
    Double,
    Date,
    Ip,
}

/// A bound: integers, dates and addresses exactly, floats as floats.
#[derive(Clone, Copy, PartialEq, PartialOrd, Debug)]
enum K {
    I(i128),
    F(f64),
}

fn cmp(a: K, b: K) -> Ordering {
    a.partial_cmp(&b).unwrap_or(Ordering::Equal)
}

fn kind_of(ty: &str) -> Option<Kind> {
    Some(match ty {
        "integer_range" => Kind::Integer,
        "long_range" => Kind::Long,
        "float_range" => Kind::Float,
        "double_range" => Kind::Double,
        "date_range" => Kind::Date,
        "ip_range" => Kind::Ip,
        _ => return None,
    })
}

impl Kind {
    fn min(self) -> K {
        match self {
            Kind::Integer => K::I(i128::from(i32::MIN)),
            Kind::Long | Kind::Date => K::I(i128::from(i64::MIN)),
            Kind::Float => K::F(f64::from(f32::NEG_INFINITY)),
            Kind::Double => K::F(f64::NEG_INFINITY),
            // IPv6 addresses as unsigned 128-bit numbers, shifted to fit.
            Kind::Ip => K::I(i128::MIN),
        }
    }

    fn max(self) -> K {
        match self {
            Kind::Integer => K::I(i128::from(i32::MAX)),
            Kind::Long | Kind::Date => K::I(i128::from(i64::MAX)),
            Kind::Float => K::F(f64::from(f32::INFINITY)),
            Kind::Double => K::F(f64::INFINITY),
            Kind::Ip => K::I(i128::MAX),
        }
    }

    /// The next representable value above (`up`) or below `k`.
    fn step(self, k: K, up: bool) -> K {
        match (self, k) {
            (Kind::Float, K::F(f)) => {
                let f = f as f32;
                K::F(f64::from(if up { f.next_up() } else { f.next_down() }))
            }
            (_, K::F(f)) => K::F(if up { f.next_up() } else { f.next_down() }),
            (_, K::I(i)) => K::I(if up { i.saturating_add(1) } else { i.saturating_sub(1) }),
        }
    }

    /// One bound value as given in a document or a query.
    fn parse(self, v: &Value, format: Option<&str>, round_up: bool, tz: i64) -> Option<K> {
        let text = match v {
            Value::String(s) => s.trim().to_string(),
            Value::Number(n) => n.to_string(),
            _ => return None,
        };
        match self {
            Kind::Integer | Kind::Long => {
                let n = match v {
                    Value::Number(n) => n.as_i64().map(i128::from).or_else(|| {
                        n.as_u64().map(i128::from).or_else(|| n.as_f64().map(|f| f.trunc() as i128))
                    }),
                    _ => text.parse::<i128>().ok().or_else(|| {
                        text.parse::<f64>()
                            .ok()
                            .filter(|f| f.is_finite())
                            .map(|f| f.trunc() as i128)
                    }),
                }?;
                let (lo, hi) = if self == Kind::Integer {
                    (i128::from(i32::MIN), i128::from(i32::MAX))
                } else {
                    (i128::from(i64::MIN), i128::from(i64::MAX))
                };
                (lo..=hi).contains(&n).then_some(K::I(n))
            }
            Kind::Float => text.parse::<f32>().ok().map(|f| K::F(f64::from(f))),
            Kind::Double => text.parse::<f64>().ok().map(K::F),
            Kind::Date => match v {
                Value::Number(n) => n.as_i64().map(|ms| K::I(i128::from(ms))),
                _ => dates::parse_math(&text, dates::now_ms(), round_up, format, tz)
                    .map(|ms| K::I(i128::from(ms))),
            },
            Kind::Ip => ip_number(&text).map(K::I),
        }
    }
}

/// An IP address as an ordered number (IPv4 as IPv4-mapped IPv6).
fn ip_number(s: &str) -> Option<i128> {
    let bits = match s.parse::<std::net::IpAddr>().ok()? {
        std::net::IpAddr::V4(v4) => u128::from(v4.to_ipv6_mapped()),
        std::net::IpAddr::V6(v6) => u128::from(v6),
    };
    Some((bits ^ (1 << 127)) as i128)
}

/// A CIDR block (`192.168.0.0/24`) as its first and last address.
fn cidr(s: &str) -> Option<(K, K)> {
    let (addr, len) = s.split_once('/')?;
    let len: u32 = len.trim().parse().ok()?;
    let (bits, total) = match addr.trim().parse::<std::net::IpAddr>().ok()? {
        std::net::IpAddr::V4(v4) => (u128::from(v4.to_ipv6_mapped()), 32),
        std::net::IpAddr::V6(v6) => (u128::from(v6), 128),
    };
    if len > total {
        return None;
    }
    let host = 128 - (128 - total + len);
    let mask = if host == 128 { u128::MAX } else { (1u128 << host) - 1 };
    let shift = |b: u128| K::I((b ^ (1 << 127)) as i128);
    Some((shift(bits & !mask), shift(bits | mask)))
}

/// The inclusive (low, high) of an interval given by `gt`/`gte` and
/// `lt`/`lte` (`from`/`to` too). A missing bound is the type's extreme;
/// a `null` exclusive bound excludes the extreme itself.
fn interval(kind: Kind, o: &Map<String, Value>, format: Option<&str>, tz: i64) -> Option<(K, K)> {
    let mut lo = kind.min();
    let mut hi = kind.max();
    for (key, upper, exclusive) in [
        ("gte", false, false),
        ("from", false, false),
        ("gt", false, true),
        ("lte", true, false),
        ("to", true, false),
        ("lt", true, true),
    ] {
        let Some(v) = o.get(key) else { continue };
        let bound = if v.is_null() {
            if upper { kind.max() } else { kind.min() }
        } else {
            // Date math rounds up for `gt` and `lte`, as for a date field.
            kind.parse(v, format, upper != exclusive, tz)?
        };
        let bound = if exclusive { kind.step(bound, !upper) } else { bound };
        if upper {
            hi = bound;
        } else {
            lo = bound;
        }
    }
    Some((lo, hi))
}

/// A document's value for a range field.
fn doc_interval(kind: Kind, v: &Value, format: Option<&str>) -> Option<(K, K)> {
    match v {
        Value::Object(o) => interval(kind, o, format, 0),
        Value::String(s) if kind == Kind::Ip => cidr(s),
        _ => None,
    }
}

/// The mapping of `path`'s range field: (kind, format).
fn range_field(mappings: &Value, field: &str) -> Option<(String, Kind, Option<String>)> {
    let (path, ty) = resolve_field(mappings, field);
    let kind = kind_of(ty.as_deref()?)?;
    let mut node = mappings;
    for seg in path.split('.') {
        node = node.get("properties")?.get(seg)?;
    }
    let format = node.get("format").and_then(Value::as_str).map(String::from);
    Some((path, kind, format))
}

fn matching(
    docs: &[CommittedDoc],
    path: &str,
    kind: Kind,
    format: Option<&str>,
    boost: f32,
    hit: impl Fn((K, K)) -> bool,
) -> HashMap<usize, f32> {
    let mut out = HashMap::new();
    for (idx, d) in docs.iter().enumerate() {
        if raw_values(&d.source, path)
            .into_iter()
            .filter_map(|v| doc_interval(kind, v, format))
            .any(&hit)
        {
            out.insert(idx, boost);
        }
    }
    out
}

/// `term`, `match` or `match_phrase` on a `date` / `date_nanos` field:
/// the value as a one-value range, so `2021-04-28T18:51:04.467Z` matches
/// that instant whatever its form in `_source` (and a date-only value its
/// whole day), as Elasticsearch's date term query does.
fn date_term(
    query: &Map<String, Value>,
    mappings: &Value,
    docs: &[CommittedDoc],
) -> Option<Result<HashMap<usize, f32>, EsError>> {
    let body = ["term", "match", "match_phrase"].iter().find_map(|k| query.get(*k))?;
    let (field, spec) = field_and_spec(body)?;
    let ty = resolve_field(mappings, field).1;
    if !matches!(ty.as_deref(), Some("date" | "date_nanos")) {
        return None;
    }
    let (value, boost) = match spec {
        Value::Object(o) => (o.get("value").or_else(|| o.get("query"))?, o.get("boost")),
        v => (v, None),
    };
    if !(value.is_string() || value.is_number()) {
        return None;
    }
    let mut cond = serde_json::json!({"gte": value, "lte": value});
    if let Some(b) = boost {
        cond["boost"] = b.clone();
    }
    Some(super::search::eval(&serde_json::json!({"range": {field: cond}}), mappings, docs))
}

/// `range`, `term` and `terms` queries on a range field (and term-like
/// queries on a date field); `None` for any other query (or field type),
/// which the general evaluator handles.
pub(super) fn eval(
    query: &Map<String, Value>,
    mappings: &Value,
    docs: &[CommittedDoc],
) -> Option<Result<HashMap<usize, f32>, EsError>> {
    if let Some(r) = date_term(query, mappings, docs) {
        return Some(r);
    }
    let (kind_name, body) =
        ["range", "term", "terms"].iter().find_map(|k| query.get(*k).map(|b| (*k, b)))?;
    let (field, spec) = if kind_name == "terms" {
        let o = body.as_object()?;
        o.iter().find(|(k, _)| k.as_str() != "boost").map(|(k, v)| (k.as_str(), v))?
    } else {
        field_and_spec(body)?
    };
    let (path, kind, field_format) = range_field(mappings, field)?;
    let boost = spec.get("boost").and_then(Value::as_f64).unwrap_or(1.0) as f32;
    let field_format = field_format.as_deref();
    Some(match kind_name {
        "range" => {
            let o = spec.as_object()?;
            let format = o.get("format").and_then(Value::as_str).or(field_format);
            let tz = match o.get("time_zone").and_then(Value::as_str) {
                Some(z) => match dates::parse_offset(z) {
                    Some(t) => t,
                    None => {
                        return Some(Err(EsError::shard_failure(
                            "illegal_argument_exception",
                            &format!("Unknown time-zone ID: {z}"),
                        )));
                    }
                },
                None => 0,
            };
            let relation =
                o.get("relation").and_then(Value::as_str).unwrap_or("intersects").to_lowercase();
            match relation.as_str() {
                "intersects" | "contains" | "within" => {}
                "disjoint" => {
                    return Some(Err(EsError::new(
                        400,
                        "illegal_argument_exception",
                        "[range] query does not support relation [disjoint]",
                    )));
                }
                other => {
                    return Some(Err(EsError::new(
                        400,
                        "illegal_argument_exception",
                        &format!("{other} is not a valid relation"),
                    )));
                }
            }
            let Some((qlo, qhi)) = interval(kind, o, format, tz) else {
                return Some(Err(EsError::shard_failure(
                    "illegal_argument_exception",
                    &format!("failed to parse the bounds of [{field}]"),
                )));
            };
            Ok(matching(docs, &path, kind, field_format, boost, |(lo, hi)| {
                match relation.as_str() {
                    "contains" => {
                        cmp(lo, qlo) != Ordering::Greater && cmp(hi, qhi) != Ordering::Less
                    }
                    "within" => cmp(lo, qlo) != Ordering::Less && cmp(hi, qhi) != Ordering::Greater,
                    _ => cmp(lo, qhi) != Ordering::Greater && cmp(hi, qlo) != Ordering::Less,
                }
            }))
        }
        _ => {
            let values: Vec<&Value> = match spec {
                Value::Array(a) => a.iter().collect(),
                Value::Object(o) => o.get("value").into_iter().collect(),
                other => vec![other],
            };
            let points: Vec<K> =
                values.iter().filter_map(|v| kind.parse(v, field_format, false, 0)).collect();
            Ok(matching(docs, &path, kind, field_format, boost, |(lo, hi)| {
                points
                    .iter()
                    .any(|p| cmp(lo, *p) != Ordering::Greater && cmp(hi, *p) != Ordering::Less)
            }))
        }
    })
}

fn shown(k: K) -> String {
    match k {
        K::I(i) => i.to_string(),
        K::F(f) if f.fract() == 0.0 && f.is_finite() => format!("{f:.1}"),
        K::F(f) => f.to_string(),
    }
}

/// A document's range fields as Elasticsearch parses them: an object of
/// bounds (or a CIDR for `ip_range`), each a valid value of the type, the
/// lower not above the upper.
pub(super) fn check_doc(mappings: &Value, source: &Value, id: &str) -> Result<(), (u16, Value)> {
    fn walk(props: Option<&Value>, prefix: &str, out: &mut Vec<(String, Kind, Option<String>)>) {
        let Some(Value::Object(m)) = props else { return };
        for (k, def) in m {
            let name = if prefix.is_empty() { k.clone() } else { format!("{prefix}.{k}") };
            if let Some(kind) = def.get("type").and_then(Value::as_str).and_then(kind_of) {
                let format = def.get("format").and_then(Value::as_str).map(String::from);
                out.push((name.clone(), kind, format));
            }
            walk(def.get("properties"), &name, out);
        }
    }
    let mut fields = Vec::new();
    walk(mappings.get("properties"), "", &mut fields);
    for (name, kind, format) in fields {
        let ty = match kind {
            Kind::Integer => "integer_range",
            Kind::Long => "long_range",
            Kind::Float => "float_range",
            Kind::Double => "double_range",
            Kind::Date => "date_range",
            Kind::Ip => "ip_range",
        };
        for v in raw_values(source, &name) {
            let failure = |preview: String, cause: (&str, String)| {
                let reason = format!(
                    "[1:1] failed to parse field [{name}] of type [{ty}] in document with id '{id}'. Preview of field's value: '{preview}'"
                );
                let mut e = super::engine::error("document_parsing_exception", &reason, 400);
                e["error"]["caused_by"] = serde_json::json!({"type": cause.0, "reason": cause.1});
                Err((400, e))
            };
            match v {
                Value::Null => {}
                Value::Object(o) => {
                    for (key, b) in o {
                        if b.is_null()
                            || !matches!(key.as_str(), "gt" | "gte" | "lt" | "lte" | "from" | "to")
                        {
                            continue;
                        }
                        if kind.parse(b, format.as_deref(), false, 0).is_none() {
                            let text =
                                b.as_str().map(String::from).unwrap_or_else(|| b.to_string());
                            return failure(
                                text.clone(),
                                (
                                    "number_format_exception",
                                    format!("For input string: \"{text}\""),
                                ),
                            );
                        }
                    }
                    if let Some((lo, hi)) = interval(kind, o, format.as_deref(), 0)
                        && cmp(lo, hi) == Ordering::Greater
                    {
                        return failure(
                            "null".into(),
                            (
                                "illegal_argument_exception",
                                format!(
                                    "min value ({}) is greater than max value ({})",
                                    shown(lo),
                                    shown(hi)
                                ),
                            ),
                        );
                    }
                }
                Value::String(s) if kind == Kind::Ip && cidr(s).is_some() => {}
                other => {
                    let text =
                        other.as_str().map(String::from).unwrap_or_else(|| other.to_string());
                    let leaf = name.rsplit('.').next().unwrap_or(&name).to_string();
                    return failure(
                        text,
                        (
                            "document_parsing_exception",
                            format!(
                                "[1:1] error parsing field [{name}], expected an object but got {leaf}"
                            ),
                        ),
                    );
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn exclusive_null_bounds_skip_the_extreme() {
        let o = json!({"gt": null, "lte": 5});
        let (lo, hi) = interval(Kind::Long, o.as_object().unwrap(), None, 0).unwrap();
        assert_eq!(lo, K::I(i128::from(i64::MIN) + 1));
        assert_eq!(hi, K::I(5));
    }

    #[test]
    fn cidr_blocks() {
        let (lo, hi) = cidr("192.168.0.0/24").unwrap();
        assert_eq!(lo, K::I(ip_number("192.168.0.0").unwrap()));
        assert_eq!(hi, K::I(ip_number("192.168.0.255").unwrap()));
    }
}
