//! Synthetic `_source`. An index whose mapping says
//! `"_source": {"mode": "synthetic"}` (or whose `index.mode` is
//! `time_series` or `logsdb`) doesn't keep the JSON it was sent:
//! Elasticsearch 8.15 rebuilds `_source` from the indexed values, so what
//! get, mget and search hits return is shaped by the mapping. Dotted names
//! become objects; an array of objects merges into one object of arrays;
//! leaf arrays come back sorted (keyword-like ones de-duplicated); values
//! come back as their mapped type (`"5"` in a `long` is `5`, a
//! `half_float` loses precision, a date is formatted); ranges as closed
//! `gte`/`lte` bounds. Values a field keeps without indexing them
//! (`ignore_above`, `ignore_malformed`) follow the indexed ones, and what
//! isn't indexed at all (unmapped fields under `dynamic: false`, disabled
//! objects, `store_array_source` arrays, field types without a synthetic
//! loader) comes back as it was sent, dotted names expanded.
//!
//! Documents keep the JSON they were sent; `source` derives the synthetic
//! view from it with the index's current mapping. `force_synthetic_source`
//! asks for the same view of an index that stores its source.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv6Addr};

use serde_json::{Map, Value, json};

use super::dates;

const TEXT_UNSUPPORTED: &str = "doesn't support synthetic source unless it is stored or has a \
                                sub-field of type [keyword] with doc values or stored and without \
                                a normalizer";

/// An `index.*` setting (settings are kept nested under `index`).
fn index_setting<'a>(settings: &'a Value, key: &str) -> Option<&'a Value> {
    let mut node = settings.get("index");
    for part in key.split('.') {
        node = node.and_then(|n| n.get(part));
    }
    node.or_else(|| settings.get(format!("index.{key}")))
}

fn index_mode(settings: &Value) -> Option<&str> {
    index_setting(settings, "mode").and_then(Value::as_str)
}

/// A boolean mapping parameter, given as a JSON boolean or a string.
fn flag(def: &Value, key: &str) -> Option<bool> {
    match def.get(key)? {
        Value::Bool(b) => Some(*b),
        Value::String(s) if s == "true" => Some(true),
        Value::String(s) if s == "false" => Some(false),
        _ => None,
    }
}

fn type_of(def: &Value) -> &str {
    def.get("type").and_then(Value::as_str).unwrap_or("object")
}

/// Whether the index's `_source` is synthetic.
pub(crate) fn enabled(mappings: &Value, settings: &Value) -> bool {
    match mappings.get("_source").and_then(|s| s.get("mode")).and_then(Value::as_str) {
        Some(m) => m.eq_ignore_ascii_case("synthetic"),
        None => matches!(index_mode(settings), Some("time_series" | "logsdb")),
    }
}

/// `_source` as get, mget and search return it for this index.
pub(crate) fn view(mappings: &Value, settings: &Value, src: &Value) -> Value {
    if enabled(mappings, settings) { source(mappings, src) } else { src.clone() }
}

fn mapper_parsing(reason: &str, cause_kind: &str) -> (u16, Value) {
    let full = format!("Failed to parse mapping: {reason}");
    // The root cause is the innermost Elasticsearch exception: the cause
    // itself when it is one, else the mapper_parsing_exception.
    let root = if cause_kind == "mapper_parsing_exception" {
        json!({"type": cause_kind, "reason": reason})
    } else {
        json!({"type": "mapper_parsing_exception", "reason": full})
    };
    (
        400,
        json!({"error": {"root_cause": [root], "type": "mapper_parsing_exception", "reason": full,
                         "caused_by": {"type": cause_kind, "reason": reason}},
               "status": 400}),
    )
}

fn illegal_argument(reason: &str) -> (u16, Value) {
    (
        400,
        json!({"error": {"root_cause": [{"type": "illegal_argument_exception", "reason": reason}],
                         "type": "illegal_argument_exception", "reason": reason},
               "status": 400}),
    )
}

/// The `_source` mapping parameters Elasticsearch refuses, and (when the
/// source is synthetic) the first field it can't rebuild.
pub(crate) fn check_mapping(mappings: &Value, settings: &Value) -> Result<(), (u16, Value)> {
    check_params(mappings, settings)?;
    if enabled(mappings, settings)
        && let Some(reason) = unsupported(mappings, settings)
    {
        return Err(illegal_argument(&reason));
    }
    Ok(())
}

fn check_params(mappings: &Value, settings: &Value) -> Result<(), (u16, Value)> {
    if let Some(src) = mappings.get("_source") {
        if let Some(mode) = src.get("mode") {
            let m = mode.as_str().map(str::to_ascii_lowercase).unwrap_or_default();
            if !matches!(m.as_str(), "synthetic" | "stored" | "disabled") {
                let shown = mode.as_str().map_or_else(|| mode.to_string(), str::to_string);
                return Err(mapper_parsing(
                    &format!(
                        "No enum constant org.elasticsearch.index.mapper.SourceFieldMapper.Mode.{}",
                        shown.to_ascii_uppercase()
                    ),
                    "illegal_argument_exception",
                ));
            }
            if src.get("enabled").is_some() {
                return Err(mapper_parsing(
                    "Cannot set both [mode] and [enabled] parameters",
                    "mapper_parsing_exception",
                ));
            }
        }
        let filtered = ["includes", "excludes"]
            .iter()
            .any(|k| src.get(*k).and_then(Value::as_array).is_some_and(|a| !a.is_empty()));
        if filtered && enabled(mappings, settings) {
            return Err(mapper_parsing(
                "filtering the stored _source is incompatible with synthetic source",
                "illegal_argument_exception",
            ));
        }
    }
    Ok(())
}

/// A put-mapping: its `_source` parameters, no switching a synthetic
/// `_source` back to stored, and (on a synthetic index) every field of the
/// `merged` mapping still rebuildable.
pub(crate) fn check_update(
    current: &Value,
    incoming: &Value,
    merged: &Value,
    settings: &Value,
) -> Result<(), (u16, Value)> {
    check_params(incoming, settings)?;
    let mode = |m: &Value| {
        m.get("_source").and_then(|s| s.get("mode")).and_then(Value::as_str).map(str::to_lowercase)
    };
    if let (Some(from), Some(to)) = (mode(current), mode(incoming))
        && from != to
        && from == "synthetic"
    {
        return Err(illegal_argument(&format!(
            "Mapper for [_source] conflicts with existing mapper:\n\tCannot update parameter \
             [mode] from [{from}] to [{to}]"
        )));
    }
    if let Some(inc) = incoming.get("_source") {
        let list = |m: Option<&Value>, k: &str| {
            let items: Vec<String> = m
                .and_then(|s| s.get(k))
                .and_then(Value::as_array)
                .map(|a| a.iter().map(|x| x.as_str().unwrap_or_default().to_string()).collect())
                .unwrap_or_default();
            items
        };
        for k in ["includes", "excludes"] {
            let (from, to) = (list(current.get("_source"), k), list(Some(inc), k));
            if from != to {
                return Err(illegal_argument(&format!(
                    "Mapper for [_source] conflicts with existing mapper:\n\tCannot update \
                     parameter [{k}] from [[{}]] to [[{}]]",
                    from.join(", "),
                    to.join(", ")
                )));
            }
        }
    }
    if enabled(merged, settings)
        && let Some(reason) = unsupported(merged, settings)
    {
        return Err(illegal_argument(&reason));
    }
    Ok(())
}

/// A put-mapping's `_source` replaces the index's (parameters it leaves
/// out go back to their defaults).
pub(crate) fn put_source(mappings: &mut Value, incoming: &Value) {
    if let Some(src) = incoming.get("_source")
        && let Some(m) = mappings.as_object_mut()
    {
        m.insert("_source".into(), src.clone());
    }
}

/// `_source` as the mapping shows it: default parameters left out.
pub(crate) fn tidy_source(mappings: &mut Value) {
    let Some(m) = mappings.as_object_mut() else { return };
    if let Some(Value::Object(src)) = m.get_mut("_source") {
        if src.get("enabled") == Some(&Value::Bool(true)) {
            src.remove("enabled");
        }
        for k in ["includes", "excludes"] {
            if src.get(k).and_then(Value::as_array).is_some_and(Vec::is_empty) {
                src.remove(k);
            }
        }
        if src.is_empty() {
            m.remove("_source");
        }
    }
}

/// Why this mapping can't produce a synthetic `_source`: the first field
/// (by name, depth first) Elasticsearch refuses, as its error reason.
pub(crate) fn unsupported(mappings: &Value, settings: &Value) -> Option<String> {
    fn walk(props: Option<&Value>, prefix: &str, lenient: bool) -> Option<String> {
        let props = props?.as_object()?;
        let mut names: Vec<&String> = props.keys().collect();
        names.sort();
        for k in names {
            let def = &props[k];
            let name = if prefix.is_empty() { k.clone() } else { format!("{prefix}.{k}") };
            let found = match type_of(def) {
                "object" | "nested" => walk(def.get("properties"), &name, lenient),
                ty => field_problem(&name, ty, def, lenient),
            };
            if found.is_some() {
                return found;
            }
        }
        None
    }
    // `logsdb` indices fall back to storing text fields as sent.
    let lenient = index_mode(settings) == Some("logsdb");
    walk(mappings.get("properties"), "", lenient)
}

fn has_doc_values(ty: &str, def: &Value) -> bool {
    flag(def, "doc_values").unwrap_or(ty != "binary")
}

fn stored(def: &Value) -> bool {
    flag(def, "store") == Some(true)
}

/// The `keyword` multi-field a non-stored `text` field is rebuilt from.
fn text_source_field(def: &Value) -> Option<&Value> {
    let subs = def.get("fields")?.as_object()?;
    let mut names: Vec<&String> = subs.keys().collect();
    names.sort();
    names.into_iter().map(|n| &subs[n]).find(|sub| {
        type_of(sub) == "keyword"
            && sub.get("normalizer").is_none_or(Value::is_null)
            && (has_doc_values("keyword", sub) || stored(sub))
    })
}

const RANGES: &[&str] =
    &["integer_range", "long_range", "float_range", "double_range", "date_range", "ip_range"];

fn field_problem(name: &str, ty: &str, def: &Value, lenient: bool) -> Option<String> {
    let because = |why: &str| {
        Some(format!(
            "field [{name}] of type [{ty}] doesn't support synthetic source because {why}"
        ))
    };
    if def.get("copy_to").is_some_and(|c| match c {
        Value::Null => false,
        Value::Array(a) => !a.is_empty(),
        _ => true,
    }) {
        return because("it declares copy_to");
    }
    match ty {
        "text" if !lenient && !stored(def) && text_source_field(def).is_none() => {
            Some(format!("field [{name}] of type [text] {TEXT_UNSUPPORTED}"))
        }
        "keyword" if def.get("normalizer").is_some_and(|n| !n.is_null()) => {
            because("it declares a normalizer")
        }
        "boolean" | "ip" | "date" | "date_nanos" | "geo_point" | "flattened" | "scaled_float"
        | "unsigned_long" | "binary"
            if !has_doc_values(ty, def) =>
        {
            because("it doesn't have doc values")
        }
        t if RANGES.contains(&t) && !has_doc_values(ty, def) => {
            because("it doesn't have doc values")
        }
        _ => None,
    }
}

/// Whether a field of this type is rebuilt from its indexed values (else
/// it keeps its values as sent).
fn native(ty: &str, def: &Value) -> bool {
    match ty {
        "keyword" => has_doc_values(ty, def) || stored(def),
        "text" => stored(def) || text_source_field(def).is_some(),
        "long" | "integer" | "short" | "byte" | "double" | "float" | "half_float" => {
            has_doc_values(ty, def)
        }
        "scaled_float" | "unsigned_long" | "boolean" | "ip" | "date" | "date_nanos"
        | "geo_point" | "flattened" | "binary" => has_doc_values(ty, def),
        t if RANGES.contains(&t) => has_doc_values(ty, def),
        "wildcard"
        | "version"
        | "constant_keyword"
        | "dense_vector"
        | "aggregate_metric_double" => true,
        _ => false,
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Dynamic {
    True,
    False,
    Runtime,
    Strict,
}

fn dynamic_of(def: &Value) -> Option<Dynamic> {
    match def.get("dynamic")? {
        Value::Bool(true) => Some(Dynamic::True),
        Value::Bool(false) => Some(Dynamic::False),
        Value::String(s) => match s.as_str() {
            "true" => Some(Dynamic::True),
            "false" => Some(Dynamic::False),
            "runtime" => Some(Dynamic::Runtime),
            "strict" => Some(Dynamic::Strict),
            _ => None,
        },
        _ => None,
    }
}

/// Where a part of the document is parsed: the object's mapped fields and
/// its (possibly inherited) `dynamic` and `subobjects`.
struct Ctx<'a> {
    props: Option<&'a Map<String, Value>>,
    dynamic: Dynamic,
    subobjects: bool,
}

impl<'a> Ctx<'a> {
    fn child(&self, def: &'a Value) -> Ctx<'a> {
        Ctx {
            props: def.get("properties").and_then(Value::as_object),
            dynamic: dynamic_of(def).unwrap_or(self.dynamic),
            subobjects: flag(def, "subobjects") != Some(false),
        }
    }
}

/// One object of the rebuilt document: what each field name collected.
#[derive(Default)]
struct Obj<'a> {
    props: Option<&'a Map<String, Value>>,
    slots: BTreeMap<String, Slot<'a>>,
}

#[derive(Default)]
struct Slot<'a> {
    def: Option<&'a Value>,
    /// Values kept as sent (unmapped, disabled, stored arrays, fallback
    /// field types); they replace whatever was indexed under the name.
    raw: Vec<Value>,
    /// A leaf field's values in document order.
    values: Vec<Value>,
    object: Option<Obj<'a>>,
    nested: Option<Vec<Obj<'a>>>,
}

impl<'a> Obj<'a> {
    fn new(props: Option<&'a Map<String, Value>>) -> Self {
        Obj { props, slots: BTreeMap::new() }
    }
}

/// The synthetic `_source` of `src` under `mappings`.
pub(crate) fn source(mappings: &Value, src: &Value) -> Value {
    let Value::Object(fields) = src else { return src.clone() };
    let ctx = Ctx {
        props: mappings.get("properties").and_then(Value::as_object),
        dynamic: dynamic_of(mappings).unwrap_or(Dynamic::True),
        subobjects: flag(mappings, "subobjects") != Some(false),
    };
    let mut root = Obj::new(ctx.props);
    if flag(mappings, "enabled") == Some(false) {
        // A disabled root object indexes nothing: every field is kept.
        for (k, v) in fields {
            let (name, v) = match k.split_once('.') {
                Some((head, rest)) if !head.is_empty() && !rest.is_empty() => {
                    (head.to_string(), json!({ rest: v }))
                }
                _ => (k.clone(), v.clone()),
            };
            root.slots.entry(name).or_default().raw.push(v);
        }
    } else {
        collect(&mut root, fields, &ctx);
    }
    Value::Object(render(root))
}

fn collect<'a>(obj: &mut Obj<'a>, src: &Map<String, Value>, ctx: &Ctx<'a>) {
    for (k, v) in src {
        field(obj, k, v, ctx);
    }
}

/// Every element of a (possibly nested) array.
fn flat(a: &[Value]) -> Vec<&Value> {
    let mut out = Vec::new();
    for e in a {
        match e {
            Value::Array(inner) => out.extend(flat(inner)),
            other => out.push(other),
        }
    }
    out
}

fn is_empty(v: &Value) -> bool {
    match v {
        Value::Null => true,
        Value::Array(a) => a.iter().all(is_empty),
        Value::Object(m) => m.is_empty(),
        _ => false,
    }
}

fn field<'a>(obj: &mut Obj<'a>, key: &str, v: &Value, ctx: &Ctx<'a>) {
    let def = ctx.props.and_then(|p| p.get(key));
    if def.is_none() {
        if ctx.subobjects {
            // `a.b` is the field `b` of the object `a`.
            if let Some((head, rest)) = key.split_once('.')
                && !head.is_empty()
                && !rest.is_empty()
            {
                return field(obj, head, &json!({ rest: v }), ctx);
            }
        } else if let Value::Object(m) = v {
            // Under `subobjects: false`, an object's fields are leaves
            // with dotted names.
            for (k, x) in m {
                field(obj, &format!("{key}.{k}"), x, ctx);
            }
            return;
        }
    }
    let slot = obj.slots.entry(key.to_string()).or_default();
    let Some(def) = def else {
        // Unmapped: kept as sent. With dynamic mapping on, an empty value
        // maps nothing and leaves nothing behind.
        let dropped = v.is_null() || (ctx.dynamic == Dynamic::True && is_empty(v));
        if !dropped {
            slot.raw.push(v.clone());
        }
        return;
    };
    slot.def = Some(def);
    match type_of(def) {
        "object" => {
            if flag(def, "enabled") == Some(false) {
                slot.raw.push(v.clone());
                return;
            }
            let child = ctx.child(def);
            match v {
                Value::Object(m) => {
                    collect(slot.object.get_or_insert_with(|| Obj::new(child.props)), m, &child)
                }
                Value::Array(a) => {
                    let keep = flag(def, "store_array_source") == Some(true)
                        || matches!(dynamic_of(def), Some(Dynamic::False | Dynamic::Runtime))
                        || ctx.dynamic == Dynamic::Runtime;
                    if keep {
                        slot.raw.push(v.clone());
                        return;
                    }
                    for e in flat(a) {
                        match e {
                            Value::Object(m) => collect(
                                slot.object.get_or_insert_with(|| Obj::new(child.props)),
                                m,
                                &child,
                            ),
                            Value::Null => {}
                            other => slot.raw.push(other.clone()),
                        }
                    }
                }
                Value::Null => {}
                other => slot.raw.push(other.clone()),
            }
        }
        "nested" => {
            let child = ctx.child(def);
            if v.is_array() && flag(def, "store_array_source") == Some(true) {
                slot.raw.push(v.clone());
                return;
            }
            let list = slot.nested.get_or_insert_with(Vec::new);
            let elems = match v {
                Value::Array(a) => flat(a),
                other => vec![other],
            };
            for e in elems {
                if let Value::Object(m) = e {
                    let mut o = Obj::new(child.props);
                    collect(&mut o, m, &child);
                    list.push(o);
                }
            }
        }
        ty => {
            if !native(ty, def) {
                if !v.is_null() {
                    slot.raw.push(v.clone());
                }
                return;
            }
            match v {
                Value::Array(a) if !whole_value(ty, a) => {
                    if ctx.dynamic == Dynamic::Runtime {
                        slot.raw.push(v.clone());
                    } else if ty == "geo_point" {
                        slot.values.extend(a.iter().cloned());
                    } else {
                        slot.values.extend(flat(a).into_iter().cloned());
                    }
                }
                _ => slot.values.push(v.clone()),
            }
        }
    }
}

/// An array that is one value of the field, not many.
fn whole_value(ty: &str, a: &[Value]) -> bool {
    match ty {
        "dense_vector" => true,
        "geo_point" => (2..=3).contains(&a.len()) && a.iter().all(Value::is_number),
        _ => false,
    }
}

fn one_or_many(mut v: Vec<Value>) -> Option<Value> {
    match v.len() {
        0 => None,
        1 => v.pop(),
        _ => Some(Value::Array(v)),
    }
}

fn render(mut obj: Obj) -> Map<String, Value> {
    // A `constant_keyword` with its value set is in every document.
    if let Some(props) = obj.props {
        for (k, def) in props {
            if type_of(def) == "constant_keyword" && def.get("value").is_some_and(|v| !v.is_null())
            {
                obj.slots.entry(k.clone()).or_default().def = Some(def);
            }
        }
    }
    let mut out = Map::new();
    for (k, slot) in obj.slots {
        let v = if !slot.raw.is_empty() {
            one_or_many(slot.raw.iter().map(expand_dots).collect())
        } else if let Some(list) = slot.nested {
            one_or_many(list.into_iter().map(|o| Value::Object(render(o))).collect())
        } else if let Some(o) = slot.object {
            Some(Value::Object(render(o))).filter(|m| m.as_object().is_some_and(|m| !m.is_empty()))
        } else if let Some(def) = slot.def {
            leaf(def, &slot.values)
        } else {
            None
        };
        if let Some(v) = v {
            out.insert(k, v);
        }
    }
    out
}

/// A value kept as sent, with dotted names turned into objects (in place,
/// as Elasticsearch re-parses it).
fn expand_dots(v: &Value) -> Value {
    match v {
        Value::Array(a) => Value::Array(a.iter().map(expand_dots).collect()),
        Value::Object(m) => {
            let mut out = Map::new();
            for (k, x) in m {
                let x = expand_dots(x);
                let (name, x) = match k.split_once('.') {
                    Some((head, rest)) if !head.is_empty() && !rest.is_empty() => {
                        (head.to_string(), expand_dots(&json!({ rest: x })))
                    }
                    _ => (k.clone(), x),
                };
                match out.get_mut(&name) {
                    // A name given twice comes back as both values.
                    Some(prev) => *prev = json!([prev.take(), x]),
                    None => {
                        out.insert(name, x);
                    }
                }
            }
            Value::Object(out)
        }
        other => other.clone(),
    }
}

/// How one field's values sort: Elasticsearch returns doc values in their
/// index order, one copy each for a sorted set.
#[derive(PartialEq, PartialOrd, Clone, Debug)]
enum Key {
    Int(i128),
    Float(f64),
    Str(String),
    Bytes(Vec<u8>),
    List(Vec<Key>),
    Range(Box<Bound>, Box<Bound>),
}

#[derive(PartialEq, PartialOrd, Clone, Debug)]
enum Bound {
    Min,
    At(Key),
    Max,
}

fn cmp_keys(a: &Key, b: &Key) -> Ordering {
    a.partial_cmp(b).unwrap_or(Ordering::Equal)
}

fn as_text(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// A leaf field's synthetic value: its indexed values in doc-values order,
/// then the values it kept without indexing.
fn leaf(def: &Value, values: &[Value]) -> Option<Value> {
    let ty = type_of(def);
    if ty == "constant_keyword"
        && let Some(v) = def.get("value").filter(|v| !v.is_null())
    {
        return Some(v.clone());
    }
    if ty == "text" {
        if stored(def) {
            return one_or_many(values.iter().filter_map(as_text).map(Value::String).collect());
        }
        let sub = text_source_field(def)?;
        return leaf(sub, values);
    }
    let null_value = def.get("null_value").filter(|v| !v.is_null());
    let values: Vec<&Value> = values
        .iter()
        .filter_map(|v| {
            let empty_bool = ty == "boolean" && v.as_str() == Some("");
            if v.is_null() || empty_bool { null_value } else { Some(v) }
        })
        .collect();
    if ty == "flattened" {
        return flattened(def, &values);
    }
    let mut indexed: Vec<(Key, Value)> = Vec::new();
    let mut kept: Vec<Value> = Vec::new();
    for v in values {
        match normalize(ty, def, v) {
            Norm::Indexed(k, out) => indexed.push((k, out)),
            Norm::Kept(out) => kept.push(out),
        }
    }
    let stored_keyword = ty == "keyword" && stored(def);
    if !stored_keyword {
        indexed.sort_by(|a, b| cmp_keys(&a.0, &b.0));
        let set = matches!(ty, "keyword" | "wildcard" | "version" | "ip" | "binary")
            || RANGES.contains(&ty);
        if set {
            indexed.dedup_by(|a, b| a.0 == b.0);
        }
    }
    one_or_many(indexed.into_iter().map(|(_, v)| v).chain(kept).collect())
}

enum Norm {
    Indexed(Key, Value),
    /// Not indexed (over `ignore_above`, or malformed): kept as sent.
    Kept(Value),
}

fn number(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.trim().parse::<f64>().ok(),
        _ => None,
    }
    .filter(|f| f.is_finite())
}

/// An integer-typed value (fractions truncated, as `coerce` does).
fn integer(v: &Value, min: i128, max: i128) -> Option<i128> {
    let n = match v {
        Value::Number(n) => n
            .as_i64()
            .map(i128::from)
            .or_else(|| n.as_u64().map(i128::from))
            .or_else(|| n.as_f64().filter(|f| f.is_finite()).map(|f| f.trunc() as i128)),
        Value::String(s) => {
            let s = s.trim();
            s.parse::<i128>().ok().or_else(|| {
                s.parse::<f64>().ok().filter(|f| f.is_finite()).map(|f| f.trunc() as i128)
            })
        }
        _ => None,
    }?;
    (min..=max).contains(&n).then_some(n)
}

/// A float's JSON form, as its shortest decimal (`1.1`, not
/// `1.100000023841858`).
fn f32_json(f: f32) -> Value {
    format!("{f}").parse::<f64>().ok().map(|d| json!(d)).unwrap_or(Value::Null)
}

/// `f` rounded to the nearest `half_float` (IEEE binary16, ties to even).
fn half(f: f32) -> f32 {
    if f == 0.0 || !f.is_finite() {
        return f;
    }
    let a = f.abs() as f64;
    if a >= 65520.0 {
        return f32::INFINITY.copysign(f);
    }
    let exp = ((a.to_bits() >> 52) & 0x7ff) as i32 - 1023;
    let ulp = if exp < -14 { 2f64.powi(-24) } else { 2f64.powi(exp - 10) };
    let q = a / ulp;
    let mut r = q.floor();
    let frac = q - r;
    if frac > 0.5 || (frac == 0.5 && r % 2.0 == 1.0) {
        r += 1.0;
    }
    ((r * ulp) as f32).copysign(f)
}

fn next_up(f: f32) -> f32 {
    if f.is_nan() || f == f32::INFINITY {
        return f;
    }
    if f == 0.0 {
        return f32::from_bits(1);
    }
    let b = f.to_bits();
    f32::from_bits(if f > 0.0 { b + 1 } else { b - 1 })
}

fn next_down(f: f32) -> f32 {
    -next_up(-f)
}

fn next_up64(f: f64) -> f64 {
    if f.is_nan() || f == f64::INFINITY {
        return f;
    }
    if f == 0.0 {
        return f64::from_bits(1);
    }
    let b = f.to_bits();
    f64::from_bits(if f > 0.0 { b + 1 } else { b - 1 })
}

fn next_down64(f: f64) -> f64 {
    -next_up64(-f)
}

/// Java's `Math.round(double)`.
fn java_round(f: f64) -> i64 {
    (f + 0.5).floor() as i64
}

fn int_bounds(ty: &str) -> (i128, i128) {
    match ty {
        "integer" | "integer_range" => (i32::MIN.into(), i32::MAX.into()),
        "short" => (i16::MIN.into(), i16::MAX.into()),
        "byte" => (i8::MIN.into(), i8::MAX.into()),
        "unsigned_long" => (0, u64::MAX.into()),
        _ => (i64::MIN.into(), i64::MAX.into()),
    }
}

fn normalize(ty: &str, def: &Value, v: &Value) -> Norm {
    normalized(ty, def, v).unwrap_or_else(|| Norm::Kept(v.clone()))
}

fn normalized(ty: &str, def: &Value, v: &Value) -> Option<Norm> {
    let indexed = |k: Key, out: Value| Some(Norm::Indexed(k, out));
    match ty {
        "keyword" | "wildcard" | "binary" | "constant_keyword" | "version" => {
            let s = as_text(v)?;
            if let Some(limit) = def.get("ignore_above").and_then(Value::as_u64)
                && s.chars().count() as u64 > limit
            {
                return Some(Norm::Kept(Value::String(s)));
            }
            let key = if ty == "version" { version_key(&s) } else { Key::Str(s.clone()) };
            indexed(key, Value::String(s))
        }
        "long" | "integer" | "short" | "byte" | "unsigned_long" => {
            let (min, max) = int_bounds(ty);
            let n = integer(v, min, max)?;
            let out = if ty == "unsigned_long" { json!(n as u64) } else { json!(n as i64) };
            indexed(Key::Int(n), out)
        }
        "double" => {
            let f = number(v)?;
            indexed(Key::Float(f), json!(f))
        }
        "float" | "half_float" => {
            let mut f = number(v)? as f32;
            if ty == "half_float" {
                f = half(f);
            }
            if !f.is_finite() {
                return None;
            }
            indexed(Key::Float(f as f64), f32_json(f))
        }
        "scaled_float" => {
            let factor = def.get("scaling_factor").and_then(number).unwrap_or(1.0);
            let f = java_round(number(v)? * factor) as f64 / factor;
            indexed(Key::Float(f), json!(f))
        }
        "boolean" => {
            let b = match v {
                Value::Bool(b) => *b,
                Value::String(s) if s == "true" => true,
                Value::String(s) if s == "false" => false,
                _ => return None,
            };
            indexed(Key::Int(b as i128), json!(b))
        }
        "date" | "date_nanos" => {
            let format = def.get("format").and_then(Value::as_str);
            let text = as_text(v).filter(|_| !v.is_boolean())?;
            let ms = dates::parse(&text, format, 0)?;
            let extra = if ty == "date_nanos" { sub_milli_nanos(&text) } else { 0 };
            let key = Key::Int(i128::from(ms) * 1_000_000 + i128::from(extra));
            match print_date(ms, extra, format, ty) {
                Some(s) => indexed(key, Value::String(s)),
                // A format this engine can't print: the value as sent.
                None => indexed(key, v.clone()),
            }
        }
        "ip" => {
            let ip = parse_ip(v.as_str()?)?;
            indexed(Key::Bytes(ip.octets().to_vec()), json!(show_ip(ip)))
        }
        "geo_point" => {
            let (lat, lon) = geo_point(v)?;
            let (la, lo) = (encode_lat(lat), encode_lon(lon));
            let key = (i64::from(la) << 32) | i64::from(lo as u32);
            indexed(
                Key::Int(key.into()),
                json!({"lat": f64::from(la) * LAT_DECODE, "lon": f64::from(lo) * LON_DECODE}),
            )
        }
        "dense_vector" => {
            let a = v.as_array()?;
            let bytes =
                matches!(def.get("element_type").and_then(Value::as_str), Some("byte" | "bit"));
            let out: Option<Vec<Value>> = a
                .iter()
                .map(|x| {
                    let f = number(x)?;
                    Some(if bytes { json!(f as i64) } else { f32_json(f as f32) })
                })
                .collect();
            indexed(Key::Int(0), Value::Array(out?))
        }
        "aggregate_metric_double" => {
            let m = v.as_object()?;
            let mut out = Map::new();
            for metric in ["min", "max", "sum", "value_count"] {
                if let Some(x) = m.get(metric) {
                    let f = number(x)?;
                    let shown = if metric == "value_count" { json!(f as i64) } else { json!(f) };
                    out.insert(metric.to_string(), shown);
                }
            }
            indexed(Key::Int(0), Value::Object(out))
        }
        t if RANGES.contains(&t) => range(t, def, v).map(|(k, out)| Norm::Indexed(k, out)),
        _ => None,
    }
}

/// A `version` field's sort key: numeric parts by number, a pre-release
/// (`1.0.0-beta`) before its release.
fn version_key(s: &str) -> Key {
    let (main, pre) = match s.split_once('-') {
        Some((m, p)) => (m, Some(p)),
        None => (s.split('+').next().unwrap_or(s), None),
    };
    let part = |p: &str| match p.parse::<u64>() {
        Ok(n) => Key::Int(n.into()),
        Err(_) => Key::Str(p.to_string()),
    };
    let mut parts: Vec<Key> = main.split('.').map(part).collect();
    parts.push(Key::Int(i128::from(pre.is_none())));
    if let Some(p) = pre {
        parts.extend(p.split('+').next().unwrap_or(p).split('.').map(part));
    }
    Key::List(parts)
}

/// The nanoseconds past the millisecond in an ISO date's fraction.
fn sub_milli_nanos(s: &str) -> i64 {
    let Some(t) = s.find(['T', 't']) else { return 0 };
    let rest = &s[t..];
    let Some(dot) = rest.find(['.', ',']) else { return 0 };
    let digits: String = rest[dot + 1..].chars().take_while(char::is_ascii_digit).collect();
    if digits.len() <= 3 {
        return 0;
    }
    format!("{:0<6}", &digits[3..digits.len().min(9)]).parse().unwrap_or(0)
}

/// A date as its field's (first) format prints it, if this engine can.
fn print_date(ms: i64, extra_nanos: i64, format: Option<&str>, ty: &str) -> Option<String> {
    let first = format.map(|f| f.split("||").next().unwrap_or(f).trim());
    match first {
        None
        | Some("strict_date_optional_time" | "date_optional_time")
        | Some("strict_date_optional_time_nanos") => {
            let base = dates::format(ms, None, 0);
            if ty != "date_nanos" {
                return Some(base);
            }
            // `strict_date_optional_time_nanos`: at least three fraction
            // digits, up to nine, trailing zeros dropped past three.
            let millis = ms.rem_euclid(1000);
            let mut frac = format!("{millis:03}{extra_nanos:06}");
            while frac.len() > 3 && frac.ends_with('0') {
                frac.pop();
            }
            let dot = base.rfind('.')?;
            Some(format!("{}.{frac}Z", &base[..dot]))
        }
        Some("epoch_millis" | "epoch_second" | "strict_date" | "date" | "year_month_day") => {
            Some(dates::format(ms, first, 0))
        }
        Some(p) => {
            let mut quoted = false;
            let printable = p.chars().all(|c| {
                if c == '\'' {
                    quoted = !quoted;
                }
                quoted || !c.is_ascii_alphabetic() || "yuMdHmsSXZ".contains(c)
            });
            printable.then(|| dates::format(ms, Some(p), 0))
        }
    }
}

fn parse_ip(s: &str) -> Option<Ipv6Addr> {
    match s.parse::<IpAddr>().ok()? {
        IpAddr::V4(a) => Some(a.to_ipv6_mapped()),
        IpAddr::V6(a) => Some(a),
    }
}

fn show_ip(ip: Ipv6Addr) -> String {
    match ip.to_ipv4_mapped() {
        Some(v4) => v4.to_string(),
        None => ip.to_string(),
    }
}

const LAT_DECODE: f64 = 1.0 / (4_294_967_296.0 / 180.0);
const LON_DECODE: f64 = 1.0 / (4_294_967_296.0 / 360.0);

/// Lucene's `GeoEncodingUtils.encodeLatitude`.
fn encode_lat(lat: f64) -> i32 {
    let lat = if lat == 90.0 { next_down64(lat) } else { lat };
    (lat / LAT_DECODE).floor() as i32
}

fn encode_lon(lon: f64) -> i32 {
    let lon = if lon == 180.0 { next_down64(lon) } else { lon };
    (lon / LON_DECODE).floor() as i32
}

/// A `geo_point` value: `{"lat", "lon"}`, GeoJSON, `"lat,lon"`, WKT
/// `POINT (lon lat)`, a geohash, or `[lon, lat]`.
fn geo_point(v: &Value) -> Option<(f64, f64)> {
    let (lat, lon) = match v {
        Value::Object(m) => {
            if let (Some(la), Some(lo)) = (m.get("lat"), m.get("lon")) {
                (number(la)?, number(lo)?)
            } else {
                let c = m.get("coordinates")?.as_array()?;
                (number(c.get(1)?)?, number(c.first()?)?)
            }
        }
        Value::Array(a) => (number(a.get(1)?)?, number(a.first()?)?),
        Value::String(s) => {
            let s = s.trim();
            if let Some((la, rest)) = s.split_once(',') {
                let lo = rest.split(',').next()?;
                (la.trim().parse().ok()?, lo.trim().parse().ok()?)
            } else if s.len() > 5 && s[..5].eq_ignore_ascii_case("point") {
                let inner = s[5..].trim().strip_prefix('(')?.strip_suffix(')')?;
                let mut it = inner.split_whitespace();
                let lo: f64 = it.next()?.parse().ok()?;
                let la: f64 = it.next()?.parse().ok()?;
                (la, lo)
            } else {
                geohash(s)?
            }
        }
        _ => return None,
    };
    ((-90.0..=90.0).contains(&lat) && (-180.0..=180.0).contains(&lon)).then_some((lat, lon))
}

/// A geohash cell's centre.
fn geohash(s: &str) -> Option<(f64, f64)> {
    const BASE32: &str = "0123456789bcdefghjkmnpqrstuvwxyz";
    if s.is_empty() || s.len() > 12 {
        return None;
    }
    let (mut lat, mut lon) = ((-90.0f64, 90.0f64), (-180.0f64, 180.0f64));
    let mut even = true;
    for c in s.chars() {
        let idx = BASE32.find(c.to_ascii_lowercase())?;
        for bit in (0..5).rev() {
            let on = idx >> bit & 1 == 1;
            let r = if even { &mut lon } else { &mut lat };
            let mid = (r.0 + r.1) / 2.0;
            if on {
                r.0 = mid;
            } else {
                r.1 = mid;
            }
            even = !even;
        }
    }
    Some(((lat.0 + lat.1) / 2.0, (lon.0 + lon.1) / 2.0))
}

/// One bound of a range value, in the range type's terms.
#[derive(Clone, Copy)]
enum Point {
    Int(i128),
    F32(f32),
    F64(f64),
    Ip(u128),
}

fn range_point(ty: &str, def: &Value, v: &Value) -> Option<Point> {
    match ty {
        "integer_range" | "long_range" => {
            let (min, max) = int_bounds(ty);
            integer(v, min, max).map(Point::Int)
        }
        "float_range" => number(v).map(|f| Point::F32(f as f32)),
        "double_range" => number(v).map(Point::F64),
        "date_range" => {
            let format = def.get("format").and_then(Value::as_str);
            dates::parse(&as_text(v)?, format, 0).map(|ms| Point::Int(ms.into()))
        }
        "ip_range" => parse_ip(v.as_str()?).map(|ip| Point::Ip(u128::from(ip))),
        _ => None,
    }
}

fn step(p: Point, up: bool) -> Point {
    match p {
        Point::Int(n) => Point::Int(if up { n + 1 } else { n - 1 }),
        Point::F32(f) => Point::F32(if up { next_up(f) } else { next_down(f) }),
        Point::F64(f) => Point::F64(if up { next_up64(f) } else { next_down64(f) }),
        Point::Ip(n) => Point::Ip(if up { n.wrapping_add(1) } else { n.wrapping_sub(1) }),
    }
}

fn point_key(p: Point) -> Key {
    match p {
        Point::Int(n) => Key::Int(n),
        Point::F32(f) => Key::Float(f.into()),
        Point::F64(f) => Key::Float(f),
        Point::Ip(n) => Key::Bytes(n.to_be_bytes().to_vec()),
    }
}

fn point_json(ty: &str, def: &Value, p: Point) -> Value {
    match p {
        Point::Int(n) if ty == "date_range" => {
            let format = def.get("format").and_then(Value::as_str);
            let ms = n as i64;
            print_date(ms, 0, format, "date").map_or(json!(ms), Value::String)
        }
        Point::Int(n) => json!(n as i64),
        Point::F32(f) => f32_json(f),
        Point::F64(f) => json!(f),
        Point::Ip(n) => json!(show_ip(Ipv6Addr::from(n))),
    }
}

/// A range value as closed bounds (`gt: 1` on an integer range is
/// `gte: 2`); an open end is `null`.
fn range(ty: &str, def: &Value, v: &Value) -> Option<(Key, Value)> {
    let (lo, hi) = match v {
        Value::String(s) if ty == "ip_range" => {
            let (addr, bits) = s.split_once('/')?;
            let ip = parse_ip(addr)?;
            let mut bits: u32 = bits.parse().ok()?;
            if ip.to_ipv4_mapped().is_some() && addr.contains('.') {
                bits += 96;
            }
            if bits > 128 {
                return None;
            }
            let mask = if bits == 0 { 0 } else { u128::MAX << (128 - bits) };
            let n = u128::from(ip) & mask;
            (Some(Point::Ip(n)), Some(Point::Ip(n | !mask)))
        }
        Value::Object(m) => {
            let (mut lo, mut hi) = (None, None);
            for (k, x) in m {
                let at = |x: &Value| range_point(ty, def, x);
                match k.as_str() {
                    "gte" | "from" if !x.is_null() => lo = Some(at(x)?),
                    "gt" if !x.is_null() => lo = Some(step(at(x)?, true)),
                    "lte" | "to" if !x.is_null() => hi = Some(at(x)?),
                    "lt" if !x.is_null() => hi = Some(step(at(x)?, false)),
                    "gte" | "gt" | "lte" | "lt" | "from" | "to" | "format" => {}
                    _ => return None,
                }
            }
            (lo, hi)
        }
        _ => return None,
    };
    let key = Key::Range(
        Box::new(lo.map_or(Bound::Min, |p| Bound::At(point_key(p)))),
        Box::new(hi.map_or(Bound::Max, |p| Bound::At(point_key(p)))),
    );
    let show = |p: Option<Point>| p.map_or(Value::Null, |p| point_json(ty, def, p));
    Some((key, json!({"gte": show(lo), "lte": show(hi)})))
}

/// A `flattened` field: every leaf as a string under its dotted key,
/// sorted and de-duplicated per key, then nested back into objects.
fn flattened(def: &Value, values: &[&Value]) -> Option<Value> {
    fn walk(
        prefix: &str,
        v: &Value,
        null: Option<&Value>,
        out: &mut BTreeMap<String, Vec<String>>,
    ) {
        match v {
            Value::Object(m) => {
                for (k, x) in m {
                    let key = if prefix.is_empty() { k.clone() } else { format!("{prefix}.{k}") };
                    walk(&key, x, null, out);
                }
            }
            Value::Array(a) => a.iter().for_each(|x| walk(prefix, x, null, out)),
            Value::Null => {
                if let Some(n) = null.and_then(as_text)
                    && !prefix.is_empty()
                {
                    out.entry(prefix.to_string()).or_default().push(n);
                }
            }
            other => {
                if !prefix.is_empty()
                    && let Some(s) = as_text(other)
                {
                    out.entry(prefix.to_string()).or_default().push(s);
                }
            }
        }
    }
    let null = def.get("null_value").filter(|v| !v.is_null());
    let mut keyed = BTreeMap::new();
    for v in values {
        walk("", v, null, &mut keyed);
    }
    let limit = def.get("ignore_above").and_then(Value::as_u64);
    // A value over `ignore_above` keeps the whole field as sent.
    if let Some(n) = limit
        && keyed.values().flatten().any(|s| s.chars().count() as u64 > n)
    {
        return one_or_many(values.iter().map(|v| expand_dots(v)).collect());
    }
    let mut out = Map::new();
    for (key, vals) in keyed {
        let (mut indexed, kept): (Vec<String>, Vec<String>) =
            vals.into_iter().partition(|s| limit.is_none_or(|n| s.chars().count() as u64 <= n));
        indexed.sort();
        indexed.dedup();
        let Some(v) = one_or_many(indexed.into_iter().chain(kept).map(Value::String).collect())
        else {
            continue;
        };
        let parts: Vec<&str> = key.split('.').collect();
        let mut node = &mut out;
        for p in &parts[..parts.len() - 1] {
            let next = node.entry(p.to_string()).or_insert_with(|| json!({}));
            if !next.is_object() {
                *next = json!({});
            }
            node = next.as_object_mut().unwrap();
        }
        node.entry(parts[parts.len() - 1].to_string()).or_insert(v);
    }
    (!out.is_empty()).then_some(Value::Object(out))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn synth(mappings: Value, src: Value) -> Value {
        source(&mappings, &src)
    }

    #[test]
    fn leaf_arrays_sort_and_keywords_deduplicate() {
        let m = json!({"properties": {"k": {"type": "keyword"}, "l": {"type": "long"},
                                      "b": {"type": "boolean"}}});
        assert_eq!(
            synth(m, json!({"k": ["b", "a", "b"], "l": [3, "1", 1.7, 1], "b": ["true", false]})),
            json!({"b": [false, true], "k": ["a", "b"], "l": [1, 1, 1, 3]})
        );
    }

    #[test]
    fn dotted_names_and_object_arrays_merge() {
        let m = json!({"properties": {"o": {"properties": {"a": {"type": "long"},
                                                            "b": {"type": "keyword"}}}}});
        assert_eq!(
            synth(m, json!({"o": [{"a": 2, "b": "y"}, {"a": 1}], "o.b": "x"})),
            json!({"o": {"a": [1, 2], "b": ["x", "y"]}})
        );
    }

    #[test]
    fn ignore_above_values_follow_indexed_ones() {
        let m = json!({"properties": {"k": {"type": "keyword", "ignore_above": 3}}});
        assert_eq!(
            synth(m, json!({"k": ["long one", "b", "a"]})),
            json!({"k": ["a", "b", "long one"]})
        );
    }

    #[test]
    fn numbers_take_their_mapped_type() {
        let m = json!({"properties": {"h": {"type": "half_float"}, "f": {"type": "float"},
                                      "s": {"type": "scaled_float", "scaling_factor": 100}}});
        assert_eq!(
            synth(m, json!({"h": 1.1, "f": [2.5, 1.1], "s": 1.234})),
            json!({"f": [1.1, 2.5], "h": 1.0996094, "s": 1.23})
        );
    }

    #[test]
    fn unmapped_and_disabled_values_come_back_as_sent() {
        let m = json!({"dynamic": false, "properties": {
            "m": {"type": "keyword"}, "d": {"enabled": false}}});
        assert_eq!(
            synth(m, json!({"m": ["b", "a"], "u": [3, 1], "d": {"z": 1, "a.b": 2}, "n": null})),
            json!({"d": {"z": 1, "a": {"b": 2}}, "m": ["a", "b"], "u": [3, 1]})
        );
    }

    #[test]
    fn nested_objects_stay_separate() {
        let m = json!({"properties": {"n": {"type": "nested", "properties": {
            "a": {"type": "long"}}}}});
        assert_eq!(
            synth(m, json!({"n": [{"a": [3, 1]}, {}, {"a": 2}]})),
            json!({"n": [{"a": [1, 3]}, {}, {"a": 2}]})
        );
    }

    #[test]
    fn ranges_become_closed_bounds() {
        let m = json!({"properties": {"r": {"type": "integer_range"},
                                      "d": {"type": "date_range"},
                                      "i": {"type": "ip_range"}}});
        assert_eq!(
            synth(
                m,
                json!({"r": [{"gt": 4, "lt": 8}, {"gt": 4, "lt": 7}],
                       "d": {"gt": "2017-09-01", "lt": "2017-09-05"},
                       "i": "74.125.227.0/25"})
            ),
            json!({"d": {"gte": "2017-09-01T00:00:00.001Z", "lte": "2017-09-04T23:59:59.999Z"},
                   "i": {"gte": "74.125.227.0", "lte": "74.125.227.127"},
                   "r": [{"gte": 5, "lte": 6}, {"gte": 5, "lte": 7}]})
        );
    }

    #[test]
    fn flattened_leaves_are_strings() {
        let m = json!({"properties": {"f": {"type": "flattened"}}});
        assert_eq!(
            synth(m, json!({"f": {"list": ["789", "1011", "1213", "1213"], "z": {"n": [3, 1]}}})),
            json!({"f": {"list": ["1011", "1213", "789"], "z": {"n": ["1", "3"]}}})
        );
    }

    #[test]
    fn unsupported_fields_are_named() {
        let m = json!({"_source": {"mode": "synthetic"},
                       "properties": {"t": {"type": "text"}, "b": {"type": "boolean",
                                                                  "doc_values": false}}});
        assert_eq!(
            unsupported(&m, &json!({})).as_deref(),
            Some(
                "field [b] of type [boolean] doesn't support synthetic source because it doesn't \
                 have doc values"
            )
        );
        let ok =
            json!({"properties": {"t": {"type": "text", "fields": {"raw": {"type": "keyword"}}}}});
        assert_eq!(unsupported(&ok, &json!({})), None);
    }
}
