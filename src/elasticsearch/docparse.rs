//! Indexing a document the way Elasticsearch's document parser does:
//!
//! - new fields are mapped by the `dynamic` setting in force for their
//!   object (`true`, `false`, `strict`, `runtime`, inherited downwards),
//!   through `dynamic_templates` (and a bulk item's named
//!   `dynamic_templates`) or the defaults (date / numeric detection);
//!   dotted names are objects unless `subobjects: false`;
//! - every value of a mapped field is checked against its type: a value
//!   the field can't take fails the document with a
//!   `document_parsing_exception` citing where the value is in the body
//!   -- unless the field has `ignore_malformed` (then the field is listed
//!   in the document's `_ignored` metadata field instead).
//!
//! `_ignored` itself isn't stored: it is worked out again from the source
//! and the mapping wherever it's read (`ignored_fields`).

use serde_json::{Map, Value, json};

use super::dates;
use super::jsonpos;
use super::search::raw_values;

/// What a document is parsed with besides its mapping.
pub struct Ctx<'a> {
    pub id: &'a str,
    /// The source as sent (positions in errors refer to it).
    pub raw: Option<&'a str>,
    /// The index settings (`index.mapping.ignore_malformed`, `coerce`).
    pub settings: &'a Value,
    /// A bulk item's `dynamic_templates`: field path -> template name.
    pub templates: Option<&'a Map<String, Value>>,
}

type Failure = (u16, Value);

#[derive(Clone, Copy, PartialEq)]
enum Dynamic {
    True,
    False,
    Strict,
    Runtime,
}

fn dynamic_of(node: &Value, inherited: Dynamic) -> Dynamic {
    match node.get("dynamic") {
        Some(Value::Bool(true)) => Dynamic::True,
        Some(Value::Bool(false)) => Dynamic::False,
        Some(Value::String(s)) => match s.as_str() {
            "true" => Dynamic::True,
            "false" => Dynamic::False,
            "strict" => Dynamic::Strict,
            "runtime" => Dynamic::Runtime,
            _ => inherited,
        },
        _ => inherited,
    }
}

fn join(path: &str, k: &str) -> String {
    if path.is_empty() { k.to_string() } else { format!("{path}.{k}") }
}

fn is_object_def(def: &Value) -> bool {
    match def.get("type").and_then(Value::as_str) {
        Some("object" | "nested") => true,
        None => true,
        Some(_) => false,
    }
}

fn subobjects_off(node: &Value) -> bool {
    matches!(node.get("subobjects"), Some(Value::Bool(false)))
        || node.get("subobjects").and_then(Value::as_str) == Some("false")
}

/// Adds `v` under `k` in `m`, merging objects (`{"a.b": 1, "a": {"c": 2}}`).
fn put(m: &mut Map<String, Value>, k: String, v: Value) {
    match (m.get_mut(&k), v) {
        (Some(Value::Object(a)), Value::Object(b)) => {
            for (bk, bv) in b {
                put(a, bk, bv);
            }
        }
        (Some(existing), v) => {
            let mut all = match existing.take() {
                Value::Array(a) => a,
                other => vec![other],
            };
            match v {
                Value::Array(b) => all.extend(b),
                other => all.push(other),
            }
            *existing = Value::Array(all);
        }
        (None, v) => {
            m.insert(k, v);
        }
    }
}

/// Every leaf under `v` as dotted names (an object under
/// `subobjects: false` maps its sub-objects' fields as leaves).
fn flatten_into(prefix: &str, v: &Value, out: &mut Map<String, Value>) {
    match v {
        Value::Object(m) if !m.is_empty() => {
            for (k, x) in m {
                flatten_into(&join(prefix, k), x, out);
            }
        }
        other => put(out, prefix.to_string(), other.clone()),
    }
}

/// One object level of `src` as the parser walks it: a dotted key
/// `a.b` is field `a` holding `{"b": ...}` (merged with any other `a`),
/// unless the object (`node`) has `subobjects: false` -- then dotted
/// names are leaf names, and sub-objects' fields become dotted leaves.
fn split_level(src: &Map<String, Value>, node: &Value) -> Map<String, Value> {
    let flat = subobjects_off(node);
    let mut out = Map::new();
    for (k, v) in src {
        if flat {
            flatten_into(k, v, &mut out);
            continue;
        }
        match k.split_once('.') {
            Some((first, rest)) if !first.is_empty() && !rest.is_empty() => {
                put(&mut out, first.to_string(), json!({ rest: v }));
            }
            _ => put(&mut out, k.clone(), v.clone()),
        }
    }
    out
}

/// The value's kind as `match_mapping_type` names it, and the type
/// `{dynamic_type}` (and the default mapping) gives it.
fn detect(v: &Value, root: &Value) -> Option<(&'static str, &'static str, Option<String>)> {
    match v {
        Value::String(s) => {
            if root.get("date_detection").and_then(Value::as_bool).unwrap_or(true)
                && let Some(fmt) = date_format_of(s, root)
            {
                return Some(("date", "date", fmt));
            }
            if root.get("numeric_detection").and_then(Value::as_bool).unwrap_or(false) {
                if s.parse::<i64>().is_ok() {
                    return Some(("long", "long", None));
                }
                if s.parse::<f64>().is_ok_and(f64::is_finite) {
                    return Some(("double", "float", None));
                }
            }
            Some(("string", "text", None))
        }
        Value::Number(n) if n.is_f64() => Some(("double", "float", None)),
        Value::Number(_) => Some(("long", "long", None)),
        Value::Bool(_) => Some(("boolean", "boolean", None)),
        Value::Object(_) => Some(("object", "object", None)),
        Value::Array(a) => a.iter().find(|x| !x.is_null()).and_then(|x| detect(x, root)),
        Value::Null => None,
    }
}

/// `strict_date_optional_time`'s shape: `yyyy-MM-dd`, optionally
/// followed by `THH:mm[:ss[.fff]]` and a zone.
fn looks_like_date(s: &str) -> bool {
    let b = s.as_bytes();
    let digits =
        |r: std::ops::Range<usize>| r.clone().all(|i| b.get(i).is_some_and(u8::is_ascii_digit));
    if b.len() < 10
        || !digits(0..4)
        || b[4] != b'-'
        || !digits(5..7)
        || b[7] != b'-'
        || !digits(8..10)
    {
        return false;
    }
    b.len() == 10
        || (b[10] == b'T' && b.len() >= 16 && digits(11..13) && b[13] == b':' && digits(14..16))
}

/// The format a string is detected as a date with (`Some(None)`: the
/// default `strict_date_optional_time`, which the mapping doesn't name).
fn date_format_of(s: &str, root: &Value) -> Option<Option<String>> {
    let formats: Vec<String> = match root.get("dynamic_date_formats").and_then(Value::as_array) {
        Some(a) => a.iter().filter_map(Value::as_str).map(str::to_string).collect(),
        None => vec!["strict_date_optional_time".into(), "yyyy/MM/dd HH:mm:ss||yyyy/MM/dd".into()],
    };
    for f in formats {
        let ok = if f == "strict_date_optional_time" || f == "date_optional_time" {
            looks_like_date(s)
        } else {
            f.split("||").any(|one| strict_parse(s, one))
        };
        if ok {
            return Some(if f == "strict_date_optional_time" {
                None
            } else {
                Some(format!("{f}||epoch_millis"))
            });
        }
    }
    None
}

/// A string matching a Java pattern exactly (same length, digits where
/// the pattern has letters), then parsed with it.
fn strict_parse(s: &str, pattern: &str) -> bool {
    let p: Vec<char> = pattern.chars().filter(|c| *c != '\'').collect();
    let c: Vec<char> = s.chars().collect();
    if p.len() != c.len() {
        return false;
    }
    let shape = p.iter().zip(&c).all(|(pc, sc)| {
        if pc.is_ascii_alphabetic() && !matches!(pc, 'Z' | 'X') {
            sc.is_ascii_digit()
        } else {
            true
        }
    });
    shape && dates::parse(s, Some(pattern), 0).is_some()
}

fn patterns(v: Option<&Value>) -> Vec<String> {
    match v {
        Some(Value::String(s)) => vec![s.clone()],
        Some(Value::Array(a)) => a.iter().filter_map(Value::as_str).map(str::to_string).collect(),
        _ => vec![],
    }
}

fn simple_match(pat: &str, s: &str) -> bool {
    match pat.split_once('*') {
        None => pat == s,
        Some((pre, rest)) => {
            s.starts_with(pre) && {
                let tail = &s[pre.len()..];
                rest.is_empty() || (0..=tail.len()).any(|i| simple_match(rest, &tail[i..]))
            }
        }
    }
}

fn name_match(t: &Value, key: &str, s: &str) -> bool {
    let regex = t.get("match_pattern").and_then(Value::as_str) == Some("regex");
    patterns(t.get(key)).iter().any(|p| {
        if regex {
            regex_lite::Regex::new(&format!("^(?:{p})$")).is_ok_and(|r| r.is_match(s))
        } else {
            simple_match(p, s)
        }
    })
}

/// Whether template `t` applies to field `name` at `path` holding a
/// `kind` value. A template without any condition is only used by name.
fn template_matches(t: &Value, name: &str, path: &str, kind: &str) -> bool {
    let conditions = [
        "match",
        "unmatch",
        "path_match",
        "path_unmatch",
        "match_mapping_type",
        "unmatch_mapping_type",
    ];
    if !conditions.iter().any(|c| t.get(*c).is_some()) {
        return false;
    }
    let types = patterns(t.get("match_mapping_type"));
    if types.is_empty() {
        // Objects are matched only when asked for by type.
        if kind == "object" {
            return false;
        }
    } else if !types.iter().any(|m| m == "*" || m == kind) {
        return false;
    }
    if patterns(t.get("unmatch_mapping_type")).iter().any(|m| m == kind) {
        return false;
    }
    (t.get("match").is_none() || name_match(t, "match", name))
        && !(t.get("unmatch").is_some() && name_match(t, "unmatch", name))
        && (t.get("path_match").is_none() || name_match(t, "path_match", path))
        && !(t.get("path_unmatch").is_some() && name_match(t, "path_unmatch", path))
}

/// `{name}` and `{dynamic_type}` filled in, keys and values alike.
fn fill(v: &Value, name: &str, dynamic_type: &str) -> Value {
    let sub = |s: &str| s.replace("{name}", name).replace("{dynamic_type}", dynamic_type);
    match v {
        Value::String(s) => Value::String(sub(s)),
        Value::Object(m) => {
            Value::Object(m.iter().map(|(k, x)| (sub(k), fill(x, name, dynamic_type))).collect())
        }
        Value::Array(a) => Value::Array(a.iter().map(|x| fill(x, name, dynamic_type)).collect()),
        other => other.clone(),
    }
}

/// A new field's mapping, or a runtime field for it.
enum NewField {
    Mapped(Value),
    Runtime(Value),
}

fn default_runtime_type(kind: &str) -> &'static str {
    match kind {
        "string" => "keyword",
        "long" => "long",
        "double" => "double",
        "boolean" => "boolean",
        "date" => "date",
        _ => "keyword",
    }
}

fn parse_error(reason: &str) -> Failure {
    (
        400,
        json!({"error": {"root_cause": [{"type": "document_parsing_exception", "reason": reason}],
            "type": "document_parsing_exception", "reason": reason}, "status": 400}),
    )
}

/// The mapping a new field at `path` (named `name`) gets for `v`.
fn new_field(
    name: &str,
    path: &str,
    v: &Value,
    dynamic: Dynamic,
    root: &Value,
    ctx: &Ctx,
    at: &dyn Fn(&str, bool) -> String,
) -> Result<Option<NewField>, Failure> {
    let Some((kind, dyn_type, date_format)) = detect(v, root) else { return Ok(None) };
    let templates: Vec<(&String, &Value)> = root
        .get("dynamic_templates")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|t| t.as_object().and_then(|o| o.iter().next()))
        .collect();
    let named = ctx.templates.and_then(|t| t.get(path)).and_then(Value::as_str);
    let template = match named {
        Some(want) => match templates.iter().find(|(n, _)| n.as_str() == want) {
            Some((_, t)) => Some(*t),
            None => {
                return Err(parse_error(&format!(
                    "{} Can't find dynamic template for dynamic template name [{want}] of field [{path}]",
                    at(path, false)
                )));
            }
        },
        None => {
            templates.iter().find(|(_, t)| template_matches(t, name, path, kind)).map(|(_, t)| *t)
        }
    };
    if let Some(t) = template {
        if let Some(rt) = t.get("runtime") {
            if kind == "object" {
                return Ok(Some(NewField::Mapped(json!({"properties": {}}))));
            }
            let mut def = fill(rt, name, default_runtime_type(kind));
            if def.get("type").is_none() {
                def["type"] = json!(default_runtime_type(kind));
            }
            return Ok(Some(NewField::Runtime(def)));
        }
        let mut def = fill(t.get("mapping").unwrap_or(&json!({})), name, dyn_type);
        if def.get("type").is_none() && kind != "object" {
            def["type"] = json!(dyn_type);
            if let Some(f) = &date_format {
                def["format"] = json!(f);
            }
        }
        return Ok(Some(NewField::Mapped(def)));
    }
    if kind == "object" {
        return Ok(Some(NewField::Mapped(json!({"properties": {}}))));
    }
    if dynamic == Dynamic::Runtime {
        let mut def = json!({"type": default_runtime_type(kind)});
        if let Some(f) = &date_format {
            def["format"] = json!(f);
        }
        return Ok(Some(NewField::Runtime(def)));
    }
    if let Value::Array(a) = v
        && let Some(def) = super::vectors::dynamic_def(a)
    {
        return Ok(Some(NewField::Mapped(def)));
    }
    Ok(Some(NewField::Mapped(match kind {
        "string" => {
            json!({"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}})
        }
        "date" => match date_format {
            Some(f) => json!({"type": "date", "format": f}),
            None => json!({"type": "date"}),
        },
        _ => json!({ "type": dyn_type }),
    })))
}

/// Dynamic mapping of the fields of `src` (an object at `path`) into
/// `node` (that object's mapping).
#[allow(clippy::too_many_arguments)]
fn map_object(
    node: &mut Value,
    src: &Map<String, Value>,
    path: &str,
    dynamic: Dynamic,
    root: &Value,
    runtime: &mut Map<String, Value>,
    ctx: &Ctx,
    at: &dyn Fn(&str, bool) -> String,
) -> Result<(), Failure> {
    let src = split_level(src, node);
    for (k, v) in &src {
        let full = join(path, k);
        let has = node.get("properties").and_then(|p| p.get(k)).is_some();
        if has {
            let child = &mut node["properties"][k];
            if !is_object_def(child) || child.get("enabled") == Some(&json!(false)) {
                continue;
            }
            let d = dynamic_of(child, dynamic);
            for o in objects(v) {
                map_object(child, o, &full, d, root, runtime, ctx, at)?;
            }
            continue;
        }
        if root.get("runtime").and_then(|r| r.get(&full)).is_some() || runtime.contains_key(&full) {
            continue;
        }
        if v.is_null() || v.as_array().is_some_and(|a| a.iter().all(Value::is_null)) {
            continue;
        }
        match dynamic {
            Dynamic::False => continue,
            Dynamic::Strict => {
                let within = if path.is_empty() { "_doc" } else { path };
                let reason = format!(
                    "{} mapping set to strict, dynamic introduction of [{k}] within [{within}] is not allowed",
                    at(&full, false)
                );
                return Err((
                    400,
                    json!({"error": {"root_cause": [{"type": "strict_dynamic_mapping_exception", "reason": reason}],
                        "type": "strict_dynamic_mapping_exception", "reason": reason}, "status": 400}),
                ));
            }
            Dynamic::True | Dynamic::Runtime => {}
        }
        match new_field(k, &full, v, dynamic, root, ctx, at)? {
            None => {}
            Some(NewField::Runtime(def)) => {
                runtime.insert(full.clone(), def);
            }
            Some(NewField::Mapped(def)) => {
                if node.get("properties").is_none() {
                    node["properties"] = json!({});
                }
                node["properties"][k] = def;
                let child = &mut node["properties"][k];
                if is_object_def(child) && child.get("enabled") != Some(&json!(false)) {
                    let d = dynamic_of(child, dynamic);
                    for o in objects(v) {
                        map_object(child, o, &full, d, root, runtime, ctx, at)?;
                    }
                    if child.get("type").is_none()
                        && child
                            .get("properties")
                            .and_then(Value::as_object)
                            .is_some_and(Map::is_empty)
                    {
                        *child = json!({"type": "object"});
                    }
                }
            }
        }
    }
    Ok(())
}

/// The objects of a value: itself, or those in an array.
fn objects(v: &Value) -> Vec<&Map<String, Value>> {
    match v {
        Value::Object(m) => vec![m],
        Value::Array(a) => a.iter().flat_map(objects).collect(),
        _ => vec![],
    }
}

/// A malformed value: the `caused_by` to report, and whether
/// `ignore_malformed` can swallow it (any value but an object).
pub struct Bad {
    cause: Value,
    ignorable: bool,
}

fn iae(reason: &str) -> Bad {
    Bad { cause: json!({"type": "illegal_argument_exception", "reason": reason}), ignorable: true }
}

fn java_type(ty: &str) -> &'static str {
    match ty {
        "long" => "Long",
        "integer" => "Integer",
        "short" => "Short",
        "byte" => "Byte",
        "double" => "Double",
        "float" | "half_float" => "Float",
        "unsigned_long" => "UnsignedLong",
        _ => "Double",
    }
}

fn int_range(ty: &str) -> Option<(f64, f64)> {
    match ty {
        "long" => Some((i64::MIN as f64, i64::MAX as f64)),
        "integer" => Some((i32::MIN as f64, i32::MAX as f64)),
        "short" => Some((i16::MIN as f64, i16::MAX as f64)),
        "byte" => Some((i8::MIN as f64, i8::MAX as f64)),
        "unsigned_long" => Some((0.0, u64::MAX as f64)),
        _ => None,
    }
}

const NUMERIC: &[&str] = &[
    "long",
    "integer",
    "short",
    "byte",
    "double",
    "float",
    "half_float",
    "scaled_float",
    "unsigned_long",
];

const KEYWORD_LIKE: &[&str] = &[
    "keyword",
    "constant_keyword",
    "wildcard",
    "text",
    "match_only_text",
    "search_as_you_type",
    "version",
    "ip",
];

/// The `[line:col]` where a parser complains about a JSON token.
type At<'a> = &'a dyn Fn(&str, bool) -> String;

fn check_number(
    ty: &str,
    def: &Value,
    settings: &Value,
    v: &Value,
    after: &str,
) -> Result<(), Bad> {
    let coerce = def
        .get("coerce")
        .and_then(Value::as_bool)
        .unwrap_or_else(|| settings["index"]["mapping"]["coerce"].as_str() != Some("false"));
    let integral = int_range(ty).is_some();
    let n: f64 = match v {
        Value::Number(n) => {
            if let Some((lo, hi)) = int_range(ty)
                && let Some(i) =
                    n.as_i64().map(|i| i as f64).or_else(|| n.as_u64().map(|u| u as f64))
                && (i < lo || i > hi)
                && matches!(ty, "integer" | "long")
            {
                let name = if ty == "integer" { "int" } else { "long" };
                let reason = format!(
                    "{after} Numeric value ({n}) out of range of {name} ({} - {})",
                    lo as i64, hi as i64
                );
                return Err(Bad {
                    cause: json!({"type": "x_content_parse_exception", "reason": reason,
                        "caused_by": {"type": "input_coercion_exception", "reason": reason[after.len() + 1..]}}),
                    ignorable: true,
                });
            }
            let f = n.as_f64().unwrap_or(0.0);
            if integral && f.fract() != 0.0 && !coerce {
                return Err(iae(&format!(
                    "{n} cannot be converted to {} without data loss",
                    java_type(ty)
                )));
            }
            f
        }
        Value::String(s) => {
            if s.is_empty() {
                return Ok(());
            }
            if !coerce {
                return Err(iae(&format!("{} value passed as String", java_type(ty))));
            }
            let plain = s
                .bytes()
                .all(|b| b.is_ascii_digit() || matches!(b, b'.' | b'-' | b'+' | b'e' | b'E'));
            match s.parse::<f64>() {
                Ok(f) if plain && f.is_finite() => f,
                _ => return Err(iae(&format!("For input string: \"{s}\""))),
            }
        }
        Value::Bool(b) => {
            let token = if *b { "VALUE_TRUE" } else { "VALUE_FALSE" };
            let reason = format!(
                "{after} Current token ({token}) not numeric, can not use numeric value accessors"
            );
            return Err(Bad {
                cause: json!({"type": "x_content_parse_exception", "reason": reason,
                    "caused_by": {"type": "json_parse_exception", "reason": reason[after.len() + 1..]}}),
                ignorable: true,
            });
        }
        Value::Object(_) => {
            return Err(Bad {
                cause: json!({"type": "illegal_argument_exception", "reason": "Cannot parse object as number"}),
                ignorable: false,
            });
        }
        _ => return Ok(()),
    };
    if let Some((lo, hi)) = int_range(ty) {
        let t = n.trunc();
        if t < lo || t > hi {
            return Err(iae(&format!(
                "Value [{}] is out of range for {}",
                fmt_num(n),
                match ty {
                    "integer" => "an integer",
                    "short" => "a short",
                    "byte" => "a byte",
                    "unsigned_long" => "an unsigned long",
                    _ => "a long",
                }
            )));
        }
    }
    Ok(())
}

fn fmt_num(n: f64) -> String {
    if n.fract() == 0.0 && n.abs() < 1e15 { format!("{}", n as i64) } else { n.to_string() }
}

fn check_date(ty: &str, def: &Value, v: &Value) -> Result<(), Bad> {
    let format = def.get("format").and_then(Value::as_str);
    let ms = match v {
        Value::String(s) => match dates::parse(s, format, 0) {
            Some(ms) => ms,
            None => {
                return Err(Bad {
                    cause: json!({"type": "illegal_argument_exception",
                        "reason": format!("failed to parse date field [{s}] with format [{}]",
                            format.unwrap_or("strict_date_optional_time||epoch_millis")),
                        "caused_by": {"type": "date_time_parse_exception",
                            "reason": "Failed to parse with all enclosed parsers"}}),
                    ignorable: true,
                });
            }
        },
        Value::Number(n) => n.as_f64().unwrap_or(0.0) as i64,
        Value::Object(_) => {
            return Err(Bad {
                cause: json!({"type": "illegal_argument_exception", "reason": "Cannot parse object as date"}),
                ignorable: false,
            });
        }
        Value::Bool(b) => return Err(iae(&format!("failed to parse date field [{b}]"))),
        _ => return Ok(()),
    };
    if ty == "date_nanos" {
        let shown = match v {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        if ms < 0 {
            return Err(iae(&format!(
                "date[{shown}] is before the epoch in 1970 and cannot be stored in nanosecond resolution"
            )));
        }
        if ms > 9_223_372_036_854 {
            return Err(iae(&format!(
                "date[{shown}] is after 2262-04-11T23:47:16.854775807 and cannot be stored in nanosecond resolution"
            )));
        }
    }
    Ok(())
}

/// Checks one value of a leaf field of type `ty`. `start` / `after` are
/// the parser positions of the value (where it starts, just past it).
fn check_leaf(
    def: &Value,
    settings: &Value,
    v: &Value,
    start: &str,
    after: &str,
) -> Result<(), Bad> {
    let ty = def.get("type").and_then(Value::as_str).unwrap_or("object");
    if NUMERIC.contains(&ty) {
        return check_number(ty, def, settings, v, after);
    }
    match ty {
        "date" | "date_nanos" => check_date(ty, def, v),
        "boolean" => match v {
            Value::Bool(_) | Value::Null => Ok(()),
            Value::String(s) if matches!(s.as_str(), "true" | "false" | "") => Ok(()),
            Value::String(s) => Err(iae(&format!(
                "Failed to parse value [{s}] as only [true] or [false] are allowed."
            ))),
            Value::Number(n) => {
                let token = if n.is_f64() { "VALUE_NUMBER_FLOAT" } else { "VALUE_NUMBER_INT" };
                let reason = format!("{after} Current token ({token}) not of boolean type");
                Err(Bad {
                    cause: json!({"type": "x_content_parse_exception", "reason": reason,
                        "caused_by": {"type": "json_parse_exception", "reason": reason[after.len() + 1..]}}),
                    ignorable: true,
                })
            }
            _ => Err(Bad {
                cause: json!({"type": "illegal_argument_exception",
                    "reason": format!("Expected text at {} but found START_OBJECT", &start[1..start.len() - 1])}),
                ignorable: false,
            }),
        },
        t if KEYWORD_LIKE.contains(&t) => match v {
            Value::Object(_) => Err(Bad {
                cause: json!({"type": "illegal_argument_exception",
                    "reason": format!("Expected text at {} but found START_OBJECT", &start[1..start.len() - 1])}),
                ignorable: false,
            }),
            Value::String(s) if t == "ip" && s.parse::<std::net::IpAddr>().is_err() => {
                Err(iae(&format!("'{s}' is not an IP string literal.")))
            }
            Value::Number(_) | Value::Bool(_) if t == "ip" => {
                Err(iae(&format!("'{v}' is not an IP string literal.")))
            }
            _ => Ok(()),
        },
        _ => Ok(()),
    }
}

/// How Java prints a parsed value in "Preview of field's value".
fn preview(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Object(m) => format!(
            "{{{}}}",
            m.iter().map(|(k, x)| format!("{k}={}", preview(x))).collect::<Vec<_>>().join(", ")
        ),
        Value::Array(a) => format!("[{}]", a.iter().map(preview).collect::<Vec<_>>().join(", ")),
        other => other.to_string(),
    }
}

fn ignore_malformed(def: &Value, settings: &Value) -> bool {
    def.get("ignore_malformed").and_then(Value::as_bool).unwrap_or_else(|| {
        let s = &settings["index"]["mapping"]["ignore_malformed"];
        s.as_bool() == Some(true) || s.as_str() == Some("true")
    })
}

/// Field types whose value is a whole array or object (not a list of
/// values to check one by one), left to their own handling.
fn whole_value(ty: &str) -> bool {
    matches!(
        ty,
        "geo_point"
            | "geo_shape"
            | "point"
            | "shape"
            | "dense_vector"
            | "sparse_vector"
            | "rank_features"
            | "flattened"
            | "completion"
            | "join"
            | "percolator"
            | "histogram"
            | "aggregate_metric_double"
            | "integer_range"
            | "long_range"
            | "float_range"
            | "double_range"
            | "date_range"
            | "ip_range"
            | "alias"
            | "semantic_text"
    )
}

fn leaf_values(v: &Value, out: &mut Vec<Value>) {
    match v {
        Value::Array(a) => a.iter().for_each(|x| leaf_values(x, out)),
        Value::Null => {}
        other => out.push(other.clone()),
    }
}

/// Checks every value of `src` (at `path`) against the mapping `node`.
fn validate(
    node: &Value,
    src: &Map<String, Value>,
    path: &str,
    ctx: &Ctx,
    pos: &Positions,
) -> Result<(), Failure> {
    let src = split_level(src, node);
    for (k, v) in &src {
        let full = join(path, k);
        let Some(def) = node.get("properties").and_then(|p| p.get(k)) else { continue };
        if is_object_def(def) {
            if def.get("enabled") == Some(&json!(false)) {
                continue;
            }
            let mut vals = vec![];
            leaf_values(v, &mut vals);
            for (n, x) in vals.iter().enumerate() {
                match x {
                    Value::Object(m) => validate(def, m, &full, ctx, pos)?,
                    _ => {
                        return Err(parse_error(&format!(
                            "{} object mapping for [{full}] tried to parse field [{k}] as object, but found a concrete value",
                            pos.at(&full, n, false)
                        )));
                    }
                }
            }
            continue;
        }
        let ty = def.get("type").and_then(Value::as_str).unwrap_or("");
        if whole_value(ty) {
            continue;
        }
        let mut vals = vec![];
        leaf_values(v, &mut vals);
        for (n, x) in vals.iter().enumerate() {
            let start = pos.at(&full, n, false);
            let after = pos.after(&full, n);
            if let Err(bad) = check_leaf(def, ctx.settings, x, &start, &after) {
                if bad.ignorable && ignore_malformed(def, ctx.settings) {
                    continue;
                }
                let at = pos.at(&full, n, x.is_object());
                let reason = format!(
                    "{at} failed to parse field [{full}] of type [{ty}] in document with id '{}'. Preview of field's value: '{}'",
                    ctx.id,
                    preview(x)
                );
                let mut e = parse_error(&reason);
                e.1["error"]["caused_by"] = bad.cause;
                return Err(e);
            }
        }
    }
    Ok(())
}

/// Value positions in the raw source.
struct Positions {
    all: Vec<jsonpos::ValuePos>,
}

impl Positions {
    /// `[line:col]` of the `n`-th value at `path`: where it starts, or
    /// (`end`) where it ends -- an object's closing brace.
    fn at(&self, path: &str, n: usize, end: bool) -> String {
        match jsonpos::nth(&self.all, path, n).or_else(|| jsonpos::nth(&self.all, path, 0)) {
            Some(p) if end => format!("[{}:{}]", p.end.0, p.end.1),
            Some(p) => format!("[{}:{}]", p.start.0, p.start.1),
            None => "[1:1]".into(),
        }
    }

    /// Just past the `n`-th value at `path` (where a token's reader
    /// stands once it has read it).
    fn after(&self, path: &str, n: usize) -> String {
        match jsonpos::nth(&self.all, path, n) {
            Some(p) => format!("[{}:{}]", p.end.0, p.end.1 + 1),
            None => "[1:1]".into(),
        }
    }
}

/// Parses document `src` into `mappings`: dynamic mapping applied (the
/// mapping changes only when the document is accepted) and every value
/// checked.
pub fn parse(mappings: &mut Value, src: &Value, ctx: &Ctx) -> Result<(), Failure> {
    let Some(obj) = src.as_object() else { return Ok(()) };
    let serialized;
    let raw = match ctx.raw {
        Some(r) => r,
        None => {
            serialized = serde_json::to_string(src).unwrap_or_default();
            &serialized
        }
    };
    let pos = Positions { all: jsonpos::scan(raw) };
    let at = |path: &str, end: bool| pos.at(path, 0, end);
    let mut next = mappings.clone();
    let root = next.clone();
    let mut runtime = Map::new();
    let dynamic = dynamic_of(&root, Dynamic::True);
    map_object(&mut next, obj, "", dynamic, &root, &mut runtime, ctx, &at)?;
    if !runtime.is_empty() {
        if next.get("runtime").is_none() {
            next["runtime"] = json!({});
        }
        for (k, v) in runtime {
            next["runtime"][k] = v;
        }
    }
    validate(&next, obj, "", ctx, &pos)?;
    *mappings = next;
    Ok(())
}

/// Every mapped leaf (multi-fields included): (full name, source path,
/// definition).
fn leaves(props: &Value, prefix: &str, out: &mut Vec<(String, String, Value)>) {
    let Some(m) = props.as_object() else { return };
    for (k, def) in m {
        let name = join(prefix, k);
        if is_object_def(def) {
            if let Some(p) = def.get("properties") {
                leaves(p, &name, out);
            }
            continue;
        }
        if let Some(Value::Object(subs)) = def.get("fields") {
            for (sk, sd) in subs {
                out.push((format!("{name}.{sk}"), name.clone(), sd.clone()));
            }
        }
        out.push((name.clone(), name, def.clone()));
    }
}

/// Whether one value of field `def` was left out of the index:
/// malformed under `ignore_malformed`, or longer than `ignore_above`.
fn ignored_value(def: &Value, settings: &Value, v: &Value) -> bool {
    let ty = def.get("type").and_then(Value::as_str).unwrap_or("");
    if let Some(limit) = def.get("ignore_above").and_then(Value::as_u64)
        && let Some(s) = v.as_str()
        && s.chars().count() as u64 > limit
    {
        return true;
    }
    if whole_value(ty) {
        return false;
    }
    check_leaf(def, settings, v, "[1:1]", "[1:1]").is_err_and(|b| b.ignorable)
        && ignore_malformed(def, settings)
}

/// The document's `_ignored` metadata field: the fields (sorted) some
/// value of which wasn't indexed.
pub fn ignored_fields(mappings: &Value, settings: &Value, source: &Value) -> Vec<String> {
    ignored_values(mappings, settings, source).into_iter().map(|(f, _)| f).collect()
}

/// Each ignored field with its ignored values (`ignored_field_values`).
pub fn ignored_values(
    mappings: &Value,
    settings: &Value,
    source: &Value,
) -> Vec<(String, Vec<Value>)> {
    let mut all = vec![];
    if let Some(p) = mappings.get("properties") {
        leaves(p, "", &mut all);
    }
    let mut out: Vec<(String, Vec<Value>)> = vec![];
    for (name, path, def) in all {
        let mut vals = vec![];
        for v in raw_values(source, &path) {
            leaf_values(v, &mut vals);
        }
        let bad: Vec<Value> =
            vals.into_iter().filter(|v| ignored_value(&def, settings, v)).collect();
        if !bad.is_empty() {
            out.push((name, bad));
        }
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx<'a>(raw: &'a str, settings: &'a Value) -> Ctx<'a> {
        Ctx { id: "1", raw: Some(raw), settings, templates: None }
    }

    #[test]
    fn malformed_values_fail_or_are_ignored() {
        let settings = json!({});
        let mut m = json!({"properties": {"ip": {"type": "ip"}, "n": {"type": "integer", "ignore_malformed": true}}});
        let raw = r#"{"ip":"garbage"}"#;
        let (status, e) =
            parse(&mut m, &serde_json::from_str(raw).unwrap(), &ctx(raw, &settings)).unwrap_err();
        assert_eq!(status, 400);
        assert_eq!(
            e["error"]["reason"],
            "[1:7] failed to parse field [ip] of type [ip] in document with id '1'. Preview of field's value: 'garbage'"
        );
        let raw = r#"{"n":"zz"}"#;
        let src: Value = serde_json::from_str(raw).unwrap();
        assert!(parse(&mut m, &src, &ctx(raw, &settings)).is_ok());
        assert_eq!(ignored_fields(&m, &settings, &src), vec!["n"]);
        let raw = r#"{"n":{"object":"wow"}}"#;
        let (_, e) =
            parse(&mut m, &serde_json::from_str(raw).unwrap(), &ctx(raw, &settings)).unwrap_err();
        assert!(
            e["error"]["reason"].as_str().unwrap().starts_with("[1:22] failed to parse field [n]")
        );
    }

    #[test]
    fn dynamic_settings_and_templates() {
        let settings = json!({});
        let mut m = json!({"dynamic": "strict", "properties": {"a": {"type": "keyword"}}});
        let raw = r#"{"a":"x","zz":1}"#;
        let (_, e) =
            parse(&mut m, &serde_json::from_str(raw).unwrap(), &ctx(raw, &settings)).unwrap_err();
        assert_eq!(
            e["error"]["reason"],
            "[1:15] mapping set to strict, dynamic introduction of [zz] within [_doc] is not allowed"
        );
        let mut m = json!({"dynamic_templates": [
            {"strs": {"match_mapping_type": "string", "match": "k_*", "mapping": {"type": "keyword"}}},
            {"all": {"match": "d_*", "mapping": {"type": "{dynamic_type}"}}}]});
        let raw = r#"{"k_a":"x","d_f":1.5,"o.p":"2020-01-01","t":"2015/09/02"}"#;
        parse(&mut m, &serde_json::from_str(raw).unwrap(), &ctx(raw, &settings)).unwrap();
        assert_eq!(m["properties"]["k_a"], json!({"type": "keyword"}));
        assert_eq!(m["properties"]["d_f"], json!({"type": "float"}));
        assert_eq!(m["properties"]["o"]["properties"]["p"], json!({"type": "date"}));
        assert_eq!(m["properties"]["t"]["format"], "yyyy/MM/dd HH:mm:ss||yyyy/MM/dd||epoch_millis");
    }
}
