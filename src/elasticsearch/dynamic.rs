//! Dynamic mapping: how the fields a document brings, that its index
//! doesn't map yet, get mapped. The `dynamic` setting (inherited by
//! sub-objects) decides whether they are mapped (`true`), ignored
//! (`false`), refused (`strict`) or added as runtime fields (`runtime`);
//! `dynamic_templates` pick the mapping of a matching new field.

use serde_json::{Map, Value, json};

use super::vectors;

/// The JSON type of a new field, as `match_mapping_type` names it.
fn detected_type(v: &Value) -> Option<&'static str> {
    match v {
        Value::Object(_) => Some("object"),
        Value::String(s) if super::engine::looks_like_date(s) => Some("date"),
        Value::String(_) => Some("string"),
        Value::Bool(_) => Some("boolean"),
        Value::Number(n) if n.is_i64() || n.is_u64() => Some("long"),
        Value::Number(_) => Some("double"),
        Value::Array(a) => a.iter().find(|x| !x.is_null()).and_then(detected_type),
        Value::Null => None,
    }
}

/// The type a `{dynamic_type}` placeholder (or a template mapping
/// without `type`) stands for.
fn default_type(detected: &str) -> &'static str {
    match detected {
        "string" => "text",
        "long" => "long",
        "double" => "float",
        "boolean" => "boolean",
        "date" => "date",
        "binary" => "binary",
        _ => "object",
    }
}

/// The type a field gets under `dynamic: runtime`.
fn runtime_type(detected: &str) -> &'static str {
    match detected {
        "string" => "keyword",
        "double" => "double",
        other => default_type(other),
    }
}

fn setting(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.to_ascii_lowercase()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

fn strings(v: Option<&Value>) -> Vec<String> {
    match v {
        Some(Value::String(s)) => vec![s.clone()],
        Some(Value::Array(a)) => a.iter().filter_map(Value::as_str).map(String::from).collect(),
        _ => Vec::new(),
    }
}

fn simple_match(pattern: &str, s: &str) -> bool {
    let parts: Vec<&str> = pattern.split('*').collect();
    if parts.len() == 1 {
        return pattern == s;
    }
    let mut rest = s;
    for (i, p) in parts.iter().enumerate() {
        if i == 0 {
            let Some(r) = rest.strip_prefix(p) else { return false };
            rest = r;
        } else if i == parts.len() - 1 {
            return rest.ends_with(p);
        } else if let Some(at) = rest.find(p) {
            rest = &rest[at + p.len()..];
        } else {
            return false;
        }
    }
    true
}

/// Whether a dynamic template applies to the field `name` at `path`.
fn template_matches(t: &Value, name: &str, path: &str, detected: &str) -> bool {
    let regex = t.get("match_pattern").and_then(Value::as_str) == Some("regex");
    let test = |pattern: &str, s: &str| {
        if regex {
            regex_lite::Regex::new(pattern).is_ok_and(|r| r.is_match(s))
        } else {
            simple_match(pattern, s)
        }
    };
    let types = strings(t.get("match_mapping_type"));
    if !types.is_empty() && !types.iter().any(|m| m == "*" || m == detected) {
        return false;
    }
    if strings(t.get("unmatch_mapping_type")).iter().any(|m| m == detected) {
        return false;
    }
    let all = |key: &str, s: &str, want: bool| {
        let ps = strings(t.get(key));
        ps.is_empty() || ps.iter().any(|p| test(p, s)) == want
    };
    all("match", name, true)
        && (strings(t.get("unmatch")).is_empty() || all("unmatch", name, false))
        && all("path_match", path, true)
        && (strings(t.get("path_unmatch")).is_empty() || all("path_unmatch", path, false))
}

/// `{name}` and `{dynamic_type}` filled in throughout a template mapping.
fn substitute(v: &Value, name: &str, dynamic_type: &str) -> Value {
    match v {
        Value::String(s) => {
            json!(s.replace("{name}", name).replace("{dynamic_type}", dynamic_type))
        }
        Value::Array(a) => {
            Value::Array(a.iter().map(|x| substitute(x, name, dynamic_type)).collect())
        }
        Value::Object(m) => Value::Object(
            m.iter().map(|(k, x)| (k.clone(), substitute(x, name, dynamic_type))).collect(),
        ),
        other => other.clone(),
    }
}

struct Ctx {
    templates: Vec<Value>,
    runtime: Map<String, Value>,
}

/// What a matching dynamic template makes of a new field.
enum Made {
    Field(Value),
    Runtime(Value),
}

impl Ctx {
    /// The mapping the first matching template gives a new field.
    fn template(&self, name: &str, path: &str, detected: &str) -> Option<Made> {
        let t = self.templates.iter().find(|t| template_matches(t, name, path, detected))?;
        if let Some(rt) = t.get("runtime") {
            let mut m = substitute(rt, name, default_type(detected));
            if m.get("type").is_none() {
                m["type"] = json!(runtime_type(detected));
            }
            return Some(Made::Runtime(m));
        }
        let mut m = substitute(t.get("mapping")?, name, default_type(detected));
        if m.get("type").is_none() && detected != "object" {
            m["type"] = json!(default_type(detected));
        }
        Some(Made::Field(m))
    }
}

/// Maps the new fields of `src` into `m` (the root mapping). `Err` is a
/// `strict` object's refusal: (field, the object it was in).
pub(super) fn apply(m: &mut Value, src: &Value) -> Result<(), (String, String)> {
    let mut ctx = Ctx {
        templates: m
            .get("dynamic_templates")
            .and_then(Value::as_array)
            .map(|a| a.iter().filter_map(|t| t.as_object()?.values().next().cloned()).collect())
            .unwrap_or_default(),
        runtime: Map::new(),
    };
    let dynamic = m.get("dynamic").and_then(setting).unwrap_or_else(|| "true".into());
    fields(m, src, "", &dynamic, &mut ctx)?;
    if !ctx.runtime.is_empty() {
        if !m.get("runtime").is_some_and(Value::is_object) {
            m["runtime"] = json!({});
        }
        for (k, v) in ctx.runtime {
            m["runtime"][k] = v;
        }
    }
    Ok(())
}

fn fields(
    node: &mut Value,
    src: &Value,
    path: &str,
    dynamic: &str,
    ctx: &mut Ctx,
) -> Result<(), (String, String)> {
    let Some(src_fields) = src.as_object() else { return Ok(()) };
    for (k, v) in src_fields {
        let full = if path.is_empty() { k.clone() } else { format!("{path}.{k}") };
        // An object (or an array of them) maps its own fields.
        let objects: Vec<&Value> = match v {
            Value::Object(_) => vec![v],
            Value::Array(a) => a.iter().filter(|e| e.is_object()).collect(),
            _ => Vec::new(),
        };
        if let Some(existing) = node.get_mut("properties").and_then(|p| p.get_mut(k)) {
            let ty = existing.get("type").and_then(Value::as_str);
            if existing.get("properties").is_some() || matches!(ty, Some("object" | "nested")) {
                let child = existing.get("dynamic").and_then(setting).unwrap_or(dynamic.into());
                for o in objects {
                    fields(existing, o, &full, &child, ctx)?;
                }
            }
            continue;
        }
        // No value (null, `[]`) maps nothing yet.
        let Some(detected) = detected_type(v) else { continue };
        let is_object = !objects.is_empty() && v.as_array().is_none_or(|a| a[0].is_object());
        match dynamic {
            "false" => continue,
            "strict" => {
                let within = if path.is_empty() { "_doc".to_string() } else { path.to_string() };
                return Err((k.clone(), within));
            }
            _ => {}
        }
        let template = ctx.template(k, &full, if is_object { "object" } else { detected });
        let template = match template {
            Some(Made::Runtime(def)) if !is_object => {
                ctx.runtime.entry(full.clone()).or_insert(def);
                continue;
            }
            Some(Made::Field(def)) => Some(def),
            _ => None,
        };
        if dynamic == "runtime" && template.is_none() {
            if is_object {
                // Runtime fields have no objects: their names are paths.
                let mut scratch = json!({});
                for o in objects {
                    fields(&mut scratch, o, &full, dynamic, ctx)?;
                }
            } else {
                ctx.runtime.entry(full.clone()).or_insert(json!({"type": runtime_type(detected)}));
            }
            continue;
        }
        if !node.get("properties").is_some_and(Value::is_object) {
            // An object shows no `type` once it has fields.
            if node.get("type").and_then(Value::as_str) == Some("object") && !path.is_empty() {
                node.as_object_mut().unwrap().remove("type");
            }
            node["properties"] = json!({});
        }
        if is_object {
            let mut def = template.unwrap_or_else(|| json!({}));
            let child = def.get("dynamic").and_then(setting).unwrap_or(dynamic.into());
            for o in objects {
                fields(&mut def, o, &full, &child, ctx)?;
            }
            if def.get("properties").is_none() && def.get("type").is_none() {
                def["type"] = json!("object");
            }
            node["properties"][k] = def;
            continue;
        }
        if let Some(def) = template {
            node["properties"][k] = def;
            continue;
        }
        if let Value::Array(a) = v
            && let Some(def) = vectors::dynamic_def(a)
        {
            node["properties"][k] = def;
            continue;
        }
        node["properties"][k] = match default_type(detected) {
            "text" => {
                json!({"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}})
            }
            ty => json!({"type": ty}),
        };
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn templates_and_dynamic_settings() {
        let mut m = json!({
            "dynamic_templates": [
                {"kw": {"match_mapping_type": "string", "mapping": {"type": "keyword", "time_series_dimension": true}}},
                {"x": {"match": "x_*", "mapping": {"type": "{dynamic_type}", "meta": {"n": "{name}"}}}}
            ],
            "properties": {"off": {"type": "object", "dynamic": false}}
        });
        apply(&mut m, &json!({"s": "a", "x_c": 1.5, "off": {"q": 1}, "d": "2021-01-01"})).unwrap();
        assert_eq!(m["properties"]["s"], json!({"type": "keyword", "time_series_dimension": true}));
        assert_eq!(m["properties"]["x_c"], json!({"type": "float", "meta": {"n": "x_c"}}));
        assert_eq!(m["properties"]["d"], json!({"type": "date"}));
        assert!(m["properties"]["off"].get("properties").is_none());

        let mut m = json!({"dynamic": "runtime"});
        apply(&mut m, &json!({"s": "a", "o": {"n": 1}})).unwrap();
        assert_eq!(m["runtime"], json!({"s": {"type": "keyword"}, "o.n": {"type": "long"}}));

        let mut m = json!({"dynamic": "strict", "properties": {"o": {"properties": {}}}});
        assert_eq!(apply(&mut m, &json!({"o": {"y": 1}})), Err(("y".into(), "o".into())));
    }
}
