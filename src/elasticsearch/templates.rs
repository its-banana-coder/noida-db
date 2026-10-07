//! Index templates: composable index templates (`_index_template`),
//! component templates (`_component_template`) and legacy templates
//! (`_template`), their CRUD and simulate APIs, and the settings, mappings
//! and aliases a new index gets from them.
//!
//! As in Elasticsearch, a new index takes the single highest-priority
//! matching composable template (its component templates in `composed_of`
//! order, then its own `template`); only when none matches do legacy
//! templates apply, all matching ones merged by ascending `order`.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::collections::HashMap;

use super::engine::{apply_settings, error, expand_dotted, merge, normalize_alias, parse_json};

#[derive(Default, Serialize, Deserialize)]
pub struct Templates {
    /// Composable index templates (persisted under the old field name).
    #[serde(default, rename = "templates")]
    pub index: HashMap<String, Value>,
    #[serde(default, rename = "component_templates")]
    pub component: HashMap<String, Value>,
    #[serde(default, rename = "legacy_templates")]
    pub legacy: HashMap<String, Value>,
}

/// What a new index starts from.
#[derive(Default)]
pub struct Resolved {
    pub settings: Value,
    pub mappings: Value,
    pub aliases: Map<String, Value>,
}

pub fn glob(pat: &str, name: &str) -> bool {
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

/// Whether some index name could match both patterns.
fn overlaps(a: &str, b: &str) -> bool {
    if !a.contains('*') {
        return glob(b, a);
    }
    if !b.contains('*') {
        return glob(a, b);
    }
    let (pa, pb) = (a.split('*').next().unwrap_or(""), b.split('*').next().unwrap_or(""));
    let (sa, sb) = (a.rsplit('*').next().unwrap_or(""), b.rsplit('*').next().unwrap_or(""));
    (pa.starts_with(pb) || pb.starts_with(pa)) && (sa.ends_with(sb) || sb.ends_with(sa))
}

fn patterns(t: &Value) -> Vec<String> {
    match t.get("index_patterns") {
        Some(Value::String(p)) => p.split(',').map(|s| s.trim().to_string()).collect(),
        Some(Value::Array(a)) => a.iter().filter_map(Value::as_str).map(str::to_string).collect(),
        _ => Vec::new(),
    }
}

fn priority(t: &Value) -> i64 {
    t.get("priority").and_then(Value::as_i64).unwrap_or(0)
}

fn order(t: &Value) -> i64 {
    t.get("order").and_then(Value::as_i64).unwrap_or(0)
}

fn matches(t: &Value, index: &str) -> bool {
    patterns(t).iter().any(|p| glob(p, index))
}

fn list(p: &[String]) -> String {
    format!("[{}]", p.join(", "))
}

/// A template body's `settings` normalized the way they are stored.
fn normalized_settings(s: Option<&Value>) -> Option<Value> {
    let s = s?;
    let mut out = json!({});
    apply_settings(&mut out, s);
    Some(out)
}

/// `template` (settings/mappings/aliases/...) with settings normalized.
fn normalize_template_section(t: &Value) -> Value {
    let mut t = t.clone();
    if let Some(s) = normalized_settings(t.get("settings")) {
        t["settings"] = s;
    }
    if let Some(Value::Object(a)) = t.get("aliases") {
        let a: Map<String, Value> =
            a.iter().map(|(k, v)| (k.clone(), normalize_alias(v))).collect();
        t["aliases"] = Value::Object(a);
    }
    t
}

fn apply_section(r: &mut Resolved, sec: &Value) {
    if let Some(s) = sec.get("settings") {
        apply_settings(&mut r.settings, s);
    }
    if let Some(m) = sec.get("mappings") {
        merge(&mut r.mappings, m.clone());
    }
    if let Some(Value::Object(a)) = sec.get("aliases") {
        for (k, v) in a {
            r.aliases.insert(k.clone(), v.clone());
        }
    }
}

impl Templates {
    /// The winning composable template for `index`: highest priority.
    fn winner(&self, index: &str) -> Option<(&String, &Value)> {
        let mut m: Vec<(&String, &Value)> =
            self.index.iter().filter(|(_, t)| matches(t, index)).collect();
        m.sort_by(|a, b| priority(b.1).cmp(&priority(a.1)).then(a.0.cmp(b.0)));
        m.into_iter().next()
    }

    fn compose(&self, t: &Value) -> Resolved {
        let mut r = Resolved { settings: json!({}), mappings: json!({}), ..Default::default() };
        for c in t.get("composed_of").and_then(Value::as_array).into_iter().flatten() {
            if let Some(ct) = c.as_str().and_then(|n| self.component.get(n))
                && let Some(sec) = ct.get("template")
            {
                apply_section(&mut r, sec);
            }
        }
        if let Some(sec) = t.get("template") {
            apply_section(&mut r, sec);
        }
        // Dotted field names expand only once everything is composed: a
        // `subobjects: false` in one component governs fields from another.
        r.mappings = expand_dotted(&r.mappings);
        r
    }

    /// What a new index named `index` gets from the templates; `None` when
    /// no template matches.
    pub fn resolve(&self, index: &str) -> Option<Resolved> {
        if let Some((_, t)) = self.winner(index) {
            return Some(self.compose(t));
        }
        let mut legacy: Vec<(&String, &Value)> =
            self.legacy.iter().filter(|(_, t)| matches(t, index)).collect();
        if legacy.is_empty() {
            return None;
        }
        legacy.sort_by(|a, b| order(a.1).cmp(&order(b.1)).then(a.0.cmp(b.0)));
        let mut r = Resolved { settings: json!({}), mappings: json!({}), ..Default::default() };
        for (_, t) in legacy {
            apply_section(&mut r, t);
        }
        r.mappings = expand_dotted(&r.mappings);
        Some(r)
    }
}

fn not_found(reason: &str) -> (u16, Value) {
    (404, error("resource_not_found_exception", reason, 404))
}

fn template_missing(name: &str) -> (u16, Value) {
    (
        404,
        error("index_template_missing_exception", &format!("index_template [{name}] missing"), 404),
    )
}

fn bad(kind: &str, reason: &str) -> (u16, Value) {
    (400, error(kind, reason, 400))
}

/// Names in `map` selected by `expr` (a name, `*`, `_all`, a wildcard, or
/// a comma-separated list), sorted.
fn select(map: &HashMap<String, Value>, expr: &str) -> Result<Vec<String>, String> {
    let mut out = Vec::new();
    for part in expr.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        if part == "_all" || part.contains('*') {
            out.extend(map.keys().filter(|k| part == "_all" || glob(part, k)).cloned());
        } else if map.contains_key(part) {
            out.push(part.to_string());
        } else {
            return Err(part.to_string());
        }
    }
    out.sort();
    out.dedup();
    Ok(out)
}

fn validation(reason: &str) -> (u16, Value) {
    bad("action_request_validation_exception", &format!("Validation Failed: 1: {reason};"))
}

fn truthy(q: &HashMap<String, String>, k: &str) -> bool {
    q.get(k).is_some_and(|v| v.is_empty() || v == "true")
}

impl Templates {
    fn index_template_out(t: &Value) -> Value {
        let mut out = Map::new();
        out.insert("index_patterns".into(), json!(patterns(t)));
        if let Some(v) = t.get("template") {
            out.insert("template".into(), v.clone());
        }
        out.insert("composed_of".into(), t.get("composed_of").cloned().unwrap_or(json!([])));
        for k in [
            "priority",
            "version",
            "_meta",
            "data_stream",
            "allow_auto_create",
            "ignore_missing_component_templates",
            "deprecated",
        ] {
            if let Some(v) = t.get(k) {
                out.insert(k.into(), v.clone());
            }
        }
        Value::Object(out)
    }

    fn validate_index_template(&self, name: &str, t: &Value) -> Result<(), (u16, Value)> {
        let pats = patterns(t);
        if pats.is_empty() {
            return Err(bad("illegal_argument_exception", "Required [index_patterns]"));
        }
        let ignore: Vec<&str> = t
            .get("ignore_missing_component_templates")
            .and_then(Value::as_array)
            .map(|a| a.iter().filter_map(Value::as_str).collect())
            .unwrap_or_default();
        let missing: Vec<String> = t
            .get("composed_of")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .filter(|c| !self.component.contains_key(*c) && !ignore.contains(c))
            .map(str::to_string)
            .collect();
        if !missing.is_empty() {
            let reason = format!(
                "index_template [{name}] invalid, cause [index template [{name}] specifies a \
                 missing component templates {} that does not exist]",
                list(&missing)
            );
            return Err(bad("invalid_index_template_exception", &reason));
        }
        let p = priority(t);
        let mut clash: Vec<(&String, Vec<String>)> = self
            .index
            .iter()
            .filter(|(n, o)| {
                n.as_str() != name
                    && priority(o) == p
                    && patterns(o).iter().any(|a| pats.iter().any(|b| overlaps(a, b)))
            })
            .map(|(n, o)| (n, patterns(o)))
            .collect();
        clash.sort();
        if !clash.is_empty() {
            let names: Vec<String> = clash.iter().map(|(n, _)| n.to_string()).collect();
            let with: Vec<String> =
                clash.iter().map(|(n, ps)| format!("{n} => {}", list(ps))).collect();
            let reason = format!(
                "index template [{name}] has index patterns {} matching patterns from existing \
                 templates [{}] with patterns ({}) that have the same priority [{p}], multiple \
                 index templates may not match during index creation, please use a different \
                 priority",
                list(&pats),
                names.join(","),
                with.join(",")
            );
            return Err(bad("illegal_argument_exception", &reason));
        }
        Ok(())
    }

    fn normalize_index_template(t: &Value) -> Value {
        let mut t = t.clone();
        t["index_patterns"] = json!(patterns(&t));
        if let Some(sec) = t.get("template") {
            t["template"] = normalize_template_section(sec);
        }
        t
    }

    /// `_index_template[/<name>]`, `_index_template/_simulate[/<name>]`,
    /// `_index_template/_simulate_index/<index>`.
    pub fn index_api(
        &mut self,
        method: &str,
        segments: &[&str],
        q: &HashMap<String, String>,
        body: &[u8],
    ) -> (u16, Value) {
        match (segments.get(1).copied(), method) {
            (Some("_simulate_index"), "POST") => {
                let Some(index) = segments.get(2) else {
                    return validation("index name is missing");
                };
                let Some(req) = parse_json(body) else { return bad("parse_exception", "bad body") };
                self.simulate(index, (!body.is_empty()).then_some(("", &req)))
            }
            (Some("_simulate"), "POST") => {
                let Some(req) = parse_json(body) else { return bad("parse_exception", "bad body") };
                let name = segments.get(2).copied().unwrap_or("");
                let t = if body.is_empty() {
                    match self.index.get(name) {
                        Some(t) => t.clone(),
                        None => {
                            return not_found(&format!(
                                "index template matching [{name}] not found"
                            ));
                        }
                    }
                } else {
                    Self::normalize_index_template(&req)
                };
                let mut r = self.compose(&t);
                if r.settings.as_object().is_some_and(|m| m.is_empty()) {
                    r.settings = json!({});
                }
                let pats = patterns(&t);
                let overlapping = self.overlapping(&pats, name);
                (200, json!({"template": resolved_out(r), "overlapping": overlapping}))
            }
            (None, "GET") => {
                let all = select(&self.index, "*").unwrap_or_default();
                (200, json!({"index_templates": self.index_list(&all)}))
            }
            (Some(name), "GET" | "HEAD") => {
                if name.contains(',') {
                    return bad("illegal_argument_exception", "template name may not contain ','");
                }
                match select(&self.index, name) {
                    Ok(names) if names.is_empty() && name.contains('*') => {
                        (404, json!({"index_templates": []}))
                    }
                    Ok(names) => (200, json!({"index_templates": self.index_list(&names)})),
                    Err(n) => not_found(&format!("index template matching [{n}] not found")),
                }
            }
            (Some(name), "PUT" | "POST") => {
                let Some(req) = parse_json(body) else {
                    return bad("parse_exception", "request body is required");
                };
                if truthy(q, "create") && self.index.contains_key(name) {
                    return bad(
                        "illegal_argument_exception",
                        &format!("index template [{name}] already exists"),
                    );
                }
                let t = Self::normalize_index_template(&req);
                if let Err(e) = self.validate_index_template(name, &t) {
                    return e;
                }
                self.index.insert(name.to_string(), t);
                (200, json!({"acknowledged": true}))
            }
            (Some(name), "DELETE") => match select(&self.index, name) {
                Ok(names) if names.is_empty() => template_missing(name),
                Ok(names) => {
                    for n in names {
                        self.index.remove(&n);
                    }
                    (200, json!({"acknowledged": true}))
                }
                Err(n) => template_missing(&n),
            },
            _ => (405, error("method_not_allowed_exception", "Incorrect HTTP method", 405)),
        }
    }

    fn index_list(&self, names: &[String]) -> Vec<Value> {
        names
            .iter()
            .filter_map(|n| {
                self.index
                    .get(n)
                    .map(|t| json!({"name": n, "index_template": Self::index_template_out(t)}))
            })
            .collect()
    }

    /// Other templates (legacy first, then composable) whose patterns
    /// overlap `pats`, excluding `skip`.
    fn overlapping(&self, pats: &[String], skip: &str) -> Vec<Value> {
        let hit = |t: &Value| patterns(t).iter().any(|a| pats.iter().any(|b| overlaps(a, b)));
        let mut out = Vec::new();
        for map in [&self.legacy, &self.index] {
            let mut names: Vec<&String> =
                map.iter().filter(|(n, t)| n.as_str() != skip && hit(t)).map(|(n, _)| n).collect();
            names.sort();
            for n in names {
                out.push(json!({"name": n, "index_patterns": patterns(&map[n])}));
            }
        }
        out
    }

    /// `_simulate_index/<index>`: the template an index named `index`
    /// would get, optionally with an extra (not stored) template.
    fn simulate(&mut self, index: &str, extra: Option<(&str, &Value)>) -> (u16, Value) {
        const PROBE: &str = "\u{0}simulated";
        if let Some((_, t)) = extra {
            let t = Self::normalize_index_template(t);
            self.index.insert(PROBE.to_string(), t);
        }
        let out = match self.winner(index) {
            Some((wname, t)) => {
                let wname = wname.clone();
                let r = self.compose(t);
                let overlapping: Vec<Value> = {
                    let mut v = Vec::new();
                    for map in [&self.legacy, &self.index] {
                        let mut names: Vec<&String> = map
                            .iter()
                            .filter(|(n, t)| {
                                **n != wname && n.as_str() != PROBE && matches(t, index)
                            })
                            .map(|(n, _)| n)
                            .collect();
                        names.sort();
                        for n in names {
                            v.push(json!({"name": n, "index_patterns": patterns(&map[n])}));
                        }
                    }
                    v
                };
                (200, json!({"template": resolved_out(r), "overlapping": overlapping}))
            }
            None => (200, json!({})),
        };
        self.index.remove(PROBE);
        out
    }

    /// `_component_template[/<name>]`.
    pub fn component_api(
        &mut self,
        method: &str,
        segments: &[&str],
        q: &HashMap<String, String>,
        body: &[u8],
    ) -> (u16, Value) {
        let name = segments.get(1).copied();
        match (name, method) {
            (None, "GET") => (200, json!({"component_templates": self.component_list("*")})),
            (Some(name), "GET" | "HEAD") => match select(&self.component, name) {
                Ok(names) if names.is_empty() && name.contains('*') => {
                    (404, json!({"component_templates": []}))
                }
                Ok(_) => (200, json!({"component_templates": self.component_list(name)})),
                Err(n) => not_found(&format!("component template matching [{n}] not found")),
            },
            (Some(name), "PUT" | "POST") => {
                let Some(req) = parse_json(body) else {
                    return bad("parse_exception", "request body is required");
                };
                if req.get("template").is_none() {
                    return validation("template is missing");
                }
                if truthy(q, "create") && self.component.contains_key(name) {
                    return bad(
                        "illegal_argument_exception",
                        &format!("component template [{name}] already exists"),
                    );
                }
                let mut t = req.clone();
                t["template"] = normalize_template_section(&req["template"]);
                self.component.insert(name.to_string(), t);
                (200, json!({"acknowledged": true}))
            }
            (Some(name), "DELETE") => {
                let names = match select(&self.component, name) {
                    Ok(n) if n.is_empty() => return not_found(name),
                    Ok(n) => n,
                    Err(n) => return not_found(&n),
                };
                let mut users: Vec<&String> = self
                    .index
                    .iter()
                    .filter(|(_, t)| {
                        t.get("composed_of")
                            .and_then(Value::as_array)
                            .is_some_and(|a| a.iter().any(|c| names.iter().any(|n| c == n)))
                    })
                    .map(|(n, _)| n)
                    .collect();
                users.sort();
                if !users.is_empty() {
                    let users: Vec<String> = users.into_iter().cloned().collect();
                    return bad(
                        "illegal_argument_exception",
                        &format!(
                            "component templates {} cannot be removed as they are still in use \
                             by index templates {}",
                            list(&names),
                            list(&users)
                        ),
                    );
                }
                for n in names {
                    self.component.remove(&n);
                }
                (200, json!({"acknowledged": true}))
            }
            _ => (405, error("method_not_allowed_exception", "Incorrect HTTP method", 405)),
        }
    }

    fn component_list(&self, expr: &str) -> Vec<Value> {
        select(&self.component, expr)
            .unwrap_or_default()
            .iter()
            .map(|n| json!({"name": n, "component_template": self.component[n]}))
            .collect()
    }

    /// `_template[/<name>]` (legacy templates).
    pub fn legacy_api(
        &mut self,
        method: &str,
        segments: &[&str],
        q: &HashMap<String, String>,
        body: &[u8],
    ) -> (u16, Value) {
        let name = segments.get(1).copied();
        let render = |names: &[String], map: &HashMap<String, Value>| {
            let mut out = Map::new();
            for n in names {
                let t = &map[n];
                let mut v = json!({
                    "order": order(t),
                    "index_patterns": patterns(t),
                    "settings": t.get("settings").cloned().unwrap_or(json!({})),
                    "mappings": t.get("mappings").cloned().unwrap_or(json!({})),
                    "aliases": t.get("aliases").cloned().unwrap_or(json!({})),
                });
                if let Some(ver) = t.get("version") {
                    v["version"] = ver.clone();
                }
                out.insert(n.clone(), v);
            }
            Value::Object(out)
        };
        match (name, method) {
            (None, "GET") => {
                let all = select(&self.legacy, "*").unwrap_or_default();
                (200, render(&all, &self.legacy))
            }
            (Some(name), "GET" | "HEAD") => match select(&self.legacy, name) {
                Ok(n) if n.is_empty() && !name.contains('*') => (404, json!({})),
                Ok(n) => (200, render(&n, &self.legacy)),
                Err(_) => (404, json!({})),
            },
            (Some(name), "PUT" | "POST") => {
                let Some(mut req) = parse_json(body) else {
                    return bad("parse_exception", "request body is required");
                };
                if req.get("index_patterns").is_none()
                    && let Some(t) = req.get("template").cloned()
                {
                    req["index_patterns"] = t;
                }
                let pats = patterns(&req);
                if pats.is_empty() {
                    return validation("index patterns are missing");
                }
                // A legacy template may not cover what a composable one
                // already does.
                let mut clash: Vec<(&String, Vec<String>)> = self
                    .index
                    .iter()
                    .filter(|(_, o)| {
                        patterns(o).iter().any(|a| pats.iter().any(|b| overlaps(a, b)))
                    })
                    .map(|(n, o)| (n, patterns(o)))
                    .collect();
                clash.sort();
                if !clash.is_empty() {
                    let names: Vec<String> = clash.iter().map(|(n, _)| n.to_string()).collect();
                    let with: Vec<String> =
                        clash.iter().map(|(n, ps)| format!("{n} => {}", list(ps))).collect();
                    return bad(
                        "illegal_argument_exception",
                        &format!(
                            "legacy template [{name}] has index patterns {} matching patterns \
                             from existing composable templates [{}] with patterns ({}), use \
                             composable templates (/_index_template) instead",
                            list(&pats),
                            names.join(","),
                            with.join(",")
                        ),
                    );
                }
                if truthy(q, "create") && self.legacy.contains_key(name) {
                    return bad(
                        "illegal_argument_exception",
                        &format!("index_template [{name}] already exists"),
                    );
                }
                let mut t = normalize_template_section(&req);
                t["order"] = json!(order(&req));
                t["index_patterns"] = json!(pats);
                t["settings"] = t.get("settings").cloned().unwrap_or(json!({}));
                t["mappings"] = t.get("mappings").cloned().unwrap_or(json!({}));
                t["aliases"] = t.get("aliases").cloned().unwrap_or(json!({}));
                if let Some(m) = t.as_object_mut() {
                    m.remove("template");
                }
                if let Some(v) = req.get("version") {
                    t["version"] = v.clone();
                }
                self.legacy.insert(name.to_string(), t);
                (200, json!({"acknowledged": true}))
            }
            (Some(name), "DELETE") => match select(&self.legacy, name) {
                Ok(n) if n.is_empty() => template_missing(name),
                Ok(n) => {
                    for x in n {
                        self.legacy.remove(&x);
                    }
                    (200, json!({"acknowledged": true}))
                }
                Err(_) => template_missing(name),
            },
            _ => (405, error("method_not_allowed_exception", "Incorrect HTTP method", 405)),
        }
    }

    /// `_cat/templates[/<name>]` rows: name, index_patterns, order,
    /// version, composed_of.
    pub fn cat_rows(&self, expr: Option<&str>) -> Result<Vec<Vec<String>>, (u16, Value)> {
        if let Some(e) = expr
            && e.contains(',')
        {
            return Err(bad("illegal_argument_exception", "template name may not contain ','"));
        }
        let want = |n: &str| expr.is_none_or(|e| glob(e, n));
        let ver = |t: &Value| t.get("version").map(|v| v.to_string()).unwrap_or_default();
        let mut rows = Vec::new();
        for (n, t) in &self.legacy {
            if want(n) {
                rows.push(vec![
                    n.clone(),
                    list(&patterns(t)),
                    order(t).to_string(),
                    ver(t),
                    String::new(),
                ]);
            }
        }
        for (n, t) in &self.index {
            if want(n) {
                let comp: Vec<String> = t
                    .get("composed_of")
                    .and_then(Value::as_array)
                    .map(|a| a.iter().filter_map(Value::as_str).map(str::to_string).collect())
                    .unwrap_or_default();
                rows.push(vec![
                    n.clone(),
                    list(&patterns(t)),
                    priority(t).to_string(),
                    ver(t),
                    list(&comp),
                ]);
            }
        }
        rows.sort();
        Ok(rows)
    }
}

fn resolved_out(r: Resolved) -> Value {
    let settings =
        if r.settings.as_object().is_some_and(|m| !m.is_empty()) { r.settings } else { json!({}) };
    let mappings =
        if r.mappings.as_object().is_some_and(|m| !m.is_empty()) { r.mappings } else { json!({}) };
    json!({"settings": settings, "mappings": mappings, "aliases": r.aliases})
}

#[cfg(test)]
mod tests {
    use super::*;

    fn call(t: &mut Templates, f: &str, method: &str, path: &str, body: &str) -> (u16, Value) {
        let segs: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
        let q = HashMap::new();
        match f {
            "index" => t.index_api(method, &segs, &q, body.as_bytes()),
            "component" => t.component_api(method, &segs, &q, body.as_bytes()),
            _ => t.legacy_api(method, &segs, &q, body.as_bytes()),
        }
    }

    #[test]
    fn highest_priority_composable_template_wins_with_components() {
        let mut t = Templates::default();
        let ct = r#"{"template":{"settings":{"number_of_replicas":2},"mappings":{"properties":{"a":{"type":"keyword"}}}}}"#;
        assert_eq!(call(&mut t, "component", "PUT", "/_component_template/ct", ct).0, 200);
        let low = r#"{"index_patterns":"logs-*","template":{"settings":{"number_of_shards":3}}}"#;
        assert_eq!(call(&mut t, "index", "PUT", "/_index_template/low", low).0, 200);
        let high = r#"{"index_patterns":["logs-*"],"priority":5,"composed_of":["ct"],"template":{"mappings":{"properties":{"b":{"type":"long"}}}}}"#;
        assert_eq!(call(&mut t, "index", "PUT", "/_index_template/high", high).0, 200);
        let r = t.resolve("logs-1").unwrap();
        assert_eq!(r.settings, json!({"index": {"number_of_replicas": "2"}}));
        assert_eq!(r.mappings["properties"]["a"]["type"], "keyword");
        assert_eq!(r.mappings["properties"]["b"]["type"], "long");
        // Same priority, overlapping patterns: refused.
        let clash = r#"{"index_patterns":["logs-a*"]}"#;
        assert_eq!(call(&mut t, "index", "PUT", "/_index_template/clash", clash).0, 400);
        // Missing component.
        let bad = r#"{"index_patterns":["x*"],"composed_of":["nope"]}"#;
        assert_eq!(call(&mut t, "index", "PUT", "/_index_template/bad", bad).0, 400);
        // In use.
        assert_eq!(call(&mut t, "component", "DELETE", "/_component_template/ct", "").0, 400);
        let (s, b) = call(&mut t, "index", "GET", "/_index_template", "");
        assert_eq!(s, 200);
        assert_eq!(b["index_templates"].as_array().unwrap().len(), 2);
        assert_eq!(call(&mut t, "index", "GET", "/_index_template/nope", "").0, 404);
    }

    #[test]
    fn legacy_templates_merge_by_order_only_without_composable_match() {
        let mut t = Templates::default();
        call(
            &mut t,
            "legacy",
            "PUT",
            "/_template/a",
            r#"{"index_patterns":["t*"],"order":1,"settings":{"number_of_shards":2}}"#,
        );
        call(
            &mut t,
            "legacy",
            "PUT",
            "/_template/b",
            r#"{"index_patterns":["t*"],"order":0,"settings":{"number_of_shards":5,"number_of_replicas":0}}"#,
        );
        let r = t.resolve("t1").unwrap();
        assert_eq!(
            r.settings,
            json!({"index": {"number_of_shards": "2", "number_of_replicas": "0"}})
        );
        let (s, b) = call(&mut t, "legacy", "GET", "/_template/a", "");
        assert_eq!(s, 200);
        assert_eq!(b["a"]["order"], 1);
        assert_eq!(call(&mut t, "legacy", "PUT", "/_template/c", "{}").0, 400);
    }
}
