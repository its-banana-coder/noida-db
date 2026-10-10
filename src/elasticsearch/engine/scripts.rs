//! Stored scripts: `PUT/GET/DELETE _scripts/{id}`, and requests that name
//! a stored script (`"script": {"id": ...}` in an update, update-by-query,
//! reindex, bulk update or search) run it as if it were given inline.

use serde_json::{Map, Value, json};

use super::{Engine, State, error, parse_json};

impl Engine {
    /// Everything under `/_scripts`.
    pub(super) fn scripts_api(&self, method: &str, seg: &[&str], body: &[u8]) -> (u16, Value) {
        match (method, &seg[1..]) {
            ("PUT" | "POST", [id] | [id, _]) => {
                let context = seg.get(2).copied();
                self.put_script(id, context, body)
            }
            ("GET", [id]) => {
                let s = self.0.lock().unwrap();
                match s.admin.scripts.get(*id) {
                    Some(sc) => (200, json!({"_id": id, "found": true, "script": sc})),
                    None => (404, json!({"_id": id, "found": false})),
                }
            }
            ("DELETE", [id]) => {
                let mut s = self.0.lock().unwrap();
                if s.admin.scripts.remove(*id).is_some() {
                    (200, json!({"acknowledged": true}))
                } else {
                    (
                        404,
                        error(
                            "resource_not_found_exception",
                            &format!("stored script [{id}] does not exist and cannot be deleted"),
                            404,
                        ),
                    )
                }
            }
            _ => super::no_handler(method, &format!("/{}", seg.join("/"))),
        }
    }

    fn put_script(&self, id: &str, context: Option<&str>, body: &[u8]) -> (u16, Value) {
        let Some(req) = parse_json(body) else { return (400, super::malformed_body()) };
        if let Some(k) = req.as_object().and_then(|m| m.keys().find(|k| *k != "script")) {
            return (
                400,
                json!({"error": {"root_cause": [{"type": "parsing_exception", "reason": format!("unexpected field [{k}], expected [script]"), "line": 1, "col": 2}],
                                 "type": "parsing_exception", "reason": format!("unexpected field [{k}], expected [script]"), "line": 1, "col": 2},
                       "status": 400}),
            );
        }
        let iae = |m: &str| (400, error("illegal_argument_exception", m, 400));
        let Some(sc) = req.get("script").and_then(Value::as_object) else {
            return iae("must specify lang for stored script");
        };
        let Some(lang) = sc.get("lang").and_then(Value::as_str) else {
            return iae("must specify lang for stored script");
        };
        if !matches!(lang, "painless" | "mustache" | "expression") {
            return iae(&format!("unable to put stored script with unsupported lang [{lang}]"));
        }
        let source = match sc.get("source").or_else(|| sc.get("code")) {
            Some(Value::String(s)) if !s.is_empty() => s.clone(),
            Some(v @ Value::Object(_)) if lang == "mustache" => v.to_string(),
            _ => return iae("must specify source for stored script"),
        };
        // With a context, the script is compiled for it first.
        if context.is_some()
            && lang == "painless"
            && let Err(e) = super::painless::compile(&source)
        {
            return (
                400,
                json!({"error": {"root_cause": [{"type": "script_exception", "reason": "compile error", "script_stack": [], "script": source, "lang": "painless"}],
                                 "type": "script_exception", "reason": "compile error", "script_stack": [], "script": source, "lang": "painless",
                                 "caused_by": {"type": "illegal_argument_exception", "reason": e}},
                       "status": 400}),
            );
        }
        let mut stored = Map::new();
        stored.insert("lang".into(), json!(lang));
        stored.insert("source".into(), json!(source));
        if lang == "mustache" {
            let options = sc
                .get("options")
                .cloned()
                .unwrap_or_else(|| json!({"content_type": "application/json;charset=utf-8"}));
            stored.insert("options".into(), options);
        } else if let Some(o) = sc.get("options") {
            stored.insert("options".into(), o.clone());
        }
        let mut s = self.0.lock().unwrap();
        s.admin.scripts.insert(id.to_string(), Value::Object(stored));
        (200, json!({"acknowledged": true}))
    }

    /// A request body with every `"script": {"id": ...}` replaced by the
    /// stored script's source (`None` when nothing names one).
    pub(super) fn inline_stored_scripts(
        &self,
        seg: &[&str],
        body: &[u8],
    ) -> Result<Option<Vec<u8>>, (u16, Value)> {
        const USES: &[&str] = &[
            "_search",
            "_update",
            "_update_by_query",
            "_delete_by_query",
            "_bulk",
            "_reindex",
            "_msearch",
            "_explain",
            "_count",
        ];
        let Some(api) = seg.iter().rev().find(|x| USES.contains(x)) else { return Ok(None) };
        let text = String::from_utf8_lossy(body);
        if !text.contains("\"id\"") || !text.contains("\"script\"") {
            return Ok(None);
        }
        let s = self.0.lock().unwrap();
        let search = matches!(*api, "_search" | "_msearch" | "_count" | "_explain");
        if matches!(*api, "_bulk" | "_msearch") {
            // Newline-delimited: only bulk `update` bodies and msearch
            // bodies carry scripts.
            let mut out = String::new();
            let mut changed = false;
            let mut after_update = false;
            let mut n = 0;
            for line in text.split_inclusive('\n') {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    out.push_str(line);
                    continue;
                }
                let is_body = if *api == "_bulk" { after_update } else { n % 2 == 1 };
                n += 1;
                after_update = false;
                if *api == "_bulk"
                    && let Ok(Value::Object(m)) = serde_json::from_str::<Value>(trimmed)
                    && m.contains_key("update")
                {
                    after_update = true;
                }
                if is_body && let Some(mut v) = parse_json(trimmed.as_bytes()) {
                    if replace_scripts(&s, &mut v, search)? {
                        changed = true;
                        out.push_str(&v.to_string());
                        out.push('\n');
                        continue;
                    }
                }
                out.push_str(line);
            }
            return Ok(changed.then(|| out.into_bytes()));
        }
        let Some(mut v) = parse_json(body) else { return Ok(None) };
        if replace_scripts(&s, &mut v, search)? {
            return Ok(Some(v.to_string().into_bytes()));
        }
        Ok(None)
    }
}

/// Replaces stored-script references in `v`; whether any was found.
fn replace_scripts(s: &State, v: &mut Value, search: bool) -> Result<bool, (u16, Value)> {
    let mut changed = false;
    match v {
        Value::Object(m) => {
            if let Some(Value::Object(sc)) = m.get_mut("script")
                && let Some(id) = sc.get("id").and_then(Value::as_str).map(String::from)
                && !sc.contains_key("source")
            {
                let Some(stored) = s.admin.scripts.get(&id) else {
                    return Err(missing_script(&id, search));
                };
                sc.remove("id");
                sc.insert("source".into(), stored["source"].clone());
                if let Some(l) = stored.get("lang") {
                    sc.insert("lang".into(), l.clone());
                }
                changed = true;
            }
            for (_, x) in m.iter_mut() {
                changed |= replace_scripts(s, x, search)?;
            }
        }
        Value::Array(a) => {
            for x in a {
                changed |= replace_scripts(s, x, search)?;
            }
        }
        _ => {}
    }
    Ok(changed)
}

fn missing_script(id: &str, search: bool) -> (u16, Value) {
    let reason = format!("unable to find script [{id}] in cluster state");
    let cause = json!({"type": "resource_not_found_exception", "reason": reason});
    if search {
        return (
            400,
            json!({"error": {"root_cause": [cause.clone()], "type": "search_phase_execution_exception",
                             "reason": "all shards failed", "phase": "query", "grouped": true,
                             "failed_shards": [], "caused_by": cause}, "status": 400}),
        );
    }
    (
        400,
        json!({"error": {"root_cause": [{"type": "illegal_argument_exception", "reason": "failed to execute script"}],
                         "type": "illegal_argument_exception", "reason": "failed to execute script",
                         "caused_by": cause}, "status": 400}),
    )
}

#[cfg(test)]
mod tests {
    use super::super::Engine;
    use serde_json::{Value, json};

    fn call(e: &Engine, method: &str, path: &str, body: Value) -> (u16, Value) {
        let (p, q) = path.split_once('?').unwrap_or((path, ""));
        let b = if body.is_null() { vec![] } else { body.to_string().into_bytes() };
        e.dispatch(method, p, q, &b)
    }

    #[test]
    fn stored_scripts_crud_and_use() {
        let e = Engine::default();
        let sc = json!({"script": {"lang": "painless", "source": "ctx._source.n += params.k"}});
        assert_eq!(call(&e, "PUT", "/_scripts/inc", sc).0, 200);
        let (_, r) = call(&e, "GET", "/_scripts/inc", Value::Null);
        assert_eq!(r["script"]["source"], json!("ctx._source.n += params.k"));
        call(&e, "PUT", "/i/_doc/1?refresh=true", json!({"n": 1}));
        let (st, _) =
            call(&e, "POST", "/i/_update/1", json!({"script": {"id": "inc", "params": {"k": 5}}}));
        assert_eq!(st, 200);
        assert_eq!(call(&e, "GET", "/i/_doc/1", Value::Null).1["_source"], json!({"n": 6}));
        let (st, r) = call(&e, "POST", "/i/_update/1", json!({"script": {"id": "nope"}}));
        assert_eq!(st, 400);
        assert_eq!(r["error"]["caused_by"]["type"], json!("resource_not_found_exception"));
        assert_eq!(call(&e, "DELETE", "/_scripts/inc", Value::Null).0, 200);
        assert_eq!(
            call(&e, "GET", "/_scripts/inc", Value::Null),
            (404, json!({"_id": "inc", "found": false}))
        );
        assert_eq!(call(&e, "PUT", "/_scripts/x", json!({"script": {"source": "1"}})).0, 400);
    }
}
