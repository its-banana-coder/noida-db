//! `script_fields`: per hit, the value of a Painless script (with `doc`
//! values, `params` and `params._source`), returned under `fields` --
//! a list, as every field value is.

use serde_json::{Map, Value, json};

use super::painless;
use super::queries::doc_view;
use super::search::{CommittedDoc, EsError, resolve_field};

/// The script fields of one hit.
pub fn values(body: &Value, mappings: &Value, d: &CommittedDoc) -> Result<Map<String, Value>, EsError> {
    let mut out = Map::new();
    let Some(Value::Object(specs)) = body.get("script_fields") else { return Ok(out) };
    for (name, spec) in specs {
        let script = spec.get("script").unwrap_or(&Value::Null);
        let (src, mut params) = painless::script_parts(script).map_err(|e| EsError::parsing(&e))?;
        let compiled = painless::compile(&src).map_err(|e| {
            EsError::shard_failure("script_exception", "compile error")
                .caused_by("illegal_argument_exception", &e)
        })?;
        // `doc['f']` on a field the index doesn't map fails at run time.
        let mut rest = src.as_str();
        while let Some(i) = rest.find("doc[") {
            rest = &rest[i + 4..];
            let q = rest.chars().next().unwrap_or('\'');
            let field: String = rest[q.len_utf8()..].chars().take_while(|c| *c != q).collect();
            if resolve_field(mappings, &field).1.is_none() {
                return Err(EsError::shard_failure("script_exception", "runtime error").caused_by(
                    "illegal_argument_exception",
                    &format!("No field found for [{field}] in mapping"),
                ));
            }
        }
        if let Some(p) = params.as_object_mut() {
            p.insert("_source".into(), d.full().clone());
        }
        let mut vars = Map::new();
        vars.insert("doc".into(), doc_view(mappings, d, &src));
        vars.insert("params".into(), params);
        let v = compiled.value(vars).map_err(|e| {
            EsError::shard_failure("script_exception", "runtime error")
                .caused_by("illegal_argument_exception", &e)
        })?;
        out.insert(
            name.clone(),
            match v {
                Value::Array(a) => Value::Array(a),
                other => json!([other]),
            },
        );
    }
    Ok(out)
}
